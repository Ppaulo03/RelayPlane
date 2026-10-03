package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/messaging/partition"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/ports"
)

// QueueConfig tunes the in-memory CommandQueue.
type QueueConfig struct {
	Partitions         int
	InlineMaxBytes     int
	DefaultMaxAttempts int
	DefaultRetryDelay  time.Duration
	Poll               time.Duration
}

// Queue is an in-memory ports.CommandQueue with the same ordering semantics
// as the Redis adapter: fixed partitions, one consumer per partition at a
// time (lease), strict FIFO per partition key, retries that never overtake.
type Queue struct {
	cfg  QueueConfig
	mu   sync.Mutex
	part []*qpart
	dlq  []ports.DeadLetterEntry
	seq  atomic.Int64
	cid  atomic.Int64
}

type qitem struct {
	cmd       ports.Command
	attempts  int
	notBefore time.Time
	running   bool
}

type qpart struct {
	items []*qitem
	owner int64 // consumer id holding the partition lease
}

// NewQueue builds a queue; zero config fields get defaults.
func NewQueue(cfg QueueConfig) *Queue {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 8
	}
	if cfg.DefaultMaxAttempts <= 0 {
		cfg.DefaultMaxAttempts = 5
	}
	if cfg.DefaultRetryDelay == 0 {
		cfg.DefaultRetryDelay = 10 * time.Millisecond
	}
	if cfg.Poll == 0 {
		cfg.Poll = 2 * time.Millisecond
	}
	q := &Queue{cfg: cfg}
	for i := 0; i < cfg.Partitions; i++ {
		q.part = append(q.part, &qpart{})
	}
	return q
}

// Partitions returns the partition count.
func (q *Queue) Partitions() int { return q.cfg.Partitions }

func (q *Queue) Publish(_ context.Context, cmd ports.Command) error {
	if cmd.PartitionKey == "" {
		return fmt.Errorf("memory queue: command %q has no partition key", cmd.ID)
	}
	raw, err := json.Marshal(cmd.Payload)
	if err != nil {
		return err
	}
	if err := media.EnforceInlineLimit(raw, q.cfg.InlineMaxBytes); err != nil {
		return err
	}
	cmd.Payload = json.RawMessage(raw)
	if cmd.ID == "" {
		cmd.ID = fmt.Sprintf("cmd-%d", q.seq.Add(1))
	}
	p := q.part[partition.Of(cmd.PartitionKey, q.cfg.Partitions)]
	q.mu.Lock()
	p.items = append(p.items, &qitem{cmd: cmd})
	q.mu.Unlock()
	return nil
}

func (q *Queue) Depth(_ context.Context) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var n int64
	for _, p := range q.part {
		n += int64(len(p.items))
	}
	return n, nil
}

func (q *Queue) DeadLetters(_ context.Context, limit int) ([]ports.DeadLetterEntry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := append([]ports.DeadLetterEntry(nil), q.dlq...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Consume runs one goroutine per partition. Several Consume calls simulate
// several workers: a partition is served by exactly one of them at a time.
func (q *Queue) Consume(ctx context.Context, h ports.CommandHandler) error {
	id := q.cid.Add(1)
	var wg sync.WaitGroup
	for i := range q.part {
		wg.Add(1)
		go func(p *qpart) {
			defer wg.Done()
			q.runPartition(ctx, id, p, h)
		}(q.part[i])
	}
	wg.Wait()
	return ctx.Err()
}

func (q *Queue) acquire(p *qpart, id int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.owner == 0 || p.owner == id {
		p.owner = id
		return true
	}
	return false
}

func (q *Queue) runPartition(ctx context.Context, id int64, p *qpart, h ports.CommandHandler) {
	defer func() {
		q.mu.Lock()
		if p.owner == id {
			p.owner = 0
		}
		q.mu.Unlock()
	}()
	for ctx.Err() == nil {
		if !q.acquire(p, id) {
			sleep(ctx, q.cfg.Poll*5)
			continue
		}
		it := q.next(p)
		if it == nil {
			sleep(ctx, q.cfg.Poll)
			continue
		}
		cmd := it.cmd
		cmd.Attempt = it.attempts + 1
		res, err := h(ctx, cmd)
		if ctx.Err() != nil && err != nil { // shutting down mid-flight: leave for redelivery
			q.mu.Lock()
			it.running = false
			q.mu.Unlock()
			return
		}
		if err != nil && res == (ports.Result{}) {
			res = ports.Result{Disposition: ports.Retry, After: q.cfg.DefaultRetryDelay, Reason: err.Error()}
		}
		q.apply(p, it, res)
	}
}

// next returns the first eligible item, honouring per-key blocking so that a
// delayed command is never overtaken by a later command of the same key.
func (q *Queue) next(p *qpart) *qitem {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	blocked := map[string]bool{}
	for _, it := range p.items {
		k := it.cmd.PartitionKey
		if blocked[k] || it.running {
			blocked[k] = true
			continue
		}
		if it.notBefore.After(now) {
			blocked[k] = true
			continue
		}
		it.running = true
		return it
	}
	return nil
}

func (q *Queue) apply(p *qpart, it *qitem, res ports.Result) {
	q.mu.Lock()
	defer q.mu.Unlock()
	remove := func() {
		for i, x := range p.items {
			if x == it {
				p.items = append(p.items[:i], p.items[i+1:]...)
				return
			}
		}
	}
	dead := func(reason string) {
		remove()
		q.dlq = append(q.dlq, ports.DeadLetterEntry{Command: it.cmd, Reason: reason, FailedAt: time.Now()})
	}
	it.running = false
	switch res.Disposition {
	case ports.Ack:
		remove()
	case ports.DeadLetter:
		dead(res.Reason)
	case ports.Defer:
		it.notBefore = time.Now().Add(res.After)
	case ports.Retry:
		it.attempts++
		max := res.MaxAttempts
		if max <= 0 {
			max = q.cfg.DefaultMaxAttempts
		}
		if it.attempts >= max {
			dead("max attempts exceeded: " + res.Reason)
			return
		}
		it.notBefore = time.Now().Add(res.After)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
