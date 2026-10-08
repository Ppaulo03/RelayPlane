// Package redisstreams implements the CommandQueue on Redis Streams. (Events do not travel on Redis: the event outbox in the database is
// the event stream.)
//
// Ordering strategy (adapter detail, not part of the port contract):
//
//   - a fixed number of partition streams (relayplane:cmd:<n>), with
//     partition = fnv32a(PartitionKey) % N, never one stream per instance;
//   - each partition is served by exactly one consumer at a time, enforced by
//     a renewable lease; the holder processes the partition sequentially, so
//     commands of one instance_id are delivered strictly in XADD order;
//   - a command that must wait (retry backoff, rate limit) delays only its own
//     key: later commands of *other* keys in the partition keep flowing, later
//     commands of the *same* key stay parked behind it;
//   - unacknowledged entries stay in the pending list and are reclaimed
//     (XAUTOCLAIM) by whoever acquires the lease next: at-least-once delivery.
package redisstreams

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"

	"github.com/relayplane/relayplane/internal/adapters/messaging/partition"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// QueueConfig configures the command queue.
type QueueConfig struct {
	Prefix             string        // key prefix, default "relayplane"
	Partitions         int           // number of partition streams, default 32
	InlineMaxBytes     int           // INV-11 broker-side payload guard
	LeaseTTL           time.Duration // partition lease, default 15s
	Block              time.Duration // XREADGROUP block, default 200ms
	Batch              int64         // entries read per call, default 20
	MaxBuffered        int           // in-memory parked entries per partition, default 500
	DefaultMaxAttempts int
	DefaultRetryDelay  time.Duration
	ConsumerID         string // default host-pid-random
	Group              string // consumer group, default "workers"
}

func (c *QueueConfig) defaults() {
	if c.Prefix == "" {
		c.Prefix = "relayplane"
	}
	if c.Partitions <= 0 {
		c.Partitions = 32
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 15 * time.Second
	}
	if c.Block <= 0 {
		c.Block = 200 * time.Millisecond
	}
	if c.Batch <= 0 {
		c.Batch = 20
	}
	if c.MaxBuffered <= 0 {
		c.MaxBuffered = 500
	}
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = 5
	}
	if c.DefaultRetryDelay <= 0 {
		c.DefaultRetryDelay = 5 * time.Second
	}
	if c.Group == "" {
		c.Group = "workers"
	}
	if c.ConsumerID == "" {
		host, _ := os.Hostname()
		b := make([]byte, 4)
		_, _ = rand.Read(b)
		c.ConsumerID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
	}
}

// Queue is the Redis Streams ports.CommandQueue.
type Queue struct {
	rdb redis.UniversalClient
	cfg QueueConfig
}

// NewQueue builds a queue and makes sure the consumer groups exist.
func NewQueue(ctx context.Context, rdb redis.UniversalClient, cfg QueueConfig) (*Queue, error) {
	cfg.defaults()
	q := &Queue{rdb: rdb, cfg: cfg}
	for p := 0; p < cfg.Partitions; p++ {
		err := rdb.XGroupCreateMkStream(ctx, q.stream(p), cfg.Group, "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return nil, err
		}
	}
	return q, nil
}

// Partitions returns the configured partition count.
func (q *Queue) Partitions() int { return q.cfg.Partitions }

func (q *Queue) stream(p int) string   { return fmt.Sprintf("%s:cmd:%03d", q.cfg.Prefix, p) }
func (q *Queue) lease(p int) string    { return fmt.Sprintf("%s:lease:%03d", q.cfg.Prefix, p) }
func (q *Queue) attempts(p int) string { return fmt.Sprintf("%s:attempts:%03d", q.cfg.Prefix, p) }
func (q *Queue) dlq() string           { return q.cfg.Prefix + ":dlq" }
func (q *Queue) consumers() string     { return q.cfg.Prefix + ":consumers" }

// Publish implements ports.CommandQueue.
func (q *Queue) Publish(ctx context.Context, cmd ports.Command) error {
	ctx, span := observability.Start(ctx, "queue.publish", attribute.String("messaging.system", "redis"))
	defer span.End()
	if cmd.PartitionKey == "" {
		return fmt.Errorf("redisstreams: command %q has no partition key", cmd.ID)
	}
	raw, err := json.Marshal(cmd.Payload)
	if err != nil {
		return err
	}
	if err := media.EnforceInlineLimit(raw, q.cfg.InlineMaxBytes); err != nil {
		return err
	}
	if cmd.ID == "" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		cmd.ID = "cmd-" + hex.EncodeToString(b)
	}
	tp := cmd.TraceParent
	if tp == "" {
		tp = observability.TraceParent(ctx)
	}
	p := partition.Of(cmd.PartitionKey, q.cfg.Partitions)
	err = q.rdb.XAdd(ctx, &redis.XAddArgs{Stream: q.stream(p), Values: map[string]any{
		"id": cmd.ID, "key": cmd.PartitionKey, "idem": cmd.IdempotencyKey, "payload": string(raw),
		"tp": tp, "at": time.Now().UnixMilli(),
	}}).Err()
	observability.Fail(span, err)
	return err
}

// Depth implements ports.CommandQueue (entries are deleted on ack).
func (q *Queue) Depth(ctx context.Context) (int64, error) {
	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, q.cfg.Partitions)
	for p := range cmds {
		cmds[p] = pipe.XLen(ctx, q.stream(p))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	var n int64
	for _, c := range cmds {
		n += c.Val()
	}
	return n, nil
}

// DeadLetters implements ports.CommandQueue.
func (q *Queue) DeadLetters(ctx context.Context, limit int) ([]ports.DeadLetterEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	msgs, err := q.rdb.XRevRangeN(ctx, q.dlq(), "+", "-", int64(limit)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]ports.DeadLetterEntry, 0, len(msgs))
	for _, m := range msgs {
		cmd := decodeCommand(m)
		reason, _ := m.Values["reason"].(string)
		at, _ := strconv.ParseInt(fmt.Sprint(m.Values["failed_at"]), 10, 64)
		out = append(out, ports.DeadLetterEntry{Command: cmd, Reason: reason, FailedAt: time.UnixMilli(at)})
	}
	return out, nil
}

func decodeCommand(m redis.XMessage) ports.Command {
	s := func(k string) string { v, _ := m.Values[k].(string); return v }
	return ports.Command{ID: s("id"), PartitionKey: s("key"), IdempotencyKey: s("idem"),
		Payload: json.RawMessage(s("payload")), TraceParent: s("tp")}
}

// ---------------- consumption ----------------

type entry struct {
	id        string
	cmd       ports.Command
	attempts  int
	notBefore time.Time
}

// Consume serves partitions until ctx is cancelled. Several processes (or
// several Consume calls) cooperate through partition leases.
func (q *Queue) Consume(ctx context.Context, handler ports.CommandHandler) error {
	id := q.cfg.ConsumerID
	// every Consume call is its own logical consumer so tests/processes can run several
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	id += "-" + hex.EncodeToString(b)

	var held sync.Map // partition -> struct{}
	heldCount := func() int {
		n := 0
		held.Range(func(_, _ any) bool { n++; return true })
		return n
	}
	go q.heartbeat(ctx, id)

	var wg sync.WaitGroup
	for p := 0; p < q.cfg.Partitions; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for ctx.Err() == nil {
				if heldCount() >= q.fairShare(ctx) {
					sleepCtx(ctx, 300*time.Millisecond+time.Duration(mrand.Intn(300))*time.Millisecond)
					continue
				}
				token := id
				ok, err := q.rdb.SetNX(ctx, q.lease(p), token, q.cfg.LeaseTTL).Result()
				if err != nil || !ok {
					sleepCtx(ctx, 200*time.Millisecond+time.Duration(mrand.Intn(300))*time.Millisecond)
					continue
				}
				held.Store(p, struct{}{})
				q.servePartition(ctx, p, id, token, handler, func() bool { return heldCount() > q.fairShare(ctx) })
				held.Delete(p)
				q.release(context.WithoutCancel(ctx), p, token)
			}
		}(p)
	}
	wg.Wait()
	_ = q.rdb.ZRem(context.WithoutCancel(ctx), q.consumers(), id).Err()
	return ctx.Err()
}

// heartbeat registers this consumer so peers can compute a fair partition share.
func (q *Queue) heartbeat(ctx context.Context, id string) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		now := time.Now().UnixMilli()
		pipe := q.rdb.Pipeline()
		pipe.ZAdd(ctx, q.consumers(), redis.Z{Score: float64(now), Member: id})
		pipe.ZRemRangeByScore(ctx, q.consumers(), "-inf", strconv.FormatInt(now-5000, 10))
		_, _ = pipe.Exec(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (q *Queue) fairShare(ctx context.Context) int {
	n, err := q.rdb.ZCard(ctx, q.consumers()).Result()
	if err != nil || n < 1 {
		n = 1
	}
	return int(math.Ceil(float64(q.cfg.Partitions) / float64(n)))
}

var releaseScript = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
var renewScript = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)

func (q *Queue) release(ctx context.Context, p int, token string) {
	_ = releaseScript.Run(ctx, q.rdb, []string{q.lease(p)}, token).Err()
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// servePartition processes one partition while the lease is held.
func (q *Queue) servePartition(ctx context.Context, p int, consumer, token string, handler ports.CommandHandler, shouldYield func() bool) {
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// renew the lease; losing it stops the loop
	go func() {
		t := time.NewTicker(q.cfg.LeaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-lctx.Done():
				return
			case <-t.C:
				res, err := renewScript.Run(lctx, q.rdb, []string{q.lease(p)}, token, q.cfg.LeaseTTL.Milliseconds()).Int()
				if lctx.Err() == nil && (err != nil || res == 0) {
					cancel() // lease lost
					return
				}
			}
		}
	}()

	stream, group := q.stream(p), q.cfg.Group
	var buf []*entry

	push := func(msgs []redis.XMessage) {
		for _, m := range msgs {
			e := &entry{id: m.ID, cmd: decodeCommand(m)}
			if n, err := q.rdb.HGet(lctx, q.attempts(p), m.ID).Int(); err == nil {
				e.attempts = n
			}
			buf = append(buf, e)
		}
		sort.SliceStable(buf, func(i, j int) bool { return idLess(buf[i].id, buf[j].id) })
	}

	// 1. recover everything still pending (ours from a previous run or a dead consumer's)
	start := "0-0"
	for lctx.Err() == nil {
		msgs, next, err := q.rdb.XAutoClaim(lctx, &redis.XAutoClaimArgs{Stream: stream, Group: group, Consumer: consumer, MinIdle: 0, Start: start, Count: 100}).Result()
		if err != nil {
			break
		}
		push(msgs)
		if next == "0-0" || next == "" {
			break
		}
		start = next
	}

	for lctx.Err() == nil {
		if len(buf) < q.cfg.MaxBuffered {
			wait := q.cfg.Block
			if len(buf) > 0 {
				wait = time.Millisecond // do not block while parked entries may become due
			}
			res, err := q.rdb.XReadGroup(lctx, &redis.XReadGroupArgs{Group: group, Consumer: consumer,
				Streams: []string{stream, ">"}, Count: q.cfg.Batch, Block: wait}).Result()
			switch {
			case err == nil:
				for _, s := range res {
					push(s.Messages)
				}
			case errors.Is(err, redis.Nil), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			case isNoGroup(err):
				// Redis lost its data (restart without persistence, failover to an empty replica, FLUSHALL):
				// the group is gone and would never come back by itself. Recreate it; entries published since
				// the wipe are still in the (re-created) stream and are read from the start.
				_ = q.rdb.XGroupCreateMkStream(lctx, stream, group, "0").Err()
			default:
				sleepCtx(lctx, 200*time.Millisecond)
			}
		}
		e := nextEligible(buf)
		if e == nil {
			if len(buf) == 0 && shouldYield() {
				return // idle and over our fair share: let a peer take this partition
			}
			if len(buf) > 0 {
				sleepCtx(lctx, 10*time.Millisecond)
			}
			continue
		}
		res := q.dispatch(lctx, handler, e)
		buf = q.apply(lctx, p, buf, e, res)
	}
}

// isNoGroup reports the error Redis returns when the stream or its consumer group does not exist.
func isNoGroup(err error) bool { return err != nil && strings.Contains(err.Error(), "NOGROUP") }

func idLess(a, b string) bool {
	am, as := splitID(a)
	bm, bs := splitID(b)
	if am != bm {
		return am < bm
	}
	return as < bs
}

func splitID(id string) (int64, int64) {
	ms, seq, _ := strings.Cut(id, "-")
	m, _ := strconv.ParseInt(ms, 10, 64)
	s, _ := strconv.ParseInt(seq, 10, 64)
	return m, s
}

// nextEligible returns the first entry whose key is not parked behind a delayed
// entry of the same key.
func nextEligible(buf []*entry) *entry {
	now := time.Now()
	blocked := map[string]bool{}
	for _, e := range buf {
		k := e.cmd.PartitionKey
		if blocked[k] {
			continue
		}
		if e.notBefore.After(now) {
			blocked[k] = true
			continue
		}
		return e
	}
	return nil
}

func (q *Queue) dispatch(ctx context.Context, h ports.CommandHandler, e *entry) ports.Result {
	cmd := e.cmd
	cmd.Attempt = e.attempts + 1
	hctx := observability.WithTraceParent(ctx, cmd.TraceParent)
	hctx, span := observability.Start(hctx, "queue.consume", attribute.String("messaging.system", "redis"))
	defer span.End()
	res, err := h(hctx, cmd)
	if err != nil {
		observability.Fail(span, err)
		if ctx.Err() != nil {
			return ports.Result{Disposition: ports.Defer, After: 0, Reason: "shutdown"} // left pending; see apply
		}
		if res == (ports.Result{}) {
			res = ports.Result{Disposition: ports.Retry, After: q.cfg.DefaultRetryDelay, Reason: err.Error()}
		}
	}
	return res
}

func (q *Queue) apply(ctx context.Context, p int, buf []*entry, e *entry, res ports.Result) []*entry {
	// the process is shutting down (or lost its lease) mid-handler with no verdict: leave the entry pending
	if ctx.Err() != nil && res.Disposition == ports.Defer && res.Reason == "shutdown" {
		return buf
	}
	bg := context.WithoutCancel(ctx)
	remove := func() []*entry {
		for i, x := range buf {
			if x == e {
				return append(buf[:i], buf[i+1:]...)
			}
		}
		return buf
	}
	ackDel := func() {
		pipe := q.rdb.Pipeline()
		pipe.XAck(bg, q.stream(p), q.cfg.Group, e.id)
		pipe.XDel(bg, q.stream(p), e.id)
		pipe.HDel(bg, q.attempts(p), e.id)
		_, _ = pipe.Exec(bg)
	}
	dead := func(reason string) []*entry {
		pipe := q.rdb.Pipeline()
		pipe.XAdd(bg, &redis.XAddArgs{Stream: q.dlq(), MaxLen: 10000, Approx: true, Values: map[string]any{
			"id": e.cmd.ID, "key": e.cmd.PartitionKey, "idem": e.cmd.IdempotencyKey, "payload": string(e.cmd.Payload.(json.RawMessage)),
			"tp": e.cmd.TraceParent, "reason": reason, "failed_at": time.Now().UnixMilli()}})
		pipe.XAck(bg, q.stream(p), q.cfg.Group, e.id)
		pipe.XDel(bg, q.stream(p), e.id)
		pipe.HDel(bg, q.attempts(p), e.id)
		_, _ = pipe.Exec(bg)
		return remove()
	}
	switch res.Disposition {
	case ports.Ack:
		ackDel()
		return remove()
	case ports.DeadLetter:
		return dead(res.Reason)
	case ports.Defer:
		e.notBefore = time.Now().Add(res.After)
	case ports.Retry:
		e.attempts++
		_ = q.rdb.HSet(bg, q.attempts(p), e.id, e.attempts).Err()
		max := res.MaxAttempts
		if max <= 0 {
			max = q.cfg.DefaultMaxAttempts
		}
		if e.attempts >= max {
			return dead("max attempts exceeded: " + res.Reason)
		}
		e.notBefore = time.Now().Add(res.After)
	}
	return buf
}
