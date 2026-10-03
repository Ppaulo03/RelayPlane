package contracttest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/ports"
)

// QueueInlineLimit is the inline payload limit queues under test must be
// configured with.
const QueueInlineLimit = 1024

// QueueFactory returns a fresh, empty queue configured with QueueInlineLimit.
// Implementations should make dead/unacked redelivery fast (short leases).
type QueueFactory func(t *testing.T) ports.CommandQueue

type recorded struct {
	key string
	seq int
	at  time.Time
}

type payload struct {
	Seq int `json:"seq"`
}

func publishSeq(t *testing.T, q ports.CommandQueue, key string, seq int) {
	t.Helper()
	err := q.Publish(context.Background(), ports.Command{
		ID: fmt.Sprintf("%s-%d-%d", key, seq, rand.Int63()), PartitionKey: key, IdempotencyKey: fmt.Sprintf("%s-%d", key, seq), Payload: payload{Seq: seq},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func decodeSeq(t *testing.T, c ports.Command) int {
	t.Helper()
	raw, ok := c.Payload.(json.RawMessage)
	if !ok {
		b, _ := json.Marshal(c.Payload)
		raw = b
	}
	var p payload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Errorf("payload decode: %v", err)
	}
	return p.Seq
}

func startConsumers(t *testing.T, q ports.CommandQueue, n int, h ports.CommandHandler) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Consume(ctx, h)
		}()
	}
	return func() { cancel(); wg.Wait() }
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// CommandQueueContract is the behavioural contract of ports.CommandQueue.
func CommandQueueContract(t *testing.T, factory QueueFactory) {
	t.Run("PublishConsumeAck", func(t *testing.T) {
		q := factory(t)
		var got atomic.Int32
		stop := startConsumers(t, q, 1, func(_ context.Context, c ports.Command) (ports.Result, error) {
			if c.PartitionKey != "inst_a" || c.Attempt < 1 || decodeSeq(t, c) != 1 {
				t.Errorf("bad delivery %+v", c)
			}
			got.Add(1)
			return ports.Result{Disposition: ports.Ack}, nil
		})
		defer stop()
		publishSeq(t, q, "inst_a", 1)
		eventually(t, 5*time.Second, "delivery", func() bool { return got.Load() == 1 })
		eventually(t, 5*time.Second, "depth 0", func() bool { d, _ := q.Depth(context.Background()); return d == 0 })
		time.Sleep(100 * time.Millisecond)
		if got.Load() != 1 {
			t.Errorf("acked command redelivered: %d", got.Load())
		}
	})

	t.Run("RejectsOversizedInlinePayloadINV11", func(t *testing.T) {
		q := factory(t)
		big := make([]byte, QueueInlineLimit*4)
		for i := range big {
			big[i] = 'A'
		}
		err := q.Publish(context.Background(), ports.Command{ID: "big", PartitionKey: "inst_a", Payload: map[string]string{"data": string(big)}})
		if !errors.Is(err, errs.ErrPayloadTooLarge) {
			t.Fatalf("INV-11: want ErrPayloadTooLarge, got %v", err)
		}
		if d, _ := q.Depth(context.Background()); d != 0 {
			t.Errorf("oversized payload reached the broker (depth %d)", d)
		}
	})

	t.Run("OrderingPerKeyUnderConcurrencyINV07", func(t *testing.T) {
		q := factory(t)
		const keys, perKey = 6, 40
		var mu sync.Mutex
		var seen []recorded
		active := map[string]int{}
		var overlap atomic.Int32
		stop := startConsumers(t, q, 3, func(_ context.Context, c ports.Command) (ports.Result, error) {
			mu.Lock()
			active[c.PartitionKey]++
			if active[c.PartitionKey] > 1 {
				overlap.Add(1)
			}
			mu.Unlock()
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond) // jitter
			mu.Lock()
			seen = append(seen, recorded{key: c.PartitionKey, seq: decodeSeq(t, c)})
			active[c.PartitionKey]--
			mu.Unlock()
			return ports.Result{Disposition: ports.Ack}, nil
		})
		defer stop()
		var wg sync.WaitGroup
		for k := 0; k < keys; k++ {
			wg.Add(1)
			go func(k int) { // concurrent publishers, each sequential per key
				defer wg.Done()
				for s := 1; s <= perKey; s++ {
					publishSeq(t, q, fmt.Sprintf("inst_%d", k), s)
				}
			}(k)
		}
		wg.Wait()
		eventually(t, 30*time.Second, "all delivered", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == keys*perKey })
		mu.Lock()
		defer mu.Unlock()
		last := map[string]int{}
		for _, r := range seen {
			if r.seq != last[r.key]+1 {
				t.Fatalf("INV-07: key %s delivered seq %d after %d", r.key, r.seq, last[r.key])
			}
			last[r.key] = r.seq
		}
		if overlap.Load() != 0 {
			t.Errorf("INV-07: %d concurrent handlers for the same key", overlap.Load())
		}
	})

	t.Run("ParallelAcrossKeys", func(t *testing.T) {
		q := factory(t)
		var cur, peak atomic.Int32
		stop := startConsumers(t, q, 2, func(_ context.Context, c ports.Command) (ports.Result, error) {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(60 * time.Millisecond)
			cur.Add(-1)
			return ports.Result{Disposition: ports.Ack}, nil
		})
		defer stop()
		for k := 0; k < 24; k++ {
			publishSeq(t, q, fmt.Sprintf("inst_p%d", k), 1)
		}
		eventually(t, 20*time.Second, "drain", func() bool { d, _ := q.Depth(context.Background()); return d == 0 })
		if peak.Load() < 2 {
			t.Errorf("different keys should run in parallel, peak concurrency %d", peak.Load())
		}
	})

	t.Run("RetryDoesNotLetLaterCommandsOvertake", func(t *testing.T) {
		q := factory(t)
		var mu sync.Mutex
		var order []string
		var attempts atomic.Int32
		stop := startConsumers(t, q, 2, func(_ context.Context, c ports.Command) (ports.Result, error) {
			seq := decodeSeq(t, c)
			if c.PartitionKey == "inst_r" && seq == 1 && attempts.Add(1) == 1 {
				return ports.Result{Disposition: ports.Retry, After: 400 * time.Millisecond, MaxAttempts: 5, Reason: "boom"}, nil
			}
			mu.Lock()
			order = append(order, fmt.Sprintf("%s/%d", c.PartitionKey, seq))
			mu.Unlock()
			return ports.Result{Disposition: ports.Ack}, nil
		})
		defer stop()
		publishSeq(t, q, "inst_r", 1)
		publishSeq(t, q, "inst_r", 2)
		publishSeq(t, q, "inst_r", 3)
		eventually(t, 15*time.Second, "all three", func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 3 })
		mu.Lock()
		defer mu.Unlock()
		if order[0] != "inst_r/1" || order[1] != "inst_r/2" || order[2] != "inst_r/3" {
			t.Fatalf("retried command was overtaken: %v", order)
		}
	})

	t.Run("RetryDelayDoesNotBlockOtherKeys", func(t *testing.T) {
		q := factory(t)
		var otherDone atomic.Int64
		var retriedAt atomic.Int64
		var attempts atomic.Int32
		stop := startConsumers(t, q, 2, func(_ context.Context, c ports.Command) (ports.Result, error) {
			if c.PartitionKey == "slow" {
				if attempts.Add(1) == 1 {
					return ports.Result{Disposition: ports.Retry, After: 3 * time.Second, MaxAttempts: 5}, nil
				}
				retriedAt.Store(time.Now().UnixNano())
				return ports.Result{Disposition: ports.Ack}, nil
			}
			otherDone.Store(time.Now().UnixNano())
			return ports.Result{Disposition: ports.Ack}, nil
		})
		defer stop()
		publishSeq(t, q, "slow", 1)
		for k := 0; k < 10; k++ {
			publishSeq(t, q, fmt.Sprintf("other%d", k), 1)
		}
		eventually(t, 15*time.Second, "both", func() bool { return retriedAt.Load() != 0 && otherDone.Load() != 0 })
		if otherDone.Load() > retriedAt.Load() {
			t.Errorf("unrelated keys waited for another key's retry backoff")
		}
	})

	t.Run("DeferDoesNotCountAttempts", func(t *testing.T) {
		q := factory(t)
		var calls atomic.Int32
		var lastAttempt atomic.Int32
		stop := startConsumers(t, q, 1, func(_ context.Context, c ports.Command) (ports.Result, error) {
			n := calls.Add(1)
			lastAttempt.Store(int32(c.Attempt))
			if n < 4 {
				return ports.Result{Disposition: ports.Defer, After: 20 * time.Millisecond}, nil
			}
			return ports.Result{Disposition: ports.Ack}, nil
		})
		defer stop()
		publishSeq(t, q, "inst_d", 1)
		eventually(t, 10*time.Second, "4 calls", func() bool { return calls.Load() == 4 })
		if lastAttempt.Load() != 1 {
			t.Errorf("deferrals must not bump the attempt counter, attempt=%d", lastAttempt.Load())
		}
		if dl, _ := q.DeadLetters(context.Background(), 10); len(dl) != 0 {
			t.Errorf("deferral dead-lettered: %v", dl)
		}
	})

	t.Run("DeadLetters", func(t *testing.T) {
		q := factory(t)
		var calls atomic.Int32
		stop := startConsumers(t, q, 1, func(_ context.Context, c ports.Command) (ports.Result, error) {
			if decodeSeq(t, c) == 1 {
				return ports.Result{Disposition: ports.DeadLetter, Reason: "poison"}, nil
			}
			calls.Add(1)
			return ports.Result{Disposition: ports.Retry, After: 5 * time.Millisecond, MaxAttempts: 3, Reason: "flaky"}, nil
		})
		defer stop()
		publishSeq(t, q, "inst_x", 1)
		publishSeq(t, q, "inst_y", 2)
		eventually(t, 10*time.Second, "two dead letters", func() bool { d, _ := q.DeadLetters(context.Background(), 10); return len(d) == 2 })
		if calls.Load() != 3 {
			t.Errorf("max attempts=3 must stop retrying after 3 deliveries, got %d", calls.Load())
		}
		dl, _ := q.DeadLetters(context.Background(), 10)
		reasons := map[string]bool{}
		for _, d := range dl {
			reasons[d.Command.PartitionKey] = d.Reason != ""
		}
		if !reasons["inst_x"] || !reasons["inst_y"] {
			t.Errorf("dead letters need reasons: %+v", dl)
		}
		eventually(t, 5*time.Second, "depth 0", func() bool { d, _ := q.Depth(context.Background()); return d == 0 })
	})

	t.Run("RedeliversUnackedAfterConsumerCrash", func(t *testing.T) {
		q := factory(t)
		started := make(chan struct{})
		ctx1, cancel1 := context.WithCancel(context.Background())
		done1 := make(chan struct{})
		go func() {
			defer close(done1)
			_ = q.Consume(ctx1, func(ctx context.Context, c ports.Command) (ports.Result, error) {
				close(started)
				<-ctx.Done() // crash mid-handler: never acks
				return ports.Result{}, ctx.Err()
			})
		}()
		publishSeq(t, q, "inst_c", 1)
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("first consumer never received the command")
		}
		cancel1()
		<-done1
		var attempt atomic.Int32
		stop := startConsumers(t, q, 1, func(_ context.Context, c ports.Command) (ports.Result, error) {
			attempt.Store(int32(c.Attempt))
			return ports.Result{Disposition: ports.Ack}, nil
		})
		defer stop()
		eventually(t, 30*time.Second, "redelivery to a new consumer", func() bool { return attempt.Load() != 0 })
		eventually(t, 5*time.Second, "depth 0", func() bool { d, _ := q.Depth(context.Background()); return d == 0 })
	})

	t.Run("DepthCountsUnacked", func(t *testing.T) {
		q := factory(t)
		for i := 1; i <= 3; i++ {
			publishSeq(t, q, "inst_q", i)
		}
		if d, _ := q.Depth(context.Background()); d != 3 {
			t.Errorf("depth %d", d)
		}
	})
}
