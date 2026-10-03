//go:build integration

package redisstreams_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/relayplane/relayplane/internal/adapters/lock/redislock"
	"github.com/relayplane/relayplane/internal/adapters/messaging/redisstreams"
	"github.com/relayplane/relayplane/internal/contracttest"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/ports"
)

var seq atomic.Int64

func client(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("RELAYPLANE_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:56390"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// each test uses its own key prefix so tests are isolated without FLUSHALL
func prefix() string { return fmt.Sprintf("rptest%d_%d", time.Now().UnixNano(), seq.Add(1)) }

func TestCommandQueueContract(t *testing.T) {
	c := client(t)
	contracttest.CommandQueueContract(t, func(t *testing.T) ports.CommandQueue {
		q, err := redisstreams.NewQueue(context.Background(), c, redisstreams.QueueConfig{
			Prefix: prefix(), Partitions: 8, InlineMaxBytes: contracttest.QueueInlineLimit,
			LeaseTTL: 900 * time.Millisecond, Block: 50 * time.Millisecond, DefaultRetryDelay: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return q
	})
}

func TestEventBusContract(t *testing.T) {
	c := client(t)
	contracttest.EventBusContract(t, func(t *testing.T) ports.EventBus {
		return redisstreams.NewBus(c, redisstreams.BusConfig{Prefix: prefix(), Block: 50 * time.Millisecond, ReclaimIdle: 100 * time.Millisecond})
	})
}

func TestLockerContract(t *testing.T) {
	c := client(t)
	contracttest.LockerContract(t, func(t *testing.T) ports.Locker { return redislock.New(c, prefix()) })
}

// Several workers share the partitions of one queue; every command is handled once.
func TestPartitionsAreSharedBetweenWorkers(t *testing.T) {
	c := client(t)
	pre := prefix()
	mk := func() *redisstreams.Queue {
		q, err := redisstreams.NewQueue(context.Background(), c, redisstreams.QueueConfig{Prefix: pre, Partitions: 8,
			LeaseTTL: time.Second, Block: 30 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	q1, q2 := mk(), mk()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var n1, n2 atomic.Int32
	go q1.Consume(ctx, func(_ context.Context, _ ports.Command) (ports.Result, error) { n1.Add(1); return ports.Result{}, nil })
	go q2.Consume(ctx, func(_ context.Context, _ ports.Command) (ports.Result, error) { n2.Add(1); return ports.Result{}, nil })
	time.Sleep(4 * time.Second) // let leases rebalance
	for i := 0; i < 200; i++ {
		if err := q1.Publish(ctx, ports.Command{PartitionKey: fmt.Sprintf("inst_%d", i), Payload: map[string]int{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for n1.Load()+n2.Load() < 200 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n1.Load()+n2.Load() != 200 {
		t.Fatalf("handled %d of 200", n1.Load()+n2.Load())
	}
	if n1.Load() == 0 || n2.Load() == 0 {
		t.Errorf("work not shared between workers: %d / %d", n1.Load(), n2.Load())
	}
}

// 15.6 A consumer group that is offline while more events than the retention are published
// is overtaken by trimming: Stats must say so (that is what the alerts are built on).
func TestEventBusStatsDetectTrimmingBeyondAConsumer(t *testing.T) {
	c := client(t)
	ctx := context.Background()
	bus := redisstreams.NewBus(c, redisstreams.BusConfig{Prefix: prefix(), MaxLen: 100, Block: 30 * time.Millisecond})
	// the group exists (created at the start of the stream) but its consumer is offline
	subCtx, stop := context.WithCancel(ctx)
	go bus.Subscribe(subCtx, "offline-soon", func(context.Context, events.Event) error { return nil })
	time.Sleep(300 * time.Millisecond)
	stop()
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 3000; i++ {
		if err := bus.Publish(ctx, events.Event{EventID: fmt.Sprintf("e%d", i), EventType: events.MessageReceived, InstanceID: "i", Payload: map[string]int{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
	st, err := bus.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Length >= 3000 || st.Retention != 100 {
		t.Fatalf("the stream must be trimmed to roughly the retention: %+v", st)
	}
	var g ports.EventBusGroupStats
	for _, x := range st.Groups {
		if x.Name == "offline-soon" {
			g = x
		}
	}
	if g.Lost < 2800 {
		t.Fatalf("events trimmed before the group read them must be reported as lost: %+v", g)
	}
	if st.TrimRisk() < 1 {
		t.Fatalf("a consumer that was overtaken has trim risk >= 1: %v", st.TrimRisk())
	}
}
