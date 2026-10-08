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

// wipe deletes every key of a prefix: what a Redis restart without persistence (or a failover to an empty
// replica) does to the broker's streams, consumer groups, leases and retry counters.
func wipe(t *testing.T, c *redis.Client, pfx string) {
	t.Helper()
	ctx := context.Background()
	keys, err := c.Keys(ctx, pfx+"*").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) > 0 {
		if err := c.Del(ctx, keys...).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// Redis losing its data must not leave consumers spinning on NOGROUP forever.
func TestQueue_ConsumersRecoverAfterRedisLosesItsData(t *testing.T) {
	c := client(t)
	pfx := prefix()
	q, err := redisstreams.NewQueue(context.Background(), c, redisstreams.QueueConfig{Prefix: pfx, Partitions: 4, InlineMaxBytes: contracttest.QueueInlineLimit,
		LeaseTTL: 900 * time.Millisecond, Block: 30 * time.Millisecond, DefaultRetryDelay: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got atomic.Int64
	go func() {
		_ = q.Consume(ctx, func(context.Context, ports.Command) (ports.Result, error) {
			got.Add(1)
			return ports.Result{Disposition: ports.Ack}, nil
		})
	}()
	pub := func(id string) {
		if err := q.Publish(ctx, ports.Command{ID: id, PartitionKey: "inst_a", Payload: map[string]string{"x": id}}); err != nil {
			t.Fatal(err)
		}
	}
	pub("before")
	waitFor(t, "first command", func() bool { return got.Load() == 1 })
	wipe(t, c, pfx)
	pub("after") // XADD recreates the stream, but not the consumer group
	waitFor(t, "command published after the wipe", func() bool { return got.Load() == 2 })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func testEvent(i int) events.Event {
	return events.Event{EventID: fmt.Sprintf("evt_%d", i), EventType: events.MessageReceived, Provider: "evolution-v2",
		TenantID: "t1", InstanceID: "inst_1", Timestamp: time.Now().UTC(),
		Payload: events.MessageReceivedPayload{ProviderMessageID: fmt.Sprintf("m%d", i), From: "5562", Type: "text", Text: "hi"}}
}
