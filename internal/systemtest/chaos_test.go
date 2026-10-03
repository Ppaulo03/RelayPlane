//go:build integration && chaos

package systemtest

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

// Chaos suite: real PostgreSQL + Redis + S3 with faults injected into the infrastructure while traffic flows.
// Needs `make infra-up` and RELAYPLANE_SYSTEMTEST_BACKEND=real (see `make test-chaos`).
//
// Invariants checked after every fault (the guarantees that must survive ANY of them):
//   - no message is stuck in QUEUED/DISPATCHING once the infrastructure is back (the outbox recovers it);
//   - the provider never receives the same message twice (at-most-once on the wire);
//   - per instance, the provider receives messages in sequence order (INV-07);
//   - only ACCEPTED or UNKNOWN (ambiguous, needs `resolve`) are acceptable end states - nothing is silently lost.

const infraFile = "../../deploy/docker/compose.infra.yml"

func compose(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"compose", "-f", infraFile}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

type traffic struct {
	e     *Env
	mu    sync.Mutex
	ids   []string
	texts map[string]string // message id -> text
	insts []string
}

// run sends `n` messages round-robin over the instances, one every `gap`; send errors during a fault are tolerated
// (the client would retry with its idempotency key) but accepted messages are tracked.
func (tr *traffic) run(n int, gap time.Duration, from int) {
	for i := from; i < from+n; i++ {
		inst := tr.insts[i%len(tr.insts)]
		text := fmt.Sprintf("m%04d", i)
		r, _, err := tr.e.SendText(tr.e.Tenant, inst, text, fmt.Sprintf("chaos-%d", i))
		if err == nil {
			tr.mu.Lock()
			tr.ids = append(tr.ids, r.MessageID)
			tr.texts[r.MessageID] = text
			tr.mu.Unlock()
		}
		time.Sleep(gap)
	}
}

func newTraffic(t *testing.T, instances int) *traffic {
	e := NewEnv(t)
	e.Worker.UnknownBarrierTimeout = 300 * time.Millisecond // keep the queue moving past UNKNOWN heads
	tr := &traffic{e: e, texts: map[string]string{}}
	for i := 0; i < instances; i++ {
		tr.insts = append(tr.insts, e.CreateInstance(e.Tenant, fmt.Sprintf("chaos-%d", i), true).ID)
	}
	e.StartWorkers(3)
	e.StartProjector()
	e.StartOutbox()
	e.wg.Add(1)
	go func() { // the reconciler maintenance loop: redispatch lost commands
		defer e.wg.Done()
		for e.ctx.Err() == nil {
			_, _ = e.App.Outbox.Redispatch(e.ctx, time.Second, 100)
			time.Sleep(300 * time.Millisecond)
		}
	}()
	return tr
}

func (tr *traffic) assertConverges(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	tr.mu.Lock()
	ids := append([]string(nil), tr.ids...)
	tr.mu.Unlock()
	if len(ids) == 0 {
		t.Fatal("no message was accepted")
	}
	Eventually(t, 60*time.Second, "all accepted messages leave QUEUED/DISPATCHING", func() bool {
		for _, id := range ids {
			m, err := tr.e.Repos.Messages.Get(ctx, id)
			if err != nil || m.Status == messaging.StatusQueued || m.Status == messaging.StatusDispatching {
				return false
			}
		}
		return true
	})
	unknown := 0
	for _, id := range ids {
		m, _ := tr.e.Repos.Messages.Get(ctx, id)
		switch m.Status {
		case messaging.StatusAccepted, messaging.StatusDelivered, messaging.StatusRead:
		case messaging.StatusUnknown:
			unknown++
		default:
			t.Errorf("message %s ended %s (%s): only ACCEPTED or UNKNOWN are legitimate after infrastructure faults", id, m.Status, m.ErrorCode)
		}
	}
	// at-most-once and per-instance ordering on the wire
	seen := map[string]int{}
	last := map[string]string{}
	for _, s := range tr.e.Provider.Sent() {
		text := s.Message.Text
		seen[text]++
		if seen[text] > 1 {
			t.Errorf("message %s reached the provider %d times", text, seen[text])
		}
		if prev := last[s.Assignment.InstanceID]; prev != "" && text < prev {
			t.Errorf("INV-07: instance %s received %s after %s", s.Assignment.InstanceID, text, prev)
		}
		last[s.Assignment.InstanceID] = text
	}
	t.Logf("%d accepted, %d sent, %d ended UNKNOWN (ambiguous, resolvable)", len(ids), len(tr.e.Provider.Sent()), unknown)
}

func TestChaos_RedisWipedWhileTrafficFlows(t *testing.T) {
	tr := newTraffic(t, 4)
	done := make(chan struct{})
	go func() { defer close(done); tr.run(60, 40*time.Millisecond, 0) }()
	time.Sleep(700 * time.Millisecond)
	compose(t, "restart", "redis") // no persistence: streams, groups, leases, retry counters - everything is gone
	<-done
	tr.run(10, 20*time.Millisecond, 60) // new traffic after the wipe
	tr.assertConverges(t)
}

func TestChaos_RedisPartition(t *testing.T) {
	tr := newTraffic(t, 3)
	done := make(chan struct{})
	go func() { defer close(done); tr.run(45, 40*time.Millisecond, 0) }()
	time.Sleep(500 * time.Millisecond)
	compose(t, "pause", "redis")
	time.Sleep(3 * time.Second)
	compose(t, "unpause", "redis")
	<-done
	tr.assertConverges(t)
}

func TestChaos_PostgresStall(t *testing.T) {
	tr := newTraffic(t, 3)
	done := make(chan struct{})
	go func() { defer close(done); tr.run(45, 40*time.Millisecond, 0) }()
	time.Sleep(500 * time.Millisecond)
	compose(t, "pause", "postgres")
	time.Sleep(3 * time.Second)
	compose(t, "unpause", "postgres")
	<-done
	tr.assertConverges(t)
}

// kill -9 of every worker mid-flight: consumers die without acking; replacements must finish the work.
func TestChaos_WorkersKilledMidFlight(t *testing.T) {
	tr := newTraffic(t, 4)
	done := make(chan struct{})
	go func() { defer close(done); tr.run(60, 30*time.Millisecond, 0) }()
	for i := 0; i < 3; i++ {
		time.Sleep(500 * time.Millisecond)
		tr.e.Stop() // abrupt: contexts cancelled while handlers run
		tr.e.StartWorkers(3)
		tr.e.StartOutbox()
	}
	<-done
	tr.assertConverges(t)
}
