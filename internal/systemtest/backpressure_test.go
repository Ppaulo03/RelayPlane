package systemtest

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// The consumer's own backpressure: a paused subscription accumulates without losing anything and resumes in order.
func TestBackpressure_PauseAccumulatesAndResumeDeliversInOrder(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subID, _ := subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.StartOutbox()
	e.StartWebhooks()
	if err := e.App.Subscriptions.Pause(bg, e.Tenant, subID); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("WA-%d", i)
		if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, id))); err != nil {
			t.Fatal(err)
		}
	}
	Eventually(t, 10*time.Second, "the events are queued for the paused subscription", func() bool {
		bl, _ := e.App.Subscriptions.Backlog(bg, e.Tenant)
		return bl[subID].Pending == 3
	})
	time.Sleep(200 * time.Millisecond)
	if n := len(e.Receiver.All()); n != 0 {
		t.Fatalf("a paused subscription is sent nothing: %d", n)
	}

	if err := e.App.Subscriptions.Resume(bg, e.Tenant, subID); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "the accumulated events are delivered", func() bool { return len(e.Receiver.Accepted(hookURL)) == 3 })
	var seqs []int64
	for _, r := range e.Receiver.Accepted(hookURL) {
		var env struct {
			Sequence int64 `json:"sequence"`
		}
		if err := json.Unmarshal(r.Body, &env); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, env.Sequence)
	}
	if fmt.Sprint(seqs) != "[1 2 3]" {
		t.Errorf("resumed in sequence order: %v", seqs)
	}
	// the receiver answers before the dispatcher records the delivery: wait for the record
	Eventually(t, 5*time.Second, "nothing is left waiting", func() bool {
		bl, _ := e.App.Subscriptions.Backlog(bg, e.Tenant)
		return bl[subID].Pending == 0
	})
}

// A consumer that answers slowly takes at most its share of the dispatcher: another consumer is still served at once.
func TestBackpressure_ASlowConsumerCannotTakeTheWholeDispatcher(t *testing.T) {
	e := NewEnv(t)
	const slowURL, fastURL = "http://slow.local/hook", "http://fast.local/hook"
	var insts []string
	for i := 0; i < 4; i++ {
		insts = append(insts, e.CreateInstance(e.Tenant, fmt.Sprintf("i%d", i), true).ID)
	}
	subscribe(t, e, e.Tenant, slowURL, string(events.MessageReceived))
	subscribe(t, e, e.Tenant, fastURL, string(events.MessageReceived))
	var mu sync.Mutex
	var fastAt time.Time
	e.Receiver.Behave = func(_ int, r Received) (int, error) {
		if r.URL == slowURL {
			time.Sleep(600 * time.Millisecond) // a consumer that is struggling
			return 200, nil
		}
		mu.Lock()
		if fastAt.IsZero() {
			fastAt = time.Now()
		}
		mu.Unlock()
		return 200, nil
	}
	e.Dispatcher.Concurrency = 4
	e.Dispatcher.MaxInFlightPerSubscription = 2
	e.StartOutbox()
	e.StartWebhooks()

	// four instances give the slow consumer four deliveries it could run in parallel: it may only run two
	start := time.Now()
	for i, id := range insts {
		inst, _ := e.Repos.Instances.Get(bg, id)
		if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(id, fmt.Sprintf("WA-S%d", i)))); err != nil {
			t.Fatal(err)
		}
	}
	Eventually(t, 10*time.Second, "the fast consumer is served", func() bool { mu.Lock(); defer mu.Unlock(); return !fastAt.IsZero() })
	mu.Lock()
	lag := fastAt.Sub(start)
	mu.Unlock()
	if lag > 400*time.Millisecond {
		t.Errorf("the fast consumer waited %v behind a slow one: the slow one took the whole dispatcher", lag)
	}
	Eventually(t, 15*time.Second, "everything is delivered in the end", func() bool {
		return len(e.Receiver.Accepted(slowURL)) == 4 && len(e.Receiver.Accepted(fastURL)) == 4
	})
}
