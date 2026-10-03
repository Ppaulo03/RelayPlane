package systemtest

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/ports"
)

func sentTexts(e *Env) []string {
	var out []string
	for _, s := range e.Provider.Sent() {
		out = append(out, s.Message.Text)
	}
	return out
}

// 9.7 The review's scenario: A is accepted but never published (crash/broker down),
// B is accepted afterwards. Recovery must deliver A then B, never B then A.
func TestOutbox_AcceptedButUnpublishedMessageIsNeverOvertaken(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.QueueFault.Down.Store(true)
	a, _, err := e.SendText(e.Tenant, inst.ID, "A", "")
	if err != nil {
		t.Fatalf("accepting must not depend on the broker: %v", err)
	}
	if d, _ := e.Queue.Depth(bg); d != 0 {
		t.Fatalf("nothing could be published yet, depth %d", d)
	}
	e.QueueFault.Down.Store(false) // broker is back; B is accepted and eagerly dispatched
	b, _, err := e.SendText(e.Tenant, inst.ID, "B", "")
	if err != nil {
		t.Fatal(err)
	}
	e.StartWorkers(3)
	e.WaitMessage(a.MessageID, messaging.StatusAccepted)
	e.WaitMessage(b.MessageID, messaging.StatusAccepted)
	if got := fmt.Sprint(sentTexts(e)); got != "[A B]" {
		t.Fatalf("INV-07: delivery order %s, want [A B]", got)
	}
	ma, _ := e.Repos.Messages.Get(bg, a.MessageID)
	mb, _ := e.Repos.Messages.Get(bg, b.MessageID)
	if ma.SequenceNo != 1 || mb.SequenceNo != 2 {
		t.Fatalf("sequences %d %d", ma.SequenceNo, mb.SequenceNo)
	}
}

// The outbox dispatcher (reconciler loop) publishes a backlog in order after an outage.
func TestOutbox_DispatcherDrainsBacklogInOrder(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.QueueFault.Down.Store(true)
	var ids []string
	for i := 0; i < 10; i++ {
		r, _, err := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%02d", i), "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.MessageID)
	}
	e.QueueFault.Down.Store(false)
	if n, err := e.App.Outbox.DispatchPending(bg, 100); err != nil || n != 10 {
		t.Fatalf("dispatch: %d %v", n, err)
	}
	if n, _ := e.App.Outbox.DispatchPending(bg, 100); n != 0 {
		t.Fatalf("published entries must not be published again: %d", n)
	}
	e.StartWorkers(2)
	for _, id := range ids {
		e.WaitMessage(id, messaging.StatusAccepted)
	}
	for i, s := range sentTexts(e) {
		if s != fmt.Sprintf("m%02d", i) {
			t.Fatalf("order: %v", sentTexts(e))
		}
	}
}

// The broker loses a command that the outbox already marked as published: the later
// message must wait for it (barrier), and the dispatcher re-publishes the lost one.
func TestOutbox_LostCommandIsRepublishedAndLaterMessagesWait(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.QueueFault.Drop.Store(true)
	a, _, _ := e.SendText(e.Tenant, inst.ID, "A", "") // "published" into the void
	e.QueueFault.Drop.Store(false)
	b, _, _ := e.SendText(e.Tenant, inst.ID, "B", "")
	e.StartOutbox() // the reconciler's outbox loop re-publishes whatever the barrier hands back
	e.StartWorkers(2)
	time.Sleep(200 * time.Millisecond)
	if len(e.Provider.Sent()) != 0 {
		t.Fatalf("B must not overtake the lost A: %v", sentTexts(e))
	}
	if testutilCounter(e, "relayplane_outbound_barrier_deferrals_total") < 1 {
		t.Error("the barrier deferral must be visible in metrics")
	}
	// (with after=0 B counts as "stuck" too: its duplicate is parked behind the barrier, harmlessly)
	if n := redispatchSoon(t, e); n < 1 {
		t.Fatalf("redispatch: %d", n)
	}
	e.WaitMessage(a.MessageID, messaging.StatusAccepted)
	e.WaitMessage(b.MessageID, messaging.StatusAccepted)
	if got := fmt.Sprint(sentTexts(e)); got != "[A B]" {
		t.Fatalf("order %s", got)
	}
}

// 9.8 UNKNOWN is an ordering barrier.
func TestBarrier_UnknownBlocksLaterMessagesUntilResolved(t *testing.T) {
	e := NewEnv(t)
	e.Worker.UnknownBarrierTimeout = 0 // strict: hold until resolved
	inst := e.CreateInstance(e.Tenant, "a", true)
	other := e.CreateInstance(e.Tenant, "other", true)
	e.Provider.FailNext(memory.FailAmbiguous) // consumed by A's send: it is the only one in flight
	a, _, _ := e.SendText(e.Tenant, inst.ID, "A", "")
	e.StartWorkers(2)
	e.WaitMessage(a.MessageID, messaging.StatusUnknown)
	b, _, _ := e.SendText(e.Tenant, inst.ID, "B", "")
	o, _, _ := e.SendText(e.Tenant, other.ID, "OTHER", "")
	e.WaitMessage(o.MessageID, messaging.StatusAccepted) // other instances are unaffected
	time.Sleep(300 * time.Millisecond)
	if m, _ := e.Repos.Messages.Get(bg, b.MessageID); m.Status != messaging.StatusQueued {
		t.Fatalf("B must wait behind the UNKNOWN A, is %s", m.Status)
	}
	for _, s := range sentTexts(e) {
		if s == "B" {
			t.Fatal("B was sent while A's outcome is unknown")
		}
	}

	// tenants cannot settle each other's messages, and only UNKNOWN can be settled
	if _, err := e.App.Messages.Resolve(bg, e.Tenant2, a.MessageID, app.OutcomeNotSent); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("cross-tenant resolve: %v", err)
	}
	if _, err := e.App.Messages.Resolve(bg, e.Tenant, o.MessageID, app.OutcomeSent); !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("resolving a non-UNKNOWN message: %v", err)
	}
	if _, err := e.App.Messages.Resolve(bg, e.Tenant, a.MessageID, "maybe"); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Fatalf("bad outcome: %v", err)
	}

	got, err := e.App.Messages.Resolve(bg, e.Tenant, a.MessageID, app.OutcomeNotSent)
	if err != nil || got.Status != messaging.StatusFailed {
		t.Fatalf("resolve: %+v %v", got, err)
	}
	e.WaitMessage(b.MessageID, messaging.StatusAccepted)
}

func TestBarrier_ResolvedAsSentAlsoReleasesTheQueue(t *testing.T) {
	e := NewEnv(t)
	e.Worker.UnknownBarrierTimeout = 0
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.FailNext(memory.FailAmbiguous)
	a, _, _ := e.SendText(e.Tenant, inst.ID, "A", "")
	b, _, _ := e.SendText(e.Tenant, inst.ID, "B", "")
	e.StartWorkers(1)
	e.WaitMessage(a.MessageID, messaging.StatusUnknown)
	if m, err := e.App.Messages.Resolve(bg, e.Tenant, a.MessageID, app.OutcomeSent); err != nil || m.Status != messaging.StatusAccepted {
		t.Fatalf("%+v %v", m, err)
	}
	e.WaitMessage(b.MessageID, messaging.StatusAccepted)
}

// With a configured timeout an unresolved UNKNOWN stops blocking after a while
// (availability over strict ordering, an explicit and observable choice).
func TestBarrier_UnknownTimeoutReleasesTheQueue(t *testing.T) {
	e := NewEnv(t)
	e.Worker.UnknownBarrierTimeout = 250 * time.Millisecond
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.FailNext(memory.FailAmbiguous)
	a, _, _ := e.SendText(e.Tenant, inst.ID, "A", "")
	b, _, _ := e.SendText(e.Tenant, inst.ID, "B", "")
	e.StartWorkers(1)
	e.WaitMessage(a.MessageID, messaging.StatusUnknown)
	start := time.Now()
	e.WaitMessage(b.MessageID, messaging.StatusAccepted)
	if time.Since(start) < 150*time.Millisecond {
		t.Errorf("B was released after only %v, before the barrier timeout", time.Since(start))
	}
	if m, _ := e.Repos.Messages.Get(bg, a.MessageID); m.Status != messaging.StatusUnknown {
		t.Errorf("the timeout must not rewrite A's status: %s", m.Status)
	}
}

// A message that definitively failed does not block its successors.
func TestBarrier_FailedPredecessorDoesNotBlock(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Provider.FailNext(memory.FailAuth)
	a, _, _ := e.SendText(e.Tenant, inst.ID, "A", "")
	b, _, _ := e.SendText(e.Tenant, inst.ID, "B", "")
	e.StartWorkers(1)
	e.WaitMessage(a.MessageID, messaging.StatusFailed)
	e.WaitMessage(b.MessageID, messaging.StatusAccepted)
}

// ---- recovery durability (second review) ----

// 15.1 A was claimed (DISPATCHING) and its command vanished from the broker. The outbox
// must recover it: the redelivery turns it into UNKNOWN (never a blind resend), which then
// holds back the successor until it is resolved.
func TestRecovery_DispatchingMessageWhoseCommandWasLostIsRecovered(t *testing.T) {
	e := NewEnv(t)
	e.Worker.UnknownBarrierTimeout = 0
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.QueueFault.Drop.Store(true)
	a, _, _ := e.SendText(e.Tenant, inst.ID, "A", "") // its command is lost
	e.QueueFault.Drop.Store(false)
	if _, err := e.Repos.Messages.Transition(bg, a.MessageID, []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{BumpAttempt: true}); err != nil {
		t.Fatal(err) // a worker had claimed it before dying together with the command
	}
	b, _, _ := e.SendText(e.Tenant, inst.ID, "B", "")
	e.StartOutbox()
	e.StartWorkers(2)
	time.Sleep(150 * time.Millisecond)
	if len(e.Provider.Sent()) != 0 {
		t.Fatalf("B must not overtake the stuck A: %v", sentTexts(e))
	}
	// The DISPATCHING age is stamped by the database clock and compared with the application's, so A only becomes "stuck"
	// once any skew has elapsed: keep running the recovery pass (what the reconciler does every few seconds) until A is
	// recovered, instead of assuming the first pass sees it.
	var recovered int
	Eventually(t, 10*time.Second, "the outbox recovers the DISPATCHING message", func() bool {
		n, err := e.App.Outbox.Redispatch(bg, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		recovered += n
		m, _ := e.Repos.Messages.Get(bg, a.MessageID)
		return m != nil && m.Status == messaging.StatusUnknown
	})
	if recovered < 1 {
		t.Fatalf("a DISPATCHING message must be recovered by the outbox: %d", recovered)
	}
	got := e.WaitMessage(a.MessageID, messaging.StatusUnknown)
	if got.ErrorCode != "WORKER_CRASH" {
		t.Errorf("code %q", got.ErrorCode)
	}
	for _, s := range sentTexts(e) {
		if s == "A" {
			t.Fatal("recovery must never resend the interrupted message")
		}
	}
	time.Sleep(150 * time.Millisecond)
	if m, _ := e.Repos.Messages.Get(bg, b.MessageID); m.Status != messaging.StatusQueued {
		t.Fatalf("strict ordering: B waits for A to be resolved, is %s", m.Status)
	}
	if _, err := e.App.Messages.Resolve(bg, e.Tenant, a.MessageID, app.OutcomeNotSent); err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(b.MessageID, messaging.StatusAccepted)
}

// 15.2 The broker is down while old outbox entries exist: maintenance must not purge the
// only copy of a command that still needs to be (re)published.
func TestRecovery_PurgeNeverDeletesEntriesOfRecoverableMessages(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.QueueFault.Drop.Store(true)
	a, _, _ := e.SendText(e.Tenant, inst.ID, "A", "") // "published" into the void: entry dispatched, message QUEUED
	e.QueueFault.Drop.Store(false)
	e.QueueFault.Down.Store(true)

	e.Reconciler.Cfg.StuckQueuedAfter = 0
	e.Reconciler.Maintenance(bg) // redispatch fails (broker down): the purge must be skipped
	if n, err := e.App.Outbox.Purge(bg, 0); err != nil || n != 0 {
		t.Fatalf("a QUEUED message's outbox entry must survive an aggressive purge: %d %v", n, err)
	}
	if es, _ := e.Repos.Messages.ListStuckOutbox(bg, time.Now().Add(time.Hour), 10); len(es) != 1 || es[0].MessageID != a.MessageID {
		t.Fatalf("the entry needed for recovery is gone: %+v", es)
	}
	e.QueueFault.Down.Store(false)
	time.Sleep(25 * time.Millisecond) // the Windows clock ticks coarsely: let "dispatched_at < now" hold
	if n := redispatchSoon(t, e); n != 1 {
		t.Fatalf("redispatch: %d", n)
	}
	e.StartWorkers(1)
	e.WaitMessage(a.MessageID, messaging.StatusAccepted)
	// once the message is finished the entry may be purged
	if n, err := e.App.Outbox.Purge(bg, 0); err != nil || n != 1 {
		t.Fatalf("purge after completion: %d %v", n, err)
	}
}

// redispatchSoon runs Redispatch(0) until it republishes something. Outbox entry ages are stamped by one clock (the
// database's, or the application's) and compared with another, so on a container whose clock lags the host's (Docker
// Desktop on Windows/macOS) an entry only becomes "old enough" once that skew has elapsed. Polling makes the tests
// independent of it without weakening what they check; production waits minutes, which dwarfs any skew.
func redispatchSoon(t *testing.T, e *Env) int {
	t.Helper()
	n := 0
	Eventually(t, 5*time.Second, "the outbox republishes the stuck command", func() bool {
		var err error
		if n, err = e.App.Outbox.Redispatch(bg, 0, 100); err != nil {
			t.Fatal(err)
		}
		return n > 0
	})
	return n
}
