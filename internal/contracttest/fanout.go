package contracttest

import (
	"context"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
)

// The event outbox carries the tenant fan-out: an event is finished only when its deliveries exist, and only then (and once the broker took
// it) may it be purged. A worker that dies holding a claim leaves the event to another.
func eventFanOutContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	ev := func(id string) events.Event {
		return events.Event{EventID: id, EventType: events.MessageReceived, TenantID: "t1", InstanceID: "inst_1", Timestamp: time.Now().UTC(),
			Payload: map[string]any{"from": "5562"}}
	}
	for _, id := range []string{"e1", "e2", "e3"} {
		if o, err := fx.r.Dedup.Accept(ctx, "k-"+id, time.Hour, ev(id), nil); err != nil || o != 0 {
			t.Fatalf("accept %s: %v %v", id, o, err)
		}
	}
	ids := func(evs []events.Event) []string {
		var out []string
		for _, e := range evs {
			out = append(out, e.EventID)
		}
		return out
	}
	// oldest first, a limit, and an exclusive lease
	a, err := fx.r.Events.ClaimForFanOut(ctx, 2, time.Minute)
	if err != nil || len(a) != 2 || a[0].EventID != "e1" || a[1].EventID != "e2" {
		t.Fatalf("first claim: %v %v", ids(a), err)
	}
	b, _ := fx.r.Events.ClaimForFanOut(ctx, 10, time.Minute)
	if len(b) != 1 || b[0].EventID != "e3" {
		t.Fatalf("a claimed event is not handed out again while its lease holds: %v", ids(b))
	}
	if n, age, err := fx.r.Events.PendingStats(ctx); err != nil || n != 3 || age < 0 {
		t.Errorf("claimed is not finished: %d %v %v", n, age, err)
	}
	// finishing two of them
	if err := fx.r.Events.MarkFannedOut(ctx, []string{"e1", "e2"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := fx.r.Events.PendingStats(ctx); n != 1 {
		t.Errorf("one left to fan out: %d", n)
	}
	// the broker publication is a different fact: a fanned-out event that the broker has not taken yet is not purgeable
	if n, _ := fx.r.Events.Purge(ctx, time.Now().Add(time.Hour)); n != 0 {
		t.Errorf("an event the broker has not taken is kept: purged %d", n)
	}
	if err := fx.r.Events.MarkPublished(ctx, []string{"e1", "e2", "e3"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// ... and an event the broker took but whose deliveries do not exist is kept too (this is the row that saves it from a broker that lost it)
	if n, _ := fx.r.Events.Purge(ctx, time.Now().Add(time.Hour)); n != 2 {
		t.Errorf("only published AND fanned-out events are purged: %d", n)
	}
	// a lease that ran out gives the event to another worker (the first one died)
	time.Sleep(30 * time.Millisecond)
	short, err := fx.r.Events.ClaimForFanOut(ctx, 10, 10*time.Millisecond)
	if err != nil || len(short) != 0 {
		t.Fatalf("e3 is still leased by the earlier claim: %v %v", ids(short), err)
	}
}

// The state change and its event are one fact: the event exists exactly when the state changed.
func observedEmittingContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	now := time.Now()
	ev := func(id string) events.Event {
		return events.Event{EventID: id, EventType: events.InstanceStatusChanged, TenantID: "t1", InstanceID: "inst_1", Timestamp: now.UTC(),
			Payload: map[string]any{"state": "CREATING"}}
	}
	queued := func() int {
		got, err := fx.r.Events.ClaimForFanOut(ctx, 100, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		return len(got)
	}
	if _, err := fx.r.Instances.SetObservedEmitting(ctx, "inst_1", 7, instance.Creating, now, ev("e_stale")); err == nil {
		t.Error("a stale epoch is refused")
	}
	if _, err := fx.r.Instances.SetObservedEmitting(ctx, "inst_1", 1, instance.Connected, now, ev("e_invalid")); err == nil {
		t.Error("an invalid transition is refused")
	}
	if n := queued(); n != 0 {
		t.Fatalf("a refused change queues no event: %d", n)
	}
	if changed, err := fx.r.Instances.SetObservedEmitting(ctx, "inst_1", 1, instance.Creating, now, ev("e_changed")); err != nil || !changed {
		t.Fatalf("a real change: %v %v", changed, err)
	}
	if n := queued(); n != 1 {
		t.Fatalf("the change queues its event: %d", n)
	}
	time.Sleep(5 * time.Millisecond)
	if changed, _ := fx.r.Instances.SetObservedEmitting(ctx, "inst_1", 1, instance.Creating, now, ev("e_same")); changed {
		t.Error("the same state is not a change")
	}
	// e_changed is still unfinished (its lease only just ran out): the unchanged write added nothing
	if got, _ := fx.r.Events.ClaimForFanOut(ctx, 100, time.Minute); len(got) != 1 || got[0].EventID != "e_changed" {
		t.Errorf("an unchanged write queues no event: %v", got)
	}
}
