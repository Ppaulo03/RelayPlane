package contracttest

import (
	"context"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

var erasureKey = []byte("contract-test-erasure-key")

// The tombstone of an erased contact: it answers one question, "was this message accepted before its author was erased?". Keyed by an
// HMAC of the number, kept for good.
func erasureTombstoneContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	now := time.Now().UTC()
	ev := func(typ events.Type, tenant, from string, accepted time.Time) events.Event {
		a := accepted
		return events.Event{EventID: "e", EventType: typ, TenantID: tenant, AcceptedAt: &a, Payload: map[string]any{"from": from}}
	}
	erased := func(e events.Event) bool {
		t.Helper()
		gone, err := ports.ErasedEvent(ctx, fx.r.Erasures, erasureKey, e)
		if err != nil {
			t.Fatal(err)
		}
		return gone
	}
	const person = "5562988887777"
	if erased(ev(events.MessageReceived, "t1", person, now)) {
		t.Fatal("nobody was erased")
	}
	if err := fx.r.Erasures.Mark(ctx, "t1", events.ErasureSubject(erasureKey, person), now); err != nil {
		t.Fatal(err)
	}
	if !erased(ev(events.MessageReceived, "t1", person, now.Add(-time.Second))) {
		t.Error("a message accepted before the erasure is erased")
	}
	if !erased(ev(events.MessageReceived, "t1", person, now)) {
		t.Error("accepted in the very moment of the erasure counts as before it")
	}
	if erased(ev(events.MessageReceived, "t1", person, now.Add(time.Second))) {
		t.Error("a message that arrives AFTER the erasure is new data")
	}
	// the author of a deletion notice is a subject too: its payload carries the number
	if !erased(ev(events.MessageDeleted, "t1", person, now.Add(-time.Second))) {
		t.Error("a message.deleted of the erased contact must not come back either")
	}
	if erased(ev(events.MessageReceived, "t2", person, now.Add(-time.Second))) {
		t.Error("another tenant's contact is not erased")
	}
	if erased(ev(events.MessageReceived, "t1", "5562900000000", now.Add(-time.Second))) {
		t.Error("another person is not erased")
	}
	// the key matters: another key never sees these tombstones, and the stored subject is not a plain hash of the number
	if gone, _ := ports.ErasedEvent(ctx, fx.r.Erasures, []byte("another key"), ev(events.MessageReceived, "t1", person, now.Add(-time.Second))); gone {
		t.Error("tombstones are keyed: another key does not match them")
	}
	// events that are not about a contact, or carry no acceptance time, are never erased
	status := events.Event{EventType: events.MessageStatus, TenantID: "t1", Payload: map[string]any{"from": person}}
	if erased(status) {
		t.Error("only inbound messages and their deletion notices are about a contact")
	}
	noTime := ev(events.MessageReceived, "t1", person, now)
	noTime.AcceptedAt = nil
	if erased(noTime) {
		t.Error("without an acceptance time nothing can be said")
	}
	// the latest mark wins; an older one never moves it back
	if err := fx.r.Erasures.Mark(ctx, "t1", events.ErasureSubject(erasureKey, person), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !erased(ev(events.MessageReceived, "t1", person, now.Add(-time.Second))) {
		t.Error("an older mark must not weaken a newer one")
	}
}

// Retention of pending deliveries leaves alone the one a dispatcher holds: it would POST a copy of a row that no longer exists.
func purgePendingRespectsLeaseContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	now := time.Now().UTC()
	if err := fx.r.Subscriptions.Create(ctx, subscription.Subscription{ID: "sub_1", TenantID: "t1", URL: "https://a.example.com/h", SecretVersion: 1, Active: true, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	ev := events.Event{EventID: "evt_p", EventType: events.MessageReceived, TenantID: "t1", InstanceID: "inst_1", Timestamp: now,
		Payload: map[string]any{"from": "5562"}}
	if _, err := fx.r.Deliveries.Enqueue(ctx, []subscription.Delivery{{ID: "dlv_p", SubscriptionID: "sub_1", TenantID: "t1", InstanceID: "inst_1", EventID: "evt_p",
		EventType: events.MessageReceived, Event: ev, Status: subscription.DeliveryPending, CreatedAt: now.Add(-48 * time.Hour), NextAttemptAt: now.Add(-48 * time.Hour)}}); err != nil {
		t.Fatal(err)
	}
	got, err := fx.r.Deliveries.ClaimDue(ctx, time.Now(), 300*time.Millisecond, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	if n, err := fx.r.Deliveries.PurgePending(ctx, now, time.Now()); err != nil || n != 0 {
		t.Fatalf("a delivery a dispatcher holds is not purged: %d %v", n, err)
	}
	time.Sleep(400 * time.Millisecond) // the lease runs out: the dispatcher died
	if n, err := fx.r.Deliveries.PurgePending(ctx, now, time.Now()); err != nil || n != 1 {
		t.Fatalf("once nobody holds it, retention takes it: %d %v", n, err)
	}
}
