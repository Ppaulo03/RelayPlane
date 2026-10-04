package contracttest

import (
	"context"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// The tombstone of an erased contact: it answers one question, "was this message accepted before its sender was erased?".
func erasureTombstoneContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	now := time.Now().UTC()
	ev := func(tenant, from string, accepted time.Time) events.Event {
		a := accepted
		return events.Event{EventID: "e", EventType: events.MessageReceived, TenantID: tenant, AcceptedAt: &a, Payload: map[string]any{"from": from}}
	}
	if gone, err := fx.r.Erasures.Erased(ctx, ev("t1", "5562988887777", now)); err != nil || gone {
		t.Fatalf("nobody was erased: %v %v", gone, err)
	}
	if err := fx.r.Erasures.Mark(ctx, "t1", "5562988887777", now); err != nil {
		t.Fatal(err)
	}
	if gone, _ := fx.r.Erasures.Erased(ctx, ev("t1", "5562988887777", now.Add(-time.Second))); !gone {
		t.Error("a message accepted before the erasure is erased")
	}
	if gone, _ := fx.r.Erasures.Erased(ctx, ev("t1", "5562988887777", now)); !gone {
		t.Error("accepted in the very moment of the erasure counts as before it")
	}
	if gone, _ := fx.r.Erasures.Erased(ctx, ev("t1", "5562988887777", now.Add(time.Second))); gone {
		t.Error("a message that arrives AFTER the erasure is new data")
	}
	if gone, _ := fx.r.Erasures.Erased(ctx, ev("t2", "5562988887777", now.Add(-time.Second))); gone {
		t.Error("another tenant's contact is not erased")
	}
	if gone, _ := fx.r.Erasures.Erased(ctx, ev("t1", "5562900000000", now.Add(-time.Second))); gone {
		t.Error("another person is not erased")
	}
	// events that are not about a contact, or carry no acceptance time, are never erased
	status := events.Event{EventType: events.MessageStatus, TenantID: "t1", Payload: map[string]any{"from": "5562988887777"}}
	if gone, _ := fx.r.Erasures.Erased(ctx, status); gone {
		t.Error("only inbound messages are about a contact")
	}
	noTime := ev("t1", "5562988887777", now)
	noTime.AcceptedAt = nil
	if gone, _ := fx.r.Erasures.Erased(ctx, noTime); gone {
		t.Error("without an acceptance time nothing can be said")
	}
	// the latest mark wins; an older one never moves it back
	if err := fx.r.Erasures.Mark(ctx, "t1", "5562988887777", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if gone, _ := fx.r.Erasures.Erased(ctx, ev("t1", "5562988887777", now.Add(-time.Second))); !gone {
		t.Error("an older mark must not weaken a newer one")
	}
	if n, err := fx.r.Erasures.Purge(ctx, now.Add(time.Minute)); err != nil || n != 1 {
		t.Errorf("purge: %d %v", n, err)
	}
	if gone, _ := fx.r.Erasures.Erased(ctx, ev("t1", "5562988887777", now.Add(-time.Second))); gone {
		t.Error("a purged tombstone no longer erases")
	}
}
