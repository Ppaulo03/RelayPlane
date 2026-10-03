package contracttest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

func outboundStatusPayload(t *testing.T, ev events.Event) events.MessageOutboundStatusPayload {
	t.Helper()
	raw, err := json.Marshal(ev.Payload)
	if err != nil {
		t.Fatal(err)
	}
	var pl events.MessageOutboundStatusPayload
	if err := json.Unmarshal(raw, &pl); err != nil {
		t.Fatal(err)
	}
	return pl
}

// A message status change and its tenant-facing event are one atomic fact.
func eventOutboxContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	m := messaging.Message{ID: "msg_1", TenantID: "t1", InstanceID: "inst_1", NodeID: "node-01", AssignmentEpoch: 1,
		PartitionKey: "inst_1", Recipient: "5562", Type: messaging.TypeText, Payload: json.RawMessage(`{"text":"hi"}`), Status: messaging.StatusQueued,
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	if err := fx.r.Messages.Create(ctx, m); err != nil {
		t.Fatal(err)
	}
	if got, _ := fx.r.Messages.Get(ctx, "msg_1"); got.TraceParent != m.TraceParent {
		t.Errorf("the trace context of the send is persisted with the message: %q", got.TraceParent)
	}
	if evs, _ := fx.r.Events.ListUnpublished(ctx, 10); len(evs) != 0 {
		t.Fatalf("QUEUED/DISPATCHING are internal: nothing is announced yet, got %d", len(evs))
	}
	if _, err := fx.r.Messages.Transition(ctx, "msg_1", []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{BumpAttempt: true}); err != nil {
		t.Fatal(err)
	}
	if evs, _ := fx.r.Events.ListUnpublished(ctx, 10); len(evs) != 0 {
		t.Fatalf("DISPATCHING is internal, got %d events", len(evs))
	}

	acc, err := fx.r.Messages.Transition(ctx, "msg_1", []messaging.Status{messaging.StatusDispatching}, messaging.StatusAccepted, ports.MessagePatch{ProviderMessageID: "WAID1"})
	if err != nil {
		t.Fatal(err)
	}
	if acc.AcceptedAt.IsZero() {
		t.Error("ACCEPTED must stamp accepted_at")
	}
	evs, err := fx.r.Events.ListUnpublished(ctx, 10)
	if err != nil || len(evs) != 1 {
		t.Fatalf("ACCEPTED must produce exactly one event: %d %v", len(evs), err)
	}
	e := evs[0]
	if e.TraceParent != m.TraceParent {
		t.Errorf("the status event carries the trace of the send: %q", e.TraceParent)
	}
	pl := outboundStatusPayload(t, e)
	if e.EventType != events.MessageOutboundStatus || e.TenantID != "t1" || e.InstanceID != "inst_1" || e.EventID == "" {
		t.Errorf("envelope: %+v", e)
	}
	if pl.MessageID != "msg_1" || pl.Status != "ACCEPTED" || pl.ProviderMessageID != "WAID1" || pl.AcceptedAt == nil {
		t.Errorf("payload: %+v", pl)
	}
	if e.SourceAssignment == nil || e.SourceAssignment.NodeID != "node-01" || e.SourceAssignment.Epoch != 1 {
		t.Errorf("the event carries the assignment that produced it: %+v", e.SourceAssignment)
	}

	// a delivery receipt is a new fact (DELIVERED), a regression is not
	if applied, err := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "WAID1", messaging.StatusDelivered); err != nil || !applied {
		t.Fatalf("delivered: %v %v", applied, err)
	}
	if applied, _ := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "WAID1", messaging.StatusAccepted); applied {
		t.Fatal("a regression must not apply")
	}
	evs, _ = fx.r.Events.ListUnpublished(ctx, 10)
	if len(evs) != 2 || outboundStatusPayload(t, evs[1]).Status != "DELIVERED" {
		t.Fatalf("expected ACCEPTED then DELIVERED, got %d events", len(evs))
	}
	if evs[0].EventID == evs[1].EventID {
		t.Error("each status is its own fact with its own id")
	}

	// publication bookkeeping
	if err := fx.r.Events.MarkPublished(ctx, []string{evs[0].EventID}, time.Now()); err != nil {
		t.Fatal(err)
	}
	left, _ := fx.r.Events.ListUnpublished(ctx, 10)
	if len(left) != 1 || left[0].EventID != evs[1].EventID {
		t.Fatalf("published events leave the unpublished list, order preserved: %+v", left)
	}
	if n, err := fx.r.Events.Purge(ctx, time.Now().Add(-time.Hour)); err != nil || n != 0 {
		t.Errorf("recent published events are kept: %d %v", n, err)
	}
	if n, err := fx.r.Events.Purge(ctx, time.Now().Add(time.Hour)); err != nil || n != 1 {
		t.Errorf("old published events are purged, unpublished ones never: %d %v", n, err)
	}
	if left, _ := fx.r.Events.ListUnpublished(ctx, 10); len(left) != 1 {
		t.Errorf("an unpublished event must survive purge: %d", len(left))
	}
}

// FAILED and UNKNOWN are announced too (the consumer must stop waiting for them).
func eventOutboxFailureStatesContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	for i, to := range []messaging.Status{messaging.StatusFailed, messaging.StatusUnknown} {
		id := fmt.Sprintf("msg_%d", i)
		if err := fx.r.Messages.Create(ctx, messaging.Message{ID: id, TenantID: "t1", InstanceID: "inst_1", NodeID: "node-01", AssignmentEpoch: 1,
			PartitionKey: "inst_1", Recipient: "5562", Type: messaging.TypeText, Payload: json.RawMessage(`{}`), Status: messaging.StatusQueued}); err != nil {
			t.Fatal(err)
		}
		if _, err := fx.r.Messages.Transition(ctx, id, []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{}); err != nil {
			t.Fatal(err)
		}
		if _, err := fx.r.Messages.Transition(ctx, id, []messaging.Status{messaging.StatusDispatching}, to, ports.MessagePatch{ErrorCode: "X"}); err != nil {
			t.Fatal(err)
		}
	}
	evs, _ := fx.r.Events.ListUnpublished(ctx, 10)
	if len(evs) != 2 || outboundStatusPayload(t, evs[0]).Status != "FAILED" || outboundStatusPayload(t, evs[1]).Status != "UNKNOWN" {
		t.Fatalf("FAILED and UNKNOWN are announced: %+v", evs)
	}
	if outboundStatusPayload(t, evs[0]).ErrorCode != "X" {
		t.Error("the error code is part of the fact")
	}
}

func newSub(id, tenant string) subscription.Subscription {
	return subscription.Subscription{ID: id, TenantID: tenant, URL: "https://agent.example.com/hook", SecretVersion: 1, Active: true, CreatedAt: time.Now().UTC()}
}

func subscriptionsContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.tenant(t, "t2")
	s := newSub("sub_1", "t1")
	s.EventTypes = []events.Type{events.MessageReceived, events.MessageOutboundStatus}
	s.InstanceIDs = []string{"inst_1"}
	s.ExcludeGroups = true
	if err := fx.r.Subscriptions.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := fx.r.Subscriptions.Create(ctx, s); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Errorf("duplicate id: %v", err)
	}
	got, err := fx.r.Subscriptions.Get(ctx, "t1", "sub_1")
	if err != nil || got.URL != s.URL || len(got.EventTypes) != 2 || got.EventTypes[0] != events.MessageReceived || len(got.InstanceIDs) != 1 || !got.Active || got.SecretVersion != 1 || !got.ExcludeGroups {
		t.Fatalf("roundtrip: %+v %v", got, err)
	}
	// tenant isolation: someone else's subscription does not exist for you
	if _, err := fx.r.Subscriptions.Get(ctx, "t2", "sub_1"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("TENANT ISOLATION on Get: %v", err)
	}
	if _, err := fx.r.Subscriptions.RotateSecret(ctx, "t2", "sub_1", time.Now()); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("TENANT ISOLATION on RotateSecret: %v", err)
	}
	if err := fx.r.Subscriptions.Delete(ctx, "t2", "sub_1"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("TENANT ISOLATION on Delete: %v", err)
	}
	if l, _ := fx.r.Subscriptions.ListByTenant(ctx, "t2"); len(l) != 0 {
		t.Errorf("TENANT ISOLATION on List: %+v", l)
	}
	if err := fx.r.Subscriptions.Create(ctx, newSub("sub_x", "ghost")); err == nil {
		t.Error("a subscription needs an existing tenant")
	}

	v, err := fx.r.Subscriptions.RotateSecret(ctx, "t1", "sub_1", time.Now())
	if err != nil || v != 2 {
		t.Fatalf("rotate: %d %v", v, err)
	}
	if got, _ = fx.r.Subscriptions.GetByID(ctx, "sub_1"); got.SecretVersion != 2 || got.RotatedAt.IsZero() {
		t.Errorf("rotation is persisted: %+v", got)
	}

	if err := fx.r.Subscriptions.Create(ctx, newSub("sub_2", "t1")); err != nil {
		t.Fatal(err)
	}
	if n, _ := fx.r.Subscriptions.CountByTenant(ctx, "t1"); n != 2 {
		t.Errorf("count %d", n)
	}
	if l, _ := fx.r.Subscriptions.ListActive(ctx, "t1"); len(l) != 2 {
		t.Errorf("active %d", len(l))
	}
	if err := fx.r.Subscriptions.Delete(ctx, "t1", "sub_2"); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.r.Subscriptions.GetByID(ctx, "sub_2"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("deleted: %v", err)
	}
}

func newDelivery(id, sub, tenant, inst, eventID string, at time.Time) subscription.Delivery {
	return subscription.Delivery{ID: id, SubscriptionID: sub, TenantID: tenant, InstanceID: inst, EventID: eventID, EventType: events.MessageReceived,
		Event:  events.Event{EventID: eventID, EventType: events.MessageReceived, TenantID: tenant, InstanceID: inst, Timestamp: at, Payload: map[string]any{"text": "hi"}},
		Status: subscription.DeliveryPending, CreatedAt: at, NextAttemptAt: at}
}

func deliveriesContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.tenant(t, "t2")
	for _, s := range []subscription.Subscription{newSub("sub_1", "t1"), newSub("sub_2", "t1")} {
		if err := fx.r.Subscriptions.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC().Truncate(time.Millisecond)

	n, err := fx.r.Deliveries.Enqueue(ctx, []subscription.Delivery{
		newDelivery("d1", "sub_1", "t1", "inst_1", "evt_1", now),
		newDelivery("d2", "sub_1", "t1", "inst_1", "evt_2", now.Add(time.Millisecond)),
		newDelivery("d3", "sub_1", "t1", "inst_2", "evt_3", now.Add(2*time.Millisecond)),
		newDelivery("d4", "sub_2", "t1", "inst_1", "evt_1", now.Add(3*time.Millisecond)),
	})
	if err != nil || n != 4 {
		t.Fatalf("enqueue: %d %v", n, err)
	}
	// re-consuming the same event from the bus must not create another delivery
	if n, err := fx.r.Deliveries.Enqueue(ctx, []subscription.Delivery{newDelivery("d1b", "sub_1", "t1", "inst_1", "evt_1", now)}); err != nil || n != 0 {
		t.Fatalf("ENQUEUE IS IDEMPOTENT per (subscription, event): %d %v", n, err)
	}

	claimed, err := fx.r.Deliveries.ClaimDue(ctx, now.Add(time.Second), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, d := range claimed {
		ids[d.ID] = true
	}
	// per (subscription, instance) only ONE delivery is in flight: d2 waits behind d1
	if !ids["d1"] || ids["d2"] || !ids["d3"] || !ids["d4"] || len(claimed) != 3 {
		t.Fatalf("best-effort per-instance ordering: claimed %v", ids)
	}
	if again, _ := fx.r.Deliveries.ClaimDue(ctx, now.Add(time.Second), time.Minute, 10); len(again) != 0 {
		t.Fatalf("a leased delivery is not claimed again: %d", len(again))
	}
	var d1 subscription.Delivery
	for _, d := range claimed {
		if d.ID == "d1" {
			d1 = d
		}
	}
	if d1.Event.EventID != "evt_1" || d1.Event.TenantID != "t1" || d1.EventType != events.MessageReceived {
		t.Errorf("the event travels with the delivery: %+v", d1.Event)
	}

	if err := fx.r.Deliveries.MarkDelivered(ctx, "d1", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if next, _ := fx.r.Deliveries.ClaimDue(ctx, now.Add(time.Second), time.Minute, 10); len(next) != 1 || next[0].ID != "d2" {
		t.Fatalf("after d1 is done, d2 (same instance) is released: %+v", next)
	}

	// failure: attempts count, retry delays, lease cleared
	if err := fx.r.Deliveries.MarkRetry(ctx, "d3", now.Add(time.Minute), "http 500"); err != nil {
		t.Fatal(err)
	}
	list, _ := fx.r.Deliveries.List(ctx, "t1", "sub_1", subscription.DeliveryPending, 10)
	var d3 subscription.Delivery
	for _, d := range list {
		if d.ID == "d3" {
			d3 = d
		}
	}
	if d3.Attempts != 1 || d3.LastError != "http 500" || !d3.NextAttemptAt.After(now.Add(30*time.Second)) {
		t.Errorf("retry bookkeeping: %+v", d3)
	}
	if early, _ := fx.r.Deliveries.ClaimDue(ctx, now.Add(2*time.Second), time.Minute, 10); containsID(early, "d3") {
		t.Error("a delivery is not claimed before its next_attempt_at")
	}
	if due, _ := fx.r.Deliveries.ClaimDue(ctx, now.Add(2*time.Minute), time.Minute, 10); !containsID(due, "d3") {
		t.Error("a delivery is claimed once its time has come")
	}

	// circuit open: postponing does NOT consume the retry budget
	if err := fx.r.Deliveries.Postpone(ctx, "d3", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	list, _ = fx.r.Deliveries.List(ctx, "t1", "sub_1", subscription.DeliveryPending, 10)
	for _, d := range list {
		if d.ID == "d3" && d.Attempts != 1 {
			t.Errorf("POSTPONE must not count an attempt: %+v", d)
		}
	}

	// DLQ and redelivery
	if err := fx.r.Deliveries.MarkDead(ctx, "d3", "gave up"); err != nil {
		t.Fatal(err)
	}
	dead, _ := fx.r.Deliveries.List(ctx, "t1", "sub_1", subscription.DeliveryDead, 10)
	if len(dead) != 1 || dead[0].ID != "d3" || dead[0].Attempts != 2 || dead[0].LastError != "gave up" {
		t.Fatalf("DLQ: %+v", dead)
	}
	if due, _ := fx.r.Deliveries.ClaimDue(ctx, now.Add(48*time.Hour), time.Minute, 10); containsID(due, "d3") {
		t.Error("DEAD deliveries are never claimed")
	}
	if err := fx.r.Deliveries.Requeue(ctx, "t2", "d3", now); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("TENANT ISOLATION on Requeue: %v", err)
	}
	if err := fx.r.Deliveries.Requeue(ctx, "t1", "d2", now); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("only DEAD deliveries can be requeued: %v", err)
	}
	if err := fx.r.Deliveries.Requeue(ctx, "t1", "d3", now.Add(49*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if due, _ := fx.r.Deliveries.ClaimDue(ctx, now.Add(50*time.Hour), time.Minute, 10); !containsID(due, "d3") {
		t.Error("a requeued delivery is claimed again with a fresh budget")
	}
	if _, err := fx.r.Deliveries.List(ctx, "t2", "sub_1", "", 10); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("TENANT ISOLATION on List: %v", err)
	}

	c, err := fx.r.Deliveries.Counts(ctx, now.Add(time.Hour))
	if err != nil || c.Delivered != 1 || c.Pending < 1 || c.OldestPending <= 0 {
		t.Errorf("counts: %+v %v", c, err)
	}

	// retention and cascade
	if n, _ := fx.r.Deliveries.PurgeDelivered(ctx, now.Add(time.Hour)); n != 1 {
		t.Errorf("delivered deliveries are purged after retention: %d", n)
	}
	if err := fx.r.Subscriptions.Delete(ctx, "t1", "sub_2"); err != nil {
		t.Fatal(err)
	}
	if c, _ := fx.r.Deliveries.Counts(ctx, now); c.Pending+c.Dead != 2 {
		t.Errorf("deleting a subscription drops its deliveries only: %+v", c)
	}
}

func containsID(ds []subscription.Delivery, id string) bool {
	for _, d := range ds {
		if d.ID == id {
			return true
		}
	}
	return false
}
