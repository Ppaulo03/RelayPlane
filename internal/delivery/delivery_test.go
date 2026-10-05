package delivery_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/delivery"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

var bg = context.Background()

var serverKey = []byte("test-server-key")

// fakeSender records requests and answers from a script.
type fakeSender struct {
	mu    sync.Mutex
	reqs  []ports.WebhookRequest
	reply func(n int, req ports.WebhookRequest) (int, error)
}

func (f *fakeSender) Send(_ context.Context, req ports.WebhookRequest) (int, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	n := len(f.reqs)
	f.mu.Unlock()
	if f.reply == nil {
		return 200, nil
	}
	return f.reply(n, req)
}

func (f *fakeSender) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.reqs) }

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

type env struct {
	store  *memory.Store
	repos  ports.Repositories
	fan    *delivery.FanOut
	disp   *delivery.Dispatcher
	sender *fakeSender
	clock  *clock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	st := memory.NewStore()
	clk := &clock{t: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)}
	st.Now = clk.Now
	repos := st.Repositories()
	for _, id := range []string{"t1", "t2"} {
		if err := repos.Tenants.Create(bg, instance.Tenant{ID: id, Name: id, APIKeyHash: "h" + id}); err != nil {
			t.Fatal(err)
		}
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := observability.NewMetrics()
	snd := &fakeSender{}
	return &env{store: st, repos: repos, sender: snd, clock: clk,
		fan: &delivery.FanOut{Repos: repos, Log: log, Now: clk.Now, Metrics: m},
		disp: &delivery.Dispatcher{Repos: repos, Sender: snd, ServerKey: serverKey, Metrics: m, Log: log, Now: clk.Now,
			Rand: func() float64 { return 0 }, Retry: subscription.DefaultRetry(), Breaker: delivery.NewBreaker(3, 10*time.Second, time.Minute)}}
}

func (e *env) sub(t *testing.T, id, tenant, url string) subscription.Subscription {
	t.Helper()
	s := subscription.Subscription{ID: id, TenantID: tenant, URL: url, SecretVersion: 1, Active: true, CreatedAt: e.clock.Now()}
	if err := e.repos.Subscriptions.Create(bg, s); err != nil {
		t.Fatal(err)
	}
	return s
}

func event(id, tenant, inst string) events.Event {
	return events.Event{EventID: id, EventType: events.MessageReceived, Provider: "p", TenantID: tenant, InstanceID: inst, Timestamp: time.Now().UTC(),
		Payload: events.MessageReceivedPayload{ProviderMessageID: "W" + id, From: "5562", Type: "text", Text: "oi", ReplyToProviderMessageID: "OURS"}}
}

func (e *env) deliveries(t *testing.T, tenant, sub string, st subscription.DeliveryStatus) []subscription.Delivery {
	t.Helper()
	l, err := e.repos.Deliveries.List(bg, tenant, sub, st, 100)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestFanOutIsTenantIsolatedFilteredAndIdempotent(t *testing.T) {
	e := newEnv(t)
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	onlyStatus := subscription.Subscription{ID: "sub_b", TenantID: "t1", URL: "https://b.example.com/h", SecretVersion: 1, Active: true, CreatedAt: e.clock.Now(),
		EventTypes: []events.Type{events.MessageOutboundStatus}}
	if err := e.repos.Subscriptions.Create(bg, onlyStatus); err != nil {
		t.Fatal(err)
	}
	e.sub(t, "sub_other", "t2", "https://other.example.com/h")

	ev := event("evt_1", "t1", "inst_1")
	for i := 0; i < 3; i++ { // the bus redelivers (at-least-once)
		if err := e.fan.Handle(bg, ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.deliveries(t, "t1", "sub_a", ""); len(got) != 1 {
		t.Fatalf("one delivery per (subscription, event) however often the bus redelivers: %d", len(got))
	}
	if got := e.deliveries(t, "t1", "sub_b", ""); len(got) != 0 {
		t.Errorf("the event type filter applies: %d", len(got))
	}
	if got := e.deliveries(t, "t2", "sub_other", ""); len(got) != 0 {
		t.Fatalf("TENANT ISOLATION: a t1 event must never be delivered to a t2 subscription: %d", len(got))
	}
	for _, internal := range []events.Event{
		{EventID: "q", EventType: events.InstanceQRCodeUpdated, TenantID: "t1", InstanceID: "i"},
		{EventID: "o", EventType: events.OwnershipViolation, TenantID: "t1", InstanceID: "i"},
		{EventID: "n", EventType: events.MessageReceived, TenantID: "", InstanceID: "i"},
	} {
		if err := e.fan.Handle(bg, internal); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.deliveries(t, "t1", "sub_a", ""); len(got) != 1 {
		t.Errorf("internal events never produce deliveries: %d", len(got))
	}
}

func TestDispatchSendsASignedRequestThatTheConsumerCanVerify(t *testing.T) {
	e := newEnv(t)
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	if err := e.fan.Handle(bg, event("evt_1", "t1", "inst_1")); err != nil {
		t.Fatal(err)
	}
	if n, err := e.disp.RunOnce(bg); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	if e.sender.count() != 1 {
		t.Fatalf("sent %d", e.sender.count())
	}
	req := e.sender.reqs[0]
	if req.URL != "https://a.example.com/h" || req.Headers[subscription.HeaderEventID] != "evt_1" || req.Headers[subscription.HeaderEventType] != "message.received" ||
		req.Headers["Content-Type"] != "application/json" || req.Headers[subscription.HeaderAttempt] != "1" {
		t.Errorf("headers: %+v", req.Headers)
	}
	ts, _ := strconv.ParseInt(req.Headers[subscription.HeaderTimestamp], 10, 64)
	secret := subscription.DeriveSecret(serverKey, "sub_a", 1)
	if err := subscription.Verify([]string{secret}, req.Headers[subscription.HeaderSignature], ts, req.Body, e.clock.Now(), 5*time.Minute); err != nil {
		t.Fatalf("the consumer must be able to verify the signature with the secret it was given: %v", err)
	}
	var body struct {
		EventID string `json:"event_id"`
		Payload struct {
			ReplyTo string `json:"reply_to_provider_message_id"`
			Text    string `json:"text"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.EventID != "evt_1" || body.Payload.ReplyTo != "OURS" || body.Payload.Text != "oi" {
		t.Errorf("the body is the canonical event: %s (%v)", req.Body, err)
	}
	if got := e.deliveries(t, "t1", "sub_a", subscription.DeliveryDelivered); len(got) != 1 {
		t.Errorf("a 2xx marks the delivery DELIVERED: %+v", e.deliveries(t, "t1", "sub_a", ""))
	}
	if n, _ := e.disp.RunOnce(bg); n != 0 || e.sender.count() != 1 {
		t.Error("a delivered event is never sent again")
	}
}

func TestSecretRotationSignsWithBothSecretsDuringTheGrace(t *testing.T) {
	e := newEnv(t)
	s := e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	if _, err := e.repos.Subscriptions.RotateSecret(bg, "t1", s.ID, e.clock.Now()); err != nil {
		t.Fatal(err)
	}
	_ = e.fan.Handle(bg, event("evt_1", "t1", "inst_1"))
	_, _ = e.disp.RunOnce(bg)
	req := e.sender.reqs[0]
	ts, _ := strconv.ParseInt(req.Headers[subscription.HeaderTimestamp], 10, 64)
	oldSecret, newSecret := subscription.DeriveSecret(serverKey, "sub_a", 1), subscription.DeriveSecret(serverKey, "sub_a", 2)
	for _, sec := range []string{oldSecret, newSecret} {
		if err := subscription.Verify([]string{sec}, req.Headers[subscription.HeaderSignature], ts, req.Body, e.clock.Now(), time.Minute); err != nil {
			t.Errorf("within the grace both the old and the new secret verify: %v", err)
		}
	}
	// after the grace only the new secret signs
	e.clock.Advance(subscription.RotationGrace + time.Hour)
	_ = e.fan.Handle(bg, event("evt_2", "t1", "inst_1"))
	_, _ = e.disp.RunOnce(bg)
	req = e.sender.reqs[1]
	ts, _ = strconv.ParseInt(req.Headers[subscription.HeaderTimestamp], 10, 64)
	if err := subscription.Verify([]string{oldSecret}, req.Headers[subscription.HeaderSignature], ts, req.Body, e.clock.Now(), time.Minute); err == nil {
		t.Error("after the grace the old secret must no longer verify")
	}
	if err := subscription.Verify([]string{newSecret}, req.Headers[subscription.HeaderSignature], ts, req.Body, e.clock.Now(), time.Minute); err != nil {
		t.Errorf("the new secret verifies: %v", err)
	}
}

func TestFailuresAreRetriedWithBackoffThenDeadLettered(t *testing.T) {
	e := newEnv(t)
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	e.disp.Breaker = delivery.NewBreaker(1000, time.Second, time.Second) // keep the breaker out of this test
	e.sender.reply = func(int, ports.WebhookRequest) (int, error) { return 500, nil }
	_ = e.fan.Handle(bg, event("evt_1", "t1", "inst_1"))

	schedule := subscription.DefaultRetry().Schedule
	for i := 0; i < len(schedule); i++ {
		if n, _ := e.disp.RunOnce(bg); n != 1 {
			t.Fatalf("attempt %d was not made", i+1)
		}
		d := e.deliveries(t, "t1", "sub_a", subscription.DeliveryPending)
		if len(d) != 1 || d[0].Attempts != i+1 || d[0].LastError != "http 500" {
			t.Fatalf("after attempt %d: %+v", i+1, d)
		}
		if want := e.clock.Now().Add(schedule[i]); !d[0].NextAttemptAt.Equal(want) {
			t.Fatalf("attempt %d: next at %v want %v", i+1, d[0].NextAttemptAt, want)
		}
		if n, _ := e.disp.RunOnce(bg); n != 0 {
			t.Fatalf("a delivery must wait for its backoff (attempt %d)", i+1)
		}
		e.clock.Advance(schedule[i])
	}
	if n, _ := e.disp.RunOnce(bg); n != 1 {
		t.Fatal("the last attempt was not made")
	}
	dead := e.deliveries(t, "t1", "sub_a", subscription.DeliveryDead)
	if len(dead) != 1 || dead[0].Attempts != subscription.DefaultRetry().MaxAttempts() {
		t.Fatalf("after the budget the delivery is in the DLQ: %+v", dead)
	}
	if got := e.sender.count(); got != subscription.DefaultRetry().MaxAttempts() {
		t.Errorf("exactly %d attempts, got %d", subscription.DefaultRetry().MaxAttempts(), got)
	}
	e.clock.Advance(48 * time.Hour)
	if n, _ := e.disp.RunOnce(bg); n != 0 {
		t.Error("DEAD deliveries are not retried")
	}

	// operator redelivers from the DLQ with a fresh budget, and a healthy endpoint gets it
	e.sender.reply = nil
	if err := e.repos.Deliveries.Requeue(bg, "t1", dead[0].ID, e.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if n, _ := e.disp.RunOnce(bg); n != 1 {
		t.Fatal("a requeued delivery is sent")
	}
	if got := e.deliveries(t, "t1", "sub_a", subscription.DeliveryDelivered); len(got) != 1 {
		t.Errorf("redelivery succeeded: %+v", e.deliveries(t, "t1", "sub_a", ""))
	}
}

func TestTransportErrorsAreRetriedButForbiddenDestinationsAreNot(t *testing.T) {
	e := newEnv(t)
	e.sub(t, "sub_net", "t1", "https://flaky.example.com/h")
	e.sub(t, "sub_bad", "t1", "https://evil.example.com/h")
	e.sender.reply = func(_ int, r ports.WebhookRequest) (int, error) {
		if r.URL == "https://evil.example.com/h" {
			return 0, fmt.Errorf("resolve: %w", errs.ErrDestinationBlocked)
		}
		return 0, errors.New("connection reset by peer")
	}
	_ = e.fan.Handle(bg, event("evt_1", "t1", "inst_1"))
	_, _ = e.disp.RunOnce(bg)
	net := e.deliveries(t, "t1", "sub_net", subscription.DeliveryPending)
	if len(net) != 1 || net[0].Attempts != 1 {
		t.Errorf("a transport error is retried: %+v", net)
	}
	bad := e.deliveries(t, "t1", "sub_bad", subscription.DeliveryDead)
	if len(bad) != 1 {
		t.Fatalf("a forbidden destination is dead-lettered at once (retrying cannot help): %+v", e.deliveries(t, "t1", "sub_bad", ""))
	}
}

func TestCircuitBreakerShieldsAFailingDestinationWithoutSpendingTheRetryBudget(t *testing.T) {
	e := newEnv(t) // breaker: opens after 3 consecutive failures, 10s cool-down
	e.sub(t, "sub_a", "t1", "https://down.example.com/h")
	e.sender.reply = func(int, ports.WebhookRequest) (int, error) { return 503, nil }
	for i := 1; i <= 6; i++ {
		_ = e.fan.Handle(bg, event(fmt.Sprintf("evt_%d", i), "t1", fmt.Sprintf("inst_%d", i))) // different instances: all claimable together
	}
	if n, _ := e.disp.RunOnce(bg); n != 6 {
		t.Fatalf("claimed %d", n)
	}
	before := e.sender.count()
	if before > 6 {
		t.Fatalf("sent %d", before)
	}
	// the destination is open now: a new event is postponed, NOT sent and NOT counted as an attempt
	_ = e.fan.Handle(bg, event("evt_late", "t1", "inst_late"))
	if _, err := e.disp.RunOnce(bg); err != nil {
		t.Fatal(err)
	}
	if e.sender.count() != before {
		t.Fatalf("an open circuit must not send: %d -> %d", before, e.sender.count())
	}
	for _, d := range e.deliveries(t, "t1", "sub_a", subscription.DeliveryPending) {
		if d.EventID == "evt_late" && d.Attempts != 0 {
			t.Errorf("POSTPONED deliveries keep their budget: %+v", d)
		}
	}
	// after the cool-down the endpoint is probed again; once it recovers everything is delivered
	e.sender.reply = nil
	e.clock.Advance(2 * time.Minute)
	for i := 0; i < 5; i++ {
		_, _ = e.disp.RunOnce(bg)
		e.clock.Advance(time.Minute)
	}
	if got := e.deliveries(t, "t1", "sub_a", subscription.DeliveryDelivered); len(got) != 7 {
		t.Errorf("everything is delivered after recovery: %d of 7 (pending %d)", len(got), len(e.deliveries(t, "t1", "sub_a", subscription.DeliveryPending)))
	}
}

func TestRemovedSubscriptionDeadLettersWhatWasClaimed(t *testing.T) {
	e := newEnv(t)
	s := e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	_ = e.fan.Handle(bg, event("evt_1", "t1", "inst_1"))
	// claim, then the tenant removes the subscription before the POST
	claimed, _ := e.repos.Deliveries.ClaimDue(bg, e.clock.Now(), time.Minute, 10)
	if len(claimed) != 1 {
		t.Fatal("setup")
	}
	if err := e.repos.Subscriptions.Delete(bg, "t1", s.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := e.disp.RunOnce(bg); err != nil || n != 0 || e.sender.count() != 0 {
		t.Errorf("nothing is sent for a removed subscription: %d %v sent=%d", n, err, e.sender.count())
	}
}

func TestPerInstanceDeliveriesAreNotConcurrent(t *testing.T) {
	e := newEnv(t)
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	for i := 1; i <= 3; i++ {
		_ = e.fan.Handle(bg, event(fmt.Sprintf("evt_%d", i), "t1", "inst_1"))
		e.clock.Advance(time.Millisecond)
	}
	if n, _ := e.disp.RunOnce(bg); n != 1 {
		t.Fatalf("one delivery per (subscription, instance) per pass keeps them in order: %d", n)
	}
	if e.sender.reqs[0].Headers[subscription.HeaderEventID] != "evt_1" {
		t.Errorf("oldest first: %s", e.sender.reqs[0].Headers[subscription.HeaderEventID])
	}
	for i := 0; i < 2; i++ {
		_, _ = e.disp.RunOnce(bg)
	}
	var order []string
	for _, r := range e.sender.reqs {
		order = append(order, r.Headers[subscription.HeaderEventID])
	}
	if fmt.Sprint(order) != "[evt_1 evt_2 evt_3]" {
		t.Errorf("delivery order %v", order)
	}
}

// The consumer's trace links to ours: an event that carries a trace (a message status carries the trace of the send)
// is delivered with that traceparent.
func TestDeliveryForwardsTheEventTraceparent(t *testing.T) {
	e := newEnv(t)
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	traced := event("evt_traced", "t1", "inst_1")
	traced.TraceParent = tp
	plain := event("evt_plain", "t1", "inst_2")
	_ = e.fan.Handle(bg, traced)
	_ = e.fan.Handle(bg, plain)
	_, _ = e.disp.RunOnce(bg)
	got := map[string]string{}
	for _, r := range e.sender.reqs {
		got[r.Headers[subscription.HeaderEventID]] = r.Headers["traceparent"]
	}
	if got["evt_traced"] != tp {
		t.Errorf("the event's trace must be forwarded verbatim: %q", got["evt_traced"])
	}
	if p := got["evt_plain"]; p != "" && !strings.HasPrefix(p, "00-") {
		t.Errorf("a traceparent, when present, must be well formed: %q", p)
	}
}

// Pausing means nothing is sent: a delivery that was already claimed when the subscription was paused, and is still waiting for its turn,
// goes back instead of being POSTed.
func TestPauseAfterClaimStopsTheDeliveriesStillWaitingTheirTurn(t *testing.T) {
	e := newEnv(t)
	e.disp.Concurrency = 1 // one POST at a time: the second delivery is claimed with the first and waits
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	_ = e.fan.Handle(bg, event("evt_1", "t1", "inst_1"))
	_ = e.fan.Handle(bg, event("evt_2", "t1", "inst_2"))
	e.sender.reply = func(n int, _ ports.WebhookRequest) (int, error) {
		if n == 1 { // the operator pauses while the first POST is going out
			if err := e.repos.Subscriptions.SetPaused(bg, "t1", "sub_a", true); err != nil {
				t.Error(err)
			}
		}
		return 200, nil
	}
	if _, err := e.disp.RunOnce(bg); err != nil {
		t.Fatal(err)
	}
	if e.sender.count() != 1 {
		t.Fatalf("the delivery claimed before the pause must not be sent: %d POSTs", e.sender.count())
	}
	if n := len(e.deliveries(t, "t1", "sub_a", subscription.DeliveryPending)); n != 1 {
		t.Fatalf("it waits: %d pending", n)
	}
	// resumed, it goes out
	if err := e.repos.Subscriptions.SetPaused(bg, "t1", "sub_a", false); err != nil {
		t.Fatal(err)
	}
	e.sender.reply = nil
	if _, err := e.disp.RunOnce(bg); err != nil {
		t.Fatal(err)
	}
	if e.sender.count() != 2 {
		t.Errorf("after the resume the held delivery is sent: %d", e.sender.count())
	}
}

// The circuit belongs to the subscription: another tenant that points at the same host is not shielded (or punished) by it.
func TestCircuitBreakerIsPerSubscriptionNotPerHost(t *testing.T) {
	e := newEnv(t) // opens after 3 consecutive failures
	const shared = "https://automation.example.com/hook"
	e.sub(t, "sub_a", "t1", shared)
	e.sub(t, "sub_b", "t2", shared)
	e.sender.reply = func(_ int, r ports.WebhookRequest) (int, error) {
		if strings.Contains(string(r.Body), `"tenant_id":"t1"`) {
			return 503, nil // only tenant 1's endpoint logic is broken
		}
		return 200, nil
	}
	for i := 1; i <= 4; i++ {
		_ = e.fan.Handle(bg, event(fmt.Sprintf("a_%d", i), "t1", fmt.Sprintf("ia_%d", i)))
	}
	_, _ = e.disp.RunOnce(bg) // t1's circuit opens
	_ = e.fan.Handle(bg, event("b_1", "t2", "ib_1"))
	_, _ = e.disp.RunOnce(bg)
	if got := e.deliveries(t, "t2", "sub_b", subscription.DeliveryDelivered); len(got) != 1 {
		t.Fatalf("tenant 2 must be served although tenant 1's circuit on the same host is open: %d delivered", len(got))
	}
}

// acceptedEvent is an inbound message of a contact, accepted "now" (the moment an erasure is compared with).
func (e *env) acceptedEvent(id, tenant, inst string, typ events.Type) events.Event {
	at := e.clock.Now().UTC()
	ev := event(id, tenant, inst)
	ev.EventType, ev.AcceptedAt = typ, &at
	if typ == events.MessageDeleted {
		ev.Payload = events.MessageDeletedPayload{ProviderMessageID: "W" + id, From: "5562988887777"}
	} else {
		ev.Payload = events.MessageReceivedPayload{ProviderMessageID: "W" + id, From: "5562988887777", Type: "text", Text: "segredo"}
	}
	return ev
}

func (e *env) erase(t *testing.T, tenant string, at time.Time) {
	t.Helper()
	if err := e.repos.Erasures.Mark(bg, tenant, events.ErasureSubject(nil, "5562988887777"), at); err != nil {
		t.Fatal(err)
	}
}

// The erasure came after the dispatcher claimed the delivery (it holds a copy of the text; the erasure deleted the row): nothing may go out
// after the erasure returned.
func TestDispatcherDropsAClaimedDeliveryOfAContactErasedAfterTheClaim(t *testing.T) {
	e := newEnv(t)
	e.disp.Concurrency = 1 // the second delivery is claimed with the first and waits for its turn
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	_ = e.fan.Handle(bg, e.acceptedEvent("evt_1", "t1", "inst_1", events.MessageReceived))
	_ = e.fan.Handle(bg, e.acceptedEvent("evt_2", "t1", "inst_2", events.MessageReceived))
	e.sender.reply = func(n int, _ ports.WebhookRequest) (int, error) {
		if n == 1 { // the contact is erased while the first POST is going out
			e.erase(t, "t1", e.clock.Now().Add(time.Second))
		}
		return 200, nil
	}
	if _, err := e.disp.RunOnce(bg); err != nil {
		t.Fatal(err)
	}
	if e.sender.count() != 1 {
		t.Fatalf("the delivery claimed before the erasure must not be sent after it: %d POSTs", e.sender.count())
	}
	if n := len(e.deliveries(t, "t1", "sub_a", subscription.DeliveryPending)); n != 0 {
		t.Errorf("it is dropped, not kept: %d pending", n)
	}
}

// A deletion notice carries the author's number too: after the erasure it must not become a delivery.
func TestFanOutDropsTheDeletionNoticeOfAnErasedContact(t *testing.T) {
	e := newEnv(t)
	e.sub(t, "sub_a", "t1", "https://a.example.com/h")
	e.erase(t, "t1", e.clock.Now().Add(time.Second))
	_ = e.fan.Handle(bg, e.acceptedEvent("evt_del", "t1", "inst_1", events.MessageDeleted))
	if n := len(e.deliveries(t, "t1", "sub_a", subscription.DeliveryPending)); n != 0 {
		t.Fatalf("a message.deleted of an erased contact must not be delivered: %d", n)
	}
	// but one that is NEW (accepted after the erasure) is
	e.clock.Advance(5 * time.Second)
	_ = e.fan.Handle(bg, e.acceptedEvent("evt_del2", "t1", "inst_1", events.MessageDeleted))
	if n := len(e.deliveries(t, "t1", "sub_a", subscription.DeliveryPending)); n != 1 {
		t.Fatalf("a notice accepted after the erasure is new data: %d", n)
	}
}
