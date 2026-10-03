package systemtest

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

const hookURL = "http://agent.local/hooks/relayplane"

// subscribe registers a webhook subscription for the tenant and returns its signing secret.
func subscribe(t *testing.T, e *Env, tenant, url string, types ...string) (id, secret string) {
	t.Helper()
	sv, err := e.App.Subscriptions.Create(bg, tenant, app.CreateSubscriptionInput{URL: url, EventTypes: types})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	return sv.ID, sv.Secret
}

// verifyReceived checks the signature the way a consumer would.
func verifyReceived(t *testing.T, r Received, secret string) {
	t.Helper()
	if err := subscription.Verify([]string{secret}, r.Signature, r.Timestamp, r.Body, time.Now(), time.Minute); err != nil {
		t.Fatalf("event %s: the consumer cannot verify the signature: %v", r.EventID, err)
	}
}

func outboundStatuses(t *testing.T, recs []Received) map[string]events.MessageOutboundStatusPayload {
	t.Helper()
	out := map[string]events.MessageOutboundStatusPayload{}
	for _, r := range recs {
		if r.EventType != string(events.MessageOutboundStatus) {
			continue
		}
		var env struct {
			Payload events.MessageOutboundStatusPayload `json:"payload"`
		}
		if err := json.Unmarshal(r.Body, &env); err != nil {
			t.Fatal(err)
		}
		out[env.Payload.MessageID+"/"+env.Payload.Status] = env.Payload
	}
	return out
}

// The consumer learns that a message it sent was ACCEPTED (with the provider's acceptance time) and later DELIVERED,
// each signed, each exactly once per event id.
func TestTenantEvents_OutboundStatusReachesTheSubscriber(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	_, secret := subscribe(t, e, e.Tenant, hookURL, string(events.MessageOutboundStatus))
	e.StartWorkers(2)
	e.StartOutbox()
	e.StartProjector()
	e.StartWebhooks()

	r, _, err := e.SendText(e.Tenant, inst.ID, "oi", "k1")
	if err != nil {
		t.Fatal(err)
	}
	acc := e.WaitMessage(r.MessageID, messaging.StatusAccepted)
	Eventually(t, 10*time.Second, "ACCEPTED event delivered", func() bool {
		_, ok := outboundStatuses(t, e.Receiver.Accepted(hookURL))[r.MessageID+"/ACCEPTED"]
		return ok
	})
	st := outboundStatuses(t, e.Receiver.Accepted(hookURL))[r.MessageID+"/ACCEPTED"]
	if st.ProviderMessageID != acc.ProviderMessageID || st.AcceptedAt == nil || st.SequenceNo != 1 {
		t.Errorf("payload: %+v (message %+v)", st, acc)
	}
	if !st.AcceptedAt.Equal(acc.AcceptedAt) && st.AcceptedAt.Sub(acc.AcceptedAt).Abs() > time.Millisecond {
		t.Errorf("accepted_at %v vs message %v", st.AcceptedAt, acc.AcceptedAt)
	}

	// the provider reports a delivery receipt -> DELIVERED reaches the consumer too
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, statusEv(inst.ID, acc.ProviderMessageID, "delivered"))); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "DELIVERED event delivered", func() bool {
		_, ok := outboundStatuses(t, e.Receiver.Accepted(hookURL))[r.MessageID+"/DELIVERED"]
		return ok
	})
	for _, rec := range e.Receiver.Accepted(hookURL) {
		verifyReceived(t, rec, secret)
		if rec.EventType == string(events.MessageStatus) {
			t.Error("the subscription asked only for message.outbound_status")
		}
	}
	for id, n := range e.Receiver.DistinctEvents(hookURL) {
		if n != 1 {
			t.Errorf("event %s accepted %d times", id, n)
		}
	}
}

// The inbound message carries what the agent needs to decide whether a "yes" answers one of ITS questions.
func TestTenantEvents_InboundMessageCarriesReplyToAndProviderTimestamp(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	_, secret := subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.StartOutbox()
	e.StartWebhooks()

	at := time.Now().Add(-3 * time.Second).UTC().Truncate(time.Second)
	ev := memory.FakeWebhookEv{InstanceID: inst.ID, Type: events.MessageReceived, ProviderMessageID: "WA-IN-1", Timestamp: at,
		Payload: json.RawMessage(`{"provider_message_id":"WA-IN-1","reply_to_provider_message_id":"OUR-PROMPT-1","from":"5562","type":"text","text":"sim"}`)}
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, ev)); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "message.received delivered", func() bool { return len(e.Receiver.Accepted(hookURL)) == 1 })
	rec := e.Receiver.Accepted(hookURL)[0]
	verifyReceived(t, rec, secret)
	var env struct {
		EventType  string    `json:"event_type"`
		TenantID   string    `json:"tenant_id"`
		InstanceID string    `json:"instance_id"`
		Timestamp  time.Time `json:"timestamp"`
		Payload    events.MessageReceivedPayload
	}
	if err := json.Unmarshal(rec.Body, &env); err != nil {
		t.Fatal(err)
	}
	if env.EventType != "message.received" || env.TenantID != e.Tenant || env.InstanceID != inst.ID {
		t.Errorf("envelope: %+v", env)
	}
	if env.Payload.ReplyToProviderMessageID != "OUR-PROMPT-1" || env.Payload.Text != "sim" {
		t.Errorf("payload: %+v", env.Payload)
	}
	if !env.Timestamp.Equal(at) {
		t.Errorf("timestamp is when the PROVIDER stamped the message (%v), got %v", at, env.Timestamp)
	}
}

// A tenant only ever receives its own events.
func TestTenantEvents_TenantsNeverSeeEachOthersEvents(t *testing.T) {
	e := NewEnv(t)
	i1 := e.CreateInstance(e.Tenant, "a", true)
	i2 := e.CreateInstance(e.Tenant2, "b", true)
	subscribe(t, e, e.Tenant, "http://t1.local/h")
	subscribe(t, e, e.Tenant2, "http://t2.local/h")
	e.StartWorkers(2)
	e.StartOutbox()
	e.StartWebhooks()

	m1, _, _ := e.SendText(e.Tenant, i1.ID, "from t1", "k1")
	m2, _, _ := e.SendText(e.Tenant2, i2.ID, "from t2", "k2")
	e.WaitMessage(m1.MessageID, messaging.StatusAccepted)
	e.WaitMessage(m2.MessageID, messaging.StatusAccepted)
	Eventually(t, 10*time.Second, "both tenants got their event", func() bool {
		return len(e.Receiver.Accepted("http://t1.local/h")) >= 1 && len(e.Receiver.Accepted("http://t2.local/h")) >= 1
	})
	time.Sleep(100 * time.Millisecond)
	for url, other := range map[string]string{"http://t1.local/h": m2.MessageID, "http://t2.local/h": m1.MessageID} {
		for _, rec := range e.Receiver.All() {
			if rec.URL == url && containsBytes(rec.Body, other) {
				t.Fatalf("TENANT ISOLATION: %s received an event about another tenant's message %s", url, other)
			}
		}
	}
}

func containsBytes(b []byte, s string) bool { return len(s) > 0 && indexOf(string(b), s) >= 0 }

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}

// The tenant's endpoint is down for a while: nothing is lost, deliveries are retried until it answers.
func TestTenantEvents_EndpointOutageIsRetriedWithoutLoss(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageOutboundStatus))
	e.Receiver.Behave = func(n int, r Received) (int, error) {
		if time.Since(start) < 300*time.Millisecond {
			return 503, nil // the agent is restarting
		}
		return 200, nil
	}
	// the retry budget must outlast the outage (that is the contract: the DLQ only gets what exhausted its budget)
	e.Dispatcher.Retry.Schedule = make([]time.Duration, 60)
	for i := range e.Dispatcher.Retry.Schedule {
		e.Dispatcher.Retry.Schedule[i] = 20 * time.Millisecond
	}
	start = time.Now()
	e.StartWorkers(2)
	e.StartOutbox()
	e.StartWebhooks()

	var ids []string
	for i := 0; i < 5; i++ {
		r, _, err := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%d", i), "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.MessageID)
	}
	for _, id := range ids {
		e.WaitMessage(id, messaging.StatusAccepted)
	}
	Eventually(t, 15*time.Second, "every ACCEPTED event delivered after the outage", func() bool {
		got := outboundStatuses(t, e.Receiver.Accepted(hookURL))
		for _, id := range ids {
			if _, ok := got[id+"/ACCEPTED"]; !ok {
				return false
			}
		}
		return true
	})
	failed := 0
	for _, r := range e.Receiver.All() {
		if r.Status >= 500 {
			failed++
		}
	}
	if failed == 0 {
		t.Error("the scenario must actually exercise failures")
	}
	for id, n := range e.Receiver.DistinctEvents(hookURL) {
		if n != 1 {
			t.Errorf("event %s accepted %d times: once the endpoint says 2xx the event is done", id, n)
		}
	}
}

var start time.Time

// The broker is down while messages are accepted: the status changes are already durable, and the events are
// published (and delivered) once the broker is back. Nothing is lost between the database and the bus.
func TestTenantEvents_BrokerOutageLosesNothing(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageOutboundStatus))
	e.StartWorkers(2)
	e.StartOutbox()
	e.StartWebhooks()

	e.Bus.Down.Store(true) // events cannot be published
	var ids []string
	for i := 0; i < 3; i++ {
		r, _, _ := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%d", i), "")
		ids = append(ids, r.MessageID)
	}
	for _, id := range ids {
		e.WaitMessage(id, messaging.StatusAccepted) // the commands use the queue, not the bus
	}
	time.Sleep(150 * time.Millisecond)
	if n := len(e.Receiver.Accepted(hookURL)); n != 0 {
		t.Fatalf("nothing can be delivered while the bus is down: %d", n)
	}
	pending, _ := e.Repos.Events.ListUnpublished(bg, 100)
	if len(pending) < 3 {
		t.Fatalf("the status changes must be waiting in the durable outbox: %d", len(pending))
	}
	e.Bus.Down.Store(false)
	Eventually(t, 15*time.Second, "events delivered after the bus recovered", func() bool {
		got := outboundStatuses(t, e.Receiver.Accepted(hookURL))
		for _, id := range ids {
			if _, ok := got[id+"/ACCEPTED"]; !ok {
				return false
			}
		}
		return true
	})
}

// A crash between "published to the bus" and "marked published" republishes the event: the consumer still gets it
// exactly once, because deliveries are unique per (subscription, event id).
func TestTenantEvents_RepublishedEventIsStillDeliveredOnce(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageOutboundStatus))
	e.StartWorkers(2)
	e.StartWebhooks() // note: the outbox publisher is NOT running yet
	r, _, _ := e.SendText(e.Tenant, inst.ID, "oi", "")
	e.WaitMessage(r.MessageID, messaging.StatusAccepted)

	pending, _ := e.Repos.Events.ListUnpublished(bg, 100)
	if len(pending) == 0 {
		t.Fatal("setup: expected an unpublished event")
	}
	for _, ev := range pending { // "crash" after publishing, before MarkPublished
		if err := e.Bus.Publish(bg, ev); err != nil {
			t.Fatal(err)
		}
	}
	e.StartOutbox() // recovery publishes them again
	Eventually(t, 10*time.Second, "delivered", func() bool { return len(e.Receiver.DistinctEvents(hookURL)) >= 1 })
	time.Sleep(200 * time.Millisecond)
	for id, n := range e.Receiver.DistinctEvents(hookURL) {
		if n != 1 {
			t.Errorf("event %s was delivered %d times", id, n)
		}
	}
	if left, _ := e.Repos.Events.ListUnpublished(bg, 100); len(left) != 0 {
		t.Errorf("recovery marks the events published: %d left", len(left))
	}
}

// Deliveries that exhaust their retries are kept in the DLQ and can be redelivered by the tenant.
func TestTenantEvents_DeadLetterAndRedelivery(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subID, _ := subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	var healthy bool
	e.Receiver.Behave = func(int, Received) (int, error) {
		if healthy {
			return 200, nil
		}
		return 500, nil
	}
	e.StartOutbox()
	e.StartWebhooks()
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "WA-1"))); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "delivery dead-lettered", func() bool {
		d, _ := e.App.Subscriptions.Deliveries(bg, e.Tenant, subID, subscription.DeliveryDead, 10)
		return len(d) == 1
	})
	dead, _ := e.App.Subscriptions.Deliveries(bg, e.Tenant, subID, subscription.DeliveryDead, 10)
	if dead[0].Attempts != e.Dispatcher.Retry.MaxAttempts() || dead[0].LastError == "" {
		t.Errorf("DLQ entry: %+v", dead[0])
	}
	// another tenant cannot touch it
	if err := e.App.Subscriptions.Redeliver(bg, e.Tenant2, dead[0].ID); err == nil {
		t.Fatal("TENANT ISOLATION: another tenant redelivered someone else's DLQ entry")
	}
	healthy = true
	if err := e.App.Subscriptions.Redeliver(bg, e.Tenant, dead[0].ID); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "redelivered", func() bool { return len(e.Receiver.Accepted(hookURL)) == 1 })
	var _ = instance.Connected
}
