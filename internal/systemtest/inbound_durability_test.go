package systemtest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// A 200 to the provider means the event is durably accepted. Whatever happens afterwards (the broker is down, the process dies
// before publishing) the event is not lost: it waits in the database and reaches the bus later, once. Before, an event was marked
// "seen" and only then published, so a crash in between turned the provider's retry into a "duplicate" of an event nobody queued.
func TestInbound_AcceptedEventSurvivesTheBrokerBeingDown(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Flush()
	base := len(e.Bus.Published())

	e.Bus.Down.Store(true) // the broker is unavailable at the moment the webhook arrives
	req := inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "wamid-durable"))
	r, err := e.App.Inbound.Handle(bg, ProviderKey, req)
	if err != nil || r.Published != 1 {
		t.Fatalf("the provider must be answered 200 even with the broker down: %+v %v", r, err)
	}
	waiting, err := e.Repos.Events.ListUnpublished(bg, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range waiting {
		found = found || (ev.EventType == events.MessageReceived && ev.InstanceID == inst.ID)
	}
	if !found {
		t.Fatalf("the accepted event must be waiting in the database: %+v", waiting)
	}
	if n := len(e.Bus.Published()) - base; n != 0 {
		t.Fatalf("nothing can have reached the bus yet: %d", n)
	}

	// the provider retries (it could not know): that is a duplicate, not a second event
	if r2, err := e.App.Inbound.Handle(bg, ProviderKey, req); err != nil || r2.Duplicates != 1 || r2.Published != 0 {
		t.Fatalf("retry: %+v %v", r2, err)
	}

	e.Bus.Down.Store(false)
	e.Flush()
	got := 0
	for _, ev := range e.Bus.Published()[base:] {
		if ev.EventType == events.MessageReceived {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("the event reaches the bus exactly once after recovery: %d", got)
	}
}

// ... and all the way to the tenant: the webhook is delivered even though the broker was down when the message arrived.
func TestInbound_AcceptedEventIsDeliveredToTheTenantAfterAnOutage(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.StartWebhooks() // also starts the outbox loop

	e.Bus.Down.Store(true)
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "wamid-outage"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(e.Receiver.Accepted(hookURL)); n != 0 {
		t.Fatalf("nothing can be delivered while the broker is down: %d", n)
	}
	e.Bus.Down.Store(false)
	Eventually(t, 10*time.Second, "the tenant receives the message", func() bool { return len(e.Receiver.Accepted(hookURL)) == 1 })
}

// An accepted event that has not been published yet holds the text and the number of the contact: erasing the contact removes it
// too, so it can never come back to the tenant after the erasure.
func TestInbound_ErasureAlsoRemovesTheEventsStillWaitingToBePublished(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.Flush()
	base := len(e.Bus.Published())
	e.Bus.Down.Store(true)
	ev := recvEv(inst.ID, "wamid-erase")
	ev.Payload = json.RawMessage(`{"provider_message_id":"wamid-erase","from":"5562988887777","type":"text","text":"segredo"}`)
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, ev)); err != nil {
		t.Fatal(err)
	}
	waiting, _ := e.Repos.Events.ListUnpublished(bg, 100)
	var from string
	for _, w := range waiting {
		if w.EventType == events.MessageReceived {
			var probe struct {
				From string `json:"from"`
			}
			raw, _ := json.Marshal(w.Payload)
			_ = json.Unmarshal(raw, &probe)
			from = probe.From
		}
	}
	if from == "" {
		t.Fatalf("the event waiting in the outbox must carry the contact: %+v", waiting)
	}
	rep, err := e.App.Contacts.Erase(bg, e.Tenant, from)
	if err != nil || rep.EventsDeleted < 1 {
		t.Fatalf("erase: %+v %v", rep, err)
	}
	e.Bus.Down.Store(false)
	e.Flush()
	for _, p := range e.Bus.Published()[base:] {
		if p.EventType == events.MessageReceived {
			t.Fatalf("an erased contact's event reached the bus: %+v", p)
		}
	}
}
