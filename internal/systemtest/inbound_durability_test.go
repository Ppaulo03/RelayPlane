package systemtest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// A 200 to the provider means the event is durably accepted: it is in the database, waiting for its deliveries to be created, whatever
// is or is not running at that moment (no consumer at all, here). A retry of the provider is a duplicate, not a second event; and when
// the consumers come up the tenant receives it exactly once.
func TestInbound_AnAcceptedEventWaitsInTheDatabaseForItsDeliveries(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	base := len(e.OutboxEvents())

	req := inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "wamid-durable"))
	r, err := e.App.Inbound.Handle(bg, ProviderKey, req)
	if err != nil || r.Published != 1 {
		t.Fatalf("the provider is answered 200 with nothing running: %+v %v", r, err)
	}
	waiting := 0
	for _, ev := range e.OutboxEvents()[base:] {
		if ev.EventType == events.MessageReceived && ev.InstanceID == inst.ID {
			waiting++
		}
	}
	if waiting != 1 {
		t.Fatalf("the accepted event must be waiting in the database: %d", waiting)
	}
	if r2, err := e.App.Inbound.Handle(bg, ProviderKey, req); err != nil || r2.Duplicates != 1 || r2.Published != 0 {
		t.Fatalf("the provider's retry is a duplicate: %+v %v", r2, err)
	}
	if n := len(e.OutboxEvents()) - base; n != 1 {
		t.Fatalf("a retry must not queue another event: %d", n)
	}

	e.StartWebhooks() // the consumers come up now
	Eventually(t, 10*time.Second, "the tenant receives the message", func() bool { return len(e.Receiver.Accepted(hookURL)) == 1 })
	time.Sleep(200 * time.Millisecond)
	if n := len(e.Receiver.Accepted(hookURL)); n != 1 {
		t.Fatalf("exactly once: %d", n)
	}
}

// An accepted event that has not been fanned out yet holds the text and the number of the contact: erasing the contact removes it too, so
// it can never reach the tenant after the erasure.
func TestInbound_ErasureAlsoRemovesTheEventsStillWaitingForTheirDeliveries(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	ev := recvEv(inst.ID, "wamid-erase")
	ev.Payload = json.RawMessage(`{"provider_message_id":"wamid-erase","from":"5562988887777","type":"text","text":"segredo"}`)
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, ev)); err != nil {
		t.Fatal(err)
	}
	rep, err := e.App.Contacts.Erase(bg, e.Tenant, "5562988887777")
	if err != nil || rep.EventsDeleted < 1 {
		t.Fatalf("erase: %+v %v", rep, err)
	}
	e.StartWebhooks()
	time.Sleep(400 * time.Millisecond)
	if n := len(e.Receiver.Accepted(hookURL)); n != 0 {
		t.Fatalf("an erased contact's event reached the tenant: %d", n)
	}
}
