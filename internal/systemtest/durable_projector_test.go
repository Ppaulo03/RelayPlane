package systemtest

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
)

// A delivery receipt is what turns ACCEPTED into DELIVERED/READ, and that change is what the tenant is told about. If the broker accepts the
// receipt event and then loses it, the receipt must still be applied: the projector works from the database, not from the broker.
func TestDurability_AReceiptTheBrokerLostIsStillApplied(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.StartWorkers(2)
	e.StartProjector()
	r, _, _ := e.SendText(e.Tenant, inst.ID, "oi", "")
	m := e.WaitMessage(r.MessageID, messaging.StatusAccepted)

	e.Bus.Drop.Store(true) // from now on the broker takes events and loses them
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, statusEv(inst.ID, m.ProviderMessageID, "delivered"))); err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(r.MessageID, messaging.StatusDelivered)
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, statusEv(inst.ID, m.ProviderMessageID, "read"))); err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(r.MessageID, messaging.StatusRead)
}

// Same for the state of a session (a logout is a fact the catalog must learn even if the broker drops the event).
func TestDurability_AnInstanceStateChangeTheBrokerLostIsStillApplied(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.StartProjector()
	e.Bus.Drop.Store(true)
	now := time.Now()
	ev := memory.FakeWebhookEv{InstanceID: inst.ID, Type: events.InstanceStatusChanged, ProviderMessageID: fmt.Sprint(now.UnixMilli()), State: "DISCONNECTED",
		Timestamp: now.Add(time.Second), Payload: json.RawMessage(`{"state":"DISCONNECTED"}`)}
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, ev)); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "projected DISCONNECTED although the broker lost the event", func() bool {
		i, _ := e.Repos.Instances.Get(bg, inst.ID)
		return i.ObservedState == instance.Disconnected
	})
}
