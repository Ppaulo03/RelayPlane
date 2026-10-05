package systemtest

import (
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// The tenant must receive an accepted event even if the broker loses it AFTER saying it was published: the broker is a transport, never the
// only durable copy between "accepted" and "a delivery exists for the tenant". The outbox row is "published" (the broker said OK) and the
// broker's data is gone before anybody read it.
func TestDurability_TheTenantStillGetsAnEventTheBrokerLostAfterPublishing(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.Bus.Drop.Store(true)
	e.StartWebhooks()

	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "wamid-lost"))); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "the outbox publisher believes it published", func() bool {
		left, _ := e.Repos.Events.ListUnpublished(bg, 100)
		return len(left) == 0
	})
	Eventually(t, 10*time.Second, "the tenant receives the event the broker lost", func() bool { return len(e.Receiver.Accepted(hookURL)) == 1 })
}

// Same for a message with an attachment: its resolved event must not exist only in the broker once the job is closed.
func TestDurability_AResolvedAttachmentMessageSurvivesTheBrokerLosingIt(t *testing.T) {
	f := newMediaFixtureWith(t, func(e *Env) { e.Bus.Drop.Store(true) })
	e := f.e
	e.StartMedia()
	f.receiveFrom(erasedNumber, "WA-MEDIA-LOST", "", attachmentOf("audio", "audio/ogg", "", []byte("OggS voz")))
	Eventually(t, 15*time.Second, "the tenant receives the resolved attachment message", func() bool { return len(e.Receiver.Accepted(hookURL)) == 1 })
}
