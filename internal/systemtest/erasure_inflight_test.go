package systemtest

import (
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/relayplane/relayplane/internal/core/events"
)

const erasedNumber = "5562988887777"

func counter(t *testing.T, e *Env, outcome string) float64 {
	t.Helper()
	var m dto.Metric
	if err := e.Metrics.InboundMedia.WithLabelValues(outcome).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// An event accepted before the erasure that is still waiting (here: in the outbox, the broker being down) does not come back to the tenant
// after the contact was erased. A message the contact sends AFTER the erasure is new data and is delivered.
func TestErasure_ANewMessageAfterTheErasureIsStillDelivered(t *testing.T) {
	f := newMediaFixtureWith(t, nil)
	e := f.e
	e.Bus.Down.Store(true)
	f.receiveFrom(erasedNumber, "WA-OLD", "segredo de antes", nil)
	if waiting, _ := e.Repos.Events.ListUnpublished(bg, 100); len(waiting) == 0 {
		t.Fatal("the accepted event waits to be published")
	}
	if _, err := e.App.Contacts.Erase(bg, e.Tenant, erasedNumber); err != nil {
		t.Fatal(err)
	}
	e.Bus.Down.Store(false)
	time.Sleep(300 * time.Millisecond)
	if n := len(e.Receiver.Accepted(hookURL)); n != 0 {
		t.Fatalf("the erased contact's event reached the tenant: %d", n)
	}
	time.Sleep(10 * time.Millisecond)
	f.receiveFrom(erasedNumber, "WA-NEW", "oi de novo", nil)
	Eventually(t, 10*time.Second, "a message sent after the erasure is delivered", func() bool { return len(e.Receiver.Accepted(hookURL)) == 1 })
}

// The fan-out had the event in hand when the erasure came: the tombstone is checked after the deliveries are written too.
func TestErasure_TheFanOutDropsWhatItHeldWhenTheErasureCame(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.StartOutbox()
	f := &mediaFixture{t: t, e: e, inst: inst}
	f.receiveFrom(erasedNumber, "WA-HELD", "segredo", nil)
	e.Flush() // the event is on the bus: accepted, published, not yet fanned out
	if _, err := e.App.Contacts.Erase(bg, e.Tenant, erasedNumber); err != nil {
		t.Fatal(err)
	}
	e.StartWebhooks() // the fan-out now receives the old event
	time.Sleep(400 * time.Millisecond)
	if n := len(e.Receiver.Accepted(hookURL)); n != 0 {
		t.Fatalf("the event of an erased contact was delivered: %d", n)
	}
	if bl, _ := e.App.Subscriptions.Backlog(bg, e.Tenant); len(bl) > 0 {
		for id, b := range bl {
			if b.Pending != 0 {
				t.Fatalf("a delivery of the erased contact was created for %s: %+v", id, b)
			}
		}
	}
}

// The erasure arrives while the attachment is being downloaded: what the download stores afterwards is removed and nothing is published.
func TestErasure_AnAttachmentBeingDownloadedIsNotKept(t *testing.T) {
	var once sync.Once
	started, release := make(chan struct{}), make(chan struct{})
	f := newMediaFixtureWith(t, func(e *Env) {
		e.Provider.OnDownload = func() {
			once.Do(func() { close(started) })
			<-release
		}
	})
	e := f.e
	e.StartMedia()
	f.receiveFrom(erasedNumber, "WA-VOICE", "", attachmentOf("audio", "audio/ogg", "", []byte("OggS minha voz")))
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the download never started")
	}
	if _, err := e.App.Contacts.Erase(bg, e.Tenant, erasedNumber); err != nil {
		t.Fatal(err)
	}
	close(release) // the download finishes and tries to store the file of a contact that was just erased
	Eventually(t, 10*time.Second, "the ingestor noticed the erasure", func() bool { return counter(t, e, "erased") >= 1 })
	if left, _ := e.Repos.Blobs.ListBySubject(bg, e.Tenant, erasedNumber); len(left) != 0 {
		t.Fatalf("an attachment of the erased contact was kept: %+v", left)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(e.Receiver.Accepted(hookURL)); n != 0 {
		t.Fatalf("the message of the erased contact was delivered: %d", n)
	}
}
