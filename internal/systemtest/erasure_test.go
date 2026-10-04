package systemtest

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

// An erasure request leaves nothing of the person behind: not in the messages sent to them, not in the events that
// mention them (delivered, waiting or dead-lettered), not in the files they sent. Everybody else is untouched.
func TestErasure_NothingOfTheContactIsLeft(t *testing.T) {
	const deadURL = "http://dead.local/hook"
	f := newMediaFixtureWith(t, func(e *Env) {
		e.Receiver.Behave = func(_ int, r Received) (int, error) {
			if r.URL == deadURL {
				return 500, nil
			}
			return 200, nil
		}
		e.Dispatcher.Retry = subscription.RetryPolicy{Schedule: []time.Duration{time.Millisecond}} // dead after the 2nd attempt
	})
	e := f.e
	e.StartWorkers(2)
	e.StartProjector()
	e.StartMedia()
	const target = "5562999999999" // the number Env.SendText writes to

	// the tenant wrote to the contact; the messages are accepted by the (fake) provider
	var anaMsgs []string
	for i := 0; i < 2; i++ {
		res, _, err := e.SendText(e.Tenant, f.inst.ID, fmt.Sprintf("segredo para Ana %d", i), fmt.Sprintf("a%d", i))
		if err != nil {
			t.Fatal(err)
		}
		anaMsgs = append(anaMsgs, res.MessageID)
	}
	for _, id := range anaMsgs {
		e.WaitMessage(id, messaging.StatusAccepted)
	}

	// the contact wrote back: text, a voice note (stored as an attachment) and something that will end in the DLQ
	deadSub, _ := subscribe(t, e, e.Tenant, deadURL, string(events.MessageReceived))
	f.receiveFrom(target, "WA-T1", "olá, aqui é a Ana", nil)
	f.receiveFrom(target, "WA-T2", "", attachmentOf("audio", "audio/ogg", "", []byte("OggS minha voz")))
	f.receiveFrom("5562900000000", "WA-OTHER", "outra pessoa", nil)
	Eventually(t, 15*time.Second, "events delivered and the dead one dead-lettered", func() bool {
		dead, _ := e.Repos.Deliveries.List(bg, e.Tenant, deadSub, subscription.DeliveryDead, 50)
		return len(e.Receiver.Accepted(hookURL)) >= 3 && len(dead) == 3
	})
	var voice string
	for _, r := range e.Receiver.Accepted(hookURL) {
		if strings.Contains(string(r.Body), `"media_id"`) && strings.Contains(string(r.Body), target) {
			voice = mediaIDOf(t, r.Body)
		}
	}
	if voice == "" {
		t.Fatal("the voice note was not delivered")
	}
	blob, _ := e.Repos.Blobs.Get(bg, voice)
	if _, err := e.Blob.Stat(bg, blob.ObjectKey); err != nil {
		t.Fatalf("the attachment is stored: %v", err)
	}
	// and one more event is still waiting for a paused consumer
	if err := e.App.Subscriptions.Pause(bg, e.Tenant, deadSub); err != nil {
		t.Fatal(err)
	}
	f.receiveFrom(target, "WA-T3", "mais uma coisa", nil)

	// ---- erase ----
	rep, err := e.App.Contacts.Erase(bg, e.Tenant, "+"+target[:2]+" ("+target[2:4]+") "+target[4:]) // however the caller typed it
	if err != nil {
		t.Fatal(err)
	}
	if rep.MessagesAnonymized != 2 || rep.AttachmentsDeleted != 1 || rep.EventsDeleted < 4 {
		t.Errorf("report: %+v", rep)
	}

	// ---- nothing is left ----
	for _, id := range anaMsgs {
		m, _ := e.Repos.Messages.Get(bg, id)
		if m.Recipient != "" || strings.Contains(string(m.Payload), "segredo") || m.ErasedAt.IsZero() || m.SequenceNo == 0 {
			t.Errorf("message %s still holds the person (or lost its ledger): %+v", id, m)
		}
	}
	for _, st := range []subscription.DeliveryStatus{subscription.DeliveryPending, subscription.DeliveryDelivered, subscription.DeliveryDead} {
		for _, sid := range []string{deadSub} {
			ds, _ := e.Repos.Deliveries.List(bg, e.Tenant, sid, st, 100)
			for _, d := range ds {
				if raw := payloadText(d); strings.Contains(raw, target) || strings.Contains(raw, "Ana") {
					t.Errorf("a %s delivery still mentions the contact: %s", st, raw)
				}
			}
		}
	}
	if _, err := e.Blob.Stat(bg, blob.ObjectKey); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("the stored voice note must be gone from the object store: %v", err)
	}
	if _, _, err := e.App.Media.Open(bg, e.Tenant, voice); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("and not downloadable: %v", err)
	}
	if left, _ := e.Repos.Blobs.ListBySubject(bg, e.Tenant, target); len(left) != 0 {
		t.Errorf("no attachment of the contact is listed: %+v", left)
	}

	// ---- everybody else is untouched ----
	other := 0
	for _, st := range []subscription.DeliveryStatus{subscription.DeliveryPending, subscription.DeliveryDelivered, subscription.DeliveryDead} {
		ds, _ := e.Repos.Deliveries.List(bg, e.Tenant, deadSub, st, 100)
		for _, d := range ds {
			if strings.Contains(payloadText(d), "5562900000000") {
				other++
			}
		}
	}
	if other != 1 {
		t.Errorf("another person's event must survive: %d", other)
	}
	// erasing again is harmless and reports nothing to do
	if again, err := e.App.Contacts.Erase(bg, e.Tenant, target); err != nil || again.MessagesAnonymized+again.AttachmentsDeleted != 0 || again.EventsDeleted != 0 {
		t.Errorf("second erasure: %+v %v", again, err)
	}
	// another tenant cannot erase this tenant's data
	if rep2, err := e.App.Contacts.Erase(bg, e.Tenant2, target); err != nil || rep2.EventsDeleted != 0 {
		t.Errorf("tenant isolation: %+v %v", rep2, err)
	}
	if _, err := e.App.Contacts.Erase(bg, e.Tenant, "not a number"); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Errorf("a malformed number is refused: %v", err)
	}
}

// Retention does on a clock what an erasure does on request.
func TestRetention_FinishedMessagesAndTheDLQDoNotStayForever(t *testing.T) {
	e := NewEnv(t)
	e.Receiver.Behave = func(int, Received) (int, error) { return 500, nil }
	e.Dispatcher.Retry = subscription.RetryPolicy{Schedule: []time.Duration{time.Millisecond}}
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.StartWorkers(1)
	e.StartProjector()
	deadURL := "http://dead.local/hook"
	sub, _ := subscribe(t, e, e.Tenant, deadURL, string(events.MessageReceived))
	e.StartOutbox()
	e.StartWebhooks()

	res, _, err := e.SendText(e.Tenant, inst.ID, "texto sensível", "k1")
	if err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(res.MessageID, messaging.StatusAccepted)
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "WA-R1"))); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "the event is dead-lettered", func() bool {
		d, _ := e.Repos.Deliveries.List(bg, e.Tenant, sub, subscription.DeliveryDead, 10)
		return len(d) == 1
	})
	time.Sleep(20 * time.Millisecond)

	// nothing is old enough yet
	if m, d, err := e.App.Retention.Apply(bg, app.RetentionPolicy{Messages: time.Hour, DeadDeliveries: time.Hour}, 100); err != nil || m+d != 0 {
		t.Fatalf("nothing is past its retention: %d %d %v", m, d, err)
	}
	// with a retention of a few milliseconds everything finished is past it
	// a row a receipt is updating at that very moment is skipped (SKIP LOCKED) and caught by the next pass: keep passing
	var m, d int64
	Eventually(t, 5*time.Second, "retention reaches the message and the DLQ entry", func() bool {
		mm, dd, err := e.App.Retention.Apply(bg, app.RetentionPolicy{Messages: time.Millisecond, DeadDeliveries: time.Millisecond}, 100)
		if err != nil {
			t.Fatal(err)
		}
		m, d = m+mm, d+dd
		return m >= 1 && d >= 1
	})
	if m != 1 || d != 1 {
		t.Fatalf("retention touched messages=%d dead=%d, want exactly 1 and 1", m, d)
	}
	msg, _ := e.Repos.Messages.Get(bg, res.MessageID)
	if msg.Recipient != "" || strings.Contains(string(msg.Payload), "sens") || msg.Status != messaging.StatusAccepted && msg.Status != messaging.StatusDelivered {
		t.Errorf("an old message loses its content and keeps its status: %+v", msg)
	}
	if dead, _ := e.Repos.Deliveries.List(bg, e.Tenant, sub, subscription.DeliveryDead, 10); len(dead) != 0 {
		t.Errorf("the DLQ is not kept forever: %d", len(dead))
	}
	// zero keeps everything
	if m, d, err := e.App.Retention.Apply(bg, app.RetentionPolicy{}, 100); err != nil || m+d != 0 {
		t.Errorf("a zero retention keeps data: %d %d %v", m, d, err)
	}
}

// payloadText is the JSON text of a delivery's payload, whatever Go type the store gave it (fmt would print the bytes of a
// json.RawMessage as numbers and make every "does it still mention" check pass for the wrong reason).
func payloadText(d subscription.Delivery) string {
	raw, _ := json.Marshal(d.Event.Payload)
	return string(raw)
}
