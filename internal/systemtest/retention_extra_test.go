package systemtest

import (
	"strings"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
)

// UNKNOWN can wait months for a decision; its recipient and text must not outlive the retention just because of that.
func TestRetention_AnUnknownMessageLosesItsContentToo(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.StartOutbox()
	e.StartWorkers(1)
	e.StartProjector()
	e.Provider.FailNext(memory.FailAmbiguous)
	res, _, err := e.SendText(e.Tenant, inst.ID, "texto sensivel", "k1")
	if err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(res.MessageID, messaging.StatusUnknown)
	time.Sleep(20 * time.Millisecond)
	Eventually(t, 5*time.Second, "retention reaches the UNKNOWN message", func() bool {
		if _, _, err := e.App.Retention.Apply(bg, app.RetentionPolicy{Messages: time.Millisecond}, 100); err != nil {
			t.Fatal(err)
		}
		m, _ := e.Repos.Messages.Get(bg, res.MessageID)
		return m.Recipient == ""
	})
	m, _ := e.Repos.Messages.Get(bg, res.MessageID)
	if m.Status != messaging.StatusUnknown || strings.Contains(string(m.Payload), "sensivel") || m.SequenceNo == 0 {
		t.Errorf("it keeps its status and place in the ledger, nothing personal: %+v", m)
	}
}

// A subscription left paused accumulates events with the user's text: they have a retention too.
func TestRetention_DeliveriesWaitingOnAPausedSubscriptionDoNotStayForever(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subID, _ := subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.StartWebhooks()
	if err := e.App.Subscriptions.Pause(bg, e.Tenant, subID); err != nil {
		t.Fatal(err)
	}
	f := &mediaFixture{t: t, e: e, inst: inst}
	f.receiveFrom(erasedNumber, "WA-P1", "mensagem parada", nil)
	Eventually(t, 10*time.Second, "the event waits for the paused subscription", func() bool {
		bl, _ := e.App.Subscriptions.Backlog(bg, e.Tenant)
		return bl[subID].Pending == 1
	})
	time.Sleep(20 * time.Millisecond)
	if _, d, err := e.App.Retention.Apply(bg, app.RetentionPolicy{PendingDeliveries: time.Hour}, 100); err != nil || d != 0 {
		t.Fatalf("nothing is old enough: %d %v", d, err)
	}
	if _, d, err := e.App.Retention.Apply(bg, app.RetentionPolicy{PendingDeliveries: time.Millisecond}, 100); err != nil || d != 1 {
		t.Fatalf("the stale pending delivery is deleted: %d %v", d, err)
	}
	if bl, _ := e.App.Subscriptions.Backlog(bg, e.Tenant); bl[subID].Pending != 0 {
		t.Errorf("nothing is left waiting: %+v", bl[subID])
	}
}
