package systemtest

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// ---- R08: a reply can quote a message ----

func sendReply(e *Env, instID string, reply *app.SendReplyTo, key string) (app.SendResult, error) {
	res, _, err := e.App.Messages.Send(bg, e.Tenant, app.SendInput{InstanceID: instID, To: "5562999999999", Type: messaging.TypeText,
		Payload: app.SendPayload{Text: "Confirma amanhã às 15h?", ReplyTo: reply}}, key)
	return res, err
}

func lastSent(t *testing.T, e *Env) messaging.OutboundMessage {
	t.Helper()
	sent := e.Provider.Sent()
	if len(sent) == 0 {
		t.Fatal("nothing reached the provider")
	}
	return sent[len(sent)-1].Message
}

func TestReply_QuotesTheUsersMessageWithItsText(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.StartOutbox()
	e.StartWorkers(1)
	e.StartProjector()

	res, err := sendReply(e, inst.ID, &app.SendReplyTo{ProviderMessageID: "3EB0USERMSG", Text: "quero agendar uma hora"}, "k1")
	if err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(res.MessageID, messaging.StatusAccepted)
	got := lastSent(t, e)
	if got.ReplyTo == nil || got.ReplyTo.ProviderMessageID != "3EB0USERMSG" || got.ReplyTo.Text != "quero agendar uma hora" || got.ReplyTo.FromMe {
		t.Fatalf("the provider must be asked to quote the user's message with its text: %+v", got.ReplyTo)
	}
	// a plain message quotes nothing
	plain, _, _ := e.SendText(e.Tenant, inst.ID, "oi", "k2")
	e.WaitMessage(plain.MessageID, messaging.StatusAccepted)
	if lastSent(t, e).ReplyTo != nil {
		t.Error("a plain message quotes nothing")
	}
}

func TestReply_QuotesOneOfOurOwnMessagesById(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	e.StartOutbox()
	e.StartWorkers(1)
	e.StartProjector()

	first, _, err := e.SendText(e.Tenant, inst.ID, "pergunta original", "k1")
	if err != nil {
		t.Fatal(err)
	}
	accepted := e.WaitMessage(first.MessageID, messaging.StatusAccepted)

	res, err := sendReply(e, inst.ID, &app.SendReplyTo{MessageID: first.MessageID}, "k2")
	if err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(res.MessageID, messaging.StatusAccepted)
	got := lastSent(t, e)
	if got.ReplyTo == nil || got.ReplyTo.ProviderMessageID != accepted.ProviderMessageID || got.ReplyTo.Text != "pergunta original" || !got.ReplyTo.FromMe {
		t.Fatalf("our own message is quoted by its provider id, text and from_me: %+v", got.ReplyTo)
	}
	// another tenant's message is indistinguishable from a missing one
	if _, _, err := e.App.Messages.Send(bg, e.Tenant2, app.SendInput{InstanceID: e.CreateInstance(e.Tenant2, "b", true).ID, To: "5562999999999", Type: messaging.TypeText,
		Payload: app.SendPayload{Text: "x", ReplyTo: &app.SendReplyTo{MessageID: first.MessageID}}}, ""); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("another tenant's message: %v", err)
	}
}

func TestReply_Validation(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	queued, _, err := e.SendText(e.Tenant, inst.ID, "ainda na fila", "kq") // no workers: stays QUEUED
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		reply *app.SendReplyTo
		want  error
	}{
		"both ids": {&app.SendReplyTo{ProviderMessageID: "x", MessageID: "y"}, errs.ErrInvalidArgument},
		"neither":  {&app.SendReplyTo{Text: "just a text"}, errs.ErrInvalidArgument},
		"a message not yet accepted has no provider id": {&app.SendReplyTo{MessageID: queued.MessageID}, errs.ErrConflict},
		"unknown message":  {&app.SendReplyTo{MessageID: "msg_nope"}, errs.ErrNotFound},
		"huge provider id": {&app.SendReplyTo{ProviderMessageID: strings.Repeat("x", 200)}, errs.ErrInvalidArgument},
	} {
		if _, err := sendReply(e, inst.ID, c.reply, ""); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// the same Idempotency-Key with another quote is a different request
	if _, err := sendReply(e, inst.ID, &app.SendReplyTo{ProviderMessageID: "A"}, "same"); err != nil {
		t.Fatal(err)
	}
	if _, err := sendReply(e, inst.ID, &app.SendReplyTo{ProviderMessageID: "B"}, "same"); !errors.Is(err, errs.ErrIdempotencyConflict) {
		t.Errorf("a key reused with another quote: %v", err)
	}
}

// ---- R07: typing indicator and read receipts ----

func TestPresence_ShowsTypingThroughTheProvider(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)

	if err := e.App.Channel.SendPresence(bg, e.Tenant, inst.ID, "5562988887777", ports.PresenceComposing, 4*time.Second); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 5*time.Second, "the provider is asked to show typing", func() bool { return len(e.Provider.Presences()) == 1 })
	p := e.Provider.Presences()[0]
	if p.To != "5562988887777" || p.State != ports.PresenceComposing || p.Duration != 4*time.Second || p.Assignment.InstanceID != inst.ID {
		t.Errorf("presence: %+v", p)
	}
	// default duration, and "paused" has none
	if err := e.App.Channel.SendPresence(bg, e.Tenant, inst.ID, "5562988887777", ports.PresenceRecording, 0); err != nil {
		t.Fatal(err)
	}
	if err := e.App.Channel.SendPresence(bg, e.Tenant, inst.ID, "5562988887777", ports.PresencePaused, 9*time.Second); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 5*time.Second, "all three reach the provider", func() bool { return len(e.Provider.Presences()) == 3 })
	got := map[ports.PresenceState]time.Duration{}
	for _, p := range e.Provider.Presences() {
		got[p.State] = p.Duration
	}
	if got[ports.PresenceRecording] != app.DefaultPresence || got[ports.PresencePaused] != 0 {
		t.Errorf("durations: %v", got)
	}

	for name, c := range map[string]struct {
		to       string
		state    ports.PresenceState
		dur      time.Duration
		instance string
		tenant   string
		want     error
	}{
		"bad state":    {"5562988887777", "dancing", 0, inst.ID, e.Tenant, errs.ErrInvalidArgument},
		"bad number":   {"x", ports.PresenceComposing, 0, inst.ID, e.Tenant, errs.ErrInvalidArgument},
		"too long":     {"5562988887777", ports.PresenceComposing, 2 * app.MaxPresence, inst.ID, e.Tenant, errs.ErrInvalidArgument},
		"other tenant": {"5562988887777", ports.PresenceComposing, 0, inst.ID, e.Tenant2, errs.ErrNotFound},
		"unknown":      {"5562988887777", ports.PresenceComposing, 0, "inst_nope", e.Tenant, errs.ErrNotFound},
	} {
		if err := e.App.Channel.SendPresence(bg, c.tenant, c.instance, c.to, c.state, c.dur); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// an instance that is not connected cannot type
	e.Provider.SetState(inst.ID, instance.Disconnected)
	_, _ = e.Repos.Instances.SetObserved(bg, inst.ID, inst.AssignmentEpoch, instance.Disconnected, time.Now())
	if err := e.App.Channel.SendPresence(bg, e.Tenant, inst.ID, "5562988887777", ports.PresenceComposing, 0); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("not connected: %v", err)
	}
}

// Typing indicators are best effort and cheap for the caller, but a provider that hangs must not pile up calls.
func TestPresence_ASlowProviderCannotPileUpCalls(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	release := make(chan struct{})
	var once sync.Once
	e.Provider.BeforeCall = func(method string, _ ownership.Assignment) {
		if method == "SendPresence" {
			<-release
		}
	}
	defer once.Do(func() { close(release) })
	for i := 0; i < app.MaxPresencePerInstance; i++ {
		if err := e.App.Channel.SendPresence(bg, e.Tenant, inst.ID, "5562988887777", ports.PresenceComposing, time.Second); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
	if err := e.App.Channel.SendPresence(bg, e.Tenant, inst.ID, "5562988887777", ports.PresenceComposing, time.Second); !errors.Is(err, errs.ErrRateLimited) {
		t.Errorf("beyond the ceiling: %v", err)
	}
	once.Do(func() { close(release) })
	Eventually(t, 5*time.Second, "the slots free up", func() bool {
		return e.App.Channel.SendPresence(bg, e.Tenant, inst.ID, "5562988887777", ports.PresenceComposing, time.Second) == nil
	})
}

func TestMarkRead_MarksTheContactsMessages(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	if err := e.App.Channel.MarkRead(bg, e.Tenant, inst.ID, "5562988887777", []string{"WA-1", "WA-2"}); err != nil {
		t.Fatal(err)
	}
	reads := e.Provider.Reads()
	if len(reads) != 1 || reads[0].Chat != "5562988887777" || len(reads[0].IDs) != 2 || reads[0].Assignment.InstanceID != inst.ID {
		t.Fatalf("reads: %+v", reads)
	}
	many := make([]string, app.MaxReadBatch+1)
	for i := range many {
		many[i] = "WA"
	}
	for name, c := range map[string]struct {
		tenant, chat string
		ids          []string
		want         error
	}{
		"none":         {e.Tenant, "5562988887777", nil, errs.ErrInvalidArgument},
		"too many":     {e.Tenant, "5562988887777", many, errs.ErrInvalidArgument},
		"empty id":     {e.Tenant, "5562988887777", []string{""}, errs.ErrInvalidArgument},
		"bad chat":     {e.Tenant, "x", []string{"WA"}, errs.ErrInvalidArgument},
		"other tenant": {e.Tenant2, "5562988887777", []string{"WA"}, errs.ErrNotFound},
	} {
		if err := e.App.Channel.MarkRead(bg, c.tenant, inst.ID, c.chat, c.ids); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// a provider failure reaches the caller (it is synchronous)
	e.Provider.FailNext(memory.FailUnavailable)
	if err := e.App.Channel.MarkRead(bg, e.Tenant, inst.ID, "5562988887777", []string{"WA-3"}); !errors.Is(err, errs.ErrProviderUnavailable) {
		t.Errorf("provider down: %v", err)
	}
}
