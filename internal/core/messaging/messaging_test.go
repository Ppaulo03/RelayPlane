package messaging

import (
	"errors"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/ownership"
)

func TestStatusMachine(t *testing.T) {
	ok := [][2]Status{
		{StatusQueued, StatusDispatching}, {StatusDispatching, StatusAccepted}, {StatusDispatching, StatusUnknown},
		{StatusDispatching, StatusQueued}, {StatusAccepted, StatusDelivered}, {StatusDelivered, StatusRead},
		{StatusUnknown, StatusAccepted}, {StatusAccepted, StatusRead},
	}
	for _, p := range ok {
		if !CanTransition(p[0], p[1]) {
			t.Errorf("%s -> %s must be allowed", p[0], p[1])
		}
	}
	bad := [][2]Status{
		{StatusRead, StatusDelivered}, {StatusFailed, StatusQueued}, {StatusAccepted, StatusQueued},
		{StatusQueued, StatusAccepted}, {StatusUnknown, StatusQueued}, // UNKNOWN is never auto-retried
		{StatusDelivered, StatusAccepted},
	}
	for _, p := range bad {
		if CanTransition(p[0], p[1]) {
			t.Errorf("%s -> %s must be rejected", p[0], p[1])
		}
	}
}

func TestPolicyHierarchyMostSpecificWins(t *testing.T) {
	global := RatePolicy{MinInterval: 1500 * time.Millisecond, Burst: 1, MaxPerMinute: 30, MaxConcurrent: 1, Cooldown: time.Minute}
	tenant := &RatePolicy{MinInterval: time.Second, MaxPerMinute: 60}
	inst := &RatePolicy{MinInterval: 200 * time.Millisecond}
	got := ResolvePolicy(global, tenant, inst)
	want := RatePolicy{MinInterval: 200 * time.Millisecond, Burst: 1, MaxPerMinute: 60, MaxConcurrent: 1, Cooldown: time.Minute}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if ResolvePolicy(global, nil, nil) != global {
		t.Error("no overrides must yield the global policy")
	}
	if ResolvePolicy(global, tenant, nil).MinInterval != time.Second {
		t.Error("tenant overrides global")
	}
}

func TestRetrySchedule(t *testing.T) {
	r := DefaultRetrySchedule
	want := []time.Duration{0, 5 * time.Second, 30 * time.Second, 2 * time.Minute}
	for i, w := range want {
		d, ok := r.Next(i + 1)
		if !ok || d != w {
			t.Errorf("attempt %d: got %v,%v want %v", i+1, d, ok, w)
		}
	}
	if _, ok := r.Next(5); ok {
		t.Error("attempt 5 must dead-letter")
	}
	if r.MaxAttempts() != 5 {
		t.Errorf("max attempts %d", r.MaxAttempts())
	}
}

func validEnvelope() Envelope {
	return Envelope{
		MessageID: "msg_1", TenantID: "t1", InstanceID: "inst_1", PartitionKey: "inst_1",
		Assignment: ownership.Assignment{InstanceID: "inst_1", NodeID: "node-01", Epoch: 1},
		Type:       TypeText, To: "5562999999999", Payload: Payload{Text: "hi"},
	}
}

func TestEnvelopeValidate(t *testing.T) {
	if err := validEnvelope().Validate(); err != nil {
		t.Fatalf("valid envelope: %v", err)
	}
	cases := map[string]func(*Envelope){
		"partition != instance": func(e *Envelope) { e.PartitionKey = "other" },
		"missing assignment":    func(e *Envelope) { e.Assignment = ownership.Assignment{} },
		"empty text":            func(e *Envelope) { e.Payload.Text = "" },
		"media on text":         func(e *Envelope) { e.Payload.Media = &media.Ref{} },
		"document w/o media":    func(e *Envelope) { e.Type = TypeDocument },
		"no recipient":          func(e *Envelope) { e.To = "" },
		"bad type":              func(e *Envelope) { e.Type = "sticker" },
	}
	for name, mut := range cases {
		e := validEnvelope()
		mut(&e)
		if err := e.Validate(); !errors.Is(err, errs.ErrInvalidArgument) {
			t.Errorf("%s: want ErrInvalidArgument, got %v", name, err)
		}
	}
}
