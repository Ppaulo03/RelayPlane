package memory

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// FakePresence and FakeRead are what the fake provider was asked to show or mark.
type FakePresence struct {
	Assignment ownership.Assignment
	To         string
	State      ports.PresenceState
	Duration   time.Duration
}

type FakeRead struct {
	Assignment ownership.Assignment
	Chat       string
	IDs        []string
}

var (
	_ ports.PresenceSender = (*FakeProvider)(nil)
	_ ports.ReadMarker     = (*FakeProvider)(nil)
)

// SendPresence records the request.
func (f *FakeProvider) SendPresence(_ context.Context, a ownership.Assignment, to string, st ports.PresenceState, d time.Duration) error {
	f.enter("SendPresence", a)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.presences = append(f.presences, FakePresence{Assignment: a, To: to, State: st, Duration: d})
	return f.injected(false)
}

// MarkRead records the request.
func (f *FakeProvider) MarkRead(_ context.Context, a ownership.Assignment, chat string, ids []string) error {
	f.enter("MarkRead", a)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads = append(f.reads, FakeRead{Assignment: a, Chat: chat, IDs: append([]string(nil), ids...)})
	return f.injected(false)
}

// Presences and Reads return what was requested (tests).
func (f *FakeProvider) Presences() []FakePresence {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakePresence(nil), f.presences...)
}

func (f *FakeProvider) Reads() []FakeRead {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeRead(nil), f.reads...)
}
