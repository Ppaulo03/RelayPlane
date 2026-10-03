package instance

import (
	"errors"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
)

func TestProvisioningTransitions(t *testing.T) {
	ok := [][2]ObservedState{
		{Allocating, Creating}, {Creating, AwaitingPairing}, {AwaitingPairing, Connecting},
		{Connecting, Connected}, {Connected, Disconnected}, {Disconnected, Reconnecting},
		{Reconnecting, Connected}, {Connected, Migrating}, {Migrating, Connected},
		{Connected, Deleting}, {Deleting, Deleted}, {Creating, Failed}, {Failed, Deleting},
		{Connected, Connected},
	}
	for _, p := range ok {
		if !CanTransition(p[0], p[1]) {
			t.Errorf("%s -> %s must be allowed", p[0], p[1])
		}
	}
	bad := [][2]ObservedState{
		{Allocating, Connected}, {Deleted, Connected}, {Deleted, Creating},
		{Deleting, Connected}, {Connected, Creating}, {Connected, Allocating},
		{Allocating, AwaitingPairing},
	}
	for _, p := range bad {
		if CanTransition(p[0], p[1]) {
			t.Errorf("%s -> %s must be rejected", p[0], p[1])
		}
	}
}

func TestTransitionObserved(t *testing.T) {
	now := time.Now()
	i := Instance{ObservedState: Creating}
	if err := i.TransitionObserved(Connected, now); err != nil {
		t.Fatal(err)
	}
	if i.LastStatusChange != now {
		t.Error("status change time must be recorded")
	}
	prev := i.LastStatusChange
	_ = i.TransitionObserved(Connected, now.Add(time.Hour))
	if i.LastStatusChange != prev {
		t.Error("no-op transition must not move last_status_change")
	}
	if err := i.TransitionObserved(Allocating, now); !errors.Is(err, errs.ErrInvalidTransition) {
		t.Errorf("want ErrInvalidTransition, got %v", err)
	}
}

func TestDrift_DesiredAndObservedAreIndependent(t *testing.T) { // INV-12 prerequisite
	i := Instance{DesiredState: DesiredConnected, ObservedState: Disconnected}
	if !i.Drifted() {
		t.Error("desired CONNECTED vs observed DISCONNECTED must signal drift")
	}
	i.ObservedState = Connected
	if i.Drifted() {
		t.Error("converged instance must not drift")
	}
	i = Instance{DesiredState: DesiredDisconnected, ObservedState: Connected}
	if !i.Drifted() {
		t.Error("desired DISCONNECTED vs observed CONNECTED drifts")
	}
	i = Instance{DesiredState: DesiredDeleted, ObservedState: Connected}
	if !i.Drifted() {
		t.Error("desired DELETED vs observed CONNECTED drifts")
	}
}
