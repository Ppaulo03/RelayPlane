// Package reconciliation contains the pure decision logic that compares an
// instance's desired_state with what the provider reports (observed_state).
//
// Decide performs no I/O; the reconciler service gathers the observation,
// calls Decide and executes the resulting Action against ports.
package reconciliation

import (
	"errors"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
)

// ActionKind is what the reconciler should do about one instance.
type ActionKind string

const (
	ActionNone           ActionKind = "NONE"
	ActionUpdateObserved ActionKind = "UPDATE_OBSERVED" // record provider truth
	ActionConnect        ActionKind = "CONNECT"         // ask the provider to (re)connect
	ActionDisconnect     ActionKind = "DISCONNECT"      // desired DISCONNECTED but still connected
	ActionDelete         ActionKind = "DELETE"          // desired DELETED, provider session still exists
	ActionFinalizeDelete ActionKind = "FINALIZE_DELETE" // provider session gone: release assignment
	ActionCreate         ActionKind = "CREATE"          // resume interrupted provisioning
)

// Observation is the result of asking the provider about one instance.
type Observation struct {
	State instance.ObservedState
	Err   error // canonical error from the provider, if the query failed
}

// Policy tunes reconciliation.
type Policy struct {
	ConnectGrace     time.Duration // do not fight the provider's own reconnect right after a drop
	CreateStuckAfter time.Duration // ALLOCATING/CREATING older than this is resumed
}

// DefaultPolicy returns the default tuning.
func DefaultPolicy() Policy {
	return Policy{ConnectGrace: 10 * time.Second, CreateStuckAfter: 30 * time.Second}
}

// Decision is the outcome of Decide.
type Decision struct {
	Action   ActionKind
	Observed instance.ObservedState // new observed state to record (when set)
	Drift    bool                   // desired != observed before acting
	Reason   string
}

// Input groups Decide's parameters.
type Input struct {
	Instance        instance.Instance
	Observation     Observation
	MigrationActive bool // a non-blocked migration currently drives this instance
	Now             time.Time
	Policy          Policy
}

// Decide computes the reconciliation action. Provider unavailability never
// changes the observed state: a node outage is a node problem (INV-10) and
// the first version does not fail sessions over.
func Decide(in Input) Decision {
	inst, obs := in.Instance, in.Observation
	d := Decision{Action: ActionNone, Drift: inst.Drifted()}

	if inst.DeletedAt != nil || inst.ObservedState == instance.Deleted {
		d.Drift = false
		return d
	}
	if in.MigrationActive { // independent of observed_state: it lags the operation row
		d.Reason = "migration in progress"
		return d
	}

	// Provider query failed.
	if obs.Err != nil {
		switch {
		case errors.Is(obs.Err, errs.ErrInstanceNotFound):
			return decideMissing(in, d)
		default:
			d.Reason = "provider query failed: " + obs.Err.Error()
			return d
		}
	}

	// Provider confirmed an existing session.
	switch inst.DesiredState {
	case instance.DesiredDeleted:
		d.Action = ActionDelete
		d.Reason = "desired DELETED but provider session exists"
		return d

	case instance.DesiredDisconnected:
		switch obs.State {
		case instance.Connected, instance.Connecting, instance.Reconnecting:
			d.Action, d.Reason = ActionDisconnect, "desired DISCONNECTED but session is "+string(obs.State)
			return d
		}

	case instance.DesiredConnected:
		if obs.State == instance.Disconnected &&
			in.Now.Sub(inst.LastStatusChange) >= in.Policy.ConnectGrace &&
			inst.ObservedState != instance.Creating {
			d.Action, d.Observed, d.Reason = ActionConnect, obs.State, "desired CONNECTED but session is DISCONNECTED"
			return d
		}
	}

	if obs.State.Valid() && obs.State != inst.ObservedState {
		d.Action, d.Observed, d.Reason = ActionUpdateObserved, obs.State, "provider state differs from catalog"
	}
	return d
}

func decideMissing(in Input, d Decision) Decision {
	inst := in.Instance
	switch {
	case inst.DesiredState == instance.DesiredDeleted:
		d.Action, d.Observed, d.Reason = ActionFinalizeDelete, instance.Deleted, "provider session already gone"
	case inst.ObservedState == instance.Allocating || inst.ObservedState == instance.Creating:
		if in.Now.Sub(inst.UpdatedAt) >= in.Policy.CreateStuckAfter {
			d.Action, d.Reason = ActionCreate, "provisioning interrupted: provider instance missing"
		} else {
			d.Reason = "provisioning in progress"
		}
	case inst.ObservedState == instance.Failed:
		d.Reason = "failed instance has no provider session"
	default:
		// The catalog believes a live session exists but the provider lost it.
		// Do not silently recreate (it would need re-pairing): surface FAILED.
		d.Action, d.Observed, d.Reason = ActionUpdateObserved, instance.Failed, "provider no longer knows this instance"
	}
	return d
}
