// Package instance models a messaging session and its lifecycle.
//
// desired_state (intent) and observed_state (what the provider reports) are
// kept strictly apart; a difference between them is what the reconciler
// converges. Provider-node health lives in core/routing and is independent.
package instance

import (
	"fmt"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
)

// DesiredState is the intent recorded by the control plane.
type DesiredState string

const (
	DesiredConnected    DesiredState = "CONNECTED"
	DesiredDisconnected DesiredState = "DISCONNECTED"
	DesiredDeleted      DesiredState = "DELETED"
)

// Valid reports whether s is a known desired state.
func (s DesiredState) Valid() bool {
	switch s {
	case DesiredConnected, DesiredDisconnected, DesiredDeleted:
		return true
	}
	return false
}

// ObservedState is the lifecycle state as confirmed by the provider.
type ObservedState string

const (
	Allocating      ObservedState = "ALLOCATING"
	Creating        ObservedState = "CREATING"
	AwaitingPairing ObservedState = "AWAITING_PAIRING"
	Connecting      ObservedState = "CONNECTING"
	Connected       ObservedState = "CONNECTED"
	Disconnected    ObservedState = "DISCONNECTED"
	Reconnecting    ObservedState = "RECONNECTING"
	LoggedOut       ObservedState = "LOGGED_OUT"
	Migrating       ObservedState = "MIGRATING"
	Deleting        ObservedState = "DELETING"
	Deleted         ObservedState = "DELETED" // terminal: provider session removed, assignment released
	Failed          ObservedState = "FAILED"
)

// Valid reports whether s is a known observed state.
func (s ObservedState) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// live states are interchangeable by observation: the provider decides.
var live = []ObservedState{AwaitingPairing, Connecting, Connected, Disconnected, Reconnecting, LoggedOut}

var transitions = buildTransitions()

func buildTransitions() map[ObservedState]map[ObservedState]bool {
	t := map[ObservedState]map[ObservedState]bool{}
	add := func(from ObservedState, to ...ObservedState) {
		if t[from] == nil {
			t[from] = map[ObservedState]bool{}
		}
		for _, x := range to {
			t[from][x] = true
		}
	}
	add(Allocating, Creating, Failed, Deleting)
	add(Creating, AwaitingPairing, Connecting, Connected, Failed, Deleting)
	for _, l := range live {
		add(l, live...)
		add(l, Migrating, Deleting, Failed)
	}
	add(Migrating, append([]ObservedState{Failed, Deleting}, live...)...)
	add(Deleting, Deleted, Failed)
	add(Failed, append([]ObservedState{Deleting}, live...)...) // provider truth may resurrect a FAILED entry
	t[Deleted] = map[ObservedState]bool{}
	return t
}

// CanTransition reports whether the observed lifecycle may move from -> to.
// Staying in the same state is always allowed (idempotent observation).
func CanTransition(from, to ObservedState) bool {
	if from == to {
		_, ok := transitions[from]
		return ok
	}
	return transitions[from][to]
}

// IsLive reports whether the state describes an existing provider session.
func (s ObservedState) IsLive() bool {
	for _, l := range live {
		if l == s {
			return true
		}
	}
	return false
}

// Instance is the catalog entry for one messaging session.
type Instance struct {
	ID                    string
	TenantID              string
	Name                  string
	Provider              string
	ProviderInstanceID    string
	NodeID                string // empty when no owner holds the instance
	AssignmentEpoch       int64
	DesiredState          DesiredState
	ObservedState         ObservedState
	LastProviderHeartbeat time.Time
	LastStatusChange      time.Time
	RatePolicy            *messaging.RatePolicy
	ReconciledAt          time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	DeletedAt             *time.Time
}

// Assignment returns the current ownership snapshot.
func (i Instance) Assignment() ownership.Assignment {
	return ownership.Assignment{InstanceID: i.ID, NodeID: i.NodeID, Epoch: i.AssignmentEpoch}
}

// Drifted reports whether desired and observed disagree (reconciliation needed).
func (i Instance) Drifted() bool {
	switch i.DesiredState {
	case DesiredConnected:
		return i.ObservedState != Connected
	case DesiredDisconnected:
		return i.ObservedState == Connected || i.ObservedState == Connecting || i.ObservedState == Reconnecting
	case DesiredDeleted:
		return i.ObservedState != Deleted
	}
	return false
}

// TransitionObserved validates and applies an observed-state change.
func (i *Instance) TransitionObserved(to ObservedState, at time.Time) error {
	if !CanTransition(i.ObservedState, to) {
		return fmt.Errorf("%w: %s -> %s", errs.ErrInvalidTransition, i.ObservedState, to)
	}
	if i.ObservedState != to {
		i.LastStatusChange = at
	}
	i.ObservedState = to
	i.UpdatedAt = at
	return nil
}

// OperationType enumerates long-running operations tracked in `operations`.
type OperationType string

const (
	OpCreateInstance OperationType = "CREATE_INSTANCE"
	OpDeleteInstance OperationType = "DELETE_INSTANCE"
	OpReconnect      OperationType = "RECONNECT"
	OpLogout         OperationType = "LOGOUT"
	OpMigrate        OperationType = "MIGRATE"
)

// OperationStatus is the coarse operation status exposed through the API.
type OperationStatus string

const (
	OpPending   OperationStatus = "PENDING"
	OpRunning   OperationStatus = "RUNNING"
	OpBlocked   OperationStatus = "BLOCKED"
	OpSucceeded OperationStatus = "SUCCEEDED"
	OpFailed    OperationStatus = "FAILED"
)

// IsActive reports whether the operation may still make progress.
func (s OperationStatus) IsActive() bool {
	return s == OpPending || s == OpRunning || s == OpBlocked
}

// Operation tracks an asynchronous operation. Step carries the fine-grained
// state (for migrations: the ownership.MigrationStep) and is advanced with
// compare-and-set so concurrent drivers cannot both move the same step.
type Operation struct {
	ID           string
	TenantID     string
	InstanceID   string
	Type         OperationType
	Status       OperationStatus
	Step         string
	TargetNodeID string // migrations: destination node
	SourceNodeID string // migrations: node being fenced
	SourceEpoch  int64
	ErrorCode    string
	ErrorMessage string
	Attempts     int
	CreatedAt    time.Time
	UpdatedAt    time.Time
	// StepStartedAt is when the current Step began (phase timeouts use it).
	StepStartedAt time.Time
	CompletedAt   *time.Time
}
