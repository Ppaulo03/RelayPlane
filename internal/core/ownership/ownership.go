// Package ownership holds the ownership, fencing and migration rules.
//
// Invariant: an instance has at most one active owner. Ownership is expressed
// as an Assignment (instance, node, epoch) whose epoch only ever grows.
//
// Two distinct fencing mechanisms exist and must not be conflated:
//
//   - Logical fencing: comparing the epoch carried by a command against the
//     current assignment (ValidateDispatch). Rejects stale *commands*.
//   - Physical fencing: confirming that the previous owner's socket is really
//     closed before a new owner is activated (MigrationStep machine).
package ownership

import (
	"fmt"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
)

// Assignment is a snapshot of who owns an instance, and at which epoch.
type Assignment struct {
	InstanceID string `json:"instance_id"`
	NodeID     string `json:"node_id"`
	Epoch      int64  `json:"epoch"`
}

// IsZero reports whether the assignment carries no owner.
func (a Assignment) IsZero() bool { return a.NodeID == "" && a.Epoch == 0 }

// ValidateDispatch implements logical fencing: a command may only be executed
// when the assignment it was accepted under is exactly the current one.
// Any difference yields ErrStaleCommand, and the provider must not be called.
func ValidateDispatch(cmd, current Assignment) error {
	if current.NodeID == "" || cmd.InstanceID != current.InstanceID ||
		cmd.NodeID != current.NodeID || cmd.Epoch != current.Epoch {
		return fmt.Errorf("%w: command epoch=%d node=%q, current epoch=%d node=%q",
			errs.ErrStaleCommand, cmd.Epoch, cmd.NodeID, current.Epoch, current.NodeID)
	}
	return nil
}

// ValidateClaim checks a webhook/node claim against the catalog. A claim that
// names a different node or epoch than the current assignment is an
// ownership violation (possible split-brain).
func ValidateClaim(claimNode string, claimEpoch int64, current Assignment) error {
	if current.NodeID == "" || claimNode != current.NodeID || (claimEpoch != 0 && claimEpoch != current.Epoch) {
		return fmt.Errorf("%w: claimed node=%q epoch=%d, current node=%q epoch=%d",
			errs.ErrOwnershipViolation, claimNode, claimEpoch, current.NodeID, current.Epoch)
	}
	return nil
}

// NextEpoch returns the epoch of the next assignment.
func NextEpoch(current int64) int64 { return current + 1 }

// ReleaseReason explains why an assignment record was closed.
type ReleaseReason string

const (
	ReleaseMigrated      ReleaseReason = "MIGRATED"
	ReleaseDeleted       ReleaseReason = "DELETED"
	ReleaseCreateFailed  ReleaseReason = "CREATE_FAILED"
	ReleaseAdminReassign ReleaseReason = "ADMIN_REASSIGN"
)

// AssignmentRecord is one row of the append-oriented assignment history.
type AssignmentRecord struct {
	InstanceID    string
	NodeID        string
	Epoch         int64
	AssignedAt    time.Time
	ReleasedAt    *time.Time
	ReleaseReason ReleaseReason
}

// MigrationStep is the explicit, persisted migration state machine.
type MigrationStep string

const (
	StepRequested       MigrationStep = "MIGRATION_REQUESTED"
	StepFencingOldOwner MigrationStep = "FENCING_OLD_OWNER"
	StepBlocked         MigrationStep = "MIGRATION_BLOCKED"
	StepOldOwnerFenced  MigrationStep = "OLD_OWNER_FENCED"
	StepAssignNewEpoch  MigrationStep = "ASSIGN_NEW_EPOCH"
	StepStartNewOwner   MigrationStep = "START_NEW_OWNER"
	StepVerifyConn      MigrationStep = "VERIFY_CONNECTION"
	StepConnected       MigrationStep = "CONNECTED"
)

var migrationTransitions = map[MigrationStep][]MigrationStep{
	StepRequested:       {StepFencingOldOwner, StepBlocked},
	StepFencingOldOwner: {StepOldOwnerFenced, StepBlocked},
	StepBlocked:         {StepFencingOldOwner},
	StepOldOwnerFenced:  {StepAssignNewEpoch},
	StepAssignNewEpoch:  {StepStartNewOwner},
	StepStartNewOwner:   {StepVerifyConn},
	StepVerifyConn:      {StepConnected},
}

// CanMigrate reports whether from -> to is an allowed migration transition.
func CanMigrate(from, to MigrationStep) bool {
	for _, n := range migrationTransitions[from] {
		if n == to {
			return true
		}
	}
	return false
}

// CanActivateNewOwner encodes INV-09: a new owner may only be assigned and
// started once the old owner has been confirmed fenced.
func CanActivateNewOwner(step MigrationStep) bool {
	switch step {
	case StepOldOwnerFenced, StepAssignNewEpoch, StepStartNewOwner, StepVerifyConn, StepConnected:
		return true
	}
	return false
}

// IsTerminal reports whether the migration finished.
func (s MigrationStep) IsTerminal() bool { return s == StepConnected }
