package ownership

import (
	"errors"
	"testing"

	"github.com/relayplane/relayplane/internal/core/errs"
)

func TestValidateDispatch_StaleEpochRejected(t *testing.T) { // INV-02 / INV-08 (logic)
	current := Assignment{InstanceID: "i1", NodeID: "node-01", Epoch: 17}
	cases := []struct {
		name string
		cmd  Assignment
		ok   bool
	}{
		{"same", current, true},
		{"older epoch", Assignment{"i1", "node-01", 16}, false},
		{"newer epoch", Assignment{"i1", "node-01", 18}, false},
		{"other node", Assignment{"i1", "node-02", 17}, false},
		{"other instance", Assignment{"i2", "node-01", 17}, false},
	}
	for _, c := range cases {
		err := ValidateDispatch(c.cmd, current)
		if c.ok && err != nil {
			t.Errorf("%s: unexpected error %v", c.name, err)
		}
		if !c.ok && !errors.Is(err, errs.ErrStaleCommand) {
			t.Errorf("%s: want ErrStaleCommand, got %v", c.name, err)
		}
	}
	// released instance (no owner) is always stale
	if err := ValidateDispatch(current, Assignment{InstanceID: "i1", Epoch: 17}); !errors.Is(err, errs.ErrStaleCommand) {
		t.Errorf("released instance must reject commands, got %v", err)
	}
}

func TestValidateClaim(t *testing.T) {
	cur := Assignment{InstanceID: "i1", NodeID: "node-02", Epoch: 3}
	if err := ValidateClaim("node-02", 3, cur); err != nil {
		t.Fatalf("valid claim rejected: %v", err)
	}
	if err := ValidateClaim("node-02", 0, cur); err != nil {
		t.Fatalf("claim without epoch should match on node only: %v", err)
	}
	for _, c := range []struct {
		node  string
		epoch int64
	}{{"node-01", 3}, {"node-02", 2}} {
		if err := ValidateClaim(c.node, c.epoch, cur); !errors.Is(err, errs.ErrOwnershipViolation) {
			t.Errorf("claim %v: want OWNERSHIP_VIOLATION, got %v", c, err)
		}
	}
}

func TestMigrationStateMachine(t *testing.T) {
	happy := []MigrationStep{StepRequested, StepFencingOldOwner, StepOldOwnerFenced,
		StepAssignNewEpoch, StepStartNewOwner, StepVerifyConn, StepConnected}
	for i := 0; i+1 < len(happy); i++ {
		if !CanMigrate(happy[i], happy[i+1]) {
			t.Errorf("%s -> %s must be allowed", happy[i], happy[i+1])
		}
	}
	// failure branch
	if !CanMigrate(StepFencingOldOwner, StepBlocked) || !CanMigrate(StepBlocked, StepFencingOldOwner) {
		t.Error("blocked branch must be reachable and retryable")
	}
	// INV-09: no shortcut around fencing
	illegal := [][2]MigrationStep{
		{StepRequested, StepAssignNewEpoch},
		{StepRequested, StepStartNewOwner},
		{StepFencingOldOwner, StepAssignNewEpoch},
		{StepBlocked, StepAssignNewEpoch},
		{StepBlocked, StepOldOwnerFenced},
		{StepFencingOldOwner, StepStartNewOwner},
		{StepConnected, StepRequested},
	}
	for _, p := range illegal {
		if CanMigrate(p[0], p[1]) {
			t.Errorf("%s -> %s must be rejected", p[0], p[1])
		}
	}
}

func TestCanActivateNewOwner_RequiresFencing(t *testing.T) { // INV-09
	for _, s := range []MigrationStep{StepRequested, StepFencingOldOwner, StepBlocked} {
		if CanActivateNewOwner(s) {
			t.Errorf("new owner must not activate at %s", s)
		}
	}
	for _, s := range []MigrationStep{StepOldOwnerFenced, StepAssignNewEpoch, StepStartNewOwner, StepVerifyConn, StepConnected} {
		if !CanActivateNewOwner(s) {
			t.Errorf("new owner may activate at %s", s)
		}
	}
}

func TestNextEpochMonotonic(t *testing.T) {
	if NextEpoch(0) != 1 || NextEpoch(16) != 17 {
		t.Fatal("epoch must increment by one")
	}
}
