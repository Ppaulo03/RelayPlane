package systemtest

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// reassignBehindTheLocksBack moves ownership with the repository directly, the
// way it would happen if a lease had been lost. It models the worst case the
// lock cannot exclude, to prove the re-read guard still protects the provider.
func reassignBehindTheLocksBack(t *testing.T, e *Env, inst *instance.Instance, target string) {
	t.Helper()
	opID := "op_bypass_" + inst.ID
	if err := e.Repos.Operations.Create(bg, instance.Operation{ID: opID, TenantID: inst.TenantID, InstanceID: inst.ID, Type: instance.OpMigrate,
		Status: instance.OpRunning, Step: string(ownership.StepRequested)}); err != nil {
		t.Error(err)
		return
	}
	for _, step := range []ownership.MigrationStep{ownership.StepFencingOldOwner, ownership.StepOldOwnerFenced} {
		cur, _ := e.Repos.Operations.Get(bg, opID)
		if _, err := e.Repos.Operations.Advance(bg, opID, cur.Step, string(step), instance.OpRunning, ports.OperationPatch{}); err != nil {
			t.Error(err)
			return
		}
	}
	if _, err := e.Repos.Instances.Reassign(bg, ports.ReassignRequest{InstanceID: inst.ID, ExpectedEpoch: inst.AssignmentEpoch,
		NewNodeID: target, OperationID: opID, Reason: ownership.ReleaseMigrated}); err != nil {
		t.Error(err)
		return
	}
	_ = e.Repos.Operations.Complete(bg, opID, instance.OpSucceeded, "", "", time.Now())
}

// 9.1 The reconciler decides at epoch N, the assignment moves to N+1: the old action must be dropped.
func TestLifecycle_ReconcilerNeverActsOnAStaleAssignment(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	other := "node-02"
	if inst.NodeID == other {
		other = "node-01"
	}
	e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Disconnected) // drift: the reconciler wants to reconnect
	var once sync.Once
	e.Provider.BeforeCall = func(method string, a ownership.Assignment) {
		if method == "GetInstanceState" {
			once.Do(func() { reassignBehindTheLocksBack(t, e, inst, other) }) // ownership changes mid-reconciliation
		}
	}
	if _, _, err := e.Reconciler.ReconcileInstance(bg, inst.ID); err != nil {
		t.Fatal(err)
	}
	for _, c := range e.Provider.Calls() {
		if c.Method == "ConnectInstance" || c.Method == "Disconnect" || c.Method == "DeleteInstance" {
			t.Fatalf("the reconciler executed %s on a stale assignment %+v", c.Method, c.Assignment)
		}
	}
	if testutilCounter(e, "relayplane_assignment_epoch_mismatch_total") < 1 {
		t.Error("the dropped action should be visible in the epoch mismatch metric")
	}
}

// While a migration owns the instance the reconciler does nothing at all.
func TestLifecycle_ReconcilerSkipsInstanceWhileMigrationHoldsTheLock(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	inFence, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	e.Provider.BeforeCall = func(method string, a ownership.Assignment) {
		if method == "Disconnect" {
			once.Do(func() { close(inFence); <-release })
		}
	}
	res, _, err := e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
	if err != nil {
		t.Fatal(err)
	}
	<-inFence // the migration is inside provider.Disconnect, holding instance-control
	e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Disconnected)
	before := len(e.Provider.Calls())
	drifted, acted, err := e.Reconciler.ReconcileInstance(bg, inst.ID)
	if err != nil || drifted || acted {
		t.Fatalf("the reconciler must skip a busy instance: %v %v %v", drifted, acted, err)
	}
	if calls := e.Provider.Calls()[before:]; len(calls) != 0 {
		t.Fatalf("the reconciler called the provider during a migration: %+v", calls)
	}
	close(release)
	waitMigration(t, e, res.OperationID, instance.OpSucceeded, instance.OpRunning)
}

// 9.2 DELETE x MIGRATE: exactly one lifecycle mutation progresses.
func TestLifecycle_DeleteAndMigrateNeverBothProgress(t *testing.T) {
	for round := 0; round < 25; round++ {
		e := NewEnv(t)
		inst := e.CreateInstance(e.Tenant, "a", true)
		var wg sync.WaitGroup
		var migErr, delErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, migErr = e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
		}()
		go func() { defer wg.Done(); _, _, delErr = e.App.Instances.Delete(bg, e.Tenant, inst.ID, "") }()
		wg.Wait()
		if migErr == nil && delErr == nil {
			t.Fatalf("round %d: both DELETE and MIGRATE were accepted", round)
		}
		for _, err := range []error{migErr, delErr} {
			if err != nil && !errors.Is(err, errs.ErrConflict) && !errors.Is(err, errs.ErrNotFound) && !errors.Is(err, errs.ErrInProgress) {
				t.Fatalf("round %d: unexpected error %v", round, err)
			}
		}
		if migErr == nil { // the migration won: nothing may have been deleted
			cur, _ := e.Repos.Instances.Get(bg, inst.ID)
			if cur.DesiredState == instance.DesiredDeleted || cur.DeletedAt != nil {
				t.Fatalf("round %d: instance deleted although the migration was accepted", round)
			}
		} else if _, err := e.Repos.Operations.FindActive(bg, inst.ID, instance.OpMigrate); err == nil {
			t.Fatalf("round %d: a migration is active on a deleted instance", round)
		}
		e.cancel()
		e.wg.Wait()
	}
}

// 9.3 RECONNECT x MIGRATE: no provider action may use a stale assignment after the fence.
func TestLifecycle_ReconnectNeverUsesAStaleAssignmentAfterFencing(t *testing.T) {
	for round := 0; round < 25; round++ {
		e := NewEnv(t)
		inst := e.CreateInstance(e.Tenant, "a", true)
		e.Provider.SetStateOn(inst.NodeID, inst.ID, instance.Disconnected)
		_, _ = e.Repos.Instances.SetObserved(bg, inst.ID, inst.AssignmentEpoch, instance.Disconnected, time.Now())
		var wg sync.WaitGroup
		var migErr, recErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, migErr = e.App.Migrations.Start(bg, e.Tenant, inst.ID, app.MigrateInput{}, "")
		}()
		go func() { defer wg.Done(); _, recErr = e.App.Instances.Reconnect(bg, e.Tenant, inst.ID) }()
		wg.Wait()
		if migErr != nil && !errors.Is(migErr, errs.ErrConflict) {
			t.Fatalf("round %d: migrate: %v", round, migErr)
		}
		if recErr != nil && !errors.Is(recErr, errs.ErrConflict) {
			t.Fatalf("round %d: reconnect: %v", round, recErr)
		}
		if migErr == nil {
			Eventually(t, 5*time.Second, "old owner fenced", func() bool {
				if op, err := e.Repos.Operations.FindActive(bg, inst.ID, instance.OpMigrate); err == nil {
					_ = e.App.Migrations.Drive(bg, op.ID)
				}
				for _, c := range e.Provider.Calls() {
					if c.Method == "Disconnect" {
						return true
					}
				}
				return false
			})
		}
		fenced := false
		for _, c := range e.Provider.Calls() {
			if c.Method == "Disconnect" && c.Assignment.Epoch == inst.AssignmentEpoch {
				fenced = true
			}
			if fenced && c.Method == "ConnectInstance" && c.Assignment.Epoch == inst.AssignmentEpoch {
				t.Fatalf("round %d: ConnectInstance on the old owner after it was fenced", round)
			}
		}
		e.cancel()
		e.wg.Wait()
	}
}
