package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/idempotency"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// MigrationService runs explicit instance migrations. Migration is a
// persisted state machine (operations.step) so any process can resume it, and
// the previous owner is *physically fenced* before a new epoch is assigned:
//
//	MIGRATION_REQUESTED -> FENCING_OLD_OWNER -> OLD_OWNER_FENCED
//	  -> ASSIGN_NEW_EPOCH -> START_NEW_OWNER -> VERIFY_CONNECTION -> CONNECTED
//	                \-> MIGRATION_BLOCKED (fencing failed; consistency over availability)
type MigrationService struct {
	d    Deps
	inst *InstanceService
}

// MigrateInput is the public migrate request.
type MigrateInput struct {
	TargetNodeID string `json:"target_node_id,omitempty"`
}

// MigrateResult is the response of POST /instances/{id}/migrate.
type MigrateResult struct {
	OperationID string                   `json:"operation_id"`
	Status      instance.OperationStatus `json:"status"`
	Step        string                   `json:"step"`
}

// Start requests a migration (idempotent). A BLOCKED migration of the same
// instance is resumed instead of starting a second one.
func (s *MigrationService) Start(ctx context.Context, tenantID, instanceID string, in MigrateInput, idemKey string) (MigrateResult, bool, error) {
	hash := idempotency.HashRequest(map[string]string{"id": instanceID, "target": in.TargetNodeID})
	return idempotency.Do(ctx, s.d.Idem, tenantID, idemKey, "migrate_instance", hash,
		func() string { return ids.New("op") },
		func(ctx context.Context, opID string) (MigrateResult, error) {
			var res MigrateResult
			var kick string
			err := s.d.WithInstanceControl(ctx, instanceID, lockWait, func(ctx context.Context) (e error) {
				res, kick, e = s.start(ctx, tenantID, instanceID, in, opID)
				return e
			})
			if err == nil && kick != "" {
				s.Kick(kick) // after the lock is released: Drive takes it
			}
			return res, err
		})
}

func (s *MigrationService) start(ctx context.Context, tenantID, instanceID string, in MigrateInput, opID string) (MigrateResult, string, error) {
	inst, err := s.d.loadForTenant(ctx, tenantID, instanceID)
	if err != nil {
		return MigrateResult{}, "", err
	}
	ctx = instanceCtx(ctx, *inst)

	if existing, err := s.d.Repos.Operations.FindActive(ctx, inst.ID, instance.OpMigrate); err == nil {
		if existing.Status == instance.OpBlocked {
			if _, err := s.d.Repos.Operations.Advance(ctx, existing.ID, string(ownership.StepBlocked),
				string(ownership.StepFencingOldOwner), instance.OpRunning, ports.OperationPatch{}); err != nil {
				return MigrateResult{}, "", err
			}
			return MigrateResult{OperationID: existing.ID, Status: instance.OpRunning, Step: string(ownership.StepFencingOldOwner)}, existing.ID, nil
		}
		return MigrateResult{OperationID: existing.ID, Status: existing.Status, Step: existing.Step}, "", nil
	}

	if err := s.inst.mustBeLive(inst); err != nil {
		return MigrateResult{}, "", err
	}
	if inst.DesiredState == instance.DesiredDeleted {
		return MigrateResult{}, "", fmt.Errorf("%w: instance is being deleted", errs.ErrConflict)
	}
	target, err := s.chooseTarget(ctx, inst, in.TargetNodeID)
	if err != nil {
		return MigrateResult{}, "", err
	}
	op := instance.Operation{ID: opID, TenantID: tenantID, InstanceID: inst.ID, Type: instance.OpMigrate,
		Status: instance.OpRunning, Step: string(ownership.StepRequested),
		SourceNodeID: inst.NodeID, SourceEpoch: inst.AssignmentEpoch, TargetNodeID: target}
	if err := s.d.Repos.Operations.Create(ctx, op); err != nil {
		if errors.Is(err, errs.ErrInProgress) { // lost a race with a concurrent request: join its operation
			cur, gerr := s.d.Repos.Operations.FindActive(ctx, inst.ID, instance.OpMigrate)
			if gerr != nil {
				return MigrateResult{}, "", gerr
			}
			return MigrateResult{OperationID: cur.ID, Status: cur.Status, Step: cur.Step}, "", nil
		}
		if errors.Is(err, errs.ErrAlreadyExists) { // resumed idempotent request
			cur, gerr := s.d.Repos.Operations.Get(ctx, opID)
			if gerr != nil {
				return MigrateResult{}, "", gerr
			}
			return MigrateResult{OperationID: cur.ID, Status: cur.Status, Step: cur.Step}, "", nil
		}
		return MigrateResult{}, "", err
	}
	return MigrateResult{OperationID: opID, Status: instance.OpRunning, Step: op.Step}, opID, nil
}

// chooseTarget validates an explicit target or places the instance anew,
// never choosing the node that currently owns it.
func (s *MigrationService) chooseTarget(ctx context.Context, inst *instance.Instance, requested string) (string, error) {
	nodes, err := s.d.Repos.Nodes.List(ctx)
	if err != nil {
		return "", err
	}
	var sameProvider []routing.Node
	for _, n := range nodes {
		if n.Provider == inst.Provider {
			sameProvider = append(sameProvider, n)
		}
	}
	if requested != "" {
		if requested == inst.NodeID {
			return "", fmt.Errorf("%w: instance already lives on %s", errs.ErrInvalidArgument, requested)
		}
		var only []routing.Node
		for _, n := range sameProvider {
			if n.ID == requested {
				only = append(only, n)
			}
		}
		if len(only) == 0 {
			return "", fmt.Errorf("%w: node %q is not a %s node", errs.ErrNotFound, requested, inst.Provider)
		}
		return routing.Place(only, routing.PlacementRequest{Provider: inst.Provider})
	}
	return routing.Place(sameProvider, routing.PlacementRequest{Provider: inst.Provider, Exclude: []string{inst.NodeID}})
}

// Kick drives the migration in the background (best effort). Anything that
// does not finish is resumed by the reconciler.
func (s *MigrationService) Kick(opID string) {
	go func() {
		ctx := context.Background()
		if err := s.Drive(ctx, opID); err != nil {
			s.d.Log.WarnContext(ctx, "migration drive failed; reconciler will resume", "operation_id", opID, "error", err)
		}
	}()
}

// Drive advances a migration as far as it can go while holding the
// instance-control lock (shared with the reconciler, delete, logout, reconnect
// and provisioning). It is safe to call from several processes: a busy
// instance is skipped and every step is a compare-and-set on the operation row.
func (s *MigrationService) Drive(ctx context.Context, opID string) error {
	op, err := s.d.Repos.Operations.Get(ctx, opID)
	if err != nil {
		return err
	}
	if op.Type != instance.OpMigrate || !op.Status.IsActive() || op.InstanceID == "" {
		return nil
	}
	err = s.d.WithInstanceControl(ctx, op.InstanceID, 2*time.Second, func(ctx context.Context) error {
		for i := 0; i < 12; i++ {
			op, err := s.d.Repos.Operations.Get(ctx, opID)
			if err != nil {
				return err
			}
			if !op.Status.IsActive() {
				return nil
			}
			inst, err := s.d.Repos.Instances.Get(ctx, op.InstanceID) // fresh, under the lock
			if err != nil {
				return err
			}
			progressed, err := s.step(ctx, op, inst)
			if err != nil {
				return err
			}
			if !progressed {
				return nil
			}
		}
		return nil
	})
	if errors.Is(err, errs.ErrInProgress) {
		return nil // another lifecycle operation owns the instance right now; try again later
	}
	return err
}

func (s *MigrationService) adv(ctx context.Context, op *instance.Operation, to ownership.MigrationStep, st instance.OperationStatus, p ports.OperationPatch) error {
	_, err := s.d.Repos.Operations.Advance(ctx, op.ID, op.Step, string(to), st, p)
	return err
}

func (s *MigrationService) fail(ctx context.Context, op *instance.Operation, code string, cause error) (bool, error) {
	s.d.Log.ErrorContext(ctx, "migration failed", "operation_id", op.ID, "code", code, "error", cause)
	err := s.d.Repos.Operations.Complete(ctx, op.ID, instance.OpFailed, code, cause.Error(), s.d.now())
	if errors.Is(err, errs.ErrAlreadyTerminal) {
		return false, nil // a faster driver already finished this operation: the first verdict stands
	}
	return false, err
}

// step performs one transition; progressed=false means "wait" (blocked, waiting
// for the provider, or finished).
func (s *MigrationService) step(ctx context.Context, op *instance.Operation, inst *instance.Instance) (bool, error) {
	ctx, span := observability.Start(ctx, "migration.step")
	defer span.End()
	ctx = observability.With(instanceCtx(ctx, *inst), observability.KeyOperation, op.ID)
	step := ownership.MigrationStep(op.Step)

	switch step {
	case ownership.StepRequested:
		if inst.AssignmentEpoch != op.SourceEpoch {
			return s.fail(ctx, op, "STALE_ASSIGNMENT", errs.ErrStaleAssignment)
		}
		if _, err := s.d.observe(ctx, *inst, instance.Migrating); err != nil && !errors.Is(err, errs.ErrInvalidTransition) {
			return false, err
		}
		return true, s.adv(ctx, op, ownership.StepFencingOldOwner, instance.OpRunning, ports.OperationPatch{})

	case ownership.StepFencingOldOwner:
		return s.fence(ctx, op, inst)

	case ownership.StepBlocked:
		return false, nil // waits for an operator-triggered resume (POST migrate)

	case ownership.StepOldOwnerFenced:
		return s.assign(ctx, op, inst)

	case ownership.StepAssignNewEpoch:
		return s.startNewOwner(ctx, op, inst)

	case ownership.StepStartNewOwner:
		provider, err := s.d.Providers.Get(inst.Provider)
		if err != nil {
			return false, err
		}
		st, err := provider.GetInstanceState(ctx, inst.Assignment())
		if err != nil {
			return false, nil // provider unavailable: retry later
		}
		if st.State.Valid() {
			if _, err := s.d.observe(ctx, *inst, st.State); err != nil && !errors.Is(err, errs.ErrInvalidTransition) {
				return false, err
			}
		}
		return true, s.adv(ctx, op, ownership.StepVerifyConn, instance.OpRunning, ports.OperationPatch{})

	case ownership.StepVerifyConn:
		return s.verify(ctx, op, inst)
	}
	return false, nil
}

// fence performs *physical* fencing: the old owner's session must be confirmed
// closed. Any doubt blocks the migration; a new owner is never activated.
func (s *MigrationService) fence(ctx context.Context, op *instance.Operation, inst *instance.Instance) (bool, error) {
	if inst.AssignmentEpoch != op.SourceEpoch || inst.NodeID != op.SourceNodeID {
		return s.fail(ctx, op, "STALE_ASSIGNMENT", errs.ErrStaleAssignment)
	}
	provider, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return false, err
	}
	old := ownership.Assignment{InstanceID: inst.ID, NodeID: op.SourceNodeID, Epoch: op.SourceEpoch}
	block := func(cause error) (bool, error) {
		s.d.Metrics.MigrationBlockedTotal.Inc()
		s.d.Log.ErrorContext(ctx, "MIGRATION_BLOCKED: old owner could not be fenced", "error", cause)
		return false, s.adv(ctx, op, ownership.StepBlocked, instance.OpBlocked,
			ports.OperationPatch{ErrorCode: "FENCING_FAILED", ErrorMessage: cause.Error(), BumpAttempts: true})
	}
	if !provider.Capabilities(ctx).Disconnect {
		return block(fmt.Errorf("%w: provider cannot confirm physical fencing", errs.ErrCapabilityMissing))
	}
	derr := provider.Disconnect(ctx, old)
	if derr != nil && !errors.Is(derr, errs.ErrInstanceNotFound) {
		return block(derr)
	}
	if derr == nil { // confirm: the socket must really be closed
		st, serr := provider.GetInstanceState(ctx, old)
		switch {
		case errors.Is(serr, errs.ErrInstanceNotFound):
		case serr != nil:
			return block(serr)
		case st.State == instance.Connected || st.State == instance.Connecting || st.State == instance.Reconnecting:
			return block(fmt.Errorf("old owner still reports %s after disconnect", st.State))
		}
	}
	s.d.Log.InfoContext(ctx, "old owner fenced", "node_id", op.SourceNodeID)
	return true, s.adv(ctx, op, ownership.StepOldOwnerFenced, instance.OpRunning, ports.OperationPatch{})
}

// assign moves ownership to the next epoch. The repository itself refuses
// when the operation is not at OLD_OWNER_FENCED (INV-09).
func (s *MigrationService) assign(ctx context.Context, op *instance.Operation, inst *instance.Instance) (bool, error) {
	if !ownership.CanActivateNewOwner(ownership.MigrationStep(op.Step)) {
		return false, errs.ErrFencingRequired
	}
	target := op.TargetNodeID
	a, err := s.d.Repos.Instances.Reassign(ctx, ports.ReassignRequest{
		InstanceID: inst.ID, ExpectedEpoch: op.SourceEpoch, NewNodeID: target,
		OperationID: op.ID, Reason: ownership.ReleaseMigrated})
	switch {
	case err == nil:
		s.d.Log.InfoContext(ctx, "new epoch assigned", "node_id", a.NodeID, "assignment_epoch", a.Epoch)
		return true, nil
	case errors.Is(err, errs.ErrNoCapacity):
		// Target filled up / started draining meanwhile. The old owner is
		// already fenced, so wait for capacity rather than guessing.
		s.d.Log.WarnContext(ctx, "migration target unavailable; waiting", "target", target)
		_, _ = s.d.Repos.Operations.Advance(ctx, op.ID, op.Step, op.Step, instance.OpRunning,
			ports.OperationPatch{ErrorCode: "TARGET_UNAVAILABLE", ErrorMessage: err.Error(), BumpAttempts: true})
		if alt, perr := s.chooseTarget(ctx, inst, ""); perr == nil && alt != target {
			_, _ = s.d.Repos.Operations.Advance(ctx, op.ID, op.Step, op.Step, instance.OpRunning, ports.OperationPatch{TargetNodeID: alt})
		}
		return false, nil
	case errors.Is(err, errs.ErrStaleAssignment):
		return s.fail(ctx, op, "STALE_ASSIGNMENT", err)
	default:
		return false, err
	}
}

func (s *MigrationService) startNewOwner(ctx context.Context, op *instance.Operation, inst *instance.Instance) (bool, error) {
	provider, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return false, err
	}
	a := inst.Assignment() // new node, new epoch
	pi, err := provider.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a, TenantID: inst.TenantID, Name: inst.Name})
	if errors.Is(err, errs.ErrInstanceAlreadyExists) {
		// adopt only after the provider confirms the session really exists for this assignment,
		// using the provider's own identifier
		pi, err = provider.LookupInstance(ctx, a)
	}
	if err != nil {
		if errs.Classify(err) == errs.NonRetryable {
			_, _ = s.d.observe(ctx, *inst, instance.Failed)
			return s.fail(ctx, op, "START_NEW_OWNER_FAILED", err)
		}
		return false, nil // retry on the next pass
	}
	pid := pi.ProviderInstanceID
	if pid == "" {
		pid = inst.ID
	}
	if err := s.d.Repos.Instances.SetProviderInstance(ctx, inst.ID, inst.AssignmentEpoch, pid); err != nil {
		return false, err
	}
	return true, s.adv(ctx, op, ownership.StepStartNewOwner, instance.OpRunning, ports.OperationPatch{})
}

func (s *MigrationService) verify(ctx context.Context, op *instance.Operation, inst *instance.Instance) (bool, error) {
	provider, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return false, err
	}
	st, err := provider.GetInstanceState(ctx, inst.Assignment())
	if err == nil && st.State == instance.Connected {
		_, _ = s.d.observe(ctx, *inst, instance.Connected)
		if err := s.adv(ctx, op, ownership.StepConnected, instance.OpRunning, ports.OperationPatch{}); err != nil {
			return false, err
		}
		if cerr := s.d.Repos.Operations.Complete(ctx, op.ID, instance.OpSucceeded, "", "", s.d.now()); cerr != nil && !errors.Is(cerr, errs.ErrAlreadyTerminal) {
			return false, cerr
		}
		return false, nil
	}
	if err == nil && st.State.Valid() {
		_, _ = s.d.observe(ctx, *inst, st.State)
	}
	// The new owner exists and is correctly assigned; it only needs the user to scan a QR code.
	// That is not an infrastructure failure and must not time out into FAILED: the operation
	// says so explicitly and keeps waiting (the user, not the platform, is the bottleneck).
	if err == nil && (st.State == instance.AwaitingPairing || st.State == instance.LoggedOut) {
		if op.Status != instance.OpAwaitingPairing {
			s.d.Log.InfoContext(ctx, "migration waiting for the user to pair the new owner")
			_, aerr := s.d.Repos.Operations.Advance(ctx, op.ID, op.Step, op.Step, instance.OpAwaitingPairing,
				ports.OperationPatch{ErrorCode: "PAIRING_REQUIRED", ErrorMessage: "scan the QR code on the new owner to finish the migration"})
			return false, aerr
		}
		return false, nil
	}
	if op.Status == instance.OpAwaitingPairing { // the session moved on (e.g. CONNECTING): back to normal verification
		if _, aerr := s.d.Repos.Operations.Advance(ctx, op.ID, op.Step, op.Step, instance.OpRunning, ports.OperationPatch{}); aerr != nil {
			return false, aerr
		}
	}
	started := op.StepStartedAt
	if started.IsZero() {
		started = op.CreatedAt
	}
	if s.d.now().Sub(started) > s.d.Cfg.MigrationVerifyTimeout { // only genuine verification time counts
		return s.fail(ctx, op, "VERIFY_TIMEOUT", errors.New("new owner did not reach CONNECTED in time"))
	}
	return false, nil
}
