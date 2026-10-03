package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/idempotency"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// InstanceService implements the instance lifecycle use cases.
type InstanceService struct{ d Deps }

// CreateInstanceInput is the public create request.
type CreateInstanceInput struct {
	Name     string `json:"name"`
	Provider string `json:"provider,omitempty"`
}

// CreateInstanceResult is the (idempotently replayable) create response.
type CreateInstanceResult struct {
	ID          string                 `json:"id"`
	Status      instance.ObservedState `json:"status"`
	OperationID string                 `json:"operation_id"`
}

// OperationResult is the response of lifecycle operations.
type OperationResult struct {
	OperationID string                   `json:"operation_id"`
	Status      instance.OperationStatus `json:"status"`
	ErrorCode   string                   `json:"error_code,omitempty"`
}

func createOpID(instanceID string) string { return "op_create_" + instanceID }
func deleteOpID(instanceID string) string { return "op_delete_" + instanceID }

// Create places and provisions a new instance. It is idempotent with an
// Idempotency-Key and resumable: the instance id is fixed by the key.
func (s *InstanceService) Create(ctx context.Context, tenantID string, in CreateInstanceInput, idemKey string) (CreateInstanceResult, bool, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" || len(in.Name) > 100 {
		return CreateInstanceResult{}, false, fmt.Errorf("%w: name must be 1-100 characters", errs.ErrInvalidArgument)
	}
	providerKey, err := s.d.resolveProvider(in.Provider)
	if err != nil {
		return CreateInstanceResult{}, false, err
	}
	hash := idempotency.HashRequest(CreateInstanceInput{Name: in.Name, Provider: providerKey})
	return idempotency.Do(ctx, s.d.Idem, tenantID, idemKey, "create_instance", hash,
		func() string { return ids.New("inst") },
		func(ctx context.Context, id string) (CreateInstanceResult, error) {
			return s.create(ctx, tenantID, id, in.Name, providerKey)
		})
}

func (s *InstanceService) create(ctx context.Context, tenantID, id, name, providerKey string) (CreateInstanceResult, error) {
	inst, err := s.d.Repos.Instances.CreateWithPlacement(ctx, ports.PlacementRequest{
		Instance: instance.Instance{ID: id, TenantID: tenantID, Name: name, Provider: providerKey,
			DesiredState: instance.DesiredConnected, ObservedState: instance.Allocating},
		Provider: providerKey,
		Choose: func(c []routing.Node) (string, error) {
			return routing.Place(c, routing.PlacementRequest{Provider: providerKey})
		},
	})
	if err != nil {
		return CreateInstanceResult{}, err
	}
	if inst.TenantID != tenantID { // key collision with another tenant's resource: impossible by construction
		return CreateInstanceResult{}, errs.ErrConflict
	}
	opID := createOpID(id)
	err = s.d.Repos.Operations.Create(ctx, instance.Operation{ID: opID, TenantID: tenantID, InstanceID: id,
		Type: instance.OpCreateInstance, Status: instance.OpRunning, Step: "PROVISIONING"})
	if err != nil && !errors.Is(err, errs.ErrAlreadyExists) {
		return CreateInstanceResult{}, err
	}
	final, err := s.Provision(ctx, *inst)
	if err != nil {
		return CreateInstanceResult{}, err
	}
	return CreateInstanceResult{ID: id, Status: final.ObservedState, OperationID: opID}, nil
}

// Provision creates the session on the provider node that owns the
// assignment. It is idempotent and is also used by the reconciler to resume
// provisioning that a crashed gateway left behind: a provider that already has
// the session ("already exists") is adopted rather than recreated.
func (s *InstanceService) Provision(ctx context.Context, inst instance.Instance) (out *instance.Instance, err error) {
	err = s.d.WithInstanceControl(ctx, inst.ID, lockWait, func(ctx context.Context) error {
		fresh, gerr := s.d.Repos.Instances.Get(ctx, inst.ID) // re-read under the lock
		if gerr != nil {
			return gerr
		}
		out, gerr = s.ProvisionLocked(ctx, *fresh)
		return gerr
	})
	return out, err
}

// ProvisionLocked is Provision for callers that already hold the
// instance-control lock (the reconciler) and loaded inst under it.
func (s *InstanceService) ProvisionLocked(ctx context.Context, inst instance.Instance) (*instance.Instance, error) {
	if inst.DeletedAt != nil || inst.DesiredState == instance.DesiredDeleted ||
		inst.ObservedState == instance.Deleting || inst.ObservedState == instance.Deleted || inst.ObservedState == instance.Failed {
		return &inst, nil // nothing to provision any more (a delete won the race)
	}
	ctx, span := observability.Start(ctx, "instance.provision")
	defer span.End()
	ctx = instanceCtx(ctx, inst)
	opID := createOpID(inst.ID)

	provider, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return nil, err
	}
	if inst.NodeID == "" {
		return nil, fmt.Errorf("%w: instance %s has no owner", errs.ErrConflict, inst.ID)
	}
	if inst.ObservedState == instance.Allocating {
		if _, err := s.d.observe(ctx, inst, instance.Creating); err != nil {
			return nil, err
		}
		inst.ObservedState = instance.Creating
	}
	a := inst.Assignment()
	pi, err := provider.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a, TenantID: inst.TenantID, Name: inst.Name})
	if errors.Is(err, errs.ErrInstanceAlreadyExists) {
		// adopt: ask the provider who it says this session is (its id may differ from ours)
		pi, err = provider.LookupInstance(ctx, a)
	}
	if err != nil {
		return s.provisionFailed(ctx, inst, opID, err)
	}
	if err := s.d.Repos.Instances.SetProviderInstance(ctx, inst.ID, inst.AssignmentEpoch, pi.ProviderInstanceID); err != nil {
		return nil, err
	}
	if pi.State.Valid() {
		if _, err := s.d.observe(ctx, inst, pi.State); err != nil && !errors.Is(err, errs.ErrInvalidTransition) {
			return nil, err
		}
	}
	_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpSucceeded, "", "", s.d.now())
	s.d.Log.InfoContext(ctx, "instance provisioned", "state", pi.State)
	return s.d.Repos.Instances.Get(ctx, inst.ID)
}

func (s *InstanceService) provisionFailed(ctx context.Context, inst instance.Instance, opID string, cause error) (*instance.Instance, error) {
	switch errs.Classify(cause) {
	case errs.Retryable, errs.Ambiguous:
		// Node down or call outcome unknown: stay CREATING; the reconciler resumes.
		s.d.Log.WarnContext(ctx, "provisioning deferred", "error", cause)
		return s.d.Repos.Instances.Get(ctx, inst.ID)
	}
	code := "PROVIDER_REJECTED"
	if errors.Is(cause, errs.ErrAuthenticationFailed) {
		code = "PROVIDER_AUTH_FAILED"
	}
	s.d.Log.ErrorContext(ctx, "provisioning failed permanently", "error", cause, "code", code)
	_, _ = s.d.observe(ctx, inst, instance.Failed)
	_ = s.d.Repos.Instances.Release(ctx, inst.ID, inst.AssignmentEpoch, ownership.ReleaseCreateFailed)
	_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpFailed, code, cause.Error(), s.d.now())
	return s.d.Repos.Instances.Get(ctx, inst.ID)
}

// Get returns an instance owned by tenantID.
func (s *InstanceService) Get(ctx context.Context, tenantID, id string) (*instance.Instance, error) {
	return s.d.loadForTenant(ctx, tenantID, id)
}

// List returns the tenant's instances.
func (s *InstanceService) List(ctx context.Context, tenantID string) ([]instance.Instance, error) {
	return s.d.Repos.Instances.List(ctx, tenantID)
}

// GetOperation returns an operation owned by tenantID.
func (s *InstanceService) GetOperation(ctx context.Context, tenantID, id string) (*instance.Operation, error) {
	op, err := s.d.Repos.Operations.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if op.TenantID != tenantID {
		return nil, errs.ErrNotFound
	}
	return op, nil
}

// Delete schedules deletion (desired DELETED) and attempts it immediately; if
// the provider node is unavailable the reconciler converges later.
func (s *InstanceService) Delete(ctx context.Context, tenantID, id, idemKey string) (OperationResult, bool, error) {
	if _, err := s.d.loadForTenant(ctx, tenantID, id); err != nil && idemKey == "" {
		return OperationResult{}, false, err
	}
	hash := idempotency.HashRequest(map[string]string{"id": id})
	return idempotency.Do(ctx, s.d.Idem, tenantID, idemKey, "delete_instance", hash,
		func() string { return id },
		func(ctx context.Context, id string) (OperationResult, error) {
			var out OperationResult
			err := s.d.WithInstanceControl(ctx, id, lockWait, func(ctx context.Context) error {
				inst, err := s.d.loadForTenant(ctx, tenantID, id) // fresh, under the lock
				if err != nil {
					return err
				}
				if op, err := s.d.Repos.Operations.FindActive(ctx, inst.ID, instance.OpMigrate); err == nil {
					if op.Status != instance.OpAwaitingPairing {
						return fmt.Errorf("%w: migration %s in progress", errs.ErrConflict, op.ID)
					}
					// the migration only waits for a QR scan: deleting the instance cancels it
					if cerr := s.d.Repos.Operations.Complete(ctx, op.ID, instance.OpFailed, "CANCELLED_BY_DELETE", "instance deleted while awaiting pairing", s.d.now()); cerr != nil && !errors.Is(cerr, errs.ErrAlreadyTerminal) {
						return cerr
					}
				}
				out, err = s.requestDelete(ctx, *inst)
				return err
			})
			return out, err
		})
}

func (s *InstanceService) requestDelete(ctx context.Context, inst instance.Instance) (OperationResult, error) {
	ctx = instanceCtx(ctx, inst)
	opID := deleteOpID(inst.ID)
	err := s.d.Repos.Operations.Create(ctx, instance.Operation{ID: opID, TenantID: inst.TenantID, InstanceID: inst.ID,
		Type: instance.OpDeleteInstance, Status: instance.OpRunning, Step: "DELETING"})
	if err != nil && !errors.Is(err, errs.ErrAlreadyExists) {
		return OperationResult{}, err
	}
	if err := s.d.Repos.Instances.UpdateDesired(ctx, inst.ID, instance.DesiredDeleted); err != nil {
		return OperationResult{}, err
	}
	if inst.ObservedState != instance.Deleting {
		if _, err := s.d.observe(ctx, inst, instance.Deleting); err != nil {
			return OperationResult{}, err
		}
		inst.ObservedState = instance.Deleting
	}
	done, err := s.FinishDeleteLocked(ctx, inst)
	if err != nil {
		return OperationResult{}, err
	}
	st := instance.OpRunning
	if done {
		st = instance.OpSucceeded
	}
	return OperationResult{OperationID: opID, Status: st}, nil
}

// FinishDelete removes the provider session and releases ownership. It returns
// done=false (and no error) when the provider is unreachable; the reconciler
// retries. A session that is already gone counts as deleted.
func (s *InstanceService) FinishDelete(ctx context.Context, inst instance.Instance) (done bool, err error) {
	err = s.d.WithInstanceControl(ctx, inst.ID, lockWait, func(ctx context.Context) error {
		fresh, gerr := s.d.Repos.Instances.Get(ctx, inst.ID) // re-read under the lock
		if gerr != nil {
			return gerr
		}
		done, gerr = s.FinishDeleteLocked(ctx, *fresh)
		return gerr
	})
	return done, err
}

// FinishDeleteLocked is FinishDelete for callers holding the instance-control lock.
func (s *InstanceService) FinishDeleteLocked(ctx context.Context, inst instance.Instance) (bool, error) {
	ctx = instanceCtx(ctx, inst)
	opID := deleteOpID(inst.ID)
	if inst.NodeID != "" {
		provider, err := s.d.Providers.Get(inst.Provider)
		if err != nil {
			return false, err
		}
		err = provider.DeleteInstance(ctx, inst.Assignment())
		switch {
		case err == nil, errors.Is(err, errs.ErrInstanceNotFound):
		case errs.Classify(err) != errs.NonRetryable:
			s.d.Log.WarnContext(ctx, "delete deferred: provider unavailable", "error", err)
			return false, nil
		default:
			s.d.Log.ErrorContext(ctx, "delete failed", "error", err)
			return false, nil
		}
	}
	if err := s.d.Repos.Instances.MarkDeleted(ctx, inst.ID, inst.AssignmentEpoch, s.d.now()); err != nil {
		return false, err
	}
	_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpSucceeded, "", "", s.d.now())
	s.d.Log.InfoContext(ctx, "instance deleted")
	return true, nil
}

func (s *InstanceService) newOp(ctx context.Context, inst *instance.Instance, t instance.OperationType) (string, error) {
	opID := ids.New("op")
	err := s.d.Repos.Operations.Create(ctx, instance.Operation{ID: opID, TenantID: inst.TenantID, InstanceID: inst.ID,
		Type: t, Status: instance.OpRunning, Step: string(t)})
	return opID, err
}

func (s *InstanceService) mustBeLive(inst *instance.Instance) error {
	switch inst.ObservedState {
	case instance.Allocating, instance.Creating, instance.Deleting, instance.Deleted, instance.Failed, instance.Migrating:
		return fmt.Errorf("%w: instance is %s", errs.ErrConflict, inst.ObservedState)
	}
	if inst.NodeID == "" {
		return fmt.Errorf("%w: instance has no owner", errs.ErrConflict)
	}
	return nil
}

// Reconnect sets the desired state to CONNECTED and asks the owner to reconnect.
func (s *InstanceService) Reconnect(ctx context.Context, tenantID, id string) (out OperationResult, err error) {
	err = s.d.WithInstanceControl(ctx, id, lockWait, func(ctx context.Context) (e error) {
		out, e = s.reconnect(ctx, tenantID, id)
		return e
	})
	return out, err
}

func (s *InstanceService) reconnect(ctx context.Context, tenantID, id string) (OperationResult, error) {
	inst, err := s.d.loadForTenant(ctx, tenantID, id)
	if err != nil {
		return OperationResult{}, err
	}
	if err := s.mustBeLive(inst); err != nil {
		return OperationResult{}, err
	}
	ctx = instanceCtx(ctx, *inst)
	opID, err := s.newOp(ctx, inst, instance.OpReconnect)
	if err != nil {
		return OperationResult{}, err
	}
	if err := s.d.Repos.Instances.UpdateDesired(ctx, inst.ID, instance.DesiredConnected); err != nil {
		return OperationResult{}, err
	}
	if inst.ObservedState == instance.Connected {
		_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpSucceeded, "", "", s.d.now())
		return OperationResult{OperationID: opID, Status: instance.OpSucceeded}, nil
	}
	provider, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return OperationResult{}, err
	}
	switch err := provider.ConnectInstance(ctx, inst.Assignment()); {
	case err == nil:
		s.refreshObserved(ctx, *inst, provider)
		_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpSucceeded, "", "", s.d.now())
		return OperationResult{OperationID: opID, Status: instance.OpSucceeded}, nil
	case errors.Is(err, errs.ErrPairingUnavailable):
		_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpFailed, "PAIRING_REQUIRED", err.Error(), s.d.now())
		return OperationResult{OperationID: opID, Status: instance.OpFailed, ErrorCode: "PAIRING_REQUIRED"}, nil
	default:
		_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpFailed, "PROVIDER_ERROR", err.Error(), s.d.now())
		return OperationResult{}, err
	}
}

// Logout sets the desired state to DISCONNECTED and closes the session.
func (s *InstanceService) Logout(ctx context.Context, tenantID, id string) (out OperationResult, err error) {
	err = s.d.WithInstanceControl(ctx, id, lockWait, func(ctx context.Context) (e error) {
		out, e = s.logout(ctx, tenantID, id)
		return e
	})
	return out, err
}

func (s *InstanceService) logout(ctx context.Context, tenantID, id string) (OperationResult, error) {
	inst, err := s.d.loadForTenant(ctx, tenantID, id)
	if err != nil {
		return OperationResult{}, err
	}
	if err := s.mustBeLive(inst); err != nil {
		return OperationResult{}, err
	}
	ctx = instanceCtx(ctx, *inst)
	opID, err := s.newOp(ctx, inst, instance.OpLogout)
	if err != nil {
		return OperationResult{}, err
	}
	if err := s.d.Repos.Instances.UpdateDesired(ctx, inst.ID, instance.DesiredDisconnected); err != nil {
		return OperationResult{}, err
	}
	provider, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return OperationResult{}, err
	}
	if err := provider.Disconnect(ctx, inst.Assignment()); err != nil {
		_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpFailed, "PROVIDER_ERROR", err.Error(), s.d.now())
		return OperationResult{}, err
	}
	s.refreshObserved(ctx, *inst, provider)
	_ = s.d.Repos.Operations.Complete(ctx, opID, instance.OpSucceeded, "", "", s.d.now())
	return OperationResult{OperationID: opID, Status: instance.OpSucceeded}, nil
}

// refreshObserved best-effort syncs observed_state from the provider.
func (s *InstanceService) refreshObserved(ctx context.Context, inst instance.Instance, p ports.MessagingProvider) {
	st, err := p.GetInstanceState(ctx, inst.Assignment())
	if err != nil || !st.State.Valid() {
		return
	}
	if _, err := s.d.observe(ctx, inst, st.State); err != nil {
		s.d.Log.WarnContext(ctx, "observed state refresh failed", "error", err)
	}
}

// PairingKind selects the pairing material requested through the API.
type PairingKind string

const (
	PairingQR   PairingKind = "qrcode"
	PairingCode PairingKind = "pairing-code"
)

// Pairing returns pairing material, honouring provider capabilities.
func (s *InstanceService) Pairing(ctx context.Context, tenantID, id string, kind PairingKind) (*ports.PairingCode, error) {
	inst, err := s.d.loadForTenant(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if err := s.mustBeLive(inst); err != nil {
		return nil, err
	}
	provider, err := s.d.Providers.Get(inst.Provider)
	if err != nil {
		return nil, err
	}
	caps := provider.Capabilities(ctx)
	if (kind == PairingQR && !caps.QRCode) || (kind == PairingCode && !caps.PairingCode) {
		return nil, fmt.Errorf("%w: %s", errs.ErrCapabilityMissing, kind)
	}
	return provider.GetPairingCode(instanceCtx(ctx, *inst), inst.Assignment())
}
