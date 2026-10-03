// Package contracttest contains reusable contract suites. Every adapter of a
// port must pass the suite for that port (memory, PostgreSQL, Redis, S3, ...).
package contracttest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/ports"
)

// RepoFactory returns a fresh, empty set of repositories.
type RepoFactory func(t *testing.T) ports.Repositories

const testProvider = "evolution-v2"

type fixture struct {
	r ports.Repositories
}

func newFixture(t *testing.T, f RepoFactory) fixture { t.Helper(); return fixture{r: f(t)} }

func (fx fixture) tenant(t *testing.T, id string) {
	t.Helper()
	if err := fx.r.Tenants.Create(context.Background(), instance.Tenant{ID: id, Name: id, APIKeyHash: "hash-" + id}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
}

func (fx fixture) node(t *testing.T, id string, capacity int) {
	t.Helper()
	ctx := context.Background()
	if err := fx.r.Nodes.Upsert(ctx, routing.Node{ID: id, Provider: testProvider, Endpoint: "http://" + id, Capacity: capacity}); err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	if _, err := fx.r.Nodes.SetStatus(ctx, id, routing.NodeReady); err != nil {
		t.Fatalf("ready node: %v", err)
	}
}

func place(inst instance.Instance) ports.PlacementRequest {
	return ports.PlacementRequest{
		Instance: inst, Provider: testProvider,
		Choose: func(c []routing.Node) (string, error) {
			return routing.Place(c, routing.PlacementRequest{Provider: testProvider})
		},
	}
}

func newInst(id, tenant string) instance.Instance {
	return instance.Instance{ID: id, TenantID: tenant, Name: id, Provider: testProvider,
		DesiredState: instance.DesiredConnected, ObservedState: instance.Allocating}
}

func (fx fixture) instance(t *testing.T, id, tenant string) *instance.Instance {
	t.Helper()
	i, err := fx.r.Instances.CreateWithPlacement(context.Background(), place(newInst(id, tenant)))
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	return i
}

// RepositoryContract exercises every repository port.
func RepositoryContract(t *testing.T, factory RepoFactory) {
	t.Run("Tenants", func(t *testing.T) { tenantsContract(t, factory) })
	t.Run("Nodes", func(t *testing.T) { nodesContract(t, factory) })
	t.Run("Placement", func(t *testing.T) { placementContract(t, factory) })
	t.Run("OwnershipINV01", func(t *testing.T) { ownershipContract(t, factory) })
	t.Run("ReassignRequiresFencingINV09", func(t *testing.T) { reassignContract(t, factory) })
	t.Run("InstanceStateGuards", func(t *testing.T) { stateGuardContract(t, factory) })
	t.Run("Operations", func(t *testing.T) { operationsContract(t, factory) })
	t.Run("Messages", func(t *testing.T) { messagesContract(t, factory) })
	t.Run("OutboxAndSequence", func(t *testing.T) { outboxContract(t, factory) })
	t.Run("Idempotency", func(t *testing.T) { idempotencyContract(t, factory) })
	t.Run("Dedup", func(t *testing.T) { dedupContract(t, factory) })
	t.Run("BlobMetadata", func(t *testing.T) { blobMetaContract(t, factory) })
}

func tenantsContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	if err := fx.r.Tenants.Create(ctx, instance.Tenant{ID: "t1", APIKeyHash: "other"}); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Errorf("duplicate id: %v", err)
	}
	if err := fx.r.Tenants.Create(ctx, instance.Tenant{ID: "t2", APIKeyHash: "hash-t1"}); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Errorf("duplicate api key: %v", err)
	}
	got, err := fx.r.Tenants.GetByAPIKeyHash(ctx, "hash-t1")
	if err != nil || got.ID != "t1" {
		t.Errorf("by key: %v %v", got, err)
	}
	if _, err := fx.r.Tenants.Get(ctx, "nope"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
}

func nodesContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.node(t, "node-01", 5)
	n, _ := fx.r.Nodes.Get(ctx, "node-01")
	if n.Status != routing.NodeReady || n.Capacity != 5 {
		t.Fatalf("unexpected node %+v", n)
	}
	// Upsert must not reset status or active_instances.
	fx.tenant(t, "t1")
	fx.instance(t, "inst_1", "t1")
	_ = fx.r.Nodes.Upsert(ctx, routing.Node{ID: "node-01", Provider: testProvider, Endpoint: "http://new", Capacity: 7})
	n, _ = fx.r.Nodes.Get(ctx, "node-01")
	if n.Status != routing.NodeReady || n.ActiveInstances != 1 || n.Capacity != 7 || n.Endpoint != "http://new" {
		t.Errorf("upsert clobbered runtime state: %+v", n)
	}
	if _, err := fx.r.Nodes.SetStatus(ctx, "node-01", routing.NodeStarting); !errors.Is(err, errs.ErrInvalidTransition) {
		t.Errorf("READY -> STARTING must be rejected: %v", err)
	}
	if _, err := fx.r.Nodes.SetStatus(ctx, "node-01", routing.NodeDraining); err != nil {
		t.Errorf("drain: %v", err)
	}
	at := time.Now()
	if err := fx.r.Nodes.RecordProbe(ctx, "node-01", routing.NodeDraining, "2.3.0", true, at); err != nil {
		t.Fatal(err)
	}
	n, _ = fx.r.Nodes.Get(ctx, "node-01")
	if n.ProviderVersion != "2.3.0" || n.HeartbeatAt.IsZero() {
		t.Errorf("probe not recorded: %+v", n)
	}
	if list, _ := fx.r.Nodes.List(ctx); len(list) != 1 {
		t.Errorf("list: %d", len(list))
	}
}

func placementContract(t *testing.T, f RepoFactory) {
	t.Run("assigns epoch 1 and reserves capacity", func(t *testing.T) {
		fx, ctx := newFixture(t, f), context.Background()
		fx.tenant(t, "t1")
		fx.node(t, "node-01", 2)
		i := fx.instance(t, "inst_1", "t1")
		if i.NodeID != "node-01" || i.AssignmentEpoch != 1 || i.ObservedState != instance.Allocating {
			t.Fatalf("bad instance %+v", i)
		}
		n, _ := fx.r.Nodes.Get(ctx, "node-01")
		if n.ActiveInstances != 1 {
			t.Errorf("capacity not reserved: %d", n.ActiveInstances)
		}
		recs, _ := fx.r.Instances.Assignments(ctx, "inst_1")
		if len(recs) != 1 || recs[0].Epoch != 1 || recs[0].ReleasedAt != nil {
			t.Errorf("assignment history: %+v", recs)
		}
	})
	t.Run("idempotent on id", func(t *testing.T) {
		fx := newFixture(t, f)
		fx.tenant(t, "t1")
		fx.node(t, "node-01", 2)
		a := fx.instance(t, "inst_1", "t1")
		b := fx.instance(t, "inst_1", "t1")
		n, _ := fx.r.Nodes.Get(context.Background(), "node-01")
		if a.ID != b.ID || n.ActiveInstances != 1 {
			t.Errorf("duplicate create consumed capacity: active=%d", n.ActiveInstances)
		}
	})
	t.Run("no capacity", func(t *testing.T) {
		fx, ctx := newFixture(t, f), context.Background()
		fx.tenant(t, "t1")
		fx.node(t, "node-01", 1)
		fx.instance(t, "inst_1", "t1")
		_, err := fx.r.Instances.CreateWithPlacement(ctx, place(newInst("inst_2", "t1")))
		if !errors.Is(err, errs.ErrNoCapacity) {
			t.Fatalf("want ErrNoCapacity, got %v", err)
		}
		if _, err := fx.r.Instances.Get(ctx, "inst_2"); !errors.Is(err, errs.ErrNotFound) {
			t.Errorf("failed placement left an instance behind: %v", err)
		}
	})
	t.Run("draining node gets nothing INV-04", func(t *testing.T) {
		fx, ctx := newFixture(t, f), context.Background()
		fx.tenant(t, "t1")
		fx.node(t, "node-01", 10)
		if _, err := fx.r.Nodes.SetStatus(ctx, "node-01", routing.NodeDraining); err != nil {
			t.Fatal(err)
		}
		if _, err := fx.r.Instances.CreateWithPlacement(ctx, place(newInst("inst_1", "t1"))); !errors.Is(err, errs.ErrNoCapacity) {
			t.Fatalf("DRAINING node must not receive instances, got %v", err)
		}
	})
	t.Run("last slot is never double booked", func(t *testing.T) {
		fx, ctx := newFixture(t, f), context.Background()
		fx.tenant(t, "t1")
		fx.node(t, "node-01", 3)
		fx.node(t, "node-02", 2)
		const attempts = 30
		var ok, noCap atomic.Int32
		var wg sync.WaitGroup
		for k := 0; k < attempts; k++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				_, err := fx.r.Instances.CreateWithPlacement(ctx, place(newInst(fmt.Sprintf("inst_%02d", k), "t1")))
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, errs.ErrNoCapacity):
					noCap.Add(1)
				default:
					t.Errorf("unexpected: %v", err)
				}
			}(k)
		}
		wg.Wait()
		if ok.Load() != 5 || noCap.Load() != attempts-5 {
			t.Fatalf("ok=%d noCap=%d, want exactly capacity (5) successes", ok.Load(), noCap.Load())
		}
		nodes, _ := fx.r.Nodes.List(ctx)
		for _, n := range nodes {
			if n.ActiveInstances > n.Capacity {
				t.Errorf("node %s overbooked %d/%d", n.ID, n.ActiveInstances, n.Capacity)
			}
		}
	})
}

func ownershipContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.node(t, "node-02", 5)
	fx.instance(t, "inst_1", "t1")
	if err := fx.r.Instances.Release(ctx, "inst_1", 99, ownership.ReleaseCreateFailed); !errors.Is(err, errs.ErrStaleAssignment) {
		t.Errorf("release with wrong epoch: %v", err)
	}
	if err := fx.r.Instances.Release(ctx, "inst_1", 1, ownership.ReleaseCreateFailed); err != nil {
		t.Fatal(err)
	}
	i, _ := fx.r.Instances.Get(ctx, "inst_1")
	if i.NodeID != "" {
		t.Errorf("released instance still has owner %q", i.NodeID)
	}
	n, _ := fx.r.Nodes.Get(ctx, "node-01")
	if n.ActiveInstances != 0 {
		t.Errorf("capacity not freed: %d", n.ActiveInstances)
	}
	recs, _ := fx.r.Instances.Assignments(ctx, "inst_1")
	if len(recs) != 1 || recs[0].ReleasedAt == nil || recs[0].ReleaseReason != ownership.ReleaseCreateFailed {
		t.Errorf("history: %+v", recs)
	}
}

func startMigration(t *testing.T, fx fixture, instID, tenant string, step ownership.MigrationStep) string {
	t.Helper()
	opID := "op_mig_" + instID
	err := fx.r.Operations.Create(context.Background(), instance.Operation{
		ID: opID, TenantID: tenant, InstanceID: instID, Type: instance.OpMigrate,
		Status: instance.OpRunning, Step: string(ownership.StepRequested),
	})
	if err != nil {
		t.Fatalf("create op: %v", err)
	}
	path := []ownership.MigrationStep{ownership.StepFencingOldOwner, ownership.StepOldOwnerFenced}
	cur := ownership.StepRequested
	for _, next := range path {
		if cur == step {
			break
		}
		if _, err := fx.r.Operations.Advance(context.Background(), opID, string(cur), string(next), instance.OpRunning, ports.OperationPatch{}); err != nil {
			t.Fatalf("advance %s->%s: %v", cur, next, err)
		}
		cur = next
	}
	return opID
}

func reassignContract(t *testing.T, f RepoFactory) {
	setup := func(t *testing.T) (fixture, context.Context) {
		fx := newFixture(t, f)
		fx.tenant(t, "t1")
		fx.node(t, "node-01", 5)
		fx.node(t, "node-02", 5)
		i := fx.instance(t, "inst_1", "t1")
		if i.NodeID != "node-01" {
			t.Fatalf("setup: %s", i.NodeID)
		}
		return fx, context.Background()
	}
	t.Run("rejected before fencing", func(t *testing.T) {
		fx, ctx := setup(t)
		for _, step := range []ownership.MigrationStep{ownership.StepRequested, ownership.StepFencingOldOwner} {
			opID := "op_x_" + string(step)
			_ = fx.r.Operations.Create(ctx, instance.Operation{ID: opID, InstanceID: "inst_1", TenantID: "t1", Type: instance.OpMigrate,
				Status: instance.OpRunning, Step: string(ownership.StepRequested)})
			if step == ownership.StepFencingOldOwner {
				_, _ = fx.r.Operations.Advance(ctx, opID, string(ownership.StepRequested), string(step), instance.OpRunning, ports.OperationPatch{})
			}
			_, err := fx.r.Instances.Reassign(ctx, ports.ReassignRequest{InstanceID: "inst_1", ExpectedEpoch: 1, NewNodeID: "node-02", OperationID: opID, Reason: ownership.ReleaseMigrated})
			if !errors.Is(err, errs.ErrFencingRequired) {
				t.Errorf("INV-09: reassign at step %s must fail, got %v", step, err)
			}
			_ = fx.r.Operations.Complete(ctx, opID, instance.OpFailed, "x", "x", time.Now())
		}
		i, _ := fx.r.Instances.Get(ctx, "inst_1")
		if i.NodeID != "node-01" || i.AssignmentEpoch != 1 {
			t.Errorf("ownership changed without fencing: %+v", i)
		}
	})
	t.Run("rejected with unknown operation", func(t *testing.T) {
		fx, ctx := setup(t)
		_, err := fx.r.Instances.Reassign(ctx, ports.ReassignRequest{InstanceID: "inst_1", ExpectedEpoch: 1, NewNodeID: "node-02", OperationID: "nope"})
		if !errors.Is(err, errs.ErrFencingRequired) {
			t.Errorf("got %v", err)
		}
	})
	t.Run("succeeds after fencing and bumps epoch", func(t *testing.T) {
		fx, ctx := setup(t)
		opID := startMigration(t, fx, "inst_1", "t1", ownership.StepOldOwnerFenced)
		a, err := fx.r.Instances.Reassign(ctx, ports.ReassignRequest{InstanceID: "inst_1", ExpectedEpoch: 1, NewNodeID: "node-02", OperationID: opID, Reason: ownership.ReleaseMigrated})
		if err != nil {
			t.Fatal(err)
		}
		if a.NodeID != "node-02" || a.Epoch != 2 {
			t.Fatalf("assignment %+v", a)
		}
		recs, _ := fx.r.Instances.Assignments(ctx, "inst_1")
		open := 0
		for _, r := range recs {
			if r.ReleasedAt == nil {
				open++
			}
		}
		if len(recs) != 2 || open != 1 || recs[0].ReleaseReason != ownership.ReleaseMigrated || recs[1].Epoch != 2 {
			t.Errorf("INV-01: history %+v", recs)
		}
		n1, _ := fx.r.Nodes.Get(ctx, "node-01")
		n2, _ := fx.r.Nodes.Get(ctx, "node-02")
		if n1.ActiveInstances != 0 || n2.ActiveInstances != 1 {
			t.Errorf("capacity moved wrongly: %d/%d", n1.ActiveInstances, n2.ActiveInstances)
		}
		op, _ := fx.r.Operations.Get(ctx, opID)
		if op.Step != string(ownership.StepAssignNewEpoch) {
			t.Errorf("op step %s", op.Step)
		}
		// replay with the old epoch is stale
		if _, err := fx.r.Instances.Reassign(ctx, ports.ReassignRequest{InstanceID: "inst_1", ExpectedEpoch: 1, NewNodeID: "node-01", OperationID: opID}); err == nil {
			t.Error("stale reassign must fail")
		}
	})
	t.Run("concurrent reassign yields a single new owner", func(t *testing.T) {
		fx, ctx := setup(t)
		opID := startMigration(t, fx, "inst_1", "t1", ownership.StepOldOwnerFenced)
		var wins atomic.Int32
		var wg sync.WaitGroup
		for k := 0; k < 8; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := fx.r.Instances.Reassign(ctx, ports.ReassignRequest{InstanceID: "inst_1", ExpectedEpoch: 1, NewNodeID: "node-02", OperationID: opID, Reason: ownership.ReleaseMigrated}); err == nil {
					wins.Add(1)
				}
			}()
		}
		wg.Wait()
		i, _ := fx.r.Instances.Get(ctx, "inst_1")
		if wins.Load() != 1 || i.AssignmentEpoch != 2 {
			t.Fatalf("wins=%d epoch=%d", wins.Load(), i.AssignmentEpoch)
		}
	})
	t.Run("target must have capacity", func(t *testing.T) {
		fx, ctx := setup(t)
		if _, err := fx.r.Nodes.SetStatus(ctx, "node-02", routing.NodeDraining); err != nil {
			t.Fatal(err)
		}
		opID := startMigration(t, fx, "inst_1", "t1", ownership.StepOldOwnerFenced)
		_, err := fx.r.Instances.Reassign(ctx, ports.ReassignRequest{InstanceID: "inst_1", ExpectedEpoch: 1, NewNodeID: "node-02", OperationID: opID})
		if !errors.Is(err, errs.ErrNoCapacity) {
			t.Errorf("draining target must be refused: %v", err)
		}
	})
}

func stateGuardContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	now := time.Now()
	if _, err := fx.r.Instances.SetObserved(ctx, "inst_1", 7, instance.Creating, now); !errors.Is(err, errs.ErrStaleAssignment) {
		t.Errorf("stale epoch: %v", err)
	}
	if _, err := fx.r.Instances.SetObserved(ctx, "inst_1", 1, instance.Connected, now); !errors.Is(err, errs.ErrInvalidTransition) {
		t.Errorf("ALLOCATING -> CONNECTED: %v", err)
	}
	if changed, err := fx.r.Instances.SetObserved(ctx, "inst_1", 1, instance.Creating, now); err != nil || !changed {
		t.Errorf("valid transition: %v %v", changed, err)
	}
	if changed, _ := fx.r.Instances.SetObserved(ctx, "inst_1", 1, instance.Creating, now); changed {
		t.Error("same-state write must report unchanged")
	}
	if err := fx.r.Instances.SetProviderInstance(ctx, "inst_1", 1, "prov-1"); err != nil {
		t.Fatal(err)
	}
	if err := fx.r.Instances.UpdateDesired(ctx, "inst_1", instance.DesiredDeleted); err != nil {
		t.Fatal(err)
	}
	if err := fx.r.Instances.TouchHeartbeat(ctx, "inst_1", 7, now); !errors.Is(err, errs.ErrStaleAssignment) {
		t.Errorf("heartbeat from another epoch must be rejected: %v", err)
	}
	if i0, _ := fx.r.Instances.Get(ctx, "inst_1"); !i0.LastProviderHeartbeat.IsZero() {
		t.Error("a stale heartbeat modified the instance")
	}
	if err := fx.r.Instances.TouchHeartbeat(ctx, "inst_1", 1, now); err != nil {
		t.Errorf("current-epoch heartbeat: %v", err)
	}
	if err := fx.r.Instances.MarkReconciled(ctx, "inst_1", now); err != nil {
		t.Fatal(err)
	}
	i, _ := fx.r.Instances.Get(ctx, "inst_1")
	if i.ProviderInstanceID != "prov-1" || i.DesiredState != instance.DesiredDeleted {
		t.Errorf("%+v", i)
	}
	due, _ := fx.r.Instances.ListDue(ctx, now.Add(time.Second), 10)
	if len(due) != 1 {
		t.Errorf("due: %d", len(due))
	}
	due, _ = fx.r.Instances.ListDue(ctx, now.Add(-time.Hour), 10)
	if len(due) != 0 {
		t.Errorf("not due yet: %d", len(due))
	}
	if err := fx.r.Instances.MarkDeleted(ctx, "inst_1", 1, now); err != nil {
		t.Fatal(err)
	}
	i, _ = fx.r.Instances.Get(ctx, "inst_1")
	if i.ObservedState != instance.Deleted || i.DeletedAt == nil || i.NodeID != "" {
		t.Errorf("after delete: %+v", i)
	}
	if list, _ := fx.r.Instances.List(ctx, "t1"); len(list) != 0 {
		t.Errorf("deleted instance listed: %d", len(list))
	}
	if n, _ := fx.r.Nodes.Get(ctx, "node-01"); n.ActiveInstances != 0 {
		t.Errorf("capacity leak: %d", n.ActiveInstances)
	}
}

func operationsContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	op := instance.Operation{ID: "op_1", TenantID: "t1", InstanceID: "inst_1", Type: instance.OpMigrate,
		Status: instance.OpRunning, Step: string(ownership.StepRequested)}
	if err := fx.r.Operations.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := fx.r.Operations.Create(ctx, op); !errors.Is(err, errs.ErrAlreadyExists) && !errors.Is(err, errs.ErrInProgress) {
		t.Errorf("duplicate id: %v", err)
	}
	op2 := op
	op2.ID = "op_2"
	if err := fx.r.Operations.Create(ctx, op2); !errors.Is(err, errs.ErrInProgress) {
		t.Errorf("second active migration for the instance must be refused: %v", err)
	}
	if _, err := fx.r.Operations.Advance(ctx, "op_1", string(ownership.StepFencingOldOwner), string(ownership.StepOldOwnerFenced), instance.OpRunning, ports.OperationPatch{}); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("CAS on wrong step: %v", err)
	}
	if _, err := fx.r.Operations.Advance(ctx, "op_1", string(ownership.StepRequested), string(ownership.StepAssignNewEpoch), instance.OpRunning, ports.OperationPatch{}); !errors.Is(err, errs.ErrInvalidTransition) {
		t.Errorf("illegal migration jump: %v", err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for k := 0; k < 6; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := fx.r.Operations.Advance(ctx, "op_1", string(ownership.StepRequested), string(ownership.StepFencingOldOwner), instance.OpRunning, ports.OperationPatch{TargetNodeID: "node-02"}); err == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Errorf("concurrent Advance must have exactly one winner, got %d", wins.Load())
	}
	stepStart := func() time.Time { o, _ := fx.r.Operations.Get(ctx, "op_1"); return o.StepStartedAt }
	before := stepStart()
	if before.IsZero() {
		t.Error("step_started_at must be set")
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := fx.r.Operations.Advance(ctx, "op_1", string(ownership.StepFencingOldOwner), string(ownership.StepFencingOldOwner), instance.OpRunning, ports.OperationPatch{BumpAttempts: true}); err != nil {
		t.Fatal(err)
	}
	if !stepStart().Equal(before) {
		t.Error("re-recording the same step must not restart its clock")
	}
	if _, err := fx.r.Operations.Advance(ctx, "op_1", string(ownership.StepFencingOldOwner), string(ownership.StepBlocked), instance.OpBlocked, ports.OperationPatch{ErrorCode: "FENCE_FAILED", ErrorMessage: "boom"}); err != nil {
		t.Fatal(err)
	}
	if !stepStart().After(before) {
		t.Error("a new step must restart the step clock")
	}
	got, _ := fx.r.Operations.Get(ctx, "op_1")
	if got.Status != instance.OpBlocked || got.ErrorCode != "FENCE_FAILED" || got.TargetNodeID != "node-02" {
		t.Errorf("%+v", got)
	}
	if a, err := fx.r.Operations.FindActive(ctx, "inst_1", instance.OpMigrate); err != nil || a.ID != "op_1" {
		t.Errorf("blocked migrations stay active: %v %v", a, err)
	}
	if list, _ := fx.r.Operations.ListActive(ctx, instance.OpMigrate, 10); len(list) != 1 {
		t.Errorf("list active: %d", len(list))
	}
	if err := fx.r.Operations.Complete(ctx, "op_1", instance.OpFailed, "X", "done", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.r.Operations.FindActive(ctx, "inst_1", instance.OpMigrate); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("completed op must not be active: %v", err)
	}
	if err := fx.r.Operations.Create(ctx, op2); err != nil {
		t.Errorf("a new migration may start after the previous one ended: %v", err)
	}
}

func messagesContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.node(t, "node-01", 5)
	fx.instance(t, "inst_1", "t1")
	m := messaging.Message{ID: "msg_1", TenantID: "t1", InstanceID: "inst_1", NodeID: "node-01", AssignmentEpoch: 1,
		PartitionKey: "inst_1", Recipient: "5562", Type: messaging.TypeText, Payload: json.RawMessage(`{"text":"hi"}`), Status: messaging.StatusQueued}
	if err := fx.r.Messages.Create(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := fx.r.Messages.Create(ctx, m); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Errorf("dup: %v", err)
	}
	if _, err := fx.r.Messages.Transition(ctx, "msg_1", []messaging.Status{messaging.StatusDispatching}, messaging.StatusAccepted, ports.MessagePatch{}); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("CAS must fail from wrong status: %v", err)
	}
	got, err := fx.r.Messages.Transition(ctx, "msg_1", []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{BumpAttempt: true})
	if err != nil || got.AttemptCount != 1 {
		t.Fatalf("dispatching: %+v %v", got, err)
	}
	// only one worker may move QUEUED -> DISPATCHING
	_, _ = fx.r.Messages.Transition(ctx, "msg_1", []messaging.Status{messaging.StatusDispatching}, messaging.StatusQueued, ports.MessagePatch{})
	var wins atomic.Int32
	var wg sync.WaitGroup
	for k := 0; k < 6; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := fx.r.Messages.Transition(ctx, "msg_1", []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{BumpAttempt: true}); err == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Errorf("exactly one dispatcher must win, got %d", wins.Load())
	}
	if _, err := fx.r.Messages.Transition(ctx, "msg_1", []messaging.Status{messaging.StatusDispatching}, messaging.StatusAccepted, ports.MessagePatch{ProviderMessageID: "pm-1"}); err != nil {
		t.Fatal(err)
	}
	if applied, err := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "pm-1", messaging.StatusRead); err != nil || !applied {
		t.Errorf("read: %v %v", applied, err)
	}
	if applied, _ := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "pm-1", messaging.StatusDelivered); applied {
		t.Error("receipt regression must be ignored")
	}
	if _, err := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "ghost", messaging.StatusRead); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("unknown provider id: %v", err)
	}
	final, _ := fx.r.Messages.Get(ctx, "msg_1")
	if final.Status != messaging.StatusRead || final.ProviderMessageID != "pm-1" || final.AttemptCount != 2 {
		t.Errorf("%+v", final)
	}
	if applied, _ := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "pm-1", messaging.StatusFailed); applied {
		t.Error("a READ message can no longer fail")
	}
	// provider-reported failure: ACCEPTED -> FAILED
	m2 := m
	m2.ID = "msg_2"
	if err := fx.r.Messages.Create(ctx, m2); err != nil {
		t.Fatal(err)
	}
	_, _ = fx.r.Messages.Transition(ctx, "msg_2", []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{})
	_, _ = fx.r.Messages.Transition(ctx, "msg_2", []messaging.Status{messaging.StatusDispatching}, messaging.StatusAccepted, ports.MessagePatch{ProviderMessageID: "pm-2"})
	if applied, err := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "pm-2", messaging.StatusFailed); err != nil || !applied {
		t.Fatalf("ACCEPTED -> FAILED: %v %v", applied, err)
	}
	if f, _ := fx.r.Messages.Get(ctx, "msg_2"); f.Status != messaging.StatusFailed || f.ErrorCode != "PROVIDER_FAILED" {
		t.Errorf("%+v", f)
	}
	// UNKNOWN -> FAILED too (an ambiguous send later reported as failed)
	m3 := m
	m3.ID = "msg_3"
	_ = fx.r.Messages.Create(ctx, m3)
	_, _ = fx.r.Messages.Transition(ctx, "msg_3", []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{})
	_, _ = fx.r.Messages.Transition(ctx, "msg_3", []messaging.Status{messaging.StatusDispatching}, messaging.StatusUnknown, ports.MessagePatch{ProviderMessageID: "pm-3"})
	if applied, _ := fx.r.Messages.ApplyProviderStatus(ctx, "inst_1", "pm-3", messaging.StatusFailed); !applied {
		t.Error("UNKNOWN -> FAILED must be applied")
	}
}

func idempotencyContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	now := time.Now()
	rec := ports.IdempotencyRecord{TenantID: "t1", Key: "k1", RequestHash: "h1", Operation: "create_instance", ResourceID: "inst_1", ExpiresAt: now.Add(time.Hour)}
	got, claimed, err := fx.r.Idempotency.Begin(ctx, rec)
	if err != nil || !claimed || got.Status != ports.IdemInProgress {
		t.Fatalf("first begin: %v %v %+v", err, claimed, got)
	}
	other := rec
	other.ResourceID = "inst_2"
	got, claimed, _ = fx.r.Idempotency.Begin(ctx, other)
	if claimed || got.ResourceID != "inst_1" {
		t.Errorf("second begin must return the first record: %+v claimed=%v", got, claimed)
	}
	if err := fx.r.Idempotency.Complete(ctx, "t1", "k1", json.RawMessage(`{"id":"inst_1"}`)); err != nil {
		t.Fatal(err)
	}
	got, claimed, _ = fx.r.Idempotency.Begin(ctx, rec)
	var decoded map[string]string
	_ = json.Unmarshal(got.Result, &decoded) // stores may normalise JSON whitespace
	if claimed || got.Status != ports.IdemCompleted || decoded["id"] != "inst_1" {
		t.Errorf("replay: %+v", got)
	}
	// tenants are isolated
	rec2 := rec
	rec2.TenantID = "t2"
	if _, claimed, _ := fx.r.Idempotency.Begin(ctx, rec2); !claimed {
		t.Error("same key under another tenant must be independent")
	}
	// concurrent begin: exactly one claim
	var claims atomic.Int32
	var wg sync.WaitGroup
	for k := 0; k < 10; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := ports.IdempotencyRecord{TenantID: "t1", Key: "race", RequestHash: "h", Operation: "x", ResourceID: "r", ExpiresAt: now.Add(time.Hour)}
			if _, c, err := fx.r.Idempotency.Begin(ctx, r); err == nil && c {
				claims.Add(1)
			}
		}()
	}
	wg.Wait()
	if claims.Load() != 1 {
		t.Errorf("concurrent begin must have one claimant, got %d", claims.Load())
	}
	// abandon frees in-progress keys only
	if err := fx.r.Idempotency.Abandon(ctx, "t1", "race"); err != nil {
		t.Fatal(err)
	}
	if _, c, _ := fx.r.Idempotency.Begin(ctx, ports.IdempotencyRecord{TenantID: "t1", Key: "race", RequestHash: "h", ExpiresAt: now.Add(time.Hour)}); !c {
		t.Error("abandoned key must be claimable again")
	}
	if err := fx.r.Idempotency.Abandon(ctx, "t1", "k1"); err != nil {
		t.Fatal(err)
	}
	if _, c, _ := fx.r.Idempotency.Begin(ctx, rec); c {
		t.Error("completed records must survive Abandon")
	}
	// expiry
	exp := ports.IdempotencyRecord{TenantID: "t1", Key: "old", RequestHash: "h", ExpiresAt: now.Add(-time.Minute)}
	_, _, _ = fx.r.Idempotency.Begin(ctx, exp)
	if n, err := fx.r.Idempotency.DeleteExpired(ctx, now); err != nil || n < 1 {
		t.Errorf("delete expired: %d %v", n, err)
	}
}

func dedupContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	ttl, inflight := time.Hour, 200*time.Millisecond
	o, err := fx.r.Dedup.Begin(ctx, "k1", "inst_1", ttl, inflight)
	if err != nil || o != ports.DedupProceed {
		t.Fatalf("first: %v %v", o, err)
	}
	if o, _ := fx.r.Dedup.Begin(ctx, "k1", "inst_1", ttl, inflight); o != ports.DedupDuplicate {
		t.Error("in-flight duplicate must be suppressed")
	}
	if err := fx.r.Dedup.Abort(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	if o, _ := fx.r.Dedup.Begin(ctx, "k1", "inst_1", ttl, inflight); o != ports.DedupProceed {
		t.Error("aborted claim must be retryable")
	}
	if err := fx.r.Dedup.Commit(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(inflight + 50*time.Millisecond)
	if o, _ := fx.r.Dedup.Begin(ctx, "k1", "inst_1", ttl, inflight); o != ports.DedupDuplicate {
		t.Error("committed keys stay duplicates forever (until ttl)")
	}
	if err := fx.r.Dedup.Abort(ctx, "k1"); err != nil {
		t.Fatal(err)
	}
	if o, _ := fx.r.Dedup.Begin(ctx, "k1", "inst_1", ttl, inflight); o != ports.DedupDuplicate {
		t.Error("Abort must not undo a commit")
	}
	// crashed publisher: stale in-flight claim is retaken
	_, _ = fx.r.Dedup.Begin(ctx, "k2", "inst_1", ttl, inflight)
	time.Sleep(inflight + 50*time.Millisecond)
	if o, _ := fx.r.Dedup.Begin(ctx, "k2", "inst_1", ttl, inflight); o != ports.DedupProceed {
		t.Error("expired in-flight claim must be retaken")
	}
	// concurrency: a single winner
	var wins atomic.Int32
	var wg sync.WaitGroup
	for k := 0; k < 12; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if o, err := fx.r.Dedup.Begin(ctx, "k3", "inst_1", ttl, time.Minute); err == nil && o == ports.DedupProceed {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Errorf("INV-05: exactly one concurrent duplicate may proceed, got %d", wins.Load())
	}
	if n, err := fx.r.Dedup.DeleteExpired(ctx, time.Now().Add(2*time.Hour)); err != nil || n < 1 {
		t.Errorf("delete expired: %d %v", n, err)
	}
}

func blobMetaContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	now := time.Now()
	b := media.Blob{ID: "med_1", TenantID: "t1", ObjectKey: "t1/media/med_1/a.pdf", ContentType: "application/pdf", Filename: "a.pdf",
		Status: media.BlobPending, ExpiresAt: now.Add(-time.Minute)}
	if err := fx.r.Blobs.Create(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := fx.r.Blobs.Create(ctx, b); !errors.Is(err, errs.ErrAlreadyExists) {
		t.Errorf("dup: %v", err)
	}
	if err := fx.r.Blobs.MarkReady(ctx, "med_1", 10, "ab", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, _ := fx.r.Blobs.GetByKey(ctx, b.ObjectKey)
	if got.Status != media.BlobReady || got.Size != 10 || got.SHA256 != "ab" {
		t.Errorf("%+v", got)
	}
	if got.ExpiresAt.Before(now) {
		t.Errorf("MarkReady must extend the retention: %v", got.ExpiresAt)
	}
	if exp, _ := fx.r.Blobs.ListExpired(ctx, now, 10); len(exp) != 0 {
		t.Errorf("a READY blob inside its retention is not expired: %d", len(exp))
	}
	exp, _ := fx.r.Blobs.ListExpired(ctx, now.Add(2*time.Hour), 10)
	if len(exp) != 1 {
		t.Errorf("expired: %d", len(exp))
	}
	if err := fx.r.Blobs.MarkDeleted(ctx, "med_1", now); err != nil {
		t.Fatal(err)
	}
	if exp, _ := fx.r.Blobs.ListExpired(ctx, now, 10); len(exp) != 0 {
		t.Errorf("deleted blobs are not expired candidates: %d", len(exp))
	}
	if err := fx.r.Blobs.MarkReady(ctx, "med_1", 1, "x", now); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("deleted blob cannot become ready: %v", err)
	}
}
