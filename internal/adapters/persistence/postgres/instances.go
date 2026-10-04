package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/ports"
)

// ---------------- tenants ----------------

type tenantRepo struct{ s *Store }

func marshalPolicy(p *messaging.RatePolicy) ([]byte, error) {
	if p == nil {
		return nil, nil
	}
	return json.Marshal(p)
}

func unmarshalPolicy(raw []byte) *messaging.RatePolicy {
	if len(raw) == 0 {
		return nil
	}
	var p messaging.RatePolicy
	if json.Unmarshal(raw, &p) != nil {
		return nil
	}
	return &p
}

func (r tenantRepo) Create(ctx context.Context, t instance.Tenant) error {
	pol, err := marshalPolicy(t.RatePolicy)
	if err != nil {
		return err
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	k := instance.InitialKey(t)
	err = r.s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO tenants(id,name,rate_policy,created_at) VALUES($1,$2,$3,$4)`, t.ID, t.Name, pol, t.CreatedAt); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO api_keys(id,tenant_id,name,key_prefix,key_hash,created_at) VALUES($1,$2,$3,$4,$5,$6)`,
			k.ID, k.TenantID, k.Name, k.Prefix, k.KeyHash, k.CreatedAt)
		return err
	})
	if _, code := constraint(err); code == "23505" {
		return errs.ErrAlreadyExists
	}
	return err
}

func scanTenant(row pgx.Row) (*instance.Tenant, error) {
	var t instance.Tenant
	var pol []byte
	if err := row.Scan(&t.ID, &t.Name, &t.APIKeyHash, &pol, &t.CreatedAt); err != nil {
		return nil, notFound(err)
	}
	t.RatePolicy = unmarshalPolicy(pol)
	return &t, nil
}

const tenantCols = `id,name,coalesce(api_key_hash,''),rate_policy,created_at`

func (r tenantRepo) Get(ctx context.Context, id string) (*instance.Tenant, error) {
	return scanTenant(r.s.pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE id=$1`, id))
}

func (r tenantRepo) GetByAPIKeyHash(ctx context.Context, hash string) (*instance.Tenant, error) {
	return scanTenant(r.s.pool.QueryRow(ctx, `SELECT t.id,t.name,coalesce(t.api_key_hash,''),t.rate_policy,t.created_at FROM tenants t JOIN api_keys k ON k.tenant_id=t.id
		WHERE k.key_hash=$1 AND k.revoked_at IS NULL AND (k.expires_at IS NULL OR k.expires_at > now())`, hash))
}

func (r tenantRepo) SetRatePolicy(ctx context.Context, id string, p *messaging.RatePolicy) error {
	pol, err := marshalPolicy(p)
	if err != nil {
		return err
	}
	tag, err := r.s.pool.Exec(ctx, `UPDATE tenants SET rate_policy=$2 WHERE id=$1`, id, pol)
	if err == nil && tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return err
}

// ---------------- nodes ----------------

type nodeRepo struct{ s *Store }

const nodeCols = `id,provider,provider_version,endpoint,capacity,active_instances,status,heartbeat_at,created_at,updated_at`

func scanNode(row pgx.Row) (routing.Node, error) {
	var n routing.Node
	var hb *time.Time
	var st string
	err := row.Scan(&n.ID, &n.Provider, &n.ProviderVersion, &n.Endpoint, &n.Capacity, &n.ActiveInstances, &st, &hb, &n.CreatedAt, &n.UpdatedAt)
	n.Status, n.HeartbeatAt = routing.NodeStatus(st), zeroIfNil(hb)
	return n, err
}

func (r nodeRepo) Upsert(ctx context.Context, n routing.Node) error {
	_, err := r.s.pool.Exec(ctx, `INSERT INTO provider_nodes(id,provider,provider_version,endpoint,capacity,status)
		VALUES($1,$2,$3,$4,$5,'STARTING')
		ON CONFLICT (id) DO UPDATE SET provider=EXCLUDED.provider, endpoint=EXCLUDED.endpoint, capacity=EXCLUDED.capacity,
			provider_version = CASE WHEN EXCLUDED.provider_version <> '' THEN EXCLUDED.provider_version ELSE provider_nodes.provider_version END,
			updated_at=now()`,
		n.ID, n.Provider, n.ProviderVersion, n.Endpoint, n.Capacity)
	if _, code := constraint(err); code == "23514" {
		return fmt.Errorf("%w: capacity cannot be lower than the instances already placed", errs.ErrInvalidArgument)
	}
	return err
}

func (r nodeRepo) Get(ctx context.Context, id string) (*routing.Node, error) {
	n, err := scanNode(r.s.pool.QueryRow(ctx, `SELECT `+nodeCols+` FROM provider_nodes WHERE id=$1`, id))
	if err != nil {
		return nil, notFound(err)
	}
	return &n, nil
}

func (r nodeRepo) List(ctx context.Context) ([]routing.Node, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT `+nodeCols+` FROM provider_nodes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []routing.Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (r nodeRepo) SetStatus(ctx context.Context, id string, to routing.NodeStatus) (*routing.Node, error) {
	var out *routing.Node
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		n, err := scanNode(tx.QueryRow(ctx, `SELECT `+nodeCols+` FROM provider_nodes WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return notFound(err)
		}
		if !routing.CanTransitionNode(n.Status, to) {
			return fmt.Errorf("%w: node %s -> %s", errs.ErrInvalidTransition, n.Status, to)
		}
		n, err = scanNode(tx.QueryRow(ctx, `UPDATE provider_nodes SET status=$2, updated_at=now() WHERE id=$1 RETURNING `+nodeCols, id, string(to)))
		out = &n
		return err
	})
	return out, err
}

func (r nodeRepo) RecordProbe(ctx context.Context, id string, status routing.NodeStatus, version string, ok bool, at time.Time) error {
	return r.s.withTx(ctx, func(tx pgx.Tx) error {
		n, err := scanNode(tx.QueryRow(ctx, `SELECT `+nodeCols+` FROM provider_nodes WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return notFound(err)
		}
		next := n.Status
		if routing.CanTransitionNode(n.Status, status) {
			next = status
		}
		var hb *time.Time
		if ok {
			hb = &at
		}
		_, err = tx.Exec(ctx, `UPDATE provider_nodes SET status=$2,
			heartbeat_at = COALESCE($3, heartbeat_at),
			provider_version = CASE WHEN $4 <> '' THEN $4 ELSE provider_version END, updated_at=$5 WHERE id=$1`,
			id, string(next), hb, version, at)
		return err
	})
}

// ---------------- instances ----------------

type instanceRepo struct{ s *Store }

const instCols = `id,tenant_id,name,provider,provider_instance_id,COALESCE(node_id,''),assignment_epoch,desired_state,observed_state,
	last_provider_heartbeat,last_status_change,rate_policy,reconciled_at,created_at,updated_at,deleted_at`

func scanInstance(row pgx.Row) (*instance.Instance, error) {
	var i instance.Instance
	var hb *time.Time
	var pol []byte
	var desired, observed string
	err := row.Scan(&i.ID, &i.TenantID, &i.Name, &i.Provider, &i.ProviderInstanceID, &i.NodeID, &i.AssignmentEpoch,
		&desired, &observed, &hb, &i.LastStatusChange, &pol, &i.ReconciledAt, &i.CreatedAt, &i.UpdatedAt, &i.DeletedAt)
	if err != nil {
		return nil, notFound(err)
	}
	i.DesiredState, i.ObservedState = instance.DesiredState(desired), instance.ObservedState(observed)
	i.LastProviderHeartbeat, i.RatePolicy = zeroIfNil(hb), unmarshalPolicy(pol)
	return &i, nil
}

func (r instanceRepo) CreateWithPlacement(ctx context.Context, req ports.PlacementRequest) (*instance.Instance, error) {
	var out *instance.Instance
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		if cur, err := scanInstance(tx.QueryRow(ctx, `SELECT `+instCols+` FROM instances WHERE id=$1`, req.Instance.ID)); err == nil {
			out = cur
			return nil
		} else if !errors.Is(err, errs.ErrNotFound) {
			return err
		}
		// Lock every candidate row in a stable order. Concurrent creators
		// serialise here; after the winner commits, the loser's WHERE clause is
		// re-evaluated against the new row version, so a full node drops out.
		rows, err := tx.Query(ctx, `SELECT `+nodeCols+` FROM provider_nodes
			WHERE provider=$1 AND status='READY' AND active_instances < capacity ORDER BY id FOR UPDATE`, req.Provider)
		if err != nil {
			return err
		}
		var cands []routing.Node
		for rows.Next() {
			n, err := scanNode(rows)
			if err != nil {
				rows.Close()
				return err
			}
			cands = append(cands, n)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		nodeID, err := req.Choose(cands)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE provider_nodes SET active_instances=active_instances+1, updated_at=now()
			WHERE id=$1 AND status='READY' AND active_instances < capacity`, nodeID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errs.ErrNoCapacity
		}
		in := req.Instance
		pol, err := marshalPolicy(in.RatePolicy)
		if err != nil {
			return err
		}
		tag, err = tx.Exec(ctx, `INSERT INTO instances(id,tenant_id,name,provider,provider_instance_id,node_id,assignment_epoch,
				desired_state,observed_state,rate_policy)
			VALUES($1,$2,$3,$4,$5,$6,1,$7,'ALLOCATING',$8) ON CONFLICT (id) DO NOTHING`,
			in.ID, in.TenantID, in.Name, in.Provider, in.ProviderInstanceID, nodeID, string(in.DesiredState), pol)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 { // a concurrent request created it first: undo our reservation
			return errConcurrentCreate
		}
		if _, err := tx.Exec(ctx, `INSERT INTO instance_assignments(instance_id,node_id,epoch) VALUES($1,$2,1)`, in.ID, nodeID); err != nil {
			return err
		}
		out, err = scanInstance(tx.QueryRow(ctx, `SELECT `+instCols+` FROM instances WHERE id=$1`, in.ID))
		return err
	})
	if errors.Is(err, errConcurrentCreate) {
		return r.Get(ctx, req.Instance.ID)
	}
	return out, err
}

var errConcurrentCreate = errors.New("concurrent create")

func (r instanceRepo) Get(ctx context.Context, id string) (*instance.Instance, error) {
	return scanInstance(r.s.pool.QueryRow(ctx, `SELECT `+instCols+` FROM instances WHERE id=$1`, id))
}

func (r instanceRepo) queryInstances(ctx context.Context, sql string, args ...any) ([]instance.Instance, error) {
	rows, err := r.s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []instance.Instance
	for rows.Next() {
		i, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *i)
	}
	return out, rows.Err()
}

func (r instanceRepo) List(ctx context.Context, tenantID string) ([]instance.Instance, error) {
	return r.queryInstances(ctx, `SELECT `+instCols+` FROM instances WHERE tenant_id=$1 AND deleted_at IS NULL ORDER BY id`, tenantID)
}

func (r instanceRepo) ListDue(ctx context.Context, before time.Time, limit int) ([]instance.Instance, error) {
	return r.queryInstances(ctx, `SELECT `+instCols+` FROM instances
		WHERE deleted_at IS NULL AND observed_state <> 'DELETED' AND reconciled_at < $1
		ORDER BY reconciled_at LIMIT $2`, before, limit)
}

func (r instanceRepo) UpdateDesired(ctx context.Context, id string, st instance.DesiredState) error {
	if !st.Valid() {
		return errs.ErrInvalidArgument
	}
	tag, err := r.s.pool.Exec(ctx, `UPDATE instances SET desired_state=$2, updated_at=now() WHERE id=$1`, id, string(st))
	if err == nil && tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return err
}

func (r instanceRepo) SetObserved(ctx context.Context, id string, epoch int64, st instance.ObservedState, at time.Time) (bool, error) {
	changed := false
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		var curEpoch int64
		var cur string
		if err := tx.QueryRow(ctx, `SELECT assignment_epoch, observed_state FROM instances WHERE id=$1 FOR UPDATE`, id).Scan(&curEpoch, &cur); err != nil {
			return notFound(err)
		}
		if curEpoch != epoch {
			return fmt.Errorf("%w: epoch %d != %d", errs.ErrStaleAssignment, epoch, curEpoch)
		}
		if instance.ObservedState(cur) == st {
			changed = false
			return nil
		}
		if !instance.CanTransition(instance.ObservedState(cur), st) {
			return fmt.Errorf("%w: %s -> %s", errs.ErrInvalidTransition, cur, st)
		}
		changed = true
		_, err := tx.Exec(ctx, `UPDATE instances SET observed_state=$2, last_status_change=$3, updated_at=$3 WHERE id=$1`, id, string(st), at)
		return err
	})
	return changed, err
}

func (r instanceRepo) SetProviderInstance(ctx context.Context, id string, epoch int64, pid string) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE instances SET provider_instance_id=$3, updated_at=now() WHERE id=$1 AND assignment_epoch=$2`, id, epoch, pid)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if _, gerr := r.Get(ctx, id); gerr != nil {
			return gerr
		}
		return errs.ErrStaleAssignment
	}
	return nil
}

func (r instanceRepo) TouchHeartbeat(ctx context.Context, id string, epoch int64, at time.Time) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE instances SET last_provider_heartbeat=$3 WHERE id=$1 AND assignment_epoch=$2`, id, epoch, at)
	if err == nil && tag.RowsAffected() == 0 {
		if _, gerr := r.Get(ctx, id); gerr != nil {
			return gerr
		}
		return errs.ErrStaleAssignment
	}
	return err
}

func (r instanceRepo) MarkReconciled(ctx context.Context, id string, at time.Time) error {
	_, err := r.s.pool.Exec(ctx, `UPDATE instances SET reconciled_at=$2 WHERE id=$1`, id, at)
	return err
}

func (r instanceRepo) SetRatePolicy(ctx context.Context, id string, p *messaging.RatePolicy) error {
	pol, err := marshalPolicy(p)
	if err != nil {
		return err
	}
	tag, err := r.s.pool.Exec(ctx, `UPDATE instances SET rate_policy=$2, updated_at=now() WHERE id=$1`, id, pol)
	if err == nil && tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return err
}

// closeAssignment closes the open assignment and frees capacity on its node.
func closeAssignment(ctx context.Context, tx pgx.Tx, instanceID string, reason ownership.ReleaseReason) error {
	var node string
	err := tx.QueryRow(ctx, `UPDATE instance_assignments SET released_at=now(), release_reason=$2
		WHERE instance_id=$1 AND released_at IS NULL RETURNING node_id`, instanceID, string(reason)).Scan(&node)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // already released
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE provider_nodes SET active_instances=GREATEST(active_instances-1,0), updated_at=now() WHERE id=$1`, node)
	return err
}

func lockInstance(ctx context.Context, tx pgx.Tx, id string) (epoch int64, node string, err error) {
	err = tx.QueryRow(ctx, `SELECT assignment_epoch, COALESCE(node_id,'') FROM instances WHERE id=$1 FOR UPDATE`, id).Scan(&epoch, &node)
	return epoch, node, notFound(err)
}

func (r instanceRepo) Reassign(ctx context.Context, req ports.ReassignRequest) (ownership.Assignment, error) {
	var out ownership.Assignment
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		epoch, oldNode, err := lockInstance(ctx, tx, req.InstanceID)
		if err != nil {
			return err
		}
		// INV-09: the migration operation must have confirmed the old owner fenced.
		var step, opInstance, opType string
		err = tx.QueryRow(ctx, `SELECT step, COALESCE(instance_id,''), type FROM operations WHERE id=$1 FOR UPDATE`, req.OperationID).Scan(&step, &opInstance, &opType)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && (opInstance != req.InstanceID || opType != string(instance.OpMigrate))) {
			return fmt.Errorf("%w: migration operation %q not found", errs.ErrFencingRequired, req.OperationID)
		}
		if err != nil {
			return err
		}
		if ownership.MigrationStep(step) != ownership.StepOldOwnerFenced {
			return fmt.Errorf("%w: operation at step %s", errs.ErrFencingRequired, step)
		}
		if epoch != req.ExpectedEpoch || oldNode == "" {
			return fmt.Errorf("%w: expected epoch %d, current %d", errs.ErrStaleAssignment, req.ExpectedEpoch, epoch)
		}
		// lock both nodes in id order (deadlock-free)
		rows, err := tx.Query(ctx, `SELECT `+nodeCols+` FROM provider_nodes WHERE id IN ($1,$2) ORDER BY id FOR UPDATE`, oldNode, req.NewNodeID)
		if err != nil {
			return err
		}
		found := map[string]routing.Node{}
		for rows.Next() {
			n, err := scanNode(rows)
			if err != nil {
				rows.Close()
				return err
			}
			found[n.ID] = n
		}
		rows.Close()
		nn, ok := found[req.NewNodeID]
		if !ok {
			return errs.ErrNotFound
		}
		if nn.Status != routing.NodeReady || nn.ActiveInstances >= nn.Capacity {
			return errs.ErrNoCapacity
		}
		if err := closeAssignment(ctx, tx, req.InstanceID, req.Reason); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE provider_nodes SET active_instances=active_instances+1, updated_at=now() WHERE id=$1`, req.NewNodeID); err != nil {
			return err
		}
		newEpoch := ownership.NextEpoch(epoch)
		if _, err := tx.Exec(ctx, `UPDATE instances SET node_id=$2, assignment_epoch=$3, updated_at=now() WHERE id=$1`, req.InstanceID, req.NewNodeID, newEpoch); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO instance_assignments(instance_id,node_id,epoch) VALUES($1,$2,$3)`, req.InstanceID, req.NewNodeID, newEpoch); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE operations SET step=$2, step_started_at=now(), updated_at=now() WHERE id=$1`, req.OperationID, string(ownership.StepAssignNewEpoch)); err != nil {
			return err
		}
		out = ownership.Assignment{InstanceID: req.InstanceID, NodeID: req.NewNodeID, Epoch: newEpoch}
		return nil
	})
	return out, err
}

func (r instanceRepo) Release(ctx context.Context, id string, epoch int64, reason ownership.ReleaseReason) error {
	return r.s.withTx(ctx, func(tx pgx.Tx) error {
		cur, node, err := lockInstance(ctx, tx, id)
		if err != nil {
			return err
		}
		if cur != epoch {
			return errs.ErrStaleAssignment
		}
		if node == "" {
			return nil
		}
		if err := closeAssignment(ctx, tx, id, reason); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE instances SET node_id=NULL, updated_at=now() WHERE id=$1`, id)
		return err
	})
}

func (r instanceRepo) MarkDeleted(ctx context.Context, id string, epoch int64, at time.Time) error {
	return r.s.withTx(ctx, func(tx pgx.Tx) error {
		cur, _, err := lockInstance(ctx, tx, id)
		if err != nil {
			return err
		}
		if cur != epoch {
			return errs.ErrStaleAssignment
		}
		if err := closeAssignment(ctx, tx, id, ownership.ReleaseDeleted); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE instances SET node_id=NULL, desired_state='DELETED', observed_state='DELETED',
			deleted_at=$2, last_status_change=$2, updated_at=$2 WHERE id=$1`, id, at)
		return err
	})
}

func (r instanceRepo) Assignments(ctx context.Context, id string) ([]ownership.AssignmentRecord, error) {
	if _, err := r.Get(ctx, id); err != nil {
		return nil, err
	}
	rows, err := r.s.pool.Query(ctx, `SELECT instance_id,node_id,epoch,assigned_at,released_at,COALESCE(release_reason,'')
		FROM instance_assignments WHERE instance_id=$1 ORDER BY epoch`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ownership.AssignmentRecord
	for rows.Next() {
		var a ownership.AssignmentRecord
		var reason string
		if err := rows.Scan(&a.InstanceID, &a.NodeID, &a.Epoch, &a.AssignedAt, &a.ReleasedAt, &reason); err != nil {
			return nil, err
		}
		a.ReleaseReason = ownership.ReleaseReason(reason)
		out = append(out, a)
	}
	return out, rows.Err()
}

func (r instanceRepo) CountByState(ctx context.Context) (map[instance.ObservedState]int, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT observed_state, count(*) FROM instances WHERE deleted_at IS NULL GROUP BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[instance.ObservedState]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[instance.ObservedState(st)] = n
	}
	return out, rows.Err()
}
