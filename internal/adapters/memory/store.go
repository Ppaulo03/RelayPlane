// Package memory provides in-memory implementations of every port. It is the
// reference semantics for the contract suites and the backbone of unit tests;
// it is not meant for production use.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/ports"
)

// Store is a single in-memory database implementing all repository ports.
type Store struct {
	mu sync.Mutex
	// Now is the clock; override in tests.
	Now func() time.Time

	tenants     map[string]instance.Tenant
	instances   map[string]*instance.Instance
	assignments map[string][]ownership.AssignmentRecord
	nodes       map[string]*routing.Node
	operations  map[string]*instance.Operation
	messages    map[string]*messaging.Message
	blobs       map[string]*media.Blob
	idem        map[string]ports.IdempotencyRecord
	dedup       map[string]dedupRow
	nextSeq     map[string]int64
	outbox      []*messaging.OutboxEntry

	// BeforeCommit lets failure tests inject a fault into multi-step writes
	// (simulating a transaction rollback). Return an error to abort.
	BeforeCommit func(op string) error
}

type dedupRow struct {
	instanceID string
	committed  bool
	claimedAt  time.Time
	expiresAt  time.Time
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		Now:         time.Now,
		tenants:     map[string]instance.Tenant{},
		instances:   map[string]*instance.Instance{},
		assignments: map[string][]ownership.AssignmentRecord{},
		nodes:       map[string]*routing.Node{},
		operations:  map[string]*instance.Operation{},
		messages:    map[string]*messaging.Message{},
		blobs:       map[string]*media.Blob{},
		idem:        map[string]ports.IdempotencyRecord{},
		dedup:       map[string]dedupRow{},
		nextSeq:     map[string]int64{},
	}
}

// Repositories returns the port bundle backed by this store.
func (s *Store) Repositories() ports.Repositories {
	return ports.Repositories{
		Tenants: tenantRepo{s}, Instances: instanceRepo{s}, Nodes: nodeRepo{s},
		Operations: opRepo{s}, Messages: msgRepo{s}, Blobs: blobRepo{s},
		Idempotency: idemRepo{s}, Dedup: dedupRepo{s},
	}
}

func (s *Store) fault(op string) error {
	if s.BeforeCommit != nil {
		return s.BeforeCommit(op)
	}
	return nil
}

// ---- tenants ----

type tenantRepo struct{ s *Store }

func (r tenantRepo) Create(_ context.Context, t instance.Tenant) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if _, ok := r.s.tenants[t.ID]; ok {
		return errs.ErrAlreadyExists
	}
	for _, x := range r.s.tenants {
		if x.APIKeyHash == t.APIKeyHash {
			return errs.ErrAlreadyExists
		}
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = r.s.Now()
	}
	r.s.tenants[t.ID] = t
	return nil
}

func (r tenantRepo) Get(_ context.Context, id string) (*instance.Tenant, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	t, ok := r.s.tenants[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	return &t, nil
}

func (r tenantRepo) GetByAPIKeyHash(_ context.Context, hash string) (*instance.Tenant, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, t := range r.s.tenants {
		if t.APIKeyHash == hash {
			t := t
			return &t, nil
		}
	}
	return nil, errs.ErrNotFound
}

func (r tenantRepo) SetRatePolicy(_ context.Context, id string, p *messaging.RatePolicy) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	t, ok := r.s.tenants[id]
	if !ok {
		return errs.ErrNotFound
	}
	t.RatePolicy = p
	r.s.tenants[id] = t
	return nil
}

// ---- nodes ----

type nodeRepo struct{ s *Store }

func (r nodeRepo) Upsert(_ context.Context, n routing.Node) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	now := r.s.Now()
	if cur, ok := r.s.nodes[n.ID]; ok {
		cur.Provider, cur.ProviderVersion, cur.Endpoint, cur.Capacity = n.Provider, n.ProviderVersion, n.Endpoint, n.Capacity
		cur.UpdatedAt = now
		return nil
	}
	n.CreatedAt, n.UpdatedAt = now, now
	if n.Status == "" {
		n.Status = routing.NodeStarting
	}
	n.ActiveInstances = 0
	r.s.nodes[n.ID] = &n
	return nil
}

func (r nodeRepo) Get(_ context.Context, id string) (*routing.Node, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	n, ok := r.s.nodes[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	c := *n
	return &c, nil
}

func (r nodeRepo) List(_ context.Context) ([]routing.Node, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	out := make([]routing.Node, 0, len(r.s.nodes))
	for _, n := range r.s.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r nodeRepo) SetStatus(_ context.Context, id string, to routing.NodeStatus) (*routing.Node, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	n, ok := r.s.nodes[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	if !routing.CanTransitionNode(n.Status, to) {
		return nil, fmt.Errorf("%w: node %s -> %s", errs.ErrInvalidTransition, n.Status, to)
	}
	n.Status, n.UpdatedAt = to, r.s.Now()
	c := *n
	return &c, nil
}

func (r nodeRepo) RecordProbe(_ context.Context, id string, status routing.NodeStatus, version string, ok bool, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	n, found := r.s.nodes[id]
	if !found {
		return errs.ErrNotFound
	}
	if ok {
		n.HeartbeatAt = at
		if version != "" {
			n.ProviderVersion = version
		}
	}
	if routing.CanTransitionNode(n.Status, status) {
		n.Status = status
	}
	n.UpdatedAt = at
	return nil
}

// ---- instances ----

type instanceRepo struct{ s *Store }

func (r instanceRepo) CreateWithPlacement(_ context.Context, req ports.PlacementRequest) (*instance.Instance, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if cur, ok := r.s.instances[req.Instance.ID]; ok {
		c := *cur
		return &c, nil
	}
	var cands []routing.Node
	for _, n := range r.s.nodes {
		if n.Provider == req.Provider && n.Status == routing.NodeReady && n.ActiveInstances < n.Capacity {
			cands = append(cands, *n)
		}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].ID < cands[j].ID })
	nodeID, err := req.Choose(cands)
	if err != nil {
		return nil, err
	}
	node := r.s.nodes[nodeID]
	if node == nil || node.ActiveInstances >= node.Capacity {
		return nil, errs.ErrNoCapacity
	}
	if err := r.s.fault("create_with_placement"); err != nil {
		return nil, err
	}
	now := r.s.Now()
	node.ActiveInstances++
	inst := req.Instance
	inst.NodeID, inst.AssignmentEpoch = nodeID, 1
	inst.ObservedState = instance.Allocating
	inst.CreatedAt, inst.UpdatedAt, inst.LastStatusChange = now, now, now
	r.s.instances[inst.ID] = &inst
	r.s.assignments[inst.ID] = []ownership.AssignmentRecord{{InstanceID: inst.ID, NodeID: nodeID, Epoch: 1, AssignedAt: now}}
	c := inst
	return &c, nil
}

func (r instanceRepo) Get(_ context.Context, id string) (*instance.Instance, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	c := *i
	return &c, nil
}

func (r instanceRepo) List(_ context.Context, tenantID string) ([]instance.Instance, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []instance.Instance
	for _, i := range r.s.instances {
		if i.TenantID == tenantID && i.DeletedAt == nil {
			out = append(out, *i)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (r instanceRepo) ListDue(_ context.Context, before time.Time, limit int) ([]instance.Instance, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []instance.Instance
	for _, i := range r.s.instances {
		if i.DeletedAt == nil && i.ObservedState != instance.Deleted && i.ReconciledAt.Before(before) {
			out = append(out, *i)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ReconciledAt.Before(out[b].ReconciledAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r instanceRepo) UpdateDesired(_ context.Context, id string, st instance.DesiredState) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return errs.ErrNotFound
	}
	if !st.Valid() {
		return errs.ErrInvalidArgument
	}
	i.DesiredState, i.UpdatedAt = st, r.s.Now()
	return nil
}

func (r instanceRepo) SetObserved(_ context.Context, id string, epoch int64, st instance.ObservedState, at time.Time) (bool, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return false, errs.ErrNotFound
	}
	if i.AssignmentEpoch != epoch {
		return false, fmt.Errorf("%w: epoch %d != %d", errs.ErrStaleAssignment, epoch, i.AssignmentEpoch)
	}
	if i.ObservedState == st {
		return false, nil
	}
	if err := i.TransitionObserved(st, at); err != nil {
		return false, err
	}
	return true, nil
}

func (r instanceRepo) SetProviderInstance(_ context.Context, id string, epoch int64, pid string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return errs.ErrNotFound
	}
	if i.AssignmentEpoch != epoch {
		return errs.ErrStaleAssignment
	}
	i.ProviderInstanceID = pid
	return nil
}

func (r instanceRepo) TouchHeartbeat(_ context.Context, id string, epoch int64, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return errs.ErrNotFound
	}
	if i.AssignmentEpoch != epoch {
		return errs.ErrStaleAssignment
	}
	i.LastProviderHeartbeat = at
	return nil
}

func (r instanceRepo) MarkReconciled(_ context.Context, id string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if i, ok := r.s.instances[id]; ok {
		i.ReconciledAt = at
		return nil
	}
	return errs.ErrNotFound
}

// closeAssignment closes the open record and frees node capacity. Caller holds the lock.
func (s *Store) closeAssignment(id string, reason ownership.ReleaseReason, at time.Time) {
	recs := s.assignments[id]
	for k := range recs {
		if recs[k].ReleasedAt == nil {
			t := at
			recs[k].ReleasedAt, recs[k].ReleaseReason = &t, reason
			if n := s.nodes[recs[k].NodeID]; n != nil && n.ActiveInstances > 0 {
				n.ActiveInstances--
			}
		}
	}
}

func (r instanceRepo) Reassign(_ context.Context, req ports.ReassignRequest) (ownership.Assignment, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[req.InstanceID]
	if !ok {
		return ownership.Assignment{}, errs.ErrNotFound
	}
	op, ok := r.s.operations[req.OperationID]
	if !ok || op.InstanceID != i.ID || op.Type != instance.OpMigrate {
		return ownership.Assignment{}, fmt.Errorf("%w: migration operation %q not found", errs.ErrFencingRequired, req.OperationID)
	}
	// INV-09: the old owner must have been confirmed fenced.
	if ownership.MigrationStep(op.Step) != ownership.StepOldOwnerFenced {
		return ownership.Assignment{}, fmt.Errorf("%w: operation at step %s", errs.ErrFencingRequired, op.Step)
	}
	if i.AssignmentEpoch != req.ExpectedEpoch || i.NodeID == "" {
		return ownership.Assignment{}, fmt.Errorf("%w: expected epoch %d, current %d", errs.ErrStaleAssignment, req.ExpectedEpoch, i.AssignmentEpoch)
	}
	nn := r.s.nodes[req.NewNodeID]
	if nn == nil {
		return ownership.Assignment{}, errs.ErrNotFound
	}
	if nn.Status != routing.NodeReady || nn.ActiveInstances >= nn.Capacity {
		return ownership.Assignment{}, errs.ErrNoCapacity
	}
	if err := r.s.fault("reassign"); err != nil {
		return ownership.Assignment{}, err
	}
	now := r.s.Now()
	r.s.closeAssignment(i.ID, req.Reason, now)
	nn.ActiveInstances++
	i.NodeID, i.AssignmentEpoch, i.UpdatedAt = req.NewNodeID, ownership.NextEpoch(i.AssignmentEpoch), now
	r.s.assignments[i.ID] = append(r.s.assignments[i.ID], ownership.AssignmentRecord{
		InstanceID: i.ID, NodeID: i.NodeID, Epoch: i.AssignmentEpoch, AssignedAt: now})
	op.Step, op.UpdatedAt, op.StepStartedAt = string(ownership.StepAssignNewEpoch), now, now
	return i.Assignment(), nil
}

func (r instanceRepo) Release(_ context.Context, id string, epoch int64, reason ownership.ReleaseReason) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return errs.ErrNotFound
	}
	if i.AssignmentEpoch != epoch {
		return errs.ErrStaleAssignment
	}
	r.s.closeAssignment(id, reason, r.s.Now())
	i.NodeID = ""
	return nil
}

func (r instanceRepo) MarkDeleted(_ context.Context, id string, epoch int64, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return errs.ErrNotFound
	}
	if i.AssignmentEpoch != epoch {
		return errs.ErrStaleAssignment
	}
	r.s.closeAssignment(id, ownership.ReleaseDeleted, at)
	t := at
	i.NodeID, i.DeletedAt, i.DesiredState = "", &t, instance.DesiredDeleted
	i.ObservedState, i.LastStatusChange, i.UpdatedAt = instance.Deleted, at, at
	return nil
}

func (r instanceRepo) Assignments(_ context.Context, id string) ([]ownership.AssignmentRecord, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if _, ok := r.s.instances[id]; !ok {
		return nil, errs.ErrNotFound
	}
	return append([]ownership.AssignmentRecord(nil), r.s.assignments[id]...), nil
}

func (r instanceRepo) SetRatePolicy(_ context.Context, id string, p *messaging.RatePolicy) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	i, ok := r.s.instances[id]
	if !ok {
		return errs.ErrNotFound
	}
	i.RatePolicy = p
	return nil
}

func (r instanceRepo) CountByState(_ context.Context) (map[instance.ObservedState]int, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	out := map[instance.ObservedState]int{}
	for _, i := range r.s.instances {
		if i.DeletedAt == nil {
			out[i.ObservedState]++
		}
	}
	return out, nil
}

// ---- operations ----

type opRepo struct{ s *Store }

func (r opRepo) Create(_ context.Context, op instance.Operation) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if _, ok := r.s.operations[op.ID]; ok {
		return errs.ErrAlreadyExists
	}
	if op.Type == instance.OpMigrate {
		for _, o := range r.s.operations {
			if o.InstanceID == op.InstanceID && o.Type == instance.OpMigrate && o.Status.IsActive() {
				return errs.ErrInProgress
			}
		}
	}
	now := r.s.Now()
	if op.CreatedAt.IsZero() {
		op.CreatedAt = now
	}
	op.UpdatedAt, op.StepStartedAt = now, now
	r.s.operations[op.ID] = &op
	return nil
}

func (r opRepo) Get(_ context.Context, id string) (*instance.Operation, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	o, ok := r.s.operations[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	c := *o
	return &c, nil
}

func (r opRepo) Advance(_ context.Context, id, from, to string, st instance.OperationStatus, p ports.OperationPatch) (*instance.Operation, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	o, ok := r.s.operations[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	if o.Step != from {
		return nil, fmt.Errorf("%w: operation step is %q, expected %q", errs.ErrConflict, o.Step, from)
	}
	if o.Type == instance.OpMigrate && from != to &&
		!ownership.CanMigrate(ownership.MigrationStep(from), ownership.MigrationStep(to)) {
		return nil, fmt.Errorf("%w: migration %s -> %s", errs.ErrInvalidTransition, from, to)
	}
	if from != to {
		o.StepStartedAt = r.s.Now()
	}
	o.Step, o.Status, o.UpdatedAt = to, st, r.s.Now()
	if p.ErrorCode != "" || st != instance.OpBlocked {
		o.ErrorCode, o.ErrorMessage = p.ErrorCode, p.ErrorMessage
	}
	if p.BumpAttempts {
		o.Attempts++
	}
	if p.TargetNodeID != "" {
		o.TargetNodeID = p.TargetNodeID
	}
	c := *o
	return &c, nil
}

func (r opRepo) Complete(_ context.Context, id string, st instance.OperationStatus, code, msg string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	o, ok := r.s.operations[id]
	if !ok {
		return errs.ErrNotFound
	}
	t := at
	o.Status, o.ErrorCode, o.ErrorMessage, o.CompletedAt, o.UpdatedAt = st, code, msg, &t, at
	return nil
}

func (r opRepo) FindActive(_ context.Context, instanceID string, t instance.OperationType) (*instance.Operation, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, o := range r.s.operations {
		if o.InstanceID == instanceID && o.Type == t && o.Status.IsActive() {
			c := *o
			return &c, nil
		}
	}
	return nil, errs.ErrNotFound
}

func (r opRepo) ListActive(_ context.Context, t instance.OperationType, limit int) ([]instance.Operation, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []instance.Operation
	for _, o := range r.s.operations {
		if o.Type == t && o.Status.IsActive() {
			out = append(out, *o)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.Before(out[b].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ---- messages ----

type msgRepo struct{ s *Store }

func (r msgRepo) Create(_ context.Context, m messaging.Message) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	_, err := r.s.createMessage(m, nil)
	return err
}

func (r msgRepo) CreateWithOutbox(_ context.Context, m messaging.Message, build func(seq int64) ([]byte, error)) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.createMessage(m, build)
}

// createMessage allocates the sequence and (optionally) the outbox row. Caller holds the lock.
func (s *Store) createMessage(m messaging.Message, build func(seq int64) ([]byte, error)) (int64, error) {
	if _, ok := s.messages[m.ID]; ok {
		return 0, errs.ErrAlreadyExists
	}
	seq := s.nextSeq[m.InstanceID] + 1
	var cmd []byte
	if build != nil {
		var err error
		if cmd, err = build(seq); err != nil {
			return 0, err
		}
	}
	if err := s.fault("create_message"); err != nil {
		return 0, err
	}
	s.nextSeq[m.InstanceID] = seq
	now := s.Now()
	m.CreatedAt, m.UpdatedAt, m.SequenceNo = now, now, seq
	if m.Status == "" {
		m.Status = messaging.StatusQueued
	}
	s.messages[m.ID] = &m
	if build != nil {
		s.outbox = append(s.outbox, &messaging.OutboxEntry{InstanceID: m.InstanceID, MessageID: m.ID, Sequence: seq, Command: cmd, CreatedAt: now})
	}
	return seq, nil
}

func (r msgRepo) Get(_ context.Context, id string) (*messaging.Message, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	m, ok := r.s.messages[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	c := *m
	return &c, nil
}

func (r msgRepo) Transition(_ context.Context, id string, from []messaging.Status, to messaging.Status, p ports.MessagePatch) (*messaging.Message, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	m, ok := r.s.messages[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	if err := r.s.fault("message_transition:" + string(to)); err != nil {
		return nil, err
	}
	allowed := false
	for _, f := range from {
		if m.Status == f {
			allowed = true
		}
	}
	if !allowed {
		return nil, fmt.Errorf("%w: message %s is %s", errs.ErrConflict, id, m.Status)
	}
	if !messaging.CanTransition(m.Status, to) {
		return nil, fmt.Errorf("%w: message %s -> %s", errs.ErrInvalidTransition, m.Status, to)
	}
	m.Status, m.UpdatedAt = to, r.s.Now()
	if p.ProviderMessageID != "" {
		m.ProviderMessageID = p.ProviderMessageID
	}
	if p.ErrorCode != "" || to == messaging.StatusFailed || to == messaging.StatusUnknown {
		m.ErrorCode, m.ErrorMessage = p.ErrorCode, p.ErrorMessage
	}
	if p.BumpAttempt {
		m.AttemptCount++
	}
	c := *m
	return &c, nil
}

var statusRank = map[messaging.Status]int{
	messaging.StatusUnknown: 0, messaging.StatusAccepted: 1, messaging.StatusDelivered: 2, messaging.StatusRead: 3,
}

func (r msgRepo) ApplyProviderStatus(_ context.Context, instanceID, pmid string, to messaging.Status) (bool, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, m := range r.s.messages {
		if m.InstanceID != instanceID || m.ProviderMessageID != pmid {
			continue
		}
		if to == messaging.StatusFailed {
			// the provider reports the send failed: only a not-yet-delivered message can be failed
			if m.Status != messaging.StatusAccepted && m.Status != messaging.StatusUnknown {
				return false, nil
			}
			m.Status, m.ErrorCode, m.ErrorMessage, m.UpdatedAt = to, "PROVIDER_FAILED", "provider reported the message as failed", r.s.Now()
			return true, nil
		}
		cur, okc := statusRank[m.Status]
		nxt, okn := statusRank[to]
		if !okc || !okn || nxt <= cur {
			return false, nil
		}
		m.Status, m.UpdatedAt = to, r.s.Now()
		return true, nil
	}
	return false, errs.ErrNotFound
}

func (r msgRepo) ListOutbox(_ context.Context, instanceID string, limit int) ([]messaging.OutboxEntry, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []messaging.OutboxEntry
	for _, e := range r.s.outbox {
		if e.InstanceID == instanceID && e.DispatchedAt.IsZero() {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Sequence < out[b].Sequence })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r msgRepo) ListInstancesWithPendingOutbox(_ context.Context, limit int) ([]string, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, e := range r.s.outbox {
		if e.DispatchedAt.IsZero() && !seen[e.InstanceID] {
			seen[e.InstanceID] = true
			out = append(out, e.InstanceID)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r msgRepo) MarkOutboxDispatched(_ context.Context, instanceID string, seq int64, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, e := range r.s.outbox {
		if e.InstanceID == instanceID && e.Sequence == seq {
			e.DispatchedAt = at
			return nil
		}
	}
	return errs.ErrNotFound
}

func (r msgRepo) ListStuckOutbox(_ context.Context, before time.Time, limit int) ([]messaging.OutboxEntry, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []messaging.OutboxEntry
	for _, e := range r.s.outbox {
		if e.DispatchedAt.IsZero() || !e.DispatchedAt.Before(before) {
			continue
		}
		m := r.s.messages[e.MessageID]
		if m == nil {
			continue
		}
		if m.Status == messaging.StatusQueued || (m.Status == messaging.StatusDispatching && m.UpdatedAt.Before(before)) {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].InstanceID != out[b].InstanceID {
			return out[a].InstanceID < out[b].InstanceID
		}
		return out[a].Sequence < out[b].Sequence
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r msgRepo) PurgeOutbox(_ context.Context, before time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	kept := r.s.outbox[:0]
	for _, e := range r.s.outbox {
		recoverable := false
		if m := r.s.messages[e.MessageID]; m != nil {
			recoverable = m.Status == messaging.StatusQueued || m.Status == messaging.StatusDispatching
		}
		if !e.DispatchedAt.IsZero() && e.DispatchedAt.Before(before) && !recoverable {
			n++
			continue
		}
		kept = append(kept, e)
	}
	r.s.outbox = kept
	return n, nil
}

func (r msgRepo) ResetOutbox(_ context.Context, instanceID string, seq int64) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, e := range r.s.outbox {
		if e.InstanceID == instanceID && e.Sequence == seq {
			e.DispatchedAt = time.Time{}
			return nil
		}
	}
	return errs.ErrNotFound
}

func (r msgRepo) FirstUnresolvedBefore(_ context.Context, instanceID string, seq int64, unknownTimeout time.Duration) (*messaging.Message, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	cutoff := r.s.Now().Add(-unknownTimeout)
	var first *messaging.Message
	for _, m := range r.s.messages {
		if m.InstanceID != instanceID || m.SequenceNo >= seq || m.SequenceNo == 0 {
			continue
		}
		blocking := m.Status == messaging.StatusQueued || m.Status == messaging.StatusDispatching ||
			(m.Status == messaging.StatusUnknown && (unknownTimeout <= 0 || m.UpdatedAt.After(cutoff)))
		if blocking && (first == nil || m.SequenceNo < first.SequenceNo) {
			first = m
		}
	}
	if first == nil {
		return nil, errs.ErrNotFound
	}
	c := *first
	return &c, nil
}

// ---- blob metadata ----

type blobRepo struct{ s *Store }

func (r blobRepo) Create(_ context.Context, b media.Blob) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if _, ok := r.s.blobs[b.ID]; ok {
		return errs.ErrAlreadyExists
	}
	for _, x := range r.s.blobs {
		if x.ObjectKey == b.ObjectKey {
			return errs.ErrAlreadyExists
		}
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = r.s.Now()
	}
	if b.Status == "" {
		b.Status = media.BlobPending
	}
	r.s.blobs[b.ID] = &b
	return nil
}

func (r blobRepo) Get(_ context.Context, id string) (*media.Blob, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	b, ok := r.s.blobs[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	c := *b
	return &c, nil
}

func (r blobRepo) GetByKey(_ context.Context, key string) (*media.Blob, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	for _, b := range r.s.blobs {
		if b.ObjectKey == key {
			c := *b
			return &c, nil
		}
	}
	return nil, errs.ErrNotFound
}

func (r blobRepo) MarkReady(_ context.Context, id string, size int64, sha string, expiresAt time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	b, ok := r.s.blobs[id]
	if !ok {
		return errs.ErrNotFound
	}
	if b.Status == media.BlobDeleted {
		return errs.ErrConflict
	}
	b.Status, b.Size, b.SHA256, b.ExpiresAt = media.BlobReady, size, sha, expiresAt
	return nil
}

func (r blobRepo) MarkDeleted(_ context.Context, id string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	b, ok := r.s.blobs[id]
	if !ok {
		return errs.ErrNotFound
	}
	t := at
	b.Status, b.DeletedAt = media.BlobDeleted, &t
	return nil
}

func (r blobRepo) ListExpired(_ context.Context, now time.Time, limit int) ([]media.Blob, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []media.Blob
	for _, b := range r.s.blobs {
		if b.Status != media.BlobDeleted && b.ExpiresAt.Before(now) {
			out = append(out, *b)
		}
	}
	sort.Slice(out, func(a, c int) bool { return out[a].ExpiresAt.Before(out[c].ExpiresAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ---- idempotency ----

type idemRepo struct{ s *Store }

func idemKey(tenant, key string) string { return tenant + "\x00" + key }

func (r idemRepo) Begin(_ context.Context, rec ports.IdempotencyRecord) (ports.IdempotencyRecord, bool, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	k := idemKey(rec.TenantID, rec.Key)
	now := r.s.Now()
	if cur, ok := r.s.idem[k]; ok && cur.ExpiresAt.After(now) {
		return cur, false, nil
	}
	rec.Status, rec.CreatedAt, rec.UpdatedAt = ports.IdemInProgress, now, now
	r.s.idem[k] = rec
	return rec, true, nil
}

func (r idemRepo) Complete(_ context.Context, tenant, key string, result json.RawMessage) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	k := idemKey(tenant, key)
	rec, ok := r.s.idem[k]
	if !ok {
		return errs.ErrNotFound
	}
	rec.Status, rec.Result, rec.UpdatedAt = ports.IdemCompleted, result, r.s.Now()
	r.s.idem[k] = rec
	return nil
}

func (r idemRepo) Abandon(_ context.Context, tenant, key string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	k := idemKey(tenant, key)
	if rec, ok := r.s.idem[k]; ok && rec.Status == ports.IdemInProgress {
		delete(r.s.idem, k)
	}
	return nil
}

func (r idemRepo) DeleteExpired(_ context.Context, now time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for k, rec := range r.s.idem {
		if !rec.ExpiresAt.After(now) {
			delete(r.s.idem, k)
			n++
		}
	}
	return n, nil
}

// ---- dedup ----

type dedupRepo struct{ s *Store }

func (r dedupRepo) Begin(_ context.Context, key, instanceID string, ttl, inflight time.Duration) (ports.DedupOutcome, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	now := r.s.Now()
	if row, ok := r.s.dedup[key]; ok && row.expiresAt.After(now) {
		if row.committed || now.Sub(row.claimedAt) < inflight {
			return ports.DedupDuplicate, nil
		}
	}
	r.s.dedup[key] = dedupRow{instanceID: instanceID, claimedAt: now, expiresAt: now.Add(ttl)}
	return ports.DedupProceed, nil
}

func (r dedupRepo) Commit(_ context.Context, key string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if row, ok := r.s.dedup[key]; ok {
		row.committed = true
		r.s.dedup[key] = row
	}
	return nil
}

func (r dedupRepo) Abort(_ context.Context, key string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if row, ok := r.s.dedup[key]; ok && !row.committed {
		delete(r.s.dedup, key)
	}
	return nil
}

func (r dedupRepo) DeleteExpired(_ context.Context, now time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for k, row := range r.s.dedup {
		if !row.expiresAt.After(now) {
			delete(r.s.dedup, k)
			n++
		}
	}
	return n, nil
}
