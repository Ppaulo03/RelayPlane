package ports

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
)

// TenantRepository persists tenants.
type TenantRepository interface {
	Create(ctx context.Context, t instance.Tenant) error
	Get(ctx context.Context, id string) (*instance.Tenant, error)
	GetByAPIKeyHash(ctx context.Context, hash string) (*instance.Tenant, error)
	// SetRatePolicy stores the tenant-level rate policy override (nil clears it).
	SetRatePolicy(ctx context.Context, id string, p *messaging.RatePolicy) error
}

// PlacementRequest creates an instance together with its first assignment,
// atomically reserving capacity on the node chosen by Choose.
type PlacementRequest struct {
	Instance instance.Instance // ObservedState ALLOCATING, epoch assigned by the repository (1)
	Provider string            // provider key candidate nodes must serve
	// Choose picks a node among the *locked* candidate rows. It is the core's
	// pure placement function; the repository guarantees atomic reservation.
	Choose func(candidates []routing.Node) (nodeID string, err error)
}

// ReassignRequest moves ownership to a new node with the next epoch.
type ReassignRequest struct {
	InstanceID    string
	ExpectedEpoch int64
	NewNodeID     string
	OperationID   string // migration operation proving the old owner was fenced
	Reason        ownership.ReleaseReason
}

// InstanceRepository persists instances and their ownership.
//
// Invariants enforced here (and, for PostgreSQL, by constraints):
//   - an instance has at most one open (unreleased) assignment (INV-01);
//   - epochs strictly increase;
//   - Reassign only succeeds when the migration operation is at step
//     OLD_OWNER_FENCED (INV-09).
type InstanceRepository interface {
	// CreateWithPlacement is idempotent on Instance.ID: when the instance
	// already exists it is returned unchanged.
	CreateWithPlacement(ctx context.Context, req PlacementRequest) (*instance.Instance, error)
	Get(ctx context.Context, id string) (*instance.Instance, error)
	List(ctx context.Context, tenantID string) ([]instance.Instance, error)
	// ListDue returns live instances not reconciled since `before`.
	ListDue(ctx context.Context, before time.Time, limit int) ([]instance.Instance, error)

	UpdateDesired(ctx context.Context, id string, state instance.DesiredState) error
	// SetObserved records an observed state. It validates the lifecycle
	// transition and rejects a stale epoch (ErrStaleAssignment).
	SetObserved(ctx context.Context, id string, epoch int64, state instance.ObservedState, at time.Time) (changed bool, err error)
	// SetObservedEmitting is SetObserved plus the tenant-facing event of the change, queued in the event outbox in the SAME transaction (only
	// when the state really changed): the catalog cannot change without the tenant eventually being told.
	SetObservedEmitting(ctx context.Context, id string, epoch int64, state instance.ObservedState, at time.Time, ev events.Event) (changed bool, err error)
	SetProviderInstance(ctx context.Context, id string, epoch int64, providerInstanceID string) error
	// TouchHeartbeat records a provider heartbeat for the given assignment epoch;
	// a heartbeat from an older assignment is rejected (ErrStaleAssignment).
	TouchHeartbeat(ctx context.Context, id string, epoch int64, at time.Time) error
	MarkReconciled(ctx context.Context, id string, at time.Time) error

	Reassign(ctx context.Context, req ReassignRequest) (ownership.Assignment, error)
	// Release closes the open assignment without a successor and frees node capacity.
	Release(ctx context.Context, id string, epoch int64, reason ownership.ReleaseReason) error
	// MarkDeleted releases ownership, sets observed DELETED and deleted_at.
	MarkDeleted(ctx context.Context, id string, epoch int64, at time.Time) error
	Assignments(ctx context.Context, id string) ([]ownership.AssignmentRecord, error)
	// SetRatePolicy stores the instance-level rate policy override (nil clears it).
	SetRatePolicy(ctx context.Context, id string, p *messaging.RatePolicy) error
	// CountByState counts non-deleted instances per observed state (gauges).
	CountByState(ctx context.Context) (map[instance.ObservedState]int, error)
}

// NodeRepository persists provider nodes.
type NodeRepository interface {
	// Upsert registers a node or refreshes its static attributes; it never
	// resets active_instances or an operator-set status.
	Upsert(ctx context.Context, n routing.Node) error
	Get(ctx context.Context, id string) (*routing.Node, error)
	List(ctx context.Context) ([]routing.Node, error)
	// SetStatus applies an administrative/health transition (validated).
	SetStatus(ctx context.Context, id string, to routing.NodeStatus) (*routing.Node, error)
	// RecordProbe stores a probe outcome; heartbeat is only advanced on success.
	RecordProbe(ctx context.Context, id string, status routing.NodeStatus, version string, ok bool, at time.Time) error
}

// OperationPatch carries optional fields applied together with a step change.
type OperationPatch struct {
	ErrorCode    string
	ErrorMessage string
	BumpAttempts bool
	TargetNodeID string
}

// OperationRepository persists long-running operations.
type OperationRepository interface {
	// Create inserts the operation. At most one active MIGRATE operation may
	// exist per instance (ErrInProgress otherwise); an existing id yields ErrAlreadyExists.
	Create(ctx context.Context, op instance.Operation) error
	Get(ctx context.Context, id string) (*instance.Operation, error)
	// Advance is a compare-and-set on the step: it fails with ErrConflict when
	// the stored step differs from `from`. A finished (SUCCEEDED/FAILED) operation
	// is immutable: Advance and Complete on it fail with ErrAlreadyTerminal (which
	// also matches ErrConflict) and change nothing, so a delayed driver can never
	// turn SUCCEEDED into FAILED.
	Advance(ctx context.Context, id, from, to string, status instance.OperationStatus, patch OperationPatch) (*instance.Operation, error)
	Complete(ctx context.Context, id string, status instance.OperationStatus, errCode, errMsg string, at time.Time) error
	FindActive(ctx context.Context, instanceID string, t instance.OperationType) (*instance.Operation, error)
	ListActive(ctx context.Context, t instance.OperationType, limit int) ([]instance.Operation, error)
}

// MessagePatch is applied together with a status transition.
type MessagePatch struct {
	ProviderMessageID string
	ErrorCode         string
	ErrorMessage      string
	BumpAttempt       bool
}

// MessageRepository persists outbound messages.
// ErasedMessages reports an erasure of the messages to one recipient.
type ErasedMessages struct {
	Anonymized int // messages whose recipient and content were removed
	Cancelled  int // of those, messages that had not been sent and will not be
}

type MessageRepository interface {
	// Create stores a message and allocates its per-instance SequenceNo (gapless,
	// ordered by commit) in the same transaction.
	Create(ctx context.Context, m messaging.Message) error
	// CreateWithOutbox is Create plus the outbox row, atomically. buildCommand
	// receives the allocated sequence and returns the serialized command. An
	// existing message id yields ErrAlreadyExists (nothing written).
	CreateWithOutbox(ctx context.Context, m messaging.Message, buildCommand func(seq int64) ([]byte, error)) (seq int64, err error)
	Get(ctx context.Context, id string) (*messaging.Message, error)
	// Transition is a compare-and-set on the status (ErrConflict if the
	// current status is not in `from`). It returns the updated message.
	Transition(ctx context.Context, id string, from []messaging.Status, to messaging.Status, patch MessagePatch) (*messaging.Message, error)
	// ApplyProviderStatus applies a delivery receipt monotonically
	// (ACCEPTED < DELIVERED < READ; regressions are ignored).
	ApplyProviderStatus(ctx context.Context, instanceID, providerMessageID string, to messaging.Status) (applied bool, err error)

	// EraseRecipient anonymizes every message the tenant sent to the number: recipient, content and error text are removed
	// (the ledger row stays). A message not yet sent (QUEUED) is cancelled (FAILED / ERASED). The copy of a command of a
	// message that is in the provider's hands (DISPATCHING) disappears with its outbox entry once it is resolved.
	EraseRecipient(ctx context.Context, tenantID, number string, at time.Time) (ErasedMessages, error)
	// ScrubTerminalBefore anonymizes (retention) up to limit finished messages created before `before` that still hold
	// a recipient and content. It returns how many it did.
	ScrubTerminalBefore(ctx context.Context, before, at time.Time, limit int) (int64, error)

	// ListByStatus returns the messages of a tenant in a status, oldest first (by instance, then sequence), optionally of one
	// instance. It is how an operator or an agent finds what is waiting for a decision (UNKNOWN) or what failed.
	ListByStatus(ctx context.Context, tenantID string, status messaging.Status, instanceID string, limit int) ([]messaging.Message, error)
	// UnknownStats counts the UNKNOWN messages of the whole platform and the age of the oldest one, by the store's own clock:
	// each is an instance whose later messages may be held back.
	UnknownStats(ctx context.Context) (count int64, oldest time.Duration, err error)

	// ListOutbox returns the undispatched outbox entries of one instance in sequence order.
	ListOutbox(ctx context.Context, instanceID string, limit int) ([]messaging.OutboxEntry, error)
	// ListInstancesWithPendingOutbox returns instances that have undispatched entries.
	ListInstancesWithPendingOutbox(ctx context.Context, limit int) ([]string, error)
	// MarkOutboxDispatched records (or refreshes) the publication of an entry.
	MarkOutboxDispatched(ctx context.Context, instanceID string, seq int64, at time.Time) error
	// ListStuckOutbox returns entries published before `before` whose message is
	// still unprocessed: QUEUED (the broker lost the command) or DISPATCHING for
	// longer than `before` (the worker or the command died mid-flight; the
	// redelivery turns DISPATCHING into UNKNOWN without resending). They are
	// published again; the sequence barrier keeps later messages behind them.
	ListStuckOutbox(ctx context.Context, before time.Time, limit int) ([]messaging.OutboxEntry, error)
	// PurgeOutbox deletes entries dispatched before `before`, but never the entry
	// of a message that may still need recovery (QUEUED or DISPATCHING): the
	// outbox is the only place the command can be rebuilt from.
	PurgeOutbox(ctx context.Context, before time.Time) (int64, error)
	// ResetOutbox marks an entry as not yet dispatched so the dispatcher publishes
	// it again (used when a command reached a worker before its predecessors).
	ResetOutbox(ctx context.Context, instanceID string, seq int64) error
	// FirstUnresolvedBefore returns the lowest-sequence message of the instance with
	// sequence < seq that has not been resolved: QUEUED, DISPATCHING, or UNKNOWN.
	// An UNKNOWN that has been UNKNOWN for longer than unknownTimeout no longer
	// blocks (0: it blocks until resolved). The age is measured by the store's own
	// clock, never compared with the caller's. ErrNotFound when nothing blocks.
	FirstUnresolvedBefore(ctx context.Context, instanceID string, seq int64, unknownTimeout time.Duration) (*messaging.Message, error)
}

// DedupOutcome is the result of accepting an inbound event.
type DedupOutcome int

const (
	DedupProceed   DedupOutcome = iota // first time: the event was durably accepted
	DedupDuplicate                     // already accepted: nothing was written
)

// Deduplicator is the durable front door of inbound events. Accept is ATOMIC: the deduplication key and the work that will
// deliver the event (the event in the transactional outbox, or the attachment job that publishes it once resolved) are written
// in ONE transaction. Either both exist or neither does, so once Accept returns the provider's request may be answered 200:
// the event can no longer be lost between "I remember having seen it" and "it was queued" (a crash there used to make the
// provider's retry look like a duplicate of an event that was never queued).
type Deduplicator interface {
	// Accept records key and, when it is new, the event (job == nil) or the attachment job (job != nil, which carries the event).
	Accept(ctx context.Context, key string, ttl time.Duration, ev events.Event, job *media.InboundJob) (DedupOutcome, error)
	DeleteExpired(ctx context.Context, now time.Time) (int64, error)
}

// BlobMetadataRepository persists blob_metadata.
type BlobMetadataRepository interface {
	Create(ctx context.Context, b media.Blob) error
	Get(ctx context.Context, id string) (*media.Blob, error)
	GetByKey(ctx context.Context, key string) (*media.Blob, error)
	// MarkReady flips PENDING -> READY and extends the retention to expiresAt.
	MarkReady(ctx context.Context, id string, size int64, sha256 string, expiresAt time.Time) error
	MarkDeleted(ctx context.Context, id string, at time.Time) error
	// ListExpired returns non-deleted blobs whose expires_at is before `now`.
	ListExpired(ctx context.Context, now time.Time, limit int) ([]media.Blob, error)
	// ListBySubject returns the live blobs of a tenant that came from the phone number (inbound attachments).
	ListBySubject(ctx context.Context, tenantID, subject string) ([]media.Blob, error)
}
