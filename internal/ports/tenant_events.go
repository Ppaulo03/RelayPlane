package ports

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

// EventOutboxRepository is the transactional outbox of events derived from database state (today: outbound
// message status changes). Rows are written by the repository in the SAME transaction as the state change, so a
// status change can never be lost. The outbox IS the event stream: the tenant fan-out and the catalog projector read it from here.
type EventOutboxRepository interface {
	// ListAll returns the events in the outbox, oldest first, whatever their state (inspection and tests: the consumers claim their work
	// with ClaimForFanOut and ClaimForProjection).
	ListAll(ctx context.Context, limit int) ([]events.Event, error)
	// ClaimForFanOut leases up to limit events whose deliveries for the tenant do not exist yet, oldest first. The lease keeps other
	// workers off them for `lease`; a worker that dies leaves them to be claimed again.
	ClaimForFanOut(ctx context.Context, limit int, lease time.Duration) ([]events.Event, error)
	// ClaimForProjection leases up to limit events the catalog has not applied yet (delivery receipts, session state changes), oldest first.
	ClaimForProjection(ctx context.Context, limit int, lease time.Duration) ([]events.Event, error)
	// MarkProjected records that the catalog applied the given events.
	MarkProjected(ctx context.Context, eventIDs []string, at time.Time) error
	// ProjectionStats counts the events the catalog has not applied yet and the age of the oldest (by the store's clock).
	ProjectionStats(ctx context.Context) (count int64, oldest time.Duration, err error)
	// MarkFannedOut records that the deliveries of the given events exist. An event leaves the outbox's purge window only after this AND
	// MarkProjected.
	MarkFannedOut(ctx context.Context, eventIDs []string, at time.Time) error
	// Purge deletes events that were fanned out AND projected before `before`.
	Purge(ctx context.Context, before time.Time) (int64, error)
	// EraseContact deletes the events of the tenant that are about the contact (payload.from == number), whatever their state:
	// an accepted inbound event waits here until its deliveries exist, and keeps the text and the number while it does.
	EraseContact(ctx context.Context, tenantID, number string) (int64, error)
	// PendingStats counts the accepted events whose deliveries for the tenant do not exist yet and the age of the oldest one (by the
	// store's clock). A growing age means nobody is fanning the outbox out (the worker is down): accepted events are safe but late.
	PendingStats(ctx context.Context) (count int64, oldest time.Duration, err error)
}

// SubscriptionRepository persists tenant webhook subscriptions. Every method that takes a tenant id is scoped by
// it: a subscription of another tenant is indistinguishable from a missing one (ErrNotFound).
type SubscriptionRepository interface {
	Create(ctx context.Context, s subscription.Subscription) error
	// CreateIfBelow creates the subscription only if the tenant has fewer than max of them, as ONE atomic step (errs.ErrConflict otherwise):
	// two requests at the limit cannot both get in. max <= 0 means no limit.
	CreateIfBelow(ctx context.Context, s subscription.Subscription, max int) error
	Get(ctx context.Context, tenantID, id string) (*subscription.Subscription, error)
	// GetByID is for the delivery pipeline, which already holds a trusted subscription id.
	GetByID(ctx context.Context, id string) (*subscription.Subscription, error)
	ListByTenant(ctx context.Context, tenantID string) ([]subscription.Subscription, error)
	// ListActive returns the active subscriptions of a tenant (fan-out).
	ListActive(ctx context.Context, tenantID string) ([]subscription.Subscription, error)
	// SetPaused pauses or resumes the subscription: while paused its deliveries accumulate and none is sent.
	SetPaused(ctx context.Context, tenantID, id string, paused bool) error
	// RotateSecret increments the secret version and returns the new one.
	RotateSecret(ctx context.Context, tenantID, id string, at time.Time) (version int, err error)
	// Delete removes the subscription and its deliveries.
	Delete(ctx context.Context, tenantID, id string) error
}

// DeliveryRepository persists webhook deliveries. Enqueue is idempotent per (subscription, event): re-consuming an
// event from the bus never produces a second delivery.
type DeliveryRepository interface {
	// Enqueue inserts deliveries, ignoring those that already exist; it returns how many were new.
	Enqueue(ctx context.Context, ds []subscription.Delivery) (int, error)
	// ClaimDue leases up to `limit` PENDING deliveries whose next_attempt_at has passed, oldest first, excluding any
	// delivery whose (subscription, instance) already has another delivery in flight (best-effort ordering).
	ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]subscription.Delivery, error)
	// ClaimDueWith is ClaimDue with a ceiling of perSubscription deliveries in flight for any one subscription (0: no
	// ceiling), so one slow consumer cannot occupy the whole dispatcher. Deliveries of paused subscriptions are never claimed.
	ClaimDueWith(ctx context.Context, now time.Time, lease time.Duration, limit, perSubscription int) ([]subscription.Delivery, error)
	// Backlog reports, per subscription of the tenant, the deliveries still waiting and the age of the oldest.
	Backlog(ctx context.Context, tenantID string, now time.Time) (map[string]Backlog, error)
	MarkDelivered(ctx context.Context, id string, at time.Time) error
	// MarkRetry records a failed attempt (attempts+1) and schedules the next one.
	MarkRetry(ctx context.Context, id string, next time.Time, lastErr string) error
	// MarkDead records a failed attempt and moves the delivery to the DLQ.
	MarkDead(ctx context.Context, id string, lastErr string) error
	// Postpone releases the lease and delays the delivery WITHOUT counting an attempt (circuit open).
	Postpone(ctx context.Context, id string, until time.Time) error
	// List returns deliveries of one subscription of a tenant, newest first ("" status: all).
	List(ctx context.Context, tenantID, subscriptionID string, status subscription.DeliveryStatus, limit int) ([]subscription.Delivery, error)
	// Requeue moves a DEAD delivery of the tenant back to PENDING with a fresh budget.
	Requeue(ctx context.Context, tenantID, id string, now time.Time) error
	PurgeDelivered(ctx context.Context, before time.Time) (int64, error)
	// PurgePending deletes deliveries still PENDING that were created before `before` (a subscription left paused, a consumer that never
	// comes back): they hold the user's text too. The subscription sees gaps in `sequence`. A delivery a dispatcher holds (leased) is left
	// alone (`now` is the application clock, the one the leases were written with): the next pass gets it.
	PurgePending(ctx context.Context, before, now time.Time) (int64, error)
	// PurgeDead deletes dead-lettered deliveries created before `before`: the DLQ keeps the user's text, so it needs a retention.
	PurgeDead(ctx context.Context, before time.Time) (int64, error)
	// EraseContact deletes every delivery of the tenant whose event is about the contact (payload.from == number), whatever
	// its status. A subscription that had not received those events yet will see gaps in its sequence.
	EraseContact(ctx context.Context, tenantID, number string) (int64, error)
	// DeleteByEvent deletes the deliveries created for one event (an erasure that arrived while it was being fanned out).
	DeleteByEvent(ctx context.Context, eventID string) (int64, error)
	// Counts feeds the gauges: deliveries per status, and the age of the oldest PENDING one.
	Counts(ctx context.Context, now time.Time) (DeliveryCounts, error)
}

// Backlog is what one subscription has waiting to be delivered.
type Backlog struct {
	Pending       int64
	OldestPending time.Duration
}

// DeliveryCounts is a snapshot for metrics/alerts.
type DeliveryCounts struct {
	Pending, Delivered, Dead int64
	OldestPending            time.Duration
}

// WebhookRequest is one signed POST.
type WebhookRequest struct {
	URL     string
	Headers map[string]string
	Body    []byte
	Timeout time.Duration
}

// WebhookSender performs the POST. Implementations MUST refuse non-public destinations at dial time
// (errs.ErrDestinationBlocked), never follow redirects and bound the response read.
type WebhookSender interface {
	// Send returns the HTTP status code. A transport error means "no response".
	Send(ctx context.Context, req WebhookRequest) (status int, err error)
}
