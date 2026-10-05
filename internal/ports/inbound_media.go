package ports

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/media"
)

// InboundMediaRepository is the work queue of inbound attachments. Every method is safe to call from several workers.
type InboundMediaRepository interface {
	// Enqueue records the job at StageDownload. It is idempotent per EventID: a provider retry of the same webhook
	// creates nothing and reports created=false.
	Enqueue(ctx context.Context, j media.InboundJob) (created bool, err error)
	// ClaimDue leases up to limit jobs of stage DOWNLOAD or PUBLISH whose next_attempt_at has passed, oldest first.
	ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]media.InboundJob, error)
	// Resolve records the outcome: the final event and StagePublish. Attempts and lease are reset.
	Resolve(ctx context.Context, id string, ev events.Event) error
	// Retry counts a failed attempt and schedules the next one (the lease is released).
	Retry(ctx context.Context, id string, next time.Time, lastErr string) error
	// Complete closes the job AND queues its resolved event in the event outbox, atomically: once the job is DONE the event exists in the
	// database, so a broker that loses it cannot lose the message.
	Complete(ctx context.Context, id string, ev events.Event, at time.Time) error
	// Purge deletes jobs finished before `before`.
	Purge(ctx context.Context, before time.Time) (int64, error)
	// EraseContact deletes the jobs of the tenant whose message came from the contact.
	EraseContact(ctx context.Context, tenantID, number string) (int64, error)
	// Drop deletes one job (its contact was erased while it was in flight).
	Drop(ctx context.Context, id string) error
	// Counts feeds the gauges.
	Counts(ctx context.Context, now time.Time) (InboundMediaCounts, error)
}

// InboundMediaCounts is a snapshot for metrics/alerts.
type InboundMediaCounts struct {
	Download, Publish int64
	OldestPending     time.Duration
}
