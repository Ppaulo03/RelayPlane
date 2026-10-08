package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// Projector applies canonical events to the catalog: delivery receipts update
// outbound messages and instance.status_changed updates observed_state within
// milliseconds, without waiting for the reconciler (which stays as the safety
// net for lost events).
type Projector struct {
	Repos ports.Repositories
	Log   *slog.Logger
	Now   func() time.Time
	// Metrics is optional (epoch mismatches are counted when set).
	Metrics *observability.Metrics
	// ReceiptGrace is how long an unknown provider message id is retried: the
	// receipt may overtake the worker recording ACCEPTED.
	ReceiptGrace time.Duration

	Batch int           // events claimed per pass (default 100)
	Lease time.Duration // how long a claimed event is exclusive; also how often one that failed is retried (default 2s)
	Poll  time.Duration // pause when idle (default 100ms)
}

func (p *Projector) defaults() {
	if p.Batch <= 0 {
		p.Batch = 100
	}
	if p.Lease <= 0 {
		p.Lease = 2 * time.Second
	}
	if p.Poll <= 0 {
		p.Poll = 100 * time.Millisecond
	}
}

// Run applies the events of the event outbox to the catalog until ctx is cancelled. It reads the DATABASE, not the broker: a receipt that the
// broker lost would otherwise leave a message ACCEPTED for good, and the tenant would never be told it was delivered or read.
func (p *Projector) Run(ctx context.Context) {
	p.defaults()
	for ctx.Err() == nil {
		n, err := p.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			p.Log.WarnContext(ctx, "projection pass failed", "error", err)
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(p.Poll):
			}
		}
	}
}

// RunOnce claims and applies one batch. Events are independent (every update is guarded against late and duplicate ones), so one that fails,
// for instance a receipt that overtook the recording of ACCEPTED, does not hold the others back: it is claimed again when its lease expires.
func (p *Projector) RunOnce(ctx context.Context) (int, error) {
	p.defaults()
	evs, err := p.Repos.Events.ClaimForProjection(ctx, p.Batch, p.Lease)
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	done := make([]string, 0, len(evs))
	for _, ev := range evs {
		if herr := p.Handle(ctx, ev); herr != nil {
			if !errors.Is(herr, ctx.Err()) {
				p.Log.DebugContext(ctx, "event not applied yet, will be retried", "event_id", ev.EventID, "error", herr)
			}
			continue
		}
		done = append(done, ev.EventID)
	}
	if err := p.Repos.Events.MarkProjected(context.WithoutCancel(ctx), done, p.Now().UTC()); err != nil {
		return len(done), err
	}
	return len(done), nil
}

// NewProjector returns a projector with defaults.
func NewProjector(repos ports.Repositories, log *slog.Logger) *Projector {
	return &Projector{Repos: repos, Log: log, Now: time.Now, ReceiptGrace: time.Minute}
}

func decode[T any](payload any) (T, error) {
	var out T
	if v, ok := payload.(T); ok {
		return v, nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	return out, err
}

// Handle applies one event to the catalog. It is idempotent and safe against late and duplicate events.
func (p *Projector) Handle(ctx context.Context, ev events.Event) error {
	ctx = observability.With(ctx, observability.KeyTenantID, ev.TenantID, observability.KeyInstanceID, ev.InstanceID, observability.KeyProvider, ev.Provider)
	ctx, span := observability.Start(ctx, "projector.handle")
	defer span.End()
	switch ev.EventType {
	case events.MessageStatus:
		return p.messageStatus(ctx, ev)
	case events.InstanceStatusChanged:
		return p.instanceStatus(ctx, ev)
	}
	return nil
}

func (p *Projector) messageStatus(ctx context.Context, ev events.Event) error {
	pl, err := decode[events.MessageStatusPayload](ev.Payload)
	if err != nil {
		p.Log.ErrorContext(ctx, "undecodable message.status payload; dropping", "event_id", ev.EventID, "error", err)
		return nil
	}
	var to messaging.Status
	switch pl.Status {
	case "delivered":
		to = messaging.StatusDelivered
	case "read":
		to = messaging.StatusRead
	case "failed":
		to = messaging.StatusFailed // ACCEPTED/UNKNOWN -> FAILED when the provider reports the send failed
	default:
		return nil // "sent" is already ACCEPTED
	}
	applied, err := p.Repos.Messages.ApplyProviderStatus(ctx, ev.InstanceID, pl.ProviderMessageID, to)
	if errors.Is(err, errs.ErrNotFound) {
		if p.Now().Sub(ev.Timestamp) < p.ReceiptGrace {
			return fmt.Errorf("receipt for unknown provider message %q: will retry", pl.ProviderMessageID)
		}
		return nil // not ours (e.g. sent from the phone): ignore
	}
	if err != nil {
		return err
	}
	if applied {
		p.Log.InfoContext(ctx, "delivery receipt applied", "status", to)
	}
	return nil
}

func (p *Projector) instanceStatus(ctx context.Context, ev events.Event) error {
	pl, err := decode[events.InstanceStatusChangedPayload](ev.Payload)
	if err != nil {
		p.Log.ErrorContext(ctx, "undecodable instance.status_changed payload; dropping", "event_id", ev.EventID, "error", err)
		return nil
	}
	st := instance.ObservedState(pl.State)
	if !st.Valid() {
		return nil
	}
	inst, err := p.Repos.Instances.Get(ctx, ev.InstanceID)
	if errors.Is(err, errs.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if inst.DeletedAt != nil || ev.Timestamp.Before(inst.LastStatusChange) {
		return nil // late event: the catalog already knows something newer
	}
	if inst.ObservedState == instance.Migrating || inst.ObservedState == instance.Deleting {
		return nil // lifecycle owned by a workflow
	}
	// Apply under the epoch that PRODUCED the event, not the one the catalog has now:
	// a late event of the previous owner must not touch the new assignment.
	epoch := inst.AssignmentEpoch
	if ev.SourceAssignment != nil {
		epoch = ev.SourceAssignment.Epoch
	}
	_, err = p.Repos.Instances.SetObserved(ctx, inst.ID, epoch, st, ev.Timestamp)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errs.ErrStaleAssignment):
		if p.Metrics != nil {
			p.Metrics.EpochMismatchTotal.Inc()
		}
		p.Log.WarnContext(ctx, "STALE_ASSIGNMENT: dropping a status event of a previous owner", "event_epoch", epoch,
			"current_epoch", inst.AssignmentEpoch, "state", st)
		return nil
	case errors.Is(err, errs.ErrInvalidTransition):
		p.Log.DebugContext(ctx, "status event not applicable", "state", st, "error", err)
		return nil
	}
	return err
}
