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

// Handle is the ports.EventHandler of the projector consumer group.
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
