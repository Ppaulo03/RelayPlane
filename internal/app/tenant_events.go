package app

import (
	"context"
	"fmt"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

// EventOutboxService publishes the events that were written to the database in the same transaction as the state
// change that produced them (outbound message statuses). It is the second half of a transactional outbox: after a
// crash an event may be published twice, never zero times; event ids are deterministic so consumers dedupe.
type EventOutboxService struct{ d Deps }

// PublishPending publishes up to limit events in order. A publish failure stops at that event so order is kept.
func (s *EventOutboxService) PublishPending(ctx context.Context, limit int) (int, error) {
	evs, err := s.d.Repos.Events.ListUnpublished(ctx, limit)
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	done := make([]string, 0, len(evs))
	var perr error
	for _, ev := range evs {
		if perr = s.d.Bus.Publish(ctx, ev); perr != nil {
			break
		}
		done = append(done, ev.EventID)
	}
	if len(done) > 0 {
		if err := s.d.Repos.Events.MarkPublished(ctx, done, s.d.now()); err != nil {
			return len(done), err
		}
		s.d.Metrics.EventOutboxPublished.Add(float64(len(done)))
	}
	return len(done), perr
}

// Purge deletes published events older than olderThan.
func (s *EventOutboxService) Purge(ctx context.Context, olderThan time.Duration) (int64, error) {
	return s.d.Repos.Events.Purge(ctx, s.d.now().Add(-olderThan))
}

// SubscriptionConfig configures webhook subscriptions.
type SubscriptionConfig struct {
	// ServerKey derives the per-subscription signing secrets. Required.
	ServerKey []byte
	// MaxPerTenant bounds the subscriptions of one tenant (default 10).
	MaxPerTenant int
	// AllowInsecureURLs accepts http:// destinations (development only).
	AllowInsecureURLs bool
	// AllowPrivateDestinations accepts loopback/private destinations (development only).
	AllowPrivateDestinations bool
}

// SubscriptionService manages the tenant's webhook subscriptions.
type SubscriptionService struct{ d Deps }

// CreateSubscriptionInput is the API payload.
type CreateSubscriptionInput struct {
	URL         string
	EventTypes  []string
	InstanceIDs []string
}

// SubscriptionView is a subscription plus, only right after creation or rotation, its signing secret.
type SubscriptionView struct {
	subscription.Subscription
	Secret string
}

func (s *SubscriptionService) cfg() SubscriptionConfig {
	c := s.d.Cfg.Subscriptions
	if c.MaxPerTenant <= 0 {
		c.MaxPerTenant = 10
	}
	return c
}

// Create registers a destination. The signing secret is derived (never stored) and shown exactly once.
func (s *SubscriptionService) Create(ctx context.Context, tenantID string, in CreateSubscriptionInput) (*SubscriptionView, error) {
	c := s.cfg()
	if len(c.ServerKey) == 0 {
		return nil, fmt.Errorf("%w: webhook subscriptions are not configured", errs.ErrCapabilityMissing)
	}
	if err := subscription.ValidateURL(in.URL, c.AllowInsecureURLs, c.AllowPrivateDestinations); err != nil {
		return nil, err
	}
	var types []events.Type
	seen := map[events.Type]bool{}
	for _, raw := range in.EventTypes {
		t := events.Type(raw)
		if !subscription.TenantFacing(t) {
			return nil, fmt.Errorf("%w: event type %q cannot be subscribed to (available: %v)", errs.ErrInvalidArgument, raw, subscription.TenantFacingTypes())
		}
		if !seen[t] {
			seen[t] = true
			types = append(types, t)
		}
	}
	for _, id := range in.InstanceIDs {
		if _, err := s.d.loadForTenant(ctx, tenantID, id); err != nil {
			return nil, fmt.Errorf("%w: instance %q does not exist", errs.ErrInvalidArgument, id)
		}
	}
	if n, err := s.d.Repos.Subscriptions.CountByTenant(ctx, tenantID); err != nil {
		return nil, err
	} else if n >= c.MaxPerTenant {
		return nil, fmt.Errorf("%w: the limit of %d subscriptions per tenant was reached", errs.ErrConflict, c.MaxPerTenant)
	}
	sub := subscription.Subscription{ID: ids.New("sub"), TenantID: tenantID, URL: in.URL, EventTypes: types, InstanceIDs: in.InstanceIDs,
		SecretVersion: 1, Active: true, CreatedAt: s.d.now().UTC()}
	if err := s.d.Repos.Subscriptions.Create(ctx, sub); err != nil {
		return nil, err
	}
	return &SubscriptionView{Subscription: sub, Secret: subscription.DeriveSecret(c.ServerKey, sub.ID, sub.SecretVersion)}, nil
}

// List returns the tenant's subscriptions (without secrets).
func (s *SubscriptionService) List(ctx context.Context, tenantID string) ([]subscription.Subscription, error) {
	return s.d.Repos.Subscriptions.ListByTenant(ctx, tenantID)
}

// Get returns one subscription of the tenant.
func (s *SubscriptionService) Get(ctx context.Context, tenantID, id string) (*subscription.Subscription, error) {
	return s.d.Repos.Subscriptions.Get(ctx, tenantID, id)
}

// Delete removes a subscription and its pending deliveries.
func (s *SubscriptionService) Delete(ctx context.Context, tenantID, id string) error {
	return s.d.Repos.Subscriptions.Delete(ctx, tenantID, id)
}

// RotateSecret issues a new signing secret. For subscription.RotationGrace the previous secret keeps signing too
// (the request carries both signatures), so the consumer can switch without a gap.
func (s *SubscriptionService) RotateSecret(ctx context.Context, tenantID, id string) (*SubscriptionView, error) {
	c := s.cfg()
	if len(c.ServerKey) == 0 {
		return nil, fmt.Errorf("%w: webhook subscriptions are not configured", errs.ErrCapabilityMissing)
	}
	v, err := s.d.Repos.Subscriptions.RotateSecret(ctx, tenantID, id, s.d.now().UTC())
	if err != nil {
		return nil, err
	}
	sub, err := s.d.Repos.Subscriptions.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	return &SubscriptionView{Subscription: *sub, Secret: subscription.DeriveSecret(c.ServerKey, id, v)}, nil
}

// Deliveries lists the deliveries of a subscription (status "" = all; DEAD is the DLQ).
func (s *SubscriptionService) Deliveries(ctx context.Context, tenantID, id string, status subscription.DeliveryStatus, limit int) ([]subscription.Delivery, error) {
	switch status {
	case "", subscription.DeliveryPending, subscription.DeliveryDelivered, subscription.DeliveryDead:
	default:
		return nil, fmt.Errorf("%w: unknown delivery status %q", errs.ErrInvalidArgument, status)
	}
	return s.d.Repos.Deliveries.List(ctx, tenantID, id, status, limit)
}

// Redeliver puts a DEAD delivery back in the queue with a fresh retry budget.
func (s *SubscriptionService) Redeliver(ctx context.Context, tenantID, deliveryID string) error {
	return s.d.Repos.Deliveries.Requeue(ctx, tenantID, deliveryID, s.d.now().UTC())
}

// RecordDeliveryGauges refreshes the webhook gauges (called by the reconciler's maintenance pass).
func RecordDeliveryGauges(ctx context.Context, d Deps) {
	c, err := d.Repos.Deliveries.Counts(ctx, d.now())
	if err != nil {
		return
	}
	d.Metrics.WebhookPending.Set(float64(c.Pending))
	d.Metrics.WebhookDead.Set(float64(c.Dead))
	d.Metrics.WebhookOldestPending.Set(c.OldestPending.Seconds())
}
