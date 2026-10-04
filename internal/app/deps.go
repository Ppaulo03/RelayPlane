// Package app holds the application services (use cases). They orchestrate the
// pure core through ports and never touch infrastructure directly.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/idempotency"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// Config holds application-level tunables.
type Config struct {
	DefaultProvider string            // e.g. "evolution-v2"
	ProviderAliases map[string]string // public name -> provider key, e.g. "evolution" -> "evolution-v2"

	MediaPolicy media.Policy
	MediaTTL    time.Duration // retention of READY blobs
	// InboundMediaMaxBytes caps what is downloaded from the provider for an inbound attachment (default 25 MiB; a negative
	// value turns inbound media off: attachments are reported as REJECTED/unsupported). InboundMediaTTL is how long the
	// stored bytes stay downloadable (default 7 days).
	InboundMediaMaxBytes int64
	InboundMediaTTL      time.Duration
	PendingTTL           time.Duration // how long a declared-but-not-uploaded blob lives

	DedupTTL time.Duration

	MigrationVerifyTimeout time.Duration
	MaxTextLength          int

	Subscriptions SubscriptionConfig

	// DefaultRate is the default outbound pacing (informational: reported by GET /limits; the worker enforces it).
	DefaultRate messaging.RatePolicy
}

// EffectiveInboundMaxBytes is the inbound attachment cap with the default applied (0 when inbound media is turned off).
func (c Config) EffectiveInboundMaxBytes() int64 {
	switch {
	case c.InboundMediaMaxBytes < 0:
		return 0
	case c.InboundMediaMaxBytes == 0:
		return DefaultConfig().InboundMediaMaxBytes
	}
	return c.InboundMediaMaxBytes
}

// EffectiveInboundTTL is the retention of stored inbound attachments with the default applied.
func (c Config) EffectiveInboundTTL() time.Duration {
	if c.InboundMediaTTL <= 0 {
		return DefaultConfig().InboundMediaTTL
	}
	return c.InboundMediaTTL
}

// DefaultConfig returns production-leaning defaults.
func DefaultConfig() Config {
	return Config{
		DefaultProvider:        "evolution-v2",
		ProviderAliases:        map[string]string{"evolution": "evolution-v2"},
		MediaPolicy:            media.DefaultPolicy(),
		MediaTTL:               24 * time.Hour,
		InboundMediaMaxBytes:   25 << 20,
		InboundMediaTTL:        7 * 24 * time.Hour,
		PendingTTL:             30 * time.Minute,
		DedupTTL:               24 * time.Hour,
		MigrationVerifyTimeout: 10 * time.Minute,
		MaxTextLength:          4096,
	}
}

// ProviderRegistry maps provider keys ("evolution-v2") to implementations.
type ProviderRegistry struct {
	providers map[string]ports.MessagingProvider
	webhooks  map[string]ports.WebhookAdapter
}

// NewProviderRegistry returns an empty registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{providers: map[string]ports.MessagingProvider{}, webhooks: map[string]ports.WebhookAdapter{}}
}

// Register adds a provider (and optionally its webhook adapter).
func (r *ProviderRegistry) Register(key string, p ports.MessagingProvider, w ports.WebhookAdapter) {
	r.providers[key] = p
	if w != nil {
		r.webhooks[key] = w
	}
}

// Get returns the provider registered under key.
func (r *ProviderRegistry) Get(key string) (ports.MessagingProvider, error) {
	p, ok := r.providers[key]
	if !ok {
		return nil, fmt.Errorf("%w: unknown provider %q", errs.ErrInvalidArgument, key)
	}
	return p, nil
}

// Webhook returns the webhook adapter of a provider.
func (r *ProviderRegistry) Webhook(key string) (ports.WebhookAdapter, bool) {
	w, ok := r.webhooks[key]
	return w, ok
}

// Keys lists registered provider keys.
func (r *ProviderRegistry) Keys() []string {
	var out []string
	for k := range r.providers {
		out = append(out, k)
	}
	return out
}

// Deps are the collaborators shared by all services.
type Deps struct {
	Repos     ports.Repositories
	Providers *ProviderRegistry
	Queue     ports.CommandQueue
	Bus       ports.EventBus
	Blob      ports.BlobStore
	Locker    ports.Locker
	Idem      *idempotency.Service
	Metrics   *observability.Metrics
	Log       *slog.Logger
	Now       func() time.Time
	Cfg       Config
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// resolveProvider maps a public provider name (possibly empty) to a key.
func (d Deps) resolveProvider(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		name = d.Cfg.DefaultProvider
	}
	if alias, ok := d.Cfg.ProviderAliases[name]; ok {
		name = alias
	}
	if _, err := d.Providers.Get(name); err != nil {
		return "", err
	}
	return name, nil
}

// instanceCtx decorates ctx with the instance's identifiers for logging.
func instanceCtx(ctx context.Context, i instance.Instance) context.Context {
	return observability.With(ctx,
		observability.KeyInstanceID, i.ID, observability.KeyTenantID, i.TenantID,
		observability.KeyNodeID, i.NodeID, observability.KeyProvider, i.Provider,
		observability.KeyEpoch, i.AssignmentEpoch)
}

// observe records an observed state under the instance's current epoch.
func (d Deps) observe(ctx context.Context, i instance.Instance, st instance.ObservedState) (bool, error) {
	changed, err := d.Repos.Instances.SetObserved(ctx, i.ID, i.AssignmentEpoch, st, d.now())
	if err != nil && isStale(err) {
		d.Metrics.EpochMismatchTotal.Inc()
	}
	return changed, err
}

func isStale(err error) bool { return errors.Is(err, errs.ErrStaleAssignment) }

// loadForTenant loads an instance and hides other tenants' (and deleted) resources.
func (d Deps) loadForTenant(ctx context.Context, tenantID, id string) (*instance.Instance, error) {
	i, err := d.Repos.Instances.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if i.TenantID != tenantID || i.DeletedAt != nil {
		return nil, errs.ErrNotFound // never reveal that another tenant's resource exists
	}
	return i, nil
}

func assignmentOf(i *instance.Instance) ownership.Assignment { return i.Assignment() }

// App bundles every service.
type App struct {
	Deps       Deps
	Instances  *InstanceService
	Migrations *MigrationService
	Messages   *MessageService
	Outbox     *OutboxService
	Media      *MediaService
	Inbound    *InboundService
	Nodes      *NodeService
	Tenants    *TenantService
	APIKeys    *APIKeyService
	Contacts   *ContactService
	Channel    *ChannelService
	Retention  *RetentionService
	// EventOutbox moves tenant-facing events from the database outbox to the event bus.
	EventOutbox   *EventOutboxService
	Subscriptions *SubscriptionService
}

// New wires the services.
func New(d Deps) *App {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Metrics == nil {
		d.Metrics = observability.NewMetrics()
	}
	a := &App{Deps: d}
	a.Instances = &InstanceService{d: d}
	a.Migrations = &MigrationService{d: d, inst: a.Instances}
	a.Outbox = &OutboxService{d: d}
	a.Messages = &MessageService{d: d, outbox: a.Outbox}
	a.Media = &MediaService{d: d}
	a.Inbound = &InboundService{d: d}
	a.Nodes = &NodeService{d: d}
	a.Tenants = &TenantService{d: d}
	a.APIKeys = &APIKeyService{d: d}
	a.Contacts = &ContactService{d: d}
	a.Channel = &ChannelService{d: d}
	a.Retention = &RetentionService{d: d}
	a.EventOutbox = &EventOutboxService{d: d}
	a.Subscriptions = &SubscriptionService{d: d}
	return a
}
