package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// InboundService handles provider webhooks. The request path does the minimum:
// authenticate, validate ownership, normalize, dedupe, publish. Business
// reactions happen in EventBus consumers.
type InboundService struct{ d Deps }

// InboundResult summarises one webhook request.
type InboundResult struct {
	Published  int `json:"published"`
	Duplicates int `json:"duplicates"`
	Ignored    int `json:"ignored"`
}

// Handle processes one raw webhook for the given provider key.
func (s *InboundService) Handle(ctx context.Context, providerKey string, req ports.InboundRequest) (InboundResult, error) {
	ctx, span := observability.Start(ctx, "webhook.handle")
	defer span.End()
	var res InboundResult

	adapter, ok := s.d.Providers.Webhook(providerKey)
	if !ok {
		return res, fmt.Errorf("%w: no webhook adapter for %q", errs.ErrNotFound, providerKey)
	}
	// 1. provider authentication (also yields the *claimed* origin)
	claim, err := adapter.Authenticate(req)
	if err != nil {
		return res, fmt.Errorf("%w: %v", errs.ErrUnauthenticated, err)
	}
	// 2. normalization into canonical events (anti-corruption layer)
	norm, err := adapter.Normalize(req)
	if err != nil {
		return res, fmt.Errorf("%w: %v", errs.ErrInvalidArgument, err)
	}

	for _, in := range norm {
		inst, err := s.d.Repos.Instances.Get(ctx, in.InstanceID)
		if errors.Is(err, errs.ErrNotFound) || (err == nil && inst.DeletedAt != nil) {
			res.Ignored++ // webhook for an unknown/deleted instance: nothing to do, do not make the provider retry
			continue
		}
		if err != nil {
			return res, err
		}
		ictx := instanceCtx(ctx, *inst)

		// 3. ownership validation against the catalog (never trust the claim)
		verr := ownership.ValidateClaim(claim.NodeID, claim.Epoch, inst.Assignment())
		if verr == nil && adapter.Provider() != inst.Provider {
			verr = fmt.Errorf("%w: webhook adapter %q != instance provider %q", errs.ErrOwnershipViolation, adapter.Provider(), inst.Provider)
		}
		if verr != nil {
			s.violation(ictx, inst, claim, verr)
			return res, verr
		}

		// 4. deduplicate (two-phase: begin -> publish -> commit)
		key := events.DedupeKey(in.InstanceID, in.Type, in.ProviderMessageID, in.State)
		out, err := s.d.Repos.Dedup.Begin(ictx, key, inst.ID, s.d.Cfg.DedupTTL, s.d.Cfg.DedupInflight)
		if err != nil {
			return res, err
		}
		if out == ports.DedupDuplicate {
			s.d.Metrics.InboundDuplicates.Inc()
			res.Duplicates++
			continue
		}

		// 5. publish the canonical event
		ts := in.Timestamp
		if ts.IsZero() {
			ts = s.d.now()
		}
		ev := events.Event{EventID: events.EventIDFor(key), EventType: in.Type, Provider: inst.Provider,
			TenantID: inst.TenantID, InstanceID: inst.ID, Timestamp: ts.UTC(), Payload: in.Payload,
			// the claim was validated against the catalog just above: remember WHICH owner/epoch spoke
			SourceAssignment: &events.SourceAssignment{NodeID: inst.NodeID, Epoch: inst.AssignmentEpoch}}
		// an attachment is resolved (downloaded and stored, or rejected) BEFORE the event is delivered: a message with
		// media is queued and the ingestor publishes it, so the tenant never sees a half-resolved media
		if job := s.admitMedia(inst, &ev, in); job != nil {
			if _, err := s.d.Repos.InboundMedia.Enqueue(ictx, *job); err != nil {
				_ = s.d.Repos.Dedup.Abort(ictx, key)
				return res, fmt.Errorf("queue inbound media: %w", err)
			}
		} else if err := s.d.Bus.Publish(ictx, ev); err != nil {
			_ = s.d.Repos.Dedup.Abort(ictx, key) // let the provider's retry publish it
			return res, fmt.Errorf("publish event: %w", err)
		}
		if err := s.d.Repos.Dedup.Commit(ictx, key); err != nil {
			s.d.Log.WarnContext(ictx, "dedupe commit failed (event may be re-published with the same id)", "error", err)
		}
		s.d.Metrics.InboundEventsTotal.WithLabelValues(string(in.Type)).Inc()
		res.Published++
	}
	return res, nil
}

func (s *InboundService) violation(ctx context.Context, inst *instance.Instance, claim ports.NodeClaim, cause error) {
	s.d.Metrics.OwnershipViolation.Inc()
	s.d.Log.ErrorContext(ctx, "OWNERSHIP_VIOLATION", "claimed_node", claim.NodeID, "claimed_epoch", claim.Epoch,
		"owner_node", inst.NodeID, "assignment_epoch", inst.AssignmentEpoch, "error", cause)
	ev := events.Event{EventID: ids.New("evt"), EventType: events.OwnershipViolation, Provider: inst.Provider,
		TenantID: inst.TenantID, InstanceID: inst.ID, Timestamp: s.d.now().UTC(),
		Payload: map[string]any{"claimed_node": claim.NodeID, "claimed_epoch": claim.Epoch,
			"owner_node": inst.NodeID, "assignment_epoch": inst.AssignmentEpoch}}
	if err := s.d.Bus.Publish(ctx, ev); err != nil {
		s.d.Log.WarnContext(ctx, "could not publish ownership violation event", "error", err)
	}
}

// ---- nodes ----

// NodeService implements administrative node operations.
type NodeService struct{ d Deps }

// List returns all provider nodes.
func (s *NodeService) List(ctx context.Context) ([]routing.Node, error) {
	return s.d.Repos.Nodes.List(ctx)
}

// Drain stops new assignments to the node. Existing sessions stay put.
func (s *NodeService) Drain(ctx context.Context, id string) (*routing.Node, error) {
	n, err := s.d.Repos.Nodes.SetStatus(ctx, id, routing.NodeDraining)
	if err == nil {
		s.d.Log.InfoContext(ctx, "node draining", observability.KeyNodeID, id)
	}
	return n, err
}

// Resume makes a DRAINING node eligible again.
func (s *NodeService) Resume(ctx context.Context, id string) (*routing.Node, error) {
	cur, err := s.d.Repos.Nodes.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.Status != routing.NodeDraining {
		return nil, fmt.Errorf("%w: node %s is %s, not DRAINING", errs.ErrConflict, id, cur.Status)
	}
	n, err := s.d.Repos.Nodes.SetStatus(ctx, id, routing.NodeReady)
	if err == nil {
		s.d.Log.InfoContext(ctx, "node resumed", observability.KeyNodeID, id)
	}
	return n, err
}

// Register upserts a node into the catalog.
func (s *NodeService) Register(ctx context.Context, n routing.Node) error {
	return s.d.Repos.Nodes.Upsert(ctx, n)
}

// ---- tenants ----

// TenantService manages tenants and API-key authentication.
type TenantService struct {
	d       Deps
	touched sync.Map // key id -> time of the last recorded use
}

// HashAPIKey is the stored form of an API key.
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Create makes a tenant and returns its API key (shown exactly once).
func (s *TenantService) Create(ctx context.Context, name string) (*instance.Tenant, string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 100 {
		return nil, "", fmt.Errorf("%w: name must be 1-100 characters", errs.ErrInvalidArgument)
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, "", err
	}
	key := newAPIKey(buf)
	t := instance.Tenant{ID: ids.New("tenant"), Name: name, APIKeyHash: HashAPIKey(key), APIKeyPrefix: keyPrefix(key), CreatedAt: time.Now()}
	if err := s.d.Repos.Tenants.Create(ctx, t); err != nil {
		return nil, "", err
	}
	return &t, key, nil
}

// Get returns a tenant (administration).
func (s *TenantService) Get(ctx context.Context, id string) (*instance.Tenant, error) {
	return s.d.Repos.Tenants.Get(ctx, id)
}

// Authenticate resolves the tenant owning an API key.
func (s *TenantService) Authenticate(ctx context.Context, apiKey string) (*instance.Tenant, error) {
	t, _, err := s.AuthenticateKey(ctx, apiKey)
	return t, err
}

// AuthenticateKey resolves the tenant and the key itself: a revoked or expired key is as unknown as a wrong one. It
// records the use (last_used_at), at most once every half minute per key per process.
func (s *TenantService) AuthenticateKey(ctx context.Context, apiKey string) (*instance.Tenant, *instance.APIKey, error) {
	if apiKey == "" {
		return nil, nil, errs.ErrUnauthenticated
	}
	now := s.d.now()
	k, err := s.d.Repos.APIKeys.FindActiveByHash(ctx, HashAPIKey(apiKey), now)
	if errors.Is(err, errs.ErrNotFound) {
		return nil, nil, errs.ErrUnauthenticated
	}
	if err != nil {
		return nil, nil, err
	}
	t, err := s.d.Repos.Tenants.Get(ctx, k.TenantID)
	if err != nil {
		return nil, nil, err
	}
	if last, ok := s.touched.Load(k.ID); !ok || now.Sub(last.(time.Time)) > 30*time.Second {
		s.touched.Store(k.ID, now)
		if err := s.d.Repos.APIKeys.Touch(ctx, k.ID, now, time.Minute); err != nil {
			s.d.Log.WarnContext(ctx, "could not record api key use", "key_id", k.ID, "error", err)
		}
	}
	return t, k, nil
}

func newAPIKey(entropy []byte) string { return "rpk_" + base64.RawURLEncoding.EncodeToString(entropy) }

// keyPrefix is the part of a key shown in listings: enough to recognise it, nowhere near enough to use it.
func keyPrefix(key string) string {
	if len(key) > 8 {
		return key[:8]
	}
	return key
}

// APIKeyService manages the keys of a tenant: several at once, so a credential can be rotated without downtime.
type APIKeyService struct{ d Deps }

// Create issues a new key. ttl <= 0 means it never expires. The secret is returned here and never again.
func (s *APIKeyService) Create(ctx context.Context, tenantID, name string, ttl time.Duration) (*instance.APIKey, string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 64 {
		return nil, "", fmt.Errorf("%w: name must be 1-64 characters", errs.ErrInvalidArgument)
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, "", err
	}
	secret := newAPIKey(buf)
	now := s.d.now().UTC()
	k := instance.APIKey{ID: ids.New("key"), TenantID: tenantID, Name: name, Prefix: keyPrefix(secret), KeyHash: HashAPIKey(secret), CreatedAt: now}
	if ttl > 0 {
		k.ExpiresAt = now.Add(ttl)
	}
	if err := s.d.Repos.APIKeys.Create(ctx, k); err != nil {
		if errors.Is(err, errs.ErrConflict) {
			return nil, "", fmt.Errorf("%w: a tenant can hold at most %d active API keys: revoke one first", errs.ErrConflict, instance.MaxActiveAPIKeys)
		}
		return nil, "", err
	}
	s.d.Log.InfoContext(ctx, "api key created", "tenant_id", tenantID, "key_id", k.ID, "name", name)
	return &k, secret, nil
}

// List returns the tenant's keys (never the secrets).
func (s *APIKeyService) List(ctx context.Context, tenantID string) ([]instance.APIKey, error) {
	return s.d.Repos.APIKeys.List(ctx, tenantID)
}

// Revoke stops a key from authenticating, at once. The last usable key of a tenant cannot be revoked (the admin can
// always issue a new one, but a tenant must not lock itself out by mistake).
func (s *APIKeyService) Revoke(ctx context.Context, tenantID, id string) error {
	if err := s.d.Repos.APIKeys.Revoke(ctx, tenantID, id, s.d.now().UTC()); err != nil {
		if errors.Is(err, errs.ErrConflict) {
			return fmt.Errorf("%w: this is the last usable API key: create another one before revoking it", errs.ErrConflict)
		}
		return err
	}
	s.d.Log.InfoContext(ctx, "api key revoked", "tenant_id", tenantID, "key_id", id)
	return nil
}
