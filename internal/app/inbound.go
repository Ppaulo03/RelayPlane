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
			TenantID: inst.TenantID, InstanceID: inst.ID, Timestamp: ts.UTC(), Payload: in.Payload}
		if err := s.d.Bus.Publish(ictx, ev); err != nil {
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
type TenantService struct{ d Deps }

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
	key := "rpk_" + base64.RawURLEncoding.EncodeToString(buf)
	t := instance.Tenant{ID: ids.New("tenant"), Name: name, APIKeyHash: HashAPIKey(key), CreatedAt: time.Now()}
	if err := s.d.Repos.Tenants.Create(ctx, t); err != nil {
		return nil, "", err
	}
	return &t, key, nil
}

// Authenticate resolves the tenant owning an API key.
func (s *TenantService) Authenticate(ctx context.Context, apiKey string) (*instance.Tenant, error) {
	if apiKey == "" {
		return nil, errs.ErrUnauthenticated
	}
	t, err := s.d.Repos.Tenants.GetByAPIKeyHash(ctx, HashAPIKey(apiKey))
	if errors.Is(err, errs.ErrNotFound) {
		return nil, errs.ErrUnauthenticated
	}
	return t, err
}
