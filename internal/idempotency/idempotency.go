// Package idempotency implements Idempotency-Key handling on top of
// ports.IdempotencyStore.
//
// Semantics (per tenant and key):
//   - first request claims the key and runs the operation;
//   - a repeat with the same payload returns the stored result (replay);
//   - a repeat with a different payload or operation is an error (INV-03);
//   - a repeat while the first is still running returns ErrInProgress, unless
//     the claim is stale (the first caller crashed), in which case the
//     operation is resumed under the same resource id. Operations therefore
//     MUST be idempotent given their resource id.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/ports"
)

// Service wraps a store with replay/conflict logic.
type Service struct {
	Store      ports.IdempotencyStore
	TTL        time.Duration // how long records live
	StaleAfter time.Duration // an IN_PROGRESS claim older than this may be resumed
	Now        func() time.Time
}

// NewService returns a Service with sane defaults.
func NewService(store ports.IdempotencyStore) *Service {
	return &Service{Store: store, TTL: 24 * time.Hour, StaleAfter: 10 * time.Second, Now: time.Now}
}

// HashRequest returns a stable hash of a request payload.
func HashRequest(v any) string {
	b, _ := json.Marshal(v) // struct field order is deterministic; maps are key-sorted
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// rejected reports errors raised before any side effect: the key can be freed
// so that a corrected request may reuse it.
func rejected(err error) bool {
	for _, e := range []error{errs.ErrInvalidArgument, errs.ErrForbidden, errs.ErrNotFound,
		errs.ErrNoCapacity, errs.ErrUnauthenticated, errs.ErrCapabilityMissing, errs.ErrPayloadTooLarge,
		errs.ErrConflict, errs.ErrInvalidTransition} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

// Do runs fn at most once for (tenant, key). newResourceID mints the id of the
// resource the operation creates; the same id is passed to fn on a resume.
// With an empty key, fn simply runs (no idempotency requested).
func Do[T any](ctx context.Context, s *Service, tenantID, key, operation, requestHash string,
	newResourceID func() string, fn func(ctx context.Context, resourceID string) (T, error)) (out T, replayed bool, err error) {

	if key == "" {
		out, err = fn(ctx, newResourceID())
		return out, false, err
	}
	now := s.Now()
	rec := ports.IdempotencyRecord{
		TenantID: tenantID, Key: key, RequestHash: requestHash, Operation: operation,
		ResourceID: newResourceID(), ExpiresAt: now.Add(s.TTL),
	}
	existing, claimed, err := s.Store.Begin(ctx, rec)
	if err != nil {
		return out, false, err
	}
	resourceID := rec.ResourceID
	if !claimed {
		if existing.RequestHash != requestHash || existing.Operation != operation {
			return out, false, fmt.Errorf("%w: key %q", errs.ErrIdempotencyConflict, key)
		}
		if existing.Status == ports.IdemCompleted {
			if err := json.Unmarshal(existing.Result, &out); err != nil {
				return out, false, fmt.Errorf("idempotency: stored result unreadable: %w", err)
			}
			return out, true, nil
		}
		if now.Sub(existing.UpdatedAt) < s.StaleAfter {
			return out, false, fmt.Errorf("%w: key %q is still being processed", errs.ErrInProgress, key)
		}
		resourceID = existing.ResourceID // resume the interrupted operation
	}

	out, err = fn(ctx, resourceID)
	if err != nil {
		if rejected(err) {
			_ = s.Store.Abandon(ctx, tenantID, key)
		}
		return out, false, err
	}
	raw, merr := json.Marshal(out)
	if merr != nil {
		return out, false, merr
	}
	if cerr := s.Store.Complete(ctx, tenantID, key, raw); cerr != nil {
		return out, false, cerr
	}
	return out, false, nil
}
