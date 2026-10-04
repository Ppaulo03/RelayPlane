package ports

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/instance"
)

// APIKeyRepository persists the API keys of tenants (hashes only).
type APIKeyRepository interface {
	// Create stores a key, atomically refusing it when the tenant already holds MaxActiveAPIKeys active keys
	// (errs.ErrConflict) or the hash exists (errs.ErrAlreadyExists).
	Create(ctx context.Context, k instance.APIKey) error
	// FindActiveByHash returns the key when it exists, is not revoked and has not expired (errs.ErrNotFound otherwise).
	FindActiveByHash(ctx context.Context, hash string, now time.Time) (*instance.APIKey, error)
	// List returns the tenant's keys, newest first, revoked ones included.
	List(ctx context.Context, tenantID string) ([]instance.APIKey, error)
	// Revoke revokes a key of the tenant. It is idempotent, and it refuses (errs.ErrConflict) to revoke the last active
	// key, so a tenant can never lock itself out. A key of another tenant is errs.ErrNotFound.
	Revoke(ctx context.Context, tenantID, id string, at time.Time) error
	// Touch records a use, but only when the last recorded one is older than minAge (one write a minute, not one per request).
	Touch(ctx context.Context, id string, at time.Time, minAge time.Duration) error
}
