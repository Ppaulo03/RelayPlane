package ports

import (
	"context"
	"encoding/json"
	"time"
)

// IdempotencyStatus is the state of an idempotency record.
type IdempotencyStatus string

const (
	IdemInProgress IdempotencyStatus = "IN_PROGRESS"
	IdemCompleted  IdempotencyStatus = "COMPLETED"
)

// IdempotencyRecord is one row of idempotency_keys. Keys are scoped per tenant.
type IdempotencyRecord struct {
	TenantID    string
	Key         string
	RequestHash string
	Operation   string
	ResourceID  string
	Status      IdempotencyStatus
	Result      json.RawMessage
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ExpiresAt   time.Time
}

// IdempotencyStore persists idempotency records.
type IdempotencyStore interface {
	// Begin atomically inserts rec (status IN_PROGRESS) or returns the record
	// already stored under (tenant, key). claimed reports whether rec won.
	Begin(ctx context.Context, rec IdempotencyRecord) (existing IdempotencyRecord, claimed bool, err error)
	// Complete stores the final result.
	Complete(ctx context.Context, tenantID, key string, result json.RawMessage) error
	// Abandon removes an in-progress record after a failure that must not be
	// replayed (e.g. validation errors), letting the client retry freshly.
	Abandon(ctx context.Context, tenantID, key string) error
	// DeleteExpired purges expired records.
	DeleteExpired(ctx context.Context, now time.Time) (int64, error)
}
