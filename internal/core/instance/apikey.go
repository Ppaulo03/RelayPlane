package instance

import "time"

// APIKey is one credential of a tenant. A tenant has several at the same time so a deployment can rotate without
// downtime: create the new key, roll it out, revoke the old one. Only the SHA-256 of the key is stored.
type APIKey struct {
	ID         string
	TenantID   string
	Name       string // what the key is for ("agent-prod", "ci")
	Prefix     string // the first characters of the key, to recognise it in a listing (never enough to use it)
	KeyHash    string
	CreatedAt  time.Time
	ExpiresAt  time.Time // zero: never
	LastUsedAt time.Time // zero: never used (updated at most once a minute)
	RevokedAt  time.Time // zero: active
}

// Active reports whether the key authenticates at the given time.
func (k APIKey) Active(now time.Time) bool {
	return k.RevokedAt.IsZero() && (k.ExpiresAt.IsZero() || k.ExpiresAt.After(now))
}

// MaxActiveAPIKeys bounds the keys a tenant can hold at once (a rotation needs two; this leaves room for CI and tools).
const MaxActiveAPIKeys = 10

// InitialKey is the key a tenant is born with, derived from Tenant.APIKeyHash.
func InitialKey(t Tenant) APIKey {
	id := "key_" + t.APIKeyHash
	if len(id) > 28 {
		id = id[:28]
	}
	return APIKey{ID: id, TenantID: t.ID, Name: "initial", Prefix: t.APIKeyPrefix, KeyHash: t.APIKeyHash, CreatedAt: t.CreatedAt}
}
