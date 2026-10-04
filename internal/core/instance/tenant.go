package instance

import (
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

// Tenant is a platform customer. The API key hash identifies the tenant of a
// request: tenant ids are derived from authentication, never from payloads.
type Tenant struct {
	ID         string
	Name       string
	APIKeyHash string
	// APIKeyPrefix is the visible start of the first key (see APIKey.Prefix).
	APIKeyPrefix string
	RatePolicy   *messaging.RatePolicy
	CreatedAt    time.Time
}
