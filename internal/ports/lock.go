package ports

import (
	"context"
	"time"
)

// Lease is a held lock. Locks are an optimisation against duplicated work;
// correctness never relies on them alone (state changes are compare-and-set
// in the repositories).
type Lease interface {
	// Extend renews the lease; it fails if the lease was lost.
	Extend(ctx context.Context, ttl time.Duration) error
	Release(ctx context.Context) error
}

// Locker hands out short-lived exclusive leases by name.
type Locker interface {
	// TryLock returns (nil, false, nil) when the lock is held by someone else.
	TryLock(ctx context.Context, name string, ttl time.Duration) (Lease, bool, error)
}
