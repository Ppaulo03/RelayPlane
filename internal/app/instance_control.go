package app

import (
	"context"
	"fmt"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/ports"
)

// Every operation that mutates an instance's lifecycle (provision, delete,
// logout, reconnect, migrate, reconciler corrective actions) runs inside the
// same per-instance critical section. PostgreSQL stays the final authority
// (epoch/step compare-and-set), but without serialisation "check, then act"
// sequences interleave: a reconciler could reconnect a node that a migration
// just fenced, or a delete could start while a migration is being requested.
//
// Code that holds the lock calls the *Locked variants of the services; it must
// re-read the instance after acquiring the lock, never trust a copy loaded
// before.

const (
	controlLockTTL = 60 * time.Second
	// lockWait is how long API-triggered operations wait for a busy instance
	// before answering 409 (the competing operation is short-lived).
	lockWait = 5 * time.Second
)

func controlKey(instanceID string) string { return "instance-control:" + instanceID }

// WithInstanceControl runs fn while holding the instance-control lock. The lock
// is renewed while fn runs; if the lease is lost the context passed to fn is
// cancelled so no further side effects start under a lock we no longer own.
// With wait == 0 a busy instance fails immediately with ErrInProgress.
func (d Deps) WithInstanceControl(ctx context.Context, instanceID string, wait time.Duration, fn func(ctx context.Context) error) error {
	deadline := time.Now().Add(wait)
	var lease ports.Lease
	for {
		l, ok, err := d.Locker.TryLock(ctx, controlKey(instanceID), controlLockTTL)
		if err != nil {
			return err
		}
		if ok {
			lease = l
			break
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: another lifecycle operation is running on instance %s", errs.ErrInProgress, instanceID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}

	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(controlLockTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if err := lease.Extend(lctx, controlLockTTL); err != nil && lctx.Err() == nil {
					d.Log.ErrorContext(ctx, "instance-control lease lost; aborting the operation", "instance_id", instanceID, "error", err)
					cancel()
					return
				}
			}
		}
	}()
	err := fn(lctx)
	close(stop)
	_ = lease.Release(context.WithoutCancel(ctx))
	return err
}
