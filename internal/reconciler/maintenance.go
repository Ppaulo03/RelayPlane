package reconciler

import (
	"context"
	"time"
)

// Maintenance performs housekeeping that keeps the platform self-healing:
// outbox recovery, blob lifecycle, expiry of idempotency/dedupe records and
// gauge refresh.
func (r *Reconciler) Maintenance(ctx context.Context) {
	d := r.deps()
	if n, err := r.App.Outbox.Redispatch(ctx, r.Cfg.StuckQueuedAfter, r.Cfg.BatchSize); err != nil {
		r.Log.WarnContext(ctx, "outbox redispatch failed", "error", err)
	} else if n > 0 {
		r.Log.WarnContext(ctx, "re-published commands the broker had lost", "count", n)
	}
	if _, err := r.App.Outbox.Purge(ctx, 24*time.Hour); err != nil {
		r.Log.WarnContext(ctx, "outbox purge failed", "error", err)
	}
	if exp, orph, err := r.App.Media.Cleanup(ctx, r.Cfg.OrphanGrace, r.Cfg.BatchSize); err != nil {
		r.Log.WarnContext(ctx, "blob cleanup failed", "error", err)
	} else if exp+orph > 0 {
		r.Log.InfoContext(ctx, "blob cleanup", "expired", exp, "orphans", orph)
	}
	now := time.Now()
	if _, err := d.Repos.Idempotency.DeleteExpired(ctx, now); err != nil {
		r.Log.WarnContext(ctx, "idempotency expiry failed", "error", err)
	}
	if _, err := d.Repos.Dedup.DeleteExpired(ctx, now); err != nil {
		r.Log.WarnContext(ctx, "dedupe expiry failed", "error", err)
	}
	r.RefreshGauges(ctx)
}

// RefreshGauges updates instance and queue gauges.
func (r *Reconciler) RefreshGauges(ctx context.Context) {
	d := r.deps()
	counts, err := d.Repos.Instances.CountByState(ctx)
	if err != nil {
		return
	}
	d.Metrics.InstancesTotal.Reset()
	for st, n := range counts {
		d.Metrics.InstancesTotal.WithLabelValues(string(st)).Set(float64(n))
	}
	d.Metrics.InstancesConnected.Set(float64(counts["CONNECTED"]))
	if depth, err := d.Queue.Depth(ctx); err == nil {
		d.Metrics.OutboundQueueDepth.Set(float64(depth))
	}
}
