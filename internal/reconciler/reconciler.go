// Package reconciler implements the control loop that converges observed_state
// towards desired_state, probes provider nodes, resumes interrupted workflows
// and performs housekeeping. Events update state quickly; the reconciler is
// the safety net for lost events and crashed processes.
package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/reconciliation"
	"github.com/relayplane/relayplane/internal/core/routing"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// Config tunes the reconciler.
type Config struct {
	Interval         time.Duration // pause between passes
	InstanceInterval time.Duration // minimum time between reconciliations of one instance
	BatchSize        int
	CallTimeout      time.Duration // per provider call
	NodeOfflineAfter time.Duration
	StuckQueuedAfter time.Duration // outbox recovery threshold
	OrphanGrace      time.Duration
	Policy           reconciliation.Policy
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		Interval: 10 * time.Second, InstanceInterval: 30 * time.Second, BatchSize: 100,
		CallTimeout: 15 * time.Second, NodeOfflineAfter: time.Minute,
		StuckQueuedAfter: 2 * time.Minute, OrphanGrace: time.Hour, Policy: reconciliation.DefaultPolicy(),
	}
}

// Reconciler is the first-class control-loop service.
type Reconciler struct {
	App *app.App
	Cfg Config
	Log *slog.Logger
	Now func() time.Time
}

// New returns a reconciler.
func New(a *app.App, cfg Config, log *slog.Logger) *Reconciler {
	return &Reconciler{App: a, Cfg: cfg, Log: log, Now: time.Now}
}

// Stats summarises one pass.
type Stats struct {
	Examined, Drifted, Acted, Failed int
}

// Run loops until ctx is cancelled. It is safe to run several replicas:
// per-instance work is guarded by leases and every state change is a
// compare-and-set in the catalog.
func (r *Reconciler) Run(ctx context.Context) error {
	t := time.NewTicker(r.Cfg.Interval)
	defer t.Stop()
	for {
		r.Pass(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Pass runs every reconciliation duty once.
func (r *Reconciler) Pass(ctx context.Context) Stats {
	ctx, span := observability.Start(ctx, "reconciler.pass")
	defer span.End()
	r.ProbeNodes(ctx)
	r.DriveMigrations(ctx)
	st := r.ReconcileInstances(ctx)
	r.Maintenance(ctx)
	return st
}

func (r *Reconciler) deps() app.Deps { return r.App.Deps }

// ProbeNodes refreshes node health. Node health never alters instance
// ownership: an unreachable node simply makes its instances unavailable.
func (r *Reconciler) ProbeNodes(ctx context.Context) {
	d := r.deps()
	nodes, err := d.Repos.Nodes.List(ctx)
	if err != nil {
		r.Log.ErrorContext(ctx, "list nodes failed", "error", err)
		return
	}
	byStatus := map[routing.NodeStatus]int{}
	for _, n := range nodes {
		prov, err := d.Providers.Get(n.Provider)
		if err != nil {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, r.Cfg.CallTimeout)
		probe, perr := prov.ProbeNode(cctx, n.ID)
		cancel()
		ok := perr == nil && probe != nil && probe.Ready
		now := r.Now()
		next := routing.NextStatusAfterProbe(n, ok, now, r.Cfg.NodeOfflineAfter)
		version := ""
		if probe != nil {
			version = probe.Version
		}
		if err := d.Repos.Nodes.RecordProbe(ctx, n.ID, next, version, ok, now); err != nil {
			r.Log.ErrorContext(ctx, "record probe failed", observability.KeyNodeID, n.ID, "error", err)
			continue
		}
		if next != n.Status {
			r.Log.WarnContext(ctx, "node status changed", observability.KeyNodeID, n.ID, "from", n.Status, "to", next, "probe_error", perr)
		}
		byStatus[next]++
		health := 0.0
		if next == routing.NodeReady {
			health = 1
		}
		d.Metrics.ProviderNodeHealth.WithLabelValues(n.ID, n.Provider).Set(health)
	}
	for _, s := range []routing.NodeStatus{routing.NodeStarting, routing.NodeReady, routing.NodeDegraded, routing.NodeDraining, routing.NodeOffline} {
		d.Metrics.ProviderNodesTotal.WithLabelValues(string(s)).Set(float64(byStatus[s]))
	}
}

// DriveMigrations resumes in-flight migrations (BLOCKED ones wait for an operator).
func (r *Reconciler) DriveMigrations(ctx context.Context) {
	ops, err := r.deps().Repos.Operations.ListActive(ctx, instance.OpMigrate, r.Cfg.BatchSize)
	if err != nil {
		r.Log.ErrorContext(ctx, "list migrations failed", "error", err)
		return
	}
	for _, op := range ops {
		if op.Status == instance.OpBlocked {
			continue
		}
		if err := r.App.Migrations.Drive(ctx, op.ID); err != nil {
			r.Log.WarnContext(ctx, "migration drive failed", observability.KeyOperation, op.ID, "error", err)
		}
	}
}

// ReconcileInstances compares desired and observed state for due instances.
func (r *Reconciler) ReconcileInstances(ctx context.Context) Stats {
	d := r.deps()
	var st Stats
	due, err := d.Repos.Instances.ListDue(ctx, r.Now().Add(-r.Cfg.InstanceInterval), r.Cfg.BatchSize)
	if err != nil {
		r.Log.ErrorContext(ctx, "list due instances failed", "error", err)
		return st
	}
	for _, inst := range due {
		st.Examined++
		drifted, acted, err := r.ReconcileInstance(ctx, inst.ID)
		if drifted {
			st.Drifted++
		}
		if acted {
			st.Acted++
		}
		if err != nil {
			st.Failed++
		}
	}
	return st
}

// ReconcileInstance reconciles one instance. It returns whether drift was
// found and whether a corrective action was taken.
func (r *Reconciler) ReconcileInstance(ctx context.Context, id string) (drifted, acted bool, err error) {
	d := r.deps()
	lease, ok, lerr := d.Locker.TryLock(ctx, "reconcile:"+id, 2*r.Cfg.CallTimeout)
	if lerr != nil || !ok {
		return false, false, lerr
	}
	defer lease.Release(ctx) //nolint:errcheck

	inst, err := d.Repos.Instances.Get(ctx, id)
	if err != nil {
		return false, false, err
	}
	ctx, span := observability.Start(ctx, "reconciler.instance")
	defer span.End()
	ctx = observability.With(ctx, observability.KeyInstanceID, inst.ID, observability.KeyTenantID, inst.TenantID,
		observability.KeyNodeID, inst.NodeID, observability.KeyProvider, inst.Provider, observability.KeyEpoch, inst.AssignmentEpoch)
	defer func() {
		_ = d.Repos.Instances.MarkReconciled(context.WithoutCancel(ctx), id, r.Now())
		if err != nil {
			d.Metrics.ReconciliationFail.Inc()
			observability.Fail(span, err)
			r.Log.ErrorContext(ctx, "reconciliation failed", "error", err)
		}
	}()

	if inst.NodeID == "" || inst.DeletedAt != nil {
		return false, false, nil // no owner, nothing to observe
	}
	prov, err := d.Providers.Get(inst.Provider)
	if err != nil {
		return false, false, err
	}

	migrationActive := false
	if op, merr := d.Repos.Operations.FindActive(ctx, inst.ID, instance.OpMigrate); merr == nil {
		migrationActive = op.Status != instance.OpBlocked
	}

	cctx, cancel := context.WithTimeout(ctx, r.Cfg.CallTimeout)
	obs := reconciliation.Observation{}
	state, qerr := prov.GetInstanceState(cctx, inst.Assignment())
	cancel()
	switch {
	case qerr != nil:
		obs.Err = qerr
	default:
		obs.State = state.State
		hb := state.Heartbeat
		if hb.IsZero() {
			hb = r.Now()
		}
		_ = d.Repos.Instances.TouchHeartbeat(ctx, inst.ID, inst.AssignmentEpoch, hb) // epoch-guarded: a stale owner cannot touch the new assignment
	}

	dec := reconciliation.Decide(reconciliation.Input{Instance: *inst, Observation: obs,
		MigrationActive: migrationActive, Now: r.Now(), Policy: r.Cfg.Policy})
	if dec.Drift {
		d.Metrics.ReconciliationDrift.Inc()
	}
	d.Metrics.ReconciliationTotal.WithLabelValues(string(dec.Action)).Inc()
	if dec.Action == reconciliation.ActionNone {
		if qerr != nil && !errors.Is(qerr, errs.ErrProviderUnavailable) && !errors.Is(qerr, errs.ErrInstanceNotFound) {
			return dec.Drift, false, qerr
		}
		return dec.Drift, false, nil
	}
	r.Log.InfoContext(ctx, "reconciling", "action", dec.Action, "reason", dec.Reason,
		"desired", inst.DesiredState, "observed", inst.ObservedState)

	switch dec.Action {
	case reconciliation.ActionUpdateObserved:
		err = r.record(ctx, inst, dec.Observed, dec.Reason)
	case reconciliation.ActionConnect:
		cctx, cancel := context.WithTimeout(ctx, r.Cfg.CallTimeout)
		cerr := prov.ConnectInstance(cctx, inst.Assignment())
		cancel()
		if cerr != nil && !errors.Is(cerr, errs.ErrPairingUnavailable) {
			err = cerr
			break
		}
		err = r.refresh(ctx, inst, prov, "reconnect requested")
	case reconciliation.ActionDisconnect:
		cctx, cancel := context.WithTimeout(ctx, r.Cfg.CallTimeout)
		err = prov.Disconnect(cctx, inst.Assignment())
		cancel()
		if err == nil {
			err = r.refresh(ctx, inst, prov, "disconnect requested")
		}
	case reconciliation.ActionDelete, reconciliation.ActionFinalizeDelete:
		cur := *inst
		if cur.ObservedState != instance.Deleting {
			if _, oerr := d.Repos.Instances.SetObserved(ctx, cur.ID, cur.AssignmentEpoch, instance.Deleting, r.Now()); oerr != nil {
				err = oerr
				break
			}
			cur.ObservedState = instance.Deleting
		}
		_, err = r.App.Instances.FinishDelete(ctx, cur)
	case reconciliation.ActionCreate:
		_, err = r.App.Instances.Provision(ctx, *inst)
	}
	return dec.Drift, err == nil, err
}

// refresh re-reads provider state after an action and records it (best effort:
// a failed read is simply observed again on the next pass).
func (r *Reconciler) refresh(ctx context.Context, inst *instance.Instance, prov ports.MessagingProvider, reason string) error {
	cctx, cancel := context.WithTimeout(ctx, r.Cfg.CallTimeout)
	defer cancel()
	st, err := prov.GetInstanceState(cctx, inst.Assignment())
	if err != nil || !st.State.Valid() || st.State == inst.ObservedState {
		return nil
	}
	return r.record(ctx, inst, st.State, reason)
}

// record stores provider truth and announces the change as an event.
func (r *Reconciler) record(ctx context.Context, inst *instance.Instance, to instance.ObservedState, reason string) error {
	d := r.deps()
	if inst.ProviderInstanceID == "" && to.IsLive() {
		if err := d.Repos.Instances.SetProviderInstance(ctx, inst.ID, inst.AssignmentEpoch, inst.ID); err != nil {
			return err
		}
	}
	if inst.ObservedState == instance.Allocating && to != instance.Creating && to != instance.Failed && to != instance.Deleting {
		// the provider already holds the session (create succeeded, the gateway died before recording it)
		if _, err := d.Repos.Instances.SetObserved(ctx, inst.ID, inst.AssignmentEpoch, instance.Creating, r.Now()); err != nil {
			return err
		}
	}
	changed, err := d.Repos.Instances.SetObserved(ctx, inst.ID, inst.AssignmentEpoch, to, r.Now())
	if err != nil {
		if errors.Is(err, errs.ErrStaleAssignment) {
			d.Metrics.EpochMismatchTotal.Inc()
		}
		return err
	}
	if changed {
		ev := events.Event{EventID: ids.New("evt"), EventType: events.InstanceStatusChanged, Provider: inst.Provider,
			TenantID: inst.TenantID, InstanceID: inst.ID, Timestamp: r.Now().UTC(),
			Payload: events.InstanceStatusChangedPayload{State: string(to), Reason: "reconciled: " + reason}}
		if perr := d.Bus.Publish(ctx, ev); perr != nil {
			r.Log.WarnContext(ctx, "could not publish status change", "error", perr)
		}
	}
	return nil
}
