package reconciliation

import (
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
)

var now = time.Now()

func in(desired instance.DesiredState, observed instance.ObservedState, obs Observation) Input {
	return Input{
		Instance: instance.Instance{
			DesiredState: desired, ObservedState: observed,
			LastStatusChange: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute),
		},
		Observation: obs, Now: now, Policy: DefaultPolicy(),
	}
}

func TestDecide(t *testing.T) {
	cases := []struct {
		name string
		in   Input
		want ActionKind
		obs  instance.ObservedState
	}{
		{"converged", in(instance.DesiredConnected, instance.Connected, Observation{State: instance.Connected}), ActionNone, ""},
		{"drift disconnected -> connect", in(instance.DesiredConnected, instance.Connected, Observation{State: instance.Disconnected}), ActionConnect, instance.Disconnected},
		{"record truth", in(instance.DesiredConnected, instance.Connecting, Observation{State: instance.Connected}), ActionUpdateObserved, instance.Connected},
		{"awaiting pairing: no connect", in(instance.DesiredConnected, instance.Connecting, Observation{State: instance.AwaitingPairing}), ActionUpdateObserved, instance.AwaitingPairing},
		{"logged out: needs human", in(instance.DesiredConnected, instance.Connected, Observation{State: instance.LoggedOut}), ActionUpdateObserved, instance.LoggedOut},
		{"desired deleted + exists", in(instance.DesiredDeleted, instance.Deleting, Observation{State: instance.Connected}), ActionDelete, ""},
		{"desired deleted + gone", in(instance.DesiredDeleted, instance.Deleting, Observation{Err: errs.ErrInstanceNotFound}), ActionFinalizeDelete, instance.Deleted},
		{"desired disconnected + connected", in(instance.DesiredDisconnected, instance.Connected, Observation{State: instance.Connected}), ActionDisconnect, ""},
		{"provider down: do nothing", in(instance.DesiredConnected, instance.Connected, Observation{Err: errs.ErrProviderUnavailable}), ActionNone, ""},
		{"auth failure: do nothing", in(instance.DesiredConnected, instance.Connected, Observation{Err: errs.ErrAuthenticationFailed}), ActionNone, ""},
		{"provider lost live session", in(instance.DesiredConnected, instance.Connected, Observation{Err: errs.ErrInstanceNotFound}), ActionUpdateObserved, instance.Failed},
	}
	for _, c := range cases {
		d := Decide(c.in)
		if d.Action != c.want || (c.obs != "" && d.Observed != c.obs) {
			t.Errorf("%s: got %+v want action=%s observed=%s", c.name, d, c.want, c.obs)
		}
	}
}

func TestDecide_ConnectGraceAvoidsFightingProvider(t *testing.T) {
	i := in(instance.DesiredConnected, instance.Connected, Observation{State: instance.Disconnected})
	i.Instance.LastStatusChange = now.Add(-2 * time.Second)
	if d := Decide(i); d.Action == ActionConnect {
		t.Fatalf("must wait out the grace period, got %+v", d)
	}
}

func TestDecide_ResumesInterruptedProvisioning(t *testing.T) {
	i := in(instance.DesiredConnected, instance.Creating, Observation{Err: errs.ErrInstanceNotFound})
	if d := Decide(i); d.Action != ActionCreate {
		t.Fatalf("stuck CREATING must resume, got %+v", d)
	}
	i.Instance.UpdatedAt = now.Add(-time.Second)
	if d := Decide(i); d.Action != ActionNone {
		t.Fatalf("fresh CREATING must be left alone, got %+v", d)
	}
}

func TestDecide_SkipsActiveMigration(t *testing.T) {
	i := in(instance.DesiredConnected, instance.Migrating, Observation{State: instance.Disconnected})
	i.MigrationActive = true
	if d := Decide(i); d.Action != ActionNone {
		t.Fatalf("must not interfere with an active migration: %+v", d)
	}
	// the operation row is the truth, not observed_state (which can lag behind it)
	i2 := in(instance.DesiredConnected, instance.Connected, Observation{State: instance.Disconnected})
	i2.MigrationActive = true
	if d := Decide(i2); d.Action != ActionNone {
		t.Fatalf("an active migration blocks corrective actions whatever observed_state says: %+v", d)
	}
	i.MigrationActive = false // blocked migration: reconcile normally
	if d := Decide(i); d.Action == ActionNone {
		t.Fatalf("blocked migration must not freeze the instance: %+v", d)
	}
}

// INV-12 (pure part): repeatedly applying Decide against an operational
// provider converges observed to desired.
func TestINV12_Converges(t *testing.T) {
	type sim struct{ providerState, catalog instance.ObservedState }
	s := sim{providerState: instance.Disconnected, catalog: instance.Disconnected}
	inst := instance.Instance{DesiredState: instance.DesiredConnected, ObservedState: s.catalog, LastStatusChange: now.Add(-time.Hour)}
	for step := 0; step < 5 && inst.ObservedState != instance.Connected; step++ {
		d := Decide(Input{Instance: inst, Observation: Observation{State: s.providerState}, Now: now, Policy: DefaultPolicy()})
		switch d.Action {
		case ActionConnect:
			s.providerState = instance.Connected // provider honours the request
		case ActionUpdateObserved:
			inst.ObservedState = d.Observed
		}
	}
	if inst.ObservedState != instance.Connected {
		t.Fatalf("expected convergence to CONNECTED, observed=%s", inst.ObservedState)
	}
	if d := Decide(Input{Instance: inst, Observation: Observation{State: s.providerState}, Now: now, Policy: DefaultPolicy()}); d.Action != ActionNone {
		t.Fatalf("converged state must be stable, got %+v", d)
	}
}
