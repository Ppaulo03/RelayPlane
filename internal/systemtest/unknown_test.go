package systemtest

import (
	"errors"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/memory"
	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/messaging"
)

// What an operator or an agent needs when a send is ambiguous: find the UNKNOWN messages, see how long they have waited (the gauge
// that feeds the alert), and watch the instance move again once one is resolved.
func TestUnknown_CanBeFoundMeasuredAndResolved(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	other := e.CreateInstance(e.Tenant, "b", true)
	e.StartOutbox()
	e.StartWorkers(2)
	e.StartProjector()

	e.Provider.FailNext(memory.FailAmbiguous) // the provider times out after it may have sent
	first, _, err := e.SendText(e.Tenant, inst.ID, "Confirma amanhã às 15h?", "k1")
	if err != nil {
		t.Fatal(err)
	}
	m := e.WaitMessage(first.MessageID, messaging.StatusUnknown)
	if m.ErrorCode != "AMBIGUOUS_DISPATCH" {
		t.Fatalf("the reason is recorded: %+v", m)
	}
	// a later message of the same instance is held back; another instance is not
	second, _, _ := e.SendText(e.Tenant, inst.ID, "segunda", "k2")
	elsewhere, _, _ := e.SendText(e.Tenant, other.ID, "outra instância", "k3")
	e.WaitMessage(elsewhere.MessageID, messaging.StatusAccepted)
	time.Sleep(300 * time.Millisecond)
	if cur, _ := e.Repos.Messages.Get(bg, second.MessageID); cur.Status != messaging.StatusQueued && cur.Status != messaging.StatusDispatching {
		t.Fatalf("the barrier holds the next message back: %s", cur.Status)
	}

	// found by status, of the instance, and nobody else's
	list, err := e.App.Messages.List(bg, e.Tenant, messaging.StatusUnknown, inst.ID, 50)
	if err != nil || len(list) != 1 || list[0].ID != first.MessageID {
		t.Fatalf("list: %+v %v", list, err)
	}
	if other, _ := e.App.Messages.List(bg, e.Tenant2, messaging.StatusUnknown, "", 50); len(other) != 0 {
		t.Errorf("another tenant sees nothing: %+v", other)
	}
	if _, err := e.App.Messages.List(bg, e.Tenant, "WEIRD", "", 50); !errors.Is(err, errs.ErrInvalidArgument) {
		t.Errorf("an unknown status is refused: %v", err)
	}
	if _, err := e.App.Messages.List(bg, e.Tenant2, messaging.StatusUnknown, inst.ID, 50); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("another tenant's instance: %v", err)
	}

	// the gauges behind the alert
	app.RecordUnknownGauges(bg, e.App.Deps)
	if v := gaugeValue(t, e, "relayplane_unknown_messages"); v != 1 {
		t.Errorf("one message waits for a decision: %v", v)
	}
	if v := gaugeValue(t, e, "relayplane_unknown_oldest_seconds"); v < 0 {
		t.Errorf("its age: %v", v)
	}

	// resolving frees the instance and the gauge
	if _, err := e.App.Messages.Resolve(bg, e.Tenant, first.MessageID, app.OutcomeNotSent); err != nil {
		t.Fatal(err)
	}
	e.WaitMessage(second.MessageID, messaging.StatusAccepted)
	app.RecordUnknownGauges(bg, e.App.Deps)
	if v := gaugeValue(t, e, "relayplane_unknown_messages"); v != 0 {
		t.Errorf("nothing waits any more: %v", v)
	}
}

func gaugeValue(t *testing.T, e *Env, name string) float64 {
	t.Helper()
	fams, err := e.Metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == name && len(f.GetMetric()) > 0 {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
