package contracttest

import (
	"context"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/ports"
)

// Finding what waits for a decision: messages of a tenant by status, in the order they were accepted, and the platform-wide count
// and age of the UNKNOWN ones.
func messageListContract(t *testing.T, f RepoFactory) {
	fx, ctx := newFixture(t, f), context.Background()
	fx.tenant(t, "t1")
	fx.tenant(t, "t2")
	fx.node(t, "node-01", 10)
	fx.instance(t, "inst_1", "t1")
	fx.instance(t, "inst_2", "t1")
	fx.instance(t, "inst_3", "t2")
	build := buildFor
	for _, c := range []struct{ id, tenant, inst string }{
		{"m1", "t1", "inst_1"}, {"m2", "t1", "inst_1"}, {"m3", "t1", "inst_2"}, {"m4", "t1", "inst_1"}, {"m5", "t2", "inst_3"},
	} {
		m := personMsg(c.id, c.tenant, c.inst, "5562111111111", "x")
		if _, err := fx.r.Messages.CreateWithOutbox(ctx, m, build(c.id)); err != nil {
			t.Fatal(err)
		}
	}
	// m1, m2 (inst_1), m3 (inst_2) and m5 (another tenant) become UNKNOWN; m4 is delivered
	for _, id := range []string{"m1", "m2", "m3", "m5"} {
		walk(t, fx, id, messaging.StatusDispatching, messaging.StatusUnknown)
	}
	walk(t, fx, "m4", messaging.StatusDispatching, messaging.StatusAccepted, messaging.StatusDelivered)

	ids := func(ms []messaging.Message) []string {
		var out []string
		for _, m := range ms {
			out = append(out, m.ID)
		}
		return out
	}
	got, err := fx.r.Messages.ListByStatus(ctx, "t1", messaging.StatusUnknown, "", 100)
	if err != nil || len(got) != 3 || got[0].ID != "m1" || got[1].ID != "m2" || got[2].ID != "m3" {
		t.Fatalf("UNKNOWN of t1, by instance then sequence: %v %v", ids(got), err)
	}
	if one, _ := fx.r.Messages.ListByStatus(ctx, "t1", messaging.StatusUnknown, "inst_1", 100); len(one) != 2 {
		t.Errorf("one instance: %v", ids(one))
	}
	if lim, _ := fx.r.Messages.ListByStatus(ctx, "t1", messaging.StatusUnknown, "", 1); len(lim) != 1 {
		t.Errorf("limit: %v", ids(lim))
	}
	if d, _ := fx.r.Messages.ListByStatus(ctx, "t1", messaging.StatusDelivered, "", 100); len(d) != 1 || d[0].ID != "m4" {
		t.Errorf("another status: %v", ids(d))
	}
	if other, _ := fx.r.Messages.ListByStatus(ctx, "t2", messaging.StatusUnknown, "", 100); len(other) != 1 || other[0].ID != "m5" {
		t.Errorf("a tenant sees only its own messages: %v", ids(other))
	}
	if none, _ := fx.r.Messages.ListByStatus(ctx, "t1", messaging.StatusUnknown, "inst_3", 100); len(none) != 0 {
		t.Errorf("another tenant's instance: %v", ids(none))
	}

	n, oldest, err := fx.r.Messages.UnknownStats(ctx)
	if err != nil || n != 4 || oldest < 0 || oldest > time.Minute {
		t.Fatalf("stats: %d %v %v", n, oldest, err)
	}
	// resolving one lowers the count
	if _, err := fx.r.Messages.Transition(ctx, "m1", []messaging.Status{messaging.StatusUnknown}, messaging.StatusAccepted, ports.MessagePatch{}); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := fx.r.Messages.UnknownStats(ctx); n != 3 {
		t.Errorf("after a resolution: %d", n)
	}
}
