package routing

import (
	"errors"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
)

func node(id string, st NodeStatus, active, cap int) Node {
	return Node{ID: id, Provider: "evolution-v2", Status: st, ActiveInstances: active, Capacity: cap}
}

func TestPlace_PrefersLeastUtilizedReadyNode(t *testing.T) {
	nodes := []Node{node("node-01", NodeReady, 75, 100), node("node-02", NodeReady, 21, 100), node("node-03", NodeDraining, 0, 100)}
	got, err := Place(nodes, PlacementRequest{Provider: "evolution-v2"})
	if err != nil || got != "node-02" {
		t.Fatalf("got %q, %v; want node-02", got, err)
	}
}

func TestPlace_IneligibleNodesNeverChosen(t *testing.T) { // INV-04
	for _, st := range []NodeStatus{NodeDraining, NodeOffline, NodeDegraded, NodeStarting} {
		_, err := Place([]Node{node("n", st, 0, 10)}, PlacementRequest{Provider: "evolution-v2"})
		if !errors.Is(err, errs.ErrNoCapacity) {
			t.Errorf("%s node must not receive assignments, err=%v", st, err)
		}
	}
	if _, err := Place([]Node{node("n", NodeReady, 10, 10)}, PlacementRequest{}); !errors.Is(err, errs.ErrNoCapacity) {
		t.Errorf("FULL node must not receive assignments, err=%v", err)
	}
}

func TestPlace_ProviderVersionAndExclusions(t *testing.T) {
	v3 := node("v3-01", NodeReady, 0, 10)
	v3.Provider = "evolution-v3"
	nodes := []Node{node("v2-01", NodeReady, 5, 10), v3}
	if got, _ := Place(nodes, PlacementRequest{Provider: "evolution-v3"}); got != "v3-01" {
		t.Errorf("provider filter: got %q", got)
	}
	if got, _ := Place(nodes, PlacementRequest{Provider: "evolution-v2", Exclude: []string{"v2-01"}}); got != "" {
		t.Errorf("excluded node chosen: %q", got)
	}
}

func TestPlace_DeterministicTieBreak(t *testing.T) {
	nodes := []Node{node("b", NodeReady, 1, 10), node("a", NodeReady, 1, 10)}
	for i := 0; i < 10; i++ {
		if got, _ := Place(nodes, PlacementRequest{}); got != "a" {
			t.Fatalf("tie must break on id, got %q", got)
		}
	}
}

func TestNodeTransitions(t *testing.T) {
	if !CanTransitionNode(NodeReady, NodeDraining) || !CanTransitionNode(NodeDraining, NodeReady) {
		t.Error("drain/resume must be allowed")
	}
	if CanTransitionNode(NodeDraining, NodeStarting) {
		t.Error("DRAINING -> STARTING is not allowed")
	}
}

func TestNextStatusAfterProbe(t *testing.T) {
	now := time.Now()
	n := Node{Status: NodeReady, HeartbeatAt: now.Add(-5 * time.Second)}
	if got := NextStatusAfterProbe(n, true, now, time.Minute); got != NodeReady {
		t.Errorf("ok probe: %s", got)
	}
	if got := NextStatusAfterProbe(n, false, now, time.Minute); got != NodeDegraded {
		t.Errorf("single failure should degrade, got %s", got)
	}
	n.HeartbeatAt = now.Add(-2 * time.Minute)
	if got := NextStatusAfterProbe(n, false, now, time.Minute); got != NodeOffline {
		t.Errorf("stale heartbeat should go offline, got %s", got)
	}
	n.Status = NodeDraining
	if got := NextStatusAfterProbe(n, true, now, time.Minute); got != NodeDraining {
		t.Errorf("DRAINING must be sticky across probes, got %s", got)
	}
}
