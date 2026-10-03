// Package routing implements provider-node health and the placement engine.
//
// Node health is deliberately independent from instance health: a node can be
// READY while one of its instances is DISCONNECTED and vice versa.
package routing

import (
	"fmt"
	"sort"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
)

// NodeStatus is the health/administrative status of a provider node.
type NodeStatus string

const (
	NodeStarting NodeStatus = "STARTING"
	NodeReady    NodeStatus = "READY"
	NodeDegraded NodeStatus = "DEGRADED"
	NodeDraining NodeStatus = "DRAINING"
	NodeOffline  NodeStatus = "OFFLINE"
)

var nodeTransitions = map[NodeStatus][]NodeStatus{
	NodeStarting: {NodeReady, NodeDegraded, NodeOffline, NodeDraining},
	NodeReady:    {NodeDegraded, NodeDraining, NodeOffline},
	NodeDegraded: {NodeReady, NodeDraining, NodeOffline},
	NodeDraining: {NodeReady, NodeOffline},
	NodeOffline:  {NodeStarting, NodeReady, NodeDegraded, NodeDraining},
}

// Valid reports whether s is a known status.
func (s NodeStatus) Valid() bool { _, ok := nodeTransitions[s]; return ok }

// CanTransitionNode reports whether a node may move from -> to (same = ok).
func CanTransitionNode(from, to NodeStatus) bool {
	if from == to {
		return from.Valid()
	}
	for _, n := range nodeTransitions[from] {
		if n == to {
			return true
		}
	}
	return false
}

// Node is a provider node: one singleton process owning a set of sessions.
type Node struct {
	ID              string
	Provider        string // e.g. "evolution-v2"
	ProviderVersion string
	Endpoint        string
	Capacity        int
	ActiveInstances int
	Status          NodeStatus
	HeartbeatAt     time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Full reports whether no more instances fit.
func (n Node) Full() bool { return n.ActiveInstances >= n.Capacity }

// Utilization is active/capacity in [0,1+].
func (n Node) Utilization() float64 {
	if n.Capacity <= 0 {
		return 1
	}
	return float64(n.ActiveInstances) / float64(n.Capacity)
}

// AcceptsNewAssignments reports whether placement may use the node.
// Only READY nodes with free capacity qualify: DRAINING, OFFLINE, DEGRADED,
// STARTING and FULL nodes never receive new assignments (INV-04).
func (n Node) AcceptsNewAssignments() bool {
	return n.Status == NodeReady && !n.Full()
}

// PlacementRequest constrains a placement decision.
type PlacementRequest struct {
	Provider string   // required provider key, e.g. "evolution-v2"
	Exclude  []string // node ids that must not be chosen (migration source)
}

// Place selects the node for a *new* assignment. It is pure: atomically
// reserving the slot is the persistence adapter's job (it calls Place on rows
// it has locked). The choice is deterministic: lowest utilization first, then
// fewest active instances, then node id.
func Place(nodes []Node, req PlacementRequest) (string, error) {
	excluded := map[string]bool{}
	for _, id := range req.Exclude {
		excluded[id] = true
	}
	var cands []Node
	for _, n := range nodes {
		if excluded[n.ID] || !n.AcceptsNewAssignments() {
			continue
		}
		if req.Provider != "" && n.Provider != req.Provider {
			continue
		}
		cands = append(cands, n)
	}
	if len(cands) == 0 {
		return "", fmt.Errorf("%w: provider=%q", errs.ErrNoCapacity, req.Provider)
	}
	sort.Slice(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.Utilization() != b.Utilization() {
			return a.Utilization() < b.Utilization()
		}
		if a.ActiveInstances != b.ActiveInstances {
			return a.ActiveInstances < b.ActiveInstances
		}
		return a.ID < b.ID
	})
	return cands[0].ID, nil
}

// HeartbeatStale reports whether a node has missed its heartbeat window.
func HeartbeatStale(n Node, now time.Time, timeout time.Duration) bool {
	return n.HeartbeatAt.IsZero() || now.Sub(n.HeartbeatAt) > timeout
}

// NextStatusAfterProbe decides the status after a health probe. Administrative
// DRAINING is never overridden by probes. A failed probe only marks the node
// OFFLINE once the heartbeat window elapsed; it never triggers instance
// failover (the first version favours consistency over availability).
func NextStatusAfterProbe(n Node, probeOK bool, now time.Time, offlineAfter time.Duration) NodeStatus {
	if n.Status == NodeDraining {
		return NodeDraining
	}
	if probeOK {
		return NodeReady
	}
	if HeartbeatStale(n, now, offlineAfter) {
		return NodeOffline
	}
	if n.Status == NodeReady {
		return NodeDegraded
	}
	return n.Status
}
