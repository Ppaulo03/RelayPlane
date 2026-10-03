package ports

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// EventHandler processes one canonical event. Returning an error leaves the
// event un-acknowledged for redelivery (at-least-once); handlers must be
// idempotent (event ids are deterministic for the same fact).
type EventHandler func(ctx context.Context, ev events.Event) error

// EventBus carries facts ("message received", "instance disconnected"),
// semantically distinct from CommandQueue even when both use the same broker.
type EventBus interface {
	Publish(ctx context.Context, ev events.Event) error
	// Subscribe blocks, delivering events to handler for the consumer group
	// `group` until ctx is cancelled. Each group sees every event once.
	Subscribe(ctx context.Context, group string, handler EventHandler) error
}

// EventBusGroupStats describes one consumer group.
type EventBusGroupStats struct {
	Name string
	// Lag is how many retained events the group has not been delivered yet.
	Lag int64
	// Pending is the number of delivered but unacknowledged events.
	Pending int64
	// OldestPending is the age of the oldest unacknowledged event.
	OldestPending time.Duration
	// Lost is how many events were trimmed away before this group could read them
	// (Lag beyond what the stream still retains): data loss, alert immediately.
	Lost int64
}

// EventBusStats is the retention health of the bus.
type EventBusStats struct {
	Length    int64 // events currently retained
	Retention int64 // configured retention (approximate maximum length)
	Groups    []EventBusGroupStats
}

// TrimRisk is the worst consumer lag as a fraction of the retention window:
// >= 1 means a consumer has already been overtaken by trimming.
func (s EventBusStats) TrimRisk() float64 {
	if s.Retention <= 0 {
		return 0
	}
	worst := 0.0
	for _, g := range s.Groups {
		r := float64(g.Lag+g.Lost) / float64(s.Retention)
		if g.Lost > 0 && r < 1 {
			r = 1 // events are already gone
		}
		if r > worst {
			worst = r
		}
	}
	return worst
}

// EventBusInspector is implemented by buses that can report retention health.
type EventBusInspector interface {
	Stats(ctx context.Context) (EventBusStats, error)
}
