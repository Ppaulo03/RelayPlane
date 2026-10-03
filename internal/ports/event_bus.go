package ports

import (
	"context"

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
