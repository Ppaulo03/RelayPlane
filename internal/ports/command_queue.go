package ports

import (
	"context"
	"time"
)

// Command represents an intent ("send this message").
//
// Ordering contract: commands sharing a PartitionKey are delivered to the
// handler in publish order, one at a time. Commands with different keys may
// run in parallel. How this is achieved is the adapter's business (Redis:
// hash partitions + partition leases; Kafka: key = PartitionKey).
type Command struct {
	ID             string
	PartitionKey   string
	IdempotencyKey string
	// Payload is any JSON-serialisable value on Publish and a json.RawMessage
	// on delivery.
	Payload any
	// Attempt is the delivery attempt number (1-based), set on delivery.
	Attempt int
	// TraceParent propagates W3C trace context across the broker.
	TraceParent string
}

// Disposition tells the queue what to do with a handled command.
type Disposition int

const (
	// Ack: processed (successfully or terminally); remove from the queue.
	Ack Disposition = iota
	// Retry: failed; redeliver after Result.After, counting an attempt.
	// The queue dead-letters once attempts reach Result.MaxAttempts.
	Retry
	// Defer: not ready yet (e.g. rate limited); redeliver after Result.After
	// without counting an attempt. Ordering for the key is preserved.
	Defer
	// DeadLetter: give up immediately and move to the DLQ.
	DeadLetter
)

// Result is a handler's verdict for one command.
type Result struct {
	Disposition Disposition
	After       time.Duration
	MaxAttempts int    // for Retry; 0 means queue default
	Reason      string // recorded with DLQ entries
}

// CommandHandler processes one command. Returning an error is equivalent to
// Result{Retry} with the queue's default delay.
type CommandHandler func(ctx context.Context, cmd Command) (Result, error)

// DeadLetterEntry is a dead-lettered command.
type DeadLetterEntry struct {
	Command  Command
	Reason   string
	FailedAt time.Time
}

// CommandQueue is the broker-independent command transport.
type CommandQueue interface {
	// Publish enqueues a command. It must reject payloads larger than the
	// configured inline limit (claim-check enforcement, INV-11).
	Publish(ctx context.Context, cmd Command) error
	// Consume delivers commands to handler until ctx is cancelled.
	Consume(ctx context.Context, handler CommandHandler) error
	// Depth returns the number of commands not yet acknowledged.
	Depth(ctx context.Context) (int64, error)
	// DeadLetters lists dead-lettered commands (operations/inspection).
	DeadLetters(ctx context.Context, limit int) ([]DeadLetterEntry, error)
}
