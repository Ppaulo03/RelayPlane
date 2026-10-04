package ports

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// ErasureRepository holds the tombstones of erased contacts. Erasing deletes what exists NOW; a tombstone also stops what was already on
// its way (an event in the broker, an attachment being downloaded) from recreating the contact's data afterwards. Whoever materialises
// personal data (the fan-out, the attachment ingestor) writes it FIRST and asks Erased AFTER, and removes what it wrote if the answer is
// yes; the erasure marks BEFORE it deletes. Whatever the interleaving, one of them cleans up.
type ErasureRepository interface {
	// Mark records that the contact was erased at `at` (the latest mark wins). The stored subject is a hash, never the number.
	Mark(ctx context.Context, tenantID, number string, at time.Time) error
	// Erased reports whether the contact an inbound event is about was erased at or after the moment the event was accepted
	// (Event.AcceptedAt). A message that arrives after the erasure is new data and is not erased. Events that are not about a
	// contact, or that carry no AcceptedAt, are never erased.
	Erased(ctx context.Context, ev events.Event) (bool, error)
	// Purge forgets tombstones older than `before`: in-flight data never lives that long.
	Purge(ctx context.Context, before time.Time) (int64, error)
}
