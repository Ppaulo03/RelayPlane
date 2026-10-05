package ports

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

// ErasureRepository holds the tombstones of erased contacts. Erasing deletes what exists NOW; a tombstone also stops what was already on
// its way (an event in the broker, a delivery already claimed, an attachment being downloaded) from recreating the contact's data afterwards.
// Whoever materialises personal data (the fan-out, the dispatcher, the attachment ingestor) writes it FIRST and asks Erased AFTER, and removes
// what it wrote if the answer is yes; the erasure marks BEFORE it deletes. Whatever the interleaving, one of them cleans up.
//
// Tombstones are never purged: they are one small row per erased contact, and nothing bounds how old an event can be when it finally reaches
// a consumer (the broker keeps entries by size, not by age), so any expiry would make "erased data does not come back" untrue after it.
// The subject is an HMAC of the number (events.ErasureSubject), never the number.
type ErasureRepository interface {
	// Mark records that the contact was erased at `at` (the latest mark wins).
	Mark(ctx context.Context, tenantID, subject string, at time.Time) error
	// Erased reports whether the contact was erased at or after the moment a message of theirs was accepted. A message that arrives after
	// the erasure is new data and is not erased.
	Erased(ctx context.Context, tenantID, subject string, acceptedAt time.Time) (bool, error)
}

// ErasedEvent asks whether an inbound event is about a contact that was erased after the event was accepted. Events that are not about a
// contact, or that carry no AcceptedAt, are never erased.
func ErasedEvent(ctx context.Context, r ErasureRepository, key []byte, ev events.Event) (bool, error) {
	subject := ev.ErasureSubject(key)
	if subject == "" || ev.AcceptedAt == nil {
		return false, nil
	}
	return r.Erased(ctx, ev.TenantID, subject, *ev.AcceptedAt)
}
