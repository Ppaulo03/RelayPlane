package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
)

// ContactService erases what RelayPlane keeps about one person (a phone number) on behalf of a tenant.
type ContactService struct{ d Deps }

// ErasureReport says what an erasure removed. It never repeats the number.
type ErasureReport struct {
	// MessagesAnonymized: messages sent to the contact whose recipient and content were removed (the ledger row stays);
	// MessagesCancelled: of those, the ones that had not been sent and now never will be.
	MessagesAnonymized int `json:"messages_anonymized"`
	MessagesCancelled  int `json:"messages_cancelled"`
	// EventsDeleted: webhook deliveries (pending, delivered or dead-lettered) about messages the contact sent, plus
	// attachment jobs still waiting.
	EventsDeleted int64 `json:"events_deleted"`
	// AttachmentsDeleted: stored files the contact sent.
	AttachmentsDeleted int       `json:"attachments_deleted"`
	ErasedAt           time.Time `json:"erased_at"`
}

// NormalizeNumber reduces what a caller typed ("+55 (62) 99999-9999") to the digits RelayPlane stores.
func NormalizeNumber(raw string) (string, error) {
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '+' || r == ' ' || r == '-' || r == '(' || r == ')' || r == '.':
		default:
			return "", fmt.Errorf("%w: a phone number has digits only", errs.ErrInvalidArgument)
		}
	}
	n := b.String()
	if len(n) < 8 || len(n) > 20 {
		return "", fmt.Errorf("%w: a phone number has between 8 and 20 digits", errs.ErrInvalidArgument)
	}
	return n, nil
}

// Erase removes the contact's personal data for the tenant: the recipient and content of the messages sent to them, the
// events (inbound messages, receipts, DLQ) that mention them and the attachments they sent. It is idempotent. What it
// cannot reach is documented in docs/OPERATIONS.md (transient broker streams, the provider's own state, logs).
func (s *ContactService) Erase(ctx context.Context, tenantID, rawNumber string) (*ErasureReport, error) {
	number, err := NormalizeNumber(rawNumber)
	if err != nil {
		return nil, err
	}
	at := s.d.now().UTC()
	rep := &ErasureReport{ErasedAt: at}

	// a recipient can have been given in any of the usual spellings
	for _, spelling := range []string{number, "+" + number, number + "@s.whatsapp.net"} {
		res, err := s.d.Repos.Messages.EraseRecipient(ctx, tenantID, spelling, at)
		if err != nil {
			return nil, fmt.Errorf("erase messages: %w", err)
		}
		rep.MessagesAnonymized += res.Anonymized
		rep.MessagesCancelled += res.Cancelled
	}
	if rep.EventsDeleted, err = s.d.Repos.Deliveries.EraseContact(ctx, tenantID, number); err != nil {
		return nil, fmt.Errorf("erase events: %w", err)
	}
	waiting, err := s.d.Repos.Events.EraseContact(ctx, tenantID, number) // accepted, not yet fanned out
	if err != nil {
		return nil, fmt.Errorf("erase queued events: %w", err)
	}
	rep.EventsDeleted += waiting
	jobs, err := s.d.Repos.InboundMedia.EraseContact(ctx, tenantID, number)
	if err != nil {
		return nil, fmt.Errorf("erase attachment jobs: %w", err)
	}
	rep.EventsDeleted += jobs

	blobs, err := s.d.Repos.Blobs.ListBySubject(ctx, tenantID, number)
	if err != nil {
		return nil, fmt.Errorf("list attachments: %w", err)
	}
	for _, b := range blobs {
		if err := s.d.Blob.Delete(ctx, b.ObjectKey); err != nil && !errors.Is(err, errs.ErrNotFound) {
			return nil, fmt.Errorf("delete attachment: %w", err)
		}
		if err := s.d.Repos.Blobs.MarkDeleted(ctx, b.ID, at); err != nil {
			return nil, fmt.Errorf("delete attachment record: %w", err)
		}
		rep.AttachmentsDeleted++
	}
	sum := sha256.Sum256([]byte(tenantID + "|" + number))
	s.d.Log.InfoContext(ctx, "contact data erased", "tenant_id", tenantID, "contact", hex.EncodeToString(sum[:6]),
		"messages", rep.MessagesAnonymized, "cancelled", rep.MessagesCancelled, "events", rep.EventsDeleted, "attachments", rep.AttachmentsDeleted)
	return rep, nil
}

// RetentionService applies the retention periods: personal content does not stay forever just because nobody asked.
type RetentionService struct{ d Deps }

// RetentionPolicy sets how long each kind of record keeps its personal content. Zero keeps it indefinitely.
type RetentionPolicy struct {
	// Messages: finished outbound messages lose their recipient and content after this long (the ledger row stays).
	Messages time.Duration
	// DeadDeliveries: dead-lettered webhook deliveries (they hold the user's text) are deleted after this long.
	DeadDeliveries time.Duration
}

// DefaultRetention is what the platform does without configuration: 90 days of message content, 30 days of DLQ.
func DefaultRetention() RetentionPolicy {
	return RetentionPolicy{Messages: 90 * 24 * time.Hour, DeadDeliveries: 30 * 24 * time.Hour}
}

// Apply runs one pass of the policy and returns how many records it changed.
func (s *RetentionService) Apply(ctx context.Context, p RetentionPolicy, batch int) (messages, dead int64, err error) {
	now := s.d.now().UTC()
	if p.Messages > 0 {
		for {
			n, e := s.d.Repos.Messages.ScrubTerminalBefore(ctx, now.Add(-p.Messages), now, batch)
			messages += n
			if e != nil {
				return messages, dead, e
			}
			if n < int64(batch) || batch <= 0 {
				break
			}
		}
	}
	if p.DeadDeliveries > 0 {
		if dead, err = s.d.Repos.Deliveries.PurgeDead(ctx, now.Add(-p.DeadDeliveries)); err != nil {
			return messages, dead, err
		}
	}
	return messages, dead, nil
}
