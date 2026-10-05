package memory

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

func (s *Store) scrub(m *messaging.Message, at time.Time) {
	m.Recipient, m.Payload, m.ErrorMessage, m.ErasedAt = "", json.RawMessage(`{}`), "", at
}

// scrubCommand removes the copy of the command of a message (it holds the text and the recipient) unless the message
// is still in the provider's hands.
func (s *Store) scrubCommand(id string) {
	if m := s.messages[id]; m != nil && (m.Status == messaging.StatusQueued || m.Status == messaging.StatusDispatching) {
		return
	}
	for _, e := range s.outbox {
		if e.MessageID == id {
			e.Command = json.RawMessage(`{}`)
		}
	}
}

func (r msgRepo) EraseRecipient(_ context.Context, tenantID, number string, at time.Time) (ports.ErasedMessages, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out ports.ErasedMessages
	for _, m := range r.s.messages {
		if m.TenantID != tenantID || m.Recipient != number || !m.ErasedAt.IsZero() {
			continue
		}
		out.Anonymized++
		if m.Status == messaging.StatusQueued {
			m.Status, m.ErrorCode = messaging.StatusFailed, "ERASED"
			out.Cancelled++
		}
		r.s.scrub(m, at)
		m.UpdatedAt = at
		r.s.scrubCommand(m.ID)
	}
	return out, nil
}

func (r msgRepo) ScrubTerminalBefore(_ context.Context, before, at time.Time, limit int) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var due []*messaging.Message
	for _, m := range r.s.messages {
		if !m.ErasedAt.IsZero() || !m.CreatedAt.Before(before) {
			continue
		}
		switch m.Status {
		case messaging.StatusAccepted, messaging.StatusDelivered, messaging.StatusRead, messaging.StatusFailed, messaging.StatusUnknown:
			due = append(due, m)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].CreatedAt.Before(due[j].CreatedAt) })
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	for _, m := range due {
		r.s.scrub(m, at)
		r.s.scrubCommand(m.ID)
	}
	return int64(len(due)), nil
}

func (r blobRepo) ListBySubject(_ context.Context, tenantID, subject string) ([]media.Blob, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var out []media.Blob
	for _, b := range r.s.blobs {
		if b.TenantID == tenantID && b.Subject == subject && b.Status != media.BlobDeleted {
			out = append(out, *b)
		}
	}
	return out, nil
}

// payloadFrom reads payload.from of an event whatever form its payload has.
func payloadFrom(p any) string {
	raw, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	var probe struct {
		From string `json:"from"`
	}
	_ = json.Unmarshal(raw, &probe)
	return probe.From
}

func (r deliveriesRepo) PurgeDead(_ context.Context, before time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for id, d := range r.s.deliveries {
		if d.Status == subscription.DeliveryDead && d.CreatedAt.Before(before) {
			delete(r.s.deliveries, id)
			n++
		}
	}
	return n, nil
}

func (r deliveriesRepo) PurgePending(_ context.Context, before, now time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for id, d := range r.s.deliveries {
		if d.Status == subscription.DeliveryPending && d.CreatedAt.Before(before) && !d.leaseUntil.After(now) {
			delete(r.s.deliveries, id)
			n++
		}
	}
	return n, nil
}

func (r deliveriesRepo) EraseContact(_ context.Context, tenantID, number string) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for id, d := range r.s.deliveries {
		if d.TenantID == tenantID && payloadFrom(d.Event.Payload) == number {
			delete(r.s.deliveries, id)
			n++
		}
	}
	return n, nil
}

func (r inboundMediaRepo) EraseContact(_ context.Context, tenantID, number string) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for id, j := range r.s.inbound {
		if j.TenantID == tenantID && payloadFrom(j.Event.Payload) == number {
			delete(r.s.inbound, id)
			n++
		}
	}
	return n, nil
}
