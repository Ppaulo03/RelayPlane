// Package messaging models outbound messages, their state machine, the
// command envelope and rate policies.
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/ownership"
)

// Type is the outbound message kind.
type Type string

const (
	TypeText     Type = "text"
	TypeImage    Type = "image"
	TypeAudio    Type = "audio"
	TypeVideo    Type = "video"
	TypeDocument Type = "document"
)

// IsMedia reports whether the type requires a claim-checked attachment.
func (t Type) IsMedia() bool {
	switch t {
	case TypeImage, TypeAudio, TypeVideo, TypeDocument:
		return true
	}
	return false
}

// Valid reports whether t is supported.
func (t Type) Valid() bool { return t == TypeText || t.IsMedia() }

// Status is the outbound delivery state.
type Status string

const (
	StatusQueued      Status = "QUEUED"
	StatusDispatching Status = "DISPATCHING"
	StatusAccepted    Status = "ACCEPTED"
	StatusDelivered   Status = "DELIVERED"
	StatusRead        Status = "READ"
	StatusFailed      Status = "FAILED"
	StatusUnknown     Status = "UNKNOWN" // ambiguous outcome; never auto-retried
)

var statusTransitions = map[Status][]Status{
	StatusQueued:      {StatusDispatching, StatusFailed},
	StatusDispatching: {StatusQueued, StatusAccepted, StatusFailed, StatusUnknown},
	StatusAccepted:    {StatusDelivered, StatusRead, StatusFailed},
	StatusDelivered:   {StatusRead},
	StatusUnknown:     {StatusAccepted, StatusDelivered, StatusRead, StatusFailed},
}

// CanTransition reports whether from -> to is allowed (same state = no-op ok).
func CanTransition(from, to Status) bool {
	if from == to {
		return true
	}
	for _, n := range statusTransitions[from] {
		if n == to {
			return true
		}
	}
	return false
}

// IsTerminal reports whether no worker will touch the message again.
func (s Status) IsTerminal() bool {
	return s == StatusAccepted || s == StatusDelivered || s == StatusRead || s == StatusFailed || s == StatusUnknown
}

// Message is the persisted outbound message record.
type Message struct {
	ID              string
	TenantID        string
	InstanceID      string
	IdempotencyKey  string
	NodeID          string
	AssignmentEpoch int64
	PartitionKey    string
	Recipient       string
	Type            Type
	Payload         json.RawMessage
	// SequenceNo is the per-instance, gapless, commit-ordered sequence that defines
	// the dispatch order (INV-07). It is allocated in the same transaction as the row.
	SequenceNo        int64
	Status            Status
	ProviderMessageID string
	AttemptCount      int
	ErrorCode         string
	ErrorMessage      string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	// AcceptedAt is when the provider accepted the send (zero until the message reaches ACCEPTED).
	AcceptedAt time.Time
	// TraceParent is the W3C trace context of the request that created the message.
	TraceParent string
	// ErasedAt is when the recipient and the content were removed (retention or an erasure request); zero while they exist.
	ErasedAt time.Time
}

// ReplyTo is the message an outbound message answers by QUOTING it (the grey box above the reply). Text is the preview
// shown in that box: the node keeps no message history, so the preview must travel with the request.
type ReplyTo struct {
	ProviderMessageID string `json:"provider_message_id"`
	Text              string `json:"text,omitempty"`
	// FromMe is true when the quoted message is one WE sent (its provider id comes from message.outbound_status).
	FromMe bool `json:"from_me,omitempty"`
}

// MaxQuotePreview bounds the preview text of a quote (characters).
const MaxQuotePreview = 1024

// Payload is the claim-check message body carried in the command.
type Payload struct {
	Text     string     `json:"text,omitempty"`
	Caption  string     `json:"caption,omitempty"`
	Filename string     `json:"filename,omitempty"`
	Media    *media.Ref `json:"media,omitempty"`
	ReplyTo  *ReplyTo   `json:"reply_to,omitempty"`
}

// Envelope is the canonical outbound command (see the design doc §17).
type Envelope struct {
	MessageID      string               `json:"message_id"`
	IdempotencyKey string               `json:"idempotency_key,omitempty"`
	TenantID       string               `json:"tenant_id"`
	InstanceID     string               `json:"instance_id"`
	Assignment     ownership.Assignment `json:"assignment"`
	PartitionKey   string               `json:"partition_key"`
	Sequence       int64                `json:"sequence_no"`
	Type           Type                 `json:"type"`
	To             string               `json:"to"`
	Payload        Payload              `json:"payload"`
	TraceID        string               `json:"trace_id,omitempty"`
	TraceParent    string               `json:"traceparent,omitempty"`
	CreatedAt      time.Time            `json:"created_at"`
}

// Validate checks structural validity (not tenant ownership of media: see
// media.ValidateRef, applied at accept time and again by the worker).
func (e Envelope) Validate() error {
	switch {
	case e.MessageID == "", e.TenantID == "", e.InstanceID == "":
		return fmt.Errorf("%w: message_id, tenant_id and instance_id are required", errs.ErrInvalidArgument)
	case e.PartitionKey != e.InstanceID:
		return fmt.Errorf("%w: partition_key must equal instance_id to preserve ordering", errs.ErrInvalidArgument)
	case e.Assignment.InstanceID != e.InstanceID || e.Assignment.NodeID == "" || e.Assignment.Epoch <= 0:
		return fmt.Errorf("%w: assignment snapshot is required", errs.ErrInvalidArgument)
	case e.Sequence <= 0:
		return fmt.Errorf("%w: sequence_no is required (ordering barrier)", errs.ErrInvalidArgument)
	case e.To == "":
		return fmt.Errorf("%w: recipient is required", errs.ErrInvalidArgument)
	case !e.Type.Valid():
		return fmt.Errorf("%w: unsupported message type %q", errs.ErrInvalidArgument, e.Type)
	}
	if e.Type == TypeText {
		if e.Payload.Text == "" || e.Payload.Media != nil {
			return fmt.Errorf("%w: text message requires payload.text and no media", errs.ErrInvalidArgument)
		}
	} else if e.Payload.Media == nil {
		return fmt.Errorf("%w: %s message requires payload.media (claim check)", errs.ErrInvalidArgument, e.Type)
	}
	return nil
}

// OutboxEntry is a command waiting in the transactional outbox. It is written
// in the same transaction as the message, so "accepted" always implies
// "will be published", and publication happens strictly in Sequence order.
type OutboxEntry struct {
	InstanceID   string
	MessageID    string
	Sequence     int64
	Command      json.RawMessage // the serialized Envelope
	CreatedAt    time.Time
	DispatchedAt time.Time // zero until published
}

// Attachment is the resolved media handed to a provider by the worker.
type Attachment struct {
	ContentType string
	Size        int64
	SHA256      string
	URL         string                                           // short-lived signed URL, when available
	Open        func(ctx context.Context) (io.ReadCloser, error) // streaming access
}

// OutboundMessage is what a MessagingProvider receives.
type OutboundMessage struct {
	ID       string
	To       string
	Type     Type
	Text     string
	Caption  string
	Filename string
	Media    *Attachment
	// ReplyTo quotes an earlier message (nil: a plain message).
	ReplyTo *ReplyTo
}

// SendResult is a provider's acknowledgement of a dispatch.
type SendResult struct {
	ProviderMessageID string
	Status            Status // usually ACCEPTED
}

// RatePolicy is a configurable send-rate policy. Zero fields mean "inherit".
type RatePolicy struct {
	MinInterval   time.Duration `json:"min_interval,omitempty"`
	Burst         int           `json:"burst,omitempty"`
	MaxPerMinute  int           `json:"max_per_minute,omitempty"`
	MaxConcurrent int           `json:"max_concurrent,omitempty"`
	Cooldown      time.Duration `json:"cooldown,omitempty"`
}

// Merge returns over applied on top of base: every non-zero field of over wins.
func (base RatePolicy) Merge(over *RatePolicy) RatePolicy {
	if over == nil {
		return base
	}
	out := base
	if over.MinInterval != 0 {
		out.MinInterval = over.MinInterval
	}
	if over.Burst != 0 {
		out.Burst = over.Burst
	}
	if over.MaxPerMinute != 0 {
		out.MaxPerMinute = over.MaxPerMinute
	}
	if over.MaxConcurrent != 0 {
		out.MaxConcurrent = over.MaxConcurrent
	}
	if over.Cooldown != 0 {
		out.Cooldown = over.Cooldown
	}
	return out
}

// ResolvePolicy applies the hierarchy global < tenant < instance.
func ResolvePolicy(global RatePolicy, tenant, inst *RatePolicy) RatePolicy {
	return global.Merge(tenant).Merge(inst)
}

// RetrySchedule is the delay before attempt n+1 (index = attempts so far - 1).
// attempt 1 -> immediate, 2 -> +5s, 3 -> +30s, 4 -> +2m, 5 -> DLQ.
type RetrySchedule []time.Duration

// DefaultRetrySchedule is the documented backoff.
var DefaultRetrySchedule = RetrySchedule{0, 5 * time.Second, 30 * time.Second, 2 * time.Minute}

// MaxAttempts is the total number of attempts before dead-lettering.
func (r RetrySchedule) MaxAttempts() int { return len(r) + 1 }

// Next returns the delay before the next attempt after `attempt` failed
// attempts (1-based), and false when the message must be dead-lettered.
func (r RetrySchedule) Next(attempt int) (time.Duration, bool) {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > len(r) {
		return 0, false
	}
	return r[attempt-1], true
}

// NotifiesTenant reports which statuses are surfaced to the tenant as message.outbound_status events.
func (s Status) NotifiesTenant() bool {
	switch s {
	case StatusAccepted, StatusDelivered, StatusRead, StatusFailed, StatusUnknown:
		return true
	}
	return false
}

// OutboundStatusEvent builds the tenant-facing event for a message that just entered m.Status. The event id is
// deterministic (message id + status), so re-publishing the same fact keeps the same id and consumers can dedupe.
func OutboundStatusEvent(m Message, provider string) events.Event {
	pl := events.MessageOutboundStatusPayload{MessageID: m.ID, Status: string(m.Status), ProviderMessageID: m.ProviderMessageID,
		SequenceNo: m.SequenceNo, ErrorCode: m.ErrorCode}
	if !m.AcceptedAt.IsZero() {
		at := m.AcceptedAt.UTC()
		pl.AcceptedAt = &at
	}
	ev := events.Event{
		EventID:     events.EventIDFor(events.DedupeKey(m.InstanceID, events.MessageOutboundStatus, m.ID, string(m.Status))),
		EventType:   events.MessageOutboundStatus,
		Provider:    provider,
		TenantID:    m.TenantID,
		InstanceID:  m.InstanceID,
		Timestamp:   m.UpdatedAt.UTC(),
		TraceParent: m.TraceParent,
		Payload:     pl,
	}
	if m.NodeID != "" {
		ev.SourceAssignment = &events.SourceAssignment{NodeID: m.NodeID, Epoch: m.AssignmentEpoch}
	}
	return ev
}
