// Package events defines the canonical event model published on the EventBus.
//
// Downstream consumers only ever see these types; raw provider payloads never
// leave the provider adapter.
package events

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

// Type is the canonical event type.
type Type string

const (
	MessageReceived       Type = "message.received"
	MessageStatus         Type = "message.status"
	InstanceStatusChanged Type = "instance.status_changed"
	InstanceQRCodeUpdated Type = "instance.qrcode_updated"
	OwnershipViolation    Type = "ownership.violation"
	// MessageOutboundStatus reports the lifecycle of a message the TENANT sent through RelayPlane
	// (ACCEPTED, DELIVERED, READ, FAILED, UNKNOWN), keyed by the RelayPlane message id.
	MessageOutboundStatus Type = "message.outbound_status"
	// MessageDeleted reports that the SENDER revoked a message ("delete for everyone"): whatever it said no longer stands.
	MessageDeleted Type = "message.deleted"
)

// SourceAssignment is the assignment under which the provider produced an event.
// It is captured when the event is accepted (after ownership validation) so that a
// late consumer can tell "this happened to the previous owner" from "this happened
// to the current one": the projector applies the event under THIS epoch, never
// under whatever epoch the catalog has by the time the event is consumed.
type SourceAssignment struct {
	NodeID string `json:"node_id"`
	Epoch  int64  `json:"epoch"`
}

// Event is the canonical envelope.
type Event struct {
	EventID   string `json:"event_id"`
	EventType Type   `json:"event_type"`
	// Provider is INTERNAL (the adapter that produced the event): a consumer is told the CHANNEL, never the provider (see Channel).
	Provider   string    `json:"provider,omitempty"`
	TenantID   string    `json:"tenant_id"`
	InstanceID string    `json:"instance_id"`
	Timestamp  time.Time `json:"timestamp"`
	// Channel is what a tenant is told instead of the provider ("whatsapp"). It exists only on the copy POSTed to a tenant's webhook: it is
	// derived from Provider there and never stored.
	Channel string `json:"channel,omitempty"`
	// SourceAssignment is INTERNAL (it never reaches a tenant): it identifies the owner that produced the event (nil only for events emitted
	// before this field existed; consumers then fall back to the current assignment).
	SourceAssignment *SourceAssignment `json:"source_assignment,omitempty"`
	// TraceParent is the W3C trace context of the request that caused the event, when there is one
	// (outbound status events carry the trace of the send); webhooks forward it as the traceparent header.
	TraceParent string `json:"traceparent,omitempty"`
	Payload     any    `json:"payload"`
	// SchemaVersion and Sequence are set only on the copy that is POSTed to a tenant's webhook (see SchemaVersion).
	// Sequence numbers the deliveries of ONE subscription for ONE instance 1, 2, 3, ... without gaps, so a consumer can
	// reorder what a retry reordered and notice what it never received (a delivery that went to the DLQ leaves a gap).
	SchemaVersion int   `json:"schema_version,omitempty"`
	Sequence      int64 `json:"sequence,omitempty"`
	// ObservedAt is INTERNAL (never sent to a tenant): when RelayPlane learned of the fact, the start of the delivery lag
	// measured by relayplane_event_delivery_lag_seconds. Events that carry the provider's own time in Timestamp
	// (message.received) need it; for the others Timestamp is already that moment.
	ObservedAt *time.Time `json:"observed_at,omitempty"`
	// AcceptedAt is INTERNAL (never sent to a tenant): when RelayPlane durably accepted an inbound message. Unlike ObservedAt it is never
	// moved (a message with an attachment is "observed" again when the download ends), so it can answer one question: was this message
	// accepted BEFORE its sender asked to be erased? (docs/OPERATIONS.md, erasure)
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`
}

// ErasureSubject is what an erasure tombstone stores in place of a phone number: an HMAC of it with a server-side key, so the table alone
// cannot be turned back into numbers by trying them all (a phone number has little entropy; a plain hash would not protect it).
func ErasureSubject(key []byte, number string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("relayplane/contact-erasure/v1|" + number))
	return hex.EncodeToString(m.Sum(nil))
}

// ErasureSubject is the tombstone subject of the contact this event is about, or "" when it is about nobody.
func (e Event) ErasureSubject(key []byte) string {
	n := e.ContactNumber()
	if n == "" {
		return ""
	}
	return ErasureSubject(key, n)
}

// ContactNumber is the phone number an inbound event is about (the subject of an erasure request): the author of a message.received or of a
// message.deleted (both carry the person's number in payload.from); "" for any other event.
func (e Event) ContactNumber() string {
	if e.EventType != MessageReceived && e.EventType != MessageDeleted {
		return ""
	}
	raw, err := json.Marshal(e.Payload)
	if err != nil {
		return ""
	}
	var probe struct {
		From string `json:"from"`
	}
	_ = json.Unmarshal(raw, &probe)
	return probe.From
}

// LagOrigin is the moment from which the delivery lag of an event is counted: when RelayPlane observed it.
func (e Event) LagOrigin(fallback time.Time) time.Time {
	switch {
	case e.ObservedAt != nil:
		return *e.ObservedAt
	case e.EventType == MessageReceived || e.EventType == MessageStatus || e.EventType == MessageDeleted:
		return fallback // Timestamp is the provider's: it says nothing about when WE learned of it
	case !e.Timestamp.IsZero():
		return e.Timestamp
	}
	return fallback
}

// SchemaVersion is the version of the tenant-facing event envelope and payloads (docs/events/*.json). It changes only
// for incompatible changes; adding an optional field does not.
const SchemaVersion = 2

// ForTenant is the copy of the event that is POSTed to a tenant's webhook: the contract's version and the delivery's sequence number, the
// CHANNEL instead of the provider, and none of the internal fields (the owner that produced it, when it was observed or accepted).
// Everything that builds the public envelope goes through here, so the schema tests and the dispatcher cannot drift apart.
func (e Event) ForTenant(sequence int64) Event {
	e.SchemaVersion, e.Sequence = SchemaVersion, sequence
	e.Channel, e.Provider = ChannelOf(e.Provider), ""
	e.SourceAssignment, e.ObservedAt, e.AcceptedAt = nil, nil, nil
	return e
}

// ChannelOf is the public name of the channel a provider serves. The provider (and the node, and the epoch) are not part of the contract: a
// consumer must not need to know how a channel is implemented, and the same channel can be served by another provider tomorrow.
func ChannelOf(provider string) string {
	switch provider {
	case "evolution-v2":
		return "whatsapp"
	}
	return "other"
}

// IsGroupMessage reports whether ev is a message.received from a group chat. The payload is a typed struct when the
// event was just produced and a decoded JSON object after it crossed the bus, so both shapes are handled.
func IsGroupMessage(ev Event) bool {
	if ev.EventType != MessageReceived {
		return false
	}
	switch p := ev.Payload.(type) {
	case MessageReceivedPayload:
		return p.Group
	case *MessageReceivedPayload:
		return p != nil && p.Group
	case map[string]any:
		g, _ := p["group"].(bool)
		return g
	case nil:
		return false
	}
	// any other representation (json.RawMessage, []byte, a provider-specific struct): look at its JSON form
	raw, err := json.Marshal(ev.Payload)
	if err != nil {
		return false
	}
	var probe struct {
		Group bool `json:"group"`
	}
	return json.Unmarshal(raw, &probe) == nil && probe.Group
}

// MessageReceivedPayload is the payload of message.received. The envelope timestamp of this event is the
// time the PROVIDER stamped the message (when the user wrote it), not when RelayPlane received it.
type MessageReceivedPayload struct {
	ProviderMessageID string `json:"provider_message_id"`
	// ReplyToProviderMessageID is the provider id of the message this one quotes/replies to ("" when it is
	// not a reply). It is the strongest evidence that an answer refers to a specific earlier message of ours.
	ReplyToProviderMessageID string `json:"reply_to_provider_message_id,omitempty"`
	From                     string `json:"from"`
	PushName                 string `json:"push_name,omitempty"`
	Type                     string `json:"type"`
	Text                     string `json:"text,omitempty"`
	Group                    bool   `json:"group,omitempty"`
	// ChatID is the group JID (<id>@g.us) of a group message; reply to it to answer the group.
	ChatID string `json:"chat_id,omitempty"`
	// SenderLID is the opaque WhatsApp LID of the sender when the provider addressed them that way. From carries the
	// phone number whenever the provider reported it.
	SenderLID string `json:"sender_lid,omitempty"`
	// Media is set when the message carries an attachment (image, audio, video, document, sticker).
	Media *MessageMedia `json:"media,omitempty"`
}

// Media outcomes reported to the tenant in message.received.
const (
	MediaReady    = "READY"    // downloaded and stored: GET /api/v1/media/{media_id}/content
	MediaRejected = "REJECTED" // not downloaded on purpose (reason: too_large, type_not_allowed, unsupported)
	MediaFailed   = "FAILED"   // the provider could not deliver the bytes (reason: expired, download_failed)
)

// MessageMedia describes the attachment of an inbound message. The adapter fills what the sender's client declared
// (kind, mime type, size, filename, seconds); the control plane adds media_id, status and reason, and the sizes are
// replaced by the real ones once the bytes are stored.
type MessageMedia struct {
	MediaID  string `json:"media_id,omitempty"`
	Status   string `json:"status,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Kind     string `json:"kind"`
	MimeType string `json:"mime_type,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Filename string `json:"filename,omitempty"`
	Seconds  int    `json:"seconds,omitempty"`
}

// InboundMedia is what a provider's normalizer reports about an attachment: the declared description and an opaque,
// provider-specific reference that only the same provider's DownloadMedia understands (it can hold decryption keys, so
// it never leaves the control plane).
type InboundMedia struct {
	Media MessageMedia
	Ref   json.RawMessage
}

// MessageDeletedPayload is the payload of message.deleted. ProviderMessageID is the id of the revoked message, as it was
// delivered in message.received.
type MessageDeletedPayload struct {
	ProviderMessageID string `json:"provider_message_id"`
	From              string `json:"from,omitempty"`
	Group             bool   `json:"group,omitempty"`
	ChatID            string `json:"chat_id,omitempty"`
}

// MessageOutboundStatusPayload is the payload of message.outbound_status.
type MessageOutboundStatusPayload struct {
	MessageID         string `json:"message_id"`
	Status            string `json:"status"` // ACCEPTED | DELIVERED | READ | FAILED | UNKNOWN
	ProviderMessageID string `json:"provider_message_id,omitempty"`
	SequenceNo        int64  `json:"sequence_no"`
	// AcceptedAt is when the provider accepted the send (set from ACCEPTED on).
	AcceptedAt *time.Time `json:"accepted_at,omitempty"`
	ErrorCode  string     `json:"error_code,omitempty"`
}

// MessageStatusPayload is the payload of message.status.
type MessageStatusPayload struct {
	ProviderMessageID string `json:"provider_message_id"`
	Status            string `json:"status"` // sent | delivered | read | failed
}

// InstanceStatusChangedPayload is the payload of instance.status_changed.
type InstanceStatusChangedPayload struct {
	State  string `json:"state"` // an instance.ObservedState value
	Reason string `json:"reason,omitempty"`
}

// QRCodeUpdatedPayload is the payload of instance.qrcode_updated.
type QRCodeUpdatedPayload struct {
	HasQRCode   bool   `json:"has_qrcode"`
	PairingCode string `json:"pairing_code,omitempty"`
}

// Inbound is a provider-neutral event produced by a provider's webhook
// normalizer, before the control plane attaches tenant and identity.
type Inbound struct {
	InstanceID        string // RelayPlane instance id (resolved by the adapter)
	Type              Type
	ProviderMessageID string
	State             string // sub-state: sent/delivered/read, connection state, ...
	Timestamp         time.Time
	Payload           any
	// Media is set for a message.received whose message has an attachment; the payload then carries the same
	// description (MessageMedia) and the control plane resolves the bytes before the event is delivered.
	Media *InboundMedia
}

// DedupeKey is the canonical dedupe key: instance + event type + provider
// message id + event state. Different states (sent/delivered/read) never
// collapse into each other.
func DedupeKey(instanceID string, t Type, providerMessageID, state string) string {
	return strings.Join([]string{instanceID, string(t), providerMessageID, state}, "|")
}

// EventIDFor derives a deterministic event id from the dedupe key so that
// at-least-once re-publication of the same fact keeps the same id and
// consumers can dedupe on it.
func EventIDFor(dedupeKey string) string {
	sum := sha256.Sum256([]byte(dedupeKey))
	return "evt_" + hex.EncodeToString(sum[:])[:32]
}
