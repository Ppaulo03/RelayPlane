// Package events defines the canonical event model published on the EventBus.
//
// Downstream consumers only ever see these types; raw provider payloads never
// leave the provider adapter.
package events

import (
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
	EventID    string    `json:"event_id"`
	EventType  Type      `json:"event_type"`
	Provider   string    `json:"provider"`
	TenantID   string    `json:"tenant_id"`
	InstanceID string    `json:"instance_id"`
	Timestamp  time.Time `json:"timestamp"`
	// SourceAssignment identifies the owner that produced the event (nil only for events emitted
	// before this field existed; consumers then fall back to the current assignment).
	SourceAssignment *SourceAssignment `json:"source_assignment,omitempty"`
	// TraceParent is the W3C trace context of the request that caused the event, when there is one
	// (outbound status events carry the trace of the send); webhooks forward it as the traceparent header.
	TraceParent string `json:"traceparent,omitempty"`
	Payload     any    `json:"payload"`
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
