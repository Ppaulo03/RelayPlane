// Package events defines the canonical event model published on the EventBus.
//
// Downstream consumers only ever see these types; raw provider payloads never
// leave the provider adapter.
package events

import (
	"crypto/sha256"
	"encoding/hex"
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
)

// Event is the canonical envelope.
type Event struct {
	EventID    string    `json:"event_id"`
	EventType  Type      `json:"event_type"`
	Provider   string    `json:"provider"`
	TenantID   string    `json:"tenant_id"`
	InstanceID string    `json:"instance_id"`
	Timestamp  time.Time `json:"timestamp"`
	Payload    any       `json:"payload"`
}

// MessageReceivedPayload is the payload of message.received.
type MessageReceivedPayload struct {
	ProviderMessageID string `json:"provider_message_id"`
	From              string `json:"from"`
	PushName          string `json:"push_name,omitempty"`
	Type              string `json:"type"`
	Text              string `json:"text,omitempty"`
	Group             bool   `json:"group,omitempty"`
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
