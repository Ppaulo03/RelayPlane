// Package ports declares the contracts between the core/application layer and
// infrastructure adapters. Dependencies point: adapters -> ports <- core.
package ports

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
)

// CreateInstanceRequest asks a provider node to create a session.
type CreateInstanceRequest struct {
	Assignment ownership.Assignment // node + epoch the session is created under
	TenantID   string
	Name       string
}

// ProviderInstance is the provider's confirmation of a created session.
type ProviderInstance struct {
	ProviderInstanceID string
	State              instance.ObservedState
}

// InstanceState is the provider's view of a session.
type InstanceState struct {
	State       instance.ObservedState
	Heartbeat   time.Time // last time the provider confirmed the session
	Description string
}

// PairingCode carries pairing material. Secrets in it must never be logged.
type PairingCode struct {
	QRCode      string // raw QR payload / data URL, provider-neutral
	PairingCode string
	ExpiresAt   time.Time
}

// SendResult mirrors messaging.SendResult for the port surface.
type SendResult = messaging.SendResult

// ProviderCapabilities declares what a provider supports.
type ProviderCapabilities struct {
	QRCode      bool `json:"qr_code"`
	PairingCode bool `json:"pairing_code"`
	Groups      bool `json:"groups"`
	Media       bool `json:"media"`
	Templates   bool `json:"templates"`
	Presence    bool `json:"presence"`
	Disconnect  bool `json:"disconnect"` // supports confirmed physical fencing
}

// NodeProbe is the result of a node health probe.
type NodeProbe struct {
	Ready   bool
	Version string
}

// MessagingProvider is the canonical provider contract. Implementations live
// exclusively under internal/adapters/providers and must translate every
// provider-specific type and error into the canonical ones.
type MessagingProvider interface {
	CreateInstance(ctx context.Context, req CreateInstanceRequest) (*ProviderInstance, error)
	DeleteInstance(ctx context.Context, assignment ownership.Assignment) error
	GetInstanceState(ctx context.Context, assignment ownership.Assignment) (*InstanceState, error)
	GetPairingCode(ctx context.Context, assignment ownership.Assignment) (*PairingCode, error)
	SendMessage(ctx context.Context, assignment ownership.Assignment, msg messaging.OutboundMessage) (*SendResult, error)
	Capabilities(ctx context.Context) ProviderCapabilities

	// LookupInstance returns the provider's own identifier and state of an existing session for
	// this assignment (ErrInstanceNotFound otherwise). Adoption after a crash uses it so the
	// core never assumes that the provider's id equals RelayPlane's instance id.
	LookupInstance(ctx context.Context, assignment ownership.Assignment) (*ProviderInstance, error)
	// ConnectInstance asks the owner to (re)open the session.
	ConnectInstance(ctx context.Context, assignment ownership.Assignment) error
	// Disconnect closes the session. It MUST NOT return nil until the socket
	// is confirmed closed: it is the physical-fencing primitive.
	Disconnect(ctx context.Context, assignment ownership.Assignment) error
	// ProbeNode checks a provider node's own health (not any instance's).
	ProbeNode(ctx context.Context, nodeID string) (*NodeProbe, error)
}

// DownloadedMedia is an attachment fetched from the provider. Body is bounded by the caller's limit.
type DownloadedMedia struct {
	Body        io.ReadCloser
	Size        int64
	ContentType string
	Filename    string
}

// MediaDownloader is implemented by providers that can hand over the bytes of an inbound attachment. ref is the opaque
// reference the same provider's normalizer produced (events.InboundMedia.Ref). Errors: ErrPayloadTooLarge when the content
// exceeds maxBytes, ErrNotFound when the provider no longer has it (WhatsApp keeps media for a limited time),
// ErrProviderUnavailable for anything worth retrying.
type MediaDownloader interface {
	DownloadMedia(ctx context.Context, assignment ownership.Assignment, ref json.RawMessage, maxBytes int64) (*DownloadedMedia, error)
}

// PresenceState is what the account shows to a contact.
type PresenceState string

const (
	PresenceComposing PresenceState = "composing" // "typing…"
	PresenceRecording PresenceState = "recording" // "recording audio…"
	PresencePaused    PresenceState = "paused"    // stopped typing
)

// Valid reports whether s is a state RelayPlane exposes.
func (s PresenceState) Valid() bool {
	return s == PresenceComposing || s == PresenceRecording || s == PresencePaused
}

// PresenceSender is implemented by providers that can show "typing…" / "recording…" to a contact. The provider holds the
// state for `duration` and then pauses on its own, so a forgotten "composing" never stays on.
type PresenceSender interface {
	SendPresence(ctx context.Context, assignment ownership.Assignment, to string, state PresenceState, duration time.Duration) error
}

// ReadMarker is implemented by providers that can mark received messages as read (the blue ticks on the contact's side).
type ReadMarker interface {
	MarkRead(ctx context.Context, assignment ownership.Assignment, chat string, providerMessageIDs []string) error
}

// InboundRequest is a raw webhook request handed to a provider's adapter.
type InboundRequest struct {
	Header http.Header
	Query  url.Values
	Body   []byte
}

// NodeClaim is the origin a webhook claims. It is *not* trusted until it has
// been authenticated and validated against the catalog.
type NodeClaim struct {
	NodeID string
	Epoch  int64
}

// WebhookAdapter authenticates and normalizes a provider's inbound webhooks.
type WebhookAdapter interface {
	// Provider returns the provider key this adapter handles ("evolution-v2").
	Provider() string
	// Authenticate verifies the provider credentials and returns the claim.
	Authenticate(r InboundRequest) (NodeClaim, error)
	// Normalize converts the provider payload into canonical inbound events.
	Normalize(r InboundRequest) ([]events.Inbound, error)
}
