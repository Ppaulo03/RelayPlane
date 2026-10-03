// Package ports declares the contracts between the core/application layer and
// infrastructure adapters. Dependencies point: adapters -> ports <- core.
package ports

import (
	"context"
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

	// ConnectInstance asks the owner to (re)open the session.
	ConnectInstance(ctx context.Context, assignment ownership.Assignment) error
	// Disconnect closes the session. It MUST NOT return nil until the socket
	// is confirmed closed: it is the physical-fencing primitive.
	Disconnect(ctx context.Context, assignment ownership.Assignment) error
	// ProbeNode checks a provider node's own health (not any instance's).
	ProbeNode(ctx context.Context, nodeID string) (*NodeProbe, error)
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
