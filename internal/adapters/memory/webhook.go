package memory

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/ports"
)

// FakeWebhook is a trivial WebhookAdapter for tests: the body is JSON
// {"node","epoch","token","events":[...]}; Authenticate checks the token.
type FakeWebhook struct {
	ProviderKey string
	Token       string
}

// FakeWebhookBody is the wire format understood by FakeWebhook.
type FakeWebhookBody struct {
	Node   string          `json:"node"`
	Epoch  int64           `json:"epoch"`
	Token  string          `json:"token"`
	Events []FakeWebhookEv `json:"events"`
}

// FakeWebhookEv is one provider event in FakeWebhookBody.
type FakeWebhookEv struct {
	InstanceID        string          `json:"instance_id"`
	Type              events.Type     `json:"type"`
	ProviderMessageID string          `json:"provider_message_id"`
	State             string          `json:"state"`
	Timestamp         time.Time       `json:"timestamp"`
	Payload           json.RawMessage `json:"payload"`
	// Media makes a message.received an attachment-bearing message: the description goes into the payload, Ref is the
	// provider-side download reference (see FakeMediaRef).
	Media *FakeWebhookMedia `json:"media,omitempty"`
}

// FakeWebhookMedia is the attachment of a FakeWebhookEv.
type FakeWebhookMedia struct {
	Kind     string          `json:"kind"`
	MimeType string          `json:"mime_type"`
	Size     int64           `json:"size"`
	Filename string          `json:"filename"`
	Ref      json.RawMessage `json:"ref"`
}

func (f FakeWebhook) Provider() string { return f.ProviderKey }

func (f FakeWebhook) parse(r ports.InboundRequest) (FakeWebhookBody, error) {
	var b FakeWebhookBody
	return b, json.Unmarshal(r.Body, &b)
}

func (f FakeWebhook) Authenticate(r ports.InboundRequest) (ports.NodeClaim, error) {
	b, err := f.parse(r)
	if err != nil {
		return ports.NodeClaim{}, err
	}
	if b.Token != f.Token {
		return ports.NodeClaim{}, errors.New("bad token")
	}
	return ports.NodeClaim{NodeID: b.Node, Epoch: b.Epoch}, nil
}

func (f FakeWebhook) Normalize(r ports.InboundRequest) ([]events.Inbound, error) {
	b, err := f.parse(r)
	if err != nil {
		return nil, err
	}
	var out []events.Inbound
	for _, e := range b.Events {
		var payload any
		if len(e.Payload) > 0 {
			payload = e.Payload
		}
		in := events.Inbound{InstanceID: e.InstanceID, Type: e.Type, ProviderMessageID: e.ProviderMessageID,
			State: e.State, Timestamp: e.Timestamp, Payload: payload}
		if e.Media != nil && e.Type == events.MessageReceived {
			var pl events.MessageReceivedPayload
			_ = json.Unmarshal(e.Payload, &pl)
			desc := events.MessageMedia{Kind: e.Media.Kind, MimeType: e.Media.MimeType, Size: e.Media.Size, Filename: e.Media.Filename}
			pl.Media = &desc
			in.Payload, in.Media = pl, &events.InboundMedia{Media: desc, Ref: e.Media.Ref}
		}
		out = append(out, in)
	}
	return out, nil
}
