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
		out = append(out, events.Inbound{InstanceID: e.InstanceID, Type: e.Type, ProviderMessageID: e.ProviderMessageID,
			State: e.State, Timestamp: e.Timestamp, Payload: payload})
	}
	return out, nil
}
