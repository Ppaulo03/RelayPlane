package v2

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/ports"
)

// TokenHeader carries the per-node webhook token configured at instance creation.
const TokenHeader = "X-RelayPlane-Token"

// NodeToken derives the webhook token of a node.
func NodeToken(secret, nodeID string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte("evolution-v2/webhook/" + nodeID))
	return hex.EncodeToString(m.Sum(nil))
}

func errsIs(err, target error) bool { return errors.Is(err, target) }

func encodeBase64(r io.Reader) (string, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("read attachment: %w", err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// Webhook is the ports.WebhookAdapter of Evolution v2.
type Webhook struct {
	Secret string
	Now    func() time.Time
}

var _ ports.WebhookAdapter = Webhook{}

// Provider implements ports.WebhookAdapter.
func (Webhook) Provider() string { return ProviderKey }

// Authenticate verifies the node token (constant time) and returns the claimed
// origin taken from the callback URL. The claim is untrusted until the
// application validates it against the catalog.
func (w Webhook) Authenticate(r ports.InboundRequest) (ports.NodeClaim, error) {
	node := r.Query.Get("node")
	if node == "" {
		return ports.NodeClaim{}, errors.New("missing node")
	}
	got := r.Header.Get(TokenHeader)
	want := NodeToken(w.Secret, node)
	if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return ports.NodeClaim{}, errors.New("invalid webhook token")
	}
	epoch, _ := strconv.ParseInt(r.Query.Get("epoch"), 10, 64)
	return ports.NodeClaim{NodeID: node, Epoch: epoch}, nil
}

type envelope struct {
	Event    string          `json:"event"`
	Instance string          `json:"instance"`
	Data     json.RawMessage `json:"data"`
	DateTime string          `json:"date_time"`
}

type upsertData struct {
	Key struct {
		RemoteJid   string `json:"remoteJid"`
		FromMe      bool   `json:"fromMe"`
		ID          string `json:"id"`
		Participant string `json:"participant"`
		// Recent WhatsApp versions address people by an opaque LID (<digits>@lid); the phone JID travels in the *Alt fields
		// (observed against a real node, addressingMode "lid").
		RemoteJidAlt   string `json:"remoteJidAlt"`
		ParticipantAlt string `json:"participantAlt"`
	} `json:"key"`
	PushName         string         `json:"pushName"`
	MessageType      string         `json:"messageType"`
	Message          map[string]any `json:"message"`
	MessageTimestamp json.Number    `json:"messageTimestamp"`
	ContextInfo      *struct {
		StanzaID string `json:"stanzaId"`
	} `json:"contextInfo"`
}

type deleteData struct {
	ID             string `json:"id"`
	RemoteJid      string `json:"remoteJid"`
	RemoteJidAlt   string `json:"remoteJidAlt"`
	Participant    string `json:"participant"`
	ParticipantAlt string `json:"participantAlt"`
	FromMe         bool   `json:"fromMe"`
}

type updateData struct {
	KeyID     string `json:"keyId"`
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
}

type connectionData struct {
	State        string `json:"state"`
	StatusReason int    `json:"statusReason"`
}

// Normalize is the anti-corruption layer: Evolution payload -> canonical events.
func (w Webhook) Normalize(r ports.InboundRequest) ([]events.Inbound, error) {
	var env envelope
	if err := json.Unmarshal(r.Body, &env); err != nil {
		return nil, fmt.Errorf("invalid json: %w", err)
	}
	if env.Instance == "" {
		return nil, errors.New("missing instance")
	}
	at := w.eventTime(env.DateTime)
	switch strings.ToLower(strings.ReplaceAll(env.Event, "_", ".")) {
	case "messages.upsert":
		var items []upsertData
		if err := unmarshalOneOrMany(env.Data, &items); err != nil {
			return nil, err
		}
		var out []events.Inbound
		for _, d := range items {
			if d.Key.FromMe || d.Key.ID == "" {
				continue // our own sends are reported through delivery receipts
			}
			ts := at
			if n, err := d.MessageTimestamp.Int64(); err == nil && n > 0 {
				ts = time.Unix(n, 0).UTC()
			}
			group := strings.HasSuffix(d.Key.RemoteJid, "@g.us")
			senderJid, senderAlt := d.Key.RemoteJid, d.Key.RemoteJidAlt
			if group {
				senderJid, senderAlt = d.Key.Participant, d.Key.ParticipantAlt
			}
			from, senderLID := senderNumber(senderJid, senderAlt)
			chatID := ""
			if group {
				chatID = d.Key.RemoteJid
			}
			typ, text := messageContent(d)
			inMedia := mediaOf(d)
			var payloadMedia *events.MessageMedia
			if inMedia != nil {
				m := inMedia.Media
				payloadMedia = &m
			}
			out = append(out, events.Inbound{InstanceID: env.Instance, Type: events.MessageReceived, ProviderMessageID: d.Key.ID, Timestamp: ts, Media: inMedia,
				Payload: events.MessageReceivedPayload{ProviderMessageID: d.Key.ID, ReplyToProviderMessageID: replyTo(d), From: from, PushName: d.PushName,
					Type: typ, Text: text, Group: group, ChatID: chatID, SenderLID: senderLID, Media: payloadMedia}})
		}
		return out, nil

	case "messages.update":
		var items []updateData
		if err := unmarshalOneOrMany(env.Data, &items); err != nil {
			return nil, err
		}
		var out []events.Inbound
		for _, d := range items {
			id := d.KeyID
			if id == "" {
				id = d.MessageID
			}
			status := receiptStatus(d.Status)
			if id == "" || status == "" {
				continue
			}
			out = append(out, events.Inbound{InstanceID: env.Instance, Type: events.MessageStatus, ProviderMessageID: id, State: status, Timestamp: at,
				Payload: events.MessageStatusPayload{ProviderMessageID: id, Status: status}})
		}
		return out, nil

	case "messages.delete":
		// a revoked message: {key fields..., "status":"DELETED"} (observed). The companion "messages.edited" with type REVOKE
		// carries no extra information and is not subscribed.
		var items []deleteData
		if err := unmarshalOneOrMany(env.Data, &items); err != nil {
			return nil, err
		}
		var out []events.Inbound
		for _, d := range items {
			if d.ID == "" || d.FromMe {
				continue
			}
			group := strings.HasSuffix(d.RemoteJid, "@g.us")
			senderJid, senderAlt := d.RemoteJid, d.RemoteJidAlt
			chatID := ""
			if group {
				senderJid, senderAlt, chatID = d.Participant, d.ParticipantAlt, d.RemoteJid
			}
			from, _ := senderNumber(senderJid, senderAlt)
			out = append(out, events.Inbound{InstanceID: env.Instance, Type: events.MessageDeleted, ProviderMessageID: d.ID, Timestamp: at,
				Payload: events.MessageDeletedPayload{ProviderMessageID: d.ID, From: from, Group: group, ChatID: chatID}})
		}
		return out, nil

	case "connection.update":
		var d connectionData
		if err := json.Unmarshal(env.Data, &d); err != nil {
			return nil, err
		}
		state := connectionState(d)
		if state == "" {
			return nil, nil
		}
		// the timestamp makes each transition a distinct fact (CONNECTED may recur)
		return []events.Inbound{{InstanceID: env.Instance, Type: events.InstanceStatusChanged,
			ProviderMessageID: strconv.FormatInt(at.UnixMilli(), 10), State: state, Timestamp: at,
			Payload: events.InstanceStatusChangedPayload{State: state, Reason: "provider connection.update"}}}, nil

	case "qrcode.updated":
		var d struct {
			QRCode struct {
				PairingCode string `json:"pairingCode"`
				Base64      string `json:"base64"`
				Code        string `json:"code"`
			} `json:"qrcode"`
		}
		_ = json.Unmarshal(env.Data, &d)
		// never forward QR material in events: it is a credential
		return []events.Inbound{{InstanceID: env.Instance, Type: events.InstanceQRCodeUpdated,
			ProviderMessageID: strconv.FormatInt(at.UnixMilli(), 10), Timestamp: at,
			Payload: events.QRCodeUpdatedPayload{HasQRCode: d.QRCode.Base64 != "" || d.QRCode.Code != "", PairingCode: ""}}}, nil
	}
	return nil, nil // events RelayPlane does not model are dropped, not leaked
}

// eventTime is the time RelayPlane stamps on provider events that carry no timestamp of their own (connection, receipt,
// QR). It is the ARRIVAL time: the envelope's date_time cannot be trusted. Observed against a real node, Evolution
// formats it in the container's local time and labels it "Z" (a node with TZ=America/Sao_Paulo reports 20:08Z at 23:08Z),
// which made the projector discard a real logout as an event older than the catalog. Nodes also run with TZ=UTC, but the
// adapter must not depend on that. A message's own time comes from messageTimestamp (epoch seconds), which is correct.
func (w Webhook) eventTime(string) time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}

func unmarshalOneOrMany[T any](raw json.RawMessage, out *[]T) error {
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if raw[0] == '[' {
		return json.Unmarshal(raw, out)
	}
	var one T
	if err := json.Unmarshal(raw, &one); err != nil {
		return err
	}
	*out = []T{one}
	return nil
}

// senderNumber returns the sender's phone number and, when the provider addressed the sender by LID, that LID. The phone
// number comes from the alternate JID when the primary one is a LID; if no phone is known the LID digits are returned as
// the number (they identify the sender but are not dialable).
func senderNumber(jid, alt string) (number, lid string) {
	if strings.HasSuffix(jid, "@lid") {
		lid = jidToNumber(jid)
		if alt != "" && !strings.HasSuffix(alt, "@lid") {
			return jidToNumber(alt), lid
		}
		return lid, lid
	}
	return jidToNumber(jid), ""
}

func jidToNumber(jid string) string {
	if i := strings.IndexByte(jid, '@'); i >= 0 {
		jid = jid[:i]
	}
	if i := strings.IndexByte(jid, ':'); i >= 0 {
		jid = jid[:i]
	}
	return jid
}

// replyTo returns the provider id of the message being quoted. Evolution reports it either at the top level of
// the data (contextInfo) or inside the typed message body (extendedTextMessage, imageMessage, ...).
func replyTo(d upsertData) string {
	if d.ContextInfo != nil && d.ContextInfo.StanzaID != "" {
		return d.ContextInfo.StanzaID
	}
	for _, body := range d.Message {
		m, ok := body.(map[string]any)
		if !ok {
			continue
		}
		if ci, ok := m["contextInfo"].(map[string]any); ok {
			if id, _ := ci["stanzaId"].(string); id != "" {
				return id
			}
		}
	}
	return ""
}

func messageContent(d upsertData) (typ, text string) {
	str := func(m map[string]any, k string) string { s, _ := m[k].(string); return s }
	switch d.MessageType {
	case "conversation":
		return "text", str(d.Message, "conversation")
	case "extendedTextMessage":
		if ext, ok := d.Message["extendedTextMessage"].(map[string]any); ok {
			return "text", str(ext, "text")
		}
		return "text", ""
	case "imageMessage", "videoMessage", "documentMessage", "audioMessage", "stickerMessage":
		kind := strings.TrimSuffix(d.MessageType, "Message")
		if m, ok := d.Message[d.MessageType].(map[string]any); ok {
			return kind, str(m, "caption")
		}
		return kind, ""
	case "":
		return "unknown", ""
	}
	return strings.TrimSuffix(d.MessageType, "Message"), ""
}

func receiptStatus(s string) string {
	switch strings.ToUpper(s) {
	case "SERVER_ACK", "SENT":
		return "sent"
	case "DELIVERY_ACK", "DELIVERED":
		return "delivered"
	case "READ", "PLAYED":
		return "read"
	case "ERROR", "FAILED":
		return "failed"
	}
	return ""
}

func connectionState(d connectionData) string {
	switch strings.ToLower(d.State) {
	case "open":
		return "CONNECTED"
	case "connecting":
		return "CONNECTING"
	case "close", "closed":
		if d.StatusReason == 401 { // logged out from the phone
			return "LOGGED_OUT"
		}
		return "DISCONNECTED"
	}
	return ""
}
