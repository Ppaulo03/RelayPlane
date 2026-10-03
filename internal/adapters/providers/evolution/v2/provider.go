package v2

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// Provider implements ports.MessagingProvider on Evolution API v2.
type Provider struct {
	c   *client
	cfg Config
}

var _ ports.MessagingProvider = (*Provider)(nil)

// New builds the provider.
func New(cfg Config) *Provider { return &Provider{c: newClient(cfg), cfg: cfg} }

// ---- Evolution DTOs (never leave this package) ----

type createRequest struct {
	InstanceName string      `json:"instanceName"`
	Integration  string      `json:"integration"`
	QRCode       bool        `json:"qrcode"`
	Webhook      *webhookCfg `json:"webhook,omitempty"`
}

type webhookCfg struct {
	URL      string            `json:"url"`
	ByEvents bool              `json:"byEvents"`
	Base64   bool              `json:"base64"`
	Headers  map[string]string `json:"headers,omitempty"`
	Events   []string          `json:"events"`
}

type createResponse struct {
	Instance struct {
		InstanceName string `json:"instanceName"`
		InstanceID   string `json:"instanceId"`
		Status       string `json:"status"`
	} `json:"instance"`
}

type connectionStateResponse struct {
	Instance struct {
		InstanceName string `json:"instanceName"`
		State        string `json:"state"`
	} `json:"instance"`
}

type connectResponse struct {
	PairingCode string `json:"pairingCode"`
	Code        string `json:"code"`
	Base64      string `json:"base64"`
	Count       int    `json:"count"`
}

type fetchInstance struct {
	Name             string `json:"name"`
	ConnectionStatus string `json:"connectionStatus"`
	OwnerJid         string `json:"ownerJid"`
}

type sendTextRequest struct {
	Number string `json:"number"`
	Text   string `json:"text"`
}

type sendMediaRequest struct {
	Number    string `json:"number"`
	MediaType string `json:"mediatype"`
	MimeType  string `json:"mimetype"`
	Caption   string `json:"caption,omitempty"`
	Media     string `json:"media"`
	FileName  string `json:"fileName,omitempty"`
}

type sendAudioRequest struct {
	Number string `json:"number"`
	Audio  string `json:"audio"`
}

type sendResponse struct {
	Key struct {
		ID string `json:"id"`
	} `json:"key"`
	Status string `json:"status"`
}

var webhookEvents = []string{"QRCODE_UPDATED", "CONNECTION_UPDATE", "MESSAGES_UPSERT", "MESSAGES_UPDATE", "SEND_MESSAGE"}

// WebhookToken is the per-node secret Evolution presents on every webhook.
// Deriving it from the node id means a compromised node cannot speak for another.
func (p *Provider) webhookURL(a ownership.Assignment) string {
	q := url.Values{"node": {a.NodeID}, "epoch": {strconv.FormatInt(a.Epoch, 10)}}
	return strings.TrimRight(p.cfg.WebhookBaseURL, "/") + "/webhooks/" + ProviderKey + "?" + q.Encode()
}

func (p *Provider) CreateInstance(ctx context.Context, req ports.CreateInstanceRequest) (*ports.ProviderInstance, error) {
	a := req.Assignment
	body := createRequest{InstanceName: a.InstanceID, Integration: "WHATSAPP-BAILEYS", QRCode: true}
	if p.cfg.WebhookBaseURL != "" {
		body.Webhook = &webhookCfg{URL: p.webhookURL(a), Events: webhookEvents,
			Headers: map[string]string{TokenHeader: NodeToken(p.cfg.WebhookSecret, a.NodeID)}}
	}
	resp, err := p.c.do(ctx, a.NodeID, "POST", "/instance/create", nil, body, opManage, true)
	if err != nil {
		return nil, err
	}
	var out createResponse
	if err := p.c.decode(resp, &out); err != nil {
		return nil, err
	}
	pid := out.Instance.InstanceID
	if pid == "" {
		pid = a.InstanceID
	}
	return &ports.ProviderInstance{ProviderInstanceID: pid, State: instance.AwaitingPairing}, nil
}

func (p *Provider) DeleteInstance(ctx context.Context, a ownership.Assignment) error {
	_, err := p.c.do(ctx, a.NodeID, "DELETE", "/instance/delete/"+url.PathEscape(a.InstanceID), nil, nil, opManage, true)
	return err
}

func (p *Provider) GetInstanceState(ctx context.Context, a ownership.Assignment) (*ports.InstanceState, error) {
	resp, err := p.c.do(ctx, a.NodeID, "GET", "/instance/connectionState/"+url.PathEscape(a.InstanceID), nil, nil, opManage, true)
	if err != nil {
		return nil, err
	}
	var cs connectionStateResponse
	if err := p.c.decode(resp, &cs); err != nil {
		return nil, err
	}
	st := &ports.InstanceState{Heartbeat: time.Now(), Description: cs.Instance.State}
	switch strings.ToLower(cs.Instance.State) {
	case "open":
		st.State = instance.Connected
		return st, nil
	case "connecting", "close", "closed", "":
		// "connecting"/"close" cover both "never paired" and "lost connection":
		// the owner JID tells them apart.
		paired, err := p.paired(ctx, a)
		if err != nil {
			return nil, err
		}
		switch {
		case !paired:
			st.State = instance.AwaitingPairing
		case strings.ToLower(cs.Instance.State) == "connecting":
			st.State = instance.Reconnecting
		default:
			st.State = instance.Disconnected
		}
		return st, nil
	}
	return nil, fmt.Errorf("%w: unknown connection state %q", errs.ErrProviderRejected, cs.Instance.State)
}

// paired reports whether the session ever completed pairing (has an owner JID).
func (p *Provider) paired(ctx context.Context, a ownership.Assignment) (bool, error) {
	resp, err := p.c.do(ctx, a.NodeID, "GET", "/instance/fetchInstances", url.Values{"instanceName": {a.InstanceID}}, nil, opManage, true)
	if err != nil {
		return false, err
	}
	var list []fetchInstance
	if err := p.c.decode(resp, &list); err != nil {
		return false, err
	}
	for _, i := range list {
		if i.Name == a.InstanceID {
			return i.OwnerJid != "", nil
		}
	}
	return false, fmt.Errorf("%w: %s", errs.ErrInstanceNotFound, a.InstanceID)
}

func (p *Provider) connect(ctx context.Context, a ownership.Assignment) (*connectResponse, error) {
	resp, err := p.c.do(ctx, a.NodeID, "GET", "/instance/connect/"+url.PathEscape(a.InstanceID), nil, nil, opManage, true)
	if err != nil {
		return nil, err
	}
	var out connectResponse
	if err := p.c.decode(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *Provider) GetPairingCode(ctx context.Context, a ownership.Assignment) (*ports.PairingCode, error) {
	out, err := p.connect(ctx, a)
	if err != nil {
		return nil, err
	}
	if out.Base64 == "" && out.Code == "" && out.PairingCode == "" {
		return nil, fmt.Errorf("%w: session is already connected or not waiting for pairing", errs.ErrPairingUnavailable)
	}
	qr := out.Base64
	if qr == "" {
		qr = out.Code
	}
	return &ports.PairingCode{QRCode: qr, PairingCode: out.PairingCode, ExpiresAt: time.Now().Add(45 * time.Second)}, nil
}

func (p *Provider) ConnectInstance(ctx context.Context, a ownership.Assignment) error {
	out, err := p.connect(ctx, a)
	if err != nil {
		return err
	}
	if out.Base64 != "" || out.PairingCode != "" {
		return fmt.Errorf("%w: reconnecting requires scanning a new QR code", errs.ErrPairingUnavailable)
	}
	return nil
}

// Disconnect logs the session out of the node and confirms the socket is
// closed. It is the physical-fencing primitive: it returns nil only when the
// node itself reports the session is no longer connected.
func (p *Provider) Disconnect(ctx context.Context, a ownership.Assignment) error {
	_, err := p.c.do(ctx, a.NodeID, "DELETE", "/instance/logout/"+url.PathEscape(a.InstanceID), nil, nil, opManage, true)
	if err != nil && !errsIs(err, errs.ErrProviderUnavailable) {
		// "not connected" already maps to ErrProviderUnavailable below; a
		// definitive 404 means the node has no such session: nothing to fence.
		return err
	}
	st, serr := p.GetInstanceState(ctx, a)
	if serr != nil {
		return serr
	}
	if st.State == instance.Connected || st.State == instance.Connecting || st.State == instance.Reconnecting {
		return fmt.Errorf("%w: session still %s after logout", errs.ErrProviderUnavailable, st.State)
	}
	return nil
}

func (p *Provider) SendMessage(ctx context.Context, a ownership.Assignment, m messaging.OutboundMessage) (*ports.SendResult, error) {
	var path string
	var body any
	switch m.Type {
	case messaging.TypeText:
		path, body = "/message/sendText/", sendTextRequest{Number: m.To, Text: m.Text}
	case messaging.TypeAudio:
		media, err := p.mediaRef(ctx, m)
		if err != nil {
			return nil, err
		}
		path, body = "/message/sendWhatsAppAudio/", sendAudioRequest{Number: m.To, Audio: media}
	case messaging.TypeImage, messaging.TypeVideo, messaging.TypeDocument:
		media, err := p.mediaRef(ctx, m)
		if err != nil {
			return nil, err
		}
		path, body = "/message/sendMedia/", sendMediaRequest{Number: m.To, MediaType: string(m.Type), MimeType: m.Media.ContentType,
			Caption: m.Caption, Media: media, FileName: m.Filename}
	default:
		return nil, fmt.Errorf("%w: unsupported message type %q", errs.ErrCapabilityMissing, m.Type)
	}
	resp, err := p.c.do(ctx, a.NodeID, "POST", path+url.PathEscape(a.InstanceID), nil, body, opSend, true)
	if err != nil {
		return nil, err
	}
	var out sendResponse
	if err := p.c.decode(resp, &out); err != nil {
		return nil, fmt.Errorf("%w: %v", errs.ErrAmbiguousDispatch, err) // accepted but unreadable
	}
	if out.Key.ID == "" {
		return nil, fmt.Errorf("%w: provider answered without a message id", errs.ErrAmbiguousDispatch)
	}
	return &ports.SendResult{ProviderMessageID: out.Key.ID, Status: messaging.StatusAccepted}, nil
}

func (p *Provider) Capabilities(context.Context) ports.ProviderCapabilities {
	return ports.ProviderCapabilities{QRCode: true, PairingCode: true, Groups: true, Media: true, Templates: false, Presence: true, Disconnect: true}
}

// ProbeNode checks the node's own health and version compatibility. It says
// nothing about individual sessions.
func (p *Provider) ProbeNode(ctx context.Context, nodeID string) (*ports.NodeProbe, error) {
	resp, err := p.c.do(ctx, nodeID, "GET", "/", nil, nil, opManage, false)
	if err != nil {
		return nil, err
	}
	var v struct {
		Status  int    `json:"status"`
		Version string `json:"version"`
	}
	if err := p.c.decode(resp, &v); err != nil {
		return nil, err
	}
	probe := &ports.NodeProbe{Version: v.Version, Ready: true}
	if !Compatible(v.Version) {
		probe.Ready = false
		return probe, fmt.Errorf("%w: evolution %q is not a supported v%s release", errs.ErrProviderRejected, v.Version, SupportedMajor)
	}
	return probe, nil
}

// Compatible checks the adapter/Evolution version pairing (compatibility check).
func Compatible(version string) bool { return strings.HasPrefix(version, SupportedMajor+".") }

// mediaRef returns the media argument Evolution expects: the short-lived signed
// URL when available, otherwise the streamed content as base64 *inside this
// HTTP call to the node* (never through the broker).
func (p *Provider) mediaRef(ctx context.Context, m messaging.OutboundMessage) (string, error) {
	if m.Media == nil {
		return "", fmt.Errorf("%w: media message without attachment", errs.ErrProviderRejected)
	}
	if m.Media.URL != "" {
		return m.Media.URL, nil
	}
	if m.Media.Open == nil {
		return "", fmt.Errorf("%w: attachment has neither URL nor reader", errs.ErrProviderRejected)
	}
	r, err := m.Media.Open(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: open attachment: %v", errs.ErrProviderUnavailable, err)
	}
	defer r.Close()
	return encodeBase64(r)
}
