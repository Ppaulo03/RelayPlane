package v2_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/providers/evolution/v2"
	"github.com/relayplane/relayplane/internal/contracttest"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
	"github.com/relayplane/relayplane/internal/simulator"
)

const simKey = "sim-key"

// The simulator is only worth anything if the REAL adapter treats it exactly like a node: it must pass the same provider
// contract suite as every other implementation.
func TestSimulatorPassesProviderContractSuite(t *testing.T) {
	contracttest.ProviderContractSuite(t, func(t *testing.T) contracttest.ProviderHarness {
		sim := simulator.New(simulator.Config{APIKey: simKey})
		srv := httptest.NewServer(sim.Handler())
		t.Cleanup(srv.Close)
		p := v2.New(v2.Config{Nodes: v2.StaticNodes{"node-01": {BaseURL: srv.URL, APIKey: simKey}}}) // no webhook URL: instances have none
		kinds := map[contracttest.Failure]string{contracttest.Unavailable: "unavailable", contracttest.AuthFailed: "auth",
			contracttest.NotFound: "not_found", contracttest.Ambiguous: "ambiguous"}
		return contracttest.ProviderHarness{
			Provider: p, NodeID: "node-01",
			Inject: func(f contracttest.Failure) { sim.InjectFaults(kinds[f]) },
			Pair:   func(a ownership.Assignment) { _, _ = sim.Scan(a.InstanceID) },
			Drop:   func(a ownership.Assignment) { _, _ = sim.Disconnect(a.InstanceID, false) },
		}
	})
}

// gateway is a stand-in for RelayPlane's webhook endpoint: it authenticates and normalizes with the REAL adapter code.
type gateway struct {
	t   *testing.T
	w   v2.Webhook
	mu  sync.Mutex
	got []events.Inbound
}

func newGateway(t *testing.T, secret string) (*gateway, *httptest.Server) {
	g := &gateway{t: t, w: v2.Webhook{Secret: secret}}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		req := ports.InboundRequest{Header: r.Header, Query: r.URL.Query(), Body: body}
		if _, err := g.w.Authenticate(req); err != nil {
			http.Error(rw, err.Error(), http.StatusUnauthorized)
			return
		}
		evs, err := g.w.Normalize(req)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		g.mu.Lock()
		g.got = append(g.got, evs...)
		g.mu.Unlock()
		rw.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return g, srv
}

func (g *gateway) last() events.Inbound {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.got) == 0 {
		g.t.Fatal("the gateway received no event")
	}
	return g.got[len(g.got)-1]
}

func (g *gateway) count() int { g.mu.Lock(); defer g.mu.Unlock(); return len(g.got) }

func TestSimulatorPlaysTheUsersSideThroughTheRealWebhookNormalizer(t *testing.T) {
	const secret = "whsec"
	g, gw := newGateway(t, secret)
	sim := simulator.New(simulator.Config{APIKey: simKey})
	srv := httptest.NewServer(sim.Handler())
	defer srv.Close()
	p := v2.New(v2.Config{Nodes: v2.StaticNodes{"node-01": {BaseURL: srv.URL, APIKey: simKey}}, WebhookBaseURL: gw.URL, WebhookSecret: secret})
	a := ownership.Assignment{InstanceID: "inst_sim", NodeID: "node-01", Epoch: 1}
	ctx := t.Context()
	if _, err := p.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a}); err != nil {
		t.Fatal(err)
	}

	// 1. the user scans the QR code: RelayPlane hears about it through the same webhook path as with a real node
	if st, err := sim.Scan(a.InstanceID); err != nil || st != 200 {
		t.Fatalf("scan: %d %v", st, err)
	}
	if ev := g.last(); ev.Type != events.InstanceStatusChanged || ev.State != "CONNECTED" || ev.InstanceID != a.InstanceID {
		t.Fatalf("connection event: %+v", ev)
	}

	// 2. RelayPlane sends a message: the simulator returns a provider id and remembers it
	res, err := p.SendMessage(ctx, a, messaging.OutboundMessage{ID: "msg_1", To: "5562988887777", Type: messaging.TypeText, Text: "Confirma amanhã às 15h?"})
	if err != nil || res.ProviderMessageID == "" {
		t.Fatalf("send: %+v %v", res, err)
	}
	if sent := sim.SentMessages(a.InstanceID); len(sent) != 1 || sent[0].ID != res.ProviderMessageID || sent[0].Text != "Confirma amanhã às 15h?" {
		t.Fatalf("sent: %+v", sent)
	}

	// 3. the user ANSWERS BY QUOTING that message: reply_to must survive the whole trip (this is what a confirmation relies on)
	at := time.Now().Add(-2 * time.Second).UTC().Truncate(time.Second)
	out, err := sim.Receive(a.InstanceID, simulator.Inbound{From: "5562988887777", Text: "sim", ReplyTo: "last_sent", PushName: "Ana", Timestamp: at})
	if err != nil || out.GatewayStatus != 200 {
		t.Fatalf("receive: %+v %v", out, err)
	}
	ev := g.last()
	pl, ok := ev.Payload.(events.MessageReceivedPayload)
	if !ok || ev.Type != events.MessageReceived || pl.Text != "sim" || pl.From != "5562988887777" || pl.PushName != "Ana" || pl.Group {
		t.Fatalf("inbound: %+v", ev)
	}
	if pl.ReplyToProviderMessageID != res.ProviderMessageID {
		t.Fatalf("reply_to %q must be the id of OUR message %q", pl.ReplyToProviderMessageID, res.ProviderMessageID)
	}
	if !ev.Timestamp.Equal(at) {
		t.Errorf("the event time is the provider's stamp: %v vs %v", ev.Timestamp, at)
	}

	// 4. a plain message (not a reply) and a message in a group
	if _, err := sim.Receive(a.InstanceID, simulator.Inbound{Text: "oi"}); err != nil {
		t.Fatal(err)
	}
	if pl = g.last().Payload.(events.MessageReceivedPayload); pl.ReplyToProviderMessageID != "" || pl.Text != "oi" {
		t.Errorf("a plain message is not a reply: %+v", pl)
	}
	if _, err := sim.Receive(a.InstanceID, simulator.Inbound{Text: "bom dia", Group: true, From: "5511977776666"}); err != nil {
		t.Fatal(err)
	}
	if pl = g.last().Payload.(events.MessageReceivedPayload); !pl.Group || pl.From != "5511977776666" {
		t.Errorf("group message: %+v", pl)
	}

	// 5. media types arrive with the right kind
	for typ, want := range map[string]string{"image": "image", "audio": "audio", "document": "document", "video": "video"} {
		if _, err := sim.Receive(a.InstanceID, simulator.Inbound{Type: typ, Text: "legenda", Seconds: 4}); err != nil {
			t.Fatal(err)
		}
		if pl = g.last().Payload.(events.MessageReceivedPayload); pl.Type != want {
			t.Errorf("%s -> %+v", typ, pl)
		}
	}

	// 6. receipts for the message we sent
	for status, want := range map[string]string{"delivered": "delivered", "read": "read", "failed": "failed"} {
		if st, err := sim.Receipt(a.InstanceID, res.ProviderMessageID, status); err != nil || st != 200 {
			t.Fatalf("receipt %s: %d %v", status, st, err)
		}
		ev := g.last()
		if r, ok := ev.Payload.(events.MessageStatusPayload); !ok || ev.Type != events.MessageStatus || r.Status != want || r.ProviderMessageID != res.ProviderMessageID {
			t.Errorf("receipt %s: %+v", status, ev)
		}
	}
	if _, err := sim.Receipt(a.InstanceID, "last_sent", "bogus"); err == nil {
		t.Error("an unknown receipt status is an error")
	}

	// 7. the socket dies, and the user logs the device out
	if _, err := sim.Disconnect(a.InstanceID, false); err != nil {
		t.Fatal(err)
	}
	if ev := g.last(); ev.State != "DISCONNECTED" {
		t.Errorf("disconnect: %+v", ev)
	}
	if _, err := p.SendMessage(ctx, a, messaging.OutboundMessage{ID: "m2", To: "5562988887777", Type: messaging.TypeText, Text: "x"}); err == nil {
		t.Error("a disconnected instance cannot send")
	}
	if _, err := sim.Disconnect(a.InstanceID, true); err != nil {
		t.Fatal(err)
	}
	if ev := g.last(); ev.State != "LOGGED_OUT" {
		t.Errorf("logout: %+v", ev)
	}

	// the gateway must have refused nothing: every webhook carried the per-node token
	before := g.count()
	bad := httptest.NewRequest(http.MethodPost, gw.URL+"/webhooks/evolution-v2?node=node-01&epoch=1", nil)
	if _, err := g.w.Authenticate(ports.InboundRequest{Header: bad.Header, Query: bad.URL.Query()}); err == nil {
		t.Error("a webhook without the node token must not authenticate")
	}
	if g.count() != before {
		t.Error("unexpected extra events")
	}
}

func TestSimulatorControlAPIAndFaults(t *testing.T) {
	sim := simulator.New(simulator.Config{APIKey: simKey})
	srv := httptest.NewServer(sim.Handler())
	defer srv.Close()
	call := func(method, path, key string) int {
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		if key != "" {
			req.Header.Set("apikey", key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := call("GET", "/_sim/instances", ""); c != 401 {
		t.Errorf("the control API needs the node key: %d", c)
	}
	if c := call("GET", "/_sim/instances", simKey); c != 200 {
		t.Errorf("control with the key: %d", c)
	}
	if c := call("POST", "/_sim/instances/ghost/scan", simKey); c != 502 {
		t.Errorf("an unknown instance is reported, not ignored: %d", c)
	}
	if c := call("GET", "/", ""); c != 200 {
		t.Errorf("the version probe is open: %d", c)
	}
	// a queued fault hits the next API call exactly once
	sim.InjectFaults("server_error")
	if c := call("GET", "/instance/fetchInstances", simKey); c != 500 {
		t.Errorf("injected fault: %d", c)
	}
	if c := call("GET", "/instance/fetchInstances", simKey); c != 200 {
		t.Errorf("faults are consumed: %d", c)
	}
}

// An attachment goes: user message -> the REAL normalizer (description + key-bearing reference) -> the REAL DownloadMedia
// -> the simulator's getBase64FromMediaMessage, which, like the real node, is handed the message instead of finding it.
func TestInboundAttachmentIsDownloadedThroughTheRealAdapter(t *testing.T) {
	const secret = "whsec"
	g, gw := newGateway(t, secret)
	sim := simulator.New(simulator.Config{APIKey: simKey})
	srv := httptest.NewServer(sim.Handler())
	defer srv.Close()
	p := v2.New(v2.Config{Nodes: v2.StaticNodes{"node-01": {BaseURL: srv.URL, APIKey: simKey}}, WebhookBaseURL: gw.URL, WebhookSecret: secret})
	a := ownership.Assignment{InstanceID: "inst_media", NodeID: "node-01", Epoch: 1}
	ctx := t.Context()
	if _, err := p.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a}); err != nil {
		t.Fatal(err)
	}
	var dl ports.MediaDownloader = p
	content := []byte("OggS the user's voice note")

	if _, err := sim.Receive(a.InstanceID, simulator.Inbound{Type: "audio", Seconds: 7, Content: content, ID: "SIM-AUD"}); err != nil {
		t.Fatal(err)
	}
	ev := g.last()
	pl := ev.Payload.(events.MessageReceivedPayload)
	if ev.Media == nil || pl.Media == nil || pl.Media.Kind != "audio" || pl.Media.Size != int64(len(content)) || pl.Media.Seconds != 7 {
		t.Fatalf("description (the Long-encoded fileLength must be understood): %+v", pl.Media)
	}
	got, err := dl.DownloadMedia(ctx, a, ev.Media.Ref, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(got.Body)
	if string(raw) != string(content) || got.Size != int64(len(content)) || got.ContentType != "audio/ogg; codecs=opus" {
		t.Fatalf("download: %q %+v", raw, got)
	}

	// a limit smaller than the content: refused, not truncated
	if _, err := dl.DownloadMedia(ctx, a, ev.Media.Ref, 5); !errors.Is(err, errs.ErrPayloadTooLarge) {
		t.Errorf("over the limit: %v", err)
	}

	// an attachment WhatsApp no longer has: the node answers 400 and the adapter calls it a rejection (permanent-ish)
	if _, err := sim.Receive(a.InstanceID, simulator.Inbound{Type: "image", Content: []byte("jpg"), FailDownloads: 1, ID: "SIM-IMG"}); err != nil {
		t.Fatal(err)
	}
	if _, err := dl.DownloadMedia(ctx, a, g.last().Media.Ref, 1<<20); !errors.Is(err, errs.ErrProviderRejected) {
		t.Errorf("an attachment the node cannot fetch: %v", err)
	}
	if _, err := dl.DownloadMedia(ctx, a, g.last().Media.Ref, 1<<20); err != nil {
		t.Errorf("it can be fetched afterwards: %v", err)
	}

	// an announced size above 4 GiB survives the Long encoding (low/high words)
	if _, err := sim.Receive(a.InstanceID, simulator.Inbound{Type: "video", DeclaredSize: 5 << 30, ID: "SIM-VID"}); err != nil {
		t.Fatal(err)
	}
	if m := g.last().Payload.(events.MessageReceivedPayload).Media; m.Size != 5<<30 {
		t.Errorf("announced size: %d", m.Size)
	}

	// a plain text message has no attachment
	if _, err := sim.Receive(a.InstanceID, simulator.Inbound{Text: "oi"}); err != nil {
		t.Fatal(err)
	}
	if ev = g.last(); ev.Media != nil || ev.Payload.(events.MessageReceivedPayload).Media != nil {
		t.Errorf("text has no media: %+v", ev)
	}
}
