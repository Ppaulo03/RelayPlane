package v2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/relayplane/relayplane/internal/adapters/providers/evolution/v2"
	"github.com/relayplane/relayplane/internal/contracttest"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

const apiKey = "node-key"

// fakeEvolution mimics the subset of the Evolution v2 HTTP API the adapter uses.
type fakeEvolution struct {
	mu        sync.Mutex
	srv       *httptest.Server
	instances map[string]*fakeInst
	fail      []contracttest.Failure
	version   string
	sends     int
	lastSend  map[string]any
}

type fakeInst struct{ state, owner string }

func newFake(t *testing.T) *fakeEvolution {
	f := &fakeEvolution{instances: map[string]*fakeInst{}, version: "2.3.7"}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeEvolution) inject(k contracttest.Failure) {
	f.mu.Lock()
	f.fail = append(f.fail, k)
	f.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func evoErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"status": code, "error": http.StatusText(code), "response": map[string]any{"message": []string{msg}}})
}

func (f *fakeEvolution) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path == "/" {
		writeJSON(w, 200, map[string]any{"status": 200, "message": "Welcome to the Evolution API, it is working!", "version": f.version})
		return
	}
	if len(f.fail) > 0 {
		k := f.fail[0]
		f.fail = f.fail[1:]
		switch k {
		case contracttest.Unavailable: // throttled before the node accepted it: provably not executed
			evoErr(w, 429, "Too Many Requests")
		case contracttest.AuthFailed:
			evoErr(w, 401, "Unauthorized")
		case contracttest.NotFound:
			evoErr(w, 404, "The instance does not exist")
		case contracttest.Ambiguous: // the node crashes mid-request: connection dropped, no response
			if hj, ok := w.(http.Hijacker); ok {
				c, _, _ := hj.Hijack()
				c.Close()
			}
		}
		return
	}
	if r.Header.Get("apikey") != apiKey {
		evoErr(w, 401, "Unauthorized")
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	name := ""
	if len(parts) > 2 {
		name, _ = url.PathUnescape(parts[2])
	}
	route := parts[0] + "/" + parts[1]
	inst := f.instances[name]
	missing := func() { evoErr(w, 404, fmt.Sprintf("The %q instance does not exist", name)) }

	switch {
	case route == "instance/create" && r.Method == "POST":
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		n, _ := body["instanceName"].(string)
		if _, ok := f.instances[n]; ok {
			evoErr(w, 403, fmt.Sprintf("This name %q is already in use.", n))
			return
		}
		f.instances[n] = &fakeInst{state: "connecting"}
		writeJSON(w, 201, map[string]any{"instance": map[string]any{"instanceName": n, "instanceId": "uuid-" + n, "status": "created"}, "hash": "h"})
	case route == "instance/fetchInstances":
		var out []map[string]any
		for n, i := range f.instances {
			if want := r.URL.Query().Get("instanceName"); want == "" || want == n {
				out = append(out, map[string]any{"id": "uuid-" + n, "name": n, "connectionStatus": i.state, "ownerJid": i.owner})
			}
		}
		writeJSON(w, 200, out)
	case inst == nil:
		missing()
	case route == "instance/connectionState":
		writeJSON(w, 200, map[string]any{"instance": map[string]any{"instanceName": name, "state": inst.state}})
	case route == "instance/connect":
		if inst.state == "open" {
			writeJSON(w, 200, map[string]any{"instance": map[string]any{"instanceName": name, "state": "open"}})
			return
		}
		writeJSON(w, 200, map[string]any{"pairingCode": "ABCD1234", "code": "2@qr", "base64": "data:image/png;base64,AAAA", "count": 1})
	case route == "instance/logout" && r.Method == "DELETE":
		if inst.state != "open" {
			evoErr(w, 400, "Instance is not connected")
			return
		}
		inst.state, inst.owner = "close", ""
		writeJSON(w, 200, map[string]any{"status": "SUCCESS", "error": false})
	case route == "instance/delete" && r.Method == "DELETE":
		delete(f.instances, name)
		writeJSON(w, 200, map[string]any{"status": "SUCCESS", "error": false})
	case route == "message/sendText" || route == "message/sendMedia" || route == "message/sendWhatsAppAudio":
		if inst.state != "open" {
			evoErr(w, 400, "Connection Closed")
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.sends++
		f.lastSend = body
		writeJSON(w, 201, map[string]any{"key": map[string]any{"remoteJid": "x@s.whatsapp.net", "fromMe": true, "id": fmt.Sprintf("3EB0%04d", f.sends)}, "status": "PENDING"})
	default:
		evoErr(w, 404, "route not found")
	}
}

func newProvider(f *fakeEvolution) *v2.Provider {
	return v2.New(v2.Config{
		Nodes:          v2.StaticNodes{"node-01": {BaseURL: f.srv.URL, APIKey: apiKey}},
		WebhookBaseURL: "http://gateway:8080", WebhookSecret: "whsecret",
	})
}

func TestEvolutionAdapterPassesProviderContractSuite(t *testing.T) {
	contracttest.ProviderContractSuite(t, func(t *testing.T) contracttest.ProviderHarness {
		f := newFake(t)
		return contracttest.ProviderHarness{
			Provider: newProvider(f), NodeID: "node-01",
			Inject: f.inject,
			Pair: func(a ownership.Assignment) {
				f.mu.Lock()
				f.instances[a.InstanceID].state, f.instances[a.InstanceID].owner = "open", "5562999999999@s.whatsapp.net"
				f.mu.Unlock()
			},
			Drop: func(a ownership.Assignment) {
				f.mu.Lock()
				f.instances[a.InstanceID].state = "close" // paired but disconnected
				f.mu.Unlock()
			},
		}
	})
}

func TestMediaIsSentByReferenceOrStreamedNeverThroughBroker(t *testing.T) {
	f := newFake(t)
	p := newProvider(f)
	a := ownership.Assignment{InstanceID: "inst_m", NodeID: "node-01", Epoch: 1}
	ctx := t.Context()
	_, _ = p.CreateInstance(ctx, ports.CreateInstanceRequest{Assignment: a})
	f.instances["inst_m"].state = "open"
	_, err := p.SendMessage(ctx, a, mediaMsg("http://blob/x?sig=1"))
	if err != nil {
		t.Fatal(err)
	}
	if f.lastSend["media"] != "http://blob/x?sig=1" || f.lastSend["mediatype"] != "document" || f.lastSend["mimetype"] != "application/pdf" {
		t.Fatalf("sendMedia body: %v", f.lastSend)
	}
}

func TestCompatibilityCheck(t *testing.T) {
	f := newFake(t)
	p := newProvider(f)
	probe, err := p.ProbeNode(t.Context(), "node-01")
	if err != nil || !probe.Ready || probe.Version != "2.3.7" {
		t.Fatalf("%+v %v", probe, err)
	}
	for _, bad := range []string{"3.0.1", "2.9.0", "2.3.8", "2.4.1", "1.9"} {
		f.version = bad
		probe, err = p.ProbeNode(t.Context(), "node-01")
		if err == nil || probe.Ready {
			t.Fatalf("untested version %s must not be READY: %+v %v", bad, probe, err)
		}
	}
	f.version = "2.4.0" // never validated against this adapter (the upstream 2.4.0 builds do not even migrate their database)
	if probe, err = p.ProbeNode(t.Context(), "node-01"); err == nil || probe.Ready {
		t.Fatalf("2.4.0 must be refused until it is tested: %+v %v", probe, err)
	}
	pOpt := v2.New(v2.Config{Nodes: v2.StaticNodes{"node-01": {BaseURL: f.srv.URL, APIKey: apiKey}}, AllowedVersions: []string{"2.4.0"}})
	if probe, err = pOpt.ProbeNode(t.Context(), "node-01"); err != nil || !probe.Ready {
		t.Fatalf("an operator can allow a version explicitly: %+v %v", probe, err)
	}
	if !v2.Compatible("2.3.7") || v2.Compatible("2.3.6") || !v2.Compatible("2.9.0", "2.9.0") {
		t.Error("compatibility predicate")
	}
}

func TestCreateConfiguresAuthenticatedWebhookForThisAssignment(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		writeJSON(w, 201, map[string]any{"instance": map[string]any{"instanceId": "u"}})
	}))
	defer srv.Close()
	p := v2.New(v2.Config{Nodes: v2.StaticNodes{"node-02": {BaseURL: srv.URL, APIKey: apiKey}}, WebhookBaseURL: "http://gw:8080/", WebhookSecret: "s"})
	_, err := p.CreateInstance(t.Context(), ports.CreateInstanceRequest{Assignment: ownership.Assignment{InstanceID: "inst_1", NodeID: "node-02", Epoch: 4}})
	if err != nil {
		t.Fatal(err)
	}
	wh := got["webhook"].(map[string]any)
	u, _ := url.Parse(wh["url"].(string))
	if u.Path != "/webhooks/evolution-v2" || u.Query().Get("node") != "node-02" || u.Query().Get("epoch") != "4" {
		t.Errorf("webhook url %s", wh["url"])
	}
	hdr := wh["headers"].(map[string]any)
	if hdr[v2.TokenHeader] != v2.NodeToken("s", "node-02") || got["instanceName"] != "inst_1" || got["integration"] != "WHATSAPP-BAILEYS" {
		t.Errorf("%v", got)
	}
}

// ---- webhook normalization (anti-corruption layer) ----

func hook(secret, node string, epoch int, body string) ports.InboundRequest {
	h := http.Header{}
	h.Set(v2.TokenHeader, v2.NodeToken(secret, node))
	return ports.InboundRequest{Header: h, Query: url.Values{"node": {node}, "epoch": {fmt.Sprint(epoch)}}, Body: []byte(body)}
}

func TestWebhookAuthentication(t *testing.T) {
	w := v2.Webhook{Secret: "s"}
	claim, err := w.Authenticate(hook("s", "node-01", 3, `{}`))
	if err != nil || claim.NodeID != "node-01" || claim.Epoch != 3 {
		t.Fatalf("%+v %v", claim, err)
	}
	if _, err := w.Authenticate(hook("wrong", "node-01", 3, `{}`)); err == nil {
		t.Error("wrong secret accepted")
	}
	// node-02's token cannot be used to claim node-01
	r := hook("s", "node-02", 3, `{}`)
	r.Query.Set("node", "node-01")
	if _, err := w.Authenticate(r); err == nil {
		t.Error("a token for another node was accepted: nodes could impersonate each other")
	}
	if _, err := w.Authenticate(ports.InboundRequest{Query: url.Values{"node": {"node-01"}}, Header: http.Header{}}); err == nil {
		t.Error("missing token accepted")
	}
	if _, err := w.Authenticate(ports.InboundRequest{Header: http.Header{}, Query: url.Values{}}); err == nil {
		t.Error("missing node accepted")
	}
}

func TestWebhookNormalization(t *testing.T) {
	w := v2.Webhook{Secret: "s"}
	norm := func(body string) []events.Inbound {
		t.Helper()
		out, err := w.Normalize(hook("s", "node-01", 1, body))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	got := norm(`{"event":"messages.upsert","instance":"inst_1","data":{"key":{"remoteJid":"5562999999999@s.whatsapp.net","fromMe":false,"id":"WAID1"},
		"pushName":"Ana","messageType":"conversation","message":{"conversation":"Olá"},"messageTimestamp":1759000000}}`)
	if len(got) != 1 || got[0].Type != events.MessageReceived || got[0].InstanceID != "inst_1" || got[0].ProviderMessageID != "WAID1" {
		t.Fatalf("%+v", got)
	}
	pl := got[0].Payload.(events.MessageReceivedPayload)
	if pl.From != "5562999999999" || pl.Text != "Olá" || pl.Type != "text" || pl.PushName != "Ana" || pl.Group {
		t.Errorf("%+v", pl)
	}

	if got := norm(`{"event":"messages.upsert","instance":"inst_1","data":{"key":{"remoteJid":"5562@s.whatsapp.net","fromMe":true,"id":"X"},"messageType":"conversation","message":{"conversation":"mine"}}}`); len(got) != 0 {
		t.Errorf("own messages must not become message.received: %+v", got)
	}

	got = norm(`{"event":"messages.upsert","instance":"inst_1","data":[{"key":{"remoteJid":"1203@g.us","participant":"5511888888888@s.whatsapp.net","fromMe":false,"id":"G1"},
		"messageType":"imageMessage","message":{"imageMessage":{"caption":"look"}}}]}`)
	pl = got[0].Payload.(events.MessageReceivedPayload)
	if !pl.Group || pl.From != "5511888888888" || pl.Type != "image" || pl.Text != "look" {
		t.Errorf("group/array handling: %+v", pl)
	}

	for in, want := range map[string]string{"SERVER_ACK": "sent", "DELIVERY_ACK": "delivered", "READ": "read", "PLAYED": "read", "ERROR": "failed"} {
		got := norm(fmt.Sprintf(`{"event":"messages.update","instance":"inst_1","data":{"keyId":"WAID9","status":%q}}`, in))
		if len(got) != 1 || got[0].State != want || got[0].Type != events.MessageStatus || got[0].ProviderMessageID != "WAID9" {
			t.Errorf("%s: %+v", in, got)
		}
	}
	if got := norm(`{"event":"messages.update","instance":"inst_1","data":{"keyId":"W","status":"PENDING"}}`); len(got) != 0 {
		t.Errorf("unmodelled status leaked: %+v", got)
	}

	for _, c := range []struct{ data, want string }{
		{`{"state":"open"}`, "CONNECTED"}, {`{"state":"connecting"}`, "CONNECTING"},
		{`{"state":"close","statusReason":428}`, "DISCONNECTED"}, {`{"state":"close","statusReason":401}`, "LOGGED_OUT"},
	} {
		got := norm(`{"event":"connection.update","instance":"inst_1","date_time":"2026-10-02T10:00:00Z","data":` + c.data + `}`)
		if len(got) != 1 || got[0].Type != events.InstanceStatusChanged || got[0].State != c.want {
			t.Errorf("%s: %+v", c.data, got)
		}
	}
	a := norm(`{"event":"connection.update","instance":"i","date_time":"2026-10-02T10:00:00Z","data":{"state":"open"}}`)[0]
	b := norm(`{"event":"connection.update","instance":"i","date_time":"2026-10-02T10:00:05Z","data":{"state":"open"}}`)[0]
	if events.DedupeKey("i", a.Type, a.ProviderMessageID, a.State) == events.DedupeKey("i", b.Type, b.ProviderMessageID, b.State) {
		t.Error("two separate CONNECTED transitions must not dedupe into one")
	}

	qr := norm(`{"event":"qrcode.updated","instance":"inst_1","data":{"qrcode":{"base64":"data:image/png;base64,SECRETQR","code":"2@x","pairingCode":"ABCD1234"}}}`)
	raw, _ := json.Marshal(qr)
	if len(qr) != 1 || strings.Contains(string(raw), "SECRETQR") || strings.Contains(string(raw), "ABCD1234") {
		t.Errorf("QR material leaked into a canonical event: %s", raw)
	}

	if got := norm(`{"event":"presence.update","instance":"inst_1","data":{}}`); len(got) != 0 {
		t.Errorf("unknown events must be dropped: %+v", got)
	}
	if _, err := w.Normalize(hook("s", "node-01", 1, `not json`)); err == nil {
		t.Error("malformed body must be an error")
	}
	if _, err := w.Normalize(hook("s", "node-01", 1, `{"event":"messages.upsert","data":{}}`)); err == nil {
		t.Error("missing instance must be an error")
	}
}

func mediaMsg(url string) messaging.OutboundMessage {
	return messaging.OutboundMessage{ID: "m", To: "5562999999999", Type: messaging.TypeDocument, Filename: "a.pdf", Caption: "c",
		Media: &messaging.Attachment{ContentType: "application/pdf", Size: 4, URL: url}}
}

// A gateway/proxy error on a send may have happened after the node executed it.
func TestSend5xxIsAmbiguousButManagementCallsStayRetryable(t *testing.T) {
	for _, status := range []int{500, 502, 503, 504} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/instance/connectionState/inst_1" {
				evoErr(w, status, "upstream")
				return
			}
			evoErr(w, status, "upstream")
		}))
		p := v2.New(v2.Config{Nodes: v2.StaticNodes{"node-01": {BaseURL: srv.URL, APIKey: apiKey}}})
		a := ownership.Assignment{InstanceID: "inst_1", NodeID: "node-01", Epoch: 1}
		_, err := p.SendMessage(t.Context(), a, messaging.OutboundMessage{ID: "m", To: "5562", Type: messaging.TypeText, Text: "x"})
		if errs.Classify(err) != errs.Ambiguous {
			t.Errorf("send HTTP %d must be AMBIGUOUS, got %v (%s)", status, err, errs.Classify(err))
		}
		_, err = p.GetInstanceState(t.Context(), a)
		if errs.Classify(err) != errs.Retryable {
			t.Errorf("state HTTP %d must stay RETRYABLE, got %v", status, err)
		}
		srv.Close()
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { evoErr(w, 429, "slow down") }))
	defer srv.Close()
	p := v2.New(v2.Config{Nodes: v2.StaticNodes{"node-01": {BaseURL: srv.URL, APIKey: apiKey}}})
	_, err := p.SendMessage(t.Context(), ownership.Assignment{InstanceID: "i", NodeID: "node-01", Epoch: 1}, messaging.OutboundMessage{ID: "m", To: "5562", Type: messaging.TypeText, Text: "x"})
	if errs.Classify(err) != errs.Retryable {
		t.Errorf("429 means not accepted => RETRYABLE, got %v", err)
	}
}

// The quoted message id is the strongest evidence that an answer refers to one of OUR messages.
func TestWebhookExtractsTheQuotedMessage(t *testing.T) {
	w := v2.Webhook{Secret: "s"}
	norm := func(body string) events.MessageReceivedPayload {
		t.Helper()
		out, err := w.Normalize(hook("s", "node-01", 1, body))
		if err != nil || len(out) != 1 {
			t.Fatalf("%v %+v", err, out)
		}
		return out[0].Payload.(events.MessageReceivedPayload)
	}
	nested := norm(`{"event":"messages.upsert","instance":"i","data":{"key":{"remoteJid":"5562@s.whatsapp.net","fromMe":false,"id":"R1"},
		"messageType":"extendedTextMessage","message":{"extendedTextMessage":{"text":"sim","contextInfo":{"stanzaId":"OURS1"}}}}}`)
	if nested.ReplyToProviderMessageID != "OURS1" || nested.Text != "sim" {
		t.Errorf("nested contextInfo: %+v", nested)
	}
	top := norm(`{"event":"messages.upsert","instance":"i","data":{"key":{"remoteJid":"5562@s.whatsapp.net","fromMe":false,"id":"R2"},
		"messageType":"conversation","message":{"conversation":"ok"},"contextInfo":{"stanzaId":"OURS2"}}}`)
	if top.ReplyToProviderMessageID != "OURS2" {
		t.Errorf("top-level contextInfo: %+v", top)
	}
	plain := norm(`{"event":"messages.upsert","instance":"i","data":{"key":{"remoteJid":"5562@s.whatsapp.net","fromMe":false,"id":"R3"},
		"messageType":"conversation","message":{"conversation":"oi"}}}`)
	if plain.ReplyToProviderMessageID != "" {
		t.Errorf("a plain message is not a reply: %+v", plain)
	}
}
