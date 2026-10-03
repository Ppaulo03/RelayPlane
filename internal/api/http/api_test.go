package http_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/relayplane/relayplane/internal/adapters/memory"
	"io"
	"log/slog"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apihttp "github.com/relayplane/relayplane/internal/api/http"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/systemtest"
)

type harness struct {
	t      *testing.T
	env    *systemtest.Env
	srv    *httptest.Server
	key1   string
	key2   string
	admin  string
	tenant string
}

func newHarness(t *testing.T) *harness {
	e := systemtest.NewEnv(t)
	_, k1, _ := e.App.Tenants.Create(context.Background(), "t-one")
	t2, k2, _ := e.App.Tenants.Create(context.Background(), "t-two")
	_ = t2
	api := &apihttp.Server{App: e.App, Metrics: e.Metrics, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxUpload: 1 << 20, Auth: apihttp.KeyAuthenticator{Tenants: e.App.Tenants, AdminKey: "admin-key"}}
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	return &harness{t: t, env: e, srv: srv, key1: k1, key2: k2, admin: "admin-key"}
}

func (h *harness) call(method, path, key, body string, hdr ...string) (int, map[string]any, nethttp.Header) {
	h.t.Helper()
	req, _ := nethttp.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	if out == nil {
		out = map[string]any{"_raw": string(raw)}
	}
	return resp.StatusCode, out, resp.Header
}

func TestAuthAndTenantIsolation(t *testing.T) {
	h := newHarness(t)
	if c, _, _ := h.call("GET", "/api/v1/instances", "", ""); c != 401 {
		t.Errorf("no key: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/instances", "bogus", ""); c != 401 {
		t.Errorf("bad key: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/nodes", h.key1, ""); c != 403 {
		t.Errorf("tenant on admin route: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/instances", h.admin, `{"name":"x"}`); c != 403 {
		t.Errorf("admin on tenant route: %d", c)
	}

	c, body, hdr := h.call("POST", "/api/v1/instances", h.key1, `{"name":"Comercial","provider":"evolution"}`, "Idempotency-Key", "k1")
	if c != 201 || body["status"] != string(instance.AwaitingPairing) {
		t.Fatalf("create: %d %v", c, body)
	}
	id := body["id"].(string)
	if hdr.Get("Location") != "/api/v1/instances/"+id {
		t.Errorf("location %q", hdr.Get("Location"))
	}
	c, body2, hdr2 := h.call("POST", "/api/v1/instances", h.key1, `{"name":"Comercial","provider":"evolution"}`, "Idempotency-Key", "k1")
	if c != 201 || body2["id"] != id || hdr2.Get("Idempotent-Replayed") != "true" {
		t.Errorf("replay: %d %v %v", c, body2, hdr2)
	}
	if c, b, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"Other"}`, "Idempotency-Key", "k1"); c != 422 {
		t.Errorf("key reuse: %d %v", c, b)
	}

	// tenant two sees nothing of tenant one
	for _, p := range []struct{ m, path string }{{"GET", "/api/v1/instances/" + id}, {"DELETE", "/api/v1/instances/" + id},
		{"GET", "/api/v1/instances/" + id + "/qrcode"}, {"POST", "/api/v1/instances/" + id + "/migrate"}, {"GET", "/api/v1/operations/op_create_" + id}} {
		if c, _, _ := h.call(p.m, p.path, h.key2, ""); c != 404 {
			t.Errorf("%s %s as other tenant: %d", p.m, p.path, c)
		}
	}
	if _, b, _ := h.call("GET", "/api/v1/instances", h.key2, ""); len(b["instances"].([]any)) != 0 {
		t.Errorf("tenant two lists tenant one's instances: %v", b)
	}
	// the tenant is derived from the credential: a smuggled tenant_id is rejected, not honoured
	if c, _, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"x","tenant_id":"tenant_other"}`); c != 400 {
		t.Errorf("tenant_id in payload: %d", c)
	}
}

func TestPublicAPIDoesNotLeakProviderDetails(t *testing.T) {
	h := newHarness(t)
	_, body, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"a"}`)
	id := body["id"].(string)
	h.env.Connect(id)
	var all strings.Builder
	for _, p := range []string{"/api/v1/instances", "/api/v1/instances/" + id, "/api/v1/operations/op_create_" + id, "/api/v1/instances/" + id + "/qrcode"} {
		_, b, _ := h.call("GET", p, h.key1, "")
		raw, _ := json.Marshal(b)
		all.Write(raw)
	}
	h.env.Provider.FailNext(memory.FailUnavailable) // an unavailable provider must produce a generic message
	_, b, _ := h.call("GET", "/api/v1/instances/"+id+"/pairing-code", h.key1, "")
	raw, _ := json.Marshal(b)
	all.Write(raw)
	low := strings.ToLower(all.String())
	for _, bad := range []string{"evolution", "node-01", "node-02", "baileys", "assignment_epoch", "node_id"} {
		if strings.Contains(low, bad) {
			t.Errorf("public API leaked %q: %s", bad, all.String())
		}
	}
}

func TestSendMessageContract(t *testing.T) {
	h := newHarness(t)
	_, body, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"a"}`)
	id := body["id"].(string)
	h.env.Connect(id)
	h.env.StartWorkers(1)

	c, b, _ := h.call("POST", "/api/v1/messages/send", h.key1,
		`{"instance_id":"`+id+`","to":"5562999999999","type":"text","payload":{"text":"hello"}}`, "Idempotency-Key", "order-1")
	if c != 202 || b["status"] != "QUEUED" {
		t.Fatalf("%d %v", c, b)
	}
	msg := b["message_id"].(string)
	var status string
	systemtest.Eventually(t, 5_000_000_000, "accepted", func() bool {
		_, m, _ := h.call("GET", "/api/v1/messages/"+msg, h.key1, "")
		status, _ = m["status"].(string)
		return status == "ACCEPTED"
	})
	if c, _, _ := h.call("GET", "/api/v1/messages/"+msg, h.key2, ""); c != 404 {
		t.Errorf("cross-tenant message read: %d", c)
	}
	// inline binary is structurally impossible: unknown fields are refused
	if c, _, _ := h.call("POST", "/api/v1/messages/send", h.key1,
		`{"instance_id":"`+id+`","to":"5562999999999","type":"document","payload":{"base64":"AAAA"}}`); c != 400 {
		t.Errorf("inline base64: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/messages/send", h.key1,
		`{"instance_id":"`+id+`","to":"5562999999999","type":"document","payload":{"media_id":"med_nope"}}`); c != 404 {
		t.Errorf("unknown media: %d", c)
	}
	big := strings.Repeat("A", 3000)
	if c, b, _ := h.call("POST", "/api/v1/messages/send", h.key1,
		`{"instance_id":"`+id+`","to":"5562999999999","type":"text","payload":{"text":"`+big+`"}}`); c != 413 {
		t.Errorf("oversized: %d %v", c, b)
	}
}

func TestMediaUploadFlow(t *testing.T) {
	h := newHarness(t)
	data := bytes.Repeat([]byte("x"), 2048)
	sum := sha256Hex(data)
	c, tk, _ := h.call("POST", "/api/v1/media/uploads", h.key1, `{"content_type":"application/pdf","size":2048,"sha256":"`+sum+`","filename":"a.pdf"}`)
	if c != 201 {
		t.Fatalf("%d %v", c, tk)
	}
	id := tk["media_id"].(string)
	if !strings.Contains(tk["object_key"].(string), "/media/"+id+"/") {
		t.Errorf("object key %v", tk["object_key"])
	}
	req, _ := nethttp.NewRequest("PUT", h.srv.URL+"/api/v1/media/"+id+"/content", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+h.key1)
	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("upload: %v %v", resp, err)
	}
	if _, m, _ := h.call("GET", "/api/v1/media/"+id, h.key1, ""); m["status"] != "READY" {
		t.Errorf("%v", m)
	}
	if c, _, _ := h.call("GET", "/api/v1/media/"+id, h.key2, ""); c != 404 {
		t.Errorf("cross-tenant media: %d", c)
	}
	if c, _, _ := h.call("DELETE", "/api/v1/media/"+id, h.key1, ""); c != 204 {
		t.Errorf("delete: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/media/"+id, h.key1, ""); c != 404 {
		t.Errorf("deleted media still visible: %d", c)
	}
}

func TestAdminAndHealth(t *testing.T) {
	h := newHarness(t)
	c, b, _ := h.call("POST", "/api/v1/tenants", h.admin, `{"name":"new tenant"}`)
	if c != 201 || !strings.HasPrefix(b["api_key"].(string), "rpk_") {
		t.Fatalf("%d %v", c, b)
	}
	// the new key works
	if c, _, _ := h.call("GET", "/api/v1/instances", b["api_key"].(string), ""); c != 200 {
		t.Errorf("new tenant key: %d", c)
	}
	if c, b, _ := h.call("GET", "/api/v1/nodes", h.admin, ""); c != 200 || len(b["nodes"].([]any)) != 2 {
		t.Errorf("nodes: %d %v", c, b)
	}
	if c, b, _ := h.call("POST", "/api/v1/nodes/node-01/drain", h.admin, ""); c != 200 || b["status"] != "DRAINING" {
		t.Errorf("drain: %d %v", c, b)
	}
	if c, _, _ := h.call("POST", "/api/v1/nodes/node-01/drain", h.admin, ""); c != 200 {
		t.Errorf("drain twice must be idempotent: %d", c)
	}
	if c, b, _ := h.call("POST", "/api/v1/nodes/node-01/resume", h.admin, ""); c != 200 || b["status"] != "READY" {
		t.Errorf("resume: %d %v", c, b)
	}
	if c, _, _ := h.call("POST", "/api/v1/nodes/ghost/drain", h.admin, ""); c != 404 {
		t.Errorf("unknown node: %d", c)
	}
	if c, _, _ := h.call("GET", "/health/live", "", ""); c != 200 {
		t.Errorf("live: %d", c)
	}
	if c, _, _ := h.call("GET", "/health/ready", "", ""); c != 200 {
		t.Errorf("ready: %d", c)
	}
	resp, _ := nethttp.Get(h.srv.URL + "/metrics")
	raw, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(raw), "relayplane_instances_total") && !strings.Contains(string(raw), "relayplane_http_requests_total") {
		t.Errorf("metrics missing: %s", raw[:min(len(raw), 200)])
	}
}

func TestWebhookEndpoint(t *testing.T) {
	h := newHarness(t)
	_, body, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"a"}`)
	id := body["id"].(string)
	inst, _ := h.env.Repos.Instances.Get(context.Background(), id)
	ev := `{"node":"` + inst.NodeID + `","epoch":1,"token":"secret","events":[{"instance_id":"` + id + `","type":"message.received","provider_message_id":"w1","payload":{"from":"5562","text":"hi"}}]}`
	if c, b, _ := h.call("POST", "/webhooks/evolution-v2", "", ev); c != 200 || b["published"].(float64) != 1 {
		t.Fatalf("%d %v", c, b)
	}
	if c, b, _ := h.call("POST", "/webhooks/evolution-v2", "", ev); c != 200 || b["duplicates"].(float64) != 1 {
		t.Fatalf("dup: %d %v", c, b)
	}
	bad := strings.Replace(ev, inst.NodeID, "node-xx", 1)
	if c, _, _ := h.call("POST", "/webhooks/evolution-v2", "", bad); c != 409 {
		t.Errorf("ownership violation: %d", c)
	}
	if c, _, _ := h.call("POST", "/webhooks/evolution-v2", "", strings.Replace(ev, "secret", "wrong", 1)); c != 401 {
		t.Errorf("bad token: %d", c)
	}
	if c, _, _ := h.call("POST", "/webhooks/unknown", "", ev); c != 404 {
		t.Errorf("unknown provider: %d", c)
	}
	if c, _, _ := h.call("POST", "/webhooks/evolution-v2", "", "not json"); c != 401 && c != 400 {
		t.Errorf("garbage: %d", c)
	}
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func (h *harness) tenantID(t *testing.T) string { return "" }

// The gateway never reads more than MaxUpload, and nothing of a rejected upload is kept.
func TestMediaUploadHardCap(t *testing.T) {
	h := newHarness(t)
	data := bytes.Repeat([]byte("y"), 2<<20) // 2 MiB against a 1 MiB cap
	c, tk, _ := h.call("POST", "/api/v1/media/uploads", h.key1, `{"content_type":"application/pdf","size":2097152,"sha256":"`+sha256Hex(data)+`","filename":"big.pdf"}`)
	if c != 201 {
		t.Fatalf("%d %v", c, tk)
	}
	id := tk["media_id"].(string)
	req, _ := nethttp.NewRequest("PUT", h.srv.URL+"/api/v1/media/"+id+"/content", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+h.key1)
	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 413 {
		t.Fatalf("want 413, got %d", resp.StatusCode)
	}
	if _, m, _ := h.call("GET", "/api/v1/media/"+id, h.key1, ""); m["status"] != "PENDING" {
		t.Fatalf("a rejected upload must not become READY: %v", m)
	}
	if _, err := h.env.Blob.Stat(context.Background(), tk["object_key"].(string)); err == nil {
		t.Fatal("a rejected upload left an object behind")
	}
}
