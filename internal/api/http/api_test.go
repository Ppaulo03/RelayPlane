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
	"time"

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
	// the bytes come back through the gateway, authenticated, with the checksum as ETag
	get := func(key string) (*nethttp.Response, []byte) {
		rq, _ := nethttp.NewRequest("GET", h.srv.URL+"/api/v1/media/"+id+"/content", nil)
		rq.Header.Set("Authorization", "Bearer "+key)
		rs, err := nethttp.DefaultClient.Do(rq)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Body.Close()
		body, _ := io.ReadAll(rs.Body)
		return rs, body
	}
	if rs, body := get(h.key1); rs.StatusCode != 200 || !bytes.Equal(body, data) || rs.Header.Get("ETag") != `"`+sum+`"` ||
		rs.Header.Get("Content-Type") != "application/pdf" || rs.Header.Get("X-Content-Type-Options") != "nosniff" ||
		!strings.Contains(rs.Header.Get("Content-Disposition"), "attachment") {
		t.Errorf("content: %d %v", rs.StatusCode, rs.Header)
	}
	if rs, _ := get(h.key2); rs.StatusCode != 404 {
		t.Errorf("another tenant must not read the bytes: %d", rs.StatusCode)
	}
	if rs, _ := get("rpk_wrong"); rs.StatusCode != 401 {
		t.Errorf("unauthenticated: %d", rs.StatusCode)
	}
	if c, _, _ := h.call("DELETE", "/api/v1/media/"+id, h.key1, ""); c != 204 {
		t.Errorf("delete: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/media/"+id, h.key1, ""); c != 404 {
		t.Errorf("deleted media still visible: %d", c)
	}
	if rs, _ := get(h.key1); rs.StatusCode != 404 {
		t.Errorf("deleted media content still readable: %d", rs.StatusCode)
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

func TestMetricsCanBeHiddenFromThePublicListener(t *testing.T) {
	e := systemtest.NewEnv(t)
	api := &apihttp.Server{App: e.App, Metrics: e.Metrics, HideMetrics: true,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	resp, err := nethttp.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != nethttp.StatusNotFound {
		t.Fatalf("public /metrics status = %d, want 404", resp.StatusCode)
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

func TestSubscriptionsAPI(t *testing.T) {
	h := newHarness(t)
	if c, _, _ := h.call("POST", "/api/v1/subscriptions", "", `{"url":"http://agent.local/h"}`); c != 401 {
		t.Errorf("no key: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/subscriptions", h.admin, `{"url":"http://agent.local/h"}`); c != 403 {
		t.Errorf("admin keys are not tenants: %d", c)
	}

	c, sub, _ := h.call("POST", "/api/v1/subscriptions", h.key1, `{"url":"http://agent.local/h","event_types":["message.received","message.outbound_status"]}`)
	if c != 201 {
		t.Fatalf("create: %d %v", c, sub)
	}
	id, _ := sub["id"].(string)
	secret, _ := sub["secret"].(string)
	if id == "" || !strings.HasPrefix(secret, "whsec_") {
		t.Fatalf("the signing secret is shown once, at creation: %v", sub)
	}
	types, _ := sub["event_types"].([]any)
	if len(types) != 2 || sub["active"] != true {
		t.Errorf("view: %v", sub)
	}

	// the secret is never shown again
	_, got, _ := h.call("GET", "/api/v1/subscriptions/"+id, h.key1, "")
	if _, leaked := got["secret"]; leaked || got["url"] != "http://agent.local/h" {
		t.Errorf("GET must not expose the secret: %v", got)
	}
	_, list, _ := h.call("GET", "/api/v1/subscriptions", h.key1, "")
	if raw, _ := json.Marshal(list); strings.Contains(string(raw), "whsec_") {
		t.Errorf("LIST leaked a secret: %s", raw)
	}

	// tenant isolation: another tenant sees nothing and can change nothing
	for _, call := range [][3]string{{"GET", "/api/v1/subscriptions/" + id, ""}, {"DELETE", "/api/v1/subscriptions/" + id, ""},
		{"POST", "/api/v1/subscriptions/" + id + "/rotate-secret", ""}, {"GET", "/api/v1/subscriptions/" + id + "/deliveries", ""}} {
		if c, _, _ := h.call(call[0], call[1], h.key2, call[2]); c != 404 {
			t.Errorf("TENANT ISOLATION %s %s: %d", call[0], call[1], c)
		}
	}
	if _, l2, _ := h.call("GET", "/api/v1/subscriptions", h.key2, ""); len(l2["subscriptions"].([]any)) != 0 {
		t.Errorf("TENANT ISOLATION on list: %v", l2)
	}

	// validation
	for name, body := range map[string]string{
		"scheme":        `{"url":"ftp://agent.local/h"}`,
		"credentials":   `{"url":"http://user:pw@agent.local/h"}`,
		"event type":    `{"url":"http://agent.local/h","event_types":["instance.qrcode_updated"]}`,
		"unknown field": `{"url":"http://agent.local/h","secret":"mine"}`,
		"foreign inst":  `{"url":"http://agent.local/h","instance_ids":["inst_does_not_exist"]}`,
	} {
		if c, _, _ := h.call("POST", "/api/v1/subscriptions", h.key1, body); c != 400 {
			t.Errorf("%s must be rejected with 400, got %d", name, c)
		}
	}

	// rotation: a new secret, the old one stays valid only for the grace period
	c, rot, _ := h.call("POST", "/api/v1/subscriptions/"+id+"/rotate-secret", h.key1, "")
	if c != 200 || rot["secret"] == secret || !strings.HasPrefix(rot["secret"].(string), "whsec_") || rot["previous_secret_valid_until"] == nil {
		t.Errorf("rotate: %d %v", c, rot)
	}

	_, dl, _ := h.call("GET", "/api/v1/subscriptions/"+id+"/deliveries?status=DEAD", h.key1, "")
	if ds, ok := dl["deliveries"].([]any); !ok || len(ds) != 0 {
		t.Errorf("deliveries: %v", dl)
	}
	if c, _, _ := h.call("GET", "/api/v1/subscriptions/"+id+"/deliveries?status=BOGUS", h.key1, ""); c != 400 {
		t.Errorf("unknown status: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/deliveries/dlv_nope/redeliver", h.key1, ""); c != 404 {
		t.Errorf("redeliver unknown: %d", c)
	}

	if c, _, _ := h.call("DELETE", "/api/v1/subscriptions/"+id, h.key1, ""); c != 204 {
		t.Errorf("delete: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/subscriptions/"+id, h.key1, ""); c != 404 {
		t.Errorf("deleted: %d", c)
	}
}

func TestSubscriptionsPerTenantLimit(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 10; i++ { // the default limit
		if c, _, _ := h.call("POST", "/api/v1/subscriptions", h.key1, `{"url":"http://agent.local/h"}`); c != 201 {
			t.Fatalf("create %d: %d", i, c)
		}
	}
	if c, _, _ := h.call("POST", "/api/v1/subscriptions", h.key1, `{"url":"http://agent.local/h"}`); c != 409 {
		t.Errorf("over the limit: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/subscriptions", h.key2, `{"url":"http://agent.local/h"}`); c != 201 {
		t.Errorf("the limit is per tenant: %d", c)
	}
}

func TestSubscriptionCreateIsIdempotent(t *testing.T) {
	h := newHarness(t)
	body := `{"url":"http://agent.local/h","event_types":["message.received","message.outbound_status"],"exclude_groups":true}`
	c, first, _ := h.call("POST", "/api/v1/subscriptions", h.key1, body, "Idempotency-Key", "bootstrap-1")
	if c != 201 || first["secret"] == nil || first["exclude_groups"] != true {
		t.Fatalf("first: %d %v", c, first)
	}
	// the same request again (a deploy script that runs twice): the same subscription, no second secret
	reordered := `{"exclude_groups":true,"event_types":["message.outbound_status","message.received"],"url":"http://agent.local/h"}`
	c, again, hdr := h.call("POST", "/api/v1/subscriptions", h.key1, reordered, "Idempotency-Key", "bootstrap-1")
	if c != 200 || again["id"] != first["id"] || hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %v %v", c, again, hdr)
	}
	if _, leaked := again["secret"]; leaked {
		t.Error("a replay must not re-expose the secret (rotate it if it was lost)")
	}
	_, list, _ := h.call("GET", "/api/v1/subscriptions", h.key1, "")
	if n := len(list["subscriptions"].([]any)); n != 1 {
		t.Errorf("no duplicate subscription: %d", n)
	}
	// the same key with a DIFFERENT request is a client bug
	if c, _, _ := h.call("POST", "/api/v1/subscriptions", h.key1, `{"url":"http://other.local/h"}`, "Idempotency-Key", "bootstrap-1"); c != 422 {
		t.Errorf("key reuse with another payload: %d", c)
	}
	// keys are per tenant
	if c, _, _ := h.call("POST", "/api/v1/subscriptions", h.key2, body, "Idempotency-Key", "bootstrap-1"); c != 201 {
		t.Errorf("another tenant may use the same key: %d", c)
	}
	// no key: each call creates one (the old behaviour)
	h.call("POST", "/api/v1/subscriptions", h.key1, body)
	if _, list, _ = h.call("GET", "/api/v1/subscriptions", h.key1, ""); len(list["subscriptions"].([]any)) != 2 {
		t.Errorf("without a key every call creates a subscription: %v", list)
	}
}

func TestLimitsEndpoint(t *testing.T) {
	h := newHarness(t)
	if c, _, _ := h.call("GET", "/api/v1/limits", "", ""); c != 401 {
		t.Errorf("auth required: %d", c)
	}
	c, l, _ := h.call("GET", "/api/v1/limits", h.key1, "")
	if c != 200 {
		t.Fatalf("%d %v", c, l)
	}
	if l["idempotency_retention_seconds"].(float64) != 24*3600 {
		t.Errorf("the idempotency window is the client's retry ceiling: %v", l["idempotency_retention_seconds"])
	}
	if l["max_text_length"].(float64) <= 0 {
		t.Errorf("max_text_length: %v", l)
	}
	media := l["media"].(map[string]any)
	if media["max_bytes"].(float64) <= 0 || media["inline_max_bytes"].(float64) <= 0 || len(media["allowed_types"].([]any)) == 0 {
		t.Errorf("media limits: %v", media)
	}
	subs := l["subscriptions"].(map[string]any)
	if subs["max_per_tenant"].(float64) != 10 || subs["retry_max_attempts"].(float64) != 10 || subs["retry_horizon_seconds"].(float64) < 15*3600 {
		t.Errorf("subscription limits: %v", subs)
	}
	if _, ok := l["send_rate_default"].(map[string]any); !ok {
		t.Errorf("send_rate_default: %v", l)
	}
}

func TestMessageViewExposesProviderIDAndAcceptanceTime(t *testing.T) {
	h := newHarness(t)
	h.env.StartWorkers(1)
	h.env.StartOutbox()
	_, body, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"a"}`)
	id := body["id"].(string)
	h.env.Connect(id)
	_, sent, _ := h.call("POST", "/api/v1/messages/send", h.key1, `{"instance_id":"`+id+`","to":"5562999999999","type":"text","payload":{"text":"oi"}}`)
	mid := sent["message_id"].(string)
	var view map[string]any
	deadline := 0
	for ; deadline < 400; deadline++ {
		_, view, _ = h.call("GET", "/api/v1/messages/"+mid, h.key1, "")
		if view["status"] == "ACCEPTED" {
			break
		}
	}
	if view["status"] != "ACCEPTED" {
		t.Fatalf("never accepted: %v", view)
	}
	if view["provider_message_id"] == nil || view["provider_message_id"] == "" || view["accepted_at"] == nil {
		t.Errorf("an ACCEPTED message exposes the provider id (what a reply_to refers to) and when it was accepted: %v", view)
	}
	_, queued, _ := h.call("GET", "/api/v1/messages/"+mid, h.key2, "")
	if _, leaked := queued["provider_message_id"]; leaked {
		t.Error("another tenant must not see it")
	}
}

// Rotating a credential without downtime: a second key is issued while the first still works, the old one is revoked
// from the new one, and from then on only the new one authenticates. A tenant can never revoke its way into a lock-out.
func TestAPIKeyRotationWithoutDowntime(t *testing.T) {
	h := newHarness(t)
	list := func(key string) []any {
		c, out, _ := h.call("GET", "/api/v1/api-keys", key, "")
		if c != 200 {
			t.Fatalf("list: %d %v", c, out)
		}
		return out["api_keys"].([]any)
	}
	first := list(h.key1)
	if len(first) != 1 || first[0].(map[string]any)["name"] != "initial" || first[0].(map[string]any)["current"] != true {
		t.Fatalf("a tenant starts with one key, the one it is using: %v", first)
	}
	oldID := first[0].(map[string]any)["id"].(string)

	// 1. issue the new key (the secret is shown once, with no-store)
	c, created, hdr := h.call("POST", "/api/v1/api-keys", h.key1, `{"name":"agent-prod"}`)
	if c != 201 || hdr.Get("Cache-Control") != "no-store" {
		t.Fatalf("create: %d %v %v", c, created, hdr)
	}
	newKey, newID := created["api_key"].(string), created["id"].(string)
	if !strings.HasPrefix(newKey, "rpk_") || created["prefix"] != newKey[:8] || created["name"] != "agent-prod" {
		t.Errorf("created key: %v", created)
	}
	// 2. both work during the transition
	for _, k := range []string{h.key1, newKey} {
		if c, _, _ := h.call("GET", "/api/v1/limits", k, ""); c != 200 {
			t.Errorf("a key must work during the rotation: %d", c)
		}
	}
	// 3. the listing shows who is who, last use, and never a secret
	ks := list(newKey)
	if len(ks) != 2 {
		t.Fatalf("keys: %v", ks)
	}
	for _, raw := range ks {
		k := raw.(map[string]any)
		if _, leaked := k["api_key"]; leaked || k["key_hash"] != nil {
			t.Errorf("a listing must never carry secrets: %v", k)
		}
		if k["id"] == newID && k["current"] != true || k["id"] == oldID && k["current"] == true {
			t.Errorf("current key flag: %v", k)
		}
		if k["last_used_at"] == nil {
			t.Errorf("both keys were just used: %v", k)
		}
	}
	// 4. revoke the old one with the new one: it stops at once
	if c, _, _ := h.call("DELETE", "/api/v1/api-keys/"+oldID, newKey, ""); c != 204 {
		t.Fatalf("revoke: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/limits", h.key1, ""); c != 401 {
		t.Errorf("a revoked key must be refused: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/limits", newKey, ""); c != 200 {
		t.Errorf("the new key keeps working: %d", c)
	}
	// 5. no lock-out: the last usable key cannot be revoked, not even by itself
	if c, body, _ := h.call("DELETE", "/api/v1/api-keys/"+newID, newKey, ""); c != 409 {
		t.Errorf("last key: %d %v", c, body)
	}
	// 6. isolation and validation
	if c, _, _ := h.call("DELETE", "/api/v1/api-keys/"+newID, h.key2, ""); c != 404 {
		t.Errorf("another tenant's key: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/api-keys", newKey, `{"name":""}`); c != 400 {
		t.Errorf("a key needs a name: %d", c)
	}
	// 7. the administrator can always give a tenant a way back in
	c, rescue, _ := h.call("POST", "/api/v1/tenants/"+h.tenantOf(newKey)+"/api-keys", h.admin, `{"name":"rescue","expires_in_seconds":3600}`)
	if c != 201 || rescue["expires_at"] == nil {
		t.Fatalf("admin issue: %d %v", c, rescue)
	}
	if c, _, _ := h.call("GET", "/api/v1/limits", rescue["api_key"].(string), ""); c != 200 {
		t.Errorf("rescue key: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/tenants/tenant_nope/api-keys", h.admin, `{"name":"x"}`); c != 404 {
		t.Errorf("unknown tenant: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/tenants/"+h.tenantOf(newKey)+"/api-keys", newKey, `{"name":"x"}`); c != 403 {
		t.Errorf("a tenant cannot mint keys through the admin route: %d", c)
	}
}

func (h *harness) tenantOf(key string) string {
	t, err := h.env.App.Tenants.Authenticate(context.Background(), key)
	if err != nil {
		h.t.Fatal(err)
	}
	return t.ID
}

// A tenant that spends its request budget is told to slow down (429 + Retry-After) while others keep working; the
// budget comes back with time.
func TestTenantRateLimit(t *testing.T) {
	e := systemtest.NewEnv(t)
	_, k1, _ := e.App.Tenants.Create(context.Background(), "noisy")
	_, k2, _ := e.App.Tenants.Create(context.Background(), "quiet")
	api := &apihttp.Server{App: e.App, Metrics: e.Metrics, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Auth:       apihttp.KeyAuthenticator{Tenants: e.App.Tenants, AdminKey: "admin-key"},
		TenantRate: apihttp.RateConfig{PerSecond: 2, Burst: 3}}
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	get := func(key string) *nethttp.Response {
		req, _ := nethttp.NewRequest("GET", srv.URL+"/api/v1/limits", nil)
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := nethttp.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	for i := 0; i < 3; i++ {
		if r := get(k1); r.StatusCode != 200 {
			t.Fatalf("request %d inside the burst: %d", i+1, r.StatusCode)
		}
	}
	r := get(k1)
	if r.StatusCode != 429 || r.Header.Get("Retry-After") == "" || r.Header.Get("RateLimit-Remaining") != "0" {
		t.Fatalf("over the budget: %d %v", r.StatusCode, r.Header)
	}
	if r := get(k2); r.StatusCode != 200 {
		t.Errorf("another tenant is not affected by a noisy one: %d", r.StatusCode)
	}
	if r := get("wrong-key"); r.StatusCode != 401 {
		t.Errorf("unauthenticated requests are not tenant traffic: %d", r.StatusCode)
	}
	time.Sleep(700 * time.Millisecond) // 2 tokens per second: one is back
	if r := get(k1); r.StatusCode != 200 {
		t.Errorf("the budget refills: %d", r.StatusCode)
	}
}

func TestTenantLimiterIsOffByDefault(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 200; i++ {
		if c, _, _ := h.call("GET", "/api/v1/limits", h.key1, ""); c != 200 {
			t.Fatalf("an unconfigured limiter must not limit: request %d got %d", i+1, c)
		}
	}
}

func TestSubscriptionPauseResumeAndBacklogOverHTTP(t *testing.T) {
	h := newHarness(t)
	c, sub, _ := h.call("POST", "/api/v1/subscriptions", h.key1, `{"url":"http://agent.local/hook"}`)
	if c != 201 {
		t.Fatalf("%d %v", c, sub)
	}
	id := sub["id"].(string)
	if sub["paused"] != false {
		t.Errorf("born running: %v", sub)
	}
	if c, _, _ := h.call("POST", "/api/v1/subscriptions/"+id+"/pause", h.key1, ""); c != 204 {
		t.Fatalf("pause: %d", c)
	}
	_, got, _ := h.call("GET", "/api/v1/subscriptions/"+id, h.key1, "")
	bl, _ := got["backlog"].(map[string]any)
	if got["paused"] != true || bl == nil || bl["pending"] != float64(0) {
		t.Errorf("paused subscription with its backlog: %v", got)
	}
	if c, _, _ := h.call("POST", "/api/v1/subscriptions/"+id+"/pause", h.key2, ""); c != 404 {
		t.Errorf("another tenant: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/subscriptions/"+id+"/resume", h.key1, ""); c != 204 {
		t.Fatalf("resume: %d", c)
	}
	if _, got, _ = h.call("GET", "/api/v1/subscriptions/"+id, h.key1, ""); got["paused"] != false {
		t.Errorf("resumed: %v", got)
	}
	if c, _, _ := h.call("POST", "/api/v1/subscriptions/sub_nope/pause", h.key1, ""); c != 404 {
		t.Errorf("unknown: %d", c)
	}
}

func TestContactErasureOverHTTP(t *testing.T) {
	h := newHarness(t)
	c, body, hdr := h.call("DELETE", "/api/v1/contacts/+55%20(62)%2099999-9999/data", h.key1, "")
	if c != 200 || hdr.Get("Cache-Control") != "no-store" {
		t.Fatalf("erase: %d %v %v", c, body, hdr)
	}
	for _, k := range []string{"messages_anonymized", "messages_cancelled", "events_deleted", "attachments_deleted", "erased_at"} {
		if _, ok := body[k]; !ok {
			t.Errorf("the report lacks %q: %v", k, body)
		}
	}
	if raw, _ := body["_raw"].(string); strings.Contains(raw, "999999999") {
		t.Errorf("the report must not repeat the number: %v", body)
	}
	if c, _, _ := h.call("DELETE", "/api/v1/contacts/not-a-number/data", h.key1, ""); c != 400 {
		t.Errorf("malformed number: %d", c)
	}
	if c, _, _ := h.call("DELETE", "/api/v1/contacts/5562999999999/data", "", ""); c != 401 {
		t.Errorf("unauthenticated: %d", c)
	}
	if c, _, _ := h.call("DELETE", "/api/v1/contacts/5562999999999/data", h.admin, ""); c != 403 {
		t.Errorf("an administrator is not a tenant: %d", c)
	}
}

func TestPresenceAndReadOverHTTP(t *testing.T) {
	h := newHarness(t)
	_, inst, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"a"}`)
	id := inst["id"].(string)
	h.env.Connect(id)

	if c, body, _ := h.call("POST", "/api/v1/instances/"+id+"/presence", h.key1, `{"to":"5562988887777","state":"composing","duration_ms":2000}`); c != 202 || body["accepted"] != true {
		t.Fatalf("presence: %d %v", c, body)
	}
	if c, _, _ := h.call("POST", "/api/v1/instances/"+id+"/presence", h.key1, `{"to":"5562988887777","state":"juggling"}`); c != 400 {
		t.Errorf("bad state: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/instances/"+id+"/presence", h.key2, `{"to":"5562988887777","state":"composing"}`); c != 404 {
		t.Errorf("another tenant: %d", c)
	}
	if c, body, _ := h.call("POST", "/api/v1/messages/read", h.key1, `{"instance_id":"`+id+`","chat":"5562988887777","provider_message_ids":["WA-1","WA-2"]}`); c != 200 || body["read"] != float64(2) {
		t.Fatalf("read: %d %v", c, body)
	}
	if c, _, _ := h.call("POST", "/api/v1/messages/read", h.key1, `{"instance_id":"`+id+`","chat":"5562988887777","provider_message_ids":[]}`); c != 400 {
		t.Errorf("empty read: %d", c)
	}
	if c, _, _ := h.call("POST", "/api/v1/messages/read", h.key2, `{"instance_id":"`+id+`","chat":"5562988887777","provider_message_ids":["WA-1"]}`); c != 404 {
		t.Errorf("another tenant: %d", c)
	}
}

func TestSendWithAQuotedReplyOverHTTP(t *testing.T) {
	h := newHarness(t)
	_, inst, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"a"}`)
	id := inst["id"].(string)
	h.env.Connect(id)
	c, body, _ := h.call("POST", "/api/v1/messages/send", h.key1,
		`{"instance_id":"`+id+`","to":"5562999999999","type":"text","payload":{"text":"Confirma?","reply_to":{"provider_message_id":"3EB0USER","text":"quero agendar"}}}`)
	if c != 202 {
		t.Fatalf("quoted send: %d %v", c, body)
	}
	if c, _, _ := h.call("POST", "/api/v1/messages/send", h.key1,
		`{"instance_id":"`+id+`","to":"5562999999999","type":"text","payload":{"text":"x","reply_to":{"provider_message_id":"a","message_id":"b"}}}`); c != 400 {
		t.Errorf("both ids: %d", c)
	}
}

func TestListMessagesByStatusOverHTTP(t *testing.T) {
	h := newHarness(t)
	_, inst, _ := h.call("POST", "/api/v1/instances", h.key1, `{"name":"a"}`)
	id := inst["id"].(string)
	h.env.Connect(id)
	c, body, _ := h.call("GET", "/api/v1/messages?status=UNKNOWN&instance_id="+id, h.key1, "")
	if c != 200 || body["messages"] == nil {
		t.Fatalf("list: %d %v", c, body)
	}
	if c, _, _ := h.call("GET", "/api/v1/messages?status=NOPE", h.key1, ""); c != 400 {
		t.Errorf("bad status: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/messages?status=UNKNOWN&limit=zero", h.key1, ""); c != 400 {
		t.Errorf("bad limit: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/messages?status=UNKNOWN&instance_id="+id, h.key2, ""); c != 404 {
		t.Errorf("another tenant's instance: %d", c)
	}
	if c, _, _ := h.call("GET", "/api/v1/messages?status=UNKNOWN", "", ""); c != 401 {
		t.Errorf("unauthenticated: %d", c)
	}
}
