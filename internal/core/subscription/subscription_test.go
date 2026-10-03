package subscription

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
)

func ev(tenant, inst string, t events.Type) events.Event {
	return events.Event{EventID: "e1", EventType: t, TenantID: tenant, InstanceID: inst}
}

func TestMatchesRespectsTenantFiltersAndVisibility(t *testing.T) {
	s := Subscription{ID: "s", TenantID: "t1", Active: true}
	if !s.Matches(ev("t1", "i1", events.MessageReceived)) {
		t.Error("an unfiltered subscription gets every tenant-facing event")
	}
	if s.Matches(ev("t2", "i1", events.MessageReceived)) {
		t.Fatal("TENANT ISOLATION: another tenant's event must never match")
	}
	if s.Matches(ev("t1", "i1", events.InstanceQRCodeUpdated)) || s.Matches(ev("t1", "i1", events.OwnershipViolation)) {
		t.Error("QR and ownership events are internal and never delivered")
	}
	byType := Subscription{TenantID: "t1", Active: true, EventTypes: []events.Type{events.MessageOutboundStatus}}
	if byType.Matches(ev("t1", "i1", events.MessageReceived)) || !byType.Matches(ev("t1", "i1", events.MessageOutboundStatus)) {
		t.Error("event type filter")
	}
	byInst := Subscription{TenantID: "t1", Active: true, InstanceIDs: []string{"i2"}}
	if byInst.Matches(ev("t1", "i1", events.MessageReceived)) || !byInst.Matches(ev("t1", "i2", events.MessageReceived)) {
		t.Error("instance filter")
	}
	off := s
	off.Active = false
	if off.Matches(ev("t1", "i1", events.MessageReceived)) {
		t.Error("inactive subscriptions get nothing")
	}
	if (Subscription{Active: true}).Matches(ev("", "i1", events.MessageReceived)) {
		t.Error("a subscription without tenant matches nothing, even an event without tenant")
	}
}

func TestRetryPolicyBoundsAttempts(t *testing.T) {
	p := DefaultRetry()
	if p.MaxAttempts() != 10 {
		t.Fatalf("max attempts %d", p.MaxAttempts())
	}
	d1, ok := p.Next(1, 0)
	if !ok || d1 != 5*time.Second {
		t.Fatalf("first retry %v %v", d1, ok)
	}
	if d, _ := p.Next(1, 0.999); d <= d1 || d > d1+2*time.Second {
		t.Errorf("jitter spreads up to 20%%: %v", d)
	}
	if _, ok := p.Next(9, 0); !ok {
		t.Error("the 9th failure still has a retry (10th attempt)")
	}
	if _, ok := p.Next(10, 0); ok {
		t.Error("budget exhausted after the 10th attempt: DEAD")
	}
	if _, ok := p.Next(0, 0); ok {
		t.Error("Next needs a failure count >= 1")
	}
}

func TestSignatureContract(t *testing.T) {
	body := []byte(`{"event_id":"e1"}`)
	now := time.Unix(1_800_000_000, 0)
	sec := DeriveSecret([]byte("server-key"), "sub_1", 1)
	if !strings.HasPrefix(sec, "whsec_") || sec != DeriveSecret([]byte("server-key"), "sub_1", 1) {
		t.Fatal("secret derivation is deterministic")
	}
	if sec == DeriveSecret([]byte("server-key"), "sub_1", 2) || sec == DeriveSecret([]byte("server-key"), "sub_2", 1) || sec == DeriveSecret([]byte("other"), "sub_1", 1) {
		t.Fatal("a secret is specific to key, subscription and version")
	}
	sig := Sign(sec, now.Unix(), body)
	h := SignatureHeader(sig)
	if err := Verify([]string{sec}, h, now.Unix(), body, now, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := Verify([]string{sec}, h, now.Unix(), []byte(`{"event_id":"e2"}`), now, 5*time.Minute); !errors.Is(err, ErrBadSignature) {
		t.Error("a tampered body must fail")
	}
	if err := Verify([]string{"whsec_wrong"}, h, now.Unix(), body, now, 5*time.Minute); !errors.Is(err, ErrBadSignature) {
		t.Error("a wrong secret must fail")
	}
	if err := Verify([]string{sec}, h, now.Unix(), body, now.Add(time.Hour), 5*time.Minute); !errors.Is(err, ErrBadSignature) {
		t.Error("an old timestamp must fail (replay protection)")
	}
	if err := Verify([]string{sec}, SignatureHeader(Sign("whsec_old", now.Unix(), body), sig), now.Unix(), body, now, time.Minute); err != nil {
		t.Errorf("during a rotation the header carries both signatures and either secret verifies: %v", err)
	}
	if err := Verify([]string{sec}, "v2="+sig, now.Unix(), body, now, time.Minute); err == nil {
		t.Error("unknown signature versions are not accepted")
	}
}

func TestValidateURL(t *testing.T) {
	for _, bad := range []string{"", "ftp://x.example.com/h", "http://x.example.com/h", "https://user:pw@x.example.com/h", "https://x.example.com/h#f",
		"https://127.0.0.1/h", "https://10.0.0.5/h", "https://169.254.169.254/latest", "https://[::1]/h", "https://localhost/h", "https://svc.internal/h",
		"https://" + strings.Repeat("a", 2100) + ".com/h", "not a url", "https:///nohost"} {
		if err := ValidateURL(bad, false, false); !errors.Is(err, errs.ErrInvalidArgument) {
			t.Errorf("%q must be rejected, got %v", bad, err)
		}
	}
	if err := ValidateURL("https://agent.example.com/hooks/relayplane", false, false); err != nil {
		t.Errorf("a public https url is valid: %v", err)
	}
	if err := ValidateURL("http://localhost:9000/hook", true, true); err != nil {
		t.Errorf("development mode allows http://localhost: %v", err)
	}
}

func TestIsPublicIP(t *testing.T) {
	for ip, want := range map[string]bool{"8.8.8.8": true, "1.1.1.1": true, "127.0.0.1": false, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false,
		"169.254.169.254": false, "100.64.0.1": false, "0.0.0.0": false, "224.0.0.1": false, "::1": false, "fe80::1": false, "fc00::1": false, "2606:4700::1111": true} {
		if got := IsPublicIP(net.ParseIP(ip)); got != want {
			t.Errorf("%s: %v want %v", ip, got, want)
		}
	}
}

// Golden vector shared with the Python SDK (sdk/python/tests/test_webhooks.py): both sides must compute the same
// signature, or no consumer could ever verify a delivery.
func TestSignatureGoldenVectorMatchesThePythonSDK(t *testing.T) {
	const want = "a5433ca61a8029297236aed569798e0ad71a192250b77b5d17f38893f1e1bf38"
	if got := Sign("whsec_golden", 1800000000, []byte(`{"event_id":"evt_1","payload":{"text":"oi"}}`)); got != want {
		t.Fatalf("signature changed: %s (the SDK and every consumer would break)", got)
	}
}
