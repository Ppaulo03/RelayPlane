package webhookout_test

import (
	"context"
	"errors"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/webhookout"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/ports"
)

var bg = context.Background()

type fixedResolver map[string][]string

func (f fixedResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	var out []net.IPAddr
	for _, s := range f[host] {
		out = append(out, net.IPAddr{IP: net.ParseIP(s)})
	}
	if len(out) == 0 {
		return nil, errors.New("no such host")
	}
	return out, nil
}

func TestSendPostsBodyAndHeaders(t *testing.T) {
	var gotBody, gotSig, gotCT, gotMethod string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotSig, gotCT, gotMethod = string(b), r.Header.Get("X-RelayPlane-Signature"), r.Header.Get("Content-Type"), r.Method
		w.WriteHeader(204)
	}))
	defer srv.Close()
	s := webhookout.New(webhookout.Config{AllowPrivate: true, AllowInsecure: true})
	st, err := s.Send(bg, ports.WebhookRequest{URL: srv.URL, Body: []byte(`{"a":1}`), Timeout: 2 * time.Second,
		Headers: map[string]string{"X-RelayPlane-Signature": "v1=abc", "Content-Type": "application/json"}})
	if err != nil || st != 204 {
		t.Fatalf("%d %v", st, err)
	}
	if gotBody != `{"a":1}` || gotSig != "v1=abc" || gotCT != "application/json" || gotMethod != "POST" {
		t.Errorf("request: %s %s %s %s", gotMethod, gotBody, gotSig, gotCT)
	}
}

// SSRF: by default nothing private is reachable, even when the URL itself looks innocent.
func TestSendRefusesNonPublicDestinations(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(nethttp.HandlerFunc(func(nethttp.ResponseWriter, *nethttp.Request) { hits.Add(1) }))
	defer srv.Close()
	s := webhookout.New(webhookout.Config{AllowInsecure: true}) // AllowPrivate false: production behaviour
	for _, u := range []string{srv.URL, "http://127.0.0.1:1/h", "http://169.254.169.254/latest/meta-data", "http://[::1]:1/h", "http://10.0.0.1/h", "http://localhost:1/h"} {
		_, err := s.Send(bg, ports.WebhookRequest{URL: u, Body: []byte("x"), Timeout: time.Second})
		if !errors.Is(err, errs.ErrDestinationBlocked) {
			t.Errorf("%s must be refused as a permanent error, got %v", u, err)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("a refused destination must never receive a request (%d hits)", hits.Load())
	}
}

// DNS rebinding: a name that was public when the subscription was created but resolves to a private address at
// delivery time must be refused, because the check runs on the IP actually dialled.
func TestSendDefeatsDNSRebinding(t *testing.T) {
	s := webhookout.New(webhookout.Config{AllowInsecure: true, Resolver: fixedResolver{"agent.example.com": {"192.168.1.10"}}})
	_, err := s.Send(bg, ports.WebhookRequest{URL: "http://agent.example.com/h", Body: []byte("x"), Timeout: time.Second})
	if !errors.Is(err, errs.ErrDestinationBlocked) {
		t.Fatalf("rebinding to a private address must be blocked: %v", err)
	}
	mixed := webhookout.New(webhookout.Config{AllowInsecure: true, Resolver: fixedResolver{"agent.example.com": {"10.0.0.7", "127.0.0.1"}}})
	if _, err := mixed.Send(bg, ports.WebhookRequest{URL: "http://agent.example.com/h", Body: []byte("x"), Timeout: time.Second}); !errors.Is(err, errs.ErrDestinationBlocked) {
		t.Fatalf("no public address at all: %v", err)
	}
}

func TestSendRejectsForbiddenSchemes(t *testing.T) {
	s := webhookout.New(webhookout.Config{AllowPrivate: true}) // AllowInsecure false
	for _, u := range []string{"http://example.com/h", "file:///etc/passwd", "gopher://x/", "ftp://x/h", "::bad"} {
		if _, err := s.Send(bg, ports.WebhookRequest{URL: u, Body: []byte("x")}); !errors.Is(err, errs.ErrDestinationBlocked) {
			t.Errorf("%s: %v", u, err)
		}
	}
}

// A redirect is a classic SSRF pivot: a public URL answering 302 -> http://169.254.169.254/. It is never followed.
func TestSendDoesNotFollowRedirects(t *testing.T) {
	var target atomic.Int32
	inner := httptest.NewServer(nethttp.HandlerFunc(func(nethttp.ResponseWriter, *nethttp.Request) { target.Add(1) }))
	defer inner.Close()
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		nethttp.Redirect(w, r, inner.URL, nethttp.StatusFound)
	}))
	defer srv.Close()
	s := webhookout.New(webhookout.Config{AllowPrivate: true, AllowInsecure: true})
	st, err := s.Send(bg, ports.WebhookRequest{URL: srv.URL, Body: []byte("x"), Timeout: 2 * time.Second})
	if err != nil || st != 302 {
		t.Fatalf("the 302 is reported as a (failed) delivery: %d %v", st, err)
	}
	if target.Load() != 0 {
		t.Fatal("the redirect target must never be contacted")
	}
}

func TestSendBoundsTimeAndResponse(t *testing.T) {
	slow := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}))
	defer slow.Close()
	s := webhookout.New(webhookout.Config{AllowPrivate: true, AllowInsecure: true})
	start := time.Now()
	if _, err := s.Send(bg, ports.WebhookRequest{URL: slow.URL, Body: []byte("x"), Timeout: 150 * time.Millisecond}); err == nil {
		t.Fatal("a slow destination must time out")
	}
	if time.Since(start) > time.Second {
		t.Errorf("the timeout was not honoured: %v", time.Since(start))
	}
	huge := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		_, _ = io.Copy(w, strings.NewReader(strings.Repeat("x", 10<<20)))
	}))
	defer huge.Close()
	small := webhookout.New(webhookout.Config{AllowPrivate: true, AllowInsecure: true, MaxResponseBytes: 1024})
	if st, err := small.Send(bg, ports.WebhookRequest{URL: huge.URL, Body: []byte("x"), Timeout: 2 * time.Second}); err != nil || st != 200 {
		t.Errorf("a huge response body is read only up to the cap: %d %v", st, err)
	}
}
