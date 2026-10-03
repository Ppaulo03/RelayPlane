// Package http is the public REST API. It never mentions provider details and
// derives the tenant from authentication, never from request payloads.
package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	nethttp "net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// Role is the caller's role. Only two exist today; the type leaves room for RBAC.
type Role string

const (
	RoleTenant Role = "tenant"
	RoleAdmin  Role = "admin"
)

// Principal is the authenticated caller.
type Principal struct {
	Role     Role
	TenantID string // empty for admins
}

type ctxKey struct{}

// Authenticator resolves a bearer token to a Principal.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (Principal, error)
}

// KeyAuthenticator authenticates tenant API keys and a single admin key.
type KeyAuthenticator struct {
	Tenants  *app.TenantService
	AdminKey string
}

// Authenticate implements Authenticator.
func (a KeyAuthenticator) Authenticate(ctx context.Context, token string) (Principal, error) {
	if token == "" {
		return Principal{}, errs.ErrUnauthenticated
	}
	if a.AdminKey != "" && subtle.ConstantTimeCompare([]byte(token), []byte(a.AdminKey)) == 1 {
		return Principal{Role: RoleAdmin}, nil
	}
	t, err := a.Tenants.Authenticate(ctx, token)
	if err != nil {
		return Principal{}, err
	}
	return Principal{Role: RoleTenant, TenantID: t.ID}, nil
}

// ReadyCheck is one readiness dependency.
type ReadyCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// Server wires the HTTP API.
type Server struct {
	App       *app.App
	Auth      Authenticator
	Metrics   *observability.Metrics
	Log       *slog.Logger
	Ready     []ReadyCheck
	MaxUpload int64 // bytes accepted by PUT /media/{id}/content
}

const maxJSONBody = 1 << 20

// Handler builds the router.
func (s *Server) Handler() nethttp.Handler {
	mux := nethttp.NewServeMux()
	tenant := func(h func(nethttp.ResponseWriter, *nethttp.Request, Principal)) nethttp.HandlerFunc {
		return s.authed(RoleTenant, h)
	}
	admin := func(h func(nethttp.ResponseWriter, *nethttp.Request, Principal)) nethttp.HandlerFunc {
		return s.authed(RoleAdmin, h)
	}

	mux.HandleFunc("POST /api/v1/instances", tenant(s.createInstance))
	mux.HandleFunc("GET /api/v1/instances", tenant(s.listInstances))
	mux.HandleFunc("GET /api/v1/instances/{id}", tenant(s.getInstance))
	mux.HandleFunc("DELETE /api/v1/instances/{id}", tenant(s.deleteInstance))
	mux.HandleFunc("GET /api/v1/instances/{id}/qrcode", tenant(s.pairing(app.PairingQR)))
	mux.HandleFunc("GET /api/v1/instances/{id}/pairing-code", tenant(s.pairing(app.PairingCode)))
	mux.HandleFunc("POST /api/v1/instances/{id}/reconnect", tenant(s.reconnect))
	mux.HandleFunc("POST /api/v1/instances/{id}/logout", tenant(s.logout))
	mux.HandleFunc("POST /api/v1/instances/{id}/migrate", tenant(s.migrate))
	mux.HandleFunc("PUT /api/v1/instances/{id}/rate-policy", tenant(s.setInstanceRatePolicy))

	mux.HandleFunc("POST /api/v1/messages/send", tenant(s.sendMessage))
	mux.HandleFunc("GET /api/v1/messages/{id}", tenant(s.getMessage))
	mux.HandleFunc("GET /api/v1/operations/{id}", tenant(s.getOperation))

	mux.HandleFunc("POST /api/v1/media/uploads", tenant(s.createUpload))
	mux.HandleFunc("PUT /api/v1/media/{id}/content", tenant(s.uploadContent))
	mux.HandleFunc("GET /api/v1/media/{id}", tenant(s.getMedia))
	mux.HandleFunc("DELETE /api/v1/media/{id}", tenant(s.deleteMedia))

	mux.HandleFunc("GET /api/v1/nodes", admin(s.listNodes))
	mux.HandleFunc("POST /api/v1/nodes/{id}/drain", admin(s.drainNode))
	mux.HandleFunc("POST /api/v1/nodes/{id}/resume", admin(s.resumeNode))
	mux.HandleFunc("POST /api/v1/tenants", admin(s.createTenant))
	mux.HandleFunc("PUT /api/v1/tenants/{id}/rate-policy", admin(s.setTenantRatePolicy))

	mux.HandleFunc("POST /webhooks/{provider}", s.webhook)

	mux.HandleFunc("GET /health/live", func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		writeJSON(w, 200, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Handler())
	}

	var h nethttp.Handler = mux
	h = s.instrument(h, func(r *nethttp.Request) string { _, p := mux.Handler(r); return p })
	return otelhttp.NewHandler(h, "relayplane.http", otelhttp.WithSpanNameFormatter(func(_ string, r *nethttp.Request) string {
		return r.Method + " " + r.URL.Path
	}))
}

// ---- middleware ----

type statusWriter struct {
	nethttp.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (s *Server) instrument(next nethttp.Handler, pattern func(*nethttp.Request) string) nethttp.Handler {
	return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = ids.New("req")
		}
		w.Header().Set("X-Request-Id", rid)
		ctx := observability.With(r.Context(), "request_id", rid)
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r.WithContext(ctx))
		route := pattern(r)
		if route == "" {
			route = "unmatched"
		}
		if s.Metrics != nil {
			s.Metrics.HTTPRequests.WithLabelValues(route, r.Method, statusClass(sw.code)).Inc()
			s.Metrics.HTTPLatency.WithLabelValues(route).Observe(time.Since(start).Seconds())
		}
		if r.URL.Path != "/metrics" && r.URL.Path != "/health/live" {
			s.Log.LogAttrs(ctx, levelFor(sw.code), "http request", slog.String("method", r.Method), slog.String("route", route),
				slog.Int("status", sw.code), slog.Duration("duration", time.Since(start)))
		}
	})
}

func levelFor(code int) slog.Level {
	if code >= 500 {
		return slog.LevelError
	}
	return slog.LevelInfo
}

func statusClass(code int) string {
	return string(rune('0'+code/100)) + "xx"
}

func bearer(r *nethttp.Request) string {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
	return r.Header.Get("X-API-Key")
}

func (s *Server) authed(role Role, h func(nethttp.ResponseWriter, *nethttp.Request, Principal)) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		p, err := s.Auth.Authenticate(r.Context(), bearer(r))
		if err != nil {
			writeError(w, r, s.Log, errs.ErrUnauthenticated)
			return
		}
		if p.Role != role {
			writeError(w, r, s.Log, errs.ErrForbidden)
			return
		}
		ctx := observability.With(r.Context(), observability.KeyTenantID, p.TenantID)
		h(w, r.WithContext(context.WithValue(ctx, ctxKey{}, p)), p)
	}
}

// ---- helpers ----

func writeJSON(w nethttp.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// decode parses a strict JSON body (unknown fields are rejected: inline
// base64 media and other smuggled fields never pass silently).
func decode(r *nethttp.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxJSONBody+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return errs.Wrap(errs.ErrInvalidArgument, "request body required")
		}
		return errs.Wrap(errs.ErrInvalidArgument, "invalid JSON body: %v", err)
	}
	if dec.More() {
		return errs.Wrap(errs.ErrInvalidArgument, "unexpected data after JSON body")
	}
	return nil
}

func (s *Server) ready(w nethttp.ResponseWriter, r *nethttp.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	status := map[string]string{}
	code := 200
	for _, c := range s.Ready {
		if err := c.Check(ctx); err != nil {
			status[c.Name] = "unavailable"
			code = 503
			s.Log.WarnContext(ctx, "readiness check failed", "check", c.Name, "error", err)
		} else {
			status[c.Name] = "ok"
		}
	}
	writeJSON(w, code, map[string]any{"status": map[bool]string{true: "ready", false: "not_ready"}[code == 200], "checks": status})
}

var _ ports.InboundRequest
