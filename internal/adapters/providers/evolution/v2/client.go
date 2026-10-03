// Package v2 is the Evolution API v2 adapter. It is the ONLY place that knows
// Evolution's HTTP routes, DTOs, webhook payloads and error shapes: everything
// that leaves this package is a canonical RelayPlane type or error.
package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/relayplane/relayplane/internal/core/errs"
)

// ProviderKey is the provider identifier stored on instances and nodes.
const ProviderKey = "evolution-v2"

// SupportedMajor is the Evolution major version this adapter is written for.
const SupportedMajor = "2"

// NodeInfo locates one Evolution node (one singleton container).
type NodeInfo struct {
	BaseURL string // e.g. http://evolution-node-01:8080
	APIKey  string // node-level AUTHENTICATION_API_KEY
}

// NodeLocator resolves a RelayPlane node id to its Evolution endpoint.
type NodeLocator interface {
	Locate(ctx context.Context, nodeID string) (NodeInfo, error)
}

// StaticNodes is a fixed node map (tests, simple deployments).
type StaticNodes map[string]NodeInfo

// Locate implements NodeLocator.
func (s StaticNodes) Locate(_ context.Context, id string) (NodeInfo, error) {
	n, ok := s[id]
	if !ok {
		return NodeInfo{}, fmt.Errorf("%w: unknown node %q", errs.ErrNotFound, id)
	}
	return n, nil
}

// Config configures the adapter.
type Config struct {
	Nodes          NodeLocator
	WebhookBaseURL string // public gateway URL Evolution calls back, e.g. http://gateway:8080
	WebhookSecret  string // derives the per-node webhook token
	HTTPClient     *http.Client
	Timeout        time.Duration // per request, default 20s
	// AllowedVersions overrides TestedVersions (exact Evolution versions accepted by ProbeNode).
	AllowedVersions []string
}

type client struct {
	cfg  Config
	http *http.Client
}

func newClient(cfg Config) *client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 20 * time.Second
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	}
	return &client{cfg: cfg, http: hc}
}

// opKind drives error classification (a send whose outcome is unknown is
// ambiguous; a management call that fails is simply unavailable).
type opKind int

const (
	opManage opKind = iota
	opSend
)

type apiResponse struct {
	status int
	body   []byte
}

// do performs a request against a node and translates failures.
func (c *client) do(ctx context.Context, nodeID, method, path string, query url.Values, in any, kind opKind, authenticated bool) (*apiResponse, error) {
	var node NodeInfo
	if nodeID != "" {
		var err error
		if node, err = c.cfg.Nodes.Locate(ctx, nodeID); err != nil {
			return nil, fmt.Errorf("%w: %v", errs.ErrProviderUnavailable, err)
		}
	}
	u := strings.TrimRight(node.BaseURL, "/") + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authenticated {
		req.Header.Set("apikey", node.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, transportError(err, kind)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil { // the response was cut short: the call may have been executed
		return nil, transportError(err, kind)
	}
	out := &apiResponse{status: resp.StatusCode, body: raw}
	if resp.StatusCode >= 400 {
		return out, translateStatus(resp.StatusCode, raw, kind)
	}
	return out, nil
}

// transportError classifies network failures. Failing to dial means nothing
// reached the node (safe to retry); anything later is ambiguous for sends.
func transportError(err error, kind opKind) error {
	var oe *net.OpError
	if errors.As(err, &oe) && oe.Op == "dial" {
		return fmt.Errorf("%w: %v", errs.ErrProviderUnavailable, err)
	}
	var de *net.DNSError
	if errors.As(err, &de) {
		return fmt.Errorf("%w: %v", errs.ErrProviderUnavailable, err)
	}
	if kind == opSend {
		return fmt.Errorf("%w: %v", errs.ErrAmbiguousDispatch, err)
	}
	return fmt.Errorf("%w: %v", errs.ErrProviderUnavailable, err)
}

// evolutionError is the (loosely structured) error body Evolution returns.
type evolutionError struct {
	Status   int             `json:"status"`
	Error    string          `json:"error"`
	Message  json.RawMessage `json:"message"`
	Response struct {
		Message json.RawMessage `json:"message"`
	} `json:"response"`
}

func (e evolutionError) text() string {
	var parts []string
	for _, raw := range []json.RawMessage{e.Message, e.Response.Message} {
		if len(raw) == 0 {
			continue
		}
		var s string
		var arr []any
		switch {
		case json.Unmarshal(raw, &s) == nil:
			parts = append(parts, s)
		case json.Unmarshal(raw, &arr) == nil:
			for _, a := range arr {
				parts = append(parts, fmt.Sprint(a))
			}
		}
	}
	return strings.ToLower(strings.Join(parts, " "))
}

// translateStatus maps Evolution HTTP errors to canonical errors.
func translateStatus(status int, body []byte, kind opKind) error {
	var ee evolutionError
	_ = json.Unmarshal(body, &ee)
	msg := ee.text()
	short := msg
	if len(short) > 200 {
		short = short[:200]
	}
	wrap := func(base error) error { return fmt.Errorf("%w: evolution %d: %s", base, status, short) }
	switch {
	case status == http.StatusUnauthorized:
		return wrap(errs.ErrAuthenticationFailed)
	case status == http.StatusForbidden && strings.Contains(msg, "already in use"):
		return wrap(errs.ErrInstanceAlreadyExists)
	case status == http.StatusForbidden:
		return wrap(errs.ErrAuthenticationFailed)
	case status == http.StatusNotFound || (status == http.StatusBadRequest && strings.Contains(msg, "does not exist")):
		return wrap(errs.ErrInstanceNotFound)
	case status == http.StatusBadRequest && strings.Contains(msg, "exists") && strings.Contains(msg, "false"):
		return wrap(errs.ErrInvalidRecipient)
	case status == http.StatusBadRequest && (strings.Contains(msg, "not connected") || strings.Contains(msg, "connection closed") || strings.Contains(msg, "closed")):
		return wrap(errs.ErrProviderUnavailable) // socket down: the node did not send anything
	case status == http.StatusTooManyRequests:
		return wrap(errs.ErrProviderUnavailable) // throttled before the node accepted the request
	case status >= 500:
		// A gateway/proxy error (502/503/504) can be returned *after* the node
		// executed the send and the response was lost: a send is ambiguous, never
		// blindly retried. Management calls are idempotent, so they stay retryable.
		if kind == opSend {
			return wrap(errs.ErrAmbiguousDispatch)
		}
		return wrap(errs.ErrProviderUnavailable)
	}
	return wrap(errs.ErrProviderRejected)
}

func (c *client) decode(r *apiResponse, v any) error {
	if err := json.Unmarshal(r.body, v); err != nil {
		return fmt.Errorf("%w: undecodable evolution response: %v", errs.ErrProviderRejected, err)
	}
	return nil
}
