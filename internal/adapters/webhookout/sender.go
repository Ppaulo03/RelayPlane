// Package webhookout is the HTTP adapter of ports.WebhookSender: it POSTs signed events to tenant-controlled URLs.
//
// The destination is untrusted input, so this is an SSRF boundary: the address is validated at DIAL time, on the
// IP the connection will actually use (not on the hostname that was checked earlier), which defeats DNS rebinding.
// Redirects are never followed, proxies from the environment are ignored and the response is read only up to a cap.
package webhookout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/url"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/subscription"
	"github.com/relayplane/relayplane/internal/ports"
)

// Config configures the sender.
type Config struct {
	// AllowPrivate lets requests reach loopback/private/link-local addresses (development only).
	AllowPrivate bool
	// AllowInsecure lets requests use http:// (development only).
	AllowInsecure bool
	// MaxResponseBytes caps how much of the response body is read (default 64 KiB).
	MaxResponseBytes int64
	// DialTimeout bounds connection establishment (default 3s).
	DialTimeout time.Duration
	// Resolver overrides DNS resolution (tests).
	Resolver Resolver
}

// Resolver resolves a host to IPs.
type Resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// Sender implements ports.WebhookSender.
type Sender struct {
	cfg    Config
	client *nethttp.Client
}

// New builds the sender.
func New(cfg Config) *Sender {
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = 64 << 10
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	if cfg.Resolver == nil {
		cfg.Resolver = net.DefaultResolver
	}
	s := &Sender{cfg: cfg}
	dialer := &net.Dialer{Timeout: cfg.DialTimeout}
	tr := &nethttp.Transport{
		Proxy: nil, // never route tenant traffic through an ambient proxy
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := cfg.Resolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, fmt.Errorf("resolve %s: %w", host, err)
			}
			var lastErr error
			for _, ip := range ips {
				if !cfg.AllowPrivate && !subscription.IsPublicIP(ip.IP) {
					lastErr = fmt.Errorf("%w: %s resolves to a non-public address", errs.ErrDestinationBlocked, host)
					continue
				}
				c, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
				if err == nil {
					return c, nil
				}
				lastErr = err
			}
			if lastErr == nil {
				lastErr = fmt.Errorf("resolve %s: no addresses", host)
			}
			return nil, lastErr
		},
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       30 * time.Second,
		DisableCompression:    true,
	}
	s.client = &nethttp.Client{
		Transport:     tr,
		CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return nethttp.ErrUseLastResponse },
	}
	return s
}

// Send implements ports.WebhookSender.
func (s *Sender) Send(ctx context.Context, req ports.WebhookRequest) (int, error) {
	u, err := url.Parse(req.URL)
	if err != nil || u.Host == "" {
		return 0, fmt.Errorf("%w: invalid url", errs.ErrDestinationBlocked)
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && s.cfg.AllowInsecure) {
		return 0, fmt.Errorf("%w: scheme %q", errs.ErrDestinationBlocked, u.Scheme)
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	hr, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return 0, err
	}
	for k, v := range req.Headers {
		hr.Header.Set(k, v)
	}
	resp, err := s.client.Do(hr)
	if err != nil {
		if errors.Is(err, errs.ErrDestinationBlocked) {
			return 0, err
		}
		var ue *url.Error
		if errors.As(err, &ue) && errors.Is(ue.Err, errs.ErrDestinationBlocked) {
			return 0, ue.Err
		}
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, s.cfg.MaxResponseBytes))
	return resp.StatusCode, nil
}
