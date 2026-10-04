// Package subscription models tenant-facing event delivery: a subscription (where and which events), a delivery
// (one event owed to one subscription), the retry schedule and the request signature. It is pure domain: no I/O.
package subscription

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
)

// Subscription tells RelayPlane where to POST which events of one tenant.
type Subscription struct {
	ID       string
	TenantID string
	URL      string
	// EventTypes restricts the events delivered (empty: every tenant-facing event).
	EventTypes []events.Type
	// InstanceIDs restricts the instances (empty: every instance of the tenant).
	InstanceIDs []string
	// SecretVersion selects the signing secret (it is derived, never stored). Rotation increments it.
	SecretVersion int
	RotatedAt     time.Time
	Active        bool
	// Paused keeps the subscription receiving events (deliveries accumulate, none is sent) until it is resumed: the
	// consumer's own backpressure. Unlike Active=false, nothing is lost.
	Paused bool
	// ExcludeGroups drops events of group chats (message.received with group=true).
	ExcludeGroups bool
	CreatedAt     time.Time
}

// Matches reports whether the subscription wants ev. The tenant check is the isolation boundary: an event of
// another tenant never matches, whatever the filters say.
func (s Subscription) Matches(ev events.Event) bool {
	if !s.Active || s.TenantID == "" || ev.TenantID != s.TenantID || !TenantFacing(ev.EventType) {
		return false
	}
	if len(s.EventTypes) > 0 && !containsType(s.EventTypes, ev.EventType) {
		return false
	}
	if s.ExcludeGroups && events.IsGroupMessage(ev) {
		return false
	}
	if len(s.InstanceIDs) > 0 {
		for _, id := range s.InstanceIDs {
			if id == ev.InstanceID {
				return true
			}
		}
		return false
	}
	return true
}

func containsType(list []events.Type, t events.Type) bool {
	for _, x := range list {
		if x == t {
			return true
		}
	}
	return false
}

// TenantFacing reports which event types a tenant may subscribe to. QR codes and ownership violations are
// operational/internal: they never leave the platform.
func TenantFacing(t events.Type) bool {
	switch t {
	case events.MessageReceived, events.MessageStatus, events.MessageOutboundStatus, events.MessageDeleted, events.InstanceStatusChanged:
		return true
	}
	return false
}

// TenantFacingTypes lists them (stable order).
func TenantFacingTypes() []events.Type {
	return []events.Type{events.MessageReceived, events.MessageOutboundStatus, events.MessageStatus, events.MessageDeleted, events.InstanceStatusChanged}
}

// DeliveryStatus is the state of one delivery.
type DeliveryStatus string

const (
	DeliveryPending   DeliveryStatus = "PENDING"
	DeliveryDelivered DeliveryStatus = "DELIVERED"
	DeliveryDead      DeliveryStatus = "DEAD" // retries exhausted (or the destination is not allowed): the DLQ
)

// Delivery is one event owed to one subscription.
type Delivery struct {
	ID             string
	SubscriptionID string
	TenantID       string
	InstanceID     string
	EventID        string
	EventType      events.Type
	Event          events.Event
	// Sequence is assigned when the delivery is created (Enqueue): 1, 2, 3... per (subscription, instance), gapless.
	// A redelivery keeps it.
	Sequence int64
	Status   DeliveryStatus
	// Attempts counts recorded failed attempts.
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	DeliveredAt   time.Time
}

// RetryPolicy is the backoff between failed attempts. After the schedule is exhausted the delivery is DEAD.
type RetryPolicy struct {
	Schedule []time.Duration
	// Jitter is the fraction (0..1) of random spread added to each delay.
	Jitter float64
}

// DefaultRetry spreads 10 attempts over roughly 16 hours.
func DefaultRetry() RetryPolicy {
	return RetryPolicy{
		Schedule: []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute,
			time.Hour, 2 * time.Hour, 4 * time.Hour, 8 * time.Hour},
		Jitter: 0.2,
	}
}

// MaxAttempts is the total number of attempts (the first one plus one per schedule entry).
func (p RetryPolicy) MaxAttempts() int { return len(p.Schedule) + 1 }

// Next returns the delay before the next attempt, given how many attempts have already failed (>= 1).
// ok is false when the budget is exhausted. rnd in [0,1) injects the jitter (tests pass a constant).
func (p RetryPolicy) Next(failed int, rnd float64) (time.Duration, bool) {
	if failed < 1 || failed > len(p.Schedule) {
		return 0, false
	}
	d := p.Schedule[failed-1]
	return d + time.Duration(float64(d)*p.Jitter*rnd), true
}

// ---- signing ----

const (
	HeaderEventID   = "X-RelayPlane-Event-Id"
	HeaderEventType = "X-RelayPlane-Event-Type"
	HeaderTimestamp = "X-RelayPlane-Timestamp"
	HeaderSignature = "X-RelayPlane-Signature"
	HeaderAttempt   = "X-RelayPlane-Delivery-Attempt"
	// RotationGrace is how long the previous secret keeps being used to sign (alongside the new one) after a rotation,
	// so a consumer can switch without a gap.
	RotationGrace = 24 * time.Hour
)

// DeriveSecret returns the signing secret of a subscription version. Secrets are derived from a server key, so the
// database never holds them; the value is shown to the tenant exactly once (creation / rotation).
func DeriveSecret(serverKey []byte, subscriptionID string, version int) string {
	m := hmac.New(sha256.New, serverKey)
	m.Write([]byte("relayplane/subscription-secret/v1|" + subscriptionID + "|" + strconv.Itoa(version)))
	return "whsec_" + hex.EncodeToString(m.Sum(nil))
}

// Sign computes HMAC-SHA256(secret, "<timestamp>.<body>") as lowercase hex.
func Sign(secret string, timestamp int64, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(strconv.FormatInt(timestamp, 10)))
	m.Write([]byte("."))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// SignatureHeader renders one or more signatures: "v1=<hex>,v1=<hex>".
func SignatureHeader(sigs ...string) string {
	parts := make([]string, len(sigs))
	for i, s := range sigs {
		parts[i] = "v1=" + s
	}
	return strings.Join(parts, ",")
}

// ErrBadSignature is returned by Verify.
var ErrBadSignature = errors.New("invalid webhook signature")

// Verify is the reference verification for consumers (and the contract our own tests pin): the timestamp must be
// within tolerance of now (replay protection) and any of the v1 signatures must match any of the secrets.
func Verify(secrets []string, header string, timestamp int64, body []byte, now time.Time, tolerance time.Duration) error {
	if d := now.Sub(time.Unix(timestamp, 0)); d > tolerance || d < -tolerance {
		return fmt.Errorf("%w: timestamp outside the tolerance", ErrBadSignature)
	}
	for _, part := range strings.Split(header, ",") {
		got, ok := strings.CutPrefix(strings.TrimSpace(part), "v1=")
		if !ok {
			continue
		}
		for _, sec := range secrets {
			if hmac.Equal([]byte(got), []byte(Sign(sec, timestamp, body))) {
				return nil
			}
		}
	}
	return ErrBadSignature
}

// ---- destination validation (static part; the dial-time check lives in the sender adapter) ----

// ValidateURL checks what can be checked without the network: https (http only when allowInsecure), a host, no
// credentials, no fragment, a sane length and no literal private/loopback address (unless allowPrivate).
func ValidateURL(raw string, allowInsecure, allowPrivate bool) error {
	if len(raw) > 2048 {
		return fmt.Errorf("%w: url too long", errs.ErrInvalidArgument)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%w: url is not valid", errs.ErrInvalidArgument)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !allowInsecure {
			return fmt.Errorf("%w: url must use https", errs.ErrInvalidArgument)
		}
	default:
		return fmt.Errorf("%w: url scheme %q is not allowed", errs.ErrInvalidArgument, u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("%w: credentials in the url are not allowed", errs.ErrInvalidArgument)
	}
	if u.Fragment != "" {
		return fmt.Errorf("%w: url fragment is not allowed", errs.ErrInvalidArgument)
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && !allowPrivate && !IsPublicIP(ip) {
		return fmt.Errorf("%w: destination address is not public", errs.ErrInvalidArgument)
	}
	if h := strings.ToLower(u.Hostname()); !allowPrivate && (h == "localhost" || strings.HasSuffix(h, ".localhost") || strings.HasSuffix(h, ".internal")) {
		return fmt.Errorf("%w: destination host is not public", errs.ErrInvalidArgument)
	}
	return nil
}

// IsPublicIP is false for loopback, private (RFC 1918 / ULA), link-local (incl. cloud metadata 169.254.169.254),
// multicast, unspecified and carrier-grade NAT addresses.
func IsPublicIP(ip net.IP) bool {
	if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || ip.IsInterfaceLocalMulticast() {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 100 && v4[1]&0xC0 == 64 { // 100.64.0.0/10 CGNAT
			return false
		}
		if v4[0] == 192 && v4[1] == 0 && v4[2] == 0 { // 192.0.0.0/24 IETF protocol assignments
			return false
		}
	}
	return true
}

// SigningSecrets returns the secrets a request to sub must be signed with: the current one and, within the
// rotation grace, the previous one (so a consumer can switch secrets without a gap).
func SigningSecrets(serverKey []byte, sub Subscription, now time.Time) []string {
	out := []string{DeriveSecret(serverKey, sub.ID, sub.SecretVersion)}
	if sub.SecretVersion > 1 && !sub.RotatedAt.IsZero() && now.Sub(sub.RotatedAt) < RotationGrace {
		out = append(out, DeriveSecret(serverKey, sub.ID, sub.SecretVersion-1))
	}
	return out
}
