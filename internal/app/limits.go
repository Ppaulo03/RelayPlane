package app

import (
	"github.com/relayplane/relayplane/internal/core/subscription"
)

// Limits is what a client can assert at startup instead of hard-coding assumptions: how long an Idempotency-Key is
// honoured (a sender must not retry beyond it), the size and rate limits, and the webhook retry horizon.
type Limits struct {
	// IdempotencyRetentionSeconds: a request repeated with the same Idempotency-Key inside this window returns the
	// original result; after it, the same key creates a NEW resource. A sender's retry horizon must stay below it.
	IdempotencyRetentionSeconds int64       `json:"idempotency_retention_seconds"`
	MaxTextLength               int         `json:"max_text_length"`
	Media                       MediaLimits `json:"media"`
	Rate                        RateLimits  `json:"send_rate_default"`
	Subscriptions               SubLimits   `json:"subscriptions"`
}

// MediaLimits are the media constraints.
type MediaLimits struct {
	MaxBytes       int64    `json:"max_bytes"`
	InlineMaxBytes int      `json:"inline_max_bytes"`
	AllowedTypes   []string `json:"allowed_types"`
	// InboundMaxBytes is the largest attachment RelayPlane downloads for an inbound message (a larger one is reported as
	// REJECTED/too_large); InboundTTLSeconds is how long it stays downloadable. InboundMaxBytes is 0 when inbound media is off.
	InboundMaxBytes   int64 `json:"inbound_max_bytes"`
	InboundTTLSeconds int64 `json:"inbound_ttl_seconds"`
}

// RateLimits is the default outbound pacing per instance (tenant and instance policies may tighten it).
type RateLimits struct {
	MinIntervalMillis int64 `json:"min_interval_ms"`
	Burst             int   `json:"burst"`
	MaxPerMinute      int   `json:"max_per_minute"`
	MaxConcurrent     int   `json:"max_concurrent"`
}

// SubLimits are the webhook subscription limits and delivery guarantees.
type SubLimits struct {
	MaxPerTenant        int   `json:"max_per_tenant"`
	RetryMaxAttempts    int   `json:"retry_max_attempts"`
	RetryHorizonSeconds int64 `json:"retry_horizon_seconds"`
}

// Limits reports the effective limits of this deployment.
func (a *App) Limits() Limits {
	d := a.Deps
	c := d.Cfg
	retry := subscription.DefaultRetry()
	var horizon int64
	for _, s := range retry.Schedule {
		horizon += int64(s.Seconds() * (1 + retry.Jitter))
	}
	sub := d.Cfg.Subscriptions
	if sub.MaxPerTenant <= 0 {
		sub.MaxPerTenant = 10
	}
	l := Limits{
		MaxTextLength: c.MaxTextLength,
		Media: MediaLimits{MaxBytes: c.MediaPolicy.MaxBytes, InlineMaxBytes: c.MediaPolicy.InlineMaxBytes, AllowedTypes: append([]string{}, c.MediaPolicy.AllowedTypes...),
			InboundMaxBytes: c.EffectiveInboundMaxBytes(), InboundTTLSeconds: int64(c.EffectiveInboundTTL().Seconds())},
		Rate: RateLimits{MinIntervalMillis: c.DefaultRate.MinInterval.Milliseconds(), Burst: c.DefaultRate.Burst, MaxPerMinute: c.DefaultRate.MaxPerMinute,
			MaxConcurrent: c.DefaultRate.MaxConcurrent},
		Subscriptions: SubLimits{MaxPerTenant: sub.MaxPerTenant, RetryMaxAttempts: retry.MaxAttempts(), RetryHorizonSeconds: horizon},
	}
	if d.Idem != nil {
		l.IdempotencyRetentionSeconds = int64(d.Idem.TTL.Seconds())
	}
	return l
}
