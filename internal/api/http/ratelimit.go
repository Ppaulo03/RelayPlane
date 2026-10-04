package http

import (
	"math"
	nethttp "net/http"
	"strconv"
	"sync"
	"time"
)

// RateConfig is the per-tenant request limit of the API: a token bucket of Burst tokens refilled at PerSecond.
// PerSecond <= 0 turns the limit off. The bucket lives in the gateway process, so with N gateway replicas a tenant can
// do up to N times the rate in total (size it accordingly, or put a shared limiter in front).
type RateConfig struct {
	PerSecond float64
	Burst     int
}

type bucket struct {
	tokens float64
	last   time.Time
}

// tenantLimiter keeps one bucket per tenant, so a slow agent hammering the API (or one stuck in a loop) is throttled
// without costing anybody else a request.
type tenantLimiter struct {
	cfg RateConfig
	now func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
	swept   time.Time
}

func newTenantLimiter(cfg RateConfig, now func() time.Time) *tenantLimiter {
	if cfg.PerSecond <= 0 {
		return nil
	}
	if cfg.Burst < 1 {
		cfg.Burst = int(math.Ceil(cfg.PerSecond))
	}
	if now == nil {
		now = time.Now
	}
	return &tenantLimiter{cfg: cfg, now: now, buckets: map[string]*bucket{}}
}

// allow takes a token. When the bucket is empty it says how long until one is available.
func (l *tenantLimiter) allow(tenant string) (ok bool, remaining int, retryAfter time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.swept) > time.Minute { // forget tenants that have been idle long enough to be full again
		full := time.Duration(float64(l.cfg.Burst)/l.cfg.PerSecond*float64(time.Second)) + time.Minute
		for id, b := range l.buckets {
			if now.Sub(b.last) > full {
				delete(l.buckets, id)
			}
		}
		l.swept = now
	}
	b := l.buckets[tenant]
	if b == nil {
		b = &bucket{tokens: float64(l.cfg.Burst), last: now}
		l.buckets[tenant] = b
	}
	b.tokens = math.Min(float64(l.cfg.Burst), b.tokens+now.Sub(b.last).Seconds()*l.cfg.PerSecond)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, int(b.tokens), 0
	}
	return false, 0, time.Duration((1 - b.tokens) / l.cfg.PerSecond * float64(time.Second))
}

// limit enforces the tenant's budget; it answers 429 with Retry-After and returns false when it is spent.
func (s *Server) limit(w nethttp.ResponseWriter, r *nethttp.Request, p Principal) bool {
	if s.limiter == nil || p.Role != RoleTenant {
		return true
	}
	ok, remaining, wait := s.limiter.allow(p.TenantID)
	h := w.Header()
	h.Set("RateLimit-Limit", strconv.Itoa(s.limiter.cfg.Burst))
	h.Set("RateLimit-Remaining", strconv.Itoa(remaining))
	if ok {
		return true
	}
	if s.Metrics != nil {
		s.Metrics.RateLimited.Inc()
	}
	secs := int(math.Ceil(wait.Seconds()))
	if secs < 1 {
		secs = 1
	}
	h.Set("Retry-After", strconv.Itoa(secs))
	writeError(w, r, s.Log, errRateLimited)
	return false
}
