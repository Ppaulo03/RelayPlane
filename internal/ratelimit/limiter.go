// Package ratelimit implements the per-scope outbound send limiter.
//
// A policy (core/messaging.RatePolicy) is resolved from the hierarchy
// global < tenant < instance by messaging.ResolvePolicy; this package enforces
// the resolved policy for one scope (normally one instance).
//
// Reserve never blocks: it returns how long the caller must wait. The worker
// turns a positive wait into a queue Defer, so a rate-limited instance does
// not stall other instances that share its partition.
//
// State is local to the process. Because a partition is consumed by a single
// worker at a time, all sends of an instance normally hit one limiter; after a
// partition moves to another worker the window restarts (documented limit).
package ratelimit

import (
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

// Limiter enforces rate policies per scope key.
type Limiter struct {
	mu     sync.Mutex
	now    func() time.Time
	scopes map[string]*scope
}

type scope struct {
	tokens   float64
	refilled time.Time
	sent     []time.Time // sliding one-minute window
	inflight int
	cooldown time.Time
	last     time.Time
}

// New returns a Limiter using the wall clock.
func New() *Limiter { return NewWithClock(time.Now) }

// NewWithClock returns a Limiter with an injectable clock (tests).
func NewWithClock(now func() time.Time) *Limiter {
	return &Limiter{now: now, scopes: map[string]*scope{}}
}

// concurrencyRetry is the suggested wait when MaxConcurrent is saturated.
const concurrencyRetry = 50 * time.Millisecond

// Reserve tries to take a send slot for scopeKey under policy p.
//
// If a send is allowed now it returns (0, release): the slot is consumed and
// release MUST be called when the send finishes (it frees a concurrency slot).
// Otherwise it returns the wait before the caller should try again and a nil
// release; nothing is consumed.
func (l *Limiter) Reserve(scopeKey string, p messaging.RatePolicy) (time.Duration, func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	s := l.scopes[scopeKey]
	if s == nil {
		burst := p.Burst
		if burst < 1 {
			burst = 1
		}
		s = &scope{tokens: float64(burst), refilled: now}
		l.scopes[scopeKey] = s
	}

	var wait time.Duration
	bump := func(d time.Duration) {
		if d > wait {
			wait = d
		}
	}

	if now.Before(s.cooldown) {
		bump(s.cooldown.Sub(now))
	}

	// Token bucket: capacity Burst, one token refilled per MinInterval.
	if p.MinInterval > 0 {
		burst := float64(p.Burst)
		if burst < 1 {
			burst = 1
		}
		s.tokens += float64(now.Sub(s.refilled)) / float64(p.MinInterval)
		if s.tokens > burst {
			s.tokens = burst
		}
		s.refilled = now
		if s.tokens < 1 {
			bump(time.Duration((1 - s.tokens) * float64(p.MinInterval)))
		}
	}

	// Sliding window: MaxPerMinute.
	if p.MaxPerMinute > 0 {
		cut := now.Add(-time.Minute)
		i := 0
		for i < len(s.sent) && !s.sent[i].After(cut) {
			i++
		}
		s.sent = s.sent[i:]
		if len(s.sent) >= p.MaxPerMinute {
			bump(s.sent[len(s.sent)-p.MaxPerMinute].Add(time.Minute).Sub(now))
		}
	}

	if p.MaxConcurrent > 0 && s.inflight >= p.MaxConcurrent {
		bump(concurrencyRetry)
	}

	if wait > 0 {
		return wait, nil
	}

	if p.MinInterval > 0 {
		s.tokens--
	}
	if p.MaxPerMinute > 0 {
		s.sent = append(s.sent, now)
	}
	s.inflight++
	s.last = now
	released := false
	return 0, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if !released && s.inflight > 0 {
			s.inflight--
		}
		released = true
	}
}

// Penalize starts the policy's Cooldown for the scope (e.g. after the
// provider signalled throttling).
func (l *Limiter) Penalize(scopeKey string, p messaging.RatePolicy) {
	if p.Cooldown <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if s := l.scopes[scopeKey]; s != nil {
		s.cooldown = l.now().Add(p.Cooldown)
	}
}

// Forget drops idle scope state older than idle (housekeeping).
func (l *Limiter) Forget(idle time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cut := l.now().Add(-idle)
	for k, s := range l.scopes {
		if s.inflight == 0 && s.last.Before(cut) {
			delete(l.scopes, k)
		}
	}
}
