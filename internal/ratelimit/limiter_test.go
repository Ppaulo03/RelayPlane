package ratelimit

import (
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func newClock() (*clock, *Limiter) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	return c, NewWithClock(c.now)
}

func mustReserve(t *testing.T, l *Limiter, key string, p messaging.RatePolicy) func() {
	t.Helper()
	w, rel := l.Reserve(key, p)
	if w != 0 || rel == nil {
		t.Fatalf("expected immediate slot, wait=%v", w)
	}
	return rel
}

func TestMinIntervalIsConfigurableNotHardcoded(t *testing.T) {
	for _, interval := range []time.Duration{200 * time.Millisecond, 1500 * time.Millisecond, 5 * time.Second} {
		c, l := newClock()
		p := messaging.RatePolicy{MinInterval: interval, Burst: 1}
		mustReserve(t, l, "i", p)()
		w, rel := l.Reserve("i", p)
		if rel != nil || w <= 0 || w > interval {
			t.Fatalf("interval %v: wait=%v", interval, w)
		}
		c.add(w)
		mustReserve(t, l, "i", p)()
	}
}

func TestZeroPolicyMeansUnlimited(t *testing.T) {
	_, l := newClock()
	for i := 0; i < 1000; i++ {
		mustReserve(t, l, "i", messaging.RatePolicy{})()
	}
}

func TestBurstThenPaced(t *testing.T) {
	c, l := newClock()
	p := messaging.RatePolicy{MinInterval: time.Second, Burst: 3}
	for i := 0; i < 3; i++ {
		mustReserve(t, l, "i", p)()
	}
	if w, rel := l.Reserve("i", p); rel != nil || w < 900*time.Millisecond {
		t.Fatalf("4th send must wait ~1s, wait=%v", w)
	}
	c.add(time.Second)
	mustReserve(t, l, "i", p)()
}

func TestMaxPerMinuteSlidingWindow(t *testing.T) {
	c, l := newClock()
	p := messaging.RatePolicy{MaxPerMinute: 3}
	for i := 0; i < 3; i++ {
		mustReserve(t, l, "i", p)()
		c.add(10 * time.Second)
	}
	w, rel := l.Reserve("i", p)
	if rel != nil || w != 30*time.Second {
		t.Fatalf("must wait until the oldest send leaves the window, wait=%v", w)
	}
	c.add(w)
	mustReserve(t, l, "i", p)()
}

func TestMaxConcurrent(t *testing.T) {
	_, l := newClock()
	p := messaging.RatePolicy{MaxConcurrent: 2}
	r1 := mustReserve(t, l, "i", p)
	mustReserve(t, l, "i", p)
	if w, rel := l.Reserve("i", p); rel != nil || w == 0 {
		t.Fatal("third concurrent send must be refused")
	}
	r1()
	r1() // release is idempotent
	mustReserve(t, l, "i", p)
}

func TestCooldownAfterPenalty(t *testing.T) {
	c, l := newClock()
	p := messaging.RatePolicy{Cooldown: time.Minute}
	mustReserve(t, l, "i", p)()
	l.Penalize("i", p)
	if w, rel := l.Reserve("i", p); rel != nil || w != time.Minute {
		t.Fatalf("wait=%v", w)
	}
	c.add(time.Minute)
	mustReserve(t, l, "i", p)()
}

func TestScopesAreIndependent(t *testing.T) {
	_, l := newClock()
	p := messaging.RatePolicy{MinInterval: time.Hour, Burst: 1}
	mustReserve(t, l, "a", p)()
	mustReserve(t, l, "b", p)()
	if w, _ := l.Reserve("a", p); w == 0 {
		t.Fatal("scope a must be limited")
	}
}

func TestRefusedReservationConsumesNothing(t *testing.T) {
	c, l := newClock()
	p := messaging.RatePolicy{MinInterval: time.Second, Burst: 1}
	mustReserve(t, l, "i", p)()
	for i := 0; i < 50; i++ {
		l.Reserve("i", p) // hammering while limited must not push the horizon out
	}
	c.add(time.Second)
	mustReserve(t, l, "i", p)()
}
