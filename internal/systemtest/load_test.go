//go:build integration && chaos

package systemtest

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

// proxiedEnv builds an Env whose PostgreSQL and Redis connections go through fault-injecting TCP proxies.
func proxiedEnv(t *testing.T) (*traffic, *netProxy, *netProxy) {
	pg := newNetProxy(t, envOr("RELAYPLANE_TEST_PG_HOSTPORT", "127.0.0.1:55440"))
	rd := newNetProxy(t, envOr("RELAYPLANE_TEST_REDIS_ADDR", "127.0.0.1:56390"))
	t.Setenv("RELAYPLANE_TEST_DATABASE_URL", "postgres://relayplane:relayplane@"+pg.Addr()+"/relayplane_test?sslmode=disable")
	t.Setenv("RELAYPLANE_TEST_REDIS_ADDR", rd.Addr())
	return newTraffic(t, 4), pg, rd
}

// Every packet to PostgreSQL and Redis is delayed with high jitter (a congested or cross-region network).
func TestJitter_SlowAndUnstableNetwork(t *testing.T) {
	tr, pg, rd := proxiedEnv(t)
	pg.Set(5*time.Millisecond, 40*time.Millisecond)
	rd.Set(5*time.Millisecond, 40*time.Millisecond)
	tr.run(80, 10*time.Millisecond, 0)
	tr.assertConverges(t)
}

// Connections are reset repeatedly (failing LB / NAT timeouts) on top of jitter: pools must reconnect and
// nothing may be lost or duplicated.
func TestJitter_ConnectionResetsUnderTraffic(t *testing.T) {
	tr, pg, rd := proxiedEnv(t)
	pg.Set(0, 15*time.Millisecond)
	rd.Set(0, 15*time.Millisecond)
	done := make(chan struct{})
	go func() { defer close(done); tr.run(80, 20*time.Millisecond, 0) }()
	for i := 0; i < 6; i++ {
		time.Sleep(300 * time.Millisecond)
		pg.Sever()
		rd.Sever()
	}
	<-done
	tr.assertConverges(t)
}

// Throughput/latency baseline with no faults: many instances, many workers, concurrent producers.
// Size with LOAD_MESSAGES (default 2000) and LOAD_INSTANCES (default 20).
func TestLoad_ThroughputAndOrdering(t *testing.T) {
	n, ninst := envInt("LOAD_MESSAGES", 2000), envInt("LOAD_INSTANCES", 20)
	e := NewEnv(t)
	e.Worker.UnknownBarrierTimeout = 300 * time.Millisecond
	var insts []string
	for i := 0; i < ninst; i++ {
		insts = append(insts, e.CreateInstance(e.Tenant, fmt.Sprintf("load-%d", i), true).ID)
	}
	e.StartWorkers(6)
	e.StartProjector()
	e.StartOutbox()

	var mu sync.Mutex
	var ids []string
	var accept []time.Duration
	var wg sync.WaitGroup
	start := time.Now()
	per := n / ninst
	for k, inst := range insts { // one producer per instance: acceptance order == text order
		wg.Add(1)
		go func(k int, inst string) {
			defer wg.Done()
			for j := 0; j < per; j++ {
				t0 := time.Now()
				r, _, err := e.SendText(e.Tenant, inst, fmt.Sprintf("m%05d", j), fmt.Sprintf("load-%d-%d", k, j))
				if err != nil {
					t.Errorf("send: %v", err)
					return
				}
				mu.Lock()
				ids = append(ids, r.MessageID)
				accept = append(accept, time.Since(t0))
				mu.Unlock()
			}
		}(k, inst)
	}
	wg.Wait()
	acceptedIn := time.Since(start)
	ctx := context.Background()
	Eventually(t, 3*time.Minute, "all messages processed", func() bool {
		for _, id := range ids {
			m, err := e.Repos.Messages.Get(ctx, id)
			if err != nil || m.Status == messaging.StatusQueued || m.Status == messaging.StatusDispatching {
				return false
			}
		}
		return true
	})
	total := time.Since(start)
	for _, id := range ids {
		if m, _ := e.Repos.Messages.Get(ctx, id); m.Status != messaging.StatusAccepted {
			t.Errorf("message %s ended %s", id, m.Status)
		}
	}
	last := map[string]string{}
	seen := map[string]bool{}
	for _, s := range e.Provider.Sent() {
		key := s.Assignment.InstanceID + "/" + s.Message.Text
		if seen[key] {
			t.Errorf("duplicate delivery %s", key)
		}
		seen[key] = true
		if p := last[s.Assignment.InstanceID]; p != "" && s.Message.Text < p {
			t.Errorf("INV-07: %s received %s after %s", s.Assignment.InstanceID, s.Message.Text, p)
		}
		last[s.Assignment.InstanceID] = s.Message.Text
	}
	if len(seen) != len(ids) {
		t.Errorf("provider received %d distinct messages, %d were accepted", len(seen), len(ids))
	}
	sort.Slice(accept, func(i, j int) bool { return accept[i] < accept[j] })
	t.Logf("%d messages / %d instances: accepted in %s (%.0f msg/s, accept p50=%s p95=%s p99=%s), fully delivered in %s (%.0f msg/s)",
		len(ids), ninst, acceptedIn.Round(time.Millisecond), float64(len(ids))/acceptedIn.Seconds(),
		pct(accept, .50), pct(accept, .95), pct(accept, .99), total.Round(time.Millisecond), float64(len(ids))/total.Seconds())
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return d[int(float64(len(d)-1)*p)].Round(100 * time.Microsecond)
}

func envInt(k string, def int) int {
	var v int
	if s := strings.TrimSpace(os.Getenv(k)); s != "" {
		if _, err := fmt.Sscan(s, &v); err == nil && v > 0 {
			return v
		}
	}
	return def
}
