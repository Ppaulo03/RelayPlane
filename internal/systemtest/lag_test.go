package systemtest

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/subscription"
)

// lagHistogram sums relayplane_event_delivery_lag_seconds over the label sets that match.
type lagHistogram struct {
	count   uint64
	buckets map[float64]uint64
}

func readLag(t *testing.T, e *Env, attempt string) (map[string]*lagHistogram, *lagHistogram) {
	t.Helper()
	fams, err := e.Metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byType, all := map[string]*lagHistogram{}, &lagHistogram{buckets: map[float64]uint64{}}
	for _, f := range fams {
		if f.GetName() != "relayplane_event_delivery_lag_seconds" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["attempt"] != attempt {
				continue
			}
			h := byType[labels["event_type"]]
			if h == nil {
				h = &lagHistogram{buckets: map[float64]uint64{}}
				byType[labels["event_type"]] = h
			}
			add := func(dst *lagHistogram, hist *dto.Histogram) {
				dst.count += hist.GetSampleCount()
				for _, b := range hist.GetBucket() {
					dst.buckets[b.GetUpperBound()] += b.GetCumulativeCount()
				}
			}
			add(h, m.GetHistogram())
			add(all, m.GetHistogram())
		}
	}
	return byType, all
}

// quantileBound is the upper bound of the first bucket holding at least q of the samples (a conservative estimate).
func (h *lagHistogram) quantileBound(q float64) float64 {
	if h.count == 0 {
		return math.NaN()
	}
	var bounds []float64
	for b := range h.buckets {
		bounds = append(bounds, b)
	}
	sort.Float64s(bounds)
	need := uint64(math.Ceil(q * float64(h.count)))
	for _, b := range bounds {
		if h.buckets[b] >= need {
			return b
		}
	}
	return math.Inf(1)
}

// The delivery lag is measured for every kind of event and, on a healthy consumer, stays well inside the 2 s target: a
// conversation turn waits for the ACCEPTED of the prompt before it takes a "yes" as an answer.
func TestEventDeliveryLag_IsMeasuredForEveryKindAndStaysUnderTheTarget(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived), string(events.MessageOutboundStatus))
	e.StartOutbox()
	e.StartWorkers(2)
	e.StartProjector()
	e.StartWebhooks()

	const n = 40
	var ids []string
	for i := 0; i < n; i++ {
		res, _, err := e.SendText(e.Tenant, inst.ID, fmt.Sprintf("m%d", i), fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.MessageID)
		if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, fmt.Sprintf("WA-%d", i)))); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		e.WaitMessage(id, messaging.StatusAccepted, messaging.StatusDelivered)
	}
	Eventually(t, 20*time.Second, "everything is delivered", func() bool {
		in, out := 0, 0
		for _, r := range e.Receiver.Accepted(hookURL) {
			switch r.EventType {
			case string(events.MessageReceived):
				in++
			case string(events.MessageOutboundStatus):
				out++
			}
		}
		return in == n && out >= n
	})

	// the receiver has the request before the dispatcher records its outcome and the lag: wait for the samples
	Eventually(t, 10*time.Second, "the lag of every delivery is recorded", func() bool {
		_, all := readLag(t, e, "first")
		return all.count >= 2*n
	})
	byType, all := readLag(t, e, "first")
	if byType[string(events.MessageReceived)] == nil || byType[string(events.MessageOutboundStatus)] == nil {
		t.Fatalf("the lag must be measured for inbound messages and for outbound statuses: %v", byType)
	}
	if got := byType[string(events.MessageReceived)].count; got != n {
		t.Errorf("one lag sample per inbound message: %d, want %d", got, n)
	}
	if p95 := all.quantileBound(0.95); p95 > 2 {
		t.Errorf("p95 of the delivery lag is above the 2 s target: bucket %.3fs over %d events", p95, all.count)
	}
	// nothing internal leaked into what the consumer receives (the schema forbids unknown fields and this suite validates
	// every body), and no retry happened on this healthy path
	if _, retried := readLag(t, e, "retry"); retried.count != 0 {
		t.Errorf("a healthy consumer needs no retry: %d", retried.count)
	}
}

// A consumer that fails once is retried: that delivery shows up under attempt=retry (with the backoff in it), and does not
// pollute the first-attempt latency that the SLO is about.
func TestEventDeliveryLag_RetriesAreSeparatedFromTheHealthyPath(t *testing.T) {
	e := NewEnv(t)
	inst := e.CreateInstance(e.Tenant, "a", true)
	subscribe(t, e, e.Tenant, hookURL, string(events.MessageReceived))
	e.Receiver.Behave = func(n int, _ Received) (int, error) {
		if n == 1 {
			return 500, nil
		}
		return 200, nil
	}
	e.Dispatcher.Retry = subscription.RetryPolicy{Schedule: []time.Duration{300 * time.Millisecond}}
	e.StartWebhooks()
	if _, err := e.App.Inbound.Handle(bg, ProviderKey, inboundBody(inst.NodeID, inst.AssignmentEpoch, recvEv(inst.ID, "WA-R"))); err != nil {
		t.Fatal(err)
	}
	Eventually(t, 10*time.Second, "delivered after the retry, and its lag recorded", func() bool {
		_, retry := readLag(t, e, "retry")
		return len(e.Receiver.Accepted(hookURL)) == 1 && retry.count == 1
	})
	_, retry := readLag(t, e, "retry")
	_, first := readLag(t, e, "first")
	if retry.count != 1 || first.count != 0 {
		t.Fatalf("one delivery, on its retry: first=%d retry=%d", first.count, retry.count)
	}
	if retry.quantileBound(1) < 0.25 {
		t.Errorf("the lag of a retried delivery includes the backoff: bucket %.3fs", retry.quantileBound(1))
	}
}
