package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/core/subscription"
)

// webhookSink is a tenant endpoint run by the load generator: it verifies every delivery's signature exactly as a
// consumer would, deduplicates by event id, and can answer 500 to a fraction of requests to exercise retries.
type webhookSink struct {
	secret   string
	failRate float64

	mu         sync.Mutex
	srv        *http.Server
	failedOnce map[string]bool                // events that already got their injected failure
	events     map[string]struct{}            // distinct event ids accepted
	deliveries int                            // accepted requests (including redeliveries of the same event)
	statuses   map[string]map[string]struct{} // message id -> statuses seen
	badSig     int
	failed     int
	lags       []time.Duration // first-attempt outbound status events: arrival minus the moment the status changed
}

func startSink(listen string, failRate float64) (*webhookSink, error) {
	s := &webhookSink{failRate: failRate, events: map[string]struct{}{}, failedOnce: map[string]bool{}, statuses: map[string]map[string]struct{}{}}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook", s.handle)
	s.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

func (s *webhookSink) setSecret(secret string) { s.mu.Lock(); s.secret = secret; s.mu.Unlock() }

func (s *webhookSink) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	s.mu.Lock()
	secret, failRate := s.secret, s.failRate
	s.mu.Unlock()
	ts, _ := strconv.ParseInt(r.Header.Get(subscription.HeaderTimestamp), 10, 64)
	// the secret is only known once the subscription was created; a request that raced it is retried by RelayPlane
	if secret == "" {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	if err := subscription.Verify([]string{secret}, r.Header.Get(subscription.HeaderSignature), ts, body, time.Now(), 5*time.Minute); err != nil {
		s.mu.Lock()
		s.badSig++
		s.mu.Unlock()
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	// Each event fails AT MOST ONCE. Random failures could hit the same event several times in a row (3 in a row is ~0.1 %,
	// i.e. a couple per 2000 deliveries) and push its retry past any reasonable test window (the schedule is 5 s, 30 s, 2 min,
	// 10 min...), which tests the dice, not RelayPlane. One failure per event still proves retry-then-deliver.
	if failRate > 0 && rand.Float64() < failRate {
		id := r.Header.Get(subscription.HeaderEventID)
		s.mu.Lock()
		first := !s.failedOnce[id]
		if first {
			s.failedOnce[id] = true
			s.failed++
		}
		s.mu.Unlock()
		if first {
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return
		}
	}
	var ev struct {
		EventID   string    `json:"event_id"`
		EventType string    `json:"event_type"`
		Timestamp time.Time `json:"timestamp"`
		Payload   struct {
			MessageID string `json:"message_id"`
			Status    string `json:"status"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &ev); err != nil || ev.EventID != r.Header.Get(subscription.HeaderEventID) {
		http.Error(w, "malformed", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.deliveries++
	s.events[ev.EventID] = struct{}{}
	if ev.EventType == "message.outbound_status" {
		// the delivery lag a consumer sees on the healthy path: the first attempt that succeeded (a retried event carries
		// the backoff, which is not what the p95 target is about). Skew between this host and the database clock is
		// clamped to zero.
		if r.Header.Get(subscription.HeaderAttempt) == "1" && !ev.Timestamp.IsZero() {
			lag := time.Since(ev.Timestamp)
			if lag < 0 {
				lag = 0
			}
			s.lags = append(s.lags, lag)
		}
		if s.statuses[ev.Payload.MessageID] == nil {
			s.statuses[ev.Payload.MessageID] = map[string]struct{}{}
		}
		s.statuses[ev.Payload.MessageID][ev.Payload.Status] = struct{}{}
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// missing returns the messages for which no status event of the accepted kinds has arrived yet.
func (s *webhookSink) missing(messageIDs []string, accepted ...string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, id := range messageIDs {
		ok := false
		for _, st := range accepted {
			if _, has := s.statuses[id][st]; has {
				ok = true
			}
		}
		if !ok {
			out = append(out, id)
		}
	}
	return out
}

func (s *webhookSink) summary() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("%d distinct events, %d accepted requests (%d redeliveries), %d injected failures, %d bad signatures",
		len(s.events), s.deliveries, s.deliveries-len(s.events), s.failed, s.badSig)
}

func (s *webhookSink) badSignatures() int { s.mu.Lock(); defer s.mu.Unlock(); return s.badSig }

// lagReport returns the sample count and the p50/p95/p99 of the event delivery lag.
func (s *webhookSink) lagReport() (n int, p50, p95, p99 time.Duration) {
	s.mu.Lock()
	lags := append([]time.Duration(nil), s.lags...)
	s.mu.Unlock()
	if len(lags) == 0 {
		return 0, 0, 0, 0
	}
	sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })
	at := func(q float64) time.Duration { return lags[min(len(lags)-1, int(q*float64(len(lags))))] }
	return len(lags), at(0.50), at(0.95), at(0.99)
}
