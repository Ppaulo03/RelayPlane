package memory

import (
	"context"
	"time"

	"github.com/relayplane/relayplane/internal/core/events"
)

type erasureRepo struct{ s *Store }

func (r erasureRepo) Mark(_ context.Context, tenantID, number string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if r.s.erasures == nil {
		r.s.erasures = map[string]time.Time{}
	}
	k := tenantID + "|" + number
	if cur, ok := r.s.erasures[k]; !ok || at.After(cur) {
		r.s.erasures[k] = at
	}
	return nil
}

func (r erasureRepo) Erased(_ context.Context, ev events.Event) (bool, error) {
	number := ev.ContactNumber()
	if number == "" || ev.AcceptedAt == nil {
		return false, nil
	}
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	at, ok := r.s.erasures[ev.TenantID+"|"+number]
	return ok && !at.Before(*ev.AcceptedAt), nil
}

func (r erasureRepo) Purge(_ context.Context, before time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for k, at := range r.s.erasures {
		if at.Before(before) {
			delete(r.s.erasures, k)
			n++
		}
	}
	return n, nil
}

func (r deliveriesRepo) DeleteByEvent(_ context.Context, eventID string) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for id, d := range r.s.deliveries {
		if d.EventID == eventID {
			delete(r.s.deliveries, id)
			n++
		}
	}
	return n, nil
}

func (r inboundMediaRepo) Drop(_ context.Context, id string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	delete(r.s.inbound, id)
	return nil
}
