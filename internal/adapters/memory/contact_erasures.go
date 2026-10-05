package memory

import (
	"context"
	"time"
)

type erasureRepo struct{ s *Store }

func (r erasureRepo) Mark(_ context.Context, tenantID, subject string, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if r.s.erasures == nil {
		r.s.erasures = map[string]time.Time{}
	}
	k := tenantID + "|" + subject
	if cur, ok := r.s.erasures[k]; !ok || at.After(cur) {
		r.s.erasures[k] = at
	}
	return nil
}

func (r erasureRepo) Erased(_ context.Context, tenantID, subject string, acceptedAt time.Time) (bool, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	at, ok := r.s.erasures[tenantID+"|"+subject]
	return ok && !at.Before(acceptedAt), nil
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
