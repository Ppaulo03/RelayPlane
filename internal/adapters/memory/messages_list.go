package memory

import (
	"context"
	"sort"
	"time"

	"github.com/relayplane/relayplane/internal/core/messaging"
)

func (r msgRepo) ListByStatus(_ context.Context, tenantID string, status messaging.Status, instanceID string, limit int) ([]messaging.Message, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	var out []messaging.Message
	for _, m := range r.s.messages {
		if m.TenantID == tenantID && m.Status == status && (instanceID == "" || m.InstanceID == instanceID) {
			out = append(out, *m)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].InstanceID != out[j].InstanceID {
			return out[i].InstanceID < out[j].InstanceID
		}
		if out[i].SequenceNo != out[j].SequenceNo {
			return out[i].SequenceNo < out[j].SequenceNo
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r msgRepo) UnknownStats(_ context.Context) (int64, time.Duration, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	var oldest time.Duration
	now := r.s.Now()
	for _, m := range r.s.messages {
		if m.Status != messaging.StatusUnknown {
			continue
		}
		n++
		if age := now.Sub(m.UpdatedAt); age > oldest {
			oldest = age
		}
	}
	return n, oldest, nil
}
