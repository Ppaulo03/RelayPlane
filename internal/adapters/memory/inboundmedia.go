package memory

import (
	"context"
	"sort"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/ports"
)

type inboundRow struct {
	media.InboundJob
	leaseUntil time.Time
}

type inboundMediaRepo struct{ s *Store }

func (r inboundMediaRepo) Enqueue(_ context.Context, j media.InboundJob) (bool, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	return r.s.enqueueInbound(j), nil
}

// enqueueInbound queues an attachment job unless one exists for its event. The caller holds s.mu.
func (s *Store) enqueueInbound(j media.InboundJob) bool {
	for _, e := range s.inbound {
		if e.EventID == j.EventID {
			return false
		}
	}
	if j.CreatedAt.IsZero() {
		j.CreatedAt = s.Now()
	}
	if j.NextAttemptAt.IsZero() {
		j.NextAttemptAt = j.CreatedAt
	}
	j.Stage = media.StageDownload
	s.inbound[j.ID] = &inboundRow{InboundJob: j}
	return true
}

func (r inboundMediaRepo) ClaimDue(_ context.Context, now time.Time, lease time.Duration, limit int) ([]media.InboundJob, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var due []*inboundRow
	for _, j := range r.s.inbound {
		if j.Stage != media.StageDone && !j.NextAttemptAt.After(now) && !j.leaseUntil.After(now) {
			due = append(due, j)
		}
	}
	sort.Slice(due, func(a, b int) bool {
		if !due[a].CreatedAt.Equal(due[b].CreatedAt) {
			return due[a].CreatedAt.Before(due[b].CreatedAt)
		}
		return due[a].ID < due[b].ID
	})
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	out := make([]media.InboundJob, 0, len(due))
	for _, j := range due {
		j.leaseUntil = now.Add(lease)
		out = append(out, j.InboundJob)
	}
	return out, nil
}

func (r inboundMediaRepo) row(id string) (*inboundRow, error) {
	j, ok := r.s.inbound[id]
	if !ok {
		return nil, errs.ErrNotFound
	}
	return j, nil
}

func (r inboundMediaRepo) Resolve(_ context.Context, id string, ev events.Event) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	j, err := r.row(id)
	if err != nil || j.Stage == media.StageDone {
		return err
	}
	j.Event, j.Ref, j.Stage, j.Attempts, j.leaseUntil, j.LastError, j.NextAttemptAt = ev, nil, media.StagePublish, 0, time.Time{}, "", r.s.Now()
	return nil
}

func (r inboundMediaRepo) Retry(_ context.Context, id string, next time.Time, lastErr string) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	j, err := r.row(id)
	if err != nil {
		return err
	}
	j.Attempts++
	j.NextAttemptAt, j.LastError, j.leaseUntil = next, lastErr, time.Time{}
	return nil
}

func (r inboundMediaRepo) Complete(_ context.Context, id string, ev events.Event, at time.Time) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	j, err := r.row(id)
	if err != nil {
		return err
	}
	r.s.queueEvent(ev) // the event and the closing of the job under one lock: the in-memory analogue of the SQL transaction
	j.Stage, j.DoneAt, j.leaseUntil, j.Ref = media.StageDone, at, time.Time{}, nil
	return nil
}

func (r inboundMediaRepo) Purge(_ context.Context, before time.Time) (int64, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var n int64
	for id, j := range r.s.inbound {
		if j.Stage == media.StageDone && j.DoneAt.Before(before) {
			delete(r.s.inbound, id)
			n++
		}
	}
	return n, nil
}

func (r inboundMediaRepo) Counts(_ context.Context, now time.Time) (ports.InboundMediaCounts, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	var c ports.InboundMediaCounts
	for _, j := range r.s.inbound {
		switch j.Stage {
		case media.StageDownload:
			c.Download++
		case media.StagePublish:
			c.Publish++
		default:
			continue
		}
		if age := now.Sub(j.CreatedAt); age > c.OldestPending {
			c.OldestPending = age
		}
	}
	return c, nil
}
