package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"sync"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/events"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// MediaIngestor resolves the attachments of inbound messages: it downloads the bytes from the provider, stores them in
// the blob store, records the outcome in the held message.received event and publishes it. Every outcome is final and
// published, so a message with an attachment is never stuck: success is READY, a deliberate refusal is REJECTED, and a
// provider that cannot (or no longer can) deliver the bytes ends as FAILED after a bounded number of attempts.
//
// Crash safety: the job row is the source of truth. Download -> store -> Resolve (outcome recorded) -> publish -> Done;
// each step is idempotent (deterministic ids, overwrite of the same object key), a lease frees a job whose worker died.
type MediaIngestor struct {
	Repos     ports.Repositories
	Providers *app.ProviderRegistry
	Blob      ports.BlobStore
	Bus       ports.EventBus
	Metrics   *observability.Metrics
	Log       *slog.Logger
	Now       func() time.Time

	MaxBytes int64 // largest attachment that is stored
	TTL      time.Duration
	Policy   media.Policy

	Batch       int           // jobs claimed per pass (default 8)
	Concurrency int           // parallel downloads (default 4)
	Lease       time.Duration // how long a claimed job is exclusive (default 3m: a download can take a while)
	Poll        time.Duration // pause when idle (default 500ms)
	// Backoff is the wait before attempt n+1 (default 5s, 15s, 1m, 5m, 15m); MaxAttempts bounds retries (default 6).
	Backoff     func(attempt int) time.Duration
	MaxAttempts int
}

func (m *MediaIngestor) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *MediaIngestor) defaults() {
	if m.Batch <= 0 {
		m.Batch = 8
	}
	if m.Concurrency <= 0 {
		m.Concurrency = 4
	}
	if m.Lease <= 0 {
		m.Lease = 3 * time.Minute
	}
	if m.Poll <= 0 {
		m.Poll = 500 * time.Millisecond
	}
	if m.MaxAttempts <= 0 {
		m.MaxAttempts = 6
	}
	if m.MaxBytes <= 0 {
		m.MaxBytes = app.DefaultConfig().InboundMediaMaxBytes
	}
	if m.TTL <= 0 {
		m.TTL = app.DefaultConfig().InboundMediaTTL
	}
	if m.Backoff == nil {
		sched := []time.Duration{5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}
		m.Backoff = func(n int) time.Duration {
			if n >= len(sched) {
				return sched[len(sched)-1]
			}
			return sched[n]
		}
	}
	if m.Log == nil {
		m.Log = slog.Default()
	}
}

// Run processes jobs until ctx is cancelled.
func (m *MediaIngestor) Run(ctx context.Context) {
	m.defaults()
	for ctx.Err() == nil {
		n, err := m.ProcessDue(ctx)
		if err != nil {
			m.Log.WarnContext(ctx, "inbound media pass failed", "error", err)
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(m.Poll):
			}
		}
	}
}

// ProcessDue claims and processes one batch; it returns how many jobs it handled.
func (m *MediaIngestor) ProcessDue(ctx context.Context) (int, error) {
	m.defaults()
	jobs, err := m.Repos.InboundMedia.ClaimDue(ctx, m.now(), m.Lease, m.Batch)
	if err != nil || len(jobs) == 0 {
		return 0, err
	}
	sem := make(chan struct{}, m.Concurrency)
	var wg sync.WaitGroup
	for _, j := range jobs {
		sem <- struct{}{}
		wg.Add(1)
		go func(j media.InboundJob) {
			defer func() { <-sem; wg.Done() }()
			m.handle(ctx, j)
		}(j)
	}
	wg.Wait()
	return len(jobs), nil
}

func (m *MediaIngestor) handle(ctx context.Context, j media.InboundJob) {
	ctx = observability.With(ctx, observability.KeyTenantID, j.TenantID, observability.KeyInstanceID, j.InstanceID)
	ctx, span := observability.Start(ctx, "media.ingest")
	defer span.End()
	if j.Stage == media.StageDownload {
		if err := m.download(ctx, &j); err != nil {
			m.retry(ctx, j, err)
			return
		}
	}
	// publish the resolved event, then close the job (a crash in between republishes the same event id: consumers dedupe)
	if err := m.Bus.Publish(ctx, j.Event); err != nil {
		m.retry(ctx, j, fmt.Errorf("publish: %w", err))
		return
	}
	if err := m.Repos.InboundMedia.Done(ctx, j.ID, m.now()); err != nil {
		m.Log.WarnContext(ctx, "could not close the inbound media job (it will be republished with the same event id)", "media_id", j.ID, "error", err)
	}
}

func (m *MediaIngestor) retry(ctx context.Context, j media.InboundJob, cause error) {
	next := m.now().Add(m.Backoff(j.Attempts))
	m.Metrics.InboundMedia.WithLabelValues("retry").Inc()
	m.Log.WarnContext(ctx, "inbound media attempt failed", "media_id", j.ID, "attempt", j.Attempts+1, "error", cause)
	if err := m.Repos.InboundMedia.Retry(ctx, j.ID, next, cause.Error()); err != nil {
		m.Log.ErrorContext(ctx, "could not schedule the next inbound media attempt", "media_id", j.ID, "error", err)
	}
}

// download resolves the attachment and records the outcome in j.Event / the job row. It returns an error only when the
// attempt should be repeated; every terminal outcome (stored, rejected, failed) is recorded and returns nil.
func (m *MediaIngestor) download(ctx context.Context, j *media.InboundJob) error {
	inst, err := m.Repos.Instances.Get(ctx, j.InstanceID)
	if err != nil && !errors.Is(err, errs.ErrNotFound) {
		return err
	}
	if inst == nil || inst.DeletedAt != nil {
		return m.finish(ctx, j, events.MediaFailed, "download_failed", nil)
	}
	prov, err := m.Providers.Get(inst.Provider)
	dl, ok := prov.(ports.MediaDownloader)
	if err != nil || !ok {
		return m.finish(ctx, j, events.MediaRejected, "unsupported", nil)
	}

	res, err := dl.DownloadMedia(ctx, inst.Assignment(), j.Ref, m.MaxBytes)
	switch {
	case err == nil:
	case errors.Is(err, errs.ErrPayloadTooLarge):
		return m.finish(ctx, j, events.MediaRejected, "too_large", nil)
	case errors.Is(err, errs.ErrNotFound):
		return m.finish(ctx, j, events.MediaFailed, "expired", nil) // the provider no longer has it
	case errors.Is(err, errs.ErrProviderRejected):
		// the node answered but could not fetch it (typically an attachment that WhatsApp already dropped): try a couple
		// of times, then give up instead of retrying for hours
		if j.Attempts+1 >= 3 {
			return m.finish(ctx, j, events.MediaFailed, "download_failed", nil)
		}
		return err
	default:
		if j.Attempts+1 >= m.MaxAttempts {
			return m.finish(ctx, j, events.MediaFailed, "download_failed", nil)
		}
		return err
	}
	defer res.Body.Close()

	contentType := res.ContentType
	if contentType == "" {
		contentType = declared(j.Event).MimeType
	}
	if res.Size > m.MaxBytes {
		return m.finish(ctx, j, events.MediaRejected, "too_large", nil)
	}
	if !m.Policy.TypeAllowed(contentType) {
		return m.finish(ctx, j, events.MediaRejected, "type_not_allowed", nil)
	}
	name := res.Filename
	if name == "" {
		name = declared(j.Event).Filename
	}
	if name == "" {
		name = j.ID + extensionOf(contentType)
	}
	key := media.ObjectKey(j.TenantID, j.ID, name)
	h := sha256.New()
	if err := m.Blob.Put(ctx, key, io.TeeReader(io.LimitReader(res.Body, res.Size), h), res.Size, contentType); err != nil {
		return fmt.Errorf("store media: %w", err)
	}
	m.Metrics.BlobBytes.WithLabelValues("put").Add(float64(res.Size))
	b := media.Blob{ID: j.ID, TenantID: j.TenantID, ObjectKey: key, ContentType: contentType, Size: res.Size, SHA256: hex.EncodeToString(h.Sum(nil)),
		Filename: media.SafeFilename(name), Status: media.BlobReady, ExpiresAt: m.now().Add(m.TTL), CreatedAt: m.now()}
	if err := m.Repos.Blobs.Create(ctx, b); err != nil && !errors.Is(err, errs.ErrAlreadyExists) {
		return fmt.Errorf("record media: %w", err) // a retry after a crash finds the record already there and goes on
	}
	return m.finish(ctx, j, events.MediaReady, "", &b)
}

// finish records the outcome in the held event and moves the job to the publish stage.
func (m *MediaIngestor) finish(ctx context.Context, j *media.InboundJob, status, reason string, b *media.Blob) error {
	pl, err := receivedPayload(j.Event)
	if err != nil {
		return fmt.Errorf("held event is not a message.received: %w", err)
	}
	if pl.Media == nil {
		pl.Media = &events.MessageMedia{MediaID: j.ID}
	}
	pl.Media.Status, pl.Media.Reason = status, reason
	if b != nil {
		pl.Media.Size, pl.Media.MimeType, pl.Media.Filename = b.Size, baseType(b.ContentType), b.Filename
	}
	j.Event.Payload = pl
	if err := m.Repos.InboundMedia.Resolve(ctx, j.ID, j.Event); err != nil {
		return err
	}
	j.Stage = media.StagePublish
	outcome := reason
	if status == events.MediaReady {
		outcome = "ready"
	}
	m.Metrics.InboundMedia.WithLabelValues(outcome).Inc()
	return nil
}

// receivedPayload reads the payload of the held event whatever form the repository returned it in (typed in memory,
// a decoded JSON object from the database).
func receivedPayload(ev events.Event) (events.MessageReceivedPayload, error) {
	if p, ok := ev.Payload.(events.MessageReceivedPayload); ok {
		return p, nil
	}
	raw, err := json.Marshal(ev.Payload)
	if err != nil {
		return events.MessageReceivedPayload{}, err
	}
	var p events.MessageReceivedPayload
	return p, json.Unmarshal(raw, &p)
}

func declared(ev events.Event) events.MessageMedia {
	if p, err := receivedPayload(ev); err == nil && p.Media != nil {
		return *p.Media
	}
	return events.MessageMedia{}
}

func baseType(ct string) string {
	if mt, _, err := mime.ParseMediaType(ct); err == nil {
		return mt
	}
	return ct
}

func extensionOf(ct string) string {
	if exts, err := mime.ExtensionsByType(baseType(ct)); err == nil && len(exts) > 0 {
		return exts[0]
	}
	return ""
}
