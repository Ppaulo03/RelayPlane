package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// MediaService implements the claim-check upload flow and blob lifecycle.
type MediaService struct{ d Deps }

// UploadRequest declares an object before it is uploaded.
type UploadRequest struct {
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
	Filename    string `json:"filename"`
}

// UploadTicket tells the client where to put the bytes. Uploads always go
// through the gateway (PUT content_url): it enforces the declared size while
// streaming, which a presigned PUT cannot do (it would let a client store an
// arbitrarily large object under a small declaration).
type UploadTicket struct {
	MediaID    string    `json:"media_id"`
	ObjectKey  string    `json:"object_key"`
	ContentURL string    `json:"content_url"`
	ExpiresAt  time.Time `json:"expires_at"` // the upload must finish before this
}

// CreateUpload validates the declaration against the policy and reserves a
// tenant-namespaced object key. The tenant is derived from authentication.
func (s *MediaService) CreateUpload(ctx context.Context, tenantID string, req UploadRequest) (*UploadTicket, error) {
	pol := s.d.Cfg.MediaPolicy
	req.SHA256 = strings.ToLower(req.SHA256)
	switch {
	case req.Size <= 0 || (pol.MaxBytes > 0 && req.Size > pol.MaxBytes):
		return nil, fmt.Errorf("%w: size must be between 1 and %d bytes", errs.ErrInvalidArgument, pol.MaxBytes)
	case !pol.TypeAllowed(req.ContentType):
		return nil, fmt.Errorf("%w: content type %q not allowed", errs.ErrInvalidArgument, req.ContentType)
	case len(req.SHA256) != 64 || strings.Trim(req.SHA256, "0123456789abcdef") != "":
		return nil, fmt.Errorf("%w: sha256 must be 64 hex characters", errs.ErrInvalidArgument)
	}
	id := ids.New("med")
	now := s.d.now()
	b := media.Blob{ID: id, TenantID: tenantID, ObjectKey: media.ObjectKey(tenantID, id, req.Filename),
		ContentType: req.ContentType, Size: req.Size, SHA256: req.SHA256, Filename: media.SafeFilename(req.Filename),
		Status: media.BlobPending, ExpiresAt: now.Add(s.d.Cfg.PendingTTL), CreatedAt: now}
	if err := s.d.Repos.Blobs.Create(ctx, b); err != nil {
		return nil, err
	}
	return &UploadTicket{MediaID: id, ObjectKey: b.ObjectKey, ContentURL: "/api/v1/media/" + id + "/content", ExpiresAt: b.ExpiresAt}, nil
}

func (s *MediaService) load(ctx context.Context, tenantID, id string) (*media.Blob, error) {
	b, err := s.d.Repos.Blobs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if b.TenantID != tenantID || b.Status == media.BlobDeleted {
		return nil, errs.ErrNotFound
	}
	return b, nil
}

type countingHash struct {
	h io.Writer
	n int64
}

func (c *countingHash) Write(p []byte) (int, error) { c.n += int64(len(p)); return c.h.Write(p) }

// Upload streams the body through the gateway into the blob store without
// buffering it, verifying size and checksum against the declaration.
func (s *MediaService) Upload(ctx context.Context, tenantID, id string, body io.Reader) (*media.Blob, error) {
	ctx, span := observability.Start(ctx, "media.upload")
	defer span.End()
	b, err := s.load(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if b.Status != media.BlobPending {
		return nil, fmt.Errorf("%w: media already uploaded", errs.ErrConflict)
	}
	h := sha256.New()
	cw := &countingHash{h: h}
	limited := io.LimitReader(body, b.Size+1) // one extra byte exposes oversized bodies
	if err := s.d.Blob.Put(ctx, b.ObjectKey, io.TeeReader(limited, cw), b.Size, b.ContentType); err != nil {
		_ = s.d.Blob.Delete(ctx, b.ObjectKey)
		if cw.n > b.Size { // the body outgrew its declaration; the store aborted the upload
			return nil, fmt.Errorf("%w: uploaded content is larger than the declared size", errs.ErrInvalidArgument)
		}
		return nil, fmt.Errorf("store media: %w", err)
	}
	s.d.Metrics.BlobBytes.WithLabelValues("put").Add(float64(cw.n))
	if cw.n == b.Size { // defence in depth: bytes left in the (size+1)-capped reader mean the body exceeded the declaration
		var extra [1]byte
		if n, _ := limited.Read(extra[:]); n > 0 {
			_ = s.d.Blob.Delete(ctx, b.ObjectKey)
			return nil, fmt.Errorf("%w: uploaded content is larger than the declared size", errs.ErrInvalidArgument)
		}
	}
	if cw.n != b.Size || hex.EncodeToString(h.Sum(nil)) != b.SHA256 {
		_ = s.d.Blob.Delete(ctx, b.ObjectKey)
		return nil, fmt.Errorf("%w: uploaded content does not match the declared size/sha256", errs.ErrInvalidArgument)
	}
	retain := s.d.now().Add(s.d.Cfg.MediaTTL)
	if err := s.d.Repos.Blobs.MarkReady(ctx, b.ID, b.Size, b.SHA256, retain); err != nil {
		return nil, err
	}
	b.Status, b.ExpiresAt = media.BlobReady, retain
	return b, nil
}

// Get returns blob metadata.
func (s *MediaService) Get(ctx context.Context, tenantID, id string) (*media.Blob, error) {
	return s.load(ctx, tenantID, id)
}

// Delete removes the object and marks the metadata deleted.
func (s *MediaService) Delete(ctx context.Context, tenantID, id string) error {
	b, err := s.load(ctx, tenantID, id)
	if err != nil {
		return err
	}
	if err := s.d.Blob.Delete(ctx, b.ObjectKey); err != nil {
		return err
	}
	return s.d.Repos.Blobs.MarkDeleted(ctx, b.ID, s.d.now())
}

// Cleanup enforces blob lifecycle: expired blobs are deleted and objects with
// no metadata (orphans: failed uploads, crashed gateways) older than
// orphanGrace are removed.
func (s *MediaService) Cleanup(ctx context.Context, orphanGrace time.Duration, limit int) (expired, orphans int, err error) {
	ctx, span := observability.Start(ctx, "media.cleanup")
	defer span.End()
	now := s.d.now()
	blobs, err := s.d.Repos.Blobs.ListExpired(ctx, now, limit)
	if err != nil {
		return 0, 0, err
	}
	for _, b := range blobs {
		if err := s.d.Blob.Delete(ctx, b.ObjectKey); err != nil {
			s.d.Log.WarnContext(ctx, "blob delete failed", "object_key", b.ObjectKey, "error", err)
			continue
		}
		if err := s.d.Repos.Blobs.MarkDeleted(ctx, b.ID, now); err != nil {
			return expired, orphans, err
		}
		s.d.Metrics.BlobCleanupTotal.WithLabelValues("expired").Inc()
		expired++
	}
	var stale []string
	lerr := s.d.Blob.List(ctx, "", func(o ports.ObjectInfo) error {
		if now.Sub(o.LastModified) < orphanGrace {
			return nil
		}
		if _, gerr := s.d.Repos.Blobs.GetByKey(ctx, o.Key); errors.Is(gerr, errs.ErrNotFound) {
			stale = append(stale, o.Key)
		}
		return nil
	})
	if lerr != nil {
		return expired, orphans, lerr
	}
	for _, k := range stale {
		if err := s.d.Blob.Delete(ctx, k); err == nil {
			s.d.Metrics.BlobCleanupTotal.WithLabelValues("orphan").Inc()
			orphans++
		}
	}
	return expired, orphans, nil
}
