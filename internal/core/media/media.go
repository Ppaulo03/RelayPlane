// Package media implements the Claim-Check pattern rules: binary payloads
// live in the BlobStore and only a validated reference travels in commands.
package media

import (
	"fmt"
	"mime"
	"path"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
)

// BlobStatus is the lifecycle of a blob metadata record.
type BlobStatus string

const (
	BlobPending BlobStatus = "PENDING" // reserved, content not yet verified
	BlobReady   BlobStatus = "READY"
	BlobDeleted BlobStatus = "DELETED"
)

// Blob is the metadata of a stored object (table blob_metadata).
type Blob struct {
	ID          string
	TenantID    string
	ObjectKey   string
	ContentType string
	Size        int64
	SHA256      string // hex
	Filename    string
	Status      BlobStatus
	ExpiresAt   time.Time
	CreatedAt   time.Time
	DeletedAt   *time.Time
}

// Ref is the claim check carried inside a command.
type Ref struct {
	ObjectKey   string    `json:"object_key"`
	ContentType string    `json:"content_type"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

// Ref builds the claim check for a blob.
func (b Blob) Ref() Ref {
	return Ref{ObjectKey: b.ObjectKey, ContentType: b.ContentType, Size: b.Size, SHA256: b.SHA256, ExpiresAt: b.ExpiresAt}
}

// Policy restricts which objects may be sent.
type Policy struct {
	MaxBytes       int64
	AllowedTypes   []string // exact ("application/pdf") or wildcard ("image/*")
	InlineMaxBytes int      // max serialized command payload size (broker guard)
}

// DefaultPolicy returns conservative defaults.
func DefaultPolicy() Policy {
	return Policy{
		MaxBytes: 100 << 20,
		AllowedTypes: []string{"image/*", "audio/*", "video/*",
			"application/pdf", "text/plain", "application/zip",
			"application/msword", "application/vnd.openxmlformats-officedocument.*",
			"application/vnd.ms-excel"},
		InlineMaxBytes: 256 << 10,
	}
}

// KeyPrefix is the per-tenant namespace inside the bucket.
func KeyPrefix(tenantID string) string { return tenantID + "/" }

// ObjectKey builds a tenant-namespaced key: <tenant>/media/<id>/<filename>.
func ObjectKey(tenantID, mediaID, filename string) string {
	return KeyPrefix(tenantID) + "media/" + mediaID + "/" + SafeFilename(filename)
}

// SafeFilename strips path components and control characters.
func SafeFilename(name string) string {
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, name)
	if name == "" || name == "." || name == ".." || name == "/" {
		return "file"
	}
	return name
}

// OwnedBy reports whether key lives inside tenantID's namespace. Keys with
// traversal segments never qualify.
func OwnedBy(tenantID, key string) bool {
	if tenantID == "" || strings.Contains(tenantID, "/") {
		return false
	}
	if !strings.HasPrefix(key, KeyPrefix(tenantID)) || len(key) == len(KeyPrefix(tenantID)) {
		return false
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." || seg == "." || seg == "" {
			return false
		}
	}
	return true
}

// TypeAllowed checks a content type against the allow-list.
func (p Policy) TypeAllowed(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	for _, a := range p.AllowedTypes {
		if a == mt {
			return true
		}
		if strings.HasSuffix(a, "*") && strings.HasPrefix(mt, strings.TrimSuffix(a, "*")) {
			return true
		}
	}
	return false
}

// ValidateRef validates a claim check before it enters a command or before a
// worker uses it: tenant ownership, size, MIME, checksum shape, expiry.
func ValidateRef(tenantID string, r Ref, p Policy, now time.Time) error {
	if !OwnedBy(tenantID, r.ObjectKey) {
		return fmt.Errorf("%w: object %q does not belong to tenant", errs.ErrForbidden, r.ObjectKey)
	}
	if r.Size <= 0 || (p.MaxBytes > 0 && r.Size > p.MaxBytes) {
		return fmt.Errorf("%w: media size %d out of bounds", errs.ErrInvalidArgument, r.Size)
	}
	if !p.TypeAllowed(r.ContentType) {
		return fmt.Errorf("%w: content type %q not allowed", errs.ErrInvalidArgument, r.ContentType)
	}
	if len(r.SHA256) != 64 || strings.Trim(r.SHA256, "0123456789abcdef") != "" {
		return fmt.Errorf("%w: sha256 must be 64 lowercase hex chars", errs.ErrInvalidArgument)
	}
	if !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt) {
		return fmt.Errorf("%w: media reference expired at %s", errs.ErrInvalidArgument, r.ExpiresAt.Format(time.RFC3339))
	}
	return nil
}

// EnforceInlineLimit implements INV-11: a serialized command payload larger
// than the inline limit must never reach the broker.
func EnforceInlineLimit(serialized []byte, limit int) error {
	if limit > 0 && len(serialized) > limit {
		return fmt.Errorf("%w: %d bytes > %d (use the BlobStore claim check)", errs.ErrPayloadTooLarge, len(serialized), limit)
	}
	return nil
}
