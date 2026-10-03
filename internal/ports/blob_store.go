package ports

import (
	"context"
	"io"
	"time"
)

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Key          string
	Size         int64
	ContentType  string
	LastModified time.Time
}

// SignedOp selects what a signed URL permits.
type SignedOp string

const (
	SignedGet SignedOp = "GET"
	SignedPut SignedOp = "PUT"
)

// BlobStore is the object storage port (claim-check store).
type BlobStore interface {
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Stat(ctx context.Context, key string) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	SignedURL(ctx context.Context, key string, op SignedOp, ttl time.Duration) (string, error)
	// List calls fn for every object under prefix (lifecycle/orphan cleanup).
	List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error
}
