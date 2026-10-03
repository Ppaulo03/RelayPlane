// Package s3 implements ports.BlobStore for any S3-compatible service
// (AWS S3, MinIO, ...) using minio-go.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"go.opentelemetry.io/otel/attribute"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// Config configures the store.
type Config struct {
	Endpoint       string // host:port used by the gateway/workers, e.g. minio:9000
	PublicEndpoint string // host:port used in signed URLs handed to clients/providers (defaults to Endpoint)
	AccessKey      string
	SecretKey      string
	Bucket         string
	Region         string // default us-east-1
	UseSSL         bool
	PublicUseSSL   bool
	LifecycleDays  int // bucket-level expiry safety net (0 = none)
}

// Store is the S3 BlobStore.
type Store struct {
	c      *minio.Client
	signer *minio.Client
	bucket string
}

// New connects, creates the bucket when missing and applies the lifecycle rule.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	mk := func(endpoint string, ssl bool) (*minio.Client, error) {
		return minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: ssl, Region: cfg.Region})
	}
	c, err := mk(cfg.Endpoint, cfg.UseSSL)
	if err != nil {
		return nil, err
	}
	signer := c
	if cfg.PublicEndpoint != "" && cfg.PublicEndpoint != cfg.Endpoint {
		if signer, err = mk(cfg.PublicEndpoint, cfg.PublicUseSSL); err != nil {
			return nil, err
		}
	}
	exists, err := c.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := c.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: cfg.Region}); err != nil {
			return nil, err
		}
	}
	if cfg.LifecycleDays > 0 {
		lc := lifecycle.NewConfiguration()
		lc.Rules = []lifecycle.Rule{{ID: "relayplane-expire", Status: "Enabled", Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(cfg.LifecycleDays)}}}
		if err := c.SetBucketLifecycle(ctx, cfg.Bucket, lc); err != nil {
			return nil, err
		}
	}
	return &Store{c: c, signer: signer, bucket: cfg.Bucket}, nil
}

func translate(err error) error {
	if err == nil {
		return nil
	}
	var r minio.ErrorResponse
	if errors.As(err, &r) && (r.Code == "NoSuchKey" || r.StatusCode == 404) {
		return errs.ErrNotFound
	}
	return err
}

// Ping is the readiness probe.
func (s *Store) Ping(ctx context.Context) error {
	_, err := s.c.BucketExists(ctx, s.bucket)
	return err
}

func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	ctx, span := observability.Start(ctx, "blob.put", attribute.String("blob.system", "s3"))
	defer span.End()
	// The declared size is a contract: exactReader serves exactly `size` bytes, hides io.Seeker/ReaderAt
	// (minio-go may rewind a seekable reader) and notices whether the source had MORE data, which
	// minio-go would otherwise silently truncate (or fail with a transport error, depending on the path).
	src := &exactReader{r: r, left: size, enforce: size >= 0}
	_, err := s.c.PutObject(ctx, s.bucket, key, src, size, minio.PutObjectOptions{ContentType: contentType})
	if src.over {
		_ = s.c.RemoveObject(context.WithoutCancel(ctx), s.bucket, key, minio.RemoveObjectOptions{})
		err = fmt.Errorf("%w: body is longer than the declared size (%d bytes)", errs.ErrInvalidArgument, size)
	}
	observability.Fail(span, err)
	return err
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, span := observability.Start(ctx, "blob.get", attribute.String("blob.system", "s3"))
	defer span.End()
	obj, err := s.c.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, translate(err)
	}
	if _, err := obj.Stat(); err != nil { // GetObject is lazy: surface "not found" now
		_ = obj.Close()
		return nil, translate(err)
	}
	return obj, nil
}

func (s *Store) Stat(ctx context.Context, key string) (*ports.ObjectInfo, error) {
	info, err := s.c.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, translate(err)
	}
	return &ports.ObjectInfo{Key: key, Size: info.Size, ContentType: info.ContentType, LastModified: info.LastModified}, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.c.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if errors.Is(translate(err), errs.ErrNotFound) {
		return nil
	}
	return err
}

func (s *Store) SignedURL(ctx context.Context, key string, op ports.SignedOp, ttl time.Duration) (string, error) {
	var u *url.URL
	var err error
	if op == ports.SignedPut {
		u, err = s.signer.PresignedPutObject(ctx, s.bucket, key, ttl)
	} else {
		u, err = s.signer.PresignedGetObject(ctx, s.bucket, key, ttl, nil)
	}
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *Store) List(ctx context.Context, prefix string, fn func(ports.ObjectInfo) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for o := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			return o.Err
		}
		if strings.HasSuffix(o.Key, "/") {
			continue
		}
		if err := fn(ports.ObjectInfo{Key: o.Key, Size: o.Size, ContentType: o.ContentType, LastModified: o.LastModified}); err != nil {
			return err
		}
	}
	return nil
}

// exactReader yields at most `left` bytes and records whether the source still had data after that.
type exactReader struct {
	r       io.Reader
	left    int64
	enforce bool
	over    bool
}

func (e *exactReader) Read(p []byte) (int, error) {
	if !e.enforce {
		return e.r.Read(p)
	}
	if e.left <= 0 {
		var probe [1]byte
		if n, _ := e.r.Read(probe[:]); n > 0 {
			e.over = true
		}
		return 0, io.EOF
	}
	if int64(len(p)) > e.left {
		p = p[:e.left]
	}
	n, err := e.r.Read(p)
	e.left -= int64(n)
	return n, err
}
