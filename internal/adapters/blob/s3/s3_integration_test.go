//go:build integration

package s3_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/relayplane/relayplane/internal/adapters/blob/s3"
	"github.com/relayplane/relayplane/internal/contracttest"
	"github.com/relayplane/relayplane/internal/ports"
)

func newStore(t *testing.T) *s3.Store {
	t.Helper()
	ep := os.Getenv("RELAYPLANE_TEST_S3_ENDPOINT")
	if ep == "" {
		ep = "127.0.0.1:59011" // RustFS (default backend); see `make test-integration-s3` for all of them
	}
	ak, sk := os.Getenv("RELAYPLANE_TEST_S3_ACCESS_KEY"), os.Getenv("RELAYPLANE_TEST_S3_SECRET_KEY")
	if ak == "" {
		ak, sk = "relayplane", "relayplane-secret"
	}
	s, err := s3.New(context.Background(), s3.Config{Endpoint: ep, AccessKey: ak, SecretKey: sk,
		Bucket: fmt.Sprintf("rptest-%d", time.Now().UnixNano()), LifecycleDays: 7})
	if err != nil {
		t.Skipf("s3 unavailable: %v", err)
	}
	return s
}

func TestBlobStoreContract(t *testing.T) {
	contracttest.BlobStoreContract(t, func(t *testing.T) ports.BlobStore { return newStore(t) })
}

func TestSignedURLsWork(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	putURL, err := s.SignedURL(ctx, "t1/media/m1/a.txt", ports.SignedPut, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPut, putURL, strings.NewReader("signed upload"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode/100 != 2 {
		t.Fatalf("signed PUT: %v %v", resp, err)
	}
	getURL, _ := s.SignedURL(ctx, "t1/media/m1/a.txt", ports.SignedGet, time.Minute)
	resp, err = http.Get(getURL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "signed upload" {
		t.Fatalf("signed GET: %q", body)
	}
	bad, _ := http.Get(strings.Replace(getURL, "a.txt", "b.txt", 1))
	if bad.StatusCode/100 == 2 {
		t.Fatal("a signed URL must not authorise other objects")
	}
}

// 15.9 Real MinIO: declared 3 bytes, body of 10. The store must refuse and keep nothing.
func TestOversizedBodyIsRejectedByTheRealStore(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	if err := s.Put(ctx, "t1/media/ok/a.txt", strings.NewReader("abc"), 3, "text/plain"); err != nil {
		t.Fatalf("a valid upload must work on this backend before the negative cases mean anything: %v", err)
	}
	key := "t1/media/over/a.txt"
	if err := s.Put(ctx, key, strings.NewReader("0123456789"), 3, "text/plain"); err == nil {
		t.Fatal("a body longer than the declared size was accepted")
	}
	if _, err := s.Stat(ctx, key); err == nil {
		t.Fatal("an object from the rejected upload exists")
	}
	// a body shorter than declared is refused too
	if err := s.Put(ctx, "t1/media/short/a.txt", strings.NewReader("ab"), 5, "text/plain"); err == nil {
		t.Fatal("a body shorter than the declared size was accepted")
	}
	if _, err := s.Stat(ctx, "t1/media/short/a.txt"); err == nil {
		t.Fatal("an object from the truncated upload exists")
	}
}
