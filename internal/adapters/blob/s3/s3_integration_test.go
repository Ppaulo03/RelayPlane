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
		ep = "127.0.0.1:59010"
	}
	s, err := s3.New(context.Background(), s3.Config{Endpoint: ep, AccessKey: "relayplane", SecretKey: "relayplane-secret",
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
