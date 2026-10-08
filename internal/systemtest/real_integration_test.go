//go:build integration

package systemtest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/relayplane/relayplane/internal/adapters/blob/s3"
	"github.com/relayplane/relayplane/internal/adapters/lock/redislock"
	"github.com/relayplane/relayplane/internal/adapters/messaging/redisstreams"
	"github.com/relayplane/relayplane/internal/adapters/persistence/postgres"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func init() {
	RealBackend = func(t *testing.T) *Backend {
		ctx := context.Background()
		st, err := postgres.Open(ctx, envOr("RELAYPLANE_TEST_DATABASE_URL", "postgres://relayplane:relayplane@127.0.0.1:55440/relayplane_test?sslmode=disable"))
		if err != nil {
			t.Skipf("postgres unavailable: %v", err)
		}
		t.Cleanup(st.Close)
		if err := st.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if err := st.Reset(ctx); err != nil {
			t.Fatal(err)
		}
		rdb := redis.NewClient(&redis.Options{Addr: envOr("RELAYPLANE_TEST_REDIS_ADDR", "127.0.0.1:56390")})
		if err := rdb.Ping(ctx).Err(); err != nil {
			t.Skipf("redis unavailable: %v", err)
		}
		t.Cleanup(func() { rdb.Close() })
		prefix := fmt.Sprintf("rpsys%d", time.Now().UnixNano())
		q, err := redisstreams.NewQueue(ctx, rdb, redisstreams.QueueConfig{Prefix: prefix, Partitions: 8, InlineMaxBytes: InlineLimit,
			LeaseTTL: time.Second, Block: 30 * time.Millisecond, DefaultRetryDelay: 20 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		blob, err := s3.New(ctx, s3.Config{Endpoint: envOr("RELAYPLANE_TEST_S3_ENDPOINT", "127.0.0.1:59011"),
			AccessKey: envOr("RELAYPLANE_TEST_S3_ACCESS_KEY", "relayplane"), SecretKey: envOr("RELAYPLANE_TEST_S3_SECRET_KEY", "relayplane-secret"),
			Bucket: fmt.Sprintf("rpsys-%d", time.Now().UnixNano())})
		if err != nil {
			t.Skipf("s3 unavailable: %v", err)
		}
		return &Backend{Repos: st.Repositories(), Queue: q,
			Blob: blob, Locker: redislock.New(rdb, prefix)}
	}
}
