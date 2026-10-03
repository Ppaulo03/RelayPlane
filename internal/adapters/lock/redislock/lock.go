// Package redislock implements ports.Locker with single-instance Redis leases
// (SET NX PX + token-checked renew/release). Locks only reduce duplicated
// work; correctness always rests on compare-and-set in the catalog.
package redislock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/relayplane/relayplane/internal/ports"
)

// Locker is a Redis-backed ports.Locker.
type Locker struct {
	rdb    redis.UniversalClient
	prefix string
}

// New returns a Locker; keys are stored under prefix.
func New(rdb redis.UniversalClient, prefix string) *Locker {
	if prefix == "" {
		prefix = "relayplane"
	}
	return &Locker{rdb: rdb, prefix: prefix + ":lock:"}
}

var (
	extend  = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	release = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
)

type lease struct {
	l     *Locker
	key   string
	token string
}

// TryLock implements ports.Locker.
func (l *Locker) TryLock(ctx context.Context, name string, ttl time.Duration) (ports.Lease, bool, error) {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	token := hex.EncodeToString(b)
	ok, err := l.rdb.SetNX(ctx, l.prefix+name, token, ttl).Result()
	if err != nil || !ok {
		return nil, false, err
	}
	return &lease{l: l, key: l.prefix + name, token: token}, true, nil
}

func (s *lease) Extend(ctx context.Context, ttl time.Duration) error {
	n, err := extend.Run(ctx, s.l.rdb, []string{s.key}, s.token, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("lease lost")
	}
	return nil
}

func (s *lease) Release(ctx context.Context) error {
	return release.Run(ctx, s.l.rdb, []string{s.key}, s.token).Err()
}
