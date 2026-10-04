// Package postgres implements the persistence ports on PostgreSQL (pgx).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
	"github.com/relayplane/relayplane/migrations"
)

// Store is the PostgreSQL implementation of every repository port.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects and verifies the database.
func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse url: %w", err)
	}
	cfg.ConnConfig.Tracer = queryTracer{}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Ping implements the readiness probe.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Pool exposes the pool for tests.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Repositories returns the port bundle.
func (s *Store) Repositories() ports.Repositories {
	return ports.Repositories{
		Tenants: tenantRepo{s}, APIKeys: apiKeyRepo{s}, Instances: instanceRepo{s}, Nodes: nodeRepo{s},
		Operations: opRepo{s}, Messages: msgRepo{s}, Blobs: blobRepo{s},
		Idempotency: idemRepo{s}, Dedup: dedupRepo{s},
		Events: eventsRepo{s}, Subscriptions: subsRepo{s}, Deliveries: deliveriesRepo{s}, InboundMedia: inboundMediaRepo{s},
		Erasures: erasureRepo{s},
	}
}

// Migrate applies pending migrations in order, serialised by an advisory lock
// so that several processes can start concurrently.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	const lockKey = 727274 // "rp"
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockKey) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sql" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	for _, name := range files {
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sqlBytes, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Reset truncates every table (tests only).
func (s *Store) Reset(ctx context.Context) error {
	var err error
	// the previous test's goroutines may still be finishing a statement: TRUNCATE then deadlocks with them (40P01). The
	// victim is always this statement, so trying again once they are done is safe.
	for attempt := 0; attempt < 10; attempt++ {
		_, err = s.pool.Exec(ctx, `TRUNCATE outbound_messages, operations, instance_assignments, instances,
			api_keys, provider_nodes, idempotency_keys, event_deduplication, blob_metadata, event_outbox, webhook_deliveries, delivery_sequences, inbound_media, contact_erasures,
			subscriptions, tenants CASCADE`)
		if _, code := constraint(err); err == nil || code != "40P01" {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

// ---- helpers ----

type queryTracer struct{}

type spanKey struct{}

func (queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	ctx, span := observability.Start(ctx, "db.query", attribute.String("db.system", "postgresql"))
	return context.WithValue(ctx, spanKey{}, span)
}

func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if sp, ok := ctx.Value(spanKey{}).(trace.Span); ok {
		if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
			observability.Fail(sp, data.Err)
		}
		sp.End()
	}
}

// withTx runs fn in a transaction, retrying serialization/deadlock failures.
func (s *Store) withTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		err = s.runTx(ctx, fn)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && (pe.Code == "40001" || pe.Code == "40P01") {
			time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
			continue
		}
		return err
	}
	return err
}

func (s *Store) runTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	return tx.Commit(ctx)
}

func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return errs.ErrNotFound
	}
	return err
}

// constraint returns the violated constraint/index name and SQLSTATE class of err.
func constraint(err error) (name, code string) {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.ConstraintName, pe.Code
	}
	return "", ""
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func zeroIfNil(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
