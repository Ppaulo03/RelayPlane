package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/ports"
)

// ---------------- operations ----------------

type opRepo struct{ s *Store }

const opCols = `id,tenant_id,COALESCE(instance_id,''),type,status,step,target_node_id,source_node_id,source_epoch,
	error_code,error_message,attempts,created_at,updated_at,step_started_at,completed_at`

func scanOp(row pgx.Row) (*instance.Operation, error) {
	var o instance.Operation
	var t, st string
	if err := row.Scan(&o.ID, &o.TenantID, &o.InstanceID, &t, &st, &o.Step, &o.TargetNodeID, &o.SourceNodeID, &o.SourceEpoch,
		&o.ErrorCode, &o.ErrorMessage, &o.Attempts, &o.CreatedAt, &o.UpdatedAt, &o.StepStartedAt, &o.CompletedAt); err != nil {
		return nil, notFound(err)
	}
	o.Type, o.Status = instance.OperationType(t), instance.OperationStatus(st)
	return &o, nil
}

func (r opRepo) Create(ctx context.Context, op instance.Operation) error {
	if op.CreatedAt.IsZero() {
		op.CreatedAt = time.Now()
	}
	var inst any
	if op.InstanceID != "" {
		inst = op.InstanceID
	}
	_, err := r.s.pool.Exec(ctx, `INSERT INTO operations(id,tenant_id,instance_id,type,status,step,target_node_id,source_node_id,source_epoch,
		error_code,error_message,attempts,created_at,updated_at,step_started_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13,$13)`,
		op.ID, op.TenantID, inst, string(op.Type), string(op.Status), op.Step, op.TargetNodeID, op.SourceNodeID, op.SourceEpoch,
		op.ErrorCode, op.ErrorMessage, op.Attempts, op.CreatedAt)
	if name, code := constraint(err); code == "23505" {
		if name == "one_active_migration_per_instance" {
			return errs.ErrInProgress
		}
		return errs.ErrAlreadyExists
	}
	return err
}

func (r opRepo) Get(ctx context.Context, id string) (*instance.Operation, error) {
	return scanOp(r.s.pool.QueryRow(ctx, `SELECT `+opCols+` FROM operations WHERE id=$1`, id))
}

func (r opRepo) Advance(ctx context.Context, id, from, to string, st instance.OperationStatus, p ports.OperationPatch) (*instance.Operation, error) {
	var out *instance.Operation
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		o, err := scanOp(tx.QueryRow(ctx, `SELECT `+opCols+` FROM operations WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		if !o.Status.IsActive() {
			return fmt.Errorf("%w: %w (%s)", errs.ErrConflict, errs.ErrAlreadyTerminal, o.Status)
		}
		if o.Step != from {
			return fmt.Errorf("%w: operation step is %q, expected %q", errs.ErrConflict, o.Step, from)
		}
		if o.Type == instance.OpMigrate && from != to && !ownership.CanMigrate(ownership.MigrationStep(from), ownership.MigrationStep(to)) {
			return fmt.Errorf("%w: migration %s -> %s", errs.ErrInvalidTransition, from, to)
		}
		code, msg := o.ErrorCode, o.ErrorMessage
		if p.ErrorCode != "" || st != instance.OpBlocked {
			code, msg = p.ErrorCode, p.ErrorMessage
		}
		attempts := o.Attempts
		if p.BumpAttempts {
			attempts++
		}
		target := o.TargetNodeID
		if p.TargetNodeID != "" {
			target = p.TargetNodeID
		}
		started := o.StepStartedAt
		if from != to {
			started = time.Now()
		}
		out, err = scanOp(tx.QueryRow(ctx, `UPDATE operations SET step=$2,status=$3,error_code=$4,error_message=$5,attempts=$6,
			target_node_id=$7,step_started_at=$8,updated_at=now() WHERE id=$1 RETURNING `+opCols, id, to, string(st), code, msg, attempts, target, started))
		return err
	})
	return out, err
}

func (r opRepo) Complete(ctx context.Context, id string, st instance.OperationStatus, code, msg string, at time.Time) error {
	// compare-and-set on the status: a finished operation is immutable
	tag, err := r.s.pool.Exec(ctx, `UPDATE operations SET status=$2,error_code=$3,error_message=$4,completed_at=$5,updated_at=$5
		WHERE id=$1 AND status IN ('PENDING','RUNNING','BLOCKED')`, id, string(st), code, msg, at)
	if err == nil && tag.RowsAffected() == 0 {
		cur, gerr := r.Get(ctx, id)
		if gerr != nil {
			return gerr
		}
		return fmt.Errorf("%w: %w (%s)", errs.ErrConflict, errs.ErrAlreadyTerminal, cur.Status)
	}
	return err
}

func (r opRepo) FindActive(ctx context.Context, instanceID string, t instance.OperationType) (*instance.Operation, error) {
	return scanOp(r.s.pool.QueryRow(ctx, `SELECT `+opCols+` FROM operations
		WHERE instance_id=$1 AND type=$2 AND status IN ('PENDING','RUNNING','BLOCKED') ORDER BY created_at LIMIT 1`, instanceID, string(t)))
}

func (r opRepo) ListActive(ctx context.Context, t instance.OperationType, limit int) ([]instance.Operation, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT `+opCols+` FROM operations
		WHERE type=$1 AND status IN ('PENDING','RUNNING','BLOCKED') ORDER BY created_at LIMIT $2`, string(t), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []instance.Operation
	for rows.Next() {
		o, err := scanOp(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

// ---------------- messages ----------------

type msgRepo struct{ s *Store }

const msgCols = `id,tenant_id,instance_id,idempotency_key,node_id,assignment_epoch,partition_key,recipient,type,payload,status,
	provider_message_id,attempt_count,error_code,error_message,created_at,updated_at,sequence_no`

func scanMsg(row pgx.Row) (*messaging.Message, error) {
	var m messaging.Message
	var t, st string
	var payload []byte
	if err := row.Scan(&m.ID, &m.TenantID, &m.InstanceID, &m.IdempotencyKey, &m.NodeID, &m.AssignmentEpoch, &m.PartitionKey,
		&m.Recipient, &t, &payload, &st, &m.ProviderMessageID, &m.AttemptCount, &m.ErrorCode, &m.ErrorMessage, &m.CreatedAt, &m.UpdatedAt, &m.SequenceNo); err != nil {
		return nil, notFound(err)
	}
	m.Type, m.Status, m.Payload = messaging.Type(t), messaging.Status(st), json.RawMessage(payload)
	return &m, nil
}

func (r msgRepo) Create(ctx context.Context, m messaging.Message) error {
	_, err := r.create(ctx, m, nil)
	return err
}

func (r msgRepo) CreateWithOutbox(ctx context.Context, m messaging.Message, build func(seq int64) ([]byte, error)) (int64, error) {
	return r.create(ctx, m, build)
}

func (r msgRepo) create(ctx context.Context, m messaging.Message, build func(seq int64) ([]byte, error)) (int64, error) {
	if m.Status == "" {
		m.Status = messaging.StatusQueued
	}
	payload := []byte(m.Payload)
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	var seq int64
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		// The UPDATE takes the instance row lock until commit: concurrent senders
		// to the same instance serialise here, so sequence order == commit order.
		var next int64
		if err := tx.QueryRow(ctx, `UPDATE instances SET next_sequence = next_sequence + 1 WHERE id=$1 RETURNING next_sequence - 1`, m.InstanceID).Scan(&next); err != nil {
			return notFound(err)
		}
		seq = next
		var cmd []byte
		if build != nil {
			var err error
			if cmd, err = build(seq); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO outbound_messages(id,tenant_id,instance_id,idempotency_key,node_id,assignment_epoch,
			partition_key,recipient,type,payload,status,sequence_no) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			m.ID, m.TenantID, m.InstanceID, m.IdempotencyKey, m.NodeID, m.AssignmentEpoch, m.PartitionKey, m.Recipient,
			string(m.Type), payload, string(m.Status), seq); err != nil {
			if _, code := constraint(err); code == "23505" {
				return errs.ErrAlreadyExists
			}
			return err
		}
		if build != nil {
			if _, err := tx.Exec(ctx, `INSERT INTO outbox(instance_id,sequence_no,message_id,command) VALUES($1,$2,$3,$4)`, m.InstanceID, seq, m.ID, cmd); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (r msgRepo) Get(ctx context.Context, id string) (*messaging.Message, error) {
	return scanMsg(r.s.pool.QueryRow(ctx, `SELECT `+msgCols+` FROM outbound_messages WHERE id=$1`, id))
}

func (r msgRepo) Transition(ctx context.Context, id string, from []messaging.Status, to messaging.Status, p ports.MessagePatch) (*messaging.Message, error) {
	var out *messaging.Message
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		m, err := scanMsg(tx.QueryRow(ctx, `SELECT `+msgCols+` FROM outbound_messages WHERE id=$1 FOR UPDATE`, id))
		if err != nil {
			return err
		}
		allowed := false
		for _, f := range from {
			if m.Status == f {
				allowed = true
			}
		}
		if !allowed {
			return fmt.Errorf("%w: message %s is %s", errs.ErrConflict, id, m.Status)
		}
		if !messaging.CanTransition(m.Status, to) {
			return fmt.Errorf("%w: message %s -> %s", errs.ErrInvalidTransition, m.Status, to)
		}
		pm := m.ProviderMessageID
		if p.ProviderMessageID != "" {
			pm = p.ProviderMessageID
		}
		code, msg := m.ErrorCode, m.ErrorMessage
		if p.ErrorCode != "" || to == messaging.StatusFailed || to == messaging.StatusUnknown {
			code, msg = p.ErrorCode, p.ErrorMessage
		}
		attempts := m.AttemptCount
		if p.BumpAttempt {
			attempts++
		}
		out, err = scanMsg(tx.QueryRow(ctx, `UPDATE outbound_messages SET status=$2,provider_message_id=$3,error_code=$4,error_message=$5,
			attempt_count=$6,updated_at=now() WHERE id=$1 RETURNING `+msgCols, id, string(to), pm, code, msg, attempts))
		return err
	})
	return out, err
}

var receiptRank = map[messaging.Status]int{
	messaging.StatusUnknown: 0, messaging.StatusAccepted: 1, messaging.StatusDelivered: 2, messaging.StatusRead: 3,
}

func (r msgRepo) ApplyProviderStatus(ctx context.Context, instanceID, pmid string, to messaging.Status) (bool, error) {
	applied := false
	err := r.s.withTx(ctx, func(tx pgx.Tx) error {
		var id, cur string
		err := tx.QueryRow(ctx, `SELECT id,status FROM outbound_messages WHERE instance_id=$1 AND provider_message_id=$2 FOR UPDATE`,
			instanceID, pmid).Scan(&id, &cur)
		if err != nil {
			return notFound(err)
		}
		if to == messaging.StatusFailed {
			if cur != string(messaging.StatusAccepted) && cur != string(messaging.StatusUnknown) {
				applied = false
				return nil
			}
			applied = true
			_, err = tx.Exec(ctx, `UPDATE outbound_messages SET status='FAILED',error_code='PROVIDER_FAILED',
				error_message='provider reported the message as failed',updated_at=now() WHERE id=$1`, id)
			return err
		}
		c, okc := receiptRank[messaging.Status(cur)]
		n, okn := receiptRank[to]
		if !okc || !okn || n <= c {
			applied = false
			return nil
		}
		applied = true
		_, err = tx.Exec(ctx, `UPDATE outbound_messages SET status=$2,updated_at=now() WHERE id=$1`, id, string(to))
		return err
	})
	return applied, err
}

func scanOutbox(rows pgx.Rows) ([]messaging.OutboxEntry, error) {
	defer rows.Close()
	var out []messaging.OutboxEntry
	for rows.Next() {
		var e messaging.OutboxEntry
		var cmd []byte
		var disp *time.Time
		if err := rows.Scan(&e.InstanceID, &e.Sequence, &e.MessageID, &cmd, &e.CreatedAt, &disp); err != nil {
			return nil, err
		}
		e.Command, e.DispatchedAt = json.RawMessage(cmd), zeroIfNil(disp)
		out = append(out, e)
	}
	return out, rows.Err()
}

const outboxCols = `instance_id,sequence_no,message_id,command,created_at,dispatched_at`

func (r msgRepo) ListOutbox(ctx context.Context, instanceID string, limit int) ([]messaging.OutboxEntry, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT `+outboxCols+` FROM outbox WHERE instance_id=$1 AND dispatched_at IS NULL ORDER BY sequence_no LIMIT $2`, instanceID, limit)
	if err != nil {
		return nil, err
	}
	return scanOutbox(rows)
}

func (r msgRepo) ListInstancesWithPendingOutbox(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT DISTINCT instance_id FROM outbox WHERE dispatched_at IS NULL ORDER BY instance_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (r msgRepo) MarkOutboxDispatched(ctx context.Context, instanceID string, seq int64, at time.Time) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE outbox SET dispatched_at=$3 WHERE instance_id=$1 AND sequence_no=$2`, instanceID, seq, at)
	if err == nil && tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return err
}

func (r msgRepo) ListStuckOutbox(ctx context.Context, before time.Time, limit int) ([]messaging.OutboxEntry, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT o.instance_id,o.sequence_no,o.message_id,o.command,o.created_at,o.dispatched_at
		FROM outbox o JOIN outbound_messages m ON m.id=o.message_id
		WHERE o.dispatched_at IS NOT NULL AND o.dispatched_at < $1
		  AND (m.status='QUEUED' OR (m.status='DISPATCHING' AND m.updated_at < $1))
		ORDER BY o.instance_id, o.sequence_no LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	return scanOutbox(rows)
}

func (r msgRepo) PurgeOutbox(ctx context.Context, before time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM outbox o WHERE o.dispatched_at IS NOT NULL AND o.dispatched_at < $1
		AND NOT EXISTS (SELECT 1 FROM outbound_messages m WHERE m.id = o.message_id AND m.status IN ('QUEUED','DISPATCHING'))`, before)
	return tag.RowsAffected(), err
}

func (r msgRepo) ResetOutbox(ctx context.Context, instanceID string, seq int64) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE outbox SET dispatched_at=NULL WHERE instance_id=$1 AND sequence_no=$2`, instanceID, seq)
	if err == nil && tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return err
}

func (r msgRepo) FirstUnresolvedBefore(ctx context.Context, instanceID string, seq int64, unknownTimeout time.Duration) (*messaging.Message, error) {
	// the UNKNOWN age is computed with the database clock (now()), so clock skew
	// between application hosts and the database cannot shorten or extend the barrier
	return scanMsg(r.s.pool.QueryRow(ctx, `SELECT `+msgCols+` FROM outbound_messages
		WHERE instance_id=$1 AND sequence_no > 0 AND sequence_no < $2
		  AND (status IN ('QUEUED','DISPATCHING')
		       OR (status='UNKNOWN' AND ($3::bigint <= 0 OR updated_at > now() - ($3::bigint * interval '1 microsecond'))))
		ORDER BY sequence_no LIMIT 1`, instanceID, seq, unknownTimeout.Microseconds()))
}

// ---------------- blob metadata ----------------

type blobRepo struct{ s *Store }

const blobCols = `id,tenant_id,object_key,content_type,size,sha256,filename,status,expires_at,created_at,deleted_at`

func scanBlob(row pgx.Row) (*media.Blob, error) {
	var b media.Blob
	var st string
	if err := row.Scan(&b.ID, &b.TenantID, &b.ObjectKey, &b.ContentType, &b.Size, &b.SHA256, &b.Filename, &st, &b.ExpiresAt, &b.CreatedAt, &b.DeletedAt); err != nil {
		return nil, notFound(err)
	}
	b.Status = media.BlobStatus(st)
	return &b, nil
}

func (r blobRepo) Create(ctx context.Context, b media.Blob) error {
	if b.Status == "" {
		b.Status = media.BlobPending
	}
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now()
	}
	_, err := r.s.pool.Exec(ctx, `INSERT INTO blob_metadata(id,tenant_id,object_key,content_type,size,sha256,filename,status,expires_at,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		b.ID, b.TenantID, b.ObjectKey, b.ContentType, b.Size, b.SHA256, b.Filename, string(b.Status), b.ExpiresAt, b.CreatedAt)
	if _, code := constraint(err); code == "23505" {
		return errs.ErrAlreadyExists
	}
	return err
}

func (r blobRepo) Get(ctx context.Context, id string) (*media.Blob, error) {
	return scanBlob(r.s.pool.QueryRow(ctx, `SELECT `+blobCols+` FROM blob_metadata WHERE id=$1`, id))
}

func (r blobRepo) GetByKey(ctx context.Context, key string) (*media.Blob, error) {
	return scanBlob(r.s.pool.QueryRow(ctx, `SELECT `+blobCols+` FROM blob_metadata WHERE object_key=$1`, key))
}

func (r blobRepo) MarkReady(ctx context.Context, id string, size int64, sha string, expiresAt time.Time) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE blob_metadata SET status='READY',size=$2,sha256=$3,expires_at=$4 WHERE id=$1 AND status<>'DELETED'`, id, size, sha, expiresAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if _, gerr := r.Get(ctx, id); gerr != nil {
			return gerr
		}
		return errs.ErrConflict
	}
	return nil
}

func (r blobRepo) MarkDeleted(ctx context.Context, id string, at time.Time) error {
	tag, err := r.s.pool.Exec(ctx, `UPDATE blob_metadata SET status='DELETED',deleted_at=$2 WHERE id=$1`, id, at)
	if err == nil && tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return err
}

func (r blobRepo) ListExpired(ctx context.Context, now time.Time, limit int) ([]media.Blob, error) {
	rows, err := r.s.pool.Query(ctx, `SELECT `+blobCols+` FROM blob_metadata WHERE status<>'DELETED' AND expires_at < $1
		ORDER BY expires_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []media.Blob
	for rows.Next() {
		b, err := scanBlob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

// ---------------- idempotency ----------------

type idemRepo struct{ s *Store }

func scanIdem(row pgx.Row) (ports.IdempotencyRecord, error) {
	var r ports.IdempotencyRecord
	var st string
	var res []byte
	err := row.Scan(&r.TenantID, &r.Key, &r.RequestHash, &r.Operation, &r.ResourceID, &st, &res, &r.CreatedAt, &r.UpdatedAt, &r.ExpiresAt)
	r.Status, r.Result = ports.IdempotencyStatus(st), json.RawMessage(res)
	return r, err
}

const idemCols = `tenant_id,key,request_hash,operation,resource_id,status,result,created_at,updated_at,expires_at`

func (r idemRepo) Begin(ctx context.Context, rec ports.IdempotencyRecord) (ports.IdempotencyRecord, bool, error) {
	now := time.Now()
	var claimed bool
	err := r.s.pool.QueryRow(ctx, `INSERT INTO idempotency_keys(tenant_id,key,request_hash,operation,resource_id,status,created_at,updated_at,expires_at)
		VALUES($1,$2,$3,$4,$5,'IN_PROGRESS',$6,$6,$7)
		ON CONFLICT (tenant_id,key) DO UPDATE SET request_hash=EXCLUDED.request_hash, operation=EXCLUDED.operation,
			resource_id=EXCLUDED.resource_id, status='IN_PROGRESS', result=NULL, created_at=EXCLUDED.created_at,
			updated_at=EXCLUDED.updated_at, expires_at=EXCLUDED.expires_at
		WHERE idempotency_keys.expires_at <= $6
		RETURNING true`, rec.TenantID, rec.Key, rec.RequestHash, rec.Operation, rec.ResourceID, now, rec.ExpiresAt).Scan(&claimed)
	if err == nil {
		rec.Status, rec.CreatedAt, rec.UpdatedAt = ports.IdemInProgress, now, now
		return rec, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return rec, false, err
	}
	existing, err := scanIdem(r.s.pool.QueryRow(ctx, `SELECT `+idemCols+` FROM idempotency_keys WHERE tenant_id=$1 AND key=$2`, rec.TenantID, rec.Key))
	return existing, false, notFound(err)
}

func (r idemRepo) Complete(ctx context.Context, tenantID, key string, result json.RawMessage) error {
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	tag, err := r.s.pool.Exec(ctx, `UPDATE idempotency_keys SET status='COMPLETED',result=$3,updated_at=now() WHERE tenant_id=$1 AND key=$2`, tenantID, key, []byte(result))
	if err == nil && tag.RowsAffected() == 0 {
		return errs.ErrNotFound
	}
	return err
}

func (r idemRepo) Abandon(ctx context.Context, tenantID, key string) error {
	_, err := r.s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE tenant_id=$1 AND key=$2 AND status='IN_PROGRESS'`, tenantID, key)
	return err
}

func (r idemRepo) DeleteExpired(ctx context.Context, now time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at <= $1`, now)
	return tag.RowsAffected(), err
}

// ---------------- dedup ----------------

type dedupRepo struct{ s *Store }

func (r dedupRepo) Begin(ctx context.Context, key, instanceID string, ttl, inflight time.Duration) (ports.DedupOutcome, error) {
	now := time.Now()
	var one int
	err := r.s.pool.QueryRow(ctx, `INSERT INTO event_deduplication(key,instance_id,status,claimed_at,expires_at)
		VALUES($1,$2,'IN_FLIGHT',$3,$4)
		ON CONFLICT (key) DO UPDATE SET status='IN_FLIGHT', claimed_at=EXCLUDED.claimed_at, expires_at=EXCLUDED.expires_at, instance_id=EXCLUDED.instance_id
		WHERE (event_deduplication.status='IN_FLIGHT' AND event_deduplication.claimed_at < $3 - $5::interval)
		   OR event_deduplication.expires_at <= $3
		RETURNING 1`, key, instanceID, now, now.Add(ttl), fmt.Sprintf("%d microseconds", inflight.Microseconds())).Scan(&one)
	switch {
	case err == nil:
		return ports.DedupProceed, nil
	case errors.Is(err, pgx.ErrNoRows):
		return ports.DedupDuplicate, nil
	}
	return ports.DedupDuplicate, err
}

func (r dedupRepo) Commit(ctx context.Context, key string) error {
	_, err := r.s.pool.Exec(ctx, `UPDATE event_deduplication SET status='PUBLISHED' WHERE key=$1`, key)
	return err
}

func (r dedupRepo) Abort(ctx context.Context, key string) error {
	_, err := r.s.pool.Exec(ctx, `DELETE FROM event_deduplication WHERE key=$1 AND status='IN_FLIGHT'`, key)
	return err
}

func (r dedupRepo) DeleteExpired(ctx context.Context, now time.Time) (int64, error) {
	tag, err := r.s.pool.Exec(ctx, `DELETE FROM event_deduplication WHERE expires_at <= $1`, now)
	return tag.RowsAffected(), err
}
