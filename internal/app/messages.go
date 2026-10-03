package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/ids"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/idempotency"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
)

// MessageService accepts outbound messages and enqueues them.
type MessageService struct{ d Deps }

// SendInput is the public send request. Binary content is never inline: media
// messages reference an uploaded blob (claim check) by MediaID.
type SendInput struct {
	InstanceID string         `json:"instance_id"`
	To         string         `json:"to"`
	Type       messaging.Type `json:"type"`
	Payload    SendPayload    `json:"payload"`
}

// SendPayload is the type-specific body.
type SendPayload struct {
	Text     string `json:"text,omitempty"`
	MediaID  string `json:"media_id,omitempty"`
	Caption  string `json:"caption,omitempty"`
	Filename string `json:"filename,omitempty"`
}

// SendResult is the (replayable) accept response.
type SendResult struct {
	MessageID string           `json:"message_id"`
	Status    messaging.Status `json:"status"`
}

func validRecipient(to string) bool {
	if len(to) < 5 || len(to) > 64 {
		return false
	}
	for _, r := range to {
		if !(r >= '0' && r <= '9') && r != '+' && r != '@' && r != '.' && r != '-' && !(r >= 'a' && r <= 'z') {
			return false
		}
	}
	return true
}

// Send validates, persists (QUEUED) and publishes an outbound command that
// carries the assignment snapshot valid at accept time.
func (s *MessageService) Send(ctx context.Context, tenantID string, in SendInput, idemKey string) (SendResult, bool, error) {
	in.To = strings.TrimSpace(in.To)
	switch {
	case in.InstanceID == "":
		return SendResult{}, false, fmt.Errorf("%w: instance_id is required", errs.ErrInvalidArgument)
	case !validRecipient(in.To):
		return SendResult{}, false, fmt.Errorf("%w: invalid recipient", errs.ErrInvalidArgument)
	case !in.Type.Valid():
		return SendResult{}, false, fmt.Errorf("%w: unsupported type %q", errs.ErrInvalidArgument, in.Type)
	case in.Type == messaging.TypeText && (in.Payload.Text == "" || len(in.Payload.Text) > s.d.Cfg.MaxTextLength):
		return SendResult{}, false, fmt.Errorf("%w: text must be 1-%d characters", errs.ErrInvalidArgument, s.d.Cfg.MaxTextLength)
	case in.Type == messaging.TypeText && in.Payload.MediaID != "":
		return SendResult{}, false, fmt.Errorf("%w: text messages cannot carry media", errs.ErrInvalidArgument)
	case in.Type.IsMedia() && in.Payload.MediaID == "":
		return SendResult{}, false, fmt.Errorf("%w: %s messages require payload.media_id", errs.ErrInvalidArgument, in.Type)
	}
	return idempotency.Do(ctx, s.d.Idem, tenantID, idemKey, "send_message", idempotency.HashRequest(in),
		func() string { return ids.New("msg") },
		func(ctx context.Context, msgID string) (SendResult, error) {
			return s.send(ctx, tenantID, msgID, in, idemKey)
		})
}

func (s *MessageService) send(ctx context.Context, tenantID, msgID string, in SendInput, idemKey string) (SendResult, error) {
	ctx, span := observability.Start(ctx, "message.accept")
	defer span.End()
	ctx = observability.With(ctx, observability.KeyMessageID, msgID, observability.KeyTenantID, tenantID, observability.KeyInstanceID, in.InstanceID)

	inst, err := s.d.loadForTenant(ctx, tenantID, in.InstanceID)
	if err != nil {
		return SendResult{}, err
	}
	switch {
	case inst.DesiredState == instance.DesiredDeleted, inst.NodeID == "",
		inst.ObservedState == instance.Deleting, inst.ObservedState == instance.Deleted, inst.ObservedState == instance.Failed,
		inst.ObservedState == instance.Allocating, inst.ObservedState == instance.Creating:
		return SendResult{}, fmt.Errorf("%w: instance cannot accept messages (observed %s)", errs.ErrConflict, inst.ObservedState)
	}

	payload := messaging.Payload{Text: in.Payload.Text, Caption: in.Payload.Caption, Filename: in.Payload.Filename}
	if in.Type.IsMedia() {
		blob, err := s.d.Repos.Blobs.Get(ctx, in.Payload.MediaID)
		if err != nil || blob.TenantID != tenantID { // other tenants' objects are indistinguishable from missing ones
			return SendResult{}, fmt.Errorf("%w: media %q", errs.ErrNotFound, in.Payload.MediaID)
		}
		if blob.Status != media.BlobReady {
			return SendResult{}, fmt.Errorf("%w: media %q is %s, not READY", errs.ErrConflict, blob.ID, blob.Status)
		}
		ref := blob.Ref()
		if err := media.ValidateRef(tenantID, ref, s.d.Cfg.MediaPolicy, s.d.now()); err != nil {
			return SendResult{}, err
		}
		payload.Media = &ref
		if payload.Filename == "" {
			payload.Filename = blob.Filename
		}
	}

	env := messaging.Envelope{
		MessageID: msgID, IdempotencyKey: idemKey, TenantID: tenantID, InstanceID: inst.ID,
		Assignment: inst.Assignment(), PartitionKey: inst.ID, Type: in.Type, To: in.To, Payload: payload,
		TraceID: observability.TraceID(ctx), TraceParent: observability.TraceParent(ctx), CreatedAt: s.d.now(),
	}
	if err := env.Validate(); err != nil {
		return SendResult{}, err
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return SendResult{}, err
	}
	// INV-11: nothing bigger than the inline limit may reach the broker.
	if err := media.EnforceInlineLimit(raw, s.d.Cfg.MediaPolicy.InlineMaxBytes); err != nil {
		return SendResult{}, err
	}

	pj, _ := json.Marshal(payload)
	err = s.d.Repos.Messages.Create(ctx, messaging.Message{
		ID: msgID, TenantID: tenantID, InstanceID: inst.ID, IdempotencyKey: idemKey, NodeID: inst.NodeID,
		AssignmentEpoch: inst.AssignmentEpoch, PartitionKey: inst.ID, Recipient: in.To, Type: in.Type,
		Payload: pj, Status: messaging.StatusQueued,
	})
	if err != nil && !errors.Is(err, errs.ErrAlreadyExists) { // AlreadyExists: resumed after a crash
		return SendResult{}, err
	}
	if err := s.d.Queue.Publish(ctx, ports.Command{ID: msgID, PartitionKey: inst.ID, IdempotencyKey: idemKey, Payload: env, TraceParent: env.TraceParent}); err != nil {
		// The message stays QUEUED; a retry with the same key re-publishes and
		// the outbox sweeper recovers the rest.
		return SendResult{}, fmt.Errorf("enqueue: %w", err)
	}
	s.d.Log.InfoContext(ctx, "message queued", "assignment_epoch", inst.AssignmentEpoch, "type", in.Type)
	return SendResult{MessageID: msgID, Status: messaging.StatusQueued}, nil
}

// Get returns a message owned by tenantID.
func (s *MessageService) Get(ctx context.Context, tenantID, id string) (*messaging.Message, error) {
	m, err := s.d.Repos.Messages.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if m.TenantID != tenantID {
		return nil, errs.ErrNotFound
	}
	return m, nil
}

// RepublishStale re-enqueues messages that were accepted (persisted QUEUED)
// but whose command may never have reached the broker (crash between the
// database write and the publish). Re-publication is safe: the worker's
// compare-and-set on the message status makes duplicate commands no-ops.
// Ordering relative to newer messages is only best-effort in this crash case.
func (s *MessageService) RepublishStale(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	stale, err := s.d.Repos.Messages.ListStaleQueued(ctx, s.d.now().Add(-olderThan), limit)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range stale {
		var p messaging.Payload
		if err := json.Unmarshal(m.Payload, &p); err != nil {
			continue
		}
		env := messaging.Envelope{MessageID: m.ID, IdempotencyKey: m.IdempotencyKey, TenantID: m.TenantID, InstanceID: m.InstanceID,
			Assignment: ownershipOf(m), PartitionKey: m.InstanceID, Type: m.Type, To: m.Recipient, Payload: p, CreatedAt: m.CreatedAt}
		if err := s.d.Queue.Publish(ctx, ports.Command{ID: m.ID, PartitionKey: m.InstanceID, IdempotencyKey: m.IdempotencyKey, Payload: env}); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
