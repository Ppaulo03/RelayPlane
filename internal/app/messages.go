package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
type MessageService struct {
	d      Deps
	outbox *OutboxService
}

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
		Assignment: inst.Assignment(), PartitionKey: inst.ID, Sequence: 1, // real sequence is allocated by the repository
		Type: in.Type, To: in.To, Payload: payload,
		TraceID: observability.TraceID(ctx), TraceParent: observability.TraceParent(ctx), CreatedAt: s.d.now(),
	}
	if err := env.Validate(); err != nil {
		return SendResult{}, err
	}
	// build serialises the command once the per-instance sequence is known; it runs
	// inside the transaction that stores the message and its outbox entry.
	build := func(seq int64) ([]byte, error) {
		env.Sequence = seq
		raw, err := json.Marshal(env)
		if err != nil {
			return nil, err
		}
		// INV-11: nothing bigger than the inline limit may reach the broker.
		if err := media.EnforceInlineLimit(raw, s.d.Cfg.MediaPolicy.InlineMaxBytes); err != nil {
			return nil, err
		}
		return raw, nil
	}

	pj, _ := json.Marshal(payload)
	seq, err := s.d.Repos.Messages.CreateWithOutbox(ctx, messaging.Message{
		ID: msgID, TenantID: tenantID, InstanceID: inst.ID, IdempotencyKey: idemKey, NodeID: inst.NodeID,
		AssignmentEpoch: inst.AssignmentEpoch, PartitionKey: inst.ID, Recipient: in.To, Type: in.Type,
		Payload: pj, Status: messaging.StatusQueued,
	}, build)
	if err != nil && !errors.Is(err, errs.ErrAlreadyExists) { // AlreadyExists: resumed after a crash; it is already in the outbox
		return SendResult{}, err
	}
	// The message is durable and will be published: from here on the accept can
	// no longer fail. Publish eagerly for latency; the outbox dispatcher in the
	// reconciler covers every failure (broker down, crash) in sequence order.
	if _, derr := s.outbox.DispatchInstance(ctx, inst.ID); derr != nil {
		s.d.Log.WarnContext(ctx, "eager outbox dispatch failed; the dispatcher will retry", "error", derr)
	}
	s.d.Log.InfoContext(ctx, "message queued", "assignment_epoch", inst.AssignmentEpoch, "sequence_no", seq, "type", in.Type)
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

// ResolveOutcome is an operator/application decision about an UNKNOWN message.
type ResolveOutcome string

const (
	OutcomeSent    ResolveOutcome = "sent"     // the recipient did receive it
	OutcomeNotSent ResolveOutcome = "not_sent" // it was verified as not sent
)

// Resolve settles a message whose dispatch outcome was ambiguous (UNKNOWN). Until
// it is resolved, later messages of the same instance are held back by the
// sequence barrier (or until UNKNOWN_BARRIER_TIMEOUT elapses).
func (s *MessageService) Resolve(ctx context.Context, tenantID, id string, outcome ResolveOutcome) (*messaging.Message, error) {
	m, err := s.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if m.Status != messaging.StatusUnknown {
		return nil, fmt.Errorf("%w: message is %s, only UNKNOWN messages can be resolved", errs.ErrConflict, m.Status)
	}
	switch outcome {
	case OutcomeSent:
		return s.d.Repos.Messages.Transition(ctx, id, []messaging.Status{messaging.StatusUnknown}, messaging.StatusAccepted, ports.MessagePatch{})
	case OutcomeNotSent:
		return s.d.Repos.Messages.Transition(ctx, id, []messaging.Status{messaging.StatusUnknown}, messaging.StatusFailed,
			ports.MessagePatch{ErrorCode: "NOT_SENT_CONFIRMED", ErrorMessage: "confirmed as not sent by the caller"})
	}
	return nil, fmt.Errorf("%w: outcome must be %q or %q", errs.ErrInvalidArgument, OutcomeSent, OutcomeNotSent)
}
