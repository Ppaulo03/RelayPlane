// Package worker implements the outbound command handler and the event
// projector. Workers scale horizontally: ordering per instance comes from the
// CommandQueue contract, safety from compare-and-set message states and the
// logical fencing check before every dispatch.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/relayplane/relayplane/internal/app"
	"github.com/relayplane/relayplane/internal/core/errs"
	"github.com/relayplane/relayplane/internal/core/instance"
	"github.com/relayplane/relayplane/internal/core/media"
	"github.com/relayplane/relayplane/internal/core/messaging"
	"github.com/relayplane/relayplane/internal/core/ownership"
	"github.com/relayplane/relayplane/internal/observability"
	"github.com/relayplane/relayplane/internal/ports"
	"github.com/relayplane/relayplane/internal/ratelimit"
)

// Outbound handles outbound message commands.
type Outbound struct {
	Repos     ports.Repositories
	Providers *app.ProviderRegistry
	Blob      ports.BlobStore
	Limiter   *ratelimit.Limiter
	Metrics   *observability.Metrics
	Log       *slog.Logger
	Now       func() time.Time

	GlobalPolicy  messaging.RatePolicy
	Retry         messaging.RetrySchedule
	MediaPolicy   media.Policy
	SignedURLTTL  time.Duration
	VerifyBlobSum bool // stream-verify the checksum before sending media
}

// NewOutbound builds a handler with the documented defaults.
func NewOutbound(repos ports.Repositories, providers *app.ProviderRegistry, blob ports.BlobStore, m *observability.Metrics, log *slog.Logger) *Outbound {
	return &Outbound{
		Repos: repos, Providers: providers, Blob: blob, Limiter: ratelimit.New(), Metrics: m, Log: log, Now: time.Now,
		GlobalPolicy: messaging.RatePolicy{MinInterval: 1500 * time.Millisecond, Burst: 1, MaxPerMinute: 30, MaxConcurrent: 1, Cooldown: time.Minute},
		Retry:        messaging.DefaultRetrySchedule, MediaPolicy: media.DefaultPolicy(),
		SignedURLTTL: 15 * time.Minute, VerifyBlobSum: true,
	}
}

func (w *Outbound) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func ack() ports.Result { return ports.Result{Disposition: ports.Ack} }

// Handle is the ports.CommandHandler for outbound commands.
func (w *Outbound) Handle(ctx context.Context, cmd ports.Command) (ports.Result, error) {
	var env messaging.Envelope
	raw, ok := cmd.Payload.(json.RawMessage)
	if !ok {
		raw, _ = json.Marshal(cmd.Payload)
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		w.Log.ErrorContext(ctx, "undecodable command; dead-lettering", "command_id", cmd.ID, "error", err)
		return ports.Result{Disposition: ports.DeadLetter, Reason: "undecodable envelope: " + err.Error()}, nil
	}
	ctx = observability.WithTraceParent(ctx, firstNonEmpty(cmd.TraceParent, env.TraceParent))
	ctx, span := observability.Start(ctx, "worker.outbound")
	defer span.End()
	ctx = observability.With(ctx,
		observability.KeyMessageID, env.MessageID, observability.KeyTenantID, env.TenantID,
		observability.KeyInstanceID, env.InstanceID, observability.KeyNodeID, env.Assignment.NodeID,
		observability.KeyEpoch, env.Assignment.Epoch)

	res, err := w.handle(ctx, cmd, env)
	if err != nil {
		observability.Fail(span, err)
	}
	return res, err
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (w *Outbound) handle(ctx context.Context, cmd ports.Command, env messaging.Envelope) (ports.Result, error) {
	msg, err := w.Repos.Messages.Get(ctx, env.MessageID)
	if errors.Is(err, errs.ErrNotFound) {
		return ports.Result{Disposition: ports.DeadLetter, Reason: "unknown message id"}, nil
	}
	if err != nil {
		return ports.Result{}, err
	}

	switch msg.Status {
	case messaging.StatusQueued:
	case messaging.StatusDispatching:
		// A previous attempt crashed after claiming the message: we cannot know
		// whether the provider received it. Never resend blindly.
		if _, err := w.Repos.Messages.Transition(ctx, msg.ID, []messaging.Status{messaging.StatusDispatching}, messaging.StatusUnknown,
			ports.MessagePatch{ErrorCode: "WORKER_CRASH", ErrorMessage: "dispatch interrupted; outcome unknown"}); err != nil && !errors.Is(err, errs.ErrConflict) {
			return ports.Result{}, err
		}
		w.Metrics.OutboundMessages.WithLabelValues(string(messaging.StatusUnknown)).Inc()
		w.Log.WarnContext(ctx, "redelivery of an interrupted dispatch: marked UNKNOWN, not resent")
		return ack(), nil
	default:
		return ack(), nil // already processed (duplicate delivery)
	}

	inst, err := w.Repos.Instances.Get(ctx, env.InstanceID)
	if errors.Is(err, errs.ErrNotFound) {
		return w.fail(ctx, msg, "INSTANCE_NOT_FOUND", err), nil
	}
	if err != nil {
		return ports.Result{}, err
	}

	// Logical fencing: a command accepted under an older assignment must never
	// reach the provider.
	if err := ownership.ValidateDispatch(env.Assignment, inst.Assignment()); err != nil {
		w.Metrics.StaleCommandTotal.Inc()
		w.Metrics.EpochMismatchTotal.Inc()
		w.Log.WarnContext(ctx, "STALE_COMMAND rejected", "current_epoch", inst.AssignmentEpoch, "current_node", inst.NodeID, "error", err)
		return w.fail(ctx, msg, "STALE_COMMAND", err), nil
	}
	if inst.DeletedAt != nil || inst.DesiredState == instance.DesiredDeleted {
		return w.fail(ctx, msg, "INSTANCE_DELETED", errs.ErrConflict), nil
	}
	if inst.ObservedState != instance.Connected {
		// node healthy, session not: retry with backoff (INV-10)
		return w.retryQueued(ctx, cmd, msg, fmt.Errorf("%w: instance is %s", errs.ErrProviderUnavailable, inst.ObservedState)), nil
	}

	// Rate limiting (hierarchy global < tenant < instance). Never sleeps: a
	// positive wait defers only this key, other instances keep flowing.
	var tenantPolicy *messaging.RatePolicy
	if t, err := w.Repos.Tenants.Get(ctx, inst.TenantID); err == nil {
		tenantPolicy = t.RatePolicy
	}
	policy := messaging.ResolvePolicy(w.GlobalPolicy, tenantPolicy, inst.RatePolicy)
	wait, release := w.Limiter.Reserve(inst.ID, policy)
	if wait > 0 {
		w.Metrics.RateLimitWait.Observe(wait.Seconds())
		return ports.Result{Disposition: ports.Defer, After: wait, Reason: "rate limited"}, nil
	}
	defer release()

	// Claim the dispatch (QUEUED -> DISPATCHING). Losing the race means another
	// worker owns this message.
	msg, err = w.Repos.Messages.Transition(ctx, msg.ID, []messaging.Status{messaging.StatusQueued}, messaging.StatusDispatching, ports.MessagePatch{BumpAttempt: true})
	if errors.Is(err, errs.ErrConflict) {
		return ack(), nil
	}
	if err != nil {
		return ports.Result{}, err
	}

	out := messaging.OutboundMessage{ID: msg.ID, To: env.To, Type: env.Type, Text: env.Payload.Text,
		Caption: env.Payload.Caption, Filename: env.Payload.Filename}
	if env.Payload.Media != nil {
		att, cls, err := w.resolveMedia(ctx, env)
		if err != nil {
			if cls == errs.Retryable {
				return w.retryDispatching(ctx, cmd, msg, err), nil
			}
			return w.fail(ctx, msg, mediaCode(err), err), nil
		}
		out.Media = att
	}

	provider, err := w.Providers.Get(inst.Provider)
	if err != nil {
		return w.fail(ctx, msg, "PROVIDER_UNKNOWN", err), nil
	}
	start := time.Now()
	pctx, pspan := observability.Start(ctx, "provider.send")
	sent, err := provider.SendMessage(pctx, env.Assignment, out)
	observability.Fail(pspan, err)
	pspan.End()
	outcome := "ok"
	if err != nil {
		outcome = string(errs.Classify(err))
	}
	w.Metrics.ProviderLatency.WithLabelValues("send", outcome).Observe(time.Since(start).Seconds())

	if err == nil {
		if _, terr := w.Repos.Messages.Transition(ctx, msg.ID, []messaging.Status{messaging.StatusDispatching}, messaging.StatusAccepted,
			ports.MessagePatch{ProviderMessageID: sent.ProviderMessageID}); terr != nil {
			// Sent but not recorded: returning the error redelivers the command,
			// which then sees DISPATCHING and records UNKNOWN instead of resending.
			return ports.Result{}, fmt.Errorf("record accepted message: %w", terr)
		}
		w.Metrics.OutboundMessages.WithLabelValues(string(messaging.StatusAccepted)).Inc()
		w.Log.InfoContext(ctx, "message accepted by provider", "provider_message_id", sent.ProviderMessageID)
		return ack(), nil
	}

	switch errs.Classify(err) {
	case errs.Ambiguous:
		_, _ = w.Repos.Messages.Transition(ctx, msg.ID, []messaging.Status{messaging.StatusDispatching}, messaging.StatusUnknown,
			ports.MessagePatch{ErrorCode: "AMBIGUOUS_DISPATCH", ErrorMessage: err.Error()})
		w.Metrics.OutboundMessages.WithLabelValues(string(messaging.StatusUnknown)).Inc()
		w.Log.WarnContext(ctx, "dispatch outcome ambiguous; not retrying automatically", "error", err)
		return ack(), nil
	case errs.Retryable:
		return w.retryDispatching(ctx, cmd, msg, err), nil
	}
	if errors.Is(err, errs.ErrAuthenticationFailed) {
		w.Limiter.Penalize(inst.ID, policy)
	}
	return w.fail(ctx, msg, failureCode(err), err), nil
}

// retryQueued schedules a retry for a message that is still QUEUED.
func (w *Outbound) retryQueued(ctx context.Context, cmd ports.Command, msg *messaging.Message, cause error) ports.Result {
	delay, ok := w.Retry.Next(cmd.Attempt)
	if !ok {
		w.Metrics.OutboundDLQTotal.Inc()
		w.failFrom(ctx, msg, []messaging.Status{messaging.StatusQueued}, "RETRIES_EXHAUSTED", cause)
		return ports.Result{Disposition: ports.DeadLetter, Reason: "retries exhausted: " + cause.Error()}
	}
	w.Metrics.OutboundRetryTotal.WithLabelValues(string(errs.Retryable)).Inc()
	w.Log.WarnContext(ctx, "retry scheduled", "attempt", cmd.Attempt, "delay", delay.String(), "reason", cause.Error())
	return ports.Result{Disposition: ports.Retry, After: delay, MaxAttempts: w.Retry.MaxAttempts(), Reason: cause.Error()}
}

// retryDispatching releases the claim (DISPATCHING -> QUEUED) and schedules a retry.
func (w *Outbound) retryDispatching(ctx context.Context, cmd ports.Command, msg *messaging.Message, cause error) ports.Result {
	delay, ok := w.Retry.Next(cmd.Attempt)
	if !ok {
		w.Metrics.OutboundDLQTotal.Inc()
		w.failFrom(ctx, msg, []messaging.Status{messaging.StatusDispatching}, "RETRIES_EXHAUSTED", cause)
		return ports.Result{Disposition: ports.DeadLetter, Reason: "retries exhausted: " + cause.Error()}
	}
	if _, err := w.Repos.Messages.Transition(ctx, msg.ID, []messaging.Status{messaging.StatusDispatching}, messaging.StatusQueued,
		ports.MessagePatch{ErrorCode: "RETRYING", ErrorMessage: cause.Error()}); err != nil {
		w.Log.ErrorContext(ctx, "could not release dispatch claim", "error", err)
	}
	w.Metrics.OutboundRetryTotal.WithLabelValues(string(errs.Retryable)).Inc()
	w.Log.WarnContext(ctx, "retry scheduled", "attempt", cmd.Attempt, "delay", delay.String(), "reason", cause.Error())
	return ports.Result{Disposition: ports.Retry, After: delay, MaxAttempts: w.Retry.MaxAttempts(), Reason: cause.Error()}
}

func (w *Outbound) fail(ctx context.Context, msg *messaging.Message, code string, cause error) ports.Result {
	w.failFrom(ctx, msg, []messaging.Status{messaging.StatusQueued, messaging.StatusDispatching}, code, cause)
	return ack()
}

func (w *Outbound) failFrom(ctx context.Context, msg *messaging.Message, from []messaging.Status, code string, cause error) {
	if _, err := w.Repos.Messages.Transition(ctx, msg.ID, from, messaging.StatusFailed,
		ports.MessagePatch{ErrorCode: code, ErrorMessage: cause.Error()}); err != nil {
		w.Log.ErrorContext(ctx, "could not mark message failed", "error", err)
		return
	}
	w.Metrics.OutboundMessages.WithLabelValues(string(messaging.StatusFailed)).Inc()
	w.Log.WarnContext(ctx, "message failed", "code", code, "error", cause.Error())
}

func failureCode(err error) string {
	switch {
	case errors.Is(err, errs.ErrInvalidRecipient):
		return "INVALID_RECIPIENT"
	case errors.Is(err, errs.ErrAuthenticationFailed):
		return "PROVIDER_AUTH_FAILED"
	case errors.Is(err, errs.ErrStaleAssignment), errors.Is(err, errs.ErrStaleCommand):
		return "STALE_COMMAND"
	case errors.Is(err, errs.ErrInstanceNotFound):
		return "INSTANCE_NOT_FOUND"
	case errors.Is(err, errs.ErrCapabilityMissing):
		return "CAPABILITY_NOT_SUPPORTED"
	}
	return "PROVIDER_REJECTED"
}

var errMediaUnavailable = errors.New("media unavailable")

func mediaCode(err error) string {
	if errors.Is(err, errMediaUnavailable) {
		return "MEDIA_UNAVAILABLE"
	}
	return "MEDIA_INVALID"
}

// resolveMedia validates the claim check and opens the object: tenant
// ownership, size, MIME, expiry and (optionally) checksum are all verified
// before the provider sees anything.
func (w *Outbound) resolveMedia(ctx context.Context, env messaging.Envelope) (*messaging.Attachment, errs.Class, error) {
	ctx, span := observability.Start(ctx, "worker.resolve_media")
	defer span.End()
	ref := *env.Payload.Media
	if err := media.ValidateRef(env.TenantID, ref, w.MediaPolicy, w.now()); err != nil {
		return nil, errs.NonRetryable, err
	}
	info, err := w.Blob.Stat(ctx, ref.ObjectKey)
	if errors.Is(err, errs.ErrNotFound) {
		return nil, errs.NonRetryable, fmt.Errorf("%w: %s was removed before dispatch", errMediaUnavailable, ref.ObjectKey)
	}
	if err != nil {
		return nil, errs.Retryable, fmt.Errorf("blob store: %w", err)
	}
	if info.Size != ref.Size {
		return nil, errs.NonRetryable, fmt.Errorf("%w: stored size %d != claimed %d", errMediaUnavailable, info.Size, ref.Size)
	}
	if w.VerifyBlobSum {
		r, err := w.Blob.Get(ctx, ref.ObjectKey)
		if errors.Is(err, errs.ErrNotFound) {
			return nil, errs.NonRetryable, fmt.Errorf("%w: %s was removed before dispatch", errMediaUnavailable, ref.ObjectKey)
		}
		if err != nil {
			return nil, errs.Retryable, fmt.Errorf("blob store: %w", err)
		}
		h := sha256.New()
		n, err := io.Copy(h, r)
		_ = r.Close()
		if err != nil {
			return nil, errs.Retryable, fmt.Errorf("blob read: %w", err)
		}
		w.Metrics.BlobBytes.WithLabelValues("get").Add(float64(n))
		if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
			return nil, errs.NonRetryable, errors.New("media checksum mismatch")
		}
	}
	url, err := w.Blob.SignedURL(ctx, ref.ObjectKey, ports.SignedGet, w.SignedURLTTL)
	if err != nil {
		return nil, errs.Retryable, err
	}
	key := ref.ObjectKey
	return &messaging.Attachment{ContentType: ref.ContentType, Size: ref.Size, SHA256: ref.SHA256, URL: url,
		Open: func(ctx context.Context) (io.ReadCloser, error) {
			w.Metrics.BlobBytes.WithLabelValues("get").Add(float64(ref.Size))
			return w.Blob.Get(ctx, key)
		}}, errs.NonRetryable, nil
}
