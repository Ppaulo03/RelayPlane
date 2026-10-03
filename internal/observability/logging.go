package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type ctxKey struct{}

// Log field names used across the platform.
const (
	KeyTraceID    = "trace_id"
	KeyTenantID   = "tenant_id"
	KeyInstanceID = "instance_id"
	KeyMessageID  = "message_id"
	KeyNodeID     = "node_id"
	KeyProvider   = "provider"
	KeyEpoch      = "assignment_epoch"
	KeyOperation  = "operation_id"
)

// With returns a context carrying extra log attributes; every log call that
// receives this context (via the *Context slog methods) includes them.
func With(ctx context.Context, args ...any) context.Context {
	prev, _ := ctx.Value(ctxKey{}).([]slog.Attr)
	attrs := append([]slog.Attr(nil), prev...)
	for i := 0; i+1 < len(args); i += 2 {
		k, _ := args[i].(string)
		attrs = append(attrs, slog.Any(k, args[i+1]))
	}
	return context.WithValue(ctx, ctxKey{}, attrs)
}

type ctxHandler struct{ slog.Handler }

func (h ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := ctx.Value(ctxKey{}).([]slog.Attr); ok {
		r.AddAttrs(attrs...)
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String(KeyTraceID, sc.TraceID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h ctxHandler) WithAttrs(a []slog.Attr) slog.Handler { return ctxHandler{h.Handler.WithAttrs(a)} }
func (h ctxHandler) WithGroup(n string) slog.Handler      { return ctxHandler{h.Handler.WithGroup(n)} }

var secretFragments = []string{"secret", "token", "password", "apikey", "api_key", "authorization", "qrcode", "pairing_code", "credential"}

// Redact is a slog ReplaceAttr that masks secrets and message bodies.
func Redact(_ []string, a slog.Attr) slog.Attr {
	k := strings.ToLower(a.Key)
	for _, f := range secretFragments {
		if strings.Contains(k, f) {
			return slog.String(a.Key, "[REDACTED]")
		}
	}
	if k == "text" || k == "body" || k == "caption" {
		return slog.String(a.Key, "[OMITTED]")
	}
	return a
}

// NewLogger builds the JSON logger used by every binary.
func NewLogger(w io.Writer, level slog.Level, service string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: Redact})
	return slog.New(ctxHandler{h}).With("service", service)
}
