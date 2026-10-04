package http

import (
	"errors"
	"log/slog"
	nethttp "net/http"

	"github.com/relayplane/relayplane/internal/core/errs"
)

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type errorBody struct {
	Error apiError `json:"error"`
}

// errRateLimited is what the limiter reports (the message tells the client what to do).
var errRateLimited = errs.Wrap(errs.ErrRateLimited, "too many requests: slow down and retry after the Retry-After delay")

var errorMap = []struct {
	err    error
	status int
	code   string
}{
	{errs.ErrUnauthenticated, 401, "unauthenticated"},
	{errs.ErrForbidden, 403, "forbidden"},
	{errs.ErrNotFound, 404, "not_found"},
	{errs.ErrInvalidArgument, 400, "invalid_argument"},
	{errs.ErrIdempotencyConflict, 422, "idempotency_key_reuse"},
	{errs.ErrInProgress, 409, "request_in_progress"},
	{errs.ErrConflict, 409, "conflict"},
	{errs.ErrInvalidTransition, 409, "invalid_state"},
	{errs.ErrAlreadyExists, 409, "already_exists"},
	{errs.ErrStaleCommand, 409, "stale_command"},
	{errs.ErrStaleAssignment, 409, "stale_assignment"},
	{errs.ErrOwnershipViolation, 409, "ownership_violation"},
	{errs.ErrFencingRequired, 409, "fencing_required"},
	{errs.ErrMigrationBlocked, 409, "migration_blocked"},
	{errs.ErrPairingUnavailable, 409, "pairing_unavailable"},
	{errs.ErrPayloadTooLarge, 413, "payload_too_large"},
	{errs.ErrRateLimited, 429, "rate_limited"},
	{errs.ErrCapabilityMissing, 501, "capability_not_supported"},
	{errs.ErrNoCapacity, 503, "no_capacity"},
	{errs.ErrProviderUnavailable, 503, "provider_unavailable"},
	{errs.ErrAmbiguousDispatch, 502, "provider_error"},
	{errs.ErrInstanceNotFound, 404, "not_found"},
}

// writeError renders a canonical error. Unknown errors are logged and returned
// as an opaque 500: internal details never reach the client.
func writeError(w nethttp.ResponseWriter, r *nethttp.Request, log *slog.Logger, err error) {
	for _, m := range errorMap {
		if errors.Is(err, m.err) {
			msg := err.Error()
			if m.status == 404 {
				msg = "resource not found"
			}
			if m.status == 401 {
				msg = "invalid or missing credentials"
			}
			if m.status == 403 {
				msg = "operation not permitted for this credential"
			}
			if m.code == "provider_unavailable" || m.code == "provider_error" {
				msg = "the messaging backend is temporarily unavailable" // never relay provider-specific text
				w.Header().Set("Retry-After", "5")
			}
			writeJSON(w, m.status, errorBody{apiError{Code: m.code, Message: msg}})
			return
		}
	}
	log.ErrorContext(r.Context(), "unhandled error", "error", err)
	writeJSON(w, 500, errorBody{apiError{Code: "internal", Message: "internal error"}})
}
