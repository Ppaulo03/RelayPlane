// Package errs defines the canonical error vocabulary of RelayPlane.
//
// Adapters translate infrastructure- and provider-specific failures into these
// errors; the core, workers and the HTTP API only ever reason about them.
package errs

import (
	"errors"
	"fmt"
)

// Domain errors.
var (
	ErrNotFound            = errors.New("not found")
	ErrAlreadyExists       = errors.New("already exists")
	ErrConflict            = errors.New("conflict")
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrUnauthenticated     = errors.New("unauthenticated")
	ErrForbidden           = errors.New("forbidden")
	ErrNoCapacity          = errors.New("no placement capacity")
	ErrStaleCommand        = errors.New("STALE_COMMAND")
	ErrStaleAssignment     = errors.New("stale assignment")
	ErrOwnershipViolation  = errors.New("OWNERSHIP_VIOLATION")
	ErrFencingRequired     = errors.New("old owner not fenced")
	ErrMigrationBlocked    = errors.New("MIGRATION_BLOCKED")
	ErrInvalidTransition   = errors.New("invalid state transition")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	ErrInProgress          = errors.New("operation in progress")
	ErrPayloadTooLarge     = errors.New("payload exceeds inline limit")
	ErrCapabilityMissing   = errors.New("capability not supported")
	ErrAlreadyTerminal     = errors.New("operation already finished")
	// ErrDestinationBlocked: an outbound webhook destination is not allowed (private/loopback/metadata address,
	// forbidden scheme). It is permanent: retrying cannot help.
	ErrDestinationBlocked = errors.New("destination not allowed")
)

// Provider errors (canonical translations of provider-specific failures).
var (
	ErrProviderUnavailable   = errors.New("provider unavailable")
	ErrInstanceNotFound      = errors.New("provider instance not found")
	ErrInstanceAlreadyExists = errors.New("provider instance already exists")
	ErrAuthenticationFailed  = errors.New("provider authentication failed")
	ErrPairingUnavailable    = errors.New("pairing unavailable")
	ErrAmbiguousDispatch     = errors.New("ambiguous dispatch")
	ErrInvalidRecipient      = errors.New("invalid recipient")
	ErrProviderRejected      = errors.New("provider rejected request")
)

// Class tells the retry machinery what to do with a failure.
type Class string

const (
	Retryable    Class = "RETRYABLE"
	NonRetryable Class = "NON_RETRYABLE"
	Ambiguous    Class = "AMBIGUOUS"
)

// Classify maps a (canonical) error to a retry class for outbound dispatch.
//
//   - RETRYABLE: the provider provably did not act on the request.
//   - AMBIGUOUS: the provider may have acted (timeout after the request was
//     written, 500 mid-flight). Never retried automatically.
//   - NON_RETRYABLE: retrying cannot succeed.
func Classify(err error) Class {
	switch {
	case err == nil:
		return NonRetryable
	case errors.Is(err, ErrAmbiguousDispatch):
		return Ambiguous
	case errors.Is(err, ErrProviderUnavailable):
		return Retryable
	default:
		return NonRetryable
	}
}

// Wrap annotates err keeping errors.Is semantics.
func Wrap(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}
