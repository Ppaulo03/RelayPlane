"""Canonical SDK errors. They mirror the API's error codes, nothing more."""
from __future__ import annotations


class RelayPlaneError(Exception):
    """Base class. `code` is the API's machine-readable error code."""

    def __init__(self, message: str, *, code: str = "unknown", status: int = 0, retry_after: float | None = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.status = status
        self.retry_after = retry_after

    def __str__(self) -> str:
        return f"{self.code} ({self.status}): {self.message}"


class AuthenticationError(RelayPlaneError): ...
class PermissionDenied(RelayPlaneError): ...
class NotFound(RelayPlaneError): ...
class InvalidRequest(RelayPlaneError): ...
class Conflict(RelayPlaneError): ...


class IdempotencyConflict(RelayPlaneError):
    """The Idempotency-Key was already used with a different request payload."""


class RequestInProgress(Conflict):
    """The same Idempotency-Key is still being processed; retry shortly."""


class CapabilityNotSupported(RelayPlaneError): ...


class PayloadTooLarge(RelayPlaneError):
    """Content exceeds the inline limit; upload it with `media.upload` and send by reference."""


class ServiceUnavailable(RelayPlaneError): ...
class ServerError(RelayPlaneError): ...


_BY_CODE: dict[str, type[RelayPlaneError]] = {
    "unauthenticated": AuthenticationError,
    "forbidden": PermissionDenied,
    "not_found": NotFound,
    "invalid_argument": InvalidRequest,
    "idempotency_key_reuse": IdempotencyConflict,
    "request_in_progress": RequestInProgress,
    "capability_not_supported": CapabilityNotSupported,
    "payload_too_large": PayloadTooLarge,
    "no_capacity": ServiceUnavailable,
    "provider_unavailable": ServiceUnavailable,
}


def from_response(status: int, body: object, retry_after: float | None) -> RelayPlaneError:
    code, message = "unknown", f"HTTP {status}"
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        code = str(body["error"].get("code", code))
        message = str(body["error"].get("message", message))
    cls = _BY_CODE.get(code)
    if cls is None:
        cls = Conflict if status == 409 else ServerError if status >= 500 else RelayPlaneError
    return cls(message, code=code, status=status, retry_after=retry_after)
