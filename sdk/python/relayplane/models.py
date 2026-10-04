"""Typed views of API responses."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Instance:
    id: str
    name: str
    status: str
    desired_state: str
    observed_state: str
    created_at: str = ""
    last_status_change: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Instance":
        return cls(id=d["id"], name=d.get("name", ""), status=d.get("status", ""), desired_state=d.get("desired_state", ""),
                   observed_state=d.get("observed_state", ""), created_at=d.get("created_at", ""),
                   last_status_change=d.get("last_status_change", ""))


@dataclass(frozen=True)
class Subscription:
    id: str
    url: str
    event_types: tuple
    instance_ids: tuple
    active: bool
    exclude_groups: bool = False
    replayed: bool = False
    created_at: str = ""
    # only set on creation / rotation: the signing secret is shown once
    secret: str | None = None
    previous_secret_valid_until: str | None = None
    paused: bool = False  # deliveries accumulate and none is sent until resume()
    pending: int = 0  # deliveries waiting to be sent to this subscription
    oldest_pending_seconds: int = 0

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Subscription":
        return cls(id=d["id"], url=d["url"], event_types=tuple(d.get("event_types") or ()), instance_ids=tuple(d.get("instance_ids") or ()),
                   active=bool(d.get("active", True)), exclude_groups=bool(d.get("exclude_groups", False)), created_at=d.get("created_at", ""), secret=d.get("secret"),
                   previous_secret_valid_until=d.get("previous_secret_valid_until"), paused=bool(d.get("paused", False)),
                   pending=int((d.get("backlog") or {}).get("pending", 0)), oldest_pending_seconds=int((d.get("backlog") or {}).get("oldest_pending_seconds", 0)))

    def __repr__(self) -> str:  # the secret must never reach a log line
        shown = "<redacted>" if self.secret else None
        return f"Subscription(id={self.id!r}, url={self.url!r}, event_types={self.event_types!r}, secret={shown})"


@dataclass(frozen=True)
class Limits:
    """Effective limits of the deployment (GET /api/v1/limits)."""
    idempotency_retention_seconds: int
    max_text_length: int
    media_max_bytes: int
    media_allowed_types: tuple
    max_subscriptions_per_tenant: int
    webhook_retry_max_attempts: int
    webhook_retry_horizon_seconds: int
    raw: dict

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Limits":
        media, subs = d.get("media") or {}, d.get("subscriptions") or {}
        return cls(idempotency_retention_seconds=int(d.get("idempotency_retention_seconds", 0)), max_text_length=int(d.get("max_text_length", 0)),
                   media_max_bytes=int(media.get("max_bytes", 0)), media_allowed_types=tuple(media.get("allowed_types") or ()),
                   max_subscriptions_per_tenant=int(subs.get("max_per_tenant", 0)), webhook_retry_max_attempts=int(subs.get("retry_max_attempts", 0)),
                   webhook_retry_horizon_seconds=int(subs.get("retry_horizon_seconds", 0)), raw=d)

    def assert_retry_horizon_within_idempotency(self, sender_retry_horizon_seconds: float) -> None:
        """A sender that retries with the same Idempotency-Key beyond the retention window would create a SECOND message."""
        if sender_retry_horizon_seconds > self.idempotency_retention_seconds:
            raise ValueError(f"sender retry horizon {sender_retry_horizon_seconds}s exceeds the idempotency retention "
                             f"{self.idempotency_retention_seconds}s: a late retry would duplicate the message")


@dataclass(frozen=True)
class WebhookDelivery:
    id: str
    event_id: str
    event_type: str
    instance_id: str
    status: str
    attempts: int
    last_error: str = ""
    sequence: int = 0

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "WebhookDelivery":
        return cls(id=d["id"], event_id=d["event_id"], event_type=d.get("event_type", ""), instance_id=d.get("instance_id", ""),
                   status=d.get("status", ""), attempts=int(d.get("attempts", 0)), last_error=d.get("last_error", ""),
                   sequence=int(d.get("sequence", 0)))


@dataclass(frozen=True)
class CreatedInstance:
    id: str
    status: str
    operation_id: str
    replayed: bool = False


@dataclass(frozen=True)
class Pairing:
    qrcode: str | None
    pairing_code: str | None
    expires_at: str

    def __repr__(self) -> str:  # never print pairing credentials into logs
        return "Pairing(<redacted>)"


@dataclass(frozen=True)
class Operation:
    id: str
    type: str
    status: str
    step: str = ""
    instance_id: str = ""
    error_code: str = ""
    error_message: str = ""

    @property
    def done(self) -> bool:
        return self.status in ("SUCCEEDED", "FAILED")

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Operation":
        return cls(id=d["id"], type=d.get("type", ""), status=d.get("status", ""), step=d.get("step", ""),
                   instance_id=d.get("instance_id", ""), error_code=d.get("error_code", ""),
                   error_message=d.get("error_message", ""))


@dataclass(frozen=True)
class OperationRef:
    operation_id: str
    status: str
    error_code: str = ""
    replayed: bool = False


@dataclass(frozen=True)
class Message:
    id: str
    status: str
    to: str = ""
    type: str = ""
    attempts: int = 0
    error_code: str = ""
    sequence_no: int = 0
    # set once the provider ACCEPTED the message: the id an answer's reply_to_provider_message_id refers to
    provider_message_id: str = ""
    accepted_at: str = ""
    error_message: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Message":
        return cls(id=d["id"], status=d.get("status", ""), to=d.get("to", ""), type=d.get("type", ""),
                   attempts=int(d.get("attempts", 0)), error_code=d.get("error_code", ""),
                   sequence_no=int(d.get("sequence_no", 0)), provider_message_id=d.get("provider_message_id", ""),
                   accepted_at=d.get("accepted_at", ""), error_message=d.get("error_message", ""))


@dataclass(frozen=True)
class SentMessage:
    message_id: str
    status: str
    replayed: bool = False


@dataclass(frozen=True)
class ReplyTo:
    """The message a reply quotes. Quote what the user wrote with ``ReplyTo.to_user_message(provider_message_id, text)`` (the
    id and text come from the ``message.received`` event) or one of YOUR OWN messages with ``ReplyTo.to_own_message(message_id)``.

    The WhatsApp node keeps no history, so the preview text must travel with the request: without it the quote is empty."""

    provider_message_id: str = ""
    text: str = ""
    message_id: str = ""

    @classmethod
    def to_user_message(cls, provider_message_id: str, text: str = "") -> "ReplyTo":
        return cls(provider_message_id=provider_message_id, text=text)

    @classmethod
    def to_own_message(cls, message_id: str) -> "ReplyTo":
        """The message must have been ACCEPTED already (it needs the provider's id to be quoted)."""
        return cls(message_id=message_id)

    def as_dict(self) -> dict[str, Any]:
        if bool(self.provider_message_id) == bool(self.message_id):
            raise ValueError("ReplyTo needs exactly one of provider_message_id or message_id")
        if self.message_id:
            return {"message_id": self.message_id}
        d: dict[str, Any] = {"provider_message_id": self.provider_message_id}
        if self.text:
            d["text"] = self.text
        return d


@dataclass(frozen=True)
class ApiKey:
    """An API key of your tenant. ``secret`` is set only on the object returned by ``create`` (it is never shown again)."""

    id: str
    name: str
    prefix: str
    created_at: str = ""
    expires_at: str = ""
    last_used_at: str = ""
    revoked_at: str = ""
    current: bool = False  # the key that authenticated this request
    secret: str = field(default="", repr=False)

    @property
    def active(self) -> bool:
        return not self.revoked_at

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ApiKey":
        return cls(id=d["id"], name=d.get("name", ""), prefix=d.get("prefix", ""), created_at=d.get("created_at", ""),
                   expires_at=d.get("expires_at", ""), last_used_at=d.get("last_used_at", ""), revoked_at=d.get("revoked_at", ""),
                   current=bool(d.get("current", False)), secret=d.get("api_key", ""))


@dataclass(frozen=True)
class Media:
    id: str
    status: str
    content_type: str = ""
    size: int = 0
    sha256: str = ""
    filename: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Media":
        return cls(id=d["id"], status=d.get("status", ""), content_type=d.get("content_type", ""), size=int(d.get("size", 0)),
                   sha256=d.get("sha256", ""), filename=d.get("filename", ""))
