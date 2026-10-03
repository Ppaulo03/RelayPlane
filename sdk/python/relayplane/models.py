"""Typed views of API responses."""
from __future__ import annotations

from dataclasses import dataclass
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

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Message":
        return cls(id=d["id"], status=d.get("status", ""), to=d.get("to", ""), type=d.get("type", ""),
                   attempts=int(d.get("attempts", 0)), error_code=d.get("error_code", ""))


@dataclass(frozen=True)
class SentMessage:
    message_id: str
    status: str
    replayed: bool = False


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
