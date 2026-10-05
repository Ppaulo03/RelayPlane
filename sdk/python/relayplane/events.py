"""Typed view of a RelayPlane event and an ordering/loss tracker for the consumer side.

    event = Event.from_dict(verify_request(secrets, headers, body))
    for ready in tracker.push(event):      # in order, duplicates dropped
        handle(ready)

The contract is docs/events/events.schema.json (``schema_version`` 1). ``sequence`` numbers the deliveries of one subscription
for one instance 1, 2, 3 without gaps and a redelivery keeps its number: a retry can arrive late, so reorder by it; a number that
never arrives is a delivery you did not receive (it may be in the dead-letter queue, from which it can be requeued).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SUPPORTED_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class MessageMedia:
    """The attachment of a ``message.received``. Only ``status == "READY"`` can be downloaded
    (``await rp.media.download(media.media_id)``); REJECTED and FAILED say why in ``reason``."""

    media_id: str
    status: str  # READY | REJECTED | FAILED
    kind: str  # image | audio | video | document | sticker
    reason: str = ""  # too_large | type_not_allowed | unsupported | expired | download_failed
    mime_type: str = ""
    size: int = 0
    filename: str = ""
    seconds: int = 0

    @property
    def ready(self) -> bool:
        return self.status == "READY"

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MessageMedia":
        return cls(media_id=d["media_id"], status=d["status"], kind=d["kind"], reason=d.get("reason", ""), mime_type=d.get("mime_type", ""),
                   size=int(d.get("size", 0)), filename=d.get("filename", ""), seconds=int(d.get("seconds", 0)))


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    sequence: int
    schema_version: int
    channel: str
    tenant_id: str
    instance_id: str
    timestamp: str
    payload: dict[str, Any]
    traceparent: str = ""

    @property
    def media(self) -> MessageMedia | None:
        """The attachment of a ``message.received`` (None when the message has none)."""
        m = self.payload.get("media") if self.event_type == "message.received" else None
        return MessageMedia.from_dict(m) if m else None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Event":
        version = int(d.get("schema_version", 0))
        if version != SUPPORTED_SCHEMA_VERSION:
            # an incompatible schema must not be interpreted with the old rules: fail loudly instead of guessing
            raise ValueError(f"unsupported event schema_version {version} (this SDK understands {SUPPORTED_SCHEMA_VERSION})")
        return cls(event_id=d["event_id"], event_type=d["event_type"], sequence=int(d["sequence"]), schema_version=version,
                   channel=d.get("channel", ""), tenant_id=d["tenant_id"], instance_id=d["instance_id"], timestamp=d["timestamp"],
                   payload=d.get("payload") or {}, traceparent=d.get("traceparent", ""))


@dataclass
class _Lane:
    next: int | None = None  # the sequence expected next; None until the first event defines it
    held: dict[int, Event] = field(default_factory=dict)


class SequenceTracker:
    """Releases the events of each instance in ``sequence`` order and reports what is missing.

    Not thread-safe: use one per consumer loop. The first event seen for an instance defines where it starts, so persist
    ``state()`` and restore it with ``SequenceTracker(state)`` after a restart, otherwise the tracker will treat the first
    event it sees as the beginning.
    """

    def __init__(self, state: dict[str, int] | None = None) -> None:
        self._lanes: dict[str, _Lane] = {i: _Lane(next=n) for i, n in (state or {}).items()}

    def push(self, ev: Event) -> list[Event]:
        """Accept one event; return the events that can now be processed, in order. A duplicate returns ``[]``."""
        lane = self._lanes.setdefault(ev.instance_id, _Lane())
        if lane.next is None:
            lane.next = ev.sequence
        if ev.sequence < lane.next or ev.sequence in lane.held:
            return []  # already processed or already waiting: a redelivery
        lane.held[ev.sequence] = ev
        return self._release(lane)

    def missing(self, instance_id: str) -> list[int]:
        """Sequence numbers below the highest one received that have not arrived (candidates for a late retry or the DLQ)."""
        lane = self._lanes.get(instance_id)
        if not lane or not lane.held or lane.next is None:
            return []
        return [n for n in range(lane.next, max(lane.held)) if n not in lane.held]

    def skip_gap(self, instance_id: str) -> list[Event]:
        """Give up on the numbers that are still missing and release what is held (after your own timeout)."""
        lane = self._lanes.get(instance_id)
        if not lane or not lane.held:
            return []
        lane.next = min(lane.held)
        return self._release(lane)

    def state(self) -> dict[str, int]:
        """The next expected sequence per instance (held events are not part of it: they will be redelivered)."""
        return {i: l.next for i, l in self._lanes.items() if l.next is not None}

    @staticmethod
    def _release(lane: _Lane) -> list[Event]:
        out: list[Event] = []
        while lane.next in lane.held:
            out.append(lane.held.pop(lane.next))
            lane.next += 1
        return out
