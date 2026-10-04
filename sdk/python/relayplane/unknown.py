"""An example policy for UNKNOWN messages (ambiguous sends that hold back their instance until somebody decides).

The default asks a human: resolving wrongly either duplicates a message (``not_sent`` when it was sent, then resent) or loses it
(``sent`` when it was not). Only messages the caller marked as safe to repeat are resolved as ``not_sent`` automatically.
See docs/runbooks/UNKNOWN-MESSAGES.md.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum

from .models import Message


class Decision(str, Enum):
    ASK_HUMAN = "ask_human"
    RESOLVE_SENT = "sent"
    RESOLVE_NOT_SENT = "not_sent"


@dataclass
class UnknownPolicy:
    """``safe_to_repeat`` says whether sending the message again is harmless (a repeatable reminder); ``decide`` may replace the whole rule."""
    safe_to_repeat: Callable[[Message], bool] = lambda m: False
    decide: Callable[[Message], Decision] | None = None

    def __call__(self, message: Message) -> Decision:
        if self.decide is not None:
            return self.decide(message)
        return Decision.RESOLVE_NOT_SENT if self.safe_to_repeat(message) else Decision.ASK_HUMAN


@dataclass
class UnknownReport:
    resolved: dict[str, str] = field(default_factory=dict)  # message id -> "sent" / "not_sent"
    asked: list[str] = field(default_factory=list)          # message ids handed to a human


async def resolve_unknown(client, policy: UnknownPolicy, *, ask_human: Callable[[Message], Awaitable[None]] | None = None,
                          instance_id: str | None = None) -> UnknownReport:
    """Apply ``policy`` to every UNKNOWN message of the tenant (or of one instance). Run it periodically and on ``message.status`` UNKNOWN."""
    report = UnknownReport()
    for m in await client.messages.list("UNKNOWN", instance_id=instance_id):
        d = policy(m)
        if d is Decision.ASK_HUMAN:
            report.asked.append(m.id)
            if ask_human is not None:
                await ask_human(m)
            continue
        await client.messages.resolve(m.id, sent=d is Decision.RESOLVE_SENT)
        report.resolved[m.id] = d.value
    return report
