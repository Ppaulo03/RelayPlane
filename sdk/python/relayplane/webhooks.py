"""Verification of RelayPlane webhook requests (the consumer side of tenant event delivery).

    event = verify_request(secrets, headers, body)   # raises WebhookSignatureError, otherwise returns the parsed event

The signature is HMAC-SHA256 over ``"<timestamp>.<body>"`` with the subscription secret, sent as ``X-RelayPlane-Signature: v1=<hex>``
(several ``v1=`` entries while a secret rotation is in its grace period). Reject old timestamps (replay protection) and
deduplicate by ``X-RelayPlane-Event-Id``: delivery is at-least-once.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Iterable, Mapping

HEADER_EVENT_ID = "x-relayplane-event-id"
HEADER_EVENT_TYPE = "x-relayplane-event-type"
HEADER_TIMESTAMP = "x-relayplane-timestamp"
HEADER_SIGNATURE = "x-relayplane-signature"
DEFAULT_TOLERANCE = 300.0


class WebhookSignatureError(Exception):
    """The request is not an authentic, fresh RelayPlane delivery."""


def sign(secret: str, timestamp: int, body: bytes) -> str:
    return hmac.new(secret.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256).hexdigest()


def verify_signature(secrets: Iterable[str] | str, signature_header: str, timestamp: int, body: bytes, *,
                     tolerance: float = DEFAULT_TOLERANCE, now: float | None = None) -> None:
    """Raise WebhookSignatureError unless one of the ``v1=`` signatures matches one of ``secrets``."""
    if isinstance(secrets, str):
        secrets = [secrets]
    secrets = list(secrets)
    current = time.time() if now is None else now
    if abs(current - timestamp) > tolerance:
        raise WebhookSignatureError("timestamp outside the tolerance (possible replay)")
    got = [p.strip()[3:] for p in signature_header.split(",") if p.strip().startswith("v1=")]
    for sig in got:
        for secret in secrets:
            if hmac.compare_digest(sig, sign(secret, timestamp, body)):
                return
    raise WebhookSignatureError("signature mismatch")


def verify_request(secrets: Iterable[str] | str, headers: Mapping[str, str], body: bytes, *,
                   tolerance: float = DEFAULT_TOLERANCE, now: float | None = None) -> dict[str, Any]:
    """Verify a delivery and return the parsed event (``event_id``, ``event_type``, ``tenant_id``, ``instance_id``, ``timestamp``, ``payload``)."""
    h = {k.lower(): v for k, v in headers.items()}
    try:
        ts = int(h[HEADER_TIMESTAMP])
        sig = h[HEADER_SIGNATURE]
    except (KeyError, ValueError) as exc:
        raise WebhookSignatureError("missing or malformed signature headers") from exc
    verify_signature(secrets, sig, ts, body, tolerance=tolerance, now=now)
    event = json.loads(body)
    if h.get(HEADER_EVENT_ID) and event.get("event_id") != h[HEADER_EVENT_ID]:
        raise WebhookSignatureError("event id header does not match the signed body")
    return event
