"""Async client for the RelayPlane public API (httpx)."""
from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import mimetypes
import time
from pathlib import Path
from typing import Any, Union

import httpx

from ._version import __version__
from .errors import from_response
from .models import (ApiKey, ReplyTo, CreatedInstance, Instance, Media, Message, Operation, OperationRef, Limits, Pairing, SentMessage, Subscription, WebhookDelivery)

DEFAULT_TIMEOUT = 30.0


class _Http:
    def __init__(self, base_url: str, api_key: str, timeout: float, transport: httpx.AsyncBaseTransport | None):
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport,
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": f"relayplane-python/{__version__}"})

    async def request(self, method: str, path: str, *, json: Any = None, content: Any = None,
                      idempotency_key: str | None = None, headers: dict[str, str] | None = None) -> tuple[Any, httpx.Response]:
        h = dict(headers or {})
        if idempotency_key:
            h["Idempotency-Key"] = idempotency_key
        resp = await self._client.request(method, path, json=json, content=content, headers=h)
        body: Any = None
        if resp.content:
            try:
                body = resp.json()
            except ValueError:
                body = None
        if resp.status_code >= 400:
            ra = resp.headers.get("Retry-After")
            retry = float(ra) if ra and ra.replace(".", "", 1).isdigit() else None
            raise from_response(resp.status_code, body, retry)
        return body, resp

    async def aclose(self) -> None:
        await self._client.aclose()


def _replayed(resp: httpx.Response) -> bool:
    return resp.headers.get("Idempotent-Replayed") == "true"


class InstancesAPI:
    def __init__(self, http: _Http):
        self._h = http

    async def create(self, name: str, *, idempotency_key: str | None = None) -> CreatedInstance:
        body, resp = await self._h.request("POST", "/api/v1/instances", json={"name": name}, idempotency_key=idempotency_key)
        return CreatedInstance(id=body["id"], status=body["status"], operation_id=body["operation_id"], replayed=_replayed(resp))

    async def list(self) -> list[Instance]:
        body, _ = await self._h.request("GET", "/api/v1/instances")
        return [Instance.from_dict(i) for i in body.get("instances", [])]

    async def get(self, instance_id: str) -> Instance:
        body, _ = await self._h.request("GET", f"/api/v1/instances/{instance_id}")
        return Instance.from_dict(body)

    async def get_or_create(self, name: str, *, idempotency_key: str | None = None) -> Instance:
        """Return the instance called `name`, creating it when it does not exist."""
        for inst in await self.list():
            if inst.name == name and inst.observed_state not in ("DELETING", "DELETED", "FAILED"):
                return inst
        created = await self.create(name, idempotency_key=idempotency_key or f"get-or-create:{name}")
        return await self.get(created.id)

    async def delete(self, instance_id: str, *, idempotency_key: str | None = None) -> OperationRef:
        body, resp = await self._h.request("DELETE", f"/api/v1/instances/{instance_id}", idempotency_key=idempotency_key)
        return OperationRef(body["operation_id"], body["status"], body.get("error_code", ""), _replayed(resp))

    async def get_qrcode(self, instance_id: str) -> Pairing:
        body, _ = await self._h.request("GET", f"/api/v1/instances/{instance_id}/qrcode")
        return Pairing(body.get("qrcode"), body.get("pairing_code"), body.get("expires_at", ""))

    async def get_pairing_code(self, instance_id: str) -> Pairing:
        body, _ = await self._h.request("GET", f"/api/v1/instances/{instance_id}/pairing-code")
        return Pairing(None, body.get("pairing_code"), body.get("expires_at", ""))

    async def send_presence(self, instance_id: str, to: str, state: str = "composing", *, duration_ms: int | None = None) -> None:
        """Show "typing…" (``composing``) or "recording audio…" (``recording``) to a contact for ``duration_ms`` (default 3 s, at most
        25 s); the state ends by itself, so there is nothing to clean up. ``paused`` stops it early. Best effort: it returns at once
        (202) and a failure is only logged on the server. The instance must be CONNECTED."""
        body: dict[str, Any] = {"to": to, "state": state}
        if duration_ms:
            body["duration_ms"] = duration_ms
        await self._h.request("POST", f"/api/v1/instances/{instance_id}/presence", json=body)

    async def reconnect(self, instance_id: str) -> OperationRef:
        body, _ = await self._h.request("POST", f"/api/v1/instances/{instance_id}/reconnect")
        return OperationRef(body["operation_id"], body["status"], body.get("error_code", ""))

    async def logout(self, instance_id: str) -> OperationRef:
        body, _ = await self._h.request("POST", f"/api/v1/instances/{instance_id}/logout")
        return OperationRef(body["operation_id"], body["status"], body.get("error_code", ""))

    async def migrate(self, instance_id: str, *, idempotency_key: str | None = None) -> OperationRef:
        body, resp = await self._h.request("POST", f"/api/v1/instances/{instance_id}/migrate", idempotency_key=idempotency_key)
        return OperationRef(body["operation_id"], body["status"], replayed=_replayed(resp))


class MessagesAPI:
    def __init__(self, http: _Http):
        self._h = http

    @staticmethod
    def _quote(reply_to: "ReplyTo | None") -> dict[str, Any]:
        return {"reply_to": reply_to.as_dict()} if reply_to else {}

    async def send_text(self, instance_id: str, to: str, text: str, *, reply_to: "ReplyTo | None" = None,
                        idempotency_key: str | None = None) -> SentMessage:
        """``reply_to`` quotes a message (see ``ReplyTo``): the grey box above your reply, which is what makes a "yes" unmistakably
        an answer to that message."""
        return await self._send({"instance_id": instance_id, "to": to, "type": "text", "payload": {"text": text, **self._quote(reply_to)}},
                                idempotency_key)

    async def send_media(self, instance_id: str, to: str, media_id: str, *, type: str = "document", caption: str = "", filename: str = "",
                         reply_to: "ReplyTo | None" = None, idempotency_key: str | None = None) -> SentMessage:
        """Send an uploaded object (claim check). Never pass file bytes here."""
        payload: dict[str, Any] = {"media_id": media_id, **self._quote(reply_to)}
        if caption:
            payload["caption"] = caption
        if filename:
            payload["filename"] = filename
        return await self._send({"instance_id": instance_id, "to": to, "type": type, "payload": payload}, idempotency_key)

    async def mark_read(self, instance_id: str, chat: str, provider_message_ids: list[str]) -> int:
        """Mark messages the contact sent as read (they see the blue ticks). Pass the ``provider_message_id`` of each
        ``message.received``; up to 50 per call. Meant for direct chats. Returns how many were marked."""
        out, _ = await self._h.request("POST", "/api/v1/messages/read",
                                       json={"instance_id": instance_id, "chat": chat, "provider_message_ids": provider_message_ids})
        return int(out["read"])

    async def _send(self, body: dict[str, Any], key: str | None) -> SentMessage:
        out, resp = await self._h.request("POST", "/api/v1/messages/send", json=body, idempotency_key=key)
        return SentMessage(out["message_id"], out["status"], _replayed(resp))

    async def list(self, status: str, *, instance_id: str | None = None, limit: int = 100) -> list[Message]:
        """Your messages in a status, oldest first (by instance, then sequence). ``list("UNKNOWN")`` is how you find the sends whose
        outcome is ambiguous and wait for ``resolve``; each one holds back the later messages of its instance."""
        params = f"?status={status}&limit={limit}" + (f"&instance_id={instance_id}" if instance_id else "")
        body, _ = await self._h.request("GET", "/api/v1/messages" + params)
        return [Message.from_dict(m) for m in body["messages"]]

    async def get(self, message_id: str) -> Message:
        body, _ = await self._h.request("GET", f"/api/v1/messages/{message_id}")
        return Message.from_dict(body)

    async def resolve(self, message_id: str, *, sent: bool) -> Message:
        """Settle an UNKNOWN message (ambiguous dispatch): `sent=True` if the recipient got it, `False` if it was verified as not sent.

        Until an UNKNOWN message is resolved, later messages of the same instance are held back to preserve ordering."""
        body, _ = await self._h.request("POST", f"/api/v1/messages/{message_id}/resolve",
                                        json={"outcome": "sent" if sent else "not_sent"})
        return Message.from_dict(body)

    async def wait(self, message_id: str, *, until: tuple[str, ...] = ("ACCEPTED", "DELIVERED", "READ", "FAILED", "UNKNOWN"),
                   timeout: float = 60.0, interval: float = 0.5) -> Message:
        deadline = time.monotonic() + timeout
        while True:
            msg = await self.get(message_id)
            if msg.status in until:
                return msg
            if time.monotonic() >= deadline:
                raise TimeoutError(f"message {message_id} still {msg.status} after {timeout}s")
            await asyncio.sleep(interval)


class OperationsAPI:
    def __init__(self, http: _Http):
        self._h = http

    async def get(self, operation_id: str) -> Operation:
        body, _ = await self._h.request("GET", f"/api/v1/operations/{operation_id}")
        return Operation.from_dict(body)

    async def wait(self, operation_id: str, *, timeout: float = 120.0, interval: float = 0.5) -> Operation:
        deadline = time.monotonic() + timeout
        while True:
            op = await self.get(operation_id)
            if op.done:
                return op
            if time.monotonic() >= deadline:
                raise TimeoutError(f"operation {operation_id} still {op.status} after {timeout}s")
            await asyncio.sleep(interval)


class MediaAPI:
    def __init__(self, http: _Http):
        self._h = http

    async def upload(self, data: Union[bytes, str, Path], *, filename: str | None = None, content_type: str | None = None) -> Media:
        """Upload bytes (or a file path) and return the READY media object.

        The content is streamed to the object store through the gateway; only the
        returned media id travels in message commands."""
        if isinstance(data, (str, Path)):
            path = Path(data)
            filename = filename or path.name
            data = path.read_bytes()
        filename = filename or "file"
        content_type = content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        body, _ = await self._h.request("POST", "/api/v1/media/uploads", json={
            "content_type": content_type, "size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "filename": filename})
        out, _ = await self._h.request("PUT", f"/api/v1/media/{body['media_id']}/content", content=data,
                                       headers={"Content-Type": "application/octet-stream"})
        return Media.from_dict(out)

    async def get(self, media_id: str) -> Media:
        body, _ = await self._h.request("GET", f"/api/v1/media/{media_id}")
        return Media.from_dict(body)

    async def download(self, media_id: str) -> bytes:
        """The bytes of a media: an attachment of a message you received (``message.received`` with ``media.status == "READY"``)
        or something you uploaded. The checksum the gateway reports is verified, so a truncated body is an error."""
        _, resp = await self._h.request("GET", f"/api/v1/media/{media_id}/content")
        data = resp.content
        expected = resp.headers.get("ETag", "").strip('"')
        if expected and hashlib.sha256(data).hexdigest() != expected:
            raise ValueError(f"downloaded media {media_id} does not match its checksum")
        return data

    async def delete(self, media_id: str) -> None:
        await self._h.request("DELETE", f"/api/v1/media/{media_id}")


class ContactsAPI:
    """Data about the people who talk to you."""

    def __init__(self, http: _Http):
        self._h = http

    async def erase(self, number: str) -> dict[str, Any]:
        """Erase what RelayPlane keeps about one person (LGPD/GDPR): the recipient and text of the messages you sent them,
        the events that mention them (delivered, waiting or dead-lettered) and the files they sent. Idempotent. Returns counts,
        never the number. Subscriptions that had not received some of those events yet will see gaps in ``sequence``."""
        out, _ = await self._h.request("DELETE", f"/api/v1/contacts/{number}/data")
        return out


class ApiKeysAPI:
    """Your own API keys. Rotating without downtime: ``create`` a new key, deploy it, then ``revoke`` the old one
    (both authenticate in between). The last usable key cannot be revoked."""

    def __init__(self, http: _Http):
        self._h = http

    async def create(self, name: str, *, expires_in_seconds: int | None = None) -> ApiKey:
        body: dict[str, Any] = {"name": name}
        if expires_in_seconds:
            body["expires_in_seconds"] = expires_in_seconds
        out, _ = await self._h.request("POST", "/api/v1/api-keys", json=body)
        return ApiKey.from_dict(out)

    async def list(self) -> list[ApiKey]:
        out, _ = await self._h.request("GET", "/api/v1/api-keys")
        return [ApiKey.from_dict(k) for k in out["api_keys"]]

    async def revoke(self, key_id: str) -> None:
        await self._h.request("DELETE", f"/api/v1/api-keys/{key_id}")


class LimitsAPI:
    """What this deployment guarantees; assert it at startup instead of hard-coding assumptions."""

    def __init__(self, http: _Http):
        self._h = http

    async def get(self) -> Limits:
        out, _ = await self._h.request("GET", "/api/v1/limits")
        return Limits.from_dict(out)


class SubscriptionsAPI:
    """Webhook delivery of your events (message.received, message.outbound_status, ...). See relayplane.webhooks to verify requests."""

    def __init__(self, http: _Http):
        self._h = http

    async def create(self, url: str, *, event_types: list[str] | None = None, instance_ids: list[str] | None = None,
                     exclude_groups: bool = False, idempotency_key: str | None = None) -> Subscription:
        """The returned subscription carries the signing secret ONCE; store it.

        With an ``idempotency_key`` a repeated call (a deploy script that runs twice) returns the SAME subscription with
        ``replayed=True`` and without the secret: rotate it with ``rotate_secret`` if it was lost."""
        body: dict[str, Any] = {"url": url}
        if event_types:
            body["event_types"] = event_types
        if instance_ids:
            body["instance_ids"] = instance_ids
        if exclude_groups:
            body["exclude_groups"] = True
        out, resp = await self._h.request("POST", "/api/v1/subscriptions", json=body, idempotency_key=idempotency_key)
        sub = Subscription.from_dict(out)
        return dataclasses.replace(sub, replayed=_replayed(resp))

    async def pause(self, subscription_id: str) -> None:
        """Hold deliveries (your own backpressure, e.g. while you deploy): events keep being queued, none is sent until ``resume``."""
        await self._h.request("POST", f"/api/v1/subscriptions/{subscription_id}/pause")

    async def resume(self, subscription_id: str) -> None:
        """Send what accumulated while paused, in sequence order."""
        await self._h.request("POST", f"/api/v1/subscriptions/{subscription_id}/resume")

    async def list(self) -> list[Subscription]:
        out, _ = await self._h.request("GET", "/api/v1/subscriptions")
        return [Subscription.from_dict(s) for s in out.get("subscriptions", [])]

    async def get(self, subscription_id: str) -> Subscription:
        out, _ = await self._h.request("GET", f"/api/v1/subscriptions/{subscription_id}")
        return Subscription.from_dict(out)

    async def delete(self, subscription_id: str) -> None:
        await self._h.request("DELETE", f"/api/v1/subscriptions/{subscription_id}")

    async def rotate_secret(self, subscription_id: str) -> Subscription:
        """New secret (shown once). For 24 h requests carry both signatures, so you can switch without a gap."""
        out, _ = await self._h.request("POST", f"/api/v1/subscriptions/{subscription_id}/rotate-secret")
        return Subscription.from_dict(out)

    async def deliveries(self, subscription_id: str, *, status: str | None = None, limit: int = 50) -> list[WebhookDelivery]:
        """`status="DEAD"` is the dead-letter queue."""
        q = f"?limit={limit}" + (f"&status={status}" if status else "")
        out, _ = await self._h.request("GET", f"/api/v1/subscriptions/{subscription_id}/deliveries{q}")
        return [WebhookDelivery.from_dict(d) for d in out.get("deliveries", [])]

    async def redeliver(self, delivery_id: str) -> None:
        await self._h.request("POST", f"/api/v1/deliveries/{delivery_id}/redeliver")


class RelayPlaneClient:
    """Entry point: `async with RelayPlaneClient(url, key) as rp: ...`"""

    def __init__(self, base_url: str, api_key: str, *, timeout: float = DEFAULT_TIMEOUT,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._http = _Http(base_url, api_key, timeout, transport)
        self.instances = InstancesAPI(self._http)
        self.messages = MessagesAPI(self._http)
        self.operations = OperationsAPI(self._http)
        self.media = MediaAPI(self._http)
        self.subscriptions = SubscriptionsAPI(self._http)
        self.limits = LimitsAPI(self._http)
        self.api_keys = ApiKeysAPI(self._http)
        self.contacts = ContactsAPI(self._http)

    async def __aenter__(self) -> "RelayPlaneClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()
