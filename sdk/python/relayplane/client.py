"""Async client for the RelayPlane public API (httpx)."""
from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import time
from pathlib import Path
from typing import Any, Union

import httpx

from .errors import from_response
from .models import (CreatedInstance, Instance, Media, Message, Operation, OperationRef, Pairing, SentMessage)

DEFAULT_TIMEOUT = 30.0


class _Http:
    def __init__(self, base_url: str, api_key: str, timeout: float, transport: httpx.AsyncBaseTransport | None):
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport,
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": "relayplane-python/0.1.0"})

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

    async def send_text(self, instance_id: str, to: str, text: str, *, idempotency_key: str | None = None) -> SentMessage:
        return await self._send({"instance_id": instance_id, "to": to, "type": "text", "payload": {"text": text}}, idempotency_key)

    async def send_media(self, instance_id: str, to: str, media_id: str, *, type: str = "document",
                         caption: str = "", filename: str = "", idempotency_key: str | None = None) -> SentMessage:
        """Send an uploaded object (claim check). Never pass file bytes here."""
        payload: dict[str, Any] = {"media_id": media_id}
        if caption:
            payload["caption"] = caption
        if filename:
            payload["filename"] = filename
        return await self._send({"instance_id": instance_id, "to": to, "type": type, "payload": payload}, idempotency_key)

    async def _send(self, body: dict[str, Any], key: str | None) -> SentMessage:
        out, resp = await self._h.request("POST", "/api/v1/messages/send", json=body, idempotency_key=key)
        return SentMessage(out["message_id"], out["status"], _replayed(resp))

    async def get(self, message_id: str) -> Message:
        body, _ = await self._h.request("GET", f"/api/v1/messages/{message_id}")
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

    async def delete(self, media_id: str) -> None:
        await self._h.request("DELETE", f"/api/v1/media/{media_id}")


class RelayPlaneClient:
    """Entry point: `async with RelayPlaneClient(url, key) as rp: ...`"""

    def __init__(self, base_url: str, api_key: str, *, timeout: float = DEFAULT_TIMEOUT,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._http = _Http(base_url, api_key, timeout, transport)
        self.instances = InstancesAPI(self._http)
        self.messages = MessagesAPI(self._http)
        self.operations = OperationsAPI(self._http)
        self.media = MediaAPI(self._http)

    async def __aenter__(self) -> "RelayPlaneClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()
