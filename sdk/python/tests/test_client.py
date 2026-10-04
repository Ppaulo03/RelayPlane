import hashlib
import json

import httpx
import pytest

from relayplane import (ReplyTo, IdempotencyConflict, NotFound, PayloadTooLarge, RelayPlaneClient, RequestInProgress,
                        ServiceUnavailable)


def client(handler):
    return RelayPlaneClient("http://gw.test", "key-123", transport=httpx.MockTransport(handler))


async def test_create_instance_sends_auth_and_idempotency_and_reports_replay():
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["auth"] = req.headers["authorization"]
        seen["idem"] = req.headers.get("idempotency-key")
        seen["body"] = json.loads(req.content)
        return httpx.Response(201, json={"id": "inst_1", "status": "AWAITING_PAIRING", "operation_id": "op_1"},
                              headers={"Idempotent-Replayed": "true"})

    async with client(handler) as rp:
        out = await rp.instances.create("Comercial", idempotency_key="k1")
    assert seen == {"auth": "Bearer key-123", "idem": "k1", "body": {"name": "Comercial"}}
    assert out.id == "inst_1" and out.replayed and out.operation_id == "op_1"


async def test_get_or_create_reuses_existing_instance():
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append((req.method, req.url.path))
        if req.method == "GET":
            return httpx.Response(200, json={"instances": [{"id": "inst_9", "name": "Comercial", "status": "CONNECTED",
                                                             "desired_state": "CONNECTED", "observed_state": "CONNECTED"}]})
        raise AssertionError("must not create")

    async with client(handler) as rp:
        inst = await rp.instances.get_or_create("Comercial")
    assert inst.id == "inst_9" and calls == [("GET", "/api/v1/instances")]


async def test_get_or_create_creates_with_a_stable_idempotency_key():
    keys = []

    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "GET" and req.url.path == "/api/v1/instances":
            return httpx.Response(200, json={"instances": []})
        if req.method == "POST":
            keys.append(req.headers["idempotency-key"])
            return httpx.Response(201, json={"id": "inst_2", "status": "AWAITING_PAIRING", "operation_id": "op"})
        return httpx.Response(200, json={"id": "inst_2", "name": "X", "status": "AWAITING_PAIRING",
                                         "desired_state": "CONNECTED", "observed_state": "AWAITING_PAIRING"})

    async with client(handler) as rp:
        await rp.instances.get_or_create("X")
    assert keys == ["get-or-create:X"]


async def test_send_text_and_error_mapping():
    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content)
        assert body == {"instance_id": "i", "to": "5562999999999", "type": "text", "payload": {"text": "oi"}}
        return httpx.Response(202, json={"message_id": "msg_1", "status": "QUEUED"})

    async with client(handler) as rp:
        sent = await rp.messages.send_text("i", "5562999999999", "oi", idempotency_key="order-1")
    assert sent.message_id == "msg_1" and sent.status == "QUEUED"


@pytest.mark.parametrize("status,code,exc", [
    (404, "not_found", NotFound), (422, "idempotency_key_reuse", IdempotencyConflict),
    (409, "request_in_progress", RequestInProgress), (413, "payload_too_large", PayloadTooLarge),
    (503, "no_capacity", ServiceUnavailable)])
async def test_errors_are_canonical(status, code, exc):
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"error": {"code": code, "message": "m"}}, headers={"Retry-After": "5"})

    async with client(handler) as rp:
        with pytest.raises(exc) as ei:
            await rp.instances.get("x")
    assert ei.value.code == code and ei.value.status == status and ei.value.retry_after == 5.0


async def test_media_upload_streams_content_and_sends_by_reference():
    data = b"%PDF-" + b"x" * 5000
    steps = []

    def handler(req: httpx.Request) -> httpx.Response:
        steps.append((req.method, req.url.path))
        if req.url.path == "/api/v1/media/uploads":
            meta = json.loads(req.content)
            assert meta["sha256"] == hashlib.sha256(data).hexdigest() and meta["size"] == len(data)
            assert meta["content_type"] == "application/pdf"
            return httpx.Response(201, json={"media_id": "med_1", "object_key": "t/media/med_1/a.pdf"})
        if req.url.path == "/api/v1/media/med_1/content":
            assert req.content == data
            return httpx.Response(200, json={"id": "med_1", "status": "READY", "content_type": "application/pdf",
                                             "size": len(data), "sha256": "s", "filename": "a.pdf"})
        body = json.loads(req.content)
        assert "base64" not in json.dumps(body) and body["payload"]["media_id"] == "med_1"
        return httpx.Response(202, json={"message_id": "msg_2", "status": "QUEUED"})

    async with client(handler) as rp:
        media = await rp.media.upload(data, filename="a.pdf")
        sent = await rp.messages.send_media("inst", "5562999999999", media.id, caption="contrato")
    assert media.status == "READY" and sent.message_id == "msg_2"
    assert [s[1] for s in steps] == ["/api/v1/media/uploads", "/api/v1/media/med_1/content", "/api/v1/messages/send"]


async def test_wait_polls_until_terminal_status():
    statuses = iter(["QUEUED", "DISPATCHING", "ACCEPTED"])

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "msg_1", "status": next(statuses)})

    async with client(handler) as rp:
        msg = await rp.messages.wait("msg_1", interval=0.001)
    assert msg.status == "ACCEPTED"


async def test_operation_wait_times_out():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "op", "type": "MIGRATE", "status": "RUNNING"})

    async with client(handler) as rp:
        with pytest.raises(TimeoutError):
            await rp.operations.wait("op", timeout=0.05, interval=0.01)


def test_pairing_repr_never_shows_the_credential():
    from relayplane import Pairing
    assert "QR" not in repr(Pairing("QRDATA", "ABCD", "x"))


def test_sdk_does_not_know_infrastructure():
    import pathlib
    src = "".join(p.read_text(encoding="utf-8") for p in pathlib.Path(__file__).parents[1].joinpath("relayplane").glob("*.py")).lower()
    for forbidden in ("evolution", "redis", "minio", "baileys", "node_id", "provider node"):
        assert forbidden not in src, forbidden


@pytest.mark.parametrize("sent,outcome", [(True, "sent"), (False, "not_sent")])
async def test_resolve_unknown_message(sent, outcome):
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/api/v1/messages/msg_1/resolve"
        assert json.loads(req.content) == {"outcome": outcome}
        return httpx.Response(200, json={"id": "msg_1", "status": "ACCEPTED" if sent else "FAILED", "sequence_no": 7})

    async with client(handler) as rp:
        msg = await rp.messages.resolve("msg_1", sent=sent)
    assert msg.sequence_no == 7 and msg.status == ("ACCEPTED" if sent else "FAILED")


async def test_media_download_verifies_the_checksum():
    data = b"OggS voice"

    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/api/v1/media/med_1/content"
        return httpx.Response(200, content=data, headers={"ETag": '"' + hashlib.sha256(data).hexdigest() + '"'})

    async with client(handler) as rp:
        assert await rp.media.download("med_1") == data

    def truncated(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data[:-1], headers={"ETag": '"' + hashlib.sha256(data).hexdigest() + '"'})

    async with client(truncated) as rp:
        with pytest.raises(ValueError):
            await rp.media.download("med_1")


async def test_api_key_rotation_calls():
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((req.method, req.url.path))
        if req.method == "POST":
            assert json.loads(req.content) == {"name": "agent-prod", "expires_in_seconds": 3600}
            return httpx.Response(201, json={"id": "key_2", "name": "agent-prod", "prefix": "rpk_ab12", "created_at": "2026-10-03T00:00:00Z",
                                             "api_key": "rpk_ab12secret"})
        if req.method == "GET":
            return httpx.Response(200, json={"api_keys": [{"id": "key_2", "name": "agent-prod", "prefix": "rpk_ab12", "current": True},
                                                          {"id": "key_1", "name": "initial", "prefix": "rpk_zz99", "revoked_at": "2026-10-03T01:00:00Z"}]})
        return httpx.Response(204)

    async with client(handler) as rp:
        k = await rp.api_keys.create("agent-prod", expires_in_seconds=3600)
        keys = await rp.api_keys.list()
        await rp.api_keys.revoke("key_1")
    assert k.secret == "rpk_ab12secret" and "secret" not in repr(k)
    assert [x.active for x in keys] == [True, False] and keys[0].current and keys[0].secret == ""
    assert seen == [("POST", "/api/v1/api-keys"), ("GET", "/api/v1/api-keys"), ("DELETE", "/api/v1/api-keys/key_1")]


async def test_subscription_pause_resume_and_backlog():
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((req.method, req.url.path))
        if req.method == "GET":
            return httpx.Response(200, json={"subscriptions": [{"id": "sub_1", "url": "https://a/h", "active": True, "paused": True,
                                                                "backlog": {"pending": 42, "oldest_pending_seconds": 90}}]})
        return httpx.Response(204)

    async with client(handler) as rp:
        await rp.subscriptions.pause("sub_1")
        subs = await rp.subscriptions.list()
        await rp.subscriptions.resume("sub_1")
    assert subs[0].paused and subs[0].pending == 42 and subs[0].oldest_pending_seconds == 90
    assert seen == [("POST", "/api/v1/subscriptions/sub_1/pause"), ("GET", "/api/v1/subscriptions"), ("POST", "/api/v1/subscriptions/sub_1/resume")]


async def test_contact_erasure_call():
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.method == "DELETE" and req.url.path == "/api/v1/contacts/5562999999999/data"
        return httpx.Response(200, json={"messages_anonymized": 3, "messages_cancelled": 1, "events_deleted": 5, "attachments_deleted": 1,
                                         "erased_at": "2026-10-03T00:00:00Z"})

    async with client(handler) as rp:
        out = await rp.contacts.erase("5562999999999")
    assert out["messages_anonymized"] == 3 and out["attachments_deleted"] == 1


async def test_reply_to_quotes_the_users_message_or_our_own():
    bodies = []

    def handler(req: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(req.content))
        return httpx.Response(202, json={"message_id": "msg_9", "status": "QUEUED"})

    async with client(handler) as rp:
        await rp.messages.send_text("inst", "5562", "Confirma?", reply_to=ReplyTo.to_user_message("3EB0USER", "quero agendar"))
        await rp.messages.send_text("inst", "5562", "ok", reply_to=ReplyTo.to_own_message("msg_1"))
        await rp.messages.send_text("inst", "5562", "oi")
        await rp.messages.send_media("inst", "5562", "med_1", reply_to=ReplyTo.to_user_message("3EB0DOC"))
    assert bodies[0]["payload"]["reply_to"] == {"provider_message_id": "3EB0USER", "text": "quero agendar"}
    assert bodies[1]["payload"]["reply_to"] == {"message_id": "msg_1"}
    assert "reply_to" not in bodies[2]["payload"]
    assert bodies[3]["payload"]["reply_to"] == {"provider_message_id": "3EB0DOC"}
    with pytest.raises(ValueError):
        ReplyTo().as_dict()
    with pytest.raises(ValueError):
        ReplyTo(provider_message_id="a", message_id="b").as_dict()


async def test_typing_indicator_and_read_receipts():
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((req.url.path, json.loads(req.content)))
        if req.url.path.endswith("/presence"):
            return httpx.Response(202, json={"accepted": True})
        return httpx.Response(200, json={"read": 2})

    async with client(handler) as rp:
        await rp.instances.send_presence("inst_1", "5562988887777", "recording", duration_ms=4000)
        await rp.instances.send_presence("inst_1", "5562988887777")
        n = await rp.messages.mark_read("inst_1", "5562988887777", ["WA-1", "WA-2"])
    assert n == 2
    assert seen[0] == ("/api/v1/instances/inst_1/presence", {"to": "5562988887777", "state": "recording", "duration_ms": 4000})
    assert seen[1][1] == {"to": "5562988887777", "state": "composing"}
    assert seen[2] == ("/api/v1/messages/read", {"instance_id": "inst_1", "chat": "5562988887777", "provider_message_ids": ["WA-1", "WA-2"]})


async def test_list_messages_by_status():
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.url.path == "/api/v1/messages" and req.url.params["status"] == "UNKNOWN" and req.url.params["instance_id"] == "inst_1"
        return httpx.Response(200, json={"messages": [{"id": "msg_1", "status": "UNKNOWN", "error_code": "AMBIGUOUS_DISPATCH", "sequence": 4}]})

    async with client(handler) as rp:
        ms = await rp.messages.list("UNKNOWN", instance_id="inst_1")
    assert [m.id for m in ms] == ["msg_1"] and ms[0].status == "UNKNOWN"
