import json

import httpx
import pytest

from relayplane import RelayPlaneClient, WebhookSignatureError, verify_request, verify_signature
from relayplane.webhooks import sign

# Golden vector shared with the Go side (internal/core/subscription): computing it differently breaks every consumer.
SECRET, TS, BODY = "whsec_golden", 1800000000, b'{"event_id":"evt_1","payload":{"text":"oi"}}'
GOLDEN = "a5433ca61a8029297236aed569798e0ad71a192250b77b5d17f38893f1e1bf38"


def headers(sig_header, ts=TS, event_id="evt_1"):
    return {"X-RelayPlane-Timestamp": str(ts), "X-RelayPlane-Signature": sig_header, "X-RelayPlane-Event-Id": event_id}


def test_signature_matches_the_go_implementation():
    assert sign(SECRET, TS, BODY) == GOLDEN


def test_verify_request_accepts_a_genuine_delivery_and_returns_the_event():
    event = verify_request(SECRET, headers("v1=" + GOLDEN), BODY, now=TS + 5)
    assert event["event_id"] == "evt_1" and event["payload"]["text"] == "oi"


def test_rotation_header_with_two_signatures_verifies_with_either_secret():
    old = sign("whsec_old", TS, BODY)
    hdr = f"v1={old},v1={GOLDEN}"
    verify_request(SECRET, headers(hdr), BODY, now=TS)
    verify_request("whsec_old", headers(hdr), BODY, now=TS)
    with pytest.raises(WebhookSignatureError):
        verify_request("whsec_unrelated", headers(hdr), BODY, now=TS)


@pytest.mark.parametrize("mutate", ["body", "secret", "old", "future", "unknown_version", "missing", "bad_ts", "event_id"])
def test_verify_request_rejects_everything_that_is_not_authentic_and_fresh(mutate):
    body, secret, hdr, now = BODY, SECRET, headers("v1=" + GOLDEN), TS
    if mutate == "body":
        body = BODY.replace(b"oi", b"ok")
    elif mutate == "secret":
        secret = "whsec_other"
    elif mutate == "old":
        now = TS + 3600
    elif mutate == "future":
        now = TS - 3600
    elif mutate == "unknown_version":
        hdr = headers("v2=" + GOLDEN)
    elif mutate == "missing":
        hdr = {}
    elif mutate == "bad_ts":
        hdr = headers("v1=" + GOLDEN, ts="not-a-number")
    elif mutate == "event_id":
        hdr = headers("v1=" + GOLDEN, event_id="evt_other")  # a replayed signature on a different delivery
    with pytest.raises(WebhookSignatureError):
        verify_request(secret, hdr, body, now=now)


def test_verify_signature_tolerance_is_configurable():
    verify_signature(SECRET, "v1=" + GOLDEN, TS, BODY, tolerance=10, now=TS + 9)
    with pytest.raises(WebhookSignatureError):
        verify_signature(SECRET, "v1=" + GOLDEN, TS, BODY, tolerance=10, now=TS + 11)


async def test_subscriptions_api_roundtrip_and_secret_hygiene():
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((req.method, req.url.path, req.url.query.decode(), json.loads(req.content) if req.content else None))
        sub = {"id": "sub_1", "url": "https://agent.example.com/h", "event_types": ["message.received"], "instance_ids": [], "active": True,
               "created_at": "2026-10-03T00:00:00Z"}
        if req.method == "POST" and req.url.path == "/api/v1/subscriptions":
            return httpx.Response(201, json={**sub, "secret": "whsec_abc"})
        if req.url.path.endswith("/rotate-secret"):
            return httpx.Response(200, json={**sub, "secret": "whsec_new", "previous_secret_valid_until": "2026-10-04T00:00:00Z"})
        if req.url.path.endswith("/deliveries"):
            return httpx.Response(200, json={"deliveries": [{"id": "dlv_1", "event_id": "evt_1", "event_type": "message.received",
                                                              "instance_id": "i", "status": "DEAD", "attempts": 10, "last_error": "http 500"}]})
        if req.method == "GET" and req.url.path == "/api/v1/subscriptions":
            return httpx.Response(200, json={"subscriptions": [sub]})
        return httpx.Response(204 if req.method == "DELETE" else 202)

    rp = RelayPlaneClient("http://gw.test", "key", transport=httpx.MockTransport(handler))
    async with rp:
        created = await rp.subscriptions.create("https://agent.example.com/h", event_types=["message.received"])
        assert created.secret == "whsec_abc" and created.event_types == ("message.received",)
        assert "whsec_abc" not in repr(created), "the secret must never be printed"
        assert (await rp.subscriptions.list())[0].secret is None
        rotated = await rp.subscriptions.rotate_secret("sub_1")
        assert rotated.secret == "whsec_new" and rotated.previous_secret_valid_until
        dead = await rp.subscriptions.deliveries("sub_1", status="DEAD", limit=5)
        assert dead[0].status == "DEAD" and dead[0].attempts == 10
        await rp.subscriptions.redeliver("dlv_1")
        await rp.subscriptions.delete("sub_1")
    assert seen[0][3] == {"url": "https://agent.example.com/h", "event_types": ["message.received"]}
    assert ("GET", "/api/v1/subscriptions/sub_1/deliveries", "limit=5&status=DEAD", None) in seen
    assert ("POST", "/api/v1/deliveries/dlv_1/redeliver", "", None) in seen
