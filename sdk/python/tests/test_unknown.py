import json

import httpx

from relayplane import RelayPlaneClient
from relayplane.unknown import Decision, UnknownPolicy, resolve_unknown


def make(resolved):
    def handler(req: httpx.Request) -> httpx.Response:
        if req.method == "GET":
            assert req.url.params["status"] == "UNKNOWN"
            return httpx.Response(200, json={"messages": [{"id": "m1", "status": "UNKNOWN", "to": "A"}, {"id": "m2", "status": "UNKNOWN", "to": "B"}]})
        resolved[req.url.path.split("/")[-2]] = json.loads(req.content)["outcome"]
        return httpx.Response(200, json={"id": "x", "status": "ACCEPTED"})
    return RelayPlaneClient("http://gw.test", "k", transport=httpx.MockTransport(handler))


async def test_default_policy_asks_a_human_and_resolves_nothing():
    resolved, asked = {}, []

    async def ask(m):
        asked.append(m.id)
    async with make(resolved) as rp:
        report = await resolve_unknown(rp, UnknownPolicy(), ask_human=ask)
    assert resolved == {} and asked == ["m1", "m2"] and report.asked == ["m1", "m2"]


async def test_only_messages_marked_safe_to_repeat_are_resolved_automatically():
    resolved = {}
    policy = UnknownPolicy(safe_to_repeat=lambda m: m.id == "m2")
    async with make(resolved) as rp:
        report = await resolve_unknown(rp, policy)
    assert resolved == {"m2": "not_sent"} and report.resolved == {"m2": "not_sent"} and report.asked == ["m1"]


async def test_a_custom_decision_can_resolve_as_sent():
    resolved = {}
    async with make(resolved) as rp:
        await resolve_unknown(rp, UnknownPolicy(decide=lambda m: Decision.RESOLVE_SENT))
    assert resolved == {"m1": "sent", "m2": "sent"}
