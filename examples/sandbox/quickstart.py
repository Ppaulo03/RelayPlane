"""RelayPlane sandbox quickstart: a whole conversation without a WhatsApp number.

    sh deploy/docker/sandbox.sh up
    sh deploy/docker/sandbox.sh example

What it does, against the REAL gateway/worker/reconciler and a provider simulator:
  1. creates a tenant (admin key) and a webhook subscription pointing at a receiver this script runs;
  2. creates an instance and "scans the QR" through the simulator's control API;
  3. sends a message through the public API and waits for the signed ``message.outbound_status`` (ACCEPTED) webhook;
  4. plays the user ANSWERING BY QUOTING that message and checks the signed ``message.received`` webhook carries
     ``reply_to_provider_message_id`` equal to the id of the message we sent: the evidence a confirmation relies on;
  5. plays the user sending a VOICE NOTE: RelayPlane downloads it from the node, stores it, and only then delivers the
     ``message.received`` webhook with ``media.status == "READY"``; the script downloads the audio and checks every byte;
  6. answers like a person: marks the voice note as read, shows "typing…" and replies QUOTING it.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

from relayplane import Event, RelayPlaneClient, ReplyTo, WebhookSignatureError, verify_request

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:18080")
ADMIN_KEY = os.environ["ADMIN_API_KEY"]
SIM_NODES = dict(item.split("=", 1) for item in os.environ["SIM_NODES"].split(","))  # control URL -> node api key
RECEIVER_PORT = int(os.environ.get("RECEIVER_PORT", "18095"))
RECEIVER_URL = os.environ.get("RECEIVER_URL", f"http://host.docker.internal:{RECEIVER_PORT}/hook")

received: list[dict] = []
secret_holder: dict[str, str] = {}
bad: list[str] = []


class Hook(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        try:
            event = verify_request(secret_holder.get("secret", ""), dict(self.headers), body)
        except WebhookSignatureError as exc:
            bad.append(str(exc))
            self.send_response(401)
            self.end_headers()
            return
        received.append(event)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *_):  # keep the transcript readable
        pass


def wait_for(what: str, cond, timeout: float = 30.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = cond()
        if value:
            return value
        time.sleep(0.2)
    raise SystemExit(f"timed out waiting for: {what}")


def control(url: str, key: str, method: str, path: str, **body):
    r = httpx.request(method, f"{url}{path}", headers={"apikey": key}, json=body or None, timeout=15)
    r.raise_for_status()
    return r.json() if r.content else None


async def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", RECEIVER_PORT), Hook)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    tenant = httpx.post(f"{GATEWAY}/api/v1/tenants", headers={"Authorization": f"Bearer {ADMIN_KEY}"}, json={"name": "sandbox"}, timeout=15)
    tenant.raise_for_status()
    api_key = tenant.json()["api_key"]
    print("1. tenant created")

    async with RelayPlaneClient(GATEWAY, api_key) as rp:
        limits = await rp.limits.get()
        limits.assert_retry_horizon_within_idempotency(3600)
        sub = await rp.subscriptions.create(RECEIVER_URL, event_types=["message.received", "message.outbound_status"],
                                            exclude_groups=True, idempotency_key="sandbox-quickstart")
        secret_holder["secret"] = sub.secret or ""
        print(f"2. subscription {sub.id} -> {RECEIVER_URL} (idempotency window {limits.idempotency_retention_seconds}s)")

        inst = await rp.instances.create("quickstart", idempotency_key="sandbox-instance")
        print(f"3. instance {inst.id} is {inst.status}: waiting for pairing")
        node_url = node_key = None
        for url, key in SIM_NODES.items():
            names = [i["name"] for i in control(url, key, "GET", "/_sim/instances")["instances"]]
            if inst.id in names:
                node_url, node_key = url, key
        if not node_url:
            raise SystemExit("no simulator node holds the instance")
        control(node_url, node_key, "POST", f"/_sim/instances/{inst.id}/scan")  # the user scans the QR code
        wait_for("instance CONNECTED", lambda: _connected(api_key, inst.id))
        print("   user scanned the QR code: instance CONNECTED")

        sent = await rp.messages.send_text(inst.id, "5562988887777", "Confirma o agendamento amanhã às 15h?", idempotency_key="sandbox-prompt-1")
        msg = await rp.messages.wait(sent.message_id, timeout=30)
        print(f"4. message {sent.message_id} is {msg.status}, provider id {msg.provider_message_id}, accepted at {msg.accepted_at}")
        if msg.status not in ("ACCEPTED", "DELIVERED", "READ") or not msg.provider_message_id:
            raise SystemExit(f"expected an accepted message with a provider id, got {msg}")
        wait_for("ACCEPTED status webhook", lambda: any(e["event_type"] == "message.outbound_status" and e["payload"]["message_id"] == sent.message_id
                                                          and e["payload"]["status"] == "ACCEPTED" for e in received))

        # the user answers by QUOTING our message
        res = control(node_url, node_key, "POST", f"/_sim/instances/{inst.id}/inbound", text="sim", reply_to="last_sent", push_name="Ana")
        print(f"5. user replied 'sim' quoting {res['reply_to']}")
        event = wait_for("message.received webhook", lambda: next((e for e in received if e["event_type"] == "message.received"), None))
        quoted = event["payload"]["reply_to_provider_message_id"]
        if quoted != msg.provider_message_id:
            raise SystemExit(f"reply_to {quoted!r} is not the id of our message {msg.provider_message_id!r}")
        print(f"   webhook: message.received text={event['payload']['text']!r} reply_to matches our message: confirmation evidence OK")

        # the user sends a voice note: the event is delivered once RelayPlane has downloaded and stored the audio
        voice = b"OggS" + bytes(60)
        res = control(node_url, node_key, "POST", f"/_sim/instances/{inst.id}/inbound", type="audio", seconds=3, push_name="Ana",
                      content=base64.b64encode(voice).decode())
        audio = wait_for("voice note webhook", lambda: next((e for e in received if e["event_type"] == "message.received"
                                                              and e["payload"].get("media", {}).get("kind") == "audio"), None))
        media = Event.from_dict(audio).media
        if not media or not media.ready:
            raise SystemExit(f"expected a READY attachment, got {audio['payload'].get('media')}")
        got = await rp.media.download(media.media_id)
        if got != voice:
            raise SystemExit("the downloaded voice note is not what the user sent")
        print(f"6. voice note {res['id']}: media {media.media_id} READY ({media.size} bytes, {media.mime_type}); downloaded and verified")

        # the agent answers like a person: marks the voice note as read, shows "typing…", then replies QUOTING it
        voice_id = audio["payload"]["provider_message_id"]
        sender = audio["payload"]["from"]
        await rp.messages.mark_read(inst.id, sender, [voice_id])
        await rp.instances.send_presence(inst.id, sender, "composing", duration_ms=1500)
        reply = await rp.messages.send_text(inst.id, sender, "Recebi o seu áudio, já te respondo.",
                                            reply_to=ReplyTo.to_user_message(voice_id, "(nota de voz)"))
        await rp.messages.wait(reply.message_id, timeout=30)
        sent = control(node_url, node_key, "GET", f"/_sim/instances/{inst.id}/sent")["sent"][-1]
        if sent["quoted_id"] != voice_id or sent.get("quote_dropped"):
            raise SystemExit(f"the reply must quote the voice note, node saw {sent}")
        wait_for("typing indicator", lambda: control(node_url, node_key, "GET", f"/_sim/instances/{inst.id}/presences")["presences"])
        reads = control(node_url, node_key, "GET", f"/_sim/instances/{inst.id}/reads")["reads"]
        if [r["id"] for r in reads] != [voice_id]:
            raise SystemExit(f"the voice note must be marked as read: {reads}")
        print(f"7. marked the voice note as read, showed typing, replied quoting it (node saw quoted_id={sent['quoted_id']})")

    if bad:
        raise SystemExit(f"{len(bad)} webhook(s) failed signature verification: {bad[:2]}")
    server.shutdown()
    print("quickstart OK")


def _connected(api_key: str, instance_id: str) -> bool:
    r = httpx.get(f"{GATEWAY}/api/v1/instances/{instance_id}", headers={"Authorization": f"Bearer {api_key}"}, timeout=10)
    return r.status_code == 200 and r.json().get("observed_state") == "CONNECTED"


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
