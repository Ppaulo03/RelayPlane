"""Pair a real WhatsApp number with the spike stack.

    sh deploy/docker/spike.sh pair

Creates (once) a tenant and a webhook subscription that points at the capture tap, creates an instance, saves the QR code as
captures/qr.png (and tries to open it), refreshes it while it is not scanned, and waits until the instance is CONNECTED.
Scan it from the phone: WhatsApp > Linked devices > Link a device.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import sys
import time
from pathlib import Path

import httpx

from relayplane import NotFound, RelayPlaneClient, RelayPlaneError

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:18080")
ADMIN_KEY = os.environ["ADMIN_API_KEY"]
CAPTURES = Path(os.environ.get("CAPTURES", "captures"))
STATE = CAPTURES / "state.json"
SINK_URL = os.environ.get("SINK_URL", "http://webhooktap:9000/sink")


def load_state() -> dict:
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def save_state(state: dict) -> None:
    CAPTURES.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2))


def save_qr(qr: str) -> Path | None:
    """The node returns a data URL (PNG) for the QR code; save it so it can be scanned from a screen."""
    m = re.match(r"data:image/\w+;base64,(.*)", qr or "", re.S)
    if not m:
        return None
    path = CAPTURES / "qr.png"
    path.write_bytes(base64.b64decode(m.group(1)))
    return path


def show(path: Path) -> None:
    if os.environ.get("NO_OPEN"):
        return
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)  # noqa: S606
        elif sys.platform == "darwin":
            os.system(f'open "{path}"')  # noqa: S605
    except Exception:  # noqa: BLE001 - opening the image is a convenience only
        pass


async def main() -> None:
    state = load_state()
    if not state.get("api_key"):
        r = httpx.post(f"{GATEWAY}/api/v1/tenants", headers={"Authorization": f"Bearer {ADMIN_KEY}"}, json={"name": "spike"}, timeout=15)
        r.raise_for_status()
        state["api_key"] = r.json()["api_key"]
        save_state(state)
        print("tenant created")

    async with RelayPlaneClient(GATEWAY, state["api_key"]) as rp:
        sub = await rp.subscriptions.create(SINK_URL, idempotency_key="spike-sink")
        print(f"subscription {sub.id} -> {SINK_URL} (normalized events are captured too)")

        inst = await rp.instances.create("spike", idempotency_key="spike-instance")
        state["instance_id"] = inst.id
        save_state(state)
        print(f"instance {inst.id}: {inst.status}")

        deadline = time.monotonic() + 300
        shown = False
        while time.monotonic() < deadline:
            current = await rp.instances.get(inst.id)
            if current.observed_state == "CONNECTED":
                print("CONNECTED: the number is linked. You can now run: sh deploy/docker/spike.sh send <number> \"hello\"")
                return
            try:
                pairing = await rp.instances.get_qrcode(inst.id)
                path = save_qr(pairing.qrcode or "")
                if path and not shown:
                    print(f"QR saved to {path} (opening it). WhatsApp > Linked devices > Link a device. It refreshes every few seconds.")
                    show(path)
                    shown = True
                if pairing.pairing_code:
                    print(f"pairing code (alternative to the QR): {pairing.pairing_code}")
            except (NotFound, RelayPlaneError) as exc:
                print(f"waiting for the node ({exc.__class__.__name__})...")
            await asyncio.sleep(8)
    raise SystemExit("timed out waiting for the QR code to be scanned")


if __name__ == "__main__":
    asyncio.run(main())
