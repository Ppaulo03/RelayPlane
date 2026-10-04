"""What must be true of a freshly started staging, checked against the real stack (no WhatsApp number needed).

    sh deploy/staging/staging.sh smoke

It runs the production configuration with the real Evolution node, so it proves what the sandbox cannot: the images of a release
start together in production mode, migrations apply, the production guards are on, and the real Evolution node answers through the
adapter (a QR code comes back). Pairing a phone and the messaging behaviour are covered by docs/REAL-NUMBER-SPIKE.md.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx

from relayplane import RelayPlaneClient, RelayPlaneError

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:18080")
ADMIN = os.environ["ADMIN_API_KEY"]

failures: list[str] = []


def check(ok: bool, what: str, detail: str = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'} {what}{(' -- ' + detail) if detail and not ok else ''}")
    if not ok:
        failures.append(what)


async def main() -> None:
    print(f"staging smoke against {GATEWAY}")

    # 1. the process is up and ready (Postgres, Redis, the command queue and the object store all answered)
    r = httpx.get(f"{GATEWAY}/health/ready", timeout=10)
    check(r.status_code == 200, "gateway is ready (every dependency answered)", f"{r.status_code} {r.text[:200]}")
    check(httpx.get(f"{GATEWAY}/health/live", timeout=10).status_code == 200, "gateway is live")
    metrics = httpx.get(f"{GATEWAY}/metrics", timeout=10)
    check(metrics.status_code == 200 and "relayplane_" in metrics.text, "metrics are exposed")

    # 2. the tenant API works with the admin key issuing the first credential
    t = httpx.post(f"{GATEWAY}/api/v1/tenants", headers={"Authorization": f"Bearer {ADMIN}"}, json={"name": f"smoke-{int(time.time())}"}, timeout=15)
    check(t.status_code == 201, "the admin creates a tenant", t.text[:200])
    if t.status_code != 201:
        return
    key = t.json()["api_key"]
    check(httpx.get(f"{GATEWAY}/api/v1/limits", timeout=10).status_code == 401, "the API refuses a request without credentials")

    # the reconciler probes the provider nodes before it places anything on them: wait for them to be READY, with the version
    # the adapter was validated against (an untested Evolution version is refused on purpose)
    ready = []
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        nodes = httpx.get(f"{GATEWAY}/api/v1/nodes", headers={"Authorization": f"Bearer {ADMIN}"}, timeout=10).json().get("nodes", [])
        ready = [n for n in nodes if n.get("status") == "READY"]
        if len(ready) >= 2:
            break
        time.sleep(3)
    check(len(ready) >= 2, "both Evolution nodes are READY", str(ready))
    check(bool(ready) and all(n.get("provider_version") == "2.3.7" for n in ready), "and run the Evolution version the adapter was validated against (2.3.7)",
          str([n.get("provider_version") for n in ready]))

    async with RelayPlaneClient(GATEWAY, key) as rp:
        limits = await rp.limits.get()
        check(limits.idempotency_retention_seconds > 0 and limits.max_text_length > 0, "limits are advertised")

        # 3. production guards: a tenant cannot point webhooks at insecure or private destinations
        for url in ("http://agent.example.com/hook", "https://127.0.0.1/hook", "https://10.0.0.5/hook", "https://169.254.169.254/latest"):
            try:
                await rp.subscriptions.create(url)
                check(False, f"production refuses the webhook destination {url}")
            except RelayPlaneError:
                check(True, f"production refuses the webhook destination {url}")

        # 4. credentials rotate without downtime, and the last key is protected
        second = await rp.api_keys.create("smoke-rotation")
        keys = await rp.api_keys.list()
        first = next(k for k in keys if k.current)
        async with RelayPlaneClient(GATEWAY, second.secret) as rp2:
            await rp2.api_keys.revoke(first.id)
            check(httpx.get(f"{GATEWAY}/api/v1/limits", headers={"Authorization": f"Bearer {key}"}, timeout=10).status_code == 401,
                  "a revoked API key stops working at once")
            try:
                await rp2.api_keys.revoke(second.id)
                check(False, "the last API key cannot be revoked")
            except RelayPlaneError:
                check(True, "the last API key cannot be revoked")

            # 5. the real Evolution node, through the adapter: an instance comes up and asks to be paired
            inst = await rp2.instances.create("staging-smoke", idempotency_key=f"smoke-{int(time.time())}")
            qr = None
            deadline = time.monotonic() + 150
            while time.monotonic() < deadline and not qr:
                try:
                    pairing = await rp2.instances.get_qrcode(inst.id)
                    qr = pairing.qrcode
                except RelayPlaneError:
                    pass
                if not qr:
                    await asyncio.sleep(3)
            check(bool(qr) and qr.startswith("data:image/"), "the real Evolution node produced a QR code for the new instance")
            current = await rp2.instances.get(inst.id)
            check(current.observed_state == "AWAITING_PAIRING", "the instance waits for pairing", current.observed_state)
            try:
                await rp2.instances.logout(inst.id)
            except RelayPlaneError:
                pass

    print()
    if failures:
        print(f"{len(failures)} check(s) failed: {failures}")
        sys.exit(1)
    print("staging smoke OK")


if __name__ == "__main__":
    asyncio.run(main())
