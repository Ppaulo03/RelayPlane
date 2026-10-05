"""Measure how long a burst of messages takes to be accepted and delivered, message by message.

    python tools/spike/latency.py --state captures/state.json 5562999999999 -n 6

Sends N short texts back to back from the paired number (through the RelayPlane API, so the gateway's own pacing applies) and polls every
message until it is DELIVERED/READ or FAILED. For each one it prints when it was accepted by the provider and when the recipient's
device confirmed it, counted from the moment the burst started, and the gap to the previous message. A node that stalls between
messages (for instance on a query WhatsApp never answers) shows up as gaps of a fixed, large size (60 s is Baileys' default query timeout).

The texts carry no personal data. Use a number you own as the destination.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from relayplane import RelayPlaneClient

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:18080")
DONE = ("DELIVERED", "READ", "FAILED", "UNKNOWN")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("number", help="destination, digits only with country code")
    ap.add_argument("-n", type=int, default=6, help="how many messages (default 6)")
    ap.add_argument("--state", default="captures/state.json", help="json with api_key and instance_id")
    ap.add_argument("--timeout", type=float, default=240.0, help="seconds to wait for the last message (default 240)")
    ap.add_argument("--label", default="", help="free text printed in the header (e.g. the image under test)")
    args = ap.parse_args()
    state = json.loads(Path(args.state).read_text())

    async with RelayPlaneClient(GATEWAY, state["api_key"]) as rp:
        t0 = time.monotonic()
        ids: list[str] = []
        sent_at: list[float] = []
        for i in range(1, args.n + 1):
            r = await rp.messages.send_text(state["instance_id"], args.number, f"teste de latencia {i}/{args.n}")
            ids.append(r.message_id)
            sent_at.append(time.monotonic() - t0)
        accepted: dict[str, float] = {}
        delivered: dict[str, float] = {}
        final: dict[str, str] = {}
        deadline = t0 + args.timeout
        while len(final) < len(ids) and time.monotonic() < deadline:
            for mid in ids:
                if mid in final:
                    continue
                m = await rp.messages.get(mid)
                now = time.monotonic() - t0
                if m.status in ("ACCEPTED", "DELIVERED", "READ") and mid not in accepted:
                    accepted[mid] = now
                if m.status in DONE:
                    delivered[mid] = now
                    final[mid] = m.status
            await asyncio.sleep(0.5)

    print(f"burst of {args.n} messages {('(' + args.label + ')') if args.label else ''}".rstrip())
    print(f"{'#':>2} {'api call':>9} {'accepted':>9} {'delivered':>10} {'gap':>7}  status")
    prev = 0.0
    worst = 0.0
    for i, mid in enumerate(ids, 1):
        a = accepted.get(mid)
        d = delivered.get(mid)
        gap = (d - prev) if d is not None else None
        if d is not None:
            prev = d
            worst = max(worst, gap or 0.0)
        fmt = lambda v: f"{v:8.1f}s" if v is not None else "      n/a"
        print(f"{i:>2} {sent_at[i-1]:8.1f}s {fmt(a)} {fmt(d)} {('%6.1fs' % gap) if gap is not None else '   n/a'}  {final.get(mid, 'NOT DONE')}")
    stalled = [i for i, mid in enumerate(ids, 1) if mid not in final]
    print(f"largest gap between two deliveries: {worst:.1f}s" + (f"; not finished: {stalled}" if stalled else ""))


if __name__ == "__main__":
    asyncio.run(main())
