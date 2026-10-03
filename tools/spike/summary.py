"""Analyse the captures of the real-number spike against the assumptions RelayPlane and the conversation agent rely on.

    sh deploy/docker/spike.sh summary

Reads captures/webhooks.jsonl (raw provider webhooks + the normalized events RelayPlane delivered) and prints, for each assumption,
what the real node actually did. The report contains no message text; it is safe to paste into a chat.
"""
from __future__ import annotations

import json
import os
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

CAPTURES = Path(os.environ.get("CAPTURES", "captures"))
MEDIA_KEYS = ("imageMessage", "videoMessage", "audioMessage", "documentMessage", "stickerMessage", "ptvMessage")


def load() -> list[dict]:
    path = CAPTURES / "webhooks.jsonl"
    if not path.exists():
        raise SystemExit(f"{path} not found: run the stack and exchange some messages first")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def walk(obj, path=""):
    """Yield (path, key, value) for every key of a JSON document."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            yield p, k, v
            yield from walk(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from walk(v, f"{path}[{i}]")


def items(data) -> list[dict]:
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    return [data] if isinstance(data, dict) else []


def parse_time(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main() -> None:
    recs = load()
    out: list[str] = []
    p = out.append

    provider = [r for r in recs if r["kind"] == "provider" and isinstance(r.get("body"), dict)]
    sink = [r for r in recs if r["kind"] == "sink" and isinstance(r.get("body"), dict)]
    p("== EVENTS the node sent to RelayPlane (raw) ==")
    for ev, n in Counter(r["body"].get("event", "?") for r in provider).most_common():
        p(f"  {ev}: {n}")
    p("== EVENTS RelayPlane delivered to the consumer webhook (normalized) ==")
    for ev, n in Counter(r["body"].get("event_type", "?") for r in sink).most_common():
        p(f"  {ev}: {n}")
    bad = [r for r in provider if r.get("forward_status", 200) >= 400]
    p(f"== gateway answers to the node: {len(provider) - len(bad)} ok, {len(bad)} rejected ==")
    for r in bad[:5]:
        p(f"  {r['body'].get('event')} -> HTTP {r.get('forward_status')} (check the token/epoch/ownership)")

    normalized = {}
    for r in sink:
        pl = r["body"].get("payload") or {}
        if r["body"].get("event_type") == "message.received":
            normalized[pl.get("provider_message_id")] = r["body"]

    # ---- A1/A2/A3: messages.upsert
    p("\n== A1/A2: messages.upsert (what the user sent) ==")
    deltas: list[float] = []
    quoted_paths = Counter()
    reply_ok = reply_miss = replies = 0
    media_seen: dict[str, set] = defaultdict(set)
    for r in provider:
        b = r["body"]
        if str(b.get("event", "")).lower().replace("_", ".") != "messages.upsert":
            continue
        at = parse_time(r["at"])
        for d in items(b.get("data")):
            key = d.get("key", {}) or {}
            if key.get("fromMe"):
                p(f"  (own message echoed: {d.get('messageType')})")
                continue
            mtype = d.get("messageType")
            grp = str(key.get("remoteJid", "")).endswith("@g.us")
            ts = d.get("messageTimestamp")
            line = f"  type={mtype} group={grp} keys={sorted((d.get('message') or {}).keys())}"
            if ts is not None:
                try:
                    t = float(ts)
                    unit = "ms" if t > 1e11 else "s"
                    delta = at.timestamp() - (t / 1000 if unit == "ms" else t)
                    deltas.append(delta)
                    line += f" messageTimestamp={ts!r}({type(ts).__name__},{unit}) arrived {delta:+.1f}s after it"
                except (TypeError, ValueError):
                    line += f" messageTimestamp={ts!r} (not numeric)"
            else:
                line += " messageTimestamp=MISSING"
            p(line)
            found = [(pp, v) for pp, k, v in walk(d) if k == "stanzaId"]
            for pp, v in found:
                quoted_paths[pp] += 1
            if found:
                replies += 1
                norm = normalized.get(key.get("id"))
                extracted = (norm or {}).get("payload", {}).get("reply_to_provider_message_id")
                want = found[0][1]
                if extracted == want:
                    reply_ok += 1
                    p(f"    quoted reply: stanzaId at {found[0][0]} -> RelayPlane delivered reply_to == it  OK")
                else:
                    reply_miss += 1
                    p(f"    quoted reply: stanzaId at {found[0][0]} but RelayPlane delivered reply_to={extracted!r}  MISMATCH")
            for mk in MEDIA_KEYS:
                body = (d.get("message") or {}).get(mk)
                if isinstance(body, dict):
                    media_seen[mk].update(body.keys())
            if d.get("base64"):
                media_seen["(data.base64)"].add("present")

    p("\n== A1 verdict: where does the quoted message id live? ==")
    if quoted_paths:
        for pp, n in quoted_paths.most_common():
            p(f"  {pp}  x{n}")
        p(f"  RelayPlane extracted reply_to correctly for {reply_ok} of {replies} replies; wrong for {reply_miss}")
    else:
        p("  no quoted reply captured yet: answer one of our messages by QUOTING it (long-press > Reply) and run this again")

    p("\n== A2 verdict: messageTimestamp ==")
    if deltas:
        p(f"  {len(deltas)} messages; arrival delay vs messageTimestamp: median {statistics.median(deltas):+.1f}s, min {min(deltas):+.1f}s, max {max(deltas):+.1f}s")
        p("  (a delay near 0 and positive means the stamp is close to the real send time; a large or negative value means the phone clock/offline delivery matters)")
    else:
        p("  no messages captured")

    p("\n== A3: media payloads (keys only) ==")
    if media_seen:
        for mk, keys in media_seen.items():
            p(f"  {mk}: {sorted(keys)}")
        p("  To download the file RelayPlane will need one of: a url, or directPath+mediaKey, or base64. Check which of these are present above.")
    else:
        p("  no media captured: send an image, a voice note and a document to the number")

    # ---- A4/A5
    p("\n== A4: messages.update (receipts of OUR messages) ==")
    st = Counter()
    for r in provider:
        if str(r["body"].get("event", "")).lower().replace("_", ".") == "messages.update":
            for d in items(r["body"].get("data")):
                st[(d.get("status"), tuple(sorted(k for k in d.keys() if k in ("keyId", "messageId", "status"))))] += 1
    for (status, keys), n in st.most_common():
        p(f"  status={status!r} keys={keys} x{n}")
    if not st:
        p("  none captured: send a message from RelayPlane and read it on the phone")

    p("\n== A5: connection.update ==")
    for r in provider:
        if str(r["body"].get("event", "")).lower().replace("_", ".") == "connection.update":
            d = r["body"].get("data") or {}
            p(f"  state={d.get('state')!r} statusReason={d.get('statusReason')!r}")

    p("\n== Other raw event types worth knowing about ==")
    known = {"messages.upsert", "messages.update", "connection.update", "qrcode.updated"}
    for ev in sorted({str(r["body"].get("event", "")).lower().replace("_", ".") for r in provider} - known):
        p(f"  {ev}")

    text = "\n".join(out)
    print(text)
    (CAPTURES / "summary.txt").write_text(text + "\n", encoding="utf-8")
    print(f"\n(saved to {CAPTURES / 'summary.txt'})")


if __name__ == "__main__":
    main()
