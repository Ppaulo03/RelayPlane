"""Pick one representative webhook per distinct shape from SANITIZED captures and write them as golden fixtures.

    python tools/spike/curate.py captures/sanitized/webhooks.jsonl internal/adapters/providers/evolution/v2/testdata/real

Only raw provider webhooks are kept (the body the node posted). QR code events are never kept: they carry a credential.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def shape(body: dict) -> str | None:
    ev = str(body.get("event", "")).lower().replace("_", ".")
    d = body.get("data")
    d = d[0] if isinstance(d, list) and d else d
    d = d if isinstance(d, dict) else {}
    if ev == "messages.upsert":
        key = d.get("key", {})
        where = "group" if str(key.get("remoteJid", "")).endswith("@g.us") else "direct"
        quoted = "quoted" if (d.get("contextInfo") or {}).get("stanzaId") or any(
            isinstance(v, dict) and (v.get("contextInfo") or {}).get("stanzaId") for v in (d.get("message") or {}).values()) else "plain"
        return f"upsert-{d.get('messageType')}-{where}-{quoted}"
    if ev == "messages.update":
        return f"update-{d.get('status')}"
    if ev in ("messages.delete", "messages.edited"):
        return ev.replace(".", "-")
    if ev == "connection.update":
        return f"connection-{d.get('state')}-{d.get('statusReason')}"
    if ev == "send.message":
        return "send-message"
    return None  # qrcode.updated and anything else: not kept


def main() -> None:
    src, dst = Path(sys.argv[1]), Path(sys.argv[2])
    dst.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    for line in src.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        body = rec.get("body")
        if rec.get("kind") != "provider" or not isinstance(body, dict):
            continue
        name = shape(body)
        if name is None or name in seen:
            continue
        seen.add(name)
        (dst / f"{name}.json").write_text(json.dumps(body, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {len(seen)} fixtures to {dst}: {sorted(seen)}")


if __name__ == "__main__":
    main()
