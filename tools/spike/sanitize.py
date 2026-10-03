"""Turn the raw captures of the real-number spike into fixtures that are safe to share and to commit.

    sh deploy/docker/spike.sh sanitize        # reads captures/webhooks.jsonl, writes captures/sanitized/webhooks.jsonl

The STRUCTURE is preserved exactly (that is what the fixtures are for); the CONTENT is replaced:
  * phone numbers and JIDs                -> stable fake numbers (the same real number always maps to the same fake one)
  * message ids / quoted ids / receipts   -> stable placeholders, so a quoted reply still points at the message it quotes
  * names, message text, captions         -> placeholders
  * urls, base64 blobs, media keys/hashes -> placeholders that keep only the kind and length
Timestamps, event names, key names, types and statuses are kept. Review the output before committing it.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

CAPTURES = Path(os.environ.get("CAPTURES", "captures"))

ID_KEYS = {"id", "keyId", "messageId", "stanzaId", "quotedStanzaId", "remoteJid", "participant", "ownerJid", "sender", "number"}
# key material and device metadata: dropped whole (they are byte arrays / objects, not strings)
DROP_KEYS = {"messageSecret", "deviceListMetadata", "senderKeyHash", "recipientKeyHash", "senderTimestamp", "recipientTimestamp"}
TEXT_KEYS = {"profileName", "conversation", "text", "caption", "pushName", "fileName", "title", "description", "body", "name", "displayName", "contentText"}
SECRET_KEYS = {"mediaKey", "fileSha256", "fileEncSha256", "jpegThumbnail", "thumbnailDirectPath", "thumbnailSha256", "thumbnailEncSha256",
               "waveform", "streamingSidecar", "midQualityFileSha256", "apikey", "token", "hash"}
URL_KEYS = {"url", "directPath", "mediaUrl", "base64", "webhook", "server_url"}
PHONE = re.compile(r"(?<!\d)(\d{10,15})(?!\d)")
KEEP_VALUES = {"fromMe", "status", "state", "event", "messageType", "mimetype", "type"}  # structural, never replaced


class Mapper:
    def __init__(self) -> None:
        self.maps: dict[str, dict[str, str]] = {}

    def stable(self, kind: str, real: str, make) -> str:
        table = self.maps.setdefault(kind, {})
        if real not in table:
            table[real] = make(len(table) + 1)
        return table[real]

    def phone(self, digits: str) -> str:
        return self.stable("phone", digits, lambda n: "55119" + f"{n:08d}")

    def text(self, real: str) -> str:
        return self.stable("text", real, lambda n: f"[texto {n}]")

    def ident(self, real: str) -> str:
        # keep the shape of the id (WhatsApp ids are upper-case hex of a fixed length) so parsers see realistic data
        def make(n: int) -> str:
            base = f"3EB0FAKE{n:010d}"
            return (base + "0" * len(real))[: max(len(real), len(base))]
        return self.stable("id", real, make)

    def jid(self, real: str) -> str:
        if "@" in real:
            local, domain = real.split("@", 1)
            local, _, device = local.partition(":")
            if domain == "g.us":
                return self.stable("group", real, lambda n: f"1203630000000000{n:02d}@g.us")
            fake = self.phone(local) if local.isdigit() else self.ident(local)
            return fake + (f":{device}" if device else "") + "@" + domain
        return self.phone(real) if real.isdigit() else self.ident(real)


def clean(value, key: str, m: Mapper):
    if key in DROP_KEYS or (key in SECRET_KEYS and isinstance(value, (dict, list))):
        return "<redacted>"  # byte arrays serialised as objects/lists: keys, hashes, thumbnails, waveforms
    if isinstance(value, dict):
        return {k: clean(v, k, m) for k, v in value.items()}
    if isinstance(value, list):
        return [clean(v, key, m) for v in value]
    if not isinstance(value, str):
        return value
    if value == "" or key in KEEP_VALUES:
        return value  # empty strings are structure (e.g. participant of a direct chat)
    if key in SECRET_KEYS:
        return f"<redacted:{key}:{len(value)}>"
    if key in URL_KEYS or value.startswith(("http://", "https://", "data:")):
        return f"<{key}:{len(value)}>" if key == "base64" or key == "directPath" else f"https://media.example.invalid/{m.stable('url', value, lambda n: str(n))}"
    if key in ID_KEYS:
        return m.jid(value) if ("@" in value or value.isdigit()) else m.ident(value)
    if key in TEXT_KEYS:
        return m.text(value)
    if len(value) > 200:
        return f"<blob:{len(value)}>"
    return PHONE.sub(lambda mt: m.phone(mt.group(1)), value)


def main() -> None:
    src = CAPTURES / "webhooks.jsonl"
    if not src.exists():
        raise SystemExit(f"{src} not found")
    m = Mapper()
    out_dir = CAPTURES / "sanitized"
    out_dir.mkdir(parents=True, exist_ok=True)
    lines = []
    for raw in src.read_text(encoding="utf-8").splitlines():
        if raw.strip():
            rec = json.loads(raw)
            lines.append(json.dumps(clean(rec, "", m), ensure_ascii=False))
    (out_dir / "webhooks.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    report = {k: len(v) for k, v in m.maps.items()}
    print(f"wrote {out_dir / 'webhooks.jsonl'} ({len(lines)} records); replaced distinct values: {report}")
    print("Review it before sharing or committing: sanitizing is heuristic.")


if __name__ == "__main__":
    main()
