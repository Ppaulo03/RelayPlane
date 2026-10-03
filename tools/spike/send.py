"""Send a message from the paired number through RelayPlane.

    sh deploy/docker/spike.sh send 5562999999999 "Confirma amanhã às 15h? Responda CITANDO esta mensagem."
    sh deploy/docker/spike.sh send 5562999999999 --file ./foto.jpg --caption "foto"

Prints the provider message id: that is the id a quoted reply must carry in its ``reply_to`` field.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from relayplane import RelayPlaneClient

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:18080")
STATE = Path(os.environ.get("CAPTURES", "captures")) / "state.json"


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("number", help="destination, digits only with country code (e.g. 5562999999999)")
    ap.add_argument("text", nargs="?", default="")
    ap.add_argument("--file", help="send this file (image/audio/video/document)")
    ap.add_argument("--caption", default="")
    ap.add_argument("--type", default="", help="image|audio|video|document (default: guessed from the file)")
    args = ap.parse_args()
    state = json.loads(STATE.read_text())

    async with RelayPlaneClient(GATEWAY, state["api_key"]) as rp:
        if args.file:
            media = await rp.media.upload(Path(args.file))
            kind = args.type or ("image" if media.content_type.startswith("image/") else "audio" if media.content_type.startswith("audio/")
                                 else "video" if media.content_type.startswith("video/") else "document")
            sent = await rp.messages.send_media(state["instance_id"], args.number, media.id, type=kind, caption=args.caption)
        else:
            sent = await rp.messages.send_text(state["instance_id"], args.number, args.text)
        msg = await rp.messages.wait(sent.message_id, timeout=60)
        print(f"message {msg.id}: {msg.status}")
        print(f"provider message id: {msg.provider_message_id}")
        print(f"accepted at:         {msg.accepted_at}")
        if msg.status in ("FAILED", "UNKNOWN"):
            print(f"error: {msg.error_code} {msg.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
