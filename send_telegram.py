#!/usr/bin/env python3
"""Send a text message to a Telegram chat.

Stdlib-only. Reads the message body from a file (avoids shell quoting hell
for multi-line briefs). Designed to be called from a Claude Routine.

Usage:
  python3 send_telegram.py \\
    --text-file /tmp/brief.txt \\
    --bot-token <token> \\
    --chat-id <id>
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--text-file", required=True, help="Path to the message text")
    p.add_argument("--bot-token", required=True)
    p.add_argument("--chat-id", required=True)
    args = p.parse_args()

    text = open(args.text_file, encoding="utf-8").read()
    if not text.strip():
        sys.exit(f"[ERROR] {args.text_file} is empty")

    body = json.dumps({"chat_id": args.chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{args.bot_token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", "replace")[:500]
        sys.exit(f"[ERROR] Telegram HTTP {e.code}: {msg}")

    if not resp.get("ok"):
        sys.exit(f"[ERROR] Telegram not-ok: {resp}")
    print(f"[OK] Telegram message delivered to chat {args.chat_id} ({len(text)} chars)")


if __name__ == "__main__":
    main()
