#!/usr/bin/env python3
"""Deliver a daily Slack brief — sends to Telegram AND saves to Supabase atomically.

This replaces send_telegram.py + save_brief.py so cloud-routine agents can't
skip the save step. Telegram is attempted first; the result (message_id or
error) is captured and stored alongside the brief in Supabase.

Output:
  Single "[OK] delivered" summary line showing both outcomes.

Stdlib-only Python 3.10+.

Usage:
  python3 deliver.py \\
    --client rankzero \\
    --channel-id C0A6B2JUL7K \\
    --channel-name rankzero-prospeqt \\
    --brief-file /tmp/brief.txt \\
    --transcript-file /tmp/transcript.txt \\
    --bot-token <TG_TOKEN> \\
    --chat-id <TG_CHAT> \\
    --supabase-url https://xxx.supabase.co \\
    --supabase-key <SERVICE_ROLE_KEY>
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timezone


# -- parse metadata from transcript ------------------------------------------

TOPLEVEL_RE = re.compile(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}\] ([^:]+):")
REPLY_RE = re.compile(r"^\s+└─ \[\d{2}:\d{2}\] ([^:]+):")
MENTION_RE = re.compile(r"@([A-Za-zŠšČčĆćŽžĐđÁáÉéÍíÓóÚú][\w\-.'ŠšČčĆćŽžĐđÁáÉéÍíÓóÚú ]{0,40})")


def parse_metadata(transcript: str) -> tuple[int, int, list[str], bool]:
    """Returns (message_count, reply_count, users_involved, is_empty)."""
    if not transcript.strip() or "(No messages" in transcript or "(No substantive" in transcript:
        return 0, 0, [], True
    msg_count = 0
    reply_count = 0
    users: set[str] = set()
    for line in transcript.splitlines():
        m = TOPLEVEL_RE.match(line)
        if m:
            msg_count += 1
            users.add(m.group(1).strip())
            continue
        m = REPLY_RE.match(line)
        if m:
            reply_count += 1
            users.add(m.group(1).strip())
            continue
        for u in MENTION_RE.findall(line):
            u = u.strip()
            if re.fullmatch(r"U[A-Z0-9]{8,12}", u):
                continue
            if len(u) >= 2:
                users.add(u)
    return msg_count, reply_count, sorted(users), False


# -- Telegram ----------------------------------------------------------------

def send_telegram(bot_token: str, chat_id: str, text: str) -> tuple[int | None, str | None]:
    """Returns (message_id, error). message_id is None if send failed."""
    body = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
        if resp.get("ok") and resp.get("result", {}).get("message_id"):
            return resp["result"]["message_id"], None
        return None, f"Telegram not-ok: {resp.get('description') or resp}"
    except urllib.error.HTTPError as e:
        return None, f"Telegram HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:200]}"
    except Exception as e:
        return None, f"Telegram exception: {e}"


# -- Supabase ----------------------------------------------------------------

def upsert_brief(url: str, key: str, row: dict) -> dict:
    endpoint = f"{url.rstrip('/')}/rest/v1/client_briefs"
    body = json.dumps(row).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            # Explicit User-Agent: Supabase/Cloudflare blocks Python's default UA
            # from some cloud IP ranges (HTTP 403 Cloudflare error 1010).
            "User-Agent": "rankzero-daily-brief/1.0",
            "Accept": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", "replace")[:500]
        sys.exit(f"[ERROR] Supabase HTTP {e.code}: {msg}")


# -- main --------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--client", required=True)
    p.add_argument("--channel-id", required=True)
    p.add_argument("--channel-name", required=True)
    p.add_argument("--brief-file", required=True)
    p.add_argument("--transcript-file", required=True)
    p.add_argument("--bot-token", required=True)
    p.add_argument("--chat-id", required=True)
    p.add_argument("--supabase-url", required=True)
    p.add_argument("--supabase-key", required=True)
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--brief-date", help="YYYY-MM-DD (default: today, UTC)")
    args = p.parse_args()

    brief = open(args.brief_file, encoding="utf-8").read().strip()
    transcript = open(args.transcript_file, encoding="utf-8").read().strip()
    if not brief:
        sys.exit(f"[ERROR] Brief file {args.brief_file} is empty")

    # 1. Send Telegram first — capture outcome
    msg_id, tg_error = send_telegram(args.bot_token, args.chat_id, brief)
    tg_sent_at = datetime.now(tz=timezone.utc).isoformat() if msg_id else None

    # 2. Parse metadata from transcript
    msg_count, reply_count, users, is_empty = parse_metadata(transcript)

    # 3. Upsert to Supabase (always runs, even if Telegram failed)
    row = {
        "client": args.client,
        "channel_id": args.channel_id,
        "channel_name": args.channel_name,
        "brief_date": args.brief_date or date.today().isoformat(),
        "window_hours": args.hours,
        "brief": brief,
        "transcript": transcript,
        "message_count": msg_count,
        "reply_count": reply_count,
        "users_involved": users,
        "is_empty": is_empty,
        "telegram_message_id": msg_id,
        "telegram_sent_at": tg_sent_at,
        "telegram_error": tg_error,
    }
    resp = upsert_brief(args.supabase_url, args.supabase_key, row)
    row_id = resp[0].get("id") if isinstance(resp, list) and resp else "?"

    # 4. Report outcome
    tg_status = f"message_id={msg_id}" if msg_id else f"FAILED ({tg_error})"
    print(
        f"[OK] delivered client={args.client} supabase_id={row_id} "
        f"telegram={tg_status} messages={msg_count} replies={reply_count} empty={is_empty}"
    )
    # Non-zero exit if Telegram failed but Supabase succeeded (so monitoring can flag)
    if tg_error and msg_id is None:
        sys.exit(2)


if __name__ == "__main__":
    main()
