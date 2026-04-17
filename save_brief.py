#!/usr/bin/env python3
"""Save a daily Slack brief to Supabase.

Takes brief + transcript text, extracts metadata (message count, reply count,
unique users involved), and upserts into the `client_briefs` table. Idempotent
on (client, brief_date) — re-runs UPDATE the existing row instead of erroring.

Stdlib-only. Designed to be called from a Claude Routine after the brief is
delivered to Telegram.

Usage:
  python3 save_brief.py \\
    --client rankzero \\
    --channel-id C0A6B2JUL7K \\
    --channel-name rankzero-prospeqt \\
    --brief-file /tmp/brief.txt \\
    --transcript-file /tmp/transcript.txt \\
    --supabase-url https://xxx.supabase.co \\
    --supabase-key sb_secret_... \\
    [--hours 24]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from datetime import date


# Patterns matching the output of fetch_slack_brief.py
TOPLEVEL_RE = re.compile(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}\] ([^:]+):")
REPLY_RE = re.compile(r"^\s+└─ \[\d{2}:\d{2}\] ([^:]+):")
MENTION_RE = re.compile(r"@([A-Za-zŠšČčĆćŽžĐđÁáÉéÍíÓóÚú][\w\-.'ŠšČčĆćŽžĐđÁáÉéÍíÓóÚú ]{0,40})")


def parse_metadata(transcript: str) -> tuple[int, int, list[str], bool]:
    """Return (message_count, reply_count, users_involved, is_empty)."""
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
        # Also catch mentioned users inside message text
        for u in MENTION_RE.findall(line):
            u = u.strip()
            # Filter raw IDs like "U09BF2GP6LW" — only 10-11 char all-caps-digits
            if re.fullmatch(r"U[A-Z0-9]{8,12}", u):
                continue
            # Keep anything plausibly a name
            if len(u) >= 2:
                users.add(u)

    return msg_count, reply_count, sorted(users), False


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
            # Upsert: if (client, brief_date) unique constraint hits, merge instead of erroring
            "Prefer": "resolution=merge-duplicates,return=representation",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", "replace")[:500]
        sys.exit(f"[ERROR] Supabase HTTP {e.code}: {msg}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--client", required=True, help="Client slug, e.g. 'rankzero'")
    p.add_argument("--channel-id", required=True)
    p.add_argument("--channel-name", required=True)
    p.add_argument("--brief-file", required=True, help="Path to the synthesized brief text")
    p.add_argument("--transcript-file", required=True, help="Path to the raw cleaned transcript")
    p.add_argument("--supabase-url", required=True)
    p.add_argument("--supabase-key", required=True, help="service_role secret key")
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--brief-date", help="YYYY-MM-DD (default: today, UTC)")
    args = p.parse_args()

    brief = open(args.brief_file, encoding="utf-8").read().strip()
    transcript = open(args.transcript_file, encoding="utf-8").read().strip()
    if not brief:
        sys.exit("[ERROR] Brief file is empty")

    msg_count, reply_count, users, is_empty = parse_metadata(transcript)

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
    }

    resp = upsert_brief(args.supabase_url, args.supabase_key, row)
    if isinstance(resp, list) and resp:
        print(f"[OK] Saved brief id={resp[0].get('id')} client={args.client} date={row['brief_date']} "
              f"messages={msg_count} replies={reply_count} users={len(users)} empty={is_empty}")
    else:
        print(f"[OK] Saved brief client={args.client} date={row['brief_date']} "
              f"messages={msg_count} replies={reply_count} users={len(users)} empty={is_empty}")


if __name__ == "__main__":
    main()
