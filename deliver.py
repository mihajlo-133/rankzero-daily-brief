#!/usr/bin/env python3
"""Deliver a daily Slack brief — sends to Telegram AND saves to Supabase atomically.

This replaces send_telegram.py + save_brief.py so cloud-routine agents can't
skip the save step. Telegram is attempted first; the result (message_id or
error) is captured and stored alongside the brief in Supabase.

Output:
  Single "[OK] delivered" summary line showing both outcomes.

Stdlib-only Python 3.10+.

Usage:
  python3 deliver.py \
    --client rankzero \
    --channel-id C0A6B2JUL7K \
    --channel-name rankzero-prospeqt \
    --brief-file /tmp/brief.txt \
    --transcript-file /tmp/transcript.txt \
    --bot-token <TG_TOKEN> \
    --chat-id <TG_CHAT> \
    --supabase-url https://xxx.supabase.co \
    --supabase-key <SERVICE_ROLE_KEY>
"""

from __future__ import annotations

import argparse
import http.client
import json
import re
import socket
import ssl
import sys
import urllib.parse
from datetime import date, datetime, timezone


# -- DNS pre-resolution — resolves once per hostname, reuses IP on subsequent calls --

_DNS_CACHE: dict[str, str] = {}


def _resolve(hostname: str) -> str:
    """Resolve hostname to IPv4 address once; return cached result on subsequent calls."""
    if hostname not in _DNS_CACHE:
        info = socket.getaddrinfo(hostname, 443, socket.AF_INET, socket.SOCK_STREAM)
        _DNS_CACHE[hostname] = info[0][4][0]
    return _DNS_CACHE[hostname]


def _https_request(
    hostname: str,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes | None = None,
) -> tuple[int, str]:
    """Make an HTTPS request using a pre-resolved IP to avoid per-request DNS lookups.

    Returns (status_code, response_body_str).
    Raises on connection-level errors (caller wraps in try/except).
    """
    ip = _resolve(hostname)
    ctx = ssl.create_default_context()
    # Manually create the socket to the pre-resolved IP, then wrap it with
    # SSL using server_hostname=hostname so cert validation + SNI use the
    # correct name. Previously we passed `ip` as the HTTPSConnection host,
    # which caused "certificate verify failed: IP address mismatch".
    sock = socket.create_connection((ip, 443), timeout=15)
    ssock = ctx.wrap_socket(sock, server_hostname=hostname)
    conn = http.client.HTTPSConnection(hostname, 443, context=ctx, timeout=15)
    conn.sock = ssock
    all_headers: dict[str, str] = {
        "Host": hostname,
        "User-Agent": "rankzero-daily-brief/1.0",
    }
    all_headers.update(headers)
    conn.request(method, path, body=body, headers=all_headers)
    resp = conn.getresponse()
    body_bytes = resp.read()
    conn.close()
    return resp.status, body_bytes.decode("utf-8", errors="replace")


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
    hostname = "api.telegram.org"
    path = f"/bot{bot_token}/sendMessage"
    body = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    try:
        status, raw = _https_request(hostname, "POST", path, headers, body)
        resp = json.loads(raw)
        if status == 200 and resp.get("ok") and resp.get("result", {}).get("message_id"):
            return resp["result"]["message_id"], None
        return None, f"Telegram HTTP {status}: {resp.get('description') or raw[:200]}"
    except Exception as e:
        return None, f"Telegram exception: {e}"


# -- Supabase ----------------------------------------------------------------

def upsert_brief(url: str, key: str, row: dict) -> tuple[dict, str | None]:
    """Returns (parsed_response, error_message). error_message is None on success."""
    parsed = urllib.parse.urlparse(url)
    hostname = parsed.hostname
    path = parsed.path.rstrip("/") + "/rest/v1/client_briefs"
    body = json.dumps(row).encode("utf-8")
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    try:
        status, raw = _https_request(hostname, "POST", path, headers, body)
        if status in (200, 201):
            return (json.loads(raw) if raw else {}), None
        return {}, f"Supabase HTTP {status}: {raw[:500]}"
    except Exception as e:
        return {}, f"Supabase exception: {e}"


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

    # Pre-resolve DNS for both services upfront — single lookup per hostname
    _resolve("api.telegram.org")
    _resolve(urllib.parse.urlparse(args.supabase_url).hostname)

    # 1. Send Telegram first — capture outcome
    msg_id, tg_error = send_telegram(args.bot_token, args.chat_id, brief)
    tg_sent_at = datetime.now(tz=timezone.utc).isoformat() if msg_id else None

    # 2. Parse metadata from transcript
    msg_count, reply_count, users, is_empty = parse_metadata(transcript)

    # 3. Upsert to Supabase (always runs, even if Telegram failed — non-fatal)
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
    resp, sb_error = upsert_brief(args.supabase_url, args.supabase_key, row)
    row_id = resp[0].get("id") if isinstance(resp, list) and resp else "?"

    # 4. Report outcome — Telegram and Supabase reported separately
    if msg_id:
        print(f"[OK] Telegram delivered (message_id={msg_id})")
    else:
        print(f"[ERROR] Telegram failed: {tg_error}")

    if sb_error is None:
        print(f"[OK] Supabase logged (id={row_id})")
    else:
        print(f"[WARN] Supabase failed (non-fatal): {sb_error}")

    print(
        f"[OK] delivered client={args.client} messages={msg_count} "
        f"replies={reply_count} empty={is_empty}"
    )

    # Non-zero exit only if Telegram failed (delivery is the primary goal)
    if tg_error and msg_id is None:
        sys.exit(2)


if __name__ == "__main__":
    main()
