#!/usr/bin/env python3
"""Fetch a cleaned transcript of a Slack channel's last N hours.

Prints to stdout in the format:
    [YYYY-MM-DD HH:MM] Author: message text
        └─ [HH:MM] Replier: threaded reply

Designed for use inside a Claude Routine that reads the transcript and writes
a morning brief. Stdlib-only Python 3.10+.

Usage:
  python3 fetch_slack_brief.py --token xoxp-... --channel C0A6B2JUL7K --hours 24
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


def slack_get(token: str, method: str, params: dict) -> dict:
    url = f"https://slack.com/api/{method}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    if not data.get("ok"):
        sys.exit(f"[ERROR] Slack {method} failed: {data.get('error')}")
    return data


def fetch_history(token: str, channel: str, oldest: float) -> list[dict]:
    """Fetch all messages from `oldest` to now (paginated).

    Note: Slack's `oldest` param requires an integer timestamp — float
    timestamps with decimals are silently rejected and return empty.
    """
    messages = []
    cursor = None
    while True:
        params = {"channel": channel, "oldest": str(int(oldest)), "limit": "200"}
        if cursor:
            params["cursor"] = cursor
        data = slack_get(token, "conversations.history", params)
        messages.extend(data.get("messages", []))
        if not data.get("has_more"):
            break
        cursor = data.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.3)
    return messages


def fetch_thread_replies(token: str, channel: str, thread_ts: str) -> list[dict]:
    data = slack_get(token, "conversations.replies", {"channel": channel, "ts": thread_ts, "limit": "200"})
    return data.get("messages", [])[1:]  # skip parent (already have it)


def build_user_map(token: str, user_ids: set) -> dict:
    mapping = {}
    for uid in user_ids:
        try:
            data = slack_get(token, "users.info", {"user": uid})
            u = data.get("user", {})
            profile = u.get("profile", {})
            name = profile.get("real_name") or profile.get("display_name") or u.get("name") or uid
            mapping[uid] = name
        except SystemExit:
            mapping[uid] = uid
        time.sleep(0.15)
    return mapping


def resolve_mentions(text: str, users: dict) -> str:
    return re.sub(r"<@(U[A-Z0-9]+)>", lambda m: "@" + users.get(m.group(1), m.group(1)), text)


def clean_text(text: str, users: dict) -> str:
    if not text:
        return ""
    text = resolve_mentions(text, users)
    text = re.sub(r"<(https?://[^|>]+)\|([^>]+)>", r"\2 (\1)", text)
    text = re.sub(r"<(https?://[^>]+)>", r"\1", text)
    text = re.sub(r"<#[A-Z0-9]+\|([^>]+)>", r"#\1", text)
    return text.strip()


def format_transcript(messages: list[dict], users: dict, prefetched_replies: dict) -> str:
    lines = []
    for msg in sorted(messages, key=lambda m: float(m.get("ts", "0"))):
        subtype = msg.get("subtype")
        if subtype in {"channel_join", "channel_leave", "channel_topic", "channel_purpose"}:
            continue
        user_id = msg.get("user") or msg.get("bot_id")
        author = users.get(user_id, msg.get("username") or user_id or "unknown")
        ts = datetime.fromtimestamp(float(msg["ts"]), tz=timezone.utc).astimezone()
        text = clean_text(msg.get("text", ""), users)
        for att in msg.get("attachments", []) or []:
            at = att.get("text") or att.get("fallback") or ""
            if at:
                text += f"\n    [attachment] {clean_text(at, users)[:500]}"
        for f in msg.get("files", []) or []:
            text += f"\n    [file] {f.get('name', '?')} ({f.get('filetype', '?')})"
        if not text:
            continue
        lines.append(f"[{ts:%Y-%m-%d %H:%M}] {author}: {text}")

        if msg.get("thread_ts") == msg.get("ts") and msg.get("reply_count", 0) > 0:
            for r in prefetched_replies.get(msg["ts"], []):
                r_user = r.get("user") or r.get("bot_id")
                r_author = users.get(r_user, r.get("username") or r_user or "unknown")
                r_ts = datetime.fromtimestamp(float(r["ts"]), tz=timezone.utc).astimezone()
                r_text = clean_text(r.get("text", ""), users)
                if r_text:
                    lines.append(f"    └─ [{r_ts:%H:%M}] {r_author}: {r_text}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True, help="Slack user OAuth token (xoxp-...)")
    parser.add_argument("--channel", required=True, help="Channel ID (e.g. C0A6B2JUL7K)")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()

    oldest = time.time() - args.hours * 3600
    messages = fetch_history(args.token, args.channel, oldest)

    if not messages:
        print(f"(No messages in last {args.hours}h)")
        return

    all_replies = {}
    for m in messages:
        if m.get("thread_ts") == m.get("ts") and m.get("reply_count", 0) > 0:
            try:
                all_replies[m["ts"]] = fetch_thread_replies(args.token, args.channel, m["ts"])
                time.sleep(0.2)
            except SystemExit:
                all_replies[m["ts"]] = []

    user_ids = {m["user"] for m in messages if m.get("user")}
    for reps in all_replies.values():
        for r in reps:
            if r.get("user"):
                user_ids.add(r["user"])
    users = build_user_map(args.token, user_ids)

    transcript = format_transcript(messages, users, all_replies)
    print(transcript or f"(No substantive messages in last {args.hours}h)")


if __name__ == "__main__":
    main()
