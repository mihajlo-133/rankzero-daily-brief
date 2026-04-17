# Rank Zero — Daily Slack Brief

Fetches the last N hours of messages from the #rankzero-prospeqt Slack channel,
resolves user IDs to names, and prints a clean transcript to stdout.

Designed to be run inside a Claude Routine that reads the transcript and
writes a morning brief, then sends it to Telegram.

Stdlib-only Python 3.10+. No dependencies.

## Usage

```bash
python3 fetch_slack_brief.py \
  --token xoxp-... \
  --channel C0A6B2JUL7K \
  --hours 24
```

Arguments:
- `--token` — Slack user OAuth token (needs `channels:history`, `channels:read`, `groups:history`, `groups:read`)
- `--channel` — Channel ID (not name). Get via `conversations.list` API.
- `--hours` — Lookback window. Default: 24.

Output: cleaned transcript on stdout. `(No messages in last Nh)` if empty.

## Claude Routine

Example prompt:

```
Run this in a SINGLE Bash call:

python3 fetch_slack_brief.py --token 'xoxp-...' --channel 'C0A6B2JUL7K' --hours 24

Based on the transcript output:
1. Write a morning brief in 4 sections: WHAT HAPPENED / QUESTIONS / ACTION ITEMS / NOISE
2. Use names from the transcript. Be factual. No fluff.
3. If the transcript says "(No messages...)", the brief is just "No notable activity in last 24h."

Then send the brief to Telegram in a SINGLE curl call:

curl -s -X POST "https://api.telegram.org/bot{TOKEN}/sendMessage" \
  -H "Content-Type: application/json" \
  --data "$(jq -n --arg chat '{CHAT_ID}' --arg text 'BRIEF_HERE' '{chat_id:$chat, text:$text}')"
```
