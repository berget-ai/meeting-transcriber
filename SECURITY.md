# Security notes — meeting-transcriber

Threat model and abuse analysis for the transcription bot, and the controls
that mitigate each vector. Read this before changing the invite policy, the
E2EE key handling, or the summary pipeline.

## Assets

- `MATRIX_ACCESS_TOKEN` — full control of `@transcriber:berget.ai` (read/post
  in all its rooms, invite itself anywhere).
- `BERGET_API_KEY` — billed API usage (realtime transcription + LLM summaries).
- The Olm identity + Megolm sessions in the nio store (PVC) — the bot's entire
  E2EE history and its ability to receive call media keys.
- The bot's *presence itself*: an official-looking participant that signals
  legitimacy in any room/call it joins.

## Trust boundaries

1. **Invite policy** is THE perimeter. The bot is by design an E2EE endpoint:
   any room it joins can pull it into calls, where publishers Olm-encrypt
   their media keys to it. Whoever can invite the bot gets a transcription
   oracle billed to us — and a phishing prop.
2. The bot has **power level 0** everywhere and no server admin — room-level
   blast radius only.
3. Federation on matrix.berget.ai is **open** — "users" includes external
   homeservers unless the invite policy restricts them.

## Abuse vectors and controls

| # | Vector | Impact | Control (shipped) |
|---|--------|--------|-------------------|
| A | Invite hijack (incl. external federation) → free transcription on our API key, phishing prop, resource exhaustion | Cost, reputation | `ALLOWED_INVITER_HOMESERVERS` (default: own server only), `ACCEPT_INVITES_FROM` exact list, `ROOM_IDS` pinlist |
| B | Bounce-back after kick | Persistence | `INVITE_COOLDOWN_SECONDS` (24 h) armed on any departure; `!transcriber leave` chat command for soft eviction |
| C | Invite flood | CPU/API cost | `MAX_ROOMS` (25), `MAX_CONCURRENT_CALLS` (3) |
| D | Content injection under bot identity (spoken text → posts; prompt injection → summary LLM) | Misleading/harmful posts | System-prompt injection guard; summary content stays in-room (same audience as the call) |
| E | msc3401 membership overwrite (PL 0 event type; Matrix doesn't auth state_key owners) → silent key-distribution cut | DoS on transcription | Membership guard: tick republishes within ~60 s |
| F | Wrong media keys via Olm | Garbled audio for that speaker | None needed — equivalent to the sender making noise |
| G | Token theft (env) | Account takeover | OpenBao appRole, non-root container, no secrets in Git |
| H | PVC theft (Olm identity + Megolm sessions) | E2EE history | `NIO_PICKLE_KEY` in OpenBao — *separate* from the token the store protects |
| I | Homeserver-level MITM on device keys (TOFU) | DoS on quality | Accepted risk (TOFU); the bot posts into rooms whose members already see the content |
| J | Zombie LiveKit room (SFU closes instance silently) | Stuck bot, missed meetings | dead/stale watchdog (disconnect event + 15 min activity timeout → rejoin) |

## Operational guidance

- Rotate `MATRIX_ACCESS_TOKEN` and `BERGET_API_KEY` periodically (OpenBao
  `secret/meeting-transcriber/app`; VSO rolls the pod automatically).
- The greeting message and `!transcriber leave` are the room-facing
  transparency contract — keep them working when editing the invite flow.
- Loki alert worth having: `Joining invited room` for rooms outside an
  expected set (not yet implemented).
- The transcript and the Kimi-K3 summary never leave Berget infrastructure
  (api.berget.ai) — no third-party egress to audit.

## Reporting

Security issues: follow the standard Berget incident process; rotate the two
API secrets first, then evaluate the nio store (delete the PVC to burn the
Olm identity if compromise is suspected — the bot re-registers keys on boot,
publishers re-send call keys on the next leave→join cycle).
