# meeting-transcriber

Meeting-transcription bot for Berget MatrixRTC calls.

Joins a **call as a participant** (Matrix + LiveKit), subscribes to every
remote audio track, streams 24 kHz mono PCM to Berget's OpenAI-Realtime-GA-
compatible endpoint (`wss://api.berget.ai/v1/realtime?intent=transcription`,
model `klang/pianissimo`), and posts completed turns back into the Matrix
room as text — attributed per speaker (one LiveKit track per participant).

## How it joins

Same flow as an Element Call client:

1. Matrix OpenID token (`/_matrix/client/v3/user/<bot>/openid/request_token`)
2. `POST https://matrix-rtc.berget.ai/sfu/get` → `{url, jwt}`
3. `livekit` SDK → connect → subscribe to all audio tracks
4. Per-track realtime WS session (server-VAD commits turns)
5. Completed transcripts → `m.room.message` from the bot

## Requirements

- A Matrix user for the bot (created via `mas-cli`, see Runbook below)
- A Berget API key (`sk_ber_…`) with realtime access
- Python 3.12 (runtime deps: `matrix-nio`, `livekit`, `websockets`, `aiohttp`)

## Limitations (MVP)

- Per-participant call E2EE is **not** handled: in rooms where Element Call
  encrypts per-participant media, audio subscriptions must be made in rooms
  with call E2EE disabled (dedicated transcription room) — phase 2 will add
  SFrame key negotiation.
- Speaker names resolve for legacy-style LiveKit identities
  (`@user:server:device`); slot-style identities show as `Deltagare <hash>`.
- One replica only (bot state is in-memory).

## Local run

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export MATRIX_USER_ID='@transcriber:berget.ai'
export MATRIX_ACCESS_TOKEN='mct_...'
export BERGET_API_KEY='sk_ber_...'
python -m src.main
```

## Env vars

| Var | Default | Meaning |
| --- | --- | --- |
| `HOMESERVER_URL` | `https://matrix.berget.ai` | Client-server base URL |
| `MATRIX_USER_ID` | — | Bot's full Matrix user id |
| `MATRIX_ACCESS_TOKEN` | — | Compatibility token (secret) |
| `ACCEPT_INVITES_FROM` | *(empty = everyone)* | Comma-list of inviter user ids |
| `ROOM_IDS` | *(empty)* | Rooms the bot always stays in |
| `SFU_GET_URL` | `https://matrix-rtc.berget.ai/sfu/get` | lk-jwt authorisation |
| `REALTIME_URL` | `wss://api.berget.ai/v1/realtime` | Berget realtime |
| `TRANSCRIPTION_MODEL` | `klang/pianissimo` | STT model |
| `TRANSCRIPTION_LANGUAGES` | `sv` | Languages (CSV) |
| `TRANSCRIPTION_CHUNK_SECONDS` | `3` | Latency/accuracy tradeoff |
| `POST_TRANSCRIPTS` | `true` | Post completed turns to the room |
| `POLL_INTERVAL` | `15` | Seconds between session polls |

## Deploy / Runbook (berget-internal)

GitOps via Flux. Manifests live in `k8s/app/{base,overlays/production}` in
this repo; the string constants routed from the infra repo
(`clusters/berget-internal/apps/meeting-transcriber/`, see `.github` + `k8s/`
template structure).

### 1. Create the bot Matrix user (one-time, engångs-ops)

Public registration is disabled and this stack authenticates via MAS, so the
user is created directly in the MAS database via `mas-cli` on the
berget-internal cluster:

```bash
export KUBECONFIG=~/.kube/config/berget-internal/kubeconfig
MAS_POD=element-matrix-authentication-service-<pod-id>   # kubectl get pods -n element

# `register-user` prompts for a password interactively — pipe a random
# throwaway password via stdin (bot auths via compatibility token anyway).
# (MAS pod is shell-less; mas-cli reads stdin directly.)
PW=$(openssl rand -hex 32)
printf '%s\n%s\n' "$PW" "$PW" | kubectl exec -i -n element \
  -c matrix-authentication-service "$MAS_POD" -- \
  mas-cli manage register-user -y -A -d "Transcriber" transcriber

# Mint a classic access token:
kubectl exec -n element -c matrix-authentication-service "$MAS_POD" -- \
  mas-cli manage issue-compatibility-token transcriber
# stdout: "Access token: mct_..." → save it for step 3
```

(`-y` = defaults, `-A` = no admin; config path resolves inside the pod.)

### 2. Create the Berget API key for the bot

In the console (or via Lago admin): create `sk_ber_...` on the internal
account used for infra services (same pattern as the tolk key).

### 3. Put credentials in OpenBao

```bash
export KUBECONFIG=~/.kube/config/berget-prod/kubeconfig
bao kv put secret/meeting-transcriber/app \
  MATRIX_ACCESS_TOKEN="mct_..." \
  BERGET_API_KEY="sk_ber_..."

# appRole per berget-internal pattern (roleId goes into k8s/vault-secrets.yaml,
# secretId is seeded here):
bao write auth/approle/role/meeting-transcriber \
  token_policies=meeting-transcriber-secrets

bao policy write meeting-transcriber-secrets - <<EOF
path "secret/data/meeting-transcriber/app" { capabilities = ["read"] }
EOF

# Fetch role id → replace REPLACE_WITH_ROLE_ID in k8s/vault-secrets.yaml:
bao read auth/approle/role/meeting-transcriber/role-id

# Seed the secretId secret on berget-internal:
export KUBECONFIG=~/.kube/config/berget-internal/kubeconfig
SID=$(KUBECONFIG=~/.kube/config/berget-prod/kubeconfig bao write -f -field=secret_id auth/approle/role/meeting-transcriber/secret-id)
kubectl -n meeting-transcriber create secret generic meeting-transcriber-approle-credentials \
  --from-literal=secretId="$SID"
```

(`k8s/namespace.yaml` creates the namespace before the secret is needed,
so the secret can also be applied after the first Flux sync.)

### 4. Wire into berget-internal (infra PR)

Copy `k8s/app/…` reference is already on GitHub via CI; infra-side files:
- `clusters/berget-internal/apps/meeting-transcriber/GitRepository.yaml`
- `clusters/berget-internal/apps/meeting-transcriber/flux-kustomization.yaml`
- `clusters/berget-internal/apps/meeting-transcriber/vault-secrets.yaml` (with real roleId)

Point `./k8s/app/overlays/production` from this repo; `prune: true`.

### 5. Verify

```bash
flux get kustomizations -A | grep meeting-transcriber
kubectl -n meeting-transcriber logs deploy/meeting-transcriber -f
```

Trigger by inviting `@transcriber:berget.ai` to a room with an active call —
you should see `Transkribering aktiv` + per-speaker lines in the room.
