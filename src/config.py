"""Environment-derived configuration for the meeting transcriber bot."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _csv(name: str) -> list[str]:
    raw = os.environ.get(name, "").strip()
    return [part.strip() for part in raw.split(",") if part.strip()]


@dataclass(frozen=True)
class Config:
    homeserver: str = os.environ.get("HOMESERVER_URL", "https://matrix.berget.ai").rstrip("/")
    user_id: str = os.environ["MATRIX_USER_ID"]
    access_token: str = os.environ["MATRIX_ACCESS_TOKEN"]
    # E2EE device identity (the MAS compat session's device — `whoami`).
    device_id: str = os.environ.get("MATRIX_DEVICE_ID", "")
    # Persistent vodozemac/Olm store (PVC-mounted in k8s). Ephemeral stores
    # mean new identity keys per boot → broken Olm sessions + lost call keys.
    store_path: str = os.environ.get("NIO_STORE_PATH", "/data/nio-store")
    # Separate store-encryption key (defense in depth: the PVC holds the Olm
    # identity + all Megolm sessions; without this the store is pickled with
    # the very access token it protects). Falls back to the token for
    # backwards compatibility — set NIO_PICKLE_KEY in OpenBao.
    pickle_key: str = os.environ.get("NIO_PICKLE_KEY", "")

    # Invite policy. Default: only same-homeserver users may invite the bot
    # (derived from MATRIX_USER_ID) — federation is open on our Synapse, so
    # without this anyone on the public Matrix network could borrow the bot
    # (API costs billed to us + phishing prop value).
    accept_invites_from: list[str] = field(default_factory=lambda: _csv("ACCEPT_INVITES_FROM"))
    allowed_inviter_homeservers: list[str] = field(
        default_factory=lambda: _csv("ALLOWED_INVITER_HOMESERVERS")
        or [os.environ.get("MATRIX_USER_ID", "@bot:x").rsplit(":", 1)[-1]]
    )
    # Rooms to always stay in regardless of invites.
    room_ids: list[str] = field(default_factory=lambda: _csv("ROOM_IDS"))
    # Circuit breakers.
    max_concurrent_calls: int = int(os.environ.get("MAX_CONCURRENT_CALLS", "3"))
    max_rooms: int = int(os.environ.get("MAX_ROOMS", "25"))
    invite_cooldown_seconds: int = int(os.environ.get("INVITE_COOLDOWN_SECONDS", "86400"))

    # RTC stack (Element ESS layout on our homeserver).
    sfu_get_url: str = os.environ.get("SFU_GET_URL", "https://matrix-rtc.berget.ai/sfu/get")

    # Berget realtime transcription.
    berget_api_key: str = os.environ["BERGET_API_KEY"]
    berget_api_base: str = os.environ.get("BERGET_API_BASE", "https://api.berget.ai").rstrip("/")
    realtime_url: str = os.environ.get("REALTIME_URL", "wss://api.berget.ai/v1/realtime")
    model: str = os.environ.get("TRANSCRIPTION_MODEL", "klang/pianissimo")
    languages: list[str] = field(default_factory=lambda: _csv("TRANSCRIPTION_LANGUAGES") or ["sv"])
    chunk_seconds: int = int(os.environ.get("TRANSCRIPTION_CHUNK_SECONDS", "3"))

    post_to_room: bool = os.environ.get("POST_TRANSCRIPTS", "true").lower() != "false"
    # End-of-call renskrivning (cleaned-up notes) via the LLM API.
    post_summary: bool = os.environ.get("POST_SUMMARY", "true").lower() != "false"
    summary_model: str = os.environ.get("SUMMARY_MODEL", "moonshotai/Kimi-K3")
    # How often we check MatrixRTC rooms for active sessions (seconds).
    poll_interval: int = int(os.environ.get("POLL_INTERVAL", "15"))
    # Zombie-room watchdog: leave+rejoin when no LiveKit events AND no audio
    # frames for this long (SFU can close the room instance without the
    # signal socket noticing).
    stale_seconds: int = int(os.environ.get("STALE_SECONDS", "900"))


config = Config()
