"""Join the MatrixRTC session's LiveKit room as a listener participant.

Flow (same as Element Call clients):

1. Matrix OpenID token  →  POST <sfu-get>/ (`lk-jwt` authorisation service)
2. Response {url, jwt}  →  livekit SDK connect
3. Subscribe to all remote audio tracks
"""
from __future__ import annotations

import logging
import time
from typing import Any

import aiohttp
from livekit import rtc

log = logging.getLogger(__name__)


async def fetch_sfu_credentials(
    sfu_get_url: str, openid: dict[str, Any], room_id: str
) -> dict[str, str]:
    payload = {
        "openid_token": {
            "access_token": openid["access_token"],
            "token_type": openid.get("token_type", "Bearer"),
            "matrix_server_name": openid["matrix_server_name"],
        },
        "room": room_id,
    }
    async with aiohttp.ClientSession() as http:
        async with http.post(sfu_get_url, json=payload) as resp:
            body = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"sfu/get {resp.status}: {body[:200]}")
            data = await resp.json()
    return {"url": data["url"], "jwt": data["jwt"]}


class CallRoom:
    """A joined LiveKit room we listen to.

    Zombie-safety: the SFU closes a room instance when it empties; a later
    call re-creates it under a new room_sid. Our signal websocket can stay
    open against the dead instance without any event reaching the SDK, and
    remote_participants keeps stale entries — so `empty` alone can never be
    trusted. We therefore track:
      - `dead`: set by the SDK "disconnected" event (clean leave signal), and
      - `last_activity`: bumped by any participant/track event AND by audio
        frames flowing through the transcription pump (touch()).
    The main loop leaves + rejoins when dead, or when stale() times out.
    """

    def __init__(self, matrix_room_id: str):
        self.matrix_room_id = matrix_room_id
        self.room = rtc.Room()
        self.dead = False
        self.last_activity = time.time()
        self._wire_events()

    def _wire_events(self) -> None:
        def on_disconnected(*_args):
            log.warning("[%s] LiveKit disconnected by server/SDK", self.matrix_room_id)
            self.dead = True

        def on_activity(*_args):
            self.last_activity = time.time()

        self.room.on("disconnected")(on_disconnected)
        for event in (
            "participant_connected",
            "participant_disconnected",
            "track_published",
            "track_unpublished",
            "track_subscribed",
            "reconnecting",
            "reconnected",
        ):
            self.room.on(event)(on_activity)

    def touch(self) -> None:
        """Heartbeat from the audio pump — proof the media path is alive."""
        self.last_activity = time.time()

    def stale(self, seconds: float) -> bool:
        return (time.time() - self.last_activity) > seconds

    async def connect(self, url: str, jwt: str) -> None:
        # Element Call per-participant E2EE = LiveKit FrameCryptor AES-GCM
        # (encryption_type GCM=1) with HKDF key derivation, ratchet window 10
        # and a 256-slot keyring (MatrixKeyProvider.ts values). Keys arrive
        # out-of-band via Olm-encrypted to-device events and are fed to the
        # key provider via set_participant_key().
        options = rtc.RoomOptions(
            encryption=rtc.E2EEOptions(
                key_provider_options=rtc.KeyProviderOptions(
                    ratchet_salt=b"LKFrameEncryptionKey",
                    ratchet_window_size=10,
                    key_ring_size=256,
                    # KeyDerivationFunction proto enum: PBKDF2=0, HKDF=1.
                    # JS per-participant keys are HKDF — python defaults to
                    # PBKDF2, so set it explicitly.
                    key_derivation_function=1,
                )
            )
        )
        await self.room.connect(url, jwt, options=options)
        self.last_activity = time.time()
        log.info("LiveKit connected to %s (%d remote participants)", url, len(self.room.remote_participants))

    def set_participant_key(self, identity: str, key: bytes, index: int) -> bool:
        """Install a 16-byte call media key for a participant (Olm to-device)."""
        kp = getattr(self.room.e2ee_manager, "key_provider", None)
        if kp is None:
            return False
        kp.set_key(identity, key, index)
        log.info("call key installed for %s (index %d)", identity, index)
        return True

    async def close(self) -> None:
        try:
            await self.room.disconnect()
        except Exception:
            pass

    def remote_audio(self) -> list[tuple[rtc.RemoteParticipant, rtc.RemoteAudioTrack, int]]:
        out = []
        for p in self.room.remote_participants.values():
            for pub in p.track_publications.values():
                if pub.kind == rtc.TrackKind.KIND_AUDIO and pub.track is not None:
                    # encryption_type (LiveKit proto enum) decides whether the
                    # frame payloads are per-participant SFrame ciphertext —
                    # the instance our decryption story for E2EE calls is.
                    out.append((p, pub.track, int(pub.encryption_type)))
        return out

    @property
    def empty(self) -> bool:
        return len(self.room.remote_participants) == 0
