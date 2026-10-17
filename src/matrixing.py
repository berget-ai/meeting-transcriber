"""Matrix client operations: invites, room membership state, posting text,
E2EE device keys (vodozemac) and MatrixRTC call-encryption-key reception."""
from __future__ import annotations

import asyncio
import base64
import logging
import os
from typing import Any, Callable, Awaitable

import aiohttp
from nio import AsyncClient, AsyncClientConfig
from nio.crypto.olm_machine import Olm
from nio.events.to_device import UnknownToDeviceEvent
from nio.exceptions import OlmUnverifiedDeviceError
from nio.responses import RoomSendError

from .config import Config

log = logging.getLogger(__name__)

MSC3401_MEMBER = "org.matrix.msc3401.call.member"
CALL_KEYS_EVENT = "io.element.call.encryption_keys"


def _patch_olm_custom_events() -> None:
    """nio 0.26 decrypts Olm to-device events but DROPS any payload whose type
    it doesn't know (m.room_key/m.forwarded_room_key/m.dummy only) — the event
    is consumed from the ratchet and no callback fires. Element Call's
    io.element.call.encryption_keys hits exactly that path. Wrap the payload
    in UnknownToDeviceEvent instead so add_to_device_callback can see it."""
    original = Olm._handle_olm_event

    def patched(self, sender, sender_key, payload):
        if payload.get("type") == CALL_KEYS_EVENT:
            log.info("decrypted %s from %s", CALL_KEYS_EVENT, payload.get("sender"))
            return UnknownToDeviceEvent(
                payload, payload.get("sender", sender), payload["type"]
            )
        return original(self, sender, sender_key, payload)

    Olm._handle_olm_event = patched


class Matrixing:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        _patch_olm_custom_events()
        # nio's peewee store does not create the parent dir itself.
        os.makedirs(cfg.store_path, exist_ok=True)
        config = AsyncClientConfig(
            encryption_enabled=True,
            store_sync_tokens=True,
            # pickle_key protects the on-disk Olm account in the store. The
            # access token is already the pod's most sensitive secret and lives
            # in the same boundary (OpenBao-seeded env), so reuse it.
            pickle_key=cfg.access_token,
        )
        self.client = AsyncClient(
            cfg.homeserver,
            cfg.user_id,
            device_id=cfg.device_id or None,
            store_path=cfg.store_path,
            config=config,
        )
        self.client.restore_login(
            user_id=cfg.user_id,
            device_id=cfg.device_id,
            access_token=cfg.access_token,
        )
        self.client.add_to_device_callback(self._on_to_device, UnknownToDeviceEvent)
        # Set by main.Bot: (room_id, sender, claimed_device_id, key_bytes, index)
        self.on_call_key: Callable[[str, str, str, bytes, int], Awaitable[None]] | None = None
        # ClientSession is created lazily: it must exist inside a running
        # event loop, so __init__ (called synchronously) cannot own it.
        self._http: aiohttp.ClientSession | None = None

    async def _on_to_device(self, event: UnknownToDeviceEvent) -> None:
        if event.type != CALL_KEYS_EVENT or self.on_call_key is None:
            return
        parsed = parse_call_key_event(event.source.get("content", {}))
        if parsed is None:
            log.warning("malformed %s from %s", CALL_KEYS_EVENT, event.sender)
            return
        room_id, device_id, key_bytes, index = parsed
        await self.on_call_key(room_id, event.sender, device_id, key_bytes, index)

    async def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.cfg.access_token}"}
            )
        return self._http

    async def sync_forever(self) -> None:
        first = True
        while True:
            try:
                # Raw sync() (not client.sync_forever) → we must drive key
                # uploads ourselves (identity keys + one-time keys on first
                # run, OTK top-ups later). Without this nobody can Olm-encrypt
                # call keys to us.
                if self.client.should_upload_keys:
                    await self.client.keys_upload()
                # First sync after boot needs full_state: with a persisted
                # store the resumed sync token yields zero room frames, and
                # nio's joined_rooms cache would stay empty forever.
                resp = await self.client.sync(timeout=15_000, full_state=first)
                first = False
                # nio types SyncResponse.rooms as a Rooms dataclass, not dict
                invites = getattr(resp.rooms, "invite", None) or {}
                for room_id in invites:
                    try:
                        await self._handle_invite(room_id)
                    except Exception:
                        log.exception("invite handling failed for %s", room_id)
            except Exception:
                log.exception("sync failed")
                await asyncio.sleep(5)
            await asyncio.sleep(1)

    async def sweep_invited_rooms(self) -> None:
        """Safety net: nio keeps invited rooms in client.invited_rooms even
        if a sync frame was missed — join any allowed one."""
        for room_id in list(getattr(self.client, "invited_rooms", {}) or []):
            try:
                await self._handle_invite(room_id)
            except Exception:
                log.exception("sweep-invite handling failed for %s", room_id)

    async def _handle_invite(self, room_id: str) -> None:
        inviter_ok = True
        if self.cfg.accept_invites_from and room_id not in self.cfg.room_ids:
            inviter = None
            try:
                # The bot's m.room.member state event for an invite is authored by
                # the inviter, so its sender field identifies who to check.
                resp = await self.client.room_get_state_event(room_id, "m.room.member", self.cfg.user_id)
                inviter = getattr(resp, "sender", None)
            except Exception:
                pass
            inviter_ok = inviter in self.cfg.accept_invites_from
        if inviter_ok:
            if room_id in self.client.rooms:
                return  # already joined
            log.info("Joining invited room %s", room_id)
            resp = await self.client.join(room_id)
            if getattr(resp, "message", None):
                log.warning("join(%s) returned %s", room_id, resp.message)
        else:
            log.info("Ignoring invite to %s (inviter %r not allowlisted)", room_id, inviter)

    async def joined_rooms(self) -> list[str]:
        # Authoritative REST lookup instead of nio's cache: with a resumed
        # sync token the cache can lag/empty even when full_state ran.
        http = await self._session()
        url = f"{self.cfg.homeserver}/_matrix/client/v3/joined_rooms"
        rooms = set(self.cfg.room_ids)
        try:
            async with http.get(url) as resp:
                if resp.status == 200:
                    rooms.update((await resp.json()).get("joined_rooms", []))
        except Exception:
            log.exception("joined_rooms fetch failed, falling back to nio cache")
            rooms.update(self.client.rooms.keys())
        return sorted(rooms)

    async def active_rtc_sessions(self, room_id: str) -> list[dict[str, Any]]:
        """Active per-device MSC3401 call.member contents (may be [])."""
        http = await self._session()
        url = f"{self.cfg.homeserver}/_matrix/client/v3/rooms/{room_id}/state"
        async with http.get(url) as resp:
            if resp.status != 200:
                return []
            events = await resp.json()
        return extract_sessions(events)

    async def openid_token(self) -> dict[str, Any]:
        http = await self._session()
        url = f"{self.cfg.homeserver}/_matrix/client/v3/user/{self.cfg.user_id}/openid/request_token"
        async with http.post(url, json={}) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def post_message(
        self,
        room_id: str,
        body: str,
        html: str | None = None,
        thread_root: str | None = None,
    ) -> str | None:
        """Post a message (plain or HTML-formatted, optionally as thread reply).

        Returns the new event id (transcript thread root). Element renders
        `formatted_body` (org.matrix.custom.html) instead of `body` when given.
        """
        content: dict[str, Any] = {"msgtype": "m.text", "body": body}
        if html:
            content["format"] = "org.matrix.custom.html"
            content["formatted_body"] = html
        if thread_root:
            content["m.relates_to"] = {
                "rel_type": "m.thread",
                "event_id": thread_root,
                # Without is_falling_back Element renders the reply fallback
                # as a normal quote-reply in the main timeline instead of
                # grouping the message into the thread view.
                "is_falling_back": True,
                "m.in_reply_to": {"event_id": thread_root},
            }
        try:
            resp = await self.client.room_send(
                room_id,
                message_type="m.room.message",
                content=content,
            )
        except OlmUnverifiedDeviceError:
            # Encrypted room: nio refuses to share our Megolm session with
            # unverified devices. The bot's whole job is posting into rooms it
            # was invited to, so trust-on-first-use for every device it has
            # seen there, then retry once.
            log.info("trusting all known devices after OlmUnverifiedDeviceError")
            self._trust_all_devices()
            resp = await self.client.room_send(
                room_id,
                message_type="m.room.message",
                content=content,
            )
        if isinstance(resp, RoomSendError):
            log.warning("[%s] post failed: %s", room_id, resp)
            return None
        return getattr(resp, "event_id", None)

    def _trust_all_devices(self) -> None:
        """TOFU-verify every device in the store (all are colleagues' devices
        seen in rooms the bot was invited to)."""
        from nio.crypto import TrustState

        for user_devices in self.client.device_store.values():
            for device in user_devices.values():
                if device.trust_state in (TrustState.unset, TrustState.ignored):
                    try:
                        self.client.verify_device(device)
                    except Exception:
                        log.exception("verify_device failed for %s", device)

    async def set_call_membership(self, room_id: str, active: bool) -> None:
        """Publish/clear our own msc3401 call.member state.

        Other participants only Olm-encrypt their call media keys TO call
        members (they iterate memberships for user/device pairs) — without
        this state event nobody sends us keys. Content mirrors what Element
        Call writes (per-device state key `_<user>_<device>_m.call`)."""
        state_key = f"_{self.cfg.user_id}_{self.cfg.device_id}_m.call"
        content: dict[str, Any] = {}
        if active:
            content = {
                "application": "m.call",
                "call_id": "",
                "device_id": self.cfg.device_id,
                "expires": 14_400_000,  # 4 h, same as Element Call
                "foci_preferred": [
                    {
                        "livekit_alias": room_id,
                        "livekit_service_url": "https://matrix-rtc.berget.ai",
                        "type": "livekit",
                    }
                ],
                "focus_active": {"focus_selection": "multi_sfu", "type": "livekit"},
                "m.call.intent": "video",
                "membershipID": f"{self.cfg.user_id}:{self.cfg.device_id}",
                "scope": "m.room",
            }
        http = await self._session()
        url = (
            f"{self.cfg.homeserver}/_matrix/client/v3/rooms/{room_id}"
            f"/state/{MSC3401_MEMBER}/{state_key}"
        )
        async with http.put(url, json=content) as resp:
            if resp.status != 200:
                log.warning(
                    "membership publish %s -> %s: %s",
                    room_id, resp.status, (await resp.text())[:200],
                )
            else:
                log.info("call membership %s in %s", "published" if active else "cleared", room_id)

    async def display_name(self, room_id: str, user_id: str) -> str:
        try:
            resp = await self.client.room_get_state_event(room_id, "m.room.member", user_id)
            if resp and getattr(resp, "content", None):
                return resp.content.get("displayname") or user_id
        except Exception:
            pass
        return user_id


def extract_sessions(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pure parser: MSC3401 membership from a room-state event list.

    Modern MSC3401 stores membership as per-device state events whose content
    is a single membership object — active when that object is non-empty.
    The first spec draft used one event at state_key "" holding a list of
    memberships; accept both shapes (unit-tested).
    """
    out: list[dict[str, Any]] = []
    for event in events:
        if event.get("type") != MSC3401_MEMBER:
            continue
        content = event.get("content")
        if isinstance(content, dict) and content:
            out.append(content)
        elif isinstance(content, list):
            out.extend(m for m in content if isinstance(m, dict) and m)
    return out


def parse_call_key_event(
    content: dict[str, Any],
) -> tuple[str, str, bytes, int] | None:
    """Parse io.element.call.encryption_keys content (unit-tested).

    Current shape: {room_id, member_id, media_key:{index,key}, member:
    {claimed_device_id,id}}; `keys` is the deprecated duplicate of media_key.
    Key is base64 (padded or unpadded), 16 raw bytes. None when malformed.
    """
    room_id = content.get("room_id")
    member = content.get("member") or {}
    device_id = member.get("claimed_device_id")
    key_obj = content.get("media_key") or content.get("keys")
    if not (room_id and device_id and isinstance(key_obj, dict)):
        return None
    raw = key_obj.get("key", "")
    # Spec comment says unpadded base64; js-sdk currently emits padded.
    raw += "=" * (-len(raw) % 4)
    try:
        key_bytes = base64.b64decode(raw)
    except Exception:
        return None
    if len(key_bytes) != 16:
        return None
    return room_id, device_id, key_bytes, int(key_obj.get("index", 0))
