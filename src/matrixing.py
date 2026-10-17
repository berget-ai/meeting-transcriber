"""Matrix client operations: invites, room membership state, posting text."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
from nio import AsyncClient

from .config import Config

log = logging.getLogger(__name__)

MSC3401_MEMBER = "org.matrix.msc3401.call.member"


class Matrixing:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.client = AsyncClient(cfg.homeserver)
        self.client.access_token = cfg.access_token
        self.client.user_id = cfg.user_id
        # ClientSession is created lazily: it must exist inside a running
        # event loop, so __init__ (called synchronously) cannot own it.
        self._http: aiohttp.ClientSession | None = None

    async def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.cfg.access_token}"}
            )
        return self._http

    async def sync_forever(self) -> None:
        while True:
            try:
                resp = await self.client.sync(timeout=15_000, full_state=False)
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
        rooms = set(self.cfg.room_ids) | set(self.client.rooms.keys())
        return sorted(rooms)

    async def active_rtc_sessions(self, room_id: str) -> list[dict[str, Any]]:
        """Return the msc3401 call.member event content list, or [] when no call."""
        http = await self._session()
        url = f"{self.cfg.homeserver}/_matrix/client/v3/rooms/{room_id}/state/{MSC3401_MEMBER}"
        async with http.get(url) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
        if isinstance(data, list):
            return [m for m in data if isinstance(m, dict)]
        return []

    async def openid_token(self) -> dict[str, Any]:
        http = await self._session()
        url = f"{self.cfg.homeserver}/_matrix/client/v3/user/{self.cfg.user_id}/openid/request_token"
        async with http.post(url, json={}) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def post_message(self, room_id: str, body: str) -> None:
        await self.client.room_send(
            room_id,
            message_type="m.room.message",
            content={"msgtype": "m.text", "body": body},
        )

    async def display_name(self, room_id: str, user_id: str) -> str:
        try:
            resp = await self.client.room_get_state_event(room_id, "m.room.member", user_id)
            if resp and getattr(resp, "content", None):
                return resp.content.get("displayname") or user_id
        except Exception:
            pass
        return user_id
