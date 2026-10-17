"""Tests for the invite policy: homeserver suffix, exact match, denial,
cooldown after kick/leave, and the max_rooms circuit breaker."""
import time
from types import SimpleNamespace

import pytest

from src.matrixing import Matrixing


def _cfg(**over):
    base = dict(
        user_id="@transcriber:berget.ai",
        device_id="DEV",
        homeserver="https://hs",
        accept_invites_from=[],
        allowed_inviter_homeservers=["berget.ai"],
        room_ids=[],
        max_rooms=2,
        invite_cooldown_seconds=3600,
    )
    base.update(over)
    return SimpleNamespace(**base)


def _matrixing(cfg):
    m = Matrixing.__new__(Matrixing)
    m.cfg = cfg
    m.client = SimpleNamespace(rooms={}, joined_rooms={})
    m.invite_cooldowns = {}
    return m


def test_same_homeserver_inviter_allowed():
    m = _matrixing(_cfg())
    assert m.inviter_allowed("@irony:berget.ai") is True


def test_external_inviter_denied():
    m = _matrixing(_cfg())
    assert m.inviter_allowed("@attacker:evil.example") is False
    assert m.inviter_allowed(None) is False


def test_exact_user_match_overrides_homeserver():
    m = _matrixing(_cfg(accept_invites_from=["@friend:elsewhere.org"]))
    assert m.inviter_allowed("@friend:elsewhere.org") is True
    assert m.inviter_allowed("@other:elsewhere.org") is False


@pytest.mark.asyncio
async def test_cooldown_blocks_reinvite():
    m = _matrixing(_cfg())
    m.invite_cooldowns["!r:x"] = time.time()  # just departed
    joined = []

    async def fake_join(room_id):
        joined.append(room_id)

    m._join_and_greet = fake_join
    m.joined_rooms = lambda: ["!other:x"]
    await m._handle_invite("!r:x")
    assert joined == []


@pytest.mark.asyncio
async def test_max_rooms_circuit_breaker():
    m = _matrixing(_cfg(max_rooms=1))
    joined = []

    async def fake_join(room_id):
        joined.append(room_id)

    m._join_and_greet = fake_join

    async def already_full():
        return ["!a:x", "!b:x"]

    m.joined_rooms = already_full
    await m._handle_invite("!new:x")
    assert joined == []
