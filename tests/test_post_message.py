"""Tests for post_message: thread shape, HTML formatting, TOFU retry."""
from types import SimpleNamespace

import pytest

from nio.exceptions import OlmUnverifiedDeviceError

from src.matrixing import Matrixing


class FakeResponse:
    def __init__(self, event_id="$evt1"):
        self.event_id = event_id


class FakeClient:
    def __init__(self, fail_once=False):
        self.sent = []
        self.fail_once = fail_once
        self.verified = False

    async def room_send(self, room_id, message_type, content):
        self.sent.append(content)
        if self.fail_once and len(self.sent) == 1:
            raise OlmUnverifiedDeviceError("unverified")
        return FakeResponse()


def _matrixing(client):
    m = Matrixing.__new__(Matrixing)
    m.client = client
    m.cfg = SimpleNamespace(user_id="@bot:x", device_id="D")
    return m


@pytest.mark.asyncio
async def test_thread_reply_shape():
    client = FakeClient()
    m = _matrixing(client)
    event_id = await m.post_message("!r:x", "plain", html="<b>bold</b>", thread_root="$root")
    assert event_id == "$evt1"
    content = client.sent[0]
    assert content["format"] == "org.matrix.custom.html"
    assert content["formatted_body"] == "<b>bold</b>"
    rel = content["m.relates_to"]
    assert rel["rel_type"] == "m.thread"
    assert rel["event_id"] == "$root"
    # Without this flag Element renders quote-replies in the main timeline.
    assert rel["is_falling_back"] is True
    assert rel["m.in_reply_to"]["event_id"] == "$root"


@pytest.mark.asyncio
async def test_plain_message_without_thread():
    client = FakeClient()
    m = _matrixing(client)
    await m.post_message("!r:x", "hello")
    content = client.sent[0]
    assert "m.relates_to" not in content
    assert "formatted_body" not in content


@pytest.mark.asyncio
async def test_unverified_device_triggers_trust_and_retry():
    client = FakeClient(fail_once=True)
    m = _matrixing(client)

    trusted = []

    def trust_all():
        trusted.append(True)

    m._trust_all_devices = trust_all
    event_id = await m.post_message("!r:x", "hello")
    assert event_id == "$evt1"
    assert len(client.sent) == 2  # retried once
    assert trusted == [True]
