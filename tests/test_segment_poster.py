"""Tests for SegmentPoster: same-speaker utterances coalesce via edits."""
import pytest

from src.main import SegmentPoster


class FakeMatrix:
    def __init__(self):
        self.posts = []
        self.edits = []
        self._n = 0

    async def post_message(self, room_id, body, html=None, thread_root=None):
        self._n += 1
        self.posts.append({"body": body, "html": html, "thread_root": thread_root})
        return f"$evt{self._n}"

    async def edit_message(self, room_id, target_event_id, body, html=None):
        self.edits.append({"target": target_event_id, "body": body, "html": html})
        return target_event_id


@pytest.mark.asyncio
async def test_same_speaker_coalesces_into_one_message():
    m = FakeMatrix()
    p = SegmentPoster(m, "!r:x", "$root")
    await p.add("Anna", "Hej")
    await p.add("Anna", "hur mår du")
    await p.add("Anna", "idag")
    assert len(m.posts) == 1
    assert len(m.edits) == 2
    assert m.edits[-1]["target"] == "$evt1"
    assert m.edits[-1]["body"] == "Anna: Hej hur mår du idag"


@pytest.mark.asyncio
async def test_speaker_change_starts_new_message():
    m = FakeMatrix()
    p = SegmentPoster(m, "!r:x", "$root")
    await p.add("Anna", "Hej")
    await p.add("Bo", "Tjena")
    assert len(m.posts) == 2
    assert len(m.edits) == 0
    assert m.posts[1]["thread_root"] == "$root"


@pytest.mark.asyncio
async def test_long_segment_overflows_to_new_message():
    m = FakeMatrix()
    p = SegmentPoster(m, "!r:x", "$root", max_chars=20)
    await p.add("Anna", "0123456789")
    await p.add("Anna", "0123456789")  # would exceed 20 chars
    assert len(m.posts) == 2
    assert len(m.edits) == 0
