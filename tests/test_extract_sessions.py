"""Tests for MSC3401 session extraction (modern per-device + legacy shapes)."""
from src.matrixing import MSC3401_MEMBER, extract_sessions


def _event(content, state_key="_@a:b_DEV_m.call"):
    return {"type": MSC3401_MEMBER, "state_key": state_key, "content": content}


def test_modern_per_device_membership():
    events = [
        _event({"application": "m.call", "device_id": "DEV", "membershipID": "@a:b:DEV"}),
        _event({}, state_key="_@c:d_OLD_m.call"),  # departed device: empty content
        {"type": "m.room.name", "content": {"name": "x"}},  # unrelated state
    ]
    sessions = extract_sessions(events)
    assert len(sessions) == 1
    assert sessions[0]["device_id"] == "DEV"


def test_legacy_list_shape():
    events = [
        _event(
            [{"application": "m.call", "device_id": "D1"}, {"application": "m.call", "device_id": "D2"}],
            state_key="",
        )
    ]
    sessions = extract_sessions(events)
    assert [s["device_id"] for s in sessions] == ["D1", "D2"]


def test_empty_room_no_sessions():
    assert extract_sessions([]) == []
    assert extract_sessions([{"type": "m.room.member", "content": {"membership": "join"}}]) == []
    assert extract_sessions([_event({})]) == []
