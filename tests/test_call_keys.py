"""Tests for io.element.call.encryption_keys content parsing."""
import base64

from src.matrixing import parse_call_key_event

KEY16 = bytes(range(16))


def _content(key_obj, device="DEV1", room="!r:x"):
    return {
        "room_id": room,
        "member_id": "uuid-1",
        "member": {"claimed_device_id": device, "id": "uuid-1"},
        **key_obj,
    }


def test_media_key_padded():
    c = _content({"media_key": {"index": 3, "key": base64.b64encode(KEY16).decode()}})
    assert parse_call_key_event(c) == ("!r:x", "DEV1", KEY16, 3)


def test_media_key_unpadded():
    raw = base64.b64encode(KEY16).decode().rstrip("=")
    c = _content({"media_key": {"index": 0, "key": raw}})
    assert parse_call_key_event(c) == ("!r:x", "DEV1", KEY16, 0)


def test_deprecated_keys_field():
    c = _content({"keys": {"index": 7, "key": base64.b64encode(KEY16).decode()}})
    assert parse_call_key_event(c) == ("!r:x", "DEV1", KEY16, 7)


def test_missing_fields_rejected():
    assert parse_call_key_event({}) is None
    assert parse_call_key_event(_content({"media_key": {"index": 0, "key": "AA"}}, device=None)) is None
    assert parse_call_key_event(_content({"media_key": {"index": 0, "key": "AA"}}, room=None)) is None


def test_bad_base64_and_length_rejected():
    assert parse_call_key_event(_content({"media_key": {"index": 0, "key": "!!!"}})) is None
    short = base64.b64encode(b"\x01\x02\x03").decode()
    assert parse_call_key_event(_content({"media_key": {"index": 0, "key": short}})) is None
