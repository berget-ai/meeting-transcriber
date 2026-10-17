"""Tests for the zombie-room watchdog primitives on CallRoom.

Simulates the failure modes we hit in production:
1. SFU closes the room instance without any SDK event → stale() must fire.
2. SDK delivers "disconnected" → dead must flip immediately.
3. Audio frames flowing (touch via pump) must keep stale() from firing.
"""
import time

from src.rtc import CallRoom


class FakeRoom:
    """Stands in for livekit.rtc.Room: captures event handlers."""

    def __init__(self):
        self.handlers = {}
        self.remote_participants = {}

    def on(self, event):
        def register(fn):
            self.handlers[event] = fn
            return fn
        return register

    def emit(self, event, *args):
        self.handlers[event](*args)


def _callroom():
    c = CallRoom.__new__(CallRoom)
    c.matrix_room_id = "!r:x"
    c.room = FakeRoom()
    c.dead = False
    c.last_activity = time.time()
    c._wire_events()
    return c


def test_disconnect_event_marks_dead():
    c = _callroom()
    assert not c.dead
    c.room.emit("disconnected")
    assert c.dead


def test_silent_room_death_goes_stale():
    c = _callroom()
    c.last_activity = time.time() - 1000
    assert c.stale(900)  # no events, no audio for >15 min
    assert not c.stale(1200)


def test_participant_events_reset_staleness():
    c = _callroom()
    c.last_activity = time.time() - 1000
    c.room.emit("participant_connected", object())
    assert not c.stale(900)


def test_audio_pump_heartbeat_resets_staleness():
    c = _callroom()
    c.last_activity = time.time() - 1000
    c.touch()  # called from the realtime pump every ~5 s while audio flows
    assert not c.stale(900)
