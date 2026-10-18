"""Tests for the tolk (LiveText) integration: client, parsing, formatting."""
import asyncio
from types import SimpleNamespace

import pytest

from src.main import Bot, CallContext
from src.tolk import TolkClient, extract_insights, format_insight, parse_insights


class FakeTolk(TolkClient):
    """Stubs the HTTP seam; records every call."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []
        self.script = []  # (status, data) tuples consumed in order

    async def _post(self, path, payload):
        self.calls.append((path, payload))
        status, data = self.script.pop(0) if self.script else (500, {})
        # Mimic the real seam: create returns 201+body, pushes 202+{}.
        return status, data


def make_client(**kw) -> FakeTolk:
    return FakeTolk("https://tolk.test", "key-123", "sv", **kw)


# ---- TolkClient ------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_session_stores_id_and_url():
    c = make_client()
    c.script = [(201, {"id": "ABCDEF", "listenUrl": "https://tolk.test/listen?id=ABCDEF"})]
    url = await c.create_session()
    assert url == "https://tolk.test/listen?id=ABCDEF"
    assert c.session_id == "ABCDEF"
    assert c.calls == [("/api/sessions", {"sourceLang": "sv"})]


@pytest.mark.asyncio
async def test_push_segment_creates_session_lazily_then_posts():
    c = make_client()
    c.script = [
        (201, {"id": "S1", "listenUrl": "u1"}),
        (202, {}),
    ]
    recreated = await c.push_segment("Magnus", "Vi börjar.")
    assert recreated is None  # ingen återskapelse → ingen ny länk
    assert c.calls[-1] == ("/api/sessions/S1/segments", {"speaker": "Magnus", "text": "Vi börjar."})


@pytest.mark.asyncio
async def test_push_404_recreates_session_and_returns_new_url():
    c = make_client()
    c.script = [
        (201, {"id": "S1", "listenUrl": "u1"}),
        (202, {}),          # första segmentet ok
        (404, {}),          # andra: tolk har startat om
        (201, {"id": "S2", "listenUrl": "u2"}),
        (202, {}),          # retry mot nya sessionen
    ]
    assert await c.push_segment("A", "ett") is None
    new_url = await c.push_segment("A", "två")
    assert new_url == "u2"
    assert c.session_id == "S2"
    assert c.calls[-1] == ("/api/sessions/S2/segments", {"speaker": "A", "text": "två"})


@pytest.mark.asyncio
async def test_push_gives_up_when_create_fails():
    c = make_client()
    c.script = [(500, {})]
    assert await c.push_segment("A", "text") is None
    assert c.session_id is None


@pytest.mark.asyncio
async def test_segment_truncated_at_tolk_limit():
    c = make_client()
    c.script = [(201, {"id": "S1", "listenUrl": "u1"}), (202, {})]
    await c.push_segment("A", "x" * 5000)
    assert len(c.calls[-1][1]["text"]) == 4000


# ---- parse_insights ---------------------------------------------------------

def test_parse_clean_json():
    raw = '{"agenda": ["Budget"], "åtgärder": ["Magnus bokar demo"], "beslut": []}'
    assert parse_insights(raw) == {"agenda": ["Budget"], "åtgärder": ["Magnus bokar demo"]}


def test_parse_fenced_json():
    raw = 'Här är svaret:\n```json\n{"agenda": ["Budget", "Roadmap"]}\n```'
    assert parse_insights(raw) == {"agenda": ["Budget", "Roadmap"]}


def test_parse_garbage_returns_empty():
    assert parse_insights("Jag kan inte hjälpa till") == {}
    assert parse_insights('{"agenda": "inte en lista"}') == {}
    assert parse_insights("[1,2,3]") == {}


def test_parse_caps_at_five_and_strips():
    raw = '{"agenda": [" a ", "b", "c", "d", "e", "f", ""]}'
    assert parse_insights(raw) == {"agenda": ["a", "b", "c", "d", "e"]}


# ---- format_insight ---------------------------------------------------------

def test_format_joins_with_bullets():
    assert format_insight(["a", "b"]) == "a • b"


def test_format_truncates_at_tolk_limit():
    text = format_insight(["x" * 600, "y" * 600])
    assert len(text) == 1000
    assert text.endswith("…")


# ---- _insights_loop ---------------------------------------------------------

@pytest.mark.asyncio
async def test_insights_loop_pushes_only_changes(monkeypatch):
    ctx = CallContext("!r:x")
    ctx.tolk = make_client()
    ctx.tolk.session_id = "S1"  # hoppa över create
    posted_links = []
    fake_self = SimpleNamespace(
        cfg=SimpleNamespace(
            tolk_insights_interval=0.05,
            berget_api_base="",
            berget_api_key="",
            tolk_insights_model="m",
        ),
        _post_tolk_link=lambda c, url, updated=False: posted_links.append((url, updated))
        or asyncio.sleep(0),
    )
    rounds = iter([
        {"agenda": ["Budget"]},
        {"agenda": ["Budget"]},          # oförändrat → ingen push
        {"agenda": ["Budget", "Roadmap"], "beslut": ["Vi kör"]},
    ])

    async def fake_extract(*_a, **_k):
        return next(rounds, {})

    monkeypatch.setattr("src.main.extract_insights", fake_extract)

    task = asyncio.ensure_future(Bot._insights_loop(fake_self, ctx))
    try:
        for line in ["A: ett", "B: två", "A: tre"]:
            ctx.transcript.append(line)
        await asyncio.sleep(0.12)   # runda 1: pushar agenda
        ctx.transcript.append("A: fyra")
        await asyncio.sleep(0.12)   # runda 2: samma agenda → ingen push
        ctx.transcript.append("B: fem")
        await asyncio.sleep(0.12)   # runda 3: utökad agenda + beslut → 2 pushar
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    kinds = [c[0].split("/")[-1] for c in ctx.tolk.calls]
    assert kinds == ["insights", "insights", "insights"]  # agenda×2, beslut×1
    payloads = [c[1] for c in ctx.tolk.calls]
    assert payloads[0] == {"kind": "agenda", "text": "Budget", "key": "agenda"}
    assert {"kind": "decision", "text": "Vi kör", "key": "beslut"} in payloads
