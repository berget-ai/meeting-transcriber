"""Orchestrator: poll joined rooms for active MatrixRTC sessions, join, transcribe.

One CallTask per room with an active session; the task joins LiveKit, spawns
one RealtimeSession per remote audio track (per speaker) and posts completed
turns to the Matrix room from the bot account — as HTML-formatted thread
replies under a per-meeting root message. When the call ends the collected
transcript is renskriven (cleaned up/summarised) via the Berget LLM API and
posted in the same thread.
"""
from __future__ import annotations

import asyncio
import html
import logging
import time
from dataclasses import dataclass, field

from .config import config
from .matrixing import Matrixing
from .rtc import CallRoom, fetch_sfu_credentials
from .summarize import summarize
from .transcribe import RealtimeSession

log = logging.getLogger(__name__)


@dataclass
class CallContext:
    """Shared per-call state between the call task and per-track tasks."""

    room_id: str
    thread_root: str | None = None
    transcript: list[str] = field(default_factory=list)
    started: float = field(default_factory=time.time)

    def add(self, speaker: str, text: str) -> None:
        self.transcript.append(f"{speaker}: {text}")


class Bot:
    def __init__(self):
        self.cfg = config
        self.matrix = Matrixing(self.cfg)
        self.calls: dict[str, asyncio.Task] = {}
        self.display_cache: dict[str, str] = {}

    async def run(self) -> None:
        syncer = asyncio.ensure_future(self.matrix.sync_forever())
        health = asyncio.ensure_future(_start_health_server())
        try:
            while True:
                try:
                    await self.tick()
                except Exception:
                    log.exception("tick failed")
                await asyncio.sleep(self.cfg.poll_interval)
        finally:
            syncer.cancel()
            health.cancel()

    async def tick(self) -> None:
        await self.matrix.sweep_invited_rooms()
        for room_id in await self.matrix.joined_rooms():
            sessions = await self.matrix.active_rtc_sessions(room_id)
            # msc3401 member state can hold stale entries until their
            # delayed-leave expires; a 5 s cool-down after each call keeps us
            # from re-joining instantly on those tails.
            if sessions and room_id not in self.calls:
                log.info("Active MatrixRTC session in %s (%d source-lists); joining", room_id, len(sessions))
                self.calls[room_id] = asyncio.ensure_future(
                    self._call_task(room_id)
                )
            elif not sessions and room_id in self.calls:
                log.info("Session ended in %s", room_id)

    async def _call_task(self, room_id: str) -> None:
        call = CallRoom(room_id)
        ctx = CallContext(room_id)
        track_tasks: dict[tuple[str, str], asyncio.Task] = {}
        try:
            openid = await self.matrix.openid_token()
            creds = await fetch_sfu_credentials(self.cfg.sfu_get_url, openid, room_id)
            await call.connect(creds["url"], creds["jwt"])
            if self.cfg.post_to_room:
                banner = (
                    "📝 Transkribering aktiv — mötet transkriberas till "
                    "Berget realtime. Svar i den här tråden."
                )
                ctx.thread_root = await self.matrix.post_message(room_id, banner)

            empty_since: float | None = None
            while True:
                # Manage track tasks
                wanted = {
                    (p.identity, track.sid): (p, track, enc)
                    for p, track, enc in call.remote_audio()
                }
                for key in list(track_tasks):
                    if key not in wanted:
                        track_tasks.pop(key).cancel()
                for key, (participant, track, enc) in wanted.items():
                    if key not in track_tasks:
                        speaker = await self._speaker_name(room_id, participant)
                        log.info(
                            "[%s] subscribing audio track %s from %s (encryption_type=%d, 0=NONE)",
                            room_id, track.sid, participant.identity, enc,
                        )
                        track_tasks[key] = asyncio.ensure_future(
                            self._track_task(ctx, speaker, track)
                        )

                # Leave when everyone else is gone for a while
                if call.empty:
                    empty_since = empty_since or time.time()
                    if time.time() - empty_since > 60:
                        log.info("[%s] room idle for 60 s, leaving call", room_id)
                        return
                else:
                    empty_since = None
                await asyncio.sleep(2)
        except Exception:
            log.exception("[%s] call task failed", room_id)
        finally:
            for t in track_tasks.values():
                t.cancel()
            if track_tasks:
                await asyncio.gather(*track_tasks.values(), return_exceptions=True)
            await call.close()
            await self._close_call(ctx, int(time.time() - ctx.started))
            # Dedup-delay before allowing re-join on sticky sessions
            await asyncio.sleep(5)
            self.calls.pop(room_id, None)

    async def _close_call(self, ctx: CallContext, duration_s: int) -> None:
        """Post end-of-call notes: LLM renskrivning, raw transcript fallback."""
        room_id = ctx.room_id
        if not self.cfg.post_to_room or ctx.thread_root is None:
            return
        minutes = max(1, duration_s // 60)
        if not ctx.transcript:
            try:
                await self.matrix.post_message(
                    room_id,
                    "📝 Transkriberingen avslutad — inget tal fångades.",
                    thread_root=ctx.thread_root,
                )
            except Exception:
                log.exception("[%s] closing post failed", room_id)
            return

        summary_html = None
        if self.cfg.post_summary:
            summary_html = await summarize(
                self.cfg.berget_api_base,
                self.cfg.berget_api_key,
                self.cfg.summary_model,
                ctx.transcript,
            )
        # Fallback: råtranskriptet rakt av om LLM-renskrivningen misslyckas.
        if not summary_html:
            summary_html = "<br>".join(html.escape(line) for line in ctx.transcript)
        plain = f"📄 Mötesanteckningar ({minutes} min)\n\n" + "\n".join(ctx.transcript)
        body = f"📄 <b>Mötesanteckningar</b> ({minutes} min)<br>{summary_html}"
        try:
            await self.matrix.post_message(
                room_id,
                plain,
                html=body,
                thread_root=ctx.thread_root,
            )
        except Exception:
            log.exception("[%s] summary post failed", room_id)

    async def _track_task(self, ctx: CallContext, speaker: str, track) -> None:
        from livekit.rtc import AudioStream

        # AudioStream resamples/decodes internally; output is 24 kHz mono s16 PCM,
        # exactly the format our realtime session is configured for.
        stream = AudioStream(track, sample_rate=24000, num_channels=1)

        async def on_text(sp: str, text: str, final: bool) -> None:
            if not final:
                return
            log.info("[%s] %s: %s", ctx.room_id, sp, text)
            ctx.add(sp, text)
            if self.cfg.post_to_room:
                try:
                    esc = f"<b>{html.escape(sp)}:</b> {html.escape(text)}"
                    await self.matrix.post_message(
                        ctx.room_id,
                        f"{sp}: {text}",
                        html=esc,
                        thread_root=ctx.thread_root,
                    )
                except Exception:
                    log.exception("[%s] post_message failed", ctx.room_id)

        session = RealtimeSession(self.cfg, speaker, on_text)
        try:
            await session.run(stream)
        finally:
            await session.close()
            await stream.aclose()

    async def _speaker_name(self, room_id: str, participant) -> str:
        ident = participant.identity
        if ident.startswith("@"):
            # Legacy token style identity: "@user:server:device"
            base = ident.split(":", 2)[0] + ":" + ident.split(":", 2)[1]
            if base not in self.display_cache:
                self.display_cache[base] = await self.matrix.display_name(room_id, base)
            return self.display_cache[base]
        # Slot-style identity (base64 hash) — no reliable matrix mapping server-side.
        return f"Deltagare {ident[:6]}"


async def _start_health_server(port: int = 8080) -> None:
    """Minimal /health for k8s probes (template convention)."""
    from aiohttp import web

    app = web.Application()
    app.router.add_get("/health", lambda request: web.Response(text="ok"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    bot = Bot()
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
