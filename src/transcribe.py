"""One wss://api.berget.ai/v1/realtime transcription session per remote audio track.

Per OpenAI Realtime GA shape (see Berget docs), server-side VAD commits turns
and we receive transcript deltas + completions in strict audio order.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
from typing import Awaitable, Callable

import websockets

from .config import Config

log = logging.getLogger(__name__)

OnText = Callable[[str, str, bool], Awaitable[None]]  # speaker, text, final


class RealtimeSession:
    def __init__(self, cfg: Config, speaker: str, on_text: OnText, on_audio=None):
        self.cfg = cfg
        self.speaker = speaker
        self.on_text = on_text
        # Heartbeat hook (CallRoom.touch): invoked every pump-log tick while
        # audio frames flow, so the zombie-room watchdog can tell live media
        # from a dead room instance.
        self.on_audio = on_audio
        self._ws = None
        self._closed = False
        self._transcript_parts: list[str] = []

    async def run(self, audio_stream):
        """Consume PCM frames (24 kHz mono s16) from a livekit AudioStream."""
        url = f"{self.cfg.realtime_url}?intent=transcription"
        headers = {"Authorization": f"Bearer {self.cfg.berget_api_key}"}
        async for ws in websockets.connect(
            url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=30,
            max_size=4_000_000,
            close_timeout=5,
        ):
            try:
                self._ws = ws
                await self._configure()
                recv_task = asyncio.ensure_future(self._receive())
                try:
                    await self._pump(audio_stream)
                finally:
                    recv_task.cancel()
                if self._closed:
                    return
            except websockets.ConnectionClosed as exc:
                log.warning("[%s] ws closed (%s); reconnecting", self.speaker, exc)
                await asyncio.sleep(2)
            except Exception:
                log.exception("[%s] realtime session error; reconnecting", self.speaker)
                await asyncio.sleep(5)

    async def close(self):
        self._closed = True
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    async def _configure(self) -> None:
        await self._ws.send(
            json.dumps(
                {
                    "type": "session.update",
                    "session": {
                        "type": "transcription",
                        "audio": {
                            "input": {
                                "format": {"type": "audio/pcm", "rate": 24000},
                                "transcription": {
                                    "model": self.cfg.model,
                                    "languages": self.cfg.languages,
                                    "chunk_seconds": self.cfg.chunk_seconds,
                                },
                                "turn_detection": {"type": "server_vad"},
                            }
                        },
                    },
                }
            )
        )

    async def _pump(self, audio_stream) -> None:
        # Send whole AudioFrames as they arrive (~10 ms each); any chunking works.
        # Throughput logging (every ~5 s) is our canary for silent/encrypted tracks.
        pushed = 0
        last_log = asyncio.get_running_loop().time()
        async for event in audio_stream:
            if self._closed:
                return
            data = bytes(event.frame.data)
            if not data:
                continue
            pushed += len(data)
            now = asyncio.get_running_loop().time()
            if now - last_log >= 5:
                log.info("[%s] realtime pump: %d PCM bytes pushed / 5 s", self.speaker, pushed)
                pushed = 0
                last_log = now
                if self.on_audio:
                    self.on_audio()
            await self._ws.send(
                json.dumps(
                    {
                        "type": "input_audio_buffer.append",
                        "audio": base64.b64encode(data).decode(),
                    }
                )
            )

    async def _receive(self) -> None:
        async for raw in self._ws:
            msg = json.loads(raw)
            mtype = msg.get("type", "")
            if mtype == "conversation.item.input_audio_transcription.delta":
                self._transcript_parts.append(msg.get("delta", ""))
                if msg.get("delta"):
                    await self.on_text(self.speaker, msg["delta"], False)
            elif mtype == "conversation.item.input_audio_transcription.completed":
                text = msg.get("transcript") or "".join(self._transcript_parts)
                self._transcript_parts.clear()
                log.info("[%s] completed -> %r", self.speaker, text.strip())
                if text.strip():
                    await self.on_text(self.speaker, text.strip(), True)
            elif mtype in ("session.created", "session.updated", "conversation.created"):
                log.info("[%s] %s", self.speaker, mtype)
            elif mtype in ("input_audio_buffer.speech_started", "input_audio_buffer.speech_stopped"):
                log.info("[%s] VAD %s", self.speaker, mtype.split(".")[-1])
            elif mtype == "conversation.item.input_audio_transcription.failed":
                log.error("[%s] transcription FAILED: %s", self.speaker, msg)
            elif mtype == "error":
                log.error("[%s] realtime error: %s", self.speaker, msg.get("error", msg))
