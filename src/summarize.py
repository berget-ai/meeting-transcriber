"""Renskrivning: renskrivet mötesprotokoll från råtranskript via Berget LLM API.

Körs när ett samtal anses slut (rummet tomt i 60 s). Om LLM-anropet misslyckas
returneras None och anroparen postar råtranskriptet som fallback istället.
"""
from __future__ import annotations

import logging

import aiohttp

log = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Du är en mötesassistent. Du får ett rått, tidsordnat transkript från ett "
    "videomöte med talarnamn i fetstil. Transkriptet är maskinellt och kan vara "
    "hackigt eller sakna bitar. Skriv svaret i tre avsnitt med HTML-rubriker:\n"
    "1) <b>Sammanfattning</b> – vad mötet handlade om, i några meningar.\n"
    "2) <b>Beslut</b> – punktlista (eller en mening om inga beslut framgår).\n"
    "3) <b>Åtgärder</b> – punktlista med vem/vad om det framgår, annars utelämnas.\n"
    "Svara på samma språk som transkriptet. Hitta inte på innehåll som saknas."
)

# Cap what we send: 60k chars always fits our models' context.
MAX_TRANSCRIPT_CHARS = 60_000


async def summarize(
    api_base: str, api_key: str, model: str, lines: list[str]
) -> str | None:
    transcript = "\n".join(lines)
    if len(transcript) > MAX_TRANSCRIPT_CHARS:
        transcript = "…\n" + transcript[-MAX_TRANSCRIPT_CHARS:]
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": transcript},
        ],
        "temperature": 0.3,
    }
    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.post(
                f"{api_base}/v1/chat/completions",
                json=payload,
                headers={"Authorization": f"Bearer {api_key}"},
            ) as resp:
                if resp.status != 200:
                    log.warning(
                        "summarizer http %s: %s", resp.status, (await resp.text())[:300]
                    )
                    return None
                data = await resp.json()
                return data["choices"][0]["message"]["content"]
    except Exception:
        log.exception("summarizer failed")
        return None
