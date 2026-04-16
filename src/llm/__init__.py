"""LLM-РєР»РёРµРЅС‚: СЃС‚СЂРёРјРёРЅРі РѕС‚РІРµС‚Р° С‡РµСЂРµР· DeepSeek (OpenAI-СЃРѕРІРјРµСЃС‚РёРјС‹Р№ API).

Р¤РѕСЂРјР°С‚ SSE-СЃРѕР±С‹С‚РёР№, РєРѕС‚РѕСЂС‹Рµ РІС‹РґР°С‘С‚ stream_response():
  data: {"type": "sources", "sources": [...]}
      вЂ” СЂРµР»РµРІР°РЅС‚РЅС‹Рµ С‡Р°РЅРєРё (РµСЃР»Рё РµСЃС‚СЊ), РѕС‚РїСЂР°РІР»СЏРµС‚СЃСЏ РґРѕ РЅР°С‡Р°Р»Р° СЃС‚СЂРёРјР°
  data: {"type": "delta", "content": "..."}
      вЂ” С‚РѕРєРµРЅ/С„СЂР°РіРјРµРЅС‚ РѕС‚РІРµС‚Р°
  data: {"type": "done", "full_text": "...", "_meta": {...}}
      вЂ” РєРѕРЅРµС† СЃС‚СЂРёРјР°; _meta СЃРѕРґРµСЂР¶РёС‚ model, С‚РѕРєРµРЅС‹, llm_duration_ms РґР»СЏ Р»РѕРіРіРµСЂР°
        (UI _meta РЅРµ РёСЃРїРѕР»СЊР·СѓРµС‚ вЂ” РїРѕР»Рµ РЅР°С‡РёРЅР°РµС‚СЃСЏ СЃ "_")
"""
from __future__ import annotations

import json
import time
from typing import AsyncIterator

from openai import AsyncOpenAI

from src.api.core.config import get_settings
from src.retriever import RetrievedChunk

SYSTEM_PROMPT = """РўС‹ вЂ” РР-Р°СЃСЃРёСЃС‚РµРЅС‚ РџРђРћ В«РРЅРІРµСЃС‚Р‘Р°РЅРєВ» РґР»СЏ СЃРѕС‚СЂСѓРґРЅРёРєРѕРІ Р±Р»РѕРєР° РљРР‘ \
(РєРѕСЂРїРѕСЂР°С‚РёРІРЅС‹Р№ Рё РёРЅРІРµСЃС‚РёС†РёРѕРЅРЅС‹Р№ Р±Р°РЅРєРёРЅРі).

РўС‹ РїРѕРјРѕРіР°РµС€СЊ РѕСЂРёРµРЅС‚РёСЂРѕРІР°С‚СЊСЃСЏ РІРѕ РІРЅСѓС‚СЂРµРЅРЅРёС… СЂРµРіР»Р°РјРµРЅС‚Р°С…, РЅРѕСЂРјР°С‚РёРІРЅС‹С… Р°РєС‚Р°С… Р‘Р°РЅРєР° Р РѕСЃСЃРёРё, \
РїСЂРѕС†РµРґСѓСЂР°С… РєРѕРјРїР»Р°РµРЅСЃР°, РєСЂРµРґРёС‚РѕРІР°РЅРёСЏ, РѕРїРµСЂР°С†РёР№ СЃ С†РµРЅРЅС‹РјРё Р±СѓРјР°РіР°РјРё Рё РґСЂСѓРіРѕР№ РґРѕРєСѓРјРµРЅС‚Р°С†РёРё Р±Р°РЅРєР°.

РџСЂР°РІРёР»Р°:
- РћС‚РІРµС‡Р°Р№ СЃС‚СЂРѕРіРѕ РїРѕ СЃСѓС‰РµСЃС‚РІСѓ РІРѕРїСЂРѕСЃР°, Р»Р°РєРѕРЅРёС‡РЅРѕ Рё С‚РѕС‡РЅРѕ
- РСЃРїРѕР»СЊР·СѓР№ РїСЂРѕС„РµСЃСЃРёРѕРЅР°Р»СЊРЅСѓСЋ Р±Р°РЅРєРѕРІСЃРєСѓСЋ Р»РµРєСЃРёРєСѓ
- Р•СЃР»Рё РїСЂРµРґРѕСЃС‚Р°РІР»РµРЅС‹ С„СЂР°РіРјРµРЅС‚С‹ РґРѕРєСѓРјРµРЅС‚РѕРІ вЂ” РѕРїРёСЂР°Р№СЃСЏ РЅР° РЅРёС… Рё СѓРєР°Р·С‹РІР°Р№ РёСЃС‚РѕС‡РЅРёРєРё
- Р•СЃР»Рё С‚РѕС‡РЅРѕРіРѕ РѕС‚РІРµС‚Р° РЅРµС‚ РІ РїСЂРµРґРѕСЃС‚Р°РІР»РµРЅРЅС‹С… РґРѕРєСѓРјРµРЅС‚Р°С… вЂ” СЃРєР°Р¶Рё РѕР± СЌС‚РѕРј РїСЂСЏРјРѕ
- РћС‚РІРµС‡Р°Р№ РЅР° СЂСѓСЃСЃРєРѕРј СЏР·С‹РєРµ"""


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _build_context(chunks: list[RetrievedChunk]) -> str:
    if not chunks:
        return ""
    parts = ["\n\n--- Р РµР»РµРІР°РЅС‚РЅС‹Рµ С„СЂР°РіРјРµРЅС‚С‹ РґРѕРєСѓРјРµРЅС‚РѕРІ ---"]
    for i, c in enumerate(chunks, 1):
        header = f"[{i}] {c.doc_title or c.doc_id}"
        if c.section_title:
            header += f" / {c.section_title}"
        parts.append(f"\n{header}\n{c.text}")
    parts.append("--- РљРѕРЅРµС† С„СЂР°РіРјРµРЅС‚РѕРІ ---")
    return "\n".join(parts)


async def stream_response(
    history: list[dict],
    user_message: str,
    retrieved_chunks: list[RetrievedChunk],
) -> AsyncIterator[str]:
    """Р“РµРЅРµСЂРёСЂСѓРµС‚ SSE-СЃРѕР±С‹С‚РёСЏ РґР»СЏ СЃС‚СЂРёРјРёРЅРіР° РѕС‚РІРµС‚Р° LLM."""
    settings = get_settings()

    if not settings.deepseek_api_key:
        error_text = (
            "API-РєР»СЋС‡ DeepSeek РЅРµ РЅР°СЃС‚СЂРѕРµРЅ. "
            "РЈРєР°Р¶РёС‚Рµ DEEPSEEK_API_KEY РІ С„Р°Р№Р»Рµ .env Рё РїРµСЂРµР·Р°РїСѓСЃС‚РёС‚Рµ СЃРµСЂРІРµСЂ."
        )
        yield _sse({"type": "delta",   "content":   error_text})
        yield _sse({"type": "done",    "full_text": error_text})
        return

    # РСЃС‚РѕС‡РЅРёРєРё РѕС‚РґР°С‘Рј РґРѕ СЃС‚СЂРёРјР°, С‡С‚РѕР±С‹ UI РјРѕРі РїРѕРєР°Р·Р°С‚СЊ РёС… СЃСЂР°Р·Сѓ
    if retrieved_chunks:
        sources = [
            {
                "doc_id":  c.doc_id,
                "title":   c.doc_title or c.doc_id,
                "section": c.section_title,
                "score":   round(c.score, 4),
            }
            for c in retrieved_chunks
        ]
        yield _sse({"type": "sources", "sources": sources})

    system_content = SYSTEM_PROMPT + _build_context(retrieved_chunks)
    messages: list[dict] = [{"role": "system", "content": system_content}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_message})

    client = AsyncOpenAI(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
    )

    collected: list[str] = []
    model_used: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    llm_error: str | None = None

    t0 = time.monotonic()
    try:
        stream = await client.chat.completions.create(
            model=settings.deepseek_model,
            messages=messages,
            stream=True,
            stream_options={"include_usage": True},
        )
        async for chunk in stream:
            # Р¤РёРЅР°Р»СЊРЅС‹Р№ С‡Р°РЅРє СЃ usage (choices РїСѓСЃС‚РѕР№)
            if not chunk.choices:
                if chunk.usage is not None:
                    prompt_tokens     = chunk.usage.prompt_tokens
                    completion_tokens = chunk.usage.completion_tokens
                    total_tokens      = chunk.usage.total_tokens
                if chunk.model:
                    model_used = chunk.model
                continue

            if model_used is None and chunk.model:
                model_used = chunk.model

            delta = chunk.choices[0].delta.content or ""
            if delta:
                collected.append(delta)
                yield _sse({"type": "delta", "content": delta})

    except Exception as exc:
        llm_error = str(exc)
        error_text = f"РћС€РёР±РєР° РїСЂРё РѕР±СЂР°С‰РµРЅРёРё Рє LLM: {exc}"
        collected = [error_text]
        yield _sse({"type": "delta", "content": error_text})

    llm_duration_ms = (time.monotonic() - t0) * 1000

    yield _sse({
        "type":      "done",
        "full_text": "".join(collected),
        "_meta": {
            "model":             model_used or settings.deepseek_model,
            "prompt_tokens":     prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens":      total_tokens,
            "llm_duration_ms":   round(llm_duration_ms, 1),
            "error":             llm_error,
        },
    })
