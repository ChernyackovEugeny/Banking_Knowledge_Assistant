"""Chat-СЌРЅРґРїРѕРёРЅС‚."""
from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from src.api.services.chat_logger import ChatLogger
from src.api.core.config import get_settings
from src.api.services.history import get_history, append_exchange
from src.retriever import retrieve
from src.llm import stream_response
from src.api.models.schemas import ChatRequest

router = APIRouter(prefix="/api")


@router.post("/chat")
async def chat(request: ChatRequest) -> StreamingResponse:
    settings  = get_settings()
    history   = get_history(request.session_id)
    request_id = ChatLogger.new_request_id()
    chunks    = await retrieve(
        request.message,
        top_k=settings.retrieval_top_k,
        parent_chat_request_id=request_id,
    )

    await ChatLogger.log_request_start(
        request_id,
        request.session_id,
        request.message,
        len(history),
        settings.observability_config_version,
        settings.observability_prompt_version,
        settings.observability_retrieval_version,
    )

    t_start = time.monotonic()

    async def generate():
        full_text = ""
        meta: dict = {}

        async for event in stream_response(history, request.message, chunks):
            yield event

            if event.startswith("data: "):
                try:
                    data = json.loads(event[6:].strip())
                    if data.get("type") == "done":
                        full_text = data.get("full_text", "")
                        meta      = data.get("_meta", {})
                except json.JSONDecodeError:
                    pass

        total_duration_ms = (time.monotonic() - t_start) * 1000
        status = "error" if meta.get("error") else "ok"

        # РЎРѕС…СЂР°РЅСЏРµРј РґРёР°Р»РѕРіРѕРІСѓСЋ РёСЃС‚РѕСЂРёСЋ
        if full_text:
            append_exchange(
                request.session_id,
                request.message,
                full_text,
                window=settings.history_window,
            )

        # Р›РѕРіРёСЂСѓРµРј Р·Р°РІРµСЂС€РµРЅРёРµ Р·Р°РїСЂРѕСЃР° вЂ” РЅРµ Р±Р»РѕРєРёСЂСѓРµРј РѕС‚РІРµС‚ РєР»РёРµРЅС‚Сѓ
        asyncio.create_task(
            ChatLogger.log_request_end(
                request_id       = request_id,
                session_id       = request.session_id,
                response_text    = full_text,
                chunks           = chunks,
                model            = meta.get("model"),
                prompt_tokens    = meta.get("prompt_tokens"),
                completion_tokens= meta.get("completion_tokens"),
                total_tokens     = meta.get("total_tokens"),
                llm_duration_ms  = meta.get("llm_duration_ms"),
                total_duration_ms= round(total_duration_ms, 1),
                status           = status,
                error_msg        = meta.get("error"),
            )
        )

    return StreamingResponse(generate(), media_type="text/event-stream")
