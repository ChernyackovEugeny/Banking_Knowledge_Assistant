"""Structured observability layer РґР»СЏ chat API.

РџРёС€РµС‚ СЃРѕР±С‹С‚РёСЏ РІ PostgreSQL (С‚Р°Р±Р»РёС†С‹ СЃ РїСЂРµС„РёРєСЃРѕРј chat_).
Р‘Р” РѕР±С‰Р°СЏ СЃ gen_ Рё parse_ С‚Р°Р±Р»РёС†Р°РјРё вЂ” РїСЂРµС„РёРєСЃС‹ РЅРµ РїРµСЂРµСЃРµРєР°СЋС‚СЃСЏ.

РСЃРїРѕР»СЊР·РѕРІР°РЅРёРµ:
    # Р’ lifespan FastAPI (main.py):
    ChatLogger.initialize()
    ...
    ChatLogger.shutdown()

    # Р’ СЂРѕСѓС‚РµСЂРµ вЂ” РїРµСЂРµРґ СЃС‚СЂРёРјРёРЅРіРѕРј:
    request_id = ChatLogger.new_request_id()
    await ChatLogger.log_request_start(request_id, session_id, query, history_len)

    # РџРѕСЃР»Рµ Р·Р°РІРµСЂС€РµРЅРёСЏ СЃС‚СЂРёРјРёРЅРіР° вЂ” fire-and-forget:
    asyncio.create_task(ChatLogger.log_request_end(...))

Р•СЃР»Рё PostgreSQL РЅРµРґРѕСЃС‚СѓРїРµРЅ вЂ” РїРµСЂРµС…РѕРґРёС‚ РІ disabled-СЂРµР¶РёРј, РІСЃРµ РјРµС‚РѕРґС‹ no-op.
Р“РµРЅРµСЂР°С†РёСЏ РѕС‚РІРµС‚Р° РїСЂРё СЌС‚РѕРј РїСЂРѕРґРѕР»Р¶Р°РµС‚СЃСЏ Р±РµР· РёР·РјРµРЅРµРЅРёР№.
"""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL вЂ” РёРґРµРјРїРѕС‚РµРЅС‚РЅРѕ, РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ РїСЂРё СЃС‚Р°СЂС‚Рµ
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS chat_sessions (
    session_id      TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_active_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_count   INTEGER NOT NULL DEFAULT 0,
    error_count     INTEGER NOT NULL DEFAULT 0,
    total_tokens    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chat_requests (
    request_id         TEXT PRIMARY KEY,
    session_id         TEXT NOT NULL REFERENCES chat_sessions(session_id),
    query_text         TEXT NOT NULL,
    response_text      TEXT,
    response_chars     INTEGER,
    retrieved_chunks_n INTEGER,
    history_len        INTEGER,
    status             TEXT NOT NULL DEFAULT 'in_progress',
    error_msg          TEXT,
    requested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at       TIMESTAMPTZ,
    total_duration_ms  DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_chat_req_session
    ON chat_requests (session_id, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_req_status
    ON chat_requests (status, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_req_time
    ON chat_requests (requested_at DESC);

CREATE TABLE IF NOT EXISTS chat_llm_calls (
    call_id           TEXT PRIMARY KEY,
    request_id        TEXT NOT NULL REFERENCES chat_requests(request_id) ON DELETE CASCADE,
    session_id        TEXT NOT NULL,
    model             TEXT NOT NULL,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    total_tokens      INTEGER,
    duration_ms       DOUBLE PRECISION,
    status            TEXT NOT NULL,
    error_msg         TEXT,
    called_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_llm_request
    ON chat_llm_calls (request_id);
CREATE INDEX IF NOT EXISTS idx_chat_llm_session
    ON chat_llm_calls (session_id, called_at DESC);

CREATE TABLE IF NOT EXISTS chat_retrieved_chunks (
    id           BIGSERIAL PRIMARY KEY,
    request_id   TEXT NOT NULL REFERENCES chat_requests(request_id) ON DELETE CASCADE,
    rank         INTEGER NOT NULL,
    doc_id       TEXT NOT NULL,
    chunk_id     TEXT,
    score        DOUBLE PRECISION,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_chunks_request
    ON chat_retrieved_chunks (request_id);
CREATE INDEX IF NOT EXISTS idx_chat_chunks_doc
    ON chat_retrieved_chunks (doc_id);
"""


# ---------------------------------------------------------------------------
# Р’СЃРїРѕРјРѕРіР°С‚РµР»СЊРЅС‹Рµ С„СѓРЅРєС†РёРё
# ---------------------------------------------------------------------------

def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _db_params() -> dict:
    return {
        "host":     os.getenv("POSTGRES_HOST", "localhost"),
        "port":     int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname":   os.getenv("POSTGRES_DB", "banking_assistant"),
        "user":     os.getenv("POSTGRES_USER", "banking_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "changeme"),
    }


# ---------------------------------------------------------------------------
# ChatLogger
# ---------------------------------------------------------------------------

class ChatLogger:
    """Singleton-Р»РѕРіРіРµСЂ РґР»СЏ chat API.

    Р’СЃРµ РїСѓР±Р»РёС‡РЅС‹Рµ РјРµС‚РѕРґС‹ вЂ” async, РЅРѕ РёСЃРїРѕР»РЅСЏСЋС‚ sync DB-РѕРїРµСЂР°С†РёРё
    РІ thread pool С‡РµСЂРµР· run_in_executor, РЅРµ Р±Р»РѕРєРёСЂСѓСЏ event loop.

    РЎРѕРµРґРёРЅРµРЅРёСЏ Р±РµСЂСѓС‚СЃСЏ РёР· ThreadedConnectionPool (minconn=1, maxconn=5),
    С‡С‚Рѕ Р±РµР·РѕРїР°СЃРЅРѕ РїСЂРё РїР°СЂР°Р»Р»РµР»СЊРЅС‹С… Р·Р°РїСЂРѕСЃР°С… Рє API.
    """

    _pool = None          # psycopg2.pool.ThreadedConnectionPool
    _disabled: bool = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @classmethod
    def initialize(cls) -> None:
        """Р’С‹Р·РІР°С‚СЊ РѕРґРёРЅ СЂР°Р· РІ lifespan FastAPI, РґРѕ РЅР°С‡Р°Р»Р° РїСЂРёС‘РјР° Р·Р°РїСЂРѕСЃРѕРІ."""
        try:
            import psycopg2
            from psycopg2.pool import ThreadedConnectionPool

            cls._pool = ThreadedConnectionPool(minconn=1, maxconn=5, **_db_params())
            cls._ensure_schema()
            logger.info("ChatLogger: РїРѕРґРєР»СЋС‡С‘РЅ Рє PostgreSQL")
        except Exception as exc:
            logger.warning(
                "ChatLogger: PostgreSQL РЅРµРґРѕСЃС‚СѓРїРµРЅ, Р»РѕРіРёСЂРѕРІР°РЅРёРµ РѕС‚РєР»СЋС‡РµРЅРѕ (%s)", exc
            )
            cls._disabled = True

    @classmethod
    def shutdown(cls) -> None:
        """Р’С‹Р·РІР°С‚СЊ РІ lifespan FastAPI РїСЂРё РѕСЃС‚Р°РЅРѕРІРєРµ СЃРµСЂРІРµСЂР°."""
        if cls._pool is not None:
            cls._pool.closeall()
            cls._pool = None
            logger.debug("ChatLogger: РїСѓР» СЃРѕРµРґРёРЅРµРЅРёР№ Р·Р°РєСЂС‹С‚")

    @classmethod
    def _ensure_schema(cls) -> None:
        conn = cls._pool.getconn()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(_DDL)
        finally:
            cls._pool.putconn(conn)

    # ------------------------------------------------------------------
    # Public async API
    # ------------------------------------------------------------------

    @classmethod
    def new_request_id(cls) -> str:
        """Р“РµРЅРµСЂРёСЂСѓРµС‚ UUID РґР»СЏ РЅРѕРІРѕРіРѕ Р·Р°РїСЂРѕСЃР°."""
        return _uuid()

    @classmethod
    async def log_request_start(
        cls,
        request_id: str,
        session_id: str,
        query: str,
        history_len: int,
    ) -> None:
        """Р¤РёРєСЃРёСЂСѓРµС‚ РЅР°С‡Р°Р»Рѕ Р·Р°РїСЂРѕСЃР° (status='in_progress').

        Р’С‹Р·С‹РІР°С‚СЊ РґРѕ СЃС‚Р°СЂС‚Р° СЃС‚СЂРёРјРёРЅРіР°, С‡С‚РѕР±С‹ Р·Р°РїСЂРѕСЃ Р±С‹Р» Р·Р°С„РёРєСЃРёСЂРѕРІР°РЅ
        РґР°Р¶Рµ РµСЃР»Рё СЃРµСЂРІРµСЂ СѓРїР°РґС‘С‚ РІ РїСЂРѕС†РµСЃСЃРµ РіРµРЅРµСЂР°С†РёРё.
        """
        if cls._disabled:
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            cls._sync_request_start,
            request_id, session_id, query, history_len,
        )

    @classmethod
    async def log_request_end(
        cls,
        request_id: str,
        session_id: str,
        response_text: str,
        chunks: list,
        model: str | None,
        prompt_tokens: int | None,
        completion_tokens: int | None,
        total_tokens: int | None,
        llm_duration_ms: float | None,
        total_duration_ms: float,
        status: str,
        error_msg: str | None = None,
    ) -> None:
        """Р¤РёРєСЃРёСЂСѓРµС‚ Р·Р°РІРµСЂС€РµРЅРёРµ Р·Р°РїСЂРѕСЃР° СЃРѕ РІСЃРµРјРё РјРµС‚СЂРёРєР°РјРё.

        Р’С‹Р·С‹РІР°С‚СЊ С‡РµСЂРµР· asyncio.create_task() вЂ” fire-and-forget,
        С‡С‚РѕР±С‹ РЅРµ Р·Р°РґРµСЂР¶РёРІР°С‚СЊ РѕС‚РІРµС‚ РєР»РёРµРЅС‚Сѓ.
        """
        if cls._disabled:
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            cls._sync_request_end,
            request_id, session_id, response_text, chunks,
            model, prompt_tokens, completion_tokens, total_tokens,
            llm_duration_ms, total_duration_ms, status, error_msg,
        )

    # ------------------------------------------------------------------
    # Sync internals (РІС‹РїРѕР»РЅСЏСЋС‚СЃСЏ РІ thread pool, РЅРµ РІ event loop)
    # ------------------------------------------------------------------

    @classmethod
    def _sync_request_start(
        cls,
        request_id: str,
        session_id: str,
        query: str,
        history_len: int,
    ) -> None:
        conn = cls._pool.getconn()
        try:
            conn.autocommit = True
            now = _now()
            with conn.cursor() as cur:
                # UPSERT СЃРµСЃСЃРёРё: СЃРѕР·РґР°С‚СЊ РїСЂРё РїРµСЂРІРѕРј Р·Р°РїСЂРѕСЃРµ,
                # РѕР±РЅРѕРІРёС‚СЊ last_active_at РїСЂРё РїРѕСЃР»РµРґСѓСЋС‰РёС…
                cur.execute(
                    """
                    INSERT INTO chat_sessions (session_id, created_at, last_active_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (session_id) DO UPDATE
                        SET last_active_at = EXCLUDED.last_active_at
                    """,
                    (session_id, now, now),
                )
                cur.execute(
                    """
                    INSERT INTO chat_requests
                        (request_id, session_id, query_text, history_len,
                         status, requested_at)
                    VALUES (%s, %s, %s, %s, 'in_progress', %s)
                    """,
                    (request_id, session_id, query, history_len, now),
                )
        except Exception as exc:
            logger.debug("ChatLogger._sync_request_start: %s", exc)
        finally:
            cls._pool.putconn(conn)

    @classmethod
    def _sync_request_end(
        cls,
        request_id: str,
        session_id: str,
        response_text: str,
        chunks: list,
        model: str | None,
        prompt_tokens: int | None,
        completion_tokens: int | None,
        total_tokens: int | None,
        llm_duration_ms: float | None,
        total_duration_ms: float,
        status: str,
        error_msg: str | None,
    ) -> None:
        conn = cls._pool.getconn()
        try:
            conn.autocommit = True
            completed_at = _now()

            with conn.cursor() as cur:
                # РћР±РЅРѕРІР»СЏРµРј Р·Р°РїСЂРѕСЃ
                cur.execute(
                    """
                    UPDATE chat_requests
                    SET response_text      = %s,
                        response_chars     = %s,
                        retrieved_chunks_n = %s,
                        status             = %s,
                        error_msg          = %s,
                        completed_at       = %s,
                        total_duration_ms  = %s
                    WHERE request_id = %s
                    """,
                    (
                        response_text or None,
                        len(response_text) if response_text else None,
                        len(chunks),
                        status,
                        error_msg,
                        completed_at,
                        total_duration_ms,
                        request_id,
                    ),
                )

                # Р”РµС‚Р°Р»Рё LLM-РІС‹Р·РѕРІР°
                if model is not None:
                    cur.execute(
                        """
                        INSERT INTO chat_llm_calls
                            (call_id, request_id, session_id, model,
                             prompt_tokens, completion_tokens, total_tokens,
                             duration_ms, status, error_msg, called_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            _uuid(), request_id, session_id, model,
                            prompt_tokens, completion_tokens, total_tokens,
                            llm_duration_ms,
                            "error" if status == "error" else "ok",
                            error_msg,
                            completed_at,
                        ),
                    )

                # Р§Р°РЅРєРё, РєРѕС‚РѕСЂС‹Рµ РїРѕС€Р»Рё РІ РєРѕРЅС‚РµРєСЃС‚ LLM
                for rank, chunk in enumerate(chunks, 1):
                    cur.execute(
                        """
                        INSERT INTO chat_retrieved_chunks
                            (request_id, rank, doc_id, chunk_id, score)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            request_id,
                            rank,
                            getattr(chunk, "doc_id", ""),
                            getattr(chunk, "chunk_id", None),
                            getattr(chunk, "score", None),
                        ),
                    )

                # РРЅРєСЂРµРјРµРЅС‚РёСЂСѓРµРј СЃС‡С‘С‚С‡РёРєРё СЃРµСЃСЃРёРё
                cur.execute(
                    """
                    UPDATE chat_sessions
                    SET request_count = request_count + 1,
                        error_count   = error_count   + %s,
                        total_tokens  = total_tokens  + %s
                    WHERE session_id = %s
                    """,
                    (
                        1 if status == "error" else 0,
                        total_tokens or 0,
                        session_id,
                    ),
                )

        except Exception as exc:
            logger.debug("ChatLogger._sync_request_end: %s", exc)
        finally:
            cls._pool.putconn(conn)
