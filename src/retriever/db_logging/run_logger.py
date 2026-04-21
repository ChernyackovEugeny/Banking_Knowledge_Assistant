"""Structured observability for retriever runtime."""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env")

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS ret_requests (
    request_id            TEXT PRIMARY KEY,
    query_text            TEXT NOT NULL,
    cluster               TEXT,
    top_k                 INTEGER NOT NULL,
    candidates            INTEGER NOT NULL,
    semantic_hits         INTEGER NOT NULL DEFAULT 0,
    bm25_hits             INTEGER NOT NULL DEFAULT 0,
    fused_hits            INTEGER NOT NULL DEFAULT 0,
    result_hits           INTEGER NOT NULL DEFAULT 0,
    bm25_missing_ids      INTEGER NOT NULL DEFAULT 0,
    bm25_fallback         BOOLEAN NOT NULL DEFAULT FALSE,
    semantic_duration_ms  DOUBLE PRECISION,
    bm25_duration_ms      DOUBLE PRECISION,
    total_duration_ms     DOUBLE PRECISION,
    status                TEXT NOT NULL DEFAULT 'ok',
    error_msg             TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ret_requests_time
    ON ret_requests (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ret_requests_status
    ON ret_requests (status, created_at DESC);

CREATE TABLE IF NOT EXISTS ret_chunks (
    id           BIGSERIAL PRIMARY KEY,
    request_id   TEXT NOT NULL REFERENCES ret_requests(request_id) ON DELETE CASCADE,
    rank         INTEGER NOT NULL,
    chunk_id     TEXT,
    doc_id       TEXT,
    cluster      TEXT,
    score        DOUBLE PRECISION,
    source       TEXT,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ret_chunks_request
    ON ret_chunks (request_id);
CREATE INDEX IF NOT EXISTS idx_ret_chunks_doc
    ON ret_chunks (doc_id);
"""


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _db_params() -> dict[str, Any]:
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "banking_assistant"),
        "user": os.getenv("POSTGRES_USER", "banking_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "changeme"),
    }


class RetrieverLogger:
    """Singleton logger for retrieval metrics."""

    _pool = None
    _disabled: bool = False

    @classmethod
    def initialize(cls) -> None:
        try:
            from psycopg2.pool import ThreadedConnectionPool

            cls._pool = ThreadedConnectionPool(minconn=1, maxconn=5, **_db_params())
            cls._ensure_schema()
            cls._disabled = False
            logger.info("RetrieverLogger: connected to PostgreSQL")
        except Exception as exc:
            logger.warning(
                "RetrieverLogger: PostgreSQL unavailable, DB logging disabled (%s)",
                exc,
            )
            cls._disabled = True

    @classmethod
    def shutdown(cls) -> None:
        if cls._pool is not None:
            cls._pool.closeall()
            cls._pool = None

    @classmethod
    def _ensure_schema(cls) -> None:
        conn = cls._pool.getconn()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(_DDL)
        finally:
            cls._pool.putconn(conn)

    @classmethod
    def new_request_id(cls) -> str:
        return _uuid()

    @classmethod
    async def log_request(
        cls,
        *,
        request_id: str,
        query_text: str,
        cluster: str | None,
        top_k: int,
        candidates: int,
        semantic_hits: int,
        bm25_hits: int,
        fused_hits: int,
        result_hits: int,
        bm25_missing_ids: int,
        bm25_fallback: bool,
        semantic_duration_ms: float | None,
        bm25_duration_ms: float | None,
        total_duration_ms: float,
        status: str,
        error_msg: str | None,
        chunks: list,
        source: str,
    ) -> None:
        if cls._disabled or cls._pool is None:
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            cls._sync_log_request,
            request_id,
            query_text,
            cluster,
            top_k,
            candidates,
            semantic_hits,
            bm25_hits,
            fused_hits,
            result_hits,
            bm25_missing_ids,
            bm25_fallback,
            semantic_duration_ms,
            bm25_duration_ms,
            total_duration_ms,
            status,
            error_msg,
            chunks,
            source,
        )

    @classmethod
    def _sync_log_request(
        cls,
        request_id: str,
        query_text: str,
        cluster: str | None,
        top_k: int,
        candidates: int,
        semantic_hits: int,
        bm25_hits: int,
        fused_hits: int,
        result_hits: int,
        bm25_missing_ids: int,
        bm25_fallback: bool,
        semantic_duration_ms: float | None,
        bm25_duration_ms: float | None,
        total_duration_ms: float,
        status: str,
        error_msg: str | None,
        chunks: list,
        source: str,
    ) -> None:
        conn = cls._pool.getconn()
        try:
            conn.autocommit = True
            now = _now()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ret_requests
                        (request_id, query_text, cluster, top_k, candidates,
                         semantic_hits, bm25_hits, fused_hits, result_hits, bm25_missing_ids,
                         bm25_fallback, semantic_duration_ms, bm25_duration_ms, total_duration_ms,
                         status, error_msg, created_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        request_id,
                        query_text,
                        cluster,
                        top_k,
                        candidates,
                        semantic_hits,
                        bm25_hits,
                        fused_hits,
                        result_hits,
                        bm25_missing_ids,
                        bm25_fallback,
                        semantic_duration_ms,
                        bm25_duration_ms,
                        total_duration_ms,
                        status,
                        error_msg,
                        now,
                    ),
                )

                for rank, chunk in enumerate(chunks, 1):
                    cur.execute(
                        """
                        INSERT INTO ret_chunks
                            (request_id, rank, chunk_id, doc_id, cluster, score, source, retrieved_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            request_id,
                            rank,
                            getattr(chunk, "chunk_id", None),
                            getattr(chunk, "doc_id", None),
                            getattr(chunk, "cluster", None),
                            getattr(chunk, "score", None),
                            source,
                            now,
                        ),
                    )
        except Exception as exc:
            logger.debug("RetrieverLogger._sync_log_request: %s", exc)
        finally:
            cls._pool.putconn(conn)
