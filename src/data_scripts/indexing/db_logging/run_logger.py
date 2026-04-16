"""Structured observability layer для пайплайна индексации.

Пишет события в PostgreSQL (таблицы с префиксом idx_).
БД общая с остальными модулями — префиксы не пересекаются.

Если PostgreSQL недоступен при старте — логирует WARNING и переходит
в disabled-режим: все методы становятся no-op, индексация продолжается.

Использование:
    with RunLogger(args) as run_log:
        run_log.log_doc_result(doc_id, cluster="compliance", status="ok",
                               chunks_indexed=87)
        run_log.finalize(docs_total=13, docs_ok=12, docs_failed=1,
                         docs_skipped=0, chunks_total=13477)
"""
from __future__ import annotations

import json
import logging
import os
import platform
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv

# db_logging/ → indexing/ → data_scripts/ → src/ → root (4 уровня)
load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS idx_sessions (
    session_id       TEXT PRIMARY KEY,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    duration_sec     DOUBLE PRECISION,
    flag_force       BOOLEAN NOT NULL DEFAULT FALSE,
    flag_only        TEXT,
    flag_cluster     TEXT,
    flag_source      TEXT,
    flag_skip_embed  BOOLEAN NOT NULL DEFAULT FALSE,
    flag_skip_bm25   BOOLEAN NOT NULL DEFAULT FALSE,
    log_level        TEXT NOT NULL DEFAULT 'INFO',
    docs_total       INTEGER NOT NULL DEFAULT 0,
    docs_ok          INTEGER NOT NULL DEFAULT 0,
    docs_failed      INTEGER NOT NULL DEFAULT 0,
    docs_skipped     INTEGER NOT NULL DEFAULT 0,
    chunks_total     INTEGER NOT NULL DEFAULT 0,
    python_version   TEXT,
    hostname         TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_started
    ON idx_sessions (started_at DESC);

CREATE TABLE IF NOT EXISTS idx_doc_results (
    result_id        TEXT PRIMARY KEY,
    session_id       TEXT NOT NULL REFERENCES idx_sessions(session_id) ON DELETE CASCADE,
    doc_id           TEXT NOT NULL,
    cluster          TEXT,
    source_type      TEXT,
    status           TEXT NOT NULL,
    chunks_indexed   INTEGER,
    error_msg        TEXT,
    processed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_doc_session
    ON idx_doc_results (session_id, doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_status
    ON idx_doc_results (status, doc_id);
"""


# ---------------------------------------------------------------------------
# Вспомогательные функции
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
# RunLogger
# ---------------------------------------------------------------------------

class RunLogger:
    """Пишет события индексации в PostgreSQL.

    При недоступности БД переходит в disabled-режим (все методы — no-op).
    """

    def __init__(self, args=None) -> None:
        self._args = args
        self.session_id: str = _uuid()
        self._conn: psycopg2.extensions.connection | None = None
        self._disabled: bool = False
        self._session_started_at: datetime | None = None
        self._final_stats: dict = {}

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "RunLogger":
        try:
            self._conn = psycopg2.connect(**_db_params())
            self._conn.autocommit = True
            self._init_db()
            self._start_session()
            logger.debug("RunLogger [indexing]: сессия %s создана", self.session_id)
        except psycopg2.OperationalError as exc:
            logger.warning(
                "RunLogger [indexing]: PostgreSQL недоступен, "
                "DB-логирование отключено (%s)", exc
            )
            self._disabled = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if not self._disabled:
            try:
                s = self._final_stats
                self._end_session(
                    docs_total=s.get("docs_total", 0),
                    docs_ok=s.get("docs_ok", 0),
                    docs_failed=s.get("docs_failed", 0),
                    docs_skipped=s.get("docs_skipped", 0),
                    chunks_total=s.get("chunks_total", 0),
                )
            except Exception as exc:
                logger.warning("RunLogger [indexing]: ошибка при финализации: %s", exc)
            finally:
                if self._conn:
                    self._conn.close()
                    self._conn = None
        return False

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(_DDL)

    # ------------------------------------------------------------------
    # Сессия
    # ------------------------------------------------------------------

    def _start_session(self) -> None:
        self._session_started_at = _now()
        args = self._args
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO idx_sessions
                    (session_id, started_at, flag_force, flag_only, flag_cluster,
                     flag_source, flag_skip_embed, flag_skip_bm25,
                     log_level, python_version, hostname)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    self.session_id,
                    self._session_started_at,
                    bool(getattr(args, "force", False)),
                    json.dumps(getattr(args, "only", None)),
                    getattr(args, "cluster", None),
                    getattr(args, "source", "all"),
                    bool(getattr(args, "skip_embeddings", False)),
                    bool(getattr(args, "skip_bm25", False)),
                    getattr(args, "log_level", "INFO"),
                    sys.version.split()[0],
                    platform.node(),
                ),
            )

    def _end_session(
        self,
        docs_total: int = 0,
        docs_ok: int = 0,
        docs_failed: int = 0,
        docs_skipped: int = 0,
        chunks_total: int = 0,
    ) -> None:
        finished_at = _now()
        duration_sec = (
            (finished_at - self._session_started_at).total_seconds()
            if self._session_started_at
            else None
        )
        with self._conn.cursor() as cur:
            cur.execute(
                """
                UPDATE idx_sessions
                SET finished_at=%s, duration_sec=%s,
                    docs_total=%s, docs_ok=%s, docs_failed=%s, docs_skipped=%s,
                    chunks_total=%s
                WHERE session_id=%s
                """,
                (
                    finished_at, duration_sec,
                    docs_total, docs_ok, docs_failed, docs_skipped,
                    chunks_total,
                    self.session_id,
                ),
            )

    def finalize(
        self,
        docs_total: int,
        docs_ok: int,
        docs_failed: int,
        docs_skipped: int,
        chunks_total: int = 0,
    ) -> None:
        """Вызвать из main() перед выходом из with-блока."""
        self._final_stats = {
            "docs_total":   docs_total,
            "docs_ok":      docs_ok,
            "docs_failed":  docs_failed,
            "docs_skipped": docs_skipped,
            "chunks_total": chunks_total,
        }

    # ------------------------------------------------------------------
    # Результат по документу
    # ------------------------------------------------------------------

    def log_doc_result(
        self,
        doc_id: str,
        *,
        cluster: str,
        source_type: str,
        status: str,
        chunks_indexed: int | None = None,
        error_msg: str | None = None,
    ) -> None:
        """Записывает результат индексации одного документа."""
        if self._disabled:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idx_doc_results
                        (result_id, session_id, doc_id, cluster, source_type,
                         status, chunks_indexed, error_msg, processed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _uuid(), self.session_id,
                        doc_id, cluster, source_type, status,
                        chunks_indexed, error_msg,
                        _now(),
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.log_doc_result: %s", exc)
