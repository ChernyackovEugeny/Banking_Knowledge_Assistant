"""Structured observability layer для пайплайна парсинга.

Пишет события в PostgreSQL (таблицы с префиксом parse_).
БД является общей для всех модулей проекта.

Если PostgreSQL недоступен при старте — логирует WARNING и переходит
в disabled-режим: все методы становятся no-op, парсинг продолжается.

Использование:
    with RunLogger(args) as run_log:
        op_id = run_log.start_operation(doc_id, doc_meta)
        run_log.log_http_request(url, ...)
        run_log.log_source_attempt(op_id, doc_id, ...)
        run_log.end_operation(op_id, status='ok', ...)
        run_log.log_alias_resolution(op_id, doc_id, ...)
        run_log.finalize(docs_total=N, docs_ok=M, ...)
    # __exit__ финализирует сессию и закрывает соединение
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
from urllib.parse import urlparse

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv

# Ищем .env вверх по дереву от этого файла (корень проекта)
load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL — создаётся при первом запуске, идемпотентно
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS parse_sessions (
    session_id          TEXT PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    duration_sec        DOUBLE PRECISION,
    flag_force          BOOLEAN NOT NULL DEFAULT FALSE,
    flag_fetch_force    BOOLEAN NOT NULL DEFAULT FALSE,
    flag_only_filter    TEXT,
    log_level           TEXT NOT NULL DEFAULT 'INFO',
    docs_total          INTEGER NOT NULL DEFAULT 0,
    docs_ok             INTEGER NOT NULL DEFAULT 0,
    docs_failed         INTEGER NOT NULL DEFAULT 0,
    docs_skipped        INTEGER NOT NULL DEFAULT 0,
    python_version      TEXT,
    hostname            TEXT
);

CREATE INDEX IF NOT EXISTS idx_parse_sessions_started
    ON parse_sessions (started_at DESC);

CREATE TABLE IF NOT EXISTS parse_operations (
    op_id               TEXT PRIMARY KEY,
    session_id          TEXT NOT NULL REFERENCES parse_sessions(session_id) ON DELETE CASCADE,
    doc_id              TEXT NOT NULL,
    doc_subtype         TEXT,
    short_title         TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    duration_sec        DOUBLE PRECISION,
    status              TEXT NOT NULL,
    winning_source_url  TEXT,
    winning_extractor   TEXT,
    winning_parser      TEXT,
    sections_total      INTEGER,
    sections_saved      INTEGER,
    wanted_count        INTEGER,
    sources_tried       INTEGER NOT NULL DEFAULT 0,
    error_msg           TEXT
);

CREATE INDEX IF NOT EXISTS idx_parse_ops_session
    ON parse_operations (session_id, doc_id);
CREATE INDEX IF NOT EXISTS idx_parse_ops_status
    ON parse_operations (status, doc_id);
CREATE INDEX IF NOT EXISTS idx_parse_ops_started
    ON parse_operations (started_at DESC);

CREATE TABLE IF NOT EXISTS parse_http_requests (
    req_id          TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES parse_sessions(session_id) ON DELETE CASCADE,
    doc_id          TEXT,
    url             TEXT NOT NULL,
    http_attempt    INTEGER NOT NULL DEFAULT 1,
    cached          BOOLEAN NOT NULL DEFAULT FALSE,
    status_code     INTEGER,
    response_bytes  INTEGER,
    duration_ms     DOUBLE PRECISION,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error_msg       TEXT,
    domain          TEXT
);

CREATE INDEX IF NOT EXISTS idx_parse_http_session
    ON parse_http_requests (session_id);
CREATE INDEX IF NOT EXISTS idx_parse_http_domain_cached
    ON parse_http_requests (domain, cached);
CREATE INDEX IF NOT EXISTS idx_parse_http_doc_id
    ON parse_http_requests (doc_id) WHERE doc_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS parse_source_attempts (
    attempt_id      TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES parse_sessions(session_id) ON DELETE CASCADE,
    op_id           TEXT NOT NULL REFERENCES parse_operations(op_id) ON DELETE CASCADE,
    doc_id          TEXT NOT NULL,
    source_url      TEXT NOT NULL,
    extractor_cls   TEXT NOT NULL,
    attempt_num     INTEGER NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    duration_ms     DOUBLE PRECISION,
    status          TEXT NOT NULL,
    error_type      TEXT,
    error_msg       TEXT,
    sections_count  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_parse_src_op
    ON parse_source_attempts (op_id, attempt_num);
CREATE INDEX IF NOT EXISTS idx_parse_src_status
    ON parse_source_attempts (status, source_url);
CREATE INDEX IF NOT EXISTS idx_parse_src_doc
    ON parse_source_attempts (doc_id, status);

CREATE TABLE IF NOT EXISTS parse_alias_resolutions (
    res_id          TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES parse_sessions(session_id) ON DELETE CASCADE,
    op_id           TEXT NOT NULL REFERENCES parse_operations(op_id) ON DELETE CASCADE,
    doc_id          TEXT NOT NULL,
    wanted_id       TEXT NOT NULL,
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    strategy        TEXT NOT NULL,
    text_length     INTEGER,
    resolved_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_parse_alias_op
    ON parse_alias_resolutions (op_id);
CREATE INDEX IF NOT EXISTS idx_parse_alias_resolved
    ON parse_alias_resolutions (resolved, doc_id);
CREATE INDEX IF NOT EXISTS idx_parse_alias_wanted
    ON parse_alias_resolutions (wanted_id, doc_id);
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
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "banking_assistant"),
        "user": os.getenv("POSTGRES_USER", "banking_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "changeme"),
    }


# ---------------------------------------------------------------------------
# RunLogger
# ---------------------------------------------------------------------------

class RunLogger:
    """Пишет события парсинга в PostgreSQL.

    При недоступности БД переходит в disabled-режим (все методы — no-op).
    """

    def __init__(self, args=None):
        """
        Args:
            args: argparse.Namespace из parsing.py или None.
        """
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
            logger.debug("RunLogger: сессия %s создана", self.session_id)
        except psycopg2.OperationalError as exc:
            logger.warning(
                "RunLogger: PostgreSQL недоступен, DB-логирование отключено (%s)", exc
            )
            self._disabled = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if not self._disabled:
            try:
                stats = self._final_stats
                self._end_session(
                    docs_total=stats.get("docs_total", 0),
                    docs_ok=stats.get("docs_ok", 0),
                    docs_failed=stats.get("docs_failed", 0),
                    docs_skipped=stats.get("docs_skipped", 0),
                )
            except Exception as exc:
                logger.warning("RunLogger: ошибка при финализации сессии: %s", exc)
            finally:
                if self._conn:
                    self._conn.close()
                    self._conn = None
        return False

    # ------------------------------------------------------------------
    # Инициализация схемы
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        """Создаёт таблицы если не существуют. Идемпотентно."""
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
                INSERT INTO parse_sessions
                    (session_id, started_at, flag_force, flag_fetch_force,
                     flag_only_filter, log_level, python_version, hostname)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    self.session_id,
                    self._session_started_at,
                    bool(getattr(args, "force", False)),
                    bool(getattr(args, "fetch_force", False)),
                    json.dumps(args.only) if getattr(args, "only", None) else None,
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
                UPDATE parse_sessions
                SET finished_at=%s, duration_sec=%s,
                    docs_total=%s, docs_ok=%s, docs_failed=%s, docs_skipped=%s
                WHERE session_id=%s
                """,
                (
                    finished_at, duration_sec,
                    docs_total, docs_ok, docs_failed, docs_skipped,
                    self.session_id,
                ),
            )

    def finalize(
        self,
        docs_total: int,
        docs_ok: int,
        docs_failed: int,
        docs_skipped: int,
    ) -> None:
        """Вызвать из main() перед выходом из with-блока."""
        self._final_stats = {
            "docs_total": docs_total,
            "docs_ok": docs_ok,
            "docs_failed": docs_failed,
            "docs_skipped": docs_skipped,
        }

    # ------------------------------------------------------------------
    # parse_operations
    # ------------------------------------------------------------------

    def start_operation(self, doc_id: str, doc_meta: dict) -> str:
        """Создаёт строку операции. Возвращает op_id для передачи в другие методы."""
        op_id = _uuid()
        if self._disabled:
            return op_id
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO parse_operations
                        (op_id, session_id, doc_id, doc_subtype, short_title,
                         started_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'in_progress')
                    """,
                    (
                        op_id,
                        self.session_id,
                        doc_id,
                        doc_meta.get("doc_subtype"),
                        doc_meta.get("short_title"),
                        _now(),
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.start_operation: %s", exc)
        return op_id

    def end_operation(
        self,
        op_id: str,
        *,
        status: str,
        winning_source_url: str | None = None,
        winning_extractor: str | None = None,
        winning_parser: str | None = None,
        sections_total: int | None = None,
        sections_saved: int | None = None,
        wanted_count: int | None = None,
        sources_tried: int = 0,
        error_msg: str | None = None,
    ) -> None:
        if self._disabled:
            return
        try:
            finished_at = _now()
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT started_at FROM parse_operations WHERE op_id=%s", (op_id,)
                )
                row = cur.fetchone()
                duration_sec = (
                    (finished_at - row[0]).total_seconds() if row else None
                )
                cur.execute(
                    """
                    UPDATE parse_operations
                    SET finished_at=%s, duration_sec=%s, status=%s,
                        winning_source_url=%s, winning_extractor=%s, winning_parser=%s,
                        sections_total=%s, sections_saved=%s, wanted_count=%s,
                        sources_tried=%s, error_msg=%s
                    WHERE op_id=%s
                    """,
                    (
                        finished_at, duration_sec, status,
                        winning_source_url, winning_extractor, winning_parser,
                        sections_total, sections_saved, wanted_count,
                        sources_tried, error_msg, op_id,
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.end_operation: %s", exc)

    # ------------------------------------------------------------------
    # parse_http_requests
    # ------------------------------------------------------------------

    def log_http_request(
        self,
        url: str,
        *,
        http_attempt: int,
        cached: bool,
        status_code: int | None = None,
        response_bytes: int | None = None,
        duration_ms: float | None = None,
        error_msg: str | None = None,
        doc_id: str | None = None,
    ) -> None:
        if self._disabled:
            return
        try:
            domain = urlparse(url).netloc or None
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO parse_http_requests
                        (req_id, session_id, doc_id, url, http_attempt, cached,
                         status_code, response_bytes, duration_ms,
                         requested_at, error_msg, domain)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _uuid(), self.session_id, doc_id, url,
                        http_attempt, cached,
                        status_code, response_bytes, duration_ms,
                        _now(), error_msg, domain,
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.log_http_request: %s", exc)

    # ------------------------------------------------------------------
    # parse_source_attempts
    # ------------------------------------------------------------------

    def log_source_attempt(
        self,
        op_id: str,
        doc_id: str,
        source_url: str,
        extractor_cls: str,
        attempt_num: int,
        *,
        status: str,
        duration_ms: float | None = None,
        error_type: str | None = None,
        error_msg: str | None = None,
        sections_count: int | None = None,
    ) -> None:
        if self._disabled:
            return
        try:
            now = _now()
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO parse_source_attempts
                        (attempt_id, session_id, op_id, doc_id, source_url, extractor_cls,
                         attempt_num, started_at, finished_at, duration_ms,
                         status, error_type, error_msg, sections_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _uuid(), self.session_id, op_id, doc_id,
                        source_url, extractor_cls, attempt_num,
                        now, now, duration_ms,
                        status, error_type, error_msg, sections_count,
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.log_source_attempt: %s", exc)

    # ------------------------------------------------------------------
    # parse_alias_resolutions
    # ------------------------------------------------------------------

    def log_alias_resolution(
        self,
        op_id: str,
        doc_id: str,
        wanted_id: str,
        *,
        resolved: bool,
        strategy: str,
        text_length: int | None = None,
    ) -> None:
        if self._disabled:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO parse_alias_resolutions
                        (res_id, session_id, op_id, doc_id, wanted_id,
                         resolved, strategy, text_length, resolved_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _uuid(), self.session_id, op_id, doc_id, wanted_id,
                        resolved, strategy, text_length, _now(),
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.log_alias_resolution: %s", exc)
