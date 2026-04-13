"""Structured observability layer для пайплайна генерации.

Пишет события в PostgreSQL (таблицы с префиксом gen_).
БД общая с parsing-модулем — префиксы не пересекаются.

Один класс обслуживает три скрипта: generating.py, questions.py, validator.py.
Скрипт передаётся через script_name при создании RunLogger.

Если PostgreSQL недоступен при старте — логирует WARNING и переходит
в disabled-режим: все методы становятся no-op, скрипт продолжается.

Использование (в generating.py):
    with RunLogger(args, script_name='generating') as run_log:
        op_id = run_log.start_operation(doc_id, doc_spec)
        run_log.log_llm_call(op_id, doc_id, response, duration_ms)
        run_log.end_operation(op_id, status='ok', output_path='data/generated/RG-KIB-001.md')
        run_log.finalize(docs_total=N, docs_ok=M, docs_failed=K, docs_skipped=S)
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

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv

# db_logging/ → generating/ → data_scripts/ → src/ → root (4 уровня)
load_dotenv(dotenv_path=Path(__file__).resolve().parents[4] / ".env")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL — создаётся при первом запуске, идемпотентно
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS gen_sessions (
    session_id       TEXT PRIMARY KEY,
    script_name      TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    duration_sec     DOUBLE PRECISION,
    flag_force       BOOLEAN NOT NULL DEFAULT FALSE,
    flag_only_filter TEXT,
    flag_count       INTEGER,
    log_level        TEXT NOT NULL DEFAULT 'INFO',
    docs_total       INTEGER NOT NULL DEFAULT 0,
    docs_ok          INTEGER NOT NULL DEFAULT 0,
    docs_failed      INTEGER NOT NULL DEFAULT 0,
    docs_skipped     INTEGER NOT NULL DEFAULT 0,
    python_version   TEXT,
    hostname         TEXT
);

CREATE INDEX IF NOT EXISTS idx_gen_sessions_started
    ON gen_sessions (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_gen_sessions_script
    ON gen_sessions (script_name, started_at DESC);

CREATE TABLE IF NOT EXISTS gen_operations (
    op_id        TEXT PRIMARY KEY,
    session_id   TEXT NOT NULL REFERENCES gen_sessions(session_id) ON DELETE CASCADE,
    script_name  TEXT NOT NULL,
    doc_id       TEXT NOT NULL,
    doc_title    TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ,
    duration_sec DOUBLE PRECISION,
    status       TEXT NOT NULL,
    output_path  TEXT,
    error_msg    TEXT
);

CREATE INDEX IF NOT EXISTS idx_gen_ops_session
    ON gen_operations (session_id, doc_id);
CREATE INDEX IF NOT EXISTS idx_gen_ops_status
    ON gen_operations (status, doc_id);
CREATE INDEX IF NOT EXISTS idx_gen_ops_started
    ON gen_operations (started_at DESC);

CREATE TABLE IF NOT EXISTS gen_llm_calls (
    call_id           TEXT PRIMARY KEY,
    session_id        TEXT NOT NULL REFERENCES gen_sessions(session_id) ON DELETE CASCADE,
    op_id             TEXT NOT NULL REFERENCES gen_operations(op_id) ON DELETE CASCADE,
    doc_id            TEXT NOT NULL,
    attempt_num       INTEGER NOT NULL DEFAULT 1,
    model             TEXT NOT NULL,
    max_tokens        INTEGER,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER,
    total_tokens      INTEGER,
    duration_ms       DOUBLE PRECISION,
    status            TEXT NOT NULL,
    error_msg         TEXT,
    called_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gen_llm_op
    ON gen_llm_calls (op_id, attempt_num);
CREATE INDEX IF NOT EXISTS idx_gen_llm_session
    ON gen_llm_calls (session_id);
CREATE INDEX IF NOT EXISTS idx_gen_llm_status
    ON gen_llm_calls (status, doc_id);

CREATE TABLE IF NOT EXISTS gen_validation_results (
    result_id      TEXT PRIMARY KEY,
    session_id     TEXT NOT NULL REFERENCES gen_sessions(session_id) ON DELETE CASCADE,
    op_id          TEXT,
    doc_id         TEXT NOT NULL,
    artifact_type  TEXT NOT NULL,
    check_name     TEXT NOT NULL,
    passed         BOOLEAN NOT NULL,
    expected_value TEXT,
    actual_value   TEXT,
    detail         TEXT,
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gen_val_session
    ON gen_validation_results (session_id, doc_id);
CREATE INDEX IF NOT EXISTS idx_gen_val_check
    ON gen_validation_results (check_name, passed);
CREATE INDEX IF NOT EXISTS idx_gen_val_artifact
    ON gen_validation_results (artifact_type, doc_id);
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
    """Пишет события генерации в PostgreSQL.

    При недоступности БД переходит в disabled-режим (все методы — no-op).
    """

    def __init__(self, args=None, *, script_name: str):
        self._args = args
        self._script_name = script_name
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
            logger.debug("RunLogger: сессия %s создана (%s)", self.session_id, self._script_name)
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
                INSERT INTO gen_sessions
                    (session_id, script_name, started_at, flag_force, flag_only_filter,
                     flag_count, log_level, python_version, hostname)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    self.session_id,
                    self._script_name,
                    self._session_started_at,
                    bool(getattr(args, "force", False)),
                    json.dumps(args.only) if getattr(args, "only", None) else None,
                    getattr(args, "count", None),
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
                UPDATE gen_sessions
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
    # gen_operations
    # ------------------------------------------------------------------

    def start_operation(self, doc_id: str, doc_meta: dict) -> str:
        """Создаёт строку операции. Возвращает op_id."""
        op_id = _uuid()
        if self._disabled:
            return op_id
        try:
            title = doc_meta.get("title", "")
            if title:
                title = title[:120]
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gen_operations
                        (op_id, session_id, script_name, doc_id, doc_title, started_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'in_progress')
                    """,
                    (
                        op_id,
                        self.session_id,
                        self._script_name,
                        doc_id,
                        title or None,
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
        output_path: str | None = None,
        error_msg: str | None = None,
    ) -> None:
        if self._disabled:
            return
        try:
            finished_at = _now()
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT started_at FROM gen_operations WHERE op_id=%s", (op_id,)
                )
                row = cur.fetchone()
                duration_sec = (
                    (finished_at - row[0]).total_seconds() if row else None
                )
                cur.execute(
                    """
                    UPDATE gen_operations
                    SET finished_at=%s, duration_sec=%s, status=%s,
                        output_path=%s, error_msg=%s
                    WHERE op_id=%s
                    """,
                    (
                        finished_at, duration_sec, status,
                        output_path, error_msg, op_id,
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.end_operation: %s", exc)

    # ------------------------------------------------------------------
    # gen_llm_calls
    # ------------------------------------------------------------------

    def log_llm_call(
        self,
        op_id: str,
        doc_id: str,
        response,
        duration_ms: float,
        *,
        attempt_num: int = 1,
        status: str = "ok",
        error_msg: str | None = None,
    ) -> None:
        """Фиксирует вызов LLM API.

        Args:
            response: объект ChatCompletion от OpenAI SDK или None при ошибке.
                      При None все token-поля записываются как NULL.
        """
        if self._disabled:
            return
        try:
            model = "unknown"
            max_tokens = None
            prompt_tokens = None
            completion_tokens = None
            total_tokens = None

            if response is not None:
                model = getattr(response, "model", "unknown") or "unknown"
                usage = getattr(response, "usage", None)
                if usage is not None:
                    prompt_tokens = getattr(usage, "prompt_tokens", None)
                    completion_tokens = getattr(usage, "completion_tokens", None)
                    total_tokens = getattr(usage, "total_tokens", None)

            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gen_llm_calls
                        (call_id, session_id, op_id, doc_id, attempt_num, model,
                         max_tokens, prompt_tokens, completion_tokens, total_tokens,
                         duration_ms, status, error_msg, called_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _uuid(), self.session_id, op_id, doc_id,
                        attempt_num, model,
                        max_tokens, prompt_tokens, completion_tokens, total_tokens,
                        duration_ms, status, error_msg, _now(),
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.log_llm_call: %s", exc)

    # ------------------------------------------------------------------
    # gen_validation_results
    # ------------------------------------------------------------------

    def log_validation_result(
        self,
        doc_id: str,
        artifact_type: str,
        check_name: str,
        *,
        passed: bool,
        expected_value=None,
        actual_value=None,
        detail: str | None = None,
        op_id: str | None = None,
    ) -> None:
        if self._disabled:
            return
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gen_validation_results
                        (result_id, session_id, op_id, doc_id, artifact_type,
                         check_name, passed, expected_value, actual_value, detail, checked_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _uuid(), self.session_id, op_id, doc_id, artifact_type,
                        check_name, passed,
                        str(expected_value) if expected_value is not None else None,
                        str(actual_value) if actual_value is not None else None,
                        detail, _now(),
                    ),
                )
        except Exception as exc:
            logger.debug("RunLogger.log_validation_result: %s", exc)
