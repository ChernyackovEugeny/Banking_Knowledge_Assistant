"""Вспомогательные функции для DB-логирования пайплайна чанкинга.

Изолируют вызовы RunLogger от бизнес-логики в chunking.py.
Все функции принимают run_log первым аргументом и молча возвращаются,
если run_log не задан (None).
"""
from __future__ import annotations


def log_doc_ok(
    run_log,
    doc_id: str,
    source_type: str,
    chunk_count: int,
    indexed_count: int,
) -> None:
    """Фиксирует успешную обработку документа."""
    if run_log is None:
        return
    run_log.log_doc_result(
        doc_id,
        source_type=source_type,
        status="ok",
        chunk_count=chunk_count,
        indexed_count=indexed_count,
    )


def log_doc_fail(run_log, doc_id: str, source_type: str, exc: Exception) -> None:
    """Фиксирует ошибку обработки документа."""
    if run_log is None:
        return
    run_log.log_doc_result(
        doc_id,
        source_type=source_type,
        status="failed",
        error_msg=str(exc),
    )


def log_doc_skipped(run_log, doc_id: str, source_type: str) -> None:
    """Фиксирует пропуск документа (уже чанкован, --force не задан)."""
    if run_log is None:
        return
    run_log.log_doc_result(
        doc_id,
        source_type=source_type,
        status="skipped",
    )
