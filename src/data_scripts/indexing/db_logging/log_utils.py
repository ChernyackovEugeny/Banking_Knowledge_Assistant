"""Вспомогательные функции для DB-логирования пайплайна индексации.

Изолируют вызовы RunLogger от бизнес-логики в indexing.py.
Все функции принимают run_log первым аргументом и молча возвращаются,
если run_log не задан (None).
"""
from __future__ import annotations


def log_doc_ok(
    run_log,
    doc_id: str,
    cluster: str,
    source_type: str,
    chunks_indexed: int,
) -> None:
    """Фиксирует успешную индексацию документа."""
    if run_log is None:
        return
    run_log.log_doc_result(
        doc_id,
        cluster=cluster,
        source_type=source_type,
        status="ok",
        chunks_indexed=chunks_indexed,
    )


def log_doc_fail(
    run_log,
    doc_id: str,
    cluster: str,
    source_type: str,
    exc: Exception,
) -> None:
    """Фиксирует ошибку индексации документа."""
    if run_log is None:
        return
    run_log.log_doc_result(
        doc_id,
        cluster=cluster,
        source_type=source_type,
        status="failed",
        error_msg=str(exc),
    )


def log_doc_skipped(
    run_log,
    doc_id: str,
    cluster: str,
    source_type: str,
) -> None:
    """Фиксирует пропуск документа (уже проиндексирован, --force не задан)."""
    if run_log is None:
        return
    run_log.log_doc_result(
        doc_id,
        cluster=cluster,
        source_type=source_type,
        status="skipped",
    )
