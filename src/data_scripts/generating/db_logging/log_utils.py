"""Вспомогательные функции для DB-логирования пайплайна генерации.

Изолируют многострочные вызовы RunLogger от бизнес-логики в generating.py,
questions.py и validator.py. Каждая функция — одно логическое событие.

Все функции принимают run_log первым аргументом и молча возвращаются,
если run_log не задан (None) или op_id отсутствует.
"""
from __future__ import annotations


def log_llm_ok(
    run_log,
    op_id: str | None,
    doc_id: str,
    response,
    duration_ms: float,
    attempt_num: int = 1,
) -> None:
    """Фиксирует успешный вызов LLM API."""
    if not (run_log and op_id):
        return
    run_log.log_llm_call(
        op_id, doc_id, response, duration_ms,
        attempt_num=attempt_num,
        status="ok",
    )


def log_llm_error(
    run_log,
    op_id: str | None,
    doc_id: str,
    exc: Exception,
    duration_ms: float,
    attempt_num: int = 1,
) -> None:
    """Фиксирует неудачный вызов LLM API (retry или финальная ошибка)."""
    if not (run_log and op_id):
        return
    run_log.log_llm_call(
        op_id, doc_id, None, duration_ms,
        attempt_num=attempt_num,
        status="error",
        error_msg=str(exc),
    )


def log_check(
    run_log,
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
    """Фиксирует результат одной валидационной проверки.

    expected_value и actual_value принимают любой тип — приведение к str
    выполняется в RunLogger.log_validation_result, не здесь.
    """
    if run_log is None:
        return
    run_log.log_validation_result(
        doc_id, artifact_type, check_name,
        passed=passed,
        expected_value=expected_value,
        actual_value=actual_value,
        detail=detail,
        op_id=op_id,
    )
