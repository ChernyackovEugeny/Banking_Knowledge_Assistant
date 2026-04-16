"""Вспомогательные функции для DB-логирования пайплайна парсинга.

Изолируют многострочные вызовы RunLogger от бизнес-логики в parsing.py,
fetcher.py и output.py. Каждая функция — один логический событие,
которое было бы 6-12 строк inline.

Все функции принимают run_log первым аргументом и молча возвращаются,
если run_log не задан (None) или не имеет нужного op_id.
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# HTTP-события (fetcher.py)
# ---------------------------------------------------------------------------

def log_http_cache_hit(run_log, url: str, doc_id: str | None, data: bytes, duration_ms: float) -> None:
    """Фиксирует отдачу из дискового кэша."""
    if run_log is None:
        return
    run_log.log_http_request(
        url,
        http_attempt=0,
        cached=True,
        response_bytes=len(data),
        duration_ms=duration_ms,
        doc_id=doc_id,
    )


def log_http_success(run_log, url: str, doc_id: str | None, attempt: int, resp, data: bytes, duration_ms: float) -> None:
    """Фиксирует успешный HTTP-запрос."""
    if run_log is None:
        return
    run_log.log_http_request(
        url,
        http_attempt=attempt,
        cached=False,
        status_code=resp.status_code,
        response_bytes=len(data),
        duration_ms=duration_ms,
        doc_id=doc_id,
    )


def log_http_error(run_log, url: str, doc_id: str | None, attempt: int, exc: Exception, duration_ms: float) -> None:
    """Фиксирует HTTP-ошибку."""
    if run_log is None:
        return
    status_code = getattr(getattr(exc, "response", None), "status_code", None)
    run_log.log_http_request(
        url,
        http_attempt=attempt,
        cached=False,
        status_code=status_code,
        duration_ms=duration_ms,
        error_msg=str(exc),
        doc_id=doc_id,
    )


# ---------------------------------------------------------------------------
# Алиас-события (output.py)
# ---------------------------------------------------------------------------

def log_alias(run_log, op_id: str | None, doc_id: str | None, wanted: str,
              *, resolved: bool, strategy: str, text_length: int | None = None) -> None:
    """Фиксирует попытку разрешения алиаса секции."""
    if not (run_log and op_id and doc_id):
        return
    run_log.log_alias_resolution(
        op_id, doc_id, wanted,
        resolved=resolved,
        strategy=strategy,
        text_length=text_length,
    )


# ---------------------------------------------------------------------------
# Операции с источниками (parsing.py)
# ---------------------------------------------------------------------------

def log_source_ok(run_log, op_id: str | None, doc_id: str, spec, attempt_num: int,
                  duration_ms: float, sections: list, parser_cls: type, wanted_count: int) -> None:
    """Фиксирует успешное получение и парсинг из одного источника.

    Пишет parse_source_attempts (status=ok) и финализирует parse_operations (status=ok).
    """
    if not (run_log and op_id):
        return
    run_log.log_source_attempt(
        op_id, doc_id, spec.url, spec.extractor_cls.__name__, attempt_num,
        status="ok",
        duration_ms=duration_ms,
        sections_count=len(sections),
    )
    run_log.end_operation(
        op_id,
        status="ok",
        winning_source_url=spec.url,
        winning_extractor=spec.extractor_cls.__name__,
        winning_parser=parser_cls.__name__,
        sections_total=len(sections),
        sections_saved=len(sections),
        wanted_count=wanted_count,
        sources_tried=attempt_num,
    )


def log_source_fail(run_log, op_id: str | None, doc_id: str, spec, attempt_num: int,
                    duration_ms: float, exc: Exception) -> None:
    """Фиксирует неудачную попытку источника (пайплайн продолжит со следующим)."""
    if not (run_log and op_id):
        return
    error_type = type(exc).__name__
    run_log.log_source_attempt(
        op_id, doc_id, spec.url, spec.extractor_cls.__name__, attempt_num,
        status="fetch_error" if "Request" in error_type else "extractor_error",
        duration_ms=duration_ms,
        error_type=error_type,
        error_msg=str(exc),
    )


def log_operation_manual_pdf(run_log, op_id: str | None, manual_pdf, parser_cls: type,
                             sections: list, wanted_count: int) -> None:
    """Финализирует операцию, источником которой стал ручной PDF-файл."""
    if not (run_log and op_id):
        return
    run_log.end_operation(
        op_id,
        status="manual_pdf",
        winning_source_url=f"file://{manual_pdf}",
        winning_extractor="PyMuPDFExtractor",
        winning_parser=parser_cls.__name__,
        sections_total=len(sections),
        sections_saved=len(sections),
        wanted_count=wanted_count,
        sources_tried=0,
    )


def log_operation_playwright_ok(
    run_log,
    op_id: str | None,
    source_url: str,
    parser_cls: type,
    sections: list,
    wanted_count: int,
) -> None:
    """Финализирует операцию, успешно выполненную через Garant Playwright."""
    if not (run_log and op_id):
        return
    run_log.end_operation(
        op_id,
        status="ok",
        winning_source_url=source_url,
        winning_extractor="GarantPlaywrightDownloader",
        winning_parser=parser_cls.__name__,
        sections_total=len(sections),
        sections_saved=len(sections),
        wanted_count=wanted_count,
        sources_tried=1,
    )


def log_operation_fail(run_log, op_id: str | None, sources_tried: int, last_error: Exception | None) -> None:
    """Финализирует операцию, завершившуюся провалом всех источников."""
    if not (run_log and op_id):
        return
    run_log.end_operation(
        op_id,
        status="failed",
        sources_tried=sources_tried,
        error_msg=str(last_error),
    )
