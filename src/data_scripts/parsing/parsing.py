"""Пайплайн парсинга реальных нормативных актов.

Читает real_documents[] из config.yaml, для каждого документа:
  1. Скачивает из реестра источников (registry.py) с дисковым кэшем.
  2. Извлекает текст (HTML / PDF / ODT).
  3. Разбивает на секции (парсеры ФЗ, ЦБ и спец-документов).
  4. Сохраняет в data/parsed/{doc_id}_sections.json и sections_tree.json.

Выходные файлы потребляются generating.py для инъекции реального текста НПА
в промпты генерации синтетических регламентов.

CLI:
  python parsing.py                       # пропустить уже спарсенные
  python parsing.py --force               # перепарсить все
  python parsing.py --only 115-FZ 590-P  # только указанные
  python parsing.py --fetch-force         # сбросить HTTP-кэш при скачивании
  python parsing.py --log-level DEBUG     # детальный лог
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Настройка путей - добавляем директорию parsing/ в sys.path, чтобы работали
# импорты соседних модулей (fetcher, registry и т.д.)
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

ROOT_DIR = _THIS_DIR.parents[2]  # parsing/ -> data_scripts/ -> src/ -> root
CONFIG_PATH = ROOT_DIR / "public" / "config.yaml"
PARSED_DIR = ROOT_DIR / "data" / "parsed"
CACHE_DIR = ROOT_DIR / "data" / "fetch_cache"

# Директория для ручных PDF/ODT-файлов.
# Если положить сюда файл {doc_id}.odt или {doc_id}.pdf, он будет использован
# как источник с наивысшим приоритетом.
MANUAL_PDF_DIR = ROOT_DIR / "data" / "manual_pdfs"

# ---------------------------------------------------------------------------
# Теперь можно импортировать локальные модули
# ---------------------------------------------------------------------------
import os  # noqa: E402
import yaml  # noqa: E402

from db_logging.log_utils import (  # noqa: E402
    log_operation_fail,
    log_operation_manual_pdf,
    log_operation_playwright_ok,
    log_source_fail,
    log_source_ok,
)
from db_logging.run_logger import RunLogger  # noqa: E402
from fetcher import fetch, set_cache_dir  # noqa: E402
from parsers.base import AbstractSectionParser  # noqa: E402
from parsers.cbr import CBRDocumentParser, PlanOfAccountsParser, ReportingFormsParser  # noqa: E402
from parsers.federal_law import FederalLawParser  # noqa: E402
from postprocessing.output import save_sections  # noqa: E402
from registry import SOURCES  # noqa: E402

# ---------------------------------------------------------------------------
# Маппинг subtype -> класс парсера
# ---------------------------------------------------------------------------

PARSER_BY_SUBTYPE: dict[str, type[AbstractSectionParser]] = {
    "federal_law": FederalLawParser,
    "cb_regulations": CBRDocumentParser,
}

# Документы с нестандартной структурой - переопределяем парсер
PARSER_OVERRIDE: dict[str, type[AbstractSectionParser]] = {
    "579-P": PlanOfAccountsParser,
    "6406-U": ReportingFormsParser,
}


def validate_sections(doc_id: str, sections: list) -> None:
    """Минимальная семантическая валидация результата парсинга.

    Ошибки здесь должны означать, что технический parse завершился, но структура
    документа непригодна для downstream-этапов.
    """
    if not sections:
        raise ValueError(f"{doc_id}: parser returned no sections")

    if doc_id == "579-P":
        if len(sections) < 3:
            raise ValueError(f"{doc_id}: too few top-level sections ({len(sections)})")

        empty = [sec.id for sec in sections if not (sec.text or "").strip()]
        if empty:
            raise ValueError(f"{doc_id}: empty top-level sections: {', '.join(empty[:10])}")

        largest = max(len(sec.text or "") for sec in sections)
        total = sum(len(sec.text or "") for sec in sections) or 1
        if largest / total > 0.85:
            raise ValueError(
                f"{doc_id}: one section dominates the whole document "
                f"({largest} of {total} chars)"
            )


# ---------------------------------------------------------------------------
# Загрузка конфига
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def collect_wanted_ids(config: dict, doc_id: str) -> list[str]:
    """Собирает все sec_id из references синтетических документов, ссылающихся на doc_id."""
    wanted: list[str] = []
    for doc_spec in config.get("documents", []):
        for ref in doc_spec.get("references", []):
            if not isinstance(ref, dict):
                continue
            if ref.get("doc") != doc_id:
                continue
            for sec in ref.get("sections", []):
                sid = sec.get("id")
                if sid and sid not in wanted:
                    wanted.append(sid)
    return wanted


def extract_garant_id(url: str) -> str | None:
    """Извлекает числовой ID документа из base.garant.ru URL.

    Работает только с base.garant.ru, потому что именно этот ID используется
    в ivo.garant.ru для загрузки файла. PRIME-страницы (garant.ru/products/ipo/prime/)
    имеют собственную нумерацию, отличную от base.garant.ru.
    """
    import re

    if "base.garant.ru" not in url.lower():
        return None
    m = re.search(r"/(\d{6,12})/?$", url.rstrip("/"))
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Парсинг одного документа
# ---------------------------------------------------------------------------

def parse_document(
    doc_id: str,
    doc_meta: dict,
    config: dict,
    *,
    force_parse: bool,
    fetch_force: bool,
    run_log: RunLogger | None = None,
    garant_playwright: object = None,
) -> str:
    """Парсит один документ и сохраняет sections.json.

    Args:
        doc_id: Идентификатор реального документа, например ``115-FZ``.
        doc_meta: Метаданные документа из ``config.yaml``.
        config: Полный объект конфигурации из ``public/config.yaml``.
        force_parse: Если ``True``, парсинг выполняется даже при существующем
            ``data/parsed/{doc_id}_sections.json``.
        fetch_force: Если ``True``, сетевой слой игнорирует HTTP-кэш и
            перекачивает источник заново.
        run_log: Экземпляр ``RunLogger`` для DB-логирования. Если ``None``,
            логирование в PostgreSQL отключено.
        garant_playwright: Экземпляр ``GarantPlaywrightDownloader`` для
            браузерного доступа к закрытым документам Garant. Может быть ``None``.

    Returns:
        Строка статуса обработки документа:
            - ``'ok'``: успешно распарсено из онлайн-источника или Playwright
            - ``'skipped'``: пропущено, результат уже существует и нет ``--force``
            - ``'manual_pdf'``: успешно распарсено из ручного ``.odt`` или ``.pdf``
            - ``'failed'``: все источники исчерпаны, документ не распарсен
            - ``'no_sources'``: для ``doc_id`` нет источников в ``registry.py``
            - ``'no_parser'``: не найден парсер для ``doc_subtype`` документа
    """
    import time as _time

    out_path = PARSED_DIR / f"{doc_id}_sections.json"
    if not force_parse and out_path.exists():
        logging.info("SKIP %s - уже спарсен (%s)", doc_id, out_path.name)
        if run_log:
            op_id = run_log.start_operation(doc_id, doc_meta)
            run_log.end_operation(op_id, status="skipped")
        return "skipped"

    op_id = run_log.start_operation(doc_id, doc_meta) if run_log else None

    sources = SOURCES.get(doc_id, [])
    if not sources:
        logging.warning("WARN %s - нет источников в registry.py", doc_id)
        if run_log and op_id:
            run_log.end_operation(op_id, status="no_sources")
        return "no_sources"

    subtype = doc_meta.get("doc_subtype", "")
    parser_cls = PARSER_OVERRIDE.get(doc_id) or PARSER_BY_SUBTYPE.get(subtype)
    if not parser_cls:
        logging.warning("WARN %s - неизвестный doc_subtype=%r, пропускаем", doc_id, subtype)
        if run_log and op_id:
            run_log.end_operation(op_id, status="no_parser")
        return "no_parser"

    wanted_ids = collect_wanted_ids(config, doc_id)
    last_error: Exception | None = None

    # Приоритет 0: ручные файлы в data/manual_pdfs/
    _manual_candidates = [
        (MANUAL_PDF_DIR / f"{doc_id}.odt", "ODT"),
        (MANUAL_PDF_DIR / f"{doc_id}.pdf", "PDF"),
    ]
    for manual_file, fmt_name in _manual_candidates:
        if not manual_file.exists():
            continue
        logging.info("Найден ручной %s: %s", fmt_name, manual_file)
        try:
            raw_bytes = manual_file.read_bytes()
            if fmt_name == "ODT":
                from extractors.odt import ODTExtractor

                raw_doc = ODTExtractor().extract(raw_bytes, str(manual_file))
            else:
                from extractors.pdf import PyMuPDFExtractor

                raw_doc = PyMuPDFExtractor().extract(raw_bytes, str(manual_file))

            sections = parser_cls().parse(raw_doc)
            validate_sections(doc_id, sections)
            if sections:
                path = save_sections(
                    doc_id,
                    sections,
                    PARSED_DIR,
                    wanted_ids=wanted_ids,
                    op_id=op_id,
                    run_log=run_log,
                )
                logging.info(
                    "OK %s -> %s (%d секций, ручной %s)",
                    doc_id,
                    path.name,
                    len(sections),
                    fmt_name,
                )
                log_operation_manual_pdf(run_log, op_id, manual_file, parser_cls, sections, len(wanted_ids))
                return "manual_pdf"
        except Exception as exc:
            logging.warning("Ручной %s не удалось распарсить (%s): %s", fmt_name, manual_file, exc)

    # Приоритет 0.5a: Playwright для Garant
    if garant_playwright is not None:
        garant_id: str | None = None
        for spec in sources:
            gid = extract_garant_id(spec.url)
            if gid:
                garant_id = gid
                break

        if garant_id:
            try:
                raw_doc = garant_playwright.get_document(garant_id)
                sections = parser_cls().parse(raw_doc)
                validate_sections(doc_id, sections)

                # Для HTML-извлечения (не ODT/PDF) требуем минимум 3 секции —
                # иначе это скорее всего страница с оглавлением без полного текста.
                # ODT и PDF с любым количеством секций считаются достоверными.
                is_html = not (raw_doc.is_odt or raw_doc.is_pdf)
                if sections and (not is_html or len(sections) >= 3):
                    path = save_sections(
                        doc_id,
                        sections,
                        PARSED_DIR,
                        wanted_ids=wanted_ids,
                        op_id=op_id,
                        run_log=run_log,
                    )
                    logging.info("OK %s -> %s (%d секций, Playwright)", doc_id, path.name, len(sections))
                    log_operation_playwright_ok(
                        run_log,
                        op_id,
                        source_url=f"https://base.garant.ru/{garant_id}/",
                        parser_cls=parser_cls,
                        sections=sections,
                        wanted_count=len(wanted_ids),
                    )
                    return "ok"
                elif sections and is_html:
                    logging.warning(
                        "Playwright HTML для %s дал только %d секций — "
                        "вероятно, оглавление без полного текста, пробуем HTTP-источники.",
                        doc_id, len(sections),
                    )
            except Exception as exc:
                logging.warning("Garant Playwright ошибка для %s: %s", doc_id, exc)
                last_error = exc
        else:
            logging.debug("Garant Playwright: нет Garant ID в источниках для %s", doc_id)

    # Fallback: обычная загрузка источников
    for attempt_num, spec in enumerate(sources, 1):
        t0 = _time.monotonic()
        try:
            logging.info("Fetching %s from %s ...", doc_id, spec.url)
            raw_bytes = fetch(
                spec.url,
                force=fetch_force,
                doc_id=doc_id,
                run_log=run_log,
            )

            extractor = spec.extractor_cls()
            raw_doc = extractor.extract(raw_bytes, spec.url)
            if not raw_doc.text.strip():
                raise ValueError("Экстрактор вернул пустой текст")

            parser = parser_cls()
            sections = parser.parse(raw_doc)
            validate_sections(doc_id, sections)

            if not sections:
                raise ValueError(
                    f"Парсер {parser_cls.__name__} не нашел секций. "
                    "Возможно, разметка источника изменилась."
                )

            path = save_sections(
                doc_id,
                sections,
                PARSED_DIR,
                wanted_ids=wanted_ids,
                op_id=op_id,
                run_log=run_log,
            )
            logging.info("OK %s -> %s (%d секций)", doc_id, path.name, len(sections))
            duration_ms = (_time.monotonic() - t0) * 1000
            log_source_ok(run_log, op_id, doc_id, spec, attempt_num, duration_ms, sections, parser_cls, len(wanted_ids))
            return "ok"

        except Exception as exc:
            duration_ms = (_time.monotonic() - t0) * 1000
            logging.warning("FAIL source %s for %s: %s", spec.url, doc_id, exc)
            log_source_fail(run_log, op_id, doc_id, spec, attempt_num, duration_ms, exc)
            last_error = exc
            continue

    logging.error("FAIL %s - все источники исчерпаны. Последняя ошибка: %s", doc_id, last_error)
    log_operation_fail(run_log, op_id, len(sources), last_error)
    return "failed"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Парсинг реальных нормативных актов в data/parsed/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
примеры:
  python parsing.py                        # пропустить уже спарсенные
  python parsing.py --force                # перепарсить все
  python parsing.py --only 115-FZ 590-P   # только указанные документы
  python parsing.py --fetch-force          # сбросить HTTP-кэш
  python parsing.py --log-level DEBUG      # детальный лог
        """,
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Перепарсить все документы, даже если sections.json уже существует",
    )
    p.add_argument(
        "--only",
        nargs="+",
        metavar="DOC_ID",
        help="Парсить только указанные doc_id, например 115-FZ 590-P",
    )
    p.add_argument(
        "--fetch-force",
        action="store_true",
        dest="fetch_force",
        help="Игнорировать HTTP-кэш и перекачать документы заново",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
        help="Уровень логирования (по умолчанию: INFO)",
    )
    return p.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    # Загружаем .env, чтобы GARANT_LOGIN / GARANT_PASSWORD были доступны.
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT_DIR / ".env", override=False)
    except ImportError:
        pass  # python-dotenv не установлен - переменные должны быть уже в env

    set_cache_dir(CACHE_DIR)

    # Создаем Garant-загрузчик, если заданы учетные данные.
    garant_playwright = None
    garant_login = os.environ.get("GARANT_LOGIN")
    garant_password = os.environ.get("GARANT_PASSWORD")
    if garant_login and garant_password:
        try:
            from extractors.garant_playwright import GarantPlaywrightDownloader

            garant_playwright = GarantPlaywrightDownloader(garant_login, garant_password)
            garant_playwright.start()
            logging.info(
                "Garant Playwright загрузчик готов (авторизация на base.garant.ru при первом запросе)"
            )
        except ImportError:
            logging.warning(
                "playwright не установлен - Playwright-загрузчик отключен. "
                "Установи: pip install playwright && playwright install chromium"
            )
        except Exception as exc:
            logging.warning("Не удалось запустить Playwright: %s", exc)
            if garant_playwright is not None:
                try:
                    garant_playwright.stop()
                except Exception:
                    pass
            garant_playwright = None
    else:
        logging.debug(
            "GARANT_LOGIN / GARANT_PASSWORD не заданы - авторизованное скачивание Garant отключено. "
            "Добавь их в .env для автоматического получения закрытых документов с garant.ru."
        )

    config = load_config()
    real_docs: list[dict] = config.get("real_documents", [])

    if not real_docs:
        logging.error("config.yaml не содержит real_documents[]")
        sys.exit(1)

    # Фильтрация по --only
    if args.only:
        only_set = set(args.only)
        real_docs = [d for d in real_docs if d["id"] in only_set]
        if not real_docs:
            logging.error("Ни один из указанных doc_id не найден в real_documents[]")
            sys.exit(1)

    total = len(real_docs)
    ok = 0
    skipped = 0
    failed: list[str] = []

    try:
        with RunLogger(args) as run_log:
            for i, doc_meta in enumerate(real_docs, 1):
                doc_id = doc_meta["id"]
                logging.info("[%d/%d] %s - %s", i, total, doc_id, doc_meta.get("short_title", ""))

                status = parse_document(
                    doc_id,
                    doc_meta,
                    config,
                    force_parse=args.force,
                    fetch_force=args.fetch_force,
                    run_log=run_log,
                    garant_playwright=garant_playwright,
                )

                if status == "failed":
                    failed.append(doc_id)
                else:
                    ok += 1
                    if status == "skipped":
                        skipped += 1

            run_log.finalize(
                docs_total=total,
                docs_ok=ok,
                docs_failed=len(failed),
                docs_skipped=skipped,
            )
    finally:
        if garant_playwright is not None:
            try:
                garant_playwright.stop()
            except Exception:
                pass

    logging.info("=" * 60)
    logging.info("Готово: %d/%d успешно", ok, total)
    if failed:
        logging.warning("Не удалось: %s", ", ".join(failed))
        logging.warning(
            "Для неудавшихся документов:\n"
            "  1a. Если документ закрыт за авторизацией на garant.ru:\n"
            "      - войди в аккаунт, скачай ODT, положи в data/manual_pdfs/{doc_id}.odt\n"
            "  1b. Или скачай PDF вручную и положи в data/manual_pdfs/{doc_id}.pdf\n"
            "      Затем: python parsing.py --only %s\n"
            "  2. Или обнови URL в registry.py для этого документа",
            " ".join(failed),
        )


if __name__ == "__main__":
    main()
