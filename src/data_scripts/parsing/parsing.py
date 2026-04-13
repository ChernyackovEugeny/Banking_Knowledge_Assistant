"""Пайплайн парсинга реальных нормативных актов.

Читает real_documents[] из config.yaml, для каждого документа:
  1. Скачивает из реестра источников (registry.py) с дисковым кэшем.
  2. Извлекает текст (extractor_html.py / extractor_pdf.py).
  3. Разбивает на секции (parser_federal_law.py / parser_cbr.py).
  4. Сохраняет в data/parsed/{doc_id}_sections.json.

Выходные файлы потребляются generating.py для инъекции реального текста НПА
в промпты генерации синтетических регламентов.

CLI:
  python parsing.py                       # пропустить уже спарсенные
  python parsing.py --force               # переспарсить всё
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
# Настройка путей — добавляем директорию parsing/ в sys.path,
# чтобы работали импорты соседних модулей (fetcher, registry, etc.)
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

ROOT_DIR = _THIS_DIR.parents[2]  # parsing/ → data_scripts/ → src/ → root
CONFIG_PATH = ROOT_DIR / "public" / "config.yaml"
PARSED_DIR = ROOT_DIR / "data" / "parsed"
CACHE_DIR = ROOT_DIR / "data" / "fetch_cache"

# Директория для ручных PDF-файлов.
# Если положить сюда файл {doc_id}.pdf, он будет использован как первый источник.
# Пример: data/manual_pdfs/590-P.pdf
MANUAL_PDF_DIR = ROOT_DIR / "data" / "manual_pdfs"

# ---------------------------------------------------------------------------
# Теперь можно импортировать локальные модули
# ---------------------------------------------------------------------------
import os  # noqa: E402  (до yaml, используем здесь)
import yaml  # noqa: E402  (после манипуляций с sys.path)

from db_logging.log_utils import log_operation_fail, log_operation_manual_pdf, log_source_fail, log_source_ok  # noqa: E402
from db_logging.run_logger import RunLogger  # noqa: E402
from fetcher import fetch, set_cache_dir  # noqa: E402
from parsers.base import AbstractSectionParser  # noqa: E402
from parsers.cbr import CBRDocumentParser, PlanOfAccountsParser, ReportingFormsParser  # noqa: E402
from parsers.federal_law import FederalLawParser  # noqa: E402
from postprocessing.output import save_sections  # noqa: E402
from registry import SOURCES  # noqa: E402

# ---------------------------------------------------------------------------
# Маппинг subtype → класс парсера
# ---------------------------------------------------------------------------

PARSER_BY_SUBTYPE: dict[str, type[AbstractSectionParser]] = {
    "federal_law": FederalLawParser,
    "cb_regulations": CBRDocumentParser,
}

# Документы с нестандартной структурой — переопределяем парсер
PARSER_OVERRIDE: dict[str, type[AbstractSectionParser]] = {
    "579-P": PlanOfAccountsParser,
    "6406-U": ReportingFormsParser,
}


# ---------------------------------------------------------------------------
# Загрузка конфига
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def collect_wanted_ids(config: dict, doc_id: str) -> list[str]:
    """Собирает все sec_id из references всех синтетических документов, ссылающихся на doc_id."""
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
    import re
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
        doc_id: Идентификатор реального документа (например, ``115-FZ``).
        doc_meta: Метаданные документа из ``config.yaml`` (``real_documents[]``).
        config: Полный объект конфигурации из ``public/config.yaml``.
        force_parse: Если ``True``, парсинг выполняется даже при существующем
            ``data/parsed/{doc_id}_sections.json``.
        fetch_force: Если ``True``, сетевой слой игнорирует HTTP-кэш и
            перекачивает источник заново.
        run_log: Экземпляр ``RunLogger`` для DB-логирования; если ``None``,
            логирование в PostgreSQL отключено.
        garant_playwright: Экземпляр ``GarantPlaywrightDownloader`` для
            браузерного доступа к закрытым документам Garant (приоритет 0.5a).
            Может быть ``None``.

    Returns:
        Строка статуса обработки документа:
            - ``'ok'``: успешно распарсено из онлайн-источника/Playwright
            - ``'skipped'``: пропущено (результат уже существует и нет ``--force``)
            - ``'manual_pdf'``: успешно распарсено из ручного ``.odt/.pdf`` файла
            - ``'failed'``: все источники исчерпаны, документ не распарсен
            - ``'no_sources'``: для ``doc_id`` нет источников в ``registry.py``
            - ``'no_parser'``: не найден парсер для ``doc_subtype`` документа
    """
    import time as _time

    out_path = PARSED_DIR / f"{doc_id}_sections.json"
    if not force_parse and out_path.exists():
        logging.info("SKIP %s — уже спарсен (%s)", doc_id, out_path.name)
        if run_log:
            op_id = run_log.start_operation(doc_id, doc_meta)
            run_log.end_operation(op_id, status="skipped")
        return "skipped"

    op_id = run_log.start_operation(doc_id, doc_meta) if run_log else None

    sources = SOURCES.get(doc_id, [])
    if not sources:
        logging.warning("WARN %s — нет источников в registry.py", doc_id)
        if run_log and op_id:
            run_log.end_operation(op_id, status="no_sources")
        return "no_sources"

    subtype = doc_meta.get("doc_subtype", "")
    parser_cls = PARSER_OVERRIDE.get(doc_id) or PARSER_BY_SUBTYPE.get(subtype)
    if not parser_cls:
        logging.warning("WARN %s — неизвестный doc_subtype=%r, пропускаем", doc_id, subtype)
        if run_log and op_id:
            run_log.end_operation(op_id, status="no_parser")
        return "no_parser"

    wanted_ids = collect_wanted_ids(config, doc_id)

    last_error: Exception | None = None

    # ── Приоритет 0: ручные файлы в data/manual_pdfs/ ────────────────────────
    # Поддерживаются форматы: .odt (Garant после авторизации), .pdf
    # ODT имеет более высокий приоритет — это нативный формат Garant
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
            if sections:
                path = save_sections(
                    doc_id, sections, PARSED_DIR,
                    wanted_ids=wanted_ids,
                    op_id=op_id,
                    run_log=run_log,
                )
                logging.info(
                    "OK %s → %s (%d секций, ручной %s)",
                    doc_id, path.name, len(sections), fmt_name,
                )
                log_operation_manual_pdf(run_log, op_id, manual_file, parser_cls, sections, len(wanted_ids))
                return "manual_pdf"
        except Exception as exc:
            logging.warning("Ручной %s не удалось распарсить (%s): %s", fmt_name, manual_file, exc)
    # ─────────────────────────────────────────────────────────────────────────

    # ── Приоритет 0.5a: Playwright — браузерный загрузчик (предпочтительно) ──────
    # Запускает настоящий Chromium: логинится, убирает JS-пейволл, извлекает HTML.
    # Не требует платной подписки — достаточно бесплатного аккаунта base.garant.ru.
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
                if sections:
                    path = save_sections(
                        doc_id, sections, PARSED_DIR,
                        wanted_ids=wanted_ids,
                        op_id=op_id,
                        run_log=run_log,
                    )
                    logging.info(
                        "OK %s → %s (%d секций, Playwright)", doc_id, path.name, len(sections)
                    )
                    log_operation_manual_pdf(
                        run_log, op_id,
                        Path(f"playwright/base.garant.ru/{garant_id}"),
                        parser_cls, sections, len(wanted_ids),
                    )
                    return "ok"
            except Exception as exc:
                logging.warning("Garant Playwright ошибка для %s: %s", doc_id, exc)
        else:
            logging.debug("Garant Playwright: нет Garant ID в источниках для %s", doc_id)
    # ─────────────────────────────────────────────────────────────────────────

    # Оставлен как fallback на случай если Playwright не установлен.
    for attempt_num, spec in enumerate(sources, 1):
        t0 = _time.monotonic()
        try:
            logging.info("Fetching %s from %s …", doc_id, spec.url)
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

            if not sections:
                raise ValueError(
                    f"Парсер {parser_cls.__name__} не нашёл секций. "
                    "Возможно, разметка источника изменилась."
                )

            path = save_sections(
                doc_id, sections, PARSED_DIR,
                wanted_ids=wanted_ids,
                op_id=op_id,
                run_log=run_log,
            )
            logging.info("OK %s → %s (%d секций)", doc_id, path.name, len(sections))
            duration_ms = (_time.monotonic() - t0) * 1000
            log_source_ok(run_log, op_id, doc_id, spec, attempt_num, duration_ms, sections, parser_cls, len(wanted_ids))
            return "ok"

        except Exception as exc:
            duration_ms = (_time.monotonic() - t0) * 1000
            logging.warning("FAIL source %s for %s: %s", spec.url, doc_id, exc)
            log_source_fail(run_log, op_id, doc_id, spec, attempt_num, duration_ms, exc)
            last_error = exc
            continue  # пробуем следующий источник

    logging.error("FAIL %s — все источники исчерпаны. Последняя ошибка: %s", doc_id, last_error)
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
  python parsing.py --force                # переспарсить всё
  python parsing.py --only 115-FZ 590-P   # только указанные документы
  python parsing.py --fetch-force          # сбросить HTTP-кэш
  python parsing.py --log-level DEBUG      # детальный лог
        """,
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Переспарсить все документы, даже если sections.json уже существует",
    )
    p.add_argument(
        "--only",
        nargs="+",
        metavar="DOC_ID",
        help="Парсить только указанные doc_id (напр. 115-FZ 590-P)",
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

    # Загружаем .env чтобы GARANT_LOGIN / GARANT_PASSWORD были доступны
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT_DIR / ".env", override=False)
    except ImportError:
        pass  # python-dotenv не установлен — переменные должны быть уже в env

    set_cache_dir(CACHE_DIR)

    # Создаём Garant-загрузчик если заданы учётные данные.
    garant_playwright = None
    garant_login = os.environ.get("GARANT_LOGIN")
    garant_password = os.environ.get("GARANT_PASSWORD")
    if garant_login and garant_password:
        # Приоритет 1: Playwright (не требует платной подписки)
        try:
            from extractors.garant_playwright import GarantPlaywrightDownloader
            garant_playwright = GarantPlaywrightDownloader(garant_login, garant_password)
            garant_playwright.start()
            logging.info(
                "Garant Playwright загрузчик готов для %s "
                "(авторизация на base.garant.ru при первом запросе)",
                garant_login,
            )
        except ImportError:
            logging.warning(
                "playwright не установлен — Playwright-загрузчик отключён. "
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
            "GARANT_LOGIN / GARANT_PASSWORD не заданы — авторизованное скачивание Garant отключено. "
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
                logging.info("[%d/%d] %s — %s", i, total, doc_id, doc_meta.get("short_title", ""))

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
        # Закрываем Playwright браузер в любом случае (даже при исключении)
        if garant_playwright is not None:
            try:
                garant_playwright.stop()
            except Exception:
                pass

    # Итог
    logging.info("=" * 60)
    logging.info("Готово: %d/%d успешно", ok, total)
    if failed:
        logging.warning("Не удалось: %s", ", ".join(failed))
        logging.warning(
            "Для не удавшихся документов:\n"
            "  1a. Если документ закрыт за авторизацией на garant.ru:\n"
            "      — войди в аккаунт, скачай ODT, положи в data/manual_pdfs/{doc_id}.odt\n"
            "  1b. Или скачай PDF вручную и положи в data/manual_pdfs/{doc_id}.pdf\n"
            "      Затем: python parsing.py --only %s\n"
            "  2. Или обнови URL в registry.py для этого документа",
            " ".join(failed),
        )


if __name__ == "__main__":
    main()
