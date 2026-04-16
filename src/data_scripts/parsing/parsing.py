"""РџР°Р№РїР»Р°Р№РЅ РїР°СЂСЃРёРЅРіР° СЂРµР°Р»СЊРЅС‹С… РЅРѕСЂРјР°С‚РёРІРЅС‹С… Р°РєС‚РѕРІ.

Р§РёС‚Р°РµС‚ real_documents[] РёР· config.yaml, РґР»СЏ РєР°Р¶РґРѕРіРѕ РґРѕРєСѓРјРµРЅС‚Р°:
  1. РЎРєР°С‡РёРІР°РµС‚ РёР· СЂРµРµСЃС‚СЂР° РёСЃС‚РѕС‡РЅРёРєРѕРІ (registry.py) СЃ РґРёСЃРєРѕРІС‹Рј РєСЌС€РµРј.
  2. РР·РІР»РµРєР°РµС‚ С‚РµРєСЃС‚ (HTML / PDF / ODT).
  3. Р Р°Р·Р±РёРІР°РµС‚ РЅР° СЃРµРєС†РёРё (РїР°СЂСЃРµСЂС‹ Р¤Р—, Р¦Р‘ Рё СЃРїРµС†-РґРѕРєСѓРјРµРЅС‚РѕРІ).
  4. РЎРѕС…СЂР°РЅСЏРµС‚ РІ data/parsed/{doc_id}_sections.json Рё sections_tree.json.

Р’С‹С…РѕРґРЅС‹Рµ С„Р°Р№Р»С‹ РїРѕС‚СЂРµР±Р»СЏСЋС‚СЃСЏ generating.py РґР»СЏ РёРЅСЉРµРєС†РёРё СЂРµР°Р»СЊРЅРѕРіРѕ С‚РµРєСЃС‚Р° РќРџРђ
РІ РїСЂРѕРјРїС‚С‹ РіРµРЅРµСЂР°С†РёРё СЃРёРЅС‚РµС‚РёС‡РµСЃРєРёС… СЂРµРіР»Р°РјРµРЅС‚РѕРІ.

CLI:
  python parsing.py                       # РїСЂРѕРїСѓСЃС‚РёС‚СЊ СѓР¶Рµ СЃРїР°СЂСЃРµРЅРЅС‹Рµ
  python parsing.py --force               # РїРµСЂРµРїР°СЂСЃРёС‚СЊ РІСЃРµ
  python parsing.py --only 115-FZ 590-P  # С‚РѕР»СЊРєРѕ СѓРєР°Р·Р°РЅРЅС‹Рµ
  python parsing.py --fetch-force         # СЃР±СЂРѕСЃРёС‚СЊ HTTP-РєСЌС€ РїСЂРё СЃРєР°С‡РёРІР°РЅРёРё
  python parsing.py --log-level DEBUG     # РґРµС‚Р°Р»СЊРЅС‹Р№ Р»РѕРі
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# РќР°СЃС‚СЂРѕР№РєР° РїСѓС‚РµР№ - РґРѕР±Р°РІР»СЏРµРј РґРёСЂРµРєС‚РѕСЂРёСЋ parsing/ РІ sys.path, С‡С‚РѕР±С‹ СЂР°Р±РѕС‚Р°Р»Рё
# РёРјРїРѕСЂС‚С‹ СЃРѕСЃРµРґРЅРёС… РјРѕРґСѓР»РµР№ (fetcher, registry Рё С‚.Рґ.)
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

ROOT_DIR = _THIS_DIR.parents[2]  # parsing/ -> data_scripts/ -> src/ -> root
CONFIG_PATH = ROOT_DIR / "public" / "config.yaml"
PARSED_DIR = ROOT_DIR / "data" / "parsed"
CACHE_DIR = ROOT_DIR / "data" / "fetch_cache"

# Р”РёСЂРµРєС‚РѕСЂРёСЏ РґР»СЏ СЂСѓС‡РЅС‹С… PDF/ODT-С„Р°Р№Р»РѕРІ.
# Р•СЃР»Рё РїРѕР»РѕР¶РёС‚СЊ СЃСЋРґР° С„Р°Р№Р» {doc_id}.odt РёР»Рё {doc_id}.pdf, РѕРЅ Р±СѓРґРµС‚ РёСЃРїРѕР»СЊР·РѕРІР°РЅ
# РєР°Рє РёСЃС‚РѕС‡РЅРёРє СЃ РЅР°РёРІС‹СЃС€РёРј РїСЂРёРѕСЂРёС‚РµС‚РѕРј.
MANUAL_PDF_DIR = ROOT_DIR / "data" / "manual_pdfs"

# ---------------------------------------------------------------------------
# РўРµРїРµСЂСЊ РјРѕР¶РЅРѕ РёРјРїРѕСЂС‚РёСЂРѕРІР°С‚СЊ Р»РѕРєР°Р»СЊРЅС‹Рµ РјРѕРґСѓР»Рё
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
# РњР°РїРїРёРЅРі subtype -> РєР»Р°СЃСЃ РїР°СЂСЃРµСЂР°
# ---------------------------------------------------------------------------

PARSER_BY_SUBTYPE: dict[str, type[AbstractSectionParser]] = {
    "federal_law": FederalLawParser,
    "cb_regulations": CBRDocumentParser,
}

# Р”РѕРєСѓРјРµРЅС‚С‹ СЃ РЅРµСЃС‚Р°РЅРґР°СЂС‚РЅРѕР№ СЃС‚СЂСѓРєС‚СѓСЂРѕР№ - РїРµСЂРµРѕРїСЂРµРґРµР»СЏРµРј РїР°СЂСЃРµСЂ
PARSER_OVERRIDE: dict[str, type[AbstractSectionParser]] = {
    "579-P": PlanOfAccountsParser,
    "6406-U": ReportingFormsParser,
}


# ---------------------------------------------------------------------------
# Р—Р°РіСЂСѓР·РєР° РєРѕРЅС„РёРіР°
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def collect_wanted_ids(config: dict, doc_id: str) -> list[str]:
    """РЎРѕР±РёСЂР°РµС‚ РІСЃРµ sec_id РёР· references СЃРёРЅС‚РµС‚РёС‡РµСЃРєРёС… РґРѕРєСѓРјРµРЅС‚РѕРІ, СЃСЃС‹Р»Р°СЋС‰РёС…СЃСЏ РЅР° doc_id."""
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
    """РР·РІР»РµРєР°РµС‚ С‡РёСЃР»РѕРІРѕР№ ID РґРѕРєСѓРјРµРЅС‚Р° РёР· base.garant.ru URL.

    Р Р°Р±РѕС‚Р°РµС‚ С‚РѕР»СЊРєРѕ СЃ base.garant.ru, РїРѕС‚РѕРјСѓ С‡С‚Рѕ РёРјРµРЅРЅРѕ СЌС‚РѕС‚ ID РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ
    РІ ivo.garant.ru РґР»СЏ Р·Р°РіСЂСѓР·РєРё С„Р°Р№Р»Р°. PRIME-СЃС‚СЂР°РЅРёС†С‹ (garant.ru/products/ipo/prime/)
    РёРјРµСЋС‚ СЃРѕР±СЃС‚РІРµРЅРЅСѓСЋ РЅСѓРјРµСЂР°С†РёСЋ, РѕС‚Р»РёС‡РЅСѓСЋ РѕС‚ base.garant.ru.
    """
    import re

    if "base.garant.ru" not in url.lower():
        return None
    m = re.search(r"/(\d{6,12})/?$", url.rstrip("/"))
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# РџР°СЂСЃРёРЅРі РѕРґРЅРѕРіРѕ РґРѕРєСѓРјРµРЅС‚Р°
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
    """РџР°СЂСЃРёС‚ РѕРґРёРЅ РґРѕРєСѓРјРµРЅС‚ Рё СЃРѕС…СЂР°РЅСЏРµС‚ sections.json.

    Args:
        doc_id: РРґРµРЅС‚РёС„РёРєР°С‚РѕСЂ СЂРµР°Р»СЊРЅРѕРіРѕ РґРѕРєСѓРјРµРЅС‚Р°, РЅР°РїСЂРёРјРµСЂ ``115-FZ``.
        doc_meta: РњРµС‚Р°РґР°РЅРЅС‹Рµ РґРѕРєСѓРјРµРЅС‚Р° РёР· ``config.yaml``.
        config: РџРѕР»РЅС‹Р№ РѕР±СЉРµРєС‚ РєРѕРЅС„РёРіСѓСЂР°С†РёРё РёР· ``public/config.yaml``.
        force_parse: Р•СЃР»Рё ``True``, РїР°СЂСЃРёРЅРі РІС‹РїРѕР»РЅСЏРµС‚СЃСЏ РґР°Р¶Рµ РїСЂРё СЃСѓС‰РµСЃС‚РІСѓСЋС‰РµРј
            ``data/parsed/{doc_id}_sections.json``.
        fetch_force: Р•СЃР»Рё ``True``, СЃРµС‚РµРІРѕР№ СЃР»РѕР№ РёРіРЅРѕСЂРёСЂСѓРµС‚ HTTP-РєСЌС€ Рё
            РїРµСЂРµРєР°С‡РёРІР°РµС‚ РёСЃС‚РѕС‡РЅРёРє Р·Р°РЅРѕРІРѕ.
        run_log: Р­РєР·РµРјРїР»СЏСЂ ``RunLogger`` РґР»СЏ DB-Р»РѕРіРёСЂРѕРІР°РЅРёСЏ. Р•СЃР»Рё ``None``,
            Р»РѕРіРёСЂРѕРІР°РЅРёРµ РІ PostgreSQL РѕС‚РєР»СЋС‡РµРЅРѕ.
        garant_playwright: Р­РєР·РµРјРїР»СЏСЂ ``GarantPlaywrightDownloader`` РґР»СЏ
            Р±СЂР°СѓР·РµСЂРЅРѕРіРѕ РґРѕСЃС‚СѓРїР° Рє Р·Р°РєСЂС‹С‚С‹Рј РґРѕРєСѓРјРµРЅС‚Р°Рј Garant. РњРѕР¶РµС‚ Р±С‹С‚СЊ ``None``.

    Returns:
        РЎС‚СЂРѕРєР° СЃС‚Р°С‚СѓСЃР° РѕР±СЂР°Р±РѕС‚РєРё РґРѕРєСѓРјРµРЅС‚Р°:
            - ``'ok'``: СѓСЃРїРµС€РЅРѕ СЂР°СЃРїР°СЂСЃРµРЅРѕ РёР· РѕРЅР»Р°Р№РЅ-РёСЃС‚РѕС‡РЅРёРєР° РёР»Рё Playwright
            - ``'skipped'``: РїСЂРѕРїСѓС‰РµРЅРѕ, СЂРµР·СѓР»СЊС‚Р°С‚ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚ Рё РЅРµС‚ ``--force``
            - ``'manual_pdf'``: СѓСЃРїРµС€РЅРѕ СЂР°СЃРїР°СЂСЃРµРЅРѕ РёР· СЂСѓС‡РЅРѕРіРѕ ``.odt`` РёР»Рё ``.pdf``
            - ``'failed'``: РІСЃРµ РёСЃС‚РѕС‡РЅРёРєРё РёСЃС‡РµСЂРїР°РЅС‹, РґРѕРєСѓРјРµРЅС‚ РЅРµ СЂР°СЃРїР°СЂСЃРµРЅ
            - ``'no_sources'``: РґР»СЏ ``doc_id`` РЅРµС‚ РёСЃС‚РѕС‡РЅРёРєРѕРІ РІ ``registry.py``
            - ``'no_parser'``: РЅРµ РЅР°Р№РґРµРЅ РїР°СЂСЃРµСЂ РґР»СЏ ``doc_subtype`` РґРѕРєСѓРјРµРЅС‚Р°
    """
    import time as _time

    out_path = PARSED_DIR / f"{doc_id}_sections.json"
    if not force_parse and out_path.exists():
        logging.info("SKIP %s - СѓР¶Рµ СЃРїР°СЂСЃРµРЅ (%s)", doc_id, out_path.name)
        if run_log:
            op_id = run_log.start_operation(doc_id, doc_meta)
            run_log.end_operation(op_id, status="skipped")
        return "skipped"

    op_id = run_log.start_operation(doc_id, doc_meta) if run_log else None

    sources = SOURCES.get(doc_id, [])
    if not sources:
        logging.warning("WARN %s - РЅРµС‚ РёСЃС‚РѕС‡РЅРёРєРѕРІ РІ registry.py", doc_id)
        if run_log and op_id:
            run_log.end_operation(op_id, status="no_sources")
        return "no_sources"

    subtype = doc_meta.get("doc_subtype", "")
    parser_cls = PARSER_OVERRIDE.get(doc_id) or PARSER_BY_SUBTYPE.get(subtype)
    if not parser_cls:
        logging.warning("WARN %s - РЅРµРёР·РІРµСЃС‚РЅС‹Р№ doc_subtype=%r, РїСЂРѕРїСѓСЃРєР°РµРј", doc_id, subtype)
        if run_log and op_id:
            run_log.end_operation(op_id, status="no_parser")
        return "no_parser"

    wanted_ids = collect_wanted_ids(config, doc_id)
    last_error: Exception | None = None

    # РџСЂРёРѕСЂРёС‚РµС‚ 0: СЂСѓС‡РЅС‹Рµ С„Р°Р№Р»С‹ РІ data/manual_pdfs/
    _manual_candidates = [
        (MANUAL_PDF_DIR / f"{doc_id}.odt", "ODT"),
        (MANUAL_PDF_DIR / f"{doc_id}.pdf", "PDF"),
    ]
    for manual_file, fmt_name in _manual_candidates:
        if not manual_file.exists():
            continue
        logging.info("РќР°Р№РґРµРЅ СЂСѓС‡РЅРѕР№ %s: %s", fmt_name, manual_file)
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
                    doc_id,
                    sections,
                    PARSED_DIR,
                    wanted_ids=wanted_ids,
                    op_id=op_id,
                    run_log=run_log,
                )
                logging.info(
                    "OK %s -> %s (%d СЃРµРєС†РёР№, СЂСѓС‡РЅРѕР№ %s)",
                    doc_id,
                    path.name,
                    len(sections),
                    fmt_name,
                )
                log_operation_manual_pdf(run_log, op_id, manual_file, parser_cls, sections, len(wanted_ids))
                return "manual_pdf"
        except Exception as exc:
            logging.warning("Р СѓС‡РЅРѕР№ %s РЅРµ СѓРґР°Р»РѕСЃСЊ СЂР°СЃРїР°СЂСЃРёС‚СЊ (%s): %s", fmt_name, manual_file, exc)

    # РџСЂРёРѕСЂРёС‚РµС‚ 0.5a: Playwright РґР»СЏ Garant
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

                # Р”Р»СЏ HTML-РёР·РІР»РµС‡РµРЅРёСЏ (РЅРµ ODT/PDF) С‚СЂРµР±СѓРµРј РјРёРЅРёРјСѓРј 3 СЃРµРєС†РёРё вЂ”
                # РёРЅР°С‡Рµ СЌС‚Рѕ СЃРєРѕСЂРµРµ РІСЃРµРіРѕ СЃС‚СЂР°РЅРёС†Р° СЃ РѕРіР»Р°РІР»РµРЅРёРµРј Р±РµР· РїРѕР»РЅРѕРіРѕ С‚РµРєСЃС‚Р°.
                # ODT Рё PDF СЃ Р»СЋР±С‹Рј РєРѕР»РёС‡РµСЃС‚РІРѕРј СЃРµРєС†РёР№ СЃС‡РёС‚Р°СЋС‚СЃСЏ РґРѕСЃС‚РѕРІРµСЂРЅС‹РјРё.
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
                    logging.info("OK %s -> %s (%d СЃРµРєС†РёР№, Playwright)", doc_id, path.name, len(sections))
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
                        "Playwright HTML РґР»СЏ %s РґР°Р» С‚РѕР»СЊРєРѕ %d СЃРµРєС†РёР№ вЂ” "
                        "РІРµСЂРѕСЏС‚РЅРѕ, РѕРіР»Р°РІР»РµРЅРёРµ Р±РµР· РїРѕР»РЅРѕРіРѕ С‚РµРєСЃС‚Р°, РїСЂРѕР±СѓРµРј HTTP-РёСЃС‚РѕС‡РЅРёРєРё.",
                        doc_id, len(sections),
                    )
            except Exception as exc:
                logging.warning("Garant Playwright РѕС€РёР±РєР° РґР»СЏ %s: %s", doc_id, exc)
                log_operation_fail(run_log, op_id, len(sources), exc)
                return "failed"
        else:
            logging.debug("Garant Playwright: РЅРµС‚ Garant ID РІ РёСЃС‚РѕС‡РЅРёРєР°С… РґР»СЏ %s", doc_id)

    # Fallback: РѕР±С‹С‡РЅР°СЏ Р·Р°РіСЂСѓР·РєР° РёСЃС‚РѕС‡РЅРёРєРѕРІ
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
                raise ValueError("Р­РєСЃС‚СЂР°РєС‚РѕСЂ РІРµСЂРЅСѓР» РїСѓСЃС‚РѕР№ С‚РµРєСЃС‚")

            parser = parser_cls()
            sections = parser.parse(raw_doc)

            if not sections:
                raise ValueError(
                    f"РџР°СЂСЃРµСЂ {parser_cls.__name__} РЅРµ РЅР°С€РµР» СЃРµРєС†РёР№. "
                    "Р’РѕР·РјРѕР¶РЅРѕ, СЂР°Р·РјРµС‚РєР° РёСЃС‚РѕС‡РЅРёРєР° РёР·РјРµРЅРёР»Р°СЃСЊ."
                )

            path = save_sections(
                doc_id,
                sections,
                PARSED_DIR,
                wanted_ids=wanted_ids,
                op_id=op_id,
                run_log=run_log,
            )
            logging.info("OK %s -> %s (%d СЃРµРєС†РёР№)", doc_id, path.name, len(sections))
            duration_ms = (_time.monotonic() - t0) * 1000
            log_source_ok(run_log, op_id, doc_id, spec, attempt_num, duration_ms, sections, parser_cls, len(wanted_ids))
            return "ok"

        except Exception as exc:
            duration_ms = (_time.monotonic() - t0) * 1000
            logging.warning("FAIL source %s for %s: %s", spec.url, doc_id, exc)
            log_source_fail(run_log, op_id, doc_id, spec, attempt_num, duration_ms, exc)
            last_error = exc
            continue

    logging.error("FAIL %s - РІСЃРµ РёСЃС‚РѕС‡РЅРёРєРё РёСЃС‡РµСЂРїР°РЅС‹. РџРѕСЃР»РµРґРЅСЏСЏ РѕС€РёР±РєР°: %s", doc_id, last_error)
    log_operation_fail(run_log, op_id, len(sources), last_error)
    return "failed"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="РџР°СЂСЃРёРЅРі СЂРµР°Р»СЊРЅС‹С… РЅРѕСЂРјР°С‚РёРІРЅС‹С… Р°РєС‚РѕРІ РІ data/parsed/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
РїСЂРёРјРµСЂС‹:
  python parsing.py                        # РїСЂРѕРїСѓСЃС‚РёС‚СЊ СѓР¶Рµ СЃРїР°СЂСЃРµРЅРЅС‹Рµ
  python parsing.py --force                # РїРµСЂРµРїР°СЂСЃРёС‚СЊ РІСЃРµ
  python parsing.py --only 115-FZ 590-P   # С‚РѕР»СЊРєРѕ СѓРєР°Р·Р°РЅРЅС‹Рµ РґРѕРєСѓРјРµРЅС‚С‹
  python parsing.py --fetch-force          # СЃР±СЂРѕСЃРёС‚СЊ HTTP-РєСЌС€
  python parsing.py --log-level DEBUG      # РґРµС‚Р°Р»СЊРЅС‹Р№ Р»РѕРі
        """,
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="РџРµСЂРµРїР°СЂСЃРёС‚СЊ РІСЃРµ РґРѕРєСѓРјРµРЅС‚С‹, РґР°Р¶Рµ РµСЃР»Рё sections.json СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚",
    )
    p.add_argument(
        "--only",
        nargs="+",
        metavar="DOC_ID",
        help="РџР°СЂСЃРёС‚СЊ С‚РѕР»СЊРєРѕ СѓРєР°Р·Р°РЅРЅС‹Рµ doc_id, РЅР°РїСЂРёРјРµСЂ 115-FZ 590-P",
    )
    p.add_argument(
        "--fetch-force",
        action="store_true",
        dest="fetch_force",
        help="РРіРЅРѕСЂРёСЂРѕРІР°С‚СЊ HTTP-РєСЌС€ Рё РїРµСЂРµРєР°С‡Р°С‚СЊ РґРѕРєСѓРјРµРЅС‚С‹ Р·Р°РЅРѕРІРѕ",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
        help="РЈСЂРѕРІРµРЅСЊ Р»РѕРіРёСЂРѕРІР°РЅРёСЏ (РїРѕ СѓРјРѕР»С‡Р°РЅРёСЋ: INFO)",
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

    # Р—Р°РіСЂСѓР¶Р°РµРј .env, С‡С‚РѕР±С‹ GARANT_LOGIN / GARANT_PASSWORD Р±С‹Р»Рё РґРѕСЃС‚СѓРїРЅС‹.
    try:
        from dotenv import load_dotenv

        load_dotenv(ROOT_DIR / ".env", override=False)
    except ImportError:
        pass  # python-dotenv РЅРµ СѓСЃС‚Р°РЅРѕРІР»РµРЅ - РїРµСЂРµРјРµРЅРЅС‹Рµ РґРѕР»Р¶РЅС‹ Р±С‹С‚СЊ СѓР¶Рµ РІ env

    set_cache_dir(CACHE_DIR)

    # РЎРѕР·РґР°РµРј Garant-Р·Р°РіСЂСѓР·С‡РёРє, РµСЃР»Рё Р·Р°РґР°РЅС‹ СѓС‡РµС‚РЅС‹Рµ РґР°РЅРЅС‹Рµ.
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
                "playwright РЅРµ СѓСЃС‚Р°РЅРѕРІР»РµРЅ - Playwright-Р·Р°РіСЂСѓР·С‡РёРє РѕС‚РєР»СЋС‡РµРЅ. "
                "РЈСЃС‚Р°РЅРѕРІРё: pip install playwright && playwright install chromium"
            )
        except Exception as exc:
            logging.warning("РќРµ СѓРґР°Р»РѕСЃСЊ Р·Р°РїСѓСЃС‚РёС‚СЊ Playwright: %s", exc)
            if garant_playwright is not None:
                try:
                    garant_playwright.stop()
                except Exception:
                    pass
            garant_playwright = None
    else:
        logging.debug(
            "GARANT_LOGIN / GARANT_PASSWORD РЅРµ Р·Р°РґР°РЅС‹ - Р°РІС‚РѕСЂРёР·РѕРІР°РЅРЅРѕРµ СЃРєР°С‡РёРІР°РЅРёРµ Garant РѕС‚РєР»СЋС‡РµРЅРѕ. "
            "Р”РѕР±Р°РІСЊ РёС… РІ .env РґР»СЏ Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРѕРіРѕ РїРѕР»СѓС‡РµРЅРёСЏ Р·Р°РєСЂС‹С‚С‹С… РґРѕРєСѓРјРµРЅС‚РѕРІ СЃ garant.ru."
        )

    config = load_config()
    real_docs: list[dict] = config.get("real_documents", [])

    if not real_docs:
        logging.error("config.yaml РЅРµ СЃРѕРґРµСЂР¶РёС‚ real_documents[]")
        sys.exit(1)

    # Р¤РёР»СЊС‚СЂР°С†РёСЏ РїРѕ --only
    if args.only:
        only_set = set(args.only)
        real_docs = [d for d in real_docs if d["id"] in only_set]
        if not real_docs:
            logging.error("РќРё РѕРґРёРЅ РёР· СѓРєР°Р·Р°РЅРЅС‹С… doc_id РЅРµ РЅР°Р№РґРµРЅ РІ real_documents[]")
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
    logging.info("Р“РѕС‚РѕРІРѕ: %d/%d СѓСЃРїРµС€РЅРѕ", ok, total)
    if failed:
        logging.warning("РќРµ СѓРґР°Р»РѕСЃСЊ: %s", ", ".join(failed))
        logging.warning(
            "Р”Р»СЏ РЅРµСѓРґР°РІС€РёС…СЃСЏ РґРѕРєСѓРјРµРЅС‚РѕРІ:\n"
            "  1a. Р•СЃР»Рё РґРѕРєСѓРјРµРЅС‚ Р·Р°РєСЂС‹С‚ Р·Р° Р°РІС‚РѕСЂРёР·Р°С†РёРµР№ РЅР° garant.ru:\n"
            "      - РІРѕР№РґРё РІ Р°РєРєР°СѓРЅС‚, СЃРєР°С‡Р°Р№ ODT, РїРѕР»РѕР¶Рё РІ data/manual_pdfs/{doc_id}.odt\n"
            "  1b. РР»Рё СЃРєР°С‡Р°Р№ PDF РІСЂСѓС‡РЅСѓСЋ Рё РїРѕР»РѕР¶Рё РІ data/manual_pdfs/{doc_id}.pdf\n"
            "      Р—Р°С‚РµРј: python parsing.py --only %s\n"
            "  2. РР»Рё РѕР±РЅРѕРІРё URL РІ registry.py РґР»СЏ СЌС‚РѕРіРѕ РґРѕРєСѓРјРµРЅС‚Р°",
            " ".join(failed),
        )


if __name__ == "__main__":
    main()
