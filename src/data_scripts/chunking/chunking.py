"""Пайплайн чанкинга документов для RAG.

Читает документы двух типов:
  real      — реальные НПА из data/parsed/{doc_id}_sections_tree.json
  synthetic — синтетические регламенты из data/generated/{doc_id}.md

Для каждого документа строит чанки через builder.build_chunks() и сохраняет
в data/chunks/{doc_id}_chunks.json.

CLI:
  python chunking.py                        # все документы, пропустить уже чанкованные
  python chunking.py --force                # перечанковать всё
  python chunking.py --only 590-P RG-KIB-001  # только указанные
  python chunking.py --source real          # только реальные НПА
  python chunking.py --source synthetic     # только синтетические
  python chunking.py --log-level DEBUG
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Пути — добавляем директорию chunking/ в sys.path для импортов
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
_DB_LOGGING_DIR = _THIS_DIR / "db_logging"
if str(_DB_LOGGING_DIR) not in sys.path:
    sys.path.insert(0, str(_DB_LOGGING_DIR))

ROOT_DIR   = _THIS_DIR.parents[2]   # chunking/ → data_scripts/ → src/ → root
CONFIG_PATH = ROOT_DIR / "public" / "config.yaml"
PARSED_DIR  = ROOT_DIR / "data" / "parsed"
GENERATED_DIR = ROOT_DIR / "data" / "generated"
CHUNKS_DIR  = ROOT_DIR / "data" / "chunks"

import yaml  # noqa: E402

from builder import build_chunks  # noqa: E402
from log_utils import log_doc_fail, log_doc_ok, log_doc_skipped  # noqa: E402
from run_logger import RunLogger  # noqa: E402
from md_parser import parse_markdown  # noqa: E402


# ---------------------------------------------------------------------------
# Загрузка конфигурации
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Обработка одного документа
# ---------------------------------------------------------------------------

def chunk_real_document(
    doc_id: str,
    doc_meta: dict,
    *,
    force: bool,
    run_log: RunLogger | None,
) -> str:
    """Чанкует реальный НПА из sections_tree.json.

    Returns: "ok" | "skipped" | "failed"
    """
    out_path = CHUNKS_DIR / f"{doc_id}_chunks.json"
    if out_path.exists() and not force:
        logging.info("  Пропуск %s (уже чанкован, используйте --force)", doc_id)
        log_doc_skipped(run_log, doc_id, "real")
        return "skipped"

    tree_path = PARSED_DIR / f"{doc_id}_sections_tree.json"
    if not tree_path.exists():
        msg = f"Файл не найден: {tree_path}"
        logging.warning("  %s — %s", doc_id, msg)
        log_doc_fail(run_log, doc_id, "real", FileNotFoundError(msg))
        return "failed"

    try:
        with open(tree_path, encoding="utf-8") as f:
            tree = json.load(f)

        enriched_meta = {**doc_meta, "source_type": "real"}
        chunks = build_chunks(tree, enriched_meta)
        _save_chunks(doc_id, chunks, out_path)

        indexed = sum(1 for c in chunks if c["is_indexed"])
        logging.info(
            "  %s → %d чанков (%d индексируемых)", doc_id, len(chunks), indexed
        )
        log_doc_ok(run_log, doc_id, "real", len(chunks), indexed)
        return "ok"

    except Exception as exc:
        logging.error("  %s — ошибка: %s", doc_id, exc, exc_info=True)
        log_doc_fail(run_log, doc_id, "real", exc)
        return "failed"


def chunk_synthetic_document(
    doc_id: str,
    doc_meta: dict,
    *,
    force: bool,
    run_log: RunLogger | None,
) -> str:
    """Чанкует синтетический регламент из generated/*.md.

    Returns: "ok" | "skipped" | "failed"
    """
    out_path = CHUNKS_DIR / f"{doc_id}_chunks.json"
    if out_path.exists() and not force:
        logging.info("  Пропуск %s (уже чанкован, используйте --force)", doc_id)
        log_doc_skipped(run_log, doc_id, "synthetic")
        return "skipped"

    md_path = GENERATED_DIR / f"{doc_id}.md"
    if not md_path.exists():
        msg = f"Файл не найден: {md_path} — документ ещё не сгенерирован"
        logging.warning("  %s — %s", doc_id, msg)
        log_doc_fail(run_log, doc_id, "synthetic", FileNotFoundError(msg))
        return "failed"

    try:
        text = md_path.read_text(encoding="utf-8")
        tree = parse_markdown(text, doc_id)

        enriched_meta = {**doc_meta, "source_type": "synthetic"}
        chunks = build_chunks(tree, enriched_meta)
        _save_chunks(doc_id, chunks, out_path)

        indexed = sum(1 for c in chunks if c["is_indexed"])
        logging.info(
            "  %s → %d чанков (%d индексируемых)", doc_id, len(chunks), indexed
        )
        log_doc_ok(run_log, doc_id, "synthetic", len(chunks), indexed)
        return "ok"

    except Exception as exc:
        logging.error("  %s — ошибка: %s", doc_id, exc, exc_info=True)
        log_doc_fail(run_log, doc_id, "synthetic", exc)
        return "failed"


def _save_chunks(doc_id: str, chunks: list[dict], out_path: Path) -> None:
    """Сохраняет чанки в JSON-файл."""
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    indexed = sum(1 for c in chunks if c["is_indexed"])
    payload = {
        "doc_id": doc_id,
        "chunked_at": datetime.now(timezone.utc).isoformat(),
        "chunk_count": len(chunks),
        "indexed_count": indexed,
        "chunks": chunks,
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Чанкинг документов для RAG-пайплайна",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python chunking.py                          # все документы
  python chunking.py --force                  # перечанковать всё
  python chunking.py --source real            # только реальные НПА
  python chunking.py --source synthetic       # только синтетические
  python chunking.py --only 590-P RG-KIB-001  # конкретные документы
        """,
    )
    p.add_argument(
        "--force", action="store_true",
        help="Перечанковать даже если chunks.json уже существует",
    )
    p.add_argument(
        "--only", nargs="+", metavar="DOC_ID",
        help="Чанковать только указанные doc_id",
    )
    p.add_argument(
        "--source", choices=["real", "synthetic", "all"], default="all",
        help="Источник документов (по умолчанию: all)",
    )
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
        help="Уровень логирования (по умолчанию: INFO)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config()
    only_set = set(args.only) if args.only else None

    # Собираем список документов для обработки
    real_docs: list[tuple[str, dict]] = []
    synthetic_docs: list[tuple[str, dict]] = []

    if args.source in ("real", "all"):
        for doc in config.get("real_documents", []):
            doc_id = doc["id"]
            if only_set and doc_id not in only_set:
                continue
            real_docs.append((doc_id, doc))

    if args.source in ("synthetic", "all"):
        for doc in config.get("documents", []):
            doc_id = doc["id"]
            if only_set and doc_id not in only_set:
                continue
            synthetic_docs.append((doc_id, doc))

    all_docs = [("real", doc_id, meta) for doc_id, meta in real_docs] + \
               [("synthetic", doc_id, meta) for doc_id, meta in synthetic_docs]

    if not all_docs:
        logging.error("Нет документов для обработки (проверьте --only и --source)")
        sys.exit(1)

    total = len(all_docs)
    ok = skipped = failed = 0

    with RunLogger(args) as run_log:
        for i, (source_type, doc_id, doc_meta) in enumerate(all_docs, 1):
            logging.info("[%d/%d] %s (%s)", i, total, doc_id, source_type)

            if source_type == "real":
                status = chunk_real_document(
                    doc_id, doc_meta, force=args.force, run_log=run_log
                )
            else:
                status = chunk_synthetic_document(
                    doc_id, doc_meta, force=args.force, run_log=run_log
                )

            if status == "ok":
                ok += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

        run_log.finalize(
            docs_total=total,
            docs_ok=ok,
            docs_failed=failed,
            docs_skipped=skipped,
        )

    logging.info(
        "Готово: %d обработано, %d пропущено, %d ошибок (всего %d)",
        ok, skipped, failed, total,
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
