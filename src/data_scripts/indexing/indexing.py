"""Пайплайн индексации чанков в ChromaDB + BM25.

ChromaDB работает как отдельный сервис в docker-compose (chromadb/chroma).
Подключение по HTTP: CHROMA_HOST / CHROMA_PORT из .env.

Читает data/chunks/{doc_id}_chunks.json (только is_indexed=True).
Для каждого документа:
  1. Эмбеддит тексты через sbert_large_nlu_ru.
  2. Делает upsert в ChromaDB (одна коллекция, кластер — metadata-фильтр).
  3. Обновляет state-файл data/chroma_db/index_state.json.
После всех документов:
  4. Перестраивает BM25-индексы для изменившихся кластеров.

Стратегия пропуска:
  - Без --force: документы из index_state.json пропускаются (только embed).
    BM25 перестраивается только для кластеров, в которых есть новые документы.
  - С --force: всё переиндексируется с нуля.
  - --skip-embeddings: пропустить шаги 1-3, только перестроить BM25.
  - --skip-bm25: пропустить шаг 4, только ChromaDB.

CLI:
  python indexing.py                          # только новые документы
  python indexing.py --force                  # переиндексировать всё
  python indexing.py --only 115-FZ 590-P      # конкретные doc_id
  python indexing.py --cluster compliance     # только один кластер
  python indexing.py --source real            # только реальные НПА
  python indexing.py --skip-embeddings        # только BM25
  python indexing.py --skip-bm25             # только ChromaDB
  python indexing.py --log-level DEBUG
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Пути
# ---------------------------------------------------------------------------

_THIS_DIR    = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

ROOT_DIR     = _THIS_DIR.parents[2]          # indexing/ → data_scripts/ → src/ → root
CONFIG_PATH  = ROOT_DIR / "public" / "config.yaml"
CHUNKS_DIR   = ROOT_DIR / "data" / "chunks"
# Векторы живут в контейнере chroma (docker-compose). Локально держим только
# bookkeeping-файл индексатора, чтобы знать какие doc_id уже загружены.
STATE_DIR    = ROOT_DIR / "data" / "chroma_db"
BM25_DIR     = ROOT_DIR / "data" / "bm25_indexes"
STATE_PATH   = STATE_DIR / "index_state.json"

from chroma_store import ChromaStore, chunk_to_metadata  # noqa: E402
from bm25_store import BM25Store                          # noqa: E402
from embedder import Embedder                             # noqa: E402
from db_logging.log_utils import (                        # noqa: E402
    log_doc_ok, log_doc_fail, log_doc_skipped,
)
from db_logging.run_logger import RunLogger               # noqa: E402


# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _doc_list(config: dict) -> list[dict]:
    """Возвращает все документы из конфига (real + synthetic)."""
    real      = config.get("real_documents", [])
    synthetic = config.get("documents", [])
    result = []
    for d in real:
        result.append({**d, "source_type": "real"})
    for d in synthetic:
        result.append({**d, "source_type": "synthetic"})
    return result


# ---------------------------------------------------------------------------
# State file (какие doc_id уже проиндексированы в ChromaDB)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    """Загружает {doc_id: {indexed_at, chunk_count}} из state-файла."""
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            logging.warning("Не удалось прочитать index_state.json — сброс")
    return {}


def save_state(state: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Загрузка чанков
# ---------------------------------------------------------------------------

def load_chunks_for_doc(doc_id: str) -> list[dict] | None:
    """Загружает индексируемые чанки из chunks.json.

    Returns None если файл не найден.
    """
    path = CHUNKS_DIR / f"{doc_id}_chunks.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return [c for c in data["chunks"] if c["is_indexed"]]


def load_all_chunks_by_cluster(doc_ids_to_include: list[str]) -> dict[str, list[dict]]:
    """Загружает ВСЕ индексируемые чанки для указанных doc_id, группирует по кластеру.

    Используется для перестройки BM25 (нужен полный корпус кластера).
    """
    by_cluster: dict[str, list[dict]] = {}
    for doc_id in doc_ids_to_include:
        chunks = load_chunks_for_doc(doc_id)
        if not chunks:
            continue
        for chunk in chunks:
            cluster = chunk.get("cluster", "")
            by_cluster.setdefault(cluster, []).append(chunk)
    return by_cluster


# ---------------------------------------------------------------------------
# Индексация одного документа (embed + ChromaDB upsert)
# ---------------------------------------------------------------------------

def index_document(
    doc_id: str,
    doc_meta: dict,
    *,
    embedder: Embedder,
    chroma: ChromaStore,
    force: bool,
    state: dict,
    run_log: RunLogger | None,
) -> str:
    """Индексирует один документ.

    Returns: "ok" | "skipped" | "failed"
    """
    cluster     = doc_meta.get("cluster", "")
    source_type = doc_meta.get("source_type", "real")

    # Проверяем наличие chunks.json
    chunks = load_chunks_for_doc(doc_id)
    if chunks is None:
        msg = f"Файл не найден: {CHUNKS_DIR / f'{doc_id}_chunks.json'}"
        logging.warning("  %s — %s", doc_id, msg)
        log_doc_fail(run_log, doc_id, cluster, source_type, FileNotFoundError(msg))
        return "failed"

    if not chunks:
        msg = "нет индексируемых чанков (все is_indexed=False)"
        logging.warning("  %s — %s", doc_id, msg)
        log_doc_fail(run_log, doc_id, cluster, source_type, ValueError(msg))
        return "failed"

    # Пропуск если уже проиндексирован
    if not force and doc_id in state:
        prev = state[doc_id]
        logging.info(
            "  Пропуск %s (проиндексирован %s, %d чанков)",
            doc_id, prev.get("indexed_at", "?")[:10], prev.get("chunk_count", 0),
        )
        log_doc_skipped(run_log, doc_id, cluster, source_type)
        return "skipped"

    try:
        texts      = [c["text"] for c in chunks]
        chunk_ids  = [c["chunk_id"] for c in chunks]
        metadatas  = [chunk_to_metadata(c) for c in chunks]

        logging.info("  %s: эмбеддинг %d чанков …", doc_id, len(chunks))
        embeddings = embedder.encode(texts, show_progress=False)

        chroma.upsert(chunk_ids, embeddings, texts, metadatas)

        state[doc_id] = {
            "indexed_at":  datetime.now(timezone.utc).isoformat(),
            "chunk_count": len(chunks),
            "cluster":     cluster,
            "source_type": source_type,
        }

        logging.info("  %s → %d чанков в ChromaDB (кластер: %s)", doc_id, len(chunks), cluster)
        log_doc_ok(run_log, doc_id, cluster, source_type, len(chunks))
        return "ok"

    except Exception as exc:
        logging.error("  %s — ошибка: %s", doc_id, exc, exc_info=True)
        log_doc_fail(run_log, doc_id, cluster, source_type, exc)
        return "failed"


# ---------------------------------------------------------------------------
# Построение BM25
# ---------------------------------------------------------------------------

def rebuild_bm25(
    clusters_to_rebuild: set[str],
    all_doc_ids: list[str],
    bm25: BM25Store,
) -> None:
    """Перестраивает BM25-индексы для указанных кластеров.

    Загружает ВСЕ доступные чанки (из data/chunks/) для каждого кластера.
    """
    if not clusters_to_rebuild:
        return

    logging.info("BM25: перестройка кластеров: %s", sorted(clusters_to_rebuild))
    by_cluster = load_all_chunks_by_cluster(all_doc_ids)

    for cluster in sorted(clusters_to_rebuild):
        chunks = by_cluster.get(cluster, [])
        if not chunks:
            logging.warning("BM25: кластер '%s' — нет чанков, пропуск", cluster)
            continue
        try:
            bm25.build(cluster, chunks)
        except Exception as exc:
            logging.error("BM25: ошибка при построении '%s': %s", cluster, exc, exc_info=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Индексация чанков в ChromaDB + BM25",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python indexing.py                          # только новые документы
  python indexing.py --force                  # переиндексировать всё
  python indexing.py --only 115-FZ 590-P      # конкретные doc_id
  python indexing.py --cluster compliance     # только один кластер
  python indexing.py --source real            # только реальные НПА
  python indexing.py --skip-embeddings        # только BM25 (без ChromaDB)
  python indexing.py --skip-bm25             # только ChromaDB (без BM25)
        """,
    )
    p.add_argument("--force",  action="store_true",
                   help="Переиндексировать даже уже проиндексированные документы")
    p.add_argument("--only",   nargs="+", metavar="DOC_ID",
                   help="Индексировать только указанные doc_id")
    p.add_argument("--cluster", metavar="CLUSTER",
                   help="Индексировать только документы одного кластера")
    p.add_argument("--source", choices=["real", "synthetic", "all"], default="all",
                   help="Тип документов (по умолчанию: all)")
    p.add_argument("--skip-embeddings", action="store_true", dest="skip_embeddings",
                   help="Не выполнять embed + upsert в ChromaDB, только BM25")
    p.add_argument("--skip-bm25", action="store_true", dest="skip_bm25",
                   help="Не перестраивать BM25, только ChromaDB")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   dest="log_level",
                   help="Уровень логирования (по умолчанию: INFO)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config   = load_config()
    all_docs = _doc_list(config)

    # Фильтрация по --only, --cluster, --source
    only_set = set(args.only) if args.only else None
    filtered: list[dict] = []
    for doc in all_docs:
        doc_id = doc["id"]
        if only_set and doc_id not in only_set:
            continue
        if args.cluster and doc.get("cluster") != args.cluster:
            continue
        if args.source != "all" and doc.get("source_type") != args.source:
            continue
        filtered.append(doc)

    if not filtered:
        logging.error("Нет документов для обработки (проверьте --only / --cluster / --source)")
        sys.exit(1)

    state = load_state()

    # Инициализация хранилищ
    if args.skip_embeddings:
        chroma = None
        embedder = None
    else:
        chroma_host = os.getenv("CHROMA_HOST", "localhost")
        chroma_port = int(os.getenv("CHROMA_PORT", "8000"))
        chroma   = ChromaStore(chroma_host, chroma_port)
        embedder = Embedder()
    bm25 = None if args.skip_bm25 else BM25Store(BM25_DIR)

    total = len(filtered)
    ok = skipped = failed = 0
    chunks_total = 0
    newly_indexed_clusters: set[str] = set()

    with RunLogger(args) as run_log:

        # -----------------------------------------------------------
        # Шаг 1-3: embed + upsert в ChromaDB
        # -----------------------------------------------------------
        if not args.skip_embeddings:
            logging.info("=== ChromaDB: индексация %d документов ===", total)
            for i, doc_meta in enumerate(filtered, 1):
                doc_id = doc_meta["id"]
                logging.info("[%d/%d] %s", i, total, doc_id)

                status = index_document(
                    doc_id, doc_meta,
                    embedder=embedder,
                    chroma=chroma,
                    force=args.force,
                    state=state,
                    run_log=run_log,
                )
                if status == "ok":
                    ok += 1
                    chunks_total += state[doc_id]["chunk_count"]
                    newly_indexed_clusters.add(doc_meta.get("cluster", ""))
                elif status == "skipped":
                    skipped += 1
                else:
                    failed += 1

            save_state(state)
            logging.info(
                "ChromaDB: %d проиндексировано, %d пропущено, %d ошибок | итого %d векторов",
                ok, skipped, failed, chroma.count(),
            )
        else:
            logging.info("--skip-embeddings: шаг ChromaDB пропущен")
            # Считаем все doc_id в filtered как "новые" для BM25
            for doc_meta in filtered:
                newly_indexed_clusters.add(doc_meta.get("cluster", ""))

        # -----------------------------------------------------------
        # Шаг 4: BM25
        # -----------------------------------------------------------
        if not args.skip_bm25:
            # Если --force → перестраиваем все кластеры из filtered
            if args.force:
                clusters_to_rebuild = {d.get("cluster", "") for d in filtered}
            else:
                clusters_to_rebuild = newly_indexed_clusters

            if clusters_to_rebuild:
                all_indexed_doc_ids = list(state.keys())
                rebuild_bm25(clusters_to_rebuild, all_indexed_doc_ids, bm25)
            else:
                logging.info("BM25: новых документов нет, перестройка не требуется")
        else:
            logging.info("--skip-bm25: шаг BM25 пропущен")

        run_log.finalize(
            docs_total=total,
            docs_ok=ok,
            docs_failed=failed,
            docs_skipped=skipped,
            chunks_total=chunks_total,
        )

    logging.info(
        "Готово: %d проиндексировано, %d пропущено, %d ошибок (всего %d)",
        ok, skipped, failed, total,
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
