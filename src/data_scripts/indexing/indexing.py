"""Пайплайн индексации чанков в ChromaDB.

ChromaDB работает как отдельный сервис в docker-compose (chromadb/chroma).
Подключение по HTTP: CHROMA_HOST / CHROMA_PORT из .env.

Читает data/chunks/{doc_id}_chunks.json (только is_indexed=True).
Для каждого документа:
  1. Эмбеддит тексты через sentence-transformers модель.
  2. Делает upsert в ChromaDB (одна коллекция, кластер — metadata-фильтр).
  3. Обновляет state-файл data/chroma_db/index_state.json.
После индексации:
  4. Помечает изменившиеся кластеры как dirty для отдельной сборки BM25.

Стратегия пропуска:
  - Без --force: документы из index_state.json пропускаются.
  - С --force: всё переиндексируется с нуля.
  - BM25 больше не строится автоматически в этом скрипте.
    Для BM25 используйте отдельный bm25_rebuild.py.

CLI:
  python indexing.py                          # только новые документы
  python indexing.py --force                  # переиндексировать всё
  python indexing.py --only 115-FZ 590-P      # конкретные doc_id
  python indexing.py --cluster compliance     # только один кластер
  python indexing.py --source real            # только реальные НПА
  python indexing.py --batch-size 4          # размер батча эмбеддинга
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
from time import perf_counter

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
STATE_PATH   = STATE_DIR / "index_state.json"
BM25_DIR     = ROOT_DIR / "data" / "bm25_indexes"
BM25_STATE_PATH = BM25_DIR / "rebuild_state.json"
DEFAULT_DOC_BATCH_SIZE = 64

from stores.chroma_store import ChromaStore, chunk_to_metadata  # noqa: E402
from embedding.embedder import Embedder                          # noqa: E402
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


def load_bm25_state() -> dict:
    """Загружает состояние rebuild BM25."""
    if BM25_STATE_PATH.exists():
        try:
            data = json.loads(BM25_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            logging.warning("Не удалось прочитать rebuild_state.json — сброс")
            return {"dirty_clusters": []}
        dirty_clusters = data.get("dirty_clusters", [])
        if not isinstance(dirty_clusters, list):
            return {"dirty_clusters": []}
        return {"dirty_clusters": sorted({str(cluster) for cluster in dirty_clusters if cluster})}
    return {"dirty_clusters": []}


def save_bm25_state(state: dict) -> None:
    """Сохраняет состояние rebuild BM25."""
    BM25_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "dirty_clusters": sorted({str(cluster) for cluster in state.get("dirty_clusters", []) if cluster}),
    }
    BM25_STATE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def mark_bm25_clusters_dirty(clusters: set[str]) -> set[str]:
    """Помечает кластеры грязными для последующей сборки BM25."""
    clean_clusters = {cluster for cluster in clusters if cluster}
    if not clean_clusters:
        return set()

    state = load_bm25_state()
    dirty = set(state.get("dirty_clusters", []))
    dirty.update(clean_clusters)
    save_bm25_state({"dirty_clusters": sorted(dirty)})
    return clean_clusters


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


def iter_chunk_batches(chunks: list[dict], batch_size: int):
    """Итерирует чанки документа небольшими порциями."""
    for start in range(0, len(chunks), batch_size):
        end = start + batch_size
        yield start, end, chunks[start:end]


# ---------------------------------------------------------------------------
# Индексация одного документа (embed + ChromaDB upsert)
# ---------------------------------------------------------------------------

def index_document(
    doc_id: str,
    doc_meta: dict,
    *,
    embedder: Embedder,
    chroma: ChromaStore,
    batch_size: int,
    doc_batch_size: int,
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
        doc_t0 = perf_counter()
        embed_total_s = 0.0
        upsert_total_s = 0.0
        logging.info(
            "  %s: эмбеддинг %d чанков батчами по %d …",
            doc_id, len(chunks), doc_batch_size,
        )
        for start, end, chunk_batch in iter_chunk_batches(chunks, doc_batch_size):
            texts = [c["text"] for c in chunk_batch]
            chunk_ids = [c["chunk_id"] for c in chunk_batch]
            metadatas = [chunk_to_metadata(c) for c in chunk_batch]
            batch_t0 = perf_counter()
            embed_t0 = perf_counter()
            embeddings = embedder.encode(texts, batch_size=batch_size, show_progress=False)
            embed_s = perf_counter() - embed_t0
            upsert_t0 = perf_counter()
            chroma.upsert(chunk_ids, embeddings, texts, metadatas)
            upsert_s = perf_counter() - upsert_t0
            batch_s = perf_counter() - batch_t0
            embed_total_s += embed_s
            upsert_total_s += upsert_s
            logging.info(
                "  %s: батч %d-%d/%d | embed %.2fs | upsert %.2fs | total %.2fs",
                doc_id,
                start + 1,
                min(end, len(chunks)),
                len(chunks),
                embed_s,
                upsert_s,
                batch_s,
            )

        state[doc_id] = {
            "indexed_at":  datetime.now(timezone.utc).isoformat(),
            "chunk_count": len(chunks),
            "cluster":     cluster,
            "source_type": source_type,
        }

        doc_total_s = perf_counter() - doc_t0
        logging.info(
            "  %s → %d чанков в ChromaDB (кластер: %s) | embed %.2fs | upsert %.2fs | total %.2fs",
            doc_id,
            len(chunks),
            cluster,
            embed_total_s,
            upsert_total_s,
            doc_total_s,
        )
        log_doc_ok(run_log, doc_id, cluster, source_type, len(chunks))
        return "ok"

    except Exception as exc:
        logging.error("  %s — ошибка: %s", doc_id, exc, exc_info=True)
        log_doc_fail(run_log, doc_id, cluster, source_type, exc)
        return "failed"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Индексация чанков в ChromaDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python indexing.py                          # только новые документы
  python indexing.py --force                  # переиндексировать всё
  python indexing.py --only 115-FZ 590-P      # конкретные doc_id
  python indexing.py --cluster compliance     # только один кластер
  python indexing.py --source real            # только реальные НПА
  python indexing.py --batch-size 6          # размер батча эмбеддинга
  python indexing.py --doc-batch-size 64     # размер батча чанков для upsert
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
    p.add_argument("--batch-size", type=int, default=6, dest="batch_size",
                   help="Размер батча для sentence-transformers (по умолчанию: 6)")
    p.add_argument("--doc-batch-size", type=int, default=DEFAULT_DOC_BATCH_SIZE, dest="doc_batch_size",
                   help="Размер батча чанков для embed + upsert (по умолчанию: 64)")
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
    if args.batch_size < 1:
        logging.error("--batch-size должен быть >= 1")
        sys.exit(1)
    if args.doc_batch_size < 1:
        logging.error("--doc-batch-size должен быть >= 1")
        sys.exit(1)
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

    chroma_host = os.getenv("CHROMA_HOST", "localhost")
    chroma_port = int(os.getenv("CHROMA_PORT", "8000"))
    chroma   = ChromaStore(chroma_host, chroma_port)
    embedder = Embedder(batch_size=args.batch_size)

    total = len(filtered)
    ok = skipped = failed = 0
    chunks_total = 0
    newly_indexed_clusters: set[str] = set()

    with RunLogger(args) as run_log:

        # -----------------------------------------------------------
        # Шаг 1-3: embed + upsert в ChromaDB
        # -----------------------------------------------------------
        logging.info("=== ChromaDB: индексация %d документов ===", total)
        for i, doc_meta in enumerate(filtered, 1):
            doc_id = doc_meta["id"]
            logging.info("[%d/%d] %s", i, total, doc_id)

            status = index_document(
                doc_id, doc_meta,
                embedder=embedder,
                chroma=chroma,
                batch_size=args.batch_size,
                doc_batch_size=args.doc_batch_size,
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

        dirty_clusters = mark_bm25_clusters_dirty(newly_indexed_clusters)
        logging.info(
            "ChromaDB: %d проиндексировано, %d пропущено, %d ошибок | итого %d векторов",
            ok, skipped, failed, chroma.count(),
        )
        if dirty_clusters:
            logging.info(
                "BM25: помечены dirty-кластеры %s. Запустите bm25_rebuild.py --dirty-only",
                sorted(dirty_clusters),
            )
        else:
            logging.info("BM25: dirty-кластеры не изменились")

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
