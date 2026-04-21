"""Отдельная пересборка BM25-индексов.

Скрипт читает уже проиндексированные документы из data/chroma_db/index_state.json,
собирает полный корпус для нужных кластеров и строит BM25 отдельно от ChromaDB.

Поддерживает отложенную сборку через dirty_clusters в data/bm25_indexes/rebuild_state.json.

Примеры:
  python bm25_rebuild.py                       # rebuild всех кластеров из index_state.json
  python bm25_rebuild.py --dirty-only         # rebuild только dirty-кластеров
  python bm25_rebuild.py --cluster compliance # rebuild одного кластера
  python bm25_rebuild.py --only 115-FZ 590-P  # rebuild кластеров, где лежат эти doc_id
"""
from __future__ import annotations

import argparse
import logging
import sys

from indexing import (  # noqa: E402
    BM25_DIR,
    BM25_STATE_PATH,
    _doc_list,
    load_all_chunks_by_cluster,
    load_bm25_state,
    load_config,
    load_state,
    save_bm25_state,
)
from stores.bm25_store import BM25Store  # noqa: E402


def rebuild_bm25(
    clusters_to_rebuild: set[str],
    all_doc_ids: list[str],
    bm25: BM25Store,
) -> set[str]:
    """Перестраивает BM25-индексы для указанных кластеров.

    Возвращает множество кластеров, которые удалось построить успешно.
    """
    clean_clusters = {cluster for cluster in clusters_to_rebuild if cluster}
    if not clean_clusters:
        return set()

    logging.info("BM25: перестройка кластеров: %s", sorted(clean_clusters))
    by_cluster = load_all_chunks_by_cluster(all_doc_ids)
    rebuilt: set[str] = set()

    for cluster in sorted(clean_clusters):
        chunks = by_cluster.get(cluster, [])
        if not chunks:
            logging.warning("BM25: кластер '%s' — нет чанков, пропуск", cluster)
            continue
        try:
            bm25.build(cluster, chunks)
        except Exception as exc:
            logging.error("BM25: ошибка при построении '%s': %s", cluster, exc, exc_info=True)
        else:
            rebuilt.add(cluster)
    return rebuilt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Отдельная пересборка BM25-индексов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python bm25_rebuild.py                       # rebuild всех кластеров из index_state.json
  python bm25_rebuild.py --dirty-only         # rebuild только dirty-кластеров
  python bm25_rebuild.py --cluster compliance # rebuild одного кластера
  python bm25_rebuild.py --only 115-FZ 590-P  # rebuild кластеров по выбранным doc_id
        """,
    )
    parser.add_argument("--dirty-only", action="store_true",
                        help="Строить только кластеры, помеченные dirty")
    parser.add_argument("--cluster", metavar="CLUSTER",
                        help="Строить только один указанный кластер")
    parser.add_argument("--only", nargs="+", metavar="DOC_ID",
                        help="Строить только кластеры, где лежат указанные doc_id")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        dest="log_level",
                        help="Уровень логирования (по умолчанию: INFO)")
    return parser.parse_args()


def _resolve_clusters(args: argparse.Namespace, indexed_docs: dict) -> set[str]:
    if args.cluster:
        return {args.cluster}

    if args.dirty_only:
        bm25_state = load_bm25_state()
        return set(bm25_state.get("dirty_clusters", []))

    if args.only:
        config = load_config()
        docs_by_id = {doc["id"]: doc for doc in _doc_list(config)}
        clusters = {
            docs_by_id[doc_id].get("cluster", "")
            for doc_id in args.only
            if doc_id in docs_by_id and doc_id in indexed_docs
        }
        return {cluster for cluster in clusters if cluster}

    return {
        doc_meta.get("cluster", "")
        for doc_meta in indexed_docs.values()
        if doc_meta.get("cluster")
    }


def _clear_dirty_clusters(rebuilt_clusters: set[str]) -> None:
    if not rebuilt_clusters:
        return
    bm25_state = load_bm25_state()
    dirty = set(bm25_state.get("dirty_clusters", []))
    if not dirty:
        return
    remaining = sorted(dirty - rebuilt_clusters)
    save_bm25_state({"dirty_clusters": remaining})


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    indexed_docs = load_state()
    if not indexed_docs:
        logging.error("index_state.json пуст. Сначала выполните indexing.py")
        sys.exit(1)

    clusters_to_rebuild = _resolve_clusters(args, indexed_docs)
    if not clusters_to_rebuild:
        if args.dirty_only:
            logging.info("BM25: dirty-кластеров нет (%s)", BM25_STATE_PATH)
            return
        logging.error("Нет кластеров для пересборки")
        sys.exit(1)

    bm25 = BM25Store(BM25_DIR)
    rebuilt_clusters = rebuild_bm25(clusters_to_rebuild, list(indexed_docs.keys()), bm25)
    _clear_dirty_clusters(rebuilt_clusters)

    logging.info(
        "BM25: успешно перестроено %d/%d кластеров",
        len(rebuilt_clusters),
        len(clusters_to_rebuild),
    )
    if rebuilt_clusters != {cluster for cluster in clusters_to_rebuild if cluster}:
        sys.exit(1)


if __name__ == "__main__":
    main()
