"""Hybrid retriever: BM25 + semantic search (ChromaDB)."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from src.api.core.config import get_settings
from src.data_scripts.indexing.bm25_store import BM25Store
from src.data_scripts.indexing.chroma_store import ChromaStore
from src.data_scripts.indexing.embedder import Embedder
from src.data_scripts.indexing.hybrid import reciprocal_rank_fusion
from src.retriever.db_logging import RetrieverLogger

logger = logging.getLogger(__name__)


@dataclass
class RetrievedChunk:
    chunk_id: str
    doc_id: str
    text: str
    score: float
    cluster: str
    section_title: str = ""
    doc_title: str = ""


@dataclass
class _Runtime:
    embedder: Embedder
    chroma: ChromaStore
    bm25: BM25Store


_runtime: _Runtime | None = None
_runtime_init_lock = asyncio.Lock()


def _build_runtime() -> _Runtime:
    settings = get_settings()
    chroma = ChromaStore(host=settings.chroma_host, port=settings.chroma_port)
    bm25 = BM25Store(indexes_dir=Path(settings.bm25_dir))
    embedder = Embedder()
    return _Runtime(embedder=embedder, chroma=chroma, bm25=bm25)


async def _get_runtime() -> _Runtime:
    global _runtime
    if _runtime is not None:
        return _runtime
    async with _runtime_init_lock:
        if _runtime is None:
            _runtime = await asyncio.to_thread(_build_runtime)
            logger.info(
                "Retriever initialized (CHROMA_HOST=%s, CHROMA_PORT=%s, BM25_DIR=%s)",
                get_settings().chroma_host,
                get_settings().chroma_port,
                get_settings().bm25_dir,
            )
    return _runtime


async def retrieve(
    query: str,
    top_k: int = 5,
    cluster: str | None = None,
) -> list[RetrievedChunk]:
    """Return top-k relevant chunks with hybrid retrieval (semantic + BM25 + RRF)."""
    clean_query = query.strip()
    if not clean_query:
        return []

    settings = get_settings()
    candidates = max(top_k, settings.retrieval_candidates)
    request_id = RetrieverLogger.new_request_id()
    t_total = time.monotonic()
    semantic_duration_ms: float | None = None
    bm25_duration_ms: float | None = None
    semantic_hits = 0
    bm25_hits = 0
    fused_hits = 0
    result_hits = 0
    bm25_missing_ids = 0
    bm25_fallback = False
    source = "hybrid_rrf"
    status = "ok"
    error_msg: str | None = None
    results: list[RetrievedChunk] = []

    try:
        runtime = await _get_runtime()

        t_semantic = time.monotonic()
        embedding = await asyncio.to_thread(
            runtime.embedder.encode,
            [clean_query],
            32,
            False,
        )
        query_vector = embedding[0].tolist()

        where = {"cluster": cluster} if cluster else None
        semantic_results = await asyncio.to_thread(
            runtime.chroma.query,
            query_vector,
            candidates,
            where,
        )
        semantic_duration_ms = round((time.monotonic() - t_semantic) * 1000, 1)
        semantic_hits = len(semantic_results)

        try:
            t_bm25 = time.monotonic()
            if cluster:
                bm25_results = await asyncio.to_thread(
                    runtime.bm25.query,
                    cluster,
                    clean_query,
                    candidates,
                )
            else:
                bm25_results = await asyncio.to_thread(
                    runtime.bm25.query_all_clusters,
                    clean_query,
                    candidates,
                )
            bm25_duration_ms = round((time.monotonic() - t_bm25) * 1000, 1)
        except FileNotFoundError:
            logger.warning(
                "BM25 indexes were not found for cluster=%s; fallback to semantic only.",
                cluster,
            )
            bm25_results = []
            bm25_fallback = True

        semantic_ids = [row["chunk_id"] for row in semantic_results]
        bm25_ids = [chunk_id for chunk_id, _ in bm25_results]
        bm25_hits = len(bm25_results)
        fused = reciprocal_rank_fusion(
            [bm25_ids, semantic_ids],
            k=settings.retrieval_rrf_k,
        )[:top_k]
        fused_hits = len(fused)

        semantic_by_id = {row["chunk_id"]: row for row in semantic_results}
        missing_ids = [chunk_id for chunk_id, _ in fused if chunk_id not in semantic_by_id]
        bm25_missing_ids = len(missing_ids)
        if missing_ids:
            missing_rows = await asyncio.to_thread(runtime.chroma.get_chunks_by_ids, missing_ids)
            for row in missing_rows:
                semantic_by_id[str(row.get("chunk_id") or "")] = row

        for chunk_id, score in fused:
            row = semantic_by_id.get(chunk_id)
            if row is None:
                continue
            meta = row.get("metadata") or {}
            doc_id = str(meta.get("doc_id") or "")
            results.append(
                RetrievedChunk(
                    chunk_id=chunk_id,
                    doc_id=doc_id,
                    text=str(row.get("text") or ""),
                    score=float(score),
                    cluster=str(meta.get("cluster") or ""),
                    section_title=str(meta.get("section_id") or ""),
                    doc_title=doc_id,
                    )
                )

        result_hits = len(results)

        # If BM25 and semantic do not overlap, fallback to semantic top-k.
        if not results:
            source = "semantic_fallback"
            for rank, row in enumerate(semantic_results[:top_k]):
                meta = row.get("metadata") or {}
                doc_id = str(meta.get("doc_id") or "")
                results.append(
                    RetrievedChunk(
                        chunk_id=str(row.get("chunk_id") or ""),
                        doc_id=doc_id,
                        text=str(row.get("text") or ""),
                        score=1.0 / (settings.retrieval_rrf_k + rank + 1),
                        cluster=str(meta.get("cluster") or ""),
                        section_title=str(meta.get("section_id") or ""),
                        doc_title=doc_id,
                    )
                )
            result_hits = len(results)

        return results[:top_k]
    except Exception as exc:
        status = "error"
        error_msg = str(exc)
        logger.exception("Retriever failed; returning empty context.")
        return []
    finally:
        total_duration_ms = round((time.monotonic() - t_total) * 1000, 1)
        asyncio.create_task(
            RetrieverLogger.log_request(
                request_id=request_id,
                query_text=clean_query,
                cluster=cluster,
                top_k=top_k,
                candidates=candidates,
                semantic_hits=semantic_hits,
                bm25_hits=bm25_hits,
                fused_hits=fused_hits,
                result_hits=result_hits,
                bm25_missing_ids=bm25_missing_ids,
                bm25_fallback=bm25_fallback,
                semantic_duration_ms=semantic_duration_ms,
                bm25_duration_ms=bm25_duration_ms,
                total_duration_ms=total_duration_ms,
                status=status,
                error_msg=error_msg,
                chunks=results[:top_k],
                source=source,
            )
        )
