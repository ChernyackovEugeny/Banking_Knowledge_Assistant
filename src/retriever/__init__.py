"""Гибридный retriever: BM25 + семантический поиск (ChromaDB).

Текущее состояние: stub, возвращает пустой список.
Подключить после завершения пайплайна индексации:

    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).parents[1] / "data_scripts" / "indexing"))
    from chroma_store import ChromaStore
    from bm25_store import BM25Store
    from embedder import Embedder
    from hybrid import merge_bm25_and_semantic
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RetrievedChunk:
    chunk_id: str
    doc_id: str
    text: str
    score: float
    cluster: str
    section_title: str = ""
    doc_title: str = ""


async def retrieve(
    query: str,
    top_k: int = 5,
    cluster: str | None = None,
) -> list[RetrievedChunk]:
    """Возвращает top_k наиболее релевантных чанков.

    TODO: подключить ChromaStore + BM25Store через merge_bm25_and_semantic().
    """
    return []
