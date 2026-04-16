"""ChromaDB persistent store для индексации и семантического поиска.

Одна коллекция "banking_docs" для всех документов.
Кластеризация через metadata-фильтр: where={"cluster": "compliance"}.

Схема метаданных чанка в коллекции:
    doc_id, cluster, source_type, is_internal (bool),
    doc_type, doc_subtype, section_id, chunk_type,
    parent_chunk_id, as_of_date, char_count (int)

Все значения приводятся к типам, допустимым ChromaDB (str/int/float/bool).
None → "" для строк, None → False для булевых.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

COLLECTION_NAME = "banking_docs"
_CHROMA_BATCH   = 500  # max рекомендуемый размер одного upsert


class ChromaStore:
    """Обёртка над ChromaDB PersistentClient."""

    def __init__(self, db_path: Path) -> None:
        """
        Args:
            db_path: директория для персистентного хранилища (создаётся автоматически)
        """
        try:
            import chromadb  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "chromadb не установлен. Выполни: pip install chromadb"
            ) from exc

        db_path.mkdir(parents=True, exist_ok=True)
        self._client = chromadb.PersistentClient(path=str(db_path))
        self._collection = self._client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        logger.debug(
            "ChromaStore: коллекция '%s' готова (%d векторов)",
            COLLECTION_NAME,
            self._collection.count(),
        )

    # ------------------------------------------------------------------
    # Запись
    # ------------------------------------------------------------------

    def upsert(
        self,
        chunk_ids: list[str],
        embeddings: np.ndarray,
        texts: list[str],
        metadatas: list[dict],
    ) -> None:
        """Upsert пачки чанков (идемпотентно по chunk_id).

        Args:
            chunk_ids:  детерминированные ID из builder.py
            embeddings: np.ndarray (N, dim), dtype float32
            texts:      исходные тексты чанков
            metadatas:  список словарей с метаданными (sanitize применяется внутри)
        """
        clean_meta = [_sanitize_metadata(m) for m in metadatas]
        emb_list = embeddings.tolist()

        for start in range(0, len(chunk_ids), _CHROMA_BATCH):
            end = start + _CHROMA_BATCH
            self._collection.upsert(
                ids=chunk_ids[start:end],
                embeddings=emb_list[start:end],
                documents=texts[start:end],
                metadatas=clean_meta[start:end],
            )
        logger.debug("ChromaStore: upsert %d чанков", len(chunk_ids))

    # ------------------------------------------------------------------
    # Чтение
    # ------------------------------------------------------------------

    def query(
        self,
        embedding: list[float],
        n_results: int = 20,
        where: dict | None = None,
    ) -> list[dict]:
        """Семантический поиск по эмбеддингу.

        Returns:
            Список словарей с полями: chunk_id, text, metadata, distance
        """
        kwargs: dict = dict(
            query_embeddings=[embedding],
            n_results=n_results,
            include=["documents", "metadatas", "distances"],
        )
        if where:
            kwargs["where"] = where

        result = self._collection.query(**kwargs)
        ids        = result["ids"][0]
        docs       = result["documents"][0]
        metas      = result["metadatas"][0]
        distances  = result["distances"][0]

        return [
            {
                "chunk_id": ids[i],
                "text":     docs[i],
                "metadata": metas[i],
                "distance": distances[i],
            }
            for i in range(len(ids))
        ]

    def get_ids_for_doc(self, doc_id: str) -> set[str]:
        """Возвращает множество chunk_id уже проиндексированных для doc_id."""
        result = self._collection.get(
            where={"doc_id": doc_id},
            include=[],
        )
        return set(result["ids"])

    def count(self) -> int:
        """Количество векторов в коллекции."""
        return self._collection.count()

    def delete_doc(self, doc_id: str) -> int:
        """Удаляет все чанки указанного документа. Возвращает количество удалённых."""
        ids = list(self.get_ids_for_doc(doc_id))
        if ids:
            self._collection.delete(ids=ids)
        return len(ids)


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def chunk_to_metadata(chunk: dict) -> dict:
    """Превращает чанк-словарь в метаданные для ChromaDB."""
    return {
        "doc_id":          chunk.get("doc_id", ""),
        "cluster":         chunk.get("cluster", ""),
        "source_type":     chunk.get("source_type", ""),
        "is_internal":     bool(chunk.get("is_internal", False)),
        "doc_type":        chunk.get("doc_type", ""),
        "doc_subtype":     chunk.get("doc_subtype", ""),
        "section_id":      chunk.get("section_id", ""),
        "chunk_type":      chunk.get("chunk_type", ""),
        "parent_chunk_id": chunk.get("parent_chunk_id") or "",
        "as_of_date":      chunk.get("as_of_date", ""),
        "char_count":      int(chunk.get("char_count", 0)),
        "order_in_doc":    int(chunk.get("order_in_doc", 0)),
    }


def _sanitize_metadata(meta: dict) -> dict:
    """Убирает None-значения: None → "" для строк, None → False для bool."""
    clean = {}
    for k, v in meta.items():
        if v is None:
            clean[k] = ""
        elif isinstance(v, (str, int, float, bool)):
            clean[k] = v
        else:
            clean[k] = str(v)
    return clean
