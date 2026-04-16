"""Гибридный поиск: Reciprocal Rank Fusion (RRF) + утилиты для retrieval.

RRF объединяет ранки из разных источников (BM25, семантика) в единый score.
Формула: score(d) = Σ 1 / (k + rank(d) + 1)  для каждого списка

Рекомендуемые параметры:
    k=60 — классическое значение из Cormack et al. 2009,
           хорошо работает без тюнинга на большинстве корпусов.

Модуль не зависит от ChromaDB или BM25 напрямую — принимает и возвращает
простые списки chunk_id. Это позволяет тестировать его изолированно.
"""
from __future__ import annotations


def reciprocal_rank_fusion(
    ranked_lists: list[list[str]],
    k: int = 60,
) -> list[tuple[str, float]]:
    """Объединяет несколько ранжированных списков через RRF.

    Args:
        ranked_lists: список упорядоченных списков chunk_id (лучшие первые).
                      Обычно [bm25_ids, semantic_ids].
        k:            сглаживающий параметр (по умолчанию 60).

    Returns:
        Список (chunk_id, rrf_score), отсортированных по убыванию score.
    """
    scores: dict[str, float] = {}
    for ranked in ranked_lists:
        for rank, chunk_id in enumerate(ranked):
            scores[chunk_id] = scores.get(chunk_id, 0.0) + 1.0 / (k + rank + 1)
    return sorted(scores.items(), key=lambda x: -x[1])


def extract_ids(results: list[tuple[str, float]]) -> list[str]:
    """Извлекает только chunk_id из результатов RRF (без score)."""
    return [chunk_id for chunk_id, _ in results]


def merge_bm25_and_semantic(
    bm25_results: list[tuple[str, float]],
    semantic_results: list[dict],
    k: int = 60,
    n_results: int = 20,
) -> list[str]:
    """Удобная обёртка для типичного случая использования.

    Args:
        bm25_results:     список (chunk_id, score) из BM25Store.query()
        semantic_results: список dict с полем chunk_id из ChromaStore.query()
        k:                RRF-параметр
        n_results:        количество результатов в итоге

    Returns:
        Список chunk_id после RRF-слияния, топ n_results.
    """
    bm25_ids     = [chunk_id for chunk_id, _ in bm25_results]
    semantic_ids = [r["chunk_id"] for r in semantic_results]

    fused = reciprocal_rank_fusion([bm25_ids, semantic_ids], k=k)
    return extract_ids(fused[:n_results])
