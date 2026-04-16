"""BM25 индексы для лексического поиска.

Один индекс на кластер (compliance, credit, securities, general, reporting).
Хранится как {cluster}_bm25.pkl + {cluster}_meta.json в data/bm25_indexes/.

Токенизация:
  - регексп: только кириллица и латиница ([а-яёa-z]+)
  - лемматизация через pymorphy2 с lru_cache (кеш по слову → снижает дубли)

Поиск возвращает список (chunk_id, bm25_score), отфильтрованный score > 0.
"""
from __future__ import annotations

import json
import logging
import pickle
import re
import inspect
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from collections import namedtuple

logger = logging.getLogger(__name__)

# Совместимость с pymorphy2 на Python 3.11+: библиотека вызывает removed inspect.getargspec.
if not hasattr(inspect, "getargspec"):
    ArgSpec = namedtuple("ArgSpec", ["args", "varargs", "keywords", "defaults"])

    def _getargspec_compat(func):
        spec = inspect.getfullargspec(func)
        return ArgSpec(spec.args, spec.varargs, spec.varkw, spec.defaults)

    inspect.getargspec = _getargspec_compat  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Токенизатор (инициализируется лениво)
# ---------------------------------------------------------------------------

_morph = None


def _get_morph():
    global _morph
    if _morph is None:
        try:
            import pymorphy2  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "pymorphy2 не установлен. Выполни: pip install pymorphy2"
            ) from exc
        _morph = pymorphy2.MorphAnalyzer()
    return _morph


@lru_cache(maxsize=200_000)
def _lemmatize(word: str) -> str:
    """Лемматизирует одно слово. Кешируется глобально."""
    return _get_morph().parse(word)[0].normal_form


_TOKEN_RE = re.compile(r"[а-яёa-z]+")


def tokenize(text: str) -> list[str]:
    """Токенизирует и лемматизирует текст для BM25."""
    tokens = _TOKEN_RE.findall(text.lower())
    return [_lemmatize(t) for t in tokens]


# ---------------------------------------------------------------------------
# BM25Store
# ---------------------------------------------------------------------------

class BM25Store:
    """Управляет персистентными BM25-индексами по кластерам."""

    def __init__(self, indexes_dir: Path) -> None:
        self._dir = indexes_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Построение
    # ------------------------------------------------------------------

    def build(self, cluster: str, chunks: list[dict]) -> None:
        """Строит BM25-индекс для кластера из списка чанков.

        Args:
            cluster: название кластера ("compliance", "credit", …)
            chunks:  список чанков с полями chunk_id, text, doc_id
        """
        try:
            from rank_bm25 import BM25Okapi  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "rank-bm25 не установлен. Выполни: pip install rank-bm25"
            ) from exc

        logger.info(
            "BM25: строю индекс '%s' (%d чанков) …", cluster, len(chunks)
        )

        chunk_ids = [c["chunk_id"] for c in chunks]
        corpus = [tokenize(c["text"]) for c in chunks]

        bm25 = BM25Okapi(corpus)

        pkl_path  = self._dir / f"{cluster}_bm25.pkl"
        meta_path = self._dir / f"{cluster}_meta.json"

        with open(pkl_path, "wb") as f:
            pickle.dump(bm25, f, protocol=pickle.HIGHEST_PROTOCOL)

        meta = {
            "cluster":     cluster,
            "chunk_ids":   chunk_ids,
            "doc_ids":     sorted({c["doc_id"] for c in chunks}),
            "chunk_count": len(chunks),
            "built_at":    datetime.now(timezone.utc).isoformat(),
        }
        meta_path.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info("BM25: индекс '%s' сохранён → %s", cluster, pkl_path)

    # ------------------------------------------------------------------
    # Загрузка
    # ------------------------------------------------------------------

    def _load(self, cluster: str):
        """Возвращает (BM25Okapi, list[chunk_id])."""
        pkl_path  = self._dir / f"{cluster}_bm25.pkl"
        meta_path = self._dir / f"{cluster}_meta.json"

        if not pkl_path.exists():
            raise FileNotFoundError(
                f"BM25-индекс для кластера '{cluster}' не найден: {pkl_path}"
            )

        with open(pkl_path, "rb") as f:
            bm25 = pickle.load(f)

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return bm25, meta["chunk_ids"]

    # ------------------------------------------------------------------
    # Поиск
    # ------------------------------------------------------------------

    def query(
        self,
        cluster: str,
        text: str,
        n_results: int = 20,
    ) -> list[tuple[str, float]]:
        """Поиск по кластеру.

        Returns:
            Список (chunk_id, score), отсортированных по убыванию score.
            Только score > 0.
        """
        bm25, chunk_ids = self._load(cluster)
        tokens = tokenize(text)
        scores = bm25.get_scores(tokens)

        indexed = sorted(enumerate(scores), key=lambda x: -x[1])
        results = [
            (chunk_ids[i], float(score))
            for i, score in indexed[:n_results]
            if score > 0
        ]
        return results

    def query_all_clusters(
        self,
        text: str,
        n_results: int = 20,
    ) -> list[tuple[str, float]]:
        """Поиск по всем доступным кластерам, результаты объединяются.

        Используется когда классификатор запроса не уверен в кластере.
        """
        combined: dict[str, float] = {}
        for cluster in self.available_clusters():
            for chunk_id, score in self.query(cluster, text, n_results):
                if chunk_id not in combined or combined[chunk_id] < score:
                    combined[chunk_id] = score
        return sorted(combined.items(), key=lambda x: -x[1])[:n_results]

    # ------------------------------------------------------------------
    # Вспомогательное
    # ------------------------------------------------------------------

    def available_clusters(self) -> list[str]:
        """Список кластеров с готовыми индексами."""
        return [p.stem.replace("_bm25", "") for p in self._dir.glob("*_bm25.pkl")]

    def meta(self, cluster: str) -> dict:
        """Метаданные индекса: doc_ids, chunk_count, built_at."""
        meta_path = self._dir / f"{cluster}_meta.json"
        if not meta_path.exists():
            return {}
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def is_stale(self, cluster: str, chunk_files: list[Path]) -> bool:
        """Возвращает True если индекс устарел (старше самого нового chunk-файла)."""
        meta_path = self._dir / f"{cluster}_meta.json"
        if not meta_path.exists():
            return True
        index_mtime = meta_path.stat().st_mtime
        for cf in chunk_files:
            if cf.stat().st_mtime > index_mtime:
                return True
        return False
