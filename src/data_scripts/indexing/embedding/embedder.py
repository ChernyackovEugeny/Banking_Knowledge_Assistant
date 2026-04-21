"""Обёртка над sentence-transformers для получения эмбеддингов.

Модель по умолчанию: sergeyzh/BERTA
  normalize_embeddings=True → косинусное расстояние = dot product

Загружается лениво при первом вызове encode().
"""
from __future__ import annotations

import logging
from importlib.metadata import PackageNotFoundError, version
import numpy as np

logger = logging.getLogger(__name__)

MODEL_NAME = "sergeyzh/BERTA"
DEFAULT_BATCH_SIZE = 6


def _pkg_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "not installed"


class Embedder:
    """Ленивая загрузка модели + батчевое кодирование."""

    def __init__(
        self,
        model_name: str = MODEL_NAME,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._model_name = model_name
        self._batch_size = batch_size
        self._model = None

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def encode(
        self,
        texts: list[str],
        batch_size: int | None = None,
        show_progress: bool = True,
    ) -> np.ndarray:
        """Кодирует список текстов в L2-нормализованные эмбеддинги.

        Args:
            texts:         список строк (уже фильтрованных, не пустых)
            batch_size:    размер батча; снижайте при OOM
            show_progress: отображать tqdm-прогрессбар

        Returns:
            np.ndarray shape (len(texts), hidden_size), dtype float32
        """
        self._ensure_loaded()
        effective_batch_size = batch_size or self._batch_size
        logger.debug("Embedding %d texts (batch_size=%d)", len(texts), effective_batch_size)
        embeddings = self._model.encode(
            texts,
            batch_size=effective_batch_size,
            show_progress_bar=show_progress,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )
        return embeddings.astype(np.float32)

    def dim(self) -> int:
        """Размерность вектора."""
        self._ensure_loaded()
        return self._model.get_sentence_embedding_dimension()

    # ------------------------------------------------------------------
    # Внутреннее
    # ------------------------------------------------------------------

    def _ensure_loaded(self) -> None:
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "sentence-transformers не установлен. "
                "Выполни: pip install sentence-transformers"
            ) from exc
        except Exception as exc:  # pragma: no cover - environment-specific dependency conflict
            raise RuntimeError(
                "Не удалось загрузить sentence-transformers из-за конфликта зависимостей. "
                f"Текущие версии: torch={_pkg_version('torch')}, "
                f"transformers={_pkg_version('transformers')}, "
                f"sentence-transformers={_pkg_version('sentence-transformers')}. "
                "Рекомендуется закрепить совместимые версии и переустановить зависимости."
            ) from exc

        logger.info("Загрузка модели %s …", self._model_name)
        self._model = SentenceTransformer(self._model_name)
        logger.info(
            "Модель загружена (dim=%d)", self._model.get_sentence_embedding_dimension()
        )
