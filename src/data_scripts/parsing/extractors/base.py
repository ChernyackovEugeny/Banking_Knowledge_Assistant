"""Базовые типы для экстракторов — промежуточный слой между сырыми байтами и текстом."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class RawDocument:
    """Результат экстракции: чистый текст + метаданные источника."""
    text: str
    source_url: str
    title: str | None = None
    is_pdf: bool = False
    is_odt: bool = False


class AbstractExtractor(ABC):
    """Превращает сырые байты ответа в RawDocument."""

    @abstractmethod
    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        """
        Args:
            raw_bytes: контент страницы или PDF-файла
            url: откуда получен (для метаданных и отладки)
        Returns:
            RawDocument с очищенным текстом
        Raises:
            ValueError: если документ не может быть извлечён из этих байтов
        """
        ...
