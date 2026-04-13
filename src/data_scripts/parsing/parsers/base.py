"""Базовые типы для секционных парсеров."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from extractors.base import RawDocument


@dataclass
class Section:
    """Единица структуры документа — статья, глава, пункт, приложение."""

    id: str
    """Нормализованный идентификатор: «ст.3», «гл.2», «п.1.1», «Приложение»."""

    title: str
    """Заголовок секции (может быть пустым для нумерованных пунктов)."""

    text: str
    """Полный текст секции, включая текст дочерних элементов."""

    children: list[Section] = field(default_factory=list)
    """Дочерние подпункты (не используются при сохранении — только для агрегации)."""


class AbstractSectionParser(ABC):
    """Разбивает RawDocument на список Section."""

    @abstractmethod
    def parse(self, raw_doc: RawDocument) -> list[Section]:
        """
        Returns:
            Список секций верхнего уровня. Порядок соответствует порядку в документе.
            Пустой список означает провал парсинга (триггер для fallback).
        """
        ...
