"""Парсер федеральных законов.

Структура ФЗ:
  Статья 3. Понятия, используемые в настоящем Федеральном законе
  Статья 3.1. ...
  Статья 10.1. ...

Нормализованные ID: «ст.3», «ст.3.1», «ст.10.1».
"""
from __future__ import annotations

import logging
import re

from extractors.base import RawDocument
from parsers.base import AbstractSectionParser, Section

logger = logging.getLogger(__name__)

# Паттерн статьи.
# Примеры:
#   «Статья 3. Понятия...»
#   «СТАТЬЯ 3.1»
#   «Статья 7.5.»
#   «Статья 10.1. Специальные...»
_ARTICLE_RE = re.compile(
    r"(?m)^\s*(?:СТАТЬЯ|Стать?я)\s+"     # ключевое слово
    r"(\d+(?:\.\d+)?)"                    # номер статьи (может быть дробным: 3.1, 10.1)
    r"\s*\.?\s*"                           # необязательная точка и пробелы
    r"(.*?)$"                              # заголовок до конца строки
)


def _normalize_article_id(num: str) -> str:
    """«3» → «ст.3», «10.1» → «ст.10.1»."""
    return f"ст.{num}"


class FederalLawParser(AbstractSectionParser):
    """Разбивает федеральный закон по статьям.

    Каждая статья становится Section с id=«ст.N».
    Текст статьи включает все части и пункты без дополнительного разбиения
    (части и пункты сохраняются как сплошной текст — их добавление в индекс
    избыточно для RAG на уровне статей).
    """

    def parse(self, raw_doc: RawDocument) -> list[Section]:
        text = raw_doc.text
        matches = list(_ARTICLE_RE.finditer(text))

        if not matches:
            logger.warning(
                "FederalLawParser: статьи не найдены в документе %s (%d символов)",
                raw_doc.source_url, len(text),
            )
            return []

        sections: list[Section] = []
        for i, m in enumerate(matches):
            art_num = m.group(1).strip()
            art_title = m.group(2).strip()

            # Текст статьи — от конца заголовка до начала следующей статьи
            body_start = m.end()
            body_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            body = text[body_start:body_end].strip()

            sec = Section(
                id=_normalize_article_id(art_num),
                title=art_title,
                text=body,
            )
            sections.append(sec)

        logger.info(
            "FederalLawParser: извлечено %d статей из %s",
            len(sections), raw_doc.source_url,
        )
        return sections
