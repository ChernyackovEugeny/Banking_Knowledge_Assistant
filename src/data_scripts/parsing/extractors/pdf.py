"""PDF-экстрактор на базе PyMuPDF (fitz).

Используется как основной экстрактор для документов ЦБ РФ в формате PDF,
а также как fallback внутри HTML-экстракторов (cbr.ru возвращает PDF по HTTP).
"""
from __future__ import annotations

import logging
import re

import fitz  # PyMuPDF

from extractors.base import AbstractExtractor, RawDocument

logger = logging.getLogger(__name__)


def _clean_hyphenation(text: str) -> str:
    """Склеивает слова, перенесённые через дефис в конце строки.

    Пример: «иденти-\nфикации» → «идентификации».
    """
    return re.sub(r"-\n(\S)", r"\1", text)


def _normalize_whitespace(text: str) -> str:
    """Убирает лишние пустые строки (>2 подряд → 2)."""
    return re.sub(r"\n{3,}", "\n\n", text)


class PyMuPDFExtractor(AbstractExtractor):
    """Извлекает текст из PDF через PyMuPDF с восстановлением структуры страниц."""

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if raw_bytes[:4] != b"%PDF":
            raise ValueError(f"Байты не похожи на PDF: {url}")

        try:
            doc = fitz.open(stream=raw_bytes, filetype="pdf")
        except Exception as exc:
            raise ValueError(f"PyMuPDF не смог открыть PDF {url}: {exc}") from exc

        pages: list[str] = []
        title: str | None = None

        # Пробуем извлечь название из метаданных
        meta = doc.metadata
        if meta and meta.get("title"):
            title = meta["title"].strip() or None

        for page in doc:
            # "blocks" даёт список блоков (текст, изображение) с координатами
            # Сортируем по y-координате для правильного порядка чтения
            blocks = sorted(page.get_text("blocks"), key=lambda b: (b[1], b[0]))
            page_text = "\n".join(
                b[4].strip()
                for b in blocks
                if b[6] == 0  # type 0 = text block
                and b[4].strip()
            )
            if page_text:
                pages.append(page_text)

        text = "\n\n".join(pages)
        text = _clean_hyphenation(text)
        text = _normalize_whitespace(text)

        if not text.strip():
            raise ValueError(f"PDF пустой или содержит только изображения: {url}")

        logger.debug("PDF извлечён: %d страниц, %d символов, URL=%s", len(doc), len(text), url)
        return RawDocument(text=text, source_url=url, title=title, is_pdf=True)
