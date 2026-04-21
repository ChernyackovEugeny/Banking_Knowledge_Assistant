"""PDF-экстрактор: PyMuPDF для текста + pdfplumber для таблиц.

PyMuPDF блочно извлекает текст и быстро работает, но не восстанавливает
структуру таблиц — ячейки схлопываются в отдельные блоки и теряют связь со
строками. pdfplumber надёжно находит таблицы по ruling lines / text alignment,
но его ``extract_text`` хуже сохраняет абзацы. Поэтому здесь используем оба:

* ``pdfplumber`` — только для ``page.find_tables()`` (bbox + ячейки);
* ``PyMuPDF``    — для текста страниц через ``get_text("blocks")``.

Блоки PyMuPDF, чей центр попадает внутрь bbox таблицы, выкидываются — чтобы
не дублировать содержимое. Оставшиеся блоки и Markdown-таблицы совместно
сортируются по ``y0`` и склеиваются в финальный текст страницы.
"""
from __future__ import annotations

import io
import logging
import re

import fitz  # PyMuPDF
import pdfplumber

from extractors.base import AbstractExtractor, RawDocument
from tables import TableIdGenerator, format_table

logger = logging.getLogger(__name__)


def _clean_hyphenation(text: str) -> str:
    """Склеивает слова, перенесённые через дефис в конце строки.

    Пример: «иденти-\\nфикации» → «идентификации». Негативный lookahead на ``⟦``
    защищает от склейки с открывающим маркером таблицы, когда таблица стоит
    сразу после переноса строки.
    """
    return re.sub(r"-\n(?!⟦)(\S)", r"\1", text)


def _normalize_whitespace(text: str) -> str:
    """Убирает лишние пустые строки (>2 подряд → 2)."""
    return re.sub(r"\n{3,}", "\n\n", text)


def _extract_tables_by_page(
    raw_bytes: bytes,
    id_gen: TableIdGenerator,
) -> dict[int, list[tuple[tuple[float, float, float, float], str]]]:
    """Находит таблицы в PDF через pdfplumber и готовит Markdown-рендер.

    Returns:
        Словарь ``{page_index: [(bbox, markdown), ...]}``. bbox сохраняет
        координаты исходной таблицы — по ним PyMuPDF-блоки, попадающие внутрь,
        будут отфильтрованы, а Markdown-блок вставлен в правильное y-место.
        Если pdfplumber не смог открыть PDF — возвращает пустой словарь и пишет
        warning, чтобы не блокировать извлечение обычного текста.
    """
    result: dict[int, list[tuple[tuple[float, float, float, float], str]]] = {}
    try:
        pdf = pdfplumber.open(io.BytesIO(raw_bytes))
    except Exception as exc:  # pdfplumber бросает разные типы, не только ValueError
        logger.warning("pdfplumber не смог открыть PDF: %s — таблицы не будут извлечены", exc)
        return result

    with pdf:
        for page_idx, page in enumerate(pdf.pages):
            try:
                found = page.find_tables()
            except Exception as exc:
                logger.warning(
                    "pdfplumber.find_tables() упал на странице %d: %s — пропускаем",
                    page_idx,
                    exc,
                )
                continue

            page_tables: list[tuple[tuple[float, float, float, float], str]] = []
            for table in found:
                try:
                    rows = table.extract()
                except Exception as exc:
                    logger.debug("table.extract() упал на странице %d: %s", page_idx, exc)
                    continue

                if not rows:
                    continue
                # Отбрасываем пустышки, которые pdfplumber иногда находит на странице
                # с линиями без содержимого.
                if not any(any(cell for cell in row if cell) for row in rows):
                    continue

                md = format_table(rows, id_gen.next_id())
                if md:
                    page_tables.append((tuple(table.bbox), md))  # type: ignore[arg-type]

            if page_tables:
                result[page_idx] = page_tables
    return result


def _block_center_inside_any(
    block: tuple[float, float, float, float],
    bboxes: list[tuple[float, float, float, float]],
) -> bool:
    """Проверяет, лежит ли центр блока внутри одного из bbox-ов таблицы."""
    x0, y0, x1, y1 = block
    cx = (x0 + x1) / 2
    cy = (y0 + y1) / 2
    for tx0, ty0, tx1, ty1 in bboxes:
        if tx0 <= cx <= tx1 and ty0 <= cy <= ty1:
            return True
    return False


class PyMuPDFExtractor(AbstractExtractor):
    """Извлекает текст из PDF через PyMuPDF, с inline-вставкой Markdown-таблиц от pdfplumber."""

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if raw_bytes[:4] != b"%PDF":
            raise ValueError(f"Байты не похожи на PDF: {url}")

        try:
            doc = fitz.open(stream=raw_bytes, filetype="pdf")
        except Exception as exc:
            raise ValueError(f"PyMuPDF не смог открыть PDF {url}: {exc}") from exc

        title: str | None = None
        meta = doc.metadata
        if meta and meta.get("title"):
            title = meta["title"].strip() or None

        id_gen = TableIdGenerator()
        tables_by_page = _extract_tables_by_page(raw_bytes, id_gen)

        pages: list[str] = []
        for page_idx, page in enumerate(doc):
            page_tables = tables_by_page.get(page_idx, [])
            table_bboxes = [bbox for bbox, _ in page_tables]

            blocks = page.get_text("blocks")

            # (y0, x0, kind, payload) — общая очередь на сортировку
            items: list[tuple[float, float, str, str]] = []

            for block in blocks:
                # block: (x0, y0, x1, y1, text, block_no, block_type)
                if block[6] != 0:
                    continue
                block_text = block[4].strip()
                if not block_text:
                    continue
                if _block_center_inside_any((block[0], block[1], block[2], block[3]), table_bboxes):
                    # Содержимое этой области будет вставлено Markdown-таблицей.
                    continue
                items.append((block[1], block[0], "text", _clean_hyphenation(block_text)))

            for bbox, md in page_tables:
                items.append((bbox[1], bbox[0], "table", md))

            items.sort(key=lambda item: (item[0], item[1]))

            page_parts: list[str] = []
            for _, _, kind, payload in items:
                if kind == "table":
                    # Пустые строки вокруг блока отделяют таблицу от соседних абзацев.
                    page_parts.append("")
                    page_parts.append(payload)
                    page_parts.append("")
                else:
                    page_parts.append(payload)

            page_text = "\n".join(page_parts).strip()
            if page_text:
                pages.append(page_text)

        text = "\n\n".join(pages)
        text = _normalize_whitespace(text)

        if not text.strip():
            raise ValueError(f"PDF пустой или содержит только изображения: {url}")

        logger.debug(
            "PDF извлечён: %d страниц, %d символов, %d таблиц, URL=%s",
            len(doc),
            len(text),
            sum(len(v) for v in tables_by_page.values()),
            url,
        )
        return RawDocument(text=text, source_url=url, title=title, is_pdf=True)
