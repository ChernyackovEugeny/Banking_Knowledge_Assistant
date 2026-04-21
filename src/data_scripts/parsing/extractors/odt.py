"""ODT-экстрактор на базе стандартной библиотеки (zipfile + lxml).

ODT — это ZIP-архив, внутри которого content.xml содержит текст документа
в формате ODF (Open Document Format). lxml уже является зависимостью проекта,
дополнительных пакетов не требуется.

Используется как fallback для документов с garant.ru, которые закрыты за
авторизацией и доступны только для скачивания в формате ODT.
"""
from __future__ import annotations

import io
import logging
import re
import zipfile

from lxml import etree

from extractors.base import AbstractExtractor, RawDocument
from tables import TableIdGenerator, format_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ODF XML namespace
# ---------------------------------------------------------------------------

_NS = {
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "meta": "urn:oasis:names:tc:opendocument:xmlns:meta:1.0",
    "dc": "http://purl.org/dc/elements/1.1/",
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
}

_TEXT_P  = f"{{{_NS['text']}}}p"
_TEXT_H  = f"{{{_NS['text']}}}h"
_TEXT_S  = f"{{{_NS['text']}}}s"          # &nbsp;-пробелы
_TEXT_TAB = f"{{{_NS['text']}}}tab"       # табуляция
_TEXT_LB = f"{{{_NS['text']}}}line-break" # явный перенос строки внутри абзаца

_TABLE_TABLE = f"{{{_NS['table']}}}table"
_TABLE_ROW = f"{{{_NS['table']}}}table-row"
_TABLE_CELL = f"{{{_NS['table']}}}table-cell"
_TABLE_COVERED = f"{{{_NS['table']}}}covered-table-cell"
_TABLE_COLUMNS_SPANNED = f"{{{_NS['table']}}}number-columns-spanned"
_TABLE_ROWS_SPANNED = f"{{{_NS['table']}}}number-rows-spanned"
_OFFICE_TEXT = f"{{{_NS['office']}}}text"


def _is_odt(raw_bytes: bytes) -> bool:
    """Проверяет, является ли файл ODT-архивом."""
    # ZIP magic bytes
    if raw_bytes[:2] != b"PK":
        return False
    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as z:
            # Проверяем mimetype — в ODT он обязателен
            if "mimetype" in z.namelist():
                mime = z.read("mimetype").decode("utf-8", errors="ignore").strip()
                return "opendocument" in mime
            # Если mimetype нет, но есть content.xml — тоже считаем ODT
            return "content.xml" in z.namelist()
    except zipfile.BadZipFile:
        return False


def _element_text(elem: etree._Element) -> str:
    """Рекурсивно собирает текст из ODF-элемента, учитывая специальные теги."""
    parts: list[str] = []

    # Текст самого элемента (до первого дочернего)
    if elem.text:
        parts.append(elem.text)

    for child in elem:
        tag = child.tag

        if tag == _TEXT_S:
            # <text:s text:c="3"/> — N пробелов
            count_str = child.get(f"{{{_NS['text']}}}c", "1")
            try:
                parts.append(" " * int(count_str))
            except ValueError:
                parts.append(" ")

        elif tag == _TEXT_TAB:
            parts.append("\t")

        elif tag == _TEXT_LB:
            parts.append("\n")

        else:
            # Рекурсивно для вложенных span, a, bookmark-ref и т.д.
            parts.append(_element_text(child))

        # Текст после закрывающего тега дочернего элемента (tail)
        if child.tail:
            parts.append(child.tail)

    return "".join(parts)


def _cell_text(cell_el: etree._Element) -> str:
    """Собирает текст ячейки таблицы из всех вложенных <text:p>/<text:h>.

    Каждый абзац внутри ячейки даёт самостоятельную строку визуально, но в
    pipe-таблице multiline-ячейки не поддерживаются, поэтому соединяем их
    через пробел (в итоговой нормализации ``format_table`` схлопнет пробелы).
    """
    parts: list[str] = []
    for sub in cell_el.iter(_TEXT_P, _TEXT_H):
        parts.append(_element_text(sub))
    return " ".join(p.strip() for p in parts if p.strip())


def _odt_table_to_markdown(table_el: etree._Element, id_gen: TableIdGenerator) -> str:
    """Преобразует <table:table> в Markdown pipe-формат с маркерами ⟦TABLE⟧.

    Поддерживает colspan/rowspan через дублирование содержимого ячейки во
    все spanned-позиции сетки. ``<table:covered-table-cell>`` игнорируем —
    они уже учтены занятыми позициями в grid.
    """
    grid: dict[tuple[int, int], str] = {}

    row_elems = [child for child in table_el if child.tag == _TABLE_ROW]
    for r, row_el in enumerate(row_elems):
        c = 0
        for cell_el in row_el:
            tag = cell_el.tag
            if tag == _TABLE_COVERED:
                # covered-cell — позиция, перекрытая предыдущим rowspan/colspan;
                # в grid она уже записана, пропускаем, двигая курсор колонки.
                c += 1
                continue
            if tag != _TABLE_CELL:
                continue

            while (r, c) in grid:
                c += 1

            text = _cell_text(cell_el)
            try:
                cs = max(1, int(cell_el.get(_TABLE_COLUMNS_SPANNED, "1") or 1))
                rs = max(1, int(cell_el.get(_TABLE_ROWS_SPANNED, "1") or 1))
            except (TypeError, ValueError):
                cs, rs = 1, 1

            for dr in range(rs):
                for dc in range(cs):
                    grid[(r + dr, c + dc)] = text
            c += cs

    if not grid:
        return ""

    max_row = max(r for r, _ in grid)
    max_col = max(c for _, c in grid)
    rows: list[list[str | None]] = [
        [grid.get((r, c), "") for c in range(max_col + 1)]
        for r in range(max_row + 1)
    ]
    return format_table(rows, id_gen.next_id())


def _walk_body(
    elem: etree._Element,
    lines: list[str],
    id_gen: TableIdGenerator,
) -> None:
    """Обходит тело документа в порядке XML и выдаёт строки текста.

    * ``<text:p>``/``<text:h>`` → одна строка с нормализованными пробелами.
    * ``<table:table>`` → Markdown-блок с маркерами, окружённый пустыми строками.
    * Прочие обёртки (sections, lists, frames) проходим рекурсивно, чтобы не
      потерять вложенные абзацы и таблицы, но сохранить их порядок.
    """
    for child in elem:
        tag = child.tag
        if tag in (_TEXT_P, _TEXT_H):
            text = _element_text(child)
            text = re.sub(r"[ \t]+", " ", text).strip()
            lines.append(text)
        elif tag == _TABLE_TABLE:
            md = _odt_table_to_markdown(child, id_gen)
            if md:
                # Пустые строки вокруг блока отделяют таблицу от соседних абзацев.
                lines.append("")
                lines.append(md)
                lines.append("")
        else:
            _walk_body(child, lines, id_gen)


def _extract_title(meta_root: etree._Element | None) -> str | None:
    """Ищет заголовок документа в meta-данных ODT."""
    if meta_root is None:
        return None
    for tag in (
        f"{{{_NS['dc']}}}title",
        f"{{{_NS['meta']}}}title",
    ):
        el = meta_root.find(f".//{tag}")
        if el is not None and el.text and el.text.strip():
            return el.text.strip()
    return None


class ODTExtractor(AbstractExtractor):
    """Извлекает текст из ODT-файла (Open Document Text).

    Ожидает сырые байты ODT-файла. Кладётся в data/manual_pdfs/{doc_id}.odt,
    пайплайн подхватывает его раньше сетевых источников (приоритет 0).
    """

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if not _is_odt(raw_bytes):
            raise ValueError(
                f"ODTExtractor: байты не похожи на ODT-файл: {url}"
            )

        try:
            zf = zipfile.ZipFile(io.BytesIO(raw_bytes))
        except zipfile.BadZipFile as exc:
            raise ValueError(f"ODTExtractor: повреждённый ZIP-архив: {url}") from exc

        with zf:
            # Основной контент
            try:
                content_xml = zf.read("content.xml")
            except KeyError as exc:
                raise ValueError(f"ODTExtractor: content.xml не найден в {url}") from exc

            # Метаданные (опционально)
            meta_root: etree._Element | None = None
            if "meta.xml" in zf.namelist():
                try:
                    meta_root = etree.fromstring(zf.read("meta.xml"))
                except etree.XMLSyntaxError:
                    pass

        # Разбираем content.xml
        try:
            root = etree.fromstring(content_xml)
        except etree.XMLSyntaxError as exc:
            raise ValueError(f"ODTExtractor: XML-ошибка в content.xml: {url} — {exc}") from exc

        title = _extract_title(meta_root)

        # Обходим тело документа в XML-порядке: параграфы, заголовки и таблицы
        # перемешаны в исходнике, поэтому плоский root.iter() для text:p/text:h
        # сломал бы порядок таблиц относительно окружающего текста.
        body = root.find(f".//{_OFFICE_TEXT}")
        if body is None:
            body = root

        lines: list[str] = []
        id_gen = TableIdGenerator()
        _walk_body(body, lines, id_gen)

        # Убираем более двух пустых строк подряд, склеиваем в итоговый текст
        text = "\n".join(lines)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        if not text:
            raise ValueError(f"ODTExtractor: документ пустой или не содержит текстовых элементов: {url}")

        logger.debug("ODTExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(text=text, source_url=url, title=title, is_odt=True)
