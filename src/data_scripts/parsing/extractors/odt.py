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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ODF XML namespace
# ---------------------------------------------------------------------------

_NS = {
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "meta": "urn:oasis:names:tc:opendocument:xmlns:meta:1.0",
    "dc": "http://purl.org/dc/elements/1.1/",
}

_TEXT_P  = f"{{{_NS['text']}}}p"
_TEXT_H  = f"{{{_NS['text']}}}h"
_TEXT_S  = f"{{{_NS['text']}}}s"          # &nbsp;-пробелы
_TEXT_TAB = f"{{{_NS['text']}}}tab"       # табуляция
_TEXT_LB = f"{{{_NS['text']}}}line-break" # явный перенос строки внутри абзаца


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

        # Собираем параграфы и заголовки в порядке появления в XML
        lines: list[str] = []
        for elem in root.iter(_TEXT_P, _TEXT_H):
            text = _element_text(elem)
            # Убираем лишние пробелы внутри строки, но сохраняем явные переносы
            text = re.sub(r"[ \t]+", " ", text).strip()
            lines.append(text)  # Пустые строки сохраняем — они разделяют абзацы

        # Убираем более двух пустых строк подряд, склеиваем в итоговый текст
        text = "\n".join(lines)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        if not text:
            raise ValueError(f"ODTExtractor: документ пустой или не содержит текстовых элементов: {url}")

        logger.debug("ODTExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(text=text, source_url=url, title=title, is_odt=True)
