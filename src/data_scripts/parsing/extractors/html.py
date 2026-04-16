"""HTML-экстракторы для разных источников правовых документов.

Каждый класс заточен под структуру конкретного сайта:
- ConsultantExtractor  — consultant.ru
- PravoGovExtractor    — pravo.gov.ru (официальный портал НПА)
- CBRExtractor         — cbr.ru (авто-детект HTML vs PDF)
- GarantExtractor      — garant.ru / base.garant.ru
- CNTDExtractor        — docs.cntd.ru
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime

from bs4 import BeautifulSoup, Tag

from extractors.base import AbstractExtractor, RawDocument

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _soup(raw_bytes: bytes) -> BeautifulSoup:
    """Parse HTML bytes robustly across requests and Playwright sources.

    Playwright returns the DOM as a Unicode string. When we encode that string
    back to UTF-8 bytes, the original page may still contain a stale
    ``<meta charset=windows-1251>`` tag. If we pass those bytes directly into
    BeautifulSoup/lxml, the parser trusts the stale meta tag and decodes the
    whole document incorrectly, which breaks CSS selectors.

    To avoid that, prefer parsing as decoded UTF-8 text when the byte stream is
    valid UTF-8. For regular HTTP responses in legacy encodings this decode
    fails, and we fall back to the original byte-based parsing so lxml can use
    the document-declared charset.
    """
    try:
        return BeautifulSoup(raw_bytes.decode("utf-8"), "lxml")
    except UnicodeDecodeError:
        return BeautifulSoup(raw_bytes, "lxml")


def _tag_to_text(tag: Tag) -> str:
    """Превращает HTML-тег в чистый текст, сохраняя переносы строк у блочных элементов."""
    # Заменяем блочные теги переносами, чтобы сохранить структуру абзацев
    for block in tag.find_all(["p", "div", "li", "br", "h1", "h2", "h3", "h4", "h5", "h6"]):
        block.insert_before("\n")

    text = tag.get_text(separator="")

    # Убираем избыточные пробелы внутри строк, но сохраняем переносы
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    text = "\n".join(lines)
    
    # Убираем более двух пустых строк подряд
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _check_pdf_magic(raw_bytes: bytes) -> bool:
    return raw_bytes[:4] == b"%PDF"


def _find_first(soup: BeautifulSoup, selectors: list[str]) -> Tag | None:
    """Возвращает первый найденный элемент из списка CSS-селекторов."""
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return el
    return None


_RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}

_DATE_NUMERIC_RE = re.compile(r"\b(\d{1,2})[./](\d{1,2})[./](\d{4})\b")
_DATE_TEXTUAL_RE = re.compile(
    r"\b(\d{1,2})\s+("
    + "|".join(_RU_MONTHS.keys())
    + r")\s+(\d{4})\s*(?:г\.|года)?\b",
    flags=re.IGNORECASE,
)
_REVISION_CONTEXT_RE = re.compile(
    r"(дата\s+последней\s+редакции|последн(?:яя|ей)\s+редакц(?:ия|ии)|ред(?:акц(?:ия|ии))?\s*от|с\s+изменениями\s+и\s+дополнениями\s+от)",
    flags=re.IGNORECASE,
)


def _normalize_ru_date(raw: str) -> str | None:
    val = raw.strip().lower()
    m_num = _DATE_NUMERIC_RE.search(val)
    if m_num:
        day, month, year = map(int, m_num.groups())
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    m_txt = _DATE_TEXTUAL_RE.search(val)
    if not m_txt:
        return None

    day = int(m_txt.group(1))
    month_name = m_txt.group(2).lower()
    year = int(m_txt.group(3))
    month = _RU_MONTHS.get(month_name)
    if not month:
        return None
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _extract_dates_from_text(text: str) -> list[str]:
    candidates: list[str] = []
    for m in _DATE_NUMERIC_RE.finditer(text):
        norm = _normalize_ru_date(m.group(0))
        if norm:
            candidates.append(norm)
    for m in _DATE_TEXTUAL_RE.finditer(text):
        norm = _normalize_ru_date(m.group(0))
        if norm:
            candidates.append(norm)
    return candidates


def extract_garant_last_revision_date(soup: BeautifulSoup) -> str | None:
    """Extract last revision date from garant/base.garant page as ISO YYYY-MM-DD."""
    sources: list[str] = []
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    if title:
        sources.append(title)
    for meta_name in ("description", "keywords"):
        meta = soup.find("meta", attrs={"name": meta_name})
        if meta and isinstance(meta, Tag):
            content = (meta.get("content") or "").strip()
            if content:
                sources.append(content)
    sources.append(soup.get_text(" ", strip=True))

    dated_contexts: list[str] = []
    for src in sources:
        for m in _REVISION_CONTEXT_RE.finditer(src):
            window = src[max(0, m.start() - 60): min(len(src), m.end() + 220)]
            dated_contexts.append(window)

    context_dates: list[str] = []
    for ctx in dated_contexts:
        context_dates.extend(_extract_dates_from_text(ctx))

    if not context_dates:
        return None

    today = datetime.now().date().isoformat()
    not_future = [d for d in context_dates if d <= today]
    pool = not_future or context_dates
    return max(pool)


# ---------------------------------------------------------------------------
# Экстракторы
# ---------------------------------------------------------------------------

class ConsultantExtractor(AbstractExtractor):
    """Извлекает текст документа с consultant.ru.

    КонсультантПлюс возвращает текст НПА в div с классами document / DocumentMainPart.
    Навигация, сноски и реклама удаляются перед конвертацией в текст.
    """

    _CONTENT_SELECTORS = [
        "div.document-page__content",
        "div.content.document-page",
        "div.document",
        "div.DocumentMainPart",
        "div#content",
        "div.document__content",
    ]
    _NOISE_SELECTORS = [
        "nav", ".doc-toc", ".doc-sidebar", ".ads", ".footer",
        "script", "style", ".footnote", ".docFootnote",
        "[class*='popup']", "[class*='Popup']", "[class*='banner']",
    ]

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if _check_pdf_magic(raw_bytes):
            from extractors.pdf import PyMuPDFExtractor
            return PyMuPDFExtractor().extract(raw_bytes, url)

        soup = _soup(raw_bytes)

        # Удаляем шум
        for noise in soup.select(", ".join(self._NOISE_SELECTORS)):
            noise.decompose()

        # Детектируем JS-блокировку: consultant.ru отдаёт пустую страницу
        # с метатегом refresh или минимальным body при антискрапинге
        body_text = soup.get_text(strip=True)
        if len(body_text) < 500:
            raise ValueError(
                f"ConsultantExtractor: слишком короткий ответ ({len(body_text)} символов) — "
                "вероятно, JS-рендеринг или антискрапинг. Используй другой источник."
            )

        container = _find_first(soup, self._CONTENT_SELECTORS)
        if not container:
            raise ValueError(
                f"ConsultantExtractor: не найден контейнер документа на {url}. "
                "Возможно, сайт заблокировал запрос или изменил разметку."
            )

        title_tag = soup.select_one("h1, .doc-title, .document__title")
        title = title_tag.get_text(strip=True) if title_tag else None

        text = _tag_to_text(container)
        logger.debug("ConsultantExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(text=text, source_url=url, title=title)


class KonturExtractor(AbstractExtractor):
    """Извлекает текст документа с normativ.kontur.ru."""

    _CONTENT_SELECTORS = [
        "div#js-doc-text",
        "div.doc_text",
        "div#js-doc-container",
        "div.doc_container",
        "div#js-doc-frame-container",
    ]
    _NOISE_SELECTORS = [
        "script",
        "style",
        "nav",
        "header",
        "footer",
        ".sidebar-block",
        ".doc_toolbar",
        ".doc_titlebar-toggle",
        ".doc_frame-scroll-shadow",
    ]

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if _check_pdf_magic(raw_bytes):
            from extractors.pdf import PyMuPDFExtractor
            return PyMuPDFExtractor().extract(raw_bytes, url)

        soup = _soup(raw_bytes)

        for noise in soup.select(", ".join(self._NOISE_SELECTORS)):
            noise.decompose()

        container = _find_first(soup, self._CONTENT_SELECTORS)
        if not container:
            raise ValueError(f"KonturExtractor: контейнер документа не найден на {url}")

        title_tag = soup.select_one("h1.doc_titlebar-title")
        title = title_tag.get_text(strip=True) if title_tag else None

        raw_text = container.get_text(separator="\n")
        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in raw_text.splitlines()]
        text = "\n".join(line for line in lines if line)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        logger.debug("KonturExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(text=text, source_url=url, title=title)


class PravoGovExtractor(AbstractExtractor):
    """Извлекает текст с pravo.gov.ru.

    Официальный портал НПА использует простую HTML-разметку.
    Основной контент в <div id='document'> или <body>.
    """

    _CONTENT_SELECTORS = [
        "div#document",
        "div.document",
        "div#content",
        "article",
        "body",
    ]

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if _check_pdf_magic(raw_bytes):
            from extractors.pdf import PyMuPDFExtractor
            return PyMuPDFExtractor().extract(raw_bytes, url)

        soup = _soup(raw_bytes)

        for noise in soup.select("nav, header, footer, script, style"):
            noise.decompose()

        container = _find_first(soup, self._CONTENT_SELECTORS)
        if not container:
            raise ValueError(f"PravoGovExtractor: контейнер не найден на {url}")

        text = _tag_to_text(container)
        logger.debug("PravoGovExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(text=text, source_url=url)


class CBRExtractor(AbstractExtractor):
    """Извлекает документы с cbr.ru.

    cbr.ru отдаёт документы как в HTML, так и в PDF. Авто-определяем формат.
    HTML-документы ЦБ обычно имеют минимальную разметку.
    """

    _CONTENT_SELECTORS = [
        "div.DocumentPage",
        "div.document-body",
        "div#doc-content",
        "div.body",
        "div[class*='document']",
        "article",
        "main",
    ]

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if _check_pdf_magic(raw_bytes):
            from extractors.pdf import PyMuPDFExtractor
            return PyMuPDFExtractor().extract(raw_bytes, url)

        soup = _soup(raw_bytes)

        for noise in soup.select("nav, header, footer, script, style, .breadcrumb"):
            noise.decompose()

        container = _find_first(soup, self._CONTENT_SELECTORS)
        if not container:
            # Fallback: body
            container = soup.find("body")
            if not container:
                raise ValueError(f"CBRExtractor: контейнер не найден на {url}")
            logger.warning("CBRExtractor: используем <body> как fallback для %s", url)

        text = _tag_to_text(container)
        logger.debug("CBRExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(text=text, source_url=url)


class GarantExtractor(AbstractExtractor):
    """Извлекает текст с garant.ru / base.garant.ru.

    Garant.ru на главной странице документа показывает только оглавление.
    Полный текст доступен по URL с суффиксом /print/.
    Если переданный URL уже оканчивается на /print/ — используем его напрямую,
    иначе пробуем подставить /print/ и возвращаем как print_url в RawDocument,
    чтобы parsing.py мог перефетчить при необходимости.

    Наиболее надёжный способ: сразу передавать URL с /print/ в registry.py.
    """

    _CONTENT_SELECTORS = [
        "div.docBody",
        "div#docBody",
        "div.document-text",
        "div[id*='doc']",
        "article",
        "body",
    ]
    # Минимальное количество символов, чтобы считать страницу содержательной
    _MIN_TEXT_LEN = 3000

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if _check_pdf_magic(raw_bytes):
            from extractors.pdf import PyMuPDFExtractor
            return PyMuPDFExtractor().extract(raw_bytes, url)

        soup = _soup(raw_bytes)

        for noise in soup.select("nav, header, footer, script, style, .ads, .banner"):
            noise.decompose()

        container = _find_first(soup, self._CONTENT_SELECTORS)
        if not container:
            raise ValueError(f"GarantExtractor: контейнер не найден на {url}")

        text = _tag_to_text(container)

        # Garant возвращает TOC-only страницу без полного текста
        if len(text) < self._MIN_TEXT_LEN and not url.rstrip("/").endswith("/print"):
            raise ValueError(
                f"GarantExtractor: слишком мало текста ({len(text)} символов) на {url}. "
                "Используй URL с суффиксом /print/ для полного текста, "
                f"напр.: {url.rstrip('/')}/print/"
            )

        logger.debug("GarantExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(
            text=text,
            source_url=url,
        )


class CNTDExtractor(AbstractExtractor):
    """Извлекает текст с docs.cntd.ru."""

    _CONTENT_SELECTORS = [
        "div.document",
        "div#document",
        "div.doc-content",
        "div[id*='content']",
    ]

    def extract(self, raw_bytes: bytes, url: str) -> RawDocument:
        if _check_pdf_magic(raw_bytes):
            from extractors.pdf import PyMuPDFExtractor
            return PyMuPDFExtractor().extract(raw_bytes, url)

        soup = _soup(raw_bytes)

        for noise in soup.select("nav, header, footer, script, style"):
            noise.decompose()

        container = _find_first(soup, self._CONTENT_SELECTORS)
        if not container:
            raise ValueError(f"CNTDExtractor: контейнер не найден на {url}")

        text = _tag_to_text(container)
        logger.debug("CNTDExtractor: %d символов, URL=%s", len(text), url)
        return RawDocument(text=text, source_url=url)
