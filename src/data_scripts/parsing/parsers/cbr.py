"""Парсер нормативных актов Банка России (Положения, Инструкции, Указания).

Структура документов ЦБ:
  Глава 1. Общие положения
  1.1. Настоящее Положение устанавливает...
  1.1.1. Банк обязан...
  Глава 2. Порядок идентификации
  2.1. ...
  Приложение к Положению...
  Приложение 1. ...

Нормализованные ID:
  «гл.1», «гл.2», «п.1.1», «п.1.1.1», «Приложение», «Приложение 1»

Спец-документы:
  - 579-П (план счетов): разделы А/Б/В, диапазоны счетов — отдельный парсер.
  - 6406-У (формы отчётности): идентификаторы форм 0409xxx — отдельный парсер.
"""
from __future__ import annotations

import logging
import re

from extractors.base import RawDocument
from parsers.base import AbstractSectionParser, Section

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Регулярные выражения
# ---------------------------------------------------------------------------

# Глава: «Глава 1.», «Глава 2», «ГЛАВА 5. Нормативы»
_CHAPTER_RE = re.compile(
    r"(?m)^\s*(?:ГЛАВ[АЕ]|Глав[ае])\s+"
    r"(\d+)"
    r"\s*\.?\s*(.*?)$"
)

# Приложение: «Приложение», «Приложение 1», «Приложение N 1», «Приложение к...»
_APPENDIX_RE = re.compile(
    r"(?m)^\s*(?:ПРИЛОЖЕНИЕ|Приложение)"
    r"(?:\s+(?:N\s*)?(\d+))?"             # необязательный номер: Приложение 1 или Приложение N 1
    r"\s*(?:к\s+.+)?$"                    # необязательное «к ...»
)

# Пункт: «1.1.», «2.3.4.», «10.1.» в начале строки
# Не матчим одиночные числа вида «1.» — это могут быть нумерованные списки в тексте.
_PARA_RE = re.compile(
    r"(?m)^\s*(\d+\.\d+(?:\.\d+)*)\.\s+(.{5,})"  # минимум 5 символов текста после номера
)


def _chapter_id(num: str) -> str:
    return f"гл.{num}"


def _para_id(num: str) -> str:
    return f"п.{num}"


def _appendix_id(num: str | None) -> str:
    if num:
        return f"Приложение {num}"
    return "Приложение"


# ---------------------------------------------------------------------------
# CBRDocumentParser — основной парсер для документов ЦБ
# ---------------------------------------------------------------------------

class CBRDocumentParser(AbstractSectionParser):
    """Разбивает нормативный акт ЦБ на главы и пункты.

    Алгоритм:
    1. Находит все главы (CHAPTER_RE) и приложения (APPENDIX_RE).
    2. Для каждой главы — извлекает текст до следующей главы/приложения.
    3. Внутри текста каждой главы ищет пункты (PARA_RE) и сохраняет их как children.
    4. Пункты также попадают в плоский индекс через output.flatten_sections().

    Если глав нет — парсит только пункты (fallback для документов без глав).
    """

    def parse(self, raw_doc: RawDocument) -> list[Section]:
        text = raw_doc.text

        # Собираем все «якорные» позиции: главы + приложения
        anchors: list[tuple[int, str, str, str]] = []  # (pos, kind, num, title)

        for m in _CHAPTER_RE.finditer(text):
            anchors.append((m.start(), "chapter", m.group(1), m.group(2).strip()))

        for m in _APPENDIX_RE.finditer(text):
            anchors.append((m.start(), "appendix", m.group(1) or "", m.end()))  # type: ignore[arg-type]

        anchors.sort(key=lambda a: a[0])

        if not anchors:
            return self._parse_paragraphs_only(text, raw_doc.source_url)

        sections: list[Section] = []
        for i, (pos, kind, num, title) in enumerate(anchors):
            # Начало тела секции — сразу после заголовка
            # Ищем конец строки заголовка
            eol = text.find("\n", pos)
            body_start = eol + 1 if eol != -1 else pos
            body_end = anchors[i + 1][0] if i + 1 < len(anchors) else len(text)
            body = text[body_start:body_end].strip()

            if kind == "chapter":
                sec_id = _chapter_id(num)
                children = self._extract_paragraphs(body)
                sec = Section(id=sec_id, title=str(title), text=body, children=children)
            else:  # appendix
                sec_id = _appendix_id(num if num else None)
                sec = Section(id=sec_id, title="", text=body)

            sections.append(sec)

        logger.info(
            "CBRDocumentParser: %d секций верхнего уровня из %s",
            len(sections), raw_doc.source_url,
        )
        return sections

    def _extract_paragraphs(self, chapter_text: str) -> list[Section]:
        """Извлекает пункты внутри текста главы."""
        children: list[Section] = []
        matches = list(_PARA_RE.finditer(chapter_text))
        for j, pm in enumerate(matches):
            para_num = pm.group(1)
            # Текст пункта — до следующего пункта
            para_start = pm.start()
            para_end = matches[j + 1].start() if j + 1 < len(matches) else len(chapter_text)
            para_text = chapter_text[para_start:para_end].strip()
            children.append(Section(id=_para_id(para_num), title="", text=para_text))
        return children

    def _parse_paragraphs_only(self, text: str, source_url: str) -> list[Section]:
        """Fallback для документов без явных глав — только пункты верхнего уровня."""
        sections: list[Section] = []
        matches = list(_PARA_RE.finditer(text))
        for i, pm in enumerate(matches):
            para_num = pm.group(1)
            start = pm.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            sec_text = text[start:end].strip()
            sections.append(Section(id=_para_id(para_num), title="", text=sec_text))

        logger.warning(
            "CBRDocumentParser (fallback): главы не найдены, %d пунктов из %s",
            len(sections), source_url,
        )
        return sections


# ---------------------------------------------------------------------------
# PlanOfAccountsParser — специализированный парсер для 579-П
# ---------------------------------------------------------------------------

# Разделы плана счетов: «А. Балансовые счета», «Б. Счета доверительного управления» и т.д.
_SECTION_LETTER_RE = re.compile(r"(?m)^\s*([А-ЯЁ])\.\s+(.*?)$")

# Счёт: строка начинается с 3-значного числа
# «441» или «441 — Кредиты...»
_ACCOUNT_RE = re.compile(r"(?m)^\s*(\d{3,5})\s*[-—–]\s*(.+?)$")


class PlanOfAccountsParser(AbstractSectionParser):
    """Парсер 579-П (план счетов).

    Создаёт секции по буквенным разделам (А, Б, В...) с id «гл.А», «гл.Б» и т.д.
    Внутри каждого раздела — диапазоны счетов как children.

    Диапазонные ID вида «сч.441-458» строятся в output.py через alias-резолвер.
    """

    def parse(self, raw_doc: RawDocument) -> list[Section]:
        text = raw_doc.text
        anchors = list(_SECTION_LETTER_RE.finditer(text))

        if not anchors:
            logger.warning("PlanOfAccountsParser: буквенные разделы не найдены в %s", raw_doc.source_url)
            return []

        sections: list[Section] = []
        for i, m in enumerate(anchors):
            letter = m.group(1)
            sec_title = m.group(2).strip()
            body_start = m.end()
            body_end = anchors[i + 1].start() if i + 1 < len(anchors) else len(text)
            body = text[body_start:body_end].strip()

            children: list[Section] = []
            for am in _ACCOUNT_RE.finditer(body):
                acc_num = am.group(1)
                acc_title = am.group(2).strip()
                children.append(Section(id=f"сч.{acc_num}", title=acc_title, text=acc_title))

            sections.append(Section(
                id=f"гл.{letter}",
                title=sec_title,
                text=body,
                children=children,
            ))

        logger.info("PlanOfAccountsParser: %d разделов из %s", len(sections), raw_doc.source_url)
        return sections


# ---------------------------------------------------------------------------
# ReportingFormsParser — специализированный парсер для 6406-У
# ---------------------------------------------------------------------------

# Форма отчётности: начинается с «0409» + 3 цифры
_FORM_RE = re.compile(r"(?m)^\s*(0409\d{3})\b.*?$")


class ReportingFormsParser(AbstractSectionParser):
    """Парсер 6406-У (формы отчётности ЦБ).

    Создаёт секции с id «форма 0409115», «форма 0409135» и т.д.
    Алиасы «формы 0409115» (мн.ч.) и «0409115» (без префикса) добавляются в output.py.
    """

    def parse(self, raw_doc: RawDocument) -> list[Section]:
        text = raw_doc.text
        matches = list(_FORM_RE.finditer(text))

        if not matches:
            logger.warning("ReportingFormsParser: формы 0409xxx не найдены в %s", raw_doc.source_url)
            # Fallback — общий парсер ЦБ
            return CBRDocumentParser().parse(raw_doc)

        # Дедуплицируем: одна форма может встречаться несколько раз (заголовок + описание)
        seen: set[str] = set()
        anchors: list[tuple[int, str]] = []
        for m in matches:
            form_num = m.group(1)
            if form_num not in seen:
                seen.add(form_num)
                anchors.append((m.start(), form_num))

        sections: list[Section] = []
        for i, (pos, form_num) in enumerate(anchors):
            eol = text.find("\n", pos)
            body_start = eol + 1 if eol != -1 else pos
            body_end = anchors[i + 1][0] if i + 1 < len(anchors) else len(text)
            body = text[body_start:body_end].strip()
            sections.append(Section(id=f"форма {form_num}", title=form_num, text=body))

        logger.info("ReportingFormsParser: %d форм из %s", len(sections), raw_doc.source_url)
        return sections
