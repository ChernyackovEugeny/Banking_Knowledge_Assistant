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

# Одиночный пункт: «1.», «2.», «10.» в начале строки — для коротких Указаний ЦБ
# (например, 7081-У) без иерархической нумерации.
_SIMPLE_PARA_RE = re.compile(
    r"(?m)^\s*(\d+)\.\s+(.{10,})"  # минимум 10 символов, чтобы избежать нумерованных списков
)


def _chapter_id(num: str) -> str:
    return f"гл.{num}"


def _para_id(num: str) -> str:
    return f"п.{num}"


def _appendix_id(num: str | None) -> str:
    if num:
        return f"Приложение {num}"
    return "Приложение"


def _section_priority(section: Section) -> tuple[int, int, int]:
    text_cf = section.text.casefold()
    looks_service = "см. предыдущ" in text_cf or "см. будущ" in text_cf
    return (
        int(not looks_service),
        int(bool(section.text.strip())),
        len(section.children),
    )


def _dedupe_sections(sections: list[Section], *, scope: str) -> list[Section]:
    deduped: list[Section] = []
    by_id: dict[str, int] = {}

    for sec in sections:
        existing_index = by_id.get(sec.id)
        if existing_index is None:
            by_id[sec.id] = len(deduped)
            deduped.append(sec)
            continue

        current = deduped[existing_index]
        if _section_priority(sec) > _section_priority(current):
            logger.warning("CBRDocumentParser: duplicate %s section_id %s replaced", scope, sec.id)
            deduped[existing_index] = sec
        else:
            logger.warning("CBRDocumentParser: duplicate %s section_id %s skipped", scope, sec.id)

    return deduped


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
                children = self._extract_appendix_paragraphs(body, sec_id)
                sec = Section(id=sec_id, title="", text=body, children=children)

            sections.append(sec)

        sections = _dedupe_sections(sections, scope="top-level")
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
        return _dedupe_sections(children, scope="child")

    def _extract_appendix_paragraphs(self, appendix_text: str, appendix_id: str) -> list[Section]:
        """Извлекает пункты из приложения.

        Сначала пробуем иерархические пункты (1.1.), затем простые (1.).
        """
        children: list[Section] = []
        matches = list(_PARA_RE.finditer(appendix_text))
        if not matches:
            matches = list(_SIMPLE_PARA_RE.finditer(appendix_text))
        for j, pm in enumerate(matches):
            para_num = pm.group(1)
            para_start = pm.start()
            para_end = matches[j + 1].start() if j + 1 < len(matches) else len(appendix_text)
            para_text = appendix_text[para_start:para_end].strip()
            # В приложениях одинаковая нумерация пунктов встречается часто,
            # поэтому включаем id приложения в section_id дочернего узла.
            child_id = f"{appendix_id}:{_para_id(para_num)}"
            children.append(Section(id=child_id, title="", text=para_text))
        return _dedupe_sections(children, scope="appendix-child")

    def _parse_paragraphs_only(self, text: str, source_url: str) -> list[Section]:
        """Fallback для документов без явных глав — только пункты верхнего уровня.

        Сначала ищет иерархические пункты (1.1., 2.3. и т.д.).
        Если не нашёл — пробует одиночные номера (1., 2., 3.) для коротких
        Указаний ЦБ без иерархической нумерации (например, 7081-У).
        """
        sections: list[Section] = []
        matches = list(_PARA_RE.finditer(text))

        if not matches:
            # Попытка разобрать пункты вида «1. Текст», «2. Текст»
            matches = list(_SIMPLE_PARA_RE.finditer(text))
            if matches:
                logger.warning(
                    "CBRDocumentParser (fallback): нет глав и иерархических пунктов, "
                    "найдено %d простых пунктов (1., 2., ...) из %s",
                    len(matches), source_url,
                )
                for i, pm in enumerate(matches):
                    para_num = pm.group(1)
                    start = pm.start()
                    end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
                    sec_text = text[start:end].strip()
                    sections.append(Section(id=_para_id(para_num), title="", text=sec_text))
                return _dedupe_sections(sections, scope="fallback")

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
        return _dedupe_sections(sections, scope="fallback")


# ---------------------------------------------------------------------------
# PlanOfAccountsParser — специализированный парсер для 579-П
# ---------------------------------------------------------------------------

# Разделы плана счетов: «А. Балансовые счета», «Б. Счета доверительного управления» и т.д.
_SECTION_LETTER_RE = re.compile(
    r"(?m)^\s*(?:ГЛАВ[АЕ]|Глав[ае])?\s*([А-ЯЁ])\.\s+(.*?)$"
)

# Счёт: строка начинается с 3-значного числа
# «441» или «441 — Кредиты...»
_ACCOUNT_RE = re.compile(r"(?m)^\s*(\d{3,5})\s*$\n\s*([^\n]+)")


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

        unique_anchors: list[re.Match[str]] = []
        seen_letters: set[str] = set()
        for match in anchors:
            letter = match.group(1)
            if letter in seen_letters:
                continue
            seen_letters.add(letter)
            unique_anchors.append(match)
        anchors = unique_anchors

        sections: list[Section] = []
        duplicate_accounts_within_sections = 0
        for i, m in enumerate(anchors):
            letter = m.group(1)
            sec_title = m.group(2).strip()
            body_start = m.end()
            body_end = anchors[i + 1].start() if i + 1 < len(anchors) else len(text)
            body = text[body_start:body_end].strip()

            children_raw: list[Section] = []
            account_matches = list(_ACCOUNT_RE.finditer(body))
            for j, am in enumerate(account_matches):
                acc_num = am.group(1)
                acc_title = am.group(2).strip()
                acc_start = am.start()
                acc_end = account_matches[j + 1].start() if j + 1 < len(account_matches) else len(body)
                # Сохраняем весь блок счета до следующего номера, а не только заголовок.
                acc_text = body[acc_start:acc_end].strip()
                children_raw.append(Section(id=f"сч.{acc_num}", title=acc_title, text=acc_text))

            # В пределах раздела оставляем наиболее информативный вариант счета.
            by_id: dict[str, Section] = {}
            for child in children_raw:
                existing = by_id.get(child.id)
                if existing is None:
                    by_id[child.id] = child
                    continue
                duplicate_accounts_within_sections += 1
                if len(child.text) > len(existing.text):
                    by_id[child.id] = child
            children = list(by_id.values())

            sections.append(Section(
                id=f"гл.{letter}",
                title=sec_title,
                text=body,
                children=children,
            ))

        # Между разделами также выбираем наиболее содержательный вариант счета.
        seen_child_ids: dict[str, Section] = {}
        duplicate_accounts_across_sections = 0
        for sec in sections:
            unique_children: list[Section] = []
            for child in sec.children:
                existing = seen_child_ids.get(child.id)
                if existing is not None:
                    duplicate_accounts_across_sections += 1
                    if len(child.text) > len(existing.text):
                        existing.text = child.text
                        existing.title = child.title
                    continue
                seen_child_ids[child.id] = child
                unique_children.append(child)
            sec.children = unique_children

        sections = _dedupe_sections(sections, scope="top-level")
        if duplicate_accounts_within_sections or duplicate_accounts_across_sections:
            logger.info(
                "PlanOfAccountsParser: deduped duplicate accounts (within sections=%d, across sections=%d)",
                duplicate_accounts_within_sections,
                duplicate_accounts_across_sections,
            )
        logger.info("PlanOfAccountsParser: %d разделов из %s", len(sections), raw_doc.source_url)
        return sections


# ---------------------------------------------------------------------------
# ReportingFormsParser — специализированный парсер для 6406-У
# ---------------------------------------------------------------------------

# Форма отчётности: начинается с «0409» + 3 цифры
_FORM_RE = re.compile(r"(?m)^\s*(0409\d{3})\b.*?$")
# Явный заголовок формы: "Форма 0409xxx"
_FORM_HEADER_RE = re.compile(r"(?im)^\s*форма\s+(0409\d{3})\b")
# Внутри бланка: "Код формы по ОКУД ... 0409xxx"
_FORM_CODE_RE = re.compile(r"(?im)код\s+формы\s+по\s+окуд[^\n]{0,80}?(0409\d{3})\b")


class ReportingFormsParser(AbstractSectionParser):
    """Парсер 6406-У (формы отчётности ЦБ).

    Создаёт секции с id «форма 0409115», «форма 0409135» и т.д.
    Алиасы «формы 0409115» (мн.ч.) и «0409115» (без префикса) добавляются в output.py.
    """

    def parse(self, raw_doc: RawDocument) -> list[Section]:
        text = raw_doc.text
        header_matches = list(_FORM_HEADER_RE.finditer(text))
        code_matches = list(_FORM_CODE_RE.finditer(text))

        # Предпочитаем явные заголовки "Форма 0409xxx"; если их нет —
        # используем маркеры "Код формы по ОКУД ... 0409xxx".
        matches = header_matches or code_matches
        if not matches:
            # Последний fallback: старый паттерн (только число в начале строки).
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

        if not anchors:
            return CBRDocumentParser().parse(raw_doc)

        sections: list[Section] = []
        for i, (pos, form_num) in enumerate(anchors):
            eol = text.find("\n", pos)
            body_start = eol + 1 if eol != -1 else pos
            body_end = anchors[i + 1][0] if i + 1 < len(anchors) else len(text)
            body = text[body_start:body_end].strip()
            sections.append(Section(id=f"форма {form_num}", title=form_num, text=body))

        logger.info("ReportingFormsParser: %d форм из %s", len(sections), raw_doc.source_url)
        return sections
