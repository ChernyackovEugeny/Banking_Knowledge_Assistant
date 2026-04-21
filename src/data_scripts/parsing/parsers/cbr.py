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
from tables import mask_tables

logger = logging.getLogger(__name__)


class _AnchoredLineMatch:
    """Лёгкая обёртка над line-level regex match с абсолютными offsets."""

    def __init__(self, match: re.Match[str], start: int, end: int) -> None:
        self._match = match
        self._start = start
        self._end = end

    def group(self, *args):
        return self._match.group(*args)

    def start(self) -> int:
        return self._start

    def end(self) -> int:
        return self._end

# ---------------------------------------------------------------------------
# Регулярные выражения
# ---------------------------------------------------------------------------

# Глава: «Глава 1.», «Глава 2», «ГЛАВА 5. Нормативы»
_CHAPTER_RE = re.compile(
    r"(?m)^\s*(?:ГЛАВ[АЕ]|Глав[ае])\s+"
    r"(\d+)"
    r"\s*\.?\s*(.*?)$"
)
_CHAPTER_LINE_RE = re.compile(
    r"^\s*(?:ГЛАВ[АЕ]|Глав[ае])\s+"
    r"(\d+)"
    r"\s*\.?\s*(.*?)\s*$"
)

# Приложение: «Приложение», «Приложение 1», «Приложение N 1», «Приложение к...»
_APPENDIX_RE = re.compile(
    r"(?m)^\s*(?:ПРИЛОЖЕНИЕ|Приложение)"
    r"(?:\s+(?:N\s*)?(\d+))?"             # необязательный номер: Приложение 1 или Приложение N 1
    r"\s*(?:к\s+.+)?$"                    # необязательное «к ...»
)
_APPENDIX_LINE_RE = re.compile(
    r"^\s*(?:ПРИЛОЖЕНИЕ|Приложение)"
    r"(?:\s+(?:N\s*)?(\d+))?"
    r"\s*(?:к\s+.+)?\s*$"
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


def _top_level_section_priority(section: Section) -> tuple[int, int, int, int, int, int]:
    text = section.text.lstrip()
    text_cf = text.casefold()
    looks_service = "см. предыдущ" in text_cf or "см. будущ" in text_cf

    starts_with_heading = 0
    heading_matches_id = 0
    starts_with_expected_para = 0

    if section.id.startswith("гл."):
        chapter_num = section.id.split(".", 1)[1]
        starts_with_heading = int(text_cf.startswith("глава"))
        heading_matches_id = int(bool(re.match(
            rf"^\s*(?:ГЛАВ[АЕ]|Глав[ае])\s+{re.escape(chapter_num)}\b",
            text,
        )))
        starts_with_expected_para = int(bool(re.match(
            rf"^\s*{re.escape(chapter_num)}(?:\.\d+)+\.",
            text,
        )))
    elif section.id.startswith("Приложение"):
        starts_with_heading = int(text_cf.startswith("приложение"))
        appendix_num = section.id.removeprefix("Приложение").strip()
        if appendix_num:
            heading_matches_id = int(bool(re.match(
                rf"^\s*(?:ПРИЛОЖЕНИЕ|Приложение)\s+(?:N\s*)?{re.escape(appendix_num)}\b",
                text,
            )))
        else:
            heading_matches_id = int(bool(re.match(
                r"^\s*(?:ПРИЛОЖЕНИЕ|Приложение)\b",
                text,
            )))

    return (
        heading_matches_id,
        starts_with_heading,
        starts_with_expected_para,
        int(not looks_service),
        min(len(section.children), 999),
        min(len(text), 250_000),
    )


def _looks_like_change_note(title: str) -> bool:
    title_cf = title.strip().casefold()
    return title_cf.startswith((
        "дополнен ",
        "дополнена",
        "дополнено",
        "дополнены",
        "изменен ",
        "изменена",
        "изменено",
        "изменены",
        "изложен ",
        "изложена",
        "изложено",
        "изложены",
        "утратил",
        "утратила",
        "утратило",
        "утратили",
        "признан ",
        "признана",
        "признано",
        "признаны",
    ))


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
        current_priority = (
            _top_level_section_priority(current)
            if scope == "top-level"
            else _section_priority(current)
        )
        candidate_priority = (
            _top_level_section_priority(sec)
            if scope == "top-level"
            else _section_priority(sec)
        )
        if candidate_priority > current_priority:
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
        anchors = self._find_top_level_anchors(text)

        anchors.sort(key=lambda a: a[0])

        if not anchors:
            return self._parse_paragraphs_only(text, raw_doc.source_url)

        sections: list[Section] = []
        for i, (pos, kind, num, title) in enumerate(anchors):
            body_end = anchors[i + 1][0] if i + 1 < len(anchors) else len(text)
            body = text[pos:body_end].strip()

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

    def _find_top_level_anchors(self, text: str) -> list[tuple[int, str, str, str]]:
        """Ищет главы и приложения построчно.

        Для ODT/PDF-версий документов ЦБ посрочный поиск даёт стабильнее границы
        top-level секций, чем полный `finditer()` по всему тексту.
        """
        masked_text = mask_tables(text)
        anchors: list[tuple[int, str, str, str]] = []
        offset = 0
        for line in masked_text.splitlines(keepends=True):
            chapter_match = _CHAPTER_LINE_RE.match(line)
            if chapter_match:
                title = chapter_match.group(2).strip()
                if _looks_like_change_note(title):
                    offset += len(line)
                    continue
                anchors.append((
                    offset + chapter_match.start(),
                    "chapter",
                    chapter_match.group(1),
                    title,
                ))
            else:
                appendix_match = _APPENDIX_LINE_RE.match(line)
                if appendix_match:
                    anchors.append((
                        offset + appendix_match.start(),
                        "appendix",
                        appendix_match.group(1) or "",
                        "",
                    ))
            offset += len(line)
        return anchors

    def _extract_paragraphs(self, chapter_text: str) -> list[Section]:
        """Извлекает пункты внутри текста главы."""
        children: list[Section] = []
        masked_text = mask_tables(chapter_text)
        matches = list(_PARA_RE.finditer(masked_text))
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
        masked_text = mask_tables(appendix_text)
        matches = list(_PARA_RE.finditer(masked_text))
        if not matches:
            matches = list(_SIMPLE_PARA_RE.finditer(masked_text))
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
        masked_text = mask_tables(text)
        matches = list(_PARA_RE.finditer(masked_text))

        if not matches:
            # Попытка разобрать пункты вида «1. Текст», «2. Текст»
            matches = list(_SIMPLE_PARA_RE.finditer(masked_text))
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
_SECTION_LETTER_LINE_RE = re.compile(
    r"^\s*(?:ГЛАВ[АЕ]|Глав[ае])?\s*([А-ЯЁ])\.\s+(.*?)\s*$"
)

# Счёт: строка начинается с 3-значного числа
# «441» или «441 — Кредиты...»
_ACCOUNT_RE = re.compile(r"(?m)^\s*(\d{3,5})\s*$\n\s*([^\n]+)")
_ACCOUNT_INLINE_RE = re.compile(
    r"(?:Счет|Счета)\s*N\s*(\d{3,5})\b(?:\s+\"([^\"]+)\")?",
    flags=re.IGNORECASE,
)
_ACCOUNT_N_RE = re.compile(r"\bN\s*(\d{3,5})\b")


class PlanOfAccountsParser(AbstractSectionParser):
    """Парсер 579-П (план счетов).

    Создаёт секции по буквенным разделам (А, Б, В...) с id «гл.А», «гл.Б» и т.д.
    Внутри каждого раздела — диапазоны счетов как children.

    Диапазонные ID вида «сч.441-458» строятся в output.py через alias-резолвер.
    """

    def parse(self, raw_doc: RawDocument) -> list[Section]:
        text = raw_doc.text
        anchors = self._find_letter_anchors(text)

        if not anchors:
            logger.warning("PlanOfAccountsParser: буквенные разделы не найдены в %s", raw_doc.source_url)
            return []

        anchors = self._select_best_section_anchors(anchors, len(text))

        sections: list[Section] = []
        duplicate_accounts_within_sections = 0
        for i, m in enumerate(anchors):
            letter = m.group(1)
            sec_title = m.group(2).strip()
            body_start = m.end()
            body_end = anchors[i + 1].start() if i + 1 < len(anchors) else len(text)
            body = text[body_start:body_end].strip()

            children_raw = self._extract_accounts(body)
            if not children_raw:
                account_matches = list(_ACCOUNT_RE.finditer(mask_tables(body)))
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

    def _find_letter_anchors(self, text: str) -> list[re.Match[str]]:
        """Ищет буквенные главы построчно.

        Полный regex.finditer() по мегабайтным табличным документам может быть
        слишком медленным. Построчный проход линейный и устойчивый.
        """
        masked_text = mask_tables(text)
        matches: list[re.Match[str]] = []
        offset = 0
        for line in masked_text.splitlines(keepends=True):
            match = _SECTION_LETTER_LINE_RE.match(line)
            if match:
                # Восстанавливаем абсолютную позицию как будто это матч в полном тексте.
                start_in_line = match.start()
                start = offset + start_in_line
                end = offset + match.end()
                matches.append(_AnchoredLineMatch(match, start, end))
            offset += len(line)
        return matches

    def _select_best_section_anchors(
        self,
        anchors: list[re.Match[str]],
        text_len: int,
    ) -> list[re.Match[str]]:
        """Выбирает лучший согласованный набор anchor-кандидатов для буквенных глав.

        В документах формата "буквенные главы" один и тот же набор разделов может
        повторяться несколько раз:
        оглавление, тело документа, затем повтор в приложении/служебном блоке.
        Поэтому нельзя просто брать первое или последнее совпадение по букве.

        Алгоритм общий:
        1. Разбиваем поток anchors на последовательности, где буквы не повторяются.
           Как только буква повторилась — это начало нового прогона.
        2. Для каждого прогона оцениваем размеры секций между соседними anchors.
        3. Выбираем наиболее содержательный прогон целиком.

        Такой подход не привязан к конкретному набору букв и работает для
        документов того же формата, даже если разделов больше четырёх.
        """
        if not anchors:
            return anchors

        sequences: list[list[re.Match[str]]] = []
        current: list[re.Match[str]] = []
        seen_letters: set[str] = set()
        for match in anchors:
            letter = match.group(1)
            if current and letter in seen_letters:
                sequences.append(current)
                current = [match]
                seen_letters = {letter}
                continue
            current.append(match)
            seen_letters.add(letter)
        if current:
            sequences.append(current)

        candidates: list[tuple[tuple[int, int, int, int], list[re.Match[str]]]] = []
        for idx, seq in enumerate(sequences):
            if len(seq) < 2:
                continue
            next_seq_start = sequences[idx + 1][0].start() if idx + 1 < len(sequences) else text_len
            sizes: list[int] = []
            for pos, match in enumerate(seq):
                end = seq[pos + 1].start() if pos + 1 < len(seq) else next_seq_start
                sizes.append(end - match.start())

            section_scores = [self._score_plan_section(size) for size in sizes]
            score = (
                sum(section_scores),
                len(seq),
                sum(1 for size in sizes if size >= 5_000),
                min(sizes),
                -max(sizes),
            )
            candidates.append((score, seq))

        if not candidates:
            return anchors

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    @staticmethod
    def _score_plan_section(size: int) -> int:
        """Грубая оценка качества кандидата по размеру раздела.

        Нам нужны разделы не пустые, не следы оглавления и не микрофрагменты.
        При этом один огромный монолитный блок хуже, чем несколько содержательных
        секций сопоставимого масштаба.
        """
        if size < 1_000:
            return -10
        if size < 5_000:
            return -2
        if size < 25_000:
            return 3
        if size < 250_000:
            return 6
        if size < 500_000:
            return 4
        return 1

    def _extract_accounts(self, section_text: str) -> list[Section]:
        """Извлекает счета из markdown-таблиц.

        В ODT-версии 579-П большая часть плана счетов приходит как markdown-таблицы
        с первым столбцом `Номер счета`. Разбор через `iter_table_blocks()` на
        мегабайтных документах получается слишком дорогим, поэтому здесь идём
        линейно по строкам и извлекаем account rows напрямую.
        """
        accounts: list[Section] = []
        seen_ids: set[str] = set()

        for line in section_text.splitlines():
            row = self._split_markdown_row(line)
            if not row:
                for match in _ACCOUNT_INLINE_RE.finditer(line):
                    account_num = match.group(1).strip()
                    account_id = f"сч.{account_num}"
                    if account_id in seen_ids:
                        continue
                    title = (match.group(2) or "").strip()
                    accounts.append(Section(id=account_id, title=title, text=line.strip()))
                    seen_ids.add(account_id)
                # Иногда строка выглядит как "Счета: N 90601 ... N 90602 ..."
                # и только первый N идёт с явным префиксом "Счета". Дособираем
                # остальные номера из той же строки.
                if ("Счет" in line or "Счета" in line) and len(seen_ids) < 20_000:
                    for match in _ACCOUNT_N_RE.finditer(line):
                        account_num = match.group(1).strip()
                        account_id = f"сч.{account_num}"
                        if account_id in seen_ids:
                            continue
                        accounts.append(Section(id=account_id, title="", text=line.strip()))
                        seen_ids.add(account_id)
                continue
            account_num = row[0].strip()
            if re.fullmatch(r"\d{3,5}", account_num):
                account_id = f"сч.{account_num}"
                if account_id in seen_ids:
                    continue
                title = row[1].strip() if len(row) > 1 else ""
                row_text = " | ".join(cell for cell in row if cell).strip()
                accounts.append(Section(id=account_id, title=title, text=row_text))
                seen_ids.add(account_id)

        return accounts

    @staticmethod
    def _split_markdown_row(row: str) -> list[str]:
        stripped = row.strip()
        if not stripped.startswith("|") or not stripped.endswith("|"):
            return []
        return [cell.strip().replace(r"\|", "|") for cell in stripped[1:-1].split("|")]


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
        masked_text = mask_tables(text)
        header_matches = list(_FORM_HEADER_RE.finditer(masked_text))
        code_matches = list(_FORM_CODE_RE.finditer(masked_text))

        # Предпочитаем явные заголовки "Форма 0409xxx"; если их нет —
        # используем маркеры "Код формы по ОКУД ... 0409xxx".
        matches = header_matches or code_matches
        if not matches:
            # Последний fallback: старый паттерн (только число в начале строки).
            matches = list(_FORM_RE.finditer(masked_text))

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
