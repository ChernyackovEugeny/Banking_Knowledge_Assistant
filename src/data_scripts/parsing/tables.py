"""Единый формат таблиц для экстракторов и парсеров секций.

Все три экстрактора (html, odt, pdf) рендерят таблицы в Markdown pipe-формат,
обёрнутый в явные маркеры ⟦TABLE id=Tn⟧ ... ⟦/TABLE⟧. Маркеры нужны для
последующих этапов пайплайна:

* парсерам секций — чтобы маскировать таблицу перед поиском якорей глав/статей;
* чанкеру (будущий этап) — чтобы надёжно находить таблицы без regex-а по `|`
  и обрабатывать их отдельной стратегией (row-split с повтором
  шапки + LLM-summary через enrichment).

Формат блока:

    ⟦TABLE id=T1⟧
    | заголовок | заголовок |
    |---|---|
    | ячейка    | ячейка    |
    ⟦/TABLE⟧
"""
from __future__ import annotations

import re
from dataclasses import dataclass


TABLE_OPEN_PREFIX = "⟦TABLE"
TABLE_CLOSE = "⟦/TABLE⟧"

# Находит блок таблицы вместе с маркерами.
# Используется парсерами секций для маскирования и чанкером для извлечения.
TABLE_BLOCK_RE = re.compile(
    r"⟦TABLE\s+id=[^⟧]+⟧\n.*?\n⟦/TABLE⟧",
    flags=re.DOTALL,
)


@dataclass
class TableIdGenerator:
    """Per-document генератор идентификаторов таблиц: T1, T2, T3, ..."""

    _counter: int = 0

    def next_id(self) -> str:
        self._counter += 1
        return f"T{self._counter}"


@dataclass(frozen=True)
class TableBlock:
    """Parsed table block with original span in the source text."""

    table_id: str
    raw: str
    start: int
    end: int
    open_marker: str
    header: str
    separator: str
    rows: list[str]
    close_marker: str


def _clean_cell(text: str | None) -> str:
    """Нормализует содержимое ячейки для pipe-формата.

    * Схлопывает любые пробельные символы (включая переводы строк) в один пробел,
      иначе многострочные ячейки сломают построчный Markdown.
    * Экранирует литеральный `|`, который в pipe-таблицах является разделителем.
    """
    if text is None:
        return ""
    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    cleaned = cleaned.replace("|", r"\|")
    return cleaned


def _normalize_rows(rows: list[list[str | None]]) -> list[list[str]]:
    """Выравнивает строки по ширине и очищает ячейки."""
    if not rows:
        return []
    width = max(len(row) for row in rows)
    normalized: list[list[str]] = []
    for row in rows:
        padded = list(row) + [None] * (width - len(row))
        normalized.append([_clean_cell(cell) for cell in padded])
    return normalized


def format_table(
    rows: list[list[str | None]],
    table_id: str,
    caption: str | None = None,
) -> str:
    """Рендерит таблицу в Markdown pipe-формат с маркерами ⟦TABLE⟧.

    Args:
        rows: список строк, каждая строка — список ячеек. Первая строка
            трактуется как заголовок (под ней ставится разделитель ``|---|``).
        table_id: идентификатор таблицы в пределах документа (например, ``"T3"``).
        caption: необязательная подпись, ставится перед открывающим маркером
            как обычный текст.

    Returns:
        Готовый текстовый блок для вставки в `RawDocument.text`.
        Если после нормализации таблица пустая — возвращает пустую строку.
    """
    normalized = _normalize_rows(rows)
    if not normalized:
        return ""
    if not any(any(cell for cell in row) for row in normalized):
        return ""

    header = normalized[0]
    body = normalized[1:]

    header_line = "| " + " | ".join(header) + " |"
    separator = "| " + " | ".join(["---"] * len(header)) + " |"
    body_lines = ["| " + " | ".join(row) + " |" for row in body]

    lines = [f"⟦TABLE id={table_id}⟧", header_line, separator]
    lines.extend(body_lines)
    lines.append(TABLE_CLOSE)

    block = "\n".join(lines)
    if caption and caption.strip():
        return f"{caption.strip()}\n{block}"
    return block


def mask_tables(text: str) -> str:
    """Возвращает копию текста, в которой содержимое таблиц заменено пробелами.

    Длина результата и позиции символов вне таблиц совпадают с оригиналом,
    поэтому позиции matches от regex-ов можно сразу применять к исходному
    тексту без пересчёта смещений.

    Это нужно парсерам секций (cbr.py, federal_law.py), чтобы при поиске
    якорей глав/статей/пунктов regex-ы не срабатывали внутри табличных строк.
    """
    def _repl(m: re.Match[str]) -> str:
        span = m.group(0)
        # Сохраняем переводы строк, чтобы режим MULTILINE работал как раньше,
        # а остальные символы превращаем в пробелы для сохранения смещений.
        return "".join("\n" if ch == "\n" else " " for ch in span)

    return TABLE_BLOCK_RE.sub(_repl, text)


def iter_table_blocks(text: str) -> list[TableBlock]:
    """Return parsed table blocks with their exact spans in source text."""
    blocks: list[TableBlock] = []
    for match in TABLE_BLOCK_RE.finditer(text):
        raw = match.group(0)
        lines = raw.splitlines()
        if len(lines) < 4:
            continue

        open_marker = lines[0]
        close_marker = lines[-1]
        header = lines[1]
        separator = lines[2]
        rows = lines[3:-1]

        id_match = re.search(r"id=([^\s⟧]+)", open_marker)
        if not id_match:
            continue

        blocks.append(
            TableBlock(
                table_id=id_match.group(1),
                raw=raw,
                start=match.start(),
                end=match.end(),
                open_marker=open_marker,
                header=header,
                separator=separator,
                rows=rows,
                close_marker=close_marker,
            )
        )
    return blocks


def contains_tables(text: str) -> bool:
    """Fast check for presence of table markers."""
    return TABLE_BLOCK_RE.search(text) is not None


def extract_table_ids(text: str) -> list[str]:
    """Collect table ids in source order."""
    return [block.table_id for block in iter_table_blocks(text)]


def inject_table_summaries(text: str, summaries_by_id: dict[str, str] | None) -> str:
    """Insert compact prose summaries before matching table blocks."""
    if not summaries_by_id:
        return text

    parts: list[str] = []
    pos = 0
    for block in iter_table_blocks(text):
        parts.append(text[pos:block.start])
        summary = (summaries_by_id.get(block.table_id) or "").strip()
        if summary:
            parts.append(f"Сводка таблицы {block.table_id}: {summary}\n\n")
        parts.append(block.raw)
        pos = block.end
    parts.append(text[pos:])
    return "".join(parts)


def split_table_block(
    block: TableBlock,
    max_chars: int,
    *,
    summary: str | None = None,
) -> list[str]:
    """Split one table block by rows, repeating header in every fragment."""
    summary_prefix = ""
    if summary:
        summary_prefix = f"Сводка таблицы {block.table_id}: {summary.strip()}\n\n"

    raw_with_summary = summary_prefix + block.raw
    if len(raw_with_summary) <= max_chars:
        return [raw_with_summary]

    frame = "\n".join(
        [block.open_marker, block.header, block.separator, block.close_marker]
    )
    if len(summary_prefix) + len(frame) + 2 >= max_chars:
        summary_prefix = ""
        raw_with_summary = block.raw

    min_overhead = len(summary_prefix) + len(frame) + 2
    if min_overhead >= max_chars or not block.rows:
        return [
            raw_with_summary[i:i + max_chars]
            for i in range(0, len(raw_with_summary), max_chars)
        ]

    fragments: list[str] = []
    current_rows: list[str] = []

    def _render(rows: list[str]) -> str:
        lines = [block.open_marker, block.header, block.separator, *rows, block.close_marker]
        return summary_prefix + "\n".join(lines)

    for row in block.rows:
        candidate_rows = [*current_rows, row]
        if current_rows and len(_render(candidate_rows)) > max_chars:
            fragments.append(_render(current_rows))
            current_rows = [row]
        else:
            current_rows = candidate_rows

    if current_rows:
        fragments.append(_render(current_rows))

    return fragments
