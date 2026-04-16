"""Разбивка текста на фрагменты фиксированного размера.

Алгоритм — два прохода:
  1. _clean_split  — рекурсивная разбивка по иерархии разделителей без overlap.
  2. _add_overlap  — добавление перекрытия между соседними сегментами.

Разделители (от крупных к мелким):
  \n\n  абзацы
  \n    переносы строк
  ". "  конец предложения
  "; "  точка с запятой
  ", "  запятая
  " "   слово (последний осмысленный уровень)

Если ни один разделитель не даёт нужного размера — fallback на нарезку по символам.
"""
from __future__ import annotations

SEPARATORS: list[str] = ["\n\n", "\n", ". ", "; ", ", ", " "]


def split_text(text: str, max_chars: int, overlap: int) -> list[str]:
    """Разбивает text на фрагменты ≤ max_chars с overlap между соседними.

    Args:
        text:      исходный текст
        max_chars: максимальный размер одного фрагмента (символов)
        overlap:   кол-во символов перекрытия из хвоста предыдущего фрагмента

    Returns:
        Список строк. Каждая ≤ max_chars (кроме единственного фрагмента,
        если исходный текст короче max_chars).
    """
    if len(text) <= max_chars:
        return [text]
    segments = _clean_split(text, max_chars, sep_index=0)
    return _add_overlap(segments, overlap)


# ---------------------------------------------------------------------------
# Внутренние функции
# ---------------------------------------------------------------------------

def _clean_split(text: str, max_chars: int, sep_index: int) -> list[str]:
    """Рекурсивно разбивает text, используя SEPARATORS[sep_index] как разделитель.

    Жадно сливает части в сегменты ≤ max_chars. Сегменты, которые всё равно
    не влезают, рекурсивно разбиваются следующим разделителем.
    """
    if len(text) <= max_chars:
        return [text]

    if sep_index >= len(SEPARATORS):
        # Fallback: нарезаем по символам
        return [text[i: i + max_chars] for i in range(0, len(text), max_chars)]

    sep = SEPARATORS[sep_index]
    parts = text.split(sep)

    if len(parts) == 1:
        # Разделитель не встречается — пробуем следующий
        return _clean_split(text, max_chars, sep_index + 1)

    # Жадное слияние частей в сегменты
    segments: list[str] = []
    current = ""
    for part in parts:
        if not part:
            continue
        candidate = current + sep + part if current else part
        if len(candidate) > max_chars and current:
            segments.append(current)
            current = part
        else:
            current = candidate
    if current:
        segments.append(current)

    # Рекурсивно разбиваем сегменты, которые всё ещё не влезают
    result: list[str] = []
    for seg in segments:
        if len(seg) > max_chars:
            result.extend(_clean_split(seg, max_chars, sep_index + 1))
        else:
            result.append(seg)
    return result


def _add_overlap(segments: list[str], overlap: int) -> list[str]:
    """Добавляет overlap символов из хвоста предыдущего сегмента к началу текущего.

    Хвост обрезается до ближайшего пробела, чтобы не разрывать слово.
    """
    if len(segments) <= 1 or overlap <= 0:
        return segments

    result = [segments[0]]
    for i in range(1, len(segments)):
        tail = segments[i - 1][-overlap:]
        # Не разрезаем слово: берём только с ближайшего пробела
        space_idx = tail.find(" ")
        if space_idx != -1:
            tail = tail[space_idx + 1:]
        if tail:
            result.append(tail + " " + segments[i])
        else:
            result.append(segments[i])
    return result
