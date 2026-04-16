"""Сериализация секций в sections.json и разрешение alias-идентификаторов.

Контракт с generating.py:
  data/parsed/{doc_id}_sections.json
  {"ст.3": {"title": "...", "text": "..."}, "гл.2": {...}, ...}

Проблема: config.yaml использует «аннотированные» ID, которые парсер не производит
автоматически:
  - «гл.6 (Н6)»  → алиас на «гл.6»
  - «п.3.1-3.5»  → конкатенация п.3.1 ... п.3.5
  - «сч.441-458 (кредиты юрлицам)» → конкатенация сч.441 ... сч.458
  - «формы 0409115» → алиас на «форма 0409115»
  - «0409135»    → алиас на «форма 0409135»

Алгоритм resolve_aliases() обрабатывает все эти случаи автоматически.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone

from db_logging.log_utils import log_alias
from parsers.base import Section

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Уплощение дерева секций
# ---------------------------------------------------------------------------

def flatten_sections(sections: list[Section], *, doc_id: str) -> dict[str, dict]:
    """Рекурсивно строит плоский словарь из дерева секций.

    Дочерние секции (пункты внутри глав) тоже попадают в индекс,
    потому что generating.py ищет по sec_id напрямую.
    """
    result: dict[str, dict] = {}
    for sec in sections:
        if sec.id in result:
            raise ValueError(f"{doc_id}: duplicate section_id in flat index: {sec.id}")
        result[sec.id] = {"title": sec.title, "text": sec.text}
        if sec.children:
            child_flat = flatten_sections(sec.children, doc_id=doc_id)
            duplicate_ids = sorted(set(result).intersection(child_flat))
            if duplicate_ids:
                raise ValueError(
                    f"{doc_id}: duplicate section_id in flat index: {', '.join(duplicate_ids)}"
                )
            result.update(child_flat)
    return result


def _build_sections_tree(
    doc_id: str,
    sections: list[Section],
) -> dict:
    """Строит иерархическое представление секций с метаданными для chunking/retrieval."""
    order_counter = {"value": 0}

    def _walk(sec: Section, parent_id: str | None, parent_path: list[str], level: int) -> dict:
        order_counter["value"] += 1
        current_path = [*parent_path, sec.id]
        children_nodes = [
            _walk(child, sec.id, current_path, level + 1)
            for child in sec.children
        ]
        return {
            "doc_id": doc_id,
            "section_id": sec.id,
            "parent_section_id": parent_id,
            "level": level,
            "order": order_counter["value"],
            "path": current_path,
            "title": sec.title,
            "text": sec.text,
            "char_count": len(sec.text),
            "is_leaf": len(sec.children) == 0,
            "children": children_nodes,
        }

    return {
        "doc_id": doc_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sections": [_walk(sec, None, [], 1) for sec in sections],
    }


# ---------------------------------------------------------------------------
# Алиас-резолвер
# ---------------------------------------------------------------------------

def _strip_annotation(raw_id: str) -> str:
    """Убирает скобочную аннотацию: «гл.6 (Н6)» → «гл.6»."""
    return raw_id.split("(")[0].strip()


def _normalize(raw_id: str) -> str:
    """Убирает пробелы: «ст. 3» → «ст.3», «п. 1.1» → «п.1.1»."""
    return re.sub(r"\s+", "", raw_id)


def _try_lookup(candidate: str, sections: dict) -> dict | None:
    """Ищет секцию сначала как есть, потом в нормализованном виде."""
    if candidate in sections:
        return sections[candidate]
    norm = _normalize(candidate)
    return sections.get(norm)


def _parse_range(raw_id: str) -> tuple[str, int, int] | None:
    """Разбирает диапазонный ID.

    Примеры:
      «п.3.1-3.5»           → prefix=«п.», lo_minor=1, hi_minor=5, major=3
      «сч.441-458»          → prefix=«сч.», lo=441, hi=458
      «сч.441-458 (...)»    → то же после strip_annotation

    Returns:
      (prefix, lo, hi) если это числовой диапазон, иначе None.
    """
    clean = _strip_annotation(raw_id)
    # Паттерн: <prefix><lo>-<hi>  (например, п.3.1-3.5 или сч.441-458)
    m = re.match(r"^((?:п|сч|гл)\.)(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)$", _normalize(clean))
    if not m:
        return None
    prefix = m.group(1)
    lo_str = m.group(2)
    hi_str = m.group(3)
    try:
        lo = float(lo_str)
        hi = float(hi_str)
    except ValueError:
        return None
    return prefix, lo, hi


def _collect_range(prefix: str, lo: float, hi: float, sections: dict) -> str | None:
    """Собирает тексты секций, попадающих в диапазон [lo, hi]."""
    collected: list[str] = []
    for key, val in sections.items():
        norm_key = _normalize(key)
        if not norm_key.startswith(prefix):
            continue
        num_str = norm_key[len(prefix):]
        try:
            num = float(num_str)
        except ValueError:
            continue
        if lo <= num <= hi:
            text = val.get("text", "") if isinstance(val, dict) else str(val)
            collected.append(text)

    return "\n\n".join(collected) if collected else None


def _resolve_reporting_form(raw_id: str, sections: dict) -> dict | None:
    """Обрабатывает алиасы форм отчётности.

    «формы 0409115» → «форма 0409115»
    «0409115»       → «форма 0409115»
    """
    clean = _normalize(_strip_annotation(raw_id))
    if re.fullmatch(r"0409\d+", clean):
        return _try_lookup(f"форма{clean}", sections) or _try_lookup(f"форма {clean}", sections)
    m = re.match(r"форм[ыа](\d+)", clean)
    if m:
        num = m.group(1)
        return _try_lookup(f"форма{num}", sections) or _try_lookup(f"форма {num}", sections)
    return None


def resolve_aliases(
    wanted_ids: list[str],
    sections: dict,
    *,
    doc_id: str | None = None,
    op_id: str | None = None,
    run_log: object = None,
) -> dict:
    """Добавляет в sections записи для всех wanted_ids, которые там отсутствуют.

    Стратегия (в порядке приоритета):
    1. Точное совпадение (уже есть → пропускаем)
    2. Нормализация (убрать пробелы)
    3. Сброс аннотации (убрать скобки)
    4. Диапазон — конкатенация нескольких секций
    5. Алиас формы отчётности

    Принцип: не добавлять «похожие» секции по эвристике префикса.
    Если wanted_id не разрешился строго — он считается отсутствующим.
    """
    result = dict(sections)

    for wanted in wanted_ids:
        if wanted in result:
            log_alias(run_log, op_id, doc_id, wanted, resolved=True, strategy="exact",
                      text_length=len(result[wanted].get("text", "")))
            continue

        resolved: dict | None = None
        strategy_used: str = "not_found"

        resolved = _try_lookup(wanted, result)
        if resolved:
            strategy_used = "normalize"

        if not resolved:
            base = _strip_annotation(wanted)
            resolved = _try_lookup(base, result)
            if resolved:
                strategy_used = "strip_annotation"

        if not resolved:
            rng = _parse_range(wanted)
            if rng:
                prefix, lo, hi = rng
                combined_text = _collect_range(prefix, lo, hi, result)
                if combined_text:
                    resolved = {"title": wanted, "text": combined_text}
                    strategy_used = "range"

        if not resolved:
            resolved = _resolve_reporting_form(wanted, result)
            if resolved:
                strategy_used = "reporting_form"

        if resolved:
            result[wanted] = resolved
            logger.debug("Alias: «%s» → %d символов текста", wanted, len(resolved.get("text", "")))
        else:
            logger.warning("Alias не разрешён: «%s» — секция не найдена в документе", wanted)

        log_alias(run_log, op_id, doc_id, wanted, resolved=resolved is not None,
                  strategy=strategy_used, text_length=len(resolved.get("text", "")) if resolved else None)

    return result


# ---------------------------------------------------------------------------
# Сохранение
# ---------------------------------------------------------------------------

def save_sections(
    doc_id: str,
    sections: list[Section],
    output_dir: Path,
    wanted_ids: list[str] | None = None,
    *,
    op_id: str | None = None,
    run_log: object = None,
) -> Path:
    """Сохраняет секции в legacy flat-формате + flat/tree для structured chunking."""
    flat = flatten_sections(sections, doc_id=doc_id)

    if wanted_ids:
        flat = resolve_aliases(
            wanted_ids, flat,
            doc_id=doc_id,
            op_id=op_id,
            run_log=run_log,
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    # Legacy flat: используется текущим generating.py
    legacy_path = output_dir / f"{doc_id}_sections.json"
    legacy_path.write_text(
        json.dumps(flat, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Иерархический формат для document-structured chunking и parent-child retrieval.
    tree_payload = _build_sections_tree(
        doc_id,
        sections,
    )
    tree_path = output_dir / f"{doc_id}_sections_tree.json"
    tree_path.write_text(
        json.dumps(tree_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Сохранено: %s, %s (%d flat-секций)",
        legacy_path.name, tree_path.name, len(flat),
    )
    return legacy_path
