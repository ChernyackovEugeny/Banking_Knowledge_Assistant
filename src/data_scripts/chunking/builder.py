"""Построение чанков из иерархического дерева секций.

Принимает sections_tree (dict из sections_tree.json или md_parser.parse_markdown())
и doc_meta (из config.yaml) → возвращает список чанков.

Три типа чанков:

  leaf         — секция мала (≤ CHILD_MAX_CHARS), индексируется целиком.
                 parent_chunk_id → структурный split_parent главы (если есть).

  split_parent — полный текст секции, не индексируется, хранится для LLM-контекста
                 при retrieval. Создаётся для:
                   а) нелистовых секций (глав), у которых есть leaf-дети;
                   б) листовых секций, которые слишком велики для индексации.

  split_child  — фрагмент большой листовой секции, индексируется.
                 parent_chunk_id → split_parent той же секции.

Retrieval-логика (в будущем retrieval-модуле):
  found chunk → if chunk_type in ("leaf", "split_child") and parent_chunk_id:
                    context_text = chunks[parent_chunk_id].text
                else:
                    context_text = chunk.text
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from splitter import prepare_text_for_chunking, split_text, split_text_table_aware

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

CHILD_MAX_CHARS  = 800    # ~200 токенов для sbert_large_nlu_ru (max_seq_length=512)
OVERLAP_CHARS    = 120    # ~15% от CHILD_MAX — достаточно для boundary coverage
PARENT_MAX_CHARS = 3500   # ~900 токенов — комфортный контекст для LLM


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def build_chunks(tree: dict, doc_meta: dict) -> list[dict]:
    """Строит список чанков из дерева секций.

    Args:
        tree:     dict из sections_tree.json или md_parser.parse_markdown().
                  Обязательные поля root-уровня: doc_id, sections.
        doc_meta: словарь метаданных документа из config.yaml.
                  Ожидаемые поля: source_type, doc_type, doc_subtype,
                  cluster, approval_date (для синтетических).

    Returns:
        Список плоских dict-чанков с полной метаинформацией.
    """
    doc_id: str = tree["doc_id"]
    now: str = datetime.now(timezone.utc).isoformat()
    as_of_date: str = _get_as_of_date(doc_meta)

    chunks: list[dict] = []
    for section in tree["sections"]:
        _process_section(
            section=section,
            doc_id=doc_id,
            doc_meta=doc_meta,
            now=now,
            as_of_date=as_of_date,
            chunks=chunks,
            structural_parent_chunk_id=None,
        )
    return chunks


# ---------------------------------------------------------------------------
# Рекурсивная обработка секций
# ---------------------------------------------------------------------------

def _process_section(
    section: dict,
    doc_id: str,
    doc_meta: dict,
    now: str,
    as_of_date: str,
    chunks: list[dict],
    structural_parent_chunk_id: str | None,
) -> None:
    """DFS-обход дерева секций с построением чанков.

    structural_parent_chunk_id — chunk_id ближайшего split_parent-предка,
    который будет использоваться как LLM-контекст для leaf-потомков.
    """
    section_path: list[str] = section["path"]
    section_order: int = section["order"]
    table_summaries = _section_table_summaries(section)
    section_text = prepare_text_for_chunking(section["text"], table_summaries)
    char_count: int = len(section_text)
    is_leaf: bool = section["is_leaf"]

    if not is_leaf:
        # Нелистовая секция (например, глава ЦБ-документа с пунктами).
        # Создаём structural split_parent — полный текст главы как контекст для детей.
        # Только если текст влезает в PARENT_MAX (иначе он всё равно не пригодится LLM).
        if char_count <= PARENT_MAX_CHARS:
            sp_id = _chunk_id(doc_id, section_path, section_order, "parent")
            chunks.append(_make_chunk(
                chunk_id=sp_id,
                doc_id=doc_id,
                section=section,
                chunk_type="split_parent",
                is_indexed=False,
                parent_chunk_id=structural_parent_chunk_id,
                chunk_index=0,
                chunk_total=1,
                text=section_text,
                doc_meta=doc_meta,
                now=now,
                as_of_date=as_of_date,
            ))
            children_context_id = sp_id
        else:
            # Глава слишком большая для LLM — дети наследуют внешний контекст
            children_context_id = structural_parent_chunk_id

        for child in section["children"]:
            _process_section(
                section=child,
                doc_id=doc_id,
                doc_meta=doc_meta,
                now=now,
                as_of_date=as_of_date,
                chunks=chunks,
                structural_parent_chunk_id=children_context_id,
            )
        return

    # ----- Листовая секция -----

    if char_count <= CHILD_MAX_CHARS:
        # Маленькая: один leaf чанк.
        # parent_chunk_id → структурный родитель (глава), если есть.
        # При retrieval: если parent_chunk_id задан → LLM получает текст главы,
        # иначе — текст самого чанка.
        chunks.append(_make_chunk(
            chunk_id=_chunk_id(doc_id, section_path, section_order, 0),
            doc_id=doc_id,
            section=section,
            chunk_type="leaf",
            is_indexed=True,
            parent_chunk_id=structural_parent_chunk_id,
            chunk_index=0,
            chunk_total=1,
            text=section_text,
            doc_meta=doc_meta,
            now=now,
            as_of_date=as_of_date,
        ))

    else:
        # Большая листовая секция: split_parent + split_children.
        sp_id = _chunk_id(doc_id, section_path, section_order, "parent")

        # split_parent: полный текст, не индексируется, используется LLM как контекст
        chunks.append(_make_chunk(
            chunk_id=sp_id,
            doc_id=doc_id,
            section=section,
            chunk_type="split_parent",
            is_indexed=False,
            parent_chunk_id=structural_parent_chunk_id,
            chunk_index=0,
            chunk_total=1,
            text=section_text,
            doc_meta=doc_meta,
            now=now,
            as_of_date=as_of_date,
        ))

        # split_children: фрагменты с overlap, все индексируются
        fragments = split_text_table_aware(
            section["text"],
            CHILD_MAX_CHARS,
            OVERLAP_CHARS,
            table_summaries=table_summaries,
        )
        for i, fragment in enumerate(fragments):
            chunks.append(_make_chunk(
                chunk_id=_chunk_id(doc_id, section_path, section_order, i),
                doc_id=doc_id,
                section=section,
                chunk_type="split_child",
                is_indexed=True,
                parent_chunk_id=sp_id,
                chunk_index=i,
                chunk_total=len(fragments),
                text=fragment,
                doc_meta=doc_meta,
                now=now,
                as_of_date=as_of_date,
            ))


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _chunk_id(doc_id: str, path: list[str], order: int, suffix: object) -> str:
    """Детерминированный читаемый chunk_id.

    Пример: "590-P__гл_1__parent", "115-FZ__ст_7__0", "590-P__п_1_7__2"
    Детерминированность важна для upsert в ChromaDB при перезапуске с --force.
    """
    safe_path = "__".join(re.sub(r"[^\w-]", "_", part) for part in path)
    return f"{doc_id}__{safe_path}__ord_{order}__{suffix}"


def _get_as_of_date(doc_meta: dict) -> str:
    """Дата актуальности чанка.

    Для синтетических документов: approval_date из config (формат DD.MM.YYYY).
    Для реальных: сегодняшняя дата.
    """
    approval_date = doc_meta.get("approval_date")
    if approval_date:
        try:
            d, m, y = str(approval_date).split(".")
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except (ValueError, AttributeError):
            pass
    return datetime.now(timezone.utc).date().isoformat()


def _make_chunk(
    *,
    chunk_id: str,
    doc_id: str,
    section: dict,
    chunk_type: str,
    is_indexed: bool,
    parent_chunk_id: str | None,
    chunk_index: int,
    chunk_total: int,
    text: str,
    doc_meta: dict,
    now: str,
    as_of_date: str,
) -> dict:
    """Собирает плоский dict чанка из всех источников метаданных."""
    source_type = doc_meta.get("source_type", "real")
    return {
        "chunk_id":                   chunk_id,
        "doc_id":                     doc_id,
        "section_id":                 section["section_id"],
        "parent_chunk_id":            parent_chunk_id,
        "chunk_type":                 chunk_type,
        "is_indexed":                 is_indexed,
        "parent_section_id":          section["parent_section_id"],
        "path":                       section["path"],
        "level":                      section["level"],
        "order_in_doc":               section["order"],
        "chunk_index":                chunk_index,
        "chunk_total":                chunk_total,
        "source_type":                source_type,
        "doc_type":                   doc_meta.get("doc_type", ""),
        "doc_subtype":                doc_meta.get("doc_subtype", ""),
        "cluster":                    doc_meta.get("cluster", ""),
        "is_internal":                source_type == "synthetic",
        "chunk_created_at":           now,
        "chunk_verified_at":          None,
        "as_of_date":                 as_of_date,
        "text":                       text,
        "char_count":                 len(text),
        "contains_table":             bool(_table_ids_for_chunk(text)),
        "table_ids":                  _table_ids_for_chunk(text),
        "table_summary":              _table_summary_for_chunk(text, section),
    }


def _section_table_summaries(section: dict) -> dict[str, str]:
    """Return table_id -> summary mapping from optional section metadata."""
    metadata = section.get("metadata") or {}
    tables = metadata.get("tables") or []
    result: dict[str, str] = {}
    for item in tables:
        if not isinstance(item, dict):
            continue
        table_id = str(item.get("table_id") or "").strip()
        summary = str(item.get("summary") or "").strip()
        if table_id and summary:
            result[table_id] = summary
    return result


def _table_ids_for_chunk(text: str) -> list[str]:
    """Collect table ids that actually appear in the chunk text."""
    return re.findall(r"⟦TABLE\s+id=([^\s⟧]+)⟧", text)


def _table_summary_for_chunk(text: str, section: dict) -> str:
    """Collect only summaries for tables that survived into this chunk."""
    table_ids = _table_ids_for_chunk(text)
    if not table_ids:
        return ""
    summaries = _section_table_summaries(section)
    parts = [summaries[table_id] for table_id in table_ids if table_id in summaries]
    return "\n".join(parts)
