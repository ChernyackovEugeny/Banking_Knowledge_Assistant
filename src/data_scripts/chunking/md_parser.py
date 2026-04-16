"""Парсинг Markdown-документов в иерархическое дерево секций.

Синтетические документы (data/generated/*.md) содержат структурированный Markdown
с заголовками #/##/###. Этот модуль превращает их в tree-dict того же формата,
что _build_sections_tree() в parsing/postprocessing/output.py.

Builder.py работает единообразно с обоими форматами — он не знает,
откуда пришло дерево: из sections_tree.json или из parse_markdown().
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

_HEADING_RE = re.compile(r"^(#{1,4})\s+(.+)$", re.MULTILINE)


def parse_markdown(text: str, doc_id: str) -> dict:
    """Разбирает Markdown-текст в sections_tree dict.

    Returns:
        dict с полями doc_id, generated_at, sections —
        аналогично _build_sections_tree() из parsing/postprocessing/output.py.
    """
    matches = list(_HEADING_RE.finditer(text))

    if not matches:
        # Весь документ — одна безымянная листовая секция
        return {
            "doc_id": doc_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sections": [_make_flat_section(doc_id, text)],
        }

    # Собираем плоский список «сырых» узлов
    raw_nodes: list[dict] = []
    for i, m in enumerate(matches):
        level = len(m.group(1))
        title = m.group(2).strip()
        body_start = m.end()
        body_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        own_text = text[body_start:body_end].strip()
        raw_nodes.append({"level": level, "title": title, "own_text": own_text})

    tree_sections = _build_tree(raw_nodes, doc_id)

    return {
        "doc_id": doc_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sections": tree_sections,
    }


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _make_flat_section(doc_id: str, text: str) -> dict:
    return {
        "doc_id": doc_id,
        "section_id": "h1_1",
        "parent_section_id": None,
        "level": 1,
        "order": 1,
        "path": ["h1_1"],
        "title": "",
        "text": text,
        "char_count": len(text),
        "is_leaf": True,
        "children": [],
    }


def _build_tree(raw_nodes: list[dict], doc_id: str) -> list[dict]:
    """Строит иерархическое дерево из плоского списка заголовков.

    Алгоритм:
    1. Присваиваем section_id в виде h{level}_{counter}.
    2. Стек предков: для каждого узла убираем из стека всё ≥ текущего уровня,
       оставшийся верхний элемент — родитель.
    3. Рекурсивно агрегируем text: text нелистового узла =
       own_text + тексты всех потомков (как в Section.text в parsing).
    """
    # Шаг 1: создаём узлы с section_id
    level_counters: dict[int, int] = {}
    order_counter = 0
    nodes: list[dict] = []
    for raw in raw_nodes:
        level = raw["level"]
        level_counters[level] = level_counters.get(level, 0) + 1
        order_counter += 1
        nodes.append({
            "section_id": f"h{level}_{level_counters[level]}",
            "level": level,
            "title": raw["title"],
            "own_text": raw["own_text"],
            "order": order_counter,
            "children": [],
            "parent": None,
        })

    # Шаг 2: строим связи родитель-потомок через стек
    stack: list[dict] = []
    roots: list[dict] = []
    for node in nodes:
        while stack and stack[-1]["level"] >= node["level"]:
            stack.pop()
        if stack:
            node["parent"] = stack[-1]
            stack[-1]["children"].append(node)
        else:
            roots.append(node)
        stack.append(node)

    # Шаг 3: агрегируем text и path, формируем финальный dict
    def _to_dict(node: dict, parent_path: list[str], parent_id: str | None) -> dict:
        path = [*parent_path, node["section_id"]]
        child_dicts = [
            _to_dict(child, path, node["section_id"])
            for child in node["children"]
        ]
        is_leaf = len(child_dicts) == 0
        if is_leaf:
            full_text = node["own_text"]
        else:
            parts = [node["own_text"]] + [c["text"] for c in child_dicts]
            full_text = "\n\n".join(p for p in parts if p)
        return {
            "doc_id": doc_id,
            "section_id": node["section_id"],
            "parent_section_id": parent_id,
            "level": node["level"],
            "order": node["order"],
            "path": path,
            "title": node["title"],
            "text": full_text,
            "char_count": len(full_text),
            "is_leaf": is_leaf,
            "children": child_dicts,
        }

    return [_to_dict(root, [], None) for root in roots]
