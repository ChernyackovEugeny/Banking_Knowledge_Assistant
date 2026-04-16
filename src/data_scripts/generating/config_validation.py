from __future__ import annotations

import json
from pathlib import Path


def validate_generation_order(config: dict) -> list[str]:
    """Validate documents[].dependencies against pipeline.generation_order."""
    documents = config.get("documents", [])
    docs_by_id = {doc["id"]: doc for doc in documents}
    generation_order = config.get("pipeline", {}).get("generation_order", [])

    errors: list[str] = []
    order_index = {doc_id: idx for idx, doc_id in enumerate(generation_order)}

    if len(order_index) != len(generation_order):
        seen: set[str] = set()
        duplicates: list[str] = []
        for doc_id in generation_order:
            if doc_id in seen and doc_id not in duplicates:
                duplicates.append(doc_id)
            seen.add(doc_id)
        errors.append("generation_order contains duplicates: " + ", ".join(duplicates))

    order_set = set(generation_order)
    doc_ids = set(docs_by_id)

    missing_in_order = sorted(doc_ids - order_set)
    if missing_in_order:
        errors.append("documents missing from generation_order: " + ", ".join(missing_in_order))

    unknown_in_order = sorted(order_set - doc_ids)
    if unknown_in_order:
        errors.append("generation_order contains unknown doc_id: " + ", ".join(unknown_in_order))

    for doc_id, doc_spec in docs_by_id.items():
        if doc_id not in order_index:
            continue
        doc_pos = order_index[doc_id]
        for dep_id in doc_spec.get("dependencies", []):
            if dep_id not in docs_by_id:
                errors.append(f"{doc_id}: dependency {dep_id} is missing from documents")
                continue
            if dep_id not in order_index:
                errors.append(f"{doc_id}: dependency {dep_id} is missing from generation_order")
                continue
            if order_index[dep_id] >= doc_pos:
                errors.append(f"{doc_id}: dependency {dep_id} must come before the dependent document in generation_order")

    return errors


def validate_references(config: dict, parsed_dir: Path) -> list[str]:
    """Validate references against real_documents and parsed flat section indexes."""
    real_doc_ids = {doc["id"] for doc in config.get("real_documents", [])}
    sections_cache: dict[str, dict] = {}
    errors: list[str] = []

    def load_sections(doc_id: str) -> dict:
        if doc_id not in sections_cache:
            path = parsed_dir / f"{doc_id}_sections.json"
            if not path.exists():
                sections_cache[doc_id] = {}
            else:
                sections_cache[doc_id] = json.loads(path.read_text(encoding="utf-8"))
        return sections_cache[doc_id]

    for doc_spec in config.get("documents", []):
        doc_id = doc_spec["id"]
        refs = doc_spec.get("references", [])
        if not refs:
            continue

        if isinstance(refs[0], str):
            errors.append(f"{doc_id}: legacy references format list[str] is not supported")
            continue

        for ref in refs:
            ref_doc_id = ref.get("doc")
            if not ref_doc_id:
                errors.append(f"{doc_id}: reference entry without doc id")
                continue
            if ref_doc_id not in real_doc_ids:
                errors.append(f"{doc_id}: reference doc {ref_doc_id} is missing from real_documents")
                continue

            sections = load_sections(ref_doc_id)
            if not sections:
                errors.append(f"{doc_id}: parsed sections for reference doc {ref_doc_id} are missing")
                continue

            for sec in ref.get("sections", []):
                sec_id = sec.get("id")
                if not sec_id:
                    errors.append(f"{doc_id}: reference doc {ref_doc_id} contains a section without id")
                    continue
                if sec_id not in sections:
                    errors.append(f"{doc_id}: reference section {ref_doc_id} {sec_id} is missing from parsed data")

    return errors


def validate_pipeline_config(config: dict, parsed_dir: Path) -> list[str]:
    errors = []
    errors.extend(validate_generation_order(config))
    errors.extend(validate_references(config, parsed_dir))
    return errors
