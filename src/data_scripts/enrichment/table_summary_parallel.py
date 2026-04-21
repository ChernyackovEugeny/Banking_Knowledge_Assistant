"""Table summary enrichment for parsed real documents.

Reads:
  data/parsed/{doc_id}_sections.json
  data/parsed/{doc_id}_sections_tree.json

Finds sections that contain explicit table markers, calls DeepSeek for concise
table summaries, and writes the summaries back into section metadata.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from openai import APIConnectionError, APIStatusError, APITimeoutError, OpenAI, RateLimitError

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

ROOT_DIR = _THIS_DIR.parents[2]
PARSING_DIR = ROOT_DIR / "src" / "data_scripts" / "parsing"
GENERATING_DIR = ROOT_DIR / "src" / "data_scripts" / "generating"

ordered_paths = [str(GENERATING_DIR), str(PARSING_DIR)]
sys.path[:0] = [path for path in ordered_paths if path not in sys.path]

load_dotenv(ROOT_DIR / ".env")

from tables import iter_table_blocks  # noqa: E402
from db_logging.run_logger import RunLogger  # noqa: E402
from db_logging.log_utils import log_llm_error, log_llm_ok  # noqa: E402

logger = logging.getLogger(__name__)

PARSED_DIR = ROOT_DIR / "data" / "parsed"
MODEL = "deepseek-chat"
MAX_CONTEXT_CHARS = 4000
MAX_TABLE_PROMPT_CHARS = 12000
MAX_TABLE_PROMPT_ROWS = 24

SYSTEM_PROMPT = """Ты анализируешь таблицы из нормативных документов банка России.
Сделай короткую полезную сводку таблицы для retrieval.
Требования:
- 2-4 предложения
- укажи, что это за таблица и как её использовать
- если в таблице есть важные пороги, сроки, коды, классификации или перечни, упомяни их
- не пересказывай каждую строку подряд
- не добавляй ничего, чего нет в таблице или ближайшем контексте
Верни только саму сводку, без markdown и без вступлений."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate DeepSeek summaries for table blocks in parsed sections (parallel API requests)",
    )
    parser.add_argument("--only", nargs="+", metavar="DOC_ID",
                        help="Enrich only selected doc_id values")
    parser.add_argument("--force", action="store_true",
                        help="Regenerate summaries even if metadata already exists")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        dest="log_level")
    parser.add_argument("--workers", type=int, default=8,
                        help="Max parallel DeepSeek requests per section (recommended: 4-12)")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def _load_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _section_map_from_tree(tree: dict) -> dict[str, dict]:
    result: dict[str, dict] = {}

    def _walk(node: dict) -> None:
        result[node["section_id"]] = node
        for child in node.get("children", []):
            _walk(child)

    for section in tree.get("sections", []):
        _walk(section)
    return result


def _context_around_table(section_text: str, start: int, end: int) -> str:
    left = max(0, start - (MAX_CONTEXT_CHARS // 2))
    right = min(len(section_text), end + (MAX_CONTEXT_CHARS // 2))
    return section_text[left:right].strip()


def _prepare_table_for_prompt(block) -> str:
    """Сжимает крупные таблицы до безопасного размера для LLM-подсказки."""
    if len(block.raw) <= MAX_TABLE_PROMPT_CHARS and len(block.rows) <= MAX_TABLE_PROMPT_ROWS:
        return block.raw

    head_rows = max(1, MAX_TABLE_PROMPT_ROWS // 2)
    tail_rows = max(1, MAX_TABLE_PROMPT_ROWS - head_rows)
    selected_rows = block.rows[:head_rows]
    omitted = max(0, len(block.rows) - (head_rows + tail_rows))
    if omitted > 0:
        selected_rows.append(
            f"| ... | Пропущено {omitted} строк для сокращения prompt |"
        )
        selected_rows.extend(block.rows[-tail_rows:])
    else:
        selected_rows = block.rows[:MAX_TABLE_PROMPT_ROWS]

    lines = [
        block.open_marker,
        block.header,
        block.separator,
        *selected_rows,
        block.close_marker,
    ]
    compact = "\n".join(lines)
    if len(compact) <= MAX_TABLE_PROMPT_CHARS:
        return compact

    budget = MAX_TABLE_PROMPT_CHARS - len(
        "\n".join([block.open_marker, block.header, block.separator, block.close_marker])
    ) - 64
    clipped_rows: list[str] = []
    used = 0
    for row in selected_rows:
        row_len = len(row) + 1
        if clipped_rows and used + row_len > budget:
            break
        clipped_rows.append(row)
        used += row_len

    if omitted == 0 and len(clipped_rows) < len(selected_rows):
        clipped_rows.append("| ... | Таблица дополнительно обрезана по длине |")

    return "\n".join(
        [
            block.open_marker,
            block.header,
            block.separator,
            *clipped_rows,
            block.close_marker,
        ]
    )


def _already_enriched(section_entry: dict, *, force: bool) -> bool:
    if force:
        return False
    metadata = section_entry.get("metadata") or {}
    tables = metadata.get("tables") or []
    return bool(tables) and all((item.get("summary") or "").strip() for item in tables if isinstance(item, dict))


def _summarize_table(
    client: OpenAI,
    *,
    doc_id: str,
    section_id: str,
    section_title: str,
    section_text: str,
    block,
    run_log,
    op_id: str | None,
) -> str:
    context = _context_around_table(section_text, block.start, block.end)
    table_for_prompt = _prepare_table_for_prompt(block)
    prompt = f"""Документ: {doc_id}
Секция: {section_id}
Заголовок секции: {section_title or "(без заголовка)"}
Таблица: {block.table_id}

Ближайший контекст:
{context}

Сама таблица:
{table_for_prompt}
"""
    max_attempts = 5
    for attempt in range(max_attempts):
        t0 = time.monotonic()
        try:
            response = client.chat.completions.create(
                model=MODEL,
                temperature=0,
                max_tokens=300,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            )
            duration_ms = (time.monotonic() - t0) * 1000
            log_llm_ok(run_log, op_id, doc_id, response, duration_ms, attempt_num=attempt + 1)
            summary = response.choices[0].message.content.strip()
            if not summary:
                raise ValueError(f"{doc_id}/{section_id}/{block.table_id}: empty summary")
            return summary
        except Exception as exc:
            duration_ms = (time.monotonic() - t0) * 1000
            log_llm_error(run_log, op_id, doc_id, exc, duration_ms, attempt_num=attempt + 1)
            is_retryable = isinstance(exc, (RateLimitError, APIConnectionError, APITimeoutError))
            if isinstance(exc, APIStatusError):
                is_retryable = exc.status_code in {429, 500, 502, 503, 504}
            if attempt >= (max_attempts - 1) or not is_retryable:
                raise
            base = min(20.0, 1.5 * (2 ** attempt))
            delay = base + random.uniform(0.0, 0.8)
            logger.warning(
                "%s/%s/%s: retryable LLM error (%s), sleeping %.1fs before retry %d/%d",
                doc_id, section_id, block.table_id, type(exc).__name__, delay, attempt + 2, max_attempts,
            )
            time.sleep(delay)


def _build_table_metadata(
    client: OpenAI,
    *,
    doc_id: str,
    section_id: str,
    section_title: str,
    section_text: str,
    workers: int,
    run_log,
    op_id: str | None,
) -> dict:
    blocks = list(iter_table_blocks(section_text))
    tables: list[dict] = []
    combined: list[str] = []

    if not blocks:
        return {
            "contains_table": True,
            "tables": tables,
            "table_summary": "",
            "table_enriched_at": datetime.now(timezone.utc).isoformat(),
            "table_enrichment_model": MODEL,
        }

    summaries: list[str | None] = [None] * len(blocks)
    max_workers = max(1, min(workers, len(blocks)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(
                _summarize_table,
                client,
                doc_id=doc_id,
                section_id=section_id,
                section_title=section_title,
                section_text=section_text,
                block=block,
                run_log=run_log,
                op_id=op_id,
            ): idx
            for idx, block in enumerate(blocks)
        }
        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            summaries[idx] = future.result()

    for idx, block in enumerate(blocks):
        summary = summaries[idx] or ""
        tables.append({
            "table_id": block.table_id,
            "summary": summary,
            "row_count": len(block.rows),
            "column_count": block.header.count("|") - 1,
        })
        combined.append(f"{block.table_id}: {summary}")

    return {
        "contains_table": True,
        "tables": tables,
        "table_summary": "\n".join(combined),
        "table_enriched_at": datetime.now(timezone.utc).isoformat(),
        "table_enrichment_model": MODEL,
    }


def enrich_document(
    doc_id: str,
    client: OpenAI,
    *,
    force: bool,
    workers: int,
    run_log: RunLogger,
) -> str:
    flat_path = PARSED_DIR / f"{doc_id}_sections.json"
    tree_path = PARSED_DIR / f"{doc_id}_sections_tree.json"

    if not flat_path.exists() or not tree_path.exists():
        logger.warning("%s: parsed artifacts not found", doc_id)
        return "failed"

    flat = _load_json(flat_path)
    tree = _load_json(tree_path)
    tree_sections = _section_map_from_tree(tree)

    if not tree_sections:
        logger.warning("%s: empty sections tree", doc_id)
        return "failed"

    op_id = run_log.start_operation(doc_id, {"title": doc_id})
    changed = 0

    try:
        for section_id, tree_node in tree_sections.items():
            if section_id not in flat:
                continue
            if not iter_table_blocks(tree_node.get("text", "")):
                continue
            if _already_enriched(tree_node, force=force):
                continue

            metadata = _build_table_metadata(
                client,
                doc_id=doc_id,
                section_id=section_id,
                section_title=tree_node.get("title", ""),
                section_text=tree_node.get("text", ""),
                workers=workers,
                run_log=run_log,
                op_id=op_id,
            )
            tree_node["metadata"] = metadata
            flat_entry = dict(flat[section_id])
            flat_entry["metadata"] = metadata
            flat[section_id] = flat_entry
            changed += 1

        if changed:
            _save_json(flat_path, flat)
            _save_json(tree_path, tree)
            run_log.end_operation(
                op_id,
                status="ok",
                output_path=f"data/parsed/{doc_id}_sections.json",
            )
            logger.info("%s: enriched %d sections with table summaries", doc_id, changed)
            return "ok"

        run_log.end_operation(op_id, status="skipped")
        logger.info("%s: no new table summaries needed", doc_id)
        return "skipped"

    except Exception as exc:
        run_log.end_operation(op_id, status="failed", error_msg=str(exc))
        logger.error("%s: enrichment failed: %s", doc_id, exc)
        return "failed"


def _discover_doc_ids() -> list[str]:
    result = []
    for path in sorted(PARSED_DIR.glob("*_sections_tree.json")):
        name = path.name
        if name.endswith("_sections_tree.json"):
            result.append(name[:-len("_sections_tree.json")])
    return result


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is not set in environment")

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    doc_ids = _discover_doc_ids()
    if args.only:
        only = set(args.only)
        doc_ids = [doc_id for doc_id in doc_ids if doc_id in only]

    if not doc_ids:
        raise RuntimeError("No parsed documents found for enrichment")

    docs_ok = docs_failed = docs_skipped = 0
    with RunLogger(args, script_name="table_enrichment") as run_log:
        for doc_id in doc_ids:
            status = enrich_document(
                doc_id,
                client,
                force=args.force,
                workers=args.workers,
                run_log=run_log,
            )
            if status == "ok":
                docs_ok += 1
            elif status == "skipped":
                docs_skipped += 1
            else:
                docs_failed += 1

        run_log.finalize(
            docs_total=len(doc_ids),
            docs_ok=docs_ok,
            docs_failed=docs_failed,
            docs_skipped=docs_skipped,
        )


if __name__ == "__main__":
    main()
