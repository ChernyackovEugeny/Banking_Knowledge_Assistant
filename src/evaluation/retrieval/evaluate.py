"""Offline retrieval evaluation on QA dataset.

Computes:
  - Recall@k (doc_id relevance)
  - MRR@k (doc_id relevance)
  - nDCG@k (doc_id relevance)
  - nDCG@k (graded relevance from doc_id + source_hint heuristic)

Usage:
  python src/evaluation/retrieval/evaluate.py
  python src/evaluation/retrieval/evaluate.py --top-k 5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.retriever import retrieve, RetrievedChunk  # noqa: E402

QUESTIONS_DIR = ROOT_DIR / "data" / "questions"
OUT_PATH = ROOT_DIR / "data" / "eval" / "retrieval_eval_report.json"

_NUM_REF_RE = re.compile(r"\d+(?:\.\d+)*")


@dataclass
class EvalCase:
    case_id: str
    difficulty: str
    question: str
    expected_doc_id: str
    source_hint: str


def _normalize(text: str) -> str:
    return (text or "").lower().replace("ё", "е").strip()


def _extract_num_refs(text: str) -> set[str]:
    return set(_NUM_REF_RE.findall(_normalize(text)))


def _hint_match(source_hint: str, chunk: RetrievedChunk) -> bool:
    hint = _normalize(source_hint)
    if not hint:
        return False

    section = _normalize(chunk.section_title)
    if section and (section in hint or hint in section):
        return True

    hint_nums = _extract_num_refs(hint)
    if not hint_nums:
        return False

    section_nums = _extract_num_refs(section)
    if hint_nums & section_nums:
        return True

    text_nums = _extract_num_refs(chunk.text[:1200])
    return bool(hint_nums & text_nums)


def _graded_rel(case: EvalCase, chunk: RetrievedChunk) -> int:
    rel = 0
    if chunk.doc_id == case.expected_doc_id:
        rel += 1
    if _hint_match(case.source_hint, chunk):
        rel += 1
    return rel


def _dcg(rels: list[int]) -> float:
    score = 0.0
    for idx, rel in enumerate(rels):
        if rel <= 0:
            continue
        score += (2**rel - 1) / math.log2(idx + 2)
    return score


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _load_cases() -> list[EvalCase]:
    cases: list[EvalCase] = []
    for path in sorted(QUESTIONS_DIR.glob("*_questions.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        expected_doc_id = str(data.get("doc_id") or path.stem.removesuffix("_questions"))
        for q in data.get("questions", []):
            if not isinstance(q, dict):
                continue
            question = str(q.get("question") or "").strip()
            if not question:
                continue
            cases.append(
                EvalCase(
                    case_id=str(q.get("id") or f"{expected_doc_id}_{len(cases)+1}"),
                    difficulty=str(q.get("difficulty") or "unknown"),
                    question=question,
                    expected_doc_id=expected_doc_id,
                    source_hint=str(q.get("source_hint") or ""),
                )
            )
    return cases


async def _evaluate_case(case: EvalCase, top_k: int) -> dict[str, Any]:
    chunks = await retrieve(case.question, top_k=top_k)
    rel_doc = [1 if c.doc_id == case.expected_doc_id else 0 for c in chunks]
    rel_hint = [_graded_rel(case, c) for c in chunks]

    recall_doc = 1.0 if any(rel_doc) else 0.0
    precision_doc = float(sum(rel_doc) / top_k) if top_k > 0 else 0.0
    rr_doc = 0.0
    for rank, rel in enumerate(rel_doc, 1):
        if rel > 0:
            rr_doc = 1.0 / rank
            break

    ndcg_doc = _dcg(rel_doc) / 1.0
    max_rel_hint = 2 if case.source_hint else 1
    idcg_hint = (2**max_rel_hint - 1) / math.log2(2)
    ndcg_hint = _dcg(rel_hint) / idcg_hint if idcg_hint > 0 else 0.0

    return {
        "case_id": case.case_id,
        "difficulty": case.difficulty,
        "expected_doc_id": case.expected_doc_id,
        "recall_doc": recall_doc,
        "precision_doc": precision_doc,
        "rr_doc": rr_doc,
        "ndcg_doc": ndcg_doc,
        "ndcg_hint": ndcg_hint,
        "top_results": [
            {
                "rank": i + 1,
                "doc_id": c.doc_id,
                "section_title": c.section_title,
                "score": round(float(c.score), 6),
                "rel_doc": rel_doc[i],
                "rel_hint": rel_hint[i],
            }
            for i, c in enumerate(chunks)
        ],
    }


async def run_eval(top_k: int) -> dict[str, Any]:
    cases = _load_cases()
    if not cases:
        raise RuntimeError(f"No QA cases found in {QUESTIONS_DIR}")

    rows: list[dict[str, Any]] = []
    for case in cases:
        rows.append(await _evaluate_case(case, top_k=top_k))

    by_diff_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_diff_rows[str(row.get("difficulty") or "unknown")].append(row)

    by_difficulty: dict[str, Any] = {}
    for diff, diff_rows in sorted(by_diff_rows.items()):
        by_difficulty[diff] = {
            "queries": len(diff_rows),
            "recall_at_k_doc": round(_mean([float(r["recall_doc"]) for r in diff_rows]), 4),
            "precision_at_k_doc": round(_mean([float(r["precision_doc"]) for r in diff_rows]), 4),
            "mrr_at_k_doc": round(_mean([float(r["rr_doc"]) for r in diff_rows]), 4),
            "ndcg_at_k_doc": round(_mean([float(r["ndcg_doc"]) for r in diff_rows]), 4),
            "ndcg_at_k_hint": round(_mean([float(r["ndcg_hint"]) for r in diff_rows]), 4),
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_k": top_k,
        "queries_total": len(rows),
        "metrics": {
            "recall_at_k_doc": round(_mean([float(r["recall_doc"]) for r in rows]), 4),
            "precision_at_k_doc": round(_mean([float(r["precision_doc"]) for r in rows]), 4),
            "mrr_at_k_doc": round(_mean([float(r["rr_doc"]) for r in rows]), 4),
            "ndcg_at_k_doc": round(_mean([float(r["ndcg_doc"]) for r in rows]), 4),
            "ndcg_at_k_hint": round(_mean([float(r["ndcg_hint"]) for r in rows]), 4),
        },
        "by_difficulty": by_difficulty,
        "cases": rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline retrieval evaluation on QA dataset.")
    parser.add_argument("--top-k", type=int, default=5, dest="top_k")
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.top_k < 1:
        raise ValueError("--top-k must be >= 1")

    report = asyncio.run(run_eval(top_k=args.top_k))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    metrics = report.get("metrics", {})
    logging.info("Queries: %d", report.get("queries_total", 0))
    logging.info("Recall@%d (doc): %.4f", args.top_k, metrics.get("recall_at_k_doc", 0.0))
    logging.info("Precision@%d (doc): %.4f", args.top_k, metrics.get("precision_at_k_doc", 0.0))
    logging.info("MRR@%d (doc): %.4f", args.top_k, metrics.get("mrr_at_k_doc", 0.0))
    logging.info("nDCG@%d (doc): %.4f", args.top_k, metrics.get("ndcg_at_k_doc", 0.0))
    logging.info("nDCG@%d (hint): %.4f", args.top_k, metrics.get("ndcg_at_k_hint", 0.0))
    logging.info("Report: %s", args.out)


if __name__ == "__main__":
    main()
