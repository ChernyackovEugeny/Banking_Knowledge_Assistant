"""Auto-select retrieval k from offline evaluation metrics.

Runs retrieval eval on a k-grid and picks best k by objective:
    score = F1(recall_at_k_doc, precision_at_k_doc)

Optionally applies hard constraints (e.g., min recall/precision).

Usage:
  python src/evaluation/retrieval/tune_k.py
  python src/evaluation/retrieval/tune_k.py --k-grid 3,5,8,10,12
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.evaluation.retrieval.evaluate import run_eval  # noqa: E402

OUT_PATH = ROOT_DIR / "data" / "eval" / "retrieval_k_recommendation.json"


def _parse_k_grid(raw: str) -> list[int]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    ks = sorted({int(p) for p in parts})
    if not ks or any(k < 1 for k in ks):
        raise ValueError("k-grid must contain positive integers")
    return ks


def _obj(metrics: dict[str, Any]) -> float:
    recall = float(metrics.get("recall_at_k_doc", 0.0) or 0.0)
    precision = float(metrics.get("precision_at_k_doc", 0.0) or 0.0)
    denom = precision + recall
    if denom <= 0:
        return 0.0
    return 2.0 * precision * recall / denom


async def run_tuning(
    *,
    k_grid: list[int],
    min_recall: float | None,
    min_precision: float | None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for k in k_grid:
        report = await run_eval(top_k=k)
        metrics = report.get("metrics", {})
        score = _obj(metrics)
        row = {
            "k": k,
            "metrics": metrics,
            "score": round(score, 6),
            "queries_total": int(report.get("queries_total", 0) or 0),
        }
        rows.append(row)

    def _ok(r: dict[str, Any]) -> bool:
        m = r["metrics"]
        recall = float(m.get("recall_at_k_doc", 0.0) or 0.0)
        precision = float(m.get("precision_at_k_doc", 0.0) or 0.0)
        if min_recall is not None and recall < min_recall:
            return False
        if min_precision is not None and precision < min_precision:
            return False
        return True

    feasible = [r for r in rows if _ok(r)]
    pool = feasible if feasible else rows

    # Primary: max objective score, Secondary: smaller k
    best = sorted(pool, key=lambda r: (-float(r["score"]), int(r["k"])))[0]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "objective": {
            "formula": "2 * precision_at_k_doc * recall_at_k_doc / (precision_at_k_doc + recall_at_k_doc)",
            "min_recall": min_recall,
            "min_precision": min_precision,
        },
        "k_grid": k_grid,
        "evaluations": rows,
        "feasible_count": len(feasible),
        "recommended": best,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-select retrieval k from offline eval.")
    parser.add_argument("--k-grid", default="3,5,8,10,12,15,20")
    parser.add_argument("--min-recall", type=float, default=None, dest="min_recall")
    parser.add_argument("--min-precision", type=float, default=None, dest="min_precision")
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

    if args.min_recall is not None and not (0 <= args.min_recall <= 1):
        raise ValueError("--min-recall must be in [0,1]")
    if args.min_precision is not None and not (0 <= args.min_precision <= 1):
        raise ValueError("--min-precision must be in [0,1]")

    k_grid = _parse_k_grid(args.k_grid)
    report = asyncio.run(
        run_tuning(
            k_grid=k_grid,
            min_recall=args.min_recall,
            min_precision=args.min_precision,
        )
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    best = report["recommended"]
    metrics = best["metrics"]
    logging.info("Recommended k: %d", best["k"])
    logging.info("Recall@k: %.4f", float(metrics.get("recall_at_k_doc", 0.0)))
    logging.info("Precision@k: %.4f", float(metrics.get("precision_at_k_doc", 0.0)))
    logging.info("MRR@k: %.4f", float(metrics.get("mrr_at_k_doc", 0.0)))
    logging.info("nDCG@k(doc): %.4f", float(metrics.get("ndcg_at_k_doc", 0.0)))
    logging.info("Objective score: %.6f", float(best["score"]))
    logging.info("Report: %s", args.out)


if __name__ == "__main__":
    main()
