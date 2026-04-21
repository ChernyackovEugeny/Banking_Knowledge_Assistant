"""Dashboard API вЂ” Р°РЅР°Р»РёС‚РёС‡РµСЃРєРёРµ СЌРЅРґРїРѕРёРЅС‚С‹ РґР»СЏ РїСЂРѕСЃРјРѕС‚СЂР° Р»РѕРіРѕРІ PostgreSQL.

Р­РЅРґРїРѕРёРЅС‚С‹ СЃРёРЅС…СЂРѕРЅРЅС‹Рµ (FastAPI Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё Р·Р°РїСѓСЃРєР°РµС‚ РёС… РІ thread pool).
РљР°Р¶РґС‹Р№ Р·Р°РїСЂРѕСЃ РѕС‚РєСЂС‹РІР°РµС‚ РѕС‚РґРµР»СЊРЅРѕРµ РєРѕСЂРѕС‚РєРѕРµ СЃРѕРµРґРёРЅРµРЅРёРµ СЃ PostgreSQL.
РџСЂРё РѕС‚СЃСѓС‚СЃС‚РІРёРё С‚Р°Р±Р»РёС† РёР»Рё РЅРµРґРѕСЃС‚СѓРїРЅРѕСЃС‚Рё Р‘Р” РІРѕР·РІСЂР°С‰Р°РµС‚ РїСѓСЃС‚С‹Рµ РґР°РЅРЅС‹Рµ вЂ” РґСЌС€Р±РѕСЂРґ
РЅРµ Р»РѕРјР°РµС‚ СЂР°Р±РѕС‚Сѓ API РґР°Р¶Рµ Р±РµР· Р·Р°РїСѓС‰РµРЅРЅРѕРіРѕ Docker.

РњР°СЂС€СЂСѓС‚С‹:
    GET /api/dashboard/overview            вЂ” KPI-РјРµС‚СЂРёРєРё Р·Р° 24 С‡Р°СЃР°
    GET /api/dashboard/chat/timeline       вЂ” Р·Р°РїСЂРѕСЃС‹ РїРѕ С‡Р°СЃР°Рј (24h)
    GET /api/dashboard/chat/tokens         вЂ” С‚РѕРєРµРЅС‹ РїРѕ РґРЅСЏРј (14d)
    GET /api/dashboard/chat/recent         вЂ” РїРѕСЃР»РµРґРЅРёРµ 20 Р·Р°РїСЂРѕСЃРѕРІ
    GET /api/dashboard/chat/anomalies      вЂ” Р·Р°РІРёСЃС€РёРµ / РјРµРґР»РµРЅРЅС‹Рµ / РїСЂРѕР±Р»РµРјРЅС‹Рµ СЃРµСЃСЃРёРё
    GET /api/dashboard/chat/docs           вЂ” С‚РѕРї РґРѕРєСѓРјРµРЅС‚РѕРІ РІ РєРѕРЅС‚РµРєСЃС‚Рµ LLM
    GET /api/dashboard/pipeline            вЂ” Р·Р°РїСѓСЃРєРё parse_ Рё gen_ РїР°Р№РїР»Р°Р№РЅРѕРІ
"""
from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import APIRouter

load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env")

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

ROOT_DIR = Path(__file__).resolve().parents[3]
PARSED_DIR = ROOT_DIR / "data" / "parsed"
GENERATED_DIR = ROOT_DIR / "data" / "generated"
QUESTIONS_DIR = ROOT_DIR / "data" / "questions"
CHUNKS_DIR = ROOT_DIR / "data" / "chunks"
_TOKEN_RE = re.compile(r"\S+", flags=re.UNICODE)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_params() -> dict:
    return {
        "host":     os.getenv("POSTGRES_HOST", "localhost"),
        "port":     int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname":   os.getenv("POSTGRES_DB", "banking_assistant"),
        "user":     os.getenv("POSTGRES_USER", "banking_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "changeme"),
    }


@contextmanager
def _cursor():
    """РћС‚РєСЂС‹РІР°РµС‚ СЃРѕРµРґРёРЅРµРЅРёРµ, РѕС‚РґР°С‘С‚ RealDictCursor, Р·Р°РєСЂС‹РІР°РµС‚ РїСЂРё РІС‹С…РѕРґРµ."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    conn = psycopg2.connect(**_db_params())
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


def _safe(v, cast=None, default=None):
    """Р‘РµР·РѕРїР°СЃРЅРѕРµ РїСЂРёРІРµРґРµРЅРёРµ С‚РёРїР° СЃ РґРµС„РѕР»С‚РѕРј РїСЂРё None."""
    if v is None:
        return default
    try:
        return cast(v) if cast else v
    except (TypeError, ValueError):
        return default


def _dt(v) -> str | None:
    """datetime/date в†’ ISO-СЃС‚СЂРѕРєР° РґР»СЏ JSON."""
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _serialize_rows(rows) -> list[dict]:
    return [
        {k: _dt(v) for k, v in dict(r).items()}
        for r in (rows or [])
    ]


def _safe_read_json(path: Path) -> dict | list | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _estimate_tokens(text: str) -> int:
    """Approximate tokens as count of non-whitespace fragments."""
    if not text:
        return 0
    return len(_TOKEN_RE.findall(text))


def _avg(nums: list[int | float]) -> float:
    if not nums:
        return 0.0
    return float(sum(nums) / len(nums))


def _summarize(items: list[dict], *, value_keys: tuple[str, ...] = ("chars", "tokens")) -> dict[str, Any]:
    summary: dict[str, Any] = {"files_count": len(items)}
    for key in value_keys:
        values = [int(i.get(key, 0) or 0) for i in items]
        summary[f"total_{key}"] = int(sum(values))
        summary[f"avg_{key}"] = round(_avg(values), 1) if values else 0.0
        summary[f"max_{key}"] = int(max(values)) if values else 0
    return summary


def _build_artifact_stats() -> dict[str, Any]:
    parsed_items: list[dict[str, Any]] = []
    generated_items: list[dict[str, Any]] = []
    questions_items: list[dict[str, Any]] = []
    chunks_items: list[dict[str, Any]] = []

    for path in sorted(PARSED_DIR.glob("*_sections.json")):
        data = _safe_read_json(path)
        if not isinstance(data, dict):
            continue
        doc_id = path.stem.removesuffix("_sections")
        section_count = len(data)
        chars = sum(len(str(v.get("text", ""))) for v in data.values() if isinstance(v, dict))
        sections_with_tables = 0
        sections_enriched = 0
        total_tables = 0
        enriched_tables = 0
        for value in data.values():
            if not isinstance(value, dict):
                continue
            metadata = value.get("metadata") or {}
            tables = metadata.get("tables") or []
            if tables:
                sections_with_tables += 1
                total_tables += len(tables)
                enriched_in_section = sum(
                    1 for t in tables
                    if isinstance(t, dict) and str(t.get("summary", "")).strip()
                )
                enriched_tables += enriched_in_section
                if enriched_in_section > 0:
                    sections_enriched += 1
        parsed_items.append({
            "doc_id": doc_id,
            "sections_count": section_count,
            "sections_with_tables": sections_with_tables,
            "sections_enriched": sections_enriched,
            "tables_count": total_tables,
            "enriched_tables_count": enriched_tables,
            "chars": chars,
            "tokens": _estimate_tokens(" ".join(str(v.get("text", "")) for v in data.values() if isinstance(v, dict))),
            "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        })

    for path in sorted(GENERATED_DIR.glob("*.md")):
        doc_id = path.stem
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        generated_items.append({
            "doc_id": doc_id,
            "chars": len(text),
            "tokens": _estimate_tokens(text),
            "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        })

    for path in sorted(QUESTIONS_DIR.glob("*_questions.json")):
        data = _safe_read_json(path)
        if not isinstance(data, dict):
            continue
        doc_id = path.stem.removesuffix("_questions")
        questions = data.get("questions", [])
        if not isinstance(questions, list):
            questions = []
        q_texts = [str(q.get("question", "")) for q in questions if isinstance(q, dict)]
        a_texts = [str(q.get("answer", "")) for q in questions if isinstance(q, dict)]
        qa_text = "\n".join([*q_texts, *a_texts])
        question_chars = [len(t) for t in q_texts]
        answer_chars = [len(t) for t in a_texts]
        questions_items.append({
            "doc_id": doc_id,
            "questions_count": len(questions),
            "chars": len(qa_text),
            "tokens": _estimate_tokens(qa_text),
            "avg_question_chars": round(_avg(question_chars), 1) if question_chars else 0.0,
            "max_question_chars": max(question_chars) if question_chars else 0,
            "avg_answer_chars": round(_avg(answer_chars), 1) if answer_chars else 0.0,
            "max_answer_chars": max(answer_chars) if answer_chars else 0,
            "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        })

    for path in sorted(CHUNKS_DIR.glob("*_chunks.json")):
        data = _safe_read_json(path)
        if not isinstance(data, dict):
            continue
        doc_id = data.get("doc_id") or path.stem.removesuffix("_chunks")
        chunks = data.get("chunks", [])
        if not isinstance(chunks, list):
            chunks = []
        chunk_chars = [int(c.get("char_count", 0) or 0) for c in chunks if isinstance(c, dict)]
        total_chars = sum(chunk_chars)
        chunks_items.append({
            "doc_id": str(doc_id),
            "chunk_count": int(data.get("chunk_count", len(chunks)) or len(chunks)),
            "indexed_count": int(data.get("indexed_count", 0) or 0),
            "chars": total_chars,
            "tokens": int(round(total_chars / 4.0)),
            "avg_chunk_chars": round(_avg(chunk_chars), 1) if chunk_chars else 0.0,
            "max_chunk_chars": max(chunk_chars) if chunk_chars else 0,
            "updated_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        })
    parsed_items.sort(key=lambda x: x["chars"], reverse=True)
    generated_items.sort(key=lambda x: x["chars"], reverse=True)
    questions_items.sort(key=lambda x: x["questions_count"], reverse=True)
    chunks_items.sort(key=lambda x: x["chunk_count"], reverse=True)

    questions_summary = _summarize(questions_items)
    questions_counts = [int(i.get("questions_count", 0) or 0) for i in questions_items]
    questions_summary["total_questions"] = int(sum(questions_counts))
    questions_summary["avg_questions_per_doc"] = round(_avg(questions_counts), 1) if questions_counts else 0.0
    questions_summary["max_questions_per_doc"] = int(max(questions_counts)) if questions_counts else 0

    chunks_summary = _summarize(chunks_items)
    chunk_counts = [int(i.get("chunk_count", 0) or 0) for i in chunks_items]
    indexed_counts = [int(i.get("indexed_count", 0) or 0) for i in chunks_items]
    chunks_summary["total_chunks"] = int(sum(chunk_counts))
    chunks_summary["avg_chunks_per_doc"] = round(_avg(chunk_counts), 1) if chunk_counts else 0.0
    chunks_summary["max_chunks_per_doc"] = int(max(chunk_counts)) if chunk_counts else 0
    chunks_summary["total_indexed_chunks"] = int(sum(indexed_counts))
    chunks_summary["avg_tokens_per_chunk"] = (
        round(chunks_summary["total_tokens"] / chunks_summary["total_chunks"], 1)
        if chunks_summary["total_chunks"] > 0 else 0.0
    )

    docs_with_tables = [i for i in parsed_items if int(i.get("tables_count", 0) or 0) > 0]
    docs_enriched = [i for i in parsed_items if int(i.get("enriched_tables_count", 0) or 0) > 0]
    parsed_summary = _summarize(parsed_items)
    parsed_summary["docs_with_tables"] = len(docs_with_tables)
    parsed_summary["docs_enriched"] = len(docs_enriched)
    parsed_summary["sections_with_tables"] = int(sum(int(i.get("sections_with_tables", 0) or 0) for i in parsed_items))
    parsed_summary["sections_enriched"] = int(sum(int(i.get("sections_enriched", 0) or 0) for i in parsed_items))
    parsed_summary["total_tables"] = int(sum(int(i.get("tables_count", 0) or 0) for i in parsed_items))
    parsed_summary["enriched_tables"] = int(sum(int(i.get("enriched_tables_count", 0) or 0) for i in parsed_items))

    return {
        "parsed_docs": {
            "summary": parsed_summary,
            "items": parsed_items,
        },
        "generated_docs": {
            "summary": _summarize(generated_items),
            "items": generated_items,
        },
        "questions": {
            "summary": questions_summary,
            "items": questions_items,
        },
        "chunks": {
            "summary": chunks_summary,
            "items": chunks_items,
        },
    }


# ---------------------------------------------------------------------------
# GET /api/dashboard/overview
# ---------------------------------------------------------------------------

@router.get("/overview")
def get_overview() -> dict[str, Any]:
    """РљР»СЋС‡РµРІС‹Рµ РјРµС‚СЂРёРєРё Р·Р° РїРѕСЃР»РµРґРЅРёРµ 24 С‡Р°СЃР°."""
    defaults: dict[str, Any] = {
        "total_requests": 0, "ok_count": 0, "error_count": 0,
        "in_progress_count": 0, "avg_latency_ms": None, "p95_latency_ms": None,
        "total_tokens_24h": 0, "active_sessions_24h": 0, "stuck_count": 0,
    }
    try:
        with _cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                                       AS total_requests,
                    SUM(CASE WHEN status = 'ok'          THEN 1 ELSE 0 END)       AS ok_count,
                    SUM(CASE WHEN status = 'error'       THEN 1 ELSE 0 END)       AS error_count,
                    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END)       AS in_progress_count,
                    AVG(CASE WHEN status = 'ok' THEN total_duration_ms END)       AS avg_latency_ms,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms)
                        FILTER (WHERE status = 'ok')                              AS p95_latency_ms
                FROM chat_requests
                WHERE requested_at >= NOW() - INTERVAL '24 hours'
            """)
            row = cur.fetchone() or {}

            cur.execute("""
                SELECT COUNT(DISTINCT session_id) AS active_sessions
                FROM chat_requests
                WHERE requested_at >= NOW() - INTERVAL '24 hours'
            """)
            sess = cur.fetchone() or {}

            cur.execute("""
                SELECT COALESCE(SUM(total_tokens), 0) AS total_tokens
                FROM chat_llm_calls
                WHERE called_at >= NOW() - INTERVAL '24 hours' AND status = 'ok'
            """)
            tok = cur.fetchone() or {}

            cur.execute("""
                SELECT COUNT(*) AS stuck_count
                FROM chat_requests
                WHERE status = 'in_progress'
                  AND requested_at < NOW() - INTERVAL '5 minutes'
            """)
            stuck = cur.fetchone() or {}

            return {
                "total_requests":      _safe(row.get("total_requests"), int, 0),
                "ok_count":            _safe(row.get("ok_count"), int, 0),
                "error_count":         _safe(row.get("error_count"), int, 0),
                "in_progress_count":   _safe(row.get("in_progress_count"), int, 0),
                "avg_latency_ms":      _safe(row.get("avg_latency_ms"), float),
                "p95_latency_ms":      _safe(row.get("p95_latency_ms"), float),
                "total_tokens_24h":    _safe(tok.get("total_tokens"), int, 0),
                "active_sessions_24h": _safe(sess.get("active_sessions"), int, 0),
                "stuck_count":         _safe(stuck.get("stuck_count"), int, 0),
            }
    except Exception:
        return defaults


# ---------------------------------------------------------------------------
# GET /api/dashboard/chat/timeline
# ---------------------------------------------------------------------------

@router.get("/chat/timeline")
def get_chat_timeline(hours: int = 24) -> list[dict]:
    """Р—Р°РїСЂРѕСЃС‹ РїРѕ С‡Р°СЃР°Рј Р·Р° РїРѕСЃР»РµРґРЅРёРµ N С‡Р°СЃРѕРІ (РґР»СЏ LineChart)."""
    try:
        with _cursor() as cur:
            cur.execute("""
                SELECT
                    DATE_TRUNC('hour', requested_at)                              AS hour,
                    COUNT(*)                                                       AS total,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)            AS errors
                FROM chat_requests
                WHERE requested_at >= NOW() - INTERVAL '1 hour' * %s
                GROUP BY 1
                ORDER BY 1
            """, (hours,))
            return [
                {
                    "hour":   r["hour"].strftime("%H:%M") if r.get("hour") else "",
                    "total":  _safe(r.get("total"), int, 0),
                    "errors": _safe(r.get("errors"), int, 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# GET /api/dashboard/chat/tokens
# ---------------------------------------------------------------------------

@router.get("/chat/tokens")
def get_tokens_timeline(days: int = 14) -> list[dict]:
    """Р Р°СЃС…РѕРґ С‚РѕРєРµРЅРѕРІ РїРѕ РґРЅСЏРј Р·Р° РїРѕСЃР»РµРґРЅРёРµ N РґРЅРµР№ (РґР»СЏ BarChart)."""
    try:
        with _cursor() as cur:
            cur.execute("""
                SELECT
                    DATE_TRUNC('day', called_at)::date                            AS day,
                    COALESCE(SUM(total_tokens), 0)                                AS total_tokens,
                    COALESCE(SUM(prompt_tokens), 0)                               AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0)                           AS completion_tokens,
                    COUNT(*)                                                       AS calls
                FROM chat_llm_calls
                WHERE called_at >= NOW() - INTERVAL '1 day' * %s AND status = 'ok'
                GROUP BY 1
                ORDER BY 1
            """, (days,))
            return [
                {
                    "day":               r["day"].isoformat() if r.get("day") else "",
                    "total_tokens":      _safe(r.get("total_tokens"), int, 0),
                    "prompt_tokens":     _safe(r.get("prompt_tokens"), int, 0),
                    "completion_tokens": _safe(r.get("completion_tokens"), int, 0),
                    "calls":             _safe(r.get("calls"), int, 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# GET /api/dashboard/chat/recent
# ---------------------------------------------------------------------------

@router.get("/chat/recent")
def get_recent_requests(limit: int = 20) -> list[dict]:
    """РџРѕСЃР»РµРґРЅРёРµ N Р·Р°РїСЂРѕСЃРѕРІ СЃ РїСЂРµРІСЊСЋ Р·Р°РїСЂРѕСЃР° Рё РјРµС‚СЂРёРєР°РјРё."""
    try:
        with _cursor() as cur:
            cur.execute("""
                SELECT
                    r.request_id,
                    LEFT(r.session_id, 8)                                         AS session_short,
                    LEFT(r.query_text, 120)                                       AS query_preview,
                    r.status,
                    r.total_duration_ms,
                    l.total_tokens,
                    r.retrieved_chunks_n,
                    r.requested_at,
                    r.error_msg
                FROM chat_requests r
                LEFT JOIN chat_llm_calls l ON l.request_id = r.request_id
                ORDER BY r.requested_at DESC
                LIMIT %s
            """, (limit,))
            return [
                {
                    "request_id":        r["request_id"],
                    "session_short":     r["session_short"],
                    "query_preview":     r["query_preview"],
                    "status":            r["status"],
                    "total_duration_ms": _safe(r.get("total_duration_ms"), float),
                    "total_tokens":      _safe(r.get("total_tokens"), int),
                    "retrieved_chunks_n": _safe(r.get("retrieved_chunks_n"), int),
                    "requested_at":      _dt(r.get("requested_at")),
                    "error_msg":         r.get("error_msg"),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# GET /api/dashboard/chat/anomalies
# ---------------------------------------------------------------------------

@router.get("/chat/anomalies")
def get_anomalies() -> dict[str, list]:
    """РђРЅРѕРјР°Р»РёРё: Р·Р°РІРёСЃС€РёРµ Р·Р°РїСЂРѕСЃС‹, РјРµРґР»РµРЅРЅС‹Рµ РѕС‚РІРµС‚С‹, РїСЂРѕР±Р»РµРјРЅС‹Рµ СЃРµСЃСЃРёРё."""
    defaults: dict[str, list] = {"stuck": [], "slow": [], "error_sessions": []}
    try:
        with _cursor() as cur:
            # Р—Р°РІРёСЃС€РёРµ (in_progress > 5 РјРёРЅСѓС‚)
            cur.execute("""
                SELECT
                    request_id,
                    LEFT(session_id, 8)                                           AS session_short,
                    LEFT(query_text, 80)                                          AS query,
                    ROUND(EXTRACT(EPOCH FROM (NOW() - requested_at)) / 60.0, 1)  AS minutes_ago
                FROM chat_requests
                WHERE status = 'in_progress'
                  AND requested_at < NOW() - INTERVAL '5 minutes'
                ORDER BY requested_at
                LIMIT 10
            """)
            stuck = [
                {**dict(r), "minutes_ago": float(r.get("minutes_ago") or 0)}
                for r in (cur.fetchall() or [])
            ]

            # РњРµРґР»РµРЅРЅС‹Рµ РѕС‚РІРµС‚С‹ (> 30 СЃРµРє, РїРѕСЃР»РµРґРЅРёРµ 24С‡)
            cur.execute("""
                SELECT
                    request_id,
                    LEFT(session_id, 8)                                           AS session_short,
                    LEFT(query_text, 80)                                          AS query,
                    total_duration_ms
                FROM chat_requests
                WHERE total_duration_ms > 30000
                  AND requested_at >= NOW() - INTERVAL '24 hours'
                  AND status = 'ok'
                ORDER BY total_duration_ms DESC
                LIMIT 10
            """)
            slow = [
                {**dict(r), "total_duration_ms": float(r.get("total_duration_ms") or 0)}
                for r in (cur.fetchall() or [])
            ]

            # РџСЂРѕР±Р»РµРјРЅС‹Рµ СЃРµСЃСЃРёРё (РµСЃС‚СЊ РѕС€РёР±РєРё)
            cur.execute("""
                SELECT
                    LEFT(session_id, 8)                                           AS session_short,
                    session_id,
                    request_count,
                    error_count,
                    ROUND(100.0 * error_count / NULLIF(request_count, 0), 1)     AS error_pct
                FROM chat_sessions
                WHERE error_count > 0
                ORDER BY error_pct DESC, request_count DESC
                LIMIT 10
            """)
            error_sessions = [
                {**dict(r), "error_pct": float(r.get("error_pct") or 0)}
                for r in (cur.fetchall() or [])
            ]

            return {"stuck": stuck, "slow": slow, "error_sessions": error_sessions}
    except Exception:
        return defaults


# ---------------------------------------------------------------------------
# GET /api/dashboard/chat/docs
# ---------------------------------------------------------------------------

@router.get("/chat/docs")
def get_top_docs(limit: int = 10) -> list[dict]:
    """РўРѕРї РґРѕРєСѓРјРµРЅС‚РѕРІ РїРѕ С‡Р°СЃС‚РѕС‚Рµ РїРѕРїР°РґР°РЅРёСЏ РІ РєРѕРЅС‚РµРєСЃС‚ LLM."""
    try:
        with _cursor() as cur:
            cur.execute("""
                SELECT
                    doc_id,
                    COUNT(*)                            AS appearances,
                    COUNT(DISTINCT request_id)          AS unique_requests,
                    COALESCE(AVG(score), 0)             AS avg_score,
                    ROUND(AVG(rank)::numeric, 1)        AS avg_rank
                FROM chat_retrieved_chunks
                GROUP BY doc_id
                ORDER BY appearances DESC
                LIMIT %s
            """, (limit,))
            return [
                {
                    "doc_id":          r["doc_id"],
                    "appearances":     _safe(r.get("appearances"), int, 0),
                    "unique_requests": _safe(r.get("unique_requests"), int, 0),
                    "avg_score":       _safe(r.get("avg_score"), float, 0.0),
                    "avg_rank":        _safe(r.get("avg_rank"), float, 0.0),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# GET /api/dashboard/retrieve/overview
# ---------------------------------------------------------------------------

@router.get("/retrieve/overview")
def get_retrieve_overview() -> dict[str, Any]:
    """Ключевые retrieval-метрики за последние 24 часа."""
    defaults: dict[str, Any] = {
        "total_requests": 0,
        "ok_count": 0,
        "error_count": 0,
        "avg_total_ms": None,
        "p95_total_ms": None,
        "avg_semantic_ms": None,
        "avg_bm25_ms": None,
        "bm25_fallback_count": 0,
        "avg_result_hits": 0.0,
    }
    try:
        with _cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)                                                       AS total_requests,
                    SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END)               AS ok_count,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)            AS error_count,
                    AVG(total_duration_ms)                                        AS avg_total_ms,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms)
                        FILTER (WHERE status = 'ok')                              AS p95_total_ms,
                    AVG(semantic_duration_ms)                                     AS avg_semantic_ms,
                    AVG(bm25_duration_ms)                                         AS avg_bm25_ms,
                    SUM(CASE WHEN bm25_fallback THEN 1 ELSE 0 END)               AS bm25_fallback_count,
                    AVG(result_hits)                                              AS avg_result_hits
                FROM ret_requests
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                """
            )
            row = cur.fetchone() or {}
            return {
                "total_requests": _safe(row.get("total_requests"), int, 0),
                "ok_count": _safe(row.get("ok_count"), int, 0),
                "error_count": _safe(row.get("error_count"), int, 0),
                "avg_total_ms": _safe(row.get("avg_total_ms"), float),
                "p95_total_ms": _safe(row.get("p95_total_ms"), float),
                "avg_semantic_ms": _safe(row.get("avg_semantic_ms"), float),
                "avg_bm25_ms": _safe(row.get("avg_bm25_ms"), float),
                "bm25_fallback_count": _safe(row.get("bm25_fallback_count"), int, 0),
                "avg_result_hits": round(_safe(row.get("avg_result_hits"), float, 0.0), 2),
            }
    except Exception:
        return defaults


# ---------------------------------------------------------------------------
# GET /api/dashboard/retrieve/recent
# ---------------------------------------------------------------------------

@router.get("/retrieve/recent")
def get_retrieve_recent(limit: int = 20) -> list[dict]:
    """Последние retrieval-вызовы с этапными метриками."""
    try:
        with _cursor() as cur:
            cur.execute(
                """
                SELECT
                    request_id,
                    LEFT(query_text, 120)                                         AS query_preview,
                    cluster,
                    status,
                    candidates,
                    semantic_hits,
                    bm25_hits,
                    fused_hits,
                    result_hits,
                    bm25_missing_ids,
                    bm25_fallback,
                    semantic_duration_ms,
                    bm25_duration_ms,
                    total_duration_ms,
                    created_at,
                    error_msg
                FROM ret_requests
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "request_id": r["request_id"],
                    "query_preview": r.get("query_preview") or "",
                    "cluster": r.get("cluster"),
                    "status": r.get("status") or "ok",
                    "candidates": _safe(r.get("candidates"), int, 0),
                    "semantic_hits": _safe(r.get("semantic_hits"), int, 0),
                    "bm25_hits": _safe(r.get("bm25_hits"), int, 0),
                    "fused_hits": _safe(r.get("fused_hits"), int, 0),
                    "result_hits": _safe(r.get("result_hits"), int, 0),
                    "bm25_missing_ids": _safe(r.get("bm25_missing_ids"), int, 0),
                    "bm25_fallback": bool(r.get("bm25_fallback")),
                    "semantic_duration_ms": _safe(r.get("semantic_duration_ms"), float),
                    "bm25_duration_ms": _safe(r.get("bm25_duration_ms"), float),
                    "total_duration_ms": _safe(r.get("total_duration_ms"), float),
                    "created_at": _dt(r.get("created_at")),
                    "error_msg": r.get("error_msg"),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# GET /api/dashboard/retrieve/top_docs
# ---------------------------------------------------------------------------

@router.get("/retrieve/top_docs")
def get_retrieve_top_docs(limit: int = 10) -> list[dict]:
    """Топ документов по попаданиям в retrieval-контекст."""
    try:
        with _cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(doc_id, '')                                          AS doc_id,
                    COUNT(*)                                                       AS appearances,
                    COUNT(DISTINCT request_id)                                     AS unique_requests,
                    COALESCE(AVG(score), 0)                                        AS avg_score,
                    ROUND(AVG(rank)::numeric, 1)                                   AS avg_rank
                FROM ret_chunks
                GROUP BY COALESCE(doc_id, '')
                ORDER BY appearances DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "doc_id": r.get("doc_id") or "",
                    "appearances": _safe(r.get("appearances"), int, 0),
                    "unique_requests": _safe(r.get("unique_requests"), int, 0),
                    "avg_score": _safe(r.get("avg_score"), float, 0.0),
                    "avg_rank": _safe(r.get("avg_rank"), float, 0.0),
                }
                for r in (cur.fetchall() or [])
                if (r.get("doc_id") or "").strip()
            ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# GET /api/dashboard/artifacts
# ---------------------------------------------------------------------------

@router.get("/artifacts")
def get_artifacts_stats() -> dict[str, Any]:
    """Сводная статистика по parsed/generated/questions/chunks артефактам."""
    defaults: dict[str, Any] = {
        "parsed_docs": {"summary": {"files_count": 0, "total_chars": 0, "avg_chars": 0.0, "max_chars": 0,
                                    "total_tokens": 0, "avg_tokens": 0.0, "max_tokens": 0,
                                    "docs_with_tables": 0, "docs_enriched": 0, "sections_with_tables": 0,
                                    "sections_enriched": 0, "total_tables": 0, "enriched_tables": 0}, "items": []},
        "generated_docs": {"summary": {"files_count": 0, "total_chars": 0, "avg_chars": 0.0, "max_chars": 0,
                                       "total_tokens": 0, "avg_tokens": 0.0, "max_tokens": 0}, "items": []},
        "questions": {"summary": {"files_count": 0, "total_chars": 0, "avg_chars": 0.0, "max_chars": 0,
                                  "total_tokens": 0, "avg_tokens": 0.0, "max_tokens": 0,
                                  "total_questions": 0, "avg_questions_per_doc": 0.0, "max_questions_per_doc": 0},
                      "items": []},
        "chunks": {"summary": {"files_count": 0, "total_chars": 0, "avg_chars": 0.0, "max_chars": 0,
                               "total_tokens": 0, "avg_tokens": 0.0, "max_tokens": 0,
                               "total_chunks": 0, "avg_chunks_per_doc": 0.0, "max_chunks_per_doc": 0,
                               "total_indexed_chunks": 0, "avg_tokens_per_chunk": 0.0},
                   "items": []},
    }
    try:
        return _build_artifact_stats()
    except Exception:
        return defaults


# ---------------------------------------------------------------------------
# GET /api/dashboard/pipeline
# ---------------------------------------------------------------------------

@router.get("/pipeline")
def get_pipeline() -> dict[str, list]:
    """РџРѕСЃР»РµРґРЅРёРµ Р·Р°РїСѓСЃРєРё РїР°Р№РїР»Р°Р№РЅРѕРІ РїР°СЂСЃРёРЅРіР° Рё РіРµРЅРµСЂР°С†РёРё, СЃРІРѕРґРєР° РІР°Р»РёРґР°С†РёРё."""
    result: dict[str, list] = {
        "parse_sessions": [],
        "gen_sessions": [],
        "validation_summary": [],
    }

    # Parse sessions
    try:
        with _cursor() as cur:
            cur.execute("""
                SELECT
                    session_id,
                    started_at,
                    COALESCE(duration_sec, 0)     AS duration_sec,
                    docs_total, docs_ok, docs_failed, docs_skipped,
                    flag_force,
                    flag_only_filter,
                    (finished_at IS NULL)          AS is_running
                FROM parse_sessions
                ORDER BY started_at DESC
                LIMIT 10
            """)
            result["parse_sessions"] = [
                {
                    **{k: v for k, v in dict(r).items() if not isinstance(v, (datetime, date))},
                    "started_at":   _dt(r.get("started_at")),
                    "duration_sec": float(r.get("duration_sec") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        pass

    # Gen sessions
    try:
        with _cursor() as cur:
            cur.execute("""
                SELECT
                    session_id,
                    script_name,
                    started_at,
                    COALESCE(duration_sec, 0)     AS duration_sec,
                    docs_total, docs_ok, docs_failed, docs_skipped,
                    flag_force,
                    (finished_at IS NULL)          AS is_running
                FROM gen_sessions
                ORDER BY started_at DESC
                LIMIT 10
            """)
            result["gen_sessions"] = [
                {
                    **{k: v for k, v in dict(r).items() if not isinstance(v, (datetime, date))},
                    "started_at":   _dt(r.get("started_at")),
                    "duration_sec": float(r.get("duration_sec") or 0),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        pass

    # Validation summary (РїРѕСЃР»РµРґРЅРёР№ Р·Р°РїСѓСЃРє validator.py)
    try:
        with _cursor() as cur:
            cur.execute("""
                WITH last_val AS (
                    SELECT session_id FROM gen_sessions
                    WHERE script_name = 'validator'
                    ORDER BY started_at DESC LIMIT 1
                )
                SELECT
                    r.check_name,
                    r.artifact_type,
                    COUNT(*)                                                       AS total,
                    SUM(CASE WHEN r.passed THEN 1 ELSE 0 END)                    AS passed,
                    ROUND(
                        100.0 * SUM(CASE WHEN r.passed THEN 1 ELSE 0 END) / COUNT(*),
                    1)                                                             AS pass_rate
                FROM gen_validation_results r
                JOIN last_val l ON l.session_id = r.session_id
                GROUP BY r.check_name, r.artifact_type
                ORDER BY r.artifact_type, pass_rate ASC
            """)
            result["validation_summary"] = [
                {
                    "check_name":    r["check_name"],
                    "artifact_type": r["artifact_type"],
                    "total":         _safe(r.get("total"), int, 0),
                    "passed":        _safe(r.get("passed"), int, 0),
                    "pass_rate":     _safe(r.get("pass_rate"), float, 0.0),
                }
                for r in (cur.fetchall() or [])
            ]
    except Exception:
        pass

    return result
