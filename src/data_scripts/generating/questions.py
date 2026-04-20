"""Генерация вопросов и ответов по синтетическим банковским документам.

Читает data/generated/{doc_id}.md, вызывает LLM, сохраняет
data/questions/{doc_id}_questions.json.

CLI:
  python questions.py                          # пропустить уже сгенерированные
  python questions.py --force                  # перегенерировать всё
  python questions.py --only RG-KIB-001        # только указанные
  python questions.py --count 15               # N вопросов (default из config)
  python questions.py --log-level DEBUG
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
import yaml
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI
from config_validation import validate_pipeline_config

# ---------------------------------------------------------------------------
# Настройка путей
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

# generating/ → data_scripts/ → src/ → root (3 уровня)
ROOT_DIR = Path(__file__).resolve().parents[3]
load_dotenv(dotenv_path=ROOT_DIR / ".env")

CONFIG_PATH = ROOT_DIR / "public" / "config.yaml"
GENERATED_DIR = ROOT_DIR / "data" / "generated"
QUESTIONS_DIR = ROOT_DIR / "data" / "questions"
PARSED_DIR = ROOT_DIR / "data" / "parsed"

from db_logging.run_logger import RunLogger           # noqa: E402
from db_logging.log_utils import log_llm_ok, log_llm_error  # noqa: E402

# ---------------------------------------------------------------------------
# Системный промпт
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """Ты — эксперт по банковской документации и нормативному регулированию.
Твоя задача — генерировать вопросы и ответы для обучения RAG-системы,
которая помогает сотрудникам банка ориентироваться во внутренних регламентах.

Вопросы должны звучать как реальные вопросы опытного сотрудника банка,
а не как формальные тестовые задания. Каждый ответ должен быть развёрнутым
(не менее 3 предложений) и содержать конкретные детали из документа."""

MAX_DOC_CHARS = 90_000


# ---------------------------------------------------------------------------
# Конфиг
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Построение промпта
# ---------------------------------------------------------------------------

def _calc_distribution(count: int, qa_config: dict) -> dict[str, int]:
    """Вычисляет распределение вопросов по типам пропорционально count.

    Базовое распределение берётся из qa_config.types[*].count.
    Остаток добавляется к первому типу.
    """
    types = qa_config.get("types", {})
    base_total = sum(v.get("count", 0) for v in types.values())
    if base_total == 0:
        base_total = len(types) or 1

    distribution: dict[str, int] = {}
    assigned = 0
    type_keys = list(types.keys())

    for key in type_keys:
        base_count = types[key].get("count", 1)
        n = max(1, math.floor(count * base_count / base_total))
        distribution[key] = n
        assigned += n

    # Остаток кладём в первый тип
    if type_keys:
        distribution[type_keys[0]] += count - assigned

    return distribution




def _clip_doc_text(text: str, max_chars: int = MAX_DOC_CHARS) -> tuple[str, bool]:
    """Limit document length for prompt: head + middle + tail."""
    if len(text) <= max_chars:
        return text, False

    part = max_chars // 3
    head = text[:part]
    middle_start = max(0, (len(text) // 2) - (part // 2))
    middle = text[middle_start: middle_start + part]
    tail = text[-part:]
    clipped = (
        f"{head}\n\n...[DOCUMENT TRUNCATED]...\n\n"
        f"{middle}\n\n...[DOCUMENT TRUNCATED]...\n\n"
        f"{tail}"
    )
    return clipped[:max_chars], True

def build_questions_prompt(
    doc_id: str,
    doc_text: str,
    count: int,
    qa_config: dict,
) -> str:
    """Строит промпт для генерации вопросов."""
    types = qa_config.get("types", {})
    distribution = _calc_distribution(count, qa_config)
    style_rules = "\n".join(f"- {r}" for r in qa_config.get("style_requirements", []))
    clipped_text, was_clipped = _clip_doc_text(doc_text)
    truncation_note = (
        "\\nNOTE: document was truncated to fit context limits. Use only the provided excerpt.\\n"
        if was_clipped else ""
    )

    type_descriptions = []
    for type_key, type_cfg in types.items():
        n = distribution.get(type_key, 0)
        desc = type_cfg.get("description", "")
        example = type_cfg.get("example", "")
        type_descriptions.append(
            f'  - "{type_key}" ({n} вопросов): {desc}\n'
            f'    Пример: «{example}»'
        )

    prompt = f"""Ниже приведён внутренний банковский регламент с идентификатором {doc_id}.
Сгенерируй ровно {count} вопросов и ответов по этому документу.

Распределение по типам сложности:
{chr(10).join(type_descriptions)}

Требования к стилю вопросов:
{style_rules}

Требования к ответам:
- Каждый ответ должен содержать не менее 3 развёрнутых предложений
- Ответ должен опираться на конкретные положения документа
- Включать точные цифры, сроки, пороговые значения из документа

Верни ТОЛЬКО JSON-массив следующего формата, без какого-либо текста до или после:
[
  {{
    "difficulty": "<тип из списка выше>",
    "question": "<текст вопроса>",
    "answer": "<развёрнутый ответ>",
    "source_hint": "<раздел или пункт документа>"
  }},
  ...
]

--- ДОКУМЕНТ ---
{clipped_text}
"""
    return prompt


# ---------------------------------------------------------------------------
# Парсинг ответа LLM
# ---------------------------------------------------------------------------

def parse_llm_response(raw: str, doc_id: str) -> list[dict]:
    """Извлекает JSON-массив из ответа LLM.

    LLM иногда оборачивает JSON в markdown-блок ```json...```.
    Стратегия:
    1. Прямой json.loads(raw.strip())
    2. Найти первый '[' и последний ']', взять подстроку, повторить json.loads
    """
    text = raw.strip()

    # Убрать возможный markdown-блок
    if text.startswith("```"):
        lines = text.split("\n")
        # Убрать первую строку (```json или ```) и последнюю (```)
        inner = "\n".join(lines[1:])
        if inner.endswith("```"):
            inner = inner[: inner.rfind("```")]
        text = inner.strip()

    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        raise ValueError(f"LLM вернул не массив, а {type(result).__name__}")
    except json.JSONDecodeError:
        pass

    # Fallback: найти границы массива
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(
            f"[{doc_id}] Не удалось найти JSON-массив в ответе LLM. "
            f"Начало ответа: {text[:200]!r}"
        )

    try:
        result = json.loads(text[start: end + 1])
        if isinstance(result, list):
            return result
        raise ValueError(f"LLM вернул не массив, а {type(result).__name__}")
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"[{doc_id}] JSON парсинг не удался даже после выделения [...]: {exc}"
        ) from exc


def _normalize_questions(questions: list[dict], doc_id: str) -> list[dict]:
    """Добавляет поле 'id' если отсутствует, проверяет обязательные поля."""
    normalized = []
    for i, q in enumerate(questions):
        q = dict(q)
        if "id" not in q:
            q["id"] = f"{doc_id}_q{i + 1:03d}"
        # Убедиться что обязательные поля есть
        if "question" not in q or "answer" not in q:
            logging.warning("[%s] Вопрос %d пропущен — отсутствуют поля question/answer", doc_id, i + 1)
            continue
        if "difficulty" not in q:
            q["difficulty"] = "factoid"
        if "source_hint" not in q:
            q["source_hint"] = ""
        normalized.append(q)
    return normalized


# ---------------------------------------------------------------------------
# Генерация вопросов
# ---------------------------------------------------------------------------

def generate_questions(
    doc_id: str,
    doc_spec: dict,
    doc_text: str,
    count: int,
    qa_config: dict,
    client: OpenAI,
    *,
    run_log=None,
    op_id: str | None = None,
) -> list[dict]:
    """Вызывает LLM и возвращает список вопросов."""
    prompt = build_questions_prompt(doc_id, doc_text, count, qa_config)

    for attempt in range(2):
        t0 = time.monotonic()
        try:
            response = client.chat.completions.create(
                model="deepseek-chat",
                max_tokens=8000,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            )
            duration_ms = (time.monotonic() - t0) * 1000
            log_llm_ok(run_log, op_id, doc_id, response, duration_ms,
                       attempt_num=attempt + 1)
            raw = response.choices[0].message.content
            questions = parse_llm_response(raw, doc_id)
            return _normalize_questions(questions, doc_id)
        except Exception as e:
            duration_ms = (time.monotonic() - t0) * 1000
            log_llm_error(run_log, op_id, doc_id, e, duration_ms,
                          attempt_num=attempt + 1)
            if attempt == 0:
                logging.warning("[RETRY] %s: %s", doc_id, e)
                time.sleep(5)
            else:
                raise


# ---------------------------------------------------------------------------
# Сохранение
# ---------------------------------------------------------------------------

def save_questions(
    doc_id: str,
    doc_spec: dict,
    questions: list[dict],
    model: str,
) -> Path:
    """Сохраняет {doc_id}_questions.json в QUESTIONS_DIR."""
    QUESTIONS_DIR.mkdir(parents=True, exist_ok=True)
    output = {
        "doc_id": doc_id,
        "doc_title": doc_spec.get("title", ""),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "questions_count": len(questions),
        "questions": questions,
    }
    path = QUESTIONS_DIR / f"{doc_id}_questions.json"
    path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(default_count: int = 10) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Генерация вопросов и ответов по синтетическим регламентам",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
примеры:
  python questions.py                          # пропустить уже сгенерированные
  python questions.py --force                  # перегенерировать всё
  python questions.py --only RG-KIB-001        # только указанные
  python questions.py --count 15               # N вопросов на документ
  python questions.py --log-level DEBUG
        """,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--force", action="store_true",
                       help="Перегенерировать все вопросы")
    group.add_argument("--only", nargs="+", metavar="DOC_ID",
                       help="Только указанные document ID")
    parser.add_argument("--count", type=int, default=default_count,
                        metavar="N", help=f"Количество вопросов (default: {default_count})")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        dest="log_level")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    config = load_config()
    qa_config = config.get("qa_generation", {})
    default_count = qa_config.get("per_document", 10)

    args = parse_args(default_count=default_count)
    setup_logging(args.log_level)

    validation_errors = validate_pipeline_config(config, PARSED_DIR)
    if validation_errors:
        for error in validation_errors:
            logging.error("CONFIG: %s", error)
        sys.exit(1)

    client = OpenAI(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url="https://api.deepseek.com",
    )

    docs_by_id = {doc["id"]: doc for doc in config["documents"]}
    generation_order = config["pipeline"]["generation_order"]
    total = len(generation_order)

    if args.only:
        target_ids: set[str] | None = set(args.only)
        force = True
    elif args.force:
        target_ids = None
        force = True
    else:
        target_ids = None
        force = False

    docs_ok = 0
    docs_failed = 0
    docs_skipped = 0

    with RunLogger(args, script_name="questions") as run_log:
        for i, doc_id in enumerate(generation_order, 1):
            if doc_id not in docs_by_id:
                logging.debug("[%d/%d] SKIP %s — нет в documents", i, total, doc_id)
                docs_skipped += 1
                continue

            if target_ids is not None and doc_id not in target_ids:
                docs_skipped += 1
                continue

            doc_spec = docs_by_id[doc_id]
            md_path = GENERATED_DIR / f"{doc_id}.md"
            questions_path = QUESTIONS_DIR / f"{doc_id}_questions.json"

            if not md_path.exists():
                logging.warning(
                    "[%d/%d] SKIP %s — не найден %s (сначала запусти generating.py)",
                    i, total, doc_id, md_path.name,
                )
                docs_skipped += 1
                continue

            if not force and questions_path.exists():
                logging.info("[%d/%d] SKIP %s — вопросы уже сгенерированы", i, total, doc_id)
                op_id = run_log.start_operation(doc_id, doc_spec)
                run_log.end_operation(op_id, status="skipped")
                docs_skipped += 1
                continue

            logging.info("[%d/%d] Generating questions for %s…", i, total, doc_id)
            op_id = run_log.start_operation(doc_id, doc_spec)

            try:
                doc_text = md_path.read_text(encoding="utf-8")
                questions = generate_questions(
                    doc_id, doc_spec, doc_text, args.count, qa_config, client,
                    run_log=run_log, op_id=op_id,
                )
                path = save_questions(doc_id, doc_spec, questions, model="deepseek-chat")
                run_log.end_operation(
                    op_id,
                    status="ok",
                    output_path=str(path.relative_to(ROOT_DIR)),
                )
                logging.info("  [OK] %s (%d вопросов)", path.name, len(questions))
                docs_ok += 1
            except Exception as e:
                logging.error("  [FAIL] %s: %s", doc_id, e)
                run_log.end_operation(op_id, status="failed", error_msg=str(e))
                docs_failed += 1

        run_log.finalize(
            docs_total=total,
            docs_ok=docs_ok,
            docs_failed=docs_failed,
            docs_skipped=docs_skipped,
        )

    logging.info("=" * 60)
    logging.info("Готово: %d/%d успешно. Output: %s", docs_ok, total, QUESTIONS_DIR)
    if docs_failed:
        logging.warning("Не удалось: %d документов", docs_failed)


if __name__ == "__main__":
    main()
