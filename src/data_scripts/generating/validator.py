"""Валидация сгенерированных документов и вопросов.

Проверяет data/generated/*.md и data/questions/*.json.
Сохраняет отчёт в data/validation_report.json.

CLI:
  python validator.py                          # валидировать всё
  python validator.py --only RG-KIB-001        # только указанные
  python validator.py --check documents        # только документы
  python validator.py --check questions        # только вопросы
  python validator.py --check all              # по умолчанию
  python validator.py --log-level DEBUG
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import yaml
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

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
REPORT_PATH = ROOT_DIR / "data" / "validation_report.json"

from db_logging.run_logger import RunLogger   # noqa: E402
from db_logging.log_utils import log_check    # noqa: E402


# ---------------------------------------------------------------------------
# CheckResult
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    doc_id: str
    artifact_type: str    # 'document' | 'questions'
    check_name: str
    passed: bool
    expected_value: str | None = None
    actual_value: str | None = None
    detail: str | None = None


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ref_variants(doc_id: str) -> list[str]:
    """Строит варианты написания doc_id для поиска в тексте.

    Примеры:
        '115-FZ'  → ['115-ФЗ', '115-FZ', '№ 115']
        '499-P'   → ['499-П',  '499-P',  '№ 499']
        '6406-U'  → ['6406-У', '6406-U', '№ 6406']
        '220-I'   → ['220-И',  '220-I',  '№ 220']
    """
    numeric = re.sub(r"[^0-9].*$", "", doc_id)   # '115-FZ' → '115'
    ru = (
        doc_id
        .replace("-FZ", "-ФЗ")
        .replace("-P", "-П")
        .replace("-U", "-У")
        .replace("-I", "-И")
    )
    return [ru, doc_id, f"№ {numeric}"]


# ---------------------------------------------------------------------------
# Проверки документов
# ---------------------------------------------------------------------------

# Ключевые слова для поиска обязательных разделов.
# LLM переформулирует заголовки, поэтому ищем по подстроке без учёта регистра.
_SECTION_KEYWORDS = [
    "ОБЩИЕ ПОЛОЖЕНИЯ",
    "ОСНОВНАЯ ЧАСТЬ",
    "КОНТРОЛЬ",
    "ЗАКЛЮЧИТЕЛЬН",
]


def check_word_count(doc_id: str, text: str, target_words: int) -> CheckResult:
    """Проверяет, что объём документа находится в диапазоне target ±30%."""
    actual = len(text.split())
    lo = int(target_words * 0.7)
    hi = int(target_words * 1.3)
    passed = lo <= actual <= hi
    return CheckResult(
        doc_id=doc_id,
        artifact_type="document",
        check_name="word_count",
        passed=passed,
        expected_value=f"{lo}-{hi} слов",
        actual_value=str(actual),
        detail=(
            f"Фактически {actual} слов, цель {target_words} ±30% [{lo}-{hi}]"
            if not passed
            else f"{actual} слов — в пределах нормы"
        ),
    )


def check_sections_present(
    doc_id: str,
    text: str,
    required_sections: list[str],
) -> CheckResult:
    """Проверяет наличие четырёх обязательных разделов по ключевым словам."""
    text_upper = text.upper()
    missing = [
        kw for kw in _SECTION_KEYWORDS
        if kw not in text_upper
    ]
    passed = len(missing) == 0
    return CheckResult(
        doc_id=doc_id,
        artifact_type="document",
        check_name="sections_present",
        passed=passed,
        expected_value=json.dumps(_SECTION_KEYWORDS, ensure_ascii=False),
        actual_value=json.dumps(missing, ensure_ascii=False),
        detail=(
            f"Отсутствуют разделы: {', '.join(missing)}"
            if missing
            else "Все обязательные разделы присутствуют"
        ),
    )


def check_references_cited(
    doc_id: str,
    text: str,
    references: list,
) -> CheckResult:
    """Проверяет, что каждый real_doc из references упомянут в тексте."""
    if not references or isinstance(references[0], str):
        # Старый формат или пустые ссылки — пропускаем
        return CheckResult(
            doc_id=doc_id,
            artifact_type="document",
            check_name="references_cited",
            passed=True,
            detail="Ссылки отсутствуют или в старом формате — проверка пропущена",
        )

    text_lower = text.lower()
    not_found: list[str] = []
    ref_doc_ids = list(dict.fromkeys(r["doc"] for r in references if isinstance(r, dict)))

    for ref_doc_id in ref_doc_ids:
        variants = _ref_variants(ref_doc_id)
        if not any(v.lower() in text_lower for v in variants):
            not_found.append(ref_doc_id)

    passed = len(not_found) == 0
    return CheckResult(
        doc_id=doc_id,
        artifact_type="document",
        check_name="references_cited",
        passed=passed,
        expected_value=json.dumps(ref_doc_ids, ensure_ascii=False),
        actual_value=json.dumps(not_found, ensure_ascii=False),
        detail=(
            f"Не упомянуты в тексте: {', '.join(not_found)}"
            if not_found
            else "Все нормативные источники упомянуты"
        ),
    )


# ---------------------------------------------------------------------------
# Проверки вопросов
# ---------------------------------------------------------------------------

def check_questions_count(
    doc_id: str,
    questions: list[dict],
    expected_count: int,
) -> CheckResult:
    """Проверяет минимальное количество вопросов."""
    actual = len(questions)
    passed = actual >= expected_count
    return CheckResult(
        doc_id=doc_id,
        artifact_type="questions",
        check_name="questions_count",
        passed=passed,
        expected_value=f"≥{expected_count}",
        actual_value=str(actual),
        detail=f"Вопросов: {actual} (требуется ≥{expected_count})",
    )


def check_answer_length(doc_id: str, questions: list[dict]) -> CheckResult:
    """Проверяет, что каждый ответ содержит не менее 50 слов."""
    min_words = 50
    short: list[dict] = []
    for q in questions:
        words = len(q.get("answer", "").split())
        if words < min_words:
            short.append({"id": q.get("id", "?"), "words": words})

    passed = len(short) == 0
    return CheckResult(
        doc_id=doc_id,
        artifact_type="questions",
        check_name="answer_length",
        passed=passed,
        expected_value=f"≥{min_words} слов в каждом ответе",
        actual_value=json.dumps(short, ensure_ascii=False) if short else "[]",
        detail=(
            f"Короткие ответы ({len(short)} шт.): {short}"
            if short
            else "Все ответы достаточно длинные"
        ),
    )


def check_difficulty_distribution(
    doc_id: str,
    questions: list[dict],
) -> CheckResult:
    """Проверяет наличие хотя бы 1 вопроса каждого типа сложности."""
    required = {"factoid", "synthetic", "cross_document"}
    counts: dict[str, int] = {}
    for q in questions:
        d = q.get("difficulty", "unknown")
        counts[d] = counts.get(d, 0) + 1

    missing = [d for d in required if counts.get(d, 0) == 0]
    passed = len(missing) == 0
    return CheckResult(
        doc_id=doc_id,
        artifact_type="questions",
        check_name="difficulty_distribution",
        passed=passed,
        expected_value="≥1 вопрос каждого типа: factoid, synthetic, cross_document",
        actual_value=json.dumps(counts, ensure_ascii=False),
        detail=(
            f"Отсутствуют типы: {', '.join(missing)}"
            if missing
            else f"Все типы представлены: {counts}"
        ),
    )


# ---------------------------------------------------------------------------
# Валидация по типу артефакта
# ---------------------------------------------------------------------------

def validate_document(
    doc_id: str,
    doc_spec: dict,
    required_sections: list[str],
) -> list[CheckResult]:
    """Запускает все три проверки документа."""
    md_path = GENERATED_DIR / f"{doc_id}.md"
    if not md_path.exists():
        return [CheckResult(
            doc_id=doc_id,
            artifact_type="document",
            check_name="file_exists",
            passed=False,
            detail=f"Файл не найден: {md_path.name}",
        )]

    text = md_path.read_text(encoding="utf-8")
    target_words = doc_spec.get("target_words", 2000)
    references = doc_spec.get("references", [])

    return [
        check_word_count(doc_id, text, target_words),
        check_sections_present(doc_id, text, required_sections),
        check_references_cited(doc_id, text, references),
    ]


def validate_questions(
    doc_id: str,
    expected_count: int,
) -> list[CheckResult]:
    """Загружает questions.json и запускает все три проверки."""
    questions_path = QUESTIONS_DIR / f"{doc_id}_questions.json"
    if not questions_path.exists():
        return [CheckResult(
            doc_id=doc_id,
            artifact_type="questions",
            check_name="file_exists",
            passed=False,
            detail=f"Файл не найден: {questions_path.name}",
        )]

    data = json.loads(questions_path.read_text(encoding="utf-8"))
    questions = data.get("questions", [])

    return [
        check_questions_count(doc_id, questions, expected_count),
        check_answer_length(doc_id, questions),
        check_difficulty_distribution(doc_id, questions),
    ]


# ---------------------------------------------------------------------------
# Построение отчёта
# ---------------------------------------------------------------------------

def build_report(results: list[CheckResult]) -> dict:
    """Собирает финальный JSON-отчёт."""
    total = len(results)
    passed_count = sum(1 for r in results if r.passed)
    failed_count = total - passed_count

    by_document: dict[str, dict] = {}
    for r in results:
        doc = by_document.setdefault(r.doc_id, {"document": [], "questions": []})
        doc[r.artifact_type].append(asdict(r))

    failed_checks = [asdict(r) for r in results if not r.passed]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_checks": total,
            "passed": passed_count,
            "failed": failed_count,
            "pass_rate": round(passed_count / total, 3) if total > 0 else 0.0,
        },
        "by_document": by_document,
        "failed_checks": failed_checks,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Валидация сгенерированных документов и вопросов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
примеры:
  python validator.py                          # валидировать всё
  python validator.py --only RG-KIB-001        # только указанный документ
  python validator.py --check documents        # только документы
  python validator.py --check questions        # только вопросы
  python validator.py --log-level DEBUG
        """,
    )
    parser.add_argument("--only", nargs="+", metavar="DOC_ID",
                        help="Только указанные document ID")
    parser.add_argument("--check", default="all",
                        choices=["documents", "questions", "all"],
                        help="Что проверять (default: all)")
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
    args = parse_args()
    setup_logging(args.log_level)

    config = _load_config()
    docs_by_id = {doc["id"]: doc for doc in config["documents"]}
    generation_order = config["pipeline"]["generation_order"]
    required_sections = config.get("document_template", {}).get("sections", [])
    expected_q_count = config.get("qa_generation", {}).get("per_document", 10)

    if args.only:
        doc_ids = [d for d in generation_order if d in set(args.only)]
    else:
        doc_ids = [d for d in generation_order if d in docs_by_id]

    total = len(doc_ids)
    docs_ok = 0
    docs_failed = 0
    all_results: list[CheckResult] = []

    with RunLogger(args, script_name="validator") as run_log:
        for i, doc_id in enumerate(doc_ids, 1):
            doc_spec = docs_by_id.get(doc_id, {})
            logging.info("[%d/%d] Validating %s…", i, total, doc_id)

            op_id = run_log.start_operation(doc_id, doc_spec)
            doc_results: list[CheckResult] = []
            q_results: list[CheckResult] = []

            if args.check in ("documents", "all"):
                doc_results = validate_document(doc_id, doc_spec, required_sections)
                all_results.extend(doc_results)
                for r in doc_results:
                    log_check(
                        run_log, doc_id, "document", r.check_name,
                        passed=r.passed,
                        expected_value=r.expected_value,
                        actual_value=r.actual_value,
                        detail=r.detail,
                        op_id=op_id,
                    )

            if args.check in ("questions", "all"):
                q_results = validate_questions(doc_id, expected_q_count)
                all_results.extend(q_results)
                for r in q_results:
                    log_check(
                        run_log, doc_id, "questions", r.check_name,
                        passed=r.passed,
                        expected_value=r.expected_value,
                        actual_value=r.actual_value,
                        detail=r.detail,
                        op_id=op_id,
                    )

            combined = doc_results + q_results
            any_failed = any(not r.passed for r in combined)
            op_status = "failed" if any_failed else "ok"
            run_log.end_operation(
                op_id,
                status=op_status,
                output_path=str(REPORT_PATH.relative_to(ROOT_DIR)),
            )

            passed_here = sum(1 for r in combined if r.passed)
            total_here = len(combined)
            logging.info(
                "  %s — %d/%d проверок пройдено%s",
                doc_id, passed_here, total_here,
                "" if not any_failed else f" (FAILED: {[r.check_name for r in combined if not r.passed]})",
            )

            if any_failed:
                docs_failed += 1
            else:
                docs_ok += 1

        run_log.finalize(
            docs_total=total,
            docs_ok=docs_ok,
            docs_failed=docs_failed,
            docs_skipped=0,
        )

    # Сохранить отчёт
    report = build_report(all_results)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    logging.info("=" * 60)
    logging.info(
        "Итог: %d документов, %d/%d проверок пройдено (%.0f%%)",
        total,
        report["summary"]["passed"],
        report["summary"]["total_checks"],
        report["summary"]["pass_rate"] * 100,
    )
    logging.info("Отчёт: %s", REPORT_PATH)
    if docs_failed:
        logging.warning("Документов с ошибками: %d", docs_failed)


if __name__ == "__main__":
    main()
