"""Генерация синтетических внутрибанковских регламентов.

Читает documents[] из config.yaml, для каждого документа:
  1. Собирает нормативную базу из data/parsed/{doc_id}_sections.json.
  2. Строит промпт с контекстом банка, стилевыми требованиями и ссылками.
  3. Вызывает DeepSeek через OpenAI SDK.
  4. Сохраняет {doc_id}.md и {doc_id}_meta.json в data/generated/.

CLI:
  python generating.py                          # пропустить уже сгенерированные
  python generating.py --force                  # перегенерировать всё
  python generating.py --only RG-KIB-001 RG-KIB-003
  python generating.py --changed changed.txt    # один doc_id на строку
  python generating.py --log-level DEBUG
"""
from __future__ import annotations

import argparse
import json
import logging
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
OUTPUT_DIR = ROOT_DIR / "data" / "generated"
PARSED_DIR = ROOT_DIR / "data" / "parsed"

# ---------------------------------------------------------------------------
# Импорты DB-логирования (после манипуляций с sys.path)
# ---------------------------------------------------------------------------
from db_logging.run_logger import RunLogger          # noqa: E402
from db_logging.log_utils import log_llm_ok, log_llm_error  # noqa: E402

# ---------------------------------------------------------------------------
# Инициализация клиента и конфига (уровень модуля — без побочных эффектов)
# ---------------------------------------------------------------------------
client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
)

with open(CONFIG_PATH, encoding="utf-8") as f:
    config = yaml.safe_load(f)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Загрузка спарсенных секций реальных НПА
# ---------------------------------------------------------------------------
_sections_cache: dict[str, dict] = {}
MAX_REF_SECTION_CHARS = 20_000
MAX_REF_TOTAL_CHARS = 90_000
_CLIP_MARKER = "\n\n[...фрагмент сокращён для ограничения размера контекста...]\n\n"


def _load_doc_sections(doc_id: str) -> dict:
    """Загружает индекс секций документа (с кэшированием)."""
    if doc_id not in _sections_cache:
        path = PARSED_DIR / f"{doc_id}_sections.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                _sections_cache[doc_id] = json.load(f)
        else:
            _sections_cache[doc_id] = {}
    return _sections_cache[doc_id]


def _clip_text_middle(text: str, max_chars: int) -> tuple[str, bool]:
    """Ограничивает размер текста, сохраняя начало и конец."""
    if len(text) <= max_chars:
        return text, False
    if max_chars <= len(_CLIP_MARKER) + 200:
        return text[:max_chars], True

    body_budget = max_chars - len(_CLIP_MARKER)
    head = int(body_budget * 0.7)
    tail = body_budget - head
    clipped = text[:head].rstrip() + _CLIP_MARKER + text[-tail:].lstrip()
    return clipped, True


def build_reference_context(doc_spec: dict) -> str:
    """Формирует блок нормативной базы для промпта.

    Если секции реальных документов спарсены — вставляет текст дословно.
    Если хоть одна требуемая секция отсутствует — выбрасывает ошибку.
    """
    refs = doc_spec.get("references", [])
    if not refs:
        return ""

    if isinstance(refs[0], str):
        raise ValueError(
            "Неподдерживаемый формат references (list[str]). "
            "Используй структуру list[dict] с полями doc/sections."
        )

    found: list[str] = []
    missing: list[str] = []
    shortened_notes: list[str] = []
    remaining_budget = MAX_REF_TOTAL_CHARS

    for ref in refs:
        doc_id = ref["doc"]
        sections = _load_doc_sections(doc_id)
        for sec in ref.get("sections", []):
            sec_id = sec["id"]
            sec_note = sec.get("note", "без пояснения")
            entry = sections.get(sec_id)
            if entry:
                text = entry.get("text", entry) if isinstance(entry, dict) else str(entry)
                title = entry.get("title", "") if isinstance(entry, dict) else ""
                header = f"**{doc_id}, {sec_id}**" + (f" — {title}" if title else f" ({sec_note})")

                normalized = text.strip()
                normalized, section_clipped = _clip_text_middle(
                    normalized, MAX_REF_SECTION_CHARS
                )

                if len(normalized) > remaining_budget:
                    if remaining_budget <= 0:
                        shortened_notes.append(f"{doc_id} {sec_id}: пропущено по лимиту контекста")
                        continue
                    normalized, budget_clipped = _clip_text_middle(normalized, remaining_budget)
                    section_clipped = section_clipped or budget_clipped

                remaining_budget -= len(normalized)
                if section_clipped:
                    shortened_notes.append(f"{doc_id} {sec_id}: текст сокращён")

                found.append(f"\n{header}\n{normalized}")
            else:
                missing.append(f"{doc_id} {sec_id} ({sec_note})")

    lines: list[str] = []
    if found:
        lines.append(
            "Нормативная база — извлечения из актуальных документов "
            "(используй эти формулировки и цифры дословно, не изменяй):"
        )
        if shortened_notes:
            lines.append(
                "Примечание: часть длинных извлечений сокращена, чтобы уложиться в лимит входного контекста."
            )
        lines.extend(found)
    if missing:
        raise ValueError(
            "Отсутствуют обязательные секции нормативной базы для генерации: "
            f"{'; '.join(missing)}. "
            "Сначала обнови parsing.py / registry.py и пересобери data/parsed/*.json."
        )
    if shortened_notes:
        logging.warning(
            "%s: нормативный контекст сокращён (%s)",
            doc_spec.get("id", "unknown"),
            "; ".join(shortened_notes),
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Построение промптов
# ---------------------------------------------------------------------------
_tmpl = config["document_template"]
_style_rules = "\n".join(f"- {r}" for r in _tmpl["style_requirements"])

SYSTEM_PROMPT = f"""
Ты — генератор внутрибанковской документации.
Ты создаёшь внутренние регламенты для вымышленного банка {config['bank']['name']}.

Справочник банка:
{yaml.dump(config['bank'], allow_unicode=True)}

Требования к стилю:
{_style_rules}
"""


def build_structure_block(doc_spec: dict) -> str:
    """Формирует блок структуры документа из config['document_template']."""
    header = _tmpl["header"]
    approved_by = doc_spec.get("approved_by", header["approved_by"])
    date = doc_spec.get("approval_date", header["default_date"])

    lines = [
        f"[Гриф: {header['classification']}]",
        f"[Номер: {doc_spec['id']}]",
        f"[Утверждён: {approved_by}, {date}]",
        f"[Статус: {header['status']}]",
        f"[Срок пересмотра: {header['review_period']}]",
        "",
        "Структура документа:",
    ]
    lines += [f"  {section}" for section in _tmpl["sections"]]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Генерация документа
# ---------------------------------------------------------------------------

def generate_document(
    doc_spec: dict,
    *,
    run_log=None,
    op_id: str | None = None,
) -> str:
    """Вызывает LLM и возвращает текст документа."""
    prompt = f"""
Сгенерируй внутренний регламент:
Номер: {doc_spec['id']}
Название: {doc_spec['title']}
Ответственное подразделение: {doc_spec['responsible_dept']}

{build_reference_context(doc_spec)}

Документ должен раскрывать:
{chr(10).join(f'- {t}' for t in doc_spec['key_topics'])}

Включить таблицы:
{chr(10).join(f'- {t}' for t in doc_spec.get('tables', []))}

Целевой объём: ~{doc_spec['target_words']} слов.

{build_structure_block(doc_spec)}
"""
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
            log_llm_ok(run_log, op_id, doc_spec["id"], response, duration_ms,
                       attempt_num=attempt + 1)
            return response.choices[0].message.content
        except Exception as e:
            duration_ms = (time.monotonic() - t0) * 1000
            log_llm_error(run_log, op_id, doc_spec["id"], e, duration_ms,
                          attempt_num=attempt + 1)
            if attempt == 0:
                logging.warning("[RETRY] %s", e)
                time.sleep(5)
            else:
                raise


# ---------------------------------------------------------------------------
# Сохранение метаданных
# ---------------------------------------------------------------------------

def save_metadata(doc_spec: dict) -> None:
    meta = {
        "doc_id": doc_spec["id"],
        "title": doc_spec["title"],
        "layer": "internal",
        "doc_type": doc_spec["doc_type"],
        "doc_subtype": doc_spec["doc_subtype"],
        "cluster": doc_spec["cluster"],
        "approved_by": doc_spec.get("approved_by"),
        "approval_date": doc_spec.get("approval_date"),
        "last_revision_date": doc_spec.get("approval_date"),
        "responsible_dept": doc_spec.get("responsible_dept"),
        "references": doc_spec.get("references", []),
        "key_topics": doc_spec.get("key_topics", []),
        "source_url": None,
        "parsed_date": None,
        "target_words": doc_spec.get("target_words"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    meta_path = OUTPUT_DIR / f"{doc_spec['id']}_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Генерация синтетических банковских документов из config.yaml",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
примеры:
  python generating.py                          # пропустить уже сгенерированные
  python generating.py --force                  # перегенерировать всё
  python generating.py --only RG-KIB-001 RG-KIB-003
  python generating.py --changed changed.txt    # один doc_id на строку
  python generating.py --log-level DEBUG
        """,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--force",
        action="store_true",
        help="Перегенерировать все документы, даже если файлы уже существуют",
    )
    group.add_argument(
        "--only",
        nargs="+",
        metavar="DOC_ID",
        help="Сгенерировать только указанные document ID",
    )
    group.add_argument(
        "--changed",
        metavar="FILE",
        help="Путь к файлу со списком doc_id (по одному на строку, # — комментарий)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        dest="log_level",
        help="Уровень логирования (по умолчанию: INFO)",
    )
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def load_ids_from_file(path: str) -> set[str]:
    """Читает список doc_id из файла (пустые строки и # игнорируются)."""
    ids: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                ids.add(line)
    return ids


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    validation_errors = validate_pipeline_config(config, PARSED_DIR)
    if validation_errors:
        for error in validation_errors:
            logging.error("CONFIG: %s", error)
        sys.exit(1)

    if args.force:
        target_ids: set[str] | None = None
        force = True
    elif args.only:
        target_ids = set(args.only)
        force = True
    elif args.changed:
        target_ids = load_ids_from_file(args.changed)
        force = True
    else:
        target_ids = None
        force = False

    docs_by_id = {doc["id"]: doc for doc in config["documents"]}
    generation_order = config["pipeline"]["generation_order"]
    total = len(generation_order)

    docs_ok = 0
    docs_failed = 0
    docs_skipped = 0

    with RunLogger(args, script_name="generating") as run_log:
        for i, doc_id in enumerate(generation_order, 1):
            if doc_id not in docs_by_id:
                logging.info("[%d/%d] SKIP %s — нет в списке documents", i, total, doc_id)
                docs_skipped += 1
                continue

            if target_ids is not None and doc_id not in target_ids:
                logging.info("[%d/%d] SKIP %s — не в целевом списке", i, total, doc_id)
                docs_skipped += 1
                continue

            doc_spec = docs_by_id[doc_id]
            out_path = OUTPUT_DIR / f"{doc_id}.md"

            if not force and out_path.exists():
                logging.info("[%d/%d] SKIP %s — уже сгенерирован", i, total, doc_id)
                op_id = run_log.start_operation(doc_id, doc_spec)
                run_log.end_operation(op_id, status="skipped")
                docs_skipped += 1
                continue

            logging.info("[%d/%d] Generating %s: %s…", i, total, doc_id,
                         doc_spec["title"][:60])

            op_id = run_log.start_operation(doc_id, doc_spec)
            try:
                text = generate_document(doc_spec, run_log=run_log, op_id=op_id)
                out_path.write_text(text, encoding="utf-8")
                save_metadata(doc_spec)
                run_log.end_operation(
                    op_id,
                    status="ok",
                    output_path=str(out_path.relative_to(ROOT_DIR)),
                )
                logging.info("  [OK] %s", out_path.name)
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
    logging.info("Готово: %d/%d успешно. Output: %s", docs_ok, total, OUTPUT_DIR)
    if docs_failed:
        logging.warning("Не удалось: %d документов", docs_failed)


if __name__ == "__main__":
    main()
