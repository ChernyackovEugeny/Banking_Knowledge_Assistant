"""Parser for Russian federal laws split by articles."""
from __future__ import annotations

import logging
import re

from extractors.base import RawDocument
from parsers.base import AbstractSectionParser, Section
from tables import mask_tables

logger = logging.getLogger(__name__)

_ARTICLE_RE = re.compile(
    r"(?m)^\s*(?:СТАТЬЯ|Статья)\s+"
    r"(\d+(?:\.\d+)*(?:-\d+)*)"
    r"\s*\.?\s*"
    r"(.*?)$"
)

_SERVICE_MARKERS = (
    "измен",
    "дополнен",
    "новая редакц",
    "см. предыдущую",
    "см. будущую",
    "утратил",
)

_HEADING_SERVICE_MARKERS = _SERVICE_MARKERS + (
    "оглавлен",
    "содержан",
)


def _normalize_article_id(num: str) -> str:
    return f"ст.{num}"


def _section_priority(title: str, body: str) -> tuple[int, int, int]:
    """Prefer substantive article bodies over service fragments."""
    title_cf = title.casefold()
    body_cf = body.casefold()
    looks_service = any(marker in title_cf or marker in body_cf for marker in _SERVICE_MARKERS)
    return (
        int(not looks_service),
        int(bool(body.strip())),
        len(body.strip()),
    )


def _looks_like_service_fragment(text: str) -> bool:
    text_cf = text.casefold()
    return any(marker in text_cf for marker in _HEADING_SERVICE_MARKERS)


def _looks_like_toc_fragment(body: str) -> bool:
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    if not lines:
        return True
    if len(lines) > 5:
        return False

    article_like = 0
    for line in lines:
        if _ARTICLE_RE.match(line):
            article_like += 1
    return article_like >= max(2, len(lines) - 1)


def _is_valid_article_candidate(title: str, body: str) -> bool:
    if _looks_like_service_fragment(title):
        return False
    if _looks_like_toc_fragment(body):
        return False

    body_cf = body.casefold()
    if body_cf.startswith("оглавление") or body_cf.startswith("содержание"):
        return False
    return True


def _dedupe_sections(sections: list[Section]) -> list[Section]:
    deduped: list[Section] = []
    by_id: dict[str, int] = {}
    duplicate_stats: dict[str, dict[str, int]] = {}

    for sec in sections:
        existing_index = by_id.get(sec.id)
        if existing_index is None:
            by_id[sec.id] = len(deduped)
            deduped.append(sec)
            continue

        current = deduped[existing_index]
        stats = duplicate_stats.setdefault(sec.id, {"replaced": 0, "skipped": 0})
        if _section_priority(sec.title, sec.text) > _section_priority(current.title, current.text):
            stats["replaced"] += 1
            deduped[existing_index] = sec
        else:
            stats["skipped"] += 1

    for section_id, stats in sorted(duplicate_stats.items()):
        logger.warning(
            "FederalLawParser: duplicate section_id %s filtered (replaced=%d, skipped=%d)",
            section_id,
            stats["replaced"],
            stats["skipped"],
        )

    return deduped


class FederalLawParser(AbstractSectionParser):
    """Split a federal law into article-level sections."""

    def parse(self, raw_doc: RawDocument) -> list[Section]:
        text = raw_doc.text
        masked_text = mask_tables(text)
        matches = list(_ARTICLE_RE.finditer(masked_text))

        if not matches:
            logger.warning(
                "FederalLawParser: articles not found in document %s (%d chars)",
                raw_doc.source_url,
                len(text),
            )
            return []

        sections: list[Section] = []
        for i, match in enumerate(matches):
            article_num = match.group(1).strip()
            article_title = match.group(2).strip()

            body_start = match.end()
            body_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            body = text[body_start:body_end].strip()

            if not _is_valid_article_candidate(article_title, body):
                logger.debug(
                    "FederalLawParser: skipped false positive article %s (%r)",
                    article_num,
                    article_title[:80],
                )
                continue

            sections.append(
                Section(
                    id=_normalize_article_id(article_num),
                    title=article_title,
                    text=body,
                )
            )

        sections = _dedupe_sections(sections)
        logger.info(
            "FederalLawParser: extracted %d articles from %s",
            len(sections),
            raw_doc.source_url,
        )
        return sections
