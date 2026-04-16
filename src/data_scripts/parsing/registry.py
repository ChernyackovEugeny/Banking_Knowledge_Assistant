"""Реестр источников для каждого документа из config.yaml real_documents[].

Структура:
  SOURCES[doc_id] = [SourceSpec(url, ExtractorClass), ...]

Источники перечислены в порядке приоритета. Пайплайн пробует их по очереди
и останавливается на первом успешном.

Как обновлять:
  1. Если URL устарел — замени его, не трогая структуру.
  2. Если новый источник надёжнее — добавь его первым.
  3. Если документ стал доступен только в PDF — используй CBRExtractor
     (он авто-определяет PDF и делегирует PyMuPDFExtractor).

Статус проверки URL (апрель 2026):
  ✓ base.garant.ru — работает для ФЗ и для документов ЦБ
  ✓ garant.ru/products/ipo/prime/doc/ — полный текст (ПРАЙМ), работает
  ✓ consultant.ru — JS-рендеринг; ConsultantExtractor может
    упасть с ошибкой «контейнер не найден» → тогда нужен ручной PDF
  ✓ legalacts.ru — работает для документов ЦБ (полный текст без пэйволла)
  ✓ normativ.kontur.ru — работает для документов ЦБ (полный текст)
  ✓ docs.cntd.ru — работает для некоторых документов ЦБ

Для документов ЦБ онлайн-источники найдены на garant.ru (ПРАЙМ),
legalacts.ru и normativ.kontur.ru. При неудаче → ручной PDF в
data/manual_pdfs/{doc_id}.pdf

Парсер для каждого doc_id выбирается в parsing.py по doc_subtype из config.yaml.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Type

from extractors.base import AbstractExtractor
from extractors.html import (
    CBRExtractor,
    CNTDExtractor,
    ConsultantExtractor,
    GarantExtractor,
    KonturExtractor,
    PravoGovExtractor,
)
from extractors.pdf import PyMuPDFExtractor


@dataclass(frozen=True)
class SourceSpec:
    url: str
    extractor_cls: Type[AbstractExtractor]


# =============================================================================
# Федеральные законы
# =============================================================================
# Статус (апрель 2026):
#   base.garant.ru — работает для 115-ФЗ, 395-1-ФЗ, 173-ФЗ
#   consultant.ru — JS-рендеринг; может не отдать текст → fallback
# =============================================================================

SOURCES: dict[str, list[SourceSpec]] = {

    # -------------------------------------------------------------------------
    # 115-ФЗ — О противодействии легализации доходов
    # base.garant.ru/12123862/ — работает (текст за пэйволлом, но
    #   GarantExtractor вытащит что есть; fallback на consultant)
    # -------------------------------------------------------------------------
    "115-FZ": [
        SourceSpec(
            "https://base.garant.ru/12123862/",
            GarantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 39-ФЗ — О рынке ценных бумаг
    # base.garant.ru/10106807/ — ФЗ от 22.04.1996 № 39-ФЗ; нужен для Playwright ID
    # consultant.ru — JS-рендеринг, может не отдать текст
    # NB: без подписки garant возвращает только оглавление (~5–6 кБ).
    #     При неудаче: data/manual_pdfs/39-FZ.odt (скачать авторизованным ODT с garant.ru)
    # -------------------------------------------------------------------------
    "39-FZ": [
        SourceSpec(
            "https://normativ.kontur.ru/document?documentId=505599&moduleId=1",
            KonturExtractor,
        ),
        SourceSpec(
            "https://base.garant.ru/10106807/",
            GarantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 152-ФЗ — О персональных данных
    # base.garant.ru/12148567/ — ФЗ от 27.07.2006 № 152-ФЗ, полный текст
    # consultant.ru — JS-рендеринг, может не отдать текст → fallback
    # -------------------------------------------------------------------------
    "152-FZ": [
        SourceSpec(
            "https://base.garant.ru/12148567/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/12048567/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_61801/",
            ConsultantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 395-1-ФЗ — О банках и банковской деятельности
    # -------------------------------------------------------------------------
    "395-1-FZ": [
        SourceSpec(
            "https://base.garant.ru/10105800/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_5842/",
            ConsultantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 173-ФЗ — О валютном регулировании и контроле
    # -------------------------------------------------------------------------
    "173-FZ": [
        SourceSpec(
            "https://base.garant.ru/12133556/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_45458/",
            ConsultantExtractor,
        ),
    ],

    # =========================================================================
    # Документы Банка России
    # =========================================================================
    # Актуальные источники (апрель 2026):
    #   - base.garant.ru — полный текст для большинства документов
    #   - garant.ru/products/ipo/prime/doc/ — полный текст (лента ПРАЙМ)
    #   - consultant.ru — JS-рендеринг, не всегда работает
    #   - legalacts.ru — полный текст без пэйволла
    #   - normativ.kontur.ru — полный текст
    #   - docs.cntd.ru — полный текст (старые редакции)
    # При неудаче всех источников → ручной PDF:
    #   data/manual_pdfs/{doc_id}.pdf
    # =========================================================================

    # -------------------------------------------------------------------------
    # 499-П — Об идентификации клиентов (2015, ред. 2024)
    # base.garant.ru (71255014) — нужен для Playwright ID; HTTP: TOC без подписки
    # consultant.ru — JS-рендеринг → обычно заблокирован
    # NB: документ за пэйволлом garant, Playwright не скачивает ODT для него.
    #     При неудаче: data/manual_pdfs/499-P.odt (скачать авторизованным ODT с garant.ru)
    # -------------------------------------------------------------------------
    "499-P": [
        SourceSpec(
            "https://normativ.kontur.ru/document?documentId=449792&moduleId=1",
            KonturExtractor,
        ),
        SourceSpec(
            "https://base.garant.ru/71255014/",
            GarantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 860-П — О требованиях к ПВК в целях ПОД/ФТ/ЭД (2025, заменило 375-П)
    # Опубликован 13.08.2025. Полный текст на garant и legalacts.
    # -------------------------------------------------------------------------
    "860-P": [
        SourceSpec(
            "https://base.garant.ru/412480152/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/412380152/",
            GarantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 7081-У — О порядке представления сведений в Росфинмониторинг (2025)
    # Зарег. 16.07.2025. Полный текст на base.garant.
    # -------------------------------------------------------------------------
    "7081-U": [
        SourceSpec(
            "https://base.garant.ru/412383024/",
            GarantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 851-П — О защите информации при банковских операциях (2025, заменило 683-П)
    # Зарег. 06.03.2025. Полный текст на base.garant и normativ.kontur.
    # -------------------------------------------------------------------------
    "851-P": [
        SourceSpec(
            "https://base.garant.ru/411701713/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/411601713/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_501055/",
            ConsultantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 590-П — О порядке формирования резервов по ссудам (2017, ред. 2023)
    # Полный текст на base.garant, ПРАЙМ, consultant, docs.cntd.
    # -------------------------------------------------------------------------
    "590-P": [
        SourceSpec(
            "https://base.garant.ru/71721612/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/71621612/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_220089/",
            ConsultantExtractor,
        ),
        SourceSpec(
            "http://docs.cntd.ru/document/456079148",
            CNTDExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 611-П — О порядке формирования резервов на возможные потери (2017)
    # ПРАЙМ (71801656) — полный текст с пунктами; ставим первым для HTTP-цепочки
    # base.garant.ru (71901656) — частичный текст (только главы); нужен для Playwright ID
    # consultant.ru — JS-рендеринг → обычно заблокирован
    # docs.cntd.ru — резервный, timeout бывает
    # NB: Playwright извлекает ID только из base.garant.ru, поэтому порядок
    #     источников не влияет на Playwright — он всегда использует 71901656.
    # -------------------------------------------------------------------------
    "611-P": [
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/71801656/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://base.garant.ru/71901656/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_293612/",
            ConsultantExtractor,
        ),
        SourceSpec(
            "http://docs.cntd.ru/document/542611725",
            CNTDExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 220-И — Об обязательных нормативах (2025, заменила 199-И)
    # Зарег. 11.07.2025, вступила 18.08.2025. Полный текст на base.garant.
    # -------------------------------------------------------------------------
    "220-I": [
        SourceSpec(
            "https://base.garant.ru/412342996/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/412242996/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_510143/",
            ConsultantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 716-П — О требованиях к СУОР (2020, ред. 2024)
    # Полный текст на base.garant, ПРАЙМ, consultant.
    # -------------------------------------------------------------------------
    "716-P": [
        SourceSpec(
            "https://base.garant.ru/74279372/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/74179372/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_355380/",
            ConsultantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 6681-У — О требованиях к брокерской деятельности (2024)
    # Зарег. 02.07.2024, вступило 01.04.2025. Полный текст на base.garant.
    # -------------------------------------------------------------------------
    "6681-U": [
        SourceSpec(
            "https://base.garant.ru/409352026/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/409252026/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_480576/",
            ConsultantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 3624-У — О требованиях к системе управления рисками и капиталом (2015)
    # Полный текст на base.garant, normativ.kontur, docs.cntd.
    # -------------------------------------------------------------------------
    "3624-U": [
        SourceSpec(
            "https://base.garant.ru/71057396/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.garant.ru/products/ipo/prime/doc/70957396/",
            GarantExtractor,
        ),
        SourceSpec(
            "http://docs.cntd.ru/document/420277295",
            CNTDExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 6406-У — О формах отчётности кредитных организаций в ЦБ (2023)
    # Полный текст на base.garant и consultant.
    # -------------------------------------------------------------------------
    "6406-U": [
        SourceSpec(
            "https://base.garant.ru/406750235/",
            GarantExtractor,
        ),
        SourceSpec(
            "https://www.consultant.ru/document/cons_doc_LAW_444612/",
            ConsultantExtractor,
        ),
    ],

    # -------------------------------------------------------------------------
    # 579-П — О Плане счетов бухгалтерского учёта (2017)
    # consultant.ru — JS-рендеринг, обычно заблокирован
    # base.garant.ru (71645626) — нужен для Playwright ID; HTTP: TOC без подписки
    # NB: очень большой документ с таблицами. Playwright не скачивает ODT.
    #     При неудаче: data/manual_pdfs/579-P.odt (скачать авторизованным ODT с garant.ru)
    # -------------------------------------------------------------------------
    "579-P": [
        SourceSpec(
            "https://normativ.kontur.ru/document?documentId=430142&moduleId=1",
            KonturExtractor,
        ),
        SourceSpec(
            "https://base.garant.ru/71645626/",
            GarantExtractor,
        ),
    ],
}
