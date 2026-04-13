"""Браузерный загрузчик документов с garant.ru через Playwright.

Зачем нужен:
  Garant показывает документы частично — остальное скрыто JS-пейволлом.
  requests не выполняет JS, поэтому пейволл всегда присутствует.
  Playwright запускает настоящий Chromium: JS выполняется, после авторизации
  кнопка "Открыть полностью" появляется, клик раскрывает весь текст.

  Не требует платной подписки на account.garant.ru — достаточно бесплатного
  аккаунта на base.garant.ru.

Использование:
  with GarantPlaywrightDownloader(login, password) as dl:
      raw_doc = dl.get_document("411701713")

  # Или без контекстного менеджера:
  dl = GarantPlaywrightDownloader(login, password)
  dl.start()
  raw_doc = dl.get_document("411701713")
  dl.stop()

Зависимости:
  pip install playwright
  playwright install chromium
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from extractors.base import RawDocument

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Исключения
# ---------------------------------------------------------------------------

class GarantPlaywrightError(RuntimeError):
    """Ошибка при работе с garant.ru через Playwright."""


class GarantPlaywrightAuthError(GarantPlaywrightError):
    """Ошибка авторизации на garant.ru."""


# ---------------------------------------------------------------------------
# Загрузчик
# ---------------------------------------------------------------------------

class GarantPlaywrightDownloader:
    """Браузерный загрузчик документов с base.garant.ru.

    Запускает Chromium, логинится на base.garant.ru, обходит JS-пейволл
    и возвращает полный текст документа. Браузер переиспользуется между
    документами — не нужно авторизовываться заново на каждый запрос.

    Args:
        login:    Email аккаунта на garant.ru (бесплатного достаточно)
        password: Пароль аккаунта
        headless: True = без GUI (по умолчанию), False = показывать браузер
        timeout:  Таймаут операций в миллисекундах (по умолчанию 30 сек)
    """

    _BASE_URL = "https://base.garant.ru"

    def __init__(
        self,
        login: str,
        password: str,
        headless: bool = True,
        timeout: int = 30_000,
    ) -> None:
        self._login = login
        self._password = password
        self._headless = headless
        self._timeout = timeout

        self._pw = None
        self._browser = None
        self._page = None
        self._logged_in = False

    # ------------------------------------------------------------------
    # Жизненный цикл
    # ------------------------------------------------------------------

    def start(self) -> "GarantPlaywrightDownloader":
        """Запускает Playwright и Chromium. Вызывается автоматически при __enter__."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise GarantPlaywrightError(
                "playwright не установлен. "
                "Установи: pip install playwright && playwright install chromium"
            ) from exc

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=self._headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
            viewport={"width": 1280, "height": 800},
        )
        self._page = context.new_page()
        # Убираем navigator.webdriver — признак автоматизации
        self._page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.debug("GarantPlaywright: браузер запущен (headless=%s)", self._headless)
        return self

    def stop(self) -> None:
        """Закрывает браузер и Playwright."""
        if self._page is not None:
            try:
                self._page.context.close()
            except Exception:
                pass
            self._page = None
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._pw:
            self._pw.stop()
            self._pw = None
        self._logged_in = False

    def __enter__(self) -> "GarantPlaywrightDownloader":
        return self.start()

    def __exit__(self, *args) -> None:
        self.stop()

    # ------------------------------------------------------------------
    # Публичный интерфейс
    # ------------------------------------------------------------------

    def get_document(self, garant_id: str) -> RawDocument:
        """Возвращает полный текст документа garant_id как RawDocument.

        Алгоритм:
          1. Логин (если ещё не залогинен)
          2. Переход на страницу документа
          3. Клик "Открыть полностью" если есть пейволл
          4. Попытка скачать ODT (если доступна кнопка скачивания)
          5. Иначе — извлечь полный HTML текст со страницы

        Args:
            garant_id: числовой ID документа на garant.ru

        Returns:
            RawDocument с полным текстом документа

        Raises:
            GarantPlaywrightAuthError: ошибка авторизации
            GarantPlaywrightError: прочие ошибки
        """
        if self._page is None:
            raise GarantPlaywrightError(
                "Downloader не запущен. Используй контекстный менеджер или вызови start()"
            )

        self._ensure_logged_in()

        doc_url = f"{self._BASE_URL}/{garant_id}/"
        logger.info("GarantPlaywright: открываем документ %s …", garant_id)
        self._page.goto(doc_url, wait_until="domcontentloaded", timeout=self._timeout)
        try:
            self._page.wait_for_load_state("networkidle", timeout=self._timeout)
        except Exception as exc:
            # Некоторые страницы Garant держат фоновые запросы открытыми и
            # никогда не достигают networkidle. В этом случае продолжаем
            # работу с уже загруженным DOM вместо ложного падения.
            logger.debug(
                "GarantPlaywright: networkidle не дождались для %s: %s",
                garant_id, exc,
            )

        # Убираем JS-пейволл если есть
        self._dismiss_paywall()

        # Сначала пробуем скачать ODT (быстрее парсится, сохраняет структуру)
        try:
            odt_bytes = self._try_download_odt()
            if odt_bytes:
                from extractors.odt import ODTExtractor
                logger.info(
                    "GarantPlaywright: ODT скачан (%d байт) для %s", len(odt_bytes), garant_id
                )
                return ODTExtractor().extract(odt_bytes, doc_url)
        except Exception as exc:
            logger.debug("GarantPlaywright: ODT недоступен для %s: %s", garant_id, exc)

        # Иначе — парсим HTML со страницы
        try:
            raw_doc = self._extract_html(doc_url)
            logger.info(
                "GarantPlaywright: HTML извлечён (%d символов) для %s",
                len(raw_doc.text), garant_id,
            )
            return raw_doc
        except Exception as exc:
            print_url = f"{doc_url.rstrip('/')}/print/"
            logger.debug(
                "GarantPlaywright: базовая страница не подошла для %s: %s. "
                "Пробуем print-версию %s",
                garant_id, exc, print_url,
            )
            self._page.goto(print_url, wait_until="domcontentloaded", timeout=self._timeout)
            try:
                self._page.wait_for_load_state("networkidle", timeout=self._timeout)
            except Exception as print_exc:
                logger.debug(
                    "GarantPlaywright: networkidle не дождались для print %s: %s",
                    garant_id, print_exc,
                )
            self._dismiss_paywall()
            raw_doc = self._extract_html(print_url)
            logger.info(
                "GarantPlaywright: HTML извлечён из print-версии (%d символов) для %s",
                len(raw_doc.text), garant_id,
            )
            return raw_doc

    # ------------------------------------------------------------------
    # Приватные методы
    # ------------------------------------------------------------------

    def _ensure_logged_in(self) -> None:
        """Логинится на base.garant.ru через AJAX-эндпоинт если ещё не залогинен.

        Garant загружает форму входа динамически через JS, поэтому надёжнее
        отправить AJAX-запрос напрямую через Playwright APIRequest — те же самые
        куки (.garant.ru) затем используются при открытии страниц документов.
        """
        if self._logged_in:
            return

        logger.info("GarantPlaywright: авторизуемся как %s …", self._login)

        # Шаг 1: посетить главную страницу, чтобы домен .garant.ru получил базовые куки
        self._page.goto(self._BASE_URL, wait_until="domcontentloaded", timeout=self._timeout)
        self._page.wait_for_timeout(1000)

        # Шаг 2: AJAX POST /ajax/login/ через Playwright context.request
        # Куки из браузерного контекста автоматически отправляются и принимаются
        try:
            resp = self._page.context.request.post(
                f"{self._BASE_URL}/ajax/login/",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Origin": self._BASE_URL,
                    "Referer": f"{self._BASE_URL}/",
                },
                form={
                    "form_data[login]": self._login,
                    "form_data[pass]": self._password,
                    "form_data[rem_me]": "0",
                },
            )
        except Exception as exc:
            raise GarantPlaywrightAuthError(
                f"Сетевая ошибка при AJAX-логине на garant.ru: {exc}"
            ) from exc

        body = resp.text()
        if not body.upper().startswith("OK"):
            try:
                import json as _json
                data = _json.loads(body)
                msg = data.get("message") or data.get("error") or body[:200]
            except Exception:
                msg = body[:200]
            raise GarantPlaywrightAuthError(
                f"Garant отклонил авторизацию: {msg!r}. "
                "Проверь GARANT_LOGIN и GARANT_PASSWORD в .env"
            )

        # Проверяем что garantId кука установлена
        cookies = self._page.context.cookies()
        if not any(c["name"] == "garantId" for c in cookies):
            raise GarantPlaywrightAuthError(
                "Логин вернул OK, но garantId кука не установлена. "
                "Структура авторизации garant.ru могла измениться."
            )

        self._logged_in = True
        logger.debug("GarantPlaywright: авторизован, garantId кука получена")

    def _dismiss_paywall(self) -> None:
        """Кликает 'Открыть документ полностью' если пейволл присутствует."""
        page = self._page
        _paywall_selectors = [
            ".freemium-paywall-button-open-document",
            ".js-open-document",
            "[class*='paywall'] button",
            "[class*='freemium'] button",
        ]
        for sel in _paywall_selectors:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    logger.debug("GarantPlaywright: кликаем пейволл: %s", sel)
                    btn.click()
                    page.wait_for_load_state("networkidle", timeout=self._timeout)
                    break
            except Exception:
                continue

    def _try_download_odt(self) -> bytes | None:
        """Ищет кнопку скачивания ODT и скачивает файл.

        Returns:
            Байты ODT-файла или None если кнопка не найдена.
        """
        page = self._page
        _odt_selectors = [
            "[href*='.odt']",
            "[data-format='odt']",
            "a[href*='d.garant.ru']",
            ".js-download-odt",
            "[title*='ODT']",
            "[title*='odt']",
        ]

        odt_btn = None
        for sel in _odt_selectors:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    odt_btn = el
                    logger.debug("GarantPlaywright: найдена кнопка ODT: %s", sel)
                    break
            except Exception:
                continue

        if odt_btn is None:
            return None

        # Перехватываем скачивание
        with tempfile.TemporaryDirectory() as tmpdir:
            page.context.set_default_timeout(60_000)
            try:
                with page.expect_download(timeout=60_000) as dl_info:
                    odt_btn.click()
                download = dl_info.value
                # Playwright сохраняет файл во временной директории
                tmp_path = Path(tmpdir) / "document.odt"
                download.save_as(str(tmp_path))
                return tmp_path.read_bytes()
            except Exception as exc:
                logger.debug("GarantPlaywright: ошибка при скачивании ODT: %s", exc)
                return None

    def _extract_html(self, doc_url: str) -> RawDocument:
        """Извлекает текст из текущей страницы через GarantExtractor."""
        page = self._page

        # Получаем HTML страницы как строку
        html_content = page.content()
        html_bytes = html_content.encode("utf-8")

        # Используем существующий GarantExtractor — он умеет находить
        # нужный контейнер и очищать текст на страницах garant.ru
        from extractors.html import GarantExtractor
        raw_doc = GarantExtractor().extract(html_bytes, doc_url)

        if len(raw_doc.text.strip()) < 500:
            raise GarantPlaywrightError(
                f"Слишком мало текста извлечено со страницы {doc_url} "
                f"({len(raw_doc.text)} символов). "
                "Возможно, авторизация не прошла или документ не открылся полностью."
            )

        return raw_doc
