"""Browser-based downloader for garant.ru documents via Playwright.

Why this exists:
  Garant often shows only a partial document in plain HTML and hides the rest
  behind JavaScript-driven UI/paywall elements. Simple HTTP requests do not run
  that JavaScript, so the full text is not always available.

  Playwright launches a real Chromium browser, performs login, can click the
  "open full document" controls, and can download the document through the
  built-in "save to file" flow on ivo.garant.ru.

Usage:
  with GarantPlaywrightDownloader(login, password) as dl:
      raw_doc = dl.get_document("411701713")

  dl = GarantPlaywrightDownloader(login, password)
  dl.start()
  raw_doc = dl.get_document("411701713")
  dl.stop()

Dependencies:
  pip install playwright
  playwright install chromium
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

from extractors.base import RawDocument

logger = logging.getLogger(__name__)


def _mask_login(login: str) -> str:
    """Hide most of the login in logs."""
    if "@" not in login:
        return "***"
    local_part, domain = login.split("@", 1)
    if len(local_part) <= 2:
        masked_local = "*" * len(local_part)
    else:
        masked_local = f"{local_part[:2]}***"
    return f"{masked_local}@{domain}"


class GarantPlaywrightError(RuntimeError):
    """Generic Playwright downloader error for garant.ru."""


class GarantPlaywrightAuthError(GarantPlaywrightError):
    """Authentication error for garant.ru."""


class GarantPlaywrightDownloader:
    """Browser-based downloader for documents from base.garant.ru.

    The downloader launches Chromium, logs into Garant, opens the base.garant
    document page, and tries to download the document through ivo.garant.ru
    using the "save to file" button. The browser instance is reused across
    documents.

    Download architecture discovered during diagnostics:
      base.garant.ru/{id}/
        -> a.save-to-file[href="http://ivo.garant.ru/#/document/{id}"]
            -> ivo.garant.ru ExtJS SPA
                -> a.viewFrameToolbarSaveToFile
                    -> direct download or ExtJS format menu

    Args:
        login: Garant account email.
        password: Garant account password.
        headless: Run Chromium without GUI.
        timeout: Operation timeout in milliseconds.
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

    def start(self) -> "GarantPlaywrightDownloader":
        """Start Playwright and Chromium."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise GarantPlaywrightError(
                "playwright is not installed. Install it with "
                "'pip install playwright' and then run "
                "'playwright install chromium'."
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
        self._page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.debug("GarantPlaywright: browser started (headless=%s)", self._headless)
        return self

    def stop(self) -> None:
        """Close browser and Playwright."""
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

    def get_document(self, garant_id: str) -> RawDocument:
        """Return the full document text for ``garant_id`` as ``RawDocument``.

        Flow:
          1. Ensure authenticated session.
          2. Open ``base.garant.ru/{garant_id}/``.
          3. Try to download the file through ivo.garant.ru.
          4. If download fails, try HTML extraction from the base page.
          5. If base HTML is too short, try the ``/print/`` page.
        """
        if self._page is None:
            raise GarantPlaywrightError(
                "Downloader is not started. Use a context manager or call start()."
            )

        self._ensure_logged_in()

        doc_url = f"{self._BASE_URL}/{garant_id}/"
        logger.info("GarantPlaywright: opening document %s", garant_id)
        self._page.goto(doc_url, wait_until="domcontentloaded", timeout=self._timeout)
        try:
            self._page.wait_for_load_state("networkidle", timeout=self._timeout)
        except Exception as exc:
            logger.debug(
                "GarantPlaywright: networkidle was not reached for %s: %s",
                garant_id,
                exc,
            )

        self._dismiss_paywall()
        doc_bytes = self._download_via_ivo(garant_id)
        if doc_bytes:
            raw_doc = self._extract_downloaded_document(doc_bytes, doc_url, garant_id)
            return raw_doc

        logger.info(
            "GarantPlaywright: file download failed for %s, falling back to HTML",
            garant_id,
        )
        self._page.goto(doc_url, wait_until="domcontentloaded", timeout=self._timeout)
        try:
            self._page.wait_for_load_state("networkidle", timeout=self._timeout)
        except Exception:
            pass
        self._dismiss_paywall()

        try:
            raw_doc = self._extract_html(doc_url)
            logger.info(
                "GarantPlaywright: extracted HTML (%d chars) for %s",
                len(raw_doc.text),
                garant_id,
            )
            return raw_doc
        except Exception as exc:
            print_url = f"{doc_url.rstrip('/')}/print/"
            logger.debug(
                "GarantPlaywright: base page HTML was not enough for %s: %s. "
                "Trying print view %s",
                garant_id,
                exc,
                print_url,
            )
            self._page.goto(print_url, wait_until="domcontentloaded", timeout=self._timeout)
            try:
                self._page.wait_for_load_state("networkidle", timeout=self._timeout)
            except Exception as print_exc:
                logger.debug(
                    "GarantPlaywright: networkidle was not reached for print %s: %s",
                    garant_id,
                    print_exc,
                )
            self._dismiss_paywall()
            raw_doc = self._extract_html(print_url)
            logger.info(
                "GarantPlaywright: extracted HTML from print view (%d chars) for %s",
                len(raw_doc.text),
                garant_id,
            )
            return raw_doc

    def _download_via_ivo(self, garant_id: str) -> bytes | None:
        """Open ivo.garant.ru and download the document via the save button."""
        ivo_url = self._get_ivo_url(garant_id)
        logger.info("GarantPlaywright: opening ivo page for %s: %s", garant_id, ivo_url)

        self._page.goto(ivo_url, wait_until="domcontentloaded", timeout=self._timeout)
        try:
            self._page.wait_for_selector(".viewFrameToolbarSaveToFile", timeout=25_000)
        except Exception as exc:
            logger.debug(
                "GarantPlaywright: save button did not appear for %s: %s",
                garant_id,
                exc,
            )
            return None

        logger.debug("GarantPlaywright: save button found for %s", garant_id)
        return self._click_save_button(garant_id)

    def _get_ivo_url(self, garant_id: str) -> str:
        """Get the ivo.garant.ru URL from ``a.save-to-file`` on the current page."""
        try:
            el = self._page.query_selector("a.save-to-file[href*='ivo.garant.ru']")
            if el:
                href = el.get_attribute("href")
                if href:
                    return href
        except Exception:
            pass
        return f"http://ivo.garant.ru/#/document/{garant_id}"

    def _click_save_button(self, garant_id: str) -> bytes | None:
        """Click the save button and capture the download if it starts."""
        page = self._page

        btn = page.query_selector(".viewFrameToolbarSaveToFile")
        if not btn or not btn.is_visible():
            logger.debug("GarantPlaywright: save button is not visible for %s", garant_id)
            return None

        _pending_downloads: list = []

        def _on_download(dl) -> None:
            _pending_downloads.append(dl)

        page.on("download", _on_download)
        try:
            data = self._download_via_save_button_keyboard(btn, garant_id, _pending_downloads)
            if data is not None:
                return data

            btn.click()
            page.wait_for_timeout(1000)

            if _pending_downloads:
                data = self._save_pending_download(_pending_downloads[0], garant_id)
                if data is not None:
                    logger.info(
                        "GarantPlaywright: file downloaded directly (%d bytes) for %s",
                        len(data),
                        garant_id,
                    )
                    return data

            logger.debug("GarantPlaywright: looking for ExtJS format menu for %s", garant_id)
            data = self._click_menu_format_item(garant_id)
            if data is not None:
                return data

            if not _pending_downloads:
                try:
                    page.wait_for_event("download", timeout=5_000)
                except Exception:
                    pass
            if _pending_downloads:
                data = self._save_pending_download(_pending_downloads[0], garant_id)
                if data is not None:
                    return data
        finally:
            page.remove_listener("download", _on_download)

        logger.debug("GarantPlaywright: file was not downloaded for %s", garant_id)
        return None

    def _download_via_save_button_keyboard(
        self, btn, garant_id: str, pending_downloads: list
    ) -> bytes | None:
        """Try keyboard interaction with the ExtJS selectButton."""
        page = self._page
        try:
            btn.focus()
            page.wait_for_timeout(150)
            page.keyboard.press("Enter")
            page.wait_for_timeout(250)
            page.keyboard.press("ArrowDown")
            page.wait_for_timeout(150)

            with page.expect_download(timeout=8_000) as dl_info:
                page.keyboard.press("Enter")
            data = self._save_pending_download(dl_info.value, garant_id)
            if data is not None:
                logger.info(
                    "GarantPlaywright: file downloaded via keyboard selectButton "
                    "(%d bytes) for %s",
                    len(data),
                    garant_id,
                )
                return data
        except Exception as exc:
            logger.debug(
                "GarantPlaywright: keyboard strategy did not work for %s: %s",
                garant_id,
                exc,
            )

        if pending_downloads:
            data = self._save_pending_download(pending_downloads[0], garant_id)
            if data is not None:
                return data
        return None

    def _click_menu_format_item(self, garant_id: str) -> bytes | None:
        """Click the first active save item in the ExtJS menu."""
        page = self._page

        active_item_sel = ".widgetSelectButtonMenu .x-menu-item:not(.x-menu-item-disabled)"
        try:
            page.wait_for_selector(active_item_sel, state="attached", timeout=5_000)
        except Exception as exc:
            logger.debug("GarantPlaywright: menu did not appear for %s: %s", garant_id, exc)
            return self._click_extjs_save_menu_item(garant_id)

        save_text_loc = page.locator(
            ".widgetSelectButtonMenu .x-menu-item-text"
        ).filter(has_text="Сохранить")
        item = save_text_loc.locator(
            "xpath=ancestor::div[contains(@class, 'x-menu-item')][1]"
        ).first
        if item.count() == 0:
            logger.debug(
                "GarantPlaywright: save menu item not found in DOM for %s, "
                "trying ExtJS API",
                garant_id,
            )
            return self._click_extjs_save_menu_item(garant_id)

        label = (item.inner_text() or "").strip()[:60]
        logger.debug("GarantPlaywright: clicking menu item %r for %s", label, garant_id)
        data = self._click_and_capture_download(item, timeout=30_000, force=True)
        if data is not None:
            logger.info(
                "GarantPlaywright: ODT downloaded from menu (%d bytes) for %s",
                len(data),
                garant_id,
            )
            return data

        logger.debug(
            "GarantPlaywright: DOM click on save menu did not work for %s, "
            "trying ExtJS API",
            garant_id,
        )
        return self._click_extjs_save_menu_item(garant_id)

    def _click_extjs_save_menu_item(self, garant_id: str) -> bytes | None:
        """Try to trigger the save menu item through ExtJS API."""
        page = self._page

        try:
            with page.expect_download(timeout=15_000) as dl_info:
                triggered = page.evaluate(
                    """() => {
                        const Ext = window.Ext;
                        if (!Ext || !Ext.ComponentQuery) return null;

                        const menus = Ext.ComponentQuery.query('menu');
                        const menu = menus.find(m => {
                            const texts = (m.items?.items || []).map(
                                it => (it.text || it.ariaLabel || it.itemId || '')
                            );
                            return texts.some(t => typeof t === 'string' && t.includes('Сохранить'));
                        });
                        if (!menu) return null;

                        if (typeof menu.show === 'function' && !(menu.isVisible && menu.isVisible())) {
                            try { menu.show(); } catch (e) {}
                        }

                        const item = (menu.items?.items || []).find(it => {
                            const label = it.text || it.ariaLabel || it.itemId || '';
                            return !it.disabled && typeof label === 'string' && label.includes('Сохранить');
                        });
                        if (!item) return null;

                        if (typeof item.focus === 'function') {
                            try { item.focus(); } catch (e) {}
                        }
                        if (typeof item.handler === 'function') {
                            item.handler.call(item.scope || item, item, null);
                            return item.text || item.ariaLabel || item.itemId || 'handler';
                        }
                        if (typeof item.onClick === 'function') {
                            item.onClick(null);
                            return item.text || item.ariaLabel || item.itemId || 'onClick';
                        }
                        if (typeof item.fireEvent === 'function') {
                            item.fireEvent('click', item, null);
                            return item.text || item.ariaLabel || item.itemId || 'fireEvent';
                        }
                        const el = item.getEl?.()?.dom || item.el?.dom || null;
                        if (el) {
                            el.click();
                            return item.text || item.ariaLabel || item.itemId || 'dom-click';
                        }
                        return null;
                    }"""
                )
                logger.debug(
                    "GarantPlaywright: ExtJS API trigger result for %s: %r",
                    garant_id,
                    triggered,
                )
            data = self._save_pending_download(dl_info.value, garant_id)
            if data is not None:
                logger.info(
                    "GarantPlaywright: file downloaded through ExtJS API (%d bytes) "
                    "for %s",
                    len(data),
                    garant_id,
                )
                return data
        except Exception as exc:
            logger.debug(
                "GarantPlaywright: ExtJS API did not work for %s: %s",
                garant_id,
                exc,
            )
        return None

    def _save_pending_download(self, download, garant_id: str) -> bytes | None:
        """Save a started Playwright download into a temp file and return bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                filename = download.suggested_filename or "document"
                tmp_path = Path(tmpdir) / filename
                download.save_as(str(tmp_path))
                data = tmp_path.read_bytes()
                if data:
                    logger.debug(
                        "GarantPlaywright: saved download %r (%d bytes) for %s",
                        filename,
                        len(data),
                        garant_id,
                    )
                    return data
            except Exception as exc:
                logger.debug("GarantPlaywright: failed to save download: %s", exc)
        return None

    def _click_and_capture_download(
        self, element, *, timeout: int, force: bool = False
    ) -> bytes | None:
        """Click an element and return bytes of the downloaded file if any."""
        click_coords: tuple[float, float] | None = None
        if force:
            try:
                element.scroll_into_view_if_needed(timeout=2_000)
            except Exception:
                pass
            bb = element.bounding_box()
            if bb:
                click_coords = (bb["x"] + bb["width"] / 2, bb["y"] + bb["height"] / 2)
                logger.debug(
                    "GarantPlaywright: bounding_box found (%.0f, %.0f), using mouse.click",
                    *click_coords,
                )
            else:
                logger.debug("GarantPlaywright: bounding_box=None, fallback to el.click()")

        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                with self._page.expect_download(timeout=timeout) as dl_info:
                    if force:
                        if click_coords:
                            self._page.mouse.click(*click_coords)
                        else:
                            element.evaluate("el => el.click()")
                    else:
                        element.click()
                download = dl_info.value
                filename = download.suggested_filename or "document"
                tmp_path = Path(tmpdir) / filename
                download.save_as(str(tmp_path))
                data = tmp_path.read_bytes()
                if data:
                    logger.debug(
                        "GarantPlaywright: download %r (%d bytes)", filename, len(data)
                    )
                    return data
            except Exception as exc:
                logger.debug("GarantPlaywright: download was not captured: %s", exc)
        return None

    def _extract_downloaded_document(
        self, data: bytes, doc_url: str, garant_id: str
    ) -> RawDocument:
        """Detect downloaded file format and extract document text."""
        magic = data[:4]

        if magic[:2] == b"PK":
            from extractors.odt import ODTExtractor

            logger.info(
                "GarantPlaywright: ODT format detected, extracting text (%d bytes) for %s",
                len(data),
                garant_id,
            )
            return ODTExtractor().extract(data, doc_url)

        if magic == b"%PDF":
            from extractors.pdf import PyMuPDFExtractor

            logger.info(
                "GarantPlaywright: PDF format detected, extracting text (%d bytes) for %s",
                len(data),
                garant_id,
            )
            return PyMuPDFExtractor().extract(data, doc_url)

        rtf_magic = b"{\\rtf"
        if data[:5] == rtf_magic or data[:5] == rtf_magic.decode().encode("utf-8")[:5]:
            logger.info(
                "GarantPlaywright: RTF format detected, extracting text (%d bytes) for %s",
                len(data),
                garant_id,
            )
            return self._extract_rtf(data, doc_url)

        raise GarantPlaywrightError(
            f"Unknown downloaded file format for {garant_id}. "
            f"First bytes: {data[:8].hex()!r}. "
            "Expected ODT (PK), PDF (%PDF), or RTF ({\\rtf)."
        )

    @staticmethod
    def _extract_rtf(data: bytes, doc_url: str) -> RawDocument:
        """Extract plain text from RTF using striprtf or a regex fallback."""
        for enc in ("cp1251", "utf-8", "latin-1"):
            try:
                text_raw = data.decode(enc, errors="replace")
                break
            except Exception:
                continue
        else:
            text_raw = data.decode("latin-1", errors="replace")

        try:
            from striprtf.striprtf import rtf_to_text

            text = rtf_to_text(text_raw)
        except ImportError:
            import re

            text = re.sub(r"\\[a-z]+[-]?\d*[ ]?", " ", text_raw)
            text = re.sub(r"[{}\\]", "", text)
            text = re.sub(r"\s+", " ", text)
            text = text.strip()

        return RawDocument(text=text, source_url=doc_url, is_odt=False)

    def _ensure_logged_in(self) -> None:
        """Authenticate against garant.ru via the AJAX login endpoint."""
        if self._logged_in:
            return

        logger.info("GarantPlaywright: signing in as %s", _mask_login(self._login))

        self._page.goto(self._BASE_URL, wait_until="domcontentloaded", timeout=self._timeout)
        self._page.wait_for_timeout(1000)

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
                f"Network error during AJAX login to garant.ru: {exc}"
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
                f"Garant rejected authentication: {msg!r}. "
                "Check GARANT_LOGIN and GARANT_PASSWORD in .env."
            )

        cookies = self._page.context.cookies()
        cookie_names = {c["name"] for c in cookies}
        auth_cookie_variants = [
            {"garantId"},
            {"g_user_sid", "g_username"},
            {"g_user_sid"},
        ]
        if not any(variant.issubset(cookie_names) for variant in auth_cookie_variants):
            raise GarantPlaywrightAuthError(
                "Login returned OK, but no known auth cookies were set "
                f"(got: {sorted(cookie_names)!r}). The garant.ru auth flow may have changed."
            )

        self._logged_in = True
        logger.debug(
            "GarantPlaywright: authenticated successfully, auth cookies found: %s",
            ", ".join(sorted(name for name in cookie_names if name in {"garantId", "g_user_sid", "g_username"})),
        )

    def _dismiss_paywall(self) -> None:
        """Click the 'open full document' control if it is present."""
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
                    logger.debug("GarantPlaywright: clicking paywall control %s", sel)
                    btn.click()
                    page.wait_for_load_state("networkidle", timeout=self._timeout)
                    break
            except Exception:
                continue

    def _extract_html(self, doc_url: str) -> RawDocument:
        """Extract text from the current page using ``GarantExtractor``."""
        html_bytes = self._page.content().encode("utf-8")
        from extractors.html import GarantExtractor

        raw_doc = GarantExtractor().extract(html_bytes, doc_url)
        if len(raw_doc.text.strip()) < 500:
            raise GarantPlaywrightError(
                f"Too little text was extracted from {doc_url} "
                f"({len(raw_doc.text)} chars). "
                "Authentication may have failed or the document was not fully opened."
            )
        return raw_doc
