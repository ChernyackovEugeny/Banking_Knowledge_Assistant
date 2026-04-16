"""Диагностический скрипт: находит селектор кнопки сохранения на ivo.garant.ru.

Запуск (из папки parsing/):
    python diagnose_garant.py

Что делает:
  1. Логинится на base.garant.ru
  2. Открывает страницу документа 115-FZ
  3. Переходит на ivo.garant.ru (как при клике "Сохранить в файл")
  4. Ждёт рендера SPA, делает скриншот
  5. Дампит HTML тулбара и все кнопки/ссылки на странице
  6. Пробует interceptить download при клике на кнопку сохранения
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

ROOT_DIR = _THIS_DIR.parents[2]

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT_DIR / ".env", override=False)
except ImportError:
    pass

GARANT_LOGIN    = os.environ["GARANT_LOGIN"]
GARANT_PASSWORD = os.environ["GARANT_PASSWORD"]

GARANT_ID = "12123862"  # 115-FZ
BASE_URL  = "https://base.garant.ru"
IVO_URL   = f"http://ivo.garant.ru/#/document/{GARANT_ID}"

SCREENSHOTS_DIR = ROOT_DIR / "data" / "diagnose_garant"
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)


def _dump_buttons(page, label: str) -> None:
    """Выводит все интерактивные элементы на текущей странице."""
    print(f"\n{'='*60}")
    print(f"[{label}] URL: {page.url}")
    print(f"{'='*60}")

    # Все кнопки
    buttons = page.query_selector_all("button, a, [role='button'], [onclick]")
    print(f"\n--- Все кнопки/ссылки ({len(buttons)} шт.) ---")
    for el in buttons[:60]:  # первые 60
        try:
            tag      = el.evaluate("e => e.tagName.toLowerCase()")
            text     = (el.inner_text() or "").strip()[:60]
            title    = el.get_attribute("title") or ""
            aria     = el.get_attribute("aria-label") or ""
            cls      = el.get_attribute("class") or ""
            href     = el.get_attribute("href") or ""
            data_tip = el.get_attribute("data-tooltip") or el.get_attribute("data-tip") or ""
            src      = ""
            # Ищем img внутри элемента
            img = el.query_selector("img")
            if img:
                src = img.get_attribute("src") or ""
            print(
                f"  <{tag}> text={text!r:30} title={title!r:30} "
                f"aria={aria!r:20} class={cls[:40]!r} href={href[:40]!r} "
                f"img_src={src[:40]!r} data-tip={data_tip!r}"
            )
        except Exception as e:
            print(f"  [err reading element: {e}]")

    # Все img с атрибутами (могут быть иконки без обёрток)
    imgs = page.query_selector_all("img[src*='save'], img[src*='Save'], img[title*='сохранить'], img[alt*='сохранить']")
    if imgs:
        print(f"\n--- img с 'save' в src/title/alt ({len(imgs)} шт.) ---")
        for img in imgs:
            try:
                src   = img.get_attribute("src") or ""
                alt   = img.get_attribute("alt") or ""
                title = img.get_attribute("title") or ""
                print(f"  src={src!r} alt={alt!r} title={title!r}")
            except Exception:
                pass


def _dump_html_fragment(page, label: str) -> None:
    """Дампит часть HTML страницы с тулбаром."""
    print(f"\n--- HTML тулбара / toolbar [{label}] ---")
    for sel in [
        "toolbar", ".toolbar", "[class*='toolbar']",
        "nav", ".document-toolbar", "#toolbar",
        "[class*='panel']", "[class*='header']",
        "[class*='actions']", "[class*='controls']",
    ]:
        try:
            el = page.query_selector(sel)
            if el:
                html = el.inner_html()
                print(f"  Selector {sel!r} → {len(html)} chars:")
                print("  " + html[:800].replace("\n", "\n  "))
                print()
        except Exception:
            pass


def run_diagnostics() -> None:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,  # показываем браузер чтобы видеть что происходит
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
            viewport={"width": 1440, "height": 900},
        )
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # ── 1. Авторизация ────────────────────────────────────────────────
        print("1. Открываем base.garant.ru …")
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(1000)

        resp = ctx.request.post(
            f"{BASE_URL}/ajax/login/",
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{BASE_URL}/",
            },
            form={
                "form_data[login]":  GARANT_LOGIN,
                "form_data[pass]":   GARANT_PASSWORD,
                "form_data[rem_me]": "0",
            },
        )
        print(f"   Login response: {resp.text()[:80]!r}")
        cookies = ctx.cookies()
        garant_id_cookie = next((c for c in cookies if c["name"] == "garantId"), None)
        print(f"   garantId cookie: {garant_id_cookie is not None}")

        # ── 2. Открываем документ на base.garant.ru ───────────────────────
        doc_url = f"{BASE_URL}/{GARANT_ID}/"
        print(f"\n2. Открываем документ: {doc_url}")
        page.goto(doc_url, wait_until="domcontentloaded", timeout=30_000)
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass

        ss_path = str(SCREENSHOTS_DIR / "01_base_garant.png")
        page.screenshot(path=ss_path, full_page=False)
        print(f"   Скриншот: {ss_path}")

        _dump_buttons(page, "base.garant.ru")

        # Ищем ссылки на ivo.garant.ru
        print("\n--- Ссылки на ivo/d.garant ---")
        hrefs = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => ({href: e.href, title: e.title, text: e.innerText.trim().slice(0,50)}))"
        )
        for h in hrefs:
            if "ivo.garant" in h["href"] or "d.garant" in h["href"]:
                print(f"  href={h['href']!r} title={h['title']!r} text={h['text']!r}")

        # ── 3. Переходим на ivo.garant.ru ─────────────────────────────────
        print(f"\n3. Переходим на ivo.garant.ru: {IVO_URL}")
        page.goto(IVO_URL, wait_until="domcontentloaded", timeout=30_000)

        # SPA: ждём networkidle + дополнительное время на рендер
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        page.wait_for_timeout(4000)

        ss_path = str(SCREENSHOTS_DIR / "02_ivo_after_load.png")
        page.screenshot(path=ss_path, full_page=False)
        print(f"   Скриншот: {ss_path}")

        _dump_buttons(page, "ivo.garant.ru (после 4с)")
        _dump_html_fragment(page, "ivo.garant.ru")

        # Дампим первые 3000 символов body HTML для поиска паттернов
        print("\n--- body innerHTML (первые 3000 символов) ---")
        body_html = page.eval_on_selector("body", "el => el.innerHTML") or ""
        print(body_html[:3000])

        # ── 4. Пробуем найти кнопку save по разным селекторам ─────────────
        print("\n4. Пробуем селекторы кнопки сохранения …")
        save_selectors = [
            '[title*="Сохранить"]',
            '[title*="сохранить"]',
            '[title*="Save"]',
            '[aria-label*="Сохранить"]',
            '[aria-label*="сохранить"]',
            'img[src*="save"]',
            'img[src*="Save"]',
            '[class*="save"]',
            '[class*="Save"]',
            '[data-tooltip*="Сохранить"]',
            '[data-title*="Сохранить"]',
            'button:first-of-type',
        ]
        for sel in save_selectors:
            try:
                els = page.query_selector_all(sel)
                if els:
                    print(f"  НАЙДЕНО [{sel}]: {len(els)} элементов")
                    for el in els[:3]:
                        try:
                            tag   = el.evaluate("e => e.tagName")
                            text  = (el.inner_text() or "").strip()[:40]
                            title = el.get_attribute("title") or ""
                            cls   = el.get_attribute("class") or ""
                            vis   = el.is_visible()
                            outer = el.evaluate("e => e.outerHTML")[:200]
                            print(f"    tag={tag} text={text!r} title={title!r} class={cls[:40]!r} visible={vis}")
                            print(f"    outerHTML={outer!r}")
                        except Exception as e2:
                            print(f"    [err: {e2}]")
                else:
                    print(f"  не найдено: {sel}")
            except Exception as e:
                print(f"  ошибка [{sel}]: {e}")

        # ── 5. Ждём дольше и повторяем ────────────────────────────────────
        print("\n5. Ждём ещё 5 секунд (медленный рендер) …")
        page.wait_for_timeout(5000)

        ss_path = str(SCREENSHOTS_DIR / "03_ivo_after_9s.png")
        page.screenshot(path=ss_path, full_page=False)
        print(f"   Скриншот: {ss_path}")

        # Повторяем поиск
        found_els = page.query_selector_all('[title*="Сохранить"], [aria-label*="Сохранить"], img[src*="save"]')
        print(f"\n   Итог повторного поиска [title/aria/img-save]: {len(found_els)} элементов")
        for el in found_els[:5]:
            try:
                outer = el.evaluate("e => e.outerHTML")[:300]
                print(f"   {outer!r}")
            except Exception:
                pass

        # ── 6+7. Кликаем кнопку сохранения, сразу тестируем mouse.click() ──
        save_btn = page.query_selector(".viewFrameToolbarSaveToFile")
        if save_btn and save_btn.is_visible():
            print(f"\n6. Кликаем .viewFrameToolbarSaveToFile")

            # Регистрируем download listener ДО клика
            _downloads = []
            page.on("download", lambda d: _downloads.append(d))

            save_btn.click()
            page.wait_for_timeout(1500)  # ждём рендера меню

            ss_path = str(SCREENSHOTS_DIR / "04_after_click.png")
            page.screenshot(path=ss_path, full_page=False)
            print(f"   Скриншот после клика: {ss_path}")

            if _downloads:
                print(f"   ПРЯМОЙ DOWNLOAD: {_downloads[0].suggested_filename!r}")
            else:
                print("   Прямого download нет — ищем меню")

            # ── НЕМЕДЛЕННО проверяем геометрию и кликаем через mouse ────────
            # (ДО любого DOM-дампа, пока меню не закрылось)
            print("\n7. Тест page.mouse.click() (isTrusted=True) — СРАЗУ после открытия меню:")

            # Selector для DIV-контейнера пункта (не якорь <a>) — имеет корректный bbox
            active_item_sel = ".widgetSelectButtonMenu .x-menu-item:not(.x-menu-item-disabled)"
            item_loc = page.locator(active_item_sel).first

            print(f"   Элементов найдено: {item_loc.count()}")

            # Проверяем bbox у DIV
            bb_div = item_loc.bounding_box() if item_loc.count() > 0 else None
            print(f"   bounding_box DIV (.x-menu-item): {bb_div}")

            # Также проверяем bbox у ссылки для сравнения
            link_loc = page.locator(
                ".widgetSelectButtonMenu .x-menu-item:not(.x-menu-item-disabled) .x-menu-item-link"
            ).first
            bb_link = link_loc.bounding_box() if link_loc.count() > 0 else None
            print(f"   bounding_box LINK (.x-menu-item-link): {bb_link}")

            # Используем тот элемент, у которого есть bbox
            target_bb = bb_div or bb_link
            target_loc = item_loc if bb_div else link_loc

            if target_bb:
                cx = target_bb["x"] + target_bb["width"] / 2
                cy = target_bb["y"] + target_bb["height"] / 2
                print(f"   Кликаем mouse.click({cx:.0f}, {cy:.0f}) …")

                _downloads3 = []
                _requests3: list[dict] = []

                def _on_req3(req):
                    if req.resource_type in ("document", "xhr", "fetch", "other"):
                        _requests3.append({"url": req.url, "method": req.method, "type": req.resource_type})

                page.on("request", _on_req3)
                page.on("download", lambda d: _downloads3.append(d))

                try:
                    with page.expect_download(timeout=15_000) as dl_info:
                        page.mouse.click(cx, cy)
                    dl = dl_info.value
                    print(f"   DOWNLOAD (expect_download): {dl.suggested_filename!r}")
                    tmp_path = SCREENSHOTS_DIR / (dl.suggested_filename or "doc.odt")
                    dl.save_as(str(tmp_path))
                    data = tmp_path.read_bytes()
                    print(f"   Размер: {len(data)} байт, PK={data[:2]==b'PK'}, magic={data[:4].hex()!r}")
                except Exception as exc:
                    print(f"   expect_download timeout/error: {exc}")
                    page.wait_for_timeout(3000)
                    print(f"   Downloads via listener: {len(_downloads3)}")
                    print(f"   Requests: {len(_requests3)}")
                    for r in _requests3[-30:]:
                        print(f"     [{r['type']}] {r['method']} {r['url'][:120]}")

                page.remove_listener("request", _on_req3)
            else:
                print("   bounding_box=None для обоих — меню закрылось или элементы не отрендерены")

                # Дампим меню HTML для диагностики
                menu_el = page.query_selector(".widgetSelectButtonMenu")
                if menu_el:
                    print(f"   Меню в DOM: {menu_el.is_visible()=}")
                    print(f"   outerHTML (500): {menu_el.evaluate('e => e.outerHTML')[:500]!r}")
                else:
                    print("   .widgetSelectButtonMenu не найден — меню закрылось!")

            # Скриншот после всех операций
            ss_path = str(SCREENSHOTS_DIR / "05_after_menu_click.png")
            page.screenshot(path=ss_path, full_page=False)
            print(f"   Скриншот: {ss_path}")
        else:
            print("\n6. Кнопка .viewFrameToolbarSaveToFile не найдена")

        print(f"\nСкриншоты сохранены в: {SCREENSHOTS_DIR}")
        try:
            input("\nНажми Enter чтобы закрыть браузер …")
        except EOFError:
            pass

        ctx.close()
        browser.close()


if __name__ == "__main__":
    run_diagnostics()
