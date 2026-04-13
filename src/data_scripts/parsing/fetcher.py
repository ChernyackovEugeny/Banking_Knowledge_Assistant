"""HTTP-клиент с дисковым кэшем и retry.

Все запросы кэшируются в data/fetch_cache/ по SHA-256 хэшу URL.
Повторный запуск пайплайна не тратит сетевые ресурсы.
--force-fetch сбрасывает кэш для конкретного URL.
"""
import hashlib
import time
import logging
from pathlib import Path

import requests

from db_logging.log_utils import log_http_cache_hit, log_http_error, log_http_success

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
}

# Пауза между запросами, чтобы не перегружать источники
REQUEST_DELAY_SEC = 1.5

_CACHE_DIR: Path | None = None


def set_cache_dir(path: Path) -> None:
    global _CACHE_DIR
    _CACHE_DIR = path
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(url: str) -> Path:
    if _CACHE_DIR is None:
        raise RuntimeError("Вызови set_cache_dir() перед использованием fetcher")
    key = hashlib.sha256(url.encode()).hexdigest()
    return _CACHE_DIR / key


def fetch(
    url: str,
    *,
    force: bool = False,
    retries: int = 3,
    timeout: int = 30,
    doc_id: str | None = None,
    run_log: object = None,
) -> bytes:
    """Скачивает URL, используя дисковый кэш.

    Args:
        url: целевой URL
        force: если True — игнорирует кэш и скачивает заново
        retries: количество попыток при сетевых ошибках
        timeout: таймаут соединения в секундах
        doc_id: идентификатор документа для DB-логирования (опционально)
        run_log: экземпляр RunLogger для DB-логирования (опционально)

    Returns:
        Сырые байты ответа (HTML или PDF)

    Raises:
        requests.HTTPError: если сервер вернул 4xx/5xx после всех попыток
        requests.RequestException: при сетевых проблемах после всех попыток
    """
    cache = _cache_path(url)

    if not force and cache.exists():
        logger.debug("Кэш: %s", url)
        t0 = time.monotonic()
        data = cache.read_bytes()
        duration_ms = (time.monotonic() - t0) * 1000
        log_http_cache_hit(run_log, url, doc_id, data, duration_ms)
        return data

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        t0 = time.monotonic()
        try:
            logger.info("GET [%d/%d] %s", attempt, retries, url)
            time.sleep(REQUEST_DELAY_SEC)

            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            data = resp.content

            duration_ms = (time.monotonic() - t0) * 1000
            log_http_success(run_log, url, doc_id, attempt, resp, data, duration_ms)
            
            cache.write_bytes(data)
            logger.debug("Сохранено в кэш: %s байт", len(data))
            return data
        except requests.RequestException as exc:
            duration_ms = (time.monotonic() - t0) * 1000
            log_http_error(run_log, url, doc_id, attempt, exc, duration_ms)
            last_exc = exc
            if attempt < retries:
                wait = 2 ** attempt  # 2, 4, 8 секунд
                logger.warning("Retry %d/%d через %ds: %s — %s", attempt, retries, wait, url, exc)
                time.sleep(wait)

    raise last_exc  # type: ignore[misc]
