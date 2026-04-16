"""РќР°СЃС‚СЂРѕР№РєРё API РёР· РїРµСЂРµРјРµРЅРЅС‹С… РѕРєСЂСѓР¶РµРЅРёСЏ / .env С„Р°Р№Р»Р°."""
from __future__ import annotations

from pathlib import Path
from pydantic_settings import BaseSettings

_ROOT_DIR = Path(__file__).parents[3]  # src/api/ в†’ src/ в†’ root


class Settings(BaseSettings):
    # LLM
    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"
    deepseek_model: str = "deepseek-chat"

    # Р”РёР°Р»РѕРі
    history_window: int = 20  # РїРѕСЃР»РµРґРЅРёРµ N СЃРѕРѕР±С‰РµРЅРёР№ (user + assistant РІРјРµСЃС‚Рµ)

    # Retrieval
    retrieval_top_k: int = 5

    model_config = {
        "env_file": str(_ROOT_DIR / ".env"),
        "env_ignore_empty": True,
        "extra": "ignore",
    }


_instance: Settings | None = None


def get_settings() -> Settings:
    global _instance
    if _instance is None:
        _instance = Settings()
    return _instance
