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
    retrieval_candidates: int = 20
    retrieval_rrf_k: int = 60
    chroma_host: str = "localhost"
    chroma_port: int = 8000
    bm25_dir: str = str(_ROOT_DIR / "data" / "bm25_indexes")

    # Observability versions (for metric comparability across runs)
    observability_config_version: str = "v1"
    observability_prompt_version: str = "v1"
    observability_retrieval_version: str = "hybrid_rrf_v1"

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
