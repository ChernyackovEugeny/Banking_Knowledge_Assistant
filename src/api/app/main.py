from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routers.chat import router as chat_router
from src.api.routers.dashboard import router as dashboard_router
from src.api.services.chat_logger import ChatLogger


@asynccontextmanager
async def lifespan(app: FastAPI):
    ChatLogger.initialize()
    yield
    ChatLogger.shutdown()


app = FastAPI(
    title="ИнвестБанк · КИБ Ассистент",
    description="RAG-ассистент по корпоративным документам блока КИБ",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(chat_router)
app.include_router(dashboard_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
