"""Pydantic-схемы для API."""
from pydantic import BaseModel


class ChatRequest(BaseModel):
    session_id: str
    message: str


class Source(BaseModel):
    doc_id: str
    title: str = ""
    section: str = ""
    text: str = ""
    score: float = 0.0
