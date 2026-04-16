"""In-memory хранилище истории диалогов.

Ключ — session_id (UUID, генерируется на клиенте).
Хранит последние history_window сообщений на сессию.
"""
from __future__ import annotations

from collections import defaultdict
from typing import TypedDict


class ChatMessage(TypedDict):
    role: str   # "user" | "assistant"
    content: str


_sessions: dict[str, list[ChatMessage]] = defaultdict(list)


def get_history(session_id: str) -> list[ChatMessage]:
    """Возвращает копию истории для сессии."""
    return list(_sessions[session_id])


def append_exchange(
    session_id: str,
    user_message: str,
    assistant_reply: str,
    window: int,
) -> None:
    """Добавляет пару user/assistant и обрезает историю до window."""
    _sessions[session_id].extend([
        {"role": "user",      "content": user_message},
        {"role": "assistant", "content": assistant_reply},
    ])
    if len(_sessions[session_id]) > window:
        _sessions[session_id] = _sessions[session_id][-window:]
