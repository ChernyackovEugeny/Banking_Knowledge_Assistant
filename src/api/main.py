"""Compatibility entrypoint.

Preferred: `uvicorn src.api.main:app --reload --port 8000`
"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.api.app.main import app
