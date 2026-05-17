"""Utilitas bersama modul benchmark."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def metrics_dir() -> Path:
    return Path(os.environ.get("AQE_METRICS_DIR", "metrics"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def speedup_pct(duration_off: float, duration_on: float) -> float | None:
    if duration_off <= 0:
        return None
    return round((duration_off - duration_on) / duration_off * 100.0, 2)


def throughput_rows_per_sec(rows: int, duration_sec: float) -> float | None:
    if duration_sec <= 0:
        return None
    return round(rows / duration_sec, 2)
