"""Utilitas bersama modul benchmark."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def metrics_dir() -> Path:
    """Direktori metrik: di Docker Airflow = /opt/airflow/metrics (volume ./metrics)."""
    env = os.environ.get("AQE_METRICS_DIR")
    if env:
        return Path(env)
    docker_mount = Path("/opt/airflow/metrics")
    if docker_mount.is_dir():
        return docker_mount
    return Path("metrics")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def write_json(path: Path, payload: dict[str, Any]) -> Path:
    out = path if path.is_absolute() else metrics_dir() / path.name
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    except PermissionError as exc:
        raise PermissionError(
            f"Tidak bisa menulis {out}. Di host jalankan: "
            f"mkdir -p metrics && chmod 1777 metrics"
        ) from exc
    return out


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
