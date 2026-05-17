#!/usr/bin/env python3
"""
Jalankan workload SQL Trino pada layer Gold (W4–W6).
Catat durasi → metrics/workloads_trino_*.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmark._common import metrics_dir, utc_now, write_json
from benchmark.workloads import TRINO_GOLD_WORKLOADS

logger = logging.getLogger("benchmark.trino_workloads")

DEFAULT_TRINO = os.environ.get("TRINO_URL", "http://trino:8080")
DEFAULT_USER = os.environ.get("TRINO_USER", "admin")
DEFAULT_CATALOG = os.environ.get("TRINO_CATALOG", "lakehouse")
DEFAULT_SCHEMA = os.environ.get("TRINO_SCHEMA", "gold")


class TrinoClient:
    """Klien minimal Trino REST API (tanpa dependency eksternal)."""

    def __init__(
        self,
        base_url: str = DEFAULT_TRINO,
        user: str = DEFAULT_USER,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
    ):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.catalog = catalog
        self.schema = schema

    def _headers(self, extra: dict | None = None) -> dict:
        h = {
            "X-Trino-User": self.user,
            "X-Trino-Catalog": self.catalog,
            "X-Trino-Schema": self.schema,
        }
        if extra:
            h.update(extra)
        return h

    def _request(self, method: str, url: str, data: bytes | None = None, headers: dict | None = None):
        req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
        with urllib.request.urlopen(req, timeout=600) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def execute(self, sql: str) -> tuple[list[list[Any]], float]:
        """Eksekusi query; return (rows, duration_sec)."""
        t0 = time.perf_counter()
        body = sql.strip().encode("utf-8")
        state = self._request(
            "POST",
            f"{self.base_url}/v1/statement",
            data=body,
            headers={**self._headers(), "Content-Type": "text/plain"},
        )
        rows: list[list[Any]] = []
        columns = [c["name"] for c in state.get("columns", [])] if state.get("columns") else []

        while True:
            if "data" in state and state["data"]:
                for row in state["data"]:
                    rows.append(row)
            if state.get("nextUri"):
                state = self._request("GET", state["nextUri"], headers=self._headers())
                continue
            if state.get("stats", {}).get("state") == "FAILED":
                err = state.get("error", {})
                raise RuntimeError(err.get("message", str(err)))
            break

        duration = round(time.perf_counter() - t0, 3)
        return rows, duration, columns


def run_workloads(aqe_context: str = "unknown") -> dict:
    started_at = utc_now()
    client = TrinoClient()
    results: dict = {}

    for wl in TRINO_GOLD_WORKLOADS:
        wid = wl["id"]
        logger.info("Trino workload %s (%s) | silver_context=%s", wid, wl["name"], aqe_context)
        try:
            rows, duration_sec, _cols = client.execute(wl["sql"])
            results[wid] = {
                "id": wid,
                "name": wl["name"],
                "workload_type": wl["workload_type"],
                "duration_sec": duration_sec,
                "row_count": len(rows),
                "status": "ok",
            }
            logger.info("  %s: %.3fs, %s rows", wid, duration_sec, len(rows))
        except (urllib.error.URLError, RuntimeError, TimeoutError) as exc:
            logger.error("  %s failed: %s", wid, exc)
            results[wid] = {
                "id": wid,
                "name": wl["name"],
                "workload_type": wl["workload_type"],
                "duration_sec": None,
                "row_count": 0,
                "status": "error",
                "error": str(exc),
            }

    ended_at = utc_now()
    ctx = aqe_context.upper()
    payload = {
        "engine": "trino",
        "layer": "gold",
        "aqe_context_silver": ctx,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_sec_total": round((ended_at - started_at).total_seconds(), 3),
        "trino_url": client.base_url,
        "workloads": results,
    }
    ts = ended_at.strftime("%Y%m%d_%H%M%S")
    out = metrics_dir() / f"workloads_trino_ctx_{ctx}_{ts}.json"
    write_json(out, payload)
    payload["metrics_file"] = str(out)
    logger.info("Trino workloads metrics → %s", out)
    return payload


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run Trino Gold workloads (W4–W6)")
    parser.add_argument(
        "--aqe-context",
        default=os.environ.get("SPARK_AQE_SCENARIO", "unknown"),
        help="Konteks Silver saat Gold dibangun (OFF/ON)",
    )
    parser.add_argument("--trino-url", default=DEFAULT_TRINO)
    args = parser.parse_args()
    os.environ["TRINO_URL"] = args.trino_url
    out = run_workloads(args.aqe_context)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
