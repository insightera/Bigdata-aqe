#!/usr/bin/env python3
"""
HTTP exporter format Prometheus dari metrics/*.json
Dipakai Grafana (datasource Prometheus) untuk dashboard AQE.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

METRICS_DIR = Path(os.environ.get("AQE_METRICS_DIR", "metrics"))


def _escape_label(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _metric_line(name: str, value: float, labels: dict | None = None) -> str:
    if labels:
        lbl = ",".join(f'{k}="{_escape_label(str(v))}"' for k, v in labels.items())
        return f"{name}{{{lbl}}} {value}\n"
    return f"{name} {value}\n"


def build_prometheus_text(metrics_dir: Path) -> str:
    lines: list[str] = []
    latest = metrics_dir / "experiment_summary_latest.json"
    if latest.is_file():
        try:
            summary = json.loads(latest.read_text(encoding="utf-8"))
            cmp_ = summary.get("aqe_comparison", {})
            sp = cmp_.get("silver_pipeline", {})
            speedup = sp.get("speedup_pct")
            if speedup is not None:
                lines.append(_metric_line("lakehouse_aqe_silver_speedup_percent", float(speedup)))
            for side, key in (("off", "off"), ("on", "on")):
                block = sp.get(side) or {}
                dur = block.get("duration_sec")
                if dur is not None:
                    lines.append(
                        _metric_line(
                            "lakehouse_pipeline_duration_seconds",
                            float(dur),
                            {"pipeline": "bronze_to_silver", "aqe_scenario": side.upper()},
                        )
                    )
        except (json.JSONDecodeError, OSError):
            pass

    for pattern, pipeline, label_key in (
        ("staging_to_bronze_*.json", "staging_to_bronze", None),
        ("bronze_to_silver_aqe_OFF_*.json", "bronze_to_silver", "OFF"),
        ("bronze_to_silver_aqe_ON_*.json", "bronze_to_silver", "ON"),
        ("silver_to_gold_*.json", "silver_to_gold", None),
    ):
        files = sorted(metrics_dir.glob(pattern), key=lambda p: p.stat().st_mtime)
        if not files:
            continue
        try:
            data = json.loads(files[-1].read_text(encoding="utf-8"))
            dur = data.get("duration_sec")
            if dur is None:
                continue
            labels = {"pipeline": pipeline}
            if label_key:
                labels["aqe_scenario"] = label_key
            elif data.get("aqe_scenario"):
                labels["aqe_scenario"] = str(data["aqe_scenario"])
            elif data.get("aqe_context_silver"):
                labels["aqe_scenario"] = str(data["aqe_context_silver"]).upper()
            lines.append(_metric_line("lakehouse_pipeline_duration_seconds", float(dur), labels))
        except (json.JSONDecodeError, OSError):
            continue

    for pattern, engine in (
        ("workloads_spark_aqe_OFF_*.json", "spark"),
        ("workloads_spark_aqe_ON_*.json", "spark"),
        ("workloads_trino_ctx_OFF_*.json", "trino"),
        ("workloads_trino_ctx_ON_*.json", "trino"),
    ):
        files = sorted(metrics_dir.glob(pattern), key=lambda p: p.stat().st_mtime)
        if not files:
            continue
        try:
            data = json.loads(files[-1].read_text(encoding="utf-8"))
            scenario = (
                data.get("aqe_scenario")
                or data.get("aqe_context_silver")
                or "unknown"
            )
            scenario = str(scenario).upper()
            for wid, wl in (data.get("workloads") or {}).items():
                dur = wl.get("duration_sec")
                if dur is None:
                    continue
                lines.append(
                    _metric_line(
                        "lakehouse_workload_duration_seconds",
                        float(dur),
                        {
                            "workload_id": wid,
                            "engine": engine,
                            "aqe_scenario": scenario,
                            "workload_type": wl.get("workload_type", "unknown"),
                        },
                    )
                )
        except (json.JSONDecodeError, OSError):
            continue

    if not lines:
        lines.append("# No metrics yet\n")
    return "".join(lines)


class MetricsHandler(BaseHTTPRequestHandler):
    metrics_dir: Path = METRICS_DIR

    def do_GET(self):
        if self.path in ("/metrics", "/"):
            body = build_prometheus_text(self.metrics_dir).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description="Prometheus metrics exporter for AQE JSON")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("METRICS_EXPORTER_PORT", "9101")))
    parser.add_argument("--metrics-dir", default=str(METRICS_DIR))
    args = parser.parse_args()

    MetricsHandler.metrics_dir = Path(args.metrics_dir)
    server = HTTPServer((args.host, args.port), MetricsHandler)
    print(f"Metrics exporter on http://{args.host}:{args.port}/metrics (dir={args.metrics_dir})")
    server.serve_forever()


if __name__ == "__main__":
    main()
