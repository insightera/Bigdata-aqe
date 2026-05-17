#!/usr/bin/env python3
"""
Bandingkan metrik AQE OFF vs ON dari file JSON pipeline & workload.
Output: metrics/aqe_comparison_*.json + ringkasan stdout (Markdown-friendly).
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmark._common import load_json, metrics_dir, speedup_pct, throughput_rows_per_sec, utc_now, write_json

logger = logging.getLogger("benchmark.compare")


def _latest(pattern: str, directory: Path) -> Path | None:
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _pipeline_duration(path: Path | None) -> dict | None:
    if not path:
        return None
    data = load_json(path)
    summary = data.get("summary", {})
    return {
        "file": str(path.name),
        "duration_sec": data.get("duration_sec"),
        "rows_written": summary.get("rows_written"),
        "throughput_rows_per_sec": throughput_rows_per_sec(
            int(summary.get("rows_written", 0)),
            float(data.get("duration_sec", 0)),
        ),
    }


def _workload_comparison(off_path: Path | None, on_path: Path | None) -> list[dict]:
    off_w = load_json(off_path).get("workloads", {}) if off_path else {}
    on_w = load_json(on_path).get("workloads", {}) if on_path else {}
    ids = sorted(set(off_w.keys()) | set(on_w.keys()))
    rows = []
    for wid in ids:
        d_off = (off_w.get(wid) or {}).get("duration_sec")
        d_on = (on_w.get(wid) or {}).get("duration_sec")
        rows.append({
            "workload_id": wid,
            "name": (off_w.get(wid) or on_w.get(wid) or {}).get("name"),
            "workload_type": (off_w.get(wid) or on_w.get(wid) or {}).get("workload_type"),
            "duration_off_sec": d_off,
            "duration_on_sec": d_on,
            "speedup_pct": speedup_pct(float(d_off or 0), float(d_on or 0))
            if d_off and d_on
            else None,
        })
    return rows


def compare(
    metrics_path: Path | None = None,
    off_silver: Path | None = None,
    on_silver: Path | None = None,
    off_spark_wl: Path | None = None,
    on_spark_wl: Path | None = None,
    off_trino_wl: Path | None = None,
    on_trino_wl: Path | None = None,
) -> dict:
    mdir = metrics_path or metrics_dir()

    off_silver = off_silver or _latest("bronze_to_silver_aqe_OFF_*.json", mdir)
    on_silver = on_silver or _latest("bronze_to_silver_aqe_ON_*.json", mdir)
    off_spark_wl = off_spark_wl or _latest("workloads_spark_aqe_OFF_*.json", mdir)
    on_spark_wl = on_spark_wl or _latest("workloads_spark_aqe_ON_*.json", mdir)
    off_trino_wl = off_trino_wl or _latest("workloads_trino_ctx_OFF_*.json", mdir)
    on_trino_wl = on_trino_wl or _latest("workloads_trino_ctx_ON_*.json", mdir)

    silver_off = _pipeline_duration(off_silver)
    silver_on = _pipeline_duration(on_silver)

    pipeline_speedup = None
    if silver_off and silver_on and silver_off.get("duration_sec") and silver_on.get("duration_sec"):
        pipeline_speedup = speedup_pct(
            float(silver_off["duration_sec"]),
            float(silver_on["duration_sec"]),
        )

    report = {
        "generated_at": utc_now().isoformat(),
        "silver_pipeline": {
            "off": silver_off,
            "on": silver_on,
            "speedup_pct": pipeline_speedup,
        },
        "spark_workloads": _workload_comparison(off_spark_wl, on_spark_wl),
        "trino_workloads": _workload_comparison(off_trino_wl, on_trino_wl),
        "source_files": {
            "silver_off": str(off_silver) if off_silver else None,
            "silver_on": str(on_silver) if on_silver else None,
            "spark_workloads_off": str(off_spark_wl) if off_spark_wl else None,
            "spark_workloads_on": str(on_spark_wl) if on_spark_wl else None,
            "trino_off": str(off_trino_wl) if off_trino_wl else None,
            "trino_on": str(on_trino_wl) if on_trino_wl else None,
        },
    }
    return report


def print_markdown_table(report: dict) -> None:
    sp = report.get("silver_pipeline", {})
    print("\n## Perbandingan Silver Pipeline (AQE OFF vs ON)\n")
    print("| Metrik | OFF | ON | Speedup % |")
    print("|--------|-----|-----|-----------|")
    off = sp.get("off") or {}
    on = sp.get("on") or {}
    print(
        f"| Durasi (s) | {off.get('duration_sec', '—')} | {on.get('duration_sec', '—')} | "
        f"{sp.get('speedup_pct', '—')} |"
    )
    print(
        f"| Throughput (rows/s) | {off.get('throughput_rows_per_sec', '—')} | "
        f"{on.get('throughput_rows_per_sec', '—')} | — |"
    )

    for section, title in (
        ("spark_workloads", "Spark Workloads (Silver)"),
        ("trino_workloads", "Trino Workloads (Gold)"),
    ):
        rows = report.get(section) or []
        if not rows:
            continue
        print(f"\n## {title}\n")
        print("| ID | Tipe | OFF (s) | ON (s) | Speedup % |")
        print("|----|------|---------|--------|-----------|")
        for r in rows:
            print(
                f"| {r.get('workload_id')} | {r.get('workload_type')} | "
                f"{r.get('duration_off_sec', '—')} | {r.get('duration_on_sec', '—')} | "
                f"{r.get('speedup_pct', '—')} |"
            )


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Compare AQE OFF vs ON metrics")
    parser.add_argument("--metrics-dir", default=None)
    parser.add_argument("--silver-off", default=None, help="Path bronze_to_silver_aqe_OFF_*.json")
    parser.add_argument("--silver-on", default=None)
    parser.add_argument("--markdown", action="store_true")
    args = parser.parse_args()

    mdir = Path(args.metrics_dir) if args.metrics_dir else metrics_dir()
    report = compare(
        metrics_path=mdir,
        off_silver=Path(args.silver_off) if args.silver_off else None,
        on_silver=Path(args.silver_on) if args.silver_on else None,
    )
    ts = utc_now().strftime("%Y%m%d_%H%M%S")
    out = mdir / f"aqe_comparison_{ts}.json"
    write_json(out, report)
    logger.info("Comparison report → %s", out)

    if args.markdown:
        print_markdown_table(report)
    else:
        import json
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
