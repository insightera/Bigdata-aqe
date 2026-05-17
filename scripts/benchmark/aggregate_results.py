#!/usr/bin/env python3
"""
Agregasi semua metrik eksperimen → metrics/experiment_summary_*.json
Untuk laporan BAB IV, Grafana exporter, dan Superset (manual import).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmark._common import load_json, metrics_dir, utc_now, write_json
from benchmark.compare_aqe_runs import compare

logger = None


def _collect_by_glob(directory: Path, pattern: str) -> list[dict]:
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    return [load_json(f) | {"_file": f.name} for f in files[-5:]]


def aggregate(metrics_path: Path | None = None, experiment_id: str | None = None) -> dict:
    mdir = metrics_path or metrics_dir()
    comparison = compare(metrics_path=mdir)

    summary = {
        "experiment_id": experiment_id or f"EXP-{utc_now().strftime('%Y%m%d-%H%M%S')}",
        "generated_at": utc_now().isoformat(),
        "metrics_directory": str(mdir.resolve()),
        "dataset_summaries": _collect_by_glob(mdir, "dataset_summary_*.json"),
        "pipelines": {
            "staging_to_bronze": _collect_by_glob(mdir, "staging_to_bronze_*.json"),
            "bronze_to_silver_off": _collect_by_glob(mdir, "bronze_to_silver_aqe_OFF_*.json"),
            "bronze_to_silver_on": _collect_by_glob(mdir, "bronze_to_silver_aqe_ON_*.json"),
            "silver_to_gold": _collect_by_glob(mdir, "silver_to_gold_*.json"),
        },
        "workloads": {
            "spark_off": _collect_by_glob(mdir, "workloads_spark_aqe_OFF_*.json"),
            "spark_on": _collect_by_glob(mdir, "workloads_spark_aqe_ON_*.json"),
            "trino_off": _collect_by_glob(mdir, "workloads_trino_ctx_OFF_*.json"),
            "trino_on": _collect_by_glob(mdir, "workloads_trino_ctx_ON_*.json"),
        },
        "aqe_comparison": comparison,
    }
    return summary


def main():
    global logger
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("benchmark.aggregate")

    parser = argparse.ArgumentParser(description="Aggregate experiment metrics")
    parser.add_argument("--metrics-dir", default=None)
    parser.add_argument("--experiment-id", default=None)
    parser.add_argument("--write-latest", action="store_true", help="Tulis juga experiment_summary_latest.json")
    args = parser.parse_args()

    mdir = Path(args.metrics_dir) if args.metrics_dir else metrics_dir()
    summary = aggregate(mdir, experiment_id=args.experiment_id)
    ts = utc_now().strftime("%Y%m%d_%H%M%S")
    out = mdir / f"experiment_summary_{ts}.json"
    write_json(out, summary)
    if args.write_latest:
        write_json(mdir / "experiment_summary_latest.json", summary)
    logger.info("Experiment summary → %s", out)
    import json
    print(json.dumps({"experiment_summary": str(out), "silver_speedup_pct": summary["aqe_comparison"].get("silver_pipeline", {}).get("speedup_pct")}, indent=2))


if __name__ == "__main__":
    main()
