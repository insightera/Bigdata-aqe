#!/usr/bin/env python3
"""
Orkestrator eksperimen AQE end-to-end.

Alur:
  1. Ringkasan dataset (opsional)
  2. Staging → Bronze
  3. Bronze → Silver (AQE OFF) + Spark workloads OFF
  4. Silver → Gold (konteks OFF) + Trino workloads OFF
  5. Bronze → Silver (AQE ON) + Spark workloads ON
  6. Silver → Gold (konteks ON) + Trino workloads ON
  7. compare_aqe_runs + aggregate_results

Jalankan dari host (stack Docker harus hidup):
  PYTHONPATH=scripts python3 scripts/benchmark/run_experiment.py

Dari container Airflow:
  docker exec lhaqe-airflow-scheduler python3 /opt/airflow/scripts/benchmark/run_experiment.py
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logger = logging.getLogger("benchmark.experiment")


def _step(name: str, fn):
    logger.info("=" * 60)
    logger.info("STEP: %s", name)
    logger.info("=" * 60)
    return fn()


def run_local(
    *,
    skip_dataset: bool = False,
    skip_staging: bool = False,
    skip_gold_off: bool = False,
    staging_dir: str | None = None,
) -> None:
    from benchmark.aggregate_results import aggregate
    from benchmark.compare_aqe_runs import compare, print_markdown_table
    from benchmark.dataset_summary import summarize_staging
    from benchmark.run_spark_workloads import run_workloads as run_spark_wl
    from benchmark.run_trino_workloads import run_workloads as run_trino_wl
    from pathlib import Path

    from benchmark._common import metrics_dir, utc_now, write_json

    if not skip_dataset and staging_dir:
        staging = Path(staging_dir)
        if staging.is_dir() and any(staging.glob("*.csv")):
            from benchmark._common import write_json as wj
            payload = summarize_staging(staging)
            wj(metrics_dir() / f"dataset_summary_{utc_now().strftime('%Y%m%d_%H%M%S')}.json", payload)

    if not skip_staging:
        from spark.staging_to_bronze import run_staging_to_bronze
        _step("staging_to_bronze", run_staging_to_bronze)

    from spark.bronze_to_silver import run_bronze_to_silver
    from spark.silver_to_gold import run_silver_to_gold

    _step("bronze_to_silver AQE=OFF", lambda: run_bronze_to_silver(aqe_scenario="OFF"))
    os.environ["SPARK_AQE_SCENARIO"] = "OFF"
    _step("spark workloads OFF", lambda: run_spark_wl("OFF"))

    if not skip_gold_off:
        _step("silver_to_gold (after OFF)", lambda: run_silver_to_gold(aqe_context="OFF"))
        _step("trino workloads OFF", lambda: run_trino_wl("OFF"))

    _step("bronze_to_silver AQE=ON", lambda: run_bronze_to_silver(aqe_scenario="ON"))
    os.environ["SPARK_AQE_SCENARIO"] = "ON"
    _step("spark workloads ON", lambda: run_spark_wl("ON"))

    _step("silver_to_gold (after ON)", lambda: run_silver_to_gold(aqe_context="ON"))
    _step("trino workloads ON", lambda: run_trino_wl("ON"))

    report = _step("compare AQE", compare)
    print_markdown_table(report)

    from benchmark._common import write_json

    summary = aggregate()
    mdir = metrics_dir()
    ts = utc_now().strftime("%Y%m%d_%H%M%S")
    out = mdir / f"experiment_summary_{ts}.json"
    write_json(out, summary)
    write_json(mdir / "experiment_summary_latest.json", summary)
    logger.info("Experiment complete → %s", out)


def run_via_airflow(docker_container: str = "lhaqe-airflow-scheduler") -> None:
    """Trigger DAG aqe_full_experiment (lebih andal di cluster Docker)."""
    cmd = [
        "docker", "exec", docker_container,
        "airflow", "dags", "trigger", "aqe_full_experiment",
    ]
    logger.info("Triggering Airflow DAG: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="End-to-end AQE experiment orchestrator")
    parser.add_argument(
        "--mode",
        choices=["local", "airflow"],
        default="local",
        help="local=import Spark langsung; airflow=trigger DAG",
    )
    parser.add_argument("--skip-dataset-summary", action="store_true")
    parser.add_argument("--skip-staging", action="store_true", help="Bronze sudah terisi")
    parser.add_argument("--skip-gold-off", action="store_true", help="Lewati Gold+Trino setelah OFF")
    parser.add_argument(
        "--staging-dir",
        default=os.environ.get("STAGING_DATA_DIR", "data/staging"),
    )
    parser.add_argument("--docker-container", default="lhaqe-airflow-scheduler")
    args = parser.parse_args()

    if args.mode == "airflow":
        run_via_airflow(args.docker_container)
    else:
        run_local(
            skip_dataset=args.skip_dataset_summary,
            skip_staging=args.skip_staging,
            skip_gold_off=args.skip_gold_off,
            staging_dir=args.staging_dir,
        )


if __name__ == "__main__":
    main()
