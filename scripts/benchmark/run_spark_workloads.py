#!/usr/bin/env python3
"""
Jalankan workload Spark SQL pada layer Silver (W1–W3).
Catat durasi per skenario AQE OFF/ON → metrics/workloads_spark_*.json
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmark._common import metrics_dir, utc_now, write_json
from benchmark.workloads import SPARK_SILVER_WORKLOADS
from spark.aqe_config import read_applied_aqe_configs, resolve_aqe_scenario
from spark.bronze_to_silver import get_spark_session

logger = logging.getLogger("benchmark.spark_workloads")


def run_workloads(aqe_scenario: str, warmup: int = 0) -> dict:
    scenario = resolve_aqe_scenario(aqe_scenario)
    os.environ["SPARK_AQE_SCENARIO"] = scenario
    started_at = utc_now()

    spark = get_spark_session(scenario)
    applied = read_applied_aqe_configs(spark)
    results: dict = {}

    try:
        for wl in SPARK_SILVER_WORKLOADS:
            wid = wl["id"]
            sql = wl["sql"].strip()
            logger.info("Workload %s (%s) | AQE=%s", wid, wl["name"], scenario)

            for i in range(warmup):
                spark.sql(sql).collect()

            t0 = time.perf_counter()
            df = spark.sql(sql)
            row_count = df.count()
            duration_sec = round(time.perf_counter() - t0, 3)

            results[wid] = {
                "id": wid,
                "name": wl["name"],
                "workload_type": wl["workload_type"],
                "duration_sec": duration_sec,
                "row_count": row_count,
                "status": "ok",
            }
            logger.info("  %s: %.3fs, %s rows", wid, duration_sec, f"{row_count:,}")

    finally:
        spark.stop()

    ended_at = utc_now()
    payload = {
        "engine": "spark",
        "layer": "silver",
        "aqe_scenario": scenario,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_sec_total": round((ended_at - started_at).total_seconds(), 3),
        "spark_configs": applied,
        "workloads": results,
    }
    ts = ended_at.strftime("%Y%m%d_%H%M%S")
    out = metrics_dir() / f"workloads_spark_aqe_{scenario}_{ts}.json"
    write_json(out, payload)
    payload["metrics_file"] = str(out)
    logger.info("Spark workloads metrics → %s", out)
    return payload


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run Spark Silver workloads (W1–W3)")
    parser.add_argument("--aqe-scenario", choices=["OFF", "ON", "off", "on"], default="OFF")
    parser.add_argument("--warmup", type=int, default=0, help="Jumlah run pemanasan per query")
    args = parser.parse_args()
    out = run_workloads(args.aqe_scenario, warmup=args.warmup)
    print(json_dumps(out))


def json_dumps(obj):
    import json
    return json.dumps(obj, indent=2, default=str)


if __name__ == "__main__":
    main()
