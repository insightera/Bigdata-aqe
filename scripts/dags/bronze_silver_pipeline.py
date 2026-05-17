"""
DAG: Bronze → Silver Pipeline (layer utama eksperimen AQE)
==========================================================
Transformasi Bronze → Silver dengan konfigurasi Spark:

  SPARK_AQE_SCENARIO=OFF  → baseline (AQE disabled)
  SPARK_AQE_SCENARIO=ON   → Adaptive Query Execution aktif

Trigger dengan conf JSON:
  {"aqe_scenario": "ON"}
  {"aqe_scenario": "OFF"}
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")


def _resolve_scenario_from_context(context) -> str:
    from spark.aqe_config import resolve_aqe_scenario

    dag_run = context.get("dag_run")
    conf_scenario = None
    if dag_run and getattr(dag_run, "conf", None):
        conf_scenario = dag_run.conf.get("aqe_scenario")
    return resolve_aqe_scenario(conf_scenario or os.environ.get("SPARK_AQE_SCENARIO"))


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def run_spark_bronze_to_silver(**context):
    scenario = _resolve_scenario_from_context(context)
    os.environ["SPARK_AQE_SCENARIO"] = scenario
    logging.info("Bronze → Silver | SPARK_AQE_SCENARIO=%s", scenario)

    from spark.bronze_to_silver import run_bronze_to_silver

    profiling_results = run_bronze_to_silver(aqe_scenario=scenario)

    aqe_meta = profiling_results.pop("_aqe_meta", {})
    context["ti"].xcom_push(key="aqe_scenario", value=scenario)
    context["ti"].xcom_push(key="aqe_meta", value=aqe_meta)
    context["ti"].xcom_push(key="silver_profiling", value=profiling_results)

    written = sum(1 for r in profiling_results.values() if r.get("written"))
    total_rows = sum(
        r.get("row_count", 0) for r in profiling_results.values() if r.get("written")
    )
    logging.info(
        "Silver complete | AQE=%s | %d/%d tables | %s rows | duration=%ss | metrics=%s",
        scenario,
        written,
        len(profiling_results),
        f"{total_rows:,}",
        aqe_meta.get("duration_sec", "?"),
        aqe_meta.get("metrics_file", "?"),
    )

    quality_summary = {}
    for name, prof in profiling_results.items():
        q = prof.get("quality", {})
        quality_summary[name] = {
            "status": q.get("source_status", q.get("status", "?")),
            "score": q.get("source_score", q.get("quality_score", 0)),
            "written": prof.get("written", False),
        }

    context["ti"].xcom_push(key="quality_summary", value=quality_summary)
    return written


def log_quality_report(**context):
    scenario = context["ti"].xcom_pull(task_ids="bronze_to_silver", key="aqe_scenario") or "?"
    aqe_meta = context["ti"].xcom_pull(task_ids="bronze_to_silver", key="aqe_meta") or {}
    quality = context["ti"].xcom_pull(task_ids="bronze_to_silver", key="quality_summary")
    profiling = context["ti"].xcom_pull(task_ids="bronze_to_silver", key="silver_profiling")

    logging.info("\n" + "=" * 60)
    logging.info("  QUALITY REPORT — Bronze → Silver")
    logging.info("  AQE scenario: %s", scenario)
    if aqe_meta.get("spark_configs"):
        logging.info("  Spark adaptive.enabled: %s", aqe_meta["spark_configs"].get(
            "spark.sql.adaptive.enabled"
        ))
    logging.info("  Metrics file: %s", aqe_meta.get("metrics_file", "—"))
    logging.info("=" * 60)

    for name, q in (quality or {}).items():
        status_icon = {"PASS": "✅", "QUARANTINE": "⚠️", "REJECT": "❌"}.get(
            q.get("status", ""), "❓"
        )
        prof = (profiling or {}).get(name, {})
        transforms = prof.get("transformations", [])
        logging.info(
            "\n  %s %s"
            "\n    Score: %.1f%%  |  Status: %s  |  Written: %s"
            "\n    Rows: %s  |  Transformations: %d",
            status_icon,
            name,
            q.get("score", 0),
            q.get("status", "?"),
            q.get("written", False),
            f"{prof.get('row_count', 0):,}",
            len(transforms),
        )


with DAG(
    dag_id="bronze_to_silver_pipeline",
    description="Bronze → Silver — eksperimen AQE (OFF vs ON via dag_run.conf)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    params={"aqe_scenario": "OFF"},
    tags=["lakehouse", "aqe", "iceberg", "silver", "quality", "pipeline"],
) as dag:

    spark_etl = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_spark_bronze_to_silver,
        execution_timeout=timedelta(hours=2),
    )

    quality_report = PythonOperator(
        task_id="quality_report",
        python_callable=log_quality_report,
    )

    spark_etl >> quality_report
