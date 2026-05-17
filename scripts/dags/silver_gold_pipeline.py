"""
DAG: Silver → Gold Pipeline (Star Schema IKU)
==============================================
Silver (enriched) → Gold (5 dimensi + 10 fakta) untuk konsumsi Trino/Superset.
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def run_spark_silver_to_gold(**context):
    from spark.silver_to_gold import run_silver_to_gold

    profiling_results = run_silver_to_gold()
    context["ti"].xcom_push(key="gold_profiling", value=profiling_results)

    written = sum(1 for r in profiling_results.values() if r.get("written"))
    dims = sum(
        1
        for r in profiling_results.values()
        if r.get("table_type") == "dimension" and r.get("written")
    )
    facts = sum(
        1
        for r in profiling_results.values()
        if r.get("table_type") == "fact" and r.get("written")
    )
    total_rows = sum(
        r.get("row_count", 0) for r in profiling_results.values() if r.get("written")
    )

    logging.info(
        "Gold layer: %d tables (%d dim + %d fact), %s total rows",
        written,
        dims,
        facts,
        f"{total_rows:,}",
    )
    return written


with DAG(
    dag_id="silver_to_gold_pipeline",
    description="Silver → Gold star schema (IKU) — konsumsi Trino/Superset",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "aqe", "iceberg", "gold", "star-schema", "kpi", "superset", "pipeline"],
) as dag:

    spark_etl = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_spark_silver_to_gold,
        execution_timeout=timedelta(hours=2),
    )

    spark_etl
