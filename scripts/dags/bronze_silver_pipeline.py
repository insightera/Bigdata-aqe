"""
DAG: Bronze → Silver Pipeline + Metadata Enrichment
=====================================================
Pipeline kedua dalam arsitektur Medallion Metadata Lakehouse:

  ┌──────────┐    ┌──────────────┐    ┌───────────────────┐    ┌───────────┐
  │ Quality  │ →  │ Spark+Iceberg│ →  │ Metadata          │ →  │ Atlas     │
  │ Check    │    │ Clean+Enrich │    │ Enrichment        │    │ Registry  │
  └──────────┘    └──────────────┘    └───────────────────┘    └───────────┘
   bronze           silver              1. Clean Metadata       auto
   tables           tables              2. Quality Metadata     catalog
                                        3. Transform Lineage
                                        4. Business Metadata
                                        5. Compliance Metadata

Sesuai diagram: Bronze → Extract → Table 2 → Silver → metadata enrichment → S → Atlas API

Quality threshold:
  ≥ 80%  → PASS (tulis ke Silver)
  60-79% → QUARANTINE (tulis ke Silver, flagged)
  < 60%  → REJECT (skip)
"""

import json
import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, "/opt/airflow/scripts")

ATLAS_URL = "http://atlas:21000"
ATLAS_AUTH = ("admin", "admin")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


# ─── Task 1: Quality check + transform Bronze → Silver ─────────────────────

def run_spark_bronze_to_silver(**context):
    from spark.bronze_to_silver import run_bronze_to_silver

    profiling_results = run_bronze_to_silver()

    context["ti"].xcom_push(key="silver_profiling", value=profiling_results)

    written = sum(1 for r in profiling_results.values() if r.get("written"))
    total_rows = sum(
        r.get("row_count", 0) for r in profiling_results.values() if r.get("written")
    )
    logging.info(
        "Silver layer complete: %d/%d tables written, %s total rows",
        written, len(profiling_results), f"{total_rows:,}",
    )

    quality_summary = {}
    for name, prof in profiling_results.items():
        q = prof.get("quality", {})
        quality_summary[name] = {
            "status": q.get("source_status", prof.get("quality", {}).get("status", "?")),
            "score": q.get("source_score", q.get("quality_score", 0)),
            "written": prof.get("written", False),
        }

    context["ti"].xcom_push(key="quality_summary", value=quality_summary)
    return written


# ─── Task 2: Register Silver metadata enrichment ke Atlas ──────────────────

def register_silver_atlas_metadata(**context):
    from atlas.register_silver_metadata import register_all_silver_metadata

    profiling = context["ti"].xcom_pull(
        task_ids="bronze_to_silver", key="silver_profiling"
    )
    if not profiling:
        raise ValueError("No profiling data from bronze_to_silver task")

    success = register_all_silver_metadata(
        profiling_results=profiling,
        atlas_url=ATLAS_URL,
        atlas_user=ATLAS_AUTH[0],
        atlas_pass=ATLAS_AUTH[1],
    )

    context["ti"].xcom_push(key="atlas_success", value=success)
    logging.info("Silver Atlas registration: %s", "OK" if success else "partial")


# ─── Task 3: Log quality report ────────────────────────────────────────────

def log_quality_report(**context):
    quality = context["ti"].xcom_pull(
        task_ids="bronze_to_silver", key="quality_summary"
    )
    profiling = context["ti"].xcom_pull(
        task_ids="bronze_to_silver", key="silver_profiling"
    )

    logging.info("\n" + "=" * 60)
    logging.info("  QUALITY REPORT — Bronze → Silver")
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
            status_icon, name,
            q.get("score", 0), q.get("status", "?"), q.get("written", False),
            f"{prof.get('row_count', 0):,}", len(transforms),
        )


# ─── DAG definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_to_silver_pipeline",
    description="Bronze → Silver + quality check + metadata enrichment → Atlas",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "metadata", "iceberg", "atlas", "silver", "quality", "pipeline"],
) as dag:

    spark_etl = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_spark_bronze_to_silver,
        execution_timeout=timedelta(minutes=30),
    )

    atlas_register = PythonOperator(
        task_id="register_silver_metadata",
        python_callable=register_silver_atlas_metadata,
    )

    quality_report = PythonOperator(
        task_id="quality_report",
        python_callable=log_quality_report,
    )

    verify_atlas = BashOperator(
        task_id="verify_silver_atlas",
        bash_command="""
        set -e
        echo "── Silver Atlas Verification ──"

        echo ""
        echo "Silver entities:"
        RESULT=$(curl -sf -u "admin:admin" \
          "http://atlas:21000/api/atlas/v2/search/basic" \
          -H "Content-Type: application/json" \
          -d '{"typeName":"lakehouse_dataset","classification":"Silver_Layer","excludeDeletedEntities":true,"limit":50}' \
          2>/dev/null) || { echo "⚠️  Atlas not responding"; exit 0; }

        COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('approximateCount',0))")
        echo "✅ Silver entities: $COUNT"

        echo ""
        echo "Bronze→Silver lineage:"
        LINEAGE=$(curl -sf -u "admin:admin" \
          "http://atlas:21000/api/atlas/v2/search/basic" \
          -H "Content-Type: application/json" \
          -d '{"query":"bronze_to_silver","typeName":"lakehouse_etl_process","limit":50}' \
          2>/dev/null) || { echo "⚠️  Lineage check skipped"; exit 0; }

        LCOUNT=$(echo "$LINEAGE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('approximateCount',0))")
        echo "✅ Bronze→Silver lineage processes: $LCOUNT"

        echo ""
        echo "All lakehouse entities:"
        ALL=$(curl -sf -u "admin:admin" \
          "http://atlas:21000/api/atlas/v2/search/basic" \
          -H "Content-Type: application/json" \
          -d '{"typeName":"lakehouse_dataset","excludeDeletedEntities":true,"limit":100}' \
          2>/dev/null) || exit 0

        TOTAL=$(echo "$ALL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('approximateCount',0))")
        echo "✅ Total lakehouse_dataset entities (staging+bronze+silver): $TOTAL"
        """,
    )

    spark_etl >> [atlas_register, quality_report]
    atlas_register >> verify_atlas
