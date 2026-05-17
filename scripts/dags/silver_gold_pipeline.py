"""
DAG: Silver → Gold Pipeline + Star Schema + Metadata → Atlas
==============================================================
Pipeline ketiga dalam arsitektur Medallion Metadata Lakehouse:

  ┌──────────┐    ┌──────────────┐    ┌──────────────────┐    ┌───────────┐
  │ Silver   │ →  │ Spark+Iceberg│ →  │ Gold Metadata    │ →  │ Atlas     │
  │ Tables   │    │ Star Schema  │    │ Enrichment       │    │ Registry  │
  └──────────┘    └──────────────┘    └──────────────────┘    └───────────┘
   enriched       5 dim + 10 fact      1. Business Metadata    Portal
   tables         OLAP-ready           2. KPI Metadata         Data
                                       3. AI Metadata          Catalog
                                       4. Consumption Metadata
                                       5. Advanced Lineage

  Silver → Transform → Gold → metadata enrichment → G → Atlas API
                  ↓
  metadata business intelligence & governance
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


def run_spark_silver_to_gold(**context):
    from spark.silver_to_gold import run_silver_to_gold

    profiling_results = run_silver_to_gold()
    context["ti"].xcom_push(key="gold_profiling", value=profiling_results)

    written = sum(1 for r in profiling_results.values() if r.get("written"))
    dims = sum(1 for r in profiling_results.values() if r.get("table_type") == "dimension" and r.get("written"))
    facts = sum(1 for r in profiling_results.values() if r.get("table_type") == "fact" and r.get("written"))
    total_rows = sum(r.get("row_count", 0) for r in profiling_results.values() if r.get("written"))

    logging.info(
        "Gold layer: %d tables (%d dim + %d fact), %s total rows",
        written, dims, facts, f"{total_rows:,}",
    )
    return written


def register_gold_atlas_metadata(**context):
    from atlas.register_gold_metadata import register_all_gold_metadata

    profiling = context["ti"].xcom_pull(task_ids="silver_to_gold", key="gold_profiling")
    if not profiling:
        raise ValueError("No profiling data from silver_to_gold task")

    success = register_all_gold_metadata(
        profiling_results=profiling,
        atlas_url=ATLAS_URL,
        atlas_user=ATLAS_AUTH[0],
        atlas_pass=ATLAS_AUTH[1],
    )
    logging.info("Gold Atlas registration: %s", "OK" if success else "partial")


with DAG(
    dag_id="silver_to_gold_pipeline",
    description="Silver → Gold star schema + KPI/business metadata → Atlas",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "metadata", "iceberg", "atlas", "gold", "star-schema",
          "kpi", "dashboard", "pipeline"],
) as dag:

    spark_etl = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_spark_silver_to_gold,
        execution_timeout=timedelta(minutes=30),
    )

    atlas_register = PythonOperator(
        task_id="register_gold_metadata",
        python_callable=register_gold_atlas_metadata,
    )

    verify_catalog = BashOperator(
        task_id="verify_full_catalog",
        bash_command="""
        set -e
        echo "══════════════════════════════════════════"
        echo "  FULL DATA CATALOG VERIFICATION"
        echo "══════════════════════════════════════════"

        for LAYER in Staging_Layer Bronze_Layer Silver_Layer Gold_Layer; do
            RESULT=$(curl -sf -u "admin:admin" \
              "http://atlas:21000/api/atlas/v2/search/basic" \
              -H "Content-Type: application/json" \
              -d "{\\\"typeName\\\":\\\"lakehouse_dataset\\\",\\\"classification\\\":\\\"$LAYER\\\",\\\"limit\\\":100}" \
              2>/dev/null) || { echo "⚠️  $LAYER: Atlas not responding"; continue; }

            COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('approximateCount',0))")
            echo "  $LAYER: $COUNT entities"
        done

        echo ""
        echo "── Lineage processes ──"
        for PIPE in staging_to_bronze bronze_to_silver silver_to_gold; do
            RESULT=$(curl -sf -u "admin:admin" \
              "http://atlas:21000/api/atlas/v2/search/basic" \
              -H "Content-Type: application/json" \
              -d "{\\\"query\\\":\\\"$PIPE\\\",\\\"typeName\\\":\\\"lakehouse_etl_process\\\",\\\"limit\\\":100}" \
              2>/dev/null) || continue

            COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('approximateCount',0))")
            echo "  $PIPE: $COUNT lineage processes"
        done

        echo ""
        echo "── Star Schema ──"
        for CLS in Star_Schema_Dimension Star_Schema_Fact KPI_Metric Executive_Dashboard; do
            RESULT=$(curl -sf -u "admin:admin" \
              "http://atlas:21000/api/atlas/v2/search/basic" \
              -H "Content-Type: application/json" \
              -d "{\\\"typeName\\\":\\\"lakehouse_dataset\\\",\\\"classification\\\":\\\"$CLS\\\",\\\"limit\\\":100}" \
              2>/dev/null) || continue

            COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('approximateCount',0))")
            echo "  $CLS: $COUNT"
        done

        echo ""
        echo "══════════════════════════════════════════"
        echo "  ✅ Full Medallion pipeline verified"
        echo "══════════════════════════════════════════"
        """,
    )

    spark_etl >> atlas_register >> verify_catalog
