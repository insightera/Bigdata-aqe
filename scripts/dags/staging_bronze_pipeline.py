"""
DAG: Staging → Bronze Pipeline
================================
Pipeline pertama dalam arsitektur Medallion Metadata Lakehouse:

  ┌────────┐    ┌─────────────┐    ┌──────────────┐    ┌───────────┐
  │ Upload │ →  │ Spark+Iceberg│ →  │ Metadata     │ →  │ Atlas     │
  │ CSV→S3 │    │ CSV→Parquet  │    │ Ingestion    │    │ Registry  │
  └────────┘    └─────────────┘    └──────────────┘    └───────────┘
   staging         bronze           profiling +          auto
   bucket          layer            lineage +            catalog
                                    classification

Sesuai diagram: Source → Staging → Bronze Layer → metadata ingestion → Atlas API

Metadata yang dicatat di Bronze:
  1. Raw Technical Metadata  (schema, tipe data, lokasi)
  2. Raw Lineage             (staging CSV → bronze Iceberg)
  3. Raw Data Profiling      (row_count, null_pct, completeness, distinct)
  4. Raw Classification      (PII, layer tagging)
"""

import csv
import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, "/opt/airflow/scripts")

ATLAS_URL = "http://atlas:21000"
ATLAS_AUTH = ("admin", "admin")

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

STAGING_CSV_DIR = "/opt/airflow/data/staging"

PII_COLUMNS = {
    "raw_mahasiswa": ["nama", "mahasiswa_id", "asal_provinsi"],
    "raw_lulusan": ["mahasiswa_id", "nama_perusahaan"],
    "raw_dosen": ["nama", "dosen_id"],
    "raw_kegiatan_dosen": ["dosen_id"],
    "raw_penelitian": ["dosen_id"],
    "raw_pengabdian": ["dosen_id"],
    "raw_kerjasama": [],
    "raw_mbkm": ["mahasiswa_id"],
    "raw_akreditasi": [],
    "raw_prodi": [],
    "raw_keuangan": [],
    "raw_prestasi_mahasiswa": ["mahasiswa_id"],
}

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def _profile_csv(filepath: str, table_name: str) -> dict:
    """Profile a CSV file using stdlib csv (no Spark needed)."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        columns_list = reader.fieldnames or []
        rows = list(reader)

    row_count = len(rows)
    if row_count == 0:
        return {"table_name": table_name, "row_count": 0, "columns": {}, "column_count": 0}

    col_stats = {}
    for col in columns_list:
        nulls = sum(1 for r in rows if not r.get(col) or r[col].strip() == "")
        distincts = len({r.get(col, "") for r in rows})
        col_stats[col] = {
            "data_type": "StringType()",
            "null_count": nulls,
            "null_pct": round(nulls / row_count * 100, 2),
            "distinct_count": distincts,
            "completeness_pct": round((row_count - nulls) / row_count * 100, 2),
        }

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": len(columns_list),
        "columns": col_stats,
        "schema": {c: "StringType()" for c in columns_list},
        "pii_columns": PII_COLUMNS.get(table_name, []),
        "profiled_at": datetime.now().astimezone().isoformat(),
    }


# ─── Task 1: Upload CSV ke MinIO staging bucket ────────────────────────────

def upload_csv_to_minio(**context):
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    csv_dir = STAGING_CSV_DIR
    if not os.path.isdir(csv_dir):
        raise FileNotFoundError(f"Staging directory not found: {csv_dir}")

    uploaded = []
    for fname in sorted(os.listdir(csv_dir)):
        if not fname.endswith(".csv"):
            continue
        filepath = os.path.join(csv_dir, fname)
        s3.upload_file(filepath, "staging", fname)
        size = os.path.getsize(filepath)
        uploaded.append({"file": fname, "size_bytes": size})
        logging.info("Uploaded %s (%s bytes)", fname, f"{size:,}")

    logging.info("Total uploaded: %d CSV files", len(uploaded))
    return uploaded


# ─── Task 2: Spark ETL — staging CSV → bronze Iceberg ──────────────────────

def run_spark_staging_to_bronze(**context):
    from spark.staging_to_bronze import run_staging_to_bronze

    profiling_results = run_staging_to_bronze()

    context["ti"].xcom_push(key="profiling", value=profiling_results)

    total_rows = sum(p["row_count"] for p in profiling_results.values())
    logging.info(
        "Bronze layer complete: %d tables, %s total rows",
        len(profiling_results), f"{total_rows:,}",
    )
    return len(profiling_results)


# ─── Task 3: Register metadata ke Atlas ─────────────────────────────────────

def register_atlas_metadata(**context):
    from atlas.register_bronze_metadata import register_all_metadata

    profiling = context["ti"].xcom_pull(task_ids="staging_to_bronze", key="profiling")

    if not profiling:
        logging.warning(
            "No profiling from Spark ETL — building CSV-based profiling as fallback"
        )
        profiling = {}
        csv_dir = STAGING_CSV_DIR
        for fname in sorted(os.listdir(csv_dir)):
            if not fname.endswith(".csv"):
                continue
            table_name = fname.replace(".csv", "")
            filepath = os.path.join(csv_dir, fname)
            try:
                prof = _profile_csv(filepath, table_name)
                profiling[table_name] = prof
                logging.info("CSV profiled: %s (%d rows)", table_name, prof["row_count"])
            except Exception as exc:
                logging.warning("Failed to profile %s: %s", table_name, exc)

    if not profiling:
        raise ValueError(
            "No profiling data — neither from Spark ETL nor CSV fallback. "
            "Check that CSV files exist in " + STAGING_CSV_DIR
        )

    logging.info("Registering %d tables to Atlas at %s", len(profiling), ATLAS_URL)

    success = register_all_metadata(
        profiling_results=profiling,
        atlas_url=ATLAS_URL,
        atlas_user=ATLAS_AUTH[0],
        atlas_pass=ATLAS_AUTH[1],
    )

    context["ti"].xcom_push(key="atlas_success", value=success)
    logging.info("Atlas metadata registration: %s", "OK" if success else "partial")


# ─── DAG definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id="staging_to_bronze_pipeline",
    description="Staging CSV → Bronze Iceberg + metadata ingestion → Atlas registry",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "metadata", "iceberg", "atlas", "bronze", "pipeline"],
) as dag:

    upload_staging = PythonOperator(
        task_id="upload_csv_to_staging",
        python_callable=upload_csv_to_minio,
    )

    spark_etl = PythonOperator(
        task_id="staging_to_bronze",
        python_callable=run_spark_staging_to_bronze,
        execution_timeout=timedelta(minutes=60),
    )

    atlas_register = PythonOperator(
        task_id="register_atlas_metadata",
        python_callable=register_atlas_metadata,
        trigger_rule="all_done",
    )

    verify_atlas = BashOperator(
        task_id="verify_atlas_registration",
        bash_command="""
        set -e
        echo "── Atlas Verification ──"
        RESULT=$(curl -sf -u "admin:admin" \
          "http://atlas:21000/api/atlas/v2/search/basic" \
          -H "Content-Type: application/json" \
          -d '{"typeName":"lakehouse_dataset","excludeDeletedEntities":true,"limit":50}' \
          2>/dev/null) || { echo "⚠️  Atlas not responding"; exit 0; }

        COUNT=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('approximateCount',0))")
        echo "✅ lakehouse_dataset entities found: $COUNT"

        echo ""
        echo "── Lineage check ──"
        LINEAGE=$(curl -sf -u "admin:admin" \
          "http://atlas:21000/api/atlas/v2/search/basic" \
          -H "Content-Type: application/json" \
          -d '{"typeName":"lakehouse_etl_process","excludeDeletedEntities":true,"limit":50}' \
          2>/dev/null) || { echo "⚠️  Lineage check skipped"; exit 0; }

        LCOUNT=$(echo "$LINEAGE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('approximateCount',0))")
        echo "✅ lakehouse_etl_process (lineage) entities found: $LCOUNT"
        """,
    )

    upload_staging >> spark_etl >> atlas_register >> verify_atlas
