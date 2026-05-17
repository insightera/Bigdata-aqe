"""
DAG: Staging → Bronze Pipeline
================================
Medallion ingest: upload CSV ke MinIO → Spark/Iceberg ke layer Bronze.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

STAGING_CSV_DIR = "/opt/airflow/data/staging"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


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

    if not os.path.isdir(STAGING_CSV_DIR):
        raise FileNotFoundError(f"Staging directory not found: {STAGING_CSV_DIR}")

    uploaded = []
    for fname in sorted(os.listdir(STAGING_CSV_DIR)):
        if not fname.endswith(".csv"):
            continue
        filepath = os.path.join(STAGING_CSV_DIR, fname)
        s3.upload_file(filepath, "staging", fname)
        size = os.path.getsize(filepath)
        uploaded.append({"file": fname, "size_bytes": size})
        logging.info("Uploaded s3://staging/%s (%s bytes)", fname, f"{size:,}")

    context["ti"].xcom_push(key="uploaded_files", value=uploaded)
    logging.info("Total uploaded: %d CSV files", len(uploaded))
    return len(uploaded)


def run_spark_staging_to_bronze(**context):
    from spark.staging_to_bronze import run_staging_to_bronze

    profiling_results = run_staging_to_bronze()

    context["ti"].xcom_push(key="profiling", value=profiling_results)

    total_rows = sum(p["row_count"] for p in profiling_results.values())
    logging.info(
        "Bronze layer complete: %d tables, %s total rows",
        len(profiling_results),
        f"{total_rows:,}",
    )
    return len(profiling_results)


with DAG(
    dag_id="staging_to_bronze_pipeline",
    description="Staging CSV → Bronze Iceberg (Medallion ingest)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "aqe", "iceberg", "bronze", "pipeline"],
) as dag:

    upload_staging = PythonOperator(
        task_id="upload_csv_to_staging",
        python_callable=upload_csv_to_minio,
    )

    spark_etl = PythonOperator(
        task_id="staging_to_bronze",
        python_callable=run_spark_staging_to_bronze,
        execution_timeout=timedelta(hours=2),
    )

    upload_staging >> spark_etl
