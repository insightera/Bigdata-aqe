"""
Staging → Bronze ETL  (PySpark + Iceberg)
==========================================
Membaca CSV dari MinIO staging bucket, menulis sebagai tabel Iceberg
di namespace Bronze, dan menghitung profiling metadata per tabel.

Bisa dijalankan:
  - Dari Airflow PythonOperator (import run_staging_to_bronze)
  - Langsung:  spark-submit staging_to_bronze.py
  - Dari Jupyter Notebook
"""

import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession

from spark.spark_python import apply_cluster_resource_configs, apply_pyspark_python_configs
from pyspark.sql import functions as F

logger = logging.getLogger("staging_to_bronze")

TABLES = [
    "raw_mahasiswa", "raw_lulusan", "raw_dosen", "raw_kegiatan_dosen",
    "raw_penelitian", "raw_pengabdian", "raw_kerjasama", "raw_mbkm",
    "raw_akreditasi", "raw_prodi", "raw_keuangan", "raw_prestasi_mahasiswa",
]

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


def _resolve_jars() -> str:
    """Return comma-separated JAR paths if pre-downloaded, else packages string."""
    import glob
    import os

    jars_dir = os.environ.get("SPARK_JARS_DIR", "/opt/spark-jars")
    jars = glob.glob(os.path.join(jars_dir, "*.jar"))
    if jars:
        logger.info("Using pre-downloaded JARs from %s (%d files)", jars_dir, len(jars))
        return ",".join(sorted(jars))
    return ""


def get_spark_session():
    import os
    import socket

    spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

    try:
        sock = socket.create_connection(("spark-master", 7077), timeout=5)
        sock.close()
        logger.info("Spark master reachable at %s", spark_master)
    except (OSError, socket.timeout):
        spark_master = "local[*]"
        logger.warning("Spark master unreachable — falling back to %s", spark_master)

    builder = (
        SparkSession.builder
        .appName("staging_to_bronze")
        .master(spark_master)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    local_jars = _resolve_jars()
    if local_jars:
        builder = builder.config("spark.jars", local_jars)
    else:
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )

    builder = apply_cluster_resource_configs(builder, app_name="staging_to_bronze")
    return apply_pyspark_python_configs(builder).getOrCreate()


def profile_dataframe(df, table_name: str) -> dict:
    """Profiling efisien — satu pass agregasi untuk semua kolom."""
    row_count = df.count()
    if row_count == 0:
        return {"table_name": table_name, "row_count": 0, "columns": {}}

    agg_exprs = []
    for col_name in df.columns:
        agg_exprs.append(
            F.sum(
                F.when(
                    F.col(col_name).isNull()
                    | (F.trim(F.col(col_name).cast("string")) == ""),
                    1,
                ).otherwise(0)
            ).alias(f"{col_name}__nulls")
        )
        agg_exprs.append(
            F.countDistinct(col_name).alias(f"{col_name}__distinct")
        )

    stats_row = df.agg(*agg_exprs).collect()[0]

    columns = {}
    for col_name in df.columns:
        null_count = int(stats_row[f"{col_name}__nulls"])
        distinct_count = int(stats_row[f"{col_name}__distinct"])
        columns[col_name] = {
            "data_type": str(df.schema[col_name].dataType),
            "null_count": null_count,
            "null_pct": round(null_count / row_count * 100, 2),
            "distinct_count": distinct_count,
            "completeness_pct": round((row_count - null_count) / row_count * 100, 2),
        }

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": len(df.columns),
        "columns": columns,
        "schema": {c.name: str(c.dataType) for c in df.schema},
        "pii_columns": PII_COLUMNS.get(table_name, []),
        "profiled_at": datetime.utcnow().isoformat() + "Z",
    }


def process_table(spark, table_name: str) -> dict | None:
    """Baca CSV staging → tulis Iceberg Bronze → return profiling."""
    csv_path = f"s3a://staging/{table_name}.csv"
    logger.info("Processing %s from %s", table_name, csv_path)

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(csv_path)
        )
        row_count = df.count()
    except Exception as exc:
        logger.warning("Skipping %s: %s", table_name, exc)
        return None

    if row_count == 0:
        logger.warning("Empty table: %s", table_name)
        return None

    logger.info("  %s: %s rows, %d columns", table_name, f"{row_count:,}", len(df.columns))

    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

    iceberg_table = f"lakehouse.bronze.{table_name}"
    df.writeTo(iceberg_table).using("iceberg").createOrReplace()
    logger.info("  Written → %s", iceberg_table)

    profiling = profile_dataframe(df, table_name)
    logger.info(
        "  Profiling done — %s rows, avg completeness %.1f%%",
        f"{profiling['row_count']:,}",
        sum(c["completeness_pct"] for c in profiling["columns"].values()) / max(len(profiling["columns"]), 1),
    )
    return profiling


def run_staging_to_bronze() -> dict:
    """Entry-point utama — proses semua tabel, return profiling dict."""
    spark = get_spark_session()
    try:
        results = {}
        for table_name in TABLES:
            profiling = process_table(spark, table_name)
            if profiling:
                results[table_name] = profiling

        total_rows = sum(p["row_count"] for p in results.values())
        logger.info(
            "Pipeline complete: %d tables, %s total rows",
            len(results), f"{total_rows:,}",
        )
        return results
    finally:
        try:
            spark.stop()
        except Exception as exc:
            logger.warning("spark.stop() skipped: %s", exc)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    results = run_staging_to_bronze()
    print(json.dumps(results, indent=2))
