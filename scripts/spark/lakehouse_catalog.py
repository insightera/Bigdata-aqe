"""
Penamaan katalog Iceberg / Hive & path MinIO untuk eksperimen AQE.

Bronze (satu salinan, input bersama):
  lakehouse.bronze.*  →  s3a://warehouse/

Silver & Gold per skenario (dua salinan untuk audit SQL):
  lakehouse.silver_aqe_off.* / lakehouse.gold_aqe_off.*  →  s3a://warehouse-aqe-off/
  lakehouse.silver_aqe_on.*  / lakehouse.gold_aqe_on.*   →  s3a://warehouse-aqe-on/

Trino: katalog lakehouse (semua schema) atau lakehouse_aqe_off / lakehouse_aqe_on
       (metastore sama; gunakan schema silver_aqe_* / gold_aqe_*).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from spark.aqe_config import resolve_aqe_scenario

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.session import SparkSession

SHARED_CATALOG = "lakehouse"
BRONZE_SCHEMA = "bronze"

WAREHOUSE_BRONZE = os.environ.get("LAKEHOUSE_WAREHOUSE", "s3a://warehouse/")
WAREHOUSE_AQE_OFF = os.environ.get("LAKEHOUSE_WAREHOUSE_AQE_OFF", "s3a://warehouse-aqe-off/")
WAREHOUSE_AQE_ON = os.environ.get("LAKEHOUSE_WAREHOUSE_AQE_ON", "s3a://warehouse-aqe-on/")

TRINO_CATALOG_SHARED = os.environ.get("TRINO_CATALOG", "lakehouse")
TRINO_CATALOG_OFF = os.environ.get("TRINO_CATALOG_AQE_OFF", "lakehouse_aqe_off")
TRINO_CATALOG_ON = os.environ.get("TRINO_CATALOG_AQE_ON", "lakehouse_aqe_on")

HIVE_METASTORE_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")


def _norm_warehouse(path: str) -> str:
    return path if path.endswith("/") else f"{path}/"


def aqe_warehouse(scenario: str | None) -> str:
    sc = resolve_aqe_scenario(scenario)
    if sc == "ON":
        return _norm_warehouse(WAREHOUSE_AQE_ON)
    return _norm_warehouse(WAREHOUSE_AQE_OFF)


def silver_schema(scenario: str | None) -> str:
    return f"silver_aqe_{resolve_aqe_scenario(scenario).lower()}"


def gold_schema(scenario: str | None) -> str:
    return f"gold_aqe_{resolve_aqe_scenario(scenario).lower()}"


def bronze_table(table: str) -> str:
    return f"{SHARED_CATALOG}.{BRONZE_SCHEMA}.{table}"


def silver_table(scenario: str | None, table: str) -> str:
    return f"{SHARED_CATALOG}.{silver_schema(scenario)}.{table}"


def gold_table(scenario: str | None, table: str) -> str:
    return f"{SHARED_CATALOG}.{gold_schema(scenario)}.{table}"


def trino_catalog_for_scenario(scenario: str | None) -> str:
    sc = resolve_aqe_scenario(scenario)
    return TRINO_CATALOG_ON if sc == "ON" else TRINO_CATALOG_OFF


def trino_gold_schema(scenario: str | None) -> str:
    return gold_schema(scenario)


def table_location(scenario: str | None, layer: str, table: str) -> str:
    """Path fisik di MinIO (bucket terpisah per skenario AQE)."""
    base = aqe_warehouse(scenario)
    schema = silver_schema(scenario) if layer == "silver" else gold_schema(scenario)
    return f"{base}{schema}/{table}"


def ensure_namespace(spark: SparkSession, scenario: str | None, layer: str) -> None:
    schema = silver_schema(scenario) if layer == "silver" else gold_schema(scenario)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SHARED_CATALOG}.{schema}")


def write_iceberg_table(
    df: DataFrame,
    scenario: str | None,
    layer: str,
    table: str,
) -> str:
    """Tulis/replace tabel Iceberg; return FQN."""
    ensure_namespace(df.sparkSession, scenario, layer)
    fqn = silver_table(scenario, table) if layer == "silver" else gold_table(scenario, table)
    location = table_location(scenario, layer, table)
    (
        df.writeTo(fqn)
        .using("iceberg")
        .tableProperty("location", location)
        .createOrReplace()
    )
    return fqn


def apply_hadoop_s3a(builder):
    return (
        builder.config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3A_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("S3A_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("S3A_SECRET_KEY", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )


def configure_spark_catalog(builder, scenario: str | None = None):
    """Katalog lakehouse (bronze + semua schema AQE di HMS)."""
    builder = (
        builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SHARED_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SHARED_CATALOG}.type", "hive")
        .config(f"spark.sql.catalog.{SHARED_CATALOG}.uri", HIVE_METASTORE_URI)
        .config(f"spark.sql.catalog.{SHARED_CATALOG}.warehouse", _norm_warehouse(WAREHOUSE_BRONZE))
        .config("spark.sql.defaultCatalog", SHARED_CATALOG)
    )
    return apply_hadoop_s3a(builder)
