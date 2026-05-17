"""
Adaptive Query Execution (AQE) — konfigurasi Spark terpusat.

Skenario:
  OFF — baseline (rencana statis, tanpa re-optimasi runtime)
  ON  — AQE aktif (coalescing, skew join, local shuffle reader)

Prioritas penentuan skenario:
  1. Argumen eksplisit (run_bronze_to_silver(aqe_scenario=...))
  2. Variabel lingkungan SPARK_AQE_SCENARIO
  3. Default OFF
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("aqe_config")

VALID_SCENARIOS = frozenset({"OFF", "ON"})

# Kunci konfigurasi yang dicatat ke file metrik / log eksperimen
AQE_SPARK_KEYS = (
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.localShuffleReader.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.shuffle.partitions",
)


def resolve_aqe_scenario(scenario: str | None = None) -> str:
    """Normalisasi skenario ke OFF atau ON."""
    raw = (scenario or os.environ.get("SPARK_AQE_SCENARIO") or "OFF").strip().upper()
    if raw in ("1", "TRUE", "YES", "ENABLE", "ENABLED"):
        return "ON"
    if raw in ("0", "FALSE", "NO", "DISABLE", "DISABLED"):
        return "OFF"
    if raw not in VALID_SCENARIOS:
        logger.warning("Unknown AQE scenario %r — using OFF", raw)
        return "OFF"
    return raw


def _shuffle_partitions() -> str:
    return os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200")


def aqe_spark_configs(scenario: str | None = None) -> dict[str, str]:
    """Map konfigurasi Spark untuk skenario AQE."""
    sc = resolve_aqe_scenario(scenario)
    shuffle = _shuffle_partitions()

    if sc == "ON":
        return {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": os.environ.get(
                "SPARK_AQE_ADVISORY_PARTITION_SIZE", "64MB"
            ),
            "spark.sql.adaptive.coalescePartitions.minPartitionSize": os.environ.get(
                "SPARK_AQE_MIN_PARTITION_SIZE", "1MB"
            ),
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": os.environ.get(
                "SPARK_AQE_SKEW_PARTITION_FACTOR", "5"
            ),
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": os.environ.get(
                "SPARK_AQE_SKEW_THRESHOLD", "256MB"
            ),
            "spark.sql.shuffle.partitions": shuffle,
        }

    return {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false",
        "spark.sql.adaptive.localShuffleReader.enabled": "false",
        "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled": "false",
        "spark.sql.shuffle.partitions": shuffle,
    }


def apply_aqe_configs(builder, scenario: str | None = None):
    """Terapkan konfigurasi AQE ke SparkSession.Builder."""
    sc = resolve_aqe_scenario(scenario)
    configs = aqe_spark_configs(sc)
    for key, value in configs.items():
        builder = builder.config(key, value)
    logger.info(
        "AQE scenario=%s | adaptive.enabled=%s | shuffle.partitions=%s",
        sc,
        configs["spark.sql.adaptive.enabled"],
        configs["spark.sql.shuffle.partitions"],
    )
    return builder


def app_name_with_aqe(base: str, scenario: str | None = None) -> str:
    sc = resolve_aqe_scenario(scenario)
    safe = re.sub(r"[^a-zA-Z0-9_]+", "_", base)
    return f"{safe}_AQE_{sc}"


def read_applied_aqe_configs(spark) -> dict[str, str]:
    """Baca konfigurasi efektif dari SparkContext (setelah session aktif)."""
    conf = spark.sparkContext.getConf()
    out: dict[str, str] = {}
    for key in AQE_SPARK_KEYS:
        val = conf.get(key, None)
        if val is not None:
            out[key] = val
    out["spark.app.name"] = spark.sparkContext.appName
    return out


def persist_aqe_run_metrics(
    *,
    pipeline: str,
    scenario: str,
    results: dict[str, Any],
    spark_configs: dict[str, str],
    started_at: datetime,
    ended_at: datetime,
    metrics_dir: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    """Tulis hasil run ke JSON untuk analisis OFF vs ON."""
    from spark.pipeline_metrics import persist_pipeline_run_metrics

    sc = resolve_aqe_scenario(scenario)
    path = persist_pipeline_run_metrics(
        pipeline=f"{pipeline}_aqe",
        results=results,
        started_at=started_at,
        ended_at=ended_at,
        metrics_dir_path=metrics_dir,
        scenario=sc,
        spark_configs=spark_configs,
        extra=extra,
    )
    logger.info("AQE metrics written → %s", path)
    return path


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
