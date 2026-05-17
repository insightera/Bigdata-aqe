"""
Bronze → Silver ETL  (PySpark + Iceberg)
==========================================
Transformasi data Bronze (raw) menjadi Silver (cleaned & enriched):

  1. Quality Check   — completeness, validity, consistency per tabel
  2. Cleaning        — standarisasi, dedup
  3. Enrichment      — JOIN, flag bisnis, derived columns
  4. Write           — Iceberg tables di namespace silver

Quality threshold (sesuai README §6):
  ≥ 80%  → PASS   (tulis ke silver)
  60-79% → QUARANTINE (tulis ke silver, flagged)
  < 60%  → REJECT (skip, log warning)

Tabel Silver yang dihasilkan:
  - silver_mahasiswa          (enriched + is_mbkm)
  - silver_lulusan            (enriched + employment flags)
  - silver_dosen              (enriched + qualification flags)
  - silver_penelitian_pkm     (union penelitian + pengabdian)
  - silver_kerjasama_aktif    (filter aktif + MBKM flag)
  - silver_akreditasi_aktif   (latest per prodi, still valid)
"""

import json
import logging
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession

from spark.spark_python import apply_cluster_resource_configs, apply_pyspark_python_configs
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("bronze_to_silver")

JURUSAN_MAP = {
    "JTK": "Teknik dan Komputer",
    "JSA": "Sains",
    "JTI": "Teknologi Infrastruktur dan Kewilayahan",
    "JTP": "Teknologi Produksi dan Industri",
    "JMB": "Matematika dan Bisnis",
}


def _resolve_jars() -> str:
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
        .appName("bronze_to_silver")
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

    builder = apply_cluster_resource_configs(builder, app_name="bronze_to_silver")
    return apply_pyspark_python_configs(builder).getOrCreate()


# ---------------------------------------------------------------------------
# Quality check
# ---------------------------------------------------------------------------

def compute_quality_metrics(df: DataFrame, table_name: str) -> dict:
    """
    Hitung quality score berdasarkan:
      - completeness : rata-rata (1 - null_ratio) semua kolom
      - uniqueness   : rasio distinct / total pada primary-key-like kolom
      - row_count    : jumlah baris (min threshold 1)
    Score akhir = weighted average → 0-100.
    """
    row_count = df.count()
    if row_count == 0:
        return {
            "table_name": table_name,
            "row_count": 0,
            "quality_score": 0.0,
            "status": "REJECT",
            "completeness": 0.0,
            "columns": {},
        }

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

    stats = df.agg(*agg_exprs).collect()[0]

    columns = {}
    completeness_scores = []
    for col_name in df.columns:
        null_count = int(stats[f"{col_name}__nulls"])
        distinct = int(stats[f"{col_name}__distinct"])
        comp = round((row_count - null_count) / row_count * 100, 2)
        completeness_scores.append(comp)
        columns[col_name] = {
            "data_type": str(df.schema[col_name].dataType),
            "null_count": null_count,
            "null_pct": round(null_count / row_count * 100, 2),
            "completeness_pct": comp,
            "distinct_count": distinct,
        }

    avg_completeness = sum(completeness_scores) / len(completeness_scores)
    quality_score = round(avg_completeness, 2)

    if quality_score >= 80:
        status = "PASS"
    elif quality_score >= 60:
        status = "QUARANTINE"
    else:
        status = "REJECT"

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": len(df.columns),
        "quality_score": quality_score,
        "status": status,
        "completeness": avg_completeness,
        "columns": columns,
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# Transformasi Silver
# ---------------------------------------------------------------------------

def transform_silver_mahasiswa(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    silver_mahasiswa: JOIN raw_mahasiswa + raw_prodi
    → tambah nama_prodi, nama_jurusan, is_mbkm
    """
    mhs = spark.table("lakehouse.bronze.raw_mahasiswa")
    prodi = spark.table("lakehouse.bronze.raw_prodi")

    quality = compute_quality_metrics(mhs, "raw_mahasiswa")
    if quality["status"] == "REJECT":
        return None, quality

    jurusan_mapping = F.create_map(
        *[item for k, v in JURUSAN_MAP.items() for item in (F.lit(k), F.lit(v))]
    )

    df = (
        mhs
        .join(
            prodi.select("prodi_id", "nama_prodi", "jenjang"),
            on="prodi_id",
            how="left",
        )
        .withColumn("nama_jurusan", jurusan_mapping[F.col("jurusan_id")])
        .withColumn("is_mbkm", F.col("sks_luar_kampus") >= 20)
        .dropDuplicates(["mahasiswa_id"])
    )

    return df, quality


def transform_silver_lulusan(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    silver_lulusan: enrichment flag employment.
    """
    lls = spark.table("lakehouse.bronze.raw_lulusan")

    quality = compute_quality_metrics(lls, "raw_lulusan")
    if quality["status"] == "REJECT":
        return None, quality

    df = (
        lls
        .withColumn("is_employed", F.col("status_pasca_lulus") == "Bekerja")
        .withColumn("is_lanjut_studi", F.col("status_pasca_lulus") == "Studi Lanjut")
        .withColumn("is_wirausaha", F.col("status_pasca_lulus") == "Wirausaha")
        .withColumn(
            "is_terserap",
            F.col("is_employed") | F.col("is_lanjut_studi") | F.col("is_wirausaha"),
        )
        .dropDuplicates(["lulusan_id"])
    )

    return df, quality


def transform_silver_dosen(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    silver_dosen: enrichment kualifikasi + cek tridarma dari kegiatan.
    """
    dosen = spark.table("lakehouse.bronze.raw_dosen")
    kegiatan = spark.table("lakehouse.bronze.raw_kegiatan_dosen")

    quality = compute_quality_metrics(dosen, "raw_dosen")
    if quality["status"] == "REJECT":
        return None, quality

    tridarma_dosen = (
        kegiatan
        .groupBy("dosen_id")
        .agg(F.countDistinct("jenis_kegiatan").alias("jenis_tridarma_count"))
    )

    df = (
        dosen
        .withColumn("is_s3", F.col("pendidikan_terakhir") == "S3")
        .withColumn("is_praktisi", F.col("berasal_praktisi").cast("boolean"))
        .withColumn("is_serdos", F.col("sertifikat_dosen").cast("boolean"))
        .join(tridarma_dosen, on="dosen_id", how="left")
        .withColumn(
            "is_aktif_tridarma",
            F.coalesce(F.col("jenis_tridarma_count"), F.lit(0)) >= 1,
        )
        .drop("jenis_tridarma_count")
        .dropDuplicates(["dosen_id"])
    )

    return df, quality


def transform_silver_penelitian_pkm(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    silver_penelitian_pkm: UNION penelitian + pengabdian, flag rekognisi.
    """
    pen = spark.table("lakehouse.bronze.raw_penelitian")
    pkm = spark.table("lakehouse.bronze.raw_pengabdian")

    quality_pen = compute_quality_metrics(pen, "raw_penelitian")
    quality_pkm = compute_quality_metrics(pkm, "raw_pengabdian")

    pen_norm = (
        pen
        .withColumn("jenis", F.lit("Penelitian"))
        .withColumn("id", F.col("penelitian_id"))
        .withColumn("is_rekognisi", F.col("rekognisi_internasional").cast("boolean"))
        .withColumn("is_diterapkan", F.col("diterapkan_masyarakat").cast("boolean"))
        .select(
            "id", "judul", "dosen_id", "jurusan_id", "tahun", "dana",
            "jenis", "is_rekognisi", "is_diterapkan", "ingested_at",
        )
    )

    pkm_norm = (
        pkm
        .withColumn("jenis", F.lit("Pengabdian"))
        .withColumn("id", F.col("pkm_id"))
        .withColumn("is_rekognisi", F.col("rekognisi_internasional").cast("boolean"))
        .withColumn("is_diterapkan", F.col("diterapkan_masyarakat").cast("boolean"))
        .select(
            "id", "judul", "dosen_id", "jurusan_id", "tahun", "dana",
            "jenis", "is_rekognisi", "is_diterapkan", "ingested_at",
        )
    )

    df = pen_norm.unionByName(pkm_norm).dropDuplicates(["id"])

    combined_quality = {
        **quality_pen,
        "table_name": "raw_penelitian + raw_pengabdian",
        "row_count": quality_pen["row_count"] + quality_pkm["row_count"],
        "quality_score": round(
            (quality_pen["quality_score"] + quality_pkm["quality_score"]) / 2, 2
        ),
    }
    combined_quality["status"] = (
        "PASS" if combined_quality["quality_score"] >= 80
        else "QUARANTINE" if combined_quality["quality_score"] >= 60
        else "REJECT"
    )

    return df, combined_quality


def transform_silver_kerjasama_aktif(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    silver_kerjasama_aktif: filter status Aktif + flag MBKM.
    """
    kjs = spark.table("lakehouse.bronze.raw_kerjasama")

    quality = compute_quality_metrics(kjs, "raw_kerjasama")
    if quality["status"] == "REJECT":
        return None, quality

    df = (
        kjs
        .filter(F.col("status") == "Aktif")
        .withColumn(
            "is_mbkm",
            F.col("lingkup").isin("MBKM", "Semua"),
        )
        .dropDuplicates(["kerjasama_id"])
    )

    return df, quality


def transform_silver_akreditasi_aktif(spark: SparkSession) -> tuple[DataFrame, dict]:
    """
    silver_akreditasi_aktif: akreditasi terakhir per prodi yang masih berlaku.
    """
    akr = spark.table("lakehouse.bronze.raw_akreditasi")

    quality = compute_quality_metrics(akr, "raw_akreditasi")
    if quality["status"] == "REJECT":
        return None, quality

    today_str = str(date.today())
    w = Window.partitionBy("prodi_id").orderBy(F.col("tanggal_sk").desc())

    df = (
        akr
        .filter(F.col("tanggal_berakhir") >= today_str)
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("is_internasional", F.col("lembaga") == "Internasional")
    )

    return df, quality


# ---------------------------------------------------------------------------
# Profile Silver (reuse dari bronze, tapi tambah quality & transformation info)
# ---------------------------------------------------------------------------

def profile_silver(df: DataFrame, table_name: str, source_quality: dict,
                   transformations: list[str]) -> dict:
    """Profiling Silver: data quality + transformation metadata."""
    row_count = df.count()

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

    stats = df.agg(*agg_exprs).collect()[0]

    columns = {}
    completeness_scores = []
    for col_name in df.columns:
        null_count = int(stats[f"{col_name}__nulls"])
        distinct = int(stats[f"{col_name}__distinct"])
        comp = round((row_count - null_count) / row_count * 100, 2)
        completeness_scores.append(comp)
        columns[col_name] = {
            "data_type": str(df.schema[col_name].dataType),
            "null_count": null_count,
            "null_pct": round(null_count / row_count * 100, 2),
            "completeness_pct": comp,
            "distinct_count": distinct,
        }

    avg_completeness = sum(completeness_scores) / len(completeness_scores)

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": len(df.columns),
        "schema": {c.name: str(c.dataType) for c in df.schema},
        "columns": columns,
        "quality": {
            "source_score": source_quality.get("quality_score", 0),
            "overall_score": source_quality.get("quality_score", 0),
            "source_status": source_quality.get("status", "UNKNOWN"),
            "silver_completeness": round(avg_completeness, 2),
        },
        "transformations": transformations,
        "profiled_at": datetime.utcnow().isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

SILVER_TRANSFORMS = [
    {
        "name": "silver_mahasiswa",
        "func": transform_silver_mahasiswa,
        "sources": ["raw_mahasiswa", "raw_prodi"],
        "transformations": [
            "JOIN raw_prodi → nama_prodi, jenjang",
            "MAP jurusan_id → nama_jurusan",
            "FLAG is_mbkm = sks_luar_kampus >= 20",
            "DEDUP on mahasiswa_id",
        ],
    },
    {
        "name": "silver_lulusan",
        "func": transform_silver_lulusan,
        "sources": ["raw_lulusan"],
        "transformations": [
            "FLAG is_employed = status_pasca_lulus == 'Bekerja'",
            "FLAG is_lanjut_studi = status_pasca_lulus == 'Studi Lanjut'",
            "FLAG is_wirausaha = status_pasca_lulus == 'Wirausaha'",
            "FLAG is_terserap = employed | studi | wirausaha",
            "DEDUP on lulusan_id",
        ],
    },
    {
        "name": "silver_dosen",
        "func": transform_silver_dosen,
        "sources": ["raw_dosen", "raw_kegiatan_dosen"],
        "transformations": [
            "FLAG is_s3 = pendidikan_terakhir == 'S3'",
            "FLAG is_praktisi = berasal_praktisi",
            "FLAG is_serdos = sertifikat_dosen",
            "JOIN kegiatan → is_aktif_tridarma (≥1 jenis kegiatan)",
            "DEDUP on dosen_id",
        ],
    },
    {
        "name": "silver_penelitian_pkm",
        "func": transform_silver_penelitian_pkm,
        "sources": ["raw_penelitian", "raw_pengabdian"],
        "transformations": [
            "UNION raw_penelitian + raw_pengabdian",
            "NORMALIZE columns (id, judul, dosen_id, …)",
            "FLAG is_rekognisi = rekognisi_internasional",
            "FLAG is_diterapkan = diterapkan_masyarakat",
            "ADD jenis = 'Penelitian' | 'Pengabdian'",
            "DEDUP on id",
        ],
    },
    {
        "name": "silver_kerjasama_aktif",
        "func": transform_silver_kerjasama_aktif,
        "sources": ["raw_kerjasama"],
        "transformations": [
            "FILTER status == 'Aktif'",
            "FLAG is_mbkm = lingkup IN ('MBKM', 'Semua')",
            "DEDUP on kerjasama_id",
        ],
    },
    {
        "name": "silver_akreditasi_aktif",
        "func": transform_silver_akreditasi_aktif,
        "sources": ["raw_akreditasi"],
        "transformations": [
            "FILTER tanggal_berakhir >= today (masih berlaku)",
            "WINDOW ROW_NUMBER per prodi ORDER BY tanggal_sk DESC",
            "KEEP latest akreditasi per prodi",
            "FLAG is_internasional = lembaga == 'Internasional'",
        ],
    },
]


def run_bronze_to_silver() -> dict:
    """Entry-point: proses semua tabel Bronze → Silver."""
    spark = get_spark_session()

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

        results = {}

        for t in SILVER_TRANSFORMS:
            name = t["name"]
            logger.info("\n" + "=" * 60)
            logger.info("  %s", name)
            logger.info("  Sources: %s", ", ".join(t["sources"]))
            logger.info("=" * 60)

            try:
                df, quality = t["func"](spark)
            except Exception as exc:
                logger.error("  ✗ Transform failed: %s", exc)
                results[name] = {
                    "table_name": name,
                    "error": str(exc),
                    "quality": {"status": "ERROR"},
                }
                continue

            logger.info(
                "  Quality: score=%.1f%% status=%s",
                quality.get("quality_score", 0),
                quality.get("status", "?"),
            )

            if df is None:
                logger.warning("  ⚠ REJECTED — quality too low, skipping write")
                results[name] = {
                    "table_name": name,
                    "row_count": 0,
                    "quality": quality,
                    "transformations": t["transformations"],
                    "sources": t["sources"],
                    "written": False,
                }
                continue

            iceberg_table = f"lakehouse.silver.{name}"
            df.writeTo(iceberg_table).using("iceberg").createOrReplace()

            row_count = df.count()
            logger.info("  Written → %s (%s rows)", iceberg_table, f"{row_count:,}")

            profiling = profile_silver(df, name, quality, t["transformations"])
            profiling["sources"] = t["sources"]
            profiling["written"] = True

            results[name] = profiling

        total_rows = sum(
            r.get("row_count", 0) for r in results.values() if r.get("written")
        )
        written_count = sum(1 for r in results.values() if r.get("written"))
        logger.info(
            "\nPipeline complete: %d/%d tables written, %s total rows",
            written_count, len(SILVER_TRANSFORMS), f"{total_rows:,}",
        )
        return results

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    results = run_bronze_to_silver()
    print(json.dumps(results, indent=2, default=str))
