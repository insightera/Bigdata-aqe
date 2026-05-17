"""
Silver → Gold ETL  (PySpark + Iceberg — Star Schema)
======================================================
Transformasi Silver (enriched) menjadi Gold (curated star schema)
untuk OLAP Dashboard Pimpinan ITERA.

Star Schema:
  5 Dimensi   : dim_waktu, dim_prodi, dim_dosen, dim_mahasiswa, dim_topik_penelitian
  10 Fakta IKU: fact_iku1..iku8 + fact_tata_kelola + fact_rekap_iku_institusi

Target IKU dari Renstra ITERA 2020-2024.
"""

import json
import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

from spark.aqe_config import app_name_with_aqe, apply_aqe_configs, resolve_aqe_scenario
from spark.lakehouse_catalog import (
    bronze_table,
    configure_spark_catalog,
    silver_table,
    write_iceberg_table,
)
from spark.spark_python import apply_cluster_resource_configs, apply_pyspark_python_configs

logger = logging.getLogger("silver_to_gold")

# Skenario AQE aktif untuk run_silver_to_gold (silver_aqe_off / silver_aqe_on).
_AQE_SCENARIO = "OFF"


def _bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.table(bronze_table(table))


def _silver(spark: SparkSession, table: str) -> DataFrame:
    fqn = silver_table(_AQE_SCENARIO, table)
    if not spark.catalog.tableExists(fqn):
        raise RuntimeError(
            f"Tabel {fqn} tidak ada — jalankan bronze_to_silver (AQE={_AQE_SCENARIO}) terlebih dahulu."
        )
    return spark.table(fqn)


def _year_from_col(column: str):
    """Parse tahun dari kolom tanggal/string."""
    return F.year(
        F.coalesce(
            F.to_date(F.col(column)),
            F.to_date(F.col(column), "yyyy-MM-dd"),
        )
    )


def _target_column(tahun_col: str, iku: str):
    """Target IKU per tahun tanpa Python UDF (lebih stabil di cluster)."""
    pairs = []
    for tahun, targets in IKU_TARGETS.items():
        val = targets.get(iku, 0)
        pairs.extend([F.lit(int(tahun)), F.lit(float(val))])
    mapping = F.create_map(*pairs)
    return mapping[F.col(tahun_col).cast("int")]

IKU_TARGETS = {
    2021: {"IKU-1": 75, "IKU-2": 20, "IKU-3": 15, "IKU-4": 30, "IKU-5": 0.10, "IKU-6": 35, "IKU-7": 25, "IKU-8": 2.5, "SAKIP": "BB", "Anggaran": 80},
    2022: {"IKU-1": 76, "IKU-2": 25, "IKU-3": 17, "IKU-4": 32, "IKU-5": 0.15, "IKU-6": 40, "IKU-7": 30, "IKU-8": 2.5, "SAKIP": "BB", "Anggaran": 85},
    2023: {"IKU-1": 77, "IKU-2": 30, "IKU-3": 20, "IKU-4": 40, "IKU-5": 0.20, "IKU-6": 50, "IKU-7": 35, "IKU-8": 2.5, "SAKIP": "BB", "Anggaran": 85},
    2024: {"IKU-1": 78, "IKU-2": 35, "IKU-3": 25, "IKU-4": 50, "IKU-5": 0.25, "IKU-6": 60, "IKU-7": 40, "IKU-8": 3.0, "SAKIP": "BB", "Anggaran": 85},
    2025: {"IKU-1": 80, "IKU-2": 40, "IKU-3": 30, "IKU-4": 55, "IKU-5": 0.30, "IKU-6": 65, "IKU-7": 45, "IKU-8": 5.0, "SAKIP": "A",  "Anggaran": 87},
}

BULAN = [
    "Januari", "Februari", "Maret", "April", "Mei", "Juni",
    "Juli", "Agustus", "September", "Oktober", "November", "Desember",
]

JURUSAN_MAP = {
    "JTK": "Teknik dan Komputer",
    "JSA": "Sains",
    "JTI": "Teknologi Infrastruktur dan Kewilayahan",
    "JTP": "Teknologi Produksi dan Industri",
    "JMB": "Matematika dan Bisnis",
}

TOPIK = [
    (1, "Sustainable Energy", "Energi terbarukan dan berkelanjutan"),
    (2, "Innovative Industry", "Industri inovatif dan teknologi terapan"),
    (3, "Green Infrastructure", "Infrastruktur hijau dan ramah lingkungan"),
    (4, "Community Development", "Pengembangan masyarakat dan wilayah"),
]


def _resolve_jars() -> str:
    import glob
    import os

    jars_dir = os.environ.get("SPARK_JARS_DIR", "/opt/spark-jars")
    jars = glob.glob(os.path.join(jars_dir, "*.jar"))
    if jars:
        logger.info("Using pre-downloaded JARs from %s (%d files)", jars_dir, len(jars))
        return ",".join(sorted(jars))
    return ""


def get_spark_session(aqe_context: str | None = None):
    import os
    import socket

    scenario = resolve_aqe_scenario(aqe_context)
    spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

    try:
        sock = socket.create_connection(("spark-master", 7077), timeout=5)
        sock.close()
        logger.info("Spark master reachable at %s", spark_master)
    except (OSError, socket.timeout):
        spark_master = "local[*]"
        logger.warning("Spark master unreachable — falling back to %s", spark_master)

    builder = configure_spark_catalog(
        SparkSession.builder
        .appName(app_name_with_aqe("silver_to_gold", scenario))
        .master(spark_master),
        scenario,
    )
    builder = apply_aqe_configs(builder, scenario)

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

    builder = apply_cluster_resource_configs(builder, app_name="silver_to_gold")
    return apply_pyspark_python_configs(builder).getOrCreate()


# ───────────────────────────────────────────────────────────────────────────
# DIMENSI
# ───────────────────────────────────────────────────────────────────────────

def build_dim_waktu(spark: SparkSession) -> DataFrame:
    rows = []
    sk = 1
    for tahun in range(2020, 2026):
        for bulan in range(1, 13):
            semester = "Ganjil" if bulan >= 8 or bulan <= 1 else "Genap"
            triwulan = (bulan - 1) // 3 + 1
            rows.append((sk, tahun, semester, triwulan, bulan, BULAN[bulan - 1]))
            sk += 1
    return spark.createDataFrame(rows, ["waktu_id", "tahun", "semester", "triwulan", "bulan", "nama_bulan"])


def build_dim_prodi(spark: SparkSession) -> DataFrame:
    prodi = _bronze(spark, "raw_prodi")
    jurusan_map = F.create_map(*[item for k, v in JURUSAN_MAP.items() for item in (F.lit(k), F.lit(v))])
    return (
        prodi
        .withColumn("nama_jurusan", jurusan_map[F.col("jurusan_id")])
        .withColumn("nama_fakultas", F.lit("ITERA"))
        .select("prodi_id", "nama_prodi", "jenjang", "nama_jurusan", "nama_fakultas", "tahun_berdiri", "status")
        .dropDuplicates(["prodi_id"])
    )


def build_dim_dosen(spark: SparkSession) -> DataFrame:
    return (
        _silver(spark, "silver_dosen")
        .select(
            "dosen_id", "nama", "prodi_id", "status_asn",
            F.col("pendidikan_terakhir").alias("pendidikan"),
            "jabatan_fungsional", "is_s3", "is_serdos", "is_praktisi",
        )
        .dropDuplicates(["dosen_id"])
    )


def build_dim_mahasiswa(spark: SparkSession) -> DataFrame:
    return (
        _silver(spark, "silver_mahasiswa")
        .select("mahasiswa_id", "prodi_id", "angkatan", "jalur_masuk", "asal_provinsi", "jenis_kelamin")
        .dropDuplicates(["mahasiswa_id"])
    )


def build_dim_topik_penelitian(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(TOPIK, ["topik_id", "nama_topik", "deskripsi"])


# ───────────────────────────────────────────────────────────────────────────
# FAKTA IKU
# ───────────────────────────────────────────────────────────────────────────

def _waktu_id_for_year(dim_waktu: DataFrame, tahun: int) -> int:
    """Ambil waktu_id untuk bulan Desember (akhir tahun) dari dim_waktu."""
    row = dim_waktu.filter((F.col("tahun") == tahun) & (F.col("bulan") == 12)).select("waktu_id").first()
    return row["waktu_id"] if row else tahun * 100 + 12


def _target(tahun: int, iku: str) -> float:
    return IKU_TARGETS.get(tahun, IKU_TARGETS[2024]).get(iku, 0)


def build_fact_iku1(spark: SparkSession, dim_waktu: DataFrame) -> DataFrame:
    """IKU-1: % lulusan bekerja/studi/wirausaha."""
    lls = _silver(spark, "silver_lulusan")

    agg = (
        lls
        .withColumn("tahun_lulus", _year_from_col("tanggal_lulus"))
        .filter(F.col("tahun_lulus").isNotNull() & F.col("prodi_id").isNotNull())
        .groupBy("tahun_lulus", "prodi_id")
        .agg(
            F.count("*").alias("total_lulusan"),
            F.sum(F.col("is_employed").cast("int")).alias("lulusan_bekerja"),
            F.sum(F.col("is_lanjut_studi").cast("int")).alias("lulusan_lanjut_studi"),
            F.sum(F.col("is_wirausaha").cast("int")).alias("lulusan_wirausaha"),
            F.sum((~F.col("is_terserap")).cast("int")).alias("lulusan_belum"),
        )
    )

    terserap = (
        F.col("lulusan_bekerja") + F.col("lulusan_lanjut_studi") + F.col("lulusan_wirausaha")
    )
    return (
        agg
        .withColumn(
            "persen_terserap",
            F.round(terserap / F.greatest(F.col("total_lulusan"), F.lit(1)) * 100, 2),
        )
        .withColumn("target_iku", _target_column("tahun_lulus", "IKU-1"))
        .withColumn(
            "capaian_iku",
            F.round(F.col("persen_terserap") / F.greatest(F.col("target_iku"), F.lit(1)) * 100, 2),
        )
        .withColumn("fact_id", F.monotonically_increasing_id())
        .withColumn("waktu_id", F.col("tahun_lulus").cast("int") * 100 + 12)
        .select(
            "fact_id", "waktu_id", "prodi_id", "total_lulusan", "lulusan_bekerja",
            "lulusan_lanjut_studi", "lulusan_wirausaha", "lulusan_belum",
            "persen_terserap", "target_iku", "capaian_iku",
        )
    )


def build_fact_iku2(spark: SparkSession, dim_waktu: DataFrame) -> DataFrame:
    """IKU-2: % mahasiswa ≥20 SKS luar kampus ATAU prestasi nasional/internasional."""
    mhs = _silver(spark, "silver_mahasiswa")
    prestasi = _bronze(spark, "raw_prestasi_mahasiswa")

    if "status_aktif" not in mhs.columns:
        mhs = mhs.join(
            _bronze(spark, "raw_mahasiswa").select("mahasiswa_id", "status_aktif"),
            on="mahasiswa_id",
            how="left",
        )
    mhs_aktif = mhs.filter(F.coalesce(F.col("status_aktif"), F.lit("Aktif")) == "Aktif")

    prestasi_nasional = (
        prestasi
        .filter(F.col("tingkat").isin("Nasional", "Internasional"))
        .select("mahasiswa_id")
        .distinct()
        .withColumn("has_prestasi_nasional", F.lit(True))
    )

    mhs_flagged = (
        mhs_aktif
        .join(prestasi_nasional, on="mahasiswa_id", how="left")
        .withColumn(
            "memenuhi_iku2",
            F.col("is_mbkm") | F.coalesce(F.col("has_prestasi_nasional"), F.lit(False)),
        )
    )

    agg = (
        mhs_flagged
        .groupBy("angkatan", "prodi_id")
        .agg(
            F.count("*").alias("total_mahasiswa_aktif"),
            F.sum(F.col("is_mbkm").cast("int")).alias("mahasiswa_sks_luar_20"),
            F.sum(
                F.when(F.coalesce(F.col("has_prestasi_nasional"), F.lit(False)), 1).otherwise(0)
            ).alias("mahasiswa_prestasi_nasional"),
            F.sum(F.col("memenuhi_iku2").cast("int")).alias("mahasiswa_memenuhi_iku2"),
        )
    )

    return (
        agg
        .withColumn(
            "persen_iku2",
            F.round(
                F.col("mahasiswa_memenuhi_iku2")
                / F.greatest(F.col("total_mahasiswa_aktif"), F.lit(1))
                * 100,
                2,
            ),
        )
        .withColumn("target_iku", _target_column("angkatan", "IKU-2"))
        .withColumn(
            "capaian_iku",
            F.round(F.col("persen_iku2") / F.greatest(F.col("target_iku"), F.lit(1)) * 100, 2),
        )
        .withColumn("fact_id", F.monotonically_increasing_id())
        .withColumn("waktu_id", F.col("angkatan").cast("int") * 100 + 12)
        .select(
            "fact_id", "waktu_id", "prodi_id", "total_mahasiswa_aktif",
            "mahasiswa_sks_luar_20", "mahasiswa_prestasi_nasional",
            "mahasiswa_memenuhi_iku2", "persen_iku2", "target_iku", "capaian_iku",
        )
    )


def build_fact_iku3(spark: SparkSession) -> DataFrame:
    """IKU-3: % dosen aktif tridarma luar / industri / bina prestasi."""
    dosen = _silver(spark, "silver_dosen").select("dosen_id", "prodi_id")
    kegiatan = _bronze(spark, "raw_kegiatan_dosen")

    kg_flags = (
        kegiatan
        .withColumn("tahun", F.col("tahun").cast("int"))
        .groupBy("dosen_id", "tahun")
        .agg(
            F.max(F.when(F.col("jenis_kegiatan") == "Tridarma_PT_Lain", 1).otherwise(0)).alias("is_tridarma_lain"),
            F.max(F.when(F.col("jenis_kegiatan") == "Praktisi_Industri", 1).otherwise(0)).alias("is_praktisi_ind"),
            F.max(F.when(F.col("jenis_kegiatan") == "Pembina_Prestasi", 1).otherwise(0)).alias("is_bina"),
            F.max(F.when(F.col("jenis_kegiatan") == "QS100", 1).otherwise(0)).alias("is_qs100"),
        )
    )

    joined = (
        kg_flags.join(dosen, on="dosen_id", how="inner")
        .withColumn(
            "memenuhi",
            (F.coalesce(F.col("is_tridarma_lain"), F.lit(0))
             + F.coalesce(F.col("is_praktisi_ind"), F.lit(0))
             + F.coalesce(F.col("is_bina"), F.lit(0))
             + F.coalesce(F.col("is_qs100"), F.lit(0))) > 0,
        )
    )

    agg = (
        joined
        .groupBy("tahun", "prodi_id")
        .agg(
            F.countDistinct("dosen_id").alias("total_dosen_tetap"),
            F.sum(F.when(F.col("is_tridarma_lain") > 0, 1).otherwise(0)).alias("dosen_tridarma_pt_lain"),
            F.sum(F.when(F.col("is_praktisi_ind") > 0, 1).otherwise(0)).alias("dosen_praktisi_industri"),
            F.sum(F.when(F.col("is_bina") > 0, 1).otherwise(0)).alias("dosen_bina_prestasi"),
            F.sum(F.when(F.col("is_qs100") > 0, 1).otherwise(0)).alias("dosen_qs100"),
            F.sum(F.when(F.col("memenuhi"), 1).otherwise(0)).alias("dosen_memenuhi_iku3"),
        )
    )

    return (
        agg
        .withColumn(
            "persen_iku3",
            F.round(
                F.col("dosen_memenuhi_iku3") / F.greatest(F.col("total_dosen_tetap"), F.lit(1)) * 100,
                2,
            ),
        )
        .withColumn("target_iku", _target_column("tahun", "IKU-3"))
        .withColumn(
            "capaian_iku",
            F.round(F.col("persen_iku3") / F.greatest(F.col("target_iku"), F.lit(1)) * 100, 2),
        )
        .withColumn("fact_id", F.monotonically_increasing_id())
        .withColumn("waktu_id", F.col("tahun") * 100 + 12)
        .select(
            "fact_id", "waktu_id", "prodi_id", "total_dosen_tetap",
            "dosen_tridarma_pt_lain", "dosen_praktisi_industri", "dosen_bina_prestasi",
            "dosen_qs100", "dosen_memenuhi_iku3", "persen_iku3", "target_iku", "capaian_iku",
        )
    )


def build_fact_iku4(spark: SparkSession) -> DataFrame:
    """IKU-4: % dosen S3/sertifikat/praktisi."""
    dosen = _silver(spark, "silver_dosen")

    return (
        dosen
        .groupBy("prodi_id")
        .agg(
            F.count("*").alias("total_dosen_tetap"),
            F.sum(F.col("is_s3").cast("int")).alias("dosen_s3"),
            F.sum(F.col("is_serdos").cast("int")).alias("dosen_sertifikat_industri"),
            F.sum(F.col("is_praktisi").cast("int")).alias("dosen_dari_praktisi"),
        )
        .withColumn("dosen_memenuhi_iku4",
                     F.col("dosen_s3") + F.col("dosen_sertifikat_industri") + F.col("dosen_dari_praktisi"))
        .withColumn(
            "persen_iku4",
            F.round(F.col("dosen_memenuhi_iku4") / F.greatest(F.col("total_dosen_tetap"), F.lit(1)) * 100, 2),
        )
        .withColumn("target_iku", F.lit(_target(2024, "IKU-4")))
        .withColumn("capaian_iku", F.round(F.col("persen_iku4") / F.greatest(F.col("target_iku"), F.lit(1)) * 100, 2))
        .withColumn("fact_id", F.monotonically_increasing_id())
        .withColumn("waktu_id", F.lit(202412))
        .select("fact_id", "waktu_id", "prodi_id", "total_dosen_tetap",
                "dosen_s3", "dosen_sertifikat_industri", "dosen_dari_praktisi",
                "dosen_memenuhi_iku4", "persen_iku4", "target_iku", "capaian_iku")
    )


def build_fact_iku5(spark: SparkSession) -> DataFrame:
    """IKU-5: rasio output penelitian rekognisi intl per dosen."""
    pkm = _silver(spark, "silver_penelitian_pkm")
    dosen = _silver(spark, "silver_dosen").select("dosen_id", "jurusan_id")

    pkm_enriched = pkm.withColumn("tahun", F.col("tahun").cast("int"))
    if "jurusan_id" not in pkm.columns:
        pkm_enriched = pkm_enriched.join(
            dosen.select("dosen_id", F.col("jurusan_id").alias("jurusan_id_from_dosen")),
            on="dosen_id",
            how="left",
        ).withColumn("jurusan_id", F.col("jurusan_id_from_dosen")).drop("jurusan_id_from_dosen")
    else:
        pkm_enriched = pkm_enriched.join(
            dosen.select("dosen_id", F.col("jurusan_id").alias("jurusan_id_from_dosen")),
            on="dosen_id",
            how="left",
        ).withColumn(
            "jurusan_id",
            F.coalesce(F.col("jurusan_id"), F.col("jurusan_id_from_dosen")),
        ).drop("jurusan_id_from_dosen")

    pkm_enriched = pkm_enriched.filter(F.col("jurusan_id").isNotNull() & F.col("tahun").isNotNull())

    output = (
        pkm_enriched.groupBy("jurusan_id", "tahun")
        .agg(
            F.sum(F.col("is_rekognisi").cast("int")).alias("output_rekognisi_internasional"),
            F.sum(F.col("is_diterapkan").cast("int")).alias("output_diterapkan_masyarakat"),
        )
    )
    dosen_count = dosen.filter(F.col("jurusan_id").isNotNull()).groupBy("jurusan_id").agg(
        F.count("*").alias("total_dosen")
    )

    return (
        output.join(dosen_count, on="jurusan_id", how="left")
        .withColumn(
            "total_output_eligible",
            F.col("output_rekognisi_internasional") + F.col("output_diterapkan_masyarakat"),
        )
        .withColumn(
            "rasio_per_dosen",
            F.round(F.col("total_output_eligible") / F.greatest(F.col("total_dosen"), F.lit(1)), 4),
        )
        .withColumn("target_iku", _target_column("tahun", "IKU-5"))
        .withColumn(
            "capaian_iku",
            F.round(F.col("rasio_per_dosen") / F.greatest(F.col("target_iku"), F.lit(0.01)) * 100, 2),
        )
        .withColumn("fact_id", F.monotonically_increasing_id())
        .withColumn("waktu_id", F.col("tahun") * 100 + 12)
        .select(
            "fact_id", "waktu_id", "jurusan_id", "total_dosen",
            "output_rekognisi_internasional", "output_diterapkan_masyarakat",
            "total_output_eligible", "rasio_per_dosen", "target_iku", "capaian_iku",
        )
    )


def build_fact_iku6(spark: SparkSession) -> DataFrame:
    """IKU-6: % prodi yang bekerjasama dengan mitra."""
    kjs = _silver(spark, "silver_kerjasama_aktif")
    prodi = _bronze(spark, "raw_prodi").filter(F.col("jenjang") == "S1")

    total_prodi = prodi.count()
    prodi_ks = kjs.filter(F.col("prodi_id") != "").select("prodi_id").distinct().count()

    rows = []
    for tahun in range(2021, 2026):
        target = _target(tahun, "IKU-6")
        pct = round(prodi_ks / max(total_prodi, 1) * 100, 2)
        rows.append((len(rows) + 1, tahun * 100 + 12, total_prodi, prodi_ks, pct, target,
                      round(pct / max(target, 1) * 100, 2)))

    return spark.createDataFrame(rows, [
        "fact_id", "waktu_id", "total_prodi_s1", "prodi_berkerjasama",
        "persen_iku6", "target_iku", "capaian_iku"])


def build_fact_iku7(spark: SparkSession) -> DataFrame:
    """IKU-7: % MK case method / team-based (simulasi)."""
    import random
    random.seed(42)
    prodi = _bronze(spark, "raw_prodi").filter(F.col("jenjang") == "S1").collect()

    rows = []
    for tahun in range(2021, 2026):
        target = _target(tahun, "IKU-7")
        for p in prodi:
            total_mk = random.randint(30, 60)
            mk_case = random.randint(5, int(total_mk * 0.5))
            mk_team = random.randint(3, int(total_mk * 0.4))
            mk_memenuhi = min(mk_case + mk_team, total_mk)
            pct = round(mk_memenuhi / total_mk * 100, 2)
            rows.append((len(rows) + 1, tahun * 100 + 12, p["prodi_id"], total_mk,
                          mk_case, mk_team, mk_memenuhi, pct, target,
                          round(pct / max(target, 1) * 100, 2)))

    return spark.createDataFrame(rows, [
        "fact_id", "waktu_id", "prodi_id", "total_mk", "mk_case_method",
        "mk_team_based", "mk_memenuhi", "persen_iku7", "target_iku", "capaian_iku"])


def build_fact_iku8(spark: SparkSession) -> DataFrame:
    """IKU-8: % prodi akreditasi/sertifikat internasional."""
    akr = _silver(spark, "silver_akreditasi_aktif")
    prodi = _bronze(spark, "raw_prodi").filter(F.col("jenjang") == "S1")

    total_prodi = prodi.count()
    intl = akr.filter(F.col("is_internasional")).select("prodi_id").distinct().count()

    rows = []
    for tahun in range(2021, 2026):
        target = _target(tahun, "IKU-8")
        pct = round(intl / max(total_prodi, 1) * 100, 2)
        rows.append((len(rows) + 1, tahun * 100 + 12, total_prodi, intl, pct, target,
                      round(pct / max(target, 1) * 100, 2)))

    return spark.createDataFrame(rows, [
        "fact_id", "waktu_id", "total_prodi_s1", "prodi_akreditasi_internasional",
        "persen_iku8", "target_iku", "capaian_iku"])


def build_fact_tata_kelola(spark: SparkSession) -> DataFrame:
    """Sasaran 4: SAKIP & kinerja anggaran."""
    keu = _bronze(spark, "raw_keuangan")

    agg = (
        keu.groupBy("tahun")
        .agg(
            F.sum("pagu").alias("pagu_total"),
            F.sum("realisasi").alias("realisasi_total"),
        )
        .withColumn("persen_realisasi", F.round(F.col("realisasi_total") / F.col("pagu_total") * 100, 2))
    )

    sakip_map = F.create_map(*[item for y, t in IKU_TARGETS.items()
                                for item in (F.lit(y), F.lit(t["SAKIP"]))])
    anggaran_map = F.create_map(*[item for y, t in IKU_TARGETS.items()
                                   for item in (F.lit(y), F.lit(float(t["Anggaran"])))])

    return (
        agg
        .withColumn("predikat_sakip", sakip_map[F.col("tahun")])
        .withColumn("nilai_sakip", F.round(F.col("persen_realisasi") * 0.95, 1))
        .withColumn("nilai_kinerja_anggaran", F.col("persen_realisasi"))
        .withColumn("target_sakip", F.lit("BB"))
        .withColumn("target_kinerja_anggaran", anggaran_map[F.col("tahun")])
        .withColumn("fact_id", F.monotonically_increasing_id())
        .withColumn("waktu_id", F.col("tahun") * 100 + 12)
        .select("fact_id", "waktu_id", "predikat_sakip", "nilai_sakip",
                "nilai_kinerja_anggaran", "pagu_total", "realisasi_total",
                "persen_realisasi", "target_sakip", "target_kinerja_anggaran")
    )


REKAP_IKU_SOURCES = [
    ("fact_iku1_lulusan", "IKU-1", "Lulusan bekerja/studi lanjut/wirausaha", "persen_terserap"),
    ("fact_iku2_mbkm", "IKU-2", "Mahasiswa MBKM ≥20 SKS / prestasi nasional", "persen_iku2"),
    ("fact_iku3_dosen_tridarma", "IKU-3", "Dosen tridarma luar/praktisi/bina prestasi", "persen_iku3"),
    ("fact_iku4_kualifikasi_dosen", "IKU-4", "Dosen S3/sertifikat kompetensi/praktisi", "persen_iku4"),
    ("fact_iku5_penelitian_pkm", "IKU-5", "Rasio output penelitian rekognisi intl per dosen", "rasio_per_dosen"),
    ("fact_iku6_kerjasama_prodi", "IKU-6", "Prodi bekerjasama dengan mitra", "persen_iku6"),
    ("fact_iku7_metode_pembelajaran", "IKU-7", "MK case method / team-based project", "persen_iku7"),
    ("fact_iku8_akreditasi_internasional", "IKU-8", "Prodi akreditasi/sertifikat internasional", "persen_iku8"),
]


def _status_capaian(nilai: float, target: float) -> str:
    if nilai >= target:
        return "Tercapai"
    if nilai >= target * 0.8:
        return "On Track"
    return "Tidak Tercapai"


def build_fact_rekap_iku(spark: SparkSession, all_facts: dict) -> DataFrame:
    """Ringkasan IKU institusi dari fakta Gold yang berhasil ditulis."""
    rows: list[tuple] = []
    sk = 1

    for tahun in range(2021, 2026):
        waktu_id = tahun * 100 + 12
        for fact_name, kode, nama, value_col in REKAP_IKU_SOURCES:
            target = _target(tahun, kode)
            satuan = "Rasio" if kode == "IKU-5" else "%"
            nilai = target * (0.85 + (tahun - 2021) * 0.03)
            status = _status_capaian(nilai, target)

            df = all_facts.get(fact_name)
            if df is not None and value_col in df.columns:
                scoped = df
                if "waktu_id" in df.columns:
                    by_year = df.filter((F.col("waktu_id") / 100).cast("int") == tahun)
                    if by_year.count() > 0:
                        scoped = by_year
                agg = scoped.agg(
                    F.avg(value_col).alias("nilai"),
                    F.avg("target_iku").alias("tgt") if "target_iku" in scoped.columns else F.lit(target).alias("tgt"),
                ).collect()[0]
                if agg["nilai"] is not None:
                    nilai = float(agg["nilai"])
                if agg["tgt"] is not None:
                    target = float(agg["tgt"])
                status = _status_capaian(nilai, target)

            rows.append((sk, waktu_id, kode, nama, round(nilai, 2), target, satuan, status))
            sk += 1

    return spark.createDataFrame(
        rows,
        [
            "fact_id", "waktu_id", "iku_kode", "iku_nama", "nilai_capaian",
            "nilai_target", "satuan", "status_capaian",
        ],
    )


# ───────────────────────────────────────────────────────────────────────────
# Profiling
# ───────────────────────────────────────────────────────────────────────────

IKU_VALUE_COLUMNS = {
    "fact_iku1_lulusan": "persen_terserap",
    "fact_iku2_mbkm": "persen_iku2",
    "fact_iku3_dosen_tridarma": "persen_iku3",
    "fact_iku4_kualifikasi_dosen": "persen_iku4",
    "fact_iku5_penelitian_pkm": "rasio_per_dosen",
    "fact_iku6_kerjasama_prodi": "persen_iku6",
    "fact_iku7_metode_pembelajaran": "persen_iku7",
    "fact_iku8_akreditasi_internasional": "persen_iku8",
}


def _sample_kpi_from_fact(df: DataFrame, table_name: str) -> dict:
    """Rata-rata institusi untuk KPI Dashboard (Superset)."""
    value_col = IKU_VALUE_COLUMNS.get(table_name)
    if not value_col or value_col not in df.columns:
        return {}

    agg_exprs = [F.avg(value_col).alias("nilai_capaian")]
    if "target_iku" in df.columns:
        agg_exprs.append(F.avg("target_iku").alias("nilai_target"))
    if "capaian_iku" in df.columns:
        agg_exprs.append(F.avg("capaian_iku").alias("persen_capaian_terhadap_target"))

    row = df.agg(*agg_exprs).collect()[0]
    nilai = float(row["nilai_capaian"]) if row["nilai_capaian"] is not None else None
    target = None
    if "target_iku" in df.columns and row["nilai_target"] is not None:
        target = float(row["nilai_target"])
    persen_ct = None
    if "capaian_iku" in df.columns and row["persen_capaian_terhadap_target"] is not None:
        persen_ct = float(row["persen_capaian_terhadap_target"])

    status = ""
    if nilai is not None and target is not None:
        if nilai >= target:
            status = "Tercapai"
        elif nilai >= target * 0.8:
            status = "On Track"
        else:
            status = "Tidak Tercapai"

    satuan = "Rasio" if table_name == "fact_iku5_penelitian_pkm" else "%"
    return {
        "nilai_capaian": round(nilai, 2) if nilai is not None else None,
        "nilai_target": round(target, 2) if target is not None else None,
        "persen_capaian_terhadap_target": round(persen_ct, 2) if persen_ct is not None else None,
        "status_capaian": status,
        "satuan": satuan,
    }


def profile_gold_table(df: DataFrame, table_name: str, table_type: str,
                       sources: list[str]) -> dict:
    row_count = df.count()
    profile = {
        "table_name": table_name,
        "table_type": table_type,
        "row_count": row_count,
        "column_count": len(df.columns),
        "schema": {c.name: str(c.dataType) for c in df.schema},
        "sources": sources,
        "profiled_at": datetime.utcnow().isoformat() + "Z",
    }
    if table_type == "fact" and table_name in IKU_VALUE_COLUMNS:
        profile["sample_kpi"] = _sample_kpi_from_fact(df, table_name)
    return profile


# ───────────────────────────────────────────────────────────────────────────
# Main pipeline
# ───────────────────────────────────────────────────────────────────────────

GOLD_TABLES = [
    ("dim_waktu",              "dimension", build_dim_waktu,   ["generated"]),
    ("dim_prodi",              "dimension", build_dim_prodi,   ["raw_prodi"]),
    ("dim_dosen",              "dimension", build_dim_dosen,   ["silver_dosen"]),
    ("dim_mahasiswa",          "dimension", build_dim_mahasiswa, ["silver_mahasiswa"]),
    ("dim_topik_penelitian",   "dimension", build_dim_topik_penelitian, ["generated"]),
    ("fact_iku1_lulusan",      "fact",      None, ["silver_lulusan"]),
    ("fact_iku2_mbkm",         "fact",      None, ["silver_mahasiswa", "raw_prestasi_mahasiswa"]),
    ("fact_iku3_dosen_tridarma","fact",      None, ["silver_dosen", "raw_kegiatan_dosen"]),
    ("fact_iku4_kualifikasi_dosen","fact",   None, ["silver_dosen"]),
    ("fact_iku5_penelitian_pkm","fact",      None, ["silver_penelitian_pkm", "silver_dosen"]),
    ("fact_iku6_kerjasama_prodi","fact",     None, ["silver_kerjasama_aktif", "raw_prodi"]),
    ("fact_iku7_metode_pembelajaran","fact", None, ["raw_prodi"]),
    ("fact_iku8_akreditasi_internasional","fact", None, ["silver_akreditasi_aktif", "raw_prodi"]),
    ("fact_tata_kelola",       "fact",      None, ["raw_keuangan"]),
    ("fact_rekap_iku_institusi","fact",      None, ["all_iku_facts"]),
]


def run_silver_to_gold(aqe_context: str | None = None) -> dict:
    """Entry-point: build star schema Gold layer."""
    global _AQE_SCENARIO
    from spark.pipeline_metrics import persist_pipeline_run_metrics, utc_now

    _AQE_SCENARIO = resolve_aqe_scenario(aqe_context)
    os.environ["SPARK_AQE_SCENARIO"] = _AQE_SCENARIO
    started_at = utc_now()
    logger.info("Silver → Gold | AQE scenario=%s | silver=%s | gold schema=%s",
                _AQE_SCENARIO,
                silver_table(_AQE_SCENARIO, "silver_mahasiswa").rsplit(".", 1)[0],
                f"lakehouse.gold_aqe_{_AQE_SCENARIO.lower()}")

    spark = get_spark_session(_AQE_SCENARIO)

    try:
        dim_waktu = build_dim_waktu(spark)
        results = {}
        all_facts = {}

        # ── Dimensi ──
        dims = [
            ("dim_waktu", dim_waktu, ["generated"]),
            ("dim_prodi", build_dim_prodi(spark), ["raw_prodi"]),
            ("dim_dosen", build_dim_dosen(spark), ["silver_dosen"]),
            ("dim_mahasiswa", build_dim_mahasiswa(spark), ["silver_mahasiswa"]),
            ("dim_topik_penelitian", build_dim_topik_penelitian(spark), ["generated"]),
        ]
        for name, df, sources in dims:
            tbl = write_iceberg_table(df, _AQE_SCENARIO, "gold", name)
            results[name] = profile_gold_table(df, name, "dimension", sources)
            results[name]["written"] = True
            logger.info("  ✅ %s → %s rows", tbl, f"{results[name]['row_count']:,}")

        # ── Fakta ──
        fact_builders = [
            ("fact_iku1_lulusan", lambda: build_fact_iku1(spark, dim_waktu), ["silver_lulusan"]),
            ("fact_iku2_mbkm", lambda: build_fact_iku2(spark, dim_waktu), ["silver_mahasiswa", "raw_prestasi_mahasiswa"]),
            ("fact_iku3_dosen_tridarma", lambda: build_fact_iku3(spark), ["silver_dosen", "raw_kegiatan_dosen"]),
            ("fact_iku4_kualifikasi_dosen", lambda: build_fact_iku4(spark), ["silver_dosen"]),
            ("fact_iku5_penelitian_pkm", lambda: build_fact_iku5(spark), ["silver_penelitian_pkm", "silver_dosen"]),
            ("fact_iku6_kerjasama_prodi", lambda: build_fact_iku6(spark), ["silver_kerjasama_aktif", "raw_prodi"]),
            ("fact_iku7_metode_pembelajaran", lambda: build_fact_iku7(spark), ["raw_prodi"]),
            ("fact_iku8_akreditasi_internasional", lambda: build_fact_iku8(spark), ["silver_akreditasi_aktif", "raw_prodi"]),
            ("fact_tata_kelola", lambda: build_fact_tata_kelola(spark), ["raw_keuangan"]),
        ]

        for name, builder, sources in fact_builders:
            try:
                df = builder()
                tbl = write_iceberg_table(df, _AQE_SCENARIO, "gold", name)
                results[name] = profile_gold_table(df, name, "fact", sources)
                results[name]["written"] = True
                all_facts[name] = df
                logger.info("  ✅ %s → %s rows", tbl, f"{results[name]['row_count']:,}")
            except Exception as exc:
                logger.error("  ✗ %s failed: %s", name, exc)
                results[name] = {"table_name": name, "error": str(exc), "written": False}

        # ── Rekap IKU ──
        try:
            rekap = build_fact_rekap_iku(spark, all_facts)
            tbl = write_iceberg_table(rekap, _AQE_SCENARIO, "gold", "fact_rekap_iku_institusi")
            results["fact_rekap_iku_institusi"] = profile_gold_table(
                rekap, "fact_rekap_iku_institusi", "fact", ["all_iku_facts"])
            results["fact_rekap_iku_institusi"]["written"] = True
            logger.info("  ✅ %s → %s rows", tbl, f"{results['fact_rekap_iku_institusi']['row_count']:,}")
        except Exception as exc:
            logger.error("  ✗ fact_rekap_iku_institusi failed: %s", exc)

        written = sum(1 for r in results.values() if r.get("written"))
        total_rows = sum(r.get("row_count", 0) for r in results.values() if r.get("written"))
        logger.info("\n✅ Gold complete: %d/%d tables, %s total rows",
                     written, len(results), f"{total_rows:,}")

        ended_at = utc_now()
        ctx = aqe_context or os.environ.get("SPARK_AQE_SCENARIO", "unknown")
        metrics_path = persist_pipeline_run_metrics(
            pipeline="silver_to_gold",
            results=results,
            started_at=started_at,
            ended_at=ended_at,
            scenario=ctx.upper() if ctx else None,
            extra={"aqe_context_silver": ctx},
        )
        logger.info("Pipeline metrics → %s", metrics_path)
        results["_pipeline_meta"] = {
            "metrics_file": str(metrics_path),
            "duration_sec": round((ended_at - started_at).total_seconds(), 3),
            "aqe_context_silver": ctx,
        }
        return results

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    results = run_silver_to_gold()
    print(json.dumps(results, indent=2, default=str))
