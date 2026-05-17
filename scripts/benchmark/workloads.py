"""
Definisi workload eksperimen AQE — Spark (Silver) dan Trino (Gold).
Selaras docs/eksperimen/README.md §7 dan README.md §5.
"""

from __future__ import annotations

from typing import TypedDict


class Workload(TypedDict):
    id: str
    name: str
    workload_type: str  # join | aggregation | filtering
    sql: str
    description: str


SPARK_SILVER_WORKLOADS: list[Workload] = [
    {
        "id": "W1",
        "name": "silver_join_mhs_dosen",
        "workload_type": "join",
        "description": "Join silver_mahasiswa ⋈ silver_dosen on prodi_id",
        "sql": """
            SELECT m.prodi_id, COUNT(*) AS cnt
            FROM lakehouse.silver.silver_mahasiswa m
            INNER JOIN lakehouse.silver.silver_dosen d ON m.prodi_id = d.prodi_id
            GROUP BY m.prodi_id
        """,
    },
    {
        "id": "W2",
        "name": "silver_agg_by_prodi",
        "workload_type": "aggregation",
        "description": "GROUP BY prodi_id pada silver_mahasiswa",
        "sql": """
            SELECT prodi_id, COUNT(*) AS n_mhs, AVG(CAST(angkatan AS INT)) AS avg_angkatan
            FROM lakehouse.silver.silver_mahasiswa
            GROUP BY prodi_id
        """,
    },
    {
        "id": "W3",
        "name": "silver_filter_join",
        "workload_type": "filtering",
        "description": "Filter angkatan + join prodi",
        "sql": """
            SELECT m.mahasiswa_id, m.prodi_id, p.nama_prodi
            FROM lakehouse.silver.silver_mahasiswa m
            LEFT JOIN lakehouse.bronze.raw_prodi p ON m.prodi_id = p.prodi_id
            WHERE CAST(m.angkatan AS INT) >= 2020
        """,
    },
]

TRINO_GOLD_WORKLOADS: list[Workload] = [
    {
        "id": "W4",
        "name": "gold_join_iku1_prodi",
        "workload_type": "join",
        "description": "Join fact_iku1_lulusan ⋈ dim_prodi",
        "sql": """
            SELECT p.nama_prodi, COUNT(*) AS n
            FROM lakehouse.gold.fact_iku1_lulusan f
            JOIN lakehouse.gold.dim_prodi p ON f.prodi_id = p.prodi_id
            GROUP BY p.nama_prodi
        """,
    },
    {
        "id": "W5",
        "name": "gold_agg_rekap_iku",
        "workload_type": "aggregation",
        "description": "Agregasi rekap IKU per tahun",
        "sql": """
            SELECT w.tahun, AVG(r.nilai_capaian) AS avg_capaian
            FROM lakehouse.gold.fact_rekap_iku_institusi r
            JOIN lakehouse.gold.dim_waktu w ON r.waktu_id = w.waktu_id
            GROUP BY w.tahun
        """,
    },
    {
        "id": "W6",
        "name": "gold_filter_rekap",
        "workload_type": "filtering",
        "description": "Filter status capaian pada fact rekap",
        "sql": """
            SELECT iku_kode, status_capaian, nilai_capaian
            FROM lakehouse.gold.fact_rekap_iku_institusi
            WHERE status_capaian = 'Tidak Tercapai'
        """,
    },
]

ALL_WORKLOAD_IDS = [w["id"] for w in SPARK_SILVER_WORKLOADS + TRINO_GOLD_WORKLOADS]
