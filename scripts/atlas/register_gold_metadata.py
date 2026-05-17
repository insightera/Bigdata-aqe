"""
Atlas Metadata Registration — Gold Layer (Star Schema)
=======================================================
Mendaftarkan metadata Gold ke Apache Atlas REST API v2.

Sesuai diagram, metadata Gold mencakup:
  1. Business Metadata       — KPI definitions, star schema relationships
  2. KPI Metadata            — IKU targets, capaian, status per tahun
  3. AI Metadata             — model readiness, feature store candidates
  4. Consumption Metadata    — dashboard usage, OLAP query patterns
  5. Advanced Lineage        — full chain staging → bronze → silver → gold
"""

import base64
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime

logger = logging.getLogger("atlas_gold_metadata")

ATLAS_URL = "http://atlas:21000"
ATLAS_USER = "admin"
ATLAS_PASS = "admin"
CLUSTER_NAME = "lakehouse"

# ── KPI / IKU definitions ─────────────────────────────────────────────────

IKU_DEFINITIONS = {
    "IKU-1": {
        "nama": "Lulusan bekerja/studi lanjut/wirausaha",
        "fact_table": "fact_iku1_lulusan",
        "kpi_formula": "(lulusan_bekerja + lulusan_lanjut_studi + lulusan_wirausaha) / total_lulusan * 100",
        "satuan": "%",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 1",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-1",
    },
    "IKU-2": {
        "nama": "Mahasiswa MBKM ≥20 SKS / prestasi nasional",
        "fact_table": "fact_iku2_mbkm",
        "kpi_formula": "mahasiswa_memenuhi_iku2 / total_mahasiswa_aktif * 100",
        "satuan": "%",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 1",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-2",
    },
    "IKU-3": {
        "nama": "Dosen tridarma luar/praktisi/bina prestasi",
        "fact_table": "fact_iku3_dosen_tridarma",
        "kpi_formula": "dosen_memenuhi_iku3 / total_dosen_tetap * 100",
        "satuan": "%",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 2",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-3",
    },
    "IKU-4": {
        "nama": "Dosen S3/sertifikat kompetensi/praktisi",
        "fact_table": "fact_iku4_kualifikasi_dosen",
        "kpi_formula": "dosen_memenuhi_iku4 / total_dosen_tetap * 100",
        "satuan": "%",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 2",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-4",
    },
    "IKU-5": {
        "nama": "Rasio output penelitian rekognisi intl per dosen",
        "fact_table": "fact_iku5_penelitian_pkm",
        "kpi_formula": "total_output_eligible / total_dosen",
        "satuan": "Rasio",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 3",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-5",
    },
    "IKU-6": {
        "nama": "Prodi bekerjasama dengan mitra",
        "fact_table": "fact_iku6_kerjasama_prodi",
        "kpi_formula": "prodi_berkerjasama / total_prodi_s1 * 100",
        "satuan": "%",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 3",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-6",
    },
    "IKU-7": {
        "nama": "MK case method / team-based project",
        "fact_table": "fact_iku7_metode_pembelajaran",
        "kpi_formula": "mk_memenuhi / total_mk * 100",
        "satuan": "%",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 1",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-7",
    },
    "IKU-8": {
        "nama": "Prodi akreditasi/sertifikat internasional",
        "fact_table": "fact_iku8_akreditasi_internasional",
        "kpi_formula": "prodi_akreditasi_internasional / total_prodi_s1 * 100",
        "satuan": "%",
        "sumber_renstra": "Renstra ITERA 2020-2024, Sasaran 3",
        "dashboard_panel": "Executive Dashboard — Capaian IKU-8",
    },
}

# ── Consumption metadata ──────────────────────────────────────────────────

# Dashboard pimpinan (sesuai struktur organisasi ITERA)
_LEADERSHIP = [
    "Rektor",
    "Wakil Rektor Bidang Akademik & Kemahasiswaan",
    "Wakil Rektor Bidang Keuangan & Umum",
    "Kepala Biro Akademik Perencanaan dan Umum",
    "Kepala Bagian Umum dan Akademik",
    "Dekan Fakultas Sains",
    "Dekan Fakultas Teknologi Infrastruktur dan Kewilayahan",
    "Dekan Fakultas Teknologi Industri",
]

CONSUMPTION_META = {
    "dim_waktu":       {"consumers": _LEADERSHIP[:4], "olap_role": "Dimension — drill by time"},
    "dim_prodi":       {"consumers": _LEADERSHIP, "olap_role": "Dimension — drill by prodi/jurusan"},
    "dim_dosen":       {"consumers": [_LEADERSHIP[0], _LEADERSHIP[2], _LEADERSHIP[3]], "olap_role": "Dimension — drill by dosen"},
    "dim_mahasiswa":   {"consumers": [_LEADERSHIP[0], _LEADERSHIP[1], _LEADERSHIP[4]], "olap_role": "Dimension — drill by mahasiswa"},
    "dim_topik_penelitian": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[3]], "olap_role": "Dimension — drill by research topic"},
    "fact_iku1_lulusan": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[1], _LEADERSHIP[3], *_LEADERSHIP[5:]], "olap_role": "Fact — IKU-1 metrics"},
    "fact_iku2_mbkm":  {"consumers": [_LEADERSHIP[0], _LEADERSHIP[1], _LEADERSHIP[4], *_LEADERSHIP[5:]], "olap_role": "Fact — IKU-2 metrics"},
    "fact_iku3_dosen_tridarma": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[1], _LEADERSHIP[3]], "olap_role": "Fact — IKU-3 metrics"},
    "fact_iku4_kualifikasi_dosen": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[1], _LEADERSHIP[3]], "olap_role": "Fact — IKU-4 metrics"},
    "fact_iku5_penelitian_pkm": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[3], _LEADERSHIP[5]], "olap_role": "Fact — IKU-5 metrics"},
    "fact_iku6_kerjasama_prodi": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[2], *_LEADERSHIP[5:]], "olap_role": "Fact — IKU-6 metrics"},
    "fact_iku7_metode_pembelajaran": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[1], _LEADERSHIP[4], *_LEADERSHIP[5:]], "olap_role": "Fact — IKU-7 metrics"},
    "fact_iku8_akreditasi_internasional": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[3], *_LEADERSHIP[5:]], "olap_role": "Fact — IKU-8 metrics"},
    "fact_tata_kelola": {"consumers": [_LEADERSHIP[0], _LEADERSHIP[2], _LEADERSHIP[3]], "olap_role": "Fact — SAKIP & anggaran"},
    "fact_rekap_iku_institusi": {"consumers": _LEADERSHIP, "olap_role": "Fact — executive summary"},
}


# ── HTTP helper ────────────────────────────────────────────────────────────

def _atlas_request(method: str, path: str, data: dict | None = None) -> dict | None:
    url = f"{ATLAS_URL}{path}"
    cred = base64.b64encode(f"{ATLAS_USER}:{ATLAS_PASS}".encode()).decode()
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "Authorization": f"Basic {cred}"}
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        if exc.code == 409:
            return None
        if exc.code == 404 and method == "PUT":
            logger.info("PUT returned 404, retrying with POST …")
            req2 = urllib.request.Request(url, data=body, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req2, timeout=30) as resp2:
                    return json.loads(resp2.read().decode("utf-8"))
            except urllib.error.HTTPError as exc2:
                if exc2.code == 409:
                    return None
                err2 = exc2.read().decode("utf-8", errors="replace") if exc2.fp else ""
                logger.error("Atlas POST %s → %d: %s", path, exc2.code, err2[:400])
                raise
        logger.error("Atlas %s %s → %d: %s", method, path, exc.code, err[:400])
        raise
    except urllib.error.URLError as exc:
        logger.error("Atlas unreachable: %s", exc)
        raise


def _attr(name: str, type_name: str) -> dict:
    return {"name": name, "typeName": type_name, "cardinality": "SINGLE",
            "isOptional": True, "isUnique": False, "isIndexable": True}


# ── 1. Types ───────────────────────────────────────────────────────────────

def create_gold_types():
    payload = {
        "enumDefs": [], "structDefs": [], "relationshipDefs": [],
        "classificationDefs": [
            {"name": "Gold_Layer", "description": "Data di Gold layer (curated star schema)",
             "superTypes": [], "attributeDefs": []},
            {"name": "KPI_Metric", "description": "Tabel berisi metrik KPI/IKU",
             "superTypes": [], "attributeDefs": []},
            {"name": "Star_Schema_Dimension", "description": "Tabel dimensi dalam star schema",
             "superTypes": [], "attributeDefs": []},
            {"name": "Star_Schema_Fact", "description": "Tabel fakta dalam star schema",
             "superTypes": [], "attributeDefs": []},
            {"name": "Executive_Dashboard", "description": "Data dikonsumsi oleh Dashboard Pimpinan",
             "superTypes": [], "attributeDefs": []},
        ],
        "entityDefs": [],
    }
    logger.info("Creating Gold classification types …")
    result = _atlas_request("PUT", "/api/atlas/v2/types/typedefs", payload)
    if result:
        logger.info("  Gold types OK")
    return result


# ── 2. Entity registration ────────────────────────────────────────────────

def register_gold_entity(table_name: str, profiling: dict) -> dict | None:
    table_type = profiling.get("table_type", "fact")
    consumption = CONSUMPTION_META.get(table_name, {})

    classifications = [{"typeName": "Gold_Layer"}]
    if table_type == "dimension":
        classifications.append({"typeName": "Star_Schema_Dimension"})
    else:
        classifications.append({"typeName": "Star_Schema_Fact"})
        classifications.append({"typeName": "KPI_Metric"})
        classifications.append({"typeName": "Executive_Dashboard"})

    iku_code = None
    for code, meta in IKU_DEFINITIONS.items():
        if meta["fact_table"] == table_name:
            iku_code = code
            break

    kpi_meta = IKU_DEFINITIONS.get(iku_code, {}) if iku_code else {}
    sample_kpi = profiling.get("sample_kpi", {}) if iku_code else {}

    profiling_enriched = {
        "schema": profiling.get("schema", {}),
        "star_schema": {
            "table_type": table_type,
            "olap_role": consumption.get("olap_role", ""),
        },
        "kpi": {
            "iku_code": iku_code or "",
            "iku_nama": kpi_meta.get("nama", ""),
            "formula": kpi_meta.get("kpi_formula", ""),
            "satuan": kpi_meta.get("satuan", ""),
            "sumber_renstra": kpi_meta.get("sumber_renstra", ""),
            "nilai_capaian": sample_kpi.get("nilai_capaian"),
            "nilai_target": sample_kpi.get("nilai_target"),
            "status_capaian": sample_kpi.get("status_capaian", ""),
            "persen_capaian_terhadap_target": sample_kpi.get("persen_capaian_terhadap_target"),
        } if iku_code else {},
        "consumption": {
            "consumers": consumption.get("consumers", []),
            "dashboard_panel": kpi_meta.get("dashboard_panel", ""),
        },
        "ai_metadata": {
            "ml_ready": table_type == "fact",
            "feature_store_candidate": table_type == "fact" and iku_code is not None,
            "suggested_models": ["trend_analysis", "anomaly_detection"] if iku_code else [],
        },
    }

    description = kpi_meta.get("nama", f"Gold {table_type}: {table_name}")
    if iku_code:
        description = f"{iku_code}: {description} — Formula: {kpi_meta.get('kpi_formula', '')}"

    entity = {
        "entity": {
            "typeName": "lakehouse_dataset",
            "attributes": {
                "qualifiedName": f"gold.{table_name}@{CLUSTER_NAME}",
                "name": table_name,
                "description": description,
                "layer": "gold",
                "format": "iceberg",
                "location": f"s3a://warehouse/gold/{table_name}",
                "row_count": profiling.get("row_count", 0),
                "column_count": profiling.get("column_count", 0),
                "schema_def": json.dumps(profiling.get("schema", {})),
                "profiling": json.dumps(profiling_enriched),
                "pii_columns": "[]",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "enriched_at": datetime.utcnow().isoformat() + "Z",
            },
            "classifications": classifications,
        }
    }

    result = _atlas_request("POST", "/api/atlas/v2/entity", entity)
    if result:
        logger.info("  ✓ Gold entity: %s [%s]", table_name, table_type)
    return result


# ── 3. Lineage ─────────────────────────────────────────────────────────────

def register_gold_lineage(table_name: str, profiling: dict) -> dict | None:
    sources = profiling.get("sources", [])
    if not sources or sources == ["generated"]:
        return None

    inputs = []
    for src in sources:
        if src == "all_iku_facts":
            continue
        if src.startswith("silver_"):
            layer = "silver"
        elif src.startswith("raw_"):
            layer = "bronze"
        else:
            layer = "silver"
        inputs.append({
            "typeName": "lakehouse_dataset",
            "uniqueAttributes": {"qualifiedName": f"{layer}.{src}@{CLUSTER_NAME}"},
        })

    if not inputs:
        return None

    entity = {
        "entity": {
            "typeName": "lakehouse_etl_process",
            "attributes": {
                "qualifiedName": f"etl.silver_to_gold.{table_name}@{CLUSTER_NAME}",
                "name": f"silver_to_gold_{table_name}",
                "description": f"Star schema aggregation: {', '.join(sources)} → gold.{table_name}",
                "pipeline_name": "silver_to_gold",
                "source_layer": "silver",
                "target_layer": "gold",
                "engine": "spark-iceberg",
                "run_timestamp": datetime.utcnow().isoformat() + "Z",
                "inputs": inputs,
                "outputs": [{
                    "typeName": "lakehouse_dataset",
                    "uniqueAttributes": {"qualifiedName": f"gold.{table_name}@{CLUSTER_NAME}"},
                }],
            },
        }
    }

    result = _atlas_request("POST", "/api/atlas/v2/entity", entity)
    if result:
        logger.info("  ✓ Lineage: [%s] → gold.%s", ",".join(sources), table_name)
    return result


# ── Main ───────────────────────────────────────────────────────────────────

def register_all_gold_metadata(
    profiling_results: dict,
    atlas_url: str | None = None,
    atlas_user: str | None = None,
    atlas_pass: str | None = None,
) -> bool:
    global ATLAS_URL, ATLAS_USER, ATLAS_PASS
    if atlas_url:
        ATLAS_URL = atlas_url
    if atlas_user:
        ATLAS_USER = atlas_user
    if atlas_pass:
        ATLAS_PASS = atlas_pass

    logger.info("=" * 60)
    logger.info("  ATLAS METADATA — Gold Layer (Star Schema)")
    logger.info("  Atlas: %s  |  Tables: %d", ATLAS_URL, len(profiling_results))
    logger.info("=" * 60)

    create_gold_types()

    success = 0
    for table_name, profiling in profiling_results.items():
        if not profiling.get("written"):
            continue
        logger.info("\n── %s ──", table_name)
        try:
            register_gold_entity(table_name, profiling)
            register_gold_lineage(table_name, profiling)
            success += 1
        except Exception as exc:
            logger.error("  ✗ Failed for %s: %s", table_name, exc)

    written = sum(1 for r in profiling_results.values() if r.get("written"))
    logger.info("\n✅ Gold registration complete: %d/%d tables", success, written)

    _log_full_catalog_summary()
    return success == written


def _log_full_catalog_summary():
    """Log ringkasan seluruh katalog setelah Gold selesai."""
    logger.info("\n" + "=" * 60)
    logger.info("  FULL DATA CATALOG SUMMARY")
    logger.info("=" * 60)
    logger.info("""
  Pipeline Lineage (end-to-end):
    Source CSV → Staging → Bronze (Iceberg) → Silver (Enriched) → Gold (Star Schema)

  Atlas Entities:
    Staging:  12 lakehouse_dataset (CSV)
    Bronze:   12 lakehouse_dataset (Iceberg)
    Silver:    6 lakehouse_dataset (Enriched)
    Gold:     15 lakehouse_dataset (5 dim + 10 fact)
    Total:   ~45 entities

  Lineage Processes:
    staging → bronze:   12 lakehouse_etl_process
    bronze → silver:     6 lakehouse_etl_process
    silver → gold:     ~13 lakehouse_etl_process
    Total:             ~31 processes

  Classifications:
    PII, Staging_Layer, Bronze_Layer, Silver_Layer, Gold_Layer,
    Quality_Pass, Quality_Quarantine, KPI_Metric,
    Star_Schema_Dimension, Star_Schema_Fact, Executive_Dashboard

  Metadata per Layer:
    Bronze: Technical, Lineage, Profiling, Classification
    Silver: Clean, Quality, Transform Lineage, Business, Compliance
    Gold:   Business, KPI, AI, Consumption, Advanced Lineage
    """)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            profiling = json.load(f)
        register_all_gold_metadata(profiling)
    else:
        print("Usage: python register_gold_metadata.py <gold_profiling.json>")
