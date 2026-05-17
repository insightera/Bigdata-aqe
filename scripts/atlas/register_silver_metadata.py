"""
Atlas Metadata Enrichment — Silver Layer
==========================================
Mendaftarkan metadata Silver (enriched) ke Apache Atlas REST API v2.

Sesuai diagram, metadata Silver mencakup:
  1. Clean Metadata             — schema tabel setelah cleaning/enrichment
  2. Quality Metadata           — quality score, completeness, status (PASS/QUARANTINE)
  3. Transformation Lineage     — bronze → silver + deskripsi transformasi
  4. Business Metadata          — deskripsi bisnis, glossary terms, konteks IKU
  5. Compliance Metadata        — PII handling, data classification
"""

import base64
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime

logger = logging.getLogger("atlas_silver_metadata")

ATLAS_URL = "http://atlas:21000"
ATLAS_USER = "admin"
ATLAS_PASS = "admin"
CLUSTER_NAME = "lakehouse"

# Business metadata per tabel Silver → konteks IKU
BUSINESS_METADATA = {
    "silver_mahasiswa": {
        "business_description": (
            "Data mahasiswa aktif ITERA yang telah di-enriched dengan nama prodi, "
            "jurusan, dan flag MBKM (≥20 SKS luar kampus). Sumber utama untuk "
            "IKU-2 (mahasiswa MBKM/prestasi) dan basis dimensi mahasiswa di Gold layer."
        ),
        "business_owner": "Biro Akademik & Kemahasiswaan",
        "iku_relevance": ["IKU-2"],
        "glossary_terms": ["Mahasiswa Aktif", "MBKM", "SKS Luar Kampus"],
        "update_frequency": "Semesteran",
    },
    "silver_lulusan": {
        "business_description": (
            "Data lulusan ITERA dengan flag status pasca-lulus (bekerja, studi lanjut, "
            "wirausaha). Merupakan sumber utama IKU-1 (persentase lulusan terserap)."
        ),
        "business_owner": "Pusat Karir & Alumni",
        "iku_relevance": ["IKU-1"],
        "glossary_terms": ["Lulusan Terserap", "Tracer Study", "Masa Tunggu"],
        "update_frequency": "Tahunan (pasca wisuda)",
    },
    "silver_dosen": {
        "business_description": (
            "Data dosen ITERA dengan flag kualifikasi (S3, sertifikasi, praktisi) dan "
            "aktivitas tridarma. Sumber IKU-3 (dosen tridarma luar) dan IKU-4 "
            "(kualifikasi dosen)."
        ),
        "business_owner": "Biro Sumber Daya Manusia",
        "iku_relevance": ["IKU-3", "IKU-4"],
        "glossary_terms": ["Dosen Tetap", "Tridarma", "Serdos", "Jabatan Fungsional"],
        "update_frequency": "Semesteran",
    },
    "silver_penelitian_pkm": {
        "business_description": (
            "Gabungan data penelitian dan pengabdian masyarakat (PkM) dosen ITERA "
            "dengan flag rekognisi internasional dan penerapan masyarakat. Sumber "
            "IKU-5 (rasio output penelitian per dosen)."
        ),
        "business_owner": "LPPM (Lembaga Penelitian & Pengabdian Masyarakat)",
        "iku_relevance": ["IKU-5"],
        "glossary_terms": ["Rekognisi Internasional", "Pengabdian Masyarakat", "Hibah Penelitian"],
        "update_frequency": "Tahunan",
    },
    "silver_kerjasama_aktif": {
        "business_description": (
            "Kerjasama (MoU/PKS) ITERA yang masih aktif, dengan flag program MBKM. "
            "Sumber IKU-6 (persentase prodi bekerjasama dengan mitra)."
        ),
        "business_owner": "Biro Kerjasama & Hubungan Internasional",
        "iku_relevance": ["IKU-6"],
        "glossary_terms": ["MoU", "PKS", "Mitra Kerjasama", "MBKM"],
        "update_frequency": "Setiap ada MoU/PKS baru",
    },
    "silver_akreditasi_aktif": {
        "business_description": (
            "Akreditasi terakhir per prodi yang masih berlaku, termasuk flag akreditasi "
            "internasional. Sumber IKU-8 (prodi akreditasi/sertifikat internasional)."
        ),
        "business_owner": "LP3M (Lembaga Penjaminan Mutu)",
        "iku_relevance": ["IKU-8"],
        "glossary_terms": ["BAN-PT", "LAM", "Akreditasi Internasional", "Unggul"],
        "update_frequency": "Setiap siklus akreditasi (5 tahun)",
    },
}

# Compliance metadata
COMPLIANCE_METADATA = {
    "silver_mahasiswa": {
        "contains_pii": True,
        "pii_columns": ["nama", "mahasiswa_id", "asal_provinsi"],
        "data_classification": "Internal",
        "retention_policy": "7 tahun setelah lulus",
        "access_restriction": "Role: admin, akademik",
    },
    "silver_lulusan": {
        "contains_pii": True,
        "pii_columns": ["mahasiswa_id", "nama_perusahaan"],
        "data_classification": "Internal",
        "retention_policy": "10 tahun",
        "access_restriction": "Role: admin, karir",
    },
    "silver_dosen": {
        "contains_pii": True,
        "pii_columns": ["nama", "dosen_id"],
        "data_classification": "Internal",
        "retention_policy": "Selama aktif + 5 tahun",
        "access_restriction": "Role: admin, sdm",
    },
    "silver_penelitian_pkm": {
        "contains_pii": False,
        "pii_columns": ["dosen_id"],
        "data_classification": "Public",
        "retention_policy": "Permanen",
        "access_restriction": "Role: semua",
    },
    "silver_kerjasama_aktif": {
        "contains_pii": False,
        "pii_columns": [],
        "data_classification": "Public",
        "retention_policy": "Permanen",
        "access_restriction": "Role: semua",
    },
    "silver_akreditasi_aktif": {
        "contains_pii": False,
        "pii_columns": [],
        "data_classification": "Public",
        "retention_policy": "Permanen",
        "access_restriction": "Role: semua",
    },
}


# ---------------------------------------------------------------------------
# HTTP helper (reuse pattern dari register_bronze_metadata.py)
# ---------------------------------------------------------------------------

def _atlas_request(method: str, path: str, data: dict | None = None) -> dict | None:
    url = f"{ATLAS_URL}{path}"
    cred = base64.b64encode(f"{ATLAS_USER}:{ATLAS_PASS}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Basic {cred}",
    }
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
    return {
        "name": name,
        "typeName": type_name,
        "cardinality": "SINGLE",
        "isOptional": True,
        "isUnique": False,
        "isIndexable": True,
    }


# ---------------------------------------------------------------------------
# 1. Extend types — Silver-specific classifications & attributes
# ---------------------------------------------------------------------------

def create_silver_types():
    """Tambah classification Silver + extend lakehouse_dataset attributes."""
    payload = {
        "enumDefs": [],
        "structDefs": [],
        "classificationDefs": [
            {
                "name": "Silver_Layer",
                "description": "Data di Silver layer (cleaned & enriched)",
                "superTypes": [],
                "attributeDefs": [],
            },
            {
                "name": "Quality_Pass",
                "description": "Data lulus quality check (score ≥ 80%)",
                "superTypes": [],
                "attributeDefs": [],
            },
            {
                "name": "Quality_Quarantine",
                "description": "Data dalam quarantine (score 60-79%)",
                "superTypes": [],
                "attributeDefs": [],
            },
        ],
        "entityDefs": [],
        "relationshipDefs": [],
    }

    logger.info("Creating Silver classification types …")
    result = _atlas_request("PUT", "/api/atlas/v2/types/typedefs", payload)
    if result:
        logger.info("  Silver types OK")
    return result


# ---------------------------------------------------------------------------
# 2. Register Silver entities (dengan enriched metadata)
# ---------------------------------------------------------------------------

def register_silver_entity(table_name: str, profiling: dict) -> dict | None:
    """Register Silver table entity dengan metadata lengkap."""
    biz = BUSINESS_METADATA.get(table_name, {})
    comp = COMPLIANCE_METADATA.get(table_name, {})
    quality = profiling.get("quality", {})

    classifications = [{"typeName": "Silver_Layer"}]
    if quality.get("source_status") == "PASS":
        classifications.append({"typeName": "Quality_Pass"})
    elif quality.get("source_status") == "QUARANTINE":
        classifications.append({"typeName": "Quality_Quarantine"})
    if comp.get("contains_pii"):
        classifications.append({"typeName": "PII"})

    entity = {
        "entity": {
            "typeName": "lakehouse_dataset",
            "attributes": {
                "qualifiedName": f"silver.{table_name}@{CLUSTER_NAME}",
                "name": table_name,
                "description": biz.get("business_description", f"Silver table: {table_name}"),
                "layer": "silver",
                "format": "iceberg",
                "location": f"s3a://warehouse/silver/{table_name}",
                "row_count": profiling.get("row_count", 0),
                "column_count": profiling.get("column_count", 0),
                "schema_def": json.dumps(profiling.get("schema", {})),
                "profiling": json.dumps({
                    "columns": profiling.get("columns", {}),
                    "quality": quality,
                    "business": {
                        "owner": biz.get("business_owner", ""),
                        "iku_relevance": biz.get("iku_relevance", []),
                        "glossary_terms": biz.get("glossary_terms", []),
                        "update_frequency": biz.get("update_frequency", ""),
                    },
                    "compliance": comp,
                    "transformations": profiling.get("transformations", []),
                }),
                "pii_columns": json.dumps(comp.get("pii_columns", [])),
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "enriched_at": datetime.utcnow().isoformat() + "Z",
            },
            "classifications": classifications,
        }
    }

    result = _atlas_request("POST", "/api/atlas/v2/entity", entity)
    if result:
        logger.info("  ✓ Silver entity: %s", table_name)
    return result


# ---------------------------------------------------------------------------
# 3. Lineage: bronze → silver (transformation process)
# ---------------------------------------------------------------------------

def register_silver_lineage(table_name: str, profiling: dict) -> dict | None:
    """Create lineage: bronze sources → ETL → silver table."""
    sources = profiling.get("sources", [])
    transformations = profiling.get("transformations", [])

    inputs = [
        {
            "typeName": "lakehouse_dataset",
            "uniqueAttributes": {
                "qualifiedName": f"bronze.{src}@{CLUSTER_NAME}",
            },
        }
        for src in sources
    ]

    entity = {
        "entity": {
            "typeName": "lakehouse_etl_process",
            "attributes": {
                "qualifiedName": f"etl.bronze_to_silver.{table_name}@{CLUSTER_NAME}",
                "name": f"bronze_to_silver_{table_name}",
                "description": (
                    f"Spark ETL: {' + '.join(sources)} → {table_name}\n"
                    f"Transformations: {'; '.join(transformations)}"
                ),
                "pipeline_name": "bronze_to_silver",
                "source_layer": "bronze",
                "target_layer": "silver",
                "engine": "spark-iceberg",
                "run_timestamp": datetime.utcnow().isoformat() + "Z",
                "inputs": inputs,
                "outputs": [
                    {
                        "typeName": "lakehouse_dataset",
                        "uniqueAttributes": {
                            "qualifiedName": f"silver.{table_name}@{CLUSTER_NAME}",
                        },
                    }
                ],
            },
        }
    }

    result = _atlas_request("POST", "/api/atlas/v2/entity", entity)
    if result:
        src_str = ", ".join(sources)
        logger.info("  ✓ Lineage: [%s] → %s", src_str, table_name)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def register_all_silver_metadata(
    profiling_results: dict,
    atlas_url: str | None = None,
    atlas_user: str | None = None,
    atlas_pass: str | None = None,
) -> bool:
    """Registrasi lengkap Silver metadata ke Atlas."""
    global ATLAS_URL, ATLAS_USER, ATLAS_PASS
    if atlas_url:
        ATLAS_URL = atlas_url
    if atlas_user:
        ATLAS_USER = atlas_user
    if atlas_pass:
        ATLAS_PASS = atlas_pass

    logger.info("=" * 60)
    logger.info("  ATLAS METADATA ENRICHMENT — Silver Layer")
    logger.info("  Atlas: %s  |  Tables: %d", ATLAS_URL, len(profiling_results))
    logger.info("=" * 60)

    create_silver_types()

    success = 0
    for table_name, profiling in profiling_results.items():
        if not profiling.get("written"):
            logger.warning("  ⚠ %s was not written (rejected/error), skipping Atlas", table_name)
            continue

        logger.info("\n── %s ──", table_name)
        try:
            register_silver_entity(table_name, profiling)
            register_silver_lineage(table_name, profiling)
            success += 1
        except Exception as exc:
            logger.error("  ✗ Failed for %s: %s", table_name, exc)

    written_count = sum(1 for r in profiling_results.values() if r.get("written"))
    logger.info(
        "\n✅ Silver registration complete: %d/%d tables",
        success, written_count,
    )

    _log_metadata_summary(profiling_results)
    return success == written_count


def _log_metadata_summary(results: dict):
    """Log ringkasan metadata yang telah didaftarkan."""
    logger.info("\n" + "=" * 60)
    logger.info("  METADATA ENRICHMENT SUMMARY")
    logger.info("=" * 60)

    for name, prof in results.items():
        if not prof.get("written"):
            continue
        quality = prof.get("quality", {})
        biz = BUSINESS_METADATA.get(name, {})
        comp = COMPLIANCE_METADATA.get(name, {})
        logger.info(
            "\n  %s:"
            "\n    Rows: %s  |  Columns: %d"
            "\n    Quality: %s (source=%.1f%%)"
            "\n    Clean Metadata: schema + profiling ✓"
            "\n    Quality Metadata: score=%.1f%% status=%s ✓"
            "\n    Transformation Lineage: %d steps ✓"
            "\n    Business Metadata: owner=%s, IKU=%s ✓"
            "\n    Compliance Metadata: PII=%s, class=%s ✓",
            name,
            f"{prof.get('row_count', 0):,}", prof.get("column_count", 0),
            quality.get("source_status", "?"), quality.get("source_score", 0),
            quality.get("silver_completeness", 0), quality.get("source_status", "?"),
            len(prof.get("transformations", [])),
            biz.get("business_owner", "N/A"),
            ",".join(biz.get("iku_relevance", [])),
            comp.get("contains_pii", False),
            comp.get("data_classification", "N/A"),
        )


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            profiling = json.load(f)
        register_all_silver_metadata(profiling)
    else:
        print("Usage: python register_silver_metadata.py <silver_profiling.json>")
