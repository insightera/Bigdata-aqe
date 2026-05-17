"""
Atlas Metadata Registration — Bronze Layer
============================================
Mendaftarkan metadata Bronze ke Apache Atlas via REST API v2:
  1. Custom type definitions  (lakehouse_dataset, lakehouse_etl_process)
  2. Entity per tabel          (staging CSV + Bronze Iceberg)
  3. Lineage                   (staging → bronze via ETL process)
  4. Classification            (PII, Bronze_Layer, Staging_Layer)
  5. Profiling metadata        (row_count, null_pct, completeness, dll.)

Menggunakan urllib (stdlib) sehingga bisa berjalan tanpa pip install tambahan.
"""

import base64
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime

logger = logging.getLogger("atlas_metadata")

ATLAS_URL = "http://atlas:21000"
ATLAS_USER = "admin"
ATLAS_PASS = "admin"
CLUSTER_NAME = "lakehouse"


# ---------------------------------------------------------------------------
# HTTP helper
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
            logger.debug("Type/entity already exists (409) — OK")
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


# ---------------------------------------------------------------------------
# 1. Type definitions
# ---------------------------------------------------------------------------

def create_lakehouse_types():
    """Buat/update custom Atlas types untuk lakehouse pipeline."""
    payload = {
        "enumDefs": [],
        "structDefs": [],
        "classificationDefs": [
            {
                "name": "PII",
                "description": "Personally Identifiable Information",
                "superTypes": [],
                "attributeDefs": [],
            },
            {
                "name": "Bronze_Layer",
                "description": "Data di Bronze layer (raw Iceberg tables)",
                "superTypes": [],
                "attributeDefs": [],
            },
            {
                "name": "Staging_Layer",
                "description": "Data di Staging layer (raw CSV files)",
                "superTypes": [],
                "attributeDefs": [],
            },
        ],
        "entityDefs": [
            {
                "name": "lakehouse_dataset",
                "description": "Dataset dalam data lakehouse (staging/bronze/silver/gold)",
                "superTypes": ["DataSet"],
                "attributeDefs": [
                    _attr("layer", "string"),
                    _attr("format", "string"),
                    _attr("location", "string"),
                    _attr("row_count", "long"),
                    _attr("column_count", "int"),
                    _attr("schema_def", "string"),
                    _attr("profiling", "string"),
                    _attr("pii_columns", "string"),
                    _attr("ingested_at", "string"),
                ],
            },
            {
                "name": "lakehouse_etl_process",
                "description": "Proses ETL dalam pipeline lakehouse",
                "superTypes": ["Process"],
                "attributeDefs": [
                    _attr("pipeline_name", "string"),
                    _attr("source_layer", "string"),
                    _attr("target_layer", "string"),
                    _attr("engine", "string"),
                    _attr("run_timestamp", "string"),
                ],
            },
        ],
        "relationshipDefs": [],
    }

    logger.info("Creating/updating Atlas types …")
    result = _atlas_request("PUT", "/api/atlas/v2/types/typedefs", payload)
    if result:
        entity_count = len(result.get("entityDefs", []))
        class_count = len(result.get("classificationDefs", []))
        logger.info("  Types OK — %d entity defs, %d classification defs", entity_count, class_count)
    return result


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
# 2. Entity registration
# ---------------------------------------------------------------------------

def register_staging_entity(table_name: str) -> dict | None:
    """Daftarkan file CSV staging sebagai entity Atlas."""
    entity = {
        "entity": {
            "typeName": "lakehouse_dataset",
            "attributes": {
                "qualifiedName": f"staging.{table_name}@{CLUSTER_NAME}",
                "name": table_name,
                "description": f"Raw CSV source file: {table_name}.csv",
                "layer": "staging",
                "format": "csv",
                "location": f"s3a://staging/{table_name}.csv",
            },
            "classifications": [{"typeName": "Staging_Layer"}],
        }
    }
    result = _atlas_request("POST", "/api/atlas/v2/entity", entity)
    if result:
        logger.info("  ✓ Staging entity: %s", table_name)
    return result


def register_bronze_entity(table_name: str, profiling: dict) -> dict | None:
    """Daftarkan tabel Iceberg Bronze sebagai entity Atlas + metadata profiling."""
    classifications = [{"typeName": "Bronze_Layer"}]
    if profiling.get("pii_columns"):
        classifications.append({"typeName": "PII"})

    entity = {
        "entity": {
            "typeName": "lakehouse_dataset",
            "attributes": {
                "qualifiedName": f"bronze.{table_name}@{CLUSTER_NAME}",
                "name": f"bronze_{table_name}",
                "description": f"Iceberg table in Bronze layer: lakehouse.bronze.{table_name}",
                "layer": "bronze",
                "format": "iceberg",
                "location": f"s3a://warehouse/bronze/{table_name}",
                "row_count": profiling.get("row_count", 0),
                "column_count": profiling.get("column_count", 0),
                "schema_def": json.dumps(profiling.get("schema", {})),
                "profiling": json.dumps(profiling.get("columns", {})),
                "pii_columns": json.dumps(profiling.get("pii_columns", [])),
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            },
            "classifications": classifications,
        }
    }
    result = _atlas_request("POST", "/api/atlas/v2/entity", entity)
    if result:
        logger.info("  ✓ Bronze entity: %s (rows=%s)", table_name, f"{profiling.get('row_count', 0):,}")
    return result


# ---------------------------------------------------------------------------
# 3. Lineage  (staging CSV → ETL process → bronze Iceberg)
# ---------------------------------------------------------------------------

def register_lineage(table_name: str) -> dict | None:
    """Buat lineage: staging.{table} -[ETL]→ bronze.{table}."""
    entity = {
        "entity": {
            "typeName": "lakehouse_etl_process",
            "attributes": {
                "qualifiedName": f"etl.staging_to_bronze.{table_name}@{CLUSTER_NAME}",
                "name": f"staging_to_bronze_{table_name}",
                "description": f"Spark+Iceberg ETL: CSV→Iceberg for {table_name}",
                "pipeline_name": "staging_to_bronze",
                "source_layer": "staging",
                "target_layer": "bronze",
                "engine": "spark-iceberg",
                "run_timestamp": datetime.utcnow().isoformat() + "Z",
                "inputs": [
                    {
                        "typeName": "lakehouse_dataset",
                        "uniqueAttributes": {
                            "qualifiedName": f"staging.{table_name}@{CLUSTER_NAME}",
                        },
                    }
                ],
                "outputs": [
                    {
                        "typeName": "lakehouse_dataset",
                        "uniqueAttributes": {
                            "qualifiedName": f"bronze.{table_name}@{CLUSTER_NAME}",
                        },
                    }
                ],
            },
        }
    }
    result = _atlas_request("POST", "/api/atlas/v2/entity", entity)
    if result:
        logger.info("  ✓ Lineage: staging.%s → bronze.%s", table_name, table_name)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def register_all_metadata(
    profiling_results: dict,
    atlas_url: str | None = None,
    atlas_user: str | None = None,
    atlas_pass: str | None = None,
) -> bool:
    """Registrasi lengkap: types → entities → lineage → classifications."""
    global ATLAS_URL, ATLAS_USER, ATLAS_PASS
    if atlas_url:
        ATLAS_URL = atlas_url
    if atlas_user:
        ATLAS_USER = atlas_user
    if atlas_pass:
        ATLAS_PASS = atlas_pass

    logger.info("=" * 60)
    logger.info("  ATLAS METADATA REGISTRATION — Bronze Layer")
    logger.info("  Atlas: %s  |  Tables: %d", ATLAS_URL, len(profiling_results))
    logger.info("=" * 60)

    create_lakehouse_types()

    success = 0
    for table_name, profiling in profiling_results.items():
        logger.info("\n── %s ──", table_name)
        try:
            register_staging_entity(table_name)
            register_bronze_entity(table_name, profiling)
            register_lineage(table_name)
            success += 1
        except Exception as exc:
            logger.error("  ✗ Failed for %s: %s", table_name, exc)

    logger.info(
        "\n✅ Registration complete: %d/%d tables registered",
        success, len(profiling_results),
    )
    return success == len(profiling_results)


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            profiling = json.load(f)
        register_all_metadata(profiling)
    else:
        print("Usage: python register_bronze_metadata.py <profiling.json>")
        print("       (profiling.json dihasilkan oleh staging_to_bronze.py)")
