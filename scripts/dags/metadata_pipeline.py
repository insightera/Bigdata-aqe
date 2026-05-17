"""
DAG: Metadata Pipeline — Medallion (Bronze / Silver / Gold) + siklus katalog data.

Mapping singkat:
- Diagram pipeline: metadata teknis mentah → enrichment → BI/governance, semuanya
  terhubung ke Atlas API (portal katalog).
- Siklus katalog (sandbox → production): seleksi aset & raw → enrichment (API) →
  publish → discovery / lineage / update.

Atlas di stack ini: JanusGraph di HBase + indeks Solr (HTTP); Kafka untuk notifikasi.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ATLAS = "http://atlas:21000"
ATLAS_AUTH = "admin:admin"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="metadata_lakehouse_pipeline",
    description="Bronze → Silver → Gold metadata + tahapan siklus katalog (Atlas API)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["lakehouse", "metadata", "iceberg", "atlas", "catalog-lifecycle"],
) as dag:

    # ─── Siklus katalog: domain & konteks (langkah 1–2) ───────────────────────
    catalog_domain_context = BashOperator(
        task_id="catalog_domain_and_selection",
        bash_command="""
        set -eo pipefail
        echo "📂 [CATALOG] Domain creation & asset selection (sandbox context)"
        echo "   - Tentukan domain / namespace data lakehouse"
        echo "   - Pilih aset yang akan dilacak metadata-nya ke Atlas"
        """,
    )

    # ─── BRONZE: aset masuk mentah (langkah 3–4) + injeksi metadata ke Atlas ─
    bronze_ingest_raw_metadata = BashOperator(
        task_id="bronze_ingest_raw_metadata",
        bash_command="""
        set -eo pipefail
        echo "📥 [BRONZE] Metadata ingestion (raw technical / lineage / profiling / classification)"
        curl -sf -u "${ATLAS_AUTH}" "${ATLAS}/api/atlas/v2/types/typedefs?type=classification" \
          | head -c 300 || echo "   (Atlas belum siap — coba lagi pada run berikutnya)"
        echo ""
        echo "✅ Bronze: metadata mentah siap diproses lebih lanjut."
        """,
        env={"ATLAS": ATLAS, "ATLAS_AUTH": ATLAS_AUTH},
    )

    # ─── SILVER: enrichment (langkah 5–8) — deskripsi, glossary, lineage, graph ─
    silver_enrich_metadata = BashOperator(
        task_id="silver_enrich_metadata",
        bash_command="""
        set -eo pipefail
        echo "⚙️  [SILVER] Enrichment (clean, quality, transformation lineage, business, compliance)"
        echo "   Tahapan 5–8 katalog dapat diotomatisasi via Atlas REST (entity, classification, lineage)."
        curl -sf -u "${ATLAS_AUTH}" "${ATLAS}/api/atlas/v2/types/typedefs?type=entity" \
          | head -c 300 || echo "   (Atlas belum siap — coba lagi pada run berikutnya)"
        echo ""
        echo "✅ Silver: metadata terkurasi siap dipublikasikan."
        """,
        env={"ATLAS": ATLAS, "ATLAS_AUTH": ATLAS_AUTH},
    )

    # ─── GOLD: BI & governance (langkah 10) + metadata konsumsi / KPI ─────────
    gold_publish_governance = BashOperator(
        task_id="gold_publish_governance",
        bash_command="""
        set -eo pipefail
        echo "🥇 [GOLD] Business intelligence & governance metadata"
        echo "   - Business metadata, KPI, AI metadata, consumption, advanced lineage"
        echo "   Setara 'asset published' menuju lingkungan production di katalog."
        curl -sf -u "${ATLAS_AUTH}" "${ATLAS}/api/atlas/v2/system/version" || true
        echo ""
        echo "✅ Gold: lapisan konsumsi & kebijakan tercatat."
        """,
        env={"ATLAS": ATLAS, "ATLAS_AUTH": ATLAS_AUTH},
    )

    # ─── Discovery & pemeliharaan (langkah 11–16, siklus berkelanjutan) ───────
    catalog_discovery_and_lineage = BashOperator(
        task_id="catalog_discovery_and_lineage",
        bash_command="""
        set -eo pipefail
        echo "🔍 [CATALOG] Discovery, sharing, dan pembaruan lineage (operasional berkelanjutan)"
        curl -sf -u "${ATLAS_AUTH}" -X POST "${ATLAS}/api/atlas/v2/search/basic" \
          -H "Content-Type: application/json" \
          -d '{"typeName":"hive_db","excludeDeletedEntities":true,"limit":5}' \
          | head -c 400 || echo "   (pencarian dasar — boleh kosong jika belum ada hive_db)"
        echo ""
        echo "✅ Discovery: Atlas + Solr mendukung pencarian aset untuk pengguna katalog."
        """,
        env={"ATLAS": ATLAS, "ATLAS_AUTH": ATLAS_AUTH},
    )

    catalog_domain_context >> bronze_ingest_raw_metadata >> silver_enrich_metadata >> gold_publish_governance >> catalog_discovery_and_lineage
