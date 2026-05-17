# Template — Runtime Pipeline Medallion (BAB IV §4.1.1)

## Ringkasan eksekusi

| Item | Nilai |
|------|-------|
| Tanggal run produksi | |
| Profil data | aqe |
| Skenario AQE saat Silver (run produksi awal) | OFF / ON |
| Status keseluruhan | Berhasil / Gagal |

## Tabel runtime per tahap

| Tahap | DAG ID | Task ID | Mulai | Selesai | Durasi (s) | Status | Baris / catatan |
|-------|--------|---------|-------|---------|------------|--------|-----------------|
| Staging → Bronze | staging_to_bronze_pipeline | staging_to_bronze | | | | | |
| | | upload_csv_to_staging | | | | | |
| Bronze → Silver | bronze_to_silver_pipeline | bronze_to_silver | | | | | |
| Silver → Gold | silver_to_gold_pipeline | silver_to_gold | | | | | |
| **Total pipeline** | | | | | | | |

Sumber: Airflow UI → DAG Runs → Duration, atau log task.

## Verifikasi output

| Layer | Cek | Hasil |
|-------|-----|-------|
| Bronze | `SHOW TABLES IN lakehouse.bronze` | OK / |
| Silver | `SHOW TABLES IN lakehouse.silver` | OK / |
| Gold | `SHOW TABLES FROM lakehouse.gold` (Trino) | OK / |
| MinIO | bucket warehouse terisi | OK / |

## Screenshot / lampiran

| No | File | Keterangan |
|----|------|------------|
| 1 | | Airflow graph sukses |
| 2 | | Spark UI completed app (ingest) |

## Diagram alur (opsional)

Tempel diagram Medallion atau screenshot pipeline Airflow.

## Catatan error (jika ada)

-
