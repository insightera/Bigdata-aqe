# Pipeline 3: Silver → Gold (Consumption Layer)

Panduan membangun **star schema IKU** di layer Gold dan menyiapkan data untuk **Trino** (query engine) serta **Apache Superset** (dashboard). Transformasi Gold memanfaatkan data yang sudah dioptimasi di Silver; pengukuran AQE di Gold biasanya berupa **query konsumsi** (agregasi BI), bukan ulang seluruh eksperimen shuffle di Silver.

```
Silver (6 tabel enriched)
        │
        ▼
  PySpark star schema  →  Gold (5 dim + 10 fact, Iceberg)
        │
        ├─→ Trino (SQL interaktif, workload BI)
        ├─→ Superset (dashboard KPI IKU)
        └─→ Grafana (runtime query Trino/Spark jika di-instrument)
```

Diagram arsitektur: [`../../pipeline-aqe.png`](../../pipeline-aqe.png).

---

## Star schema

### Dimensi (5)

| Tabel | Sumber | Deskripsi |
|-------|--------|-----------|
| `dim_waktu` | Generated | Tahun 2020–2025, semester, triwulan |
| `dim_prodi` | `raw_prodi` | Program studi, jurusan, fakultas |
| `dim_dosen` | `silver_dosen` | Profil + kualifikasi (S3, serdos) |
| `dim_mahasiswa` | `silver_mahasiswa` | Profil + demografi |
| `dim_topik_penelitian` | Generated | Topik strategis Renstra |

### Fakta IKU (10)

| Tabel | IKU | Sumber utama |
|-------|-----|----------------|
| `fact_iku1_lulusan` | IKU-1 | `silver_lulusan` |
| `fact_iku2_mbkm` | IKU-2 | `silver_mahasiswa` + prestasi |
| `fact_iku3_dosen_tridarma` | IKU-3 | `silver_dosen` |
| `fact_iku4_kualifikasi_dosen` | IKU-4 | `silver_dosen` |
| `fact_iku5_penelitian_pkm` | IKU-5 | `silver_penelitian_pkm` |
| `fact_iku6_kerjasama_prodi` | IKU-6 | `silver_kerjasama_aktif` |
| `fact_iku7_metode_pembelajaran` | IKU-7 | simulasi dari prodi |
| `fact_iku8_akreditasi_intl` | IKU-8 | `silver_akreditasi_aktif` |
| `fact_tata_kelola` | SAKIP | `raw_keuangan` |
| `fact_rekap_iku_institusi` | All | agregasi eksekutif |

Target Renstra (contoh): lihat tabel target di [`../../README.md`](../../README.md) atau dokumen Renstra — dipakai di panel Superset sebagai garis target.

---

## Menjalankan pipeline

### 1. Pastikan Silver siap

```bash
docker exec <airflow-scheduler> airflow dags trigger bronze_to_silver_pipeline \
  --conf '{"aqe_scenario": "ON"}'
```

Verifikasi:

```bash
# Spark
docker exec <spark-master> spark-sql \
  -e "SHOW TABLES IN lakehouse.silver"
```

### 2. Trigger DAG Gold

**Airflow UI:** DAG `silver_to_gold_pipeline` → Trigger.

**CLI:**

```bash
docker exec <airflow-scheduler> airflow dags trigger silver_to_gold_pipeline
```

**Konfigurasi Spark di Gold:** ETL star schema bisa memakai **AQE ON** (default produksi) karena banyak join/agregasi; untuk isolasi hipotesis, dokumentasikan apakah capaian Gold diukur **hanya via Trino** atau juga via Spark write.

### 3. Task pipeline (ringkas)

| Task | Isi |
|------|-----|
| `silver_to_gold` | Build dim + fact, tulis `lakehouse.gold.*` |
| `verify_gold_tables` | `COUNT(*)`, sanity check FK |
| `record_gold_metrics` | Waktu pipeline, baris per fact (log / JSON untuk Grafana) |

> **Tidak ada** registrasi Apache Atlas. Metadata bisnis KPI disajikan lewat **Superset** (dataset + chart), bukan katalog Atlas.

---

## Verifikasi Gold

### Spark / spark-sql

```sql
SHOW TABLES IN lakehouse.gold;

SELECT iku_kode, iku_nama, nilai_capaian, nilai_target, status_capaian
FROM lakehouse.gold.fact_rekap_iku_institusi
WHERE waktu_id = 202412
ORDER BY iku_kode;
```

### Trino

Setelah connector Hive/Iceberg dikonfigurasi di `docker-compose` (katalog `lakehouse`):

```sql
SHOW TABLES FROM lakehouse.gold;

SELECT iku_kode, nilai_capaian, nilai_target, status_capaian
FROM lakehouse.gold.fact_rekap_iku_institusi
WHERE waktu_id = 202412
ORDER BY iku_kode;
```

**Workload eksperimen di Gold (untuk BAB IV §4.1.6):**

| Query | Tipe workload |
|-------|----------------|
| Rekap IKU per tahun | Aggregation |
| IKU-1 per prodi (join fact–dim) | Join |
| Filter capaian di bawah target | Filtering |

Jalankan query yang sama pada cluster yang Silver-nya dihasilkan dengan **AQE OFF** vs **AQE ON** (dua run pipeline penuh 1→2→3, atau dua snapshot Silver jika Anda menyimpan keduanya).

---

## Apache Superset (dashboard)

Setelah Superset running:

1. **Database connection** → Trino (`trino://trino@trino:8080/lakehouse` — sesuaikan host/port compose).
2. **Dataset** → `gold.fact_rekap_iku_institusi`, `gold.fact_iku1_lulusan`, dll.
3. **Dashboard contoh:**
   - Executive KPI: 8 IKU + status capaian vs target
   - Per prodi: drill-down `dim_prodi`
   - Panel eksperimen (opsional): tabel hasil benchmark AQE dari view `metrics.aqe_results` jika diekspor ke Postgres/Trino

Superset menggantikan peran **portal katalog / BI** pada diagram lama; tidak memerlukan Atlas.

---

## Grafana (monitoring)

Panel yang relevan setelah pipeline Gold:

| Panel | Sumber |
|-------|--------|
| Pipeline duration (1→2→3) | Airflow / JSON metrics |
| Trino query latency | Trino JMX / exporter |
| Spark stage time (Gold ETL) | Spark UI / Prometheus |
| Perbandingan AQE OFF vs ON | Label `scenario` dari benchmark Silver |

---

## Contoh query OLAP (Trino / Superset)

```sql
-- Capaian IKU per tahun
SELECT w.tahun, r.iku_kode, r.iku_nama,
       r.nilai_capaian, r.nilai_target, r.status_capaian
FROM lakehouse.gold.fact_rekap_iku_institusi r
JOIN lakehouse.gold.dim_waktu w ON r.waktu_id = w.waktu_id
ORDER BY w.tahun, r.iku_kode;

-- IKU-1 per prodi
SELECT p.nama_prodi, f.total_lulusan, f.persen_terserap, f.target_iku
FROM lakehouse.gold.fact_iku1_lulusan f
JOIN lakehouse.gold.dim_prodi p ON f.prodi_id = p.prodi_id
ORDER BY f.capaian_iku DESC;
```

---

## Troubleshooting

| Gejala | Tindakan |
|--------|----------|
| `silver.*` not found | Jalankan pipeline 2 |
| Gold kosong | Cek log Spark; pastikan Silver PASS |
| Trino tidak melihat tabel | Refresh metastore; cek `hive.metastore.uris` dan warehouse S3 |
| Superset error koneksi | Test `trino` CLI dari container Superset |

---

## Alur Medallion lengkap

```
Pipeline 1: Staging → Bronze     (ingest, Parquet/Iceberg)
Pipeline 2: Bronze → Silver      (AQE OFF vs ON — hipotesis utama)
Pipeline 3: Silver → Gold        (star schema — konsumsi)
Konsumsi: Trino + Superset
Observabilitas: Grafana (+ Prometheus)
```

---

## Arsitektur file

```
scripts/
├── spark/silver_to_gold.py
└── dags/silver_gold_pipeline.py
```

**Dokumen terkait:** [`../README.md`](../README.md) (arsitektur AQE), [`../../README.md`](../../README.md) (ringkasan penelitian & metrik).
