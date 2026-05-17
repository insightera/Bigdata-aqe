# Gold → Serving Layer: Trino + Apache Superset

Panduan menyajikan data **Gold Layer** ke lapisan konsumsi (serving) melalui **Trino** sebagai query engine SQL dan **Apache Superset** sebagai dashboard BI. Selaras dengan §9–§10 pada [`../README.md`](../README.md).

**Prasyarat:** stack berjalan (`./start.sh`), pipeline **Silver → Gold** selesai ([`../silver-to-gold/README.md`](../silver-to-gold/README.md)).

---

## 1. Arsitektur serving layer

```
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Iceberg / Parquet di MinIO)                        │
│  Star schema: 5 dimensi + 10 fakta IKU                          │
│  Namespace Hive/Iceberg: lakehouse.gold.*                       │
└────────────────────────────┬────────────────────────────────────┘
                             │ metadata (Hive Metastore)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  SERVING — QUERY ENGINE                                         │
│  Trino (katalog: lakehouse, connector: Iceberg + Hive MS)       │
│  Port: http://localhost:18088                                   │
└────────────────────────────┬────────────────────────────────────┘
                             │ SQL (JDBC / sqlalchemy-trino)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  SERVING — PRESENTATION                                         │
│  Apache Superset — dataset, chart, dashboard                    │
│  Port: http://localhost:18089  (admin / admin)                  │
└─────────────────────────────────────────────────────────────────┘
```

| Lapisan | Komponen | Peran |
|---------|----------|--------|
| **Storage** | MinIO `s3a://warehouse/` | File Parquet/Iceberg fisik |
| **Catalog** | Hive Metastore + Iceberg | Definisi tabel `gold.*` |
| **Query** | **Trino** | Eksekusi SQL interaktif & untuk Superset |
| **Visualisasi** | **Superset** | KPI dashboard, drill-down, laporan |

Spark **tidak** dipakai untuk query BI rutin setelah Gold terbentuk; konsumen membaca lewat Trino (pola *query federation* pada lakehouse).

---

## 2. Model OLAP yang digunakan

### 2.1 Star schema (skema bintang)

Gold layer memakai **star schema** klasik (Kimball):

| Tipe | Tabel | Peran OLAP |
|------|-------|------------|
| **Dimensi** | `dim_waktu`, `dim_prodi`, `dim_dosen`, `dim_mahasiswa`, `dim_topik_penelitian` | Konteks analitik (who, when, where) |
| **Fakta** | `fact_iku1_lulusan` … `fact_iku8_*`, `fact_tata_kelola`, `fact_rekap_iku_institusi` | Metrik terukur (measures) |

Relasi: setiap **fact** memiliki foreign key ke satu atau lebih **dimension** (misalnya `fact_iku1_lulusan.prodi_id` → `dim_prodi`).

### 2.2 Jenis OLAP: ROLAP via Trino (bukan MOLAP cube terpisah)

| Pendekatan | Dipakai di penelitian ini? | Keterangan |
|------------|----------------------------|------------|
| **ROLAP** (Relational OLAP) | **Ya** | Trino menjalankan SQL langsung ke tabel Iceberg di MinIO |
| **MOLAP** (cube OLAP ter-preaggregate) | Tidak | Tidak ada SSAS / Mondrian cube server |
| **HOLAP** | Tidak | — |

**Kesimpulan untuk laporan:** penelitian memakai **ROLAP star schema** yang materialisasi fisiknya berupa tabel **Apache Iceberg**; **Trino** berperan sebagai mesin query SQL; **Superset** sebagai lapisan presentasi (semantik chart + dashboard).

Operasi OLAP yang didukung lewat SQL Trino + Superset:

| Operasi OLAP | Implementasi | Contoh |
|--------------|--------------|--------|
| **Slice** | `WHERE` pada dimensi | Filter `tahun = 2024` |
| **Dice** | `WHERE` multi-dimensi | `tahun = 2024 AND nama_jurusan = 'JTK'` |
| **Roll-up** | Agregasi ke level lebih tinggi | `GROUP BY` jurusan, bukan prodi |
| **Drill-down** | Join ke dimensi lebih detail | Dari rekap institusi → per prodi |
| **Pivot** | Superset Pivot Table / heatmap | Capaian IKU per prodi × tahun |

### 2.3 Dampak AQE terhadap serving

Optimasi **AQE** terjadi utama di **Silver** (Spark). Gold dan query Trino **menerima manfaat tidak langsung**: data sudah terkurasi dan agregasi lebih ringan. Untuk BAB IV, ukur juga **waktu query Trino** pada tabel Gold setelah pipeline dijalankan dengan AQE OFF vs ON di Silver.

---

## 3. Verifikasi Gold sebelum serving

### 3.1 Pastikan tabel Gold ada (Spark)

```bash
docker exec lhaqe-spark-master /opt/spark/bin/spark-sql \
  -e "SHOW TABLES IN lakehouse.gold"
```

### 3.2 Uji Trino CLI

```bash
docker exec -it lhaqe-trino trino --server http://localhost:8080
```

```sql
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.gold;

SELECT COUNT(*) FROM lakehouse.gold.fact_rekap_iku_institusi;
SELECT COUNT(*) FROM lakehouse.gold.dim_prodi;
```

Jika schema `gold` tidak muncul, pastikan Hive Metastore sudah mendaftar namespace dan pipeline `silver_to_gold_pipeline` sukses.

---

## 4. Konfigurasi Trino (sudah di repo)

Katalog `lakehouse` didefinisikan di [`../../trino/etc/catalog/lakehouse.properties`](../../trino/etc/catalog/lakehouse.properties):

- Connector: **Iceberg**
- Metastore: **Hive** (`thrift://hive-metastore:9083`)
- Storage: **MinIO** (`s3://warehouse/`)

Tidak perlu langkah tambahan jika stack dari `docker-compose.yml` sudah naik.

**Akses dari host:** http://localhost:18088  
**UI Trino (jika diaktifkan):** coordinator di port yang sama.

---

## 5. Menghubungkan Superset ke Trino

### 5.1 Login Superset

1. Buka http://localhost:18089  
2. Login: `admin` / `admin` (default dev)

### 5.2 Tambah database (koneksi Trino)

**Settings → Database → + Database**

| Field | Nilai (dari container Superset) | Nilai (dari browser host) |
|-------|----------------------------------|---------------------------|
| **Supported databases** | Trino | Trino |
| **Display name** | `Lakehouse Trino` | sama |
| **SQLAlchemy URI** | `trino://admin@trino:8080/lakehouse` | `trino://admin@localhost:18088/lakehouse` |

Parameter opsional di URI:

```text
trino://admin@trino:8080/lakehouse?source=superset&session_properties=query_max_run_time=10m
```

Klik **Test connection** → **Connect**.

> Driver `sqlalchemy-trino` sudah dipasang di image [`../../superset/Dockerfile`](../../superset/Dockerfile).

### 5.3 Buat dataset

Untuk setiap tabel Gold yang akan divisualisasikan:

1. **Data → Datasets → + Dataset**
2. Database: `Lakehouse Trino`
3. Schema: `gold`
4. Table: pilih tabel (mis. `fact_rekap_iku_institusi`)

**Dataset inti untuk dashboard IKU:**

| Dataset | Tipe | Penggunaan dashboard |
|---------|------|----------------------|
| `fact_rekap_iku_institusi` | Fact | Executive summary 8 IKU |
| `fact_iku1_lulusan` | Fact | Drill-down per prodi |
| `dim_prodi` | Dimension | Filter jurusan/prodi |
| `dim_waktu` | Dimension | Filter tahun/semester |

### 5.4 Relasi antar dataset (opsional)

Di **Dataset → Edit → Metrics & columns**, definisikan metrik:

- `nilai_capaian` — AVG atau MAX  
- `nilai_target` — MAX  
- `capaian_pct` — calculated: `nilai_capaian / nilai_target`

Untuk join antar dataset di Superset 4.x: gunakan **Virtual dataset** (SQL Lab) dengan SQL eksplisit (disarankan untuk star schema).

---

## 6. Contoh virtual dataset (SQL Lab → Save as dataset)

### 6.1 Rekap IKU per tahun (executive)

```sql
SELECT
  w.tahun,
  r.iku_kode,
  r.iku_nama,
  r.nilai_capaian,
  r.nilai_target,
  r.status_capaian,
  CASE
    WHEN r.nilai_target > 0 THEN r.nilai_capaian / r.nilai_target
    ELSE NULL
  END AS rasio_capaian
FROM lakehouse.gold.fact_rekap_iku_institusi r
JOIN lakehouse.gold.dim_waktu w ON r.waktu_id = w.waktu_id
ORDER BY w.tahun, r.iku_kode;
```

Simpan sebagai dataset **「IKU Rekap Institusi」** → buat chart **Bar chart** (iku_kode × nilai_capaian), **gauge** atau **big number** per IKU.

### 6.2 IKU-1 per program studi (drill-down)

```sql
SELECT
  p.nama_prodi,
  p.nama_jurusan,
  p.jenjang,
  f.total_lulusan,
  f.persen_terserap,
  f.target_iku,
  f.capaian_iku
FROM lakehouse.gold.fact_iku1_lulusan f
JOIN lakehouse.gold.dim_prodi p ON f.prodi_id = p.prodi_id
ORDER BY f.capaian_iku DESC;
```

Chart: **table** atau **bar** — filter interaktif `nama_jurusan` di dashboard.

### 6.3 Pivot capaian vs target (OLAP roll-up)

```sql
SELECT
  w.tahun,
  p.nama_jurusan,
  r.iku_kode,
  AVG(r.nilai_capaian) AS avg_capaian,
  AVG(r.nilai_target) AS avg_target
FROM lakehouse.gold.fact_rekap_iku_institusi r
JOIN lakehouse.gold.dim_waktu w ON r.waktu_id = w.waktu_id
CROSS JOIN lakehouse.gold.dim_prodi p
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

*(Sesuaikan join jika fact sudah membawa `prodi_id` — lihat skema aktual di `SHOW COLUMNS`.)*

---

## 7. Susunan dashboard Superset (rekomendasi)

### Dashboard: **Executive IKU ITERA**

| No | Chart | Sumber | Tipe visual |
|----|-------|--------|-------------|
| 1 | Ringkasan 8 IKU | `fact_rekap_iku_institusi` | Bar / bullet |
| 2 | Status capaian (Tercapai / On track) | `status_capaian` | Pie |
| 3 | Trend per tahun | join `dim_waktu` | Line |
| 4 | IKU-1 per prodi | `fact_iku1` + `dim_prodi` | Table + bar |
| 5 | Filter global | `dim_waktu`, `dim_prodi` | Native filter |

### Dashboard: **Evaluasi AQE (penelitian)**

Jika metrik eksperimen diekspor ke Postgres atau tabel Gold khusus:

| Panel | Data |
|-------|------|
| Execution time OFF vs ON | `metrics/*.json` → ETL ke tabel `gold.experiment_aqe` (opsional) |
| Speedup % | calculated di Superset |

Untuk fase awal, panel AQE bisa tetap di **Grafana** ([`../monitoring-grafana/README.md`](../monitoring-grafana/README.md)); dashboard Superset fokus **KPI bisnis**.

---

## 8. Workload query Trino (untuk pengukuran performa)

Jalankan query yang sama setelah pipeline Silver dengan **AQE OFF** dan **AQE ON**, catat waktu di Trino:

```sql
-- Join workload
SELECT p.nama_prodi, COUNT(*) AS n
FROM lakehouse.gold.fact_iku1_lulusan f
JOIN lakehouse.gold.dim_prodi p ON f.prodi_id = p.prodi_id
GROUP BY p.nama_prodi;

-- Aggregation workload
SELECT w.tahun, AVG(r.nilai_capaian) AS avg_capaian
FROM lakehouse.gold.fact_rekap_iku_institusi r
JOIN lakehouse.gold.dim_waktu w ON r.waktu_id = w.waktu_id
GROUP BY w.tahun;

-- Filtering workload
SELECT *
FROM lakehouse.gold.fact_rekap_iku_institusi
WHERE status_capaian = 'Tidak Tercapai';
```

Catat dari Trino UI atau:

```sql
SHOW STATS FOR lakehouse.gold.fact_rekap_iku_institusi;
```

---

## 9. Troubleshooting

| Gejala | Penyebab | Solusi |
|--------|----------|--------|
| `Schema gold not found` | Pipeline Gold belum jalan | Trigger `silver_to_gold_pipeline` |
| Trino: table not found | Metastore belum sync | `SHOW TABLES FROM lakehouse.gold`; restart hive-metastore |
| Superset: connection failed | URI salah / Trino down | Pakai `trino:8080` dari dalam Docker |
| Query lambat | Data besar, no partition | Filter `waktu_id` / `tahun`; pertimbangkan partition Iceberg |
| Chart kosong | Dataset schema salah | Cek preview dataset di Superset |

---

## 10. Ringkasan alur kerja (checklist)

1. [ ] `./start.sh` — Trino + Superset healthy  
2. [ ] Pipeline Gold selesai (`silver_to_gold_pipeline`)  
3. [ ] `SHOW TABLES FROM lakehouse.gold` di Trino  
4. [ ] Koneksi Superset → Trino (`Lakehouse Trino`)  
5. [ ] Dataset + virtual SQL untuk fact/dim  
6. [ ] Dashboard Executive IKU  
7. [ ] (Opsional) Ukur query latency Trino untuk laporan dampak AQE di Gold  

**Dokumen terkait:** [`../README.md`](../README.md) · [`../silver-to-gold/README.md`](../silver-to-gold/README.md) · [`../monitoring-grafana/README.md`](../monitoring-grafana/README.md)
