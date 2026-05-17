# Gold → Serving Layer: Trino + Apache Superset

Panduan menyajikan data **Gold Layer** ke lapisan konsumsi (serving) melalui **Trino** sebagai query engine SQL dan **Apache Superset** sebagai dashboard BI. Selaras dengan §9–§10 pada [`../README.md`](../README.md).

**Prasyarat:** stack berjalan (`./start.sh`), pipeline **Silver → Gold** selesai ([`../silver-to-gold/README.md`](../silver-to-gold/README.md)).

---

## 1. Arsitektur serving layer

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE (satu salinan) — lakehouse.bronze.* → warehouse/        │
└────────────────────────────┬────────────────────────────────────┘
                             │
         ┌───────────────────┴───────────────────┐
         ▼                                       ▼
┌─────────────────────────┐           ┌─────────────────────────┐
│ GOLD AQE OFF            │           │ GOLD AQE ON             │
│ gold_aqe_off.*          │           │ gold_aqe_on.*         │
│ MinIO warehouse-aqe-off │           │ MinIO warehouse-aqe-on│
└────────────┬────────────┘           └────────────┬────────────┘
             │                                     │
             └──────────────────┬──────────────────┘
                                │ metadata (Hive Metastore)
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  SERVING — QUERY ENGINE                                         │
│  Trino: lakehouse | lakehouse_aqe_off | lakehouse_aqe_on      │
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
| **Storage** | MinIO `warehouse/` (bronze), `warehouse-aqe-off/`, `warehouse-aqe-on/` | Dua salinan Gold untuk audit AQE |
| **Catalog** | Hive Metastore + Iceberg | Schema `gold_aqe_off`, `gold_aqe_on` |
| **Query** | **Trino** | Katalog `lakehouse`, `lakehouse_aqe_off`, `lakehouse_aqe_on` |
| **Visualisasi** | **Superset** | Satu atau dua koneksi Trino per skenario |

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

### 2.4 Dua salinan Gold (audit OFF vs ON)

| Skenario | Schema Iceberg | Katalog Trino (disarankan) | URI Superset (contoh) |
|----------|----------------|----------------------------|------------------------|
| AQE OFF | `lakehouse.gold_aqe_off` | `lakehouse_aqe_off` | `trino://admin@trino:8080/lakehouse_aqe_off` |
| AQE ON | `lakehouse.gold_aqe_on` | `lakehouse_aqe_on` | `trino://admin@trino:8080/lakehouse_aqe_on` |
| Semua schema | — | `lakehouse` | `trino://admin@trino:8080/lakehouse` |

Contoh audit baris identik:

```sql
SELECT 'OFF' AS aqe, COUNT(*) FROM lakehouse.gold_aqe_off.dim_mahasiswa
UNION ALL
SELECT 'ON', COUNT(*) FROM lakehouse.gold_aqe_on.dim_mahasiswa;
```

---


## 3. Verifikasi Gold sebelum serving

### 3.1 Pastikan tabel Gold ada

**Disarankan — Trino** (tanpa Ivy/JAR di Spark CLI):

```bash
docker exec lhaqe-trino trino --execute "SHOW TABLES FROM lakehouse.gold_aqe_off"
docker exec lhaqe-trino trino --execute "SHOW TABLES FROM lakehouse.gold_aqe_on"
```

**Alternatif — Spark SQL** (butuh JAR di `lib/`; unduh dulu):

```bash
./scripts/download-jars.sh
docker compose up -d spark-master spark-worker-1 spark-worker-2

# Wrapper (menonaktifkan spark.jars.packages / Ivy)
chmod +x scripts/spark-sql-lakehouse.sh
./scripts/spark-sql-lakehouse.sh "SHOW TABLES IN lakehouse.gold_aqe_off"
```

> Jangan memanggil `spark-sql` mentah tanpa JAR lokal — error `FileNotFoundException` di `/home/spark/.ivy2/cache` artinya Spark mencoba unduh paket Maven dan cache tidak writable.
>
> Error `path must be absolute` pada event log S3A: gunakan `./scripts/spark-sql-lakehouse.sh` (event log dimatikan untuk CLI) atau cek Gold lewat Trino.

### 3.2 Uji Trino CLI (interaktif)

```bash
docker exec -it lhaqe-trino trino --server http://localhost:8080
```

```sql
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.gold_aqe_off;
SHOW TABLES FROM lakehouse.gold_aqe_on;

SELECT COUNT(*) FROM lakehouse.gold_aqe_off.fact_rekap_iku_institusi;
SELECT COUNT(*) FROM lakehouse.gold_aqe_on.fact_rekap_iku_institusi;
```

Jika schema `gold_aqe_off` / `gold_aqe_on` tidak muncul, pastikan DAG `aqe_full_experiment` (task `silver_to_gold_off` / `silver_to_gold_on`) sukses dan bucket MinIO `warehouse-aqe-off` / `warehouse-aqe-on` terisi.

---

## 4. Konfigurasi Trino (sudah di repo)

| Katalog | File properties | Dipakai untuk |
|---------|-----------------|---------------|
| `lakehouse` | [`lakehouse.properties`](../../trino/etc/catalog/lakehouse.properties) | Semua schema: `bronze`, `silver_aqe_off`, `silver_aqe_on`, `gold_aqe_*` |
| `lakehouse_aqe_off` | [`lakehouse_aqe_off.properties`](../../trino/etc/catalog/lakehouse_aqe_off.properties) | Audit / Superset khusus run AQE OFF |
| `lakehouse_aqe_on` | [`lakehouse_aqe_on.properties`](../../trino/etc/catalog/lakehouse_aqe_on.properties) | Audit / Superset khusus run AQE ON |

Semua katalog memakai connector **Iceberg** + **Hive Metastore** (`thrift://hive-metastore:9083`) ke MinIO. Data fisik Gold OFF/ON ada di bucket terpisah (lihat [`lakehouse_catalog.py`](../../scripts/spark/lakehouse_catalog.py)).

```bash
# CLI dengan katalog eksplisit
docker exec lhaqe-trino trino --catalog lakehouse_aqe_off --schema gold_aqe_off \
  --execute "SELECT COUNT(*) FROM fact_iku4_kualifikasi_dosen"
```

Setelah menambah file `.properties`, restart Trino: `docker compose restart trino`.

**Akses dari host:** http://localhost:18088

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
| **SQLAlchemy URI (semua schema)** | `trino://admin@trino:8080/lakehouse` | `trino://admin@localhost:18088/lakehouse` |
| **URI khusus Gold OFF** | `trino://admin@trino:8080/lakehouse_aqe_off` | `trino://admin@localhost:18088/lakehouse_aqe_off` |
| **URI khusus Gold ON** | `trino://admin@trino:8080/lakehouse_aqe_on` | `trino://admin@localhost:18088/lakehouse_aqe_on` |

Parameter opsional di URI:

```text
trino://admin@trino:8080/lakehouse?source=superset&session_properties=query_max_run_time=10m
```

Klik **Test connection** → **Connect**.

> Driver `trino` (trino-python-client, termasuk dialect SQLAlchemy) sudah dipasang di image [`../../superset/Dockerfile`](../../superset/Dockerfile).

### 5.3 Buat dataset

Sesuaikan dengan **tabel yang benar-benar ada** setelah pipeline Gold (`silver_to_gold_off` / `silver_to_gold_on` di DAG [`aqe_full_experiment`](../../scripts/dags/aqe_full_experiment.py)). Cek dulu (ganti schema jika memakai satu koneksi `lakehouse`):

```bash
docker exec lhaqe-trino trino --execute "SHOW TABLES FROM lakehouse.gold"
# atau di MinIO: bucket warehouse → namespace gold (lihat folder Iceberg)
```

#### 5.3.1 Inventaris Gold (hasil eksperimen tipikal)

Setelah `aqe_full_experiment` sukses, layer Gold di MinIO/Iceberg biasanya berisi:

| Tabel | Tipe | Status | Sumber Silver/Bronze |
|-------|------|--------|----------------------|
| `dim_waktu` | Dimensi | ✅ | Generated (2020–2025) |
| `dim_prodi` | Dimensi | ✅ | `raw_prodi` |
| `dim_dosen` | Dimensi | ✅ | `silver_dosen` |
| `dim_mahasiswa` | Dimensi | ✅ | `silver_mahasiswa` |
| `dim_topik_penelitian` | Dimensi | ✅ | Master topik riset |
| `fact_iku4_kualifikasi_dosen` | Fakta | ✅ | `silver_dosen` |
| `fact_iku6_kerjasama_prodi` | Fakta | ✅ | `silver_kerjasama_aktif` |
| `fact_iku7_metode_pembelajaran` | Fakta | ✅ | Simulasi per prodi S1 |
| `fact_iku8_akreditasi_internasional` | Fakta | ✅ | `silver_akreditasi_aktif` |
| `fact_tata_kelola` | Fakta | ✅ | `raw_keuangan` |
| `fact_iku1_lulusan` | Fakta | ✅ | `silver_lulusan` + parse `tanggal_lulus` |
| `fact_iku2_mbkm` | Fakta | ✅ | MBKM **atau** prestasi nasional (logika OR) |
| `fact_iku3_dosen_tridarma` | Fakta | ✅ | `raw_kegiatan_dosen` ⋈ `silver_dosen` |
| `fact_iku5_penelitian_pkm` | Fakta | ✅ | `silver_penelitian_pkm` + `jurusan_id` |
| `fact_rekap_iku_institusi` | Fakta | ✅ | Agregat dari fakta IKU yang berhasil ditulis |

> Jika di MinIO hanya terlihat **5 dimensi + 5 fakta** (seperti screenshot eksperimen), dashboard fokus ke **IKU-4, IKU-6, IKU-7, IKU-8**, dan **tata kelola** — bukan 8 IKU penuh.

#### 5.3.2 Langkah membuat dataset fisik (Superset)

Untuk **setiap tabel yang ada** di `SHOW TABLES`:

1. **Data → Datasets → + Dataset**
2. Database: `Lakehouse Trino`
3. Schema: `gold`
4. Table: pilih nama tabel (contoh: `fact_iku4_kualifikasi_dosen`)

**Daftar dataset fisik yang disarankan (minimum, sesuai data aktual):**

| # | Dataset Superset | Tabel Trino | Kolom kunci untuk chart |
|---|------------------|-------------|-------------------------|
| 1 | `dim_prodi` | `dim_prodi` | `prodi_id`, `nama_prodi`, `nama_jurusan`, `jenjang` |
| 2 | `dim_waktu` | `dim_waktu` | `waktu_id`, `tahun`, `semester` |
| 3 | `dim_dosen` | `dim_dosen` | `dosen_id`, `prodi_id`, `is_s3`, `is_serdos`, `is_praktisi` |
| 4 | `dim_mahasiswa` | `dim_mahasiswa` | `mahasiswa_id`, `prodi_id`, `angkatan` |
| 5 | `fact_iku4_kualifikasi_dosen` | `fact_iku4_kualifikasi_dosen` | `persen_iku4`, `target_iku`, `capaian_iku`, `prodi_id` |
| 6 | `fact_iku6_kerjasama_prodi` | `fact_iku6_kerjasama_prodi` | `persen_iku6`, `prodi_berkerjasama`, `total_prodi_s1` |
| 7 | `fact_iku7_metode_pembelajaran` | `fact_iku7_metode_pembelajaran` | `persen_iku7`, `mk_case_method`, `mk_team_based`, `prodi_id` |
| 8 | `fact_iku8_akreditasi_internasional` | `fact_iku8_akreditasi_internasional` | `persen_iku8`, `prodi_akreditasi_internasional` |
| 9 | `fact_tata_kelola` | `fact_tata_kelola` | `persen_realisasi`, `pagu_total`, `realisasi_total`, `predikat_sakip` |

**Dataset virtual (SQL Lab → Save as dataset)** — untuk join star schema; buat setelah dataset fisik ada:

| Dataset virtual | Join utama | Gunakan untuk |
|-----------------|------------|---------------|
| `v_iku4_per_prodi` | `fact_iku4` ⋈ `dim_prodi` | Bar capaian IKU-4 per prodi |
| `v_iku7_per_prodi` | `fact_iku7` ⋈ `dim_prodi` | Metode pembelajaran per prodi |
| `v_iku4_sd` | filter `nama_prodi` = **Sains Data** | KPI fokus prodi SD (skew eksperimen) |
| `v_tata_kelola_tahun` | `fact_tata_kelola` ⋈ `dim_waktu` | Trend anggaran & SAKIP |

Contoh SQL virtual **IKU-4 per prodi** (salin di SQL Lab):

```sql
SELECT
  p.prodi_id,
  p.nama_prodi,
  p.nama_jurusan,
  f.total_dosen_tetap,
  f.dosen_s3,
  f.dosen_sertifikat_industri,
  f.dosen_dari_praktisi,
  f.persen_iku4,
  f.target_iku,
  f.capaian_iku
FROM lakehouse.gold_aqe_off.fact_iku4_kualifikasi_dosen f
JOIN lakehouse.gold_aqe_off.dim_prodi p ON f.prodi_id = p.prodi_id
ORDER BY f.persen_iku4 DESC;
```

#### 5.3.3 KPI yang dapat ditampilkan di dashboard (berdasarkan data aktual)

**Dashboard A — Executive IKU (subset 4 indikator + tata kelola)**

| Panel | KPI | Dataset / metrik | Visualisasi |
|-------|-----|------------------|-------------|
| A1 | **IKU-4** — % dosen memenuhi kualifikasi (S3 / sertifikat / praktisi) | `persen_iku4` vs `target_iku` | Bullet / bar per `nama_prodi` |
| A2 | **IKU-6** — % prodi S1 yang berkerjasama dengan mitra | `persen_iku6`, `prodi_berkerjasama` / `total_prodi_s1` | Big number + trend per `tahun` (`dim_waktu`) |
| A3 | **IKU-7** — % MK case method & team-based | `persen_iku7`, `mk_case_method`, `mk_team_based` | Stacked bar per prodi |
| A4 | **IKU-8** — % prodi berakreditasi/sertifikasi internasional | `persen_iku8`, `prodi_akreditasi_internasional` | Gauge |
| A5 | **Tata kelola** — realisasi anggaran & predikat SAKIP | `persen_realisasi`, `predikat_sakip` | Line (tahun) + tabel |
| A6 | **Capaian vs target** (4 IKU) | `capaian_iku` dari fakta 4/6/7/8 | Heatmap atau bar grouped |

**Dashboard B — Profil institusi (dimensi)**

| Panel | KPI | Dataset | Visualisasi |
|-------|-----|---------|-------------|
| B1 | Jumlah dosen per prodi | `dim_dosen` + `dim_prodi` | Bar `COUNT(dosen_id)` |
| B2 | Mahasiswa per angkatan & prodi | `dim_mahasiswa` + `dim_prodi` | Line / area |
| B3 | Sebaran prodi per jurusan | `dim_prodi` | Pie `nama_jurusan` |
| B4 | Topik penelitian strategis | `dim_topik_penelitian` | Table |

**Dashboard C — Fokus prodi Sains Data (SD)** — selaras skew eksperimen AQE

| Panel | KPI | Catatan |
|-------|-----|---------|
| C1 | Kualifikasi dosen prodi SD | `v_iku4_sd` — bandingkan `persen_iku4` dengan rata institusi |
| C2 | Metode pembelajaran SD | `fact_iku7` filter `prodi_id = 'SD'` |
| C3 | Mahasiswa & dosen SD | `dim_mahasiswa` / `dim_dosen` filter `prodi_id = 'SD'` |

**Dashboard D — Evaluasi AQE (penelitian)** — data teknis, bukan KPI bisnis

| Panel | KPI | Sumber |
|-------|-----|--------|
| D1 | Durasi pipeline Gold | `metrics/silver_to_gold_*.json` |
| D2 | Latency query Trino W4–W6 | `metrics/workloads_trino_ctx_*.json` |
| D3 | Speedup Silver AQE | Grafana / `aqe_comparison_*.json` |

> Panel D lebih natural di **Grafana** ([`../monitoring-grafana/README.md`](../monitoring-grafana/README.md)); Superset untuk **KPI ITERA**.

**Filter global dashboard (Native filters):**

- `dim_waktu.tahun` (2021–2025)
- `dim_prodi.nama_prodi` / `nama_jurusan`
- `dim_prodi.prodi_id` (preset **SD** untuk analisis skew)

### 5.4 Relasi antar dataset (opsional)

Di **Dataset → Edit → Metrics & columns**, definisikan metrik:

- `nilai_capaian` — AVG atau MAX  
- `nilai_target` — MAX  
- `capaian_pct` — calculated: `nilai_capaian / nilai_target`

Untuk join antar dataset di Superset 4.x: gunakan **Virtual dataset** (SQL Lab) dengan SQL eksplisit (disarankan untuk star schema).

---

## 6. Contoh virtual dataset (SQL Lab → Save as dataset)

Gunakan query di bawah **hanya jika tabelnya ada** (`SHOW TABLES FROM lakehouse.gold`).

### 6.1 Ringkasan capaian IKU-4/6/7/8 (executive — tanpa `fact_rekap`)

Menggantikan rekap 8 IKU jika `fact_rekap_iku_institusi` belum terbentuk:

```sql
SELECT w.tahun, 'IKU-4' AS iku_kode, AVG(f.persen_iku4) AS nilai_capaian, AVG(f.target_iku) AS nilai_target, AVG(f.capaian_iku) AS capaian_iku
FROM lakehouse.gold_aqe_off.fact_iku4_kualifikasi_dosen f
JOIN lakehouse.gold_aqe_off.dim_waktu w ON f.waktu_id = w.waktu_id
GROUP BY w.tahun
UNION ALL
SELECT w.tahun, 'IKU-6', AVG(f.persen_iku6), AVG(f.target_iku), AVG(f.capaian_iku)
FROM lakehouse.gold_aqe_off.fact_iku6_kerjasama_prodi f
JOIN lakehouse.gold_aqe_off.dim_waktu w ON f.waktu_id = w.waktu_id
GROUP BY w.tahun
UNION ALL
SELECT w.tahun, 'IKU-7', AVG(f.persen_iku7), AVG(f.target_iku), AVG(f.capaian_iku)
FROM lakehouse.gold_aqe_off.fact_iku7_metode_pembelajaran f
JOIN lakehouse.gold_aqe_off.dim_waktu w ON f.waktu_id = w.waktu_id
GROUP BY w.tahun
UNION ALL
SELECT w.tahun, 'IKU-8', AVG(f.persen_iku8), AVG(f.target_iku), AVG(f.capaian_iku)
FROM lakehouse.gold_aqe_off.fact_iku8_akreditasi_internasional f
JOIN lakehouse.gold_aqe_off.dim_waktu w ON f.waktu_id = w.waktu_id
GROUP BY w.tahun
ORDER BY tahun, iku_kode;
```

Simpan sebagai **`v_rekap_iku_subset`** → chart **bar** (`iku_kode` × `nilai_capaian`).

### 6.2 IKU-7 metode pembelajaran per prodi (drill-down)

```sql
SELECT
  p.nama_prodi,
  p.nama_jurusan,
  f.total_mk,
  f.mk_case_method,
  f.mk_team_based,
  f.persen_iku7,
  f.target_iku,
  f.capaian_iku
FROM lakehouse.gold_aqe_off.fact_iku7_metode_pembelajaran f
JOIN lakehouse.gold_aqe_off.dim_prodi p ON f.prodi_id = p.prodi_id
ORDER BY f.capaian_iku DESC;
```

Chart: **stacked bar** (`mk_case_method`, `mk_team_based`) atau **table** dengan filter `nama_prodi = 'Sains Data'`.

### 6.3 Tata kelola — realisasi anggaran per tahun

```sql
SELECT
  w.tahun,
  f.pagu_total,
  f.realisasi_total,
  f.persen_realisasi,
  f.predikat_sakip,
  f.nilai_sakip,
  f.target_kinerja_anggaran
FROM lakehouse.gold_aqe_off.fact_tata_kelola f
JOIN lakehouse.gold_aqe_off.dim_waktu w ON f.waktu_id = w.waktu_id
ORDER BY w.tahun;
```

Chart: **line** (`tahun` × `persen_realisasi`) + **big number** predikat SAKIP.

### 6.4 (Opsional) Jika `fact_rekap_iku_institusi` ada

```sql
SELECT w.tahun, r.iku_kode, r.iku_nama, r.nilai_capaian, r.nilai_target, r.status_capaian
FROM lakehouse.gold_aqe_off.fact_rekap_iku_institusi r
JOIN lakehouse.gold_aqe_off.dim_waktu w ON r.waktu_id = w.waktu_id
ORDER BY w.tahun, r.iku_kode;
```

### 6.5 (Opsional) Jika `fact_iku1_lulusan` ada

```sql
SELECT p.nama_prodi, f.total_lulusan, f.persen_terserap, f.target_iku, f.capaian_iku
FROM lakehouse.gold_aqe_off.fact_iku1_lulusan f
JOIN lakehouse.gold_aqe_off.dim_prodi p ON f.prodi_id = p.prodi_id
ORDER BY f.capaian_iku DESC;
```

---

## 7. Susunan dashboard Superset (rekomendasi)

Selaras **§5.3.3** dan inventaris Gold dari `aqe_full_experiment`.

### Dashboard: **Executive IKU ITERA (subset)**

| No | Chart | Sumber | Tipe visual |
|----|-------|--------|-------------|
| 1 | Capaian IKU-4/6/7/8 | `v_rekap_iku_subset` (§6.1) | Grouped bar |
| 2 | IKU-4 per prodi | `v_iku4_per_prodi` | Bar horizontal |
| 3 | IKU-6 kerjasama mitra | `fact_iku6_kerjasama_prodi` + `dim_waktu` | Big number + line |
| 4 | IKU-7 metode pembelajaran | §6.2 | Stacked bar |
| 5 | IKU-8 akreditasi internasional | `fact_iku8_akreditasi_internasional` | Gauge |
| 6 | Tata kelola & anggaran | §6.3 | Line + table |
| 7 | Filter global | `dim_waktu`, `dim_prodi` | Native filter |

### Dashboard: **Prodi Sains Data (SD)**

| No | Chart | Sumber |
|----|-------|--------|
| 1 | Kualifikasi dosen SD | `v_iku4_sd` |
| 2 | Mahasiswa per angkatan | `dim_mahasiswa` WHERE `prodi_id='SD'` |
| 3 | Capaian IKU-7 SD vs rata institusi | `fact_iku7` + agregat institusi |

### Dashboard: **Evaluasi AQE (penelitian)**

Panel teknis → **Grafana** ([`../monitoring-grafana/README.md`](../monitoring-grafana/README.md)); Superset untuk KPI bisnis ITERA.

---

## 8. Workload query Trino (untuk pengukuran performa)

Jalankan setelah pipeline Silver **AQE OFF** lalu **ON** (task `trino_workloads_*` di `aqe_full_experiment`). Query disesuaikan tabel Gold yang ada:

```sql
-- W4 Join (Gold) — IKU-4 + dim_prodi
SELECT p.nama_prodi, AVG(f.persen_iku4) AS avg_iku4
FROM lakehouse.gold_aqe_off.fact_iku4_kualifikasi_dosen f
JOIN lakehouse.gold_aqe_off.dim_prodi p ON f.prodi_id = p.prodi_id
GROUP BY p.nama_prodi;

-- W5 Aggregation — IKU-7 per tahun
SELECT w.tahun, AVG(f.persen_iku7) AS avg_iku7
FROM lakehouse.gold_aqe_off.fact_iku7_metode_pembelajaran f
JOIN lakehouse.gold_aqe_off.dim_waktu w ON f.waktu_id = w.waktu_id
GROUP BY w.tahun;

-- W6 Filtering — prodi capaian IKU-8 di bawah target
SELECT *
FROM lakehouse.gold_aqe_off.fact_iku8_akreditasi_internasional
WHERE capaian_iku < 100;
```

Otomatis: `python3 scripts/benchmark/run_trino_workloads.py --aqe-context ON --trino-url http://localhost:18088`

---

## 9. Troubleshooting

| Gejala | Penyebab | Solusi |
|--------|----------|--------|
| Spark: `ivy2/cache` Permission denied / FileNotFoundException | `spark.jars.packages` + folder `lib/` kosong | `./scripts/download-jars.sh`; restart Spark; pakai `./scripts/spark-sql-lakehouse.sh` atau Trino |
| `Schema gold not found` | Pipeline Gold belum jalan | Trigger `silver_to_gold_pipeline` |
| Trino: table not found | Metastore belum sync | `SHOW TABLES FROM lakehouse.gold_aqe_off`; restart trino/hive-metastore |
| Superset: connection failed | URI salah / Trino down | Pakai `trino:8080` dari dalam Docker |
| Query lambat | Data besar, no partition | Filter `waktu_id` / `tahun`; pertimbangkan partition Iceberg |
| Chart kosong | Dataset schema salah | Cek preview dataset di Superset |

---

## 10. Ringkasan alur kerja (checklist)

1. [ ] `./start.sh` — Trino + Superset healthy  
2. [ ] Pipeline Gold selesai (`aqe_full_experiment` → `silver_to_gold_on` atau `silver_to_gold_pipeline`)  
3. [ ] `SHOW TABLES FROM lakehouse.gold_aqe_off` dan `gold_aqe_on` — minimal 5 dim + 5 fakta per salinan  
4. [ ] Koneksi Superset → Trino (`Lakehouse Trino`)  
5. [ ] Dataset fisik §5.3.2 + virtual SQL §6  
6. [ ] Dashboard **Executive IKU (subset)** + opsional **Prodi SD**  
7. [ ] Latency Trino W4–W6 → `metrics/workloads_trino_*.json` (§4.1.6)  

**Dokumen terkait:** [`../README.md`](../README.md) · [`../silver-to-gold/README.md`](../silver-to-gold/README.md) · [`../monitoring-grafana/README.md`](../monitoring-grafana/README.md)
