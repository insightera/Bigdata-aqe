# Data Lakehouse — Adaptive Query Execution (AQE) pada Medallion Architecture

Repositori ini mendukung penelitian big data tentang **efektivitas Adaptive Query Execution (AQE)** pada pipeline **Data Lakehouse Medallion (Bronze → Silver → Gold)**. Fokus penelitian adalah perbandingan eksperimen **AQE OFF (baseline)** vs **AQE ON** terhadap performa query, distribusi partisi, dan efektivitas komponen AQE (DPP, shuffle coalescing, skew join). Konsumsi analitik memakai **Trino** dan **Apache Superset**; observabilitas memakai **Grafana** (sumber metrik dari Spark Event Log, Prometheus, dan metrik cluster).

Narasi arsitektur lengkap: [`docs/README.md`](docs/README.md). Kerangka metodologi dan BAB IV: [`rancangan-metodologi-dan-hasil-pembahasan.md`](rancangan-metodologi-dan-hasil-pembahasan.md).

## Tech Stack

### Infrastructure & Orchestration

![Docker](https://img.shields.io/badge/Docker-24.x-2496ED?logo=docker&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker_Compose-v2-2496ED?logo=docker&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white)

### Data Processing & Storage

![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-E25A1C?logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-1.5.2-4E9BCD?logo=data:image/svg+xml;base64,&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-latest-C72E49?logo=minio&logoColor=white)
![Apache Hive](https://img.shields.io/badge/Apache_Hive-4.0.0-FDEE21?logo=apachehive&logoColor=black)

### Query Engine & Analytics

![Trino](https://img.shields.io/badge/Trino-latest-DD00A1?logo=trino&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache_Superset-latest-20A6C9?logo=apachesuperset&logoColor=white)

### Monitoring

![Grafana](https://img.shields.io/badge/Grafana-latest-F46800?logo=grafana&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-latest-E6522C?logo=prometheus&logoColor=white)

### Database

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15--alpine-4169E1?logo=postgresql&logoColor=white)

### Languages & Libraries

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![boto3](https://img.shields.io/badge/boto3-latest-232F3E?logo=amazonaws&logoColor=white)

### Development & Notebook

![Jupyter](https://img.shields.io/badge/Jupyter_Notebook-latest-F37626?logo=jupyter&logoColor=white)

---

## 1. Rancangan penelitian

### 1.1 Arsitektur pipeline dan posisi AQE

Diagram berikut menggambarkan alur Medallion, **Spark SQL Engine** dengan dua skenario (AQE OFF / ON), workload query eksperimen, serta lapisan metrik dan konsumsi (Trino, Superset, Grafana).

![Arsitektur Data Lakehouse Medallion dengan eksperimen AQE](./Data-lakehouse-AQE.png)

**Ringkasan (BAB IV 4.1.1)**

| Aspek | Penjelasan singkat |
|--------|---------------------|
| **Alur data** | Sumber → Staging → Columnar (CSV/Parquet) → Bronze (raw) → Silver (transform + optimasi AQE) → Gold (star schema / KPI). |
| **Eksperimen AQE** | **Skenario A (OFF):** rencana query statis, shuffle partition tetap. **Skenario B (ON):** re-optimasi runtime dari statistik shuffle/join/skew. |
| **Komponen AQE** | Dynamic Partition Pruning, Shuffle Partition Coalescing, Skew Join Optimization. |
| **Konsumsi** | **Trino** membaca tabel Gold (Iceberg/Hive Metastore); **Superset** untuk dashboard KPI dan visualisasi hasil eksperimen. |
| **Monitoring** | **Grafana** menampilkan runtime, shuffle, partisi, resource, dan metrik efektivitas AQE. |

### 1.2 Variabel dan desain eksperimen

| Elemen | Definisi |
|--------|----------|
| **Jenis penelitian** | Eksperimental kuantitatif, komparatif (AQE ON vs OFF), controlled experiment. |
| **Variabel independen** | Konfigurasi AQE (`spark.sql.adaptive.*`). |
| **Variabel dependen** | Execution time, throughput, speedup, distribusi partisi, metrik efektivitas AQE. |
| **Variabel kontrol** | Dataset, ukuran cluster, set query workload. |
| **Workload** | Join, agregasi, filtering (lihat §5). |
| **Format data** | CSV (row-based) vs Parquet (columnar). |

---

## 2. Metodologi penelitian (langkah demi langkah)

1. **Studi literatur** — AQE Spark 3.x, partition skew, DPP, dan lakehouse Medallion.
2. **Perancangan arsitektur** — Sesuai `pipeline-aqe.png` dan [`docs/README.md`](docs/README.md).
3. **Implementasi lingkungan** — Stack kontainer: MinIO, Spark, Hive Metastore, Airflow, Trino, Superset, Grafana (+ Prometheus); `docker-compose.yml` disusun mengikuti panduan ini.
4. **Persiapan dataset** — Data sintetis ITERA (CSV staging); variasi skala dan skew bila diperlukan.
5. **Pipeline Medallion** — Tiga tahap ETL (Staging→Bronze→Silver→Gold) dengan konfigurasi Spark terkontrol.
6. **Eksperimen AQE** — Jalankan workload yang sama pada **AQE OFF** dan **AQE ON**; kumpulkan metrik dari Spark UI / event log dan Grafana.
7. **Analisis** — Perbandingan runtime, distribusi partisi, efektivitas DPP/coalescing/skew, dampak per layer dan format data.

---

## 3. Ringkas teknologi (stack target)

| Komponen | Fungsi dalam penelitian |
|----------|-------------------------|
| **MinIO** | Object storage S3-compatible: staging, bronze, silver, gold, warehouse. |
| **Apache Spark** | ETL Medallion; **mesin eksperimen AQE** (Scenario A/B). |
| **Hive Metastore** | Katalog tabel Iceberg untuk Spark dan Trino. |
| **Apache Iceberg** | Format tabel terkelola di warehouse. |
| **Apache Airflow** | Orkestrasi DAG pipeline (`scripts/dags/`). |
| **Trino** | Query engine SQL interaktif pada layer Gold (dan Silver untuk uji). |
| **Apache Superset** | Dashboard BI, KPI IKU, visualisasi hasil perbandingan AQE. |
| **Grafana** | Dashboard monitoring: runtime, shuffle, partisi, CPU/memori, efektivitas AQE. |
| **Prometheus** | Scraping metrik cluster/Spark exporter (untuk Grafana). |
| **PostgreSQL** | Metastore Hive, metadata Superset, metadata Airflow. |

> **Catatan:** Repositori mungkin masih memuat artefak stack metadata lama (Atlas, Solr, portal Next.js). Stack **target penelitian AQE** tidak memakai Apache Atlas. Setelah `docker-compose.yml` diperbarui, layanan Atlas tidak lagi dijalankan.

---

## 4. Kerangka BAB IV — Hasil dan Pembahasan

Template isi BAB IV; isi angka dan tangkapan layar dari lingkungan eksperimen Anda.

### 4.1 Hasil

#### 4.1.1 Hasil eksekusi pipeline Data Lakehouse

Ringkasan Bronze → Silver → Gold: status sukses/gagal, waktu total pipeline, tabel runtime per tahap.

#### 4.1.2 Perbandingan runtime AQE vs non-AQE

| Skenario | Workload | Execution Time (s) | Speedup (%) |
|----------|----------|-------------------|-------------|
| AQE OFF | *diisi* | *diisi* | — |
| AQE ON | *diisi* | *diisi* | *diisi* |

Wajib: grafik bar/line dan persentase speedup.

#### 4.1.3 Distribusi partisi dan data skew

Metrik: mean partition size, std dev, coefficient of variation, Gini coefficient — **sebelum vs sesudah AQE**.

#### 4.1.4 Efektivitas komponen AQE

| Komponen | Metrik | Sebelum | Sesudah | Reduction / ratio |
|----------|--------|---------|---------|-------------------|
| DPP | Jumlah partisi dibaca | *diisi* | *diisi* | *%* |
| Coalescing | Jumlah shuffle partitions | *diisi* | *diisi* | *ratio* |
| Skew join | Distribusi task / partisi | *diisi* | *diisi* | *catatan* |

#### 4.1.5 Perbandingan format data (CSV vs Parquet)

Execution time dan resource usage per format, untuk kedua skenario AQE.

#### 4.1.6 Dampak per layer Medallion

| Layer | Beban query utama | Dampak AQE (ringkas) |
|-------|-------------------|----------------------|
| Bronze | Ingest, scan mentah | *diisi* |
| Silver | Join, agregasi, filter | *diisi* — biasanya paling terasa |
| Gold | Agregasi BI, star schema | *diisi* |

### 4.2 Pembahasan

Bahas: mengapa Silver paling sensitif terhadap AQE; peran runtime statistics; trade-off overhead adaptasi; keterbatasan lingkungan Docker lokal; implikasi untuk tuning produksi.

---

## 5. Workload query eksperimen

Jalankan set query yang sama pada **AQE OFF** dan **AQE ON**:

| Kelompok | Contoh query | Tujuan pengukuran |
|----------|--------------|-------------------|
| **Join** | Star/snowflake join, large-large join | Skew join, shuffle |
| **Aggregation** | `GROUP BY`, rollup/cube | Coalescing, partisi |
| **Filtering** | Filter selektif, range | DPP, I/O reduction |

Skrip benchmark dan konfigurasi: lihat panduan [`docs/bronze-to-silver/README.md`](docs/bronze-to-silver/README.md) (Silver = layer utama AQE) dan rencana skrip di `scripts/benchmark/` (akan ditambahkan bersama docker-compose).

---

## 6. Konfigurasi Spark AQE

### Skenario A — Baseline (AQE OFF)

```properties
spark.sql.adaptive.enabled=false
spark.sql.adaptive.coalescePartitions.enabled=false
spark.sql.adaptive.skewJoin.enabled=false
spark.sql.adaptive.localShuffleReader.enabled=false
# shuffle partition tetap, misalnya:
spark.sql.shuffle.partitions=200
```

### Skenario B — AQE ON

```properties
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.localShuffleReader.enabled=true
spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB
spark.sql.shuffle.partitions=200
```

Parameter disetel lewat `conf/spark-defaults.conf`, variabel lingkungan Airflow (`SPARK_AQE_SCENARIO`), atau argumen `spark-submit`. Detail operasional: panduan pipeline di folder `docs/`.

---

## 7. Arsitektur ringkas (teks)

```
Sumber → Staging → [CSV | Parquet] → Bronze → Silver (AQE) → Gold
                              ↓
                    Spark SQL (Scenario A | B)
                              ↓
              Runtime stats → Adaptive Optimizer (jika AQE ON)
                              ↓
         ┌────────────────────┼────────────────────┐
         ↓                    ↓                    ↓
    Metrics/Grafana      Trino (SQL)         Superset (BI)
```

---

## 8. Layer mapping (Medallion)

| Layer | Storage | Peran dalam penelitian AQE |
|-------|---------|----------------------------|
| **Staging** | `s3a://staging/` | Landing CSV; variasi format row-based |
| **Bronze** | `s3a://bronze/` / Iceberg | Raw ingestion; baseline scan |
| **Silver** | `s3a://silver/` | Transform, join, agregasi — **fokus optimasi AQE** |
| **Gold** | `s3a://gold/` | Star schema IKU; query Trino/Superset |

---

## 9. Stack layanan (rencana) dan menjalankan

Port berikut adalah **rencana** setelah `docker-compose.yml` diselaraskan dengan penelitian AQE (rentang 15xxx–22xxx, sama seperti konvensi repo).

| Service | Fungsi | Port host (rencana) |
|---------|--------|---------------------|
| Spark Master + Workers | ETL + eksperimen AQE | **18080** (UI), **17077** (RPC) |
| MinIO | S3 API + console | **19000**, **19001** |
| Hive Metastore | Katalog Iceberg | **19083** |
| Airflow | Orkestrasi DAG | **18681** |
| Trino | Query SQL Gold/Silver | **18088** |
| Superset | Dashboard BI | **18089** |
| Grafana | Monitoring AQE | **13001** |
| Prometheus | Scraping metrik | **19090** |
| PostgreSQL | Metastore, Airflow, Superset | **15432** |
| Jupyter (opsional) | Eksplorasi Spark | **18888** |

Port final diset di `.env` / `docker-compose.yml` (variabel `LHAQE_*` atau setara) agar tidak bentrok dengan layanan lain di host.

**Menjalankan (setelah compose diperbarui):**

```bash
chmod +x start.sh
./start.sh
# atau manual bertahap: postgres → minio → hive → spark → airflow → trino → superset → grafana
```

**Kredensial default (dev):**

| Service | User | Password |
|---------|------|----------|
| MinIO | minioadmin | minioadmin123 |
| Airflow | airflow | airflow |
| PostgreSQL | admin | admin123 |
| Grafana | admin | admin |
| Superset | admin | admin |

---

## 10. Pipeline Medallion (implementasi)

### 10.1 Pipeline 1: Staging → Bronze

- `scripts/spark/staging_to_bronze.py` — CSV → Iceberg (Parquet)
- `scripts/dags/staging_bronze_pipeline.py` — Airflow DAG

**Panduan:** [`docs/staging-to-bronze/README.md`](docs/staging-to-bronze/README.md)

### 10.2 Pipeline 2: Bronze → Silver (layer AQE utama)

- `scripts/spark/bronze_to_silver.py` — cleaning, join, quality
- `scripts/dags/bronze_silver_pipeline.py` — Airflow DAG
- Konfigurasi **AQE OFF/ON** diterapkan di tahap ini

**Panduan:** [`docs/bronze-to-silver/README.md`](docs/bronze-to-silver/README.md)

### 10.3 Pipeline 3: Silver → Gold

- `scripts/spark/silver_to_gold.py` — star schema (5 dim + 10 fakta IKU)
- `scripts/dags/silver_gold_pipeline.py` — Airflow DAG

**Panduan:** [`docs/silver-to-gold/README.md`](docs/silver-to-gold/README.md)

### 10.4 Konsumsi: Trino + Superset

- **Trino:** konektor Hive/Iceberg ke `lakehouse.gold.*`; jalankan workload BI dan query ad-hoc untuk mengukur waktu respons pasca-AQE di Silver.
- **Superset:** dataset dari Trino; dashboard KPI IKU (`fact_rekap_iku_institusi`, dll.) dan panel perbandingan runtime eksperimen (data dari tabel/log hasil benchmark).

### 10.5 Monitoring: Grafana

Dashboard rencana:

- Runtime: execution time, throughput per skenario
- Shuffle: read/write bytes, spill
- Partisi: histogram ukuran partisi, CV, Gini
- AQE: DPP reduction %, coalescing ratio, indikator skew join
- Resource: CPU, memori executor (dari Prometheus/node exporter)

---

## 11. Metrik evaluasi (ringkas)

| Kategori | Metrik |
|----------|--------|
| **Runtime** | Execution time, throughput, speedup |
| **AQE** | DPP reduction, coalescing ratio, skew reduction |
| **Partisi** | Mean, std dev, CV, Gini |
| **Resource** | CPU, memory, disk/network I/O |
| **Per layer** | Dampak relatif Bronze / Silver / Gold |

Sumber data: Spark UI (aplikasi selesai), Spark event log, log pipeline Airflow, panel Grafana, hasil query Trino yang di-timestamp.

---

## 12. Berkas pendukung di repositori

| Berkas | Keterangan |
|--------|------------|
| `Data-lakehouse-AQE.png` | Diagram arsitektur penelitian AQE |
| `docs/README.md` | Narasi alur arsitektur dan komponen AQE |
| `rancangan-metodologi-dan-hasil-pembahasan.md` | Outline metodologi & BAB IV |
| `docker-compose.yml` | Stack layanan — **akan diselaraskan** (Trino, Superset, Grafana; tanpa Atlas) |
| `conf/spark-defaults.conf` | Konfigurasi Spark + skenario AQE |
| `scripts/dags/` | DAG Airflow per tahap Medallion |
| `scripts/spark/` | ETL PySpark |
| `scripts/generate_bronze_data.py` | Generator data sintetis ITERA |

---

## 13. Langkah berikutnya (untuk Anda)

1. Baca [`docs/README.md`](docs/README.md) lalu panduan per pipeline di `docs/*/README.md`.
2. Susun `docker-compose.yml` sesuai tabel §9 (Spark, MinIO, Hive, Airflow, Trino, Superset, Grafana, Prometheus — **tanpa** Atlas/HBase/Solr/Kafka metadata).
3. Tambahkan skrip benchmark AQE (`scripts/benchmark/`) dan dashboard Grafana/Superset sebagai artefak hasil.

---
