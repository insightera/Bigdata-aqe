# Pipeline 1: Staging → Bronze (Ingestion Layer)

Panduan menjalankan tahap pertama Medallion: memuat data mentah dari CSV ke **Bronze** (Iceberg/Parquet). Tahap ini menjadi **baseline ingest** sebelum eksperimen AQE di Silver; belum membandingkan AQE OFF/ON (rencana query statis, partisi ingest by time).

```
CSV (staging)  →  Spark + Iceberg  →  Bronze (Parquet, partitioned)
                         │
              profiling ringkas (row_count, null_pct, completeness)
```

Narasi arsitektur: [`../README.md`](../README.md). Diagram: [`../../pipeline-aqe.png`](../../pipeline-aqe.png).

---

## Prasyarat

Stack **target penelitian AQE** (setelah `docker-compose.yml` diperbarui):

| Service | Fungsi | Cek kesehatan |
|---------|--------|----------------|
| MinIO | `staging`, `warehouse` buckets | Console `http://<host>:19001` |
| Spark Master + Workers | ETL | UI `http://<host>:18080` |
| Hive Metastore | Katalog Iceberg | `nc -z <host> 19083` |
| Airflow | Orkestrasi DAG | `http://<host>:18681/health` |
| PostgreSQL | Backend metastore | `pg_isready -h <host> -p 15432` |

```bash
docker compose up -d
```

---

## Langkah 1: Generate data staging (CSV)

Dari root repositori:

```bash
# Default penelitian AQE (~1M mahasiswa, ~1,5–2,5 juta baris total, skew 75% ke prodi IF)
python3 scripts/generate_bronze_data.py --mode full

# Uji cepat (~77 ribu baris)
python3 scripts/generate_bronze_data.py --profile dev --no-skew

# Stress test (~3 juta baris)
python3 scripts/generate_bronze_data.py --profile aqe-large

# Perbesar lagi: profile aqe × scale 2 → ~2M mahasiswa
python3 scripts/generate_bronze_data.py --profile aqe --scale 2.0

# Lihat rencana volume tanpa menulis file
python3 scripts/generate_bronze_data.py --profile aqe --dry-run
```

Output di `data/staging/`:

```
data/staging/
├── raw_mahasiswa.csv
├── raw_lulusan.csv
├── raw_mbkm.csv
├── … (12 file CSV domain ITERA)
└── raw_prodi.csv
```

Untuk eksperimen **format data**, simpan salinan CSV; konversi ke Parquet di Bronze memungkinkan perbandingan CSV vs Parquet di tahap berikutnya (lihat [`../README.md`](../README.md) §2).

---

## Langkah 2: Build image Airflow (jika belum)

Pipeline membutuhkan Java + PySpark + boto3 di container Airflow:

```bash
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d airflow-init airflow-webserver airflow-scheduler
```

---

## Langkah 3: Trigger pipeline

### Via Airflow UI

1. Buka `http://<host>:18681` — login `airflow` / `airflow`
2. DAG: **`staging_to_bronze_pipeline`**
3. Enable → **Trigger DAG**

### Via CLI

```bash
docker exec <airflow-scheduler-container> \
  airflow dags trigger staging_to_bronze_pipeline
```

---

## Apa yang terjadi di pipeline

### Task 1: `upload_csv_to_staging`

Upload `data/staging/*.csv` → `s3://staging/raw_*.csv`.

### Task 2: `staging_to_bronze`

PySpark (cluster mode):

1. Baca CSV dari `s3a://staging/`
2. Infer schema
3. Tulis **Iceberg** `lakehouse.bronze.<table>` → Parquet di `s3a://warehouse/bronze/`
4. Hitung **profiling ringkas** per kolom: `row_count`, `null_count`, `null_pct`, `distinct_count`, `completeness_pct`

**Konfigurasi Spark (Bronze):** gunakan skenario **AQE OFF** atau default konservatif — transformasi di Bronze umumnya scan/write, bukan fokus hipotesis AQE.

```properties
spark.sql.adaptive.enabled=false
spark.sql.shuffle.partitions=50
```

Verifikasi:

- Spark UI: aplikasi completed
- MinIO: `warehouse/bronze/` terisi file Parquet

---

## Verifikasi hasil

### MinIO Console

Bucket `staging` dan prefix `warehouse/bronze/`.

### Spark / Jupyter

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hive") \
    .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/") \
    .getOrCreate()

spark.sql("SHOW TABLES IN lakehouse.bronze").show()
spark.sql("SELECT COUNT(*) FROM lakehouse.bronze.raw_mahasiswa").show()
```

### Trino (setelah layanan Trino aktif)

```sql
SHOW SCHEMAS FROM lakehouse;
SELECT COUNT(*) FROM lakehouse.bronze.raw_mahasiswa;
```

---

## Incremental update

```bash
python3 scripts/generate_bronze_data.py --mode append --batch-size 2000
docker exec <airflow-scheduler> airflow dags trigger staging_to_bronze_pipeline
```

Ukur ulang waktu ingest dan ukuran partisi — input untuk analisis **layer Bronze** di BAB IV.

---

## Troubleshooting

| Gejala | Tindakan |
|--------|----------|
| DAG tidak muncul | `airflow dags list-import-errors` di scheduler |
| Spark timeout | Cek worker di UI; pastikan MinIO & Hive healthy |
| Iceberg catalog error | Buat DB `iceberg_catalog` di Postgres jika dipakai REST catalog |
| OOM di ingest skala besar | Turunkan `--scale` atau naikkan `spark.executor.memory` |

---

## Arsitektur file

```
scripts/
├── generate_bronze_data.py
├── dags/staging_bronze_pipeline.py
└── spark/staging_to_bronze.py
conf/
├── spark-defaults.conf      # override skenario AQE per lingkungan
└── core-site.xml
data/staging/                # CSV input
docker-compose.yml
```

**Langkah berikutnya:** [`../bronze-to-silver/README.md`](../bronze-to-silver/README.md) — layer utama eksperimen **AQE OFF vs ON**.
