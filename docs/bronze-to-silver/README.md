# Pipeline 2: Bronze → Silver (Processing & AQE Layer)

Panduan transformasi **Bronze → Silver**: cleaning, join, agregasi, dan quality gate. **Silver adalah layer utama eksperimen AQE** — di sini Anda membandingkan **Skenario A (AQE OFF)** dan **Skenario B (AQE ON)** pada workload yang sama.

```
Bronze (Iceberg)  →  Quality check  →  Silver (enriched Iceberg)
                              │
                    Spark SQL + AQE (OFF | ON)
                              │
              runtime stats → adaptive optimizer (jika ON)
                              │
                    metrik → Grafana / event log
```

Narasi komponen AQE (DPP, coalescing, skew join): [`../README.md`](../README.md).

---

## Prasyarat

1. **Pipeline 1 selesai** — tabel `lakehouse.bronze.*` ada.
2. Spark cluster dan Hive Metastore healthy.
3. (Opsional) Grafana + Prometheus sudah running untuk menangkap metrik saat benchmark.

```bash
docker exec <airflow-scheduler> airflow dags trigger staging_to_bronze_pipeline
# tunggu sukses, lalu lanjut pipeline 2
```

---

## Konfigurasi eksperimen AQE

Set skenario lewat **salah satu** mekanisme (pilih satu agar konsisten):

### A. File `conf/spark-defaults.conf`

Salin blok **Scenario A** atau **Scenario B** dari [`../../README.md`](../../README.md) §6.

### B. Variabel lingkungan Airflow

Di DAG `bronze_silver_pipeline.py` (rencana):

```python
# conf DAG run
aqe_scenario = "{{ dag_run.conf.get('aqe_scenario', 'OFF') }}"
```

Trigger eksplisit:

```bash
# Baseline
docker exec <airflow-scheduler> airflow dags trigger bronze_to_silver_pipeline \
  --conf '{"aqe_scenario": "OFF"}'

# AQE aktif
docker exec <airflow-scheduler> airflow dags trigger bronze_to_silver_pipeline \
  --conf '{"aqe_scenario": "ON"}'
```

### C. `spark-submit` / argumen PySpark

```bash
spark-submit \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  scripts/spark/bronze_to_silver.py
```

### Parameter kunci

| Parameter | AQE OFF | AQE ON |
|-----------|---------|--------|
| `spark.sql.adaptive.enabled` | `false` | `true` |
| `spark.sql.adaptive.coalescePartitions.enabled` | `false` | `true` |
| `spark.sql.adaptive.skewJoin.enabled` | `false` | `true` |
| `spark.sql.shuffle.partitions` | tetap (mis. `200`) | awal sama; AQE menyesuaikan saat runtime |

**Kontrol eksperimen:** dataset identik, cluster identik, query/workload identik — hanya konfigurasi AQE yang berubah.

---

## Trigger pipeline

### Via Airflow UI

1. `http://<host>:18681` → DAG **`bronze_to_silver_pipeline`**
2. Trigger dengan conf `aqe_scenario` jika UI mendukung JSON conf
3. Ulangi run kedua dengan skenario berlawanan

### Via CLI

Lihat contoh `--conf` di atas.

---

## Apa yang terjadi di pipeline

### Task 1: `bronze_to_silver` (Spark ETL)

#### Quality gate

| Score completeness | Status | Aksi |
|--------------------|--------|------|
| ≥ 80% | PASS | Tulis Silver |
| 60–79% | QUARANTINE | Tulis + flag |
| < 60% | REJECT | Skip |

#### Transformasi (ringkas)

| Silver | Sumber Bronze | Operasi berat (untuk AQE) |
|--------|---------------|---------------------------|
| `silver_mahasiswa` | `raw_mahasiswa` + `raw_prodi` | JOIN, dedup |
| `silver_lulusan` | `raw_lulusan` | Filter, flag |
| `silver_dosen` | `raw_dosen` + `raw_kegiatan_dosen` | JOIN, agregasi |
| `silver_penelitian_pkm` | penelitian + pengabdian | UNION |
| `silver_kerjasama_aktif` | `raw_kerjasama` | Filter |
| `silver_akreditasi_aktif` | `raw_akreditasi` | Window |

Operasi join/agregasi inilah yang memicu **shuffle** — pantau di Spark UI tab **SQL** / **Stages** untuk perbandingan OFF vs ON.

### Task 2: `quality_report`

Log ringkasan PASS/QUARANTINE/REJECT per tabel.

### Task 3 (rencana): `run_aqe_benchmark`

Jalankan paket query workload **setelah** Silver terisi:

| Workload | Contoh |
|----------|--------|
| Join | `silver_mahasiswa` ⋈ `silver_dosen` pada key prodi |
| Aggregation | `GROUP BY prodi_id`, `COUNT`, `SUM` |
| Filtering | `WHERE tahun_masuk >= 2020` + join ke dimensi |

Simpan hasil ke `metrics/silver_aqe_<scenario>_<timestamp>.json`:

```json
{
  "scenario": "ON",
  "workload": "join_mahasiswa_dosen",
  "execution_time_sec": 42.3,
  "shuffle_read_bytes": 0,
  "num_shuffle_partitions": 0,
  "aqe_coalesced_partitions": 0
}
```

---

## Pengumpulan metrik (Grafana / Spark)

| Sumber | Metrik |
|--------|--------|
| **Spark UI** (`:18080`) | Duration, shuffle read/write, spill, jumlah tasks |
| **Spark event log** | Detail stage, `spark.sql.adaptive.*` di plan |
| **Grafana** | Panel runtime, shuffle, partition histogram, AQE effectiveness |
| **Log Airflow** | Wall-clock per task |

**Metrik wajib BAB IV:**

- Execution time per workload (OFF vs ON) → **speedup %**
- Mean / std dev ukuran partisi shuffle
- DPP: partisi file yang dibaca sebelum/sesudah (dari explain atau log)
- Coalescing ratio: `partitions_before / partitions_after`
- Skew: CV atau Gini coefficient distribusi task duration

### Data skew (opsional)

Untuk menguji **skew join optimization**, inject skew pada key join (mis. 80% baris pada satu `prodi_id`) lewat generator atau filter — dokumentasikan di laporan §3.3.3.

---

## Verifikasi hasil

### Spark SQL

```python
spark.sql("SHOW TABLES IN lakehouse.silver").show()
spark.sql("""
    SELECT prodi_id, COUNT(*) AS n, SUM(CAST(is_mbkm AS INT)) AS mbkm
    FROM lakehouse.silver.silver_mahasiswa
    GROUP BY prodi_id
    ORDER BY n DESC
""").show()
```

### Trino (konsumsi / benchmark SQL)

```sql
SELECT prodi_id, COUNT(*) AS n
FROM lakehouse.silver.silver_mahasiswa
GROUP BY prodi_id
ORDER BY n DESC;
```

Jalankan query yang sama dua kali setelah materialisasi Silver — bandingkan waktu Trino vs Spark jika hipotesis Anda mencakup konsumsi; fokus utama tetap **Spark AQE** saat penulisan Silver.

### Grafana

Buka dashboard **AQE Experiment** (akan didefinisikan di provisioning Grafana): pilih label `scenario=OFF|ON`, `pipeline=bronze_to_silver`.

---

## Alur lengkap (Pipeline 1 + 2)

```bash
docker exec <airflow-scheduler> airflow dags trigger staging_to_bronze_pipeline
# setelah sukses:
docker exec <airflow-scheduler> airflow dags trigger bronze_to_silver_pipeline \
  --conf '{"aqe_scenario": "OFF"}'
docker exec <airflow-scheduler> airflow dags trigger bronze_to_silver_pipeline \
  --conf '{"aqe_scenario": "ON"}'
```

Bandingkan metrik kedua run sebelum melanjutkan ke Gold.

---

## Troubleshooting

| Gejala | Tindakan |
|--------|----------|
| Tabel Bronze tidak ada | Jalankan pipeline 1 |
| Tidak ada perbedaan OFF vs ON | Pastikan query memicu shuffle (join besar); naikkan data `--scale` |
| AQE ON lebih lambat | Dataset kecil — overhead adaptasi; catat sebagai pembahasan |
| Partisi tidak coalesce | Cek `spark.sql.adaptive.coalescePartitions.enabled=true` dan ukuran shuffle kecil |

---

## Arsitektur file

```
scripts/
├── spark/bronze_to_silver.py
└── dags/bronze_silver_pipeline.py
conf/spark-defaults.conf
metrics/                    # hasil benchmark (rencana)
```

**Langkah berikutnya:** [`../silver-to-gold/README.md`](../silver-to-gold/README.md) — star schema Gold, query Trino, dashboard Superset.
