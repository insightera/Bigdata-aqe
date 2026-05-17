# Monitoring Pipeline AQE dengan Grafana

Panduan observabilitas stack Data Lakehouse selaras dengan **§11 Metrics & Monitoring Layer** pada [`../README.md`](../README.md): runtime, shuffle, partisi, resource, dan efektivitas AQE.

**Prasyarat:** Prometheus + Grafana running (`./start.sh`).

| Service | URL | Login |
|---------|-----|-------|
| **Grafana** | http://localhost:13001 | admin / admin |
| **Prometheus** | http://localhost:19090 | — |
| **Spark UI** | http://localhost:18080 | — |
| **Airflow** | http://localhost:18681 | airflow / airflow |

---

## 1. Arsitektur monitoring

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Spark ETL    │     │ Trino        │     │ Airflow DAG  │
│ (AQE OFF/ON) │     │ (query Gold) │     │ (orchestrate)│
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       │ event log          │ (opsional JMX)       │ task logs
       ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────────┐
│  Sumber metrik                                           │
│  • Spark UI / History Server                             │
│  • metrics/*.json (hasil scripts/spark/aqe_config.py)    │
│  • Prometheus (scrape)                                   │
└────────────────────────────┬─────────────────────────────┘
                             ▼
                    ┌─────────────────┐
                    │   Prometheus    │  :19090
                    └────────┬────────┘
                             ▼
                    ┌─────────────────┐
                    │    Grafana      │  :13001
                    │  (dashboards)   │
                    └─────────────────┘
```

Provisioning Grafana sudah ada di [`../../monitoring/grafana/provisioning/`](../../monitoring/grafana/provisioning/) — datasource **Prometheus** otomatis terdaftar.

---

## 2. Pemetaan metrik (sesuai docs/README §11)

### 2.1 Runtime metrics

| Metrik | Definisi | Sumber data | Panel Grafana (rekomendasi) |
|--------|----------|-------------|------------------------------|
| **Execution time** | Wall-clock job/query (detik) | `metrics/bronze_to_silver_aqe_*.json`, Spark UI | Time series per skenario |
| **Throughput** | Baris/detik atau MB/detik | Hitung dari JSON + ukuran staging | Stat / gauge |
| **Speedup** | `(T_off - T_on) / T_off × 100%` | Dua file metrik OFF vs ON | Bar chart perbandingan |

**Contoh isi file metrik** (dihasilkan pipeline Silver):

```json
{
  "pipeline": "bronze_to_silver",
  "aqe_scenario": "ON",
  "duration_sec": 842.5,
  "spark_configs": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "200"
  },
  "summary": {
    "tables_written": 6,
    "rows_written": 1250000
  }
}
```

Lokasi: [`../../metrics/`](../../metrics/) (volume Docker: `/opt/airflow/metrics`).

**Penyimpanan lakehouse:** Silver/Gold OFF dan ON tidak saling timpa di MinIO (`warehouse-aqe-off` vs `warehouse-aqe-on`). File metrik tetap per skenario (`bronze_to_silver_aqe_OFF_*.json`, `workloads_trino_ctx_ON_*.json`). Validasi audit baris lewat Trino — lihat [`../eksperimen/README.md`](../eksperimen/README.md) §1.1 dan [`../gold-to-serving/README.md`](../gold-to-serving/README.md) §2.4.

### 2.2 Shuffle metrics

| Metrik | Definisi | Sumber |
|--------|----------|--------|
| Shuffle read bytes | Total data dibaca shuffle | Spark UI → Stages → Shuffle Read |
| Shuffle write bytes | Total data ditulis shuffle | Spark UI → Stages |
| Spill (memory/disk) | Data spill ke disk | Spark UI executor |
| Records shuffle | Jumlah record shuffle | Spark UI |

**Cara ambil manual (per aplikasi Spark):**

1. Buka http://localhost:18080 → aplikasi `bronze_to_silver_AQE_ON` atau `_OFF`
2. Tab **Stages** → sort by **Shuffle Read**
3. Catat untuk tabel laporan BAB IV §4.1.4

### 2.3 Partition metrics

| Metrik | Definisi | Sumber |
|--------|----------|--------|
| Ukuran partisi (mean, std) | Distribusi ukuran partisi shuffle | Spark UI, event log |
| **Coefficient of variation (CV)** | `std / mean` | Hitung dari sampling partisi |
| **Gini coefficient** | Ketimpangan distribusi partisi | Skrip analisis / notebook |
| Skew distribution | Histogram ukuran task | Spark UI **Tasks** |

Fokus penelitian: **sebelum vs sesudah AQE** pada workload join/agregasi di Silver.

### 2.4 Resource metrics

| Metrik | Definisi | Sumber (saat ini) |
|--------|----------|-------------------|
| CPU utilization | % CPU executor/host | Docker stats, (future) cAdvisor |
| Memory utilization | Heap executor, spill | Spark UI **Executors** |
| Disk I/O | Read/write MinIO | MinIO console, (future) node_exporter |
| Network I/O | Shuffle network | Spark UI |

Perintah cepat:

```bash
docker stats lhaqe-spark-worker-1 lhaqe-spark-worker-2 lhaqe-spark-master --no-stream
```

### 2.5 AQE effectiveness metrics

Selaras §6 [`../README.md`](../README.md) (komponen AQE):

| Metrik | Komponen AQE | Cara ukur |
|--------|--------------|-----------|
| **DPP reduction %** | Dynamic Partition Pruning | Bandingkan partisi/file scan di plan OFF vs ON |
| **Coalescing ratio** | Shuffle partition coalescing | `partitions_before / partitions_after` dari Spark UI |
| **Skew reduction** | Skew join optimization | CV atau Gini task duration OFF vs ON |

Spark UI → SQL tab → lihat rencana fisik: node **AdaptiveSparkPlan**, **CustomShuffleReader**, **OptimizeSkewedJoin**.

---

## 3. Setup Grafana (langkah demi langkah)

### 3.1 Akses pertama

1. Buka http://localhost:13001  
2. Login `admin` / `admin`  
3. **Connections → Data sources** → pastikan **Prometheus** ada (url `http://prometheus:9090`)  
   - Sudah diprovision dari [`datasources/prometheus.yml`](../../monitoring/grafana/provisioning/datasources/prometheus.yml)

### 3.2 Prometheus saat ini

File [`../../monitoring/prometheus/prometheus.yml`](../../monitoring/prometheus/prometheus.yml) baru memonitor **Prometheus sendiri**. Itu normal untuk fase dev — dashboard infrastruktur Prometheus tetap berguna.

**Perluas scrape (opsional, untuk produksi):**

```yaml
# contoh tambahan di prometheus.yml
  - job_name: spark-master
    metrics_path: /metrics
    static_configs:
      - targets: ["spark-master:4040"]   # butuh Spark metrics servlet / JMX exporter

  - job_name: node
    static_configs:
      - targets: ["node-exporter:9100"]
```

Setelah edit: `docker compose restart prometheus`.

### 3.3 Dashboard manual (disarankan untuk penelitian)

**Create → Dashboard → Add visualization**

Tanpa exporter Spark, gunakan kombinasi:

| Panel | Tipe | Data source | Catatan |
|-------|------|-------------|---------|
| Pipeline duration OFF vs ON | Bar chart | **Infinity** / JSON | Import dari `metrics/*.json` |
| Prometheus up | Time series | Prometheus | `up{job="prometheus"}` |
| Catatan eksperimen | Text | — | Skenario, tanggal, profile data |

#### Import metrik JSON ke Grafana

1. Install plugin **Infinity** (opsional): **Administration → Plugins → Infinity**  
2. Atau: konversi JSON ke tabel Postgres dan query dari Grafana  
3. **Praktis untuk laporan:** gunakan spreadsheet dari file `metrics/bronze_to_silver_aqe_*.json`

**Script ringkas agregasi (host):**

```bash
ls -la metrics/bronze_to_silver_aqe_*.json
python3 -c "
import json, glob
for f in sorted(glob.glob('metrics/bronze_to_silver_aqe_*.json')):
    d=json.load(open(f))
    print(d['aqe_scenario'], d['duration_sec'], 's', f)
"
```

---

## 4. Dashboard rekomendasi: **Lakehouse AQE Experiment**

Buat folder dashboard **Lakehouse AQE** dengan panel berikut.

### Row 1 — Ringkasan eksperimen

| Panel | Isi |
|-------|-----|
| Skenario aktif | Text: AQE OFF / ON dari label run |
| Total duration (s) | Stat dari JSON metrik |
| Speedup % | `100 * (dur_off - dur_on) / dur_off` |
| Tables / rows written | Dari `summary` JSON |

### Row 2 — Runtime & throughput (§11 Runtime)

| Panel | Query / sumber |
|-------|----------------|
| Duration by scenario | Bar: OFF vs ON |
| Throughput (rows/s) | `rows_written / duration_sec` |

### Row 3 — Shuffle & partition (§11 Shuffle + Partition)

| Panel | Sumber |
|-------|--------|
| Shuffle read (MB) | Input manual dari Spark UI atau future metric |
| Shuffle write (MB) | Idem |
| Partition CV / Gini | Input dari analisis notebook |

### Row 4 — Audit data OFF vs ON (Trino)

Panel **Infinity** atau catatan manual dari query:

```sql
SELECT 'OFF' AS aqe, COUNT(*) AS n FROM lakehouse.gold_aqe_off.dim_mahasiswa
UNION ALL SELECT 'ON', COUNT(*) FROM lakehouse.gold_aqe_on.dim_mahasiswa;
```

Nilai `n` harus sama (transform identik); yang dibandingkan di penelitian tetap **durasi** pipeline/query di panel Row 1–3.

### Row 5 — AQE effectiveness (§11 AQE)

| Panel | Metrik |
|-------|--------|
| Coalescing ratio | ON only |
| DPP reduction % | ON only |
| Skew reduction | Perbandingan task skew |

### Row 6 — Resource (§11 Resource)

| Panel | Sumber |
|-------|--------|
| Executor memory | Screenshot / input Spark UI |
| CPU (docker stats) | Manual saat run |

---

## 5. Spark Event Log (analisis mendalam)

Konfigurasi di [`../../conf/spark-defaults.conf`](../../conf/spark-defaults.conf):

```properties
spark.eventLog.enabled=true
spark.eventLog.dir=s3a://spark-event-logs/
```

**Akses:**

- UI live: http://localhost:18080 (aplikasi sedang/terakhir jalan)
- History Server (opsional): deploy `spark-history-server` jika perlu replay log dari MinIO

**Analisis untuk laporan:**

1. Buka aplikasi `bronze_to_silver_AQE_OFF` dan `bronze_to_silver_AQE_ON`
2. Bandingkan **Duration**, **Shuffle Read/Write**, jumlah **Stages**
3. Screenshot tab **SQL** / **DAG visualization** untuk subbab efektivitas AQE

---

## 6. Alur monitoring per tahap pipeline

| Tahap | Apa yang dimonitor | Tool |
|-------|-------------------|------|
| **Staging → Bronze** | Durasi ingest, ukuran CSV | Airflow logs, MinIO console |
| **Bronze → Silver** | **AQE OFF vs ON**, metrik JSON | Spark UI, `metrics/*.json`, Grafana |
| **Silver → Gold** | Durasi star schema build | Airflow, Spark UI |
| **Gold serving** | Latency query Trino | Trino UI, Superset query time |
| **End-to-end** | SLA pipeline | Airflow DAG duration |

### Contoh workflow eksperimen

```bash
# 1. Run baseline
docker exec lhaqe-airflow-scheduler airflow dags trigger bronze_to_silver_pipeline \
  --conf '{"aqe_scenario": "OFF"}'

# 2. Run AQE
docker exec lhaqe-airflow-scheduler airflow dags trigger bronze_to_silver_pipeline \
  --conf '{"aqe_scenario": "ON"}'

# 3. Kumpulkan metrik
ls metrics/
# bronze_to_silver_aqe_OFF_*.json
# bronze_to_silver_aqe_ON_*.json

# 4. Bandingkan di Grafana / spreadsheet
# 5. Ambil screenshot Spark UI untuk shuffle & skew
```

---

## 7. Korelasi dengan BAB IV (template tabel)

### Tabel — kualitas runtime pipeline

| Skenario | Execution time (s) | Throughput (rows/s) | Speedup (%) |
|----------|-------------------|---------------------|-------------|
| AQE OFF | *dari JSON* | *hitung* | — |
| AQE ON | *dari JSON* | *hitung* | *isi* |

### Tabel — efektivitas AQE

| Komponen | Metrik | OFF | ON | Reduction / ratio |
|----------|--------|-----|-----|-------------------|
| DPP | Partisi dibaca | | | % |
| Coalescing | Jumlah shuffle partitions | | | ratio |
| Skew join | CV task time | | | |

---

## 8. Troubleshooting

| Gejala | Solusi |
|--------|--------|
| Grafana: no data | Cek Prometheus http://localhost:19090/targets |
| Folder `metrics/` kosong | Pastikan DAG Silver selesai; cek volume mount di Airflow |
| Spark UI tidak ada aplikasi | Job belum pernah sukses; cek Airflow task log |
| JSON tidak terbuat | Cek log task `bronze_to_silver`; path `AQE_METRICS_DIR` |
| Plugin Infinity gagal | Pakai agregasi manual / export CSV untuk laporan |

---

## 9. Roadmap monitoring (opsional)

Untuk observabilitas penuh tanpa input manual:

1. **JMX Exporter** pada Spark master/worker → scrape Prometheus  
2. **node_exporter** → CPU, disk, network host  
3. **Trino JMX** → query latency, failed queries  
4. **Loki** → log Airflow terpusat  
5. Dashboard Grafana provisioned dari JSON di `monitoring/grafana/provisioning/dashboards/json/`

---

## 10. Checklist

1. [ ] Grafana & Prometheus healthy (`docker compose ps`)  
2. [ ] Datasource Prometheus terlihat di Grafana  
3. [ ] Pipeline Silver dijalankan OFF dan ON  
4. [ ] File `metrics/bronze_to_silver_aqe_*.json` ada  
5. [ ] Screenshot Spark UI untuk shuffle/skew  
6. [ ] Dashboard **Lakehouse AQE Experiment** terisi  
7. [ ] Tabel BAB IV §4.1.2–4.1.4 terisi dari sumber di atas  

**Dokumen terkait:** [`../README.md`](../README.md) · [`../bronze-to-silver/README.md`](../bronze-to-silver/README.md) · [`../gold-to-serving/README.md`](../gold-to-serving/README.md)
