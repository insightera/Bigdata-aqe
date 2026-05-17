# Template — Lingkungan Eksperimen (BAB III §3.2)

> Salin ke laporan atau simpan di `experiment-runs/run-YYYY-MM-DD/`

## Identitas run

| Field | Nilai |
|-------|-------|
| ID eksperimen | EXP-001 |
| Tanggal | |
| Peneliti | |
| Repositori / commit | |

## Infrastruktur (§3.2.1)

| Komponen | Spesifikasi |
|----------|-------------|
| Host OS | |
| CPU (model, core) | |
| RAM (GB) | |
| Disk kosong (GB) | |
| Docker version | |
| Docker Compose version | |

## Konfigurasi cluster Spark (§3.2.1)

| Parameter | Nilai |
|-----------|-------|
| Spark version | 3.5.1 |
| Master | spark://spark-master:7077 |
| Jumlah worker | 2 |
| Executor cores / worker | 2 |
| Executor memory | 2500m |
| Driver memory | 2g |
| `spark.sql.shuffle.partitions` | 200 |

## Tools & framework (§3.2.2)

| Tool | Versi |
|------|-------|
| Apache Iceberg | 1.5.2 |
| Hive Metastore | 4.0.0 |
| Airflow | 2.9.1 |
| Trino | 435 |
| Superset | 4.0.x |
| Grafana / Prometheus | 11 / 2.52 |
| MinIO | latest |

## Konfigurasi AQE (§3.2.3)

### Skenario OFF

| Parameter | Nilai |
|-----------|-------|
| `spark.sql.adaptive.enabled` | false |
| `spark.sql.adaptive.coalescePartitions.enabled` | false |
| `spark.sql.adaptive.skewJoin.enabled` | false |
| `spark.sql.adaptive.localShuffleReader.enabled` | false |

### Skenario ON

| Parameter | Nilai |
|-----------|-------|
| `spark.sql.adaptive.enabled` | true |
| `spark.sql.adaptive.coalescePartitions.enabled` | true |
| `spark.sql.adaptive.skewJoin.enabled` | true |
| `spark.sql.adaptive.localShuffleReader.enabled` | true |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB |

## Catatan

-
