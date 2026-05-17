# Template — Perbandingan AQE OFF vs ON (BAB IV §4.1.2)

## Pipeline Silver (fokus utama)

| Skenario | File metrik JSON | Airflow duration (s) | `duration_sec` JSON | Throughput (rows/s) |
|----------|------------------|----------------------|---------------------|---------------------|
| **AQE OFF** | metrics/bronze_to_silver_aqe_OFF_*.json | | | |
| **AQE ON** | metrics/bronze_to_silver_aqe_ON_*.json | | | |

Throughput = `summary.rows_written / duration_sec`

## Speedup

| Rumus | Nilai |
|-------|-------|
| T_OFF (s) | |
| T_ON (s) | |
| Speedup (%) = (T_OFF − T_ON) / T_OFF × 100 | |

## Workload query (durasi detik)

| Workload ID | Tipe | AQE OFF | AQE ON | Speedup (%) |
|-------------|------|---------|--------|-------------|
| W1 | Join | | | |
| W2 | Aggregation | | | |
| W3 | Filtering | | | |
| W4 | Join (Gold/Trino) | | | |
| W5 | Aggregation (Gold/Trino) | | | |

## Konfigurasi Spark efektif (dari JSON)

### OFF

| Key | Nilai |
|-----|-------|
| spark.sql.adaptive.enabled | |
| spark.sql.shuffle.partitions | |

### ON

| Key | Nilai |
|-----|-------|
| spark.sql.adaptive.enabled | |
| spark.sql.shuffle.partitions | |

## Grafik wajib (lampiran)

| No | Jenis grafik | File |
|----|--------------|------|
| 1 | Bar chart: duration OFF vs ON | |
| 2 | Bar chart: speedup per workload | |

## Screenshot Spark UI

| Skenario | Aplikasi Spark | File screenshot |
|----------|----------------|-----------------|
| OFF | bronze_to_silver_AQE_OFF | |
| ON | bronze_to_silver_AQE_ON | |

## Catatan

-
