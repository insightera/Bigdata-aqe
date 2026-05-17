# Template — Dampak AQE per Layer Medallion (BAB IV §4.1.6)

## Tabel utama

| Layer | Beban kerja utama | AQE diterapkan? | Dampak (ringkas) | Bukti |
|-------|-------------------|-----------------|------------------|-------|
| **Bronze** | Ingest CSV→Iceberg, scan mentah | Tidak (fokus OFF) | Minimal / tidak signifikan | Durasi staging_to_bronze |
| **Silver** | Join, agregasi, quality, shuffle | **Ya (OFF vs ON)** | **Paling besar** | metrics JSON, Spark UI |
| **Gold** | Star schema build + query BI | Tidak langsung; terima manfaat dari Silver | Sedang | Trino latency, silver_to_gold duration |

## Detail per layer

### Bronze

| Metrik | Nilai |
|--------|-------|
| Durasi pipeline (s) | |
| Rows ingested | |
| Catatan AQE | Ingest tidak menjadi variabel independen |

### Silver

| Metrik | AQE OFF | AQE ON |
|--------|---------|--------|
| Durasi (s) | | |
| Speedup (%) | — | |
| Workload terpengaruh | join mahasiswa–dosen, dll. | |

### Gold

| Metrik | Setelah Silver OFF | Setelah Silver ON |
|--------|--------------------|-------------------|
| Durasi silver_to_gold (s) | | |
| Trino W4 join (s) | | |
| Trino W5 agg (s) | | |

## Grafik (opsional)

Bar chart: relative impact Bronze vs Silver vs Gold.

## Interpretasi awal (draft pembahasan §4.2)

-
