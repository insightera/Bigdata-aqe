# Template — Perbandingan Format Data (BAB IV §4.1.5)

> Opsional jika menjalankan variasi §3.5.3

## Skenario

| Run | Format staging | Konfigurasi AQE Silver | Keterangan |
|-----|----------------|------------------------|------------|
| F1 | CSV | OFF / ON | Baseline |
| F2 | Parquet | OFF / ON | Setelah konversi staging |

## Execution time (detik)

| Tahap | CSV + OFF | CSV + ON | Parquet + OFF | Parquet + ON |
|-------|-----------|----------|---------------|--------------|
| Staging → Bronze | | | | |
| Bronze → Silver | | | | |
| **Total** | | | | |

## Resource usage

| Format | AQE | CPU avg (%) | Memory peak | Shuffle total (MB) |
|--------|-----|-------------|-------------|---------------------|
| CSV | OFF | | | |
| CSV | ON | | | |
| Parquet | OFF | | | |
| Parquet | ON | | | |

## Kesimpulan singkat (untuk pembahasan)

-
