# Template — Distribusi Partisi & Efektivitas AQE (BAB IV §4.1.3–4.1.4)

## §4.1.3 Distribusi partisi & skew

Workload / stage yang dianalisis: _______________________

| Metrik | AQE OFF | AQE ON |
|--------|---------|--------|
| Mean partition size (MB) | | |
| Std dev (MB) | | |
| Coefficient of variation (CV) | | |
| Gini coefficient | | |

### Grafik

| No | Deskripsi | File |
|----|-----------|------|
| 1 | Histogram ukuran partisi OFF | |
| 2 | Histogram ukuran partisi ON | |

Sumber: Spark UI → Stage → Tasks; atau analisis event log.

---

## §4.1.4.1 Dynamic Partition Pruning (DPP)

| Metrik | Sebelum (OFF) | Sesudah (ON) | Reduction (%) |
|--------|---------------|--------------|---------------|
| Jumlah partisi file dibaca | | | |
| Bytes scan | | | |

Cara observasi: Spark SQL → physical plan node `BatchScan`, `Filter`; bandingkan `number of files read`.

Screenshot plan: _______________________

---

## §4.1.4.2 Shuffle Partition Coalescing

| Metrik | OFF | ON |
|--------|-----|-----|
| Jumlah shuffle partitions (awal) | | |
| Jumlah shuffle partitions (akhir) | | |
| Coalescing ratio (awal/akhir) | | |
| Mean partition size setelah shuffle (MB) | | |

---

## §4.1.4.3 Skew Join Optimization

| Metrik | OFF | ON |
|--------|-----|-----|
| Skew terdeteksi? (ya/tidak) | | |
| Jumlah task skewed | | |
| Max task duration (s) | | |
| Min task duration (s) | | |
| CV task duration | | |

Screenshot: stage join dengan `OptimizeSkewedJoin` (ON).

---

## Resource (untuk §4.1.5 / pembahasan)

| Resource | OFF | ON |
|----------|-----|-----|
| Peak executor memory | | |
| Shuffle read total (MB) | | |
| Shuffle write total (MB) | | |
| Spill to disk | | |

## Catatan

-
