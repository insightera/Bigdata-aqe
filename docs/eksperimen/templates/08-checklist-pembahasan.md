# Template — Checklist Pembahasan (BAB IV §4.2)

Centang setelah narasi ditulis di laporan.

## 4.2.1 Analisis performa & AQE

- [ ] Mengapa completeness/kualitas data Silver relevan dengan performa?
- [ ] Mengapa **Silver > Bronze** dalam dampak AQE?
- [ ] Jelaskan peran **runtime statistics** dan Adaptive Optimizer
- [ ] Bahas hasil **speedup** (positif/negatif) dengan angka dari §4.1.2
- [ ] Komponen mana yang dominan: **DPP**, **coalescing**, atau **skew join**?

## 4.2.2 Keterbatasan

- [ ] Lingkungan Docker single-host (bukan cluster YARN/K8s produksi)
- [ ] Ukuran data masih lab (profil `aqe`, bukan PB skala DC)
- [ ] Prometheus/Grafana terbatas (metrik manual dari Spark UI)
- [ ] Dua run Silver menimpa tabel — jelaskan mitigasi metodologi

## 4.2.3 Implikasi

- [ ] Rekomendasi konfigurasi produksi (`AQE ON`, `shuffle.partitions`)
- [ ] Kapan AQE kurang menguntungkan (data kecil, overhead adaptasi)
- [ ] Hubungan dengan serving layer (Trino/Superset)

## Kontribusi penelitian (kalimat siap edit)

1.
2.
3.
