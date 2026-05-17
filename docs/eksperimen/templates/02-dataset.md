# Template — Dataset (BAB III §3.3)

## Generator

| Field | Nilai |
|-------|-------|
| Perintah | `python3 scripts/generate_bronze_data.py --mode full --profile aqe` |
| Profil | aqe / dev / aqe-large |
| Scale | 1.0 |
| Skew prodi | IF |
| Skew fraction | 0.75 |
| Seed | 42 |

## Volume (§3.3.2)

| Tabel staging | Jumlah baris | Ukuran file (MB) |
|---------------|--------------|------------------|
| raw_mahasiswa | | |
| raw_dosen | | |
| raw_lulusan | | |
| raw_mbkm | | |
| raw_penelitian | | |
| raw_pengabdian | | |
| raw_kerjasama | | |
| raw_prestasi_mahasiswa | | |
| raw_keuangan | | |
| raw_akreditasi | | |
| raw_prodi | | |
| **Total** | | |

## Karakteristik skew (§3.3.3)

| Metrik | Nilai |
|--------|-------|
| Key yang di-skew | prodi_id = IF |
| Fraksi baris ke hot key | % |
| Top-3 prodi_id (count) | 1. … 2. … 3. … |
| Metode identifikasi | COUNT GROUP BY prodi_id (Bronze/Spark) |

## Sumber data (§3.3.1)

Data sintetis domain ITERA (12 CSV), mensimulasikan SIAK, SIMPEG, SIPPMA, dll.

## Catatan

-
