# Bronze Layer — Raw Data Generator

Data sintetis ITERA untuk 12 tabel Bronze (Station 1) pada pipeline Medallion.

## Prasyarat

- Python 3.10+ (tidak perlu install library tambahan, semua pakai stdlib)

## Cara Eksekusi

Jalankan dari **root project** (`Data-Lakehouse-Metadata/`):

### 1. Full Generate (data awal)

Membuat semua CSV dari nol. File lama di-overwrite.

```bash
python scripts/generate_bronze_data.py --mode full --scale 1.0
```

| Flag | Nilai | Keterangan |
|------|-------|------------|
| `--scale` | `1.0` | ~78 ribu baris, ~9 MB |
| `--scale` | `5.0` | ~390 ribu baris, ~45 MB |
| `--scale` | `10.0` | ~780 ribu baris, ~90 MB |
| `--seed` | `42` | Random seed (default 42, ubah untuk variasi data berbeda) |

### 2. Append / Incremental (simulasi data baru masuk)

Menambahkan batch baru ke CSV yang sudah ada **tanpa menghapus data lama**.
Cocok untuk men-trigger pipeline data engineering secara berkala.

```bash
python scripts/generate_bronze_data.py --mode append --batch-size 500
```

| Flag | Default | Keterangan |
|------|---------|------------|
| `--batch-size` | `500` | Jumlah mahasiswa baru per batch (tabel lain proporsional) |

Setiap eksekusi append menghasilkan data dengan `ingested_at` timestamp terbaru.

### 3. Custom Output Directory

```bash
python scripts/generate_bronze_data.py --mode full --output-dir /path/ke/folder/lain
```

## File yang Dihasilkan

```
data/bronze/
├── raw_mahasiswa.csv          ← 50.000 baris (scale 1.0)
├── raw_lulusan.csv            ←  8.714
├── raw_mbkm.csv               ←  7.836
├── raw_prestasi_mahasiswa.csv ←  5.000
├── raw_kegiatan_dosen.csv     ←  1.994
├── raw_penelitian.csv         ←  1.583
├── raw_pengabdian.csv         ←    855
├── raw_dosen.csv              ←    800
├── raw_keuangan.csv           ←    480
├── raw_kerjasama.csv          ←    400
├── raw_akreditasi.csv         ←     46
└── raw_prodi.csv              ←     25
```

## Contoh Alur Kerja

```bash
# 1. Generate data awal skala besar
python scripts/generate_bronze_data.py --mode full --scale 5.0

# 2. Upload ke MinIO (staging bucket)
mc cp --recursive data/bronze/ local/staging/bronze/

# 3. Beberapa hari kemudian — simulasi data baru
python scripts/generate_bronze_data.py --mode append --batch-size 2000

# 4. Upload ulang → pipeline Airflow mendeteksi perubahan
mc cp --recursive data/bronze/ local/staging/bronze/
```

## Catatan

- Mode `full` menggunakan `--seed 42` sehingga hasilnya **reproducible** (data sama setiap kali dijalankan dengan seed yang sama).
- Mode `append` menggunakan random seed acak agar setiap batch menghasilkan data berbeda.
- `raw_lulusan` tidak muncul saat append karena mahasiswa angkatan 2024–2025 belum lulus — ini sesuai logika bisnis.
- Referential integrity antar tabel terjaga (FK mahasiswa ↔ lulusan, dosen ↔ penelitian, dsb).
