# Data Pipeline ITERA — Medallion Architecture + Metadata Governance

## Kasus: Dashboard IKU Pimpinan ITERA

---

## 1. Gambaran Arsitektur

Pipeline ini mengikuti arsitektur **Medallion (Staging → Bronze → Silver → Gold)** dengan **metadata governance** terpusat di **Apache Atlas**. Setiap layer menghasilkan metadata yang dicatat ke Atlas secara paralel, sesuai diagram `Bigdata-pipeline-Metadata.jpg`.

```
[Sumber Data]
     │  CSV (SIAK, SIMPEG, SIPPMA, SIM Kerjasama, SIM Keuangan, BAN-PT)
     ▼
┌──────────┐     ┌──────────────┐     ┌──────────────┐
│ Staging  │ ──→ │ Bronze Layer │ ──→ │ Silver Layer │
│ (CSV)    │     │ (Iceberg)    │     │ (Enriched)   │
└──────────┘     └──────┬───────┘     └──────┬───────┘
                        │                     │
                   metadata B            metadata S
                   ┌─────────┐          ┌──────────────┐
                   │Raw Tech │          │Clean Metadata │
                   │Lineage  │          │Quality        │
                   │Profiling│          │Business       │
                   │Classif. │          │Compliance     │
                   └────┬────┘          └──────┬───────┘
                        │                      │
                        ▼                      ▼
                 ┌──────────────────────────────────┐
                 │       Apache Atlas REST API      │
                 │   (HBase + Solr + JanusGraph)    │
                 └──────────────┬───────────────────┘
                                │
                                ▼
┌──────────────┐     ┌──────────────────┐
│  Gold Layer  │ ──→ │  Portal Data     │
│ (Star Schema)│     │  Catalog (Next.js)│
└──────┬───────┘     └──────────────────┘
       │
  metadata G
  ┌──────────┐
  │Business  │
  │KPI       │
  │AI        │
  │Consumption│
  │Adv.Lineage│
  └──────────┘
       │
       ▼
  Dashboard Pimpinan (IKU)
```

---

## 2. Tech Stack

| Layer | Teknologi | Versi | Fungsi |
|-------|-----------|-------|--------|
| **Object Storage** | MinIO | latest | S3-compatible storage (staging, bronze, silver, gold, warehouse) |
| **Table Format** | Apache Iceberg | 1.5.2 | ACID table format untuk Bronze, Silver, Gold |
| **Processing** | Apache Spark + PySpark | 3.5.1 | ETL engine untuk transformasi antar layer |
| **Metastore** | Apache Hive Metastore | 4.0.0 | Catalog tabel Iceberg |
| **Orchestration** | Apache Airflow | 2.9.1 | DAG orchestrator pipeline per layer |
| **Metadata** | Apache Atlas | 2.3.0 | Metadata management, lineage, classification |
| **Graph Storage** | Apache HBase | 2.1 | Backend JanusGraph untuk Atlas lineage graph |
| **Search Index** | Apache Solr | 8.11.2 | Full-text search untuk Atlas discovery |
| **Messaging** | Apache Kafka (Confluent) | 7.5.0 | Notifikasi entitas Atlas |
| **Coordination** | Apache ZooKeeper | 7.5.0 | Koordinasi Kafka dan HBase |
| **Database** | PostgreSQL | 15-alpine | Backend Hive Metastore, Airflow, Iceberg REST |
| **Catalog Portal** | Next.js + React + Bootstrap | 16 / 19.2 / 5.3 | Frontend Data Catalog Management |
| **Notebook** | Jupyter + PySpark | latest | Eksplorasi data interaktif |
| **Containerization** | Docker + Docker Compose | 24.x / v2 | Orkestrasi seluruh layanan |

---

## 3. Sumber Data (Staging Layer)

Semua sumber data disimpan sebagai **CSV** (simulasi batch dari sistem informasi akademik ITERA), kemudian di-upload ke MinIO `staging` bucket oleh Airflow DAG.

### Domain Data

| No | Domain | Sistem Sumber | Format | Tabel |
|----|--------|--------------|--------|-------|
| 1 | Mahasiswa & Akademik | SIAK / PDDikti | CSV | `raw_mahasiswa`, `raw_prestasi_mahasiswa` |
| 2 | Lulusan & Karir | SIAK / Tracer Study | CSV | `raw_lulusan` |
| 3 | Dosen & Kepegawaian | SIMPEG | CSV | `raw_dosen`, `raw_kegiatan_dosen` |
| 4 | Penelitian & PkM | SIPPMA / LPPM | CSV | `raw_penelitian`, `raw_pengabdian` |
| 5 | Kerjasama & MBKM | SIM Kerjasama | CSV | `raw_kerjasama`, `raw_mbkm` |
| 6 | Keuangan & Anggaran | SIM Keuangan | CSV | `raw_keuangan` |
| 7 | Akreditasi & Mutu | BAN-PT / LP3M | CSV | `raw_akreditasi` |
| 8 | Program Studi | SIAK | CSV | `raw_prodi` |

---

## 4. Desain Tabel per Layer

### 4.1 LAYER BRONZE — Raw Iceberg Tables

> CSV dari staging dikonversi ke **Apache Iceberg** tables di MinIO (`s3a://warehouse/bronze/`).
> ETL: `scripts/spark/staging_to_bronze.py` via PySpark.
> DAG: `scripts/dags/staging_bronze_pipeline.py`.

**Metadata yang dicatat ke Atlas (layer B):**
1. Raw Technical Metadata — skema, tipe kolom, lokasi S3, format
2. Raw Lineage — staging CSV → bronze Iceberg table
3. Raw Data Profiling — row_count, null_count, distinct_count, completeness
4. Raw Classification — `PII`, `Staging_Layer`, `Bronze_Layer`

#### `raw_mahasiswa`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| mahasiswa_id | STRING | NIM mahasiswa |
| nama | STRING | Nama lengkap |
| prodi_id | STRING | Kode program studi |
| jurusan_id | STRING | Kode jurusan |
| angkatan | INT | Tahun masuk |
| jalur_masuk | STRING | SNBP / SNBT / Mandiri |
| jenis_kelamin | STRING | L / P |
| asal_provinsi | STRING | Provinsi asal |
| status_aktif | STRING | Aktif / Cuti / DO |
| ipk_terakhir | FLOAT | IPK kumulatif |
| total_sks | INT | SKS yang sudah ditempuh |
| sks_luar_kampus | INT | SKS MBKM di luar kampus |
| tanggal_masuk | DATE | Tanggal registrasi pertama |
| ingested_at | TIMESTAMP | Waktu data masuk pipeline |

#### `raw_lulusan`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| lulusan_id | STRING | ID unik lulusan |
| mahasiswa_id | STRING | FK ke mahasiswa |
| prodi_id | STRING | Kode prodi |
| tanggal_lulus | DATE | Tanggal wisuda |
| ipk | FLOAT | IPK akhir |
| lama_studi_bulan | INT | Durasi studi dalam bulan |
| status_pasca_lulus | STRING | Bekerja/Studi/Wirausaha/Belum |
| nama_perusahaan | STRING | Tempat kerja (jika ada) |
| bidang_kerja | STRING | Sesuai prodi / Tidak |
| masa_tunggu_bulan | INT | Bulan sampai dapat kerja pertama |
| sumber_data | STRING | Tracer Study / Laporan Wisuda |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_dosen`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| dosen_id | STRING | NIDN / NIDK |
| nama | STRING | Nama dosen |
| prodi_id | STRING | Prodi home base |
| jurusan_id | STRING | Jurusan |
| jenis_kelamin | STRING | L / P |
| status_asn | STRING | PNS / PPPK / Non-ASN |
| pendidikan_terakhir | STRING | S2 / S3 |
| jabatan_fungsional | STRING | Tenaga Pengajar / Asisten Ahli / Lektor / dst |
| sedang_tugas_belajar | BOOLEAN | True/False |
| sertifikat_dosen | BOOLEAN | True/False |
| berasal_praktisi | BOOLEAN | True/False |
| tahun_bergabung | INT | Tahun rekrut |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_kegiatan_dosen`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| kegiatan_id | STRING | ID unik kegiatan |
| dosen_id | STRING | FK ke dosen |
| jenis_kegiatan | STRING | Tridarma_PT_Lain / Praktisi_Industri / Pembina_Prestasi / QS100 |
| nama_institusi | STRING | Nama PT/Industri mitra |
| tanggal_mulai | DATE | Mulai kegiatan |
| tanggal_selesai | DATE | Selesai kegiatan |
| tahun | INT | Tahun pelaksanaan |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_penelitian`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| penelitian_id | STRING | ID penelitian |
| judul | STRING | Judul penelitian |
| dosen_id | STRING | Ketua peneliti (FK) |
| jurusan_id | STRING | Jurusan |
| tahun | INT | Tahun pelaksanaan |
| skema | STRING | Mandiri / Dana ITERA / Hibah Dikti / Industri |
| dana | BIGINT | Nominal dana (Rp) |
| topik | STRING | Sustainable Energy / Innovative Industry / Green Infrastructure / Community Dev |
| status_publikasi | STRING | Belum / Jurnal Nasional / Jurnal Internasional |
| rekognisi_internasional | BOOLEAN | True/False |
| diterapkan_masyarakat | BOOLEAN | True/False |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_pengabdian`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| pkm_id | STRING | ID PkM |
| judul | STRING | Judul pengabdian |
| dosen_id | STRING | Ketua (FK) |
| jurusan_id | STRING | Jurusan |
| tahun | INT | Tahun |
| dana | BIGINT | Nominal dana (Rp) |
| lokasi | STRING | Kabupaten/Kota |
| rekognisi_internasional | BOOLEAN | True/False |
| diterapkan_masyarakat | BOOLEAN | True/False |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_kerjasama`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| kerjasama_id | STRING | ID MoU/PKS |
| jenis | STRING | MoU / PKS |
| mitra | STRING | Nama institusi mitra |
| kategori_mitra | STRING | PT_DN / PT_LN / Industri / Pemerintah / Litbang |
| lingkup | STRING | Pendidikan / Penelitian / PkM / MBKM / Semua |
| prodi_id | STRING | Prodi terkait (FK, nullable) |
| tanggal_mulai | DATE | Efektif dari |
| tanggal_berakhir | DATE | Berakhir |
| status | STRING | Aktif / Tidak Aktif |
| tahun | INT | Tahun penandatanganan |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_mbkm`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| mbkm_id | STRING | ID kegiatan MBKM |
| mahasiswa_id | STRING | FK mahasiswa |
| prodi_id | STRING | Prodi asal |
| jenis_mbkm | STRING | Magang / KKN Tematik / Riset / Pertukaran / Wirausaha / Proyek Kemanusiaan |
| institusi_mitra | STRING | Nama mitra |
| sks_diakui | INT | SKS yang diakui |
| tahun | INT | Tahun pelaksanaan |
| semester | STRING | Ganjil / Genap |
| prestasi_nasional | BOOLEAN | True/False (jika kompetisi) |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_akreditasi`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| akreditasi_id | STRING | ID akreditasi |
| prodi_id | STRING | FK prodi |
| lembaga | STRING | BAN-PT / LAM / Internasional |
| nama_lembaga_detail | STRING | Nama spesifik |
| predikat | STRING | Baik / Baik Sekali / Unggul / A / B / Internasional |
| tanggal_sk | DATE | Tanggal SK |
| tanggal_berakhir | DATE | Masa berlaku |
| tahun | INT | Tahun akreditasi |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_prodi`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| prodi_id | STRING | Kode prodi |
| nama_prodi | STRING | Nama program studi |
| jenjang | STRING | S1 / S2 / D3 |
| jurusan_id | STRING | FK jurusan |
| tahun_berdiri | INT | Tahun SK pendirian |
| status | STRING | Aktif / Proses |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_keuangan`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| anggaran_id | STRING | ID anggaran |
| tahun | INT | Tahun anggaran |
| sumber_dana | STRING | PNBP / APBN / SBSN / Hibah |
| komponen | STRING | Gaji / Operasional / Penelitian / Investasi / PkM |
| pagu | BIGINT | Pagu anggaran (Rp) |
| realisasi | BIGINT | Realisasi (Rp) |
| triwulan | INT | 1 / 2 / 3 / 4 |
| ingested_at | TIMESTAMP | Waktu data masuk |

#### `raw_prestasi_mahasiswa`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| prestasi_id | STRING | ID prestasi |
| mahasiswa_id | STRING | FK mahasiswa |
| nama_kompetisi | STRING | Nama lomba/kompetisi |
| bidang | STRING | Riset / Olahraga / Seni / Teknologi / dst |
| tingkat | STRING | Institusi / Regional / Nasional / Internasional |
| peringkat | STRING | Juara 1 / 2 / 3 / Finalis |
| tahun | INT | Tahun |
| dosen_pembina_id | STRING | FK dosen (nullable) |
| ingested_at | TIMESTAMP | Waktu data masuk |

---

### 4.2 LAYER SILVER — Enriched Iceberg Tables

> Data dari Bronze yang telah lolos quality check, di-cleaning, dan di-enrich.
> ETL: `scripts/spark/bronze_to_silver.py` via PySpark.
> DAG: `scripts/dags/bronze_silver_pipeline.py`.
> Storage: `lakehouse.silver.*` Iceberg tables di MinIO (`s3a://warehouse/silver/`).

**Data Quality Rules:**

| Status | Completeness Score | Aksi |
|--------|-------------------|------|
| **PASS** | ≥ 80% | Lanjut ke Silver layer |
| **QUARANTINE** | 60% — 79% | Flag untuk review, lanjut dengan peringatan |
| **REJECT** | < 60% | Ditolak — tidak diproses |

**Metadata yang dicatat ke Atlas (layer S):**
1. Clean Metadata — skema setelah transformasi
2. Quality Metadata — quality_score, status (PASS/QUARANTINE)
3. Transformation Lineage — multi-source bronze → silver
4. Business Metadata — owner, IKU relevance, glossary terms
5. Compliance Metadata — PII fields, data classification, retention, access

**Tabel Silver (transformasi dari Bronze):**

| Tabel | Sumber Bronze | Transformasi |
|-------|--------------|-------------|
| `silver_mahasiswa` | raw_mahasiswa + raw_prodi | Join prodi, tambah flag `is_mbkm` (sks_luar_kampus ≥ 20) |
| `silver_lulusan` | raw_lulusan | Tambah flag `is_employed`, `is_lanjut_studi`, `is_wirausaha`, `is_terserap` |
| `silver_dosen` | raw_dosen | Tambah flag `is_s3`, `is_praktisi`, `is_serdos`, `is_aktif_tridarma` |
| `silver_penelitian_pkm` | raw_penelitian + raw_pengabdian | Union dan normalisasi, flag `is_rekognisi`, `is_diterapkan` |
| `silver_kerjasama_aktif` | raw_kerjasama | Filter aktif saja, tambah flag `is_mbkm` |
| `silver_akreditasi_aktif` | raw_akreditasi | Akreditasi terakhir per prodi yang masih berlaku, flag `is_internasional` |

---

### 4.3 LAYER GOLD — Star Schema (Iceberg)

> Tabel dimensi dan fakta untuk **OLAP Data Warehousing** yang menjadi sumber **Dashboard Pimpinan**.
> ETL: `scripts/spark/silver_to_gold.py` via PySpark.
> DAG: `scripts/dags/silver_gold_pipeline.py`.
> Storage: `lakehouse.gold.*` Iceberg tables di MinIO (`s3a://warehouse/gold/`).

**Metadata yang dicatat ke Atlas (layer G):**
1. Business Metadata — KPI definitions, star schema relationships, ownership
2. KPI Metadata — target Renstra per tahun, capaian aktual, formula, status
3. AI Metadata — ML readiness, feature store candidates, suggested models
4. Consumption Metadata — consumers (Rektor, WR, LP3M, dll), dashboard panel
5. Advanced Lineage — full chain staging → bronze → silver → gold

---

#### 4.3A. DIMENSI (5 tabel)

##### `dim_waktu`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| waktu_id | INT | Surrogate key |
| tahun | INT | Tahun (2020–2025) |
| semester | STRING | Ganjil / Genap |
| triwulan | INT | 1–4 |
| bulan | INT | 1–12 |
| nama_bulan | STRING | Januari–Desember |

##### `dim_prodi`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| prodi_id | STRING | NK dari raw |
| nama_prodi | STRING | Nama prodi |
| jenjang | STRING | S1/S2/D3 |
| nama_jurusan | STRING | Teknik dan Komputer / Sains / dst |
| nama_fakultas | STRING | ITERA |
| tahun_berdiri | INT | Tahun SK |
| status | STRING | Aktif/Proses |

##### `dim_dosen`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| dosen_id | STRING | NIDN/NIDK |
| nama | STRING | |
| prodi_id | STRING | FK |
| status_asn | STRING | PNS/Non-ASN |
| pendidikan | STRING | S2/S3 |
| jabatan_fungsional | STRING | |
| is_s3 | BOOLEAN | |
| is_serdos | BOOLEAN | |
| is_praktisi | BOOLEAN | |

##### `dim_mahasiswa`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| mahasiswa_id | STRING | NIM |
| prodi_id | STRING | FK |
| angkatan | INT | |
| jalur_masuk | STRING | |
| asal_provinsi | STRING | |
| jenis_kelamin | STRING | |

##### `dim_topik_penelitian`
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| topik_id | INT | SK |
| nama_topik | STRING | Sustainable Energy / Innovative Industry / Green Infrastructure / Community Dev |
| deskripsi | STRING | |

---

#### 4.3B. TABEL FAKTA IKU (10 tabel)

##### `fact_iku1_lulusan` — IKU-1: Lulusan Bekerja/Studi/Wirausaha
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK dim_waktu |
| prodi_id | STRING | FK dim_prodi |
| total_lulusan | INT | Jumlah lulusan periode |
| lulusan_bekerja | INT | |
| lulusan_lanjut_studi | INT | |
| lulusan_wirausaha | INT | |
| lulusan_belum | INT | |
| persen_terserap | FLOAT | (bekerja+studi+wirausaha)/total |
| target_iku | FLOAT | Target sesuai Renstra |
| capaian_iku | FLOAT | Persen capaian |

##### `fact_iku2_mbkm` — IKU-2: Mahasiswa ≥20 SKS di Luar Kampus / Prestasi Nasional
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| prodi_id | STRING | FK |
| total_mahasiswa_aktif | INT | |
| mahasiswa_sks_luar_20 | INT | Mahasiswa ≥20 SKS luar kampus |
| mahasiswa_prestasi_nasional | INT | Prestasi ≥ nasional |
| mahasiswa_memenuhi_iku2 | INT | Union keduanya |
| persen_iku2 | FLOAT | |
| target_iku | FLOAT | |
| capaian_iku | FLOAT | |

##### `fact_iku3_dosen_tridarma` — IKU-3: Dosen Aktif Tridarma Luar / Industri / Bina Prestasi
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| prodi_id | STRING | FK |
| total_dosen_tetap | INT | |
| dosen_tridarma_pt_lain | INT | |
| dosen_praktisi_industri | INT | |
| dosen_bina_prestasi | INT | |
| dosen_qs100 | INT | |
| dosen_memenuhi_iku3 | INT | Union |
| persen_iku3 | FLOAT | |
| target_iku | FLOAT | |
| capaian_iku | FLOAT | |

##### `fact_iku4_kualifikasi_dosen` — IKU-4: Dosen S3/Sertifikat/Praktisi
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| prodi_id | STRING | FK |
| total_dosen_tetap | INT | |
| dosen_s3 | INT | Bergelar Doktor |
| dosen_sertifikat_industri | INT | Sertifikat kompetensi/profesi |
| dosen_dari_praktisi | INT | Dari industri/dunia kerja |
| dosen_memenuhi_iku4 | INT | Union |
| persen_iku4 | FLOAT | |
| target_iku | FLOAT | |
| capaian_iku | FLOAT | |

##### `fact_iku5_penelitian_pkm` — IKU-5: Rekognisi Internasional / Diterapkan Masyarakat per Dosen
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| jurusan_id | STRING | FK (level jurusan) |
| total_dosen | INT | |
| output_rekognisi_internasional | INT | Penelitian + PkM dengan rekognisi intl |
| output_diterapkan_masyarakat | INT | Yang digunakan langsung masyarakat |
| total_output_eligible | INT | |
| rasio_per_dosen | FLOAT | total_output / total_dosen |
| target_iku | FLOAT | |
| capaian_iku | FLOAT | |

##### `fact_iku6_kerjasama_prodi` — IKU-6: Prodi Bekerjasama dengan Mitra
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| total_prodi_s1 | INT | |
| prodi_berkerjasama | INT | Punya ≥1 kerjasama aktif |
| persen_iku6 | FLOAT | |
| target_iku | FLOAT | |
| capaian_iku | FLOAT | |

##### `fact_iku7_metode_pembelajaran` — IKU-7: MK Case Method / Team-Based
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| prodi_id | STRING | FK |
| total_mk | INT | Total mata kuliah |
| mk_case_method | INT | MK pakai case method |
| mk_team_based | INT | MK pakai team-based project |
| mk_memenuhi | INT | Union |
| persen_iku7 | FLOAT | |
| target_iku | FLOAT | |
| capaian_iku | FLOAT | |

##### `fact_iku8_akreditasi_internasional` — IKU-8: Prodi Akreditasi / Sertifikat Internasional
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| total_prodi_s1 | INT | |
| prodi_akreditasi_internasional | INT | |
| persen_iku8 | FLOAT | |
| target_iku | FLOAT | |
| capaian_iku | FLOAT | |

##### `fact_tata_kelola` — Sasaran 4: SAKIP & Kinerja Anggaran
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| predikat_sakip | STRING | BB / B / A / AA |
| nilai_sakip | FLOAT | Skor numerik |
| nilai_kinerja_anggaran | FLOAT | Skor RKA-K/L |
| pagu_total | BIGINT | Total pagu anggaran |
| realisasi_total | BIGINT | Total realisasi |
| persen_realisasi | FLOAT | |
| target_sakip | STRING | BB |
| target_kinerja_anggaran | FLOAT | 80/85 |

##### `fact_rekap_iku_institusi` — Ringkasan Semua IKU (Executive Dashboard)
| Atribut | Tipe | Keterangan |
|---------|------|------------|
| fact_id | INT | SK |
| waktu_id | INT | FK |
| iku_kode | STRING | IKU-1 s.d. IKU-8 |
| iku_nama | STRING | Deskripsi IKU |
| nilai_capaian | FLOAT | Nilai aktual |
| nilai_target | FLOAT | Target Renstra |
| satuan | STRING | % / Rasio / Predikat |
| status_capaian | STRING | Tercapai / Tidak Tercapai / On Track |

---

## 5. Target IKU per Tahun (Renstra ITERA 2020–2024)

| IKU | Indikator | 2021 | 2022 | 2023 | 2024 | 2025 |
|-----|-----------|------|------|------|------|------|
| IKU-1 | % Lulusan bekerja/studi/wirausaha | 75% | 76% | 77% | 78% | 80% |
| IKU-2 | % Mahasiswa ≥20 SKS luar kampus / prestasi nasional | 20% | 25% | 30% | 35% | 40% |
| IKU-3 | % Dosen tridarma luar/praktisi/bina prestasi | 15% | 17% | 20% | 25% | 30% |
| IKU-4 | % Dosen S3/sertifikat kompetensi/praktisi | 30% | 32% | 40% | 50% | 55% |
| IKU-5 | Rasio output penelitian rekognisi intl per dosen | 0.10 | 0.15 | 0.20 | 0.25 | 0.30 |
| IKU-6 | % Prodi bekerjasama dengan mitra | 35% | 40% | 50% | 60% | 65% |
| IKU-7 | % MK case method / team-based project | 25% | 30% | 35% | 40% | 45% |
| IKU-8 | % Prodi akreditasi / sertifikat internasional | 2.5% | 2.5% | 2.5% | 3.0% | 5.0% |
| SAKIP | Predikat SAKIP | BB | BB | BB | BB | A |
| Anggaran | Nilai Kinerja Anggaran | 80 | 85 | 85 | 85 | 87 |

---

## 6. Metadata per Layer (sesuai diagram arsitektur)

| Layer | Repositori | Metadata | Atlas Classifications |
|-------|-----------|----------|----------------------|
| **Bronze (B)** | Raw Technical | 1. Raw Technical Metadata<br>2. Raw Lineage<br>3. Raw Data Profiling<br>4. Raw Classification | `Staging_Layer`, `Bronze_Layer`, `PII` |
| **Silver (S)** | Clean + Quality | 1. Clean Metadata<br>2. Quality Metadata<br>3. Transformation Lineage<br>4. Business Metadata<br>5. Compliance Metadata | `Silver_Layer`, `Quality_Pass`, `Quality_Quarantine` |
| **Gold (G)** | KPI + BI | 1. Business Metadata<br>2. KPI Metadata<br>3. AI Metadata<br>4. Consumption Metadata<br>5. Advanced Lineage | `Gold_Layer`, `KPI_Metric`, `Star_Schema_Dimension`, `Star_Schema_Fact`, `Executive_Dashboard` |

---

## 7. Struktur Storage

### MinIO Buckets

| Bucket | Fungsi |
|--------|--------|
| `staging` | Landing raw CSV dari sumber |
| `warehouse` | Iceberg tables (bronze, silver, gold namespaces) |
| `airflow-logs` | Log Airflow |

### Iceberg Catalog (Hive Metastore)

```
lakehouse
├── bronze/               ← namespace Bronze
│   ├── raw_mahasiswa     (Iceberg table)
│   ├── raw_lulusan
│   ├── raw_dosen
│   ├── raw_kegiatan_dosen
│   ├── raw_penelitian
│   ├── raw_pengabdian
│   ├── raw_kerjasama
│   ├── raw_mbkm
│   ├── raw_akreditasi
│   ├── raw_prodi
│   ├── raw_keuangan
│   └── raw_prestasi_mahasiswa
│
├── silver/               ← namespace Silver
│   ├── silver_mahasiswa
│   ├── silver_lulusan
│   ├── silver_dosen
│   ├── silver_penelitian_pkm
│   ├── silver_kerjasama_aktif
│   └── silver_akreditasi_aktif
│
└── gold/                 ← namespace Gold (Star Schema)
    ├── dim_waktu
    ├── dim_prodi
    ├── dim_dosen
    ├── dim_mahasiswa
    ├── dim_topik_penelitian
    ├── fact_iku1_lulusan
    ├── fact_iku2_mbkm
    ├── fact_iku3_dosen_tridarma
    ├── fact_iku4_kualifikasi_dosen
    ├── fact_iku5_penelitian_pkm
    ├── fact_iku6_kerjasama_prodi
    ├── fact_iku7_metode_pembelajaran
    ├── fact_iku8_akreditasi_internasional
    ├── fact_tata_kelola
    └── fact_rekap_iku_institusi
```

---

## 8. Pipeline & Script

| Pipeline | ETL Script | Atlas Script | Airflow DAG |
|----------|-----------|-------------|-------------|
| **Staging → Bronze** | `scripts/spark/staging_to_bronze.py` | `scripts/atlas/register_bronze_metadata.py` | `scripts/dags/staging_bronze_pipeline.py` |
| **Bronze → Silver** | `scripts/spark/bronze_to_silver.py` | `scripts/atlas/register_silver_metadata.py` | `scripts/dags/bronze_silver_pipeline.py` |
| **Silver → Gold** | `scripts/spark/silver_to_gold.py` | `scripts/atlas/register_gold_metadata.py` | `scripts/dags/silver_gold_pipeline.py` |

**Generator data sintetis:** `scripts/generate_bronze_data.py` — menghasilkan 12 CSV untuk staging layer.

---

## 9. Atlas Data Catalog Summary

```
Atlas Entities:
  Staging:  12 lakehouse_dataset (CSV sources)
  Bronze:   12 lakehouse_dataset (Iceberg tables)
  Silver:    6 lakehouse_dataset (Enriched tables)
  Gold:     15 lakehouse_dataset (5 dim + 10 fact)
  Total:   ~45 entities

Lineage Processes:
  staging → bronze:   12 lakehouse_etl_process
  bronze → silver:     6 lakehouse_etl_process
  silver → gold:     ~13 lakehouse_etl_process
  Total:             ~31 processes

Classifications (11):
  PII, Staging_Layer, Bronze_Layer, Silver_Layer, Gold_Layer,
  Quality_Pass, Quality_Quarantine, KPI_Metric,
  Star_Schema_Dimension, Star_Schema_Fact, Executive_Dashboard
```

---

## 10. Catatan Implementasi

- Semua data disimpan sebagai **Apache Iceberg** tables di **MinIO** (S3-compatible), bukan file Parquet langsung
- Iceberg menyediakan fitur **ACID transactions**, **time travel**, dan **schema evolution**
- **Hive Metastore** (backed by PostgreSQL) menjadi catalog untuk Iceberg tables
- **PySpark 3.5.1** digunakan sebagai engine ETL di semua pipeline
- **Apache Airflow 2.9.1** mengorkestrasi setiap pipeline sebagai DAG terpisah
- **Apache Atlas 2.3.0** menjadi metadata backbone — menyimpan entity, classification, lineage, dan mendukung search via **Solr** serta graph via **JanusGraph** (HBase)
- **Data Catalog Portal** (Next.js 16) menjadi antarmuka visual untuk discovery, lineage, dan KPI dashboard
- File CSV di staging layer mensimulasikan data dari sistem informasi akademik ITERA
- Quality check di Silver menggunakan completeness-based scoring: PASS ≥80%, QUARANTINE 60-79%, REJECT <60%
- Gold layer menggunakan **star schema** (5 dimensi + 10 fakta IKU) untuk OLAP query Dashboard Pimpinan
