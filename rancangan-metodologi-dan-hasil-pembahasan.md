3. Metodologi Penelitian
3.1 Desain dan Alur Penelitian
3.1.1 Jenis dan Pendekatan Penelitian
Isi:
Jenis: eksperimental kuantitatif
Pendekatan:
komparatif (AQE ON vs OFF)
controlled experiment
Variabel:
Independen: konfigurasi AQE
Dependen: performa query
Kontrol: dataset, cluster, query

3.1.2 Diagram Alir Penelitian

3.2 Lingkungan Eksperimen dan Konfigurasi Sistem
3.2.1 Infrastruktur dan Spesifikasi Cluster
Isi:
cluster config
jumlah node
executor config

3.2.2 Tools dan Framework
3.2.3 Konfigurasi Apache Spark
Isi:
parameter dasar:
executor memory
cores
parameter AQE:
spark.sql.adaptive.enabled
spark.sql.adaptive.skewJoin.enabled
spark.sql.adaptive.coalescePartitions.enabled
spark.sql.shuffle.partitions
jelaskan:
fungsi tiap parameter (singkat tapi jelas)

3.3 Dataset dan Karakteristik Data
3.3.1 Sumber dan Struktur Data
3.3.2 Ukuran dan Skala Data
Isi:
jumlah record
ukuran file
distribusi data

3.3.3 Karakteristik Distribusi Data (Data Skew)
- penting untuk AQE
Isi:
definisi skew
cara identifikasi:
 - distribusi key
jika dibuat:
 - metode inject skew

3.4 Desain Pipeline Data Lakehouse
3.4.1 Arsitektur Pipeline Secara Umum
3.4.2 Bronze Layer (Data Ingestion Layer)
 - proses dan alur kerja di bronze layer
3.4.3 Silver Layer (Data Processing dan Optimasi AQE)
 3.4.3.1 Proses Transformasi Data
 3.4.3.2 Implementasi Adaptive Query Execution (AQE)
  Isi:
   - AQE diaktifkan
   - berbasis runtime statistics
 3.4.3.3 Dynamic Partition Pruning
   Isi:
   - cara kerja
   - dampak ke I/O
 3.4.3.4 Shuffle Partition Coalescing
  Isi:
  - penggabungan partisi kecil
  - dampak ke overhead
 3.4.3.5 Skew Join Optimization
  Isi:
 - deteksi skew
 - pembagian partisi besar
3.4.4 Gold Layer (Data Consumption Layer)
3.4.4.1 Transformasi ke Model Analitik
Isi:
 - agregasi
 - fact-dimension
3.4.4.2 Optimasi Query pada Layer Gold
Isi:
 - hasil dari AQE di Silver
 - efisiensi query BI

3.5 Skenario Eksperimen
3.5.1 Variasi Konfigurasi AQE
Isi:
AQE OFF
AQE ON

3.5.2 Skenario Workload Query
Isi:
join
aggregation
filtering

3.5.3 Variasi Format Data
Isi:
CSV
Parquet

3.6 Metrik Evaluasi
3.6.1 Metrik Runtime
Isi:
execution time
throughput
speedup

3.6.2 Metrik Efektivitas AQE
Isi:
DPP reduction
coalescing ratio
skew reduction

3.6.3 Metrik Distribusi Partisi
Isi:
mean
std dev
coefficient of variation
Gini coefficient

4. Hasil dan Pembahasan
4.1 Hasil
4.1.1 Hasil Eksekusi Pipeline Data Lakehouse
Isi:
ringkasan pipeline:
Bronze -> Silver -> Gold
status:
1. berhasil / gagal
2. waktu total pipeline
Tampilkan:
1. tabel runtime pipeline
2. diagram alur (opsional)

4.1.2 Perbandingan Runtime AQE vs Non-AQE
Isi:
waktu eksekusi tiap query
perbandingan tabel:
Skenario	| Execution Time
Visual: bar chart / line chart
Wajib: tampilkan speedup (%)

4.1.3 Hasil Distribusi Partisi dan Data Skew
Isi:
metrik:
1. mean partition size
2. std dev
3. coefficient of variation
4. Gini coefficient
Tampilkan:
1. tabel distribusi
2. grafik distribusi partisi
 Fokus: sebelum vs sesudah AQE

4.1.4 Efektivitas Komponen AQE
Pisahkan jelas (ini penting banget):
4.1.4.1 Dynamic Partition Pruning
Isi:
1. jumlah partisi sebelum vs sesudah
2. reduction %
4.1.4.2 Shuffle Coalescing
Isi:
1. jumlah partisi sebelum vs sesudah
2. ukuran partisi
4.1.4.3 Skew Join Optimization
Isi:
1. deteksi skew
2. perubahan distribusi task

4.1.5 Perbandingan Berdasarkan Format Data
Isi:
CSV vs Parquet vs lainnya (jika ada)
Tampilkan:
1. execution time per format
2. resource usage

4.1.6 Hasil Berdasarkan Layer (Medallion Analysis)
Isi tabel:
Layer | AQE Impact
bronze
silver
gold
Fokus: Silver = paling berat -> AQE paling terasa