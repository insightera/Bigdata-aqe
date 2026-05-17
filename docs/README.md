## Deskripsi Alur Arsitektur Pipeline dan Eksperimen Adaptive Query Execution (AQE)

Arsitektur pipeline pada penelitian ini dirancang menggunakan pendekatan **Data Lakehouse Medallion Architecture** yang terdiri atas tiga lapisan utama, yaitu **Bronze Layer**, **Silver Layer**, dan **Gold Layer**, dengan integrasi mekanisme **Adaptive Query Execution (AQE)** pada proses pemrosesan data menggunakan Apache Spark. Selain itu, penelitian juga melibatkan skenario eksperimen komparatif antara konfigurasi **AQE OFF** dan **AQE ON** untuk mengukur efektivitas optimasi query terhadap performa pipeline big data.

---

# 1. Data Sources dan Staging Layer

Alur penelitian dimulai dari berbagai sumber data (*data sources*) yang dapat berupa:

* database operasional,
* file log,
* API/web services,
* maupun data IoT.

Data mentah dari berbagai sumber tersebut terlebih dahulu masuk ke **Staging Layer** sebagai area penampungan sementara (*landing area*). Pada tahap ini dilakukan proses awal berupa:

* validasi struktur data,
* pengecekan skema (*schema enforcement*),
* serta standarisasi format data.

Tahap staging bertujuan untuk memastikan bahwa data yang masuk ke sistem memiliki kualitas dan konsistensi yang memadai sebelum diproses lebih lanjut.

---

# 2. Columnar Processing dan Representasi Format Data

Setelah melalui staging, data diproses pada tahap **Columnar Processing & Format Layer**. Pada tahap ini dilakukan:

* parsing data,
* normalisasi,
* pembersihan data,
* dan konversi format data.

Penelitian ini menggunakan dua variasi format data sebagai bagian dari eksperimen, yaitu:

* **CSV** sebagai representasi format row-based,
* **Parquet** sebagai representasi format columnar.

Konversi dari CSV ke Parquet dilakukan untuk mendukung optimasi query dan efisiensi I/O pada Apache Spark, khususnya dalam implementasi Adaptive Query Execution.

---

# 3. Bronze Layer (Raw Ingestion Layer)

Data hasil preprocessing kemudian disimpan pada **Bronze Layer** sebagai lapisan penyimpanan data mentah (*raw ingestion layer*).

Karakteristik Bronze Layer meliputi:

* penyimpanan data secara append-only,
* data masih bersifat mentah,
* belum dilakukan optimasi analitik,
* serta mempertahankan data historis secara lengkap.

Pada layer ini data umumnya disimpan dalam format Parquet terpartisi berdasarkan waktu ingest (*partitioned by ingestion time*) untuk mendukung efisiensi akses data.

Bronze Layer berfungsi sebagai fondasi utama pipeline data lakehouse sebelum dilakukan transformasi dan optimasi lebih lanjut.

---

# 4. Spark SQL Engine dan Skenario Eksperimen AQE

Data pada Bronze Layer diproses menggunakan **Apache Spark SQL Engine** dengan dua skenario eksperimen utama:

## 4.1 Scenario A — AQE OFF (Baseline)

Pada skenario pertama, fitur Adaptive Query Execution dinonaktifkan.

Karakteristik skenario ini:

* query plan bersifat statis,
* jumlah shuffle partition tetap,
* optimasi runtime tidak dilakukan,
* seluruh eksekusi mengikuti logical dan physical plan awal.

Skenario ini digunakan sebagai baseline untuk membandingkan performa terhadap konfigurasi AQE aktif.

---

## 4.2 Scenario B — AQE ON

Pada skenario kedua, fitur Adaptive Query Execution diaktifkan menggunakan konfigurasi:

```text
spark.sql.adaptive.enabled=true
```

Pada kondisi ini Spark akan melakukan:

* pengumpulan runtime statistics,
* evaluasi ulang query plan saat eksekusi,
* serta optimasi adaptif berdasarkan kondisi aktual data.

AQE memungkinkan Spark melakukan re-optimasi terhadap query yang sedang berjalan untuk meningkatkan efisiensi pemrosesan data.

---

# 5. Runtime Statistics Flow dan Adaptive Optimization

Saat query dijalankan, Spark mengumpulkan berbagai informasi runtime seperti:

* ukuran shuffle partition,
* distribusi join key,
* cardinality data,
* jumlah records,
* dan tingkat skew partisi.

Informasi tersebut dikirim ke komponen:

```text
Adaptive Optimizer
```

yang bertugas melakukan re-optimization terhadap physical execution plan secara dinamis.

Hasil optimasi runtime ini menghasilkan:

* pengurangan overhead shuffle,
* distribusi task yang lebih seimbang,
* pengurangan partisi kosong,
* dan peningkatan paralelisme query.

---

# 6. AQE Optimization Components

Pada skenario AQE ON, beberapa komponen optimasi utama diterapkan, yaitu:

---

## 6.1 Dynamic Partition Pruning (DPP)

Dynamic Partition Pruning bekerja dengan menghapus partisi yang tidak relevan saat runtime berdasarkan kondisi filter dan join.

Tujuan utama DPP:

* mengurangi pembacaan data yang tidak diperlukan,
* menurunkan I/O,
* mempercepat proses scanning data.

Efektivitas DPP diukur menggunakan:

* jumlah partisi sebelum dan sesudah pruning,
* serta persentase reduction.

---

## 6.2 Shuffle Partition Coalescing

Komponen ini berfungsi menggabungkan partisi-partisi kecil hasil shuffle menjadi partisi yang lebih optimal.

Tujuan utama:

* mengurangi jumlah task berukuran kecil,
* menurunkan scheduling overhead,
* meningkatkan efisiensi resource utilization.

Pengukuran dilakukan terhadap:

* jumlah partisi,
* ukuran rata-rata partisi,
* dan coalescing ratio.

---

## 6.3 Skew Join Optimization

Skew Join Optimization mendeteksi partisi dengan ukuran tidak seimbang (*data skew*) pada proses join.

Jika ditemukan skew, Spark akan:

* membagi partisi besar menjadi beberapa sub-partition,
* mendistribusikan ulang task,
* serta meningkatkan keseimbangan workload antar executor.

Efektivitas optimasi ini diukur menggunakan:

* distribusi ukuran partisi,
* coefficient of variation,
* dan Gini coefficient.

---

# 7. Silver Layer (Processing & Optimization Layer)

Hasil transformasi dan optimasi AQE kemudian disimpan pada **Silver Layer**.

Silver Layer merupakan lapisan utama pemrosesan data yang berisi:

* data yang telah dibersihkan,
* dinormalisasi,
* dan dioptimalkan untuk kebutuhan analitik.

Pada layer ini biasanya dilakukan:

* filtering,
* aggregation,
* join antar dataset,
* dan enrichment data.

Silver Layer menjadi area yang paling signifikan menerima dampak optimasi AQE karena sebagian besar proses query kompleks terjadi pada tahap ini.

---

# 8. Query Workload Layer (Experimental Workload)

Untuk menguji efektivitas AQE, penelitian menggunakan beberapa skenario workload query, yaitu:

## Join Workload

Mengukur performa query join berskala besar.

## Aggregation Workload

Mengukur proses group by dan agregasi data.

## Filtering Workload

Mengukur efektivitas filtering dan partition pruning.

Setiap workload dijalankan pada dua kondisi:

* AQE OFF,
* AQE ON,

untuk memperoleh perbandingan performa secara kuantitatif.

---

# 9. Gold Layer (Consumption Layer)

Data hasil optimasi dari Silver Layer kemudian dimuat ke **Gold Layer** sebagai lapisan konsumsi analitik.

Pada tahap ini dilakukan:

* pembentukan model analitik,
* agregasi bisnis,
* pembuatan fact table dan dimension table,
* serta penyusunan curated dataset.

Gold Layer dirancang untuk mendukung kebutuhan:

* business intelligence,
* dashboard analitik,
* dan query reporting.

Karena data telah melalui optimasi AQE pada layer sebelumnya, query pada Gold Layer menjadi lebih efisien dan responsif.

---

# 10. Dashboard dan Business Intelligence

Data pada Gold Layer diakses melalui:

* Query Engine,
* BI tools,
* dan Dashboard Visualization.

Dashboard digunakan untuk:

* monitoring KPI,
* visualisasi hasil analitik,
* serta penyajian insight bisnis.

Selain sebagai media konsumsi data, dashboard juga digunakan untuk menampilkan hasil evaluasi eksperimen AQE.

---

# 11. Metrics & Monitoring Layer

Seluruh proses pipeline dipantau melalui **Metrics & Monitoring Layer**.

Monitoring dilakukan menggunakan:

* Spark Event Log,
* Spark UI,
* cluster metrics,
* executor metrics,
* dan system metrics.

Metrik yang dikumpulkan meliputi:

## Runtime Metrics

* execution time,
* throughput,
* speedup.

## Shuffle Metrics

* shuffle read/write,
* spill,
* jumlah records.

## Partition Metrics

* ukuran partisi,
* distribusi partisi,
* skew distribution.

## Resource Metrics

* CPU utilization,
* memory utilization,
* disk I/O,
* network I/O.

## AQE Effectiveness Metrics

* DPP reduction,
* coalescing ratio,
* skew reduction.

---

# 12. Evaluasi dan Analisis Hasil Eksperimen

Seluruh metrik hasil eksperimen kemudian dianalisis untuk membandingkan performa antara:

```text
AQE OFF vs AQE ON
```

Analisis dilakukan berdasarkan:

* runtime query,
* distribusi partisi,
* efektivitas optimasi AQE,
* penggunaan resource,
* serta pengaruh format data terhadap performa pipeline.

Hasil evaluasi ini digunakan untuk menentukan sejauh mana Adaptive Query Execution mampu meningkatkan efisiensi dan performa pipeline Data Lakehouse pada lingkungan big data terdistribusi.
