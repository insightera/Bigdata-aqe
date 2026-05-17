#!/usr/bin/env python3
"""
Bronze Data Generator — ITERA Metadata Lakehouse
==================================================
Membuat data sintetis skala big-data untuk 12 tabel Bronze (Station 1).
Mendukung mode:
  --mode full     : generate semua data dari awal (overwrite)
  --mode append   : tambahkan batch baru ke CSV yang sudah ada (incremental)

Contoh:
  python generate_bronze_data.py --mode full --scale 1.0
  python generate_bronze_data.py --mode append --batch-size 500

Scale 1.0 menghasilkan ~100 ribu baris total. Naikkan ke 5.0 atau 10.0
untuk stress-test pipeline.
"""

import argparse
import csv
import os
import random
import string
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Reference / Master Data  (ITERA-specific)
# ---------------------------------------------------------------------------

JURUSAN = {
    "JTK": "Teknik dan Komputer",
    "JSA": "Sains",
    "JTI": "Teknologi Infrastruktur dan Kewilayahan",
    "JTP": "Teknologi Produksi dan Industri",
    "JMB": "Matematika dan Bisnis",
}

PRODI_MASTER: list[dict] = [
    {"prodi_id": "IF", "nama_prodi": "Informatika", "jenjang": "S1", "jurusan_id": "JTK", "tahun_berdiri": 2014},
    {"prodi_id": "TI", "nama_prodi": "Teknik Informatika", "jenjang": "S1", "jurusan_id": "JTK", "tahun_berdiri": 2017},
    {"prodi_id": "SI", "nama_prodi": "Sistem Informasi", "jenjang": "S1", "jurusan_id": "JTK", "tahun_berdiri": 2018},
    {"prodi_id": "TE", "nama_prodi": "Teknik Elektro", "jenjang": "S1", "jurusan_id": "JTK", "tahun_berdiri": 2014},
    {"prodi_id": "TL", "nama_prodi": "Teknik Telekomunikasi", "jenjang": "S1", "jurusan_id": "JTK", "tahun_berdiri": 2020},
    {"prodi_id": "BIO", "nama_prodi": "Biologi", "jenjang": "S1", "jurusan_id": "JSA", "tahun_berdiri": 2017},
    {"prodi_id": "KIM", "nama_prodi": "Kimia", "jenjang": "S1", "jurusan_id": "JSA", "tahun_berdiri": 2017},
    {"prodi_id": "FIS", "nama_prodi": "Fisika", "jenjang": "S1", "jurusan_id": "JSA", "tahun_berdiri": 2014},
    {"prodi_id": "FT", "nama_prodi": "Farmasi", "jenjang": "S1", "jurusan_id": "JSA", "tahun_berdiri": 2019},
    {"prodi_id": "SK", "nama_prodi": "Sains Komunikasi", "jenjang": "S1", "jurusan_id": "JSA", "tahun_berdiri": 2021},
    {"prodi_id": "TK", "nama_prodi": "Teknik Kimia", "jenjang": "S1", "jurusan_id": "JTP", "tahun_berdiri": 2019},
    {"prodi_id": "AG", "nama_prodi": "Teknik Pertanian", "jenjang": "S1", "jurusan_id": "JTP", "tahun_berdiri": 2017},
    {"prodi_id": "TP", "nama_prodi": "Teknologi Pangan", "jenjang": "S1", "jurusan_id": "JTP", "tahun_berdiri": 2019},
    {"prodi_id": "TM", "nama_prodi": "Teknik Mesin", "jenjang": "S1", "jurusan_id": "JTP", "tahun_berdiri": 2014},
    {"prodi_id": "TS", "nama_prodi": "Teknik Sipil", "jenjang": "S1", "jurusan_id": "JTI", "tahun_berdiri": 2014},
    {"prodi_id": "AR", "nama_prodi": "Arsitektur", "jenjang": "S1", "jurusan_id": "JTI", "tahun_berdiri": 2017},
    {"prodi_id": "PWK", "nama_prodi": "Perencanaan Wilayah dan Kota", "jenjang": "S1", "jurusan_id": "JTI", "tahun_berdiri": 2014},
    {"prodi_id": "GL", "nama_prodi": "Teknik Geologi", "jenjang": "S1", "jurusan_id": "JTI", "tahun_berdiri": 2017},
    {"prodi_id": "GD", "nama_prodi": "Teknik Geodesi", "jenjang": "S1", "jurusan_id": "JTI", "tahun_berdiri": 2019},
    {"prodi_id": "TG", "nama_prodi": "Teknik Geofisika", "jenjang": "S1", "jurusan_id": "JTI", "tahun_berdiri": 2018},
    {"prodi_id": "TLK", "nama_prodi": "Teknik Lingkungan", "jenjang": "S1", "jurusan_id": "JTI", "tahun_berdiri": 2019},
    {"prodi_id": "MTK", "nama_prodi": "Matematika", "jenjang": "S1", "jurusan_id": "JMB", "tahun_berdiri": 2017},
    {"prodi_id": "MB", "nama_prodi": "Manajemen Bisnis", "jenjang": "S1", "jurusan_id": "JMB", "tahun_berdiri": 2019},
    {"prodi_id": "TIF_S2", "nama_prodi": "Teknik Informatika", "jenjang": "S2", "jurusan_id": "JTK", "tahun_berdiri": 2022},
    {"prodi_id": "TS_S2", "nama_prodi": "Teknik Sipil", "jenjang": "S2", "jurusan_id": "JTI", "tahun_berdiri": 2021},
]

PRODI_IDS = [p["prodi_id"] for p in PRODI_MASTER]
PRODI_S1_IDS = [p["prodi_id"] for p in PRODI_MASTER if p["jenjang"] == "S1"]
JURUSAN_IDS = list(JURUSAN.keys())
PROVINSI = [
    "Lampung", "DKI Jakarta", "Jawa Barat", "Jawa Tengah", "Jawa Timur",
    "Banten", "Sumatera Selatan", "Sumatera Utara", "Sumatera Barat",
    "Bengkulu", "Riau", "Jambi", "Aceh", "Bali", "NTB", "NTT",
    "Kalimantan Barat", "Kalimantan Timur", "Sulawesi Selatan", "Papua",
    "Yogyakarta", "Kalimantan Selatan", "Sulawesi Utara", "Kepulauan Riau",
    "Bangka Belitung", "Maluku",
]
JALUR_MASUK = ["SNBP", "SNBT", "Mandiri"]
STATUS_AKTIF = ["Aktif", "Aktif", "Aktif", "Aktif", "Cuti", "DO"]
JENIS_KELAMIN = ["L", "P"]

JABATAN_FUNGSIONAL = [
    "Tenaga Pengajar", "Asisten Ahli", "Lektor",
    "Lektor Kepala", "Guru Besar",
]
STATUS_ASN = ["PNS", "PPPK", "Non-ASN"]
PENDIDIKAN = ["S2", "S3"]

SKEMA_PENELITIAN = ["Mandiri", "Dana ITERA", "Hibah Dikti", "Industri"]
TOPIK_PENELITIAN = [
    "Sustainable Energy", "Innovative Industry",
    "Green Infrastructure", "Community Development",
]
STATUS_PUBLIKASI = ["Belum", "Jurnal Nasional", "Jurnal Internasional"]

JENIS_MBKM = [
    "Magang", "KKN Tematik", "Riset", "Pertukaran",
    "Wirausaha", "Proyek Kemanusiaan",
]
JENIS_KERJASAMA = ["MoU", "PKS"]
KATEGORI_MITRA = ["PT_DN", "PT_LN", "Industri", "Pemerintah", "Litbang"]
LINGKUP = ["Pendidikan", "Penelitian", "PkM", "MBKM", "Semua"]
LEMBAGA_AKREDITASI = ["BAN-PT", "LAM", "Internasional"]
PREDIKAT_AKREDITASI = ["Baik", "Baik Sekali", "Unggul", "A", "B", "Internasional"]
SUMBER_DANA = ["PNBP", "APBN", "SBSN", "Hibah"]
KOMPONEN_ANGGARAN = ["Gaji", "Operasional", "Penelitian", "Investasi", "PkM"]

BIDANG_PRESTASI = ["Riset", "Olahraga", "Seni", "Teknologi", "Debat", "Desain"]
TINGKAT_PRESTASI = ["Institusi", "Regional", "Nasional", "Internasional"]
PERINGKAT = ["Juara 1", "Juara 2", "Juara 3", "Finalis"]
STATUS_PASCA_LULUS = ["Bekerja", "Studi Lanjut", "Wirausaha", "Belum Bekerja"]

NAMA_DEPAN = [
    "Ahmad", "Budi", "Citra", "Dewi", "Eka", "Fajar", "Gita", "Hadi",
    "Indra", "Joni", "Kartika", "Lestari", "Muhammad", "Nadia", "Omar",
    "Putri", "Qori", "Rina", "Sari", "Taufik", "Umar", "Vina", "Wahyu",
    "Yusuf", "Zahra", "Andi", "Bayu", "Dian", "Fitri", "Galih",
    "Hendra", "Irma", "Joko", "Kiki", "Lia", "Mira", "Novi", "Okta",
    "Prima", "Rizki", "Sinta", "Tina", "Ulfa", "Wati", "Yani", "Zaki",
    "Arif", "Bella", "Cahya", "Dinda", "Elsa", "Fandi", "Gilang",
    "Hana", "Ilham", "Jihan", "Kevin", "Luna", "Maulana", "Nisa",
]
NAMA_BELAKANG = [
    "Pratama", "Saputra", "Wijaya", "Kusuma", "Hidayat", "Nugroho",
    "Permana", "Santoso", "Wibowo", "Ramadhani", "Setiawan", "Utami",
    "Purnama", "Suherman", "Gunawan", "Wulandari", "Handayani",
    "Susanti", "Fitriani", "Maharani", "Laksono", "Prasetyo", "Surya",
    "Mahendra", "Hakim", "Fauzi", "Ramadhan", "Aditya", "Putra",
    "Anggraini", "Safitri", "Damayanti", "Suryadi", "Firmansyah",
]

INSTITUSI_MITRA = [
    "Universitas Indonesia", "ITB", "UGM", "ITS", "Unila", "Unsri",
    "Universitas Lampung", "Politeknik Negeri Lampung", "IPB University",
    "Telkom University", "PT PLN", "PT Pertamina", "PT Bukit Asam",
    "PT Telkom Indonesia", "Bank Indonesia", "PT Astra International",
    "Huawei Indonesia", "Google Indonesia", "Toyota Motor Manufacturing",
    "BPS Lampung", "BMKG", "LIPI", "BRIN", "UNDP Indonesia",
    "World Bank Jakarta", "JICA Indonesia", "NUS Singapore",
    "Universiti Malaya", "Chulalongkorn University",
    "Kyushu University", "Osaka University", "TU Delft",
]

KOTA_LAMPUNG = [
    "Bandar Lampung", "Metro", "Pringsewu", "Pesawaran",
    "Lampung Selatan", "Lampung Tengah", "Lampung Utara",
    "Tanggamus", "Way Kanan", "Lampung Timur", "Tulang Bawang",
    "Mesuji", "Pesisir Barat", "Lampung Barat",
]

KOMPETISI = [
    "Gemastik", "KMIPN", "PKM Dikti", "LKTIN Nasional", "Pilmapres",
    "ONMIPA", "Smart City Competition", "IoT Hackathon", "Data Science Challenge",
    "Imagine Cup", "Shell Eco-Marathon", "ACM ICPC Regional",
    "Kontes Robot Indonesia", "Electric Vehicle Competition",
    "National Bridge Competition", "Geospatial Hackathon",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rand_name() -> str:
    return f"{random.choice(NAMA_DEPAN)} {random.choice(NAMA_BELAKANG)}"


def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))


def _now_ts() -> str:
    return datetime.now(tz=None).astimezone().isoformat(timespec="seconds")


def _uid(prefix: str = "") -> str:
    short = uuid.uuid4().hex[:8]
    return f"{prefix}{short}" if prefix else short


def _write_csv(filepath: Path, rows: list[dict], append: bool = False):
    """Write or append rows to a CSV file."""
    if not rows:
        return
    mode = "a" if append and filepath.exists() else "w"
    write_header = mode == "w" or not filepath.exists()
    with open(filepath, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    print(f"  {'Appended' if append else 'Wrote'} {len(rows):>7,} rows → {filepath.name}")

# ---------------------------------------------------------------------------
# Generators (setiap fungsi mengembalikan list[dict])
# ---------------------------------------------------------------------------

def gen_prodi(scale: float) -> list[dict]:
    ts = _now_ts()
    return [
        {**p, "status": "Aktif", "ingested_at": ts}
        for p in PRODI_MASTER
    ]


def gen_mahasiswa(n: int, angkatan_range: tuple[int, int]) -> list[dict]:
    ts = _now_ts()
    rows = []
    for i in range(n):
        angkatan = random.randint(*angkatan_range)
        prodi = random.choice(PRODI_S1_IDS)
        jurusan = next(p["jurusan_id"] for p in PRODI_MASTER if p["prodi_id"] == prodi)
        sks_total = random.randint(0, 144) if angkatan < 2024 else random.randint(0, 48)
        rows.append({
            "mahasiswa_id": f"{angkatan}{prodi}{i:05d}",
            "nama": _rand_name(),
            "prodi_id": prodi,
            "jurusan_id": jurusan,
            "angkatan": angkatan,
            "jalur_masuk": random.choice(JALUR_MASUK),
            "jenis_kelamin": random.choice(JENIS_KELAMIN),
            "asal_provinsi": random.choices(PROVINSI, weights=[30]+[3]*(len(PROVINSI)-1), k=1)[0],
            "status_aktif": random.choice(STATUS_AKTIF),
            "ipk_terakhir": round(random.uniform(2.0, 4.0), 2),
            "total_sks": sks_total,
            "sks_luar_kampus": random.choices([0, 0, 0, 6, 12, 20, 22, 24], k=1)[0],
            "tanggal_masuk": f"{angkatan}-08-{random.randint(1,28):02d}",
            "ingested_at": ts,
        })
    return rows


def gen_lulusan(mahasiswa_rows: list[dict], pct: float = 0.35) -> list[dict]:
    ts = _now_ts()
    eligible = [m for m in mahasiswa_rows if m["angkatan"] <= 2023 and m["status_aktif"] == "Aktif"]
    sample = random.sample(eligible, k=min(int(len(eligible) * pct), len(eligible)))
    rows = []
    for m in sample:
        lama = random.randint(42, 72)
        rows.append({
            "lulusan_id": _uid("LLS"),
            "mahasiswa_id": m["mahasiswa_id"],
            "prodi_id": m["prodi_id"],
            "tanggal_lulus": str(_rand_date(date(m["angkatan"]+4, 1, 1), date(min(m["angkatan"]+6, 2025), 12, 28))),
            "ipk": round(random.uniform(2.75, 3.95), 2),
            "lama_studi_bulan": lama,
            "status_pasca_lulus": random.choices(STATUS_PASCA_LULUS, weights=[50, 15, 10, 25], k=1)[0],
            "nama_perusahaan": random.choice(INSTITUSI_MITRA) if random.random() > 0.3 else "",
            "bidang_kerja": random.choice(["Sesuai", "Tidak Sesuai"]),
            "masa_tunggu_bulan": random.randint(0, 18),
            "sumber_data": random.choice(["Tracer Study", "Laporan Wisuda"]),
            "ingested_at": ts,
        })
    return rows


def gen_dosen(n: int) -> list[dict]:
    ts = _now_ts()
    rows = []
    for i in range(n):
        prodi = random.choice(PRODI_IDS)
        jurusan = next(p["jurusan_id"] for p in PRODI_MASTER if p["prodi_id"] == prodi)
        pend = random.choices(PENDIDIKAN, weights=[60, 40], k=1)[0]
        rows.append({
            "dosen_id": f"0{random.randint(100000, 999999)}{i:03d}",
            "nama": _rand_name(),
            "prodi_id": prodi,
            "jurusan_id": jurusan,
            "jenis_kelamin": random.choice(JENIS_KELAMIN),
            "status_asn": random.choices(STATUS_ASN, weights=[40, 20, 40], k=1)[0],
            "pendidikan_terakhir": pend,
            "jabatan_fungsional": random.choice(JABATAN_FUNGSIONAL),
            "sedang_tugas_belajar": random.random() < 0.08,
            "sertifikat_dosen": random.random() < 0.55,
            "berasal_praktisi": random.random() < 0.15,
            "tahun_bergabung": random.randint(2014, 2025),
            "ingested_at": ts,
        })
    return rows


def gen_kegiatan_dosen(dosen_rows: list[dict], avg_per_dosen: float = 3.0) -> list[dict]:
    ts = _now_ts()
    jenis_list = ["Tridarma_PT_Lain", "Praktisi_Industri", "Pembina_Prestasi", "QS100"]
    rows = []
    for d in dosen_rows:
        k = max(0, int(random.gauss(avg_per_dosen, 1.5)))
        for _ in range(k):
            tahun = random.randint(2020, 2025)
            mulai = _rand_date(date(tahun, 1, 1), date(tahun, 6, 30))
            rows.append({
                "kegiatan_id": _uid("KGT"),
                "dosen_id": d["dosen_id"],
                "jenis_kegiatan": random.choice(jenis_list),
                "nama_institusi": random.choice(INSTITUSI_MITRA),
                "tanggal_mulai": str(mulai),
                "tanggal_selesai": str(mulai + timedelta(days=random.randint(30, 365))),
                "tahun": tahun,
                "ingested_at": ts,
            })
    return rows


def gen_penelitian(dosen_rows: list[dict], avg: float = 2.5) -> list[dict]:
    ts = _now_ts()
    rows = []
    for d in dosen_rows:
        k = max(0, int(random.gauss(avg, 1.2)))
        for _ in range(k):
            rows.append({
                "penelitian_id": _uid("PNL"),
                "judul": f"Penelitian {random.choice(TOPIK_PENELITIAN)} - {_uid()}",
                "dosen_id": d["dosen_id"],
                "jurusan_id": d["jurusan_id"],
                "tahun": random.randint(2020, 2025),
                "skema": random.choice(SKEMA_PENELITIAN),
                "dana": random.choice([5_000_000, 10_000_000, 25_000_000, 50_000_000, 100_000_000, 250_000_000]),
                "topik": random.choice(TOPIK_PENELITIAN),
                "status_publikasi": random.choices(STATUS_PUBLIKASI, weights=[30, 45, 25], k=1)[0],
                "rekognisi_internasional": random.random() < 0.12,
                "diterapkan_masyarakat": random.random() < 0.20,
                "ingested_at": ts,
            })
    return rows


def gen_pengabdian(dosen_rows: list[dict], avg: float = 1.5) -> list[dict]:
    ts = _now_ts()
    rows = []
    for d in dosen_rows:
        k = max(0, int(random.gauss(avg, 1.0)))
        for _ in range(k):
            rows.append({
                "pkm_id": _uid("PKM"),
                "judul": f"Pengabdian {random.choice(TOPIK_PENELITIAN)} - {_uid()}",
                "dosen_id": d["dosen_id"],
                "jurusan_id": d["jurusan_id"],
                "tahun": random.randint(2020, 2025),
                "dana": random.choice([3_000_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000]),
                "lokasi": random.choice(KOTA_LAMPUNG),
                "rekognisi_internasional": random.random() < 0.05,
                "diterapkan_masyarakat": random.random() < 0.45,
                "ingested_at": ts,
            })
    return rows


def gen_kerjasama(n: int) -> list[dict]:
    ts = _now_ts()
    rows = []
    for _ in range(n):
        tahun = random.randint(2019, 2025)
        mulai = _rand_date(date(tahun, 1, 1), date(tahun, 12, 28))
        berakhir = mulai + timedelta(days=random.choice([365, 730, 1095, 1825]))
        rows.append({
            "kerjasama_id": _uid("KJS"),
            "jenis": random.choice(JENIS_KERJASAMA),
            "mitra": random.choice(INSTITUSI_MITRA),
            "kategori_mitra": random.choice(KATEGORI_MITRA),
            "lingkup": random.choice(LINGKUP),
            "prodi_id": random.choice(PRODI_S1_IDS + [""]),
            "tanggal_mulai": str(mulai),
            "tanggal_berakhir": str(berakhir),
            "status": "Aktif" if berakhir >= date.today() else "Tidak Aktif",
            "tahun": tahun,
            "ingested_at": ts,
        })
    return rows


def gen_mbkm(mahasiswa_rows: list[dict], pct: float = 0.25) -> list[dict]:
    ts = _now_ts()
    eligible = [m for m in mahasiswa_rows if m["angkatan"] >= 2020 and m["angkatan"] < 2025]
    sample = random.sample(eligible, k=min(int(len(eligible) * pct), len(eligible)))
    rows = []
    for m in sample:
        tahun = random.randint(max(m["angkatan"] + 1, 2021), 2025)
        rows.append({
            "mbkm_id": _uid("MBKM"),
            "mahasiswa_id": m["mahasiswa_id"],
            "prodi_id": m["prodi_id"],
            "jenis_mbkm": random.choice(JENIS_MBKM),
            "institusi_mitra": random.choice(INSTITUSI_MITRA),
            "sks_diakui": random.choice([6, 12, 20, 20, 22, 24]),
            "tahun": tahun,
            "semester": random.choice(["Ganjil", "Genap"]),
            "prestasi_nasional": random.random() < 0.08,
            "ingested_at": ts,
        })
    return rows


def gen_akreditasi() -> list[dict]:
    ts = _now_ts()
    rows = []
    for p in PRODI_MASTER:
        n_akred = random.randint(1, 3)
        for i in range(n_akred):
            tahun = p["tahun_berdiri"] + 2 + i * 3
            if tahun > 2025:
                continue
            tgl_sk = _rand_date(date(tahun, 1, 1), date(tahun, 12, 28))
            rows.append({
                "akreditasi_id": _uid("AKR"),
                "prodi_id": p["prodi_id"],
                "lembaga": random.choices(LEMBAGA_AKREDITASI, weights=[70, 20, 10], k=1)[0],
                "nama_lembaga_detail": random.choice(["BAN-PT", "LAM-Teknik", "LAM-Sains", "ABET", "ASIIN"]),
                "predikat": random.choices(PREDIKAT_AKREDITASI, weights=[15, 25, 30, 10, 15, 5], k=1)[0],
                "tanggal_sk": str(tgl_sk),
                "tanggal_berakhir": str(tgl_sk + timedelta(days=1825)),
                "tahun": tahun,
                "ingested_at": ts,
            })
    return rows


def gen_keuangan(tahun_range: tuple[int, int]) -> list[dict]:
    ts = _now_ts()
    rows = []
    for tahun in range(tahun_range[0], tahun_range[1] + 1):
        for triwulan in range(1, 5):
            for sumber in SUMBER_DANA:
                for komponen in KOMPONEN_ANGGARAN:
                    pagu = random.randint(500_000_000, 50_000_000_000)
                    realisasi = int(pagu * random.uniform(0.4, 0.98))
                    rows.append({
                        "anggaran_id": _uid("ANG"),
                        "tahun": tahun,
                        "sumber_dana": sumber,
                        "komponen": komponen,
                        "pagu": pagu,
                        "realisasi": realisasi,
                        "triwulan": triwulan,
                        "ingested_at": ts,
                    })
    return rows


def gen_prestasi(mahasiswa_rows: list[dict], dosen_rows: list[dict], n: int) -> list[dict]:
    ts = _now_ts()
    dosen_ids = [d["dosen_id"] for d in dosen_rows]
    rows = []
    for _ in range(n):
        m = random.choice(mahasiswa_rows)
        rows.append({
            "prestasi_id": _uid("PRS"),
            "mahasiswa_id": m["mahasiswa_id"],
            "nama_kompetisi": random.choice(KOMPETISI),
            "bidang": random.choice(BIDANG_PRESTASI),
            "tingkat": random.choices(TINGKAT_PRESTASI, weights=[30, 25, 30, 15], k=1)[0],
            "peringkat": random.choice(PERINGKAT),
            "tahun": random.randint(2020, 2025),
            "dosen_pembina_id": random.choice(dosen_ids) if random.random() > 0.2 else "",
            "ingested_at": ts,
        })
    return rows

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ITERA Bronze Data Generator")
    parser.add_argument(
        "--mode", choices=["full", "append"], default="full",
        help="full = overwrite semua CSV; append = tambah batch baru",
    )
    parser.add_argument(
        "--scale", type=float, default=1.0,
        help="Multiplier jumlah baris (1.0 ≈ 100k total, 5.0 ≈ 500k)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=500,
        help="Jumlah mahasiswa baru per batch (mode append)",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Direktori output (default: data/bronze/ relatif ke script)",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed untuk reprodusibilitas",
    )
    args = parser.parse_args()

    random.seed(args.seed if args.mode == "full" else None)

    if args.output_dir:
        out = Path(args.output_dir)
    else:
        out = Path(__file__).resolve().parent.parent / "data" / "staging"
    out.mkdir(parents=True, exist_ok=True)

    append = args.mode == "append"
    s = args.scale

    print(f"\n{'='*60}")
    print(f"  ITERA Bronze Data Generator — mode={args.mode} scale={s}")
    print(f"  Output: {out}")
    print(f"{'='*60}\n")

    # --- raw_prodi (master, selalu overwrite) ---
    prodi_rows = gen_prodi(s)
    _write_csv(out / "raw_prodi.csv", prodi_rows, append=False)

    if args.mode == "full":
        n_mhs = int(50_000 * s)
        n_dosen = int(800 * s)
        n_kerjasama = int(400 * s)
        n_prestasi = int(5_000 * s)
        angkatan = (2018, 2025)
        tahun_keuangan = (2020, 2025)
    else:
        n_mhs = args.batch_size
        n_dosen = max(10, args.batch_size // 50)
        n_kerjasama = max(5, args.batch_size // 100)
        n_prestasi = max(20, args.batch_size // 25)
        angkatan = (2024, 2025)
        tahun_keuangan = (2025, 2025)

    # --- raw_mahasiswa ---
    mhs = gen_mahasiswa(n_mhs, angkatan)
    _write_csv(out / "raw_mahasiswa.csv", mhs, append=append)

    # --- raw_lulusan ---
    lulusan = gen_lulusan(mhs, pct=0.35)
    _write_csv(out / "raw_lulusan.csv", lulusan, append=append)

    # --- raw_dosen ---
    dosen = gen_dosen(n_dosen)
    _write_csv(out / "raw_dosen.csv", dosen, append=append)

    # --- raw_kegiatan_dosen ---
    kegiatan = gen_kegiatan_dosen(dosen, avg_per_dosen=3.0)
    _write_csv(out / "raw_kegiatan_dosen.csv", kegiatan, append=append)

    # --- raw_penelitian ---
    penelitian = gen_penelitian(dosen, avg=2.5)
    _write_csv(out / "raw_penelitian.csv", penelitian, append=append)

    # --- raw_pengabdian ---
    pengabdian = gen_pengabdian(dosen, avg=1.5)
    _write_csv(out / "raw_pengabdian.csv", pengabdian, append=append)

    # --- raw_kerjasama ---
    kerjasama = gen_kerjasama(n_kerjasama)
    _write_csv(out / "raw_kerjasama.csv", kerjasama, append=append)

    # --- raw_mbkm ---
    mbkm = gen_mbkm(mhs, pct=0.25)
    _write_csv(out / "raw_mbkm.csv", mbkm, append=append)

    # --- raw_akreditasi ---
    akreditasi = gen_akreditasi()
    _write_csv(out / "raw_akreditasi.csv", akreditasi, append=append)

    # --- raw_keuangan ---
    keuangan = gen_keuangan(tahun_keuangan)
    _write_csv(out / "raw_keuangan.csv", keuangan, append=append)

    # --- raw_prestasi_mahasiswa ---
    prestasi = gen_prestasi(mhs, dosen, n_prestasi)
    _write_csv(out / "raw_prestasi_mahasiswa.csv", prestasi, append=append)

    total = (
        len(prodi_rows) + len(mhs) + len(lulusan) + len(dosen) +
        len(kegiatan) + len(penelitian) + len(pengabdian) +
        len(kerjasama) + len(mbkm) + len(akreditasi) +
        len(keuangan) + len(prestasi)
    )
    print(f"\n✅ Total baris dihasilkan: {total:,}")
    print(f"   File CSV di: {out}/\n")


if __name__ == "__main__":
    main()
