#!/usr/bin/env python3
"""Ringkasan dataset staging → metrics/dataset_summary_*.json (BAB III §3.3)."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from benchmark._common import metrics_dir, utc_now, write_json

DEFAULT_STAGING = os.environ.get(
    "STAGING_DATA_DIR",
    os.path.join(os.path.dirname(__file__), "../../data/staging"),
)


def summarize_staging(staging_dir: Path, skew_prodi: str = "SD") -> dict:
    tables = []
    total_rows = 0
    total_bytes = 0
    prodi_counts: Counter = Counter()

    for csv_path in sorted(staging_dir.glob("*.csv")):
        size = csv_path.stat().st_size
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        n = len(rows)
        total_rows += n
        total_bytes += size
        tables.append({
            "file": csv_path.name,
            "row_count": n,
            "size_bytes": size,
            "size_mb": round(size / (1024**2), 2),
        })
        if csv_path.name == "raw_mahasiswa.csv" and rows and "prodi_id" in rows[0]:
            prodi_counts.update(r.get("prodi_id", "") for r in rows)

    skew_info = {}
    if prodi_counts:
        total_mhs = sum(prodi_counts.values())
        hot = prodi_counts.get(skew_prodi, 0)
        skew_info = {
            "skew_key": "prodi_id",
            "hot_key": skew_prodi,
            "hot_key_rows": hot,
            "hot_key_fraction_pct": round(hot / total_mhs * 100, 2) if total_mhs else 0,
            "top3_prodi": prodi_counts.most_common(3),
        }

    return {
        "staging_dir": str(staging_dir.resolve()),
        "generated_at": utc_now().isoformat(),
        "summary": {
            "table_count": len(tables),
            "total_rows": total_rows,
            "total_size_mb": round(total_bytes / (1024**2), 2),
        },
        "tables": tables,
        "skew": skew_info,
    }


def main():
    parser = argparse.ArgumentParser(description="Summarize staging CSV dataset")
    parser.add_argument("--staging-dir", default=DEFAULT_STAGING)
    parser.add_argument("--skew-prodi", default="SD", help="Hot key skew (default: SD / Sains Data)")
    args = parser.parse_args()

    staging = Path(args.staging_dir)
    if not staging.is_dir():
        raise SystemExit(f"Staging directory not found: {staging}")

    payload = summarize_staging(staging, skew_prodi=args.skew_prodi)
    ts = utc_now().strftime("%Y%m%d_%H%M%S")
    out = metrics_dir() / f"dataset_summary_{ts}.json"
    write_json(out, payload)
    print(f"Dataset summary → {out}")
    print(f"  Total rows: {payload['summary']['total_rows']:,}")
    print(f"  Total size: {payload['summary']['total_size_mb']} MB")


if __name__ == "__main__":
    main()
