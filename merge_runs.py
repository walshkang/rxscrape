#!/usr/bin/env python3
"""
Merge all per-run CSV snapshots under runs/ into one file with a Run_ID column
(filename stem, e.g. prices_20260423_143000) for time-series analysis.
"""
from __future__ import annotations

import argparse
import csv
import os
import re

# Keep in sync with scraper.OUTPUT_FIELDNAMES
BASE_FIELDS = ["Date", "Zip_Code", "Drug_Name", "Pharmacy_Name", "Retail_Price", "GoodRx_Price"]
OUT_FIELDS = ["Run_ID"] + BASE_FIELDS

PRICES_PATTERN = re.compile(r"^prices_\d{8}_\d{6}\.csv$")


def run_id_from_filename(name: str) -> str:
    base = name[:-4] if name.lower().endswith(".csv") else name
    return base


def discover_csvs(runs_dir: str) -> list[str]:
    if not os.path.isdir(runs_dir):
        return []
    out: list[str] = []
    for n in sorted(os.listdir(runs_dir)):
        if not PRICES_PATTERN.match(n):
            continue
        path = os.path.join(runs_dir, n)
        if os.path.isfile(path):
            out.append(path)
    return out


def merge(
    runs_dir: str,
    out_path: str,
    *,
    include_legacy: bool,
) -> tuple[int, int]:
    paths = list(discover_csvs(runs_dir))
    # Old single-file output (typical location: current working directory / repo root).
    legacy = "national_pharmacy_pricing.csv"
    if include_legacy and os.path.isfile(legacy):
        paths.append(os.path.abspath(legacy))

    file_count = len(paths)
    row_total = 0
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as outf:
        w = csv.DictWriter(outf, fieldnames=OUT_FIELDS)
        w.writeheader()

        for path in paths:
            rid = (
                "legacy_national_pharmacy_pricing"
                if os.path.basename(path) == "national_pharmacy_pricing.csv"
                else run_id_from_filename(os.path.basename(path))
            )
            with open(path, newline="", encoding="utf-8") as inf:
                r = csv.DictReader(inf)
                for row in r:
                    if not row:
                        continue
                    out_row: dict[str, str] = {"Run_ID": rid}
                    for k in BASE_FIELDS:
                        v = row.get(k) or ""
                        out_row[k] = v.strip() if isinstance(v, str) else ""
                    w.writerow(out_row)
                    row_total += 1

    return file_count, row_total


def main() -> None:
    p = argparse.ArgumentParser(
        description="Merge runs/prices_*.csv snapshots into one CSV with a Run_ID column."
    )
    p.add_argument(
        "--runs-dir",
        default="runs",
        help="Directory containing prices_YYYYMMDD_HHMMSS.csv files (default: runs)",
    )
    p.add_argument(
        "-o",
        "--output",
        default=os.path.join("runs", "merged_all_runs.csv"),
        help="Output path (default: runs/merged_all_runs.csv)",
    )
    p.add_argument(
        "--include-legacy",
        action="store_true",
        help="Also include national_pharmacy_pricing.csv in repo root, if present (Run_ID=legacy_...).",
    )
    args = p.parse_args()

    n_files, n_rows = merge(args.runs_dir, args.output, include_legacy=args.include_legacy)
    print(f"Wrote {args.output} — {n_rows} data rows from {n_files} snapshot file(s).")


if __name__ == "__main__":
    main()
