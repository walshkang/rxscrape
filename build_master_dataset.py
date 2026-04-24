#!/usr/bin/env python3
"""
Join GoodRx scrape output with CMS NADAC baselines and write one analysis-ready CSV.

Typical flow:
  python merge_runs.py
  python build_master_dataset.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import pandas as pd
import requests

from nadac_cms import (
    NADAC_DATASET_UUID,
    NADAC_DRUG_CONFIG,
    fetch_nadac_baselines,
)

ANALYSIS_COLUMNS = [
    "CMS_Baseline_Cost",
    "NADAC_Median_Per_Unit",
    "NADAC_Quantity_Units",
    "Retail_Spread",
    "GoodRx_Spread",
]


def _baseline_to_row(drug: str, b: dict[str, Any] | None) -> dict[str, Any]:
    if not b:
        return {
            "Drug_Name": drug,
            "CMS_Baseline_Cost": float("nan"),
            "NADAC_Median_Per_Unit": float("nan"),
            "NADAC_Quantity_Units": float("nan"),
        }
    q = b.get("quantity_units", b.get("NADAC_Quantity_Units"))
    if isinstance(q, str) and q.strip().isdigit():
        q = int(q)
    elif q is not None and not isinstance(q, (int, float)):
        try:
            q = int(q)
        except (TypeError, ValueError):
            q = float("nan")
    return {
        "Drug_Name": drug,
        "CMS_Baseline_Cost": b.get("cms_baseline_total", b.get("CMS_Baseline_Cost")),
        "NADAC_Median_Per_Unit": b.get("median_per_unit", b.get("NADAC_Median_Per_Unit")),
        "NADAC_Quantity_Units": q,
    }


def load_baselines_from_api(dataset_uuid: str | None) -> dict[str, dict[str, Any]]:
    raw = fetch_nadac_baselines(dataset_uuid=dataset_uuid)
    return {k: dict(v) for k, v in raw.items()}


def load_baselines_from_json(path: str) -> dict[str, dict[str, Any]]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Baselines JSON must be an object keyed by drug name.")
    return {str(k): v for k, v in data.items()}


def apply_baselines(
    df: pd.DataFrame,
    baselines: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Map baseline fields from `baselines` onto rows by `Drug_Name` and add spread columns."""
    for col in ("Retail_Price", "GoodRx_Price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Drug_Name" not in df.columns:
        raise ValueError("Input CSV must include a Drug_Name column.")

    for c in ANALYSIS_COLUMNS:
        if c in df.columns:
            df = df.drop(columns=[c])

    drug_key = df["Drug_Name"].astype(str).str.strip()
    base_rows = [
        _baseline_to_row(d, baselines.get(d))
        for d in sorted(baselines.keys(), key=str)
    ]
    b_df = pd.DataFrame(base_rows)
    if b_df.empty:
        b_df = pd.DataFrame(
            columns=["Drug_Name", "CMS_Baseline_Cost", "NADAC_Median_Per_Unit", "NADAC_Quantity_Units"]
        )

    df = df.copy()
    df["Drug_Name"] = drug_key
    df = df.merge(b_df, on="Drug_Name", how="left")

    for c in ("CMS_Baseline_Cost", "NADAC_Median_Per_Unit", "NADAC_Quantity_Units"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    base = df["CMS_Baseline_Cost"]
    df["Retail_Spread"] = df["Retail_Price"] - base
    df["GoodRx_Spread"] = df["GoodRx_Price"] - base
    return df


def merge_master(
    goodrx_csv: str,
    output_path: str,
    *,
    baselines_json: str | None,
    dataset_uuid: str | None,
) -> None:
    if not os.path.isfile(goodrx_csv):
        raise FileNotFoundError(goodrx_csv)

    if baselines_json:
        print(f"Loading NADAC baselines from {baselines_json}...")
        baselines = load_baselines_from_json(baselines_json)
    else:
        print("Fetching CMS NADAC baselines from data.medicaid.gov ...")
        baselines = load_baselines_from_api(dataset_uuid)

    print(f"Loading GoodRx data from {goodrx_csv} ...")
    df = pd.read_csv(goodrx_csv)
    df = apply_baselines(df, baselines)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Wrote {output_path} — {len(df)} rows, columns include {', '.join(ANALYSIS_COLUMNS)}.")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Merge GoodRx CSV with CMS NADAC baselines into one analysis master CSV."
    )
    p.add_argument(
        "-i",
        "--input",
        default=os.path.join("runs", "merged_all_runs.csv"),
        help="GoodRx merged CSV (default: runs/merged_all_runs.csv)",
    )
    p.add_argument(
        "-o",
        "--output",
        default="final_pricing_puzzle_dataset.csv",
        help="Output path (default: final_pricing_puzzle_dataset.csv)",
    )
    p.add_argument(
        "--baselines-json",
        metavar="FILE",
        help="Skip the API and use this JSON (keys = Drug_Name, same structure as API output).",
    )
    p.add_argument(
        "--dataset-uuid",
        default=NADAC_DATASET_UUID,
        help="NADAC datastore UUID on data.medicaid.gov (default: built-in 2026 NADAC).",
    )
    args = p.parse_args()
    try:
        merge_master(
            args.input,
            args.output,
            baselines_json=args.baselines_json,
            dataset_uuid=args.dataset_uuid if not args.baselines_json else None,
        )
    except FileNotFoundError as e:
        print(f"File not found: {e}. Run merge_runs.py (and the scraper) first.", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"NADAC API error: {e}. Retry later or use --baselines-json.", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
