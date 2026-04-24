"""
CMS NADAC (National Average Drug Acquisition Cost) via Medicaid open data.

NADAC is the published average pharmacy acquisition cost used as a public benchmark;
it is not Medicare plan pricing, but it is the standard open \"wholesale-like\" series
for retail generics. Dataset rotates by year; update NADAC_DATASET_UUID when CMS
publishes a new NADAC file on data.medicaid.gov.
"""
from __future__ import annotations

from typing import Any

import pandas as pd
import requests

# 2026 NADAC weekly (verify on data.medicaid.gov if this UUID stops resolving).
NADAC_DATASET_UUID = "fbb83258-11c7-47f5-8b18-5f8e79f7e704"

# Keys must match `name` in scraper.DRUGS (the value written to `Drug_Name`).
# Values: (ndc_description exact string in NADAC, assumed quantity in units for total baseline).
NADAC_DRUG_CONFIG: dict[str, tuple[str, int]] = {
    "Atorvastatin": ("ATORVASTATIN 40 MG TABLET", 30),
    "Amoxicillin": ("AMOXICILLIN 500 MG CAPSULE", 21),
    "Imatinib": ("IMATINIB MESYLATE 400 MG TAB", 30),
}


def nadac_query_url(dataset_uuid: str) -> str:
    return f"https://data.medicaid.gov/api/1/datastore/query/{dataset_uuid}/0"


def fetch_median_baseline_for_description(
    session: requests.Session,
    base_url: str,
    ndc_description: str,
    quantity: int,
) -> tuple[float, float]:
    """
    Return (median_nadac_per_unit, total_baseline_for_quantity) for one NADAC product line.
    Median is taken over rows returned (multiple NDCs / manufacturers).
    """
    params: dict[str, str | int] = {
        "conditions[0][property]": "ndc_description",
        "conditions[0][value]": ndc_description,
        "conditions[0][operator]": "=",
        "limit": 5000,
    }
    response = session.get(base_url, params=params, timeout=60)
    response.raise_for_status()
    data: dict[str, Any] = response.json()
    results = data.get("results") or []
    per_unit: list[float] = []
    for row in results:
        raw = row.get("nadac_per_unit")
        if raw is None or raw == "":
            continue
        per_unit.append(float(raw))
    if not per_unit:
        n = float("nan")
        return n, n
    median_pu = float(pd.Series(per_unit).median())
    total = median_pu * quantity
    return median_pu, float(round(total, 2)) if total == total else float("nan")


def fetch_nadac_baselines(
    drug_config: dict[str, tuple[str, int]] | None = None,
    dataset_uuid: str | None = None,
) -> dict[str, dict[str, float | int | str | None]]:
    """
    For each drug in `drug_config`, query NADAC and compute median per-unit and total baseline.

    Returns a dict keyed by drug display name, with:
      - ndc_description, quantity_units, median_per_unit, cms_baseline_total
    """
    drug_config = drug_config or NADAC_DRUG_CONFIG
    uuid = dataset_uuid or NADAC_DATASET_UUID
    url = nadac_query_url(uuid)
    out: dict[str, dict[str, float | int | str | None]] = {}
    with requests.Session() as session:
        for drug, (ndc_desc, quantity) in drug_config.items():
            med_pu, total = fetch_median_baseline_for_description(
                session, url, ndc_desc, quantity
            )
            out[drug] = {
                "ndc_description": ndc_desc,
                "quantity_units": quantity,
                "median_per_unit": med_pu,
                "cms_baseline_total": total,
            }
    return out
