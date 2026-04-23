import argparse
import csv
import os
import re
from dataclasses import dataclass
from typing import Optional


RUNS_DIR = "runs"
LEGACY_CSV = "national_pharmacy_pricing.csv"  # pre–per-run-snapshots
DEFAULT_DOM_DIR = "error_dom_artifacts"


def resolve_default_csv() -> str:
    """Prefer newest `runs/prices_*.csv`; else legacy single file if present."""
    if os.path.isdir(RUNS_DIR):
        names = [n for n in os.listdir(RUNS_DIR) if n.startswith("prices_") and n.endswith(".csv")]
        if names:
            paths = [os.path.join(RUNS_DIR, n) for n in names]
            return max(paths, key=lambda p: os.path.getmtime(p))
    if os.path.isfile(LEGACY_CSV):
        return LEGACY_CSV
    return os.path.join(RUNS_DIR, "prices_<run_id>.csv")


@dataclass
class AcceptanceResult:
    name: str
    passed: bool
    detail: str


def pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def read_csv_metrics(csv_path: str) -> tuple[int, int, int]:
    if not os.path.exists(csv_path):
        return 0, 0, 0

    total = 0
    base_complete = 0
    retail_complete = 0

    with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            pharmacy = (row.get("Pharmacy_Name") or "").strip()
            goodrx = (row.get("GoodRx_Price") or "").strip()
            retail = (row.get("Retail_Price") or "").strip()

            if pharmacy and goodrx and goodrx.upper() != "N/A":
                base_complete += 1
            if retail and retail.upper() != "N/A":
                retail_complete += 1

    return total, base_complete, retail_complete


def read_log(log_file: Optional[str]) -> str:
    if not log_file:
        return ""
    if not os.path.exists(log_file):
        return ""
    with open(log_file, mode="r", encoding="utf-8") as f:
        return f.read()


def parse_skip_rates(log_text: str) -> tuple[int, int]:
    # Matches lines like: Row parse skip rate: 8.3% (1/12)
    rates = re.findall(r"Row parse skip rate:\s*([0-9]+(?:\.[0-9]+)?)%", log_text)
    if not rates:
        return 0, 0
    below_10 = sum(1 for r in rates if float(r) < 10.0)
    return below_10, len(rates)


def parse_retry_bounds(log_text: str) -> tuple[bool, int]:
    # If attempts beyond 3 appear, bounds are violated.
    attempts = re.findall(r"zip_run_attempt_(\d+)", log_text)
    if not attempts:
        return True, 0
    max_attempt = max(int(a) for a in attempts)
    return max_attempt <= 3, max_attempt


def parse_overlay_failures(log_text: str) -> int:
    patterns = [
        r"savings-tip-row-modal",
        r"overlay[- ]intercept",
        r"intercepts pointer events",
    ]
    count = 0
    for p in patterns:
        count += len(re.findall(p, log_text, flags=re.I))
    return count


def dom_artifact_count(dom_dir: str) -> int:
    if not os.path.isdir(dom_dir):
        return 0
    return len(
        [
            name
            for name in os.listdir(dom_dir)
            if os.path.isfile(os.path.join(dom_dir, name)) and name.endswith(".html")
        ]
    )


def build_report(
    csv_path: str,
    dom_dir: str,
    log_file: Optional[str],
) -> list[AcceptanceResult]:
    total_rows, base_complete_rows, retail_complete_rows = read_csv_metrics(csv_path)
    log_text = read_log(log_file)
    skip_below_10, skip_total = parse_skip_rates(log_text)
    retry_ok, max_attempt_seen = parse_retry_bounds(log_text)
    overlay_failures = parse_overlay_failures(log_text)
    dom_count = dom_artifact_count(dom_dir)

    base_rate = pct(base_complete_rows, total_rows)
    retail_rate = pct(retail_complete_rows, total_rows)

    results = [
        AcceptanceResult(
            name="Base completion >= 95%",
            passed=total_rows > 0 and base_rate >= 95.0,
            detail=f"{base_complete_rows}/{total_rows} rows ({base_rate:.1f}%)",
        ),
        AcceptanceResult(
            name="Retail enrichment >= 70% (best effort)",
            passed=total_rows > 0 and retail_rate >= 70.0,
            detail=f"{retail_complete_rows}/{total_rows} rows ({retail_rate:.1f}%)",
        ),
    ]

    if log_text:
        results.append(
            AcceptanceResult(
                name="Row skip rate < 10% (from logs)",
                passed=skip_total > 0 and skip_below_10 == skip_total,
                detail=f"{skip_below_10}/{skip_total} reported skip-rate lines below 10%",
            )
        )
        results.append(
            AcceptanceResult(
                name="Overlay intercept failures handled (from logs)",
                passed=overlay_failures == 0,
                detail=f"{overlay_failures} overlay/intercept markers found",
            )
        )
        results.append(
            AcceptanceResult(
                name="Retry bound respected (max 3 attempts/ZIP)",
                passed=retry_ok,
                detail=f"max zip_run_attempt seen: {max_attempt_seen}",
            )
        )
    else:
        results.append(
            AcceptanceResult(
                name="Log-based checks",
                passed=False,
                detail="No --log-file provided, so skip-rate/overlay/retry checks were not evaluated.",
            )
        )

    results.append(
        AcceptanceResult(
            name="DOM artifacts captured",
            passed=dom_count > 0,
            detail=f"{dom_count} html artifacts found in {dom_dir}",
        )
    )
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate scraper acceptance targets from CSV, DOM artifacts, and optional run logs."
    )
    parser.add_argument(
        "--csv",
        default=None,
        help=f"Path to pricing CSV (default: newest in {RUNS_DIR}/, else {LEGACY_CSV} if it exists)",
    )
    parser.add_argument("--dom-dir", default=DEFAULT_DOM_DIR, help="Path to DOM artifact directory")
    parser.add_argument("--log-file", default=None, help="Optional scraper stdout log file")
    args = parser.parse_args()
    csv_path = args.csv or resolve_default_csv()
    if not os.path.isfile(csv_path):
        print(f"No CSV at {csv_path!r} — run the scraper first (outputs under {RUNS_DIR}/).")
        return

    results = build_report(csv_path, args.dom_dir, args.log_file)
    passed = 0

    print("Acceptance Metrics")
    print("==================")
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        if result.passed:
            passed += 1
        print(f"[{status}] {result.name}: {result.detail}")

    print("------------------")
    print(f"Checks passed: {passed}/{len(results)}")
    if passed == len(results):
        print("Overall: PASS")
    else:
        print("Overall: FAIL")


if __name__ == "__main__":
    main()
