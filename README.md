# GoodRx Price Scraper (rxscrape)

A Playwright-based scraper to collect generic drug pricing from GoodRx.com across multiple U.S. ZIP codes, with optional join to **CMS NADAC** (wholesale-style acquisition baselines) for a single analysis CSV.

## Features

- **PerimeterX bypass**: Bezier mouse paths, jitter, and pacing tuned to reduce CAPTCHA friction.
- **Stealth configuration**: `playwright-stealth` and a persistent browser profile to mimic real sessions.
- **Retail extraction**: Clicks pharmacy rows to surface retail prices when they are hidden behind UI affordances.
- **Run-scoped output**: Each full scrape run writes a timestamped snapshot under `runs/` (not mixed with other runs).
- **Analysis export**: `build_master_dataset.py` merges GoodRx data with [Medicaid open data](https://data.medicaid.gov) NADAC via API and adds spread columns for regression work.

## Getting Started

### Prerequisites

- Python 3.9+
- [uv](https://github.com/astral-sh/uv) (recommended) or `pip`

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/walshkang/rxscrape.git
   cd rxscrape
   ```

2. Create and activate a virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies and browser:

   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

## Usage

### Scrape GoodRx

```bash
python3 scraper.py
```

The scraper iterates the configured drugs and ZIP codes. Each run saves one file:

- `runs/prices_YYYYMMDD_HHMMSS.csv`

Columns include `Date`, `Zip_Code`, `Drug_Name`, `Pharmacy_Name`, `Option_Type`, `Retail_Price`, `Retail_Flag`, and `GoodRx_Price`. For older workflows, a legacy `national_pharmacy_pricing.csv` in the repo root can be merged in via `merge_runs.py` (see below).

Optional manual CAPTCHA fallback:

- Set `GOODRX_MANUAL_PERIMETERX=fallback` to keep automated solve first, then pause for manual solve only if PerimeterX still blocks after configured attempts.
- Set `GOODRX_MANUAL_PERIMETERX=always` to always pause for manual solve whenever a PerimeterX challenge is detected.
- Manual mode requires a visible browser window and interactive terminal input (`stdin` must be a TTY); in non-interactive runs, the scraper raises a clear error instead of hanging.

### Merge multiple runs (optional)

Combine every `runs/prices_*.csv` (and optionally `national_pharmacy_pricing.csv`) into one file with a `Run_ID` column for time-series work:

```bash
python3 merge_runs.py
# default output: runs/merged_all_runs.csv
```

### Master dataset with CMS NADAC baselines

NADAC (National Average Drug Acquisition Cost) is the published **average pharmacy acquisition** benchmark from CMS, distributed on **Medicaid** open data. It is not Medicare plan pricing, but it is a standard public series for generic retail economics.

1. Start from merged GoodRx output (or any CSV with the same column names, including `Drug_Name`):

   ```bash
   python3 merge_runs.py
   ```

2. Build the analysis file (live NADAC query plus spreads):

   ```bash
   python3 build_master_dataset.py
   ```

   Defaults: input `runs/merged_all_runs.csv`, output `final_pricing_puzzle_dataset.csv`.

   Added columns: `CMS_Baseline_Cost`, `NADAC_Median_Per_Unit`, `NADAC_Quantity_Units`, `Retail_Spread`, `GoodRx_Spread`.

Useful flags: `-i` / `--input`, `-o` / `--output`, `--baselines-json` (offline or CI, skip API), `--dataset-uuid` (when CMS publishes a new NADAC dataset). Drug names and assumed NADAC line items live in `nadac_cms.py` and must stay aligned with `scraper.py`’s `DRUGS` list when you add or change products.

## Development

- `pytest`: Unit tests (including master-dataset join logic).
- `error_screenshots/`: Screenshots from failed extractions.
- `python3 acceptance_metrics.py --log-file run.log`: Evaluate acceptance targets after a run.

## License

MIT
