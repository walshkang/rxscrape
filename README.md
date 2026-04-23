# GoodRx Price Scraper (rxscrape)

A robust, stealth-enabled Python scraper built with Playwright to extract generic drug pricing data from GoodRx.com across multiple US zip codes.

## Features
- **PerimeterX Bypass**: Advanced CAPTCHA solver using Bezier mouse curves and randomized jitter.
- **Stealth Configuration**: Utilizes `playwright-stealth` and persistent browser profiles to mimic human behavior.
- **Retail Price Extraction**: Automatically reveals hidden retail prices by interacting with pharmacy rows.
- **Data Persistence**: Saves results immediately to `national_pharmacy_pricing.csv`.

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
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

### Usage
Run the scraper:
```bash
python3 scraper.py
```
The scraper will loop through pre-configured drugs and zip codes, saving the data to `national_pharmacy_pricing.csv`.

## Development
- `pytest`: Run unit tests for data cleaning logic.
- `error_screenshots/`: Check this directory for screenshots of any failed extraction attempts for debugging.
- `python3 acceptance_metrics.py --log-file run.log`: Evaluate acceptance targets after a run.

## License
MIT
