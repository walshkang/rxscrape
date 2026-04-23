# Project Context — rxscrape

## Purpose

Automate GoodRx price collection for a fixed list of drugs and ZIP codes, then append normalized pricing rows to `national_pharmacy_pricing.csv`.

The script uses Playwright with stealth behavior and human-like interaction patterns to handle bot checks and dynamic UI changes.

## Current Architecture

Main runtime workflow lives in `scraper.py`, with parsing helpers and tests in separate files:

- `scraper.py`: browser automation, ZIP iteration, modal/captcha handling, row extraction, CSV append.
- `scraper_utils.py`: normalization utilities like `parse_price`.
- `tests/test_scraper.py`: unit tests for price parsing behavior.

Runtime flow in `scraper.py`:

1. Launch persistent Chromium context (`browser_profile`) with stealth.
2. For each drug URL:
   - navigate and warm page session
   - iterate configured ZIPs
3. For each ZIP:
   - detect/clear captcha (`check_and_handle_captcha`, `solve_px_captcha_button`)
   - open location modal and set location
   - wait for pharmacy rows and parse top rows
   - append normalized result rows to CSV
4. On per-zip failures:
   - take screenshot in `error_screenshots/`
   - log structured failure line
   - reload drug URL and continue

## Configuration At A Glance

Primary runtime configuration is defined at the top of `scraper.py`:

- `DRUGS`: fixed list of drug pages to scrape.
- `ZIP_CODES`: fixed ZIP sample set to iterate.
- `CSV_FILE`: output path (`national_pharmacy_pricing.csv`).
- `SCREENSHOT_DIR`: failure artifact directory (`error_screenshots`).
- `USER_DATA_DIR`: persistent Chromium profile directory (`browser_profile`).
- `LOCATION_TRIGGER_RE`: regex used to locate location trigger variants.

Current runtime defaults in code:

- Browser mode: headed (`headless=False`) with persistent profile.
- Per ZIP extraction target: up to 3 parsed rows (`results_count >= 3` stop condition).
- Candidate scan window: first 10 row candidates per ZIP.

## Key Components

- **Captcha handling**
  - `solve_px_captcha_button()` performs press-and-hold with jitter.
  - `clear_px_captcha_if_blocking()` loops while `iframe#px-captcha-modal` is visible.

- **Modal scoping**
  - `location_modal(page)` targets `get_by_test_id("locationModal")`.
  - Avoids strict-mode collisions with Osano dialogs.

- **Error taxonomy**
  - `LocationTriggerError`, `LocationModalError`, `ZipInputError`, `ResultsNotFoundError`
  - `fail_step()` prints consistent per-zip failure messages.

- **Selector resilience**
  - Expanded location trigger regex (`Set location`, `Your location`, `Current location`, ZIP text).
  - Fallbacks for aria-label trigger variants.
  - ZIP input resolver supports role/placeholder/label/autocomplete fallback chain.

## What Was Fixed Recently

1. **Strict-mode dialog conflicts**
   - Root cause: generic dialog locators matching Osano + GoodRx modal.
   - Fix: explicit `locationModal` target.

2. **PerimeterX pointer interception**
   - Root cause: `#px-captcha-modal` iframe blocking clicks.
   - Fix: pre-click `clear_px_captcha_if_blocking()` with retries.

3. **Location trigger copy drift**
   - Root cause: button labels changed to include current ZIP phrasing.
   - Fix: broader trigger match + aria-label fallback.

4. **Error handling cleanup**
   - Bare `except:` replaced with explicit exception handling where touched.
   - Added structured failure output and graceful top-level cancellation handling.

## Current Status (as of latest live runs)

Major blocker categories have shifted from initial strict-mode failures to downstream UI variance:

1. **Location modal submit not always closing**
   - Some runs fail on `expect(locationModal).to_be_hidden(...)`.

2. **Savings-tip overlay intercepts interactions**
   - `savings-tip-row-modal` overlay can block row clicks.

3. **Row extraction still brittle on some layouts**
   - Broader row selectors can match containers.
   - Some row candidates still miss reliable name/price fields.

4. **Result presence can be partial**
   - Script advances further than before, but still emits many row parse skips.

## Next Meaningful Work

1. **Overlay-aware parsing**
   - Detect and close `savings-tip-row-modal` before iterating rows.
   - Skip row click actions while blocking overlay is present.
   - Acceptance target: 0 unhandled overlay-intercept failures across 50 ZIP iterations.

2. **Tighten row candidate selection**
   - Exclude container sections and target only leaf row cards.
   - Add explicit `is_row_candidate` guard before parsing.
   - Acceptance target: row-parse skip rate under 10% across first 10 candidates on a 3-drug x 5-ZIP run.

3. **Decouple retail-price enrichment from main parse**
   - Save GoodRx price/name without row click dependency.
   - Attempt retail enrichment only when safe and non-blocked.
   - Acceptance target: base fields (`Pharmacy_Name`, `GoodRx_Price`) populated for at least 95% of saved rows; retail enrichment best-effort at 70%+ without reducing base completion.

4. **Add debug artifacts for failed ZIP parses**
   - On `ResultsNotFoundError` or repeated row skip, persist a small DOM snapshot artifact for selector tuning.
   - Acceptance target: 100% artifact capture for qualifying failures, with timestamped files and 14-day local retention.

5. **Keep retry behavior bounded**
   - Preserve per-zip retries and continue-on-failure to avoid full-run stalls.
   - Acceptance target: max 2 retries per ZIP and a per-zip asyncio budget (`ZIP_ATTEMPT_BUDGET_SEC` in `scraper.py`) before fail-and-continue.

## Flow Overview

```mermaid
flowchart TD
    startRun[StartRun] --> launchCtx[LaunchPersistentContext]
    launchCtx --> drugLoop[LoopDrugs]
    drugLoop --> zipLoop[LoopZips]
    zipLoop --> captchaCheck[CheckAndClearCaptcha]
    captchaCheck --> setLocation[OpenLocationModalAndSetZip]
    setLocation --> rowsReady[WaitForPharmacyRows]
    rowsReady --> parseRows[ParseTopRows]
    parseRows --> saveCsv[AppendNormalizedCsvRows]
    saveCsv --> zipDone[ZipDone]
    zipDone --> zipLoop
    zipLoop -->|PerZipFailure| failShot[SaveFailureScreenshot]
    failShot --> reloadDrug[ReloadDrugPage]
    reloadDrug --> zipLoop
    zipLoop --> drugLoop
    drugLoop --> endRun[EndRun]
```

## Operational Notes

- Canonical install/run steps live in `README.md` (use that as source of truth).
- Quick run (inside activated venv): `python3 scraper.py`
- Browser install (fresh environment): `playwright install chromium`
- Runtime stack: Playwright + `playwright-stealth` (version pins in `requirements.txt`).
- Current default is headed mode for stability debugging (`headless=False`); switch only with explicit validation in anti-bot/captcha conditions.
- Persistent profile lock can require killing stale Chromium processes using `browser_profile`.

## Data Outputs

- `national_pharmacy_pricing.csv`
  - `Date, Zip_Code, Drug_Name, Pharmacy_Name, Retail_Price, GoodRx_Price`
- `error_screenshots/fail_<drug>_<zip>_<timestamp>.png`

