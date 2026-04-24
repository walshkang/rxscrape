import asyncio
import csv
import math
import os
import random
import re
import time
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, expect, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth

# Configuration
DRUGS = [
    {"name": "Atorvastatin", "url": "https://www.goodrx.com/atorvastatin"},
    {"name": "Amoxicillin", "url": "https://www.goodrx.com/amoxicillin"},
    {"name": "Imatinib", "url": "https://www.goodrx.com/imatinib"},
]
# Representative metros across NY, CA, MI, TX, SD (national spread; not exhaustive).
ZIP_CODES = [
    # New York
    "10001",
    "11211",
    # California
    "90012",
    "94102",
    "90210",
    # Michigan
    "48201",
    "49503",
    # Texas
    "77002",
    "75201",
    "78701",
    # South Dakota
    "57104",
    "57701",
]
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
# One snapshot file per `main()` run (prices change over time; do not mix runs in one file).
RUNS_DIR = "runs"
OUTPUT_FIELDNAMES = [
    "Date",
    "Zip_Code",
    "Drug_Name",
    "Pharmacy_Name",
    "Option_Type",
    "Retail_Price",
    "Retail_Flag",
    "GoodRx_Price",
]
_run_csv_path: str | None = None
SCREENSHOT_DIR = "error_screenshots"
USER_DATA_DIR = "browser_profile"
DOM_ARTIFACT_DIR = "error_dom_artifacts"
LOCATION_TRIGGER_RE = re.compile(r"\d{5}|Set location|Your location|Current location", re.I)
MAX_ROW_CANDIDATES = 30
MAX_ROW_SCAN_PASSES = 4
MAX_ROW_SKIP_BEFORE_ARTIFACT = 4
MAX_ZIP_RETRIES = 2
# Must cover: PerimeterX (several 8–12s holds + delays), location modal, human_delay,
# and 3× retail row enrichment. 90s routinely trips asyncio.wait_for() mid-scrape.
ZIP_ATTEMPT_BUDGET_SEC = 360
DOM_ARTIFACT_RETENTION_DAYS = 14
CAPTCHA_MAX_SOLVE_ATTEMPTS = 3
CAPTCHA_RECHECK_WAIT_SEC = 2.0
# GoodRx splits local pickup vs mail-order: retail is under the main list; "Home delivery" rows
# live in a separate region (see data-qa on the mail-order container in the live DOM).
MAIL_ORDER_PHARMACY_ROWS_CONTAINER_QA = "mail-order-pharmacy-rows-container"
# Tunable pacing (seconds, min–max). Shorter = faster; too low may look bot-like or race the UI.
DELAY_POST_DRUG_LOAD_S = (2.0, 3.5)  # after drug URL; was ~4–6s
DELAY_POST_RESULTS_VISIBLE_S = (1.0, 1.8)  # after pharmacy rows first visible; was ~3–5s
DELAY_BETWEEN_ZIPS_S = (1.5, 3.5)  # after a successful zip; was ~5–10s
DELAY_WARMUP_GOOGLE_S = (0.5, 1.0)  # after google hop; was ~1–2s
DELAY_ON_ZIP_RETRY_S = (2.0, 3.5)  # reload after failed zip; was ~3–5s
ZIP_TYPE_CHAR_DELAY_MS = (35, 75)  # one-shot type(); was 150–400ms per char


class ScrapeError(Exception):
    pass


class LocationTriggerError(ScrapeError):
    pass


class LocationModalError(ScrapeError):
    pass


class ZipInputError(ScrapeError):
    pass


class ResultsNotFoundError(ScrapeError):
    pass


def fail_step(step: str, exc: BaseException, drug_name: str, zip_code: str) -> str:
    return f"  FAILED [{drug_name} {zip_code}::{step}] {type(exc).__name__}: {exc}"


def location_trigger_locator(page):
    # Prefer semantic button matching, then aria-label fallbacks.
    return page.get_by_role("button", name=LOCATION_TRIGGER_RE)


async def first_visible_text(row, selector_candidates, timeout_ms=2500):
    """Return first visible non-empty text from selector candidates in a row."""
    for sel in selector_candidates:
        loc = row.locator(sel)
        try:
            if await loc.count() == 0:
                continue
            candidate = loc.first
            await expect(candidate).to_be_visible(timeout=timeout_ms)
            txt = (await candidate.inner_text(timeout=timeout_ms)).strip()
            if txt:
                return txt
        except Exception:
            continue
    return None


# Marketing, upsell, and status lines — not a brick-and-mortar / mail-order label.
PHARMACY_LINE_JUNK = re.compile(
    r"(?i)included with|goodrx gold|\bgold member|free trial|"
    r"^sign up\b|save (an extra|up to)|\bdownload the goodrx|try goodrx|"
    r"^get (a |the )?coupon|member (price|savings)|^special offer|limited time|"
    r"^with goodrx$|^sponsored|advertisement|price as of|"
    r"^same[- ]day( pickup)?$|^(pick up|pickup|in stock|open now|closed|drive[- ]?thru)!?$|"
    r"^est\.?\s*retail|^mail order|^buy online|continue to"
)
GENERIC_PHARMACY_LABEL_RE = re.compile(
    r"(?i)^(home\s*delivery|free\s*shipping|mail\s*order|buy\s*online|ship(?:ping)?\s*to\s*home|pick\s*up|pickup|delivery)$"
)

MAIL_ORDER_PHARMACY_RE = re.compile(
    # Only classify true mail-order labels/providers; allow local chains with delivery promos.
    r"(?i)^home\s*delivery$|^free\s*shipping$|^mail\s*order$|^ship(?:ping)?\s*to\s*home$|^buy\s*online$|"
    r"\bdirx\b|\bcost\s*plus\b|\bgeniusrx\b|\bhealthwarehouse\b|\bgoodrx\s*home\s*delivery\b"
)


def is_plausible_pharmacy_name(s: str) -> bool:
    t = (s or "").strip()
    if len(t) < 2 or len(t) > 120:
        return False
    if PHARMACY_LINE_JUNK.search(t):
        return False
    return bool(re.search(r"[A-Za-z]", t))


def is_mail_order_pharmacy(name: str) -> bool:
    return bool(MAIL_ORDER_PHARMACY_RE.search((name or "").strip()))


def extract_price_with_regex(text: str) -> float | None:
    """Strict extraction using requested regex to clean up messy text blobs."""
    if not text:
        return None
    match = re.search(r"\$([0-9,.]+)", text)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except ValueError:
            return None
    return None


def local_pharmacy_price_rows_locator(page):
    """
    Locator for in-store / local pickup price rows only.

    GoodRx puts mail-order offers in data-qa="mail-order-pharmacy-rows-container" (the
    "Home delivery" block below the local pharmacy list). A broad CSS match on pharmacy+row
    also hits that container, so we exclude it with XPath not(ancestor::...).
    """
    q = MAIL_ORDER_PHARMACY_ROWS_CONTAINER_QA
    return page.locator(
        f"xpath=//button[.//*[@data-qa=\"seller-name\"]][not(ancestor::*[@data-qa=\"{q}\"])]"
        f" | //a[.//*[@data-qa=\"seller-name\"]][not(ancestor::*[@data-qa=\"{q}\"])]"
        f" | //div[@data-qa=\"pharmacy-row\"][not(ancestor::*[@data-qa=\"{q}\"])]"
        f" | //*[contains(@class, \"priceRow\")][not(ancestor::*[@data-qa=\"{q}\"])]"
        f" | //*[@data-testid and contains(@data-testid, \"pharmacy\") and contains(@data-testid, \"row\")]"
        f"[not(ancestor::*[@data-qa=\"{q}\"])]"
    )


async def _first_plausible_n(row, sel: str, timeout_ms: int = 2500):
    """All matches for `sel` in DOM order; return first that looks like a pharmacy name."""
    loc = row.locator(sel)
    try:
        n = await loc.count()
    except Exception:
        return None
    for i in range(n):
        try:
            el = loc.nth(i)
            if not await el.is_visible():
                continue
            txt = (await el.inner_text(timeout=timeout_ms)).strip()
            if GENERIC_PHARMACY_LABEL_RE.search(txt):
                continue
            if is_plausible_pharmacy_name(txt):
                return txt
        except Exception:
            continue
    return None


async def extract_pharmacy_name(row):
    """
    Resolve chain / store name. Prefer named slots and /pharmacy/ links; skip GoodRx
    marketing (e.g. 'Included with GoodRx Gold') that often wins generic strong/h4.
    """
    for sel in (
        '[data-qa="seller-name"]',
        'a[href*="/pharmacy/"]',
        '[data-qa*="pharmacy"] h2, [data-qa*="pharmacy"] h3, [data-qa*="pharmacy"] h4, [data-qa*="pharmacy"] h5',
        '[data-testid*="pharmacy"] h2, [data-testid*="pharmacy"] h3, [data-testid*="pharmacy"] h4, [data-testid*="pharmacy"] h5',
        '[data-qa="pharmacy-name"]',
        '[data-qa*="pharmacy-name"]',
        '[data-qa*="pharmacyName"]',
        '[data-testid*="pharmacy-name"]',
        '[data-testid*="pharmacyName"]',
        '[class*="pharmacyName"]',
        '[class*="PharmacyName"]',
        "h2",
        "h3",
        "h4",
        "h5",
        "strong",
        "b",
    ):
        got = await _first_plausible_n(row, sel)
        if got:
            return got
    try:
        row_text = await row.inner_text(timeout=2500)
    except Exception:
        row_text = ""
    for line in row_text.splitlines():
        t = line.strip()
        if not t:
            continue
        if re.search(r"\$[0-9]", t):
            continue
        if GENERIC_PHARMACY_LABEL_RE.search(t):
            continue
        if is_plausible_pharmacy_name(t):
            return t
    return None

async def human_delay(min_s=2, max_s=4):
    await asyncio.sleep(random.uniform(min_s, max_s))

async def bezier_mouse_move(page, end_x, end_y):
    """Moves the mouse in a natural Bezier curve from current position to target."""
    # Get current mouse position (default to 0,0 if unknown, but usually starts at last pos)
    # Since we can't easily get current mouse pos in Playwright without a trick,
    # we'll just move from a random offset nearby if it's the first move.
    start_x, start_y = random.randint(0, 100), random.randint(0, 100)
    
    # Create 1-2 control points for the curve
    control_x = (start_x + end_x) / 2 + random.uniform(-150, 150)
    control_y = (start_y + end_y) / 2 + random.uniform(-150, 150)
    
    steps = random.randint(20, 40)
    for i in range(steps + 1):
        t = i / steps
        # Quadratic Bezier formula: (1-t)^2*P0 + 2(1-t)t*P1 + t^2*P2
        x = (1 - t)**2 * start_x + 2 * (1 - t) * t * control_x + t**2 * end_x
        y = (1 - t)**2 * start_y + 2 * (1 - t) * t * control_y + t**2 * end_y
        await page.mouse.move(x, y)
        if i % 5 == 0:
            await asyncio.sleep(random.uniform(0.001, 0.005))

async def solve_px_captcha_button(page, button):
    """Advanced solver for a 'Press & Hold' button element with mouse jitter."""
    try:
        await expect(button).to_be_visible(timeout=5000)
        box = await button.bounding_box()
        if not box: 
            print("    Could not get button bounding box.")
            return False
            
        # Target a random spot in the inner 50% of the button
        target_x = box["x"] + box["width"] * random.uniform(0.3, 0.7)
        target_y = box["y"] + box["height"] * random.uniform(0.3, 0.7)
        
        await bezier_mouse_move(page, target_x, target_y)
        print(f"    Pressing button at ({target_x:.1f}, {target_y:.1f})...")
        
        await page.mouse.down()
        
        # Hold for 8-12 seconds with "pressure jitter" (small mouse movements)
        start_time = datetime.now()
        hold_duration = random.uniform(8.5, 12.0)
        
        print(f"    Holding for {hold_duration:.1f}s with jitter...")
        while (datetime.now() - start_time).total_seconds() < hold_duration:
            # Subtle jitter (less than 1 pixel)
            jitter_x = target_x + random.uniform(-0.5, 0.5)
            jitter_y = target_y + random.uniform(-0.5, 0.5)
            await page.mouse.move(jitter_x, jitter_y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
            
        await page.mouse.up()
        print("    Button released. Waiting for validation...")
        await human_delay(6, 9)
        
        # Check if captcha is gone (both text and modal iframe checks).
        captcha_text = page.get_by_text(re.compile(r"Before we continue", re.I))
        iframe = page.locator("iframe#px-captcha-modal")
        text_present = await captcha_text.count() > 0
        iframe_visible = False
        try:
            iframe_visible = await iframe.count() > 0 and await iframe.first.is_visible()
        except Exception:
            iframe_visible = False
        if not text_present and not iframe_visible:
            print("    Successfully bypassed CAPTCHA.")
            return True
        return False
    except Exception as e:
        print(f"    Error solving button: {e}")
        return False

async def check_and_handle_captcha(page, max_attempts: int = CAPTCHA_MAX_SOLVE_ATTEMPTS):
    """Checks for captcha and attempts solve if found."""
    captcha_button_name = re.compile(r"Press & Hold", re.I)
    captcha_text = page.get_by_text(re.compile(r"Before we continue", re.I))
    iframe = page.locator("iframe#px-captcha-modal")

    for attempt in range(max_attempts):
        # 1. Check main page
        button = page.get_by_role("button", name=captcha_button_name)
        if await button.count() > 0:
            if await solve_px_captcha_button(page, button.first):
                return True
            print(f"    CAPTCHA solve attempt {attempt + 1}/{max_attempts} did not clear challenge.")
            await asyncio.sleep(CAPTCHA_RECHECK_WAIT_SEC)
            continue

        # 2. Check all frames
        solved_from_frame = False
        for frame in page.frames:
            try:
                frame_button = frame.get_by_role("button", name=captcha_button_name)
                if await frame_button.count() == 0:
                    continue
                print(f"    Found button in frame: {frame.url[:40]}...")
                if await solve_px_captcha_button(page, frame_button.first):
                    return True
                solved_from_frame = True
                break
            except Exception:
                continue
        if solved_from_frame:
            print(f"    CAPTCHA solve attempt {attempt + 1}/{max_attempts} did not clear challenge.")
            await asyncio.sleep(CAPTCHA_RECHECK_WAIT_SEC)
            continue

        # 3. If challenge shell exists without button, back off briefly then retry.
        text_present = await captcha_text.count() > 0
        iframe_visible = False
        try:
            iframe_visible = await iframe.count() > 0 and await iframe.first.is_visible()
        except Exception:
            iframe_visible = False
        if text_present or iframe_visible:
            print("    CAPTCHA challenge still present; waiting before retry...")
            await asyncio.sleep(CAPTCHA_RECHECK_WAIT_SEC)
            continue

        # No captcha indicators found.
        return False

    print(f"    CAPTCHA not cleared after {max_attempts} attempts; continuing with bounded retry flow.")
    return False

def location_modal(page):
    """
    GoodRx's ZIP modal; must not use a generic [role=dialog] — Osano cookies also
    expose role=dialog+aria-modal while hidden, which breaks strict locators.
    """
    return page.get_by_test_id("locationModal")

async def clear_px_captcha_if_blocking(page, max_passes: int = 6):
    """#px-captcha-modal sits above the page and blocks clicks on the location control."""
    for _ in range(max_passes):
        iframe = page.locator("iframe#px-captcha-modal")
        if await iframe.count() == 0:
            return True
        try:
            if not await iframe.first.is_visible():
                return True
        except Exception:
            return True
        solved = await check_and_handle_captcha(page)
        if not solved:
            await asyncio.sleep(1.2)
    # Best-effort: if still visible we return False so callers can escalate/retry.
    iframe = page.locator("iframe#px-captcha-modal")
    if await iframe.count() == 0:
        return True
    try:
        return not await iframe.first.is_visible()
    except Exception:
        return True

async def resolve_zip_input_locator(page, dialog):
    """
    Find the city/ZIP field inside the location UI. GoodRx has changed
    placeholders/roles over time, so we try several accessibility-based locators
    and fall back to the first visible text-like input in the dialog.
    """
    name_or_placeholder = re.compile(
        r"zip|postal|city|state|location|search|enter|where|pharmacy|"
        r"set\s*location|address|find|neighborhood",
        re.I,
    )
    for root in (dialog, page):
        strategies = [
            root.get_by_role("combobox", name=name_or_placeholder),
            root.get_by_role("textbox", name=name_or_placeholder),
            root.get_by_placeholder(name_or_placeholder),
            root.get_by_label(re.compile(r"zip|location|city|search|address|pharmacy", re.I)),
            root.locator("input[autocomplete='postal-code']"),
            root.locator("input[type='search']"),
        ]
        for strat in strategies:
            try:
                if await strat.count() == 0:
                    continue
                el = strat.first
                await expect(el).to_be_visible(timeout=5000)
                return el
            except Exception:
                continue
    n = await dialog.locator("input").count()
    for i in range(n):
        inp = dialog.locator("input").nth(i)
        t = (await inp.get_attribute("type") or "text").lower()
        if t in ("hidden", "submit", "button", "checkbox", "radio", "file", "image"):
            continue
        try:
            if await inp.is_visible():
                return inp
        except Exception:
            continue
    raise Exception("No zip/location input found (selectors out of date).")

def row_identity_key(pharmacy_name: str, goodrx: float) -> tuple:
    """Deduplicate DOM copies of the same offer (name + GoodRx price per zip)."""
    return (pharmacy_name.strip().lower(), round(goodrx, 2))


async def save_to_csv(data):
    path = _run_csv_path
    if not path:
        raise RuntimeError("Output CSV not set — run via main() so each run gets its own file under runs/.")
    file_exists = os.path.isfile(path)
    with open(path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)


def sanitize_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", value)


async def cleanup_old_dom_artifacts(retention_days: int = DOM_ARTIFACT_RETENTION_DAYS):
    cutoff = datetime.now() - timedelta(days=retention_days)
    for file_name in os.listdir(DOM_ARTIFACT_DIR):
        path = os.path.join(DOM_ARTIFACT_DIR, file_name)
        try:
            if not os.path.isfile(path):
                continue
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                os.remove(path)
        except Exception:
            continue


async def snapshot_retail_debug_modal_text(page) -> str:
    """Visible non-location dialog text (coupon / price detail) for forensics. Truncated."""
    chunks: list[str] = []
    try:
        dlog = page.locator('[role="dialog"]')
        n = await dlog.count()
        for i in range(min(n, 4)):
            el = dlog.nth(i)
            try:
                if not await el.is_visible():
                    continue
            except Exception:
                continue
            try:
                tid = (await el.get_attribute("data-testid") or "")
                eid = (await el.get_attribute("id") or "")
                if re.search(r"location", tid, re.I) or eid == "locationModal":
                    continue
            except Exception:
                pass
            try:
                t = (await el.inner_text(timeout=2500) or "").strip()
                if t:
                    chunks.append(t[:6000])
            except Exception:
                pass
    except Exception:
        pass
    if not chunks:
        return ""
    return "\n---\n".join(chunks)[:10000]


async def log_retail_suspicious_artifact(
    page,
    drug_name: str,
    zip_code: str,
    pharmacy_name: str,
    goodrx: float,
    retail: float,
    modal_text: str,
) -> None:
    """When retail < GoodRx, save modal text + full-page screenshot for manual review."""
    os.makedirs(DOM_ARTIFACT_DIR, exist_ok=True)
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = (
        f"retail_susp_{sanitize_name(drug_name)}_{zip_code}_"
        f"{sanitize_name(pharmacy_name)[:80]}_{ts}"
    )
    path_txt = os.path.join(DOM_ARTIFACT_DIR, f"{base}.txt")
    try:
        with open(path_txt, "w", encoding="utf-8") as f:
            f.write(f"url: {page.url}\n")
            f.write(
                f"drug={drug_name!r} zip={zip_code!r} pharmacy={pharmacy_name!r}\n"
            )
            f.write(
                f"GoodRx={goodrx} retail={retail} (retail < GoodRx — worth verifying)\n\n"
            )
            f.write("--- visible non-location dialog text (captured before popup close) ---\n")
            f.write((modal_text or "(none)").strip() + "\n")
            f.write(
                "\n(Page PNG is full_page after the coupon was closed.)\n"
            )
        print(f"    [Retail check] {path_txt}")
    except Exception as exc:
        print(f"    [Retail check] could not write text artifact: {exc}")
    try:
        path_png = os.path.join(SCREENSHOT_DIR, f"{base}.png")
        await page.screenshot(path=path_png, full_page=True)
        print(f"    [Retail check] {path_png}")
    except Exception as exc:
        print(f"    [Retail check] screenshot failed: {exc}")


async def save_dom_artifact(page, drug_name: str, zip_code: str, reason: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_drug = sanitize_name(drug_name)
    safe_reason = sanitize_name(reason)
    path = os.path.join(DOM_ARTIFACT_DIR, f"dom_{safe_drug}_{zip_code}_{safe_reason}_{ts}.html")
    try:
        html = await page.content()
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"<!-- URL: {page.url} -->\n")
            f.write(f"<!-- CapturedAt: {datetime.now().isoformat()} -->\n")
            f.write(f"<!-- Reason: {reason} -->\n")
            f.write(html)
        print(f"    Saved DOM artifact: {path}")
    except Exception as exc:
        print(f"    Failed to save DOM artifact ({reason}): {exc}")


def is_overlay_visible_locator(page):
    return page.locator(
        '[data-testid*="savings-tip-row-modal"], '
        '[data-qa*="savings-tip-row-modal"], '
        '[id*="savings-tip-row-modal"], '
        '[class*="savings-tip-row-modal"]'
    )


async def clear_savings_tip_overlay(page, max_passes: int = 3):
    overlay = is_overlay_visible_locator(page)
    for _ in range(max_passes):
        try:
            if await overlay.count() == 0 or not await overlay.first.is_visible():
                return True
        except Exception:
            return True
        close_candidates = [
            page.get_by_role("button", name=re.compile(r"close|dismiss|got it|ok", re.I)),
            overlay.locator('button[aria-label*="close" i]'),
            overlay.locator('button[data-testid*="close"]'),
            overlay.locator("button"),
        ]
        closed = False
        for candidate in close_candidates:
            try:
                if await candidate.count() == 0:
                    continue
                await candidate.first.click(timeout=3000)
                await asyncio.sleep(0.4)
                closed = True
                break
            except Exception:
                continue
        if not closed:
            try:
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.4)
            except Exception:
                pass
    try:
        return await overlay.count() == 0 or not await overlay.first.is_visible()
    except Exception:
        return True


async def clear_goodrx_pets_modal(page, max_passes: int = 3):
    modal = page.locator(
        '[data-testid*="goodrx"][data-testid*="pets"], '
        '[data-qa*="goodrx"][data-qa*="pets"], '
        '[class*="pets"]'
    )
    heading = page.get_by_text(re.compile(r"GoodRx for Pets|Continue to GoodRx for Pets", re.I))
    for _ in range(max_passes):
        modal_visible = False
        try:
            modal_visible = (await modal.count() > 0 and await modal.first.is_visible()) or (
                await heading.count() > 0 and await heading.first.is_visible()
            )
        except Exception:
            modal_visible = False
        if not modal_visible:
            return True

        close_candidates = [
            page.get_by_role("button", name=re.compile(r"cancel|close|dismiss|not now", re.I)),
            page.locator('button[aria-label*="close" i]'),
            page.locator('button:has-text("Cancel")'),
            page.locator('button:has-text("Close")'),
            page.locator("button"),
        ]
        dismissed = False
        for candidate in close_candidates:
            try:
                if await candidate.count() == 0:
                    continue
                # Prefer non-navigation actions; avoid clicking "Continue to GoodRx for Pets".
                label = (await candidate.first.inner_text(timeout=1000)).strip().lower()
                if "continue to goodrx for pets" in label:
                    continue
                await candidate.first.click(timeout=2500)
                await asyncio.sleep(0.4)
                dismissed = True
                break
            except Exception:
                continue
        if not dismissed:
            try:
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.4)
            except Exception:
                pass

    try:
        still_visible = (await modal.count() > 0 and await modal.first.is_visible()) or (
            await heading.count() > 0 and await heading.first.is_visible()
        )
        return not still_visible
    except Exception:
        return True


async def clear_known_interstitials(page):
    await clear_savings_tip_overlay(page)
    await clear_goodrx_pets_modal(page)


async def close_price_detail_popup(page, max_passes: int = 3):
    popup = page.locator(
        '[role="dialog"][data-state="open"], '
        '[data-testid*="modal"][data-state="open"], '
        '[data-qa*="modal"][data-state="open"]'
    )
    # Coupon detail (incl. stricken “Retail price”) — not the ZIP location dialog.
    price_heading = page.get_by_text(
        re.compile(
            r"Buy online|Mail order price|GoodRx|Retail price|\bBIN\b|Member ID|Continue to|Press\s*&\s*Hold",
            re.I,
        )
    )
    for _ in range(max_passes):
        visible = False
        try:
            visible = (await popup.count() > 0 and await popup.first.is_visible()) and (
                await price_heading.count() > 0
            )
        except Exception:
            visible = False
        if not visible:
            return True

        close_candidates = [
            page.locator('button[aria-label*="close" i]'),
            page.get_by_role("button", name=re.compile(r"cancel|close|back|done", re.I)),
            page.locator("button"),
        ]
        for candidate in close_candidates:
            try:
                if await candidate.count() == 0:
                    continue
                # Avoid the outbound CTA.
                label = (await candidate.first.inner_text(timeout=1000)).strip().lower()
                if "continue to" in label:
                    continue
                await candidate.first.click(timeout=2000)
                await asyncio.sleep(0.4)
                break
            except Exception:
                continue
        else:
            try:
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.4)
            except Exception:
                pass
    return False


async def is_row_candidate(row):
    if not await row.is_visible():
        return False
    try:
        row_text = (await row.inner_text(timeout=2500)).strip()
    except Exception:
        return False
    if not row_text:
        return False
    has_price = bool(re.search(r"\$[0-9,]+(?:\.[0-9]{2})?", row_text))
    has_name = bool(re.search(r"[A-Za-z]{3,}", row_text))
    return has_price and has_name


# Digits for a list / retail / cash / was amount (0–2 decimal places, optional cents).
_RETAIL_DOLLAR_AMT = r"[0-9,]+(?:\.[0-9]{1,2})?"

# Label in text, then (within a window) a dollar — handles line breaks, colons, “Retail price”.
RETAIL_LABELED_PRICE_RE = re.compile(
    r"(?is)(?:"
    r"retail(?:\s+price)?\b(?!\s+sa)"
    r"|est\.?\s*retail\b"
    r"|typical\s+retail\b"
    r"|suggested\s+retail\b"
    r"|list\s+price\b"
    r"|original\s+price\b"
    r"|full\s+price\b"
    r"|cash\s+price\b"
    r"|compare\s*at"
    r"|\bmsrp\b"
    r")\s*[:#]?"
    r".{0,140}?"
    rf"\$({_RETAIL_DOLLAR_AMT})"
)

# “Was $12.34” (list / crossed-out) — line or same blob.
WAS_DOLLAR_PRICE_RE = re.compile(
    rf"(?i)(?:^|[\n\r]|\b)was\s+\$({_RETAIL_DOLLAR_AMT})\b"
)
WAS_DOLLAR_PRICE_INLINE_RE = re.compile(rf"(?i)\bwas:\s*\$({_RETAIL_DOLLAR_AMT})")


def _float_from_captured_dollar_string(m: re.Match[str], group: int = 1) -> float | None:
    if not m:
        return None
    raw = m.group(group)
    return extract_price_with_regex(f"${raw}")


def extract_retail_from_blurb(text: str):
    """Pull a retail / cash / list / was price from a row or modal text blob (regex + line scan)."""
    if not text or not text.strip():
        return None

    m = RETAIL_LABELED_PRICE_RE.search(text)
    if m:
        v = _float_from_captured_dollar_string(m, 1)
        if v is not None:
            return v
    m = WAS_DOLLAR_PRICE_RE.search(text) or WAS_DOLLAR_PRICE_INLINE_RE.search(text)
    if m:
        v = _float_from_captured_dollar_string(m, 1)
        if v is not None:
            return v

    line_keywords = re.compile(
        r"(?i)retail(\s+price)?\b|est\.?\s*retail|"
        r"cash\s+price\b|original\s+price|list\s+price|"
        r"^was\s+\$|typical\s+retail|full\s+price"
    )
    for line in text.splitlines():
        t = line.strip()
        if "$" not in t or not t:
            continue
        if line_keywords.search(t):
            parsed = extract_price_with_regex(t)
            if parsed is not None:
                return parsed

    for pat in (
        rf"(?i)retail[^$\n]{{0,48}}\$({_RETAIL_DOLLAR_AMT})\b",
        rf"(?i)est\.?\s*retail[^$\n]{{0,48}}\$({_RETAIL_DOLLAR_AMT})\b",
        rf"(?i)typical\s+retail[^$\n]{{0,48}}\$({_RETAIL_DOLLAR_AMT})\b",
        rf"(?i)was\s+\$({_RETAIL_DOLLAR_AMT})\b",
    ):
        m2 = re.search(pat, text)
        if m2:
            v = _float_from_captured_dollar_string(m2, 1)
            if v is not None:
                return v
    return None


# After opening a pharmacy row, the coupon often shows “Retail price:” with the list amount
# in <s>/<del>, via CSS line-through, and/or as muted gray text — not always semantic <s> tags.
_STRIKETHROUGH_RETAIL_IN_MODAL_JS = r"""
() => {
    const isRetail = (s) => /retail|est\.\s*retail|list\s+price|typical|msrp|suggested|compare\s*at|was|cash\s+price/i.test(s);
    const pickFirst = (s) => {
        const m = (s || "").match(/\$([0-9,]+(?:\.[0-9]{1,2})?)/);
        if (!m) return null;
        const v = parseFloat(m[1].replace(/,/g, ""));
        if (Number.isNaN(v) || v <= 0) return null;
        return v;
    };
    const isLocModal = (d) => {
        if (!d) return true;
        if (/location/i.test(d.getAttribute("data-testid") || "")) return true;
        if (d.getAttribute("id") === "locationModal") return true;
        return false;
    };
    const hasRetailAncestor = (el, maxH) => {
        let p = el;
        for (let i = 0; i < maxH && p; i++) {
            if (isRetail(p.textContent || "")) return true;
            p = p.parentElement;
        }
        return false;
    };
    const isStruckTag = (el) => {
        const t = (el && el.tagName) || "";
        return t === "S" || t === "DEL" || t === "STRIKE";
    };
    const isLineThroughComputed = (el) => {
        if (!el || el.nodeType !== 1) return false;
        try {
            const st = getComputedStyle(el);
            const t = (st.textDecorationLine || "") + " " + (st.textDecoration || "");
            return t.indexOf("line-through") >= 0;
        } catch (e) {
            return false;
        }
    };
    /** GoodRx often applies strikethrough via CSS on a wrapper; walk up a few levels. */
    const isStruckOrAncestor = (el, maxH) => {
        let p = el;
        for (let i = 0; i < maxH && p; p = p.parentElement, i++) {
            if (isStruckTag(p) || isLineThroughComputed(p)) return true;
        }
        return false;
    };
    const isGrayish = (el) => {
        if (!el || el.nodeType !== 1) return false;
        try {
            const c = getComputedStyle(el).color;
            const m = c.match(/rgba?\s*\(\s*(\d+)[\s,]+(\d+)[\s,]+(\d+)/);
            if (!m) return false;
            const r = +m[1], g = +m[2], b = +m[3];
            if (Math.max(r, g, b) - Math.min(r, g, b) > 60) return false;
            const avg = (r + g + b) / 3;
            return avg > 75 && avg < 220;
        } catch (e) {
            return false;
        }
    };
    const tryPriceWithRetail = (el) => {
        const raw = (el.textContent || "").replace(/\s+/g, " ").trim();
        if (!/\$[0-9,]+/.test(raw)) return null;
        if (!hasRetailAncestor(el, 7)) return null;
        return pickFirst(raw);
    };
    const roots = [];
    for (const d of document.querySelectorAll('[role="dialog"]')) {
        if (d.getClientRects().length === 0) continue;
        if (isLocModal(d)) continue;
        roots.push(d);
    }
    if (roots.length === 0) roots.push(document.body);
    const walkSelector = "span, div, p, li, em, small, s, del, strike, label, a, b, i, u, font, h2, h3, h4";
    for (const root of roots) {
        for (const el of root.querySelectorAll("s, del, strike")) {
            if (el.getClientRects().length === 0) continue;
            const v = tryPriceWithRetail(el);
            if (v != null) return v;
        }
        for (const el of root.querySelectorAll(walkSelector)) {
            if (el.getClientRects().length === 0) continue;
            if (!isStruckOrAncestor(el, 4)) continue;
            const v = tryPriceWithRetail(el);
            if (v != null) return v;
        }
        for (const el of root.querySelectorAll(walkSelector)) {
            if (el.getClientRects().length === 0) continue;
            if (!isGrayish(el)) continue;
            const raw = (el.textContent || "").replace(/\s+/g, " ").trim();
            if (raw.length > 36) continue;
            if (!hasRetailAncestor(el, 7)) continue;
            const v = pickFirst(raw);
            if (v != null) return v;
        }
    }
    return null;
}
"""


async def try_extract_strikethrough_retail_in_open_modal(page) -> float | None:
    """List / retail price from the open coupon: <s>/<del>, CSS line-through, or short gray $ text near a retail label."""
    try:
        v = await page.evaluate(_STRIKETHROUGH_RETAIL_IN_MODAL_JS)
        if v is not None and isinstance(v, (int, float)) and v > 0:
            return float(v)
    except Exception:
        pass
    return None


async def maybe_extract_retail_price(
    page, row, physical_pickup: bool
) -> tuple[float | None, str | None]:
    """
    (retail price or None, optional plain-text dump of the open price/coupon dialog).
    The dialog text is only captured for physical_pickup (after row click), before the popup is closed.
    """
    # Scroll so sticky footers and bottom price rows (retail) are in the layout viewport.
    try:
        await row.scroll_into_view_if_needed()
        await asyncio.sleep(0.2)
    except Exception:
        pass

    if not physical_pickup:
        try:
            blob = (await row.inner_text(timeout=3000)) or ""
            got = extract_retail_from_blurb(blob)
            if got is not None:
                return got, None
        except Exception:
            pass

        retail_text = await first_visible_text(
            row,
            [
                'text=/Retail:?\\s*\\$[0-9]/i',
                'text=/was\\s*\\$[0-9]/i',
                'text=/Est\\.?\\s*retail/i',
                'text=/Cash\\s*price/i',
                'text=/Original\\s*price/i',
                'text=/List\\s*price/i',
                '[data-qa*="retail"]',
                '[data-testid*="retail"]',
            ],
            timeout_ms=2000,
        )
        if retail_text:
            parsed = extract_price_with_regex(retail_text)
            if parsed is not None:
                return parsed, None
        return None, None

    await clear_known_interstitials(page)
    overlay_ok = await clear_savings_tip_overlay(page)
    if not overlay_ok:
        for _ in range(2):
            try:
                await page.keyboard.press("Escape")
                await asyncio.sleep(0.35)
            except Exception:
                break
        await clear_savings_tip_overlay(page)
    # Still attempt detail click: force helps when a translucent overlay claims visibility but row is tappable.
    use_force = not overlay_ok

    result: float | None = None
    modal_debug: str | None = None
    try:
        try:
            await row.scroll_into_view_if_needed()
            await asyncio.sleep(0.15)
        except Exception:
            pass
        try:
            await row.click(timeout=3000, force=use_force)
        except Exception:
            await row.click(timeout=3000, force=True)
        await asyncio.sleep(random.uniform(1.0, 2.0))

        st = await try_extract_strikethrough_retail_in_open_modal(page)
        if st is not None:
            result = st
        else:
            main_retail_text = await first_visible_text(
                page,
                [
                    'text=/Retail:?\\s*\\$[0-9]/i',
                    'text=/Retail:?/i',
                    '[data-qa*="retail"]',
                    '[data-testid*="retail"]',
                ],
                timeout_ms=3500,
            )
            if main_retail_text:
                p1 = extract_retail_from_blurb(main_retail_text)
                p2 = extract_price_with_regex(main_retail_text) if p1 is None else None
                if p1 is not None:
                    result = p1
                elif p2 is not None:
                    result = p2

        if result is None:
            for loc in (
                page.get_by_text(
                    re.compile(
                        rf"Retail:?\s*\${_RETAIL_DOLLAR_AMT}\b", re.I
                    )
                ),
                page.get_by_text(re.compile(r"Est\.?\s*retail:?\s*\$", re.I)),
                page.get_by_text(re.compile(r"Typical\s+retail:?\s*\$", re.I)),
            ):
                try:
                    if await loc.count() == 0:
                        continue
                    t = (await loc.first.inner_text(timeout=2000)).strip()
                    if t:
                        parsed = extract_retail_from_blurb(t) or extract_price_with_regex(
                            t
                        )
                        if parsed is not None:
                            result = parsed
                            break
                except Exception:
                    continue
    except Exception:
        result = None
    finally:
        if physical_pickup:
            try:
                t = await snapshot_retail_debug_modal_text(page)
                if t and t.strip():
                    modal_debug = t[:12000]
            except Exception:
                pass
        try:
            await close_price_detail_popup(page)
        except Exception:
            pass
    return result, modal_debug

async def scrape_drug_data(page, drug_name, zip_code):
    """Scrapes discoverable pharmacy options for a drug/zip."""
    t0 = time.monotonic()
    print(f"  Scraping {drug_name} in {zip_code}...")
    
    await check_and_handle_captcha(page)
    await clear_px_captcha_if_blocking(page, max_passes=8)
    print("    Phase: preflight (captcha / blocking iframe) done")

    location_trigger = location_trigger_locator(page)
    if await location_trigger.count() == 0:
        await asyncio.sleep(2.0)
        await check_and_handle_captcha(page)
        await clear_px_captcha_if_blocking(page, max_passes=8)
        location_trigger = location_trigger_locator(page)
    if await location_trigger.count() == 0:
        location_trigger = page.locator(
            "[aria-label*='Current location'], [aria-label*='Set your location'], "
            "[data-qa='locationModalTrigger'], [data-testid='locationModalTrigger']"
        )
    if await location_trigger.count() == 0:
        raise LocationTriggerError("Location trigger missing after captcha check and short retry.")
    print("    Phase: open location, set zip, load results...")

    modal = location_modal(page)
    modal_opened = False
    last_modal_error = None
    for modal_try in range(2):
        try:
            await clear_px_captcha_if_blocking(page, max_passes=8)
            await location_trigger.first.click(timeout=15000)
            await expect(modal).to_be_visible(timeout=7000)
            modal_opened = True
            break
        except Exception as exc:
            last_modal_error = exc
            await check_and_handle_captcha(page)
            await clear_px_captcha_if_blocking(page, max_passes=8)
            try:
                await location_trigger.first.click(timeout=30000, force=True)
                await expect(modal).to_be_visible(timeout=10000)
                modal_opened = True
                break
            except Exception as retry_exc:
                last_modal_error = retry_exc
                if modal_try == 0:
                    await asyncio.sleep(1.5)
                    location_trigger = location_trigger_locator(page)
                    continue
    if not modal_opened:
        raise LocationModalError("Could not open location modal.") from last_modal_error
    await asyncio.sleep(0.4)
    
    try:
        zip_input = await resolve_zip_input_locator(page, modal)
    except Exception as exc:
        raise ZipInputError("No zip/location input found in location modal.") from exc
    await zip_input.click(force=True)
    await zip_input.press("Meta+A")
    await zip_input.press("Backspace")
    
    lo, hi = ZIP_TYPE_CHAR_DELAY_MS
    await zip_input.type(zip_code, delay=random.randint(lo, hi))
    # Some GoodRx variants require committing the combobox value.
    try:
        await zip_input.press("Enter")
    except Exception:
        pass
    await asyncio.sleep(0.6)
    
    set_button = modal.get_by_role(
        "button",
        name=re.compile(
            r"Set location|Save|Update|Apply|Use this location|Use selected location|Confirm|Done|"
            r"See prices|View prices|See coupons|Continue|Search",
            re.I,
        ),
    )
    if await set_button.count() == 0:
        set_button = page.get_by_role(
            "button",
            name=re.compile(
                r"Set location|Save|Update|Apply|Use this location|Use selected location|Confirm|Done|"
                r"See prices|View prices|See coupons|Continue|Search",
                re.I,
            ),
        )
    if await set_button.count() == 0:
        # No explicit submit action found: try committing typed location via options/Enter.
        option = modal.get_by_role("option")
        if await option.count() > 0:
            try:
                await option.first.click(timeout=3000)
                await asyncio.sleep(0.3)
            except Exception:
                pass
        try:
            await zip_input.press("Enter")
            await asyncio.sleep(0.5)
        except Exception:
            pass
    else:
        await set_button.first.click()

    modal_closed = False
    try:
        await expect(modal).to_be_hidden(timeout=10000)
        modal_closed = True
    except Exception:
        # Retry by selecting first suggestion/option then resubmitting.
        option = modal.get_by_role("option")
        if await option.count() > 0:
            await option.first.click()
            await asyncio.sleep(0.3)
        if await set_button.count() > 0:
            try:
                await set_button.first.click(force=True)
            except Exception:
                pass
        try:
            await zip_input.press("Enter")
        except Exception:
            pass
        try:
            await expect(modal).to_be_hidden(timeout=8000)
            modal_closed = True
        except Exception:
            modal_closed = False

    if not modal_closed:
        # Treat visible pharmacy rows as implicit success when modal state is flaky.
        rows_probe = local_pharmacy_price_rows_locator(page)
        try:
            await expect(rows_probe.first).to_be_visible(timeout=5000)
        except Exception as exc:
            raise LocationModalError("Location modal stayed open and rows did not appear.") from exc
    await check_and_handle_captcha(page)
    await clear_known_interstitials(page)
    
    price_rows_locator = local_pharmacy_price_rows_locator(page)
    try:
        await expect(price_rows_locator.first).to_be_visible(timeout=25000)
    except Exception as exc:
        await save_dom_artifact(page, drug_name, zip_code, "results_not_found")
        raise ResultsNotFoundError("Pharmacy rows did not appear after setting location.") from exc
    lo, hi = DELAY_POST_RESULTS_VISIBLE_S
    await asyncio.sleep(random.uniform(lo, hi))
    print("    Phase: result rows on page; reading pharmacy rows")
    print("    [DEBUG] Priming lazy-loaded rows...")
    last_row_count = -1
    stable_checks = 0
    scrolled_down = False
    for step in range(6):
        try:
            row_count = await price_rows_locator.count()
        except Exception:
            row_count = 0
        print(f"    [DEBUG] Lazy-load primer step {step + 1}: row locator count={row_count}")
        if row_count == last_row_count:
            stable_checks += 1
        else:
            stable_checks = 0
        if stable_checks >= 2 and row_count > 0:
            break
        try:
            await page.mouse.wheel(0, 900)
            scrolled_down = True
        except Exception:
            pass
        await asyncio.sleep(random.uniform(0.4, 0.7))
        last_row_count = row_count
    if scrolled_down:
        try:
            await page.mouse.wheel(0, -1200)
        except Exception:
            pass
        await asyncio.sleep(0.8)
    
    results_count = 0
    row_skip_count = 0
    row_skip_artifact_saved = False
    candidate_count = 0
    duplicate_row_skips = 0
    seen_row_keys: set = set()
    seen_row_indices: set[int] = set()
    skipped_mail_order_count = 0
    retail_count = 0
    for scan_pass in range(MAX_ROW_SCAN_PASSES):
        try:
            dom_rows = await price_rows_locator.count()
        except Exception:
            dom_rows = 0
        max_idx_this_pass = min(dom_rows, MAX_ROW_CANDIDATES * (scan_pass + 1))
        if max_idx_this_pass <= 0:
            continue

        for i in range(max_idx_this_pass):
            if i in seen_row_indices:
                continue
            seen_row_indices.add(i)
            row = price_rows_locator.nth(i)
            try:
                await clear_known_interstitials(page)
                if not await is_row_candidate(row):
                    continue
                candidate_count += 1
            except Exception:
                continue

            try:
                pharmacy_name = await extract_pharmacy_name(row)
                if not pharmacy_name:
                    raise ValueError("Pharmacy name not found in row.")
                if is_mail_order_pharmacy(pharmacy_name):
                    skipped_mail_order_count += 1
                    print(f"    [DEBUG] Skipping Delivery/Mail Order: {pharmacy_name.strip()}")
                    continue

                option_type = "retail_pickup"
                print(f"    [DEBUG] Found Retail Pharmacy Candidate: {pharmacy_name.strip()}")

                goodrx_price_text = await first_visible_text(
                    row,
                    [
                        '[data-qa="seller-price"]',
                        '[data-qa="price"]',
                        '[data-testid*="price"]',
                        '[class*="price"]',
                        'text=/\\$\\s?[0-9]/',
                    ],
                )
                if not goodrx_price_text:
                    row_text = await row.inner_text(timeout=2500)
                    goodrx_price_text = row_text
                print(f"    [DEBUG] Raw GoodRx Text: '{goodrx_price_text}'")
                if not goodrx_price_text:
                    raise ValueError("GoodRx price not found in row.")
                goodrx_price = extract_price_with_regex(goodrx_price_text)
                print(f"    [DEBUG] Regex Extracted GoodRx Price: {goodrx_price}")
                if goodrx_price is None:
                    print("    [DEBUG] FAIL: GoodRx price not found via regex. Skipping row.")
                    raise ValueError("GoodRx price not found via regex.")

                rid = row_identity_key(pharmacy_name, goodrx_price)
                if rid in seen_row_keys:
                    duplicate_row_skips += 1
                    continue
                seen_row_keys.add(rid)

                # Base fields are primary; retail enrichment is best effort.
                retail_raw, retail_modal_debug = await maybe_extract_retail_price(
                    page,
                    row,
                    physical_pickup=True,
                )
                if retail_raw is None:
                    retail_price = None
                elif isinstance(retail_raw, (int, float)):
                    retail_price = float(retail_raw)
                else:
                    retail_price = extract_price_with_regex(str(retail_raw))

                retail_flag = ""
                if (
                    retail_price is not None
                    and goodrx_price is not None
                    and retail_price < goodrx_price - 1e-6
                ):
                    retail_flag = "suspicious_retail_lt_goodrx"
                    print(
                        f"    [Retail check] retail ${retail_price:.2f} < "
                        f"GoodRx ${goodrx_price:.2f} — saving text + screenshot"
                    )
                    await log_retail_suspicious_artifact(
                        page,
                        drug_name,
                        zip_code,
                        pharmacy_name.strip(),
                        goodrx_price,
                        retail_price,
                        retail_modal_debug or "",
                    )

                await save_to_csv({
                    "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Zip_Code": zip_code,
                    "Drug_Name": drug_name,
                    "Pharmacy_Name": pharmacy_name.strip(),
                    "Option_Type": option_type,
                    "Retail_Price": retail_price or "N/A",
                    "Retail_Flag": retail_flag,
                    "GoodRx_Price": goodrx_price,
                })
                results_count += 1
                retail_count += 1
                nm = pharmacy_name.strip()
                if len(nm) > 50:
                    nm = nm[:47] + "..."
                rpv = f"${retail_price:.2f}" if retail_price is not None else "N/A"
                print(
                    f"    Saved row: {nm} | GoodRx ${goodrx_price:.2f} | retail {rpv}"
                )
            except Exception as row_exc:
                row_skip_count += 1
                print(f"    Row parse skipped ({type(row_exc).__name__}): {row_exc}")
                if row_skip_count >= MAX_ROW_SKIP_BEFORE_ARTIFACT and not row_skip_artifact_saved:
                    await save_dom_artifact(page, drug_name, zip_code, "repeated_row_skip")
                    row_skip_artifact_saved = True
                continue

        if scan_pass >= (MAX_ROW_SCAN_PASSES - 1):
            break
        try:
            await clear_known_interstitials(page)
            if max_idx_this_pass > 0:
                await price_rows_locator.nth(max_idx_this_pass - 1).scroll_into_view_if_needed()
            await asyncio.sleep(random.uniform(0.5, 1.0))
        except Exception:
            try:
                await page.mouse.wheel(0, 1000)
                await asyncio.sleep(random.uniform(0.5, 1.0))
            except Exception:
                pass
    if candidate_count > 0 or results_count > 0:
        # uniques = DOM candidates minus duplicate cards; good = uniques that did not throw.
        unique_candidates = candidate_count - duplicate_row_skips
        good = unique_candidates - row_skip_count
        dup_part = f"; dup DOM rows skipped: {duplicate_row_skips}" if duplicate_row_skips else ""
        denom = unique_candidates if unique_candidates > 0 else (candidate_count or 1)
        print(
            f"    Rows written: {results_count}; "
            f"candidate rows OK: {good}/{denom} ({row_skip_count} parse failures); "
            f"mail-order skipped: {skipped_mail_order_count}; retail: {retail_count}{dup_part}"
        )
    else:
        print("    Wrote 0 rows (no row candidates, or all candidates failed to parse).")
    elapsed = time.monotonic() - t0
    print(
        f"    Timing: {elapsed:.1f}s this zip (asyncio cap {ZIP_ATTEMPT_BUDGET_SEC}s) | url={page.url}"
    )

async def main():
    global _run_csv_path
    if not os.path.exists(SCREENSHOT_DIR): os.makedirs(SCREENSHOT_DIR)
    if not os.path.exists(USER_DATA_DIR): os.makedirs(USER_DATA_DIR)
    if not os.path.exists(DOM_ARTIFACT_DIR): os.makedirs(DOM_ARTIFACT_DIR)
    if not os.path.exists(RUNS_DIR):
        os.makedirs(RUNS_DIR)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    _run_csv_path = os.path.join(RUNS_DIR, f"prices_{run_id}.csv")
    print(
        f"Run output (this scrape only, timestamped): {os.path.abspath(_run_csv_path)}"
    )
    with open(_run_csv_path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES).writeheader()
    await cleanup_old_dom_artifacts()

    async with Stealth().use_async(async_playwright()) as p:
        # Use Persistent Context to warm up the session
        context = await p.chromium.launch_persistent_context(
            user_data_dir=os.path.abspath(USER_DATA_DIR),
            headless=False,
            user_agent=USER_AGENT,
            # Force desktop side-by-side layout to avoid mobile accordion UI.
            viewport={"width": 1920, "height": 1080},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized"
            ]
        )
        page = context.pages[0] if context.pages else await context.new_page()
        
        for drug in DRUGS:
            print(f"Starting drug: {drug['name']}")
            try:
                await page.goto("https://www.google.com/", wait_until="domcontentloaded")
                g0, g1 = DELAY_WARMUP_GOOGLE_S
                await asyncio.sleep(random.uniform(g0, g1))
                await page.goto(drug["url"], wait_until="domcontentloaded", timeout=60000)
                d0, d1 = DELAY_POST_DRUG_LOAD_S
                await asyncio.sleep(random.uniform(d0, d1))
                
                for zip_code in ZIP_CODES:
                    zip_completed = False
                    for attempt in range(MAX_ZIP_RETRIES + 1):
                        try:
                            await asyncio.wait_for(
                                scrape_drug_data(page, drug["name"], zip_code),
                                timeout=ZIP_ATTEMPT_BUDGET_SEC,
                            )
                            b0, b1 = DELAY_BETWEEN_ZIPS_S
                            await asyncio.sleep(random.uniform(b0, b1))
                            zip_completed = True
                            break
                        except Exception as e:
                            last_attempt = attempt == MAX_ZIP_RETRIES
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            await page.screenshot(path=os.path.join(SCREENSHOT_DIR, f"fail_{drug['name']}_{zip_code}_{ts}.png"))
                            print(fail_step(f"zip_run_attempt_{attempt + 1}", e, drug["name"], zip_code))
                            if isinstance(e, TimeoutError):
                                print(
                                    f"    Zip step hit asyncio timeout ({ZIP_ATTEMPT_BUDGET_SEC}s total budget per zip; "
                                    "includes CAPTCHA + location + row scraping)."
                                )
                            await save_dom_artifact(page, drug["name"], zip_code, f"zip_attempt_{attempt + 1}_failure")
                            if last_attempt:
                                break
                            await page.goto(drug["url"], wait_until="domcontentloaded")
                            r0, r1 = DELAY_ON_ZIP_RETRY_S
                            await asyncio.sleep(random.uniform(r0, r1))
                    if not zip_completed:
                        print(f"  Skipping {drug['name']} {zip_code} after {MAX_ZIP_RETRIES + 1} failed attempts.")
            except Exception as e:
                print(f"CRITICAL: {e}")
                
        await context.close()
    print("Scraping complete.")
    if _run_csv_path and os.path.isfile(_run_csv_path):
        ap = os.path.abspath(_run_csv_path)
        try:
            with open(_run_csv_path, encoding="utf-8") as cf:
                row_count = max(sum(1 for _ in cf) - 1, 0)  # minus header
        except OSError:
            row_count = -1
        if row_count >= 0:
            print(f"Run CSV: {ap} ({row_count} data rows)")
        else:
            print(f"Run CSV: {ap}")
    elif _run_csv_path:
        print(f"Run CSV was set but file missing: {_run_csv_path}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
    except asyncio.CancelledError:
        print("Run cancelled.")
