import asyncio
import csv
import os
import random
import re
import math
from datetime import datetime
from playwright.async_api import async_playwright, expect, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
from scraper_utils import parse_price

# Configuration
DRUGS = [
    {"name": "Atorvastatin", "url": "https://www.goodrx.com/atorvastatin"},
    {"name": "Amoxicillin", "url": "https://www.goodrx.com/amoxicillin"},
    {"name": "Imatinib", "url": "https://www.goodrx.com/imatinib"},
]
ZIP_CODES = ['10012', '90210', '48201', '75024', '57701']
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
CSV_FILE = "national_pharmacy_pricing.csv"
SCREENSHOT_DIR = "error_screenshots"
USER_DATA_DIR = "browser_profile"
LOCATION_TRIGGER_RE = re.compile(r"\d{5}|Set location|Your location|Current location", re.I)


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
        
        # Check if captcha is gone
        if await page.get_by_text(re.compile(r"Before we continue", re.I)).count() == 0:
            print("    Successfully bypassed CAPTCHA.")
            return True
        return False
    except Exception as e:
        print(f"    Error solving button: {e}")
        return False

async def check_and_handle_captcha(page):
    """Checks for captcha and attempts solve if found."""
    # 1. Check main page
    button = page.get_by_role("button", name=re.compile(r"Press & Hold", re.I))
    if await button.count() > 0:
        return await solve_px_captcha_button(page, button.first)
    
    # 2. Check all frames
    for frame in page.frames:
        try:
            button = frame.get_by_role("button", name=re.compile(r"Press & Hold", re.I))
            if await button.count() > 0:
                print(f"    Found button in frame: {frame.url[:40]}...")
                return await solve_px_captcha_button(page, button.first)
        except Exception:
            continue
            
    # 3. If "Before we continue" text is present, wait and retry
    if await page.get_by_text(re.compile(r"Before we continue", re.I)).count() > 0:
        print("    Captcha text detected, waiting for interaction...")
        await asyncio.sleep(4)
        return await check_and_handle_captcha(page)
            
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

async def save_to_csv(data):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Date", "Zip_Code", "Drug_Name", "Pharmacy_Name", "Retail_Price", "GoodRx_Price"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

async def scrape_drug_data(page, drug_name, zip_code):
    """Scrapes top 3 pharmacy prices for a drug/zip."""
    print(f"  Scraping {drug_name} in {zip_code}...")
    
    await check_and_handle_captcha(page)
    await clear_px_captcha_if_blocking(page, max_passes=8)

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
    
    for char in zip_code:
        await zip_input.type(char, delay=random.randint(150, 400))
        await asyncio.sleep(random.uniform(0.05, 0.1))
    # Some GoodRx variants require committing the combobox value.
    try:
        await zip_input.press("Enter")
    except Exception:
        pass
    await asyncio.sleep(0.6)
    
    set_button = modal.get_by_role("button", name=re.compile(
        r"Set location|Save|Update|Apply|Use this location|Confirm|Done", re.I
    ))
    if await set_button.count() == 0:
        set_button = page.get_by_role("button", name=re.compile(
            r"Set location|Save|Update|Apply|Use this location|Confirm|Done", re.I
        ))
    if await set_button.count() == 0:
        set_button = modal.get_by_role("button", name=re.compile(
            r"See prices|Search|Continue|View prices", re.I
        ))
    if await set_button.count() == 0:
        raise LocationModalError("Set location button not found in location modal.")
    await set_button.first.click()
    try:
        await expect(modal).to_be_hidden(timeout=10000)
    except Exception:
        # Retry by selecting first suggestion/option then resubmitting.
        option = modal.get_by_role("option")
        if await option.count() > 0:
            await option.first.click()
            await asyncio.sleep(0.3)
        try:
            await set_button.first.click(force=True)
        except Exception:
            try:
                await zip_input.press("Enter")
            except Exception:
                pass
        await expect(modal).to_be_hidden(timeout=12000)
    await check_and_handle_captcha(page)
    
    price_rows_locator = page.locator(
        'div[data-qa="pharmacy-row"], [class*="priceRow"], '
        '[data-testid*="pharmacy"][data-testid*="row"], [data-qa*="pharmacy"][data-qa*="row"]'
    )
    try:
        await expect(price_rows_locator.first).to_be_visible(timeout=25000)
    except Exception as exc:
        raise ResultsNotFoundError("Pharmacy rows did not appear after setting location.") from exc
    await human_delay(3, 5)
    
    results_count = 0
    for i in range(10):
        if results_count >= 3: break
        row = price_rows_locator.nth(i)
        if not await row.is_visible(): continue
            
        try:
            pharmacy_name = await first_visible_text(
                row,
                [
                    '[data-qa*="pharmacy"] h4',
                    '[data-testid*="pharmacy"] h4',
                    'h4',
                    '[class*="pharmacyName"]',
                    '[data-qa*="pharmacy-name"]',
                    '[data-testid*="pharmacy-name"]',
                    "strong",
                ],
            )
            if not pharmacy_name:
                # Fast fallback: parse row text to avoid 30s stalls.
                row_text = await row.inner_text(timeout=2500)
                pharmacy_name = next(
                    (ln.strip() for ln in row_text.splitlines() if ln.strip() and not re.search(r"\$|coupon|retail|save", ln, re.I)),
                    None,
                )
            if not pharmacy_name:
                raise ValueError("Pharmacy name not found in row.")

            goodrx_price_text = await first_visible_text(
                row,
                [
                    '[data-qa="price"]',
                    '[data-testid*="price"]',
                    '[class*="price"]',
                    'text=/\\$\\s?[0-9]/',
                ],
            )
            if not goodrx_price_text:
                row_text = await row.inner_text(timeout=2500)
                price_match = re.search(r"\$[0-9,]+(?:\.[0-9]{2})?", row_text)
                if price_match:
                    goodrx_price_text = price_match.group(0)
            if not goodrx_price_text:
                raise ValueError("GoodRx price not found in row.")
            goodrx_price = parse_price(goodrx_price_text)
            
            retail_price = None
            retail_locator = row.locator('text=/Retail/i, text=/was/i')
            if await retail_locator.count() > 0:
                try:
                    retail_price = parse_price(await retail_locator.first.inner_text())
                except Exception:
                    retail_price = None
            
            if retail_price is None:
                await row.click()
                await asyncio.sleep(2)
                retail_detail = page.locator(r'text=/Retail:? \$/i')
                if await retail_detail.count() > 0:
                    retail_price = parse_price(await retail_detail.first.inner_text())
            
            await save_to_csv({
                "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Zip_Code": zip_code,
                "Drug_Name": drug_name,
                "Pharmacy_Name": pharmacy_name.strip(),
                "Retail_Price": retail_price or "N/A",
                "GoodRx_Price": goodrx_price
            })
            results_count += 1
        except Exception as row_exc:
            print(f"    Row parse skipped ({type(row_exc).__name__}): {row_exc}")
            continue

async def main():
    if not os.path.exists(SCREENSHOT_DIR): os.makedirs(SCREENSHOT_DIR)
    if not os.path.exists(USER_DATA_DIR): os.makedirs(USER_DATA_DIR)
        
    async with Stealth().use_async(async_playwright()) as p:
        # Use Persistent Context to warm up the session
        context = await p.chromium.launch_persistent_context(
            user_data_dir=os.path.abspath(USER_DATA_DIR),
            headless=False,
            user_agent=USER_AGENT,
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
                await human_delay(1, 2)
                await page.goto(drug["url"], wait_until="domcontentloaded", timeout=60000)
                await human_delay(4, 6)
                
                for zip_code in ZIP_CODES:
                    try:
                        await scrape_drug_data(page, drug["name"], zip_code)
                        await human_delay(5, 10)
                    except Exception as e:
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        await page.screenshot(path=os.path.join(SCREENSHOT_DIR, f"fail_{drug['name']}_{zip_code}_{ts}.png"))
                        print(fail_step("zip_run", e, drug["name"], zip_code))
                        await page.goto(drug["url"], wait_until="domcontentloaded")
                        await human_delay(5, 8)
            except Exception as e:
                print(f"CRITICAL: {e}")
                
        await context.close()
        print("Scraping complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
    except asyncio.CancelledError:
        print("Run cancelled.")
