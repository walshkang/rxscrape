import asyncio
import csv
import os
import random
import re
import math
from datetime import datetime
from playwright.async_api import async_playwright, expect
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
        except:
            continue
            
    # 3. If "Before we continue" text is present, wait and retry
    if await page.get_by_text(re.compile(r"Before we continue", re.I)).count() > 0:
        print("    Captcha text detected, waiting for interaction...")
        await asyncio.sleep(4)
        return await check_and_handle_captcha(page)
            
    return False

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
    
    location_trigger = page.get_by_role("button", name=re.compile(r"\d{5}|Set location|Your location", re.I))
    if await location_trigger.count() == 0:
        if not await check_and_handle_captcha(page):
             raise Exception("Location trigger missing and no captcha detected.")
        location_trigger = page.get_by_role("button", name=re.compile(r"\d{5}|Set location|Your location", re.I))

    await location_trigger.click()
    
    modal = page.get_by_role("dialog")
    try:
        await expect(modal).to_be_visible(timeout=7000)
    except:
        await check_and_handle_captcha(page)
        await location_trigger.click()
        await expect(modal).to_be_visible(timeout=10000)
    
    # More robust input locator
    zip_input = page.locator('input[placeholder*="ZIP" i], input[placeholder*="zip" i], input[placeholder*="city" i]')
    await expect(zip_input.first).to_be_visible(timeout=10000)
    
    await zip_input.first.click(force=True)
    await zip_input.first.press("Meta+A")
    await zip_input.first.press("Backspace")
    
    # Natural typing
    for char in zip_code:
        await zip_input.first.type(char, delay=random.randint(150, 400))
        await asyncio.sleep(random.uniform(0.05, 0.1))
    
    set_button = page.get_by_role("button", name=re.compile(r"Set location|Save|Update", re.I))
    await set_button.click()
    await expect(modal).to_be_hidden(timeout=15000)
    await check_and_handle_captcha(page)
    
    price_rows_locator = page.locator('div[data-qa="pharmacy-row"], [class*="priceRow"]')
    await expect(price_rows_locator.first).to_be_visible(timeout=20000)
    await human_delay(3, 5)
    
    results_count = 0
    for i in range(10):
        if results_count >= 3: break
        row = price_rows_locator.nth(i)
        if not await row.is_visible(): continue
            
        try:
            pharmacy_name = await row.locator('h4, span[class*="pharmacyName"], strong').first.inner_text()
            goodrx_price_text = await row.locator('[data-qa="price"], span[class*="price"]').first.inner_text()
            goodrx_price = parse_price(goodrx_price_text)
            
            retail_price = None
            retail_locator = row.locator('text=/Retail/i, text=/was/i')
            if await retail_locator.count() > 0:
                try: retail_price = parse_price(await retail_locator.first.inner_text())
                except: pass
            
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
        except: continue

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
                        print(f"  FAILED: {e}")
                        await page.goto(drug["url"], wait_until="domcontentloaded")
                        await human_delay(5, 8)
            except Exception as e:
                print(f"CRITICAL: {e}")
                
        await context.close()
        print("Scraping complete.")

if __name__ == "__main__":
    asyncio.run(main())
