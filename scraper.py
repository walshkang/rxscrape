import asyncio
import csv
import math
import os
import random
import shutil
import re
import sys
import time
from datetime import datetime, timedelta
from playwright.async_api import (
    async_playwright,
    expect,
    Locator,
    TimeoutError as PlaywrightTimeoutError,
)
from playwright_stealth import Stealth

# Configuration
DRUGS = [
    {"name": "Atorvastatin", "url": "https://www.goodrx.com/atorvastatin"},
    {"name": "Amoxicillin", "url": "https://www.goodrx.com/amoxicillin"},
    {"name": "Imatinib", "url": "https://www.goodrx.com/imatinib"},
]
ZIP_CODES = [
    "10012",
    "90210",
    "48201",
    "75024",
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
LOCATION_TRIGGER_RE = re.compile(
    r"\d{5}|Set location|Change location|Edit location|Your location|Current location|"
    r"^Location\b",
    re.I,
)
# Full-page PerimeterX interstitial (see error_dom_artifacts; no GoodRx app shell on this document)
RE_GOODRX_PX_ACCESS_DENIED_TITLE = re.compile(
    r"access to this page has been denied", re.I
)
MAX_ROW_CANDIDATES = 30
MAX_ROW_SCAN_PASSES = 4
MAX_ROW_SKIP_BEFORE_ARTIFACT = 4
MAX_ZIP_RETRIES = 2
# Must cover: PerimeterX (several 8–12s holds + delays), location modal, human_delay,
# and 3× retail row enrichment. 90s routinely trips asyncio.wait_for() mid-scrape.
ZIP_ATTEMPT_BUDGET_SEC = 360
DOM_ARTIFACT_RETENTION_DAYS = 14
CAPTCHA_MAX_SOLVE_ATTEMPTS = 4
CAPTCHA_RECHECK_WAIT_SEC = 2.0
# Poll for the human-challenge iframe to inject a real control (see error_dom: iframe display:none)
CAPTCHA_INJECT_WAIT_SEC = 55
# Playwright: mousedown to mouseup delay for "Press & hold" (ms)
CAPTCHA_HOLD_MS_MIN = 8500
CAPTCHA_HOLD_MS_MAX = 12000
# Each `clear_px_captcha_if_blocking` pass can invoke a full 3-hold `check_and_handle_captcha`
# (~60s+). High pass counts can burn the whole ZIP_ATTEMPT_BUDGET_SEC before location/rows.
CAPTCHA_CLEAR_MAX_PASSES = 4
# After Press & Hold, PX may set cookies and reload before the GoodRx SPA reappears (Scenario A: nav race)
POST_CAPTCHA_SETTLE_S = (3.0, 6.0)
POST_CAPTCHA_APP_SHELL_POLL_MAX_S = 12.0
# GoodRx splits local pickup vs mail-order: retail is under the main list; "Home delivery" rows
# live in a separate region (see data-qa on the mail-order container in the live DOM).
MAIL_ORDER_PHARMACY_ROWS_CONTAINER_QA = "mail-order-pharmacy-rows-container"
# Tunable pacing (seconds, min–max). Shorter = faster; too low may look bot-like or race the UI.
DELAY_POST_DRUG_LOAD_S = (2.0, 3.5)  # after drug URL; was ~4–6s
DELAY_POST_RESULTS_VISIBLE_S = (1.0, 1.8)  # after pharmacy rows first visible; was ~3–5s
DELAY_BETWEEN_ZIPS_S = (5.0, 15.0)  # human-like pacing between ZIPs (reduces velocity flags)
DELAY_WARMUP_GOOGLE_S = (0.5, 1.0)  # after google hop; was ~1–2s
DELAY_ON_ZIP_RETRY_S = (2.0, 3.5)  # reload after failed zip; was ~3–5s
ZIP_TYPE_CHAR_DELAY_MS = (35, 75)  # one-shot type(); was 150–400ms per char
# After this many full ZIPs (any outcome), add an extra long sleep (static IP / cooldown)
ZIP_ITERATIONS_BEFORE_COOLDOWN = 5
IP_COOLDOWN_BETWEEN_ZIP_BLOCKS_S = (45.0, 120.0)
# Wipe `USER_DATA_DIR` after this many times we exhaust ZIP retries in a row (stale PerimeterX state)
CONSECUTIVE_ZIP_GIVEUPS_BEFORE_PROFILE_RESET = 3
# Optional: `http://user:pass@host:port` or `http://host:port` (rotating residential provider URL)
# Also honors `HTTP_PROXY` if `GOODRX_PLAYWRIGHT_PROXY` is unset.
GOODRX_PLAYWRIGHT_PROXY = os.environ.get("GOODRX_PLAYWRIGHT_PROXY", "").strip()
GOODRX_MANUAL_PERIMETERX = os.environ.get("GOODRX_MANUAL_PERIMETERX", "").strip().lower()
# In-app GoodRx embed (overlays the SPA)
PX_CAPTCHA_IFRAME = "iframe#px-captcha-modal"
# Full-page "Access to this page has been denied" / px interstitial (see error_dom_artifacts)
PX_CAPTCHA_WRAPPER = "#px-captcha-wrapper"
# Challenge mounts here; the nested HVF iframe is often `display:none` in HTML while this box lays out
PX_WRAPPER_INNER = f"{PX_CAPTCHA_WRAPPER} #px-captcha"
PX_HUMAN_CHALLENGE_IFRAME = 'iframe[title="Human verification challenge"]'
# Static fallback when captcha.js fails (network / ad-block); do not click through it.
PX_CAPTCHA_ERROR_CONTAINER = "div.px-captcha-error-container"
_RE_PX_STATIC_NET_ADBLOCK = re.compile(
    r"Please check your internet connection.*ad[\s-]*blocker|ad[\s-]*blocker.*internet connection",
    re.I | re.S,
)


class ScrapeError(Exception):
    pass


class CaptchaUnresolvedError(ScrapeError):
    """PerimeterX challenge still present after the configured solve/ wait budget."""

    pass


class CaptchaScriptBlockedError(ScrapeError):
    """captcha.js did not load; PerimeterX rendered the static error UI instead of the real challenge."""

    pass


class ManualCaptchaPromptUnavailableError(ScrapeError):
    """Manual PerimeterX mode requested but stdin is not interactive (TTY)."""

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


def nuke_browser_profile_dir() -> None:
    """Remove persistent Chromium profile (cookies / storage). Context must be closed first."""
    path = os.path.abspath(USER_DATA_DIR)
    if os.path.isdir(path):
        try:
            shutil.rmtree(path)
        except OSError as e:
            print(f"    [profile] could not remove {path}: {e}")


def _playwright_proxy_config() -> dict | None:
    for key in (GOODRX_PLAYWRIGHT_PROXY, os.environ.get("HTTP_PROXY", "").strip()):
        if not key:
            continue
        return {"server": key if "://" in key else f"http://{key}"}
    return None


def location_trigger_locator(page):
    # Prefer semantic button matching, then aria-label fallbacks.
    return page.get_by_role("button", name=LOCATION_TRIGGER_RE)


def location_triggers_combined_locator(page) -> Locator:
    """
    GoodRx has used role=button, data-testid, and data-qa for the control that opens
    the ZIP / location combobox. Combine so we can match any of them.
    """
    return (
        page.get_by_role("button", name=LOCATION_TRIGGER_RE)
        .or_(page.get_by_test_id("locationModalTrigger"))
        .or_(
            page.locator(
                "[data-qa='locationModalTrigger'], [data-testid='locationModalTrigger'], "
                "[aria-label*='Current location'], [aria-label*='Set your location']"
            )
        )
    )


async def is_goodrx_px_access_denial_page(page) -> bool:
    """True when the tab title is the full-page PerimeterX denial (no location UI in DOM)."""
    try:
        t = (await page.title() or "").strip()
    except Exception:
        t = ""
    return bool(t and RE_GOODRX_PX_ACCESS_DENIED_TITLE.search(t))


async def _navigate_drug_url_and_rerun_captcha(
    page, drug_url: str, reason: str
) -> None:
    """Hard navigation back to the drug page when still on a denial shell or the app has not mounted."""
    print(f"    Recovery ({reason}): navigating to {drug_url!r}…")
    await page.goto(drug_url, wait_until="domcontentloaded", timeout=60000)
    d0, d1 = DELAY_POST_DRUG_LOAD_S
    await asyncio.sleep(random.uniform(d0, d1))
    await check_and_handle_captcha(page)
    await clear_px_captcha_if_blocking(page, max_passes=CAPTCHA_CLEAR_MAX_PASSES)


async def _wait_for_combined_location_trigger(
    page, timeout_s: float, step_s: float = 0.45
) -> bool:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        loc = location_triggers_combined_locator(page)
        if await loc.count() > 0:
            return True
        await asyncio.sleep(step_s)
    return (await location_triggers_combined_locator(page).count()) > 0


async def _settle_and_wait_for_goodrx_shell_after_captcha_solve(page) -> None:
    """
    Scenario A: cookies / redirect can take a few seconds after a successful hold. Avoid
    racing the next step (e.g. location trigger) on a mid-navigation document.
    """
    lo, hi = POST_CAPTCHA_SETTLE_S
    await asyncio.sleep(random.uniform(lo, hi))
    t0 = time.monotonic()
    while time.monotonic() - t0 < POST_CAPTCHA_APP_SHELL_POLL_MAX_S:
        if not await is_goodrx_px_access_denial_page(page):
            return
        if await location_triggers_combined_locator(page).count() > 0:
            return
        await asyncio.sleep(0.4)
    if await is_goodrx_px_access_denial_page(page):
        print(
            "    Post-captcha: still on access-denial title after settle window — "
            "likely stuck/blocked (Scenario B) or very slow handoff; next steps may use recovery."
        )


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

def _bezier_slight_overshoot(t: float) -> float:
    """Smoothstep: even velocity along the Bezier (avoids machine-uniform t)."""
    t = min(1.0, max(0.0, t))
    return t * t * (3.0 - 2.0 * t)


async def bezier_mouse_move(page, end_x, end_y):
    """Smooth quadratic Bezier to target; small control-point variance (avoids 'mathy' high-frequency paths)."""
    start_x, start_y = random.randint(20, 180), random.randint(20, 120)
    span = max(abs(end_x - start_x), abs(end_y - start_y), 80.0)
    wobble = min(55.0, span * 0.12)
    mid_x, mid_y = (start_x + end_x) / 2, (start_y + end_y) / 2
    control_x = mid_x + random.uniform(-wobble, wobble)
    control_y = mid_y + random.uniform(-wobble, wobble)

    steps = random.randint(22, 32)
    for i in range(steps + 1):
        t = _bezier_slight_overshoot(i / steps)
        x = (1 - t) ** 2 * start_x + 2 * (1 - t) * t * control_x + t**2 * end_x
        y = (1 - t) ** 2 * start_y + 2 * (1 - t) * t * control_y + t**2 * end_y
        if i and i < steps:
            x += random.uniform(-0.8, 0.8)
            y += random.uniform(-0.8, 0.8)
        await page.mouse.move(x, y)
        if i % 7 == 0:
            await asyncio.sleep(random.uniform(0.003, 0.012))


async def px_pointer_tremor_while_pressed(
    page,
    origin_x: float,
    origin_y: float,
    hold_ms: int,
    *,
    jitter_max: float = 1.85,
) -> None:
    """
    Mousedown, then many tiny mousemove events while the button is down. PerimeterX
    (and similar) use pointer telemetry; a perfectly static press for many seconds
    is an easy bot signal. Humans drift 1–3 px; we jitter near the origin for hold_ms.
    Smaller jitter_max on proxy/wrapper points keeps the pointer on a narrow hotspot.
    """
    t_end = time.monotonic() + (hold_ms / 1000.0)
    await page.mouse.move(origin_x, origin_y)
    await page.mouse.down()
    while time.monotonic() < t_end - 0.0005:
        jx = float(origin_x) + random.uniform(-jitter_max, jitter_max)
        jy = float(origin_y) + random.uniform(-jitter_max, jitter_max)
        await page.mouse.move(jx, jy, steps=1)
        rem = t_end - time.monotonic()
        if rem <= 0:
            break
        await asyncio.sleep(min(random.uniform(0.07, 0.35), rem))
    rem = t_end - time.monotonic()
    if rem > 0:
        await asyncio.sleep(rem)
    await page.mouse.up()


async def _px_captcha_static_error_ui_present(page) -> bool:
    """True when PX painted the script-load / ad-block static UI instead of the real widget."""
    try:
        loc = page.locator(PX_CAPTCHA_ERROR_CONTAINER)
        if await loc.count() > 0 and await loc.first.is_visible():
            return True
    except Exception:
        pass
    try:
        dead = page.get_by_text(_RE_PX_STATIC_NET_ADBLOCK)
        if await dead.count() > 0 and await dead.first.is_visible():
            return True
    except Exception:
        pass
    return False


async def raise_if_px_captcha_script_blocked(page) -> None:
    if await _px_captcha_static_error_ui_present(page):
        raise CaptchaScriptBlockedError(
            "PerimeterX captcha.js did not load (px-captcha-error-container or static "
            "connection/ad-blocker copy). Allow *.px-cloud.net and GoodRx XHR; do not use route aborts that block them."
        )


async def _prepare_perimeterx_page_for_challenge(page) -> None:
    """
    Wait for DOM, then up to CAPTCHA_INJECT_WAIT_SEC for the real challenge shell to mount.
    Do not use networkidle (SPA + XHR can prevent it; captcha needs third-party script completion).
    """
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=20000)
    except Exception:
        pass
    deadline = time.monotonic() + CAPTCHA_INJECT_WAIT_SEC
    while time.monotonic() < deadline:
        await raise_if_px_captcha_script_blocked(page)
        for sel in (PX_CAPTCHA_IFRAME, "div#px-captcha", PX_CAPTCHA_WRAPPER):
            try:
                loc = page.locator(sel)
                if await loc.count() == 0:
                    continue
                await loc.first.wait_for(state="attached", timeout=2000)
                return
            except Exception:
                continue
        await asyncio.sleep(0.4)


async def _await_fullpage_px_inner_laid_out(page, max_s: float = 15.0) -> None:
    """
    Full-page PerimeterX HTML (error_dom_artifacts) often has the HVF iframe with
    display:none while div#px-captcha still gets a real layout. Wait until that
    inner box has a usable box so page.mouse fallbacks can target it.
    """
    try:
        if await page.locator(PX_CAPTCHA_WRAPPER).count() == 0:
            return
    except Exception:
        return
    inner = page.locator(PX_WRAPPER_INNER).first
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_s:
        await raise_if_px_captcha_script_blocked(page)
        try:
            if await inner.count() == 0:
                await asyncio.sleep(0.3)
                continue
            ok = await inner.evaluate(
                r"""(el) => {
  const r = el.getBoundingClientRect();
  return r.width > 1.5 && r.height > 1.5;
}"""
            )
        except Exception:
            await asyncio.sleep(0.3)
            continue
        if ok:
            return
        await asyncio.sleep(0.3)


async def _px_fullpage_interstitial_dom_centers(
    page,
) -> list[tuple[float, float, str]]:
    """
    Main-document hold point from the full-page challenge shell (#px-captcha-wrapper).
    When the nested HVF iframe has no rect (display:none / 0×0), the slot #px-captcha
    or .px-captcha-container often still has a real getBoundingClientRect — see
    error_dom_artifacts (Atorvastatin_10001_*).
    """
    try:
        rows = await page.evaluate(
            r"""() => {
  const wrap = document.querySelector("#px-captcha-wrapper");
  if (!wrap) {
    return [];
  }
  const out = [];
  const add = (el, label, yFrac) => {
    if (!el || typeof el.getBoundingClientRect !== "function") return;
    const r = el.getBoundingClientRect();
    if (!r || r.width < 2 || r.height < 2) return;
    out.push({
      left: r.left, top: r.top, w: r.width, h: r.height,
      yFrac: yFrac, label: label
    });
  };
  // Press-hold sits in the lower part of the inner slot
  add(wrap.querySelector("#px-captcha"), "full-page #px-captcha (under wrapper)", 0.78);
  add(wrap.querySelector(".px-captcha-container"), "full-page .px-captcha-container", 0.56);
  return out;
}"""
        )
    except Exception:
        return []
    if not isinstance(rows, list):
        return []
    out: list[tuple[float, float, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        w, h = row.get("w", 0) or 0, row.get("h", 0) or 0
        if w < 2 or h < 2:
            continue
        yf = float(row.get("yFrac", 0.65) or 0.65)
        cx = (row.get("left", 0) or 0) + w * 0.5
        cy = (row.get("top", 0) or 0) + h * yf
        out.append((cx, cy, row.get("label", "full-page px shell")))
    return out


async def _poll_until_captcha_solved_or_timeout(
    page, max_wait_s: float, step_s: float = 0.5
) -> bool:
    t0 = time.monotonic()
    while time.monotonic() - t0 < max_wait_s:
        if await _pulsing_captcha_solved(page):
            return True
        await asyncio.sleep(step_s)
    return await _pulsing_captcha_solved(page)


# Map the center of the CTA to top-page viewport (page.mouse) even when the node is in nested iframes
# and getBoundingClientRect in the child would otherwise be in the wrong coordinate space.
# PX <p> labels often return 0×0 from getBoundingClientRect; use getClientRects / offset* before giving up.
_CAPTCHA_CTA_TO_TOPViewport_JS = r"""
(el) => {
  function layoutElement(n) {
    if (!n) return null;
    // Playwright can resolve a text node; only Elements have getBoundingClientRect/offset* per spec
    if (n.nodeType !== 1) {
      return n.parentElement || null;
    }
    return n;
  }
  function bestRect(n) {
    n = layoutElement(n);
    if (!n) return null;
    const g = typeof n.getBoundingClientRect === "function" ? n.getBoundingClientRect() : null;
    if (g && g.width > 0.25 && g.height > 0.25) return { n, r: g };
    const crs = typeof n.getClientRects === "function" ? n.getClientRects() : null;
    for (let i = 0; i < (crs ? crs.length : 0); i++) {
      const c = crs[i];
      if (c && c.width > 0.25 && c.height > 0.25) return { n, r: c };
    }
    const ow = n.offsetWidth || 0, oh = n.offsetHeight || 0;
    if (g && ow > 0.5 && oh > 0.5) {
      return {
        n,
        r: { left: g.left, top: g.top, width: ow, height: oh, right: g.left + ow, bottom: g.top + oh },
      };
    }
    return null;
  }
  function hit(n) {
    for (let i = 0; i < 10 && n; i++) {
      const br = bestRect(n);
      if (br) return br;
      n = n.parentElement;
    }
    return null;
  }
  let t = hit(el);
  if (!t) {
    t = bestRect(el);
  }
  if (!t) {
    const d = el && el.ownerDocument;
    if (d) {
      for (const root of [d.getElementById("px-captcha"), d.body, d.documentElement].filter(
        Boolean
      )) {
        t = bestRect(root);
        if (t) break;
      }
    }
  }
  if (!t) {
    return { ok: false, reason: "noLayoutBox" };
  }
  const r0 = t.r;
  if (!r0) {
    return { ok: false, reason: "noLayoutBox" };
  }
  let x = (r0.left || 0) + (Number(r0.width) || 0) * 0.5;
  let y = (r0.top || 0) + (Number(r0.height) || 0) * 0.5;
  let w = t.n.ownerDocument && t.n.ownerDocument.defaultView;
  while (w && w !== w.top) {
    const f = w.frameElement;
    if (!f) {
      return { ok: false, reason: "noFrameElement" };
    }
    const pr = typeof f.getBoundingClientRect === "function" ? f.getBoundingClientRect() : null;
    if (!pr) {
      return { ok: false, reason: "noFrameRect" };
    }
    x = pr.left + x;
    y = pr.top + y;
    w = w.parent;
  }
  return { ok: true, x, y, w0: r0.width, h0: r0.height };
}
"""


async def _captcha_cta_page_viewport_center(
    button: Locator,
) -> tuple[tuple[float, float] | None, str | None]:
    try:
        res = await button.evaluate(_CAPTCHA_CTA_TO_TOPViewport_JS)
    except Exception as e:
        return None, f"viewport_map_eval:{e!s}"
    if not res or not res.get("ok"):
        return None, (res or {}).get("reason")
    return (float(res["x"]), float(res["y"])), None


# When the CTA <p> has 0×0 client rects, use the challenge document’s size and a fixed bias,
# then map that point up to the top viewport via frameElement (same as Playwright’s box math).
_INNER_CHALLENGE_FRAME_CENTER_TO_TOPViewport_JS = r"""
(el) => {
  const doc = el && el.ownerDocument;
  if (!doc || !doc.documentElement) {
    return { ok: false, reason: "noDoc" };
  }
  const de = doc.documentElement;
  const b = doc.body;
  const wnd = doc.defaultView;
  let dw = Math.max(
    de.clientWidth || 0,
    b && b.clientWidth || 0,
    wnd && wnd.innerWidth || 0,
    de.scrollWidth || 0,
    b && b.scrollWidth || 0,
    0
  );
  let dh = Math.max(
    de.clientHeight || 0,
    b && b.clientHeight || 0,
    wnd && wnd.innerHeight || 0,
    de.scrollHeight || 0,
    b && b.scrollHeight || 0,
    0
  );
  if (wnd && wnd.visualViewport) {
    const v = wnd.visualViewport;
    dw = Math.max(dw, v.width || 0, 0);
    dh = Math.max(dh, v.height || 0, 0);
  }
  if (dw < 2 || dh < 2) {
    return { ok: false, reason: "noDocSize" };
  }
  // Bottom-centered where PX usually paints the “Press & hold” control
  let x = dw * 0.5;
  let y = dh * 0.68;
  let w = wnd;
  if (!w) {
    return { ok: false, reason: "noWindow" };
  }
  while (w && w !== w.top) {
    const f = w.frameElement;
    if (!f) {
      return { ok: false, reason: "noFrameElement" };
    }
    const pr = typeof f.getBoundingClientRect === "function" ? f.getBoundingClientRect() : null;
    if (!pr) {
      return { ok: false, reason: "noFrameRect" };
    }
    x = pr.left + x;
    y = pr.top + y;
    w = w.parent;
  }
  return { ok: true, x, y, source: "innerFrame" };
}
"""


async def _captcha_inner_frame_page_viewport_center(
    button: Locator,
) -> tuple[tuple[float, float] | None, str | None]:
    try:
        res = await button.evaluate(_INNER_CHALLENGE_FRAME_CENTER_TO_TOPViewport_JS)
    except Exception as e:
        return None, f"inner_map_eval:{e!s}"
    if not res or not res.get("ok"):
        return None, (res or {}).get("reason")
    return (float(res["x"]), float(res["y"])), None


async def _main_page_iframe_px_shell_centers_from_js(
    page,
) -> list[tuple[float, float, str]]:
    """
    In the *main* document only: iframe elements get a real getBoundingClientRect in the
    top viewport even when Playwright’s bounding_box() is null (hidden / opacity).
    Catches e.g. iframe#px-captcha-modal; nested HVF iframes are not in this list—handled
    by _captcha_inner_frame_page_viewport_center on the CTA locator.
    """
    try:
        res = await page.evaluate(
            r"""() => {
  const out = [];
  for (const f of document.querySelectorAll("iframe")) {
    const r = typeof f.getBoundingClientRect === "function" ? f.getBoundingClientRect() : null;
    if (!r || r.width < 2 || r.height < 2) {
      continue;
    }
    const id = f.id || "";
    const t = f.getAttribute("title") || "";
    const src = f.getAttribute("src") || "";
    const blob = (id + " " + t + " " + src).toLowerCase();
    if (
      /px|perimeter|human|humn|verif|challeng|captcha/i.test(blob) ||
      id === "px-captcha-modal" ||
      /human verification/.test(t.toLowerCase())
    ) {
      out.push({ left: r.left, top: r.top, w: r.width, h: r.height, tag: t || id || "iframe" });
    }
  }
  if (out.length === 0) {
    for (const f of document.querySelectorAll("iframe")) {
      const r = typeof f.getBoundingClientRect === "function" ? f.getBoundingClientRect() : null;
      if (!r || r.width * r.height <= 8000) {
        continue;
      }
      out.push({ left: r.left, top: r.top, w: r.width, h: r.height, tag: "largest" });
      break;
    }
  }
  return out;
}"""
        )
    except Exception:
        return []
    if not isinstance(res, list):
        return []
    out: list[tuple[float, float, str]] = []
    for row in res:
        w, h = row.get("w", 0) or 0, row.get("h", 0) or 0
        if w < 2 or h < 2:
            continue
        cx = (row.get("left", 0) or 0) + w * 0.5
        cy = (row.get("top", 0) or 0) + h * 0.62
        out.append((cx, cy, f"main-DOM {row.get('tag', 'iframe')}"))
    return out


async def _px_iframe_shell_page_centers(page) -> list[tuple[float, float, str]]:
    """
    Fallback when inner CTA nodes report no layout box: use visible PerimeterX iframe
    `bounding_box()` (page viewport). Bias the HVF point toward the bottom where the
    press-hold control usually sits; outer modal is centered.
    """
    out: list[tuple[float, float, str]] = []
    for sel, label, y_frac in (
        (PX_HUMAN_CHALLENGE_IFRAME, "Human verification iframe", 0.62),
        (PX_CAPTCHA_IFRAME, "px-captcha-modal", 0.55),
    ):
        loc = page.locator(sel).first
        try:
            if await loc.count() == 0:
                continue
            box = await loc.bounding_box()
        except Exception:
            continue
        if not box:
            continue
        w, h = box.get("width", 0) or 0, box.get("height", 0) or 0
        if w < 2 or h < 2:
            continue
        cx = box["x"] + w * 0.5
        cy = box["y"] + h * y_frac
        out.append((cx, cy, label))
    return out


async def _known_px_iframe_getBoundingClientRect_centers(
    page,
) -> list[tuple[float, float, str]]:
    """
    Main-document `getBoundingClientRect()` on the known PX shells. Playwright
    `bounding_box()` is often `null` for opacity/hit-testing; the DOM API can
    still report a real rect (same need as _main_page_iframe_px_shell_centers_from_js
    but targets the Human verification + modal iframes explicitly).
    """
    out: list[tuple[float, float, str]] = []
    for sel, label, y_frac in (
        (PX_HUMAN_CHALLENGE_IFRAME, "HVF iframe (DOM getBoundingClientRect)", 0.62),
        (PX_CAPTCHA_IFRAME, "px-captcha-modal iframe (DOM getBoundingClientRect)", 0.55),
    ):
        loc = page.locator(sel).first
        try:
            if await loc.count() == 0:
                continue
            r = await loc.evaluate(
                r"""(el) => {
  if (!el) return null;
  const fn = el.getBoundingClientRect;
  if (typeof fn !== "function") return null;
  const r = fn.call(el);
  if (!r || r.width < 2 || r.height < 2) return null;
  return { left: r.left, top: r.top, w: r.width, h: r.height };
}"""
            )
        except Exception:
            continue
        if not r or not isinstance(r, dict):
            continue
        w, h = r.get("w", 0) or 0, r.get("h", 0) or 0
        if w < 2 or h < 2:
            continue
        cx = (r.get("left", 0) or 0) + w * 0.5
        cy = (r.get("top", 0) or 0) + h * y_frac
        out.append((cx, cy, label))
    return out


async def solve_px_captcha_button(page, button: Locator):
    """
    PerimeterX expects press-hold *on the target element* (often inside a cross-origin iframe).
    `page.mouse` in viewport can miss; use `locator.click(delay=ms)` so events hit the right node.

    PX often uses a <p> (or non-button) that is “hidden” in Playwright’s visibility check; `click(force)`
    can still run into “not visible”/scroll. We map the CTA to **page** viewport coordinates and use
    `page.mouse` with the hold duration. While pressed, we emit small randomized moves
    (pointer “tremor”); a perfectly static mousedown+sleep is a common bot tell for PX.
    """
    try:
        await expect(button).to_be_attached(timeout=20000)
        await _await_fullpage_px_inner_laid_out(page)
        hold_ms = random.randint(CAPTCHA_HOLD_MS_MIN, CAPTCHA_HOLD_MS_MAX)
        print(
            f"    Press & hold on control ({hold_ms}ms via pointer events on target)..."
        )
        # Trusted pointer: CTA center → top page viewport, then mousedown + hold + up.
        center_pt, map_reason = await _captcha_cta_page_viewport_center(button)
        how = "element map"
        inner_reason: str | None = None
        if center_pt is None:
            # Often succeeds when the <p> is 0×0 but the inner challenge document has real size
            center_pt, inner_reason = await _captcha_inner_frame_page_viewport_center(button)
            if center_pt is not None:
                how = "inner document center → top viewport (frameElement chain)"
                print(
                    f"    CTA had no element box ({map_reason!r}); {how}"
                )
        if center_pt is None:
            for cx, cy, label in await _px_fullpage_interstitial_dom_centers(page):
                center_pt, how = (cx, cy), label
                break
        if center_pt is None:
            for cx, cy, label in await _known_px_iframe_getBoundingClientRect_centers(
                page
            ):
                center_pt, how = (cx, cy), f"{label}"
                break
        if center_pt is None:
            for cx, cy, label in await _px_iframe_shell_page_centers(page):
                center_pt, how = (cx, cy), f"Playwright {label} box"
                break
        if center_pt is None:
            for cx, cy, label in await _main_page_iframe_px_shell_centers_from_js(page):
                center_pt, how = (cx, cy), f"main getBoundingClientRect: {label}"
                break
        if center_pt is not None:
            target_x, target_y = center_pt
            if how == "element map":
                target_x += random.uniform(-1.0, 1.0)
                target_y += random.uniform(-1.0, 1.0)
            else:
                target_x += random.uniform(-2.0, 2.0)
                target_y += random.uniform(-2.0, 2.0)
            await bezier_mouse_move(page, target_x, target_y)
            print(
                f"    Press & hold (page mouse at viewport {target_x:.1f}, {target_y:.1f} — {how})..."
            )
            # Wrapper/iframe-shell points are easy to miss; keep tremor tight on those
            jmax = (
                1.1
                if ("full-page" in how or "Playwright" in how or "main getBoundingClientRect" in how)
                else 1.85
            )
            await px_pointer_tremor_while_pressed(
                page, target_x, target_y, hold_ms, jitter_max=jmax
            )
        else:
            reason_bits = f"CTA: {map_reason!r}"
            if inner_reason is not None:
                reason_bits += f", inner: {inner_reason!r}"
            print(
                f"    No viewport point from element / inner frame / iframes "
                f"({reason_bits}); trying Playwright click..."
            )
            try:
                await button.scroll_into_view_if_needed(timeout=10000)
            except Exception:
                pass
            try:
                await button.click(
                    delay=hold_ms,
                    timeout=120000,
                    no_wait_after=True,
                    force=True,
                )
            except Exception as click_err:
                print(
                    f"    click(delay) also failed ({click_err!r}); no trusted hold sent."
                )
                return False

        if await _poll_until_captcha_solved_or_timeout(page, 30.0):
            print("    Successfully bypassed CAPTCHA.")
            await asyncio.sleep(random.uniform(0.4, 1.8))
            return True
        await human_delay(4, 7)
        if await _pulsing_captcha_solved(page):
            print("    Successfully bypassed CAPTCHA (after delay).")
            await asyncio.sleep(random.uniform(0.4, 1.8))
            return True
        # Top-level viewport coordinates may miss the real control inside the HVF iframe;
        # finish with a forced long-press on the resolved CTA node (same frame as the widget).
        if center_pt is not None:
            print(
                "    Viewport tremor hold did not clear challenge; trying element click(delay) on CTA…"
            )
            try:
                await button.scroll_into_view_if_needed(timeout=10000)
            except Exception:
                pass
            try:
                await button.click(
                    delay=hold_ms,
                    timeout=120000,
                    no_wait_after=True,
                    force=True,
                )
            except Exception as click_err:
                print(f"    Element click fallback failed ({click_err!r})")
                return False
            if await _poll_until_captcha_solved_or_timeout(page, 30.0):
                print("    Successfully bypassed CAPTCHA (element click).")
                await asyncio.sleep(random.uniform(0.4, 1.8))
                return True
            await human_delay(4, 7)
            if await _pulsing_captcha_solved(page):
                print("    Successfully bypassed CAPTCHA (element click, after delay).")
                await asyncio.sleep(random.uniform(0.4, 1.8))
                return True
        return False
    except Exception as e:
        print(f"    Error solving button: {e}")
        return False


async def _perimeterx_challenge_layer_visible(page) -> bool:
    """
    True if a PerimeterX challenge is blocking the app (in-app modal OR full-page
    interstitial). The full-page case does *not* use #px-captcha-modal — only
    #px-captcha-wrapper (DOM artifacts: title 'Access to this page has been denied').
    """
    for sel in (PX_CAPTCHA_IFRAME, PX_CAPTCHA_WRAPPER):
        loc = page.locator(sel)
        if await loc.count() == 0:
            continue
        try:
            if await loc.first.is_visible():
                return True
        except Exception:
            continue
    return False


async def _pulsing_captcha_solved(page) -> bool:
    """True when no visible PX layer blocks; full-page interstitial always has header copy."""
    if await is_goodrx_px_access_denial_page(page):
        return False
    if await _perimeterx_challenge_layer_visible(page):
        return False
    # In-app only: the banner text is not a permanent header, so "still here" = not solved
    on_full = await page.locator(PX_CAPTCHA_WRAPPER).count() > 0
    if not on_full:
        captcha_text = page.get_by_text(re.compile(r"Before we continue", re.I))
        if await captcha_text.count() > 0:
            return False
    return True


async def _prefer_cta_interactive_ancestor(
    text_hit: Locator, label_base: str
) -> tuple[Locator, str]:
    """
    PerimeterX often places "Press & Hold" in a <p> with 0×0 for Playwright; the real
    target is a wrapping <button>, [role=button], or a parent with layout (see error_dom
    and DOM: div#px-captcha, etc.).
    """
    for xp, bit in (
        ("xpath=ancestor::button[1]", "button ancestor"),
        ("xpath=ancestor::*[@role='button'][1]", "role=button ancestor"),
    ):
        try:
            loc = text_hit.locator(xp)
            if await loc.count() > 0:
                return loc, f"{label_base} → {bit}"
        except Exception:
            continue
    for up, bit in (("xpath=..", "parent of text node"), ("xpath=../..", "grandparent")):
        try:
            loc = text_hit.locator(up)
            if await loc.count() > 0:
                return loc, f"{label_base} → {bit}"
        except Exception:
            continue
    return text_hit, label_base


async def _captcha_button_locator(
    page, captcha_button_name: re.Pattern
) -> tuple[Locator | None, str]:
    """
    Resolve the real PerimeterX press-hold control. Do *not* scan all frames: unrelated
    iframes (e.g. about:blank) can expose a spurious "Press & Hold" and the solver
    will hold the wrong target while the real challenge stays up.
    """
    # Full-page interstitial (no iframe#px-captcha-modal) — see error_dom_artifacts
    inter = page.locator(PX_CAPTCHA_WRAPPER)
    try:
        interstitial = await inter.count() > 0 and await inter.first.is_visible()
    except Exception:
        interstitial = False
    if interstitial:
        wloc = page.locator(f"div#px-captcha {PX_HUMAN_CHALLENGE_IFRAME}")
        if await wloc.count() > 0:
            try:
                await wloc.first.wait_for(state="visible", timeout=10000)
            except Exception:
                pass

        cta_re = re.compile(r"^Press\s*&\s*Hold$", re.I)
        deadline = time.monotonic() + CAPTCHA_INJECT_WAIT_SEC
        while time.monotonic() < deadline:
            await raise_if_px_captcha_script_blocked(page)
            hvf = page.frame_locator(PX_HUMAN_CHALLENGE_IFRAME)
            hb = hvf.get_by_role("button", name=captcha_button_name)
            if await hb.count() > 0:
                return hb.first, PX_HUMAN_CHALLENGE_IFRAME
            ht = hvf.get_by_text(cta_re)
            if await ht.count() > 0:
                return await _prefer_cta_interactive_ancestor(
                    ht.first, f"{PX_HUMAN_CHALLENGE_IFRAME} (text CTA)"
                )
            dpx_hvf = page.frame_locator("div#px-captcha").frame_locator(
                PX_HUMAN_CHALLENGE_IFRAME
            )
            dhb = dpx_hvf.get_by_role("button", name=captcha_button_name)
            if await dhb.count() > 0:
                return dhb.first, "div#px-captcha > " + PX_HUMAN_CHALLENGE_IFRAME
            dht = dpx_hvf.get_by_text(cta_re)
            if await dht.count() > 0:
                return await _prefer_cta_interactive_ancestor(
                    dht.first, "div#px-captcha > HVF (text CTA)"
                )
            nested = page.frame_locator("div#px-captcha").frame_locator(
                PX_HUMAN_CHALLENGE_IFRAME
            )
            nb = nested.get_by_role("button", name=captcha_button_name)
            if await nb.count() > 0:
                return nb.first, "div#px-captcha > iframe"
            nt = nested.get_by_text(cta_re)
            if await nt.count() > 0:
                return await _prefer_cta_interactive_ancestor(
                    nt.first, "div#px-captcha>iframe (text CTA)"
                )
            try:
                cta = page.locator(PX_CAPTCHA_WRAPPER).get_by_text("Press & Hold", exact=True)
                if await cta.count() > 0:
                    return cta.last, f"{PX_CAPTCHA_WRAPPER} text=Press & Hold (exact)"
            except Exception:
                pass
            await asyncio.sleep(0.5)

    px_shell = page.locator(PX_CAPTCHA_IFRAME)
    try:
        use_px_first = await px_shell.count() > 0 and await px_shell.first.is_visible()
    except Exception:
        use_px_first = False

    px = page.frame_locator(PX_CAPTCHA_IFRAME)

    cta_re = re.compile(r"^Press\s*&\s*Hold$", re.I)

    async def in_modal_tree() -> tuple[Locator | None, str]:
        hvf_top = page.frame_locator(PX_HUMAN_CHALLENGE_IFRAME)
        hvf_css = hvf_top.locator("div#px-captcha")
        hctrl = hvf_css.locator("button, [role='button']")
        try:
            if await hctrl.count() > 0:
                return hctrl.first, f"{PX_HUMAN_CHALLENGE_IFRAME} (div#px-captcha control)"
        except Exception:
            pass
        hb = hvf_top.get_by_role("button", name=captcha_button_name)
        if await hb.count() > 0:
            return hb.first, f"{PX_HUMAN_CHALLENGE_IFRAME} (in-app top)"
        ht = hvf_top.get_by_text(cta_re)
        if await ht.count() > 0:
            return await _prefer_cta_interactive_ancestor(
                ht.first, f"{PX_HUMAN_CHALLENGE_IFRAME} (in-app text CTA)"
            )
        dpx_hvf = page.frame_locator("div#px-captcha").frame_locator(
            PX_HUMAN_CHALLENGE_IFRAME
        )
        hb2 = dpx_hvf.get_by_role("button", name=captcha_button_name)
        if await hb2.count() > 0:
            return hb2.first, f"div#px-captcha > {PX_HUMAN_CHALLENGE_IFRAME}"
        ht2 = dpx_hvf.get_by_text(cta_re)
        if await ht2.count() > 0:
            return await _prefer_cta_interactive_ancestor(
                ht2.first, "div#px-captcha > HVF (text CTA)"
            )
        nested = px.frame_locator(PX_HUMAN_CHALLENGE_IFRAME).get_by_role(
            "button", name=captcha_button_name
        )
        if await nested.count() > 0:
            return nested.first, "iframe#px-captcha-modal (nested)"
        in_px = px.get_by_role("button", name=captcha_button_name)
        if await in_px.count() > 0:
            return in_px.first, "iframe#px-captcha-modal"
        nt3 = px.frame_locator(PX_HUMAN_CHALLENGE_IFRAME).get_by_text(cta_re)
        if await nt3.count() > 0:
            return await _prefer_cta_interactive_ancestor(
                nt3.first, "iframe#px-captcha-modal (nested text CTA)"
            )
        return None, ""

    if use_px_first:
        loc, w = await in_modal_tree()
        if loc is not None:
            return loc, w

    main = page.get_by_role("button", name=captcha_button_name)
    if await main.count() > 0:
        return main.first, "main document"

    if not use_px_first:
        loc, w = await in_modal_tree()
        if loc is not None:
            return loc, w

    return None, ""


def _manual_perimeterx_mode() -> str:
    mode = GOODRX_MANUAL_PERIMETERX
    if mode in ("fallback", "always"):
        return mode
    return ""


async def _attempt_manual_perimeterx_override(page) -> bool:
    if not sys.stdin.isatty():
        raise ManualCaptchaPromptUnavailableError(
            "GOODRX_MANUAL_PERIMETERX is set, but stdin is not a TTY; refusing to wait for terminal input."
        )
    print("\n[manual] PerimeterX challenge detected. Manual override requested.")
    print("1. Go to the Chromium window.")
    print("2. Click and hold the challenge button yourself.")
    print("3. Wait for redirect back to the GoodRx app.")
    await asyncio.to_thread(
        input, "👉 Press ENTER here once the GoodRx page has loaded... "
    )
    print("    Resuming automation after manual solve...")
    await _settle_and_wait_for_goodrx_shell_after_captcha_solve(page)
    if await _pulsing_captcha_solved(page):
        print("    Manual CAPTCHA override cleared challenge.")
        return True
    return False


async def check_and_handle_captcha(page, max_attempts: int = CAPTCHA_MAX_SOLVE_ATTEMPTS):
    """Checks for captcha and attempts solve if found."""
    captcha_button_name = re.compile(r"Press & Hold", re.I)
    captcha_text = page.get_by_text(re.compile(r"Before we continue", re.I))
    manual_mode = _manual_perimeterx_mode()

    for attempt in range(max_attempts):
        await raise_if_px_captcha_script_blocked(page)
        challenge_visible = await _perimeterx_challenge_layer_visible(page)
        if challenge_visible:
            await _prepare_perimeterx_page_for_challenge(page)
        button, where = await _captcha_button_locator(page, captcha_button_name)
        if manual_mode == "always" and (challenge_visible or button is not None):
            if await _attempt_manual_perimeterx_override(page):
                return True
            print(
                f"    Manual CAPTCHA solve attempt {attempt + 1}/{max_attempts} did not clear challenge."
            )
            await asyncio.sleep(CAPTCHA_RECHECK_WAIT_SEC)
            continue
        if button is not None:
            print(f"    CAPTCHA button in {where}...")
            if await solve_px_captcha_button(page, button):
                await _settle_and_wait_for_goodrx_shell_after_captcha_solve(page)
                return True
            print(f"    CAPTCHA solve attempt {attempt + 1}/{max_attempts} did not clear challenge.")
            await asyncio.sleep(CAPTCHA_RECHECK_WAIT_SEC)
            continue

        # 3. If challenge shell still blocks but no resolvable target yet, retry after delay.
        # Do not use "Before we continue" count alone on the full-page interstitial: that
        # string is always in the static header (error_dom_artifacts) and spams this branch.
        on_full_px = await page.locator(PX_CAPTCHA_WRAPPER).count() > 0
        text_present = await captcha_text.count() > 0
        still_blocking = await _perimeterx_challenge_layer_visible(page) or (
            not on_full_px and text_present
        )
        if still_blocking:
            print("    CAPTCHA challenge still present; waiting before retry...")
            await asyncio.sleep(CAPTCHA_RECHECK_WAIT_SEC)
            continue

        # No captcha indicators found.
        return False

    if manual_mode == "fallback" and await _perimeterx_challenge_layer_visible(page):
        if await _attempt_manual_perimeterx_override(page):
            return True
        raise CaptchaUnresolvedError(
            "PerimeterX CAPTCHA remained after manual terminal override attempt."
        )

    raise CaptchaUnresolvedError(
        f"PerimeterX CAPTCHA not cleared after {max_attempts} attempt(s) (e.g. press-hold rejected or session flagged)."
    )

def location_modal(page):
    """
    GoodRx's ZIP modal; must not use a generic [role=dialog] — Osano cookies also
    expose role=dialog+aria-modal while hidden, which breaks strict locators.
    """
    return page.get_by_test_id("locationModal")


async def assert_px_captcha_iframe_hidden(page, where: str) -> None:
    """
    DOM validation: in-app `iframe#px-captcha-modal` and full-page `#px-captcha-wrapper`
    must be hidden (or absent) before location flow — same URL can serve either shape.
    """
    for desc, sel in (
        ("iframe#px-captcha-modal", PX_CAPTCHA_IFRAME),
        ("#px-captcha-wrapper (full-page PX)", PX_CAPTCHA_WRAPPER),
    ):
        loc = page.locator(sel)
        if await loc.count() == 0:
            continue
        try:
            await expect(loc.first).to_be_hidden(timeout=5000)
        except PlaywrightTimeoutError as e:
            raise CaptchaUnresolvedError(
                f"PerimeterX {desc} still visible after 5s ({where}) — aborting this zip step"
            ) from e


async def clear_px_captcha_if_blocking(
    page, max_passes: int = CAPTCHA_CLEAR_MAX_PASSES
):
    """
    In-app #px-captcha-modal or full-page #px-captcha-wrapper blocks the location control.
    """
    for _pass_i in range(max_passes):
        if not await _perimeterx_challenge_layer_visible(page):
            return
        if await check_and_handle_captcha(page):
            return
        if not await _perimeterx_challenge_layer_visible(page):
            return
        await asyncio.sleep(1.2)
    if not await _perimeterx_challenge_layer_visible(page):
        return
    raise CaptchaUnresolvedError(
        f"PerimeterX UI still present after {max_passes} clear pass(es); will not continue to location UI."
    )

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

async def scrape_drug_data(
    page, drug_name, zip_code, drug_url: str | None = None
):
    """Scrapes discoverable pharmacy options for a drug/zip.
    `drug_url` enables recovery navigation if PerimeterX leaves the access-denial
    document or the app shell has not mounted the location control.
    """
    t0 = time.monotonic()
    print(f"  Scraping {drug_name} in {zip_code}...")

    await check_and_handle_captcha(page)
    await clear_px_captcha_if_blocking(page, max_passes=CAPTCHA_CLEAR_MAX_PASSES)
    await assert_px_captcha_iframe_hidden(page, "after preflight")
    if await is_goodrx_px_access_denial_page(page) and drug_url:
        await _navigate_drug_url_and_rerun_captcha(
            page, drug_url, "access-denial title after preflight"
        )
        await assert_px_captcha_iframe_hidden(page, "after denial-title recovery")
    print("    Phase: preflight (captcha / blocking iframe) done")

    if await is_goodrx_px_access_denial_page(page) and not drug_url:
        raise LocationTriggerError(
            "Still on access-denial page title; set drug_url in scrape to allow recovery, "
            "or pass GoodRx / captcha heuristics."
        )

    location_trigger = location_triggers_combined_locator(page)
    if await location_trigger.count() == 0:
        await asyncio.sleep(2.0)
        await check_and_handle_captcha(page)
        await clear_px_captcha_if_blocking(page, max_passes=CAPTCHA_CLEAR_MAX_PASSES)
        location_trigger = location_triggers_combined_locator(page)
    if await location_trigger.count() == 0:
        await _wait_for_combined_location_trigger(page, 14.0)
        location_trigger = location_triggers_combined_locator(page)
    if await location_trigger.count() == 0 and drug_url:
        await _navigate_drug_url_and_rerun_captcha(
            page, drug_url, "no location control after preflight + wait"
        )
        await _wait_for_combined_location_trigger(page, 12.0)
        location_trigger = location_triggers_combined_locator(page)
    if await location_trigger.count() == 0:
        msg = "Location trigger missing after captcha, wait, and optional recovery navigation."
        if await is_goodrx_px_access_denial_page(page):
            msg += " Page title is still the PerimeterX access-denial string."
        raise LocationTriggerError(msg)
    await assert_px_captcha_iframe_hidden(page, "before location modal")
    print("    Phase: open location, set zip, load results...")

    modal = location_modal(page)
    modal_opened = False
    last_modal_error = None
    for modal_try in range(2):
        try:
            await clear_px_captcha_if_blocking(page, max_passes=CAPTCHA_CLEAR_MAX_PASSES)
            await location_trigger.first.click(timeout=15000)
            await expect(modal).to_be_visible(timeout=7000)
            modal_opened = True
            break
        except (CaptchaUnresolvedError, CaptchaScriptBlockedError):
            raise
        except Exception as exc:
            last_modal_error = exc
            await check_and_handle_captcha(page)
            await clear_px_captcha_if_blocking(page, max_passes=CAPTCHA_CLEAR_MAX_PASSES)
            try:
                await location_trigger.first.click(timeout=30000, force=True)
                await expect(modal).to_be_visible(timeout=10000)
                modal_opened = True
                break
            except Exception as retry_exc:
                last_modal_error = retry_exc
                if modal_try == 0:
                    await asyncio.sleep(1.5)
                    location_trigger = location_triggers_combined_locator(page)
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


async def open_persistent_chromium(p, stealth: Stealth | None = None):
    """
    Persistent Chromium with optional `GOODRX_PLAYWRIGHT_PROXY` / `HTTP_PROXY`.

    There is no `page.route()` interception in this project: do not add request aborts
    that block `*.px-cloud.net` (captcha.js), GoodRx `/xhr` JSON, or other first-party
    assets — that yields the static PX error UI instead of the real challenge.
    """
    os.makedirs(USER_DATA_DIR, exist_ok=True)
    launch_kw: dict = {
        "user_data_dir": os.path.abspath(USER_DATA_DIR),
        "headless": False,
        "user_agent": USER_AGENT,
        "viewport": {"width": 1920, "height": 1080},
        "java_script_enabled": True,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--start-maximized",
        ],
    }
    prov = _playwright_proxy_config()
    if prov:
        launch_kw["proxy"] = prov
        print("    [proxy] using proxy from GOODRX_PLAYWRIGHT_PROXY or HTTP_PROXY")
    context = await p.chromium.launch_persistent_context(**launch_kw)
    if stealth is not None:
        # playwright-stealth only hooks `launch` → Browser; persistent context must apply here.
        await stealth.apply_stealth_async(context)
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page


async def warmup_drug_entry_page(page, drug) -> None:
    await page.goto("https://www.google.com/", wait_until="domcontentloaded")
    g0, g1 = DELAY_WARMUP_GOOGLE_S
    await asyncio.sleep(random.uniform(g0, g1))
    await page.goto(drug["url"], wait_until="domcontentloaded", timeout=60000)
    d0, d1 = DELAY_POST_DRUG_LOAD_S
    await asyncio.sleep(random.uniform(d0, d1))


async def main():
    global _run_csv_path
    if not os.path.exists(SCREENSHOT_DIR): os.makedirs(SCREENSHOT_DIR)
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

    if os.environ.get("GOODRX_RESET_PROFILE_ON_START", "").lower() in ("1", "true", "yes"):
        nuke_browser_profile_dir()
    if not os.path.isdir(USER_DATA_DIR):
        os.makedirs(USER_DATA_DIR, exist_ok=True)

    stealth = Stealth()
    async with stealth.use_async(async_playwright()) as p:
        context, page = await open_persistent_chromium(p, stealth=stealth)
        giveup_streak = 0

        async def relaunch_clean_profile(current_drug: dict) -> None:
            nonlocal context, page, giveup_streak
            print(
                f"    [profile] wiping {USER_DATA_DIR!r} after {giveup_streak} "
                "consecutive zip give-up(s) (PerimeterX reputation / storage reset)"
            )
            await context.close()
            nuke_browser_profile_dir()
            os.makedirs(USER_DATA_DIR, exist_ok=True)
            context, page = await open_persistent_chromium(p, stealth=stealth)
            giveup_streak = 0
            await warmup_drug_entry_page(page, current_drug)

        for drug in DRUGS:
            print(f"Starting drug: {drug['name']}")
            try:
                await warmup_drug_entry_page(page, drug)

                for zi, zip_code in enumerate(ZIP_CODES):
                    if (
                        zi > 0
                        and zi % ZIP_ITERATIONS_BEFORE_COOLDOWN == 0
                    ):
                        lo, hi = IP_COOLDOWN_BETWEEN_ZIP_BLOCKS_S
                        cool = random.uniform(lo, hi)
                        print(
                            f"    [pacing] extra cooldown {cool:.0f}s before zip block "
                            f"({zi} zips; mitigates single-IP velocity flags)"
                        )
                        await asyncio.sleep(cool)

                    zip_completed = False
                    for attempt in range(MAX_ZIP_RETRIES + 1):
                        try:
                            await asyncio.wait_for(
                                scrape_drug_data(
                                    page, drug["name"], zip_code, drug_url=drug["url"]
                                ),
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
                        print(
                            f"  Skipping {drug['name']} {zip_code} after {MAX_ZIP_RETRIES + 1} failed attempts."
                        )
                        giveup_streak += 1
                        if giveup_streak >= CONSECUTIVE_ZIP_GIVEUPS_BEFORE_PROFILE_RESET:
                            await relaunch_clean_profile(drug)
                    else:
                        giveup_streak = 0
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
