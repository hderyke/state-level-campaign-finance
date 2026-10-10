"""
scrapers/alaska.py — Download Alaska APOC campaign finance data.

Requires a live browser session via Playwright — Alaska's WAF blocks datacenter
IPs, so this must be run from a local machine. Exports are triggered by clicking
Search then Export, mirroring normal user interaction.

aws.state.ak.us is also fronted by DataDome (captcha-delivery.com) — confirmed
live 2026-09-23 by firing a burst of export requests and getting back a real
"Slide right to secure your access" interstitial, which explicitly names
"Rapid taps or clicks" / "Automated (bot) activity" as trigger reasons. This
matches the scraper's own back-to-back year-download pattern. The slider
requires a genuine human drag gesture — it cannot be solved by script. Runs
headless=False for exactly this reason (a headless browser is itself a bot
signal DataDome checks for) and uses a persistent Playwright profile
(PROFILE_DIR) so a human-solved challenge's trust cookie survives across runs
instead of being thrown away every time — same fix already proven for
Missouri's Incapsula CAPTCHA elsewhere in this repo. See
_wait_out_datadome() below and docs/states/alaska.md.
"""

import csv
import random
import sys
import time
from datetime import datetime
from pathlib import Path


# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR      = PROJECT_ROOT / "data" / "Alaska" / "raw"
MANIFEST     = PROJECT_ROOT / "data" / "Alaska" / "manifest.csv"

# Persistent Playwright profile directory -- NOT under data/Alaska/raw,
# since parsers glob that tree for CSV exports and shouldn't see Chrome
# profile internals. See the launch_persistent_context() note in run()
# for why this needs to be persistent rather than a fresh throwaway
# context (same reasoning/pattern as Missouri's PROFILE_DIR).
PROFILE_DIR  = PROJECT_ROOT / ".playwright-profiles" / "alaska"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]



SOURCES = [
    {"name": "Alaska Public Offices Commission (APOC)",
     "url": "https://aws.state.ak.us/ApocReports/"},
]

# =============================== pages ================================

# Alaska requires a live browser session — Playwright handles this by clicking
# Search then Export just like a user would. Must be run from a local machine;
# datacenter IPs are blocked by Alaska's WAF.
PAGES = {
    "income":       "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
    "expenditures": "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDExpenditures.aspx",
    "candidates":   "https://aws.state.ak.us/apocreports/Campaign/AllCandidates.aspx?type=all",
    "groups":       "https://aws.state.ak.us/apocreports/Registration/GroupRegistration/GRForms.aspx",
    # Bulk candidate-registration listing, the candidate analog of GRForms.
    # Goes through the same year-based download_year() path. The export
    # carries candidate name, committee, treasurer, city and zip.
    "cr_forms":     "https://aws.state.ak.us/apocreports/Registration/CandidateRegistration/CRForms.aspx",
    # Independent Expenditure (Form 15-6) bulk exports -- same
    # Select-Year/Status/Search/Export flow as income/expenditures/groups
    # above (confirmed live 2026-09-24: identical ddlReportYear/ddlStatus/
    # btnSearch/btnExport/hlAllCSV markup), NOT the per-ID Common/View.aspx
    # detail sweep an earlier version of this scraper used -- that page-by-
    # page approach was unnecessary since these bulk grids already export
    # every filing. Two separate pages/exports (not one, like GR/CR) because
    # IEExpenditures.aspx and IEContributions.aspx are two independent
    # bulk-export grids, each with its own CSV, even though a single IE
    # filing's detail page shows both sides.
    "ie_expenditures":  "https://aws.state.ak.us/apocreports/IndependentExpenditures/IEExpenditures.aspx",
    "ie_contributions": "https://aws.state.ak.us/apocreports/IndependentExpenditures/IEContributions.aspx",
}

TRANSACTION_RELATIONS = {"income", "expenditures", "ie_expenditures", "ie_contributions"}
ENTITY_RELATIONS      = {"candidates", "groups", "cr_forms"}

STEMS = {
    "income":       "CDIncome",
    "expenditures": "CDExpense",
    "candidates":   "CDCandidates",
    "groups":       "GRForms",
    "cr_forms":     "CRForms",
    "ie_expenditures":  "IEExpenditures",
    "ie_contributions": "IEContributions",
}

# ========================= anti-automation setup =========================
# Chromium under CDP control (which is how Playwright drives it) sets
# navigator.webdriver=true and a few other properties by default -- the
# single most common signal basic bot-detection scripts check for. None of
# this defeats a serious bot-mitigation vendor like DataDome on its own
# (see the DataDome section below for the real handling), but it's the
# standard first line of defense, costs nothing to include, and is already
# the proven pattern in this repo for Nevada's WAF -- ported here as-is,
# just with an Alaska-appropriate timezone.
LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled"]

DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
VIEWPORT = {"width": 1440, "height": 900}
LOCALE   = "en-US"
TIMEZONE_ID = "America/Anchorage"

STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
window.chrome = window.chrome || { runtime: {} };
const _origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : _origQuery(parameters)
);
"""

# ========================== Manifest helpers ==========================
def load_manifest() -> tuple[set[tuple[str, str]], set[str]]:
    """Return (done, has_data) sets from the manifest; empty sets if it doesn't exist."""
    done: set[tuple[str, str]] = set()
    has_data: set[str] = set()
    if not MANIFEST.exists():
        return done, has_data
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["year"]))
            has_data.add(row["relation_type"])
    return done, has_data


def strip_manifest(keep_fn: callable) -> None:
    """Rewrite the manifest keeping only rows where keep_fn(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict) -> None:
    """Add or overwrite the manifest entry matching (relation_type, year)."""
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            existing = [
                r for r in csv.DictReader(f)
                if not (r["relation_type"] == record["relation_type"]
                        and r["year"] == record["year"])
            ]
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)



# ========================= Playwright helpers =========================
def get_available_years(page) -> list[str]:
    sel = page.locator("select[name*='ddlReportYear']")
    if not sel.count():
        return []
    options = sel.locator("option").all()
    years = [
        opt.get_attribute("value")
        for opt in options
        if opt.get_attribute("value") not in ("-1", "0", "", None)
    ]
    return sorted(set(years))


# ========================= DataDome bot-check =========================
# See module docstring for how this was confirmed. Detection is by content
# signature (the delivery domain + the challenge's own copy) rather than a
# specific DOM selector, since that's stable even if DataDome's widget
# markup changes. Used as a logging/pause signal only -- the actual "did we
# get real content" check downstream (dropdown present, csv_link present,
# etc.) is the real source of truth, same structural-fallback approach
# already used for Missouri's WAF handling in this repo.
DATADOME_MARKERS = [
    "captcha-delivery.com",
    "slide right to secure your access",
    "verification required",
    "access is temporarily restricted",
    "we detected unusual activity from your device or network",
]

# The hard-block variant above has no widget to solve -- it's a cooldown,
# not a challenge -- so the "solve it by hand" prompt in _wait_out_datadome
# would be actively misleading for it. Checked separately so the log
# message matches what's actually on screen.
DATADOME_HARD_BLOCK_MARKERS = [
    "access is temporarily restricted",
]

DOWNLOAD_TIMEOUT_MS     = 600_000  # ms; confirmed clean downloads for the
                                    # largest years take up to ~440s server-
                                    # side alone, well past the old 180s cap
DATADOME_WAIT_TIMEOUT_S = 900      # how long to wait for a human to solve it
DATADOME_POLL_S         = 3
DATADOME_MAX_CONSECUTIVE_BLOCKS = 6  # abort the run rather than hammer a live block


def _content_has_datadome(content: str) -> bool:
    lowered = content.lower()
    return any(marker in lowered for marker in DATADOME_MARKERS)


def _looks_like_datadome(page) -> bool:
    try:
        content = page.content()
    except Exception:
        return False
    return _content_has_datadome(content)


def _wait_out_datadome(page, log, where: str) -> bool | None:
    """If DataDome is showing (either the interactive slider challenge or
    the no-widget "Access is temporarily restricted" cooldown variant), log
    an accurate message and poll until it clears or we give up after
    DATADOME_WAIT_TIMEOUT_S.

    Returns None if DataDome was never showing at all, True if it was
    showing and cleared before the timeout, False if it was still showing
    when we gave up -- callers should treat False as a signal to back off
    hard (stop retrying at normal cadence) rather than just another
    per-item failure, since hammering a live block only reinforces it."""
    try:
        content = page.content()
    except Exception:
        return None
    if not _content_has_datadome(content):
        return None

    hard_block = any(m in content.lower() for m in DATADOME_HARD_BLOCK_MARKERS)
    if hard_block:
        log.warning(
            f"[!] DataDome hard block ('Access is temporarily restricted') at "
            f"{where} -- no widget to solve, this is an IP-level cooldown that "
            f"only clears on its own (waiting up to {DATADOME_WAIT_TIMEOUT_S}s)"
        )
    else:
        log.warning(
            f"[!] DataDome challenge detected at {where} -- solve the slider "
            f"in the visible browser window (waiting up to {DATADOME_WAIT_TIMEOUT_S}s)"
        )

    waited = 0
    while waited < DATADOME_WAIT_TIMEOUT_S:
        time.sleep(DATADOME_POLL_S)
        waited += DATADOME_POLL_S
        if not _looks_like_datadome(page):
            log.info(f"  DataDome block at {where} cleared after {waited}s")
            return True
    log.warning(
        f"  DataDome block at {where} still showing after "
        f"{DATADOME_WAIT_TIMEOUT_S}s -- giving up on this attempt"
    )
    return False


def _goto_export_link(page, csv_link, href: str | None, log) -> None:
    """Trigger the CSV download by navigating directly to the export
    link's href instead of clicking the <a> element.

    Investigated live 2026-09-23 (see docs/states/alaska.md): the export
    link (`a[id*='hlAllCSV']`) opens target="_blank", and confirmed the
    underlying export URL is a plain, deterministic GET
    (`?exportAll=True&exportFormat=CSV&isExport=True&...`) that returns the
    CSV directly once Search+Export have set server-side state -- a real
    in-page fetch() to it works standalone. Went as far as prototyping a
    full fetch()-driven replacement for the Search/Export clicks too, but
    backed off: a DataDome challenge triggered mid-fetch() would be
    invisible (nothing renders in the browser for a human to solve), which
    would quietly break the human-solvable design _wait_out_datadome()
    depends on. So Search/Export stay real Playwright clicks -- any
    DataDome hit there still renders normally in the visible window -- and
    only this last step (which needs no further server-side interaction)
    is a direct goto() to the href instead of a click. This makes download
    capture unambiguous rather than depending on Playwright's context-level
    accept_downloads correctly attributing a new-tab click's download back
    to this page's expect_download(), which usually works but isn't
    guaranteed across Chromium versions. Falls back to the original click()
    if no href was found, so this is a pure improvement, not a behavior
    change when the DOM doesn't look as expected."""
    if href:
        try:
            # wait_until="commit" resolves as soon as the response headers
            # arrive, instead of Playwright's default "load" -- a download
            # response never fires "load" (no page loads), so the default
            # would time out at 30s on every single successful download,
            # trigger a needless fallback click() (itself racing the
            # already-in-flight download and prone to its own timeout),
            # and log a scary-looking warning for what was actually a
            # clean success. Confirmed live 2026-09-23: without this, 3/5
            # of one run's downloads succeeded silently via a fast
            # ERR_ABORTED and 1/5 hit this exact false-alarm path.
            page.goto(href, timeout=30_000, wait_until="commit")
        except Exception as e:
            # A download response reclassifies the navigation and goto()
            # raises for it -- expected, the download itself is tracked
            # separately by expect_download(). Anything else (bad URL,
            # DNS, etc.) is a real failure and must not be swallowed
            # silently -- that's exactly what let the 2026-09-23
            # relative-href bug hide as "not hitting the csv button".
            msg = str(e)
            if "ERR_ABORTED" not in msg and "Download is starting" not in msg:
                log.warning(f"  [!] goto(href) for CSV export failed unexpectedly: {msg!r} -- falling back to click()")
                try:
                    csv_link.click()
                except Exception as e2:
                    log.warning(f"  [!] fallback click() also failed: {e2!r}")
    else:
        try:
            csv_link.click()
        except Exception as e:
            log.warning(f"  [!] csv_link.click() failed with no href available: {e!r}")


def download_candidates(page, context, log) -> tuple[str, int] | None:
    page_url = PAGES["candidates"]
    page.goto(page_url, timeout=30_000)
    page.wait_for_load_state("networkidle")
    _wait_out_datadome(page, log, "candidates page load")

    year_sel = page.locator("select[name*='ddlYear']")
    if year_sel.count():
        year_sel.select_option("All")

    search_btn = page.locator("input[value='Search']")
    if search_btn.count():
        page.click("input[value='Search']")
        page.wait_for_load_state("networkidle")
        _wait_out_datadome(page, log, "candidates search")

    body_text = page.locator("body").inner_text()
    if "No records" in body_text or "0 records" in body_text.lower():
        log.info("  candidates: no records found")
        return None

    page.click("input[value='Export']")
    _wait_out_datadome(page, log, "candidates export click")

    csv_link = page.locator("a[id*='hlAllCSV']")
    try:
        csv_link.wait_for(timeout=15_000)
        # get_attribute("href") returns the raw HTML attribute, which on
        # this ASP.NET page is relative -- page.goto() on a context with
        # no baseURL throws on that. evaluate() reads the DOM's resolved
        # (absolute) href property instead, matching what live DevTools
        # testing confirmed works.
        csv_href = csv_link.evaluate("el => el.href")
    except Exception:
        log.warning("  [!] Export dialog did not appear for candidates")
        return None

    filename = "CDCandidates_all.csv"
    out_path = RAW_DIR / filename

    with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
        _goto_export_link(page, csv_link, csv_href, log)

    dl = dl_info.value
    dl.save_as(str(out_path))

    text      = out_path.read_text(encoding="utf-8", errors="replace")
    row_count = text.count("\n") - 1
    return filename, row_count


def download_year(page, context, relation_type: str, year: str, log) -> tuple[str, int] | None:
    page_url = PAGES[relation_type]
    page.goto(page_url, timeout=30_000)
    page.wait_for_load_state("networkidle")
    _wait_out_datadome(page, log, f"{relation_type} {year} page load")

    year_sel = page.locator("select[name*='ddlReportYear']")
    if year_sel.count():
        year_sel.select_option(year)

    status_sel = page.locator("select[name*='ddlStatus']")
    if status_sel.count():
        try:
            status_sel.select_option(label="All Complete Forms")
        except Exception:
            status_sel.select_option("0")

    page.click("input[value='Search']")
    page.wait_for_load_state("networkidle")
    _wait_out_datadome(page, log, f"{relation_type} {year} search")

    body_text = page.locator("body").inner_text()
    if "No records" in body_text or "0 records" in body_text.lower():
        log.debug(f"  {relation_type} {year}: no records")
        return None

    filename = f"{STEMS[relation_type]}_{year}.csv"
    out_path = RAW_DIR / filename
    csv_link = page.locator("a[id*='hlAllCSV']")

    with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
        page.click("input[value='Export']")
        _wait_out_datadome(page, log, f"{relation_type} {year} export click")
        csv_href = None
        try:
            csv_link.wait_for(timeout=8_000)
            # see download_candidates() above -- evaluate() gives the
            # resolved absolute href, get_attribute() would give the raw
            # (relative) one and silently break goto().
            csv_href = csv_link.evaluate("el => el.href")
        except Exception:
            pass
        _goto_export_link(page, csv_link, csv_href, log)

    dl = dl_info.value
    dl.save_as(str(out_path))

    text      = out_path.read_text(encoding="utf-8", errors="replace")
    row_count = text.count("\n") - 1
    return filename, row_count


# ============================ orchestrator ============================
def run(
    force: bool = False,
    entities: bool = False,
    transactions: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
    contributions: bool = False,
    expenditures: bool = False,
    candidates: bool = False,
    committees: bool = False,
    independent_expenditures: bool = False,
):
    """Orchestrate download of transaction CSVs and/or candidate/group entities.

    Vertical scope (mutually exclusive):
        force=True              — re-download all years, wipe relevant manifest entries
        start_year / end_year   — restrict year-based downloads to this range

    Horizontal scope:
        No flags                — download everything
        transactions            — income + expenditures only
        entities                — candidates + groups only
        contributions           — income only (implies transactions)
        expenditures            — expenditures only (implies transactions)
        candidates              — CDCandidates + CRForms bulk only (implies entities)
        committees              — GRForms bulk only (implies entities)
        independent_expenditures — IE (Form 15-6) bulk exports only (its own filing track, own manifest vertical -- not folded into entities/transactions so an --entities/--transactions caller doesn't unexpectedly pick it up). Uses the same year-based bulk Search/Export flow as income/expenditures/groups, not a per-ID detail sweep.
    """
    log = get_logger("alaska", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Alaska scraper")
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees,
              independent_expenditures=independent_expenditures)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("[!] Playwright not installed. Run: pip install playwright && playwright install chromium")
        log._emit("scrape_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, pages_ok=0, pages_err=0,
                  error="playwright not installed")
        return

    # ── Resolve granular scope ────────────────────────────────────────
    # Any horizontal flag set → only the named types; none → everything.
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees or
                         independent_expenditures)

    do_income        = no_horizontal or transactions or contributions
    do_expend        = no_horizontal or transactions or expenditures
    do_candidates_dl = no_horizontal or entities or candidates
    do_groups_dl     = no_horizontal or entities or committees
    # Independent_expenditures is its OWN horizontal flag, not folded into
    # entities -- see run()'s docstring. no_horizontal alone still covers
    # the plain-no-flags "download everything" case.
    do_ie            = no_horizontal or independent_expenditures

    files_ok = files_err = pages_ok = pages_err = 0
    current_year = str(datetime.today().year)

    # ── Scoped manifest clearing ──────────────────────────────────────
    if force:
        relations_to_clear = set()
        if do_income:        relations_to_clear.add("income")
        if do_expend:        relations_to_clear.add("expenditures")
        if do_candidates_dl: relations_to_clear.add("candidates")
        if do_candidates_dl: relations_to_clear.add("cr_forms")
        if do_groups_dl:     relations_to_clear.add("groups")
        if do_ie:            relations_to_clear.add("ie_expenditures")
        if do_ie:            relations_to_clear.add("ie_contributions")
        strip_manifest(lambda r: r["relation_type"] not in relations_to_clear)

    elif start_year is not None or end_year is not None:
        # Year range — wipe manifest entries for year-based relations within the range
        # so they get re-downloaded, not skipped as "already done".
        year_based = set()
        if do_income:        year_based.add("income")
        if do_expend:        year_based.add("expenditures")
        if do_groups_dl:     year_based.add("groups")
        if do_candidates_dl: year_based.add("cr_forms")
        if do_ie:            year_based.add("ie_expenditures")
        if do_ie:            year_based.add("ie_contributions")

        def _outside_range(r: dict) -> bool:
            """Keep rows that are NOT in the wipe zone."""
            if r["relation_type"] not in year_based:
                return True   # non-year-based entries always kept
            try:
                yr = int(r["year"])
            except ValueError:
                return True   # non-numeric year entries (e.g. "all") always kept
            if start_year is not None and yr < start_year:
                return True   # below range — keep
            if end_year is not None and yr > end_year:
                return True   # above range — keep
            return False      # within range — wipe

        strip_manifest(_outside_range)

    done, has_data = load_manifest()

    # ── Build pages_to_run for the Playwright loop ────────────────────
    pages_to_run: set[str] = set()
    if do_income:        pages_to_run.add("income")
    if do_expend:        pages_to_run.add("expenditures")
    if do_candidates_dl: pages_to_run.add("candidates")
    if do_candidates_dl: pages_to_run.add("cr_forms")
    if do_groups_dl:     pages_to_run.add("groups")
    if do_ie:            pages_to_run.add("ie_expenditures")
    if do_ie:            pages_to_run.add("ie_contributions")

    try:
        # ── Playwright: transaction CSVs + candidate/group exports ────
        with sync_playwright() as p:
            # Persistent profile (PROFILE_DIR), not a fresh launch()/
            # new_context() pair. Confirmed live 2026-09-23: aws.state.ak.us
            # is fronted by DataDome, which serves an interactive "slide to
            # verify" challenge when it sees automated-looking traffic
            # (explicitly names "rapid taps or clicks" / "automated bot
            # activity" as trigger reasons -- matches this scraper's own
            # back-to-back year-download pattern). It requires a real human
            # drag gesture; it cannot be solved by script. A throwaway
            # new_context() starts with zero cookies every run, so DataDome
            # has no way to recognize a returning, already-trusted session,
            # and a fresh challenge is effectively guaranteed every time.
            # launch_persistent_context() writes cookies (including the
            # `datadome` trust cookie set after a challenge is solved) to
            # PROFILE_DIR on disk -- solve it once by hand and later runs
            # reuse that cookie, same fix already proven for Missouri's
            # Incapsula CAPTCHA elsewhere in this repo. If the challenge
            # ever reappears, that's the trust cookie expiring or getting
            # invalidated, not a code regression -- just solve it again once.
            context = p.chromium.launch_persistent_context(
                str(PROFILE_DIR), headless=False, accept_downloads=True,
                args=LAUNCH_ARGS, user_agent=DESKTOP_USER_AGENT,
                viewport=VIEWPORT, locale=LOCALE, timezone_id=TIMEZONE_ID,
            )
            context.add_init_script(STEALTH_INIT_SCRIPT)
            page = context.new_page()

            for relation_type, page_url in PAGES.items():
                if relation_type not in pages_to_run:
                    continue

                log.info(f"\nAlaska {relation_type}:")

                # Candidates — single all-years export, no year filter applies
                if relation_type == "candidates":
                    key = ("candidates", "all")
                    cand_file = RAW_DIR / "CDCandidates_all.csv"
                    if (key in done or cand_file.exists()) and not force:
                        log.file_download_skip(filename="CDCandidates_all.csv")
                    else:
                        log.file_download_start(filename="CDCandidates_all.csv")
                        t_file  = time.perf_counter()
                        result  = None
                        err_msg = None
                        try:
                            result = download_candidates(page, context, log)
                        except Exception as e:
                            err_msg = str(e)

                        if err_msg:
                            log.file_download_error(filename="CDCandidates_all.csv", error=err_msg)
                            files_err += 1
                        elif result:
                            filename, row_count = result
                            size = (RAW_DIR / filename).stat().st_size
                            log.file_download_ok(filename=filename, bytes=size,
                                                 rows=row_count,
                                                 duration_s=time.perf_counter() - t_file)
                            files_ok += 1
                            upsert_manifest({
                                "relation_type": "candidates",
                                "year":          "all",
                                "filename":      filename,
                                "row_count":     row_count,
                            })
                            done.add(key)
                    continue

                # Year-based relations (income, expenditures, groups)
                page.goto(page_url, timeout=30_000)
                page.wait_for_load_state("networkidle")
                _wait_out_datadome(page, log, f"{relation_type} years dropdown load")

                years = get_available_years(page)
                if not years:
                    log.warning(f"  [!] Could not read year dropdown for {relation_type} — skipping")
                    continue

                log.info(f"  Available years: {years[0]}–{years[-1]} ({len(years)} total)")
                consecutive_datadome_blocks = 0

                for year in years:
                    yr_int        = int(year)
                    expected_stem = f"{STEMS[relation_type]}_{year}.csv"

                    # Year range filter — skip years outside requested window
                    if start_year is not None and yr_int < start_year:
                        log.file_download_skip(filename=expected_stem)
                        continue
                    if end_year is not None and yr_int > end_year:
                        log.file_download_skip(filename=expected_stem)
                        continue

                    key           = (relation_type, year)
                    expected_file = RAW_DIR / expected_stem
                    # When a year range is active the manifest was already wiped for
                    # in-range entries — don't fall back to file existence or those
                    # years will still be skipped even though the manifest was cleared.
                    year_range_active = start_year is not None or end_year is not None
                    already_done = key in done or (
                        not year_range_active
                        and expected_file.exists()
                        and expected_file.stat().st_size > 0
                    )

                    if already_done and year != current_year and not force:
                        log.file_download_skip(filename=expected_stem)
                        continue

                    log.file_download_start(filename=expected_stem)
                    t_file  = time.perf_counter()
                    result  = None
                    err_msg = None
                    try:
                        result = download_year(page, context, relation_type, year, log)
                    except Exception as e:
                        err_msg = str(e)

                    if err_msg:
                        log.file_download_error(filename=expected_stem, error=err_msg)
                        files_err += 1
                        try:
                            was_datadome = not page.is_closed() and _content_has_datadome(page.content())
                        except Exception:
                            was_datadome = False
                        if was_datadome:
                            consecutive_datadome_blocks += 1
                            if consecutive_datadome_blocks >= DATADOME_MAX_CONSECUTIVE_BLOCKS:
                                log.warning(
                                    f"  {consecutive_datadome_blocks} consecutive DataDome "
                                    f"blocks on {relation_type} -- stopping this relation type "
                                    f"rather than hammering a live block; re-run later to resume"
                                )
                                break
                        continue

                    if result is None:
                        continue

                    filename, row_count = result
                    size = (RAW_DIR / filename).stat().st_size
                    log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                         duration_s=time.perf_counter() - t_file)
                    files_ok += 1
                    consecutive_datadome_blocks = 0
                    upsert_manifest({
                        "relation_type": relation_type,
                        "year":          year,
                        "filename":      filename,
                        "row_count":     row_count,
                    })
                    done.add(key)
                    # DataDome's own challenge copy calls out "rapid taps or
                    # clicks" as a trigger signal -- a little jitter between
                    # years (vs. the old flat 1s) costs almost nothing over
                    # a run and reduces how metronomic the request pattern
                    # looks.
                    time.sleep(random.uniform(2.0, 5.0))

            context.close()

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err,
                  error_type=type(e).__name__, error=str(e))
        raise


# ====== CLI ==================================
if __name__ == "__main__":
    # Vertical scope (mutually exclusive):
    #   (no flag)                    incremental — current year + fill manifest gaps
    #   --start-year / --end-year    year range only
    #   --force                      all years, wipe manifest entries in scope
    #
    # Horizontal scope:
    #   (no flag)         all types
    #   --transactions    income + expenditures
    #   --entities        candidates + groups
    #   --contributions   income only
    #   --expenditures    expenditures only
    #   --candidates      CDCandidates + CRForms bulk
    #   --committees      GRForms bulk
    import argparse
    ap = argparse.ArgumentParser(
        description="Download Alaska APOC campaign finance data."
    )

    # Vertical — mutually exclusive
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all years in scope, wipe relevant manifest entries")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year); "
                         "use with or without --start-year")

    # Horizontal — top level
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only (income + expenditures)")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (candidates, groups)")

    # Horizontal — second level
    ap.add_argument("--contributions", action="store_true",
                    help="income files only")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditure files only")
    ap.add_argument("--candidates",    action="store_true",
                    help="CDCandidates export + CRForms bulk only")
    ap.add_argument("--committees",    action="store_true",
                    help="GRForms bulk only")
    ap.add_argument("--independent-expenditures", action="store_true",
                    dest="independent_expenditures",
                    help="IE (Form 15-6) bulk exports only -- separate filing track from everything else this scraper covers")

    args, _ = ap.parse_known_args()

    # --end-year requires --start-year or stands alone; validate range
    cy = datetime.today().year
    if args.end_year:
        if args.end_year > cy:
            ap.error(f"--end-year cannot exceed current year ({cy})")
        if args.start_year and args.start_year > args.end_year:
            ap.error("--start-year cannot be greater than --end-year")
    # --force is already mutually exclusive with --start-year via the group;
    # also guard against --force + --end-year
    if args.force and args.end_year:
        ap.error("--force cannot be combined with --end-year")

    try:
        run(
            force=args.force,
            entities=args.entities,
            transactions=args.transactions,
            start_year=args.start_year,
            end_year=args.end_year,
            contributions=args.contributions,
            expenditures=args.expenditures,
            candidates=args.candidates,
            committees=args.committees,
            independent_expenditures=args.independent_expenditures,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
