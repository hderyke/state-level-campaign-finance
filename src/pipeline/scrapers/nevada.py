"""
scrapers/nevada.py — Download Nevada campaign finance data (NVSOS CEFD).

Source: Nevada Secretary of State's Campaign and Election Financial Disclosure
(CEFD) search tool —
  https://www.nvsos.gov/SOSCandidateServices/AnonymousAccess/CEFDSearchUU/Search.aspx

This is a classic ASP.NET WebForms + Telerik/AjaxControlToolkit UI with no JSON
API — every search is a full postback carrying __VIEWSTATE/__EVENTVALIDATION,
and results only exist as CSV via a "download to CSV" link on the results view.
Requires Playwright, driven like a real user (fill fields, click Search, click
the CSV export link) rather than replicated with raw POSTs — viewstate tokens
are per-page-load and chain across postbacks, and the site fronts a
bot-detection beacon (an oddly-named obfuscated analytics script observed on
the page). Must be run from a local machine, not a datacenter IP, per project
convention for WAF/bot-gated states.

Confirmed via a real run: NVSOS actively blocks automated traffic ("IP
Addresses that have an abnormally high amount of requests per second will be
blocked in real-time"), not just a passive fingerprint check. Mitigated here
three ways — none guaranteed against a determined bot-mitigation vendor, but
this is the standard first line of defense: (1) `--disable-blink-features=
AutomationControlled` plus an init script patching `navigator.webdriver` and
a few other properties the common detection snippets check, since Chromium
under CDP control sets these by default; (2) a realistic desktop UA/viewport/
locale/timezone instead of Playwright's defaults; (3) a randomized human-ish
pause after every search submission, including inside the recursive
bisection helpers — the original version only throttled between top-level
year iterations, so a broad query that triggered a 26-way letter-prefix
fallback could fire two dozen requests back-to-back with no delay, which is
the likely trigger for the block hit during testing.

NVSOS refuses to return more than some undocumented number of matching rows —
"This search returns too many results. Please specify additional or more
limiting search criteria." — instead of paginating past it. Rather than
hardcode that threshold, this scraper narrows adaptively: candidates and
transactions are split by year (election year for candidates; calendar year
for contributions/expenditures) as the primary scope, and if a single year
still comes back "too many results," transactions are recursively bisected by
date range and candidates/committees by name prefix until each slice exports
cleanly.

One shared search form covers four tabs (individual/candidate, group/
committee, contribution, expense) distinguished only by a URL fragment
(#individual_search, #group_search, #contribution_search, #expense_search —
the last is inferred from field-naming symmetry and unconfirmed against a
live run). Field ids follow ASP.NET's ctl00$MainContent$X -> id=
"ctl00_MainContent_X" convention, captured directly from a real browser
session's POST payload rather than guessed.

Unconfirmed / to verify on first real run:
  - The #expense_search anchor name.
  - Whether name-prefix search fields do prefix ("starts with") vs substring
    matching — prefix bisection (candidates/committees fallback only) assumes
    prefix matching.
  - True earliest year of electronic filing data — START_YEAR below is a
    wide, safe-but-unverified floor; empty years just cost a fast no-op
    request each run, so erring wide is cheap.
"""

import csv
import random
import string
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths =================================
RAW_DIR  = PROJECT_ROOT / "data" / "Nevada" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Nevada" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]

# ========================= NVSOS site constants =========================
BASE_URL   = "https://www.nvsos.gov/SOSCandidateServices/AnonymousAccess/CEFDSearchUU"
SEARCH_URL = f"{BASE_URL}/Search.aspx"

# In-page tab anchors — the shared search form shows/hides sections based on
# the URL fragment. "expense_search" is inferred from naming symmetry with
# the other three (confirmed via captured POST payload) and not yet verified.
TAB_ANCHORS = {
    "candidates":    "individual_search",
    "committees":    "group_search",
    "contributions": "contribution_search",
    "expenditures":  "expense_search",
}

# ASP.NET WebForms id convention: ctl00$MainContent$X -> id="ctl00_MainContent_X"
# (captured from a real browser POST payload, not guessed).
F = {
    "search_btn": "#ctl00_MainContent_btnSearchMaster",
    "export_link": "#ctl00_MainContent_lbExportCSV",
    "message": "#ctl00_MainContent_lblMessage",

    # individual/candidate search
    "first_name": "#ctl00_MainContent_txtFirstName",
    "last_name": "#ctl00_MainContent_txtLastName",
    "party": "#ctl00_MainContent_ddlParty",
    "office_name": "#ctl00_MainContent_txtOfficeName",
    "jurisdiction": "#ctl00_MainContent_ddlJurisdiction",
    "election_year": "#ctl00_MainContent_ddlElectionYear",

    # group/committee search
    "group_name": "#ctl00_MainContent_txtGroupName",
    "group_type": "#ctl00_MainContent_ddlGroupType",
    "group_city": "#ctl00_MainContent_txtGroupCity",
    "group_first_name": "#ctl00_MainContent_txtGroupFirstName",
    "group_last_name": "#ctl00_MainContent_txtGroupLastName",

    # contribution search
    "cont_name": "#ctl00_MainContent_txtContName",
    "recipient_name": "#ctl00_MainContent_txtRecipientName",
    "cont_date_min": "#ctl00_MainContent_txtRadContDateMin_dateInput",
    "cont_date_max": "#ctl00_MainContent_txtRadContDateMax_dateInput",

    # expense search
    "expense_name": "#ctl00_MainContent_txtExpenseName",
    "payer_name": "#ctl00_MainContent_txtPayerName",
    "expense_date_min": "#ctl00_MainContent_txtRadExpenseDateMin_dateInput",
    "expense_date_max": "#ctl00_MainContent_txtRadExpenseDateMax_dateInput",
    "expense_type": "#ctl00_MainContent_ddlExpenseTypes",
}

# Calendar-year floor for date-range-based relations (contributions,
# expenditures) — no dropdown drives these, so a fixed range is swept.
# See module docstring: unverified, deliberately wide.
START_YEAR = 2000

# Prefix-bisection fallback depth cap for candidates/committees (rarely
# exercised — year / group-type is the primary, reliable narrowing axis).
# depth 0 = bare year/group-type query; depth 1 = single-letter prefix;
# depth 2 = two-letter prefix; give up beyond that rather than spin forever.
MAX_PREFIX_DEPTH = 2

# Randomized pause window (seconds) inserted after every search submission —
# including inside the recursive bisection helpers, not just the top-level
# year loop. Confirmed necessary: an early test run got flagged by NVSOS's
# "abnormally high requests per second" block, almost certainly from an
# unthrottled 26-way letter-prefix fallback firing in quick succession.
HUMAN_PAUSE = (1.5, 3.5)


# ========================= anti-automation setup =========================
# Chromium under CDP control (which is how Playwright drives it) sets
# navigator.webdriver=true and a few other properties by default — the
# single most common signal basic bot-detection scripts check for. None of
# this defeats a serious bot-mitigation vendor, but it's the standard first
# line of defense and costs nothing to include.
LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled"]

DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
VIEWPORT = {"width": 1440, "height": 900}
LOCALE   = "en-US"
# Nevada is Pacific time — matching the context timezone to the target
# state is a minor but easy consistency signal to get right.
TIMEZONE_ID = "America/Los_Angeles"

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


def _new_stealth_context(p):
    """Launch a headed Chromium browser with basic automation fingerprints
    suppressed and a realistic desktop identity. Returns (browser, context, page)."""
    browser = p.chromium.launch(headless=False, args=LAUNCH_ARGS)
    context = browser.new_context(
        accept_downloads=True,
        user_agent=DESKTOP_USER_AGENT,
        viewport=VIEWPORT,
        locale=LOCALE,
        timezone_id=TIMEZONE_ID,
    )
    context.add_init_script(STEALTH_INIT_SCRIPT)
    page = context.new_page()
    return browser, context, page


def _human_pause() -> None:
    """Randomized delay after a search submission — spaces out requests so
    bisection fallbacks don't fire a burst of rapid-fire postbacks."""
    time.sleep(random.uniform(*HUMAN_PAUSE))


# ========================== manifest helpers ============================
def load_manifest() -> tuple[set[tuple[str, str]], set[str]]:
    """Return (done, has_data) — done is a set of (relation_type, year)."""
    done: set[tuple[str, str]] = set()
    has_data: set[str] = set()
    if not MANIFEST.exists():
        return done, has_data
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["year"]))
            has_data.add(row["relation_type"])
    return done, has_data


def strip_manifest(keep_fn) -> None:
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict) -> None:
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


# ========================= Playwright helpers ============================
def _get_select_options(page, selector: str) -> list[str]:
    """Option values for a <select>, excluding placeholder/empty entries."""
    sel = page.locator(selector)
    if not sel.count():
        return []
    opts = sel.locator("option").all()
    return [
        v for v in (o.get_attribute("value") for o in opts)
        if v not in (None, "", "-1", "0")
    ]


def _check_result_state(page) -> str:
    """Return 'too_many' | 'ok' | 'empty' after a search submit.

    'too_many' — NVSOS refused to render results ("This search returns too
                 many results..."); caller should narrow the query and retry.
    'ok'       — the CSV export link is present; safe to export.
    'empty'    — no message, no export link — treated as zero matching rows.
    """
    try:
        msg = page.locator(F["message"])
        if msg.count() and msg.first.is_visible():
            text = msg.first.inner_text().strip().lower()
            if "too many results" in text:
                return "too_many"
    except Exception:
        pass

    try:
        export_link = page.locator(F["export_link"])
        if export_link.count() and export_link.first.is_visible():
            return "ok"
    except Exception:
        pass

    return "empty"


def _download_and_append(page, writer, header_holder: dict, log, label: str) -> int:
    """Click the CSV export link, capture the download, append its rows
    (minus header after the first successful leaf) to `writer`. Returns the
    number of data rows appended."""
    tmp_path = RAW_DIR / "_export_tmp.csv"
    try:
        with page.expect_download(timeout=120_000) as dl_info:
            page.click(F["export_link"])
        dl_info.value.save_as(str(tmp_path))
    except Exception as e:
        log.warning(f"  [!] export failed for {label}: {e}")
        return 0

    with open(tmp_path, newline="", encoding="utf-8-sig", errors="replace") as f:
        rows = list(csv.reader(f))
    tmp_path.unlink(missing_ok=True)

    if not rows:
        return 0
    header, body = rows[0], rows[1:]
    if header_holder["fieldnames"] is None:
        header_holder["fieldnames"] = header
        writer.writerow(header)
    for row in body:
        writer.writerow(row)
    return len(body)


# ------------------------------ candidates --------------------------------
def _download_candidates_year(page, year: str, writer, header_holder: dict, log,
                              last_name_prefix: str = "", depth: int = 0) -> int:
    page.goto(f"{SEARCH_URL}#{TAB_ANCHORS['candidates']}", timeout=30_000)
    page.wait_for_load_state("networkidle")
    if page.locator(F["election_year"]).count():
        page.select_option(F["election_year"], year)
    if last_name_prefix:
        page.fill(F["last_name"], last_name_prefix)
    page.click(F["search_btn"])
    page.wait_for_load_state("networkidle")
    _human_pause()

    state = _check_result_state(page)
    if state == "empty":
        return 0
    if state == "too_many":
        if depth >= MAX_PREFIX_DEPTH:
            log.warning(f"  [!] candidates {year} prefix={last_name_prefix!r} "
                       f"still too many results at max depth — giving up")
            return 0
        return sum(
            _download_candidates_year(page, year, writer, header_holder, log,
                                      last_name_prefix + letter, depth + 1)
            for letter in string.ascii_uppercase
        )
    return _download_and_append(page, writer, header_holder, log,
                                label=f"candidates_{year}_{last_name_prefix}")


# ------------------------------ committees ---------------------------------
def _download_committees_group_type(page, group_type: str, writer, header_holder: dict, log,
                                    name_prefix: str = "", depth: int = 0) -> int:
    page.goto(f"{SEARCH_URL}#{TAB_ANCHORS['committees']}", timeout=30_000)
    page.wait_for_load_state("networkidle")
    if group_type and page.locator(F["group_type"]).count():
        page.select_option(F["group_type"], group_type)
    if name_prefix:
        page.fill(F["group_name"], name_prefix)
    page.click(F["search_btn"])
    page.wait_for_load_state("networkidle")
    _human_pause()

    state = _check_result_state(page)
    if state == "empty":
        return 0
    if state == "too_many":
        if depth >= MAX_PREFIX_DEPTH:
            log.warning(f"  [!] committees group_type={group_type!r} prefix={name_prefix!r} "
                       f"still too many results at max depth — giving up")
            return 0
        return sum(
            _download_committees_group_type(page, group_type, writer, header_holder, log,
                                            name_prefix + letter, depth + 1)
            for letter in string.ascii_uppercase
        )
    return _download_and_append(page, writer, header_holder, log,
                                label=f"committees_{group_type}_{name_prefix}")


# ---------------------------- contributions ---------------------------------
def _download_contributions_range(page, date_min: date, date_max: date,
                                  writer, header_holder: dict, log) -> int:
    page.goto(f"{SEARCH_URL}#{TAB_ANCHORS['contributions']}", timeout=30_000)
    page.wait_for_load_state("networkidle")
    page.fill(F["cont_date_min"], date_min.strftime("%m/%d/%Y"))
    page.locator(F["cont_date_min"]).press("Tab")
    page.fill(F["cont_date_max"], date_max.strftime("%m/%d/%Y"))
    page.locator(F["cont_date_max"]).press("Tab")
    page.click(F["search_btn"])
    page.wait_for_load_state("networkidle")
    _human_pause()

    state = _check_result_state(page)
    if state == "empty":
        return 0
    if state == "too_many":
        if date_min >= date_max:
            log.warning(f"  [!] contributions {date_min} still too many results "
                       f"at single-day granularity — giving up")
            return 0
        mid = date_min + (date_max - date_min) // 2
        total  = _download_contributions_range(page, date_min, mid, writer, header_holder, log)
        total += _download_contributions_range(page, mid + timedelta(days=1), date_max,
                                                writer, header_holder, log)
        return total
    return _download_and_append(page, writer, header_holder, log,
                                label=f"contributions_{date_min}_{date_max}")


# ---------------------------- expenditures ----------------------------------
def _download_expenditures_range(page, date_min: date, date_max: date,
                                 writer, header_holder: dict, log) -> int:
    page.goto(f"{SEARCH_URL}#{TAB_ANCHORS['expenditures']}", timeout=30_000)
    page.wait_for_load_state("networkidle")
    page.fill(F["expense_date_min"], date_min.strftime("%m/%d/%Y"))
    page.locator(F["expense_date_min"]).press("Tab")
    page.fill(F["expense_date_max"], date_max.strftime("%m/%d/%Y"))
    page.locator(F["expense_date_max"]).press("Tab")
    page.click(F["search_btn"])
    page.wait_for_load_state("networkidle")
    _human_pause()

    state = _check_result_state(page)
    if state == "empty":
        return 0
    if state == "too_many":
        if date_min >= date_max:
            log.warning(f"  [!] expenditures {date_min} still too many results "
                       f"at single-day granularity — giving up")
            return 0
        mid = date_min + (date_max - date_min) // 2
        total  = _download_expenditures_range(page, date_min, mid, writer, header_holder, log)
        total += _download_expenditures_range(page, mid + timedelta(days=1), date_max,
                                              writer, header_holder, log)
        return total
    return _download_and_append(page, writer, header_holder, log,
                                label=f"expenditures_{date_min}_{date_max}")


# ============================= orchestrator ==============================
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
):
    """Orchestrate download of Nevada candidates, committees, contributions,
    and expenditures via a Playwright-driven search+export.

    Vertical scope (mutually exclusive):
        force=True              — re-download all years in scope, wipe manifest
        start_year / end_year   — restrict year-based downloads to this range

    Horizontal scope:
        No flags       — download everything
        transactions   — contributions + expenditures
        entities       — candidates + committees
        contributions  — contributions only (implies transactions)
        expenditures   — expenditures only (implies transactions)
        candidates     — candidates only (implies entities)
        committees     — committees only (implies entities)

    Committees have no year dimension on NVSOS's search form and are always
    refetched in full when in scope (registry-style data, same pattern as
    Arkansas's entities) rather than tracked per-year in the manifest.
    """
    log = get_logger("nevada", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("[!] Playwright not installed. Run: pip install playwright && playwright install chromium")
        log._emit("scrape_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, error="playwright not installed")
        return

    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)
    do_candidates    = no_horizontal or entities or candidates
    do_committees    = no_horizontal or entities or committees
    do_contributions = no_horizontal or transactions or contributions
    do_expenditures  = no_horizontal or transactions or expenditures

    files_ok = files_err = 0
    current_year = datetime.today().year

    # ── Scoped manifest clearing ──────────────────────────────────────
    if force:
        relations_to_clear = set()
        if do_candidates:    relations_to_clear.add("candidates")
        if do_contributions: relations_to_clear.add("contributions")
        if do_expenditures:  relations_to_clear.add("expenditures")
        strip_manifest(lambda r: r["relation_type"] not in relations_to_clear)
        # committees has no manifest-gated skip logic — always refetched below

    elif start_year is not None or end_year is not None:
        year_based = set()
        if do_candidates:    year_based.add("candidates")
        if do_contributions: year_based.add("contributions")
        if do_expenditures:  year_based.add("expenditures")

        def _outside_range(r: dict) -> bool:
            if r["relation_type"] not in year_based:
                return True
            try:
                yr = int(r["year"])
            except ValueError:
                return True
            if start_year is not None and yr < start_year:
                return True
            if end_year is not None and yr > end_year:
                return True
            return False

        strip_manifest(_outside_range)

    done, _ = load_manifest()

    try:
        with sync_playwright() as p:
            browser, context, page = _new_stealth_context(p)

            # ── committees ───────────────────────────────────────────
            if do_committees:
                log.info("\nNevada committees:")
                filename      = "committees.csv"
                expected_file = RAW_DIR / filename
                tmp_out       = RAW_DIR / f"{filename}.tmp"
                log.file_download_start(filename=filename)
                t_file = time.perf_counter()
                try:
                    page.goto(f"{SEARCH_URL}#{TAB_ANCHORS['committees']}", timeout=30_000)
                    page.wait_for_load_state("networkidle")
                    group_types = _get_select_options(page, F["group_type"]) or [""]

                    with open(tmp_out, "w", newline="", encoding="utf-8") as fh:
                        writer        = csv.writer(fh)
                        header_holder = {"fieldnames": None}
                        row_count = sum(
                            _download_committees_group_type(page, gt, writer, header_holder, log)
                            for gt in group_types
                        )
                    tmp_out.replace(expected_file)
                    size = expected_file.stat().st_size
                    log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                         duration_s=time.perf_counter() - t_file)
                    files_ok += 1
                    upsert_manifest({"relation_type": "committees", "year": "all",
                                     "filename": filename, "row_count": row_count})
                except Exception as e:
                    log.file_download_error(filename=filename, error=str(e))
                    files_err += 1

            # ── candidates (per election year) ──────────────────────
            if do_candidates:
                log.info("\nNevada candidates:")
                page.goto(f"{SEARCH_URL}#{TAB_ANCHORS['candidates']}", timeout=30_000)
                page.wait_for_load_state("networkidle")
                years = _get_select_options(page, F["election_year"])
                if not years:
                    log.warning("  [!] Could not read election year dropdown — skipping candidates")
                else:
                    max_year = max(years, key=int)
                    for year in years:
                        if start_year is not None and int(year) < start_year:
                            continue
                        if end_year is not None and int(year) > end_year:
                            continue

                        key           = ("candidates", year)
                        filename      = f"candidates_{year}.csv"
                        expected_file = RAW_DIR / filename
                        year_range_active = start_year is not None or end_year is not None
                        already_done = key in done or (
                            not year_range_active
                            and expected_file.exists()
                            and expected_file.stat().st_size > 0
                        )
                        if already_done and year != max_year and not force:
                            log.file_download_skip(filename=filename)
                            continue

                        log.file_download_start(filename=filename)
                        t_file  = time.perf_counter()
                        tmp_out = RAW_DIR / f"{filename}.tmp"
                        try:
                            with open(tmp_out, "w", newline="", encoding="utf-8") as fh:
                                writer        = csv.writer(fh)
                                header_holder = {"fieldnames": None}
                                row_count = _download_candidates_year(page, year, writer, header_holder, log)
                            tmp_out.replace(expected_file)
                            size = expected_file.stat().st_size
                            log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                                 duration_s=time.perf_counter() - t_file)
                            files_ok += 1
                            upsert_manifest({"relation_type": "candidates", "year": year,
                                             "filename": filename, "row_count": row_count})
                            done.add(key)
                        except Exception as e:
                            log.file_download_error(filename=filename, error=str(e))
                            files_err += 1
                        time.sleep(0.5)

            # ── contributions / expenditures (per calendar year) ─────
            for relation_type, do_it, download_fn in (
                ("contributions", do_contributions, _download_contributions_range),
                ("expenditures",  do_expenditures,  _download_expenditures_range),
            ):
                if not do_it:
                    continue
                log.info(f"\nNevada {relation_type}:")
                range_start = start_year if start_year is not None else START_YEAR
                years = [y for y in range(range_start, current_year + 1)
                        if (end_year is None or y <= end_year)]

                for year in years:
                    year_str      = str(year)
                    key           = (relation_type, year_str)
                    filename      = f"{relation_type}_{year_str}.csv"
                    expected_file = RAW_DIR / filename
                    year_range_active = start_year is not None or end_year is not None
                    already_done = key in done or (
                        not year_range_active
                        and expected_file.exists()
                        and expected_file.stat().st_size > 0
                    )
                    if already_done and year != current_year and not force:
                        log.file_download_skip(filename=filename)
                        continue

                    log.file_download_start(filename=filename)
                    t_file  = time.perf_counter()
                    tmp_out = RAW_DIR / f"{filename}.tmp"
                    try:
                        with open(tmp_out, "w", newline="", encoding="utf-8") as fh:
                            writer        = csv.writer(fh)
                            header_holder = {"fieldnames": None}
                            row_count = download_fn(page, date(year, 1, 1), date(year, 12, 31),
                                                    writer, header_holder, log)
                        tmp_out.replace(expected_file)
                        size = expected_file.stat().st_size
                        log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                             duration_s=time.perf_counter() - t_file)
                        files_ok += 1
                        upsert_manifest({"relation_type": relation_type, "year": year_str,
                                         "filename": filename, "row_count": row_count})
                        done.add(key)
                    except Exception as e:
                        log.file_download_error(filename=filename, error=str(e))
                        files_err += 1
                    time.sleep(0.5)

            browser.close()

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} errors")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  error_type=type(e).__name__, error=str(e))
        raise


# ================================= CLI ===================================
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download Nevada campaign finance data (NVSOS CEFD)."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force", action="store_true",
                      help="re-download all years in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); wipes manifest for range")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")

    ap.add_argument("--transactions", action="store_true", help="transactions only")
    ap.add_argument("--entities", action="store_true", help="entities only (committees, candidates)")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures", action="store_true", help="expenditures only")
    ap.add_argument("--candidates", action="store_true", help="candidates only")
    ap.add_argument("--committees", action="store_true", help="committees only")

    args, _ = ap.parse_known_args()

    cy = datetime.today().year
    if args.end_year:
        if args.end_year > cy:
            ap.error(f"--end-year cannot exceed current year ({cy})")
        if args.start_year and args.start_year > args.end_year:
            ap.error("--start-year cannot be greater than --end-year")
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
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
