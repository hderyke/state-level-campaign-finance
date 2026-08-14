"""
scrapers/kansas.py — Drive the Kansas SOS CFR Examiner form to scrape
candidate Receipts & Expenditures filings directly into CSVs.

This replaces the old PDF-download pipeline (kept for reference as
scrapers/kansas_pdf_legacy.py, which downloaded ~3,600 individual PDFs from
the static kansas.gov/ethics/CFAScanned index pages and needed pdfplumber
coordinate heuristics to read them). This scraper reads the same underlying
data straight from the CFR Examiner's HTML, one candidate/schedule at a time:

  1. https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx
     - select category dropdown = "Candidate Campaign Filings"
     - click Submit

  2. On the resulting page, for every (office, election cycle) combination
     in RUN_CATALOG below:
     - Date Range Filed: Start/End = the cycle's date range
     - Filing Type dropdown = "Receipts and Expenditures Report"
     - Office dropdown = the cycle's office text
     - click Submit Search
     - walk every page of results, scraping each candidate's summary
       report + non-empty schedules (A/B/C/D)

Coverage (mirrors the legacy scraper's INDEX_PAGES scope):
    House         — 2016, 2018, 2020, 2022, 2024, 2026 cycles
    Senate        — 2016, 2018-special, 2020, 2022-special, 2024, 2028
    Statewide     — 2014, 2018, 2022, 2026 (Governor, AG, SOS, Treasurer, Ins.)
    District Atty — 2016, 2020, 2024, 2028

Browser: Selenium + Chrome, run HEADED (a visible window). sos.ks.gov is behind
CloudFront, which serves headless Chrome a "403 ERROR / Request blocked" page
instead of the site — confirmed on a live run, where headless got the 403 and a
headed window on the same machine and IP loaded the form fine. This matches
what Florida's and Oregon's scrapers do for their own bot defenses. Selenium 4's
Selenium Manager resolves the matching chromedriver automatically, so a local
Chrome install is the only prerequisite. (Kansas is the one state on Selenium —
the rest of the repo's browser-driven scrapers use Playwright.)

SELECTORS — WHAT'S CONFIRMED
----------------------------
Confirmed on a live headed run (2026-08-10), entry page, no frames present:
  - #ddlViewerOptions (category <select>) and its option
    "Candidate Campaign Filings" (value="Candidate")
  - #btnSubmit (Submit). Note #btnExit is "Back" — not that one.
  - the entry URL redirects to cfr_examiner_entry.aspx

Still inferred rather than dumped live, all on the *next* page:
  - txtStartDate, txtEndDate, drpdownFilingType, trOffice (from DOM notes)
  - whether the "Submit Search" button is #btnSearch or reuses #btnSubmit
  - the exact visible-text for each office in RUN_CATALOG (only "State
    Representative" was ever confirmed). A cycle that logs "no option
    containing..." means the text guess is wrong.
  - the date ranges per cycle are inferred (2-year span for House, 4-year for
    Senate/Statewide/DA), not confirmed against how KS windows its "Date
    Range Filed" filter. If a cycle returns suspiciously few/many rows,
    narrow/widen the spans in _build_run_catalog().

Run --debug-controls to dump every control at all three stages (entry page,
search form, results page); it stops after one cycle.

To stay resilient to unknown ids, this script locates dropdowns defensively
(an id that turns out to wrap a <select> rather than being one is handled),
and selects options by *visible text*, not by value.

Project integration:
    Raw output (data/Kansas/raw/):
        candidates_summary.csv, schedule_a_contributions.csv,
        schedule_b_inkind.csv, schedule_c_expenditures.csv,
        schedule_d_other.csv
        candidate_roster.csv — one row per candidate per election from the
        SOS Candidate List page, and the ONLY published source of candidate
        party for Kansas (the CFR Examiner has no party field anywhere).
        parsers/kansas.py joins it on by name/office/district/year.
        Every row in every one of these five files carries a candidate_uid
        column (same value as the manifest's candidate_key:
        "office_group|cycle_label|office_sought|district_number|name|
        original_date|amendment_date") so the parser can join schedule rows
        back to the right candidate without relying on name text alone,
        which collides across cycles/offices. See parsers/kansas.py.
    Manifest (data/Kansas/manifest.csv):
        candidate_key, office_group, cycle_label, office_text,
        candidate_name, office_sought, district_number, original_date,
        amendment_date, scraped_at
        A manifest left behind by the legacy PDF scraper has an incompatible
        schema; it is moved aside to manifest_pdf_legacy.csv on first run
        rather than being appended to.
    Logging: src.reporting.logger.get_logger("kansas", "scrape")

CLI (same vertical flags as every other scraper; horizontal flags accepted
and ignored, since a KS filing contains all data types at once):
    (no flag)        incremental — skip candidates already in the manifest;
                     always re-check cycles whose year is >= the current year
    --start-year     only scrape cycles with cycle year >= YYYY
    --end-year       only scrape cycles with cycle year <= YYYY
    --force          wipe manifest and CSVs, re-scrape everything
"""

import csv
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import NamedTuple

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

try:
    from config import USER_AGENT
except ImportError:      # config.py is repo-root local; fall back if missing
    USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/136.0.0.0 Safari/537.36")

# =============================== paths ================================
RAW_DIR         = PROJECT_ROOT / "data" / "Kansas" / "raw"
MANIFEST        = PROJECT_ROOT / "data" / "Kansas" / "manifest.csv"
LEGACY_MANIFEST = PROJECT_ROOT / "data" / "Kansas" / "manifest_pdf_legacy.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [
    "candidate_key", "office_group", "cycle_label", "office_text",
    "candidate_name", "office_sought", "district_number", "original_date",
    "amendment_date", "scraped_at",
]

# Output CSV basenames, written into the raw dir (or --out-dir).
CSV_NAMES = {
    "summary":    "candidates_summary.csv",
    "schedule_a": "schedule_a_contributions.csv",
    "schedule_b": "schedule_b_inkind.csv",
    "schedule_c": "schedule_c_expenditures.csv",
    "schedule_d": "schedule_d_other.csv",
}

# ---- candidate roster (party source) -------------------------------------
# The CFR Examiner publishes no party affiliation anywhere. The SOS's own
# Candidate List page does, in a plain table with one row per candidate per
# election, selectable by election from 2002 onward — so party is joined on in
# parsers/kansas.py from this roster rather than being unavailable.
ROSTER_URL          = "https://sos.ks.gov/elections/elections_upcoming_candidate.aspx"
ROSTER_SELECT_CSS   = "#ddlElections"     # options like "2024 General" / "2024 Primary"
ROSTER_SUBMIT_CSS   = "#btnSubmit"
ROSTER_TABLE_ID     = "gvCandidateList"
ROSTER_CSV          = "candidate_roster.csv"

# Written from the table's own headers, normalized to snake_case; the parser
# only needs election_*/party/office/district/name, the rest is kept because
# it's free and useful for hand-checking a bad match.
ROSTER_FIELDS = [
    "election_label", "election_year", "election_type",
    "candidate", "office", "district", "position", "division", "party",
    "title", "first_name", "middle", "last_name", "suffix",
    "home_city", "home_zip", "date_filed",
]

# Table header text -> our column name. Headers not listed here are ignored
# (phone numbers, email, addresses, ballot city, running mate).
ROSTER_HEADER_MAP = {
    "candidate": "candidate", "office": "office", "district": "district",
    "position": "position", "division": "division", "party": "party",
    "title": "title", "first name": "first_name", "middle": "middle",
    "last name": "last_name", "suffix": "suffix",
    "home city": "home_city", "home zip": "home_zip", "date filed": "date_filed",
}

ENTRY_URL = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx"

RESULTS_TABLE_ID = "grdviewCfrResults"

ENTRY_URL_REDIRECTS_TO = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner_entry.aspx"

# ---- selectors ----------------------------------------------------------
# CONFIRMED on a live headed run (2026-08-10), entry page:
#   #ddlViewerOptions — the category <select>, in the TOP document (no frames
#     anywhere on this page). Its options are:
#       ''  'Contribution'  'Expenditure'  'Candidate Campaign Filings'
#       'PAC/Party Political Committee'  'Gubernatorial Inauguration'
#       'Individual Entity'
#     ("Candidate Campaign Filings" has value="Candidate".)
#   #btnSubmit — the submit button (#btnExit is the "Back" button; don't
#     click that one).
#
# Still from DOM notes rather than a live dump: txtStartDate, txtEndDate,
# drpdownFilingType, trOffice on the *next* page. "trOffice" was reported as
# the office control's id, but `tr` is the usual HTML prefix for a <tr>, not a
# <select> — ASP.NET forms often wrap a dropdown's label+control in a row with
# an id like that. So we try #trOffice directly, and if it isn't itself a
# <select>, we look for a <select> nested inside it.
CATEGORY_SELECT_CSS = "#ddlViewerOptions"
CATEGORY_SUBMIT_CSS = "#btnSubmit"

START_DATE_CSS         = "#txtStartDate"
END_DATE_CSS           = "#txtEndDate"
FILING_TYPE_SELECT_CSS = "#drpdownFilingType"
OFFICE_SELECT_CSS      = "#drpdownOffice"   # confirmed live; #trOffice is its wrapper row
OFFICE_CONTAINER_ID    = "trOffice"         # fallback if the select id ever changes
SEARCH_SUBMIT_CSS      = "#btnSearch"

# How long to sit out a rate-limit block before retrying, in seconds. Roughly
# 30 minutes in total across the four attempts.
BLOCK_BACKOFFS = (60, 180, 420, 900)

# Pause between candidates. The site rate-limits a sustained burst, and a full
# scrape is tens of thousands of page loads, so a small delay is the
# difference between finishing and being cut off. --delay overrides it.
DEFAULT_REQUEST_DELAY = 0.5
REQUEST_DELAY = DEFAULT_REQUEST_DELAY

WAIT_TIMEOUT    = 20    # seconds — normal element/navigation waits
POPUP_TIMEOUT   = 1     # seconds — how long to watch for a click opening a new window
POPUP_POLL      = 0.1   # seconds — poll interval while watching for that window
RESULTS_TIMEOUT = 15    # seconds — how long to wait for the grid before calling a search empty
FRAME_TIMEOUT   = 5     # seconds — how long to hunt through frames for a control

# Markers used to find (and re-find, after a navigation resets frame focus)
# the document holding a report or schedule.
SUMMARY_MARKER_CSS = "#lblCandOrgName"
SCHEDULE_TABLE_CSS = "table[width='98%'][border='1']"

# ==================== Office / cycle catalog ===========================
# (office_group, cycle_label, office_dropdown_text, date_start, date_end)
#
# Mirrors the legacy scraper's INDEX_PAGES scope (House/Senate/Statewide/DA
# across the same election cycles), but expressed as CFR-examiner search
# parameters instead of index-page URLs. cycle_label matches the label the
# legacy scraper used, so downstream code/analysis keyed on it still lines up.
# Office strings must match the drpdownOffice options exactly enough to
# substring-match. Confirmed live (2026-08-11), the full option list is:
#   Attorney General | Court of Appeals Judge | District Attorney |
#   District Court Judge | District Magistrate Judge | Governor |
#   Insurance Commissioner | Secretary of State | State Board of Education |
#   State Representative | State Senator | State Treasurer |
#   Supreme Court Judge
# Note "State Senator", NOT "State Senate" — the latter matched nothing and
# silently failed all six Senate cycles.
OFFICE_SENATE = "State Senator"
OFFICE_HOUSE  = "State Representative"
OFFICE_DA     = "District Attorney"

# ---- viewer categories ---------------------------------------------------
# The entry page's category dropdown picks which search form you get. Both
# forms are the same shape — same control ids (txtStartDate, txtEndDate,
# drpdownFilingType, drpdownOffice, btnSearch), the same grdviewCfrResults
# grid and the same report/schedule pages — so one code path drives both. The
# only differences: the PAC form labels drpdownOffice "Type of Committee" and
# offers committee types instead of offices, and its grid has no
# office/district columns (parse_results_page just reads those as blank).
CATEGORY_CANDIDATE = "Candidate Campaign Filings"
CATEGORY_PAC       = "PAC/Party Political Committee"

COMMITTEE_PAC   = "Political Action Committee"   # drpdownOffice value="12"
COMMITTEE_PARTY = "Party Political Committee"    # drpdownOffice value="13"

# PACs and party committees file on a rolling basis rather than per election
# cycle, so they're swept one calendar year at a time. 2016 pairs with where
# the candidate data is densest; lower it for deeper history (the SOS roster
# goes back to 2002) at the cost of two more searches per year.
PAC_START_YEAR = 2016

HOUSE_CYCLES     = ["2016", "2018", "2020", "2022", "2024", "2026"]
SENATE_CYCLES    = ["2016", "2018-special", "2020", "2022-special", "2024", "2028"]
DA_CYCLES        = ["2016", "2020", "2024", "2028"]
STATEWIDE_CYCLES = ["2014", "2018", "2022", "2026"]
STATEWIDE_OFFICES = [
    "Governor", "Attorney General", "Secretary of State",
    "State Treasurer", "Insurance Commissioner",
]

# The remaining offices the dropdown offers. Both use DA_CYCLES' 4-year
# windows, which are contiguous (2013-2016, 2017-2020, 2021-2024, 2025-2028) —
# and since the form filters on *filing* date, contiguous windows guarantee
# complete coverage regardless of how long each office's term actually runs.
BOE_OFFICE = "State Board of Education"
JUDICIAL_OFFICES = [
    "Supreme Court Judge", "Court of Appeals Judge",
    "District Court Judge", "District Magistrate Judge",
]


def _cycle_span(cycle_label: str, years: int) -> tuple[str, str]:
    """'2026' + years=2 -> ('01/01/2025', '12/31/2026'). Handles
    '-special' suffixes (e.g. '2022-special') as a single-year window."""
    if cycle_label.endswith("-special"):
        year = int(cycle_label.split("-")[0])
        return f"01/01/{year}", f"12/31/{year}"
    year = int(cycle_label)
    return f"01/01/{year - years + 1}", f"12/31/{year}"


class Search(NamedTuple):
    """One search to run: which viewer category, which dropdown option, and
    the date window. `group` and `cycle_label` are what land in the manifest
    key and candidate_uid, so the parser can tell a PAC filing from a House
    one without re-deriving it."""
    category:    str    # CATEGORY_CANDIDATE | CATEGORY_PAC
    group:       str    # House | Senate | DA | Statewide | PAC | Party
    cycle_label: str    # election cycle, or the calendar year for PAC/Party
    option_text: str    # office, or committee type on the PAC form
    date_start:  str
    date_end:    str


def _build_run_catalog() -> list[Search]:
    """Build the full list of searches run() will perform. Called once at
    import time to populate RUN_CATALOG."""
    catalog: list[Search] = []

    # House: one search per 2-year cycle, all using the same dropdown text.
    for cycle in HOUSE_CYCLES:
        start, end = _cycle_span(cycle, 2)
        catalog.append(Search(CATEGORY_CANDIDATE, "House", cycle, OFFICE_HOUSE, start, end))

    # Senate: 4-year cycles, except "-special" cycles which get a 1-year
    # window since they're a single off-cycle election, not a full term.
    for cycle in SENATE_CYCLES:
        years = 1 if cycle.endswith("-special") else 4
        start, end = _cycle_span(cycle, years)
        catalog.append(Search(CATEGORY_CANDIDATE, "Senate", cycle, OFFICE_SENATE, start, end))

    # District Attorney: 4-year cycles, same pattern as Senate (no specials).
    for cycle in DA_CYCLES:
        start, end = _cycle_span(cycle, 4)
        catalog.append(Search(CATEGORY_CANDIDATE, "DA", cycle, OFFICE_DA, start, end))

    # Statewide offices each need their own separate search — the site's
    # Office dropdown only accepts one selection at a time — so every cycle
    # expands into 5 catalog entries sharing that cycle's date range.
    for cycle in STATEWIDE_CYCLES:
        start, end = _cycle_span(cycle, 4)
        for office_text in STATEWIDE_OFFICES:
            catalog.append(Search(CATEGORY_CANDIDATE, "Statewide", cycle, office_text, start, end))

    # State Board of Education: partisan, statewide-by-district.
    for cycle in DA_CYCLES:
        start, end = _cycle_span(cycle, 4)
        catalog.append(Search(CATEGORY_CANDIDATE, "BOE", cycle, BOE_OFFICE, start, end))

    # Judicial races. District Court and District Magistrate seats are elected
    # on a partisan ballot where they're elected at all, and the SOS roster
    # gives them a party; appellate seats (Supreme Court, Court of Appeals) are
    # retention elections and the roster leaves their party blank.
    for cycle in DA_CYCLES:
        start, end = _cycle_span(cycle, 4)
        for office_text in JUDICIAL_OFFICES:
            catalog.append(Search(CATEGORY_CANDIDATE, "Judicial", cycle,
                                  office_text, start, end))

    # PACs and party committees: not tied to an election cycle, so sweep one
    # calendar year at a time. Two searches per year — the form's committee
    # type dropdown takes one selection at a time, exactly like Office.
    for year in range(PAC_START_YEAR, datetime.today().year + 1):
        start, end = f"01/01/{year}", f"12/31/{year}"
        catalog.append(Search(CATEGORY_PAC, "PAC", str(year), COMMITTEE_PAC, start, end))
        catalog.append(Search(CATEGORY_PAC, "Party", str(year), COMMITTEE_PARTY, start, end))

    return catalog


RUN_CATALOG = _build_run_catalog()


def _cycle_year(cycle_label: str) -> int:
    try:
        return int(cycle_label.split("-")[0])
    except ValueError:
        return 0


def is_current_cycle(cycle_label: str, current_year: int) -> bool:
    """True if this cycle could still receive new filings (year >= current_year)."""
    return _cycle_year(cycle_label) >= current_year


# ========================= Manifest helpers ===========================

def load_manifest() -> dict[str, dict]:
    """Return {candidate_key: row} for all entries in the manifest.

    A manifest written by the legacy PDF scraper (filename/office/period
    columns) is moved aside rather than appended to — the two schemas are
    incompatible and mixing them would corrupt both.
    """
    if not MANIFEST.exists():
        return {}  # first run ever — nothing scraped yet
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        if fields is None or "candidate_key" not in fields:
            f.close()
            MANIFEST.replace(LEGACY_MANIFEST)
            print(f"NOTE: {MANIFEST.name} had an incompatible schema (no "
                  f"'candidate_key' column — most likely the legacy PDF "
                  f"scraper's manifest). Moved it to {LEGACY_MANIFEST.name} "
                  f"and starting a fresh manifest.")
            return {}
        return {row["candidate_key"]: row for row in reader}


def strip_manifest(keep_fn) -> None:
    """Rewrite MANIFEST keeping only rows for which keep_fn(row) is True.
    Used by run() to wipe everything (--force) or to drop rows outside a
    --start-year/--end-year window before re-scraping that window."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "candidate_key" not in reader.fieldnames:
            return  # foreign/legacy manifest — load_manifest() handles it
        rows = list(reader)
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict) -> None:
    """Append one candidate's manifest record to disk immediately after
    scraping them, so progress survives an interrupted run."""
    write_header = not MANIFEST.exists() or MANIFEST.stat().st_size == 0
    with open(MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow(record)


def make_candidate_key(office_group: str, cycle_label: str, row: dict) -> str:
    """Stable-ish key for manifest/resume dedup, built from fields already
    visible on the results grid (no need to open the candidate's report to
    decide whether to skip it)."""
    return "|".join([
        office_group,
        cycle_label,
        row.get("office_sought", ""),
        row.get("district_number", ""),
        row.get("name", ""),
        row.get("original_date", ""),
        row.get("amendment_date", ""),
    ])


# ========================== Browser helpers ===========================

def make_driver(headless: bool = False) -> webdriver.Chrome:
    """Launch Chrome.

    HEADED BY DEFAULT — same as Florida's and Oregon's scrapers, and for the
    same reason. sos.ks.gov sits behind CloudFront, which serves headless
    Chrome a "403 ERROR / Request blocked" page instead of the site
    (confirmed on a live run: headless got the 403, a headed window on the
    same machine and IP loaded the form fine). --headless is available if you
    want to try it (e.g. from a residential IP where the block may not
    trigger), but expect 403s.

    Also spoofs config.USER_AGENT and turns off the AutomationControlled
    blink feature, the two cheapest ways to look less like automation.
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-agent={USER_AGENT}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(WAIT_TIMEOUT * 3)
    return driver


# CloudFront's block page — matched on the title/body so a blocked run says
# "you were blocked" instead of "selector not found on a page with no
# controls", which is what the first live run had to be diagnosed from.
_BLOCK_MARKERS = (
    "The request could not be satisfied",
    "Request blocked",
    "403 ERROR",
)


def _clamp_future_date(date_str: str, today: date | None = None) -> str:
    """Clamp an MM/DD/YYYY date to today if it's in the future.

    The Date Range Filed fields reject a future date outright ("Cannot be
    future date") and the search never runs. Every still-open cycle in
    RUN_CATALOG ends 12/31 of its election year, so House 2026, Senate 2028
    and DA 2028 were all silently returning nothing — the run logged them as
    "empty cycles" when in fact the form had refused the input.
    """
    today = today or datetime.today().date()
    try:
        d = datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        return date_str
    return today.strftime("%m/%d/%Y") if d > today else date_str


# Text the form shows when it rejects the date range, matched case-insensitively
# so a rejected search is reported as a rejection rather than "no results".
_VALIDATION_PHRASES = (
    "cannot be future date",
    "future date",
    "invalid date",
    "date range",
)


def _page_validation_error(driver) -> str | None:
    """Return the form's validation message, if the page is showing one."""
    try:
        body = driver.execute_script(
            "return (document.body && document.body.innerText || '')") or ""
    except WebDriverException:
        return None
    for line in (l.strip() for l in body.splitlines()):
        low = line.lower()
        if line and any(p in low for p in _VALIDATION_PHRASES):
            return line[:200]
    return None


class Blocked(Exception):
    """The edge served a block page instead of the site.

    Distinct from a scrape failure: nothing is wrong with the filing, we're
    just being rate-limited, and the cure is to wait rather than to skip.
    """


def is_blocked(driver) -> bool:
    """True if the current page is CloudFront's block page rather than the site."""
    try:
        title = driver.title or ""
        body = driver.execute_script(
            "return (document.body && document.body.innerText || '').slice(0, 400)") or ""
    except WebDriverException:
        return False
    return any(m in f"{title}\n{body}" for m in _BLOCK_MARKERS)


def _raise_if_blocked(driver) -> None:
    """Raise a self-explanatory error if the edge served a block page."""
    if is_blocked(driver):
        raise RuntimeError(
            f"sos.ks.gov returned a CloudFront block page "
            f"(title={driver.title!r}) instead of the CFR Examiner. Headless "
            f"Chrome always gets this — run without --headless (the default). "
            f"Mid-run it means rate limiting instead: see --delay."
        )


def wait_out_block(driver, log, timeout: int = WAIT_TIMEOUT) -> bool:
    """Sit out a rate-limit block, returning True once the site answers again.

    sos.ks.gov's edge starts refusing after a burst of requests — a live run
    hit it ~54 candidates in, and every page after that was the block page.
    Backing off and retrying is the only thing that helps (and is the polite
    response); skipping ahead just burns through cycles marking good filings
    as failures.
    """
    for attempt, pause in enumerate(BLOCK_BACKOFFS, start=1):
        log.warning(f"  blocked by the site — backing off {pause}s "
                    f"({attempt}/{len(BLOCK_BACKOFFS)})")
        time.sleep(pause)
        try:
            driver.get(ENTRY_URL)
            if not is_blocked(driver):
                log.info("  block cleared — resuming")
                return True
        except WebDriverException as e:
            log.warning(f"  reload during backoff failed: {e}")
    return False


def wait_for(driver, css_selector: str, timeout: int = WAIT_TIMEOUT):
    """Wait for a CSS selector in the *currently focused* document.

    Selenium's own TimeoutException carries an empty message, which in a log
    is indistinguishable from any other timeout; this re-raises with the
    selector, URL and page title so a failure says what was being looked for
    and where.
    """
    try:
        return WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
        )
    except TimeoutException:
        raise TimeoutException(
            f"no element matching {css_selector!r} after {timeout}s "
            f"(url={getattr(driver, 'current_url', '?')!r}, "
            f"title={getattr(driver, 'title', '?')!r})"
        ) from None


# ---- frame handling -------------------------------------------------------
# The CFR Examiner renders its form inside a frame rather than the top-level
# document (a first live run found zero <select> and zero <input> elements in
# the top document). Selenium only sees the focused document, so every step
# that looks for a control first focuses whichever frame actually contains it.

def focus_frame_with(driver, css_selector: str, timeout: int = WAIT_TIMEOUT):
    """Switch the driver to the document containing `css_selector`.

    Checks the top document first, then each <iframe>/<frame> (one level
    deep, then one level inside those). Returns a description of where it
    landed, or None if nothing matched anywhere — in which case the driver is
    left on the top document and the caller's own wait_for() produces the
    error.
    """
    deadline = time.time() + timeout
    while True:
        driver.switch_to.default_content()
        if driver.find_elements(By.CSS_SELECTOR, css_selector):
            return "top"

        outer = driver.find_elements(By.TAG_NAME, "iframe") + \
                driver.find_elements(By.TAG_NAME, "frame")
        for i in range(len(outer)):
            driver.switch_to.default_content()
            frames = driver.find_elements(By.TAG_NAME, "iframe") + \
                     driver.find_elements(By.TAG_NAME, "frame")
            if i >= len(frames):
                break
            try:
                driver.switch_to.frame(frames[i])
            except WebDriverException:
                continue
            if driver.find_elements(By.CSS_SELECTOR, css_selector):
                return f"frame[{i}]"
            # one more level down — some ASP.NET portals nest a content frame
            inner = driver.find_elements(By.TAG_NAME, "iframe") + \
                    driver.find_elements(By.TAG_NAME, "frame")
            for j in range(len(inner)):
                try:
                    driver.switch_to.frame(inner[j])
                except WebDriverException:
                    continue
                if driver.find_elements(By.CSS_SELECTOR, css_selector):
                    return f"frame[{i}][{j}]"
                driver.switch_to.parent_frame()

        if time.time() >= deadline:
            driver.switch_to.default_content()
            return None
        time.sleep(0.5)   # controls may still be rendering


def _wait_ready(driver, timeout: int = WAIT_TIMEOUT) -> None:
    """Wait for document.readyState == 'complete'."""
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def _select_option_by_text(element, text_substring: str) -> str:
    """Choose the option under `element` whose visible text contains
    text_substring (case-insensitive), and return its label.

    Matching on visible text rather than `value` is deliberate: the option
    values can't be seen without loading the page, and the visible strings are
    what was observed. Clicking the <option> is exactly what Selenium's own
    Select._set_selected does, without pulling in Select's stricter
    exact-text matching.
    """
    target = text_substring.strip().lower()
    options = element.find_elements(By.TAG_NAME, "option")
    for option in options:
        if target in option.text.strip().lower():
            if not option.is_selected():
                option.click()
            return option.text
    # No match — raise with the real option list so it's obvious whether the
    # text guess was wrong or the option truly isn't there.
    raise ValueError(
        f"No option containing {text_substring!r} found. "
        f"Available options: {[o.text for o in options]}"
    )


def select_by_visible_text_containing(driver, select_css: str, text_substring: str,
                                      timeout: int = WAIT_TIMEOUT) -> str:
    """Visible-text option selection for a <select> addressed by CSS."""
    el = wait_for(driver, select_css, timeout)
    try:
        return _select_option_by_text(el, text_substring)
    except ValueError as e:
        raise ValueError(f"{e} (selector {select_css!r})") from None


def resolve_select_element(driver, container_id: str, timeout: int = WAIT_TIMEOUT):
    """
    Given an id that might belong directly to a <select>, or to a wrapping
    element (e.g. a <tr>/<td>/<div>) that contains the actual <select>,
    return the <select> element itself.
    """
    el = wait_for(driver, f"#{container_id}", timeout)
    if el.tag_name.lower() == "select":
        return el  # the id belongs directly to the <select>
    nested = el.find_elements(By.TAG_NAME, "select")
    if nested:
        return nested[0]  # id was a wrapper (e.g. <tr>) — use the <select> in it
    raise ValueError(
        f"#{container_id} is a <{el.tag_name}>, not a <select>, and no "
        f"<select> was found nested inside it."
    )


def reset_pager_to_first_page(driver, log, timeout: int = WAIT_TIMEOUT) -> None:
    """Make sure a freshly-submitted search is showing page 1.

    The GridView keeps its page index in server-side state across searches:
    after one cycle's walk ended on page 85, the *next* cycle's search
    re-rendered the grid still at page 85. That page is past the end of the
    new (smaller) result set, so the scrape saw a handful of rows, then
    "advanced" to a page that doesn't exist and ended the cycle. In a live run
    this meant every cycle after the first House one returned ~10 candidates
    instead of hundreds, with no error anywhere.
    """
    try:
        current = get_current_page_number(driver)
    except WebDriverException:
        return
    if current == 1:
        return

    log.info(f"  grid opened on page {current} (state left over from the previous "
             f"search) — resetting to page 1")

    # Page 1 usually isn't clickable from here: the pager only shows one group
    # (e.g. pages 81-90 when sitting on page 85), so getting back means walking
    # groups down via the backward "..." until page 1 is on screen.
    MAX_GROUP_JUMPS = 40      # 40 groups x 10 pages — far beyond any real result set
    try:
        for _ in range(MAX_GROUP_JUMPS):
            if 1 in get_available_pager_pages(driver):
                go_to_results_page(driver, 1, timeout)
                break
            backward, _forward = _ellipsis_ranks(driver)
            if backward is None or not _click_ellipsis(driver, backward, timeout):
                log.warning("  no backward '...' to walk down — cannot reach page 1")
                break
        else:
            log.warning(f"  gave up walking back to page 1 after "
                        f"{MAX_GROUP_JUMPS} page-group jumps")
    except Exception as e:
        log.warning(f"  could not reset the grid to page 1: {e}")
        return

    landed = get_current_page_number(driver)
    if landed != 1:
        log.warning(f"  grid still on page {landed} after asking for page 1 — "
                    f"this cycle may be incomplete")


def select_office_by_text(driver, container_id: str, text_substring: str,
                          timeout: int = WAIT_TIMEOUT) -> str:
    """Visible-text selection for the Office / Type of Committee dropdown.

    Prefers the confirmed select id and falls back to the wrapper row, whose
    id was the only one known before a live dump ({OFFICE_SELECT_CSS} is a
    <select>; #trOffice is the <tr> around it).
    """
    if driver.find_elements(By.CSS_SELECTOR, OFFICE_SELECT_CSS):
        return select_by_visible_text_containing(driver, OFFICE_SELECT_CSS,
                                                 text_substring, timeout)
    el = resolve_select_element(driver, container_id, timeout)
    try:
        return _select_option_by_text(el, text_substring)
    except ValueError as e:
        raise ValueError(f"{e} (under #{container_id})") from None


def _safe_click(driver, element) -> None:
    """Click an element, tolerating GridView links WebDriver won't touch.

    Every link in the results grid and on the report pages is an ASP.NET
    postback anchor (`javascript:__doPostBack(...)`). WebDriver refuses to
    click an element it judges non-interactable — off-screen, zero-sized, or
    covered — and a live run lost ~40% of rows to
    `ElementNotInteractableException`, clustered on rows far down a long page.

    Escalates: plain click → scroll to centre and click → JS `.click()`. The
    JS click fires the same postback the anchor's own href would, so it's the
    behaviour we want, just without WebDriver's visibility gate.
    """
    try:
        element.click()
        return
    except (ElementNotInteractableException, ElementClickInterceptedException):
        pass

    try:
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
        time.sleep(0.1)
        element.click()
        return
    except (ElementNotInteractableException, ElementClickInterceptedException):
        pass

    driver.execute_script("arguments[0].click();", element)


def _click_and_wait(driver, element, timeout: int = WAIT_TIMEOUT) -> None:
    """Click something that triggers an ASP.NET postback and wait for the
    resulting page load: the whole document is replaced, so the old <body>
    going stale is the signal that the new page has arrived."""
    try:
        marker = driver.find_element(By.TAG_NAME, "body")
    except (NoSuchElementException, WebDriverException):
        marker = None

    _safe_click(driver, element)

    if marker is not None:
        try:
            WebDriverWait(driver, timeout).until(EC.staleness_of(marker))
        except TimeoutException:
            pass  # partial/async postback — no full navigation, that's fine
    try:
        _wait_ready(driver, timeout)
    except TimeoutException:
        pass


def click_and_capture_page(driver, element, timeout: int = WAIT_TIMEOUT):
    """
    Click `element` and return (html, new_window_handle_or_None), handling
    both ways Kansas's ASP.NET postback links have been observed to behave:

      (a) In-place navigation: the click submits the form and the server
          redirects the *same* window to a new .aspx page (this is what the
          saved report/schedule HTML shows — exp_report_main.aspx,
          schedule_a_report.aspx, etc.)
      (b) New window/tab: some links (e.g. the candidate name link in the
          results grid) carry a title like "open ... in a new window".

    If a new window opened, the driver is left focused on it and the caller
    must pass that handle to close_and_return() when done; otherwise pass
    None and close_and_return() will use driver.back().
    """
    original_handles = set(driver.window_handles)
    try:
        marker = driver.find_element(By.TAG_NAME, "body")
    except (NoSuchElementException, WebDriverException):
        marker = None

    _safe_click(driver, element)

    # Case (b): did a new window/tab appear? Kept short and polled fast — in
    # practice every observed click navigates in place, so this wait is pure
    # overhead on the common path, and it runs ~5x per candidate (summary plus
    # each non-empty schedule). At 3s it was the single largest cost in a
    # multi-hour run; a popup that is going to open does so immediately.
    try:
        WebDriverWait(driver, POPUP_TIMEOUT, poll_frequency=POPUP_POLL).until(
            lambda d: len(set(d.window_handles) - original_handles) > 0
        )
        new_handle = (set(driver.window_handles) - original_handles).pop()
        driver.switch_to.window(new_handle)
        try:
            _wait_ready(driver, timeout)
        except TimeoutException:
            pass
        return driver.page_source, new_handle
    except TimeoutException:
        pass  # no new window — assume case (a), in-place navigation

    if marker is not None:
        try:
            WebDriverWait(driver, timeout).until(EC.staleness_of(marker))
        except TimeoutException:
            pass
    try:
        _wait_ready(driver, timeout)
    except TimeoutException:
        pass
    return driver.page_source, None


def close_and_return(driver, new_handle, return_handle, timeout: int = WAIT_TIMEOUT) -> None:
    """Undo click_and_capture_page(): close the popup window if one was
    opened, otherwise navigate back in history. Always ends with focus on
    return_handle."""
    if new_handle is not None:
        driver.switch_to.window(new_handle)
        driver.close()
        driver.switch_to.window(return_handle)
        return
    driver.back()
    try:
        _wait_ready(driver, timeout)
    except TimeoutException:
        pass


def dismiss_datepicker(driver) -> None:
    """
    The date fields open a JS calendar popup on focus/input. If it's still
    open it visually overlaps other controls (like the Submit Search button)
    and intercepts clicks meant for them. Escape + blurring the active
    element closes it without clicking anywhere that might itself navigate.
    """
    try:
        driver.switch_to.active_element.send_keys(Keys.ESCAPE)
    except Exception:
        pass
    try:
        driver.execute_script("document.activeElement && document.activeElement.blur();")
    except Exception:
        pass
    time.sleep(0.3)


def _dump_controls_here(driver, where: str) -> int:
    """Print every select/input in the currently focused document. Returns
    how many controls were found, so the caller can tell "this frame is
    empty" apart from "this frame is the one"."""
    selects = driver.find_elements(By.TAG_NAME, "select")
    inputs  = driver.find_elements(By.TAG_NAME, "input")
    buttons = driver.find_elements(By.TAG_NAME, "button")
    print(f"\n=== controls in {where} ===")
    for el in selects:
        print(f"  <select> id={el.get_attribute('id')!r} name={el.get_attribute('name')!r}")
        for opt in el.find_elements(By.TAG_NAME, "option"):
            print(f"      option: {opt.text.strip()!r} value={opt.get_attribute('value')!r}")
    for el in inputs:
        print(f"  <input>  id={el.get_attribute('id')!r} name={el.get_attribute('name')!r} "
              f"type={el.get_attribute('type')!r} value={el.get_attribute('value')!r}")
    for el in buttons:
        print(f"  <button> id={el.get_attribute('id')!r} name={el.get_attribute('name')!r} "
              f"text={el.text.strip()!r}")
    if not (selects or inputs or buttons):
        print("  (no form controls in this document)")
    return len(selects) + len(inputs) + len(buttons)


def debug_dump_controls(driver) -> None:
    """Print where we are and every form control we can reach — the top
    document plus every frame, one level deep.

    Run with --debug-controls (or let a selector failure trigger it) when the
    form's ids drift. It reports the URL/title/readyState and a snippet of
    visible text first, because "no controls anywhere" usually means the page
    that loaded isn't the page we think it is (redirect, error page, or a
    block), not that the ids changed.
    """
    try:
        driver.switch_to.default_content()
    except WebDriverException:
        pass

    print("\n================ PAGE DIAGNOSTICS ================")
    print(f"  url        : {driver.current_url}")
    print(f"  title      : {driver.title!r}")
    try:
        print(f"  readyState : {driver.execute_script('return document.readyState')}")
        html_len = driver.execute_script("return document.documentElement.outerHTML.length")
        print(f"  html bytes : {html_len}")
        body_text = driver.execute_script(
            "return (document.body && document.body.innerText || '').trim().slice(0, 600)")
        print(f"  body text  : {body_text!r}")
    except WebDriverException as e:
        print(f"  (page introspection failed: {e})")

    frames = driver.find_elements(By.TAG_NAME, "iframe") + \
             driver.find_elements(By.TAG_NAME, "frame")
    print(f"  frames     : {len(frames)}")
    for i, fr in enumerate(frames):
        print(f"     [{i}] id={fr.get_attribute('id')!r} name={fr.get_attribute('name')!r} "
              f"src={fr.get_attribute('src')!r}")

    _dump_controls_here(driver, "top document")

    for i in range(len(frames)):
        driver.switch_to.default_content()
        current = driver.find_elements(By.TAG_NAME, "iframe") + \
                  driver.find_elements(By.TAG_NAME, "frame")
        if i >= len(current):
            break
        try:
            driver.switch_to.frame(current[i])
        except WebDriverException as e:
            print(f"\n=== frame[{i}] — could not switch in: {e}")
            continue
        _dump_controls_here(driver, f"frame[{i}] ({driver.current_url})")

    driver.switch_to.default_content()
    print("==================================================\n")


# ====================== Results grid parsing ==========================

def _refocus_and_capture(driver, marker_css: str, fallback_html: str,
                         timeout: int = FRAME_TIMEOUT) -> str:
    """After a navigation, focus the document containing `marker_css` and
    return its HTML. Falls back to the HTML captured at click time if the
    marker isn't found anywhere (e.g. an empty or unexpected page), so the
    caller still gets something to parse rather than an exception."""
    if focus_frame_with(driver, marker_css, timeout) is None:
        return fallback_html
    return driver.page_source


def _grid_soup(driver):
    """Return the results grid's BeautifulSoup table, or None if the grid
    isn't on the page (an empty search)."""
    return BeautifulSoup(driver.page_source, "html.parser").find(id=RESULTS_TABLE_ID)


def parse_results_page(driver) -> list[dict]:
    """
    Parse the current page of the grdviewCfrResults grid into a list of
    dicts, using the row_N suffix convention confirmed from the saved
    results-page HTML (e.g. grdviewCfrResults_lblOriginalDate_0,
    _lnkbtnName_0, _lblAddress_0, _lblCity_0, _lblZip_0,
    _labelOfficeSought_0, _lblDistrictNumber_0).
    """
    table = _grid_soup(driver)
    if table is None:
        return []

    def text_by_id_prefix(prefix, index):
        # Each grid cell's id follows "{TABLE_ID}_{prefix}_{row_index}",
        # e.g. grdviewCfrResults_lblOriginalDate_0 for row 0's date cell.
        el = table.find(id=f"{RESULTS_TABLE_ID}_{prefix}_{index}")
        return el.get_text(strip=True) if el else ""

    def name_and_report_flag(index):
        """Candidate name, plus whether this filing has an HTML report.

        Two kinds of row look identical apart from one detail: e-filed reports
        put the candidate's name *inside* the <a>, while filings that exist
        only as a scanned paper PDF leave the anchor empty and render the name
        as plain text beside it:

            <a id="..._lnkbtnName_0">PRINGLE JAN</a>        -> has a report
            <a id="..._lnkbtnName_4"></a>JENKINS ERIC        -> paper only

        The empty anchor has no size, so WebDriver calls it non-interactable,
        and clicking it fires no postback — which is what produced both the
        `ElementNotInteractableException` failures and the blank candidates.
        Reading the name from the surrounding cell keeps it available for the
        manifest key and the skip log.
        """
        el = table.find(id=f"{RESULTS_TABLE_ID}_lnkbtnName_{index}")
        if el is None:
            return "", False
        link_text = el.get_text(strip=True)
        cell = el.find_parent("td")
        name = (cell.get_text(" ", strip=True) if cell else "") or link_text
        return name, bool(link_text)

    rows = []
    index = 0
    # Keep reading row 0, 1, 2, ... until a row's date cell no longer exists
    # — that's how we know we've hit the end of this page's rows.
    while table.find(id=f"{RESULTS_TABLE_ID}_lblOriginalDate_{index}") is not None:
        name, has_report = name_and_report_flag(index)
        rows.append({
            "original_date":   text_by_id_prefix("lblOriginalDate", index),
            "amendment_date":  text_by_id_prefix("lblAmendmentDate", index),
            "name":            name,
            "has_report":      has_report,
            "address":         text_by_id_prefix("lblAddress", index),
            "other":           text_by_id_prefix("lblOther", index),
            "city":            text_by_id_prefix("lblCity", index),
            "zip":             text_by_id_prefix("lblZip", index),
            "office_sought":   text_by_id_prefix("labelOfficeSought", index),
            "district_number": text_by_id_prefix("lblDistrictNumber", index),
            "row_index":       index,
        })
        index += 1
    return rows


def _pager_cells(tr):
    """Digit-only, *id-less* span/anchor children of a row.

    The id-less part is what separates the pager from candidate data. Every
    data cell the grid renders carries a generated id
    ("grdviewCfrResults_lblDistrictNumber_0", "..._lblZip_0", ...), while
    GridView's pager links and its current-page span are plain, unnamed
    elements. Counting digit-only cells alone is not enough: a data row has
    two of them whenever the ZIP is a bare 5-digit code (district + zip), so
    on a 2-page result set a data row would tie with the real pager row and
    could win — silently capping the scrape at page 1.
    """
    return [el for el in tr.select("td > span, td > a")
            if el.get_text(strip=True).isdigit() and not el.get("id")]


def _find_pager_row(table):
    """
    Return the <tr> most likely to be the pager row, or None if the table
    doesn't seem to have one (a single page of results).

    A pager row is identified as the row with the most digit-only, id-less
    span/anchor cells (see _pager_cells) that also offers at least one
    *clickable* page number — a pager always links to some page other than
    the one being viewed, and no candidate-data row ever contains a bare
    numeric link.
    """
    best_row, best_count = None, 0
    for tr in table.find_all("tr"):
        cells = _pager_cells(tr)
        if not any(el.name == "a" for el in cells):
            continue  # nothing clickable — not a pager
        if len(cells) > best_count:
            best_row, best_count = tr, len(cells)
    return best_row


def get_current_page_number(driver) -> int:
    """The pager's current page is a plain <span> (not clickable); other
    pages are <a> links. Returns the int shown as the non-link span,
    searching only within the detected pager row (see _find_pager_row)."""
    table = _grid_soup(driver)
    if table is None:
        return 1
    pager_row = _find_pager_row(table)
    if pager_row is None:
        return 1  # no pager row found — single page of results
    for el in _pager_cells(pager_row):
        if el.name == "span":
            return int(el.get_text(strip=True))  # the non-link page-number span
    return 1


def get_available_pager_pages(driver) -> list[int]:
    """Return the page numbers currently shown in the pager row, including
    the current page. Scoped to the detected pager row only, so data-column
    digits can't pollute the result."""
    table = _grid_soup(driver)
    if table is None:
        return [1]
    pager_row = _find_pager_row(table)
    if pager_row is None:
        return [1]
    nums = {int(el.get_text(strip=True)) for el in _pager_cells(pager_row)}
    return sorted(nums) or [1]


def _pager_links(driver, page_number):
    """Elements for the pager's link to `page_number`.

    Matched as "an anchor inside the results grid whose entire text is that
    number" — the grid's other anchors are candidate-name links, and its
    district numbers are <span>s, not links, so a digit-only anchor is a
    pager link. normalize-space(.) rather than text() also matches a number
    wrapped in a nested element (e.g. <a><span>2</span></a>).
    """
    return driver.find_elements(
        By.XPATH, f"//*[@id='{RESULTS_TABLE_ID}']//a[normalize-space(.)='{page_number}']"
    )


def _settle_grid(driver, timeout: int = WAIT_TIMEOUT) -> None:
    """After a pager postback: re-focus the grid's document and let it render."""
    focus_frame_with(driver, f"#{RESULTS_TABLE_ID}", FRAME_TIMEOUT)
    wait_for(driver, f"#{RESULTS_TABLE_ID}", timeout)
    time.sleep(0.5)


def go_to_results_page(driver, page_number: int, timeout: int = WAIT_TIMEOUT) -> None:
    """Click the pager link for page_number and wait for the postback."""
    links = _pager_links(driver, page_number)
    if not links:
        raise ValueError(f"No pager link found for page {page_number}.")
    _click_and_wait(driver, links[0], timeout)
    _settle_grid(driver, timeout)


_ELLIPSIS_TEXTS = ("...", "…")


def _ellipsis_ranks(driver) -> tuple[int | None, int | None]:
    """(backward_rank, forward_rank) of the "..." links in the pager, 1-based
    among all "..." links in the grid, or None where there isn't one.

    A middle group renders one on each side with identical text; they're told
    apart by their position relative to the current-page span.
    """
    table = _grid_soup(driver)
    if table is None:
        return None, None
    pager_row = _find_pager_row(table)
    if pager_row is None:
        return None, None

    cells = pager_row.select("td > span, td > a")
    current_idx = next((i for i, c in enumerate(cells)
                        if c.name == "span" and c.get_text(strip=True).isdigit()
                        and not c.get("id")), -1)

    rank = 0
    backward = forward = None
    for i, c in enumerate(cells):
        if c.name == "a" and c.get_text(strip=True) in _ELLIPSIS_TEXTS:
            rank += 1
            if i < current_idx and backward is None:
                backward = rank          # first one before the current page
            elif i > current_idx:
                forward = rank           # last one after it
    return backward, forward


def _click_ellipsis(driver, rank: int, timeout: int = WAIT_TIMEOUT) -> bool:
    xpath = (f"(//*[@id='{RESULTS_TABLE_ID}']//a[normalize-space(.)='...' "
             f"or normalize-space(.)='…'])[{rank}]")
    links = driver.find_elements(By.XPATH, xpath)
    if not links:
        return False
    _click_and_wait(driver, links[0], timeout)
    _settle_grid(driver, timeout)
    return True


def _forward_ellipsis_rank(driver) -> int | None:
    """1-based rank (among all "..." links in the grid) of the one that moves
    *forward* a page group, or None if there isn't one.

    A GridView pager in a middle group renders "..." on BOTH sides — a
    previous-group link before the numbers and a next-group link after them.
    They're identical text, so clicking "the first one" walks backwards; this
    is what made a live run bounce from page 20 back to the first group and
    stop early. Forward is the one positioned after the current-page span.
    """
    table = _grid_soup(driver)
    if table is None:
        return None
    pager_row = _find_pager_row(table)
    if pager_row is None:
        return None

    cells = pager_row.select("td > span, td > a")
    current_idx = next((i for i, c in enumerate(cells)
                        if c.name == "span" and c.get_text(strip=True).isdigit()
                        and not c.get("id")), -1)

    rank = 0
    forward_rank = None
    for i, c in enumerate(cells):
        if c.name == "a" and c.get_text(strip=True) in _ELLIPSIS_TEXTS:
            rank += 1
            if i > current_idx:
                forward_rank = rank      # last one after the current page wins
    return forward_rank


def advance_pager_window(driver, timeout: int = WAIT_TIMEOUT) -> bool:
    """Click the forward "..." control, revealing (and usually landing on) the
    next group of page numbers. Returns False when there isn't one — i.e. the
    visible pages really are all there are."""
    rank = _forward_ellipsis_rank(driver)
    if rank is None:
        return False
    xpath = (f"(//*[@id='{RESULTS_TABLE_ID}']//a[normalize-space(.)='...' "
             f"or normalize-space(.)='…'])[{rank}]")
    links = driver.find_elements(By.XPATH, xpath)
    if not links:
        return False
    _click_and_wait(driver, links[0], timeout)
    _settle_grid(driver, timeout)
    return True


def advance_to_next_page(driver, log, timeout: int = WAIT_TIMEOUT) -> bool:
    """Move the results grid forward exactly one page. Returns False when
    there is no next page.

    Clicking the forward "..." does not merely *reveal* the next group of page
    numbers — GridView also navigates to the first page of that group. So
    after clicking it we re-read the pager instead of assuming we still have
    to click a number: a live run raised "No pager link found for page 11"
    because page 11 had become the current page, rendered as a plain <span>
    with no link to click.
    """
    current = get_current_page_number(driver)
    target = current + 1

    if target in get_available_pager_pages(driver):
        go_to_results_page(driver, target, timeout)
        return True

    if not advance_pager_window(driver, timeout):
        log.info(f"  pager: no page {target} and no forward '...' after page "
                 f"{current} — treating it as the last page.")
        return False

    landed = get_current_page_number(driver)
    if landed == target:
        return True                      # the "..." click landed us on it
    if target in get_available_pager_pages(driver):
        go_to_results_page(driver, target, timeout)
        return True
    if landed > current:
        # Landed somewhere forward but not on `target` (an unusual group
        # size). Accept it — the caller's seen-pages guard keeps this from
        # looping, and skipping backwards is what we must avoid.
        log.info(f"  pager: '...' moved from page {current} to {landed} "
                 f"(expected {target}); continuing from there.")
        return True

    log.warning(f"  pager: '...' moved backwards from page {current} to "
                f"{landed} — stopping to avoid a loop.")
    return False


# ---- schedule / report page parsing -------------------------------------
# Built and verified against sample HTML from all four schedule types plus
# the summary "Receipts and Expenditures Report" page. These are pure
# functions over HTML strings — independent of the browser driver.

def cell_lines(td):
    """
    Reconstruct the visually-rendered <br>-separated lines of a table cell,
    regardless of whether each line's text is a bare NavigableString or sits
    inside a nested <span id="...">. Necessary because the Kansas markup
    mixes both (an individual contributor's name is bare text, but their zip
    is `<span id="Repeater2_lblZip_0">`), and plain get_text() would lose the
    line breaks entirely.
    """
    if td is None:
        return []
    lines, current = [], []
    for node in td.contents:
        if getattr(node, "name", None) == "br":
            lines.append(current)
            current = []
        else:
            current.append(node)
    lines.append(current)
    out = []
    for nodes in lines:
        text = "".join(n.get_text() if hasattr(n, "get_text") else str(n) for n in nodes)
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            out.append(text)
    return out


def split_city_state_zip(line):
    """'Olathe KS 66061-3943' -> ('Olathe', 'KS', '66061-3943')."""
    m = re.match(r"^(.*?)\s+([A-Z]{2})\s+([\d-]+)$", line or "")
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    return (line or "").strip(), "", ""


def money_to_float(text):
    """'$1,400.00' -> 1400.0. Returns None if not parseable."""
    if not text:
        return None
    cleaned = text.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _label_text(soup, span_id):
    el = soup.find(id=span_id)
    return el.get_text(strip=True) if el else ""


def parse_summary_report(html, source_url=""):
    """
    Parse the "Campaign Finance Receipts and Expenditures Report" page
    (exp_report_main.aspx) — the page you land on after clicking a
    candidate's name in the results grid. Returns one flat dict with the
    candidate's filer info and the 7 summary lines (cash on hand, total
    contributions/Schedule A, total expenditures/Schedule C, in-kind/
    Schedule B, other transactions/Schedule D, etc).
    """
    soup = BeautifulSoup(html, "html.parser")
    get = lambda sid: _label_text(soup, sid)  # shorthand: one <span id="..."> lookup

    # Every field here is a direct id-lookup on the summary page — no
    # table-row iteration needed, since this page has one filer/one report.
    return {
        "candidate_name": get("lblCandOrgName"),
        "address": get("lblAddress"),
        "address2": get("lblOther"),
        "city": get("lblCity"),
        "zip": get("lblZip"),
        "county": get("lblCountyName"),
        "office_sought": get("lblOfficeSoughtName"),
        "district": get("lblDistrictNo"),
        "period_start": get("lblFileStartDate"),
        "period_end": get("lblFileEndDate"),
        "cash_on_hand_beginning": money_to_float(get("lblCashBeginning")),
        "total_contributions_schedule_a": money_to_float(get("lblTotalContributions")),
        "cash_available_this_period": money_to_float(get("lblCashThisPeriod")),
        "total_expenditures_schedule_c": money_to_float(get("lblTotalExpenditures")),
        "cash_on_hand_close": money_to_float(get("lblCashOnHandClose")),
        "in_kind_contributions_schedule_b": money_to_float(get("lblInKindContributions")),
        "other_transactions_schedule_d": money_to_float(get("lblOtherTransactions")),
        "filed_date": get("lblDate"),
        "signature_name": get("lblElectronicSignatureName"),
        "source_url": source_url,
    }


def _find_schedule_tables(soup):
    """
    Every schedule page has (in this order among width=98% border=1 tables):
    the itemized-transaction data table (has <th> headers), then one or more
    small totals tables (each row's first <td> is a bold9 label like
    'TOTAL EXPENDITURES...'). Returns (data_table, totals_table).
    """
    candidates = soup.find_all("table", attrs={"width": "98%", "border": "1"})
    data_table, totals_table = None, None
    for t in candidates:
        if t.find("th") is not None:
            data_table = t  # the itemized rows have column headers (<th>)
        elif t.find("td", class_="bold9") is not None:
            totals_table = t  # last one wins if there happen to be several
    return data_table, totals_table


def _totals_dict(totals_table):
    """Turn a schedule's small totals table into {label_text: amount}."""
    totals = {}
    if totals_table is None:
        return totals
    for row in totals_table.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 2:
            continue
        label = tds[0].get_text(strip=True)   # e.g. "TOTAL CONTRIBUTIONS THIS PERIOD"
        value = money_to_float(tds[-1].get_text(strip=True))
        totals[label] = value
    return totals


def parse_schedule_a(html, candidate_key, source_url=""):
    """Schedule A — Contributions and Other Receipts. Returns (rows, totals)
    where rows is a list of dicts, one per itemized entry.

    Shared pattern used by all four parse_schedule_* functions below:
      1. find the data table + totals table via _find_schedule_tables()
      2. for each <tr>, skip rows without enough <td>s (headers/spacers)
      3. column 0 is always the date; column 1 is always a multi-line
         name+address cell, decoded with cell_lines() and split_city_state_zip()
      4. remaining columns are schedule-specific (payment type, amounts, etc)
    """
    soup = BeautifulSoup(html, "html.parser")
    data_table, totals_table = _find_schedule_tables(soup)
    rows = []
    if data_table is not None:
        for tr in data_table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 7:
                continue  # header/spacer rows
            date = tds[0].get_text(strip=True)
            lines = cell_lines(tds[1])
            name = lines[0] if lines else ""
            address = lines[1] if len(lines) > 2 else ""
            city, state, zip_ = split_city_state_zip(lines[-1] if lines else "")
            rows.append({
                "candidate": candidate_key,
                "date": date,
                "contributor_name": name,
                "contributor_address": address,
                "contributor_city": city,
                "contributor_state": state,
                "contributor_zip": zip_,
                "type_of_payment": tds[2].get_text(strip=True),
                "occupation": tds[3].get_text(strip=True),
                "primary_total": money_to_float(tds[4].get_text(strip=True)),
                "general_total": money_to_float(tds[5].get_text(strip=True)),
                "amount": money_to_float(tds[6].get_text(strip=True)),
                "source_url": source_url,
            })
    totals = _totals_dict(totals_table)
    return rows, totals


def parse_schedule_b(html, candidate_key, source_url=""):
    """Schedule B — In-Kind (Non-Monetary) Contributions."""
    soup = BeautifulSoup(html, "html.parser")
    data_table, totals_table = _find_schedule_tables(soup)
    rows = []
    if data_table is not None:
        for tr in data_table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 5:
                continue
            date = tds[0].get_text(strip=True)
            lines = cell_lines(tds[1])
            name = lines[0] if lines else ""
            address = lines[1] if len(lines) > 2 else ""
            city, state, zip_ = split_city_state_zip(lines[-1] if lines else "")
            rows.append({
                "candidate": candidate_key,
                "date": date,
                "contributor_name": name,
                "contributor_address": address,
                "contributor_city": city,
                "contributor_state": state,
                "contributor_zip": zip_,
                "occupation": tds[2].get_text(strip=True),
                "description": tds[3].get_text(strip=True),
                "value": money_to_float(tds[4].get_text(strip=True)),
                "source_url": source_url,
            })
    totals = _totals_dict(totals_table)
    return rows, totals


def parse_schedule_c(html, candidate_key, source_url=""):
    """
    Schedule C — Expenditures and Other Disbursements.
    NOTE: the "Purpose" column mixes a category label (e.g. "Reimbursement",
    "Printing") and a free-text description with only a space between them
    and no reliable markup boundary (e.g. "Printing printing/mailing").
    Rather than guess wrong, it's kept as a single `purpose_raw` field.
    """
    soup = BeautifulSoup(html, "html.parser")
    data_table, totals_table = _find_schedule_tables(soup)
    rows = []
    if data_table is not None:
        for tr in data_table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 6:
                continue
            date = tds[0].get_text(strip=True)
            lines = cell_lines(tds[1])
            name = lines[0] if lines else ""
            address = lines[1] if len(lines) > 2 else ""
            city, state, zip_ = split_city_state_zip(lines[-1] if lines else "")
            purpose_lines = cell_lines(tds[2])
            rows.append({
                "candidate": candidate_key,
                "date": date,
                "payee_name": name,
                "payee_address": address,
                "payee_city": city,
                "payee_state": state,
                "payee_zip": zip_,
                "purpose_raw": purpose_lines[0] if purpose_lines else "",
                "primary_total": money_to_float(tds[3].get_text(strip=True)),
                "general_total": money_to_float(tds[4].get_text(strip=True)),
                "amount": money_to_float(tds[5].get_text(strip=True)),
                "source_url": source_url,
            })
    totals = _totals_dict(totals_table)
    return rows, totals


def parse_schedule_d(html, candidate_key, source_url=""):
    """Schedule D — Other Transactions (loans, start-up costs, etc)."""
    soup = BeautifulSoup(html, "html.parser")
    data_table, totals_table = _find_schedule_tables(soup)
    rows = []
    if data_table is not None:
        for tr in data_table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 4:
                continue
            date = tds[0].get_text(strip=True)
            lines = cell_lines(tds[1])
            name = lines[0] if lines else ""
            address = lines[1] if len(lines) > 2 else ""
            city, state, zip_ = split_city_state_zip(lines[-1] if lines else "")
            rows.append({
                "candidate": candidate_key,
                "date": date,
                "name": name,
                "address": address,
                "city": city,
                "state": state,
                "zip": zip_,
                "nature_of_account": tds[2].get_text(strip=True),
                "balance_at_close": money_to_float(tds[3].get_text(strip=True)),
                "source_url": source_url,
            })
    # Schedule D's grand-total cell has no id/label markup to key off of, so
    # fall back to summing the itemized rows.
    total = sum(r["balance_at_close"] for r in rows if r["balance_at_close"] is not None)
    totals = {"TOTAL OTHER TRANSACTIONS": total} if rows else {}
    return rows, totals


# ---- incremental CSV writing ---------------------------------------------
# Each writer appends rows and writes the header only once, so a long-running
# scrape can be interrupted/resumed without losing already-collected data.

class IncrementalCsvWriter:
    def __init__(self, path, fieldnames):
        self.path = path
        self.fieldnames = fieldnames
        self._wrote_header = False
        try:
            # If the file already exists and has at least one line, assume
            # that line is the header — so future writes append, not overwrite.
            with open(path, "r", encoding="utf-8") as f:
                self._wrote_header = bool(f.readline())
        except FileNotFoundError:
            pass  # first write() will create it with a header

    def write_rows(self, rows):
        if not rows:
            return
        mode = "a" if self._wrote_header else "w"
        with open(self.path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames, extrasaction="ignore")
            if not self._wrote_header:
                writer.writeheader()
                self._wrote_header = True
            writer.writerows(rows)


SUMMARY_FIELDS = [
    "candidate_uid", "candidate_name", "address", "address2", "city", "zip", "county",
    "office_sought", "district", "period_start", "period_end",
    "cash_on_hand_beginning", "total_contributions_schedule_a",
    "cash_available_this_period", "total_expenditures_schedule_c",
    "cash_on_hand_close", "in_kind_contributions_schedule_b",
    "other_transactions_schedule_d", "filed_date", "signature_name",
    "source_url",
]
SCHEDULE_A_FIELDS = [
    "candidate_uid", "candidate", "date", "contributor_name", "contributor_address",
    "contributor_city", "contributor_state", "contributor_zip",
    "type_of_payment", "occupation", "primary_total", "general_total",
    "amount", "source_url",
]
SCHEDULE_B_FIELDS = [
    "candidate_uid", "candidate", "date", "contributor_name", "contributor_address",
    "contributor_city", "contributor_state", "contributor_zip",
    "occupation", "description", "value", "source_url",
]
SCHEDULE_C_FIELDS = [
    "candidate_uid", "candidate", "date", "payee_name", "payee_address", "payee_city",
    "payee_state", "payee_zip", "purpose_raw", "primary_total",
    "general_total", "amount", "source_url",
]
SCHEDULE_D_FIELDS = [
    "candidate_uid", "candidate", "date", "name", "address", "city", "state", "zip",
    "nature_of_account", "balance_at_close", "source_url",
]

CSV_FIELDS = {
    "summary":    SUMMARY_FIELDS,
    "schedule_a": SCHEDULE_A_FIELDS,
    "schedule_b": SCHEDULE_B_FIELDS,
    "schedule_c": SCHEDULE_C_FIELDS,
    "schedule_d": SCHEDULE_D_FIELDS,
}


# ---- per-candidate orchestration -----------------------------------------

# Schedule view/print link ids on the summary report page, and which
# summary-page dict key tells us whether that schedule is worth opening
# (skip it if the total is $0 — nothing to itemize).
SCHEDULE_LINKS = [
    ("lnkbtnScheduleAView", "total_contributions_schedule_a", parse_schedule_a, "schedule_a"),
    ("lnkbtnScheduleBView", "in_kind_contributions_schedule_b", parse_schedule_b, "schedule_b"),
    ("lnkbtnScheduleCView", "total_expenditures_schedule_c", parse_schedule_c, "schedule_c"),
    ("lnkbtnScheduleDView", "other_transactions_schedule_d", parse_schedule_d, "schedule_d"),
]


class PaperFilingSkipped(Exception):
    """A candidate link led to a scanned PDF rather than the HTML report.

    Not a failure: these filings were submitted on paper and have no
    machine-readable report anywhere in the CFR Examiner. Most are caught from
    the grid (see parse_results_page); this covers any that slip through.
    """


def scrape_candidate_report(driver, row_index, writers, candidate_uid,
                            skip_empty_schedules=True):
    """
    From the results grid, click the row_index'th candidate's name link,
    scrape the summary report, then open+scrape each non-empty schedule
    (A/B/C/D), writing everything to the shared `writers` dict of
    IncrementalCsvWriter (keys: 'summary', 'schedule_a'..'schedule_d').
    Leaves the driver back on the results grid when done.

    `candidate_uid` is the manifest key computed by the caller (see
    make_candidate_key) — it's stamped onto every row this candidate produces
    (summary AND each schedule row) so the parser can join schedule rows back
    to the right summary row without relying on candidate name text alone,
    which collides across cycles/offices for people with the same name.
    """
    # Rows are buffered and written only once the whole candidate has been
    # scraped. Writing as we went would leave a half-written candidate on disk
    # whenever a schedule click failed — and since the manifest only records
    # successes, the next incremental run would re-scrape that candidate and
    # append their rows a second time, double-counting the contributions that
    # did make it. Buffering makes each candidate all-or-nothing.
    pending: dict[str, list[dict]] = {key: [] for key in writers}

    grid_handle = driver.current_window_handle
    name_link = driver.find_element(By.ID, f"{RESULTS_TABLE_ID}_lnkbtnName_{row_index}")

    # Step 1: open the candidate's summary report and write it to the
    # "summary" CSV. If the site opened it in a new window, that's where the
    # driver now is — every subsequent lookup for this candidate happens
    # there, and summary_window is what schedule pages return to.
    summary_html, summary_handle = click_and_capture_page(driver, name_link)
    summary_window = summary_handle or grid_handle
    # Navigation resets frame focus, and the report may render inside a frame
    # — re-focus on the document that actually holds it and re-read from there.
    summary_html = _refocus_and_capture(driver, SUMMARY_MARKER_CSS, summary_html)

    # Confirm we actually landed on a report before parsing anything. Some grid
    # rows carry a name link that opens nothing, and the click then leaves us
    # on the results page — parse_summary_report() finds none of its labels and
    # happily returns a dict of empty strings. A live run wrote 495 such blank
    # candidates (a quarter of the file), each recorded in the manifest as done,
    # so they would have been skipped forever on later runs. Raising here makes
    # it a counted failure that nothing is written for and a re-run retries.
    if SUMMARY_MARKER_CSS.lstrip("#") not in summary_html:
        landed = driver.current_url
        if summary_handle is not None:
            close_and_return(driver, summary_handle, grid_handle)
        # A block page is served *at the report's own URL*, so this looked
        # like "the link opened nothing" in a live run when the site had
        # actually started rate-limiting us.
        if is_blocked(driver):
            raise Blocked(landed)
        # Backstop for a paper filing the grid-level check missed: some of
        # these pop the scanned PDF into a new tab instead of doing nothing.
        # Not an error — there is no HTML report to scrape, ever.
        if ".pdf" in landed.lower():
            raise PaperFilingSkipped(landed)
        raise RuntimeError(
            f"the candidate link did not open a report — still on "
            f"{landed.rsplit('/', 1)[-1]}")

    summary = parse_summary_report(summary_html, source_url=driver.current_url)
    summary["candidate_uid"] = candidate_uid
    pending["summary"].append(summary)
    candidate_name_text = summary["candidate_name"] or f"row_{row_index}"

    # Step 2: for each schedule (A/B/C/D), skip it if its summary total is $0
    # (nothing to itemize), otherwise open it, parse its rows, write them to
    # that schedule's CSV, then navigate back to the summary page.
    for link_id, total_key, parse_fn, csv_key in SCHEDULE_LINKS:
        if skip_empty_schedules and not summary.get(total_key):
            continue
        # Returning from the previous schedule reset frame focus.
        focus_frame_with(driver, f"#{link_id}", FRAME_TIMEOUT)
        link_els = driver.find_elements(By.ID, link_id)
        if not link_els:
            continue  # this schedule's link wasn't on the page
        sched_html, sched_handle = click_and_capture_page(driver, link_els[0])
        sched_html = _refocus_and_capture(driver, SCHEDULE_TABLE_CSS, sched_html)
        rows, _totals = parse_fn(sched_html, candidate_name_text,
                                 source_url=driver.current_url)
        for r in rows:
            r["candidate_uid"] = candidate_uid
        pending[csv_key].extend(rows)
        close_and_return(driver, sched_handle, summary_window)  # back to the summary

    # Step 3: everything for this candidate parsed — commit it, then head back
    # to the results grid ready for the next row.
    for key, rows in pending.items():
        writers[key].write_rows(rows)

    close_and_return(driver, summary_handle, grid_handle)
    focus_frame_with(driver, f"#{RESULTS_TABLE_ID}", FRAME_TIMEOUT)


def _resume_at_page(driver, log, page_number: int, entry) -> bool:
    """After a block, re-run this cycle's search and page back to where we were.

    Waiting out a block means navigating away, which loses the grid's
    server-side state, so the search has to be re-submitted. Paging forward
    costs one request per page but no candidate loads, and the manifest means
    nothing already scraped is fetched twice.
    """
    if entry is None:
        return False
    try:
        if not submit_search(driver, log, entry.option_text, entry.date_start,
                             entry.date_end, category=entry.category):
            log.warning("  re-submitted search returned no results grid")
            return False
    except Exception as e:
        log.warning(f"  could not re-submit the search after a block: {e}")
        return False

    for _ in range(max(0, page_number - 1)):
        if not advance_to_next_page(driver, log):
            break
    landed = get_current_page_number(driver)
    if landed != page_number:
        log.warning(f"  resumed at page {landed}, wanted {page_number} — "
                    f"ending this cycle; a re-run picks up the rest.")
        return False
    log.info(f"  resumed at page {page_number}")
    return True


def scrape_all_candidates_with_schedules(driver, writers, office_group, cycle_label,
                                         done, force, log, max_pages=None,
                                         skip_empty_schedules=True, entry=None):
    """
    Full pipeline for one (office_group, cycle_label) search already submitted
    on the results grid (see run()): walks every page, and for every row,
    first builds a manifest key from the fields already on the grid
    (name/office/district/dates) to decide whether it's already been scraped
    in a previous run. If not skipped, opens the candidate's summary report
    plus each non-empty Schedule A/B/C/D breakdown and appends rows to the
    shared `writers` (created once per run() call so CSVs accumulate across
    cycles).

    `done` is the manifest dict (candidate_key -> row) loaded at the start of
    run(); newly-scraped candidates are added to it and appended to MANIFEST
    on disk as they're processed, so an interrupted run keeps everything
    scraped up to that point and a later run can resume.
    """
    is_current = is_current_cycle(cycle_label, datetime.today().year)
    today = datetime.today().strftime("%Y-%m-%d")

    seen_pages = set()
    page_count = 0
    scraped = skipped = failed = paper = 0
    while True:
        # Stop once we loop back to a page number we've already scraped
        # (guards against a pager click that doesn't actually advance).
        current = get_current_page_number(driver)
        if current in seen_pages:
            break
        seen_pages.add(current)
        page_count += 1

        # Parse this page's grid rows up front (name/office/district/dates)
        # so we can build each row's manifest key BEFORE clicking into it —
        # that lets us skip already-scraped candidates without ever opening
        # their report.
        page_rows = parse_results_page(driver)

        for row_index, row in enumerate(page_rows):
            # Paper-only filings: the name link is an empty anchor that opens
            # a scanned PDF (or nothing at all), never the HTML report this
            # scraper reads. Recognised from the grid, so they cost nothing —
            # previously each one was clicked, waited on, and written out as a
            # blank candidate.
            if not row.get("has_report", True):
                paper += 1
                continue

            candidate_key = make_candidate_key(office_group, cycle_label, row)

            # Skip logic: skip if already in the manifest, UNLESS --force was
            # passed or this cycle is still "current" (year >= this year) and
            # so could have new/updated filings worth re-checking.
            already_done = candidate_key in done
            if already_done and not force and not is_current:
                skipped += 1
                continue

            try:
                scrape_candidate_report(driver, row_index, writers, candidate_key,
                                        skip_empty_schedules=skip_empty_schedules)
                scraped += 1
            except Blocked:
                # Not this filing's fault — wait for the block to lift, then
                # have another go at the same row.
                if not wait_out_block(driver, log):
                    log.warning(f"  {office_group} {cycle_label}: still blocked after "
                                f"backing off — ending this cycle; re-run to resume.")
                    return scraped, skipped, failed, paper
                if not _resume_at_page(driver, log, current, entry):
                    return scraped, skipped, failed, paper
                # This row stays unscraped and unmanifested, so a later run
                # retries it; carry on with the rest of the page.
                continue
            except PaperFilingSkipped:
                paper += 1
                _recover_to_grid(driver, log)
                continue
            except Exception as e:
                # Selenium exceptions carry a multi-line stack trace that
                # buries the actual message; keep the first line only.
                reason = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
                log.warning(f"  [{office_group} {cycle_label}, page {current}, "
                            f"row {row_index}] failed: {reason}")
                failed += 1
                # Nothing was written for this candidate (see the buffering in
                # scrape_candidate_report), so they'll simply be retried on the
                # next run — they never entered the manifest.
                if not _recover_to_grid(driver, log):
                    log.warning(f"  {office_group} {cycle_label}: lost the results "
                                f"grid on page {current} — ending this cycle early.")
                    return scraped, skipped, failed, paper
                continue

            # Record this candidate as done — both to the in-memory `done`
            # dict (so later rows/cycles in this same run see it too) and to
            # disk immediately (so an interrupted run doesn't lose it).
            record = {
                "candidate_key":   candidate_key,
                "office_group":    office_group,
                "cycle_label":     cycle_label,
                "office_text":     row.get("office_sought", ""),
                "candidate_name":  row.get("name", ""),
                "office_sought":   row.get("office_sought", ""),
                "district_number": row.get("district_number", ""),
                "original_date":   row.get("original_date", ""),
                "amendment_date":  row.get("amendment_date", ""),
                "scraped_at":      today,
            }
            append_manifest(record)
            done[candidate_key] = record

            # Be a good citizen and stay under the rate limit.
            if REQUEST_DELAY:
                time.sleep(REQUEST_DELAY)

        if max_pages is not None and page_count >= max_pages:
            break

        # Advance one page (handling grouped "1 2 3 ... " pagers). A failure
        # here ends this cycle's walk rather than raising: the candidates
        # already scraped are on disk and in the manifest, and the summary
        # line below still runs, so a pager problem costs the remaining pages
        # of one cycle instead of the whole cycle.
        try:
            if not advance_to_next_page(driver, log):
                break
        except Exception as e:
            if is_blocked(driver):
                if not wait_out_block(driver, log):
                    log.warning(f"  {office_group} {cycle_label}: blocked while paging "
                                f"after page {current} — ending this cycle; the "
                                f"remaining pages are picked up on the next run.")
                    break
                if not _resume_at_page(driver, log, current + 1, entry):
                    break
                continue
            log.warning(f"  {office_group} {cycle_label}: pagination stopped after "
                        f"page {current}: {e}")
            break

    log.info(f"  {office_group} {cycle_label}: {scraped} scraped, "
             f"{skipped} skipped, {failed} failed"
             + (f", {paper} paper-only (no HTML report)" if paper else ""))
    return scraped, skipped, failed, paper


def _recover_to_grid(driver, log, max_back: int = 4) -> bool:
    """Get back to the results grid after a row failed part-way through.

    This is what turns one bad row into one bad row instead of a lost cycle.
    A failure inside scrape_candidate_report leaves the driver wherever it
    died — typically on the candidate's summary or a schedule page — and the
    old handler only waited for the grid to appear, which it never would. Every
    later row on that page then failed too (its name link isn't on a summary
    page), and pagination stopped because the pager wasn't there either: in a
    live run this produced long contiguous blocks of failures that ended the
    cycle early.

    Closes any stray windows, then steps back through history until the grid
    reappears. Returns False if it can't be recovered, which tells the caller
    to stop this cycle rather than log a failure per remaining row.
    """
    _close_stray_windows(driver)

    for attempt in range(max_back + 1):
        if focus_frame_with(driver, f"#{RESULTS_TABLE_ID}", FRAME_TIMEOUT) is not None:
            return True
        if attempt == max_back:
            break
        try:
            driver.back()
            _wait_ready(driver, WAIT_TIMEOUT)
        except (TimeoutException, WebDriverException):
            break

    log.warning("  could not get back to the results grid after a failed row")
    return False


def _close_stray_windows(driver) -> None:
    """Close any windows opened by a candidate/schedule click that failed
    part-way through, so the next row starts from a clean single-window
    state focused on the results grid."""
    handles = driver.window_handles
    if len(handles) <= 1:
        return
    keep = handles[0]
    for h in handles[1:]:
        try:
            driver.switch_to.window(h)
            driver.close()
        except Exception:
            pass
    try:
        driver.switch_to.window(keep)
    except Exception:
        pass


# ---- candidate roster (party) ---------------------------------------------

def _roster_election_options(driver) -> list[tuple[str, str]]:
    """[(label, value)] for every election in the roster page's dropdown,
    e.g. ("2024 General", "34")."""
    el = wait_for(driver, ROSTER_SELECT_CSS)
    out = []
    for opt in el.find_elements(By.TAG_NAME, "option"):
        label = (opt.text or "").strip()
        if label:
            out.append((label, opt.get_attribute("value")))
    return out


def _parse_roster_election_label(label: str) -> tuple[str, str]:
    """'2024 Presidential Preference Primary' -> ('2024', 'Presidential
    Preference Primary'). Returns ('', label) if it doesn't start with a year."""
    m = re.match(r"\s*(\d{4})\s*(.*)$", label or "")
    return (m.group(1), m.group(2).strip()) if m else ("", (label or "").strip())


def parse_roster_table(html: str, label: str) -> list[dict]:
    """Parse the gvCandidateList table into roster rows.

    Columns are located by *header text*, not position, so the page adding or
    reordering columns (it carries 25 of them, most of which we ignore)
    doesn't silently shift party into the wrong field.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find(id=ROSTER_TABLE_ID)
    if table is None:
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Header row: the site renders it as <th>, but fall back to the first row's
    # <td>s in case that changes.
    header_cells = rows[0].find_all("th") or rows[0].find_all("td")
    index_of = {}
    for i, cell in enumerate(header_cells):
        key = ROSTER_HEADER_MAP.get(cell.get_text(" ", strip=True).lower())
        if key:
            index_of[key] = i
    if "party" not in index_of or "candidate" not in index_of:
        return []   # not the table we think it is

    year, etype = _parse_roster_election_label(label)
    out = []
    for tr in rows[1:]:
        cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
        if not cells:
            continue
        rec = {"election_label": label, "election_year": year, "election_type": etype}
        for key, idx in index_of.items():
            rec[key] = cells[idx] if idx < len(cells) else ""
        if not (rec.get("candidate") or rec.get("last_name")):
            continue
        out.append(rec)
    return out


def scrape_candidate_roster(driver, log, out_path: Path,
                            start_year: int | None = None,
                            end_year: int | None = None) -> int:
    """Walk the Candidate List page's election dropdown and write one CSV of
    every candidate with their party.

    Rewritten wholesale on each run rather than tracked in the manifest: it's
    one page load per election (~28 total for all of 2002-2026), the rows are
    small, and a filed candidate's party can be corrected/withdrawn later, so
    a fresh copy is both cheap and more correct than an incremental merge.
    """
    driver.get(ROSTER_URL)
    _raise_if_blocked(driver)

    try:
        options = _roster_election_options(driver)
    except Exception as e:
        log.warning(f"  Candidate roster: election dropdown "
                    f"{ROSTER_SELECT_CSS} not found ({e}) — skipping party roster. "
                    f"Candidates will parse with an empty party.")
        return 0

    wanted = []
    for label, _value in options:
        yr, _etype = _parse_roster_election_label(label)
        yr_int = int(yr) if yr.isdigit() else 0
        if start_year is not None and yr_int < start_year:
            continue
        if end_year is not None and yr_int > end_year:
            continue
        wanted.append(label)

    log.info(f"Candidate roster: {len(wanted)} of {len(options)} elections in scope")

    all_rows: list[dict] = []
    for label in wanted:
        try:
            select_by_visible_text_containing(driver, ROSTER_SELECT_CSS, label)
            _click_and_wait(driver, driver.find_element(By.CSS_SELECTOR, ROSTER_SUBMIT_CSS))
            rows = parse_roster_table(driver.page_source, label)
            all_rows.extend(rows)
            log.info(f"  roster {label}: {len(rows)} candidates")
        except Exception as e:
            log.warning(f"  roster {label}: failed ({e})")
        # Each submit re-renders the same page; go back to a clean copy so the
        # dropdown selection for the next election starts from a known state.
        driver.get(ROSTER_URL)

    if not all_rows:
        log.warning("  Candidate roster: no rows scraped — party will be empty.")
        return 0

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ROSTER_FIELDS, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(all_rows)
    log.info(f"  Candidate roster: {len(all_rows):,} rows -> {out_path}")
    return len(all_rows)


# ---- search submission ----------------------------------------------------

def submit_search(driver, log, option_text, date_start, date_end,
                  category: str = CATEGORY_CANDIDATE, debug: bool = False) -> bool:
    """
    From a fresh load of ENTRY_URL: select category dropdown + Submit, then
    fill Date Range Filed / Filing Type / Office and Submit Search.

    Returns True if a results grid rendered, False if the search ran but
    matched nothing (a legitimately empty cycle — e.g. a future election —
    which should not be treated as a failure). Raises on selector failures,
    after dumping the page's controls for debugging.

    With debug=True (--debug-controls), the full control dump is printed at
    each of the three stages — entry page, search form, results page — so one
    run reveals every id/option instead of only the page that happened to fail.
    """
    driver.get(ENTRY_URL)
    _raise_if_blocked(driver)   # CloudFront 403s headless Chrome
    if debug:
        print("\n########## STAGE 1: entry page ##########")
        debug_dump_controls(driver)

    # ---- Step 1: category dropdown ("Candidate Campaign Filings") + Submit
    # The form lives in a frame, and every postback/navigation resets frame
    # focus, so each step re-focuses whichever document holds its control.
    where = focus_frame_with(driver, CATEGORY_SELECT_CSS)
    if where is None:
        log.warning(f"Category dropdown {CATEGORY_SELECT_CSS} not found in the top "
                    f"document or any frame — dumping page diagnostics.")
        debug_dump_controls(driver)
        raise TimeoutException(
            f"{CATEGORY_SELECT_CSS} not present anywhere on {driver.current_url}")
    log.debug(f"  category dropdown found in {where}")

    try:
        select_by_visible_text_containing(driver, CATEGORY_SELECT_CSS, category)
    except Exception as e:
        log.warning(f"Category dropdown selector failed: {e}")
        debug_dump_controls(driver)
        raise

    try:
        _click_and_wait(driver, driver.find_element(By.CSS_SELECTOR, CATEGORY_SUBMIT_CSS))
    except Exception as e:
        log.warning(f"Category Submit button selector failed: {e}")
        debug_dump_controls(driver)
        raise

    # ---- Step 2: wait for the search form to load
    _raise_if_blocked(driver)
    if debug:
        print("\n########## STAGE 2: search form (after category Submit) ##########")
        debug_dump_controls(driver)
    focus_frame_with(driver, START_DATE_CSS)
    try:
        start_date = wait_for(driver, START_DATE_CSS)
    except Exception as e:
        log.warning(f"Start-date field never appeared after Submit: {e}")
        debug_dump_controls(driver)
        raise

    # Fill in this catalog entry's date range, clamping a future end date the
    # form would reject (see _clamp_future_date).
    effective_end = _clamp_future_date(date_end)
    if effective_end != date_end:
        log.info(f"  end date {date_end} is in the future — searching through "
                 f"{effective_end} instead (the form rejects future dates)")

    start_date.clear()
    start_date.send_keys(date_start)
    end_date = driver.find_element(By.CSS_SELECTOR, END_DATE_CSS)
    end_date.clear()
    end_date.send_keys(effective_end)
    dismiss_datepicker(driver)  # close the JS calendar before touching other fields

    # Filing type is the same for every catalog entry.
    try:
        select_by_visible_text_containing(driver, FILING_TYPE_SELECT_CSS,
                                          "Receipts and Expenditures Report")
    except Exception as e:
        log.warning(f"Filing Type dropdown selector failed: {e}")
        debug_dump_controls(driver)
        raise

    # drpdownOffice is the Office dropdown on the candidate form and the Type
    # of Committee dropdown on the PAC form — same id, same handling.
    try:
        select_office_by_text(driver, OFFICE_CONTAINER_ID, option_text)
    except Exception as e:
        log.warning(f"Office/committee-type dropdown failed for {option_text!r}: {e}")
        debug_dump_controls(driver)
        raise

    # ---- Step 3: submit the search and wait for the results grid
    try:
        dismiss_datepicker(driver)
        _click_and_wait(driver, driver.find_element(By.CSS_SELECTOR, SEARCH_SUBMIT_CSS))
    except Exception as e:
        log.warning(f"Submit Search button selector failed: {e}")
        debug_dump_controls(driver)
        raise

    if debug:
        print("\n########## STAGE 3: results page (after Submit Search) ##########")
        debug_dump_controls(driver)

    # The results grid may render in a different frame than the search form.
    grid_found = focus_frame_with(driver, f"#{RESULTS_TABLE_ID}", RESULTS_TIMEOUT)
    if grid_found is None:
        # No grid. Distinguish "the search ran and matched nothing" from "the
        # form refused the input" — the two look identical from here, and
        # conflating them is what hid the future-date rejection behind a
        # cheerful "empty cycle" for every 2026/2028 search.
        problem = _page_validation_error(driver)
        if problem:
            log.warning(f"  the form rejected this search: {problem!r}")
        return False
    try:
        wait_for(driver, f"#{RESULTS_TABLE_ID}", RESULTS_TIMEOUT)
    except TimeoutException:
        return False

    # The grid can come back paged where the previous search left it.
    reset_pager_to_first_page(driver, log)
    return True


# ================================ run =================================

def _resolve_scope(transactions=False, contributions=False, expenditures=False,
                   entities=False, candidates=False, committees=False,
                   **_unused) -> tuple[bool, bool]:
    """Turn the horizontal scope flags into (do_filings, do_roster).

    A KS filing carries contributions, expenditures and filer info together, so
    the transaction-vs-entity flags can't split the *filings* scrape — but they
    do cleanly separate the two things this scraper fetches: the R&E filings
    (transactions) and the SOS candidate roster (entity/party data).
        --transactions / --contributions / --expenditures  -> filings only
        --entities / --candidates / --committees           -> roster only
        (neither, or both)                                 -> both
    """
    txn = any((transactions, contributions, expenditures))
    ent = any((entities, candidates, committees))
    if txn and not ent:
        return True, False
    if ent and not txn:
        return False, True
    return True, True


def run(headless: bool = False, out_dir: str | None = None,
        force: bool = False, start_year: int | None = None,
        end_year: int | None = None, skip_empty_schedules: bool = True,
        debug_controls: bool = False, roster: bool = True,
        delay: float | None = None, **scope_flags):
    """
    Scrape Kansas candidate R&E filings for every (office, cycle) in
    RUN_CATALOG, writing five CSVs into out_dir (default: data/Kansas/raw/)
    and tracking progress in data/Kansas/manifest.csv so re-runs skip
    already-scraped candidates.

    Also scrapes the SOS Candidate List roster into candidate_roster.csv —
    the only published source of candidate *party* for Kansas, joined on in
    parsers/kansas.py (see scrape_candidate_roster). Skip it with roster=False
    / --no-roster.

    Horizontal scope flags select between the two: --transactions (and
    friends) = filings only, --entities/--candidates/--committees = roster
    only, neither/both = both. See _resolve_scope.

    Vertical scope:
        (no flag)        incremental — skip candidates already in the
                         manifest; always re-check cycles whose year is
                         >= the current year.
        --start-year     only scrape cycles with cycle year >= YYYY
        --end-year       only scrape cycles with cycle year <= YYYY
        --force          wipe manifest and CSVs, re-scrape everything
    """
    global REQUEST_DELAY
    if delay is not None:
        REQUEST_DELAY = max(0.0, delay)

    log = get_logger("kansas", "scrape")
    t0 = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year,
              request_delay=REQUEST_DELAY)

    base_dir = Path(out_dir) if out_dir else RAW_DIR
    base_dir.mkdir(parents=True, exist_ok=True)

    # The five CSV outputs this run writes into (shared across every catalog
    # entry so candidates from different offices/cycles land in the same files).
    csv_paths = {key: base_dir / name for key, name in CSV_NAMES.items()}

    year_range_active = start_year is not None or end_year is not None

    # ── Manifest / CSV prep ────────────────────────────────────────────
    if force:
        # --force: start completely clean — wipe the manifest and delete any
        # existing CSVs so every candidate gets re-scraped from scratch.
        strip_manifest(lambda _: False)
        for p in csv_paths.values():
            p.unlink(missing_ok=True)
        done = {}
    elif year_range_active:
        # --start-year/--end-year: drop manifest rows for cycles inside the
        # requested window (so they'll be re-scraped) but keep rows for cycles
        # outside it (so an unrelated earlier scrape isn't touched).
        def _outside_range(r: dict) -> bool:
            """True for manifest rows to KEEP — the ones outside the window.

            Both bounds have to fail together for a row to be inside the
            window. Testing them independently (drop if yr >= start, drop if
            yr <= end) means a row only has to satisfy *one* of them, so
            `--start-year 2025 --end-year 2025` matched every cycle and wiped
            the whole manifest. It did exactly that once, and the next run then
            treated 8,086 already-scraped filings as unscraped.
            """
            yr = _cycle_year(r.get("cycle_label", ""))
            if start_year is not None and yr < start_year:
                return True
            if end_year is not None and yr > end_year:
                return True
            return False
        strip_manifest(_outside_range)
        done = load_manifest()
    else:
        # Default/incremental: keep the whole manifest; per-candidate skip
        # logic happens in scrape_all_candidates_with_schedules.
        done = load_manifest()

    # One IncrementalCsvWriter per output file, reused across every catalog
    # entry in this run so rows accumulate rather than overwrite.
    writers = {key: IncrementalCsvWriter(csv_paths[key], CSV_FIELDS[key])
               for key in CSV_NAMES}

    # Narrow RUN_CATALOG to the cycles requested via --start-year/--end-year.
    catalog = [
        entry for entry in RUN_CATALOG
        if (start_year is None or _cycle_year(entry.cycle_label) >= start_year)
        and (end_year is None or _cycle_year(entry.cycle_label) <= end_year)
    ]

    total_scraped = total_skipped = total_failed = total_paper = 0
    cycles_blocked = 0
    cycles_failed = cycles_empty = 0
    roster_rows = 0

    do_filings, do_roster = _resolve_scope(**scope_flags)
    do_roster = do_roster and roster

    try:
        driver = make_driver(headless=headless)
        try:
            # ── Candidate roster (party) ──────────────────────────────
            # Done first: it's ~28 quick page loads and it's the only source
            # of party, so an interrupted long filings scrape still leaves
            # the roster on disk for the parser to join.
            if do_roster:
                try:
                    roster_rows = scrape_candidate_roster(
                        driver, log, base_dir / ROSTER_CSV,
                        start_year=start_year, end_year=end_year)
                except Exception as e:
                    # Non-blocking: party is enrichment, not core data.
                    log.warning(f"Candidate roster scrape failed ({e}) — "
                                f"continuing; candidates will have no party.")

            if not do_filings:
                catalog = []
                log.info("Scope: roster only — skipping R&E filings.")

            # Main loop: one full search + scrape per (office, cycle). Each
            # entry is independent — a failure in one doesn't stop the rest.
            for entry in catalog:
                label = f"{entry.group} {entry.cycle_label} ({entry.option_text})"
                # A window that hasn't opened yet can't have filings, and the
                # form rejects future dates anyway — skip rather than search.
                if _clamp_future_date(entry.date_start) != entry.date_start:
                    log.info(f"Skipping {label}: its filing window starts "
                             f"{entry.date_start}, in the future.")
                    cycles_empty += 1
                    continue

                log.info(f"Searching: {label}, {entry.date_start}-{entry.date_end}")
                try:
                    has_results = submit_search(
                        driver, log, entry.option_text, entry.date_start,
                        entry.date_end, category=entry.category,
                        debug=debug_controls)
                except Exception as e:
                    log.warning(f"  Search submission failed for {label}: {e}")
                    cycles_failed += 1
                    continue  # move on to the next catalog entry

                if is_blocked(driver):
                    # A block here means the search itself was refused; every
                    # later cycle would "succeed" with zero rows otherwise.
                    if not wait_out_block(driver, log):
                        raise RuntimeError(
                            "sos.ks.gov is still blocking after backing off. Re-run "
                            "later (the manifest resumes where this stopped), and "
                            "consider a larger --delay.")
                    cycles_blocked += 1
                    continue

                if not has_results:
                    log.info(f"  {label}: no results grid — treating as an empty cycle")
                    cycles_empty += 1
                    continue

                try:
                    scraped, skipped, failed, paper = scrape_all_candidates_with_schedules(
                        driver, writers, entry.group, entry.cycle_label, done, force, log,
                        skip_empty_schedules=skip_empty_schedules, entry=entry,
                    )
                    total_scraped += scraped
                    total_skipped += skipped
                    total_failed  += failed
                    total_paper   += paper
                except Exception as e:
                    log.warning(f"  Scraping results grid failed for {label}: {e}")
                    cycles_failed += 1
                    _close_stray_windows(driver)
                    continue

                if debug_controls:
                    # --debug-controls is for inspecting the form, not for a
                    # full historical scrape — one cycle has already dumped
                    # all three stages, so stop rather than grinding through
                    # the remaining searches.
                    log.info("  --debug-controls: stopping after the first cycle.")
                    break
        finally:
            # Always tear the browser down — the pipeline runs this as a
            # subprocess and a lingering browser would hang the whole run.
            try:
                driver.quit()
            except Exception:
                pass

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_scraped:,} scraped, "
                 f"{total_skipped:,} skipped, {total_failed} failed, "
                 f"{total_paper:,} paper-only skipped, "
                 f"{cycles_failed} cycles failed, {cycles_empty} cycles empty, "
                 f"{cycles_blocked} blocked, "
                 f"{roster_rows:,} roster rows. CSVs written to {base_dir}/")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  candidates_scraped=total_scraped, candidates_skipped=total_skipped,
                  candidates_failed=total_failed, cycles_failed=cycles_failed,
                  cycles_empty=cycles_empty, cycles_blocked=cycles_blocked,
                  roster_rows=roster_rows,
                  candidates_paper_only=total_paper)

        # A run where every single cycle failed means the selectors have
        # drifted (or the site is down) — fail loudly so the pipeline aborts
        # this state instead of quietly parsing stale data.
        if catalog and cycles_failed == len(catalog):
            raise RuntimeError(
                f"All {cycles_failed} search(es) failed — the CFR Examiner's "
                f"form selectors have most likely changed. Re-run with "
                f"--debug-controls to dump the live control ids/options."
            )

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  candidates_scraped=total_scraped, candidates_skipped=total_skipped,
                  candidates_failed=total_failed)
        raise

    except Exception as e:
        # Log the reason as well as emitting it: orc.py surfaces a failed
        # scraper's stderr, but a bare sys.exit(1) would tell the operator
        # nothing about *why* the scrape died.
        log.error(f"Scrape failed: {type(e).__name__}: {e}")
        if isinstance(e, WebDriverException) and "chrome" in str(e).lower():
            log.error("Chrome couldn't be started. Selenium Manager resolves "
                      "chromedriver automatically, but a local Google Chrome "
                      "install is required.")
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  candidates_scraped=total_scraped, candidates_skipped=total_skipped,
                  candidates_failed=total_failed,
                  error_type=type(e).__name__, error=str(e))
        raise


# ============================= CLI ===================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Scrape Kansas candidate R&E filings from the SOS CFR Examiner into CSVs."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force", action="store_true",
                      help="re-scrape everything, wipe manifest and CSVs")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest cycle year to scrape")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest cycle year to scrape")
    ap.add_argument("--headless", action="store_true",
                    help="run Chrome headless — NOT recommended: CloudFront serves "
                         "headless Chrome a 403 block page instead of the site")
    ap.add_argument("--headed", action="store_true",
                    help="(default, kept for compatibility) visible Chrome window")
    ap.add_argument("--debug-controls", action="store_true",
                    help="dump every select/input/button id + option at all three "
                         "stages (entry page, search form, results page), then stop "
                         "after the first cycle")
    ap.add_argument("--delay", type=float, default=DEFAULT_REQUEST_DELAY,
                    metavar="SECONDS",
                    help=f"pause between candidates (default {DEFAULT_REQUEST_DELAY}); "
                         f"raise it if the site starts blocking mid-run")
    ap.add_argument("--out-dir", type=str, default=None,
                    help="directory for output CSVs (default: data/Kansas/raw/)")

    ap.add_argument("--no-roster", action="store_true",
                    help="skip the SOS Candidate List scrape (the only source of "
                         "candidate party); candidates then parse with party blank")

    # Horizontal scope flags — for KS these select between the R&E filings and
    # the candidate roster; see _resolve_scope(). A single filing contains both
    # contributions and expenditures, so those can't be split further.
    for flag in ("--transactions", "--contributions", "--expenditures"):
        ap.add_argument(flag, action="store_true",
                        help="R&E filings only (skip the candidate/party roster)")
    for flag in ("--entities", "--candidates", "--committees"):
        ap.add_argument(flag, action="store_true",
                        help="candidate/party roster only (skip the R&E filings)")

    args, _ = ap.parse_known_args()

    # Sanity-check the year flags before kicking off a (possibly long) run.
    cy = datetime.today().year
    if args.end_year and args.end_year > cy + 4:
        ap.error(f"--end-year cannot exceed {cy + 4}")
    if getattr(args, "start_year", None) and args.end_year:
        if args.start_year > args.end_year:
            ap.error("--start-year cannot be greater than --end-year")
    if args.force and args.end_year:
        ap.error("--force cannot be combined with --end-year")

    try:
        run(
            headless=args.headless,
            out_dir=args.out_dir,
            force=args.force,
            start_year=getattr(args, "start_year", None),
            end_year=args.end_year,
            debug_controls=args.debug_controls,
            roster=not args.no_roster,
            delay=args.delay,
            transactions=args.transactions, contributions=args.contributions,
            expenditures=args.expenditures, entities=args.entities,
            candidates=args.candidates, committees=args.committees,
        )
    except KeyboardInterrupt:
        sys.exit(130)  # conventional exit code for Ctrl-C
    except Exception as e:
        # Written to stderr (not just the log) so orc.py's subprocess_error
        # event and the run report show the actual cause, not just exit 1.
        print(f"[!] Kansas scrape failed: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
