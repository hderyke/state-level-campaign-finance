"""
kansas_v2.py — Drive the Kansas SOS CFR Examiner form to scrape candidate
Receipts & Expenditures filings directly into CSVs, replacing the old
kansas.py PDF-download pipeline (scrapers/kansas.py) with a scraper that
reads the same underlying data straight from the site's HTML, one
candidate/schedule at a time:

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

Coverage (mirrors kansas.py's INDEX_PAGES scope):
    House         — 2016, 2018, 2020, 2022, 2024, 2026 cycles
    Senate        — 2016, 2018-special, 2020, 2022-special, 2024, 2028
    Statewide     — 2014, 2018, 2022, 2026 (Governor, AG, SOS, Treasurer, Ins.)
    District Atty — 2016, 2020, 2024, 2028

IMPORTANT CAVEATS
-----------------
I could not load the live page's rendered HTML to confirm every element
ID myself (sos.ks.gov blocked automated fetch attempts, and it isn't
reachable from this sandbox's network either). Confirmed via your own
DOM inspection: txtStartDate, txtEndDate, drpdownFilingType, trOffice,
and btnSubmit (category-page Submit button). The category dropdown's own
id, and whether the final "Submit Search" button reuses btnSubmit or has
a distinct id, are still unconfirmed guesses.

Two more things below are new guesses, not confirmed live, and worth
checking on a first run:
  - The exact visible-text strings for each office in RUN_CATALOG (only
    "State Representative" was ever confirmed). If a cycle logs a
    "no option containing..." error, use debug_dump_controls() to get the
    real option text and fix the entry.
  - The date ranges per cycle are inferred (2-year span for House cycles,
    4-year span for Senate/Statewide/DA), not confirmed against how KS
    actually windows its "Date Range Filed" filter. If a cycle returns
    suspiciously few/many rows, narrow/widen OFFICE_CYCLE_SPAN_YEARS.

To make this resilient to unknown/incorrect IDs, this script:
  - Locates dropdowns via *substring* CSS attribute matches on id
    (e.g. any <select> whose id contains "Category"), not exact IDs.
  - Selects dropdown options by their *visible text*, not by value.
  - Has a debug_dump_controls() you can call if a selector fails — it
    prints every select/input's id/name/type on the current page so you
    can tell me what to hardcode.

Run this once with headless OFF (default below) so you can watch it and
confirm each step before trusting it in a headless/batch pipeline.

Project integration (mirrors kansas.py):
    Output (data/Kansas/):
        candidates_summary.csv, schedule_a_contributions.csv,
        schedule_b_inkind.csv, schedule_c_expenditures.csv,
        schedule_d_other.csv
        Every row in every one of these five files carries a
        candidate_uid column (same value as the manifest's
        candidate_key: "office_group|cycle_label|office_sought|
        district_number|name|original_date|amendment_date") so a
        downstream parser can join schedule rows back to the right
        candidate without relying on name text alone, which can collide
        across different cycles/offices. See parsers/kansas_v2.py.
    Manifest (data/Kansas/manifest_v2.csv — a different filename from the
        old kansas.py scraper's manifest.csv, which uses an incompatible
        schema; they live in the same data/Kansas/ directory but must
        never collide):
        candidate_key, office_group, cycle_label, office_text,
        candidate_name, office_sought, district_number, original_date,
        amendment_date, scraped_at
    Logging: src.reporting.logger.get_logger("kansas", "scrape")

CLI (mirrors kansas.py):
    (no flag)        incremental — skip candidates already in the
                     manifest; always re-check cycles whose year is
                     >= the current year.
    --start-year     only scrape cycles with cycle year >= YYYY
    --end-year       only scrape cycles with cycle year <= YYYY
    --force          wipe manifest and CSVs, re-scrape everything
"""

import csv
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
DATA_DIR      = PROJECT_ROOT / "data" / "Kansas"
MANIFEST      = DATA_DIR / "manifest_v2.csv"

DATA_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [
    "candidate_key", "office_group", "cycle_label", "office_text",
    "candidate_name", "office_sought", "district_number", "original_date",
    "amendment_date", "scraped_at",
]

ENTRY_URL = "https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx"

RESULTS_TABLE_ID = "grdviewCfrResults"

# ---- selectors ----------------------------------------------------------
# Confirmed via live DOM inspection: txtStartDate, txtEndDate, drpdownFilingType.
# "trOffice" was reported as the office control's id, but `tr` is the usual
# HTML prefix for a <tr> (table row), not a <select> — it's common for
# ASP.NET forms to wrap a dropdown's label+control in a row with an id like
# that. So we try #trOffice directly first, and if it isn't itself a
# <select>, we look for a <select> nested inside it. Same defensive
# handling applied to the still-unconfirmed category dropdown/buttons.
CATEGORY_SELECT_CSS = "#ddlViewerOptions"
CATEGORY_SUBMIT_CSS = "#btnSubmit"

START_DATE_CSS = "#txtStartDate"
END_DATE_CSS = "#txtEndDate"
FILING_TYPE_SELECT_CSS = "#drpdownFilingType"
OFFICE_CONTAINER_ID = "trOffice"
SEARCH_SUBMIT_CSS = "#btnSearch"

WAIT_TIMEOUT = 20

# ==================== Office / cycle catalog ===========================
# (office_group, cycle_label, office_dropdown_text, date_start, date_end)
#
# Mirrors kansas.py's INDEX_PAGES scope (House/Senate/Statewide/DA across
# the same election cycles), but expressed as CFR-examiner search
# parameters instead of index-page URLs. cycle_label matches the cycle
# label used in kansas.py so downstream code/analysis keyed on it still
# lines up.
#
# GUESS, UNCONFIRMED LIVE: the office_dropdown_text strings (only "State
# Representative" was ever confirmed against a real page) and the date
# spans (2-year window ending on the cycle year for House, 4-year window
# ending on the cycle year for Senate/Statewide/DA). If a run logs "no
# option containing..." for an office, use debug_dump_controls() on the
# results-form page to see real option text and fix it here.
HOUSE_CYCLES    = ["2016", "2018", "2020", "2022", "2024", "2026"]
SENATE_CYCLES   = ["2016", "2018-special", "2020", "2022-special", "2024", "2028"]
DA_CYCLES       = ["2016", "2020", "2024", "2028"]
STATEWIDE_CYCLES = ["2014", "2018", "2022", "2026"]
STATEWIDE_OFFICES = [
    "Governor", "Attorney General", "Secretary of State",
    "State Treasurer", "Insurance Commissioner",
]


def _cycle_span(cycle_label: str, years: int) -> tuple[str, str]:
    """'2026' + years=2 -> ('01/01/2025', '12/31/2026'). Handles
    '-special' suffixes (e.g. '2022-special') as a single-year window."""
    if cycle_label.endswith("-special"):
        year = int(cycle_label.split("-")[0])
        return f"01/01/{year}", f"12/31/{year}"
    year = int(cycle_label)
    return f"01/01/{year - years + 1}", f"12/31/{year}"


def _build_run_catalog() -> list[tuple[str, str, str, str, str]]:
    """Build the full list of searches run() will perform, one tuple per
    (office, cycle) combination. Called once at import time to populate
    RUN_CATALOG."""
    catalog: list[tuple[str, str, str, str, str]] = []

    # House: one search per 2-year cycle, all using the same dropdown text.
    for cycle in HOUSE_CYCLES:
        start, end = _cycle_span(cycle, 2)
        catalog.append(("House", cycle, "State Representative", start, end))

    # Senate: 4-year cycles, except "-special" cycles which get a 1-year
    # window since they're a single off-cycle election, not a full term.
    for cycle in SENATE_CYCLES:
        years = 1 if cycle.endswith("-special") else 4
        start, end = _cycle_span(cycle, years)
        catalog.append(("Senate", cycle, "State Senate", start, end))

    # District Attorney: 4-year cycles, same pattern as Senate (no specials).
    for cycle in DA_CYCLES:
        start, end = _cycle_span(cycle, 4)
        catalog.append(("DA", cycle, "District Attorney", start, end))

    # Statewide offices (Governor, AG, SOS, Treasurer, Insurance Commissioner)
    # each need their own separate search — the site's Office dropdown only
    # accepts one selection at a time — so every cycle expands into 5
    # catalog entries, one per office, all sharing that cycle's date range.
    for cycle in STATEWIDE_CYCLES:
        start, end = _cycle_span(cycle, 4)
        for office_text in STATEWIDE_OFFICES:
            catalog.append(("Statewide", cycle, office_text, start, end))

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

    Fails safe rather than crashing if MANIFEST exists but has the wrong
    columns (e.g. it's actually a different file, or a manifest from an
    incompatible version of this script) — logs a warning and starts as
    if nothing had been scraped yet, rather than raising a KeyError that
    would kill the whole run over a single bad/foreign file.
    """
    if not MANIFEST.exists():
        return {}  # first run ever — nothing scraped yet
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "candidate_key" not in reader.fieldnames:
            print(f"WARNING: {MANIFEST} exists but doesn't look like a "
                  f"kansas_v2.py manifest (missing 'candidate_key' column) — "
                  f"ignoring it and starting fresh. If this file belongs to "
                  f"something else, move it aside; kansas_v2.py will only "
                  f"ever write to {MANIFEST.name}.")
            return {}
        return {row["candidate_key"]: row for row in reader}


def strip_manifest(keep_fn) -> None:
    """Rewrite MANIFEST keeping only rows for which keep_fn(row) is True.
    Used by run() to wipe everything (--force) or to drop rows outside a
    --start-year/--end-year window before re-scraping that window."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))  # read the whole manifest into memory
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))  # write back only the kept rows


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
    visible on the results grid (no need to open the candidate's report
    to decide whether to skip it)."""
    return "|".join([
        office_group,
        cycle_label,
        row.get("office_sought", ""),
        row.get("district_number", ""),
        row.get("name", ""),
        row.get("original_date", ""),
        row.get("amendment_date", ""),
    ])


def wait_for(driver, css_selector, timeout=WAIT_TIMEOUT):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
    )


def select_by_visible_text_containing(driver, select_css, text_substring, timeout=WAIT_TIMEOUT):
    """
    Find a <select> matching select_css and choose the option whose visible
    text contains text_substring (case-insensitive). More robust than
    matching on the option's `value`, which we can't see without loading
    the page ourselves.
    """
    el = wait_for(driver, select_css, timeout)
    dropdown = Select(el)
    target = text_substring.strip().lower()
    for option in dropdown.options:  # scan every <option> for a text match
        if target in option.text.strip().lower():
            dropdown.select_by_visible_text(option.text)
            return
    # No match found — raise with the real option list so it's obvious
    # whether the dropdown text guess was wrong or the option truly isn't there.
    available = [o.text for o in dropdown.options]
    raise ValueError(
        f"No option containing {text_substring!r} found for selector "
        f"{select_css!r}. Available options: {available}"
    )


def select_by_typeahead(driver, select_css, keys_to_type, pause=0.3):
    """
    Fallback for selects that don't populate their <option> elements in time
    for Select()-based matching (e.g. options injected by JS after the
    initial page load). Clicks the element and sends keystrokes, mimicking
    a user typing to jump to a matching option — this works against the
    native <select> control itself rather than relying on its DOM options
    being present yet.
    """
    el = wait_for(driver, select_css)
    el.click()
    el.send_keys(keys_to_type)
    time.sleep(pause)
    return el


def resolve_select_element(driver, container_id, timeout=WAIT_TIMEOUT):
    """
    Given an id that might belong directly to a <select>, or to a wrapping
    element (e.g. a <tr>/<td>/<div>) that contains the actual <select>,
    return the <select> WebElement.
    """
    el = wait_for(driver, f"#{container_id}", timeout)
    if el.tag_name.lower() == "select":
        return el  # the id belongs directly to the <select>
    nested = el.find_elements(By.TAG_NAME, "select")
    if nested:
        return nested[0]  # id was a wrapper (e.g. <tr>) — use the <select> inside it
    raise ValueError(
        f"#{container_id} is a <{el.tag_name}>, not a <select>, and no "
        f"<select> was found nested inside it."
    )


def select_office_by_text(driver, container_id, text_substring, timeout=WAIT_TIMEOUT):
    """Same visible-text matching as select_by_visible_text_containing,
    but for the office dropdown, whose id might belong to a wrapper
    element rather than the <select> itself (see resolve_select_element)."""
    el = resolve_select_element(driver, container_id, timeout)
    dropdown = Select(el)
    target = text_substring.strip().lower()
    for option in dropdown.options:
        if target in option.text.strip().lower():
            dropdown.select_by_visible_text(option.text)
            return
    available = [o.text for o in dropdown.options]
    raise ValueError(
        f"No option containing {text_substring!r} found under #{container_id}. "
        f"Available options: {available}"
    )


def parse_results_page(driver):
    """
    Parse the current page of the grdviewCfrResults grid into a list of
    dicts, using the row_N suffix convention confirmed from the saved
    results-page HTML (e.g. grdviewCfrResults_lblOriginalDate_0,
    _lnkbtnName_0, _lblAddress_0, _lblCity_0, _lblZip_0,
    _labelOfficeSought_0, _lblDistrictNumber_0).
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find(id=RESULTS_TABLE_ID)
    if table is None:
        raise ValueError(f"No table with id={RESULTS_TABLE_ID!r} found on page.")

    def text_by_id_prefix(prefix, index):
        # Each grid cell's id follows "{TABLE_ID}_{prefix}_{row_index}",
        # e.g. grdviewCfrResults_lblOriginalDate_0 for row 0's date cell.
        el = table.find(id=f"{RESULTS_TABLE_ID}_{prefix}_{index}")
        return el.get_text(strip=True) if el else ""

    rows = []
    index = 0
    # Keep reading row 0, 1, 2, ... until a row's date cell no longer
    # exists — that's how we know we've hit the end of this page's rows.
    while table.find(id=f"{RESULTS_TABLE_ID}_lblOriginalDate_{index}") is not None:
        rows.append({
            "original_date": text_by_id_prefix("lblOriginalDate", index),
            "amendment_date": text_by_id_prefix("lblAmendmentDate", index),
            "name": text_by_id_prefix("lnkbtnName", index),
            "address": text_by_id_prefix("lblAddress", index),
            "other": text_by_id_prefix("lblOther", index),
            "city": text_by_id_prefix("lblCity", index),
            "zip": text_by_id_prefix("lblZip", index),
            "office_sought": text_by_id_prefix("labelOfficeSought", index),
            "district_number": text_by_id_prefix("lblDistrictNumber", index),
            "row_index": index,
        })
        index += 1
    return rows


def _find_pager_row(table):
    """
    Return the <tr> most likely to be the pager row, or None if the table
    doesn't seem to have one (a single page of results).

    The old approach scanned every <span>/<a> in the WHOLE results table
    for digit-only text — but the grid also has a district_number column
    full of plain digit spans (e.g. "12", "45"), which could get mixed in
    with real pager buttons and silently corrupt page detection. This
    scopes detection to whichever single <tr> has the most digit-only
    span/anchor children, which is a much stronger signal that it's
    actually the pager row and not a row of candidate data.
    """
    best_row, best_count = None, 1   # require at least 2 digit cells to count as a pager
    for tr in table.find_elements(By.TAG_NAME, "tr"):
        cells = tr.find_elements(By.CSS_SELECTOR, "td > span, td > a")
        digit_count = sum(1 for el in cells if el.text.strip().isdigit())
        if digit_count > best_count:
            best_row, best_count = tr, digit_count
    return best_row


def get_current_page_number(driver):
    """The pager's current page is a plain <span> (not clickable); other
    pages are <a> links. Returns the int shown as the non-link span,
    searching only within the detected pager row (see _find_pager_row)."""
    table = driver.find_element(By.ID, RESULTS_TABLE_ID)
    pager_row = _find_pager_row(table)
    if pager_row is None:
        return 1  # no pager row found — single page of results
    for s in pager_row.find_elements(By.TAG_NAME, "span"):
        t = s.text.strip()
        if t.isdigit():
            return int(t)  # found the non-link page-number span
    return 1


def go_to_results_page(driver, page_number, timeout=WAIT_TIMEOUT):
    """
    Click the pager link for page_number within the results grid and wait
    for the postback to refresh the grid (detected via staleness of the
    current table element).
    """
    table = driver.find_element(By.ID, RESULTS_TABLE_ID)
    pager_row = _find_pager_row(table)
    search_root = pager_row if pager_row is not None else table
    try:
        # normalize-space(.) (not text()) also matches page numbers wrapped
        # in a nested element (e.g. <a><span>2</span></a>), not just a bare
        # text node directly inside the <a>.
        link = search_root.find_element(By.XPATH, f".//a[normalize-space(.)='{page_number}']")
    except Exception:
        raise ValueError(f"No pager link found for page {page_number}.")
    link.click()
    # ASP.NET postback replaces the whole grid; wait for the old <table>
    # element to go stale, then for the new one to appear before continuing.
    WebDriverWait(driver, timeout).until(EC.staleness_of(table))
    wait_for(driver, f"#{RESULTS_TABLE_ID}", timeout)
    time.sleep(0.5)  # let the grid finish rendering


def get_available_pager_pages(driver):
    """Return the list of page numbers (ints) currently shown in the pager
    row, including the current page. Scoped to the detected pager row only
    (see _find_pager_row) so data-column digits can't pollute the result."""
    table = driver.find_element(By.ID, RESULTS_TABLE_ID)
    pager_row = _find_pager_row(table)
    if pager_row is None:
        return [1]  # no pager row found — treat as a single page
    nums = set()
    for el in pager_row.find_elements(By.CSS_SELECTOR, "td > span, td > a"):
        t = el.text.strip()
        if t.isdigit():
            nums.add(int(t))
    return sorted(nums)


def advance_pager_window(driver, timeout=WAIT_TIMEOUT) -> bool:
    """
    ASP.NET GridView pagers commonly show only a fixed window of page
    numbers (e.g. "1 2 3 4 ...") and require clicking a "..." link to
    reveal the next window of page numbers, rather than showing every
    page number at once. If the page we want isn't in
    get_available_pager_pages()'s result, this looks for that "..."
    control and clicks it so a wider set of page numbers becomes
    available. Returns True if a "..." control was found and clicked,
    False if there wasn't one (meaning the visible pages really are all
    there are).

    UNCONFIRMED LIVE: "..." is the standard GridView convention, but the
    exact text/rendering on this site hasn't been verified. If pages are
    still being missed after this, use debug_dump_controls() on the
    results page and tell me what the pager's overflow control looks like.
    """
    table = driver.find_element(By.ID, RESULTS_TABLE_ID)
    try:
        ellipsis = table.find_element(
            By.XPATH, ".//a[normalize-space(text())='...' or normalize-space(text())='…']"
        )
    except Exception:
        return False
    ellipsis.click()
    WebDriverWait(driver, timeout).until(EC.staleness_of(table))
    wait_for(driver, f"#{RESULTS_TABLE_ID}", timeout)
    time.sleep(0.5)
    return True


def scrape_all_result_pages(driver, max_pages=None):
    """
    Walk every page of the results grid (following the pager's numbered
    links — e.g. "1 2 3 4" — until no higher page number is offered) and
    return the combined list of row dicts. If the pager only shows a
    window of page numbers, advance_pager_window() clicks past it rather
    than stopping early.
    """
    all_rows = []
    seen_pages = set()

    while True:
        current = get_current_page_number(driver)
        if current in seen_pages:
            break
        seen_pages.add(current)

        page_rows = parse_results_page(driver)
        for r in page_rows:
            r["page"] = current
        all_rows.extend(page_rows)

        if max_pages is not None and len(seen_pages) >= max_pages:
            break

        available = get_available_pager_pages(driver)
        next_page = current + 1
        if next_page not in available:
            if not advance_pager_window(driver):
                break  # no "..." control — this really is the last page
            available = get_available_pager_pages(driver)
            if next_page not in available:
                break  # widened the window and it's still not there — stop

        go_to_results_page(driver, next_page)

    return all_rows


def click_and_capture_page(driver, element, timeout=WAIT_TIMEOUT):
    """
    Click `element` and return the resulting page's HTML, handling both
    ways Kansas's ASP.NET postback links have been observed to behave:

      (a) In-place navigation: the click submits the form and the server
          redirects the *same* window to a new .aspx page (this is what
          the saved report/schedule HTML files you gave me show — each
          one is a distinct URL: exp_report_main.aspx,
          schedule_a_report.aspx, etc.)
      (b) New window/tab: some links (e.g. the candidate name link in the
          results grid) carry a title like "open ... in a new window",
          which may mean a JS popup instead.

    Returns (html, new_window_handle_or_None). If a new window was opened,
    the caller must pass that handle to close_and_return() when done with
    it. Otherwise pass None and close_and_return() will use driver.back().
    """
    original_handles = set(driver.window_handles)
    try:
        marker = driver.find_element(By.TAG_NAME, "body")
    except Exception:
        marker = None

    element.click()

    # Case (b): did a new window/tab appear?
    try:
        WebDriverWait(driver, 3).until(
            lambda d: len(set(d.window_handles) - original_handles) > 0
        )
        new_handle = (set(driver.window_handles) - original_handles).pop()
        driver.switch_to.window(new_handle)
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        return driver.page_source, new_handle
    except Exception:
        pass

    # Case (a): assume in-place navigation; wait for the old DOM to die.
    if marker is not None:
        try:
            WebDriverWait(driver, timeout).until(EC.staleness_of(marker))
        except Exception:
            pass
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    return driver.page_source, None


def close_and_return(driver, new_handle, original_handle, timeout=WAIT_TIMEOUT):
    """Undo click_and_capture_page(): close the popup window if one was
    opened, otherwise navigate back in history. Always ends with focus on
    original_handle."""
    if new_handle is not None:
        driver.close()
        driver.switch_to.window(original_handle)
    else:
        driver.back()
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )


# ---- schedule / report page parsing -------------------------------------
# Built and verified against the sample HTML you provided (all four
# schedule types plus the summary "Receipts and Expenditures Report" page).

def cell_lines(td):
    """
    Reconstruct the visually-rendered <br>-separated lines of a table cell,
    regardless of whether each line's text is a bare NavigableString or
    sits inside a nested <span id="...">. This is necessary because the
    Kansas markup mixes both (e.g. an individual contributor's name is bare
    text, but their zip code is `<span id="Repeater2_lblZip_0">`), and
    plain get_text() would lose the line breaks entirely.
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
    get = lambda sid: _label_text(soup, sid)  # shorthand: look up one <span id="..."> by id

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
    Every schedule page has (in this order among width=98% border=1
    tables): the itemized-transaction data table (has <th> headers), then
    one or more small totals tables (each row's first <td> is a bold9
    label like 'TOTAL EXPENDITURES...'). Returns (data_table, totals_table).
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
    """Schedule A — Contributions and Other Receipts. Returns
    (rows, totals) where rows is a list of dicts, one per itemized entry.

    Shared pattern used by all four parse_schedule_* functions below:
      1. find the data table + totals table via _find_schedule_tables()
      2. for each <tr>, skip rows that don't have enough <td>s (headers/
         spacers)
      3. column 0 is always the date; column 1 is always a multi-line
         name+address cell, decoded with cell_lines() and
         split_city_state_zip()
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
    NOTE: the "Purpose" column mixes a category label (e.g.
    "Reimbursement", "Printing") and a free-text description with only a
    space between them and no reliable markup boundary (confirmed against
    your sample file — e.g. "Printing printing/mailing"). Rather than
    guess wrong, this is kept as a single `purpose_raw` field; split it
    manually if you need the category isolated.
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
    # Schedule D's grand-total cell has no id/label markup to key off of
    # (see the sample file), so fall back to summing the itemized rows.
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
            pass  # file doesn't exist yet — first write() will create it with a header

    def write_rows(self, rows):
        if not rows:
            return
        mode = "a" if self._wrote_header else "w"
        with open(self.path, mode, newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
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


def scrape_candidate_report(driver, row_index, writers, candidate_uid, skip_empty_schedules=True):
    """
    From the results grid, click the row_index'th candidate's name link,
    scrape the summary report, then open+scrape each non-empty schedule
    (A/B/C/D), writing everything to the shared `writers` dict of
    IncrementalCsvWriter (keys: 'summary', 'schedule_a'..'schedule_d').
    Leaves the driver back on the results grid when done.

    `candidate_uid` is the manifest key computed by the caller (see
    make_candidate_key) — it's stamped onto every row this candidate
    produces (summary AND each schedule row) so a downstream parser can
    join schedule rows back to the right summary row without relying on
    candidate name text alone, which can collide across different
    cycles/offices for someone with the same name.
    """
    original_handle = driver.current_window_handle
    name_link = driver.find_element(By.ID, f"{RESULTS_TABLE_ID}_lnkbtnName_{row_index}")

    # Step 1: open the candidate's summary report and write it to the
    # "summary" CSV. candidate_name_text (the site's own name string) is
    # what schedule rows carry in their "candidate" column — candidate_uid
    # is the reliable join key across all five output files.
    summary_html, new_handle = click_and_capture_page(driver, name_link)
    summary_url = driver.current_url
    summary = parse_summary_report(summary_html, source_url=summary_url)
    summary["candidate_uid"] = candidate_uid
    writers["summary"].write_rows([summary])
    candidate_name_text = summary["candidate_name"] or f"row_{row_index}"

    # Step 2: for each schedule (A/B/C/D), skip it if its summary total is
    # $0 (nothing to itemize), otherwise open it, parse its rows, write
    # them to that schedule's CSV, then navigate back to the summary page.
    for link_id, total_key, parse_fn, csv_key in SCHEDULE_LINKS:
        if skip_empty_schedules and not summary.get(total_key):
            continue
        try:
            link_el = driver.find_element(By.ID, link_id)
        except Exception:
            continue  # this schedule's link wasn't on the page
        sched_html, sched_new_handle = click_and_capture_page(driver, link_el)
        sched_url = driver.current_url
        rows, _totals = parse_fn(sched_html, candidate_name_text, source_url=sched_url)
        for r in rows:
            r["candidate_uid"] = candidate_uid
        writers[csv_key].write_rows(rows)
        # back to the summary page
        close_and_return(driver, sched_new_handle, driver.current_window_handle)

    # Step 3: back to the results grid, ready for the next row.
    close_and_return(driver, new_handle, original_handle)


def scrape_all_candidates_with_schedules(driver, writers, office_group, cycle_label,
                                          done, force, log, max_pages=None,
                                          skip_empty_schedules=True):
    """
    Full pipeline for one (office_group, cycle_label) search already
    submitted on the results grid (see run()): walks every page, and for
    every row, first builds a manifest key from the fields already on the
    grid (name/office/district/dates) to decide whether it's already been
    scraped in a previous run. If not skipped, opens the candidate's
    summary report plus each non-empty Schedule A/B/C/D breakdown and
    appends rows to the shared `writers` (IncrementalCsvWriter instances,
    created once per full run() call so CSVs accumulate across cycles).

    `done` is the manifest dict (candidate_key -> row) loaded at the start
    of run(); newly-scraped candidates are added to it and appended to
    MANIFEST on disk as they're processed, so an interrupted run keeps
    everything scraped up to that point and a later run can resume.
    """
    is_current = is_current_cycle(cycle_label, datetime.today().year)
    today = datetime.today().strftime("%Y-%m-%d")

    seen_pages = set()
    page_count = 0
    scraped = skipped = failed = 0
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
            candidate_key = make_candidate_key(office_group, cycle_label, row)

            # Skip logic: skip if already in the manifest, UNLESS --force
            # was passed or this cycle is still "current" (year >= this
            # year) and so could have new/updated filings worth re-checking.
            already_done = candidate_key in done
            if already_done and not force and not is_current:
                skipped += 1
                continue

            try:
                scrape_candidate_report(driver, row_index, writers, candidate_key,
                                         skip_empty_schedules=skip_empty_schedules)
                scraped += 1
            except Exception as e:
                log.warning(f"  [{office_group} {cycle_label}, page {current}, row {row_index}] failed: {e}")
                failed += 1
                # best effort: make sure we're back on the results grid
                # before moving on to the next row
                try:
                    wait_for(driver, f"#{RESULTS_TABLE_ID}")
                except Exception:
                    pass
                continue

            # Record this candidate as done — both to the in-memory `done`
            # dict (so later rows/cycles in this same run see it too) and
            # to disk immediately (so an interrupted run doesn't lose it).
            record = {
                "candidate_key": candidate_key,
                "office_group": office_group,
                "cycle_label": cycle_label,
                "office_text": row.get("office_sought", ""),
                "candidate_name": row.get("name", ""),
                "office_sought": row.get("office_sought", ""),
                "district_number": row.get("district_number", ""),
                "original_date": row.get("original_date", ""),
                "amendment_date": row.get("amendment_date", ""),
                "scraped_at": today,
            }
            append_manifest(record)
            done[candidate_key] = record

        if max_pages is not None and page_count >= max_pages:
            break

        # Advance to the next page of results. If the pager only shows a
        # window of page numbers, try clicking past it (advance_pager_window)
        # before concluding we've actually reached the last page.
        available = get_available_pager_pages(driver)
        next_page = current + 1
        if next_page not in available:
            if not advance_pager_window(driver):
                log.info(f"  {office_group} {cycle_label}: stopping after page "
                         f"{current} — pager shows pages {available}, no "
                         f"'...' control found. If more pages should exist, "
                         f"call debug_dump_controls() on the results page to "
                         f"inspect the pager markup.")
                break  # no "..." control — this really is the last page
            available = get_available_pager_pages(driver)
            if next_page not in available:
                log.info(f"  {office_group} {cycle_label}: stopping after page "
                         f"{current} — widened pager still only shows "
                         f"{available}.")
                break  # widened the window and it's still not there — stop
        go_to_results_page(driver, next_page)

    log.info(f"  {office_group} {cycle_label}: {scraped} scraped, {skipped} skipped, {failed} failed")
    return scraped, skipped, failed


def capture_pdf_url_for_row(driver, row_index, timeout=10):
    """
    Click a row's name link (opens the filing PDF in a new window/tab per
    its title attribute) and capture the resulting URL, then close that
    tab and switch back. Returns the PDF URL, or None if nothing new
    opened (e.g. it renders inline instead of a new window — unconfirmed
    without a live run).
    """
    original_handles = set(driver.window_handles)
    link = driver.find_element(By.ID, f"{RESULTS_TABLE_ID}_lnkbtnName_{row_index}")
    link.click()

    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(set(d.window_handles) - original_handles) > 0
        )
    except Exception:
        return None  # no new tab/window appeared

    new_handle = (set(driver.window_handles) - original_handles).pop()
    driver.switch_to.window(new_handle)
    time.sleep(1)  # let the PDF viewer finish navigating
    pdf_url = driver.current_url
    driver.close()
    driver.switch_to.window(list(original_handles)[0])
    return pdf_url


def save_rows_to_csv(rows, path):
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def dismiss_datepicker(driver):
    """
    The date fields open a JS calendar popup on focus/input. If it's still
    open, it visually overlaps other controls (like the Submit Search
    button) and intercepts clicks meant for them. Pressing Escape and then
    clicking a neutral part of the page closes it.
    """
    try:
        driver.switch_to.active_element.send_keys(Keys.ESCAPE)
    except Exception:
        pass
    try:
        driver.find_element(By.TAG_NAME, "body").click()
    except Exception:
        pass
    time.sleep(0.3)


def debug_dump_controls(driver):
    """Print every select/input element's id/name/type — use this if a
    selector above doesn't match, then send me the output."""
    print("\n--- SELECT elements on page ---")
    for el in driver.find_elements(By.TAG_NAME, "select"):
        print(f"  id={el.get_attribute('id')!r} name={el.get_attribute('name')!r}")

    print("--- INPUT elements on page ---")
    for el in driver.find_elements(By.TAG_NAME, "input"):
        print(
            f"  id={el.get_attribute('id')!r} name={el.get_attribute('name')!r} "
            f"type={el.get_attribute('type')!r} value={el.get_attribute('value')!r}"
        )
    print("--------------------------------\n")


def make_driver(headless: bool = False) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    return webdriver.Chrome(options=options)


def submit_candidate_filings_search(driver, log, office_text, date_start, date_end):
    """
    From a fresh load of ENTRY_URL: select category dropdown + Submit,
    then fill Date Range Filed / Filing Type / Office and Submit Search.
    Leaves the driver on the results grid. Raises on any selector failure
    (after dumping the page's controls for debugging).
    """
    driver.get(ENTRY_URL)

    # ---- Step 1: category dropdown ("Candidate Campaign Filings") + Submit
    try:
        select_by_visible_text_containing(
            driver, CATEGORY_SELECT_CSS, "Candidate Campaign Filings"
        )
    except Exception as e:
        log.warning(f"Visible-text match failed ({e}); trying type-ahead fallback ('CA')...")
        try:
            select_by_typeahead(driver, CATEGORY_SELECT_CSS, "CA")
        except Exception as e2:
            log.warning(f"Type-ahead fallback also failed: {e2}")
            debug_dump_controls(driver)
            raise

    try:
        driver.find_element(By.CSS_SELECTOR, CATEGORY_SUBMIT_CSS).click()
    except Exception as e:
        log.warning(f"Category Submit button selector failed: {e}")
        debug_dump_controls(driver)
        raise

    # ---- Step 2: wait for the next (Candidate Campaign Filings) form to load
    try:
        wait_for(driver, START_DATE_CSS)
    except Exception as e:
        log.warning(f"Start-date field never appeared after Submit: {e}")
        debug_dump_controls(driver)
        raise

    # Fill in this catalog entry's date range.
    start_date = driver.find_element(By.CSS_SELECTOR, START_DATE_CSS)
    start_date.clear()
    start_date.send_keys(date_start)

    end_date = driver.find_element(By.CSS_SELECTOR, END_DATE_CSS)
    end_date.clear()
    end_date.send_keys(date_end)
    dismiss_datepicker(driver)  # close the JS calendar popup before touching other fields

    # Filing type is always the same for every catalog entry.
    try:
        select_by_visible_text_containing(
            driver, FILING_TYPE_SELECT_CSS, "Receipts and Expenditures Report"
        )
    except Exception as e:
        log.warning(f"Filing Type dropdown selector failed: {e}")
        debug_dump_controls(driver)
        raise

    # Office varies per catalog entry (e.g. "State Representative", "Governor").
    try:
        select_office_by_text(driver, OFFICE_CONTAINER_ID, office_text)
    except Exception as e:
        log.warning(f"Office dropdown selector failed for {office_text!r}: {e}")
        debug_dump_controls(driver)
        raise

    # ---- Step 3: submit the search and wait for the results grid
    try:
        dismiss_datepicker(driver)
        driver.find_element(By.CSS_SELECTOR, SEARCH_SUBMIT_CSS).click()
    except Exception as e:
        log.warning(f"Submit Search button selector failed: {e}")
        debug_dump_controls(driver)
        raise

    # give the ASP.NET postback time to render the results grid
    time.sleep(2)
    wait_for(driver, f"#{RESULTS_TABLE_ID}")


def run(headless: bool = False, out_dir: str | None = None,
        force: bool = False, start_year: int | None = None,
        end_year: int | None = None, skip_empty_schedules: bool = True):
    """
    Scrape Kansas candidate R&E filings for every (office, cycle) in
    RUN_CATALOG, writing five CSVs into out_dir (default: data/Kansas/)
    and tracking progress in MANIFEST so re-runs skip already-scraped
    candidates.

    Vertical scope:
        (no flag)        incremental — skip candidates already in the
                         manifest; always re-check cycles whose year is
                         >= the current year.
        --start-year     only scrape cycles with cycle year >= YYYY
        --end-year       only scrape cycles with cycle year <= YYYY
        --force          wipe manifest and CSVs, re-scrape everything
    """
    out_dir = out_dir or str(DATA_DIR)
    log = get_logger("kansas", "scrape")
    t0 = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year)

    import os

    # The five CSV outputs this run writes into (shared across every
    # catalog entry so a candidate scraped under one office/cycle and one
    # scraped under another both land in the same summary/schedule files).
    csv_paths = {
        "summary": os.path.join(out_dir, "candidates_summary.csv"),
        "schedule_a": os.path.join(out_dir, "schedule_a_contributions.csv"),
        "schedule_b": os.path.join(out_dir, "schedule_b_inkind.csv"),
        "schedule_c": os.path.join(out_dir, "schedule_c_expenditures.csv"),
        "schedule_d": os.path.join(out_dir, "schedule_d_other.csv"),
    }

    year_range_active = start_year is not None or end_year is not None

    # ── Manifest / CSV prep ────────────────────────────────────────────
    if force:
        # --force: start completely clean — wipe the manifest and delete
        # any existing CSVs so every candidate gets re-scraped from scratch.
        strip_manifest(lambda _: False)
        for p in csv_paths.values():
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
        done = {}
    elif year_range_active:
        # --start-year/--end-year: drop manifest rows for cycles inside the
        # requested window (so they'll be re-scraped) but keep rows for
        # cycles outside it (so an unrelated earlier scrape isn't touched).
        def _outside_range(r: dict) -> bool:
            yr = _cycle_year(r.get("cycle_label", ""))
            if start_year is not None and yr >= start_year:
                return False
            if end_year is not None and yr <= end_year:
                return False
            return True
        strip_manifest(_outside_range)
        done = load_manifest()
    else:
        # Default/incremental: keep the whole manifest as-is; per-candidate
        # skip logic happens later in scrape_all_candidates_with_schedules.
        done = load_manifest()

    # One IncrementalCsvWriter per output file, reused across every
    # catalog entry in this run so rows accumulate rather than overwrite.
    writers = {
        "summary": IncrementalCsvWriter(csv_paths["summary"], SUMMARY_FIELDS),
        "schedule_a": IncrementalCsvWriter(csv_paths["schedule_a"], SCHEDULE_A_FIELDS),
        "schedule_b": IncrementalCsvWriter(csv_paths["schedule_b"], SCHEDULE_B_FIELDS),
        "schedule_c": IncrementalCsvWriter(csv_paths["schedule_c"], SCHEDULE_C_FIELDS),
        "schedule_d": IncrementalCsvWriter(csv_paths["schedule_d"], SCHEDULE_D_FIELDS),
    }

    # Narrow RUN_CATALOG down to just the cycles requested via --start-year/--end-year.
    catalog = [
        entry for entry in RUN_CATALOG
        if (start_year is None or _cycle_year(entry[1]) >= start_year)
        and (end_year is None or _cycle_year(entry[1]) <= end_year)
    ]

    driver = make_driver(headless=headless)
    total_scraped = total_skipped = total_failed = 0
    cycles_failed = 0

    try:
        # Main loop: one full search + scrape per (office, cycle) entry.
        # Each entry is independent — a failure in one doesn't stop the rest.
        for office_group, cycle_label, office_text, date_start, date_end in catalog:
            log.info(f"Searching: {office_group} {cycle_label} ({office_text}), "
                     f"{date_start}-{date_end}")
            try:
                submit_candidate_filings_search(driver, log, office_text, date_start, date_end)
            except Exception as e:
                log.warning(f"  Search submission failed for {office_group} {cycle_label} "
                            f"({office_text}): {e}")
                cycles_failed += 1
                continue  # move on to the next catalog entry

            try:
                scraped, skipped, failed = scrape_all_candidates_with_schedules(
                    driver, writers, office_group, cycle_label, done, force, log,
                    skip_empty_schedules=skip_empty_schedules,
                )
                total_scraped += scraped
                total_skipped += skipped
                total_failed += failed
            except Exception as e:
                log.warning(f"  Scraping results grid failed for {office_group} {cycle_label} "
                            f"({office_text}): {e}")
                cycles_failed += 1
                continue

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_scraped:,} scraped, "
                 f"{total_skipped:,} skipped, {total_failed} failed, "
                 f"{cycles_failed} cycles failed. CSVs written to {out_dir}/")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  candidates_scraped=total_scraped, candidates_skipped=total_skipped,
                  candidates_failed=total_failed, cycles_failed=cycles_failed)

        # Uncomment once you've confirmed everything works end-to-end:
        # driver.quit()
        return None

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  candidates_scraped=total_scraped, candidates_skipped=total_skipped,
                  candidates_failed=total_failed)
        log.warning("Leaving browser window open for inspection (not quitting driver).")
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  candidates_scraped=total_scraped, candidates_skipped=total_skipped,
                  candidates_failed=total_failed,
                  error_type=type(e).__name__, error=str(e))
        log.warning("Leaving browser window open for inspection (not quitting driver).")
        raise


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
                    help="run Chrome headless (default: visible, for first-run verification)")
    ap.add_argument("--out-dir", type=str, default=None,
                    help="directory for output CSVs and manifest (default: data/Kansas/)")

    # Horizontal scope flags — all accepted but ignored, for CLI parity
    # with kansas.py (Kansas filings contain all data types together).
    for flag in ("--transactions", "--entities", "--contributions",
                 "--expenditures", "--candidates", "--committees"):
        ap.add_argument(flag, action="store_true",
                        help="(ignored — Kansas filings contain all data types)")

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
        )
    except KeyboardInterrupt:
        sys.exit(130)  # conventional exit code for Ctrl-C
    except Exception:
        sys.exit(1)