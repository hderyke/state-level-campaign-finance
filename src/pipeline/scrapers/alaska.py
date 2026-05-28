"""
scrapers/alaska.py — Download Alaska APOC campaign finance data.

Requires a live browser session via Playwright — Alaska's WAF blocks datacenter
IPs, so this must be run from a local machine. Exports are triggered by clicking
Search then Export, mirroring normal user interaction. GR and CR detail pages
are scraped individually by numeric ID with a consecutive-blank cutoff.
"""

import csv
import html as html_mod
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR      = PROJECT_ROOT / "data" / "Alaska" / "raw"
MANIFEST     = PROJECT_ROOT / "data" / "Alaska" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]



# =============================== pages ================================

# Alaska requires a live browser session — Playwright handles this by clicking
# Search then Export just like a user would. Must be run from a local machine;
# datacenter IPs are blocked by Alaska's WAF.
PAGES = {
    "income":       "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
    "expenditures": "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDExpenditures.aspx",
    "candidates":   "https://aws.state.ak.us/apocreports/Campaign/AllCandidates.aspx?type=all",
    "groups":       "https://aws.state.ak.us/apocreports/Registration/GroupRegistration/GRForms.aspx",
}

TRANSACTION_RELATIONS = {"income", "expenditures"}
ENTITY_RELATIONS      = {"candidates", "groups"}

STEMS = {
    "income":       "CDIncome",
    "expenditures": "CDExpense",
    "candidates":   "CDCandidates",
    "groups":       "GRForms",
}

# ========================== GR detail scrape ==========================
GR_DETAIL_URL        = "https://aws.state.ak.us/apocreports/Common/View.aspx?ID={id}&ViewType=GR"
GR_DETAILS_PATH      = RAW_DIR / "gr_details.csv"
MIN_GR_ID            = 0
MAX_CONSECUTIVE_BLANK = 2000   # stop if this many consecutive IDs return blank

GR_DETAILS_COLS = [
    "gr_id", "group_name", "abbreviation", "group_type", "purpose",
    "address", "city", "zip",
    "chair_name", "chair_phone", "chair_email",
    "treasurer_name", "treasurer_phone", "treasurer_email",
    "election_year", "submission_date", "previously_registered",
]

# ========================== CR detail scrape ==========================
CR_DETAIL_URL        = "https://aws.state.ak.us/apocreports/Common/View.aspx?ID={id}&ViewType=CR"
CR_DETAILS_PATH      = RAW_DIR / "cr_details.csv"
MIN_CR_ID            = 0
MAX_CONSECUTIVE_CR_BLANK = 2500

CR_DETAILS_COLS = [
    "cr_id", "candidate_display_name", "candidate_first", "candidate_last",
    "committee_name", "city", "zip",
    "treasurer_name", "treasurer_phone", "treasurer_email",
    "election_year", "election", "office_type",
    "submission_date", "previously_registered",
]

# ======================== Field label pattern =========================
FIELD_LABELS = [
    # GR fields
    "Group Name",
    "Abbreviation",
    "Purpose",
    "Group Type",
    "Group Mailing Address",
    "Additional Email Addresses to Notify",
    "Chair Name",
    "Treasurer Name",
    # CR fields
    "Candidate Display Name",
    "Candidate Legal First Name",
    "Candidate Legal Last Name",
    "Campaign Committee Name",
    "Campaign Mailing Address",
    "Office Type",
    "Election",
    "Name of Bank",
    # Shared
    "City, State Zip",
    "Phone",
    "E-mail",
    "Fax (Optional)",
    "Election Year",
    "Submission Date",
    "Previously Registered",
]

FIELD_PATTERN = "|".join(
    re.escape(f)
    for f in sorted(FIELD_LABELS, key=len, reverse=True)
)

def clean_date(value: str) -> str:
    m = re.search(r"\d{1,2}/\d{1,2}/\d{4}", value)
    return m.group(0) if m else value.strip()

def _get(text: str, label: str) -> str:
    pattern = rf"""
        \b{re.escape(label)}
        \s*:\s*
        (.*?)
        (?=
            \b(?:{FIELD_PATTERN})\s*:
            |\Z
        )
    """

    m = re.search(
        pattern,
        text,
        re.IGNORECASE | re.VERBOSE | re.DOTALL,
    )

    return " ".join(m.group(1).split()) if m else ""


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



# ========================== Field extractors ==========================
def extract_name(section: str) -> str:
    m = re.match(r"^(.*?)(?=\s+Address\s*:|\s+Phone\s*:|\Z)", section)
    return " ".join(m.group(1).split()) if m else ""

def parse_city_state_zip(text: str) -> tuple[str, str]:
    csz = _get(text, "City, State Zip")

    if not csz:
        return "", ""

    csz = " ".join(csz.split())

    m = re.search(
        r"^(.*?),\s+.*?\s+(\d{5}(?:-\d{4})?)",
        csz
    )

    if not m:
        return "", ""

    city = m.group(1).strip()
    zip_code = m.group(2)

    return city, zip_code


# ========================= GR detail helpers ==========================
def _strip_html(raw: str) -> str:
    raw = re.sub(
        r"<script.*?</script>",
        " ",
        raw,
        flags=re.DOTALL | re.IGNORECASE,
    )

    raw = re.sub(
        r"<style.*?</style>",
        " ",
        raw,
        flags=re.DOTALL | re.IGNORECASE,
    )

    text = re.sub(r"<[^>]+>", " ", raw)
    text = html_mod.unescape(text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()




def _get_in(section: str, label: str) -> str:
    """Same as _get but scoped to a pre-extracted section string."""
    return _get(section, label)


def parse_gr_page(raw_html: str) -> dict | None:
    """Parse a GR detail page into a flat dict. Returns None if blank/invalid."""
    text = _strip_html(raw_html)
    if "Group Name" not in text:
        return None

    group_name = _get(text, "Group Name")
    if not group_name:
        return None

    # Section-aware parsing — "Name:", "Phone:", "E-mail:" appear under both
    # Chair and Treasurer sections; extract each section's text block first.
    chair_m = re.search(
        r"Chair\s+Name\s*:\s*(.+?)(?=Treasurer\s+Name\b|Deputy\b|Type of Group|\Z)",
        text,
        re.IGNORECASE,
    )

    treas_m = re.search(
        r"Treasurer\s+Name\s*:\s*(.+?)(?=Deputy\b|Type of Group|\Z)",
        text,
        re.IGNORECASE,
    )
    chair_text = chair_m.group(1)  if chair_m  else ""
    treas_text = treas_m.group(1) if treas_m else ""

    city, zip_code = parse_city_state_zip(text)

    def clean_email(value: str) -> str:
        m = re.search(r"[\w.+-]+@[\w.-]+\.\w+", value)
        return m.group(0) if m else value.strip()

    return {
        "group_name": group_name,
        "abbreviation": _get(text, "Abbreviation"),
        "group_type": _get(text, "Group Type"),
        "purpose": _get(text, "Purpose"),
        "address": _get(text, "Group Mailing Address"),
        "city": city,
        "zip": zip_code,

        "chair_name": extract_name(chair_text),
        "chair_phone": _get_in(chair_text, "Phone"),

        "treasurer_name": extract_name(treas_text),
        "treasurer_phone": _get_in(treas_text, "Phone"),

        "chair_email": clean_email(_get_in(chair_text, "E-mail")),
        "treasurer_email": clean_email(_get_in(treas_text, "E-mail")),

        "election_year": _get(text, "Election Year"),
        "submission_date": clean_date(_get(text, "Submission Date")),
        "previously_registered": _get(text, "Previously Registered"),
    }


GR_INCREMENTAL_CUSHION = 1500  # IDs below current-year floor to re-check


def load_done_gr_ids() -> tuple[set[int], int | None]:
    """Return (done_ids, min_current_year_id).
    min_current_year_id is the lowest gr_id whose submission_date is in the
    current year, or None if no current-year records exist yet."""
    if not GR_DETAILS_PATH.exists():
        return set(), None
    current_year = str(datetime.today().year)
    done: set[int] = set()
    min_cy: int | None = None
    with open(GR_DETAILS_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            raw_id = row.get("gr_id", "")
            if not raw_id:
                continue
            gid = int(raw_id)
            done.add(gid)
            sd = row.get("submission_date", "")
            if sd.startswith(current_year):
                if min_cy is None or gid < min_cy:
                    min_cy = gid
    return done, min_cy


def _gr_sweep_floor(done_ids: set[int], min_cy: int | None) -> int:
    """Return the lowest ID to include in an incremental sweep."""
    if min_cy is not None:
        return max(MIN_GR_ID, min_cy - GR_INCREMENTAL_CUSHION)
    if done_ids:
        return max(MIN_GR_ID, max(done_ids) - GR_INCREMENTAL_CUSHION)
    return MIN_GR_ID


def download_gr_details(page, log, force: bool = False) -> tuple[int, int]:
    if force:
        done_ids, min_cy = set(), None
        floor = MIN_GR_ID
    else:
        done_ids, min_cy = load_done_gr_ids()
        floor = _gr_sweep_floor(done_ids, min_cy)

    log.info(f"GR details: probing from ID {floor} "
             f"({len(done_ids)} already done, stops after {MAX_CONSECUTIVE_BLANK} consecutive blanks)")

    if force and GR_DETAILS_PATH.exists():
        GR_DETAILS_PATH.unlink()

    write_header = force or not GR_DETAILS_PATH.exists()

    ok = err = consecutive_blank = 0
    t0 = time.perf_counter()

    with open(GR_DETAILS_PATH, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=GR_DETAILS_COLS,
            extrasaction="ignore",
        )

        if write_header:
            writer.writeheader()

        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(desc="  GR details", unit="id", dynamic_ncols=True, colour="green") as bar:
                gr_id = floor
                while True:
                    if gr_id in done_ids:
                        gr_id += 1
                        continue

                    url = GR_DETAIL_URL.format(id=gr_id)

                    try:
                        page.goto(url, timeout=30_000)
                        page.wait_for_load_state("load")

                        html = page.content()
                        text = page.locator("body").inner_text()

                        # Detect WAF block
                        if "Request Rejected" in text:
                            log.warning(
                                f"WAF rejection at GR ID {gr_id}; sleeping and retrying"
                            )
                            time.sleep(5)

                            page.goto(url, timeout=30_000)
                            page.wait_for_load_state("load")

                            html = page.content()
                            text = page.locator("body").inner_text()

                            if "Request Rejected" in text:
                                err += 1
                                gr_id += 1
                                bar.update(1)
                                continue

                        parsed = parse_gr_page(html)

                        if parsed is None:
                            consecutive_blank += 1

                            if consecutive_blank >= MAX_CONSECUTIVE_BLANK:
                                log.info(
                                    f"{MAX_CONSECUTIVE_BLANK} consecutive blanks — stopping at {gr_id}"
                                )
                                bar.update(1)
                                break

                            time.sleep(0.1)
                            gr_id += 1
                            bar.update(1)
                            continue

                        consecutive_blank = 0

                        parsed["gr_id"] = gr_id
                        writer.writerow(parsed)

                        bar.set_postfix_str(
                            parsed["group_name"][:45].ljust(45),
                            refresh=False,
                        )

                        ok += 1
                        time.sleep(0.2)

                    except Exception as e:
                        log.page_scrape_error(entity="group", page_id=gr_id, error=str(e))
                        err += 1
                        time.sleep(2)

                    gr_id += 1
                    bar.update(1)

    total_rows = sum(1 for _ in open(GR_DETAILS_PATH, encoding="utf-8")) - 1 if GR_DETAILS_PATH.exists() else 0
    log.page_scrape_complete(filename=str(GR_DETAILS_PATH), rows=total_rows,
                             duration_s=time.perf_counter() - t0, ok=ok, err=err)
    return ok, err


# ========================== CR page parsing ===========================
def _clean_na(val: str) -> str:
    """Return '' for APOC's n/a / 'Did Not Report' sentinel values."""
    v = " ".join(val.split())
    return "" if v.lower() in ("n/a", "na", "did not report", "none", "") else v


def parse_cr_page(raw_html: str) -> dict | None:
    """Parse a CR detail page. Returns None if blank/invalid."""
    text = _strip_html(raw_html)
    if "Candidate Display Name" not in text and "Candidate Legal First Name" not in text:
        return None

    candidate_first = _clean_na(_get(text, "Candidate Legal First Name"))
    candidate_last  = _clean_na(_get(text, "Candidate Legal Last Name"))
    if not (candidate_first or candidate_last):
        return None

    # CR pages have <h2> section headers (Candidate Information, Chair,
    # Treasurer, Deputy Treasurers, Bank Account) that become plain text after
    # HTML stripping and leak into _get() captures.  This local helper adds
    # those headers as extra stop markers (no colon required).
    _CR_SECTION_STOPS = r"Candidate\s+Information|Deputy\s+Treasurers|Bank\s+Account"

    def _get_cr(label: str) -> str:
        pattern = rf"""
            \b{re.escape(label)}
            \s*:\s*
            (.*?)
            (?=
                \b(?:{FIELD_PATTERN})\s*:
                |\b(?:{_CR_SECTION_STOPS})\b
                |\Z
            )
        """
        m = re.search(pattern, text, re.IGNORECASE | re.VERBOSE | re.DOTALL)
        return " ".join(m.group(1).split()) if m else ""

    # Treasurer section
    treas_m = re.search(
        r"Treasurer\s+Name\s*:\s*(.+?)(?=\bDeputy\b|\bBank\b|\bName\s+of\s+Bank\b|\Z)",
        text, re.IGNORECASE | re.DOTALL,
    )
    treas_text = treas_m.group(1) if treas_m else ""

    city, zip_code = parse_city_state_zip(text)

    def clean_email(value: str) -> str:
        m = re.search(r"[\w.+-]+@[\w.-]+\.\w+", value)
        return m.group(0) if m else value.strip()

    # "Previously Registered" is a checkbox on the CR form — no colon label.
    # Presence of the full parenthetical text in the stripped page means checked.
    prev_registered = (
        "Yes"
        if re.search(r"Previously\s+Registered\s*\(From\s+MJE\s+or\s+LOI\s+Form\)",
                     text, re.IGNORECASE)
        else ""
    )

    return {
        "candidate_display_name": _clean_na(_get(text, "Candidate Display Name")),
        "candidate_first":        candidate_first,
        "candidate_last":         candidate_last,
        "committee_name":         _clean_na(_get(text, "Campaign Committee Name")),
        "address":                _clean_na(_get(text, "Campaign Mailing Address")),
        "city":                   city,
        "zip":                    zip_code,
        "treasurer_name":         _clean_na(extract_name(treas_text)),
        "treasurer_phone":        _clean_na(_get_in(treas_text, "Phone")),
        "treasurer_email":        clean_email(_clean_na(_get_in(treas_text, "E-mail"))),
        "election_year":          _clean_na(_get_cr("Election Year")),
        "election":               _clean_na(_get_cr("Election")),
        "office_type":            _clean_na(_get_cr("Office Type")),
        "submission_date":        clean_date(_get_cr("Submission Date")),
        "previously_registered":  prev_registered,
    }


CR_INCREMENTAL_CUSHION = 1500


def load_done_cr_ids() -> tuple[set[int], int | None]:
    """Return (done_ids, min_current_year_id) based on submission_date."""
    if not CR_DETAILS_PATH.exists():
        return set(), None
    current_year = str(datetime.today().year)
    done: set[int] = set()
    min_cy: int | None = None
    with open(CR_DETAILS_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            raw_id = row.get("cr_id", "")
            if not raw_id:
                continue
            cid = int(raw_id)
            done.add(cid)
            sd = row.get("submission_date", "")
            if sd.startswith(current_year):
                if min_cy is None or cid < min_cy:
                    min_cy = cid
    return done, min_cy


def _cr_sweep_floor(done_ids: set[int], min_cy: int | None) -> int:
    if min_cy is not None:
        return max(MIN_CR_ID, min_cy - CR_INCREMENTAL_CUSHION)
    if done_ids:
        return max(MIN_CR_ID, max(done_ids) - CR_INCREMENTAL_CUSHION)
    return MIN_CR_ID


def download_cr_details(page, log, force: bool = False) -> tuple[int, int]:
    if force:
        done_ids, min_cy = set(), None
        floor = MIN_CR_ID
    else:
        done_ids, min_cy = load_done_cr_ids()
        floor = _cr_sweep_floor(done_ids, min_cy)

    log.info(f"CR details: probing from ID {floor} "
             f"({len(done_ids)} already done, stops after {MAX_CONSECUTIVE_CR_BLANK} consecutive blanks)")

    if force and CR_DETAILS_PATH.exists():
        CR_DETAILS_PATH.unlink()

    write_header = force or not CR_DETAILS_PATH.exists()
    ok = err = consecutive_blank = 0
    t0 = time.perf_counter()

    with open(CR_DETAILS_PATH, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CR_DETAILS_COLS, extrasaction="ignore")
        if write_header:
            writer.writeheader()

        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(desc="  CR details", unit="id", dynamic_ncols=True, colour="cyan") as bar:
                cr_id = floor
                while True:
                    if cr_id in done_ids:
                        cr_id += 1
                        continue

                    url = CR_DETAIL_URL.format(id=cr_id)
                    try:
                        page.goto(url, timeout=30_000)
                        page.wait_for_load_state("load")

                        html = page.content()
                        text = page.locator("body").inner_text()

                        if "Request Rejected" in text:
                            log.warning(f"WAF rejection at CR ID {cr_id}; retrying")
                            time.sleep(5)
                            page.goto(url, timeout=30_000)
                            page.wait_for_load_state("load")
                            html = page.content()
                            text = page.locator("body").inner_text()
                            if "Request Rejected" in text:
                                err += 1
                                cr_id += 1
                                bar.update(1)
                                continue

                        parsed = parse_cr_page(html)

                        if parsed is None:
                            consecutive_blank += 1
                            if consecutive_blank >= MAX_CONSECUTIVE_CR_BLANK:
                                log.info(f"{MAX_CONSECUTIVE_CR_BLANK} consecutive blanks — stopping at CR ID {cr_id}")
                                bar.update(1)
                                break
                            time.sleep(0.1)
                            cr_id += 1
                            bar.update(1)
                            continue

                        consecutive_blank = 0
                        parsed["cr_id"] = cr_id
                        writer.writerow(parsed)

                        label = (parsed["candidate_last"] + ", " + parsed["candidate_first"])[:45]
                        bar.set_postfix_str(label.ljust(45), refresh=False)

                        ok += 1
                        time.sleep(0.2)

                    except Exception as e:
                        log.page_scrape_error(entity="candidate", page_id=cr_id, error=str(e))
                        err += 1
                        time.sleep(2)

                    cr_id += 1
                    bar.update(1)

    total_rows = sum(1 for _ in open(CR_DETAILS_PATH, encoding="utf-8")) - 1 if CR_DETAILS_PATH.exists() else 0
    log.page_scrape_complete(filename=str(CR_DETAILS_PATH), rows=total_rows,
                             duration_s=time.perf_counter() - t0, ok=ok, err=err)
    return ok, err


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


def download_candidates(page, context, log) -> tuple[str, int] | None:
    page_url = PAGES["candidates"]
    page.goto(page_url, timeout=30_000)
    page.wait_for_load_state("networkidle")

    year_sel = page.locator("select[name*='ddlYear']")
    if year_sel.count():
        year_sel.select_option("All")

    search_btn = page.locator("input[value='Search']")
    if search_btn.count():
        page.click("input[value='Search']")
        page.wait_for_load_state("networkidle")

    body_text = page.locator("body").inner_text()
    if "No records" in body_text or "0 records" in body_text.lower():
        log.info("  candidates: no records found")
        return None

    page.click("input[value='Export']")

    csv_link = page.locator("a[id*='hlAllCSV']")
    try:
        csv_link.wait_for(timeout=15_000)
    except Exception:
        log.warning("  [!] Export dialog did not appear for candidates")
        return None

    filename = "CDCandidates_all.csv"
    out_path = RAW_DIR / filename

    with page.expect_download(timeout=180_000) as dl_info:
        csv_link.click()

    dl = dl_info.value
    dl.save_as(str(out_path))

    text      = out_path.read_text(encoding="utf-8", errors="replace")
    row_count = text.count("\n") - 1
    return filename, row_count


def download_year(page, context, relation_type: str, year: str, log) -> tuple[str, int] | None:
    page_url = PAGES[relation_type]
    page.goto(page_url, timeout=30_000)
    page.wait_for_load_state("networkidle")

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

    body_text = page.locator("body").inner_text()
    if "No records" in body_text or "0 records" in body_text.lower():
        log.debug(f"  {relation_type} {year}: no records")
        return None

    filename = f"{STEMS[relation_type]}_{year}.csv"
    out_path = RAW_DIR / filename
    csv_link = page.locator("a[id*='hlAllCSV']")

    with page.expect_download(timeout=180_000) as dl_info:
        page.click("input[value='Export']")
        try:
            csv_link.wait_for(timeout=8_000)
            csv_link.click()
        except Exception:
            pass

    dl = dl_info.value
    dl.save_as(str(out_path))

    text      = out_path.read_text(encoding="utf-8", errors="replace")
    row_count = text.count("\n") - 1
    return filename, row_count


# ============================ orchestrator ============================
def run(force: bool = False, entities: bool = False, transactions: bool = False):
    """Orchestrate download of transaction CSVs and/or candidate/group entities."""
    log = get_logger("alaska", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Alaska scraper")
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("[!] Playwright not installed. Run: pip install playwright && playwright install chromium")
        log._emit("scrape_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, pages_ok=0, pages_err=0,
                  error="playwright not installed")
        return

    # Resolve scope
    do_both         = not entities and not transactions
    do_transactions = transactions or do_both
    do_entities     = entities     or do_both

    files_ok = files_err = pages_ok = pages_err = 0
    current_year = str(datetime.today().year)

    # Scoped manifest clearing
    if force:
        if do_both:
            if MANIFEST.exists():
                MANIFEST.unlink()
            done, has_data = set(), set()
        elif do_transactions:
            strip_manifest(lambda r: r["relation_type"] not in TRANSACTION_RELATIONS)
            done, has_data = load_manifest()
        else:  # do_entities only
            strip_manifest(lambda r: r["relation_type"] not in ENTITY_RELATIONS)
            done, has_data = load_manifest()
    else:
        done, has_data = load_manifest()

    pages_to_run = (
        set(PAGES.keys())   if do_both         else
        TRANSACTION_RELATIONS if do_transactions else
        ENTITY_RELATIONS
    )

    try:
        # Playwright: transaction CSVs + candidate/group exports
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(accept_downloads=True)
            page    = context.new_page()

            for relation_type, page_url in PAGES.items():
                if relation_type not in pages_to_run:
                    continue

                log.info(f"\nAlaska {relation_type}:")

                # Candidates
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

                # Transactional tables + groups
                page.goto(page_url, timeout=30_000)
                page.wait_for_load_state("networkidle")

                years = get_available_years(page)
                if not years:
                    log.warning(f"  [!] Could not read year dropdown for {relation_type} — skipping")
                    continue

                log.info(f"  Available years: {years[0]}–{years[-1]} ({len(years)} total)")

                for year in years:
                    key           = (relation_type, year)
                    expected_stem = f"{STEMS[relation_type]}_{year}.csv"
                    expected_file = RAW_DIR / expected_stem
                    already_done  = key in done or (expected_file.exists() and
                                                    expected_file.stat().st_size > 0)

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
                        continue

                    if result is None:
                        continue

                    filename, row_count = result
                    size = (RAW_DIR / filename).stat().st_size
                    log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                         duration_s=time.perf_counter() - t_file)
                    files_ok += 1
                    upsert_manifest({
                        "relation_type": relation_type,
                        "year":          year,
                        "filename":      filename,
                        "row_count":     row_count,
                    })
                    done.add(key)
                    time.sleep(1)

            browser.close()

        # GR + CR detail scrape (entities only)
        if do_entities:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context(accept_downloads=True)
                page    = context.new_page()

                page.goto(PAGES["groups"])
                page.wait_for_load_state("networkidle")

                p_ok, p_err = download_gr_details(page, log, force=force)
                pages_ok  += p_ok
                pages_err += p_err

                p_ok, p_err = download_cr_details(page, log, force=force)
                pages_ok  += p_ok
                pages_err += p_err

                browser.close()

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

# ====== CLI ==================================
if __name__ == "__main__":
    # flag semantics
    # --------------
    # (no flags)              current-year transactions + any missing entities
    # --transactions          transactions only (current-year always, past years if missing)
    # --entities              entities only (GR/CR details incremental, candidates if missing)
    # --force                 force-refresh everything
    # --force --transactions  force-refresh all transaction years
    # --force --entities      force-refresh all entities (GR/CR/candidates/groups)
    import argparse
    ap = argparse.ArgumentParser(
        description="Download Alaska APOC campaign finance data. "
                    "Fetches transaction CSVs and/or candidate/group registration details."
    )
    ap.add_argument("--force",        action="store_true",
                    help="force re-download (scope: all, or --transactions/--entities)")
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (GR/CR details, candidates, groups)")
    args = ap.parse_args()
    try:
        run(force=args.force, entities=args.entities, transactions=args.transactions)
    except KeyboardInterrupt:
        sys.exit(130)
