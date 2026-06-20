"""
scrapers/delaware.py — Download Delaware campaign finance data.

All data comes from the CFRS (Campaign Finance Reporting System):
  cfrs.elections.delaware.gov

Transactions (Playwright):
  Contributions and expenditures, one CSV per year via year-filter dropdown.

Entities (Playwright, two-phase):
  Phase 1 — scrape_committee_links():
    For each committee type, navigate ViewCommittees, click Search, parse the HTML
    results grid to extract (memberID, CF_ID, ShowReview URL) per committee.
    Saves de_committee_links.csv. Handles grid pagination.

  Phase 2 — scrape_committee_details():
    For each memberID not yet scraped, navigate to its ShowReview page and extract
    full detail: committee name, status, office, district, party, candidate name,
    treasurer, address, purpose. Saves de_committee_details.csv.

    ShowReview URL pattern (ftype and memVersID vary per committee, extracted from link):
      /Public/ShowReview?memberID={id}&memVersID={v}&cTypeCode={code}&ftype={ft}&isPublic=true

    cTypeCode mapping:
      01 = Candidate Committee
      02 = Political Action Committee
      03 = Political Committee
      04 = 3rd Party Advertiser
      05 = Certification of Intention

No data is fetched from elections.delaware.gov — CFRS is the single source for
both transactions and entities.

Manifest tracks by (relation_type, key):
  contributions / expenditures  → key = year
  committee_links               → key = ctype_code
  committee_detail              → key = member_id
"""

import csv
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Delaware" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Delaware" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "key", "filename", "downloaded_at", "row_count"]

# ============================= constants ==============================

CFRS_BASE = "https://cfrs.elections.delaware.gov"

CFRS_PAGES = {
    "contributions": f"{CFRS_BASE}/Public/ViewReceipts?theme=vista",
    "expenditures":  f"{CFRS_BASE}/Public/ViewExpenses?theme=vista",
}

TRANSACTION_RELATIONS = {"contributions", "expenditures"}
ENTITY_RELATIONS      = {"committee_links", "committee_detail"}

CFRS_COMMITTEES_URL = f"{CFRS_BASE}/Public/ViewCommittees"

CFRS_EXPORT_URL = f"{CFRS_BASE}/Public/ExportCSVNew?page=1&orderBy=~&filter=~&Grid-size=15&theme=vista"

CFRS_START_YEAR = 2000

# cTypeCode → (dropdown label, safe filename key)
COMMITTEE_TYPES = {
    "01": ("Candidate Committee",        "candidate_committee"),
    "02": ("Political Action Committee", "political_action_committee"),
    "03": ("Political Committee",        "political_committee"),
    "04": ("3rd Party Advertiser",       "third_party_advertiser"),
    # "05" (Certification of Intention) excluded — these filers raise/spend <$5k
    # and are not required to file reports, so they have no transaction data.
}

COMMITTEE_LINKS_PATH   = RAW_DIR / "de_committee_links.csv"
COMMITTEE_DETAILS_PATH = RAW_DIR / "de_committee_details.csv"

COMMITTEE_LINKS_COLS = [
    "member_id", "cf_id", "ctype_code", "ctype_label", "show_review_url",
]

COMMITTEE_DETAIL_COLS = [
    "member_id", "cf_id", "ctype_code", "ctype_label",
    "committee_name", "other_name", "status",
    "established_date", "end_date", "purpose",
    "email", "web_address",
    "physical_address", "physical_city", "physical_state", "physical_zip",
    # Election participation (Candidate Committees)
    "office_type", "county", "office_sought", "district", "party",
    # Candidate info (Candidate Committees only)
    "candidate_name", "candidate_email", "candidate_phone", "candidate_address",
    # Treasurer
    "treasurer_name", "treasurer_email", "treasurer_phone", "treasurer_address",
    "scraped_at",
]


# ========================== manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    done: set[tuple[str, str]] = set()
    if not MANIFEST.exists():
        return done
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["key"]))
    return done


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
                        and r["key"] == record["key"])
            ]
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)


# ====================== CFRS Playwright helpers =======================

def get_available_years(page) -> list[str]:
    for selector in [
        "select[id*='Year']", "select[id*='year']",
        "select[name*='Year']", "select[name*='year']",
        "select[id*='FilingYear']", "select[id*='ReportYear']",
    ]:
        sel = page.locator(selector)
        if sel.count():
            options = sel.locator("option").all()
            years = [opt.get_attribute("value") or "" for opt in options]
            return sorted(set(y for y in years if y and y.isdigit() and int(y) >= 1990))
    return []


def click_search(page) -> None:
    for selector in [
        "input[value='Search']", "input[value='View']",
        "input[id*='btnSearch']", "input[id*='Search']",
        "button:has-text('Search')", "input[type='submit']",
    ]:
        btn = page.locator(selector)
        if btn.count():
            btn.first.click()
            # Use load instead of networkidle — large result sets can keep
            # background requests open indefinitely, causing networkidle to timeout.
            try:
                page.wait_for_load_state("load", timeout=60_000)
            except Exception:
                pass
            time.sleep(0.5)
            return


def select_committee_type(page, label: str) -> bool:
    for selector in [
        "select[id*='CommitteeType']", "select[id*='committeeType']",
        "select[name*='CommitteeType']", "select[id*='Type']",
    ]:
        sel = page.locator(selector)
        if sel.count():
            sel.first.select_option(label=label)
            return True
    return False


def trigger_export(page) -> None:
    """Navigate to the CSV export endpoint using the link's href."""
    csv_link = page.locator("a[id='export']").first
    if csv_link.count():
        href = csv_link.get_attribute("href") or ""
        if href:
            full_url = (CFRS_BASE + href) if href.startswith("/") else href
            page.evaluate(f'window.location.href = "{full_url}"')
            return
    page.evaluate(f'window.location.href = "{CFRS_EXPORT_URL}"')


# ====================== transaction downloads ========================

NO_DATA_PHRASES = ("No records", "no records found", "0 records", "No data")


def download_transactions(page, context, log, force: bool = False,
                          current_year: str = "",
                          start_year: int | None = None,
                          end_year: int | None = None,
                          contributions: bool = False,
                          expenditures: bool = False) -> tuple[int, int]:
    """Download contributions and expenditures year-by-year from CFRS."""
    year_range_explicit = start_year is not None or end_year is not None

    if contributions and not expenditures:
        active_pages = {k: v for k, v in CFRS_PAGES.items() if k == "contributions"}
    elif expenditures and not contributions:
        active_pages = {k: v for k, v in CFRS_PAGES.items() if k == "expenditures"}
    else:
        active_pages = CFRS_PAGES

    done = load_manifest()
    ok = err = 0

    for relation_type, page_url in active_pages.items():
        log.info(f"\nDelaware {relation_type}:")

        try:
            page.goto(page_url, timeout=50_000)
            page.wait_for_load_state("networkidle")
        except Exception as e:
            log.warning(f"  [!] Could not load {page_url}: {e}")
            err += 1
            continue

        years = get_available_years(page)
        if not years:
            log.warning(f"  [!] Could not read year dropdown for {relation_type} — skipping")
            err += 1
            continue

        years = [y for y in years if int(y) >= CFRS_START_YEAR]
        if start_year is not None:
            years = [y for y in years if int(y) >= start_year]
        if end_year is not None:
            years = [y for y in years if int(y) <= end_year]
        log.info(f"  Available years: {years[0]}–{years[-1]} ({len(years)} total)")

        for year in years:
            key          = (relation_type, year)
            filename     = f"de_{relation_type}_{year}.csv"
            out_path     = RAW_DIR / filename
            already_done = key in done or (out_path.exists() and out_path.stat().st_size > 0)

            if already_done and year != current_year and not force and not year_range_explicit:
                log.file_download_skip(filename=filename)
                continue

            log.file_download_start(filename=filename)
            t0 = time.perf_counter()

            try:
                page.goto(page_url, timeout=50_000)
                page.wait_for_load_state("networkidle")

                for selector in ["select[id*='Year']", "select[id*='year']",
                                 "select[name*='Year']", "select[name*='year']",
                                 "select[id*='FilingYear']", "select[id*='ReportYear']"]:
                    sel = page.locator(selector)
                    if sel.count():
                        sel.first.select_option(year)
                        break

                click_search(page)
                time.sleep(0.5)

                body_text = page.locator("body").inner_text()
                if any(p in body_text for p in NO_DATA_PHRASES):
                    log.debug(f"  {relation_type} {year}: no records")
                    continue

                with page.expect_download(timeout=120_000) as dl_info:
                    trigger_export(page)
                dl = dl_info.value
                dl.save_as(str(out_path))

            except Exception as e:
                log.file_download_error(filename=filename, error=str(e))
                err += 1
                continue

            if not out_path.exists() or out_path.stat().st_size == 0:
                log.file_download_error(filename=filename, error="empty download")
                err += 1
                continue

            try:
                row_count = max(0, out_path.read_text(encoding="utf-8",
                                                       errors="replace").count("\n") - 1)
            except Exception:
                row_count = 0

            log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                                 rows=row_count, duration_s=time.perf_counter() - t0,
                                 year=year)
            ok += 1
            upsert_manifest({"relation_type": relation_type, "key": year,
                             "filename": filename,
                             "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                             "row_count": row_count})
            done.add(key)
            time.sleep(1)

    return ok, err


# ======================= entity downloads — phase 1 ==================

def _parse_links_from_grid(page, ctype_code: str, ctype_label: str) -> list[dict]:
    """Extract (memberID, CF_ID, ShowReview URL) from the current ViewCommittees results page.

    The grid renders a table where each committee name is a link to ShowReview.
    The CF_ID column (2nd column, 0-indexed) contains the committee ID.
    The ShowReview href encodes memberID, memVersID, cTypeCode, and ftype — we
    extract the full href rather than constructing it, since ftype and memVersID
    vary per committee.
    """
    html  = page.content()
    soup  = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []

    # Find the data grid table — it contains ShowReview links
    for table in soup.find_all("table"):
        links = table.find_all("a", href=re.compile(r"ShowReview", re.IGNORECASE))
        if not links:
            continue

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # Find the ShowReview link in this row
            link_tag = None
            for td in tds:
                a = td.find("a", href=re.compile(r"ShowReview", re.IGNORECASE))
                if a:
                    link_tag = a
                    break

            if not link_tag:
                continue

            href = link_tag.get("href", "")
            full_url = (CFRS_BASE + href) if href.startswith("/") else href

            m_id = re.search(r"memberID=(\d+)", href, re.IGNORECASE)
            member_id = m_id.group(1) if m_id else ""

            # CF_ID is typically the 2nd column (index 1), looks like "01004350"
            cf_id = ""
            if len(tds) > 1:
                candidate = tds[1].get_text(strip=True)
                if re.match(r"^0[1-5]\d+$", candidate) or re.match(r"^\d{6,10}$", candidate):
                    cf_id = candidate

            if member_id:
                rows.append({
                    "member_id":      member_id,
                    "cf_id":          cf_id,
                    "ctype_code":     ctype_code,
                    "ctype_label":    ctype_label,
                    "show_review_url": full_url,
                })

        break   # found the right table — stop searching

    return rows


def _maximize_page_size(page) -> None:
    """Set the Kendo grid page-size dropdown to its largest available value.
    Dramatically reduces pagination (e.g. 630 pages at 5/page → ~13 at 250/page)."""
    for selector in [
        "select.t-page-size-select",
        "select[aria-label*='page size']",
        "select[aria-label*='Page size']",
        ".t-pager select",
    ]:
        ps = page.locator(selector)
        if ps.count():
            options = ps.locator("option").all()
            if options:
                # Pick the largest numeric value, or "All" if available
                best_val  = None
                best_num  = 0
                for opt in options:
                    val  = opt.get_attribute("value") or ""
                    text = (opt.inner_text() or "").strip().lower()
                    if text == "all":
                        best_val = val
                        break
                    try:
                        n = int(val)
                        if n > best_num:
                            best_num = n
                            best_val = val
                    except ValueError:
                        pass
                if best_val:
                    ps.select_option(value=best_val)
                    page.wait_for_load_state("networkidle")
                    time.sleep(0.3)
            return


def _pager_position(page) -> tuple[int, int]:
    """Parse 'Displaying items X - Y of Z' from the grid pager.
    Returns (current_end, total). Returns (0, 0) if not found."""
    pager = page.locator(".t-grid-pager, .t-pager")
    if pager.count():
        text = pager.first.inner_text()
        m = re.search(
            r"Displaying items\s+(\d[\d,]*)\s*[-–]\s*(\d[\d,]*)\s+of\s+(\d[\d,]*)",
            text, re.IGNORECASE
        )
        if m:
            end   = int(m.group(2).replace(",", ""))
            total = int(m.group(3).replace(",", ""))
            return end, total
    return 0, 0


def _next_page(page, log=None) -> bool:
    """Click the Kendo grid Next Page button if enabled.
    The next button is a <span class="t-icon t-arrow-next"> inside an <a>.
    The parent <a> gets class t-state-disabled on the last page.
    Returns True if clicked, False if on last page or not found."""
    btn = page.locator("a:has(span.t-arrow-next):not(.t-state-disabled)")
    if btn.count():
        btn.first.click()
        try:
            page.wait_for_load_state("load", timeout=60_000)
        except Exception:
            pass
        time.sleep(0.3)
        return True

    # Fallback: try clicking the span directly
    span = page.locator("span.t-icon.t-arrow-next")
    if span.count():
        parent = span.first.locator("xpath=..")
        if "t-state-disabled" not in (parent.get_attribute("class") or ""):
            span.first.click()
            try:
                page.wait_for_load_state("load", timeout=60_000)
            except Exception:
                pass
            time.sleep(0.3)
            return True

    return False


def scrape_committee_links(page, log, force: bool = False) -> int:
    """Phase 1: for each committee type, search ViewCommittees and parse the HTML
    results grid to extract memberID, CF_ID, and ShowReview URL per committee.
    Appends to de_committee_links.csv; manifest key is ctype_code.
    Returns total link count across all types."""
    done = load_manifest()

    # Load existing links to avoid re-scraping types already done
    existing_links: list[dict] = []
    if COMMITTEE_LINKS_PATH.exists() and not force:
        with open(COMMITTEE_LINKS_PATH, newline="", encoding="utf-8") as f:
            existing_links = list(csv.DictReader(f))

    existing_member_ids = {r["member_id"] for r in existing_links}
    all_links = list(existing_links)

    log.info("\nDelaware committee links (phase 1):")

    for ctype_code, (ctype_label, _) in COMMITTEE_TYPES.items():
        key = ("committee_links", ctype_code)

        if key in done and not force:
            count = sum(1 for r in existing_links if r["ctype_code"] == ctype_code)
            log.file_download_skip(filename=f"links/{ctype_label} ({count} cached)")
            continue

        log.info(f"  Scraping links: {ctype_label}")
        t0 = time.perf_counter()

        try:
            page.goto(CFRS_COMMITTEES_URL, timeout=50_000)
            page.wait_for_load_state("networkidle")

            if not select_committee_type(page, ctype_label):
                sel = page.locator("select").first
                if sel.count():
                    sel.select_option(label=ctype_label)

            # Clear status filter so we get ALL committees (Active + Inactive + Closed).
            # Wrapped in try/except — if the selector doesn't exist or the option
            # isn't selectable, we just proceed without filtering by status.
            try:
                for selector in [
                    "select[id*='Status']", "select[name*='Status']",
                    "select[id*='status']", "select[name*='status']",
                ]:
                    sel = page.locator(selector)
                    if sel.count():
                        sel.first.select_option(index=0)    # first option = all/blank
                        time.sleep(0.2)
                        break
            except Exception:
                pass

            click_search(page)
            time.sleep(0.5)

            body_text = page.locator("body").inner_text()
            if any(p in body_text for p in NO_DATA_PHRASES):
                log.debug(f"  {ctype_label}: no records")
                upsert_manifest({"relation_type": "committee_links", "key": ctype_code,
                                 "filename": "de_committee_links.csv",
                                 "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                                 "row_count": 0})
                done.add(key)
                continue

            # Maximize page size to minimize pagination round-trips
            _maximize_page_size(page)

            new_links: list[dict] = []
            page_num = 1
            while True:
                current_end, total = _pager_position(page)
                page_links = _parse_links_from_grid(page, ctype_code, ctype_label)
                new_this_page = 0
                for link in page_links:
                    if link["member_id"] not in existing_member_ids:
                        new_links.append(link)
                        existing_member_ids.add(link["member_id"])
                        new_this_page += 1

                # Stop when pager says we've reached the last item
                if total > 0 and current_end >= total:
                    break

                # Fallback: stop if Next button can't be clicked
                if not _next_page(page, log=log):
                    break

                page_num += 1

            all_links.extend(new_links)
            duration = round(time.perf_counter() - t0, 1)
            log.info(f"  {ctype_label}: {len(new_links)} new links in {duration}s")

            # Write incrementally after each type so Ctrl+C doesn't lose data
            with open(COMMITTEE_LINKS_PATH, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COMMITTEE_LINKS_COLS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(all_links)

            upsert_manifest({"relation_type": "committee_links", "key": ctype_code,
                             "filename": "de_committee_links.csv",
                             "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                             "row_count": len(new_links)})
            done.add(key)

        except Exception as e:
            log.warning(f"  [!] Failed to scrape links for {ctype_label}: {e}")

    total = len(all_links)
    log.info(f"  Total links saved: {total:,}")
    return total


# ======================= entity downloads — phase 2 ==================

# Field labels that always appear with a colon on the page: "Label: value".
# In the stop pattern these require a colon, so "State: Delaware" stops
# extraction but "State of Delaware" in body text does not.
_LABEL_STOPS = (
    "CF ID", "CFID", "Committee Name", "Other Name", "Short Name",
    "Status", "Established Date", "Date of Origination", "End Date",
    "Purpose", "Party Affiliation",
    "Email", "Fax", "Web Address",
    "Address Line 1", "Address Line 2", "Address Line",
    "City", "State", "Zip",
    "Name of Party if entire ticket is supported",
)

# Section headers that appear without a colon. These are matched as plain
# text (no colon required) since they act as dividers between sections.
_HEADER_STOPS = (
    "Contact Information",
    "Physical Address", "Residence Address", "Organization Street Address",
    "Mailing Address",
    "Affiliated Candidate Information",
    "Election Participation",
    "Candidate Information", "Treasurer Information",
    "Additional Contacts", "Documents", "Violations", "Filing Information",
    "Displaying page",
)

# Pre-compiled stop pattern: labeled fields require a trailing colon so that
# common English words (State, City, Party, etc.) don't fire inside body text.
_STOP_PAT = "|".join(
    [re.escape(s) + r"\s*:" for s in _LABEL_STOPS] +
    [re.escape(s)           for s in _HEADER_STOPS]
)


def _field(label: str, text: str) -> str:
    """Extract value after 'label:' stopping at any known page label or section
    header.  The colon after the label is required to avoid false matches on
    common words that appear in body text (State, City, Party, etc.)."""
    m = re.search(
        rf"(?:^|\s){re.escape(label)}\s*:\s*(.*?)(?={_STOP_PAT}|\Z)",
        text, re.IGNORECASE | re.DOTALL,
    )
    return m.group(1).strip() if m else ""


def _section(header: str, text: str, stop_headers: tuple[str, ...]) -> str:
    """Return the text of a named section, clipped at the first stop header."""
    start = text.lower().find(header.lower())
    if start == -1:
        return ""
    section = text[start + len(header):]
    stop_pat = "|".join(re.escape(h) for h in stop_headers)
    m = re.search(stop_pat, section, re.IGNORECASE)
    return section[:m.start()].strip() if m else section.strip()


def _extract_table_row(data: str) -> dict[str, str]:
    """Pull Name / Email / Address / Phone from a collapsed table data string.

    Email is the primary anchor: everything before it is the name, everything
    after (up to the first date or phone) is the address.

    When there is no email (common for older registrations), falls back to an
    address-start heuristic: "PO Box" or a street-number prefix splits the
    name from the address.
    """
    # Strip table column-header words that precede the actual data rows
    data = re.sub(
        r"(?i)\b(?:Name|Email|Mailing\s+Address|Office\s+Phone|"
        r"Start\s+Date|End\s+Date|Status)\b",
        " ", data,
    )
    data = re.sub(r"\s+", " ", data).strip()

    def _after_cutoff(after: str) -> tuple[str, str]:
        """Extract (address, phone) from text that follows the name/email anchor.
        Searches directly on `after` to avoid offset-calculation bugs."""
        date_m  = re.search(r"\b\d{2}/\d{2}/\d{4}\b", after)
        phone_m = re.search(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", after)
        kw_m    = re.search(
            r"(?i)\b(?:Displaying|Additional|Filing|Documents|Violations)\b", after
        )
        cutoffs = [m.start() for m in [date_m, phone_m, kw_m] if m]
        address = after[:min(cutoffs)].strip() if cutoffs else after.strip()
        phone   = phone_m.group(0) if phone_m else ""
        return address, phone

    # ── Primary: email anchor ───────────────────────────────────────────
    email_m = re.search(r"[\w.+\-]+@[\w\-]+\.[\w.]+", data)
    if email_m:
        name    = data[:email_m.start()].strip()
        email   = email_m.group(0)
        after   = data[email_m.end():]          # no strip — keeps index clean
        address, phone = _after_cutoff(after)
        return {"name": name, "email": email, "address": address, "phone": phone}

    # ── Fallback: address-start heuristic ──────────────────────────────
    # "PO Box …" or street number ("217 Main St.") splits name from address
    addr_m = re.search(
        r"(?i)(?:"
        r"P\.?\s*O\.?\s+Box"
        r"|(?<!\d)\d+\s+(?:[NSEW]\s+)?[A-Za-z]+"
        r"(?:\s+(?:St|Ave|Rd|Dr|Blvd|Ln|Way|Ct|Pl|Pike|Hwy|Route|Rte|Pkwy|Loop|Ter)\.?)?"
        r")",
        data,
    )
    if addr_m:
        name    = data[:addr_m.start()].strip()
        after   = data[addr_m.start():]
        address, phone = _after_cutoff(after)
        return {"name": name, "email": "", "address": address, "phone": phone}

    # ── Last resort ─────────────────────────────────────────────────────
    date_m  = re.search(r"\b\d{2}/\d{2}/\d{4}\b", data)
    phone_m = re.search(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", data)
    if date_m:
        return {"name": data[:date_m.start()].strip(), "email": "", "address": "", "phone": ""}
    if phone_m:
        return {"name": data[:phone_m.start()].strip(), "email": "", "address": "", "phone": phone_m.group(0)}
    return {"name": data[:80].strip(), "email": "", "address": "", "phone": ""}


def _parse_show_review(html: str, member_id: str, ctype_code: str, ctype_label: str) -> dict:
    """Parse a rendered ShowReview page into a flat dict.

    Collapses whitespace to a single line, then uses a comprehensive list of
    known page labels as lookahead stops so each field() extraction is cleanly
    bounded.  Treasurer and candidate table rows are extracted by email anchor
    rather than by splitlines() (which fails on the collapsed text).
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    # ── Core scalar fields ─────────────────────────────────────────────
    cf_id            = _field("CF ID", text) or _field("CFID", text)
    committee_name   = _field("Committee Name", text)
    other_name       = _field("Other Name", text) or _field("Short Name", text)
    status           = _field("Status", text)
    # "Date of Origination" is used by older registrations in place of "Established Date"
    established_date = _field("Established Date", text) or _field("Date of Origination", text)
    end_date         = _field("End Date", text)
    purpose          = _field("Purpose", text)
    # Strip "Fax:" suffix — some pages show "Email: addr Fax: number" on one line
    email            = re.sub(r"\s+Fax:.*$", "", _field("Email", text), flags=re.IGNORECASE).strip()
    web_address      = _field("Web Address", text)

    # Physical address — prefer "Physical Address" section; fall back to
    # "Residence Address" (Political Committees) or "Organization Street Address"
    # (3rd Party Advertisers).
    for addr_header in ("Physical Address", "Residence Address", "Organization Street Address"):
        phys_section = _section(addr_header, text,
                                ("Mailing Address", "Election Participation",
                                 "Candidate Information", "Treasurer Information",
                                 "Contact Information", "Displaying page"))
        if phys_section:
            break

    # Address Line 1 is the first real address line inside the section
    addr_m = re.search(r"Address Line\s*(?:1)?\s*:?\s*(.*?)(?=Address Line|City|$)",
                       phys_section, re.IGNORECASE | re.DOTALL)
    physical_address = addr_m.group(1).strip() if addr_m else ""
    physical_city    = _field("City",  phys_section) or _field("City",  text)
    physical_state   = _field("State", phys_section) or _field("State", text)
    physical_zip     = _field("Zip",   phys_section) or _field("Zip",   text)

    # ── Election participation (Candidate Committees only) ──────────────
    office_type = county = office_sought = district = party = ""
    if ctype_code == "01":
        ep = _section("Election Participation", text,
                      ("Candidate Information", "Treasurer Information",
                       "Additional Contacts", "Filing Information",
                       "Documents", "Violations", "Displaying page"))
        if ep:
            # Skip the column-header row: "Office Type County/Municipality
            # Office Sought District Party Affiliation"
            header_m = re.search(
                r"Office Type\s+County/Municipality\s+Office Sought\s+District\s+Party Affiliation",
                ep, re.IGNORECASE,
            )
            data_ep = ep[header_m.end():].strip() if header_m else ep

            # Data row pattern: [office_type] [county] [office_sought] [district] [party]
            # "State Office" / "County Office" / "Municipal Office" / "School Board"
            m = re.match(
                r"(State Office|County Office|Municipal Office|School Board)"
                r"\s+(.*?)\s+"
                r"(State (?:Senator|Representative)|Governor|Lieutenant Governor|"
                r"Insurance Commissioner|Attorney General|State Treasurer|"
                r"Auditor|Comptroller|[A-Z][a-zA-Z\- ]+)"
                r"\s+(District \d+|At Large|Na)\s+"
                r"([A-Z][a-zA-Z ]+)",
                data_ep,
            )
            if m:
                office_type   = m.group(1).strip()
                county        = m.group(2).strip()
                office_sought = m.group(3).strip()
                district      = m.group(4).strip()
                party         = m.group(5).strip()
            else:
                # Fallback: split on 2+ spaces
                parts = re.split(r"\s{2,}", data_ep)
                if len(parts) >= 5:
                    office_type, county, office_sought, district, party = (
                        parts[0], parts[1], parts[2], parts[3], parts[4]
                    )

    # ── Candidate info (Candidate Committees only) ──────────────────────
    candidate_name = candidate_email = candidate_phone = candidate_address = ""
    if ctype_code == "01":
        ci = _section("Candidate Information", text,
                      ("Treasurer Information", "Additional Contacts",
                       "Filing Information", "Documents", "Violations",
                       "Displaying page"))
        if ci:
            header_m = re.search(
                r"Name\s+Email\s+Mailing Address\s+(?:Office\s+)?Phone",
                ci, re.IGNORECASE,
            )
            data_ci = ci[header_m.end():].strip() if header_m else ci
            row = _extract_table_row(data_ci)
            candidate_name    = row["name"]
            candidate_email   = row["email"]
            candidate_address = row["address"]
            candidate_phone   = row["phone"]

    # ── Treasurer ──────────────────────────────────────────────────────
    treasurer_name = treasurer_email = treasurer_phone = treasurer_address = ""
    ti = _section("Treasurer Information", text,
                  ("Additional Contacts", "Filing Information",
                   "Documents", "Violations", "Displaying page"))
    if ti:
        header_m = re.search(
            r"Name\s+Email\s+Mailing Address\s+Start Date\s+End Date\s+Status",
            ti, re.IGNORECASE,
        )
        data_ti = ti[header_m.end():].strip() if header_m else ti
        row = _extract_table_row(data_ti)
        treasurer_name    = row["name"]
        treasurer_email   = row["email"]
        treasurer_address = row["address"]
        treasurer_phone   = row["phone"]

    return {
        "member_id":          member_id,
        "cf_id":              cf_id,
        "ctype_code":         ctype_code,
        "ctype_label":        ctype_label,
        "committee_name":     committee_name,
        "other_name":         other_name,
        "status":             status,
        "established_date":   established_date,
        "end_date":           end_date,
        "purpose":            purpose,
        "email":              email,
        "web_address":        web_address,
        "physical_address":   physical_address,
        "physical_city":      physical_city,
        "physical_state":     physical_state,
        "physical_zip":       physical_zip,
        "office_type":        office_type,
        "county":             county,
        "office_sought":      office_sought,
        "district":           district,
        "party":              party,
        "candidate_name":     candidate_name,
        "candidate_email":    candidate_email,
        "candidate_phone":    candidate_phone,
        "candidate_address":  candidate_address,
        "treasurer_name":     treasurer_name,
        "treasurer_email":    treasurer_email,
        "treasurer_phone":    treasurer_phone,
        "treasurer_address":  treasurer_address,
        "scraped_at":         datetime.today().strftime("%Y-%m-%d"),
    }


def scrape_committee_details(page, log, force: bool = False) -> tuple[int, int]:
    """Phase 2: for each memberID in de_committee_links.csv not yet in manifest,
    navigate to ShowReview and scrape full detail into de_committee_details.csv.
    Returns (ok, err) counts."""
    done = load_manifest()
    ok = err = 0

    if not COMMITTEE_LINKS_PATH.exists():
        log.warning("  [!] de_committee_links.csv not found — run phase 1 first")
        return 0, 1

    with open(COMMITTEE_LINKS_PATH, newline="", encoding="utf-8") as f:
        links = list(csv.DictReader(f))

    if not links:
        log.warning("  [!] No committee links found")
        return 0, 0

    to_scrape = [
        lnk for lnk in links
        if force or ("committee_detail", lnk["member_id"]) not in done
    ]

    log.info(f"\nDelaware committee details (phase 2):")
    log.info(f"  {len(links):,} total links, {len(to_scrape):,} to scrape")

    # Count rows already in the file so the final total is accurate
    existing_count = 0
    if COMMITTEE_DETAILS_PATH.exists() and not force:
        with open(COMMITTEE_DETAILS_PATH, newline="", encoding="utf-8") as f:
            existing_count = sum(1 for _ in csv.DictReader(f))

    # Open for incremental append; write header only when creating fresh
    write_header = force or not COMMITTEE_DETAILS_PATH.exists()
    if force and COMMITTEE_DETAILS_PATH.exists():
        COMMITTEE_DETAILS_PATH.unlink()

    out_f = open(COMMITTEE_DETAILS_PATH, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=COMMITTEE_DETAIL_COLS,
                            extrasaction="ignore", restval="")
    if write_header:
        writer.writeheader()

    try:
        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(to_scrape, desc="  ShowReview", unit="cmte",
                      dynamic_ncols=True, colour="green") as bar:
                for lnk in bar:
                    member_id  = lnk["member_id"]
                    ctype_code = lnk["ctype_code"]
                    ctype_label= lnk["ctype_label"]
                    url        = lnk["show_review_url"]

                    bar.set_postfix_str(f"{ctype_label[:20]} id={member_id}", refresh=False)

                    try:
                        page.goto(url, timeout=50_000)
                        page.wait_for_load_state("networkidle")
                        time.sleep(0.2)

                        html = page.content()
                        detail = _parse_show_review(html, member_id, ctype_code, ctype_label)
                        writer.writerow(detail)
                        out_f.flush()

                        upsert_manifest({
                            "relation_type": "committee_detail",
                            "key":           member_id,
                            "filename":      "de_committee_details.csv",
                            "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                            "row_count":     1,
                        })
                        done.add(("committee_detail", member_id))
                        ok += 1
                        time.sleep(0.3)

                    except Exception as e:
                        log.page_scrape_error(entity="committee", page_id=member_id, error=str(e))
                        err += 1
                        time.sleep(1)
    finally:
        out_f.close()

    log.page_scrape_complete(
        filename=str(COMMITTEE_DETAILS_PATH),
        rows=existing_count + ok,
        duration_s=0,
        ok=ok,
        err=err,
    )
    return ok, err


# ============================ orchestrator ============================

def run(force: bool = False, entities: bool = False, transactions: bool = False,
        start_year: int | None = None, end_year: int | None = None,
        contributions: bool = False, expenditures: bool = False):
    """Orchestrate download of CFRS transaction CSVs and committee entities.

    Transactions: contributions + expenditures, one CSV per year.
    Entities (two-phase):
      Phase 1 — scrape ViewCommittees HTML for memberID + ShowReview URL per committee
      Phase 2 — navigate each ShowReview page and extract full detail
    """
    log = get_logger("delaware", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Delaware scraper")
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("[!] Playwright not installed — run: pip install playwright && playwright install chromium")
        log._emit("scrape_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, error="playwright not installed")
        return

    transactions_implied = contributions or expenditures
    do_both         = not entities and not transactions and not transactions_implied
    do_transactions = transactions or transactions_implied or do_both
    do_entities     = entities     or do_both
    current_year    = str(datetime.today().year)

    files_ok = files_err = 0

    if force:
        if do_both:
            if MANIFEST.exists():
                MANIFEST.unlink()
        elif do_transactions:
            strip_manifest(lambda r: r["relation_type"] not in TRANSACTION_RELATIONS)
        else:
            strip_manifest(lambda r: r["relation_type"] in TRANSACTION_RELATIONS)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(accept_downloads=True)
            page    = context.new_page()

            if do_transactions:
                t_ok, t_err = download_transactions(
                    page, context, log, force=force, current_year=current_year,
                    start_year=start_year, end_year=end_year,
                    contributions=contributions, expenditures=expenditures,
                )
                files_ok  += t_ok
                files_err += t_err

            if do_entities:
                # Phase 1: collect committee links from ViewCommittees HTML
                scrape_committee_links(page, log, force=force)

                # Phase 2: scrape ShowReview detail for each committee
                d_ok, d_err = scrape_committee_details(page, log, force=force)
                files_ok  += d_ok
                files_err += d_err

            browser.close()

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} errors")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log.warning("Interrupted")
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


# ====== CLI ==================================
if __name__ == "__main__":
    import argparse
    ap   = argparse.ArgumentParser(
        description="Download Delaware campaign finance data from CFRS."
    )
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",        action="store_true",
                      help="re-download everything, ignoring the manifest")
    vert.add_argument("--start-year",   type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); re-downloads all in-range years")
    ap.add_argument("--end-year",       type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")
    ap.add_argument("--transactions",   action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",       action="store_true",
                    help="committee links + ShowReview detail only")
    ap.add_argument("--contributions",  action="store_true",
                    help="contributions only")
    ap.add_argument("--expenditures",   action="store_true",
                    help="expenditures only")
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
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
