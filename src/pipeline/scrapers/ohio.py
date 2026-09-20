"""
scrapers/ohio.py — Download Ohio campaign finance data from the Ohio
Secretary of State's File Transfer Page, an Oracle APEX application at
https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:73.

## How this was found

The original plan for this scraper drove CFDISCLOSURE's "advanced search"
forms (Contributions/Expenditures/Candidate-Committee) directly, since that
looked like the only path in. Two things ruled that out on a real run:

  1. ohiosos.gov fingerprints the TLS/HTTP2 handshake, not just headers —
     plain `requests` gets a 403 on every request even with a full Chrome
     User-Agent, while a real browser on the same network succeeds. Fixed
     by switching to curl_cffi's browser-impersonation mode (still not a
     browser — see IMPERSONATE below).
  2. Past that, every search kept failing to reach a results page. It
     turned out the searches weren't broken — Ohio's advanced search
     silently refuses to return more than 10,000 records ("Users
     attempting to query very large amounts of data ... will be required
     to narrow the search criteria"), and a blank/near-blank search (which
     is what bulk collection needs) blows past that on every entity type.
     The same "too many records" response body links to exactly the right
     tool: the **File Transfer Page**, described in its own text as where
     "persons seeking large amounts of campaign finance data" should go.

## What the File Transfer Page actually is

`f?p=CFDISCLOSURE:73` has three tabs, each a plain bookmarkable GET (no
form submission, no session dance):

    Candidate Files  P73_TYPE=CAN
    PAC Files        P73_TYPE=PAC
    Party Files      P73_TYPE=PARTY

Each tab lists pre-generated bulk CSV files — one per (transaction type,
year) going back to 1990, e.g. "Candidate Contributions - 2004",
"Candidate Expenditures - 2019", plus a handful of one-off "All ...
Contributions/Expenditures - <committee>" files for specific
(mostly high-profile/leadership) committees, an "Active ... List" entity
roster, and a "... Cover Pages" file (aggregate per-filing totals — not
itemized, not currently parsed into the canonical schema). Every row's
"Download" link is a plain GET to
`f?p=CFDISCLOSURE:72:::NO::P72_GETID:<id>` — no session needed, no search
row cap, since it's just streaming a pre-built file.

This is what the scraper now does: list each of the three tabs, classify
each row by its label (year-tagged contributions/expenditures, entity
roster, or a one-off committee file), and download whichever ones are in
scope. Available years are discovered from the listing itself rather than
guessed — no more EARLIEST_YEAR constant to keep in sync with the site.

## Incremental updates

Each listing row carries the file's actual last-modified timestamp from
the site itself (`DATE_MODIFIED`). The manifest stores this per file and
skips re-downloading unless it has changed (or --force is given). The
current year's contributions/expenditures files are the one exception:
they're always re-fetched regardless of DATE_MODIFIED, rather than trusting
that Ohio's export has already regenerated to reflect a same-day filing —
this is the explicit "always re-fetch current year" behavior other
scrapers implement via year logic; here it overrides the DATE_MODIFIED
check specifically for `year == current_year` (see `is_current_year` in
`run()`).

## Format quirks

Every raw CSV uses bare `\\r` line endings (old Mac-style), not `\\n` or
`\\r\\n` — Python's default text-mode universal-newline translation
handles this transparently as long as files are opened WITHOUT
`newline=""` (confirmed against a real downloaded file). Do not add
`newline=""` when reading these files in the parser.

## Verified vs assumed

Column layout for Candidate files (`CAC_CON_*`, `CAC_EXP_*`,
`ACT_CAN_LIST`, `CAN_COVER`) was confirmed against real downloaded
samples — see parsers/ohio.py for the exact mapping and a header quirk
(ACT_CAN_LIST repeats the column name "OFFICE" for what is actually the
party column — position-based, not DictReader-based, parsing is required
for that one file). PAC and Party files were NOT sampled — their columns
are assumed identical to the Candidate files by symmetry (same
underlying export, different committee-type filter) and unverified.
"""

import csv
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

try:
    # Ohio's site fingerprints the TLS/HTTP2 handshake, not just headers —
    # confirmed empirically: a plain `requests` session gets a clean 403 on
    # every request (including the very first GET, before any search logic
    # runs) even with a full Chrome User-Agent, while a real browser on the
    # same network/IP loads the same page fine. That combination (browser
    # succeeds, `requests` with correct headers still 403s) is the
    # signature of TLS/JA3 or HTTP/2 fingerprinting (Akamai/Imperva/
    # PerimeterX-style protection, common on .gov disclosure portals)
    # rather than an IP block or a missing header — no amount of header
    # spoofing fixes it, because Python's ssl/urllib3 stack produces a
    # distinctly different TLS ClientHello than a real browser regardless
    # of what headers ride on top of it. curl_cffi wraps a patched libcurl
    # that replicates a real browser's TLS/HTTP2 fingerprint
    # (`impersonate="chrome136"` below), which is what actually gets past
    # this — NOT a browser/Playwright, just a different TLS layer under
    # the same request/response API.
    from curl_cffi import requests
except ImportError as e:
    raise ImportError(
        "scrapers/ohio.py requires curl_cffi (pip install curl_cffi). "
        "Ohio's site blocks plain `requests` sessions with a 403 on every "
        "request — even with a full browser User-Agent — because it "
        "fingerprints the TLS/HTTP2 handshake, not just headers. See the "
        "top of this file for details."
    ) from e
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from src.pipeline import fec_ie as fec_ie_mod

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Ohio" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Ohio" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["group", "label", "getid", "filename", "date_modified", "rows", "downloaded_at"]

# ========================= state-specific constants ===================

BASE = "https://www6.ohiosos.gov/ords"
APP  = "CFDISCLOSURE"

# Backs the `sources` table (cloud/supabase/sources_schema.sql) -- the
# "Sources" section on state/race/candidate-profile.html. Hand-maintained
# here, pushed by cloud/supabase/push_sources.py, NOT part of the normal
# scrape/parse pipeline. URL is the public File Transfer Page this
# module's own docstring already names -- loads fine in a real browser;
# a plain curl gets a 403 from ohiosos.gov's TLS/HTTP2 fingerprinting
# (see this file's module docstring), not a sign of a bad URL.
SOURCES = [
    {"name": "Ohio Secretary of State — Campaign Finance Disclosure",
     "url": "https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:73"},
]

FTP_PAGE = 73   # File Transfer Page — lists files
DL_PAGE  = 72   # Download page — f?p=CFDISCLOSURE:72:::NO::P72_GETID:<id>

# (P73_TYPE value, slug used in filenames)
ENTITY_GROUPS = [
    ("CAN",   "candidates"),
    ("PAC",   "pacs"),
    ("PARTY", "parties"),
]

# NOTE: no User-Agent here on purpose — curl_cffi's impersonate= sets one
# that matches the TLS fingerprint it's replicating. Overriding it with a
# hardcoded string would make the header and the handshake disagree, which
# is exactly the kind of mismatch fingerprinting WAFs look for.
SESSION_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"https://www6.ohiosos.gov/ords/f?p={APP}:73",
}

# Passed to curl_cffi's Session(impersonate=...). Keep this in step with
# config.py's USER_AGENT (currently Chrome/136) if that's ever bumped.
IMPERSONATE = "chrome136"

# ========================= label classification =========================
#
# File Transfer Page rows are classified by their "Entity Name" label text.
# Order matters — more specific patterns first.

_ACTIVE_LIST_RE  = re.compile(r"^Active\s+.*\s+List$", re.I)
_COVER_PAGES_RE  = re.compile(r"Cover\s+Pages$", re.I)
# Handles the label's inconsistent dash spacing seen live: "--2026", " - 2023", "-2024"
_CONTRIB_YEAR_RE = re.compile(r"Contributions\s*-+\s*(\d{4})\s*$", re.I)
_EXPEND_YEAR_RE  = re.compile(r"Expenditures\s*-+\s*(\d{4})\s*$", re.I)
# One-off per-committee files that don't carry a plain year suffix, e.g.
# "All Candidate Contributions - DEWINE HUSTED FOR OHIO"
_OTHER_CONTRIB_RE = re.compile(r"Contributions\s*-+\s*(.+)$", re.I)
_OTHER_EXPEND_RE  = re.compile(r"Expenditures\s*-+\s*(.+)$", re.I)


def _slugify(text: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", text.strip().lower()).strip("_")
    return s[:60] or "unnamed"


def classify_row(label: str, slug: str) -> tuple[str, str]:
    """Return (category, filename) for a File Transfer Page row.

    category is one of: entity_list, cover_pages, contributions_year,
    expenditures_year, contributions_other, expenditures_other, unknown.
    """
    label = label.strip()

    if _ACTIVE_LIST_RE.search(label):
        return "entity_list", f"entities_{slug}_active.csv"
    if _COVER_PAGES_RE.search(label):
        return "cover_pages", f"cover_pages_{slug}.csv"

    m = _CONTRIB_YEAR_RE.search(label)
    if m:
        return "contributions_year", f"contributions_{slug}_{m.group(1)}.csv"
    m = _EXPEND_YEAR_RE.search(label)
    if m:
        return "expenditures_year", f"expenditures_{slug}_{m.group(1)}.csv"

    m = _OTHER_CONTRIB_RE.search(label)
    if m:
        return "contributions_other", f"contributions_{slug}_supp_{_slugify(m.group(1))}.csv"
    m = _OTHER_EXPEND_RE.search(label)
    if m:
        return "expenditures_other", f"expenditures_{slug}_supp_{_slugify(m.group(1))}.csv"

    return "unknown", f"unknown_{slug}_{_slugify(label)}.csv"


# ========================= manifest helpers ===========================

def load_manifest() -> dict[str, dict]:
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {row["filename"]: row for row in csv.DictReader(f)}


def strip_manifest(keep_fn) -> None:
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict, done: dict) -> None:
    filename = record["filename"]
    rows = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    rows = [r for r in rows if r["filename"] != filename]
    rows.append(record)
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(rows)
    done[filename] = record


# ========================= File Transfer Page ==========================

def _list_files(session, log, type_val: str) -> list[dict]:
    """GET one File Transfer Page tab and return its rows as
    [{label, date_modified, filesize, getid, url}, ...].
    """
    url = f"{BASE}/f?p={APP}:{FTP_PAGE}::{type_val}:NO:RP:P73_TYPE:{type_val}"
    r = session.get(url, timeout=60)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="t-Report-report")
    if not table:
        raise RuntimeError(f"no report table found for File Transfer Page type={type_val!r}")

    # Defensive: this listing wasn't seen paginated in practice (66-94 rows,
    # no pagination control), but warn loudly if that ever changes — the
    # classic-report pagination control is client-side JS only (see the
    # scrapers/ohio.py git history / original search-based approach) and
    # has no non-JS equivalent, so a paginated listing would silently lose rows.
    if soup.find("select", attrs={"data-action": "paginate"}):
        log.warning(f"  ! File Transfer Page listing for type={type_val!r} appears to be "
                   f"paginated — only the first page's rows will be collected")

    tbody = table.find("tbody")
    rows = []
    for tr in (tbody.find_all("tr") if tbody else []):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        label = tds[0].get_text(strip=True)
        date_modified = tds[1].get_text(strip=True)
        filesize = tds[2].get_text(strip=True)
        link = tds[3].find("a")
        if not link or not link.get("href"):
            continue
        # Live pages carry a RELATIVE href (e.g. "f?p=CFDISCLOSURE:72:::NO::P72_GETID:120")
        # — only a saved/rendered browser copy rewrites these to absolute URLs, which is
        # what earlier testing was based on. Resolve against the listing page's own final
        # URL (r.url, not a hardcoded base) so this holds up even if the site adds a redirect.
        download_url = urljoin(r.url, link["href"])
        rows.append({
            "label": label,
            "date_modified": date_modified,
            "filesize": filesize,
            "url": download_url,
        })
    return rows


def _download_file(session, url: str, dest: Path) -> int:
    r = session.get(url, timeout=180)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return max(r.content.count(b"\n") + r.content.count(b"\r") - r.content.count(b"\r\n") - 1, 0)


# ========================= FEC nonfederal-IE enrichment ================
# Columns kept in data/Ohio/raw/fec_nonfederal_ie_{year}.csv — a curated
# subset of schedule_b's ~70 fields (drops the always-null conduit/memo
# columns for this use case) plus enough provenance to audit any row back
# to its source filing. See src/pipeline/fec_ie.py for what this data is.
FEC_IE_FIELDS = [
    "committee_id", "committee_name",
    "disbursement_date", "disbursement_amount", "disbursement_description",
    "disbursement_purpose_category", "category_code_full",
    "recipient_name", "recipient_city", "recipient_state", "recipient_zip",
    "recipient_committee_id",
    "candidate_id", "candidate_name",
    "two_year_transaction_period", "report_year", "amendment_indicator",
    "file_number", "sub_id", "transaction_id", "image_number", "pdf_url",
    "matched_state",
]


def _write_fec_ie_csv(rows: list[dict], dest: Path) -> int:
    """Write matched FEC rows to `dest` using the fixed FEC_IE_FIELDS header
    (written even when rows is empty, so downstream globs/parses see a
    consistent, if empty, file rather than a missing one)."""
    with open(dest, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FEC_IE_FIELDS, extrasaction="ignore", restval="")
        w.writeheader()
        for row in rows:
            w.writerow(row)
    return len(rows)


# ==================== FEC nonfederal-IE: committee receipts ==============
# The OTHER side of the same filers above: who funds them (Schedule A),
# not what they spend (Schedule B). See src/pipeline/fec_ie.py's
# fetch_committee_receipts() docstring for why this is committee_id-scoped
# rather than a free-text state search, and why receipts are never
# attributed to a candidate/side. Reads committee_ids straight from
# src/aliases/fec_ie_patterns.csv rather than importing parsers/ohio.py's
# loader (scraper and parser stay independent, same as everywhere else in
# this file) -- only committee_ids already reviewed and added there ever
# get their receipts pulled, so this can't silently expand scope on its
# own.
_FEC_IE_PATTERNS_PATH = PROJECT_ROOT / "src" / "aliases" / "fec_ie_patterns.csv"

RECEIPT_FIELDS = [
    "committee_id", "committee_name",
    "contribution_receipt_date", "contribution_receipt_amount",
    "contributor_name", "contributor_city", "contributor_state", "contributor_zip",
    "contributor_employer", "contributor_occupation", "entity_type",
    "two_year_transaction_period", "report_year", "amendment_indicator",
    "file_number", "sub_id", "transaction_id", "image_number", "pdf_url",
]


def _load_fec_ie_committee_ids() -> list[str]:
    """committee_id column of src/aliases/fec_ie_patterns.csv, in file
    order -- the closed set of committees already confirmed (by their own
    disbursement language) to be doing OH nonfederal-IE spending."""
    ids = []
    if not _FEC_IE_PATTERNS_PATH.exists():
        return ids
    with open(_FEC_IE_PATTERNS_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cid = (row.get("committee_id") or "").strip()
            if cid and cid not in ids:
                ids.append(cid)
    return ids


def _write_fec_ie_receipts_csv(rows: list[dict], dest: Path) -> int:
    """Same rationale as _write_fec_ie_csv() -- fixed header, always
    written even when empty."""
    with open(dest, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RECEIPT_FIELDS, extrasaction="ignore", restval="")
        w.writeheader()
        for row in rows:
            w.writerow(row)
    return len(rows)


# ============================== run ====================================

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
    fec_ie: bool = False,
):
    """
    Download Ohio campaign finance data from the File Transfer Page.

    Horizontal scope:
        (no flag)          everything
        --entities         active-entity rosters only
        --candidates       same as --entities (source doesn't split these)
        --committees       same as --entities (source doesn't split these)
        --transactions     contributions + expenditures only
        --contributions    contribution files only
        --expenditures     expenditure files only

    Vertical scope (filters which discovered years are fetched — years are
    discovered from the File Transfer Page listing itself, not hardcoded):
        (no flag)          incremental — skip files whose DATE_MODIFIED on
                           the site matches what's already in the manifest.
                           The current year's contribution/expenditure files
                           are always re-fetched regardless of DATE_MODIFIED.
        --start-year YYYY  only fetch contribution/expenditure years >= YYYY
        --end-year YYYY    only fetch contribution/expenditure years <= YYYY
        --force            ignore DATE_MODIFIED matches, re-fetch everything
                           in scope

    "Cover Pages" (aggregate per-filing totals, not itemized) and one-off
    per-committee supplemental files are always fetched when their
    category (entities / contributions / expenditures) is in scope — the
    parser currently ignores Cover Pages files but they're kept on disk in
    case a future totals cross-check wants them.

    fec_ie=True additionally pulls FEC-reported "nonfederal candidate"
    independent-expenditure disbursements that mention Ohio (see
    src/pipeline/fec_ie.py for what this is and its limitations) — money
    spent by FEDERALLY-registered committees on Ohio's own (nonfederal)
    races, which Ohio's own File Transfer Page data never captures at all.
    Opt-in and separate from do_all/--transactions/--expenditures since
    it's a fundamentally different-reliability source (best-effort free-
    text search, not an official Ohio filing). Always re-fetches the
    current year and the prior year regardless of manifest state (FEC
    amendments can land well after the fact); older years are cached like
    everything else unless --force.
    """
    log = get_logger("ohio", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year)

    do_all           = not any([entities, transactions, contributions, expenditures,
                                 candidates, committees])
    do_entities      = do_all or entities or candidates or committees
    do_contributions = do_all or transactions or contributions
    do_expenditures  = do_all or transactions or expenditures

    today = time.strftime("%Y-%m-%d")
    current_year = int(time.strftime("%Y"))
    files_ok = files_err = files_skip = 0

    try:
        session = requests.Session(impersonate=IMPERSONATE)
        session.headers.update(SESSION_HEADERS)

        done = {} if force else load_manifest()
        if force:
            strip_manifest(lambda _: False)

        for type_val, slug in ENTITY_GROUPS:
            log.info(f"  Listing File Transfer Page files for {slug}…")
            try:
                listing = _list_files(session, log, type_val)
            except Exception as e:
                log.file_download_error(filename=f"(listing for {slug})", error=str(e))
                files_err += 1
                continue
            log.info(f"    {len(listing)} files listed")

            for row in listing:
                category, filename = classify_row(row["label"], slug)

                in_scope = (
                    (category == "entity_list" and do_entities)
                    or (category == "cover_pages" and do_entities)
                    or (category in ("contributions_year", "contributions_other") and do_contributions)
                    or (category in ("expenditures_year", "expenditures_other") and do_expenditures)
                    or (category == "unknown")   # always fetch unclassified rows — cheap insurance
                )
                if not in_scope:
                    continue

                year = None
                if category in ("contributions_year", "expenditures_year"):
                    year = int(re.search(r"(\d{4})", filename).group(1))
                    if start_year is not None and year < start_year:
                        continue
                    if end_year is not None and year > end_year:
                        continue

                # The current year's file gets new filings added continuously,
                # so its DATE_MODIFIED can legitimately change between two
                # runs on the same day (or even not change yet if Ohio's export
                # hasn't regenerated since a filing came in). Rather than trust
                # that timing, always re-fetch it — matches the "always
                # re-fetch current year" contract other scrapers follow via
                # explicit year logic instead of a site-provided timestamp.
                is_current_year = (year is not None and year == current_year)

                dest = RAW_DIR / filename
                prior = done.get(filename)
                unchanged = (prior is not None
                            and prior.get("date_modified") == row["date_modified"]
                            and dest.exists() and dest.stat().st_size > 0
                            and not is_current_year)
                if not force and unchanged:
                    log.file_download_skip(filename=filename)
                    files_skip += 1
                    continue

                log.file_download_start(filename=filename)
                t_file = time.perf_counter()
                try:
                    n_rows = _download_file(session, row["url"], dest)
                    log.file_download_ok(filename=filename, bytes=dest.stat().st_size,
                                         rows=n_rows,
                                         duration_s=round(time.perf_counter() - t_file, 2))
                    upsert_manifest({
                        "group": slug, "label": row["label"], "getid": row["url"],
                        "filename": filename, "date_modified": row["date_modified"],
                        "rows": n_rows, "downloaded_at": today,
                    }, done)
                    files_ok += 1
                except Exception as e:
                    log.file_download_error(filename=filename, error=str(e))
                    files_err += 1
                time.sleep(0.3)

        if fec_ie:
            log.info("  Fetching FEC nonfederal-IE enrichment (Schedule B, all committees)...")
            fec_start_year = start_year or (current_year - 1)
            fec_end_year   = end_year or current_year
            for year in range(fec_start_year, fec_end_year + 1):
                filename = f"fec_nonfederal_ie_{year}.csv"
                # FEC filings get amended well after the fact (unlike a same-day
                # state filing), so — like the current-year rule above but with
                # a wider buffer — always re-fetch the current and prior year
                # regardless of manifest state; older, fully-elapsed years are
                # cached normally.
                always_refetch = year >= current_year - 1
                prior = done.get(filename)
                if not force and prior and not always_refetch:
                    log.file_download_skip(filename=filename)
                    files_skip += 1
                    continue

                log.file_download_start(filename=filename)
                t_file = time.perf_counter()
                try:
                    matches = fec_ie_mod.fetch_state_matches(
                        "OH", f"{year}-01-01", f"{year}-12-31", state_name="Ohio",
                    )
                    dest = RAW_DIR / filename
                    n_rows = _write_fec_ie_csv(matches, dest)
                    log.file_download_ok(filename=filename, bytes=dest.stat().st_size,
                                         rows=n_rows,
                                         duration_s=round(time.perf_counter() - t_file, 2))
                    upsert_manifest({
                        "group": "fec_ie", "label": f"FEC Nonfederal IE - {year}",
                        "getid": "", "filename": filename, "date_modified": today,
                        "rows": n_rows, "downloaded_at": today,
                    }, done)
                    files_ok += 1
                except Exception as e:
                    log.file_download_error(filename=filename, error=str(e))
                    files_err += 1

            # ── committee receipts (Schedule A) — same year range, nested
            #    per year so each committee's receipts get the same
            #    always-refetch-current-and-prior-year treatment as the
            #    disbursement side above. Only committee_ids already in
            #    src/aliases/fec_ie_patterns.csv are ever fetched -- see
            #    _load_fec_ie_committee_ids()'s docstring.
            log.info("  Fetching FEC nonfederal-IE committee receipts (Schedule A, "
                     "known committees only)...")
            committee_ids = _load_fec_ie_committee_ids()
            for year in range(fec_start_year, fec_end_year + 1):
                always_refetch = year >= current_year - 1
                for committee_id in committee_ids:
                    filename = f"fec_nonfederal_ie_receipts_{committee_id}_{year}.csv"
                    prior = done.get(filename)
                    if not force and prior and not always_refetch:
                        log.file_download_skip(filename=filename)
                        files_skip += 1
                        continue

                    log.file_download_start(filename=filename)
                    t_file = time.perf_counter()
                    try:
                        receipts = fec_ie_mod.fetch_committee_receipts(
                            committee_id, f"{year}-01-01", f"{year}-12-31",
                        )
                        dest = RAW_DIR / filename
                        n_rows = _write_fec_ie_receipts_csv(receipts, dest)
                        log.file_download_ok(filename=filename, bytes=dest.stat().st_size,
                                             rows=n_rows,
                                             duration_s=round(time.perf_counter() - t_file, 2))
                        upsert_manifest({
                            "group": "fec_ie_receipts",
                            "label": f"FEC Nonfederal IE Receipts - {committee_id} - {year}",
                            "getid": "", "filename": filename, "date_modified": today,
                            "rows": n_rows, "downloaded_at": today,
                        }, done)
                        files_ok += 1
                    except Exception as e:
                        log.file_download_error(filename=filename, error=str(e))
                        files_err += 1

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} downloaded, {files_skip} skipped, "
                f"{files_err} errors")
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


# ================================ CLI ==================================

if __name__ == "__main__":
    import argparse
    from datetime import datetime

    ap = argparse.ArgumentParser(
        description="Download Ohio campaign finance data from the File Transfer Page."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="ignore DATE_MODIFIED matches, re-fetch everything in scope")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="only fetch contribution/expenditure years >= YYYY")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="only fetch contribution/expenditure years <= YYYY")

    ap.add_argument("--entities",      action="store_true", help="entities only")
    ap.add_argument("--transactions",  action="store_true", help="transactions only")
    ap.add_argument("--contributions", action="store_true")
    ap.add_argument("--expenditures",  action="store_true")
    ap.add_argument("--candidates",    action="store_true")
    ap.add_argument("--committees",    action="store_true")
    ap.add_argument("--fec-ie",        action="store_true",
                    help="also pull FEC-reported 'nonfederal candidate' independent-"
                         "expenditure disbursements mentioning Ohio (opt-in enrichment "
                         "from federally-registered committees; see src/pipeline/fec_ie.py)")

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
            fec_ie=args.fec_ie,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
