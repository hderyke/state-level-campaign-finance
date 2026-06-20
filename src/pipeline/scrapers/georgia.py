"""
scrapers/georgia.py — Download Georgia campaign finance data.

Two separate systems are supported via the --legacy flag:

── Peachfile (default, 2025–present) ────────────────────────────────────────
POSTs to the Georgia Government Transparency & Campaign Finance Commission's
Peachfile API (new e-filing system, live Nov 2021):
  - Transactions: ExportPublicData/GetExportPublicDownloadData with
    TransactionTypeCode (TCON/TEXP) and FilingYear — returns raw CSV content.
  - Entities: PublicFilerDetails/GetCandidateDetails paged JSON — all candidate
    and committee registrations written to candidates.csv and committees.csv.
  - Public (non-candidate) committees: PublicFilerDetails/GetCommitteeDetails
    paged JSON — backs the "Non-Candidate Committee" search at
    peachfile.ethics.ga.gov/public/cf/publiccommitte. GetCandidateDetails in
    practice returns candidate registrations only (committees.csv above is
    always empty), so this endpoint is the only source for PACs, party
    committees, leadership committees, independent committees, and
    ballot-question committees. Records whose filerEntityId already appears
    in candidates.csv/committees.csv are skipped; the rest are written to
    public_committees.csv.

API base: https://api-peachfile.ethics.ga.gov/api
Public frontend: https://peachfile.ethics.ga.gov

No authentication required. Origin/Referer headers required (CORS checking).
WAF blocks pageSize >= 200 and Python default User-Agent — browser UA required.

── Legacy site (--legacy, 2006–2024) ────────────────────────────────────────
Scrapes the legacy Georgia Ethics search portal (media.ethics.ga.gov) by:
  1. GETting the pre-built search results URL (parameters in query string)
  2. POSTing the Export image button to receive a CSV response

Legacy entities are pulled from the same portal's name search:
  - Candidates: Campaign_Namesearchresults.aspx swept by last-name initial
    A–Z (Method=0 = "begins with") → legacy_candidates_{letter}.csv.
    This page has NO Export control — each result row's View link is fired
    as a __doPostBack, the 302 redirect gives Campaign_Name.aspx?NameID=..
    &FilerID=.., and the detail page is parsed (one output row per DOI /
    registration).
  - Non-candidate committees: Campaign_Namesearchresults_NC.aspx swept by
    CommitteeType 1–9 → legacy_committees_type{id}.csv via the btnExport
    image button ("Exported results contains all registration information").

The export endpoint is slow — each year/type combination takes 60-180 seconds.
MUST be run locally; the scraper will hang in restricted network environments.

Legacy contributions types: Monetary, In-Kind, Loan
Legacy expenditure types:   Expenditure, Reimbursement, Credit Card, In-Kind

The contribution pages' Export streams true CSV, but the expenditure pages
render the results GridView as a bare HTML <table> fragment (the classic
ASP.NET export-to-Excel pattern). Table responses are detected and converted
to CSV before writing, so everything in raw/ is uniform CSV.

Note on election years: some contributions on the old site have Election_Year
values of 2025-2026 even though the contribution date is pre-2025. This is
expected — donors file early for upcoming cycles. The scraper filters by
contribution date (not election year), so scraping through 2024 avoids overlap
with Peachfile data.

Downloads are tracked in manifest.csv — re-running skips already-fetched years
except the current year, which is always re-fetched.
"""

import csv
import sys
import re
import time
from datetime import datetime
from pathlib import Path
from calendar import monthrange
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from config import USER_AGENT

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Georgia" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Georgia" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["transaction_type", "year", "filename", "downloaded_at", "row_count"]

# ============================ constants ==============================

API_BASE        = "https://api-peachfile.ethics.ga.gov/api"
EXPORT_ENDPOINT = f"{API_BASE}/ExportPublicData/GetExportPublicDownloadData"
ENTITY_ENDPOINT    = f"{API_BASE}/PublicFilerDetails/GetCandidateDetails"
COMMITTEE_ENDPOINT = f"{API_BASE}/PublicFilerDetails/GetCommitteeDetails"

TRANSACTION_TYPES = {
    "TCON": "contributions",
    "TEXP": "expenditures",
}

# Earliest year with substantive data in the Peachfile system.
# 2021-2022 return 404; 2023-2024 return header-only CSVs (no rows).
START_YEAR = 2025

# Page size for the candidate/committee registry endpoint.
# WAF on api-peachfile.ethics.ga.gov blocks requests with pageSize >= 200.
ENTITY_PAGE_SIZE = 100

# Fields present in GetCandidateDetails response items
ENTITY_FIELDS = [
    "filerEntityId", "filerRegistrationId", "guid",
    "filerName", "committeeName",
    "candidateFirstName", "candidateMiddleName", "candidateMiddleInitial",
    "candidateLastName",
    "office", "officeId",
    "districtType", "districtTypeId", "districtName", "districtId",
    "jurisdiction", "jurisdictionId", "jurisdictionTypeName", "jurisdictionTypeId",
    "politicalPartyCode", "partyAffiliation", "partyAffiliationId",
    "filerStatus", "filerStatusCode",
    "filingCycleId", "filingCycleName",
    "electionCycleId", "electionCycleName",
    "totalContributions", "totalExpenditures", "cashOnHand",
    "isCoosa", "isTerminated", "isRenewed",
    "treasurerFirstName", "treasurerLastName",
    "chairPersonFirstName", "chairPersonLastName",
    "candidateMailingAddress1", "candidateMailingAddress2",
    "candidateMailingCity", "candidateMailingStateCode", "candidateMailingZipCode",
]

# Fields present in GetCommitteeDetails response items (the "Non-Candidate
# Committee" public search at peachfile.ethics.ga.gov/public/cf/publiccommitte).
# Distinct from ENTITY_FIELDS: uses committeeMailing* instead of
# candidateMailing*, adds filerType/filerTypeCode and combined "Last, First"
# chairPerson/treasurer strings, plus leadership-PAC and ballot-question
# fields (affiliatedCommittee*, stance, ballotQuestion*, leadershipRole,
# nominationDate, designationDate).
PUBLIC_COMMITTEE_FIELDS = [
    "filerEntityId", "filerRegistrationId", "guid",
    "filerName", "filerType", "filerTypeCode",
    "filerStatus", "filerStatusCode",
    "filingCycleId", "filingCycleName",
    "districtName", "districtId", "jurisdictionTypeName", "jurisdictionTypeId",
    "partyAffiliation",
    "totalContributions", "totalExpenditures",
    "isTerminated", "isRenewed",
    "treasurer", "treasurerFirstName", "treasurerLastName",
    "chairPerson", "chairPersonFirstName", "chairPersonLastName",
    "committeeMailingAddress1", "committeeMailingAddress2",
    "committeeMailingCity", "committeeMailingStateCode", "committeeMailingZipCode",
    "leadershipRole", "nominationDate", "designationDate",
    "affiliatedCommitteeName", "affiliatedCommitteeAddress1", "affiliatedCommitteeAddress2",
    "affiliatedCommitteeCity", "affiliatedCommitteeStateCode", "affiliatedCommitteeZipCode",
    "stance", "ballotQuestionName", "ballotQuestionDescription",
]

# filerStatusCode values that indicate a candidate (vs. committee-only) entity
# CC = Campaign Committee, PAC = PAC, IC = Independent Committee, etc.
# FACT = Active Candidate/Committee — used for filtering in the parser, not here;
# we write all rows and let the parser decide.

# ========================= manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    """Returns {(transaction_type, year)} for every completed entry."""
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="") as f:
        return {(r["transaction_type"], r["year"]) for r in csv.DictReader(f)}


def strip_manifest(keep_fn):
    """Rewrite manifest keeping only rows that satisfy keep_fn."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict):
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


# ========================= download helpers ==========================

def _make_session() -> requests.Session:
    """Create a session with headers required to avoid CORS rejection and WAF blocking."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":   USER_AGENT,
        "Content-Type": "application/json",
        "Accept":       "application/json, text/plain, */*",
        "Origin":       "https://peachfile.ethics.ga.gov",
        "Referer":      "https://peachfile.ethics.ga.gov/",
    })
    return s


def _probe_year(session: requests.Session, year: str, tx_type: str = "TCON") -> str:
    """
    Quick check for whether a year has data.
    Returns 'data', 'empty' (header-only), or 'missing' (404).
    """
    payload = {"Type": "CSV", "FilingYear": year, "TransactionTypeCode": tx_type}
    try:
        resp = session.post(EXPORT_ENDPOINT, json=payload, timeout=30)
        if resp.status_code == 404:
            return "missing"
        if resp.status_code != 200:
            return f"error_{resp.status_code}"
        # Count newlines in first 50 KB to check for data rows
        sample = resp.content[:51200]
        return "data" if sample.count(b"\n") > 1 else "empty"
    except Exception:
        return "error"


# ============================ entities ==============================

def download_entities(log, session: requests.Session) -> tuple[int, int] | None:
    """
    Fetch all campaign finance filers from the public candidate registry.
    Paginates through GetCandidateDetails until all records are retrieved.
    Writes candidates.csv and committees.csv to RAW_DIR.
    Returns (candidate_count, committee_count) or None on failure.
    """
    log.file_download_start(filename="entities (candidates + committees)")
    t0 = time.perf_counter()

    all_items = []
    page = 1
    total_items = None

    try:
        while True:
            payload = {"pageNumber": page, "pageSize": ENTITY_PAGE_SIZE}
            resp = session.post(ENTITY_ENDPOINT, json=payload, timeout=60)
            resp.raise_for_status()
            body = resp.json()
            data = body["data"]

            if total_items is None:
                total_items = data.get("totalItems", 0)

            items = data.get("items") or []
            if not items:
                break

            all_items.extend(items)

            if len(all_items) >= total_items:
                break
            page += 1
            time.sleep(0.2)

    except Exception as e:
        log.file_download_error(filename="entities", error=str(e))
        return None

    # Split: rows with a candidateLastName go to candidates; rest to committees.
    # Georgia doesn't have a clean filerTypeCode distinguishing them, so we use
    # the presence of candidateLastName as the heuristic.
    candidates = [r for r in all_items if r.get("candidateLastName")]
    committees = [r for r in all_items if not r.get("candidateLastName")]

    for filename, rows in [("candidates.csv", candidates), ("committees.csv", committees)]:
        out_path = RAW_DIR / filename
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ENTITY_FIELDS, extrasaction="ignore",
                                    restval="")
            writer.writeheader()
            writer.writerows(rows)

    total = len(all_items)
    log.file_download_ok(
        filename="candidates.csv + committees.csv",
        bytes=sum((RAW_DIR / fn).stat().st_size
                  for fn in ("candidates.csv", "committees.csv")),
        rows=total,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return len(candidates), len(committees)


def download_public_committees(log, session: requests.Session) -> int | None:
    """
    Fetch non-candidate committee registrations from the public committee
    registry (PublicFilerDetails/GetCommitteeDetails — backs
    https://peachfile.ethics.ga.gov/public/cf/publiccommitte).

    download_entities() already splits any GetCandidateDetails row without a
    candidateLastName into committees.csv, but in practice every
    GetCandidateDetails row has a candidateLastName (committees.csv is always
    empty). GetCommitteeDetails is the only source for PACs, party
    committees, leadership committees, independent committees, and
    ballot-question committees.

    Skips any record whose filerEntityId already appears in candidates.csv or
    committees.csv, so it never duplicates entities download_entities() already
    captured. The remainder is written to public_committees.csv.
    Returns the number of new committees written, or None on failure.
    """
    log.file_download_start(filename="public committees")
    t0 = time.perf_counter()

    # filerEntityIds already captured via GetCandidateDetails.
    existing_ids = set()
    for fn in ("candidates.csv", "committees.csv"):
        path = RAW_DIR / fn
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    fid = (row.get("filerEntityId") or "").strip()
                    if fid:
                        existing_ids.add(fid)

    all_items = []
    page = 1
    total_items = None

    try:
        while True:
            payload = {"pageNumber": page, "pageSize": ENTITY_PAGE_SIZE}
            resp = session.post(COMMITTEE_ENDPOINT, json=payload, timeout=60)
            resp.raise_for_status()
            body = resp.json()
            data = body["data"]

            if total_items is None:
                total_items = data.get("totalItems", 0)

            items = data.get("items") or []
            if not items:
                break

            all_items.extend(items)

            if len(all_items) >= total_items:
                break
            page += 1
            time.sleep(0.2)

    except Exception as e:
        log.file_download_error(filename="public_committees", error=str(e))
        return None

    new_committees = [r for r in all_items
                      if str(r.get("filerEntityId", "")).strip() not in existing_ids]

    out_path = RAW_DIR / "public_committees.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PUBLIC_COMMITTEE_FIELDS,
                                extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(new_committees)

    log.file_download_ok(
        filename="public_committees.csv",
        bytes=out_path.stat().st_size,
        rows=len(new_committees),
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return len(new_committees)


# ========================== transactions ============================

def download_transaction(log, tx_type: str, year: str,
                         session: requests.Session) -> tuple[str, int] | None:
    """
    POST to the Peachfile export API and save the CSV response.
    Returns (filename, row_count) or None on failure.
    """
    label    = TRANSACTION_TYPES[tx_type]
    filename = f"{label}_{year}.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    payload = {"Type": "CSV", "FilingYear": year, "TransactionTypeCode": tx_type}

    try:
        resp = session.post(EXPORT_ENDPOINT, json=payload, timeout=120)
        if resp.status_code == 404:
            log.file_download_error(filename=filename,
                                    error=f"year {year} not found (404)")
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    # Detect and normalize UTF-16 encoding (common from .NET backends)
    content = resp.content
    if content[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = content.decode("utf-16")
    elif len(content) > 1 and content[1] == 0:
        text = content.decode("utf-16-le")
    elif content[:3] == b"\xef\xbb\xbf":
        text = content[3:].decode("utf-8")
    else:
        text = content.decode("utf-8", errors="replace")

    # Treat header-only responses as effectively empty
    row_count = max(text.count("\n") - 1, 0)
    if row_count == 0:
        log.file_download_error(filename=filename,
                                error=f"year {year} returned header-only (no rows)")
        return None

    out_path.write_text(text, encoding="utf-8")

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


# ======================= recordsearch (GACFIS) =======================
#
# recordsearch.ethics.ga.gov ("GEORGIA Campaign Finance System" / GACFIS,
# backend api-recordsearch.ethics.ga.gov) is a 4th, independent GA data
# source — a public, no-auth, row-level transaction database that is much
# larger than Peachfile + legacy combined (see docs/states/georgia.md).
# Activated via --recordsearch.
#
# Chunked by (year, month) for resumability: each (kind, year, month) is
# tracked in manifest.csv as transaction_type="recordsearch_{kind}",
# year="YYYY-MM". pageSize is capped at 100 by the WAF (>=200 -> 400
# "Potentially harmful payload detected!").

RECORDSEARCH_API_BASE         = "https://api-recordsearch.ethics.ga.gov/api"
RECORDSEARCH_CONTRIB_ENDPOINT = f"{RECORDSEARCH_API_BASE}/PublicTransactionDetails/GetTransactionDetails"
RECORDSEARCH_EXPEND_ENDPOINT  = f"{RECORDSEARCH_API_BASE}/PublicTransactionDetails/GetExpenditureDetails"

RECORDSEARCH_PAGE_SIZE  = 100
RECORDSEARCH_START_YEAR = 2014

RECORDSEARCH_KINDS = {
    "contributions": RECORDSEARCH_CONTRIB_ENDPOINT,
    "expenditures":  RECORDSEARCH_EXPEND_ENDPOINT,
}

# Base request payloads — fromDate/toDate/pageNumber are filled in per request.
RECORDSEARCH_CONTRIB_PAYLOAD = {
    "pageNumber": 1, "pageSize": RECORDSEARCH_PAGE_SIZE,
    "sortBy": "Transaction Date", "sortType": "asc",
    "transactionTypeCode": "TCON",
    "filerName": "", "sourceName": "",
    "transactionAmountMax": None, "sourceTypeCode": "",
    "committeeType": "", "electionID": "", "reportName": "",
    "toDate": None, "fromDate": None,
    "byState": "", "electionType": "", "electionYear": "",
    "filerRegistrationGuid": None,
}

RECORDSEARCH_EXPEND_PAYLOAD = {
    "pageNumber": 1, "pageSize": RECORDSEARCH_PAGE_SIZE,
    "sortBy": "Transaction Date", "sortType": "asc",
    "transactionTypeCode": "TEXP",
    "electionYear": "", "electionType": None,
    "searchedTransactionTypeCode": None,
    "filerName": "", "candidateName": "", "measure": "",
    "officeSought": "", "district": "",
    "transactionAmountMax": None, "sourceTypeCode": None,
    "committeeType": "", "electionID": None,
    "sourceName": "", "sourceAddress": "",
    "toDate": None, "fromDate": None,
    "reportName": "", "purpose": None,
    "city": "", "state": "", "stance": "", "address": "",
    "transactionCategory": "",
}

RECORDSEARCH_PAYLOADS = {
    "contributions": RECORDSEARCH_CONTRIB_PAYLOAD,
    "expenditures":  RECORDSEARCH_EXPEND_PAYLOAD,
}

# Raw CSV columns — superset of fields observed on live contribution and
# expenditure items. Used for both kinds; extrasaction='ignore' on write
# drops whichever half doesn't apply to a given item.
RECORDSEARCH_FIELDS = [
    "transactionId", "guid",
    "transactionDate", "sortTransactionDate", "transactionAmount",
    "transactionTypeCode", "transactionTypeCodeDescription",
    "transactionCategoryCode", "transactionCategory",
    "transactionSubTypeCode", "transactionSubTypeDesc",
    "transactionStatusCode",
    "electionYear", "electionType", "electionTypeCode", "electionTypeDescription",
    "filerName", "filerEntityId", "filerTypeCode", "filerTypeDesc",
    "campaignCommittee", "reportName", "filerReportId", "filerReportGuid",
    "candidateFirstName", "candidateMiddleName", "candidateMiddleInitial", "candidateLastName",
    "sourceName", "transactionSource", "transactionSourceTypeCode",
    "transactionSourceAddressLine1", "transactionSourceAddressLine2",
    "transactionSourceCity", "transactionSourceStateCode",
    "transactionSourceZipcode", "transactionSourceCountryCode", "transactionSourceCountry",
    "payeeFirstName", "payeeMiddleName", "payeeLastName",
    "payeeOccupation", "payeeEmployer",
    "transactionPurposeDescription", "description",
    "numberOfUnItemizedExpenditures",
    "isRegulatedEntity", "regulatedEntityName",
]


def _recordsearch_session() -> requests.Session:
    """Session with headers required by recordsearch's WAF/CORS checks."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":   USER_AGENT,
        "Content-Type": "application/json",
        "Accept":       "application/json, text/plain, */*",
        "Origin":       "https://recordsearch.ethics.ga.gov",
        "Referer":      "https://recordsearch.ethics.ga.gov/",
    })
    return s


def download_recordsearch(log, kind: str, year: int, month: int,
                          session: requests.Session) -> tuple[str, int] | None:
    """
    Page through GetTransactionDetails (kind="contributions") or
    GetExpenditureDetails (kind="expenditures") for one calendar month and
    write all rows to a CSV. pageSize is capped at 100 by the WAF.
    Returns (filename, row_count) — row_count may be 0 — or None on
    repeated request failure.
    """
    filename = f"recordsearch_{kind}_{year}_{month:02d}.csv"
    out_path = RAW_DIR / filename
    endpoint = RECORDSEARCH_KINDS[kind]
    base_payload = RECORDSEARCH_PAYLOADS[kind]

    from_date = f"{year}-{month:02d}-01"
    to_date   = f"{year}-{month:02d}-{monthrange(year, month)[1]:02d}"

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    all_items = []
    total_items = None
    page = 1

    while True:
        payload = dict(base_payload, pageNumber=page, fromDate=from_date, toDate=to_date)

        body = None
        for attempt in range(3):
            try:
                resp = session.post(endpoint, json=payload, timeout=60)
                resp.raise_for_status()
                body = resp.json()
                break
            except Exception as e:
                if attempt == 2:
                    log.file_download_error(filename=filename, error=str(e))
                    return None
                time.sleep(2 * (attempt + 1))

        data = body.get("data") or {}
        if total_items is None:
            total_items = data.get("totalItems", 0)

        items = data.get("items") or []
        if not items:
            break

        all_items.extend(items)

        if len(all_items) >= total_items:
            break
        page += 1
        time.sleep(0.2)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RECORDSEARCH_FIELDS,
                                extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(all_items)

    row_count = len(all_items)
    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


# ===================== recordsearch run ==============================

def run_recordsearch(force: bool = False,
                     start_year: int = RECORDSEARCH_START_YEAR,
                     end_year: int | None = None):
    """
    Download recordsearch.ethics.ga.gov contributions + expenditures,
    chunked by (year, month), for start_year through end_year (default:
    current year). The current month is always re-fetched (still filing);
    earlier months are skipped if already in manifest.csv unless --force.

    Long-running: the full 2014-present range is ~25,000+ contribution
    requests and ~2,000 expenditure requests at pageSize=100. Run as a
    background process for a full scrape.
    """
    log = get_logger("georgia", "scrape")
    t0  = time.perf_counter()
    today = datetime.today()
    if end_year is None:
        end_year = today.year

    log._emit("scrape_started", mode="recordsearch", force=force,
              start_year=start_year, end_year=end_year)

    if force:
        strip_manifest(lambda r: not r["transaction_type"].startswith("recordsearch_"))

    done    = load_manifest()
    session = _recordsearch_session()
    downloaded_at = today.strftime("%Y-%m-%d")
    files_ok  = 0
    files_err = 0

    tasks = []
    for year in range(start_year, end_year + 1):
        max_month = today.month if year == today.year else 12
        for month in range(1, max_month + 1):
            for kind in RECORDSEARCH_KINDS:
                tasks.append((kind, year, month))

    try:
        for kind, year, month in tasks:
            key      = (f"recordsearch_{kind}", f"{year}-{month:02d}")
            filename = f"recordsearch_{kind}_{year}_{month:02d}.csv"
            is_current_month = (year == today.year and month == today.month)

            if key in done and not is_current_month:
                log.file_download_skip(filename=filename)
                continue

            result = download_recordsearch(log, kind, year, month, session)
            if result is None:
                files_err += 1
            else:
                _, row_count = result
                files_ok += 1
                strip_manifest(
                    lambda r, k=key: not (r["transaction_type"] == k[0]
                                          and r["year"] == k[1])
                )
                append_manifest({
                    "transaction_type": key[0],
                    "year":             key[1],
                    "filename":         filename,
                    "downloaded_at":    downloaded_at,
                    "row_count":        row_count,
                })

            time.sleep(0.3)

        duration = round(time.perf_counter() - t0, 1)
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


# =================== legacy site constants ==========================

LEGACY_BASE = "https://media.ethics.ga.gov/search/Campaign/"

# Results page URLs — parameters are embedded in the query string.
# The server runs the query on GET and holds results in session state;
# the Export postback then streams the full CSV.
LEGACY_CONTRIB_URL  = (
    LEGACY_BASE
    + "Campaign_ByContributionsearchresults.aspx"
    + "?Contributor=&Zip=&City=&ContTypeID={type}&PAC=&Employer="
    + "&Occupation=&From={from_date}&To={to_date}&Cash=&InK="
    + "&Filer=&Candidate=&Committee="
)
LEGACY_EXPEND_URL   = (
    LEGACY_BASE
    + "campaign_ByExpendituresearchresults.aspx"
    + "?Name=&ExpTypeID={type}&OccEmp=&Purpose=&From={from_date}"
    + "&To={to_date}&Item=&Paid=&Filer=&Candidate=&Committee="
)

# Contribution types in scope for the project.
# Common Source and Credit Received on Loan are derivative/out-of-scope.
LEGACY_CONTRIB_TYPES = ["Monetary", "In-Kind", "Loan"]

# Expenditure types in scope.
# 3rd Party, End Recipient, Deferred Payment are derivative records.
LEGACY_EXPEND_TYPES  = ["Expenditure", "Reimbursement", "Credit Card", "In-Kind"]

# Entity name-search results pages. Empty params must stay in the query
# string — the server errors them out entirely absent. Method=0 = "begins
# with" on the last name, so an A–Z sweep partitions candidates cleanly.
LEGACY_CAND_RESULTS_URL = (
    LEGACY_BASE
    + "Campaign_Namesearchresults.aspx"
    + "?CommitteeName=&LastName={letter}&FirstName=&Method=0"
)
LEGACY_NC_RESULTS_URL = (
    LEGACY_BASE
    + "Campaign_Namesearchresults_NC.aspx"
    + "?CommitteeType={type_id}&CommitteeName="
)

# Non-candidate committee type IDs 1–9, matching the radio-button order on
# Campaign_ByName.aspx (1=PAC, ..., 9=All Other than Candidate Committees).
# Only ID 1 is verified; output filenames use the numeric ID and the export's
# own committee-type column is authoritative. Empty types are skipped.
LEGACY_NC_TYPE_IDS = range(1, 10)

# Output columns for the candidate detail-page sweep. One row per DOI
# (Declaration of Intent — one registration per filing cycle); contact info
# comes from the registration the View link redirected to.
LEGACY_CAND_FIELDS = ["name_id", "filer_id", "candidate_name", "office",
                      "status", "address", "city_state_zip", "telephone"]

# Earliest year with data on the legacy site.
LEGACY_START_YEAR = 2006

# Last year to pull from legacy site — contributions after this date
# are covered by Peachfile (2025+). Set to 2024 to avoid overlap.
LEGACY_END_YEAR = 2024

# Export timeout — the server is slow; 60-180s per request is typical.
# Must be run locally; will hang behind restrictive firewalls.
LEGACY_EXPORT_TIMEOUT = 300


# =================== legacy download helpers ========================

def _legacy_session() -> requests.Session:
    """Session for the legacy media.ethics.ga.gov site."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":   USER_AGENT,
        "Accept":       "text/html,application/xhtml+xml,*/*",
        "Referer":      LEGACY_BASE,
    })
    return s


def _legacy_tokens(html: str) -> dict:
    """Extract ASP.NET form tokens from a results page."""
    def find(field):
        m = re.search(rf'id="{field}" value="([^"]*)"', html)
        return m.group(1) if m else ""
    return {
        "__VIEWSTATE":          find("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": find("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION":    find("__EVENTVALIDATION"),
        "__EVENTTARGET":        "",
        "__EVENTARGUMENT":      "",
    }


def _legacy_has_results(html: str) -> bool:
    """Return True if the results page has at least one data row."""
    m = re.search(r"Page \d+ of (\d+)", html)
    return bool(m) and int(m.group(1)) > 0


def _table_to_csv(html: str) -> str:
    """
    Convert a GridView-rendered HTML <table> export to CSV text.

    The legacy expenditure pages implement Export as the ASP.NET
    "render the GridView" pattern, so the response is a bare <table>
    fragment instead of CSV (the contribution pages stream real CSV).
    The first <tr> is the header row; empty cells arrive as &nbsp;.
    """
    import io

    soup   = BeautifulSoup(html, "html.parser")
    buf    = io.StringIO()
    writer = csv.writer(buf)
    for tr in soup.find_all("tr"):
        cells = [td.get_text().replace("\xa0", " ").strip()
                 for td in tr.find_all(["td", "th"])]
        if cells:
            writer.writerow(cells)
    return buf.getvalue()


def _write_export(out_path: Path, content: bytes) -> int:
    """
    Write an Export response to disk and return its data row count.

    Some legacy exports (expenditures, possibly entities) come back as a bare
    HTML <table> fragment rather than CSV (see module docstring) — those are
    converted before writing so everything in raw/ is uniform CSV.
    """
    if content.lstrip()[:6].lower() == b"<table":
        text = _table_to_csv(content.decode("utf-8", errors="replace"))
        out_path.write_text(text, encoding="utf-8")
        return max(text.count("\n") - 1, 0)
    out_path.write_bytes(content)
    return max(content.count(b"\n") - 1, 0)


def _legacy_export_post(session: requests.Session, url: str,
                        html: str) -> requests.Response | None:
    """
    Fire the Export control on a legacy results page.

    The transaction pages use an image button named
    ctl00$ContentPlaceHolder1$Export; the entity pages haven't been verified,
    so the control is discovered from the page HTML at runtime — first as an
    image button (POSTs name.x/name.y), then as a LinkButton
    (__doPostBack target). Returns the response, or None if no Export
    control is present on the page.
    """
    tokens = _legacy_tokens(html)

    # Image button: <input type="image" name="ctl00$...Export..." ...>
    # (attribute order varies, so try name-first and type-first)
    m = (re.search(r'<input[^>]*name="(ctl00\$[^"]*Export[^"]*)"[^>]*type="image"', html)
         or re.search(r'<input[^>]*type="image"[^>]*name="(ctl00\$[^"]*Export[^"]*)"', html))
    if m:
        data = {**tokens, f"{m.group(1)}.x": "1", f"{m.group(1)}.y": "1"}
        return session.post(url, timeout=LEGACY_EXPORT_TIMEOUT, data=data)

    # LinkButton: href="javascript:__doPostBack('ctl00$...Export...','')"
    m = re.search(r"__doPostBack\('(ctl00\$[^']*Export[^']*)'", html)
    if m:
        data = {**tokens, "__EVENTTARGET": m.group(1)}
        return session.post(url, timeout=LEGACY_EXPORT_TIMEOUT, data=data)

    return None


def download_legacy_entity(log, session: requests.Session,
                           filename: str, url: str) -> tuple[str, int] | None:
    """
    Fetch one entity sweep slice (candidate letter or committee type) from
    the legacy name search and export it to CSV.

    Same GET-then-Export-postback flow as download_legacy, but entity result
    pages have no "Page N of M" text, so emptiness is detected via the
    "Search Returned No Results" message instead.
    Returns (filename, row_count) on success, None on failure or no data.
    """
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        r = session.get(url, timeout=60)
        r.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=f"GET failed: {e}")
        return None

    if "Search Returned No Results" in r.text:
        log.file_download_error(filename=filename, error="no results")
        return None

    try:
        r2 = _legacy_export_post(session, url, r.text)
        if r2 is None:
            log.file_download_error(filename=filename,
                                    error="no Export control found on results page")
            return None
        r2.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=f"Export failed: {e}")
        return None

    content = r2.content
    if b"<!DOCTYPE" in content[:200] or b"<html" in content[:200]:
        log.file_download_error(filename=filename,
                                error="Export returned HTML — session may have expired")
        return None

    row_count = _write_export(out_path, content)

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


def _parse_legacy_candidate(html: str) -> tuple[dict, list[dict]]:
    """
    Extract candidate info from a Campaign_Name.aspx detail page.

    Returns (info, dois) where info holds name/address/phone for the
    registration the page was opened on, and dois has one dict per
    Declaration of Intent row (filer_id, office, status) — one per
    registration/filing cycle for this person.
    """
    soup = BeautifulSoup(html, "html.parser")

    def span(suffix: str) -> str:
        el = soup.find("span", id=re.compile(re.escape(suffix) + "$"))
        return el.get_text(" ", strip=True) if el else ""

    info = {
        "candidate_name": span("lblName"),
        "address":        span("lblAddress"),
        "city_state_zip": span("lblCSZ"),
        "telephone":      span("lblTelephone"),
    }

    dois = []
    tbl = soup.find("table", id=re.compile(r"dlDOIs$"))
    if tbl:
        for tr in tbl.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td", recursive=False)]
            # DOI rows start with a filer ID like C2016000169 / NC2006000148;
            # the recursive=False guard skips rows of nested tables
            if len(tds) >= 4 and re.match(r"^N?C\d{4}", tds[0]):
                dois.append({"filer_id": tds[0], "office": tds[1], "status": tds[3]})
    return info, dois


def sweep_legacy_candidates(log, session: requests.Session,
                            letter: str) -> tuple[str, int] | None:
    """
    Sweep all legacy candidate registrations whose last name begins with
    `letter`, writing legacy_candidates_{letter}.csv.

    The candidate name-search results have no Export control (unlike the
    transaction and committee pages), so each row's View link is fired as a
    __doPostBack instead. The server answers with a 302 to
    Campaign_Name.aspx?NameID=..&FilerID=..&Type=candidate, which is then
    GETted and parsed. ASP.NET accepts repeated postbacks against the same
    page tokens, so the slow results GET happens only once per letter.

    Rows are streamed to disk as they're scraped, and a .progress sidecar
    tracks the last completed grid row — an interrupted letter resumes
    mid-sweep on the next run instead of starting over. The sidecar is
    deleted (and the manifest entry written by the caller) only when the
    letter completes.
    """
    filename      = f"legacy_candidates_{letter}.csv"
    out_path      = RAW_DIR / filename
    progress_path = RAW_DIR / f"legacy_candidates_{letter}.progress"
    url           = LEGACY_CAND_RESULTS_URL.format(letter=letter)

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        r = session.get(url, timeout=LEGACY_EXPORT_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=f"GET failed: {e}")
        return None

    if "Search Returned No Results" in r.text:
        log.file_download_error(filename=filename, error="no results")
        return None

    tokens  = _legacy_tokens(r.text)
    targets = list(dict.fromkeys(re.findall(
        r"__doPostBack\('(ctl00\$ContentPlaceHolder1\$Search_List\$ctl\d+\$lnkViewID)'",
        r.text)))

    # Resume an interrupted letter: the sidecar holds the count of grid rows
    # already attempted. The grid is sorted and the legacy site is frozen, so
    # target order is stable across sessions.
    start_at = 0
    if progress_path.exists() and out_path.exists():
        try:
            start_at = min(int(progress_path.read_text().strip() or 0),
                           len(targets))
        except ValueError:
            start_at = 0
    resume = start_at > 0
    if resume:
        log.info(f"  resuming {filename} at row {start_at}/{len(targets)}")

    ok, err = 0, 0
    refreshed = False   # one token refresh per letter, see below

    fh = open(out_path, "a" if resume else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=LEGACY_CAND_FIELDS,
                            extrasaction="ignore", restval="")
    if not resume:
        writer.writeheader()

    try:
        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(total=len(targets), initial=start_at,
                      desc=f"  candidates {letter}",
                      unit="cand", dynamic_ncols=True) as bar:
                for i, target in enumerate(targets):
                    if i < start_at:
                        continue
                    try:
                        p = session.post(url, timeout=60, allow_redirects=False,
                                         data={**tokens, "__EVENTTARGET": target})
                        loc = p.headers.get("Location", "")

                        # Stale tokens render the page instead of redirecting —
                        # re-GET once per letter to refresh, then retry this row
                        if (p.status_code != 302 or "Campaign_Name.aspx" not in loc) \
                                and not refreshed:
                            refreshed = True
                            r = session.get(url, timeout=LEGACY_EXPORT_TIMEOUT)
                            tokens = _legacy_tokens(r.text)
                            p = session.post(url, timeout=60, allow_redirects=False,
                                             data={**tokens, "__EVENTTARGET": target})
                            loc = p.headers.get("Location", "")

                        if p.status_code != 302 or "Campaign_Name.aspx" not in loc:
                            raise ValueError(f"no detail redirect (HTTP {p.status_code})")

                        qs       = parse_qs(urlparse(loc).query)
                        name_id  = (qs.get("NameID")  or [""])[0]
                        filer_id = (qs.get("FilerID") or [""])[0]

                        d = session.get(urljoin(LEGACY_BASE, loc), timeout=60)
                        d.raise_for_status()
                        info, dois = _parse_legacy_candidate(d.text)

                        # No DOI table — keep the row anyway with the URL's filer ID
                        if not dois:
                            dois = [{"filer_id": filer_id, "office": "", "status": ""}]
                        for doi in dois:
                            writer.writerow({"name_id": name_id, **info, **doi})

                        ok += 1
                        bar.set_postfix_str(info["candidate_name"][:40], refresh=False)
                    except Exception as e:
                        err += 1
                        log.page_scrape_error(entity="candidate", page_id=target,
                                              error=str(e))
                    # Flush so the file and sidecar stay consistent if killed
                    fh.flush()
                    progress_path.write_text(str(i + 1))
                    bar.update(1)
                    time.sleep(0.2)
    finally:
        fh.close()

    progress_path.unlink(missing_ok=True)

    # Row count from the file itself — covers resumed runs too
    with open(out_path, encoding="utf-8") as f:
        row_count = max(sum(1 for _ in f) - 1, 0)

    log.page_scrape_complete(filename=str(out_path), rows=row_count,
                             duration_s=round(time.perf_counter() - t0, 1),
                             ok=ok, err=err)
    return filename, row_count


def download_legacy(log, session: requests.Session,
                    relation: str, tx_type: str, year: int) -> tuple[str, int] | None:
    """
    Fetch one year/type combination from the legacy Georgia Ethics site.

    Steps:
      1. GET the results URL — server runs the query, caches results in session.
      2. POST the Export image button to the same URL — returns CSV response.

    Returns (filename, row_count) on success, None on any failure.

    relation must be 'contributions' or 'expenditures'.
    """
    from_date = f"01/01/{year}"
    to_date   = f"12/31/{year}"
    safe_type = tx_type.replace(" ", "%20")

    if relation == "contributions":
        url = LEGACY_CONTRIB_URL.format(
            type=safe_type, from_date=from_date.replace("/", "%2f"),
            to_date=to_date.replace("/", "%2f"))
    else:
        url = LEGACY_EXPEND_URL.format(
            type=safe_type, from_date=from_date.replace("/", "%2f"),
            to_date=to_date.replace("/", "%2f"))

    label    = tx_type.lower().replace(" ", "_")
    filename = f"legacy_{relation}_{label}_{year}.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    # Step 1: GET results page (runs the query server-side)
    try:
        r = session.get(url, timeout=60)
        r.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=f"GET failed: {e}")
        return None

    if not _legacy_has_results(r.text):
        log.file_download_error(filename=filename,
                                error=f"no results for {year} {tx_type}")
        return None

    tokens = _legacy_tokens(r.text)

    # Step 2: POST Export image button — streams back the CSV
    try:
        r2 = session.post(url, timeout=LEGACY_EXPORT_TIMEOUT, data={
            **tokens,
            "ctl00$ContentPlaceHolder1$Export.x": "1",
            "ctl00$ContentPlaceHolder1$Export.y": "1",
        })
        r2.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=f"Export failed: {e}")
        return None

    # Validate we got data, not an error/search page (full HTML documents
    # start with <!DOCTYPE or <html; the export never does)
    content = r2.content
    if b"<!DOCTYPE" in content[:200] or b"<html" in content[:200]:
        log.file_download_error(filename=filename,
                                error="Export returned HTML — session may have expired")
        return None

    row_count = _write_export(out_path, content)

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


# ========================= legacy run ===============================

def run_legacy(force: bool = False, entities: bool = False,
               transactions: bool = False,
               start_year: int = LEGACY_START_YEAR,
               end_year: int = LEGACY_END_YEAR):
    """
    Download historical Georgia campaign finance data from media.ethics.ga.gov.

    Transactions: loops through years start_year–end_year, downloading each
    contribution type and expenditure type as a separate CSV.
    Entities: sweeps candidates by last-name initial A–Z and non-candidate
    committees by type ID 1–9 (see module docstring).
    Skips slices already in the manifest unless --force is passed.

    MUST be run locally — the export endpoint takes 60-300s per request and
    will time out in most CI/cloud environments.
    """
    log = get_logger("georgia", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", mode="legacy", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year)

    # Default: download everything if neither flag is set
    do_entities     = entities or (not entities and not transactions)
    do_transactions = transactions or (not entities and not transactions)

    session  = _legacy_session()
    files_ok = 0
    files_err = 0
    today    = datetime.today().strftime("%Y-%m-%d")

    # Build full transaction task list: (relation, type, year)
    tasks = (
        [("contributions", t) for t in LEGACY_CONTRIB_TYPES]
        + [("expenditures", t) for t in LEGACY_EXPEND_TYPES]
    )

    # Scope force-stripping to what's actually being re-downloaded
    if force and do_transactions:
        strip_manifest(lambda r: not r["transaction_type"].startswith(
            ("legacy_contributions", "legacy_expenditures")))
    if force and do_entities:
        strip_manifest(lambda r: not r["transaction_type"].startswith(
            ("legacy_candidates", "legacy_committees")))

    done = load_manifest()

    try:
        # ------------------------------------------------------------------ #
        # entities                                                            #
        # ------------------------------------------------------------------ #
        if do_entities:
            # Candidates — A–Z "begins with" last-name sweep partitions the
            # registry with no overlap between letters. No bulk export exists
            # for this page, so each letter is a View-postback page sweep.
            cand_slices = [(f"legacy_candidates_{c}.csv",
                            ("legacy_candidates", c), c, "candidates")
                           for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
            # Non-candidate committees — one Export download per type ID
            cmte_slices = [(f"legacy_committees_type{i}.csv",
                            ("legacy_committees", str(i)), i, "committees")
                           for i in LEGACY_NC_TYPE_IDS]

            for filename, key, arg, kind in cand_slices + cmte_slices:
                if key in done:
                    log.file_download_skip(filename=filename)
                    continue

                if kind == "candidates":
                    result = sweep_legacy_candidates(log, session, arg)
                else:
                    result = download_legacy_entity(
                        log, session, filename,
                        LEGACY_NC_RESULTS_URL.format(type_id=arg))
                if result is None:
                    files_err += 1
                else:
                    _, row_count = result
                    files_ok += 1
                    strip_manifest(
                        lambda r, k=key: not (r["transaction_type"] == k[0]
                                              and r["year"] == k[1])
                    )
                    append_manifest({
                        "transaction_type": key[0],
                        "year":             key[1],
                        "filename":         filename,
                        "downloaded_at":    today,
                        "row_count":        row_count,
                    })

                time.sleep(1.0)

        # ------------------------------------------------------------------ #
        # transactions                                                        #
        # ------------------------------------------------------------------ #
        for year in (range(start_year, end_year + 1) if do_transactions else []):
            for relation, tx_type in tasks:
                label    = tx_type.lower().replace(" ", "_")
                filename = f"legacy_{relation}_{label}_{year}.csv"
                key      = (f"legacy_{relation}_{label}", str(year))

                if key in done:
                    log.file_download_skip(filename=filename)
                    continue

                result = download_legacy(log, session, relation, tx_type, year)
                if result is None:
                    files_err += 1
                else:
                    _, row_count = result
                    files_ok += 1
                    strip_manifest(
                        lambda r, k=key: not (r["transaction_type"] == k[0]
                                              and r["year"] == k[1])
                    )
                    append_manifest({
                        "transaction_type": f"legacy_{relation}_{label}",
                        "year":             str(year),
                        "filename":         filename,
                        "downloaded_at":    today,
                        "row_count":        row_count,
                    })

                # Polite pause between requests — the server is old
                time.sleep(1.0)

        duration = round(time.perf_counter() - t0, 1)
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


# ============================== run =================================

def run(force: bool = False, entities: bool = False, transactions: bool = False):
    """Orchestrate download of transaction files and/or entity registry."""
    log = get_logger("georgia", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions)

    # Default: download everything if neither flag is set
    do_entities     = entities or (not entities and not transactions)
    do_transactions = transactions or (not entities and not transactions)

    session     = _make_session()
    files_ok    = 0
    files_err   = 0
    today       = datetime.today().strftime("%Y-%m-%d")
    current_year = str(datetime.today().year)

    try:
        # ------------------------------------------------------------------ #
        # entities                                                            #
        # ------------------------------------------------------------------ #
        if do_entities:
            if force:
                strip_manifest(lambda r: r["transaction_type"] != "entities")

            result = download_entities(log, session)
            if result is None:
                files_err += 1
            else:
                cand_count, comm_count = result
                files_ok += 1
                log.info(f"  candidates: {cand_count:,}  committees: {comm_count:,}")

                strip_manifest(lambda r: r["transaction_type"] != "entities")
                append_manifest({"transaction_type": "entities", "year": "candidates",
                                 "filename": "candidates.csv", "downloaded_at": today,
                                 "row_count": cand_count})
                append_manifest({"transaction_type": "entities", "year": "committees",
                                 "filename": "committees.csv", "downloaded_at": today,
                                 "row_count": comm_count})

            pub_count = download_public_committees(log, session)
            if pub_count is None:
                files_err += 1
            else:
                files_ok += 1
                log.info(f"  public committees: {pub_count:,}")

                strip_manifest(lambda r: r["transaction_type"] != "public_committees")
                append_manifest({"transaction_type": "public_committees", "year": "committees",
                                 "filename": "public_committees.csv", "downloaded_at": today,
                                 "row_count": pub_count})

        # ------------------------------------------------------------------ #
        # transactions                                                        #
        # ------------------------------------------------------------------ #
        if do_transactions:
            if force:
                strip_manifest(lambda r: r["transaction_type"] not in TRANSACTION_TYPES)

            done = load_manifest()

            # Probe years forward from START_YEAR until two consecutive misses.
            # This auto-discovers new years without needing manual updates.
            years_to_try = []
            consecutive_missing = 0
            for y in range(START_YEAR, datetime.today().year + 3):
                year_str = str(y)
                # Always include current year; for future years probe first
                if y <= datetime.today().year:
                    years_to_try.append(year_str)
                else:
                    status = _probe_year(session, year_str)
                    if status == "data":
                        years_to_try.append(year_str)
                        consecutive_missing = 0
                    else:
                        consecutive_missing += 1
                        if consecutive_missing >= 2:
                            break

            for year in years_to_try:
                for tx_type, label in TRANSACTION_TYPES.items():
                    key = (tx_type, year)
                    filename = f"{label}_{year}.csv"

                    # Skip if already in manifest, unless it's the current year
                    # (current-year files are updated in place by the source)
                    if key in done and year != current_year:
                        log.file_download_skip(filename=filename)
                        continue

                    result = download_transaction(log, tx_type, year, session)
                    if result is None:
                        files_err += 1
                    else:
                        _, row_count = result
                        files_ok += 1
                        # Remove old entry for this key then append fresh one
                        strip_manifest(
                            lambda r, k=key: not (r["transaction_type"] == k[0]
                                                  and r["year"] == k[1])
                        )
                        append_manifest({
                            "transaction_type": tx_type,
                            "year": year,
                            "filename": filename,
                            "downloaded_at": today,
                            "row_count": row_count,
                        })

                    time.sleep(0.5)

        duration = round(time.perf_counter() - t0, 1)
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


# =============================== cli ================================

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description=(
            "Download Georgia campaign finance data.\n\n"
            "Default: Peachfile API (2025–present).\n"
            "Use --legacy to pull historical data from media.ethics.ga.gov (2006–2024).\n"
            "IMPORTANT: --legacy must be run locally — exports take 60-300s each."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--force",        action="store_true",
                    help="re-download everything, ignoring the manifest")
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only — candidates, committees, public committees")
    ap.add_argument("--legacy",       action="store_true",
                    help="download from legacy site (media.ethics.ga.gov) instead of Peachfile")
    ap.add_argument("--recordsearch", action="store_true",
                    help="download from recordsearch.ethics.ga.gov (GACFIS), a 4th GA source "
                         "covering 2014-present. Long-running for a full scrape.")
    ap.add_argument("--start-year",   type=int, default=None,
                    help=f"first year to download (default: {LEGACY_START_YEAR} for --legacy, "
                         f"{RECORDSEARCH_START_YEAR} for --recordsearch)")
    ap.add_argument("--end-year",     type=int, default=None,
                    help=f"last year to download (default: {LEGACY_END_YEAR} for --legacy, "
                         "current year for --recordsearch)")
    args = ap.parse_args()
    try:
        if args.legacy:
            run_legacy(force=args.force, entities=args.entities,
                       transactions=args.transactions,
                       start_year=args.start_year or LEGACY_START_YEAR,
                       end_year=args.end_year or LEGACY_END_YEAR)
        elif args.recordsearch:
            run_recordsearch(force=args.force,
                             start_year=args.start_year or RECORDSEARCH_START_YEAR,
                             end_year=args.end_year)
        else:
            run(force=args.force, entities=args.entities,
                transactions=args.transactions)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
