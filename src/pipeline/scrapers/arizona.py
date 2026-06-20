"""
scrapers/arizona.py — Download Arizona campaign finance data.

Bulk transaction data from Arizona's SeeTheMoney disclosure site:
  https://seethemoney.az.gov/Reporting/AdvancedSearch/

Transactions are fetched via the AdvancedSearch JSON API (POST).  No browser
required — a plain requests session with the right headers works fine.

Key request structure (confirmed from browser network capture):
  - Search criteria go in the URL *query string*
  - POST body is DataTables format: draw, columns[N][data], start, length, order
  - Pagination: start=0 for first page, start+=length for each subsequent page

The committee registry and detail pages are fetched via the same session.

Downloaded files are tracked in manifest.csv. A normal (no-flag) run refreshes
only the current calendar year: transactions dated Jan 1 of this year onward are
re-fetched and merged into the active cycle's file, preserving the previously
downloaded prior-year portion (cycles are two-year, labeled by end year).
Historical cycles are immutable and only re-fetched with --force or an explicit
--start-year / --end-year range.

Output CSV columns for Income/Expenditures files (TX_COLS):
  CommitteeID, CommitteeName, TransactionDate, Amount, TransactionName,
  TransactionType, Occupation, Employer, City, State, ZipCode,
  FirstName, LastName, FilerName, Memo
"""

import argparse
import csv
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests as req_lib

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

from config import USER_AGENT

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Arizona" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Arizona" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["cycle_label", "filer_type", "category_type", "filename", "downloaded_at", "row_count"]

# ========================= state-specific constants ===================

BASE_URL      = "https://seethemoney.az.gov/Reporting/AdvancedSearch/"
REPORTING_URL = "https://seethemoney.az.gov/Reporting"

TABLE_LENGTH = 3000  # rows per API page — server maxJsonLength rejects ~10k+; 3k is safe

CYCLES = [
    ("2026",        "44~1/1/2025 12:00:00 AM~12/31/2026 11:59:59 PM"),
    ("2024",        "43~1/1/2023 12:00:00 AM~12/31/2024 11:59:59 PM"),
    ("2022",        "39~1/1/2021 12:00:00 AM~12/31/2022 11:59:59 PM"),
    ("Recall_Fann", "40~7/1/2021 12:00:00 AM~3/31/2022 11:59:59 PM"),
    ("2020",        "30~1/1/2019 12:00:00 AM~12/31/2020 11:59:59 PM"),
    ("2018",        "29~11/9/2016 12:00:00 AM~12/31/2018 11:59:59 PM"),
    ("2016",        "28~11/25/2014 12:00:00 AM~11/8/2016 11:59:59 PM"),
    ("2014",        "27~11/27/2012 12:00:00 AM~11/24/2014 11:59:59 PM"),
    ("2012",        "26~11/23/2010 12:00:00 AM~11/26/2012 11:59:59 PM"),
    ("2010",        "25~11/25/2008 12:00:00 AM~11/22/2010 11:59:59 PM"),
    ("2008",        "8~11/28/2006 12:00:00 AM~11/24/2008 11:59:59 PM"),
    ("2006",        "7~11/23/2004 12:00:00 AM~11/27/2006 11:59:59 PM"),
    ("2004",        "6~11/26/2002 12:00:00 AM~11/22/2004 11:59:59 PM"),
    ("2002",        "5~11/28/2000 12:00:00 AM~11/25/2002 11:59:59 PM"),
    ("2000",        "4~11/24/1998 12:00:00 AM~11/27/2000 11:59:59 PM"),
    ("1998",        "3~11/26/1996 12:00:00 AM~11/23/1998 11:59:59 PM"),
]

CATEGORY_TYPES = ["Income", "Expenditures"]

FILER_TYPES = [
    ("130", "Candidate"),
    ("131", "PAC"),
    ("132", "Party"),
    ("96",  "Officeholder"),
]

REGISTRY_PAGES = {
    1: "Candidate",
    2: "PAC",
    3: "Party",
    4: "BallotMeasure",
    5: "Officeholder",
    6: "Other",
}

REGISTRY_COLS = [
    "entity_id", "public_transaction_table_id", "filer_type",
    "entity_type_name", "entity_type_id", "committee_name",
    "entity_last_name", "entity_first_name", "entity_middle_name",
    "office_name", "party_name", "city", "state", "zip",
    "income", "expense", "cash_balance",
    "ie_support", "ie_opposition", "ballot_measure_id", "ballot_name", "identifier",
]

DETAIL_COLS = [
    "entity_id", "committee_name", "committee_type_name",
    "status", "registration_date", "last_amended_date", "last_filed_date",
    "phone", "email", "mailing_address", "filer_address",
    "city", "state", "zip", "county",
    "chairman_name", "treasurer_name", "master_committee_id",
]

# Columns written to Income_* / Expenditures_* CSV files.
# CommitteeID and CommitteeName are present in the JSON API response but
# absent from the old CSV export — this is why we use the API.
TX_COLS = [
    "CommitteeID", "CommitteeName", "TransactionDate",
    "Amount", "TransactionName", "TransactionType",
    "Occupation", "Employer", "City", "State", "ZipCode",
    "FirstName", "LastName", "FilerName", "Memo",
]


# ========================= requests-only test =========================

def test_requests_only():
    """Test whether a plain requests session works with the correct URL structure.
    Run: python scrapers/arizona.py --test-requests
    """
    import uuid
    from urllib.parse import urlencode

    uid = str(uuid.uuid4())
    session = req_lib.Session()
    session.headers.update({
        "User-Agent":       ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/147.0.0.0 Safari/537.36"),
        "X-Requested-With": "XMLHttpRequest",
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin":           "https://seethemoney.az.gov",
        "Referer":          BASE_URL,
        "Sec-Fetch-Site":   "same-origin",
        "Sec-Fetch-Mode":   "cors",
        "Sec-Fetch-Dest":   "empty",
    })
    session.cookies.set(
        "SeeTheMoneyUserHistory",
        f"UserID={uid}&URLHash=JurisdictionId=0|Page=1|startYear=2025|endYear=2027"
        "|IsLessActive=false|ShowOfficeHolder=false|View=Detail|TablePage=1|TableLength=10",
        domain="seethemoney.az.gov",
    )

    # 2022 PAC Income — small-ish cycle, good test case
    cycle_id_str = "39~1/1/2021 12:00:00 AM~12/31/2022 11:59:59 PM"
    start_date, end_date = parse_cycle_dates(cycle_id_str)

    query_params = {
        "CommiteeReportId": "", "CategoryType": "Income", "JurisdictionId": "0",
        "CycleId": cycle_id_str, "StartDate": start_date, "EndDate": end_date,
        "FilerName": "", "FilerId": "", "BallotName": "", "BallotMeasureId": "",
        "FilerTypeId": "131",  # PAC
        "OfficeTypeId": "", "OfficeId": "", "PartyId": "", "ContributorName": "",
        "VendorName": "", "StateId": "", "City": "", "Employer": "", "Occupation": "",
        "CandidateName": "", "CandidateFilerId": "", "Position": "Support",
        "LowAmount": "", "HighAmount": "",
    }
    dt_body = {
        "draw": "1", "start": "0", "length": "10",
        "search[value]": "", "search[regex]": "false",
        "order[0][column]": "0", "order[0][dir]": "asc",
    }
    for i, col in enumerate(_DT_COLUMNS):
        dt_body[f"columns[{i}][data]"] = col
        dt_body[f"columns[{i}][name]"] = ""
        dt_body[f"columns[{i}][searchable]"] = "true"
        dt_body[f"columns[{i}][orderable]"] = "true"
        dt_body[f"columns[{i}][search][value]"] = ""
        dt_body[f"columns[{i}][search][regex]"] = "false"

    url = BASE_URL + "?" + urlencode(query_params)
    print(f"POST {url[:80]}...")
    r = session.post(url, data=dt_body, timeout=30)
    print(f"Status: {r.status_code}  len={len(r.text)}  preview={repr(r.text[:200])}")
    if r.status_code == 200 and r.text not in ('""', ''):
        try:
            j = r.json()
            print(f"✓ Got JSON: keys={list(j.keys())}  "
                  f"recordsTotal={j.get('recordsTotal')}  rows={len(j.get('data', []))}")
            if j.get("data"):
                print(f"  Sample row: {j['data'][0]}")
        except Exception as e:
            print(f"JSON parse error: {e}")
    else:
        print("✗ No data — Playwright still needed")


# ========================= shared helpers =============================

def parse_net_date(val) -> str:
    """Parse a .NET JSON Date(ms) or plain string to YYYY-MM-DD."""
    if not val:
        return ""
    if isinstance(val, str):
        m = re.search(r"/Date\((-?\d+)\)/", val)
        if m:
            return datetime.fromtimestamp(int(m.group(1)) / 1000,
                                          tz=timezone.utc).strftime("%Y-%m-%d")
    return str(val)


def s(v) -> str:
    return str(v).strip() if v is not None else ""


def build_session() -> req_lib.Session:
    """Build a plain requests session — no browser required.

    The AdvancedSearch endpoint only needs correct headers + the right
    URL structure (search params in query string, DataTables body in POST).
    A random UserID in SeeTheMoneyUserHistory satisfies the cookie check.
    """
    import uuid
    ua = (USER_AGENT if isinstance(USER_AGENT, str) and USER_AGENT
          else ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"))
    session = req_lib.Session()
    session.headers.update({
        "User-Agent":       ua,
        "X-Requested-With": "XMLHttpRequest",
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin":           "https://seethemoney.az.gov",
        "Referer":          BASE_URL,
        "Sec-Fetch-Site":   "same-origin",
        "Sec-Fetch-Mode":   "cors",
        "Sec-Fetch-Dest":   "empty",
    })
    session.cookies.set(
        "SeeTheMoneyUserHistory",
        f"UserID={uuid.uuid4()}&URLHash=JurisdictionId=0|Page=1"
        "|startYear=2025|endYear=2027|IsLessActive=false"
        "|ShowOfficeHolder=false|View=Detail|TablePage=1|TableLength=10",
        domain="seethemoney.az.gov",
    )
    return session


# ============================= registry ===============================

def fetch_registry_page(session: req_lib.Session, page_num: int) -> list[dict]:
    params = {
        "Page":             str(page_num),
        "JurisdictionId":   "0",
        "TablePage":        "1",
        "TableLength":      "999999",
        "IsLessActive":     "false",
        "ShowOfficeHolder": "false",
        "ChartName":        str(page_num),
        "ShowAllYears":     "true",
    }
    r = session.post(f"{REPORTING_URL}/GetTableData", params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def normalize_registry_row(raw: dict, filer_type: str) -> dict:
    return {
        "entity_id":                   s(raw.get("EntityID")),
        "public_transaction_table_id": s(raw.get("PublicTransactionTableID")),
        "filer_type":                  filer_type,
        "entity_type_name":            s(raw.get("EntityTypeName")),
        "entity_type_id":              s(raw.get("EntityTypeId")),
        "committee_name":              s(raw.get("CommitteeName")),
        "entity_last_name":            s(raw.get("EntityLastName")),
        "entity_first_name":           s(raw.get("EntityFirstName")),
        "entity_middle_name":          s(raw.get("EntityMiddleName")),
        "office_name":                 s(raw.get("OfficeName")),
        "party_name":                  s(raw.get("PartyName")),
        "city":                        s(raw.get("PhysicalCity")),
        "state":                       s(raw.get("PhysicalState")),
        "zip":                         s(raw.get("PhysicalZipCode")),
        "income":                      s(raw.get("Income")),
        "expense":                     s(raw.get("Expense")),
        "cash_balance":                s(raw.get("CashBalance")),
        "ie_support":                  s(raw.get("IESupport")),
        "ie_opposition":               s(raw.get("IEOpposition")),
        "ballot_measure_id":           s(raw.get("BallotMeasureId")),
        "ballot_name":                 s(raw.get("BallotName")),
        "identifier":                  s(raw.get("Identifier")),
    }


def download_registry(log, session: req_lib.Session) -> tuple[int, int]:
    """Fetch all registry pages and write az_committees_all.csv. Returns (ok, err)."""
    out   = RAW_DIR / "az_committees_all.csv"
    total = 0
    ok = err = 0
    t0 = time.perf_counter()
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=REGISTRY_COLS, extrasaction="ignore")
        writer.writeheader()
        for page_num, filer_type in REGISTRY_PAGES.items():
            log.info(f"  Registry page={page_num} ({filer_type})...")
            try:
                rows = fetch_registry_page(session, page_num)
            except Exception as e:
                log.page_scrape_error(entity=filer_type, page_id=page_num, error=str(e))
                err += 1
                continue
            if not isinstance(rows, list):
                log.page_scrape_error(entity=filer_type, page_id=page_num,
                                      error=f"unexpected type: {type(rows)}")
                err += 1
                continue
            for raw in rows:
                writer.writerow(normalize_registry_row(raw, filer_type))
            ok += 1
            total += len(rows)
            log.info(f"    → {len(rows):,} rows")
            time.sleep(0.5)
    log.page_scrape_complete(filename=str(out), rows=total,
                             duration_s=round(time.perf_counter() - t0, 2),
                             ok=ok, err=err)
    return ok, err


# =========================== transactions =============================

def parse_cycle_dates(cycle_id_str: str) -> tuple[str, str]:
    _, raw_start, raw_end = cycle_id_str.split("~", 2)
    fmt   = "%m/%d/%Y %I:%M:%S %p"
    start = datetime.strptime(raw_start.strip(), fmt).strftime("%Y-%m-%d")
    end   = datetime.strptime(raw_end.strip(),   fmt).strftime("%Y-%m-%d")
    return start, end


def cycle_is_current(cycle_id_str: str) -> bool:
    """True if today's date falls inside the cycle's date range.

    Date-based rather than label-based on purpose: AZ cycles are labeled by
    their even end year (e.g. "2026" covers 1/1/2025–12/31/2026), so a naive
    `current_year in cycle_label` check misses the active cycle in odd years.
    """
    start, end = parse_cycle_dates(cycle_id_str)
    today = datetime.today().strftime("%Y-%m-%d")
    return start <= today <= end   # ISO strings compare chronologically


_DT_COLUMNS = [
    "TransactionDate", "CommitteeName", "Amount", "TransactionName",
    "TransactionType", "Occupation", "Employer", "City", "State", "ZipCode",
]


def fetch_transactions_page(session: req_lib.Session, cycle_id_str: str,
                             category_type: str, filer_type_id: str,
                             start: int = 0,
                             length: int = TABLE_LENGTH,
                             start_date: str | None = None) -> tuple[list[dict], int]:
    """POST to AdvancedSearch and return (rows, total_records).

    Request structure (confirmed from browser network capture):
      - Search params go in the URL query string
      - POST body is DataTables format: draw, columns[N][data], start, length, order
      - Pagination: start=0 for first page, start+=length for each subsequent page

    start_date (ISO date) narrows the search window within the cycle — used by
    incremental runs to fetch only the current calendar year. Defaults to the
    cycle's own start date.
    """
    from urllib.parse import urlencode

    cycle_start, end_date = parse_cycle_dates(cycle_id_str)
    start_date = start_date or cycle_start

    query_params = {
        "CommiteeReportId": "",
        "CategoryType":     category_type,
        "JurisdictionId":   "0",
        "CycleId":          cycle_id_str,
        "StartDate":        start_date,
        "EndDate":          end_date,
        "FilerName":        "",
        "FilerId":          "",
        "BallotName":       "",
        "BallotMeasureId":  "",
        "FilerTypeId":      filer_type_id,
        "OfficeTypeId":     "",
        "OfficeId":         "",
        "PartyId":          "",
        "ContributorName":  "",
        "VendorName":       "",
        "StateId":          "",
        "City":             "",
        "Employer":         "",
        "Occupation":       "",
        "CandidateName":    "",
        "CandidateFilerId": "",
        "Position":         "Support",
        "LowAmount":        "",
        "HighAmount":       "",
    }

    # DataTables POST body — mirrors exactly what the SPA sends
    dt_body: dict[str, str] = {
        "draw":             "1",
        "start":            str(start),
        "length":           str(length),
        "search[value]":    "",
        "search[regex]":    "false",
        "order[0][column]": "0",
        "order[0][dir]":    "asc",
    }
    for i, col in enumerate(_DT_COLUMNS):
        dt_body[f"columns[{i}][data]"]            = col
        dt_body[f"columns[{i}][name]"]            = ""
        dt_body[f"columns[{i}][searchable]"]      = "true"
        dt_body[f"columns[{i}][orderable]"]       = "true"
        dt_body[f"columns[{i}][search][value]"]   = ""
        dt_body[f"columns[{i}][search][regex]"]   = "false"

    url = BASE_URL + "?" + urlencode(query_params)

    text = None
    for attempt in range(3):
        r = session.post(url, data=dt_body, timeout=60)
        r.raise_for_status()
        text = r.text
        if "maxJsonLength" in text:
            raise RuntimeError(f"maxJsonLength exceeded at length={length} — reduce TABLE_LENGTH")
        if text and text != '""':
            break
        time.sleep(5 * (attempt + 1))
    else:
        raise RuntimeError(f"empty response (len={len(text)}) after 3 attempts")

    inner = r.json()
    if not isinstance(inner, dict):
        raise RuntimeError(f"unexpected response type={type(inner)}: {repr(text[:100])}")
    rows  = inner.get("data") or []
    total = int(inner.get("recordsTotal") or inner.get("iTotalRecords") or 0)
    return rows, total


def normalize_tx_row(raw: dict) -> dict:
    return {
        "CommitteeID":    s(raw.get("CommitteeID")),
        "CommitteeName":  s(raw.get("CommitteeName")),
        "TransactionDate": parse_net_date(raw.get("TransactionDate")),
        "Amount":         s(raw.get("Amount")),
        "TransactionName": s(raw.get("TransactionName")),
        "TransactionType": s(raw.get("TransactionType")),
        "Occupation":     s(raw.get("Occupation")),
        "Employer":       s(raw.get("Employer")),
        "City":           s(raw.get("City")),
        "State":          s(raw.get("State")),
        "ZipCode":        s(raw.get("ZipCode")),
        "FirstName":      s(raw.get("FirstName")),
        "LastName":       s(raw.get("LastName")),
        "FilerName":      s(raw.get("FilerName")),
        "Memo":           s(raw.get("Memo")),
    }


def download_one_api(log, session: req_lib.Session,
                     cycle_label: str, cycle_id_str: str,
                     filer_type_id: str, filer_type_label: str,
                     category_type: str,
                     year_floor: str | None = None) -> tuple[str, int] | None:
    """Download one cycle/filer/category via JSON API. Returns (filename, row_count) or None.

    year_floor (ISO date, e.g. "2026-01-01") — fetch only transactions dated on
    or after this date and merge them with the existing file's earlier rows.
    Used by normal incremental runs to refresh just the current calendar year
    without re-downloading (or losing) the prior year of the two-year cycle.
    """
    filename = f"{category_type}_{cycle_label}_{filer_type_label}.csv"
    out_path = RAW_DIR / filename
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    t0 = time.perf_counter()
    log.file_download_start(filename=filename)

    if year_floor and not out_path.exists():
        year_floor = None   # nothing to merge with — fetch the full cycle

    # Rows from the existing file that predate the refresh window — kept as-is
    preserved = []
    if year_floor:
        with open(out_path, newline="", encoding="utf-8") as fh:
            preserved = [r for r in csv.DictReader(fh)
                         if (r.get("TransactionDate") or "") < year_floor]

    new_rows = 0   # rows written from this fetch
    fetched  = 0   # raw rows returned by the API (drives pagination)
    start = 0
    worker_session = session
    try:
        # Write to a temp file and replace on success so a mid-download failure
        # can't truncate an existing good file (matters for the merge path).
        with open(tmp_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=TX_COLS,
                                    extrasaction="ignore", restval="")
            writer.writeheader()
            writer.writerows(preserved)

            while True:
                try:
                    rows, total = fetch_transactions_page(worker_session, cycle_id_str,
                                                          category_type, filer_type_id,
                                                          start, TABLE_LENGTH,
                                                          start_date=year_floor)
                except RuntimeError as e:
                    if "empty response" in str(e):
                        # Session likely expired — rebuild and retry this page once
                        log.info(f"    {filename}: session expired at offset {start}, rebuilding...")
                        worker_session = build_session()
                        time.sleep(10)
                        rows, total = fetch_transactions_page(worker_session, cycle_id_str,
                                                              category_type, filer_type_id,
                                                              start, TABLE_LENGTH,
                                                              start_date=year_floor)
                    else:
                        raise

                if not rows:
                    break

                for raw in rows:
                    rec = normalize_tx_row(raw)
                    # Client-side guard — don't trust the server to honor
                    # StartDate; without this a merge could duplicate rows
                    # already preserved from the existing file.
                    if year_floor and rec["TransactionDate"] < year_floor:
                        continue
                    writer.writerow(rec)
                    new_rows += 1
                fetched += len(rows)

                if start == 0:
                    log.info(f"    {filename}: {total:,} total records")

                if fetched >= total or len(rows) < TABLE_LENGTH:
                    break   # last page

                start += TABLE_LENGTH
                time.sleep(0.1)

    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        log.file_download_error(filename=filename, error=str(e))
        return None

    total_rows = len(preserved) + new_rows
    if total_rows == 0:
        tmp_path.unlink(missing_ok=True)
        log.info(f"  {filename}: no records, skipping")
        return filename, 0

    tmp_path.replace(out_path)
    log.file_download_ok(filename=filename,
                         bytes=out_path.stat().st_size,
                         rows=total_rows,
                         duration_s=round(time.perf_counter() - t0, 2))
    return filename, total_rows


def load_manifest() -> set[tuple[str, str, str]]:
    if not MANIFEST.exists():
        return set()
    done = set()
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            if "filer_type" in row and row["filer_type"]:
                filer = row["filer_type"]
            else:
                parts = row.get("filename", "").replace(".csv", "").split("_")
                filer = parts[2] if len(parts) >= 3 else ""
            done.add((row["cycle_label"], filer, row["category_type"]))
    return done


def strip_manifest(keep_fn):
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict):
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(record)


PARALLEL_WORKERS = 4   # concurrent download threads; raise if server doesn't rate-limit


def download_transactions(log, session: req_lib.Session, done: set,
                          force: bool = False,
                          start_year: int | None = None,
                          end_year: int | None = None,
                          categories: list[str] | None = None,
                          _counts: list | None = None) -> tuple[int, int]:
    """Download transaction CSVs via JSON API. Returns (ok, err).

    Downloads PARALLEL_WORKERS combinations simultaneously; each thread gets its
    own session so there are no shared-state issues.  Manifest writes are
    serialised with a lock.

    Scope:
      - Normal run (no force, no year range): only the cycle(s) covering
        today's date, restricted to transactions from Jan 1 of the current
        year onward (merged into the existing cycle file). Historical cycles
        are immutable and never touched.
      - force: every cycle, full date range.
      - start_year / end_year: numeric cycle labels in range, full date range.
        Non-numeric cycles (e.g. Recall_Fann) are skipped when a range is active.

    categories filters which category types to download (e.g. ["Income"]).
    """
    active_categories = categories or CATEGORY_TYPES

    year_range_active = start_year is not None or end_year is not None
    incremental = not force and not year_range_active
    # Date floor for incremental refresh — only current-calendar-year rows are
    # re-fetched; the prior year of the cycle is preserved from the file on disk.
    year_floor = f"{datetime.today().year}-01-01" if incremental else None

    cycles = []
    for lbl, cid in CYCLES:
        if year_range_active:
            try:
                cycle_year = int(lbl)
            except ValueError:
                continue   # skip Recall_Fann etc. when year range is active
            if start_year is not None and cycle_year < start_year:
                continue
            if end_year is not None and cycle_year > end_year:
                continue
        elif incremental and not cycle_is_current(cid):
            continue   # normal run — historical cycles never re-fetched
        cycles.append((lbl, cid))

    # Build the full work list. Current cycles are never skipped via the
    # manifest — their files are updated in place by the source.
    tasks = []
    for cycle_label, cycle_id_str in cycles:
        for filer_type_id, filer_type_label in FILER_TYPES:
            for category_type in active_categories:
                key = (cycle_label, filer_type_label, category_type)
                if key in done and not cycle_is_current(cycle_id_str):
                    log.file_download_skip(
                        filename=f"{category_type}_{cycle_label}_{filer_type_label}.csv")
                else:
                    tasks.append((cycle_label, cycle_id_str,
                                  filer_type_id, filer_type_label, category_type))

    ok = err = 0
    manifest_lock = threading.Lock()

    def _run_one(task):
        cycle_label, cycle_id_str, filer_type_id, filer_type_label, category_type = task
        worker_session = build_session()   # each thread owns its session
        return download_one_api(log, worker_session, cycle_label, cycle_id_str,
                                filer_type_id, filer_type_label, category_type,
                                year_floor=year_floor)

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
        futures = {pool.submit(_run_one, t): t for t in tasks}
        for future in as_completed(futures):
            cycle_label, _, _, filer_type_label, category_type = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                log.file_download_error(
                    filename=f"{category_type}_{cycle_label}_{filer_type_label}.csv",
                    error=str(exc))
                err += 1
            else:
                if result is None:
                    err += 1
                else:
                    filename, row_count = result
                    if row_count > 0:
                        ok += 1
                        key = (cycle_label, filer_type_label, category_type)
                        with manifest_lock:
                            append_manifest({
                                "cycle_label":   cycle_label,
                                "filer_type":    filer_type_label,
                                "category_type": category_type,
                                "filename":      filename,
                                "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                                "row_count":     row_count,
                            })
                            done.add(key)

            if _counts is not None:
                _counts[0] = ok
                _counts[1] = err

    return ok, err


# ========================= committee details ==========================

def load_detail_done() -> set[str]:
    out = RAW_DIR / "az_committee_details.csv"
    if not out.exists():
        return set()
    with open(out, newline="") as f:
        return {row["entity_id"] for row in csv.DictReader(f)}


def load_entity_ids() -> list[str]:
    reg = RAW_DIR / "az_committees_all.csv"
    if not reg.exists():
        reg = RAW_DIR / "az_committees.csv"
    if not reg.exists():
        print("[!] No committee registry found — run without --entities first")
        sys.exit(1)
    ids = []
    with open(reg, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = (row.get("entity_id") or "").strip()
            if eid:
                ids.append(eid)
    return sorted(set(ids), key=lambda x: int(x) if x.isdigit() else 0)


def extract_detail_fields(entity_id: str, info: dict) -> dict:
    row = {col: "" for col in DETAIL_COLS}
    row["entity_id"] = entity_id
    if not info:
        return row
    row["committee_name"]      = s(info.get("CommitteeName"))
    row["committee_type_name"] = s(info.get("CommitteeTypeName"))
    row["status"]              = s(info.get("Status"))
    row["registration_date"]   = parse_net_date(info.get("RegistrationDate"))
    row["last_amended_date"]   = parse_net_date(info.get("LastAmendedDate"))
    row["last_filed_date"]     = parse_net_date(info.get("LastFiledDate"))
    row["phone"]               = s(info.get("PhoneNo"))
    row["email"]               = s(info.get("Email"))
    row["mailing_address"]     = s(info.get("CommitteeAddress"))
    row["filer_address"]       = s(info.get("FilerAddress"))
    row["city"]                = s(info.get("City"))
    row["state"]               = s(info.get("State"))
    row["zip"]                 = s(info.get("Zip") or info.get("ZipCode"))
    row["county"]              = s(info.get("County"))
    row["chairman_name"]       = s(info.get("ChairmanName"))
    row["treasurer_name"]      = s(info.get("TreasurerName"))
    row["master_committee_id"] = str(info.get("MasterCommitteeId") or "")
    return row


def download_committee_details(log, session: req_lib.Session,
                               force: bool = False) -> tuple[int, int]:
    """Fetch detail pages for all known entity IDs. Returns (ok, err)."""
    out        = RAW_DIR / "az_committee_details.csv"
    entity_ids = load_entity_ids()
    done       = set() if force else load_detail_done()
    todo       = [eid for eid in entity_ids if eid not in done]

    log.info(f"Committee details: {len(entity_ids)} total, {len(done)} done, {len(todo)} to fetch")
    if not todo:
        log.info("Nothing to do.")
        return 0, 0

    ok = err = 0
    t0 = time.perf_counter()

    with open(out, "w" if force else "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=DETAIL_COLS, extrasaction="ignore")
        if force or not out.exists():
            writer.writeheader()

        for i, eid in enumerate(todo, 1):
            params = {
                "Page": "11", "startYear": "2025", "endYear": "2025",
                "JurisdictionId": "0", "TablePage": "1", "TableLength": "10",
                "Name": f"2~{eid}",
            }
            try:
                r = session.post(f"{REPORTING_URL}/GetDetailedInformation",
                                 params=params, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    info = (data.get("ReportFilerInfo") or {}) if isinstance(data, dict) else {}
                    writer.writerow(extract_detail_fields(eid, info))
                    ok += 1
                else:
                    log.page_scrape_error(entity="committee", page_id=eid,
                                          error=f"HTTP {r.status_code}")
                    err += 1
            except Exception as e:
                log.page_scrape_error(entity="committee", page_id=eid, error=str(e))
                err += 1

            if i % 100 == 0:
                log.info(f"  {i}/{len(todo)} ({100*i/len(todo):.0f}%)  "
                         f"fetched={ok}  errors={err}")

            time.sleep(0.15)

    log.page_scrape_complete(filename=str(out), rows=ok,
                             duration_s=round(time.perf_counter() - t0, 2),
                             ok=ok, err=err)
    return ok, err


# ============================= orchestrator ===========================

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
    """Download Arizona campaign finance data from SeeTheMoney.

    Vertical scope (mutually exclusive):
        No flags                — current calendar year only: re-fetch this
                                  year's transactions and merge into the active
                                  cycle's file; historical cycles untouched
        force=True              — re-download everything, wipe manifest
        start_year / end_year   — restrict cycle downloads to this range
                                  (non-numeric cycles like Recall_Fann skipped when active)

    Horizontal scope:
        No flags                — download everything
        transactions            — all cycle files (Income + Expenditures)
        entities                — registry + committee details
        contributions           — Income cycle files only
        expenditures            — Expenditures cycle files only
        candidates              — registry only (no committee details sweep)
        committees              — registry + committee details
    """
    log = get_logger("arizona", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    # ── Resolve granular scope ────────────────────────────────────────
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    do_transactions = no_horizontal or transactions or contributions or expenditures
    do_registry     = no_horizontal or entities or candidates or committees
    do_details      = no_horizontal or entities or committees

    # Category filter for transactions
    if contributions and not expenditures:
        active_categories = ["Income"]
    elif expenditures and not contributions:
        active_categories = ["Expenditures"]
    else:
        active_categories = None   # all

    files_ok = files_err = pages_ok = pages_err = 0
    _tx_counts = [0, 0]

    try:
        if force and do_transactions and MANIFEST.exists():
            MANIFEST.unlink()

        elif (start_year is not None or end_year is not None) and do_transactions:
            # Year range — wipe manifest entries within the range so they re-download.
            # Non-numeric cycles (Recall_Fann etc.) are left untouched.
            def _outside_range(r: dict) -> bool:
                """Keep rows that are NOT in the wipe zone."""
                try:
                    cycle_year = int(r["cycle_label"])
                except (ValueError, KeyError):
                    return True   # non-numeric cycle — always keep
                if start_year is not None and cycle_year < start_year:
                    return True   # below range — keep
                if end_year is not None and cycle_year > end_year:
                    return True   # above range — keep
                if active_categories and r.get("category_type") not in active_categories:
                    return True   # different category scope — keep
                return False      # within range and in scope — wipe

            strip_manifest(_outside_range)

        done = load_manifest()
        session = build_session()

        if do_registry:
            log.info("Downloading registry...")
            ok, err = download_registry(log, session)
            pages_ok += ok; pages_err += err

        if do_transactions:
            log.info("Downloading transactions...")
            ok, err = download_transactions(log, session, done,
                                            force=force,
                                            start_year=start_year,
                                            end_year=end_year,
                                            categories=active_categories,
                                            _counts=_tx_counts)
            files_ok += ok; files_err += err

        if do_details:
            log.info("Downloading committee details...")
            ok, err = download_committee_details(log, session, force=force)
            pages_ok += ok; pages_err += err

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)

    except KeyboardInterrupt:
        interrupted_files_ok  = files_ok  or _tx_counts[0]
        interrupted_files_err = files_err or _tx_counts[1]
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=interrupted_files_ok, files_err=interrupted_files_err,
                  pages_ok=pages_ok, pages_err=pages_err)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err,
                  error_type=type(e).__name__, error=str(e))
        raise


# ============================= diagnostic ==============================

def run_diagnostic():
    """Capture all network traffic and page state to debug the AdvancedSearch endpoint."""
    import json
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[!] pip install playwright && playwright install chromium")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page    = context.new_page()

        # Capture ALL requests to the site (GET and POST) via route interceptor
        # page.route gives us reliable POST body access; on("response") does not
        route_log = []
        def handle_route(route):
            req = route.request
            if "seethemoney.az.gov" in req.url:
                route_log.append({
                    "method":  req.method,
                    "url":     req.url,
                    "headers": dict(req.headers),
                    "body":    req.post_data or "",
                })
            route.continue_()
        page.route("**", handle_route)

        # Also capture response bodies
        resp_log = []
        def on_response(resp):
            if "seethemoney.az.gov" in resp.url:
                try:
                    body = resp.text()
                except Exception:
                    body = "[unreadable]"
                resp_log.append({"url": resp.url, "status": resp.status, "body": body[:400]})
        page.on("response", on_response)

        print(f"\n>>> Navigating to {BASE_URL} ...")
        page.goto(BASE_URL, timeout=30_000)
        page.wait_for_load_state("networkidle")
        print(f">>> networkidle — {len(route_log)} request(s) to site\n")

        for i, r in enumerate(route_log):
            rb = resp_log[i] if i < len(resp_log) else {}
            print(f"[{i}] {r['method']} {r['url']}")
            if r['body']:
                print(f"     req_body: {r['body'][:200]}")
            print(f"     resp {rb.get('status','?')}: {repr(rb.get('body',''))[:200]}\n")

        # Dump page JS state
        state = page.evaluate("""() => ({
            hasJquery:    typeof $ !== 'undefined',
            jqVersion:    typeof $ !== 'undefined' ? $.fn.jquery : null,
            ajaxHeaders:  typeof $ !== 'undefined' && $.ajaxSettings && $.ajaxSettings.headers
                          ? JSON.stringify($.ajaxSettings.headers) : null,
            csrfToken:    (document.querySelector('input[name="__RequestVerificationToken"]') || {}).value || null,
            pageUrl:      location.href,
            formCount:    document.querySelectorAll('form').length,
            inputs:       Array.from(document.querySelectorAll('form input,form select'))
                              .map(el => ({ name: el.name, type: el.type, value: (el.value||'').slice(0,40) })),
        })""")
        print(">>> Page JS state:")
        print(json.dumps(state, indent=2))

        # Dump the actual CycleId dropdown options
        print("\n>>> CycleId dropdown options:")
        cycle_opts = page.evaluate("""() =>
            Array.from(document.querySelectorAll('[name="CycleId"] option'))
                .map(o => ({ value: o.value, text: o.textContent.trim() }))
        """)
        for o in cycle_opts:
            print(f"  value={repr(o['value'])}  text={repr(o['text'])}")

        # Dump FilerTypeId dropdown options too
        print("\n>>> FilerTypeId dropdown options:")
        filer_opts = page.evaluate("""() =>
            Array.from(document.querySelectorAll('[name="FilerTypeId"] option'))
                .map(o => ({ value: o.value, text: o.textContent.trim() }))
        """)
        for o in filer_opts:
            print(f"  value={repr(o['value'])}  text={repr(o['text'])}")

        # Pick the first real CycleId option and do a real search via the form
        first_cycle = next((o["value"] for o in cycle_opts if o["value"]), None)
        first_filer = next((o["value"] for o in filer_opts if o["value"]), None)
        print(f"\n>>> Will trigger a real search: CycleId={first_cycle!r}  FilerTypeId={first_filer!r}")

        if first_cycle:
            # --- Strategy A: navigate with pre-filled URL hash ---
            # The SPA reads the hash on load and auto-searches when params are present.
            hash_params = (
                f"JurisdictionId=0|CommiteeReportId=|CategoryType=Income"
                f"|CycleId={first_cycle}"
                f"|StartDate=|EndDate=|FilerName=|FilerId=|BallotName=|BallotMeasureId="
                f"|FilerTypeId={first_filer or ''}|OfficeTypeId=|OfficeId=|PartyId="
                f"|ContributorName=|VendorName=|StateId=|City=|Employer=|Occupation="
                f"|CandidateName=|CandidateFilerId=|Position=Support|LowAmount=|HighAmount="
                f"|TablePage=1|TableLength=10"
            )
            hash_url = BASE_URL + "#" + hash_params
            print(f"\n>>> Strategy A: navigate with hash URL ...")
            print(f"  {hash_url}")

            hash_captured = []
            def on_hash_response(resp):
                if "AdvancedSearch" in resp.url and resp.request.method == "POST":
                    try:
                        body = resp.text()
                    except Exception:
                        body = "[unreadable]"
                    hash_captured.append({
                        "req_body": resp.request.post_data or "",
                        "status":   resp.status,
                        "resp":     body[:600],
                    })
            page.on("response", on_hash_response)
            page.goto(hash_url, timeout=30_000)
            page.wait_for_load_state("networkidle", timeout=20_000)

            print(f"  Captured {len(hash_captured)} AdvancedSearch response(s)")
            for sc in hash_captured:
                print(f"  req_body: {sc['req_body'][:400]}")
                print(f"  status:   {sc['status']}")
                print(f"  resp:     {repr(sc['resp'])}")

            # --- Strategy B: use Playwright .click() on the search button ---
            print(f"\n>>> Strategy B: Playwright click on Search button ...")
            # Dump every button first so we know exactly what's there
            all_btns = page.evaluate("""() =>
                Array.from(document.querySelectorAll('button, input[type=button], input[type=submit], a.btn'))
                    .map(b => ({
                        tag:     b.tagName,
                        type:    b.type || '',
                        id:      b.id   || '',
                        cls:     b.className || '',
                        text:    b.textContent.trim().slice(0, 50),
                        visible: b.offsetParent !== null,
                    }))
            """)
            print(f"  All buttons ({len(all_btns)}):")
            for b in all_btns:
                print(f"    {b}")

            # Search button is id="Search" (confirmed from button dump)
            btn_sel = '#Search'
            print(f"  Using button selector: {btn_sel}")
            if page.locator(btn_sel).count() == 0:
                print("  #Search not found — skipping Strategy B")
                btn_sel = None
            if btn_sel:
                # Reset logs so only click-triggered requests appear
                before_click = len(route_log)

                # Set cycle in form first
                page.select_option('[name="CycleId"]', first_cycle)
                if first_filer:
                    page.select_option('[name="FilerTypeId"]', first_filer)

                # Verify the DOM values actually stuck
                vals = page.evaluate("""() => ({
                    cycleId:    document.querySelector('[name="CycleId"]').value,
                    filerTypeId: document.querySelector('[name="FilerTypeId"]').value,
                    categoryType: document.querySelector('[name="CategoryType"]:checked')?.value,
                })""")
                print(f"  Form values before click: {vals}")

                page.click(btn_sel)
                try:
                    page.wait_for_load_state("networkidle", timeout=10_000)
                except Exception:
                    pass

                new_requests = route_log[before_click:]
                az_requests = [r for r in new_requests if "seethemoney.az.gov" in r["url"]]
                print(f"  {len(az_requests)} seethemoney request(s) triggered by click:")
                for r in az_requests:
                    resp_entry = next((x for x in resp_log if x["url"] == r["url"]), {})
                    print(f"    {r['method']} {r['url']}")
                    if r['body']:
                        print(f"      req_body: {r['body'][:400]}")
                    print(f"      resp {resp_entry.get('status','?')}: {repr(resp_entry.get('body',''))[:300]}")

            # --- Strategy C: raw XHR (fix: pass args as single object) ---
            print(f"\n>>> Strategy C: raw XHR with correct CycleId ...")
            xhr_result = page.evaluate("""async (args) => {
                const body = new URLSearchParams({
                    CommiteeReportId:'', CategoryType:'Income', JurisdictionId:'0',
                    CycleId:    args.cycleId,
                    StartDate:'', EndDate:'',
                    FilerName:'', FilerId:'', BallotName:'', BallotMeasureId:'',
                    FilerTypeId: args.filerTypeId, OfficeTypeId:'', OfficeId:'', PartyId:'',
                    ContributorName:'', VendorName:'', StateId:'', City:'',
                    Employer:'', Occupation:'', CandidateName:'', CandidateFilerId:'',
                    Position:'Support', LowAmount:'', HighAmount:'',
                    TablePage:'1', TableLength:'10',
                });
                return new Promise((resolve) => {
                    const xhr = new XMLHttpRequest();
                    xhr.open('POST', '/Reporting/AdvancedSearch/', true);
                    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded; charset=UTF-8');
                    xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
                    xhr.onload = () => resolve({ status: xhr.status, body: xhr.responseText.slice(0, 600) });
                    xhr.onerror = () => resolve({ error: 'network error' });
                    xhr.send(body.toString());
                });
            }""", {"cycleId": first_cycle, "filerTypeId": first_filer or ""})
            print(f"  status: {xhr_result.get('status')}")
            print(f"  body:   {repr(xhr_result.get('body', ''))}")
        else:
            print(">>> No CycleId options found — cannot trigger search")

        input("\n>>> Press Enter to close the browser...")
        browser.close()


# ================================= CLI ================================
if __name__ == "__main__":
    # Vertical scope (mutually exclusive):
    #   (no flag)                    current calendar year only — re-fetch this year's
    #                                transactions, merge into the active cycle file;
    #                                historical cycles untouched
    #   --start-year / --end-year    restrict to this cycle range (non-numeric cycles skipped)
    #   --force                      wipe manifest, re-download all in scope
    #
    # Horizontal scope:
    #   (no flag)         all types
    #   --transactions    Income + Expenditures cycle files
    #   --entities        registry + committee details
    #   --contributions   Income only
    #   --expenditures    Expenditures only
    #   --candidates      registry only (no committee details)
    #   --committees      registry + committee details
    ap = argparse.ArgumentParser(
        description="Download Arizona campaign finance data from SeeTheMoney."
    )

    # Vertical — mutually exclusive
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest cycle year to download (inclusive). "
                           "Arizona uses 2-year election cycles labeled by their end year "
                           "(e.g. '2016' covers Nov 2014 – Nov 2016), so odd years will "
                           "match no cycle — use the even end-year of the cycle you want.")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest cycle year to download (inclusive, ≤ current year). "
                         "See --start-year note on cycle labeling.")

    # Horizontal — top level
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only (Income + Expenditures)")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (registry + committee details)")

    # Horizontal — second level
    ap.add_argument("--contributions", action="store_true",
                    help="Income cycle files only")
    ap.add_argument("--expenditures",  action="store_true",
                    help="Expenditures cycle files only")
    ap.add_argument("--candidates",    action="store_true",
                    help="registry only (no committee details sweep)")
    ap.add_argument("--committees",    action="store_true",
                    help="registry + committee details")

    # Dev/diagnostic flags
    ap.add_argument("--diag",          action="store_true",
                    help="capture page traffic and test API, then exit")
    ap.add_argument("--test-requests", action="store_true",
                    help="test whether plain requests can reach the API")

    args, _ = ap.parse_known_args()

    if args.diag:
        run_diagnostic()
        sys.exit(0)
    if args.test_requests:
        test_requests_only()
        sys.exit(0)

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
