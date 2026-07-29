"""
scrapers/montana.py — Download Montana campaign finance data from CERS.

Montana's Campaign Electronic Reporting System (CERS, https://cers-ext.mt.gov/
CampaignTracker/public/search) has no bulk export -- the public UI only lets a
user search one candidate or committee for one election year at a time, then
export a single result. The reference R function this scraper replaces
literally drove a Selenium browser through that UI: click the Contributions
tab, spin the election-year picker up/down, click Search, tick each result
row's checkbox one at a time, click Download, repeat across pages.

This scraper instead talks directly to the JSON/text endpoints the CERS
front-end itself calls via AJAX (jQuery DataTables server-side processing).
These endpoints, payloads, and the pipe-delimited schedule export format were
identified from Montana Free Press's open-source "cers-interface" project
(https://github.com/eidietrich/cers-interface), which has scraped this same
site every election cycle through 2026 using this API -- strong evidence it is
still current. IMPORTANT CAVEAT: this sandbox's network egress does not reach
cers-ext.mt.gov, so these endpoints could not be smoke-tested live from here.
Run a small slice locally (e.g. `--start-year 2024 --end-year 2024
--candidates`) and check data/Montana/raw/ before trusting a full backfill.

Flow:
  Phase 1 (entity discovery, parallel over years):
    Per (election_year, entity_type): POST search params (blank except
    electionYear) to establish server-side search state for the session, then
    GET the DataTables results endpoint to list every candidate/committee
    active that year (up to 1,000 -- CERS years have never come close to that
    count per the source project). Saved as candidates_{year}.json /
    committees_{year}.json. Results are deduplicated by (entity_type,
    entity_id) across years, so an entity listed under several election years
    is only fetched once per run.

  Phase 2 (report fetch, batched, parallel over *reports*):
    Entities are handled in batches of ENTITY_BATCH_SIZE. For each batch:
    POST each entity's ID to the report-list endpoint and GET the DataTables
    list of filed reports (reportId, formTypeCode, date range); then push every
    outstanding report in the whole batch through one flat worker pool.

    Parallelising at report level rather than entity level matters: a filer
    with dozens of reports would otherwise pin a single worker for the whole
    run while the other workers idle. It is safe because each report is
    self-contained -- prepareDownloadFileFromSearch takes reportId explicitly
    and needs no session context, and C7/C7E's retrieveReport ->
    financeRepDetailList pair runs start-to-finish inside one task on one
    thread's own session, so two reports' contexts can never interleave.

    For each report, fetch full transaction detail depending on form type:
      - C5 (candidate periodic) / C6 (committee periodic) / C4 (committee
        independent-expenditure-style periodic): POST reportId+scheduleCode
        to prepareDownloadFileFromSearch, then GET downloadFile with the
        params it returns -- response is a pipe-delimited text export with
        a header row (schedule A/C6A/C4A = contributions, B/C6B/C4B =
        expenditures). Streamed line-by-line into list-of-dict rows and saved
        as JSON, preserving the server's own column headers verbatim.
      - C7 (last-minute contribution notice) / C7E (last-minute expenditure
        notice): no bulk export exists for these -- POST retrieveReport to
        set report context, then POST financeRepDetailList once per
        sub-table (individual/committee/loan donors for C7; expendOther for
        C7E) to get the line items directly as JSON. Saved in the server's
        native field names (entityName, datePaid as epoch ms, totalAmt,
        etc.) -- the parser normalizes both shapes into the pipeline schema.

Each candidate/committee's full bundle (metadata + every filed report's
itemized data) is written to one raw JSON file: candidate_{id}.json /
committee_{id}.json. The per-year entity search lists are also saved
(candidates_{year}.json / committees_{year}.json) since they carry registry
fields (office, party, status) the report endpoints don't repeat.

No authentication required. CERS's own election-year picker floors at 2000
(confirmed from the search page's ace_spinner config), so that's the scrape
floor.

Performance notes
─────────────────
An earlier revision of this scraper took multiple days for a full sweep. It
was slow for five compounding reasons, all addressed here:

  1. A brand-new requests.Session() was built for *every* POST+GET pair --
     meaning a fresh TCP connect + TLS handshake for every report list, every
     schedule download, and every C7 sub-table. On a report-heavy entity that
     is dozens of handshakes, each costing more than the query it wrapped.
     Now each worker thread owns one session (thread-local) with an
     HTTPAdapter connection pool, so connections are kept alive and reused.

     Why this is safe: CERS keys search/report context to the session cookie,
     so the original code isolated every lookup in its own session to avoid
     cross-contamination. But contamination only happens if two *concurrent*
     lookups share a cookie jar. Within a single thread, every context-setting
     POST is immediately followed by its dependent GET/POST before the thread
     moves on, so the context is never read by anything but its own setter.
     Threads never share a session. reset_session() rebuilds a thread's
     session if it ever does get into a bad state.

  2. Everything was sequential -- one entity, then one report, then one
     schedule at a time. Work is now spread across a ThreadPoolExecutor
     (PARALLEL_WORKERS, default 6) at *report* granularity, in batches of
     ENTITY_BATCH_SIZE entities. An earlier revision parallelised over entities
     only, which left report-heavy filers as stragglers: one candidate with 48
     reports held a single worker for the entire run. See Phase 2 above for why
     report-level concurrency is still safe against CERS's session state.

  3. Unconditional time.sleep() of 0.1s per report and 0.15s per entity. At
     Montana's volume that alone was hours of pure sleeping. Politeness is
     now enforced by the bounded worker count rather than by blanket sleeps;
     REQUEST_DELAY is available (default 0.0) if the site turns out to
     rate-limit.

  4. No report-level incrementality. Any entity whose electionYear is the
     current year was re-fetched in full on every run, re-downloading every
     report it had ever filed. Reports are now cached from the existing raw
     file and keyed by a fingerprint of (formTypeCode, statusDescr,
     amendedDate, fromDateStr, toDateStr) -- an unchanged report is reused
     from disk, while a new or amended one is re-fetched. This is what turns
     an incremental run from hours into minutes.

  5. Schedule exports (the "big JSON" case) were buffered whole into memory
     as one string, then re-parsed out of a StringIO copy. They are now
     streamed line-by-line straight into csv.DictReader, and raw files are
     written as compact JSON (no indent=2), which cut both write time and
     on-disk size substantially.

A sixth problem showed up only against the live site: read timeouts were being
retried at the adapter level. A read timeout on prepareDownloadFileFromSearch
means CERS is still generating a large export, so retrying it made the server
start over -- one oversized report could burn 4 x 180s and still return
nothing. Read retries are now disabled on the adapter (read=0) and handled
once, deliberately, in fetch_schedule() on a fresh session. Connect failures
and 5xx/429 are still retried blindly, where that is actually appropriate.

Additionally, a report whose fetch *failed* is now marked with a fetchError
key rather than silently recorded as empty, so the next run retries it
instead of caching the failure forever, and an entity with any failed report
is deliberately left out of manifest.csv -- otherwise a past-year filer with
one timed-out report would be skipped forever, since the manifest skip rule
only exempts the current year. Raw files are written atomically (temp file +
os.replace) so an interrupted run can never leave a truncated JSON that the
parser would skip.

Downloads are tracked in manifest.csv keyed by (entity_type, entity_id) --
re-running skips already-fetched entities except ones whose own electionYear
is the current year (they may still be actively filing reports).

Progress visibility
────────────────────
Because a single schedule fetch can legitimately take up to TIMEOUT_PREPARE[1]
(180s) before returning anything -- success or failure -- a report-heavy
entity used to go completely silent between the "N reports to fetch, spread
across M workers" line and its own final summary, with nothing to distinguish
"still working" from "stuck". fetch_report_detail() now logs immediately
before each report's request goes out and again on success, and the batch
report loop in run() logs a running "X/Y reports done so far" line for any
entity with 20+ pending reports, every 5 completions. A wall of timeout
errors with no other output for minutes at a time should no longer happen;
you should instead see a steady trickle of "→ fetching" / "✓ fetched" /
progress lines even while individual slow reports are still in flight.
"""

import csv
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
import config

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Montana" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Montana" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["entity_type", "entity_id", "election_year", "filename",
                 "downloaded_at", "num_reports"]

# ========================= state-specific constants ===================

BASE = "https://cers-ext.mt.gov/CampaignTracker/public"

CANDIDATE_SEARCH_URL = f"{BASE}/searchResults/searchCandidates"
CANDIDATE_LIST_URL   = f"{BASE}/searchResults/listCandidateResults"
COMMITTEE_SEARCH_URL = f"{BASE}/searchResults/searchFinancials"
COMMITTEE_LIST_URL   = f"{BASE}/searchResults/listFinancialCommitteeResults"

CAND_REPORTS_POST_URL = f"{BASE}/publicReportList/retrieveCampaignReports"
CMTE_REPORTS_POST_URL = f"{BASE}/publicReportList/retrieveCommitteeReports"
REPORTS_LIST_URL      = f"{BASE}/publicReportList/listFinanceReports"

RETRIEVE_REPORT_URL  = f"{BASE}/viewFinanceReport/retrieveReport"
DETAIL_LIST_URL      = f"{BASE}/viewFinanceReport/financeRepDetailList"
PREPARE_DOWNLOAD_URL = f"{BASE}/viewFinanceReport/prepareDownloadFileFromSearch"
DOWNLOAD_FILE_URL    = f"{BASE}/viewFinanceReport/downloadFile"

# CERS's own election-year spinner floors here (see search page JS: min:2000)
START_YEAR = 2000

CANDIDATE_SEARCH_DEFAULT = {
    "lastName": "", "firstName": "", "middleInitial": "",
    "electionYear": "", "candidateTypeCode": "", "officeCode": "",
    "countyCode": "", "partyCode": "",
}

COMMITTEE_SEARCH_DEFAULT = {
    "independentExpendSearch": "false", "electioneeringCommSearch": "false",
    "financialSearchType": "EXPEND", "expendSearchTypeCode": "COMMITTEE",
    "expendCanLastName": "", "expendCanFirstName": "", "expendCommitteeName": "",
    "payeeLastName": "", "payeeFirstName": "", "expendPartyCode": "",
    "expendCandidateTypeCode": "", "expendOfficeCode": "", "expendAmountRangeCode": "",
    "electionYear": "", "expendSearchFromDate": "", "expendSearchToDate": "",
}

# Periodic reports with a bulk pipe-delimited export. Value = (contrib code, expend code).
SCHEDULE_CODES = {
    "C5": ("A", "B"),      # candidate periodic report
    "C6": ("C6A", "C6B"),  # committee periodic report
    "C4": ("C4A", "C4B"),  # committee periodic report (independent-expenditure style)
}

# Last-minute notices -- no bulk export; fetched as JSON line items per sub-table.
# The parser only reads individual/committee/loan (C7) and expendOther (C7E), but
# every sub-table is still fetched so the raw capture stays complete and the
# parser can be widened later without a re-scrape. C7/C7E are a small minority
# of reports, so the extra requests are not a meaningful share of runtime.
C7_LIST_NAMES  = ["individual", "committee", "loan", "candidate", "fundraisers", "refunds", "payment"]
C7E_LIST_NAMES = ["expendOther", "candidate", "pettyCash", "debtLoan"]

MAX_ROWS = 1000  # iDisplayLength -- comfortably above any single year's candidate/committee count

# --------------------------- tuning knobs ----------------------------
# Concurrent workers. Each worker owns one keep-alive session, so this is also
# the peak number of open connections to CERS. Raise if the site tolerates it;
# lower to 1 to get fully-sequential behaviour back.
PARALLEL_WORKERS = 6

# Entities are processed in batches: report lists for a batch are fetched
# first, then every report in the batch is fetched through one flat worker pool
# (see run()). Batching bounds peak memory -- a whole backfill's worth of
# itemized rows will not fit in RAM at once -- while still letting a single
# report-heavy filer spread its reports across all workers instead of
# monopolising one. Larger = better straggler smoothing, more memory.
ENTITY_BATCH_SIZE = 25

# Optional politeness delay between requests within a worker. 0.0 by default --
# the bounded worker count is the rate limit. Set to e.g. 0.1 if CERS starts
# returning 429s.
REQUEST_DELAY = 0.0

# Timeouts (connect, read), tiered by how much work the endpoint does.
# prepareDownloadFileFromSearch makes CERS generate a whole export server-side,
# so it is the slow one; downloadFile then streams what it produced.
TIMEOUT_SHORT    = (10, 90)
TIMEOUT_PREPARE  = (10, 180)
TIMEOUT_DOWNLOAD = (10, 300)

# Transient-failure retry policy, applied at the adapter level so it covers
# every request without per-call try/except. POST is included because all of
# these POSTs are read-only lookups despite the verb.
#
# read=0 is deliberate and important. Read timeouts are NOT retried here:
# a read timeout on prepareDownloadFileFromSearch means CERS is still grinding
# out a large export, and retrying makes it start over from scratch. With the
# adapter retrying reads, one big report could burn 4 x 180s and still yield
# nothing, which is exactly how a 48-report filer turned into an all-day stall.
# Read timeouts are instead handled once, deliberately, in fetch_schedule().
# Connect failures and 5xx/429 responses are still worth retrying blindly.
RETRY_TOTAL            = 3
RETRY_BACKOFF_FACTOR   = 0.6
RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)

# Controlled attempts for a schedule fetch. Attempt 2 runs on a fresh session,
# which also covers the case where CERS has quietly invalidated a long-lived
# one. Kept at 2 so a genuinely oversized report fails fast, gets marked
# fetchError, and is retried by the *next run* rather than blocking this one.
SCHEDULE_ATTEMPTS = 2

# Entities with at least this many reports still to fetch get a running
# progress summary logged every PROGRESS_LOG_EVERY completions, instead of
# staying silent until their final per-entity summary. See "Progress
# visibility" in the module docstring.
HEAVY_ENTITY_THRESHOLD = 20
PROGRESS_LOG_EVERY     = 5


# ========================= manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="") as f:
        return {(row["entity_type"], row["entity_id"]) for row in csv.DictReader(f)}


def strip_manifest(keep_fn):
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


_manifest_lock = threading.Lock()


def append_manifest(record: dict):
    """Thread-safe -- called from worker threads as each entity finishes."""
    with _manifest_lock:
        write_header = not MANIFEST.exists()
        with open(MANIFEST, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
            if write_header:
                writer.writeheader()
            writer.writerow(record)


# ========================== request helpers ===========================

_thread_local = threading.local()


def _build_session() -> requests.Session:
    """One keep-alive session with a connection pool and adapter-level retries."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": config.USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    })
    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=0,                 # see RETRY_* notes -- read timeouts are not retried here
        status=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _session() -> requests.Session:
    """Thread-local session.

    Reused across every request this thread makes. Safe with respect to CERS's
    session-scoped search/report state because a thread always issues a
    context-setting POST and its dependent request back-to-back, and no two
    threads ever touch the same session -- see the Performance notes in the
    module docstring.
    """
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = _build_session()
        _thread_local.session = s
    return s


def reset_session() -> requests.Session:
    """Drop this thread's session and build a fresh one (bad server-side state)."""
    old = getattr(_thread_local, "session", None)
    if old is not None:
        try:
            old.close()
        except Exception:
            pass
    _thread_local.session = _build_session()
    return _thread_local.session


def _pause():
    if REQUEST_DELAY:
        time.sleep(REQUEST_DELAY)


def _cache_bust() -> int:
    return int(time.time() * 1000)


def fetch_candidate_list(election_year: str) -> list[dict]:
    """All candidates active in a given election year, across every office."""
    search = CANDIDATE_SEARCH_DEFAULT.copy()
    search["electionYear"] = election_year
    s = _session()
    s.post(CANDIDATE_SEARCH_URL, data=search, timeout=TIMEOUT_SHORT)
    url = (
        f"{CANDIDATE_LIST_URL}?sEcho=1&iColumns=9&sColumns=&iDisplayStart=0"
        f"&iDisplayLength={MAX_ROWS}&mDataProp_0=checked&mDataProp_1=candidateName"
        f"&mDataProp_2=electionYear&mDataProp_3=candidateStatusDescr&mDataProp_4=c3FiledInd"
        f"&mDataProp_5=candidateAddress&mDataProp_6=candidateTypeDescr&mDataProp_7=officeTitle"
        f"&mDataProp_8=resCountyDescr&sSearch=&bRegex=false"
        f"&sSearch_0=&bRegex_0=false&bSearchable_0=true"
        f"&sSearch_1=&bRegex_1=false&bSearchable_1=true"
        f"&sSearch_2=&bRegex_2=false&bSearchable_2=true"
        f"&sSearch_3=&bRegex_3=false&bSearchable_3=true"
        f"&sSearch_4=&bRegex_4=false&bSearchable_4=true"
        f"&sSearch_5=&bRegex_5=false&bSearchable_5=true"
        f"&sSearch_6=&bRegex_6=false&bSearchable_6=true"
        f"&sSearch_7=&bRegex_7=false&bSearchable_7=true"
        f"&sSearch_8=&bRegex_8=false&bSearchable_8=true"
        f"&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1"
        f"&bSortable_0=false&bSortable_1=true&bSortable_2=true&bSortable_3=true"
        f"&bSortable_4=false&bSortable_5=false&bSortable_6=true&bSortable_7=true"
        f"&bSortable_8=true&_={_cache_bust()}"
    )
    r = s.get(url, timeout=TIMEOUT_SHORT)
    r.raise_for_status()
    return r.json().get("aaData", [])


def fetch_committee_list(election_year: str) -> list[dict]:
    """All committees with reported financial activity in a given election year."""
    search = COMMITTEE_SEARCH_DEFAULT.copy()
    search["electionYear"] = election_year
    s = _session()
    s.post(COMMITTEE_SEARCH_URL, data=search, timeout=TIMEOUT_SHORT)
    url = (
        f"{COMMITTEE_LIST_URL}?sEcho=1&iColumns=4&sColumns=&iDisplayStart=0"
        f"&iDisplayLength={MAX_ROWS}&mDataProp_0=checked&mDataProp_1=committeeName"
        f"&mDataProp_2=electionYear&mDataProp_3=committeeTypeDescr&sSearch=&bRegex=false"
        f"&sSearch_0=&bRegex_0=false&bSearchable_0=true"
        f"&sSearch_1=&bRegex_1=false&bSearchable_1=true"
        f"&sSearch_2=&bRegex_2=false&bSearchable_2=true"
        f"&sSearch_3=&bRegex_3=false&bSearchable_3=true"
        f"&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1"
        f"&bSortable_0=false&bSortable_1=true&bSortable_2=true&bSortable_3=true"
        f"&_={_cache_bust()}"
    )
    r = s.get(url, timeout=TIMEOUT_SHORT)
    r.raise_for_status()
    return r.json().get("aaData", [])


def fetch_entity_reports(entity_type: str, entity_id) -> list[dict]:
    """List every report (any form type) filed by one candidate or committee."""
    if entity_type == "candidate":
        post_url = CAND_REPORTS_POST_URL
        payload  = {"candidateId": entity_id, "searchType": "", "searchPage": "public"}
    else:
        post_url = CMTE_REPORTS_POST_URL
        payload  = {"committeeId": entity_id, "financialSearchType": "COMMITTEE",
                    "searchPage": "public"}
    s = _session()
    s.post(post_url, data=payload, timeout=TIMEOUT_SHORT)
    url = (
        f"{REPORTS_LIST_URL}?sEcho=1&iColumns=6&sColumns=&iDisplayStart=0"
        f"&iDisplayLength={MAX_ROWS}&mDataProp_0=checked&mDataProp_1=fromDateStr"
        f"&mDataProp_2=toDateStr&mDataProp_3=formTypeDescr&mDataProp_4=formTypeCode"
        f"&mDataProp_5=statusDescr&sSearch=&bRegex=false"
        f"&sSearch_0=&bRegex_0=false&bSearchable_0=true"
        f"&sSearch_1=&bRegex_1=false&bSearchable_1=true"
        f"&sSearch_2=&bRegex_2=false&bSearchable_2=true"
        f"&sSearch_3=&bRegex_3=false&bSearchable_3=true"
        f"&sSearch_4=&bRegex_4=false&bSearchable_4=true"
        f"&sSearch_5=&bRegex_5=false&bSearchable_5=true"
        f"&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1"
        f"&bSortable_0=false&bSortable_1=true&bSortable_2=true&bSortable_3=true"
        f"&bSortable_4=true&bSortable_5=true&_={_cache_bust()}"
    )
    r = s.get(url, timeout=TIMEOUT_SHORT)
    r.raise_for_status()
    return r.json().get("aaData", [])


def _nonempty(lines):
    """Drop blank lines so csv.DictReader doesn't emit phantom rows."""
    for line in lines:
        if line and line.strip():
            yield line


def _fetch_schedule_once(report_id, schedule_code: str, entity_name: str,
                         session: requests.Session) -> list[dict]:
    payload = {"reportId": report_id, "scheduleCode": schedule_code, "fname": entity_name}
    p = session.post(PREPARE_DOWNLOAD_URL, data=payload, timeout=TIMEOUT_PREPARE)
    p.raise_for_status()
    meta = p.json()
    if "fileName" not in meta:
        # No export was generated -- the report genuinely has no rows on this
        # schedule. Distinct from a failure, which raises and is recorded as
        # a fetchError by the caller.
        return []
    _pause()
    with session.get(DOWNLOAD_FILE_URL, params=meta,
                     timeout=TIMEOUT_DOWNLOAD, stream=True) as r:
        r.raise_for_status()
        if not r.encoding:
            r.encoding = "utf-8"
        reader = csv.DictReader(_nonempty(r.iter_lines(decode_unicode=True)),
                                delimiter="|", quoting=csv.QUOTE_NONE)
        return [row for row in reader]


def fetch_schedule(report_id, schedule_code: str, entity_name: str) -> list[dict]:
    """C5/C6/C4 bulk schedule export -- pipe-delimited text, parsed to list-of-dict
    rows with the server's own column headers preserved verbatim.

    Streamed line-by-line rather than buffered whole: a large report's export is
    the single biggest response this scraper handles, and a
    text -> StringIO -> DictReader path holds two full copies in memory before
    yielding a row.

    Retries at most once, on a fresh session. The adapter deliberately does not
    retry read timeouts (see RETRY_* notes): a slow prepareDownload means CERS is
    still building a large export, and hammering it just makes it restart. One
    clean retry covers a dropped connection or a silently-expired session; past
    that the report is left to the next run rather than stalling this one.
    """
    last_err = None
    for attempt in range(1, SCHEDULE_ATTEMPTS + 1):
        session = _session() if attempt == 1 else reset_session()
        try:
            return _fetch_schedule_once(report_id, schedule_code, entity_name, session)
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            last_err = e
            if attempt < SCHEDULE_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_FACTOR * attempt)
    raise last_err


def fetch_detail_lists(entity_type: str, entity_id, report_id, list_names: list[str]) -> dict:
    """C7/C7E line items -- no bulk export exists, so each sub-table (individual
    donors, committee donors, loans, etc.) is fetched as JSON directly.

    The retrieveReport POST sets report context on this thread's session; the
    financeRepDetailList POSTs that follow read it. They run back-to-back on
    the same thread, so no other lookup can land in between.
    """
    id_field = "candidateId" if entity_type == "candidate" else "committeeId"
    s = _session()
    s.post(RETRIEVE_REPORT_URL,
           data={id_field: entity_id, "reportId": report_id, "searchPage": "public"},
           timeout=TIMEOUT_SHORT)
    out = {}
    for list_name in list_names:
        try:
            r = s.post(DETAIL_LIST_URL, data={"listName": list_name}, timeout=TIMEOUT_SHORT)
            out[list_name] = r.json() if r.text.strip() else []
        except Exception:
            out[list_name] = []
        _pause()
    return out


# ======================== raw file / cache helpers =====================

def _raw_path(entity_type: str, entity_id) -> Path:
    return RAW_DIR / f"{entity_type}_{entity_id}.json"


def write_json_atomic(path: Path, obj) -> int:
    """Compact JSON via temp file + os.replace.

    Atomic so an interrupted run never leaves a truncated file for the parser
    to trip over. Compact (no indent) because indent=2 inflated these files
    several-fold, costing write time, disk, and parse time downstream.
    """
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, separators=(",", ":"), default=str)
    os.replace(tmp, path)
    return path.stat().st_size


def report_fingerprint(rep: dict) -> str:
    """Identity of a report *version*.

    Anything that changes when a filer amends or refiles must be in here, or a
    cached copy would shadow the update. statusDescr and amendedDate are the
    fields CERS moves on an amendment; the date range guards against a reportId
    being reused for a different period.
    """
    return "|".join(str(rep.get(k) or "") for k in
                    ("formTypeCode", "statusDescr", "amendedDate",
                     "fromDateStr", "toDateStr"))


def load_cached_reports(path: Path) -> dict[str, dict]:
    """Map reportId -> previously-fetched report entry from the raw file on disk.

    Entries that recorded a fetchError are excluded so failures are retried
    rather than cached forever.
    """
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict] = {}
    for entry in data.get("reports") or []:
        if not isinstance(entry, dict) or entry.get("fetchError"):
            continue
        rid = str(entry.get("reportId") or "")
        if rid:
            out[rid] = entry
    return out


# ============================ entity fetch =============================

def fetch_report_detail(log, entity_type: str, entity_id, entity_name: str,
                        rep: dict) -> dict:
    """Fetch one report's itemized detail. Returns the entry dict for the bundle."""
    form_type = rep.get("formTypeCode", "")
    report_id = rep.get("reportId")

    entry = {
        "reportId":         report_id,
        "formTypeCode":     form_type,
        "formTypeDescr":    rep.get("formTypeDescr"),
        "fromDateStr":      rep.get("fromDateStr"),
        "toDateStr":        rep.get("toDateStr"),
        "statusDescr":      rep.get("statusDescr"),
        "amendedDate":      rep.get("amendedDate"),
        "fetchFingerprint": report_fingerprint(rep),
        "contributions":    [],
        "expenditures":     [],
    }

    # Logged *before* the request goes out, not only on success/failure. A
    # schedule fetch can legitimately block for up to TIMEOUT_PREPARE[1]
    # (180s) before anything else is heard from this report, so without this
    # line a report that is slowly working and a report that was never picked
    # up by a worker look identical in the log.
    log.debug(f"  \u2192 fetching {entity_type} {entity_id} report {report_id} "
              f"({form_type}, {rep.get('fromDateStr')}\u2013{rep.get('toDateStr')})")

    try:
        if form_type in SCHEDULE_CODES:
            code_a, code_b = SCHEDULE_CODES[form_type]
            entry["contributions"] = fetch_schedule(report_id, code_a, entity_name)
            entry["expenditures"]  = fetch_schedule(report_id, code_b, entity_name)
        elif form_type == "C7":
            entry["contributions_c7"] = fetch_detail_lists(
                entity_type, entity_id, report_id, C7_LIST_NAMES)
        elif form_type == "C7E":
            entry["expenditures_c7e"] = fetch_detail_lists(
                entity_type, entity_id, report_id, C7E_LIST_NAMES)
        else:
            log.warning(f"  Unhandled report type {form_type!r} "
                        f"({entity_type} {entity_id}, report {report_id})")
    except Exception as e:
        # Recorded on the entry so load_cached_reports() refuses to reuse it and
        # the next run retries this report instead of treating it as empty.
        entry["fetchError"] = str(e)
        log.page_scrape_error(entity=entity_type, page_id=f"{entity_id}/{report_id}",
                              error=str(e))
        reset_session()   # a failure may have left session state unusable
    else:
        # Explicit success confirmation -- without this, a quiet run and a
        # hung one both just... show no more lines for this report.
        if form_type in SCHEDULE_CODES:
            log.debug(f"  \u2713 fetched {entity_type} {entity_id} report {report_id}: "
                      f"{len(entry['contributions'])} contribution row(s), "
                      f"{len(entry['expenditures'])} expenditure row(s)")
        elif form_type in ("C7", "C7E"):
            log.debug(f"  \u2713 fetched {entity_type} {entity_id} report {report_id} "
                      f"({form_type})")

    return entry


def plan_entity(log, entity_type: str, entity_data: dict, year: int,
                use_cache: bool = True) -> dict:
    """Phase 2a for one entity: list its reports and decide what actually needs
    fetching.

    Returns a job dict. `slots` holds one entry per report in the order CERS
    listed them -- already filled in for cache hits, left None for reports the
    flat report pool still has to fetch. Keeping the order here means report
    tasks can complete in any order and the bundle still comes out in the
    source's own sequence.
    """
    id_field    = "candidateId" if entity_type == "candidate" else "committeeId"
    name_field  = "candidateName" if entity_type == "candidate" else "committeeName"
    entity_id   = entity_data[id_field]
    entity_name = entity_data.get(name_field, "")

    raw_reports = fetch_entity_reports(entity_type, entity_id)
    cached = load_cached_reports(_raw_path(entity_type, entity_id)) if use_cache else {}

    slots: list[dict | None] = []
    pending: list[tuple[int, dict]] = []
    n_cached = 0

    for idx, rep in enumerate(raw_reports):
        report_id = str(rep.get("reportId") or "")
        prior = cached.get(report_id)
        if prior is not None and prior.get("fetchFingerprint") == report_fingerprint(rep):
            slots.append(prior)
            n_cached += 1
        else:
            slots.append(None)
            pending.append((idx, rep))

    log.debug(f"  {entity_type} {entity_id} ({entity_name}): {len(raw_reports)} reports "
              f"listed, {n_cached} reused from cache, {len(pending)} to fetch")

    return {
        "entity_type": entity_type,
        "entity_id":   entity_id,
        "entity_name": entity_name,
        "entity_data": entity_data,
        "year":        year,
        "slots":       slots,
        "pending":     pending,
        "n_cached":    n_cached,
        "n_fetched":   0,
        "n_errors":    0,
    }


def finalize_job(job: dict) -> tuple[Path, int]:
    """Assemble one entity's bundle from its filled slots and write it."""
    reports = [s for s in job["slots"] if s is not None]
    bundle = {
        "entityType": job["entity_type"],
        **job["entity_data"],
        "reports": reports,
    }
    out_path = _raw_path(job["entity_type"], job["entity_id"])
    size = write_json_atomic(out_path, bundle)
    return out_path, size


# ========================== entity discovery ===========================

def discover_entities(log, years: list[int], do_candidates: bool, do_committees: bool
                      ) -> tuple[dict[tuple[str, str], dict], int, int]:
    """Phase 1 -- fetch and save every year's entity list, deduplicated.

    Returns (entities, files_ok, files_err) where entities maps
    (entity_type, entity_id) -> {"data": row, "year": max election year seen}.

    Year lists are fetched in parallel: for a full 2000-present backfill that's
    ~50 round trips which used to run strictly one after another.
    """
    tasks = []
    for year in years:
        if do_candidates:
            tasks.append(("candidate", year))
        if do_committees:
            tasks.append(("committee", year))

    entities: dict[tuple[str, str], dict] = {}
    files_ok = files_err = 0
    lock = threading.Lock()

    def _fetch_list(task):
        entity_type, year = task
        year_str = str(year)
        if entity_type == "candidate":
            return task, fetch_candidate_list(year_str), f"candidates_{year_str}.json"
        return task, fetch_committee_list(year_str), f"committees_{year_str}.json"

    with ThreadPoolExecutor(max_workers=min(PARALLEL_WORKERS, max(len(tasks), 1))) as pool:
        futures = {pool.submit(_fetch_list, t): t for t in tasks}
        for future in as_completed(futures):
            entity_type, year = futures[future]
            year_str = str(year)
            fname = (f"candidates_{year_str}.json" if entity_type == "candidate"
                     else f"committees_{year_str}.json")
            try:
                _, rows, fname = future.result()
            except Exception as e:
                log.file_download_error(filename=fname, error=str(e))
                files_err += 1
                continue

            if not rows:
                continue

            out_path = RAW_DIR / fname
            size = write_json_atomic(out_path, rows)
            log.file_download_ok(filename=out_path.name, bytes=size,
                                 rows=len(rows), duration_s=0.0)
            files_ok += 1

            id_field = "candidateId" if entity_type == "candidate" else "committeeId"
            with lock:
                for row in rows:
                    entity_id = row.get(id_field)
                    if entity_id is None:
                        continue
                    key = (entity_type, str(entity_id))
                    prior = entities.get(key)
                    # Keep the latest election year an entity appears under, so
                    # the current-year refresh rule below sees it.
                    if prior is None:
                        entities[key] = {"data": row, "year": year}
                    elif year > prior["year"]:
                        entities[key] = {"data": row, "year": year}

    return entities, files_ok, files_err


# ============================== run =================================

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
    workers: int | None = None,
):
    """
    Vertical scope (mutually exclusive):
        force=True             -- re-download all entities in scope, wipe manifest
        start_year / end_year  -- restrict to this election-year range

    Horizontal scope:
        No flags / entities / transactions   -- both candidates and committees
        candidates (alone)                   -- candidates only
        committees (alone)                   -- committees only

    workers -- override PARALLEL_WORKERS for this run.

    Note: contributions/expenditures flags are accepted for interface
    consistency but ignored -- fetching an entity's filed reports always
    yields both contributions and expenditures together, so there's no
    cheaper partial fetch to do. transactions/entities are likewise
    equivalent here since CERS doesn't separate "registry" from "financial
    activity" the way e.g. Arkansas's API does.
    """
    global PARALLEL_WORKERS
    if workers:
        PARALLEL_WORKERS = max(1, workers)

    log = get_logger("montana", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees,
              workers=PARALLEL_WORKERS)

    if candidates and not committees:
        do_candidates, do_committees = True, False
    elif committees and not candidates:
        do_candidates, do_committees = False, True
    else:
        do_candidates, do_committees = True, True

    current_year = datetime.today().year
    range_start  = start_year if start_year is not None else START_YEAR
    years = [y for y in range(range_start, current_year + 1) if end_year is None or y <= end_year]

    year_range_active = start_year is not None or end_year is not None

    if force:
        strip_manifest(lambda r: not (
            (do_candidates and r["entity_type"] == "candidate") or
            (do_committees and r["entity_type"] == "committee")
        ) or int(r.get("election_year") or 0) not in years)
        done = load_manifest()
    elif year_range_active:
        strip_manifest(lambda r: int(r.get("election_year") or 0) not in years)
        done = load_manifest()
    else:
        done = load_manifest()

    files_ok = files_err = 0
    reports_fetched = reports_cached = reports_failed = 0
    entities_skipped = 0
    counter_lock = threading.Lock()

    # force means "trust nothing on disk" -- so the report-level cache is off.
    use_cache = not force

    try:
        # ── Phase 1: discover every entity in scope ──────────────────
        log.info(f"Discovering entities for {len(years)} year(s) "
                 f"({years[0]}–{years[-1]})..." if years else "No years in scope")
        found, ok, err = discover_entities(log, years, do_candidates, do_committees)
        files_ok += ok
        files_err += err

        # ── Build the work list ──────────────────────────────────────
        tasks = []
        for (entity_type, entity_id), info in sorted(found.items()):
            is_current_year = (info["year"] == current_year)
            key = (entity_type, entity_id)
            if key in done and not is_current_year and not force and not year_range_active:
                log.file_download_skip(filename=f"{entity_type}_{entity_id}.json")
                entities_skipped += 1
                continue
            tasks.append((entity_type, entity_id, info))

        log.info(f"{len(found):,} unique entities discovered — "
                 f"{len(tasks):,} to fetch, {entities_skipped:,} already complete. "
                 f"Using {PARALLEL_WORKERS} workers"
                 f"{' (report cache on)' if use_cache else ' (force: report cache off)'}")

        # ── Phase 2: fetch reports, batched, parallel at REPORT level ──
        #
        # Entities are processed in batches. For each batch: fetch every
        # entity's report list, then push *all* of that batch's outstanding
        # reports through one flat worker pool.
        #
        # Report-level (rather than entity-level) parallelism is what keeps a
        # filer with dozens of reports from becoming a straggler that pins one
        # worker for the whole run while the others idle. It is safe because
        # each report is self-contained: prepareDownloadFileFromSearch takes
        # reportId explicitly and needs no session context at all, and C7/C7E's
        # retrieveReport -> financeRepDetailList pair runs start-to-finish inside
        # a single task on one thread's own session, so no two reports' contexts
        # can interleave on the same session.
        entities_done = 0

        for bstart in range(0, len(tasks), ENTITY_BATCH_SIZE):
            batch = tasks[bstart:bstart + ENTITY_BATCH_SIZE]

            # -- 2a: report lists for this batch (parallel over entities) --
            jobs = []
            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
                futs = {pool.submit(plan_entity, log, et, info["data"], info["year"],
                                    use_cache): (et, eid)
                        for et, eid, info in batch}
                for fut in as_completed(futs):
                    entity_type, entity_id = futs[fut]
                    try:
                        jobs.append(fut.result())
                    except Exception as e:
                        # Report list unavailable -- nothing to write for this
                        # entity, and no manifest row, so it is retried next run.
                        log.page_scrape_error(entity=entity_type, page_id=entity_id,
                                              error=f"report list: {e}")
                        with counter_lock:
                            files_err += 1

            # -- 2b: every outstanding report in the batch, one flat pool --
            report_tasks = [(job, idx, rep)
                            for job in jobs for idx, rep in job["pending"]]

            heavy = [j for j in jobs if len(j["pending"]) >= HEAVY_ENTITY_THRESHOLD]
            for j in heavy:
                log.info(f"  {j['entity_type']} {j['entity_id']} ({j['entity_name']}) "
                         f"has {len(j['pending'])} reports to fetch — spread across "
                         f"{PARALLEL_WORKERS} workers. Progress will be logged every "
                         f"{PROGRESS_LOG_EVERY} completions below.")

            if report_tasks:
                def _fetch_report(rt):
                    job, idx, rep = rt
                    return job, idx, fetch_report_detail(
                        log, job["entity_type"], job["entity_id"],
                        job["entity_name"], rep)

                with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
                    futs = [pool.submit(_fetch_report, rt) for rt in report_tasks]
                    for fut in as_completed(futs):
                        try:
                            job, idx, entry = fut.result()
                        except Exception as e:
                            # fetch_report_detail catches per-report failures
                            # itself, so this is unexpected -- log and continue
                            # rather than losing the rest of the batch.
                            log.warning(f"  report task failed unexpectedly: {e}")
                            continue
                        job["slots"][idx] = entry
                        if entry.get("fetchError"):
                            job["n_errors"] += 1
                        else:
                            job["n_fetched"] += 1

                        # Heavy entities (flagged above) can take minutes to
                        # finish and would otherwise go completely silent
                        # between the "N reports to fetch" line and the final
                        # per-entity summary in step 2c. Surface progress as
                        # each of their reports lands so a slow entity reads
                        # as "in progress" rather than "stuck".
                        total_pending = len(job["pending"])
                        if total_pending >= HEAVY_ENTITY_THRESHOLD:
                            done = job["n_fetched"] + job["n_errors"]
                            if done == total_pending or done % PROGRESS_LOG_EVERY == 0:
                                log.info(f"    {job['entity_type']} {job['entity_id']} "
                                         f"({job['entity_name']}): {done}/{total_pending} "
                                         f"reports done so far ({job['n_fetched']} ok, "
                                         f"{job['n_errors']} failed)")

            # -- 2c: write each entity's bundle and record it --
            for job in jobs:
                entity_type, entity_id = job["entity_type"], job["entity_id"]
                try:
                    out_path, size = finalize_job(job)
                except Exception as e:
                    log.page_scrape_error(entity=entity_type, page_id=entity_id,
                                          error=f"write: {e}")
                    with counter_lock:
                        files_err += 1
                    continue

                # Only mark the entity complete in the manifest if every report
                # came back. Otherwise a past-year filer with a timed-out report
                # would be skipped on the next run (the manifest skip rule only
                # exempts the current year) and that report would never be
                # retried. Leaving it out of the manifest means the next run
                # revisits the entity, and the report cache makes that cheap --
                # only the reports that actually failed are re-fetched.
                incomplete = (job["n_errors"] > 0
                              or any(s is None for s in job["slots"]))
                if incomplete:
                    log.warning(f"  {entity_type} {entity_id} ({job['entity_name']}): "
                                f"{job['n_errors']} report(s) failed — not marking "
                                f"complete, will retry next run "
                                f"(a JSON file with the reports that DID succeed was "
                                f"still written to disk)")
                else:
                    append_manifest({
                        "entity_type":   entity_type,
                        "entity_id":     str(entity_id),
                        "election_year": str(job["year"]),
                        "filename":      out_path.name,
                        "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                        "num_reports":   len([s for s in job["slots"] if s is not None]),
                    })
                log.page_scrape_ok(entity=entity_type, page_id=entity_id,
                                   duration_s=0.0,
                                   reports=len([s for s in job["slots"] if s is not None]),
                                   fetched=job["n_fetched"], cached=job["n_cached"],
                                   errors=job["n_errors"], bytes=size)
                with counter_lock:
                    files_ok        += 1
                    reports_fetched += job["n_fetched"]
                    reports_cached  += job["n_cached"]
                    reports_failed  += job["n_errors"]

            entities_done += len(batch)
            log.info(f"  [{entities_done:,}/{len(tasks):,}] entities done "
                     f"({reports_fetched:,} reports fetched, "
                     f"{reports_cached:,} reused from cache, "
                     f"{reports_failed:,} failed)")

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s -- {files_ok} ok, {files_err} errors, "
                 f"{reports_fetched:,} reports fetched, "
                 f"{reports_cached:,} reused, {reports_failed:,} failed")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err,
                  entities_skipped=entities_skipped,
                  reports_fetched=reports_fetched, reports_cached=reports_cached,
                  reports_failed=reports_failed)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  reports_fetched=reports_fetched, reports_cached=reports_cached,
                  reports_failed=reports_failed)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  reports_fetched=reports_fetched, reports_cached=reports_cached,
                  reports_failed=reports_failed,
                  error_type=type(e).__name__, error=str(e))
        raise


# ================================ CLI ================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download Montana campaign finance data from CERS."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all entities in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest election year to download (inclusive)")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest election year to download (inclusive, <= current year)")

    ap.add_argument("--workers", type=int, metavar="N",
                    help=f"concurrent entity fetches (default {PARALLEL_WORKERS})")

    ap.add_argument("--transactions", action="store_true", help="ignored -- see module docstring")
    ap.add_argument("--entities",     action="store_true", help="ignored -- see module docstring")
    ap.add_argument("--contributions", action="store_true", help="ignored -- see module docstring")
    ap.add_argument("--expenditures",  action="store_true", help="ignored -- see module docstring")
    ap.add_argument("--candidates",    action="store_true", help="candidates only")
    ap.add_argument("--committees",    action="store_true", help="committees only")

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
            workers=args.workers,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)