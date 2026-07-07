"""
scrapers/michigan.py — Download Michigan campaign finance data.

Two components:

1. Transactions — bulk ZIP downloads from the MiTN CFR export system.
   File list (public, no auth):
     GET page.request.do?page=gov.mi.boe.component.cfrexport.page.cfrexportresults
       &pageSize=200&pageNumber=1&sortDirection=DESC&sortBy=year&type=
     Returns JSON: {"data":{"totalRecords":N,"list":[{"transactiontype","year","download":ID}]}}
   File download (public, no auth):
     GET page.request.do?page=gov.mi.boe.component.cfrexport.page.cfrexportfile&id={ID}
     Returns a ZIP containing a single tab-delimited .txt file.
   Coverage: Contribution, Expenditure, Receipts × year (1997–present, 88 files).
   File IDs are opaque and change when files are refreshed — always fetch the list
   fresh rather than caching IDs in the manifest.

2. Entities — HTMX-based committee search + per-committee detail sweep.
   Requires a JSESSIONID session (obtained by GETting the main search page).
   Search (returns HTML fragments, 100/page):
     POST page.request.do?page=page.miboeCommitteePublicSearch&action=search
     Body: sortColumn=createdOn, sortDirection=desc, perPage=100, currentPage=N,
           option=committee, all form.* fields blank.
   Per-committee detail (returns HTML fragment):
     POST page.request.do?page=page.miboeCommitteePublicSearch&action=showCommitteeDetails
     Body: parameters={"id":<internal_id>}
   The internal entellitrak ID (in hx-vals on each <tr>) differs from the
   displayed Committee ID (cfr_com_id / state_filer_id). We collect both in
   Pass 1 (search pagination → entities_index.csv) then enrich in Pass 2
   (parallel detail sweep → entities.csv). 10,700+ committees total.
   Detail pages add: candidate name, party, office sought, district, county,
   date formed. Sweep runs at 8 workers with checkpoint for resumability.
"""

import csv
import io
import json
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================

RAW_DIR        = PROJECT_ROOT / "data" / "Michigan" / "raw"
MANIFEST       = PROJECT_ROOT / "data" / "Michigan" / "manifest.csv"
ENTITIES_OUT   = RAW_DIR / "entities.csv"
ENTITIES_INDEX = RAW_DIR / "entities_index.csv"
ENTITIES_CKP   = RAW_DIR / "entities.checkpoint"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["transaction_type", "year", "filename", "downloaded_at", "row_count"]

# ========================== constants =================================

BASE_URL   = "https://mi-boe.entellitrak.com/etk-mi-boe-prod"
LIST_URL   = f"{BASE_URL}/page.request.do"
SEARCH_URL = f"{BASE_URL}/page.request.do"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    ),
}

CURRENT_YEAR = datetime.today().year
SLEEP_SEC    = 0.25
WORKERS      = 8

# Human-readable committee type labels for the 10 search type codes (12–21).
# Used in comments only — we search with blank type to get all committees at once.
COMMITTEE_TYPE_CODES = {
    12: "Ballot Question",
    13: "Candidate",
    14: "Independent",
    15: "Caucus",
    16: "Political",
    17: "Independent Expenditure",
    18: "Qualifying Political Party",
    19: "State Political Party",
    20: "County Political Party",
    21: "District Political Party",
}

# ========================= manifest helpers ===========================

def _load_manifest() -> set[tuple[str, str]]:
    """Return set of (transaction_type, year) already downloaded."""
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="") as f:
        return {(r["transaction_type"], r["year"]) for r in csv.DictReader(f)}


def _strip_manifest(keep_fn):
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))


def _append_manifest(record: dict):
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow(record)


# ═══════════════════════════════════════════════════════════════════════
# PART 1 — TRANSACTIONS
# ═══════════════════════════════════════════════════════════════════════

def _fetch_file_list(session: requests.Session) -> list[dict]:
    """
    Fetch the full list of available export files from the MiTN JSON endpoint.
    Returns list of dicts with keys: transactiontype (str), year (int),
    lastupdated (str), download (int — opaque file ID).
    No session required.
    """
    resp = session.get(
        LIST_URL,
        params={
            "page":          "gov.mi.boe.component.cfrexport.page.cfrexportresults",
            "pageSize":      200,
            "pageNumber":    1,
            "sortDirection": "DESC",
            "sortBy":        "year",
            "type":          "",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["data"]["list"]


def _download_zip(session: requests.Session, file_id: int, out_path: Path) -> int:
    """
    Download the ZIP for file_id, extract the inner tab-delimited .txt, write to out_path.
    Returns row count (lines minus header).
    """
    resp = session.get(
        LIST_URL,
        params={
            "page": "gov.mi.boe.component.cfrexport.page.cfrexportfile",
            "id":   file_id,
        },
        timeout=180,
        stream=True,
    )
    resp.raise_for_status()
    content = resp.content

    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            raw = zf.read(zf.namelist()[0])
    except zipfile.BadZipFile:
        raw = content

    # Decode — MiTN delivers UTF-8; handle BOM variants just in case
    if raw[:3] == b"\xef\xbb\xbf":
        text = raw[3:].decode("utf-8", errors="replace")
    elif raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw.decode("utf-16")
    elif len(raw) > 1 and raw[1] == 0:
        text = raw.decode("utf-16-le", errors="replace")
    else:
        text = raw.decode("utf-8", errors="replace")

    out_path.write_text(text, encoding="utf-8")
    return max(text.count("\n") - 1, 0)


def run_transactions(
    log,
    force: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
    contributions: bool = False,
    expenditures: bool = False,
) -> tuple[int, int]:
    """
    Download transaction ZIPs for all years × types.
    Receipts are always included in --transactions unless contributions or
    expenditures is the only flag active, since receipts have the same schema.
    Returns (ok, err).
    """
    current_year_str    = str(CURRENT_YEAR)
    year_range_explicit = start_year is not None or end_year is not None

    session = requests.Session()
    session.headers.update({**HEADERS,
                             "Referer": f"{BASE_URL}/page.request.do?"
                                        "page=gov.mi.boe.component.cfrexport."
                                        "page.cfrexportdownload"})

    # Fetch current file list — IDs change as files refresh so never cache them
    log.info("  Fetching transaction file list …")
    file_list = _fetch_file_list(session)

    # Determine which transaction types to include
    # Receipts are included in --transactions (same schema as contributions)
    # but excluded when only --contributions or --expenditures is active
    if contributions and not expenditures:
        active_types = {"contribution"}
    elif expenditures and not contributions:
        active_types = {"expenditure"}
    else:
        active_types = {"contribution", "expenditure", "receipts"}

    # Build download entries, normalising type to lowercase
    entries = []
    for item in file_list:
        tt   = item["transactiontype"].lower()   # "contribution","expenditure","receipts"
        year = item["year"]                       # int
        fid  = item["download"]                   # int

        if tt not in active_types:
            continue
        if start_year is not None and year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue

        # Normalise filename: "receipt" (singular, drop trailing 's')
        file_label = "receipt" if tt == "receipts" else tt
        filename   = f"{file_label}_{year}.txt"
        entries.append({
            "type":      tt,
            "file_label": file_label,
            "year":      year,
            "id":        fid,
            "filename":  filename,
        })

    # Manifest — keys use file_label so "receipt" matches on re-runs
    if force:
        _strip_manifest(lambda r: False)
        done = set()
    elif year_range_explicit:
        in_range = {str(y) for y in range(
            start_year or 1997, (end_year or CURRENT_YEAR) + 1
        )}
        _strip_manifest(lambda r: r.get("year") not in in_range)
        done = _load_manifest()
    else:
        done = _load_manifest()

    ok = err = 0
    for e in entries:
        key      = (e["file_label"], str(e["year"]))
        filename = e["filename"]
        out_path = RAW_DIR / filename

        if key in done and str(e["year"]) != current_year_str and not year_range_explicit:
            log.file_download_skip(filename=filename)
            continue

        t_file = time.perf_counter()
        log.file_download_start(filename=filename)

        try:
            row_count = _download_zip(session, e["id"], out_path)
        except Exception as ex:
            log.file_download_error(filename=filename, error=str(ex))
            err += 1
            continue

        log.file_download_ok(
            filename=filename,
            bytes=out_path.stat().st_size,
            rows=row_count,
            duration_s=round(time.perf_counter() - t_file, 2),
        )
        _append_manifest({
            "transaction_type": e["file_label"],
            "year":             str(e["year"]),
            "filename":         filename,
            "downloaded_at":    datetime.today().strftime("%Y-%m-%d"),
            "row_count":        row_count,
        })
        done.add(key)
        ok += 1
        time.sleep(0.3)

    return ok, err


# ═══════════════════════════════════════════════════════════════════════
# PART 2 — ENTITIES (committee search + detail sweep)
# ═══════════════════════════════════════════════════════════════════════

# Columns written to entities_index.csv (Pass 1 — search pagination)
INDEX_COLS = [
    "internal_id", "committee_id", "committee_type",
    "committee_name", "committee_status",
]

# Columns written to entities.csv (Pass 2 — detail pages)
ENTITY_COLS = INDEX_COLS + [
    "candidate_last", "candidate_first", "candidate_middle",
    "county", "party", "office_sought", "office_sought_district",
    "date_formed", "scraped_at",
]


# ── Session ────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    """
    GET the main committee search page to establish a JSESSIONID session.
    The entity search endpoint requires a valid session; transactions do not.
    """
    session = requests.Session()
    session.headers.update({
        **HEADERS,
        "Referer": f"{BASE_URL}/page.request.do?page=page.miboeCommitteePublicSearch",
    })
    session.get(
        SEARCH_URL,
        params={"page": "page.miboeCommitteePublicSearch"},
        timeout=30,
    ).raise_for_status()
    return session


# ── Pass 1 — search pagination ─────────────────────────────────────────

def _fetch_search_page(session: requests.Session, page_num: int) -> str:
    """POST the committee search for the given page. Returns raw HTML."""
    data = {
        "sortColumn":                   "createdOn",
        "sortDirection":                "desc",
        "form.committeeId":             "",
        "form.committeeType":           "",   # blank = all types
        "form.committeeStatus":         "",
        "form.committeeName":           "",
        "form.committeeAcronym":        "",
        "form.candidateFirstName":      "",
        "form.candidateMiddleName":     "",
        "form.candidateLastName":       "",
        "form.countyOfResidence":       "",
        "form.party":                   "",
        "form.county":                  "",
        "form.congressionalDistrict":   "",
        "form.officeSought":            "",
        "form.officeSoughtDistrict":    "",
        "form.officeHeld":              "",
        "form.officeHeldDistrict":      "",
        "form.termExpirationDateBegin": "",
        "form.termExpirationDateEnd":   "",
        "form.sponsoringOrganization":  "",
        "perPage":                      "100",
        "option":                       "committee",
        "currentPage":                  str(page_num),
    }
    resp = session.post(
        SEARCH_URL,
        params={"page": "page.miboeCommitteePublicSearch", "action": "search"},
        data=data,
        headers={
            "hx-request": "true",
            "hx-target":  "search-results",
            "hx-trigger": "searchForm",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text


def _parse_search_page(html: str) -> list[dict]:
    """
    Parse one page of committee search results HTML.
    Each <tr aria-rowindex=N> carries the internal ID in one of two places:
      - x-bind:hx-vals="JSON.stringify({ parameters: { id: 23161 } })"  (current)
      - hx-vals='{"parameters":{"id":INTERNAL_ID}}'                     (legacy)
    Four <td> cells follow: Committee ID, Type, Name, Status.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.find_all("tr", attrs={"aria-rowindex": True}):
        # Current: Alpine x-bind:hx-vals with a JS expression
        hx_bind = tr.get("x-bind:hx-vals", "")
        m = re.search(r'id:\s*(\d+)', hx_bind)
        if m:
            internal_id = int(m.group(1))
        else:
            # Legacy: static HTMX hx-vals JSON attribute
            hx_vals_str = tr.get("hx-vals", "{}")
            try:
                internal_id = json.loads(hx_vals_str)["parameters"]["id"]
            except (json.JSONDecodeError, KeyError, TypeError):
                continue

        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        rows.append({
            "internal_id":      str(internal_id),
            "committee_id":     tds[0].get_text(strip=True),
            "committee_type":   tds[1].get_text(strip=True),
            "committee_name":   tds[2].get_text(strip=True),
            "committee_status": tds[3].get_text(strip=True),
        })
    return rows


# ── Pass 2 — detail fetch ──────────────────────────────────────────────

_DASH_RE = re.compile(r"^[\s—\-]+$")


def _parse_detail_html(html: str) -> dict:
    """
    Parse the HTML fragment returned by showCommitteeDetails.
    Structure: <dl><div><dt>Label</dt><dd>Value</dd></div>...</dl>
    We find each <dt> by label text, then take its sibling <dd>.
    Empty values render as &mdash; — filtered by _DASH_RE.
    """
    soup = BeautifulSoup(html, "html.parser")

    def _val(label: str) -> str:
        """Return the <dd> text paired with the <dt> matching label."""
        dt = soup.find("dt", string=re.compile(rf"^\s*{re.escape(label)}\s*$"))
        if dt is None:
            return ""
        dd = dt.find_next_sibling("dd")
        if dd is None:
            return ""
        t = dd.get_text(separator=" ", strip=True)
        if not t or _DASH_RE.match(t):
            return ""
        return t

    return {
        "candidate_last":         _val("Candidate Last Name"),
        "candidate_first":        _val("Candidate First Name"),
        "candidate_middle":       _val("Candidate Middle Name"),
        "county":                 _val("County of Residence"),
        "party":                  _val("Party"),
        "office_sought":          _val("Office Sought"),
        "office_sought_district": _val("Office Sought District"),
        "date_formed":            _val("Date Formed"),
    }


def _fetch_detail(session: requests.Session, internal_id: int | str) -> dict:
    """POST showCommitteeDetails for one committee. Returns parsed detail dict."""
    resp = session.post(
        SEARCH_URL,
        params={"page": "page.miboeCommitteePublicSearch",
                "action": "showCommitteeDetails"},
        data={"parameters": json.dumps({"id": int(internal_id)})},
        headers={
            "hx-request":   "true",
            "hx-target":    "#committeeDetailsContent",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return _parse_detail_html(resp.text)


# ── Entity runner ──────────────────────────────────────────────────────

def run_entities(log, force: bool = False, workers: int = WORKERS) -> tuple[int, int]:
    """
    Two-pass entity sweep for all Michigan committees.
    Pass 1: paginate search results → entities_index.csv
    Pass 2: parallel detail fetch → entities.csv (enriched)
    Checkpoint at entities.checkpoint allows resuming an interrupted Pass 2.
    Returns (found, errors).
    """
    if force:
        for p in [ENTITIES_OUT, ENTITIES_CKP, ENTITIES_INDEX]:
            if p.exists():
                p.unlink()

    # ── Pass 1: Paginate search results ───────────────────────────────
    if ENTITIES_INDEX.exists() and not force:
        with open(ENTITIES_INDEX, newline="", encoding="utf-8") as f:
            index = list(csv.DictReader(f))
        log.info(f"  Loaded existing index: {len(index):,} committees")
    else:
        log.info("  Building committee index (search pagination) …")
        session = _make_session()
        index   = []
        page_num = 1
        while True:
            html = _fetch_search_page(session, page_num)
            rows = _parse_search_page(html)
            if not rows:
                break
            index.extend(rows)
            log.info(f"    Page {page_num}: {len(rows)} rows "
                     f"(total: {len(index):,})")
            page_num += 1
            time.sleep(SLEEP_SEC)

        with open(ENTITIES_INDEX, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=INDEX_COLS)
            w.writeheader()
            w.writerows(index)
        log.info(f"  Index complete: {len(index):,} committees")
        session.close()

    # ── Pass 2: Detail fetch (parallel, resumable) ────────────────────
    # Load checkpoint — set of internal_ids already fetched
    done_ids: set[int] = set()
    if ENTITIES_CKP.exists():
        with open(ENTITIES_CKP, encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s.isdigit():
                    done_ids.add(int(s))

    remaining = [e for e in index if int(e["internal_id"]) not in done_ids]

    if not remaining:
        log.info("  Detail sweep already complete.")
        found = len(index)
        return found, 0

    log.info(f"  Detail sweep: {len(remaining):,} committees "
             f"({len(done_ids):,} already done) …")

    # Establish session; share cookies across threads
    main_session = _make_session()
    main_cookies = dict(main_session.cookies)

    _tl = threading.local()

    def _tl_session() -> requests.Session:
        if not hasattr(_tl, "sess"):
            s = requests.Session()
            s.headers.update({
                **HEADERS,
                "Referer": f"{BASE_URL}/page.request.do?"
                           "page=page.miboeCommitteePublicSearch",
            })
            s.cookies.update(main_cookies)
            _tl.sess = s
        return _tl.sess

    write_lock = threading.Lock()
    ckp_lock   = threading.Lock()

    write_header = not ENTITIES_OUT.exists()
    entities_fh  = open(ENTITIES_OUT, "a", newline="", encoding="utf-8")
    entity_w     = csv.DictWriter(entities_fh, fieldnames=ENTITY_COLS,
                                  extrasaction="ignore")
    if write_header:
        entity_w.writeheader()

    ckp_fh = open(ENTITIES_CKP, "a", encoding="utf-8")

    found = len(done_ids)
    err   = 0
    t0    = time.perf_counter()

    def _process(entry: dict) -> bool:
        iid  = int(entry["internal_id"])
        sess = _tl_session()
        try:
            detail = _fetch_detail(sess, iid)
            time.sleep(SLEEP_SEC)
        except Exception as ex:
            log.page_scrape_error(entity="committee", page_id=iid, error=str(ex))
            return False

        row = {**entry, **detail,
               "scraped_at": datetime.today().strftime("%Y-%m-%d")}
        with write_lock:
            entity_w.writerow(row)
        with ckp_lock:
            ckp_fh.write(f"{iid}\n")
            ckp_fh.flush()
        return True

    try:
        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(total=len(remaining), desc="  committees",
                      unit="id", dynamic_ncols=True) as bar:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = {pool.submit(_process, e): e for e in remaining}
                    for future in as_completed(futures):
                        if future.result():
                            found += 1
                        else:
                            err += 1
                        bar.update(1)
    finally:
        entities_fh.close()
        ckp_fh.close()
        main_session.close()

    log.page_scrape_complete(
        filename=str(ENTITIES_OUT),
        rows=found,
        duration_s=round(time.perf_counter() - t0, 1),
        ok=found - len(done_ids),
        err=err,
    )
    return found, err


# ═══════════════════════════════════════════════════════════════════════
# TOP-LEVEL RUNNER
# ═══════════════════════════════════════════════════════════════════════

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
    workers: int = WORKERS,
):
    """
    Entry point used by orc.py.

    Horizontal scope:
        (no flags)     — download everything (transactions + entities)
        transactions   — contribution + expenditure + receipt ZIPs
        entities       — committee search + detail sweep
        contributions  — contribution ZIPs only
        expenditures   — expenditure ZIPs only
        candidates     — entities only (same as --entities; entity file covers both)
        committees     — entities only

    Year flags apply only to transactions.
    """
    log = get_logger("michigan", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures)

    tx_implied  = contributions or expenditures
    ent_implied = candidates or committees
    do_both         = not (entities or transactions or tx_implied or ent_implied)
    do_transactions = transactions or tx_implied or do_both
    do_entities     = entities or ent_implied or do_both

    files_ok = files_err = 0
    pages_ok = pages_err = 0

    try:
        if do_transactions:
            files_ok, files_err = run_transactions(
                log, force=force,
                start_year=start_year, end_year=end_year,
                contributions=contributions, expenditures=expenditures,
            )

        if do_entities:
            pages_ok, pages_err = run_entities(log, force=force, workers=workers)

        duration = round(time.perf_counter() - t0, 1)
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)

    except KeyboardInterrupt:
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


# =============================== CLI ==================================

if __name__ == "__main__":
    import argparse

    ap   = argparse.ArgumentParser(description="Download Michigan campaign finance data.")
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything, wipe manifest + entity checkpoint")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest transaction year to download (inclusive)")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="latest transaction year to download (inclusive, ≤ current year)")
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only (contributions + expenditures + receipts)")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (committee search + detail sweep)")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="entities only (alias for --entities)")
    ap.add_argument("--committees",    action="store_true",
                    help="entities only (alias for --entities)")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"parallel workers for entity detail sweep (default {WORKERS})")

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
