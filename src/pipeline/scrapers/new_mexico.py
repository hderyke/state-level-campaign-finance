"""
scrapers/new_mexico.py — Download New Mexico campaign finance data.

Source: the New Mexico Secretary of State Campaign Finance Information System
(CFIS) at https://login.cfis.sos.state.nm.us. Plain HTTPS + JSON — no browser
automation, no Playwright, no authentication.

Two acquisition paths, both bulk (one request each, no page sweeps):

  1. Transactions — the site's Data Download page serves one CSV per year per
     transaction type from a single GET:
         /api/DataDownload/GetCSVDownloadReport
             ?year=YYYY&transactionType=CON|EXP&reportFormat=csv&fileName=...
     CON = "Contributions and Loans", EXP = "Expenditures". Coverage starts at
     2020 — the year CFIS replaced the legacy cfis.state.nm.us system; earlier
     filings live only in the decommissioned site and are not reachable here.

  2. Entities — the Explore screens are backed by JSON endpoints that accept a
     page size and return the whole result set in one shot:
         POST /api///Organization/SearchCandidates    (body: ElectionYear, paging)
         POST /api///Organization/SearchCommittees    (body: ElectionYear, paging)
         GET  /api///Organization/GetOffices          (query: year, paging)
     The tripled slash after /api is what the CFIS front-end itself sends; the
     server normalizes it. It is reproduced verbatim rather than "fixed" so the
     request is byte-identical to one the site is known to accept.

Cookies. The captured browser requests carry two cookies (`TS01dc4fc6`, an F5
BIG-IP ASM session cookie, and `OClmoOot`, a bot-defense token). Neither is an
auth credential and both expire, so nothing is hardcoded here — the session
warms up with a GET to the site root first, which is enough to be issued the
cookies the WAF wants to see on subsequent API calls.

Entity responses are written to raw/ as untouched JSON rather than flattened to
CSV. CFIS is an undocumented internal API whose field names could not be
confirmed against a live response when this scraper was written, so the raw
payload is preserved and all key resolution happens in parsers/new_mexico.py —
a field-name surprise is then a `reparse` away from fixed, not a re-scrape.

Downloads are tracked in manifest.csv — re-running skips already-fetched years
except the current year, which is always re-fetched (CFIS updates the in-progress
year's file in place as new reports are filed).
"""

import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# Make project root importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "New Mexico" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "New Mexico" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "downloaded_at", "row_count"]

# ========================= state-specific constants ===================

BASE = "https://login.cfis.sos.state.nm.us"

DOWNLOAD_API  = f"{BASE}/api/DataDownload/GetCSVDownloadReport"
# Tripled slash is intentional — see module docstring.
CANDIDATE_API = f"{BASE}/api///Organization/SearchCandidates"
COMMITTEE_API = f"{BASE}/api///Organization/SearchCommittees"
OFFICES_API   = f"{BASE}/api///Organization/GetOffices"

# CFIS transaction type codes → local filename stem
TRANSACTION_TYPES = {
    "CON": "contributions",   # contributions and loans received
    "EXP": "expenditures",    # expenditures and loan payments
}

# CFIS went live for the 2020 primary; the Data Download page offers 2020 onward
# and nothing earlier. Probing below this just returns empty files.
START_YEAR = 2020

# The CFIS paging contract is "give me everything" — int32 max is what the site's
# own front-end sends for pageSize on these screens.
PAGE_SIZE_ALL = 2147483647

# Entity relation → (raw filename stem, manifest relation_type)
ENTITY_RELATIONS = {
    "candidates": "candidates",
    "committees": "committees",
    "offices":    "offices",
}

REQUEST_HEADERS = {
    "User-Agent":     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0",
    "Accept":         "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":        f"{BASE}/",
    "Origin":         BASE,
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

# ========================= manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    """Return set of (relation_type, year) pairs already downloaded."""
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {(row["relation_type"], row["year"]) for row in csv.DictReader(f)}


def strip_manifest(keep_fn):
    """Rewrite manifest keeping only rows where keep_fn(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict):
    write_header = not MANIFEST.exists() or MANIFEST.stat().st_size == 0
    with open(MANIFEST, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


# ========================== session helpers ==========================

def _make_session() -> requests.Session:
    """Build a session and warm it up so the WAF issues its cookies.

    CFIS sits behind an F5 BIG-IP ASM that expects a `TS…` cookie on API calls.
    A plain GET to the site root is enough to be issued one; the failure is
    tolerated rather than fatal because the API sometimes answers without it and
    a hard failure here would mask the real error from the first data request.
    """
    s = requests.Session()
    s.headers.update(REQUEST_HEADERS)
    try:
        s.get(f"{BASE}/", timeout=60)
    except requests.RequestException:
        pass
    return s


def _decode_response(content: bytes) -> str:
    """Normalize UTF-16 / BOM-prefixed .NET responses to plain UTF-8 text."""
    if content[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return content.decode("utf-16")
    if len(content) > 1 and content[1] == 0:
        return content.decode("utf-16-le")
    if content[:3] == b"\xef\xbb\xbf":
        return content[3:].decode("utf-8")
    return content.decode("utf-8", errors="replace")


def _record_count(payload) -> int:
    """Best-effort row count for a JSON entity payload of unknown envelope.

    CFIS's envelope shape is not documented; this walks the two or three shapes
    it plausibly uses (bare list, {data|Data|results|Results|items|Items: [...]},
    or one level of nesting) purely so the manifest and log get a meaningful
    number. The parser does its own, stricter unwrapping — a wrong guess here
    costs a log line, not data.
    """
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("data", "Data", "results", "Results", "items", "Items",
                    "Table", "records", "Records"):
            inner = payload.get(key)
            if isinstance(inner, list):
                return len(inner)
            if isinstance(inner, dict):
                for k2 in ("items", "Items", "results", "Results", "data", "Data"):
                    if isinstance(inner.get(k2), list):
                        return len(inner[k2])
    return 0


# ========================== transactions =============================

def download_transaction(log, session: requests.Session,
                         type_code: str, year: str) -> tuple[str, int] | None:
    """
    GET one year's bulk CSV for a transaction type. Returns (filename, rows)
    or None on failure.
    """
    label    = TRANSACTION_TYPES[type_code]
    filename = f"{label}_{year}.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    params = {
        "year":            year,
        "transactionType": type_code,
        "reportFormat":    "csv",
        # CFIS echoes fileName back in Content-Disposition; it does not affect
        # which rows are returned, but the endpoint 400s when it is missing.
        "fileName":        f"{type_code}_{year}.csv",
    }

    try:
        resp = session.get(DOWNLOAD_API, params=params, timeout=600)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    text = _decode_response(resp.content)

    # A year with no filings returns a header-only (or empty) body. Writing it
    # anyway keeps the manifest honest — the parser skips zero-byte files.
    out_path.write_text(text, encoding="utf-8")
    row_count = max(text.count("\n") - 1, 0)

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


# ============================= entities ==============================

def download_candidates(log, session: requests.Session,
                        year: str) -> tuple[str, int] | None:
    """POST the candidate search for one election year; save the raw JSON."""
    filename = f"candidates_{year}.json"
    payload = {
        "ElectionYear":      year,
        "Party":             None,
        "OfficeSought":      None,
        "JurisdictionType":  None,
        "Jurisdiction":      None,
        "FinanceType":       None,
        "TransactionType":   None,
        "TransactionAmount": None,
        "DistrictId":        None,
        "IsCompliance":      None,
        "pageNumber":        1,
        "pageSize":          PAGE_SIZE_ALL,
        "sortDir":           "ASC",
        "sortedBy":          "CandidateName",
    }
    return _post_entity(log, session, CANDIDATE_API, payload, filename)


def download_committees(log, session: requests.Session,
                        year: str) -> tuple[str, int] | None:
    """POST the committee search for one election year; save the raw JSON.

    The endpoint takes an (entirely empty) duplicate set of filters on the query
    string in addition to the JSON body — the front-end sends both and the
    server reads the body, so the query string is reproduced as-is.
    """
    filename = f"committees_{year}.json"
    url = (f"{COMMITTEE_API}?electionYear=&party=&committeeType=&transactionType="
           f"&transactionAmount=&ballotQuestions=&stance=&pacType=&status="
           f"&BallotQuestionOnly=")
    payload = {
        "ElectionYear":       year,
        "party":              "",
        "committeeType":      "",
        "transactionType":    "",
        "transactionAmount":  None,
        "IsCompliance":       None,
        "ballotQuestions":    None,
        "stance":             "",
        "pacType":            "",
        "status":             "",
        "BallotQuestionOnly": None,
        "pageNumber":         1,
        "pageSize":           PAGE_SIZE_ALL,
        "sortDir":            "asc",
        "sortedBy":           "",
    }
    return _post_entity(log, session, url, payload, filename)


def download_offices(log, session: requests.Session,
                     year: str) -> tuple[str, int] | None:
    """GET the office list for one election year; save the raw JSON."""
    filename = f"offices_{year}.json"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    params = {
        "jurisdiction":     "",
        "jurisdictionType": "",
        "officeSought":     "",
        "year":             year,
        "district":         "",
        "pageNumber":       1,
        "pageSize":         PAGE_SIZE_ALL,
        "sortDir":          "asc",
        "sortedBy":         "",
    }

    try:
        resp = session.get(OFFICES_API, params=params, timeout=180)
        resp.raise_for_status()
        payload = json.loads(_decode_response(resp.content))
    except (requests.RequestException, ValueError) as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    rows = _record_count(payload)

    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=rows, duration_s=round(time.perf_counter() - t0, 2))
    return filename, rows


def _post_entity(log, session: requests.Session, url: str,
                 payload: dict, filename: str) -> tuple[str, int] | None:
    """Shared POST-and-persist path for the two entity search endpoints."""
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        resp = session.post(url, json=payload, timeout=180)
        resp.raise_for_status()
        data = json.loads(_decode_response(resp.content))
    except (requests.RequestException, ValueError) as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    out_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    rows = _record_count(data)

    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=rows, duration_s=round(time.perf_counter() - t0, 2))
    return filename, rows


# =============================== run =================================

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
    """Download New Mexico CFIS transaction files and/or entity registries.

    Vertical scope (mutually exclusive):
        force=True              — wipe manifest entries in scope, re-download all
        start_year / end_year   — restrict to this year range and re-download it

    Horizontal scope (additive):
        no flags                — everything
        transactions            — contributions + expenditures
        entities                — candidates + committees + offices
        contributions           — contributions only
        expenditures            — expenditures only
        candidates              — candidates only
        committees              — committees only

    Offices are registry context for candidates (they carry the election name and
    candidate count per seat), so they ride along with --candidates as well as
    --entities; there is no separate --offices flag in the standard flag set.
    """
    log = get_logger("new mexico", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    # ── Resolve granular scope ────────────────────────────────────────
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    do_transactions = no_horizontal or transactions or contributions or expenditures
    do_candidates   = no_horizontal or entities or candidates
    do_committees   = no_horizontal or entities or committees
    do_offices      = no_horizontal or entities or candidates

    if contributions and not expenditures:
        active_tx_types = {"CON": TRANSACTION_TYPES["CON"]}
    elif expenditures and not contributions:
        active_tx_types = {"EXP": TRANSACTION_TYPES["EXP"]}
    else:
        active_tx_types = dict(TRANSACTION_TYPES)

    active_entities = {}
    if do_candidates: active_entities["candidates"] = download_candidates
    if do_committees: active_entities["committees"] = download_committees
    if do_offices:    active_entities["offices"]    = download_offices

    # ── Year range ────────────────────────────────────────────────────
    current_year     = datetime.today().year
    current_year_str = str(current_year)
    range_start      = start_year if start_year is not None else START_YEAR
    years = [str(y) for y in range(range_start, current_year + 1)
             if (end_year is None or y <= end_year)]

    year_range_explicit = start_year is not None or end_year is not None
    today = datetime.today().strftime("%Y-%m-%d")

    files_ok = files_err = 0

    try:
        # ── Manifest bookkeeping ──────────────────────────────────────
        # Relation types touched by this run — anything outside the active
        # horizontal scope must survive a --force so an unrelated flag
        # combination can't silently orphan another relation's entries.
        in_scope = set(active_tx_types.values()) | set(active_entities)

        def _year_in_range(row: dict) -> bool:
            try:
                yr = int(row["year"])
            except (ValueError, KeyError):
                return False
            if start_year is not None and yr < start_year:
                return False
            if end_year is not None and yr > end_year:
                return False
            return True

        if force:
            # Wipe in-scope relations; narrow to the year range if one is set.
            strip_manifest(lambda r: not (
                r["relation_type"] in in_scope
                and (not year_range_explicit or _year_in_range(r))
            ))
            done = set()
        elif year_range_explicit:
            strip_manifest(lambda r: not (
                r["relation_type"] in in_scope and _year_in_range(r)
            ))
            done = load_manifest()
        else:
            done = load_manifest()

        session = _make_session()

        # ── Transactions ──────────────────────────────────────────────
        if do_transactions:
            for type_code, label in active_tx_types.items():
                for year in years:
                    key = (label, year)
                    # Current year is always re-fetched: CFIS rewrites the
                    # in-progress year's export as new reports are filed.
                    if (key in done and year != current_year_str
                            and not year_range_explicit):
                        log.file_download_skip(filename=f"{label}_{year}.csv")
                        continue

                    result = download_transaction(log, session, type_code, year)
                    if result is None:
                        files_err += 1
                        continue

                    filename, row_count = result
                    strip_manifest(lambda r, k=key: (r["relation_type"], r["year"]) != k)
                    append_manifest({
                        "relation_type": label,
                        "year":          year,
                        "filename":      filename,
                        "downloaded_at": today,
                        "row_count":     row_count,
                    })
                    done.add(key)
                    files_ok += 1
                    time.sleep(0.5)

        # ── Entities ──────────────────────────────────────────────────
        # Registration data is mutable in a way transaction exports are not (a
        # committee's status or a candidate's district can change mid-cycle), so
        # entity years are always re-fetched rather than skipped on a manifest
        # hit. There are only a handful of years and each is one request.
        for relation, fetch in active_entities.items():
            for year in years:
                result = fetch(log, session, year)
                if result is None:
                    files_err += 1
                    continue

                filename, row_count = result
                key = (relation, year)
                strip_manifest(lambda r, k=key: (r["relation_type"], r["year"]) != k)
                append_manifest({
                    "relation_type": relation,
                    "year":          year,
                    "filename":      filename,
                    "downloaded_at": today,
                    "row_count":     row_count,
                })
                files_ok += 1
                time.sleep(0.3)

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


# ================================ CLI ================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download New Mexico campaign finance data from CFIS."
    )

    # Vertical — mutually exclusive
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all years in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); wipes manifest for range")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")

    # Horizontal — top level
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only (contributions + expenditures)")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (candidates + committees + offices)")

    # Horizontal — second level
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true", help="candidates (+ offices) only")
    ap.add_argument("--committees",    action="store_true", help="committees only")

    args, _ = ap.parse_known_args()   # orc.py may forward flags this state ignores

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
