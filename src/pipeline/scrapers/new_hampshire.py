"""
scrapers/new_hampshire.py — Download New Hampshire campaign finance data
from the state's Campaign Finance System (CFS), a JS-only Angular
application at https://cfs.sos.nh.gov/public/cf/downloads.

## How this was found

The Download Data page (https://cfs.sos.nh.gov/public/cf/downloads) is an
Angular 20 single-page app -- the rendered DOM (captured in a saved copy of
the page) shows a table with two rows ("Receipts", "Expenditures"), each
with a clickable link per filing year (2016-2026 as of this writing), but
every link is bound to a (click) handler rather than a real <a href>, and
the app's JS bundle isn't reachable from this build environment (no
outbound network access to cfs.sos.nh.gov -- every fetch attempt, even to
/robots.txt, came back empty; see docs/states/new_hampshire.md Data Notes).
No Playwright/browser automation was used or is needed, though -- the user
captured the real request from their own browser's Network tab and
provided it directly:

    POST https://cfsapi.sos.nh.gov/api/ExportData/GetExportPublicDownloadData
    Content-Type: application/json
    Origin: https://cfs.sos.nh.gov
    Referer: https://cfs.sos.nh.gov/
    body: {"type": "CSV", "filingYear": "2024", "transactionTypeCode": "TCON"}

    -> raw CSV text (confirmed by the user directly, not assumed)

`transactionTypeCode` is "TCON" for the Receipts row and "TEXP" for the
Expenditures row. The user also supplied real sample responses for both
(one filingYear each) plus the site's own "Data Key" PDFs, which is what
parsers/new_hampshire.py's column mapping is built and verified against --
see that module's docstring.

## What this scraper does

For each filing year in scope and each of the two transaction types
(receipts/expenditures), POST the body above and save the raw CSV
response to disk.

There IS a separate candidate roster export -- a different API
(ENTITIES_URL, api/PublicGridDownload/DownloadPublicGridData) behind the
"Search for a Candidate" page at
https://cfs.sos.nh.gov/public/cf/publiccandidate, confirmed directly
against the user's own browser Network tab (copy-as-cURL) plus a real
sample CSV response (2,535 rows). It's fetched as a single flat snapshot
per run (not one-per-year like receipts/expenditures -- the request has no
year parameter), and gives real office/district/county/party/treasurer/
address/status data per candidate that the transaction files alone don't
carry (see ENTITIES_FILTER below and parsers/new_hampshire.py for how it's
merged with the receipts/expenditures-backfilled records). PAC/party
committees still have no separate roster and are still backfilled
entirely from the transaction files (see parsers/new_hampshire.py).

## Year range

The Download Data page listed years 2016 through the current year at
snapshot time (2026-07-21) -- 2016 is treated as the discovered floor.
There's no API-exposed "list of valid years"; MIN_YEAR is hardcoded from
the observed page and MAX_YEAR defaults to the current year. Use
--start-year/--end-year to override if NH extends the floor/ceiling later.

## Incremental updates

The API gives no last-modified signal per (year, type) the way Ohio's
File Transfer Page does, so this scraper uses a simpler rule: every year
strictly before the current year is treated as closed/final and skipped
if already on disk (unless --force or an explicit --start-year/--end-year
covers it); the current year is always re-fetched in full, since filings
for it are still being submitted (same "always refresh the open cycle"
pattern used by scrapers/virginia.py).

## Verified vs assumed

The endpoint, request body shape, and "plain CSV text" response were
confirmed directly against the user's own browser (DevTools Network tab
copy-as-cURL) and real sample responses for both transaction types -- not
guessed.

A live pass initially failed with 403 Forbidden from every request (an
Akamai edge "Access Denied" page, `errors.edgesuite.net`) -- including on
a plain GET of the HTML download page itself, with no cookies ever set.
Confirmed via side-by-side test: `curl_cffi` impersonating Chrome's
TLS/HTTP2 fingerprint got a 200 on the exact same URL/headers/body where
`requests` got 403. So this is TLS/HTTP2 fingerprint blocking at Akamai's
edge, not a missing header, cookie, or CORS/session issue -- `requests`'
handshake is denylisted outright before any application-layer logic runs.
Fix: use `curl_cffi`'s requests-compatible client with
`impersonate="chrome124"` instead of the `requests` library. (The backend
itself, once past the Akamai edge, is Azure App Service -- see the
`ARRAffinity` sticky-session cookie in responses -- which is incidental
and not part of the blocking.)
"""

import csv
import sys
import time
from datetime import date
from pathlib import Path

from curl_cffi import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
import config

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "New Hampshire" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "New Hampshire" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["type", "year", "filename", "bytes", "rows", "downloaded_at"]

# ========================= state-specific constants ===================

BASE_URL = "https://cfsapi.sos.nh.gov/api/ExportData/GetExportPublicDownloadData"

# (transactionTypeCode, slug used in filenames, manifest "type" label)
TRANSACTION_GROUPS = [
    ("TCON", "receipts",     "receipts"),
    ("TEXP", "expenditures", "expenditures"),
]

MIN_YEAR = 2016   # earliest year listed on the live Download Data page (2026-07-21 snapshot)

# ------------------------- candidate roster (entities) ---------------------
#
# A second, separate CFS API confirmed directly against the user's own
# browser Network tab (copy-as-cURL) -- NOT the same endpoint as the
# receipts/expenditures export above. This one drives the "Search for a
# Candidate" grid at https://cfs.sos.nh.gov/public/cf/publiccandidate and
# doubles as a bulk CSV export of that grid when "type":"CSV" is passed in
# the body -- confirmed against a real sample response (2,535 rows,
# filerTypeCode=CAN/accountStatus=FACT, spanning election cycles 2016-2026)
# supplied directly by the user, not assumed. Unlike the receipts/
# expenditures export, this is a single flat snapshot of every active
# candidate filing -- there's no per-year request parameter, so it's fetched
# once per run as one file (see ENTITIES_FILENAME below), not looped per year.
ENTITIES_URL = "https://cfsapi.sos.nh.gov/api/PublicGridDownload/DownloadPublicGridData"

ENTITIES_FILENAME = "entities_candidates.csv"

# The exact filter object captured from the live page's own request (with a
# candidate search and no manual filters applied) -- reused verbatim rather
# than reverse-engineered. "pageSize": 10 is the grid's on-screen page size;
# confirmed it's ignored server-side when "type" is "CSV" (the sample
# response has all 2,535 matching rows, not 10), so no pagination loop is
# needed. "electionCycle" is a fixed CSV of internal cycle ids -- the exact
# string the page itself sends for "no cycle filter selected" (i.e. every
# cycle); NH exposes no lookup for what each id means, so this is carried
# forward unchanged rather than guessed at or reconstructed from year values.
# "accountStatus": "FACT" scopes to active filers only (every sampled row is
# Filer Status=Active); older/withdrawn candidacies under a different status
# code, if any exist, are out of scope here -- unconfirmed, not guessed.
ENTITIES_FILTER = {
    "pageNumber": 1,
    "pageSize": 10,
    "sortBy": "FilerName",
    "sortType": "asc",
    "filerTypeCode": "CAN",
    "filerSearchTypeCode": "CAN",
    "filerSubTypeCode": None,
    "filerName": None,
    "politicalPartyCode": None,
    "officeSought": None,
    "totalRaisedMax": None,
    "totalRaisedMin": None,
    "totalSpentMax": None,
    "totalSpentMin": None,
    "accountStatus": "FACT",
    "officeType": None,
    "electionCycle": ("0,1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,"
                      "21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,"
                      "110,111,112,113,114"),
    "county": None,
    "CommitteeMakingIE": "",
}

ENTITIES_BODY = {
    "publicGridName": "FilingEntitiesPublicGrid",
    "candidateCommitteeSearchFilter": ENTITIES_FILTER,
    "type": "CSV",
    "openInNewTab": False,
}

SESSION_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://cfs.sos.nh.gov",
    "Referer": "https://cfs.sos.nh.gov/",
    # No explicit User-Agent here: curl_cffi's impersonate="chrome124" sets
    # a User-Agent that matches its TLS/HTTP2 fingerprint automatically.
    # Overriding it with config.USER_AGENT would desync the UA string from
    # the actual handshake fingerprint, which is itself a bot-detection
    # signal -- let curl_cffi own the whole browser identity consistently.
}


# ========================= manifest helpers ============================

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


# ========================= download ====================================

def _download_one(session, year: int, type_code: str, dest: Path) -> tuple[int, int]:
    """POST the export request for one (year, type_code) and write the raw
    CSV response to dest. Returns (bytes_written, row_count)."""
    body = {"type": "CSV", "filingYear": str(year), "transactionTypeCode": type_code}
    r = session.post(BASE_URL, json=body, timeout=180)
    r.raise_for_status()
    content = r.content
    dest.write_bytes(content)
    # rows = newline count minus the header row (best-effort; content may use
    # either \n or \r\n -- count \n occurrences, which covers both).
    n_rows = max(content.count(b"\n") - 1, 0)
    return len(content), n_rows


def _download_candidates(session, dest: Path) -> tuple[int, int]:
    """POST the candidate-roster export request (ENTITIES_BODY) and write the
    raw CSV response to dest. Returns (bytes_written, row_count).

    Unlike _download_one's response, this CSV has one extra non-data line
    before the header -- a title/timestamp row, e.g. "FilingEntityDownload
    Download as of 2026-07-23 00:14:40" -- confirmed in the user's real
    sample response, so the row count subtracts 2 (title + header) instead
    of 1."""
    r = session.post(ENTITIES_URL, json=ENTITIES_BODY, timeout=180)
    r.raise_for_status()
    content = r.content
    dest.write_bytes(content)
    n_rows = max(content.count(b"\n") - 2, 0)
    return len(content), n_rows


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
):
    """
    Download New Hampshire campaign finance data from the CFS export API.

    Horizontal scope:
        (no flag)          receipts + expenditures + candidate roster
        --transactions     receipts + expenditures only (no candidate roster)
        --contributions    receipts (TCON) only
        --expenditures     expenditures (TEXP) only
        --entities / --candidates / --committees
                           candidate roster only (ENTITIES_URL -- see module
                           docstring). One request covers every active
                           candidate/committee across all election cycles;
                           there's no year scoping for this file, so
                           --start-year/--end-year/--force don't affect it.
                           NH's roster endpoint only exposes candidate
                           filers (filerTypeCode=CAN) -- --committees is
                           kept as a distinct flag for CLI consistency with
                           other scrapers, but fetches the same file, since
                           a candidate's own committee info (name,
                           treasurer, address) is embedded in each roster
                           row rather than exposed as a separate PAC/party
                           roster. PAC/party committees are still backfilled
                           at parse time from receipts/expenditures only.

    Vertical scope (receipts/expenditures only -- the candidate roster is
    always fetched fresh in full when in scope, see above):
        (no flag)          incremental -- every year before the current
                           year is skipped if already on disk; the
                           current year is always re-fetched
        --start-year YYYY  only fetch years >= YYYY
        --end-year YYYY    only fetch years <= YYYY
        --force            re-fetch every year in scope, current or not
    """
    log = get_logger("new hampshire", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year)

    do_all            = not any([entities, transactions, contributions, expenditures,
                                  candidates, committees])
    do_receipts        = do_all or transactions or contributions
    do_expenditures     = do_all or transactions or expenditures
    do_entities         = do_all or entities or candidates or committees

    cur_year = date.today().year
    lo = start_year if start_year is not None else MIN_YEAR
    hi = end_year   if end_year   is not None else cur_year
    years = list(range(lo, hi + 1))

    today_str = time.strftime("%Y-%m-%d")
    files_ok = files_err = files_skip = 0

    try:
        # Akamai's edge blocks plain `requests` on TLS/HTTP2 fingerprint
        # alone (confirmed: identical URL/headers/body gets 403 from
        # `requests` and 200 from `curl_cffi` impersonating Chrome) -- so
        # use curl_cffi's Chrome impersonation rather than plain requests.
        session = requests.Session(impersonate="chrome124")
        session.headers.update(SESSION_HEADERS)
        # Prime the session with a GET of the download page first, same as
        # a real browser visit -- picks up the ARRAffinity sticky-session
        # cookie the Azure backend uses, so subsequent POSTs land on the
        # same backend instance.
        try:
            priming = session.get(
                "https://cfs.sos.nh.gov/public/cf/downloads",
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                timeout=30,
            )
            log.info(f"  Primed session: GET downloads page -> {priming.status_code}, "
                     f"cookies={list(session.cookies.get_dict().keys())}")
        except Exception as e:
            log.warning(f"  Session priming request failed ({e}); continuing anyway")

        done = {} if force else load_manifest()
        if force:
            strip_manifest(lambda _: False)

        if do_entities:
            # Single flat snapshot, not year-scoped -- always re-fetched in
            # full on every run regardless of --force/--start-year/--end-year
            # (there's no per-year request param on this endpoint, and the
            # roster reflects live filer status, so a stale cached copy is
            # worse than just re-downloading the ~700KB response).
            dest = RAW_DIR / ENTITIES_FILENAME
            log.file_download_start(filename=ENTITIES_FILENAME)
            t_file = time.perf_counter()
            try:
                n_bytes, n_rows = _download_candidates(session, dest)
                log.file_download_ok(filename=ENTITIES_FILENAME, bytes=n_bytes, rows=n_rows,
                                     duration_s=round(time.perf_counter() - t_file, 2))
                upsert_manifest({
                    "type": "candidates", "year": "", "filename": ENTITIES_FILENAME,
                    "bytes": n_bytes, "rows": n_rows, "downloaded_at": today_str,
                }, done)
                files_ok += 1
            except Exception as e:
                log.file_download_error(filename=ENTITIES_FILENAME, error=str(e))
                files_err += 1
            time.sleep(0.3)

        for type_code, slug, label in TRANSACTION_GROUPS:
            if slug == "receipts" and not do_receipts:
                continue
            if slug == "expenditures" and not do_expenditures:
                continue

            for year in years:
                filename = f"{slug}_{year}.csv"
                dest = RAW_DIR / filename

                is_open_cycle = (year >= cur_year)
                prior = done.get(filename)
                already_have = (prior is not None and dest.exists() and dest.stat().st_size > 0)
                if not force and already_have and not is_open_cycle:
                    log.file_download_skip(filename=filename)
                    files_skip += 1
                    continue

                log.file_download_start(filename=filename)
                t_file = time.perf_counter()
                try:
                    n_bytes, n_rows = _download_one(session, year, type_code, dest)
                    log.file_download_ok(filename=filename, bytes=n_bytes, rows=n_rows,
                                         duration_s=round(time.perf_counter() - t_file, 2))
                    upsert_manifest({
                        "type": label, "year": year, "filename": filename,
                        "bytes": n_bytes, "rows": n_rows, "downloaded_at": today_str,
                    }, done)
                    files_ok += 1
                except Exception as e:
                    log.file_download_error(filename=filename, error=str(e))
                    files_err += 1
                time.sleep(0.3)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s -- {files_ok} downloaded, {files_skip} skipped, "
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
        description="Download New Hampshire campaign finance data from the CFS export API."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-fetch every year in scope, current or not")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="only fetch years >= YYYY")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="only fetch years <= YYYY")

    ap.add_argument("--entities",      action="store_true", help="candidate roster only -- see module docstring")
    ap.add_argument("--transactions",  action="store_true", help="receipts + expenditures")
    ap.add_argument("--contributions", action="store_true", help="receipts (TCON) only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures (TEXP) only")
    ap.add_argument("--candidates",    action="store_true", help="same as --entities")
    ap.add_argument("--committees",    action="store_true", help="same as --entities -- see module docstring")

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
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)