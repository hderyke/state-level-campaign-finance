"""
scrapers/wisconsin.py — Download Wisconsin campaign finance data.

Source: Sunshine, the Wisconsin Ethics Commission disclosure site
(https://campaignfinance.wi.gov). The site's "Download results" button on each
browse tab calls a JSON-parameterized CSV endpoint:

    GET /api/data-download/transactions?queryParams={"dateFrom":...,"dateTo":...}
    GET /api/data-download/reports?queryParams={...}
    GET /api/data-download/committees?queryParams={}

`queryParams` is a URL-encoded JSON object mirroring the browse-page filter
sidebar. The ones used here:

    dateFrom / dateTo   ISO timestamps, inclusive on both ends
    amountFrom / amountTo   plain numbers (used only to break up huge days)

Row cap
───────
Every download is truncated server-side at 99,999 rows with no warning — a
capped response looks like a complete one. Wisconsin has ~13.1M transactions,
so transactions are pulled in date windows and every response is row-counted:
any window that comes back at the cap is discarded and split (month → halves →
… → single day → amount bands) until each piece is under it. Windows are
disjoint by construction, so the parser can concatenate chunks without
deduplicating 13M IDs.

Committees (the "registrants" tab) fits in one unfiltered download. Reports are
windowed on their *updated-at* date, which is what dateFrom/dateTo filter on
for that endpoint.

Access notes
────────────
  - No authentication. Cloudflare fronts the site and normally lets a plain
    requests GET through with a browser User-Agent. If it starts returning 403
    HTML challenges, copy a `cf_clearance` cookie out of a logged-in browser
    session and export it:  WI_COOKIE="cf_clearance=…"
  - Responses are checked for a CSV content type / header line, so a challenge
    page is treated as a failed download rather than written to disk as data.
  - Full 2008–present backfill is ~1,500–3,000 requests. Incremental runs
    re-fetch only the current year plus any window missing from the manifest.
"""

import csv
import io
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# Make project root importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from config import USER_AGENT

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Wisconsin" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Wisconsin" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [
    "relation_type",    # transactions | reports | committees
    "year",             # year of window_from — lets year flags filter the manifest
    "window_from",      # inclusive start (YYYY-MM-DD); blank for committees
    "window_to",        # inclusive end   (YYYY-MM-DD); blank for committees
    "amount_from",      # amount band lower bound, blank when unused
    "amount_to",        # amount band upper bound, blank when unused
    "filename",
    "downloaded_at",
    "row_count",
    "truncated",        # "1" when the chunk still hit the row cap and could not
                        # be split further — flags a known incomplete window
]

# ========================= source constants ===========================
BASE_URL   = "https://campaignfinance.wi.gov"
API_BASE   = f"{BASE_URL}/api/data-download"
BROWSE_URL = f"{BASE_URL}/browse-data"

# Server-side truncation limit on every CSV download. A response with exactly
# this many data rows is assumed to be capped, not complete.
ROW_CAP = 99_999

# Electronic filing on the current system starts in the mid-2000s; 2008 is the
# first year with consistently complete coverage. Earlier registrations exist
# (some registration dates read 01/01/1978) but transaction data before 2008 is
# sparse — pass --start-year to reach further back.
DEFAULT_START_YEAR = 2008

# Amount bands used only as a last resort, when a single calendar day is over
# the row cap and can't be split by date any further. Bounds are inclusive and
# chosen not to overlap at cent precision, so the union is still disjoint.
# A leading (None, 24.99) catches zero, blank and negative (refund) amounts.
AMOUNT_BANDS: list[tuple[float | None, float | None]] = [
    (None,     24.99),
    (25.00,    99.99),
    (100.00,   249.99),
    (250.00,   999.99),
    (1000.00,  9_999.99),
    (10_000.00, None),
]

# Politeness delay between requests — the endpoint runs a live query per call
REQUEST_SLEEP = 0.4

# Retry schedule for 403 / 429 / 5xx
RETRY_WAITS = (5, 20, 60)


# ========================= manifest helpers ==========================

def _manifest_key(relation: str, w_from: str, w_to: str,
                  a_from: str, a_to: str) -> str:
    """Stable identity for one downloaded chunk."""
    return "|".join((relation, w_from, w_to, a_from, a_to))


def load_manifest() -> dict[str, dict]:
    """Return {chunk_key: row} for chunks already downloaded."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="") as f:
        return {
            _manifest_key(r.get("relation_type", ""), r.get("window_from", ""),
                          r.get("window_to", ""), r.get("amount_from", ""),
                          r.get("amount_to", "")): r
            for r in csv.DictReader(f)
        }


def _write_manifest(rows: list[dict]):
    with open(MANIFEST, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore",
                           restval="")
        w.writeheader()
        w.writerows(rows)


def upsert_manifest(record: dict):
    """Write or overwrite a single manifest row, keyed on the chunk identity."""
    rows: list[dict] = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            rows = list(csv.DictReader(f))
    key = _manifest_key(record["relation_type"], record["window_from"],
                        record["window_to"], record["amount_from"],
                        record["amount_to"])
    rows = [
        r for r in rows
        if _manifest_key(r.get("relation_type", ""), r.get("window_from", ""),
                         r.get("window_to", ""), r.get("amount_from", ""),
                         r.get("amount_to", "")) != key
    ]
    rows.append(record)
    _write_manifest(rows)


def strip_manifest(keep: "callable"):
    """Rewrite the manifest keeping only rows for which keep(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = [r for r in csv.DictReader(f) if keep(r)]
    _write_manifest(rows)


def _drop_chunk_files(relation: str, keep: "callable"):
    """
    Delete raw files for a relation whose manifest row fails keep(). Stale chunk
    files matter here in a way they don't for year-per-file states: a window that
    gets re-split produces different filenames, and leaving the old, wider file
    on disk would double-count its rows at parse time.
    """
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("relation_type") != relation or keep(r):
                continue
            stale = RAW_DIR / (r.get("filename") or "")
            if stale.name and stale.exists():
                stale.unlink()


# ========================== http helpers =============================

def _make_session() -> requests.Session:
    """Session with a browser UA (Cloudflare 403s obvious bots) and optional cookie."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":      USER_AGENT,
        "Accept":          "text/csv,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         f"{BROWSE_URL}/transactions",
        "sec-fetch-site":  "same-origin",
        "sec-fetch-mode":  "cors",
        "sec-fetch-dest":  "empty",
    })
    # Optional escape hatch for Cloudflare challenges — see module docstring
    cookie = os.environ.get("WI_COOKIE", "").strip()
    if cookie:
        s.headers["Cookie"] = cookie
    return s


def _build_url(relation: str, params: dict) -> str:
    """
    Build a data-download URL. The site sends queryParams as URL-encoded JSON
    with no spaces; it rejects nothing else, but matching the site's exact shape
    keeps the request indistinguishable from the download button's.
    """
    payload = json.dumps(params, separators=(",", ":"))
    return f"{API_BASE}/{relation}?queryParams={quote(payload, safe='')}"


def _iso(d: date) -> str:
    """Date → the ISO timestamp shape the site's own filters send."""
    return d.strftime("%Y-%m-%dT00:00:00.000Z")


def _fetch_csv(session: requests.Session, url: str) -> str:
    """
    GET a data-download URL and return the CSV body.

    Raises on a non-CSV response — a Cloudflare challenge or Next.js error page
    is HTML and must never be written to raw/ as if it were data.
    """
    last_err: Exception | None = None
    for attempt, wait in enumerate((0,) + RETRY_WAITS):
        if wait:
            time.sleep(wait)
        try:
            resp = session.get(url, timeout=300)
            if resp.status_code in (403, 429) or resp.status_code >= 500:
                last_err = RuntimeError(
                    f"HTTP {resp.status_code} from {url[:120]}"
                )
                continue
            resp.raise_for_status()
            body = resp.text
            ctype = resp.headers.get("Content-Type", "")
            if "csv" not in ctype.lower() and not body.lstrip().startswith("ID,"):
                raise RuntimeError(
                    f"expected CSV, got Content-Type={ctype!r} "
                    f"body={body[:120]!r}"
                )
            return body
        except requests.RequestException as e:
            last_err = e
    raise RuntimeError(f"request failed after {len(RETRY_WAITS) + 1} attempts: {last_err}")


def _count_rows(body: str) -> int:
    """
    Count CSV data rows. Line counting is wrong here — registrant addresses are
    multi-line and quoted, so a record can span several physical lines.
    """
    # newline="" is required: StringIO otherwise normalizes line endings, which
    # mangles the CR/LF sequences embedded in quoted multi-line address fields
    # and makes csv.reader raise "new-line character seen in unquoted field".
    reader = csv.reader(io.StringIO(body, newline=""))
    try:
        next(reader)          # header
    except StopIteration:
        return 0
    return sum(1 for _ in reader)


# ========================= window generation =========================

def _month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Disjoint calendar-month windows covering [start, end]."""
    out: list[tuple[date, date]] = []
    cur = start.replace(day=1)
    while cur <= end:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        w_from = max(cur, start)
        w_to   = min(nxt - timedelta(days=1), end)
        out.append((w_from, w_to))
        cur = nxt
    return out


def _split_window(w_from: date, w_to: date) -> list[tuple[date, date]]:
    """Halve a multi-day window; returns [] for a single day (use amount bands)."""
    span = (w_to - w_from).days
    if span < 1:
        return []
    mid = w_from + timedelta(days=span // 2)
    return [(w_from, mid), (mid + timedelta(days=1), w_to)]


def _chunk_filename(relation: str, w_from: date, w_to: date,
                    a_from: float | None, a_to: float | None) -> str:
    name = f"{relation}_{w_from.isoformat()}_{w_to.isoformat()}"
    if a_from is not None or a_to is not None:
        lo = "min" if a_from is None else f"{a_from:g}"
        hi = "max" if a_to   is None else f"{a_to:g}"
        name += f"_amt{lo}-{hi}"
    return name + ".csv"


# ========================== chunk download ===========================

def _download_chunk(log, session: requests.Session, relation: str,
                    w_from: date, w_to: date,
                    a_from: float | None = None, a_to: float | None = None,
                    keep_capped: bool = False) -> tuple[str, int, bool]:
    """
    Download one window (optionally amount-banded) and write it to raw/.

    Returns (filename, row_count, capped). `capped` is True when the response
    came back at exactly the server row cap, meaning it is almost certainly
    truncated and the caller should split the window instead of keeping it.

    `keep_capped` writes the chunk anyway, flagged truncated in the manifest —
    used when there is no split left to try.
    """
    params: dict = {"dateFrom": _iso(w_from), "dateTo": _iso(w_to)}
    if a_from is not None:
        params["amountFrom"] = a_from
    if a_to is not None:
        params["amountTo"] = a_to

    filename = _chunk_filename(relation, w_from, w_to, a_from, a_to)
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t_file = time.perf_counter()

    body = _fetch_csv(session, _build_url(relation, params))
    rows  = _count_rows(body)
    capped = rows >= ROW_CAP

    if capped and not keep_capped:
        # Don't keep a chunk we know is short — the caller will split it. An
        # earlier run may have left a file here under this exact name.
        if out_path.exists():
            out_path.unlink()
        return filename, rows, True

    tmp = out_path.with_suffix(".csv.part")
    tmp.write_text(body, encoding="utf-8", newline="")
    tmp.replace(out_path)

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=rows,
        duration_s=round(time.perf_counter() - t_file, 2),
    )
    upsert_manifest({
        "relation_type": relation,
        "year":          str(w_from.year),
        "window_from":   w_from.isoformat(),
        "window_to":     w_to.isoformat(),
        "amount_from":   "" if a_from is None else f"{a_from:g}",
        "amount_to":     "" if a_to   is None else f"{a_to:g}",
        "filename":      filename,
        "downloaded_at": date.today().isoformat(),
        "row_count":     str(rows),
        "truncated":     "1" if capped else "",
    })
    return filename, rows, capped


def _download_amount_bands(log, session: requests.Session, relation: str,
                           day: date) -> tuple[int, int, int]:
    """
    Last-resort split for a single day over the row cap: slice it by amount.

    Returns (files_ok, files_err, rows). A band that is *still* capped is kept
    anyway and flagged truncated="1" in the manifest — better a recorded,
    visible gap than a silently short table.
    """
    ok = err = rows_total = 0
    for a_from, a_to in AMOUNT_BANDS:
        try:
            filename, rows, capped = _download_chunk(
                log, session, relation, day, day, a_from, a_to,
                keep_capped=True,
            )
            time.sleep(REQUEST_SLEEP)
            if capped:
                log.warning(
                    f"{filename}: still at the {ROW_CAP:,}-row cap after amount "
                    f"banding — chunk is incomplete (flagged truncated in manifest)"
                )
            ok += 1
            rows_total += rows
        except Exception as e:
            log.file_download_error(
                filename=_chunk_filename(relation, day, day, a_from, a_to),
                error=str(e),
            )
            err += 1
    return ok, err, rows_total


def download_windowed(log, session: requests.Session, relation: str,
                      start: date, end: date, done: dict[str, dict],
                      refresh_current: bool) -> tuple[int, int]:
    """
    Download a date-partitioned relation (transactions, reports) over [start, end].

    Starts from calendar months and recursively splits any window that returns
    at the row cap. Windows already in the manifest are skipped unless they fall
    in the current year (end-of-period data is still being amended) or the
    caller cleared them.

    Returns (files_ok, files_err).
    """
    queue: list[tuple[date, date]] = _month_windows(start, end)
    queue.reverse()          # pop() from the end → chronological order
    ok = err = 0
    cur_year = date.today().year

    with logging_redirect_tqdm(loggers=[log._log]):
        with tqdm(desc=f"  {relation}", unit="chunk", dynamic_ncols=True,
                  total=len(queue)) as bar:
            while queue:
                w_from, w_to = queue.pop()
                bar.set_postfix_str(f"{w_from} → {w_to}", refresh=False)

                key      = _manifest_key(relation, w_from.isoformat(),
                                         w_to.isoformat(), "", "")
                is_current = w_to.year >= cur_year
                if key in done and not (refresh_current and is_current):
                    existing = RAW_DIR / (done[key].get("filename") or "")
                    if existing.name and existing.exists():
                        log.file_download_skip(filename=existing.name)
                        bar.update(1)
                        continue

                try:
                    _, rows, capped = _download_chunk(
                        log, session, relation, w_from, w_to
                    )
                    time.sleep(REQUEST_SLEEP)
                except Exception as e:
                    log.file_download_error(
                        filename=_chunk_filename(relation, w_from, w_to, None, None),
                        error=str(e),
                    )
                    err += 1
                    bar.update(1)
                    continue

                if not capped:
                    ok += 1
                    bar.update(1)
                    continue

                # Capped → drop the manifest row for this window (it no longer
                # describes a file that exists) and split it.
                strip_manifest(
                    lambda r, k=key: _manifest_key(
                        r.get("relation_type", ""), r.get("window_from", ""),
                        r.get("window_to", ""), r.get("amount_from", ""),
                        r.get("amount_to", "")) != k
                )
                children = _split_window(w_from, w_to)
                if children:
                    log.info(
                        f"  {relation} {w_from} → {w_to} hit the {ROW_CAP:,}-row "
                        f"cap — splitting into {len(children)} windows"
                    )
                    queue.extend(reversed(children))
                    bar.total = (bar.total or 0) + len(children)
                else:
                    log.info(
                        f"  {relation} {w_from} is a single day over the cap — "
                        f"splitting by amount band"
                    )
                    b_ok, b_err, _ = _download_amount_bands(
                        log, session, relation, w_from
                    )
                    ok  += b_ok
                    err += b_err
                bar.update(1)

    return ok, err


def download_committees(log, session: requests.Session) -> tuple[int, int]:
    """
    Download the full registrant list in one unfiltered call.

    This is the "registrants" browse tab; the endpoint is named `committees`.
    It's ~10k rows — well under the cap — and it carries the candidate name,
    registration date, status and party for every filer, so it is always
    re-fetched rather than skipped from the manifest.
    """
    filename = "committees.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t_file = time.perf_counter()
    try:
        body = _fetch_csv(session, _build_url("committees", {}))
        rows = _count_rows(body)
        if rows >= ROW_CAP:
            log.warning(
                f"{filename}: at the {ROW_CAP:,}-row cap — the registrant list "
                f"has outgrown a single download and now needs partitioning"
            )
        tmp = out_path.with_suffix(".csv.part")
        tmp.write_text(body, encoding="utf-8", newline="")
        tmp.replace(out_path)

        log.file_download_ok(
            filename=filename,
            bytes=out_path.stat().st_size,
            rows=rows,
            duration_s=round(time.perf_counter() - t_file, 2),
        )
        upsert_manifest({
            "relation_type": "committees",
            "year":          "",
            "window_from":   "",
            "window_to":     "",
            "amount_from":   "",
            "amount_to":     "",
            "filename":      filename,
            "downloaded_at": date.today().isoformat(),
            "row_count":     str(rows),
            "truncated":     "1" if rows >= ROW_CAP else "",
        })
        return 1, 0
    except Exception as e:
        log.file_download_error(filename=filename, error=str(e))
        return 0, 1


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
    reports: bool = False,
):
    """
    Download Wisconsin transactions, reports and the registrant list.

    Horizontal scope (default = everything):
        transactions                  the transactions endpoint (contributions
                                      and disbursements share one feed, so
                                      --contributions / --expenditures are
                                      accepted but both mean "transactions")
        entities                      registrant list + reports
        candidates / committees       registrant list only
        reports                       report index only

    Vertical scope: --start-year / --end-year / --force bound the windowed
    relations (transactions, reports). The registrant list is unfiltered and is
    refreshed on every run.
    """
    log = get_logger("wisconsin", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions)

    # ── Resolve scope ─────────────────────────────────────────────────
    no_h = not (entities or transactions or contributions or expenditures
                or candidates or committees or reports)

    do_transactions = no_h or transactions or contributions or expenditures
    do_committees   = no_h or entities or candidates or committees
    do_reports      = no_h or entities or reports

    today = date.today()
    start = date(start_year or DEFAULT_START_YEAR, 1, 1)
    end   = date(end_year, 12, 31) if end_year else today
    end   = min(end, today)

    files_ok = files_err = 0

    try:
        session = _make_session()

        # ── Manifest scoping ──────────────────────────────────────────
        # Chunk files are window-named, so anything dropped from the manifest
        # must also be dropped from disk or its rows get counted twice.
        year_range_active = start_year is not None or end_year is not None

        def _outside_range(r: dict) -> bool:
            """True → keep the manifest row (it's outside the requested range)."""
            try:
                yr = int(r["year"])
            except (ValueError, KeyError, TypeError):
                return True          # non-year rows (committees) always kept
            if start_year is not None and yr < start_year:
                return True
            if end_year is not None and yr > end_year:
                return True
            return False

        if force:
            for relation in (["transactions"] if do_transactions else []) + \
                            (["reports"] if do_reports else []):
                _drop_chunk_files(relation, lambda r: False)
                strip_manifest(lambda r, rel=relation: r.get("relation_type") != rel)
        elif year_range_active:
            for relation in (["transactions"] if do_transactions else []) + \
                            (["reports"] if do_reports else []):
                _drop_chunk_files(relation, _outside_range)
            strip_manifest(_outside_range)

        done = load_manifest()

        # ── Transactions ──────────────────────────────────────────────
        if do_transactions:
            log.info(f"Transactions: {start} → {end}")
            ok, err = download_windowed(
                log, session, "transactions", start, end, done,
                # An explicit year range is a request for a refresh; a bare run
                # only re-pulls the current year.
                refresh_current=not year_range_active,
            )
            files_ok  += ok
            files_err += err

        # ── Reports ───────────────────────────────────────────────────
        # dateFrom/dateTo filter the report's updated-at date on this endpoint,
        # so a window holds every report touched in that period — which is
        # exactly what an incremental run wants.
        if do_reports:
            log.info(f"Reports: {start} → {end}")
            ok, err = download_windowed(
                log, session, "reports", start, end, done,
                refresh_current=not year_range_active,
            )
            files_ok  += ok
            files_err += err

        # ── Registrant list ───────────────────────────────────────────
        if do_committees:
            ok, err = download_committees(log, session)
            files_ok  += ok
            files_err += err

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} err")
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
        description="Download Wisconsin campaign finance data from Sunshine."
    )

    # Vertical — mutually exclusive
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all windows in scope, wipe manifest and chunks")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); wipes manifest for range")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")

    # Horizontal — top level
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (registrant list + reports)")

    # Horizontal — second level
    ap.add_argument("--contributions", action="store_true",
                    help="contributions only (same feed as --transactions)")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditures only (same feed as --transactions)")
    ap.add_argument("--candidates",    action="store_true",
                    help="registrant list only")
    ap.add_argument("--committees",    action="store_true",
                    help="registrant list only")
    ap.add_argument("--reports",       action="store_true",
                    help="report index only")

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
            reports=args.reports,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
