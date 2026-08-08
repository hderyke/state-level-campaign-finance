"""
scrapers/oregon.py — Download Oregon campaign finance data.

Source: ORESTAR, the Oregon Secretary of State's campaign finance disclosure
system (https://secure.sos.state.or.us/orestar).

Transactions
────────────
The public search form at gotoPublicTransactionSearch.do posts (via a JS
onclick handler, not a real <input type="submit">) to a results endpoint, but
the site's own internal links use a simpler GET action directly:

    GET /orestar/cneSearch.do?cneSearchButtonName=search
        &cneSearchTranStartDate=MM/DD/YYYY&cneSearchTranEndDate=MM/DD/YYYY
        [&cneSearchTranAmountFrom=...&cneSearchTranAmountTo=...]

confirmed by the "Campaign Finance Activity" link on committee detail pages
(sooDetail.do), which points at exactly this URL. It returns an HTML results
page whose "Search Criteria" block echoes back the parsed query — a cheap way
to read the true match count (via the "N records found" text) without paying
for a full export.

Once a search has run, the same browser session can click:

    "Export To Excel Format"  ->  GET /orestar/XcelCNESearch

which exports whatever the last cneSearch.do call matched, as a real (binary,
OLE2 "CDFV2") .xls file — not xlsx. Contributions and expenditures come back
interleaved in one file, distinguished by the "Sub Type" column; the parser
is responsible for splitting them.

The on-page results table is never scraped for data, only used indirectly
via the "N records found" count (see Row cap below) — it renders just 7
columns (Tran ID, Date, Status, Filer/Committee, Contributor/Payee, Sub Type,
Amount). The .xls export has 45, including everything actually required
downstream: contributor occupation/employer, full address, `Filer Id` and
`Contributor/Payee Committee ID` (the join keys back to committees/
candidates), purpose codes, etc. Confirmed by diffing the two column sets
directly — the export is not optional, it's the only source with the fields
the schema needs.

Row cap
───────
The export (and the on-page display) is hard-capped at 5,000 rows regardless
of how many actually match — a committee/year query that matched 14,886 rows
came back with exactly 5,000 in the .xls, with no error, so a capped response
looks identical to a complete one unless you check the "N records found" text
from the search step first. Because that count comes for free before the
(much larger) export request, chunks are only exported once confirmed to be
at or under the cap; capped windows are split without ever downloading their
oversized export. Windows start at calendar months and are recursively
halved on a cap hit, down to single days, then further split by amount band
(ORESTAR's out-of-state/in-kind-heavy PACs can pile thousands of small
identical-amount transactions on one day, the same pattern seen in Wisconsin
and Florida) as a last resort.

Entities
────────
The election-search form (GotoSearchByElection.do -> CommitteeSearchSecondPage.do,
a real POST form) has its own, much lower cap: 999, confirmed on both the
on-page count and the "Export To Excel Format" (XcelSooSearch) download —
same "check the count before paying for the export" pattern as transactions.

Unlike transactions, ORESTAR gives a real category axis to split on before
ever touching dates: the `filerType` dropdown (11 values — Candidate
Committee, PAC by sub-type, Petition Committee by sub-type) plus two more
categories that sit outside that dropdown as their own checkboxes, Slate
Mailer Organizations and Independent Expenditure Filers (confirmed small on
manual inspection — 4 and roughly 100-200 committees respectively — but swept
as their own buckets regardless of whether they turn out to already be
included in a blank search, since the cost of a possibly-redundant sweep is
two extra Playwright navigations and any overlap is harmless: dedup by
Committee Id happens at parse time, same as everywhere else in this
pipeline).

Sweeping every filerType (11) + the two special buckets = 13 base requests.
Any bucket at the cap is split by `yearActive` (1989 through the current
year); any (bucket, year) that is *still* at the cap after that falls back
to `committeeOffice` (40 values) as a last resort. That third level was
never observed triggering in testing but is kept rather than accepting
silent truncation if a future run's data grows into it.

Base (blank-yearActive) sweep is NOT trustworthy on its own — bug found
after the fact
    An earlier version of this scraper only split a bucket by year when its
    blank-yearActive count hit ENTITY_ROW_CAP, on the assumption that a
    count under the cap meant a complete result. A real run's output proved
    that assumption wrong: CANDALL/CPCALL/PACALL all came back comfortably
    under 999 (991/998/996) with year left unset, but the registry those
    counts produced had a hard skew toward pre-2013 committees and *zero*
    rows with a 2022 Filing Date — including well-known, definitely-real
    2022 committees like "Friends of Christine Drazan" (2022 gubernatorial
    nominee) that only ever showed up as transaction filers, never in any
    entities bucket. So whatever the untouched `yearActive` <select> element
    actually defaults to server-side when the page loads (never confirmed
    directly — this site's F5 Bot Defense blocks the raw-HTTP reconnaissance
    that would show the option list), it silently narrows the result set
    rather than truly meaning "all years," and a below-cap count from it is
    not evidence of completeness. Fix: every bucket is now swept by year
    unconditionally (not just as a cap-triggered fallback) in addition to
    the blank sweep; Committee Id dedup at parse time absorbs the overlap.

All six filing-status checkboxes (Approved, Discontinued, Pending Approval,
Pending Insufficient, Pending Resolved, Rejected) are checked on every
search — the form defaults to Approved-only, which would silently drop
terminated/rejected committees that still have historical transactions tied
to them.

CSRF token (OWASP CSRFGuard)
─────────────────────────────
Because Playwright drives a real browser, OWASP CSRFGuard (visible elsewhere
on this site as the `OWASP_CSRFTOKEN` query param on POST-protected actions)
is a non-issue: its own JS runs normally and injects tokens into forms the
way it would for any real visitor, so the entities form here is just filled
and submitted like any of Florida's Playwright forms — no manual
token-fetching needed.

Access notes — Playwright required
────────────────────────────────────
ORESTAR runs F5 Bot Defense (cookies prefixed "TSPD_101" — formerly Shape
Security). A plain `requests` session gets real data for a modest burst of
calls and then starts receiving a ~6.3KB JS-challenge stub (heavily obfuscated
inline script, no "records found" text) instead of the actual results page —
confirmed from both a cloud sandbox IP *and* a residential home connection,
so this is a request-signature/fingerprint check (no real JS engine, no
browser TLS/CDP fingerprint), not an IP-reputation block. A sustained burst
of plain requests upgrades this from a JS challenge to an outright WAF block
("Please Contact Us") for that IP, so don't re-attempt plain-requests
reconnaissance against this site even for read-only checks. This scraper
drives a real Chromium via Playwright instead; the challenge JS runs and
resolves itself transparently inside an actual browser, the same fix Florida
uses for its own WAF. Headless mode is avoided (headless=False) for the same
reason Florida avoids it — bot-defense products are more likely to
fingerprint headless Chromium.

Electronic filing volume jumps sharply starting 2007 (2006: ~8.6k txns all
year; 2007: ~79.5k) but sparse historical records go back to the 1990s, so
the default transaction floor is 1990 to match ORESTAR's own entity-search
year dropdown (which lists 1989 as its earliest option) — pass --start-year
to narrow this for faster incremental testing.
"""

import csv
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

import xlrd
from playwright.sync_api import sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Oregon" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Oregon" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [
    "relation_type",   # "transactions" or "entities"
    "year",            # transactions: year of window_from. entities: yearActive
                       # when a bucket was year-split, blank otherwise
    "window_from",     # transactions only — inclusive start (YYYY-MM-DD)
    "window_to",       # transactions only — inclusive end   (YYYY-MM-DD)
    "amount_from",     # transactions only — amount band lower bound
    "amount_to",       # transactions only — amount band upper bound
    "bucket",          # entities only — filerType code, "SLATE_MAILER",
                       # "IE_FILERS", or "{bucket}:OFFICE:{code}" for the
                       # committeeOffice last-resort split
    "filename",
    "downloaded_at",
    "row_count",
    "truncated",       # "1" when the chunk still hit the row cap and could not
                       # be split further — flags a known incomplete window
]

# ========================= source constants ===========================
BASE_URL            = "https://secure.sos.state.or.us/orestar"
SEARCH_PAGE         = f"{BASE_URL}/gotoPublicTransactionSearch.do"
SEARCH_ACTION       = f"{BASE_URL}/cneSearch.do"
ENTITY_SEARCH_PAGE  = f"{BASE_URL}/GotoSearchByElection.do"

# Server-side cap confirmed empirically: a query matching 14,886 rows
# exported exactly 5,000 with no error or truncation warning in the response.
ROW_CAP = 5_000

# Entities' own cap, confirmed on both the on-page count and the export —
# much lower than transactions', which is why filerType splitting matters
# even before dates enter the picture. See module docstring.
ENTITY_ROW_CAP = 999

# ORESTAR's own year-search dropdown starts at 1989; transaction volume before
# 2007 is sparse (electronic filing ramp-up) but real records exist back into
# the 1990s, so default to the same floor rather than silently dropping them.
DEFAULT_START_YEAR = 1990
ENTITY_START_YEAR  = 1989   # matches the yearActive dropdown's earliest option

# Amount bands used only as a last resort, when a single calendar day is over
# the row cap and can't be split by date any further. Bounds are inclusive and
# chosen not to overlap at cent precision, so the union is still disjoint.
# A leading (None, 24.99) catches zero, blank and negative (refund) amounts.
AMOUNT_BANDS: list[tuple[float | None, float | None]] = [
    (None,      24.99),
    (25.00,     99.99),
    (100.00,    249.99),
    (250.00,    999.99),
    (1000.00,   9_999.99),
    (10_000.00, None),
]

# filerType dropdown values from the election-search form. Earlier recon
# (raw HTTP, not this Playwright path) found CANDALL/CPCALL sitting at
# exactly 999 with no year filter at all; a real Playwright run instead got
# 991/998/996 for CANDALL/CPCALL/PACALL — under the cap, but proven
# incomplete regardless (missing ~all of 2013+) — see module docstring's
# "Base sweep is NOT trustworthy" section. Every bucket is now year-swept
# unconditionally, so neither number matters for correctness anymore.
FILER_TYPES = [
    ("CANDALL", "Candidate Committee (All)"),
    ("CPCALL",  "Petition Committee (All)"),
    ("CPCINIT", "Petition Committee - Initiative"),
    ("CPCRC",   "Petition Committee - Recall"),
    ("CPCREF",  "Petition Committee - Referendum"),
    ("PACALL",  "Political Action Committee (All)"),
    ("PACCAUC", "Political Action Committee - Caucus"),
    ("PACMEAS", "Political Action Committee - Measure"),
    ("PACMISC", "Political Action Committee - Miscellaneous"),
    ("PACPP",   "Political Action Committee - Political Party"),
    ("PACRC",   "Political Action Committee - Recall"),
]

# Categories outside the filerType dropdown — see module docstring.
SPECIAL_BUCKETS = [
    ("SLATE_MAILER", "allSlateMailer"),
    ("IE_FILERS",     "allIndpendFilers"),
]

# Last-resort third split axis — see module docstring. Values are the
# committeeOffice dropdown's option values.
COMMITTEE_OFFICES = [
    "AG", "BM", "CBCC", "CTYA", "CTYC", "CC", "CTYR", "CM", "BOLI", "CAS",
    "CAU", "CCL", "CCM", "CJ", "CLC", "SH", "CS", "CTC", "CT", "DIR", "DA",
    "GOV", "JCC", "JCA", "JDC", "JTC", "JSC", "JP", "MYR", "MA", "MCP",
    "MC", "MJ", "BOL2", "SOS", "SR", "SS", "TR", "SPI",
]

# All six filing-status checkboxes — the form defaults to Approved-only,
# which would silently drop terminated/rejected committees.
STATUS_CHECKBOXES = [
    "discontinuedSOO", "approvedSOO", "pendingApprovalSOO",
    "insufficientSOO", "resolvedSOO", "rejectedSOO",
]

REQUEST_SLEEP = 1.0   # politeness delay between chunk fetches — real browser navigations
                      # are already much slower than raw HTTP, this just adds a margin


# ========================= manifest helpers ==========================

def _manifest_key(relation: str, *parts: str) -> str:
    """Stable identity for one downloaded chunk, transactions or entities."""
    return "|".join((relation, *parts))


def _row_key(r: dict) -> str | None:
    """Rebuild a row's manifest key from its own columns, dispatching on
    relation_type since transactions and entities key on different fields."""
    rel = r.get("relation_type", "")
    if rel == "transactions":
        return _manifest_key("transactions", r.get("window_from", ""), r.get("window_to", ""),
                             r.get("amount_from", ""), r.get("amount_to", ""))
    if rel == "entities":
        return _manifest_key("entities", r.get("bucket", ""), r.get("year", ""))
    return None


def load_manifest() -> dict[str, dict]:
    """Return {chunk_key: row} for chunks already downloaded, across both relations."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="") as f:
        out: dict[str, dict] = {}
        for r in csv.DictReader(f):
            key = _row_key(r)
            if key:
                out[key] = r
        return out


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
    key = _row_key(record)
    rows = [r for r in rows if _row_key(r) != key]
    rows.append(record)
    _write_manifest(rows)


def strip_manifest(keep) -> None:
    """Rewrite the manifest keeping only rows for which keep(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = [r for r in csv.DictReader(f) if keep(r)]
    _write_manifest(rows)


def _drop_chunk_files(keep) -> None:
    """
    Delete raw files whose manifest row fails keep(). A window/bucket that
    gets re-split produces different filenames, so the old, wider file must
    not be left on disk or its rows get double-counted at parse time.
    """
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        for r in csv.DictReader(f):
            if keep(r):
                continue
            stale = RAW_DIR / (r.get("filename") or "")
            if stale.name and stale.exists():
                stale.unlink()


# ========================== browser helpers ===========================

def _new_page(p):
    """Launch a fresh browser context/page with downloads enabled.

    headless=False, matching Florida's scraper — bot-defense products are
    more likely to fingerprint headless Chromium, and this site's F5 Bot
    Defense challenge blocked plain `requests` from both a datacenter IP and
    a residential one, so a real, headed browser is the safer bet.
    """
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(accept_downloads=True)
    return browser, context.new_page()


# Navigation errors that indicate a stale browser session rather than a
# transient chunk-level problem — restart the browser rather than just retry.
_NAV_ERROR_PHRASES = (
    "ERR_TOO_MANY_REDIRECTS",
    "ERR_HTTP_RESPONSE_CODE_FAILURE",
    "net::ERR_",
    "Timeout",
    "Target page, context or browser has been closed",
)


def _is_nav_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(p in msg for p in _NAV_ERROR_PHRASES)


# ============================ transactions =============================

def _fmt_date(d: date) -> str:
    return d.strftime("%m/%d/%Y")


def _build_search_url(date_from: date, date_to: date,
                      a_from: float | None, a_to: float | None) -> str:
    params = {
        "cneSearchButtonName":   "search",
        "cneSearchTranStartDate": _fmt_date(date_from),
        "cneSearchTranEndDate":   _fmt_date(date_to),
    }
    if a_from is not None:
        params["cneSearchTranAmountFrom"] = f"{a_from:g}"
    if a_to is not None:
        params["cneSearchTranAmountTo"] = f"{a_to:g}"
    return f"{SEARCH_ACTION}?{urlencode(params)}"


_RECORDS_RE = re.compile(r"([\d,]+)\s*records found for the above search criteria")


def _run_search(page, date_from: date, date_to: date,
                a_from: float | None = None, a_to: float | None = None,
                max_retries: int = 3) -> int:
    """
    Navigate to the search URL (sets server-side state for the export click
    that follows) and return the true match count parsed from the results
    page's "N records found" text.

    Retries with a fresh page.goto() on transient failures. Raises on
    navigation-level errors (caller restarts the browser) or if the result
    text can't be found after all retries.
    """
    url = _build_search_url(date_from, date_to, a_from, a_to)
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        if attempt > 0:
            time.sleep(2 + attempt * 3)
        try:
            page.goto(url, timeout=45_000)
            page.wait_for_load_state("networkidle", timeout=30_000)
            text = page.inner_text("body")
            m = _RECORDS_RE.search(text)
            if not m:
                raise RuntimeError(
                    f"could not find result count on page "
                    f"(title={page.title()!r}, {len(text)} chars of body text)"
                )
            return int(m.group(1).replace(",", ""))
        except Exception as e:
            last_exc = e
            if _is_nav_error(e):
                raise
            continue

    raise RuntimeError(f"search failed after {max_retries} attempts: {last_exc}") from last_exc


def _click_export(page, out_path: Path, max_retries: int = 3) -> int:
    """
    Click "Export To Excel Format" for whatever search is currently active on
    the page and save the download. Returns the row count (excluding header)
    read back from the saved file via xlrd — the ground truth, independent of
    whatever the search step parsed from the HTML.

    Shared by both transactions (XcelCNESearch) and entities (XcelSooSearch)
    — the link text and download mechanics are identical, only the target
    endpoint (set by whichever search just ran) differs.
    """
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        if attempt > 0:
            time.sleep(2 + attempt * 3)
        try:
            # Not an exact href match: OWASP CSRFGuard's own JS rewrites every
            # link's href on page load to append its token (e.g.
            # "XcelCNESearch?OWASP_CSRFTOKEN=..."), which only happens once JS
            # actually executes — i.e. exactly the difference between this
            # Playwright-driven page and the raw-HTML fetches used to
            # originally locate this link during exploration.
            link = page.locator('a:has-text("Export To Excel Format")').first
            link.wait_for(state="visible", timeout=10_000)

            with page.expect_download(timeout=60_000) as dl_info:
                link.click()
            download = dl_info.value

            tmp = out_path.with_suffix(".xls.part")
            download.save_as(str(tmp))

            if not tmp.exists() or tmp.stat().st_size == 0:
                raise RuntimeError("empty download")

            wb = xlrd.open_workbook(str(tmp))
            rows = max(0, wb.sheet_by_index(0).nrows - 1)   # exclude header

            tmp.replace(out_path)
            return rows
        except Exception as e:
            out_path.with_suffix(".xls.part").unlink(missing_ok=True)
            last_exc = e
            if _is_nav_error(e):
                raise
            continue

    raise RuntimeError(f"export failed after {max_retries} attempts: {last_exc}") from last_exc


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


def _chunk_filename(w_from: date, w_to: date,
                    a_from: float | None, a_to: float | None) -> str:
    name = f"or_transactions_{w_from.isoformat()}_{w_to.isoformat()}"
    if a_from is not None or a_to is not None:
        lo = "min" if a_from is None else f"{a_from:g}"
        hi = "max" if a_to   is None else f"{a_to:g}"
        name += f"_amt{lo}-{hi}"
    return name + ".xls"


def _download_chunk(log, page,
                    w_from: date, w_to: date,
                    a_from: float | None = None, a_to: float | None = None,
                    keep_capped: bool = False) -> tuple[str | None, int, bool]:
    """
    Run the search for one window (optionally amount-banded); export and save
    it only if the match count is at or under the cap.

    Returns (filename_or_None, row_count, capped). `capped` True means the
    caller should split further (unless keep_capped, in which case the
    export is saved anyway and flagged truncated in the manifest — used when
    there's no split left to try).
    """
    count = _run_search(page, w_from, w_to, a_from, a_to)

    if count == 0:
        return None, 0, False

    if count > ROW_CAP and not keep_capped:
        return None, count, True

    filename = _chunk_filename(w_from, w_to, a_from, a_to)
    out_path = RAW_DIR / filename
    rows = _click_export(page, out_path)

    upsert_manifest({
        "relation_type": "transactions",
        "year":          str(w_from.year),
        "window_from":   w_from.isoformat(),
        "window_to":     w_to.isoformat(),
        "amount_from":   "" if a_from is None else f"{a_from:g}",
        "amount_to":     "" if a_to   is None else f"{a_to:g}",
        "bucket":        "",
        "filename":      filename,
        "downloaded_at": date.today().isoformat(),
        "row_count":     str(rows),
        "truncated":     "1" if rows >= ROW_CAP else "",
    })
    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=rows, duration_s=0)
    return filename, rows, rows >= ROW_CAP


def _download_amount_bands(log, page, day: date) -> tuple[int, int, int]:
    """
    Last-resort split for a single day over the row cap: slice it by amount.
    Returns (files_ok, files_err, rows). A band that is *still* capped is kept
    anyway and flagged truncated="1" — a visible gap beats a silently short
    table.
    """
    ok = err = rows_total = 0
    for a_from, a_to in AMOUNT_BANDS:
        try:
            filename, rows, capped = _download_chunk(
                log, page, day, day, a_from, a_to, keep_capped=True,
            )
            time.sleep(REQUEST_SLEEP)
            if filename is None:
                continue
            if capped:
                log.warning(
                    f"{filename}: still at the {ROW_CAP:,}-row cap after "
                    f"amount banding — chunk is incomplete (flagged truncated)"
                )
            ok += 1
            rows_total += rows
        except Exception as e:
            log.file_download_error(
                filename=_chunk_filename(day, day, a_from, a_to), error=str(e),
            )
            err += 1
    return ok, err, rows_total


def download_windowed(log, p,
                      start: date, end: date, done: dict[str, dict],
                      refresh_current: bool) -> tuple[int, int]:
    """
    Download transactions over [start, end], starting from calendar months
    and recursively splitting (by date, then amount) any window that hits
    ROW_CAP. Windows already in the manifest are skipped unless they fall in
    the current year (still being amended) or the caller cleared them.

    `p` is the running Playwright instance (from sync_playwright()); this
    function owns the browser/page lifecycle and restarts it on navigation
    errors, mirroring Florida's transaction scraper.
    """
    queue: list[tuple[date, date]] = _month_windows(start, end)
    queue.reverse()   # pop() from the end → chronological order
    ok = err = 0
    cur_year = date.today().year

    from tqdm import tqdm
    from tqdm.contrib.logging import logging_redirect_tqdm

    browser, page = _new_page(p)

    try:
        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(desc="  transactions", unit="chunk", dynamic_ncols=True,
                      total=len(queue)) as bar:
                while queue:
                    w_from, w_to = queue.pop()
                    bar.set_postfix_str(f"{w_from} → {w_to}", refresh=False)

                    key        = _manifest_key("transactions", w_from.isoformat(), w_to.isoformat(), "", "")
                    is_current = w_to.year >= cur_year
                    if key in done and not (refresh_current and is_current):
                        existing = RAW_DIR / (done[key].get("filename") or "")
                        if existing.name and existing.exists():
                            log.file_download_skip(filename=existing.name)
                            bar.update(1)
                            continue
                        if not done[key].get("filename"):
                            # A prior run recorded "0 rows, nothing to save"
                            # for this window — still a valid, complete result.
                            bar.update(1)
                            continue

                    try:
                        filename, rows, capped = _download_chunk(log, page, w_from, w_to)
                        time.sleep(REQUEST_SLEEP)
                    except Exception as e:
                        if _is_nav_error(e):
                            log.warning(
                                f"    [!] Browser session error — restarting "
                                f"({type(e).__name__})"
                            )
                            try:
                                browser.close()
                            except Exception:
                                pass
                            time.sleep(3)
                            browser, page = _new_page(p)
                            try:
                                filename, rows, capped = _download_chunk(log, page, w_from, w_to)
                                time.sleep(REQUEST_SLEEP)
                            except Exception as e2:
                                log.file_download_error(
                                    filename=_chunk_filename(w_from, w_to, None, None),
                                    error=str(e2),
                                )
                                err += 1
                                bar.update(1)
                                continue
                        else:
                            log.file_download_error(
                                filename=_chunk_filename(w_from, w_to, None, None),
                                error=str(e),
                            )
                            err += 1
                            bar.update(1)
                            continue

                    if not capped:
                        ok += 1
                        bar.update(1)
                        continue

                    # Capped → drop the stale manifest row (if any) and split.
                    strip_manifest(lambda r, k=key: _row_key(r) != k)

                    children = _split_window(w_from, w_to)
                    if children:
                        log.info(
                            f"  {w_from} → {w_to} hit the {ROW_CAP:,}-row cap — "
                            f"splitting into {len(children)} windows"
                        )
                        queue.extend(reversed(children))
                        bar.total = (bar.total or 0) + len(children)
                    else:
                        log.info(
                            f"  {w_from} is a single day over the cap — "
                            f"splitting by amount band"
                        )
                        b_ok, b_err, _ = _download_amount_bands(log, page, w_from)
                        ok  += b_ok
                        err += b_err
                    bar.update(1)
    finally:
        try:
            browser.close()
        except Exception:
            pass

    return ok, err


# ============================== entities ===============================

_ENTITY_FOUND_RE = re.compile(r"([\d,]+)\s*found for the above search criteria")


def _run_entity_search(page, *, filer_type: str | None = None,
                       special_checkbox: str | None = None,
                       committee_office: str | None = None,
                       year: int | None = None,
                       max_retries: int = 3) -> int:
    """
    Fill and submit the election/committee search form for one bucket
    (a filerType value, or one of the two special checkbox categories),
    optionally narrowed by committeeOffice and/or yearActive, with every
    filing-status checkbox checked so nothing is excluded by default (the
    form defaults to Approved-only). Returns the true match count from the
    "N found for the above search criteria" text.
    """
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        if attempt > 0:
            time.sleep(2 + attempt * 3)
        try:
            page.goto(ENTITY_SEARCH_PAGE, timeout=45_000)
            page.wait_for_load_state("networkidle", timeout=30_000)

            if year is not None:
                page.select_option('select[name="yearActive"]', value=str(year))
            if filer_type:
                page.select_option('select[name="filerType"]', value=filer_type)
            if committee_office:
                page.select_option('select[name="committeeOffice"]', value=committee_office)
            if special_checkbox:
                cb = page.locator(f'input[name="{special_checkbox}"]').first
                if not cb.is_checked():
                    cb.check()
            for cb_name in STATUS_CHECKBOXES:
                loc = page.locator(f'input[type="checkbox"][name="{cb_name}"]')
                if loc.count() > 0 and not loc.first.is_checked():
                    loc.first.check()

            page.locator('input[type="submit"][name="search"]').first.click()
            page.wait_for_load_state("networkidle", timeout=30_000)

            text = page.inner_text("body")
            m = _ENTITY_FOUND_RE.search(text)
            if not m:
                raise RuntimeError(
                    f"could not find result count on page "
                    f"(title={page.title()!r}, {len(text)} chars of body text)"
                )
            return int(m.group(1).replace(",", ""))
        except Exception as e:
            last_exc = e
            if _is_nav_error(e):
                raise
            continue

    raise RuntimeError(f"entity search failed after {max_retries} attempts: {last_exc}") from last_exc


def _entity_filename(bucket: str, year: int | None, office: str | None) -> str:
    name = f"or_entities_{bucket}"
    if year is not None:
        name += f"_{year}"
    if office is not None:
        name += f"_off-{office}"
    return name + ".xls"


def _download_entity_bucket(log, page, bucket: str, *,
                            filer_type: str | None = None,
                            special_checkbox: str | None = None,
                            year: int | None = None,
                            office: str | None = None,
                            keep_capped: bool = False) -> tuple[str | None, int, bool]:
    """
    Same run-search-then-export-if-under-cap pattern as transactions'
    _download_chunk, for one (bucket, year, office) combination.
    """
    count = _run_entity_search(page, filer_type=filer_type,
                               special_checkbox=special_checkbox,
                               committee_office=office, year=year)

    if count == 0:
        return None, 0, False
    if count > ENTITY_ROW_CAP and not keep_capped:
        return None, count, True

    filename = _entity_filename(bucket, year, office)
    out_path = RAW_DIR / filename
    rows = _click_export(page, out_path)

    upsert_manifest({
        "relation_type": "entities",
        "year":          str(year) if year is not None else "",
        "window_from":   "",
        "window_to":     "",
        "amount_from":   "",
        "amount_to":     "",
        "bucket":        bucket if office is None else f"{bucket}:OFFICE:{office}",
        "filename":      filename,
        "downloaded_at": date.today().isoformat(),
        "row_count":     str(rows),
        "truncated":     "1" if rows >= ENTITY_ROW_CAP else "",
    })
    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=rows, duration_s=0)
    return filename, rows, rows >= ENTITY_ROW_CAP


def download_entities(log, p, done: dict[str, dict], end_year: int) -> tuple[int, int]:
    """
    Sweep every filerType plus the two special checkbox categories.

    Each bucket gets BOTH a blank-yearActive base fetch AND an unconditional
    per-year sweep (1989..end_year) — not year-splitting only on a cap hit.
    The base fetch alone was proven incomplete on real data (see module
    docstring's "Base sweep is NOT trustworthy on its own" section): a
    below-cap blank-yearActive count silently excluded the most recent
    ~10 years of committees, not just capped-and-truncated ones. Any
    (bucket, year) that's still at the cap after that falls back to
    committeeOffice as a last resort — see module docstring.

    The base (unsplit) buckets are always re-run — this is a registry, not
    time-partitioned data, so a committee's status can change between runs
    the same way Wisconsin's committees.csv is always refreshed rather than
    manifest-skipped. The year- and office-level sub-fetches are skipped via
    the manifest unless the caller already cleared it.

    Committee IDs are not deduplicated here — the blank sweep, the per-year
    sweep, and the two special buckets all legitimately overlap (the same
    committee shows up in its blank-sweep row AND in whichever year(s) it
    was active). Dedup by Committee Id belongs to the parser, same as the
    raw layer everywhere else in this pipeline.
    """
    buckets = (
        [(code, {"filer_type": code}) for code, _label in FILER_TYPES]
        + [(code, {"special_checkbox": flag}) for code, flag in SPECIAL_BUCKETS]
    )
    ok = err = 0

    from tqdm import tqdm
    from tqdm.contrib.logging import logging_redirect_tqdm

    state = {"browser": None, "page": None}
    state["browser"], state["page"] = _new_page(p)

    def attempt(bucket, kwargs, year=None, office=None, keep_capped=False):
        """Run one download with nav-error retry (restarts the browser once)."""
        try:
            result = _download_entity_bucket(log, state["page"], bucket, year=year,
                                             office=office, keep_capped=keep_capped, **kwargs)
            time.sleep(REQUEST_SLEEP)
            return result
        except Exception as e:
            if not _is_nav_error(e):
                raise
            log.warning(f"    [!] Browser session error — restarting ({type(e).__name__})")
            try:
                state["browser"].close()
            except Exception:
                pass
            time.sleep(3)
            state["browser"], state["page"] = _new_page(p)
            result = _download_entity_bucket(log, state["page"], bucket, year=year,
                                             office=office, keep_capped=keep_capped, **kwargs)
            time.sleep(REQUEST_SLEEP)
            return result

    try:
        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(buckets, desc="  entities", unit="bucket", dynamic_ncols=True) as bar:
                for bucket, kwargs in bar:
                    bar.set_postfix_str(bucket, refresh=False)

                    # Blank-yearActive base fetch — cheap, and catches
                    # committees whose Active Election field is blank (PACs
                    # mostly), but NOT trustworthy as a completeness signal
                    # on its own — see module docstring. Still split on cap.
                    try:
                        filename, count, capped = attempt(bucket, kwargs)
                    except Exception as e:
                        log.file_download_error(filename=_entity_filename(bucket, None, None), error=str(e))
                        err += 1
                    else:
                        if capped:
                            log.info(f"  {bucket} ({count:,}) hit the {ENTITY_ROW_CAP:,}-row cap on the blank sweep")
                        ok += 1

                    # Unconditional per-year sweep — the actual completeness
                    # guarantee. Runs regardless of whether the blank sweep
                    # was capped.
                    for year in range(ENTITY_START_YEAR, end_year + 1):
                        year_key = _manifest_key("entities", bucket, str(year))
                        if year_key in done and done[year_key].get("filename"):
                            existing = RAW_DIR / done[year_key]["filename"]
                            if existing.exists():
                                log.file_download_skip(filename=existing.name)
                                continue

                        try:
                            y_filename, y_count, y_capped = attempt(bucket, kwargs, year=year)
                        except Exception as e:
                            log.file_download_error(
                                filename=_entity_filename(bucket, year, None), error=str(e),
                            )
                            err += 1
                            continue

                        if not y_capped:
                            ok += 1
                            continue

                        log.info(
                            f"    {bucket} {year} ({y_count:,}) still at the cap — "
                            f"splitting by committee office"
                        )
                        for office in COMMITTEE_OFFICES:
                            try:
                                # Last resort — no more axes to split by, so keep
                                # whatever comes back even if still at the cap.
                                o_filename, o_count, o_capped = attempt(
                                    bucket, kwargs, year=year, office=office, keep_capped=True,
                                )
                            except Exception as e:
                                log.file_download_error(
                                    filename=_entity_filename(bucket, year, office), error=str(e),
                                )
                                err += 1
                                continue
                            if o_capped:
                                log.warning(
                                    f"    [!] {bucket} {year} office={office} still at cap "
                                    f"after every split — accepting truncated data"
                                )
                            ok += 1
    finally:
        try:
            state["browser"].close()
        except Exception:
            pass

    return ok, err


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
):
    """
    Download Oregon transactions and entities from ORESTAR.

    Horizontal scope (default = everything):
        transactions                  the cneSearch.do feed (contributions
                                      and expenditures come back interleaved
                                      in one file, so --contributions /
                                      --expenditures both just mean
                                      "transactions" — same as Wisconsin)
        entities                      the election-search feed (candidates
                                      and committees come back interleaved
                                      too, so --candidates / --committees
                                      both just mean "entities")

    Vertical scope: --start-year / --end-year / --force bound the windowed
    transaction sweep. --end-year also bounds how far back the entities
    year-split fallback goes (default: current year); --start-year does not
    apply to entities — the base filerType/special-bucket sweep always
    covers full history regardless, since it's a registry, not a
    time-partitioned feed (see download_entities docstring). --force clears
    both relations' manifest entries and raw files in scope.
    """
    log = get_logger("oregon", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year)

    no_h = not (entities or transactions or contributions or expenditures
               or candidates or committees)
    do_transactions = no_h or transactions or contributions or expenditures
    do_entities     = no_h or entities or candidates or committees

    today = date.today()
    start = date(start_year or DEFAULT_START_YEAR, 1, 1)
    end   = date(end_year, 12, 31) if end_year else today
    end   = min(end, today)
    entities_end_year = end_year or today.year

    files_ok = files_err = 0

    try:
        if do_entities:
            if force:
                _drop_chunk_files(lambda r: r.get("relation_type") != "entities")
                strip_manifest(lambda r: r.get("relation_type") != "entities")

            done = load_manifest()

            log.info(f"Entities: filerType × special buckets, year fallback through {entities_end_year}")
            with sync_playwright() as p:
                ok, err = download_entities(log, p, done, entities_end_year)
            files_ok  += ok
            files_err += err

        if do_transactions:
            year_range_active = start_year is not None or end_year is not None

            def _outside_range(r: dict) -> bool:
                """True → keep the manifest row (it's outside the requested range)."""
                if r.get("relation_type") != "transactions":
                    return True   # never touch entities rows from this filter
                try:
                    yr = int(r["year"])
                except (ValueError, KeyError, TypeError):
                    return True
                if start_year is not None and yr < start_year:
                    return True
                if end_year is not None and yr > end_year:
                    return True
                return False

            if force:
                _drop_chunk_files(lambda r: r.get("relation_type") != "transactions")
                strip_manifest(lambda r: r.get("relation_type") != "transactions")
            elif year_range_active:
                _drop_chunk_files(_outside_range)
                strip_manifest(_outside_range)

            done = load_manifest()

            log.info(f"Transactions: {start} → {end}")
            with sync_playwright() as p:
                ok, err = download_windowed(
                    log, p, start, end, done,
                    # An explicit year range is a request for a refresh; a bare
                    # run only re-pulls the current year.
                    refresh_current=not year_range_active,
                )
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
        description="Download Oregon campaign finance data from ORESTAR."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, wipe manifest and raw files")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); transactions only; wipes manifest for range")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year); "
                         "also bounds the entities year-split fallback")

    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only")
    ap.add_argument("--contributions", action="store_true",
                    help="contributions only (same feed as --transactions)")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditures only (same feed as --transactions)")
    ap.add_argument("--candidates",    action="store_true",
                    help="candidates only (same feed as --entities)")
    ap.add_argument("--committees",    action="store_true",
                    help="committees only (same feed as --entities)")

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
