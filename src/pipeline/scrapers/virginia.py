"""
scrapers/virginia.py — Download Virginia Dept. of Elections campaign finance CSVs.

Source (plain Apache/IIS-style directory listing, no browser automation needed):

    https://apps.elections.virginia.gov/SBE_CSV/CF/

The listing is a two-level directory tree:

    CF/1999/ .. CF/2011/          one directory per calendar YEAR (1999-2011)
    CF/2012_01/ .. CF/{cur}_{mo}/  one directory per YEAR_MONTH (2012-present)

Each period directory contains a fixed(ish) set of Schedule CSVs. The exact
file list is NOT identical across eras, so this scraper discovers files by
crawling each period's own directory listing rather than hard-coding
filenames:

  - 1999-2011 (yearly dirs): schedules are split into a per-committee-type
    pair, e.g. ScheduleA_PAC.csv / ScheduleB.csv / ScheduleB_PAC.csv / ...
    There is no Report.csv for these years — VA's old system did not
    publish a report-level cover sheet file (see parsers/virginia.py's
    "legacy era" handling and docs/states/virginia.md for how contribution/
    expenditure rows from this era are handled without a committee-name
    join key).
  - 2012-present (monthly dirs): Report.csv (report/committee cover sheet)
    plus ScheduleA.csv through ScheduleI.csv (no _PAC suffix — candidate and
    PAC filings share one file per schedule from this point on).

Both eras are handled identically here — this scraper just walks whatever
<a href="...csv"> links each period's own listing page contains and saves
them as-is. No CSV parsing happens here at all (see parsers/virginia.py).

Every page in this hierarchy is a bare, unauthenticated, un-paginated HTML
directory listing (Microsoft IIS style <pre> block of <a href> + <br>),
so requests + BeautifulSoup is sufficient — no Selenium/Playwright needed.
The site sits behind Akamai (bot-management headers are visible in the
response), but has not been observed to block a normal browser User-Agent
making a modest number of sequential GETs; if that changes, add a short
sleep between period fetches (see RATE_LIMIT_S below, already used).

Per the state's own reporting page: "Submitted reports and data feeds are
updated daily starting at 5:15 p.m. and again at 12:05 a.m." — so the
*current* year_month directory keeps gaining/changing files all month and
is always re-fetched in full, same "still-open cycle" handling used by
other states' scrapers (e.g. pennsylvania.py's current-year zip).

Project integration:
    Output (data/Virginia/raw/):
        {period}/{Filename}.csv — one subdirectory per period (e.g. "1999",
        "2012_03"), containing every CSV that period's directory listing
        advertised, byte-for-byte as served.
    Manifest (data/Virginia/manifest.csv): period, filename, source_url,
        bytes, scraped_at — one row per successfully downloaded file.
    Logging: src.reporting.logger.get_logger("virginia", "scrape")
        - page_scrape_* for the per-period directory-listing fetches
        - file_download_* for each individual CSV download

CLI:
    (no args)          incremental — fetch every period's file list that
                        isn't fully on disk yet; always re-fetch the
                        current year_month in full.
    --start-year YYYY   only consider periods whose leading year is >= YYYY
    --end-year YYYY     only consider periods whose leading year is <= YYYY
    --force             re-download every file in scope, even if already
                        on disk
"""

import csv
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
DATA_DIR = PROJECT_ROOT / "data" / "Virginia"
RAW_DIR  = DATA_DIR / "raw"
MANIFEST = DATA_DIR / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["period", "filename", "source_url", "bytes", "scraped_at"]

# ========================= state-specific constants ===================
ROOT_URL = "https://apps.elections.virginia.gov/SBE_CSV/CF/"

EARLIEST_YEAR    = 1999
REQUEST_TIMEOUT  = 60
DOWNLOAD_CHUNK   = 1 << 20   # 1 MiB
RATE_LIMIT_S     = 0.2       # small pause between period-listing fetches

# Matches a period directory's own path segment: "1999" (yearly era,
# 1999-2011) or "2012_03" (monthly era, 2012-present). Trailing slash is
# stripped by the caller before matching.
PERIOD_RE = re.compile(r"^((?:19|20)\d{2})(?:_(\d{2}))?$")

# Plain requests.get() with the default python-requests UA is a common
# trigger for .gov bot filters (Akamai in this case) — a normal browser
# UA avoids that without evading any robots.txt disallow.
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36")
}


# ============================ period helpers ===========================

def period_year(period: str) -> int:
    """Leading 4-digit year for a period string ("2012_03" -> 2012, "1999" -> 1999)."""
    m = PERIOD_RE.match(period)
    return int(m.group(1)) if m else 0


def current_period() -> str:
    """The period string covering today — used to always force-refresh the
    still-accumulating current month (monthly era only; VA has published
    monthly directories since 2012, so "today" always falls in that era)."""
    now = datetime.today()
    return f"{now.year}_{now.month:02d}"


# ========================= Directory-listing crawl =====================

def discover_periods(session: requests.Session, log) -> list[tuple[str, str]]:
    """
    Fetch ROOT_URL and return [(period, absolute_url), ...] sorted
    chronologically, for every subdirectory link matching PERIOD_RE.

    Matches on the href's own trailing path segment, not on link text —
    VA's listing shows "1999", "2012_03" etc. as both, but matching the
    href is more robust to any future link-text formatting change (same
    reasoning as pennsylvania.py's discover_year_links, which matches link
    *text* instead of URL shape for the opposite reason — pick whichever
    of the two a given site's markup makes more reliable).
    """
    resp = session.get(ROOT_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    periods: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = urljoin(ROOT_URL, a["href"])
        segment = href.rstrip("/").rsplit("/", 1)[-1]
        if PERIOD_RE.match(segment):
            periods[segment] = href if href.endswith("/") else href + "/"

    if not periods:
        raise ValueError(
            f"No period subdirectories found under {ROOT_URL} — the page "
            f"layout may have changed. Inspect resp.text to update PERIOD_RE "
            f"/ the parsing logic."
        )

    return sorted(periods.items(), key=lambda kv: (period_year(kv[0]), kv[0]))


def discover_files(session: requests.Session, period_url: str, log) -> list[tuple[str, str]]:
    """
    Fetch a single period directory's listing and return [(filename,
    absolute_url), ...] for every *.csv link found. Returns [] (logged as
    a page_scrape_error, not raised) on a fetch failure — one bad period
    shouldn't abort the whole run.
    """
    try:
        resp = session.get(period_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.page_scrape_error(entity="period", page_id=period_url, error=str(e))
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    files: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = urljoin(period_url, a["href"])
        name = href.rstrip("/").rsplit("/", 1)[-1]
        if name.lower().endswith(".csv"):
            files.append((name, href))
    return files


# ========================= Manifest helpers ============================

def _key(period: str, filename: str) -> str:
    return f"{period}/{filename}"


def load_manifest() -> dict[str, dict]:
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "period" not in reader.fieldnames:
            print(f"WARNING: {MANIFEST} exists but doesn't look like a "
                  f"virginia.py manifest (missing 'period' column) — "
                  f"ignoring it and starting fresh.")
            return {}
        return {_key(row["period"], row["filename"]): row for row in reader}


def write_manifest(rows: dict[str, dict]) -> None:
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        for row in sorted(rows.values(),
                          key=lambda r: (period_year(r["period"]), r["period"], r["filename"])):
            w.writerow(row)


def strip_manifest(manifest: dict[str, dict], predicate) -> dict[str, dict]:
    """Drop manifest entries whose row matches predicate(row) — used to
    force a re-download for an explicit --start-year/--end-year/--force
    scope (see docs/contributing.md's "Manifest clearing for year flags")."""
    return {k: v for k, v in manifest.items() if not predicate(v)}


# ========================= Download ====================================

def download_file(session: requests.Session, url: str, dest: Path, log) -> tuple[bool, int]:
    """Stream url to dest via a .part temp file, then atomically move it
    into place. Returns (success, bytes_written); leaves no partial file
    behind on failure."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    bytes_written = 0
    try:
        with session.get(url, headers=HEADERS, stream=True,
                          timeout=REQUEST_TIMEOUT) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK):
                    if chunk:
                        f.write(chunk)
                        bytes_written += len(chunk)
    except requests.RequestException as e:
        tmp.unlink(missing_ok=True)
        log.file_download_error(filename=dest.name, error=str(e))
        return False, 0

    tmp.replace(dest)
    return True, bytes_written


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
    Download every CSV VA publishes under SBE_CSV/CF/ for the requested
    scope. VA's export bundles committees, candidates, contributions,
    expenditures, and loans/debts into the same handful of Schedule files
    per period rather than splitting them into separate downloads, so
    `entities`/`transactions`/`contributions`/`expenditures`/`candidates`/
    `committees` are accepted for CLI-flag parity with other states'
    scrapers but otherwise ignored — every period download always fetches
    that period's complete file set.

    Vertical scope (start_year/end_year/force) filters by each period's
    *leading* year — e.g. --start-year 2020 covers "2020_01".."2020_12"
    and every later period.
    """
    log = get_logger("virginia", "scrape")
    t0 = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year)

    manifest = load_manifest()

    year_range_explicit = start_year is not None or end_year is not None
    if force or year_range_explicit:
        lo = start_year if start_year is not None else EARLIEST_YEAR
        hi = end_year if end_year is not None else datetime.today().year

        def _in_scope(row: dict) -> bool:
            return lo <= period_year(row["period"]) <= hi

        manifest = strip_manifest(manifest, _in_scope)

    session = requests.Session()

    try:
        periods = discover_periods(session, log)
    except Exception as e:
        log.warning(f"  Failed to load {ROOT_URL}: {e}")
        raise

    if start_year is not None:
        periods = [(p, u) for p, u in periods if period_year(p) >= start_year]
    if end_year is not None:
        periods = [(p, u) for p, u in periods if period_year(p) <= end_year]

    if not periods:
        print("ERROR: no periods in scope")
        sys.exit(1)

    cur = current_period()
    log.info(f"  {len(periods)} period(s) in scope "
             f"({periods[0][0]} .. {periods[-1][0]})")

    files_ok = files_err = files_skipped = 0

    try:
        for period, period_url in periods:
            time.sleep(RATE_LIMIT_S)
            t_period = time.perf_counter()
            files = discover_files(session, period_url, log)
            log.page_scrape_ok(entity="period", page_id=period,
                               duration_s=time.perf_counter() - t_period)

            is_current = period == cur

            for filename, file_url in files:
                key  = _key(period, filename)
                dest = RAW_DIR / period / filename

                already_done = key in manifest or (
                    not year_range_explicit and not force
                    and dest.exists() and dest.stat().st_size > 0
                )
                if already_done and not is_current and not force:
                    log.file_download_skip(filename=key)
                    files_skipped += 1
                    continue

                log.file_download_start(filename=key)
                t_dl = time.perf_counter()
                ok, nbytes = download_file(session, file_url, dest, log)
                duration = time.perf_counter() - t_dl

                if not ok:
                    files_err += 1
                    continue

                files_ok += 1
                log.file_download_ok(filename=key, bytes=nbytes, rows=0,
                                     duration_s=duration)
                manifest[key] = {
                    "period":     period,
                    "filename":   filename,
                    "source_url": file_url,
                    "bytes":      nbytes,
                    "scraped_at": datetime.now().isoformat(timespec="seconds"),
                }

            write_manifest(manifest)   # rewrite after every period — cheap, keeps re-runs safe

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} downloaded, "
                 f"{files_skipped} skipped, {files_err} failed. Files in {RAW_DIR}/")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err, files_skipped=files_skipped)

    except KeyboardInterrupt:
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err, files_skipped=files_skipped)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err, files_skipped=files_skipped,
                  error_type=type(e).__name__, error=str(e))
        raise


# ============================= CLI =====================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download Virginia Dept. of Elections campaign finance CSVs."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force", action="store_true",
                      help="re-download all periods in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); wipes manifest for range")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, <= current year)")

    # Accepted for CLI parity with orc.py's standard flag set — VA's export
    # bundles every data type into the same per-period file set, so these
    # don't change scraper behavior (see run()'s docstring).
    for flag in ("--transactions", "--entities", "--contributions",
                 "--expenditures", "--candidates", "--committees"):
        ap.add_argument(flag, action="store_true", help="(ignored — see module docstring)")

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
