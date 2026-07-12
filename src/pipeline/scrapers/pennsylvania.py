"""
scrapers/pennsylvania.py — Download the PA Department of State's yearly
"Full Campaign Finance Export" zip files.

    https://www.pa.gov/agencies/dos/resources/voting-and-elections-resources/campaign-finance-data

No browser automation needed (unlike kansas_v2.py): PA's Campaign Finance
Data page is a plain server-rendered link list — "2026 Full Export",
"2025 Full Export", etc. — not a paginated ASP.NET search form behind
postbacks. Every year's zip is one <a href> away, so this scraper just
fetches that one listing page with requests + BeautifulSoup, matches each
link's *visible text* ("NNNN Full Export") against the years requested,
and downloads the zip straight over HTTP.

That's also a deliberate change from the old R scraper's approach, which
drove a real Selenium browser to `.clickElement()` the year's <li> by
COUNTING BACK from today's date:
    li:nth-child(2 + (current_year - i))
— i.e. "the 2024 link is 2 positions below the 2026 link, assuming the
list is contiguous, sorted descending, and never gets a year added,
removed, or reordered above the target." Matching on the link's own text
instead of its position means this scraper can't be thrown off by any of
that, and doesn't need a browser at all.

Each zip is saved as-is (still zipped) to data/Pennsylvania/raw/{year}.zip —
parsers/pennsylvania.py reads directly out of the zip, so there's no
unzip/rename/cleanup dance here like the R version's Processing/ folder
shuffle (unzip, rename each of the 5 files up a directory for pre-2025
zips whose contents are nested one level deeper, delete everything but
contrib_*.txt). None of that is needed when the zip is left intact —
parsers/pennsylvania.py's zip-member lookup handles both the 2025+
flat layout (filer_2026.txt at the zip root) and the pre-2025 nested
layout (2018/filer_2018.txt) transparently.

Known data-quality wrinkle on PA's own listing page, caught by testing
this scraper's year-matching against the live page rather than trusting
a guessed URL pattern: as of this writing, the "2002 Full Export" link's
href actually points at .../2022.zip, not .../2002.zip — almost
certainly a copy/paste error on PA's site, not a URL-naming convention
change. Guessing the URL as f"{year}.zip" would silently download the
wrong year's data for 2002. This scraper instead always uses the href
the listing page actually provides for the requested year's link text,
and cross-checks the downloaded zip's own contents against the year that
was requested (see verify_zip_year) — the zip's internal filenames
(filer_YYYY.txt etc.) reflect the data's true year regardless of what
the outer page or URL claims. A mismatch is logged loudly and the file
is NOT saved as data/Pennsylvania/{requested_year}.zip, so a website bug
like this can't silently corrupt that year's output. If PA fixes the
link, this scraper picks up the fix automatically; if they don't, re-run
with --force periodically to notice if it starts resolving correctly.

Project integration (mirrors kansas_v2.py where it still applies):
    Output (data/Pennsylvania/raw/):
        {year}.zip — one per year, 2000-present, untouched from the
        Department of State (see parsers/pennsylvania.py for what's
        inside each one).
    Manifest (data/Pennsylvania/manifest.csv): year, source_url,
        link_text, bytes, scraped_at — one row per successfully
        downloaded year. Used for incremental skip logic; NOT the same
        file/schema as any Kansas manifest.
    Logging: src.reporting.logger.get_logger("pennsylvania", "scrape")

CLI:
    (no args)         incremental — download every year 2000-current that
                       isn't already on disk; always re-download the
                       current year (it keeps gaining filings all year).
    2024 2025 2026     only download these specific years
    --start-year       only download years >= YYYY
    --end-year         only download years <= YYYY
    --force            re-download every year in scope, even if already
                       on disk
    --out-dir          directory for the zips + manifest (default:
                       data/Pennsylvania/)
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
DATA_DIR = PROJECT_ROOT / "data" / "Pennsylvania"
RAW_DIR  = DATA_DIR / "raw"
MANIFEST = DATA_DIR / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["year", "source_url", "link_text", "bytes", "scraped_at"]

LISTING_URL = ("https://www.pa.gov/agencies/dos/resources/"
               "voting-and-elections-resources/campaign-finance-data")

EARLIEST_YEAR = 2000   # earliest year DOS publishes a full export for
REQUEST_TIMEOUT = 60
DOWNLOAD_CHUNK = 1 << 20   # 1 MiB

# Plain requests.get() with no User-Agent gets blocked by some .gov sites'
# bot filters — a normal browser UA avoids that without doing anything
# deceptive (we're not evading a robots.txt disallow, just not announcing
# ourselves as the default python-requests client).
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36")
}

LINK_TEXT_RE = re.compile(r"(\d{4})\s+Full Export", re.IGNORECASE)


# ========================= Listing page ================================

def discover_year_links(session: requests.Session, log) -> dict[int, dict]:
    """
    Fetch LISTING_URL and return {year: {"url": absolute_href, "text":
    link_text}} for every "NNNN Full Export" link found.

    Matches purely on link *text*, not on any assumption about how the
    href is structured — see module docstring on the 2002/2022 link bug.
    """
    resp = session.get(LISTING_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    links: dict[int, dict] = {}
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        m = LINK_TEXT_RE.search(text)
        if not m:
            continue
        year = int(m.group(1))
        href = urljoin(LISTING_URL, a["href"])
        if year in links and links[year]["url"] != href:
            log.warning(f"  Duplicate/conflicting link text for {year}: "
                        f"keeping {links[year]['url']!r}, ignoring {href!r}")
            continue
        links[year] = {"url": href, "text": text}

    if not links:
        raise ValueError(
            f"No 'NNNN Full Export' links found on {LISTING_URL} — the "
            f"page layout may have changed. Inspect resp.text to update "
            f"LINK_TEXT_RE / the parsing logic."
        )
    return links


# ========================= Manifest helpers ===========================

def load_manifest() -> dict[str, dict]:
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or "year" not in reader.fieldnames:
            print(f"WARNING: {MANIFEST} exists but doesn't look like a "
                  f"pennsylvania.py manifest (missing 'year' column) — "
                  f"ignoring it and starting fresh.")
            return {}
        return {row["year"]: row for row in reader}


def write_manifest(rows: dict[str, dict]) -> None:
    """Rewrite the whole manifest from the in-memory dict. Cheap here —
    at most ~30 rows (one per year DOS has ever published) — unlike
    Kansas's per-candidate manifest, which is appended to incrementally
    instead."""
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        for year in sorted(rows, key=lambda y: int(y)):
            w.writerow(rows[year])


# ========================= Download + verify ===========================

def verify_zip_year(zip_path: Path, year: int) -> tuple[bool, str]:
    """
    Confirm the zip actually contains a filer_{year}.txt — the data's own
    internal naming, which reflects its true year regardless of what URL
    or link text it was fetched under. Returns (ok, message).

    Handles both known DOS zip layouts: 2025+ zips have the five .txt
    files at the zip root (filer_2026.txt); pre-2025 zips nest them one
    level down inside a "{year}/" folder (2018/filer_2018.txt) — the
    same nesting the old R scraper's `if(i < 2025)` block unpacked by
    hand. Either layout satisfies the check.
    """
    import zipfile
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
    except zipfile.BadZipFile:
        return False, "not a valid zip file"

    expected = f"filer_{year}.txt"
    if any(n == expected or n.endswith("/" + expected) for n in names):
        return True, "ok"

    # Figure out what year it actually looks like, for a useful error —
    # match filer_YYYY.txt at root or nested under any folder.
    found_years = sorted({
        m.group(1) for n in names
        if (m := re.search(r"(?:^|/)filer_(\d{4})\.txt$", n))
    })
    return False, (
        f"expected {expected!r} inside the zip (at root or nested under "
        f"a folder), found: {names}. This zip's data looks like it's "
        f"actually for {', '.join(found_years) if found_years else 'an unknown year'}."
    )


def download_year(session: requests.Session, year: int, url: str,
                   dest: Path, log) -> tuple[bool, int]:
    """
    Stream-download url to dest via a .part temp file, verify it's a
    valid zip for `year`, then atomically move it into place. Returns
    (success, bytes_written). Leaves no partial file behind on failure.
    """
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
        log.warning(f"  ✗ {year}: download failed: {e}")
        return False, 0

    ok, msg = verify_zip_year(tmp, year)
    if not ok:
        tmp.unlink(missing_ok=True)
        log.warning(f"  ✗ {year}: {msg} — not saving as {dest.name} "
                    f"(source: {url})")
        return False, 0

    tmp.replace(dest)
    return True, bytes_written


# ============================== run ==================================

def run(years: list[int] | None = None, start_year: int | None = None,
        end_year: int | None = None, force: bool = False,
        out_dir: str | None = None):
    """
    Download data/Pennsylvania/{year}.zip for every requested year that
    isn't already on disk (or every year, with --force).

    Scope, in priority order:
        years            an explicit list — only these years
        start_year/
        end_year         a range (either bound optional)
        (neither)         every year from EARLIEST_YEAR through the
                          current year

    Regardless of scope, the current year is always re-downloaded even
    if already on disk (it keeps gaining filings all year) — same
    "always re-check the still-open cycle" logic as kansas_v2.py.
    """
    global DATA_DIR, RAW_DIR, MANIFEST
    if out_dir is not None:
        DATA_DIR = Path(out_dir)
        RAW_DIR = DATA_DIR / "raw"
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        MANIFEST = DATA_DIR / "manifest.csv"

    log = get_logger("pennsylvania", "scrape")
    t0 = time.perf_counter()
    log._emit("scrape_started", force=force, years=years,
              start_year=start_year, end_year=end_year)

    current_year = datetime.today().year

    if years:
        wanted = sorted(set(years))
    else:
        lo = start_year if start_year is not None else EARLIEST_YEAR
        hi = end_year if end_year is not None else current_year
        wanted = list(range(lo, hi + 1))

    out_of_range = [y for y in wanted if y < EARLIEST_YEAR or y > current_year]
    if out_of_range:
        log.warning(f"  Ignoring out-of-range year(s) (DOS publishes "
                    f"{EARLIEST_YEAR}-{current_year}): {out_of_range}")
        wanted = [y for y in wanted if EARLIEST_YEAR <= y <= current_year]

    if not wanted:
        print("ERROR: no valid years to scrape")
        sys.exit(1)

    manifest = load_manifest()
    session = requests.Session()

    try:
        links = discover_year_links(session, log)
    except Exception as e:
        log.warning(f"  Failed to load the listing page: {e}")
        raise

    missing = [y for y in wanted if y not in links]
    if missing:
        log.warning(f"  No link found on the listing page for: {missing} "
                    f"(skipping)")
        wanted = [y for y in wanted if y in links]

    log.info(f"  Requested {len(wanted)} year(s): "
             f"{', '.join(str(y) for y in wanted)}")

    downloaded = skipped = failed = 0

    for year in wanted:
        dest = RAW_DIR / f"{year}.zip"
        is_current = year >= current_year

        if not force and not is_current and dest.exists():
            log.debug(f"  – {year}: already on disk, skipping")
            skipped += 1
            continue

        link = links[year]
        log.file_download_start(dest.name)
        t_dl = time.perf_counter()
        ok, nbytes = download_year(session, year, link["url"], dest, log)
        duration = time.perf_counter() - t_dl

        if not ok:
            failed += 1
            continue

        downloaded += 1
        log.file_download_ok(dest.name, bytes=nbytes, rows=0, duration_s=duration)
        manifest[str(year)] = {
            "year": year,
            "source_url": link["url"],
            "link_text": link["text"],
            "bytes": nbytes,
            "scraped_at": datetime.now().isoformat(timespec="seconds"),
        }
        write_manifest(manifest)   # small file — cheap to rewrite after every year

    duration = round(time.perf_counter() - t0, 1)
    log.info(f"Done in {duration}s — {downloaded} downloaded, "
             f"{skipped} skipped, {failed} failed. Files in {RAW_DIR}/")
    log._emit("scrape_completed", status="completed", duration_s=duration,
              downloaded=downloaded, skipped=skipped, failed=failed)


# ============================= CLI ===================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download PA DOS's yearly Full Campaign Finance Export zips."
    )
    ap.add_argument("years", type=int, nargs="*",
                    help="specific year(s) to download, e.g. 2024 2025 2026")
    ap.add_argument("--start-year", type=int, metavar="YYYY",
                    help="earliest year to download")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download")
    ap.add_argument("--force", action="store_true",
                    help="re-download every year in scope, even if already on disk")
    ap.add_argument("--out-dir", type=str, default=None,
                    help="base directory for raw/ zips + manifest.csv "
                         "(default: data/Pennsylvania/)")

    # CLI parity with kansas_v2.py — accepted but ignored. PA's export
    # bundles all data types together (same reason Kansas ignores these),
    # and there's no browser to run headless/visible.
    for flag in ("--headless", "--transactions", "--entities", "--contributions",
                 "--expenditures", "--candidates", "--committees"):
        ap.add_argument(flag, action="store_true", help="(ignored)")

    args, _ = ap.parse_known_args()

    if args.years and (args.start_year or args.end_year):
        ap.error("pass either explicit years or --start-year/--end-year, not both")
    if args.start_year and args.end_year and args.start_year > args.end_year:
        ap.error("--start-year cannot be greater than --end-year")

    try:
        run(
            years=args.years or None,
            start_year=args.start_year,
            end_year=args.end_year,
            force=args.force,
            out_dir=args.out_dir,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)