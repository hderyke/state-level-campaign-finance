"""
scrapers/kentucky.py — Download Kentucky campaign finance data from the
Kentucky Registry of Election Finance (KREF) public search portal at
https://secure.kentucky.gov/kref/publicsearch/

All data is available as flat CSV exports via GET requests — no pagination,
no PDFs.  Four export types are used:

  1. Candidates (per party)
       GET /kref/publicsearch/ExportSearch?PoliticalParty={party}&ElectionDate=...
       Six requests (Republican, Democratic, Libertarian, Independent, Other,
       NotApplicable) to capture party affiliation, which is absent from the
       all-parties export.  Saved as raw/candidates_{party}.csv.

  2. Organizations (committees, PACs)
       GET /kref/publicsearch/Home/ExportOrganizationSearch
       One request.  Saved as raw/organizations.csv.

  3. Contributions (per year)
       GET /kref/publicsearch/ExportContributors?MinimalDate=YYYY-01-01&MaximalDate=YYYY-12-31&...
       One file per calendar year from EARLIEST_YEAR to current.
       Saved as raw/contributions_{year}.csv.

  4. Expenditures (per year)
       GET /kref/publicsearch/Export?MinimalDate=MM%2FDD%2FYYYY...&MaximalDate=...
       One file per calendar year from EARLIEST_YEAR to current.
       Saved as raw/expenditures_{year}.csv.
       Note: this endpoint uses MM/DD/YYYY 00:00:00 date encoding, not ISO.

Data history: reliable from 1996; sparse before.
Party data: available via per-party candidate export.
Filer IDs: not present in any export → name_hash ID model in parser.

Manifest (data/Kentucky/manifest.csv):
    file_type, filename, rows, downloaded_at
"""

import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Kentucky" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Kentucky" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["file_type", "filename", "rows", "downloaded_at"]

# ============================= constants ==============================
BASE_URL     = "https://secure.kentucky.gov/kref/publicsearch"
EARLIEST_YEAR = 1996
PARTIES = [
    ("Republican",    "republican"),
    ("Democratic",    "democratic"),
    ("Libertarian",   "libertarian"),
    ("Independent",   "independent"),
    ("Other",         "other"),
    ("NotApplicable", "notapplicable"),
]

SESSION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://secure.kentucky.gov/kref/publicsearch/",
}

# ========================= manifest helpers ==========================

def load_manifest() -> dict[str, dict]:
    """Return {filename: row} for all entries."""
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
    """Append or overwrite manifest row for record['filename']."""
    filename = record["filename"]
    write_header = not MANIFEST.exists() or MANIFEST.stat().st_size == 0
    mode = "a" if MANIFEST.exists() and not write_header else "w" if write_header else "a"
    if filename in done:
        # Rewrite whole manifest with updated row
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
    else:
        with open(MANIFEST, mode, newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
            if write_header:
                w.writeheader()
            w.writerow(record)
    done[filename] = record

# ========================= HTTP helper ===============================

def _get(session: requests.Session, url: str, retries: int = 4,
         timeout: int = 120) -> requests.Response:
    last_err = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            wait = 3 * (attempt + 1)
            time.sleep(wait)
    raise last_err


def _row_count(content: bytes) -> int:
    """Count newlines in CSV bytes as a proxy for row count."""
    return content.count(b"\n")


# ========================= download helpers ==========================

def _download_and_save(session, url, dest: Path, file_type: str,
                       filename: str, done: dict, log, today: str,
                       force: bool = False) -> bool:
    """
    Download url → dest. Skip if already in manifest and file exists,
    unless force=True. Returns True on success.
    """
    if not force and filename in done and dest.exists() and dest.stat().st_size > 0:
        log.file_download_skip(filename=filename)
        return True

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()
    try:
        r = _get(session, url)
        dest.write_bytes(r.content)
        rows = _row_count(r.content)
        log.file_download_ok(
            filename=filename, bytes=len(r.content), rows=rows,
            duration_s=round(time.perf_counter() - t0, 2),
        )
        upsert_manifest(
            {"file_type": file_type, "filename": filename,
             "rows": rows, "downloaded_at": today},
            done,
        )
        return True
    except Exception as e:
        log.file_download_error(filename=filename, error=str(e))
        return False


# ============================== run ==================================

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
    Download Kentucky KREF campaign finance CSVs.

    Horizontal scope:
        (no flag)          download everything
        --entities         candidates + organizations only
        --candidates       candidate exports only (all parties)
        --committees       organization export only
        --transactions     contributions + expenditures only
        --contributions    contribution exports only
        --expenditures     expenditure exports only

    Vertical scope (applies to per-year transaction files only):
        (no flag)          incremental — skip existing files; always re-download
                           current year
        --start-year YYYY  re-download contributions/expenditures from YYYY onward
        --end-year YYYY    re-download contributions/expenditures up to YYYY
        --force            wipe manifest, re-download everything
    """
    log = get_logger("kentucky", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year)

    # Resolve which sections to download
    do_all          = not any([entities, transactions, contributions, expenditures,
                                candidates, committees])
    do_candidates   = do_all or entities or candidates
    do_committees   = do_all or entities or committees
    do_contributions = do_all or transactions or contributions
    do_expenditures = do_all or transactions or expenditures

    current_year = datetime.today().year
    today        = datetime.today().strftime("%Y-%m-%d")
    files_ok = files_err = 0

    try:
        session = requests.Session()
        session.headers.update(SESSION_HEADERS)

        # ── Manifest prep ──────────────────────────────────────────────
        year_range_active = start_year is not None or end_year is not None

        if force:
            strip_manifest(lambda _: False)
            done = {}
        elif year_range_active:
            # Wipe manifest entries for yearly transaction files in range
            def _outside_range(r: dict) -> bool:
                ft = r.get("file_type", "")
                if not (ft.startswith("contributions_") or ft.startswith("expenditures_")):
                    return True   # keep non-yearly files
                try:
                    yr = int(ft.split("_")[1])
                except (ValueError, IndexError):
                    return True
                if start_year is not None and yr >= start_year:
                    return False
                if end_year is not None and yr <= end_year:
                    return False
                return True
            strip_manifest(_outside_range)
            done = load_manifest()
        else:
            done = load_manifest()

        # ── Candidates (per party) ─────────────────────────────────────
        if do_candidates:
            log.info("  Downloading candidate exports (per party)…")
            # ElectionDate=01/01/0001 = all dates
            base = (f"{BASE_URL}/ExportSearch"
                    f"?ElectionDate=01%2F01%2F0001%2000%3A00%3A00"
                    f"&ExemptionStatus=All")
            for party_param, party_slug in PARTIES:
                filename = f"candidates_{party_slug}.csv"
                url = f"{base}&PoliticalParty={party_param}"
                dest = RAW_DIR / filename
                ok = _download_and_save(
                    session, url, dest, f"candidates_{party_slug}",
                    filename, done, log, today, force=force,
                )
                files_ok += ok; files_err += (not ok)
                time.sleep(0.5)

        # ── Organizations ──────────────────────────────────────────────
        if do_committees:
            log.info("  Downloading organization export…")
            url = (f"{BASE_URL}/Home/ExportOrganizationSearch"
                   f"?OrganizationName=&OrganizationType=&IsActiveFlag=")
            ok = _download_and_save(
                session, url, RAW_DIR / "organizations.csv",
                "organizations", "organizations.csv",
                done, log, today, force=force,
            )
            files_ok += ok; files_err += (not ok)
            time.sleep(0.5)

        # ── Contributions (per year) ───────────────────────────────────
        if do_contributions:
            log.info("  Downloading contribution exports…")
            y_start = start_year or EARLIEST_YEAR
            y_end   = end_year   or current_year
            for year in range(y_start, y_end + 1):
                filename = f"contributions_{year}.csv"
                dest     = RAW_DIR / filename
                is_current = (year == current_year)
                # Skip past years already downloaded unless forced/in range
                if (not force and not year_range_active and not is_current
                        and filename in done and dest.exists()
                        and dest.stat().st_size > 0):
                    log.file_download_skip(filename=filename)
                    continue
                url = (
                    f"{BASE_URL}/ExportContributors"
                    f"?ElectionDate=01%2F01%2F0001%2000%3A00%3A00"
                    f"&ContributionSearchType=All"
                    f"&MinimalDate={year}-01-01"
                    f"&MaximalDate={year}-12-31"
                )
                ok = _download_and_save(
                    session, url, dest, f"contributions_{year}",
                    filename, done, log, today, force=True,
                )
                files_ok += ok; files_err += (not ok)
                time.sleep(0.4)

        # ── Expenditures (per year) ────────────────────────────────────
        if do_expenditures:
            log.info("  Downloading expenditure exports…")
            y_start = start_year or EARLIEST_YEAR
            y_end   = end_year   or current_year
            for year in range(y_start, y_end + 1):
                filename = f"expenditures_{year}.csv"
                dest     = RAW_DIR / filename
                is_current = (year == current_year)
                if (not force and not year_range_active and not is_current
                        and filename in done and dest.exists()
                        and dest.stat().st_size > 0):
                    log.file_download_skip(filename=filename)
                    continue
                # Expenditure export uses MM/DD/YYYY 00:00:00 date format
                min_date = quote(f"01/01/{year} 00:00:00")
                max_date = quote(f"12/31/{year} 00:00:00")
                url = (
                    f"{BASE_URL}/Export"
                    f"?ElectionDate=01%2F01%2F0001%2000%3A00%3A00"
                    f"&MinimalDate={min_date}"
                    f"&MaximalDate={max_date}"
                )
                ok = _download_and_save(
                    session, url, dest, f"expenditures_{year}",
                    filename, done, log, today, force=True,
                )
                files_ok += ok; files_err += (not ok)
                time.sleep(0.4)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} downloaded, {files_err} errors")
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
        description="Download Kentucky KREF campaign finance CSVs."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all files, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year for transaction files (contributions/expenditures)")

    ap.add_argument("--end-year",      type=int, metavar="YYYY",
                    help="latest year for transaction files")

    ap.add_argument("--entities",      action="store_true",
                    help="download candidates + organizations only")
    ap.add_argument("--transactions",  action="store_true",
                    help="download contributions + expenditures only")
    ap.add_argument("--contributions", action="store_true")
    ap.add_argument("--expenditures",  action="store_true")
    ap.add_argument("--candidates",    action="store_true")
    ap.add_argument("--committees",    action="store_true")

    args, _ = ap.parse_known_args()

    cy = datetime.today().year
    if args.end_year and args.end_year > cy:
        ap.error(f"--end-year cannot exceed current year ({cy})")
    if getattr(args, "start_year", None) and args.end_year:
        if args.start_year > args.end_year:
            ap.error("--start-year cannot be greater than --end-year")
    if args.force and args.end_year:
        ap.error("--force cannot be combined with --end-year")

    try:
        run(
            force=args.force,
            entities=args.entities,
            transactions=args.transactions,
            start_year=getattr(args, "start_year", None),
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
