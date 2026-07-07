"""
scrapers/louisiana.py — Download Louisiana campaign finance data.

Bulk CSV exports from the Louisiana Board of Ethics campaign finance portal:
  https://www.ethics.la.gov/CampaignFinanceSearch/ShowPremadereports.aspx

Three transaction types are available as pre-built CSVs organized by 4-year
range (1995-and-earlier through 2024-2027):
  contributions  → ContributionReports/Contributions_{slug}.csv
  expenditures   → ExpenditureReports/Expenditures_{slug}.csv
  loans          → LoanReports/Loans_{slug}.csv

9 files per type (27 total). The range containing the current year is always
re-fetched. Older ranges are cached via manifest.

No authentication or session handling required — all are direct GET URLs.
No separate filer registry endpoint; candidates/committees are built from
unique FilerNumbers in transaction files by the parser.

Manifest (data/Louisiana/manifest.csv):
    file_type, filename, rows, downloaded_at
"""

import csv
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# ================================ paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Louisiana" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Louisiana" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["file_type", "filename", "rows", "downloaded_at"]

# ============================= constants ==============================
BASE_URL = "https://www.ethics.la.gov/Pub/CampFinan/DataDownload"

# (range_start, range_end) — range_start=None means "1995 and earlier"
YEAR_RANGES = [
    (None, 1995),
    (1996, 1999),
    (2000, 2003),
    (2004, 2007),
    (2008, 2011),
    (2012, 2015),
    (2016, 2019),
    (2020, 2023),
    (2024, 2027),
]

DATA_TYPES = {
    "contributions": ("ContributionReports", "Contributions"),
    "expenditures":  ("ExpenditureReports",  "Expenditures"),
    "loans":         ("LoanReports",          "Loans"),
}

SESSION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://www.ethics.la.gov/CampaignFinanceSearch/ShowPremadereports.aspx",
}

# ========================== range helpers =============================

def _slug(rs, re) -> str:
    """Build the year-range slug used in filenames and URLs."""
    return f"{re}_and_earlier" if rs is None else f"{rs}_to_{re}"


def _range_in_scope(rs, re, start_year, end_year) -> bool:
    """Returns True if year range (rs, re) overlaps with [start_year, end_year]."""
    eff_rs = rs if rs is not None else 0
    lo = start_year if start_year is not None else 0
    hi = end_year   if end_year   is not None else 9999
    return re >= lo and eff_rs <= hi


def _current_range_slug() -> str:
    """Return the slug for the range that contains the current calendar year."""
    cy = datetime.today().year
    for rs, re in YEAR_RANGES:
        eff_rs = rs if rs is not None else 0
        if eff_rs <= cy <= re:
            return _slug(rs, re)
    # Fallback — should never happen unless YEAR_RANGES is stale
    return _slug(*YEAR_RANGES[-1])


# ========================= manifest helpers ===========================

def load_manifest() -> dict[str, dict]:
    """Return {filename: row} for all manifest entries."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {row["filename"]: row for row in csv.DictReader(f)}


def strip_manifest(keep_fn) -> None:
    """Rewrite manifest keeping only rows where keep_fn(row) is True."""
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
    if filename in done:
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
        mode = "w" if write_header else "a"
        with open(MANIFEST, mode, newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
            if write_header:
                w.writeheader()
            w.writerow(record)
    done[filename] = record


# ============================ HTTP helper =============================

def _get(session: requests.Session, url: str, retries: int = 4,
         timeout: int = 300) -> requests.Response:
    last_err = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(4 * (attempt + 1))
    raise last_err


def _row_count(content: bytes) -> int:
    """Count newlines as a proxy for CSV row count."""
    return content.count(b"\n")


# ========================= download helpers ===========================

def _download_file(session, url: str, dest: Path, file_type: str,
                   filename: str, done: dict, log, today: str,
                   force_this: bool = False) -> bool:
    """
    Download url → dest. Skip if already in manifest and file exists,
    unless force_this=True. Returns True on success.
    """
    if not force_this and filename in done and dest.exists() and dest.stat().st_size > 0:
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


# ============================== run ===================================

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
    Download Louisiana Ethics Board campaign finance CSVs.

    Horizontal scope:
        (no flag)          download everything (all 3 transaction types)
        --transactions     contributions + expenditures
        --contributions    contributions only
        --expenditures     expenditures only
        --entities         no-op (no separate entity download for LA)
        --candidates       no-op (candidates built from transactions by parser)
        --committees       no-op (committees built from transactions by parser)

    Vertical scope (applies to all file types):
        (no flag)          incremental — skip existing files; always re-download
                           the current 4-year range
        --start-year YYYY  re-download ranges whose end year >= YYYY
        --end-year YYYY    re-download ranges whose start year <= YYYY
        --force            wipe manifest, re-download everything
    """
    log = get_logger("louisiana", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year)

    # Resolve which types to download
    do_all          = not any([transactions, contributions, expenditures,
                                entities, candidates, committees])
    do_contributions = do_all or transactions or contributions
    do_expenditures  = do_all or transactions or expenditures
    do_loans         = do_all  # loans always with everything; no separate flag

    today         = datetime.today().strftime("%Y-%m-%d")
    current_slug  = _current_range_slug()
    year_range_active = start_year is not None or end_year is not None
    files_ok = files_err = 0

    try:
        session = requests.Session()
        session.headers.update(SESSION_HEADERS)

        # ── Manifest prep ─────────────────────────────────────────────
        if force:
            strip_manifest(lambda _: False)
            done = {}
        elif year_range_active:
            # Wipe entries for ranges that overlap with the requested window
            def _keep(r: dict) -> bool:
                # file_type format: "contributions_2020_to_2023"
                parts = r.get("file_type", "").split("_", 1)
                if len(parts) != 2:
                    return True
                slug = parts[1]
                for rs, re in YEAR_RANGES:
                    if _slug(rs, re) == slug:
                        # Keep if NOT in scope (i.e. outside the wipe window)
                        return not _range_in_scope(rs, re, start_year, end_year)
                return True
            strip_manifest(_keep)
            done = load_manifest()
        else:
            done = load_manifest()

        # ── Download each enabled type ─────────────────────────────────
        type_map = {}
        if do_contributions:
            type_map["contributions"] = DATA_TYPES["contributions"]
        if do_expenditures:
            type_map["expenditures"] = DATA_TYPES["expenditures"]
        if do_loans:
            type_map["loans"] = DATA_TYPES["loans"]

        for dtype, (url_dir, url_prefix) in type_map.items():
            log.info(f"  Downloading {dtype}…")
            for rs, re in YEAR_RANGES:
                slug     = _slug(rs, re)
                filename = f"{dtype}_{slug}.csv"
                dest     = RAW_DIR / filename
                file_type = f"{dtype}_{slug}"

                # Re-fetch current range and anything in the year-range window
                is_current = (slug == current_slug)
                force_this = force or (year_range_active and
                                       _range_in_scope(rs, re, start_year, end_year))

                if not force_this and not is_current:
                    # Normal incremental — skip if in manifest and file exists
                    if filename in done and dest.exists() and dest.stat().st_size > 0:
                        log.file_download_skip(filename=filename)
                        continue

                url = f"{BASE_URL}/{url_dir}/{url_prefix}_{slug}.csv"
                ok = _download_file(session, url, dest, file_type,
                                    filename, done, log, today,
                                    force_this=True)
                files_ok += ok
                files_err += not ok
                time.sleep(0.5)

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
        description="Download Louisiana Ethics Board campaign finance CSVs."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all files, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="re-download ranges whose end year >= YYYY")

    ap.add_argument("--end-year",      type=int, metavar="YYYY",
                    help="re-download ranges whose start year <= YYYY")

    ap.add_argument("--transactions",  action="store_true",
                    help="contributions + expenditures only")
    ap.add_argument("--entities",      action="store_true",
                    help="no-op for Louisiana (entities built from transactions)")
    ap.add_argument("--contributions", action="store_true")
    ap.add_argument("--expenditures",  action="store_true")
    ap.add_argument("--candidates",    action="store_true",
                    help="no-op for Louisiana")
    ap.add_argument("--committees",    action="store_true",
                    help="no-op for Louisiana")

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
