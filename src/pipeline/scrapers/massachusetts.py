"""
scrapers/massachusetts.py — Download Massachusetts campaign finance data.

Bulk ZIP downloads from the Massachusetts Office of Campaign and Political Finance
(OCPF) via Azure Blob Storage (ocpf2.blob.core.windows.net):
  - Filer database (entities): ocpf-filers.zip — all registered filers, updated nightly
  - Transaction reports by year (2002–present): ocpf-{year}-reports.zip — one per year

Each year ZIP contains:
  reports.txt       — filing summaries with CPF_ID, office, district, report year
  report-items.txt  — all transaction items (contributions, expenditures, etc.),
                       tagged by Record_Type_ID

The filer ZIP contains all_filers.txt with every committee, candidate, PAC, and
local party committee registered with OCPF.

No authentication required. All URLs are public Azure blob endpoints; no headers needed.
Data is updated nightly at 03:30 Eastern.
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

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Massachusetts" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Massachusetts" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "downloaded_at", "size_bytes"]

# ============================ constants ==============================

BASE_URL   = "https://ocpf2.blob.core.windows.net/downloads/data2"
FILERS_URL = f"{BASE_URL}/ocpf-filers.zip"

# Earliest year available in the OCPF bulk download system
START_YEAR = 2002


# ========================= manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    """Return set of (relation_type, year) already downloaded."""
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="") as f:
        return {(row["relation_type"], row["year"])
                for row in csv.DictReader(f)}


def strip_manifest(keep_fn):
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict):
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


# ========================= download helpers ==========================

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    })
    return s


def _download_zip(log, session: requests.Session, url: str,
                  out_path: Path, label: str) -> tuple[bool, int]:
    """
    Stream-download a ZIP to out_path.
    Returns (success, size_bytes).
    Uses a 30s connection timeout and 600s read timeout — year ZIPs can be
    up to ~100 MB and the read timeout needs headroom for slow connections.
    """
    log.file_download_start(filename=label)
    t0 = time.perf_counter()
    try:
        resp = session.get(url, stream=True, timeout=(30, 600))
        resp.raise_for_status()
        size = 0
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                size += len(chunk)
        log.file_download_ok(
            filename=label,
            bytes=size,
            rows=0,    # row count not knowable without extracting
            duration_s=round(time.perf_counter() - t0, 2),
        )
        return True, size
    except Exception as e:
        log.file_download_error(filename=label, error=str(e))
        if out_path.exists():
            out_path.unlink()
        return False, 0


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
    Orchestrate download of OCPF filer database and transaction year ZIPs.

    Vertical scope (mutually exclusive):
        force=True              — re-download everything in scope, wipe manifest
        start_year / end_year   — restrict transaction downloads to this year range

    Horizontal scope:
        No flags                — download everything
        transactions            — year ZIPs only (all transactions in one file per year)
        entities                — filer database only
        contributions / expenditures — same as --transactions (no source-level split)
        candidates / committees — same as --entities (no source-level split)

    Note: OCPF does not split contributions from expenditures at the source — all
    transaction types are in a single report-items.txt per year ZIP.  The filer
    database similarly bundles candidates, PACs, and LPCs into all_filers.txt.
    The horizontal fine-grained flags are accepted but treated as their parent scope.
    """
    log = get_logger("massachusetts", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)
    do_entities     = no_horizontal or entities or candidates or committees
    do_transactions = no_horizontal or transactions or contributions or expenditures

    current_year     = datetime.today().year
    current_year_str = str(current_year)
    range_start      = start_year if start_year is not None else START_YEAR
    years = [
        str(y) for y in range(range_start, current_year + 1)
        if (end_year is None or y <= end_year)
    ]

    files_ok = files_err = 0

    try:
        session = _make_session()

        # ── Filer database (entities) ─────────────────────────────────
        # Always re-fetch on any run that includes entities — filer registrations
        # change continuously and there is no year-based versioning for this file.
        if do_entities:
            out_path = RAW_DIR / "ocpf-filers.zip"
            ok, size = _download_zip(log, session, FILERS_URL, out_path, "ocpf-filers.zip")
            if ok:
                files_ok += 1
                today = datetime.today().strftime("%Y-%m-%d")
                strip_manifest(lambda r: r["relation_type"] != "entities")
                append_manifest({
                    "relation_type": "entities",
                    "year":          "filers",
                    "filename":      "ocpf-filers.zip",
                    "downloaded_at": today,
                    "size_bytes":    size,
                })
            else:
                files_err += 1

        # ── Transaction year ZIPs ─────────────────────────────────────
        if do_transactions:
            if force:
                # Wipe all transaction manifest entries and start fresh
                strip_manifest(lambda r: r["relation_type"] != "transactions")
                done = set()
            else:
                done = load_manifest()

            year_range_explicit = start_year is not None or end_year is not None

            if year_range_explicit and not force:
                # Wipe in-range manifest entries so the loop re-downloads them
                def _outside_range(r: dict) -> bool:
                    if r["relation_type"] != "transactions":
                        return True
                    try:
                        yr = int(r["year"])
                    except (ValueError, KeyError):
                        return True
                    if start_year is not None and yr < start_year:
                        return True
                    if end_year is not None and yr > end_year:
                        return True
                    return False  # within range — wipe
                strip_manifest(_outside_range)
                done = load_manifest()

            for year in years:
                filename = f"ocpf-{year}-reports.zip"
                out_path = RAW_DIR / filename
                key      = ("transactions", year)

                # Skip already-downloaded years unless it's the current year or
                # the user explicitly requested a year range
                if key in done and year != current_year_str and not year_range_explicit:
                    log.file_download_skip(filename=filename)
                    files_ok += 1
                    continue

                url = f"{BASE_URL}/{filename}"
                ok, size = _download_zip(log, session, url, out_path, filename)
                if ok:
                    files_ok += 1
                    append_manifest({
                        "relation_type": "transactions",
                        "year":          year,
                        "filename":      filename,
                        "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                        "size_bytes":    size,
                    })
                    done.add(key)
                else:
                    files_err += 1

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
        description="Download Massachusetts OCPF campaign finance data."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all years in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")

    ap.add_argument("--end-year",      type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")
    ap.add_argument("--transactions",  action="store_true",
                    help="transaction year ZIPs only")
    ap.add_argument("--entities",      action="store_true",
                    help="filer database only")
    ap.add_argument("--contributions", action="store_true",
                    help="contributions (same as --transactions — no source-level split)")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditures (same as --transactions — no source-level split)")
    ap.add_argument("--candidates",    action="store_true",
                    help="candidates (same as --entities — no source-level split)")
    ap.add_argument("--committees",    action="store_true",
                    help="committees (same as --entities — no source-level split)")

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
