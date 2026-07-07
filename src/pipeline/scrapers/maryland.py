"""
scrapers/maryland.py — Download Maryland campaign finance data.

POSTs to the Maryland State Board of Elections campaign finance JSON API:
  https://api-campaignfinance.maryland.gov/api/ExportPublicData/GetExportPublicDownloadData

  - Contributions & Loans: transactionTypeCode=TCON, per filing year
  - Expenditures:          transactionTypeCode=TEXP, per filing year
  - Committees:            transactionTypeCode=TCMD, filingYear=0 (all at once)

No authentication required — only Content-Type and Accept headers needed.
Data begins in 2021; earlier filing years return empty responses.
Response is text/csv (CRLF line endings) with a timestamp title row on
line 0 followed by a standard header row on line 1. Some fields (ZipCode)
use Excel-style quoting (="VALUE") — stripped in the parser.
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
RAW_DIR  = PROJECT_ROOT / "data" / "Maryland" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Maryland" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["transaction_type", "year", "filename", "downloaded_at", "row_count"]

# ============================ constants ================================

API_URL = "https://api-campaignfinance.maryland.gov/api/ExportPublicData/GetExportPublicDownloadData"

TRANSACTION_TYPES = {
    "TCON": "contributions",
    "TEXP": "expenditures",
}

# Earliest filing year that returns data — 2020 and earlier return empty responses.
START_YEAR = 2021

# ========================= manifest helpers ===========================

def load_manifest() -> set[tuple[str, str]]:
    """Return set of (transaction_type_code, year) already downloaded."""
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="") as f:
        return {(row["transaction_type"], row["year"])
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
    """Create a requests session with headers that avoid 403s from the .NET backend."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36",
        "Accept":       "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Referer":      "https://campaignfinance.maryland.gov/",
        "Origin":       "https://campaignfinance.maryland.gov",
    })
    return s


def _decode_response(content: bytes) -> str:
    """Detect and normalize UTF-16 or BOM-prefixed UTF-8 to plain UTF-8 string."""
    if content[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return content.decode("utf-16")
    if len(content) > 1 and content[1] == 0:
        return content.decode("utf-16-le")
    if content[:3] == b"\xef\xbb\xbf":
        return content[3:].decode("utf-8")
    return content.decode("utf-8", errors="replace")


# ============================ committees ============================

def download_committees(log, session: requests.Session) -> tuple[str, int] | None:
    """
    Fetch all registered committees in a single request (filingYear=0).
    Returns (filename, row_count) or None on failure.
    Always re-fetched — committee registrations change continuously.
    """
    filename = "committees.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    payload = {
        "filingYear":          "0",
        "transactionTypeCode": "TCMD",
        "type":                "CSV",
        "fileName":            "committees",
        "openInNewTab":        False,
    }

    try:
        resp = session.post(API_URL, json=payload, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    text = _decode_response(resp.content)
    out_path.write_text(text, encoding="utf-8")
    # Row count: subtract 2 for the title row and header row
    row_count = max(text.count("\n") - 2, 0)

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


# ========================== transactions ============================

def download_transaction(log, transaction_type: str, year: str,
                         session: requests.Session) -> tuple[str, int] | None:
    """
    POST to the Maryland CF API and save the CSV response.
    Returns (filename, row_count) or None on failure.
    """
    label    = TRANSACTION_TYPES[transaction_type]
    filename = f"{label}_{year}.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    payload = {
        "filingYear":          year,
        "transactionTypeCode": transaction_type,
        "type":                "CSV",
        "fileName":            filename,
        "openInNewTab":        False,
    }

    try:
        resp = session.post(API_URL, json=payload, timeout=300)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    text = _decode_response(resp.content)

    # Empty response (pre-2021 years return 0 bytes — skip silently)
    if not text.strip():
        log.file_download_skip(filename=filename)
        return filename, 0

    out_path.write_text(text, encoding="utf-8")
    # Subtract 2 for the title row and header row
    row_count = max(text.count("\n") - 2, 0)

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


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
    """Orchestrate download of Maryland campaign finance data.

    Vertical scope (mutually exclusive):
        force=True              — re-download all years in scope, wipe manifest
        start_year / end_year   — restrict transaction downloads to this year range

    Horizontal scope:
        No flags                — download everything
        transactions            — contributions + expenditures only
        entities                — committees only
        contributions           — contributions only
        expenditures            — expenditures only
        candidates / committees — treated as entities (single committee file)

    Note: committees are always re-downloaded regardless of manifest — they
    update continuously as new committees register throughout the cycle.
    Year flags do not apply to committees (single all-committee request).
    """
    log = get_logger("maryland", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    # ── Resolve granular scope ────────────────────────────────────────
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    do_transactions  = no_horizontal or transactions or contributions or expenditures
    do_entities      = no_horizontal or entities or candidates or committees

    # Transaction type filter
    if contributions and not expenditures:
        active_tx_types = {"TCON": "contributions"}
    elif expenditures and not contributions:
        active_tx_types = {"TEXP": "expenditures"}
    else:
        active_tx_types = TRANSACTION_TYPES

    # Year range — from start_year (or START_YEAR floor) to current year
    current_year     = datetime.today().year
    range_start      = start_year if start_year is not None else START_YEAR
    years            = [
        str(y) for y in range(range_start, current_year + 1)
        if (end_year is None or y <= end_year)
    ]
    current_year_str = str(current_year)

    files_ok = files_err = 0

    try:
        session = _make_session()

        # ── Committees ────────────────────────────────────────────────
        # Always re-download — no manifest check. Committees register year-round
        # and a stale file quickly becomes out of date.
        if do_entities:
            result = download_committees(log, session)
            if result:
                filename, row_count = result
                # Upsert manifest entry for committees
                strip_manifest(lambda r: r["transaction_type"] != "TCMD")
                append_manifest({
                    "transaction_type": "TCMD",
                    "year":             "0",
                    "filename":         filename,
                    "downloaded_at":    datetime.today().strftime("%Y-%m-%d"),
                    "row_count":        row_count,
                })
                log.info(f"  committees: {row_count:,} rows")
                files_ok += 1
            else:
                files_err += 1

        # ── Transactions ──────────────────────────────────────────────
        if do_transactions:
            if force:
                strip_manifest(lambda r: r["transaction_type"] in active_tx_types)
                done = set()
            else:
                done = load_manifest()

            year_range_explicit = start_year is not None or end_year is not None
            for transaction_type, label in active_tx_types.items():
                for year in years:
                    key = (transaction_type, year)
                    if key in done and year != current_year_str and not year_range_explicit:
                        log.file_download_skip(filename=f"{label}_{year}.csv")
                        continue

                    result = download_transaction(log, transaction_type, year, session)
                    if result is None:
                        files_err += 1
                        continue

                    filename, row_count = result
                    append_manifest({
                        "transaction_type": transaction_type,
                        "year":             year,
                        "filename":         filename,
                        "downloaded_at":    datetime.today().strftime("%Y-%m-%d"),
                        "row_count":        row_count,
                    })
                    done.add(key)
                    files_ok += 1
                    time.sleep(0.5)

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
        description="Download Maryland campaign finance data from the SBE bulk export API."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all years in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")

    ap.add_argument("--transactions", action="store_true",
                    help="transactions only (contributions + expenditures)")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (committees)")
    ap.add_argument("--contributions", action="store_true",
                    help="contributions only")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="committees only (alias; MD has no separate candidate file)")
    ap.add_argument("--committees",    action="store_true",
                    help="committees only")

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
