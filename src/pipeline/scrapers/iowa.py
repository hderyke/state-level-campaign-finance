"""
scrapers/iowa.py — Download Iowa campaign finance data.

POSTs to the Iowa Ethics and Campaign Disclosure Board public reports API:
  https://webapp.iecdb.iowa.gov/api/publicreports/state
which returns all ~17,000+ DR-2 filing records as JSON, each with a fileUrl
pointing to a pre-generated PDF on Azure Blob Storage. Both the index API
and the individual PDF downloads require no authentication.

The index is always fetched on every run (~7.5 MB). PDFs are downloaded
incrementally — already-present files are skipped unless they belong to the
current year (which are always re-checked for amendments) or --force is set.

When a committee amends a filing, Iowa generates a new PDF with a new
filename (timestamp in name). The index always returns the latest version.
Old amendment files persist on disk as orphans but are never re-parsed,
since the parser reads only files listed in the manifest.
"""

import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Iowa" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Iowa" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [
    "file_id", "period_year", "period_description", "report_type",
    "organization_type", "candidate_name", "committee_code", "committee_name",
    "filed_on", "filename", "file_url", "downloaded_at",
]

# ============================ constants ==============================

INDEX_URL = "https://webapp.iecdb.iowa.gov/api/publicreports/state"

# Required headers — without Content-Type/Origin the API returns 400.
REQUEST_HEADERS = {
    "User-Agent":    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/147.0.0.0 Safari/537.36",
    "Content-Type":  "application/json",
    "Accept":        "application/json, text/plain, */*",
    "Referer":       "https://webapp.iecdb.iowa.gov/publicReports/state-reports",
    "Origin":        "https://webapp.iecdb.iowa.gov",
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

# ========================= manifest helpers ==========================

def load_manifest() -> dict[str, dict]:
    """Return dict of filename → manifest row for all downloaded files."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {row["filename"]: row for row in csv.DictReader(f)}


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


# ========================== index fetch =============================

def fetch_index(session: requests.Session) -> list[dict]:
    """
    POST to the public reports state endpoint and return all filing records.
    The empty JSON body triggers a full dump (no filters).
    """
    resp = session.post(INDEX_URL, json={}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    return results


# ========================== download =================================

def download_pdf(session: requests.Session, file_url: str,
                 dest: Path, log) -> bool:
    """Download a single PDF to dest. Returns True on success."""
    try:
        r = session.get(file_url, timeout=120, stream=True)
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
        return True
    except Exception as e:
        log.file_download_error(filename=dest.name, error=str(e))
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
    Download Iowa DR-2 filing PDFs from the IECDB public reports API.

    Iowa doesn't separate entities vs transactions — everything is in the
    PDFs, so horizontal scope flags are ignored. Vertical scope (year range
    and --force) is fully supported.

    Vertical scope:
        (no flag)        incremental — skip already-downloaded files;
                         always re-check current-year PDFs for amendments
        --start-year     re-download all PDFs with period_year >= YYYY
        --end-year       re-download all PDFs with period_year <= YYYY
        --force          wipe manifest and re-download everything
    """
    log = get_logger("iowa", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year)

    current_year     = datetime.today().year
    current_year_str = str(current_year)
    year_range_active = start_year is not None or end_year is not None

    files_ok = files_err = 0

    try:
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)

        # ── Fetch the full index ──────────────────────────────────────
        log.info("Fetching filing index from IECDB public reports API…")
        t_idx = time.perf_counter()
        try:
            records = fetch_index(session)
        except Exception as e:
            log._emit("scrape_completed", status="error",
                      duration_s=round(time.perf_counter() - t0, 1),
                      files_ok=0, files_err=0,
                      error_type=type(e).__name__, error=str(e))
            raise

        log.info(f"  Index: {len(records):,} filings "
                 f"({round(time.perf_counter() - t_idx, 1)}s)")

        # ── Filter by year range ──────────────────────────────────────
        if start_year is not None:
            records = [r for r in records if int(r.get("periodYear", 0)) >= start_year]
        if end_year is not None:
            records = [r for r in records if int(r.get("periodYear", 0)) <= end_year]

        # ── Prepare manifest ──────────────────────────────────────────
        if force:
            # If year range active with --force that's blocked by CLI validation,
            # but handle gracefully: wipe only in-range years.
            if year_range_active:
                def _in_range(r):
                    try:
                        yr = int(r["period_year"])
                    except (ValueError, KeyError):
                        return True
                    if start_year is not None and yr >= start_year:
                        return False
                    if end_year is not None and yr <= end_year:
                        return False
                    return True
                strip_manifest(_in_range)
            else:
                strip_manifest(lambda _: False)
            done = {}
        elif year_range_active:
            # Wipe manifest entries in the requested range so we re-download them.
            def _outside_range(r):
                try:
                    yr = int(r["period_year"])
                except (ValueError, KeyError):
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

        # ── Download PDFs ─────────────────────────────────────────────
        today = datetime.today().strftime("%Y-%m-%d")

        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(records, desc="  iowa PDFs", unit="pdf",
                      dynamic_ncols=True,
                      total=len(records)) as bar:
                for rec in bar:
                    filename    = rec.get("fileName", "")
                    file_url    = rec.get("fileUrl", "")
                    period_year = str(rec.get("periodYear", ""))
                    file_id     = str(rec.get("id", ""))

                    if not filename or not file_url:
                        bar.set_postfix_str("skip:no-url", refresh=False)
                        continue

                    dest = RAW_DIR / filename
                    is_current_year = (period_year == current_year_str)

                    # Skip if already downloaded and not current year /
                    # not in a forced re-download range.
                    if filename in done and not year_range_active:
                        if not is_current_year:
                            log.file_download_skip(filename=filename)
                            bar.set_postfix_str(filename[:40], refresh=False)
                            continue
                        # Current year: skip only if file still on disk
                        # (unchanged filename = no new amendment)
                        if dest.exists() and dest.stat().st_size > 0:
                            log.file_download_skip(filename=filename)
                            bar.set_postfix_str(filename[:40], refresh=False)
                            continue

                    log.file_download_start(filename=filename)
                    t_file = time.perf_counter()

                    ok = download_pdf(session, file_url, dest, log)
                    if not ok:
                        files_err += 1
                        bar.set_postfix_str(f"ERR:{filename[:30]}", refresh=False)
                        continue

                    log.file_download_ok(
                        filename=filename,
                        bytes=dest.stat().st_size,
                        rows=0,    # PDFs don't have a meaningful "row count" at scrape time
                        duration_s=round(time.perf_counter() - t_file, 2),
                    )

                    # Upsert manifest — remove old entry for same file_id if any
                    # (handles the case where a filing was amended and filename changed)
                    append_manifest({
                        "file_id":            file_id,
                        "period_year":        period_year,
                        "period_description": rec.get("periodDescription", ""),
                        "report_type":        rec.get("reportType", ""),
                        "organization_type":  rec.get("organizationType", ""),
                        "candidate_name":     rec.get("candidateName") or "",
                        "committee_code":     rec.get("committeeCode", ""),
                        "committee_name":     rec.get("committeeName", ""),
                        "filed_on":           rec.get("filedOn", ""),
                        "filename":           filename,
                        "file_url":           file_url,
                        "downloaded_at":      today,
                    })
                    done[filename] = {}
                    files_ok += 1
                    bar.set_postfix_str(filename[:40], refresh=False)

                    # Polite delay — blob storage is fast but ~17k requests warrants courtesy
                    time.sleep(0.05)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok:,} downloaded, {files_err} errors")
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
        description="Download Iowa campaign finance PDFs from the IECDB public reports API."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all PDFs in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest period_year to download (inclusive)")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="latest period_year to download (inclusive, ≤ current year)")

    ap.add_argument("--transactions", action="store_true",
                    help="(ignored — Iowa PDFs contain all data types)")
    ap.add_argument("--entities",     action="store_true",
                    help="(ignored — Iowa PDFs contain all data types)")
    ap.add_argument("--contributions", action="store_true")
    ap.add_argument("--expenditures",  action="store_true")
    ap.add_argument("--candidates",    action="store_true")
    ap.add_argument("--committees",    action="store_true")

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
