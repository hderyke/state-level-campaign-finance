"""
scrapers/arkansas.py — Download Arkansas campaign finance data.

POSTs to the Arkansas Ethics Commission JSON API:
  - Transactions: GetExportPublicDownloadData with transactionTypeCode (TCON/TEXP)
    and filingYear — returns raw CSV content.
  - Entities: GetCandidateCommitteDetails with an open query (pageSize 25,000) —
    returns a JSON array split into candidates.csv and committees.csv.

No authentication required. Referer/Origin headers are required to avoid 403s.
Response may arrive as UTF-16 (common for .NET portals) — detected and normalized
to UTF-8 before writing.

Downloads are tracked in manifest.csv — re-running skips already-fetched years
except the current year, which is always re-fetched.
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
RAW_DIR  = PROJECT_ROOT / "data" / "Arkansas" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Arkansas" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["transaction_type", "year", "filename", "downloaded_at", "row_count"]

# ============================ constants ==============================

TRANSACTION_API = "https://api-ethics-disclosures.sos.arkansas.gov/api/ExportData/GetExportPublicDownloadData"
ENTITY_API      = "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFilerDetails/GetCandidateCommitteDetails"

TRANSACTION_TYPES = {
    "TCON": "contributions",
    "TEXP": "expenditures",
}

# Earliest year available on the portal — auto-extended to current year at runtime
START_YEAR = 2022

# Entity filer type codes — SFIFILER is personal financial disclosure, not campaign finance
CANDIDATE_CODES = {"CAN"}
COMMITTEE_CODES = {"PAC", "CPAC", "IEF", "PP", "ECOMM"}

ENTITY_FIELDS = [
    "filerEntityID", "filerEntityVersionID", "filerTypeCode", "filerType",
    "firstName", "lastName", "suffix", "filerName", "committeeName",
    "office", "officeDistrictName", "jurisdictionName",
    "politicalParty", "filerStatus", "electionYear", "filingYear",
    "totalRaised", "totalSpent", "balanceofFunds",
    "filingTypeCode", "isPaperFiler", "guid",
]

# ========================= manifest helpers ==========================

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
    """Create a session with the headers required to avoid 403s."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Referer":      "https://ethics-disclosures.sos.arkansas.gov/",
        "Origin":       "https://ethics-disclosures.sos.arkansas.gov",
    })
    return s


def _decode_response(content: bytes) -> str:
    """Detect and normalize UTF-16 or UTF-8 BOM encoded content to plain UTF-8."""
    if content[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return content.decode("utf-16")
    if len(content) > 1 and content[1] == 0:
        return content.decode("utf-16-le")
    if content[:3] == b"\xef\xbb\xbf":
        return content[3:].decode("utf-8")
    return content.decode("utf-8", errors="replace")


# ============================ entities ==============================

def download_entities(log, session: requests.Session,
                      write_candidates: bool = True,
                      write_committees: bool = True) -> tuple[int, int] | None:
    """
    Fetch all campaign finance filers from the public registry API.
    Always fetches the full result (single API call — no server-side filtering).
    Splits results into candidates.csv and/or committees.csv based on write_* flags.
    Returns (candidate_count, committee_count) or None on failure.
    """
    label = (
        "candidates.csv + committees.csv" if (write_candidates and write_committees)
        else "candidates.csv" if write_candidates
        else "committees.csv"
    )
    log.file_download_start(filename=label)
    t0 = time.perf_counter()

    payload = {
        "filerTypeCode": "", "accountStatus": "", "filerName": "",
        "OfficeSought": "", "election": "", "politicalPartyCode": "",
        "jurisdictionType": "", "jurisdiction": "",
        "totalRaisedMin": None, "totalRaisedMax": None,
        "totalSpentMin": None, "totalSpentMax": None,
        "balanceFundsMin": None, "balanceFundsMax": None,
        "transactionSourceTypeCode": None,
        "pageNumber": 1, "pageSize": 25000,
    }

    try:
        resp = session.post(ENTITY_API, json=payload, timeout=60)
        resp.raise_for_status()
        items = resp.json()["data"]["items"]
    except Exception as e:
        log.file_download_error(filename=label, error=str(e))
        return None

    candidates = [r for r in items if r.get("filerTypeCode") in CANDIDATE_CODES]
    committees = [r for r in items if r.get("filerTypeCode") in COMMITTEE_CODES]

    to_write = []
    if write_candidates: to_write.append(("candidates.csv", candidates))
    if write_committees: to_write.append(("committees.csv", committees))

    for filename, rows in to_write:
        out_path = RAW_DIR / filename
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ENTITY_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    total = len(candidates) + len(committees)
    log.file_download_ok(
        filename=label,
        bytes=len(resp.content),
        rows=total,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return len(candidates), len(committees)


# ========================== transactions ============================

def download_transaction(log, transaction_type: str, year: str,
                         session: requests.Session) -> tuple[str, int] | None:
    """
    POST to the Arkansas ethics API and save the CSV response.
    Returns (filename, row_count) or None on failure.
    """
    label    = TRANSACTION_TYPES[transaction_type]
    filename = f"{label}_{year}.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    payload = {
        "transactionTypeCode": transaction_type,
        "type":                "CSV",
        "filingYear":          year,
    }

    try:
        resp = session.post(TRANSACTION_API, json=payload, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    text = _decode_response(resp.content)
    out_path.write_text(text, encoding="utf-8")
    row_count = max(text.count("\n") - 1, 0)

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, row_count


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
    """Orchestrate download of transaction files and/or entity registry.

    Vertical scope (mutually exclusive):
        force=True              — re-download all years in scope, wipe manifest
        start_year / end_year   — restrict transaction downloads to this year range

    Horizontal scope:
        No flags                — download everything
        transactions            — contributions + expenditures
        entities                — candidates + committees registry
        contributions           — contributions only
        expenditures            — expenditures only
        candidates              — candidates registry only
        committees              — committees registry only

    Note: entities are a single API call regardless of candidate/committee split —
    both are always fetched; write_* flags only control which files are written.
    Year flags do not apply to entities (no year param on the registry endpoint).
    """
    log = get_logger("arkansas", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    # ── Resolve granular scope ────────────────────────────────────────
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    do_transactions    = no_horizontal or transactions or contributions or expenditures
    do_entities        = no_horizontal or entities or candidates or committees
    write_candidates   = no_horizontal or entities or candidates
    write_committees   = no_horizontal or entities or committees

    # Transaction type filter
    if contributions and not expenditures:
        active_tx_types = {k: v for k, v in TRANSACTION_TYPES.items() if v == "contributions"}
    elif expenditures and not contributions:
        active_tx_types = {k: v for k, v in TRANSACTION_TYPES.items() if v == "expenditures"}
    else:
        active_tx_types = TRANSACTION_TYPES

    # Year range — from start_year (or START_YEAR floor) to current year
    current_year = datetime.today().year
    range_start  = start_year if start_year is not None else START_YEAR
    years = [
        str(y) for y in range(range_start, current_year + 1)
        if (end_year is None or y <= end_year)
    ]
    current_year_str = str(current_year)

    files_ok = files_err = 0

    try:
        session = _make_session()

        # ── Entities ──────────────────────────────────────────────────
        if do_entities:
            result = download_entities(log, session,
                                       write_candidates=write_candidates,
                                       write_committees=write_committees)
            if result:
                cand_count, comm_count = result
                log.info(f"  candidates: {cand_count:,}  committees: {comm_count:,}")
                today = datetime.today().strftime("%Y-%m-%d")
                strip_manifest(lambda r: r["transaction_type"] != "entities")
                if write_candidates:
                    append_manifest({"transaction_type": "entities", "year": "candidates",
                                     "filename": "candidates.csv", "downloaded_at": today,
                                     "row_count": cand_count})
                if write_committees:
                    append_manifest({"transaction_type": "entities", "year": "committees",
                                     "filename": "committees.csv", "downloaded_at": today,
                                     "row_count": comm_count})
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

            # When a year range is explicitly requested, re-download all years in scope.
            # Otherwise (incremental), skip already-fetched years except the current one.
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
    # Vertical scope (mutually exclusive):
    #   (no flag)                    incremental — fill gaps, always refresh current year
    #   --start-year / --end-year    restrict to this year range
    #   --force                      wipe manifest entries in scope, re-download all
    #
    # Horizontal scope:
    #   (no flag)         all types
    #   --transactions    contributions + expenditures
    #   --entities        candidates + committees registry
    #   --contributions   contributions only
    #   --expenditures    expenditures only
    #   --candidates      candidates registry only
    #   --committees      committees registry only
    ap = argparse.ArgumentParser(
        description="Download Arkansas campaign finance data from the Ethics Commission API."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")

    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (candidates + committees registry)")
    ap.add_argument("--contributions", action="store_true",
                    help="contributions only")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="candidates registry only")
    ap.add_argument("--committees",    action="store_true",
                    help="committees registry only")

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
