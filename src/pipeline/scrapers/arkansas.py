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

# Years available on the downloads page — update annually
YEARS = ["2022", "2023", "2024", "2025", "2026"]

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

def download_entities(log, session: requests.Session) -> tuple[int, int] | None:
    """
    Fetch all campaign finance filers from the public registry API.
    Splits results into candidates.csv and committees.csv.
    Returns (candidate_count, committee_count) or None on failure.
    """
    log.file_download_start(filename="entities (candidates + committees)")
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
        log.file_download_error(filename="entities", error=str(e))
        return None

    candidates = [r for r in items if r.get("filerTypeCode") in CANDIDATE_CODES]
    committees = [r for r in items if r.get("filerTypeCode") in COMMITTEE_CODES]

    for filename, rows in [("candidates.csv", candidates), ("committees.csv", committees)]:
        out_path = RAW_DIR / filename
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ENTITY_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    total = len(candidates) + len(committees)
    log.file_download_ok(
        filename="candidates.csv + committees.csv",
        bytes=resp.content.__len__(),
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

def run(force: bool = False, entities: bool = False, transactions: bool = False):
    """Orchestrate download of transaction files and/or entity registry."""
    log = get_logger("arkansas", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions)

    # If neither flag is set, do both
    do_both         = not entities and not transactions
    do_transactions = transactions or do_both
    do_entities     = entities     or do_both

    current_year = str(datetime.today().year)
    files_ok = files_err = 0

    try:
        session = _make_session()

        # ── Entities ──────────────────────────────────────────────────
        if do_entities:
            if force:
                strip_manifest(lambda r: r["transaction_type"] != "entities")
            result = download_entities(log, session)
            if result:
                cand_count, comm_count = result
                log.info(f"  candidates: {cand_count:,}  committees: {comm_count:,}")
                today = datetime.today().strftime("%Y-%m-%d")
                strip_manifest(lambda r: r["transaction_type"] != "entities")
                append_manifest({"transaction_type": "entities", "year": "candidates",
                                 "filename": "candidates.csv", "downloaded_at": today,
                                 "row_count": cand_count})
                append_manifest({"transaction_type": "entities", "year": "committees",
                                 "filename": "committees.csv", "downloaded_at": today,
                                 "row_count": comm_count})
                files_ok += 1
            else:
                files_err += 1

        # ── Transactions ──────────────────────────────────────────────
        if do_transactions:
            if force:
                strip_manifest(lambda r: r["transaction_type"] in TRANSACTION_TYPES)
                done = set()
            else:
                done = load_manifest()

            for transaction_type, label in TRANSACTION_TYPES.items():
                for year in YEARS:
                    key = (transaction_type, year)
                    if key in done and year != current_year:
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
        description="Download Arkansas campaign finance data from the Ethics Commission API."
    )
    ap.add_argument("--force",        action="store_true",
                    help="re-download everything, ignoring the manifest")
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (candidates, committees)")
    args = ap.parse_args()
    try:
        run(force=args.force, entities=args.entities, transactions=args.transactions)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
