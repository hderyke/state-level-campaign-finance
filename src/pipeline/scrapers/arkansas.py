import csv
import time
from datetime import datetime
from pathlib import Path

import requests

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "arkansas" / "raw"
MANIFEST     = PROJECT_ROOT / "data" / "arkansas" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

TRANSACTION_API = "https://api-ethics-disclosures.sos.arkansas.gov/api/ExportData/GetExportPublicDownloadData"
ENTITY_API      = "https://api-ethics-disclosures.sos.arkansas.gov/api/PublicFilerDetails/GetCandidateCommitteDetails"

MANIFEST_COLS = ["transaction_type", "year", "filename", "downloaded_at", "row_count"]

# ── Data types ─────────────────────────────────────────────────────────────────
# From the site's downloads table — transactionTypeCode → label
TRANSACTION_TYPES = {
    "TCON": "contributions",
    "TEXP": "expenditures",
}

# Years available on the downloads page (2022–present)
YEARS = ["2022", "2023", "2024", "2025", "2026"]

# Entity filer type codes — exclude SFIFILER (financial disclosure, not campaign finance)
CANDIDATE_CODES  = {"CAN"}
COMMITTEE_CODES  = {"PAC", "CPAC", "IEF", "PP", "ECOMM"}


# ── Manifest helpers ──────────────────────────────────────────────────────────
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


# ── Entity download ───────────────────────────────────────────────────────────
ENTITY_FIELDS = [
    "filerEntityID", "filerEntityVersionID", "filerTypeCode", "filerType",
    "firstName", "lastName", "suffix", "filerName", "committeeName",
    "office", "officeDistrictName", "jurisdictionName",
    "politicalParty", "filerStatus", "electionYear", "filingYear",
    "totalRaised", "totalSpent", "balanceofFunds",
    "filingTypeCode", "isPaperFiler", "guid",
]


def download_entities(session: requests.Session) -> tuple[int, int] | None:
    """
    Fetch all campaign finance filers from the public registry API.
    Splits into candidates.csv and committees.csv.
    Returns (candidate_count, committee_count) or None on failure.
    """
    print("  Arkansas entities...", end=" ", flush=True)

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
        print(f"failed: {e}")
        return None

    candidates  = [r for r in items if r.get("filerTypeCode") in CANDIDATE_CODES]
    committees  = [r for r in items if r.get("filerTypeCode") in COMMITTEE_CODES]

    for filename, rows in [("candidates.csv", candidates), ("committees.csv", committees)]:
        out_path = RAW_DIR / filename
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ENTITY_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    return len(candidates), len(committees)


# ── Transaction download ──────────────────────────────────────────────────────
def download(transaction_type: str, year: str,
             session: requests.Session) -> tuple[str, int] | None:
    """
    POST to the Arkansas ethics API and save the CSV response.
    Returns (filename, row_count) or None on failure.
    """
    label    = TRANSACTION_TYPES[transaction_type]
    filename = f"{label}_{year}.csv"
    out_path = RAW_DIR / filename

    payload = {
        "transactionTypeCode": transaction_type,
        "type":                "CSV",
        "filingYear":          year,
    }

    try:
        resp = session.post(TRANSACTION_API, json=payload, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"failed: {e}")
        return None

    # Response is the raw CSV content
    out_path.write_bytes(resp.content)

    # Detect encoding (may be UTF-16 like other .NET sites)
    raw = resp.content
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw.decode("utf-16")
    elif len(raw) > 1 and raw[1] == 0:
        text = raw.decode("utf-16-le")
    elif raw[:3] == b"\xef\xbb\xbf":
        text = raw[3:].decode("utf-8")
    else:
        text = raw.decode("utf-8", errors="replace")

    out_path.write_text(text, encoding="utf-8")
    row_count = max(text.count("\n") - 1, 0)
    return filename, row_count


# ── Main runner ───────────────────────────────────────────────────────────────
def run(force: bool = False, update_transactions: bool = False,
        update_entities: bool = False):
    current_year = str(datetime.today().year)

    if force:
        if MANIFEST.exists():
            MANIFEST.unlink()
        done = set()
    elif update_transactions:
        strip_manifest(lambda r: r["year"] != current_year)
        done = load_manifest()
    else:
        done = load_manifest()

    session = requests.Session()
    session.headers.update({
        "User-Agent":   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Referer":      "https://ethics-disclosures.sos.arkansas.gov/",
        "Origin":       "https://ethics-disclosures.sos.arkansas.gov",
    })

    # ── Entities ──────────────────────────────────────────────────────────────
    if update_entities or force:
        result = download_entities(session)
        if result:
            cand_count, comm_count = result
            print(f"→ candidates.csv ({cand_count:,} rows), "
                  f"committees.csv ({comm_count:,} rows)")
            today = datetime.today().strftime("%Y-%m-%d")
            # Update manifest entries for entities (remove old, write new)
            strip_manifest(lambda r: r["transaction_type"] != "entities")
            append_manifest({"transaction_type": "entities", "year": "candidates",
                             "filename": "candidates.csv", "downloaded_at": today,
                             "row_count": cand_count})
            append_manifest({"transaction_type": "entities", "year": "committees",
                             "filename": "committees.csv", "downloaded_at": today,
                             "row_count": comm_count})
        else:
            print("failed — will retry next run")

    # If only updating entities, stop here
    if update_entities and not force:
        print("Arkansas: done.")
        return

    # ── Transactions ──────────────────────────────────────────────────────────
    for transaction_type, label in TRANSACTION_TYPES.items():
        for year in YEARS:
            if update_transactions and year != current_year:
                continue

            key = (transaction_type, year)
            if key in done and year != current_year:
                print(f"  Arkansas {label} {year}: already downloaded — skipping")
                continue

            print(f"  Arkansas {label} {year}...", end=" ", flush=True)
            result = download(transaction_type, year, session)

            if result is None:
                print("failed — will retry next run")
                continue

            filename, row_count = result
            print(f"→ {filename} ({row_count:,} rows)")
            append_manifest({
                "transaction_type": transaction_type,
                "year":             year,
                "filename":         filename,
                "downloaded_at":    datetime.today().strftime("%Y-%m-%d"),
                "row_count":        row_count,
            })
            done.add(key)
            time.sleep(0.5)

    print("Arkansas: done.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",               action="store_true")
    ap.add_argument("--update-transactions", action="store_true")
    ap.add_argument("--update-entities",     action="store_true")
    args = ap.parse_args()
    run(force=args.force,
        update_transactions=args.update_transactions,
        update_entities=args.update_entities)
