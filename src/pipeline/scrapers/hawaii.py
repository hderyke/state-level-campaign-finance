"""
scrapers/hawaii.py — Download Hawaii Campaign Spending Commission (CSC) data
via the Socrata Open Data (SODA) API at hicscdata.hawaii.gov.

Pure HTTP/requests — no Playwright needed.

Datasets (14 total), all on hicscdata.hawaii.gov:

  Candidate Committee (CC) side — Schedules A-F:
    cc_contributions   jexd-xbcg  Contributions Received (Sched A)
    cc_expenditures    3maa-4fgr  Expenditures Made (Sched B)
    cc_other_receipts  ue3d-efjr  Other Receipts (Sched C)
    cc_loans           yf4f-x3r4  Loans Received (Sched D)
    cc_unpaid          rrkr-p5kv  Unpaid Expenditures (Sched E)
    cc_durable         fmfj-bac2  Durable Assets (Sched F)

  Noncandidate Committee (NC / PAC) side:
    nc_contributions             rajm-32md  Contributions Received (Sched A)
    nc_contributions_to_candidates 6huc-dcuw  Contributions Made To Candidates (Sched B1)
    nc_expenditures               riiu-7d4b  Expenditures Made (Sched B2)
    nc_other_receipts             m822-j8iy  Other Receipts (Sched C)
    nc_unpaid                     dq35-6ks5  Unpaid Expenditures (Sched D)
    nc_durable                    i778-my94  Durable Assets (Sched E)

  Entity registries (all years, full pull, no year split):
    soi         hc7x-8745  Statement of Intent — candidate registry
    affidavits  3fbc-bviy  Affidavits — candidate registry

The 12 transaction datasets are split by year using
`$where=date_extract_y(date)={year}` (the `date between ...` / `date>=...`
syntax returns empty results on this Socrata instance — date_extract_y is the
working approach). The 2 entity datasets are pulled in full each run.

Raw files (data/Hawaii/raw/):
  {Stem}_{year}.csv   — transaction datasets, one file per relation per year
  SOI_all.csv         — Statement of Intent, all years
  Affidavits_all.csv  — Affidavits, all years
"""

import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Hawaii" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Hawaii" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]

BASE_URL  = "https://hicscdata.hawaii.gov/resource/{id}.json"
PAGE_SIZE = 50000

# ============================== datasets ===============================
# field lists are used as csv.DictWriter fieldnames (extrasaction="ignore",
# restval="") so every output row has a stable column set regardless of
# which fields a given page happens to populate.

DATASETS = {
    "cc_contributions": {
        "id": "jexd-xbcg", "stem": "CCSchedA",
        "fields": ["candidate_name", "contributor_type", "contributor_name", "date",
                   "amount", "aggregate", "employer", "occupation",
                   "street_address_1", "street_address_2", "city", "state", "zip_code",
                   "non_resident_yes_or_no_", "non_monetary_yes_or_no",
                   "non_monetary_category", "non_monetary_description",
                   "office", "district", "county", "party", "reg_no",
                   "election_period", "mapping_address", "inoutstate", "range"],
    },
    "cc_expenditures": {
        "id": "3maa-4fgr", "stem": "CCSchedB",
        "fields": ["candidate_name", "vendor_type", "vendor_name", "date", "amount",
                   "authorized_use", "expenditure_category", "purpose_of_expenditure",
                   "address_1", "city", "state", "zip_code",
                   "office", "district", "reg_no", "election_period",
                   "inoutstate", "location_1"],
    },
    "cc_other_receipts": {
        "id": "ue3d-efjr", "stem": "CCSchedC",
        "fields": ["candidate_name", "source_type", "source_name", "date", "amount",
                   "other_receipt_category", "other_receipt_description",
                   "address_1", "city", "state", "zip_code",
                   "office", "reg_no", "election_period"],
    },
    "cc_loans": {
        "id": "yf4f-x3r4", "stem": "CCSchedD",
        "fields": ["candidate_name", "lender_type", "lender_name", "date", "amount",
                   "loan_type", "loan_source", "purpose_of_loan", "repay_amount",
                   "loan_id", "forgiven", "address_1", "city", "state", "zip_code",
                   "office", "district", "county", "party", "reg_no",
                   "election_period", "location_1"],
    },
    "cc_unpaid": {
        "id": "rrkr-p5kv", "stem": "CCSchedE",
        "fields": ["candidate_name", "vendor_type", "vendor_name", "date", "amount",
                   "authorized_use", "expenditure_category", "purpose_of_expenditure",
                   "unpaid_expenditure_id", "forgiven", "repay_amount",
                   "address_1", "city", "state", "zip_code",
                   "office", "reg_no", "election_period"],
    },
    "cc_durable": {
        "id": "fmfj-bac2", "stem": "CCSchedF",
        "fields": ["candidate_name", "vendor_type", "vendor_name", "date", "amount",
                   "durable_asset_description", "diposition_amount", "durable_asset_id",
                   "address_1", "city", "state", "zip_code",
                   "office", "district", "county", "party", "reg_no",
                   "election_period", "location_1", "method", "to_whom"],
    },
    "nc_contributions": {
        "id": "rajm-32md", "stem": "NCSchedA",
        "fields": ["noncandidate_committee_name", "contributor_type", "contributor_name",
                   "date", "amount", "aggregate",
                   "address_1", "city", "state", "zip_code",
                   "non_monetary_yes_or_no", "reg_no", "election_period", "location_1"],
    },
    "nc_contributions_to_candidates": {
        "id": "6huc-dcuw", "stem": "NCSchedB1",
        "fields": ["noncandidate_committee_name", "candidate_name", "cc_reg_no",
                   "candidate_committee_name", "date", "amount", "aggregate",
                   "address_1", "city", "state", "zip_code",
                   "non_monetary_yes_or_no", "non_monetary_category", "non_monetary_description",
                   "reg_no", "election_period", "office", "district", "county", "party"],
    },
    "nc_expenditures": {
        "id": "riiu-7d4b", "stem": "NCSchedB2",
        "fields": ["noncandidate_committee_name", "vendor_type", "vendor_name", "date",
                   "amount", "expenditure_category", "purpose_of_expenditure",
                   "independent_expenditure", "candidate_name_s", "support_oppose",
                   "address_1", "address_2", "city", "state", "zip_code",
                   "reg_no", "election_period", "location_1"],
    },
    "nc_other_receipts": {
        "id": "m822-j8iy", "stem": "NCSchedC",
        "fields": ["noncandidate_committee_name", "source_type", "source_name", "date",
                   "amount", "other_receipt_category", "other_receipt_description",
                   "address_1", "address_2", "city", "state", "zip_code",
                   "reg_no", "election_period", "location_1"],
    },
    "nc_unpaid": {
        "id": "dq35-6ks5", "stem": "NCSchedD",
        "fields": ["noncandidate_committee_name", "vendor_type", "vendor_name", "date",
                   "amount", "expenditure_category", "purpose_of_exenditure",
                   "independent_expenditure", "unpaid_expenditure_id", "forgiven",
                   "repay_amount", "address_1", "address_2", "city", "state", "zip_code",
                   "reg_no", "election_period"],
    },
    "nc_durable": {
        "id": "i778-my94", "stem": "NCSchedE",
        "fields": ["noncandidate_committee_name", "vendor_type", "vendor_name", "date",
                   "amount", "durable_asset_description", "diposition_amount",
                   "durable_asset_id", "address_1", "address_2", "city", "state", "zip_code",
                   "reg_no", "election_period", "method", "to_whom"],
    },
    "soi": {
        "id": "hc7x-8745", "stem": "SOI_all",
        "fields": ["candidate_name", "statement_of_intent_filing_date", "minimum_qc",
                   "maximum_pf_per_election", "office", "district", "county",
                   "reg_no", "election_period", "election", "np"],
    },
    "affidavits": {
        "id": "3fbc-bviy", "stem": "Affidavits_all",
        "fields": ["candidate_name", "affidavit_filing_date", "expenditure_limit_per_election",
                   "office", "district", "county",
                   "reg_no", "election_period", "election", "np"],
    },
}

TRANSACTION_RELATIONS = [k for k in DATASETS if k not in ("soi", "affidavits")]
ENTITY_RELATIONS      = ["soi", "affidavits"]

# Used to scope --contributions / --expenditures
CONTRIBUTION_LIKE = {
    "cc_contributions", "cc_other_receipts", "cc_loans",
    "nc_contributions", "nc_contributions_to_candidates", "nc_other_receipts",
}
EXPENDITURE_LIKE = {
    "cc_expenditures", "cc_unpaid", "cc_durable",
    "nc_expenditures", "nc_unpaid", "nc_durable",
}


# ========================== Manifest helpers ==========================
def load_manifest() -> set[tuple[str, str]]:
    """Return set of (relation_type, year) already recorded in the manifest."""
    done: set[tuple[str, str]] = set()
    if not MANIFEST.exists():
        return done
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["year"]))
    return done


def strip_manifest(keep_fn) -> None:
    """Rewrite the manifest keeping only rows where keep_fn(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict) -> None:
    """Add or overwrite the manifest entry matching (relation_type, year)."""
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            existing = [
                r for r in csv.DictReader(f)
                if not (r["relation_type"] == record["relation_type"]
                        and r["year"] == record["year"])
            ]
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)


# ============================ Socrata helpers ===========================
def _request(dataset_id: str, params: dict, retries: int = 4) -> list[dict]:
    """GET one page from a Socrata dataset, retrying on transient errors."""
    url = BASE_URL.format(id=dataset_id)
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise last_err


def get_date_range(dataset_id: str) -> tuple[int | None, int | None]:
    """Return (min_year, max_year) for a dataset's `date` field."""
    data = _request(dataset_id, {"$select": "min(date) as min_date, max(date) as max_date"})
    if not data:
        return None, None
    row     = data[0]
    min_str = row.get("min_date") or ""
    max_str = row.get("max_date") or ""
    min_y   = int(min_str[:4]) if min_str else None
    max_y   = int(max_str[:4]) if max_str else None
    return min_y, max_y


def fetch_dataset(dataset_id: str, fields: list[str], out_path: Path,
                   where: str | None = None) -> int:
    """Page through a Socrata dataset (optionally filtered by `where`),
    writing rows to out_path as CSV. Returns total row count."""
    total = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", restval="")
        writer.writeheader()
        offset = 0
        while True:
            params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}
            if where:
                params["$where"] = where
            page = _request(dataset_id, params)
            if not page:
                break
            for row in page:
                clean_row = {
                    k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
                    for k, v in row.items()
                }
                writer.writerow(clean_row)
            total += len(page)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            time.sleep(0.2)
    return total


# ============================ orchestrator ============================
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
    """Orchestrate download of Hawaii CSC transaction + entity data.

    Vertical scope (mutually exclusive):
        force=True              — re-download everything in scope, wipe relevant manifest entries
        start_year / end_year   — restrict year-based downloads to this range

    Horizontal scope:
        No flags                — download everything
        transactions            — all 12 CC/NC transaction datasets
        entities                — SOI + Affidavits candidate registries
        contributions           — "money in" schedules (CC A/C/D, NC A/B1/C)
        expenditures            — "money out" schedules (CC B/E/F, NC B2/D/E)
        candidates / committees — SOI + Affidavits (no separate committee registry exists)
    """
    log = get_logger("hawaii", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Hawaii scraper")
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    files_ok = files_err = 0
    current_year = datetime.today().year

    try:
        # ── Resolve horizontal scope ───────────────────────────────────
        no_horizontal = not (entities or transactions or contributions or
                              expenditures or candidates or committees)

        if no_horizontal or transactions:
            txn_relations = list(TRANSACTION_RELATIONS)
        else:
            txn_relations = []
            if contributions:
                txn_relations += [k for k in TRANSACTION_RELATIONS if k in CONTRIBUTION_LIKE]
            if expenditures:
                txn_relations += [k for k in TRANSACTION_RELATIONS if k in EXPENDITURE_LIKE]

        do_entities = no_horizontal or entities or candidates or committees
        entity_relations = list(ENTITY_RELATIONS) if do_entities else []

        # ── Scoped manifest clearing ───────────────────────────────────
        if force:
            relations_to_clear = set(txn_relations) | set(entity_relations)
            strip_manifest(lambda r: r["relation_type"] not in relations_to_clear)
        elif start_year is not None or end_year is not None:
            txn_set = set(txn_relations)

            def _outside_range(r: dict) -> bool:
                if r["relation_type"] not in txn_set:
                    return True
                try:
                    yr = int(r["year"])
                except ValueError:
                    return True
                if start_year is not None and yr < start_year:
                    return True
                if end_year is not None and yr > end_year:
                    return True
                return False

            strip_manifest(_outside_range)

        done = load_manifest()

        # ── Transaction datasets, year-split ───────────────────────────
        for relation in txn_relations:
            cfg    = DATASETS[relation]
            stem   = cfg["stem"]
            fields = cfg["fields"]

            log.info(f"\nHawaii {relation} ({cfg['id']}):")

            min_y, max_y = get_date_range(cfg["id"])
            if min_y is None:
                log.warning(f"  [!] Could not determine date range for {relation} — skipping")
                continue

            # Future-dated filings (next election cycle) can exceed "today";
            # pad by one year so those rows aren't missed.
            max_y = max(max_y, current_year) + 1
            log.info(f"  Date range: {min_y}-{max_y}")

            for year in range(min_y, max_y + 1):
                if start_year is not None and year < start_year:
                    continue
                if end_year is not None and year > end_year:
                    continue

                key           = (relation, str(year))
                expected_stem = f"{stem}_{year}.csv"
                expected_file = RAW_DIR / expected_stem

                year_range_active = start_year is not None or end_year is not None
                already_done = key in done or (
                    not year_range_active
                    and expected_file.exists()
                    and expected_file.stat().st_size > 0
                )

                if already_done and year != current_year and not force:
                    log.file_download_skip(filename=expected_stem)
                    continue

                log.file_download_start(filename=expected_stem)
                t_file  = time.perf_counter()
                try:
                    where     = f"date_extract_y(date)={year}"
                    row_count = fetch_dataset(cfg["id"], fields, expected_file, where=where)
                except Exception as e:
                    log.file_download_error(filename=expected_stem, error=str(e))
                    files_err += 1
                    continue

                size = expected_file.stat().st_size
                log.file_download_ok(filename=expected_stem, bytes=size, rows=row_count,
                                      duration_s=time.perf_counter() - t_file)
                files_ok += 1
                upsert_manifest({
                    "relation_type": relation,
                    "year":          str(year),
                    "filename":      expected_stem,
                    "row_count":     row_count,
                })
                done.add(key)
                time.sleep(0.3)

        # ── Entity registries, full pull ───────────────────────────────
        for relation in entity_relations:
            cfg    = DATASETS[relation]
            stem   = cfg["stem"]
            fields = cfg["fields"]

            expected_stem = f"{stem}.csv"
            expected_file = RAW_DIR / expected_stem
            key           = (relation, "all")

            already_done = key in done or (
                expected_file.exists() and expected_file.stat().st_size > 0
            )

            if already_done and not force:
                log.file_download_skip(filename=expected_stem)
                continue

            log.info(f"\nHawaii {relation} ({cfg['id']}):")
            log.file_download_start(filename=expected_stem)
            t_file = time.perf_counter()
            try:
                row_count = fetch_dataset(cfg["id"], fields, expected_file)
            except Exception as e:
                log.file_download_error(filename=expected_stem, error=str(e))
                files_err += 1
                continue

            size = expected_file.stat().st_size
            log.file_download_ok(filename=expected_stem, bytes=size, rows=row_count,
                                  duration_s=time.perf_counter() - t_file)
            files_ok += 1
            upsert_manifest({
                "relation_type": relation,
                "year":          "all",
                "filename":      expected_stem,
                "row_count":     row_count,
            })
            done.add(key)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
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


# ====== CLI ==================================
if __name__ == "__main__":
    # Vertical scope (mutually exclusive):
    #   (no flag)                    incremental — current year + fill manifest gaps
    #   --start-year / --end-year    year range only (transaction datasets)
    #   --force                      re-download everything in scope, wipe manifest entries
    #
    # Horizontal scope:
    #   (no flag)         all types
    #   --transactions    all 12 CC/NC transaction datasets
    #   --entities        SOI + Affidavits candidate registries
    #   --contributions   "money in" schedules (CC A/C/D, NC A/B1/C)
    #   --expenditures    "money out" schedules (CC B/E/F, NC B2/D/E)
    #   --candidates      SOI + Affidavits
    #   --committees      SOI + Affidavits (no separate committee registry exists)
    import argparse
    ap = argparse.ArgumentParser(
        description="Download Hawaii CSC campaign finance data via Socrata."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, wipe relevant manifest entries")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive, transaction datasets)")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive); use with or without --start-year")

    ap.add_argument("--transactions", action="store_true",
                    help="all 12 CC/NC transaction datasets only")
    ap.add_argument("--entities",     action="store_true",
                    help="SOI + Affidavits candidate registries only")

    ap.add_argument("--contributions", action="store_true",
                    help="'money in' schedules only (CC A/C/D, NC A/B1/C)")
    ap.add_argument("--expenditures",  action="store_true",
                    help="'money out' schedules only (CC B/E/F, NC B2/D/E)")
    ap.add_argument("--candidates",    action="store_true",
                    help="SOI + Affidavits candidate registries only")
    ap.add_argument("--committees",    action="store_true",
                    help="SOI + Affidavits candidate registries only")

    args, _ = ap.parse_known_args()

    cy = datetime.today().year
    if args.end_year:
        if args.end_year > cy + 2:
            ap.error(f"--end-year cannot exceed {cy + 2}")
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
