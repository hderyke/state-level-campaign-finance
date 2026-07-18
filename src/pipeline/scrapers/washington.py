"""
scrapers/washington.py — Download Washington Public Disclosure Commission (PDC)
campaign finance data via the Socrata Open Data (SODA) API at data.wa.gov.

Pure HTTP/requests — no Playwright needed.

Datasets (4 total), all on data.wa.gov:

    contributions  kv7h-kjye  Contributions to Candidates and Political Committees
    expenditures   tijg-9zyp  Expenditures by Candidates and Political Committees
    debt           3r6b-hsaa  Debt Reported by Candidates and Political Committees
    loans          d2ig-r3q4  Loans to Candidates and Political Committees

Every row on all 4 datasets already carries the filer's identity inline
(filer_id/filer_name/office/district/party/jurisdiction + committee_id) — PDC
does not publish a separate candidate/committee registry dataset, so
src/pipeline/parsers/washington.py builds the candidates/committees tables
directly from these 4 files.

Each dataset is split by year using `$where=date_extract_y({date_field})={year}`
(same approach used by scrapers/hawaii.py — `date between ...` is avoided since
it's the less reliable form on Socrata). A handful of rows per dataset have a
NULL date or a garbage year (e.g. "202", "1024", "2202" — data-entry errors in
the source, confirmed by querying `date_extract_y` distributions directly
against the API), which would otherwise force the year loop to span
centuries. Those rows are swept into one extra "misc" bucket per dataset via
`$where=({date_field} IS NULL) OR (date_extract_y({date_field}) < {EARLIEST_YEAR})
OR (date_extract_y({date_field}) > {MAX_YEAR})` so nothing is silently dropped.

Raw files (data/Washington/raw/):
  {Stem}_{year}.csv   — one file per relation per year, {year} in 2000..MAX_YEAR
  {Stem}_misc.csv      — NULL-date / out-of-range rows for that relation
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
RAW_DIR  = PROJECT_ROOT / "data" / "Washington" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Washington" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]

BASE_URL  = "https://data.wa.gov/resource/{id}.json"
PAGE_SIZE = 50000

# WA's real data starts ~2000 (a handful of stray pre-2000 rows are data-entry
# errors, e.g. "1900", "1916", "1024" — swept into the misc bucket below rather
# than making the year loop span centuries).
EARLIEST_YEAR = 2000

# ============================== datasets ===============================
# field lists are used as csv.DictWriter fieldnames (extrasaction="ignore",
# restval="") so every output row has a stable column set regardless of
# which fields a given page happens to populate. `date_field` drives the
# per-year $where split.

DATASETS = {
    "contributions": {
        "id": "kv7h-kjye", "stem": "Contributions", "date_field": "receipt_date",
        "fields": ["id", "report_number", "origin", "committee_id", "fund_id",
                   "filer_id", "type", "filer_name", "office", "legislative_district",
                   "position", "party", "ballot_number", "for_or_against",
                   "jurisdiction", "jurisdiction_county", "jurisdiction_type",
                   "election_year", "amount", "cash_or_in_kind", "receipt_date",
                   "description", "memo", "primary_general", "code",
                   "contributor_category", "contributor_name", "contributor_address",
                   "contributor_city", "contributor_state", "contributor_zip",
                   "contributor_occupation", "contributor_employer_name",
                   "contributor_employer_city", "contributor_employer_state",
                   "url", "contributor_location"],
    },
    "expenditures": {
        "id": "tijg-9zyp", "stem": "Expenditures", "date_field": "expenditure_date",
        "fields": ["id", "report_number", "origin", "committee_id", "fund_id",
                   "filer_id", "type", "filer_name", "office", "legislative_district",
                   "position", "party", "ballot_number", "for_or_against",
                   "jurisdiction", "jurisdiction_county", "jurisdiction_type",
                   "election_year", "amount", "itemized_or_non_itemized",
                   "expenditure_date", "description", "code", "recipient_name",
                   "recipient_address", "recipient_city", "recipient_state",
                   "recipient_zip", "url", "recipient_location", "payee", "creditor"],
    },
    "debt": {
        "id": "3r6b-hsaa", "stem": "Debt", "date_field": "debt_date",
        "fields": ["id", "report_number", "origin", "committee_id", "fund_id",
                   "filer_id", "filer_type", "filer_name", "office",
                   "legislative_district", "position", "party", "jurisdiction",
                   "jurisdiction_county", "jurisdiction_type", "election_year",
                   "amount", "record_type", "from_date", "thru_date", "debt_date",
                   "code", "description", "vendor_name", "vendor_address",
                   "vendor_city", "vendor_state", "vendor_zip", "url"],
    },
    "loans": {
        "id": "d2ig-r3q4", "stem": "Loans", "date_field": "receipt_date",
        "fields": ["id", "report_number", "origin", "committee_id", "fund_id",
                   "filer_id", "type", "filer_name", "office", "legislative_district",
                   "position", "party", "jurisdiction", "jurisdiction_county",
                   "jurisdiction_type", "election_year", "cash_or_in_kind",
                   "receipt_date", "repayment_schedule", "loan_due_date",
                   "lender_or_endorser", "transaction_type", "amount",
                   "endorser_liable_amount", "primary_general", "lenders_name",
                   "lenders_address", "lenders_city", "lenders_state", "lenders_zip",
                   "lenders_occupation", "lenders_employer", "employers_city",
                   "employers_state", "url", "carry_forward_loan", "description"],
    },
}

RELATIONS = list(DATASETS)

# Used to scope --contributions / --expenditures
CONTRIBUTION_LIKE = {"contributions"}
EXPENDITURE_LIKE  = {"expenditures"}
DEBT_LOAN_LIKE    = {"debt", "loans"}


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
    start_year: int | None = None,
    end_year: int | None = None,
    contributions: bool = False,
    expenditures: bool = False,
    candidates: bool = False,
    committees: bool = False,
):
    """Orchestrate download of Washington PDC data.

    Vertical scope (mutually exclusive):
        force=True              — re-download everything in scope, wipe relevant manifest entries
        start_year / end_year   — restrict downloads to this range (misc bucket is always
                                   re-fetched since it isn't year-bounded)

    Horizontal scope:
        No flags                — download all 4 relations
        contributions            — contributions only
        expenditures              — expenditures only
        candidates / committees — all 4 relations (no separate registry dataset exists —
                                   candidates/committees are built from these transaction
                                   files by the parser, so there's nothing narrower to scope to)
    """
    log = get_logger("washington", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Washington scraper")
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    files_ok = files_err = 0
    current_year = datetime.today().year
    max_year = current_year + 2   # pad for next-cycle future filings

    try:
        # ── Resolve horizontal scope ───────────────────────────────────
        no_horizontal = not (contributions or expenditures or candidates or committees)

        if no_horizontal or candidates or committees:
            relations = list(RELATIONS)
        else:
            relations = []
            if contributions:
                relations += [r for r in RELATIONS if r in CONTRIBUTION_LIKE]
            if expenditures:
                relations += [r for r in RELATIONS if r in EXPENDITURE_LIKE]

        # ── Scoped manifest clearing ───────────────────────────────────
        if force:
            relations_to_clear = set(relations)
            strip_manifest(lambda r: r["relation_type"] not in relations_to_clear)
        elif start_year is not None or end_year is not None:
            rel_set = set(relations)

            def _outside_range(r: dict) -> bool:
                if r["relation_type"] not in rel_set:
                    return True
                if r["year"] == "misc":
                    return True   # misc bucket always re-fetched on a scoped run
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

        for relation in relations:
            cfg        = DATASETS[relation]
            stem       = cfg["stem"]
            fields     = cfg["fields"]
            date_field = cfg["date_field"]

            log.info(f"\nWashington {relation} ({cfg['id']}):")

            for year in range(EARLIEST_YEAR, max_year + 1):
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
                    where     = f"date_extract_y({date_field})={year}"
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

            # ── Misc bucket: NULL date or garbage year (out of [EARLIEST_YEAR, max_year]) ──
            misc_stem = f"{stem}_misc.csv"
            misc_file = RAW_DIR / misc_stem
            misc_key  = (relation, "misc")

            if misc_key in done and not force and not (start_year is not None or end_year is not None):
                log.file_download_skip(filename=misc_stem)
            else:
                log.file_download_start(filename=misc_stem)
                t_file = time.perf_counter()
                try:
                    where = (
                        f"{date_field} IS NULL OR "
                        f"date_extract_y({date_field}) < {EARLIEST_YEAR} OR "
                        f"date_extract_y({date_field}) > {max_year}"
                    )
                    row_count = fetch_dataset(cfg["id"], fields, misc_file, where=where)
                except Exception as e:
                    log.file_download_error(filename=misc_stem, error=str(e))
                    files_err += 1
                else:
                    size = misc_file.stat().st_size
                    log.file_download_ok(filename=misc_stem, bytes=size, rows=row_count,
                                          duration_s=time.perf_counter() - t_file)
                    files_ok += 1
                    upsert_manifest({
                        "relation_type": relation,
                        "year":          "misc",
                        "filename":      misc_stem,
                        "row_count":     row_count,
                    })
                    done.add(misc_key)

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
    #   --start-year / --end-year    year range only
    #   --force                      re-download everything in scope, wipe manifest entries
    #
    # Horizontal scope:
    #   (no flag)         all 4 relations
    #   --contributions   contributions only
    #   --expenditures    expenditures only
    #   --candidates      all 4 relations (no separate registry dataset — see docs)
    #   --committees      all 4 relations (no separate registry dataset — see docs)
    import argparse
    ap = argparse.ArgumentParser(
        description="Download Washington PDC campaign finance data via Socrata."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, wipe relevant manifest entries")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive); use with or without --start-year")

    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="all 4 relations (no separate registry dataset exists)")
    ap.add_argument("--committees",    action="store_true",
                    help="all 4 relations (no separate registry dataset exists)")

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
