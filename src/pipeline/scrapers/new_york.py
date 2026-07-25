"""
scrapers/new_york.py — Download New York State Board of Elections (NYSBOE)
campaign finance data via the Socrata Open Data (SODA) API at data.ny.gov.

Pure HTTP/requests — no Playwright, no browser TLS impersonation needed.
data.ny.gov is a stock Socrata tenant and answers plain unauthenticated
`requests` fine; every endpoint, field name, schedule code and row count
quoted below was confirmed directly against the live API while writing this
module, not assumed.

Datasets (4 total), all on data.ny.gov:

    disclosure         e9ss-239a  Campaign Finance Disclosure Reports Data (Beginning 1999)
    filers             7x2g-h32p  Campaign Finance Filer Data (Beginning 1974)
    active_committees  udeh-rt5n  Campaign Finance Active Committees Data
    active_candidates  epr8-9fny  Campaign Finance Active Candidates Data

`disclosure` is the money: ~18.4M transaction rows across 21 lettered
schedules (A–U), holding contributions, expenditures, transfers, loans and
liabilities in a single table (see parsers/new_york.py for the
schedule → canonical-table mapping). It is the only large dataset here — the
other three are entity registries in the tens of thousands of rows.

`filers` is the full historical registry of everyone who has ever registered
with NYSBOE (64,464 rows as of 2026-07): both CANDIDATE and COMMITTEE
records, keyed by the same `filer_id` the disclosure rows carry. It is the
source of office/district/county/treasurer/address for the canonical
candidates + committees tables — the disclosure rows themselves carry only
`cand_comm_name`, no office or party.

`active_committees` / `active_candidates` are the `filer_status = ACTIVE`
slice of `filers`, split by `compliance_type_desc`. They're fetched
separately rather than derived from `filers` because NYSBOE refreshes them
on their own cadence — the parser uses them to set `committees.active` and
to backfill any filer that shows up as active before the full `filers`
extract catches up.

Year splitting
--------------
`disclosure` is split one file per **election year** via
`$where=election_year={year}`. Note this is NYSBOE's "Disclosure Report
Year" (the reporting cycle a transaction was filed under), not the calendar
year of `sched_date` — a January 2011 transaction can legitimately be filed
under election_year 2010. Splitting on `election_year` rather than on
`date_extract_y(sched_date)` (the approach scrapers/washington.py and
scrapers/hawaii.py use) is deliberate: it's the column NYSBOE itself
partitions reporting by, it's populated on effectively every row, and it
matches what `--start-year`/`--end-year` mean to someone asking for "the
2024 cycle".

Observed distribution (live `$select=election_year,count(*)&$group=...`):
1999 through 2027, ~120K–950K rows per year, 18,358,201 rows total. A
`misc` bucket per the usual convention catches NULL / out-of-range values so
nothing is silently dropped if NYSBOE ever loads a row with a garbage year.

Raw files (data/New York/raw/):
  Disclosure_{year}.csv    — one file per election year, {year} in 1999..MAX_YEAR
  Disclosure_misc.csv      — NULL / out-of-range election_year rows
  Filers.csv               — full historical filer registry (snapshot)
  ActiveCommittees.csv     — active committees (snapshot)
  ActiveCandidates.csv     — active candidates (snapshot)
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
RAW_DIR  = PROJECT_ROOT / "data" / "New York" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "New York" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]

BASE_URL  = "https://data.ny.gov/resource/{id}.json"
PAGE_SIZE = 50000

# NYSBOE's disclosure extract starts at election_year 1999 (confirmed via
# `$select=min(election_year)` against the live API — the dataset title says
# "Beginning 1999" and the data agrees). The filer registry goes back to 1974
# but carries no year column to split on, so it's fetched as a snapshot.
EARLIEST_YEAR = 1999

# ============================== datasets ===============================
# `fields` is used as the csv.DictWriter fieldnames (extrasaction="ignore",
# restval="") so every output row has a stable column set regardless of which
# fields a given page happens to populate — Socrata omits NULL keys from its
# JSON rows entirely rather than emitting them as null.
#
# `year_field = None` marks a snapshot relation (no per-year split).

DISCLOSURE_FIELDS = [
    "filer_id", "filer_previous_id", "cand_comm_name", "election_year",
    "election_type", "county_desc", "filing_abbrev", "filing_desc", "r_amend",
    "filing_cat_desc", "filing_sched_abbrev", "filing_sched_desc",
    "loan_lib_number", "trans_number", "trans_mapping", "sched_date",
    "org_date", "cntrbr_type_desc", "cntrbn_type_desc", "transfer_type_desc",
    "receipt_type_desc", "receipt_code_desc", "purpose_code_desc",
    "r_subcontractor", "flng_ent_name", "flng_ent_first_name",
    "flng_ent_middle_name", "flng_ent_last_name", "flng_ent_add1",
    "flng_ent_city", "flng_ent_state", "flng_ent_zip", "flng_ent_country",
    "payment_type_desc", "pay_number", "owed_amt", "org_amt",
    "loan_other_desc", "trans_explntn", "r_itemized", "r_liability",
    "election_year_r", "office_desc", "district", "dist_off_cand_bal_prop",
    "treas_occupation", "treas_employer", "treas_add1", "treas_city",
    "treas_state", "treas_zip", "ie_cntrbr_occ", "ie_cntrbr_emp",
    "r_ie_supported", "r_claim", "r_in_district", "r_minor", "r_vendor",
    "r_lobbyist", "r_contributions", "r_support_oppose",
    "r_is_qualified_expense", "filing_trans_id",
]

# All three registry datasets share one schema. active_candidates has no
# committee/treasurer columns and active_committees has no office/district,
# but Socrata simply omits those keys from the row rather than nulling them —
# DictWriter's restval="" fills the gap, so one shared field list is correct
# for all three and keeps the parser's reader logic uniform.
FILER_FIELDS = [
    "filer_id", "filer_name", "compliance_type_desc", "filer_type_desc",
    "filer_status", "committee_type_desc", "office_desc", "district",
    "county_desc", "municipality_desc_subdivision", "treasurer_first_name",
    "treasurer_middle_name", "treasurer_last_name", "address", "city",
    "state", "zipcode",
]

DATASETS = {
    "disclosure": {
        "id": "e9ss-239a", "stem": "Disclosure",
        "year_field": "election_year", "fields": DISCLOSURE_FIELDS,
    },
    "filers": {
        "id": "7x2g-h32p", "stem": "Filers",
        "year_field": None, "fields": FILER_FIELDS,
    },
    "active_committees": {
        "id": "udeh-rt5n", "stem": "ActiveCommittees",
        "year_field": None, "fields": FILER_FIELDS,
    },
    "active_candidates": {
        "id": "epr8-9fny", "stem": "ActiveCandidates",
        "year_field": None, "fields": FILER_FIELDS,
    },
}

RELATIONS = list(DATASETS)

# Used to resolve the horizontal scope flags. The disclosure dataset holds
# contributions AND expenditures AND loans in one table (distinguished only by
# filing_sched_abbrev at parse time), so --contributions and --expenditures
# both resolve to the same single download — there is nothing narrower to
# scope to without splitting raw files by schedule letter, which would change
# the layout the parser reads for no bandwidth saving.
TRANSACTION_LIKE = {"disclosure"}
CANDIDATE_LIKE   = {"filers", "active_candidates"}
COMMITTEE_LIKE   = {"filers", "active_committees"}


# ========================== Manifest helpers ==========================
def load_manifest() -> set[tuple[str, str]]:
    """Return set of (relation_type, year) already recorded in the manifest.
    Snapshot relations use the literal year value "snapshot"."""
    done: set[tuple[str, str]] = set()
    if not MANIFEST.exists():
        return done
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["year"]))
    return done


def strip_manifest(keep_fn) -> None:
    """Rewrite the manifest keeping only rows where keep_fn(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict) -> None:
    """Add or overwrite the manifest entry matching (relation_type, year)."""
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="", encoding="utf-8") as f:
            existing = [
                r for r in csv.DictReader(f)
                if not (r["relation_type"] == record["relation_type"]
                        and r["year"] == record["year"])
            ]
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)


# ============================ Socrata helpers ===========================
def _request(dataset_id: str, params: dict, retries: int = 4) -> list[dict]:
    """GET one page from a Socrata dataset, retrying on transient errors.

    Backoff is linear (2s, 4s, 6s, 8s) rather than exponential: Socrata's
    throttling for an unauthenticated (no app token) client resets on a
    per-minute window, so a few seconds clears it, and a long exponential
    sleep would just stall an 18M-row pull for no benefit."""
    url = BASE_URL.format(id=dataset_id)
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=120)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise last_err


def fetch_dataset(dataset_id: str, fields: list[str], out_path: Path,
                  where: str | None = None) -> int:
    """Page through a Socrata dataset (optionally filtered by `where`),
    writing rows to out_path as CSV. Returns total row count.

    `$order=:id` pins a stable sort so `$offset` paging can't skip or repeat
    rows — Socrata's default order is unspecified, and paging over an
    unspecified order on a dataset this size silently loses records.

    Written to a `.part` file and renamed on success. Writing straight to
    out_path would truncate it up front, so a failure hundreds of pages into
    an 18M-row year would leave a non-empty partial file with no manifest row
    — and the next incremental run's file-existence fallback would see
    `exists() and st_size > 0`, call the year done, and never re-fetch it."""
    total = 0
    part_path = out_path.with_suffix(out_path.suffix + ".part")
    with open(part_path, "w", newline="", encoding="utf-8") as f:
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
                # Socrata renders location/point columns as nested objects.
                # None of the four NY datasets has one today, but flatten
                # defensively so a future schema addition can't crash the writer.
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
    part_path.replace(out_path)
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
    """Orchestrate download of New York NYSBOE data from data.ny.gov.

    Vertical scope (mutually exclusive; applies to `disclosure` only —
    the three registry datasets are year-less snapshots):
        force=True              — re-download everything in scope, wipe relevant
                                  manifest entries
        start_year / end_year   — restrict the disclosure year loop to this range
                                  (the misc bucket is always re-fetched, since it
                                  isn't year-bounded)

    Horizontal scope (additive):
        No flags                — all 4 datasets
        transactions            — disclosure only
        contributions           — disclosure only (the disclosure table holds
                                  every schedule; it can't be split server-side
                                  without changing the raw file layout)
        expenditures            — disclosure only, same reason
        entities                — filers + active_committees + active_candidates
        candidates              — filers + active_candidates
        committees              — filers + active_committees
    """
    log = get_logger("new york", "scrape")
    t0  = time.perf_counter()
    log.info("Starting New York scraper")
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year,
              entities=entities, transactions=transactions,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    files_ok = files_err = files_skip = 0
    current_year = datetime.today().year
    # Pad two years past the current one: NYSBOE already carries election_year
    # 2027 rows in mid-2026 (candidates filing for the next cycle), so a
    # current-year ceiling would silently drop them.
    max_year = current_year + 2

    try:
        # ── Resolve horizontal scope ───────────────────────────────────
        no_horizontal = not (entities or transactions or contributions or
                             expenditures or candidates or committees)

        if no_horizontal:
            relations = list(RELATIONS)
        else:
            selected: set[str] = set()
            if transactions or contributions or expenditures:
                selected |= TRANSACTION_LIKE
            if entities:
                selected |= CANDIDATE_LIKE | COMMITTEE_LIKE
            if candidates:
                selected |= CANDIDATE_LIKE
            if committees:
                selected |= COMMITTEE_LIKE
            # Preserve the canonical RELATIONS order rather than set order so
            # the log reads the same way on every run.
            relations = [r for r in RELATIONS if r in selected]

        # ── Scoped manifest clearing ───────────────────────────────────
        if force:
            relations_to_clear = set(relations)
            strip_manifest(lambda r: r["relation_type"] not in relations_to_clear)
        elif start_year is not None or end_year is not None:
            rel_set = set(relations)

            def _outside_range(r: dict) -> bool:
                """True = keep this manifest row (it's outside the refresh scope)."""
                if r["relation_type"] not in rel_set:
                    return True
                if r["year"] in ("misc", "snapshot"):
                    return True   # always re-fetched on a scoped run anyway
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
        year_range_active = start_year is not None or end_year is not None

        for relation in relations:
            cfg        = DATASETS[relation]
            stem       = cfg["stem"]
            fields     = cfg["fields"]
            year_field = cfg["year_field"]

            log.info(f"\nNew York {relation} ({cfg['id']}):")

            # ── Snapshot relations (the three registries) ──────────────
            if year_field is None:
                # Always re-fetched in full: live registry snapshots with no
                # year to scope by, small (tens of thousands of rows), and a
                # stale copy is strictly worse than re-pulling.
                filename = f"{stem}.csv"
                dest     = RAW_DIR / filename
                log.file_download_start(filename=filename)
                t_file = time.perf_counter()
                try:
                    row_count = fetch_dataset(cfg["id"], fields, dest)
                except Exception as e:
                    log.file_download_error(filename=filename, error=str(e))
                    files_err += 1
                    continue
                log.file_download_ok(filename=filename, bytes=dest.stat().st_size,
                                     rows=row_count,
                                     duration_s=round(time.perf_counter() - t_file, 2))
                files_ok += 1
                upsert_manifest({
                    "relation_type": relation, "year": "snapshot",
                    "filename": filename, "row_count": row_count,
                })
                done.add((relation, "snapshot"))
                time.sleep(0.3)
                continue

            # ── Year-split relation (disclosure) ───────────────────────
            for year in range(EARLIEST_YEAR, max_year + 1):
                if start_year is not None and year < start_year:
                    continue
                if end_year is not None and year > end_year:
                    continue

                key           = (relation, str(year))
                expected_stem = f"{stem}_{year}.csv"
                expected_file = RAW_DIR / expected_stem

                already_done = key in done or (
                    not year_range_active
                    and expected_file.exists()
                    and expected_file.stat().st_size > 0
                )

                # The current cycle is still being filed against, so it's
                # re-fetched every run regardless of the manifest. Future years
                # (current+1, current+2) are re-fetched for the same reason —
                # they're open cycles, not closed ones.
                if already_done and year < current_year and not force:
                    log.file_download_skip(filename=expected_stem)
                    files_skip += 1
                    continue

                log.file_download_start(filename=expected_stem)
                t_file = time.perf_counter()
                try:
                    where     = f"{year_field}={year}"
                    row_count = fetch_dataset(cfg["id"], fields, expected_file, where=where)
                except Exception as e:
                    log.file_download_error(filename=expected_stem, error=str(e))
                    files_err += 1
                    continue

                log.file_download_ok(filename=expected_stem,
                                     bytes=expected_file.stat().st_size,
                                     rows=row_count,
                                     duration_s=round(time.perf_counter() - t_file, 2))
                files_ok += 1
                upsert_manifest({
                    "relation_type": relation,
                    "year":          str(year),
                    "filename":      expected_stem,
                    "row_count":     row_count,
                })
                done.add(key)
                time.sleep(0.3)

            # ── Misc bucket: NULL or out-of-range election_year ────────
            # No such rows exist today (all 18.36M rows carry an election_year
            # in 1999..2027 — the live group-by sums exactly to the table
            # total), but the bucket costs one request and guarantees a future
            # bad load can't fall through the gap between the year loop's floor
            # and ceiling unnoticed.
            misc_stem = f"{stem}_misc.csv"
            misc_file = RAW_DIR / misc_stem
            misc_key  = (relation, "misc")

            if misc_key in done and not force and not year_range_active:
                log.file_download_skip(filename=misc_stem)
                files_skip += 1
            else:
                log.file_download_start(filename=misc_stem)
                t_file = time.perf_counter()
                try:
                    where = (
                        f"{year_field} IS NULL OR "
                        f"{year_field} < {EARLIEST_YEAR} OR "
                        f"{year_field} > {max_year}"
                    )
                    row_count = fetch_dataset(cfg["id"], fields, misc_file, where=where)
                except Exception as e:
                    log.file_download_error(filename=misc_stem, error=str(e))
                    files_err += 1
                else:
                    log.file_download_ok(filename=misc_stem,
                                         bytes=misc_file.stat().st_size,
                                         rows=row_count,
                                         duration_s=round(time.perf_counter() - t_file, 2))
                    files_ok += 1
                    upsert_manifest({
                        "relation_type": relation,
                        "year":          "misc",
                        "filename":      misc_stem,
                        "row_count":     row_count,
                    })
                    done.add(misc_key)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} downloaded, {files_skip} skipped, "
                 f"{files_err} errors")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err, files_skip=files_skip)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err, files_skip=files_skip)
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
    #   (no flag)                    incremental — current + future years, fill manifest gaps
    #   --start-year / --end-year    year range only
    #   --force                      re-download everything in scope, wipe manifest entries
    #
    # Horizontal scope:
    #   (no flag)         all 4 datasets
    #   --transactions    disclosure only
    #   --contributions   disclosure only (single combined table — see run() docstring)
    #   --expenditures    disclosure only (same)
    #   --entities        filers + active committees + active candidates
    #   --candidates      filers + active candidates
    #   --committees      filers + active committees
    import argparse

    ap = argparse.ArgumentParser(
        description="Download New York NYSBOE campaign finance data via Socrata."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, wipe relevant manifest entries")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest election year to download (inclusive)")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest election year to download (inclusive); use with or without --start-year")

    ap.add_argument("--transactions",  action="store_true", help="disclosure dataset only")
    ap.add_argument("--contributions", action="store_true",
                    help="disclosure dataset only (contributions share one table with expenditures)")
    ap.add_argument("--expenditures",  action="store_true",
                    help="disclosure dataset only (see --contributions)")
    ap.add_argument("--entities",      action="store_true",
                    help="filers + active committees + active candidates")
    ap.add_argument("--candidates",    action="store_true", help="filers + active candidates")
    ap.add_argument("--committees",    action="store_true", help="filers + active committees")

    args, _ = ap.parse_known_args()

    cy = datetime.today().year
    if args.end_year:
        # NYSBOE files future-cycle rows ahead of time (election_year 2027 rows
        # already exist in mid-2026), so the ceiling here is cy+2, not cy.
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
