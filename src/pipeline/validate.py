"""
src/pipeline/validate.py — Validate a state's cleaned CSVs before loading into the database.

This checks data, not code — it's pipeline stage 3 (scrape -> parse -> validate
-> tabulate -> aggregate), invoked as a subprocess by orc.py. It lives next to
tabulate.py/aggregate.py rather than under tests/, which no longer exists —
the report cache now lives in metadata/ at the project root.

Usage:
    python src/pipeline/validate.py alabama
    python src/pipeline/validate.py Alaska

Output:
    - Terminal: pass/fail/warn summary
    - metadata/{state}_latest.json: full structured report
    - logs/dev/{ts}-{state}-validate.jsonl (or logs/runs/{run_id}.jsonl in orc mode)

Exit codes:
    0 — all tier 1 checks passed (warnings may exist)
    1 — one or more tier 1 checks failed
"""

import csv
import gzip
import json
import os
import random
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path

# Python's csv module defaults to 131 072 bytes per field — too small for some
# state data files that embed long text fields (e.g. AL expenditure descriptions).
csv.field_size_limit(10 * 1024 * 1024)  # 10 MB should be more than enough

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.reporting.logger import get_logger, run_dir_for
from utils import find_clean_dir, find_table_csv

# ── State name normalization ──────────────────────────────────────────────────

def state_key(state: str) -> str:
    """Canonical lookup key for a state name: lowercase, single-space-separated.

    A state reaches this module under at least three spellings — the CLI and
    directory form `south_carolina`, the states.csv form `South Carolina`, and
    whatever the caller typed. Every table keyed by state name in this file is
    built through this function and read through it, so the three converge.

    This was a real failure, not a hypothetical. `run()` used a bare
    `state.lower()`, which leaves `south_carolina` with its underscore while
    STATE_ABBR and STATES_WITHOUT_FILER_ID are keyed on the space form straight
    out of states.csv. Both lookups missed, and each miss failed silently in a
    way that looked like bad data rather than a bad key:

      - STATE_ABBR missed, so `state_upper` fell back to "SOUTH_CAROLINA", and
        check_state_col then compared every row's `state` against that instead
        of "SC" — failing all four tables on 100% of rows.
      - STATES_WITHOUT_FILER_ID missed, so `state_filer_id` was held to a
        tier-1 fill rate the source structurally cannot meet, despite
        states.csv recording has_filer_id=0 for it.

    South Carolina was the first state that is both multi-word AND has no filer
    ID, which is why nothing caught this earlier: Alaska, Idaho, Kansas and
    Kentucky are single words, and the multi-word states registered before it
    all have filer IDs.
    """
    return " ".join((state or "").replace("_", " ").replace("-", " ").lower().split())


# ── Per-state config, read once from src/aliases/states.csv ───────────────────
# STATE_ABBR mirrors the abbr column (see enrich.py's NAME_TO_ABBR — same file,
# same convention: reuse the one abbr<->name registration everyone reads from,
# rather than hand-maintaining a second copy that can drift from it).
# STATES_WITHOUT_FILER_ID mirrors has_filer_id == 0 (no numeric filer ID
# anywhere in the source; state_filer_id is structurally unfillable for these
# states). Both come from one pass over states.csv, so adding/removing a
# state, or fixing an abbreviation, never requires touching validate.py —
# just edit states.csv.
_STATES_CSV = Path(__file__).resolve().parents[2] / "src" / "aliases" / "states.csv"
with open(_STATES_CSV, encoding="utf-8") as _f:
    _STATES_ROWS = list(csv.DictReader(_f))

STATE_ABBR = {state_key(row["name"]): row["abbr"].strip().upper() for row in _STATES_ROWS}

STATES_WITHOUT_FILER_ID = {
    state_key(row["name"])
    for row in _STATES_ROWS
    # states.csv rows without a has_filer_id column (e.g. a state registered
    # before this column existed) get None from DictReader, not "1" — default
    # defensively to "has a filer ID" rather than crashing on .strip().
    if (row.get("has_filer_id") or "1").strip() == "0"
}

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR  = PROJECT_ROOT / "metadata"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

EARLIEST_YEAR    = 1990
LATEST_YEAR      = date.today().year + 4   # allow up to a full election cycle ahead
DRIFT_THRESHOLD  = 0.05                    # 5% row count drop = warning
MAX_SAMPLE_ROWS  = 500_000                   # rows held in memory for checks; full file still counted
TIER1_PASS_RATE  = 0.99                   # value-level checks pass if ≥99% of rows are valid

# Required columns per table — tier 1 value-level checks (must be ≥99% filled)
REQUIRED_COLS = {
    "candidates":    ["state", "state_filer_id", "candidate_name"],
    "committees":    ["state", "state_filer_id"],
    "contributions": ["state", "committee_name", "amount", "date", "raw_file", "row_num"],
    "expenditures":  ["state", "committee_name", "amount", "date", "raw_file", "row_num"],
}

ENRICHMENT_FIELDS = {
    "candidates": [
        "candidate_first", "candidate_last",
        "office", "district", "jurisdiction", "party", "election_year",
        "incumbent",
    ],
    "committees": [
        "committee_type", "committee_name", "candidate_name",
        "treasurer_name", "city", "zip",
    ],
    "contributions": [
        "contributor_name", "contributor_type",
        "contributor_city", "contributor_state", "contributor_zip",
        "employer", "occupation",
        "candidate_name", "office", "election_year",
    ],
    "expenditures": [
        "payee_name", "transaction_type", "purpose", "category",
        "payee_city", "payee_state", "payee_zip",
        "candidate_name", "office", "election_year",
    ],
}

# Per-table columns that get downgraded from a tier-1 failure to a tier-2 warning
# for states in STATES_WITHOUT_FILER_ID — these are columns where REQUIRED_COLS
# demands a fill rate that the source data structurally can't provide for those
# states (no numeric filer ID anywhere in the source, period).
#
# NOTE: this used to also carry "expenditures": {"committee_name"} for TN,
# on the theory that TN's expenditure export had no per-row filer identifier
# at all. That turned out to be a parsers/tennessee.py bug, not a source gap:
# the filer name is 100% populated under a *different* header
# ("Candidate/PAC Name") than the one the parser was reading
# ("Recipient Name", 0% filled for expenditures) — see get_recipient() in
# that parser. Fixed 2026-08-08; committee_name is now 100% filled for TN
# expenditures, and AK/KS/KY (the other name_hash states) were already 100%
# filled, so this entry is gone rather than left as dead code.
TIER1_OPTIONAL_FOR_NAME_HASH = {
    "candidates": {"state_filer_id"},
    "committees": {"state_filer_id"},
}

# Per-state, per-table columns downgraded from a tier-1 failure to a tier-2
# warning for a gap specific to *that* state's source data — as opposed to
# TIER1_OPTIONAL_FOR_NAME_HASH above, which applies to every state lacking a
# filer ID. Checked independently of `lacks_filer_id` so it never loosens the
# check for other states in that class.
TIER1_OPTIONAL_BY_STATE = {
    "tennessee": {
        # TNCAMP's expenditure export has no Date value at all for 2000-2002
        # (100% blank) and 2003 (98.9% blank), tapering from ~21% blank in
        # 2004 down to under 3% by 2007 and near-zero after. Confirmed against
        # the raw CSVs directly — the Date column itself is empty for these
        # rows, not a parser/header-alias miss. Contributions from the same
        # years are dated fine (0.1% blank overall), so this is specific to
        # TN's expenditure schedule. See docs/states/tennessee.md.
        "expenditures": {"date"},
    },
}

# Tables that have amount fields
AMOUNT_TABLES = {
    "contributions": "amount",
    "expenditures":  "amount",
}

# Tier-2 amount threshold — single transaction above this triggers a count warning
LARGE_AMOUNT_THRESHOLD = 10_000_000

# Tables/fields for election_year range check
ELECTION_YEAR_FIELDS = {
    "contributions": "election_year",
    "expenditures":  "election_year",
    "candidates":    "election_year",
}

# Boolean integer fields (must be 0, 1, or empty)
BOOL_INT_FIELDS = {
    "contributions": ["amended"],
    "expenditures":  ["amended"],
    "committees":    ["active"],
}

# Tables with date fields
DATE_TABLES = {"contributions", "expenditures"}

# Valid US state/territory codes for contributor_state / payee_state checks.
# Donors and payees can be from any state or territory, not just pipeline states.
VALID_STATE_CODES = set(STATE_ABBR.values()) | {
    "DC", "PR", "GU", "VI", "AS", "MP", "UM",
}

# ZIP code patterns: 5-digit, ZIP+4 with hyphen, or 9-digit without hyphen
_ZIP_RE = re.compile(r"^\d{5}(-\d{4}|\d{4})?$")

# Fields to check for valid US state codes {table: [field, ...]}
STATE_CODE_FIELDS = {
    "contributions": ["contributor_state"],
    "expenditures":  ["payee_state"],
}

# Fields to check for valid ZIP format {table: [field, ...]}
ZIP_FIELDS = {
    "contributions": ["contributor_zip"],
    "expenditures":  ["payee_zip"],
}

# Categorical fields to show value breakdowns for in tier 2
BREAKDOWN_FIELDS = {
    "committees": ["committee_type", "active"],
    # Provenance for externally joined party values. Deliberately a breakdown
    # rather than a fill-rate row: these are only ever populated where `party`
    # itself was joined in from outside the state's disclosure data, so their
    # fill rate carries no information beyond party's, whereas the split
    # between "exact" and "high" matches is what tells you how much to trust
    # it. Blank for every state that publishes party directly.
    "candidates": ["party_source", "match_confidence"],
}

# Breakdown fields that are dropped entirely — from both the console report and
# the JSON — when a state populates none of them, instead of printing a lone
# "(blank) 100%" row.
#
# Only a couple of states ever write these, so on the other forty that row says
# nothing except "this column doesn't apply here", while adding two lines to
# every report and two dead entries to every metadata JSON. Fields NOT listed
# here keep the old always-print behaviour: `committee_type` and `active` are
# expected on every state, so an all-blank breakdown there is a genuine finding
# worth surfacing rather than noise worth hiding.
SPARSE_BREAKDOWN_FIELDS = {"party_source", "match_confidence"}


# ── Helpers ────────────────────────────────────────────────────────────────────
def load_csv(path: Path) -> tuple[list[dict], int, bool]:
    """Load a CSV (plain or .gz), returning (sample, total_rows, is_sampled).

    If total rows exceed MAX_SAMPLE_ROWS, reservoir sampling (Algorithm R) is
    used to select a uniformly random sample — ensuring recent rows are
    represented even when files are ordered chronologically.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    reservoir, total = [], 0
    with opener(path, "rt", newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            total += 1
            if total <= MAX_SAMPLE_ROWS:
                reservoir.append(row)
            else:
                # Reservoir sampling — replace a random existing entry
                j = random.randint(0, total - 1)
                if j < MAX_SAMPLE_ROWS:
                    reservoir[j] = row
    is_sampled = total > MAX_SAMPLE_ROWS
    return reservoir, total, is_sampled


def load_tables(tables: list[str], clean_dir: Path) -> tuple[dict, dict, dict, list[dict]]:
    """Load each table's CSV from clean_dir.

    Returns (all_rows, row_counts, sampled_tables, missing_file_failures) — the
    last is a list of tier1_failures-shaped dicts, one per table whose
    CSV(.gz) wasn't found (that table's all_rows entry is [] and it gets no
    row_counts entry, same as any table that loaded zero rows).
    """
    all_rows: dict[str, list[dict]] = {}
    row_counts: dict[str, int] = {}
    sampled_tables: dict[str, int] = {}
    failures: list[dict] = []
    for table in tables:
        path = find_table_csv(clean_dir, table)
        if path is None:
            failures.append({"table": table, "check": "file_exists",
                             "errors": [f"{table}.csv(.gz) not found in {clean_dir}"]})
            all_rows[table] = []
            continue
        all_rows[table], row_counts[table], sampled = load_csv(path)
        if sampled:
            sampled_tables[table] = row_counts[table]
    return all_rows, row_counts, sampled_tables, failures


def parse_date_str(val: str):
    """Return datetime.date or None."""
    val = (val or "").strip()
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        return None


def pct(n, total):
    return round(100 * n / total, 1) if total else 0.0


# ── Check functions ─────────────────────────────────────────────────────────────
def check_columns(rows: list[dict], required: list[str]) -> list[str]:
    """Tier 1: all required columns must be present."""
    if not rows:
        return []
    actual = set(rows[0].keys())
    missing = [c for c in required if c not in actual]
    return [f"Missing required column(s): {missing}"] if missing else []


def check_nonempty(rows: list[dict]) -> list[str]:
    """Tier 1: table must have at least one row."""
    if not rows:
        return [f"Table is empty (0 rows)"]
    return []


def check_state_col(rows: list[dict], expected_state: str) -> list[str]:
    """Tier 1: ≥99% of rows must have the correct state code."""
    if not rows or "state" not in rows[0]:
        return []
    bad = sum(1 for r in rows if r.get("state", "").strip() != expected_state)
    if bad / len(rows) > (1 - TIER1_PASS_RATE):
        return [f"{bad}/{len(rows)} rows have wrong state value (expected '{expected_state}') — "
                f"{pct(bad, len(rows))}% bad (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)"]
    return []


def check_required_filled(rows: list[dict], col: str) -> list[str]:
    """Tier 1: ≥99% of rows must have a non-empty value for the given column."""
    if not rows or col not in rows[0]:
        return []
    bad = sum(1 for r in rows if not str(r.get(col, "") or "").strip())
    if bad / len(rows) > (1 - TIER1_PASS_RATE):
        return [f"{bad}/{len(rows)} rows have empty '{col}' — "
                f"{pct(bad, len(rows))}% empty (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)"]
    return []


def check_amounts(rows: list[dict], col: str) -> list[str]:
    """Tier 1: amount fields must be numeric."""
    non_numeric  = 0
    total_valued = 0
    for r in rows:
        val = r.get(col, "").strip()
        if not val:
            continue
        total_valued += 1
        try:
            float(val)
        except ValueError:
            non_numeric += 1
    if total_valued and non_numeric / total_valued > (1 - TIER1_PASS_RATE):
        return [f"{non_numeric}/{total_valued} non-empty {col} values are non-numeric — "
                f"{pct(non_numeric, total_valued)}% bad (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)"]
    return []


def check_dates(rows: list[dict]) -> tuple[list[str], int]:
    """Tier 1: dates must be valid YYYY-MM-DD within plausible range."""
    errors = []
    invalid  = 0
    too_old  = 0
    future   = 0
    for r in rows:
        val = r.get("date", "").strip()
        if not val:
            continue
        d = parse_date_str(val)
        if d is None:
            invalid += 1
        elif d.year < EARLIEST_YEAR:
            too_old += 1
        elif d.year > LATEST_YEAR:
            future += 1
    total_valued = sum(1 for r in rows if r.get("date", "").strip())
    if total_valued and invalid / total_valued > (1 - TIER1_PASS_RATE):
        errors.append(f"{invalid}/{total_valued} non-empty dates are unparseable — "
                      f"{pct(invalid, total_valued)}% bad (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)")
    if total_valued and future / total_valued > (1 - TIER1_PASS_RATE):
        errors.append(f"{future}/{total_valued} dates are after {LATEST_YEAR} — "
                      f"{pct(future, total_valued)}% bad (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)")
    return errors, too_old


def check_row_num(rows: list[dict]) -> list[str]:
    """Tier 1: row_num must be a positive integer where present."""
    if not rows or "row_num" not in rows[0]:
        return []
    bad = sum(1 for r in rows
              if r.get("row_num", "").strip() and
              not str(r.get("row_num", "")).strip().lstrip("-").isdigit())
    if bad and bad / len(rows) > (1 - TIER1_PASS_RATE):
        return [f"{bad}/{len(rows)} row_num values are non-integer — "
                f"{pct(bad, len(rows))}% bad"]
    return []


def check_election_year(rows: list[dict], col: str) -> list[str]:
    """Tier 2: non-empty election_year should be a 4-digit year in plausible range."""
    if not rows or col not in rows[0]:
        return []
    bad = []
    for r in rows:
        val = str(r.get(col, "") or "").strip()
        if not val:
            continue
        if not val.isdigit() or not (EARLIEST_YEAR <= int(val) <= LATEST_YEAR):
            bad.append(val)
    if not bad:
        return []
    unique_bad = sorted(set(bad))
    return [f"{len(bad):,} non-empty '{col}' values are outside {EARLIEST_YEAR}–{LATEST_YEAR} "
            f"or non-numeric — e.g. {unique_bad[:5]}"]


def check_bool_int(rows: list[dict], col: str) -> list[str]:
    """Tier 2: field should be 0, 1, or empty."""
    if not rows or col not in rows[0]:
        return []
    bad = [str(r.get(col, "") or "").strip() for r in rows
           if str(r.get(col, "") or "").strip() not in ("", "0", "1")]
    if not bad:
        return []
    unique_bad = sorted(set(bad))
    return [f"{len(bad):,} '{col}' values are not 0/1/empty — e.g. {unique_bad[:5]}"]


def check_large_amounts(rows: list[dict], col: str) -> list[str]:
    """Tier 2: count rows where abs(amount) exceeds the large-amount threshold."""
    if not rows or col not in rows[0]:
        return []
    large = []
    for r in rows:
        val = r.get(col, "").strip()
        if not val:
            continue
        try:
            if abs(float(val)) >= LARGE_AMOUNT_THRESHOLD:
                large.append(val)
        except ValueError:
            pass
    if not large:
        return []
    return [f"{len(large):,} rows have |{col}| ≥ ${LARGE_AMOUNT_THRESHOLD:,} "
            f"— may indicate data entry errors or large transfers"]


def check_state_codes(rows: list[dict], col: str) -> tuple[list[str], list[str]]:
    """
    Tier 2: non-empty state code values should be in the known valid set.
    Returns (tier1_errors, tier2_warnings) — currently all soft (tier2).
    Foreign addresses may legitimately appear, so this is a warning not a failure.
    """
    if not rows or col not in rows[0]:
        return [], []
    invalid = [r.get(col, "").strip() for r in rows
               if r.get(col, "").strip() and r.get(col, "").strip() not in VALID_STATE_CODES]
    if not invalid:
        return [], []
    unique_bad = sorted(set(invalid))
    rate = pct(len(invalid), len(rows))
    msg = (f"{len(invalid):,} non-empty '{col}' values are not recognised US state/territory codes "
           f"({rate:.1f}%) — e.g. {unique_bad[:5]}")
    return [], [msg]


def check_zips(rows: list[dict], col: str) -> tuple[list[str], list[str]]:
    """
    Tier 2: non-empty ZIP values should match 5-digit, ZIP+4, or 9-digit format.
    Returns (tier1_errors, tier2_warnings).
    """
    if not rows or col not in rows[0]:
        return [], []
    invalid = [r.get(col, "").strip() for r in rows
               if r.get(col, "").strip() and not _ZIP_RE.match(r.get(col, "").strip())]
    if not invalid:
        return [], []
    unique_bad = sorted(set(invalid))
    rate = pct(len(invalid), len(rows))
    msg = (f"{len(invalid):,} non-empty '{col}' values don't match ZIP format "
           f"({rate:.1f}%) — e.g. {unique_bad[:5]}")
    return [], [msg]


# ── Report data builders ────────────────────────────────────────────────────────
def _fill_rates(rows: list[dict], fields: list[str]) -> dict:
    """Return {field: pct_filled} for each field in the list."""
    total = len(rows)
    return {
        field: pct(sum(1 for r in rows if r.get(field, "").strip()), total)
        for field in fields
    }


def enrichment_stats(all_rows: dict[str, list[dict]]) -> dict:
    """Compute fill rates for every enrichment field across all tables."""
    result = {}
    for table, fields in ENRICHMENT_FIELDS.items():
        rows = all_rows.get(table, [])
        result[table] = {"total": len(rows), **_fill_rates(rows, fields)}
    return result


def tier1_fill_rate_report(tables: list[str], all_rows: dict[str, list[dict]]) -> dict:
    """{table: {"_total": n, field: pct_filled, ...}} for each table's
    REQUIRED_COLS. Tables with no rows, or no required columns, are omitted.
    Shared by the console report and the JSON report so the two can't drift.
    """
    result = {}
    for table in tables:
        rows     = all_rows[table]
        required = REQUIRED_COLS.get(table, [])
        if not rows or not required:
            continue
        result[table] = {"_total": len(rows), **_fill_rates(rows, required)}
    return result


def _breakdown(rows: list[dict], bfield: str) -> dict[str, int] | None:
    """Value counts for one categorical field, ordered by count descending.

    Returns None when the field is in SPARSE_BREAKDOWN_FIELDS and every row is
    blank (or the column is absent entirely, as it is in any state parsed
    before the column existed) — the caller then omits it.

    Shared by the console report and the JSON builder so the two can't drift:
    they previously carried separate copies of this counting loop.
    """
    counts: dict[str, int] = {}
    for r in rows:
        val = (r.get(bfield) or "").strip() or "(blank)"
        counts[val] = counts.get(val, 0) + 1
    if bfield in SPARSE_BREAKDOWN_FIELDS and set(counts) <= {"(blank)"}:
        return None
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def tier2_breakdown_report(tables: list[str], all_rows: dict[str, list[dict]]) -> dict:
    """{table: {field: {value: count, ...}}} for each table's BREAKDOWN_FIELDS.
    A table is omitted entirely if it has no rows, or if every one of its
    breakdowns came back sparse-and-blank (see _breakdown()). Shared by the
    console report and the JSON report so the two can't drift.
    """
    result = {}
    for table in tables:
        rows = all_rows[table]
        if not rows:
            continue
        table_breakdowns = {}
        for bfield in BREAKDOWN_FIELDS.get(table, []):
            counts = _breakdown(rows, bfield)
            if counts is not None:
                table_breakdowns[bfield] = counts
        if table_breakdowns:
            result[table] = table_breakdowns
    return result


def drift_check(table: str, current: int, previous: int | None) -> dict | None:
    """Tier 2: warn if row count dropped more than DRIFT_THRESHOLD."""
    if previous is None:
        return None
    drop = (previous - current) / previous if previous else 0
    if drop > DRIFT_THRESHOLD:
        return {
            "table":    table,
            "previous": previous,
            "current":  current,
            "drop_pct": round(drop * 100, 1),
        }
    return None


# ── Main ───────────────────────────────────────────────────────────────────────
def run_tier1_checks(tables: list[str], all_rows: dict[str, list[dict]],
                      state_upper: str, state_slug: str,
                      lacks_filer_id: bool) -> tuple[list[dict], list[dict]]:
    """Run every tier-1 value check (plus the tier-2 checks that ride along
    with them) across all tables. Returns (tier1_failures, tier2_warnings) —
    fresh lists, for the caller to .extend() onto its running totals.
    """
    tier1_failures = []
    tier2_warnings = []
    state_optional_cols = TIER1_OPTIONAL_BY_STATE.get(state_slug, {})

    for table in tables:
        rows = all_rows[table]

        checks = [
            ("nonempty",    check_nonempty(rows)),
            ("columns",     check_columns(rows, REQUIRED_COLS.get(table, []))),
            ("state_col",   check_state_col(rows, state_upper)),
        ]

        # Non-empty value check for every required column except 'state' (handled above)
        for col in REQUIRED_COLS.get(table, []):
            if col == "state":
                continue
            fill_errors = check_required_filled(rows, col)
            if (fill_errors and lacks_filer_id
                    and col in TIER1_OPTIONAL_FOR_NAME_HASH.get(table, set())):
                # Documented structural gap — this state's source data has no
                # numeric filer ID at all — downgrade to a tier-2 warning.
                for e in fill_errors:
                    tier2_warnings.append({
                        "table":   table,
                        "warning": f"{e} — expected, this state's source data has "
                                   f"no filer ID (see docs/states/{state_slug}.md)",
                    })
            elif fill_errors and col in state_optional_cols.get(table, set()):
                # Documented structural gap specific to this state alone (see
                # TIER1_OPTIONAL_BY_STATE) — downgrade to a tier-2 warning
                # without affecting any other state.
                for e in fill_errors:
                    tier2_warnings.append({
                        "table":   table,
                        "warning": f"{e} — expected, documented source gap for "
                                   f"this state (see docs/states/{state_slug}.md)",
                    })
            else:
                checks.append((f"fill:{col}", fill_errors))

        checks.append(("row_num", check_row_num(rows)))

        if table in AMOUNT_TABLES:
            col = AMOUNT_TABLES[table]
            checks.append(("amounts", check_amounts(rows, col)))
            for w in check_large_amounts(rows, col):
                tier2_warnings.append({"table": table, "warning": w})

        if table in DATE_TABLES:
            date_errors, too_old = check_dates(rows)
            checks.append(("dates", date_errors))
            if too_old:
                tier2_warnings.append({
                    "table":   table,
                    "warning": f"{too_old} rows have dates before {EARLIEST_YEAR} — may be legitimate old records",
                })

        if table in ELECTION_YEAR_FIELDS:
            for w in check_election_year(rows, ELECTION_YEAR_FIELDS[table]):
                tier2_warnings.append({"table": table, "warning": w})

        for col in BOOL_INT_FIELDS.get(table, []):
            for w in check_bool_int(rows, col):
                tier2_warnings.append({"table": table, "warning": w})

        for col in STATE_CODE_FIELDS.get(table, []):
            _, warns = check_state_codes(rows, col)
            tier2_warnings.extend([{"table": table, "warning": w} for w in warns])

        for col in ZIP_FIELDS.get(table, []):
            _, warns = check_zips(rows, col)
            tier2_warnings.extend([{"table": table, "warning": w} for w in warns])

        for check_name, errors in checks:
            if errors:
                tier1_failures.append({
                    "table":  table,
                    "check":  check_name,
                    "errors": errors,
                })

    return tier1_failures, tier2_warnings


def print_report(report: dict, tables: list[str], all_rows: dict[str, list[dict]],
                  prev_counts: dict[str, int], state_slug: str,
                  lacks_filer_id: bool) -> None:
    """Print the full console validation report: schema/value errors, the
    tier-1 fill-rate table, tier-2 enrichment/breakdowns, row counts,
    warnings, and the final PASS/FAIL line.

    `report` is the same dict that gets written out as the JSON report —
    see its assembly in _run() for the field shapes.
    """
    state_upper      = report["state"]
    passed           = report["passed"]
    row_counts       = report["row_counts"]
    sampled_tables   = report["sampled_tables"]
    tier1_failures   = report["tier1_failures"]
    tier1_fill_rates = report["tier1_fill_rates"]
    tier2_warnings   = report["tier2_warnings"]
    enrich           = report["tier2_enrichment"]
    tier2_breakdowns = report["tier2_breakdowns"]
    drift_warnings   = report["drift_warnings"]

    def _section_header(title: str) -> None:
        print(title)
        print("-" * 40)

    # Schema-level failures (file missing, column absent, table empty) — binary
    schema_failures = [f for f in tier1_failures
                       if f["check"] in ("file_exists", "columns", "nonempty", "committee_id")]
    value_failures  = [f for f in tier1_failures
                       if f["check"] not in ("file_exists", "columns", "nonempty", "committee_id")]

    # `fill:<col>` failures are already legible as a ✗ against that column in the
    # fill-rate table below. Every other value check — state_col, amounts, dates,
    # row_num — has nowhere to appear there, so until this block existed those
    # failures were counted in the FAIL total and never shown.
    #
    # That is how a South Carolina run reported "6 tier 1 check(s) failed" while
    # displaying exactly two ✗: the other four were state_col failures, one per
    # table, caused by a state-name key miss (see state_key). The report gave no
    # way to tell which four checks were failing or why, and the only place the
    # detail existed was the JSON under `tier1_failures`.
    unshown_failures = [f for f in value_failures
                        if not f["check"].startswith("fill:")]

    if schema_failures:
        _section_header("TIER 1 — Schema errors")
        for f in schema_failures:
            for err in f["errors"]:
                print(f"  ✗ [{f['table']}] {err}")
        print()

    if unshown_failures:
        _section_header("TIER 1 — Value errors")
        for f in unshown_failures:
            for err in f["errors"]:
                print(f"  ✗ [{f['table']}] {f['check']}: {err}")
        print()

    def _bar(rate: float) -> str:
        filled = int(rate / 10)
        return "▓" * filled + "░" * (10 - filled)

    pass_pct = int(TIER1_PASS_RATE * 100)
    _section_header(f"TIER 1 — Required field fill rates  (pass ≥{pass_pct}%)")
    for table in tables:
        if table not in tier1_fill_rates:
            continue
        rows     = all_rows[table]
        required = REQUIRED_COLS.get(table, [])
        rates = tier1_fill_rates[table]
        total_for_table = row_counts.get(table, len(rows))
        if table in sampled_tables:
            print(f"  {table.capitalize()} (sampled {len(rows):,} of {total_for_table:,} rows)")
        else:
            print(f"  {table.capitalize()} ({len(rows):,} rows)")
        any_downgraded = False
        any_state_downgraded = False
        state_optional_cols = TIER1_OPTIONAL_BY_STATE.get(state_slug, {})
        for field in required:
            rate            = rates[field]
            downgraded      = (lacks_filer_id and rate < TIER1_PASS_RATE * 100
                                and field in TIER1_OPTIONAL_FOR_NAME_HASH.get(table, set()))
            state_downgraded = (rate < TIER1_PASS_RATE * 100
                                 and field in state_optional_cols.get(table, set()))
            if downgraded:
                ok = "↓"
                any_downgraded = True
            elif state_downgraded:
                ok = "◇"
                any_state_downgraded = True
            else:
                ok = "✓" if rate >= TIER1_PASS_RATE * 100 else "✗"
            print(f"    {field:<25} {_bar(rate)}  {rate:5.1f}%  {ok}")
        if any_downgraded:
            print(f"    ↓ = tier-2 (no filer ID in source data; see docs/states/{state_slug}.md)")
        if any_state_downgraded:
            print(f"    ◇ = tier-2 (documented source gap for this state; see docs/states/{state_slug}.md)")
        print()

    _section_header("TIER 2 — Enrichment / fill rates")
    for table in tables:
        rows     = all_rows[table]
        t_enrich = enrich.get(table, {})
        total    = t_enrich.get("total", 0)
        fields   = ENRICHMENT_FIELDS.get(table, [])
        print(f"  {table.capitalize()} ({total:,} rows)")
        for field in fields:
            rate = t_enrich.get(field, 0.0)
            print(f"    {field:<25} {_bar(rate)}  {rate:5.1f}%")

        # Value breakdowns for categorical fields (e.g. committee_type, active)
        for bfield, counts in tier2_breakdowns.get(table, {}).items():
            print(f"    {bfield + ' breakdown':<25}")
            for val, count in counts.items():
                rate = pct(count, len(rows))
                print(f"      {val:<30} {count:>7,}  {rate:5.1f}%")

        print()

    _section_header("ROW COUNTS")
    for table in tables:
        prev = prev_counts.get(table)
        curr = row_counts.get(table, 0)
        drift_str = ""
        if prev is not None:
            diff = curr - prev
            drift_str = f"  (prev: {prev:,}, Δ {diff:+,})"
        sample_str = f"  [sampled {MAX_SAMPLE_ROWS:,} of {curr:,}]" if table in sampled_tables else ""
        print(f"  {table:<15} {curr:>10,}{sample_str}{drift_str}")
    print()

    if tier2_warnings:
        _section_header("TIER 2 — Warnings")
        for w in tier2_warnings:
            print(f"  ⚠ [{w['table']}] {w['warning']}")
        print()

    if drift_warnings:
        _section_header("DRIFT WARNINGS")
        for w in drift_warnings:
            print(f"  ⚠ [{w['table']}] dropped {w['drop_pct']}% "
                  f"({w['previous']:,} → {w['current']:,})")
        print()

    print("=" * 60)
    if passed:
        print(f"  RESULT: PASS — {state_upper} data looks good")
    else:
        # Broken down by where each failure was reported, so the total can
        # always be reconciled against what is on screen (see unshown_failures
        # above for why this split exists).
        fill_failures = len(value_failures) - len(unshown_failures)
        parts = [f"{len(schema_failures)} schema"] if schema_failures else []
        if unshown_failures:
            parts.append(f"{len(unshown_failures)} value")
        if fill_failures:
            parts.append(f"{fill_failures} fill-rate")
        print(f"  RESULT: FAIL — {len(tier1_failures)} tier 1 check(s) failed"
              + (f" ({', '.join(parts)})" if parts else ""))
    print("=" * 60)


def run(state: str):
    # state_name is the space-separated lookup key -- the ONLY form
    # STATE_ABBR and STATES_WITHOUT_FILER_ID are keyed and consulted with,
    # both built through state_key() (see its docstring for what broke when
    # this used to be a bare .lower()). state_slug is the underscored
    # filesystem/report form, used for clean_dir and everything state_slug
    # touches downstream in _run().
    state_name  = state_key(state)
    state_slug  = state_name.replace(" ", "_")
    state_upper = STATE_ABBR.get(state_name, state_slug.upper())  # "alabama" -> "AL"
    clean_dir   = find_clean_dir(state)

    # Logger first: a missing cleaned dir is a real validation failure and has
    # to leave a record, otherwise orc's run report shows no validate event at
    # all for the state rather than showing why it failed.
    log = get_logger(state_slug, "validate")
    t0  = time.perf_counter()
    log._emit("validate_started")

    if clean_dir is None:
        print(f"ERROR: cleaned dir not found for state '{state}'")
        log._emit("validate_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error="cleaned dir not found")
        sys.exit(1)

    try:
        _run(state_name, state_slug, state_upper, clean_dir, log, t0)
    except KeyboardInterrupt:
        log._emit("validate_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("validate_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


def _run(state_name: str, state_slug: str, state_upper: str, clean_dir: Path,
         log, t0: float):
    """state_name is the space-separated lookup key; state_slug the slug form.

    Only STATES_WITHOUT_FILER_ID is consulted with state_name — everything else
    below is a path or a display string and wants the slug.
    """
    print(f"\n{'='*60}")
    print(f"  Validating {state_upper} — {clean_dir}")
    print(f"{'='*60}\n")

    # Load previous report for drift detection.
    # The filename is slugified (spaces → underscores) so a multi-word state
    # lands on the same file whether it was invoked as "new york" (how orc.py
    # spells it, from states.csv) or "new_york" (how a person types it on the
    # command line) — otherwise the two spellings write two different files
    # and drift detection silently compares a run against nothing.
    report_path  = REPORTS_DIR / f"{state_slug}_latest.json"
    prev_counts  = {}
    if report_path.exists():
        try:
            prev = json.loads(report_path.read_text())
            prev_counts = prev.get("row_counts", {})
        except Exception:
            pass

    tables = ["candidates", "committees", "contributions", "expenditures"]
    lacks_filer_id = state_name in STATES_WITHOUT_FILER_ID
    tier1_failures = []
    tier2_warnings = []
    drift_warnings = []

    # ── Load all tables ────────────────────────────────────────────────────────
    all_rows, row_counts, sampled_tables, load_failures = load_tables(tables, clean_dir)
    tier1_failures.extend(load_failures)

    # ── Tier 1 checks ──────────────────────────────────────────────────────────
    table_failures, table_warnings = run_tier1_checks(
        tables, all_rows, state_upper, state_slug, lacks_filer_id)
    tier1_failures.extend(table_failures)
    tier2_warnings.extend(table_warnings)

    # tier1_failures is complete at this point — nothing computed below adds
    # to it — so whether the run passed is decided here rather than buried
    # inside print_report().
    passed = not tier1_failures

    # ── Tier 2: enrichment stats ───────────────────────────────────────────────
    enrich = enrichment_stats(all_rows)

    # ── Tier 1: required-field fill rates ── used by the console report below
    # and the JSON report at the end of this function.
    tier1_fill_rates = tier1_fill_rate_report(tables, all_rows)

    # ── Tier 2: categorical breakdowns ── same two consumers as above.
    tier2_breakdowns = tier2_breakdown_report(tables, all_rows)

    # ── Tier 2: drift detection ────────────────────────────────────────────────
    for table in tables:
        current  = row_counts.get(table, 0)
        previous = prev_counts.get(table)
        w = drift_check(table, current, previous)
        if w:
            drift_warnings.append(w)

    # ── Newest record date (max transaction date across contributions/expenditures,
    #    capped at today — LATEST_YEAR is a generous tier-1 validity bound for
    #    future-scheduled filings, but "newest record" should read as "as of today") ──
    today = date.today()
    newest_record = None
    for table in DATE_TABLES:
        for r in all_rows.get(table, []):
            d = parse_date_str(r.get("date", ""))
            if d is not None and d <= today and (newest_record is None or d > newest_record):
                newest_record = d
    newest_record_str = newest_record.isoformat() if newest_record else None

    # ── Assemble the report ── built once here and shared by the console
    # report below and the JSON report written at the end of this function.
    report = {
        "state":             state_upper,
        "run_at":            datetime.today().isoformat(),
        "clean_dir":         str(clean_dir),
        "passed":            passed,
        "row_counts":        row_counts,
        "newest_record":     newest_record_str,
        "sampled_tables":    sampled_tables,
        "tier1_failures":    tier1_failures,
        "tier1_fill_rates":  tier1_fill_rates,
        "tier2_warnings":    tier2_warnings,
        "tier2_enrichment":  enrich,
        "tier2_breakdowns":  tier2_breakdowns,
        "drift_warnings":    drift_warnings,
    }

    # ── Print results ──────────────────────────────────────────────────────────
    print_report(report, tables, all_rows, prev_counts, state_slug, lacks_filer_id)

    # ── Write JSON report ──────────────────────────────────────────────────────
    report_path.write_text(json.dumps(report, indent=2))
    print(f"\n  Report saved to: {report_path}")

    # Also copy into the run dir when running under orc
    run_id = os.environ.get("CF_RUN_ID")
    if run_id:
        run_dir = run_dir_for(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        run_report = run_dir / f"{state_slug}_validate.json"
        run_report.write_text(json.dumps(report, indent=2))
        print(f"  Report copied to: {run_report}")
    print()

    # ── Emit JSONL event — lean signal for orc ─────────────────────────────────
    status = "passed" if passed else "failed"
    log._emit("validate_completed",
              status=status,
              duration_s=round(time.perf_counter() - t0, 1),
              tier1_failures=len(tier1_failures),
              tier2_warnings=len(tier2_warnings),
              drift_warnings=len(drift_warnings),
              row_counts=row_counts,
              sampled_tables=sampled_tables,
              newest_record=newest_record_str)

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/pipeline/validate.py <state>")
        print("Example: python src/pipeline/validate.py alabama")
        sys.exit(1)
    try:
        run(sys.argv[1])
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
