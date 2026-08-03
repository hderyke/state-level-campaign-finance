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

# ── States without a real filer ID in their source data ───────────────────────
# Read from src/aliases/states.csv's has_filer_id column (0 = no numeric filer ID
# anywhere in the source; state_filer_id is structurally unfillable for these
# states). Kept in one place so adding/removing a state here doesn't require
# touching validate.py — just flip the column in states.csv.
_STATES_CSV = Path(__file__).resolve().parents[2] / "src" / "aliases" / "states.csv"
with open(_STATES_CSV, encoding="utf-8") as _f:
    STATES_WITHOUT_FILER_ID = {
        row["name"].strip().lower()
        for row in csv.DictReader(_f)
        # states.csv rows without a has_filer_id column (e.g. a state registered
        # before this column existed) get None from DictReader, not "1" — default
        # defensively to "has a filer ID" rather than crashing on .strip().
        if (row.get("has_filer_id") or "1").strip() == "0"
    }

# ── State name → abbreviation map ─────────────────────────────────────────────
STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR  = PROJECT_ROOT / "metadata"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

EARLIEST_YEAR    = 1990
LATEST_YEAR      = date.today().year + 4   # allow up to a full election cycle ahead
DRIFT_THRESHOLD  = 0.05                    # 5% row count drop = warning
MAX_SAMPLE_ROWS  = 500_000                   # rows held in memory for checks; full file still counted
TIER1_PASS_RATE  = 0.99                   # value-level checks pass if ≥99.5% of rows are valid

# Required columns per table — tier 1 value-level checks (must be ≥99.5% filled)
REQUIRED_COLS = {
    "candidates":    ["state", "state_filer_id", "candidate_name"],
    "committees":    ["state", "state_filer_id"],
    "contributions": ["state", "committee_name", "amount", "date", "raw_file", "row_num"],
    "expenditures":  ["state", "committee_name", "amount", "date", "raw_file", "row_num"],
}

# Per-table columns that get downgraded from a tier-1 failure to a tier-2 warning
# for states in STATES_WITHOUT_FILER_ID — these are columns where REQUIRED_COLS
# demands a fill rate that the source data structurally can't provide for those
# states (no numeric filer ID anywhere in the source, period).
TIER1_OPTIONAL_FOR_NAME_HASH = {
    "candidates": {"state_filer_id"},
    "committees": {"state_filer_id"},
}

# Tables that have amount fields
AMOUNT_TABLES = {
    "contributions": "amount",
    "expenditures":  "amount",
}

# Tables where negative amounts are allowed
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
def check_columns(table: str, rows: list[dict], required: list[str]) -> list[str]:
    """Tier 1: all required columns must be present."""
    if not rows:
        return []
    actual = set(rows[0].keys())
    missing = [c for c in required if c not in actual]
    return [f"Missing required column(s): {missing}"] if missing else []


def check_nonempty(table: str, rows: list[dict]) -> list[str]:
    """Tier 1: table must have at least one row."""
    if not rows:
        return [f"Table is empty (0 rows)"]
    return []


def check_state_col(table: str, rows: list[dict], expected_state: str) -> list[str]:
    """Tier 1: ≥99.5% of rows must have the correct state code."""
    if not rows or "state" not in rows[0]:
        return []
    bad = sum(1 for r in rows if r.get("state", "").strip() != expected_state)
    if bad / len(rows) > (1 - TIER1_PASS_RATE):
        return [f"{bad}/{len(rows)} rows have wrong state value (expected '{expected_state}') — "
                f"{pct(bad, len(rows))}% bad (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)"]
    return []


def check_required_filled(table: str, rows: list[dict], col: str) -> list[str]:
    """Tier 1: ≥99.5% of rows must have a non-empty value for the given column."""
    if not rows or col not in rows[0]:
        return []
    bad = sum(1 for r in rows if not str(r.get(col, "") or "").strip())
    if bad / len(rows) > (1 - TIER1_PASS_RATE):
        return [f"{bad}/{len(rows)} rows have empty '{col}' — "
                f"{pct(bad, len(rows))}% empty (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)"]
    return []


def check_amounts(table: str, rows: list[dict], col: str) -> list[str]:
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


def check_dates(table: str, rows: list[dict]) -> list[str]:
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


def check_row_num(table: str, rows: list[dict]) -> list[str]:
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


def check_election_year(table: str, rows: list[dict], col: str) -> list[str]:
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


def check_bool_int(table: str, rows: list[dict], col: str) -> list[str]:
    """Tier 2: field should be 0, 1, or empty."""
    if not rows or col not in rows[0]:
        return []
    bad = [str(r.get(col, "") or "").strip() for r in rows
           if str(r.get(col, "") or "").strip() not in ("", "0", "1")]
    if not bad:
        return []
    unique_bad = sorted(set(bad))
    return [f"{len(bad):,} '{col}' values are not 0/1/empty — e.g. {unique_bad[:5]}"]


def check_large_amounts(table: str, rows: list[dict], col: str) -> list[str]:
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


def check_state_codes(table: str, rows: list[dict], col: str) -> tuple[list[str], list[str]]:
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


def check_zips(table: str, rows: list[dict], col: str) -> tuple[list[str], list[str]]:
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


# ── Tier 2 enrichment stats ─────────────────────────────────────────────────────
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
def _state_key(name: str) -> str:
    """Normalize a state name for lookups: lowercase, underscores → spaces.

    Multi-word states get referred to both ways — "west virginia" from orc.py
    (which reads states.csv) and "west_virginia" by anyone typing the module
    name. STATE_ABBR is keyed on the spaced form, so the underscore form used
    to miss and fall through to `state.upper()`, yielding "WEST_VIRGINIA" as
    the expected value of the `state` column. Every row then failed the
    tier-1 state check against the correct "WV" — a 100% failure that looked
    like a data problem but was purely a key mismatch.
    """
    return re.sub(r"[\s_]+", " ", (name or "").strip().lower())


def run(state: str):
    state_lower = state.lower()
    state_key   = _state_key(state)
    state_upper = STATE_ABBR.get(state_key, state.upper())  # "alabama" → "AL"
    clean_dir   = PROJECT_ROOT / "data" / state_lower / "cleaned"

    # Try capitalized dir too (Alabama vs alabama)
    if not clean_dir.exists():
        clean_dir = PROJECT_ROOT / "data" / state.capitalize() / "cleaned"

    # Last resort: scan data/ comparing normalized names, so "West Virginia",
    # "west_virginia" and "West_Virginia" all resolve to the same directory.
    # str.capitalize() only uppercases the FIRST word, so a multi-word state
    # resolves to "West virginia" / "New hampshire" and misses the real
    # directory on any case-sensitive filesystem — invisible on Windows and
    # macOS, broken on Linux.
    if not clean_dir.exists():
        data_dir = PROJECT_ROOT / "data"
        if data_dir.exists():
            for d in data_dir.iterdir():
                if d.is_dir() and _state_key(d.name) == state_key:
                    clean_dir = d / "cleaned"
                    break

    if not clean_dir.exists():
        print(f"ERROR: cleaned dir not found for state '{state}'")
        sys.exit(1)

    log = get_logger(state_lower, "validate")
    t0  = time.perf_counter()
    log._emit("validate_started")

    try:
        _run(state_lower, state_upper, clean_dir, log, t0)
    except KeyboardInterrupt:
        log._emit("validate_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("validate_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


def _run(state_lower: str, state_upper: str, clean_dir: Path, log, t0: float):
    print(f"\n{'='*60}")
    print(f"  Validating {state_upper} — {clean_dir}")
    print(f"{'='*60}\n")

    # Load previous report for drift detection
    report_path  = REPORTS_DIR / f"{state_lower}_latest.json"
    prev_counts  = {}
    if report_path.exists():
        try:
            prev = json.loads(report_path.read_text())
            prev_counts = prev.get("row_counts", {})
        except Exception:
            pass

    tables = ["candidates", "committees", "contributions", "expenditures"]
    lacks_filer_id = state_lower in STATES_WITHOUT_FILER_ID
    all_rows      = {}
    row_counts    = {}
    sampled_tables = {}   # table → total row count when sampling was applied
    tier1_failures = []
    tier2_warnings = []
    drift_warnings = []

    # ── Load all tables ────────────────────────────────────────────────────────
    for table in tables:
        path = next(
            (clean_dir / f"{table}{ext}" for ext in (".csv.gz", ".csv")
             if (clean_dir / f"{table}{ext}").exists()),
            None,
        )
        if path is None:
            tier1_failures.append({"table": table, "check": "file_exists",
                                   "errors": [f"{table}.csv(.gz) not found in {clean_dir}"]})
            all_rows[table] = []
            continue
        all_rows[table], row_counts[table], _sampled = load_csv(path)
        if _sampled:
            sampled_tables[table] = row_counts[table]

    # ── Tier 1 checks ──────────────────────────────────────────────────────────
    for table in tables:
        rows = all_rows[table]

        checks = [
            ("nonempty",    check_nonempty(table, rows)),
            ("columns",     check_columns(table, rows, REQUIRED_COLS.get(table, []))),
            ("state_col",   check_state_col(table, rows, state_upper)),
        ]

        # Non-empty value check for every required column except 'state' (handled above)
        for col in REQUIRED_COLS.get(table, []):
            if col == "state":
                continue
            fill_errors = check_required_filled(table, rows, col)
            if (fill_errors and lacks_filer_id
                    and col in TIER1_OPTIONAL_FOR_NAME_HASH.get(table, set())):
                # Documented structural gap — this state's source data has no
                # numeric filer ID at all — downgrade to a tier-2 warning.
                for e in fill_errors:
                    tier2_warnings.append({
                        "table":   table,
                        "warning": f"{e} — expected, this state's source data has "
                                   f"no filer ID (see docs/states/{state_lower}.md)",
                    })
            else:
                checks.append((f"fill:{col}", fill_errors))

        checks.append(("row_num", check_row_num(table, rows)))

        if table in AMOUNT_TABLES:
            col = AMOUNT_TABLES[table]
            checks.append(("amounts", check_amounts(table, rows, col)))
            for w in check_large_amounts(table, rows, col):
                tier2_warnings.append({"table": table, "warning": w})

        if table in DATE_TABLES:
            date_errors, old_count = check_dates(table, rows)
            checks.append(("dates", date_errors))
            if old_count:
                tier2_warnings.append({
                    "table":   table,
                    "warning": f"{old_count} rows have dates before {EARLIEST_YEAR} — may be legitimate old records",
                })

        if table in ELECTION_YEAR_FIELDS:
            for w in check_election_year(table, rows, ELECTION_YEAR_FIELDS[table]):
                tier2_warnings.append({"table": table, "warning": w})

        for col in BOOL_INT_FIELDS.get(table, []):
            for w in check_bool_int(table, rows, col):
                tier2_warnings.append({"table": table, "warning": w})

        for col in STATE_CODE_FIELDS.get(table, []):
            _, warns = check_state_codes(table, rows, col)
            tier2_warnings.extend([{"table": table, "warning": w} for w in warns])

        for col in ZIP_FIELDS.get(table, []):
            _, warns = check_zips(table, rows, col)
            tier2_warnings.extend([{"table": table, "warning": w} for w in warns])

        for check_name, errors in checks:
            if errors:
                tier1_failures.append({
                    "table":  table,
                    "check":  check_name,
                    "errors": errors,
                })

    # ── Tier 2: enrichment stats ───────────────────────────────────────────────
    enrich = enrichment_stats(all_rows)

    # ── Tier 2: drift detection ────────────────────────────────────────────────
    for table in tables:
        current  = row_counts.get(table, 0)
        previous = prev_counts.get(table)
        w = drift_check(table, current, previous)
        if w:
            drift_warnings.append(w)

    # ── Print results ──────────────────────────────────────────────────────────
    # Schema-level failures (file missing, column absent, table empty) — binary
    schema_failures = [f for f in tier1_failures
                       if f["check"] in ("file_exists", "columns", "nonempty", "committee_id")]
    value_failures  = [f for f in tier1_failures
                       if f["check"] not in ("file_exists", "columns", "nonempty", "committee_id")]
    passed = not tier1_failures

    if schema_failures:
        print("TIER 1 — Schema errors")
        print("-" * 40)
        for f in schema_failures:
            for err in f["errors"]:
                print(f"  ✗ [{f['table']}] {err}")
        print()

    def _bar(rate: float) -> str:
        filled = int(rate / 10)
        return "▓" * filled + "░" * (10 - filled)

    def _required_fill_rates(rows: list[dict], fields: list[str]) -> dict[str, float]:
        total = len(rows)
        return {
            field: pct(sum(1 for r in rows if r.get(field, "").strip()), total)
            for field in fields
        }

    pass_pct = int(TIER1_PASS_RATE * 100)
    print(f"TIER 1 — Required field fill rates  (pass ≥{pass_pct}%)")
    print("-" * 40)
    for table in tables:
        rows     = all_rows[table]
        required = REQUIRED_COLS.get(table, [])
        if not rows or not required:
            continue
        rates = _required_fill_rates(rows, required)
        total_for_table = row_counts.get(table, len(rows))
        if table in sampled_tables:
            print(f"  {table.capitalize()} (sampled {len(rows):,} of {total_for_table:,} rows)")
        else:
            print(f"  {table.capitalize()} ({len(rows):,} rows)")
        any_downgraded = False
        for field in required:
            rate       = rates[field]
            downgraded = (lacks_filer_id and rate < TIER1_PASS_RATE * 100
                          and field in TIER1_OPTIONAL_FOR_NAME_HASH.get(table, set()))
            if downgraded:
                ok = "↓"
                any_downgraded = True
            else:
                ok = "✓" if rate >= TIER1_PASS_RATE * 100 else "✗"
            print(f"    {field:<25} {_bar(rate)}  {rate:5.1f}%  {ok}")
        if any_downgraded:
            print(f"    ↓ = tier-2 (no filer ID in source data; see docs/states/{state_lower}.md)")
        print()



    print("TIER 2 — Enrichment / fill rates")
    print("-" * 40)
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
        for bfield in BREAKDOWN_FIELDS.get(table, []):
            counts: dict[str, int] = {}
            for r in rows:
                val = r.get(bfield, "").strip() or "(blank)"
                counts[val] = counts.get(val, 0) + 1
            print(f"    {bfield + ' breakdown':<25}")
            for val, count in sorted(counts.items(), key=lambda x: -x[1]):
                rate = pct(count, len(rows))
                print(f"      {val:<30} {count:>7,}  {rate:5.1f}%")

        print()

    print("ROW COUNTS")
    print("-" * 40)
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
        print("TIER 2 — Warnings")
        print("-" * 40)
        for w in tier2_warnings:
            print(f"  ⚠ [{w['table']}] {w['warning']}")
        print()

    if drift_warnings:
        print("DRIFT WARNINGS")
        print("-" * 40)
        for w in drift_warnings:
            print(f"  ⚠ [{w['table']}] dropped {w['drop_pct']}% "
                  f"({w['previous']:,} → {w['current']:,})")
        print()

    print("=" * 60)
    if passed:
        print(f"  RESULT: PASS — {state_upper} data looks good")
    else:
        print(f"  RESULT: FAIL — {len(tier1_failures)} tier 1 check(s) failed")
    print("=" * 60)

    # ── Build tier-1 fill rates for JSON (same computation used for printing) ──
    tier1_fill_rates = {}
    for table in tables:
        rows     = all_rows[table]
        required = REQUIRED_COLS.get(table, [])
        if not rows or not required:
            continue
        rates = _required_fill_rates(rows, required)
        tier1_fill_rates[table] = {"_total": len(rows), **rates}

    # ── Build tier-2 breakdowns for JSON ──────────────────────────────────────
    tier2_breakdowns: dict[str, dict] = {}
    for table in tables:
        rows = all_rows[table]
        if not rows:
            continue
        bfields = BREAKDOWN_FIELDS.get(table, [])
        if not bfields:
            continue
        tier2_breakdowns[table] = {}
        for bfield in bfields:
            counts: dict[str, int] = {}
            for r in rows:
                val = r.get(bfield, "").strip() or "(blank)"
                counts[val] = counts.get(val, 0) + 1
            # Sort by count descending
            tier2_breakdowns[table][bfield] = dict(
                sorted(counts.items(), key=lambda x: -x[1])
            )

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

    # ── Write JSON report ──────────────────────────────────────────────────────
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
    report_path.write_text(json.dumps(report, indent=2))
    print(f"\n  Report saved to: {report_path}")

    # Also copy into the run dir when running under orc
    run_id = os.environ.get("CF_RUN_ID")
    if run_id:
        run_dir = run_dir_for(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        run_report = run_dir / f"{state_lower}_validate.json"
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
