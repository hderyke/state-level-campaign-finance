"""
tests/validate.py — Validate a state's cleaned CSVs before loading into the database.

Usage:
    python tests/validate.py alabama
    python tests/validate.py Alaska

Output:
    - Terminal: pass/fail/warn summary
    - tests/reports/{state}_latest.json: full structured report
    - logs/dev/{ts}-{state}-validate.jsonl (or logs/runs/{run_id}.jsonl in orc mode)

Exit codes:
    0 — all tier 1 checks passed (warnings may exist)
    1 — one or more tier 1 checks failed
"""

import csv
import gzip
import json
import sys
import time
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.logger import get_logger

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
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR  = PROJECT_ROOT / "tests" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

EARLIEST_YEAR    = 1990
LATEST_YEAR      = date.today().year + 4   # allow up to a full election cycle ahead
DRIFT_THRESHOLD  = 0.05                    # 5% row count drop = warning
MAX_SAMPLE_ROWS  = 1_000_000                 # rows held in memory for checks; full file still counted
TIER1_PASS_RATE  = 0.99                   # value-level checks pass if ≥99.5% of rows are valid

# Required columns per table — tier 1 value-level checks (must be ≥99.5% filled)
REQUIRED_COLS = {
    "candidates":    ["state", "person_id", "state_filer_id", "candidate_name"],
    "committees":    ["state", "state_filer_id"],
    "contributions": ["state", "committee_name", "amount", "date", "raw_file", "row_num"],
    "expenditures":  ["state", "committee_name", "amount", "date", "raw_file", "row_num"],
}

# Tables that have amount fields
AMOUNT_TABLES = {
    "contributions": "amount",
    "expenditures":  "amount",
}

# Tables where negative amounts are allowed
NEGATIVE_OK = set()

# Tables with date fields
DATE_TABLES = {"contributions", "expenditures"}

# Categorical fields to show value breakdowns for in tier 2
BREAKDOWN_FIELDS = {
    "committees": ["committee_type", "active"],
}

ENRICHMENT_FIELDS = {
    "candidates": [
        "person_id", "candidate_first", "candidate_last",
        "office", "district", "jurisdiction", "party", "election_year",
        "status", "incumbent",
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
def load_csv(path: Path) -> tuple[list[dict], int]:
    opener = gzip.open if path.suffix == ".gz" else open
    sample, total = [], 0
    with opener(path, "rt", newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            total += 1
            if total <= MAX_SAMPLE_ROWS:
                sample.append(row)
    return sample, total


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


def check_amounts(table: str, rows: list[dict], col: str) -> tuple[list[str], list[str]]:
    """
    Tier 1: amount fields must be numeric (hard fail).
    Tier 2: negative amounts in contributions/expenditures are a warning.
    Returns (tier1_errors, tier2_warnings).
    """
    t1, t2 = [], []
    non_numeric = 0
    negative    = 0
    total_valued = 0
    allow_negative = table in NEGATIVE_OK
    for r in rows:
        val = r.get(col, "").strip()
        if not val:
            continue
        total_valued += 1
        try:
            f = float(val)
            if f < 0 and not allow_negative:
                negative += 1
        except ValueError:
            non_numeric += 1
    if total_valued and non_numeric / total_valued > (1 - TIER1_PASS_RATE):
        t1.append(f"{non_numeric}/{total_valued} non-empty {col} values are non-numeric — "
                  f"{pct(non_numeric, total_valued)}% bad (threshold: {(1-TIER1_PASS_RATE)*100:.1f}%)")
    if negative:
        t2.append(f"{negative} rows have negative {col} — may indicate refunds/reversals")
    return t1, t2


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
def run(state: str):
    state_lower = state.lower()
    state_upper = STATE_ABBR.get(state_lower, state.upper())  # "alabama" → "AL"
    clean_dir   = PROJECT_ROOT / "data" / state_lower / "cleaned"

    # Try capitalized dir too (Alabama vs alabama)
    if not clean_dir.exists():
        clean_dir = PROJECT_ROOT / "data" / state.capitalize() / "cleaned"
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
    all_rows   = {}
    row_counts = {}
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
        all_rows[table], row_counts[table] = load_csv(path)

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
            checks.append((f"fill:{col}", check_required_filled(table, rows, col)))

        if table in AMOUNT_TABLES:
            col = AMOUNT_TABLES[table]
            t1_errs, t2_warns = check_amounts(table, rows, col)
            checks.append(("amounts", t1_errs))
            if t2_warns:
                tier2_warnings.extend([{"table": table, "warning": w} for w in t2_warns])

        if table in DATE_TABLES:
            date_errors, old_count = check_dates(table, rows)
            checks.append(("dates", date_errors))
            if old_count:
                tier2_warnings.append({
                    "table":   table,
                    "warning": f"{old_count} rows have dates before {EARLIEST_YEAR} — may be legitimate old records",
                })

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
        print(f"  {table.capitalize()} ({len(rows):,} rows sampled)")
        for field in required:
            rate  = rates[field]
            ok    = "✓" if rate >= TIER1_PASS_RATE * 100 else "✗"
            print(f"    {field:<25} {_bar(rate)}  {rate:5.1f}%  {ok}")
        print()

    # Warn if key enrichment fields have low fill rates (tier 2 warnings)
    FILL_WARN_THRESHOLD = 80.0
    FILL_WARN_FIELDS = {
        "candidates":    ["person_id", "office", "party"],
        "committees":    ["treasurer_name"],
        "contributions": ["contributor_name", "filing_id"],
        "expenditures":  ["payee_name", "filing_id"],
    }
    for table, fields in FILL_WARN_FIELDS.items():
        t_enrich = enrich.get(table, {})
        if t_enrich.get("total", 0) == 0:
            continue
        for field in fields:
            rate = t_enrich.get(field, 100.0)
            if rate < FILL_WARN_THRESHOLD:
                tier2_warnings.append({
                    "table":   table,
                    "warning": f"Only {rate}% of rows have {field} — check parser",
                })

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
        print(f"  {table:<15} {curr:>10,}{drift_str}")
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

    # ── Write JSON report ──────────────────────────────────────────────────────
    report = {
        "state":          state_upper,
        "run_at":         datetime.today().isoformat(),
        "clean_dir":      str(clean_dir),
        "passed":         passed,
        "row_counts":     row_counts,
        "tier1_failures":   tier1_failures,
        "tier2_warnings":   tier2_warnings,
        "tier2_enrichment": enrich,
        "drift_warnings":   drift_warnings,
    }
    report_path.write_text(json.dumps(report, indent=2))
    print(f"\n  Report saved to: {report_path}\n")

    # ── Emit JSONL event — lean signal for orc ─────────────────────────────────
    status = "passed" if passed else "failed"
    log._emit("validate_completed",
              status=status,
              duration_s=round(time.perf_counter() - t0, 1),
              tier1_failures=len(tier1_failures),
              tier2_warnings=len(tier2_warnings),
              drift_warnings=len(drift_warnings))

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tests/validate.py <state>")
        print("Example: python tests/validate.py alabama")
        sys.exit(1)
    try:
        run(sys.argv[1])
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
