"""
tests/validate.py — Validate a state's cleaned CSVs before loading into the database.

Usage:
    python tests/validate.py alabama
    python tests/validate.py alaska

Output:
    - Terminal: pass/fail/warn summary
    - tests/reports/{state}_latest.json: full structured report

Exit codes:
    0 — all tier 1 checks passed (warnings may exist)
    1 — one or more tier 1 checks failed
"""

import csv
import json
import sys
import os
from datetime import datetime, date
from pathlib import Path

# ── State name → abbreviation map ─────────────────────────────────────────────
STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "California": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
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

EARLIEST_YEAR = 1990
LATEST_YEAR   = date.today().year + 1   # allow next year (future filings)
DRIFT_THRESHOLD = 0.05                  # 5% row count drop = warning

# Required columns per table (must all be present)
REQUIRED_COLS = {
    "candidates":    ["state", "candidate_name"],
    "committees":    ["state", "committee_type"],   # see COMMITTEE_ID_COLS below
    "contributions": ["state", "state_filer_id", "amount", "date", "raw_file", "row_num"],
    "expenditures":  ["state", "state_filer_id", "amount", "date", "raw_file", "row_num"],
    "loans_debts":   ["state", "state_filer_id", "record_type", "raw_file", "row_num"],
}

# Committees must have at least one of these as an identifier
COMMITTEE_ID_COLS = ["state_filer_id", "committee_name"]

# Tables that have amount fields
AMOUNT_TABLES = {
    "contributions": "amount",
    "expenditures":  "amount",
    "loans_debts":   "original_amount",
}

# Tables where negative amounts are allowed
NEGATIVE_OK = {"loans_debts"}

# Tables with state_filer_id that must be non-empty on every row
# (committees excluded — some states use committee_name as the identifier instead)
FILER_ID_TABLES = {"contributions", "expenditures", "loans_debts"}

# Tables with date fields
DATE_TABLES = {"contributions", "expenditures", "loans_debts"}


# ── Helpers ────────────────────────────────────────────────────────────────────
def load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f))


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
    """Tier 1: every row's state column must equal the expected state code."""
    if not rows or "state" not in rows[0]:
        return []
    bad = sum(1 for r in rows if r.get("state", "").strip() != expected_state)
    if bad:
        return [f"{bad} rows have wrong state value (expected '{expected_state}')"]
    return []


def check_filer_id(table: str, rows: list[dict]) -> list[str]:
    """Tier 1: state_filer_id must be non-empty on every row."""
    if not rows or "state_filer_id" not in rows[0]:
        return []
    bad = sum(1 for r in rows if not r.get("state_filer_id", "").strip())
    if bad:
        return [f"{bad} rows have empty state_filer_id"]
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
    allow_negative = table in NEGATIVE_OK
    for r in rows:
        val = r.get(col, "").strip()
        if not val:
            continue
        try:
            f = float(val)
            if f < 0 and not allow_negative:
                negative += 1
        except ValueError:
            non_numeric += 1
    if non_numeric:
        t1.append(f"{non_numeric} rows have non-numeric {col}")
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
    if invalid:
        errors.append(f"{invalid} rows have unparseable date values")
    if future:
        errors.append(f"{future} rows have dates after {LATEST_YEAR}")
    return errors, too_old


# ── Tier 2 enrichment stats ─────────────────────────────────────────────────────
def enrichment_stats(candidates: list[dict], committees: list[dict]) -> dict:
    total_cands = len(candidates)
    total_cmtes = len(committees)
    return {
        "candidates": {
            "total":        total_cands,
            "with_office":  pct(sum(1 for r in candidates if r.get("office",  "").strip()), total_cands),
            "with_party":   pct(sum(1 for r in candidates if r.get("party",   "").strip()), total_cands),
            "with_district":pct(sum(1 for r in candidates if r.get("district","").strip()), total_cands),
        },
        "committees": {
            "total":           total_cmtes,
            "with_treasurer":  pct(sum(1 for r in committees if r.get("treasurer_name","").strip()), total_cmtes),
            "pct_active":      pct(sum(1 for r in committees if r.get("active","").strip() == "1"),  total_cmtes),
            "pct_dissolved":   pct(sum(1 for r in committees if r.get("active","").strip() == "0"),  total_cmtes),
        },
    }


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

    tables = ["candidates", "committees", "contributions", "expenditures", "loans_debts"]
    all_rows   = {}
    row_counts = {}
    tier1_failures = []
    tier2_warnings = []
    drift_warnings = []

    # ── Load all tables ────────────────────────────────────────────────────────
    for table in tables:
        path = clean_dir / f"{table}.csv"
        if not path.exists():
            tier1_failures.append({"table": table, "check": "file_exists",
                                   "errors": [f"{table}.csv not found in {clean_dir}"]})
            all_rows[table] = []
            continue
        all_rows[table] = load_csv(path)
        row_counts[table] = len(all_rows[table])

    # ── Tier 1 checks ──────────────────────────────────────────────────────────
    for table in tables:
        rows = all_rows[table]

        checks = [
            ("nonempty",    check_nonempty(table, rows)),
            ("columns",     check_columns(table, rows, REQUIRED_COLS.get(table, []))),
            ("state_col",   check_state_col(table, rows, state_upper)),
        ]

        # Committees: need at least one of state_filer_id or committee_name
        if table == "committees" and rows:
            actual_cols = set(rows[0].keys())
            has_id = any(c in actual_cols for c in COMMITTEE_ID_COLS)
            if not has_id:
                checks.append(("committee_id", [
                    f"Must have at least one of: {COMMITTEE_ID_COLS}"
                ]))

        if table in FILER_ID_TABLES:
            checks.append(("filer_id", check_filer_id(table, rows)))

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
    enrich = enrichment_stats(all_rows["candidates"], all_rows["committees"])

    # ── Tier 2: drift detection ────────────────────────────────────────────────
    for table in tables:
        current  = row_counts.get(table, 0)
        previous = prev_counts.get(table)
        w = drift_check(table, current, previous)
        if w:
            drift_warnings.append(w)

    # ── Print results ──────────────────────────────────────────────────────────
    passed = not tier1_failures

    print("TIER 1 — Hard checks")
    print("-" * 40)
    if passed:
        print("  ✓ All checks passed\n")
    else:
        for f in tier1_failures:
            for err in f["errors"]:
                print(f"  ✗ [{f['table']}] {err}")
        print()

    print("TIER 2 — Enrichment stats")
    print("-" * 40)
    c = enrich["candidates"]
    print(f"  Candidates : {c['total']:,} total | "
          f"{c['with_office']}% with office | "
          f"{c['with_party']}% with party | "
          f"{c['with_district']}% with district")
    m = enrich["committees"]
    print(f"  Committees : {m['total']:,} total | "
          f"{m['with_treasurer']}% with treasurer | "
          f"{m['pct_active']}% active | "
          f"{m['pct_dissolved']}% dissolved")
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

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tests/validate.py <state>")
        print("Example: python tests/validate.py alabama")
        sys.exit(1)
    run(sys.argv[1])
