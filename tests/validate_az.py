"""
tests/validate_az.py — Streaming validator for Arizona cleaned CSVs.
Designed to handle large files (303MB contributions) without OOM.

Usage:
    python tests/validate_az.py
"""

import csv
import sys
from collections import Counter
from pathlib import Path

CLEAN_DIR = Path(__file__).resolve().parents[1] / "data" / "arizona" / "cleaned"

PASS = "✓"
WARN = "⚠"
FAIL = "✗"

errors   = []
warnings = []

def err(msg):  errors.append(msg);   print(f"  {FAIL} {msg}")
def warn(msg): warnings.append(msg); print(f"  {WARN} {msg}")
def ok(msg):                          print(f"  {PASS} {msg}")


def check_file(name: str, required_cols: list[str], checks: dict):
    """
    checks dict keys:
      min_rows        : int
      max_blank_pct   : dict[col -> float]  (0.0–1.0)
      amount_cols     : list[str]  (must be numeric or blank)
      date_cols       : list[str]  (must be YYYY-MM-DD or blank)
      allowed_values  : dict[col -> set]
    """
    path = CLEAN_DIR / name
    print(f"\n── {name} ──")

    if not path.exists():
        err(f"File not found: {path}")
        return

    size_mb = path.stat().st_size / 1e6
    print(f"  size: {size_mb:.1f} MB")

    blank_counts  = Counter()
    amount_errors = Counter()
    date_errors   = Counter()
    value_errors  = Counter()
    row_count     = 0
    header        = None

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []

        # Check required columns
        missing = [c for c in required_cols if c not in header]
        if missing:
            err(f"Missing columns: {missing}")
        else:
            ok(f"All required columns present")

        for row in reader:
            row_count += 1

            # Blank tracking
            for col in checks.get("max_blank_pct", {}):
                if col in row and not (row[col] or "").strip():
                    blank_counts[col] += 1

            # Amount validation
            for col in checks.get("amount_cols", []):
                val = (row.get(col) or "").strip()
                if val:
                    try:
                        float(val)
                    except ValueError:
                        amount_errors[col] += 1

            # Date validation (YYYY-MM-DD or blank)
            for col in checks.get("date_cols", []):
                val = (row.get(col) or "").strip()
                if val:
                    parts = val.split("-")
                    if len(parts) != 3 or not (len(parts[0]) == 4 and parts[0].isdigit()):
                        date_errors[col] += 1

            # Allowed values
            for col, allowed in checks.get("allowed_values", {}).items():
                val = (row.get(col) or "").strip()
                if val and val not in allowed:
                    value_errors[col] += 1

    ok(f"{row_count:,} rows")

    # Min rows
    if row_count < checks.get("min_rows", 0):
        err(f"Too few rows: {row_count:,} < {checks['min_rows']:,}")

    # Blank rate checks
    for col, max_pct in checks.get("max_blank_pct", {}).items():
        if row_count == 0:
            continue
        pct = blank_counts[col] / row_count
        msg = f"{col}: {pct:.1%} blank ({blank_counts[col]:,}/{row_count:,})"
        if pct > max_pct:
            warn(msg + f"  [threshold: {max_pct:.0%}]")
        else:
            ok(msg)

    # Amount errors
    for col, n in amount_errors.items():
        if n:
            warn(f"{col}: {n:,} non-numeric values")
        else:
            ok(f"{col}: all numeric")

    # Date errors
    for col, n in date_errors.items():
        if n:
            warn(f"{col}: {n:,} malformed dates")
        else:
            ok(f"{col}: all valid YYYY-MM-DD")

    # Value errors
    for col, n in value_errors.items():
        if n:
            warn(f"{col}: {n:,} unexpected values")


# ── Run checks ────────────────────────────────────────────────────────────────

check_file("committees.csv",
    required_cols=["state", "state_filer_id", "committee_name", "committee_type",
                   "city", "zip", "active"],
    checks={
        "min_rows": 10_000,  # expect 43K now
        "max_blank_pct": {
            "state_filer_id": 0.05,
            "committee_name": 0.05,
            "committee_type": 0.10,
        },
    }
)

check_file("candidates.csv",
    required_cols=["state", "candidate_name", "office", "party"],
    checks={
        "min_rows": 8_000,
        "max_blank_pct": {
            "candidate_name": 0.02,
        },
    }
)

check_file("contributions.csv",
    required_cols=["state", "state_filer_id", "committee_name", "contributor_name",
                   "amount", "date", "election_year"],
    checks={
        "min_rows": 1_000_000,
        "amount_cols": ["amount"],
        "date_cols":   ["date"],
        "max_blank_pct": {
            "amount":         0.01,
            "date":           0.05,
            "election_year":  0.01,
            # PAC/Party files have empty state_filer_id — known limitation
            "state_filer_id": 0.95,
        },
    }
)

check_file("expenditures.csv",
    required_cols=["state", "state_filer_id", "committee_name", "payee_name",
                   "amount", "date", "election_year"],
    checks={
        "min_rows": 500_000,
        "amount_cols": ["amount"],
        "date_cols":   ["date"],
        "max_blank_pct": {
            "amount":         0.01,
            "date":           0.05,
            "state_filer_id": 0.95,
        },
    }
)

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"Errors:   {len(errors)}")
print(f"Warnings: {len(warnings)}")
if errors:
    print("\nFailed:")
    for e in errors:
        print(f"  {FAIL} {e}")
sys.exit(1 if errors else 0)
