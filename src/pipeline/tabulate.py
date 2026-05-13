import argparse
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TABLES       = ["contributions", "expenditures", "committees", "candidates"]
OPTS         = "null_padding=true, ignore_errors=true, parallel=false"


def tabulate(state: str):
    # Case-insensitive match against data/ subdirectories
    matches = [d for d in (PROJECT_ROOT / "data").iterdir()
               if d.is_dir() and d.name.lower() == state.lower()]
    if not matches:
        print(f"[!] No data directory found for '{state}'")
        sys.exit(1)

    state_dir = matches[0]
    clean_dir = state_dir / "cleaned"
    if not clean_dir.exists():
        print(f"[!] No cleaned/ directory at {clean_dir}")
        sys.exit(1)

    db_path = clean_dir / f"{state_dir.name.lower()}.db"
    con     = duckdb.connect(str(db_path))

    print(f"Building {db_path.name} from {clean_dir}")

    for table in TABLES:
        csv_path = clean_dir / f"{table}.csv"
        if not csv_path.exists():
            print(f"  {table}: not found — skipping")
            continue

        con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM read_csv_auto('{csv_path}', {OPTS})
        """)
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {n:,} rows")

    con.close()
    print(f"\nDone → {db_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("state", help="State name (e.g. arizona, Alabama)")
    args = ap.parse_args()
    tabulate(args.state)
