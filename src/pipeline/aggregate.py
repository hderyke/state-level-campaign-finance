"""
aggregate.py — Combine all finished state .db files into one.

Scans data/{state}/cleaned/{state}.db for every state that has been tabulated,
then merges all four tables (contributions, expenditures, committees, candidates)
into data/state-level-cf.db.

Usage:
    python3 src/pipeline/aggregate.py
"""

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR     = PROJECT_ROOT / "data"
OUT_DB       = DATA_DIR / "state-level-cf.db"

TABLES = ["contributions", "expenditures", "committees", "candidates"]


def find_state_dbs() -> list[tuple[str, Path]]:
    """Return (state_name, db_path) for every state that has a .db file."""
    found = []
    for state_dir in sorted(DATA_DIR.iterdir()):
        if not state_dir.is_dir():
            continue
        db_path = state_dir / "cleaned" / f"{state_dir.name.lower()}.db"
        if db_path.exists():
            found.append((state_dir.name, db_path))
    return found


def run():
    state_dbs = find_state_dbs()
    if not state_dbs:
        print("[!] No state .db files found. Run tabulate.py for at least one state first.")
        sys.exit(1)

    print(f"Found {len(state_dbs)} state db(s):")
    for name, path in state_dbs:
        print(f"  {name:<15} {path}")

    # Remove old aggregate db so we start clean
    if OUT_DB.exists():
        OUT_DB.unlink()

    con = duckdb.connect(str(OUT_DB))

    # Create tables with all-VARCHAR schema using canonical column definitions
    # (avoids type inference from a single state's data casting other states out)
    sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
    import columns as C

    col_map = {
        "contributions": C.CONTRIBUTIONS,
        "expenditures":  C.EXPENDITURES,
        "committees":    C.COMMITTEES,
        "candidates":    C.CANDIDATES,
    }
    print("\nCreating tables with VARCHAR schema...")
    for table, cols in col_map.items():
        col_defs = ", ".join(f'"{c}" VARCHAR' for c in cols)
        con.execute(f"CREATE TABLE {table} ({col_defs})")

    # Insert from each state db
    totals = {t: 0 for t in TABLES}
    for state_name, db_path in state_dbs:
        print(f"\n  {state_name}:")
        con.execute(f"ATTACH '{db_path}' AS _src (READ_ONLY)")
        for table in TABLES:
            try:
                cols      = col_map[table]
                cast_cols = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in cols)
                before    = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                con.execute(f"INSERT INTO {table} SELECT {cast_cols} FROM _src.{table}")
                after     = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                n = after - before
            except Exception as e:
                print(f"    {table:<15} skipped ({e})")
                n = 0
            totals[table] += n
            print(f"    {table:<15} {n:>10,} rows")
        con.execute("DETACH _src")

    print(f"\n{'='*50}")
    print(f"  Totals in {OUT_DB.name}:")
    for table in TABLES:
        print(f"    {table:<15} {totals[table]:>10,} rows")
    print(f"{'='*50}")
    print(f"\nDone → {OUT_DB}")

    con.close()


if __name__ == "__main__":
    run()
