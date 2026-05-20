"""
aggregate.py — Combine all finished state .db files into one.

Scans data/{state}/cleaned/{state}.db for every state that has been tabulated,
then merges all four tables (contributions, expenditures, committees, candidates)
into data/state-level-cf.db.

Usage:
    python3 src/pipeline/aggregate.py
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.logger import get_logger

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
    log = get_logger(None, "aggregate")
    t0  = time.perf_counter()

    state_dbs = find_state_dbs()
    if not state_dbs:
        print("[!] No state .db files found. Run tabulate.py for at least one state first.")
        log._emit("aggregate_completed", status="error",
                  duration_s=0.0, error="no state db files found")
        sys.exit(1)

    state_names = [name for name, _ in state_dbs]
    log._emit("aggregate_started", states=state_names, states_count=len(state_dbs))

    print(f"Found {len(state_dbs)} state db(s):")
    for name, path in state_dbs:
        print(f"  {name:<15} {path}")

    con = None
    tables_ok  = 0
    tables_err = 0
    totals     = {}

    try:
        # Remove or retire the old aggregate DB.
        if OUT_DB.exists():
            try:
                OUT_DB.unlink()
            except PermissionError:
                OUT_DB.rename(str(OUT_DB) + f".{int(time.time())}.old")

        wal = Path(str(OUT_DB) + ".wal")
        if wal.exists():
            wal.rename(str(wal) + f".{int(time.time())}.old")

        sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
        import columns as C

        col_map = {
            "contributions": C.CONTRIBUTIONS,
            "expenditures":  C.EXPENDITURES,
            "committees":    C.COMMITTEES,
            "candidates":    C.CANDIDATES,
        }

        def _retire_wal():
            w = Path(str(OUT_DB) + ".wal")
            if w.exists():
                w.rename(str(w) + f".{int(time.time())}.old")

        _retire_wal()
        con = duckdb.connect(str(OUT_DB))

        aliases = []
        for i, (state_name, db_path) in enumerate(state_dbs):
            alias = f"s{i}"
            con.execute(f"ATTACH '{db_path}' AS {alias} (READ_ONLY)")
            aliases.append((alias, state_name))
            print(f"  attached: {state_name}")

        for table, cols in col_map.items():
            ft    = time.perf_counter()
            parts = []
            for alias, state_name in aliases:
                existing = {
                    row[0]
                    for row in con.execute(
                        f"SELECT column_name FROM information_schema.columns "
                        f"WHERE table_catalog='{alias}' AND table_name='{table}'"
                    ).fetchall()
                }
                select_cols = ", ".join(
                    f'CAST(t."{c}" AS VARCHAR) AS "{c}"' if c in existing
                    else f'NULL AS "{c}"'
                    for c in cols
                )
                parts.append(f"SELECT {select_cols} FROM {alias}.{table} t")

            union_sql = "\nUNION ALL\n".join(parts)
            print(f"\n  Building {table}...", end=" ", flush=True)
            con.execute(f"CREATE OR REPLACE TABLE {table} AS {union_sql}")
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            duration = round(time.perf_counter() - ft, 2)
            totals[table] = n
            print(f"{n:,} rows")
            log._emit("table_built", table=table, rows=n, duration_s=duration)
            tables_ok += 1

        con.close()
        _retire_wal()

        print(f"\n{'='*50}")
        print(f"  Totals in {OUT_DB.name}:")
        for table in TABLES:
            print(f"    {table:<15} {totals.get(table, 0):>10,} rows")
        print(f"{'='*50}")
        print(f"\nDone → {OUT_DB}")

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("aggregate_completed", status="completed", duration_s=duration,
                  states_count=len(state_dbs), tables_ok=tables_ok,
                  tables_err=tables_err, totals=totals)

    except KeyboardInterrupt:
        if con:
            con.close()
        log._emit("aggregate_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  states_count=len(state_dbs), tables_ok=tables_ok,
                  tables_err=tables_err)
        raise
    except Exception as e:
        if con:
            con.close()
        # DuckDB converts SIGINT to RuntimeError("Query interrupted")
        if isinstance(e, RuntimeError) and "interrupted" in str(e).lower():
            log._emit("aggregate_completed", status="interrupted",
                      duration_s=round(time.perf_counter() - t0, 1),
                      states_count=len(state_dbs), tables_ok=tables_ok,
                      tables_err=tables_err)
            raise KeyboardInterrupt() from e
        log._emit("aggregate_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  states_count=len(state_dbs), tables_ok=tables_ok,
                  tables_err=tables_err, error_type=type(e).__name__, error=str(e))
        raise


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
