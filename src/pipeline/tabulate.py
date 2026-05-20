import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.logger import get_logger

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

    log = get_logger(state.lower(), "tabulate")
    t0  = time.perf_counter()
    log._emit("tabulate_started", db=f"{state_dir.name.lower()}.db")

    db_path = clean_dir / f"{state_dir.name.lower()}.db"
    con     = duckdb.connect(str(db_path))
    print(f"Building {db_path.name} from {clean_dir}")

    tables_ok  = 0
    tables_err = 0

    try:
        for table in TABLES:
            csv_path = next(
                (clean_dir / f"{table}{ext}" for ext in (".csv.gz", ".csv")
                 if (clean_dir / f"{table}{ext}").exists()),
                None,
            )
            if csv_path is None:
                print(f"  {table}: not found — skipping")
                log._emit("table_skipped", table=table, reason="file not found")
                tables_err += 1
                continue

            ft = time.perf_counter()
            con.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT * FROM read_csv_auto('{csv_path}', {OPTS})
            """)
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            duration = round(time.perf_counter() - ft, 2)
            print(f"  {table}: {n:,} rows")
            log._emit("table_loaded", table=table, rows=n, duration_s=duration)
            tables_ok += 1

        con.close()
        print(f"\nDone → {db_path}")
        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("tabulate_completed", status="completed", duration_s=duration,
                  tables_ok=tables_ok, tables_err=tables_err)

    except KeyboardInterrupt:
        con.close()
        log._emit("tabulate_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  tables_ok=tables_ok, tables_err=tables_err)
        raise
    except Exception as e:
        con.close()
        # DuckDB converts SIGINT to RuntimeError("Query interrupted") before
        # Python's KeyboardInterrupt fires — treat it as interrupted, not error
        if isinstance(e, RuntimeError) and "interrupted" in str(e).lower():
            log._emit("tabulate_completed", status="interrupted",
                      duration_s=round(time.perf_counter() - t0, 1),
                      tables_ok=tables_ok, tables_err=tables_err)
            raise KeyboardInterrupt() from e
        log._emit("tabulate_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  tables_ok=tables_ok, tables_err=tables_err,
                  error_type=type(e).__name__, error=str(e))
        raise


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("state", help="State name (e.g. arizona, Alabama)")
    args = ap.parse_args()
    try:
        tabulate(args.state)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
