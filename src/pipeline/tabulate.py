"""
tabulate.py — Load cleaned CSVs into a per-state DuckDB database.

Reads all available cleaned relations from data/<State>/cleaned/ and writes
a single DuckDB file (e.g. alaska.db) queryable directly in SQL or loadable
into R via duckdb::dbConnect(). Always rebuilds from scratch to avoid page
bloat from incremental updates.
"""

import argparse
import sys
import time
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
from src.reporting.logger import get_logger
import columns as C


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TABLES       = ["contributions", "expenditures", "committees", "candidates"]
OPTS         = "null_padding=true, ignore_errors=true, parallel=false"


# parallel=false: DuckDB parallel reads can collide when writing to a single
# file; disabling avoids spurious lock errors on sequential table loads.

# DuckDB map literal passed to read_csv_auto's types= parameter; enforces
# consistent typing and prevents all-NULL columns from defaulting to VARCHAR.
_TYPES_STR = "{" + ", ".join(f"'{k}': '{v}'" for k, v in C.COLUMN_TYPES.items()) + "}"


def tabulate(state: str):
    # Case-insensitive match against data/ subdirectories. Underscores and
    # spaces are treated as equivalent so a multi-word state resolves whether
    # it's given as orc.py passes it ("new mexico", from states.csv) or as the
    # scraper/parser module is named ("new_mexico"), which is what a human
    # hand-running this stage will reach for.
    want    = state.lower().replace("_", " ")
    matches = [d for d in (PROJECT_ROOT / "data").iterdir()
               if d.is_dir() and d.name.lower().replace("_", " ") == want]
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
    if db_path.exists():
        db_path.unlink()   # always start fresh — avoids page bloat from OR REPLACE
    con     = duckdb.connect(str(db_path))
    print(f"Building {db_path.name} from {clean_dir}")

    tables_ok  = 0
    tables_err = 0

    try:
        for table in TABLES:
            # prefer .csv.gz; fall back to uncompressed .csv
            csv_path = next(
                (clean_dir / f"{table}{ext}" for ext in (".csv.gz", ".csv")
                 if (clean_dir / f"{table}{ext}").exists()),
                None,
            )
            if csv_path is None:
                print(f"  {table}: not found — skipping")
                log._emit("table_skipped", table=table, reason="file not found")
                continue

            if csv_path.stat().st_size == 0:
                print(f"  [!] {table}: EMPTY FILE — parser may not have finished. "
                      f"Re-run the parser, then tabulate again.")
                log._emit("table_skipped", table=table, reason="empty file",
                          path=str(csv_path))
                tables_err += 1
                continue

            ft = time.perf_counter()
            try:
                con.execute(f"""
                    CREATE OR REPLACE TABLE {table} AS
                    SELECT * FROM read_csv_auto('{csv_path}', {OPTS}, types={_TYPES_STR})
                """)
                n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                duration = round(time.perf_counter() - ft, 2)
                print(f"  {table}: {n:,} rows")
                log._emit("table_loaded", table=table, rows=n, duration_s=duration)
                tables_ok += 1
            except Exception as table_err:
                duration = round(time.perf_counter() - ft, 2)
                print(f"  {table}: ERROR — {table_err}")
                log._emit("table_skipped", table=table, reason="load error",
                          error=str(table_err), duration_s=duration)
                tables_err += 1

        con.close()
        db_bytes = db_path.stat().st_size if db_path.exists() else 0
        print(f"\nDone → {db_path}")
        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("tabulate_completed", status="completed", duration_s=duration,
                  tables_ok=tables_ok, tables_err=tables_err, bytes=db_bytes)

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

# ====== CLI ==================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Build a DuckDB database from a state's cleaned CSVs."
    )
    ap.add_argument("state", help="State name (e.g. arizona, Alabama)")
    args = ap.parse_args()
    try:
        tabulate(args.state)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
