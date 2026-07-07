"""
aggregate.py — Combine all finished state .db files into one.

Scans data/{state}/cleaned/{state}.db for every state that has been tabulated,
then merges all four tables (contributions, expenditures, committees, candidates)
into data/state-level-cf.db.

Normalizations applied at aggregate time (raw values preserved in per-state DBs):
  contributions.contributor_type     → canonical via src/aliases/contributor_types.csv
  contributions.transaction_category → derived via src/aliases/transaction_categories.csv
  expenditures.transaction_category  → derived inline from transaction_type via src/aliases/expenditure_categories.csv
  committees.committee_type          → canonical via src/aliases/committee_types.csv

Usage:
    python3 src/pipeline/aggregate.py
"""

import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
from src.reporting.logger import get_logger
import columns as C
from src.aliases import (
    contributor_type_mappings,
    transaction_category_mappings,
    committee_type_mappings,
    expenditure_category_mappings,
    office_type_mappings,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR     = PROJECT_ROOT / "data"
OUT_DB       = DATA_DIR / "state-level-cf.db"

TABLES = ["contributions", "expenditures", "committees", "candidates"]  # used for totals summary only; COL_MAP drives the build loop

# committees must come before contributions — the contributor backfill joins against it
COL_MAP = {
    "committees":    C.COMMITTEES_AGG,
    "candidates":    C.CANDIDATES_AGG,
    "contributions": C.CONTRIBUTIONS_AGG,
    "expenditures":  C.EXPENDITURES_AGG,
}

def find_state_dbs() -> list[tuple[str, Path]]:
    """Return (state_name, db_path) for every state that has a .db file."""
    found = []
    for state_dir in sorted(DATA_DIR.iterdir()):
        if not state_dir.is_dir() or state_dir.name.endswith(".db"):
            continue
        db_path = state_dir / "cleaned" / f"{state_dir.name.lower()}.db"
        if db_path.exists():
            found.append((state_dir.name, db_path))
    return found


def _cast(col: str, existing: set[str]) -> str:
    """ Enforces types across the state dbs"""
    cast_type = C.COLUMN_TYPES.get(col, "VARCHAR")
    if col not in existing:
        return f'CAST(NULL AS {cast_type}) AS "{col}"'
    return f'CAST(t."{col}" AS {cast_type}) AS "{col}"'


def _case_expr(
    mapping: dict[tuple[str, str], str | None],
    state_col: str,
    raw_col: str,
    else_expr: str | None = None,
) -> str:
    """
    Build a DuckDB CASE expression from a (STATE_UPPER, RAW_UPPER) → canonical|None dict.

    else_expr controls what happens for unmapped values:
      - contributor_type:    ELSE raw_col  (keep raw for unknown states/values)
      - transaction_category: ELSE NULL    (don't invent categories for unknown types)
    """
    whens = []
    for (state, raw), canon in mapping.items():
        safe_raw = raw.replace("'", "''")
        if canon is None:
            whens.append(
                f"WHEN {state_col} = '{state}' AND UPPER({raw_col}) = '{safe_raw}' THEN NULL"
            )
        else:
            safe_canon = canon.replace("'", "''")
            whens.append(
                f"WHEN {state_col} = '{state}' AND UPPER({raw_col}) = '{safe_raw}' THEN '{safe_canon}'"
            )
    resolved_else = else_expr if else_expr is not None else raw_col
    if not whens:
        return resolved_else
    return "CASE\n            " + "\n            ".join(whens) + f"\n            ELSE {resolved_else} END"


def run():
    """Discover all tabulated state .db files and merge them into a single aggregate database."""
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

    # Pre-build alias CASE expressions for normalization
    ctype_case   = _case_expr(
        contributor_type_mappings(),
        'state', 'contributor_type',
        else_expr='contributor_type',   # unknown → keep raw
    )
    tcat_case    = _case_expr(
        transaction_category_mappings(),
        'state', 'transaction_type',
        else_expr='NULL',               # unknown → NULL (don't invent categories)
    )
    cmtetype_case = _case_expr(
        committee_type_mappings(),
        'state', 'committee_type',
        else_expr='committee_type',     # unknown → keep raw
    )
    ecat_case    = _case_expr(
        expenditure_category_mappings(),
        'state', 'transaction_type',
        else_expr='NULL',               # unknown → NULL (don't invent categories)
    )
    ofc_case     = _case_expr(
        office_type_mappings(),
        'state', 'office',
        else_expr='NULL',               # unknown → NULL (don't invent canonical labels)
    )

    con        = None
    tables_ok  = 0
    tables_err = 0
    totals     = {}

    # Some mount filesystems (macOS FUSE, network mounts) forbid DuckDB from
    # deleting its own WAL after a checkpoint.  Build in /tmp and copy the
    # finished file to the final destination to avoid that restriction.
    tmp_db = Path(tempfile.mktemp(suffix=".db", prefix="state-level-cf-"))

    try:
        if OUT_DB.exists():
            try:
                OUT_DB.unlink()
            except PermissionError:
                OUT_DB.rename(str(OUT_DB) + f".{int(time.time())}.old")

        # A stale .wal from a previously interrupted run can prevent DuckDB
        # from opening the file — rename rather than delete to preserve it.
        wal = Path(str(OUT_DB) + ".wal")
        if wal.exists():
            wal.rename(str(wal) + f".{int(time.time())}.old")

        con = duckdb.connect(str(tmp_db))

        # DuckDB ATTACH requires identifier-safe names; use s0, s1, ... instead
        # of state names which may contain spaces or reserved words.
        aliases = []
        for i, (state_name, db_path) in enumerate(state_dbs):
            alias = f"s{i}"
            con.execute(f"ATTACH '{db_path}' AS {alias} (READ_ONLY)")
            aliases.append((alias, state_name))
            print(f"  attached: {state_name}")

        for table, cols in COL_MAP.items():
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
                if table == "expenditures":
                    # transaction_category is computed inline from transaction_type
                    # (which stays in per-state DBs only). All other cols cast normally.
                    exprs = []
                    for c in cols:
                        if c == "transaction_category":
                            exprs.append(f'{ecat_case} AS "transaction_category"')
                        else:
                            exprs.append(_cast(c, existing))
                    select_cols = ", ".join(exprs)
                else:
                    select_cols = ", ".join(_cast(c, existing) for c in cols)
                parts.append(f"SELECT {select_cols} FROM {alias}.{table} t")

            union_sql = "\nUNION ALL\n".join(parts)

            try:
                print(f"\n  Building {table}...", end=" ", flush=True)
                con.execute(f"CREATE OR REPLACE TABLE {table} AS\n{union_sql}")

                # Table-specific normalization
                if table == "committees":
                    con.execute(f"""
                        UPDATE committees
                        SET committee_type = {cmtetype_case}
                        WHERE committee_type IS NOT NULL
                    """)



                if table == "candidates":
                    # canonical_office → derived from office via office_types.csv (exact match)
                    con.execute(f"""
                        UPDATE candidates
                        SET canonical_office = {ofc_case}
                        WHERE office IS NOT NULL
                    """)
                    # LIKE-based fallbacks for states that embed district info in the office field
                    # (e.g. GA "State Representative District: 15" → "State Representative")
                    for like_state, like_pattern, canon in [
                        ("GA", "State Representative%", "State Representative"),
                        ("GA", "State Senate%",         "State Senator"),
                    ]:
                        con.execute(f"""
                            UPDATE candidates
                            SET canonical_office = '{canon}'
                            WHERE canonical_office IS NULL
                            AND state = '{like_state}'
                            AND office LIKE '{like_pattern}'
                        """)

                if table == "contributions":
                    # contributor_type → canonical (keep raw for unmapped values)
                    con.execute(f"""
                        UPDATE contributions
                        SET contributor_type = {ctype_case}
                        WHERE contributor_type IS NOT NULL
                    """)
                    # transaction_category → derived from transaction_type
                    con.execute(f"""
                        UPDATE contributions
                        SET transaction_category = {tcat_case}
                    """)
                    # contributor_type backfill — join against committees for rows
                    # where contributor_type is still NULL (e.g. Alaska, Arizona).
                    # committees has already been normalized so values are canonical.
                    null_before = con.execute(
                        "SELECT COUNT(*) FROM contributions WHERE contributor_type IS NULL"
                    ).fetchone()[0]
                    print(f"    backfilling contributor_type from committees...", end=" ", flush=True)
                    con.execute("""
                        UPDATE contributions
                        SET contributor_type = (
                            SELECT committee_type
                            FROM committees
                            WHERE committees.state        = contributions.state
                            AND   committees.committee_name = contributions.contributor_name
                            LIMIT 1
                        )
                        WHERE contributor_type IS NULL
                        AND   contributor_name IS NOT NULL
                    """)
                    null_after = con.execute(
                        "SELECT COUNT(*) FROM contributions WHERE contributor_type IS NULL"
                    ).fetchone()[0]
                    backfilled = null_before - null_after
                    print(f"{backfilled:,} rows filled")
                    log._emit("contributor_backfill", backfilled=backfilled,
                              still_null=null_after)

                n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                duration = round(time.perf_counter() - ft, 2)
                totals[table] = n
                print(f"{n:,} rows")
                log._emit("table_built", table=table, rows=n, duration_s=duration)
                tables_ok += 1

            except Exception as table_err:
                duration = round(time.perf_counter() - ft, 2)
                print(f"ERROR — {table_err}")
                log._emit("table_error", table=table, error=str(table_err), duration_s=duration)
                tables_err += 1

        con.close()
        shutil.copy2(tmp_db, OUT_DB)
        tmp_db.unlink(missing_ok=True)

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
        tmp_db.unlink(missing_ok=True)
        log._emit("aggregate_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  states_count=len(state_dbs), tables_ok=tables_ok,
                  tables_err=tables_err)
        raise
    except Exception as e:
        if con:
            con.close()
        tmp_db.unlink(missing_ok=True)
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

# ====== CLI ==================================
if __name__ == "__main__":
    import argparse
    argparse.ArgumentParser(
        description="Merge all tabulated state .db files into data/state-level-cf.db."
    ).parse_args()
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
