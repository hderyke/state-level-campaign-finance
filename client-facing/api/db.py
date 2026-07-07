import os
import time
import duckdb

DB_PATH = os.getenv("DB_PATH", "../data/state-level-cf.db")

# Summary DB lives next to the main DB
_db_dir = os.path.dirname(os.path.abspath(DB_PATH))
SUMMARY_DB_PATH = os.path.join(_db_dir, "committee_summary.db")


def build_summary():
    """
    Aggregate total_raised / total_spent per (state, committee_name) and
    write to a small sidecar DuckDB file.  Called once at API startup.
    """
    print(f"[summary] building committee_summary → {SUMMARY_DB_PATH} …")
    t0 = time.time()

    # Remove stale summary file if present
    if os.path.exists(SUMMARY_DB_PATH):
        os.remove(SUMMARY_DB_PATH)

    con = duckdb.connect(SUMMARY_DB_PATH)
    con.execute(f"ATTACH '{DB_PATH}' AS cf (READ_ONLY)")
    con.execute("""
        CREATE TABLE committee_summary AS
        SELECT
            state,
            committee_name,
            COALESCE(SUM(CASE WHEN src = 'c' THEN total ELSE 0 END), 0) AS total_raised,
            COALESCE(SUM(CASE WHEN src = 'e' THEN total ELSE 0 END), 0) AS total_spent
        FROM (
            SELECT state, committee_name, SUM(amount) AS total, 'c' AS src
            FROM cf.contributions
            WHERE committee_name IS NOT NULL AND LENGTH(committee_name) > 0
            GROUP BY state, committee_name

            UNION ALL

            SELECT state, committee_name, SUM(amount) AS total, 'e' AS src
            FROM cf.expenditures
            WHERE committee_name IS NOT NULL AND LENGTH(committee_name) > 0
            GROUP BY state, committee_name
        )
        GROUP BY state, committee_name
    """)
    # Lookup: distinct committee names → their registered state (pick one per name)
    con.execute("""
        CREATE TABLE committee_name_lookup AS
        SELECT LOWER(committee_name) AS lcn,
               MIN(state)            AS state,
               ANY_VALUE(committee_name) AS canonical_name
        FROM cf.committees
        WHERE committee_name IS NOT NULL AND LENGTH(committee_name) > 0
        GROUP BY LOWER(committee_name)
    """)

    con.execute("DETACH cf")
    con.close()

    elapsed = time.time() - t0
    print(f"[summary] done in {elapsed:.1f}s")


def get_db():
    con = duckdb.connect(DB_PATH, read_only=True)
    if os.path.exists(SUMMARY_DB_PATH):
        try:
            con.execute(f"ATTACH '{SUMMARY_DB_PATH}' AS summary (READ_ONLY)")
        except Exception:
            pass  # already attached on this connection's shared catalog
    try:
        yield con
    finally:
        con.close()
