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

# committee_type values for which a transaction's candidate_name is unambiguous
# (the committee IS that candidate's own committee, so "money to/from this
# committee" == "money to/from this candidate"). Everything else — PACs,
# independent expenditure committees, party committees, ballot measure
# committees, etc. — can legitimately record a candidate_name while SUPPORTING
# or OPPOSING that candidate (e.g. CA's SUP_OPP_CD='O' on Form 496 late
# independent-expenditure filings), and our schema has no field to carry that
# polarity. Rather than let "candidate_name = Gavin Newsom" look identical for
# a $1.5M pro-Newsom contribution and a $1.5M anti-Newsom independent
# expenditure, we only trust candidate_name when the receiving/associated
# committee is confirmed (via committee_type) to be the candidate's own.
# Determined empirically 2026-07-10 by checking committees.candidate_name fill
# rate per committee_type in state-level-cf.db: 'Candidate Committee'/'Candidate'
# and the office-titled variants below are 85-100% filled (unambiguous); PAC/
# Independent Expenditure/Ballot Measure/Party Committee are 12-37% (ambiguous);
# everything else is ~0%. See feedback/project memory for the full breakdown.
CANDIDATE_COMMITTEE_TYPES = {
    "Candidate Committee",
    "Candidate",
    "Governor",
    "Lt Governor",
    "Attorney General",
    "Secretary of State",
    "Secretary of Agriculture",
    "Auditor of State",
    "Treasurer of State",
    "City Candidate - Mayor",
    "City Candidate - City Council",
    "County Candidate - Sheriff",
    "County Candidate - Supervisor",
    "Other Political Subdivision Candidate",
}

# committees must come before contributions — the contributor backfill joins against it
COL_MAP = {
    "committees":    C.COMMITTEES_AGG,
    "candidates":    C.CANDIDATES_AGG,
    "contributions": C.CONTRIBUTIONS_AGG,
    "expenditures":  C.EXPENDITURES_AGG,
}

# Full-text search indexes built into the final db — power fast contributor/
# committee name search in client-facing/api/routers/contributions.py instead
# of an ILIKE '%...%' scan across the whole contributions table (that timed
# out the API Lambda's 29s cap on an unconstrained search, e.g. "Jim Walton"
# with no state filter, 2026-07-19).
#
# Built HERE (this machine), not inside cloud/lambda/db_sync's handler, where
# it was tried first and repeatedly OOM'd/timed out at real scale
# (2026-07-20) — that Lambda's memory is hard-capped at 3008MB by an AWS
# account quota that can't currently be raised. This function already solves
# the same class of problem for the indexes below (a real 103.6M-row OOM,
# see the CREATE INDEX comment further down) using a real disk-backed spill
# directory and freeing memory by detaching sources first — same fix, same
# place it already works, just extended to cover FTS too. Once this ships
# inside state-level-cf.db, cloud/lambda/db_sync's handler goes back to being
# a pure atomic file copy — no building, no memory pressure, regardless of
# how large the db gets.
#
# Each column gets its own small "docs" table (rowid + just that column)
# rather than indexing `contributions` directly, twice, for two different
# columns. DuckDB's PRAGMA create_fts_index names its generated schema
# fts_main_<input_table>, keyed only off the table name, not which column was
# indexed — calling it twice against `contributions` itself would silently
# clobber the first index (confirmed empirically: match_bm25 started
# returning multi-row garbage on the first index after the second call).
# Queried via fts_main_<docs_table>.match_bm25(rowid, ...) in
# client-facing/api/routers/contributions.py.
FTS_INDEXES = [
    ("contributor_name_docs", "contributions", "contributor_name"),
    ("committee_name_docs",   "contributions", "committee_name"),
]

# Physical row order for each built table. DuckDB's CREATE TABLE AS materializes
# rows in the order the SELECT produces them, and row groups are filled
# sequentially in that order — so an ORDER BY here directly controls each
# row-group's min/max zonemap. That's what lets range/sort queries on these
# columns skip row groups instead of scanning the whole table (DuckDB's ART
# indexes, added below, don't help range predicates — see CREATE INDEX
# comments).
#
# This is only the SECONDARY key — `state` is not listed here because it's no
# longer produced by a sort at all. It used to be an explicit `ORDER BY state,
# ...` over one giant 23-way UNION ALL, but that OOM'd on a real full-scale
# run (2026-07-11, Henry's machine: 16GB RAM / DuckDB's 12.7GB default
# ceiling) building the 103M-row contributions table — and kept failing
# identically even after disabling insertion-order preservation and reducing
# the sort to a single low-cardinality column, which pointed at the 23-way
# UNION ALL scan/cast itself (not the sort, not the index built later) as
# what was holding too much in flight simultaneously. Fixed by building the
# table state-by-state (see run()) instead: each state's chunk is INSERTed
# separately, so state-clustering falls out for free from insertion order
# (no sort needed for it), and peak memory is bounded to roughly one state's
# chunk at a time instead of all 23 states' pipelines open at once. That
# per-state scoping is also what makes it affordable to sort each chunk by
# the column below — a per-state sort (even California's) is a much smaller
# job than a single 103M-row global sort would have been.
SECONDARY_SORT_KEYS = {
    "committees":    "committee_name",
    "candidates":    "candidate_name",
    "contributions": "date",
    "expenditures":  "date",
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

        # The new physical ORDER BY on contributions/expenditures (see
        # TABLE_SORT_KEYS) is a real external sort over 100M+ rows and can
        # need to spill to disk. DuckDB's default temp_directory is wherever
        # tmp_db landed (Python's tempfile default, usually a small /tmp
        # partition — same issue the TMPDIR env var workaround elsewhere in
        # this file's docs addresses for disk space, not memory). Without an
        # explicit spill location DuckDB can hit its memory_limit and error
        # out instead of spilling, rather than just running slower — confirmed
        # via a sandboxed reproduction (2-state, 12M-row subset OOM'd with the
        # default temp dir, succeeded once pointed at DATA_DIR, which is on
        # the same large disk as the state DBs themselves).
        tmp_spill_dir = DATA_DIR / ".aggregate_tmp"
        tmp_spill_dir.mkdir(exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{tmp_spill_dir}'")

        # DuckDB's default keeps parallel operators' output in the same order
        # rows were produced, so a large ORDER BY (contributions is 103M+
        # rows) has to buffer/merge partial sorted runs across all threads
        # before it can write anything out — that's what OOM'd on a real
        # full-scale run (12.7GB hit). We don't need that guarantee: the
        # physical order we actually want is the explicit ORDER BY in the
        # CREATE TABLE AS below, which is enforced regardless of this
        # setting. Disabling it lets DuckDB write out row groups as each
        # thread finishes its own sorted chunk instead of holding everything
        # for a global merge — this is DuckDB's own suggested fix for
        # exactly this OOM (see its error message).
        con.execute("PRAGMA preserve_insertion_order=false")

        # DuckDB ATTACH requires identifier-safe names; use s0, s1, ... instead
        # of state names which may contain spaces or reserved words.
        aliases = []
        for i, (state_name, db_path) in enumerate(state_dbs):
            alias = f"s{i}"
            con.execute(f"ATTACH '{db_path}' AS {alias} (READ_ONLY)")
            aliases.append((alias, state_name))
            print(f"  attached: {state_name}")

        for table, cols in COL_MAP.items():
            ft     = time.perf_counter()
            chunks = []  # (state_name, select_sql) — one per attached state db

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
                chunks.append((state_name, f"SELECT {select_cols} FROM {alias}.{table} t"))

            try:
                print(f"\n  Building {table}...", end=" ", flush=True)

                # Built state-by-state — CREATE ... AS <first state's SELECT>
                # LIMIT 0 for the schema, then one INSERT per state — instead
                # of a single 23-way UNION ALL. See SECONDARY_SORT_KEYS'
                # comment for why: the UNION ALL form OOM'd on a real
                # full-scale run. Each INSERT also carries its own ORDER BY on
                # the secondary sort key, so the physical layout still ends up
                # state-clustered (free, from insertion order) with each
                # state's block internally sorted — same end result as the
                # old single global ORDER BY, built at bounded, per-state cost
                # instead of all at once.
                secondary_key = SECONDARY_SORT_KEYS.get(table)
                _, first_sql = chunks[0]
                con.execute(f"CREATE OR REPLACE TABLE {table} AS\n{first_sql}\nLIMIT 0")
                for state_name, select_sql in chunks:
                    order_clause = f"\nORDER BY {secondary_key}" if secondary_key else ""
                    con.execute(f"INSERT INTO {table}\n{select_sql}{order_clause}")
                print("done")

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
                    # SC patterns are uppercase because the SC parser normalizes
                    # office through utils.clean_name; LIKE is case-sensitive.
                    for like_state, like_pattern, canon in [
                        ("GA", "State Representative%", "State Representative"),
                        ("GA", "State Senate%",         "State Senator"),
                        ("SC", "SC SENATE%",            "State Senator"),
                        ("SC", "SC HOUSE%",             "State Representative"),
                        ("SC", "SCHOOL BOARD TRUSTEE%", "School Board Trustee"),
                        ("SC", "COUNTY COUNCIL%",       "County Council"),
                        ("SC", "%SHERIFF",              "County Sheriff"),
                        ("SC", "%CORONER",              "County Coroner"),
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

                    # candidate_name is only trustworthy when the receiving committee
                    # is confirmed to be that candidate's own committee — see
                    # CANDIDATE_COMMITTEE_TYPES comment above. Blank it everywhere else
                    # (PACs, independent expenditure committees, party/ballot-measure
                    # committees, and any committee_name that doesn't match a known
                    # committee at all) so a PAC's opposition spending can't masquerade
                    # as support for the named candidate.
                    blanked_before = con.execute(
                        "SELECT COUNT(*) FROM contributions WHERE candidate_name IS NOT NULL AND candidate_name != ''"
                    ).fetchone()[0]
                    types_sql = ", ".join(f"'{t}'" for t in CANDIDATE_COMMITTEE_TYPES)
                    con.execute(f"""
                        UPDATE contributions
                        SET candidate_name = NULL
                        WHERE candidate_name IS NOT NULL AND candidate_name != ''
                        AND NOT EXISTS (
                            SELECT 1 FROM committees c
                            WHERE c.state = contributions.state
                              AND c.committee_name = contributions.committee_name
                              AND c.committee_type IN ({types_sql})
                        )
                    """)
                    blanked_after = con.execute(
                        "SELECT COUNT(*) FROM contributions WHERE candidate_name IS NOT NULL AND candidate_name != ''"
                    ).fetchone()[0]
                    print(f"    candidate_name: blanked {blanked_before - blanked_after:,} of {blanked_before:,} "
                          f"non-candidate-committee rows")
                    log._emit("candidate_name_scoped", table="contributions",
                              blanked=blanked_before - blanked_after, kept=blanked_after)

                if table == "expenditures":
                    # transaction_category is handled inline above (computed per-row
                    # from transaction_type before this block runs). Same candidate_name
                    # scoping as contributions — see CANDIDATE_COMMITTEE_TYPES comment.
                    blanked_before = con.execute(
                        "SELECT COUNT(*) FROM expenditures WHERE candidate_name IS NOT NULL AND candidate_name != ''"
                    ).fetchone()[0]
                    types_sql = ", ".join(f"'{t}'" for t in CANDIDATE_COMMITTEE_TYPES)
                    con.execute(f"""
                        UPDATE expenditures
                        SET candidate_name = NULL
                        WHERE candidate_name IS NOT NULL AND candidate_name != ''
                        AND NOT EXISTS (
                            SELECT 1 FROM committees c
                            WHERE c.state = expenditures.state
                              AND c.committee_name = expenditures.committee_name
                              AND c.committee_type IN ({types_sql})
                        )
                    """)
                    blanked_after = con.execute(
                        "SELECT COUNT(*) FROM expenditures WHERE candidate_name IS NOT NULL AND candidate_name != ''"
                    ).fetchone()[0]
                    print(f"    candidate_name: blanked {blanked_before - blanked_after:,} of {blanked_before:,} "
                          f"non-candidate-committee rows")
                    log._emit("candidate_name_scoped", table="expenditures",
                              blanked=blanked_before - blanked_after, kept=blanked_after)

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

        # Indexes are built here — after every table is fully built/normalized
        # and the 23 read-only per-state DBs are detached — not inline during
        # the loop above. Originally they were built inline (committees' index
        # early, so the contributor_type backfill/candidate_name scoping
        # UPDATEs above could use it too), but a real full-scale run
        # (2026-07-11, 103.6M-row contributions) OOM'd at DuckDB's default
        # memory ceiling even after the ORDER BY sort was reduced to a trivial
        # single low-cardinality column — which pointed at CREATE INDEX itself
        # as the actual memory-heavy step (DuckDB's ART index build isn't as
        # gracefully out-of-core as sorts/joins), not the sort. Detaching the
        # 23 source DBs first frees their read buffers before the index build
        # needs the headroom. Trade-off: the committees index no longer speeds
        # up this run's own backfill/scoping UPDATEs (they ran unindexed,
        # same as before this whole change) — acceptable, since that was a
        # bonus, not the point; the committee-profile API win is unaffected
        # either way, since it only needs the index to exist in the final file.
        print("\n  Detaching source DBs before index build...", end=" ", flush=True)
        for alias, _ in aliases:
            con.execute(f"DETACH {alias}")
        print("done")

        for label, sql in [
            ("idx_committees_state_name",          "CREATE INDEX IF NOT EXISTS idx_committees_state_name ON committees(state, committee_name)"),
            ("idx_contributions_state_committee",  "CREATE INDEX IF NOT EXISTS idx_contributions_state_committee ON contributions(state, committee_name)"),
            ("idx_expenditures_state_committee",   "CREATE INDEX IF NOT EXISTS idx_expenditures_state_committee ON expenditures(state, committee_name)"),
        ]:
            it0 = time.perf_counter()
            print(f"  Building {label}...", end=" ", flush=True)
            con.execute(sql)
            print(f"{round(time.perf_counter() - it0, 1)}s")

        # See FTS_INDEXES' comment for why this lives here instead of
        # cloud/lambda/db_sync. Runs on the same connection as the CREATE
        # INDEX calls above, after sources are already detached, so it gets
        # the same memory headroom and spill directory for free. INSTALL/LOAD
        # (not a curl-fetched explicit-path load, like the Lambda images use)
        # is fine here — this runs natively on this machine, not inside a
        # QEMU-emulated Docker build, so there's no segfault risk and normal
        # internet access to fetch the extension.
        print("\n  Building full-text search indexes...")
        con.execute("INSTALL fts")
        con.execute("LOAD fts")
        for docs_table, src_table, column in FTS_INDEXES:
            it0 = time.perf_counter()
            print(f"    {docs_table}...", end=" ", flush=True)
            con.execute(f"DROP TABLE IF EXISTS {docs_table}")
            con.execute(f"CREATE TABLE {docs_table} AS SELECT rowid, {column} FROM {src_table}")
            con.execute(f"PRAGMA create_fts_index('{docs_table}', 'rowid', '{column}', overwrite=1)")
            print(f"{round(time.perf_counter() - it0, 1)}s")

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
