"""
parsers/california.py — Transform California CAL-ACCESS raw TSVs into the
5 normalized relations.

Input:  data/California/raw/
  CVR_CAMPAIGN_DISCLOSURE_CD.tsv  — cover records (one per filing/amendment)
  FILERNAME_CD.tsv                — filer name/address registry
  FILER_TO_FILER_TYPE_CD.tsv      — filer type codes, party, active flag
  RCPT_CD.tsv                     — contributions received  (~19 M rows)
  EXPN_CD.tsv                     — expenditures made       (~15 M rows)
  LOAN_CD.tsv                     — loans received/made     (~96 K rows)
  DEBT_CD.tsv                     — debts owed              (~715 K rows)

Output: data/California/cleaned/
  contributions.csv, expenditures.csv, committees.csv,
  candidates.csv, loans_debts.csv

Implementation
──────────────
  Uses DuckDB for all heavy file I/O so that multi-GB TSVs (RCPT_CD 3.5 GB,
  EXPN_CD 2.8 GB) are processed in seconds rather than minutes.  Python is
  used only for the small reference-table work and for utils.assign_person_ids.

Amendment dedup
───────────────
  CAL-ACCESS stores every amendment as a separate set of rows sharing the
  same FILING_ID but with increasing AMEND_ID.  We pre-build a cvr_dedup
  table that keeps only the max-AMEND_ID row per FILING_ID; joining the
  transaction tables on (FILING_ID, AMEND_ID) automatically retains only
  the most-recent version of each contribution/expenditure.

Encoding
────────
  All TSVs are latin-1 (ISO-8859-1).  DuckDB's read_csv handles this via
  the encoding parameter.

Amount format
─────────────
  Already plain numeric strings ('109.89', '2000') — no $ or commas.
"""

import sys
import time
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import utils

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR  = PROJECT_ROOT / "data" / "California" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "California" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "CA"


def raw(name: str) -> str:
    return str(RAW_DIR / name).replace("'", "''")


def out(name: str) -> str:
    return str(CLEAN_DIR / name).replace("'", "''")


# ── DuckDB read_csv options ─────────────────────────────────────────────────
# all_varchar avoids type-inference surprises; ignore_errors skips NUL-byte
# rows and other malformed lines; null_padding handles short rows.
ROPT = "sep='\\t', all_varchar=true, ignore_errors=true, null_padding=true, strict_mode=false"


# ── SQL expression helpers ───────────────────────────────────────────────────
def build_name(last: str, first: str) -> str:
    """SQL expression → 'LAST, FIRST' or whichever part is non-empty."""
    return f"""
        CASE
            WHEN NULLIF(TRIM({last}), '') IS NOT NULL
             AND NULLIF(TRIM({first}), '') IS NOT NULL
                THEN TRIM({last}) || ', ' || TRIM({first})
            WHEN NULLIF(TRIM({last}), '') IS NOT NULL
                THEN TRIM({last})
            ELSE COALESCE(NULLIF(TRIM({first}), ''), '')
        END""".strip()


def build_cand(last: str, first: str) -> str:
    """SQL expression → 'LAST, FIRST' (raw case), or '' for placeholder values."""
    return f"""
        CASE
            WHEN {last} IS NULL
              OR LOWER(TRIM({last})) IN ('n/a', 'na', 'none', 'unknown', '-', '')
                THEN ''
            WHEN NULLIF(TRIM({first}), '') IS NOT NULL
                THEN TRIM({last}) || ', ' || TRIM({first})
            ELSE TRIM({last})
        END""".strip()


def parse_date(col: str) -> str:
    """SQL expression → DATE from CAL-ACCESS date string, NULL on failure or outside 1975–2035."""
    inner = (
        f"COALESCE("
        f"TRY_STRPTIME(TRIM({col}), '%m/%d/%Y %I:%M:%S %p'), "
        f"TRY_STRPTIME(TRIM({col}), '%m/%d/%Y'), "
        f"TRY_STRPTIME(TRIM({col}), '%Y-%m-%d')"
        f")::DATE"
    )
    return f"CASE WHEN YEAR({inner}) BETWEEN 1975 AND 2035 THEN ({inner}) ELSE NULL END"


def fmt_date(col: str) -> str:
    """SQL expression → DATE string 'YYYY-MM-DD', '' on failure."""
    return f"COALESCE(CAST({parse_date(col)} AS VARCHAR), '')"


def election_yr(col: str) -> str:
    """SQL expression → 4-digit election year INTEGER, NULL on failure."""
    return f"YEAR({parse_date(col)})"


PARTY_CASE = """
    CASE PARTY_CD
        WHEN '16012' THEN 'Democratic'
        WHEN '16013' THEN 'Republican'
        WHEN '16020' THEN 'Green'
        WHEN '16023' THEN 'Libertarian'
        WHEN '16025' THEN 'American Independent'
        WHEN '16027' THEN 'Peace and Freedom'
        WHEN '16029' THEN 'Reform'
        WHEN '16999' THEN 'Other'
        ELSE ''
    END""".strip()


# ── State_filer_id resolution for transaction rows ───────────────────────────
# Prefer CMTE_ID from the transaction row; fall back to FILER_ID from CVR.
def resolve_fid(cmte_col: str, cvr_fid: str) -> str:
    return f"COALESCE(NULLIF(TRIM({cmte_col}), ''), {cvr_fid}, '')"


# ── Candidate name for a transaction row ────────────────────────────────────
# Use row-level CAND_NAML/NAMF if present, else fall back to CVR cand_name.
def txn_cand(row_last: str, row_first: str, cvr_cand: str) -> str:
    row = build_cand(row_last, row_first)
    return f"CASE WHEN ({row}) != '' THEN ({row}) ELSE COALESCE({cvr_cand}, '') END"


# Path for the persistent reference-table DB (written by stage 1, read by 2-4)
REF_DB  = str(CLEAN_DIR / "_ca_ref.db")
# Final output DB — stages 2-4 write large tables directly here (skip CSV)
MAIN_DB = str(CLEAN_DIR / "california.db")


def _open_main_db() -> duckdb.DuckDBPyConnection:
    """Open california.db for writing, working around the FUSE-filesystem
    limitation that prevents DuckDB from unlinking the WAL file.

    On macOS FUSE mounts (e.g. NFS, osxfuse), DuckDB's call to unlink()
    the WAL returns EPERM.  We rename any existing WAL before opening
    (so DuckDB starts fresh) and rename the newly-created WAL after the
    connection closes (so the next open doesn't hit the same error).

    Usage:
        con = _open_main_db()
        try:
            ...
        finally:
            con.close()
            _retire_wal()
    """
    wal = Path(MAIN_DB + ".wal")
    if wal.exists():
        wal.rename(str(wal) + f".{int(time.time())}.old")
    return duckdb.connect(MAIN_DB)


def _retire_wal() -> None:
    """Rename any WAL left behind after a write session (FUSE workaround)."""
    wal = Path(MAIN_DB + ".wal")
    if wal.exists():
        wal.rename(str(wal) + f".{int(time.time())}.old")


def _build_ref_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Load CVR, FILERNAME, and FILER_TYPES into `con` as DuckDB tables."""
    print("  Loading CVR (max-amend dedup)...", end=" ", flush=True)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS cvr_dedup AS
        SELECT
            FILING_ID, FILER_ID, AMEND_ID,
            ELECT_DATE, OFFICE_CD, CAND_NAML, CAND_NAMF,
            CMTTE_TYPE, DIST_NO, JURIS_CD,
            TRES_NAML, TRES_NAMF, FILER_CITY, FILER_ZIP4,
            {build_cand('CAND_NAML', 'CAND_NAMF')} AS cand_name
        FROM (
            SELECT *,
                   MAX(TRY_CAST(AMEND_ID AS INT))
                       OVER (PARTITION BY FILING_ID) AS max_amid
            FROM read_csv('{raw("CVR_CAMPAIGN_DISCLOSURE_CD.tsv")}', {ROPT})
            WHERE NULLIF(TRIM(FILING_ID), '') IS NOT NULL
        ) sub
        WHERE TRY_CAST(AMEND_ID AS INT) = max_amid
          AND NULLIF(TRIM(FILING_ID), '') IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM cvr_dedup").fetchone()[0]
    print(f"{n:,} filings")

    print("  Loading FILERNAME_CD...", end=" ", flush=True)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS filername AS
        SELECT
            FILER_ID,
            {build_name('NAML', 'NAMF')} AS committee_name,
            COALESCE(NULLIF(TRIM(CITY), ''), '')       AS city,
            COALESCE(NULLIF(TRIM(ZIP4), ''), '')       AS zip,
            COALESCE(NULLIF(TRIM(FILER_TYPE), ''), '') AS filer_type,
            CASE WHEN UPPER(TRIM(STATUS)) = 'ACTIVE' THEN '1' ELSE '0' END AS active_fn
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY FILER_ID
                       ORDER BY
                           CASE WHEN UPPER(TRIM(STATUS)) = 'ACTIVE' THEN 0 ELSE 1 END,
                           XREF_FILER_ID DESC NULLS LAST
                   ) AS rn
            FROM read_csv('{raw("FILERNAME_CD.tsv")}', {ROPT})
            WHERE NULLIF(TRIM(FILER_ID), '') IS NOT NULL
        ) sub
        WHERE rn = 1
    """)
    n = con.execute("SELECT COUNT(*) FROM filername").fetchone()[0]
    print(f"{n:,} filers")

    print("  Loading FILER_TO_FILER_TYPE_CD...", end=" ", flush=True)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS filer_types AS
        SELECT
            FILER_ID,
            {PARTY_CASE} AS party,
            CASE WHEN UPPER(TRIM(ACTIVE)) IN ('T', 'Y') THEN '1' ELSE '0' END AS active_ft
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY FILER_ID
                       ORDER BY COALESCE(
                           TRY_STRPTIME(TRIM(EFFECT_DT), '%m/%d/%Y %I:%M:%S %p'),
                           TRY_STRPTIME(TRIM(EFFECT_DT), '%m/%d/%Y')
                       ) DESC NULLS LAST
                   ) AS rn
            FROM read_csv('{raw("FILER_TO_FILER_TYPE_CD.tsv")}', {ROPT})
            WHERE NULLIF(TRIM(FILER_ID), '') IS NOT NULL
        ) sub
        WHERE rn = 1
    """)
    n = con.execute("SELECT COUNT(*) FROM filer_types").fetchone()[0]
    print(f"{n:,} entries")


def run(stage: int | None = None):
    """
    stage=None  → run all four stages in sequence
    stage=1     → build ref tables + write candidates.csv + committees.csv
    stage=2     → write contributions.csv  (RCPT_CD.tsv)
    stage=3     → write expenditures.csv   (EXPN_CD.tsv)
    stage=4     → write loans_debts.csv    (LOAN_CD + DEBT_CD)
    """
    run_all = stage is None

    # ── Stage 1: reference tables + candidates + committees ───────────────────
    if run_all or stage == 1:
        # Persist the ref tables to disk so later stages can reload without
        # re-scanning the raw TSVs.
        ref_con = duckdb.connect(REF_DB)
        _build_ref_tables(ref_con)

        # ── Candidates from CVR ───────────────────────────────────────────────
        print("  candidates...", end=" ", flush=True)
        cand_path = out("candidates.csv")
        ref_con.execute(f"""
            COPY (
                WITH cp_stats AS (
                    -- Per-filer count of C/P filings vs total filings.
                    -- Used to exclude large PACs that have a handful of
                    -- mis-typed C/P rows alongside thousands of G/NULL rows.
                    SELECT
                        FILER_ID,
                        COUNT(*) FILTER (WHERE CMTTE_TYPE IN ('C', 'P')) AS cp_count,
                        COUNT(*)                                           AS total_count
                    FROM cvr_dedup
                    WHERE NULLIF(TRIM(FILER_ID), '') IS NOT NULL
                    GROUP BY FILER_ID
                )
                SELECT
                    'CA'                                                  AS state,
                    ''                                                    AS person_id,
                    c.FILER_ID                                            AS state_filer_id,
                    c.cand_name                                           AS candidate_name,
                    COALESCE(NULLIF(TRIM(c.CAND_NAMF), ''), '')          AS candidate_first,
                    COALESCE(NULLIF(TRIM(c.CAND_NAML), ''), '')          AS candidate_last,
                    COALESCE(NULLIF(TRIM(c.OFFICE_CD), ''), '')           AS office,
                    COALESCE(NULLIF(TRIM(c.DIST_NO),   ''), '')           AS district,
                    COALESCE(NULLIF(TRIM(c.JURIS_CD),  ''), '')           AS jurisdiction,
                    COALESCE(ft.party, '')                                AS party,
                    {election_yr('c.ELECT_DATE')}                        AS election_year,
                    ''                                                    AS status,
                    ''                                                    AS incumbent,
                    'CVR_CAMPAIGN_DISCLOSURE_CD.tsv'                      AS raw_file,
                    ''                                                    AS row_num
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY FILER_ID
                               ORDER BY TRY_CAST(FILING_ID AS BIGINT) DESC NULLS LAST
                           ) AS rn
                    FROM cvr_dedup
                    WHERE cand_name != ''
                      AND NULLIF(TRIM(FILER_ID), '') IS NOT NULL
                      AND CMTTE_TYPE IN ('C', 'P')
                ) c
                JOIN cp_stats s ON c.FILER_ID = s.FILER_ID
                -- Require filer to be in the official registry (FILERNAME_CD).
                -- Multi-candidate PACs that sporadically file as C/P are absent
                -- from filername; legitimate candidate committees are registered.
                JOIN filername fn ON c.FILER_ID = fn.FILER_ID
                LEFT JOIN filer_types ft ON c.FILER_ID = ft.FILER_ID
                WHERE c.rn = 1
                  -- Require genuine candidate committee: ≥3 C/P filings AND
                  -- at least 0.5% of all filings are C/P.  This keeps real
                  -- candidate committees (which range from 1%–100%) while
                  -- rejecting large PACs that have a handful of mis-typed C/P
                  -- rows (e.g. filer 810163: 3/1896 = 0.16%).
                  AND s.cp_count >= 2
                  AND s.cp_count * 1.0 / s.total_count >= 0.005
                ORDER BY c.FILER_ID
            ) TO '{cand_path}' (HEADER, DELIMITER ',')
        """)
        n_cands = ref_con.execute(
            f"SELECT COUNT(*) FROM read_csv('{cand_path}', all_varchar=true)"
        ).fetchone()[0]
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv", id_model="committee")
        print(f"{n_cands:,} candidates")

        # ── Committees from FILERNAME_CD ──────────────────────────────────────
        print("  committees...", end=" ", flush=True)
        cmte_path = out("committees.csv")
        ref_con.execute(f"""
            COPY (
                SELECT
                    'CA'                          AS state,
                    fn.FILER_ID                   AS state_filer_id,
                    fn.committee_name             AS committee_name,
                    fn.filer_type                 AS committee_type,
                    COALESCE(cd.cand_name, '')    AS candidate_name,
                    ''                            AS treasurer_name,
                    fn.city                       AS city,
                    fn.zip                        AS zip,
                    COALESCE(ft.active_ft, fn.active_fn) AS active
                FROM filername fn
                LEFT JOIN filer_types ft ON fn.FILER_ID = ft.FILER_ID
                LEFT JOIN (
                    SELECT FILER_ID, cand_name
                    FROM (
                        SELECT FILER_ID, cand_name,
                               ROW_NUMBER() OVER (
                                   PARTITION BY FILER_ID
                                   ORDER BY TRY_CAST(FILING_ID AS BIGINT) DESC NULLS LAST
                               ) AS rn
                        FROM cvr_dedup
                        WHERE cand_name != ''
                          AND CMTTE_TYPE IN ('C', 'P')
                    ) sub
                    WHERE rn = 1
                ) cd ON fn.FILER_ID = cd.FILER_ID
                ORDER BY fn.FILER_ID
            ) TO '{cmte_path}' (HEADER, DELIMITER ',')
        """)
        n_cmtes = ref_con.execute(
            f"SELECT COUNT(*) FROM read_csv('{cmte_path}', all_varchar=true)"
        ).fetchone()[0]
        print(f"{n_cmtes:,} committees")

        ref_con.close()
        print(f"  Reference tables saved → {REF_DB}")

        # Also seed california.db with candidates + committees so tabulate.py
        # doesn't need to regenerate them from CSV.
        print("  Seeding california.db with candidates + committees...", end=" ", flush=True)
        main_con = _open_main_db()
        main_con.execute(f"""
            CREATE OR REPLACE TABLE candidates AS
            SELECT * FROM read_csv('{cand_path}', all_varchar=true, null_padding=true)
        """)
        main_con.execute(f"""
            CREATE OR REPLACE TABLE committees AS
            SELECT * FROM read_csv('{cmte_path}', all_varchar=true, null_padding=true)
        """)
        main_con.close()
        _retire_wal()
        print("done")

        if not run_all:
            return

    # For stages 2-4: open california.db (file-based, not in-memory) and
    # attach the ref DB so we can join CVR + FILERNAME without re-scanning TSVs.
    def open_main_with_ref() -> duckdb.DuckDBPyConnection:
        """Open the california.db file and attach ref DB read-only."""
        con = _open_main_db()
        con.execute(f"ATTACH '{REF_DB}' AS ref (READ_ONLY)")
        return con

    # ── Stage 2: Contributions ────────────────────────────────────────────────
    # Writes directly to california.db (DuckDB native format — much faster than
    # writing a multi-GB CSV to the mounted filesystem).
    if run_all or stage == 2:
        con = open_main_with_ref()
        print(f"  contributions  RCPT_CD.tsv...", end=" ", flush=True)
        con.execute(f"""
            CREATE OR REPLACE TABLE contributions AS
            SELECT
                'CA'                                                       AS state,
                COALESCE(fn.committee_name, '')                            AS committee_name,
                {build_name('r.CTRIB_NAML', 'r.CTRIB_NAMF')}             AS contributor_name,
                TRY_CAST(TRIM(r.AMOUNT) AS DOUBLE)                        AS amount,
                {parse_date('r.RCPT_DATE')}                               AS date,
                COALESCE(NULLIF(TRIM(r.TRAN_TYPE), ''), '')                AS transaction_type,
                COALESCE(NULLIF(TRIM(r.ENTITY_CD), ''), '')                AS contributor_type,
                COALESCE(NULLIF(TRIM(r.CTRIB_CITY), ''), '')               AS contributor_city,
                COALESCE(NULLIF(TRIM(r.CTRIB_ST),   ''), '')               AS contributor_state,
                COALESCE(NULLIF(TRIM(r.CTRIB_ZIP4), ''), '')               AS contributor_zip,
                COALESCE(NULLIF(TRIM(r.CTRIB_EMP),  ''), '')               AS employer,
                COALESCE(NULLIF(TRIM(r.CTRIB_OCC),  ''), '')               AS occupation,
                {txn_cand('r.CAND_NAML', 'r.CAND_NAMF', 'c.cand_name')}  AS candidate_name,
                COALESCE(NULLIF(TRIM(r.OFFICE_CD), ''), NULLIF(TRIM(c.OFFICE_CD), ''), '') AS office,
                {election_yr('c.ELECT_DATE')}                              AS election_year,
                COALESCE(NULLIF(TRIM(r.TRAN_ID), ''), '')                  AS filing_id,
                ''                                                         AS amended,
                'RCPT_CD.tsv'                                              AS raw_file,
                0                                                          AS row_num
            FROM read_csv('{raw("RCPT_CD.tsv")}', {ROPT}) r
            JOIN ref.cvr_dedup c
                ON  TRIM(r.FILING_ID) = TRIM(c.FILING_ID)
                AND TRY_CAST(r.AMEND_ID AS INT) = TRY_CAST(c.AMEND_ID AS INT)
            LEFT JOIN ref.filername fn
                ON {resolve_fid('r.CMTE_ID', 'c.FILER_ID')} = fn.FILER_ID
            WHERE TRY_CAST(r.AMOUNT AS DOUBLE) IS NOT NULL
              AND TRIM(r.AMOUNT) != ''
        """)
        n_cont = con.execute("SELECT COUNT(*) FROM contributions").fetchone()[0]
        print(f"{n_cont:,} rows")
        con.execute(f"""
            COPY contributions TO '{out("contributions.csv.gz")}'
            (HEADER, DELIMITER ',', COMPRESSION gzip)
        """)
        con.close()
        _retire_wal()

        if not run_all:
            return

    # ── Stage 3: Expenditures ─────────────────────────────────────────────────
    if run_all or stage == 3:
        con = open_main_with_ref()
        print(f"  expenditures   EXPN_CD.tsv...", end=" ", flush=True)
        con.execute(f"""
            CREATE OR REPLACE TABLE expenditures AS
            SELECT
                'CA'                                                       AS state,
                COALESCE(fn.committee_name, '')                            AS committee_name,
                {build_name('r.PAYEE_NAML', 'r.PAYEE_NAMF')}             AS payee_name,
                TRY_CAST(TRIM(r.AMOUNT) AS DOUBLE)                        AS amount,
                {parse_date('r.EXPN_DATE')}                               AS date,
                COALESCE(NULLIF(TRIM(r.EXPN_CODE), ''), '')                AS transaction_type,
                COALESCE(NULLIF(TRIM(r.EXPN_DSCR), ''), '')                AS purpose,
                COALESCE(NULLIF(TRIM(r.FORM_TYPE), ''), '')                AS category,
                COALESCE(NULLIF(TRIM(r.PAYEE_CITY), ''), '')               AS payee_city,
                COALESCE(NULLIF(TRIM(r.PAYEE_ST),   ''), '')               AS payee_state,
                COALESCE(NULLIF(TRIM(r.PAYEE_ZIP4), ''), '')               AS payee_zip,
                {txn_cand('r.CAND_NAML', 'r.CAND_NAMF', 'c.cand_name')}  AS candidate_name,
                COALESCE(NULLIF(TRIM(r.OFFICE_CD), ''), NULLIF(TRIM(c.OFFICE_CD), ''), '') AS office,
                {election_yr('c.ELECT_DATE')}                              AS election_year,
                COALESCE(NULLIF(TRIM(r.TRAN_ID), ''), '')                  AS filing_id,
                ''                                                         AS amended,
                'EXPN_CD.tsv'                                              AS raw_file,
                0                                                          AS row_num
            FROM read_csv('{raw("EXPN_CD.tsv")}', {ROPT}) r
            JOIN ref.cvr_dedup c
                ON  TRIM(r.FILING_ID) = TRIM(c.FILING_ID)
                AND TRY_CAST(r.AMEND_ID AS INT) = TRY_CAST(c.AMEND_ID AS INT)
            LEFT JOIN ref.filername fn
                ON {resolve_fid('r.CMTE_ID', 'c.FILER_ID')} = fn.FILER_ID
            WHERE TRY_CAST(r.AMOUNT AS DOUBLE) IS NOT NULL
              AND TRIM(r.AMOUNT) != ''
        """)
        n_expn = con.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
        print(f"{n_expn:,} rows")
        con.execute(f"""
            COPY expenditures TO '{out("expenditures.csv.gz")}'
            (HEADER, DELIMITER ',', COMPRESSION gzip)
        """)
        con.close()
        _retire_wal()

        if not run_all:
            return

        _retire_wal()
        Path(REF_DB).unlink(missing_ok=True)

    print(f"\nCalifornia: stage {stage or 'all'} done.")
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Parse California CAL-ACCESS TSVs into cleaned CSVs.",
        epilog=(
            "Run in four stages to avoid timeout on large files:\n"
            "  python3 california.py --stage 1   # ref tables + candidates + committees\n"
            "  python3 california.py --stage 2   # contributions  (RCPT_CD.tsv)\n"
            "  python3 california.py --stage 3   # expenditures   (EXPN_CD.tsv)\n"
            "  python3 california.py --stage 4   # loans + debts  (LOAN_CD + DEBT_CD)\n"
            "Or run all at once (may timeout for large installs):\n"
            "  python3 california.py             # all stages"
        ),
    )
    ap.add_argument(
        "--stage", type=int, choices=[1, 2, 3, 4],
        help="Which stage to run (1=ref+cands+cmtes, 2=contribs, 3=expns, 4=loans). "
             "Omit to run all stages sequentially.",
    )
    args = ap.parse_args()
    run(stage=args.stage)
