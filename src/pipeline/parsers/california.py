"""
parsers/california.py — Transform California CAL-ACCESS raw TSVs into the
5 normalized relations.

Input:  data/California/raw/
  CVR_CAMPAIGN_DISCLOSURE_CD.tsv  — cover records (one per filing/amendment)
  FILERNAME_CD.tsv                — filer name/address registry
  FILER_TO_FILER_TYPE_CD.tsv      — filer type codes, party, active flag
  RCPT_CD.tsv                     — contributions received  (~19 M rows)
  EXPN_CD.tsv                     — expenditures made       (~15 M rows)

Output: data/California/cleaned/
  contributions.csv, expenditures.csv, committees.csv,
  candidates.csv

Implementation
==============
  Uses DuckDB for all heavy file I/O so that multi-GB TSVs (RCPT_CD 3.5 GB,
  EXPN_CD 2.8 GB) are processed in seconds rather than minutes.  Python is
  used only for the small reference-table work and for utils.assign_person_ids.

Amendment dedup
===============
  CAL-ACCESS stores every amendment as a separate set of rows sharing the
  same FILING_ID but with increasing AMEND_ID.  We pre-build a cvr_dedup
  table that keeps only the max-AMEND_ID row per FILING_ID; joining the
  transaction tables on (FILING_ID, AMEND_ID) automatically retains only
  the most-recent version of each contribution/expenditure.

Encoding
========
  All TSVs are latin-1 (ISO-8859-1).  DuckDB's read_csv handles this via
  the encoding parameter.

Amount format
=============
  Already plain numeric strings ('109.89', '2000') — no $ or commas.
"""

import sys
import time
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import utils

RAW_DIR  = PROJECT_ROOT / "data" / "California" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "California" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "CA"


def raw(name: str) -> str:
    return str(RAW_DIR / name).replace("'", "''")


def out(name: str) -> str:
    return str(CLEAN_DIR / name).replace("'", "''")


# == DuckDB read_csv options =================================================
# all_varchar avoids type-inference surprises; ignore_errors skips NUL-byte
# rows and other malformed lines; null_padding handles short rows.
ROPT = "sep='\\t', all_varchar=true, ignore_errors=true, null_padding=true, strict_mode=false"


# == SQL expression helpers ===================================================
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


# == State_filer_id resolution for transaction rows ===========================
# Prefer CMTE_ID from the transaction row; fall back to FILER_ID from CVR.
def resolve_fid(cmte_col: str, cvr_fid: str) -> str:
    return f"COALESCE(NULLIF(TRIM({cmte_col}), ''), {cvr_fid}, '')"


# == Candidate name for a transaction row ====================================
# Use row-level CAND_NAML/NAMF if present, else fall back to CVR cand_name.
def txn_cand(row_last: str, row_first: str, cvr_cand: str) -> str:
    row = build_cand(row_last, row_first)
    return f"CASE WHEN ({row}) != '' THEN ({row}) ELSE COALESCE({cvr_cand}, '') END"


# Path for the persistent reference-table DB (written by stage 1, read by 2-3)
REF_DB  = str(CLEAN_DIR / "_ca_ref.db")
# Final output DB — stages 2-3 write large tables directly here (skip CSV)
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


def _build_ref_tables(con: duckdb.DuckDBPyConnection, log=None) -> None:
    """Load CVR, FILERNAME, and FILER_TYPES into `con` as DuckDB tables."""
    if log:
        log.info("  Loading CVR (max-amend dedup)...")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS cvr_dedup AS
        SELECT
            FILING_ID, FILER_ID, AMEND_ID,
            ELECT_DATE, THRU_DATE, OFFICE_CD, CAND_NAML, CAND_NAMF,
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
    if log:
        log.info(f"  CVR: {n:,} filings")

    if log:
        log.info("  Loading FILERNAME_CD...")
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
    if log:
        log.info(f"  FILERNAME: {n:,} filers")

    if log:
        log.info("  Building filername_xref (renamed filers)...")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS filername_xref AS
        SELECT TRIM(XREF_FILER_ID) AS xref_id,
               {build_name('NAML', 'NAMF')} AS committee_name
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY TRIM(XREF_FILER_ID)
                       ORDER BY
                           CASE WHEN UPPER(TRIM(STATUS)) = 'ACTIVE' THEN 0 ELSE 1 END,
                           FILER_ID DESC NULLS LAST
                   ) AS rn
            FROM read_csv('{raw("FILERNAME_CD.tsv")}', {ROPT})
            WHERE NULLIF(TRIM(XREF_FILER_ID), '') IS NOT NULL
              AND TRIM(XREF_FILER_ID) != TRIM(FILER_ID)
        ) sub
        WHERE rn = 1
    """)
    n = con.execute("SELECT COUNT(*) FROM filername_xref").fetchone()[0]
    if log:
        log.info(f"  filername_xref: {n:,} xref entries")

    if log:
        log.info("  Loading FILER_TO_FILER_TYPE_CD...")
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
    if log:
        log.info(f"  FILER_TO_FILER_TYPE: {n:,} entries")


def run(entities: bool = False, contributions: bool = False, expenditures: bool = False,
        transactions: bool = False):
    """
    No flags → run all three stages in sequence.

        --entities       → stage 1: ref tables + candidates + committees
        --contributions  → stage 2: contributions (RCPT_CD.tsv)
        --expenditures   → stage 3: expenditures  (EXPN_CD.tsv)
        --transactions   → stages 2 + 3
    """
    log = get_logger("california", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    if transactions:
        contributions = expenditures = True

    if entities or contributions or expenditures:
        _stages = set()
        if entities:      _stages.add(1)
        if contributions: _stages.add(2)
        if expenditures:  _stages.add(3)
        run_all = False
    else:
        _stages = {1, 2, 3}
        run_all = True
    n_cont     = 0
    n_expn     = 0
    n_cands    = 0
    n_cmtes    = 0

    def _bytes(name: str) -> int:
        p = CLEAN_DIR / name
        return p.stat().st_size if p.exists() else 0

    try:
        # == Stage 1: reference tables + candidates + committees ===============
        if run_all or 1 in _stages:
            ref_con = duckdb.connect(REF_DB)
            _build_ref_tables(ref_con, log=log)

            # == Candidates from CVR ===========================================
            log.info("  candidates...")
            t1 = time.perf_counter()
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
            log.info(f"  candidates: {n_cands:,}")
            log.file_parsed("CVR_CAMPAIGN_DISCLOSURE_CD.tsv", "candidates", n_cands,
                            duration_s=round(time.perf_counter() - t1, 1),
                            bytes=(RAW_DIR / "CVR_CAMPAIGN_DISCLOSURE_CD.tsv").stat().st_size)

            # == Committees from FILERNAME_CD ==================================
            log.info("  committees...")
            t1 = time.perf_counter()
            cmte_path = out("committees.csv")
            ref_con.execute(f"""
                COPY (
                    SELECT
                        'CA'                          AS state,
                        fn.FILER_ID                   AS state_filer_id,
                        fn.committee_name             AS committee_name,
                        -- Candidate committees register as "RECIPIENT COMMITTEE" in
                        -- CAL-ACCESS (same as PACs). Use the presence of a C/P cover
                        -- page (cd.cand_name populated) to reclassify them correctly.
                        CASE WHEN cd.cand_name IS NOT NULL AND cd.cand_name != ''
                             THEN 'Candidate Committee'
                             ELSE fn.filer_type END             AS committee_type,
                        -- Only candidate committees have a meaningful election cycle
                        CASE WHEN cd.cand_name IS NOT NULL AND cd.cand_name != ''
                             THEN ey.election_year ELSE NULL END AS election_year,
                        COALESCE(cd.cand_name, '')    AS candidate_name,
                        ''                            AS treasurer_name,
                        fn.city                       AS city,
                        fn.zip                        AS zip,
                        COALESCE(ft.active_ft, fn.active_fn) AS active
                    FROM filername fn
                    LEFT JOIN filer_types ft ON fn.FILER_ID = ft.FILER_ID
                    LEFT JOIN (
                        SELECT FILER_ID, {election_yr('ELECT_DATE')} AS election_year
                        FROM (
                            SELECT FILER_ID, ELECT_DATE,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY FILER_ID
                                       ORDER BY TRY_CAST(FILING_ID AS BIGINT) DESC NULLS LAST
                                   ) AS rn
                            FROM cvr_dedup
                            -- Only candidate committee types have a meaningful election cycle
                            WHERE NULLIF(TRIM(ELECT_DATE), '') IS NOT NULL
                              AND CMTTE_TYPE IN ('C', 'P')
                        ) sub
                        WHERE rn = 1
                    ) ey ON fn.FILER_ID = ey.FILER_ID
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
            log.info(f"  committees: {n_cmtes:,}")
            log.file_parsed("FILERNAME_CD.tsv", "committees", n_cmtes,
                            duration_s=round(time.perf_counter() - t1, 1),
                            bytes=(RAW_DIR / "FILERNAME_CD.tsv").stat().st_size)

            ref_con.close()
            log.debug(f"  Reference tables saved → {REF_DB}")

            # Seed california.db with candidates + committees so tabulate.py
            # doesn't need to regenerate them from CSV.
            log.info("  Seeding california.db with candidates + committees...")
            main_con = _open_main_db()
            main_con.execute(f"""
                CREATE OR REPLACE TABLE candidates AS
                SELECT * FROM read_csv('{cand_path}', all_varchar=true, null_padding=true)
            """)
            main_con.execute(f"""
                CREATE OR REPLACE TABLE committees AS
                SELECT * FROM read_csv('{cmte_path}', all_varchar=true, null_padding=true)
            """)

            # assign_committee_person_ids links committee rows to candidate person_ids
            # by matching committee.candidate_name → candidate.candidate_name
            utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv",
                                              CLEAN_DIR / "candidates.csv")

            main_con.close()
            _retire_wal()
            log.info("  Seeded california.db")

            if not run_all:
                _emit_completed(log, t0, n_cont, n_expn, n_cands, n_cmtes)
                return

        # For stages 2-3: open california.db and attach ref DB read-only so
        # we can join CVR + FILERNAME without re-scanning the raw TSVs.
        def open_main_with_ref() -> duckdb.DuckDBPyConnection:
            """Open the california.db file and attach ref DB read-only."""
            con = _open_main_db()
            con.execute(f"ATTACH '{REF_DB}' AS ref (READ_ONLY)")
            return con

        # == Stage 2: Contributions ============================================
        # Writes directly to california.db (DuckDB native format — much faster than
        # writing a multi-GB CSV to the mounted filesystem).
        if run_all or 2 in _stages:
            con = open_main_with_ref()
            log.info("  contributions  RCPT_CD.tsv...")
            t2 = time.perf_counter()
            con.execute(f"""
                CREATE OR REPLACE TABLE contributions AS
                SELECT
                    'CA'                                                       AS state,
                    COALESCE(fn.committee_name, fn_x.committee_name, '')       AS committee_name,
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
                    ROW_NUMBER() OVER ()                                       AS row_num
                FROM read_csv('{raw("RCPT_CD.tsv")}', {ROPT}) r
                JOIN ref.cvr_dedup c
                    ON  TRIM(r.FILING_ID) = TRIM(c.FILING_ID)
                    AND TRY_CAST(r.AMEND_ID AS INT) = TRY_CAST(c.AMEND_ID AS INT)
                -- Join both tables independently; COALESCE in SELECT picks the right name.
                -- Keeping these as independent joins lets DuckDB parallelize the hash builds.
                LEFT JOIN ref.filername fn  ON c.FILER_ID = fn.FILER_ID
                LEFT JOIN ref.filername_xref fn_x ON c.FILER_ID = fn_x.xref_id
                WHERE TRY_CAST(r.AMOUNT AS DOUBLE) IS NOT NULL
                  AND TRIM(r.AMOUNT) != ''
            """)
            n_cont = con.execute("SELECT COUNT(*) FROM contributions").fetchone()[0]
            log.info(f"  contributions: {n_cont:,} rows")
            con.execute(f"""
                COPY contributions TO '{out("contributions.csv.gz")}'
                (HEADER, DELIMITER ',', COMPRESSION gzip)
            """)
            con.close()
            _retire_wal()
            log.file_parsed("RCPT_CD.tsv", "contributions", n_cont,
                            duration_s=round(time.perf_counter() - t2, 1),
                            bytes=(RAW_DIR / "RCPT_CD.tsv").stat().st_size)

            if not run_all:
                _emit_completed(log, t0, n_cont, n_expn, n_cands, n_cmtes)
                return

        # == Stage 3: Expenditures =============================================
        if run_all or 3 in _stages:
            con = open_main_with_ref()
            log.info("  expenditures   EXPN_CD.tsv...")
            t3 = time.perf_counter()
            con.execute(f"""
                CREATE OR REPLACE TABLE expenditures AS
                SELECT
                    'CA'                                                       AS state,
                    COALESCE(fn.committee_name, fn_x.committee_name, '')       AS committee_name,
                    {build_name('r.PAYEE_NAML', 'r.PAYEE_NAMF')}             AS payee_name,
                    TRY_CAST(TRIM(r.AMOUNT) AS DOUBLE)                        AS amount,
                    -- Fall back to CVR THRU_DATE (end of reporting period) when EXPN_DATE is null.
                    -- ~35% of older Form-E rows lack EXPN_DATE in the raw data; THRU_DATE gives
                    -- a bounding date that keeps these rows usable for time-series analysis.
                    COALESCE({parse_date('r.EXPN_DATE')}, {parse_date('c.THRU_DATE')}) AS date,
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
                    ROW_NUMBER() OVER ()                                       AS row_num
                FROM read_csv('{raw("EXPN_CD.tsv")}', {ROPT}) r
                JOIN ref.cvr_dedup c
                    ON  TRIM(r.FILING_ID) = TRIM(c.FILING_ID)
                    AND TRY_CAST(r.AMEND_ID AS INT) = TRY_CAST(c.AMEND_ID AS INT)
                -- Join both tables independently; COALESCE in SELECT picks the right name.
                -- Keeping these as independent joins lets DuckDB parallelize the hash builds.
                LEFT JOIN ref.filername fn  ON c.FILER_ID = fn.FILER_ID
                LEFT JOIN ref.filername_xref fn_x ON c.FILER_ID = fn_x.xref_id
                WHERE TRY_CAST(r.AMOUNT AS DOUBLE) IS NOT NULL
                  AND TRIM(r.AMOUNT) != ''
            """)
            n_expn = con.execute("SELECT COUNT(*) FROM expenditures").fetchone()[0]
            log.info(f"  expenditures: {n_expn:,} rows")
            con.execute(f"""
                COPY expenditures TO '{out("expenditures.csv.gz")}'
                (HEADER, DELIMITER ',', COMPRESSION gzip)
            """)
            con.close()
            _retire_wal()
            log.file_parsed("EXPN_CD.tsv", "expenditures", n_expn,
                            duration_s=round(time.perf_counter() - t3, 1),
                            bytes=(RAW_DIR / "EXPN_CD.tsv").stat().st_size)

            if not run_all:
                _emit_completed(log, t0, n_cont, n_expn, n_cands, n_cmtes)
                return

        # Clean up the ref DB after a full run (it can be rebuilt from raw TSVs)
        Path(REF_DB).unlink(missing_ok=True)
        log.debug("  Cleaned up ref DB")

        # Log output file stats
        log.file_parsed("contributions.csv.gz", "contributions", n_cont,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  n_expn,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("committees.csv",        "committees",    n_cmtes,
                        role="output", bytes=_bytes("committees.csv"))
        log.file_parsed("candidates.csv",        "candidates",    n_cands,
                        role="output", bytes=_bytes("candidates.csv"))

        scope = "+".join(
            ([" entities"] if 1 in _stages else []) +
            (["contributions"] if 2 in _stages else []) +
            (["expenditures"] if 3 in _stages else [])
        ) or "all"
        log.info(f"California: {scope} done.")
        _emit_completed(log, t0, n_cont, n_expn, n_cands, n_cmtes)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=n_cont, expenditures=n_expn,
                  committees=n_cmtes, candidates=n_cands)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=n_cont, expenditures=n_expn,
                  committees=n_cmtes, candidates=n_cands,
                  error_type=type(e).__name__, error=str(e))
        raise


def _emit_completed(log, t0, n_cont, n_expn, n_cands, n_cmtes):
    """Emit parse_completed event. Extracted to avoid repetition across early-return paths."""
    log._emit("parse_completed", status="completed",
              duration_s=round(time.perf_counter() - t0, 1),
              contributions=n_cont, expenditures=n_expn,
              committees=n_cmtes, candidates=n_cands)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Parse California CAL-ACCESS TSVs into cleaned CSVs."
    )
    ap.add_argument("--entities",      action="store_true",
                    help="entities only (ref tables + candidates + committees)")
    ap.add_argument("--transactions",  action="store_true",
                    help="contributions + expenditures")
    ap.add_argument("--contributions", action="store_true",
                    help="contributions only (RCPT_CD.tsv)")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditures only (EXPN_CD.tsv)")
    args, _ = ap.parse_known_args()
    try:
        run(entities=args.entities, contributions=args.contributions,
            expenditures=args.expenditures, transactions=args.transactions)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
