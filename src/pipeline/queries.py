"""
src/pipeline/queries.py — Exploratory spot-check queries against a state's .db file.

Manual QA tool, not an automated test — run by hand after a sync/reparse to
eyeball whether top contributors/recipients/amounts look real for the state.
Output is also captured by orc.py into metadata/{state}_queries.txt.

Usage:
    python3 src/pipeline/queries.py arizona
    python3 src/pipeline/queries.py alabama
"""

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parents[0]))
from utils import find_clean_dir

W_NAME  = 42
W_TXNS  =  7
W_MONEY = 14


def find_db(state: str) -> Path:
    if state.lower() == "all":
        db_path = PROJECT_ROOT / "data" / "state-level-cf.db"
        if not db_path.exists():
            print("[!] state-level-cf.db not found. Run aggregate.py first.")
            sys.exit(1)
        return db_path
    clean_dir = find_clean_dir(state)
    if clean_dir is None:
        print(f"[!] No data directory found for '{state}'")
        sys.exit(1)
    # Name the db off the matched directory, not the argument — tabulate.py
    # builds it as f"{state_dir.name.lower()}.db", so "new_mexico" would
    # otherwise look for new_mexico.db next to the "new mexico.db" that exists.
    db_path = clean_dir / f"{clean_dir.parent.name.lower()}.db"
    if not db_path.exists():
        print(f"[!] No .db file at {db_path}. Run tabulate.py first.")
        sys.exit(1)
    return db_path


def trunc(s, n):
    s = str(s or "")
    return s[:n-1] + "…" if len(s) > n else s


def fmt_money(val):
    if val is None:
        return f"{'—':>{W_MONEY}}"
    return f"${val:>{W_MONEY-1},.0f}"


def section(title, state):
    line = "=" * 80
    print(f"\n{line}")
    print(f"  {title}  [{state.upper()}]")
    print(line)


def _cell_text(value, width):
    """Left-aligned truncated text; trunc() already blanks a missing value."""
    return trunc(value, width)


def _cell_text_or_dash(value, width):
    """Left-aligned truncated text; em-dash stands in for a missing value."""
    return trunc(value or "—", width)


def _cell_int(value, width):
    """Comma-grouped integer."""
    return f"{value:,}"


def _cell_money(value, width):
    """Dollar amount via fmt_money (fmt_money owns its own fixed width)."""
    return fmt_money(value)


def _cell_pct(value, width):
    """Percentage, padded to width before the trailing '%' is appended."""
    return f"{value:>{width}.1f}%"


def _cell_raw(value, width):
    """Plain string; blank for a missing value (dates, years)."""
    return str(value or "")


def print_table(columns, rows):
    """Print a header line, a '-' separator, and one line per row.

    columns: [(label, width, align, formatter)], where align is '<' or '>'
    and formatter(value, width) returns the cell's display string.
    """
    header = "  ".join(f"{label:{align}{width}}" for label, width, align, _ in columns)
    sep    = "  ".join("-" * width for _, width, _, _ in columns)
    print(f"  {header}")
    print(f"  {sep}")
    for row in rows:
        cells = "  ".join(
            f"{fmt(value, width):{align}{width}}"
            for value, (_, width, align, fmt) in zip(row, columns)
        )
        print(f"  {cells}")


def _state_checks(state: str, con) -> list[tuple[str, bool, str]]:
    """Return [(label, passed, detail)] for state-specific integrity checks.

    No state currently defines any. (A former FL-only check asserting three
    specific candidate names existed was removed 2026-08 -- it had gone stale
    as the underlying data window rolled to 2026-only records and was failing
    unconditionally. If a future state needs a check, a small per-state
    dict/registry is the natural next step; not worth building for a single
    hypothetical case.)"""
    return []


def run(state: str):
    db_path = find_db(state)
    con     = duckdb.connect(str(db_path), read_only=True)

    # ── 1. Top 20 contributors ────────────────────────────────────────────────
    section("TOP 20 CONTRIBUTORS — total donated & top recipient committee", state)
    rows = con.execute("""
        WITH donor_totals AS (
            SELECT contributor_name, SUM(TRY_CAST(amount AS DOUBLE)) AS total, COUNT(*) AS n
            FROM contributions
            WHERE contributor_name IS NOT NULL AND contributor_name != ''
              AND TRY_CAST(amount AS DOUBLE) IS NOT NULL
            GROUP BY contributor_name
        ),
        top_cmte AS (
            SELECT contributor_name, committee_name, SUM(TRY_CAST(amount AS DOUBLE)) AS to_cmte,
                   ROW_NUMBER() OVER (PARTITION BY contributor_name ORDER BY SUM(TRY_CAST(amount AS DOUBLE)) DESC) AS rn
            FROM contributions
            WHERE contributor_name IS NOT NULL AND contributor_name != ''
              AND TRY_CAST(amount AS DOUBLE) IS NOT NULL AND committee_name IS NOT NULL AND committee_name != ''
            GROUP BY contributor_name, committee_name
        )
        SELECT d.contributor_name, d.n, ROUND(d.total,0), t.committee_name, ROUND(t.to_cmte,0)
        FROM donor_totals d
        LEFT JOIN top_cmte t ON d.contributor_name = t.contributor_name AND t.rn = 1
        ORDER BY d.total DESC LIMIT 20
    """).fetchall()

    print_table([
        ("Contributor",   W_NAME,  "<", _cell_text),
        ("Txns",          W_TXNS,  ">", _cell_int),
        ("Total Donated", W_MONEY, ">", _cell_money),
        ("Top Recipient", W_NAME,  "<", _cell_text_or_dash),
        ("To Top",        W_MONEY, ">", _cell_money),
    ], rows)

    # ── 2. Top 20 recipient candidates ───────────────────────────────────────
    # Join via candidate_name (contributions no longer carries state_filer_id).
    section("TOP 20 RECIPIENT CANDIDATES — total contributions received", state)
    rows = con.execute("""
        WITH dedup_candidates AS (
            -- One row per (state, candidate_name): person_id is per-office in
            -- the "committee" model (AZ, AL, CA), so grouping by person_id would
            -- fan-out for candidates who ran for multiple offices.  Deduping by
            -- name instead ensures each candidate_name matches exactly once.
            -- Pick the most-recent election_year so the displayed office is current.
            SELECT DISTINCT ON (state, LOWER(TRIM(candidate_name)))
                person_id, candidate_name, state, office, party
            FROM candidates
            ORDER BY state, LOWER(TRIM(candidate_name)), election_year DESC NULLS LAST
        )
        SELECT ca.candidate_name, ca.state, ca.office, ca.party,
               COUNT(*) AS n,
               ROUND(SUM(TRY_CAST(co.amount AS DOUBLE)), 0) AS total
        FROM dedup_candidates ca
        JOIN contributions co
            ON ca.state = co.state
           AND co.candidate_name IS NOT NULL AND co.candidate_name != ''
           AND LOWER(TRIM(ca.candidate_name)) = LOWER(TRIM(co.candidate_name))
        WHERE TRY_CAST(co.amount AS DOUBLE) IS NOT NULL
        GROUP BY ca.candidate_name, ca.state, ca.office, ca.party
        ORDER BY total DESC LIMIT 20
    """).fetchall()

    print_table([
        ("Candidate",      W_NAME,  "<", _cell_text),
        ("St",             4,       "<", _cell_text),
        ("Office",         26,      "<", _cell_text),
        ("Party",          12,      "<", _cell_text),
        ("Txns",           W_TXNS,  ">", _cell_int),
        ("Total Received", W_MONEY, ">", _cell_money),
    ], rows)

    # ── 3. Top 20 non-candidate committees ───────────────────────────────────
    # Excludes committees whose name matches a known candidate_name, AND
    # (added — see PA "Shapiro for Pennsylvania" double-counting fix,
    # 2026-07-12) committees that utils.assign_committee_person_ids has
    # already linked to a candidate via person_id. The name-match check
    # alone only catches committees that are *literally named* the same
    # as their candidate's own registration (e.g. a self-referential
    # "TOM CORBETT FOR GOVERNOR" candidate_name with no separate
    # candidate row to differ from) — it misses committees correctly
    # linked to a *differently-named* candidate row (e.g. "Shapiro for
    # Pennsylvania" linked to candidate "SHAPIRO, JOSHUA D"), which
    # were showing up in both this table and "Recipient Candidates"
    # simultaneously, double-counting the same dollars. person_id is a
    # stronger, pre-existing, cross-state signal for "this committee IS
    # some candidate's own committee" and only ever removes rows here
    # (a committee with no real candidate link never gets a person_id),
    # so this doesn't change behavior for genuinely independent PACs.
    section("TOP 20 NON-CANDIDATE COMMITTEES — total contributions received", state)
    rows = con.execute("""
        WITH cmte_types AS (
            -- One row per committee_name: prefer a non-blank type, take MAX alphabetically
            SELECT LOWER(TRIM(committee_name)) AS name_key,
                   MAX(CASE WHEN committee_type IS NOT NULL AND committee_type != ''
                            THEN committee_type END) AS committee_type,
                   MAX(CASE WHEN person_id IS NOT NULL
                            THEN 1 ELSE 0 END) AS linked_to_candidate
            FROM committees
            GROUP BY LOWER(TRIM(committee_name))
        )
        SELECT co.state, co.committee_name, ct.committee_type,
               COUNT(*) AS n,
               ROUND(SUM(TRY_CAST(co.amount AS DOUBLE)), 0) AS total
        FROM contributions co
        LEFT JOIN cmte_types ct ON LOWER(TRIM(co.committee_name)) = ct.name_key
        WHERE TRY_CAST(co.amount AS DOUBLE) IS NOT NULL
          AND co.committee_name IS NOT NULL AND co.committee_name != ''
          AND (ct.committee_type IS NULL OR ct.committee_type NOT ILIKE 'Candidate%')
          AND (ct.linked_to_candidate IS NULL OR ct.linked_to_candidate = 0)
          AND NOT EXISTS (
              SELECT 1 FROM candidates ca
              WHERE LOWER(TRIM(ca.candidate_name)) = LOWER(TRIM(co.committee_name))
          )
        GROUP BY co.state, co.committee_name, ct.committee_type
        ORDER BY total DESC LIMIT 20
    """).fetchall()

    print_table([
        ("St",             4,       "<", _cell_text),
        ("Committee",      W_NAME,  "<", _cell_text),
        ("Type",           28,      "<", _cell_text),
        ("Txns",           W_TXNS,  ">", _cell_int),
        ("Total Received", W_MONEY, ">", _cell_money),
    ], rows)

    # ── 4. Top 10 expenditure recipients ─────────────────────────────────────
    section("TOP 10 EXPENDITURE RECIPIENTS — total paid & largest client", state)
    rows = con.execute("""
        WITH payee_totals AS (
            SELECT payee_name, SUM(TRY_CAST(amount AS DOUBLE)) AS total, COUNT(*) AS n
            FROM expenditures
            WHERE payee_name IS NOT NULL AND payee_name != ''
              AND TRY_CAST(amount AS DOUBLE) IS NOT NULL
              AND payee_name NOT ILIKE '%unitemized%'
              AND payee_name NOT ILIKE '%not pertaining%'
              AND payee_name NOT ILIKE '%previous disbursements%'
              AND payee_name NOT ILIKE 'itemized principal campaign%'
              AND payee_name NOT ILIKE 'non-itemized principal campaign%'
              AND payee_name NOT ILIKE '%offset%loan%'
            GROUP BY payee_name
        ),
        top_client AS (
            SELECT payee_name, committee_name, SUM(TRY_CAST(amount AS DOUBLE)) AS from_client,
                   ROW_NUMBER() OVER (PARTITION BY payee_name ORDER BY SUM(TRY_CAST(amount AS DOUBLE)) DESC) AS rn
            FROM expenditures
            WHERE payee_name IS NOT NULL AND payee_name != ''
              AND TRY_CAST(amount AS DOUBLE) IS NOT NULL
              AND committee_name IS NOT NULL AND committee_name != ''
              AND payee_name NOT ILIKE '%unitemized%'
              AND payee_name NOT ILIKE '%not pertaining%'
              AND payee_name NOT ILIKE '%previous disbursements%'
              AND payee_name NOT ILIKE 'itemized principal campaign%'
              AND payee_name NOT ILIKE 'non-itemized principal campaign%'
              AND payee_name NOT ILIKE '%offset%loan%'
            GROUP BY payee_name, committee_name
        )
        SELECT p.payee_name, p.n, ROUND(p.total,0), t.committee_name, ROUND(t.from_client,0)
        FROM payee_totals p
        LEFT JOIN top_client t ON p.payee_name = t.payee_name AND t.rn = 1
        ORDER BY p.total DESC LIMIT 10
    """).fetchall()

    print_table([
        ("Payee",          W_NAME,  "<", _cell_text),
        ("Txns",           W_TXNS,  ">", _cell_int),
        ("Total Received", W_MONEY, ">", _cell_money),
        ("Largest Client", W_NAME,  "<", _cell_text_or_dash),
        ("From Client",    W_MONEY, ">", _cell_money),
    ], rows)

    # ── 5. Contributions & expenditures by year ──────────────────────────────
    section("ACTIVITY BY YEAR — contribution and expenditure row counts and totals", state)
    rows = con.execute("""
        WITH cy AS (
            SELECT YEAR(date) AS yr,
                   COUNT(*) AS cont_n,
                   ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 0) AS cont_total
            FROM contributions
            WHERE date IS NOT NULL AND YEAR(date) BETWEEN 1990 AND 2030
            GROUP BY yr
        ),
        ey AS (
            SELECT YEAR(date) AS yr,
                   COUNT(*) AS expn_n,
                   ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 0) AS expn_total
            FROM expenditures
            WHERE date IS NOT NULL AND YEAR(date) BETWEEN 1990 AND 2030
            GROUP BY yr
        )
        SELECT COALESCE(cy.yr, ey.yr) AS year,
               COALESCE(cy.cont_n,     0) AS cont_n,
               COALESCE(cy.cont_total, 0) AS cont_total,
               COALESCE(ey.expn_n,     0) AS expn_n,
               COALESCE(ey.expn_total, 0) AS expn_total
        FROM cy FULL OUTER JOIN ey ON cy.yr = ey.yr
        ORDER BY year
    """).fetchall()

    # W_N and W_T (row-count / dollar-total column widths) carry through
    # blocks 6-8 below unchanged — they're the same two columns repeated
    # for contributor type, contributor state, and expenditure type.
    W_N = 10; W_T = 16
    print_table([
        ("Year",       6,   ">", _cell_raw),
        ("Cont N",     W_N, ">", _cell_int),
        ("Cont Total", W_T, ">", _cell_money),
        ("Expn N",     W_N, ">", _cell_int),
        ("Expn Total", W_T, ">", _cell_money),
    ], rows)

    # ── 6. Contributor type breakdown ─────────────────────────────────────────
    section("CONTRIBUTOR TYPE BREAKDOWN — raw codes, counts, and share of total", state)
    rows = con.execute("""
        SELECT
            CASE WHEN contributor_type IS NULL OR contributor_type = ''
                 THEN '(blank)' ELSE contributor_type END AS contributor_type,
            COUNT(*) AS n,
            ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 0) AS total,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_rows
        FROM contributions
        GROUP BY contributor_type
        ORDER BY n DESC
    """).fetchall()

    # W_CAT (category-label width) and W_P (percent width) also carry
    # through blocks 7-8 — block 8 reuses W_CAT for "Transaction Type",
    # not just contributor type, hence the generic name.
    W_CAT = 30; W_P = 8
    print_table([
        ("Contributor Type", W_CAT, "<", _cell_text),
        ("N",                W_N,   ">", _cell_int),
        ("Total",            W_T,   ">", _cell_money),
        ("% rows",           W_P,   ">", _cell_pct),
    ], rows)

    # ── 7. Top 10 contributor states ──────────────────────────────────────────
    section("TOP 10 CONTRIBUTOR STATES — where the money comes from", state)
    rows = con.execute("""
        SELECT
            CASE WHEN contributor_state IS NULL OR contributor_state = ''
                 THEN '(blank)' ELSE contributor_state END AS contributor_state,
            COUNT(*) AS n,
            ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 0) AS total,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_rows
        FROM contributions
        GROUP BY contributor_state
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()

    print_table([
        ("State",  18,  "<", _cell_text),
        ("N",      W_N, ">", _cell_int),
        ("Total",  W_T, ">", _cell_money),
        ("% rows", W_P, ">", _cell_pct),
    ], rows)

    # ── 8. Expenditure transaction type breakdown ─────────────────────────────
    # Per-state DBs expose transaction_type; the aggregate DB drops it in favour
    # of the normalised transaction_category.  Detect which column is present.
    section("EXPENDITURE TYPE BREAKDOWN — raw codes, counts, and share of total", state)
    expn_cols = {r[1] for r in con.execute("PRAGMA table_info(expenditures)").fetchall()}
    tx_col    = "transaction_type" if "transaction_type" in expn_cols else "transaction_category"
    rows = con.execute(f"""
        SELECT
            CASE WHEN {tx_col} IS NULL OR {tx_col} = ''
                 THEN '(blank)' ELSE {tx_col} END AS tx_type,
            COUNT(*) AS n,
            ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 0) AS total,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_rows
        FROM expenditures
        GROUP BY 1
        ORDER BY n DESC
    """).fetchall()

    print_table([
        ("Transaction Type", W_CAT, "<", _cell_text),
        ("N",                W_N,   ">", _cell_int),
        ("Total",            W_T,   ">", _cell_money),
        ("% rows",           W_P,   ">", _cell_pct),
    ], rows)

    # ── 9. 10 largest single contributions ────────────────────────────────────
    section("10 LARGEST SINGLE CONTRIBUTIONS — outlier and transfer check", state)
    rows = con.execute("""
        SELECT date, contributor_name, contributor_type, committee_name,
               ROUND(TRY_CAST(amount AS DOUBLE), 0) AS amount
        FROM contributions
        WHERE TRY_CAST(amount AS DOUBLE) IS NOT NULL
        ORDER BY TRY_CAST(amount AS DOUBLE) DESC
        LIMIT 10
    """).fetchall()

    print_table([
        ("Date",        12, "<", _cell_raw),
        ("Contributor", 34, "<", _cell_text),
        ("Type",        6,  "<", _cell_text),
        ("Committee",   36, "<", _cell_text),
        ("Amount",      16, ">", _cell_money),
    ], rows)

    # ── 10. 10 random contribution rows ───────────────────────────────────────
    section("10 RANDOM CONTRIBUTION ROWS — raw data spot check", state)
    rows = con.execute("""
        SELECT date, contributor_name, contributor_type,
               ROUND(TRY_CAST(amount AS DOUBLE), 0) AS amount,
               committee_name, contributor_city, contributor_state,
               employer, occupation
        FROM contributions
        USING SAMPLE 10
        ORDER BY date
    """).fetchall()

    print_table([
        ("Date",       12, "<", _cell_raw),
        ("Contributor",28, "<", _cell_text),
        ("T",          5,  "<", _cell_text),
        ("Amount",     12, ">", _cell_money),
        ("Committee",  30, "<", _cell_text),
        ("City",       18, "<", _cell_text),
        ("St",         4,  "<", _cell_text),
        ("Employer",   22, "<", _cell_text),
        ("Occupation", 20, "<", _cell_text),
    ], rows)

    # ── 11. 10 most recent contributions ─────────────────────────────────────
    section("10 MOST RECENT CONTRIBUTIONS — data freshness check", state)
    rows = con.execute("""
        SELECT date, contributor_name, contributor_type,
               ROUND(TRY_CAST(amount AS DOUBLE), 0) AS amount,
               committee_name
        FROM contributions
        WHERE date IS NOT NULL
        ORDER BY date DESC
        LIMIT 10
    """).fetchall()

    print_table([
        ("Date",        12, "<", _cell_raw),
        ("Contributor", 32, "<", _cell_text),
        ("Type",        6,  "<", _cell_text),
        ("Amount",      14, ">", _cell_money),
        ("Committee",   34, "<", _cell_text),
    ], rows)

    # ── 12. 10 most recent expenditures ──────────────────────────────────────
    section("10 MOST RECENT EXPENDITURES — data freshness check", state)
    rows = con.execute(f"""
        SELECT date, payee_name,
               ROUND(TRY_CAST(amount AS DOUBLE), 0) AS amount,
               committee_name,
               {tx_col}
        FROM expenditures
        WHERE date IS NOT NULL
        ORDER BY date DESC
        LIMIT 10
    """).fetchall()

    print_table([
        ("Date",      12, "<", _cell_raw),
        ("Payee",     32, "<", _cell_text),
        ("Amount",    14, ">", _cell_money),
        ("Committee", 28, "<", _cell_text),
        ("Type",      22, "<", _cell_text),
    ], rows)

    # ── State-specific integrity checks ──────────────────────────────────────
    checks = _state_checks(state.upper(), con)
    if checks:
        section("STATE-SPECIFIC INTEGRITY CHECKS", state)
        all_pass = True
        for label, passed, detail in checks:
            icon = "✓" if passed else "✗"
            print(f"  {icon} {label}")
            if detail:
                print(f"      {detail}")
            if not passed:
                all_pass = False
        if not all_pass:
            print("\n  ⚠ One or more integrity checks failed.")

    con.close()
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 src/pipeline/queries.py <state>")
        sys.exit(1)
    run(sys.argv[1])
