"""
tests/test_queries.py — Exploratory queries against a state's .db file.

Usage:
    python3 tests/test_queries.py arizona
    python3 tests/test_queries.py alabama
"""

import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]

W_NAME  = 42
W_TXNS  =  7
W_MONEY = 14
W_TYPE  = 28
W_PARTY = 12
W_OFF   = 26


def find_db(state: str) -> Path:
    if state.lower() == "all":
        db_path = PROJECT_ROOT / "data" / "state-level-cf.db"
        if not db_path.exists():
            print("[!] state-level-cf.db not found. Run aggregate.py first.")
            sys.exit(1)
        return db_path
    matches = [d for d in (PROJECT_ROOT / "data").iterdir()
               if d.is_dir() and d.name.lower() == state.lower()]
    if not matches:
        print(f"[!] No data directory found for '{state}'")
        sys.exit(1)
    db_path = matches[0] / "cleaned" / f"{state.lower()}.db"
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

    c1, c2, c3, c4, c5 = W_NAME, W_TXNS, W_MONEY, W_NAME, W_MONEY
    print(f"  {'Contributor':<{c1}}  {'Txns':>{c2}}  {'Total Donated':>{c3}}  {'Top Recipient':<{c4}}  {'To Top':>{c5}}")
    print(f"  {'-'*c1}  {'-'*c2}  {'-'*c3}  {'-'*c4}  {'-'*c5}")
    for r in rows:
        print(f"  {trunc(r[0],c1):<{c1}}  {r[1]:>{c2},}  {fmt_money(r[2]):>{c3}}  {trunc(r[3] or '—',c4):<{c4}}  {fmt_money(r[4]):>{c5}}")

    # ── 2. Top 20 recipient candidates ───────────────────────────────────────
    # Join via candidate_name (contributions no longer carries state_filer_id).
    section("TOP 20 RECIPIENT CANDIDATES — total contributions received", state)
    rows = con.execute("""
        WITH dedup_candidates AS (
            -- One row per (state, candidate_name): person_id is per-office in
            -- the "committee" model (AZ, AL, CA), so grouping by person_id would
            -- fan-out for candidates who ran for multiple offices.  Deduping by
            -- name instead ensures each contributor_name matches exactly once.
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

    W_ST = 4
    c1, c2, c3, c4, c5, c6 = W_NAME, W_ST, W_OFF, W_PARTY, W_TXNS, W_MONEY
    print(f"  {'Candidate':<{c1}}  {'St':<{c2}}  {'Office':<{c3}}  {'Party':<{c4}}  {'Txns':>{c5}}  {'Total Received':>{c6}}")
    print(f"  {'-'*c1}  {'-'*c2}  {'-'*c3}  {'-'*c4}  {'-'*c5}  {'-'*c6}")
    for r in rows:
        print(f"  {trunc(r[0],c1):<{c1}}  {trunc(r[1] or '',c2):<{c2}}  {trunc(r[2] or '',c3):<{c3}}  {trunc(r[3] or '',c4):<{c4}}  {r[4]:>{c5},}  {fmt_money(r[5]):>{c6}}")

    # ── 3. Top 20 non-candidate committees ───────────────────────────────────
    # Excludes committees whose name matches a known candidate_name.
    section("TOP 20 NON-CANDIDATE COMMITTEES — total contributions received", state)
    rows = con.execute("""
        WITH cmte_types AS (
            -- One row per committee_name: prefer a non-blank type, take MAX alphabetically
            SELECT LOWER(TRIM(committee_name)) AS name_key,
                   MAX(CASE WHEN committee_type IS NOT NULL AND committee_type != ''
                            THEN committee_type END) AS committee_type
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
          AND NOT EXISTS (
              SELECT 1 FROM candidates ca
              WHERE LOWER(TRIM(ca.candidate_name)) = LOWER(TRIM(co.committee_name))
          )
        GROUP BY co.state, co.committee_name, ct.committee_type
        ORDER BY total DESC LIMIT 20
    """).fetchall()

    W_ST = 4
    c1, c2, c3, c4, c5 = W_ST, W_NAME, W_TYPE, W_TXNS, W_MONEY
    print(f"  {'St':<{c1}}  {'Committee':<{c2}}  {'Type':<{c3}}  {'Txns':>{c4}}  {'Total Received':>{c5}}")
    print(f"  {'-'*c1}  {'-'*c2}  {'-'*c3}  {'-'*c4}  {'-'*c5}")
    for r in rows:
        print(f"  {trunc(r[0] or '',c1):<{c1}}  {trunc(r[1],c2):<{c2}}  {trunc(r[2] or '',c3):<{c3}}  {r[3]:>{c4},}  {fmt_money(r[4]):>{c5}}")

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
            GROUP BY payee_name, committee_name
        )
        SELECT p.payee_name, p.n, ROUND(p.total,0), t.committee_name, ROUND(t.from_client,0)
        FROM payee_totals p
        LEFT JOIN top_client t ON p.payee_name = t.payee_name AND t.rn = 1
        ORDER BY p.total DESC LIMIT 10
    """).fetchall()

    c1, c2, c3, c4, c5 = W_NAME, W_TXNS, W_MONEY, W_NAME, W_MONEY
    print(f"  {'Payee':<{c1}}  {'Txns':>{c2}}  {'Total Received':>{c3}}  {'Largest Client':<{c4}}  {'From Client':>{c5}}")
    print(f"  {'-'*c1}  {'-'*c2}  {'-'*c3}  {'-'*c4}  {'-'*c5}")
    for r in rows:
        print(f"  {trunc(r[0],c1):<{c1}}  {r[1]:>{c2},}  {fmt_money(r[2]):>{c3}}  {trunc(r[3] or '—',c4):<{c4}}  {fmt_money(r[4]):>{c5}}")

    con.close()
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 tests/test_queries.py <state>")
        sys.exit(1)
    run(sys.argv[1])
