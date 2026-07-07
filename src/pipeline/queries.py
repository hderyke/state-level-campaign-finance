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

    W_YR = 6; W_N = 10; W_T = 16
    print(f"  {'Year':>{W_YR}}  {'Cont N':>{W_N}}  {'Cont Total':>{W_T}}  {'Expn N':>{W_N}}  {'Expn Total':>{W_T}}")
    print(f"  {'-'*W_YR}  {'-'*W_N}  {'-'*W_T}  {'-'*W_N}  {'-'*W_T}")
    for r in rows:
        yr, cn, ct, en, et = r
        print(f"  {str(yr):>{W_YR}}  {cn:>{W_N},}  {fmt_money(ct):>{W_T}}  {en:>{W_N},}  {fmt_money(et):>{W_T}}")

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

    W_CT = 30; W_N2 = 10; W_T2 = 16; W_P = 8
    print(f"  {'Contributor Type':<{W_CT}}  {'N':>{W_N2}}  {'Total':>{W_T2}}  {'% rows':>{W_P}}")
    print(f"  {'-'*W_CT}  {'-'*W_N2}  {'-'*W_T2}  {'-'*W_P}")
    for r in rows:
        ct, n, total, pct = r
        print(f"  {trunc(ct, W_CT):<{W_CT}}  {n:>{W_N2},}  {fmt_money(total):>{W_T2}}  {pct:>{W_P}.1f}%")

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

    W_ST2 = 18
    print(f"  {'State':<{W_ST2}}  {'N':>{W_N2}}  {'Total':>{W_T2}}  {'% rows':>{W_P}}")
    print(f"  {'-'*W_ST2}  {'-'*W_N2}  {'-'*W_T2}  {'-'*W_P}")
    for r in rows:
        st2, n, total, pct = r
        print(f"  {trunc(st2, W_ST2):<{W_ST2}}  {n:>{W_N2},}  {fmt_money(total):>{W_T2}}  {pct:>{W_P}.1f}%")

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

    print(f"  {'Transaction Type':<{W_CT}}  {'N':>{W_N2}}  {'Total':>{W_T2}}  {'% rows':>{W_P}}")
    print(f"  {'-'*W_CT}  {'-'*W_N2}  {'-'*W_T2}  {'-'*W_P}")
    for r in rows:
        tt, n, total, pct = r
        print(f"  {trunc(tt, W_CT):<{W_CT}}  {n:>{W_N2},}  {fmt_money(total):>{W_T2}}  {pct:>{W_P}.1f}%")

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

    W_DT = 12; W_CN = 34; W_CT3 = 6; W_CMT = 36; W_AM = 16
    print(f"  {'Date':<{W_DT}}  {'Contributor':<{W_CN}}  {'Type':<{W_CT3}}  {'Committee':<{W_CMT}}  {'Amount':>{W_AM}}")
    print(f"  {'-'*W_DT}  {'-'*W_CN}  {'-'*W_CT3}  {'-'*W_CMT}  {'-'*W_AM}")
    for r in rows:
        dt, cn, ct3, cmt, amt = r
        print(f"  {str(dt or ''):<{W_DT}}  {trunc(cn or '',W_CN):<{W_CN}}  {trunc(ct3 or '',W_CT3):<{W_CT3}}  {trunc(cmt or '',W_CMT):<{W_CMT}}  {fmt_money(amt):>{W_AM}}")

    # ── 9. 10 random contribution rows ────────────────────────────────────────
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

    W_DT2 = 12; W_CN2 = 28; W_CT4 = 5; W_AM2 = 12
    W_CMT2 = 30; W_CITY = 18; W_ST3 = 4; W_EMP = 22; W_OCC = 20
    print(f"  {'Date':<{W_DT2}}  {'Contributor':<{W_CN2}}  {'T':<{W_CT4}}  {'Amount':>{W_AM2}}  {'Committee':<{W_CMT2}}  {'City':<{W_CITY}}  {'St':<{W_ST3}}  {'Employer':<{W_EMP}}  {'Occupation':<{W_OCC}}")
    print(f"  {'-'*W_DT2}  {'-'*W_CN2}  {'-'*W_CT4}  {'-'*W_AM2}  {'-'*W_CMT2}  {'-'*W_CITY}  {'-'*W_ST3}  {'-'*W_EMP}  {'-'*W_OCC}")
    for r in rows:
        dt, cn, ct4, amt, cmt, city, st3, emp, occ = r
        print(f"  {str(dt or ''):<{W_DT2}}  {trunc(cn or '',W_CN2):<{W_CN2}}  {trunc(ct4 or '',W_CT4):<{W_CT4}}  {fmt_money(amt):>{W_AM2}}  {trunc(cmt or '',W_CMT2):<{W_CMT2}}  {trunc(city or '',W_CITY):<{W_CITY}}  {trunc(st3 or '',W_ST3):<{W_ST3}}  {trunc(emp or '',W_EMP):<{W_EMP}}  {trunc(occ or '',W_OCC):<{W_OCC}}")

    # ── 10. 10 most recent contributions ─────────────────────────────────────
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

    W_DT3 = 12; W_CN3 = 32; W_CT5 = 6; W_AM3 = 14; W_CMT3 = 34
    print(f"  {'Date':<{W_DT3}}  {'Contributor':<{W_CN3}}  {'Type':<{W_CT5}}  {'Amount':>{W_AM3}}  {'Committee':<{W_CMT3}}")
    print(f"  {'-'*W_DT3}  {'-'*W_CN3}  {'-'*W_CT5}  {'-'*W_AM3}  {'-'*W_CMT3}")
    for r in rows:
        dt, cn, ct, amt, cmt = r
        print(f"  {str(dt or ''):<{W_DT3}}  {trunc(cn or '',W_CN3):<{W_CN3}}  {trunc(ct or '',W_CT5):<{W_CT5}}  {fmt_money(amt):>{W_AM3}}  {trunc(cmt or '',W_CMT3):<{W_CMT3}}")

    # ── 11. 10 most recent expenditures ──────────────────────────────────────
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

    W_DT4 = 12; W_PN = 32; W_AM4 = 14; W_CMT4 = 28; W_TX = 22
    print(f"  {'Date':<{W_DT4}}  {'Payee':<{W_PN}}  {'Amount':>{W_AM4}}  {'Committee':<{W_CMT4}}  {'Type':<{W_TX}}")
    print(f"  {'-'*W_DT4}  {'-'*W_PN}  {'-'*W_AM4}  {'-'*W_CMT4}  {'-'*W_TX}")
    for r in rows:
        dt, pn, amt, cmt, tx = r
        print(f"  {str(dt or ''):<{W_DT4}}  {trunc(pn or '',W_PN):<{W_PN}}  {fmt_money(amt):>{W_AM4}}  {trunc(cmt or '',W_CMT4):<{W_CMT4}}  {trunc(tx or '',W_TX):<{W_TX}}")

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


def _state_checks(state: str, con) -> list[tuple[str, bool, str]]:
    """Return [(label, passed, detail)] for state-specific integrity checks."""
    results = []

    if state == "FL":
        # Synthesized candidate committees must be present
        known = ["DESANTIS, RON", "CRIST, CHARLIE", "SCOTT, RICK"]
        for name in known:
            row = con.execute("""
                SELECT committee_name, state_filer_id, election_year
                FROM committees
                WHERE state = 'FL'
                  AND LOWER(committee_name) = LOWER(?)
                LIMIT 1
            """, [name]).fetchone()
            if row:
                detail = (f"state_filer_id={row[1]}  election_year={row[2]}")
                results.append((f"FL committees: '{name}' present", True, detail))
            else:
                results.append((f"FL committees: '{name}' MISSING — synthesized pass may have failed", False, ""))

        # Synthesized rows should not dominate (sanity: real rows > synthesized)
        counts = con.execute("""
            SELECT
                COUNT(*) FILTER (WHERE raw_file = 'fl_committee_details.csv') AS real,
                COUNT(*) FILTER (WHERE raw_file LIKE '%synthesized%')          AS synth
            FROM committees WHERE state = 'FL'
        """).fetchone()
        real, synth = counts
        passed = real > synth
        results.append((
            f"FL committees: real ({real:,}) > synthesized ({synth:,})",
            passed,
            "" if passed else "More synthesized rows than real ones — check parse_committees logic",
        ))

    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 src/pipeline/queries.py <state>")
        sys.exit(1)
    run(sys.argv[1])
