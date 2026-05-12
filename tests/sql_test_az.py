import duckdb

con = duckdb.connect()
CLEAN = "data/arizona/cleaned"
opts  = "null_padding=true, ignore_errors=true, parallel=false"

con.execute(f"CREATE TABLE contributions AS SELECT * FROM read_csv_auto('{CLEAN}/contributions.csv', {opts})")
con.execute(f"CREATE TABLE committees    AS SELECT * FROM read_csv_auto('{CLEAN}/committees.csv',    {opts})")
con.execute(f"CREATE TABLE expenditures  AS SELECT * FROM read_csv_auto('{CLEAN}/expenditures.csv',  {opts})")

print("Row counts:")
for tbl in ["contributions", "committees", "expenditures"]:
    n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"  {tbl:20s} {n:>10,}")

print("\nCommittee types:")
for r in con.execute("""
    SELECT committee_type, COUNT(*) n
    FROM committees GROUP BY 1 ORDER BY 2 DESC LIMIT 10
""").fetchall():
    print(f"  {str(r[0])[:40]:40s} {r[1]:>7,}")

print("\nTop 15 PAC contributors by total dollars:")
for r in con.execute("""
    SELECT
        COALESCE(NULLIF(committee_name,''), state_filer_id) AS pac,
        COUNT(*)                                             AS txns,
        ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 2)           AS total_raised
    FROM contributions
    WHERE contributor_type = 'PAC'
      AND (committee_name != '' OR state_filer_id != '')
    GROUP BY 1
    ORDER BY total_raised DESC NULLS LAST
    LIMIT 15
""").fetchall():
    print(f"  {str(r[0])[:45]:45s}  {r[1]:>6,} txns  ${r[2]:>13,.2f}")

print("\nTop 15 PAC spenders:")
for r in con.execute("""
    SELECT
        COALESCE(NULLIF(committee_name,''), state_filer_id) AS pac,
        COUNT(*)                                             AS txns,
        ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 2)           AS total_spent
    FROM expenditures
    WHERE category = 'PAC'
      AND (committee_name != '' OR state_filer_id != '')
    GROUP BY 1
    ORDER BY total_spent DESC NULLS LAST
    LIMIT 15
""").fetchall():
    print(f"  {str(r[0])[:45]:45s}  {r[1]:>6,} txns  ${r[2]:>13,.2f}")

print("\nstate_filer_id coverage by filer type:")
for r in con.execute("""
    SELECT contributor_type,
           COUNT(*) total,
           ROUND(100.0 * SUM(CASE WHEN state_filer_id != '' THEN 1 ELSE 0 END) / COUNT(*), 1) pct_attributed
    FROM contributions
    GROUP BY 1 ORDER BY total DESC
""").fetchall():
    print(f"  {str(r[0]):15s}  {r[1]:>9,} rows  {r[2]:>5}% attributed")
