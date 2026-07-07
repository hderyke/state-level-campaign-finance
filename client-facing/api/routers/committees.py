from fastapi import APIRouter, Depends, Query
from typing import Optional, List
import duckdb

from db import get_db

router = APIRouter()

SORTABLE = {
    "committee_name", "committee_type", "candidate_name",
    "state", "election_year", "city",
    "total_raised", "total_spent",
}


@router.get("/")
def get_committees(
    state: Optional[List[str]] = Query(None, description="Filter by state abbreviation (repeatable)"),
    committee_name: Optional[str] = Query(None, description="Partial match on committee name"),
    committee_type: Optional[str] = Query(None, description="Exact committee type"),
    candidate_last: Optional[str] = Query(None, description="Partial match on candidate last name"),
    person_id: Optional[int] = Query(None, description="Exact person_id match"),
    city: Optional[str] = Query(None, description="Partial match on city"),
    zip_code: Optional[str] = Query(None, alias="zip", description="Partial match on zip code"),
    election_year: Optional[int] = Query(None, description="Exact election year"),
    sort_by: Optional[str] = Query(None, enum=list(SORTABLE)),
    sort_dir: str = Query("asc", enum=["asc", "desc"]),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    clauses, params = [], []

    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    if committee_name:
        clauses.append("committee_name ILIKE ?")
        params.append(f"%{committee_name}%")
    if committee_type:
        clauses.append("committee_type = ?")
        params.append(committee_type)
    if candidate_last:
        clauses.append("candidate_name ILIKE ?")
        params.append(f"%{candidate_last}%")
    if person_id is not None:
        clauses.append("person_id = ?")
        params.append(person_id)
    if city:
        clauses.append("city ILIKE ?")
        params.append(f"%{city}%")
    if zip_code:
        clauses.append("zip ILIKE ?")
        params.append(f"{zip_code}%")
    if election_year is not None:
        clauses.append("election_year = ?")
        params.append(election_year)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    order = f"ORDER BY {sort_by} {sort_dir.upper()}" if sort_by in SORTABLE else "ORDER BY committee_name ASC"

    total = db.execute(f"SELECT COUNT(*) FROM committees {where}", params).fetchone()[0]

    rows = db.execute(
        f"""
        SELECT c.state, c.person_id, c.committee_name, c.committee_type,
               c.election_year, c.candidate_name, c.treasurer_name, c.city, c.zip,
               COALESCE(s.total_raised, 0) AS total_raised,
               COALESCE(s.total_spent,  0) AS total_spent
        FROM (
            SELECT state, person_id, committee_name, committee_type,
                   election_year, candidate_name, treasurer_name, city, zip
            FROM committees
            {where}
        ) c
        LEFT JOIN summary.committee_summary s
          ON s.state = c.state AND s.committee_name = c.committee_name
        {order}
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    ).fetchall()

    cols = ["state", "person_id", "committee_name", "committee_type",
            "election_year", "candidate_name", "treasurer_name", "city", "zip",
            "total_raised", "total_spent"]
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": [dict(zip(cols, row)) for row in rows],
    }


@router.get("/profile")
def get_committee_profile(
    name: str = Query(..., min_length=1),
    state: str = Query(..., min_length=2, max_length=2),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    from fastapi import HTTPException
    s = state.upper()

    con_summary = db.execute(
        "SELECT COUNT(*), COALESCE(ROUND(SUM(amount),2),0) FROM contributions WHERE committee_name = ? AND state = ?",
        [name, s],
    ).fetchone()
    exp_summary = db.execute(
        "SELECT COUNT(*), COALESCE(ROUND(SUM(amount),2),0) FROM expenditures WHERE committee_name = ? AND state = ?",
        [name, s],
    ).fetchone()

    if con_summary[0] < 50 and con_summary[1] < 25000:
        raise HTTPException(404, "Insufficient activity for profile")

    top_donors = db.execute(
        """SELECT contributor_name, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS total
           FROM contributions WHERE committee_name = ? AND state = ?
           AND contributor_name IS NOT NULL AND LENGTH(contributor_name) > 0
           GROUP BY contributor_name ORDER BY total DESC LIMIT 10""",
        [name, s],
    ).fetchall()

    top_payees = db.execute(
        """SELECT payee_name, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS total
           FROM expenditures WHERE committee_name = ? AND state = ?
           AND payee_name IS NOT NULL AND LENGTH(payee_name) > 0
           GROUP BY payee_name ORDER BY total DESC LIMIT 10""",
        [name, s],
    ).fetchall()

    by_year = db.execute(
        """SELECT YEAR(date) AS yr, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS total
           FROM contributions WHERE committee_name = ? AND state = ?
           AND date IS NOT NULL GROUP BY YEAR(date) ORDER BY yr""",
        [name, s],
    ).fetchall()

    by_year_exp = db.execute(
        """SELECT YEAR(date) AS yr, COUNT(*) AS cnt, ROUND(SUM(amount),2) AS total
           FROM expenditures WHERE committee_name = ? AND state = ?
           AND date IS NOT NULL GROUP BY YEAR(date) ORDER BY yr""",
        [name, s],
    ).fetchall()

    by_type = db.execute(
        """SELECT COALESCE(contributor_type,'Unknown') AS type, ROUND(SUM(amount),2) AS total
           FROM contributions WHERE committee_name = ? AND state = ?
           GROUP BY contributor_type ORDER BY total DESC""",
        [name, s],
    ).fetchall()

    top_states = db.execute(
        """SELECT COALESCE(contributor_state,'Unknown') AS cstate, ROUND(SUM(amount),2) AS total
           FROM contributions WHERE committee_name = ? AND state = ?
           AND contributor_state IS NOT NULL AND LENGTH(contributor_state) > 0
           GROUP BY contributor_state ORDER BY total DESC LIMIT 5""",
        [name, s],
    ).fetchall()
    top_states_sum = sum(r[1] for r in top_states)
    other_states   = round(con_summary[1] - top_states_sum, 2)

    return {
        "name": name,
        "state": s,
        "summary": {
            "contribution_count": con_summary[0],
            "total_raised":       con_summary[1],
            "expenditure_count":  exp_summary[0],
            "total_spent":        exp_summary[1],
        },
        "top_donors":   [{"name": r[0], "count": r[1], "total": r[2]} for r in top_donors],
        "top_payees":   [{"name": r[0], "count": r[1], "total": r[2]} for r in top_payees],
        "by_year":      [{"year": r[0], "count": r[1], "total": r[2]} for r in by_year],
        "by_year_exp":  [{"year": r[0], "count": r[1], "total": r[2]} for r in by_year_exp],
        "by_type":      [{"type": r[0], "total": r[1]} for r in by_type],
        "top_states":   [{"state": r[0], "total": r[1]} for r in top_states],
        "other_states": other_states,
    }


@router.get("/name-search")
def search_committee_names(
    q: str = Query(..., min_length=3),
    state: Optional[List[str]] = Query(None),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    clauses = ["committee_name ILIKE ?"]
    params = [f"{q}%"]
    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    where = "WHERE " + " AND ".join(clauses)
    rows = db.execute(
        f"""
        SELECT DISTINCT committee_name FROM committees
        {where} ORDER BY committee_name LIMIT 10
        """,
        params,
    ).fetchall()
    return [r[0] for r in rows if r[0]]


@router.get("/types")
def get_committee_types(
    state: Optional[List[str]] = Query(None),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    """Return distinct committee types (for dropdown population)."""
    clauses, params = [], []
    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = db.execute(
        f"SELECT DISTINCT committee_type FROM committees {where} ORDER BY committee_type",
        params,
    ).fetchall()
    return [r[0] for r in rows if r[0]]
