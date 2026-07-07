from fastapi import APIRouter, Depends, Query
from typing import Optional, List
import duckdb

from db import get_db

router = APIRouter()

SORTABLE = {"election_year", "candidate_last", "candidate_name", "state", "canonical_office"}


@router.get("/")
def get_candidates(
    state: Optional[List[str]] = Query(None, description="Filter by state abbreviation (repeatable: ?state=CA&state=TX)"),
    candidate_first: Optional[str] = Query(None, description="Partial match on first name (case-insensitive)"),
    candidate_last: Optional[str] = Query(None, description="Partial match on last name (case-insensitive)"),
    office_search: Optional[str] = Query(None, description="Partial match on office or canonical_office"),
    district: Optional[str] = Query(None, description="Partial match on district"),
    party: Optional[str] = Query(None, description="Partial match on party"),
    election_year_min: Optional[int] = Query(None),
    election_year_max: Optional[int] = Query(None),
    person_id: Optional[int] = Query(None, description="Exact person_id lookup"),
    sort_by: Optional[str] = Query(None, enum=list(SORTABLE)),
    sort_dir: str = Query("desc", enum=["asc", "desc"]),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    clauses, params = [], []

    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    if candidate_first:
        clauses.append("candidate_first ILIKE ?")
        params.append(f"%{candidate_first}%")
    if candidate_last:
        clauses.append("candidate_last ILIKE ?")
        params.append(f"%{candidate_last}%")
    if office_search:
        clauses.append("(office ILIKE ? OR canonical_office ILIKE ?)")
        params.extend([f"%{office_search}%", f"%{office_search}%"])
    if district:
        clauses.append("district ILIKE ?")
        params.append(f"%{district}%")
    if party:
        clauses.append("party ILIKE ?")
        params.append(f"%{party}%")
    if election_year_min:
        clauses.append("election_year >= ?")
        params.append(election_year_min)
    if election_year_max:
        clauses.append("election_year <= ?")
        params.append(election_year_max)
    if person_id is not None:
        clauses.append("person_id = ?")
        params.append(person_id)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    order = f"ORDER BY {sort_by} {sort_dir.upper()}" if sort_by in SORTABLE else ""

    total = db.execute(f"SELECT COUNT(*) FROM candidates {where}", params).fetchone()[0]

    rows = db.execute(
        f"""
        SELECT state, person_id, candidate_name, candidate_first, candidate_last,
               office, canonical_office, election_year, party, district
        FROM candidates
        {where}
        {order}
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    ).fetchall()

    cols = ["state", "person_id", "candidate_name", "candidate_first", "candidate_last",
            "office", "canonical_office", "election_year", "party", "district"]
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": [dict(zip(cols, row)) for row in rows],
    }
