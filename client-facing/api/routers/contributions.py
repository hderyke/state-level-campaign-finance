from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
import duckdb

from db import get_db

router = APIRouter()

SORTABLE  = {
    "amount", "date",
    "contributor_name", "candidate_name",
    "state", "contributor_state", "contributor_city", "contributor_zip",
    "contributor_type", "transaction_category",
    "committee_name", "employer", "occupation", "office", "election_year",
}
GROUPABLE = {"state", "contributor_type", "transaction_category", "year", "committee_name", "contributor_name"}


def _build_where(
    state, committee_name, contributor_name, contributor_name_mode,
    person_id, candidate_first, candidate_last,
    contributor_type, contributor_city, contributor_state,
    contributor_zip, transaction_category,
    amount_min, amount_max, date_from, date_to,
):
    clauses, params = [], []

    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    if committee_name:
        clauses.append("committee_name ILIKE ?")
        params.append(f"%{committee_name}%")

    # Multi-name contributor search (OR across rows)
    if contributor_name:
        name_clauses = []
        for i, name in enumerate(contributor_name):
            if not name:
                continue
            mode = (contributor_name_mode[i]
                    if contributor_name_mode and i < len(contributor_name_mode)
                    else "contains")
            if mode == "exact":
                name_clauses.append("LOWER(contributor_name) = LOWER(?)")
                params.append(name)
            elif mode == "starts":
                name_clauses.append("contributor_name ILIKE ?")
                params.append(f"{name}%")
            else:
                name_clauses.append("contributor_name ILIKE ?")
                params.append(f"%{name}%")
        if name_clauses:
            clauses.append(f"({' OR '.join(name_clauses)})")

    if person_id:
        # Resolve person_id → all known candidate_name variants, then filter
        clauses.append("""
            candidate_name IN (
                SELECT DISTINCT candidate_name FROM candidates WHERE person_id = ?
            )
        """)
        params.append(int(person_id))
    if candidate_first:
        clauses.append("candidate_name ILIKE ?")
        params.append(f"%{candidate_first}%")
    if candidate_last:
        clauses.append("candidate_name ILIKE ?")
        params.append(f"%{candidate_last}%")
    if contributor_type:
        clauses.append("contributor_type = ?")
        params.append(contributor_type)
    if contributor_city:
        clauses.append("contributor_city ILIKE ?")
        params.append(f"%{contributor_city}%")
    if contributor_state:
        clauses.append(f"contributor_state IN ({', '.join(['?'] * len(contributor_state))})")
        params.extend([s.upper() for s in contributor_state])
    if contributor_zip:
        clauses.append("contributor_zip = ?")
        params.append(contributor_zip)
    if transaction_category:
        clauses.append("transaction_category = ?")
        params.append(transaction_category)
    if amount_min is not None:
        clauses.append("amount >= ?")
        params.append(amount_min)
    if amount_max is not None:
        clauses.append("amount <= ?")
        params.append(amount_max)
    if date_from:
        clauses.append("date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("date <= ?")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


# ── shared filter params for both endpoints ───────────────────────────────────
FILTER_PARAMS = dict(
    state=Query(None),
    committee_name=Query(None),
    contributor_name=Query(None),
    contributor_name_mode=Query(None),
    person_id=Query(None),
    candidate_first=Query(None),
    candidate_last=Query(None),
    contributor_type=Query(None),
    contributor_city=Query(None),
    contributor_state=Query(None),
    contributor_zip=Query(None),
    transaction_category=Query(None),
    amount_min=Query(None),
    amount_max=Query(None),
    date_from=Query(None),
    date_to=Query(None),
)


@router.get("/")
def get_contributions(
    state: Optional[List[str]] = Query(None),
    committee_name: Optional[str] = Query(None),
    contributor_name: Optional[List[str]] = Query(None),
    contributor_name_mode: Optional[List[str]] = Query(None),
    person_id: Optional[str] = Query(None),
    candidate_first: Optional[str] = Query(None),
    candidate_last: Optional[str] = Query(None),
    contributor_type: Optional[str] = Query(None),
    contributor_city: Optional[str] = Query(None),
    contributor_state: Optional[List[str]] = Query(None),
    contributor_zip: Optional[str] = Query(None),
    transaction_category: Optional[str] = Query(None),
    amount_min: Optional[float] = Query(None),
    amount_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None, enum=list(SORTABLE)),
    sort_dir: str = Query("desc", enum=["asc", "desc"]),
    sort_by2: Optional[str] = Query(None, enum=list(SORTABLE)),
    sort_dir2: str = Query("asc", enum=["asc", "desc"]),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    where, params = _build_where(
        state, committee_name, contributor_name, contributor_name_mode,
        person_id, candidate_first, candidate_last,
        contributor_type, contributor_city, contributor_state,
        contributor_zip, transaction_category,
        amount_min, amount_max, date_from, date_to,
    )
    def order_expr(col, d):
        return f"TRY_CAST({col} AS DATE) {d}" if col == 'date' else f"{col} {d}"

    order_parts = []
    if sort_by in SORTABLE:
        order_parts.append(order_expr(sort_by, sort_dir.upper()))
    if sort_by2 in SORTABLE:
        order_parts.append(order_expr(sort_by2, sort_dir2.upper()))
    order = ("ORDER BY " + ", ".join(order_parts)) if order_parts else ""

    total = db.execute(f"SELECT COUNT(*) FROM contributions {where}", params).fetchone()[0]

    # Try to join against committee name lookup (requires rebuilt sidecar)
    try:
        rows = db.execute(
            f"""
            SELECT c.state, c.committee_name, c.amount, c.date, c.transaction_category,
                   c.contributor_name, c.contributor_type,
                   c.contributor_city, c.contributor_state, c.contributor_zip,
                   c.candidate_name, c.employer, c.occupation, c.office, c.election_year,
                   cl.canonical_name AS contributor_committee_name,
                   cl.state         AS contributor_committee_state
            FROM (
                SELECT state, committee_name, amount, date, transaction_category,
                       contributor_name, contributor_type,
                       contributor_city, contributor_state, contributor_zip,
                       candidate_name, employer, occupation, office, election_year
                FROM contributions
                {where}
                {order}
                LIMIT ? OFFSET ?
            ) c
            LEFT JOIN summary.committee_name_lookup cl ON cl.lcn = LOWER(c.contributor_name)
            {order}
            """,
            params + [limit, offset],
        ).fetchall()
        cols = [
            "state", "committee_name", "amount", "date", "transaction_category",
            "contributor_name", "contributor_type",
            "contributor_city", "contributor_state", "contributor_zip",
            "candidate_name", "employer", "occupation", "office", "election_year",
            "contributor_committee_name", "contributor_committee_state",
        ]
    except Exception:
        rows = db.execute(
            f"""
            SELECT state, committee_name, amount, date, transaction_category,
                   contributor_name, contributor_type,
                   contributor_city, contributor_state, contributor_zip,
                   candidate_name, employer, occupation, office, election_year
            FROM contributions
            {where}
            {order}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
        cols = [
            "state", "committee_name", "amount", "date", "transaction_category",
            "contributor_name", "contributor_type",
            "contributor_city", "contributor_state", "contributor_zip",
            "candidate_name", "employer", "occupation", "office", "election_year",
        ]
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": [dict(zip(cols, row)) for row in rows],
    }


@router.get("/aggregate")
def aggregate_contributions(
    group_by: str = Query(..., description="Dimension to group by"),
    metrics: List[str] = Query(["count", "total"]),
    state: Optional[List[str]] = Query(None),
    committee_name: Optional[str] = Query(None),
    contributor_name: Optional[List[str]] = Query(None),
    contributor_name_mode: Optional[List[str]] = Query(None),
    person_id: Optional[str] = Query(None),
    candidate_first: Optional[str] = Query(None),
    candidate_last: Optional[str] = Query(None),
    contributor_type: Optional[str] = Query(None),
    contributor_city: Optional[str] = Query(None),
    contributor_state: Optional[List[str]] = Query(None),
    contributor_zip: Optional[str] = Query(None),
    transaction_category: Optional[str] = Query(None),
    amount_min: Optional[float] = Query(None),
    amount_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    if group_by not in GROUPABLE:
        raise HTTPException(400, f"group_by must be one of: {', '.join(sorted(GROUPABLE))}")

    where, params = _build_where(
        state, committee_name, contributor_name, contributor_name_mode,
        person_id, candidate_first, candidate_last,
        contributor_type, contributor_city, contributor_state,
        contributor_zip, transaction_category,
        amount_min, amount_max, date_from, date_to,
    )

    # For "year" we extract from the date column
    group_expr = "YEAR(date)" if group_by == "year" else group_by

    select_parts = [f"{group_expr} AS {group_by}"]
    if "count" in metrics:
        select_parts.append("COUNT(*) AS count")
    if "total" in metrics:
        select_parts.append("ROUND(SUM(amount), 2) AS total")
    if "avg" in metrics:
        select_parts.append("ROUND(AVG(amount), 2) AS avg")
    if "max" in metrics:
        select_parts.append("ROUND(MAX(amount), 2) AS max")

    # Default sort: total if selected, else count
    order_col = "total" if "total" in metrics else "count"

    rows = db.execute(
        f"""
        SELECT {', '.join(select_parts)}
        FROM contributions
        {where}
        GROUP BY {group_expr}
        ORDER BY {order_col} DESC
        LIMIT 500
        """,
        params,
    ).fetchall()

    col_names = [group_by] + [m for m in ["count", "total", "avg", "max"] if m in metrics]
    return {
        "group_by": group_by,
        "metrics": metrics,
        "results": [dict(zip(col_names, row)) for row in rows],
        "truncated": len(rows) == 500,
    }


@router.get("/committee-search")
def search_committees(
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
        SELECT DISTINCT committee_name FROM contributions
        {where}
        ORDER BY committee_name
        LIMIT 10
        """,
        params,
    ).fetchall()
    return [r[0] for r in rows if r[0]]


@router.get("/contributor-types")
def get_contributor_types(
    state: Optional[List[str]] = Query(None),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    clauses, params = [], []
    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = db.execute(
        f"SELECT DISTINCT contributor_type FROM contributions {where} ORDER BY contributor_type",
        params,
    ).fetchall()
    return [r[0] for r in rows if r[0]]


@router.get("/transaction-categories")
def get_transaction_categories(
    state: Optional[List[str]] = Query(None),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    clauses, params = [], []
    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = db.execute(
        f"SELECT DISTINCT transaction_category FROM contributions {where} ORDER BY transaction_category",
        params,
    ).fetchall()
    return [r[0] for r in rows if r[0]]
