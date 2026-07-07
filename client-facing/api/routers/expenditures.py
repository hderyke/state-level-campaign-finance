from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List
import duckdb

from db import get_db

router = APIRouter()

SORTABLE   = {"amount", "date", "committee_name", "payee_name", "transaction_category", "state"}
GROUPABLE  = {"state", "transaction_category", "year", "committee_name", "payee_name"}


@router.get("/")
def get_expenditures(
    state: Optional[List[str]] = Query(None, description="Filter by state abbreviation (repeatable)"),
    committee_name: Optional[str] = Query(None, description="Partial match on committee name"),
    payee_name: Optional[str] = Query(None, description="Partial match on payee name"),
    transaction_category: Optional[str] = Query(None, description="Exact transaction category"),
    amount_min: Optional[float] = Query(None),
    amount_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
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
    if committee_name:
        clauses.append("committee_name ILIKE ?")
        params.append(f"%{committee_name}%")
    if payee_name:
        clauses.append("payee_name ILIKE ?")
        params.append(f"%{payee_name}%")
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
    order = (f"ORDER BY TRY_CAST({sort_by} AS DATE) {sort_dir.upper()}" if sort_by == 'date'
             else f"ORDER BY {sort_by} {sort_dir.upper()}") if sort_by in SORTABLE else ""

    total = db.execute(f"SELECT COUNT(*) FROM expenditures {where}", params).fetchone()[0]

    # Try to join against committee name lookup (requires rebuilt sidecar)
    try:
        rows = db.execute(
            f"""
            SELECT e.state, e.committee_name, e.amount, e.date, e.transaction_category, e.payee_name,
                   cl.canonical_name AS payee_committee_name,
                   cl.state         AS payee_committee_state
            FROM (
                SELECT state, committee_name, amount, date, transaction_category, payee_name
                FROM expenditures
                {where}
                {order}
                LIMIT ? OFFSET ?
            ) e
            LEFT JOIN summary.committee_name_lookup cl ON cl.lcn = LOWER(e.payee_name)
            """,
            params + [limit, offset],
        ).fetchall()
        cols = ["state", "committee_name", "amount", "date", "transaction_category", "payee_name",
                "payee_committee_name", "payee_committee_state"]
    except Exception:
        rows = db.execute(
            f"""
            SELECT state, committee_name, amount, date, transaction_category, payee_name
            FROM expenditures
            {where}
            {order}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
        cols = ["state", "committee_name", "amount", "date", "transaction_category", "payee_name"]
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": [dict(zip(cols, row)) for row in rows],
    }


@router.get("/aggregate")
def aggregate_expenditures(
    group_by: str = Query(..., description="Dimension to group by"),
    metrics: List[str] = Query(["count", "total"]),
    state: Optional[List[str]] = Query(None),
    committee_name: Optional[str] = Query(None),
    payee_name: Optional[str] = Query(None),
    transaction_category: Optional[str] = Query(None),
    amount_min: Optional[float] = Query(None),
    amount_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    if group_by not in GROUPABLE:
        raise HTTPException(400, f"group_by must be one of: {', '.join(sorted(GROUPABLE))}")

    clauses, params = [], []
    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    if committee_name:
        clauses.append("committee_name ILIKE ?")
        params.append(f"%{committee_name}%")
    if payee_name:
        clauses.append("payee_name ILIKE ?")
        params.append(f"%{payee_name}%")
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
    group_expr = "YEAR(date)" if group_by == "year" else group_by

    select_parts = [f"{group_expr} AS {group_by}"]
    if "count" in metrics: select_parts.append("COUNT(*) AS count")
    if "total" in metrics: select_parts.append("ROUND(SUM(amount), 2) AS total")
    if "avg"   in metrics: select_parts.append("ROUND(AVG(amount), 2) AS avg")
    if "max"   in metrics: select_parts.append("ROUND(MAX(amount), 2) AS max")

    order_col = "total" if "total" in metrics else "count"

    rows = db.execute(
        f"""
        SELECT {', '.join(select_parts)}
        FROM expenditures
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


@router.get("/transaction-categories")
def get_transaction_categories(
    state: Optional[List[str]] = Query(None),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
):
    """Return distinct transaction categories (for dropdown population)."""
    clauses, params = [], []
    if state:
        clauses.append(f"state IN ({', '.join(['?'] * len(state))})")
        params.extend([s.upper() for s in state])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = db.execute(
        f"SELECT DISTINCT transaction_category FROM expenditures {where} ORDER BY transaction_category",
        params,
    ).fetchall()
    return [r[0] for r in rows if r[0]]
