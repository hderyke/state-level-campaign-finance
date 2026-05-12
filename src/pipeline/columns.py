"""
columns.py — Canonical column definitions for all 5 cleaned CSV tables.

Every state parser must output exactly these columns, in this order.
Fields the state doesn't provide are written as empty strings.
This guarantees all CSVs are structurally identical and can be
stacked / UNION ALL'd without any schema mismatches.
"""

CANDIDATES = [
    "state",
    "candidate_name",
    "candidate_first",
    "candidate_last",
    "office",
    "district",
    "jurisdiction",
    "party",
    "election_year",
    "status",
    "incumbent",
    "raw_file",
    "row_num",
]

COMMITTEES = [
    "state",
    "state_filer_id",
    "committee_name",
    "committee_type",
    "candidate_name",
    "treasurer_name",
    "city",
    "zip",
    "active",
]

CONTRIBUTIONS = [
    "state",
    "state_filer_id",
    "committee_name",
    "contributor_name",
    "amount",
    "date",
    "transaction_type",
    "contributor_type",
    "contributor_city",
    "contributor_state",
    "contributor_zip",
    "employer",
    "occupation",
    "candidate_name",
    "office",
    "election_year",
    "filing_id",
    "amended",
    "raw_file",
    "row_num",
]

EXPENDITURES = [
    "state",
    "state_filer_id",
    "committee_name",
    "payee_name",
    "amount",
    "date",
    "transaction_type",
    "purpose",
    "category",
    "payee_city",
    "payee_state",
    "payee_zip",
    "candidate_name",
    "office",
    "election_year",
    "filing_id",
    "amended",
    "raw_file",
    "row_num",
]

LOANS_DEBTS = [
    "state",
    "state_filer_id",
    "committee_name",
    "record_type",
    "counterparty_name",
    "counterparty_city",
    "counterparty_state",
    "counterparty_zip",
    "original_amount",
    "outstanding_balance",
    "date",
    "purpose",
    "candidate_name",
    "election_year",
    "filing_id",
    "amended",
    "raw_file",
    "row_num",
]

ALL_TABLES = {
    "candidates":    CANDIDATES,
    "committees":    COMMITTEES,
    "contributions": CONTRIBUTIONS,
    "expenditures":  EXPENDITURES,
    "loans_debts":   LOANS_DEBTS,
}
