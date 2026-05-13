"""
columns.py — Canonical column definitions for all cleaned output tables.

Every parser imports this as `import columns as C` and uses these lists as
the fieldnames for csv.DictWriter.  Extra keys written by a parser are
silently ignored (extrasaction='ignore'); missing keys become empty strings
via restval=''.

The superset approach means a single DuckDB table can hold data from every
state without schema conflicts.
"""

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
    "date",
    "candidate_name",
    "election_year",
    "filing_id",
    "amended",
    "raw_file",
    "row_num",
]
