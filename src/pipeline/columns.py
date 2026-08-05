"""
columns.py — Canonical column definitions for all cleaned output tables.

Every parser imports this as `import columns as C` and uses these lists as
the fieldnames for csv.DictWriter.  Extra keys written by a parser are
silently ignored (extrasaction='ignore'); missing keys become empty strings
via restval=''.

The superset approach means a single DuckDB table can hold data from every
state without schema conflicts.

Per-state CSVs and .db files include state_filer_id (needed by
assign_person_ids to compute person_id) and person_id itself. The aggregate
combined DB (state-level-cf.db) drops both via the *_AGG lists below —
state_filer_id is inconsistent across states, and person_id was retired
from validation/aggregation/the API/frontend (2026-07-10) since the
person/committee/name_hash id_model split made it an unreliable identity
across cycles and sources in practice. Both fields remain in the per-state
DBs; the master db identifies candidates/committees by name + state instead.
Raw source traceability is preserved via raw_file + row_num.
"""

# ============================= committees =============================
COMMITTEES = [
    "state",
    "person_id",        # person key — populated by utils.assign_committee_person_ids()
                        # NULL for PACs and other non-candidate committees
    "committee_name",
    "committee_type",
    "election_year",    # year the committee was registered/active for; sparse — only written
                        # by states where committees are cycle-specific (e.g. CT, FL, AR)
    "candidate_name",
    "treasurer_name",
    "city",
    "zip",
    "active",           # sparse — only written by states with official status; omit if unavailable
    "state_filer_id",   # source-system ID — kept for per-state traceability
    # affiliation enrichment -- populated by enrich.py from either a state's own
    # disclosure data (e.g. FL's Statement of Organization Section 7) when the
    # parser can extract it, or src/registries/committees/{state}.csv as a
    # hand-reviewed fallback when it can't. Identifies which candidate a PAC/
    # CCE/ECO is tied to but legally separate from -- distinct from
    # candidate_name above, which is only populated for a candidate's OWN
    # committee. Deliberately name-based rather than person_id-based: see the
    # AGGREGATE note below on why person_id was retired as a cross-cycle
    # identity -- this field would inherit the same unreliability if it
    # resolved to person_id instead of storing the name directly.
    "affiliated_candidate_name",
    "support_oppose",   # "S" or "O" -- blank if not (yet) known
    "raw_file",
    "row_num",
]

# ============================= candidates =============================
CANDIDATES = [
    "state",
    "person_id",        # cross-cycle person key — see utils.assign_person_ids()
    "candidate_name",
    "candidate_first",
    "candidate_last",
    "office",
    "canonical_office",  # derived at aggregate time via src/aliases/office_types.csv
    "district",
    "jurisdiction",
    "party",
    "election_year",
    "incumbent",
    "state_filer_id",   # source-system ID — kept for per-state traceability
    "raw_file",
    "row_num",
]

# =========================== contributions ============================
CONTRIBUTIONS = [
    # state
    "state",
    # required
    "committee_name",
    "amount",
    "date",
    "transaction_type",
    # enrichment
    "contributor_name",
    "contributor_type",
    "contributor_city",
    "contributor_state",
    "contributor_zip",
    "employer",
    "occupation",
    "candidate_name",
    "office",
    "election_year",
    # identifiers
    "amended",
    "filing_id",
    "raw_file",
    "row_num",
]

# ============================ expenditures ============================
EXPENDITURES = [
    # state
    "state",
    # required
    "committee_name",
    "amount",
    "date",
    "transaction_type",
    # enrichment
    "payee_name",
    "purpose",
    "category",
    "payee_city",
    "payee_state",
    "payee_zip",
    "candidate_name",
    "office",
    "election_year",
    # independent expenditure enrichment -- which candidate this specific
    # expenditure supports or opposes, and the stance ("S"/"O", blank if
    # unknown). Distinct from candidate_name above, which is only populated
    # when the FILING committee is itself that candidate's own committee --
    # an IE committee spending against a candidate never IS that candidate.
    # Sparse: most states' transaction feeds carry no such signal at all
    # (confirmed absent from Oregon's, for example -- see parsers/oregon.py);
    # populated only where a per-state parser has a real source for it.
    "affiliated_candidate_name",
    "support_oppose",
    # identifiers
    "amended",
    "filing_id",
    "raw_file",
    "row_num",
]

# ========================== loans and debts ===========================
LOANS_DEBTS = [
    # state
    "state",
    # required
    "committee_name",
    "original_amount",
    "date",
    "record_type",
    # enrichment
    "counterparty_name",
    "counterparty_city",
    "counterparty_state",
    "counterparty_zip",
    "candidate_name",
    "election_year",
    # identifiers
    "amended",
    "filing_id",
    "raw_file",
    "row_num",
]

# ============================ Column types ============================
# Maps every column name to its DuckDB storage type. Imported by
# tabulate.py and aggregate.py so type enforcement is consistent across
# all states and there is one place to update when the schema changes.
COLUMN_TYPES = {
    # shared / provenance
    "state":                "VARCHAR",
    "person_id":            "BIGINT",
    "state_filer_id":       "VARCHAR",
    "raw_file":             "VARCHAR",
    "row_num":              "BIGINT",
    # committee fields
    "committee_name":       "VARCHAR",
    "committee_type":       "VARCHAR",
    "treasurer_name":       "VARCHAR",
    "city":                 "VARCHAR",
    "zip":                  "VARCHAR",
    "active":               "BIGINT",
    # independent expenditure enrichment (committees + expenditures)
    "affiliated_candidate_name": "VARCHAR",
    "support_oppose":            "VARCHAR",
    # candidate fields
    "candidate_name":       "VARCHAR",
    "canonical_office":     "VARCHAR",
    "candidate_first":      "VARCHAR",
    "candidate_last":       "VARCHAR",
    "office":               "VARCHAR",
    "district":             "VARCHAR",
    "jurisdiction":         "VARCHAR",
    "party":                "VARCHAR",
    "election_year":        "BIGINT",
    "incumbent":            "VARCHAR",
    # transaction fields
    "amount":               "DOUBLE",
    "original_amount":      "DOUBLE",
    "date":                 "DATE",
    "transaction_type":     "VARCHAR",
    "transaction_category": "VARCHAR",
    "record_type":          "VARCHAR",
    "filing_id":            "VARCHAR",
    "amended":              "VARCHAR",  # inconsistent across states (0/1 vs Y/N vs empty) — can't safely use BIGINT
    # contributor fields
    "contributor_name":     "VARCHAR",
    "contributor_type":     "VARCHAR",
    "contributor_city":     "VARCHAR",
    "contributor_state":    "VARCHAR",
    "contributor_zip":      "VARCHAR",
    "employer":             "VARCHAR",
    "occupation":           "VARCHAR",
    # payee fields
    "payee_name":           "VARCHAR",
    "purpose":              "VARCHAR",
    "category":             "VARCHAR",
    "payee_city":           "VARCHAR",
    "payee_state":          "VARCHAR",
    "payee_zip":            "VARCHAR",
    # loans/debts
    "counterparty_name":    "VARCHAR",
    "counterparty_city":    "VARCHAR",
    "counterparty_state":   "VARCHAR",
    "counterparty_zip":     "VARCHAR",
}

# ====================== Aggregate-DB column lists ======================
# Used by aggregate.py when building state-level-cf.db.
# Drops state_filer_id (inconsistent across states — kept per-state only, for
# traceability) and person_id (retired from the master db: the id_model split
# — person/committee/name_hash — made it an unreliable cross-cycle identity in
# practice; see docs. Identification in the master db is by candidate_name /
# committee_name / state instead. state_filer_id still lives in the per-state
# DBs for states where it's a real, meaningful ID.)

CANDIDATES_AGG    = [c for c in CANDIDATES if c not in ("state_filer_id", "person_id")]
COMMITTEES_AGG    = [c for c in COMMITTEES if c not in ("state_filer_id", "active", "person_id")]

# CONTRIBUTIONS_AGG adds transaction_category (derived normalization) after transaction_type.
# contributor_type is kept but aggregate.py replaces raw values with canonical ones.
_contrib = list(CONTRIBUTIONS)
_contrib.insert(_contrib.index("transaction_type") + 1, "transaction_category")
CONTRIBUTIONS_AGG = _contrib

# EXPENDITURES_AGG drops both category (inconsistent per state) and transaction_type
# (raw; preserved in per-state DBs). Adds transaction_category (derived normalization)
# in their place after date. aggregate.py computes it inline from transaction_type.
_expend = [c for c in EXPENDITURES if c not in ("category", "transaction_type")]
_expend.insert(_expend.index("date") + 1, "transaction_category")
EXPENDITURES_AGG  = _expend

LOANS_DEBTS_AGG   = LOANS_DEBTS     # no state_filer_id to drop
