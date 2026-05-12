-- =============================================================================
-- State-Level Campaign Finance Database Schema
-- =============================================================================
-- Design principles:
--   • Every table carries a `state` column (2-letter abbreviation) so the full
--     database can be queried cross-state without ambiguity.
--   • `state_*_id` columns preserve the original ID from each state's system
--     for traceability and intra-state joins; they are NOT globally unique.
--   • Fields below the "near-universal" line are nullable — present when the
--     state provides them, NULL when it doesn't.
--   • `raw_file` + `row_num` on every transaction table let you trace any row
--     back to the exact source file and line.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- CANDIDATES
-- Dimension table. One row per candidate per election cycle.
-- Not all filers are candidates (PACs, parties, ballot measures aren't),
-- so this table is smaller than committees.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS candidates (
    id                  INTEGER PRIMARY KEY,

    -- Geography / identity
    state               TEXT    NOT NULL,           -- e.g. 'CA', 'AK'
    state_candidate_id  TEXT,                       -- state's own ID, nullable

    -- Name (some states split, some don't)
    candidate_name      TEXT    NOT NULL,           -- full name, normalized
    first_name          TEXT,
    last_name           TEXT,

    -- Race
    office              TEXT,                       -- 'Governor', 'State Senate', etc.
    district            TEXT,                       -- '14', 'SD-5', etc.
    party               TEXT,                       -- 'Democrat', 'Republican', etc.
    election_year       INTEGER,

    -- Status
    incumbent           INTEGER,                    -- 0/1 boolean
    status              TEXT,                       -- 'Won', 'Lost', 'Withdrawn', etc.

    -- Lineage
    raw_file            TEXT,
    row_num             INTEGER
);

CREATE INDEX IF NOT EXISTS idx_candidates_state       ON candidates(state);
CREATE INDEX IF NOT EXISTS idx_candidates_name        ON candidates(candidate_name);
CREATE INDEX IF NOT EXISTS idx_candidates_office_year ON candidates(state, office, election_year);


-- ---------------------------------------------------------------------------
-- COMMITTEES
-- Dimension table. One row per registered filer.
-- PACs, party committees, and ballot measure committees are included here
-- with candidate_id = NULL. Candidate committees link to candidates.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS committees (
    id                  INTEGER PRIMARY KEY,

    -- Geography / identity
    state               TEXT    NOT NULL,
    state_filer_id      TEXT,                       -- state's own filer ID
    candidate_id        INTEGER REFERENCES candidates(id),  -- NULL for non-candidate cmtes

    -- Committee details
    committee_name      TEXT    NOT NULL,
    committee_type      TEXT,                       -- 'Candidate', 'PAC', 'Party',
                                                    -- 'Ballot Measure', etc.
    party               TEXT,

    -- Contact
    treasurer_name      TEXT,
    address             TEXT,
    city                TEXT,
    committee_state     TEXT,                       -- committee's own state (usually = state)
    zip                 TEXT,
    phone               TEXT,

    -- Registration
    registration_date   TEXT,                       -- YYYY-MM-DD
    termination_date    TEXT,                       -- YYYY-MM-DD, nullable
    active              INTEGER,                    -- 0/1 boolean

    -- Lineage
    raw_file            TEXT,
    row_num             INTEGER
);

CREATE INDEX IF NOT EXISTS idx_committees_state       ON committees(state);
CREATE INDEX IF NOT EXISTS idx_committees_filer_id    ON committees(state, state_filer_id);
CREATE INDEX IF NOT EXISTS idx_committees_name        ON committees(committee_name);
CREATE INDEX IF NOT EXISTS idx_committees_candidate   ON committees(candidate_id);


-- ---------------------------------------------------------------------------
-- CONTRIBUTIONS
-- Transactional table. Money / in-kind goods flowing IN to a committee.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contributions (
    id                  INTEGER PRIMARY KEY,

    -- Which state + committee received this
    state               TEXT    NOT NULL,
    committee_id        INTEGER REFERENCES committees(id),
    state_filer_id      TEXT,                       -- denormalized for fast lookup
    committee_name      TEXT,                       -- denormalized for convenience

    -- Near-universal fields
    contributor_name    TEXT,                       -- full name or org name
    amount              REAL    NOT NULL,
    date                TEXT    NOT NULL,           -- YYYY-MM-DD
    transaction_type    TEXT,                       -- 'Monetary', 'In-Kind', 'Anonymous', etc.

    -- Common but not guaranteed
    contributor_type    TEXT,                       -- 'Individual', 'Organization',
                                                    -- 'PAC', 'Party', 'Self', etc.
    contributor_city    TEXT,
    contributor_state   TEXT,
    contributor_zip     TEXT,
    employer            TEXT,
    occupation          TEXT,

    -- Flat-file states (candidate embedded directly on transaction row)
    candidate_name      TEXT,                       -- NULL when joined via committee
    office              TEXT,

    -- Cycle / filing context
    election_year       INTEGER,
    cycle               TEXT,                       -- e.g. '2023-2024' for 2-year cycles
    filing_id           TEXT,                       -- state's filing/report ID
    state_transaction_id TEXT,                      -- state's own transaction ID
    amended             INTEGER,                    -- 0/1 boolean

    -- Lineage
    raw_file            TEXT    NOT NULL,
    row_num             INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_contributions_state        ON contributions(state);
CREATE INDEX IF NOT EXISTS idx_contributions_committee    ON contributions(committee_id);
CREATE INDEX IF NOT EXISTS idx_contributions_date         ON contributions(date);
CREATE INDEX IF NOT EXISTS idx_contributions_contributor  ON contributions(contributor_name);
CREATE INDEX IF NOT EXISTS idx_contributions_amount       ON contributions(amount);
CREATE INDEX IF NOT EXISTS idx_contributions_year         ON contributions(state, election_year);


-- ---------------------------------------------------------------------------
-- EXPENDITURES
-- Transactional table. Money flowing OUT of a committee.
-- Mirrors contributions but contributor → payee, and employer/occupation
-- replaced by purpose/category.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS expenditures (
    id                  INTEGER PRIMARY KEY,

    -- Which state + committee made this payment
    state               TEXT    NOT NULL,
    committee_id        INTEGER REFERENCES committees(id),
    state_filer_id      TEXT,
    committee_name      TEXT,                       -- denormalized for convenience

    -- Near-universal fields
    payee_name          TEXT,
    amount              REAL    NOT NULL,
    date                TEXT    NOT NULL,           -- YYYY-MM-DD
    transaction_type    TEXT,                       -- 'Monetary', 'In-Kind', etc.

    -- Common but not guaranteed
    payee_type          TEXT,                       -- 'Vendor', 'Individual', 'PAC', etc.
    payee_city          TEXT,
    payee_state         TEXT,
    payee_zip           TEXT,
    purpose             TEXT,                       -- description of what it was for
    category            TEXT,                       -- standardized category if state provides

    -- Flat-file states
    candidate_name      TEXT,
    office              TEXT,

    -- Cycle / filing context
    election_year       INTEGER,
    cycle               TEXT,
    filing_id           TEXT,
    state_transaction_id TEXT,
    amended             INTEGER,                    -- 0/1 boolean

    -- Lineage
    raw_file            TEXT    NOT NULL,
    row_num             INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_expenditures_state       ON expenditures(state);
CREATE INDEX IF NOT EXISTS idx_expenditures_committee   ON expenditures(committee_id);
CREATE INDEX IF NOT EXISTS idx_expenditures_date        ON expenditures(date);
CREATE INDEX IF NOT EXISTS idx_expenditures_payee       ON expenditures(payee_name);
CREATE INDEX IF NOT EXISTS idx_expenditures_amount      ON expenditures(amount);
CREATE INDEX IF NOT EXISTS idx_expenditures_year        ON expenditures(state, election_year);


-- ---------------------------------------------------------------------------
-- LOANS_DEBTS
-- Obligations — loans received (money in, must be repaid) and debts owed
-- (obligations out, not yet paid). Spottier coverage than contributions/
-- expenditures; many nullable fields expected.
--
-- `record_type` distinguishes loans from debts:
--   'loan'  — committee borrowed money (from candidate, bank, individual, etc.)
--   'debt'  — committee owes money to a vendor/creditor
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS loans_debts (
    id                  INTEGER PRIMARY KEY,

    -- Which state + committee
    state               TEXT    NOT NULL,
    committee_id        INTEGER REFERENCES committees(id),
    state_filer_id      TEXT,
    committee_name      TEXT,                       -- denormalized for convenience
    record_type         TEXT    NOT NULL,           -- 'loan' or 'debt'

    -- Counterparty (lender for loans, creditor for debts)
    counterparty_name   TEXT,
    counterparty_type   TEXT,                       -- 'Individual', 'Bank', 'Self', 'Vendor'
    counterparty_city   TEXT,
    counterparty_state  TEXT,
    counterparty_zip    TEXT,

    -- Amounts
    original_amount     REAL,
    date                TEXT,                       -- YYYY-MM-DD, origination date
    due_date            TEXT,                       -- YYYY-MM-DD, nullable
    amount_repaid       REAL,                       -- cumulative repayments
    outstanding_balance REAL,                       -- original - repaid

    -- Loan-specific
    interest_rate       REAL,                       -- nullable, percentage
    collateral          TEXT,                       -- nullable

    -- Cycle / filing context
    election_year       INTEGER,
    cycle               TEXT,
    filing_id           TEXT,
    state_transaction_id TEXT,
    amended             INTEGER,

    -- Lineage
    raw_file            TEXT    NOT NULL,
    row_num             INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_loans_debts_state     ON loans_debts(state);
CREATE INDEX IF NOT EXISTS idx_loans_debts_committee ON loans_debts(committee_id);
CREATE INDEX IF NOT EXISTS idx_loans_debts_type      ON loans_debts(record_type);
CREATE INDEX IF NOT EXISTS idx_loans_debts_year      ON loans_debts(state, election_year);
