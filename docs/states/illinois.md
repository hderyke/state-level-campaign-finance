# Illinois (IL)

## Overview

| | |
|---|---|
| **State** | Illinois (IL) |
| **Source** | [Illinois State Board of Elections (ISBE)](https://elections.il.gov/campaigndisclosuredatafiles/) — bulk TSV data files |
| **Access method** | Direct unauthenticated TSV download (full-history flat files, no API, updated nightly) |
| **Coverage** | 1994 – present (Receipts/Expenditures); registry data current as of download date |
| **person_id model** | `committee` — `state_filer_id` = `Candidates.ID` / `Committees.ID`; `person_id` = min ID for a given `(candidate_name, office, district)` |

## Raw Data Structure

All files are tab-delimited, latin-1 encoded, `QUOTE_NONE`. Each is the full 1994–present history — no year-splitting. `FiledDocs.txt` (~140MB) is out of scope; Receipts/Expenditures carry their own `FiledDocID` (written through as `filing_id`), so no join is needed.

#### Candidates.txt (~32K rows)

| Field | Description |
|---|---|
| `ID` | Candidate ID — used as `state_filer_id` |
| `LastName` / `FirstName` | Candidate name |
| `Address1` / `Address2` / `City` / `State` / `Zip` | Candidate address |
| `Office` / `DistrictType` / `District` | Office and geography |
| `ResidenceCounty` | County of residence |
| `PartyAffiliation` | Party |
| `RedactionRequested` | Redaction flag |

#### Committees.txt (~34K rows)

| Field | Description |
|---|---|
| `ID` | Committee ID — used as `state_filer_id` |
| `TypeOfCommittee` | e.g. "Candidate", "Political Action", "Political Party", "Ballot Initiative", "Independent Expenditure", "Limited Activity Committee"; blank for ~13K committees inferred as Candidate via CmteCandidateLinks |
| `Name` | Committee name |
| `StateCommittee` / `StateID` / `LocalCommittee` / `LocalID` | State/local registration IDs |
| `ReferName` | Referral name |
| `Address1` / `Address2` / `Address3` / `City` / `State` / `Zip` | Committee address |
| `Status` | `A` = active, `F` = final (dissolved) |
| `StatusDate` / `CreationDate` / `CreationAmount` | Lifecycle dates and initial creation amount |
| `DispFundsReturn` / `DispFundsPolComm` / `DispFundsCharity` / `DispFunds95` / `DispFundsDescrip` | Disposition-of-funds fields (not used by parser) |
| `CanSuppOpp` / `PolicySuppOpp` | Candidate/policy support-or-oppose designation |
| `PartyAffiliation` | Party |
| `Purpose` | Stated committee purpose |

#### CmteCandidateLinks.txt (~37K rows)

| Field | Description |
|---|---|
| `ID` | Row ID |
| `CommitteeID` | Links to `Committees.ID` |
| `CandidateID` | Links to `Candidates.ID` |

~24.7K distinct committees have at least one link. 4,538 committees link to more than one candidate (reassigned across cycles) — the parser takes the last link per committee.

#### Receipts.txt (~6.48M rows after Archived filter, ~1.0 GB raw)

`Archived="True"` rows (~41% of raw) are skipped — they are superseded copies re-emitted when a report is amended. `D2Part` values `3A` (Loans Received) are routed to `loans_debts` rather than contributions.

| Field | Description |
|---|---|
| `ID` | Row ID |
| `CommitteeID` | Links to `Committees.ID` |
| `FiledDocID` | Filing document ID — written through as `filing_id` |
| `LastOnlyName` / `FirstName` | Contributor name; org name goes in `LastOnlyName` with `FirstName` blank |
| `RcvDate` | Receipt date |
| `Amount` | Dollar amount |
| `AggregateAmount` | Aggregate amount for the filing period |
| `LoanAmount` | Loan amount (populated on `3A` rows) |
| `Occupation` / `Employer` | Contributor occupation and employer |
| `Address1` / `Address2` / `City` / `State` / `Zip` | Contributor address |
| `D2Part` | Schedule A part code: `1A` individual, `2A` PAC/committee, `3A` loan, `4A` other receipt, `5A` in-kind — used as `transaction_type` |
| `Description` | Free-text description (populated on `5A` in-kind rows) |
| `VendorLastOnlyName` / `VendorFirstName` / `VendorAddress*` / `VendorCity` / `VendorState` / `VendorZip` | Vendor info for in-kind contributions |
| `Archived` | `"True"` = superseded by amendment (skipped); `"False"` = current |
| `Country` | Contributor country |

#### Expenditures.txt (~4.82M rows after Archived filter, ~0.8 GB raw)

`Archived="True"` rows (~30% of raw) are skipped. `D2Part` value `7B` (Loans Made) is routed to `loans_debts`.

| Field | Description |
|---|---|
| `ID` | Row ID |
| `CommitteeID` | Links to `Committees.ID` |
| `FiledDocID` | Filing document ID — written through as `filing_id` |
| `LastOnlyName` / `FirstName` | Payee name |
| `ExpendedDate` | Expenditure date |
| `Amount` / `AggregateAmount` | Dollar amount and aggregate |
| `Address1` / `Address2` / `City` / `State` / `Zip` | Payee address |
| `D2Part` | Schedule B part code: `6B`/`8B` operating expenditure, `7B` loan made, `9B` independent expenditure — used as `transaction_type` |
| `Purpose` | Free-text expenditure purpose |
| `CandidateName` / `Office` / `Supporting` / `Opposing` | Populated on `9B` independent expenditure rows only |
| `Archived` | `"True"` = superseded by amendment (skipped); `"False"` = current |
| `Country` | Payee country |

## Scraper

`src/pipeline/scrapers/illinois.py` — pure `requests`, same Range-based streaming approach as `california.py`. HEAD for `Content-Length`/`Last-Modified`, then resumable streaming GET to `<name>.part` with a `.progress.json` sidecar. `--entities`/`--transactions` flags split scope; `--time-budget` controls how long a single invocation downloads before checkpointing.

**Limitations:**
- `Accept-Encoding: identity` must be set in session headers — Cloudflare otherwise gzip-compresses the response, stripping `Content-Length` and breaking Range resume
- Receipts.txt (~1 GB) and Expenditures.txt (~800 MB) must be downloaded locally with a generous `--time-budget`; not feasible in sandboxed environments

## Parser

`src/pipeline/parsers/illinois.py`. `id_model="committee"`: `state_filer_id` = `Candidates.ID` for candidates, `Committees.ID` for committees; `person_id` is grouped by `(state, candidate_name, office, district)`.

Key transformations:
- Candidates.txt and Committees.txt are direct registry exports (not built incrementally from transactions like Hawaii). CmteCandidateLinks.txt builds a `CommitteeID -> CandidateID` map used to populate each committee's `candidate_name`/`office`, and each contribution/expenditure's `candidate_name`/`office` via the transaction's `CommitteeID`.
- **Committee-type inference**: ~13K of ~19K committees with blank `TypeOfCommittee` ARE linked to a candidate via CmteCandidateLinks — these get `committee_type = "Candidate"` inferred. The remaining ~6K blanks (no link) are left blank.
- **`active`**: derived from `Status` (`A` → `1`, `F` → `0`, blank → blank). 4,388 active, 29,520 final, 1 blank.
- **D2Part** (ISBE's Schedule A/B "part" code) is the closest thing IL has to a `transaction_type` and is passed through raw for most rows:
  - Receipts (Schedule A): `1A` individual (4.8M), `2A` other committee/PAC (1.1M), `4A` other receipts — interest/investment/bank (110K), `5A` in-kind/other receipts with a `Description` (370K).
  - Expenditures (Schedule B): `6B`/`8B` operating expenditures (808K / 3.96M), `9B` independent expenditures (49K) — the only D2Part where `CandidateName`/`Office`/`Supporting`/`Opposing` are populated on the row itself.
  - `3A` (Loans Received, 76,065 rows) and `7B` (Loans Made, 4,853 rows) are diverted to `loans_debts.csv.gz` as `record_type = "Loan Received"` / `"Loan Made"` instead of contributions/expenditures.
- **`contributor_type`** is a heuristic, not a source field: ISBE's D-2 forms only collect `FirstName`+`LastOnlyName` for individuals — PACs/organizations are recorded in `LastOnlyName` alone. So `contributor_type = "Individual"` if `FirstName` is non-blank, else `"Organization"`.
- **9B independent expenditures**: `candidate_name`/`office` come from the row's own `CandidateName`/`Office` (the IE's target), not the committee's linked candidate. `category` records the direction: `"Independent Expenditure - Supporting"` / `"- Opposing"` / `"Independent Expenditure"`.
- **Malformed rows**: ~33/6.48M Receipts rows and ~2/4.82M Expenditures rows have embedded newlines/tabs in free-text fields that break column alignment (field count != header count). Detected via `None in row.values() or row.get(None) is not None` (DictReader's `restval`/`restkey` behavior) and skipped.
- **`Archived="True"` rows are skipped entirely.** ISBE re-emits a fresh copy (new `ID`/`FiledDocID`, `Archived="False"`) of every transaction each time a committee's report is amended; `Archived="True"` marks the prior, superseded copy. ~2.66M/6.48M Receipts rows (41%) and ~1.47M/4.82M Expenditures rows (30%) carry `Archived="True"`. A sample check found 95-96% of these have an exact `(CommitteeID, Amount, date, name)` match among `Archived="False"` rows — confirming they're stale duplicates, not independent transactions. Including them roughly doubled contribution/expenditure totals (e.g. Receipts: $6.43B `False` vs $9.05B `True`).
- A debug-only `--limit N` flag caps rows read from Receipts/Expenditures for smoke testing; omitted in production runs (~4 min for the full 11.3M rows).

## Data Notes

- **Committee→candidate link ambiguity**: 4,538 committees in CmteCandidateLinks.txt link to more than one distinct `CandidateID` (committees occasionally get reassigned to a new candidate across cycles). The parser takes the LAST link encountered per committee, so the most recently-linked candidate "wins" for `committee_name`/`candidate_name`/`office` enrichment on older transactions too. We don't have a per-transaction election year (FiledDocs.txt out of scope) to disambiguate by cycle.
- **Resolved by the `Archived` filter (previously flagged as outliers)**: an `Expenditures.txt` row (CommitteeID 34537, "Friends of Katrina R Thompson") recorded `Amount`/`AggregateAmount = 8105654619` ($8.1B) paid to "EF Design Group, Inc" for "Printing" on 2025-07-18; and three Receipts.txt rows recorded $401,638,558 (ActBlue Illinois), $400,164,048 (Joe Iosbaker → Friends for Celina Villanueva), and $250,068,679 (Realtors PAC → Friends of Don Harmon) with `AggregateAmount = 0`. All four rows have `Archived="True"` with no `Archived="False"` counterpart — i.e. they were draft-filing artifacts that got corrected away in a later amendment, not real transactions. None of these appear in the cleaned output now.
- Top contributors/recipients pass a sanity check against known IL political figures: JB Pritzker (self-funded, by far #1), Ken Griffin, Bruce Rauner, Richard Uihlein, Madigan, etc. all appear where expected. Confirmed clean after the Archived-filter re-run (top 10 contributions show no duplicates: Pritzker $90M/$51.5M/$35M/$25M x2/$20M, Griffin $26.75M/$25M/$20M, Rauner $50M).
- Alias mappings (`src/aliases/{contributor_types,transaction_categories,expenditure_categories,committee_types}.csv`): `contributor_types` (Individual/Organization, both pass through unchanged); `transaction_categories` (Receipts D2Part: 1A/2A→Monetary, 4A→Other, 5A→In-Kind; 3A excluded — loans); `expenditure_categories` (Expenditures D2Part: 6B/8B→Monetary, 9B→Independent Expenditure; 7B excluded — loans); `committee_types` (Candidate→Candidate Committee, Political Action→PAC, Political Party→Party Committee, Ballot Initiative→Ballot Measure, Independent Expenditure→Independent Expenditure, Limited Activity Committee→Other; blank TypeOfCommittee unmapped).

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-13 |
| Parser | 2026-06-13 |
