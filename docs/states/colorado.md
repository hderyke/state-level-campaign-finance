# Colorado — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Colorado (CO) |
| **Source** | [Colorado TRACER System](https://tracer.sos.colorado.gov/PublicSite/) |
| **Access method** | Bulk ZIP downloads (transactions) + HTML page sweep (candidates + committees) |
| **Coverage** | 2000 – present (transactions); 1990s – present (candidates/committees) |
| **person_id model** | `person` — `candidate_id` (CO_ID) is a stable person-level ID that persists across cycles; `person_id` set directly |

---

## Raw Data Structure


### Transaction Files

Three file types per year, named `{type}_{year}.csv`:

#### contributions_{year}.csv
| Field | Description |
|---|---|
| `CO_ID` | Filer's person-level stable ID (same across all registrations for that person) |
| `ContributionAmount` | Dollar amount |
| `ContributionDate` | Date of contribution (`YYYY-MM-DD HH:MM:SS` in raw) |
| `LastName` / `FirstName` / `MI` / `Suffix` | Contributor name parts (org name in `LastName` when `FirstName` is blank) |
| `Address1` / `Address2` / `City` / `State` / `Zip` | Contributor address |
| `Explanation` | Free-text description |
| `RecordID` | Unique transaction ID |
| `FiledDate` | Date the report was filed |
| `ContributionType` | e.g. `"Monetary (Itemized)"`, `"Monetary (Itemized) - LLC Contribution (Total Amount: 625.00)"` |
| `ReceiptType` | Payment method (e.g. `"Check"`, `"Credit/Debit Card"`) |
| `ContributorType` | e.g. `"Individual"`, `"Business"`, `"Individual (Member of LLC: XYZ LLC)"` |
| `CommitteeType` / `CommitteeName` | Recipient committee |
| `CandidateName` | Associated candidate's full name in "FIRST LAST" format (blank for non-candidate committees) |
| `Employer` / `Occupation` / `OccupationComments` | Contributor employment info |
| `Amended` / `Amendment` / `AmendedRecordID` | Amendment tracking |
| `Jurisdiction` | e.g. `"STATEWIDE"`, `"DENVER"` |

#### expenditures_{year}.csv
Same structure as contributions with `ExpenditureAmount`, `ExpenditureDate`, `ExpenditureType`, `DisbursementType`, `PaymentType` in place of contribution-specific fields. `DisbursementType` is more granular than `ExpenditureType` and is used as `category` in the cleaned output.

#### loans_{year}.csv
| Field | Description |
|---|---|
| `CO_ID` | Filer's person-level stable ID |
| `Type` | `"O"` = origination, `"P"` = payment/repayment |
| `LoanAmount` / `LoanDate` | Origination details |
| `PaymentAmount` / `PaymentDate` | Repayment details (Type P rows) |
| `LoanBalance` | Remaining balance |
| `LoanSourceType` | e.g. `"Candidate"`, `"Bank"` |
| `InterestRate` / `InterestPayment` | Interest terms |
| `Name` / `Address1` / `City` / `State` / `Zip` | Counterparty address |
| `RecordID` / `FiledDate` / `Amended` / `Amendment` / `AmendedRecordID` | Filing metadata |

### Entity Files

#### candidates_all.csv
Scraped from `CandidateDetail.aspx?SeqID=<n>` for all valid SeqIDs. One row per (SeqID, election cycle). Key fields:

| Field | Description |
|---|---|
| `seq_id` | SeqID on TRACER (not a person-level ID) |
| `candidate_id` | Person-level stable CO_ID — same person keeps this ID across all cycles |
| `name` | Committee name as displayed on TRACER (often "LAST, FIRST" for older registrations, committee name for newer ones) |
| `office` / `district` / `jurisdiction` | Office sought |
| `election_cycle` / `election_year` / `party` | Cycle metadata |
| `cycle_status` / `current_status` | e.g. `"Active"`, `"Inactive"`, `"Terminated"` |
| `address1` / `address2` / `city_state_zip` | Mailing address |
| `voluntary_spending_limit` / `date_affidavit_filed` / `term_date` | Filing metadata |

PACs and non-candidate filers also appear here — they get numeric office codes (`"4"`, `"5"`) instead of real office names like `"Governor"`. The parser filters to entries with alphabetic office names.

#### committees.csv
Scraped from `CommitteeDetail.aspx?OrgID=<n>` for all valid OrgIDs. One row per committee registration.

| Field | Description |
|---|---|
| `org_id` | OrgID on TRACER (sequential, not a stable person ID) |
| `committee_id` | CO_ID — matches `CO_ID` in transaction files and `candidate_id` in candidates_all.csv |
| `committee_name` | Committee name |
| `committee_type` | e.g. `"Candidate"`, `"Candidate Committee"`, `"Issue Committee"`, `"Political Committee"` |
| `status` | `"Active"` or `"Terminated"` |
| `date_registered` / `date_terminated` | Lifecycle dates |
| `registered_agent` / `agent_phone` / `agent_email` | Registered agent (used as treasurer proxy) |
| `address1` / `city_state_zip` | Physical address (`"CITY ST ZIP"` format, no comma) |
| `mail_address1` / `mail_city_state_zip` | Mailing address |
| `dfa` / `dfa_phone` | Designated Filing Agent info |

---

## Scraper

`src/pipeline/scrapers/colorado.py`

**Transactions:** Fetches ZIP files from the TRACER BulkDataDownloads endpoint for each year × file type combination (contributions, expenditures, loans). Extracts the inner CSV, handles UTF-8 BOM and UTF-16 encoding variants. Tracked in `manifest.csv`; current-year files always re-fetched.

**Candidates:** Sweeps `CandidateDetail.aspx?SeqID=<n>` from SeqID 1 to a binary-search-detected max. Uses a rolling ThreadPoolExecutor window (default 8 workers). Resumable via `candidates_all.checkpoint`. One row per (SeqID, election cycle) written to `candidates_all.csv`.

**Committees:** Sweeps `CommitteeDetail.aspx?OrgID=<n>` from OrgID 1 to a binary-search-detected max, same pattern as candidates. Resumable via `committees.checkpoint`.

**Limitations:**
- Entity sweeps are slow (~67K SeqIDs, ~53K OrgIDs at 0.25s/request with 8 workers — several hours each)
- TRACER has no bulk entity export; the page sweep is the only option
- `KNOWN_MAX_SEQ_ID` and `KNOWN_COMM_MAX_ORG_ID` constants need updating periodically (binary search handles drift, but verify after major gaps)

**Expected runtime:** Transactions ~5–10 min. Entity sweeps: several hours each on first run; incremental updates pick up from checkpoint.

---

## Parser

`src/pipeline/parsers/colorado.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz`, `candidates.csv.gz`, `committees.csv.gz`

**Key transformations:**

- **Candidate name resolution:** `candidates_all.csv` stores the committee name in the `name` field, not the person's name. The parser pre-scans transaction files to build a `CO_ID → "FIRST LAST"` name map, then uses that to populate `candidate_name` on candidate rows. Names are stored in "FIRST LAST" format to match the `CandidateName` field in transactions (required for the spot-check query join).

- **LLC member flattening:** Colorado requires LLCs to disclose member contributions individually. Rows with `ContributorType = "Individual (Member of LLC: XYZ LLC)"` and a declared total in `ContributionType` are deduplicated to one row per LLC contribution event, using the declared total as the amount. Rows without a declared total (typically refunds/amendments) are kept individually but normalized to the LLC name.

- **Loans:** Type `"O"` (origination) uses `LoanAmount` and `LoanDate`; Type `"P"` (payment) uses `PaymentAmount` / `PaymentDate`. Both written to `loans_debts` with `record_type` set accordingly.

- **Date normalization:** `YYYY-MM-DD HH:MM:SS` → `YYYY-MM-DD` (time component stripped).

- **Amendment flag:** `"Y"` → `"1"`, `"N"` → `"0"`.

- **Committee `city_state_zip` parsing:** Format is `"CITY ST ZIP"` (no comma) — parsed via regex.

**person_id model:** `person` — `candidate_id` in TRACER is a person-level stable CO_ID that persists across election cycles and committee registrations. `person_id = _make_person_id("CO", candidate_id)`, prefixed with Colorado's FIPS code (08), producing 13-digit integers.

**Committee–candidate linking:** Colorado's `committee_id` == `candidate_id` == CO_ID, so committees are linked to candidates by direct ID lookup (not name-based matching). After `assign_person_ids` stamps `person_id` onto candidates, the parser reads that mapping back and stamps it directly onto matching committee rows. This gives near-100% coverage for candidate committees, compared to ~13% via name-based matching.

**Expected runtime:** ~2–3 min (26 years of transactions + entity files).

---

## Data Notes

- **CandidateName sparsity:** The `CandidateName` field in transaction files is blank for PACs, issue committees, and party committees — only candidate committee filings carry it. Overall fill rate is ~43% for contributions and ~55% for expenditures.

- **LLC member contributions:** ~13,000 contribution rows across all years are individual LLC member disclosures, collapsed to ~8,700 LLC-level rows by the parser. The `ContributionType` field carries the declared total on each member row; the dedup key is `(CO_ID, llc_name, contribution_date, filed_date)`. ~370 rows (mostly refunds) lack a declared total and are kept individually.

- **Election year garbage values (candidates — fixed):** `candidates_all.csv`'s `election_year` is `election_cycle.split()[0]` from `CandidateDetail.aspx`'s campaigns table, which is normally a 4-digit year (e.g. `"2022 General"` → `"2022"`). For 21 candidates running for "(PROPOSED)" metro/water districts — `TACINCALA METROPOLITAN DISTRICT (PROPOSED) ELECTION CYCLE` (10 rows), `LORETTO HEIGHTS PROGRAMMING METROPOLITAN DISTRICT (PROPOSED)...` (5), `PTARMIGAN WEST METROPOLITAN DISTRICT NO. 1 (PROPOSED)...` (2), `VILLAGE AT NORTH CREEK METROPOLITAN DISTRICT (PROPOSED)...` (4) — the cycle label is the district's own name, so `split()[0]` yields `"TACINCALA"`/`"LORETTO"`/`"PTARMIGAN"`/`"VILLAGE"` instead of a year. All 21 are real "METROPOLITAN DISTRICT DIRECTOR" candidates whose proposed districts were ultimately terminated/never formed. Because `election_year` is `BIGINT` in `tabulate.py` and tabulate uses `ignore_errors=true`, a non-numeric value doesn't just leave the cell blank — it silently drops the **entire row** from the table. The same garbage value also gets backfilled onto the linked committee's `election_year` (committees inherit `election_year` from the candidate via `co_id_to_election_year`), so ~20 committee rows were dropped too. This is the source of the validate-vs-tabulate row-count gaps (candidates 52,948→52,927, diff 21; committees 49,845→49,825, diff 20). **Fix:** `parsers/colorado.py` now has `valid_election_year()`, applied to both the candidate row and the `co_id_to_election_year` backfill — non-numeric or out-of-[1990, today+2]-range values become `""` instead of corrupting the row. **Verified fixed** — reparse now tabulates 52,948 candidates and 49,845 committees, matching the validate sample exactly (previously 52,927/49,825, a gap of 21/20). Folded into `state-level-cf.db` via aggregate.

- **Election year garbage values (contributions, 2000–2002 — documented, not fixed):** ~14 contribution/expenditure rows from the earliest TRACER years (2000–2002) have a `ContributionDate`/`ExpenditureDate` with an implausible year (`"1929-..."`, `"2202-..."`, `"2500-..."`, etc.) — e.g. `contributions_2002.csv` rows 259/260 (`ContributionDate` → election_year `"2202"`). `parse_date()` already rejects these (the `date` column is blank for them), but `election_year` is derived independently as `ContributionDate[:4]` *before* that validation, so the bad year leaks through. These look like filer data-entry typos in TRACER's earliest records, not a parsing bug. Volume is negligible (~14 of 71M+23M rows) — not fixed.

- **`contributor_type` column-shift (documented, not fixed):** A small number of `contributions_*.csv` rows (e.g. `"ARAPAHOE COUNTY DEMOCRATIC PARTY"` ×22, `"BOULDER COUNTY DEMOCRATIC PAR…"` ×1, plus a couple more) have a committee name in `ContributorType` instead of a type label (`"Individual"`, `"Political Committee"`, etc.). Root cause: in TRACER's bulk export, a handful of rows have an unescaped/non-doubled quote character inside a name field (e.g. `FirstName` containing `PERVAIZ "PK"`), which breaks Python's `csv` field-boundary parsing and shifts every subsequent column in that row to the right by several positions — `ContributorType` ends up holding what should be `CommitteeName`/`ReceiptType`. Affects ~24 of 71M contribution rows; not worth a targeted repair given the volume.

- **Negative amounts:** Present in all three transaction types, representing reversals, refunds, and corrections. Not filtered out.

- **Duplicate committee names in non-candidate table:** "PROTECTING COLORADO'S ENVIRONMENT, ECONOMY & FAMILIES" appears twice in the non-candidate committees query due to two distinct committees sharing a truncated name prefix in TRACER. These are separate registrations, not duplicates.

- **`incumbent` field:** Not available in TRACER — always blank.

- **`office` field on transactions:** Not available on individual transaction rows — always blank in contributions and expenditures.

- **Pre-2000 records:** A small number of contributions (< 100 rows) have dates before 2000, predating the bulk download coverage window. These appear to be data entry corrections filed after the fact.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-29 |
| Parser | 2026-06-14 |
