# Indiana (IN)

## Overview

| | |
|---|---|
| **State** | Indiana (IN) |
| **Source** | [Indiana Campaign Finance System](https://campaignfinance.in.gov/PublicSite/) — bulk ZIP downloads (transactions) + HTML entity sweep (committees/candidates) |
| **Access method** | Direct unauthenticated ZIP downloads; sequential `requests` GET over `CommitteeDetail.aspx?OrgId=N` (no Playwright needed) |
| **Coverage** | 2000 – present |
| **person_id model** | `committee` — `org_id` is per-registration; `person_id` = min ID for a given `(candidate_name, office, district)` |

## Raw Data Structure

### Transaction Files — one per year per type

`contributions_{YYYY}.csv` and `expenditures_{YYYY}.csv`, 2000–present. Unzipped from the bulk download endpoint at parse time; stored as plain CSV in `raw/`.

#### contributions_{YYYY}.csv

| Field | Description |
|---|---|
| `FileNumber` | Matches `org_id` in entities.csv — joins to committee/candidate registry |
| `CommitteeType` | e.g. "Regular Party", "Candidate", "PAC" |
| `Committee` | Committee name as filed |
| `CandidateName` | Candidate name (may be blank for PAC/party committees) |
| `ContributorType` | e.g. "Individual", "Corporation", "LLC", "Political Action Committee" |
| `Name` | Contributor name |
| `Address` | Contributor street address |
| `City` / `State` / `Zip` | Contributor address components |
| `Occupation` | Contributor occupation |
| `Type` | Transaction type: "Direct", "In-Kind", "Loan", "Debt", "Debt - Debts Owed by this Committee", "Debt - Debts Owed to this Committee" — loan/debt types routed to loans_debts |
| `Description` | Free-text transaction description |
| `Amount` | Dollar amount (e.g. `"2500.0000"`) |
| `ContributionDate` | Date as `YYYY-MM-DD HH:MM:SS` |
| `Received_By` | Name of treasurer who received the contribution |
| `Amended` | `0`/`1` |

#### expenditures_{YYYY}.csv

| Field | Description |
|---|---|
| `FileNumber` | Matches `org_id` in entities.csv |
| `CommitteeType` | Committee type |
| `Committee` | Committee name |
| `CandidateName` | Candidate name |
| `ExpenditureCode` | Standardized category code (e.g. "Contributions", "Media", "Loan Payment") — written as `category`; "Loan Payment" rows routed to loans_debts |
| `Name` | Payee name |
| `Address` | Payee street address |
| `City` / `State` / `Zip` | Payee address components |
| `Occupation` | Payee occupation |
| `OfficeSought` | Office sought by the associated candidate |
| `ExpenditureType` | Expenditure type label (e.g. "Direct - Contributions", "Direct - Operating") — written as `transaction_type` |
| `Description` | Free-text description |
| `Purpose` | Purpose of expenditure |
| `Amount` | Dollar amount |
| `Expenditure_Date` | Date as `YYYY-MM-DD HH:MM:SS` |
| `Amended` | `0`/`1` |

### Entity Registry — one full pull

`entities.csv` — one row per registered committee or candidate, produced by the OrgId sweep. Candidates appear as "Candidate" committee_type rows with the candidate-specific fields populated.

| Field | Description |
|---|---|
| `org_id` | Sequential numeric ID from `CommitteeDetail.aspx?OrgId=N` — used as `state_filer_id` |
| `committee_type` | e.g. "Candidate", "Regular Party", "PAC", "Exploratory", "Legislative Caucus", "Small Donor" |
| `committee_name` | Full committee name |
| `abbrev_name` | Abbreviated committee name |
| `address1` / `address2` | Street address lines |
| `city_state_zip` | City, state, zip as a single string (e.g. `"Indianapolis IN 46204"`) |
| `party` | Party affiliation |
| `phone` / `fax` | Contact numbers |
| `status` | "Active" or "Disbanded" |
| `date_organized` / `date_terminated` | Lifecycle dates |
| `registered_fec` | Whether registered with FEC |
| `purpose` | Committee purpose statement |
| `affiliations` | Affiliated organizations |
| `supports_entire_ticket` / `supports_party` | Ticket/party support flags |
| `public_question` / `question_position` | Ballot measure fields |
| `candidate_name` | Candidate name (Candidate-type committees only; blank for ~520 older disbanded committees — see Data Notes) |
| `county` | Candidate's county |
| `exploratory` | Exploratory committee flag |
| `district` | District number |
| `office` | Office sought |
| `bank_depositories` | Bank name(s) |
| `treasurer_name` / `treasurer_phone` | Treasurer contact |
| `scraped_at` | Date the page was fetched |

## Scraper

`src/pipeline/scrapers/indiana.py` — pure `requests`, no Playwright needed.

**Transactions:** GET `https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/{year}_{Contribution|Expenditure}Data.csv.zip` for each year 2000–present. Response is a ZIP containing one CSV. Handles BOM/encoding variants common in .NET exports (UTF-8 BOM, UTF-16 LE/BE). Tracked in `manifest.csv`; prior years skipped unless `--force` or `--start-year` specified. Current year always re-fetched.

**Entities:** Sequential GET over `CommitteeDetail.aspx?OrgId=N` from 1 to auto-detected max (~8,400 as of 2026-06-14). Max OrgId is found via binary search anchored at `KNOWN_MAX_ORG_ID = 8400`. Invalid/unused OrgIds return a page with all info spans empty — detected via blank `CommitteeName`/`CommitteeID` spans. 8 parallel workers via `ThreadPoolExecutor`. Checkpointed to `entities.checkpoint` every 50 fetches; safe to interrupt and resume.

**Limitations:**
- Entity sweep requires ~8,400 HTTP requests even at 8 workers — takes significant time; run locally, not in time-limited sandboxes
- No WAF issues observed; standard browser User-Agent used as a precaution
- `OrgId = 1` is a "System Application" placeholder, not a real committee; written to entities.csv and filtered in the parser by committee_type

## Parser

`src/pipeline/parsers/indiana.py`. `id_model = "committee"`: `state_filer_id` = `org_id` from the entity sweep; `person_id` grouped by `(state, candidate_name, office, district)`.

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz`, `committees.csv.gz`, `candidates.csv.gz`

**Key transformations:**
- `FileNumber` in transactions joins to `org_id` in entities for `committee_name`/`candidate_name`/`office` enrichment. Falls back to the transaction row's own `Committee`/`CandidateName` fields where the join fails.
- **Loan routing** — contributions with `Type` in `{Loan, Debt, Debt - Debts Owed by this Committee, Debt - Debts Owed to this Committee}` → `loans_debts` with `record_type = "loan"` or `"debt"`; expenditures with `ExpenditureCode == "Loan Payment"` → `loans_debts` with `record_type = "loan_payment"`.
- **Candidate name recovery** — ~520 old "Candidate"-type entities have a blank `candidate_name` on the source site (source-data gap, verified live). Recovery runs in two passes: (1) scan all transaction files for the most-common non-blank `CandidateName` for that `FileNumber` (~83% coverage); (2) heuristic regex extraction from `committee_name` for the remainder (patterns: "FRIENDS OF X", "COMMITTEE TO ELECT X", "X FOR STATE SENATE", "X COMMITTEE", etc., applied iteratively). Fallback: the committee_name itself (safe for person_id grouping even when not a real candidate name).
- Dates from `ContributionDate`/`Expenditure_Date` in format `YYYY-MM-DD HH:MM:SS` → `YYYY-MM-DD`.
- `election_year` set to the file's year, not a transaction-level field (Indiana's bulk files have no election year column).
- `ExpenditureCode` written as `category` on expenditure rows.
- `employer` always blank — contribution files have no employer column.
- `active` derived from `status` (`"Active"` → 1, `"Disbanded"` → 0).
- `city`/`zip` parsed from `city_state_zip` via regex (`"CITY ST ZIP"` format).
- **`get_amended()` edge case** — some 2024 rows have an unescaped quote in `Received_By` (e.g. `'Raymond (Butch") L. Kramer'`) that shifts `Amended` into a `None` key in `csv.DictReader`. The parser checks `row.get(None)` as a fallback.

**person_id model:** `committee` — `org_id` is per-registration; the same candidate running in different cycles gets a new `org_id`. `assign_person_ids(id_model="committee")` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(org_id)` prefixed with Indiana's FIPS code (18), producing 14-digit integers.

**Limitations:**
- `employer` always blank — not in source data
- `election_year` is the file year, not a transaction-level date — contributions from late in an election cycle are correctly year-stamped but lose any within-year cycle precision

## Data Notes

- **~520 Candidate-type entities with blank `candidate_name`** — a source-data gap on the live `CommitteeDetail.aspx` pages (verified for OrgId 587 and others), not a scraper bug. All are status="Disbanded", registered from the 1970s–2020s. The parser recovers a name for ~83% via transaction file scan and the remainder via committee-name heuristics. See parser section for detail.
- **No employer field** — Indiana's bulk contribution files carry `Occupation` but not `Employer`. `employer` is always blank.
- **No separate loans file** — loan and debt activity is embedded within the contribution and expenditure bulk files as `Type`/`ExpenditureCode` values. `loans_debts.csv.gz` is populated entirely from those routed rows.
- **`OrgId = 1` is a system placeholder** — "System Application" at 123 Main Street, Brownsburg IN. Written to entities.csv by the scraper; committee_type "System" is not mapped in `committee_types.csv` and will appear as-is.
- **`election_year` is file-year only** — the bulk downloads carry no election-cycle column. `election_year` equals the year in the filename, not a cycle the candidate registered for.
- **Earmarked contributions double-counted** — donations made through conduit committees (ActBlue Indiana, WinRed Indiana, etc.) are reported twice by ICFS: once as a receipt to the conduit (e.g. "ACTBLUE INDIANA") and once as a pass-through to the final recipient (e.g. "WELLS FOR INDIANA"), same contributor, same amount, same date. This is source-data behavior, not a parser bug, and is not specific to any one conduit. Aggregate totals for individual donors or for candidate committees will be inflated if conduit rows aren't excluded. No deduplication is applied by the parser.
- **Negative amounts present** — some contribution and expenditure rows have negative amounts (refunds/reversals). Not filtered.

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-14 |
| Parser | 2026-06-15 |
