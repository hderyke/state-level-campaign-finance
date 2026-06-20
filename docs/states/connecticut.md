# Connecticut — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Connecticut (CT) |
| **Source** | [SEEC eCRIS Data Download Page](https://seec.ct.gov/portal/ecris/CurPreYears) |
| **Access method** | Bulk CSV/XLSX download (transactions) + HTML page scrape by numeric ID (committees) |
| **Coverage** | 2010 – present (transactions); 1999 – present (committee history) |
| **person_id model** | `committee` — new `Committee ID` per registration cycle; `person_id` = min ID for a given `(candidate_name, office, district)` |

---

## Raw Data Structure

Two categories: bulk transaction CSVs and a committee history file.

### Transaction Files

Four file types per year, named `{relation_type}_{year}.csv`:

| Filename pattern | Content |
|---|---|
| `receipts_calendar_partypac_{year}.csv` | Contributions to Party and PAC committees |
| `receipts_election_candidateexploratory_{year}.csv` | Contributions to Candidate and Exploratory committees |
| `disbursements_calendar_partypac_{year}.csv` | Expenditures by Party and PAC committees |
| `disbursements_election_candidateexploratory_{year}.csv` | Expenditures by Candidate and Exploratory committees |

#### Receipt files (both types share these core columns)

| Field | Description |
|---|---|
| `Committee` | Name of the filing committee |
| `Committee ID` | Unique numeric ID for the committee (absent in 2010–2013 files) |
| `Committee Type` | e.g. "Candidate Committee", "Political Action Committee" |
| `Contributor Name` | Combined contributor name |
| `Contributor First Name` / `Contributor Middle Initial` / `Contributor Last Name` | Split name fields (absent in 2010–2013) |
| `Receipt Type` | Transaction classification (e.g. "Itemized Contributions from Individuals", "In Kind Contribution", "Loans Received") |
| `Transaction Date` | Date of the contribution (MM/DD/YYYY; empty for some 2010–2013 rows) |
| `File To State` | Date the report was filed — used as fallback when `Transaction Date` is missing |
| `Amount` | Dollar amount |
| `Street Address` / `City` / `State` / `zip` | Contributor address (`State` has heavy trailing whitespace; `zip` has trailing dash) |
| `Employer` / `Occupation` | Contributor employment (present from 2014+) |
| `ElectionYear` | Election cycle year |
| `Contractor` / `Lobbyist` | Boolean flags for contractor/lobbyist status |
| `Data Source` | "eFILE" or "Data Entry" (paper filings entered by SEEC staff) |
| `Refiled Electronically` | YES/NO — paper report later re-entered electronically |
| `Report ID` | Filing report identifier (absent in 2010–2013) |

Candidate/Exploratory receipt files additionally include:

| Field | Description |
|---|---|
| `Candidate First Name` / `Candidate Middle Intial` / `Candidate Last Name` | Candidate name [sic: "Intial" is SEEC's typo] |
| `District` | District number |
| `Office Sought` | Office being sought |
| `Description` | Free-text note |

#### Disbursement files

| Field | Description |
|---|---|
| `Committee` | Name of the filing committee (`"Committee "` with trailing space in 2010 files — stripped by parser) |
| `Committee ID` | Numeric ID (absent in 2010–2013) |
| `Committee Type` | Committee type |
| `Payee` | Payee name |
| `Purpose of Expenditure` | Code + description (e.g. "CNTRB(Contributions to another committee)") |
| `Description` | Free-text note |
| `Payment Date` | Date of payment (MM/DD/YYYY; "NULL" for some 2010-era paper-filed rows) |
| `Amount` | Dollar amount |
| `Status` | "Original" or "Amendment" — used as the amended flag |
| `Street Address` / `City` / `State` | Payee address (no zip column in disbursements) |
| `Election Year` | Election cycle year |
| `Report ID` | Filing report identifier (absent in 2010–2013) |

### Committee History File

`committee_history.csv` — scraped from `CommitteeHistory.aspx?c={id}` pages.

| Field | Description |
|---|---|
| `committee_id` | Numeric ID matching `Committee ID` in transaction files |
| `committee_name` | Official registered name |
| `committee_type` | e.g. "Candidate Committee", "Political Action Committee" |
| `committee_subtype` | e.g. "Two or More Individuals" (PAC subtypes only) |
| `status` | "ACTIVE" or "TERMINATED On MM/DD/YYYY HH:MM:SS AM" |
| `address` | Street address from most recent registration |
| `city` / `state` / `zip` | Address components |
| `candidate_name` | Officer listed as "Candidate" (Candidate/Exploratory committees) |
| `chairperson_name` | Officer listed as "Chairperson" (Party/PAC committees) |
| `treasurer_name` | Officer listed as "Treasurer" |
| `downloaded_at` | Date the page was scraped |

---

## Scraper

`src/pipeline/scrapers/connecticut.py`

**Transactions:** Plain HTTP GET requests to predictable URLs on the SEEC download page. No authentication or Playwright required. 2022–2023 files are published as XLSX only (CSV returns 404); those are downloaded and converted to CSV via `openpyxl`. All other years have CSV directly. Current year is always re-fetched.

**Entities:** Sweeps `CommitteeHistory.aspx?c={id}` from `id=1` to `id=14690` (observed ceiling as of 2026-05). Valid pages are detected by the presence of a `PanelHistory` div. The most recent registration (ctl01 in the repeater) is parsed using regex against ASP.NET span IDs. Incremental runs start from `max_done_id - 200` to skip already-scraped low IDs and push through to the active region near the ceiling. `MAX_CONSECUTIVE_BLANK = 1500` to bridge the gap above ID ~6,000 where the ID space becomes sparse.

**Limitations:**
- The committee ID space has a large sparse region between ~6,000 and ~10,000 that requires a high consecutive-blank tolerance to bridge
- Pre-2014 transaction files have no `Committee ID` column, so those committees are only linkable to history via committee name string matching
- 2022–2023 XLSX conversion requires `openpyxl` (`pip install openpyxl`)

**Expected runtime:** Transactions ~5 min (68 files). Full entity sweep ~45–60 min (14,690 IDs at 0.2s/request). Incremental entity update ~20–30 min once initial sweep is complete.

---

## Parser

`src/pipeline/parsers/connecticut.py`

CT is a flat-file state for transactions: `Committee`, `Committee Type`, and `Committee ID` appear on every transaction row. The parser synthesizes the committees table from these fields, then enriches it with committee history data.

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `candidates.csv.gz`, `committees.csv.gz`, `loans_debts.csv.gz` (empty — CT has no separate loan table)

**Key transformations:**
- `Transaction Date` normalized from MM/DD/YYYY; XLSX-converted 2022–2023 files produce "YYYY-MM-DD HH:MM:SS" datetimes (time portion truncated). Rows with no `Transaction Date` fall back to `File To State` (report filing date) — affects ~33K rows in 2010–2017.
- `State` field stripped of heavy trailing whitespace; `zip` stripped of trailing dash
- Contributor name assembled from First/Middle/Last split fields (2014+) or combined `Contributor Name` field (2010–2013)
- `contributor_type` mirrors `transaction_type` (Receipt Type) so the `contributor_types.csv` aliases can normalize it
- 2010 disbursements header has `"Committee "` (trailing space) — stripped via fieldnames normalization on `DictReader`
- Disbursements `Status` (Original/Amendment) → `amended` flag; receipts `Refiled Electronically` (YES/NO) → `amended` flag
- Committees table built in two passes: (1) ID-keyed entries from 2014+ files enriched by history ID lookup, (2) name-keyed entries from 2010–2013 files enriched by history name match — only written when a valid `committee_id` resolves to avoid empty `state_filer_id`
- `candidate_name` in committees is populated from transaction data (`candidate_by_cid` lookup) as primary source, falling back to history page data — ensures 2014+ candidate committees are linked even when history sweep is incomplete

**person_id model:** `committee` — SEEC assigns a new `Committee ID` per registration cycle, so the same candidate gets a different ID each cycle. `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(state_filer_id)` across all registrations.

**Limitations:**
- Pre-2014 candidate committees have no candidate name in the source files (`Candidate First/Last Name` columns were added in 2014). These committees get `candidate_name` only if the committee history sweep covered their ID.
- Some 2010-era "Data Entry" disbursement rows have column misalignment — the `City` field contains a zip code and `State` contains the payment date. Affects ~3,355 rows; left as-is (source data artifact).
- `payee_zip` is always empty — CT disbursement files don't include a payee zip column.
- `party`, `jurisdiction`, `incumbent` are not available in the source data.

**Expected runtime:** ~55 sec (68 source files + committee registry build).

---

## Data Notes

- **Citizens' Election Program (CEP):** CT's public financing system. Qualifying candidates receive large state grants recorded as `"Public Grants"` in `Receipt Type`. These appear as contributions to the committee with `contributor_name` blank or set to the state. Not a personal contribution.
- **Self-financing:** CT allows non-CEP candidates to contribute personal funds. "Personal Funds of Candidate" and "Expenses Paid by Candidate" receipt types are genuine candidate self-contributions, not data errors. Norm Needleman (State Senator) is a prominent example with multiple $100K+ self-contributions.
- **Exploratory committees:** Separate from candidate committees. An exploratory committee raises seed money; if the candidate qualifies for CEP, they transfer proceeds to the candidate committee and the exploratory terminates. Both committee types appear in the data.
- **Candidate/Exploratory file sizes vary sharply by year:** Files for even-numbered years (election cycles) are typically 5–10x larger than odd-numbered years. 2018 candidate/exploratory receipts alone total 186,470 rows.
- **2022–2023 XLSX conversion:** SEEC stopped publishing CSVs for these two years; XLSX files are downloaded and converted. Dates in XLSX cells are serialized by openpyxl as "YYYY-MM-DD HH:MM:SS" — the time portion is stripped during parsing.
- **Free-text expenditure codes:** CT's expenditure `Purpose of Expenditure` field is a free-text code+description (e.g. "PRNT(Cost of printing...)"). Committees frequently abbreviate or mistype codes, producing hundreds of distinct values. Only the top ~40 codes are mapped in `expenditure_categories.csv`; the rest are NULL in the aggregate.
- **"Data Entry" rows (2010-era):** Paper-filed reports entered manually by SEEC staff. Some of these rows have misaligned address columns (zip code in City, payment date in State). Not fixable without original documents.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-30 |
| Parser | 2026-05-30 |
