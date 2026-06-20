# Florida — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Florida (FL) |
| **Source** | [Florida Division of Elections](https://dos.elections.myflorida.com/) |
| **Access method** | HTML page sweep (committees, two-phase) + bulk per-election-cycle downloads (candidates) + Playwright-driven date/election-range queries (transactions) |
| **Coverage** | 1996 – present |
| **person_id model** | `committee` — `AcctNum` is per-registration; `person_id` = min `AcctNum` for a given `(candidate_name, office, district)` |

---

## Raw Data Structure


### Committee Files

#### fl_committee_links.csv (Phase 1 — A-Z name search results)
| Field | Description |
|---|---|
| `account_id` | Committee account number |
| `name` | Committee name |
| `type` | Committee type code |
| `status` | Active/Closed — authoritative status used by the parser |

#### fl_committee_details.csv (Phase 2 — scraped ComDetail.asp pages)
| Field | Description |
|---|---|
| `account_id`, `name`, `type`, `status` | Same as links file |
| `address` | Combined mailing address string |
| `phone` | Phone number |
| `chairperson_name` / `chairperson_address` | Chairperson contact info |
| `treasurer_name` / `treasurer_address` | Treasurer contact info |
| `registered_agent_name` / `registered_agent_address` | Registered agent contact info |
| `purpose` / `affiliates` | Free-text fields; often blank due to scraper artifacts (see Data Notes) |
| `scraped_at` | Timestamp of scrape |

#### fl_committees_active.txt (bulk active-committee download)
| Field | Description |
|---|---|
| `AcctNum` | Committee account number |
| `Name`, `Type`, `TypeDesc` | Name and committee type (TypeDesc is the full label, e.g. "Political Committee") |
| `Addr1` / `Addr2` / `City` / `State` / `Zip` | Structured address |
| `County` | County (not available from detail pages) |
| `Phone` | Phone number |
| `ChrNameLast` / `ChrNameFirst` / `ChrNameMiddle` | Chairperson name parts |
| `TrsNameLast` / `TrsNameFirst` / `TrsNameMiddle` | Treasurer name parts |

### Candidate Files

#### fl_candidates_{election_slug}.txt (bulk per election cycle, e.g. `fl_candidates_1996_general_election.txt`)
| Field | Description |
|---|---|
| `AcctNum` | Candidate account number — per-registration, not a stable person ID |
| `VoterID` | Voter registration ID |
| `ElectionID` | e.g. `"19961105-GEN"` — year prefix used to derive `election_year` |
| `OfficeCode` / `OfficeDesc` | Office sought |
| `Juris1num` / `Juris2num` | District/circuit number and group/seat number |
| `StatusCode` / `StatusDesc` | Candidacy status |
| `PartyCode` / `PartyDesc` | Party |
| `NameLast` / `NameFirst` / `NameMiddle` | Candidate name parts |
| `SuppressAddress`, `Addr1` / `Addr2` / `City` / `State` / `Zip` | Mailing address (zip has trailing-zero 9-digit format) |
| `CountyCode` / `Phone` | County code, phone |
| `TrsNameLast` / `TrsNameFirst` / `TrsNameMiddle` | Treasurer name parts |
| `Email` | Email address |

#### fl_candidate_details.csv (scraped CanDetail.asp pages — enrichment only)
| Field | Description |
|---|---|
| `account_id`, `election`, `office`, `district`, `name`, `party`, `address`, `phone`, `status` | Mirrors bulk fields (district unreliable — see Data Notes) |
| `date_filed` / `date_qualified` | Filing dates |
| `method` / `email` / `scraped_at` | Filing method, email, scrape timestamp |

### Transaction Files

One file per year per type, named `fl_{type}_{year}.txt`, covering 1996–present (~120 files).

#### fl_contributions_{year}.txt
| Field | Description |
|---|---|
| `Candidate/Committee` | Recipient, e.g. `"Smith, John (REP)(GOV)"` (candidate committee) or `"Florida Medical Assoc (PAC)"` (PAC) — type suffix stripped by parser |
| `Date` | Contribution date |
| `Amount` | Dollar amount |
| `Typ` | Code mapped to `transaction_type` via `TYP_MAP` (CHE→Check, CAS→Cash, INK→In-Kind, INT→Interest, REF→Refund, LOA→Loan, DUE→Dues, MON→Money Order, CRE→Credit Card, ELE→Electronic Transfer, X→Other) |
| `Contributor Name` | Contributor/payer name |
| `Address` | Street address |
| `City State Zip` | Combined field, e.g. `"MIAMI, FL 33101"` — parsed by the parser into city/state/zip |
| `Occupation` | Contributor occupation |
| `Inkind Desc` | Description for in-kind contributions |

#### fl_expenditures_{year}.txt
| Field | Description |
|---|---|
| `Candidate/Committee`, `Date`, `Amount`, `Address`, `City State Zip` | Same as contributions |
| `Payee Name` | Recipient of the expenditure |
| `Purpose` | Free-text expenditure purpose |
| `Type` | Free-text expenditure type — passed through as `transaction_type` (no canonical mapping) |

#### fl_transfers_{year}.txt (fund transfers — routed to `contributions`)
| Field | Description |
|---|---|
| `Candidate/Committee`, `Date`, `Amount`, `Address`, `City State Zip` | Same as contributions |
| `Funds Transferred To` | Recipient committee |
| `Nature Of Account` / `Type` | Account/type metadata |

#### fl_other_{year}.txt (other distributions — routed to `expenditures`)
| Field | Description |
|---|---|
| `Candidate/Committee`, `Date`, `Amount`, `Address`, `City State Zip` | Same as expenditures |
| `Distributed To` | Recipient of the distribution |
| `Purpose` | Free-text purpose |

---

## Scraper

`src/pipeline/scrapers/florida.py`

**Committees (two-phase, requests-based, no WAF issues):**
- Phase 1 `scrape_committee_links()` — A-Z name search via `ComLkupByName.asp` (searchtype=1 "containing"), deduped by account_id, writes `fl_committee_links.csv`.
- Phase 2 `scrape_committee_details()` — scrapes `ComDetail.asp?account=X` for each ID found in Phase 1, writes `fl_committee_details.csv`.
- Update mode: `download_active_committees()` POSTs to `extractComList.asp` → `fl_committees_active.txt`, then `_sync_active_to_links()` appends any new IDs to the links file for detail scraping.

**Candidates (single-phase bulk):** `download_candidate_bulk()` POSTs to `extractCanList.asp` per election cycle (e.g. `elecID="20241105-GEN"`), saving one tab-delimited `fl_candidates_{slug}.txt` per cycle. `recent_only=True` limits to the 2 most recent general elections plus current-year specials.

**Transactions (Playwright-based — `/cgi-bin/*.exe` endpoints block datacenter IPs at the CDN, so requests-based scraping doesn't work):**
- 4 types: `contributions`, `expenditures`, `transfers`, `other`.
- `expenditures`, `transfers`, `other` ("ELECTION_BASED_TYPES") must be queried per specific `election_id` — `election=All` with blank date criteria returns "Invalid Date Range" on `expend.exe`/`FundXfers.exe`/`OtherDist.exe`.
- `contributions` use date-range chunking: `_CHUNK_LEVELS = [10, 7, 3, 1]` days, with `ROW_LIMIT = 32000` per query. If even 1-day chunks hit the row limit, falls back to `AMOUNT_RANGES` splitting (8 dollar bands: <$1, =$1, $1–10, $10–100, $100–1k, $1k–10k, $10k–100k, >$100k).
- `START_DATE = date(1996, 1, 1)`.
- Force mode: full 1996–present rebuild (~6,000 chunk queries collapsed into ~120 output files, one per year per type). Update mode: current calendar year only.

**Limitations:**
- Transaction scraping requires Playwright (browser automation) due to CDN blocking — slower and more fragile than the requests-based committee/candidate scrapers.
- Committee `purpose`/`affiliates` fields from detail pages are sometimes scraper parsing artifacts (next-label text captured instead of the actual value) — filtered by the parser but source data is imperfect.
- Candidate campaign committees are not separately registered in the committee system — they only appear by name in transaction files (handled in the parser's second pass).

**Expected runtime:** Committees: A-Z sweep + per-account detail scrape, tens of thousands of accounts (multi-hour on first run). Candidates: fast (one POST per election cycle). Transactions: full 1996–present rebuild is the most expensive part — many hours via Playwright due to chunking; incremental updates (current year only) are much faster.

---

## Parser

`src/pipeline/parsers/florida.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz`, `candidates.csv.gz`, `committees.csv.gz`

**Key transformations:**

- **Committees — two-pass construction:** Pass 1 writes one row per account in `fl_committee_details.csv` (the richest source — PACs, political committees, CCEs registered through the committee system), enriching `committee_type` from `fl_committees_active.txt`'s `TypeDesc` (falling back to `CMTE_TYPE_MAP` on the detail page's `type` code), `city`/`zip`/`county` from the active download where available, and `status` from `fl_committee_links.csv` (authoritative active/closed flag — overrides the detail page's possibly-stale status). Pass 2 scans all contribution and expenditure files for `Candidate/Committee` names not already written (candidate campaign committees, which never appear in `fl_committee_details.csv`) and synthesizes one committee row per unique name, using `strip_committee_type()` to detect candidate committees (two trailing paren groups, e.g. `"Smith, John (REP)(GOV)"`) vs. PACs (one group). Synthesized candidate-committee rows get their `state_filer_id`/`election_year` from a `candidate_name → AcctNum` map built from `fl_candidates_*.txt`; if no match is found, `state_filer_id` falls back to an MD5 hash of the committee name.

- **Candidates:** `fl_candidates_*.txt` bulk files are the sole source, deduplicated by `AcctNum` across all election-cycle files. `district` is built from `Juris1num`/`Juris2num` (e.g. `"5 Group 2"`). `election_year` is extracted from `ElectionID`'s 4-digit year prefix via `election_year_from_id()`.

- **Contributions:** `Typ` codes mapped to `transaction_type` via `TYP_MAP`. `Candidate/Committee` split into `committee_name`/`candidate_name` via `strip_committee_type()`; candidate names (raw format "Last, First") are inverted to "First Last" via `_invert_name()` to match the `candidates` table's name format. `City State Zip` split into city/state/zip via `split_city_state_zip()` — placeholder `'00000'` zips (used for aggregate/bulk contributions with no real address) are discarded as blank. Rows with a contributor name >200 chars are dropped (malformed source rows where unescaped newlines caused multiple records to merge into one field).

- **Loans:** `Typ="LOA"` rows are diverted from contributions to `loans_debts.csv.gz` with `record_type="Loan"`.

- **Transfers:** `fl_transfers_{year}.txt` rows are written to `contributions.csv.gz` with `transaction_type="Transfer"` (inter-committee transfers treated as contributions to the recipient).

- **Expenditures:** Payee name taken from whichever of `Payee Name` / `Recipient Name` / `Transferred To` is present. `transaction_type` taken from the raw `Typ`/`Type` field as free text (no canonical mapping, unlike contributions).

- **Other distributions:** `fl_other_{year}.txt` rows are written to `expenditures.csv.gz` with a fixed `transaction_type="Other Distribution"`.

- **Zip normalization:** `normalize_zip()` trims Florida's 9-digit trailing-zero format (`"342210000"` → `"34221"`).

**person_id model:** `committee` — `AcctNum` is per-registration; the same person running in different cycles gets a different `AcctNum`/`person_id`. `assign_person_ids(id_model="committee")` and `assign_committee_person_ids()` are run after all files are written.

**Limitations:**
- `contributor_type`, `employer`, and `election_year` are always blank on contributions — the raw contribution files carry no equivalent columns.
- `category`, `candidate_name`, `office`, and `election_year` are always blank on expenditures — same reason (no equivalent raw columns; `candidate_name` is only derived for contributions via the Candidate/Committee paren-group heuristic).
- `office` is always blank on candidates' transaction-level data (only present on the `candidates` table itself, from `OfficeDesc`).
- `committee_type` is ~93.5% filled — the remaining ~6.5% are synthesized non-candidate committees (Pass 2) with no `TypeDesc`/type-code match.

**Expected runtime:** Contributions dominate (27.6M rows across 30 years) — full reparse takes tens of minutes, dominated by `contributions.csv.gz` writing.

---

## Data Notes

- **Low-fill transaction fields:** Florida's raw transaction files simply don't carry `contributor_type`, `employer`, `election_year` (contributions) or `category`, `candidate_name`, `office`, `election_year` (expenditures) — these are 0.0% filled by design, not a parsing gap.

- **Committee `committee_type` "(blank)" (1,078 of 16,555, 6.5%):** Synthesized candidate-campaign committees (Pass 2) for which neither `fl_committees_active.txt` nor `CMTE_TYPE_MAP` had a type code. `committee_type="Candidate Committee"` is set for synthesized rows where `strip_committee_type()` found two trailing paren groups; the remainder (non-candidate synthesized committees with no resolvable type) are blank.

- **Committee `active` "(blank)" (8,548 of 16,555, 51.6%):** Synthesized committees (Pass 2) have no entry in `fl_committee_links.csv`, so `status` — and therefore `active` — is never set.

- **Committee `candidate_name`/`treasurer_name`/`city`/`zip` ~44-46% filled:** These fields are only populated for committees that appear in `fl_committee_details.csv` and/or `fl_committees_active.txt`; the ~6,500 synthesized candidate committees from Pass 2 generally lack address/treasurer detail.

- **`AcctNum` is per-registration, not a stable person ID** (`id_model="committee"`): the same individual running in 2022 and 2026 gets two different AcctNums and `person_id`s.

- **Zip codes** in bulk candidate files have trailing zeros (`"342210000"` → `"34221"`), normalized by the parser.

- **Combined `City State Zip` field:** contribution/expenditure files store `"MIAMI, FL 33101"` as one column; parsed via regex. Placeholder `', 00000'` or `', FL 00000'` (used for aggregate/bulk contributions with no real address) is treated as blank.

- **`Candidate/Committee` includes a type suffix:** `"Name (PAC)"` or `"Smith, John (REP)(GOV)"` — stripped by `strip_committee_type()`, which also distinguishes candidate committees (2 paren groups) from PACs (1 paren group).

- **Outliers:** 1 contribution and 3 expenditure rows have `|amount| ≥ $10,000,000` — plausible large transfers/loans, not filtered.

- **Non-standard state/zip codes:** ~0.0–0.1% of `contributor_state`/`payee_state` values are non-US codes (military/territory codes like `AE`, `AP`, or Canadian provinces like `BC`); ~0.1–0.2% of zips don't match US ZIP format. Not filtered.

- **`expenditures.date` fill rate is 99.9%** (vs. 100% elsewhere) — a small number of expenditure rows have unparseable dates.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-13 |
| Parser | 2026-06-13 |
