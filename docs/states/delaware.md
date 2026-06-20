# Delaware — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Delaware (DE) |
| **Source** | [Delaware CFRS (Campaign Finance Reporting System)](https://cfrs.elections.delaware.gov) |
| **Access method** | Playwright — year-filter CSV export (transactions) + HTML grid + ShowReview page scrape (entities) |
| **Coverage** | 2000 – present |
| **person_id model** | `committee` — new CF_ID per registration cycle; `person_id` = min CF_ID for a given `(candidate_name, office, district)` |

---

## Raw Data Structure


### Transaction Files

One CSV per year per type: `de_contributions_{year}.csv` and `de_expenditures_{year}.csv`.

#### de_contributions_{year}.csv

| Field | Description |
|---|---|
| `Contribution Date` | Date of contribution (M/D/YYYY) |
| `Contributor Name` | Contributor full name (individual or organization) |
| `Contributor Address Line 1` | Street address |
| `Contributor Address Line 2` | Suite/unit (usually blank) |
| `Contributor City` | Contributor city |
| `Contributor State` | Contributor state |
| `Contributor Zip` | Contributor zip code (variable format) |
| `Contributor Type` | e.g. "Individual", "Business/Group/Organization", "PAC Committee" |
| `Employer Name` | Employer (individuals only) |
| `Employer Occupation` | Occupation (individuals only) |
| `Contribution Type` | Payment method (Check, Credit Card, Cash, EFT, etc.) |
| `Contribution Amount` | Dollar amount (4 decimal places in raw) |
| `CF_ID` | CFRS committee identifier — underscore-separated (links to committee registry) |
| `Receiving Committee` | Name of the recipient committee |
| `Filing Period` | Filing period name and election date |
| `Office` | Office sought, e.g. "(State Representative)" — parenthesized in most rows |
| `Fixed Asset` | Yes/No — whether the contribution was a fixed asset |

**Note:** `CF_ID` uses underscores in contributions; `CF ID` uses a space in expenditures. Both refer to the same CFRS identifier.

**Loan routing:** Rows with `Contribution Type` of `"Candidate Loan"` or `"Non Candidate Loan"` are routed to `loans_debts` rather than `contributions`.

**Out-of-state aggregates:** Delaware allows out-of-state contributions below a threshold to be reported as a lump sum under ~20 different label spellings (e.g. "CONTRIBUTIONS NON-DELAWARE", "Non DE Transactions", "non-DE individuals  Receipts   from"). The parser normalizes all variants to `[Non-Delaware Aggregate]` and excludes them from itemized analysis.

#### de_expenditures_{year}.csv

| Field | Description |
|---|---|
| `Expenditure Date` | Date of expenditure (M/D/YYYY) |
| `Payee Name` | Name of the payee |
| `Payee Address` | Payee street address |
| `Payee City` | Payee city |
| `Payee State` | Payee state |
| `Payee Zip` | Payee zip code |
| `Expense Method` | Payment method (Check, Credit Card, EFT, etc.) |
| `Expense Purpose` | Free-text description |
| `Expense Category` | Standardized category (e.g. "Media", "Contributions", "In-Kind") |
| `Amount($)` | Dollar amount |
| `CF ID` | CFRS committee identifier (space-separated) |
| `Committee Name` | Name of the spending committee |
| `Candidate Name` | Associated candidate (not reliably populated in raw) |
| `Filing Period` | Filing period name and election date |
| `Office` | Office sought |

### Entity Files

#### de_committee_links.csv

Intermediate file produced by Phase 1 of the entity scrape. One row per committee with its internal CFRS `member_id` and the corresponding ShowReview URL. Used by Phase 2 to drive the detail scrape.

| Field | Description |
|---|---|
| `member_id` | Internal CFRS database ID (not the public CF_ID; required to construct the ShowReview URL) |
| `cf_id` | Public CFRS committee identifier |
| `ctype_code` | Committee type code (01–04) |
| `ctype_label` | Human-readable committee type |
| `show_review_url` | Full ShowReview page URL for this committee |

#### de_committee_details.csv

Main entity file produced by Phase 2. One row per committee, covering all four active types (~2,977 committees total as of 2026).

| Field | Description |
|---|---|
| `member_id` | Internal CFRS ID |
| `cf_id` | Public CFRS committee ID (format: `{type_code}{6-digit-number}`, e.g. `01004350`) |
| `ctype_code` | `01` = Candidate Committee, `02` = PAC, `03` = Political Committee, `04` = 3rd Party Advertiser |
| `ctype_label` | Human-readable type |
| `committee_name` | Registered committee name |
| `other_name` / `short_name` | Alternate names (often blank) |
| `status` | Active / Inactive / Closed |
| `established_date` | Date committee was established (M/D/YYYY) |
| `end_date` | Date committee was closed (blank if active) |
| `purpose` | Stated committee purpose (free text) |
| `email` | Committee contact email |
| `web_address` | Committee website (usually blank) |
| `physical_address` | Street address |
| `physical_city` / `physical_state` / `physical_zip` | Address components |
| `office_type` | State Office / County Office / Municipal Office / School Board (type 01 only) |
| `county` | County/municipality (type 01 only; "Na" for statewide offices) |
| `office_sought` | Office being sought (type 01 only) |
| `district` | District number (type 01 only; "Na" for statewide) |
| `party` | Party affiliation (type 01 only) |
| `candidate_name` | Candidate full name (type 01 only) |
| `candidate_email` / `candidate_phone` / `candidate_address` | Candidate contact (type 01 only) |
| `treasurer_name` | Treasurer full name |
| `treasurer_email` / `treasurer_phone` / `treasurer_address` | Treasurer contact |
| `scraped_at` | Date the record was scraped |

---

## Scraper

`src/pipeline/scrapers/delaware.py`

All data comes from CFRS. No data is fetched from elections.delaware.gov.

**Transactions (Playwright):** Navigates to the CFRS bulk download page, selects each year from a dropdown, and exports contributions and expenditures as CSV. The current year is always re-fetched; prior years are skipped if already in the manifest.

**Entities (Playwright, two-phase):**

*Phase 1 — `scrape_committee_links()`:* For each of the 4 active committee types, navigates ViewCommittees, runs a search, paginates through the Telerik/Kendo UI grid, and parses each row's committee name link to extract `memberID` and CF_ID. Saves to `de_committee_links.csv`. The `memberID` (an internal DB key distinct from the public CF_ID) is required to construct the ShowReview URL and is not available in any CSV export — HTML parsing of the results grid is the only way to obtain it.

*Phase 2 — `scrape_committee_details()`:* For each memberID in the links file not yet in the manifest, navigates to its ShowReview page and scrapes the full detail record using a BeautifulSoup + regex parser. Writes rows incrementally to `de_committee_details.csv` (one row flushed and manifest-upserted per committee) so partial runs are resumable without re-scraping completed records.

**Committee type scope:** Type 05 (Certification of Intention) is excluded — those filers raise/spend under $5,000, are not required to file financial reports, and have no transaction data in the system.

**Limitations:**
- Requires a local Playwright browser session (WAF blocks datacenter IPs)
- Phase 1: ~5–10 min for all types. Phase 2: ~35–50 min for ~3,000 committees at 0.3–0.5s per page
- ~45% of older/inactive candidate committees have no Election Participation section on their ShowReview page; `office_sought`, `district`, and `party` are empty for those rows
- Candidate and treasurer contact fields are blank for committees that registered without an email address (common pre-2010)

**Expected runtime:** Transactions ~5 min. Entities (full): ~50 min. Entities (incremental): seconds to minutes depending on new committees.

---

## Parser

`src/pipeline/parsers/delaware.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `candidates.csv.gz`, `committees.csv.gz`, `loans_debts.csv.gz`

**Key transformations:**
- Committee registry loaded from `de_committee_details.csv` keyed by `cf_id`; type 05 rows excluded. A field-bleed artifact cleanup pass normalizes residual parsing artifacts from any scrape run that pre-dates the current parser version.
- Candidates table built from type 01 rows where `candidate_name` is populated. Honorific prefixes (MR., MS., DR., HON., etc.) and credential suffixes (JR, SR, ESQUIRE, MD, PHD, etc.) are stripped from `candidate_name` before writing, so registrations like `"MR. JOSEPH BIDEN"` and `"JOSEPH BIDEN"` resolve to the same person record.
- `candidate_name` written to each contribution row via CF_ID lookup against the registry, enabling candidate-level aggregation in the aggregate database.
- `candidate_name` on the committees table enables `assign_committee_person_ids` to link each type 01 committee to its candidate's `person_id`.
- Loans routed to `loans_debts` based on `Contribution Type` (`Candidate Loan`, `Non Candidate Loan`).
- Out-of-state aggregate labels normalized to `[Non-Delaware Aggregate]` and excluded from contributions.
- Dates normalized from `M/D/YYYY` → `YYYY-MM-DD`.
- Zip codes cleaned (non-numeric characters stripped).

**person_id model:** `committee` — CFRS assigns a new CF_ID per committee registration, so the same candidate gets a different ID each cycle. `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(cf_id)` prefixed with Delaware's FIPS code (10), producing 14-digit integers.

**Expected runtime:** ~18s.

---

## Data Notes

- **office/district/party coverage ~55%:** Older and inactive candidate committees often have no Election Participation section on their ShowReview page. This is a CFRS source data limitation — those fields were not consistently recorded for committees that closed before the current portal version. Not a parsing gap.
- **candidate_name on contributions ~56%:** Only type 01 (Candidate Committee) contributions receive a candidate name lookup. PAC, political committee, and 3rd party advertiser contributions have no associated candidate by definition.
- **Zip format warnings:** A small number of contributor and payee zip codes in the raw CSVs contain malformed values (negative numbers, decimals, truncated codes). Pre-existing data entry artifacts on the CFRS portal; not introduced by the parser.
- **AE contributor state code:** A handful of rows have `AE` (Armed Forces Europe) as the contributor state, a valid APO/FPO abbreviation. Flagged by the validator as unrecognized but not an error.
- **No election_year on candidates:** Candidates are derived from committee registrations, which don't carry a specific election cycle. The `election_year` field is left empty.
- **Treasurer contact coverage:** All committees have a treasurer name (100%). Email and address coverage is lower for pre-2010 registrations, which frequently used paper-only contact information.
- **"Certification of Intention" excluded:** ~3,000 type 05 filers (small local and school board candidates) are present in the CFRS system but excluded from the pipeline entirely — they file no reports and have no transaction data.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-31 |
| Parser | 2026-05-31 |
