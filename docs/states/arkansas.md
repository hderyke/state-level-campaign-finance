# Arkansas — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Arkansas (AR) |
| **Source** | [Arkansas Secretary of State Ethics Disclosures](https://ethics-disclosures.sos.arkansas.gov) |
| **Access method** | JSON API (POST requests) for both transactions and entity registry |
| **Coverage** | 2022 – present (2022 is sparse — see Data Quirks) |

---

## Raw Data Structure

Files live in `data/Arkansas/raw/`. Two transaction files per year plus two static entity registry files.

### Transaction Files

One file per year per type: `contributions_{year}.csv` and `expenditures_{year}.csv`

#### contributions_{year}.csv

| Field | Description |
|---|---|
| `Filing Entity ID` | Matches `filerEntityID` in the entity registry |
| `Entity Name` | Committee or candidate name as registered; candidates may have committee name appended in parens |
| `FilerType` | "Candidate" or committee type label |
| `Transaction Type` | Always "Contribution" |
| `Transaction Sub Type` | e.g. "Itemized Monetary", "Non-Itemized Monetary" |
| `Funding Source / Loan Source Type` | e.g. "Individual", "Business/Organization/Unlisted PAC" |
| `Source Name` | Contributor name (blank for non-itemized rows) |
| `Source Address` | Single string: "Street, City, ST ZIP" |
| `Employer Name` | Contributor employer |
| `Occupation` | Contributor occupation (standardized) |
| `Occupation Other` | Free-text occupation when standardized value is "Other" |
| `Transaction Date` | Date of contribution (MM/DD/YYYY in raw) |
| `Transaction Amount` | Dollar amount formatted as "$1,000.00" |
| `Transaction Description` | Optional free-text note |
| `Transaction ID` | Unique transaction ID |
| `Election Type` | e.g. "General", "Primary" |
| `Election Year` | Year of the associated election |
| `Guarantor Name` | Loan guarantor name (populated for guaranteed loans) |
| `Guarantor Address` | Loan guarantor address |
| `Report Filed Date` | Date the report was filed |
| `Report Name` | Name of the filing period report |
| `Amended` | Y/N |

#### expenditures_{year}.csv

| Field | Description |
|---|---|
| `Filing Entity ID` | Matches `filerEntityID` in the entity registry |
| `Entity Name` | Committee or candidate name |
| `FilerType` | "Candidate" or committee type label |
| `Transaction Type` | Always "Expenditure" |
| `Transaction Sub Type` | e.g. "Itemized Monetary" |
| `Payee Type` | e.g. "Individual", "Business" |
| `Payee Name` | Name of the expenditure recipient |
| `Payee Address` | Single string: "Street, City, ST ZIP" |
| `Transaction Date` | Date of expenditure (MM/DD/YYYY in raw) |
| `Transaction Amount` | Dollar amount formatted as "$1,000.00" |
| `Transaction Description` | Free-text description of the expenditure |
| `Transaction ID` | Unique transaction ID |
| `Transaction Category` | Standardized category (e.g. "Advertising", "Other(list)") |
| `Transaction Category Others` | Free-text category when standardized value is "Other(list)" |
| `Election Type` | e.g. "General", "Primary" |
| `Election Year` | Year of the associated election |
| `Report Filed Date` | Date the report was filed |
| `Report Name` | Name of the filing period report |
| `Amended` | Y/N |

### Entity Registry Files

`candidates.csv` and `committees.csv` share the same schema (pulled from the same API endpoint, split by filer type code):

| Field | Description |
|---|---|
| `filerEntityID` | Primary key — joins to `Filing Entity ID` in transaction files |
| `filerEntityVersionID` | Internal versioning ID |
| `filerTypeCode` | Code: `CAN` (candidate), `PAC`, `CPAC`, `IEF`, `PP`, `ECOMM` |
| `filerType` | Human-readable filer type label |
| `firstName` / `lastName` / `suffix` | Candidate name parts (blank for committees) |
| `filerName` | Full name as registered |
| `committeeName` | Official committee name |
| `office` | Office sought (candidates only) |
| `officeDistrictName` | District name |
| `jurisdictionName` | Jurisdiction |
| `politicalParty` | Party affiliation |
| `filerStatus` | "Active" or "Inactive" |
| `electionYear` / `filingYear` | Election and filing year |
| `totalRaised` / `totalSpent` / `balanceofFunds` | Running financial totals from the portal |
| `filingTypeCode` | Internal filing type classification |
| `isPaperFiler` | True/False — whether the filer submits paper reports |
| `guid` | Portal-assigned unique identifier |

---

## Scraper

`src/pipeline/scrapers/arkansas.py`

**Transactions:** POSTs to `GetExportPublicDownloadData` with a `transactionTypeCode` (`TCON` or `TEXP`) and `filingYear`. Response is a raw CSV. The scraper detects and handles UTF-16 encoding (common on .NET-based portals) and normalizes to UTF-8 before saving. A `manifest.csv` tracks completed `(type, year)` pairs — already-downloaded files are skipped except the current year, which is always re-fetched.

**Entities:** POSTs to `GetCandidateCommitteDetails` with an open query (pageSize 25,000). Response is a JSON array of all registered filers. Split into `candidates.csv` (filerTypeCode `CAN`) and `committees.csv` (`PAC`, `CPAC`, `IEF`, `PP`, `ECOMM`). `SFIFILER` (personal financial disclosure) is explicitly excluded — those are not campaign finance filers.

**Limitations:**
- No pagination on the entity endpoint — relies on a single 25,000-row page being sufficient; will silently truncate if the registry grows beyond that
- Years list (2022–2026) is hardcoded and will need updating annually
- No SSL issues; standard HTTPS with Referer/Origin headers required to avoid 403s

**Expected runtime:** ~1–3 min total (10 transaction files + 1 entity API call, 0.5s sleep between transaction requests).

---

## Parser

`src/pipeline/parsers/arkansas.py`

Arkansas has a proper entity registry, so committees and candidates are loaded from the registry files first and joined to transactions by `Filing Entity ID` → `filerEntityID`.

**person_id model:** `person` — `filerEntityID` is a stable person-level ID that persists across election cycles (unlike Alabama/Arizona which re-register per cycle). `person_id` is set to `filerEntityID` directly, prefixed with Arkansas's FIPS code (05), producing 14-digit integers.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv` (empty — no loan data available from this source)

**Key transformations:**
- Amounts stripped of `$` and commas (`"$1,000.00"` → `"1000.00"`)
- Dates normalized from `MM/DD/YYYY` → `YYYY-MM-DD`; implausible years (before 1990 or more than 2 years out) discarded
- Addresses parsed from a single concatenated string (`"Street, City, ST ZIP"`) using regex — city extracted as the last comma-delimited segment before the state/zip
- Candidate `Entity Name` values with committee names in parentheses are stripped: `"Smith, John (Smith for Gov)"` → `"Smith, John"`
- `Transaction Category Others` preferred over `Transaction Category` when more specific
- `Occupation Other` used as fallback when `Occupation` is blank
- Candidate filers are written to both `candidates.csv` and `committees.csv` (as type "Candidate") since they have campaign accounts
- `Amended` Y/N → 1/0

**Limitations:**
- Address regex may fail for non-standard formats; those rows get blank city/state/zip silently
- Non-itemized contribution rows have no `Source Name` — kept as aggregate totals with blank contributor
- No loans data available; `loans_debts.csv` is written empty

**Expected runtime:** ~1–2 min (10 CSVs across 5 years).

---

## Data Quirks

- **No loans data** — the Arkansas ethics portal does not export loan/debt transactions. `loans_debts.csv` exists but is always empty.
- **2022 data is sparse** — only covers November–December 2022, and candidates only (no committee data for that year). Included as-is but not representative of a full year.
- **Address is a single string** — contributor and payee addresses are not pre-split into components. The regex parser handles the common format but will silently drop city/state/zip for unusual address strings.
- **Non-itemized contributions** — rows with `Transaction Sub Type` of "Non-Itemized Monetary" have no `Source Name`. These are aggregate totals for small contributions below the itemization threshold and cannot be attributed to individual donors.
- **Dual occupation fields** — `Occupation` holds a standardized value; when that value is "Other", the free-text detail is in `Occupation Other`. The parser merges these, preferring whichever is non-blank.
- **Dual category fields** — same pattern for expenditures: `Transaction Category` is standardized, `Transaction Category Others` is the free-text version. Parser prefers the free-text when available.
- **Committee type variety** — committees include `IEF` (Independent Expenditure Filer), `PP` (Political Party), `ECOMM`, and `CPAC` in addition to standard `PAC`. These are all included but worth being aware of when filtering.
- **Portal financial totals** — `totalRaised`, `totalSpent`, and `balanceofFunds` in the registry reflect the portal's running totals, not necessarily what's in the downloaded transaction data (especially for 2022).

---

## Status

- [x] Scraper complete
- [x] Parser complete
- [x] Loaded into DB
- [x] Verified / QA'd (2026-05-29)

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-29 |
| Parser | 2026-05-29 |
