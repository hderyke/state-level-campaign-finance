# Maryland — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Maryland (MD) |
| **Source** | [Maryland State Board of Elections Campaign Finance System (MDCRIS)](https://campaignfinance.maryland.gov) |
| **Access method** | POST JSON API — bulk CSV export endpoint, no authentication required |
| **Coverage** | 2021 – present (a small number of pre-2021 transaction dates appear from late-filed reports) |
| **person_id model** | `committee` — the SBE assigns a new `Filing Entity Id` per committee registration; `person_id` = min ID for a given `(candidate_name, office, district)` across all registrations |

---

## Raw Data Structure

Three file types land in `data/Maryland/raw/`:

### `committees.csv`

Downloaded once per run with `transactionTypeCode=TCMD` and `filingYear=0` (all registered committees). Contains both active and dissolved committees; `Registration Dissolved Date` is non-empty for dissolved ones.

| Field | Description |
|---|---|
| `Filing Entity Id` | Unique committee ID — join key for transaction files |
| `Committee Name` | Full committee name |
| `Committee Type` | e.g. "Candidate Committee", "Political Action Committee (PAC)", "Slate Committee" |
| `Election` | Election name/cycle the committee is registered for |
| `Election Year` | Four-digit election year |
| `Treasurer/Authorized Agent Name` | Primary contact |
| `Committee City` / `Committee ZipCode` | Committee mailing address (sparsely populated — ~3% fill rate in practice) |
| `Registration Submission Date` / `Registration Approval Date` / `Registration Dissolved Date` | Lifecycle dates |
| `Candidate LastName` / `Candidate First Name` / `Candidate Middle Name` / `Candidate Suffix` | Candidate name parts (present only for Candidate Committee rows) |
| `Office Sought` | Office the candidate is running for |
| `Party Affiliation` | Political party |
| `Jurisdiction` | Geographic jurisdiction (county, state, etc.) |

### `contributions_{year}.csv`

One file per filing year (2021–present), downloaded with `transactionTypeCode=TCON`. Despite the filename, this file contains **both contributions and loans** — the `Transaction Type` field distinguishes them.

| Field | Description |
|---|---|
| `Filing Entity Id` | Committee ID — matches committees.csv |
| `Committee Name` | Filing committee name |
| `Committee Type` | Abbreviated type (e.g. "Candidate", "PAC") — differs from the long-form values in committees.csv |
| `Contributor Type` | e.g. "Individual", "Business/Group/Organization", "Self" |
| `Contributor Company Name` | Organization name (populated when contributor is not a person) |
| `Contributor Last Name` / `Contributor First Name` / `Contributor Middle Name` | Person name parts |
| `Contributor City` / `Contributor State` / `Contributor ZipCode` | Contributor address |
| `Transaction Type` | e.g. "Contribution", "Loan Received", "Loan Forgiven", "Loan Payment", "Return Contribution" |
| `Transaction Date` | MM/DD/YYYY |
| `Transaction Amount` | Dollar amount formatted as `$1,000.00` |
| `Payment Type` | e.g. "Check", "Credit Card" |
| `Fund Type` | e.g. "Electoral", "Administrative" |
| `Report Name` | Filing report name (used as `filing_id`) |

Note: `Contributor ZipCode` (and some other fields) are Excel-escaped as `="VALUE"` — stripped by the parser.

### `expenditures_{year}.csv`

One file per filing year (2021–present), downloaded with `transactionTypeCode=TEXP`. Contains both regular expenditures and outstanding obligation records.

| Field | Description |
|---|---|
| `Filing Entity Id` | Committee ID — matches committees.csv |
| `Committee Name` | Filing committee name |
| `Payee Type` | e.g. "Business/Group/Organization", "Individual" |
| `Payee Company Name` / `Payee Last Name` / `Payee First Name` | Payee name parts |
| `Payee City` / `Payee State` / `Payee Zip Code` | Payee address |
| `Transaction Type` | e.g. "Expenditure", "Independent Expenditure / Electioneering Communication", "Outstanding Obligation", "Outstanding Obligation Payment", "Outstanding Obligation Forgiven", "Return Expenditure" |
| `Transaction Date` | MM/DD/YYYY |
| `Transaction Amount` | Dollar amount formatted as `$15.00` |
| `Category` | Standardized expense category (e.g. "Media", "Salaries and Other Compensation") |
| `Purpose` | Free-text purpose description |
| `Candidate/Ballot Issue` | Associated candidate or ballot measure name |
| `Office Sought` | Office associated with the expenditure |
| `Report Name` | Filing report name (used as `filing_id`) |

---

## Scraper

`src/pipeline/scrapers/maryland.py`

POSTs to a single API endpoint with different `transactionTypeCode` values:

```
https://api-campaignfinance.maryland.gov/api/ExportPublicData/GetExportPublicDownloadData
```

No authentication required. Only `Content-Type: application/json` and `Accept` headers are needed; `Referer` and `Origin` are also sent as a precaution against .NET WAF blocks.

**Committees:** Downloaded in a single request with `filingYear=0`. Always re-fetched on every run — the registry changes continuously as new committees register throughout the cycle. The manifest records the last download but does not suppress re-fetching.

**Transactions:** One request per year per type (TCON/TEXP). Years 2020 and earlier return empty responses from the API — `START_YEAR = 2021` is the effective floor. The current year is always re-fetched even on incremental runs.

**Limitations:**
- Data before 2021 is not available via the bulk export API. Pre-2021 records that appear in output (2020, 2018, etc.) are late-filed reports where the filer entered an old transaction date.
- Large files (e.g. 2022 contributions at ~232 MB, 2026 contributions at ~204 MB) can take several seconds each to transfer.

**Expected runtime:** ~25–35s for a full sync (committees + 6 years × 2 types).

---

## Parser

`src/pipeline/parsers/maryland.py`

**Input quirks handled:**
- Each raw CSV has a timestamp title row on line 0 (e.g. `"Contributions and Loan Download as of 06/28/2026 01:01 AM,"`). The parser discards this row and uses line 1 as the header.
- `ZipCode` fields (and occasionally others) are Excel-escaped as `="21074"` — stripped by `_unquote()`.
- Large files (notably 2026 contributions) contain NUL bytes (`\x00`), which crash Python's csv module. Filtered line-by-line via a generator before passing to `DictReader`.

**Key transformations:**
- Dates normalized from `MM/DD/YYYY` → `YYYY-MM-DD`.
- Amounts normalized from `$1,000.00` → `1000.00`; parentheses treated as negative.
- Contributor names assembled from `Last Name / First Name / Middle Name` parts; falls back to `Contributor Company Name` when no person name is present. Output format: `"LastName, FirstName MiddleName"`.
- Payee names assembled similarly from `Payee Last Name / First Name`, falling back to `Payee Company Name` then `Vendor Name`.
- `contributions_{year}.csv` rows are **split by Transaction Type**: rows matching `\bloan\b` (case-insensitive) are routed to `loans_debts`; all other rows go to `contributions`.
- `committees.csv` produces both a `committees` row (every row) and a `candidates` row (Candidate Committee rows where a candidate name is present).
- Committee registry is keyed by `Filing Entity Id` and used to enrich contributions and expenditures with `candidate_name`, `office`, and `election_year`.

**Output files:**

| File | Notes |
|---|---|
| `contributions.csv.gz` | Non-loan rows from TCON files (~3.35M rows for 2021–2026) |
| `expenditures.csv.gz` | All rows from TEXP files (~310K rows for 2021–2026) |
| `committees.csv.gz` | One row per registered committee (~2,547) |
| `candidates.csv.gz` | One row per candidate committee where candidate info is present (~1,967) |
| `loans_debts.csv.gz` | Loan-type rows from TCON files (~4,843 rows for 2021–2026) |

**person_id model:** `committee` — the SBE assigns a new `Filing Entity Id` per committee registration, so the same candidate gets a different ID each cycle. `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(state_filer_id)` across all registrations, prefixed with Maryland's FIPS code (24) to produce 14-digit integers.

**Limitations:**
- `employer` and `occupation` are not collected by Maryland's system — always blank.
- `district` is not available in the committee file — always blank.
- `election_year` on contributions is sparsely populated (~4–5% fill rate) because the registry `Election Year` field is often empty and the transaction files don't carry it directly.
- `candidate_name` and `office` on contributions are enriched via the registry join (~17–25% fill rate depending on year), so PAC and party committee spending has no candidate attribution.
- Committee `city` and `zip` are populated for only ~3% of rows — the SBE portal collects a mailing address separately from the candidate address and most committees leave it blank.

**Expected runtime:** ~1m 20s for 2021–2026 (dominated by the large 2022 and 2024 contribution files).

---

## Data Notes

- **TCON covers loans too.** The "Contributions and Loan Download" file combines contribution and loan transactions. Loan rows are identified by `Transaction Type` matching `\bloan\b` and routed to `loans_debts`. The five loan types observed are: `Loan Received`, `Loan Forgiven`, `Loan Payment`, `Return Contribution` (not a loan — stays in contributions), and occasionally others.
- **Pre-2021 dates.** A small number of transaction rows have dates before 2021 (2005–2020). These are legitimate late-filed or amended reports entered into MDCRIS after the system launched. They're kept as-is.
- **Excel-escaped zips.** The API returns zip codes (and occasionally Filing Entity Ids) as Excel formula strings (`="21074"`) to prevent spreadsheet applications from stripping leading zeros. The parser strips this formatting; downstream data has plain string zips.
- **NUL bytes in large files.** The 2022 and 2026 contribution files contain embedded NUL bytes (`\x00`), likely a serialization artifact from the .NET export layer. Python's csv module raises `_csv.Error: line contains NUL` on these — filtered by the generator in `open_md_csv()`.
- **Committee type abbreviation mismatch.** Transaction files use shortened Committee Type values (e.g. `"Candidate"`, `"PAC"`) while the committee registry file uses full names (e.g. `"Candidate Committee"`, `"Political Action Committee (PAC)"`). The parser handles both forms when detecting candidate committees.
- **Large Super PAC transfers.** Maryland's contribution data includes very large transfers to Super PACs (e.g. $52M and $50M to Democracy PAC in June 2026). These are real, disclosed transfers — not data errors.
- **Payee state codes.** International expenditures produce non-US state codes (`BC`, `Berlin`, etc.) in `payee_state`. These are data quality issues in the source system. They trigger tier-2 validator warnings but are kept as-is.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-28 |
| Parser | 2026-06-28 |
| Alias CSVs | 2026-06-28 |
