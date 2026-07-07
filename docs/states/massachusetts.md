# Massachusetts — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Massachusetts (MA) |
| **Source** | [OCPF (Office of Campaign and Political Finance) Bulk Data Downloads](https://ocpf.us/Data/Downloads) |
| **Access method** | Direct HTTP download from Azure Blob Storage — no authentication required |
| **Coverage** | 2002 – present |
| **person_id model** | `committee` — OCPF assigns a new CPF ID per committee registration; `person_id` = min CPF ID for a given `(candidate_name, office, district)` |

---

## Raw Data Structure

Two categories of files land in `data/Massachusetts/raw/`:

### `ocpf-filers.zip`

Contains a single file, `all_filers.txt` — a tab-delimited registry of every committee ever registered with OCPF. Always re-fetched on each run (no year-based versioning).

| Field | Description |
|---|---|
| `CPF ID` | Unique committee ID — join key for all transaction files |
| `Comm_Name` | Committee name |
| `Account Type Code` | Single letter encoding committee type (see Account Codes below) |
| `Candidate First Name` / `Candidate Last Name` | Candidate name parts (blank for PACs, party committees, etc.) |
| `Treasurer First Name` / `Treasurer Last Name` | Treasurer name parts |
| `Office Type Sought` | Office the candidate is running for (e.g. `"Governor"`, `"State Representative"`) |
| `District Name Sought` | Electoral district (e.g. `"1st Middlesex"`) — `"N/A"` for statewide offices |
| `Party Affiliation` | Political party |
| `Comm City` / `Comm Zip Code` | Committee mailing address |
| `Closed Date` | Non-empty if the committee has been dissolved |

**Account Type Codes:**

| Code(s) | Committee Type |
|---|---|
| D, U, Z, z, N, O | Candidate Committee (various legacy and depository sub-types) |
| P, Y | PAC |
| I | Independent Expenditure |
| L, E | Party Committee |
| X | Ballot Measure |
| V, S, B | Other (segregated accounts, non-partisan bodies, banks) |

Codes `D` through `O` are candidate-type accounts; these get both a committee row and a candidate row in the output.

### `ocpf-{year}-reports.zip`

One ZIP per year from 2002 through the current year. Each contains two tab-delimited files:

#### `reports.txt`

One row per filed report. Used to join `report-items.txt` to a CPF ID and enrich transaction rows with filing context.

| Field | Description |
|---|---|
| `Report_ID` | Primary join key to `report-items.txt` |
| `CPF_ID` | Committee ID — links to `all_filers.txt` |
| `Report_Type_ID` | Numeric type code (60 = Deposit Report, 11 = Year-End D102, 70–72 = Bank Report, etc.) |
| `Report_Type_Description` | Human-readable report type name |
| `Report_Year` | The election year the report is filed under |
| `Start_Date` / `End_Date` | Reporting period |
| `OCPF_Candidate_First_Name` / `OCPF_Candidate_Last_Name` | Candidate name at time of filing |
| `OCPF_Office` | Office being sought at time of filing (preferred over filer registry for candidates who sought multiple offices) |
| `OCPF_District_Code` / `OCPF_District` | District at time of filing |
| `OCPF_Comm_Name` | Committee name at time of filing (preferred over registry snapshot) |

#### `report-items.txt`

One row per transaction. The main data file — typically several hundred thousand rows per year ZIP.

| Field | Description |
|---|---|
| `Item_ID` | Unique transaction ID (used as `filing_id`) |
| `Report_ID` | Join key to `reports.txt` |
| `Record_Type_ID` | Numeric code determining transaction type and routing (see Record Type Routing below) |
| `Date` | Transaction date (MM/DD/YYYY) |
| `Amount` | Dollar amount |
| `Name` | Last name (individuals) or full name (organizations) |
| `First_Name` | First name (individuals only) |
| `City` / `State` / `Zip` | Counterparty address |
| `Description` | Free-text purpose or description |
| `Occupation` / `Employer` | Contributor occupation and employer (contributions only; sparsely populated for small amounts) |
| `Related_CPF_ID` | CPF ID of the related committee (for inter-committee transfers) |
| `Clarified_Name` | Committee-annotated counterparty name override for bank-reported expenditures (type 311) |
| `Clarified_Purpose` | Committee-annotated purpose override for bank-reported expenditures (type 311) |

**Record Type Routing:**

| Record Type IDs | Table | Description |
|---|---|---|
| 201, 202, 203, 204, 210, 211 | contributions | Monetary contributions (individual, committee, union, non-contrib receipt, payroll deduction, corporate) |
| 401, 402, 403, 405 | contributions | In-kind contributions (individual, committee, union, corporate) |
| 301, 303, 307, 308, 309, 311, 315, 316, 319, 332 | expenditures | Expenditures (various types including bank-reported, independent, reimbursements) |
| 206, 331, 501, 502 | loans_debts | Loans and liabilities (candidate loan, out-of-pocket loan, liability, IE liability) |
| 351, 354, 951, 952 | skipped | Sub-items of reimbursement/credit card/vendor wrappers — skipped to avoid double-counting the parent |
| 220, 320, 420 | skipped | Aggregated unitemized totals |
| all others | skipped | Bank/savings entries, internal accounting entries, transfers |

---

## Scraper

`src/pipeline/scrapers/massachusetts.py`

Downloads from Azure Blob Storage at `https://ocpf2.blob.core.windows.net/downloads/data2/`. No authentication required. The OCPF public downloads page (`https://ocpf.us/Data/Downloads`) is JavaScript-rendered and not used directly — file URLs are constructed from the known Azure path pattern.

**Filer registry:** `ocpf-filers.zip` is always re-fetched — the registry changes as committees register and dissolve throughout each cycle. The manifest records the download but does not suppress re-fetching.

**Year ZIPs:** One file per year from 2002 through the current year. Existing files are skipped on incremental runs (manifest check), except the current year which is always re-fetched. `--force` clears manifest entries in scope and re-downloads everything.

**Limitations:**
- ZIPs from recent active years can reach ~20 MB each and take several seconds to transfer. A 600-second read timeout is used to handle this.
- OCPF does not split files by transaction type — every year ZIP contains all report types interleaved.

**Expected runtime:** ~20–30s for a full sync (1 filer ZIP + 25 year ZIPs), or ~2–5s incremental (filer ZIP + current year only).

---

## Parser

`src/pipeline/parsers/massachusetts.py`

**Three-level join path:**

```
report-items.txt
  → reports.txt        (on Report_ID → CPF_ID, report_year, office, district, comm_name)
  → all_filers.txt     (on CPF_ID → committee_type, candidate_name, party, etc.)
```

Report-level fields (`OCPF_Office`, `OCPF_District`, `OCPF_Comm_Name`) are preferred over the static registry snapshot when both are present — they reflect the filer's position at the time of filing, which matters for candidates who sought multiple offices across cycles.

**Entity pass:** A single pass over `all_filers.txt` simultaneously builds the in-memory filer registry and writes `committees.csv.gz` and `candidates.csv.gz`. Candidate rows are written only for filers with a candidate name and a candidate-type account code (D/U/Z/z/N/O). When `Comm_Name` is blank (common in older pre-digital registrations), a committee name is synthesized as `"{Candidate Name} Committee"`.

**Transaction pass:** For each year ZIP, `reports.txt` is loaded first to build the `Report_ID → metadata` lookup (one pass). Then `report-items.txt` is streamed and each item is routed by `Record_Type_ID`.

**D102 Year-End Report deduplication:** OCPF's D102 form (Report_Type_ID = 11) is a year-end summary for candidate committees using the MA depository banking system. It re-lists all monetary contributions and expenditures already present in the periodic Deposit Reports (type 60) for that year — keeping both would double-count every transaction. The parser skips D102 monetary types (201–211, 301–332) for any CPF ID that also filed Deposit Reports in the same year ZIP. Two categories are always kept from D102 regardless:
- **In-kind types (401/402/403/405):** Deposit reports carry only cash transactions; D102 is the sole source of in-kind data for depository filers.
- **Loan/liability types (206/331/501/502):** D102 loan records are never exact duplicates of deposit-report loans (verified: 0 exact overlaps across all sample years).

**Bank-reported expenditures (type 311):** Massachusetts requires most candidate committees to use a state-designated depository bank, which files expense reports on the committee's behalf. These bank-reported entries can be annotated by the committee after the fact using `Clarified_Name` (overrides counterparty) and `Clarified_Purpose` (overrides description). The parser applies these overrides when present. Type-311 records account for roughly 49% of all expenditure rows and explain why Citizens Bank appears as the top payee by volume (it's the most common depository institution, not a campaign vendor).

**Reimbursement/credit card deduplication:** Types 307 (Reimbursement), 308 (Credit Card Payment), and 309 (Vendor Payment) are wrapper records. Their sub-items (351, 354, 951) are skipped to avoid counting the same dollar twice.

**Amendment handling:** OCPF's bulk ZIPs already contain only the latest version of each report. No amendment deduplication is needed in the parser.

**Key transformations:**
- Dates normalized from `MM/DD/YYYY` → `YYYY-MM-DD`
- `Name` + `First_Name` assembled into `"Last, First"` for individuals; plain `Name` used for organizations
- `Report_Year` used as `election_year` (the year the filing is attributed to, not necessarily the transaction date)
- `Item_ID` used as `filing_id`
- `N/A` in `Office Type Sought` / `District Name Sought` coerced to empty string

**person_id model:** `committee` — OCPF issues a new CPF ID for each committee registration. The same candidate running in multiple cycles gets a different CPF ID each time. `assign_person_ids` groups registrations by `(state, candidate_name, office, district)` and assigns `person_id` = min CPF ID across all registrations, prefixed with Massachusetts's FIPS code (25) to produce 14-digit integers.

**Output files:**

| File | Notes |
|---|---|
| `contributions.csv.gz` | ~5.96M rows (2002–2026) |
| `expenditures.csv.gz` | ~1.53M rows (2002–2026) |
| `loans_debts.csv.gz` | ~51K rows |
| `committees.csv.gz` | ~10,141 registered filers |
| `candidates.csv.gz` | ~7,242 candidate-type filers |

**Expected runtime:** ~3–4 minutes (dominated by streaming ~230 MB of year ZIPs through Python's zipfile module).

---

## Data Notes

- **Depository banking system.** Massachusetts is unusual in that most candidate committees are required to conduct all financial activity through a state-designated depository bank. The bank files reports on the committee's behalf (type 311 = Bank-Reported Expenditure), which is why bank-reported records account for ~49% of all expenditures and why depository banks (Citizens Bank, Bank of America) appear as top payees by transaction count.
- **D102 double-counting.** Without the deduplication logic described above, OCPF's Year-End D102 reports inflate contribution and expenditure totals by ~$72M across the full dataset. The most visible symptom was a blank-name "$10.76M Individual Contribution" to the Gabrieli Committee (2006) — the D102 annual rollup for Christopher Gabrieli's self-funded gubernatorial campaign.
- **In-kinds on D102 only.** For depository candidate committees, in-kind contributions (types 401–405) are reported exclusively on the D102 year-end form, not in periodic deposit reports. These are kept even when the D102 is otherwise suppressed.
- **Report-level vs. registry office/district.** `OCPF_Office` and `OCPF_District` in `reports.txt` reflect the office at time of filing. For candidates who sought different offices across cycles, this is more accurate than the static registry value and is preferred.
- **Employer/occupation fill rate.** Approximately 70% fill rate — OCPF only requires these fields for contributions above certain thresholds. Small contributions and many PAC/party committee receipts are filed without contributor details.
- **`candidate_name` on contributions (~57% fill rate).** PACs, party committees, ballot question committees, and independent expenditure committees have no associated candidate, so roughly 43% of contribution rows have a blank `candidate_name`. This is expected.
- **Raw state/zip data quality in older filings.** Contributions before ~2010 sometimes have digit codes in `contributor_state` (e.g. `"01"`, `"02"`) and Excel-apostrophe-prefixed zip codes (e.g. `"'01431"`). Expenditures have similar issues in `payee_state` and `payee_zip` (wildcard characters, multi-character strings). These are source artifacts and trigger tier-2 validator warnings but are kept as-is.
- **No pre-2002 data.** OCPF's bulk download system begins with the 2002 cycle. Earlier records exist in paper/scanned form only.
- **Gig-economy ballot questions.** The two largest non-candidate committees in the dataset are both registrations of the "Flexibility and Benefits for Massachusetts" ballot question committee (2022 Question 3), funded primarily by Uber, Lyft, DoorDash, and Instacart to a combined ~$73M. Both entries are legitimate separate registrations.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-28 |
| Parser | 2026-06-29 |
| Alias CSVs | 2026-06-28 |
| This document | 2026-06-29 |
