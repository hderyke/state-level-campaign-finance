# Alabama — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Alabama (AL) |
| **Source** | [Alabama Secretary of State FCPA System](https://fcpa.alabamavotes.gov) |
| **Access method** | Bulk CSV zip download (transactions) + JSON API & HTML scraping (committees) |
| **Coverage** | 2013 – present |

---

## Raw Data Structure

Files live in `data/Alabama/raw/`. There are two categories: transaction files (one per year per type) and committee registry files (scraped separately).

### Transaction Files

Four file types per year, named `{year}_{Type}Extract1.csv`:

#### CashContributions
| Field | Description |
|---|---|
| `CommitteeId` | Unique ID for the filing committee |
| `ContributionAmount` | Dollar amount |
| `ContributionDate` | Date of contribution (MM/DD/YYYY in raw) |
| `LastName` | Contributor last name, or full org name if individual fields are blank |
| `FirstName` | Contributor first name |
| `MI` | Middle initial |
| `Suffix` | Name suffix |
| `Address1` | Contributor street address |
| `City` | Contributor city |
| `State` | Contributor state |
| `Zip` | Contributor zip code |
| `ContributionID` | Unique transaction ID |
| `FiledDate` | Date the report was filed |
| `ContributionType` | e.g. "Cash (Itemized)" |
| `ContributorType` | e.g. "Individual", "Political Action Committee" |
| `CommitteeType` | e.g. "Principal Campaign Committee" |
| `CommitteeName` | Name of the filing committee |
| `CandidateName` | Associated candidate name (blank for PACs) |
| `Amended` | Y/N — whether this is an amended filing |

#### InKindContributions
Same schema as CashContributions, with these differences:
- Uses `InKindContributionID` instead of `ContributionID`
- Adds `NatureOfInKindContribution` (e.g. "Advertising")

#### Expenditures
| Field | Description |
|---|---|
| `CommitteeId` | Unique ID for the filing committee |
| `ExpenditureAmount` | Dollar amount |
| `ExpenditureDate` | Date of expenditure (MM/DD/YYYY in raw) |
| `LastName` / `FirstName` / `MI` / `Suffix` | Payee name parts |
| `Address1` / `City` / `State` / `Zip` | Payee address |
| `Explanation` | Free-text description (maps to `category` in cleaned output) |
| `ExpenditureID` | Unique transaction ID |
| `FiledDate` | Date the report was filed |
| `Purpose` | Standardized purpose code (e.g. "Advertising") |
| `ExpenditureType` | e.g. "Itemized" |
| `CommitteeType` / `CommitteeName` / `CandidateName` | Committee/candidate context |
| `Amended` | Y/N |

#### OtherReceipts
Same core fields as CashContributions (using `ReceiptAmount`, `ReceiptDate`, `ReceiptID`, `ReceiptType`, `ReceiptSourceType`), plus up to 3 endorser/guarantor sets:
`EndorserName1-3`, `EndorserAddress1-3`, `EndorserGuaranteedAMT1-3`

### Committee Registry Files

`pac_committees.csv` and `pcc_committees.csv` share the same schema:

| Field | Description |
|---|---|
| `committee_id` | Matches `CommitteeId` in transaction files |
| `committee_name` / `committee_type` / `pac_type` | Committee identity |
| `committee_status` | "Active" or "Dissolved" |
| `registered_date` / `dissolution_date` | Lifecycle dates |
| `address_line1` / `city` / `committee_state` / `zip_code` | Committee address |
| `phone` / `email` | Contact info |
| `purpose_of_pac` / `duration_of_pac` | PAC-specific metadata |
| `party` | Political party (PCC only) |
| `candidate_first` / `candidate_last` / `office` / `district` / `jurisdiction` | Candidate info (PCC only) |
| `treasurer_first` / `treasurer_last` / `treasurer_phone` / `treasurer_email` | Treasurer |
| `chairperson_first` / `chairperson_last` | Chairperson |
| `internal_id` | Internal FCPA system ID (used for API calls) |
| `downloaded_at` | Date the record was scraped |

---

## Scraper

`src/pipeline/scrapers/alabama.py`

**Transactions:** Requests IDs 1–56 from the FCPA bulk export endpoint. Each ID maps to one year+file-type combination and returns a zip containing a single CSV. A `manifest.csv` tracks what's been downloaded — existing files are skipped on re-run, except the current year which is always re-fetched.

**Committees:** Two-step process — (1) a paginated JSON API returns committee stubs (500 per page), then (2) each committee's detail page is fetched individually using base64-encoded ID and type parameters. Committee details are parsed out of a `committeeDetailsObj` JS variable embedded in the HTML response.

**Limitations:**
- SSL verification disabled site-wide (`verify=False`) due to a certificate issue on the FCPA server
- Transaction ID range (1–56) is hardcoded and will need updating as new years are added
- Committee detail scraping is fragile — depends on the structure of an embedded JS object in HTML

**Expected runtime:** Transactions ~1–2 min. Committee scraping: several minutes (0.25s sleep per request across thousands of committees).

---

## Parser

`src/pipeline/parsers/alabama.py`

Alabama is a **flat-file state**: `CandidateName` and `CommitteeName` appear on every transaction row rather than in separate entity filings. The parser synthesizes committees and candidates as it reads transactions, then enriches them using the registry files.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv`

**Key transformations:**
- `OtherReceipts` split by `ReceiptType` — `"Loan"` → `loans_debts`, everything else (refunds, interest, etc.) → `contributions`
- Contributor/payee names assembled from `LastName/FirstName/MI/Suffix`; a blank `FirstName` signals an organization stored entirely in `LastName`
- Dates normalized from `MM/DD/YYYY` → `YYYY-MM-DD`
- `Amended` Y/N → 1/0

**Limitations:**
- Malformed/column-shifted rows (non-numeric amount or missing `CommitteeId`) are silently skipped with a count printed to stdout
- Candidate-to-committee matching relies on exact `CandidateName` string match, which is fragile

**Expected runtime:** ~1–2 min (52+ CSVs across 13 years).

---

## Data Quirks

- **Flat-file structure** — no authoritative entity table exists in the raw download. Committees and candidates that filed no transactions in the download window will be missing entirely.
- **State field bug in InKind** — some `InKindContributions` rows have `"2"` in the `State` field instead of `"AL"`. Likely a data entry artifact on the portal, not a real out-of-state indicator.
- **Negative amounts** — ~4,400 contribution rows and ~535 expenditure rows have negative amounts, likely representing reversals or corrections. The parser does not filter these out.
- **0% candidate enrichment** — office, party, and district fields are empty for all candidates in the current output. The `CandidateName` string on transaction rows often doesn't match the PCC registry format exactly, so enrichment lookups silently fail. Known issue.
- **Low committee enrichment** — only ~7% of committees receive treasurer/address info from the registry. Many committees visible in transactions appear to be absent from the current API results (likely older or dissolved committees).
- **Endorser fields dropped** — `OtherReceipts` loans include up to 3 endorser/guarantor fields, but these are not carried into the `loans_debts` output table.
- **No Address2 or county** — the FCPA system only exposes `Address1` for contributor/payee addresses.

---

## Status

- [x] Scraper complete
- [x] Parser complete
- [x] Loaded into DB
- [ ] Verified / QA'd ⚠️ drift warnings in last test run — see `tests/reports/alabama_latest.json`


- [ ] Broken/needs maintainance

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-12 |
| Parser | 2026-05-13 |
