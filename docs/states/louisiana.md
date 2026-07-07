# Louisiana — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Louisiana (LA) |
| **Source** | [Louisiana Board of Ethics — Campaign Finance Downloadable Reports](https://www.ethics.la.gov/CampaignFinanceSearch/ShowPremadereports.aspx) |
| **Access method** | Direct unauthenticated HTTP GET — pre-built CSVs, no Playwright, no pagination |
| **Coverage** | 1995 – present |
| **person_id model** | `person` — `FilerNumber` is a stable person-level ID; `person_id = 2200<FilerNumber>` (FIPS 22 prefix + 12-digit zero-padded FilerNumber) |

---

## Raw Data Structure

27 files in `data/Louisiana/raw/` across three transaction types, each split into 4-year ranges:

| Filename pattern | Years |
|---|---|
| `{type}_1995_and_earlier.csv` | ≤ 1995 |
| `{type}_1996_to_1999.csv` | 1996–1999 |
| `{type}_2000_to_2003.csv` | 2000–2003 |
| `{type}_2004_to_2007.csv` | 2004–2007 |
| `{type}_2008_to_2011.csv` | 2008–2011 |
| `{type}_2012_to_2015.csv` | 2012–2015 |
| `{type}_2016_to_2019.csv` | 2016–2019 |
| `{type}_2020_to_2023.csv` | 2020–2023 |
| `{type}_2024_to_2027.csv` | 2024–present (always refreshed) |

Where `{type}` is `contributions`, `expenditures`, or `loans`.

### contributions_{range}.csv

| Field | Description |
|---|---|
| `FilerNumber` | Stable numeric ID for the filer — same across all cycles and form types |
| `FilerLastName` | Filer last name (or full committee name for PACs/committees) |
| `FilerFirstName` | Filer first name — blank for PAC/committee filers |
| `ReportCode` | Filing period code (e.g. `30P`, `10P`, `EDAY`, `ANN`, `MON`, `SUP`) — not written to output |
| `ReportType` | Form type code (e.g. `F102`, `F202`, `F305`) |
| `ReportNumber` | Unique report identifier (e.g. `"LA-118402"`) — written as `filing_id` |
| `ContributorTypeCode` | Contributor category code (23+ codes) — mapped to canonical labels via `contributor_types.csv` |
| `ContributorName` | Contributor name |
| `ContributorAddr1` / `ContributorAddr2` | Contributor street address |
| `ContributorCity` | Contributor city |
| `ContributorrState` | Contributor state — double-r typo in source; parser handles both spellings |
| `ContributorZip` | Contributor ZIP code |
| `ContributionType` | Transaction type (typically `CONTRIB`; `INKIND` for in-kind contributions) |
| `ContributionDescription` | Free-text description |
| `ContributionDate` | Date as `"M/D/YYYY 12:00:00 AM"` |
| `ContributionAmt` | Dollar amount |
| `ContributionDesignatedElectionAdditionInfo` | Election designation info — not written to output |

### expenditures_{range}.csv

| Field | Description |
|---|---|
| `FilerNumber` | Stable filer ID |
| `FilerLastName` / `FilerFirstName` | Filer name components |
| `ReportCode` / `ReportType` / `ReportNumber` | Filing metadata (same as contributions) |
| `Schedule` | Schedule code — written as `transaction_type`; interpretation varies by form type (see Parser section) |
| `RecipientName` | Payee name |
| `RecipientAddr1` / `RecipientAddr2` | Payee street address |
| `RecipientCity` / `RecipientState` / `RecipientZip` | Payee address |
| `ExpenditureDescription` | Free-text description |
| `CandidateBeneficiary` | Candidate benefited (populated for independent expenditures only) — written as `candidate_name` |
| `ExpenditureDate` | Date as `"M/D/YYYY 12:00:00 AM"` |
| `ExpenditureAmt` | Dollar amount |

### loans_{range}.csv

| Field | Description |
|---|---|
| `FilerNumber` | Stable filer ID |
| `FilerLastName` / `FilerFirstName` | Filer name components |
| `ReportCode` / `ReportType` / `ReportNumber` | Filing metadata |
| `ContributorTypeCode` | Lender type code |
| `LoanHolderName` | Lender name |
| `LoanHolderAddr1` / `LoanHolderAddr2` | Lender street address |
| `LoanHolderCity` / `LoanHolderState` / `LoanHolderZip` | Lender address |
| `LoanDate` | Date as `"M/D/YYYY 12:00:00 AM"` |
| `LoanAmt` | Dollar amount |
| `LoanRate` | Interest rate — not written to any output column (no rate field in `LOANS_DEBTS` schema) |

---

## Scraper

`src/pipeline/scrapers/louisiana.py`

Direct GET requests to the Ethics Board's pre-built CSV export URLs — no authentication, no Playwright, no pagination required.

The 2024–2027 file is always re-fetched on every run since the Ethics Board updates it in place as new filings arrive. All other files are cached via `manifest.csv` and skipped unless `--force` or `--start-year` is specified.

Year-range flags (`--start-year` / `--end-year`) operate at the 4-year range level: if the requested year falls within a range, the entire range file is re-downloaded (e.g. `--start-year 2020` re-downloads the 2020–2023 and 2024–2027 files).

`--entities`, `--candidates`, and `--committees` are no-ops: there is no separate filer registry endpoint. Entity data is derived from transaction files by the parser.

**Expected runtime:** ~2–5 min on first run (27 files; recent years are larger). Subsequent incremental runs download only the current-period file (~30 seconds).

---

## Parser

`src/pipeline/parsers/louisiana.py`

Three passes (contributions → expenditures → loans), writing output incrementally. Candidates and committees are built from the unique `FilerNumber` values collected across all three passes.

**Filer name:** `FilerFirstName + " " + FilerLastName` for candidates (e.g. `"Linton F. Broussard, Jr."`); `FilerLastName` alone for PACs/committees (e.g. `"Blue Line PAC"`). Passed through `utils.clean_name()`.

**Candidate vs. PAC:** `FilerFirstName` non-blank → candidate; `FilerFirstName` blank → PAC/committee. Individual filers always have a first name in the Louisiana source; PAC/committee filers use only `FilerLastName` for their full name.

**Date parsing:** Source format is `"M/D/YYYY 12:00:00 AM"`. The time portion is stripped before parsing.

**`transaction_type` — contributions:** `ContributionType` (typically `CONTRIB`; `INKIND` for in-kind contributions).

**`transaction_type` — expenditures:** `Schedule` code. Interpretation varies by form type:

| Schedule | Form type | Meaning |
|---|---|---|
| `E-1` | F102 (Candidate) | Contribution to another candidate/committee |
| `E-1` | F202 (Committee/PAC) | Contribution to a candidate |
| `E-2` | F102, F202 | Operational expenditure |
| `E-3` | F202 | Independent expenditure |
| `E-4` | All | Administrative/filing fees |
| `A`, `C`, `D` | F305 (Other Persons) | Operational expenditure |
| `D-2` | F305 | Contribution to a committee |
| `B` | All | Loan/debt repayment (Schedule B) |

All schedule codes are mapped to canonical categories via `expenditure_categories.csv`.

**`contributor_type`:** `ContributorTypeCode` (23+ codes) → canonical labels via `contributor_types.csv`. Key mappings: `IND`/`I` = Individual; `BUS`/`LEG`/`ORG`/`FIN`/`BAN` = Organization; `CAN` = Candidate; `PAC`/`COM`/`PCM`/`PCC` = PAC; `PTY` = Party Committee; `CHR`/`TRE`/`PRE`/`OFF`/`DTR`/`SCM` = Organization (officer/director role codes acting on behalf of an entity); `LBY` = Individual (lobbyist); `ANO`/`BNM`/`BEN`/`OTH` = suppressed.

**`filing_id`:** `ReportNumber` (e.g. `"LA-118402"`).

**`CandidateBeneficiary`** in expenditures: populated only for independent expenditures; written to `candidate_name` on expenditure rows.

**`_clean_state`:** Uppercases the raw state value; converts `N/A`, `NA`, `0`, and numeric-only codes to `""`.

**Deduplication:** The bulk CSVs contain exact duplicate rows (all fields identical) — approximately 3,500 per 4-year contributions file and 1,800 per expenditures file. The parser deduplicates within each source file using a full-row tuple set (`seen_rows: set[tuple]`). Only fully identical rows are removed; rows that differ only in `ReportNumber` (amended reports) are retained.

**person_id model:** `person` — `FilerNumber` is a stable person-level integer ID assigned by the Ethics Board, consistent for the same filer across all election cycles, committee types, and years. `state_filer_id = FilerNumber`. `assign_person_ids(id_model="person")` sets `person_id = 2200<FilerNumber>` (FIPS 22 prefix + 12-digit zero-padded FilerNumber).

**Expected runtime:** ~2–3 min (27 files, contributions and expenditures files are large).

---

## Data Notes

- **No office, party, or district data** — the bulk CSVs do not include office sought, party affiliation, or district for candidates. These fields are blank in all output. Filer profile pages exist on the Ethics Board website (searchable by `FilerNumber`) but there is no bulk export of this data.
- **`committee_type` blank** — Louisiana does not provide committee type (candidate committee vs. PAC vs. party) in the bulk CSVs. The parser infers candidate vs. PAC from `FilerFirstName` presence but does not distinguish sub-types within each category.
- **`LEG` is the second-largest contributor type** — "Legal Entity" (`LEG`) accounts for ~12% of contributions ($476M+). It covers LLCs, corporations, law firms, and other business entities that did not file as `BUS`. Mapped to Organization.
- **Amended report duplicates** — when the Ethics Board includes both an original and an amended report in the same bulk export, the same transaction appears twice with different `ReportNumber` values (e.g. `LA-56116` and `LA-56117`). These are not caught by full-row dedup. The result is a small number of inflated totals for the affected filers. The most visible example: a ~$2M Republican Governors Association contribution on 2015-10-26 appears twice.
- **`ReportCode` filing period codes** — `ANN` (annual), `MON` (monthly), `SUP` (supplemental), `30P`/`10P` (pre-election), `EDAY` (election day), `SPCL` (special election), `180P`/`90P`/`40P`/`10G`/`40G` (other pre-election windows). Describes the filing period, not the transaction type. Not written to any output column.
- **UTF-8 BOM** — all source CSVs have a UTF-8 BOM. Parser opens files with `encoding="utf-8-sig"` to strip it.
- **`LoanRate` not captured** — the loans files include an interest rate field (`LoanRate`) with no corresponding column in the `LOANS_DEBTS` schema. It is not written to output.
- **Invalid contributor state codes** — a small number of rows have non-standard state codes in `ContributorrState` (e.g. `C`, `DI`, `L`, `LO`, `AE`). These are genuine source data errors and are not corrected.
- **Invalid payee state codes** — similarly, a small number of expenditure rows carry foreign codes in `RecipientState` (e.g. `BC`, `GB`, `JP`, `ON`) corresponding to Canadian provinces and other foreign jurisdictions. Not corrected.
- **Negative ZIP codes** — a small number of contributor ZIP values are negative integers in the source. Not corrected.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-26 |
| Parser | 2026-06-27 |
| Aliases | 2026-06-27 |
| Documentation | 2026-06-27 |
