# Minnesota (MN)

## Overview

| | |
|---|---|
| **State** | Minnesota (MN) |
| **Source** | [Minnesota Campaign Finance Board (CFB)](https://cfb.mn.gov/reports-and-data/self-help/data-downloads/campaign-finance/) — bulk CSV downloads + viewer API |
| **Access method** | Unauthenticated GET for transaction files; POST to viewer API (PHPSESSID cookie required) for entity details |
| **Coverage** | 2015 – present |
| **person_id model** | `person` — `CandidateMasterNameID` is a stable cross-cycle person ID for candidates; `person_id` set directly |

## Raw Data Structure

Three bulk transaction files (full history, single download each) plus three entity JSON caches built by the scraper.

### Transaction Files

#### mn_contributions.csv (~550K rows)

| Field | Description |
|---|---|
| `Recipient reg num` | `RegisteredEntityID` of the receiving committee — joins to entity JSON |
| `Recipient` | Committee name as it appears in the transaction data |
| `Recipient type` | Entity type: `PCC`, `PCF`, or `PTU` |
| `Recipient subtype` | Sub-classification (rarely populated) |
| `Amount` | Dollar amount (plain decimal, no `$` or commas) |
| `Receipt date` | Date of receipt (M/D/YYYY) |
| `Year` | Election year associated with the transaction |
| `Contributor` | Contributor name |
| `Contrib Reg Num` | Contributor's own CFB registration number (if registered filer) |
| `Contrib type` | Contributor registration type: `Individual`, `Political Committee/Fund`, `Party Unit`, `Candidate Committee`, `Lobbyist`, `Self`, `Corporation`, `Other` |
| `Receipt type` | Transaction type: `Donation`, `In-Kind`, `Non-Itemized`, `Loan Payable`, `Interest Income`, `Return Excess Contribution`, etc. |
| `In kind?` | `Y`/`N` — flags in-kind contributions (parser uses this to override `Receipt type` as `"In-Kind"`) |
| `In-kind descr` | Free-text description for in-kind contributions |
| `Contrib zip` | Contributor ZIP code only (no city or state in source) |
| `Contrib Employer name` | Contributor employer |

#### mn_expenditures.csv (~361K rows)

| Field | Description |
|---|---|
| `Committee reg num` | `RegisteredEntityID` of the spending committee |
| `Committee name` | Committee name from the transaction record |
| `Entity type` | `PCC`, `PCF`, or `PTU` |
| `Entity sub-type` | Sub-classification |
| `Vendor name` | Payee name |
| `Vendor city` / `Vendor state` / `Vendor zip` | Payee address components |
| `Amount` | Dollar amount |
| `Unpaid amount` | Accrued but unpaid portion (not written to output) |
| `Date` | Expenditure date |
| `Purpose` | Free-text expenditure description |
| `Year` | Election year |
| `Type` | Expenditure type: `General Expenditure`, `Campaign Expenditure`, `Non-Campaign Disbursement`, `Contribution`, `Other Disbursement`, `Ballot Question Expenditure` |
| `In-kind descr` / `In-kind?` | In-kind flag and description (column order differs from contributions) |
| `Affected committee name` / `Affected committee reg num` | For contributions to other committees |

#### mn_ind_expenditures.csv (~41K rows)

Independent expenditure filings — merged into `expenditures.csv.gz` by the parser.

| Field | Description |
|---|---|
| `Spender` | Spending committee name |
| `Spender Reg Num` | `RegisteredEntityID` of the spender |
| `Spender type` / `Spender sub-type` | Entity type of the spender |
| `Affected Comte Name` | Name of the supported/opposed committee (typo in source header) |
| `Affected Cmte Reg Num` | Registration number of the affected committee |
| `For /Against` | `"For"` or `"Against"` — merged into `transaction_type` by the parser |
| `Year` / `Date` / `Type` / `Amount` / `Unpaid amount` | Standard transaction fields |
| `In kind?` / `In kind descr` | Note: no hyphen (differs from regular expenditure headers) |
| `Purpose` | Free-text purpose |
| `Vendor name` / `Vendor city` / `Vendor State` / `Vendor zip` | Note: `Vendor State` has capital S (differs from regular expenditure header) |

### Entity JSON Files

Built by the scraper from the CFB viewer APIs. Keyed by `RegisteredEntityID`.

#### candidates_entity.json (~1,976 entries)

POST to `/reports-and-data/viewers/campaign-finance/candidates/api`. Response fields include `RegisteredEntityID`, `CandidateMasterNameID` (stable person-level ID), `CommitteeName`, `CandidateFullName` (format: `"Last, First"`), `PartyAffiliation`, `OfficeSoughtFullName`, `District`, `ElectionYear`, `Incumbent`, `TerminationDate`, `RegisteredEntityType` (`PCC`).

#### committees_entity.json (~770 entries)

POST to `/reports-and-data/viewers/campaign-finance/political-committee-fund/api`. Uses `FormattedName` instead of `CommitteeName`; includes address fields (`City`, `State`, `ZipCode`); no `CandidateFullName` or `CandidateMasterNameID`. `CommitteeType` contains a verbose description.

#### party_units_entity.json (~456 entries)

POST to `/reports-and-data/viewers/campaign-finance/party-unit/api`. Same structure as PCF (uses `FormattedName`, has address fields). No candidate or election-year fields.

## Scraper

`src/pipeline/scrapers/minnesota.py`

**Transactions:** Three plain GET requests to the CFB self-help download page using `?download={id}` parameters. Full-history files are always re-fetched on each sync run (no year-splitting, manifest only used to track entity sweep progress). Manifest is intentionally skipped for the transaction files so that `sync` always pulls fresh data.

**Entities:** POSTs to three viewer API endpoints (candidates, political-committee-fund, party-unit). Requires a `PHPSESSID` cookie obtained by GETting the viewer page before POSTing. Builds a `(reg_num → max_year)` map from the already-downloaded transaction CSVs to determine which reg_nums to fetch. Results are cached in JSON files — incremental runs only fetch reg_nums not yet in the cache. `--force` clears entity caches so all reg_nums are re-fetched.

**Year logic for entity POST:** MN uses biennial election cycles. For year Y, POST body includes `ElectionSegmentEndDate=Y` and `ElectionSegmentStartDate=Y-1`. If the entity returns no data at the current year, the scraper falls back to the max transaction year for that reg_num.

**Limitations:**
- The CFB viewer API WAF blocks POST requests from datacenter/VPS/cloud IPs (same restriction as Florida). Entity downloads (`--entities`) must be run from a local machine with a residential or institutional IP.
- Entity details for historical filers that no longer appear at any year may return empty `{}` — these are cached as empty and skipped by the parser.

**Expected runtime:** ~45 min total (three large CSV downloads: 74 MB + 61 MB + 8.6 MB). Entity sweep timing depends on cache warmth (~1,976 + 770 + 456 = ~3,200 POST requests at ~0.2s each = ~11 min additional).

## Parser

`src/pipeline/parsers/minnesota.py`

**person_id model:** `person` — `CandidateMasterNameID` from the CFB viewer API is a stable person-level ID (persists across election cycles and committee re-registrations). Written as `state_filer_id` in `candidates.csv.gz`; `person_id` set directly via `assign_person_ids(id_model="person")`.

**Entity name resolution:** PCC entries use `CommitteeName`; PCF and PTU entries use `FormattedName` (different field name from the same API family). The parser falls back to the raw `Recipient`/`Committee name`/`Spender` column from the transaction file when an entity's reg_num is not in the JSON cache.

**Candidates de-duplication:** A candidate (person) may appear across multiple `RegisteredEntityID` values (one per election cycle). The parser groups by `CandidateMasterNameID` and keeps the entry with the highest `ElectionYear`, yielding one row per person. 1,695 unique candidates from 1,976 PCC registry entries.

**Committee types:** `RegisteredEntityType` codes (`PCC`/`PCF`/`PTU`) are mapped to canonical display values at parse time (`Candidate Committee` / `PAC` / `Party Committee`) so that per-state DB queries (which don't apply alias tables) work correctly.

**Loans:** `Receipt type == "Loan Payable"` rows from contributions (~14 rows) are diverted to `loans_debts.csv.gz` with `record_type = "Loan Payable"`.

**Independent expenditures:** Merged into `expenditures.csv.gz`. The `For /Against` field is appended to the base `Type` to produce `transaction_type` values like `"Independent Expenditure For"` / `"Independent Expenditure Against"`. The `Affected Comte Name` field (a CFB header typo) is folded into `purpose` in brackets.

**Contributor state inference:** MN's contribution data includes ZIP but not city or state for contributors. The parser infers `contributor_state` from the first three digits of `Contrib zip` using a hardcoded USPS ZIP3-prefix → state lookup table. Accuracy is ~97-98% for domestic addresses; foreign, military, and unassigned prefixes return blank. 4-digit ZIPs with a dropped leading zero (e.g. `"1001"` → `"01001"`) are zero-padded before lookup.

**Output:** 550,461 contributions, 402,402 expenditures (including ~40,949 IE rows), 14 loans, 2,966 committees, 1,695 candidates.

**Expected runtime:** ~21s.

## Data Notes

- **Contributor city/state not in source** — MN's bulk download only includes `Contrib zip`, not city or state. `contributor_city` is always blank. `contributor_state` is inferred from ZIP prefix (~99.8% fill rate after inference).
- **Occupation not in source** — the CFB bulk download does not include contributor occupation. `occupation` is always blank.
- **Jurisdiction not in source** — the CFB viewer API returns `OfficeSoughtFullName` and `District` but not `jurisdiction`. Always blank for MN.
- **6% of candidates have no district** — statewide offices (Governor, AG, Secretary of State, etc.) return `null` for `District` in the API. Expected.
- **PCF/PTU committees have no PCC addresses** — the PCC viewer API does not return address fields; only PCF and PTU entries have `City`/`ZipCode` populated. `city` and `zip` on the committees table are ~39% filled (PCF+PTU only).
- **Treasurer not in source** — the CFB API does not expose treasurer name. `treasurer_name` always blank.
- **IE header inconsistencies** — `mn_ind_expenditures.csv` contains several header name quirks relative to `mn_expenditures.csv`: `"Vendor State"` (capital S), `"In kind?"` and `"In kind descr"` (no hyphen), and `"Affected Comte Name"` (typo for "Affected Committee Name"). Parser handles these explicitly.
- **Contributor type "Unknown/Null"** — 556 rows (~0.1%) have `Contrib type = "Unknown/Null"`. Suppressed in alias mapping.
- **"Registered with Hennepin County" contributor type** — 228 rows with this unusual value, likely a local data-entry artifact. Suppressed in alias mapping.
- **General Expenditure** — the single largest expenditure type (31.9% of rows, $277M) covers routine operating spending that predates the CFB's more granular type codes.
- **MN DFL name inconsistency** — the MN DFL State Central Committee appears under two names: `"MN DFL State Central Committee"` in transaction data (from the `Recipient`/`Committee name` columns) and `"Minn DFL State Central Committee"` from the entity JSON (`FormattedName`). Both refer to the same committee. The entity JSON name takes precedence when the reg_num is in cache.
- **Biennial cycle data sparsity** — MN's off-cycle years (2017, 2019, 2021, 2023, 2025) show ~29K-42K contributions vs 45K-95K in election years. Expected — most MN legislative races occur in even years.

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-01 |
| Parser | 2026-07-01 |
