# Arizona — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Arizona (AZ) |
| **Source** | [Arizona SeeTheMoney](https://seethemoney.az.gov) |
| **Access method** | Playwright browser automation (transactions) + requests session (committee registry & details) |
| **Coverage** | 1998 – present + `Recall_Fann` special election cycle |

---

## Raw Data Structure

Files live in `data/Arizona/raw/`. Transaction files are organized by **election cycle** (not calendar year) and filer type, plus two committee registry files.

### Transaction Files

Named `{category}_{cycle}_{filer_type}.csv` — e.g. `Income_2024_Candidate.csv`:

- **Categories:** `Income`, `Expenditures`
- **Filer types:** `Candidate`, `PAC`, `Party`, `Officeholder`
- **Cycles:** 1998, 2000, 2002, ... 2026 (each covers a ~2-year window), plus `Recall_Fann` (July 2021–March 2022)

| Field | Description |
|---|---|
| `FilerName` | The committee/candidate that received the income or made the expenditure. Format is "Last, First" for candidates. **Blank for most PAC and Party rows** — see Data Quirks. |
| `TransactionDate` | Date of transaction (`12/31/2024 12:00:00 AM` format in raw) |
| `CommitteeName` | Counterparty committee name (contributing committee, or payee committee) |
| `Amount` | Dollar amount formatted as `5000.0000` (4 decimal places) |
| `TransactionName` | Counterparty name: contributor (Income) or payee (Expenditures) |
| `TransactionType` | Transaction type label (e.g. "Monetary Contribution", "In-Kind") |
| `Occupation` | Contributor/payee occupation |
| `Employer` | Contributor/payee employer |
| `City` | Contributor/payee city |
| `State` | Contributor/payee state |
| `ZipCode` | Contributor/payee zip code |

### Committee Registry — `az_committees_all.csv`

Full registry of all filer types (Candidate, PAC, Party, BallotMeasure, Officeholder, Other), 43K+ entities:

| Field | Description |
|---|---|
| `entity_id` | Primary key — used as `state_filer_id` in cleaned output |
| `public_transaction_table_id` | Internal table ID |
| `filer_type` | Registry page category (e.g. "Candidate", "PAC") |
| `entity_type_name` | Detailed entity type (e.g. "Candidate (not participating in Clean Elections)") |
| `committee_name` | Official committee name |
| `entity_last_name` / `entity_first_name` / `entity_middle_name` | Candidate name parts |
| `office_name` | Office sought |
| `party_name` | Political party |
| `city` / `state` / `zip` | Committee address |
| `income` / `expense` / `cash_balance` | Running financial totals from the portal |
| `ie_support` / `ie_opposition` | Independent expenditure amounts |
| `ballot_measure_id` / `ballot_name` | Ballot measure info (where applicable) |
| `identifier` | Additional identifier field |

### Committee Details — `az_committee_details.csv`

Per-committee detail record scraped separately from each entity's detail page:

| Field | Description |
|---|---|
| `entity_id` | Matches `entity_id` in `az_committees_all.csv` |
| `committee_name` / `committee_type_name` / `status` | Committee identity and status |
| `registration_date` / `last_amended_date` / `last_filed_date` | Lifecycle dates |
| `phone` / `email` | Contact info |
| `mailing_address` / `filer_address` / `city` / `state` / `zip` / `county` | Address fields |
| `chairman_name` / `treasurer_name` | Leadership |
| `master_committee_id` | Parent committee linkage |

---

## Scraper

`src/pipeline/scrapers/arizona.py`

Arizona's SeeTheMoney portal uses a hash-based URL scheme and requires an authenticated browser session to trigger CSV exports. The scraper uses a **hybrid approach**: Playwright for transactions, a `requests` session (seeded with browser cookies) for the committee registry API.

**Transactions:** For each combination of cycle, filer type, and category (Income/Expenditures), Playwright navigates to a constructed hash URL, clicks the Export button, selects CSV format, and downloads the file. Response may be UTF-16 encoded and is normalized to UTF-8. A manifest tracks completed `(cycle, filer_type, category)` triples.

**Committee registry (`az_committees_all.csv`):** Posted to the `GetTableData` endpoint (6 pages, one per filer-type group) using the cookie-seeded requests session. Returns JSON arrays normalized to CSV.

**Committee details (`az_committee_details.csv`):** Each entity's detail is fetched individually via `GetDetailedInformation` (0.15s sleep per request). Details include treasurer, chairman, address, and status not available in the registry summary.

**Limitations:**
- Playwright required for transactions (`pip install playwright && playwright install chromium`); runs headless=False
- Cycle date ranges and IDs are hardcoded — will need updating for future election cycles
- Committee detail scraping can take a long time at scale (43K+ entities × 0.15s)
- No `Amended` flag in exports — `amended` is hardcoded to 0 in all output rows

**Expected runtime:** Transactions: 30–60 min (16 cycles × 4 filer types × 2 categories = 128 files). Registry: ~5 min. Committee details: ~2–3 hours for a full refresh.

---

## Parser

`src/pipeline/parsers/arizona.py`

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv` (empty — no loan data available)

**Registry join strategy:**
- Candidate files: `FilerName` is "Last, First" — last name extracted and looked up in `entity_last_name` index
- Officeholder/PAC/Party files: `FilerName` matched against `committee_name` index, with `entity_last_name` as fallback
- On match, `entity_id` is used as `state_filer_id`; on miss, the raw `FilerName` string is used as a fallback

**Key transformations:**
- Amounts normalized from `5000.0000` → plain float string; parenthetical negatives converted
- Dates normalized from `12/31/2024 12:00:00 AM` → `YYYY-MM-DD`; implausible years discarded
- Candidate names converted from "Last, First" → "First Last" for the `candidate_name` column
- Committees and candidates both derived from `az_committees_all.csv` (candidates = rows where `entity_type_name` contains "Candidate")
- `amended` hardcoded to 0 (no amendment flag in source data)

**Limitations:**
- PAC and Party files have a blank `FilerName` for the vast majority of rows — these rows get no `state_filer_id` (see Data Quirks)
- Registry lookup is by last name for candidates, meaning two candidates with the same last name may resolve to the wrong committee
- No loans/debts data; `loans_debts.csv` is always empty

**Expected runtime:** ~10–20 min (128+ CSVs across 16 cycles).

---

## Data Quirks

- **PAC/Party files have no per-row filer ID** — `FilerName` is blank for the vast majority of PAC and Party transaction rows. These are aggregate exports without a per-row committee identifier. As a result, ~48% of contributions and ~67% of expenditures have an empty `state_filer_id` in the cleaned output. Known limitation of the SeeTheMoney export format.
- **Election cycle files, not calendar year** — each cycle covers a 2-year window (e.g. the "2024" cycle covers 1/1/2023–12/31/2024). The `election_year` column reflects the cycle label, not necessarily the transaction date year.
- **`Recall_Fann` special cycle** — a separate election cycle for the 2021 Arizona Senate recall election (Karen Fann). Covers July 2021–March 2022. Included alongside the regular cycles.
- **No loans or debts data** — SeeTheMoney exports don't include a loans/debts table. `loans_debts.csv` is always empty.
- **`amended` always 0** — the export format provides no amendment indicator. All rows get `amended = 0` regardless of whether the filing was amended.
- **Large contributions drift warning** — the last QA run flagged a 97.3% drop in contribution row count vs. the previous run (10M → 278K). This is likely a manifest or export issue and should be investigated before relying on contribution totals.
- **Officeholder files are very sparse** — Officeholder cycle files typically contain only a handful of rows (21 rows in 2026), reflecting the narrow use of that filer category.
- **No treasurer info in cleaned committees** — `az_committee_details.csv` has treasurer/chairman data but the parser doesn't currently join it into the committees output.

---

## Status

- [x] Scraper complete
- [x] Parser complete
- [x] Loaded into DB
- [ ] Verified / QA'd ⚠️ large contribution count drift — see `tests/reports/arizona_latest.json`

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-12 |
| Parser | 2026-05-13 |
