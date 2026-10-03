# Arizona — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Arizona (AZ) |
| **Source** | [Arizona Secretary of State SeeTheMoney](https://seethemoney.az.gov/Reporting/AdvancedSearch/) |
| **Access method** | `requests` session — no browser required |
| **Coverage** | 1998 – present |
| **person_id model** | `committee` — new `entity_id` per registration cycle; `person_id` = min ID for a given `(candidate_name, office, district)` |

---

## Raw Data Structure

Three categories: transaction files (one per cycle × filer type × category), a committee registry, and committee detail records.

### Transaction Files

Named `{Category}_{Cycle}_{FilerType}.csv` — e.g. `Income_2024_Candidate.csv`, `Expenditures_2022_PAC.csv`.

Cycles span two-year election periods. One special cycle exists: `Recall_Fann` (2021–2022 recall election).

Filer types: `Candidate`, `PAC`, `Party`, `Officeholder`
Categories: `Income` (contributions received), `Expenditures` (spending)

| Field | Description |
|---|---|
| `CommitteeID` | Numeric filer ID for the receiving committee |
| `CommitteeName` | Full name of the receiving committee |
| `TransactionDate` | Date of transaction (`.NET Date(ms)` format in raw, normalized to `YYYY-MM-DD`) |
| `Amount` | Dollar amount (e.g. `5000.0`) |
| `TransactionName` | Counterparty — contributor name (Income) or payee name (Expenditures) |
| `TransactionType` | e.g. `"Contribution from Individuals"`, `"CCEC $5 Qualifying Contribution"`, `"Operating Expense"` |
| `Occupation` | Contributor occupation (Income only) |
| `Employer` | Contributor employer (Income only) |
| `City` | Counterparty city |
| `State` | Counterparty state |
| `ZipCode` | Counterparty zip code |
| `FirstName` / `LastName` | Counterparty first and last name (individuals only) |
| `FilerName` | Committee or candidate identifier — "Last, First" for Candidate/Officeholder; full committee name or blank for PAC/Party |
| `Memo` | Free-text memo field |

### Committee Registry

`az_committees_all.csv` — full registry of all filer types fetched from the SeeTheMoney reporting API.

| Field | Description |
|---|---|
| `entity_id` | Unique numeric filer ID (used as `state_filer_id` in cleaned output) |
| `filer_type` | Registry page category: Candidate, PAC, Party, BallotMeasure, Officeholder, Other |
| `entity_type_name` | Detailed entity type (e.g. `"Candidate"`, `"$500 Threshold Candidate"`) |
| `committee_name` | Full committee name |
| `entity_last_name` / `entity_first_name` / `entity_middle_name` | Candidate name (Candidate filer type only) |
| `office_name` | Office sought |
| `party_name` | Political party |
| `city` / `state` / `zip` | Committee address |
| `income` / `expense` / `cash_balance` | Aggregate financial figures |
| `ie_support` / `ie_opposition` | Independent expenditure totals |
| `ballot_measure_id` / `ballot_name` | Ballot measure info (BallotMeasure type only) |

### Independent Expenditure Files

Named `IndependentExpenditures_{Cycle}.csv` — one file per cycle, itemized IE transactions across all spending committees. Separate raw source from the transaction files above: fetched from a different pair of endpoints (see Scraper below), not the `AdvancedSearch` flow.

| Field | Description |
|---|---|
| `TransactionId` / `PublicTransactionId` | Transaction identifiers (source's own IDs) |
| `TransactionDate` | Date of transaction (`.NET Date(ms)` format in raw, normalized to `YYYY-MM-DD`) |
| `CommitteeId` / `CommitteeName` | The committee making the independent expenditure |
| `Amount` | Dollar amount |
| `BenefitedOpposed` | `"Benefited"` or `"Opposed"` — which side of the race this IE was for |
| `SubjectCommitteeId` / `SubjectCommitteeName` | The candidate committee this IE concerns — `SubjectCommitteeId` matches `entity_id` in `az_committees_all.csv` directly |
| `TransactionFirstName` / `TransactionMiddleName` / `TransactionLastName` | Payee name (individuals) |
| `TransactionType` | e.g. `"Ind. Expend. (Non-Recall) - cash"`, `"Independent Expenditure - Pay Cash/Check"`, `"Recall Ind. Expend. - Pay Cash/Check"` |
| `TransactionCity` / `TransactionState` / `TransactionZipCode` | Payee address |
| `TransactionOccupation` / `TransactionEmployer` | Payee occupation/employer |
| `Memo` | Free-text memo field |

### Committee Detail Records

`az_committee_details.csv` — enriched committee records fetched per-entity from the `GetDetailedInformation` API endpoint.

| Field | Description |
|---|---|
| `entity_id` | Matches `entity_id` in registry |
| `committee_name` | Full committee name |
| `committee_type_name` | Detailed committee type |
| `status` | Registration status (e.g. `"Active"`, `"Terminated"`) |
| `registration_date` / `last_amended_date` / `last_filed_date` | Lifecycle dates (normalized to `YYYY-MM-DD`) |
| `phone` / `email` | Committee contact info |
| `mailing_address` / `filer_address` | Addresses |
| `city` / `state` / `zip` / `county` | Location |
| `chairman_name` / `treasurer_name` | Officers |
| `master_committee_id` | Parent committee reference |

---

## Scraper

`src/pipeline/scrapers/arizona.py`

**No browser required.** The AdvancedSearch endpoint accepts plain HTTP POSTs once the correct request structure is used. A `requests.Session` with minimal headers and a randomly-generated `SeeTheMoneyUserHistory` cookie is sufficient — the cookie is client-side SPA state only and is not validated server-side.

**Request structure (critical detail):** The endpoint requires search criteria in the URL *query string* and pagination/column spec in the POST body as a DataTables payload (`draw`, `columns[N][data]`, `start`, `length`, `order`). Sending search params in the POST body returns an empty response. This structure was confirmed from a browser network capture.

**Pagination:** offset-based via the `start` parameter (0, 3000, 6000, …). `TABLE_LENGTH = 3000` rows per page — the server's ASP.NET `maxJsonLength` rejects responses above roughly 5000 rows for large files.

**Parallel downloads:** `PARALLEL_WORKERS = 4` concurrent threads via `ThreadPoolExecutor`. Each thread creates its own `build_session()` to avoid shared state. Manifest writes are serialized with a `threading.Lock`.

**Registry:** Fetched via `POST /Reporting/GetTableData` — one request per registry page (six total). Uses the same `build_session()` approach.

**Committee details:** Fetched via `POST /Reporting/GetDetailedInformation` per entity ID, with a 0.15s sleep between requests.

**Manifest:** `manifest.csv` tracks completed cycle × filer type × category combinations.

**Incremental scope:** a normal (no-flag) run touches only the current calendar year — it fetches transactions dated Jan 1 of this year onward (via the API's `StartDate` parameter, with a client-side date guard) and merges them into the active cycle's file, preserving the prior-year rows already on disk. Cycles are two-year and labeled by their even end year, so the active cycle is detected by date range, not label. Historical cycles are immutable and only re-fetched with `--force` or `--start-year`/`--end-year`. If the active cycle's file is missing, the full cycle is fetched instead. Note this means amendments to prior-year transactions are not picked up by a normal run — use `--start-year` to refresh the full cycle.

**Limitations:**
- Must be run locally — the site blocks datacenter IPs. `build_session()` uses a real browser User-Agent and `Sec-Fetch-*` headers to avoid WAF fingerprinting.
- A full historical run covers ~128 combinations (16 cycles × 4 filer types × 2 categories) plus the Recall_Fann cycle.
- A diagnostic function (`run_diagnostic()`, invoked with `--diag`) that uses Playwright remains in the file for debugging network traffic. It is not part of the normal run path.

**Expected runtime:** ~1–2 hours for a full `--force` run with 8 parallel workers (dominated by the large PAC Income files: 2024 PAC Income ~919k rows, 2026 PAC Income ~780k rows). Incremental runs are much faster since most historical cycles are already in the manifest.

**Independent Expenditures (`--independent-expenditures`):** a separate sweep, not part of the normal Income/Expenditures flow above. Two-step per cycle:
1. `POST /Reporting/GetNEWTableData` (`ChartName=5`) — one page (paginated via `IE_SUMMARY_LENGTH`) giving every candidate committee with a nonzero `IESupport` or `IEOpposition` total in the cycle's year window.
2. `POST /Reporting/GetNEWDetailedTableData` (`ChartName=60` for support, `61` for oppose, `Name=5~{entityId}`) — one call per `(entity, side)` pair from step 1 with a nonzero total on that side, itemizing the underlying transactions. Fetched concurrently across `PARALLEL_WORKERS` threads, each with its own session.

Writes `IndependentExpenditures_{cycle_label}.csv`, deduped on the source's own `TransactionId`. Logged with `page_scrape_*` (a sweep over many entities, like `download_committee_details()`) rather than `file_download_*`.

**Calendar-year granularity (important limitation):** unlike `AdvancedSearch`'s day-precision date filtering, these two endpoints only accept whole calendar years (`startYear`/`endYear`). A cycle whose day-level date range reduces to the same `(startYear, endYear)` pair as another cycle — most notably `Recall_Fann` (7/2021–3/2022) vs. the `2022` cycle (1/2021–12/2022), both → years 2021–2022 — would pull an identical dataset if fetched separately. `download_independent_expenditures_all()` sweeps each distinct year-window once per run and aliases any other cycle sharing it to that file (a zero-row manifest entry pointing at the source cycle's filename) rather than re-fetching. This only affects the IE sweep; the main Income/Expenditures scrape is unaffected since `AdvancedSearch` filters by exact date.

This same year-granularity also means **adjacent historical cycles genuinely overlap by one calendar year** (e.g. `2016` = years 2014–2016, `2014` = years 2012–2014 — both include 2014's IE data), which the window-aliasing above does *not* catch since the windows aren't identical, just overlapping. The parser's global `TransactionId` dedup (see below) is the actual safety net for this broader case.

---

## Parser

`src/pipeline/parsers/arizona.py`

Arizona is a **registry-joined state**: transaction files contain `FilerName` as a string identifier. The parser loads the committee registry into two lookup dicts (by last name and by committee name) and joins each transaction row to get the canonical committee name and `entity_id`. `CommitteeID` and `CommitteeName` from the API response are written directly — the registry join is a fallback for cases where those fields are blank.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv`

**Key transformations:**
- `CommitteeID` and `CommitteeName` written directly from the API response — previously absent from the old CSV export, which was the original motivation for switching to the JSON API.
- `contributor_type` derived from `TransactionType` (e.g. `"Contribution from Individuals"` → `"Individual"`, `"CCEC $5 Qualifying Contribution"` → `"Individual"`, `"Contributions from PACs"` → `"PAC"`). Normalized to canonical values at aggregate time via `src/aliases/contributor_types.csv`.
- `office_name` values like `"State Representative - District 20"` are split into `office = "State Representative"` and `district = "20"`. Handles both `"- District N"` and `"- District No. N"` variants.
- `FilerName` join strategy: Candidate files use `entity_last_name` index (FilerName is "Last, First"); PAC/Party/Officeholder files try full committee name first, then last-name fallback.
- Dates normalized from `.NET Date(ms)` timestamps to `YYYY-MM-DD`.
- `committees.csv` and `candidates.csv` are derived from the registry, not from transaction rows.
- Candidates filtered to `entity_type_name` values containing `"Candidate"` or `"$500 Threshold"`.
- `loans_debts.csv` is always written empty — SeeTheMoney does not distinguish loan transactions.
- **Independent Expenditures → `expenditures.csv`:** `IndependentExpenditures_*.csv` files are parsed separately from the Income/Expenditures loop above and folded into the same `expenditures.csv` output (`category = "Independent Expenditure"`). `committee_name` is the IE-spending committee; `affiliated_candidate_name` and `support_oppose` (`"S"`/`"O"`, from `BenefitedOpposed`) identify who the spending was for or against. `candidate_name` is left blank — that field means "the filer committee is this candidate's own committee," which is never true for an IE. `SubjectCommitteeId` is joined against the registry's `entity_id` (a direct ID match, no name-matching ambiguity) to get the candidate's personal name for `affiliated_candidate_name`; falls back to the raw `SubjectCommitteeName` when the subject isn't in the registry snapshot on disk (a different cycle's sweep, or a ballot-measure committee). A global `TransactionId` dedup set spans all `IndependentExpenditures_*.csv` files during parsing — adjacent historical cycles genuinely overlap by one calendar year (see Scraper), so this is a real, not just defensive, safeguard against double-counting.

**person_id model:** `committee` — SeeTheMoney assigns a new `entity_id` per committee registration (a candidate who re-registers for a new cycle gets a new ID). `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(entity_id)` across all registrations for that combination.

**Limitations:**
- Registry join by last name only for Candidate files — two candidates sharing a last name can cause misassignment. Known limitation of the FilerName format.
- PAC/Party transaction files sometimes have blank `FilerName` — these rows are written with an empty `committee_name` rather than skipped.
- Some PAC/committee names appear in the candidates table due to `$500 Threshold` entity types in the registry. Minor issue, no current cleanup pass.
- `az_committee_details.csv` is downloaded but not currently used in the parser — the registry provides sufficient metadata.

**Expected runtime:** ~5–15 min depending on how many raw files are present.

---

## Data Notes

- **CCEC qualifying contributions** — Arizona's Clean Elections system generates large volumes of $5 qualifying contributions (`TransactionType = "CCEC $5 Qualifying Contribution"`). These are contributions from individual voters, correctly classified as `contributor_type = "Individual"`.
- **Recall_Fann cycle** — a special cycle for the 2021 recall election of State Senator Fann. Overlaps date-wise with the 2022 cycle.
- **Registry covers all filer types** — `az_committees_all.csv` includes Ballot Measure committees and Other entities. These are written to `committees.csv` but not `candidates.csv`.
- **No explicit amended flag** — SeeTheMoney exports do not include an amended indicator. The `amended` field is left blank.
- **Committee details not yet wired into parser** — `az_committee_details.csv` contains richer metadata (treasurer, phone, status) but the parser uses only `az_committees_all.csv` for enrichment.
- **Independent Expenditures data notes:**
  - Full-history backfill verified against the source site: Abeytia For Az's 2024-cycle IE support total ($5,840.24) matches exactly.
  - Historical raw IE files exist only for cycles where the source actually had nonzero IE activity in that year-window; `2004` and `1998` cycles produced 0 rows (valid — Arizona's independent expenditure reporting only picks up meaningfully starting in the 2000s).
  - Observed `TransactionType` values: `"Ind. Expend. (Non-Recall) - cash"`, `"Ind. Expend. (Non-Recall) - credit"`, `"Independent Expenditure - Pay Cash/Check"`, `"Independent Expenditure - Purchase on Terms/Credit"`, `"Recall Ind. Expend. - Pay Cash/Check"`, `"Refund Ind. Expend. (Non-Recall) - cash"` — all mapped in `src/aliases/expenditure_categories.csv`.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-09-22 (added Independent Expenditures sweep) |
| Parser | 2026-09-22 (added Independent Expenditures → expenditures.csv) |
