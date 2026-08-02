# Virginia — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Virginia (VA) |
| **Source** | [Dept. of Elections — Campaign Finance Reports](https://www.elections.virginia.gov/candidatepac-info/reporting/) → bulk CSV directory at [apps.elections.virginia.gov/SBE_CSV/CF/](https://apps.elections.virginia.gov/SBE_CSV/CF/) |
| **Access method** | Plain, unauthenticated Apache/IIS-style directory listing — no browser automation needed |
| **Coverage** | 1999 – present |
| **person_id model** | `committee` — `state_filer_id` is VA's CommitteeCode (e.g. `CC-15-00531`), assigned per committee registration; candidates typically re-register a new committee (new code) each cycle |
| **FIPS** | 51 |

---

## Raw Data Structure

`data/Virginia/raw/{period}/` — one subdirectory per period, mirroring the source's own directory tree:

- **1999 – 2011** (`period` = bare year, e.g. `"1999"`): one directory per calendar year. Schedules are split by filer type with a `_PAC` suffix pair — `ScheduleA_PAC.csv`, `ScheduleB.csv` / `ScheduleB_PAC.csv`, `ScheduleC.csv` / `ScheduleC_PAC.csv`, ... `ScheduleI.csv` / `ScheduleI_PAC.csv`. **No `Report.csv`** exists for this era (confirmed absent from every 1999-2011 listing checked) — see Data Notes for how this is handled.
- **2012 – present** (`period` = `YYYY_MM`, e.g. `"2012_03"`): one directory per month. Contains `Report.csv` plus `ScheduleA.csv` through `ScheduleI.csv` (no `_PAC` suffix — candidate and PAC filings share one file per schedule from this point forward).

### Report.csv (2012+ only) — report/committee cover sheet
`ReportId` (join key for every Schedule file below), `CommitteeCode` (persistent committee registration ID — `state_filer_id`), `CommitteeName`, `CommitteeType` (Candidate Campaign Committee / PAC / Political Party Committee / Referendum Committee / Out of State Political Committee / Inaugural Committee), `CandidateName`, `IsStateWide` / `IsGeneralAssembly` / `IsLocal` (jurisdiction flags), `Party`, `ReportYear`, `ElectionCycle` (e.g. `"11/2015"`), `OfficeSought`, `District`, `IsFinalReport`, `IsAmendment`, `City`, `ZipCode`, and address/contact fields.

### ScheduleA.csv — itemized contributions (>$100)
`ReportId`, contributor name parts (`FirstName`/`MiddleName`/`LastOrCompanyName`/`Prefix`/`Suffix`), `IsIndividual`, `NameOfEmployer`, `OccupationOrTypeOfBusiness`, address fields, `TransactionDate`, `Amount`, `TotalToDate`.

### ScheduleB.csv — itemized in-kind contributions
Same contributor-detail columns as Schedule A, plus `ValuationBasis` and `ProductOrService`.

### ScheduleC.csv — other receipts
Same shape as Schedule A/B minus employer/occupation, plus `ReceiptType` (free text — "Interest Received", "refund", "Voided Check: ...", etc).

### ScheduleD.csv — itemized expenditures
Payee name parts, address, `TransactionDate`, `Amount`, `AuthorizingName`, `ItemOrService`.

### ScheduleE.csv — loans
`TransactionType` (`R` = received, `P` = payment), lender name/address fields, optional co-borrower fields (not currently carried into output — see Data Notes), `LoanBalance`.

### ScheduleF.csv — unpaid obligations/debts
Counterparty name/address, `TransactionDate`, `Amount`, `PurposeOfObligation`.

### ScheduleG.csv / ScheduleH.csv — per-report summary totals
Running contribution/expenditure/loan totals and receipts-and-disbursements balances for the report as a whole — not itemized transaction rows, and every figure in them is derivable from summing Schedules A-F for the same `ReportId`. **Not parsed into any output table.**

### ScheduleI.csv — disposition of surplus funds
Filed alongside a final report when a committee disburses its remaining balance. Recipient name/address, `TransactionDate`, `Amount`, `TypeOfDisposition`. Folded into `expenditures.csv.gz`.

---

## Scraper

`src/pipeline/scrapers/virginia.py`

The source is a bare directory listing two levels deep — `requests` + BeautifulSoup fetches the top-level `CF/` index, matches every subdirectory link against a `YYYY` or `YYYY_MM` pattern, then fetches each period's own listing to discover its `*.csv` links (rather than hard-coding filenames, since the file set genuinely differs between the 1999-2011 `_PAC`-suffixed era and the 2012+ era). No Selenium/Playwright needed — confirmed against real saved copies of the root listing, a 1999 yearly listing, and a 2012 monthly listing.

**Output:** `data/Virginia/raw/{period}/{Filename}.csv`. `manifest.csv` (period, filename, source_url, bytes, scraped_at) at `data/Virginia/`.

**Limitations:** the current calendar month (`YYYY_MM` matching today) is always re-fetched in full, per the state's own notice that "submitted reports and data feeds are updated daily." Historical 1999-2011 years are frozen (the state's own site says amendments to those years are paper-only), so they're only re-fetched with `--force` or an explicit `--start-year`/`--end-year` covering them.

**Expected runtime:** ~186 period directories as of this writing (13 yearly + ~173 monthly); a full initial pull does one listing fetch and up to ~17 file downloads per period. Incremental runs are fast (skip-if-on-disk except the current month).

---

## Parser

`src/pipeline/parsers/virginia.py`

**Output tables:** `contributions.csv.gz` (Schedule A + B + C), `expenditures.csv.gz` (Schedule D + I), `loans_debts.csv.gz` (Schedule E + F), `candidates.csv.gz`, `committees.csv.gz` — committees/candidates deduped globally by `CommitteeCode` across every period processed (VA's committee codes are stable identifiers once assigned), keeping the chronologically last-seen values.

**Key transformations:**
- Two-pass design: pass 1 reads every period's `Report.csv` (2012+ only) into an in-memory `ReportId -> metadata` registry and builds the deduped committee/candidate tables; pass 2 reads every period's Schedule A/B/C/D/E/F/I files and joins each row to its `ReportId` in that registry.
- `contributor_name`/`payee_name` are built in natural order ("First Middle Last"), unlike Maryland's "Last, First" convention — this matches how VA's own COMET system displays names.
- `election_year` prefers the year embedded in `ElectionCycle` (e.g. `"11/2015"` → `2015`) over `ReportYear`, since one election cycle spans multiple `ReportYear`s.
- `jurisdiction` is derived by OR-ing `IsStateWide`/`IsGeneralAssembly`/`IsLocal` into a readable label ("Statewide", "General Assembly", "Local").
- `candidate_first`/`candidate_last` are a best-effort split of the free-text `CandidateName` field (VA doesn't provide separate name-part columns for candidates the way it does for contributors/payees) — strips common prefixes ("Mrs.", "Dr.") and suffixes ("Jr", "III"), then takes the first and last remaining tokens. Not exact for compound surnames.
- Schedule E's co-borrower fields are not carried into `loans_debts.csv.gz` — only the primary lender is captured.

**person_id model:** `committee` (`utils.assign_person_ids(id_model="committee")`). VA's `CommitteeCode` is assigned per committee registration (the `-15-` in `CC-15-00531` tracks the registration year), so a candidate who runs in multiple cycles typically accumulates a different `person_id` per registration unless `assign_person_ids`'s `(candidate_name, office, district)` grouping successfully merges them.

**Legacy era (1999-2011) handling:** confirmed there is no `Report.csv`-equivalent file anywhere in the 1999-2011 directories, so Schedule rows from those years have no committee-name join key available in the raw data at all. Rather than drop these rows or guess a name, `committee_name` is set to a clearly-flagged placeholder — `"UNKNOWN (VA pre-2012, ReportId <id>)"` — and `candidate_name`/`office`/`district`/`party` are left blank. The parser logs a warning with the total count of affected rows at the end of each run. **This is a real, currently-unresolved data-quality gap, not a bug** — see Data Notes.

**Expected runtime:** untested at full scale in this environment (no outbound network access to `apps.elections.virginia.gov` from the sandbox this was built in — see Data Notes); the parser itself was validated end-to-end (through `validate.py` and `tabulate.py`, both passing) against real Report/Schedule A-I row samples captured from the live 2012+ CSVs.

---

## Data Notes

- **Built and tested without live access to `apps.elections.virginia.gov`.** The site sits behind Akamai and returned empty bodies to this environment's fetch tool (likely a bot-management challenge, not a hard block — a normal browser `User-Agent` is set in the scraper's `HEADERS` for exactly this reason). The scraper's directory-listing parser (`discover_periods`/`discover_files`) was instead validated against real saved copies of the site's own HTML (root listing, a 1999 yearly listing, a 2012 monthly listing) supplied by the user, and its download/manifest/force/skip/year-range logic was validated end-to-end against a local HTTP server standing in for the real site. The parser was validated against real Report.csv/ScheduleA-I.csv row samples fetched from a third-party GitHub mirror of VA's own 2016-08 export. **Run a live `--force` pass and check `logs/` before trusting a full historical sync** — bot-protection behavior against a real outbound request from wherever this eventually runs has not been observed directly.
- **Pre-2012 committee-name gap (see Parser section above).** Every 1999-2011 Schedule row's `ReportId` has no corresponding `Report.csv` row anywhere in the raw tree, so `committee_name` for those rows is a placeholder rather than a real name. If a resolution surfaces later (e.g. a legacy committee-registry file discovered elsewhere on the site, or VPAP/OpenElections publishing a crosswalk), this should be revisited — it currently affects all contribution/expenditure/loan rows from 1999-2011 rather than a subset.
- **Schedule G/H (per-report summary totals) are intentionally not parsed into any output table** — every figure in them is a sum over that same report's Schedule A-F rows, so nothing is lost by skipping them; they exist in `raw/` for anyone who wants report-level running-balance data directly.
- **Schedule E's `TransactionType` R/P coding** ("Loan Received" vs "Loan Payment") is inferred from context (values and column shape), not from an official VA data dictionary — no such dictionary was located during this build. Worth confirming against a large real sample if loan totals look off in validation.
- **`amended` is the raw `"True"`/`"False"` string** from `Report.csv`'s `IsAmendment` field, not normalized to `0`/`1` — flagged by `validate.py`'s tier-2 warning but expected, since `columns.py` deliberately keeps `amended` as `VARCHAR` for exactly this reason (inconsistent across states).
- **Alias mappings added for all four files** (`src/aliases/{contributor_types,transaction_categories,expenditure_categories,committee_types}.csv`) and verified against a live `aggregate.py` run (see below): `contributor_type` ("Individual"/"Organization", identity mapping — parser-derived, not a raw source column), `committee_type` (all six raw `CommitteeType` values from `Report.csv`), and the *fixed* `transaction_type` values only (`"Contribution"` → Monetary, `"Expenditure"` → Monetary, `"Disposition of Surplus Funds"` → Other). Schedule B/C's `transaction_type` carries a dynamic parenthetical/suffix (`ProductOrService`/`ReceiptType`) that will never exact-match a fixed alias row — those are **intentionally left unmapped** (`canonical_transaction_category` correctly returns `None` for them, confirmed) rather than guessed, per the pipeline's own convention. `office_types.csv` was **not** attempted — VA's `OfficeSought` free-text field spans a huge combinatorial space of local offices (Sheriff, Commissioner of Revenue, School Board, Town Council, Board of Supervisors district seats, etc.) on top of the statewide/General Assembly offices, and building a clean crosswalk needs a full historical scan of raw values this build didn't do; flagged as a follow-up.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-15 |
| Parser | 2026-07-15 |
| Alias mappings | 2026-07-15 |
| Docs | 2026-07-15 |
