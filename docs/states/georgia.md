# Georgia — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Georgia (GA) |
| **Source(s)** | [Peachfile](https://peachfile.ethics.ga.gov) (`api-peachfile.ethics.ga.gov`, 2025–present); [legacy ETHICS system](https://media.ethics.ga.gov) (2006–2024); [GACFIS recordsearch](https://recordsearch.ethics.ga.gov) (`api-recordsearch.ethics.ga.gov`, 2014–2026) |
| **Access method** | REST API (`api-peachfile.ethics.ga.gov/api`) — bulk CSV export (transactions) + paginated JSON (entities); legacy ASP.NET HTML scraping + CSV postbacks; GACFIS paginated JSON (`api-recordsearch.ethics.ga.gov/api`, pageSize=100 max) |
| **Coverage** | 2006 – present (legacy 2006–2024; Peachfile 2025–present; recordsearch 2014–2026) |
| **person_id model** | `committee` — `filerRegistrationId` is per cycle; `person_id` = min ID for a given `(candidate_name, office, district)` |

---

## Raw Data Structure

### Peachfile (api-peachfile.ethics.ga.gov, 2025–present)

One file per year per type for transactions; one full pull for entities.

#### contributions_{year}.csv

| Field | Description |
|---|---|
| `Filing Entity ID` | Numeric filer ID — joins to `filerEntityId` in candidates.csv |
| `Filing Entity Name` | Committee or candidate name as registered |
| `Campaign Committee Name` | Committee name (may differ from Filing Entity Name) |
| `Registration Type` | e.g. "Candidate/Candidate Campaign Committee" |
| `Transaction Id` | Unique transaction ID |
| `Transaction Type` | Top-level type: "Contribution", "Loan Received", "Loan Payment", "Loan Forgiven", "Return Contribution", "Interest Earned (Non-Investment Account)" |
| `Transaction Sub Type` | e.g. "Itemized Contribution", "Unitemized Contribution", "In-Kind Contribution" |
| `Contributor Type` | e.g. "Individual", "Corporation / Business / Unregistered Committee" |
| `Contributor Last Name` | Org name (for entities) or last name (for individuals) |
| `Contributor First Name` / `Contributor Middle Name` | Individual name parts (blank for orgs) |
| `Contributor Address City/State/Zip Code` | Contributor address; ZIP arrives as `="12345"` (Excel formula format) |
| `Contributor/Person Responsible for Loan Occupation` | Occupation — combined field, often blank for orgs |
| `Contributor/Person Responsible for Loan Employer` | Employer — combined field, often blank for orgs |
| `Transaction Date` | MM/DD/YYYY |
| `Transaction Amount` | Dollar amount with `$` prefix and commas |
| `Election Type` | e.g. "Primary", "General" |
| `Election Year` | 4-digit year of the associated election |
| `Amended` | "Y" or "N" |
| `Disclosure Report Name` | Full report name |

#### expenditures_{year}.csv

Similar structure. Notable differences:

| Field | Description |
|---|---|
| `Filing Entity Id` | Note lowercase `d` and trailing space in header — stripped by parser |
| `Transaction Type` | "Expenditure" or "Return Expenditure" |
| `Transaction Sub Type` | e.g. "Itemized Expenditure", "Reimbursement", "Credit Card", "In-Kind Expenditure" |
| `Purpose` | Free-text expenditure purpose |
| `Payee Last Name` / `Payee First Name` | Payee name parts |
| `Payee Address City/State/Zip Code` | Payee address |
| `Transaction Amount` | Blank for Reimbursement and some Credit Card rows — parser falls back to `End Recipient Transaction Amount` |
| `End Recipient Transaction Amount` | Amount paid to end recipient; used as fallback when Transaction Amount is blank |
| `Transaction ID` | Note capital "ID" (vs lowercase in contributions) |

#### candidates.csv

Fetched from `PublicFilerDetails/GetCandidateDetails` (paginated, max 100 rows/page due to WAF limit).

| Field | Description |
|---|---|
| `filerEntityId` | Stable numeric entity ID — used to join transactions to entities |
| `filerRegistrationId` | Per-cycle registration ID — used as `state_filer_id` |
| `filerName` | Display name, format "Last, First M." |
| `candidateFirstName` / `candidateLastName` / `candidateMiddleName` | Name parts |
| `committeeName` | Populated for PACs/IECs; blank for candidate campaign committees |
| `office` / `districtName` / `jurisdiction` | Office and geography |
| `politicalPartyCode` / `partyAffiliation` | Party |
| `filerStatusCode` | "FACT" (active) or "TERMN" (terminated) |
| `filingCycleName` / `electionCycleName` | Filing and election cycle names |
| `treasurerFirstName` / `treasurerLastName` | Treasurer name parts |

#### public_committees.csv

Fetched from `PublicFilerDetails/GetPublicCommittees` (paginated, max 100 rows/page due to WAF limit).

| Field | Description |
|---|---|
| `filerEntityId` | Stable numeric entity ID |
| `filerRegistrationId` | Per-cycle registration ID — used as `state_filer_id` |
| `filerName` | Committee name |
| `filerType` / `filerTypeCode` | Committee type string and code (e.g. "Leadership Committee", "Independent Committee") |
| `filerStatus` / `filerStatusCode` | Active/terminated status |
| `filingCycleName` | Filing cycle name |
| `districtName` / `jurisdictionTypeName` | District and jurisdiction geography |
| `partyAffiliation` | Party |
| `treasurerFirstName` / `treasurerLastName` | Treasurer name |
| `chairPersonFirstName` / `chairPersonLastName` | Chairperson name |
| `committeeMailingAddress1` / `committeeMailingCity` / `committeeMailingStateCode` / `committeeMailingZipCode` | Mailing address |

### Legacy ETHICS System (media.ethics.ga.gov, 2006–2024)

Downloaded with `--legacy`.

#### legacy_contributions_{monetary|in-kind|loan}_{year}.csv

One file per year per subtype. The scraper converts all exports to uniform CSV (expenditure exports arrive as HTML `<table>` fragments from the ASP.NET GridView — the scraper handles the conversion before writing).

#### legacy_expenditures_{expenditure|reimbursement|credit_card|in-kind}_{year}.csv

Same year-per-subtype structure as contributions. Expenditure exports have a different column set from contributions, adding `Key`, `Ref`, `Purpose`, `Paid`, `Other`.

#### legacy_candidates_{A–Z}.csv

Candidate registrations swept by last-name initial (`Method=0`, "begins with"). The results page has no Export control — each row's View link is fired as a `__doPostBack`; the 302 redirect is followed and the detail page parsed. Columns: `name_id`, `filer_id`, `candidate_name`, `office` (raw — includes `County:`/district text), `status`, `address`, `city_state_zip`, `telephone` — one row per DOI (per-cycle registration). ~825 candidates under "A" alone, 2 requests each; the full A–Z sweep takes several hours.

#### legacy_committees_type{1–9}.csv

Non-candidate committees swept by `CommitteeType` ID via the `btnExport` image button. 1 = PAC; IDs follow the radio-button order on Campaign_ByName.aspx; the export's own type column is authoritative.

### GACFIS Recordsearch (api-recordsearch.ethics.ga.gov, 2014–2026)

Downloaded with `--recordsearch`, chunked per (kind, year, month). Public, no auth required.

#### recordsearch_contributions_{year}_{month:02d}.csv
#### recordsearch_expenditures_{year}_{month:02d}.csv

Columns are a shared superset across both kinds (`RECORDSEARCH_FIELDS` in the scraper): `transactionId`, `transactionDate`/`sortTransactionDate`, `transactionAmount`, `transactionCategory` (e.g. "Monetary Itemized", "Monetary Non-Itemized", "In-Kind", "Interest"), `filerName`/`campaignCommittee` (the receiving/spending committee), `candidateFirstName`/`Middle`/`LastName`, `sourceName`/`transactionSource*` (**contributor for contributions, payee for expenditures** — note the field names are counterintuitive), `transactionPurposeDescription`/`description`, `electionYear`, etc.

---

## Scraper

`src/pipeline/scrapers/georgia.py`

Posts to two endpoints on `api-peachfile.ethics.ga.gov/api`:

- **Transactions**: `ExportPublicData/GetExportPublicDownloadData` — returns a full-year CSV in one request. Called once per year per transaction type.
- **Entities**: `PublicFilerDetails/GetCandidateDetails` — paginated JSON, 100 records/page. WAF blocks `pageSize >= 200`; the scraper uses 100.

Both endpoints require `Origin: https://peachfile.ethics.ga.gov` and a browser `User-Agent` — the WAF returns 400 "Potentially harmful payload detected" for Python's default agent.

The scraper auto-detects available years by probing from `START_YEAR = 2025` forward, stopping after two consecutive 404s. The current year is always re-fetched regardless of the manifest.

**Expected runtime**: ~10–15s for entities, ~10s/year for contributions, ~2s/year for expenditures. 2026 contributions are ~60MB and take longer.

**Limitations**: Pre-2025 data is not available from this API. 2023–2024 return header-only CSVs.

### Legacy mode (`--legacy`)

Pulls 2006–2024 from the legacy search portal (`media.ethics.ga.gov`) via GET-results-page → POST-Export postback. Covers transactions (per year per type) and entities (candidates A–Z, non-candidate committees by type ID). Exports take 60–300s each — must be run locally; hangs in restricted network environments. Expenditure exports arrive as HTML tables and are converted to CSV in the scraper.

**`--entities` / `--transactions` run independently in legacy mode too** (same flags as Peachfile mode; default = both). This matters because the candidate sweep is by far the slowest part: ~825 candidates under "A" alone, 2 requests each, so the full A–Z sweep can take on the order of days. Run it on its own:

- `python3 src/pipeline/scrapers/georgia.py --legacy --transactions` — just the per-year/type contribution and expenditure exports (fast, hours not days).
- `python3 src/pipeline/scrapers/georgia.py --legacy --entities` — just the candidate A–Z sweep + non-candidate committee exports.

The entities sweep is checkpointed at two levels, so a multi-day `--entities` run is safe to interrupt and resume: each completed letter is recorded in the manifest (`sweep_legacy_candidates` is skipped on rerun if its key is already `done`), and *within* a letter a `.progress` sidecar tracks the last completed grid row so an interrupted letter resumes mid-sweep rather than restarting.

### recordsearch mode (`--recordsearch`)

`run_recordsearch(force=False, start_year=2014, end_year=None)` pages through
`POST /api/PublicTransactionDetails/GetTransactionDetails` (contributions) and
`.../GetExpenditureDetails` (expenditures) on `api-recordsearch.ethics.ga.gov`,
one calendar month at a time, `pageSize=100` (the WAF 400s on `>=200`).
Requires `Origin: https://recordsearch.ethics.ga.gov` /
`Referer: https://recordsearch.ethics.ga.gov/` headers (`_recordsearch_session()`).

- `--recordsearch [--start-year YYYY] [--end-year YYYY]` — defaults to
  2014-current year.
- Resumable at **(kind, year-month)** granularity via
  `manifest.csv` (`transaction_type="recordsearch_{contributions|expenditures}"`,
  `year="YYYY-MM"`); the current month is always re-fetched. `--force` clears
  all `recordsearch_*` manifest rows and restarts.
- Smoke-tested against the live API (2026-06-14): 2014-01 contributions (0
  rows) and 2022-01 expenditures (2,977 rows / 30 pages) both download and
  write correctly.

**Scale**: full 2014-2026 is ~25,634 contribution requests + ~2,081
expenditure requests (~2.5M + 208K rows). `download_recordsearch()` buffers an
entire month in memory and writes the CSV + manifest entry only once the month
completes — heavy months (e.g. 2022, ~1,200+ pages) take many minutes each.
**Must be run as a long background job in Henry's local environment**, not in
the sandbox (no persistent background processes across sandbox calls, and a
single heavy month exceeds the sandbox's per-call time limit). Run:
`python3 src/pipeline/scrapers/georgia.py --recordsearch` — safe to interrupt
and resume (manifest-checkpointed per month).

---

## Parser

`src/pipeline/parsers/georgia.py`

**Key transformations**

- **ZIP codes**: Strips Excel formula wrapper `="30339"` → `30339`.
- **Amounts**: Strips `$` and commas; falls back to `End Recipient Transaction Amount` for blank expenditure amounts (Reimbursements, some Credit Cards).
- **Contributor name**: Combines Last + First + Middle for individuals; uses Last only for organizations.
- **Transaction routing**: `Loan Received`, `Loan Payment`, `Loan Forgiven` rows from the contributions file go to `loans_debts.csv.gz`. All others go to `contributions.csv.gz`.
- **Entity split**: Candidates.csv rows with a non-blank `committeeName` go to `committees.csv.gz`; the rest go to `candidates.csv.gz`. All 1,262 rows have `candidateLastName` set — the split relies on `committeeName` presence, not a filer type code.
- **Registry enrichment**: `Filing Entity ID` in transactions is joined to `filerEntityId` in candidates.csv to populate `office` and `candidate_name` on transaction rows.
- **Header normalization**: Expenditure CSV has trailing spaces on some column names — stripped before parsing.

**person_id model**

`"committee"` — `filerRegistrationId` is assigned per election cycle, not per person. `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(state_filer_id)` across registrations.

**Legacy data (2006–2024)**

The parser now consumes all three GA raw sources — previously only the Peachfile
transaction/entity files (above) were processed.

- **Legacy registry**: `build_legacy_registry()` loads `legacy_candidates_{A-Z}.csv`
  and `legacy_committees_type{1-8}.csv` into a dict keyed by `filer_id`/`FilerID`
  (e.g. `C2011004358`, `NC2010000038`). Used to enrich legacy transaction rows
  whose own `Candidate_*`/`Committee_Name` columns are blank — common for
  non-candidate-committee filers.
- **`committee_name` fallback chain**: for legacy
  contributions/loans/expenditures, `committee_name` is resolved in order:
  (1) the row's own `Committee_Name`; (2) `legacy_registry`'s `committee_name`
  (only populated for `"NC..."` non-candidate-committee filer IDs —
  `legacy_candidates_*.csv` has no committee field for `"C..."` candidate
  filers); (3) `build_legacy_committee_name_lookup()` — a `FilerID →
  Committee_Name` map built by scanning every legacy transaction file for the
  first non-blank `Committee_Name` per filer; (4) `legacy_candidate_name(row)`
  (or `legacy_registry`'s `candidate_name`) as a final catch-all. The first
  first full run with only tiers 1-3 still failed
  validation at 97.6%/93.3% committee_name fill (contributions/expenditures,
  threshold 99%) — most candidate-filed rows with a blank `Committee_Name`
  never carry one anywhere in their files. Tier (4) was added to close this
  gap, on the reasoning that an unnamed candidate committee is reasonably
  represented by the candidate's own name. Smoke-tested at 0 blank
  `committee_name` for legacy contributions/expenditures; full-run
  re-validation pending.
- **Legacy contributions** (`legacy_contributions_{monetary,in-kind}_{year}.csv`)
  → `contributions.csv.gz`, `transaction_type = "Contribution – {Monetary|In-Kind}"`.
  `amount` comes from `Cash_Amount` (Monetary) or `In_Kind_Amount` (In-Kind).
  Contributor name/type derived by `legacy_contributor_info()`: the `PAC` column
  holds a contributing committee's name ("Non-Candidate Committee"); otherwise
  `LastName`/`FirstName` identify an individual or organization.
- **Legacy loans** (`legacy_contributions_loan_{year}.csv`) → `loans_debts.csv.gz`,
  `record_type` = raw `Type` ("Loan" / "Credit Received on Loan").
- **Legacy expenditures**
  (`legacy_expenditures_{expenditure,reimbursement,credit_card,in-kind}_{year}.csv`)
  → `expenditures.csv.gz`, `transaction_type = "Expenditure – {Type}"`. `amount`
  comes from `Paid` (falls back to `Other`). `payee_name` built from
  `LastName`/`FirstName` via `legacy_payee_name()`.
- **Legacy candidates** (`legacy_candidates_{A-Z}.csv`) → `candidates.csv.gz`.
  `candidate_name` parsed from the combined `"Last, First Middle Suffix"` string;
  `office` is written **raw and unparsed** (e.g. `"State Representative District: 89"`,
  `"Coroner County: Lincoln"`) — `district`/`jurisdiction`/`party` are left blank.
  `election_year` is extracted from the `filer_id` prefix (`C2011004358` → 2011)
  via `LEGACY_FILER_ID_YEAR_RE`.
- **Legacy committees** (`legacy_committees_type{1-8}.csv`) → `committees.csv.gz`.
  `committee_type` is the raw `CommitteeType` string (e.g. `"Political Action
  Committee"`, `"Political Party"`); mapped to canonical values via
  `src/aliases/committee_types.csv` (type2 = "Independent Committee" already
  matched an existing row).

**recordsearch data (2014–present)**

`recordsearch_contributions_*.csv` / `recordsearch_expenditures_*.csv` are
parsed in a dedicated block after the legacy expenditures section, mapping
into `contributions.csv.gz` / `expenditures.csv.gz` with
`transaction_type = "Contribution – {transactionCategory}"` /
`"Expenditure – {transactionCategory}"` (e.g. "Contribution – Monetary
Itemized"). `committee_name` = `campaignCommittee` (falls back to
`filerName`); contributor/payee name = `sourceName`; `candidate_name` via
`recordsearch_candidate_name()` (built from `candidateLastName`/`First`/
`Middle`, falling back to `filerName` for non-candidate filers), enriched with
`office` from the Peachfile candidate registry by `filerEntityId` where
available. Dates (`sortTransactionDate`/`transactionDate`, format
`"YYYY-MM-DD[ HH:MM:SS...]"` or ISO) parsed by `parse_recordsearch_date()`
(first 10 chars + `parse_date()` validation).

**Dedup**: recordsearch is overwhelmingly net-new vs. Peachfile + legacy
(~0.4% overlap in 2022, ~79% in 2025 spot-checks — see
[[georgia_recordsearch_source]]), but a composite-key dedup guards against
double-counting the overlap. While parsing Peachfile + legacy rows, every
contribution/expenditure is indexed by `_index_row()` under key
`(date, amount rounded to 2dp)` → set of "significant tokens" (lowercase,
punctuation-stripped, length≥3, minus a GA-specific `STOP_WORDS` list like
"committee"/"pac"/"georgia"/"inc") from its contributor/payee + committee
names. Each recordsearch row is then checked via `_is_duplicate()`: same
`(date, amount)` key **and** any token overlap (either direction) between its
source/committee names and an indexed row's tokens → skipped as a duplicate
(counted but not written); otherwise written as net-new. `log.file_parsed()`
records `skipped=<dupe count>` per recordsearch file.

Smoke-tested standalone (see [[georgia_recordsearch_source]]): `recordsearch_expenditures_2022_01.csv`
(2,977 rows) against the 1,433 existing `georgia.db` Jan-2022 expenditure rows
→ 172 dupes (5.78%), 2,805 net-new; sampled matches/non-matches looked
correct. Full parser run pending the full recordsearch scrape.

**Known limitation — legacy/Peachfile person_id split**: Because legacy `office`
is a raw combined string (different format from Peachfile's `office`/`districtName`
fields), `assign_person_ids`'s `(state, candidate_name, office, district)` grouping
will **not** unify a candidate's pre-2025 legacy registrations with their 2025+
Peachfile registrations — they get different `person_id`s even for the same person.
Fixing this would require parsing the legacy `office` string into office/district/
county and reconciling against Peachfile's office naming. Not yet done.

**Running the full parse**: the legacy monetary-contribution files alone total
~600MB (2006: 75MB down to ~1KB for 2023-2024). A full `run()` over all legacy +
Peachfile files exceeds the sandbox's bash time limit — run the parser in Henry's
local environment. The new legacy code paths were smoke-tested in-sandbox against
small/truncated files (2026-06-13) with no errors and correct field mapping.

---

## Data Notes

- **2023-2024 near-void (not a scraper gap)**: The Peachfile bulk export returns header-only CSVs for 2023-2024 (and 404s for 2021-2022) — `START_YEAR = 2025` in the scraper. The legacy scraper hits `media.ethics.ga.gov`'s live search directly (no third party / Accountability Project involved, despite an earlier stale note here). Checked the live "Search By Contribution" results for `From=01/01/2023&To=12/31/2023` and `…2024`: the live system itself returns only **6 records for all of 2023** and **4 for all of 2024** — exactly matching `legacy_contributions_*_2023/2024.csv`. 2022 likewise: live search ≈19,920 records, our `legacy_contributions_monetary_2022.csv` + `..._in-kind_2022.csv` = 19,873 + 33 = 19,906 (rounding from pagination). So 2022-2024 are fully captured on both sides — this is a genuine reporting gap in Georgia's source data (committees largely stopped itemizing in the legacy system after 2022 and Peachfile's bulk export doesn't backfill it), not missing scraper coverage. No scraper overhaul needed. These two sources overlap in 2025 and would need deduplication if combined.
- **Unitemized contributions**: ~45% of contribution rows are unitemized aggregates with no contributor name/address. The `contributor_type` field is also blank for these rows.
- **2032 election year**: Some candidate registrations reference 2032 filing cycles (pre-registration). The validator flags these as out-of-range warnings — they're legitimate future cycle registrations.
- **Committee type / candidate-controlled committees mapped as PAC (fixed)**: `committees.csv.gz` is populated from two sources. Rows from `candidates.csv` with a `committeeName` (934 rows) are the candidate's own campaign committee — e.g. "Jackson for Governor, Inc.", "Carr for Georgia, Inc.", "Keisha for Governor" — and `candidate_name`/`office`/`district` are populated. These were previously hardcoded to `committee_type = "PAC"`, which collided with the genuinely non-candidate PACs/IECs/leadership/party committees from `public_committees.csv` (328 rows, granular `filerType`, `candidate_name` blank). The parser now writes `"Candidate Committee"` for the candidates.csv rows; `aggregate.py` carries a reclassification override (`state='GA' AND committee_type='PAC' AND candidate_name != ''` → `"Candidate Committee"`) for older `georgia.db` files.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-14 |
| Parser | 2026-06-14 |
