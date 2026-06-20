# Idaho (ID)

## Overview

| | |
|---|---|
| **State** | Idaho (ID) |
| **Source(s)** | `sunshine.voteidaho.gov` (current portal, 2020–2026); `canvass.sos.idaho.gov` (electionstats donate/spend activity feed, 2020–2022 gap-fill); `archive.sos.idaho.gov` (biennium archive, 2000–2018, plus per-filer 2019 C-2 PDF reports) |
| **Access method** | REST API (portal, unauthenticated JSON→CSV export); static cached CSV download with HTTP Range resume (legacy); direct `.xls`/`.xlsx` bulk download (archive); per-filer PDF download + `pdftotext -layout` extraction (2019 reports) |
| **Coverage** | 1999–2026 (contribution/expenditure dates); registry data 2020–2026 |
| **person_id model** | `name_hash` — no numeric filer ID anywhere in the 26-year dataset; `person_id` derived from MD5 of normalized name |


## Raw Data Structure

### Portal (api-sunshine.voteidaho.gov, 2023–present)

Real data starts 2023; 2020–2022 files exist but are near-empty (header-only or a handful of rows) — those years are covered by the electionstats gap-fill instead.

#### portal_contributions_{YYYY}.csv

Header is row 1 (no junk line). **Known quirk: "Election Type" and "Election Year" columns are swapped** — "Election Type" actually holds the 4-digit year, "Election Year" holds the type (Primary/General/etc).

| Field | Description |
|---|---|
| `Filing Entity ID` | Per-cycle registration ID — joins to `Filing Entity Id` in registry files |
| `Filing Entity Name` | Committee or candidate name |
| `Campaign Name` | Campaign name |
| `Registration Type` | "Candidate" or "Committee" |
| `Transaction Id` | Unique transaction ID |
| `Transaction Type` | e.g. "Contribution", "In-Kind", "Loan Received", "Loan Forgiven", "Outstanding Loan" — loan types routed to loans_debts |
| `Transaction Sub Type` | Sub-classification |
| `Contributor Type` | e.g. "Individual", "Company" |
| `Contributor Last Name` / `Contributor First Name` / `Contributor Company Name` | Contributor name parts |
| `Contributor Address Line 1/2` / `City` / `State` / `Zip Code` | Contributor address |
| `Transaction Date` | MM/DD/YYYY |
| `Transaction Amount` | Dollar amount |
| `Loan Interest Amount` / `Total Loan Amount` | Loan-specific fields |
| `Election Type` | ⚠️ Actually holds the 4-digit election year (column header is mislabeled) |
| `Election Year` | ⚠️ Actually holds the election type — "Primary", "General", etc. (column header is mislabeled) |
| `Amended` | `Y`/`N`/blank (normalized to `1`/`0`/`` by parser) |
| `Report Name` / `Report Filed Date` | Filing report metadata |

#### portal_expenditures_{YYYY}.csv

Same header-swap quirk as contributions. Notable differences from contributions:

| Field | Description |
|---|---|
| `Filing Entity Name ` | Trailing space in header — stripped by parser |
| `Purpose` | Free-text expenditure purpose |
| `Payee Type` / `Payee Last Name` / `Payee First Name` / `Payee Company Name` | Payee name parts |
| `Payee Address Line 1/2` / `City` / `State` / `Zip Code` | Payee address |
| `Transaction Type` | e.g. "Expenditure", "In-Kind", "Debt Payment", "Loan Payment", "Debt", "Outstanding Debt" — debt/loan types routed to loans_debts |
| `Candidate Supported/Opposed` / `Candidate Office Sought` / `Measure Supported/Opposed` / `Stance` | Independent expenditure fields |

#### portal_candidates.csv

Full registry pull (`year="all"`). Row 1 is a junk metadata line (`"ï»¿Candidate Download as of..."`); real header is row 2.

| Field | Description |
|---|---|
| `Filing Entity Id` | Per-cycle registration ID — used as `state_filer_id` |
| `Candidate Last Name` / `First Name` / `Middle Name` | Candidate name parts |
| `Office Sought` / `District Type` / `Seat Zone` / `District` | Office and geography |
| `Party Affiliation` | Party |
| `Candidate Address1/2` / `City` / `State` / `Zip Code` | Candidate address |
| `Treasurer Last Name` / `Treasurer First Name` | Treasurer |
| `Campaign Name` | Campaign name |
| `Election Year` / `Account Status` | Cycle and status |

#### portal_committees.csv

Full registry pull (`year="all"`). Same junk-line-on-row-1 quirk as candidates.

| Field | Description |
|---|---|
| `Filing Entity Id` | Per-cycle registration ID — used as `state_filer_id` |
| `Committee Name` | Committee name |
| `Jurisdiction` | Jurisdiction |
| `Committee Type` | e.g. "Political Action Committee", "Political Party Committee" |
| `Party Affiliation` | Party |
| `Treasurer Last Name` / `Treasurer First Name` | Treasurer |
| `Chairperson Last Name` / `Chairperson First Name` | Chairperson |
| `Filing Year` / `Account Status` | Cycle and status |

### Electionstats Activity Feed (canvass.sos.idaho.gov, 2020–2022 gap-fill)

`electionstats_activity.csv` — full pull of `get_activity.json` (paginated POST, 383 pages, 382,124 rows spanning 2020–2024). 40+ column schema with `activity_type` ∈ {`donate`, `spend`, `file`} and both From- and To- entity enrichment (name/type/address/office/district/party). The parser emits only 2020–2022 rows to avoid double-counting the portal (2023+) and archive (≤2018). `file`-type rows carry no transaction and are skipped.

### Archive (archive.sos.idaho.gov, 2000–2018)

`archive_{YYYY}_{cand|comm}_{cont|exp}.{xls,xlsx}` — 40 files across 10 biennia (4 files/biennium: `cand_cont`/`cand_exp`/`comm_cont`/`comm_exp`). `.xls` through 2012, `.xlsx` from 2014 on. ~7 distinct header layouts across years — the parser handles all via alias tuples. 2018 uses a different layout from 2014/2016 (no `2018_` filename prefix, different column names) and includes committee-level bulk files.

### 2019 Per-Filer PDFs (archive.sos.idaho.gov/ELECT/Finance/2020/, 2019 only)

`legacy_2019_pdfs/{First_Annual|Mid-Year}_{filer_id}[_amended|_terminated].pdf` — 461 per-filer C-2 "Campaign Financial Disclosure Report" PDFs across 5 index pages. Text-extracted via `pdftotext -layout`; ~1.7% are scanner-image PDFs with no text layer (skipped). Covers Schedules A–D (contributions, expenditures, in-kind, loans). 14 filer IDs have both a Mid-Year and First-Annual report (non-overlapping periods).

## Scraper

`src/pipeline/scrapers/idaho.py` — pure `requests`, no Playwright needed for any source.

**Portal (api-sunshine.voteidaho.gov):** Unauthenticated REST API. Requires `Origin: https://sunshine.voteidaho.gov` header. Registry files (`portal_candidates.csv`, `portal_committees.csv`) are pulled with `year="all"`; transaction files are year-split (2020–present). Current year always re-fetched; prior years skipped if in manifest.

**Electionstats activity feed (canvass.sos.idaho.gov):** Paginated POST to `get_activity.json` (`limit=1000` — larger values 404). 383 pages / 382,124 rows total. This is a one-time historical pull, not part of the normal incremental run; resumable via a `.progress` sidecar. Up to 10 concurrent workers tolerated.

**Archive (archive.sos.idaho.gov):** Direct `.xls`/`.xlsx` bulk downloads, 40 files across 10 biennia (2000–2018). No auth required.

**2019 per-filer PDFs (archive.sos.idaho.gov):** `fetch_legacy_2019_reports()` fetches 5 index pages on first call (cached to `legacy_2019_pdfs/_links.json`, 461 links) then downloads each PDF with skip-if-exists resumability. A full pull from empty takes multiple invocations in sandboxed environments; `run()` reports `done=False` with a remaining count until all 461 are present.

**Limitations:**
- `Origin: https://sunshine.voteidaho.gov` header is required for the portal API — omitting it returns empty results with no error
- Electionstats full pull (383 pages) is slow without parallelism; not part of the normal update cycle once done
- 2019 PDF download requires multiple invocations in time-limited environments; run locally for a single-pass pull

## Parser

`src/pipeline/parsers/idaho.py`. `id_model = "name_hash"`: Idaho has no person-level ID anywhere in the 26-year dataset — the portal's "Filing Entity Id" is per-registration/cycle (one person can have multiple IDs across cycles), and the 2000–2018 archive and the 2020–2022 electionstats_activity feed carry no numeric ID at all. `person_id` is derived from `MD5(state + normalized candidate_name)` via `utils.assign_person_ids(id_model="name_hash")`, unifying a person across cycles/sources purely by name.

Key transformations:
- `candidate_name` / `committee_name` / `contributor_name` / `payee_name` are uppercased via `utils.clean_name()` across **all** tables (a deliberate deviation from the Hawaii template) — needed so names match across the four very differently-formatted source families.
- `election_year` is read from the portal's mislabeled "Election Type" column (see header-swap quirk above).
- `amended` is normalized from `Y`/`N`/blank to `1`/`0`/`` via a local `bool01()` helper.
- Loan/debt routing to `loans_debts.csv.gz`: portal contributions with Transaction Type in `{Loan Received, Loan Forgiven, Outstanding Loan}`; portal expenditures in `{Debt Payment, Loan Payment, Debt, Outstanding Debt}`; electionstats `donate_type == "Loan"`; electionstats `spend_type` in `{Loan Payment, Loan Interest, Credit Card Payment, Credit Card Interest/Fee}`; archive contribution rows with Type code `{"L","Loan"}`; archive expenditure rows with Type code `"Repayment"`.
- `EARLIEST_YEAR = 1999` (not 2000) — the 2000 archive files legitimately contain ~8.3k contribution/expenditure rows dated in late 1999 (the 2000 election cycle); a handful of pre-1999 outliers (<10 rows) are still rejected.
- Contributor/payee name fallback for entity rows: when an archive row has no person name (first/last/suffix all empty) but carries a `...Committee/Company...`-style entity name column, `contributor_name`/`payee_name` falls back to that entity name (via `CONTRIBUTOR_ENTITY`/`RECIP_ENTITY` alias tuples) instead of being left blank, and `contributor_type` defaults to `"Company"` in that case. Relevant especially for the 2018 archive files, which have ~36.6k rows (combined across all 4 files) following this pattern; applies to all archive years since the alias tuples are checked everywhere.
- **electionstats_activity.csv (2020–2022 gap-fill)**: `activity_type == "donate"` rows resolve the recipient via `to_entity_type` (`"Candidate"` → `_register_candidate_committee`, else → `_register_pac`, i.e. a PAC/Central Committee) and the contributor from `from_*`; `activity_type == "spend"` rows resolve the spending committee via `from_entity_type` similarly and the payee from `to_*`. `donate_type`/`spend_type` map to "Type – SubType"-style `transaction_type` values via `ES_DONATE_TYPE_MAP`/`ES_SPEND_TYPE_MAP` (e.g. `"Cash"` → `"Contribution – Cash"`, `"Credit Card Item"` (spend side) → `"Expenditure"`). **"Credit Card Item" double-entry**: the same real-world credit-card transaction can appear as both a `donate`/"Credit Card Item" row (card issuer → candidate, e.g. "Beehive Federal Credit Union" → "Douglas Ricks", $44.48) *and* a `spend`/"Credit Card Item" row (candidate → vendor, e.g. "Douglas Ricks" → "R&R BBQ", $44.48, same date) — two halves of one transaction, analogous to Loan Received/Loan Payment. Both sides are preserved as-is per the source structure (968 donate + 968 spend rows, ~923 each within the 2020-2022 window).
- **legacy_2019_pdfs (2019 per-filer C-2 PDF reports)**: each PDF is converted to text via `pdftotext -layout` (`_pdf_to_text`) and parsed against the C-2 "Campaign Financial Disclosure Report" template. `_pdf_parse_header()` reads the filer name/office/district from the page-1 header line (office blank for PACs/committees — routed via `_register_pac`; non-blank office → `_register_candidate_committee` with prefix `"leg19"`). `_pdf_section_header()` detects `Schedule A`-`D` headers, stripping the `\x0c` form-feed pdftotext inserts before page breaks and rejecting summary-page references like "...Total of all Schedule A sheets...". Schedule A (`_pdf_schedule_a`, contributions >$50) strips a leading `Primary`/`General`/etc. ElectionType token from the contributor name (a column between Date and Contributor that's often populated). Schedule B (`_pdf_schedule_b`, expenditures ≥$25) splits each wrapped address/explanation line on 3+-space column gaps, dropping bare `Support:`/`Oppose:` tokens and concatenating the remaining fragments into `purpose` (multi-line explanations are common). Schedule C (`_pdf_schedule_c`, in-kind) emits a paired contribution + expenditure row. Schedule D (`_pdf_schedule_d`, loans) emits a "Loan Received" row for the period's new-loan amount and a "Loan Payment" row if a repayment is present; the "Grand Total" row is skipped. Schedules E (Credit Card/Debt) and F (Pledged Contributions) were not observed populated in any sample and are not parsed. All ~461 PDFs are parsed inline within `IDAHO_PHASE=2` (folded in alongside archive `comm_cont` via a new `legacy_2019` stage in `raw_files()`'s phase-gating) — `pdftotext` is fast enough (~13ms/file, ~6s total) that no separate text-caching pass is needed. Logging is aggregated once for the whole batch rather than per-file. **Caveat**: 14 filer IDs have both a Mid-Year and a First-Annual-style PDF; a spot check (filer 10376) suggests First-Annual covers the post-Mid-Year period (non-overlapping), so processing both should not double-count, but this has not been exhaustively verified across all 14.

**Re-running the full parser** (`src/pipeline/parsers/idaho.py`) takes ~55-60s — over the 45s wall-clock cap of some sandboxed environments. The parser supports an optional `IDAHO_PHASE` env var (`1`, `2`, or `3`) that splits the run into three checkpointed passes (pickled to `data/Idaho/cleaned/_checkpoint_idaho_phase{1,2}.pkl`, cleaned up after phase 3): phase 1 = portal/electionstats/archive cand_cont/cand_exp (~38s, includes the 382K-row electionstats pass); phase 2 = archive comm_cont (all years, ~14s) + legacy_2019_pdfs (~6s for all 461 PDFs); phase 3 = archive comm_exp (all years) + final candidate/committee flush + person_id assignment (~5s). Unset (default): normal single-pass full run, unchanged from before.

## Data Notes

- **`state_filer_id` is structurally incomplete for candidates/committees** — 4,579/6,592 candidates (69.5%) and 5,119/7,567 committees (67.6%) originate from the 2000–2018 archive or the 2020–2022 legacy export, neither of which carries a filer/registration ID in the source data at all. Only portal-sourced (2020–2026, mostly 2023+) records have a `state_filer_id` ("Filing Entity Id"). This is a genuine data-availability gap, not a parser defect, and won't be resolved without a different ID source. As of 2026-06-12, `tests/validate.py` downgrades these two fill checks from tier-1 failures to tier-2 warnings for `id_model="name_hash"` states (`NAME_HASH_STATES` / `TIER1_OPTIONAL_FOR_NAME_HASH`), since `person_id` is derived from `candidate_name`, not `state_filer_id` — validate.py reports `PASS` for Idaho, with these two fields marked `↓` in the tier-1 fill-rate table.
- **2020–2022 gap: RESOLVED.** Previously, donations TO PAC/Central Committees and ALL spending by Candidate/PAC committees for 2020–2022 were unavailable — the only prior source (`id.electionstats.com`, non-.gov, donate→Candidate only) covered donations TO Candidate committees only. Replaced with a full pull of `canvass.sos.idaho.gov/eng/finances/get_activity.json` (`electionstats_activity.csv`, 382,124 rows, 2020-2024 — see Raw Data Structure), which covers donate AND spend for both Candidate and PAC committees; the parser emits only the 2020-2022 rows from this feed. 2023+ is fully covered by the current portal.
- **2019 calendar-year gap: RESOLVED.** Previously, 2019 appeared almost-empty (19 contributions/$12,286 and 30 expenditures/$8,703 by transaction `date`, vs. tens of thousands in every adjacent odd year) because there is no `archive_2020_*` biennium — Idaho's archive program ended with the 2018 biennium, the legacy export only starts 2020-01-01, and the portal's real data starts in 2023. Resolved by parsing 461 per-filer C-2 "Campaign Financial Disclosure Report" PDFs from `archive.sos.idaho.gov/ELECT/Finance/2020/` (see Raw Data Structure / Parser above) — 452/461 files parsed cleanly; ~1.7% are scanner-image PDFs with no text layer and are skipped.
- One outlier: a single row in `archive_2018_cand_cont.xlsx` has `date = 2028-08-28` (10 years past the file's biennium) — almost certainly a source data-entry typo (e.g. `2018` mistyped as `2028`). Left as-is (1 row, <0.001% of contributions); flagged here in case it skews a future year-filtered query.
- Minor tier-2 source-data-quality issues (all <1% of rows, left as warnings): ~360 `election_year` values of `"0"` (residual from the legacy file); ~0.1–0.2% of `contributor_state`/`payee_state` values are non-standard codes (e.g. `"10"`, `"boise"`); ~0.1–0.7% of `contributor_zip`/`payee_zip` values are malformed (e.g. `"0"`, `"*3702"`).
- Archive expenditure rows carry an extra single-letter `category` code (A–W, plus `Itemized`/`Unitemized`/`In-Kind`/`Independent Expenditure`) whose meaning varies by year — not currently mapped to an alias table.
- Alias mappings (`src/aliases/{contributor_types,transaction_categories,expenditure_categories,committee_types}.csv`) have not yet been added for Idaho — same as Hawaii and Florida, this is outstanding across several "done" states, not Idaho-specific.

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-14 |
| Parser | 2026-06-14 |
