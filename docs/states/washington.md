# Washington — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Washington (WA) |
| **Source** | [Washington Public Disclosure Commission (PDC) Open Data](https://data.wa.gov) — Socrata SODA API |
| **Access method** | Pure HTTP/requests against the Socrata API (no Playwright needed) |
| **Coverage** | Contributions/expenditures/loans/debt — real data from ~2004–present (a handful of stray pre-2004 rows exist back to the 1900s/1990s; these are data-entry errors in the source, not gaps in scraping) |
| **person_id model** | `person` — `filer_id` is already person-level ("consistent across election years" per PDC's own field docs) — same family as AR/CO/MN, no grouping needed |

---

## Raw Data Structure

4 Socrata datasets total, identified by a dataset ID (e.g. `kv7h-kjye`). **None of them is a standalone candidate/committee registry** — every row already carries the filer's identity inline (`filer_id`, `filer_name`, `office`, `legislative_district`, `party`, `jurisdiction`(`_county`/`_type`), plus `committee_id`), so candidates and committees are built directly from these 4 transaction files rather than from a separate entity pass.

| Relation | Stem | Dataset ID | Maps to | Date field |
|---|---|---|---|---|
| Contributions to Candidates and Political Committees | `Contributions` | `kv7h-kjye` | contributions | `receipt_date` |
| Expenditures by Candidates and Political Committees | `Expenditures` | `tijg-9zyp` | expenditures | `expenditure_date` |
| Debt Reported by Candidates and Political Committees | `Debt` | `3r6b-hsaa` | loans_debts | `debt_date` |
| Loans to Candidates and Political Committees | `Loans` | `d2ig-r3q4` | loans_debts | `receipt_date` |

### Two different identifiers

- **`filer_id`** — person-level. Per PDC's own field description: "consistent across election years", with one documented exception — a candidate running for a second office in the same election year gets a second `filer_id` with no link back to the first (a real ambiguity in the source, not a parsing bug). Used as `state_filer_id` on **candidates**; `id_model="person"`.
- **`committee_id`** — committee-level. Per PDC: single-year committees and candidate committees get a new id every year even for the same person/org; continuing committees and surplus accounts keep one id across years. Used as `state_filer_id` on **committees** (`committees.person_id` is filled in afterwards by `utils.assign_committee_person_ids()` via `candidate_name` matching, not from `committee_id` itself).

### Key raw fields (shared across contributions/expenditures/loans)

| Field | Description |
|---|---|
| `filer_id` / `filer_name` | Person/entity identity — see above |
| `type` | `"Candidate"` or `"Political Committee"` (debt spells this `filer_type` = `"CA"`/`"CO"` instead) |
| `office` / `legislative_district` / `party` / `jurisdiction` / `jurisdiction_county` / `jurisdiction_type` | Candidate registration details; blank for Political Committee rows |
| `election_year` | The election year (candidates/single-year committees) or reporting year (continuing committees) |
| `origin` | Form/schedule code, e.g. `C3` (cash contribution), `A/GT50` (itemized expenditure). Codes starting `C.` (e.g. `C.1`, `C.2`, `C.3`, distinct from `C3`/`C3.1A`) mark **correction** records — used to derive `amended` |
| `report_number` | Groups all records filed together; used as `filing_id` |

### Dataset-specific fields

- **Contributions**: `cash_or_in_kind` (→ `transaction_type`), `code`/`contributor_category` (→ `contributor_type`), `contributor_name/address/city/state/zip`, `contributor_occupation`, `contributor_employer_name/city/state`.
- **Expenditures**: `itemized_or_non_itemized` (→ `transaction_type`), `code` (→ `category`), `recipient_name/address/city/state/zip` (→ `payee_*`). `payee`/`creditor` (reimbursement-chain fields) exist but aren't mapped to canonical columns — `recipient_name` is the officially reported payee and is populated on every row; `payee`/`creditor` are sparse, reimbursement-specific overlays.
- **Loans**: `transaction_type` = `Received` / `Payment` / `Interest` / `Forgiven` (all 4 map to `loans_debts.record_type` as `"Loan {transaction_type}"`), `lenders_name/address/city/state/zip`.
- **Debt**: `record_type` is always the literal string `"DEBT"` — `loans_debts.record_type` is hard-set to `"Debt"` rather than passed through. `vendor_name/address/city/state/zip`. `from_date`/`thru_date` (reporting period) exist alongside `debt_date` (when the debt was incurred); the parser uses `debt_date`, falling back to `thru_date` when blank.

### Data-quality notes confirmed directly against the live API

- Contributions is large — **6.3M rows** (Expenditures ~1.1M, Debt ~84K, Loans ~25K).
- A small long tail of rows (~14.5K out of 6.3M contributions, proportionally similar elsewhere) has a NULL date field or a garbage year (`"202"`, `"1024"`, `"1900"`, `"2202"`, `"2041"`, etc. — confirmed via `$select=date_extract_y(...) as y,count(*)&$group=y` against the API, not a parsing artifact).

---

## Scraper

`src/pipeline/scrapers/washington.py`

Pure `requests`-based pull from the Socrata SODA API — no browser automation needed.

**Pagination:** `$limit=50000` with `$offset` and `$order=:id`, looping until a page returns fewer than `PAGE_SIZE` rows (same pattern as `scrapers/hawaii.py`).

**Year splitting:** Each of the 4 relations is fetched one file per year via `$where=date_extract_y({date_field})={year}`, looping `EARLIEST_YEAR=2000` through `current_year+2`. A **misc bucket** (`{Stem}_misc.csv`) is fetched separately per relation via `$where=({date_field} IS NULL) OR (date_extract_y({date_field}) < 2000) OR (date_extract_y({date_field}) > current_year+2)` to sweep up the NULL-date/garbage-year long tail described above without forcing the year loop to span centuries.

**Manifest-based resumability:** `data/Washington/manifest.csv` records `(relation_type, year)` → filename/row_count, where `year` is either a 4-digit year or the literal `"misc"`. A no-flag run skips any `(relation, year)` already in the manifest with a non-empty file, except the current year and the misc bucket (both always re-fetched — the misc bucket isn't bounded by a manifest-year check since new rows could enter or leave it at any time). `--start-year`/`--end-year` strip matching manifest entries first (forcing re-download), and always re-fetch misc.

**Scope flags:** `--contributions`, `--expenditures` narrow to one relation; `--candidates`/`--committees` pull all 4 (there's no narrower registry dataset to scope to — see above); `--force` wipes + redownloads in scope.

**Expected runtime:** A full scrape (4 datasets × ~28 years, dominated by the 6.3M-row Contributions dataset) takes a while on the first run; reruns with the manifest populated take well under a minute (just the current year + misc bucket per relation).

---

## Parser

`src/pipeline/parsers/washington.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz`, `committees.csv.gz`, `candidates.csv.gz`

**Processing order:** Contributions → Expenditures → Loans → Debt (all years + misc, per relation). Candidates/committees are registered incrementally as each file is processed, then flushed at the end.

**Key transformations:**
- Rows with unparseable/blank `amount` are dropped (`if not amount: continue`); `"0.00"` is kept.
- `filer_kind()` normalizes `type` (`"Candidate"`/`"Political Committee"`) and debt's `filer_type` (`"CA"`/`"CO"`) into one shared `"Candidate"` / `"Political Committee"` value used throughout.
- `is_amended()` flags `origin` codes starting `C.` (e.g. `"C.1"`) as `amended="1"` — distinct from originalfiling codes that merely start with `C` (e.g. `"C3"`, `"C3.1A"`).
- Names arrive as `"First [Middle] Last"`, sometimes with a trailing parenthetical nickname (e.g. `"Luz D. Barefoot (Lucy Barefoot)"`, `"Loujanna Rohrer (Loujanna \"LJ\" Rohrer)"`). `split_name()` strips the trailing `(...)` before splitting into `candidate_first`/`candidate_last` (no comma-inversion needed, unlike Hawaii). The full raw name (nickname included) is kept as `candidate_name`/`committee_name`/`payee_name` via `utils.clean_name()`.
- **Candidate registry (`register_candidate`, keyed by `filer_id`) is recency-weighted, not first-wins:** the same `filer_id` can recur across many election cycles with a different office/district/party each time (someone running for State Representative in 2018 and State Senate in 2022 is the same `filer_id`). A row whose `election_year` is ≥ the best year seen so far for that `filer_id` overwrites `office`/`district`/`party`/`jurisdiction` outright; an older-cycle row only backfills currently-blank fields. `election_year` on the output row separately tracks the max year seen, regardless of the recency tie-break. This differs from Hawaii's simple first-non-blank-wins, because Hawaii's `reg_no` is already split per cycle — WA's `filer_id` is not.
- **Committee registry (`register_committee`, keyed by `committee_id`)** uses plain first-wins enrichment (`_fill()`) — `committee_id` is already scoped to one committee-cycle (or a genuinely continuing committee), so there's no cross-cycle ambiguity to resolve.
- A filer only becomes a **candidates** row when `filer_kind()` says `"Candidate"`; Political Committee filer_ids only produce a **committees** row, with blank `candidate_name` (same convention as Hawaii's NC/PAC committees).
- Loan `record_type` = `"Loan {transaction_type}"` (`"Loan Received"`, `"Loan Payment"`, `"Loan Interest"`, `"Loan Forgiven"`); debt `record_type` is hard-set to `"Debt"`.
- WA has no committee address fields in any of the 4 datasets (only contributor/vendor/lender addresses) — `committees.treasurer_name`/`city`/`zip` are blank for every row, same gap as Hawaii.

**person_id model:** `person` — `filer_id` is already a stable person-level ID across election cycles, so `assign_person_ids(id_model="person")` derives `person_id` directly from `state_filer_id` (FIPS-prefixed), no grouping pass needed. `utils._numeric_id()` strips the embedded spaces/letters WA pads `filer_id` with (e.g. `"THOMP  165"` → `165`, `"WHEEM--024"` → `024`) before the int conversion `_make_person_id` requires.

**Expected runtime:** Well under a minute even at full multi-million-row scale (single sequential pass per file, no re-reads).

---

## Data Notes

- **`incumbent` 0%:** Not present anywhere in the 4 PDC datasets — left blank for all candidates, same gap as most other states without a dedicated registry file.
- **Committees `treasurer_name`/`city`/`zip` 0%:** No committee address/officer fields exist in any of the 4 Socrata datasets (only contributor/vendor/lender-side addresses are captured).
- **Committees `candidate_name` split reflects the true Candidate/PAC mix:** Candidate Committees get `candidate_name` populated (same as `committee_name`); Political Committees are intentionally blank.
- **`contributor_type`/`category` fields:** `contributor_type` uses the richer `code` field (`"Individual"`, `"Business"`, `"Political Action Committee"`, etc., already human-readable in the API — not the single-letter codes PDC's documentation describes) with `contributor_category` (`"Individual"`/`"Organization"`) as fallback.
- **`amended` is sparse by design:** only the contributions/expenditures/loans datasets carry `C.`-prefixed correction origin codes at all (and even there, corrections are a small minority); debt's `origin` is always `"B.3"` with no correction variant observed.
- **A couple of `election_year` outliers (e.g. `"2202"`)** will fail the validator's plausibility range (1990–2030) as a tier-2 warning, not a blocking failure — real data-entry noise in the source, confirmed against the live API, same category of issue as Hawaii's malformed `election_period` values.
- **No alias-table entries added yet** (`src/aliases/{committee_types,contributor_types,expenditure_categories,transaction_categories,office_types}.csv` have no `WA` rows) — these are aggregate-layer canonicalization tables, optional polish beyond the scraper/parser itself; a future pass could populate them from the `code`/`type` values documented above.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-18 |
| Parser | 2026-07-18 |
