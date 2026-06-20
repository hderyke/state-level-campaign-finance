# Hawaii — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Hawaii (HI) |
| **Source** | [Hawaii Campaign Spending Commission (CSC) Open Data](https://hicscdata.hawaii.gov) — Socrata SODA API |
| **Access method** | Pure HTTP/requests against the Socrata API (no Playwright needed) |
| **Coverage** | Contributions, expenditures, loans/debts — CC committees ~2006–present (start year varies 2006–2008 by schedule), NC committees ~2007–present; Socrata datasets always include the upcoming election cycle, so the scraped upper bound runs ~1 year ahead of today |
| **person_id model** | `committee` — `reg_no` is per-registration and alphanumeric (e.g. `CC12091`); `person_id` = min numeric portion for a given `(candidate_name, office, district)` |

---

## Raw Data Structure

14 Socrata datasets total, identified by an 8-character dataset ID (e.g. `jexd-xbcg`).

### Transaction Files — one CSV per relation per year: `{Stem}_{year}.csv`

**Candidate Committee (CC) side — Schedules A–F:**

| Relation | Stem | Dataset ID | Maps to |
|---|---|---|---|
| Contributions Received (Sched A) | `CCSchedA` | `jexd-xbcg` | contributions |
| Expenditures Made (Sched B) | `CCSchedB` | `3maa-4fgr` | expenditures |
| Other Receipts (Sched C) | `CCSchedC` | `ue3d-efjr` | contributions |
| Loans Received (Sched D) | `CCSchedD` | `yf4f-x3r4` | loans_debts |
| Unpaid Expenditures (Sched E) | `CCSchedE` | `rrkr-p5kv` | loans_debts |
| Durable Assets (Sched F) | `CCSchedF` | `fmfj-bac2` | loans_debts |

**Noncandidate Committee (NC / PAC) side:**

| Relation | Stem | Dataset ID | Maps to |
|---|---|---|---|
| Contributions Received (Sched A) | `NCSchedA` | `rajm-32md` | contributions |
| Contributions Made to Candidates (Sched B1) | `NCSchedB1` | `6huc-dcuw` | expenditures |
| Expenditures Made (Sched B2) | `NCSchedB2` | `riiu-7d4b` | expenditures |
| Other Receipts (Sched C) | `NCSchedC` | `m822-j8iy` | contributions |
| Unpaid Expenditures (Sched D) | `NCSchedD` | `dq35-6ks5` | loans_debts |
| Durable Assets (Sched E) | `NCSchedE` | `i778-my94` | loans_debts |

### Entity Registry Files — full pull, all years: `{Stem}_all.csv`

| Relation | Stem | Dataset ID | Purpose |
|---|---|---|---|
| Statement of Intent | `SOI_all` | `hc7x-8745` | Candidate registry (office/district/county/election) |
| Affidavits | `Affidavits_all` | `3fbc-bviy` | Candidate registry (additional registrants/cycles) |

### Key raw fields (CC side)

| Field | Description |
|---|---|
| `reg_no` | Committee registration number, e.g. `CC12091` (CC) / `NC20717` (NC). Used as `state_filer_id`. |
| `candidate_name` | Raw name in `"Last, First Middle"` format |
| `date` | Socrata floating timestamp (`YYYY-MM-DDTHH:MM:SS.000`) |
| `amount` | Dollar amount; parentheses indicate negatives |
| `office` / `district` / `county` / `party` | Candidate registration details (Sched A/D/F only — not on Sched B/C/E) |
| `election_period` | e.g. `"2024-2026"` — `election_year` = the later of the two years |
| `contributor_name` / `vendor_name` / `lender_name` / `source_name` | Counterparty name, varies by schedule |
| `non_resident_yes_or_no_`, `inoutstate`, `range`, `mapping_address`, `location_1` | Socrata-added geocoding/derived columns — not used by the parser |

### Key raw fields (NC side)

| Field | Description |
|---|---|
| `reg_no` | NC committee registration number, e.g. `NC20717` |
| `noncandidate_committee_name` | PAC/committee name — used directly as `committee_name` (no separate name field exists) |
| `cc_reg_no` / `candidate_committee_name` / `candidate_name` | On Sched B1 only — identifies the recipient CC committee for a contribution-to-candidate |
| `candidate_name_s` | On Sched B2 — free-text list of candidates supported/opposed by an independent expenditure |
| `independent_expenditure` | On Sched B2 — transaction type label |

---

## Scraper

`src/pipeline/scrapers/hawaii.py`

Pure `requests`-based pull from the Socrata SODA API — no browser automation needed.

**Pagination:** `$limit=50000` with `$offset` and `$order=:id`, looping until a page returns fewer than `PAGE_SIZE` rows.

**Year splitting:** For each of the 12 transaction relations, `get_date_range()` queries `min(date)`/`max(date)` to find the dataset's span, then fetches one file per year via `$where=date_extract_y(date)={year}` (the `date between ...` / `date>=...` syntax returns empty results on this Socrata instance — `date_extract_y` is the working approach). The max year is padded by one beyond `max(today, dataset max)` to catch future-dated filings for the next election cycle.

**Entity registries:** SOI and Affidavits are pulled in full (no year split) into `SOI_all.csv` / `Affidavits_all.csv`.

**Manifest-based resumability:** `data/Hawaii/manifest.csv` records `(relation_type, year)` → filename/row_count. A no-flag run skips any `(relation, year)` already in the manifest with a non-empty file, except the current year (always re-fetched). `--start-year`/`--end-year` strip matching manifest entries first, so they force re-download rather than resume — only a no-flag run is resumable.

**Scope flags:** `--transactions`, `--entities`, `--contributions` (CC A/C/D, NC A/B1/C), `--expenditures` (CC B/E/F, NC B2/D/E), `--force` (wipe + redownload in scope).

**Expected runtime:** Full scrape (14 datasets × up to ~20 years) takes several minutes; reruns with manifest mostly populated take well under a minute.

---

## Parser

`src/pipeline/parsers/hawaii.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz`, `committees.csv.gz`, `candidates.csv.gz`

**Processing order:** `SOI_all.csv` → `Affidavits_all.csv` → CC Sched A–F (all years) → NC Sched A/B1/B2/C/D/E (all years). SOI/Affidavits run first so every registrant gets a candidates + committees row even with zero transactions — this drives the high tier-1 fill rates.

**Key transformations:**
- `reg_no` is used directly as `state_filer_id` for both candidates and committees. CC reg_nos start with `CC`, NC reg_nos with `NC`.
- Names arrive as `"Last, First Middle"`. `format_name()` reorders to `"First Middle Last"` for `candidate_name`/`committee_name`/`payee_name`; `split_name()` populates `candidate_first`/`candidate_last`.
- **CC committees:** `committee_name = candidate_name` (no separate committee-name field exists in the source). `committee_type = "Candidate Committee"`.
- **NC committees:** `committee_name = noncandidate_committee_name`, `candidate_name = ""` (blank → no `person_id` via `assign_committee_person_ids`). `committee_type = "Noncandidate Committee"`.
- `register_cc()` / `register_nc()` create-or-enrich the candidate/committee registries incrementally as each file is processed; `_fill()` only sets a field if it's currently empty, so the first file to populate office/district/party/county "wins" and later blank values don't overwrite it.
- `election_year` = the later year in `election_period` (e.g. `"2024-2026"` → `"2026"`), via `parse_election_year()`.
- Rows with unparseable/blank `amount` are dropped (`if not amount: continue`); `"0"` is kept.
- **NC Sched B1** (Contributions Made to Candidates) is recorded as an **expenditure** from the NC committee's books (`transaction_type = "Contribution to Candidate"`), and also enriches the recipient CC committee/candidate registry via `cc_reg_no`.
- Loan/debt `record_type` values: `"Loan Received"` (CC Sched D), `"Unpaid Expenditure"` (CC Sched E / NC Sched D), `"Durable Asset"` (CC Sched F / NC Sched E).

**person_id model:** `committee` — `reg_no` is per-committee-registration (a candidate gets a new `CC#####` each cycle), so `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(reg_no)` for the group. Because `reg_no` is alphanumeric (`"CC12091"`), `utils._numeric_id()` (added 2026-06-11, see project memory) strips the non-digit prefix before the int conversion that `_make_person_id` requires.

**Expected runtime:** ~10s.

---

## Data Notes

- **Tier 1 — PASS:** `state_filer_id` 100%, `candidate_name` 99.9%, `person_id` 99.9% (1,328 candidates).
- **`incumbent` 0%:** Not present anywhere in the CSC source data (SOI/Affidavits/Schedules) — left blank for all candidates.
- **Committees `candidate_name` 64.9%:** Reflects the true CC/NC split — 1,328 Candidate Committees (64.9%) have `candidate_name` populated, 717 Noncandidate Committees (35.1%) are intentionally blank (PACs have no associated candidate).
- **Committees `treasurer_name`/`city`/`zip` 0%:** No treasurer or address fields exist in any of the 14 Socrata datasets.
- **Contributions `employer`/`occupation` ~25%:** Only populated for individual contributors on CC Sched A; PAC-side and non-individual contributions have no employer/occupation in the source.
- **Contributions/Expenditures `candidate_name`/`office` ~55–91%:** Only CC-side rows (and NC Sched B1, which carries a recipient candidate) have a candidate association; pure NC contributions/expenditures (Sched A/C/B2) have none by definition.
- **`election_year` 2 outliers:** A couple of candidate rows have a malformed `election_period` like `"2021/2022"` (slash instead of hyphen) that doesn't match the `\d{4}` extraction cleanly — flagged by the validator, not blocking.
- **`contributor_state`/`payee_state` non-standard codes:** A handful of free-text entries (`"BC"`, `"Hi"`, `"ca"`, `"FM"`, `"JP"`, `"**"`, etc.) — real-world data entry variance (Canadian provinces, lowercase state codes, foreign countries, placeholder values), not a parsing error.
- **`contributor_zip`/`payee_zip` ~0.6% non-standard:** Malformed ZIPs (`"0"`, `"000000"`, `"*****"`, truncated codes) present in the raw CSC data.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-11 |
| Parser | 2026-06-11 |
