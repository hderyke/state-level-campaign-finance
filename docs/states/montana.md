# Montana — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Montana (MT) |
| **Source** | [Campaign Electronic Reporting System (CERS)](https://cers-ext.mt.gov/CampaignTracker/public/search) — Commissioner of Political Practices |
| **Access method** | Reverse-engineered JSON/text AJAX API (no bulk export exists on the public site) |
| **Coverage** | 2000 – present (CERS's own election-year picker floors at 2000) |
| **person_id model** | `committee` — a candidate's `electionYear` is embedded in their own CERS record, suggesting per-cycle re-registration rather than one stable ID across cycles (assumption, not yet confirmed — see Data Notes) |

**Replaces:** an R/Selenium function that drove a real browser through the CERS search UI (click the Contributions tab, spin the election-year picker, tick each result row's checkbox, click Download, repeat per page). This scraper instead calls the same endpoints that UI calls via AJAX, identified from a verified third-party implementation of this API (see Scraper section).

---

## Raw Data Structure

`data/Montana/raw/` contains:

| File | Content |
|---|---|
| `candidates_{year}.json` | Every candidate CERS returns for an election-year search (all offices, all parties) |
| `committees_{year}.json` | Every committee with reported financial activity that year |
| `candidate_{id}.json` | One candidate's full bundle: registry fields + every filed report, each with its itemized contributions/expenditures |
| `committee_{id}.json` | Same, for one committee |

### candidates_{year}.json / committees_{year}.json

Raw `aaData` rows from CERS's DataTables search-results endpoint — `candidateId`/`committeeId`, `candidateName`/`committeeName`, `electionYear`, `officeTitle` (candidates), `candidateTypeDescr`/`committeeTypeDescr`, `partyDescr` (candidates), `candidateStatusDescr`/`committeeStatusDescr`.

### candidate_{id}.json / committee_{id}.json

Top-level fields mirror the corresponding yearly list row, plus a `reports` array. Each report entry has `reportId`, `formTypeCode` (`C5`/`C6`/`C4`/`C7`/`C7E`), `fromDateStr`, `toDateStr`, `statusDescr`, `amendedDate`, and one of two shapes depending on form type:

**C5 (candidate periodic) / C6 (committee periodic) / C4 (committee independent-expenditure-style periodic)** — `contributions`/`expenditures` are list-of-dict rows read directly from CERS's pipe-delimited bulk schedule export, with the server's own column headers preserved: `Date Paid`, `Entity Name`, `First Name`, `Middle Initial`, `Last Name`, `Addr Line1`, `City`, `State`, `Zip`, `Contribution Type` (numeric 1–9), `Amount`, `Amount Type` (`CA`/`IK`/`Mixed`), `Purpose`, `Election Type`, `Previous Transaction (Y/N)` for contributions; `Date Paid`, `Entity Name`, `Expenditure Type`, `Amount`, `Purpose`, `Election Type` for expenditures.

**C7 (last-minute contribution notice) / C7E (last-minute expenditure notice)** — no bulk export exists; `contributions_c7`/`expenditures_c7e` hold the server's native JSON line items per sub-table (`individual`/`committee`/`loan` donors for C7; `expendOther` for C7E), with fields `entityName`, `entityAddress` (single `"street, city, ST zip"` string), `datePaid` (epoch milliseconds), `cashAmt`, `inKindAmt`, `totalAmt`, `occupationDescr`, `employerDescr`, `amountTypeDescr` (the election phase — Primary/General, not a cash/in-kind flag), `previousTransactionInd`.

A companion `manifest.csv` tracks `(entity_type, entity_id)` pairs already fetched.

---

## Scraper

`src/pipeline/scrapers/montana.py`

CERS's public search UI has no bulk export — it's limited to one candidate/committee for one election year at a time, with a per-search CSV export button. Rather than automate that UI (what the original R function did via Selenium), this scraper calls the underlying AJAX endpoints directly:

1. POST blank search params + `electionYear` to establish server-side search state, then GET the DataTables results endpoint — returns every candidate/committee active that year in one call (candidates and committees are fetched independently, both filterable by year alone).
2. For each entity, POST its ID to list every report it has filed.
3. For each report: C5/C6/C4 reports get their bulk pipe-delimited schedule downloaded (POST to get a filename token, GET the file); C7/C7E reports have their line-item sub-tables fetched directly as JSON (no download step).

These endpoints, payloads, and the pipe-delimited export format were identified from Montana Free Press's open-source [`cers-interface`](https://github.com/eidietrich/cers-interface) project, which has scraped this same site every election cycle through the 2026 cycle using this exact API — strong evidence it's still current.

**IMPORTANT CAVEAT:** this development environment's network egress does not reach `cers-ext.mt.gov`, so these endpoints could not be smoke-tested against the live site. The scraper's control flow (manifest incremental/skip logic, current-year refresh, JSON structure, pipe-delimited parsing) was verified with a mocked HTTP layer standing in for CERS's real responses — see the parser section for what that test confirmed — but the actual endpoint URLs, payload field names, and response shapes are only as reliable as the third-party reference they were copied from. **Run a small slice locally first** (e.g. `python3 src/pipeline/scrapers/montana.py --start-year 2024 --end-year 2024 --candidates`) and inspect `data/Montana/raw/` before trusting a full backfill.

**Limitations:**
- No pagination needed — `iDisplayLength` is set to 1,000, comfortably above any single year's candidate/committee count based on the reference project's experience.
- A fresh `requests.Session()` is created for nearly every POST+GET pair, mirroring the reference implementation — the app appears to key search/report state off the session cookie, and reusing one session across unrelated lookups risked cross-contaminating server-side state.
- `--contributions`/`--expenditures`/`--entities`/`--transactions` flags are accepted (for CLI-contract consistency with other states) but ignored — fetching an entity's reports always yields both contributions and expenditures together, so there's no cheaper partial fetch. Only `--candidates`/`--committees` meaningfully narrow scope here.
- Only the current year's entities are re-fetched on an incremental run; a candidate/committee's data from a past election year is treated as final once fetched (use `--force` or `--start-year` to refresh it, e.g. after an amendment).

**Expected runtime:** unverified against the live site, but likely long for a full 2000–present backfill — each entity requires several sequential requests (report list, then per-report schedule/detail fetches), and Montana has run many election cycles across state, legislative, and local races. Design this as an overnight/background job for the first run; incremental re-runs should be much faster since only the current year is re-swept.

---

## Parser

`src/pipeline/parsers/montana.py`

Builds `candidates.csv`/`committees.csv` from the yearly search-result files (the authoritative roster — every searched entity gets a row even if its full-report fetch later failed) and `contributions.csv`/`expenditures.csv` from the per-entity report bundles.

**person_id model:** `committee` — see Overview. This groups candidates by `(state, candidate_name, office, district)` and takes the earliest `state_filer_id`, same strategy used for Alabama/Arizona/California.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv` (empty — CERS has no separate loans/debts schedule; loans surface as itemized contribution rows instead, `Contribution Type` code 3).

**Key transformations:**
- `Contribution Type` (numeric 1–9, describes contributor category) → `contributor_type`, raw code preserved and canonicalized via `src/aliases/contributor_types.csv`
- `Amount Type` (`CA`/`IK`/`Mixed`) → `transaction_type`, canonicalized via `transaction_categories.csv`; C7 rows have this derived from `cashAmt`/`inKindAmt` since the JSON API gives the raw dollar splits instead of a precomputed flag
- C7/C7E dates arrive as epoch milliseconds, converted to `YYYY-MM-DD`
- C7/C7E addresses arrive as a single `"street, city, ST zip"` string, parsed with the same regex approach used by Arkansas
- Candidates have no separate campaign-committee entity in CERS's data model — `committee_name` on candidate-sourced rows is just the candidate's own name
- `Previous Transaction (Y/N)` / `previousTransactionInd` → `amended` (1/0)

**Limitations:**
- Pipe-delimited schedule column headers were sourced from the same third-party reference implementation as the scraper's endpoints, not a live sample. If a real scrape's headers differ, the parser's `.get(...)` lookups (all written defensively — missing keys just yield blank) will need updating to match.
- `Expenditure Type` (numeric code on the C5/C6/C4 schedule) is left completely unmapped in `expenditure_categories.csv` — its code meanings were not confirmed against real data.
- `district` is left blank for all candidates — CERS's `officeTitle` embeds the district in free text (e.g. "House District No. 42") but no separate structured field was confirmed available; a future pass could regex-extract it.

**Expected runtime:** fast — parsing is pure local JSON/CSV processing with no network calls; runtime scales with however much raw data the scraper collected.

---

## Data Notes

- **No candidate-committee linkage** — CERS candidates and committees are distinct filer types with no cross-reference exposed by the search/report APIs this scraper uses. Every `committees.csv` row has a blank `candidate_name`, so `assign_committee_person_ids()` will not match any of them to a candidate — expected, not a bug.
- **person_id model is an assumption** — whether `candidateId` is stable across a person's multiple election cycles, or reassigned each cycle (like Alabama/Arizona), was not confirmed against real multi-cycle data in this environment. If a real scrape shows the same person keeping one `candidateId` across cycles, switch `id_model` to `"person"` in the parser.
- **Live endpoints unverified** — this environment's network egress doesn't reach `cers-ext.mt.gov`. Every endpoint URL, payload, and response shape in the scraper was taken from Montana Free Press's `cers-interface` reference project (actively used through the 2026 cycle) rather than confirmed firsthand. Treat the first run as a smoke test.
- **Loans surface as contributions** — CERS has no dedicated loans/debts schedule; a loan to a campaign appears as a normal itemized contribution row with `Contribution Type` code 3. `loans_debts.csv` is therefore always empty, same treatment as Arkansas and Kansas.
- **C7/C7E "last-minute" notices are pre-election-only filings** — they cover large contributions/expenditures in the final days before an election and don't roll up into the periodic C5/C6/C4 totals. Both are itemized separately in this parser's output rather than merged, since they come from genuinely different report filings.
- **Expenditure category codes unmapped** — see Parser Limitations above.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-12 |
| Parser | 2026-07-12 |
| Documentation | 2026-07-12 |
