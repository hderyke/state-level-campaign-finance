# State-Level Campaign Finance Pipeline

## Contents

1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
3. [Individual Components](#individual-components)
4. [Contributing](#contributing)
5. [Progress](#progress)

---

## Introduction

This project aims to build a unified database of campaign finance disclosures across U.S. states. The pipeline extracts raw data from each state's specific disclosure website, standardizes it to a common schema, and loads it into both state-level databases and a combined master database- all queryable via SQL and R.

### Goal
Collect and normalize campaign finance data from all 50 states into a single, unified interface for analyzing state-level money in politics.

### Output
- **State-level databases**: Individual SQLite/DuckDB databases for each state with standardized tables
- **State-level CSVs**: Canonical CSV exports of contributions, expenditures, candidates, and committees for each state
- **Combined master database**: A unified SQLite/DuckDB database merging all states' normalized data

---

## Quick Start

### Setup

**Clone the repository:**
```bash
git clone https://github.com/hderyke/state-level-campaign-finance.git
cd state-level-campaign-finance
```

**Install dependencies:**
```bash
pip install -r requirements.txt
playwright install
```

**Configure your user agent:**

`config.py` contains a `USER_AGENT` string used by all scrapers to avoid 403s from state disclosure sites. The default is a Mac Chrome string — if you're on Linux or Windows, replace it with your own. You can find your user agent at [whatismybrowser.com](https://www.whatismybrowser.com).

### Running the Pipeline

Run the full pipeline (scrape → parse → validate → tabulate → aggregate) for one or more states using two-letter abbreviations:

```bash
python3 src/main.py sync AL AK AZ
```

Or run all implemented states at once:

```bash
python3 src/main.py sync all
```

**Pipeline commands:**

| Command | What it does |
|---|---|
| `sync <states>` | Full pipeline — scrape → parse → validate → tabulate → aggregate |
| `reparse <states>` | Skip scrape; re-run parse → validate → tabulate → aggregate on existing raw data |

**Scraper flags** — passed after the state list, forwarded to each scraper. Not all states support every flag; unsupported flags are silently ignored.

Vertical scope — controls *which years* are downloaded (mutually exclusive):

```
(no flag)              incremental — fill manifest gaps, always refresh current year
--force                re-download everything in scope, wipe manifest
--start-year YYYY      re-download years ≥ YYYY
--end-year YYYY        re-download years ≤ YYYY  (combine with --start-year for a range)
```

Horizontal scope — controls *which data types* are downloaded (additive, stack freely):

```
(no flag)              everything
--transactions         contributions + expenditures
--entities             committees + candidates
--contributions        contributions only
--expenditures         expenditures only
--candidates           candidates only
--committees           committees only
```

**Global flags:**
- `--daemon` — silent mode for scheduled/cron runs
- `--no-report` — skip HTML report generation after run

**Examples:**

```bash
python3 src/main.py sync AK --start-year 2023             # re-download AK from 2023 onwards
python3 src/main.py sync AK --force --transactions        # force-refresh AK transactions only
python3 src/main.py sync AL AK --start-year 2022 --end-year 2024 --contributions
python3 src/main.py reparse AL                            # re-parse AL without scraping
python3 src/main.py --daemon sync all                     # full run, silent mode
```

### S3 Data Sync

The `push` and `pull` commands sync state data and the master database to and from S3. A `push db` also regenerates the downloads manifest (`metadata/manifest.csv`, the list of available states the client-facing downloads page reads) from the master database and uploads it alongside — so the site's state list stays current on every db push without a separate deploy step. **They depend on `cloud/s3.py`, a personal AWS backend that is not included in this repo** — running `push`/`pull` on a fresh clone exits with a "bring your own bucket" message. To use them you'll need your own S3 bucket and a compatible `cloud/s3.py` implementation. See [docs/pipeline.md](docs/pipeline.md) for the expected interface. The core pipeline (`sync`/`reparse`) works fully without any of this.

### Output

After a pipeline run, data is written to the following locations:

- `data/{State}/raw/` — raw downloaded files
- `data/{State}/cleaned/` — normalized CSVs (contributions, expenditures, candidates, committees, loans_debts)
- `data/{State}/{state}.db` — state-level database
- `data/state-level-cf.db` — master database combining all states

Each run also generates a log and HTML report under `logs/prod/{run_id}/` (or `logs/daemon/{run_id}/` for runs made through the scheduling wrapper, so scheduled runs don't bury one-off manual ones):

- `log.jsonl` — structured event log for the run
- `report.html` — human-readable summary of the run, with a tab per state for multi-state runs
- `{state}_validate.json` — validation report for each state processed

---

## Individual Components

> **Note:** Running components directly operates in dev mode. Logs are written to `logs/dev/{timestamp}-{state}-{operation}.jsonl` (no HTML report generated). Console output is more verbose than a full pipeline run via `main.py`.

### Scrapers

Downloads raw campaign finance data from a state's disclosure website. Each state has a dedicated scraper that handles its unique source format. Downloaded files are written to `data/{State}/raw/` and tracked in `data/{State}/manifest.csv` to allow resumable downloads.

```bash
python3 src/pipeline/scrapers/alabama.py [flags]
```

Scrapers accept the same vertical and horizontal flags as the `sync` command. See [Running the Pipeline](#running-the-pipeline) for the full flag reference. Not every state supports every flag — check `docs/states/{state}.md` for state-specific details.

### Parsers

Reads a state's raw files from `data/{State}/raw/` and normalizes them into five canonical CSVs written to `data/{State}/cleaned/`: `contributions.csv`, `expenditures.csv`, `candidates.csv`, `committees.csv`, and (if availiable)`loans_debts.csv`. Each parser is written specifically for its state's raw data format.

```bash
python3 src/pipeline/parsers/alabama.py
```


### Validate

Runs a tiered validation check on a state's cleaned CSVs, checking for hard failures (missing columns, bad types), warnings (implausible dates, missing IDs), and schema drift between runs. Writes a report to `metadata/{state}_latest.json`.

```bash
python3 src/pipeline/validate.py alabama
```


### Spot-Check Queries

Runs queries against a state's database to evaluate data quality after tabulation.

```bash
python3 src/pipeline/queries.py alabama
```


### Aggregate

Merges all tabulated state databases into the master `data/state-level-cf.db`. Operates on all states present in `data/`.

```bash
python3 src/pipeline/aggregate.py
```


For more detailed source code documentation, see [docs/pipeline.md](docs/pipeline.md).

---

## Contributing

For full details on adding a new state, see [docs/contributing.md](docs/contributing.md).

### Adding a New State

1. **Create scraper**: `src/pipeline/scrapers/{state}.py`
2. **Create parser**: `src/pipeline/parsers/{state}.py`
3. **Test & validate**: `python3 src/pipeline/validate.py {state}`
4. **Register**: Add state to the main orchestrator
5. **Document & commit**: Add `docs/states/{state}.md` and alias mappings, then commit to the repo

---

## Progress

| State | Scraper | Parser | Notes |
|-------|---------|--------|-------|
| Alabama (AL) | ✅ | ✅ | |
| Alaska (AK) | ✅ | ✅ | Playwright required (WAF blocks datacenter IPs) |
| Arizona (AZ) | ✅ | ✅ | Normal sync refreshes current calendar year only |
| Arkansas (AR) | ✅ | ✅ | |
| California (CA) | ✅ | ✅ | HTTP Range ZIP extraction; large files (~34M rows) |
| Colorado (CO) | ✅ | ✅ | Candidate/committee entity sweep via SeqID probe |
| Connecticut (CT) | ✅ | ✅ | |
| Delaware (DE) | ✅ | ✅ | |
| Florida (FL) | ✅ | ✅ | Playwright required; large files take time on first run |
| Georgia (GA) | ✅ | ✅ | Three sources: Peachfile (2025+), legacy API (2006–2024), CSC bulk |
| Hawaii (HI) | ✅ | ✅ | Socrata SODA API; 14 datasets |
| Idaho (ID) | ✅ | ✅ | Three source eras (2020+ portal, ES bulk, legacy PDFs) |
| Illinois (IL) | ✅ | ✅ | Full-history flat files updated nightly; large |
| Indiana (IN) | ✅ | ✅ | Bulk ZIP by year; entity sweep via CommitteeDetail pages |
| Iowa (IA) | ✅ | ✅ | Individual PDFs via IECDB API |
| Kansas (KS) | 🚧 | 🚧 | In development — a `kansas_v2` rewrite is underway (see `kansas_v2.py`). The original `kansas.py` works (individual PDFs from KPDC static index pages, name_hash ID, no party data in source) but is being superseded; treat output as provisional |
| Kentucky (KY) | ✅ | ✅ | KREF flat CSV exports; per-party candidate exports for party data; per-year contributions + expenditures; name_hash ID |
| Louisiana (LA) | ✅ | ✅ | Bulk CSVs in 4-year ranges; no committee_type in source; person_id from stable FilerNumber |
| Maine (ME) | ⚠️ | ⚠️ | In progress; occupation/employer enrichment requires a per-transaction detail-page scrape via Playwright (~400K pages) |
| Maryland (MD) | ✅ | ✅ | POST JSON bulk export API (MDCRIS); committee person_id = min Filing Entity Id per (candidate_name, office, district) |
| Massachusetts (MA) | ✅ | ✅ | Direct download from Azure Blob Storage (OCPF); committee person_id = min CPF ID per (candidate_name, office, district) |
| Michigan (MI) | ✅ | ✅ | REST/JSON API for transactions + HTMX session search for entities (MiTN); committee person_id = min cfr_com_id per (candidate_name, office, district) |
| Minnesota (MN) | ✅ | ✅ | Bulk CSVs + viewer API for entities (WAF blocks datacenter IPs for entity POSTs); contributor_state inferred from ZIP prefix |
| Mississippi (MS) | ✅ | ✅ | Done, but **no 2024–2026 data** — MS doesn't mandate e-filing until 2027, so recent paper filings aren't yet digitized (see [Notes](#notes-on-specific-states)). Playwright required (WAF blocks non-browser traffic); GUID-based filer IDs; candidate↔committee linking via name-token + office-tiebreak heuristic |
| Montana (MT) | 🚧 | 🚧 | In development — source fully documented (`docs/states/montana.md`, reverse-engineered CERS AJAX API), but the scraper/parser are not yet implemented in the pipeline |
| New Hampshire (NH) | ✅ | ✅ | CSV export API (CFS, `cfsapi.sos.nh.gov`) via curl_cffi (Akamai edge blocks plain `requests`' TLS); 2016–present. No entity roster in source — candidates/committees reconstructed from transaction files, so office/district/party are unavailable |
| Ohio (OH) | ✅ | ✅ | Bulk CSV files via curl_cffi (browser TLS impersonation); validated end-to-end for the Candidate Committee group — PAC/Party groups not yet sampled |
| Oregon (OR) | ✅ | ✅ | ORESTAR; Playwright required (F5 Bot Defense blocks plain HTTP). Two feeds — transactions (1990–present) and an entities registry (1989–present) — joined for office/party/committee_type; `committee` person_id since Committee Id resets each election cycle. ~2M contribution / 966K expenditure rows |
| Pennsylvania (PA) | ✅ | ✅ | Plain HTTP zip-per-year download (2000–present, ~25M contribution rows); no shared filer ID between a candidate and their own money committee in source data — hand-verified override table links the largest statewide committees, smaller ones remain unlinked |
| Virginia (VA) | ✅ | ✅ | Dept. of Elections bulk CSV directory (`apps.elections.virginia.gov/SBE_CSV/CF/`), plain HTTP listing; two eras (yearly 1999–2011, monthly 2012–present with a `Report.csv` cover sheet). Office/party/jurisdiction carried inline from 2012+; `committee` person_id from CommitteeCode. QA/smoke-test filings (`XXXXX_*`, "Test PAC - Ignore", etc.) are suppressed in the parser. ~5.6M contribution rows |
| Washington (WA) | ✅ | ✅ | Socrata (SODA) API on `data.wa.gov` — PDC data across 4 datasets (contributions/expenditures/debt/loans), split by year; pure HTTP, no Playwright. Filer office/party/jurisdiction carried inline per row, so no separate registry needed (~6.3M contribution rows) |
| Wisconsin (WI) | ✅ | ✅ | Sunshine (`campaignfinance.wi.gov`) CSV download endpoints; pure HTTP, no Playwright. **Every download is silently truncated at 99,999 rows**, so the ~13.1M transactions are pulled in adaptive date windows (month → halves → day → amount bands) that are row-counted and re-split on any capped response. One combined contribution/disbursement feed routed by `Transaction Type`. Office/district come from transactions (absent from the registrant list); see [docs/states/wisconsin.md](docs/states/wisconsin.md) |

**Key:** ✅ Done &nbsp; 🚧 In development &nbsp; ⚠️ Partial / known issues &nbsp; ❌ Broken

---

### Notes on specific states

**Mississippi — the 2024–2026 data gap.** Mississippi is fully implemented and validated, but if you query it you'll notice contributions and expenditures cut off cleanly after 2023, with recent years all but empty. This is **not** a scraper bug. It was investigated by querying the live source API directly with explicit date ranges, which returns the same near-empty result — ruling out silent truncation. The root cause is state law: Mississippi does not require electronic filing until **January 1, 2027** ([HB1334, 2025 Regular Session](https://legiscan.com/MS/bill/HB1334/2025)). Until then, candidates and committees may file on paper, and those filings aren't reliably transcribed into the searchable system this pipeline reads from. The window should backfill naturally once mandatory e-filing begins; re-scrapes after that date should be checked for a sudden jump in 2024–2026 coverage. Full detail in [docs/states/mississippi.md](docs/states/mississippi.md).

**Kansas and Montana — in development.** These are not production-ready and their output (if any) should be treated as provisional:

- **Kansas** has a working original scraper/parser (`kansas.py`) but is mid-rewrite — `kansas_v2.py` is the in-progress replacement.
- **Montana** has its source fully reverse-engineered and documented (`docs/states/montana.md`), but the scraper and parser are not yet wired into the pipeline.

