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

### Cloudflare Data Sync

Sync data to and from Cloudflare R2 without running the pipeline. To grab or upload just the master database:

```bash
python3 src/main.py pull db
python3 src/main.py push db
```

Or pull data for specific states, or push your own:

- `pull <states|all|db>` — download state data or master DB from Cloudflare R2
- `push <states|all|db>` — upload state data or master DB to Cloudflare R2

> **Note:** Cloudflare credentials are required. See [docs/pipeline.md](docs/pipeline.md) for setup details.

### Output

After a pipeline run, data is written to the following locations:

- `data/{State}/raw/` — raw downloaded files
- `data/{State}/cleaned/` — normalized CSVs (contributions, expenditures, candidates, committees, loans_debts)
- `data/{State}/{state}.db` — state-level database
- `data/state-level-cf.db` — master database combining all states

Each run also generates a log and HTML report under `logs/prod/{run_id}/`:

- `log.jsonl` — structured event log for the run
- `report.html` — human-readable summary of the run
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

Runs a tiered validation check on a state's cleaned CSVs, checking for hard failures (missing columns, bad types), warnings (implausible dates, missing IDs), and schema drift between runs. Writes a report to `tests/reports/{state}_latest.json`.

```bash
python3 tests/validate.py alabama
```


### Test Queries

Runs queries against a state's database to evaluate data quality after tabulation.

```bash
python3 tests/test_queries.py alabama
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
3. **Test & validate**: `python3 tests/validate.py {state}`
4. **Register**: Add state to the main orchestrator
5. **Upload**: Push to master db and repo

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

**Key:** ✅ Done &nbsp; ⚠️ Partial / known issues &nbsp; ❌ Broken

