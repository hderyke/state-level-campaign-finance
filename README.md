# State-Level Campaign Finance Pipeline

## Introduction

This project aims to build a unified database of campaign finance disclosures across U.S. states. The pipeline downloads raw data from each state's disclosure website, standardizes it to a common schema, and loads it into both state-level databases and a combined master database—all queryable via SQL and R.

### Goal
Collect and normalize campaign finance data from all 50 states into a single, unified interface for analyzing state-level money in politics.

### Output
- **State-level databases**: Individual SQLite databases for each state with standardized tables
- **State-level CSVs**: Canonical CSV exports of contributions, expenditures, candidates, and committees for each state
- **Combined master database**: A unified SQLite database merging all states' normalized data

---

## Section 1: Running the Pipeline

### Prerequisites
- Python 3.8+
- Required packages: `playwright`, `requests`, `pandas`, `sqlite3`
- Install: `pip install -r requirements.txt`
- Playwright setup: `playwright install`

### Quick Start

**Step 1: Download raw data for a specific state:**
```bash
python src/pipeline/scrapers/alabama.py
python src/pipeline/scrapers/alaska.py
python src/pipeline/scrapers/arizona.py
```
Each downloader writes to `data/{state}/raw/` and updates `data/{state}/manifest.csv` to track progress. Resume interrupted downloads by running the same command again—already-downloaded files are skipped.

**Scraper flags:**
- `--force` — Clear manifest and re-download everything from scratch
- `--update-transactions` — Download only transaction data (contributions/expenditures); skip committee/candidate registry
- `--update-entities` — Download only entity data (committees/candidates); skip transaction data

Example:
```bash
python src/pipeline/scrapers/alabama.py --force              # Full re-download
python src/pipeline/scrapers/alaska.py --update-transactions # Transactions only
python src/pipeline/scrapers/arizona.py --update-entities    # Entities only
```

**Step 2: Parse raw data**
```bash
python src/pipeline/parsers/alabama.py
```
Outputs standardized CSVs: `contributions.csv`, `expenditures.csv`, `candidates.csv`, `committees.csv`. Each state's parser is designed specifically for it's raw data format.

**Step 3: Validate and evaluate cleaned data:**
```bash
python tests/validate.py alabama
```
Generates a validation report in `tests/reports/alabama_latest.json` with warnings and errors.

**Step 4: Build state-level database:**
```bash
python src/pipeline/tabulate.py alabama
```
Creates `data/alabama/cleaned/alabama.db` with normalized tables.

**Step 5: Aggregate into master database:**
```bash
python src/pipeline/aggregate.py
```
Creates `state-level-cf.db` with all states' data in a unified schema.

---

## Source Code Overview

### Scrapers (`src/pipeline/scrapers/`)

Each state has a dedicated downloader that handles that state's unique disclosure website. No manual downloads required. Scrapers respect manifest files to enable resumable downloads—already-downloaded data is skipped.

States currently implemented: Alabama, Alaska, Arizona, Arkansas, California. See individual state documentation for scraper details.

### Parsers (`src/pipeline/parsers/`)

Parsers normalize each state's raw data into a canonical five-table schema:
- `contributions` (income transactions)
- `expenditures` (spending transactions)
- `candidates` (candidate registry)
- `committees` (committee/PAC registry)
- `loans_debts` (outstanding debt)

States currently implemented: Alabama, Alaska, Arizona, Arkansas, California. See individual state documentation for parser details and limitations.

### Shared Infrastructure

#### `src/pipeline/columns.py`
Canonical column definitions for all tables across all states. Ensures consistent schema across the entire pipeline.

```python
CONTRIBUTIONS = [
    'date', 'amount', 'contributor_name', 'contributor_type',
    'committee_name', 'candidate_name', 'state_filer_id', ...
]
```

#### `tests/validate.py`
Multi-tiered validator that checks:
- **Hard failures**: Missing required columns, invalid data types
- **Warnings**: Implausible dates, missing IDs
- **Drift detection**: Schema changes between parsing runs

Reports are saved to `tests/reports/{state}_latest.json`

**⚠️ Known issue**: OOM-kills on files >~100 MB (e.g., Arizona's 303 MB contributions.csv). Solution: rewrite to stream rows instead of loading all into memory.

### Tabulater (`src/pipeline/tabulater.py`)

_In progress_

Converts parsed CSVs into state-level SQLite databases with proper schemas and indexes.

### Aggregater (`src/pipeline/aggregater.py`)

_In progress_

Merges all state databases into a single master database with a unified schema, deduplicating and aligning identifiers across states.

### Main & Control Functions

_In progress_

Top-level orchestrator script that ties all components together:
- Decides which states to process
- Manages pipeline execution order
- Handles error recovery and resumption
- Logs progress and generates reports

---

## Directory Structure

```
state-level campaign finance/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── src/
│   ├── main.py                        # Main orchestrator (in progress)
│   ├── states/                        # Per-state downloaders
│   │   ├── alabama.py
│   │   ├── alaska.py
│   │   ├── arizona.py
│   │   ├── arkansas.py
│   │   ├── california.py
│   │   └── ...
│   └── pipeline/
│       ├── parsers/                   # Per-state parsers
│       │   ├── alabama.py
│       │   ├── alaska.py
│       │   ├── arizona.py
│       │   ├── arkansas.py
│       │   ├── california.py
│       │   └── ...
│       ├── columns.py                 # Canonical schema definitions
│       ├── tabulater.py               # CSV → SQLite (in progress)
│       ├── aggregater.py              # Master database builder (in progress)
│       └── loader.py                  # Load parser output to state DB
├── data/
│   ├── alabama/
│   │   ├── raw/                       # Downloaded raw files
│   │   ├── manifest.csv               # Download tracking
│   │   └── state.db                   # State-level SQLite database
│   ├── alaska/
│   │   └── ...
│   ├── master.db                      # Combined master database
│   └── ...
├── tests/
│   ├── validate.py                    # Data validator
│   └── reports/
│       ├── alabama_latest.json
│       ├── alaska_latest.json
│       └── ...
```



---


## Development Notes

### Adding a New State

1. **Create scraper**: `src/states/{state}.py`
   - Download raw data from the state's disclosure site
   - Populate `data/{state}/raw/` with files
   - Maintain `data/{state}/manifest.csv` for resumable downloads

2. **Create parser**: `src/pipeline/parsers/{state}.py`
   - Read raw files, normalize to five canonical tables
   - Output: `{contributions, expenditures, candidates, committees, loans_debts}.csv`

3. **Test & validate**: Run `tests/validate.py {state}`
   - Check for hard failures, warnings, schema drift

4. **Register**: Add state to main orchestrator for pipeline inclusion

