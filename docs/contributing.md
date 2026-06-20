# Contributing: Adding a New State

This guide covers everything needed to add a new state to the pipeline — from registration through a finished, validated scraper and parser. Once both are in place, the orchestrator handles validation, tabulation, aggregation, and logging automatically.

For source code internals (schema, person IDs, aliases, logging API), see [docs/pipeline.md](pipeline.md).

---

## Contents

1. [What You're Responsible For](#1-what-youre-responsible-for)
2. [Register the State](#2-register-the-state)
3. [Add Dependencies](#3-add-dependencies)
4. [Write the Scraper](#4-write-the-scraper)
5. [Write the Parser](#5-write-the-parser)
6. [Person IDs](#6-person-ids)
7. [Add Alias Mappings](#7-add-alias-mappings)
8. [CLI](#8-cli)
9. [Test Your Work](#9-test-your-work)
10. [Documentation](#10-documentation)

---

## 1. What You're Responsible For

You write:
- `src/pipeline/scrapers/{state}.py` — downloads raw files from the state's disclosure site
- `src/pipeline/parsers/{state}.py` — normalizes those files into canonical CSVs

The pipeline handles everything after that: validation, loading into DuckDB, merging into the aggregate database, logging, and HTML reports. Your scraper and parser just need to meet the contracts described below.

State names throughout are lowercase (e.g. `arkansas`), matching the filename and `data/` directory. The two-letter abbreviation (e.g. `AR`) is used in the `state` column of every output row.

---

## 2. Register the State

### states.csv

Add a row to `src/aliases/states.csv`:

```
abbr,name,fips
AR,arkansas,05
```

The `fips` column is the two-digit FIPS code (zero-padded) — look it up at [census.gov](https://www.census.gov/library/reference/code-lists/ansi.html) or from any existing row in the file. It's used to build globally unique `person_id` values across states.

This is the only step needed to make the orchestrator recognize the abbreviation. After this, `python3 src/main.py update AR` will know where to look for the scraper and parser.

### Data directory

The data directory is created automatically by the scraper on first run (`RAW_DIR.mkdir(parents=True, exist_ok=True)`), but you can also create it manually:

```
data/{State}/raw/
data/{State}/cleaned/
```

Note the capitalized `{State}` in the directory name (e.g. `data/Arkansas/`) — this matches the convention used by the existing states and is what `tabulate.py` and `aggregate.py` look for.

---

## 3. Add Dependencies

If your scraper or parser needs a library not already in `requirements.txt`, add it there. Check what's already installed before adding — `requests`, `playwright`, `duckdb`, `tqdm`, and `beautifulsoup4` are all available.

If your scraper uses Playwright, note that it requires a separate install step that is already documented in the README:

```bash
playwright install
```

---

---

## 4. Write the Scraper

The patterns below are conventions, not strict rules — function matters more than form. How you structure path resolution, how you name helpers, whether `run()` takes extra arguments for your state's specific needs — all of that can vary. What the pipeline requires is that a `run()` entry point exists, that the manifest is respected, and that the output lands in the right place with the right structure. Everything else is guidance.

### Checklist

- [ ] `run()` entry point with `force`, `entities`, `transactions` keyword args
- [ ] Writes raw files to `data/{State}/raw/`
- [ ] Manifest respected — already-downloaded files skipped; `--force` clears relevant entries
- [ ] Current-year files always re-fetched regardless of manifest
- [ ] CLI block using the standard flag set (see [Section 8](#8-cli))
- [ ] `KeyboardInterrupt` and `Exception` both caught and re-raised
- [ ] `scrape_started` emitted at the top of `run()`
- [ ] `scrape_completed` emitted in all exit paths (success, interrupt, error)
- [ ] `file_download_*` or `page_scrape_*` events used correctly for the acquisition pattern
- [ ] Module docstring describing the data source and any caveats

### File location

```
src/pipeline/scrapers/{state}.py
```

Where `{state}` is the lowercase state name (e.g. `arkansas.py`).

### Standard boilerplate

Every scraper starts with the same structure. Copy this and fill in the state-specific parts:

```python
"""
scrapers/{state}.py — Download {State} campaign finance data.

{Brief description of the source — website name, download mechanism (bulk
CSV, API, Playwright, etc.), and any special requirements or caveats, e.g.:
  "Requires Playwright — WAF blocks datacenter IPs, must run locally."
  "Uses verify=False — SSL cert triggers urllib3 warnings, suppressed below."
  "API returns UTF-16 encoded responses — decoded before writing."}
"""

import csv
import sys
import time
from pathlib import Path

# add any state-specific imports here (requests, playwright, tqdm, etc.)

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "{State}" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "{State}" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [...]   # define columns that make sense for this state's source

# ========================= state-specific constants ===================
# URLs, year ranges, API endpoints, type codes, etc.
```

A few notes on the fixed parts:

- The `PROJECT_ROOT = Path(__file__).resolve().parents[3]` line is always exactly this — three levels up from the scraper file puts you at the project root.
- `sys.path.insert(0, str(PROJECT_ROOT))` must come before the logger import so Python can find `src/`.
- Use the **capitalized** state name in the data path (e.g. `"Arkansas"`, `"Alabama"`) — this is what `tabulate.py` and `aggregate.py` look for.
- `MANIFEST_COLS` varies by state. At minimum it should include `filename` and enough context to answer "have I already downloaded this?" (commonly `year`, `relation_type`, or similar).

### The manifest

The manifest is a CSV that tracks which files have already been downloaded, allowing incremental updates — re-running the scraper skips files that are already present and current rather than re-downloading everything.

At minimum your manifest should track enough to answer "have I already downloaded this file?" The existing states use columns like `relation_type`, `year`, `filename`, and `row_count`, but the exact structure can vary depending on how the source organizes its data (by year, by file type, by a unique ID, etc.).

The key behaviors to implement:

- **Normal run:** check the manifest before downloading each file; skip if already done. Files associated with the current year should always be re-fetched even on a normal run — end-of-year files are updated in place by most sources, so a manifest hit from earlier in the year may be stale.
- **`--force` run:** clear the relevant manifest entries before downloading
- **After a successful download:** append or upsert an entry to the manifest

### The `run()` function

The scraper's entry point is a `run()` function. Its signature should accept at minimum `force`, `entities`, and `transactions` as boolean keyword arguments:

```python
def run(force: bool = False, entities: bool = False, transactions: bool = False):
    ...
```

These correspond to the pipeline commands (`update`, `rescrape`, `update-entities`, etc.) that `orc.py` translates into scraper flags. If your state's source doesn't distinguish between entities and transactions, you can ignore those flags and treat everything as a single scope — just make sure `--force` still forces a re-download.

### Logging

Get a logger at the top of `run()`:

```python
log = get_logger("{state}", "scrape")
t0  = time.perf_counter()
log._emit("scrape_started", force=force, entities=entities, transactions=transactions)
```

The logging API distinguishes between two fundamentally different acquisition patterns. Use the right one for your source.

**Downloading** — fetching a bulk file from a URL or API in a single request (most states). Use the `file_download_*` methods:

```python
# Before issuing the request — shows up immediately so long fetches are visible
log.file_download_start(filename="contributions_2024.csv")

# After a successful download
log.file_download_ok(filename="contributions_2024.csv",
                     bytes=path.stat().st_size,
                     rows=row_count,
                     duration_s=round(time.perf_counter() - t_file, 2))

# On failure
log.file_download_error(filename="contributions_2024.csv", error=str(e))

# When skipping an already-downloaded file (manifest hit)
log.file_download_skip(filename="contributions_2024.csv")
```

**Scraping** — hitting individual pages or IDs one at a time (detail pages, paginated APIs without a bulk export). Use the `page_scrape_*` methods. These are designed to work alongside a progress bar — per-page console output would be too noisy, so individual hits log to JSONL only and the summary fires once at the end:

```python
# Per-page error (also prints to console as a warning)
log.page_scrape_error(entity="committee", page_id=page_id, error=str(e))

# Once the full sweep is done — one summary event covering all pages
log.page_scrape_complete(filename=str(out_path), rows=total_rows,
                         duration_s=elapsed, ok=ok, err=err)
```

If your source mixes both patterns (e.g. bulk CSV export for transactions, individual detail pages for committee metadata), use the download methods for the bulk files and the scrape methods for the page sweep.

Emit `scrape_completed` at the end in all exit paths (see error handling below):

```python
log._emit("scrape_completed", status="completed", duration_s=duration,
          files_ok=files_ok, files_err=files_err)
```

### Error handling

Wrap the body of `run()` in a try/except block. `KeyboardInterrupt` and general `Exception` must be handled separately:

```python
try:
    # ... all download logic ...
    duration = round(time.perf_counter() - t0, 1)
    log._emit("scrape_completed", status="completed", duration_s=duration,
              files_ok=files_ok, files_err=files_err)

except KeyboardInterrupt:
    log._emit("scrape_completed", status="interrupted",
              duration_s=round(time.perf_counter() - t0, 1),
              files_ok=files_ok, files_err=files_err)
    raise

except Exception as e:
    log._emit("scrape_completed", status="error",
              duration_s=round(time.perf_counter() - t0, 1),
              files_ok=files_ok, files_err=files_err,
              error_type=type(e).__name__, error=str(e))
    raise
```

Both exception paths must re-raise — `orc.py` relies on a non-zero exit code from the CLI to mark the state as failed. The `except Exception` block logs the error to JSONL before re-raising so the run record includes the failure reason even in daemon mode.

Individual file failures inside the download loop are a different matter — catch those, log with `file_download_error`, increment `files_err`, and continue to the next file. Only truly unrecoverable errors (lost connection, unexpected response format, etc.) should bubble up to the top-level handler.

### Tips

Use `tqdm` for page sweeps. If your scraper hits hundreds or thousands of individual pages or IDs, a progress bar is much cleaner than per-page print statements. `tqdm` is already available. Use `logging_redirect_tqdm` to keep log output from colliding with the bar. If a manifest from a previous run exists, its row count makes a decent `total=` estimate — it won't be exact on the first run or after `--force`, but it gives tqdm enough to show a meaningful ETA on incremental runs:

```python
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

with logging_redirect_tqdm(loggers=[log._log]):
    with tqdm(desc="  committees", unit="id", dynamic_ncols=True) as bar:
        for page_id in id_range:
            # ... fetch and parse page ...
            bar.set_postfix_str(some_label[:40], refresh=False)
            bar.update(1)
```

**Consecutive-blank cutoff for ID sweeps.** If you're probing a numeric ID space (e.g. `/view?id=1234`), don't try to determine the max ID upfront — just stop after N consecutive IDs that return no data. Alaska uses 2000–2500 depending on the entity type. Pick a value that's safely above the expected gap between valid IDs.

**Incremental ID sweeps.** For ID-based scrapes, track the lowest ID seen from the current year and use that minus a cushion as your floor on the next run, rather than re-probing from 0. This makes incremental runs fast even as the ID space grows. See Alaska's `_gr_sweep_floor` for an example.

**Rate limiting.** Add a small sleep between requests (`time.sleep(0.2)`) for page scrapes to avoid hammering the source server. Increase it on retry after a failure or rate-limit response. For bulk file downloads a sleep is usually not needed.

**WAF / SSL issues.** Some states block datacenter IPs (use Playwright from a local machine) or have expired/self-signed SSL certs (pass `verify=False` to `requests` and suppress `urllib3` warnings with `urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)`). Document whichever applies in the module docstring.

**Encoding.** Some state APIs (particularly .NET-based sites) return UTF-16 encoded responses. Check `resp.content[:2]` for the BOM (`\xff\xfe` or `\xfe\xff`) and decode accordingly before writing to disk.

---

## 5. Write the Parser

Same principle as the scraper — the boilerplate below is a starting point, not a template to copy exactly. As long as your parser reads from `raw/`, writes the four (or five) canonical CSVs to `cleaned/`, and exposes a `run()` entry point, the internals are yours to structure however makes sense for that state's data.

### Checklist

- [ ] `run()` entry point
- [ ] Reads raw files from `data/{State}/raw/`
- [ ] Writes four (or five) canonical CSVs to `data/{State}/cleaned/`
- [ ] `raw_file` and `row_num` populated on every contributions and expenditures row
- [ ] `assign_person_ids` and `assign_committee_person_ids` called after file handles are closed
- [ ] File handles closed in `finally` block
- [ ] `KeyboardInterrupt` and `Exception` both caught and re-raised
- [ ] `parse_started` emitted at the top of `run()`
- [ ] `parse_completed` emitted in all exit paths (success, interrupt, error)
- [ ] `file_parsed` emitted for each source file and each output file
- [ ] Module docstring describing the raw files consumed and any data quality notes
- [ ] Passes validator tier 1 with no hard failures
- [ ] Alias mappings added to `src/aliases/` for contributor types, transaction categories, expenditure categories, and committee types
- [ ] `docs/states/{state}.md` filled out

### File location

```
src/pipeline/parsers/{state}.py
```

### Imports and path setup

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "{State}" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "{State}" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "AR"   # two-letter abbreviation — written into every output row
```

### Output files

Every parser writes to the same five output files in `data/{State}/cleaned/`. Use `.csv.gz` (gzip-compressed) unless there's a specific reason not to:

```
contributions.csv.gz
expenditures.csv.gz
candidates.csv.gz
committees.csv.gz
loans_debts.csv.gz    ← write an empty file if the source has no loan data
```

Open them with a helper that sets the correct `DictWriter` options:

```python
import gzip, csv

def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w

cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
```

`extrasaction="ignore"` drops extra keys silently. `restval=""` fills missing keys with empty strings. You don't need to populate every field in every row.

### Required helper functions

Every parser should define at minimum these three helpers. Exact implementations may vary by source format but the behavior should be consistent:

```python
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()

def parse_amount(val: str) -> str:
    """Parse a dollar amount to a plain numeric string. Returns '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]   # parentheses = negative
    try:
        float(v)
        return v
    except ValueError:
        return ""

def parse_date(val: str) -> str:
    """Normalize a date string to YYYY-MM-DD. Returns '' on failure or implausible year."""
    from datetime import datetime, date
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > date.today().year + 2:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""
```

Use `utils.clean_name(val)` for any name field that will be used in a join (candidate names, committee names) — it uppercases and collapses whitespace consistently across all states. Use your local `clean()` for fields that don't participate in joins.

### Required fields per table

Every row must include at minimum:

| Table | Required fields |
|---|---|
| contributions | `state`, `committee_name`, `amount`, `date`, `raw_file`, `row_num` |
| expenditures | `state`, `committee_name`, `amount`, `date`, `raw_file`, `row_num` |
| candidates | `state`, `state_filer_id`, `candidate_name` |
| committees | `state`, `state_filer_id` |

`raw_file` should be the source filename (e.g. `"contributions_2024.csv"`). `row_num` should be the 1-based row index within that file — use `enumerate(reader, start=2)` to account for the header row. These two fields together allow any row in the database to be traced back to the exact line in the original download.

### The `run()` function

The parser's entry point is a `run()` function with no arguments:

```python
def run():
    log = get_logger("{state}", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        # ... open remaining writers ...
        file_handles = [cont_fh, ...]

        # ... parse raw files, write rows ...

        # Close handles BEFORE person-ID assignment (see section 6)
        for fh in file_handles:
            fh.close()
        file_handles = []

        # Assign person IDs
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        # Log output file stats
        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        # ... repeat for other output files ...

        duration = round(time.perf_counter() - t0, 1)
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass
```

The `finally` block ensures output file handles are always closed even on an unhandled exception, preventing truncated `.gz` files.

### Logging

Log each source file as it's parsed:

```python
log.file_parsed(path.name, "contributions", row_count,
                duration_s=round(time.perf_counter() - ft, 2),
                bytes=path.stat().st_size)
```

If you load a registry or lookup table for enrichment (e.g. a candidates CSV joined against transactions):

```python
log.registry_loaded("candidates.csv", entries=len(registry), relation="committees")
```

Log output files at the end with `role="output"`:

```python
log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                role="output", bytes=_bytes("contributions.csv.gz"))
```

### Tips

**Use `tqdm` for large files.** If a state has multi-million-row transaction files, a progress bar helps confirm the parser is still running during a long parse. Wrap the `csv.DictReader` loop:

```python
from tqdm import tqdm

with open(path, newline="", encoding="utf-8", errors="replace") as f:
    reader = csv.DictReader(f)
    for row_num, row in enumerate(tqdm(reader, desc=f"  {path.name}", unit="row",
                                       dynamic_ncols=True), start=2):
        # ... process row ...
```

**Deduplication.** Some states include amended filings alongside their originals, or produce duplicate rows across overlapping exports. Build a `seen` dict keyed on a meaningful tuple (contributor, amount, date, committee) and keep the most recent version — typically identified by a result number, filing ID, or timestamp. Write from `seen.values()` at the end of each file rather than writing row by row.

**Registry pattern for enrichment.** When transactions don't include full entity details (committee type, treasurer, city), load the entities file into a dict keyed by a normalized name or ID, then look up each transaction row as you parse it. This is the same pattern used by Alabama and Alaska. Log the registry load with `log.registry_loaded(...)` and the match results with `log.enrichment_summary(...)`.

**Multi-file sources.** Many states export one file per year. Use a glob pattern to iterate all matching files in order:

```python
def raw_files(pattern: str) -> list[Path]:
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )

for path in raw_files("contributions_*.csv"):
    # ... parse each file ...
```

**`csv.field_size_limit`.** Some state files embed long text fields that exceed Python's default CSV field size limit of ~131 KB. Set this at the top of the file to avoid `_csv.Error: field larger than field limit`:

```python
import csv, sys
csv.field_size_limit(sys.maxsize)
```

---

## 6. Person IDs

Call these two functions at the end of `run()`, after all output file handles are closed and before logging final stats. They read and rewrite `candidates.csv.gz` and `committees.csv.gz` in place.

```python
utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                  CLEAN_DIR / "candidates.csv.gz")
```

### Choosing an `id_model`

**`"person"`** — the source assigns a stable person-level ID that persists across election cycles and committee registrations (e.g. Arkansas, Colorado). Use `state_filer_id` directly as the person key. Set `state_filer_id` to this ID in your candidates rows.

**`"committee"`** — the source assigns IDs per committee registration, so the same candidate gets a different ID each cycle (e.g. Alabama, Arizona, California). `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(state_filer_id)` across all registrations for that person. Set `state_filer_id` to the committee's registration ID in your candidates rows.

**`"name_hash"`** — the source has no numeric ID at all (e.g. Alaska). `assign_person_ids` derives a stable 9-digit integer from `MD5(state + normalized_name)`. The same name always produces the same number across runs.

If you're unsure which to use: if filers re-register per cycle with new IDs, use `"committee"`. If the same ID follows a person across cycles, use `"person"`. If there are no IDs at all, use `"name_hash"`.

For matching to work well in `assign_committee_person_ids`, the `candidate_name` field in your committees rows should match the `candidate_name` in your candidates rows as closely as possible. The function handles minor variations (middle initials, nickname expansion) but not fundamentally different name formats like `"SMITH, JOHN"` vs `"JOHN SMITH"`.

---

## 7. Add Alias Mappings

Once your parser is working, add normalization mappings for your state to the alias CSVs in `src/aliases/`. These are applied at aggregate time and aren't required for the parser itself to work, but they're needed for the aggregate database to have clean, consistent values across states.

**`contributor_types.csv`** — map your state's raw `contributor_type` values to canonical labels (`Individual`, `Organization`, `PAC`, `Candidate Committee`, etc.). Leave `canonical` blank to suppress ambiguous or inapplicable values.

**`transaction_categories.csv`** — map your state's raw `transaction_type` values to broad categories (`Monetary`, `In-Kind`, `Refund`, `Other`). Unknown values should be left unmapped rather than guessed — a `NULL` category in the aggregate is better than a wrong one.

**`expenditure_categories.csv`** — same pattern for expenditure transaction types (`Monetary`, `Contribution`, `Independent Expenditure`, etc.).

**`committee_types.csv`** — map your state's raw committee type strings to canonical labels (`Candidate Committee`, `PAC`, `Party Committee`, `Other`).

Each file uses the format `state,raw,canonical`. Lines beginning with `#` are treated as comments — use them to annotate non-obvious decisions. See the existing Alabama and Alaska entries as examples.

---

## 8. CLI

Every scraper and parser must be runnable directly from the command line. Exit codes are what `orc.py` reads to decide whether to proceed to the next stage: 0 = success, 1 = failure, 130 = interrupted.

### Flag taxonomy

Scraper flags are divided into two axes. Implement whichever your source actually supports — states that can't filter by year or split by data type just ignore the irrelevant flags.

**Vertical scope** (controls which time period is downloaded — mutually exclusive):

| Flag | Behavior |
|---|---|
| *(no flag)* | Incremental — fill manifest gaps, always refresh current year |
| `--start-year YYYY` | Wipe manifest entries for years ≥ YYYY and re-download them |
| `--end-year YYYY` | Wipe manifest entries for years ≤ YYYY and re-download them (combine with `--start-year` for a range) |
| `--force` | Wipe all manifest entries in scope and re-download everything |

`--force` and `--start-year` are mutually exclusive. `--force` and `--end-year` must be validated manually (argparse can't enforce this in a group when `--end-year` stands alone). `--end-year` must not exceed the current year.

Year flags apply only to year-based downloads. States whose source is a single bulk file (e.g. California) or uses opaque IDs (e.g. Alabama) do not implement year flags.

**Horizontal scope** (controls which data types are downloaded — additive, not exclusive):

| Flag | Scope |
|---|---|
| *(no flag)* | Everything |
| `--transactions` | Contributions + expenditures |
| `--entities` | Committees + candidates |
| `--contributions` | Contributions only (implies transactions) |
| `--expenditures` | Expenditures only (implies transactions) |
| `--candidates` | Candidates only (implies entities) |
| `--committees` | Committees only (implies entities) |

Second-level flags (`--contributions`, `--expenditures`, `--candidates`, `--committees`) are additive — combining them just unions their scopes. Only implement them if the source organizes data in a way that allows the split. Entity sources that return everything in a single API call (e.g. Arkansas) can still support `--candidates` / `--committees` by filtering which output files get written.

### `run()` signature

The standard `run()` signature accepts all flags even if the state ignores some of them:

```python
def run(
    force: bool = False,
    entities: bool = False,
    transactions: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
    contributions: bool = False,
    expenditures: bool = False,
    candidates: bool = False,
    committees: bool = False,
):
```

States that don't support a flag simply don't use the corresponding parameter. `orc.py` always passes the full set.

### Scraper CLI block

```python
if __name__ == "__main__":
    import argparse
    from datetime import datetime

    ap = argparse.ArgumentParser(
        description="Download {State} campaign finance data."
    )

    # Vertical — mutually exclusive
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all years in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); wipes manifest for range")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")

    # Horizontal — top level
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities only (committees, candidates)")

    # Horizontal — second level (only add flags the source supports)
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true", help="candidates only")
    ap.add_argument("--committees",    action="store_true", help="committees only")

    args, _ = ap.parse_known_args()   # parse_known_args — orc may forward unknown flags

    cy = datetime.today().year
    if args.end_year:
        if args.end_year > cy:
            ap.error(f"--end-year cannot exceed current year ({cy})")
        if args.start_year and args.start_year > args.end_year:
            ap.error("--start-year cannot be greater than --end-year")
    if args.force and args.end_year:
        ap.error("--force cannot be combined with --end-year")

    try:
        run(
            force=args.force,
            entities=args.entities,
            transactions=args.transactions,
            start_year=args.start_year,
            end_year=args.end_year,
            contributions=args.contributions,
            expenditures=args.expenditures,
            candidates=args.candidates,
            committees=args.committees,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
```

Use `parse_known_args` rather than `parse_args` — `orc.py` may forward flags the scraper doesn't define, and `parse_args` would error on unknown arguments.

### Parser CLI block

Most parsers take no arguments — their scope is determined entirely by what raw files exist on disk:

```python
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
```

For states with very large files where parsing individual stages is useful (e.g. California), parsers can accept the same horizontal scope flags as scrapers (`--entities`, `--transactions`, `--contributions`, `--expenditures`). Use `parse_known_args` so orc-forwarded flags don't error:

```python
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Parse {State} campaign finance data.")
    ap.add_argument("--entities",      action="store_true")
    ap.add_argument("--transactions",  action="store_true")
    ap.add_argument("--contributions", action="store_true")
    ap.add_argument("--expenditures",  action="store_true")
    args, _ = ap.parse_known_args()
    try:
        run(entities=args.entities, transactions=args.transactions,
            contributions=args.contributions, expenditures=args.expenditures)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
```

### Manifest clearing for year flags

When `--start-year` or `--end-year` is active, wipe the manifest entries for the in-range years before loading `done`. Without this, already-downloaded files are skipped by the file-existence fallback even though the manifest was cleared.

```python
elif start_year is not None or end_year is not None:
    def _outside_range(r: dict) -> bool:
        try:
            yr = int(r["year"])
        except (ValueError, KeyError):
            return True  # non-year entries always kept
        if start_year is not None and yr < start_year:
            return True
        if end_year is not None and yr > end_year:
            return True
        return False     # within range — wipe

    strip_manifest(_outside_range)
```

Also suppress the file-existence fallback inside the year loop when a range is active, so the manifest is the sole source of truth:

```python
year_range_active = start_year is not None or end_year is not None
already_done = key in done or (
    not year_range_active
    and expected_file.exists()
    and expected_file.stat().st_size > 0
)
```

When a year range is explicitly set, re-download all in-range years even if they're already in the manifest — the user is explicitly asking for a refresh. The pattern used across states:

```python
year_range_explicit = start_year is not None or end_year is not None

# In the download loop:
if key in done and year != current_year_str and not year_range_explicit:
    log.file_download_skip(filename=filename)
    continue
```

This ensures that `--start-year 2020 --end-year 2022` re-fetches 2020–2022 regardless of manifest state, while a plain incremental run still skips completed years.

---

## 9. Test Your Work

Run each component individually before running through the full orchestrator:

```bash
# Scraper
python3 src/pipeline/scrapers/{state}.py

# Parser
python3 src/pipeline/parsers/{state}.py

# Validator — checks cleaned CSVs
python3 tests/validate.py {state}

# Spot-check queries — run after tabulate
python3 tests/test_queries.py {state}
```

Once both pass individually, run the full pipeline:

```bash
python3 src/main.py sync {AB}
```

Check `logs/prod/{run_id}/report.html` for a summary. A few things to verify before considering a state done:

- Validator passes tier 1 (exit code 0) with no schema failures
- Row counts look plausible for the state
- `test_queries.py` output shows recognizable names and reasonable dollar amounts
- Every `contributions` and `expenditures` row has a non-empty `raw_file` and `row_num`
- Candidates and committees have unique `person_id` populated (a quick DuckDB query against the `.db` file confirms this)

## 10. Documentation

### Comments and docstrings

Good comments make it significantly easier for someone new to pick up a state and understand what's happening and why — especially since every state's source is different.

### Module docstring

Every scraper and parser must have a module-level docstring at the top of the file. This is the first thing a contributor reads, so it should answer: what is this file doing, where does the data come from, and are there any caveats?

For scrapers, the docstring should describe the data source, how files are acquired (bulk download, API, Playwright page sweep, etc.), and any non-obvious requirements:

```python
"""
scrapers/arkansas.py — Download Arkansas campaign finance data.

Bulk CSV exports from the Arkansas Ethics Commission ORCA system.
Files are available by year and transaction type at:
  https://www.arkansasethics.com/

No authentication required. All files are plain CSV with UTF-8 encoding.
Downloads are tracked in manifest.csv — re-running skips already-fetched years.
"""
```

For parsers, describe what raw files are consumed, what the output tables represent, and any significant data quality issues or workarounds:

```python
"""
parsers/arkansas.py — Parse Arkansas campaign finance data.

Reads bulk CSV exports from data/Arkansas/raw/ and writes normalized output
to data/Arkansas/cleaned/.

Notes:
  - Contribution and expenditure files include both itemized and non-itemized
    rows; non-itemized rows are written with amount=0 and flagged in notes.
  - Candidate IDs are person-level and stable across cycles (id_model="person").
  - Some committee names in the transaction files don't match the committee
    registry exactly — the registry lookup falls back to a normalized name match.
"""
```

### Section header style

Use `==` banners for top-level section separators inside scrapers and parsers. The banner should be 72 characters wide (including `# `), with the label centered and padded evenly on both sides:

```python
# ============================== constants ==============================
# ========================= state-specific constants ===================
# ============================= registry ===============================
```

Use this style consistently — don't mix in solid-dash lines (`# ------`) or unbalanced padding. Subsections within a block (e.g. a comment label above a few lines) are plain inline comments, not banners.

### Inline comments

Comment the *why*, not the *what*. Avoid restating what the code clearly does:

```python
# Bad — this just repeats the code
rows.sort(key=lambda r: r["date"])

# Good — explains why the sort is necessary
# Sort ascending so the earliest state_filer_id wins in deduplication below
rows.sort(key=lambda r: r["date"])
```

Non-obvious decisions deserve a comment. If you're doing something that looks wrong but is intentional — a `verify=False`, a hardcoded year floor, a magic number, a deliberately missing field — explain it:

```python
# Suppress urllib3 warnings — Arkansas uses a self-signed cert on their download server
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Start sweep from 2003 — ORCA system only goes back to 2003 even though earlier
# records exist in paper form; probing before that just wastes requests
START_YEAR = 2003
```

### Function docstrings

Helper functions that aren't self-evident should have a short docstring explaining what they return and any edge cases. One-liners are fine:

```python
def committee_key(row: dict) -> str:
    """Normalized committee name for manifest lookups — uppercased, whitespace collapsed."""
    return re.sub(r"\s+", " ", (row.get("committee_name") or "").strip().upper())
```

Functions lifted from a pattern used by another state (e.g. `_gr_sweep_floor` from Alaska, the registry pattern from Alabama) should note the origin so future contributors know where to look for context.

### State markdown file

Each state gets a markdown file at `docs/states/{state}.md` (full lowercase name, e.g. `arkansas.md`). Once your scraper and parser are working, fill it out. The sections are:

- **Overview** — a small table with state name, source URL, access method, and data coverage range
- **Raw Data Structure** — describe the files that land in `data/{State}/raw/` and their fields; this doesn't need to be exhaustive but should cover anything non-obvious
- **Scraper** — short description of how the scraper works, any limitations, and expected runtime
- **Parser** — short description of key transformations and any known limitations; include the `person_id` model used (`person`, `committee`, or `name_hash`) and a brief explanation of why
- **Data Notes** — quirks, gaps, or anomalies in the source data worth flagging (missing fields, enrichment failures, duplicate patterns, etc.)
- **Last Updated** — a small table with the date each component was last touched

See `docs/states/alabama.md` and `docs/states/alaska.md` as reference examples.

