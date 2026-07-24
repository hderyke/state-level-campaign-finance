# Pipeline Internals

Source code documentation for the state-level campaign finance pipeline. For CLI usage, output file locations, and how to run individual components, see the [README](../README.md). For adding a new state scraper or parser, see [contributing.md](contributing.md).

---

## Contents

1. [Architecture](#1-architecture)
2. [Schema](#2-schema)
3. [Orchestration](#3-orchestration)
4. [Pipeline Stages](#4-pipeline-stages)
5. [Validation and Queries](#5-validation-and-queries)
6. [Person IDs and Name Matching](#6-person-ids-and-name-matching)
7. [Aliases](#7-aliases)
8. [S3](#8-s3)
9. [Logging](#9-logging)
10. [Error Handling](#10-error-handling)

---

## 1. Architecture

A full pipeline run flows through five sequential stages for each state, then merges all states into one aggregate database.

```
python3 src/main.py sync AL AK AZ
         │
         ▼
      main.py  ──── parse flags, resolve command
         │
         ▼
       orc.py  ──── build run ID, set CF_RUN_ID env var
         │
         │  for each state:
         ├──▶  scrapers/{state}.py   → data/{State}/raw/
         ├──▶  parsers/{state}.py    → data/{State}/cleaned/*.csv
         ├──▶  pipeline/validate.py  → metadata/{state}_latest.json
         ├──▶  pipeline/tabulate.py  → data/{State}/cleaned/{state}.db
         │
         │  after all states pass:
         └──▶  pipeline/aggregate.py → data/state-level-cf.db


python3 src/main.py push AL   (separate branch — no pipeline stages; run after sync/reparse finishes)
         │
         ▼
      main.py ──▶ s3.py ──▶ S3 bucket
```

Each stage runs as a **subprocess** spawned by `orc.py`. This means a parser crash cannot take down the orchestrator, and all stage output streams to the terminal in real time. The aggregate step only runs if every state in the batch passed validation and tabulation.

---

## 2. Schema

`src/pipeline/columns.py` is the single source of truth for column names and types across the entire pipeline. Every parser imports it and writes its output CSVs against these lists. `tabulate.py` and `aggregate.py` enforce the types when loading into DuckDB.

### Canonical column lists

There are four tables: `contributions`, `expenditures`, `candidates`, and `committees`. The lists in `columns.py` define the exact fieldnames and ordering for the cleaned CSVs. When a parser writes with `csv.DictWriter(..., extrasaction='ignore', restval='')`, extra keys are silently dropped and missing keys become empty strings — so parsers don't need to fill in every field.

Every table carries three provenance columns at a minimum: `state`, `raw_file` (the source filename), and `row_num` (the row index within that file). These allow any row in the database to be traced back to the exact line in the original download.

### COLUMN_TYPES

`COLUMN_TYPES` is a dict mapping every column name to its DuckDB storage type. Both `tabulate.py` and `aggregate.py` import this and pass it as the `types=` argument to `read_csv_auto`, ensuring consistent typing across all states. Without this, all-NULL columns default to `VARCHAR` and amounts can silently stay as strings.

A few type decisions worth noting:

- `amended` is `VARCHAR`, not `BIGINT` — raw values vary by state (`0`/`1`, `Y`/`N`, blank), so it cannot be safely cast at load time.
- `person_id` and `state_filer_id` are `BIGINT` and `VARCHAR` respectively — person IDs are always numeric surrogates, while filer IDs from source systems may have leading zeros or non-numeric formats.
- `date` is stored as DuckDB's native `DATE` type.

### Per-state vs. aggregate schema

The per-state `.db` files produced by `tabulate.py` use the full column lists including `state_filer_id`. The aggregate `state-level-cf.db` produced by `aggregate.py` uses the `_AGG` variants defined at the bottom of `columns.py`, which differ in two ways:

- `state_filer_id` is **dropped** from `candidates` and `committees`. It is inconsistent across states (some use numeric IDs, some use strings, some are committee-level rather than person-level) and its traceability role is covered by `raw_file` + `row_num`. Cross-state joins use `person_id` instead.
- `transaction_category` is **added** to `contributions` and `expenditures` (after `transaction_type`). This is a derived normalization computed at aggregate time from the raw `transaction_type` via the aliases system — it is not present in per-state CSVs or `.db` files.

---

## 3. Orchestration

`src/orc.py` is responsible for coordinating a full pipeline run: resolving state names, dispatching each stage as a subprocess, and deciding whether to proceed to the next stage or abort.

### State registration

States are registered in `src/aliases/states.csv` (columns: `abbr`, `name`, `fips`). `orc.py` reads this at startup into `ABBR_TO_NAME`, a dict mapping two-letter abbreviations to lowercase state names. To add a new state to the orchestrator, add a row to `states.csv` — no code change needed. The orchestrator will then recognize the abbreviation and know where to look for scraper and parser files.

### Run IDs

Every managed run gets a unique `run_id` of the form `{YYYYMMDD_HHMMSS}_{command}[_force]_{states}`, e.g. `20250528_143012_sync_AL-AK` or `20250528_143012_sync_force_AL-AK`. The `_force` suffix is appended when `--force` is active so log filenames are self-describing. This is set as the `CF_RUN_ID` environment variable before any subprocesses are spawned, so it is inherited by every stage. All logging (JSONL events, HTML reports, validation reports) is scoped to this ID. See [Logging](#9-logging) for details.

### Subprocess dispatch

Each stage — scraper, parser, validate, tabulate — is run via `subprocess.run()` rather than a direct function call. This has a few practical consequences:

- A crash in one stage (e.g. an unhandled parser exception) cannot corrupt the orchestrator's state or skip the failure check.
- `stdout` and `stderr` stream to the terminal in real time.
- The exit code is the failure signal. A non-zero exit from any stage causes `orc.py` to abort that state and record it as failed. Subsequent states in the batch still run.

### Pipeline commands

| Command | Stages |
|---|---|
| `sync` | scrape → parse → validate → tabulate → aggregate |
| `reparse` | *(skip scrape)* parse → validate → tabulate → aggregate |

`reparse` is useful when the raw data is already current (e.g. a scrape just finished) and only the parser, alias mappings, or schema has changed.

### Scraper flags

Scraper flags are passed after the state list and forwarded transparently by `orc.py` to each scraper subprocess. All flags are optional — scrapers use `parse_known_args` so unsupported flags are silently ignored.

**Vertical scope** (controls which time period is downloaded — mutually exclusive):

| Flag | Behavior |
|---|---|
| *(none)* | Incremental — fill manifest gaps, always refresh current year |
| `--force` | Wipe manifest entries in scope and re-download everything |
| `--start-year YYYY` | Wipe and re-download years ≥ YYYY |
| `--end-year YYYY` | Wipe and re-download years ≤ YYYY (combine with `--start-year` for a range) |

`--force` and `--start-year` are mutually exclusive. `--end-year` cannot exceed the current calendar year.

**Horizontal scope** (controls which data types are downloaded — additive):

| Flag | Scope |
|---|---|
| *(none)* | Everything |
| `--transactions` | Contributions + expenditures |
| `--entities` | Committees + candidates |
| `--contributions` | Contributions only |
| `--expenditures` | Expenditures only |
| `--candidates` | Candidates only |
| `--committees` | Committees only |

Horizontal flags are additive — stacking them unions their scopes. Not every state supports every flag; unsupported flags are silently ignored. See `docs/contributing.md § CLI` for the full flag contract.

**Example invocations:**

```bash
python3 src/main.py sync AK                               # incremental, all types
python3 src/main.py sync AK --force                       # re-download everything
python3 src/main.py sync AK --start-year 2023             # re-download 2023 onwards
python3 src/main.py sync AK --force --transactions        # force-refresh transactions only
python3 src/main.py sync AK --start-year 2022 --end-year 2024 --contributions
python3 src/main.py reparse AL                            # re-parse without scraping
```

### Aggregate gating

After all states finish, `orc.py` checks whether any failed. Aggregate only runs if every state in the batch passed. If one or more states failed, aggregate is skipped and the operator is notified. This prevents a partial or corrupted state from polluting the master database.

---

## 4. Pipeline Stages

> **Note:** Scrapers and parsers vary significantly across states — each is written specifically for its source system's structure, format, and update mechanism. For state-specific details on how a scraper or parser works, see `docs/states/{state}.md`.

### Tabulate (`src/pipeline/tabulate.py`)

Tabulate reads all available cleaned CSVs for a state and loads them into a per-state DuckDB file (e.g. `data/Alabama/cleaned/alabama.db`).

**Always rebuilds from scratch.** The existing `.db` file is deleted before opening a new connection. This avoids DuckDB page bloat from incremental `OR REPLACE` updates and ensures the database always reflects the current state of the cleaned CSVs exactly.

**Type enforcement.** The `types=` parameter is passed to `read_csv_auto` using the full `COLUMN_TYPES` dict from `columns.py`, serialized as a DuckDB map literal. This means every column gets the correct type on load — amounts become `DOUBLE`, dates become `DATE`, and so on — rather than relying on inference.

**`parallel=false`.** DuckDB's parallel CSV reader can cause lock collisions when multiple tables are being loaded sequentially into the same file. Disabling parallelism avoids spurious errors on this sequential workload with no meaningful performance cost.

Tabulate exits with code 1 if any table fails to load, which causes `orc.py` to record the state as failed and skip aggregate.

### Aggregate (`src/pipeline/aggregate.py`)

Aggregate discovers all tabulated state `.db` files by scanning `data/*/cleaned/*.db`, attaches them all to a single DuckDB connection, and builds `data/state-level-cf.db` by unioning them.

**ATTACH and UNION ALL.** Each state `.db` is attached with a short alias (`s0`, `s1`, ...) rather than the state name, which avoids issues with spaces or reserved words. For each table, aggregate checks which columns actually exist in each state's `.db` (some states may be missing optional columns), casts everything to the canonical type via `COLUMN_TYPES`, and emits a `NULL` with the correct cast for any column not present. The full union across all states is then `CREATE OR REPLACE TABLE ... AS SELECT ... UNION ALL SELECT ...`.

**Normalizations.** Four normalizations are applied at aggregate time using DuckDB `CASE` expressions generated from the aliases CSVs. Raw values are preserved in per-state `.db` files; only the aggregate database carries canonical values:

- `committees.committee_type` → canonical label via `committee_types.csv`
- `contributions.contributor_type` → canonical label via `contributor_types.csv`
- `contributions.transaction_category` → derived from `transaction_type` via `transaction_categories.csv`
- `expenditures.transaction_category` → derived from `transaction_type` via `expenditure_categories.csv`

**Contributor type backfill.** After normalizing `contributor_type`, many contributions still have `NULL` for that field (e.g. Alaska, Arizona do not include contributor type in their transaction exports). Aggregate joins these rows against the `committees` table on `(state, contributor_name = committee_name)` and fills in the committee's normalized `committee_type`. This gives a meaningful contributor type for the large fraction of contributions that come from registered committees.

**Temp file workaround.** The aggregate database is built in `/tmp` and then copied to its final location. Some macOS filesystem mounts (FUSE, network drives) prevent DuckDB from deleting its own WAL file after checkpointing, which causes errors if the database is opened directly at its final path. Building in `/tmp` and copying the finished file sidesteps this restriction entirely.

---

## 5. Validation and Queries

### Validator (`src/pipeline/validate.py`)

The validator runs automatically as part of every pipeline run (between parse and tabulate) and gates tabulation on success. It can also be run manually against any state's cleaned CSVs.

**Two-tier check system.** Checks are divided into tier 1 (hard failures) and tier 2 (warnings).

Tier 1 failures cause `validate.py` to exit with code 1, which causes `orc.py` to skip tabulation for that state. These checks include: required files exist, required columns are present, the table has at least one row, the `state` column has the correct value in ≥99% of rows, required fields (e.g. `amount`, `date`, `committee_name`) are non-empty in ≥99% of rows, amounts are numeric, and dates are valid `YYYY-MM-DD` within a plausible range.

The 99% threshold (`TIER1_PASS_RATE = 0.99`) rather than 100% tolerance is intentional — real state disclosure data contains a small number of malformed rows, and zero-tolerance would cause validation to fail on otherwise good datasets. Failures are reported with counts and percentages so the operator can judge severity.

Tier 2 warnings are printed and saved to the report but do not affect the exit code. These include: `election_year` out of range, `amended` not in `0`/`1`/blank, amounts above $10M (possible data entry errors), unrecognized state codes in `contributor_state`/`payee_state`, malformed ZIP codes, and enrichment fill rates for optional fields.

**Drift detection.** On each run, the validator compares current row counts against the previous run's report (stored at `metadata/{state}_latest.json`). A drop of more than 5% in any table triggers a tier 2 drift warning. This catches accidental data loss from parser regressions.

**Row sampling.** For memory safety, large files are validated on a random sample rather than loading everything into memory. The default sample size is 500,000 rows (tunable via `MAX_SAMPLE_ROWS` at the top of `src/pipeline/validate.py`). Sampling uses reservoir sampling (Algorithm R) so the sample is uniformly random across the full file rather than just the first N rows — important for chronologically ordered files. The total row count is always computed by streaming the full file regardless of sampling. When sampling is active, the terminal output and HTML report note "sampled X of Y rows" for transparency.

**Output.** Each run writes `metadata/{state}_latest.json` with row counts, tier 1 fill rates, tier 2 warnings, enrichment stats, and drift deltas. When running under `orc.py`, the report is also copied to `{state}_validate.json` in the run's directory (`logs/prod/{run_id}/` or `logs/daemon/{run_id}/` — see §9).

### Spot-check queries (`src/pipeline/queries.py`)

A manual spot-check tool run after tabulation to evaluate data quality by eye. Accepts a state name or `all` (which targets `data/state-level-cf.db` instead of a state `.db`).

Runs four queries and prints formatted tables to the terminal:

1. **Top 20 contributors** — total donated and the single committee that received the most from each.
2. **Top 20 recipient candidates** — total contributions received, joined via `person_id` through the candidates table. Uses `DISTINCT ON (state, person_id)` to deduplicate candidates before joining — without this, a candidate with multiple rows (e.g. re-registered across cycles) fans out and inflates totals.
3. **Top 20 non-candidate committees** — total received, excluding any committee whose name matches a known candidate name and excluding committees with a `Candidate`-type `committee_type`.
4. **Top 10 expenditure payees** — total paid out and the single largest client committee for each payee. Filters known junk rows (`unitemized`, `not pertaining`, etc.).

This script does not write any output files and has no effect on the pipeline. It is purely for human review.

---

## 6. Person IDs and Name Matching

A key goal of the pipeline is linking the same real person across multiple committees, election cycles, and states. This is handled by `src/pipeline/utils.py` in two steps: assigning `person_id` to candidates, then propagating it to committees.

### `assign_person_ids` — three strategies

Each state parser calls `assign_person_ids(candidates_path, id_model)` at the end of its run. The function reads the written `candidates.csv`, fills in the `person_id` column, and rewrites the file atomically via a temp file.

The `id_model` argument determines the strategy, chosen based on what the state's source system provides:

**`"person"`** — the source system already assigns a stable person-level ID (e.g. Arkansas, Colorado). `person_id` is set equal to `state_filer_id` directly. One-to-one, no grouping needed.

**`"committee"`** — the source system assigns IDs per committee registration, not per person (e.g. Alabama, Arizona, California). The same candidate may have a different `state_filer_id` for each election cycle or office they run for. To produce a stable person-level key, `assign_person_ids` groups rows by `(state, normalized_name, office, district)` and assigns `person_id = min(state_filer_id)` across all committee registrations in that group. Including `office` and `district` in the group key prevents false merges when two people share a name but run for different seats. Using the minimum ID ensures the same person always gets the same `person_id` regardless of which runs have been processed.

**`"name_hash"`** — no numeric ID exists in the source at all (e.g. Alaska). `person_id` is a stable 9-digit integer derived from `MD5(state + normalized_name)`. The same name always produces the same number across runs, so IDs are stable without needing to track state. Collision probability at state-level candidate counts is negligible.

Name normalization for grouping in all models: uppercase + collapse whitespace. This handles trailing spaces and minor formatting differences, but will not merge `DOUG DUCEY` with `DUCEY, DOUG` — name format must be consistent within a state's own data, which it generally is.

### `assign_committee_person_ids` — three-pass matching

After candidates have `person_id` assigned, parsers call `assign_committee_person_ids(committees_path, candidates_path)` to propagate `person_id` onto committee rows. PACs and other non-candidate committees (blank `candidate_name`) are left with an empty `person_id`.

Matching proceeds in three passes, stopping at the first hit:

1. **Exact normalized name match** — `PETE HIGGINS` → `PETE HIGGINS`. Handles the majority of cases.
2. **First + last token fallback** — strips middle initials and tries matching on just the first and last name tokens (e.g. `PETE B. HIGGINS` → tokens `PETE` and `HIGGINS`). Only applied when exactly one candidate has that first+last combination — if two candidates share first and last name, the match is ambiguous and the committee is left unmatched.
3. **Nickname expansion** — tries expanding the first token through `nicknames.csv` (e.g. `MIKE` → `MICHAEL`) and repeats the first+last lookup. Again only applied when the result is unambiguous.

---

## 7. Aliases

`src/aliases/` contains CSV lookup tables that drive normalization across the pipeline. All files are loaded once at import time into module-level caches in `src/aliases/__init__.py`.

### Files

**`states.csv`** (`abbr`, `name`, `fips`) — maps two-letter abbreviations to lowercase state names and two-digit FIPS codes. Read by `orc.py` to populate `ABBR_TO_NAME`; FIPS codes are used by `utils.py` when building globally unique `person_id` values. Adding a row here is the only step needed to register a new state with the orchestrator.

**`contributor_types.csv`** (`state`, `raw`, `canonical`) — maps raw contributor type strings from each state's data to canonical labels. Used by `aggregate.py` to normalize `contributions.contributor_type`. A blank `canonical` value means the raw value is intentionally suppressed (e.g. Alaska's contributor type field is a registration status, not a donor category, so all its values map to `None`).

**`transaction_categories.csv`** (`state`, `raw`, `canonical`) — maps raw `transaction_type` values to broad categories (`Monetary`, `In-Kind`, `Refund`, `Other`). Used by `aggregate.py` to derive `contributions.transaction_category`. Unknown or ambiguous values map to `None` rather than passing through — `transaction_category` should be clean or absent, not raw.

**`expenditure_categories.csv`** (`state`, `raw`, `canonical`) — same pattern as transaction categories but for expenditures (`Monetary`, `Contribution`, `Independent Expenditure`, etc.).

**`committee_types.csv`** (`state`, `raw`, `canonical`) — maps raw committee type strings to canonical labels (`Candidate Committee`, `PAC`, `Party Committee`, etc.).

**`nicknames.csv`** (`nickname`, `formal`) — maps common nicknames to their formal equivalents for use in name matching. One nickname can map to multiple formals (e.g. `AL` → `ALBERT` and `ALFRED`; `BERT` → `ALBERT` and `ROBERT`).

### Format notes

Lines beginning with `#` in the `state`/`raw`/`canonical` CSVs are treated as comments and skipped. This is used to annotate decisions — e.g. why Alaska's contributor types are all suppressed.

### Adding a mapping

To add a new normalization mapping, append a row to the relevant CSV. The `(state, raw)` key is case-insensitive (the loader uppercases both before storing). Changes take effect the next time `aggregate.py` runs — no code changes needed.

---

## 8. S3

`cloud/s3.py` handles syncing data to and from an S3 bucket, talking to AWS directly via `boto3` — no proxy or intermediary service sits in front of it. It replaces the project's earlier Cloudflare R2 setup (`src/cloudflare.py`, now removed, and the Worker under `worker/campaign-finance-r2/`, which is no longer used but left in place for reference). It lives in `cloud/` rather than under `src/`, since it doesn't talk to state data directly — it's AWS glue sitting between the pipeline and the client-facing API. The aggregate `metadata/manifest.csv` that the downloads page reads is now kept current by `push_db` itself, which regenerates it from the db on every db push (see `cloud/regenerate_manifest.py`); the older S3-triggered `cloud/lambda/manifest_updater/` function that once did this incrementally is superseded.

Push is a **separate, manual step** run after `sync`/`reparse` completes — it is not triggered automatically by `orc.py`. This means every push publishes whatever is currently on disk, including that state's most recent validation results.

### Bucket layout

```
data/{State}/{state}.db
data/{State}/{state}_raw.zip        (zip of data/{State}/raw/)
data/{State}/{state}_clean.zip      (zip of data/{State}/cleaned/, .db excluded)
data/state-level-cf.db

metadata/latest/{State}/manifest.json
metadata/latest/{State}/report.html
metadata/latest/{State}/validate.json
metadata/latest/{State}/queries.txt

metadata/successful/{State}/...     (same four files)
```

`metadata/latest/{State}/` is overwritten on every push, pass or fail. `metadata/successful/{State}/` is only touched when that push's validation passed, so it always reflects the last known-good run for that state — neither directory keeps history beyond the single most recent write.

`manifest.json` is built at push time from `metadata/{state}_latest.json` (always the freshest validation report regardless of run mode) — it carries `last_updated`, `status` (`success`/`failed`), `row_counts`, and tier-1 fill rates. This is what a future `downloads.py` FastAPI router would read to describe available state data without touching the heavier `report.html`/`validate.json` files. `report.html` is sourced from the most recent single-state `logs/prod/{ts}_{sync|reparse}[_force]_{ABBR}/` run directory — if a state has never been run standalone (only as part of a multi-state batch), push skips the report and logs a warning rather than failing.

### Credentials setup

Create a `.env` file in the project root with the following variables:

```
AWS_ACCESS_KEY_ID=<AWS access key>
AWS_SECRET_ACCESS_KEY=<AWS secret key>
AWS_REGION=<e.g. us-east-1>
S3_BUCKET=<bucket name>
```

`main.py` loads this file via `python-dotenv` before any S3 operations run. The variables can also be set directly in the environment (e.g. in a cron job or CI), or supplied via the standard AWS credential chain (shared config file, instance role, etc.) if `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are left unset.

### Delta detection

There's no server-side manifest tracking what's already in the bucket — each push checks live against S3 itself. For every candidate file, `s3.py`:

1. Computes an MD5 of the local file.
2. Calls `head_object` on the target key and reads the `md5` value stashed in that object's metadata from its last upload.
3. Uploads only if the hashes differ (or the object doesn't exist yet), stashing the new MD5 in the object's metadata as part of the `PutObject` call. Matching hashes are skipped (`noop`) and logged as such.

This avoids trusting a local record of "what we think is in the bucket," which could drift from reality after a partial failure or a push from another machine — `head_object` always reflects the bucket's actual current state.

### Push, pull

`push_state(abbr, state_name)` / `pull_state(abbr, state_name)` — sync a single state. Push zips `raw/` and `cleaned/` fresh each time, uploads the three `data/` artifacts plus the four `metadata/` artifacts (to `latest/` and, if the run passed, `successful/`). Pull downloads and unzips the same artifacts back into `data/{State}/`, without deleting anything already on disk.

`push_all` / `pull_all` — iterate over every state with a local `data/` directory (push) or every state registered in `states.csv` (pull), pushing/pulling one at a time, followed by the aggregate db. Both require typing `yes` at a confirmation prompt.

`push_db` / `pull_db` — sync just the aggregate database (`push db` / `pull db`). `push_db` also regenerates `metadata/manifest.csv` from the db (via `cloud/regenerate_manifest.py`) and uploads it right after the db, so the client-facing downloads page and state dropdown — which read `metadata/manifest.csv` — can't drift from what's actually in the db that just went up. This happens on every db push, daemon or manual, and is MD5-idempotent like every other upload (an unchanged manifest is a `noop`). The manifest upload is best-effort: a regeneration or upload failure is logged as a file error and counted in the push summary, but does not abort the db push it rides along with.

---

## 9. Logging

Every pipeline stage writes structured events to a JSONL log file. The logging system lives in `src/reporting/logger.py`.

### CF_RUN_ID

The `CF_RUN_ID` environment variable is the thread that ties an entire pipeline run together. It is set by `orc.py` before any subprocesses are spawned and inherited by every stage. A second variable, `CF_DAEMON`, is set only by `ops/daemon.py`. Together they decide where a run's files land:

- **`CF_RUN_ID` set, `CF_DAEMON` not set** (orc mode — a manual `main.py sync`/`reparse`/`push`/`pull`): all stages write to a single shared `logs/prod/{run_id}/log.jsonl`
- **`CF_RUN_ID` and `CF_DAEMON` both set** (daemon mode — anything run through `ops/daemon.py`, whether cron triggered it or you ran it by hand): same shared-log structure, but under `logs/daemon/{run_id}/log.jsonl` instead, so scheduled runs don't bury one-off manual runs in the same directory
- **Neither set** (dev mode, running a component directly): logs go to `logs/dev/{timestamp}-{state}-{operation}.jsonl`

The bucket decision lives in one place — `src/reporting.logger.run_dir_for(run_id)` — and everything that writes a side-car file into "the run dir" (query output, validation JSON, `report.html`, `emailer.py`'s run-dir lookup) goes through it, so they can't drift out of sync with where a run's `log.jsonl` actually landed.

### Event format

Each log line is a JSON object. Every event has four fixed fields: `ts` (ISO 8601 UTC timestamp), `state` (lowercase state name or null for state-less operations), `operation` (e.g. `scrape`, `parse`, `tabulate`, `aggregate`), and `type` (the event name, e.g. `file_download`, `page_scrape`, `file_parsed`). Additional fields are event-specific:

```json
{"ts": "2025-05-28T14:30:12+00:00", "state": "alabama", "operation": "scrape",
 "type": "file_download", "status": "ok", "filename": "contributions_2024.csv",
 "bytes": 1048576, "rows": 42301, "duration_s": 1.23}
```

### HTML reports

After a pipeline run completes, `main.py` reads the run's `log.jsonl` and passes it to `src/reporting/log_report.py`, which renders a human-readable HTML summary at `report.html` in the run's directory (`logs/prod/{run_id}/` for a manual run, `logs/daemon/{run_id}/` if it went through `ops/daemon.py`). The report includes per-state pass/fail status, stage durations, row counts, and any warnings or errors — and, for runs covering more than one state, per-state tabs so you're not scrolling past states you don't care about to find the one that failed. Report generation can be suppressed with `--no-report`.

### Reading a failed run

To diagnose a failure, check `log.jsonl` in the run's directory (`logs/prod/{run_id}/` manual, `logs/daemon/{run_id}/` via the daemon) for events with `level: ERROR` or `status: failed`. The `{state}_validate.json` files in the same directory contain the full tiered validation report for each state processed in that run.

---

## 10. Error Handling

Error isolation in the pipeline comes from the subprocess architecture rather than a centralized error handler. Each stage (scraper, parser, validate, tabulate) runs as its own `subprocess.run()` call inside `orc.py`, so an unhandled exception in one stage cannot corrupt or crash the orchestrator.

### Subprocess failure detection

`orc.py`'s `_subprocess` helper runs each stage and checks the exit code:

- **Exit 0** — stage succeeded; proceed to next stage
- **Non-zero exit** — stage failed; `orc.py` marks the state as failed and skips remaining stages for that state (but continues with other states in the batch)

When a subprocess exits non-zero, `orc.py` captures its stderr output (Python tracebacks, assertion errors, etc.), prints it to the terminal, and emits a `subprocess_error` JSONL event containing the label, exit code, and full stderr text. This means the run log always contains the actual exception, not just "exit 1".

### Within scrapers and parsers

Individual stages are responsible for their own exception handling. The pattern used across all scrapers and parsers:

```python
try:
    # ... all work ...
    log._emit("scrape_completed", status="completed", ...)

except KeyboardInterrupt:
    log._emit("scrape_completed", status="interrupted", ...)
    raise   # re-raise so sys.exit(130) fires in the CLI block

except Exception as e:
    log._emit("scrape_completed", status="error",
              error_type=type(e).__name__, error=str(e), ...)
    raise   # re-raise so sys.exit(1) fires in the CLI block
```

Both paths re-raise — the CLI block at the bottom of each file catches them and exits with the correct code (130 for interrupt, 1 for error). `orc.py` reads this exit code to decide whether to proceed.

Individual file failures inside a download or parse loop are handled locally: catch the exception, log it with `file_download_error` or `file_parse_error`, and continue to the next file. Only truly unrecoverable errors (corrupted source format, lost connection, etc.) should bubble up to the top-level handler.

### Dev vs. orc mode

Error behavior is identical in both modes — the subprocess architecture applies either way. The difference is in log routing: in dev mode (no `CF_RUN_ID`), events go to `logs/dev/`. In orc mode, events go to the run's shared `log.jsonl`. The `subprocess_error` event is only emitted in orc mode since `orc.py` is what captures stderr.
