# Pipeline Internals

Source code documentation for the state-level campaign finance pipeline. For CLI usage, output file locations, and how to run individual components, see the [README](../README.md). For adding a new state scraper or parser, see [contributing.md](contributing.md).

---

## Contents

1. [Architecture](#1-architecture)
2. [Schema](#2-schema)
3. [Orchestration](#3-orchestration)
4. [Pipeline Stages](#4-pipeline-stages)
5. [Tests](#5-tests)
6. [Person IDs and Name Matching](#6-person-ids-and-name-matching)
7. [Aliases](#7-aliases)
8. [Cloudflare R2](#8-cloudflare-r2)
9. [Logging](#9-logging)
10. [Error Handling](#10-error-handling)

---

## 1. Architecture

A full pipeline run flows through five sequential stages for each state, then merges all states into one aggregate database.

```
python3 src/main.py update AL AK AZ
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
         ├──▶  tests/validate.py     → tests/reports/{state}_latest.json
         ├──▶  pipeline/tabulate.py  → data/{State}/cleaned/{state}.db
         │
         │  after all states pass:
         └──▶  pipeline/aggregate.py → data/state-level-cf.db


python3 src/main.py push AL   (separate branch — no pipeline stages)
         │
         ▼
      main.py ──▶ cloudflare.py ──▶ Cloudflare R2 Worker
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

States are registered in `src/aliases/states.csv` (columns: `abbr`, `name`). `orc.py` reads this at startup into `ABBR_TO_NAME`, a dict mapping two-letter abbreviations to lowercase state names. To add a new state to the orchestrator, add a row to `states.csv` — no code change needed. The orchestrator will then recognize the abbreviation and know where to look for scraper and parser files.

### Run IDs

Every managed run gets a unique `run_id` of the form `{YYYYMMDD_HHMMSS}_{command}_{states}`, e.g. `20250528_143012_update_AL-AK`. This is set as the `CF_RUN_ID` environment variable before any subprocesses are spawned, so it is inherited by every stage. All logging (JSONL events, HTML reports, validation reports) is scoped to this ID. See [Logging](#9-logging) for details.

### Subprocess dispatch

Each stage — scraper, parser, validate, tabulate — is run via `subprocess.run()` rather than a direct function call. This has a few practical consequences:

- A crash in one stage (e.g. an unhandled parser exception) cannot corrupt the orchestrator's state or skip the failure check.
- `stdout` and `stderr` stream to the terminal in real time.
- The exit code is the failure signal. A non-zero exit from any stage causes `orc.py` to abort that state and record it as failed. Subsequent states in the batch still run.

### Scraper flags

Different states use different CLI interfaces for their scrapers, so `_scraper_flags(state, mode)` translates a pipeline command (e.g. `update-entities`) into the correct flags for each state's scraper. Colorado uses `--update`/`--force`, Alabama and Alaska use `--entities`/`--transactions`, and most others use `--update-entities`/`--update-transactions`. This mapping lives entirely in `orc.py` and does not need to be touched when adding a new state that follows the standard flag convention.

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

## 5. Tests

### Validator (`tests/validate.py`)

The validator runs automatically as part of every pipeline run (between parse and tabulate) and gates tabulation on success. It can also be run manually against any state's cleaned CSVs.

**Two-tier check system.** Checks are divided into tier 1 (hard failures) and tier 2 (warnings).

Tier 1 failures cause `validate.py` to exit with code 1, which causes `orc.py` to skip tabulation for that state. These checks include: required files exist, required columns are present, the table has at least one row, the `state` column has the correct value in ≥99% of rows, required fields (e.g. `amount`, `date`, `committee_name`) are non-empty in ≥99% of rows, amounts are numeric, and dates are valid `YYYY-MM-DD` within a plausible range.

The 99% threshold (`TIER1_PASS_RATE = 0.99`) rather than 100% tolerance is intentional — real state disclosure data contains a small number of malformed rows, and zero-tolerance would cause validation to fail on otherwise good datasets. Failures are reported with counts and percentages so the operator can judge severity.

Tier 2 warnings are printed and saved to the report but do not affect the exit code. These include: `election_year` out of range, `amended` not in `0`/`1`/blank, amounts above $10M (possible data entry errors), unrecognized state codes in `contributor_state`/`payee_state`, malformed ZIP codes, and enrichment fill rates for optional fields.

**Drift detection.** On each run, the validator compares current row counts against the previous run's report (stored at `tests/reports/{state}_latest.json`). A drop of more than 5% in any table triggers a tier 2 drift warning. This catches accidental data loss from parser regressions.

**Row sampling.** For memory safety, large files are validated on a random sample rather than loading everything into memory. The default sample size is 500,000 rows (tunable via `MAX_SAMPLE_ROWS` at the top of `tests/validate.py`). Sampling uses reservoir sampling (Algorithm R) so the sample is uniformly random across the full file rather than just the first N rows — important for chronologically ordered files. The total row count is always computed by streaming the full file regardless of sampling. When sampling is active, the terminal output and HTML report note "sampled X of Y rows" for transparency.

**Output.** Each run writes `tests/reports/{state}_latest.json` with row counts, tier 1 fill rates, tier 2 warnings, enrichment stats, and drift deltas. When running under `orc.py`, the report is also copied to `logs/prod/{run_id}/{state}_validate.json`.

### Test queries (`tests/test_queries.py`)

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

**`states.csv`** (`abbr`, `name`) — maps two-letter abbreviations to lowercase state names. Read by `orc.py` to populate `ABBR_TO_NAME`. Adding a row here is the only step needed to register a new state with the orchestrator.

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

## 8. Cloudflare R2

`src/cloudflare.py` handles syncing data to and from a Cloudflare R2 bucket via a Cloudflare Worker that acts as a thin authentication and manifest proxy.

### The Worker

The Cloudflare Worker is a lightweight serverless function deployed to Cloudflare's edge that sits in front of the R2 bucket. It serves three purposes:

**Authentication.** The R2 bucket is not publicly accessible. All push and pull operations go through the Worker, which validates the `X-Api-Key` header against a shared secret before doing anything. This keeps the bucket private without needing to distribute R2 credentials to every consumer.

**Manifest tracking.** The Worker maintains a server-side manifest of every file in the bucket — its size, MD5 hash, last pusher, and timestamp. This is what makes the `noop` check possible: on a `push/intent` request, the Worker compares the incoming file hashes against its manifest and tells the client which files have actually changed. Without this, every push would re-upload everything regardless of whether it changed.

**Pre-signed URL generation.** For files that do need uploading, the Worker generates a short-lived pre-signed R2 URL and returns it to the client. The actual upload then goes directly from the client to R2, bypassing the Worker entirely. This keeps large files (multi-hundred-MB state exports) out of the Worker's request body, which has size limits and would be a bottleneck.

### Credentials setup

Create a `.env` file in the project root with the following variables:

```
R2_ACCOUNT_ID=<your Cloudflare account ID>
R2_ACCESS_KEY_ID=<R2 access key>
R2_SECRET_ACCESS_KEY=<R2 secret key>
R2_BUCKET=<bucket name>
WORKER_URL=https://<your-worker>.workers.dev
WORKER_API_KEY=<shared secret for Worker auth>
```

`main.py` loads this file via `python-dotenv` before any cloudflare operations run. The variables can also be set directly in the environment (e.g. in a cron job or CI).

### Intent / confirm pattern

Uploads do not go directly to R2. Instead, `push_*` functions use a two-step protocol via the Worker:

1. **`/push/intent`** — send a list of files with their sizes and MD5 hashes. The Worker checks each against its manifest and responds with an action for each file: `upload` (new or changed) or `noop` (unchanged). For files to upload, the Worker returns a pre-signed R2 URL.
2. **Upload** — files marked `upload` are PUT directly to R2 via the pre-signed URL. The Worker is not in the upload path, so large files do not pass through it.
3. **`/push/confirm`** — after all uploads succeed, send confirmation to the Worker so it can update its manifest (byte deltas, pusher identity, timestamp).

This design means the Worker handles auth and manifest tracking without being a bandwidth bottleneck. The `noop` check means unchanged files are skipped automatically on every push.

### Push, pull, diff

`push_state` / `pull_state` — sync a single state's `data/{State}/` directory. `push_state` also runs a diff first and deletes any R2 objects that no longer exist locally (i.e. files removed from the local data directory are removed from R2 too).

`push_all` / `pull_all` — sync the entire `data/` directory. Both require typing `yes` at a confirmation prompt since they can touch a large amount of data.

`push_file` / `pull_file` — sync a single file (used for the aggregate database: `push db` / `pull db`).

`diff_state` / `diff_all` — compare local files against R2 by size and print a report showing which files are only local, only remote, or mismatched in size. Does not upload or download anything.

---

## 9. Logging

Every pipeline stage writes structured events to a JSONL log file. The logging system lives in `src/reporting/logger.py`.

### CF_RUN_ID

The `CF_RUN_ID` environment variable is the thread that ties an entire pipeline run together. It is set by `orc.py` before any subprocesses are spawned and inherited by every stage. Each stage's logger uses it to determine where to write its log:

- **With `CF_RUN_ID`** (orc/cron mode): logs go to `logs/prod/{run_id}/{stage}.jsonl`
- **Without `CF_RUN_ID`** (dev mode, running a component directly): logs go to `logs/dev/{timestamp}-{state}-{stage}.jsonl`

### Event format

Each log line is a JSON object with at minimum `ts` (ISO timestamp), `level` (`INFO`, `WARNING`, `ERROR`), `component` (e.g. `scraper`, `parser`, `tabulate`), and `event` (e.g. `table_loaded`, `validate_completed`). Additional fields are event-specific — for example, `table_loaded` carries `table`, `rows`, and `duration_s`.

### HTML reports

After a pipeline run completes, `main.py` reads the run's `log.jsonl` and passes it to `src/reporting/log_report.py`, which renders a human-readable HTML summary at `logs/prod/{run_id}/report.html`. The report includes per-state pass/fail status, stage durations, row counts, and any warnings or errors. Report generation can be suppressed with `--no-report`.

### Reading a failed run

To diagnose a failure, check `logs/prod/{run_id}/log.jsonl` for events with `level: ERROR` or `status: failed`. The `{state}_validate.json` files in the same directory contain the full tiered validation report for each state processed in that run.

---

## 10. Error Handling

`src/errors.py` provides a context manager, `pipeline_stage`, that controls how exceptions are handled depending on whether the pipeline is running in managed mode or dev mode.

```python
from src.errors import pipeline_stage

with pipeline_stage(log, component="scraper"):
    alabama.run()
```

### Dev mode (no `CF_RUN_ID`)

`pipeline_stage` is a complete no-op — it yields immediately and does nothing else. Exceptions propagate to the terminal as normal, giving full stack traces. This is the behavior when running any component directly (`python3 src/pipeline/scrapers/alabama.py`).

### Orc / daemon mode (`CF_RUN_ID` set)

Exceptions are caught, logged to the state's JSONL with a full traceback (`pipeline_error` event), and swallowed. Control returns to `orc.py`, which records the state as failed and moves on to the next state in the batch. `KeyboardInterrupt` is always re-raised — Ctrl+C stops the whole run regardless of mode.

The distinction exists because in a multi-state cron run you want one state's failure to be recorded and skipped rather than crashing the entire run, but in dev mode you want the raw error immediately.
