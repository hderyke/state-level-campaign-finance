# CLAUDE.md

This file tells you what to do when asked to add a new state to the pipeline.

---

## Scope

You are writing exactly two files:

```
src/pipeline/scrapers/{state}.py
src/pipeline/parsers/{state}.py
```

Don't touch `columns.py`, `aggregate.py`, `orc.py`, `utils.py`, or anything else in the pipeline unless something is clearly broken. Validation, tabulation, aggregation, logging, and HTML report generation are all automatic once the scraper and parser are in place.

---

## Read first

1. **`docs/contributing.md`** — the full contract. Contains checklists, boilerplate, and the CLI flag taxonomy. Start here.
2. **`docs/pipeline.md`** — internals reference for schema, person IDs, aliases, and the logging API. Use as a lookup when you need implementation details.
3. **README.md** - high-level summary for users

---

## Pick reference examples

Read 2–3 existing scrapers and parsers that match the new state's source pattern. Suggested starting points:

| Source pattern | Reference |
|---|---|
| Bulk CSV by year, simple HTTP | `arkansas.py` (simplest end-to-end example) |
| Bulk ZIP by year + entity page sweep | `indiana.py` |
| Single full-history file, no year split | `illinois.py` |
| Playwright / JS-gated site | `alaska.py` |
| Multi-source or three eras | `idaho.py` |
| Registry enrichment (join entity file against transactions) | `alabama.py` or `alaska.py` |

When in doubt, start with Arkansas.

---

## Use any URLs or headers the user provides

If the user gives you API endpoints, request headers, or cookies (e.g. copied from browser DevTools), use them exactly. Don't guess at endpoints or fabricate headers. Drop real headers directly into the scraper's session setup.

---

## Register the state

Add one row to `src/aliases/states.csv` before anything else:

```
{AB},{state},{fips}
```

`fips` is the two-digit zero-padded FIPS code (look it up from any existing row or at census.gov). This is all that's needed to make `orc.py` recognize the abbreviation.

---

## Write the scraper

Follow the checklist in `docs/contributing.md §4`. Things that are easy to miss:

- Manifest respected on normal runs — skip files already in `done`; always re-fetch current year
- `--force` clears in-scope manifest entries before downloading
- `scrape_started` emitted at top of `run()`; `scrape_completed` emitted in all three exit paths (success, interrupt, error)
- Use `file_download_*` methods for bulk file fetches; `page_scrape_*` for ID/page sweeps
- CLI block uses `parse_known_args` — `orc.py` may forward unknown flags

---

## Write the parser

Follow the checklist in `docs/contributing.md §5`. Things that are easy to miss:

- `raw_file` and `row_num` on every contributions and expenditures row
- All five output files written (`loans_debts.csv.gz` can be empty if state has no loan data)
- File handles closed in `finally` block
- `assign_person_ids` and `assign_committee_person_ids` called **after** file handles are closed
- `parse_started` emitted at top of `run()`; `parse_completed` emitted in all three exit paths

See `docs/contributing.md §6` for which `id_model` to pass to `assign_person_ids` (`"person"`, `"committee"`, or `"name_hash"`).

---

## Add alias mappings

After the parser produces clean output, check what raw values appear in each field and add entries to:

```
src/aliases/committee_types.csv
src/aliases/contributor_types.csv
src/aliases/transaction_categories.csv
src/aliases/expenditure_categories.csv
```

Format is `state,raw,canonical`. Leave `canonical` blank to suppress a value. Use `#` comment lines to explain non-obvious decisions. See `docs/contributing.md §7` and `docs/pipeline.md §7` for canonical values and format details.

---

## Test your work

Run the scraper alone first to confirm downloads land correctly:

```bash
python3 src/pipeline/scrapers/{state}.py
```

Then run the full pipeline. Use `sync` on first run; use `reparse` if raw data is already on disk and you only changed the parser or aliases:

```bash
python3 src/main.py sync {AB}
python3 src/main.py reparse {AB}    # parser-only iteration
```

Both generate an HTML report at `logs/prod/{run_id}/report.html` and a validation report at `logs/prod/{run_id}/{state}_validate.json`. **Read the HTML report** — it shows row counts, stage durations, tier-1 pass rates, and any warnings.

Then run the spot-check queries and review the output by eye:

```bash
python3 src/pipeline/queries.py {state}
```

Look for:
- Top contributor names and amounts look real for that state
- Top recipient candidates are actual candidates (not junk rows)
- No implausible amounts (e.g. $100M contributions)
- Committees and candidates have `person_id` populated

---

## Done when

- [ ] Validator exits 0, tier-1 pass rates all ≥ 99%
- [ ] Row counts plausible for the state
- [ ] `test_queries.py` output shows recognizable names and reasonable amounts
- [ ] All four alias CSVs have entries for this state
- [ ] `docs/states/{state}.md` written (see `docs/contributing.md §10` for the required sections)
