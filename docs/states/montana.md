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

Top-level fields mirror the corresponding yearly list row, plus a `reports` array. Each report entry has `reportId`, `formTypeCode` (`C5`/`C6`/`C4`/`C7`/`C7E`), `fromDateStr`, `toDateStr`, `statusDescr`, `amendedDate`, `fetchFingerprint` (the scraper's report-level cache key — see Scraper), and one of two shapes depending on form type:

**C5 (candidate periodic) / C6 (committee periodic) / C4 (committee independent-expenditure-style periodic)** — `contributions`/`expenditures` are list-of-dict rows read directly from CERS's pipe-delimited bulk schedule export, with the server's own column headers preserved: `Date Paid`, `Entity Name`, `First Name`, `Middle Initial`, `Last Name`, `Addr Line1`, `City`, `State`, `Zip`, `Contribution Type` (numeric 1–9), `Amount`, `Amount Type` (`CA`/`IK`/`Mixed`), `Purpose`, `Election Type`, `Previous Transaction (Y/N)` for contributions; `Date Paid`, `Entity Name`, `Expenditure Type`, `Amount`, `Purpose`, `Election Type` for expenditures.

**C7 (last-minute contribution notice) / C7E (last-minute expenditure notice)** — no bulk export exists; `contributions_c7`/`expenditures_c7e` hold the server's native JSON line items per sub-table (`individual`/`committee`/`loan` donors for C7; `expendOther` for C7E), with fields `entityName`, `entityAddress` (single `"street, city, ST zip"` string), `datePaid` (epoch milliseconds), `cashAmt`, `inKindAmt`, `totalAmt`, `occupationDescr`, `employerDescr`, `amountTypeDescr` (the election phase — Primary/General, not a cash/in-kind flag), `previousTransactionInd`.

A report the scraper could not fetch carries a `fetchError` key and no itemized rows. Those are never cached — the next run retries them — and the parser counts them as skipped rather than reading them as a filer with no activity.

Raw JSON is written compactly (no `indent=2`) and atomically (temp file + `os.replace`), so an interrupted run cannot leave a truncated file behind. Pipe a file through `python3 -m json.tool` when reading one by hand.

A companion `manifest.csv` tracks `(entity_type, entity_id)` pairs already fetched.

---

## Scraper

`src/pipeline/scrapers/montana.py`

CERS's public search UI has no bulk export — it's limited to one candidate/committee for one election year at a time, with a per-search CSV export button. Rather than automate that UI (what the original R function did via Selenium), this scraper calls the underlying AJAX endpoints directly:

**Phase 1 — entity discovery (parallel over years):**

1. POST blank search params + `electionYear` to establish server-side search state, then GET the DataTables results endpoint — returns every candidate/committee active that year in one call (candidates and committees are fetched independently, both filterable by year alone). Results are deduplicated by `(entity_type, entity_id)` across years, so an entity listed under several election years is fetched once per run rather than once per year.

**Phase 2 — report fetch (parallel over entities):**

2. For each entity, POST its ID to list every report it has filed.
3. For each report: C5/C6/C4 reports get their bulk pipe-delimited schedule downloaded (POST to get a filename token, GET the file, streamed line-by-line into rows); C7/C7E reports have their line-item sub-tables fetched directly as JSON (no download step). A report already on disk whose `fetchFingerprint` still matches is reused instead of re-fetched.

These endpoints, payloads, and the pipe-delimited export format were identified from Montana Free Press's open-source [`cers-interface`](https://github.com/eidietrich/cers-interface) project, which has scraped this same site every election cycle through the 2026 cycle using this exact API — strong evidence it's still current.

**IMPORTANT CAVEAT:** this development environment's network egress does not reach `cers-ext.mt.gov`, so these endpoints could not be smoke-tested against the live site. Everything *except* the endpoint contract itself was verified against a local mock CERS server (an HTTP/1.1 `ThreadingHTTPServer` implementing the same routes, with per-connection session state): entity dedupe across years, keep-alive connection reuse, real concurrency overlap, report-cache hit/miss, amendment-triggered re-fetch, `fetchError` marking and retry, raw JSON shape, and the full scrape → parse → `validate.py` (PASS) → `tabulate.py` chain. But the actual endpoint URLs, payload field names, and response shapes are only as reliable as the third-party reference they were copied from. **Run a small slice locally first** (e.g. `python3 src/pipeline/scrapers/montana.py --start-year 2024 --end-year 2024 --candidates`) and inspect `data/Montana/raw/` before trusting a full backfill.

Two things to watch on that first live slice, since both were assumptions the mock could not falsify: whether CERS tolerates 6 concurrent sessions (drop `--workers` to 1–2 and set `REQUEST_DELAY` if you see 429s or truncated exports), and whether `statusDescr`/`amendedDate` really do move when a filer amends a report — if they don't, the report cache would shadow amendments, and `report_fingerprint()` needs another field.

If a specific report times out repeatedly, it is generating an export large enough to exceed `TIMEOUT_DOWNLOAD` / `TIMEOUT_PREPARE`. Raise those rather than `SCHEDULE_ATTEMPTS` — extra attempts just make CERS rebuild the same export from scratch. The report is marked `fetchError`, its entity stays out of the manifest, and the next run retries only that report, so a stubborn one degrades a run's completeness rather than blocking it.

### Performance

An earlier revision of this scraper took several days for a full sweep. Six compounding causes, all now addressed:

| Cause | Fix |
|---|---|
| A brand-new `requests.Session()` per POST+GET pair — a fresh TCP connect + TLS handshake for every report list, schedule download and C7 sub-table | One keep-alive session per worker thread (thread-local) with an `HTTPAdapter` connection pool |
| Fully sequential — one entity, one report, one schedule at a time | `ThreadPoolExecutor` at **report** granularity, in batches of `ENTITY_BATCH_SIZE` entities (`PARALLEL_WORKERS`, default 6); year-list fetches are parallel too |
| Read timeouts retried at the adapter level — a slow `prepareDownloadFileFromSearch` means CERS is still building a large export, so retrying made it restart. One oversized report cost 4 × 180s and still returned nothing | `read=0` on the `Retry`; read timeouts handled once, deliberately, in `fetch_schedule()` on a fresh session (`SCHEDULE_ATTEMPTS`, default 2). Connect failures and 5xx/429 are still retried blindly |
| Unconditional `time.sleep()` of 0.1s per report + 0.15s per entity — hours of pure sleeping at Montana's volume | Politeness comes from the bounded worker count; `REQUEST_DELAY` (default 0.0) is there if CERS turns out to rate-limit |
| No report-level incrementality — a current-year entity re-downloaded its entire filing history every run | Reports cached from the raw file, keyed by `fetchFingerprint` = `(formTypeCode, statusDescr, amendedDate, fromDateStr, toDateStr)`; unchanged reports are reused, new/amended ones re-fetched |
| Schedule exports (the large-response case) buffered whole into a string, then re-parsed from a `StringIO` copy | Streamed line-by-line straight into `csv.DictReader`; raw files written compactly |

**Why session reuse is safe:** CERS keys search/report context to the session cookie, which is why the original isolated every lookup in its own session. Contamination only occurs if two *concurrent* lookups share a cookie jar. Every task sets its context and consumes it before the thread moves on, and no two threads share a session. `reset_session()` rebuilds a thread's session if one ever does go bad (it's called after any report-level failure, and on a schedule retry).

**Why report-level concurrency is safe:** each report is self-contained. `prepareDownloadFileFromSearch` takes `reportId` explicitly and needs no session context at all, so C5/C6/C4 schedule fetches are stateless. C7/C7E's `retrieveReport` → `financeRepDetailList` pair runs start-to-finish inside a single task on one thread's own session, so two reports' contexts can never interleave.

**Why this matters — the straggler problem.** An earlier revision parallelised over *entities*, which is fine until one filer has a lot of reports: a candidate with 48 reports (≈96 schedule fetches) pinned a single worker for the whole run while the other five idled, and any timeout on those reports was multiplied by the adapter's read retries. Verified with a mock filer carrying 48 reports: with every other entity cached, in-entity concurrency reaches the full worker count and wall time is ~3.2s against a ~9.6s serial lower bound.

**Failure handling.** A report that fails is marked `fetchError` and left for the next run, and — importantly — an entity with *any* failed report is deliberately **not** written to `manifest.csv`. Otherwise a past-election-year filer with one timed-out report would be skipped forever, since the manifest skip rule only exempts the current year. The next run revisits it, and the report cache makes that cheap: only the reports that actually failed are re-fetched.

Tuning: `PARALLEL_WORKERS` (default 6) or `--workers N` per run; `ENTITY_BATCH_SIZE` (default 25) trades memory for straggler smoothing; `SCHEDULE_ATTEMPTS` (default 2) and `TIMEOUT_PREPARE`/`TIMEOUT_DOWNLOAD` bound what a single slow report can cost. Set workers to 1 for fully-sequential behaviour.

**Limitations:**
- No pagination needed — `iDisplayLength` is set to 1,000, comfortably above any single year's candidate/committee count based on the reference project's experience.
- `--contributions`/`--expenditures`/`--entities`/`--transactions` flags are accepted (for CLI-contract consistency with other states) but ignored — fetching an entity's reports always yields both contributions and expenditures together, so there's no cheaper partial fetch. Only `--candidates`/`--committees` meaningfully narrow scope here.
- Only the current year's entities are re-fetched on an incremental run; a candidate/committee's data from a past election year is treated as final once fetched (use `--force` or `--start-year` to refresh it, e.g. after an amendment). `--force` also disables the report-level cache, since it means "trust nothing on disk."
- Every C7/C7E sub-table is fetched even though the parser only reads `individual`/`committee`/`loan` and `expendOther`, so the raw capture stays complete and the parser can be widened later without a re-scrape. C7/C7E are a small minority of reports, so this is not a meaningful share of runtime.

**Expected runtime:** still unverified against the live site. The first full 2000–present backfill remains a background job — the volume of entity-years is irreducible — but per-entity cost is now dominated by actual data transfer rather than handshakes and sleeps, and runs 6 entities wide. Incremental re-runs should be dramatically faster: only the current year is re-swept, and within it only reports that are new or amended are re-downloaded.

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
- Reports carrying a `fetchError` (the scraper could not download them) are skipped and reported via the `skipped` count on each `file_parsed` event, plus a run-level `reports_incomplete` total on `parse_completed`. A partial scrape therefore shows up in the parse log instead of looking like a filer with no activity. `fetchFingerprint` is ignored here — it exists only for the scraper's cache.

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
