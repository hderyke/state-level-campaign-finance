# South Carolina — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | South Carolina (SC) |
| **Source** | [SC State Ethics Commission Public Reporting](https://ethicsfiling.sc.gov/public/campaign-reports) — [contributions](https://ethicsfiling.sc.gov/public/campaign-reports/contributions), [expenditures](https://ethicsfiling.sc.gov/public/campaign-reports/expenditures), [reports](https://ethicsfiling.sc.gov/public/campaign-reports/reports) |
| **Secondary source** | [SC Election Commission election history](https://electionhistory.scvotes.gov/search) (CSV export via `sc.elstats.civera.com`) — tier-2 backfill only |
| **Access method** | Selenium (Chrome + CDP network logging). The scraper runs one UI search per screen to capture the app's own JSON search request, then replays that request per year with an in-page `fetch()` |
| **Coverage** | 2008 – present (the earliest option in every year dropdown on the portal) |
| **person_id model** | `name_hash` — the portal exposes no filer ID on any transaction row; see [Parser](#parser) |
| **has_filer_id** | `0` in `src/aliases/states.csv` |

---

## Raw Data Structure

Files land in `data/South Carolina/raw/`.

### `contributions_{year}.json`, `expenditures_{year}.json`, `reports_{year}.json`

Each is the scraper's envelope around the search response:

```json
{"relation": "contributions", "year": 2018,
 "retrieved_at": "2026-08-01T09:14:22", "rows": [ ... ]}
```

`rows` is the search endpoint's record list verbatim. **The key names in those records are not documented and not contractual** — the endpoint is private to the Angular app. The parser therefore reads every field through a normalized-key lookup rather than a fixed schema (see [Parser](#parser)). The fields the grid renders, and which the parser maps, are:

| Screen | Grid columns |
|---|---|
| Contributions | Date, Amount, Candidate Name, Office Run Contributed To, Election Date, Contributor Name, Contributor Occupation, Group?, Contributor Address, Description |
| Expenditures | Date, Amount, Candidate Name, Office Run, Vendor Name, Vendor Address, Description |
| Reports | Report Name, Candidate Name, Office, Election Year, Election Type, Last Updated |

Notable absences across all three: no filer ID, no committee registry, no party, no district field, no contributor type beyond a yes/no "Group?" flag, no transaction type, no amended flag, and no separate address components — address is one unsplit line.

### `election_history.csv`

Full CSV export from the SC Election Commission's election history search, covering 2008 – current year. Supplies `party`, `district`, `jurisdiction` (county) and `incumbent`, which the ethics portal publishes nowhere. Joined on candidate name at parse time.

### `api_recipe.json`

Not raw data — a cache of the discovered search request per screen (`{method, url, post_data, headers, probe_year}`). Delete it, or pass `--rediscover`, to force re-derivation.

### `manifest.csv`

`relation, year, filename, downloaded_at, row_count, partial_years`. One row per relation-year, plus one for `election_history`.

`partial_years` is space-separated and populated only for `election_history` — it lists the years the service truncated mid-stream and stage-slicing could not fully recover (see "Election-history export defects"). Empty means the export is complete as far as the scraper can tell. A non-empty value makes the next run re-download rather than skip, and the parser warns when it loads a file recorded this way. The column is read defensively: a manifest written before it existed is treated as "none", not as an error.

---

## Scraper

`src/pipeline/scrapers/south_carolina.py`

### Why endpoint discovery instead of the download button

The grid's "Download Results" button is a Kendo `kendoGridExcelCommand`. Kendo builds the `.xlsx` in the browser and hands it to a Blob URL — no HTTP request is made for the file, so there is nothing to intercept or replay, and the export is limited to the columns the grid renders. Driving that button (the approach in the original prototype script) also requires rendering 30k+ rows per year before the export can start.

The grid is **not** server-paged: clicking Search fires a single JSON request that returns the entire year in one response, carrying every field the API knows about. That request's route lives in a lazy-loaded Angular chunk, so it can't be read off a saved copy of the page and hardcoding it would be a guess.

So the scraper derives it:

1. Open the search page in Chrome with CDP network logging enabled.
2. Set the year dropdown and click Search. **Only** the year is ever set — filling more than one search field triggers a client-side validation error that silently prevents the grid from loading.
3. Of the JSON XHR responses in the performance log, take the one that returned the most records. The year search dwarfs the dropdown-population lookups by orders of magnitude, so there is no ambiguity.
4. Save `{method, url, post_data, headers}` to `api_recipe.json`.
5. For every other year, rewrite the year token in the URL path/query and JSON body and replay with an in-page `fetch()`. No UI interaction, no grid render.

Year substitution is exact-match only (a value that *is* the year, as int or string) rather than a blind `str.replace`, so an unrelated id containing those digits is left alone.

If a cached recipe returns zero rows the scraper rediscovers once and retries. If that rediscovery also fails, the zero-row result is discarded rather than recorded — writing it would leave a 0-row file and a manifest entry that make every later run skip that year permanently.

### Why Selenium, and how the capture works

SC is the only state in the pipeline on Selenium; every other browser-driven scraper here uses Playwright. Selenium has no direct network-event listener, so the capture goes through Chrome DevTools Protocol in two parts:

- The `goog:loggingPrefs {"performance": "ALL"}` capability makes chromedriver buffer CDP events, readable via `driver.get_log("performance")`. That yields `Network.requestWillBeSent` (url, method, headers, post body) and `Network.responseReceived` (requestId, mimeType, resource type) — but **not** response bodies.
- `Network.getResponseBody` fetches each body by requestId.

Two details matter and are easy to get wrong:

- **Buffer size.** Response bodies only live in Chrome's per-page buffer, and the default is far too small for a 30k-row response. `Network.enable` is re-issued with enlarged `maxResourceBufferSize`/`maxTotalBufferSize`; without it the discovery body comes back evicted and the run can't find its own endpoint.
- **Draining.** `get_log` clears the buffer, so it is drained and discarded immediately before the search click to keep page-load traffic out of the candidate set.

Chrome truncates `postData` above ~64 KB and sets `hasPostData` instead; the scraper detects that and re-fetches the real body with `Network.getRequestPostData` so the recipe replays faithfully.

Replays run as `fetch()` inside the live page via `execute_async_script` rather than through Python's `requests` — same reasoning as Mississippi's scraper, the call executes in a real browser on the real origin and inherits the session's cookies, headers and TLS fingerprint. It is same-origin, so no CORS preflight is involved.

Performance logging is Chrome/Chromium-only; this scraper will not run against Firefox or Safari.

### Election-history download

`sc.elstats.civera.com` is a different origin from the ethics portal, so the in-page `fetch()` used for the year replays cannot reach it — CORS would block the response. Instead Chrome navigates directly at the CSV URL and the file is collected out of the browser's download directory (`data/South Carolina/.downloads/`, transient — the file is moved into `raw/` as `election_history.csv` and the original deleted). If the server serves the CSV inline rather than as an attachment, no file appears and the scraper falls back to reading the rendered body text. The result is sanity-checked for a comma in its header line before being written.

The service saves as `elstats_search_<hash>.csv`. `_wait_for_download` matches on "a new, complete, non-scratch file" rather than on the extension — the export is the only thing a run downloads, so the extension adds nothing, and the header-comma check settles whether the bytes are CSV. Stale entries are purged before each download so `before` is a clean baseline.

> **Known-bad — this export is not currently trustworthy.** See "Election-history export defects" below. A real download is ~822 MB, ends early, and supplies almost none of the columns it is here for.

### Election-history export defects

Measured against a real 2008–present export (822 MB, 4,710,175 rows). All four of these previously passed silently — the old code's only validation was a comma in the header line.

**The stream ends early, mid-file, with a JSON error.** Rows arrive newest-first, and when the service reaches a contest it cannot render it appends

```
{"errors":[{"message":"this contest does not have a division assigned"}]}
```

and closes the connection. The observed 2008–present request died on a 2022 contest, so the "full" export actually covered **2025–2022 only**. It still ends in a newline and its header is intact, so it looks complete. The scraper requests **one year per call** and scans for this sentinel: a poisoned year costs only that year, and each response is small enough to finish inside `DOWNLOAD_TIMEOUT`. Be aware the per-year loop writes considerably more data than the old truncated file did — the four years that fit in 822 MB imply roughly 200 MB per year.

**Stage-slice recovery.** Per-year requests contain the damage but don't undo it: rows arrive newest-first, so a poisoned year keeps its newest contests and loses every older one. A run against 2008–2026 saw 2016 truncate to 242,989 rows and 2022 to 1,971,954. When a year truncates the scraper re-requests it one `stages` value at a time, so each stage streams on its own connection and the bad contest costs its own stage rather than the rest of the year. Slices are unioned onto the rows already kept and deduped by a 16-byte digest per line — slices overlap the original partial by construction, since the partial holds every stage up to the poison point.

The stage vocabulary is read out of the truncated year's own header/rows first (that spells each value exactly as the service does) and topped up from `_STAGE_SLICE_FALLBACK` to cover stages that were cut off before appearing. **Whether the `stages` filter accepts these display names or numeric IDs is unconfirmed** — no live request has been made against it. The recovery path is written so a wrong guess is inert rather than harmful: the original partial is always the floor, slices only ever append, and a year is cleared of its partial flag only when at least one slice returned rows (proving the filter is honoured), none was itself cut short, and none failed to download. An empty slice counts as a real "no contests at this stage" only under that same proof, because the API returns an empty body for a rejected search object as well as for a genuinely empty one. If recovery never improves a year, check `_STAGE_SLICE_FALLBACK` first — the log line "no stage slice returned any rows" is the specific symptom of a rejected filter value.

Years that survive all that are written to `manifest.csv` as `partial_years`, which makes the next run re-download instead of skipping and makes the parser warn at load time. Per-year scratch files live in `data/South Carolina/.history_tmp/` — deliberately not in `.downloads/`, which is purged before every request.

**46% of rows are not people.** `candidate_name` carries per-contest tallies — `Total Ballots Cast`, `Total Votes Cast`, `Overvotes/Undervotes` — which `person_name()` normalizes into perfectly plausible names. In a 300k-row sample, 138,210 rows were tallies. `_TALLY_ROWS` in the parser drops them before they reach the name index, where the first+last fallback join could otherwise match a real filer against one.

**Two of the four backfill columns never resolved.** `load_election_history()` looked for `party` / `partyName` / `politicalParty` / `affiliation` and `county` / `countyName` / `jurisdiction` / `municipality`. The export's actual columns are `candidate_party_name` and `division_name`, and `_nk("candidate_party_name")` is `candidatepartyname` — matching none of them. Measured on 161,790 real candidate rows: party filled **0 → 160,680**, jurisdiction **0 → 3,624**. `division_name` is only a county when `division_type` says `County`; on the other 158,166 rows it holds a precinct name (`Windy Hill 02`) and is correctly excluded.

**`incumbent` has no source column at all.** The export has 26 columns and none of them is incumbency (`is_winner` is a different fact). The tier-2 table below still lists `incumbent` as sourced from this file; it will be empty for every candidate.

**Grain is ~383× finer than needed.** 4,710,175 rows collapse to 3,805 distinct candidates and 12,303 distinct (candidate, year, party, district, office) tuples — the export is at candidate × precinct × vote-channel grain and the parser reads none of that. Downloads are now streamed line by line into the combined CSV rather than `read_text()`-ed, which previously materialized the whole export as a Python `str` and again as a list from `splitlines()`.

### Year discovery

The year dropdowns are Kendo widgets whose options don't exist in the DOM until opened. The scraper opens each one and reads the live option list, so a new year appears without a code change. `FALLBACK_FIRST_YEAR = 2008` is used only if the widget can't be read — and that path logs a warning, because the fallback range (2008 – current year) is currently identical to what the live dropdown returns, so the year count alone can't tell you which path ran. A dropdown that can't be read is also a dropdown that can't be *set*, which is why the distinction matters.

**Options are read as `textContent`, never as Selenium's `element.text`.** `element.text` returns *visible* text: the WebDriver atom yields `''` for a node clipped by an `overflow` ancestor. Kendo's popup is a fixed-height scrolling list inside an `overflow:hidden` animation container, so every option below the fold reads back empty and is then silently discarded by the caller's truthiness filter. On an ascending year list that truncates the **newest** years — the observed symptom was contributions resolving to 2008–2024 while expenditures got the full 2008–2026, from identical code, because the two popups differed only in how much of the list happened to be rendered in view. Selection has the same failure mode and scrolls the target option into the popup before clicking, rather than waiting on its visibility.

The option list is also scoped to the widget's own popup via the ARIA relationship (`aria-owns`/`aria-controls` → listbox id) instead of a document-wide `li.k-item` query, and polled until two consecutive reads agree — the year lists arrive on their own lookup XHR, so a popup read too early is present but short.

### When discovery fails

Discovery drives a live UI, so it is the most breakable part of this scraper. Three things are watched concurrently after the Search click rather than waiting blindly on a grid selector:

- **The app's own validation notice.** Setting more than one search field, or failing to set any, makes the app render "Please search for something using at least one of the fields above" into `<app-server-error>` and never issue the request. The scraper fails immediately and quotes it, instead of burning `SEARCH_TIMEOUT` and reporting only that a locator never appeared.
- **Kendo's "no records" placeholder.** A search that ran and legitimately matched nothing — distinct from one that never ran. Means `probe_year` needs changing in `PAGES`.
- **A large JSON response finishing, or grid rows appearing.** The network signal is primary: it's what the recipe is derived from, and unlike Kendo's DOM structure it doesn't move between Kendo major versions. Four grid-row selectors are accepted as a secondary signal for the same reason.

The dropdown value is also read back after selection. A JS-dispatched click on a Kendo `<li>` can land without Angular committing it to the model — the widget keeps its placeholder, the Search is treated as empty, and you get the validation notice. That failure is otherwise indistinguishable from a grid that never loaded.

If the search still doesn't resolve, a screenshot, the page's visible text, and the full list of requests the page made are written to `logs/{prod,daemon}/{run_id}/sc_discovery/{relation}.{png,txt}`.

**The browser runs visibly by default.** The portal is an Angular SPA behind a WAF, and Alaska and Mississippi in this repo both had to run headed for the same class of reason. `--headless` opts back in once it's confirmed working on a given machine; `--headed` forces visible.

### Flags

Standard vertical flags (`--force`, `--start-year`, `--end-year`) are fully supported. Horizontal: `--transactions` → contributions + expenditures, `--entities` → the reports screen plus `election_history.csv`. `--candidates` and `--committees` both resolve to entities — SC publishes no separate registry for either.

Two extra flags: `--rediscover` (ignore the cached recipe) and `--headed` (run Chrome visibly, useful when discovery fails).

### Requirements

`pip install selenium` (pinned in `requirements.txt`) plus a local Google Chrome install. Selenium 4.6+ resolves a matching chromedriver itself via Selenium Manager, so there is no separate driver step — nothing equivalent to `playwright install` is needed.

### Runtime

Roughly 1–3 minutes per screen for a full 2008–present backfill; a single year's contributions response is on the order of 30k records. A 0.5 s pause sits between year requests — each one is a full-table scan on their side.

---

## Parser

`src/pipeline/parsers/south_carolina.py`

### Tolerant field lookup

Because the search API's key names are undocumented and unversioned, every field is read via `pick(idx, *names)` against a normalized index of the row's keys (lowercased, punctuation stripped). `contributorName`, `Contributor_Name` and `CONTRIBUTORNAME` resolve identically. Nested objects are flattened into the same index both bare and parent-prefixed, so `{"contributor": {"name": ...}}` resolves for either `name` or `contributorName`. A renamed key costs an alias entry, not a rewrite.

### Name format normalization

The contributions screen renders candidate names as `Allen Wooten Jr.`; expenditures and reports render the same people as `Kendrick, Robert S`. Left alone the two halves of the dataset would never join. `person_name()` detects the inverted form and flips it to `FIRST MIDDLE LAST`, keeping a generational suffix at the end. Organization names (which also appear in these fields) contain no comma and pass through unchanged apart from case normalization.

### Address splitting

Addresses arrive as one line with no delimiter between street and city — the only comma sits between city and state:

```
515 Handsome Oak Drive Hardeeville, SC 29927
PO Box 190012 N. Charleston,SC 29419
```

State and ZIP come off the tail reliably. City is recovered by walking backwards from the state code and stopping at the first token that contains a digit or is a street-type word (`Drive`, `Box`, `Ste`, …), capped at three tokens. Anything that doesn't match cleanly leaves `city` empty — a wrong city is worse than a missing one, since these feed geographic rollups.

### Office and district

SC's "Office Run" is free text with the district glued on: `SC Senate District 10`, `School Board Trustee District GREENVILLE`, `Coroner No. 2`. The full string is kept in `office` (truncating loses meaning — "School Board Trustee" without its county is useless) and the district is additionally surfaced in `district`. `canonical_office` is resolved at aggregate time by exact match for statewide/county titles and by LIKE patterns for the district-suffixed forms.

### person_id: `name_hash`

There is no filer ID on any transaction row. The only identifiers on the site — `personId`, `seiId`, `officeId` — appear in report-detail deep links, not in the search results. The normalized name is the only key shared by all three screens, so `name_hash` is the only model that produces a consistent identity across transaction-derived and report-derived filers. `states.csv` sets `has_filer_id=0`, which downgrades the `state_filer_id` fill-rate check from tier 1 to tier 2. Where a reports row *does* carry `personId`, the parser writes it to `state_filer_id` for traceability even though `person_id` is name-derived.

### Committees

SC has no committee registry of any kind. Committees are synthesized one per distinct filer, with `committee_name = candidate_name` and `committee_type = "Candidate Committee"` — every filer reachable through these screens is a candidate or public official (the screens sit under `/candidates-public-officials`). Standalone PACs file through a separate system that publishes no public search and are therefore absent from this dataset entirely.

### Election-history backfill

`party`, `jurisdiction` and `incumbent` come exclusively from `election_history.csv`; `office`, `district` and `election_year` fall back to it only when the ethics portal left them blank. Matching is by normalized candidate name — exact first, then an unambiguous first+last fallback, mirroring `utils.assign_committee_person_ids`. When a candidate appears in several contests the most recent one wins, merged over older rows so an older party or county isn't lost to a newer blank. Unmatched candidates keep those columns empty; the join rate is reported through `log.enrichment_summary`.

### Loans and debts

SC publishes no loan or debt schedule through this portal. `loans_debts.csv.gz` is written header-only so tabulate has a consistent set of inputs.

---

## Data Notes

- **No contributor type.** The only contributor classification published is a yes/no "Group?" flag. It is written as `Individual`/`Group` and mapped to `Individual`/`Organization` in `contributor_types.csv`. Rows where the flag is blank are backfilled at aggregate time from the committees table like every other state.
- **No transaction type.** Neither screen publishes an in-kind, refund, or category field, so `transaction_type` is empty for every SC row and `transaction_category` resolves to NULL. Mappings exist in the alias CSVs for forward compatibility but are not exercised by current data.
- **No amended flag.** Nothing in the source distinguishes an amended filing from an original, so `amended` is left empty.
- **Rows dropped at parse.** A contribution or expenditure missing filer, amount, or date is skipped rather than written — those three are tier-1 required and an untraceable row is worse than a missing one. Counts appear in the `file_parsed` events as `skipped`.
- **`election_year` semantics differ by screen.** Contributions carry an explicit Election Date, so `election_year` is taken from it. Expenditures have no election date at all — `election_year` there is the year of expenditure, which is the closest available proxy and may differ from the actual cycle.
- **Election-history coverage.** The SC Election Commission dataset starts at 2008 and only covers people who actually appeared on a ballot. Candidates who filed with the Ethics Commission but withdrew before the ballot, and non-candidate filers, will never match and keep empty party/district/incumbent.
- **PACs are missing.** This is a candidate/public-official dataset only. Any cross-state analysis of PAC activity should treat SC as having no PAC coverage rather than zero PAC activity.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-01 |
| Parser | 2026-08-01 |
| Docs | 2026-08-01 |
