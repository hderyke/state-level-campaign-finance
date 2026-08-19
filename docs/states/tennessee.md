# Tennessee — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Tennessee (TN) |
| **Source** | [Registry of Election Finance — TNCAMP](https://apps.tn.gov/tncamp/public/search.htm) |
| **Access method** | Plain HTTP — `requests` + BeautifulSoup driving a session cookie against a server-rendered JSP app (no Playwright) |
| **Coverage** | 2000 – present (future election-cycle years, e.g. 2028/2030, appear pre-populated and are mostly empty) |
| **person_id model** | `name_hash` — no filer ID of any kind in source; `person_id` derived from MD5 of `state + normalized candidate name` |

---

## Raw Data Structure

Four file types, all paginated ~100 rows per CSV (TNCAMP's export link only ever covers the currently-loaded results page):

```
contributions_{year}_p{NNN}.csv        -> contributions
expenditures_{year}_p{NNN}.csv         -> expenditures
candidates_{year}_e{id}_p{NNN}.csv     -> candidates (+ their committees)
pacs_p{NNN}.csv                        -> committees (unyeared — PAC search has no election criterion)
```

`{id}` in the candidate filename is TNCAMP's own opaque `electionYearSelection` option value — a year alone doesn't identify an election (special elections are registered separately, so a single calendar year can span several). The parser ignores the ID; it only keeps otherwise-colliding filenames apart.

### Transaction files (contributions / expenditures)

Header text is whatever the search form's "display these fields" checkboxes were set to, not a stable machine key — the parser resolves every header through a snake-cased alias table rather than an exact match. Key fields:

| Field | Notes |
|---|---|
| `Type` / `Adj` (Adjustment) | Combined into `transaction_type`; `Adj` also drives the `amended` flag |
| `Amount` | `$1,234.56` / parenthetical negatives |
| `Date` | `M/D/YYYY`; genuinely blank for a large share of early-year expenditure rows — see Data Notes |
| `Recipient Name` / `Candidate/PAC Name` | The filer this transaction is attributed to — **not the same column for both transaction types**. `Recipient Name` is 100% filled / `Candidate/PAC Name` 0% filled on every contribution row; the exact reverse on every expenditure row. Both headers are always present regardless of which one a given file actually populates. `get_recipient()` reads both and takes whichever is non-blank (see Parser) |
| `Contributor Name` / `Contributor Address` | Contributions only; address is one combined string (street, city, state, zip) |
| `Vendor Name` / `Vendor Address` | Expenditures only (the payee) |
| `Election Year`, `Report Name` | Election cycle and filing period |

### Entity roster (candidates_*.csv / pacs_*.csv)

Both files mix person and organization rows regardless of which search produced them — classification is by row content (`First Name` present = person), not by source file. Name arrives split as `First Name`/`Last Name` (no combined column in practice), an organization row has its full name in `Last Name` with `First Name` blank. `Primary`/`General` carry the election date with the outcome parenthesized on the end (`"08/07/2014 (W)"`), not a plain Y/N flag.

---

## Scraper

`src/pipeline/scrapers/tennessee.py`

TNCAMP is a plain server-rendered JSP app — no Playwright needed. A scrape walks pagination session-by-session: `POST` the search criteria for one (relation, year), then follow the "next page" control until it disappears. Each page lands as its own raw file, and the manifest records a `page = "complete"` sentinel once a year's walk finishes — pagination is session-based (no `?page=7` jump), so an interrupted year has to be re-walked from page 1 rather than resumed, and only years with the sentinel are treated as done.

**Concurrency:** each (relation, year) pair is independent (own search POST, own session cookie), so `run()` walks several years in parallel via a thread pool (`--workers`, default 4). A full sequential run took ~3 days; concurrency divides that roughly by worker count. Turn `--workers` down to 1 if TN.gov starts returning more 429/503s.

**Rate limiting:** requests carry a browser User-Agent/Accept headers and pages within a year's walk are spaced by a randomized sleep — TN.gov's WAF resets connections that look like bare scripts.

**Drift:** a `--discover` flag prints every live form field/option so request-body drift can be caught before a full run. A 2026-07-25 discover run found the candidate/PAC search (`cpsearch.htm`) had drifted significantly from the field names an earlier version of this scraper assumed (`searchType` not `findType`, `officeSelection`/`districtSelection`/`partySelection` not `officeSought`/`district`/`party`, one `winner` radio not separate primary/general fields, singular not plural `searchType` values) — all fixed in the current body builders. Whether this fully restored `office`/`committee_affiliation`/`contact_info` in the roster export was unconfirmed as of that fix; current runs show `office` and `district` filling well (see Data Notes), so it did.

**Limitations:** fails loudly (raises, doesn't silently write zero files) if the first search of a run returns no export link at all, since "form fields drifted" and "this year has no data" would otherwise look identical from the outside.

---

## Parser

`src/pipeline/parsers/tennessee.py`

**Output tables:** `committees.csv.gz`, `candidates.csv.gz`, `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz` (always empty — TNCAMP's public search exposes no loan schedules)

**Key transformations:**
- Every header is snake-cased and resolved through an alias table rather than indexed by exact text, so a cosmetic relabel on TN's side degrades one column instead of crashing the parse.
- `get_recipient()` reads the filer name from whichever of `Recipient Name` / `Candidate/PAC Name` is actually populated (see Raw Data Structure) rather than a single hardcoded column — the two are type-dependent, not interchangeable spellings of the same header.
- The combined address string (`"1385 5th Ave #11E, Nashville, TN, 37203"`) is peeled right-to-left — ZIP first (unambiguous), then a 2-letter state from a closed set, then city — leaving the street. Anything that doesn't fit the pattern keeps the whole string as street rather than being force-fit.
- The recipient a transaction row resolves to could be a candidate or a PAC — TNCAMP gives no type flag. Resolved against the candidate roster (loaded first): a name already known as a candidate populates `candidate_name`; anything else is registered as a committee.
- Roster names arrive `Last, First`; transaction names arrive forward. Both are normalized to forward order so committee↔candidate name-matching (name-based, no comma handling) works consistently.
- Registries (`candidates`, `committees`) are keyed by normalized name and de-duplicated across every election-year file a person/PAC appears in. `register_committee()` enrichment is first-non-blank-wins. `register_candidate()` is different: `office`/`district`/`party`/`incumbent` follow the *most recent* election_year seen, not the first non-blank value — TN's roster re-registers the same person fresh every time they run for anything, and people run for different offices across a career far more often than they change their name (e.g. Jerri Green: 2020 TN House District 83, lost; 2026 Governor). An older first-non-blank-wins policy would let a stale early-career office permanently outrank a current statewide candidacy under the same name; a blank field on the newest registration still falls back to the last known value rather than clearing it.
- Raw files are read through `open_raw_csv()`, which strips any embedded NUL bytes before handing the content to `csv.reader` — Python's `csv` module refuses to parse a line containing one at all. A handful of raw files (2 expenditure, 4 contribution, all found 2026-08-08) carry a single stray NUL where a decimal point looks like it should be (`"$1,693\x0038"`). Stripping rather than guessing leaves the Amount field unparseable, so `parse_amount()` correctly drops just that one row instead of the whole file crashing the parse.

**person_id model:** `name_hash` — TNCAMP publishes no filer ID anywhere in its public exports (not on transactions, not on the roster). Same model as Alaska, Kansas, and Kentucky.

**Limitations:**
- `state_filer_id` is unfillable — structurally absent from the source (`has_filer_id = 0` in `src/aliases/states.csv`), downgraded from a tier-1 to a tier-2 validator check.
- Expenditure `Date` is blank for a large share of pre-2004 rows — a genuine source gap (see Data Notes), downgraded to tier-2 for this state specifically.

---

## Data Notes

- **Expenditure dates missing for early years.** TNCAMP's expenditure export has no `Date` value at all for 2000–2002 (100% blank) and 2003 (98.9% blank), tapering from ~21% blank in 2004 to under 3% by 2007 and near-zero from 2011 on. Confirmed directly against the raw CSVs — the column itself is empty for these rows, not a parsing miss. Contributions from the same years are dated fine (0.1% blank overall across the full run), so this is specific to TN's expenditure schedule. `src/pipeline/validate.py`'s `TIER1_OPTIONAL_BY_STATE["tennessee"]["expenditures"] = {"date"}` downgrades this from a tier-1 failure to a documented tier-2 warning.
- **No filer ID anywhere.** Neither transactions nor the roster carry a numeric filer/committee ID — `state_filer_id` is 100% blank for both candidates and committees (expected, tier-2).
- **Expenditure `committee_name` is 100% filled** (fixed 2026-08-08). It was 100% *blank* until then — a parser bug, not a source gap: the parser read the filer name from `Recipient Name`, which is 0% filled on expenditure rows, instead of `Candidate/PAC Name`, which is 100% filled on the exact same rows. Caught by comparing a live TNCAMP search page against the parser's output. See Parser above (`get_recipient()`) and Raw Data Structure. Fixing this also lifted expenditure `candidate_name`/`office` enrichment from 0% to ~50%, since a real filer name is now available to resolve against the candidate roster.
- **A candidate's `office`/`district`/`party` reflect their most recent TNCAMP registration, not their first** (fixed 2026-08-08). Previously first-non-blank-wins, which meant a name that filed for one office early in TNCAMP's history and a different one later (state legislature → statewide, most commonly) stayed pinned to the earliest office forever, even as `election_year` correctly advanced. Concretely: Phil Bredesen and Diane Black both showed `office=Senate`/an old district from early-2000s filings while their real, money-dominant candidacies were their (state-level) Governor runs; Jerri Green showed `office=House of Representatives, District 83` — a 2020 race she lost — instead of `Governor`, her actual 2026 nomination. See Parser above (`register_candidate()`). Note `district` can still lag: a newer registration with a genuinely blank district field (as Green's 2026 filing has, since Governor has no district) falls back to the last known non-blank value rather than clearing it, so a stale district number can persist alongside a correct, current office.
- **Committee `jurisdiction`, `city`, `zip` are 0% filled.** Not published by the source at the committee-registry level.
- **Committee `candidate_name` is 0% filled** even though the parser has a branch for it (via the roster's Committee Affiliation column) — that column has not been observed populated in any scrape to date; the branch is kept dormant in case TNCAMP's export gains it back.
- **`committee_type` is ~87% PAC, ~13% blank** — TNCAMP's roster doesn't distinguish committee subtypes beyond PAC; the blank share is committees synthesized directly from transaction rows that never appeared in the PAC roster.
- **Contribution `contributor_type` and expenditure `category` are 0% filled** — no source field to derive either from.
- **Contribution `candidate_name`/`office` are ~22% filled; expenditure `candidate_name`/`office` are ~50% filled** — populated only when the transaction's recipient resolves to a known candidate directly; the rest go to committees instead. The expenditure side jumped from 0% after the `committee_name` fix above gave the resolver a real name to work with.
- **Expenditure `transaction_type` is 32.4% filled** — TN's `Type` column is sparsely populated on the expenditure side; the `Adjustment` half of the combined field is denser.
- **A handful of raw files carry a stray NUL byte** (2 expenditure files, 4 contribution files, found 2026-08-08) — almost certainly a byte-level glitch from the scrape, not anything TNCAMP served intentionally. `open_raw_csv()` strips these before parsing so the affected row is dropped via the normal unparseable-amount path instead of crashing the whole file.
- **Alias mappings** (`src/aliases/transaction_categories.csv`, `expenditure_categories.csv`) were filled in 2026-08-08 against the real scraped vocabulary — contributions map `Monetary`/`InKind` (+ `— Y` amended variants) and expenditures map `Neither`/`Independent`/`InKind` (+ `— Y` variants). `committee_types.csv` already covered `PAC`; `contributor_types.csv` is deliberately empty (TNCAMP publishes no contributor-type field at all).

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-26 |
| Parser | 2026-08-08 |
| Alias CSVs | 2026-08-08 |
| This doc | 2026-08-08 |
