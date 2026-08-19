# New Jersey

## Overview

| | |
|---|---|
| **State** | New Jersey (NJ), FIPS 34 |
| **Agency** | Election Law Enforcement Commission (ELEC) |
| **Source** | [njelecefilesearch.com](https://www.njelecefilesearch.com) — "ELEC Reports and Data Search System" |
| **Access** | Plain HTTP JSON API under `/api/`. No Playwright, no authentication, no antiforgery token |
| **Coverage** | Filings from the 1999 primary onward; scraper floors at **2000** |
| **person_id model** | `committee` |
| **Loans / debts** | Not published through this search system — `loans_debts.csv.gz` is header-only |

Data refreshes between **8–10 AM ET on business days**, so there is no value in
running more than once a day.

---

## Raw Data Structure

### How the source actually works

Every table on the site is a **server-side DataTables grid**. The page HTML
ships as an empty `<thead>`-only shell and the rows arrive over XHR, which has
two consequences worth knowing before touching this state:

- **A saved page is useless for reverse-engineering.** Chrome's MHTML
  serializer strips `<script>` tags, and the page JS is inline in the Razor
  view rather than an external file, so neither an MHTML snapshot nor a plain
  `GET` reveals the endpoints. They have to come off the wire (DevTools →
  Network → Fetch/XHR → Copy as cURL).
- **There is no bulk export.** The "Download Data - 65000 Max." and "Excel"
  buttons are DataTables `buttons-html5` widgets that serialize rows already
  loaded in the browser. They expose nothing the paged API doesn't, and the
  65,000 ceiling is just the classic `.xls` row limit.

Requests are form-encoded and carry the standard DataTables parameter block
(`draw`, `columns[i][data|name|searchable|orderable|search[value]|search[regex]]`,
`order[i][column|dir|name]`, `start`, `length`, `search[value]`) with ELEC's
own filter fields appended. `X-Requested-With: XMLHttpRequest` and a
same-origin `Referer` are both required. `ARRAffinity` cookies are Azure
load-balancer stickiness — harmless, but `_make_session()` primes them so a
long sweep pins to one backend.

### Endpoints

| Route | Method | Scoped by | Status |
|---|---|---|---|
| `/api/VWEntity/Entities20` | POST | `ElectionYears` + `NONPACOnly` | **verified** |
| `/api/VWEntity/GetEntityDataWithCommittee` | GET | `ENTITY_S` | **verified** |
| `/api/VWContributionDetail/GetContBitsDataByObject` | POST | `EntityName` + `ElectionYears` | **verified** |
| `/api/VWExpenseDetail/GetBitsDataByObject` | POST | `EntityName` + `ElectionYears` | **verified** |

Verified entity-listing filter fields: `NONPACOnly`, `FirstName`, `LastName`,
`MI`, `Suffix`, `NonIndName`, `OfficeCodes`, `PartyCodes`, `LocationCodes`,
`ElectionTypeCodes`, `ElectionYears`, `SortColumn`, `SortBy`. Empty string
means "no filter" — the site posts every field on every search, and
`_entity_filters()` mirrors that rather than sending a minimal body, because
.NET model binders are less forgiving than they look.

### Five things that will bite you

**1. The transaction endpoints ignore `ENTITY_S`.** The interactive pages carry
`?eid=411086` on their URL and *do* send an `ENTITY_S` field — **empty** — then
scope the query with `EntityName` + `ElectionYears`. So the unit of work for a
transaction sweep is a *name + year*, not an entity id. Consequences:

- The sweep dedupes targets on `(name, year)`. This is correctness, not
  optimization: a filer who ran in both the primary and the general has two
  eids and one name, and sweeping per eid would fetch and write their entire
  transaction history twice.
- Contributions cannot be attributed to a specific cycle (expenditures can —
  see point 5). A filer's primary and general contributions land under one
  committee row.
- Two different people sharing a name in one election year would merge into a
  single result set. Uncommon, but real, and it's a property of the endpoint —
  ELEC exposes no id-scoped alternative.

**2. `columns[i][data]` is not always `columns[i][name]`.** On the contribution
grid, `Address`→`STREET1` and `EmployerAddress`→`EMP_STREET1`. `data` is the key
on the returned JSON row; `name` is the DB column used for sort/search. Send
`name` where `data` belongs and the handler returns rows whose keys don't match
what you then read — which fails *silently* as empty columns, not as an error.
`*_COLUMNS` are `(data, name)` tuples for this reason.

**3. Display headers lie about the underlying columns.** Both transaction grids
show a "Recipient" header over a `CAND_NAME` column, and an "Entity_S" header
over a column that is only *sometimes* the entity id — `ENTITY_S` on the
expense grid but `CONTRIB_S` on the contribution grid. Always trust
`columns[i][data]` from a capture over the rendered header. See point 5.

**4. The two transaction routes are not symmetrically named.** Contributions
is `VWContributionDetail/GetContBitsDataByObject`; expenses is
`VWExpenseDetail/`**`GetBitsDataByObject`** — the action does not repeat the
entity. Guessing `GetExpBitsDataByObject` by analogy 404s. Their bodies differ
too: `AmountFrom`/`AmountTo` are `""` on the contribution route and `"0"` on
the expense one, which is why the amount bounds sit in the per-relation branch
of `_transaction_filters()` rather than the shared block.

**5. Only expenditures can be pinned to an election cycle.** The expense grid
returns `ENTITY_S` on every row, so the parser resolves an expenditure to its
exact per-cycle entity and only falls back to name+year. The contribution grid
returns `CONTRIB_S`, which despite sitting under an "Entity_S" header is a
*contributor* surrogate key — the `cid` in the dashboard's
`?eid=…&cid=…` links — not an entity or transaction id. Contributions are
therefore resolvable only to name+year, and `filing_id` is deliberately left
blank for them rather than filled with a contributor id.

### Confirming a route without a browser

```bash
python3 src/pipeline/scrapers/new_jersey.py --probe
python3 src/pipeline/scrapers/new_jersey.py --probe "AARON, CHARLES S JR" --probe-year 2023
```

Walks the candidate route names, reports which answer with JSON rows, and
prints the row keys so `*_COLUMNS` and `FIELD_ALIASES` can be checked against
reality. Both routes are currently confirmed, so this now serves as a
regression check for when ELEC next renames an action. It distinguishes a 404
(wrong route name — keep guessing) from a 400/500 (right route, wrong body —
only `*_COLUMNS` / `_transaction_filters()` need fixing), which are very
different repairs. Diagnostic only — `orc.py` never calls it.

### If you get `certificate verify failed`

```
SSLError: certificate verify failed: unable to get local issuer certificate
```

This is a local trust problem, not ELEC — the tell is that the same URL loads
fine in your browser. On a network that inspects TLS (Zscaler, Netskope, a
proxy appliance) every response is re-signed by an internal root CA that lives
in the OS trust store, which browsers use and `requests` doesn't.

`config.ca_bundle()` handles this with no extra dependencies: on Windows it
exports the OS trust store via `ssl.enum_certificates()` — the same store the
browser reads — merges in certifi's roots, caches the result in the temp dir,
and hands it to `requests`. The scraper wires it up in `_make_session()`. It's
a no-op on macOS/Linux, where certifi normally suffices.

If it still fails, the scraper aborts immediately with instructions rather
than retrying (a trust store doesn't fix itself on attempt two). In order:

```bash
# 1. rebuild the cached bundle
python3 -c "import config; print(config.ca_bundle(refresh=True))"

# 2. or point requests at your corporate root explicitly
setx REQUESTS_CA_BUNDLE "C:\path\to\corp-root.pem"
```

Export that PEM from Edge: lock icon → Connection is secure → certificate →
Details → Copy to File → **Base-64 encoded X.509**. Being on/off VPN also
matters, since split-tunnel setups often only inspect on-network traffic.

`verify=False` is deliberately **not** the answer here. It would accept any
certificate at all on a network already known to be intercepting traffic.
(Alabama's scraper does use it, but for the opposite reason — a genuinely
malformed cert on the state's own server, not an intercepting proxy.)

### Files written to `data/New Jersey/raw/`

| File | Source | Contents |
|---|---|---|
| `entities_{year}.csv` | `Entities20`, `NONPACOnly=true` | Candidate, joint-candidate and election-related committees |
| `pacs_{year}.csv` | `Entities20`, `NONPACOnly=false` | PAC, party and legislative leadership committees |
| `entity_details_{year}.csv` | `GetEntityDataWithCommittee` | Treasurer, mailing address, joint committee, status |
| `contributions_{year}.csv` | contribution API | Every name+year in that election year, concatenated |
| `expenditures_{year}.csv` | expense API | Every name+year in that election year, concatenated |

Entity files key on `eid`; **transaction files key on `entity_name` +
`election_year`**, because that's the only scope the endpoints accept.

Column names in these files are **ours**, not ELEC's — they are the contract
between the scraper and the parser, declared as `ENTITY_COLS`,
`ENTITY_DETAIL_COLS`, `CONTRIBUTION_COLS` and `EXPENDITURE_COLS` in the
scraper. ELEC's real JSON keys are noted inline beside each.

Contribution grid, display header → actual JSON key:

| Header | Key | Note |
|---|---|---|
| Contributor | `CONTRIBUTOR` | |
| Address | `Address` | DB column `STREET1` |
| Employer | `EMP_NAME` | |
| Emp. Address | `EmployerAddress` | DB column `EMP_STREET1` |
| Occupation | `OccupationName` | |
| Recipient | `CAND_NAME` | header and column disagree |
| Contributor Type | `ContributorType` | |
| Contribution Type | `ContributionType` | |
| Date | `CONT_DATE` | |
| Amount | `CONT_AMT` | |
| Entity_S | `CONTRIB_S` | a **contributor** id (the dashboard's `cid`) — not an entity or transaction id |

Expense grid:

| Header | Key | Note |
|---|---|---|
| Receiver | `PAYEE` | |
| Address | `Address` | DB column `STREET1` |
| Recipient | `CAND_NAME` | |
| Expense Desc | `EXPENSE_DESC` | |
| Receiver Type | `PAYEE_TYPE` | |
| Check # | `CHECK_NUM` | |
| Date | `CK_DATE` | |
| Amount | `CK_AMT` | |
| Entity_S | `ENTITY_S` | genuinely the entity id here — enables per-cycle attribution |

---

## Scraper

`src/pipeline/scrapers/new_jersey.py`. Three stages, all paged through
`_fetch_all()`:

1. **Entity listings** — one paged pass per election year per kind. Cheap:
   two requests-plus-paging per year.
2. **Entity details** — one `GET` per entity. The only source of treasurer,
   mailing city/ZIP and joint-committee linkage; the entity grid carries none
   of it.
3. **Transactions** — one paged pass per entity per relation.

Stages 2 and 3 read entity ids off the CSVs stage 1 wrote, so a
`--transactions`-only run against a state with no entity files warns and
skips rather than failing. A default run does all three in one invocation.

**Manifest** — `(relation_type, year)` at file granularity, with
`relation_type` in `entities` / `pacs` / `details` / `contributions` /
`expenditures`. The current year is always refetched; an explicit
`--start-year` / `--end-year` range always refetches in-range years.

### Cost and performance

Measured from a real 2000–2026 entity sweep:

| | |
|---|---|
| Years covered | 26 (2000–2026) |
| Unique `(name, year)` pairs | **162,100** ← the transaction sweep unit |
| Unique names across all years | 82,132 (so a name recurs ~1.97× across cycles) |
| Transaction requests, full backfill | ~324,200 (2 relations) |

At one request at a time that's **~54 hours**. Three things bring it down:

**1. Concurrency (implemented).** `--workers` (default 8) parallelises **both
per-entity sweeps** — stage 2 (details) and stage 3 (transactions) — through
one shared helper, `_parallel_sweep()`. Each worker gets its own session:
`requests`' `Session` isn't documented as thread-safe and its connection pool
is per-session, so sharing one would both risk races and serialise everything
on a pool of 10. Only the CSV write is locked; the fetch stays fully parallel.
Verified to produce identical output at 1, 8 and 16 workers.

~54 h → **~4 h** at 8 workers. `--workers 1` restores serial behaviour.

**Where the workers actually apply**, since it isn't all of them:

| Stage | Requests (full backfill) | Parallel? |
|---|---|---|
| 1. Entity listings | 52 (paged) | No — and it doesn't matter at 52 |
| 2. Entity details | ~162,100 (1 GET/entity) | **Yes** — this is what `--workers` is for |
| 3. Transactions | ~600 (paged per year) | No, and no longer needs to be — see below |

Stage 2 is now the dominant cost, which is the opposite of where this started.

**2. Skipping the detail sweep — mechanism implemented, but it does NOT
trigger.** Measured 2026-08-16 on a real listing: `Entities20` returns only
its six declared display columns. `first_name`, `middle_initial`, `last_name`,
`suffix`, `non_ind_name` and `entity_type` came back blank in all 5,969 rows,
so the hoped-for shortcut doesn't apply — unlike most server-side DataTables
handlers, ELEC's projects to exactly the requested columns.

`_details_needed()` and the opportunistic columns on `ENTITY_COLS` stay in
place: they cost nothing, and they'll start paying off if ELEC ever widens
that projection. But budget for stage 2 running in full (~162,100 GETs,
≈16 h serial, ≈2 h at 8 workers).

Also note the detail route can return **more than one row per entity** —
6,753 entities for 2000 produced 8,631 detail rows, the `SEQ_NUM > 1` case
for filers tied to more than one committee.

**3. Year-wide transaction sweep (implemented) — the big one.**

The per-entity sweep was pathological, and measuring it is what showed why:

| | Detail lookups | Per-entity contribution queries |
|---|---|---|
| Latency | ~0.53 s | **~6.0 s** |
| Throughput at 8 workers | 15 req/s | **1.3 req/s** |
| Queries returning ≥1 row | — | **3.5%** |

Two compounding problems. The query is ~11× heavier — details are a GET on an
indexed `ENTITY_S`, while transactions filter a multi-million-row table by
`EntityName` *as a string* and DataTables makes the backend compute
`recordsFiltered` (a full COUNT over that same scan) on every request. And
97% of those 6-second queries returned nothing, because most NJ filers are
school-board and fire-district candidates who never filed.

**The endpoints accept a blank `EntityName`.** Verified 2026-08-16: year 2000
returns `recordsFiltered=16,894` in a single query. So the sweep unit is now
the *year*, not the entity — ~4 paged requests per year instead of ~5,229
per-entity queries.

```
year 2000, contributions:  5,229 queries × ~6s  →  4 requests
full backfill:             ~68 h                →  well under 2 h
```

Also tested and **not** available, so don't retry them:

| Attempt | Result |
|---|---|
| `ElectionYears="2000,1999"` | 0 rows — parsed as one invalid year |
| `ElectionYears="1999-2000"` | HTTP 500 |
| `ElectionYears=""` (all years) | HTTP 500 — a year is required |

`_collect_window()` halves the date range whenever a window reports more than
`YEAR_WINDOW_SPLIT_THRESHOLD` (50,000) matches, keeping every query inside
ELEC's 65,000-row comfort zone; a 120,000-row year splits into 6 windows and
28 requests with no truncation. If `DateFrom`/`DateTo` ever stop being
honoured the split is detected as a no-op and it pages straight through with a
warning rather than silently returning a short year.

The per-entity sweep is kept as an automatic fallback, used only if a
year-wide query returns nothing for a year that demonstrably has entities —
the signal that ELEC has started requiring a name again. `--workers` applies
only to that fallback and to stage 2; the year-wide path is a short serial
page-walk.

**Still on the table:** parallelising across the 52 `(relation, year)` tasks.
Each writes its own file so there's no shared state, and the pool already
exists — worth doing if ~2 h is still too slow.

Incremental runs are unaffected by any of this — they only touch the current
year.

**Pagination caveat.** The entity grid is ordered by name, which is not a
unique key — a row can in principle shift between pages mid-sweep. Entity rows
are deduped on `eid` for exactly this reason. Transaction sweeps are
per-entity and almost never exceed one `PAGE_SIZE` page, so the same risk
doesn't meaningfully apply there.

### Flags

Standard set. Two are repurposed because NJ has no separate candidate
registry — a candidate and their committee are one ELEC record:

| Flag | Meaning here |
|---|---|
| `--candidates` | Candidate-side listing only (`NONPACOnly=true`) |
| `--committees` | PAC / party listing only (`NONPACOnly=false`) |

| `--workers N` | Concurrent per-entity transaction fetches (default 8, `1` = serial) |

```bash
python3 src/pipeline/scrapers/new_jersey.py --start-year 2023 --end-year 2023
python3 src/pipeline/scrapers/new_jersey.py --entities --start-year 2025
python3 src/pipeline/scrapers/new_jersey.py --workers 1 --start-year 2025   # serial
python3 src/main.py sync NJ --start-year 2024
```

---

## Parser

`src/pipeline/parsers/new_jersey.py`. Key transformations:

**Entity registry drives everything, joined on name + year.**
`load_entities()` returns two views of the same records: `by_eid` for the
committees/candidates tables, and `by_name_year` keyed on
`(clean_name(name), election_year)` for the transaction join — because that
pair is all a transaction row can be tied back to. `office`, `party` and
`election_year` on a contribution come from there rather than from the row's
own `CAND_NAME`, which is a display string that drifts from the canonical
entity name on joint-committee filings. `CAND_NAME` is the last-resort
fallback when a name+year isn't in the registry, which happens if the
transaction sweep ran against a stale or partial entity file.

Where two eids share a name+year (primary and general), the first wins. They
differ only in `election_type`, which the transaction tables don't carry.

**One overloaded column.** ELEC's Office/Cmte field holds a real office for
candidate filers (`GOVERNOR`, `STATE SENATE`, `MAYOR`, …) and a committee type
for everyone else (`JOINT CANDIDATES CMTE`, `CMTE BALLOT QUESTION`,
`ELECTION RELATED POL CMTE`, `INDEPENDENT EXPENDITURE CMTE (Z)`, `INAUGURAL`,
`OTHER`). `CANDIDATE_OFFICES` is the discriminator. It decides three things:
whether a candidates.csv row is written at all, and whether `candidate_name` /
`office` are populated on that entity's transactions — a PAC's spending must
never be attributed to a candidate.

**Name order.** ELEC files surname-first (`AARON, CHARLES S JR`).
`committee_name` keeps that as-filed form, but `candidate_name` and
`treasurer_name` are flipped to `CHARLES S AARON JR`. This is not cosmetic:
`assign_committee_person_ids` tokenizes on the first and last whitespace-
delimited tokens, which on the raw form yields `AARON,` and `JR` and defeats
both the first+last fallback and nickname expansion. Suffixes are moved past
the surname rather than left to masquerade as a middle initial.

**Addresses** arrive as one free-text line. `split_address()` peels
`CITY, ST ZIP` (or a whitespace-only `CITY ST ZIP`) off the tail and is
deliberately conservative — anything it can't confidently split stays in the
street portion with the structured fields blank. A wrong city is worse than a
missing one, and guessing would manufacture exactly the malformed-ZIP and
unknown-state-code warnings tier 2 exists to catch.

**Location is jurisdiction, not a mailing address.** `25TH LEGISLATIVE
DISTRICT`, `STATEWIDE`, `TOMS RIVER (DOVER TWP)`. Legislative districts are
reduced to a bare number for `candidates.district`; everything else becomes
`candidates.jurisdiction` with district left blank. `committees.city` comes
from the detail sweep's mailing address and never from Location.

**`person_id` model: `committee`.** ELEC mints a new `ENTITY_S` per entity per
election cycle, so a senator who ran in 2019, 2021 and 2023 has three filer
ids. `assign_person_ids` groups on `(state, candidate_name, office, district)`
and takes `min(state_filer_id)`.

### Known limitations

- **No expenditure type.** ELEC publishes no category field separate from the
  free-text Expense Desc, so `transaction_type` and `purpose` both carry that
  string and `expenditure_categories.csv` maps the recognizable descriptions.
  The long tail of vendor-purpose text is left unmapped rather than guessed.
  A bare `_` is ELEC's placeholder for a filer who left the purpose blank.
- **No `amended` flag** is exposed on the transaction grids, so the column is
  blank for every NJ row.
- **`filing_id`** is the filer-supplied check number on expenditures (often
  blank) and is always blank on contributions — ELEC publishes no filing or
  transaction identifier for them.
- **Cycle attribution differs by relation.** Expenditures resolve to an exact
  entity via per-row `ENTITY_S`. Contributions can only be resolved to
  name+year, so a filer's primary and general contributions land under one
  committee row. The entity and candidate tables keep full per-cycle
  granularity via `state_filer_id` regardless.
- **`active`** is not populated. The detail record has a status field but it
  hasn't been sampled across enough entities to map confidently.
- **Joint candidates committees.** `entity_details_*.csv` captures the joint
  committee an entity files through, but the parser doesn't yet write it to
  `affiliated_candidate_name` — the semantics differ from the support/oppose
  model that column was built for. It's in the raw data when someone wants it.

---

## Data Notes

- **`election_year` is CYCLE ATTRIBUTION, not payment year.** ELEC files an
  expenditure against the election it was incurred for, which is frequently
  not the calendar year the cheque was written. Measured across the real data:
  **11.8% of expenditures were paid in a different calendar year than their
  `election_year`**, spread from four years before to four years after.

  Jon Corzine is the clean illustration — he ran for Governor in 2005 and
  2009, so `ElectionYears` buckets all 12,329 of his expenses into those two
  years, while their payment dates spread across 2003–2011:

  | | by payment date | by ElectionYears |
  |---|---|---|
  | 2003 | 1 | 0 |
  | 2005 | 9,342 | 9,373 |
  | 2006 | 374 | 0 |
  | 2009 | 1,801 | 2,956 |
  | **total** | **12,329** | **12,329** |

  Both recover 100% — they are different *groupings*, not different coverage.
  Query `date` for "money spent in calendar 2003"; query `election_year` for
  "money spent on the 2003 election". They are not interchangeable.

- **Sparse early expenditure years are real, not a scraper bug.** Sixteen of
  27 years have fewer than 55% as many expenditures as contributions, and
  2000/2002/2003/2004 have none at all — while the same years hold tens of
  thousands of contributions. Verified three ways: every filter variant
  returns 0; date-range and `ElectionYears` scoping agree exactly on the
  committees that *do* have data; and both recover 100% of a known ground
  truth. ELEC evidently keyed in contribution schedules for years whose
  expense schedules were never digitized.

  **Do not "fix" an empty `expenditures_2000.csv`.** This has been
  re-investigated three times. Confirm with
  `--diagnose-entity "CORZINE, JON S"` before touching anything.

  A caution learned the hard way: `--diagnose-year`'s third column is the
  committee's **all-time** total, not its total for the year under test. A
  committee with 10,789 lifetime expenses and none in 2003 looks, at a glance,
  like proof 2003 has data. It isn't. `--diagnose-entity` breaks a committee
  down year by year against its lifetime total and is the tool to trust.

  The scraper distinguishes an empty year from a broken query rather than
  assuming: when a year-wide query returns nothing it spot-checks
  `EMPTY_YEAR_SPOTCHECK` (50) entities — the busiest contribution recipients
  of that year, since only ~5% of entities file anything and an alphabetical
  sample can miss them all by luck. All empty → recorded as genuinely empty.
  Any hits → the year-wide query is suspect, full per-entity sweep, warning.
- A per-entity query for an entity that didn't run in that year correctly
  returns 0. `BUCCO, ANTHONY M` is a 2020 filer, so probing him against 2001
  returns nothing — that's not evidence of a broken query.
- Negative amounts are normal, not corrupt. Refunds and returned contributions
  post negative, and ELEC's own `$1-$200` summary bucket routinely nets below
  zero. Both parenthesized and leading-minus forms are handled.
- Dates come back as `YYYY-MM-DDT00:00:00` from the JSON API and as
  `MM/DD/YYYY` in some grids; `parse_date()` handles both.
- Contributor and payee names are left surname-first as filed. This is
  consistent within NJ, which is what matters for grouping.
- Pre-2021 reports can't be viewed or downloaded as documents, but their
  itemized contribution and expense rows are still in the search database, so
  the backfill is not limited to 2021+.
- Location values include `NEW JERSEY FEDERAL CANDIDATE` and `OUTSIDE NEW
  JERSEY`. Both are treated as jurisdictions with no district.
- ELEC's own spelling of "Receiver" as `Reciever` on the dashboard summary
  tables is theirs, not a typo here. The transaction grid spells it correctly.
- **The Party column is overloaded too.** For candidate entities it holds a
  real party (`DEMOCRAT`, `REPUBLICAN`, `INDEPENDENT`, `NONPARTISAN`); for PAC
  entities it holds a PAC classification (`LABOR ORG`, `BUSINESS ASSOC`,
  `IDEOLOGIC ORG`, `TRADE ASSOC`, `COMM-NOT QUALIFIED`). No mapping is needed
  in `parties.csv` — the four real values already pass through `canonical_party`
  unchanged, and the classifications never reach a party column because
  `committees` has no party field and PAC entities never produce a
  `candidates` row. If a future change starts writing party for committees,
  this needs revisiting.

### Row counts by election year (scraped 2026-08-16)

Recorded so the sparse early expenditure years are not mistaken for a
regression on a future run. `election_year` is cycle attribution — see
Data Notes.

| Year | Contributions | Expenditures |
|---|---|---|
| 2000 | 16,894 | 0 |
| 2001 | 60,044 | 4,412 |
| 2002 | 16,321 | 0 |
| 2003 | 46,014 | 0 |
| 2004 | 14,659 | 0 |
| 2005 | 43,043 | 15,769 |
| 2006 | 8,670 | 130 |
| 2007 | 44,759 | 18,575 |
| 2008 | 7,770 | 600 |
| 2009 | 55,218 | 27,335 |
| 2010 | 42,981 | 4,819 |
| 2011 | 43,617 | 45,625 |
| 2012 | 23,309 | 3,280 |
| 2013 | 53,894 | 35,365 |
| 2014 | 33,275 | 5,881 |
| 2015 | 42,191 | 20,527 |
| 2016 | 28,365 | 6,925 |
| 2017 | 61,154 | 37,537 |
| 2018 | 37,465 | 10,290 |
| 2019 | 42,198 | 47,043 |
| 2020 | 30,101 | 25,584 |
| 2021 | 93,923 | 92,405 |
| 2022 | 71,523 | 61,237 |
| 2023 | 113,316 | 95,110 |
| 2024 | 103,665 | 57,823 |
| 2025 | 166,798 | 120,758 |
| 2026 | 47,919 | 44,954 |
| **total** | **1,349,086** | **781,984** |

---

## Last Updated

| Component | Date | Note |
|---|---|---|
| Scraper | 2026-08-16 | All four routes verified. Year-wide scoping for contributions; expenditure coverage verified complete against ground truth |
| Parser | 2026-08-16 | Verified end-to-end against fixtures through validate → tabulate → aggregate, including the two-eids-one-name-year collapse case |
| Aliases | 2026-08-16 | committee / contributor / transaction / expenditure / office types |
| Docs | 2026-08-16 | |
