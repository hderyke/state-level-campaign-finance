# Wisconsin — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Wisconsin (WI) |
| **Source** | [Sunshine — Wisconsin Campaign Finance](https://campaignfinance.wi.gov) (Wisconsin Ethics Commission) |
| **Access method** | JSON-parameterized CSV endpoints behind the site's "Download results" button (`/api/data-download/…`), plain HTTP GET, no authentication |
| **Coverage** | 2008 – present by default (source holds registrations back to 1978; `--start-year` reaches further back) |
| **person_id model** | `person` — `Registrant ID` is stable across election cycles |

---

## Raw Data Structure

Three relations, all delivered as CSV from the same API shape:

```
GET /api/data-download/transactions?queryParams={"dateFrom":"…","dateTo":"…"}
GET /api/data-download/reports?queryParams={"dateFrom":"…","dateTo":"…"}
GET /api/data-download/committees?queryParams={}
```

`queryParams` is URL-encoded JSON mirroring the browse-page filter sidebar. The
pipeline uses `dateFrom` / `dateTo` (ISO timestamps, **inclusive on both ends**)
and, only when a single day is too large, `amountFrom` / `amountTo`.

**Every download is truncated at 99,999 rows**, silently — a capped response is
indistinguishable from a complete one except by counting. With ~13.1M
transactions statewide, this drives the whole scraper design.

### `transactions_{from}_{to}[_amt{lo}-{hi}].csv`

One file per date window. 61 columns; contributions and disbursements share the
table, with the filer on whichever side applies.

| Field | Description |
|---|---|
| `ID` | Transaction ID (unique per transaction) |
| `Date` | Transaction date (MM/DD/YYYY) |
| `Amount` | Dollar amount |
| `Comment` | Free-text note |
| `Contributor Entity ID (-> Related Payer Entity ID if applicable)` | Entity ID of the paying side — **a different ID space from `Registrant ID`** |
| `Contributor Name (…)` / `Contributor Entity Type` | Payer name and type (`Individual`, `Business`, `Registrant`, …) |
| `Contributor Address 1/2`, `City`, `State`, `Zip`, `Country` | Payer address; `State` is sometimes `WI`, sometimes `Wisconsin` |
| `Contributor Occupation` | Occupation (individuals only; no employer field exists) |
| `Payee Entity ID`, `Payee Name`, `Payee Entity Type`, address block | Receiving side |
| `Transaction Type ID` / `Transaction Type` | 1 = Contribution, 2 = Disbursement (routing discriminator) |
| `Transaction Category ID` / `Transaction Category` | `Monetary`, `In-Kind`, `Independent Expenditure`, … |
| `Transaction Purpose ID` / `Transaction Purpose` | Spending purpose (`Media - Online Advertising`, `Office Rent`, …) |
| `Related Entity …` | Committee supported or opposed by an independent expenditure |
| `Final Recipient …` | Ultimate recipient for conduit / pass-through transactions |
| `Communication Date`, `Support Stance` | IE communication date and `For` / `Against` |
| `Related Ballot Event ID / Name / Date` | Election the transaction was reported against |
| `Related Office`, `Related District`, `Related Branch` | Office context — the only source of office/district for candidates |
| `Registrant ID`, `Registrant Name`, `Registrant Address`, `Registrant Type`, `Registrant Party` | The filing committee |
| `Reports` | Report(s) the transaction was filed on, e.g. `2025 July Continuing (ID: 7946)` |

#### `Individual` vs `Registrant` entity types

`Contributor Entity Type` / `Payee Entity Type` mix two different ideas:

- **`Individual`** — a natural person with no filing obligation, i.e. a donor.
  These are the only rows that carry `Contributor Occupation`.
- **`Registrant`** — the counterparty is *itself a filer* registered with the
  Ethics Commission. It's a status, not a kind of entity: behind it could be a
  candidate committee, a county party, a labor PAC, a conduit or an IE
  committee. A registrant-to-registrant row is money moving between committees
  (a party transferring to a candidate, a PAC making a contribution).

How each side of the pipeline handles it:

- **Scraper** — no effect. Entity type isn't a filter axis; windows are cut on
  date (and amount), so both kinds of row arrive in the same chunk.
- **Parser, routing** — on a row where the registrant is the payer, Sunshine
  still fills the *contributor* columns with the registrant's own name and
  address. So `contributor_name` on a disbursement is the spender, not a donor,
  and routing has to happen before any of those columns are read as donor data.
  The fallback direction test ("is the registrant the payer?") therefore matches
  on **name**, not ID — see the two-ID-spaces note below.
- **Parser, contributor_type** — the raw value is passed through as-is;
  `src/aliases/contributor_types.csv` maps `Individual` → `Individual` but leaves
  `WI,Registrant` **blank on purpose**. Mapping it to `Organization` or `PAC`
  would be wrong a large share of the time (a candidate committee giving to
  another candidate committee is neither). Code that needs that breakdown should
  join to `committees.committee_type`, which is where the distinction really
  lives.

### `committees.csv`

The "registrants" tab — every filer, in one unfiltered download (~10k rows).

| Field | Description |
|---|---|
| `Registrant ID` | Stable filer ID (e.g. `0106162`) |
| `Registrant Name` | Committee name |
| `Registrant Address` | Multi-line block: `street\ncity, State zip, Country` |
| `Registrant Email` / `Registrant Phone` | Contact info |
| `Registrant Registration date` | Registration date (party committees read `01/01/1978`) |
| `Registrant Type` | Hierarchical type, e.g. `PAC  -> Labor`, `State Candidate  -> Personal Campaign Committee` |
| `Candidate Name` | Candidate behind the committee, or `N/A` |
| `Registrant Party` | Party, or `-` |
| `Registrant Status` | `Registered` / `Terminated` → `active` |
| `Ballot events` | Newline-joined list of every seat the registrant has been on a ballot for: `2026 Fall General (Date: 11/03/2026, Type: Election) - State Assembly / State Assembly, District No. 92`. **The only source of office, district and jurisdiction for candidates** — see the parser notes below. Referendum events stop after the parenthesis and carry no office path. |
| `Reports submitted`, `Exempt`, `Has a segregated fund` | Additional registration detail (unused) |

### `reports_{from}_{to}.csv`

The report index (~130k rows total). On this endpoint `dateFrom`/`dateTo` filter
the report's **updated-at** date, so a window holds every report touched in that
period.

| Field | Description |
|---|---|
| `ID` | Report ID — matches the `(ID: …)` in a transaction's `Reports` cell |
| `Updated at` | Last update date (what the date filter matches) |
| `Report name`, `Filing period`, `Period start`, `Period end` | Filing period |
| `Registrant …` | Same registrant block as `committees.csv` |
| `Number of transactions`, `Number of submissions` | Counts |
| `Amended` | `Yes` / `No` → the `amended` flag on transactions |

---

## Scraper

`src/pipeline/scrapers/wisconsin.py`

**Transactions and reports** are pulled in date windows, starting from calendar
months. Every response is row-counted with `csv.reader` (line counting is wrong —
address fields are quoted and multi-line). Any window that returns exactly
99,999 rows is discarded and split: month → halves → … → single day. A single
day still over the cap is sliced by `amountFrom`/`amountTo` bands. Windows and
bands are disjoint by construction, so chunks concatenate at parse time without
deduplicating 13M IDs.

**Committees** is one unfiltered call, re-fetched every run.

**Manifest** (`data/Wisconsin/manifest.csv`) records one row per chunk —
relation, window, amount band, filename, row count, and a `truncated` flag for
the pathological case where even an amount band hits the cap. Because chunk
filenames encode their window, anything dropped from the manifest is also deleted
from `raw/`: leaving a wider, superseded file behind would double-count its rows.
Windows in the current year are always re-fetched; a bare run fills manifest gaps
only.

**Limitations:**
- Cloudflare fronts the site. A browser `User-Agent` (from `config.py`) is
  normally enough; if 403s start, export a cookie: `WI_COOKIE="cf_clearance=…"`.
  Responses are content-type checked, so a challenge page fails the download
  rather than landing in `raw/` as data.
- The row cap is undocumented and detected only by count. If the Ethics
  Commission raises or lowers it, update `ROW_CAP`.
- `--contributions` / `--expenditures` are accepted but both mean
  `--transactions`: the source publishes one combined feed.

**Expected runtime:** full 2008–present backfill is ~1,500–3,000 requests
(several hours at the 0.4s inter-request delay, longer where election-season
months split down to days). Incremental runs are ~25 requests.

---

## Parser

`src/pipeline/parsers/wisconsin.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`,
`candidates.csv.gz`, `committees.csv.gz`, `loans_debts.csv.gz`

**Key transformations:**
- **Routing** — one feed, two directions. `Transaction Type` decides:
  contribution-ish → contributions, disbursement-ish → expenditures,
  loan/debt/obligation → loans_debts. Any type the source adds later falls back
  to "is the registrant the payer?", matched on *name* (the Contributor/Payee
  columns use entity IDs from a different ID space than `Registrant ID`).
  Types that route nowhere are counted and warned, never guessed silently.
- `committee_name` is always `Registrant Name`, whichever side of the
  transaction the filer sits on.
- Independent expenditures: `Support Stance` and `Related Entity` are folded
  into `purpose` (`[For Friends of David Liners]`) because the aggregate DB
  drops `category`.
- `filing_id` is the first report ID in the `Reports` cell; `amended` comes from
  joining that ID against the report index (blank when no `reports_*.csv` exist).
- `election_year` prefers the related ballot event date, then a leading 4-digit
  year in the event name, then the transaction date.
- Contributor/payee `State` accepts `WI` or `Wisconsin`; full names are mapped
  to two-letter codes via `src/aliases/states.csv`.
- **Office, district and jurisdiction** come from the `Ballot events` cell in
  `committees.csv`, which lists every seat a registrant has been on a ballot
  for. The registrant is described by its most recent event that names an
  office. The office path is a `" / "`-separated hierarchy — level 1 is the
  office, the deepest level is the specific seat, level 2 names the county for
  county-scoped offices — and `office_path_parts()` splits it:

  | Path | office | district | jurisdiction |
  |---|---|---|---|
  | `Governor` | Governor | | Statewide |
  | `State Assembly / State Assembly, District No. 92` | State Assembly | 92 | Statewide |
  | `Circuit Court / Sauk County Circuit Court / Sauk County Circuit Court, Branch 03` | Circuit Court | Branch 3 | Sauk County |
  | `District Attorney / Dane County District Attorney` | District Attorney | | Dane County |
  | `Court of Appeals / Court of Appeals, District 04` | Court of Appeals | 4 | |
  | `Municipal Judge / Municipal Judge (450301)` | Municipal Judge | | |

  A circuit-court `district` keeps the word `Branch` because a bare number
  there would read as a legislative district and mean something else.
  `jurisdiction` is `Statewide` for the offices in `STATEWIDE_OFFICES`, the
  county wherever the path names one, and blank otherwise — a Court of Appeals
  district spans several counties without being statewide, and a Municipal
  Judge seat is identified by a bare numeric municipality code rather than a
  place name. This resolves an office for **99.9% of candidate registrants**.
- The transaction pass still harvests the same fields (`Related Office` is
  level 1, `Related District` level 2, `Related Branch` level 3) as a fallback
  for registrants with an empty `Ballot events` cell. The path is reassembled
  and run through the same `office_path_parts()`, so both sources produce
  identical shapes. This matters: for single-seat offices the source repeats
  the office name in all three columns, so copying `Related District` straight
  through used to write `Governor` into `candidates.district`.
- On contributions and expenditures, `office` falls back to the registrant's
  own seat when the row itself names no ballot event — which most routine
  receipts don't. Non-candidate filers (PACs, parties, conduits) keep a blank
  `office`; they aren't seeking one.
- `treasurer_name` is always blank — see the note below.
- `Registrant Type` whitespace is collapsed to a single `X -> Y` form;
  canonicalization happens at aggregate time.
- Registrants that appear in transactions but not in `committees.csv`
  (terminated before the list was generated) are written as committee rows with
  name and type only, and reported via `enrichment_summary`.

**person_id model:** `person` — `Registrant ID` follows a filer across cycles
(county parties still carry their 1978 registration), so the source ID is the
person key. `assign_committee_person_ids` then links committees to candidates by
name.

**Limitations:**
- No cross-chunk deduplication, by design (an ID set over 13M rows costs ~1 GB).
  Correctness depends on the scraper's windows being disjoint; the parser checks
  the ranges encoded in chunk filenames and warns on overlap, which only happens
  if a stale file survives an interrupted run. Fix with `--force` for the
  affected years.
- `employer` is always blank — Sunshine collects occupation only.
- `treasurer_name` is always blank, deliberately. It's the one field on the
  per-registrant detail page (`/browse-data/registrant/{id}`) that isn't already
  in the registrant list — everything else that page shows, including the
  election events, is in `committees.csv`. Collecting it would cost one page
  load per registrant (~10.3k, and `robots.txt` sets `Crawl-delay: 10`, so ~28
  hours) against an *undocumented internal API*: Sunshine is a client-rendered
  SPA, so every route returns a loading shell, and the detail page is keyed on a
  surrogate ID (`12389010`) that has no derivable relationship to the registrant
  ID (`01072987`) — you'd have to scrape the list API first just to learn the
  surrogate IDs. That's two undocumented endpoints and the repo's most
  breakage-prone scraper, for a column no join, alias table or query in this
  project reads, and which ~13 other states also leave blank. If it's ever
  wanted, scope the crawl to `active` registrants (1,799 rows, not 10,283) — a
  treasurer for a committee terminated in 2003 is worth little.
- `incumbent` is always blank — Sunshine publishes no incumbency flag anywhere,
  on the registrant list or the detail page.
- `candidates.district` is legitimately blank for single-seat offices
  (Governor, Supreme Court, a county DA, Municipal Judge). A low fill rate on
  that column is the office mix, not missing data.

### Registrant types and the committees/candidates split

Every registrant is a **filer**, so every registrant gets a `committees.csv.gz`
row — candidates included. A candidate registrant *additionally* gets a
`candidates.csv.gz` row. This is the project-wide convention (see
`parsers/texas.py`, `parsers/maryland.py`, `parsers/maine.py`) and it is load
bearing: `committee_name` on every WI transaction is the registrant name, and
`aggregate.py` NULLs out `candidate_name` on any contribution or expenditure
whose `committee_name` doesn't match a known candidate committee. Dropping
candidate registrants from `committees` would silently blank `candidate_name`
across all 12.7M WI transactions.

`State Candidate` and `State Candidate  -> Personal Campaign Committee` are the
**same entity type recorded under two different registration flows**, not two
kinds of filer:

| Type | Rows | Registration dates | Has candidate name |
|---|---|---|---|
| `State Candidate  -> Personal Campaign Committee` | 5,827 | 1978–2019 | 5,816 |
| `State Candidate` | 1,262 | 2019–present | 1,262 |

The subtype was dropped when Sunshine replaced the legacy CFIS system in 2019 —
the split is chronological, with essentially no overlap. Both map to
`Candidate Committee` in `src/aliases/committee_types.csv`, which is in
`aggregate.py`'s `CANDIDATE_COMMITTEE_TYPES`, so they behave identically
downstream. `State Candidate  -> Support Committee` (3 rows) is a third-party
committee supporting a named candidate and also maps to `Candidate Committee`.
- `incumbent` and `jurisdiction` are always blank.

---

## Data Notes

- **99,999-row cap, silently applied** — the single most important quirk of this
  source. Any tool that hits these endpoints without counting rows will produce
  quietly incomplete data.
- **Two ID spaces** — `Contributor Entity ID` / `Payee Entity ID` are entity IDs;
  `Registrant ID` is the filer ID. The same committee is entity `16226` and
  registrant `0106162`. They are not comparable.
- **Redacted registrants** — some address/email/phone fields read
  `Redacted pursuant to Wis. Stat. § 19.55(2)(cm)2.` (judicial candidates,
  mainly). Treated as blank.
- **`-` and `N/A` as null** — used throughout for party, candidate name, purpose.
- **Malformed addresses** — a minority of address blocks don't parse
  (`MADISON, CA WI2428`); city and ZIP are left blank rather than guessed.
- **Hierarchical registrant types** — `PAC  -> Labor` etc. carry two levels with
  irregular internal spacing; both levels are mapped in
  `src/aliases/committee_types.csv`.
- **Conduits** — Wisconsin's conduit filers pass earmarked individual money
  through to candidates, and those transactions carry a `Final Recipient` block.
  The parser keeps the conduit as `committee_name`; the final recipient is not
  yet broken out into its own row.
- **Dates are inclusive on both ends** of the API's date filter — the scraper's
  windows are built to be non-overlapping given that.
- **Reports filter on updated-at**, not on filing period, which is why an
  incremental reports pull picks up amendments to old periods.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-28 |
| Parser | 2026-07-28 |
| Documentation | 2026-07-28 |
