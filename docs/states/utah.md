# Utah — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Utah (UT) |
| **Source** | [Lieutenant Governor's Financial Disclosures](https://disclosures.utah.gov/) — `/Search/AdvancedSearch` |
| **Access method** | Plain HTTP (GET for bulk CSVs, form POST for the entity roster) via `curl_cffi` browser TLS impersonation. **No Playwright.** |
| **Coverage** | Report years 1998 – present in the site's dropdown; itemized data is thin before ~2008 |
| **person_id model** | `committee` — a Utah candidate folder is per-**candidacy**, not per-person (see [Parser](#parser)) |
| **Party source** | External overlay: Open States + vote.utah.gov canvasses (`--party`) |

**Status: verified end-to-end against the live site. Full pipeline passes validation on real data.**

Real run, end to end: all 232 (type, year) combinations downloaded (121 non-empty, 191 MB), roster swept (4,463 entities), party overlay applied from both sources, **928,231 contributions / 357,764 expenditures / 7,251 loans / 4,477 committees / 2,413 candidates**, validator **PASS**, tabulate OK.

Filer resolution **97.8%**. Party filled on **909 of 2,413 candidates (37.7%)** — 796 from the canvasses, 113 from Open States; 564 `exact` / 345 `high`.

| Component | Status | Evidence |
|---|---|---|
| `GenerateReport?ReportYear=&EntityType=` bulk export | ✅ **Verified** | 232/232 combinations fetched on a real run |
| Bulk CSV header shape | ✅ **Verified** | Real header is `FILED,PCC,REPORT,…`; `_header_map()` anchors on `REPORT`, entity name at index 1 |
| HTML-instead-of-CSV on an empty combination | ✅ **Verified** | ~111 empty combinations detected correctly by `Content-Type` |
| Quote-repair on real data | ✅ **Verified** | 285 malformed rows skipped out of 1,205,495 — 0.02% |
| Filer names in the transaction export | ✅ **Verified — bare, never annotated** | 0 of 358 distinct filers in `transactions_pcc_2024.csv` carry `(YYYY Office-District)`. Every candidate filer goes through the base-name fallback in `Roster.resolve()` |
| `PURPOSE` vocabulary | ✅ **Verified — free text, not the statutory list** | See [below](#purpose-is-free-text-not-a-controlled-vocabulary); design corrected |
| `curl (56)` cause | ✅ **Diagnosed** | ~33-request keep-alive limit; see [below](#curl-56-a-keep-alive-limit-not-rate-limiting) |
| `ReportYear` does not filter the roster grid | ✅ **Verified** | Two independent proofs, see [Entity roster](#the-three-source-endpoints) |
| Roster grid parsing, 25 rows/page, ids, `[CLOSED]`, the annotation | ✅ Parsed real markup | `_parse_roster_page()` run against a saved live results page |
| Roster sweep | ✅ **Verified** | 4,463 entities; 2,420 annotated candidacies; 548 with `(aka. …)` aliases |
| Filer resolution | ✅ **97.8%** | Alias + annotation reconciliation took it from 60.2%; ceiling is source-imposed, see below |
| Full pipeline | ✅ **PASS** | parse → validate → tabulate on 1.29M real rows |
| TLS via `config.ca_bundle()` | ✅ Verified on a TLS-inspecting network | Fixed a `curl (60)` that failed every request |
| Canvass workbook layout | ✅ **Verified and rewritten** | County matrix, candidates in the header row. All 16 real workbooks extract: **1,915 rows** where the original heuristic managed 186 |
| Party overlay applied | ✅ **Verified** | 909 of 2,413 candidates (37.7%); spot-checked against real races |

**Remaining gap:** the 62.3% of candidates without a party are mostly cycles the Excel canvasses don't cover — 2002/2004/2006 and 2022/2024/2026 are PDF-only — plus county and municipal races the statewide canvasses never list. Parsing the PDF canvasses is the obvious next lever, and 2022/2024 would be the highest-value two.

---

## Raw Data Structure

`data/Utah/raw/`

| File | Written by | Content |
|---|---|---|
| `transactions_{slug}_{year}.csv` | `run()` | Every itemized contribution **and** expenditure filed by every entity of that type in that report year. `slug` ∈ `pcc, pac, pic, party, corp, labor, elect, indexp` |
| `entities.csv` | `run()` | Accumulated entity roster: `entity_id, entity_name, entity_type, entity_type_label, active, ending_balance, data_years` |
| `OpenStates_People.csv` | `run_party()` | `openstates_id, name, given_name, family_name, party, chamber, district, entity_id` |
| `UT_ElectionResults.csv` | `run_party()` | `election_year, stage, race_raw, office, district, candidate_name, party, source_file` |
| `canvass/canvass_{year}_{stage}_{hash}.xlsx\|xls` | `run_party()` | The downloaded canvass workbooks, kept so the extractor can be improved without re-downloading. The URL hash is in the name because one election's list item can link more than one workbook |

### The three source endpoints

Everything hangs off one ASP.NET MVC controller. No login, no ViewState, no anti-forgery token.

**1. Bulk transaction export** — the workhorse:

```
GET /Search/AdvancedSearch/GenerateReport?ReportYear=2020&EntityType=PAC
```

8 entity types × 1998–present ≈ 230 combinations, roughly 100 with data. The site's own UI never links this form — it only links the per-entity variant — which is why it needed a third-party source to confirm.

A combination with no data returns **an HTML page with status 200**, not an empty CSV or a 404. `_is_csv_response()` discriminates on `Content-Type` (`application/csv`), falling back to sniffing for a doctype. Empty combinations are recorded in the manifest with `rows=0` so the next incremental run doesn't re-ask.

**2. Per-entity export** — linked from the results grid as "Download Data by Year":

```
GET /Search/AdvancedSearch/GenerateReport/484?ReportYear=2014
```

Same columns minus the leading `FILED`. Not used for bulk collection, but it is the right tool for spot-checking one committee. There is also a per-*filing* variant, `/Search/PublicSearch/CSVDownload/{filing_id}?year=YYYY`, reachable from a folder's detail page — strictly narrower again, unused.

**3. Entity roster** — the search grid as an HTML partial:

```
POST /Search/AdvancedSearch/GetEntityReportList
Search=&EntityType=PAC&ReportYear=2020&HideContributions=false
&HideExpenditures=false&PageNumber=1&X-Requested-With=XMLHttpRequest
```

`X-Requested-With` goes in **both** the header and the form body, because the site's own JS does that. 25 rows per page; one row per filer, carrying the entity name (linked to `/Search/PublicSearch/FolderDetails/{entity_id}`), the type label, per-report filed marks, the ending balance, and a cell listing every year that entity has downloadable data for.

This roster is the only place `entity_id` appears — and, as it turns out, the only place office, district and election cycle appear either.

**`ReportYear` does not filter this grid.** It only selects which report-period columns are displayed. Proven twice:

- A saved results page whose period columns are dated `4/16/18` — so, a 2018 search — lists `1 Powerful Voice`, whose only downloadable year is 2026, and `Citizens For PARAT`, whose only year is 2014.
- A real per-year sweep ran to page 69 for 1998, page 70 for 1999 and page 69 for 2000: the same result set three times, with the connection resets landing on the same page numbers each pass.

So the scraper sweeps **once per entity type**, not once per (type, year) — ~560 requests instead of ~16,000 for identical data. Each row already carries that entity's full year coverage, so nothing is lost. The original per-year design is what tripped the source's limits; if you see the sweep iterating years, something has regressed.

### Transaction CSV columns

Bulk (`EntityType`) shape:

```
FILED,<TYPE>,REPORT,TRAN_ID,TRAN_TYPE,TRAN_DATE,TRAN_AMT,INKIND,LOAN,
AMENDS,NAME,PURPOSE,ADDRESS1,ADDRESS2,CITY,STATE,ZIP,INKIND_COMMENTS
```

Column 2's **header name changes with the entity type** (`PAC`, `PIC`, `CORPORATION`, …); its value is always the filing entity's name. The per-entity shape drops `FILED`, putting the entity name in column 1. `_header_map()` anchors on `REPORT` and takes the column immediately to its left, so both shapes parse without hardcoding either.

| Column | Meaning | Mapped to |
|---|---|---|
| `FILED` | `X` = appears on a filed report; blank = sitting unfiled in the ledger | Not stored (rows kept either way) |
| `<TYPE>` | Filing entity name | `committee_name` (via roster resolution) |
| `REPORT` | Reporting period — "Convention", "Primary", "General", "Year End", "August 31st", … (15 distinct) | Not stored |
| `TRAN_TYPE` | `Contribution` or `Expenditure` — the only routing key | table selection |
| `TRAN_DATE` | `M/D/YYYY` | `date` |
| `TRAN_AMT` | May be negative or parenthesised | `amount` |
| `INKIND` | `X` or blank | folded into `transaction_type` |
| `LOAN` | `X` or blank | diverts the row to `loans_debts` |
| `AMENDS` | the `TRAN_ID` this row amends | `amended` = 1 when non-blank |
| `NAME` | counterparty — contributor or payee, never the filer | `contributor_name` / `payee_name` |
| `PURPOSE` | On expenditures, Utah's statutory category. ~always blank on contributions | `category` **and** `transaction_type` |
| `ADDRESS1/2` | street address | dropped — no such column in the canonical schema |
| `CITY/STATE/ZIP` | counterparty location | `*_city/_state/_zip` |
| `INKIND_COMMENTS` | free-text in-kind description | `expenditures.purpose` |

#### `PURPOSE` is free text, not a controlled vocabulary

The LG's quick guides state that expenditures "must fall within one of the following categories" — *advertising; association expense; campaign expense; constituent services; donations; loans; office; political support; return of a contribution; signature gathering; supplies; travel expenses;* or *other* with a description. **The exported data does not honour that.** Measured across four real PAC files, the most common `PURPOSE` values are:

```
1,652  'Campaign Contribution'        407  'Processing fee'
1,549  'Contribution'                 406  'Political contribution'
  555  'POLITICAL CONTRIBUTION'       387  'Donation'
  535  'contribution'                 222  'Campaign contribution'
  482  ''                             201  'Campaign'
  172  'Candidate Financial Assistance'   158  'Office Expenses'
  129  'Contribution to Candidate'    117  'Campaign Donation'
  103  'Consulting'                   100  'CONTRIBUTATION'   (sic)
```

Filer-typed free text: hundreds of spellings, arbitrary case, typos, blanks. Not one of the statutory labels appears in the top twenty.

This is why the parser does **not** write `PURPOSE` into `transaction_type`. No finite `(state, raw)` alias list can cover free text, and an unmapped row yields a `NULL` `transaction_category`, which is worse than a coarse-but-correct value on every row. Instead:

- `category` gets the raw string, verbatim, preserved in the per-state DB.
- `transaction_type` gets a **derived** label from a controlled set of four, which `expenditure_categories.csv` maps exhaustively.

The one distinction worth rescuing from the text is contribution-versus-operating-spend — a committee's donation to a candidate is a categorically different act from buying printing, and it is by far the most common thing these strings say. `expenditure_type()` matches on the stem `contribut|donat`, so `Contribution`, `POLITICAL CONTRIBUTION`, `Campaign Donation` and `CONTRIBUTATION` all land together. The `INKIND` flag wins over the text reading, being a hard flag rather than an inference.

Known limitation: `'Candidate Financial Assistance'` is substantively a contribution but doesn't match the stem, so it reads as `Monetary`. Widening the pattern to catch it would start pulling in operating spend, so it's left alone — the raw string is in `category` for anyone who needs to do better.

### The `(YYYY Office-District)` annotation

Every "Candidates & Office Holders" folder is named:

```
Abbott, Nelson (2022 House-57)
Aagard, Doug (2008 House-15)      [CLOSED]
Aalders, Tim (2012 Lieutenant Governor)
```

**This is the only place Utah publishes a candidate's office, district or election cycle** — none of it is in the transaction export. `parse_candidacy()` reads all three off it. Organization folders reuse the same bracket syntax for aliases (`4Life Research USA (aka. 4Life)`, `AARP Utah (aka. AARP Utah)`), so the split only fires when the group opens with a four-digit year, and only for PCC entities.

`[CLOSED]` on a name means the committee is dissolved. The marker is stripped from `committee_name` (so it still joins against transaction files that predate the closure) and recorded as `active=0`.

---

## Scraper

`src/pipeline/scrapers/utah.py`

Two independent passes:

**Bulk transactions** — iterate the 8 entity types × the report years in scope, GET each `GenerateReport` combination, write the response bytes verbatim. Manifest keyed on filename; the current year is always re-fetched.

**Entity roster** — for each (type, year), page `GetEntityReportList` until a page returns fewer than 25 rows, folding rows into an accumulated `entities.csv` that unions each entity's year sets. Merged into whatever is already on disk, so a run narrowed by `--start-year` doesn't drop older filers.

Files are written **byte-for-byte as received**. Utah's export doesn't escape quotes inside fields, but that is repaired at parse time so raw stays raw.

### `curl (56)`: a keep-alive limit, not rate limiting

```
curl: (56) Recv failure: Connection was reset
```

The server accepts the request, begins responding, then cuts the connection. Two runs' worth of evidence identify the cause precisely: on a roster sweep the resets landed at pages **4, 37 and 69** of one pass and **5, 38** of the next — a 32–33 request stride, repeating. That regularity is an IIS/F5 `MaxKeepAliveRequests` limit closing the connection after ~33 requests, not a rate or volume threshold. libcurl surfaces that close as an error rather than reconnecting transparently.

So the fix is to reconnect *before* the server forces it, rather than treating predictable server behaviour as a failure to retry through:

| Mechanism | Value | Why |
|---|---|---|
| `SESSION_MAX_REQUESTS` | 25 | `RecyclingSession` reconnects below the ~33 limit, so the reset never happens |
| `DOWNLOAD_RETRIES` | 4 | For the resets that still slip through |
| `DOWNLOAD_BACKOFF` | 15 s × attempt | Backs off rather than hammering |
| `DOWNLOAD_PAUSE` | 1.5 s | Raised from 0.5 s |
| `MAX_CONSECUTIVE_ERRORS` | 4 | Stops instead of grinding through the rest |

`RecyclingSession` wraps the curl_cffi session and proxies `.get`/`.post`, rebuilding the underlying handle every 25 requests and on `recycle()`. Wrapping rather than returning a fresh session from every helper is deliberate: an explicit "remember to reassign the session" contract invites exactly the bug it's meant to prevent, since libcurl pools connections and a reset one keeps failing every request made through it. That pooling is why the very first run showed *no* recovery after its first error — every subsequent request reused the dead connection.

A first reset is logged at debug, not warning: it's routine. Only a retry that also fails escalates.

Residual risk: the bulk CSVs are **generated per request** — `GenerateReport` runs a query and builds the file — so a big election year takes real server-side time and may still time out independently of keep-alive. If it fails at the *same* file every time regardless of pacing, that's this, not the connection limit.

**Recovery is a re-run.** Successful downloads are already in `data/Utah/manifest.csv`, so a plain re-run (no year flags) skips everything that landed and fetches only the gaps. Verified: a resumed run skipped 37 files and re-fetched only the missing ones. Note that `--start-year`/`--end-year` deliberately force a re-download of everything in range, so **don't** use them to resume — use no year flags at all.

If it recurs immediately on a fresh run, slow it down rather than retrying harder:

```bash
python3 src/pipeline/scrapers/utah.py --pause 5                        # wider gap
python3 src/pipeline/scrapers/utah.py --start-year 2015 --end-year 2018 # smaller bites
python3 src/pipeline/scrapers/utah.py --entities                        # roster alone
```

`--pause` is local to this scraper and not part of orc.py's forwarded flag set. If it fails at the *same* file every time regardless of pacing, that file is the problem rather than the pacing — check whether that (type, year) is unusually large or simply times out being generated.

### TLS: two different problems, don't confuse them

The scraper builds both its sessions with `verify=config.ca_bundle()`. On Windows that exports the OS trust store — the same one the browser uses — to a PEM and hands libcurl its path; on macOS/Linux it returns `True`, i.e. ordinary certifi verification. It is never `verify=False`.

Without it, any machine behind a TLS-inspecting proxy (Zscaler, Netskope, a corporate appliance) fails **every** request with:

```
curl: (60) SSL certificate ... unable to get local issuer certificate (20)
```

That is a *verification* failure, not the WAF, and the two are easy to mix up because both look like "the site is blocking me":

| Symptom | Cause | Fix |
|---|---|---|
| `curl (60)`, certificate/verify in the message | Internal CA in the OS store but not in certifi's bundle | `config.ca_bundle()` (already wired). If it still fails, rebuild it — see below |
| HTML challenge page instead of CSV, on every request | F5 BIG-IP ASM fingerprinting the TLS handshake | Bump `IMPERSONATE` to a newer Chrome build |

Bumping `IMPERSONATE` will not fix a curl (60), and no CA bundle will fix a WAF challenge.

A certificate error is a property of the machine and its network, not of the file being fetched, so the scraper **stops on the first one** rather than producing the same message 232 times. It prints the remediation steps — rebuild the cached bundle with `config.ca_bundle(refresh=True)`, ask `config.diagnose_tls()` which `verify=` actually works here, or point `REQUESTS_CA_BUNDLE` at your organisation's own PEM, which `config.ca_bundle()` honours ahead of everything else.

### Why `curl_cffi`

The captured request carries `TSPD_101`, `TSPD_101_DID` and several `TS<hex>` cookies — F5 BIG-IP ASM's fingerprint-and-challenge set. ASM fingerprints the TLS/HTTP2 handshake, not just headers, so plain `requests` is identifiable as non-browser no matter what User-Agent it sends. `curl_cffi` wraps a patched libcurl that reproduces a real Chrome ClientHello under the same requests-style API — the same fix `scrapers/ohio.py` and `scrapers/new_hampshire.py` already use. It is **not** a browser: no JS executes, and Playwright is deliberately not used.

`_session()` also GETs `/Search/AdvancedSearch` once before any POST, so the ASM cookie jar is primed and the first POST doesn't arrive out of nowhere.

If a run comes back with HTML challenge pages on every request, bump `IMPERSONATE` to a newer Chrome build before reaching for anything heavier.

### Flags

```bash
python3 src/pipeline/scrapers/utah.py                          # bulk + roster, incremental
python3 src/pipeline/scrapers/utah.py --force                   # re-download everything
python3 src/pipeline/scrapers/utah.py --start-year 2020         # 2020 onwards
python3 src/pipeline/scrapers/utah.py --end-year 2010           # 2010 and earlier
python3 src/pipeline/scrapers/utah.py --transactions            # bulk CSVs only
python3 src/pipeline/scrapers/utah.py --entities                # roster only
python3 src/pipeline/scrapers/utah.py --pause 5                 # slower pacing
```

To **resume an interrupted run**, use no year flags — the manifest handles it. `--start-year`/`--end-year` are for deliberately refreshing a range and will re-download everything in it.

`--contributions` / `--expenditures` collapse onto `--transactions`, and `--candidates` / `--committees` onto `--entities`: Utah's export puts both transaction kinds in one file keyed by `TRAN_TYPE`, and one roster covers both entity kinds. Pretending to a finer granularity than the source has would just be a lie in the CLI.

### Expected runtime

~100 bulk files totalling ~160 MB on a full cold run, plus roster paging at 8 types × 29 years × N pages. With `DOWNLOAD_PAUSE` at 1.5 s the pauses alone add ~6 minutes across ~230 combinations, and the source's own pacing limits (above) mean a cold full run may need more than one pass. Budget a couple of hours and expect to re-run; an incremental run is minutes.

Partial progress is never lost — the manifest is written after each file, so a re-run resumes.

### `--party`

A separate, manual mode. It is **not** in `orc.py`'s forwarded flag set and `main.py` never passes it — the same arrangement Texas and New York use.

```bash
python3 src/pipeline/scrapers/utah.py --party                # both sources
python3 src/pipeline/scrapers/utah.py --party --openstates    # Open States only (fast)
python3 src/pipeline/scrapers/utah.py --party --canvass       # canvasses only
```

**Open States** — `https://data.openstates.org/people/current/ut.csv`, CC0, no API key. Beyond name/party/chamber/district, its `links`/`sources` columns contain each legislator's own `disclosures.utah.gov/Search/PublicSearch/FolderDetails/{id}` URL. That is the same id the roster records as `state_filer_id`, which turns the party join from a name guess into an **exact identity match** for sitting legislators. Verified against the live file: 83 rows, and Cory Maloy's row does carry `FolderDetails/1414486`.

Coverage is limited to currently-serving legislators (~104 people). Open States' retired legislators exist only as YAML under `data/ut/retired/` in the openstates/people GitHub repo, not in the nightly CSV export; pulling those would mean walking a third-party repo tree and is deliberately not done.

**Canvasses** — `https://vote.utah.gov/historical-election-results/`. Links are discovered from that page rather than hardcoded. Only the Excel canvasses are taken (2000, 2008–2020); the PDF-only years (2024, 2022, and everything pre-2008 except 2000) are skipped, because pdfplumber over 60 years of inconsistent scanned canvass layouts is a project of its own and the Excel span already covers the years where Utah's itemized finance data is dense.

#### Canvass workbook layout (verified against real files)

Every canvass — general and primary alike — is a **county matrix, one worksheet per office group**:

```
r0/r1   race titles, each positioned at the first column of its race
r+1     COUNTY | REGISTERED VOTERS | BALLOTS CAST | PERCENT | <candidate columns>
r+2…    one row per county, vote counts
```

**Candidates and parties live in the header row, not in any data row**, and a candidate belongs to whichever race title sits at or to the left of its column. The original heuristic scanner assumed a party label in a data cell with a name beside it, which is why it extracted 0 rows from most files. Against the six real workbooks the layout-aware parser gets **810 rows where the scanner got 186**, with every party token recognised:

| File | heuristic | layout parser |
|---|---:|---:|
| `2000_primary.xls` | 0 | 42 |
| `2008_general.xls` | 0 | 261 |
| `2010_general.xls` | 4 | 216 |
| `2010_primary.xls` | 0 | 20 |
| `2012_general.xls` | 182 | 226 |
| `2012_primary.xlsx` | — | 45 |

Two conventions for the party marker, both handled: `Rob Bishop "R"` / `McCain & Palin "Republican"` (2008–2010) and `Mitt Romney, Paul Ryan (R)` (2012). Parenthesised form is tried **first** because the double quote is overloaded — candidate nicknames are quoted too, and `Michael L."Mike" Binyon (D)` would otherwise report party `Mike`. A party token is only accepted if it's one we recognise, so `David "Dave" L. Thomas` (a nickname, no party) correctly falls through to the race title rather than reporting party `L. Thomas`.

Primaries name the party in the **race title** (`State House District 20 Republican`) and leave the candidate cell a bare name; the title supplies the party whenever the cell doesn't. Joint tickets (`Huntsman & Herbert`, `Jill Stein, Cheri Honkala`) resolve to the lead name — note that in canvasses a comma separates running mates, the opposite of the disclosure roster's `Last, First`.

Party letters seen: `R D C L G J U` plus write-in variants. `U` is **Unaffiliated**, not United Utah — that party wasn't founded until 2017, and the 2010 `Anderson & Maxfield "U"` line is Farley Anderson, who ran unaffiliated. `J` is Rocky Anderson's Justice Party.

---

## Parser

`src/pipeline/parsers/utah.py`

### Quoting repair

Utah quotes most fields but **does not escape double quotes inside a field**, so the files are not valid CSV. `_repair_line()` rewrites any quote that neither opens nor closes a field into an apostrophe, leaving only structural quotes:

```
"a","Jane "Janie" Doe","b"   ->   "a","Jane 'Janie' Doe","b"
```

Once quotes are structural-only, `csv.reader` can also join genuinely multi-line quoted fields instead of breaking on them. One case stays ambiguous — a field containing both a stray quote *and* a comma can still mis-split — so rows whose field count doesn't match the header are skipped and counted (`malformed` in the log) rather than shifted a column. The Accountability Project's cleaner has the same limitation with the same approach.

### Identity model

A Utah folder is per-**candidacy** for candidates: one man's 2008 House-15 run and his 2012 House-15 run are two folders with two ids. So `state_filer_id` is a registration id, and **`id_model="committee"`** — `assign_person_ids()` groups on `(state, candidate_name, office, district)` and takes `person_id = min(state_filer_id)`. That merges the same person's runs for the same seat while keeping two same-named people in different seats apart, which is exactly why the annotation's office and district matter beyond reporting.

Committees and candidates are one row per entity, not per report year: the annotation *is* the cycle for candidates, and organizations keep one folder for life. `committees.election_year` is populated for candidates and blank for organizations, matching that column's "sparse — cycle-specific states only" contract.

### Reconciling two spellings of a filer name

The roster annotates every candidate folder, but the itemized export has been observed writing **bare** personal names — "King, Brian S", "Eliason, Steven" in the IRW pull. `Roster.resolve()` therefore:

1. tries the full annotated name, when that name belongs to exactly one entity;
2. otherwise treats the value as a base name and uses the file's report year to pick the candidacy — exact year match first, then the most recent candidacy at or before that year (a committee keeps filing after its election for year-end reports and debt retirement).

Both spellings land on the same roster record, so a filer gets one committee row rather than two with its transactions split between them, and `contributions.committee_name` carries the roster's canonical name. Traceability to the literal source cell is preserved by `raw_file` + `row_num`.

An unresolved filer keeps its raw name and no `state_filer_id`. Two name forms have to be reconciled for this to work, both measured on real data:

- **548 of 4,463 roster entities carry an `(aka. …)` group** the transaction export omits — `Zions Bancorporation Political Action Committee (aka. ZB NA)` in the roster, `ZIONS BANCORPORATION POLITICAL ACTION COMMITTEE` in the data. `split_alias()` indexes the primary name and each alias alongside the full one. Without this, resolution was **60.2%**.
- **Candidate folders are annotated; the export writes bare names** — 0 of 358 filers in a real `transactions_pcc_2024.csv` carried a `(YYYY Office-District)` suffix.

Together these take filer resolution from 60.2% to **97.8%** on the full 1.29M-row corpus.

### Filer resolution has a source-imposed ceiling of ~98%

`state_filer_id` is a **tier-2 warning** for Utah, via the `"utah"` entry in `validate.py`'s `TIER1_OPTIONAL_BY_STATE`, rather than the tier-1 failure it is for most states with filer ids.

That was not the original judgment — the check was deliberately left hard, on the reasoning that a complete roster should cover every filer and blanks therefore mean staleness. Real data settled it the other way. With a **complete** roster (4,463 entities, swept clean), resolution tops out at **97.8%** of 1,293,527 rows, and the residue is 28,740 rows across just **106 named entities** — `Life Elevated`, `Weber County Democrats`, `Libertarian Party of Utah`, `Comcast`. These filed real transactions and have simply been purged from the live search grid since. No amount of re-sweeping recovers them, which makes this a source-shaped ceiling, not a staleness problem, and 99% unreachable.

The number to watch is the parser's own `Filer resolution: X of Y (Z%)` line. It warns below **95%** — comfortably under the ~98% ceiling, so it fires only when the roster really is stale or partial, which a plain re-run of `scrapers/utah.py --entities` fixes.

Measured fill on a real run: candidates 96.9%, committees 96.6%.

A name shared by two registered entities gets no `state_filer_id` at all rather than one of them: a wrong id silently merges two real filers under one `person_id`, which is worse than no id.

### Field mapping

| Canonical | From |
|---|---|
| `transaction_type` (contributions) | `Contribution` / `In-Kind Contribution` |
| `transaction_type` (expenditures) | `PURPOSE`, or `Expenditure` when blank, with `" (In-Kind)"` appended when `INKIND` is set |
| `category` (expenditures) | raw `PURPOSE` |
| `purpose` (expenditures) | `INKIND_COMMENTS` |
| `election_year` (transactions) | the filer's candidacy year when it has one, else the file's report year |
| `amended` | `1` when `AMENDS` is non-blank, else `0` |
| `filing_id` | `TRAN_ID` |
| `contributor_type` | **deliberately blank** — see below |
| `loans_debts` | any row with `LOAN=X`; `record_type` = `Loan Received` / `Loan Made` by `TRAN_TYPE` |

**Why `PURPOSE` goes into `transaction_type` and not just `category`:** `EXPENDITURES_AGG` drops *both* `category` and `transaction_type`, deriving `transaction_category` from `transaction_type` alone. Flattening every expenditure to the literal string "Expenditure" would throw away the one signal that distinguishes a UT committee's $5,000 donation to a candidate (`Donations` → `Contribution`) from $5,000 of printing (`Supplies` → `Monetary`). The in-kind flag is appended rather than replacing the category, so neither is lost. Both halves are enumerated in `expenditure_categories.csv`; a `PURPOSE` string outside that list stays unmapped (NULL) by design, with the raw value still in `category` in the per-state DB.

**Why `contributor_type` is blank:** Utah publishes no contributor category anywhere — the only party-identifying column on a transaction is a free-text `NAME`. `aggregate.py` already backfills `contributor_type` for any contributor whose name matches a registered committee, which covers Utah's committee-to-committee flow without this parser guessing. `contributor_types.csv` carries a UT comment block so the absence reads as deliberate.

Loans go to `loans_debts` **only**, not also to contributions/expenditures, so they aren't double-counted. Note that `tabulate.py` does not currently load `loans_debts` into the per-state DB for any state — the CSV is written and correct, but won't appear in `utah.db`.

### Party overlay

`UTEnrichment` only ever *fills blanks*. Three tiers, first hit wins:

| Tier | Source | Confidence | Rule |
|---|---|---|---|
| 1 | Open States `entity_id` | `exact` | Same disclosures.utah.gov folder id on both sides. An identity, not a similarity — no name comparison happens |
| 2 | Open States name | always `high` | Full normalized name, else first+last. Its `current_district` is *today's* seat with no year attached, so it can only raise confidence in the pick, never reject a candidate whose historical seat differs |
| 3 | Canvass name | `exact` / `high` | Every canvass row names its year, so office and district are comparable: a contradicting district means a different seat and is discarded; `exact` requires district **and** year to agree |

A name that maps to two different parties is declined outright. No nickname expansion, soundex or edit distance — a wrong party on a real person is worse than a blank one.

Tiers 2 and 3 also backfill `office`/`district` on candidates whose folder carried no annotation. Everything written to `candidates.office` passes through `office_out()` first, so Utah's own "House" and the overlay's "State Representative" both land as one canonical label. This inverts the usual "store the raw source value" rule on purpose: `assign_person_ids(id_model="committee")` groups on `(state, candidate_name, office, district)` with only case/whitespace normalization, so two spellings of one office would split a single person across two `person_id`s — the exact failure the model exists to prevent. Nothing is lost, since `committee_name` retains the whole folder name, `(2022 House-57)` included. `party_source` / `match_confidence` mark which rows came from outside.

`_known_office()` deliberately returns `""` for any office string that doesn't resolve to a *recognized* canonical label, and a blank disables the office check rather than failing the match — an office spelling Utah invents tomorrow should cost precision, not silently reject every candidate who holds it.

---

## Data Notes

### First-run checklist

Nothing below was observable from the authoring environment. Work through it on the first real run.

0. **Does TLS verify at all?** If the first download dies with `curl (60) ... unable to get local issuer certificate`, you are behind a TLS-inspecting proxy. The scraper stops immediately and prints what to do; see [TLS: two different problems](#tls-two-different-problems-dont-confuse-them). Fastest check:
   ```bash
   python3 -c "import config; config.diagnose_tls('https://disclosures.utah.gov/')"
   ```

1. **Does the roster POST work at all?**
   ```bash
   python3 src/pipeline/scrapers/utah.py --entities --start-year 2024 --end-year 2024
   ```
   Expect thousands of rows in `data/Utah/raw/entities.csv`. Zero rows, or an HTML challenge page, means the ASM mitigation failed — bump `IMPERSONATE` first.

2. **Does pagination terminate correctly?** ⚠️ Still open — the sweep has never completed. Check the log for the `400-page cap` warning. If it fires, the short-page stop condition is wrong (the grid may pad pages or repeat the last one) and `_sweep_roster` needs a real stop signal instead. Real sweeps reached page ~69 per type, so the 400 cap has ample headroom.

   Note also that `--start-year`/`--end-year` values outside 1998–current year produce an empty window; the scraper logs "No report years in scope" and exits 0 rather than crashing.

3. **Do PCC folder names still carry `(YYYY Office-District)`?** The parser logs `N candidate folders carry a '(YYYY Office-District)' annotation`. If N is near zero, office/district/election_year will be empty for every candidate and `id_model="committee"` degrades to grouping on name alone. (Confirmed present on the live grid; confirmed **absent** from the transaction export, which is expected — see 4.)

4. ~~**Which name spelling does the transaction export use?**~~ ✅ **Answered: bare personal names.** 0 of 358 filers in a real `transactions_pcc_2024.csv` carry the annotation — they read `Abbott, Nelson`, `Acton, Cheryl`, `Adams, Stuart`. So every candidate filer resolves through the base-name + report-year fallback, never the direct full-name hit. Watch the `Filer resolution: X of Y (Z%)` line once a real roster exists; below 99% it warns, because that is where validate.py's tier-1 check fails.

5. **Does `EntityType`-without-an-id still work?** ✅ Verified — 232/232 combinations answered. Kept here because if it is ever withdrawn, every combination comes back HTML, and the fallback is the per-entity form (`GenerateReport/{id}?ReportYear=`) driven off `entities.csv` — far more requests, but only URLs the site itself links.

6. **Validate the canvass extractor.** ❌ Still the weakest link, and `--party` has not been run:
   ```bash
   python3 src/pipeline/scrapers/utah.py --party --canvass
   head -30 data/Utah/raw/UT_ElectionResults.csv
   ```
   Each file logs its row count, and a file yielding zero logs a warning naming `_extract_canvass`. Check that: candidate names look like people (not "Total" or a county), parties are Utah parties, and `office`/`district` track the race the row actually belongs to. Pay particular attention to **rows near the top of each worksheet** — the race context resets at every sheet boundary and falls back to the sheet name, so a workbook that names its sheets something other than the office ("Sheet1", "By County") leaves the first few rows of each tab with no race until the first heading appears. The scanner recognizes two layouts — a bare party label with a person-shaped cell beside it, and `"Jane Q. Public (REP)"` in one cell — treating the nearest single-cell row above as the race title. A workbook laid out differently should produce **zero** rows rather than plausible nonsense; if it produces nonsense instead, that is a bug worth fixing before the data is used.

### Party abbreviation collision

`src/aliases/parties.csv` is national, not state-keyed, and two Utah ballot codes collide there: Utah's **`CON` is the Constitution Party**, while `parties.csv` maps `CON` → `CONSERVATIVE` (New York's Conservative Party), and `IAP` (Independent American Party) wasn't in the table at all. The scraper therefore expands Utah's abbreviations to unambiguous long forms (`_CANVASS_PARTY_CANON`) *before* anything reaches `canonical_party()`, and `parties.csv` gained long-form rows for Utah's third parties. `parties.csv` does now carry `IAP` and `UUP` as well as their long forms, because neither abbreviation is claimed by any other state; `CON` is deliberately **not** added, since it is already taken. When adding a Utah party label, expand it in `_CANVASS_PARTY_CANON` first and only add a bare abbreviation to `parties.csv` if nothing else in the table claims it.

### Coverage and known gaps

- **Itemized data is thin before ~2008.** The dropdown offers 1998, but the IRW pull found contributions effectively starting in 2000 and only reported in bulk from 2008. Early years will produce small or empty files; empty ones are recorded with `rows=0` and skipped on later runs.
- **Party coverage is structurally capped.** Tier 1 reaches only sitting legislators; tier 3 reaches only offices the Excel canvasses cover (statewide, legislative, federal) for 2000 and 2008–2020. County, municipal and judicial candidates, and the 2022/2024 cycles (PDF-only canvasses), are out of reach of both. A low overall party fill rate is a source ceiling, not a broken matcher.
- **In-kind contribution descriptions are dropped.** `INKIND_COMMENTS` maps to `expenditures.purpose`, but the contributions table has no purpose/description column, so the narrative on an in-kind *contribution* is lost.
- **`FILED` is not stored.** A blank `FILED` means the transaction is in the ledger but not yet on a filed report. Those rows are kept (they are real disclosed transactions) but the distinction isn't recorded anywhere in the canonical schema.
- **Duplicate rows exist in the source.** IRW found 28,053 fully duplicated records (same date, amount, names) among 729K contributions, distinguished only by `TRAN_ID`. These are not de-duplicated here — with distinct transaction ids and no amendment link between them, there is no principled way to tell a genuine repeat donation from a double entry.
- **Jurisdiction is never populated.** Utah's data has no jurisdiction field, and the folder annotation gives office/district only.

### Municipal and federal filings are elsewhere

County and city candidate reports live at `disclosures.utah.gov/Municipal`, federal candidates at `fec.gov`, and lobbyist reports at `lobbyist.utah.gov`. None are in scope here.

---

## Last Updated

| Component | Date | Note |
|---|---|---|
| `src/pipeline/scrapers/utah.py` | 2026-08-22 | Written; not yet run against the live site |
| `src/pipeline/parsers/utah.py` | 2026-08-22 | Verified end-to-end against fixtures built from the real sample CSV and roster HTML |
| `src/aliases/*` (UT rows) | 2026-08-22 | committee/contributor/transaction/expenditure/office types + Utah party long forms |
| `docs/states/utah.md` | 2026-08-22 | |
