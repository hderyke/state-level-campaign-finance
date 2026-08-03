# West Virginia — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | West Virginia (WV) |
| **Source** | [WV Secretary of State — Campaign Finance Reporting System (CFRS)](https://cfrs.wvsos.gov/) |
| **Access method** | Plain HTTP against the site's own JSON/CSV API. **No Playwright** — the SPA shell is skipped entirely and the API service is hit directly |
| **Coverage** | 2018 – present (mandatory e-filing era; earlier filings were paper and were never published as structured data) |
| **person_id model** | `committee` — `state_filer_id` is CFRS's `ORG ID`, issued per committee registration, so a candidate carries a different ID each cycle |
| **FIPS** | 54 |

---

## Where the data comes from

CFRS was rebuilt some time after 2023. The rebuild retired the bulk-CSV export, so the paged JSON grid is the single transaction source.

**Current — `https://cfrs.wvsos.gov/api/Public-Service/`**
POST with a JSON body, camelCase fields, paginated grids. These are the routes the live site's own XHRs use and were captured from DevTools on `/public/gettoknow` and `/public/trackfinance`.

| Route | Method | Feeds |
|---|---|---|
| `Committee/getPublicCandidatesCommitteeDataList` | POST | Candidate + committee registry — the only source of office, district, party and registration status |
| `CommitteeTransactions/getAllPublicTransactionDataList` | POST | Per-row transaction grid, filtered by `transactionCategory` (`CON`/`EXP`) and `transactionYear` |
| `CommitteeTransactions/getContributorTypeByTransactionType/{CON\|EXP}` | GET | Contributor/payee type vocabulary |

The registry POST body carries a block of empty-string and `null` filter keys (`entityId`, `orgStatus`, `orgSubTypeCode`, `candidateName`, `officerName`, `orgName`, `orgType`, `registrationStartDate`, `registrationEndDate`, `electionID`, `officeID`, `districtID`, `partyCode`, `reportingCycleId`, `isJointFundrisingOrg`). Those placeholders are **load-bearing** — CFRS rejects requests that omit them. They are reproduced verbatim in `REGISTRY_BODY`.

**Legacy — `https://cfrs.wvsos.gov/CFIS_APIService/api/DataDownload/` — RETIRED**
GET, whole-year CSV files. Documented by the state through published record-layout PDFs and used by the Investigative Reporting Workshop's Accountability Project through 2023.

| Route | Purpose |
|---|---|
| `GetCheckDatadownload?pageNumber=1&pageSize=100` | Catalog of every bulk file: `TransactionKey`, `ElectionYear`, `NameOfFile`, `TransactionType` |
| `GetCSVDownloadReport?year=&transactionType=&reportFormat=csv&fileName=` | The CSV itself |

> **Confirmed dead as of 2026-08-02**, verified against the live site twice rather than assumed:
>
> | Base | Response |
> |---|---|
> | `/api/Public-Service/DataDownload` | `HTTP 404` |
> | `/CFIS_APIService/api/DataDownload` | `HTTP 200` serving the React shell — the SPA catch-all for an unrouted path |
>
> The second is the dangerous one: a check that only inspects the status code would read `200` and treat a dead route as live, then fail confusingly downstream when the "JSON" turns out to be HTML.
>
> **The download path has been REMOVED from the scraper** (241 lines: `_resolve_download_base`, `_catalog`, `download_csv`, `_count_rows`, `_bulk_probe_due`, and the tier-1 branch of `run()`). The old `CFIS_APIService` generation was replaced by a different architecture, not relocated, so a periodic tripwire was not worth 14% of the module. This document and git history record how it worked if it ever returns.
>
> **`parsers/west_virginia.py` still reads the legacy `CON_*.csv` / `EXP_*.csv` layouts** — quote repair, positional columns, loan splitting, all intact. Files already on disk from the bulk era, or from the IRW archive, continue to parse. Only the *download* is gone.

Record layouts:
[Contributions and Loans](https://cfrs.wvsos.gov/CFIS_APIService/Template/KeyDownloads/Contributions%20and%20Loans%20File%20Layout%20Key.pdf) ·
[Expenditures](https://cfrs.wvsos.gov/CFIS_APIService/Template/KeyDownloads/Expenditures%20File%20Layout%20Key.pdf)

### Service topology

CFRS is not one API — it's several, each with its own base, and a route only works against the right one. Extracted from the app's own JS bundle (`/assets/index-*.js`), where routes are assembled as `` `${base}${routeConstant}` ``:

| Base | Scope | Routes seen |
|---|---|---|
| `https://cfrs.wvsos.gov/api/Public-Service` | Public site — the only one this scraper uses | (in the lazy `PublicRoutes-*.js` chunk) |
| `https://cfrs.wvsos.gov/api/Common-Service` | Shared lookups, grid/export plumbing | 49 |
| `https://cfrs.wvsos.gov/api/Auth-Service` | Login, users, roles, admin | 43 |
| `https://cfrs.wvsos.gov/api/MessageCentral` | Correspondence, help library | 13 |
| `https://cfrs.wvsos.gov/api/ExternalAuth` | SSO | — |

**The same route path exists on more than one base.** `/CommitteeTransactions/getContributorTypeByTransactionType` appears under `Common-Service` in the bundle, but the public site calls it under `Public-Service` (confirmed by DevTools capture). The authenticated and public variants are distinct endpoints. Don't assume a path found in the bundle works against `Public-Service` — the bundle's admin routes are for the logged-in portal.

### Whole-grid export — public, unauthenticated, one call per grid

```
POST https://cfrs.wvsos.gov/api/Common-Service/DataGrid/generateExportGridDataExcel
```

**It's on `Common-Service`, not `Public-Service`**, despite being a public call — `"moduleType": "PUBLIC"` in the body is what makes it work with **no `Authorization` header**. The scraper sends no credentials.

**The response embeds the file.** Confirmed live 2026-08-02:

```json
{"isSuccess": true,
 "responseData": {"fileBytes": "<base64>"},
 "message": null, "skipRecords": null}
```

There is **no CloudFront round-trip**, contrary to what the bundle's admin routes (`AmazonCloudFront/getDownloadLinkWithoutCookies`) implied — those belong to the authenticated portal. `_extract_file_bytes()` reads `responseData.fileBytes` (base64 or a byte array, since .NET serializes `byte[]` either way) and picks the extension from magic bytes rather than trusting the route name: `PK\x03\x04` → `.xlsx`, `\xd0\xcf` → `.xls`, otherwise `.csv`. The handle/link path is kept only as a fallback.

**`filterRequest` uses PascalCase** (`SortColumn`, `EntityId`, `OrgStatus`) while the paged grid on `Public-Service` uses camelCase for the identical fields. Reusing one body for both silently yields an unfiltered or empty result.

**No `pageNumber`/`pageSize`** — it exports the whole grid.

Confirmed body (candidate/committee registry):

```json
{"moduleType":"PUBLIC","gridName":"GETTOKNOW_CANDIDATECOMMITTEES",
 "pageName":"PUB_GTK_CNCM","fieldType":"G",
 "filterRequest":{"SortColumn":"registrationDate","SortDirection":"desc", ...}}
```

`gridName` / `pageName` are opaque server-side identifiers — they can't be derived, only captured per grid via DevTools → Network → click Export.

**Transaction grids — captured 2026-08-02.** `TrackFinance_Contributions` (and, inferred from its naming, `TrackFinance_Expenditures`). Their bodies differ structurally from the registry export:

```json
{"moduleType":"PUBLIC","gridName":"TrackFinance_Contributions",
 "filterRequest":{"TransactionCategory":"CON","SortColumn":"transactionDate",
                  "SortDirection":"desc","TransactionYear":"2026","IsPublic":true}}
```

**No `pageName`, no `fieldType`** — those are per-grid, not universal, so `export_grid()` sends them only when a spec declares them. `IsPublic` is new, and `TransactionYear` is a string.

`run()` now tries the export first for each year and falls back to the paged sweep on any failure — wrong grid name, `isSuccess: false`, unreadable body. It can therefore only make a run faster, never fail one. `TrackFinance_Expenditures` is inferred rather than captured; if it's wrong, that year silently falls back to paging and the error names the grid.

### Export output format

CFRS only emits Excel — there is no CSV route and no format parameter anywhere in the app bundle (only `generateExportGridDataExcel`, `/Common/generateExportGridData` and `/ExportGridData/...`, none taking a `fileType`/`exportFormat`). The scraper therefore converts to CSV at write time via `_write_export()` / `_xlsx_to_csv()`, controlled by `EXPORT_AS_CSV` (default `True`; set `False` to keep the `.xlsx`).

This is the one place the scraper does **not** keep a raw file byte-for-byte as served. Deliberate: a single-sheet grid export converts losslessly, CSV is diffable and greppable where `.xlsx` is an opaque zip, and the rest of the pipeline is CSV-native. Conversion streams (`read_only`/`values_only`) so a large export doesn't build a full cell graph, dates are emitted ISO-8601 rather than Excel's locale-dependent rendering, and if conversion isn't possible (no `openpyxl`, unreadable workbook) it falls back to writing the original bytes rather than failing the download.

Worth noting: because Python's `csv` writer quotes properly, these exports **do not** have the unescaped-embedded-quote defect that the state's own legacy CSV export had. A payee literally named `Friends of "Big Jim", LLC` round-trips intact.

`parsers/west_virginia.py` reads the result with `DictReader` (`_iter_export_csv`) rather than the positional reader used for the legacy `CON_*.csv`. The two never collide — export files are prefixed `export_`, legacy files start with the category code.

### Grid export vs. paged grid — why the grid stays primary

Both sources were compared against real 2018 data. The export is far faster and materially thinner, and the gaps land on fields this pipeline depends on:

| | JSON grid | CSV export |
|---|---|---|
| Fields | 52 | 11 (CON) / 10 (EXP) |
| Full backfill | ~44 min | seconds per year |
| Transaction ID | `transactionID` | **absent** — no `filing_id`, **no cross-year dedup** |
| Filer ID | `orgID` = `1517` | `RegistrantID` = `1020001517` — a **different** id, so `state_filer_id`/`person_id` wouldn't line up across sources |
| Amended flag | `amendedFlag` | absent |
| Election year | `electionYear` (int) | absent — falls back to the file's year |
| City / state / zip | separate fields | one combined address; recovered by `_split_address()` for ~61% (CON) / ~87% (EXP) |
| Loans | `transactionTypeDesc` | `ContributionType` carries `Loans` / `Loan Payment` / `Loan Forgiveness` ✓ |

The dedup loss is decisive: CFRS republishes amended transactions under their **original** ID in later year files, and that ID is the only thing that detects it. `EXPORT_TRANSACTIONS_READY = False` is therefore the default — transactions come from the paged grid. Flip it to `True` to trade completeness for speed.

The **registry** export (`ENTITY_EXPORTS`) stays enabled either way; it isn't subject to any of these gaps.

Measured export fill rates (4,000 real 2018 rows): `committee_name`, `amount`, `date`, `transaction_type`, `contributor_name`, `contributor_type`, `election_year` all 100%; `candidate_name` ~79%; `employer` 31%; `occupation` 27%; `filing_id` **0%**.

### Reference lookups — what actually resolves

Measured live 2026-08-02. All successes were on **Common-Service, unauthenticated**:

| Relation | Route | Rows |
|---|---|---|
| `parties` | `CommitteeRegistration/getAllPartyDistrictList` | 174 |
| `committee_types` | `CommitteeRegistration/getAllOrgType` | 5 |
| `transaction_types` | `CommitteeTransactions/getTransactionType` | 6 |
| `occupations` | `CommitteeRegistration/getAllOccupations` | 33 |
| `contributor_types` (CON/EXP) | `CommitteeTransactions/getContributorTypeByTransactionType` | 6 each |

Still unresolved on either base: `offices`, `elections`, `jurisdictions`, `contribution_purposes`, `violations`. The paths for these came from the app bundle and are real, but evidently belong to the authenticated portal only — their public equivalents haven't been captured. `office_types.csv` therefore remains unpopulated for WV.

Registry: **2,954 committees**.

### TLS

`cfrs.wvsos.gov` serves an **incomplete certificate chain**. Browsers paper over it; Python's `certifi` bundle does not, and the site's own curl examples pass `--insecure`. The scraper therefore sets `VERIFY_SSL = False` and suppresses `urllib3.exceptions.InsecureRequestWarning`. This disables chain validation for one `.gov` host serving already-public data. Flip `VERIFY_SSL` back to `True` if the state ever fixes the chain.

---

## Raw Data Structure

`data/West Virginia/raw/`

### `CON_{year}.csv` — contributions **and loans**, 30 columns

Read positionally. Field order per the state's layout key:

| # | Field | # | Field |
|---|---|---|---|
| 0 | `ORG ID` | 15 | `RECEIPT SOURCE TYPE` |
| 1 | `RECEIPT AMOUNT` | 16 | `AMENDED` (Y/N) |
| 2 | `RECEIPT DATE` | 17 | `RECEIPT TYPE` |
| 3 | `LAST NAME` | 18 | `COMMITTEE TYPE` |
| 4 | `FIRST NAME` | 19 | `COMMITTEE NAME` |
| 5 | `MIDDLE NAME` | 20 | `CANDIDATE NAME` |
| 6 | `SUFFIX` | 21 | `EMPLOYER` |
| 7 | `ADDRESS 1` | 22 | `OCCUPATION` |
| 8 | `ADDRESS 2` | 23 | `OCCUPATION COMMENT` |
| 9 | `CITY` | 24 | `FORGIVEN LOAN` |
| 10 | `STATE` | 25 | `RELATED FUNDRAISER EVENT DATE` |
| 11 | `ZIP` | 26 | `RELATED FUNDRAISER EVENT TYPE` |
| 12 | `Description` | 27 | `RELATED FUNDRAISER PLACE OF EVENT` |
| 13 | `RECEIPT ID` | 28 | `REPORT NAME` |
| 14 | `FILED DATE` | 29 | `CONTRIBUTION TYPE` |

Observed `CONTRIBUTION TYPE` values: `Monetary`, `In-Kind`, `Other Income`, `Receipt of Transfer of Excess Funds`.

### `EXP_{year}.csv` — expenditures, 28 columns

| # | Field | # | Field |
|---|---|---|---|
| 0 | `ORG ID` | 14 | `FILED DATE` |
| 1 | `EXPENDITURE AMOUNT` | 15 | `PURPOSE` |
| 2 | `EXPENDITURE DATE` | 16 | `AMENDED` |
| 3 | `LAST NAME` | **17** | **`EXPENDITURE TYPE`** — schedule grouping |
| 4 | `FIRST NAME` | 18 | `COMMITTEE TYPE` |
| 5 | `MIDDLE NAME` | 19 | `COMMITTEE NAME` |
| 6 | `SUFFIX` | 20 | `CANDIDATE NAME` |
| 7 | `ADDRESS 1` | 21 | `FUNDRAISER EVENT DATE` |
| 8 | `ADDRESS 2` | 22 | `FUNDRAISER EVENT TYPE` |
| 9 | `CITY` | 23 | `FUNDRAISER EVENT PLACE` |
| 10 | `STATE` | 24 | `SUPPORT OR OPPOSE` |
| 11 | `ZIP` | 25 | `CANDIDATE` |
| 12 | `EXPLANATION` | 26 | `REPORT NAME` |
| 13 | `EXPENDITURE ID` | **27** | **`EXPENDITURE TYPE`** — monetary classification |

> **Columns 17 and 27 share the header name `EXPENDITURE TYPE`.** `csv.DictReader` silently collapses duplicate headers and the second value overwrites the first, so one of the two fields would vanish with no error at all. This is the primary reason both CSVs are read **positionally, by index** rather than by header name. Column 17 is the schedule grouping (`Expenditures`, `Independent Expenditures`, …) and lands in `category`; column 27 is the monetary classification (`Monetary`, `In-Kind`, `Disbursement of Excess Funds`, …) and lands in `transaction_type`.

### `transactions_{CAT}_{year}.jsonl` — the current production source

Newline-delimited JSON, one record per line, streamed on both write and read. Field names below are **observed** from a real 2018 payload:

| Canonical column | CFRS field | Note |
|---|---|---|
| `amount` | `transactionAmount` | float |
| `date` | `transactionDate` | ISO |
| `committee_name` | `committeeName` | |
| `candidate_name` | `candidateName` | |
| `contributor_name` / `payee_name` | `contributorPayeeName` | **one combined field** — not split like the CSVs |
| `contributor_type` | `entityTypeDesc` | |
| `transaction_type` | `transactionCategoryDesc` | Monetary / In-Kind / Other Income |
| `category` | `transactionTypeDesc` | Contributions / Expenditures / Loans |
| `purpose` | `transactionPurpose` | EXP |
| `employer` | `employerName` | |
| `occupation` | `employerOccupation` | |
| `contributor_state` | `stateCode` | |
| `election_year` | `electionYear` | integer field — **not** parsed from the report name |
| `amended` | `amendedFlag` | JSON boolean |
| `filing_id` | `transactionID` | |
| `state_filer_id` | `orgID` | matches the CSVs' `ORG ID` |

Two traps worth recording:

- `reportFileName` is often just `"Final Report"` with no year, so `election_year` must come from the `electionYear` integer. The CSV tier's report-name parsing would silently yield nothing here.
- An earlier **inferred** alias table matched `transactionAmount` and `transactionDate` but missed name, type, employer, occupation and amended. Because amount and date resolved, a naive "did the mapping work?" check passed while five columns came through empty. `_parse_json_file` therefore validates the contributor/payee name separately.

Loans are split out of the CON category here too, keyed on `transactionTypeDesc` — measured at ~2% of rows.

Measured fill rates over 5,000 real 2018 contribution rows: every schema-required field 100%; city 66%, state/zip 78%, candidate_name 82%, employer 29%, occupation 26%.

### Other files

| File | Contents |
|---|---|
| `entities_committees.json` | Candidate + committee registry from the current API |
| `lookup_contributor_types_{CON,EXP}.json` | Contributor/payee type vocabulary |
| `entities_{offices,elections,parties,violations}.json` | Written only if a probed route responded (see below) |
| `transactions_{CAT}_{year}.json` | Tier-2 fallback only — present when the bulk CSVs were unreachable |
| `_endpoints.json` | Cache of resolved routes, so steady-state runs don't re-probe |

---

## Scraper

`src/pipeline/scrapers/west_virginia.py`

### Transaction acquisition

The scraper pages `getAllPublicTransactionDataList` per `(category, year)` and streams the rows to `transactions_{CAT}_{year}.jsonl` — newline-delimited, written page-by-page via a `.part` file so memory stays flat regardless of year size.

Paging stops on any of: a short page, an empty page, reaching the server-reported total, or `GRID_MAX_PAGES`. The redundancy is deliberate — a grid that silently ignored `pageNumber` would otherwise refetch page 1 forever.

### Reading the response envelope

`_unwrap()` finds the record array in three passes: a bare array, then a known envelope key, then **structurally — any value that is a list of dicts**, preferring the longest.

The third pass matters. The envelope key names (`data`, `items`, `results`, …) were inferred rather than observed, and a fixed key list means an unrecognized envelope reads as *zero records* — indistinguishable from a genuinely empty result. Matching on shape degrades gracefully when CFRS renames a field. Lists of scalars are ignored, since those are never record sets.

When a grid's first page still yields nothing, the scraper:

1. retries once at `pageSize=10`, the only page size CFRS has been directly observed to accept (from the site's own XHR) — some .NET grids validate `pageSize` against an allow-list and return an empty set rather than an error; and
2. logs the actual response shape via `_shape_of()`, so a shape change is diagnosable from the scraper's own output instead of requiring DevTools.

### Paging correctness

`_fetch_grid()` returns `(records, complete)`. `complete=False` means paging stopped early — page cap or mid-sweep HTTP error — and the caller is holding a **partial** result.

This is not cosmetic. `GRID_MAX_PAGES` was a flat 5,000, and if CFRS forces the `pageSize=10` fallback, WV's 2022 contributions (188,125 rows) need ~18,800 pages. The sweep would have stopped at 50,000 rows and been written to the manifest as a **finished download** — three quarters of the year silently missing, with the pipeline reporting success. The cap is now derived from the server-reported total, and a truncated sweep is reported, not swallowed.

An incomplete transaction year is **deliberately not written to disk**. Writing it would be worse than useless: it wouldn't be in the manifest, but the tier-2 loop's existence fallback (`dest.exists() and st_size > 0`) treats any file on disk as done, so a re-run would skip it forever and the truncation would become permanent. A partial **registry** is kept, with a warning — it's enrichment, and the committees it does cover get correct office/district/party.

Progress is reported via tqdm, falling back to a log line every 25 pages. At `pageSize=10` a single year can take over two hours; the first live run looked like a hang precisely because this loop printed nothing.

An empty year is reported as `API returned no rows for this year` with a distinct `status="empty"` event. It previously reused `file_download_skip`, whose console text reads *"already downloaded, skipping"* — which made a run where **every year returned zero rows** look like a fully-cached no-op. Worth remembering as a failure mode: the log claimed success while nothing was fetched.

### Offline behaviour

`_preflight()` resolves the host once before doing anything. Without it an offline run spent minutes in retry backoff — `MAX_RETRIES` × `RETRY_BACKOFF_S`, multiplied across every probed endpoint — and then surfaced as a pile of per-file download errors that read like an API change rather than a local network problem. DNS failures are also excluded from the retry loop (`_is_dns_failure`), since a name that doesn't resolve won't resolve five seconds later.

### Entity acquisition

The registry and contributor-type routes are confirmed and fetched directly. The remaining `gettoknow` tabs (offices, elections, party, violations) follow the same controller/camelCase-action shape but their exact action names were **not** captured; `UNVERIFIED_ENDPOINTS` holds ordered guesses, each tried once, with any success cached in `_endpoints.json`. These are enrichment only — a miss is logged, never raised.

> To pin one down: open the tab in Chrome → F12 → Network → Fetch/XHR, and add the real path as the first entry in the relevant `UNVERIFIED_ENDPOINTS` list. No other code needs to change.

### Manifest and scope

`data/West Virginia/manifest.csv` — `relation_type`, `year`, `filename`, `source_url`, `bytes`, `rows`, `scraped_at`. The manifest is rewritten after every file so an interrupted run resumes cleanly.

The **current calendar year is always re-fetched**, manifest hit or not: CFRS rewrites the open year's file in place as amended reports land, so an earlier hit is stale by definition. Year rows are wiped from the manifest when `--force` / `--start-year` / `--end-year` put them in scope; entity rows carry no year and are never wiped by year flags.

`--candidates` and `--committees` both map to the same entity sweep — CFRS returns candidates and committees from one registry route and offers no way to split them server-side.

**Expected runtime:** roughly 1–3 minutes for a full tier-1 backfill (about 18 year-files plus the registry). Tier 2 takes considerably longer.

---

## Parser

`src/pipeline/parsers/west_virginia.py`

### The quoting defect

CFRS wraps character columns in double-quotes but **does not escape double-quotes inside them**:

```
...,"Friends of "Big Jim" Smith","Charleston",...
```

No CSV dialect reads this correctly. `_split_row()` runs a staged repair per line:

1. Parse as served. Accept only if the field count is right **and no field still contains a `"`**.
2. Otherwise rewrite interior quotes — any `"` not adjacent to a comma or a line boundary — to apostrophes and re-parse.
3. Otherwise accept the closest near-miss: pad a row one field short, truncate an over-wide one.

Step 1's second condition is the subtle one. Python's `csv` reader is non-strict: given the line above it closes the quote early and appends the remainder literally, returning the **right number of fields** with a mangled value inside. A field-count check alone waves that straight through.

A minority of `CON` rows are also short one column — the state omits the trailing `CONTRIBUTION TYPE` rather than emitting it empty — so a row one field short is padded, not discarded. Every outcome is counted and reported on the per-file `file_parsed` event as `repaired` / `padded` / `truncated` / `malformed`. A jump in those counts is the earliest signal that CFRS changed its export.

Raw files are left byte-for-byte as downloaded; all repair happens in memory, so `raw_file` + `row_num` still point at the real source line.

### Key transformations

**`committee_name` resolution.** `committee_name` is required by the schema but is empty on *every* candidate-committee transaction row — CFRS populates `CANDIDATE NAME` and leaves the committee blank. Resolved in three steps: the row's own value → the registry entry for that `ORG ID` → the candidate's own name. Without the third step the state fails tier-1 validation outright.

**Loan splitting.** `CON` is the "Contributions **and Loans**" file. Rows whose `RECEIPT TYPE` or `CONTRIBUTION TYPE` mentions a loan, or that carry a `FORGIVEN LOAN` value, are routed to `loans_debts.csv.gz`. A row that lands there only because of `FORGIVEN LOAN` gets `record_type` rewritten to `Forgiven Loan` — its raw `RECEIPT TYPE` still reads `Contributions`, which would be actively misleading on a loans row.

**Name assembly.** Names arrive split across `LAST`/`FIRST`/`MIDDLE`/`SUFFIX`, with non-individuals carrying the whole entity name in `LAST NAME` and a literal single space (not an empty string) in the other three. `_person_name()` treats whitespace-only parts as absent, so an organization comes back unchanged and a person as `First Middle Last Suffix`.

**`election_year`** comes from the leading year of `REPORT NAME` (`"2022 4th Quarter Report"` → `2022`), falling back to the source file's year. It is deliberately *not* taken from the transaction date, which frequently lands in the year before the cycle it was reported under.

**Deduplication** is on `RECEIPT ID` / `EXPENDITURE ID`, which are unique per transaction. CFRS republishes an amended transaction under its original ID in the year file it was amended into, so the same ID can appear in more than one year's export; the first occurrence wins. The same `seen` set spans the CSV and JSON tiers, so if both exist for a year the richer CSV rows win and the JSON adds nothing duplicate.

**`amended`** is normalized from CFRS's `Y`/`N` to the schema's `1`/`0`, matching the convention in `alabama.py` / `arkansas.py`.

### Entity output

The registry feed is authoritative. Any `ORG ID` seen only in transactions is then appended from an accumulator, so a committee that has since dropped off the public grid still gets a row rather than leaving its transactions orphaned.

Derived candidates inherit `office` / `district` / `jurisdiction` / `party` from a registry candidate with the same name — but **only when that name is unambiguous** in the registry. This matters more than it looks: `assign_person_ids(id_model="committee")` groups on `(state, candidate_name, office, district)`, so a derived row with those fields blank would not merge with the same person's registry row and the candidate would end up with two different `person_id` values.

---

## Data Notes

- **Coverage starts at 2018.** WV did not require electronic filing before then; earlier filings exist as scanned paper and were never published as structured data. `EARLIEST_YEAR` is set accordingly rather than probing years that return nothing.
- **`committee_name` is blank in the source on candidate-committee rows** — 100% of them in the samples checked. See the resolution chain above. Rows resolved via the third fallback carry the candidate's name as the committee name, which is a reasonable display value but is *not* the committee's registered name.
- **`contributor_city` / `contributor_zip` are missing on roughly half of `CON` rows** in the source itself. Not a parsing artifact.
- **`ADDRESS 1` / `ADDRESS 2` are not carried into output** — the canonical schema has no street-address field. They remain in `raw/`.
- **Office, district and party exist only in the registry feed.** If `entities_committees.json` is absent, candidates are reconstructed from transaction rows and those three fields come out empty — the same tradeoff `new_hampshire.py` documents.
- **Alias mappings** cover the values confirmed against real exports plus the remainder of CFRS's controlled vocabularies, mapped on the unambiguous reading of each label. After a live run, refresh them against `raw/lookup_contributor_types_{CON,EXP}.json`, which is the state's own list. Unrecognized values are left unmapped rather than guessed — a `NULL` category in the aggregate beats a wrong one.
- **`office_types.csv` has not been populated for WV.** Office values come from the registry feed, which had not been sampled live at the time of writing; build the crosswalk from a real `entities_committees.json` rather than from assumptions.

---

### `election_year` semantics — read this before filtering by it

`election_year` is CFRS's own `electionYear` field, and it is the **committee's registration cycle**, not the transaction's year.

Verified against the loaded database: 44,503 contributions carry `election_year = 2012`, but their transaction dates span **2018-01-15 to 2025-05-07**, and they belong to committees such as `MORRISEY FOR AG 2012` and `TOMBLIN, EARL RAY` — registered for the 2012 cycle and still filing years later.

The value is genuine source data, so the parser passes it through rather than silently reinterpreting it. **For "transactions in year N", filter on `date`.**

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-02 |
| Parser | 2026-08-02 |
| Alias mappings | 2026-08-02 |
| Docs | 2026-08-02 |
