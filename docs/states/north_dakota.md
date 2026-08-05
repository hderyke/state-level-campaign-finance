# North Dakota — Pipeline Documentation

> **Status.** The parser is verified end-to-end against real CFRS downloads (2026 Contributions, 2025 Expenditure, 2026 Registration, 2026 FiledReports, 2027 ReportingSchedule, and the Candidates & Committees roster export): 5,340 contributions / 1,093 expenditures / 482 committees / 279 candidates, validator `PASS`, zero rows skipped, all alias values mapped. All three endpoints' request payloads and response envelopes are confirmed from captured browser traffic, and the scraper's HTTP logic is covered by a 78-assertion offline stub suite. **What remains unverified is only the live round trip** — no request has been issued to `cfrs.sos.nd.gov` from this pipeline. Two known scope limits: **CFRS holds only 2025-onward** (2024-and-prior lives in a separate legacy archive) and **independent expenditures are absent from the bulk exports** — see [Coverage](#coverage-and-the-legacy-archive) and [Independent expenditures](#independent-expenditures--a-known-gap). See also the [first live run checklist](#first-live-run-checklist).

---

## Overview

| | |
|---|---|
| **State** | North Dakota (ND) |
| **Source** | [ND Secretary of State Campaign Finance Reporting System (CFRS)](https://cfrs.sos.nd.gov/public/accessreports?tab=datadownload) — "Ethics Solution" platform, built by TGS Technology LLC |
| **Access method** | Two paths, both pure HTTP, **no Playwright**: the Data Download catalog (POST JSON → signed CloudFront link → streaming GET) and the public ["Get to Know" DataGrid export](https://cfrs.sos.nd.gov/public/gettoknow?tab=candidate) (POST JSON → workbook) |
| **Coverage** | **2025 – present.** CFRS launched January 2026 and holds 2025 year-end reporting onward. Reports filed **2024 and earlier live in a separate legacy Archive** that is not scraped — see [Coverage and the legacy archive](#coverage-and-the-legacy-archive) |
| **person_id model** | `committee` — see [Parser](#parser) for why, and for the name-collision caveat |

---

## Raw Data Structure

### How the download works

The Data Download tab is a React SPA backed by two undocumented JSON endpoints. Neither needs authentication, cookies, or a CSRF token, but both require an `Origin`/`Referer` pair matching the public site — without them CFRS answers `403`.

**1. Catalog listing**

```
POST https://cfrs.sos.nd.gov/api/Public-Service/AccessReport/getDataDownloadDataList
Content-Type: application/json
Origin:  https://cfrs.sos.nd.gov
Referer: https://cfrs.sos.nd.gov/public/accessreports?tab=datadownload

{"pageNumber": 1, "pageSize": 10}
```

Returns the catalog of pre-generated bulk files. Each entry carries an S3 key under `nd-cfs/CFDataDownload/`, named `{Category}_{Year}_{YYYYMMDDHHMMSS}.csv` — e.g. `nd-cfs/CFDataDownload/Contributions_2026_20260804132501.csv`. The trailing timestamp is the generation time, so **filenames change whenever ND regenerates a file**. That's what the manifest keys off.

**2. Link resolution**

```
POST https://cfrs.sos.nd.gov/api/Common-Service/AmazonCloudFront/getDownloadLinkWithoutCookies
{"s3FilePath": "nd-cfs/CFDataDownload/Contributions_2026_20260804132501.csv"}
```

Returns a short-lived signed CloudFront URL, fetched with a plain `GET` carrying only a `User-Agent` — forwarding the JSON API's `Content-Type`/`Origin` headers onto an S3 GET can invalidate the signature.

### The server's TLS chain is broken — `verify=False` is deliberate

`cfrs.sos.nd.gov` transmits an invalid certificate chain. Confirmed via Qualys SSL Labs (grade **B**, cert chain `"issues": 6`): the chain it sends is

```
[ leaf, leaf ]                        <- the same certificate twice
```

while every trust path to a root requires

```
leaf -> 6542d176...(intermediate) -> root
```

The intermediate CA is **never sent**. Browsers hide this by downloading it from the leaf's AIA (Authority Information Access) extension; OpenSSL does not. So `requests` fails on every machine and every network with:

```
SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED]
certificate verify failed: unable to get local issuer certificate
```

**This is the server's bug, not a client or corporate-proxy problem** — it reproduces identically on unmanaged home machines. Confirm it yourself:

```bash
openssl s_client -connect cfrs.sos.nd.gov:443 -servername cfrs.sos.nd.gov -showcerts </dev/null
```

A correct chain lists two or more **distinct** certificates. CFRS lists the same one twice.

**How the scraper handles it.** `verify=False` plus `urllib3.disable_warnings`, the same pattern `scrapers/alabama.py` already uses and what `docs/contributing.md` §4 prescribes for states with broken SSL. No new dependency, no per-machine setup, nothing to regenerate when the certificate rotates.

**Certificate fingerprint check.** Because verification is off, the server isn't authenticated, so `check_server_fingerprint()` runs once at the top of every scrape: it reads the SHA-256 of the certificate the server actually presents and compares it to `EXPECTED_LEAF_SHA256` (`4cc054e0...07dd`, recorded from the SSL Labs scan). A mismatch logs a loud warning and emits a `tls_fingerprint_changed` JSONL event; the observed value also goes into a `tls_fingerprint` event on every run.

It is **a warning, not a failure** — deliberately. A hard failure would stop the pipeline on rotation day for an entirely routine event. Be clear-eyed about what that buys: this is an audit trail, not enforcement. A warning nobody reads prevents nothing. Its real value is that the fingerprint is in the run log, so you can tell after the fact whether and when the certificate changed. It is a strict improvement over bare `verify=False` at no extra cost, but it is **not** equivalent to keeping verification on.

When the warning fires, check the new fingerprint against the site in a browser, then update `EXPECTED_LEAF_SHA256`.

**If you'd rather keep verification on**, two options, neither currently wired up:

- `pip install truststore` and call `truststore.inject_into_ssl()` at startup — routes verification through the OS trust store, which on Windows and macOS completes the chain via AIA like the browser. Would **not** help on Linux (OpenSSL does no AIA fetching).
- Build a CA bundle of certifi's roots plus the omitted intermediate and pass its path as `verify=`. Keeps full verification but needs a per-machine setup step and regeneration on each rotation.

### Coverage and the legacy archive

**CFRS does not hold ND's history.** Per the [SOS's own transition notice](https://www.sos.nd.gov/elections/campaign-finance-statement-interests/search-reports):

- **CFRS** (`cfrs.sos.nd.gov`) launched **January 2026** and holds **2025 year-end reporting and later**.
- **Reports filed 2024 and earlier** live in the **Campaign Finance Archive** at [`cf.sos.nd.gov/search/cfsearch.aspx`](https://cf.sos.nd.gov/search/cfsearch.aspx) — a separate legacy ASP.NET WebForms application (v1.6.0.1, © 2013) whose year dropdown offers 2014–2025.
- ND is migrating history into CFRS on a published timeline: **launch Jan 2026 → data migration summer 2026 → transfer complete year-end 2027**, with CFRS intended to hold **a full five years by January 2028**.

Consequences for this scraper:

- **There is deliberately no hardcoded start year.** An earlier version pinned `START_YEAR = 2014`, inferred from the legacy portal's dropdown — that was wrong, and worse, a fixed floor would silently drop years as ND migrates them in. The catalog is now the sole source of truth: no `--start-year` means "everything the catalog offers". `CFRS_EARLIEST_KNOWN_YEAR = 2025` exists as documentation only and filters nothing. A regression test asserts a pre-2025 catalog entry is picked up by a plain run.
- **Expect ~2 years of data today, growing over time.** Re-running the scraper after each migration milestone should pick up more history for free.
- **The legacy Archive is not scraped.** It has no bulk export — it's a search-and-view WebForms app, so a sweep would mean replaying `__VIEWSTATE`/`__EVENTVALIDATION` postbacks and most likely harvesting per-filing PDFs. That's a separate project of comparable size to this one, and it would be partly throwaway work since CFRS is absorbing the same records by 2028. Options if 2014–2024 is needed sooner: build that scraper, or request a bulk extract from the SOS elections office (`soselect@nd.gov` / 701-328-4146) — the [Accountability Project](https://publicaccountability.org/datasets/home/) obtained ND 2014–2022 data by records request rather than scraping.

### Independent expenditures — a known gap

**IE spending is absent from the bulk exports.** Proven against real data:

- `Registration` lists three Independent Expenditure Committees (`RegistrantID` prefix `104`): North Dakotans for Public Schools, Prairie Leadership Partners, StrongND Fund.
- Those three have **zero rows** in `Contributions` or `Expenditure`. `Expenditure_2025`'s RegistrantID prefixes are only `101`/`102`/`103` — no `104` at all.
- `FiledReports` nonetheless shows **6 "IE Report" filings** from exactly those committees, so the activity is disclosed; it just never reaches the Data Download CSVs.
- The IE vendor `Edgerton Strategies` appears nowhere in either export.

There *is* a public grid for it — `TrackFinance_IndependentExpenditures` at the same `generateExportGridDataExcel` endpoint, `Referer: /public/trackfinance?tab=IE`, filter `{"TransactionCategory":"IE","TransactionYear":"2026","IsPublic":true}`. It is **not wired up**, for three concrete reasons:

1. **76% of it is duplicate contributions.** 4,548 of its 6,031 rows for 2026 match a `Contributions_2026` row exactly on amount + date + normalized name tokens. The `TransactionCategory: "IE"` filter is largely ignored server-side — `RecipientType` carries the *contributor* vocabulary (`Individual`/`Candidate`/`Self`) and the max transaction date matches Contributions exactly. Ingesting it wholesale would double-count ~$4.2M.
2. **The genuinely-IE rows multiply-count.** Only **52 rows** have `Stance` or `AssociatedCandidate` populated, and they represent just **16 distinct payments** — one row per candidate the payment supported. `Edgerton Strategies` $2,000 appears **18 times**. Naive sum $208,647.42; correct total **$70,447.14**. Any ingestion must dedupe on `(payee, amount, date)`.
3. **There is no spender column.** The grid has no `RegistrantID`, no `CommitteeName` — nothing identifying who made the expenditure. `committee_name` is a tier-1 required column, so these rows cannot be loaded as `expenditures` without either leaving it blank (failing validation) or inventing a value.

A fourth practical snag: the grid's addresses are a **third** format, and internally inconsistent — 4,147 of 5,168 omit the country slot (`311 E. Superior St., Unit 1105, Duluth, MN, 55805`) while 1,021 include it (`PO Box 1091, Bismarck, ND, UAS, 58502`). `parse_grid_address()` anchors on the country position and would silently drop ~80% of them.

If IE becomes a priority, the shape of the work is: filter to rows with `Stance`/`AssociatedCandidate`, dedupe to distinct payments, resolve the spender (probably by joining `FiledReports` IE Report filings on `FiledDate`), and generalize the address parser to tolerate a missing country field.

### Categories

Five, confirmed against the live catalog as of 2026-08. The scraper renames each download to a stable timestamp-free local name so the parser never has to know generation times:

| Source category | Local file | Feeds | Scope group |
|---|---|---|---|
| `Contributions` | `contributions_{year}.csv` | contributions | `contributions` |
| `Expenditure` (singular) | `expenditures_{year}.csv` | expenditures | `expenditures` |
| `Registration` | `committees_{year}.csv` | committees **and** candidates | `committees` |
| `FiledReports` | `filed_reports_{year}.csv` | `amended` + `filing_id` enrichment | `reports` |
| `ReportingSchedule` | `reporting_schedule_{year}.csv` | nothing — archived only | `reference` |

`reports` is pulled under every scope (it enriches transactions and is filer metadata, ~140 KB/year). `reference` is pulled on full runs only. Categories the map doesn't recognize are still downloaded, under a slug derived from their own label and grouped `other`.

Note that `ReportingSchedule` is **forward-dated**: the 2026 filing cycle's schedule is published as `ReportingSchedule_2027` because the year-end report is due in January 2027. The scraper therefore applies no default upper year bound.

### The roster grid — second acquisition path

**Nothing in the Data Download catalog carries office, district, party or a filer address.** The public "Get to Know" grids do, and they can be exported wholesale:

```
POST https://cfrs.sos.nd.gov/api/Common-Service/DataGrid/generateExportGridDataExcel
Referer: https://cfrs.sos.nd.gov/public/gettoknow?tab=candidate

{"moduleType":"PUBLIC","gridName":"GETTOKNOW_CANDIDATECOMMITTEES",
 "filterRequest":{"SortColumn":"registrationDate","SortDirection":"desc",
                  ...all 15 filter keys empty/null...},
 "pageName":"PUB_GTK_CNCM","fieldType":"G"}
```

Empty/null filters mean "everything". Note the `Referer` must point at the grid's own page, not the Data Download tab. Lands as `candidate_committees.xlsx`; scope group `committees`, so `--entities`, `--committees` and `--candidates` all pull it. It is **not year-scoped** — one all-cycles snapshot — so it's re-fetched every run like any year-less export.

The SPA bundle enumerates five grid page codes (`PUB_GTK_CNCM`, `_ELE`, `_OFC`, `_PTY`, `_VIOL`). Only `CNCM` carries filer attributes, which is why it's the only one wired up; the others are elections/offices/party reference lists and a violations register with no home in the five-relation schema.

**Response envelope** — confirmed 2026-08. Unlike the Data Download path, this endpoint returns the workbook inline as base64, not a link:

```json
{
  "isSuccess": true,
  "responseData": { "fileBytes": "UEsDBBQAAAAIA…<base64 xlsx>" },
  "message": null,
  "skipRecords": null
}
```

`_resolve_grid_body()` decodes `responseData.fileBytes` directly and verified byte-identical against the real workbook. Two guards come with it:

- `isSuccess: false` raises immediately, quoting `message` — no silent empty file.
- A non-null `skipRecords` logs a warning and emits a `grid_export_skiprecords` event. Nothing observed populates it, but a silently row-capped export is exactly the failure mode Wisconsin hits (see `scrapers/wisconsin.py`), so it's surfaced rather than trusted.

Three fallbacks are retained behind the primary path in case CFRS reshapes the envelope — its Data Download siblings already differ between releases: the raw file inline, a JSON-wrapped http URL, or a JSON-wrapped S3 key resolved via `LINK_URL`. If none apply it raises with a body excerpt. All paths are stub-tested. A failure here is caught broadly and counted as one file error rather than aborting the run — the roster is optional enrichment on a second endpoint, and losing it must not discard a good catalog scrape.

> Capturing this yourself: the response body is a ~50 KB single-line JSON string, so DevTools' Response tab may render it unhelpfully. Right-click the request → **Open in Sources panel** shows the full body.

### Columns

Confirmed from real downloads. All dates are ISO `YYYY-MM-DD`; amounts are plain decimals with four places (`960.6000`).

**`Contributions`** — `RegistrantID`, `CommitteeName`, `CandidateName`, `TransactionType`, `TransactionCategory`, `TransactionDate`, `TransactionAmount`, `ContributorPayeeType`, `ContributorPayeeName`, `ContributorAddress`, `EmployerName`, `FiledDate`

**`Expenditure`** — `RegistrantID`, `CommitteeName`, `CandidateName`, `TransactionType`, `ExpenditureType`, `ExpenditurePurpose`, `TransactionDate`, `TransactionAmount`, `RecipientType`, `RecipientName`, `RecipientAddress`, `FiledDate`

**`Registration`** — `RegistrantID`, `CommitteeName`, `CandidateName`, `CommitteeType`, `CommitteeSubType`, `RegistrationDate`, `CommitteeStatus`

**`FiledReports`** — `RegistrantID`, `CommitteeName`, `CandidateName`, `ReportName`, `ReportType`, `StartDate`, `EndDate`, `DueDate`, `FiledDate`, `ReportVersion`

**`ReportingSchedule`** — `ElectionName`, `ReportingCycle`, `ReportingPeriodDescription`, `FormType`, `ReportType`, `BeginDate`, `Enddate`, `DueDate`

**roster grid** — `CommitteeName`, `CandidateName`, `CommitteeAddress`, `Office`, `District`, `Party`, `CommitteeStatus`. **No `RegistrantID`**, which is what forces a name-based join (see [Parser](#parser)). Note it has an `OfficerName` *filter* but no officer column in the output, so treasurer remains unavailable.

Three traps in that list:

- **`TransactionType` is a constant.** It is literally `"Contributions"` on every contribution row and `"Expenditures"` on every expenditure row. The real vocabulary lives in `TransactionCategory` and `ExpenditureType`, which is what the parser writes to `transaction_type`.
- **`ContributorAddress` / `RecipientAddress` are a single concatenated blob**, with runs of spaces where the empty sub-fields were: `"PO BOX 179   Minot ND 58702  "`. There are no separate city / state / ZIP columns.
- **`RegistrantID` prefix encodes the filer class** — `101` candidate, `102` PAC, `103` party, `104` independent expenditure. Not relied on by the parser (`CommitteeType` is explicit) but useful when spot-checking.

Columns are still resolved through an ordered candidate-spelling list against a normalized header index, with the confirmed CFRS name first — CFRS has renamed columns between releases, and this way a rename empties one field instead of crashing the parse. `python3 src/pipeline/parsers/north_dakota.py --show-headers` prints every raw header next to the field it resolved to.

---

## Scraper

`src/pipeline/scrapers/north_dakota.py`

**Flow.** Enumerate the catalog (paginated, `pageSize=100`) → filter to the requested vertical/horizontal scope → resolve a signed link per file and stream it to `raw/` → upsert the manifest.

**Shape-agnostic response handling.** Both endpoints' JSON envelopes differ between CFRS releases, so the scraper walks the decoded response recursively rather than indexing a key path. For the listing, every string is tested against a filename regex and category/year are derived from the filename itself; for the link, the first `^https?://` string wins. Full S3 keys, CloudFront URLs and bare filenames are all accepted. If the listing walk finds nothing the scraper raises with the first 1 KB of the body rather than silently downloading zero files. Stub-tested against three listing envelope shapes and three link-response shapes.

**Pagination** stops as soon as a page contributes no new keys — the endpoint's total-count field name isn't stable either, so that's more reliable than trusting a reported total. `max_pages=50` guards against an endpoint that ignores `pageNumber`.

**Manifest.** `data/North Dakota/manifest.csv`, columns `category, year, source_file, local_file, downloaded_at, row_count`. `source_file` holds the *timestamped* source name, which makes incremental runs precise: a year is re-downloaded only when ND actually regenerated it. Rows are **upserted**, not appended — source names change on every refresh, so an append-only manifest would accumulate a stale row per run.

Re-fetch happens when any of these hold: `--force`; an explicit year range was requested; the source timestamp changed; or the year is the current calendar year.

**Encoding and archives.** UTF-16 and BOM-prefixed UTF-8 bodies are normalized to plain UTF-8 on write. `.zip` responses are extracted in memory and CSV members written out (multi-member archives get numeric suffixes). ND currently ships plain CSV, but the SPA bundle has a zip-extraction path, so a future export may be zipped.

**Flags.** Full vertical (`--force`, `--start-year`, `--end-year`) and horizontal (`--transactions`, `--entities`, `--contributions`, `--expenditures`, `--candidates`, `--committees`) sets. `--candidates` and `--committees` both pull `Registration`, since one export feeds both relations.

**Expected runtime.** Unmeasured against the live host. Volume is small — the 2026 contributions file is 853 KB / 5,340 rows — and CFRS currently holds only 2025-onward, so a full backfill is a handful of files and likely well under a minute.

**Limitations.**

- Pre-2025 filings are not in CFRS at all (see [Coverage](#coverage-and-the-legacy-archive)) and are out of scope for this scraper.
- Signed CloudFront links are short-lived, so each is resolved and fetched immediately rather than batching resolutions up front.
- `time.sleep(0.5)` between files. Not a documented rate limit, just politeness.

---

## Parser

`src/pipeline/parsers/north_dakota.py`

**Order.** Roster grid first, then `Registration` (which the roster enriches, and whose registry carries `office` onto transactions), then `FiledReports`, then contributions and expenditures.

**Roster join.** The grid has no `RegistrantID`, so `Registration` rows are matched on normalized committee name first, then on an honorific-stripped `(FIRST, LAST)` name key — the grid writes `"Mr. Coachman, Michael"` where Registration writes `"Doug Goehring"`, so `name_key()` normalizes both orderings and strips `Mr./Ms./Mrs./Dr./Hon.` prefixes plus `Jr./Sr./III/MD/PhD`-style suffixes. Registration wins wherever it already has a value; the roster only fills blanks, because Registration is the authoritative per-cycle record while the roster is an all-cycles snapshot.

Measured against real data: **479 of 482 registrations enriched (99.4%)** — 304 by committee name, 169 by name key. Resulting tier-2 lift:

| Field | Before | After |
|---|---|---|
| `candidates.office` | 0.0% | **98.6%** |
| `candidates.district` | 0.0% | **91.0%** |
| `candidates.party` | 0.0% | **82.8%** |
| `committees.city` / `zip` | 0.0% | **70.3%** |
| `contributions.office` | 0.0% | **34.4%** |
| `expenditures.office` | 0.0% | **18.1%** |

`contributions.office` tops out around 34% because only candidate-committee filers have an office at all — PACs and party committees legitimately have none, and they account for the remainder.

Seven candidate name keys are ambiguous (the same name twice in the grid). Rather than dropping them, fields are merged **per field**: any field where all matching rows agree is used, and only the disagreeing ones are withheld. Six of the seven disagree on `office` — usually the same person in two cycles running for different seats, which the grid can't disambiguate because it carries no election year. Withholding just that field keeps their `party` and `district`.

**Key transformations.**

- **`committee_name` falls back to `CandidateName`.** CFRS leaves `CommitteeName` empty for filers registered as `CommitteeSubType = "Candidate"` — a candidate filing personally, 177 of 482 registrations in 2026 — so only 79% of contribution rows carry one. `committee_name` is a tier-1 required column at ≥99% fill, so without this fallback the state fails validation. With it, fill is 100% and the committee name is the candidate's own name, which is what the filer actually is.
- **Address blob → city / state / ZIP.** `parse_address()` anchors on the trailing `ST ZIP` pair (matches 99.8% of non-blank values), then recovers the city from the remainder. CFRS usually separates street from city with a run of 2+ spaces, so a clean-looking last segment is trusted verbatim — that's what preserves `Watford City`, `Grand Forks`, `W Fargo` and `L Anse`. When the separator is missing the parser walks tokens right-to-left, stopping at anything numeric, a street/unit keyword, or a lone letter *preceded by* one (so `Suite B Fargo` → `Fargo`, while `L Anse` keeps its `L`). Street address is discarded — the schema has no column for it. Result: 4,752 of 4,762 non-blank addresses yield a city, across 401 distinct city names, with no numeric or single-letter artifacts.
- **`transaction_type`** comes from `TransactionCategory` / `ExpenditureType`, never the constant `TransactionType`.
- **`amended` and `filing_id` are joined from `FiledReports`** on `(RegistrantID, FiledDate)` — transactions carry a filed date but no report identifier, so that's the only available link. `amended` is set only when every report matching the key agrees (≈5% of keys have both an Original and an Amended filed the same day; those stay blank rather than guessed), and `filing_id` only when the key maps to a single `ReportName`. Against real data: 4,335 rows `0`, 245 rows `1`, 760 unresolved. Note ~40% of `FiledReports` rows carry no `RegistrantID` at all (and no name either) and are unusable for the join.
- **`RecipientType` → `category`.** The payee classification has no home in the canonical expenditure schema. `columns.py` documents `category` as per-state-only and drops it at aggregate time, so this preserves a 42%-filled real column without polluting anything cross-state.
- **`election_year` comes from the filename.** No source column carries it; the exports are per-year.
- **Candidate names are `First [Middle] Last`**, not `Last, First`. `candidate_first` / `candidate_last` are the first and last whitespace tokens.

**person_id model: `committee`.** `RegistrantID` may or may not be stable across cycles — one year of `Registration` data can't settle it. The `committee` model is safe either way: it groups by `(state, candidate_name, office, district)` and takes `min(state_filer_id)`, so it collapses per-cycle IDs if they differ and is a no-op if they don't. Roster enrichment matters here: with `office` and `district` now populated on ~98%/91% of candidates, grouping is genuinely `(name, office, district)` rather than degenerating to name alone, so same-name candidates for different seats no longer merge.

**Limitations.**

- **`treasurer_name`, `jurisdiction` and `incumbent` remain empty.** No CFRS export publishes them — the roster grid has an `OfficerName` filter but no officer column in its output. Source limitation, not a parser gap; don't synthesize them.
- **The roster is a single all-cycles snapshot with no election year.** So office/district/party reflect a filer's *current or most recent* race, not necessarily the race for the year a given transaction belongs to. For a filer who ran for different seats in different cycles this can attach the wrong office to older rows. The per-field agreement rule limits the damage for names that appear twice, but a filer appearing once with a since-changed office is not detectable from this source.
- **`loans_debts` is always empty.** CFRS publishes no loan or debt export. `"Campaign Loan Repayment"` appears as an `ExpenditurePurpose` value, but that's a disbursement, not a loan record, and stays in expenditures. A `_LOAN_TYPE_RE` guard is in place so that if CFRS ever adds a loan type to an existing export those rows route to `loans_debts` instead of being counted as contributions.
- **No occupation column**, and `EmployerName` is only ~6% filled.
- **No deduplication of amended filings.** It isn't yet established whether an amended report's transactions are published *alongside* the originals or *replace* them. If double-counting shows up, the `amended` and `filing_id` columns give enough to add a `seen`-dict pass without a re-scrape. Worth checking explicitly once two cycles are on disk.

---

## Data Notes

- **Filenames carry a generation timestamp.** Nothing downstream should assume a stable source filename; the manifest's `source_file` column is the only place it's tracked.
- **Fill rates pass straight through from the source.** `contributor_name` and `contributor_type` 85.2%, addresses 80.5%, `payee_name` 42.5%, `purpose` 51.4% — each matches its source column exactly, so nothing is being lost in translation. The gaps are aggregate lines: `Total - $200 or less` contributions and `Lumpsum` expenditures have no itemized counterparty by law.
- **Cross-year enrichment gaps are expected on partial scrapes.** With only 2026 `Registration` on disk, 84.6% of 2025 expenditure rows match a registry entry (versus 99.8% of 2026 contributions), and `aggregate.py` blanks `candidate_name` on the unmatched ones. A full backfill fixes this — always scrape `Registration` for every year in scope.
- **`ReportingSchedule` is downloaded but never read.** It's a filing-deadline calendar (one row in the 2027 file). Archived for completeness only; `--show-headers` reports it as "not consumed by this parser".
- **`state_filer_id` is real and fully populated** (`RegistrantID`, 100%), so `has_filer_id=1` in `src/aliases/states.csv` is correct for ND.
- **Alias coverage is complete.** All 9 committee types, 6 contributor types, 5 contribution categories, 5 expenditure types and 14 office values observed in real data are mapped. `committee_type` is written as `"CommitteeType -> CommitteeSubType"` (the Wisconsin convention) because neither level suffices alone: `CommitteeType` collapses PAC / Multicandidate / Ballot Measure into one bucket, while `CommitteeSubType` labels ND's Independent Expenditure Committees as plain `"Organization"`.
- **`Tax Commissioner` is a new canonical office label.** ND elects one to head the state tax department. It isn't a comptroller (which audits accounts), so it gets its own label rather than being folded into `State Comptroller`. `Public Service Commissioner` maps to `Public Utility Commissioner` and `Supreme Court Justice` to `State Supreme Court Justice`, both matching existing WA labels; `Governor and Lt. Governor` maps to `Governor/Lt. Governor Ticket` alongside KY and MD.
- **`party` is written raw** (`"North Dakota Republican Party"`, `"North Dakota Democratic-NPL Party"`, `"Independent"`, `"Libertarian Party of North Dakota"`). `src/aliases/parties.csv` doesn't exist in this repo yet, so party normalization is left to whenever that file is introduced — consistent with every other state.
- **`UAS` is a typo in the source.** The roster address's country slot reads `UAS` about three times as often as `USA`. `parse_grid_address()` deliberately does not validate that slot, or it would drop the majority of addresses.

---

## First live run checklist

The scraper has never talked to the live host from this pipeline. Start small:

```bash
# 1. One year — confirms the catalog endpoints, the signed GET, and the
#    roster grid (--entities scope is what pulls the grid)
python3 src/pipeline/scrapers/north_dakota.py --start-year 2026 --contributions --entities

# 2. Confirm the catalog was enumerated and files landed under stable names
ls -la "data/North Dakota/raw/"
cat "data/North Dakota/manifest.csv"

# 3. Confirm headers still resolve (nothing should be (unmapped))
python3 src/pipeline/parsers/north_dakota.py --show-headers

# 4. Full backfill, then a real pipeline run
python3 src/pipeline/scrapers/north_dakota.py
python3 src/main.py sync ND
```

Things to check specifically:

- **Does the catalog list more than the five known categories, or more years than expected?** An unrecognized label lands under `other` with a slug derived from itself — add it to `_CATEGORIES` with the right relation group.
- **Watch for a `skipRecords` warning on the roster grid.** If it fires, the roster is truncated and the enrichment join will silently under-cover; the grid would then need paging or filter-splitting the way Wisconsin does.
- **What years does the catalog actually list?** Expect 2025 and 2026 only. There is deliberately no hardcoded lower bound, so as ND migrates history in (through 2027) newly available years are picked up automatically — no code change needed.
- **Re-run the scraper immediately** and confirm past years are skipped while current-year files re-download — that exercises the timestamp comparison against real regenerated filenames.
- **Then check for amended double-counting** (see Parser limitations) once two or more cycles are on disk.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-04 (catalog + roster grid paths) |
| Parser | 2026-08-04 (verified against real 2025/2026 downloads + roster export) |
| Alias CSVs | 2026-08-04 (committee/contributor/transaction/expenditure/office all populated) |
