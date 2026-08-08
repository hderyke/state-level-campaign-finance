# Texas — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Texas (TX) |
| **Source** | [Texas Ethics Commission](https://www.ethics.state.tx.us/search/cf/) — bulk campaign finance CSV database |
| **Access method** | Plain HTTP download of a single ~1 GB zip (no Playwright, no API) |
| **Coverage** | Everything filed electronically since 1 July 2000 |
| **person_id model** | `committee` — `filerIdent` is a per-account number, and 425 candidate names hold more than one account, so accounts are collapsed by (candidate_name, office, district) |

**Disk:** budget ~11 GB. The archive is ~1 GB compressed and the members this pipeline extracts expand to roughly 9 GB, and the zip is kept afterwards so members can be re-extracted without re-downloading.

---

## Raw Data Structure

TEC publishes one zip containing 138 CSVs plus two documentation text files. Every record type shares the same nine-field prefix — `recordType`, `formTypeCd`, `schedFormTypeCd`, `reportInfoIdent`, `receivedDt`, `infoOnlyFlag`, `filerIdent`, `filerTypeCd`, `filerName` — which is why one set of helpers in the parser works across all of them.

### Members this pipeline uses

| Member | Rows | Maps to |
|---|---|---|
| `filers.csv` | 20,748 registration rows → 20,125 accounts | candidates + committees |
| `cover.csv` | one row per filed report (195 MB) | candidate party, election date, per-report office |
| `spacs.csv` | 503 | committee → candidate links |
| `contribs_##.csv` | 102 shards, ~34M rows | contributions |
| `credits.csv` | ~101K | contributions (Schedule K) |
| `expend_##.csv` | 13 shards | expenditures |
| `cand.csv` | 190,070 | expenditure enrichment (direct-expenditure beneficiaries) |
| `loans.csv` | 20,148 | loans_debts (Schedule E) |
| `debts.csv` | 7,678 | loans_debts (Schedule L) |
| `expn_catg.csv` | 20 | expenditure category code → label |
| `CFS-ReadMe.txt`, `CFS-Codes.txt` | — | record layouts and code lists (provenance) |

Members deliberately **not** extracted: `pledges.csv`/`pldg_*`, `notices.csv`, `assets.csv`, `travel.csv`, `finals.csv`, `purpose.csv`, `returns.csv` — the canonical schema has nowhere to put them — plus `cover_ss.csv`/`cover_t.csv` and the three duplicate-risk files below.

### The three files that would double-count

`cont_ss.csv`, `cont_t.csv` and `expn_t.csv` hold special-session and special pre-election ("Telegram") report rows. TEC's own README explains they are kept in separate files *because those transactions are re-reported on the next regular campaign finance report*. They are neither extracted nor parsed. Anything that reads the raw archive directly should skip them too.

### Schedules

`schedFormTypeCd` is what says which canonical table a row belongs to. The parser's `CONTRIB_SCHEDULES` / `EXPEND_SCHEDULES` / `LOAN_SCHEDULES` maps are transcribed from CFS-Codes.txt:

| Category | Schedules |
|---|---|
| Contributions | A1, A2, A2SS, AJ1, AL, AS1, AS2, C1, C2, C3, C4 (+ K from `credits.csv`) |
| Expenditures | F1, F2, F3, F4, FL, FS, G, H, I, COHUC2, SPKUCFS |
| Loans | E, EJ, EL, ES (Schedule E) and L (Schedule L outstanding) |
| Pledges | B, BJ, BJSS, BSS, D — not parsed (a pledge is a promise, not a transaction) |

### Filer types

`filerTypeCd` drives the candidate/committee split. Candidate-side: `COH`, `JCOH`, `SCC`, `SPK`. Everything else (`GPAC`, `MPAC`, `SPAC`, `JSPC`, `SCPC`, `ASIFSPAC`, `CEC`, `MCEC`, `PTYCORP`, `LEG`, `DCE`) is an organization.

---

## Scraper

`src/pipeline/scrapers/texas.py`

### The URL matters

The live archive is at **`https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip`**, on TEC's filing-application host.

The obvious-looking `https://www.ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip` is a **stale 2019 snapshot**. On TEC's index page that link is commented out and labelled "As of 11/11/2019", while the live one points at the ethicsefile host. Pointing the scraper at the www URL would silently yield seven-year-old data, so it's recorded in the module as `LEGACY_ARCHIVE_URL` with a do-not-use note rather than left for someone to rediscover.

### Detecting a new publication

TEC renders the archive's as-of date in the link text on its index page — "Campaign Finance CSV Database (As of 07/24/2026)". `published_date()` scrapes that string; it's the most authoritative freshness signal available, since it's TEC's own statement of what the data covers. HTTP `ETag` and `Last-Modified` from a HEAD are recorded alongside and used when the page can't be read.

A no-flag run re-downloads only when the published date, ETag or Last-Modified has moved, **or** when a member file the manifest lists is missing or empty on disk — so a partially-completed extraction can't be mistaken for a finished one. `--force` always re-downloads.

The download streams to `TEC_CF_CSV.zip.part` and renames on success, and verifies the byte count against `Content-Length`. A gigabyte transfer interrupted halfway would otherwise leave a truncated file that looks complete until `zipfile` chokes on it much later.

### Scope flags

`--contributions` / `--expenditures` / `--entities` / `--candidates` / `--committees` / `--transactions` all work, but they scope **extraction**, not download — the archive is one file and comes down whole whenever it has changed.

`--start-year` / `--end-year` are accepted (orc forwards them to every scraper) and **do nothing**, with a warning in the log. The archive has no year dimension: its `contribs_##` / `expend_##` shards are split by internal report id, not by year — the same situation as California's single bulk file.

---

## Party enrichment scraper

`src/pipeline/scrapers/texas.py --party`

A separate, optional mode of `texas.py` above — different hosts, different cadence, never load-bearing for the main TEC pull. It downloads the two fallback sources the enrichment section of `parsers/texas.py` joins onto `candidates.party` wherever TEC's own `cover.csv` leaves it blank: the Texas SOS's legacy race-summary canvass (`elections.sos.state.tx.us`, 1992–2019) and Open States' nightly bulk CSV (`data.openstates.org/people/current/tx.csv`, current TX Legislature — CC0, no API key needed, same unauthenticated export the New York enrichment already uses). `--sos` / `--openstates` scope which of the two run; no flag runs both. Writes `SOS_RaceSummary.csv` and `OpenStates_People.csv` to `data/Texas/raw/` alongside TEC's own files — neither overwrites or is read by the main TEC scrape itself. See the "Party/office enrichment sources" section in `scrapers/texas.py` for exactly how each source's URL/page structure was confirmed, and `docs/states/texas.md`'s Data Notes section above for the matching contract.

A third source, The Green Papers (`thegreenpapers.com/G{YY}/TX`), was tried and removed — its live markup never settled into a stable, parseable contract (office section titles and candidate lines share one flat `<li>`/`<p>` list with no consistent markers, and the per-line format itself differs between decided and upcoming cycles), so it kept degrading to 0 rows rather than surfacing real data.

---

## Parser

`src/pipeline/parsers/texas.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz`, `committees.csv.gz`, `candidates.csv.gz`

**Processing order:** external party-enrichment overlay loaded → filer index → cover sheets → SPAC links → category labels → direct-expenditure beneficiaries → contributions → expenditures → loans → debts → the enrichment overlay applied to whatever `candidates.party` cover sheets left blank.

### Superseded rows are dropped

`infoOnlyFlag = Y` means the record was superseded by a later report that is *also* in the archive. About 2% of contributions and 8% of expenditures carry it. They're dropped and counted as `skipped_superseded` in the parse log — on a real run this is tens of thousands of rows, and keeping them would double-count every corrected filing.

### Every filer gets a committees row

Including candidates. In Texas a candidate files under their own account rather than through a separately registered committee, so the candidate's `filerIdent` is what appears as the recipient on their contributions. Without a committees row for them, those transactions would name a `committee_name` that exists in no table.

### One registration per account

`filers.csv` has one row per *registration*: 613 accounts appear two or three times with different office, address, type and effective dates. The parser keeps the row with the highest `filerEffStartDt` rather than whichever comes last in the file, so the office and status reflect the current registration and the output doesn't depend on TEC's row ordering. An account that re-registered under a non-candidate type has its stale candidates row dropped.

### Names

Every party to a TEC record is stored twice: a display string (`"Lucero, Homero R. (Mr.)"`) and structured components (`*NameOrganization`, or `*NameFirst`/`*NameLast`/`*NameSuffixCd`). `tec_name()` prefers the components and falls back to un-inverting the display string, so the same person comes out as `HOMERO R. LUCERO` either way. Inversion is skipped when the string carries a corporate marker, so `"Smith, Jones & Co"` isn't mangled. This matters because `utils.assign_committee_person_ids()` matches on the name string and doesn't handle comma inversion.

### Committee → candidate linkage

`spacs.csv` is an explicit link: a specific-purpose committee names the candidate it exists to support or oppose. Only `spacPositionCd = SUPPORT` is used (456 of 503 rows) — an OPPOSE committee exists to campaign *against* the named candidate, so attributing its money to them would be actively wrong, and ASSIST/UNKNOWN are too vague. Candidate-type filers get their own name as `candidate_name` directly.

### Direct campaign expenditures

`cand.csv` holds the candidate a direct campaign expenditure was made to benefit — a child record joined to the expenditure on `expendInfoId` (190,070 of them). Without it, a PAC's independent spending "for" a candidate would have no candidate attached anywhere in the output.

### person_id model

`committee`. `filerIdent` is an account number, stable for the life of an account but not per-person: 425 of the 10,788 distinct candidate names hold more than one (someone who ran, terminated, and later filed again, or who moved between COH and JCOH). `assign_person_ids(id_model="committee")` collapses accounts by (state, candidate_name, office, district) onto the lowest `filerIdent`.

**Expected runtime:** roughly 10 seconds per 100 MB of raw input on a single pass, so a full 9 GB parse is on the order of 15 minutes.

---

## Data Notes

- **Schedule K inflates a naive donor ranking.** `credits.csv` (bank interest, investment gains, credits, and contributions returned to the filer) is written to `contributions`, because the canonical schema has no "other receipts" table and dropping it would lose real money movement — the same treatment New York's Schedule E gets. The consequence is visible immediately: run `queries.py texas` and the top "contributors" are US Treasury, Wells Fargo and Edward Jones, because they're paying interest on campaign accounts, not donating. Its `transaction_type` maps to `transaction_category = 'Other'` in `src/aliases/transaction_categories.csv`, so a real donor ranking is `WHERE transaction_category <> 'Other'`.
- **`party` comes from `cover.csv` first, with a two-source fallback behind it.** `filers.csv` has no party column, but the report cover sheets do: TEC's record layout (CFS-ReadMe.txt, Record #4 `CoverSheet1Data`) declares `politicalPartyCd` (DEM, REP, LIB…), `politicalPartyOtherDescr`, `politicalDivisionCd` and `politicalPartyCountyCd`, all keyed on `filerIdent` — an exact join, no name matching. The parser reads it and takes each filer's most recently declared party.

  Whether TEC actually populates that field for CANDIDATE filers (COH/JCOH/SCC/SPK) or only for party-committee filers (CEC/MCEC/PTYCORP) — TEC's own Form C/OH cover sheet has no party box, and `CFS-Codes.txt` defines no party code list — is answered fresh on every run rather than assumed. The parser logs a breakdown by `filerTypeCd`:

  ```
  cover sheets: N reports — politicalPartyCd present for N filers,
  by filer type: {'CEC': ..., 'COH': ..., 'PTYCORP': ...}
  ```

  Whatever that join leaves blank falls through to `scrapers/texas.py --party` + the enrichment section of `parsers/texas.py`, an overlay tried in this priority order (first strict match wins — see those sections for the full matching contract):

  1. **Texas Secretary of State legacy canvass** — `elections.sos.state.tx.us`. Static HTML, no JS: `index.htm`'s election picker maps ~170 election names to a numeric `eleid`, and each election's Statewide Race Summary lives at `elchist{eleid}_state.htm` — a real `<table>` of RACE/NAME/PARTY/CANVASS VOTES/PERCENT, one row per candidate, covering statewide offices, US House/Senate (not matchable — TEC has no federal filers), State Senate/House, State Board of Education and the appellate judiciary. Covers 2000–2019 only (TEC's archive floor to the legacy site's own ceiling); primaries are included, which matters because they carry candidates who lost the primary and never reached a general ballot. No `filerIdent` in SOS data, so the join is name + office + district/year, matched strictly (a disagreeing district disqualifies rather than fuzzy-matches).
  2. **Open States nightly bulk CSV** — `data.openstates.org/people/current/tx.csv`, CC0, no API key (skipped with a warning if unreachable — never load-bearing). Current Texas Legislature members only, so it adds no historical depth over source 1, but it's the only source that speaks to *current* officeholding directly and it reaches past 2019.

  District Court, County Court, District Attorney and other county-certified offices (the bulk of TEC's `JUDGEDIST`/`JUDGESTATCO`/`DISTATTY` filer types) sit outside both sources and stay blank regardless — the parse log's `Enrichment scope` line separates that structural ceiling from a matching failure. Every value the overlay fills is written with provenance (`party_source` ∈ `tx_sos_results`/`openstates`, `match_confidence` ∈ `exact`/`high`) so a consumer can set their own trust bar; a value TEC published itself is never overwritten and carries no provenance tag.
- **`jurisdiction` is ~7%.** The only county field with any coverage is the one attached to the office being sought or held, and ~93% of TEC filers seek statewide or legislative office, which has no county. The filer's *street* county (`filerStreetCountyCd`) is 0% populated across all 20,748 rows and is not used.
- **`election_year` prefers the cover sheet's `electionDt`**, which is the actual election a report was filed for. Only filers with no usable cover sheet fall back to the calendar year of their latest transaction, which drifts — money raised in the January after an election lands in the following year.
- **Schedule L (`debts.csv`) rows have no amount and no date.** TEC's record layout gives Schedule L the lender's identity but no amount field, and its `loanInfoId` is in a different id space from Schedule E's — verified: 20,148 Schedule E ids and 7,678 Schedule L ids, zero overlap, so there is nothing to join the amount back from. The rows are still written (they're the only evidence an obligation was outstanding) with `original_amount` blank and `date` set to the report's received date. A `SUM(original_amount)` over `loans_debts` therefore ignores them, which is the correct behaviour.
- **`amended` marks correction *forms*, not superseded rows.** Superseded rows are dropped outright; `amended = 1` means the surviving row arrived on one of TEC's `COR*` correction affidavit forms.
- **`office` holds TEC's raw codes** (`STATEREP`, `JUDGEDIST`), which is what the source publishes. `src/aliases/office_types.csv` maps all 38 of them; `canonical_office` carries the readable label.
- **`employer` / `occupation` fill rates are low** on the contributions table overall because Schedule K rows (which have no contributor employer concept) and non-itemized rows dilute them; on itemized individual contributions they're well populated.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-25 |
| Parser | 2026-07-25 |
| Docs | 2026-07-25 |
