# Ohio — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Ohio (OH) |
| **Source** | [Ohio Secretary of State CFDISCLOSURE — File Transfer Page](https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:73) |
| **Access method** | Plain GET requests to pre-generated bulk CSV files, via curl_cffi (browser TLS impersonation — see Scraper section) |
| **Coverage** | 1990 – present, confirmed for the Candidate Committee group |
| **person_id model** | `committee` — MASTER_KEY may or may not be stable across a candidate's whole career; grouping by (name, office, district) is safe either way |

**Status: validated end-to-end against real downloaded data for the Candidate Committee group.** A full sync (scrape → parse → validate → tabulate → spot-check queries) was run against real `ACT_CAN_LIST.CSV`, `CAC_CON_2026.CSV`, and `CAC_EXP_2026.CSV` files: 411,757 contributions and 17,292 expenditures parsed, tier-1 validation passed, and `queries.py` returned real, recognizable names and plausible totals (e.g. $10.5M raised by Amy Acton's 2026 gubernatorial campaign). **The PAC and Party groups were not sampled** — their files are assumed to share the Candidate group's column layout (see Parser section) and the scraper/parser were only smoke-tested for graceful failure (a garbage header is skipped with a loud `file_parse_error`, not silently mismapped), not against real PAC/Party data. Confirm those before trusting PAC/Party output.

### How this was found (useful context if something breaks)

Three iterations preceded this design, each ruled out by an actual run:

1. **Advanced-search scraping** — the original approach drove CFDISCLOSURE's Contributions/Expenditures/Candidate-Committee search forms directly. Every request came back `403 Forbidden`, including the very first GET.
2. **403 root cause** — confirmed the same page loads fine in a real browser from the same network, ruling out an IP block. That combination (browser succeeds, `requests` with a full Chrome User-Agent still 403s) is the signature of TLS/HTTP2 fingerprinting, not a missing header. Fixed by switching to [curl_cffi](https://github.com/lexiforest/curl_cffi)'s browser-impersonation mode (`Session(impersonate="chrome136")`) — a different HTTP client, not a browser, not Playwright.
3. **Past the 403s, every search still failed** — it turned out Ohio's advanced search silently refuses to return more than 10,000 records ("Users attempting to query very large amounts of data ... will be required to narrow the search criteria"), and a blank/near-blank search (needed for bulk collection) exceeds that on every entity type. That same "too many records" response links to exactly the right tool: the **File Transfer Page**, described in its own text as where "persons seeking large amounts of campaign finance data" should go. That page is what this scraper now uses, and it turned the whole problem from "reverse-engineer a search UI's row cap" into "download some files."

---

## Raw Data Structure

`f?p=CFDISCLOSURE:73` has three tabs (`P73_TYPE=CAN`/`PAC`/`PARTY`), each a plain bookmarkable GET listing pre-generated bulk files. Every row's "Download" link is a plain GET to `f?p=CFDISCLOSURE:72:::NO::P72_GETID:<id>` — no session dance, no row cap, since it's just streaming a pre-built file.

Raw files in `data/Ohio/raw/` (slug in `candidates`, `pacs`, `parties`):

| File pattern | Content | Verified? |
|---|---|---|
| `entities_{slug}_active.csv` | Active-entity roster | Candidates: yes. PACs/Parties: no |
| `contributions_{slug}_{year}.csv` | Bulk contributions for that (group, year), 1990–present | Candidates: yes. PACs/Parties: assumed same layout |
| `expenditures_{slug}_{year}.csv` | Bulk expenditures, same grain | Candidates: yes. PACs/Parties: assumed same layout |
| `cover_pages_{slug}.csv` | Aggregate per-filing totals (not itemized) | Downloaded, not parsed |
| `contributions_{slug}_supp_*.csv`, `expenditures_{slug}_supp_*.csv` | One-off per-committee files (mostly legislative leadership funds) | Downloaded, not parsed by default — see Data Notes |

### Contributions columns (`CAC_CON_*` — verified against a real 2026 file, 411,781 rows)

`COM_NAME, MASTER_KEY, REPORT_DESCRIPTION, RPT_YEAR, REPORT_KEY, SHORT_DESCRIPTION, FIRST_NAME, MIDDLE_NAME, LAST_NAME, SUFFIX_NAME, NON_INDIVIDUAL, PAC_REG_NO, ADDRESS, CITY, STATE, ZIP, FILE_DATE, AMOUNT, EVENT_DATE, EMP_OCCUPATION, INKIND_DESCRIPTION, OTHER_INCOME_TYPE, RCV_EVENT, CANDIDATE_FIRST_NAME, CANDIDATE_LAST_NAME, OFFICE, DISTRICT, PARTY`

`SHORT_DESCRIPTION` is a genuine schedule/transaction-type classifier (e.g. `"31-A  Stmt of Contribution"` vs `"31-J-1 In-Kind Cont Rcvd"`) — something the search UI never exposed. `REPORT_KEY` is a real per-filing ID, used as `filing_id`.

### Expenditures columns (`CAC_EXP_*` — verified, 17,317 rows in the sampled 2026 file)

`COM_NAME, MASTER_KEY, RPT_YEAR, REPORT_KEY, REPORT_DESCRIPTION, SHORT_DESCRIPTION, FIRST_NAME, MIDDLE_NAME, LAST_NAME, SUFFIX_NAME, NON_INDIVIDUAL, ADDRESS, CITY, STATE, ZIP, EXPEND_DATE, AMOUNT, EVENT_DATE, PURPOSE, INKIND, CANDIDATE FIRST NAME, CANDIDATE LAST NAME, OFFICE, DISTRICT, PARTY`

Note the candidate name columns use spaces here (`"CANDIDATE FIRST NAME"`) but underscores in the contributions file (`CANDIDATE_FIRST_NAME`) — same logical field, inconsistent naming between the two exports. The parser resolves both via alias lists (`_CONTRIB_ALIASES`/`_EXPEND_ALIASES`), not a single hardcoded name.

### Entities columns (`ACT_CAN_LIST.CSV` — verified, 761 rows)

`COM_NAME, MASTER_KEY, COM_ADDRESS, COM_CITY, COM_STATE, COM_ZIP, TREA_FIRST_NAME, TREA_LAST_NAME, TREA_MIDDLE_NAME, TREA_SUFFIX, TREA_ADDRESS, TREA_CITY, TREA_STATE, TREA_ZIP, DEP_FIRST_NAME, DEP_LAST_NAME, CANDIDATE_FIRST_NAME, CANDIDATE_LAST_NAME, OFFICE, DISTRICT, OFFICE, SPONSOR`

**Header quirk (verified):** "OFFICE" appears twice. The real second column is PARTY — confirmed positionally against real rows (e.g. index 18 = `"HOUSE"`, index 20 = `"REPUBLICAN"`). `csv.DictReader` would silently keep only the *second* OFFICE value (Python dict construction: later key wins) and lose the real office entirely — this file is parsed with plain `csv.reader` and positional indexing specifically to avoid that. If the header ever changes, the parser detects the mismatch and skips the file with a `file_parse_error` rather than guessing.

### Also available, not currently parsed

- `CAN_COVER.CSV` ("Candidate Cover Pages") — per-filing aggregate totals (`TOTAL_CONTRIBUTIONS`, `TOTAL_EXPENDITURES`, `BALANCE_ON_HAND`, `OUTSTANDING_LOANS_OWED`, etc.), 35,939 rows in the sampled file. This is filing-level summary data, not itemized transactions, so it doesn't map cleanly onto any canonical table — downloaded (`cover_pages_{slug}.csv`) but unused. Could be useful for a future totals cross-check against the itemized contributions/expenditures sums.

---

## Scraper

`src/pipeline/scrapers/ohio.py`

Lists all three File Transfer Page tabs (Candidate/PAC/Party), classifies each row by its label text (active roster, cover pages, year-tagged contributions/expenditures, or a one-off per-committee file), and downloads whichever categories are in scope for the requested run. Years are discovered from the listing itself — there's no `EARLIEST_YEAR` constant to keep in sync with the site.

**Requires `curl_cffi`** (`pip install curl_cffi==0.15.0`) — plain `requests` gets a 403 on every request against ohiosos.gov regardless of headers (TLS/HTTP2 fingerprinting; see "How this was found" above). `curl_cffi.requests` is a drop-in-compatible API, imported as `from curl_cffi import requests` so the rest of the module reads like a normal `requests`-based scraper.

**Incremental updates:** each listing row carries the file's real `DATE_MODIFIED` from the site. The manifest stores this per file and skips re-downloading unless it changed (or `--force`). This naturally covers "always refresh the current year" — the current year's file gets a new `DATE_MODIFIED` every time a new filing lands — without any special-cased year logic.

**Limitations:**
- PAC and Party tabs were never actually listed against the live site (no sample was available) — the scraper code path is identical to the Candidate tab's (same listing/classification/download logic, just a different `P73_TYPE`), so it's very likely to work, but hasn't been confirmed.
- The listing itself wasn't seen paginated in practice (66–94 rows, no pagination control, across the "New Files" and "Candidate Files" tabs), but the scraper logs a loud warning if it ever detects a pagination control on a listing page, since that pagination (like the search UI's) is client-side-JS-only with no non-JS equivalent.
- One-off per-committee "supplemental" files are downloaded (cheap — there were only 18 across the Candidate tab) but not parsed by default; see Data Notes.

**Expected runtime:** untested at full scale; the Candidate group alone is ~74 files (37 years × 2 transaction types + roster + cover pages + 18 supplemental), each up to ~90MB.

---

## Parser

`src/pipeline/parsers/ohio.py`

**Run against real data:** 411,757 contributions + 17,292 expenditures + 743 candidates + 778 committees parsed from real files, passed `validate.py` tier 1 with no hard failures, and `queries.py` returned plausible, recognizable results.

**Header resolution:** name-based (`_CONTRIB_ALIASES`/`_EXPEND_ALIASES`/`_ENTITY_ALIASES`), not positional, for every file except `ACT_CAN_LIST.CSV` (see the duplicate-OFFICE quirk above — positional parsing is required there specifically, and only there). If a required column can't be resolved, the file is skipped with a `file_parse_error` naming the exact header it saw, rather than silently parsing with missing data.

**Contributor/payee name resolution:** individual name (joined `FIRST_NAME MIDDLE_NAME LAST_NAME SUFFIX_NAME`) if present → `contributor_type = "Individual"`; else `NON_INDIVIDUAL` → `"Non-Individual"`.

**Candidate/office population:** unlike the abandoned search-based approach, `CANDIDATE_FIRST_NAME`/`CANDIDATE_LAST_NAME`/`OFFICE`/`DISTRICT`/`PARTY` are present directly on every contribution/expenditure row for the Candidate group — no registry/enrichment step is needed. These columns are expected to be blank for PAC/Party rows (a PAC's or party's transactions aren't tied to a single candidate), which the parser handles by just passing through whatever is present.

**Committee/candidate backfill:** `entities_{slug}_active.csv` only lists *currently active* entities. Any committee (`MASTER_KEY`) seen in a contributions/expenditures file but not in the active roster is still added to `committees.csv` (with `active` left blank — unknown status — rather than assumed inactive), so historical/deregistered committees aren't silently dropped from the aggregate database.

**`transaction_type` = `SHORT_DESCRIPTION`** (e.g. `"31-A  Stmt of Contribution"`, `"31-J-1 In-Kind Cont Rcvd"`) — a real, meaningful classifier, unlike the earlier search-based design which had nothing better than a filing-period label to work with. Add mappings to `src/aliases/transaction_categories.csv`/`expenditure_categories.csv` once the full set of schedule codes is known (only the codes seen in the 2026 sample are in the alias file shipped with this — expect more from other years).

**`employer`** is left blank — Ohio's `EMP_OCCUPATION` column combines employer and occupation as one free-text field with no reliable separator; splitting it would be guessing.

**person_id model:** `committee` — see Overview.

**Known gaps:**
- PAC/Party column layout is assumed, not verified (see Overview).
- Supplemental per-committee files are not parsed (see Data Notes — dedup risk against the yearly bulk files hasn't been checked).
- `CAN_COVER.CSV` (filing-level totals) is not used for anything yet.

**Observed runtime:** ~10s to parse ~430K rows across a candidates roster + one year of contributions + one year of expenditures (single-threaded, streaming write). A full 1990–2026 sync across all three groups (~2,600 raw files if PAC/Party match Candidate's file count) has not been timed.

---

## Data Notes

- **Bare `\r` line endings.** Every Ohio bulk CSV uses old Mac-style bare-`\r` line endings, not `\n` or `\r\n`. Confirmed that Python's default universal-newline text mode (i.e. *not* passing `newline=""`) splits these correctly; `newline=""` (the pattern used by most other states' parsers in this repo) does **not** work here and will parse the whole file as one row. This is the one place Ohio's parser deliberately deviates from the repo's usual file-opening convention.
- **Supplemental per-committee files, dedup unverified.** ~18 one-off files like "All Candidate Contributions - DEWINE HUSTED FOR OHIO" exist alongside the yearly bulk files, mostly for legislative leadership funds. It's not confirmed whether their rows are already included in the corresponding year's bulk file (in which case parsing both would double-count every transaction) or whether these committees are excluded from the yearly exports for some reason (in which case skipping them creates a real gap). **To check:** download one supplemental file and the matching year's bulk file for the same committee, and compare `REPORT_KEY` values — if every supplemental `REPORT_KEY` already appears in the bulk file, they're duplicates and the current skip-by-default behavior is correct; if not, the parser should be updated to include them.
- **Messy source dates.** A handful of contributions in the sampled 2026 file carry dates outside 2026 entirely — as far back as 1991 and as "far forward" as 2028. This isn't a parser bug; the `FILE_DATE` column occasionally contains what look like data-entry errors or very old corrected/amended entries even within a single year's bulk export. `parse_date()` accepts a generous range (1970 through current-year+2) rather than trying to guess which of these are legitimate.
- **Rare encoding artifacts.** A very small number of contributor/payee names decode with stray replacement characters (e.g. one row's name rendered as `"ϻ�"` in a spot-check) — the raw files aren't consistently UTF-8, and are opened with `errors="replace"` (matching this repo's usual convention) rather than a smarter multi-encoding fallback.
- **`contributor_state`/`payee_state`/`*_zip` fill quality.** ~0.1–1% of these fields contain non-US-state codes or malformed ZIPs (e.g. `"0H"`, `"AA"`, `"-43017"`) — tier-2 warnings in `validate.py`, not tier-1 failures. Source data quality issue, not filtered.
- **`category`, `employer`, `jurisdiction`, `incumbent`, `election_year` (candidates)** are 0% filled — no source column maps to any of these for Ohio.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-14 (rewritten around the File Transfer Page after the search-UI approach hit a 10,000-row cap; curl_cffi added earlier the same day to fix TLS-fingerprint 403s) |
| Parser | 2026-07-14 (rewritten around real `CAC_CON`/`CAC_EXP`/`ACT_CAN_LIST` samples; validated end-to-end — tier 1 pass, plausible spot-check results) |
| Documentation | 2026-07-14 |
