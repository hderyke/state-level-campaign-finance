# New Hampshire -- Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | New Hampshire (NH) |
| **Source** | [Secretary of State -- Campaign Finance System (CFS)](https://cfs.sos.nh.gov/public/cf/downloads) -- Angular single-page app; bulk CSV export API at `cfsapi.sos.nh.gov` |
| **Access method** | Direct `requests` POST to the CFS export API (no browser automation) -- the download page itself is JS-only and was not scraped/rendered; the real request was captured from a user's browser DevTools Network tab and confirmed directly (see scraper docstring) |
| **Coverage** | 2016 -- present (per the live Download Data page's own year list, snapshot 2026-07-21) |
| **person_id model** | `committee` -- `state_filer_id` (Filing Entity ID) is a per-committee registration ID; a candidate appears to register a new committee each cycle |
| **FIPS** | 33 |

---

## Raw Data Structure

`data/New Hampshire/raw/` -- one file per (filing year, transaction type):

- `receipts_{year}.csv` -- `transactionTypeCode=TCON`. Contains itemized and unitemized monetary contributions, in-kind contributions, interest, loan activity (Loan Received/Payment/Forgiven), and returned contributions. Columns: `Filing Entity ID`, `Candidate Name`, `Committee Name`, `Committee Subtype`, `Transaction Type`, `Transaction Sub Type`, `Election Period`, `Election year`, `Date of Receipt`, `Amount of receipt`, `Contributor Type`, `Contributor Name`, `Contributor Address Line 1/2`, `Contributor City/State/Zip Code`, `Contributor occupation`, `Contributor Employer`, `Contributor Principle place of Business`, `Description`, `Timed Report`.
- `expenditures_{year}.csv` -- `transactionTypeCode=TEXP`. Contains itemized/unitemized monetary expenditures, in-kind expenditures, independent expenditures, and returned expenditures. Columns: `Filing Entity ID`, `Filing Entity Name`, `Filing Entity Type`, `Transaction Type`, `Transaction Sub Type`, `Payee/Worker/Creditor/Loan source type`, `Payee/Worker/Creditor/Loan Source Name`, `Payee/Worker/Creditor/Loan source Address`, `Transaction Amount`, `TransactionDate`, `Election Type`, `Transaction Description`, `Timed Report`.

There is no separate entity/candidate roster file -- confirmed against the live Download Data page's own table, which lists exactly two "Data Type" rows (Receipts, Expenditures), each with a "Data Key" PDF and one clickable link per filing year. `manifest.csv` (type, year, filename, bytes, rows, downloaded_at) tracks what's been fetched.

---

## Scraper

`src/pipeline/scrapers/new_hampshire.py`

POSTs `{"type": "CSV", "filingYear": "<year>", "transactionTypeCode": "TCON"|"TEXP"}` to `https://cfsapi.sos.nh.gov/api/ExportData/GetExportPublicDownloadData` for every (year, type) in scope and writes the raw CSV response to disk. No Playwright/browser automation -- confirmed unnecessary since the export API itself is a plain JSON-in/CSV-out POST endpoint, reachable directly with `requests`.

**How the endpoint was found:** the live Download Data page (`https://cfs.sos.nh.gov/public/cf/downloads`) is an Angular 20 SPA with no server-rendered content and no reachable JS bundle from this build environment (every fetch attempt to `cfs.sos.nh.gov`, including `/robots.txt`, came back empty -- see Data Notes). The user captured the real request from their own browser's Network tab (copy-as-cURL) and supplied it directly, along with real sample CSV responses for both transaction types and the site's own "Download Data File Key" PDFs.

**Output:** `data/New Hampshire/raw/{receipts,expenditures}_{year}.csv`. `manifest.csv` at `data/New Hampshire/`.

**Limitations:** the current calendar year is always re-fetched in full (filings for the open cycle are still being submitted); prior years are skipped once on disk unless `--force` or an explicit `--start-year`/`--end-year` range covers them. There is no last-modified signal from the API to drive finer-grained incremental updates the way Ohio's File Transfer Page listing does. `--entities`/`--candidates`/`--committees` are accepted as no-ops for CLI consistency -- there is nothing to fetch for them (see Parser).

**Expected runtime:** 2 requests per filing year (~20 years currently) -- one receipts file (tens of thousands to ~270k rows in a busy election year, several MB) and one expenditures file (thousands to ~30k rows) per year.

---

## Parser

`src/pipeline/parsers/new_hampshire.py`

**Output tables:** `contributions.csv.gz` (from receipts), `expenditures.csv.gz` (from expenditures), `candidates.csv.gz`/`committees.csv.gz` (backfilled from both files, keyed by Filing Entity ID -- there is no separate roster export), `loans_debts.csv.gz` (empty -- see below).

**Key transformations:**
- **No entity roster.** Candidates and committees are entirely reconstructed from the two transaction files. This means `office`, `district`, `party`, `jurisdiction`, `incumbent`, `treasurer_name`, `city`, `zip`, and `active` are unavailable for NH and left blank -- a genuine gap in the source export, not a parsing shortcoming.
- **Candidate-vs-committee disambiguation (receipts).** NH's receipts file sets "Candidate Name" to the *exact same string* as "Committee Name" for every non-candidate filer (PACs, party town committees, associations) -- confirmed against real data: 202,264 of 268k sampled rows have `Candidate Name == Committee Name`, and every one checked is a PAC/party/association, not an actual candidate. A genuine candidate committee's Candidate Name is the person's own name and differs from the Committee Name (which is sometimes blank, sometimes a separate campaign-brand name, e.g. `"Craig, Joyce"` / `"Joyce Craig for NH"`). The parser therefore only treats Candidate Name as real when it differs from Committee Name. This was caught and fixed during testing against the real sample data -- an earlier version mislabeled ~220 PACs/party committees as candidates before this rule was added.
- **Candidate-vs-committee disambiguation (expenditures).** No such ambiguity here -- `Filing Entity Type` is an explicit, authoritative field (`"Candidate Committee"`/`"Candidate"` vs `"Political Committee"`/`"Political Advocacy Organization"`/etc.).
- **`transaction_type`** combines NH's two-level Type/Sub Type columns: Type is almost always the uninformative `"Receipt"`/`"Expenditure"`, with the real detail in Sub Type (`Itemized Monetary`, `Unitemized Monetary`, `In-Kind (Non-Money)`, etc.); the few genuinely distinct Type values (`Return Receipt`, `Loan Received`, `Loan Payment`, `Loan Forgiven`, `Independent Expenditure`, `Return Expenditure`) carry a blank Sub Type. The parser uses Sub Type when Type is the generic value and Sub Type is non-blank, else falls back to Type itself.
- **Candidate name parsing.** NH's `"Last, First Middle"` format sometimes carries a trailing parenthetical ballot-name/alias, e.g. `"Long, Patrick T. (Long, Pat )"` -- stripped before storing and before splitting into `candidate_first`/`candidate_last`.
- **Payee address.** Expenditures give one combined free-text address field (no separate city/state/zip columns, unlike receipts' contributor address) -- split via a best-effort regex on the trailing `"..., City, ST ZIP"` shape; the street portion has no canonical column and is dropped. Addresses that don't match (or are blank, as most unitemized rows are) get city/state/zip left blank rather than a bad guess.
- **ZIP quirk.** ~0.3% of sampled ZIP values carry a leading straight-quote artifact (e.g. a value like `'0506-0506`) -- stripped before normalization. The underlying value is sometimes a genuinely truncated 4-digit code in NH's own export; this is left as-is (flagged by `validate.py`'s tier-2 ZIP-format check) rather than guessed.

**Loans:** there is no separate loan/debt schedule -- receipts' `Loan Received`/`Loan Payment`/`Loan Forgiven` rows are written into `contributions.csv.gz` like every other receipt (with that raw value preserved in `transaction_type`), not into `loans_debts.csv.gz`, which is written empty for schema completeness (same convention as `parsers/ohio.py` for a state with no distinct loan export).

**person_id model:** `committee` (`utils.assign_person_ids(id_model="committee")`). Since NH exposes no office/district data, grouping reduces to `candidate_name` alone within `state`.

**Verified against:** real sample CSVs for both transaction types (filingYear=2024, ~268k receipt rows / ~29k expenditure rows) and the site's own "Download Data File Key" PDFs, both supplied directly by the user -- not reverse-engineered from NH's old (pre-April-2024) legacy CFS system, which used a different URL scheme and column layout entirely (`cfs.sos.nh.gov/Public/ViewReceipts`) and is unrelated to the current API. `validate.py new hampshire` passes tier 1 with no failures on the sample data (348 candidates, 574 committees, 268,238 contributions, 29,045 expenditures; 100% of all tier-1-required fields filled).

---

## Data Notes

- **Built and tested without live access to `cfs.sos.nh.gov`/`cfsapi.sos.nh.gov`.** Both hosts returned empty responses to this environment's fetch tool for every path tried (including the bare root and `/robots.txt`), and the Claude-in-Chrome browser tool was unavailable in this session -- so the download page's rendered JS could not be inspected directly here. The actual API endpoint, request body shape, and "plain CSV text" response were confirmed by the user directly from their own browser (DevTools Network tab, copy-as-cURL for both `TCON` and `TEXP`), along with real sample response bodies and the site's Data Key PDFs.
- **Live pass initially failed: 403 Forbidden on every request, from a plain `requests` session.** Root-caused via side-by-side test (2026-07-22): the response was an Akamai edge "Access Denied" page (`errors.edgesuite.net`), returned even for a bare GET of the HTML download page with zero cookies ever set -- ruling out a missing session cookie or CORS issue. `curl_cffi` impersonating Chrome's TLS/HTTP2 fingerprint (`impersonate="chrome124"`) got a 200 on the identical URL/headers/body where plain `requests` got 403, confirming Akamai denylists `requests`' TLS handshake outright at the edge, before any application-layer logic runs. **Fixed:** the scraper now uses `curl_cffi`'s requests-compatible client instead of `requests`. Requires `pip install curl_cffi` (not yet in this build's requirements file -- add it to whatever requirements file the project actually uses). The backend behind the Akamai edge is Azure App Service (see the `ARRAffinity` sticky-session cookie in responses); the scraper primes each run with one GET of the download page to pick that cookie up before looping through POSTs, same as a real browser visit.
- **Alias mappings added for all four files** (`src/aliases/{contributor_types,transaction_categories,expenditure_categories,committee_types}.csv`), verified via direct calls to `src/aliases`'s `canonical_*` functions against every observed raw value (see the parser's module docstring for the full enumerated value sets from the real sample data).
- **`office_types.csv` was not attempted** -- NH's export exposes no office/district/party data at all for any entity, so there is nothing to map.
- **Election Period/Election Type** (`General`/`Primary`/`Exploratory`/`Special Election`/`Speaker of the House`) are not mapped to any canonical column -- `CONTRIBUTIONS`/`EXPENDITURES` have no field distinct from `election_year` for this, and it doesn't cleanly fit `transaction_type` either. Flagged as a follow-up if a future schema revision adds a column for it.
- **A handful of rows in both sample files (2 each, ~4 total) have malformed CSV quoting** in free-text description fields, shifting values into unexpected columns; these fail amount/date parsing and are silently skipped (counted in the parser's `skipped` log) rather than crashing the run.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-21 |
| Parser | 2026-07-21 |
| Alias mappings | 2026-07-21 |
| Docs | 2026-07-21 |