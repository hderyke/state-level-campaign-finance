# Wyoming — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Wyoming (WY) |
| **Source** | [Wyoming Campaign Finance Information System](https://www.wycampaignfinance.gov/WYCFWebApplication/GSF_SystemConfiguration/SearchContributions.aspx) (Secretary of State) |
| **Access method** | ASP.NET WebForms postback simulation (no REST API) — bulk CSV export for transactions, PDF report generation for entity rosters |
| **Coverage** | 2008 – present (single full-history export, no year-splitting) |
| **person_id model** | `name_hash` — no numeric filer ID exists anywhere in the source (confirmed against both roster PDFs and the transaction exports) |

---

## Raw Data Structure

### contributions_all.csv

Full-history bulk export (~445K rows, ~74 MB) from `SearchContributions.aspx`'s "All" tab (spans Candidate/Candidate Committees, PACs, Organizations, and Political Parties in one query — confirmed by the values in `Recipient Type`). 8 columns, no transaction ID, no election year, no filing ID:

| Field | Description |
|---|---|
| `Contributor Name` | `"LAST, FIRST MIDDLE  (CITY)"` for individuals, `"ORG NAME (CITY)"` for organizations/committees; blank for UN-ITEMIZED rows |
| `Recipient Name` | Committee or candidate name as it appears on the filing |
| `Recipient Type` | `CANDIDATE COMMITTEE`, `CANDIDATE`, `POLITICAL ACTION COMMITTEE`, or `PARTY COMMITTEE` (no `ORGANIZATION` rows observed on the contributions side) |
| `Contribution Type` | `MONETARY`, `IN-KIND`, `LOAN`, `UN-ITEMIZED`, or `ANONYMOUS` |
| `Date` | `M/D/YYYY` |
| `Filing Status` | `FILED`, `PUBLISHED`, `AMEND - ADD`, or `AMEND - DELETE` |
| `Amount` | Plain number, no `$` or commas |
| `City State Zip ` | `"CITY, ST ZIP"` (trailing space in the header name itself) |

### contributions_source_{category}.csv (×12)

Same 8 columns as `contributions_all.csv`, but each file is the export for one value of the "All" tab's `ddlSourceOfContribution` filter — `CANDIDATE COMMITTEE`, `CORPORATION`, `FEDERAL/OUT-OF-STATE PAC`, `IMMEDIATE FAMILY / PERSONAL`, `INDIVIDUAL`, `NATIONAL PARTY`, `ORGANIZATION`, `OUT OF STATE PARTY`, `WYOMING COUNTY PAC`, `WYOMING COUNTY PARTY`, `WYOMING PAC`, `WYOMING STATE PARTY`. Not itself a visible column anywhere — it's purely a search-form filter, discovered while investigating why `contributor_type` had no real source. These 12 files together are a **supplementary enrichment source**, not a replacement for `contributions_all.csv`: they summed to 439,633 rows against 442,795 in the plain export at last run (~99% coverage). The gap is exactly the `ANONYMOUS`/`UN-ITEMIZED` rows, which have no contributor name and so were never assigned a category by the site itself. See Parser section below for how the two are joined.

### expenditures_all.csv

Same export mechanism from `SearchExpenditures.aspx` (~80K rows, ~11 MB):

| Field | Description |
|---|---|
| `Filer Type` | `CANDIDATE COMMITTEE`, `CANDIDATE`, `POLITICAL ACTION COMMITTEE`, `PARTY COMMITTEE`, or `ORGANIZATION` (3 rows) |
| `Filer Name` | The spending committee/candidate |
| `Payee` | Recipient of the expenditure — occasionally a bucket label like `"NON-WY TRANSACTIONS"` rather than a real vendor name (see Data Notes) |
| `Purpose` | Free text, ~2,700 distinct values — a controlled dropdown category (`ADVERTISING - MISC.`, `TRAVEL (HOTEL, GAS, ETC.)`, `LOAN PAYMENT`, etc.) plus an `OTHER: <detail>` variant |
| `Date` | `M/D/YYYY` |
| `City State Zip` | `"CITY, ST ZIP"` |
| `Filing Status` | Same four values as contributions |
| `Amount` | Plain number |

### candidate_committee_roster.pdf

147-page ActiveReports PDF from `Reports/ResearchToolsAndLists.aspx` → "Candidate Campaign Committees" → Run Report (ContestType=All, status=Both active+terminated). Organized by office section (`GOVERNOR`, `HOUSE DISTRICT 03`, ...), each containing one block per candidate-committee registration: party, candidate name, date formed/terminated, committee name, address, chairman, treasurer, phone, email, website. 369 registrations. No numeric filer ID anywhere.

### pac_roster.pdf

37-page version of the same report for PACs (no office/party columns — PACs aren't tied to a race). 206 registrations.

---

## Scraper

`src/pipeline/scrapers/wyoming.py`

The source is a classic ASP.NET WebForms site — no API, everything is `__doPostBack` form submissions against `__VIEWSTATE`/`__EVENTVALIDATION` hidden tokens. The scraper's `_form_state()` helper snapshots every named form control's current value from the response HTML (rather than hand-listing field names), so it survives the two search pages having different field names on their "All" tab.

**Transactions:** GET the search page → `__doPostBack` the `mnuContributions` menu to tab index `"4"` (All) → submit a blank Search with Status forced to `"Both Official and Published"` → submit Export. The Export response is the full CSV — confirmed against the page's own `"Showing 1-N of N Records"` counter that it ignores pagination and returns everything, not just the visible page.

**Entities:** GET the research tools page → click the roster link (reveals a filter panel) → submit "Run Report" with Both (active + terminated) selected. The response doesn't contain the report — it embeds `window.open('ShowReports.aspx', ...)`, meaning the server rendered the report into session state. GETting `Reports/ShowReports.aspx` in the same session immediately after returns the actual PDF.

**No year-based filtering exists on this source** — `contributions_all.csv`/`expenditures_all.csv` are unpartitioned all-time snapshots, and a HEAD request confirms there's no `Last-Modified`/`ETag` to detect staleness. Normal runs therefore always re-download both transaction files in full rather than trying to skip; `--force` is accepted for CLI-contract compatibility but is a no-op.

**Source of Contribution enrichment:** whenever a contributions run happens, the scraper also downloads the 12 `contributions_source_{category}.csv` files described above (`download_contribution_sources()`), reading the category list live from the `<select>` rather than hardcoding it (same philosophy as `_form_state()`). One category (`value="-1"`, the `-- Select One --` placeholder) is explicitly excluded — submitting it is equivalent to no filter and silently returns the full unfiltered export a second time, which was caught during testing (the resulting file was byte-identical to `contributions_all.csv`). Each category gets its own fresh GET → tab-switch → filtered Search → Export, matching `download_transaction()`'s flow rather than reusing one page load's VIEWSTATE across 12 searches. `INDIVIDUAL` alone is ~421K rows — nearly the size of the whole plain export — so this roughly doubles total scrape time for a contributions run.

**Expected runtime:** ~2–3 min for `--expenditures` or entities alone. A `--contributions` run (or a full run) takes ~5–8 min once the 12 supplementary source files are included — contributions export alone takes ~100–150s (73 MB CSV streamed through a single POST response), the `INDIVIDUAL` source file another ~50s, the remaining 11 source files a few seconds each, expenditures ~15s, both roster PDFs a few seconds each.

---

## Parser

`src/pipeline/parsers/wyoming.py`

**Roster PDF parsing:** pdfplumber's flattened text layer interleaves the reports' columns ambiguously — e.g. a PAC's committee name and its street address land on the same visual line with no delimiter between them once flattened to text. The parser instead works from `extract_words()` x-coordinates: words are grouped into visual rows by y-position (small vertical jitter within a row, ~3pt, is tolerated via a gap-based clustering threshold), then bucketed into named columns by hardcoded x0 thresholds derived from each report's header row. A small state machine (`_consume_tail`) then walks the resulting rows, using the `Chairman:`/`Treasurer:`/`Email:`/`Website:` prefixes as field markers and treating unmarked lines between the name row and `Chairman:` as address lines. 369/373 candidate-roster rows and 204/206 PAC-roster rows parsed cleanly; the handful of misses are long tracking URLs in a `Website:` field that wrapped onto extra PDF lines (harmless — the URL is just truncated to its first line).

**Entity registry / enrichment:** both rosters are parsed into a `committee_name → {candidate_name, office, district}` lookup (plus a separate `candidate_name` lookup for the `CANDIDATE` recipient/filer type, where the transaction export's Recipient/Filer Name *is* the candidate's own name with no separate committee entity). Contributions/expenditures are enriched with `candidate_name`/`office` through this registry where a match exists — coverage is ~8% of contribution rows and ~33% of expenditure rows, since roughly half of `CANDIDATE COMMITTEE`-type recipients don't have a matching *current* roster entry (some historical committees have likely aged out of the "Both active+terminated" export, or registered under a slightly different name).

**Committee/candidate coverage beyond the rosters:** WY publishes no dedicated roster for Party Committees or Organizations. The parser tracks every committee name it sees in the transaction files that isn't already covered by a roster, and writes those to `committees.csv` too (committee_type from the transaction's own Recipient/Filer Type column, no address/treasurer since none is available). Standalone `CANDIDATE`-type filers with no committee registration and no roster match get a bare `candidates.csv` row (name only) so their transactions still resolve to a person — but they do **not** get a `committees.csv` row, since a raw individual filer isn't a committee.

**`election_year`:** neither transaction export nor either roster has an explicit election-year field. Since ~60% of `candidates.csv` rows are the standalone `CANDIDATE`-type filers above — pulled straight from transaction volume that's heavily weighted toward the last 2–3 years — leaving `election_year` blank made the *whole* candidates/committees output misleadingly look like it was all from the current cycle, even though the underlying roster data actually spans 2001–2026 for candidates (1990–2026 for PACs). Two proxies fix this: for roster-matched entities, `election_year` = the year the committee was *formed* (WY committees are cycle-specific — a new one is normally registered per candidacy); for everything else, `election_year` = the earliest year that name appears anywhere in the transaction data. Neither is authoritative (a committee formed in December could be for the following year's cycle), but both are real signal rather than a fabricated guess. Only 35/753 committees end up with no year at all (no roster match and, for whatever reason, no transaction hit either).

**AMEND - DELETE handling:** neither export carries a transaction ID, so a `Filing Status = AMEND - DELETE` row (a later filing retracting an earlier one) can't be reliably linked back to the specific row it retracts — several rows can share an identical `(date, amount, committee, contributor)` tuple. Rather than risk subtracting the wrong row, `AMEND - DELETE` rows are dropped entirely (1,501 / 442,705+1,501 contribution rows, 637 / 80,409+637 expenditure rows — both <0.4%). This can slightly overstate totals where a deletion wasn't offset by a corresponding `AMEND - ADD` elsewhere, which is a smaller error than an incorrect subtraction would be.

**Loans:** `Contribution Type = LOAN` rows are routed to `loans_debts.csv.gz` instead of `contributions.csv.gz` (749 rows; same pattern as Georgia's Loan Received/Payment handling). Expenditure `Purpose = LOAN PAYMENT` rows stay in `expenditures.csv.gz` — Purpose is free text with 2,700+ distinct values and no separate top-level type field to split loan payments out of cleanly.

**contributor_type:** `contributions_all.csv` itself has no contributor-type column, but the parser cross-references the 12 `contributions_source_{category}.csv` files. Neither export carries a transaction ID, so rows are matched by the full raw row tuple — contributor name, recipient name, contribution type, date, filing status, amount, and city/state/zip (`_contribution_row_key()`) — via `build_contributor_type_lookup()`. At last run this matched 438,213 of 442,795 contribution rows (98.96%). Unmatched rows (mostly `ANONYMOUS`/`UN-ITEMIZED`, which the site itself never assigns a category — plus a handful of stray misses) fall back to `guess_contributor_type()`, a name-string heuristic (comma-separated → Individual; org-indicative token like "PAC"/"COMMITTEE"/"LLC" → Organization; otherwise blank). `src/aliases/contributor_types.csv` maps all 12 raw category labels to canonical values (`Individual`, `Organization`, `PAC`, `Candidate Committee`, `Party`) plus the two heuristic fallback labels.

**expenditures `transaction_type`:** written uniformly as `"EXPENDITURE"` for every row (no distinct type field beyond free-text Purpose) — mapped to `Monetary` in `expenditure_categories.csv`.

**person_id model:** `name_hash` — no numeric ID exists anywhere in either roster PDF or either transaction export.

**Expected runtime:** ~10s.

---

## Data Notes

- **`NON-WY TRANSACTIONS` / `NON WY TRANSACTIONS` / `NON-WYOMING EXPENDITURES` as top expenditure payees** — these are real values in the raw `Payee` column, not a parsing artifact. Some filers (mostly out-of-state corporate/federal PACs with a small Wyoming presence) report their non-Wyoming spending as a single aggregated line item rather than itemizing each vendor. They show up prominently in a "top payees" ranking ($3–5M each) precisely because they're aggregates, not because any single vendor was actually paid that much.
- **UN-ITEMIZED contributions have no contributor name** — by design in the source; these are aggregate totals for small below-threshold contributions, same convention as most other states.
- **Roster coverage is partial, not stale** — the "Both active+terminated" filter was used on both roster reports, so the gap isn't a status filter issue. Some committees referenced in transactions (especially older ones) don't have a current roster entry under an exactly-matching normalized name; those still get a `committees.csv` row (via the transaction-derived fallback described above), just without address/treasurer detail or `candidate_name` enrichment on their transactions.
- **`state_filer_id` is empty for every candidates/committees row** — confirmed no numeric ID exists anywhere in the source (roster PDFs or transaction exports). `has_filer_id=0` is set for WY in `src/aliases/states.csv` so the validator doesn't flag this as a failure.
- **Party affiliation and office/district are only available for candidates that appear in the candidate committee roster** (369 of 929 candidates.csv rows) — standalone `CANDIDATE`-type filers with no roster match have blank office/district/party.
- **`contributor_type` enrichment costs ~5–8 min of extra scrape time** for ~1.2 percentage points of coverage (the `ANONYMOUS`/`UN-ITEMIZED` rows it can't reach either way). Worth it because it replaces a name-string guess with the site's own classification for 99% of rows — e.g. it's the only way to distinguish `WYOMING PAC` from `FEDERAL/OUT-OF-STATE PAC` from `IMMEDIATE FAMILY / PERSONAL`, none of which the old heuristic could tell apart (all three would've just fallen through to blank or a generic "Organization"/"Individual" guess).

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-08 |
| Parser | 2026-08-08 |
