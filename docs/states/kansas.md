# Kansas — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Kansas (KS) |
| **Source** | [Kansas Secretary of State — CFR Examiner](https://sos.ks.gov/elections/cfr_viewer/cfr_examiner.aspx) |
| **Access method** | Selenium-driven ASP.NET form search + HTML table scrape (no API, no bulk export) |
| **Filer coverage** | Candidate committees, PACs, and party political committees |
| **Coverage** | **e-filed reports only**, 2000 – present (2014 statewide cycle onward). Filings that exist only as a scanned PDF cannot be scraped — see [Paper-only filings](#paper-only-filings-are-skipped-from-the-grid) |
| **person_id model** | `name_hash` — no numeric filer IDs in source; `person_id` derived from MD5 of normalized name |

Kansas was rewritten in 2026-08 to read the CFR Examiner's HTML reports directly instead of downloading and OCR-ing ~3,600 PDFs. The previous PDF implementation is kept, unwired, as `src/pipeline/scrapers/kansas_pdf_legacy.py` and `src/pipeline/parsers/kansas_pdf_legacy.py`; see [Legacy PDF pipeline](#legacy-pdf-pipeline) for why it was replaced.

---

## Raw Data Structure

The scraper writes five CSVs into `data/Kansas/raw/`, accumulated across every search in one run (candidate filings and PAC/party filings share the same files — `candidate_uid`'s `office_group` distinguishes them):

| File | Content |
|---|---|
| `candidates_summary.csv` | One row per candidate filing — filer name/address/office/district, period dates, and the seven report totals |
| `schedule_a_contributions.csv` | Itemized contributions and other receipts |
| `schedule_b_inkind.csv` | Itemized in-kind (non-monetary) contributions |
| `schedule_c_expenditures.csv` | Itemized expenditures and other disbursements |
| `schedule_d_other.csv` | Other transactions (loans, start-up costs) — scraped but **not** loaded by the parser |

Every row in all five files carries a **`candidate_uid`**:

```
office_group|cycle_label|office_sought|district_number|name|original_date|amendment_date
```

That's the join key the parser uses — not the `candidate` name-text column, which collides across cycles and offices for people who share a name.

`data/Kansas/manifest.csv` tracks which candidate filings have been scraped, with one row per `candidate_key` (the same string as `candidate_uid`) plus `scraped_at`. A manifest left over from the legacy PDF scraper has an incompatible schema; the scraper detects that on first run, moves it to `manifest_pdf_legacy.csv`, and starts fresh rather than appending to it.

---

## Scraper

`src/pipeline/scrapers/kansas.py`

Drives the CFR Examiner form with **Selenium + Chrome**, in a **visible (headed) window** — see [the headless block](#headless-is-blocked) below. Kansas is the only browser-driven scraper in the repo on Selenium — the others (Oregon, Florida, Alaska, Mississippi) use Playwright. `selenium` is pinned in `requirements.txt`; Selenium 4's built-in Selenium Manager resolves a matching chromedriver at launch, so a local Google Chrome install is the only extra prerequisite (no `playwright install` needed for KS, and no manual chromedriver download).

1. Load the examiner entry page, select the search **category**, Submit.
2. For every entry in `RUN_CATALOG`: fill the **Date Range Filed** start/end, set **Filing Type** = "Receipts and Expenditures Report", set **Office** / **Type of Committee**, Submit Search.
3. Walk every page of the results grid. For each row, build its `candidate_key` from grid fields *before* opening it, so already-scraped candidates are skipped without a page load.
4. For each candidate: open the summary report, then open only the schedules whose summary total is non-zero, parse their tables, and append rows to the shared CSVs.

`RUN_CATALOG` covers two of the viewer's categories, 78 searches in total:

| Category | Searches | Windows |
|---|---|---|
| **Candidate Campaign Filings** | 56 | House (2016–2026, 2-year cycles), Senate (incl. the 2018 and 2022 specials), Statewide (Governor, AG, SOS, Treasurer, Insurance Commissioner), District Attorney, State Board of Education, and the four judicial offices (Supreme Court, Court of Appeals, District Court, District Magistrate) |
| **PAC/Party Political Committee** | 22 | Political Action Committee and Party Political Committee, one calendar year each from `PAC_START_YEAR` (2016) to now |

Every office the dropdown offers is covered. Within each office the date windows are **contiguous** — that's what makes coverage complete, since the form filters on *filing* date, so any gap between two windows would silently drop every filing made during it. A test asserts there are no gaps.

The two categories share one code path. Both forms use the same control ids (`txtStartDate`, `txtEndDate`, `drpdownFilingType`, `drpdownOffice`, `btnSearch`), the same `grdviewCfrResults` grid, and the same report and schedule pages — `drpdownOffice` is simply labelled "Office" on one and "Type of Committee" on the other. The PAC grid has no office/district columns, so those read as blank, which is exactly right for a committee filing. PACs file on a rolling basis rather than per election cycle, hence the calendar-year sweep; `cycle_label` carries the year and `office_group` carries `PAC` or `Party`, so the parser can tell filer types apart straight from `candidate_uid`.

**Robustness choices:**

- **Frames.** The examiner renders its form inside a frame — a first live run found zero `<select>` and zero `<input>` elements in the top document. Selenium only sees the focused document, so every step (`category dropdown → date fields → results grid → summary report → each schedule → pager`) calls `focus_frame_with()`, which searches the top document and then each frame (two levels deep) for the control it needs. Frame focus resets on every navigation, so this runs at each step rather than once.
- Dropdown options are matched by *visible text substring*, not `value`, and a failed match raises with the live option list attached.
- `#trOffice` is handled whether it is the `<select>` itself or a wrapper row containing one.
- **Grouped pagers.** GridView shows only a window of page numbers (10 at a time here) with `...` links between groups, and two live failures came from it. Clicking the forward `...` doesn't merely reveal more numbers — it *navigates* to that group's first page, so looking for a page-11 link afterwards raised "No pager link found for page 11" when page 11 had already become the current page (a plain `<span>`). And a middle group renders `...` on **both** sides with identical text, so taking the first one walked backwards from page 20 to the first group and stopped the cycle early. `advance_to_next_page()` now re-reads the pager after every jump, and the forward `...` is identified as the one positioned after the current-page span.
- **The grid keeps its page index between searches.** A new search re-renders the grid at whatever page the previous cycle stopped on — past the end of a smaller result set, so the scrape saw a few rows and quit. Every cycle after the first returned ~10 candidates instead of hundreds, silently. `reset_pager_to_first_page()` runs after each search; because the pager only shows one group, getting back from page 85 means walking *down* through groups via the backward `...` until page 1 is on screen, not clicking a "1" that isn't rendered.
- A pagination failure ends that cycle's page walk with a warning instead of raising, so the candidates already scraped are kept and the run continues with the next cycle.
- **Paper-only filings are skipped from the grid.** Not every filing has an HTML report. E-filed ones put the candidate's name *inside* the results-grid link; filings that exist only as a scanned PDF leave that anchor empty and render the name as plain text beside it:

  ```html
  <a id="..._lnkbtnName_0">PRINGLE JAN</a>       <!-- has a report  -->
  <a id="..._lnkbtnName_4"></a>JENKINS ERIC      <!-- paper only     -->
  ```

  An empty anchor has no size, so WebDriver calls it non-interactable and a JS click fires no postback — the source of both the `ElementNotInteractableException` failures and the 495 blank candidates in earlier runs. These rows are now recognised before any click, counted separately as `paper-only`, and never written or manifested. Skipping them up front also removes a ~20s dead wait each, which was the bulk of a multi-hour run. (In one sampled results page, 16 of 20 rows were paper-only.)
- **Clicks escalate.** Every link here is an ASP.NET postback anchor, and WebDriver refuses to click elements it judges non-interactable — a live run lost ~40% of rows to `ElementNotInteractableException` on rows far down long pages. `_safe_click()` tries a plain click, then scrolls the element to centre and retries, then falls back to a JS `.click()`, which fires the same postback without WebDriver's visibility gate.
- **Failed rows recover.** A row that dies mid-way leaves the driver on a summary or schedule page. Previously the handler only waited for the grid to reappear — which it never would — so every later row on that page failed too and pagination stopped, turning one bad row into a lost cycle (the contiguous failure blocks in that same run). `_recover_to_grid()` closes stray windows and steps back through history until the grid returns, and gives up the cycle explicitly if it can't.
- **Per-candidate writes are all-or-nothing.** Rows are buffered and flushed only after a candidate's summary *and* every non-empty schedule have parsed. Writing as we went left half-written candidates on disk when a schedule failed; since the manifest only records successes, the next incremental run would re-scrape them and append their rows a second time, double-counting whatever had already landed.
- **Popup detection is short.** Each click waits briefly to see whether a new window opened before assuming in-place navigation. At 3s, on ~5 clicks per candidate, this dominated a multi-hour run; it's now 1s polled every 100ms, since a popup that opens does so immediately.
- The pager row is located by digit-only, **id-less** span/anchor cells. Every data cell in the grid carries a generated id (`grdviewCfrResults_lblZip_0`, …), so this avoids mistaking a data row for the pager — a real hazard, since a row with a 5-digit ZIP and a district number has two digit-only cells and would otherwise tie with a two-page pager and silently cap the scrape at page 1.
- **Future dates are rejected by the form.** "Date Range Filed" refuses an end date after today ("Cannot be future date") and the search never runs. Every still-open cycle in `RUN_CATALOG` ends 12/31 of its election year, so House 2026, Senate 2028 and DA 2028 were quietly returning nothing and being logged as empty cycles. End dates are now clamped to today, cycles whose window hasn't opened yet are skipped outright, and a form validation message is reported as a rejection rather than as "no results".
- A search that returns no grid is treated as an **empty cycle** (future elections have no filings yet), not a failure. If *every* search in a run fails, the scraper raises so the pipeline aborts the state rather than parsing stale data.
- Postbacks are awaited by waiting for the previous `<body>` to go stale, then for `document.readyState == "complete"`. Candidate/schedule links are handled whether they navigate in place or open a new window (window handles are tracked and closed).
- `driver.quit()` runs in a `finally` block, and stray windows left by a failed row are closed before the next row — a lingering browser would hang the whole pipeline run.

### Headless is blocked

`sos.ks.gov` sits behind CloudFront, which serves headless Chrome a block page instead of the site:

```
title      : 'ERROR: The request could not be satisfied'
body text  : "403 ERROR ... Request blocked. ... Generated by cloudfront (CloudFront)"
```

A headed window on the same machine and IP loads the form fine, so the scraper runs headed by default (`--headless` exists but expect 403s), sends `config.USER_AGENT`, and disables the `AutomationControlled` blink feature. A block page is now detected explicitly and raises "sos.ks.gov returned a CloudFront block page…" rather than surfacing as a confusing "selector not found on a page with no controls".

Practical consequence: **Kansas can't run unattended on a headless box.** A scheduled/daemon run needs a display (a real desktop session, or Xvfb on Linux).

### Rate limiting

The site's edge also blocks on **volume**, not just on headless Chrome. A full-scrape attempt was cut off ~54 candidates in: every page after that came back as CloudFront's `403 ERROR / Request blocked`, served *at the requested URL*, so it first showed up as "the candidate link did not open a report — still on exp_report_main.aspx".

Handled three ways:

- **A pause between candidates** (`--delay`, default 0.5s). A full scrape is tens of thousands of page loads; this is the difference between finishing and being cut off.
- **Backoff and resume.** A block raises `Blocked` rather than counting the filing as failed. `wait_out_block()` sleeps 60s, 180s, 420s, then 900s, reloading between each; once the site answers again `_resume_at_page()` re-submits the cycle's search and pages back to where it stopped (navigating away to wait loses the grid's server-side state). Nothing already scraped is re-fetched — the manifest sees to that.
- **Loud failure if it persists.** If the site is still blocking after ~30 minutes of backoff, the run raises rather than marching through the remaining cycles recording every filing as a failure. Re-run later; the manifest resumes where it stopped.

If a run keeps getting blocked, raise `--delay` (e.g. `--delay 2`) and run it again.

### Selector status

Confirmed on a live headed run (2026-08-10), entry page, **no frames present**:

| Selector | Status |
|---|---|
| `#ddlViewerOptions` | ✅ category `<select>`, top document |
| option `Candidate Campaign Filings` | ✅ `value="Candidate"` |
| `#btnSubmit` | ✅ Submit (note `#btnExit` is "Back") |
| entry URL | ✅ redirects to `cfr_examiner_entry.aspx` |

Still inferred, all on the search form that follows: `txtStartDate`, `txtEndDate`, `drpdownFilingType`, `trOffice`, the Submit Search button id (`#btnSearch` vs. reusing `#btnSubmit`), the per-office option text (only "State Representative" was ever confirmed), and the per-cycle date spans. If a cycle logs `no option containing …`, dump the live controls:

```bash
python3 src/pipeline/scrapers/kansas.py --debug-controls --start-year 2024 --end-year 2024
```

That dumps all three stages — entry page, search form, results page — and stops after the first cycle. Each dump prints the URL, page title, `readyState`, a snippet of visible body text, every frame with its `src`, and every `<select>`/`<input>`/`<button>` (with option text and values) in the top document *and* inside each frame. A dump showing no controls anywhere means the page that loaded isn't the one expected — a redirect, an error page, or a block — rather than drifted ids.

**Flags:** the standard vertical scope flags (`--force`, `--start-year`, `--end-year`) apply to *cycle* years. Horizontal flags (`--transactions`, `--entities`, …) are accepted and ignored — a Kansas R&E filing carries contributions, expenditures and filer info together.

**Expected runtime:** roughly one page load per candidate plus one per non-empty schedule, so a full historical scrape is long (hours). Incremental runs only re-check cycles whose year is ≥ the current year.

---

## Parser

`src/pipeline/parsers/kansas.py`

Reads the four CSVs it needs from `data/Kansas/raw/` (falling back to `data/Kansas/` for output left by the pre-integration `kansas_v2` scraper), joins schedule rows to their filing on `candidate_uid`, and writes the canonical cleaned CSVs.

| Output | Source |
|---|---|
| `contributions.csv.gz` | Schedule A rows (minus loans), plus Schedule B rows with `transaction_type = "In-Kind"` |
| `expenditures.csv.gz` | Schedule C rows |
| `loans_debts.csv.gz` | Schedule A rows whose `type_of_payment` is `Loan` |
| `candidates.csv.gz` | `candidates_summary.csv`, deduped to one row per (name, office, district, election_year) |
| `committees.csv.gz` | Every filer — candidate committees plus PACs and party committees — typed from the search category that returned it |

### Counting each transaction once

The same money reaches the raw CSVs three different ways. All three are collapsed in the parser, and together they cut the contribution total from **$185M to $109.5M**:

1. **Re-scrapes.** The scraper appends, and deliberately re-scrapes cycles whose year is ≥ the current year to pick up amendments — so a filing scraped *k* times contributes *k* copies of every row (6% of contribution rows, $21M).
2. **Amendments.** An amendment is a **separate row** in the results grid, not a flag on the original, and it restates the entire period. 1,707 of 5,165 periods had been filed more than once — one as many as 12 times.
3. **Overlapping search windows.** The grid returns a filing under every search whose date window contains its *filing* date, so a 2018 report amended in 2020 comes back from both the 2018 and the 2020 search.

`resolve_amendments()` keeps the newest version of each (candidate, `period_start`, `period_end`), which handles 2 and 3 together; `_dedup_rescrapes()` handles 1.

**Cross-check:** every filing publishes its own summary totals, so the parsed itemized rows can be checked against what the filers themselves declared:

| | Filings declare | Parsed | Δ |
|---|---|---|---|
| Schedule A (contributions + loans) | $111,125,285 | $109,492,275 | −1.47% |
| Schedule B (in-kind) | $3,881,035 | $3,828,996 | −1.34% |
| Schedule C (expenditures) | $105,733,096 | $105,441,134 | −0.28% |

A small shortfall is expected and correct: unitemized contributions are summarised in the totals but never listed as rows. It runs larger for PACs and party committees (**-3.7%**) than for candidates (**-1.5%**), which is what you'd expect from more small-dollar aggregated giving. Before the dedup work the same comparison ran **67% over**.

### Field notes

- **Loans are not contributions.** Schedule A tags some receipts `type_of_payment = "Loan"` — 14% of all receipts by value. They go to `loans_debts.csv.gz` with `record_type = "Loan"` and the contributor as counterparty, matching what Wyoming and Georgia do. Left in `contributions` they inflated every "total raised" figure.
- **Expenditure purpose is split.** Schedule C's Purpose is a fixed form label followed by free text with nothing marking the boundary ("Printing printing/mailing"). The label set is closed — 29 values cover 100% of rows — so the label lands in `category` and the remainder in `purpose`. `transaction_type` is `Expenditure`, except the two labels that describe a different kind of transaction (`Refund` → `Refund`, `Donation/Contrib` → `Contribution`).
- **Dates** are accepted as `M/D/YY` or `MM/DD/YYYY` and bounded to 1990 … current year + 4. The floor used to be 2013, which silently deleted 24,821 real transactions ($12.3M): a 2014-cycle filing legitimately reports 2010–2012 activity.
- `raw_file` is the input CSV and `row_num` its line number, so a cleaned row points back at an exact source line — the traceability pair `columns.py` describes. (It previously held one constant URL for all 1.1M rows.)
- `jurisdiction` carries the filing's `county` (91% filled), which is what scopes a District Attorney race.
- `election_year` is the **earliest** cycle a period was searched under, not the cycle that returned it — otherwise a 2018 report amended in 2020 would be labelled 2020.
- State codes are filtered to real postal codes (the source writes `NA` for blanks) and ZIPs are zero-padded before validation, which recovers 23,027 New England ZIPs that had lost a leading zero.
- `person_id` = `name_hash` (FIPS 20 prefix), assigned by `utils.assign_person_ids`.
- Deliberately blank: `state_filer_id` and `filing_id` (absent from the source), `employer`, `contributor_type`, `incumbent`, `active`, and `treasurer_name` — the summary's `signature_name` may be either the candidate or the treasurer with no way to tell, so mapping it would be worse than leaving it empty.

### Alias registrations

Kansas values are canonicalized at aggregate time through `src/aliases/`; without these rows every KS transaction lands with a NULL category in the master DB:

| File | Rows added |
|---|---|
| `transaction_categories.csv` | `Check`/`Credit Card`/`E Funds`/`Cash` → Monetary, `In-Kind`, `Refund`, `Other`, `Loan` |
| `expenditure_categories.csv` | `Expenditure` → Monetary, `Refund`, `Contribution` |
| `committee_types.csv` | `Candidate` → Candidate Committee, `PAC` → PAC, `Party Committee` → Party Committee |
| `office_types.csv` | `District Attorney` → County Prosecutor |

---

## Party enrichment

The CFR Examiner has no party field on any page — not the results grid, not the summary report, not the schedules. Party instead comes from the SOS's own [Candidate List](https://sos.ks.gov/elections/elections_upcoming_candidate.aspx), which publishes one row per candidate per election with a Party column, selectable by election from **2002 Primary through 2026 General** (28 elections in the dropdown).

**Scrape.** `scrape_candidate_roster()` walks the `#ddlElections` dropdown, submits, and parses `#gvCandidateList` into `data/Kansas/raw/candidate_roster.csv`. Columns are located by *header text* rather than position — the table carries 25 columns and we keep 17 — so a reordering upstream can't silently shift party into another field. It's one page load per election, so the file is rewritten wholesale each run rather than tracked in the manifest. Skip with `--no-roster`; fetch it alone with `--entities` (see the scope flags in the scraper).

**Join.** Neither source has a filer ID, so `PartyIndex` in the parser does a graded name match and records how it got there:

| `match_confidence` | Matched on |
|---|---|
| `exact` | last + first + office bucket + district + election year |
| `high` | last + first + election year (office/district disagree or are missing) |
| *(unmatched)* | left blank — never guessed |

`party_source` is `ks_sos_candidate_list` on every hit. Supporting details:

- Names are compared as normalized tokens and handle both `"Last, First"` and `"First Last"`, since the Examiner's own format isn't uniform. Nicknames expand through `src/aliases/nicknames.csv`, so a roster "Mike" matches a filing "Michael".
- Offices are bucketed (`HOUSE`, `SENATE`, `GOVERNOR`, `AG`, `SOS`, `TREASURER`, `INSURANCE`, `DA`, `BOE`) so "Kansas House of Representatives" and "State Representative" line up. Federal offices get their own bucket and can never collapse into a state chamber.
- A key that maps to two different parties — same-named candidates, or a party switch between primary and general — is **dropped** rather than resolved arbitrarily. A blank party beats a confidently wrong one.
- Party labels are canonicalized via `src/aliases/parties.csv` ("Democratic" → `DEMOCRAT`).

**Expected coverage gaps:** judicial races are nonpartisan in Kansas and carry a blank Party in the roster (57 of 323 rows in the 2002 General list), so those candidates stay blank; candidates who filed a finance report but never appeared on a ballot won't be in the roster at all.

---

## Data Notes

- **Party comes from a second source** — the CFR Examiner publishes none. See [Party enrichment](#party-enrichment).
- ⚠️ **Coverage is e-filed reports only.** Filings submitted on paper appear in the results grid but have no machine-readable report behind them — the CFR Examiner offers them as a scanned PDF and nothing else. They are skipped, so **any candidate, PAC or party committee that filed exclusively on paper is absent from the cleaned data entirely**, and one that filed on paper for some periods and electronically for others is present but incomplete. The `paper-only skipped` count in each run's log is the size of that gap; in one sampled results page 16 of 20 rows were paper-only. Closing it would mean OCR-ing the PDFs, which is what the retired legacy pipeline did — and its accuracy problems are documented below.
- **Districts arrive as `/ 38`** — the grid prefixes them with a slash; the parser reduces them to a bare number, which also matters for the party join.
- **No filer IDs** — the CFR Examiner exposes no numeric filer identifier, hence `name_hash`. `state_filer_id` is blank; validation downgrades this to a tier-2 warning for Kansas via `has_filer_id = 0` in `src/aliases/states.csv`.
- **No `filing_id`** — no stable per-filing identifier was found in the scraped pages.
- **Judicial races split two ways.** District Court and District Magistrate judges are elected on a partisan ballot in the districts that elect rather than appoint, and the SOS roster gives them a party (67% and 74% of roster rows respectively) — they match and carry a real party. Appellate seats (Supreme Court, Court of Appeals) are retention elections: the roster lists them with a blank Party, so those candidates' `party` stays empty by design.
- **Three filer types** — candidate committees, PACs and party committees are all scraped. A PAC's transactions carry its own name in `committee_name` and a blank `candidate_name` (it is legally separate from any candidate), and PACs never enter `candidates.csv.gz`. `assign_committee_person_ids` leaves their `person_id` NULL by design; which candidate an independent PAC supports is enrichment, handled by `enrich.py` from a hand-reviewed registry.
- **Schedule C purpose is split** into `category` (the form's label) + `purpose` (the filer's free text) — see the parser's field notes.
- **Schedule D is not loaded** — its only amount column is `balance_at_close`, a period-end balance rather than an original amount, and 1,526 of 2,426 (candidate, counterparty, account) triples recur across periods, so summing it would multiply one loan by its number of reporting periods. `LOANS_DEBTS` has no balance column to hold it honestly, so those rows stay in the raw CSV only. Real loan amounts come from Schedule A instead.
- **No contributor type / employer** — absent from the source schedules. `occupation` is populated where the schedule provides it (58%); Schedule B's in-kind *description* is deliberately NOT substituted into it, as that field is transaction text, not an occupation.

---

## Legacy PDF pipeline

`*_pdf_legacy.py` — kept unwired for reference and fallback. It downloaded ~3,600 R&E PDFs from the static `kansas.gov/ethics/CFAScanned/` index pages and reconstructed rows from `pdfplumber` word coordinates. Its known output problems are what motivated the rewrite:

- Scanned-form OCR errors baked into names ("STCPHCN B OWENS" for "STEPHEN B OWENS") and blank address fields.
- `occupation` only extractable from web-form PDFs (~30% fill), `payee_city/state/zip` ~77%.
- Amendments filed as separate PDFs required a dedup pass that could not catch cross-file duplicates (e.g. a $480K Barnett contribution counted twice).
- Date text bleeding into contributor names, and amount thresholds (`A_AMT_MIN = 350`) needed to avoid reading page numbers as dollar figures.

Note that `pdfplumber` is not in `requirements.txt`; the legacy parser will not import without it.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-11 (CFR Examiner rewrite on Selenium + Chrome; paper-only/pager/page-index fixes; PAC + party committee categories) |
| Parser | 2026-08-11 (amendment/re-scrape dedup, loans split out, purpose→category, 1990 date floor, real raw_file/row_num, alias registrations) |
| Documentation | 2026-08-11 |
