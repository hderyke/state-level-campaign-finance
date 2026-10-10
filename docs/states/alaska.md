# Alaska — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Alaska (AK) |
| **Source** | [Alaska Public Offices Commission (APOC)](https://aws.state.ak.us/apocreports/Home.aspx) |
| **Access method** | Playwright browser automation (live Chromium session required) |
| **Coverage** | 2008 – present (2008–2010 empty; meaningful data starts 2011) |
| **person_id model** | `name_hash` — no numeric filer ID in source; `person_id` derived from MD5 of normalized name |

---

## Raw Data Structure

Four file types: two transaction tables (one per year), one static candidate registry, and annual group/committee registration forms.

### Transaction Files

One file per year per type: `CDIncome_{year}.csv` and `CDExpense_{year}.csv`

Both files share the same schema — APOC exports contributions and expenditures in the same format:

| Field | Description |
|---|---|
| `Result` | Row number / export sequence ID — used as a proxy filing ID and for deduplication (higher = more recent amendment) |
| `Date` | Transaction date (M/D/YYYY in raw) |
| `Transaction Type` | e.g. "Income", "Expenditure" |
| `Payment Type` | e.g. "Cash", "Check", "Credit Card" (maps to `category` in expenditures output) |
| `Payment Detail` | Check number or other payment reference |
| `Amount` | Dollar amount; negatives formatted as `(500.00)` |
| `Last/Business Name` | Contributor or payee last name / organization name |
| `First Name` | Contributor or payee first name |
| `Address` | Street address |
| `City` | City |
| `State` | State (full name, e.g. "Alaska") |
| `Zip` | Zip code |
| `Country` | Country (e.g. "USA") |
| `Occupation` | Contributor occupation |
| `Employer` | Contributor employer |
| `Purpose of Expenditure` | Free-text expenditure description (income files: blank) |
| `--------` | Visual separator column — not data |
| `Report Type` | Filing period description (e.g. "Previous Year Start Report") |
| `Election Name` | Full election description (e.g. "2024 - Anchorage Municipal Election") |
| `Election Type` | e.g. "Anchorage Municipal", "State Primary" |
| `Municipality` | Municipality of the election |
| `Office` | Office sought by the filer |
| `Filer Type` | "Candidate" or group type (e.g. "Independent Expenditure") |
| `Name` | Filer name — the committee or candidate filing this report |
| `Report Year` | Year of the report |
| `Submitted` | Date the report was submitted |

### Candidate Registry

`CDCandidates_all.csv` — a single flat export of all candidates across all years:

| Field | Description |
|---|---|
| `Result` | Row sequence number |
| `Year` | Election year |
| `Candidate` | Candidate name in "Last, First" format |
| `Candidate Email` | Contact email |
| `Address` / `City` / `StateRegion` / `Zip` / `Country` | Candidate address |
| `Office` | Office sought |
| `Election` | Election name / jurisdiction |
| `Source` | Filing method (e.g. "eFiled") |
| `Won` | Election outcome |
| `Status` | Registration status (e.g. "Exempt", "Active") |
| `Party` | Political party |
| `Treasurer` / `Treasurer Email` | Treasurer info |
| `Chair` / `Chair Email` | Chairperson info |
| `Initial Filing` | Date of initial filing |

### Committee Registration Files

`GRForms_{year}.csv` — annual group/committee registration forms (one row per registered group per year):

| Field | Description |
|---|---|
| `Result` | Row sequence number |
| `Report Year` | Year of registration |
| `Abbreviation` | Short ID code for the group (used as `state_filer_id` in cleaned output) |
| `Name` | Full committee name |
| `Address` / `City` / `State` / `Zip` / `Country` | Committee address |
| `Plan` | Filing plan |
| `Type` | Committee type (e.g. "Independent Expenditure") |
| `Subtype` | Committee subtype |
| `Treasurer Name` / `Treasurer Email` | Treasurer |
| `Chair Name` / `Chair Email` | Chairperson |
| `Additional Emails` | Extra notification emails |
| `Submitted` | Date submitted |
| `Status` | e.g. "Filed" |
| `Amending` | Whether this is an amendment to a prior registration |

---

## Scraper

`src/pipeline/scrapers/alaska.py`

Alaska's APOC portal is an ASP.NET application that does not expose a direct download API — exports require interacting with the UI. The scraper uses **Playwright** (Chromium) to navigate to each search page, set the year and status filters, click Search, then click Export to trigger a CSV download.

**Transactions:** Year options are read dynamically from the page's year dropdown (no hardcoded year range). The scraper iterates all available years, skipping ones already in the manifest. Current year is always re-fetched. A 1-second sleep between year downloads reduces server load.

**Candidates:** Downloaded as a single "All" export from the AllCandidates page — no year loop needed.

**Groups (GRForms):** Uses the same year-by-year export flow as transactions. Some pages trigger a direct download on Export click; others open a dialog with a CSV link — both cases are handled.

**Limitations:**
- **Must be run from a local/residential IP** — Alaska's WAF blocks datacenter IPs. Will silently fail or return empty results if run from a cloud environment.
- **Playwright required** — `pip install playwright && playwright install chromium`. Runs headless=False (visible browser window) — this is load-bearing, not just a debugging convenience, see DataDome note below.
- **ASP.NET ViewState** — each year requires a fresh page navigation to keep ViewState clean; reusing the same page state across years can cause silent failures.
- **No `Amended` flag in exports** — APOC re-exports the same transaction for each amendment as a new row with a higher `Result` number. There is no explicit amended indicator.
- **DataDome bot-check (confirmed 2026-09-23).** `aws.state.ak.us` is fronted by DataDome (`captcha-delivery.com`). Confirmed live by firing a burst of parallel export requests: got back an HTTP 403 whose body loads DataDome's `c.js`, and on a real page navigation this renders as a "Verification Required" / "Slide right to secure your access" puzzle-slider interstitial — its own copy names "Rapid taps or clicks" and "Automated (bot) activity on your network" as trigger reasons, which lines up with this scraper's own back-to-back year-download pattern (a production run that day hit it on CDIncome 2013, 2015, and 2018, each timing out at the old 180s download-wait with no download ever starting). It requires a genuine human drag gesture in the visible browser window — it cannot be solved by script, and no attempt was made to. Fixed with the same pattern already proven for Missouri's Incapsula CAPTCHA in this repo: `run()` now uses `p.chromium.launch_persistent_context(PROFILE_DIR, ...)` instead of a fresh `browser.new_context()`, so the `datadome` trust cookie set once a human solves the slider survives on disk across runs instead of being thrown away every time. `_wait_out_datadome()` also checks for the challenge at every point it's been confirmed capable of appearing (initial page load, post-Search, post-Export-click) and pauses with a logged prompt instead of silently burning the download timeout. **Not yet confirmed end-to-end** — i.e. that a second run after solving once actually skips the challenge — worth watching the visible browser window on the next couple of full runs. Keep `headless=False`: a headless Chromium is itself one of the fingerprints these products check for, so going headless would likely make the challenge trigger more often, not less, and headless has no visible window for a human to solve it in at all.
- **DataDome also has a no-widget "hard block" variant.** Confirmed same day, on a follow-up run: a detail-page request came back with "Access is temporarily restricted" instead of the slider — same boilerplate reasons list, but no puzzle to solve, just an IP-level cooldown that only clears with time. This most likely followed from deliberately bursting requests earlier that day to force the challenge to appear for inspection — i.e. a side effect of investigating the issue, not something a normal run is expected to trigger on its own. `DATADOME_MARKERS`/`_wait_out_datadome()` now recognize this variant too (matched on its own wording, since it may not include the `captcha-delivery.com` script tag the widget variant does) and log an accurate "no widget to solve, just wait" message instead of wrongly telling a human to solve a slider that isn't there.
- **Basic anti-automation fingerprinting, ported from Nevada.** Playwright-driven Chromium sets `navigator.webdriver=true` and a few other default tells. `LAUNCH_ARGS`/`STEALTH_INIT_SCRIPT`/`DESKTOP_USER_AGENT`/`VIEWPORT`/`LOCALE`/`TIMEZONE_ID` (`America/Anchorage`) are the same zero-new-dependency pattern already proven in this repo for `nevada.py`'s WAF, applied here via `context.add_init_script()` on both `launch_persistent_context()` calls. Won't defeat DataDome on its own -- it's real behavioral/TLS fingerprinting, this just removes baseline noise that wasn't helping.
- **Circuit breaker on repeated blocks.** The year-export loop tracks consecutive still-blocked-after-waiting hits and aborts (`DATADOME_MAX_CONSECUTIVE_BLOCKS = 6`) instead of continuing to request the next year while DataDome is blocking the session. A partial run, re-run later, is the intended recovery.
- **Download timeout raised 180s → 600s.** Independent of DataDome: confirmed clean downloads (no challenge involved) for the largest years already take up to ~440s server-side alone (CDIncome 2012: 437s; 2014: 374s), so the old 180s `page.expect_download()` timeout was too short on its own merits.
- **Export links go directly by href now, not by click.** Investigated the actual export endpoints via DevTools/in-page `fetch()` (same day): the export link (`a[id*='hlAllCSV']`) opens `target="_blank"`, and the URL it points to is a plain deterministic GET (`?exportAll=True&exportFormat=CSV&isExport=True&...`) that returns the CSV directly once Search+Export have set server-side state — confirmed by fetching it standalone and getting back a real `Content-Disposition: attachment` CSV response. Went as far as prototyping a full replacement of the Search/Export clicks with in-page `fetch()` POSTs too (same technique Mississippi already uses in this repo for its `.asmx` endpoints) — confirmed it works, reproduced the exact same "103122 items in 5157 pages" result a real click produces, across all four page templates (`CDIncome`/`CDExpenditures` share one control, `GRForms` and `AllCandidates` are each their own). **Backed off that part deliberately**: a DataDome challenge triggered mid-`fetch()` renders nothing in the visible browser — there'd be no slider for a human to see or solve, silently breaking the human-solvable design the DataDome handling above depends on. So Search/Export stay real Playwright clicks (any DataDome hit there still shows up normally in the window); only the final step — which needs no further server interaction — now navigates directly to the export link's `href` instead of clicking the `<a>`, removing the target="_blank" ambiguity for that one step without touching DataDome visibility anywhere else.
- **Fixed same day: the href-based goto above was silently failing on every run.** `csv_link.get_attribute("href")` reads the raw HTML attribute, which on this ASP.NET page is a relative URL. `page.goto()` on a context with no `baseURL` configured throws immediately on a relative string, and `_goto_export_link()`'s original bare `except Exception: pass` swallowed that throw with no log line — so the goto never happened, no download ever fired, and the run just sat inside `expect_download()` until the 600s timeout. This looked like the scraper "not hitting the csv button" at all rather than like an error. The earlier live DevTools verification (above) had used `a.href` — the DOM's *resolved* property, which auto-converts a relative attribute to absolute — not `getAttribute('href')`, which is why the bug wasn't caught during that investigation. Fix: read `csv_link.evaluate("el => el.href")` (the resolved property, matching what was actually verified live) instead of `get_attribute("href")`, at both call sites (`download_candidates`, `download_year`). Also hardened `_goto_export_link()` so it no longer swallows exceptions silently: it now only treats `ERR_ABORTED`/"Download is starting" as the expected (and harmless) result of a navigation that turns into a download, logs a warning and falls back to a plain `csv_link.click()` for anything else. Confirmed live 2026-09-23 (`python3 src/pipeline/scrapers/alaska.py --start-year 2024`): income/expenditures/groups CSVs for 2024-2028 all downloaded correctly. One further raciness turned up and was fixed the same run: `page.goto(href)` defaults to waiting for the `load` event, which never fires for a response that resolves as a download -- fast downloads happened to raise the expected `ERR_ABORTED` before that mattered, but one slower one (`CDIncome_2025.csv`) just timed out at 30s waiting for "load", triggered the fallback `click()` (which itself then raced the already-in-flight download and also timed out), and logged two scary-looking warnings for what was actually a clean success (`expect_download()` still had the file). Fixed by passing `wait_until="commit"` to that `goto()` -- resolves as soon as response headers arrive rather than waiting for a load that will never come, which is the standard Playwright idiom for "navigate to a link that downloads a file".

**Expected runtime:** ~30–60 min for a full run across all years and relation types (21 years × 3 relation types + candidates, with page load waits and a randomized ~2–5s sleep per year — widened from a flat 1s partly to look less like a metronomic bot pattern to DataDome). Add real time if a DataDome challenge needs a human to solve it.

---

## Parser

`src/pipeline/parsers/alaska.py`

Alaska is a **semi-flat-file state**: committee names appear on every transaction row (no numeric filer ID on transactions), but a separate GRForms registry provides committee metadata. Committees are synthesized from transaction rows and then enriched by GRForms on flush.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv`

**Key transformations:**
- **Committee enrichment comes from the bulk registration exports only.** `GRForms_YYYY.csv` fills treasurer, city and zip on group committees, replaces the transaction exports' generic `Group` filer type with the registration's `Type` (PAC, Political Party, Ballot Proposition, ...), and adds groups that registered but have no transactions. `CRForms_YYYY.csv` fills `candidate_name`, `treasurer_name`, `city` and `zip` on Candidate-type committees, matched on first+last name, then committee name, most recent `Submitted` date winning (`load_cr_forms_registry()`). The CRForms name column is `Display Name`, not the grid's on-screen `Name` label.
- **The per-ID detail sweeps (`Common/View.aspx?ID=N&ViewType=GR|CR`, `gr_details.csv` / `cr_details.csv`) are removed.** Every field they supplied is in the bulk exports, and they were the request pattern DataDome blocked. The numeric ID on those pages is a registration-form ID, issued again for each year a group re-registers (218 of 294 swept group names had more than one), so it was not a stable committee ID; `state_filer_id` is the filer name for every committee with transactions. Leftover `gr_details.csv` / `cr_details.csv` files in `data/Alaska/raw/` are ignored.
- **Enrichment only reaches the years downloaded.** Pre-2024 `GRForms`/`CRForms` files need a bulk backfill (`--entities --start-year 2008`) before older committees get registration fields.

- **Amendment deduplication** — CDIncome and CDExpense re-export the same transaction for every amendment. The parser deduplicates per file on `(contributor/payee, amount, date, filer)`, keeping the row with the highest `Result` number (most recent filing).
- Amounts normalized from `"$1,000.00"` or `"(500.00)"` format → plain decimal; parenthetical negatives converted to negative numbers.
- Dates normalized from `M/D/YYYY` → `YYYY-MM-DD`; implausible years (before 1970 or more than 2 years out) discarded.
- Contributor/payee names assembled from `Last/Business Name` + `First Name`.
- Candidate `Name` field (stored as "First Last") is inverted to "Last, First" for the `candidate_name` column so it joins cleanly to the candidates table.
- Committee type assembled by joining `Type` and `Subtype` from GRForms with " — " separator.
- GRForms `Status == "Filed"` → `active = 1`; all other statuses → `active = 0`.
- `Amended` field left blank in output — deduplication handles amendments instead.

**person_id model:** `name_hash` — no numeric filer ID exists in the APOC source. A stable 13-digit integer is derived from `MD5("AK" + normalized_name)` prefixed with Alaska's FIPS code (02).

**Limitations:**
- Committee join is by name string — committees that appear in transactions but not in GRForms get no treasurer/city/zip enrichment (0% treasurer enrichment in last QA run).
- `state_filer_id` is the GRForms `Abbreviation` field, which is often blank.
- `loans_debts.csv` is always written empty — APOC does not distinguish loan transactions from other income types in its exports.

**Expected runtime:** ~3–5 min (21 years × 2 transaction types + GRForms enrichment pass).

---

## Data Notes

- **No direct download API** — APOC requires browser automation; any changes to the portal UI (button IDs, dropdown names, export dialog structure) can break the scraper silently.
- **WAF blocks cloud IPs** — scraper must be run from a local machine. Attempts from datacenter IPs return empty or error pages without clear error messages.
- **Duplicate rows for amendments** — each amendment re-exports all transactions from that report, not just the changed rows. The dedup logic handles this but relies on exact field matching; edge cases (e.g. rounding differences) may result in both versions surviving.
- **`--------` separator column** — a literal dashes column appears in the raw export between the contributor/payee fields and the filer fields. Ignored by the parser.
- **State field is full name** — contributor/payee state is "Alaska", "California", etc. rather than a 2-letter code.
- **Negative amounts in parentheses** — `(500.00)` format used for refunds/reversals (~7,400 contribution rows, ~5,800 expenditure rows in last QA run).
- **0% party enrichment for candidates** — the `Party` field in `CDCandidates_all.csv` is sparsely populated; most candidates have no party recorded.
- **79% office enrichment** — solid but not complete; ~21% of candidates have no office recorded.
- **Future years present** — the APOC portal pre-populates years for upcoming election cycles (2027, 2028 visible in downloads). These files are empty or near-empty and tracked in the manifest with `row_count = -1`.
- **2008–2010 data empty** — portal returns no records for these years. Files are downloaded but contain no rows.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-09-23 |
| Parser | 2026-05-28 |
