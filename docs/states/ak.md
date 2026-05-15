# Alaska — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Alaska (AK) |
| **Source** | [Alaska Public Offices Commission (APOC)](https://aws.state.ak.us/apocreports/Home.aspx) |
| **Access method** | Playwright browser automation (live Chromium session required) |
| **Coverage** | 2008 – present (2008–2010 empty; meaningful data starts 2011) |

---

## Raw Data Structure

Files live in `data/Alaska/raw/`. Four file types: two transaction tables (one per year), one static candidate registry, and annual group/committee registration forms.

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
- **Playwright required** — `pip install playwright && playwright install chromium`. Runs headless=False (visible browser window).
- **ASP.NET ViewState** — each year requires a fresh page navigation to keep ViewState clean; reusing the same page state across years can cause silent failures.
- **No `Amended` flag in exports** — APOC re-exports the same transaction for each amendment as a new row with a higher `Result` number. There is no explicit amended indicator.

**Expected runtime:** ~30–60 min for a full run across all years and relation types (21 years × 3 relation types + candidates, with page load waits and 1s sleep per year).

---

## Parser

`src/pipeline/parsers/alaska.py`

Alaska is a **semi-flat-file state**: committee names appear on every transaction row (no numeric filer ID on transactions), but a separate GRForms registry provides committee metadata. Committees are synthesized from transaction rows and then enriched by GRForms on flush.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv`

**Key transformations:**
- **Amendment deduplication** — CDIncome and CDExpense re-export the same transaction for every amendment. The parser deduplicates per file on `(contributor/payee, amount, date, filer)`, keeping the row with the highest `Result` number (most recent filing).
- Amounts normalized from `"$1,000.00"` or `"(500.00)"` format → plain decimal; parenthetical negatives converted to negative numbers.
- Dates normalized from `M/D/YYYY` → `YYYY-MM-DD`; implausible years (before 1970 or more than 2 years out) discarded.
- Contributor/payee names assembled from `Last/Business Name` + `First Name`.
- Candidate `Name` field (stored as "First Last") is inverted to "Last, First" for the `candidate_name` column so it joins cleanly to the candidates table.
- Committee type assembled by joining `Type` and `Subtype` from GRForms with " — " separator.
- GRForms `Status == "Filed"` → `active = 1`; all other statuses → `active = 0`.
- `Amended` field left blank in output — deduplication handles amendments instead.

**Limitations:**
- Committee join is by name string — committees that appear in transactions but not in GRForms get no treasurer/city/zip enrichment (0% treasurer enrichment in last QA run).
- `state_filer_id` is the GRForms `Abbreviation` field, which is often blank.
- `loans_debts.csv` shows 12,685 rows in the last test run despite the parser not explicitly writing to it — likely populated by loan-type income rows in a prior parser version; worth investigating.

**Expected runtime:** ~3–5 min (21 years × 2 transaction types + GRForms enrichment pass).

---

## Data Quirks

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

## Status

- [x] Scraper complete
- [x] Parser complete
- [x] Loaded into DB
- [x] Verified / QA'd (passed — see `tests/reports/alaska_latest.json`)

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-08 |
| Parser | 2026-05-08 |
