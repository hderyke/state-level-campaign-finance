# Pennsylvania — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Pennsylvania (PA) |
| **Source** | [PA Dept. of State — Campaign Finance Data](https://www.pa.gov/agencies/dos/resources/voting-and-elections-resources/campaign-finance-data) |
| **Access method** | Plain HTTP download of one "Full Campaign Finance Export" zip per year (server-rendered link list, no browser automation needed) |
| **Coverage** | 2000 – present |
| **person_id model** | `committee` — `person_id` derived from `state_filer_id`, which DOS reissues per registration/candidacy. Same real person can hold several `person_id`s across different offices/cycles (see Data Notes). |
| **FIPS** | 42 |

---

## Raw Data Structure

Each `data/Pennsylvania/raw/{year}.zip` contains five fixed-format CSVs (DOS's "Technical Specifications for Electronic Filing of Campaign Expense Reports"). 2025+ zips have the files at the zip root (`filer_2026.txt`); pre-2025 zips nest them one level down inside a `{year}/` folder (`2022/filer_2022.txt`) — the parser's `_resolve_member` handles both transparently.

Every row across all five files carries a `CampaignFinanceID` (a per-report-submission ID, not the same as `FILERID`) — this is the join key back to `filer_{year}.txt`'s per-report cover sheet, not a stable per-entity ID.

### filer_{year}.txt (report cover sheets)
| Field | Description |
|---|---|
| `CampaignfinanceID` | Per-submission ID — joins to the other four files |
| `FILERID` | The filer entity's own ID (candidate or committee registration) |
| `EYEAR` / `SubmittedDate` / `CYCLE` | Filing period metadata |
| `AMMEND` | Y/N — amended filing |
| `TERMINATE` | Y/N — filer has closed out |
| `FILERTYPE` | `1`=Candidate, `2`=Committee, `3`=Lobbyist, `4`=undocumented (rare, suppressed) |
| `FILERNAME` | Filer's registered name |
| `OFFICE` / `DISTRICT` / `PARTY` | Only reliably populated for FILERTYPE=1 (candidate) rows — see Data Notes |
| `ADDRESS1/2`, `CITY`, `STATE`, `ZIPCODE`, `COUNTY`, `PHONE` | Filer address/contact |
| `BEGINNING`, `MONETARY`, `INKIND` | Reported balances for this filing period |

### contrib_{year}.txt (Schedule I Parts A-D, Schedule II Parts F-G)
`CampaignFinanceID`, `FilerID`, `Section` (raw code — see `transaction_categories.csv`), `CONTRIBUTOR`, address fields, `OCCUPATION`, `ENAME` (employer) + employer address, up to three `CONTDATE{1-3}`/`CONTAMT{1-3}` pairs (DOS's way of recording multiple contributions from the same contributor in one reporting period without a separate row each), `CONTDESC`.

### expense_{year}.txt (Schedule III)
`CampaignFinanceID`, `FILERID`, `EXPNAME`, payee address, `EXPDATE`, `EXPAMT`, `EXPDESC`. No type/category field of any kind — see Data Notes.

### debt_{year}.txt (Schedule IV)
`CampaignFinanceID`, `FILERID`, `DBTNAME`, counterparty address, `DBTDATE`, `DBTAMT`, `DBTDESC`.

### receipt_{year}.txt (Schedule I Part E — other receipts)
`CampaignFinanceID`, `FILERID`, `RECNAME`, address fields, `RECDESC`, `RECDATE`, `RECAMT`. Folded into `contributions.csv.gz` with `transaction_type="Other Receipt"` — real dollars into the filer's account, just not solicited from a contributor (refunds, interest, returned checks).

---

## Scraper

`src/pipeline/scrapers/pennsylvania.py`

The Campaign Finance Data page is a plain server-rendered link list ("2026 Full Export", "2025 Full Export", ...) — the scraper fetches it once with `requests`+BeautifulSoup, matches each link's *visible text* against the years requested, and downloads the zip straight over HTTP. No Selenium/Playwright needed.

Deliberately matches on link text rather than trusting a guessed `{year}.zip` URL pattern: as of this writing, the "2002 Full Export" link's `href` actually points at `.../2022.zip` — a copy/paste error on DOS's own site. `verify_zip_year()` cross-checks the downloaded zip's internal `filer_{year}.txt` filename against the requested year and refuses to save a mismatched file.

**Output:** `data/Pennsylvania/raw/{year}.zip`, one per year 2000–present, untouched from DOS. `manifest.csv` (year, source_url, link_text, bytes, scraped_at) lives at `data/Pennsylvania/`, one level up from `raw/`.

**Limitations:** current year is always re-downloaded even if already on disk (it keeps gaining filings all year, same pattern as other states' "still-open cycle" handling).

**Expected runtime:** a few minutes for a full 2000–present pull; incremental runs are near-instant (skip-if-on-disk except current year).

---

## Parser

`src/pipeline/parsers/pennsylvania.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `debts.csv.gz`, `candidates.csv.gz`, `committees.csv.gz` — candidates/committees deduped globally across all years processed (PA filer IDs are stable across cycles), not per-year.

**Key transformations:**
- Multiple `CONTDATE{n}`/`CONTAMT{n}` pairs per contribution row become separate output rows.
- `receipt_{year}.txt` folded into `contributions.csv.gz` as `transaction_type="Other Receipt"`.
- `candidate_name` on committee/transaction rows is populated when a filer's own cover sheet has `OFFICE` set — but see Data Notes, this misses most large statewide committees.

**person_id model:** `committee` (`utils.assign_person_ids(id_model="committee")`). DOS reissues a new `state_filer_id` per candidacy/registration, so the same real person accumulates multiple `person_id`s across their career if they've run for different offices or re-registered a committee (e.g. Josh Shapiro has separate `person_id`s for his 2004-2008 State House runs, 2010 State House run, 2016 Attorney General run, and 2022/2026 Governor runs — all correctly the same person, but not unified under one ID). This matches the id_model's documented behavior elsewhere in the pipeline, not a PA-specific bug.

**Committee → candidate linkage (`PA_COMMITTEE_CANDIDATE_OVERRIDE`):** PA's `filer_{year}.txt` has no field anywhere linking a FILERTYPE=2 money committee to its candidate's own FILERTYPE=1 registration — confirmed directly against the raw file (e.g. "Shapiro for Pennsylvania" `FILERID=20160016` and "SHAPIRO, JOSHUA D" `FILERID=2022C0206` share no column). The parser's default heuristic (candidate_name = committee's own name, only when `OFFICE` is set on the committee's own cover sheet) misses most large statewide committees, since they typically leave `OFFICE` blank. A general fuzzy-match fix (committee name contains a candidate surname unique to one person) was tried and rejected — it produced roughly 1 false link in 10 on a random sample of PA's 61k committees (dirty/committee-style `candidate_name` values already in the source, plus first-name/surname collisions, e.g. "Friends of David Freed" incorrectly matched an unrelated "Slavick David"). Replaced with a small hand-verified table (`PA_COMMITTEE_CANDIDATE_OVERRIDE` in the parser): `state_filer_id -> [(election_year, exact candidate_name)]`, currently covering Josh Shapiro, Tom Wolf, Doug Mastriano, Scott Wagner, and Lou Barletta's principal committees. **Tom Corbett is a known, deliberately unfixed gap** — his "Tom Corbett for Governor" committee has `OFFICE` populated (unlike the others) but there is no FILERTYPE=1 candidate row for him at all in 2010/2014 `filer.txt`, so there's nothing to link to without synthesizing a candidates.csv row from committee data alone.

**Limitations:**
- `contributor_type` (contributions) and `transaction_type`/`category` (expenditures) are 0% filled — PA's source data has no such fields at all (see Raw Data Structure), not a parser gap.
- `jurisdiction` and `incumbent` (candidates) are 0% filled — not available in the source.
- Committee `candidate_name` fill rate is ~26% even after the override table above — most of PA's 61k committees are genuinely independent PACs/party committees with no candidate to link to; this is expected, not a bug.

**Expected runtime:** full 2000–present parse is too slow for a single 45-second sandboxed call (~34s for one year alone with ~1.3M contribution rows); run locally for a full reparse.

---

## Data Notes

- **25M contribution rows is real, not a bug.** PA requires itemizing contributions at a much lower threshold than federal filings (no ~$200 unitemized exemption) — 68% of all 25M rows are $50 or less. Volume has been steady at ~1–1.3M rows/year since 2008, spread across thousands of committees (no single runaway source dominates). Confirmed via direct inspection, not assumed.
- **~2.9M rows (11.7%) are flagged `amended='Y'`** — whether PA's amendment process re-files entire reports (causing double-counting the way Illinois's `Archived=True` duplicate rows did until fixed) has not been verified. Flagged, not yet investigated.
- **A handful of expenditure rows are almost certainly filer data-entry typos**, not parser bugs — e.g. a single row attributes $517.2M to "ACME MARKETS" (from "Friends of Cathy Spahr", a small local committee) and another attributes $304.2M to "PNC" (from "Friends of Austin Davis"). Already caught by `validate.py`'s tier-2 warning ("11 rows have \|amount\| ≥ $10,000,000"). Left as-is by design (2026-07-12) — to be noted on a future data-quality page rather than filtered in the pipeline.
- **`committee_type='PAC'` includes candidate-controlled committees.** PA's FILERTYPE=2 is a single catch-all code for every non-candidate, non-lobbyist committee — the source doesn't distinguish PACs from party committees from a candidate's own money committee. CA and GA both have a precedent `aggregate.py` override (`committee_type='PAC' AND candidate_name != '' -> 'Candidate Committee'`) for exactly this pattern; **not yet applied to PA** — committees now correctly linked via `PA_COMMITTEE_CANDIDATE_OVERRIDE` (e.g. "Shapiro for Pennsylvania") still display `committee_type=PAC` rather than `Candidate Committee`. Flagged for a future pass.
- **`queries.py`'s "non-candidate committees" report was double-counting.** Its exclusion logic only checked for a literal string match between `committee_name` and `candidate_name` (catching self-referential cases like Corbett's) — it never checked `committees.person_id`, so a committee correctly linked to a *differently-named* candidate (e.g. "Shapiro for Pennsylvania" → "SHAPIRO, JOSHUA D") appeared in both "Recipient Candidates" and "Non-Candidate Committees" simultaneously. Fixed 2026-07-12 (added a `person_id IS NOT NULL` check) — this is a shared file used by every state's report, not a PA-specific change, though PA is what surfaced it.
- **`raw/` folder convention fix (2026-07-11):** the scraper previously wrote zips directly to `data/Pennsylvania/` instead of `data/Pennsylvania/raw/`, the only state doing so. Fixed in both scraper and parser; existing files moved.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-11 |
| Parser | 2026-07-12 |
| Alias mappings | 2026-07-12 |
| Docs | 2026-07-12 |
