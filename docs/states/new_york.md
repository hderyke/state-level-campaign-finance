# New York — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | New York (NY) |
| **Source** | [New York State Board of Elections (NYSBOE) open data](https://data.ny.gov) — Socrata SODA API |
| **Access method** | Pure HTTP/`requests` against the Socrata API (no Playwright, no TLS impersonation) |
| **Coverage** | Disclosure transactions for election years 1999–2027 (18.36M rows); filer registry back to 1974 |
| **person_id model** | `committee` — `filer_id` is per-registration, not per-person (the registry holds seven separate `filer_id`s named "Eric A. Ulrich"), so `person_id` = min `filer_id` per (state, candidate_name, office, district) |

---

## Raw Data Structure

Four Socrata datasets, each identified by a dataset ID:

| Dataset | Stem | ID | Rows | Maps to |
|---|---|---|---|---|
| Campaign Finance Disclosure Reports Data (Beginning 1999) | `Disclosure` | `e9ss-239a` | 18,358,201 | contributions / expenditures / loans_debts |
| Campaign Finance Filer Data (Beginning 1974) | `Filers` | `7x2g-h32p` | 64,464 | candidates + committees |
| Campaign Finance Active Committees Data | `ActiveCommittees` | `udeh-rt5n` | subset of Filers | `committees.active` overlay |
| Campaign Finance Active Candidates Data | `ActiveCandidates` | `epr8-9fny` | subset of Filers | candidates overlay |

`ActiveCommittees` and `ActiveCandidates` are the `filer_status = ACTIVE` slice of `Filers`, split by `compliance_type_desc`. They're pulled separately rather than derived because NYSBOE refreshes them on their own cadence.

### One table, twenty-one schedules

Everything financial lives in `Disclosure`. A row's `filing_sched_abbrev` (a single letter, A–U) is the only thing that says whether it's a contribution, an expenditure or a loan. The full map, with live row counts as of 2026-07:

| Sched | `filing_sched_desc` | Rows | Canonical table |
|---|---|---|---|
| A | Monetary Contributions Received From Ind. & Part. | 10,966,454 | contributions |
| B | Monetary Contributions Received From Corporation | 852,284 | contributions |
| C | Monetary Contributions Received From All Other | 933,445 | contributions |
| D | In-Kind (Non-Monetary) Contributions Received | 100,309 | contributions |
| E | Other Receipts Received | 106,282 | contributions |
| F | Expenditures/ Payments | 4,229,752 | expenditures |
| G | Transfers In | 75,109 | contributions |
| H | Transfers Out | 84,132 | expenditures |
| I | Loans Received | 20,345 | loans_debts |
| J | Loan Repayments | 10,504 | loans_debts |
| K | Liabilities/Loans Forgiven | 9,666 | loans_debts |
| L | Expenditure Refunds (Increases Balance) | 43,189 | expenditures |
| M | Contributions Refunded (Decreases Balance) | 94,516 | contributions |
| N | Outstanding Liabilities/Loans | 174,611 | loans_debts |
| O | LLCs/Partnership/Subcontractor | 160,842 | **none — see below** |
| P | Non-Campaign Housekeeping Receipts | 106,155 | contributions |
| Q | Non-Campaign Housekeeping Expenses | 264,520 | expenditures |
| R | Expense Allocation Among Candidates | 49,959 | expenditures |
| S | Public Fund Receipts | 1,435 | contributions |
| T | Qualified Expenditures | 74,649 | expenditures |
| U | Public Fund Repayment | 43 | expenditures |

**Schedule O is deliberately dropped.** It's a detail schedule — the itemization of who is behind an LLC contribution, or which subcontractors a vendor paid — and 71,401 of its 160,842 rows carry a `trans_mapping` pointing at a parent transaction that is itself already written. Writing O would double-count that money. The dropped rows are counted and reported in the parse log as `skipped_detail`, so the omission is visible rather than silent.

### Key raw fields

| Field | Notes |
|---|---|
| `filer_id` | Joins a transaction to the filer registry. Per-registration, not per-person |
| `cand_comm_name` | The filer's name as of that filing — the only entity info on a transaction row |
| `election_year` | NYSBOE's "Disclosure Report Year" (the cycle a transaction was filed under), **not** the calendar year of `sched_date`. Populated on every row; this is what the scraper splits files by |
| `sched_date` | Transaction date. `org_date` holds the original parent date on schedules K/L/M/N |
| `org_amt` | **The amount.** There is no column named `amount`. Confirmed by sampling rows from schedules A, D, F, I, N and O |
| `owed_amt` | Outstanding balance; populated only on the liability schedules. Used as the `original_amount` fallback |
| `flng_ent_name` / `flng_ent_first_name`+`_middle_name`+`_last_name` | Counterparty name, in two mutually exclusive shapes — organizations use the first, individuals the second |
| `r_amend` | `Y`/`N` → `amended` |
| `trans_number` | Unique per transaction; used as `filing_id` (NY exposes no report-level id) |
| `cntrbr_type_desc` | Contributor type — 14 values, mapped in `src/aliases/contributor_types.csv` |
| `purpose_code_desc` | Expense purpose code — 48 values → `expenditures.category` |
| `loan_other_desc` | Lender category on I/N (`Candidate`, `Bank`, `Other Entities`, ...) |

---

## Scraper

`src/pipeline/scrapers/new_york.py`

**Pagination:** `$limit=50000` with `$offset` and `$order=:id`, looping until a short page comes back. `$order=:id` is not optional — Socrata's default order is unspecified, and paging an unspecified order across 18M rows silently loses records.

**Year splitting:** `Disclosure` is fetched one file per election year via `$where=election_year={year}`, from `EARLIEST_YEAR = 1999` through `current_year + 2`. The `+2` matters: NYSBOE already carries election_year 2027 rows in mid-2026, and a current-year ceiling would drop them. A `Disclosure_misc.csv` bucket sweeps NULL / out-of-range years — no such rows exist today (the live per-year group-by sums exactly to the table total), but the bucket costs one request and stops a future bad load from falling through the gap unnoticed.

**Snapshots:** the three registry datasets have no year column, so they're re-fetched in full on every run. They're tens of thousands of rows, and a stale filer roster is strictly worse than re-pulling one.

**Manifest:** `data/New York/manifest.csv` keyed on `(relation_type, year)`, where `year` is a 4-digit year, `"misc"`, or `"snapshot"`. A no-flag run skips closed years; the current year and the two future years are always re-fetched, since those cycles are still being filed against.

**Scope flags:** `--transactions` / `--contributions` / `--expenditures` all resolve to the disclosure dataset — contributions and expenditures share one table, so there's nothing narrower to fetch without splitting raw files by schedule letter. `--candidates` → filers + active candidates; `--committees` → filers + active committees; `--entities` → all three registries.

**Expected runtime:** the first full run is dominated by the 18.4M-row disclosure pull (roughly 370 paged requests at 50K rows each). Reruns with a populated manifest fetch three snapshots plus three open years.

---

## Parser

`src/pipeline/parsers/new_york.py`

**Output tables:** `contributions.csv.gz`, `expenditures.csv.gz`, `loans_debts.csv.gz`, `committees.csv.gz`, `candidates.csv.gz`

**Processing order:** registries first (so every transaction row can resolve its filer in one pass), then the committee→candidate linkage pass, then the disclosure files year by year.

**Key transformations:**

- `amount` = `org_amt`, falling back to `owed_amt`. Rows with neither are dropped.
- `transaction_type` is the schedule label from `SCHEDULES`, with a **bounded** sub-type appended for schedules D/E/G only (`contribution_subtype()`). Boundedness is the point: `transaction_type` is one of the columns `src/aliases/transaction_categories.csv` maps per raw value, so its cardinality has to stay hand-enumerable. NY's two `transfer_type_desc` values are full sentences, and are shortened to `Type 1` / `Type 2`.
- `parse_date()` blanks any year outside 1990 – (current + 4) so a source typo can't fail the whole state's date check.
- Schedule R rows describe a *different* candidate than the filer (the one an allocated expense is attributed to), so the row's own `office_desc` overrides the filer's office when present.
- A `filer_id` seen in the transactions but absent from the registry gets a committee row synthesized from `cand_comm_name`. This shouldn't happen — `Filers.csv` is the full historical registry — but if NYSBOE's extracts drift, the money stays attributable to a named entity.

**Candidate ↔ committee linkage.** NYSBOE assigns a candidate and their authorized committee two unrelated `filer_id`s and publishes no join between them. `candidate_from_committee_name()` strips the conventional wrapper off a committee name and accepts the result **only** if it exactly matches a registered candidate name after normalization:

```
"Friends Of Sheila Marcotte"  -> SHEILA MARCOTTE
"Elect Jennifer Stevenson"    -> JENNIFER STEVENSON
"Joe Lhota For Mayor Inc"     -> JOE LHOTA
```

Anything ambiguous, or extracting to something no candidate is registered under, is left blank. A missing link is much cheaper than a wrong one here, because `utils.assign_committee_person_ids()` propagates whatever lands in `candidate_name` straight into `person_id`. The match count is logged per run via `enrichment_summary`.

**`candidates.election_year`** isn't published on any registry dataset, so it's derived as the latest `election_year` seen for that candidate across the disclosure files — counting both money filed under their own candidate `filer_id` and money filed by a committee that resolved to them.

**person_id model:** `committee`. `filer_id` is per-registration — the filer registry's own column statistics show seven distinct `filer_id`s named "Eric A. Ulrich" — so `assign_person_ids(id_model="committee")` collapses them by taking the minimum `filer_id` per (state, candidate_name, office, district).

---

## Party enrichment overlay

`src/pipeline/scrapers/new_york_party.py` → `src/pipeline/parsers/new_york_enrich.py`

NYSBOE's campaign-finance datasets carry no party column, so `candidates.party` can only be filled by joining something else in. This overlay does that. It is **entirely optional**: if its raw files are absent the parser logs a warning and writes the same blanks it always did.

### Sources

| Source | Endpoint | Covers | Key fields |
|---|---|---|---|
| NYSBOE Election Results | `results.elections.ny.gov` (Civera/ElectionStats backend at `ny.elstats.civera.com`) | 1994–2025, statewide + congressional + legislative + judicial | candidate, ballot line, votes, winner, office, district, year |
| Open States | `data.openstates.org/people/current/ny.csv` | currently-serving legislators only (~213) | `current_party`, `current_district`, `current_chamber` |

Two endpoints on the results backend, both read off the server-rendered markup of a live contest page rather than guessed:

```
GET /api/download_contest/{contest_id}_table.csv?split_party=true
GET /api/download_search.csv?search={url-encoded json}
```

`split_party=true` is the one that matters. It returns one row per candidate **per ballot line** instead of collapsing them, which is what makes fusion voting representable at all.

The scraper tries the whole-database search export first (one request); if that returns nothing recognisable it falls back to walking contest IDs, preferring the per-contest CSV and dropping to HTML parsing of `/contest/{id}` if the CSV headers have moved. Contest IDs are dense but unordered by year — 1994 sits in the ~4900s — so the walk stops on a run of 400 consecutive misses rather than the first one.

**Search export schema** (26 columns, observed 2026-07-25). Three columns in it are easy to get wrong, and all three are load-bearing:

| Column | Role | Trap |
|---|---|---|
| `candidate_party_name` | the candidate's ballot line | this is `party` |
| `primary_party` | which party's primary the *contest* is | **not** the candidate's party — blank on general-election rows. Used only to build the stage string ("Democratic Primary") |
| `district_name` | the seat, "127th Assembly District" | `district` parses its digits |
| `division_name` | the geography the row's votes were counted in | conflating it with `district_name` puts a county name in `district` |

`division_*` together with `vote_channel` mean the export is one row per candidate **per ballot line per division per voting method** — a statewide candidacy can span hundreds of rows differing only in county and machine-vs-absentee. `_aggregate()` collapses them to one row per (contest, candidate, ballot line), summing votes and OR-ing `is_winner`. This isn't only about file size: the vote figure on each ballot line is what orders a fusion candidate's lines, so leaving the rows split would order them by a single county's total. The scraper logs the collapse ratio (`N source rows → M candidate-ballot-line rows`) so the granularity is visible per run.

Ballot-question rows share the export with contests — they carry `question_text` and no `candidate_name`, and are skipped.

Open States uses the nightly CC0 bulk CSV, not the v3 REST API, deliberately: v3 requires a per-user API key, and making party enrichment depend on a credential would make the NY pipeline non-reproducible for anyone who hasn't registered one.

### Fusion voting

New York lets several parties nominate the same candidate, each with its own ballot line. `party` is therefore **multi-valued**, pipe-delimited, ordered by votes on that line descending:

```
DEMOCRAT|WORKING FAMILIES
REPUBLICAN|CONSERVATIVE|INDEPENDENCE
```

Take the substring before the first `|` for a single value; count the separators to detect cross-endorsement. Each line is canonicalised through `src/aliases/parties.csv` first, so spelling drift between years doesn't produce two labels for one party.

### Matching

Strict only — no nickname expansion, no soundex, no edit distance, no single-token surname matching. A match needs **name + canonical office** to agree plus **either district or election year**; a *contradicting* district disqualifies outright. Two provenance columns record the result:

| `match_confidence` | Meaning |
|---|---|
| `exact` | name + office + district + year all agree (or, for statewide offices, name + office + year — there is no district) |
| `high` | name + office agree and one of district/year agrees, the other absent from one side |

`party_source` is `nysboe_results`, `openstates`, or `nysboe_results+openstates`. Where the two sources name different parties **for the same seat**, the row is left blank and counted, rather than resolved by preferring a source. Open States is keyed on (name, chamber, district), not name alone — a name-only key would let one Eric A. Ulrich corroborate or veto another.

Name matching normalises on two keys: all tokens, and first+last. The second is a relaxation of the *middle name only* (the registry's "Mercedes Vazquez Simmons" vs the results database's "Mercedes Vazquez-Simmons"); both ends must still be identical, and office plus district/year must still agree independently.

### Coverage ceiling — read this before judging the fill rate

NYSBOE certifies statewide, congressional, state-legislative and judicial contests. **Town, village, city-council and school-board races are certified by the 62 county boards and are not in the results database at all.** Those local offices are the bulk of the filer registry:

Counts below are live `$group=office_desc` totals over `compliance_type_desc = CANDIDATE` in `7x2g-h32p`, 2026-07.

| Office group | Candidate filers | Share | In results DB? |
|---|---|---|---|
| Member of Assembly | 4,218 | 11.6% | yes |
| State Senator | 1,794 | 4.9% | yes |
| Statewide (Governor 152, Lt. Governor 84, Comptroller 84, AG 67) | 387 | 1.1% | yes |
| Supreme Court Justice | 1,852 | 5.1% | yes (elected by judicial district) |
| Other named judgeships (County 379, Family 347, City 255, Civil 241+107, District 199, Surrogate 113) | 1,641 | 4.5% | partial |
| Local / party positions (Council, Town, Village, Clerk, Highway Superintendent, County/State Committee, District Leader, …) | remainder | ~73% | no |

Two caveats on that table. The bare `Justice` (967) and `Town Justice` (1,098) office values are excluded from the judicial rows — `Town Justice` is a town office certified locally, and `Justice` is ambiguous between the two, so neither is counted as reachable. And US Senate / US House don't appear at all: federal candidates file with the FEC, not NYSBOE, so they're absent from this registry even though the results database carries their contests.

So party fill on the 36,486-row candidates table is capped structurally well below 100%, no matter how good the matching is — roughly 27% of rows hold an office the results database could even contain, and the realised fill will be lower still because not every filer in a covered office actually appeared on a certified ballot. The parser logs the split explicitly (`Enrichment scope: N of M candidates hold an office the results database covers`) so a low fill rate reads as a source ceiling rather than a broken matcher.

### Other fields the overlay fills

- **`incumbent`** — `1` if the person won the most recent prior election for the same seat, `0` if they contested it and lost. **Blank when unknowable**, which is most of the table. Writing `0` for every unmatched candidate would make the column look fully populated while asserting something the data doesn't support.
- **`district` / `election_year`** — backfilled from the matched candidacy **only where NYSBOE left them blank**. A published value always wins over an inferred one.
- **Committee → candidate linkage** — `link_committees()` now has a second acceptance test: if the name extracted from a committee name isn't a registered candidate filer but *is* someone the results database has on a ballot, the link is still made. Many NY committees are authorized for a candidate who never registered a candidate `filer_id` of their own. This can't create a wrong `person_id` — `assign_committee_person_ids()` only assigns one when `candidate_name` matches a row in `candidates`, so a results-only link yields a named committee with a NULL `person_id`, strictly more than the blank it replaces. The two link types are counted separately in the run log.

### Running it

```bash
python src/pipeline/scrapers/new_york_party.py                 # both sources
python src/pipeline/scrapers/new_york_party.py --openstates    # just Open States (fast)
python src/pipeline/scrapers/new_york_party.py --force-walk --start-id 4000 --end-id 6000
python src/pipeline/parsers/new_york.py                        # picks the overlay up automatically
```

Writes `data/New York/raw/ElectionStats_Contests.csv` and `OpenStates_People.csv`, and records both in `data/New York/manifest.csv` under relation types `electionstats` / `openstates`.

---

## Data Notes

- **NYSBOE publishes no party affiliation** in any of these four datasets. Not a parsing gap; the column doesn't exist upstream (same structural gap as New Hampshire). `party` and `incumbent` are filled instead by the external overlay described below, to the extent the overlay's sources reach.
- **`employer` / `occupation` are near-0%.** NY collects them only on independent-expenditure contributor rows (`ie_cntrbr_emp` / `ie_cntrbr_occ`); no other schedule has the fields at the source. The `treas_occupation` / `treas_employer` columns describe the *treasurer*, not the contributor, and are deliberately not mapped onto them.
- **`cntrbn_type_desc` is populated on ~99.7% of schedule D rows and essentially nowhere else** (18,259,672 of 18.36M rows are blank) — that's expected, it's the in-kind flavour field.
- **`filing_id` is a transaction number, not a report number.** NY exposes no report-level identifier; the closest thing to a "filing" is the `(filer_id, election_year, filing_abbrev)` triple, which isn't a single column.
- **`county_desc` / `municipality_desc_subdivision`** drive `jurisdiction`, falling back to `filer_type_desc` (`State` / `County`) so statewide filers aren't blank.
- **Schedule letters outside A–U** are counted as `skipped_unknown`, logged with a warning naming the letter, and not written — so a schedule NYSBOE adds later surfaces in the run log instead of silently vanishing.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-24 |
| Parser | 2026-07-24 |
| Party enrichment overlay (scraper + matcher) | 2026-07-25 |
| Docs | 2026-07-24 |
