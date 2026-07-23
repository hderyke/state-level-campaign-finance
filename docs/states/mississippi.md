# Mississippi — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Mississippi (MS) |
| **Source** | [MS Secretary of State Campaign Finance Portal](https://cfportal.sos.ms.gov/online/portal/cf/page/cf-search/Portal.aspx) |
| **Access method** | JSON POST to an ASP.NET AJAX (.asmx) ScriptService — four endpoints; three return full history in a single call, the fourth (office data) is a 12-call sweep |
| **Coverage** | Contributions/expenditures: 2001 – present (despite the portal's own UI claiming pre-9/30/2016 filings are PDF-only and unsearchable — see Data Notes) |
| **person_id model** | `committee` — `EntityId` is a GUID assigned per entity registration, not a person-level ID; `person_id` is derived by grouping candidates on normalized name (office/district are blank for MS, so grouping is effectively name-only) |

---

## Raw Data Structure

Four files land in `data/Mississippi/raw/`, each a plain `{"Table": [...]}` JSON object (the ASMX `{"d": "<json string>"}` transport envelope is unwrapped by the scraper before writing):

### `entities.json`
Source: `CandidateNameSearch` with all filters blank. ~3,300 rows.

| Field | Description |
|---|---|
| `EntityId` | GUID, unique per entity registration |
| `EntityName` | Candidate, committee, or PAC name (a handful of PAC rows have this blank) |
| `OrganizationType` | One of `Candidate`, `Candidate Committee`, `Political Committee (PAC)`, `Policical Initiative Committee` (typo in the source data itself) |

No office, party, district, or address fields are included. That metadata exists on a separate server-rendered HTML detail page per entity (`ViewXSLTFileByName.aspx?providerName=CF_CandidateDetails&EntityId=...`) which is **not** scraped — a ~3,300-page sweep wasn't worth the cost for a first pass (see Data Notes).

### `contributions.json`
Source: `ContributionSearch` with all filters blank. ~95,800 rows.

| Field | Description |
|---|---|
| `Recipient` | Candidate/committee name **receiving** the money |
| `Contributor` | Donor name |
| `ContributorType` | Free-text, e.g. "Individual", "Corporation", "LimitedLiabilityCompany" — 80+ distinct raw values, many one-off typos (see Data Notes) |
| `AddressLine1` / `City` / `StateCode` / `PostalCode` | Donor address |
| `InKind` | Dollar amount if the contribution was in-kind, else blank |
| `Occupation` | Donor occupation, often blank |
| `Date` | `M/D/YYYY 12:00:00 AM` — always midnight, only the date portion carries information |
| `Amount` | `"$1,000.00"`-style string |
| `FilingId` | GUID identifying the filing this row came from |
| `ReferenceNumber` | Human-readable filing reference (e.g. `CF20172339`) — not carried into cleaned output; `FilingId` is used as `filing_id` instead |
| `FilingDesc` | Free-text filing description, sometimes containing "Amended" — the only signal available for the `amended` flag |

### `expenditures.json`
Source: `ExpenditureSearch` with all filters blank. ~25,200 rows.

| Field | Description |
|---|---|
| `Filer` | Candidate/committee name **making** the expenditure |
| `Recipient` | Payee name (opposite role from `Recipient` in contributions.json — see scraper/parser docstrings) |
| `AddressLine1` / `City` / `StateCode` / `PostalCode` | Payee address |
| `Description` | Free-text purpose |
| `Date` / `Amount` / `FilingId` / `ReferenceNumber` / `FilingDesc` | Same shape as contributions |

No field distinguishes expenditure type (no in-kind flag, no category code).

### `districts.json`
Source: 12 calls to `DistrictSearch`, one per (DistrictType, DistrictName) pair — 8 Statewide offices (Governor, LieutenantGovernor, SecretaryOfState, AttorneyGeneral, Auditor, Treasurer, CommissionerOfAgriculture, CommissionerOfInsurance) + 4 Judicial offices (SupremeCourt, CourtOfAppeals, CircuitCourt, ChanceryCourt). ~450 rows total.

| Field | Description |
|---|---|
| `EntityId` / `EntityName` / `OrganizationType` | Same shape as `entities.json` |
| `ElectionYear` | Sparse, sometimes blank |
| `DistrictType` / `DistrictName` | **Not returned by the API** — stamped on by the scraper based on which office it queried with. See scraper docstring: the endpoint's response has no office field at all; the office is entirely implied by the request. |

Legislative (House/Senate) and StateDistrict (Public Service/Transportation Commissioner, District Attorney) offices are deliberately **not** swept — `DistrictSearch` has no per-seat granularity, so querying "House" alone pools all ~122 districts together, which is useless (worse than useless — actively misleading) for disambiguating same-surname candidates in different districts.

---

## Scraper

`src/pipeline/scrapers/mississippi.py`

Three POST calls, one per relation (`entities`, `contributions`, `expenditures`), each with every filter field blank — the API happily returns its entire table in one response rather than requiring pagination or a name/date filter. Plus a 12-call `districts.json` sweep (see above), which rides along whenever `entities` is in scope — 15 calls total per full run. There is no manifest-driven skip logic: every relation in scope is re-fetched on every run, since the source exposes no last-modified signal and the largest file (~50 MB) is cheap to refresh each time.

**Limitations:**
- **WAF blocks non-browser traffic.** A plain `requests` call gets a 403 "Access Denied" — confirmed from both a hosted/sandbox IP and a contributor's own residential IP, so it's not IP-reputation based. It's a fingerprint check (TLS handshake / header shape) that `requests`/urllib3 can't pass but a real browser can. The scraper uses Playwright (`p.chromium.launch(headless=False)`, `page.evaluate()` running an in-page `fetch()`) rather than `requests` — same category of workaround as Alaska's WAF elsewhere in this repo. Requires `playwright install chromium` once per machine. `headless=False` opens a visible browser window during the scrape.
- `--start-year`/`--end-year` are accepted but not implemented (no year-scoped request exists on this source).
- `--candidates`/`--committees` are accepted but treated the same as `--entities` (the source returns all entity types in one call regardless).

**Expected runtime:** ~40s total for all four relations (confirmed on a real run: 40.2s, 4/4 files ok) — browser launch + page load dominate; the individual fetches themselves are fast, including the 12 `districts.json` sub-calls.

---

## Parser

`src/pipeline/parsers/mississippi.py`

**Output tables:** `candidates.csv`, `committees.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv` (written empty — no loan/debt data source was found on the portal)

**Key transformations:**
- `entities.json` is split on `OrganizationType`: `Candidate` rows → `candidates.csv`; everything else (`Candidate Committee`, `Political Committee (PAC)`, `Policical Initiative Committee`) → `committees.csv`.
- `transaction_type` for contributions is derived by the parser itself (`"In-Kind"` if `InKind` is a non-zero amount, else `"Monetary"`) — not read from a raw field. Expenditures have no equivalent signal and are left blank.
- `amended` is inferred by checking whether `FilingDesc` contains the word "Amended" (case-insensitive) — there's no per-row amended flag, only this free-text filing description.
- `candidate_name` enrichment on contributions/expenditures/committees.csv goes through `link_candidate_committees()`, not just an exact-name match. See "Candidate ↔ committee linking" below — this was a significant fix, not a cosmetic one.

**Candidate ↔ committee linking (added 2026-07-12):** MS's data model has no shared filer ID between a candidate's own `Candidate` registration and their `Candidate Committee` registration — they're two independent GUIDs with nothing structurally tying them together. Originally this meant `candidate_name` was populated only when a transaction's `Recipient`/`Filer` string exactly matched a `Candidate`-type entity name verbatim — which almost never happens, since money flows through committees, not through candidates directly. The sitting governor (Tate Reeves) was a stark example: his committee "Tate for Governor" ($40M+ raised, MS's single largest committee) had no `candidate_name` at all, so he was nearly invisible in candidate-level rollups despite obviously being the state's top fundraiser.

The fix is a layered, safest-first heuristic (`link_candidate_committees()` in the parser):
1. **Name-token match.** Normalize the committee name and every candidate name into token sets (committee-naming boilerplate, office words, and suffixes stripped). If exactly one candidate shares a token that's unique to them among all ~1,500 candidates ("distinctive"), link. Resolves roughly 40% of MS's ~655 candidate committees outright.
2. **Office tiebreak.** MS is small enough that common-surname collisions are common — e.g. "Tate for Governor" name-matches both "J. Tate Reeves" and "Jeff Tate" on the token "tate" alone, so step 1 deliberately leaves it ambiguous rather than guess. When `districts.json` is available, narrow the tied candidates to whichever are registered for the *same office* as the committee. This is what actually resolves the Reeves case: both his committee and his own candidate registration show up under (Statewide, Governor); Jeff Tate doesn't appear under any Statewide/Judicial office at all.
3. **Office-only fallback.** If a committee shares no name token with any candidate (nickname, alternate spelling) but its office has exactly one registered candidate, link on office alone.
4. **Step-1 confirmation requirement.** If we know the committee's office (from step 2's data) but the step-1 name match *isn't* also registered for that office, the match is rejected rather than trusted blindly. Found by testing: "Friends of Phil Bryant" distinctive-matches an unrelated candidate ("Bryant Clark") purely because the real Phil Bryant — a two-term governor — has no separate `Candidate` entity in MS's registry at all, only his committee. Requiring office confirmation (not just absence-of-contradiction) catches this.
5. Otherwise `candidate_name` stays blank — correct behavior for PACs (which fund many candidates), House/Senate-linked committees (office data deliberately not fetched — see scraper docstring, no per-seat granularity), and genuine unresolved ambiguity.

Tested against real MS data (1,503 candidates, 655 candidate committees): pure name-token matching alone resolves ~43% cleanly and leaves ~48% ambiguous; the office tiebreak (steps 2-4) resolves most of the remainder, including the flagship Reeves case, while two token-frequency/confirmation safety nets (documented in the function's docstring) closed two false-positive classes found during testing (a generic-first-name collision, and the Phil-Bryant-style missing-registration collision). It's a heuristic, not a source-confirmed identity match — deliberately conservative: unresolvable ties stay blank rather than risk a wrong link.

Confirmed on the full production run (real 12-office `districts.json`, ~450 rows, not a partial test set): 18.1% of all committees (317 of 1,752 — i.e. most of the ~655 actual candidate committees) now carry a `candidate_name`; `candidate_name` fill rate on contributions rose to 34.5% and on expenditures to 48.0% (both were near-zero exact-match-only before this fix). J. Tate Reeves moved from largely invisible to #1 in "Top 20 Recipient Candidates" at $45,515,586 across 5,235 transactions — 5x the #2 spot. "Top 20 Non-Candidate Committees" no longer has any candidate-linked committees leaking through under the wrong name (spot-checked against the false positives found during development, e.g. "Bryant Clark" no longer appears).

**person_id model:** `committee` — `EntityId` (a GUID) is used as `state_filer_id`, one per campaign/committee registration, per an explicit decision that `state_filer_id` should be "whatever ID is distinct to the campaigns" rather than attempt to model person-level continuity. `office`/`district` are blank for MS candidates (not scraped), so `assign_person_ids` groups purely on normalized candidate name. `assign_committee_person_ids` then follows the `candidate_name` link above to assign a committee the same `person_id` as its candidate.

**Limitations:**
- No office, party, district, or election year on candidates (beyond what leaks in from the Statewide/Judicial `districts.json` sweep, which isn't currently written back to `candidates.csv`) — full per-entity metadata lives behind a server-rendered HTML detail page (`ViewXSLTFileByName.aspx`) that isn't scraped.
- Candidate ↔ committee linking only covers Statewide and Judicial races; House/Senate committees are not office-linked (see above) and fall back to name-token matching alone, or stay unlinked.
- A residual false-link risk remains for the rare case where a real candidate has *no* separate `Candidate` entity in MS's own registry (only a committee) *and* the name-token collision candidate also has no office data to check against (the step-4 safety net only catches this when the colliding candidate's office is knowable).
- `ContributorType` is passed through raw (aliased at aggregate time); only the ~15 most frequent of 80+ distinct raw values are mapped in `src/aliases/contributor_types.csv` — the long tail (mostly single-occurrence typos/garbage, e.g. `"Ms 's alabamaasa"`, a bare first name `"Robert"`) is left unmapped.

**Expected runtime:** ~2.5s (four JSON files, no per-row network calls; confirmed on a real run: 2.4s for 95,800 contributions + 25,183 expenditures + 1,503 candidates + 1,752 committees).

---

## Data Notes

- **Portal disclaimer appears inaccurate.** The site's own UI states that only reports filed electronically since 9/30/2016 are searchable, with everything earlier being PDF-only and requiring manual review. In practice, the blank `ContributionSearch` call returns itemized rows back to 2001. Taken at face value here — no special handling applied, no rows excluded.
- **Messy, PDF-derived free text.** The portal itself warns that data has been "converted from pdfs as searchable data and information may not be fully accurate" and that paper-filed reports are hand-transcribed. This shows up most visibly in `ContributorType`, which has 80+ distinct raw values including obvious transcription errors.
- **Only $200+ contributions/expenditures are itemized** by Mississippi statute — the API (and this pipeline) only ever sees itemized rows; smaller contributions are aggregated on filings and not individually searchable.
- **No entity-to-transaction ID link.** Contributions/expenditures reference `Recipient`/`Filer` by name string only, not by `EntityId` — enrichment beyond exact-name candidate matching would require fuzzy name matching or scraping the detail pages.
- **GUID-based `state_filer_id`.** Unlike most states, Mississippi's `EntityId` is a GUID rather than a numeric filer ID. It's used as-is; `assign_person_ids`' numeric-comparison logic falls back to string comparison, which still produces stable (if not human-readable) `person_id` values.
- **Same real-world event reported multiple times under different classifications.** E.g. Governor Tate Reeves' $3.2M personal-loan-to-contribution conversion on 2019-08-09 appears as three separate rows (`ContributorType` = "Other", "Individual", and "Campaign Committee") each with its own distinct `FilingId` — genuinely separate source filings, not a parser dedup bug. Confirmed by checking `filing_id`/`row_num` provenance directly. Large contributions/expenditures should be treated as "this dollar amount was reported this many times across filings," not assumed to be N independent transactions, when doing aggregate analysis.
- **First-run validation (2026-07-11):** 1,503 candidates, 1,752 committees, 95,800 contributions, 25,183 expenditures. Validator passed — 0 tier-1 failures, 4 tier-2 warnings (small numbers of non-standard `contributor_state`/`payee_state`/`*_zip` values, e.g. military postal codes like "AE" not in the recognized-code list). Spot-check queries show recognizable names (Tate Reeves, Lynn Fitch, Jim Hood, Delbert Hosemann) and plausible amounts; cross-checked the year-by-year contribution total against the contributor-type-breakdown total — both independently summed to ~$155.39M.
- **BIGINT `person_id` overflow bug (found + fixed 2026-07-11, shared `utils.py`).** MS's GUID-based `state_filer_id` (`EntityId`) could produce a 20+ digit number after `_numeric_id()` stripped non-digit characters, silently overflowing DuckDB's BIGINT `person_id` column at `tabulate.py`'s CSV-load step — no error surfaced anywhere in the pipeline. This dropped 1,227 of 1,503 candidates (82%), including the sitting governor, between parse and tabulate. Root-caused by comparing `candidates.csv.gz` row counts (1,503, correct) against the tabulated `.db` (276, before the fix). Fixed by bounding `_numeric_id()`'s output to 12 digits (matching `_make_person_id`'s `zfill(12)` padding) via modulo — safe for every other state, since their numeric filer IDs never approach 12 digits in the first place. This is the kind of bug that's invisible unless you specifically cross-check row counts at every pipeline stage; worth remembering if a future state's candidate/committee counts look implausibly low post-tabulate despite a clean parse.
- **Candidate ↔ committee linking gap (found 2026-07-11, fixed 2026-07-12).** Discovered while investigating why Tate Reeves still looked underweighted in "Top 20 Recipient Candidates" even after the BIGINT fix: his committee "Tate for Governor" had no `candidate_name` populated at all, because MS's `Candidate` and `Candidate Committee` registrations are separate GUIDs with no shared filer ID. See the Parser section above for the fix (`link_candidate_committees()`) — a name-token + office-tiebreak heuristic, not a source-confirmed link.
- **No 2024–2026 data — confirmed source-side gap, not a scraper bug (investigated 2026-07-12).** The "ACTIVITY BY YEAR" spot-check shows contributions/expenditures cut off cleanly after 2023, with a single stray 2025 contribution row and nothing else through mid-2026. This initially looked like the same class of bug found in Maine's scraper the same day (a silently-truncated blank/unfiltered query), so it was investigated the same way: queried the live `ContributionSearch` endpoint directly with an explicit date range. A 2023-scoped query returns 11,990 rows — matching our data exactly, confirming `BeginDate`/`EndDate` genuinely filters rather than being ignored. A 2024–2026-scoped query against the live API *also* returns just 1 row. Since a narrow, explicit date query for that window independently returns the same near-empty result as the blank full-history call, this rules out truncation.

  **Root cause (confirmed via [MS HB1334, 2025 Regular Session](https://legiscan.com/MS/bill/HB1334/2025)):** electronic filing is not yet mandatory in Mississippi — the bill requires it starting **January 1, 2027**. Until then, candidates/committees may file on paper (mail, email, or fax to the Secretary of State), and paper filings have no guarantee of being promptly transcribed into this searchable system, if they end up in it before 2027 at all. The near-total absence of 2024–2026 data reflects the state's own filing infrastructure, not a gap in this pipeline. No fix applied; this window should backfill naturally once MS transitions to mandatory e-filing, and re-scrapes after that date should be checked for a sudden jump in 2024–2026 coverage as backlogged paper filings get digitized.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-07-12 |
| Parser | 2026-07-12 |
