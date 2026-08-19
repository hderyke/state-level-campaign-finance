# New Mexico — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | New Mexico (NM) |
| **Source** | [New Mexico Campaign Finance Information System (CFIS)](https://login.cfis.sos.state.nm.us) |
| **Access method** | Plain HTTPS — bulk CSV download endpoint for transactions, JSON search endpoints for entities. No Playwright, no authentication |
| **Coverage** | 2020 – present (CFIS replaced the legacy `cfis.state.nm.us` system for the 2020 primary; earlier filings are not reachable from this source) |
| **person_id model** | `committee` — Org Id identifies a reporting-entity registration and NM candidates re-register per cycle, so IDs are grouped by `(candidate_name, office, district)` and collapsed to the earliest |
| **Volume** | 1,060,610 contributions · 390,672 expenditures · 2,041 loans · 2,331 candidates · 2,754 committees (2020-01-01 – 2026-07-15) |
| **Runtime** | ~4½ min scrape (35 files, ~460 MB), ~45 s parse |

---

## Raw Data Structure

Two transaction CSVs per year plus three entity JSON files per year.

```
data/New Mexico/raw/
  contributions_{year}.csv    CFIS "Contributions and Loans" export
  expenditures_{year}.csv     CFIS "Expenditures" export
  candidates_{year}.json      /Organization/SearchCandidates response
  committees_{year}.json      /Organization/SearchCommittees response
  offices_{year}.json         /Organization/GetOffices response
```

### Transaction files — positional, not named

This is the single most important thing to know about New Mexico. The SOS
documents these exports as **positional layout keys**, not as a header contract:

- [Contributions and Loans File Layout Key](https://login.cfis.sos.state.nm.us) (positions A–II)
- [Expenditures File Layout Key](https://login.cfis.sos.state.nm.us) (positions A–FF)

Both are linked from the Data Download page. The parser therefore treats
**column position as the contract** and uses header text only to correct
individual indices when a recognizable name is present — see
[Parser](#parser) below.

The files do ship a header row, and its names differ slightly from the layout
key (`OrgID` not `Org Id`; `Report Entity Type` not `Reporting Entity Type`).
Two further quirks confirmed against the real files: every data row is followed
by a **blank line** (the exports use `\r\r\n` terminators), which the parser
skips; and every row has exactly the documented column count, so there is no
ragged-row handling to worry about.

#### contributions_{year}.csv

| Pos | Field | Notes |
|---|---|---|
| A | Org Id | Unique ID of the reporting entity — the join key to everything else |
| B | Transaction Amount | |
| C | Transaction Date | |
| D | Last Name | **Entity full name goes here** when the contributor isn't an individual |
| E–H | First Name, Middle Name, Prefix, Suffix | Contributor/lender name parts |
| I–M | Address 1, Address 2, City, State, Zip | Contributor address (or the venue address for a special event) |
| N | Description | |
| O | Check Number | |
| P | Transaction ID | Unique |
| Q | Filed Date | |
| R | Election | **"General" / "Primary" / "Special" / "Local" — no year in it**, despite the layout key calling it an election description |
| S–U | Report Name, Start of Period, End of Period | |
| V | Contributor Code | `Individual` · `Other (e.g. business entity)` · `Political Committee` · `Candidate/Officeholder` · `Lobbying Organization` · `Candidate` |
| W | Contribution Type | `Contributions - Monetary` · `- In-Kind` · `- Anonymous` · `- Intra-Candidate Transfer` · `- Other` · **`Loans Received`** · `Return Contribution` · `Special Event` |
| X | Reporting Entity Type | `Candidate` · `PAC` · `PAC - Contribution or Coordination` · `PAC - Political Party/Central Committee` · `PAC - Legislative Caucus Committee` · `PAC - Mixed (…)` · `PAC - Independent Expenditure` |
| Y | Committee Name | Recipient PAC's name; blank for candidate filers |
| Z–DD | Candidate Last/First/Middle/Prefix/Suffix | The filing candidate, when the filer is one |
| EE | Amended | Y/N |
| FF–GG | Contributor Employer, Contributor Occupation | |
| HH | Occupation Comment | The real answer when the filer picked "Other" for occupation |
| II | Employment Information Requested | Y/N |

#### expenditures_{year}.csv

| Pos | Field | Notes |
|---|---|---|
| A | Org ID | Paying candidate or committee |
| B–C | Expenditure Amount, Expenditure Date | |
| D | Payee Last Name | **Entity full name goes here** when the payee isn't an individual |
| E–H | Payee First/Middle/Prefix/Suffix | |
| I–M | Payee Address 1/2, City, State, Zip | |
| N | Description | Filer's narrative |
| O | Expenditure ID | Unique |
| P–T | Filed Date, Election, Report Name, Start of Period, End of Period | |
| U | Purpose | CFIS's spending picklist (`Office expenses`, `Campaign consultants`, …) |
| V | Expenditure Type | Nominally the type, but CFIS lets filers put a *purpose* here too. `Monetary Expenditures` and `Contribution (explain nonmonetary)*` cover 93% of rows; `Other` is another 9%; the remaining ~20 values duplicate the Purpose list. **There is no `Independent Expenditure` value** — IE is signalled by Stance |
| W | Reason | **Either a candidate name or a ballot question** |
| X | Stance | `Support` (11.3K) / `Oppose` (277) / blank |
| Y | Reporting Entity Type | Same value set as the contributions file |
| Z | Committee Name | Paying PAC's name |
| AA–EE | Candidate Last/First/Middle/Prefix/Suffix | |
| FF | Amended | Y/N |

> **Note on the published expenditure key:** it labels the Purpose row "Y" while
> listing it between "T End of Period" and "V Expenditure Type" — an obvious
> typo for U — and then uses "Y" again for Reporting Entity Type. The sequence
> U…FF documented above is the one that makes the column count come out right,
> and is what `EXP_POSITIONS` in the parser encodes.

### Entity JSON files

These come from the Explore screens' backing endpoints. They are undocumented,
but the shape below was confirmed against a live 2020–2026 pull (2026-08-06).
All three return a **bare JSON array** of flat records — no envelope. The
scraper persists the response body untouched; the parser resolves fields through
alias lists so a rename degrades enrichment rather than breaking the parse.

| Endpoint | Fields the parser uses |
|---|---|
| `SearchCandidates` | `CandidateName`, `OfficeName`, `District`, `Jurisdiction`, `Party`, `ElectionYear`, `Status` |
| `SearchCommittees` | `CommitteeName`, `CommitteeType`, `CommitteeSubtype`, `ElectionYear`, `Status`, `StateID` |
| `GetOffices` | `OfficeName`, `District`, `Jurisdiction`, `JurisdictionType`, `ElectionYear`, `ElectionName` |

`GetOffices` reuses the candidate DTO wholesale — same 43 keys, with
`CandidateName` null and `NumberofCandidates` filled in.

**There is no public numeric key on these endpoints.** `IDNumber`,
`RegistrationId` and `MemberID` are opaque 44-character tokens
(`OdI2e7QFWuNTBjv2mKLfqn1lK5MpzTLbNw12UnzEQ-I1`) that match nothing in the
transaction exports. `StateID` is the one numeric field and the only one that
lines up with an Org Id — but it's populated on just **105 of 3,103** candidate
records and 1,053 of 1,452 committee records. See [Parser](#entity-construction).

`Status` is `Active`/`Inactive` (the registration status, which is what `active`
means) and is distinct from `CompliantStatus`, which is `Compliant `/`Out of
Compliance` — a filing judgement, not a registration state. `Incumbent`,
`TreasurerName`/`Treasurer` and `CommitteeAddress` exist in the schema but are
empty on every record observed.

Committee registry value sets: `CommitteeType` is `Political Committee` or
`Individual Independent Expenditure Filer`; `CommitteeSubtype` is `Contribution
or Coordination`, `Political Party/Central Committee`, `Independent
Expenditure`, `Mixed (Independent & Contribution/Coordination)`, `Legislative
Caucus Committee`, or a bare `Other` — which is the most common single value
(586 of 1,452), so the parser falls back to the parent type in that case.

**Odd years return `[]` for committees and (mostly) offices.** CFIS scopes
committee registration to even-year election cycles. This is a real property of
the source, not a scrape failure — the parser treats an empty array as a valid
answer and stays quiet about it.

---

## Scraper

`src/pipeline/scrapers/new_mexico.py`

**Transactions.** One GET per year per type against
`/api/DataDownload/GetCSVDownloadReport?year=YYYY&transactionType=CON|EXP&reportFormat=csv&fileName=…`.
`CON` is Contributions and Loans, `EXP` is Expenditures. The `fileName`
parameter only sets `Content-Disposition` — it doesn't affect which rows come
back — but the endpoint 400s without it.

**Entities.** One request per year per relation:

| Relation | Method | Endpoint |
|---|---|---|
| Candidates | POST | `/api///Organization/SearchCandidates` |
| Committees | POST | `/api///Organization/SearchCommittees` |
| Offices | GET | `/api///Organization/GetOffices` |

The tripled slash after `/api` is what the CFIS front-end itself sends and the
server normalizes it. It is reproduced verbatim rather than "fixed" so the
request stays byte-identical to one the site is known to accept. `pageSize` is
int32 max, which is also what the front-end sends — these endpoints return the
whole result set in one shot, so there is no pagination to implement.

**Cookies.** Captured browser requests carry `TS01dc4fc6` (an F5 BIG-IP ASM
session cookie) and `OClmoOot` (a bot-defense token). Neither is an auth
credential and both expire, so nothing is hardcoded — the session issues a GET
to the site root first, which is enough to be handed the cookies the WAF wants
to see on subsequent calls. No Playwright is needed.

**Raw JSON is stored unparsed.** Entity responses go to disk as-is rather than
being flattened to CSV, precisely because the field names are unconfirmed: a
naming surprise is then fixable with a `reparse` instead of a full re-scrape.

**Manifest.** Tracks `(relation_type, year)`. Transaction years are skipped on a
manifest hit except the current year, which is always re-fetched — CFIS rewrites
the in-progress year's export as new reports are filed. **Entity years are
always re-fetched**, since registration data is mutable in a way a closed year's
transaction export isn't (a committee's compliance status or a candidate's
district can change mid-cycle) and each is a single request.

**Flags.** Full standard set. `--force` and year ranges are scoped to the
relations the horizontal flags select, so `--force --expenditures` can't orphan
the contributions entries. Offices ride along with `--candidates` as well as
`--entities` — they're registry context for candidates, and the standard flag
taxonomy has no `--offices`.

**Expected runtime:** ~2–5 min for a full run (14 transaction files + 21 entity
requests across 2020–2026, 0.3–0.5s sleep between requests). Incremental runs
are well under a minute.

---

## Parser

`src/pipeline/parsers/new_mexico.py`

### Column resolution

`CON_POSITIONS` / `EXP_POSITIONS` encode the published layout keys. On each
file the parser:

1. Reads row 1 and decides whether it's a header (a header's first cell is the
   Org Id *label*; a data row's is a number). If it's data, it's parsed, not
   discarded.
2. Starts from the positional map, then overrides individual indices wherever a
   header cell matches that field's alias set.

The two passes together mean a column inserted upstream gets caught by the name
pass, while a reworded header can't break parsing on its own.

### Entity resolution

`_unwrap()` walks the plausible response envelopes (bare list — which is what
CFIS actually returns; `data` / `results` / `items` / `Table` containers; and a
bounded search for the longest list of dicts as a last resort, so an envelope
nobody anticipated still parses). `_pick()` resolves each logical field through
an ordered alias list against a punctuation-insensitive key map, so
`electionYear`, `ElectionYear` and `Election_Year` all land in the same place.

If the JSON yields nothing usable, entities are still built **in full** from the
transaction files, which always carry Org Id, Reporting Entity Type, Committee
Name and the candidate name parts. The JSON only ever *adds* office, district,
party, jurisdiction, registration status and committee subtype on top of that.

### Entity construction

`candidates.csv.gz` and `committees.csv.gz` are keyed on **Org Id harvested from
the transaction files** and enriched from the JSON registry by normalized name.
The name join lands ~92% of transaction-derived candidates.

Registry rows that can't be resolved to a *numeric* ID are used for enrichment
only and not written as their own rows. This matters more than it sounds:
`utils._numeric_id` strips non-digits and takes the remainder mod 10¹², so
writing one of CFIS's opaque tokens into `state_filer_id` would silently mint an
arbitrary `person_id` that could collide with a real filer's.
`_pick_numeric_id()` refuses anything non-numeric for that reason. A blank
`state_filer_id` is also a tier-1 failure for NM (`has_filer_id=1`), so there is
no third option.

The cost is **741 candidates and 87 committees** that registered but filed no
transaction in 2020–2026, and so carry no money data by definition. Both counts
are reported every run via `enrichment_summary` (`registry_dropped_no_id`) —
a sudden jump there would mean the name join has drifted, not that NM held an
unusually quiet cycle.

Names are assembled as `LAST, FIRST MIDDLE SUFFIX`, matching the format CFIS
uses on its own candidate screens — that's what makes the transaction ↔ registry
name join work. Non-person filers put their whole name in the last-name field
and leave first blank, so those pass through without picking up a stray comma.

### Key transformations

- Amounts stripped of `$`/commas; accounting parentheses read as negative
- Dates normalized to `YYYY-MM-DD` from `MM/DD/YYYY`, ISO, or a datetime string; years outside 1990…current+2 discarded
- `Amended` Y/N → `1`/`0`
- `Stance` Support/Oppose → `S`/`O`
- `election_year` comes from the source filename. The Election column carries no year, so the regex fallback fires only on the handful of rows where a filer typed one in — and the result is range-checked, because one 2020 row reads "2080 General"
- `Occupation Comment` replaces a literal "Other" occupation, not just a blank one
- Expenditures: `category` = Purpose (CFIS's picklist), `purpose` = Description (the narrative) — the layout puts the categorical value in the field named "Purpose", which is the opposite of what the canonical column names suggest
- All name fields (`committee_name`, `candidate_name`, `contributor_name`, `payee_name`, `counterparty_name`, `employer`) go through `utils.clean_name` so transaction rows and entity rows join cleanly

### Loans

The Contributions file carries contributions **and** loans received.

The layout key says Contribution Type "will be blank for Loans Received". **It
isn't.** The column is an enumerated list, loans carry an explicit `Loans
Received` value, and there are no blank-type rows anywhere in 2020–2026.
Routing on the documented blank produced an empty loans table; routing on the
value yields the 2,041 loan rows that are actually there. `LOAN_TYPES` in the
parser is the list, and it's the first thing to check if the loans table ever
comes back empty again.

Loan rows go to `loans_debts.csv.gz` with the contributor fields mapped to the
counterparty fields and `record_type` set to the source value.

The Expenditures file likewise contains loan *payments*, but carries no
equivalent flag, so those stay in `expenditures.csv.gz`.

### Independent expenditures

Column W ("Reason") holds either a candidate name or a ballot question, and
column X ("Stance") holds support/oppose. `support_oppose` is written whenever
Stance parses. `affiliated_candidate_name` is only written when Reason matches a
name already in the candidate registry, so ballot questions don't leak into a
candidate field. The match rate is reported via `enrichment_summary` on every
run — currently 11,041 matched / 116 unmatched, so an unexpectedly low number
would mean the name formats have drifted apart.

Note that `transaction_type` is never `Independent Expenditure` — CFIS has no
such Expenditure Type. **Stance is the only IE signal.**

### Limitations

- Rows whose amount won't parse are counted as `skipped` and dropped (11 rows across all 14 files)
- Rows with no committee name, no candidate name, and no prior row for the same Org Id are dropped — never observed in practice; Committee Name is populated on 100% of contribution rows, including candidate filers
- `city` and `zip` are not populated on committees; the search endpoints don't expose them and there's no bulk registration export
- `treasurer_name` and `incumbent` are wired up but empty — the fields exist in the registry schema and are null on every record CFIS returns

---

## Data Notes

- **The layout keys are not fully reliable.** Two of the three things they say
  about the trickiest columns are wrong: Contribution Type is not blank for
  loans, and the Election column is not an election description. The positional
  ordering, by contrast, has held exactly. Treat the position table as the
  contract and verify any *value* claim against the data.
- **Watch the `registry:` line in the parse log.** It currently reads 2,632
  candidates / 540 committees / 1,023 offices. If it reports 0 for a relation,
  CFIS has renamed a field or changed its envelope — the raw JSON on disk shows
  the real names, one edit to `CAND_KEYS` / `CMTE_KEYS` / `_unwrap()` fixes it,
  and a `reparse` picks it up without re-downloading anything.
- **Small-dollar volume is real, not a duplication bug.** One 2026
  gubernatorial committee accounts for 373,090 of the 1.06M contributions at a
  $37 average. Verified: all 1,060,610 rows have a distinct Transaction ID and
  no `(raw_file, row_num)` pair repeats.
- **~0.1% of payee/contributor address fields hold garbage** — a `payee_zip` of
  `NM`, or a whole committee name pasted into the ZIP field. These are filer
  data-entry errors in the source, not column drift: every row in every file has
  exactly the documented column count. They surface as tier-2 warnings and are
  left as-is rather than silently cleaned.
- **Nothing before 2020.** CFIS replaced `cfis.state.nm.us` for the 2020 primary.
  Older NM filings exist only in the decommissioned system, which this source
  doesn't reach. Any historical backfill would be a separate era-two scraper.
- **CFIS is slated for replacement.** The SOS has stated the current system is
  not functioning as intended and is being replaced. Expect the endpoints
  documented here to change at some point; the positional layout keys are the
  more durable half of this integration.
- **Committee type comes from two sources that spell it differently.** The
  transaction exports prefix subtypes with `PAC - `; the registry doesn't.
  `committee_types.csv` maps both forms. Subtype is preferred where it's
  informative, but it's a bare `Other` on 586 of 1,452 registry rows, in which
  case the parser falls back to the parent type.
- **`Other` expenditure type is mapped to canonical `Other`, not blanked.** It's
  34,075 rows and $29.6M — 15% of all NM expenditure dollars. Nulling that out
  would hide it from every category rollup, and calling it `Monetary` would
  assert a form the data doesn't state. The rows do have a payee and a
  description, so the detail survives in `purpose`.
- **`Contribution (explain nonmonetary)*` is the single largest expenditure
  type** (213,000 rows) and maps to `In-Kind`, following Arkansas's treatment of
  the equivalent value.
- **Two new canonical office labels.** `Magistrate Judge` and `Probate Judge`
  have no equivalent in the existing canonical set and aren't the same thing as
  a district or municipal court, so they were added rather than folded into a
  nearby label. `Judge of the Metropolitan Court` (Bernalillo) and `Municipal
  Judge (Los Alamos ONLY)` both map to the existing `Municipal Court Judge`.
- **Lobbyists are out of scope.** CFIS also exposes
  `/api//SearchLobbyist/SearchLobbyist`. The pipeline schema has no lobbyist
  relation, so it isn't scraped.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-06 |
| Parser | 2026-08-06 |
