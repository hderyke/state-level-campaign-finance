# Vermont — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Vermont (VT) |
| **Source** | [Campaign Finance System of Vermont](https://campaignfinance.vermont.gov) (Vermont Secretary of State) |
| **Access method** | Unauthenticated JSON API at `api.campaignfinance.vermont.gov`, plain HTTP POST returning CSV. No Playwright, no cookies, no TLS-fingerprint block |
| **Coverage** | 2014 – present. Bulk export covers closed filing years (2014–2025 at the 2026-08-12 snapshot); the open year comes from the browse grid |
| **Volume** | 412,611 contributions, 95,004 expenditures, 1,217 loans, 3,030 candidates, 3,941 committees (parsed 2026-08-12) |
| **person_id model** | `committee` — Filing Entity Id is present on every feed. Decided at parse time; falls back to `name_hash` if a future export drops the column |

Vermont runs the same vendor platform as **Idaho** and **New Hampshire** —
identical `DownloadPublicGridData` / `GetExportPublicDownloadData` controller
pair, identical `publicGridName` + search-filter body shape. Only the
controller prefix differs per deployment (VT `PublicFilerDetails`, ID
`ExportData`, NH `PublicGridDownload`). If you're picking this state up, read
`scrapers/idaho.py` and `scrapers/new_hampshire.py` first; most of the shape
will already be familiar.

---

## Raw Data Structure

Three request shapes, all POST with a JSON body, all returning CSV text.

### 1. Entity rosters

```
POST /api/PublicFilerDetails/DownloadPublicGridData
{"publicGridName": "CandidatePublicGrid",
 "candidateCommitteeSearchFilter": {…, "filerTypeCode": "CAN", "accountStatus": "FACT"},
 "fileName": "Candidates", "type": "CSV", "openInNewTab": false}
```

and the same with `CommitteePublicGrid` / `"filerTypeCode": "COM"`. One flat
snapshot each, no year parameter. `pageSize` is in the body but is ignored when
`type` is `CSV` — the export returns every matching row, not a page.

→ `candidates.csv`, `committees.csv`

### 2. Bulk transactions, by filing year

```
POST /api/ExportData/GetExportPublicDownloadData
{"transactionTypeCode": "TCON", "type": "CSV", "filingYear": "2014", "openInNewTab": false}
```

This is what the [Download Data page](https://campaignfinance.vermont.gov/public/cf/downloads)
calls. `TCON` = contributions and loans, `TEXP` = expenditures.

→ `contributions_{YYYY}.csv`, `expenditures_{YYYY}.csv`

### 3. Browse-grid transactions

```
POST /api/PublicFilerDetails/DownloadPublicGridData
{"publicGridName": "ContributionsPublicGrid",
 "transactionDetailsSearchFilter": {…, "fromDate": null, "toDate": null,
     "transactionAmountMin": null, "transactionAmountMax": null,
     "transactionTypeCode": "TCON", "electionID": "50"},
 "fileName": "Contributions", "type": "CSV", "openInNewTab": false}
```

and the same with `ExpendituresPublicGrid` / `TEXP` / `Expenditures`. This is
the "Download Contribution Data" button on `/public/cf/contribution`.

→ `grid_contributions_{from}_{to}[_amt{lo}-{hi}].csv`, `grid_expenditures_…`

### Why both bulk and grid

**The Download Data page only publishes closed filing years.** At the
2026-08-12 snapshot it listed 2014 through 2025 with no 2026 row, while the
browse pages were already serving 2026 transactions. Relying on the bulk export
alone would silently leave the pipeline a full year behind — the current
election cycle, which is the year most likely to be asked about.

So the scraper treats bulk as preferred and grid as fallback: each year in
scope is tried against the bulk export first, and only a year that comes back
empty or errored is pulled from the grid. **A year is never taken from both** —
when a year later graduates to the bulk export, its grid chunk files are
deleted from `raw/` and its grid rows dropped from the manifest before the bulk
file is written.

### Confirmed columns

Both bulk feeds are documented by the state's own **Download Data Key** PDFs
(linked from the `vpn_key` icons on the Download Data page), and every column
below was additionally verified against real downloaded files on 2026-08-12.

| | bulk (26–27 cols) | grid (19–23 cols) |
|---|---|---|
| filer id | `Filing Entity Id` | `Entity Id` / `Filing Entity Id` |
| filer name | `Filing Entity Name` | *(absent)* |
| campaign | `Committee Name` | `Committee Name` |
| candidate | *(in Filing Entity Name)* | `Candidate First/Last/Middle Name` |
| type | `Registration Type` | `Filer Type` |
| txn id | `Transaction Id` | *(absent)* |
| txn type | `Transaction Type` + `Transaction Subtype` | `Contribution`/`Expenditure Type` |
| counterparty | `Contributor`/`Payee Last`/`First Name` | `Contributor`/`Payee Name` |
| address | `… Address City/State/Zip Code` | `City`/`State Code`/`Zip Code` |
| date/amount | `Transaction Date`, `Transaction Amount` | same |
| election | `Election Year` (+ `Election Type`) | `Election Cycle` |
| purpose | `Purpose` | `Expenditure Purpose` |

Three mismatches between the feeds caused real bugs on the first full run and
are each handled by a named helper in the parser — see
[Parser](#parser).

---

## Scraper

`src/pipeline/scrapers/vermont.py`

### The 50,000-row cap

Browse-grid downloads are capped at 50,000 rows, and **the cap is enforced by
refusal, not truncation**: ask for more and the site returns an error instead of
a file. That is the good failure mode — there is no such thing here as a short
CSV that looks complete, unlike Wisconsin, where the same class of limit
silently truncates.

The scraper turns the refusal into a control signal, and works **top-down**:
ask for the whole year, and split only what the server refuses.

```
whole year → halves → … → single day → amount bands (transactionAmountMin/Max)
```

Starting from the year rather than pre-splitting into months matters more than
it sounds. Vermont's entire 2026 grid year is ~25K contributions and ~7K
expenditures, both far under the cap, so **each relation arrives in one
request instead of twelve**. Pre-splitting also manufactured work that didn't
need doing: August 2026 contains no transactions at all, so a monthly seed
produced a request that could only ever fail. Asking for the year asks for
what exists.

A year that genuinely is over the cap costs one refusal and then splits
normally. The refusal is issued before any data is transferred, so an
over-wide request wastes a round trip and nothing else.

Because the window scheme is now data-driven, a re-pull of a year first clears
that year's existing grid chunks — otherwise files written under an older
scheme (the monthly seed this replaced) would still be on disk and still
globbed by the parser, leaving the year present twice in two shapes.

Windows are disjoint by construction, so chunks concatenate without
deduplication. A row-count check against the cap is kept as a secondary guard
in case the site ever switches from refusing to truncating. If a single day in
a single amount band is *still* refused, that slice is not downloaded and is
logged as a `file_download_error` — a visible, recorded gap rather than a
quietly short table.

### The `fromDate` / `toDate` probe

The captured request has both date bounds as `null`, so no example of a
populated value exists, and the encoding matters enormously: a date filter the
server ignores would make every window request identical.

`_probe_date_format()` resolves it empirically. For each candidate encoding in
`DATE_FORMATS`, it walks a narrowing ladder — month, week, then three single
days in different months — and accepts an encoding only when a bounded request
returns rows and **every returned row's transaction date falls inside the
window**. A refusal at one width just means "try narrower"; only an encoding
refused at every width down to a single day is judged ignored, since single days
spread across the year cannot all legitimately exceed the cap.

**Resolved on the first live run to `%Y-%m-%dT00:00:00.000Z`**, verified
against a March 2026 window. The winner is cached to
`data/Vermont/grid_probe.json`. Delete that file to re-probe. If no encoding works, the scraper **raises** with instructions rather
than downloading anything — the failure mode it is protecting against is silent
duplication, which is much worse than a stopped run.

### `electionID`

The captured body pins `electionID` to `"50"`, whichever cycle the UI had
selected. Sending that verbatim would scope every windowed pull to one election
cycle, so the scraper sends `""` instead — the no-filter value every other
string key in the same filter object uses. This is the one deliberate deviation
from the captured request, and the live run confirms the empty string is
accepted. Override with `VT_ELECTION_ID` if that ever changes.

### TLS on a corporate network

`requests` validates certificates against certifi's bundle, which contains
public CAs only. Behind a TLS-intercepting proxy (Zscaler, Netskope — most
managed laptops) the certificate actually presented is the proxy's, signed by a
corporate root that lives in the OS trust store and nowhere else, so every
request fails:

```
SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED]
certificate verify failed: unable to get local issuer certificate'))
```

**No dependency is needed to fix this.** On Windows, `_windows_root_pems()`
reads the OS certificate store through stdlib `ssl.enum_certificates()` — the
same store the OS and every browser trust, and where a corporate proxy's root
is installed — and filters it to certificates valid for TLS server auth.

**Every CA source is merged, not ranked.** `_build_trust_bundle()` concatenates
certifi, the Windows trust store, and whatever `VT_CA_BUNDLE`,
`SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE` or `CURL_CA_BUNDLE` point at, dedupes on
the certificate body, and writes the result to `data/Vermont/os_ca_bundle.crt`
(gitignored, rebuilt every run).

That union replaced an earlier priority ordering, which had a nasty failure
mode: **Anaconda sets `SSL_CERT_FILE`** to its own public-CA-only bundle, that
outranked the Windows trust store, and so the one source that actually
contained the corporate root was never consulted — the same machine that had
worked minutes earlier failed with `CERTIFICATE_VERIFY_FAILED`. Any "pick the
highest-priority source" scheme has that bug, because these variables are
routinely set ambiently by a Python distribution rather than deliberately by
the user.

**Strict X.509 checks (Python 3.13+).** A second, separate failure looks
similar but has nothing to do with which roots are trusted:

```
CERTIFICATE_VERIFY_FAILED: Basic Constraints of CA cert not marked critical
```

That is OpenSSL's `X509_V_FLAG_X509_STRICT`, which additionally requires CA
certificates to get their structural RFC 5280 extensions exactly right.
Corporate and appliance-generated roots frequently fail it while being
perfectly valid trust anchors, and OpenSSL ignored it for years — but
**Python 3.13 turned it on by default** in `ssl.create_default_context()`. So
the same machine, same proxy, same bundle verifies fine on 3.12 and fails on
3.13. Anaconda shipping a 3.13 interpreter is the usual way to meet this.

The scraper recovers automatically: `_preflight()` classifies the failure with
`_is_strict_only_failure()` and, if it is one of the strict-only defects,
rebuilds the session with `_RelaxedStrictAdapter` and retries once.
`VT_RELAX_X509_STRICT=1` forces it without the failed first attempt.

**This is not `VT_INSECURE`.** The chain, expiry and hostname are still fully
verified; the only thing dropped is the structural extension check. A test
builds a CA with exactly this defect, serves a real TLS handshake from it, and
asserts both that the adapter completes it *and* that an untrusted chain is
still rejected.

The only true override is `VT_INSECURE=1`, which skips verification entirely.
It warns loudly and is never the default.

| | |
|---|---|
| `VT_INSECURE=1` | Skip verification. Last resort — removes protection against a real MITM |
| `VT_RELAX_X509_STRICT=1` | Drop only OpenSSL's strict RFC 5280 extension checks. Applied automatically on a strict-only failure |
| `VT_CA_BUNDLE=<path>` | Merged in. A path that doesn't exist is a hard error, since it was set deliberately |
| `SSL_CERT_FILE`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE` | Merged in. A stale one warns and is skipped rather than failing the run |
| Windows trust store | Merged in automatically. Zero install, zero configuration |
| certifi | Always the base layer |

`truststore` is **not used by default and is not a dependency.** An earlier
version called `truststore.inject_into_ssl()`, which replaces `ssl.SSLContext`
process-wide; that recursed without bound against urllib3's own context
handling, failing every request with `maximum recursion depth exceeded`. Global
ssl patching is gone. Set `VT_USE_TRUSTSTORE=1` to opt back in via
`_TruststoreAdapter`, scoped to this session's pool manager only.

### Failure containment

Two rules keep an environmental problem from turning into hundreds of
misleading log lines:

**Only `EXPECTED_HTTP_ERRORS` may demote a year to the grid.** That tuple is
`(requests.RequestException, BadResponse)` — the server answered, and this year
or window genuinely isn't there. Anything else propagates and stops the run.

`BadResponse` exists specifically because `RecursionError` is a subclass of
`RuntimeError`: an earlier version listed `RuntimeError` as expected, so a
broken ssl stack was read as "2014 isn't published", demoted to the grid, and
then fired a full 20-window probe ladder — per year, for thirteen years.
Roughly 260 doomed requests hiding one root cause.

**A preflight request runs before the year loop.** One cheap GET of the public
site exercises DNS, TLS and the proxy path. It catches `RecursionError`
explicitly, since a monkeypatched ssl stack surfaces that way rather than as a
`requests` exception, and converts it into a single actionable error.

**A transport failure aborts the run.** `TransportError` is deliberately
distinct from an HTTP-level failure: an HTTP error against one filing year
means "this year isn't published, try the grid", but an unreachable host means
nothing about that year. Without the split, one TLS problem would demote all
thirteen years of both relations to a grid fallback that was about to fail the
same way — twenty-six confusing errors instead of one actionable one. The probe
short-circuits on it too, aborting after a single request rather than walking
the full twenty-window ladder.

### HTTP 500 means "no rows"

Vermont answers a date window that matches **nothing** with an HTTP 500 rather
than an empty CSV. Confirmed against the site: the `2026-08-01 → 08-12` window
that 500s here shows no contributions at all in the browse UI.

This is worth handling explicitly, because the obvious response to a 5xx —
split the window and retry — is exactly wrong for an empty one. Every
sub-window of an empty window is also empty and also 500s, so the scraper
recursed `08-01→08-12`, `08-01→08-06`, `08-01→08-03`, `08-01→08-02`… all the
way to single days, for a range with no data in it.

But "every 500 means empty" would be reckless on its own: if the site also 500s
when a window is too *large*, that reading would silently discard real data. So
`_five_hundred_means_empty()` separates the two with a **positive control** —
one unfiltered request, which necessarily matches the whole table (440K+
contributions, far over the 50,000 cap):

| control result | conclusion |
|---|---|
| `GridLimitExceeded` | Over-limit has its own distinct signal, so a 500 is *not* the over-limit case → treat as empty. Also proves the API is up. |
| a CSV | The relation is under the cap and the API is healthy → a 500 can't be about size either → treat as empty. |
| another 500 | Over-limit and empty are indistinguishable here, or the API is down → **refuse to guess**, keep splitting, report real errors. |

One request per relation per run, memoized. An empty window is recorded in the
manifest with `source = grid:empty` and `row_count = 0` and no file — so a later
run can tell "checked, nothing there" apart from "never fetched".

The same check guards the amount-band path, where most bands on any given day
are legitimately empty.

**Can an empty window be predicted instead of discovered?** Partly, and the
top-down windowing above is most of the answer — an empty month inside a year
that fits under the cap is never requested separately, so there is nothing to
predict. What remains is a *wholly* empty window that the splitter does reach,
which still costs one request to discover.

Eliminating that too would need a **count** before the download. The browse
page clearly has one (it renders "Contributions (439,895)"), so the grid's
paged JSON search endpoint — the call that fills the on-screen table, as
opposed to the CSV export used here — almost certainly returns a total. With
it the strategy would stop guessing altogether: ask for the count of a window,
then fetch it whole if it is under the cap, split if over, and skip if zero.
That request has not been captured, so it is not implemented; capture the XHR
behind the contributions table with a date filter applied if this is worth
pursuing.

**Retries are scoped to failures that can actually be transient.** `RETRY_WAITS`
is `(5, 20, 60)` — 85 seconds of backoff before a request is given up on. That
is right for a rate limit or an overloaded backend, and wrong for the two
failures Vermont produces deterministically:

| failure | before | now |
|---|---|---|
| empty grid window (500) | 4 requests + 85s, then the empty check | 1 request, then the empty check |
| unpublished filing year (404) | 4 requests + 85s | 1 request |

A 404 for a year the Download Data page doesn't publish will still be a 404 in
85 seconds, so 4xx (other than 429) now raises immediately. Grid window
requests pass `retry_5xx=False` so an empty window reaches
`_five_hundred_means_empty()` without the wait — and if that check says a 500
is *not* empty on this deployment, the window is retried with the full backoff
budget, since then it might genuinely be transient.

Together those two accounted for about **5.7 minutes** of a ~6.6 minute run
(85s × 2 relations × 2 failure types).

### Flags### Flags

Standard vertical and horizontal scope. `--start-year` / `--end-year` /
`--force` bound the transaction years; the rosters carry no year parameter and
are refreshed on every run.

```bash
python3 src/pipeline/scrapers/vermont.py                        # incremental
python3 src/pipeline/scrapers/vermont.py --start-year 2024      # refresh 2024+
python3 src/pipeline/scrapers/vermont.py --contributions        # TCON only
python3 src/pipeline/scrapers/vermont.py --entities             # rosters only
```

### Runtime

A full 2014-present backfill is roughly 30 bulk requests plus however many
windows the open year needs (typically 12–30, more if a filing deadline pushes
a month over the cap). Incremental runs re-fetch only the current year plus the
two rosters.

---

## Parser

`src/pipeline/parsers/vermont.py`

### Three feed mismatches, three helpers

Every column is resolved by *logical* name through alias tables, matched
case-insensitively with separator normalization, so one parser reads both
feeds. Three differences between them each produced a real bug on the first
full run, and each now has a named helper and a regression note:

**`_filer_identity()` — where the filer's name lives.** `Committee Name` is
blank on *every* candidate row in both feeds. Bulk carries the person in
`Filing Entity Name` ("BROCK, RANDY"); the grid has no such column and carries
`Candidate First/Middle/Last Name` instead. A single merged alias list resolved
to the blank `Committee Name` on the grid feed, so all 4,910 June-2026
candidate rows got a blank `committee_name` — a required field.

**`_txn_type()` — coarse vs. real type.** `Transaction Type` is the literal
string "Contribution"/"Expenditure" on essentially every row; the real
distinction (Monetary, In-Kind (Non-Money), **Loan Received**) is in
`Transaction Subtype`. Reading the coarse column produced `loans_debts.csv.gz`
with **zero rows** despite 147 "Loan Received" rows in 2024 alone. Subtype
wins, Type is the fallback.

**`_row_identity()` — what may be deduplicated.** See below.

### Deduplication

Both feeds ship exact duplicate rows, and both are deduplicated — on different
keys, because one key for both destroys data:

| feed | key | why |
|---|---|---|
| bulk | `Transaction Id` | The id repeats within a file (~7% of rows). Every repeated id was checked: the copies are byte-for-byte identical across all 26 columns, so they are genuine duplicate rows in Vermont's export and collapsing them is correct. |
| grid | the entire raw row | There is no id column. Whole-row identity removes only exact duplicates (6,406 of them, also real) and can never merge two distinct transactions. |

An earlier version fell back to `(committee, contributor, date, amount, type)`
when no id resolved. That is nowhere near unique in this data — 210 different
candidates each received a $5.00 sub-threshold contribution on 2026-02-06, and
all 210 collapsed into one row. Combined with the blank `committee_name` above,
it silently discarded roughly 11,400 real 2026 transactions, about 46% of that
year. Whole-row identity is the fix.

Cross-source double-counting is prevented upstream instead: the scraper deletes
a year's grid chunks once its bulk file lands, so a year is only ever present
from one source.

### Dates

The feeds disagree, and the bulk one carries a clock:

```
bulk   6/9/2024 12:00:00 AM     unpadded, midnight suffix
grid   06/30/2026               zero-padded, date only
```

The time is always midnight and carries no information, so it's split off
before parsing. Missing this blanked **96.7% of all dates** on the first run —
`date` is a required field, so this was the single most damaging bug.

### person_id model

`committee` grouping: `Filing Entity Id` is present on every feed, and this
vendor's id is per-registration rather than per-person (confirmed empirically
in Idaho), so `min(id)` per `(name, office, district)` is what merges a
candidate's cycles. Falls back to `name_hash` if a future export drops the id
column. `state_filer_id` is always populated — real id where there is one, a
stable MD5-derived surrogate otherwise — because Vermont is `has_filer_id=1`
in `states.csv` and `validate.py` requires it non-null.

100% of candidates and all 3,030 candidate committees resolve a `person_id`.

### Roster coverage and backfill

Both rosters are pulled with `accountStatus="FACT"` (active filers), which is
what the state's own search page sends. They are a **current snapshot, not a
historical one** — a committee that deregistered in 2018 will not appear.

Those filers are backfilled from the transaction files instead: name, filer
type and election year only. `office`, `district`, `party`, `treasurer_name`,
`city` and `zip` stay blank, because a transaction row genuinely doesn't carry
them. Inventing values would corrupt the `person_id` grouping key. Same
roster-plus-backfill split New Hampshire and Idaho use.

### Name handling

Vermont publishes people as `LAST, FIRST MIDDLE`, and the surname may itself be
multi-word (`COPELAND HANZAS, SARAH LOUISE`). The comma is the authoritative
split, so candidate names are flipped to `FIRST MIDDLE LAST` — that makes
`candidate_first` / `candidate_last` meaningful and lets committees join to
candidates on the same string.

**Contributor and payee names are deliberately not flipped.** That column mixes
people (`MAHNKE, ERHARD`) with organizations (`META PLATFORMS, INC`), and
nothing in the row reliably says which, so flipping on the comma would corrupt
the organizations. Those fields don't participate in joins.

### Loan routing

Vermont folds loans into the same two exports as ordinary money. The parser
routes them out to `loans_debts.csv.gz` so the two money tables stay comparable
with other states:

| Source type | Destination |
|---|---|
| `Loan Received`, `Loan Forgiven` (contribution type) | `loans_debts` |
| `Loan Payment` (expenditure type) | `loans_debts` |
| everything else | `contributions` / `expenditures` |

### Deduplication

Contributions and expenditures are deduplicated on the source's transaction id,
falling back to a composite of committee / counterparty / date / amount / type
when no id column resolves. Bulk rows are parsed first and win on collision.

This is purely defensive — the scraper already guarantees a year comes from one
source only — but it costs one in-memory set and makes an interrupted scrape or
a hand-copied raw file incapable of double-counting.

---

## Data Notes

**Party and district come from outside the CF system entirely.** Vermont's
campaign finance system records neither — the candidate roster has 35 columns
and not one is party or district, and the search page exposes neither as a
filter nor as a column. (`politicalPartyCode` does appear in the search
request payload, but that's vendor boilerplate: Idaho's identical filter has
the same key and *does* populate it. Vermont leaves it empty.) This is
structural, not an oversight — Vermont has no voter party registration at all,
and party is a ballot-line fact held by the Elections Division. Both fields are
joined in; see [Party and district enrichment](#party-and-district-enrichment).

**2026 loans are invisible, and that's the source.** The browse page renders a
per-row "Contribution Type" of "Monetary Contribution" / "Loan Received", but
its **CSV export flattens that column to the literal "Contribution" on every
row**. There is no subtype anywhere in the grid feed, so a 2026 loan can't be
told from a 2026 contribution until the year closes and its bulk file — which
does carry `Transaction Subtype` — is published. All 1,217 rows in
`loans_debts.csv.gz` come from bulk years.

**Sub-threshold contributions are redacted, not missing.** Contributions under
the itemization threshold publish the contributor as the literal string
`Under Threshold - Name Withheld`. These are real transactions with real
amounts and are kept; `contributor_name` is normalized to empty and
`contributor_type` left blank rather than storing the placeholder as a name.
Expect ~69% of contribution rows to carry no contributor type — that's the
source, not a parsing gap.

**Filing year is not transaction year.** The bulk files are named by *filing*
year, and each contains transactions dated across the whole cycle: the 2016
file holds 2015 activity too. Odd-year files are small (1–4K rows) and
even-year files large (60–133K), but by transaction date the years even out.
Don't read the per-file row counts as annual totals.

**Vermont registers a candidate and their campaign as one entity.** There is no
separate committee registration for a candidate, which is why `committee_type`
is "Candidate" for 3,030 of 3,941 committees and why `candidate_name` and
`committee_name` are usually the same string.

**Minor source dirt, passed through as-is.** ~0.1% of `contributor_state` and
0.4% of `payee_state` values aren't US codes ("FLORIDA", "ILE DE FRANCE",
"CANADA"); ~0.3% of ZIPs are malformed ("0290", "050 H"). A handful of rows
(5 in 2024) have shifted columns, leaving a timestamp in `Transaction Type`.
These surface as tier-2 validator warnings and are left alone — they're
faithful to what the state published. 349 contribution rows (0.1%) have no
parseable date as a result.

---

## Party and district enrichment

Two sources, tried in order of authority. Whichever resolves first wins, and
`party_source` records which one, exactly as `parsers/texas.py` and
`parsers/new_york.py` do.

| | source | scope |
|---|---|---|
| 1 | **VT Elections Database** (`elections_archive_{lo}_{hi}.csv`) | Real ballot records, 2016–2024 even years. Federal / statewide / legislative / county. |
| 2 | **Open States** (`OpenStates_People.csv`) | Currently-serving legislators only (~180), no history. Covers the open-year incumbent the archive can't reach. |

Measured against the real roster:

| | filled | source split |
|---|---|---|
| party | **60.5%** of all 3,030 candidates | 1,745 archive + 89 Open States |
| district | **53.2%** | |
| State Representative | **80.6%** | |
| State Senator | **77.9%** | |
| Governor | 61.0% | |

`match_confidence` splits 1,124 `exact` / 710 `high`.

**The archive has no current-cycle data.** It holds *completed* elections only
— 2016, 2018, 2020, 2022, 2024 — so the 2026 batch downloads a header and no
rows, and will stay empty until after the November 2026 general. This is the
single biggest reason Open States earns its place: for a sitting legislator
running again, it is the only source that knows their party today.

**One file per year-batch, deduped on `(candidate_id, election_id)`.** Batches
are read in filename order and a record is claimed by whichever file gets there
first, so a batch that overlaps an earlier one legitimately reports "0 new".
The parser distinguishes that in the log from a genuinely empty file, because
as a bare row count the two are identical and the first is normal.

Once every batch in the requested range has downloaded successfully, the
scraper removes the older single-file `elections_archive.csv` it supersedes
(~95 MB). Verified as a strict superset before this was enabled: batches alone
yield 4,624 records against the legacy file's 3,795. It is only removed when
*all* batches succeeded, so a partial run never destroys the one good copy.

**Why it stops there** — the ceiling is the sources, not the matching:

- The archive holds even years 2016–2024 only. No 2014, no odd-year town
  meetings, and **no 2026** — the current cycle is exactly what it can't cover.
- It carries federal/state/county races only. Vermont's 607 **local**
  candidates (City Councilor, Selectperson, School Director) are never in it.
- It only has people who appeared on a ballot. A filer who registered a
  committee and withdrew has campaign finance but no election record.

**Matching rules.** `match_confidence` is `exact` (full normalized name) or
`high` (first+last, unambiguous). Two guards keep a small state's name
collisions from becoming false facts:

- A **local** candidate (`Office Type = OTLOC`) is matched on full name only.
  The first+last fallback is too loose to assert across offices. This removed
  28 matches. Local candidates still match when the same person genuinely also
  ran for the legislature, which is common in a state this size.
- **District is only filled when the offices agree.** A Barre city councillor
  who also ran for the House legitimately matches on party, but his council
  seat is not "Washington 3". Without this rule 232 rows got a legislative
  district attached to a local office.
- A set of candidate records that disagree on party with nothing to break the
  tie is rejected rather than guessed at.

**Fusion voting.** Vermont runs it, and the archive spells it inconsistently —
the same Democratic+Progressive pairing appears as `Progressive/Democratic`
(57×), `Dem/Prog` (36×) and `Democratic/Progressive` (17×). Each side is split
on `/`, canonicalized through `src/aliases/parties.csv`, sorted, and rejoined
pipe-delimited, so all three collapse to `DEMOCRAT|PROGRESSIVE`. The
pipe-delimited form is New York's existing convention; the **sort** is the
Vermont-specific part, because unlike NY the source order here is a clerk's
choice rather than ballot order.

**Downloading it.** The response is town-level — one row per candidate per
municipality — so a 2014–2026 request is ~200 MB. Three things make that
survivable:

- **Streamed to disk**, not buffered. The first version called `resp.text`,
  which held the whole body in memory (twice: raw bytes plus the decoded
  string) before writing anything. The download showed no progress and no disk
  activity until it was entirely finished, which behind a scanning corporate
  proxy is indistinguishable from a hang — and an interrupted run left a
  115 MB orphaned `.part`. Peak heap for a 28 MB download went from 78 MB to
  3.3 MB when this was fixed.
- **Batched into 4-year spans** (`ARCHIVE_BATCH_YEARS`), so 2014–2026 is four
  requests of a few tens of MB rather than one of 200 MB.
- **Resumable at batch granularity.** A completed batch is recorded in the
  manifest and skipped next run; only the batch containing the current year is
  re-fetched, since it is the only one that can gain rows. Orphaned `.part`
  files are removed at the start of each run.

Row counting also reads incrementally (`_count_rows_path`) — counting by
`read_text()` would have pulled the file straight back into memory and undone
the streaming.

**Source host.** The archive endpoint is a plain `GET` of
`/api/download_search.csv?search={json}` — no auth, no POST. The scraper tries
`electionarchive.vermont.gov` first and falls back to the vendor backend
`vt2.elstats.civera.com` **with a loud warning**, recording which host answered
in the manifest's `source` column. That fallback is a third-party domain
serving an official `.gov` site's data — the same situation that got
`id.electionstats.com` removed from the Idaho scraper. Check the manifest after
a run and review against the `.gov`-only source policy.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-12 |
| Parser | 2026-08-12 |
| Documentation | 2026-08-12 |
