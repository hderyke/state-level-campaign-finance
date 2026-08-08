# Oregon — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Oregon (OR) |
| **Source** | [ORESTAR — Oregon Secretary of State campaign finance disclosure system](https://secure.sos.state.or.us/orestar) |
| **Access method** | Playwright (real Chromium) — F5 Bot Defense blocks plain HTTP requests |
| **Coverage** | 1990 – present (transactions), 1989 – present (entities registry) |
| **person_id model** | `committee` — new Committee Id per election cycle; `person_id` = min ID for a given `(candidate_name, office, district)` |

---

## Raw Data Structure

Two categories: transaction exports (`or_transactions_*.xls`) and entities registry exports (`or_entities_*.xls`), both real binary OLE2 `.xls` files (not `.xlsx`), read via `xlrd`.

### Transactions — `or_transactions_{from}_{to}[_amt{lo}-{hi}].xls`

One chunk per date window (recursively split on a hard 5,000-row cap — first by month, then by half, down to single days, then by amount band as a last resort). Contributions and expenditures are interleaved in a single feed with no top-level type column — the only routing signal is the free-text `Sub Type` field.

45 columns; the ones the parser actually uses:

| Field | Description |
|---|---|
| `Tran Id` / `Original Id` | Transaction ID / the transaction this one amends (used as `filing_id`) |
| `Tran Date` | Transaction date (MM/DD/YYYY) |
| `Tran Status` | e.g. "Amended" |
| `Filer` / `Filer Id` | Filing committee name/ID — the committee's *own* name, may differ from its entities-registry name (see Data Notes) |
| `Contributor/Payee` | The other party; contributor on the contribution side, payee on the expenditure side |
| `Sub Type` | Free-text transaction type — routes the row to contributions/expenditures/loans |
| `Payer of Personal Expenditure` | For reimbursement rows, the staffer/candidate being reimbursed |
| `Amount` | Native float (not a string, unlike most states' CSV exports) |
| `Contributor/Payee Committee ID` | Populated when the other party is itself a registered committee |
| `Book Type` | Contributor type (Individual, Business Entity, Political Committee, etc.) |
| `Occptn Txt` / `Emp Name` | Occupation / employer |
| `Purp Desc` / `Purpose Codes` | Expenditure purpose |
| `City` / `State` / `Zip` / `Zip Plus Four` | Contributor/payee address |

### Entities — `or_entities_{bucket}[_{year}][_off-{office}].xls`

One file per (filerType bucket × year) combination — every bucket is swept both blank (`yearActive` unset) and once per year 1989–present (see Scraper section for why the blank sweep alone isn't trustworthy). 22 columns:

| Field | Description |
|---|---|
| `Committee Id` / `Committee Name` | Registry ID and name — may differ from the transaction feed's `Filer` name for the same ID |
| `Committee Type` / `Committee SubType` | CC / PAC / CPC, plus a finer sub-category (Measure, Recall, Political Party, Miscellaneous, or a specific initiative/referendum name) not currently folded into `committee_type` |
| `Candidate Office` | Jurisdiction/location text ("statewide", "22nd District", "Gilliam County") → `jurisdiction` |
| `Candidate Office Group` | Office title ("Governor", "State Representative") → `office` |
| `Filing Date` / `Organization Filing Date` | Most recent filing / original registration date |
| `Treasurer First/Last Name` / `Treasurer Mailing Address` | Treasurer info — address is a single space-joined string with no delimiters at all |
| `Candidate First/Last Name` | Blank (whitespace) for non-candidate committees |
| `Candidate Maling Address` [sic] | Candidate's own address, same undelimited format |
| `Active Election` | e.g. "2006 General Election" — source of `election_year` |
| `Measure` | Ballot measure reference, not currently mapped to a schema field |

No status/active-flag column exists anywhere in the export.

---

## Scraper

`src/pipeline/scrapers/oregon.py`

**Access:** ORESTAR runs F5 Bot Defense (`TSPD_101` cookies) — a plain `requests` session gets real data for a modest burst, then degrades to a JS-challenge stub and eventually an outright WAF block. Confirmed from both a cloud sandbox and a residential connection, so this is a fingerprint check, not IP-reputation. The scraper drives a real headed Chromium via Playwright instead (`headless=False` — bot-defense products are more likely to fingerprint headless Chromium), mirroring Florida's existing WAF workaround.

**Transactions:** `cneSearch.do` (GET) returns a results page whose "N records found" text gives the true match count for free before paying for the (much larger) export request. Export happens via clicking "Export To Excel Format" (`XcelCNESearch`) — located by link text, not href, since OWASP CSRFGuard's JS rewrites every href on page load. Hard-capped at 5,000 rows; capped windows are recursively split (month → half → day → amount band) without ever downloading the oversized export.

**Entities:** `GotoSearchByElection.do` → `CommitteeSearchSecondPage.do` (POST form), same check-count-before-export pattern, capped at 999. Split axis is `filerType` (11 dropdown values) plus two special checkbox categories (Slate Mailer Organizations, Independent Expenditure Filers — the latter's name is a misnomer, see Data Notes), swept both blank and per-year. **The blank sweep alone is not a valid completeness signal** — an earlier version of this scraper only year-split a bucket when its blank count hit the 999 cap, but a real run proved that assumption wrong: CANDALL/CPCALL/PACALL all came back under-cap with `yearActive` unset, yet the resulting registry had essentially zero committees with 2013+ activity, missing well-known real 2022 candidates entirely. Every bucket is now swept per-year (1989–present) unconditionally in addition to the blank sweep; Committee Id dedup at parse time absorbs the resulting overlap.

**Expected runtime:** Transactions, full history (1990–present): several hours. Entities, full history: tens of minutes (mostly search requests, not exports — only 5 of 13 buckets ever return real rows).

---

## Parser

`src/pipeline/parsers/oregon.py`

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv`

**Key transformations:**
- `route()` classifies each transaction row from `Sub Type` alone via marker substrings + a small exact-match table. Three Sub Types are deliberately left unrouted rather than guessed: `Refunds and Rebates` (direction unclear), `Cash Balance Adjustment` (no real counterparty), `Nonpartisan Activity` (too rare, undocumented).
- `load_entity_registry()` reads every `or_entities_*.xls` file and dedupes by Committee Id, keeping whichever snapshot has the *latest* `election_year` (not first-seen) — first-seen would pin a still-active committee to a stale cycle, since the blank-sweep file is processed before per-year files.
- `committee_name`/`candidate_name`/`office`/`election_year` on every contributions/expenditures/loans_debts row come from the entities registry (looked up by Filer Id), **not** the transaction feed's own `Filer` text — required because `committee_name` (not `state_filer_id`) is the join key `queries.py` and the aggregate DB use, and the two feeds sometimes disagree on a committee's name for the same ID (see Data Notes).
- Transaction-derived data is a fallback "orphans" pass for any Filer Id that shows up in money but not in the registry — same pattern Wisconsin's parser uses.
- ZIP is extracted via a trailing-digits regex off the single space-joined address blob; city is deliberately left blank rather than guessed (no reliable way to separate a multi-word city from a multi-word street with no delimiter).

**person_id model:** `committee` — confirmed against real data, not assumed. The same candidate running in multiple cycles gets a new Committee Id each time (e.g. Shane Bemis for Mayor: ID 8465 in 2006, ID 14710 in 2010), so `state_filer_id` isn't a stable per-person key and must be grouped by `(name, office, district)`.

**Expected runtime:** ~10s for full history (2M+ contribution rows, 900K+ expenditure rows).

---

## Data Notes

- **Committee name mismatches between feeds.** A small number of Filer Ids (dozens, confirmed and logged at parse time) file transactions under a different name than their entities-registry record shows — e.g. Filer Id 3591 files every transaction as "Run Betsy Run" but is registered as "Betsy PAC". This is a real ORESTAR data inconsistency between its two export feeds, not a scraping artifact (checked thousands of that filer's raw rows). The parser always uses the registry name for `committee_name`.
- **"IE_FILERS" bucket is a misnomer.** The scraper's "Independent Expenditure Filers" checkbox sweep returns ordinary county party central committees (Marion County Democratic Central Committee, Yamhill County Republican Central Committee, etc.) doing routine activity — not genuine independent-expenditure spenders. Oregon's transaction export carries no independent-expenditure signal at all (no IE Sub Type, no target-candidate or support/oppose column anywhere in the 45-column schema, confirmed against the full corpus) — the new `affiliated_candidate_name`/`support_oppose` columns on `expenditures` are left blank for Oregon.
- **Committee SubType not folded into `committee_type`.** The entities export's `Committee SubType` field (Measure/Recall/Political Party/Miscellaneous, or a one-off initiative/referendum name) would let party committees and ballot-measure PACs be distinguished from generic PACs, but isn't currently folded into the parser's output — see the alias-mapping note in `src/aliases/committee_types.csv`.
- **Orphan rate ~3%.** Filers seen in transactions but absent from every entities bucket (331 of 10,255 committees on the full-history run) get a minimal committees.csv row via the orphans fallback — no type, treasurer, or address. Spot-checked: mostly late-cycle ballot-measure/recall/petition committees (`Legislative Accountability 1`, `Recall X`, `IP N`) plus a handful of ordinary PACs with no obvious pattern.
- **Sparse pre-2007 records.** Electronic filing volume jumps sharply starting 2007 (2006: ~8.6K transactions all year; 2007: ~79.5K); a handful of real records go back to 1990. Not a parsing artifact — matches the entities registry's own year distribution.
- **`loans_debts` not in `tabulate.py`.** Oregon writes a real `loans_debts.csv.gz` (record types: Account Payable, Loan Received/Payment/Forgiven, etc.), but `tabulate.py`'s `TABLES` list doesn't currently include `loans_debts` for any state, so it isn't queryable from the per-state `.db` file. Pipeline-wide gap, not Oregon-specific — flagged here since it affects Oregon's own data visibility.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-08-02 |
| Parser | 2026-08-02 |
| Alias mappings | 2026-08-02 |
