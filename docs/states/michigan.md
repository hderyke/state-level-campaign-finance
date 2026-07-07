# Michigan — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Michigan (MI) |
| **Source** | [Michigan Transparency Network (MiTN) Campaign Finance Reporting System](https://mi-boe.entellitrak.com/etk-mi-boe-prod/) |
| **Access method** | Public REST/JSON API (transactions) + HTMX form search with session (entities) |
| **Coverage** | 1997 – present (contributions and expenditures); 1998 – present (receipts) |
| **person_id model** | `committee` — `cfr_com_id` increments per registration cycle; `person_id` = min ID for a given `(candidate_name, office, district)` |

---

## Raw Data Structure

Files land in `data/Michigan/raw/`.

### Transaction Files — one per year per type

Three file types per year, each delivered as a ZIP containing one tab-delimited `.txt` file:

| Pattern | Content |
|---|---|
| `contribution_{year}.txt` | Contributions received by committees |
| `expenditure_{year}.txt` | Expenditures by committees |
| `receipt_{year}.txt` | Other receipts (in-kind, refunds, transfers) |

#### contribution_{year}.txt

| Field | Description |
|---|---|
| `doc_seq_no` | Document sequence number |
| `contribution_id` | Unique contribution ID |
| `cont_detail_id` | Detail line ID |
| `doc_stmnt_year` | Filing statement year — used as `election_year` |
| `doc_type_desc` | Document type description |
| `com_legal_name` | Committee legal name |
| `common_name_acronym` | Committee acronym |
| `cfr_com_id` | Committee filer ID (zero-padded 7 digits) — used as `state_filer_id` |
| `com_type` | Committee type code |
| `can_first_name` | Candidate first name (inline, for candidate committees) |
| `can_last_name` | Candidate last name (inline) |
| `contribtype` | Contribution payment type (e.g. "Direct Contributions", "In-Kind") — not donor category |
| `contributor_f_name` | Contributor first name (blank for organizations) |
| `contributor_l_name_or_org` | Contributor last name or organization name |
| `contributor_address` | Contributor street address |
| `contributor_city` | Contributor city |
| `contributor_state` | Contributor state |
| `contributor_zip` | Contributor ZIP (may be 9-digit padded, e.g. `489330000`) |
| `contributor_occupation` | Contributor occupation |
| `contributor_employer` | Contributor employer |
| `received_date` | Date contribution received |
| `amount` | Dollar amount |
| `aggregate` | Year-to-date aggregate amount |
| `extra_desc` | Extra description |
| `fundraiser` | Fundraiser name |

#### expenditure_{year}.txt

| Field | Description |
|---|---|
| `doc_seq_no` | Document sequence number |
| `expenditure_type` | Expenditure type label — used as `transaction_type` |
| `gub_elec_type` | Gubernatorial election type |
| `expense_id` | Unique expenditure ID |
| `detail_id` | Detail line ID |
| `doc_stmnt_year` | Filing statement year — used as `election_year` |
| `doc_type_desc` | Document type description |
| `com_legal_name` | Committee legal name |
| `common_name_acronym` | Committee acronym |
| `cfr_com_id` | Committee filer ID — used as `state_filer_id` |
| `com_type` | Committee type code |
| `exp_desc` | Expenditure description |
| `purpose` | Purpose of expenditure |
| `payee_f_name` | Payee first name |
| `payee_l_name_or_org` | Payee last name or organization name |
| `payee_address` | Payee street address |
| `payee_city` | Payee city |
| `payee_state` | Payee state |
| `payee_zip` | Payee ZIP |
| `exp_date` | Expenditure date |
| `amount` | Dollar amount |
| `state_loc` | State location flag |
| `supp_opp` | Support/oppose flag (for independent expenditures) |
| `candidate` | Candidate name (Independent Expenditure rows only) |
| `office_dist` | Office and district as combined string (Independent Expenditure rows only) |

#### receipt_{year}.txt

Same schema as `contribution_{year}.txt` with `payer_*` substituted for `contributor_*`, plus one additional column:

| Field | Description |
|---|---|
| `receipttype` | Receipt type (e.g. "Goods/Services Purchased", "Refund/Rebate") |

### Entity Registry

| File | Description |
|---|---|
| `entities_index.csv` | Intermediate Pass 1 output: one row per committee from search pagination |
| `entities.checkpoint` | Set of internal IDs already fetched in Pass 2 (for resumability) |
| `entities.csv` | Final Pass 2 output: all entity data including candidate info |

#### entities.csv

| Field | Description |
|---|---|
| `internal_id` | Internal entellitrak ID (differs from `committee_id`) — used for detail page fetch |
| `committee_id` | Displayed committee ID (= `cfr_com_id` in transaction files, without zero-padding) |
| `committee_type` | Committee type label (e.g. "Candidate", "Political", "Ballot Question") |
| `committee_name` | Full committee name |
| `committee_status` | Registration status (e.g. "Active", "Non-Registered Committee") |
| `candidate_last` | Candidate last name |
| `candidate_first` | Candidate first name |
| `candidate_middle` | Candidate middle name or initial |
| `county` | County of residence |
| `party` | Party affiliation |
| `office_sought` | Office sought |
| `office_sought_district` | District |
| `date_formed` | Date committee was formed |
| `scraped_at` | Date the detail page was fetched |

---

## Scraper

`src/pipeline/scrapers/michigan.py` — pure `requests` + `BeautifulSoup`, no Playwright needed.

### Transactions

The MiTN export page is JavaScript-rendered; the scraper bypasses it and hits two public (no-auth) endpoints directly:

1. **File list** — `GET page.request.do?page=gov.mi.boe.component.cfrexport.page.cfrexportresults&pageSize=200&...`  
   Returns JSON with a list of `{transactiontype, year, download: FILE_ID}` entries. File IDs are opaque integers that rotate when MiTN refreshes exports (typically daily) — the list is always fetched fresh and IDs are never cached in the manifest.

2. **File download** — `GET page.request.do?page=gov.mi.boe.component.cfrexport.page.cfrexportfile&id={FILE_ID}`  
   Returns a ZIP containing one tab-delimited `.txt` file.

Manifest is keyed on `(transaction_type, year)`. Current-year files are always re-fetched.

### Entities

Requires a `JSESSIONID` session obtained by GETting the main search page first. Two passes:

**Pass 1 — Search pagination** → `entities_index.csv`  
`POST page.request.do?page=page.miboeCommitteePublicSearch&action=search` with `perPage=100`, `currentPage=N`, all filter fields blank. Retrieves all ~10,700 committees across ~107 pages. Each `<tr aria-rowindex=N>` row carries the internal entellitrak ID in an Alpine.js `x-bind:hx-vals` attribute (differs from the displayed `committee_id`).

**Pass 2 — Detail sweep** → `entities.csv`  
`POST page.request.do?page=page.miboeCommitteePublicSearch&action=showCommitteeDetails` with `parameters={"id": INTERNAL_ID}`. Adds candidate name, party, office, district, county, date formed. Runs at 8 parallel workers; checkpointed to `entities.checkpoint` for resumability.

**Runtimes:** Transactions ~2 min. Entity sweep ~13 min at 8 workers.

---

## Parser

`src/pipeline/parsers/michigan.py`. `id_model = "committee"`.

**Key transformations:**
- `cfr_com_id` → `state_filer_id`. Zero-padded to 7 digits when joining against `entities.csv` (which stores IDs without leading zeros for older committees).
- Contributor type (Individual/Organization) derived from whether `contributor_f_name` is non-blank — Michigan's `contribtype` describes payment method, not donor category.
- `doc_stmnt_year` → `election_year`.
- Candidate name from inline `can_first_name`/`can_last_name` columns; falls back to entity registry for committees where the inline fields are blank.
- **Loan routing** — contributions with `contribtype = "Direct Contributions - Loan"` → `loans_debts.csv.gz`; expenditures with `expenditure_type = "Direct Expenditures - Loan Owed to/Given By"` → `loans_debts.csv.gz`.
- **Receipts** — `receipt_*.txt` rows folded into contributions output. `receipttype = "Refund/Rebate"` rows written as negative-amount expenditures instead (represent money leaving the committee).
- **ZIP cleaning** — 9-digit padded ZIPs (`489330000`) stripped to 5-digit (`48933`).
- Entity registry keyed by `committee_id.zfill(7)` to align with `cfr_com_id` format.
- Candidates written from registry rows where `candidate_name` is non-blank. Committees written for all registry entries.

**person_id model:** `committee` — `cfr_com_id` increments per registration; the same candidate gets a new ID each cycle. `assign_person_ids(id_model="committee")` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(cfr_com_id)` prefixed with Michigan's FIPS code (26).

**Limitations:**
- `treasurer_name`, `city`, `zip` always blank on committee rows — not available from the MiTN committee search
- `election_year` is the filing statement year (`doc_stmnt_year`), not a transaction-level election cycle date
- `office` and `candidate_name` on expenditure rows: only populated for Independent Expenditure rows (where the `candidate` column in the source names the supported/opposed candidate); for all other expenditure types these come from the entity registry via `state_filer_id`

---

## Data Notes

- **Opaque file IDs**: MiTN export ZIPs are identified by opaque numeric IDs that rotate daily. The scraper always fetches the file list fresh rather than caching IDs in the manifest.
- **ZIP code padding**: many ZIP values arrive as 9 raw digits with trailing zeros (`489330000` → `48933`). Fixed in `clean_zip()`.
- **Receipts overlap with contributions**: some receipt file rows (e.g. in-kind contributions with `receipttype=Goods/Services Purchased`) appear to duplicate data also present in contribution files. The distinction is filing context, not a true duplicate — both are retained.
- **`SUPP` expenditure type**: appears in independent expenditure filings for ballot question support/oppose activity. Normalized to `Other` in `expenditure_categories.csv`.
- **`x-bind:hx-vals` vs `hx-vals`**: MiTN migrated committee search rows from a static HTMX attribute to an Alpine.js binding. The scraper handles both; if the site is redesigned again, `_parse_search_page` will need updating.
- **Session expiry during entity sweep**: JSESSIONID sessions typically outlast the ~13-min detail sweep. If a session expires mid-run, re-run with `--entities` — Pass 1 is skipped if `entities_index.csv` already exists, and Pass 2 resumes from `entities.checkpoint`.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-30 |
| Parser | 2026-06-30 |
| Alias mappings | 2026-06-30 |
| Docs | 2026-06-30 |
