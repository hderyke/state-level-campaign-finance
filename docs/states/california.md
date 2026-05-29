# California — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | California (CA) |
| **Source** | [CAL-ACCESS Bulk Export](https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip) — California Secretary of State |
| **Access method** | HTTP Range requests — reads ZIP central directory, extracts only target tables without downloading the full archive |
| **Coverage** | ~2000 – present (CAL-ACCESS history) |

---

## Raw Data Structure

Files live in `data/California/raw/`. All files are **tab-separated (TSV)** with **latin-1 (ISO-8859-1) encoding**. Extracted from a single daily-updated ZIP (~1.5 GB uncompressed) hosted by the CA Secretary of State.

### Transaction Tables

#### RCPT_CD.tsv — Contributions Received (~19M rows)

| Field | Description |
|---|---|
| `FILING_ID` | Links to the filing in CVR |
| `AMEND_ID` | Amendment sequence number — higher = more recent |
| `LINE_ITEM` | Line item number within the filing |
| `REC_TYPE` | Record type (always "RCPT") |
| `FORM_TYPE` | Schedule form (e.g. "A", "C") |
| `TRAN_ID` | Unique transaction ID within the filing |
| `ENTITY_CD` | Contributor entity type code (e.g. "IND", "COM", "OTH") |
| `CTRIB_NAML` / `CTRIB_NAMF` | Contributor name parts |
| `CTRIB_CITY` / `CTRIB_ST` / `CTRIB_ZIP4` | Contributor address |
| `CTRIB_EMP` | Contributor employer |
| `CTRIB_OCC` | Contributor occupation |
| `TRAN_TYPE` | Transaction sub-type |
| `RCPT_DATE` | Receipt date (`M/D/YYYY H:MM:SS AM/PM` in raw) |
| `AMOUNT` | Dollar amount (plain numeric, no `$` or commas) |
| `CMTE_ID` | Contributing committee ID (when contributor is a committee) |
| `CAND_NAML` / `CAND_NAMF` | Candidate name on the filing |
| `OFFICE_CD` / `JURIS_CD` / `DIST_NO` | Office, jurisdiction, district |

#### EXPN_CD.tsv — Expenditures (~15M rows)

| Field | Description |
|---|---|
| `FILING_ID` / `AMEND_ID` / `TRAN_ID` | Filing/amendment identifiers |
| `ENTITY_CD` | Payee entity type code |
| `PAYEE_NAML` / `PAYEE_NAMF` | Payee name parts |
| `PAYEE_CITY` / `PAYEE_ST` / `PAYEE_ZIP4` | Payee address |
| `EXPN_DATE` | Expenditure date — **null for ~35% of older Form-E rows** (see Data Notes) |
| `AMOUNT` | Dollar amount |
| `EXPN_CODE` | Expenditure purpose code (maps to `transaction_type`) |
| `EXPN_DSCR` | Free-text description (maps to `purpose`) |
| `FORM_TYPE` | Schedule form (maps to `category`) |
| `CMTE_ID` | Payee committee ID |
| `CAND_NAML` / `CAND_NAMF` / `OFFICE_CD` | Candidate/office context |

### Registry / Reference Tables

#### FILERNAME_CD.tsv — Filer Registry (~1.3M rows)

| Field | Description |
|---|---|
| `FILER_ID` | Primary filer ID |
| `XREF_FILER_ID` | Cross-reference ID for merged/renamed filers — used as fallback join key |
| `FILER_TYPE` | Text description of committee type |
| `STATUS` | "ACTIVE" or "INACTIVE" |
| `NAML` / `NAMF` | Committee/filer name parts |
| `CITY` / `ZIP4` | Address |

#### CVR_CAMPAIGN_DISCLOSURE_CD.tsv — Cover Records (~689K rows)

One row per filing/amendment — authoritative source for election year, candidate name, office, and reporting period dates.

| Field | Description |
|---|---|
| `FILING_ID` / `AMEND_ID` | Filing and amendment identifiers |
| `FILER_ID` | Filing committee ID |
| `ELECT_DATE` | Election date (used to derive `election_year`) |
| `THRU_DATE` | End of reporting period — used as date fallback for expenditures with null `EXPN_DATE` |
| `CAND_NAML` / `CAND_NAMF` | Candidate name on the cover page |
| `OFFICE_CD` / `JURIS_CD` / `DIST_NO` | Office, jurisdiction, district |
| `CMTTE_TYPE` | Committee type code |

#### FILER_TO_FILER_TYPE_CD.tsv — Filer Type Mapping (~694K rows)

Used only for party affiliation (`PARTY_CD`) and active status (`ACTIVE`). The `FILER_TYPE` numeric code references an unavailable session table and is not interpreted.

---

## Scraper

`src/pipeline/scrapers/california.py`

CAL-ACCESS publishes a single ZIP archive (~1.5 GB) updated daily. Rather than downloading the full file, the scraper uses **HTTP Range requests** to:

1. HEAD the ZIP URL to get total file size and `Last-Modified` date
2. Fetch the last 64 KB to locate and parse the ZIP's central directory (handles ZIP32 and ZIP64)
3. For each target table, fetch only the compressed bytes for that entry, decompress with `zlib` in streaming 8 MB chunks, and write to disk

The manifest stores the server's `Last-Modified` date per file — re-runs skip files that haven't changed. `--update-transactions` re-pulls `RCPT`, `EXPN`; `--update-entities` re-pulls `FILERNAME`, `CVR`, `FILER_TO_FILER_TYPE`.

**Limitations:**
- Relies on the ZIP's `Last-Modified` header being updated reliably when data changes
- Custom streaming decompression (no zipfile library) — edge cases in unusual ZIP64 layouts could cause failures

**Expected runtime:** ~10–30 min depending on network speed.

---

## Parser

`src/pipeline/parsers/california.py`

Uses DuckDB for all heavy file I/O — multi-GB TSVs are processed in seconds rather than minutes. Runs in three stages to avoid timeouts; stages can be run independently with `--stage N`.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv.gz`, `expenditures.csv.gz`

**Key transformations:**

**Amendment dedup** — CVR is pre-loaded with `MAX(AMEND_ID) OVER (PARTITION BY FILING_ID)`. Joining transaction tables on `(FILING_ID, AMEND_ID = max_amid)` retains only the most-recent amendment of each filing.

**Committee name resolution** — `FILERNAME_CD` is indexed two ways: by `FILER_ID` (primary) and by `XREF_FILER_ID` (for renamed/merged filers). Transaction rows join on `CVR.FILER_ID → filername.FILER_ID` first, falling back to `filername_xref.xref_id`. Before this fix, ~45% of contribution rows and ~24% of expenditure rows had blank `committee_name` because renamed filers (e.g. filer 742855 → 1018725) only appeared as `XREF_FILER_ID` in `FILERNAME_CD`.

**Expenditure date fallback** — `EXPN_DATE` is null for ~35% of older Form-E rows in the raw data. When null, the parser falls back to `CVR.THRU_DATE` (end of the reporting period). This gives a bounding date rather than an exact date — noted in the data notes.

**Encoding** — All TSVs read as latin-1; output written as UTF-8.

**Person IDs** — `id_model="committee"`: the same candidate gets a new `FILER_ID` each cycle, so `assign_person_ids` groups by `(state, candidate_name, office, district)` and assigns `person_id = min(state_filer_id)`. `assign_committee_person_ids` links committee rows to candidate `person_id` via name matching.

**Limitations:**
- Committee treasurer name not populated (CVR has it but it's not joined to committees)
- `amended` column is blank — dedup approach means only the current amendment survives
- `election_year` on contributions is sourced from `CVR.ELECT_DATE`, not the contribution date — blank for filings without an election date
- Expenditure dates falling back to `THRU_DATE` cluster on reporting period end dates rather than actual transaction dates (visible in year distribution)

**Expected runtime:** ~45–90 min total (stages 2 and 3 each scan multi-GB TSVs).

---

## Data Notes

- **Renamed filers** — ~70K historical filer IDs appear only as `XREF_FILER_ID` in `FILERNAME_CD` (the filer changed its ID at some point). Without the XREF fallback join, these show blank `committee_name`. High-volume examples: CDA PAC (742855 → 1018725) accounted for ~750K contribution rows alone.
- **Expenditure date gaps** — ~35% of Form-E expenditure rows have null `EXPN_DATE` in the raw data, concentrated in filings from ~2000–2010. Dates for these rows are filled from `CVR.THRU_DATE` and should be treated as approximate period boundaries, not exact transaction dates.
- **No loans/debts** — `LOAN_CD` and `DEBT_CD` are downloaded but not parsed. Not a priority.
- **NUL bytes in TSVs** — CAL-ACCESS TSVs occasionally contain `\x00` characters. DuckDB's `ignore_errors=true` option handles these gracefully.
- **latin-1 encoding** — not UTF-8. Misidentifying the encoding produces garbled special characters in names and addresses.
- **FILER_TO_FILER_TYPE_CD numeric codes** — `FILER_TYPE` references a session-specific lookup table not included in the bulk export. Party and active status are extracted but the numeric type code itself is unusable.
- **Duplicate contributor names** — some large donors appear under slightly different name formats across filings (e.g. "DaVita, Inc." vs "DaVita") with no deduplication in the parser. This is a source data issue.
- **Scale** — RCPT_CD and EXPN_CD together are ~34M rows before amendment filtering.

---

## Status

- [x] Scraper complete
- [x] Parser complete
- [x] Loaded into DB
- [x] Validated (tier 1 passing)
- [x] QA'd via test queries

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-29 |
| Parser | 2026-05-29 |
| Docs | 2026-05-29 |
