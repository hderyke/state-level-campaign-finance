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
| `CTRIB_NAML` / `CTRIB_NAMF` / `CTRIB_NAMT` / `CTRIB_NAMS` | Contributor name parts |
| `CTRIB_CITY` / `CTRIB_ST` / `CTRIB_ZIP4` | Contributor address |
| `CTRIB_EMP` | Contributor employer |
| `CTRIB_OCC` | Contributor occupation |
| `CTRIB_SELF` | Self-employment flag |
| `TRAN_TYPE` | Transaction sub-type |
| `RCPT_DATE` | Receipt date (`M/D/YYYY H:MM:SS AM/PM` in raw) |
| `DATE_THRU` | End date for period contributions |
| `AMOUNT` | Dollar amount (plain numeric, no `$` or commas) |
| `CUM_YTD` | Cumulative year-to-date total |
| `CTRIB_DSCR` | Contribution description |
| `CMTE_ID` | Contributing committee ID (when contributor is a committee) |
| `CAND_NAML` / `CAND_NAMF` | Candidate name on the filing |
| `OFFICE_CD` / `JURIS_CD` / `DIST_NO` | Office, jurisdiction, district |
| `BAL_NAME` / `BAL_NUM` / `BAL_JURIS` | Ballot measure info |
| `SUP_OPP_CD` | Support/oppose code |
| `INTR_NAML` / `INTR_NAMF` / `INTR_CITY` / `INTR_ST` / `INTR_ZIP4` | Intermediary (bundler) info |
| `MEMO_CODE` / `MEMO_REFNO` | Memo/reference fields |
| `INT_RATE` / `INTR_CMTEID` | Loan interest rate and intermediary committee |

#### EXPN_CD.tsv — Expenditures (~15M rows)

| Field | Description |
|---|---|
| `FILING_ID` / `AMEND_ID` / `LINE_ITEM` / `TRAN_ID` | Filing/amendment identifiers |
| `ENTITY_CD` | Payee entity type code |
| `PAYEE_NAML` / `PAYEE_NAMF` / `PAYEE_NAMT` / `PAYEE_NAMS` | Payee name parts |
| `PAYEE_CITY` / `PAYEE_ST` / `PAYEE_ZIP4` | Payee address |
| `EXPN_DATE` | Expenditure date |
| `AMOUNT` | Dollar amount |
| `EXPN_CODE` | Expenditure purpose code (maps to `transaction_type`) |
| `EXPN_DSCR` | Free-text description (maps to `purpose`) |
| `EXPN_CHKNO` | Check number |
| `FORM_TYPE` | Schedule form (maps to `category`) |
| `CMTE_ID` | Payee committee ID |
| `CAND_NAML` / `CAND_NAMF` / `OFFICE_CD` / `JURIS_CD` / `DIST_NO` | Candidate/office context |
| `AGENT_NAML` / `AGENT_NAMF` | Agent name |
| `G_FROM_E_F` / `BAL_NAME` / `SUP_OPP_CD` | Ballot measure and independent expenditure fields |

#### LOAN_CD.tsv — Loans (~96K rows)

| Field | Description |
|---|---|
| `FILING_ID` / `AMEND_ID` / `TRAN_ID` / `LOAN_TYPE` | Filing/loan identifiers |
| `ENTITY_CD` | Lender entity type |
| `LNDR_NAML` / `LNDR_NAMF` | Lender name parts |
| `LOAN_CITY` / `LOAN_ST` / `LOAN_ZIP4` | Lender address |
| `LOAN_DATE1` | Loan origination date |
| `LOAN_DATE2` | Loan due date |
| `LOAN_AMT1` | Original loan amount |
| `LOAN_AMT2` | Outstanding balance |
| `LOAN_AMT3` / `LOAN_AMT4` | Paid this period / forgiven |
| `LOAN_AMT5`–`LOAN_AMT8` | Additional amount fields |
| `LOAN_RATE` | Interest rate |
| `LOAN_EMP` / `LOAN_OCC` / `LOAN_SELF` | Lender employment info |
| `CMTE_ID` | Affiliated committee |
| `INTR_NAML` / `INTR_NAMF` / `INTR_CITY` / `INTR_ST` / `INTR_ZIP4` | Intermediary info |

#### DEBT_CD.tsv — Debts (~715K rows)

| Field | Description |
|---|---|
| `FILING_ID` / `AMEND_ID` / `TRAN_ID` | Filing identifiers |
| `ENTITY_CD` | Creditor entity type |
| `PAYEE_NAML` / `PAYEE_NAMF` / `PAYEE_NAMT` / `PAYEE_NAMS` | Creditor name |
| `PAYEE_CITY` / `PAYEE_ST` / `PAYEE_ZIP4` | Creditor address |
| `BEG_BAL` | Beginning balance |
| `AMT_INCUR` | Amount newly incurred this period |
| `AMT_PAID` | Amount paid this period |
| `END_BAL` | Ending balance |
| `EXPN_CODE` / `EXPN_DSCR` | Expenditure code and description |
| `CMTE_ID` | Affiliated committee |

### Registry / Reference Tables

#### FILERNAME_CD.tsv — Filer Registry (~1.3M rows)

| Field | Description |
|---|---|
| `FILER_ID` | Primary filer ID — joins to `CMTE_ID` / `FILER_ID` in transaction files |
| `XREF_FILER_ID` | Cross-reference ID for merged/renamed filers |
| `FILER_TYPE` | Text description of committee type (e.g. "RECIPIENT COMMITTEE") |
| `STATUS` | "ACTIVE" or "INACTIVE" |
| `EFFECT_DT` | Effective date of this record |
| `NAML` / `NAMF` / `NAMT` / `NAMS` | Committee/filer name parts |
| `ADR1` / `ADR2` / `CITY` / `ST` / `ZIP4` | Address |
| `PHON` / `FAX` / `EMAIL` | Contact info |

#### CVR_CAMPAIGN_DISCLOSURE_CD.tsv — Cover Records (~689K rows)

One row per filing/amendment — the authoritative source for election year, candidate name, office, and treasurer:

| Field | Description |
|---|---|
| `FILING_ID` / `AMEND_ID` | Filing and amendment identifiers |
| `FILER_ID` | Filing committee ID |
| `ENTITY_CD` / `FILER_NAML` | Filer type and name |
| `ELECT_DATE` | Election date (used to derive `election_year`) |
| `CAND_NAML` / `CAND_NAMF` | Candidate name on the cover page |
| `OFFICE_CD` / `JURIS_CD` / `DIST_NO` | Office, jurisdiction, district |
| `CMTTE_TYPE` | Committee type code |
| `TRES_NAML` / `TRES_NAMF` / `TRES_CITY` / `TRES_ST` / `TRES_ZIP4` | Treasurer info |
| `FILER_CITY` / `FILER_ZIP4` | Filer address |
| `FROM_DATE` / `THRU_DATE` / `RPT_DATE` | Reporting period and submission date |
| `STMT_TYPE` | Statement type (e.g. "PE" for pre-election) |

#### FILER_TO_FILER_TYPE_CD.tsv — Filer Type Mapping (~694K rows)

| Field | Description |
|---|---|
| `FILER_ID` | Links to FILERNAME_CD |
| `FILER_TYPE` | Numeric type code (references an unavailable session table — not interpreted) |
| `ACTIVE` | Active flag ("T"/"Y" = active) |
| `PARTY_CD` | Party code — decoded to party name (Democratic, Republican, etc.) |
| `EFFECT_DT` | Effective date — most recent row per filer is used |

---

## Scraper

`src/pipeline/scrapers/california.py`

CAL-ACCESS publishes a single ZIP archive (~1.5 GB) updated daily. Rather than downloading the full file, the scraper uses **HTTP Range requests** to:

1. HEAD the ZIP URL to get total file size and `Last-Modified` date
2. Fetch the last 64 KB to locate and parse the ZIP's central directory (handles both ZIP32 and ZIP64)
3. For each target table, fetch only the compressed bytes for that entry, decompress with `zlib` in streaming 8 MB chunks, and write directly to disk

The manifest stores the server's `Last-Modified` date per file — re-runs skip files that haven't changed on the server. `--update-transactions` re-pulls `RCPT`, `EXPN`, `DEBT`, `LOAN`; `--update-entities` re-pulls `FILERNAME`, `CVR`, `FILER_TO_FILER_TYPE`.

**Limitations:**
- Relies on the ZIP's `Last-Modified` header being updated reliably when data changes
- Streaming decompression is custom (no zipfile library) — edge cases in ZIP64 or unusual compression methods could cause failures
- No authentication required; standard HTTP

**Expected runtime:** ~10–30 min depending on network speed (selectively downloads only compressed table bytes, not the full 1.5 GB ZIP).

---

## Parser

`src/pipeline/parsers/california.py`

California's CAL-ACCESS schema stores amendments as additional rows sharing the same `FILING_ID` with an incrementing `AMEND_ID`. All raw tables must be filtered to the most recent amendment before use.

**Output tables:** `committees.csv`, `candidates.csv`, `contributions.csv`, `expenditures.csv`, `loans_debts.csv` (contains both loans from `LOAN_CD` and debts from `DEBT_CD`)

**Key transformations:**
- **Amendment dedup:** CVR is pre-loaded in two passes to build `{filing_id: max_amend_id}`. Transaction rows whose `AMEND_ID` is less than the maximum for their filing are skipped.
- **Encoding:** All TSVs read as latin-1; output written as UTF-8. NUL bytes (`\x00`) stripped inline — they appear sporadically in some CAL-ACCESS TSVs.
- Amounts already plain numeric strings — no `$` or commas to strip.
- Dates in `M/D/YYYY H:MM:SS AM/PM` format, normalized to `YYYY-MM-DD`.
- Candidate names title-cased on output so "NEWSOM, GAVIN" and "Newsom, Gavin" collapse to the same string across filings.
- Placeholder candidate name values (`N/A`, `NA`, `None`, `-`) treated as blank.
- `FILERNAME_CD` filer registry: when a filer_id appears multiple times, the `ACTIVE` record is preferred; otherwise the last seen record is kept.
- `FILER_TO_FILER_TYPE_CD` numeric `FILER_TYPE` codes reference an unavailable session table — this table is used **only** for party affiliation and active status (most recent `EFFECT_DT` row per filer).
- `DEBT_CD` has no date column — `date` is written as blank for all debt rows.
- Loans (`LOAN_CD`) and debts (`DEBT_CD`) are both written to `loans_debts.csv`, distinguished by `record_type` ("LOAN" or "DEBT").
- Filer ID resolved per transaction row: `CMTE_ID` field if present, else `FILER_ID` from the CVR cover record.

**Limitations:**
- CVR pre-load requires holding ~689K filing records in memory — RAM-intensive but manageable
- Committee treasurer name is not populated in the cleaned output (CVR has treasurer info but it's not joined to the committees table)
- `amended` field is left blank — dedup approach means only the current amendment survives, so there's nothing meaningful to flag

**Expected runtime:** ~45–90 min (19M + 15M row TSVs, amendment filtering pass over CVR adds ~10 min upfront).

---

## Data Quirks

- **No direct `amended` flag** — superseded amendments are dropped during parsing; surviving rows all represent the current state of each filing. The `amended` column is blank in output.
- **NUL bytes in TSVs** — CAL-ACCESS TSVs occasionally contain `\x00` characters that break standard CSV readers. Stripped inline by a wrapper class in the parser.
- **latin-1 encoding** — not UTF-8. Misidentifying the encoding produces garbled special characters in names and addresses.
- **DEBT_CD has no date** — debts owed don't carry a transaction date in CAL-ACCESS. The `date` field for all debt rows is blank.
- **FILER_TO_FILER_TYPE_CD numeric codes unresolvable** — the `FILER_TYPE` column references a session-specific lookup table that isn't included in the bulk export. Party and active status are extracted but committee type classification from this table is not possible.
- **Candidate name normalization** — names are title-cased to collapse formatting inconsistencies across filings (e.g. "SMITH, JOHN" vs. "Smith, John"). This may occasionally mis-capitalize names with unusual casing (e.g. "DeAnza").
- **CMTE_ID vs FILER_ID** — committee transactions use `CMTE_ID` as the filer identifier; individual candidate filings use `FILER_ID` from the CVR cover record. The parser checks `CMTE_ID` first.
- **Scale** — RCPT_CD and EXPN_CD together are ~34M rows before amendment filtering. Parsing is single-threaded and takes significant time.
- **No test report yet** — QA has not been run for California.

---

## Status

- [x] Scraper complete
- [x] Parser complete
- [x] Loaded into DB
- [ ] Verified / QA'd (no test report generated yet)

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-05-14 |
| Parser | 2026-05-13 |
