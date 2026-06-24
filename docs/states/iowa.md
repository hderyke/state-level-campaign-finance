# Iowa — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Iowa (IA) |
| **Source** | [Iowa Ethics and Campaign Disclosure Board — Public Reports](https://webapp.iecdb.iowa.gov/publicReports/state-reports) |
| **Access method** | POST to `api/publicreports/state` returns all filing records as JSON; PDFs served from Azure Blob Storage (no auth required) |
| **Coverage** | ~2000 – present (all DR-2 Summary filings ever submitted to IECDB) |
| **person_id model** | `committee` — Iowa committee codes are per-registration; candidates may receive a new code each election cycle |

---

## Raw Data Structure

Each raw file is a **DR-2 Summary PDF** — Iowa's standard campaign finance disclosure form for committees and candidates. All data (contributions, expenditures, loans, and committee metadata) is in the PDF; there are no separate CSV exports.

### Index API

`POST https://webapp.iecdb.iowa.gov/api/publicreports/state` with an empty JSON body (`{}`) returns all ~17,000+ filing records. Required headers: `Content-Type: application/json`, `Accept: application/json`, `Origin: https://webapp.iecdb.iowa.gov`, `Referer: .../publicReports/state-reports`.

Each record includes:

| Field | Description |
|---|---|
| `id` | Internal filing ID |
| `committeeCode` | Iowa committee code (integer string or alphanumeric, e.g. `"2677"`, `"SWGA51126"`) |
| `committeeName` | Committee name as registered |
| `candidateName` | Candidate name (blank for PACs) |
| `organizationType` | `"Candidate"` or `"Iowa PAC"` |
| `periodYear` | Reporting year |
| `periodDescription` | Human-readable period label (e.g. "2026 May Monthly") |
| `reportType` | Always `"DR2"` in practice |
| `filedOn` | ISO datetime of filing |
| `fileName` | PDF filename (unique — includes a timestamp to distinguish amendments) |
| `fileUrl` | Full Azure Blob Storage URL for the PDF |

### DR-2 PDF Structure

| Page/Section | Content |
|---|---|
| Page 1 | Committee metadata: name, committee type, county, district, committee code, political party, candidate name, status (Filed/Amended), treasurer contact info |
| Sch-A | Cash contributions: date, contributor name+address, relationship, amount |
| Sch-B | Expenditures: date, payee name+address, purpose, amount |
| Sch-D | Unpaid bills (skipped — insufficient detail for loans_debts schema) |
| Sch-E | In-kind contributions: date, contributor name+address, description, value |
| Sch-F1 | Loans received: date, lender name+address, relationship, amount |
| Sch-F2 | Loans paid/forgiven: date, lender, amount |
| Sch-G/H | Consultant breakdown, campaign property (skipped) |

---

## Scraper

`src/pipeline/scrapers/iowa.py`

POSTs to `api/publicreports/state` on every run to get the full index (~7.5 MB, ~17,000+ records). PDFs are then downloaded from Azure Blob Storage (`iecdbblobstorage.blob.core.windows.net/reports-prod/{fileName}`).

**Manifest:** keyed by `filename` (unique per PDF due to timestamp in name). Current-year PDFs are always re-checked (amended filings get new filenames). Old amendment files are orphaned on disk but never re-parsed.

**Flags:** `--start-year`, `--end-year`, `--force`. Horizontal scope flags (`--transactions`, `--entities`, etc.) are accepted but silently ignored — Iowa doesn't separate data by type.

**Polite delay:** 0.05s between PDF downloads.

**Expected runtime:** Initial full download ~6–10 hours (17,000+ PDFs, ~3 GB total at avg ~175 KB each). Incremental runs fetch only new/current-year filings.

---

## Parser

`src/pipeline/parsers/iowa.py`

Reads the manifest for API metadata (committee name, period year, organization type, candidate name), then parses each PDF with `pdfplumber`.

**Schedule detection:** Each PDF page may contain multiple schedule tables. The schedule type is determined from the header row's first cell (`classify_table()`):

| First-cell value | Schedule | Output |
|---|---|---|
| `"contribution date"` | Sch-A | contributions (Monetary) |
| `"expenditure date"` | Sch-B | expenditures |
| `"date"` + col-1 contains "name & address" | Sch-E | contributions (In-Kind) |
| `"date incurred"` | Sch-F1 | loans_debts (loan_received) |
| `"date loan paid / forgiven"` | Sch-F2 | loans_debts (loan_repaid) |

**Header extraction:** Page 1 table data (not text) is used for committee metadata. The two-column PDF layout causes text extraction to bleed right-column date values into left-column metadata fields — table extraction avoids this.

**Name+address parsing:** Contributor/payee name and address are combined in a single multi-line cell. Long organization names wrap across multiple lines within the cell. The parser identifies the street line as the first line starting with a digit (or "P O Box"), treats everything before it as the name, and the last line as city/state/zip.

**Committee deduplication:** Only the first filing seen per `committeeCode` is written to `committees.csv` and `candidates.csv`. Subsequent filings update transaction rows only.

**person_id model:** `committee` — `assign_person_ids` groups by `(state, candidate_name, office, district)` to assign stable cross-cycle IDs. `assign_committee_person_ids` joins committees back to candidates.

**Expected runtime:** ~45–90 min for full parse of 17,000 PDFs.

---

## Data Notes

- **No contributor type** — Iowa Sch-A has no contributor type column. `contributor_type` is always blank.
- **No employer/occupation** — not collected on Iowa DR-2 forms.
- **No incumbent flag** — not available from this source.
- **candidate_name fill rate ~54%** — contributions to party committees and PACs have no associated candidate. This is expected.
- **committee `candidate_name` fill rate ~75%** — the remaining 25% are PACs, party committees, and similar non-candidate filers.
- **Amended filings** — when a committee amends a report, Iowa generates a new PDF with a new timestamped filename. The manifest always points to the latest version. The `amended` flag on output rows is set from the "Status: Amended" field on page 1 of the PDF.
- **`NA` in state codes** — Iowa uses "NA" for county on PAC filings (not applicable). This can appear in `contributor_state`/`payee_state` for ~0.0% of rows when address parsing picks it up. These are flagged as tier-2 warnings by the validator.
- **Committee type variety** — raw `committee_type` from page 1 includes statewide offices not in the initial alias list (Governor, Secretary of State, Auditor of State, etc.). These pass through unmapped to canonical types but appear correctly in the committees breakdown.
- **No loans from Sch-D** — Iowa's Schedule D (Unpaid Bills) is skipped; it lacks counterparty detail needed for the `loans_debts` schema. Sch-F1 (loans received) and Sch-F2 (loans paid/forgiven) are parsed.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-23 |
| Parser | 2026-06-23 |
