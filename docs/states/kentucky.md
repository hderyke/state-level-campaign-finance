# Kentucky — Pipeline Documentation

---

## Overview

| | |
|---|---|
| **State** | Kentucky (KY) |
| **Source** | [Kentucky Registry of Election Finance (KREF)](https://kref.ky.gov) |
| **Access method** | Bulk CSV GET requests to KREF export endpoints — no Playwright, no pagination |
| **Coverage** | 1996 – present (partial 1996; meaningful data starts 1997) |
| **person_id model** | `name_hash` — no numeric filer IDs in source; `person_id` derived from MD5 of normalized name |

---

## Raw Data Structure

Six file types in `data/Kentucky/raw/`, all flat CSV:

| File pattern | Content |
|---|---|
| `candidates_{party}.csv` | One file per party (republican, democratic, libertarian, independent, other, notapplicable). Party affiliation is only available via this per-party export. |
| `organizations.csv` | Committees, PACs, executive committees, and other filer entities |
| `contributions_{year}.csv` | All contributions for the calendar year |
| `expenditures_{year}.csv` | All expenditures for the calendar year |

### Contribution files

Key fields in `contributions_{year}.csv`:

| Field | Description |
|---|---|
| `Contribution Type` | Transaction classification (e.g. `INDIVIDUAL`, `BALANCE_CARRYFORWARD`, `KYPAC`) |
| `From Organization Name` | Contributor org name (used when contributor is a PAC, committee, or executive org) |
| `First Name` / `Last Name` | Contributor personal name (used when `Contribution Type = INDIVIDUAL`) |
| `OtherText` | Free-text field; contains actual donor name in older (pre-2002) records where `From Organization Name = 'VARIOUS'` |
| `Contribution Date` | Date of contribution (MM/DD/YYYY in raw) |
| `Amount` | Dollar amount (negative values represent refunds/reversals) |
| `City` / `State` / `Zip` | Contributor address |
| `Employer` / `Occupation` | Contributor employment (not always required) |
| `Committee Name` | Name of the receiving committee |
| `Candidate Name` | Associated candidate (blank for PAC/executive committee rows) |
| `Office` | Office sought |
| `Election Year` | Election cycle year |

### Expenditure files

Key fields in `expenditures_{year}.csv`:

| Field | Description |
|---|---|
| `Expenditure Type` | Transaction classification (`MONETARY`, `INKIND`, `TRANSFER`, `DEBT_ASSUMPTION`, `ITC`, `NONE`) |
| `Organization Name` | Payee name |
| `Expenditure Date` | Date of payment (MM/DD/YYYY in raw) |
| `Amount` | Dollar amount |
| `Purpose` | Free-text description of expenditure |
| `Committee Name` | Name of the paying committee |
| `Candidate Name` / `Office` | Associated candidate and office |

Note: KREF expenditure exports do not include payee city, state, or zip. `payee_city/state/zip` are 0% filled for all KY expenditures.

---

## Scraper

`src/pipeline/scrapers/kentucky.py`

Simple GET requests to KREF export endpoints — one file per year for contributions and expenditures, one file per party for candidates, one file for organizations. No authentication, no Playwright, no pagination required.

**Limitations:**
- KREF does not expose a bulk "all years" endpoint — each year requires a separate request.
- The organizations export is a full dump with no year filter; re-scraped on every run.

**Expected runtime:** ~1–2 min (one request per year per file type, ~60 files total).

---

## Parser

`src/pipeline/parsers/kentucky.py`

Reads candidate party files, `organizations.csv`, and per-year contribution/expenditure CSVs. Contributions and expenditures are the dominant processing cost.

**Contributor name resolution:**  
KREF stores contributor info across multiple fields that vary by era and contribution type. Resolution order per row:

1. `From Organization Name` (used for organizations, PACs, executive committees)
2. `First Name` + `Last Name` (used for individuals)
3. If still blank:
   - If `Contribution Type == BALANCE_CARRYFORWARD`: set `contributor_name = committee_name` — the committee is rolling forward its own prior-period balance; no external donor exists.
   - Else if `OtherText` is non-blank and not a known placeholder value: use `OtherText` as `contributor_name` — covers pre-2002 inter-committee transfers where KREF stored the actual donor in the free-text field instead of the standard name columns.

**Skip rules:**

| Pattern | Action | Reason |
|---|---|---|
| `From Organization Name = "TOTAL"` (contributions) | Row skipped | KREF quarterly rollup rows — double-count all itemized contributions |
| `Organization Name` in `{TOTAL, TOTAL DISBURSEMENTS, IN-KIND GIVEN TOTAL, BALANCE TRANSFER TO GENERAL}` (expenditures) | Row skipped | Quarterly summary/accounting rows, not real payees |
| `From Organization Name = "NEW YEAR"` (contributions) | contributor_name blanked | Start-of-year balance carryforward encoded as an org name (`OtherText = 'BALANCE CARRIED FORWARD'`; date always Jan 1) |
| `contributor_state = "N/A"` or is numeric | Converted to `""` | KREF placeholder for non-addressable transaction types and data-entry errors |
| `contributor_city = "N/A"` | Converted to `""` | Same |
| `employer = "N/A"` | Converted to `""` | Same |

**person_id model:** `name_hash` — KREF assigns no numeric filer ID. A stable 14-digit integer is derived from `MD5("KY|" + normalized_name)` prefixed with Kentucky's FIPS code (21).

**Limitations:**
- `contributor_state` is blank for ~43% of rows — BALANCE_CARRYFORWARD, UNITEMIZED, ANONYMOUS, and CASH contribution types have no contributor address in the source, and pre-2002 records are often missing the state field.
- `payee_city/state/zip` are absent from KREF expenditure exports entirely.
- `party`, `district`, `incumbent`, `treasurer_name` are sparsely populated or absent in the source.

**Expected runtime:** ~1.5 min (26 years of contribution files + 26 years of expenditure files).

---

## Data Notes

- **BALANCE_CARRYFORWARD inflation** — 92,665 rows ($988M) are accounting entries recording prior-period balance rollovers within a committee's own account, not actual donations from an external source. The parser assigns `contributor_name = committee_name` for these rows. As a result, major candidates appear to have donated tens of millions to themselves in the top-contributors spot-check — this is the carryforward, not self-funding. Filter `WHERE contributor_type != 'BALANCE_CARRYFORWARD'` for donor analysis.
- **VARIOUS / OtherText** — in older (1996–2002) KREF records, large transfers from state and national party organizations were recorded with `From Organization Name = 'VARIOUS'` and the actual donor stored in `OtherText`. Parser reads `OtherText` as `contributor_name` when standard name fields are blank. This resolves entries like the $4.9M Kentucky State Democratic Executive Committee transfer (2000) and the $3.7M Kentucky Republican Executive Committee transfer (2000), which previously had no contributor name.
- **Slate committees** — Kentucky's Governor and Lt. Governor run as a joint ticket and file as a single "SLATE" campaign committee. Contributions and expenditures for the ticket go to this joint entity. Most major gubernatorial candidates (both Beshears, Fletcher, Bevin, Heiner, Lunsford, Conway, Adkins) appear under `office = SLATE` rather than `GOVERNOR`. `office_types.csv` maps `SLATE → Governor/Lt. Governor Ticket`.
- **Name variants** — the same candidate appears under multiple name spellings across different filing periods (e.g. "ANDREW BESHEAR" and "ANDY BESHEAR" are both present as separate entries in the top recipients, accounting for $54.7M and $7.4M respectively). The name_hash model assigns separate person_ids — cross-name-variant deduplication is not implemented.
- **"Andy Besher for Governor" payee** — a top-10 expenditure recipient with ~$9M in disbursements is listed as "Andy Besher for Governor" (misspelled). This is a filer data-entry error on the disbursing committee's side; the parser cannot correct it.
- **Republican Governors Association duplicates** — "REPUBLICAN GOVERNORS ASSOCIATION" and "Republican Governors Association" (case variant) appear as separate contributor entries totaling ~$26M. Cross-contributor name normalization is not currently applied.
- **Bad ZIP values** — 823 non-empty contributor ZIP values don't match ZIP format (0.2%), e.g. `'0'`, `'0000'`, `'011129'`, `'02102014'`. Source data errors; not filtered.
- **Year 2030 row** — 1 contribution row with a year-2030 date ($17). Filer data-entry error; not filtered.
- **KREF TEST entries** — occasional test entries in the KREF production system are present at low volume. Not filtered.

---

## Last Updated

| Component | Date |
|---|---|
| Scraper | 2026-06-26 |
| Parser | 2026-06-26 (BALANCE_CARRYFORWARD contributor_name fix; OtherText/VARIOUS fallback) |
| Documentation | 2026-06-27 |
