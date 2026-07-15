"""
parsers/ohio.py — Parse Ohio campaign finance bulk CSVs (from the File
Transfer Page, see scrapers/ohio.py) into the canonical cleaned schema.

Input files (data/Ohio/raw/), all produced by scrapers/ohio.py:
    entities_{slug}_active.csv        slug in candidates, pacs, parties —
                                       roster of currently-active entities
    contributions_{slug}_{year}.csv   one bulk file per (group, year), 1990-present
    expenditures_{slug}_{year}.csv    same grain
    cover_pages_{slug}.csv            aggregate per-filing totals — NOT parsed
                                       (not itemized; no canonical table fits it)
    contributions_{slug}_supp_*.csv,
    expenditures_{slug}_supp_*.csv    one-off per-committee files (mostly
                                       legislative leadership funds) — NOT
                                       parsed by default; see "Supplemental
                                       files" below.

Output (data/Ohio/cleaned/):
    candidates.csv.gz, committees.csv.gz,
    contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz (empty —
    no loan/debt schedule was identified in any Ohio bulk export)

id_model = "committee"
    MASTER_KEY is the closest thing Ohio has to a filer ID. Whether it's
    stable across a candidate's entire multi-cycle career or reissued per
    registration was not confirmed (no multi-cycle history was available
    to check both ways) — "committee" grouping (person_id = min MASTER_KEY
    per (state, candidate_name, office, district)) is safe either way: if
    MASTER_KEY already is stable per person, the grouping is a no-op; if
    it isn't, the grouping fixes it. See utils.assign_person_ids.

VERIFIED vs ASSUMED:
    Every column mapping below for the "candidates" group (ACT_CAN_LIST,
    CAC_CON_*, CAC_EXP_*, CAN_COVER) was checked against real downloaded
    files. The "pacs" and "parties" groups were NOT sampled — their files
    are assumed to share the same column layout as the candidates group
    (same underlying export, filtered to a different committee type), with
    the obvious candidate-specific columns (CANDIDATE_FIRST_NAME, OFFICE,
    DISTRICT) expected to just be blank for PAC/party rows rather than
    absent. Header resolution is name-based (via `_resolve_headers`) for
    exactly this reason — if PAC/party files use different column names,
    resolution fails loudly (`file_parse_error`, file skipped) instead of
    silently mismapping. Confirm against a real PAC/party file before
    trusting that data.

Known header quirks (verified):
    - ACT_CAN_LIST.CSV repeats the column name "OFFICE" — the true header
      is COM_NAME, MASTER_KEY, COM_ADDRESS, COM_CITY, COM_STATE, COM_ZIP,
      TREA_FIRST_NAME, TREA_LAST_NAME, TREA_MIDDLE_NAME, TREA_SUFFIX,
      TREA_ADDRESS, TREA_CITY, TREA_STATE, TREA_ZIP, DEP_FIRST_NAME,
      DEP_LAST_NAME, CANDIDATE_FIRST_NAME, CANDIDATE_LAST_NAME, OFFICE,
      DISTRICT, OFFICE (sic — this second one is actually PARTY),
      SPONSOR. Confirmed positionally against real data: index 18 is the
      true office, index 20 (mislabeled "OFFICE") holds party values like
      "REPUBLICAN"/"DEMOCRAT". csv.DictReader would silently drop the
      first OFFICE value on any duplicate-header file (Python dict
      construction keeps the last key written) — this file is parsed with
      plain csv.reader and positional indexing instead, specifically to
      avoid that.
    - CAC_EXP_* uses "CANDIDATE FIRST NAME"/"CANDIDATE LAST NAME" (spaces)
      while CAC_CON_* uses "CANDIDATE_FIRST_NAME"/"CANDIDATE_LAST_NAME"
      (underscores) for the same logical field — handled via alias lists,
      not a single hardcoded name, in `_CONTRIB_ALIASES`/`_EXPEND_ALIASES`.
    - All files use bare "\\r" line endings. Do not open with newline="" —
      confirmed that default universal-newline text mode splits these
      correctly and newline="" does not.

Supplemental files (contributions/expenditures_{slug}_supp_*.csv):
    These are one-off exports for specific committees (mostly legislative
    leadership funds — "House Leadership", "Senate Leadership" appear in
    several labels). It was NOT confirmed whether their contents are
    already included in the corresponding yearly bulk files (in which case
    parsing them too would double-count every transaction) or whether
    these committees are excluded from the yearly exports for some
    procedural reason (in which case skipping them creates a real gap).
    Parsing defaults to skipping them until this is checked — see
    docs/states/ohio.md for how to verify (compare REPORT_KEY values
    between a supplemental file and the matching year's bulk file for the
    same committee).

Column mapping — contributions (CAC_CON_*, verified; PAC/PARTY assumed identical):
    COM_NAME                              -> committee_name
    MASTER_KEY                            -> state_filer_id
    SHORT_DESCRIPTION                      -> transaction_type (schedule
                                              code, e.g. "31-A  Stmt of
                                              Contribution" vs "31-J-1
                                              In-Kind Cont Rcvd" — a real
                                              classifier, unlike anything
                                              available via the search UI)
    FIRST/MIDDLE/LAST/SUFFIX_NAME          -> contributor_name (individual)
    NON_INDIVIDUAL                         -> contributor_name (organization,
                                              used when no individual name)
    PAC_REG_NO                             -> not mapped directly (no
                                              canonical column for the
                                              contributing PAC's own reg no)
    CITY/STATE/ZIP                         -> contributor_city/state/zip
    FILE_DATE                              -> date
    AMOUNT                                  -> amount
    EMP_OCCUPATION                          -> occupation (combined
                                              employer+occupation free text
                                              in the source — employer is
                                              left blank rather than
                                              guessing a split)
    CANDIDATE_FIRST_NAME/CANDIDATE_LAST_NAME -> candidate_name (blank for
                                              PAC/party files, where a
                                              transaction isn't tied to one
                                              candidate)
    OFFICE                                  -> office
    RPT_YEAR                                -> election_year (approximation
                                              — the filing year, not
                                              necessarily the candidate's
                                              election year)
    REPORT_KEY                              -> filing_id

Column mapping — expenditures (CAC_EXP_*, verified; PAC/PARTY assumed identical):
    Same pattern as contributions, with EXPEND_DATE -> date, PURPOSE ->
    purpose, and payee name resolution instead of contributor name
    resolution (FIRST/MIDDLE/LAST/SUFFIX_NAME else NON_INDIVIDUAL).

Column mapping — entities (verified for "candidates" only; see quirk above):
    COM_NAME, MASTER_KEY                   -> committee_name, state_filer_id
    COM_CITY, COM_ZIP                      -> city, zip
    TREA_FIRST/MIDDLE/LAST/SUFFIX_NAME     -> treasurer_name
    CANDIDATE_FIRST_NAME, CANDIDATE_LAST_NAME -> candidate_name (candidates()
                                              group only)
    OFFICE, DISTRICT, PARTY (see quirk)    -> office, district, party
                                              (candidates.csv only —
                                              committees.csv has no party
                                              column)

    Every entity in this file is, by construction, active — this is the
    *active* roster, so committees/candidates sourced from it get
    active="1". Committees seen only in a contribution/expenditure file
    (not in the active roster — e.g. historical/inactive committees) are
    still added to committees.csv, with active left blank (unknown) rather
    than assumed inactive.
"""

import csv
import gzip
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Ohio" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Ohio" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "OH"
MAX_VALID_YEAR = date.today().year + 2
MIN_VALID_YEAR = 1970   # generous floor — Ohio's earliest bulk file seen is 1990

GROUPS = [
    ("candidates", "Candidate Committee"),
    ("pacs",       "PAC"),
    ("parties",    "Party Committee"),
]

# Confirmed exact header for ACT_CAN_LIST.CSV (the "candidates" active roster).
# Column 18 = true OFFICE, column 20 = mislabeled second "OFFICE" (actually
# PARTY) — see module docstring. Positional, not name-based, on purpose.
_ACT_CAN_LIST_HEADER = [
    "COM_NAME", "MASTER_KEY", "COM_ADDRESS", "COM_CITY", "COM_STATE", "COM_ZIP",
    "TREA_FIRST_NAME", "TREA_LAST_NAME", "TREA_MIDDLE_NAME", "TREA_SUFFIX",
    "TREA_ADDRESS", "TREA_CITY", "TREA_STATE", "TREA_ZIP",
    "DEP_FIRST_NAME", "DEP_LAST_NAME",
    "CANDIDATE_FIRST_NAME", "CANDIDATE_LAST_NAME", "OFFICE", "DISTRICT",
    "OFFICE", "SPONSOR",
]

# ========================= header resolution ===========================
#
# Name-based (not positional) so PAC/party files with slightly different
# naming (or genuinely different columns) fail loudly instead of silently
# mismapping. See module docstring for the CANDIDATE_FIRST_NAME vs
# "CANDIDATE FIRST NAME" quirk this is built to absorb.

_CONTRIB_ALIASES = {
    "committee_name":   ["COM_NAME"],
    "state_filer_id":   ["MASTER_KEY"],
    "short_description": ["SHORT_DESCRIPTION"],
    "first_name":       ["FIRST_NAME"],
    "middle_name":      ["MIDDLE_NAME"],
    "last_name":        ["LAST_NAME"],
    "suffix_name":      ["SUFFIX_NAME"],
    "non_individual":   ["NON_INDIVIDUAL"],
    "city":             ["CITY"],
    "state":            ["STATE"],
    "zip":              ["ZIP"],
    "date":             ["FILE_DATE"],
    "amount":           ["AMOUNT"],
    "occupation":       ["EMP_OCCUPATION"],
    "candidate_first":  ["CANDIDATE_FIRST_NAME", "CANDIDATE FIRST NAME"],
    "candidate_last":   ["CANDIDATE_LAST_NAME", "CANDIDATE LAST NAME"],
    "office":           ["OFFICE"],
    "rpt_year":         ["RPT_YEAR"],
    "report_key":       ["REPORT_KEY"],
}

_EXPEND_ALIASES = {
    "committee_name":   ["COM_NAME"],
    "state_filer_id":   ["MASTER_KEY"],
    "short_description": ["SHORT_DESCRIPTION"],
    "first_name":       ["FIRST_NAME"],
    "middle_name":      ["MIDDLE_NAME"],
    "last_name":        ["LAST_NAME"],
    "suffix_name":      ["SUFFIX_NAME"],
    "non_individual":   ["NON_INDIVIDUAL"],
    "city":             ["CITY"],
    "state":            ["STATE"],
    "zip":              ["ZIP"],
    "date":             ["EXPEND_DATE"],
    "amount":           ["AMOUNT"],
    "purpose":          ["PURPOSE"],
    "candidate_first":  ["CANDIDATE_FIRST_NAME", "CANDIDATE FIRST NAME"],
    "candidate_last":   ["CANDIDATE_LAST_NAME", "CANDIDATE LAST NAME"],
    "office":           ["OFFICE"],
    "rpt_year":         ["RPT_YEAR"],
    "report_key":       ["REPORT_KEY"],
}

# For pacs/parties entity rosters — unverified layout, best guess.
_ENTITY_ALIASES = {
    "committee_name":   ["COM_NAME"],
    "state_filer_id":   ["MASTER_KEY"],
    "city":             ["COM_CITY", "CITY"],
    "zip":              ["COM_ZIP", "ZIP"],
    "treasurer_first":  ["TREA_FIRST_NAME"],
    "treasurer_middle": ["TREA_MIDDLE_NAME"],
    "treasurer_last":   ["TREA_LAST_NAME"],
    "treasurer_suffix": ["TREA_SUFFIX"],
    "pac_reg_no":       ["PAC_REG_NO"],
}


def _resolve_headers(fieldnames: list[str], alias_map: dict[str, list[str]]) -> dict[str, str | None]:
    lookup = {(h or "").strip().upper(): h for h in fieldnames}
    resolved = {}
    for logical, aliases in alias_map.items():
        found = None
        for alias in aliases:
            key = alias.strip().upper()
            if key in lookup:
                found = lookup[key]
                break
        resolved[logical] = found
    return resolved


def _get(row: dict, resolved: dict, logical: str) -> str:
    col = resolved.get(logical)
    return row.get(col, "") if col else ""


# ========================= value helpers ===============================

def _clean(val) -> str:
    return (val or "").strip()


def _join_name(*parts) -> str:
    joined = " ".join(_clean(p) for p in parts if _clean(p))
    return joined


def parse_amount(val: str) -> str:
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]
    try:
        return str(float(v))
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%b-%y", "%m-%d-%Y"):
        try:
            d = datetime.strptime(v, fmt).date()
        except ValueError:
            continue
        if MIN_VALID_YEAR <= d.year <= MAX_VALID_YEAR:
            return d.strftime("%Y-%m-%d")
        return ""
    return ""


# ========================= entities (candidates/committees) ============

def parse_candidates_active(log) -> tuple[list[dict], list[dict]]:
    """Parse entities_candidates_active.csv (confirmed layout — positional,
    see module docstring for the duplicate-OFFICE quirk). Returns
    (candidate_dicts, committee_dicts).
    """
    path = RAW_DIR / "entities_candidates_active.csv"
    cand_rows, comm_rows = [], []
    if not path.exists():
        return cand_rows, comm_rows

    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return cand_rows, comm_rows

        if [h.strip().upper() for h in header] != _ACT_CAN_LIST_HEADER:
            log.file_parse_error(
                filename=path.name,
                error=f"header does not match the confirmed ACT_CAN_LIST layout "
                     f"(got {header!r}) — site format may have changed; "
                     f"skipping positional parse to avoid silent mismapping",
            )
            return cand_rows, comm_rows

        for ri, row in enumerate(reader, start=2):
            if len(row) < 22:
                continue
            committee_name = utils.clean_name(row[0])
            master_key     = _clean(row[1])
            com_city       = _clean(row[3])
            com_zip        = _clean(row[5])
            treasurer_name = _join_name(row[6], row[8], row[7], row[9])  # first, middle, last, suffix
            candidate_name = _join_name(row[16], row[17])
            office         = _clean(row[18])
            district       = _clean(row[19])
            party          = _clean(row[20])   # mislabeled "OFFICE" in the header — this is PARTY

            if committee_name:
                comm_rows.append({
                    "state": STATE, "person_id": "",
                    "committee_name": committee_name,
                    "committee_type": "Candidate Committee",
                    "election_year": "",
                    "candidate_name": utils.clean_name(candidate_name),
                    "treasurer_name": treasurer_name,
                    "city": com_city, "zip": utils.clean_zip(com_zip),
                    "active": "1",
                    "state_filer_id": master_key,
                    "raw_file": path.name, "row_num": ri,
                })
            if candidate_name:
                cand_rows.append({
                    "state": STATE, "person_id": "",
                    "candidate_name": utils.clean_name(candidate_name),
                    "candidate_first": utils.clean_name(row[16]),
                    "candidate_last": utils.clean_name(row[17]),
                    "office": office, "canonical_office": "",
                    "district": district, "jurisdiction": "",
                    "party": party, "election_year": "",
                    "incumbent": "", "state_filer_id": master_key,
                    "raw_file": path.name, "row_num": ri,
                })

    return cand_rows, comm_rows


def parse_other_entities_active(log, slug: str, committee_type: str) -> list[dict]:
    """Parse entities_{slug}_active.csv for pacs/parties. Layout is
    UNVERIFIED (no sample file was available) — resolved by column name
    via _ENTITY_ALIASES rather than position, so a mismatch fails loudly
    (file_parse_error + skip) instead of silently mismapping.
    """
    path = RAW_DIR / f"entities_{slug}_active.csv"
    rows_out = []
    if not path.exists():
        return rows_out

    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return rows_out
        if len(set(reader.fieldnames)) != len(reader.fieldnames):
            log.file_parse_error(
                filename=path.name,
                error=f"header contains duplicate column names {reader.fieldnames!r} "
                     f"— this file needs positional parsing like ACT_CAN_LIST but "
                     f"no confirmed layout exists for {slug!r}; skipping",
            )
            return rows_out

        resolved = _resolve_headers(reader.fieldnames, _ENTITY_ALIASES)
        if not resolved.get("committee_name"):
            log.file_parse_error(
                filename=path.name,
                error=f"could not resolve committee_name column in header "
                     f"{reader.fieldnames!r} — check _ENTITY_ALIASES for {slug!r}",
            )
            return rows_out

        for ri, row in enumerate(reader, start=2):
            committee_name = utils.clean_name(_get(row, resolved, "committee_name"))
            if not committee_name:
                continue
            treasurer_name = _join_name(
                _get(row, resolved, "treasurer_first"),
                _get(row, resolved, "treasurer_middle"),
                _get(row, resolved, "treasurer_last"),
                _get(row, resolved, "treasurer_suffix"),
            )
            rows_out.append({
                "state": STATE, "person_id": "",
                "committee_name": committee_name,
                "committee_type": committee_type,
                "election_year": "",
                "candidate_name": "",
                "treasurer_name": treasurer_name,
                "city": _clean(_get(row, resolved, "city")),
                "zip": utils.clean_zip(_get(row, resolved, "zip")),
                "active": "1",
                "state_filer_id": _clean(_get(row, resolved, "state_filer_id")),
                "raw_file": path.name, "row_num": ri,
            })
    return rows_out


# ========================== run ==========================================

def run():
    log = get_logger("ohio", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")
    try:
        _run(log, t0)
    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


def _run(log, t0: float):

    # ── 1. Entities → candidates + committees (active rosters) ──────────
    log.info("  Parsing active-entity rosters…")
    cand_rows, comm_rows = parse_candidates_active(log)
    comm_rows += parse_other_entities_active(log, "pacs", "PAC")
    comm_rows += parse_other_entities_active(log, "parties", "Party Committee")

    seen_filer_ids = {r["state_filer_id"] for r in comm_rows if r["state_filer_id"]}

    # ── 2. Contributions + harvest any committees missing from the active
    #      roster (historical/inactive committees) along the way ────────
    log.info("  Parsing contributions…")
    contrib_path = CLEAN_DIR / "contributions.csv.gz"
    contrib_count = 0

    with gzip.open(contrib_path, "wt", newline="", encoding="utf-8") as out_f:
        w = csv.DictWriter(out_f, fieldnames=C.CONTRIBUTIONS, extrasaction="ignore", restval="")
        w.writeheader()

        for slug, committee_type in GROUPS:
            for raw_file in sorted(RAW_DIR.glob(f"contributions_{slug}_*.csv")):
                if "_supp_" in raw_file.name:
                    continue   # see module docstring — skipped pending dedup verification
                rows_in = rows_out = 0
                with open(raw_file, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames:
                        continue
                    resolved = _resolve_headers(reader.fieldnames, _CONTRIB_ALIASES)
                    if not resolved.get("committee_name") or not resolved.get("amount"):
                        log.file_parse_error(
                            filename=raw_file.name,
                            error=f"could not resolve committee_name/amount columns in "
                                 f"header {reader.fieldnames!r} — check _CONTRIB_ALIASES",
                        )
                        continue

                    for row in reader:
                        rows_in += 1
                        committee_name = utils.clean_name(_get(row, resolved, "committee_name"))
                        if not committee_name:
                            continue
                        amount = parse_amount(_get(row, resolved, "amount"))
                        dt     = parse_date(_get(row, resolved, "date"))
                        if not amount or not dt:
                            continue

                        indiv = _join_name(
                            _get(row, resolved, "first_name"),
                            _get(row, resolved, "middle_name"),
                            _get(row, resolved, "last_name"),
                            _get(row, resolved, "suffix_name"),
                        )
                        org = _clean(_get(row, resolved, "non_individual"))
                        if indiv:
                            contributor_name, contributor_type = indiv, "Individual"
                        elif org:
                            contributor_name, contributor_type = org, "Non-Individual"
                        else:
                            contributor_name, contributor_type = "", ""

                        filer_id = _clean(_get(row, resolved, "state_filer_id"))
                        if filer_id and filer_id not in seen_filer_ids:
                            seen_filer_ids.add(filer_id)
                            comm_rows.append({
                                "state": STATE, "person_id": "",
                                "committee_name": committee_name,
                                "committee_type": committee_type,
                                "election_year": _clean(_get(row, resolved, "rpt_year")),
                                "candidate_name": utils.clean_name(_join_name(
                                    _get(row, resolved, "candidate_first"),
                                    _get(row, resolved, "candidate_last"))),
                                "treasurer_name": "", "city": "", "zip": "",
                                "active": "",   # not in the active roster — status unknown
                                "state_filer_id": filer_id,
                                "raw_file": raw_file.name, "row_num": rows_in + 1,
                            })

                        contrib_count += 1
                        rows_out += 1
                        w.writerow({
                            "state": STATE,
                            "committee_name": committee_name,
                            "amount": amount,
                            "date": dt,
                            "transaction_type": _clean(_get(row, resolved, "short_description")),
                            "contributor_name": contributor_name,
                            "contributor_type": contributor_type,
                            "contributor_city": _clean(_get(row, resolved, "city")),
                            "contributor_state": _clean(_get(row, resolved, "state")),
                            "contributor_zip": utils.clean_zip(_get(row, resolved, "zip")),
                            "employer": "",
                            "occupation": _clean(_get(row, resolved, "occupation")),
                            "candidate_name": utils.clean_name(_join_name(
                                _get(row, resolved, "candidate_first"),
                                _get(row, resolved, "candidate_last"))),
                            "office": _clean(_get(row, resolved, "office")),
                            "election_year": _clean(_get(row, resolved, "rpt_year")),
                            "amended": "",
                            "filing_id": _clean(_get(row, resolved, "report_key")),
                            "raw_file": raw_file.name,
                            "row_num": rows_in + 1,
                        })

                log.file_parsed(raw_file.name, "contributions", rows_out, skipped=rows_in - rows_out)

    log.info(f"    -> {contrib_count:,} contributions total")

    # ── 3. Expenditures ───────────────────────────────────────────────────
    log.info("  Parsing expenditures…")
    expend_path = CLEAN_DIR / "expenditures.csv.gz"
    expend_count = 0

    with gzip.open(expend_path, "wt", newline="", encoding="utf-8") as out_f:
        w = csv.DictWriter(out_f, fieldnames=C.EXPENDITURES, extrasaction="ignore", restval="")
        w.writeheader()

        for slug, committee_type in GROUPS:
            for raw_file in sorted(RAW_DIR.glob(f"expenditures_{slug}_*.csv")):
                if "_supp_" in raw_file.name:
                    continue
                rows_in = rows_out = 0
                with open(raw_file, encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames:
                        continue
                    resolved = _resolve_headers(reader.fieldnames, _EXPEND_ALIASES)
                    if not resolved.get("committee_name") or not resolved.get("amount"):
                        log.file_parse_error(
                            filename=raw_file.name,
                            error=f"could not resolve committee_name/amount columns in "
                                 f"header {reader.fieldnames!r} — check _EXPEND_ALIASES",
                        )
                        continue

                    for row in reader:
                        rows_in += 1
                        committee_name = utils.clean_name(_get(row, resolved, "committee_name"))
                        if not committee_name:
                            continue
                        amount = parse_amount(_get(row, resolved, "amount"))
                        dt     = parse_date(_get(row, resolved, "date"))
                        if not amount or not dt:
                            continue

                        payee_indiv = _join_name(
                            _get(row, resolved, "first_name"),
                            _get(row, resolved, "middle_name"),
                            _get(row, resolved, "last_name"),
                            _get(row, resolved, "suffix_name"),
                        )
                        payee_org = _clean(_get(row, resolved, "non_individual"))
                        payee_name = payee_indiv or payee_org

                        filer_id = _clean(_get(row, resolved, "state_filer_id"))
                        if filer_id and filer_id not in seen_filer_ids:
                            seen_filer_ids.add(filer_id)
                            comm_rows.append({
                                "state": STATE, "person_id": "",
                                "committee_name": committee_name,
                                "committee_type": committee_type,
                                "election_year": _clean(_get(row, resolved, "rpt_year")),
                                "candidate_name": utils.clean_name(_join_name(
                                    _get(row, resolved, "candidate_first"),
                                    _get(row, resolved, "candidate_last"))),
                                "treasurer_name": "", "city": "", "zip": "",
                                "active": "",
                                "state_filer_id": filer_id,
                                "raw_file": raw_file.name, "row_num": rows_in + 1,
                            })

                        expend_count += 1
                        rows_out += 1
                        w.writerow({
                            "state": STATE,
                            "committee_name": committee_name,
                            "amount": amount,
                            "date": dt,
                            "transaction_type": _clean(_get(row, resolved, "short_description")),
                            "payee_name": payee_name,
                            "purpose": _clean(_get(row, resolved, "purpose")),
                            "category": "",
                            "payee_city": _clean(_get(row, resolved, "city")),
                            "payee_state": _clean(_get(row, resolved, "state")),
                            "payee_zip": utils.clean_zip(_get(row, resolved, "zip")),
                            "candidate_name": utils.clean_name(_join_name(
                                _get(row, resolved, "candidate_first"),
                                _get(row, resolved, "candidate_last"))),
                            "office": _clean(_get(row, resolved, "office")),
                            "election_year": _clean(_get(row, resolved, "rpt_year")),
                            "amended": "",
                            "filing_id": _clean(_get(row, resolved, "report_key")),
                            "raw_file": raw_file.name,
                            "row_num": rows_in + 1,
                        })

                log.file_parsed(raw_file.name, "expenditures", rows_out, skipped=rows_in - rows_out)

    log.info(f"    -> {expend_count:,} expenditures total")

    # ── 4. Write candidates/committees (now includes harvested rows) ────
    cand_path = CLEAN_DIR / "candidates.csv.gz"
    with gzip.open(cand_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.CANDIDATES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(cand_rows)
    n_cands = utils.assign_person_ids(cand_path, id_model="committee")
    log.file_parsed("candidates.csv.gz", "candidates", n_cands, role="output")

    comm_path = CLEAN_DIR / "committees.csv.gz"
    with gzip.open(comm_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.COMMITTEES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(comm_rows)
    n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
    log.file_parsed("committees.csv.gz", "committees", len(comm_rows), role="output")
    log.info(f"    -> {n_cands:,} candidates, {len(comm_rows):,} committees "
            f"({n_comm_matched:,} matched to a candidate)")

    # ── 5. Loans/debts — no loan schedule identified in any Ohio export ──
    loans_path = CLEAN_DIR / "loans_debts.csv.gz"
    with gzip.open(loans_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.LOANS_DEBTS, extrasaction="ignore", restval="")
        w.writeheader()

    duration = round(time.perf_counter() - t0, 1)
    log._emit("parse_completed", status="completed", duration_s=duration,
              candidates=n_cands, committees=len(comm_rows),
              contributions=contrib_count, expenditures=expend_count)
    log.info(f"Done in {duration}s")


# ============================== CLI ======================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
