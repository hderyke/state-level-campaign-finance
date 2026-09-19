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


# ========================= FEC nonfederal-IE enrichment ================
# See src/pipeline/fec_ie.py (scraper side) for what this data is.
#
# IMPORTANT: confirmed empirically (2026-09-05) that at least one committee
# in this feed (C00892919, "V-PAC: Victors Not Victims") dual-files the
# SAME disbursements with Ohio directly under its own MASTER_KEY (16182) —
# 29 of its 30 FEC-tagged rows already exist in expenditures_pacs_*.csv,
# same date/amount. So this data must be JOINED against Ohio's own
# expenditure rows and used to fill in affiliated_candidate_name/
# support_oppose on the EXISTING row, not inserted as a second copy of the
# same dollar amount — see _run() for the join. A new row is written only
# for an FEC row that has no match in Ohio's own data at all (a real gap —
# one such row was confirmed: a $150,000 charge on 2026-02-02 present in
# FEC's filing but absent from Ohio's), or for a committee with no known
# state_filer_id mapping at all (never observed to dual-file, so nothing to
# join against).

_FEC_IE_PATTERNS_PATH = PROJECT_ROOT / "src" / "aliases" / "fec_ie_patterns.csv"


class _FecIePattern:
    __slots__ = ("regex", "state_filer_id")

    def __init__(self, regex: re.Pattern, state_filer_id: str):
        self.regex = regex
        self.state_filer_id = state_filer_id


def _load_fec_ie_patterns() -> dict[str, _FecIePattern]:
    """committee_id -> _FecIePattern(regex, state_filer_id), from
    src/aliases/fec_ie_patterns.csv. state_filer_id is "" when a committee
    is known to have no Ohio filing to join against."""
    patterns: dict[str, _FecIePattern] = {}
    if not _FEC_IE_PATTERNS_PATH.exists():
        return patterns
    with open(_FEC_IE_PATTERNS_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            committee_id = (row.get("committee_id") or "").strip()
            regex        = (row.get("regex") or "").strip()
            if committee_id and regex:
                patterns[committee_id] = _FecIePattern(
                    re.compile(regex), (row.get("state_filer_id") or "").strip())
    return patterns


def _parse_fec_ie_stance(committee_id: str, description: str,
                          patterns: dict[str, _FecIePattern]) -> tuple[str, str]:
    """Returns (affiliated_candidate_name, support_oppose) — both "" if this
    committee has no known pattern, or the pattern doesn't match this
    particular row's description (never a guess)."""
    entry = patterns.get(committee_id)
    if not entry:
        return "", ""
    m = entry.regex.search(description or "")
    if not m:
        return "", ""
    stance_raw, candidate = m.group(1), m.group(2)
    stance = "S" if stance_raw.upper().startswith("SUPP") else (
              "O" if stance_raw.upper().startswith("OPP") else "")
    return utils.clean_name(candidate), stance


def _fec_amount_key(amount_str: str) -> float:
    """Round to cents for a stable dict key across the two sources'
    slightly different float formatting."""
    try:
        return round(float(amount_str), 2)
    except (TypeError, ValueError):
        return 0.0


def _load_fec_ie_index(patterns: dict[str, _FecIePattern]):
    """Read every data/Ohio/raw/fec_nonfederal_ie_*.csv row once and split
    it into:
      by_filer_key: {(state_filer_id, date, amount) -> row_info}, for
          committees with a known Ohio MASTER_KEY — these get matched
          against Ohio's own expenditure rows in _run() and popped from
          this dict as they're consumed; whatever remains at the end is
          a genuine gap (an FEC-reported disbursement with no Ohio-side
          match) and gets written as a new row.
      unmatched: [row_info, ...], for committees with NO known
          state_filer_id — nothing to join against, so these are always
          written as new rows.
    row_info carries everything needed to write either an enrichment
    (affiliated_candidate_name/support_oppose only) or a standalone new
    expenditures row.
    """
    by_filer_key: dict[tuple[str, str, float], dict] = {}
    unmatched: list[dict] = []

    for raw_file in sorted(RAW_DIR.glob("fec_nonfederal_ie_*.csv")):
        with open(raw_file, newline="", encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                committee_id = (row.get("committee_id") or "").strip()
                committee_name = utils.clean_name(row.get("committee_name", ""))
                if not committee_name:
                    continue
                amount = parse_amount(row.get("disbursement_amount", ""))
                dt     = parse_date(row.get("disbursement_date", ""))
                if not amount or not dt:
                    continue

                description = row.get("disbursement_description", "")
                affiliated_candidate_name, support_oppose = _parse_fec_ie_stance(
                    committee_id, description, patterns)

                state_filer_id = patterns.get(committee_id, _FecIePattern(None, "")).state_filer_id

                row_info = {
                    "committee_name": committee_name,
                    # 2026-09-07: carry state_filer_id through onto row_info
                    # itself (not just as the by_filer_key dict key) so a
                    # genuine-gap standalone row (section 3b in _run()) can
                    # still resolve Ohio's OWN dual-filed spelling of this
                    # committee's name for display, instead of stamping the
                    # FEC's own self-reported spelling -- see that section's
                    # comment for why (confirmed live: V-PAC's one unmatched
                    # $150,000 row showed as "V-PAC: VICTORS, NOT VICTIMS"
                    # while its other 25 matched/enriched rows correctly show
                    # Ohio's own "V-PAC VICTORS NOT VICTIMS (SUPER PAC)" --
                    # same real committee, two source spellings, cosmetically
                    # looked like two different PACs on candidate pages).
                    "state_filer_id": state_filer_id,
                    "amount": amount,
                    "date": dt,
                    "payee_name": utils.clean_name(row.get("recipient_name", "")),
                    "purpose": description,
                    "payee_city": _clean(row.get("recipient_city", "")),
                    "payee_state": _clean(row.get("recipient_state", "")),
                    "payee_zip": utils.clean_zip(row.get("recipient_zip", "")),
                    # Derived from the actual transaction date, NOT FEC's
                    # two_year_transaction_period -- that field is a cycle
                    # LABEL (e.g. a 2025-dated disbursement in the 2025-2026
                    # cycle is labeled "2026"), which doesn't line up with
                    # how Ohio's own election_year works elsewhere in this
                    # pipeline (actual reporting/calendar year -- see
                    # rpt_year usage throughout this file). Confirmed this
                    # matters live (2026-09-06): using the cycle label broke
                    # tracked_ie_committees threading in cloud/supabase/
                    # transform.py's filter_state() for any row whose actual
                    # date and cycle label landed in different years.
                    "election_year": dt[:4],
                    "affiliated_candidate_name": affiliated_candidate_name,
                    "support_oppose": support_oppose,
                    # Ohio's own expenditure rows never populate "amended" (see
                    # the main loop's hardcoded ""), so leave it blank here too
                    # rather than introducing FEC's A/N convention for just
                    # this one field on the rare new-row case -- inconsistent
                    # values here would just be noise, not signal.
                    "amended": "",
                    "filing_id": _clean(row.get("file_number", "")),
                    "raw_file": raw_file.name,
                    "row_num": i + 2,   # +1 header, +1 to 1-index
                }

                if state_filer_id:
                    key = (state_filer_id, dt, _fec_amount_key(amount))
                    by_filer_key[key] = row_info
                else:
                    unmatched.append(row_info)

    return by_filer_key, unmatched


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

    fec_ie_patterns = _load_fec_ie_patterns()
    fec_by_filer_key, fec_unmatched = _load_fec_ie_index(fec_ie_patterns)
    fec_enriched_count = 0

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

                        # FEC nonfederal-IE join: a committee that dual-files its
                        # independent expenditures with both the FEC and Ohio
                        # directly (confirmed for MASTER_KEY 16182 -- see the
                        # module docstring above _load_fec_ie_index) already
                        # has this exact dollar amount right here, from Ohio's
                        # own filing. Match by (state_filer_id, date, amount)
                        # and pop the FEC row so it's consumed -- never
                        # written again as a second, duplicate row below.
                        affiliated_candidate_name = support_oppose = ""
                        fec_key = (filer_id, dt, _fec_amount_key(amount))
                        fec_match = fec_by_filer_key.pop(fec_key, None) if filer_id else None
                        if fec_match:
                            affiliated_candidate_name = fec_match["affiliated_candidate_name"]
                            support_oppose = fec_match["support_oppose"]
                            fec_enriched_count += 1

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
                            "affiliated_candidate_name": affiliated_candidate_name,
                            "support_oppose": support_oppose,
                            "amended": "",
                            "filing_id": _clean(_get(row, resolved, "report_key")),
                            "raw_file": raw_file.name,
                            "row_num": rows_in + 1,
                        })

                log.file_parsed(raw_file.name, "expenditures", rows_out, skipped=rows_in - rows_out)

        # ── 3b. FEC nonfederal-IE: genuine gaps only ──────────────────────
        # Everything that matched an Ohio-filed row above was already
        # written as part of that row's enrichment (see the join in the
        # loop above) — writing it again here would double the dollar
        # amount. Only two categories are left to add as NEW rows:
        #   1. fec_by_filer_key remainder — a known dual-filing committee's
        #      FEC-reported disbursement that had NO matching Ohio row at
        #      all (a real gap in Ohio's own filing, not a text-completeness
        #      difference — e.g. the confirmed $150,000 2026-02-02 case).
        #   2. fec_unmatched — committees with no known Ohio state_filer_id,
        #      i.e. never observed to dual-file, so there's nothing to
        #      join against and no way to tell if Ohio already has it.
        fec_new_rows = list(fec_by_filer_key.values()) + fec_unmatched
        if fec_new_rows:
            # 2026-09-07: same "resolve to Ohio's own dual-filed spelling
            # when we know the state_filer_id" join already used below in
            # section 3c for committee receipts -- built here too so a
            # genuine-gap standalone row (no OH-side match, but we DO know
            # this FEC committee_id's Ohio state_filer_id from
            # fec_ie_patterns.csv) uses the SAME committee_name string as
            # every other row for that same real committee, rather than the
            # FEC's own self-reported spelling. Not a guess -- the
            # committee_id -> state_filer_id link is already curated and
            # certain (fec_ie_patterns.csv), this just makes the display
            # name consistent once that identity is already established.
            # Falls back to the FEC's own name when state_filer_id is
            # unknown (fec_unmatched -- a committee never observed to
            # dual-file with Ohio at all, nothing to resolve against).
            filer_id_to_committee_name_3b = {r["state_filer_id"]: r["committee_name"]
                                              for r in comm_rows if r["state_filer_id"]}
            rows_out = 0
            for row_info in fec_new_rows:
                expend_count += 1
                rows_out += 1
                resolved_committee_name = (
                    filer_id_to_committee_name_3b.get(row_info.get("state_filer_id") or "")
                    or row_info["committee_name"]
                )
                w.writerow({
                    "state": STATE,
                    "committee_name": resolved_committee_name,
                    "amount": row_info["amount"],
                    "date": row_info["date"],
                    "transaction_type": "FEC_NONFEDERAL_IE",
                    "payee_name": row_info["payee_name"],
                    "purpose": row_info["purpose"],
                    "category": "FEC_NONFEDERAL_IE",
                    "payee_city": row_info["payee_city"],
                    "payee_state": row_info["payee_state"],
                    "payee_zip": row_info["payee_zip"],
                    "candidate_name": "",
                    "office": "",
                    "election_year": row_info["election_year"],
                    "affiliated_candidate_name": row_info["affiliated_candidate_name"],
                    "support_oppose": row_info["support_oppose"],
                    "amended": row_info["amended"],
                    "filing_id": row_info["filing_id"],
                    "raw_file": row_info["raw_file"],
                    "row_num": row_info["row_num"],
                })
            log.info(f"    FEC nonfederal-IE: {fec_enriched_count} rows matched to an "
                    f"existing OH filing (enriched in place), {rows_out} written as new "
                    f"rows (no OH-side match found)")

    log.info(f"    -> {expend_count:,} expenditures total")

    # ── 3c. FEC nonfederal-IE: committee receipts (Schedule A donors) ────
    # See src/pipeline/fec_ie.py's fetch_committee_receipts() (scraper
    # side) for what this is: who actually funds a committee already known
    # (from its own disbursement language, matched above) to be doing
    # OH-nonfederal IE spending. Fundamentally different from the
    # disbursement side: a receipt carries NO signal at all about which
    # candidate/race it's "for" (the committee's money is fungible across
    # its whole account, not earmarked per contribution), so these rows
    # get NO candidate_name/office -- leaving both blank is the honest
    # answer here, not a gap to fill in later. Never fanned out or
    # attributed to a specific candidate_id; see cloud/supabase/
    # rpc_candidate_profile.sql for how a candidate page still surfaces
    # "who funds this committee" without inventing a per-dollar link this
    # source doesn't support.
    #
    # committee_name is resolved to Ohio's OWN dual-filed name when this
    # committee has a known state_filer_id (same MASTER_KEY join used
    # above for expenditures), so these rows line up with whichever name
    # string the expenditure rows actually use -- falling back to the raw
    # FEC committee name only for a committee never observed to dual-file
    # directly with Ohio (see _load_fec_ie_index's docstring).
    #
    # Rewrites contributions.csv.gz from scratch (read the rows just
    # written, append these, write once) rather than reopening it in gzip
    # append mode -- concatenated/multi-member gzip is valid per spec, but
    # not every downstream reader (e.g. DuckDB's CSV reader) is guaranteed
    # to decompress past the first member, so a single clean gzip stream
    # is the safer choice here even though Python's own gzip module would
    # have handled either form fine.
    receipt_files = sorted(RAW_DIR.glob("fec_nonfederal_ie_receipts_*.csv"))
    if receipt_files:
        log.info("  Parsing FEC nonfederal-IE committee receipts…")
        filer_id_to_committee_name = {r["state_filer_id"]: r["committee_name"]
                                       for r in comm_rows if r["state_filer_id"]}
        with gzip.open(contrib_path, "rt", newline="", encoding="utf-8") as f:
            existing_contrib_rows = list(csv.DictReader(f))

        receipt_rows = []
        for raw_file in receipt_files:
            rows_in = rows_out = 0
            with open(raw_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    rows_in += 1
                    committee_id = (row.get("committee_id") or "").strip()
                    amount = parse_amount(row.get("contribution_receipt_amount", ""))
                    dt     = parse_date(row.get("contribution_receipt_date", ""))
                    if not amount or not dt:
                        continue
                    contributor_name = utils.clean_name(row.get("contributor_name", ""))
                    if not contributor_name:
                        continue

                    entity_type = (row.get("entity_type") or "").strip().upper()
                    contributor_type = "Individual" if entity_type == "IND" else "Non-Individual"

                    state_filer_id = fec_ie_patterns.get(
                        committee_id, _FecIePattern(None, "")).state_filer_id
                    committee_name = (
                        (filer_id_to_committee_name.get(state_filer_id) if state_filer_id else None)
                        or utils.clean_name(row.get("committee_name", ""))
                    )
                    if not committee_name:
                        continue

                    rows_out += 1
                    receipt_rows.append({
                        "state": STATE,
                        "committee_name": committee_name,
                        "amount": amount,
                        "date": dt,
                        "transaction_type": "FEC_NONFEDERAL_IE_RECEIPT",
                        "contributor_name": contributor_name,
                        "contributor_type": contributor_type,
                        "contributor_city": _clean(row.get("contributor_city", "")),
                        "contributor_state": _clean(row.get("contributor_state", "")),
                        "contributor_zip": utils.clean_zip(row.get("contributor_zip", "")),
                        "employer": utils.clean_name(row.get("contributor_employer", "")),
                        "occupation": _clean(row.get("contributor_occupation", "")),
                        "candidate_name": "",
                        "office": "",
                        # Derived from the actual contribution date, NOT
                        # FEC's two_year_transaction_period -- see the
                        # matching comment in _load_fec_ie_index() above for
                        # why (cycle label vs. actual year mismatch broke
                        # tracked_ie_committees matching, confirmed live).
                        "election_year": dt[:4],
                        "amended": "",
                        "filing_id": _clean(row.get("file_number", "")),
                        "raw_file": raw_file.name,
                        "row_num": i + 2,
                    })
            log.file_parsed(raw_file.name, "contributions", rows_out, skipped=rows_in - rows_out)

        with gzip.open(contrib_path, "wt", newline="", encoding="utf-8") as out_f:
            w = csv.DictWriter(out_f, fieldnames=C.CONTRIBUTIONS, extrasaction="ignore", restval="")
            w.writeheader()
            for r in existing_contrib_rows:
                w.writerow(r)
            for r in receipt_rows:
                w.writerow(r)
        contrib_count += len(receipt_rows)
        log.info(f"    FEC nonfederal-IE receipts: {len(receipt_rows):,} committee-level "
                 f"contribution rows added (no candidate_name/office -- see comment above)")

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
