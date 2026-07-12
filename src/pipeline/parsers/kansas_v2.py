"""
parsers/kansas_v2.py — Parse kansas_v2.py's scraped CSVs into canonical
cleaned CSVs. Replaces parsers/kansas.py (the old PDF-based parser) now
that scrapers/kansas_v2.py extracts structured data directly from the KS
SOS CFR Examiner instead of downloading PDFs.

Input:  data/Kansas/ (written by scrapers/kansas_v2.py)
    candidates_summary.csv        — one row per candidate filing
    schedule_a_contributions.csv  — itemized contributions
    schedule_b_inkind.csv         — itemized in-kind contributions
    schedule_c_expenditures.csv   — itemized expenditures
    schedule_d_other.csv          — other transactions/loans (skipped,
                                     same as Schedule D in the old parser)
    manifest_v2.csv                — not required for parsing (see below),
                                     read only to log coverage vs. the scrape

Output: data/Kansas/cleaned/ (same location/filenames as the old parser,
so nothing downstream needs to change)
    contributions.csv.gz, expenditures.csv.gz,
    candidates.csv.gz, committees.csv.gz

Why this parser is much simpler than the old PDF parser:
    kansas.py's parser had to reconstruct rows from raw PDF word
    coordinates (clustering by y, guessing column boundaries, walking
    backward across rows for wrapped names/addresses). kansas_v2.py
    already did that work at scrape time — it read the HTML tables
    directly — so this parser is just: read 4 CSVs, normalize dates/
    amounts, join, and write the canonical schema.

Joining schedule rows to their candidate:
    Every row in every one of kansas_v2.py's five output files carries a
    candidate_uid column: "office_group|cycle_label|office_sought|
    district_number|name|original_date|amendment_date" (identical to the
    manifest's candidate_key). That's the join key used here — NOT the
    "candidate" name-text column, which can collide across different
    cycles/offices for two people who happen to share a name.

    office_group and election_year are decoded straight out of
    candidate_uid (it's built from the same RUN_CATALOG entry that drove
    the search), rather than joining against manifest_v2.csv. manifest_v2.csv
    is only read to log how many scraped candidates made it into this
    parse, as a sanity check.

id_model = "name_hash" — same as the old Kansas parser (and Alaska,
Idaho): Kansas has no numeric filer ID in its source data, so person_id
is derived from MD5(state + normalized candidate_name).

Party: not available in source (same as the old parser).

Amendments: unlike the old PDF pipeline (separate PDF files for
originals vs. amendments, requiring dedup), the CFR Examiner's results
grid already resolves each filing to one row with an amendment_date
column if it was amended — kansas_v2.py scrapes that one row directly,
so there is no separate amendment file to prefer/discard here.
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

# =============================== Paths ================================
DATA_DIR  = PROJECT_ROOT / "data" / "Kansas"          # kansas_v2.py's output dir
CLEAN_DIR = PROJECT_ROOT / "data" / "Kansas" / "cleaned"
MANIFEST  = DATA_DIR / "manifest_v2.csv"

SUMMARY_CSV = DATA_DIR / "candidates_summary.csv"
SCHED_A_CSV = DATA_DIR / "schedule_a_contributions.csv"
SCHED_B_CSV = DATA_DIR / "schedule_b_inkind.csv"
SCHED_C_CSV = DATA_DIR / "schedule_c_expenditures.csv"
# schedule_d_other.csv (loans/other transactions) is skipped — same as
# Schedule D in the old PDF parser; it isn't part of the canonical
# contributions/expenditures schema.

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "KS"
EARLIEST_YEAR  = 2013          # earliest R&E filing cycle (2014 statewide covers 2011-2014)
MAX_VALID_YEAR = date.today().year + 4

# ======================== Date / amount helpers =======================
# Same normalization rules as the old PDF parser, so both pipelines'
# outputs stay consistent even though the input format is different.

def _parse_date(val: str) -> str:
    """MM/DD/YY or MM/DD/YYYY → YYYY-MM-DD. Returns '' on failure."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            d = datetime.strptime(v, fmt).date()
            if EARLIEST_YEAR <= d.year <= MAX_VALID_YEAR:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_amount(val: str) -> str:
    """kansas_v2.py already wrote amounts as plain floats (e.g. '1400.0')
    via its money_to_float(); this just re-normalizes to the same
    plain-decimal string format the old parser emitted, and handles the
    empty-string case (kansas_v2.py writes '' when a cell was blank)."""
    v = (val or "").strip()
    if not v:
        return ""
    try:
        return str(float(v))
    except ValueError:
        return ""


def _clean(val: str) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


def _election_year_clean(val: str) -> str:
    """'2022-special' → '2022'; '2026' → '2026'. Strips non-numeric suffixes."""
    m = re.match(r"(\d{4})", (val or "").strip())
    return m.group(1) if m else val


def _split_candidate_uid(candidate_uid: str) -> dict:
    """
    Decode a candidate_uid ("office_group|cycle_label|office_sought|
    district_number|name|original_date|amendment_date") back into its
    parts. This is the same string make_candidate_key() built in
    kansas_v2.py — splitting it is cheaper and more reliable than
    re-deriving office_group/cycle_label by joining against manifest_v2.csv.
    Returns all-empty-string parts if candidate_uid doesn't have the
    expected shape (e.g. blank/malformed row).
    """
    parts = (candidate_uid or "").split("|")
    keys = ("office_group", "cycle_label", "office_sought", "district_number",
            "name", "original_date", "amendment_date")
    if len(parts) != len(keys):
        return {k: "" for k in keys}
    return dict(zip(keys, parts))


def _split_name(full: str) -> tuple[str, str]:
    """Split a candidate's name into (first, last) heuristically — 'Last,
    First' if a comma is present, otherwise 'First ... Last' by token
    position. Identical heuristic to the old parser, for consistent
    candidates.csv.gz output across both pipelines."""
    if "," in full:
        parts = full.split(",", 1)
        return _clean(parts[1]), _clean(parts[0])   # (first, last)
    tokens = full.split()
    first = tokens[0] if tokens else ""
    last  = tokens[-1] if len(tokens) > 1 else ""
    return first, last


def _normalize_district(raw: str) -> str:
    """Strip leading zeros for consistency (e.g. '005' -> '5'), same as
    the old parser. Falls back to the raw value if it isn't numeric."""
    try:
        return str(int(raw))
    except (ValueError, TypeError):
        return raw or ""


# ========================== CSV loading ===============================

def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ============================== run ==================================

def run():
    log = get_logger("kansas", "parse")
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
    # ── Load kansas_v2.py's output CSVs ────────────────────────────────
    if not SUMMARY_CSV.exists():
        print("ERROR: candidates_summary.csv not found — run kansas_v2.py first")
        sys.exit(1)

    summary_rows = _read_csv(SUMMARY_CSV)
    sched_a_rows = _read_csv(SCHED_A_CSV)
    sched_b_rows = _read_csv(SCHED_B_CSV)
    sched_c_rows = _read_csv(SCHED_C_CSV)

    if MANIFEST.exists():
        manifest_rows = _read_csv(MANIFEST)
        log.info(f"  {len(summary_rows):,} candidates scraped "
                 f"({len(manifest_rows):,} in manifest)")
    else:
        log.info(f"  {len(summary_rows):,} candidates scraped (no manifest_v2.csv found)")

    # ── Build the candidate lookup: candidate_uid -> resolved metadata ──
    # One entry per row in candidates_summary.csv. office_group/election_year
    # come from candidate_uid itself (see _split_candidate_uid), NOT from a
    # manifest join — kansas_v2.py already embeds them in every row.
    cand_by_uid: dict[str, dict] = {}
    for row in summary_rows:
        uid = row.get("candidate_uid", "")
        if not uid:
            continue
        parts = _split_candidate_uid(uid)
        cand_name = utils.clean_name(_clean(row.get("candidate_name", "")))
        office    = _clean(row.get("office_sought", "")) or parts["office_sought"]
        district  = _normalize_district(row.get("district", "") or parts["district_number"])
        election_year = _election_year_clean(parts["cycle_label"])
        # candidate_uid's amendment_date component is populated by the
        # scraper only when the CFR Examiner grid shows this filing as
        # amended — that's a real, known signal, unlike filing_id (no
        # confirmed stable filing identifier exists in the scraped data,
        # so that column is left blank/restval "" downstream).
        amended = "1" if parts["amendment_date"] else ""
        cand_by_uid[uid] = {
            "candidate_name": cand_name,
            "office":         office,
            "district":       district,
            "election_year":  election_year,
            "office_group":   parts["office_group"],
            "raw_file":       row.get("source_url", ""),
            "amended":        amended,
            # Candidate committees share the candidate's own address —
            # populate committees.csv.gz's city/zip from it below.
            "city":           _clean(row.get("city", "")),
            "zip":            utils.clean_zip(row.get("zip", "")),
        }

    # ── Output writers ─────────────────────────────────────────────────
    contrib_path = CLEAN_DIR / "contributions.csv.gz"
    expend_path  = CLEAN_DIR / "expenditures.csv.gz"
    cand_path    = CLEAN_DIR / "candidates.csv.gz"
    comm_path    = CLEAN_DIR / "committees.csv.gz"

    contrib_f = gzip.open(contrib_path, "wt", newline="", encoding="utf-8")
    expend_f  = gzip.open(expend_path,  "wt", newline="", encoding="utf-8")

    contrib_w = csv.DictWriter(contrib_f, fieldnames=C.CONTRIBUTIONS,
                               extrasaction="ignore", restval="")
    expend_w  = csv.DictWriter(expend_f,  fieldnames=C.EXPENDITURES,
                               extrasaction="ignore", restval="")
    contrib_w.writeheader()
    expend_w.writeheader()

    contrib_rownum = 0
    expend_rownum  = 0
    unmatched = 0

    # ── Contributions: Schedule A rows as-is, Schedule B rows tagged
    # "In-Kind" (same convention as the old parser, which fed Schedule B
    # through the same contribution path and just overrode transaction_type).
    def _write_contribution_rows(rows: list[dict], forced_type: str):
        nonlocal contrib_rownum, unmatched
        for txn in rows:
            uid = txn.get("candidate_uid", "")
            cand = cand_by_uid.get(uid)
            if cand is None:
                unmatched += 1
                continue
            amount = _parse_amount(txn.get("amount") or txn.get("value", ""))
            txn_date = _parse_date(txn.get("date", ""))
            if not amount or not txn_date:
                continue
            contrib_rownum += 1
            contrib_w.writerow({
                "state":             STATE,
                "committee_name":    cand["candidate_name"],
                "amount":            amount,
                "date":              txn_date,
                "transaction_type":  forced_type or txn.get("type_of_payment", ""),
                "contributor_name":  _clean(txn.get("contributor_name", "")),
                "contributor_city":  _clean(txn.get("contributor_city", "")),
                "contributor_state": _clean(txn.get("contributor_state", "")),
                "contributor_zip":   utils.clean_zip(txn.get("contributor_zip", "")),
                # Schedule B has no separate "occupation" concept beyond the
                # in-kind description; fall back to it when occupation is blank.
                "occupation":        _clean(txn.get("occupation", "")) or _clean(txn.get("description", "")),
                "candidate_name":    cand["candidate_name"],
                "office":            cand["office"],
                "election_year":     cand["election_year"],
                "amended":           cand["amended"],
                "raw_file":          cand["raw_file"],
                "row_num":           contrib_rownum,
            })

    _write_contribution_rows(sched_a_rows, "")
    _write_contribution_rows(sched_b_rows, "In-Kind")

    # ── Expenditures: Schedule C rows ───────────────────────────────────
    for txn in sched_c_rows:
        uid = txn.get("candidate_uid", "")
        cand = cand_by_uid.get(uid)
        if cand is None:
            unmatched += 1
            continue
        amount = _parse_amount(txn.get("amount", ""))
        txn_date = _parse_date(txn.get("date", ""))
        if not amount or not txn_date:
            continue
        expend_rownum += 1
        expend_w.writerow({
            "state":          STATE,
            "committee_name": cand["candidate_name"],
            "amount":         amount,
            "date":           txn_date,
            "payee_name":     _clean(txn.get("payee_name", "")),
            "purpose":        _clean(txn.get("purpose_raw", "")),
            "payee_city":     _clean(txn.get("payee_city", "")),
            "payee_state":    _clean(txn.get("payee_state", "")),
            "payee_zip":      utils.clean_zip(txn.get("payee_zip", "")),
            "candidate_name": cand["candidate_name"],
            "office":         cand["office"],
            "election_year":  cand["election_year"],
            "amended":        cand["amended"],
            "raw_file":       cand["raw_file"],
            "row_num":        expend_rownum,
        })

    contrib_f.close()
    expend_f.close()
    log.info(f"  Contributions: {contrib_rownum:,}   Expenditures: {expend_rownum:,}   "
             f"Unmatched (no candidate_uid match): {unmatched}")

    # ── Write candidates.csv.gz ────────────────────────────────────────
    # Dedup key mirrors the old parser: a candidate can file multiple R&E
    # periods within the same cycle, but they collapse to one candidates.csv
    # row (name, office, district, election_year) — NOT one row per
    # candidate_uid, since candidate_uid varies per filing period.
    candidates_seen: dict[tuple, dict] = {}
    for uid, cand in cand_by_uid.items():
        cand_key = (cand["candidate_name"], cand["office"], cand["district"], cand["election_year"])
        if cand_key not in candidates_seen:
            candidates_seen[cand_key] = {
                "state":          STATE,
                "candidate_name": cand["candidate_name"],
                "office":         cand["office"],
                "district":       cand["district"],
                "election_year":  cand["election_year"],
                "party":          "",   # not available in source
                "state_filer_id": "",   # not available in source
                "raw_file":       cand["raw_file"],
                "row_num":        "",
                # carried through to committees.csv.gz below, not part of
                # C.CANDIDATES itself (extrasaction="ignore" drops it there)
                "_city":          cand["city"],
                "_zip":           cand["zip"],
            }

    cand_rows = []
    for ri, (key, meta_row) in enumerate(candidates_seen.items(), start=1):
        first, last = _split_name(meta_row["candidate_name"])
        cand_rows.append({
            **meta_row,
            "person_id":       "",   # filled by assign_person_ids
            "candidate_first": first,
            "candidate_last":  last,
            "incumbent":       "",
            "jurisdiction":    "",
            "row_num":         ri,
        })

    with gzip.open(cand_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.CANDIDATES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(cand_rows)

    n_cands = utils.assign_person_ids(cand_path, id_model="name_hash")
    log.info(f"  Candidates: {n_cands:,}")

    # ── Write committees.csv.gz ────────────────────────────────────────
    # One committee row per candidate (their own campaign), same as the
    # old parser — Kansas has no separate committee filer concept.
    # city/zip come from the candidate's own address (candidates_summary.csv)
    # since a candidate committee's address is the candidate's address.
    # treasurer_name/active aren't available in the scraped data — left blank.
    with gzip.open(comm_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.COMMITTEES, extrasaction="ignore", restval="")
        w.writeheader()
        for ri, row in enumerate(cand_rows, start=1):
            w.writerow({
                "state":          STATE,
                "person_id":      "",   # filled by assign_committee_person_ids
                "committee_name": row["candidate_name"],
                "committee_type": "Candidate",
                "election_year":  row["election_year"],
                "candidate_name": row["candidate_name"],
                "city":           row.get("_city", ""),
                "zip":            row.get("_zip", ""),
                "state_filer_id": "",
                "raw_file":       row["raw_file"],
                "row_num":        ri,
            })

    n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
    log.info(f"  Committees: {len(cand_rows):,}  (matched {n_comm_matched:,} to candidates)")

    duration = round(time.perf_counter() - t0, 1)
    log._emit("parse_completed",
              status="completed",
              duration_s=duration,
              contributions=contrib_rownum,
              expenditures=expend_rownum,
              candidates=n_cands,
              unmatched=unmatched)
    log.info(f"Done in {duration}s")


# ============================= CLI ===================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)