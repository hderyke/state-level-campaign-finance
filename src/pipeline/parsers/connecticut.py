"""
parsers/connecticut.py — Parse Connecticut SEEC eCRIS campaign finance data.

Reads from data/Connecticut/raw/ and writes normalized output to
data/Connecticut/cleaned/.

Raw files consumed:
  receipts_calendar_partypac_{year}.csv         — contributions from Party/PAC cmtes
  receipts_election_candidateexploratory_{year}.csv — contributions from Candidate/
                                                       Exploratory cmtes; also source
                                                       for the candidates table
  disbursements_calendar_partypac_{year}.csv    — expenditures from Party/PAC cmtes
  disbursements_election_candidateexploratory_{year}.csv — expenditures from
                                                            Candidate/Exploratory cmtes
  committee_history.csv                         — scraped entity data (treasurer, city,
                                                  zip, active status); may be partial
                                                  if the entity sweep is still running

Notes:
  - State column has heavy trailing whitespace — stripped on every row.
  - Zip codes have a trailing dash ("06238-") — stripped.
  - Receipt files have Contributor First/Middle/Last split; combined "Contributor Name"
    is used as fallback when the split fields are all empty (e.g. committee transfers).
  - Candidate/Exploratory receipt files add Candidate First/Middle/Last and Description
    columns not present in Party/PAC files; parsed via .get() so both schemas work.
  - Disbursements use "Status" (Original/Amendment) as the amended flag; receipts use
    "Refiled Electronically" (YES/NO).
  - id_model="committee": CT assigns one Committee ID per registration cycle, so the
    same candidate gets a different ID each cycle. assign_person_ids groups by
    (state, candidate_name, office, district) across cycles.
"""

import csv
import gzip
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Connecticut" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Connecticut" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "CT"

# ============================== helpers ===============================

def clean(val) -> str:
    return (val or "").strip()


def clean_zip(val) -> str:
    """Strip trailing dash from CT zip codes like '06238-'."""
    return clean(val).rstrip("-").strip()


def clean_state(val) -> str:
    """CT state column has massive trailing whitespace."""
    return clean(val)


def parse_amount(val) -> str:
    v = clean(val).replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val) -> str:
    """Normalize a date string to YYYY-MM-DD. Returns '' on failure or implausible year.
    Handles MM/DD/YYYY (standard CSV files) and YYYY-MM-DD HH:MM:SS (openpyxl
    datetime serialization in XLSX-converted 2022-2023 files)."""
    v = clean(val)
    if not v:
        return ""
    # openpyxl serializes date cells as "YYYY-MM-DD HH:MM:SS" — truncate to date
    if len(v) > 10 and v[10] == " ":
        v = v[:10]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > date.today().year + 2:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def parse_year(val) -> str:
    """Return a 4-digit year string or ''."""
    v = clean(val)
    if re.fullmatch(r"\d{4}", v):
        return v
    # Try parsing as a date and extracting the year
    d = parse_date(v)
    return d[:4] if d else ""


def build_name(first, mi, last) -> str:
    """Assemble 'First [M.] Last'. Falls back to last alone for org names."""
    first = clean(first)
    mi    = clean(mi).rstrip(".")
    last  = clean(last)
    if not first:
        return last  # organization or committee — name is in last/combined field
    parts = [first]
    if mi:
        parts.append(mi + ".")
    parts.append(last)
    return " ".join(parts)


def yn_amended(val) -> str:
    """Receipts: Refiled Electronically YES/NO → '1'/'0'."""
    return "1" if clean(val).upper() == "YES" else "0"


def status_amended(val) -> str:
    """Disbursements: Status 'Amendment' → '1', 'Original' → '0'."""
    return "1" if clean(val).lower() == "amendment" else "0"


def raw_files(pattern: str) -> list[Path]:
    """Return non-empty raw files matching pattern, sorted by name (i.e. year)."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ========================= committee registry ========================

def build_committee_registry() -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Scan all transaction files and return two registries:

    by_id   — keyed by Committee ID (2014+ files which include that column).
              First occurrence wins.
    by_name — keyed by utils.clean_name(committee_name) for rows that have no
              Committee ID (2010-2013 files). Used to link pre-2014 committees
              to history entries via name matching. Skips names already covered
              by by_id so there are no duplicates.
    """
    by_id:   dict[str, dict] = {}
    by_name: dict[str, dict] = {}

    import re as _re
    _year_pat = _re.compile(r'_(\d{4})\.csv$')

    for pattern in (
        "receipts_calendar_partypac_*.csv",
        "receipts_election_candidateexploratory_*.csv",
        "disbursements_calendar_partypac_*.csv",
        "disbursements_election_candidateexploratory_*.csv",
    ):
        for path in raw_files(pattern):
            # Extract election_year from election-type filenames; calendar files don't
            # map cleanly to a single election cycle so we leave election_year blank.
            is_election = "election_" in path.name
            m = _year_pat.search(path.name)
            file_year = int(m.group(1)) if (m and is_election) else None

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                reader.fieldnames = [k.strip() for k in (reader.fieldnames or [])]
                for row in reader:
                    name = clean(row.get("Committee", ""))
                    cid  = clean(row.get("Committee ID", ""))
                    rec  = {
                        "committee_name": name,
                        "committee_type": clean(row.get("Committee Type", "")),
                        "election_year":  file_year,
                    }
                    if cid:
                        if cid not in by_id:
                            by_id[cid] = rec
                    elif name:
                        norm = utils.clean_name(name)
                        if norm and norm not in by_name:
                            by_name[norm] = rec

    return by_id, by_name


def load_committee_history() -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Load committee_history.csv and return two indexes:

    by_id   — keyed by committee_id (str)
    by_name — keyed by utils.clean_name(committee_name), for name-based
              matching of pre-2014 committees that have no Committee ID in
              the transaction files.

    Returns empty dicts if the file doesn't exist (entity sweep still running).
    """
    by_id:   dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    path = RAW_DIR / "committee_history.csv"
    if not path.exists():
        return by_id, by_name
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            cid  = clean(row.get("committee_id", ""))
            name = utils.clean_name(row.get("committee_name", ""))
            if cid:
                by_id[cid] = row
            if name and name not in by_name:
                by_name[name] = row
    return by_id, by_name


# ============================== run() =================================

def run():
    log = get_logger("connecticut", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, cand_fh, cmte_fh, loan_fh]

        # ─────────────────────────── receipts ──────────────────────────
        # Candidates registry: keyed by (committee_id, election_year) to
        # capture one record per committee per cycle. First occurrence wins.
        candidates: dict[tuple[str, str], dict] = {}

        for pattern in (
            "receipts_calendar_partypac_*.csv",
            "receipts_election_candidateexploratory_*.csv",
        ):
            is_candidate_file = "candidateexploratory" in pattern

            for path in raw_files(pattern):
                ft = time.perf_counter()
                rows_in_file = 0

                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    # Strip stray whitespace from header keys (e.g. "Committee " in 2010)
                    reader.fieldnames = [k.strip() for k in (reader.fieldnames or [])]
                    for row_num, row in enumerate(reader, start=2):
                        # ── contributions ──
                        contrib_first = clean(row.get("Contributor First Name", ""))
                        contrib_mi    = clean(row.get("Contributor Middle Initial", ""))
                        contrib_last  = clean(row.get("Contributor Last Name", ""))
                        contributor   = build_name(contrib_first, contrib_mi, contrib_last)
                        if not contributor:
                            contributor = clean(row.get("Contributor Name", ""))

                        cand_first = clean(row.get("Candidate First Name", ""))
                        cand_mi    = clean(row.get("Candidate Middle Intial", ""))  # sic
                        cand_last  = clean(row.get("Candidate Last Name", ""))
                        candidate_name = build_name(cand_first, cand_mi, cand_last)

                        # Fall back to "File To State" (report filing date) when
                        # Transaction Date is missing — common in 2010-2013 source files.
                        txn_date = parse_date(row.get("Transaction Date", "")) or \
                                   parse_date(row.get("File To State", ""))

                        receipt_type = clean(row.get("Receipt Type", ""))
                        cont_w.writerow({
                            "state":             STATE,
                            "committee_name":    clean(row.get("Committee", "")),
                            "amount":            parse_amount(row.get("Amount", "")),
                            "date":              txn_date,
                            "transaction_type":  receipt_type,
                            # contributor_type mirrors transaction_type so the
                            # contributor_types.csv aliases can normalize it.
                            "contributor_type":  receipt_type,
                            "contributor_name":  contributor,
                            "contributor_city":  clean(row.get("City", "")),
                            "contributor_state": clean_state(row.get("State", "")),
                            "contributor_zip":   clean_zip(row.get("zip", "")),
                            "employer":          clean(row.get("Employer", "")),
                            "occupation":        clean(row.get("Occupation", "")),
                            "candidate_name":    candidate_name,
                            "office":            clean(row.get("Office Sought", "")),
                            "election_year":     parse_year(row.get("ElectionYear", "")),
                            "amended":           yn_amended(row.get("Refiled Electronically", "")),
                            "filing_id":         clean(row.get("Report ID", "")),
                            "raw_file":          path.name,
                            "row_num":           row_num,
                        })
                        total_contributions += 1
                        rows_in_file += 1

                        # ── candidates (candidate/exploratory files only) ──
                        if is_candidate_file and cand_last:
                            cid       = clean(row.get("Committee ID", ""))
                            elec_year = parse_year(row.get("ElectionYear", ""))
                            key       = (cid, elec_year)
                            if key not in candidates:
                                candidates[key] = {
                                    "state":          STATE,
                                    "candidate_name": utils.clean_name(candidate_name),
                                    "candidate_first": cand_first,
                                    "candidate_last":  cand_last,
                                    "office":          clean(row.get("Office Sought", "")),
                                    "district":        clean(row.get("District", "")),
                                    "election_year":   elec_year,
                                    "state_filer_id":  cid,
                                    "raw_file":        path.name,
                                    "row_num":         row_num,
                                }

                log.file_parsed(path.name, "contributions", rows_in_file,
                                duration_s=round(time.perf_counter() - ft, 2),
                                bytes=path.stat().st_size)

        # ─────────────────────────── disbursements ─────────────────────
        for pattern in (
            "disbursements_calendar_partypac_*.csv",
            "disbursements_election_candidateexploratory_*.csv",
        ):
            for path in raw_files(pattern):
                ft = time.perf_counter()
                rows_in_file = 0

                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    # Strip stray whitespace from header keys (e.g. "Committee " in 2010)
                    reader.fieldnames = [k.strip() for k in (reader.fieldnames or [])]
                    for row_num, row in enumerate(reader, start=2):
                        expn_w.writerow({
                            "state":          STATE,
                            "committee_name": clean(row.get("Committee", "")),
                            "amount":         parse_amount(row.get("Amount", "")),
                            "date":           parse_date(row.get("Payment Date", "")),
                            "transaction_type": clean(row.get("Purpose of Expenditure", "")),
                            "payee_name":     clean(row.get("Payee", "")),
                            "purpose":        clean(row.get("Description", "")),
                            "payee_city":     clean(row.get("City", "")),
                            "payee_state":    clean_state(row.get("State", "")),
                            "election_year":  parse_year(row.get("Election Year", "")),
                            "amended":        status_amended(row.get("Status", "")),
                            "filing_id":      clean(row.get("Report ID", "")),
                            "raw_file":       path.name,
                            "row_num":        row_num,
                        })
                        total_expenditures += 1
                        rows_in_file += 1

                log.file_parsed(path.name, "expenditures", rows_in_file,
                                duration_s=round(time.perf_counter() - ft, 2),
                                bytes=path.stat().st_size)

        # ─────────────────────────── candidates ────────────────────────
        for row_num, rec in enumerate(candidates.values(), start=2):
            cand_w.writerow(rec)
            candidates_written += 1

        # ─────────────────────────── committees ────────────────────────
        log.info("Building committee registry...")
        registry_by_id, registry_by_name = build_committee_registry()
        history_by_id,  history_by_name  = load_committee_history()

        if not history_by_id:
            log.info(
                "  committee_history.csv not found or empty — "
                "treasurer/city/zip/active will be blank (entity sweep may still be running)"
            )
        else:
            log.registry_loaded("committee_history.csv",
                                 entries=len(history_by_id), relation="committees")

        # candidate_name lookup from transaction data — fills in candidate name for
        # 2014+ candidate committees independently of the history sweep completeness.
        # Keyed by Committee ID; first occurrence across all election years wins.
        candidate_by_cid: dict[str, str] = {}
        for (cid, _year), rec in candidates.items():
            if cid and cid not in candidate_by_cid:
                candidate_by_cid[cid] = rec["candidate_name"]

        def _active(h: dict) -> str:
            s = clean(h.get("status", ""))
            if s == "ACTIVE":         return "1"
            if s.startswith("TERMINATED"): return "0"
            return ""

        def _write_committee(row_num: int, cid: str, cmte: dict, h: dict) -> None:
            # candidate_name: prefer history page (has it for candidate committees),
            # fall back to what we derived from receipt file candidate columns.
            cand_name = utils.clean_name(h.get("candidate_name", "")) \
                        or candidate_by_cid.get(cid, "")
            cmte_w.writerow({
                "state":          STATE,
                "committee_name": utils.clean_name(cmte["committee_name"]),
                "committee_type": cmte["committee_type"],
                "election_year":  cmte.get("election_year") or "",
                "candidate_name": cand_name,
                "treasurer_name": clean(h.get("treasurer_name", "")),
                "city":           clean(h.get("city", "")),
                "zip":            clean(h.get("zip", "")),
                "active":         _active(h),
                "state_filer_id": cid,
                "raw_file":       "committee_history.csv" if h else "",
                "row_num":        row_num,
            })

        row_num = 2
        written_cids: set[str] = set()

        # 1. ID-keyed committees (2014+) — enriched by history ID lookup
        for cid, cmte in registry_by_id.items():
            h = history_by_id.get(cid, {})
            _write_committee(row_num, cid, cmte, h)
            written_cids.add(cid)
            committees_written += 1
            row_num += 1

        # 2. Name-keyed committees (pre-2014, no Committee ID in source files).
        #    Only written when the history name-match resolves to a valid committee_id
        #    so that state_filer_id is always populated (required field). Pre-2014
        #    committees without a history match are left out of the committees table;
        #    they still appear in contributions but without a person_id link.
        for norm_name, cmte in registry_by_name.items():
            h   = history_by_name.get(norm_name, {})
            cid = clean(h.get("committee_id", ""))
            if not cid:
                continue  # no history match → skip (no valid state_filer_id)
            if cid in written_cids:
                continue  # already written via ID-based path
            _write_committee(row_num, cid, cmte, h)
            written_cids.add(cid)
            committees_written += 1
            row_num += 1

        # loans_debts: CT has no loan data in these files — write empty file
        # (header already written by open_writer)

        # ─────────── close handles before person-ID assignment ─────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # ───────────────────── person IDs ──────────────────────────────
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz",
                                id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        # ───────────────────── output stats ────────────────────────────
        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions",
                        total_contributions, role="output",
                        bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz", "expenditures",
                        total_expenditures, role="output",
                        bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("candidates.csv.gz", "candidates",
                        candidates_written, role="output",
                        bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz", "committees",
                        committees_written, role="output",
                        bytes=_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz", "loans_debts",
                        0, role="output",
                        bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=committees_written,
                  candidates=candidates_written)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=committees_written,
                  candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=committees_written,
                  candidates=candidates_written,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


# ================================ CLI =================================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
