"""
parsers/louisiana.py — Parse Louisiana campaign finance CSVs into the
canonical cleaned schema.

Input files (data/Louisiana/raw/):
    contributions_{slug}.csv   — 9 files covering 1995-present
    expenditures_{slug}.csv    — 9 files covering 1995-present
    loans_{slug}.csv           — 9 files covering 1995-present

Output (data/Louisiana/cleaned/):
    contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
    candidates.csv.gz, committees.csv.gz

id_model = "person"
    FilerNumber is a stable person-level ID assigned by the Ethics Board —
    the same filer keeps the same number across cycles and committee types.
    person_id = FIPS_prefix(22) + int(FilerNumber).

Source-file quirks:
    - UTF-8 BOM present on all files; opened with encoding="utf-8-sig".
    - Date format: "M/D/YYYY 12:00:00 AM" — time portion stripped before parsing.
    - "ContributorrState" (double-r) is the actual column name in contributions.
    - Trailing whitespace in ReportType and ReportCode fields (e.g. "F104    ").
    - FilerFirstName is blank for PAC/committee filers; non-blank for candidates.

Candidate vs. committee classification:
    FilerFirstName non-blank → candidate (F1xx report forms)
    FilerFirstName blank     → PAC or committee (F2xx report forms)

Column mapping summary:
  Contributions:
    FilerNumber                     → state_filer_id (committee side)
    FilerLastName + FilerFirstName  → committee_name / candidate_name
    ContributorTypeCode             → contributor_type (IND, BUS, CAN, ...)
    ContributorName                 → contributor_name
    ContributorCity/State/Zip       → contributor_city/state/zip
    ContributionType                → transaction_type (CONTRIB, ...)
    ContributionDate                → date
    ContributionAmt                 → amount
    ReportNumber                    → filing_id
    ReportCode                      → (informational; election period type)

  Expenditures:
    FilerNumber / FilerLastName+First → committee_name
    Schedule                        → transaction_type (E-1, E-2, B, ...)
    RecipientName                   → payee_name
    RecipientCity/State/Zip         → payee_city/state/zip
    ExpenditureDescription          → purpose
    CandidateBeneficiary            → candidate_name (independent expenditures)
    ExpenditureDate                 → date
    ExpenditureAmt                  → amount
    ReportNumber                    → filing_id

  Loans:
    FilerNumber / FilerLastName+First → committee_name
    LoanHolderName                  → counterparty_name
    LoanHolderCity/State/Zip        → counterparty_city/state/zip
    LoanDate                        → date
    LoanAmt                         → original_amount
    record_type                     → "LOAN" (literal)
    ReportNumber                    → filing_id
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
RAW_DIR   = PROJECT_ROOT / "data" / "Louisiana" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Louisiana" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE         = "LA"
EARLIEST_YEAR = 1995
MAX_YEAR      = date.today().year + 4

# ========================= helpers ====================================

def _clean(val) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


def _parse_date(val: str) -> str:
    """'M/D/YYYY 12:00:00 AM' or 'M/D/YYYY' → 'YYYY-MM-DD'. Returns '' on failure."""
    v = _clean(val)
    if not v:
        return ""
    # Strip time portion if present ("9/26/2026 12:00:00 AM" → "9/26/2026")
    v = v.split()[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt).date()
            if EARLIEST_YEAR - 1 <= d.year <= MAX_YEAR:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_year(val: str) -> str:
    """Extract 4-digit year from a date string. Returns '' on failure."""
    d = _parse_date(val)
    return d[:4] if d else ""


def _parse_amount(val: str) -> str:
    v = _clean(val).replace("$", "").replace(",", "")
    if not v:
        return ""
    try:
        float(v)
        return v
    except ValueError:
        return ""


def _filer_name(last: str, first: str) -> str:
    """Build display name from FilerLastName / FilerFirstName."""
    last, first = _clean(last), _clean(first)
    if first and last:
        return f"{first} {last}"
    return last or first


def _clean_state(val: str) -> str:
    """Return a 2-letter uppercase state abbreviation or ''."""
    s = _clean(val).upper()
    if not s or s in ("N/A", "NA", "0") or s.isdigit():
        return ""
    return s


def _raw_files(pattern: str) -> list[Path]:
    """Return sorted non-empty raw files matching glob pattern."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


# ============================== run ===================================

def run():
    log = get_logger("louisiana", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    n_contributions = 0
    n_expenditures  = 0
    n_loans         = 0
    n_candidates    = 0
    n_committees    = 0
    file_handles    = []

    try:
        cont_fh = gzip.open(CLEAN_DIR / "contributions.csv.gz", "wt",
                            encoding="utf-8", newline="")
        expn_fh = gzip.open(CLEAN_DIR / "expenditures.csv.gz",  "wt",
                            encoding="utf-8", newline="")
        loan_fh = gzip.open(CLEAN_DIR / "loans_debts.csv.gz",   "wt",
                            encoding="utf-8", newline="")
        file_handles = [cont_fh, expn_fh, loan_fh]

        cont_w = csv.DictWriter(cont_fh, fieldnames=C.CONTRIBUTIONS,
                                extrasaction="ignore", restval="")
        expn_w = csv.DictWriter(expn_fh, fieldnames=C.EXPENDITURES,
                                extrasaction="ignore", restval="")
        loan_w = csv.DictWriter(loan_fh, fieldnames=C.LOANS_DEBTS,
                                extrasaction="ignore", restval="")
        cont_w.writeheader()
        expn_w.writeheader()
        loan_w.writeheader()

        # filers dict: FilerNumber → {last, first, is_candidate}
        # Populated during each parsing pass; used to write candidates/committees.
        filers: dict[str, dict] = {}

        def _update_filer(fnum: str, last: str, first: str) -> None:
            if fnum and fnum not in filers:
                filers[fnum] = {
                    "last":         last,
                    "first":        first,
                    "is_candidate": bool(_clean(first)),
                }

        # ── 1. Contributions ───────────────────────────────────────────
        log.info("  Parsing contributions…")
        for raw_file in _raw_files("contributions_*.csv"):
            rows_in = rows_out = rows_skipped = 0
            seen_rows: set[tuple] = set()
            with open(raw_file, newline="", encoding="utf-8-sig",
                      errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows_in += 1
                    # Skip fully identical rows (source export duplicates)
                    row_key = tuple(row.get(col, "") for col in (reader.fieldnames or []))
                    if row_key in seen_rows:
                        rows_skipped += 1
                        continue
                    seen_rows.add(row_key)

                    fnum  = _clean(row.get("FilerNumber", ""))
                    last  = _clean(row.get("FilerLastName", ""))
                    first = _clean(row.get("FilerFirstName", ""))
                    _update_filer(fnum, last, first)

                    name   = _filer_name(last, first)
                    amount = _parse_amount(row.get("ContributionAmt", ""))
                    dt     = _parse_date(row.get("ContributionDate", ""))
                    if not amount or not dt:
                        continue

                    n_contributions += 1
                    rows_out += 1
                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    utils.clean_name(name),
                        "amount":            amount,
                        "date":              dt,
                        # ContributionType distinguishes CONTRIB / INKIND / etc.
                        "transaction_type":  _clean(row.get("ContributionType", "")),
                        "contributor_name":  _clean(row.get("ContributorName", "")),
                        "contributor_type":  _clean(row.get("ContributorTypeCode", "")),
                        "contributor_city":  _clean(row.get("ContributorCity", "")),
                        # Source column has a typo: "ContributorrState" (double r)
                        "contributor_state": _clean_state(
                            row.get("ContributorrState", "")
                            or row.get("ContributorState", "")
                        ),
                        "contributor_zip":   utils.clean_zip(
                            row.get("ContributorZip", "")
                        ),
                        "employer":          "",
                        "occupation":        "",
                        "candidate_name":    (
                            utils.clean_name(name)
                            if filers.get(fnum, {}).get("is_candidate") else ""
                        ),
                        "office":            "",
                        "election_year":     _parse_year(row.get("ContributionDate", "")),
                        "amended":           "",
                        "filing_id":         _clean(row.get("ReportNumber", "")),
                        "raw_file":          raw_file.name,
                        "row_num":           n_contributions,
                    })

            log.info(f"    {raw_file.name}: {rows_in:,} in → {rows_out:,} out"
                     + (f" ({rows_skipped:,} dupes skipped)" if rows_skipped else ""))

        log.info(f"    → {n_contributions:,} contributions total")

        # ── 2. Expenditures ────────────────────────────────────────────
        log.info("  Parsing expenditures…")
        for raw_file in _raw_files("expenditures_*.csv"):
            rows_in = rows_out = rows_skipped = 0
            seen_rows: set[tuple] = set()
            with open(raw_file, newline="", encoding="utf-8-sig",
                      errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows_in += 1
                    row_key = tuple(row.get(col, "") for col in (reader.fieldnames or []))
                    if row_key in seen_rows:
                        rows_skipped += 1
                        continue
                    seen_rows.add(row_key)

                    fnum  = _clean(row.get("FilerNumber", ""))
                    last  = _clean(row.get("FilerLastName", ""))
                    first = _clean(row.get("FilerFirstName", ""))
                    _update_filer(fnum, last, first)

                    name   = _filer_name(last, first)
                    amount = _parse_amount(row.get("ExpenditureAmt", ""))
                    dt     = _parse_date(row.get("ExpenditureDate", ""))
                    if not amount or not dt:
                        continue

                    n_expenditures += 1
                    rows_out += 1
                    expn_w.writerow({
                        "state":          STATE,
                        "committee_name": utils.clean_name(name),
                        "amount":         amount,
                        "date":           dt,
                        # Schedule distinguishes expenditure types: E-1, E-2, B, etc.
                        "transaction_type": _clean(row.get("Schedule", "")),
                        "payee_name":     _clean(row.get("RecipientName", "")),
                        "purpose":        _clean(row.get("ExpenditureDescription", "")),
                        "category":       "",
                        "payee_city":     _clean(row.get("RecipientCity", "")),
                        "payee_state":    _clean_state(row.get("RecipientState", "")),
                        "payee_zip":      utils.clean_zip(row.get("RecipientZip", "")),
                        # CandidateBeneficiary is populated for independent expenditures
                        "candidate_name": _clean(row.get("CandidateBeneficiary", "")),
                        "office":         "",
                        "election_year":  _parse_year(row.get("ExpenditureDate", "")),
                        "amended":        "",
                        "filing_id":      _clean(row.get("ReportNumber", "")),
                        "raw_file":       raw_file.name,
                        "row_num":        n_expenditures,
                    })

            log.info(f"    {raw_file.name}: {rows_in:,} in → {rows_out:,} out"
                     + (f" ({rows_skipped:,} dupes skipped)" if rows_skipped else ""))

        log.info(f"    → {n_expenditures:,} expenditures total")

        # ── 3. Loans ───────────────────────────────────────────────────
        log.info("  Parsing loans…")
        for raw_file in _raw_files("loans_*.csv"):
            rows_in = rows_out = rows_skipped = 0
            seen_rows: set[tuple] = set()
            with open(raw_file, newline="", encoding="utf-8-sig",
                      errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows_in += 1
                    row_key = tuple(row.get(col, "") for col in (reader.fieldnames or []))
                    if row_key in seen_rows:
                        rows_skipped += 1
                        continue
                    seen_rows.add(row_key)

                    fnum  = _clean(row.get("FilerNumber", ""))
                    last  = _clean(row.get("FilerLastName", ""))
                    first = _clean(row.get("FilerFirstName", ""))
                    _update_filer(fnum, last, first)

                    name   = _filer_name(last, first)
                    amount = _parse_amount(row.get("LoanAmt", ""))
                    dt     = _parse_date(row.get("LoanDate", ""))
                    if not amount or not dt:
                        continue

                    n_loans += 1
                    rows_out += 1
                    loan_w.writerow({
                        "state":            STATE,
                        "committee_name":   utils.clean_name(name),
                        "original_amount":  amount,
                        "date":             dt,
                        "record_type":      "LOAN",
                        "counterparty_name":  _clean(row.get("LoanHolderName", "")),
                        "counterparty_city":  _clean(row.get("LoanHolderCity", "")),
                        "counterparty_state": _clean_state(
                            row.get("LoanHolderState", "")
                        ),
                        "counterparty_zip":   utils.clean_zip(
                            row.get("LoanHolderZip", "")
                        ),
                        "candidate_name":    (
                            utils.clean_name(name)
                            if filers.get(fnum, {}).get("is_candidate") else ""
                        ),
                        "election_year":     _parse_year(row.get("LoanDate", "")),
                        "amended":           "",
                        "filing_id":         _clean(row.get("ReportNumber", "")),
                        "raw_file":          raw_file.name,
                        "row_num":           n_loans,
                    })

            log.info(f"    {raw_file.name}: {rows_in:,} in → {rows_out:,} out"
                     + (f" ({rows_skipped:,} dupes skipped)" if rows_skipped else ""))

        log.info(f"    → {n_loans:,} loans total")

        # Close transaction writers before writing entity files
        for fh in file_handles:
            fh.close()
        file_handles = []

        # ── 4. Candidates ──────────────────────────────────────────────
        log.info("  Writing candidates…")
        cand_path = CLEAN_DIR / "candidates.csv.gz"
        cand_rows: list[dict] = []

        for fnum, info in sorted(filers.items(), key=lambda kv: kv[0]):
            if not info["is_candidate"]:
                continue
            full_name = _filer_name(info["last"], info["first"])
            if not full_name:
                continue
            cand_rows.append({
                "state":           STATE,
                "person_id":       "",           # filled by assign_person_ids
                "candidate_name":  utils.clean_name(full_name),
                "candidate_first": utils.clean_name(info["first"]),
                "candidate_last":  utils.clean_name(info["last"]),
                "office":          "",
                "district":        "",
                "jurisdiction":    "",
                "party":           "",
                "election_year":   "",
                "incumbent":       "",
                "state_filer_id":  fnum,
                "raw_file":        "filers",
                "row_num":         len(cand_rows) + 1,
            })

        with gzip.open(cand_path, "wt", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=C.CANDIDATES,
                               extrasaction="ignore", restval="")
            w.writeheader()
            w.writerows(cand_rows)

        n_candidates = utils.assign_person_ids(cand_path, id_model="person")
        log.info(f"    → {n_candidates:,} candidates written")

        # ── 5. Committees ──────────────────────────────────────────────
        log.info("  Writing committees…")
        comm_path = CLEAN_DIR / "committees.csv.gz"
        comm_rows: list[dict] = []

        for fnum, info in sorted(filers.items(), key=lambda kv: kv[0]):
            if info["is_candidate"]:
                continue
            committee_name = _clean(info["last"])   # PAC name is in LastName field
            if not committee_name:
                continue
            comm_rows.append({
                "state":          STATE,
                "person_id":      "",              # filled by assign_committee_person_ids
                "committee_name": committee_name,
                "committee_type": "",
                "election_year":  "",
                "candidate_name": "",
                "treasurer_name": "",
                "city":           "",
                "zip":            "",
                "active":         "",
                "state_filer_id": fnum,
                "raw_file":       "filers",
                "row_num":        len(comm_rows) + 1,
            })

        with gzip.open(comm_path, "wt", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=C.COMMITTEES,
                               extrasaction="ignore", restval="")
            w.writeheader()
            w.writerows(comm_rows)

        n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
        n_committees   = len(comm_rows)
        log.info(f"    → {n_committees:,} committees written "
                 f"({n_comm_matched:,} matched to candidates)")

        # ── Log output file stats ──────────────────────────────────────
        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", n_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  n_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   n_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    n_candidates,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    n_committees,
                        role="output", bytes=_bytes("committees.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=n_contributions, expenditures=n_expenditures,
                  loans=n_loans, candidates=n_candidates, committees=n_committees)
        log.info(f"Done in {duration}s")

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=n_contributions, expenditures=n_expenditures,
                  loans=n_loans, candidates=n_candidates, committees=n_committees)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=n_contributions, expenditures=n_expenditures,
                  loans=n_loans, candidates=n_candidates, committees=n_committees,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


# ============================== CLI ===================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
