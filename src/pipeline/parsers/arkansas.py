"""
parsers/arkansas.py — Transform Arkansas raw CSVs into the 5 normalized relations.

Input:  data/Arkansas/raw/
  contributions_{year}.csv    — TCON transactions (2022–present)
  expenditures_{year}.csv     — TEXP transactions (2022–present)
  candidates.csv              — registered candidates (from GetCandidateCommitteDetails)
  committees.csv              — registered committees (PAC, CPAC, IEF, PP, ECOMM)

Output: data/Arkansas/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz (empty — no loan data available)

Notes
─────
  • Address field is a single string "Street, City, ST ZIP" — parsed with regex.
  • Amounts arrive as "$1,000.00" — dollar sign and commas stripped.
  • Non-itemized contributions have no Source Name — rows are kept as aggregate totals.
  • Entity Name for candidates may include committee name in parens:
      "Smith, John  (Smith for Governor)" → candidate_name = "Smith, John"
  • 2022 data is sparse (Nov–Dec only, candidates only) — included as-is.
  • No loans/debts table available from this source.
  • Registry join: Filing Entity ID in transactions = filerEntityID in registry files.
  • person_id model: "person" — filerEntityID is a stable person-level ID that
    persists across election cycles (unlike Alabama/Arizona which re-register per cycle).
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
RAW_DIR   = PROJECT_ROOT / "data" / "Arkansas" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Arkansas" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "AR"
MAX_VALID_YEAR = date.today().year + 2


# ============================== helpers ===============================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'$1,000.00' → '1000.00', '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """MM/DD/YYYY → YYYY-MM-DD, '' on failure or implausible year."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


# Matches "...street..., City, ST 12345" or "..., ST 12345-6789"
_ADDR_RE = re.compile(
    r'^(.*),\s*([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$'
)


def parse_address(addr: str) -> tuple[str, str, str]:
    """
    Parse 'Street, City, ST ZIP' → (city, state, zip).
    Returns ('', '', '') if the pattern doesn't match.
    """
    addr = (addr or "").strip()
    if not addr:
        return "", "", ""
    m = _ADDR_RE.match(addr)
    if not m:
        return "", "", ""
    street_city = m.group(1)
    state       = m.group(2).upper()
    zipcode     = m.group(3)
    # city is the last comma-delimited segment of street_city
    last_comma = street_city.rfind(",")
    city = street_city[last_comma + 1:].strip() if last_comma >= 0 else street_city.strip()
    return city, state, zipcode


def extract_candidate_name(entity_name: str) -> str:
    """
    Strip committee name in parens from Entity Name.
    'Smith, John  (Smith for Governor)' → 'Smith, John'
    """
    name = (entity_name or "").strip()
    paren_idx = name.find("(")
    if paren_idx >= 0:
        name = name[:paren_idx].strip()
    return name


def amended_flag(val: str) -> str:
    v = (val or "").strip().upper()
    if v == "Y":
        return "1"
    if v == "N":
        return "0"
    return ""


def year_from_filename(path: Path) -> str:
    m = re.search(r"(\d{4})", path.name)
    return m.group(1) if m else ""


def raw_files(pattern: str) -> list[Path]:
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


# ================================ run =================================

def run():
    log = get_logger("arkansas", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        # ── Load candidate registry ────────────────────────────────────
        # Keyed by filerEntityID (string) for transaction join
        cand_registry: dict[str, dict] = {}
        cand_path = RAW_DIR / "candidates.csv"
        ft = time.perf_counter()
        cand_count = 0
        if cand_path.exists():
            with open(cand_path, newline="", encoding="utf-8") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    fid        = clean(row.get("filerEntityID", ""))
                    first      = clean(row.get("firstName", ""))
                    last       = clean(row.get("lastName", ""))
                    suffix     = clean(row.get("suffix", ""))
                    filer_name = clean(row.get("filerName", ""))

                    # Prefer split first/last; fall back to filerName
                    if last:
                        cand_name = f"{last}, {first}".rstrip(", ")
                        if suffix:
                            cand_name += f" {suffix}"
                    else:
                        cand_name = extract_candidate_name(filer_name)

                    entry = {
                        "filerEntityID":   fid,
                        "candidate_name":  cand_name,
                        "candidate_first": first,
                        "candidate_last":  last,
                        "office":          clean(row.get("office", "")),
                        "district":        clean(row.get("officeDistrictName", "")),
                        "jurisdiction":    clean(row.get("jurisdictionName", "")),
                        "party":           clean(row.get("politicalParty", "")),
                        "election_year":   clean(row.get("electionYear", ""))
                                           or clean(row.get("filingYear", "")),
                        "status":          clean(row.get("filerStatus", "")),
                    }
                    if fid:
                        cand_registry[fid] = entry

                    cand_w.writerow({
                        "state":           STATE,
                        "state_filer_id":  fid,
                        "candidate_name":  cand_name,
                        "candidate_first": entry["candidate_first"],
                        "candidate_last":  entry["candidate_last"],
                        "office":          entry["office"],
                        "district":        entry["district"],
                        "jurisdiction":    entry["jurisdiction"],
                        "party":           entry["party"],
                        "election_year":   entry["election_year"],
                        "status":          entry["status"],
                        "incumbent":       "",
                        "raw_file":        cand_path.name,
                        "row_num":         row_num,
                    })
                    cand_count += 1

        log.registry_loaded(cand_path.name, cand_count, relation="candidates",
                            bytes=cand_path.stat().st_size if cand_path.exists() else 0)
        candidates_written = cand_count

        # ── Load committee registry ────────────────────────────────────
        cmte_registry: dict[str, dict] = {}
        cmte_path = RAW_DIR / "committees.csv"
        ft = time.perf_counter()
        cmte_count = 0
        if cmte_path.exists():
            with open(cmte_path, newline="", encoding="utf-8") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    fid        = clean(row.get("filerEntityID", ""))
                    filer_name = clean(row.get("filerName", ""))
                    cmte_name  = clean(row.get("committeeName", "")) or filer_name
                    filer_type = clean(row.get("filerType", ""))
                    status     = clean(row.get("filerStatus", ""))
                    # AR committees.csv contains only non-candidate entities (PACs,
                    # party committees, IEs). electionYear from the API reflects
                    # which election they're associated with, not that the committee
                    # itself is cycle-specific — leave election_year blank.
                    elec_year  = ""

                    entry = {
                        "committee_name": cmte_name,
                        "committee_type": filer_type,
                        "election_year":  elec_year,
                        "active":         "1" if status == "Active" else ("0" if status else ""),
                    }
                    if fid:
                        cmte_registry[fid] = entry

                    cmte_w.writerow({
                        "state":          STATE,
                        "state_filer_id": fid,
                        "committee_name": cmte_name,
                        "committee_type": filer_type,
                        "election_year":  elec_year,
                        "candidate_name": "",
                        "treasurer_name": "",
                        "city":           "",
                        "zip":            "",
                        "active":         entry["active"],
                        "raw_file":       cmte_path.name,
                        "row_num":        row_num,
                    })
                    cmte_count += 1

        log.registry_loaded(cmte_path.name, cmte_count, relation="committees",
                            bytes=cmte_path.stat().st_size if cmte_path.exists() else 0)

        # Also write candidate filers as committees — they have campaign accounts
        extra = 0
        for fid, c in cand_registry.items():
            cmte_name = c["candidate_name"]
            cmte_w.writerow({
                "state":          STATE,
                "state_filer_id": fid,
                "committee_name": cmte_name,
                "committee_type": "Candidate",
                "candidate_name": cmte_name,
                "treasurer_name": "",
                "city":           "",
                "zip":            "",
                "active":         "1" if c["status"] == "Active" else "",
                "raw_file":       cand_path.name,
                "row_num":        "",
            })
            extra += 1

        committees_written = cmte_count + extra
        log.info(f"  committees: {cmte_count:,} from registry + {extra:,} candidate accounts")

        # ── Contributions ──────────────────────────────────────────────
        for path in raw_files("contributions_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = skipped = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    fid    = clean(row.get("Filing Entity ID", ""))
                    amount = parse_amount(row.get("Transaction Amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    entity_name = clean(row.get("Entity Name", ""))
                    filer_type  = clean(row.get("FilerType", ""))
                    is_cand     = (filer_type == "Candidate")

                    cmte_name = extract_candidate_name(entity_name) if is_cand else entity_name

                    if is_cand:
                        cand_name = (cand_registry.get(fid) or {}).get("candidate_name", "") \
                                    or extract_candidate_name(entity_name)
                    else:
                        cand_name = ""

                    reg       = cand_registry.get(fid, {}) if is_cand else {}
                    office    = reg.get("office", "")
                    elec_year = clean(row.get("Election Year", "")) \
                                or reg.get("election_year", "") or file_year

                    city, st, zipcode = parse_address(row.get("Source Address", ""))

                    # Prefer Occupation; fall back to Occupation Other when blank
                    occupation = clean(row.get("Occupation", "")) \
                                 or clean(row.get("Occupation Other", ""))

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cmte_name,
                        "contributor_name":  clean(row.get("Source Name", "")),
                        "amount":            amount,
                        "date":              parse_date(row.get("Transaction Date", "")),
                        "transaction_type":  clean(row.get("Transaction Sub Type", "")),
                        "contributor_type":  clean(row.get("Funding Source / Loan Source Type", "")),
                        "contributor_city":  city,
                        "contributor_state": st,
                        "contributor_zip":   zipcode,
                        "employer":          clean(row.get("Employer Name", "")),
                        "occupation":        occupation,
                        "candidate_name":    cand_name,
                        "office":            office,
                        "election_year":     elec_year,
                        "filing_id":         clean(row.get("Transaction ID", "")),
                        "amended":           amended_flag(row.get("Amended", "")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "contributions", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += count

        # ── Expenditures ───────────────────────────────────────────────
        for path in raw_files("expenditures_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = skipped = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    fid    = clean(row.get("Filing Entity ID", ""))
                    amount = parse_amount(row.get("Transaction Amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    entity_name = clean(row.get("Entity Name", ""))
                    filer_type  = clean(row.get("FilerType", ""))
                    is_cand     = (filer_type == "Candidate")

                    cmte_name = extract_candidate_name(entity_name) if is_cand else entity_name

                    if is_cand:
                        cand_name = (cand_registry.get(fid) or {}).get("candidate_name", "") \
                                    or extract_candidate_name(entity_name)
                    else:
                        cand_name = ""

                    reg       = cand_registry.get(fid, {}) if is_cand else {}
                    office    = reg.get("office", "")
                    elec_year = clean(row.get("Election Year", "")) \
                                or reg.get("election_year", "") or file_year

                    city, st, zipcode = parse_address(row.get("Payee Address", ""))

                    # Prefer Transaction Category Others when more specific than the
                    # standardized Transaction Category value
                    category = clean(row.get("Transaction Category Others", "")) \
                               or clean(row.get("Transaction Category", ""))

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "payee_name":       clean(row.get("Payee Name", "")),
                        "amount":           amount,
                        "date":             parse_date(row.get("Transaction Date", "")),
                        "transaction_type": clean(row.get("Transaction Sub Type", "")),
                        "purpose":          clean(row.get("Transaction Description", "")),
                        "category":         category,
                        "payee_city":       city,
                        "payee_state":      st,
                        "payee_zip":        zipcode,
                        "candidate_name":   cand_name,
                        "office":           office,
                        "election_year":    elec_year,
                        "filing_id":        clean(row.get("Transaction ID", "")),
                        "amended":          amended_flag(row.get("Amended", "")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # AR uses stable person-level filerEntityIDs — person_id = state_filer_id directly
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   0,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
