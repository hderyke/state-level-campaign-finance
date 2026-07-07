"""
parsers/maryland.py — Transform Maryland raw CSVs into the 5 normalized relations.

Input:  data/Maryland/raw/
  committees.csv              — all registered committees (TCMD, filingYear=0)
  contributions_{year}.csv    — contributions + loans received (TCON, 2021–present)
  expenditures_{year}.csv     — expenditures + outstanding obligations (TEXP, 2021–present)

Output: data/Maryland/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Notes
─────
  • Each raw file has a timestamp title row on line 0; the real CSV header is
    on line 1. DictReader is initialized after skipping line 0.
  • ZipCode fields use Excel formula quoting (="21074") — stripped by _unquote().
  • Transaction Amount arrives as "$15.00" — dollar sign and commas stripped.
  • Transaction Date arrives as MM/DD/YYYY.
  • TCON files contain both contributions and loan transactions. Rows whose
    Transaction Type contains "Loan" are routed to loans_debts; all others
    go to contributions.
  • Candidate info (name, office, party) is extracted from the committee file
    for rows where Committee Type == "Candidate" and a Candidate LastName or
    FirstName is present. These become both a candidates row and a committee row.
  • Non-candidate committees (PAC, Party, Slate, etc.) get a committee row only.
  • person_id model: "committee" — Maryland assigns a new Filing Entity Id per
    committee registration; the same candidate gets a different ID each cycle.
    assign_person_ids groups by (state, candidate_name, office, district) and
    assigns person_id = min(state_filer_id) across all registrations.
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

# ================================ paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Maryland" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Maryland" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "MD"
MAX_VALID_YEAR = date.today().year + 2

# Transaction types in the TCON file that belong in loans_debts, not contributions.
# Maryland uses "Loan Received" and "Loan Forgiven" as primary loan types.
_LOAN_TYPE_RE = re.compile(r"\bloan\b", re.IGNORECASE)


# ============================== helpers ================================

def clean(val) -> str:
    return (val or "").strip()


def _unquote(val: str) -> str:
    """Strip Excel formula quoting from zip/ID fields: =\"VALUE\" → VALUE."""
    v = (val or "").strip()
    m = re.match(r'^="(.*)"$', v)
    return m.group(1) if m else v


def parse_amount(val: str) -> str:
    """'$1,000.00' → '1000.00', '' on failure."""
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


def parse_date(val: str) -> str:
    """MM/DD/YYYY → YYYY-MM-DD, '' on failure or implausible year."""
    v = clean(val)
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


def build_name(last: str, first: str, middle: str = "", suffix: str = "") -> str:
    """
    Construct 'Last, First Middle Suffix' from parts.
    Falls back to just 'First Middle' if no last name given.
    """
    last   = clean(last)
    first  = clean(first)
    middle = clean(middle)
    suffix = clean(suffix)

    given = " ".join(p for p in [first, middle, suffix] if p)
    if last and given:
        return f"{last}, {given}"
    if last:
        return last
    return given


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


def open_md_csv(path: Path):
    """
    Open a Maryland raw CSV file and return a DictReader positioned at the
    first data row. Maryland files have a timestamp title on line 0 and the
    real column header on line 1 — we skip line 0 before handing off to
    DictReader.

    NUL bytes occasionally appear in large API downloads (seen in the 2026
    contributions file). We filter them via a generator so the csv module
    doesn't raise "_csv.Error: line contains NUL".
    """
    fh = open(path, encoding="utf-8", errors="replace")
    fh.readline()   # discard the title row (e.g. "Contributions and Loan Download as of …")
    # Strip NUL bytes line-by-line; keeps memory use flat on large files
    return fh, csv.DictReader(line.replace("\x00", "") for line in fh)


# ================================= run =================================

def run():
    log = get_logger("maryland", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
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

        # ── Committee registry ─────────────────────────────────────────
        # Keyed by Filing Entity Id for transaction enrichment.
        # Committee type "Candidate" rows also produce a candidates row.
        cmte_registry: dict[str, dict] = {}
        cmte_path = RAW_DIR / "committees.csv"
        ft = time.perf_counter()
        cmte_count = 0

        if cmte_path.exists():
            raw_fh, reader = open_md_csv(cmte_path)
            try:
                for row_num, row in enumerate(reader, start=3):  # row 1=title, 2=header
                    fid        = clean(row.get("Filing Entity Id", ""))
                    cmte_name  = clean(row.get("Committee Name", ""))
                    cmte_type  = clean(row.get("Committee Type", ""))
                    elec_year  = clean(row.get("Election Year", ""))
                    treasurer  = clean(row.get("Treasurer/Authorized Agent Name", ""))
                    city       = clean(row.get("Committee City", ""))
                    zipcode    = _unquote(row.get("Committee ZipCode", ""))
                    dissolved  = clean(row.get("Registration Dissolved Date", ""))
                    active     = "0" if dissolved else "1"
                    jurisdiction = clean(row.get("Jurisdiction", ""))
                    office     = clean(row.get("Office Sought", ""))
                    party      = clean(row.get("Party Affiliation", ""))

                    # Candidate info — present on rows where Committee Type = "Candidate"
                    cand_last  = clean(row.get("Candidate LastName", ""))
                    cand_first = clean(row.get("Candidate First Name", ""))
                    cand_mid   = clean(row.get("Candidate Middle Name", ""))
                    cand_suf   = clean(row.get("Candidate Suffix", ""))
                    cand_name  = build_name(cand_last, cand_first, cand_mid, cand_suf)

                    entry = {
                        "committee_name": cmte_name,
                        "committee_type": cmte_type,
                        "candidate_name": cand_name,
                        "candidate_first": cand_first,
                        "candidate_last":  cand_last,
                        "office":          office,
                        "party":           party,
                        "jurisdiction":    jurisdiction,
                        "election_year":   elec_year,
                        "city":            city,
                        "zip":             zipcode,
                        "treasurer_name":  treasurer,
                        "active":          active,
                    }
                    if fid:
                        cmte_registry[fid] = entry

                    cmte_w.writerow({
                        "state":          STATE,
                        "state_filer_id": fid,
                        "committee_name": cmte_name,
                        "committee_type": cmte_type,
                        "election_year":  elec_year,
                        "candidate_name": cand_name,
                        "treasurer_name": treasurer,
                        "city":           city,
                        "zip":            zipcode,
                        "active":         active,
                        "raw_file":       cmte_path.name,
                        "row_num":        row_num,
                    })
                    cmte_count += 1

                    # Write a candidates row for every candidate committee that has a name.
                    # The committee file uses "Candidate Committee" (full form); transaction
                    # files use the abbreviated "Candidate" — both indicate a candidate filer.
                    if cmte_type in ("Candidate Committee", "Candidate") and (cand_last or cand_first):
                        cand_w.writerow({
                            "state":           STATE,
                            "state_filer_id":  fid,
                            "candidate_name":  cand_name,
                            "candidate_first": cand_first,
                            "candidate_last":  cand_last,
                            "office":          office,
                            "district":        "",
                            "jurisdiction":    jurisdiction,
                            "party":           party,
                            "election_year":   elec_year,
                            "incumbent":       "",
                            "raw_file":        cmte_path.name,
                            "row_num":         row_num,
                        })
                        candidates_written += 1
            finally:
                raw_fh.close()

        log.registry_loaded(cmte_path.name, cmte_count, relation="committees",
                            bytes=cmte_path.stat().st_size if cmte_path.exists() else 0)
        committees_written = cmte_count
        log.info(f"  committees: {cmte_count:,}  candidate committees: {candidates_written:,}")

        # ── Contributions & Loans ──────────────────────────────────────
        # TCON files contain both contribution and loan transaction types.
        # Rows whose Transaction Type matches the loan pattern go to loans_debts;
        # everything else goes to contributions.
        for path in raw_files("contributions_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            cont_count = loan_count = skipped = 0

            raw_fh, reader = open_md_csv(path)
            try:
                for row_num, row in enumerate(reader, start=3):
                    amount = parse_amount(row.get("Transaction Amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    fid       = clean(row.get("Filing Entity Id", ""))
                    cmte_name = clean(row.get("Committee Name", ""))
                    tx_type   = clean(row.get("Transaction Type", ""))
                    tx_date   = parse_date(row.get("Transaction Date", ""))
                    zipcode   = _unquote(row.get("Contributor ZipCode", ""))

                    # Build contributor name from split parts; fall back to company
                    contrib_last  = clean(row.get("Contributor Last Name", ""))
                    contrib_first = clean(row.get("Contributor First Name", ""))
                    contrib_mid   = clean(row.get("Contributor Middle Name", ""))
                    contrib_co    = clean(row.get("Contributor Company Name", ""))
                    if contrib_last or contrib_first:
                        contrib_name = build_name(contrib_last, contrib_first, contrib_mid)
                    else:
                        contrib_name = contrib_co

                    # Enrich from committee registry
                    reg        = cmte_registry.get(fid, {})
                    cand_name  = reg.get("candidate_name", "")
                    office     = reg.get("office", "")
                    elec_year  = reg.get("election_year", file_year)

                    if _LOAN_TYPE_RE.search(tx_type):
                        # Route loan transactions to the loans_debts table
                        loan_w.writerow({
                            "state":              STATE,
                            "committee_name":     cmte_name,
                            "original_amount":    amount,
                            "date":               tx_date,
                            "record_type":        tx_type,
                            "counterparty_name":  contrib_name,
                            "counterparty_city":  clean(row.get("Contributor City", "")),
                            "counterparty_state": clean(row.get("Contributor State", "")),
                            "counterparty_zip":   zipcode,
                            "candidate_name":     cand_name,
                            "election_year":      elec_year,
                            "amended":            "",
                            "filing_id":          clean(row.get("Report Name", "")),
                            "raw_file":           path.name,
                            "row_num":            row_num,
                        })
                        loan_count += 1
                    else:
                        cont_w.writerow({
                            "state":             STATE,
                            "committee_name":    cmte_name,
                            "amount":            amount,
                            "date":              tx_date,
                            "transaction_type":  tx_type,
                            "contributor_name":  contrib_name,
                            "contributor_type":  clean(row.get("Contributor Type", "")),
                            "contributor_city":  clean(row.get("Contributor City", "")),
                            "contributor_state": clean(row.get("Contributor State", "")),
                            "contributor_zip":   zipcode,
                            "employer":          "",
                            "occupation":        "",
                            "candidate_name":    cand_name,
                            "office":            office,
                            "election_year":     elec_year,
                            "amended":           "",
                            "filing_id":         clean(row.get("Report Name", "")),
                            "raw_file":          path.name,
                            "row_num":           row_num,
                        })
                        cont_count += 1
            finally:
                raw_fh.close()

            log.file_parsed(path.name, "contributions", cont_count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += cont_count
            total_loans         += loan_count

        # ── Expenditures ───────────────────────────────────────────────
        for path in raw_files("expenditures_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = skipped = 0

            raw_fh, reader = open_md_csv(path)
            try:
                for row_num, row in enumerate(reader, start=3):
                    amount = parse_amount(row.get("Transaction Amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    fid       = clean(row.get("Filing Entity Id", ""))
                    cmte_name = clean(row.get("Committee Name", ""))
                    tx_type   = clean(row.get("Transaction Type", ""))
                    tx_date   = parse_date(row.get("Transaction Date", ""))
                    payee_zip = _unquote(row.get("Payee Zip Code", ""))

                    # Build payee name from split parts; fall back to company/vendor
                    payee_last  = clean(row.get("Payee Last Name", ""))
                    payee_first = clean(row.get("Payee First Name", ""))
                    payee_co    = clean(row.get("Payee Company Name", ""))
                    vendor_name = clean(row.get("Vendor Name", ""))
                    if payee_last or payee_first:
                        payee_name = build_name(payee_last, payee_first)
                    elif payee_co:
                        payee_name = payee_co
                    else:
                        payee_name = vendor_name

                    # Candidate/office come directly from the expenditures file
                    cand_name = clean(row.get("Candidate/Ballot Issue", ""))
                    office    = clean(row.get("Office Sought", ""))

                    # If not present in the row, fall back to registry
                    if not cand_name:
                        reg       = cmte_registry.get(fid, {})
                        cand_name = reg.get("candidate_name", "")
                        office    = office or reg.get("office", "")

                    reg       = cmte_registry.get(fid, {})
                    elec_year = reg.get("election_year", file_year)

                    # Purpose: prefer Purpose field; fall back to Description
                    purpose  = clean(row.get("Purpose", "")) or clean(row.get("Description", ""))
                    category = clean(row.get("Category", ""))

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "amount":           amount,
                        "date":             tx_date,
                        "transaction_type": tx_type,
                        "payee_name":       payee_name,
                        "purpose":          purpose,
                        "category":         category,
                        "payee_city":       clean(row.get("Payee City", "")),
                        "payee_state":      clean(row.get("Payee State", "")),
                        "payee_zip":        payee_zip,
                        "candidate_name":   cand_name,
                        "office":           office,
                        "election_year":    elec_year,
                        "amended":          "",
                        "filing_id":        clean(row.get("Report Name", "")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1
            finally:
                raw_fh.close()

            log.file_parsed(path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # MD assigns a new Filing Entity Id per committee registration; the same
        # candidate gets a different ID each cycle → use "committee" model, which
        # groups by (state, candidate_name, office, district) and picks min ID.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
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
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_contributions:,} contributions, "
                 f"{total_expenditures:,} expenditures, {total_loans:,} loans")
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
