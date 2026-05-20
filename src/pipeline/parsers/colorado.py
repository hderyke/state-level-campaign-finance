"""
parsers/colorado.py — Transform Colorado TRACER raw files into the 5 normalized relations.

Input:  data/Colorado/raw/
  candidates_all.csv          — scraped from CandidateDetail.aspx (one row per cycle)
  committees.csv              — scraped from CommitteeDetail.aspx (one row per committee)
  contributions_{year}.csv    — bulk download (2000–present)
  expenditures_{year}.csv     — bulk download (2000–present)
  loans_{year}.csv            — bulk download (2000–present)

Output: data/Colorado/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Notes
─────
  • candidates_all.csv has one row per (seq_id, election_cycle).  We deduplicate
    to one row per seq_id, keeping the most recent election_year.  candidate_id
    is the person-level stable ID → state_filer_id on candidates/committees.
  • CO_ID appears in the source transaction files but is not written to
    contributions/expenditures; join transactions to committees via committee_name.
    Transactions carry a CandidateName text field for direct name-based joins.
  • Loan Type O = original loan; Type P = payment/repayment.  Both are kept in
    loans_debts.csv with record_type set accordingly.
  • Amendment = Y in transactions means this row supersedes an earlier filing.
  • Dates arrive as "YYYY-MM-DD HH:MM:SS" — just strip the time component.
  • committees.csv city_state_zip is "CITY ST ZIP" (no comma) — parsed with regex.
"""

import csv
import gzip
import re
import sys
from pathlib import Path
from datetime import datetime, date

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Colorado" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Colorado" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "CO"
MAX_VALID_YEAR = date.today().year + 2


# ── Helpers ───────────────────────────────────────────────────────────────────
def clean(val) -> str:
    return (val or "").strip().strip('"')


def parse_amount(val: str) -> str:
    """'$1,000.00' or plain decimal → clean decimal string, '' on failure."""
    v = (val or "").strip().strip('"').replace("$", "").replace(",", "")
    if not v:
        return ""
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """
    'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' or 'MM/DD/YYYY' → 'YYYY-MM-DD'.
    Returns '' on failure or implausible year.
    """
    v = (val or "").strip().strip('"')
    if not v:
        return ""
    # Strip trailing timestamp
    v = v.split(" ")[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1970 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def amended_flag(val: str) -> str:
    """'Y' → '1', 'N' → '0', else ''."""
    v = clean(val).upper()
    if v == "Y":
        return "1"
    if v == "N":
        return "0"
    return ""


# "DENVER CO 80201" or "GOLDEN CO 80401-1234"
_CSZ_RE = re.compile(
    r"^(.*?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$"
)

def parse_city_state_zip(val: str) -> tuple[str, str, str]:
    """'CITY ST ZIP' → (city, state, zip).  Returns ('','','') on no match."""
    v = (val or "").strip()
    if not v:
        return "", "", ""
    m = _CSZ_RE.match(v)
    if not m:
        return "", "", ""
    return m.group(1).strip(), m.group(2), m.group(3)


def split_name(name: str) -> tuple[str, str]:
    """'LAST, FIRST' → (last, first).  Falls back gracefully."""
    name = (name or "").strip()
    if "," in name:
        last, _, first = name.partition(",")
        return last.strip(), first.strip()
    return name, ""


_SUFFIXES = {"JR.", "JR", "SR.", "SR", "II", "III", "IV", "V", "ESQ.", "ESQ"}

def invert_name(name: str) -> str:
    """'FIRST [M] LAST [SUFFIX]' (Colorado CandidateName format) → 'LAST, FIRST'.
    Strips generational suffixes (JR., SR., II, III …) before inverting so that
    'BILL OWENS JR.' → 'OWENS, BILL' rather than 'JR., BILL'.
    """
    parts = (name or "").strip().split()
    # Drop trailing suffix(es)
    while parts and parts[-1].upper() in _SUFFIXES:
        parts.pop()
    if not parts:
        return name.strip()
    if len(parts) == 1:
        return parts[0]
    last  = parts[-1]
    first = parts[0]
    return f"{last}, {first}"


def year_from_filename(path: Path) -> str:
    m = re.search(r"(\d{4})", path.name)
    return m.group(1) if m else ""


def raw_files(pattern: str) -> list[Path]:
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    path = CLEAN_DIR / filename
    fh = gzip.open(path, "wt", newline="", encoding="utf-8") if filename.endswith(".gz") \
         else open(path, "w", newline="", encoding="utf-8")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
    cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
    cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
    expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
    loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)

    # ── Candidates ────────────────────────────────────────────────────────────
    # Colorado TRACER stores the committee name (e.g. "CARAVEO FOR COLORADO")
    # in lblCandName, not the person's name.  The actual person name is in the
    # CandidateName field of the transaction files ("YADIRA CARAVEO").
    # Pre-pass: build CO_ID → person_name (inverted to "LAST, FIRST") from
    # all contribution/expenditure files so we can populate candidate_name
    # with the real person name.
    #
    # Also: the SeqID scraper returns ALL filer types (PACs, party orgs, ballot
    # issue committees).  We filter to only rows with a non-blank office so that
    # only actual candidates end up in the candidates table.

    cand_path = RAW_DIR / "candidates_all.csv"
    print("  candidates     pre-pass for person names...", end=" ", flush=True)

    # Pre-pass 1: collect candidate_ids that have an office (real candidates)
    best: dict[str, dict] = {}   # seq_id → best raw row
    if cand_path.exists() and cand_path.stat().st_size > 0:
        with open(cand_path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                sid = clean(row.get("seq_id", ""))
                if not sid:
                    continue
                prev      = best.get(sid)
                cur_year  = clean(row.get("election_year", ""))
                cur_office = clean(row.get("office", ""))
                if prev is None:
                    best[sid] = row
                else:
                    prev_office = clean(prev.get("office", ""))
                    # Prefer row with office; if both have office take more recent year
                    if cur_office and not prev_office:
                        best[sid] = row
                    elif cur_office and prev_office and cur_year > clean(prev.get("election_year", "")):
                        best[sid] = row

    # Only keep entries where office contains at least one letter.
    # PACs, party committees, and ballot issue filers get a numeric office code
    # (e.g. "4", "5") in TRACER instead of a real office name.  Actual candidates
    # have names like "Governor", "Colorado Senate", "Attorney General", etc.
    best = {
        sid: row for sid, row in best.items()
        if any(c.isalpha() for c in clean(row.get("office", "")))
    }

    # Collect the candidate_ids we need person names for
    need_names: set[str] = {
        clean(row.get("candidate_id", "")) for row in best.values()
        if clean(row.get("candidate_id", ""))
    }

    # Pre-pass 2: scan transaction files for CandidateName by CO_ID
    co_id_to_person: dict[str, str] = {}   # CO_ID → "LAST, FIRST"
    remaining = set(need_names)
    for path in sorted(RAW_DIR.glob("contributions_*.csv"), reverse=True):
        if not remaining:
            break
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                co_id = clean(row.get("CO_ID", ""))
                if co_id not in remaining:
                    continue
                cname = clean(row.get("CandidateName", ""))
                if cname:
                    co_id_to_person[co_id] = invert_name(cname)
                    remaining.discard(co_id)
    # Fall back to expenditures for any still missing
    for path in sorted(RAW_DIR.glob("expenditures_*.csv"), reverse=True):
        if not remaining:
            break
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                co_id = clean(row.get("CO_ID", ""))
                if co_id not in remaining:
                    continue
                cname = clean(row.get("CandidateName", ""))
                if cname:
                    co_id_to_person[co_id] = invert_name(cname)
                    remaining.discard(co_id)

    found_names = len(need_names) - len(remaining)
    print(f"{found_names:,}/{len(need_names):,} person names resolved")

    # Write candidates (only entries with an office)
    print("  candidates     candidates_all.csv...", end=" ", flush=True)
    cand_count = 0
    for row_num, (sid, row) in enumerate(best.items(), start=2):
        cand_id   = clean(row.get("candidate_id", ""))
        cmte_name = clean(row.get("name", ""))   # Colorado stores committee name here

        # Use person name from transaction lookup; fall back to committee name
        person_name = co_id_to_person.get(cand_id, "")
        if person_name:
            display_name = person_name
            cand_last, cand_first = split_name(person_name)
        else:
            display_name = cmte_name
            cand_last, cand_first = split_name(cmte_name)

        cand_w.writerow({
            "state":           STATE,
            "state_filer_id":  cand_id,
            "candidate_name":  display_name,
            "candidate_first": cand_first,
            "candidate_last":  cand_last,
            "office":          clean(row.get("office", "")),
            "district":        clean(row.get("district", "")),
            "jurisdiction":    clean(row.get("jurisdiction", "")),
            "party":           clean(row.get("party", "")),
            "election_year":   clean(row.get("election_year", "")),
            "status":          clean(row.get("current_status", "")),
            "incumbent":       "",
            "raw_file":        cand_path.name,
            "row_num":         row_num,
        })
        cand_count += 1

    cand_fh.flush()
    cand_fh.close()
    utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")
    print(f"{cand_count:,} candidates")

    # ── Committees ────────────────────────────────────────────────────────────
    cmte_path = RAW_DIR / "committees.csv"
    print("  committees     committees.csv...", end=" ", flush=True)
    cmte_count = 0
    if cmte_path.exists() and cmte_path.stat().st_size > 0:
        with open(cmte_path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                cmte_id   = clean(row.get("committee_id", ""))
                cmte_name = clean(row.get("committee_name", ""))
                status    = clean(row.get("status", ""))

                city, _, zipcode = parse_city_state_zip(clean(row.get("city_state_zip", "")))

                # Registered agent serves as treasurer proxy in this data
                treasurer = clean(row.get("registered_agent", ""))

                cmte_w.writerow({
                    "state":          STATE,
                    "state_filer_id": cmte_id,
                    "committee_name": cmte_name,
                    "committee_type": clean(row.get("committee_type", "")),
                    "candidate_name": "",   # joinable via CandidateName in transactions
                    "treasurer_name": treasurer,
                    "city":           city,
                    "zip":            zipcode,
                    "active":         "1" if status == "Active" else ("0" if status else ""),
                })
                cmte_count += 1
    print(f"{cmte_count:,} committees")

    # ── Contributions ─────────────────────────────────────────────────────────
    total_cont = 0
    for path in raw_files("contributions_*.csv"):
        file_year = year_from_filename(path)
        print(f"  contributions  {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                amount = parse_amount(row.get("ContributionAmount", ""))
                if not amount:
                    continue

                # Build contributor name from split fields; business names land in LastName
                contributor = " ".join(filter(None, [
                    clean(row.get("FirstName", "")),
                    clean(row.get("MI", "")),
                    clean(row.get("LastName", "")),
                    clean(row.get("Suffix", "")),
                ])).strip()
                if not contributor:
                    contributor = clean(row.get("LastName", ""))

                date_val  = parse_date(row.get("ContributionDate", ""))
                elec_year = clean(row.get("ContributionDate", ""))[:4] or file_year

                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    clean(row.get("CommitteeName", "")),
                    "contributor_name":  contributor,
                    "amount":            amount,
                    "date":              date_val,
                    "transaction_type":  clean(row.get("ContributionType", "")),
                    "contributor_type":  clean(row.get("ContributorType", "")),
                    "contributor_city":  clean(row.get("City", "")),
                    "contributor_state": clean(row.get("State", "")),
                    "contributor_zip":   clean(row.get("Zip", "")),
                    "employer":          clean(row.get("Employer", "")),
                    "occupation":        clean(row.get("Occupation", ""))
                                         or clean(row.get("OccupationComments", "")),
                    "candidate_name":    clean(row.get("CandidateName", "")),
                    "office":            "",
                    "election_year":     elec_year,
                    "filing_id":         clean(row.get("RecordID", "")),
                    "amended":           amended_flag(row.get("Amendment", "")),
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                count += 1
        print(f"{count:,} rows")
        total_cont += count

    # ── Expenditures ─────────────────────────────────────────────────────────
    total_expn = 0
    for path in raw_files("expenditures_*.csv"):
        file_year = year_from_filename(path)
        print(f"  expenditures   {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                amount = parse_amount(row.get("ExpenditureAmount", ""))
                if not amount:
                    continue

                payee = " ".join(filter(None, [
                    clean(row.get("FirstName", "")),
                    clean(row.get("MI", "")),
                    clean(row.get("LastName", "")),
                    clean(row.get("Suffix", "")),
                ])).strip()
                if not payee:
                    payee = clean(row.get("LastName", ""))

                date_val  = parse_date(row.get("ExpenditureDate", ""))
                elec_year = clean(row.get("ExpenditureDate", ""))[:4] or file_year

                # DisbursementType is more granular; fall back to ExpenditureType
                category = clean(row.get("DisbursementType", "")) \
                           or clean(row.get("ExpenditureType", ""))

                expn_w.writerow({
                    "state":            STATE,
                    "committee_name":   clean(row.get("CommitteeName", "")),
                    "payee_name":       payee,
                    "amount":           amount,
                    "date":             date_val,
                    "transaction_type": clean(row.get("ExpenditureType", "")),
                    "purpose":          clean(row.get("Explanation", "")),
                    "category":         category,
                    "payee_city":       clean(row.get("City", "")),
                    "payee_state":      clean(row.get("State", "")),
                    "payee_zip":        clean(row.get("Zip", "")),
                    "candidate_name":   clean(row.get("CandidateName", "")),
                    "office":           "",
                    "election_year":    elec_year,
                    "filing_id":        clean(row.get("RecordID", "")),
                    "amended":          amended_flag(row.get("Amendment", "")),
                    "raw_file":         path.name,
                    "row_num":          row_num,
                })
                count += 1
        print(f"{count:,} rows")
        total_expn += count

    # ── Loans / Debts ─────────────────────────────────────────────────────────
    # Type O = loan originated; Type P = payment/repayment.
    # Both rows written; record_type distinguishes them.
    # LoanDate is the origination date; PaymentDate is the repayment date.
    total_loan = 0
    for path in raw_files("loans_*.csv"):
        file_year = year_from_filename(path)
        print(f"  loans          {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                rec_type = clean(row.get("Type", ""))
                if rec_type not in ("O", "P"):
                    continue   # skip malformed rows

                loan_amt = parse_amount(row.get("LoanAmount", ""))
                pay_amt  = parse_amount(row.get("PaymentAmount", ""))
                # For O rows use LoanAmount; for P rows prefer PaymentAmount
                if rec_type == "O":
                    amount = loan_amt
                    date_val = parse_date(row.get("LoanDate", ""))
                else:
                    amount = pay_amt or loan_amt
                    date_val = parse_date(row.get("PaymentDate", "")
                                          or row.get("LoanDate", ""))

                if not amount:
                    continue

                elec_year = (date_val[:4] if date_val else "") or file_year

                loan_w.writerow({
                    "state":              STATE,
                    "committee_name":     clean(row.get("CommitteeName", "")),
                    "record_type":        rec_type,
                    "counterparty_name":  clean(row.get("Name", "")),
                    "counterparty_city":  clean(row.get("City", "")),
                    "counterparty_state": clean(row.get("State", "")),
                    "counterparty_zip":   clean(row.get("Zip", "")),
                    "original_amount":    amount,
                    "date":               date_val,
                    "candidate_name":     clean(row.get("CandidateName", "")),
                    "election_year":      elec_year,
                    "filing_id":          clean(row.get("RecordID", "")),
                    "amended":            amended_flag(row.get("Amendment", "")),
                    "raw_file":           path.name,
                    "row_num":            row_num,
                })
                count += 1
        print(f"{count:,} rows")
        total_loan += count

    for fh in (cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh):
        fh.close()

    print(f"\nColorado: done.")
    print(f"  {cand_count:,} candidates  {cmte_count:,} committees")
    print(f"  {total_cont:,} contributions  {total_expn:,} expenditures  {total_loan:,} loans/debts")


if __name__ == "__main__":
    run()
