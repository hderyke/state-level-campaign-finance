"""
parsers/colorado.py — Transform Colorado TRACER raw files into the 5 normalized relations.

Input:  data/Colorado/raw/
  candidates_all.csv       — scraped from CandidateDetail.aspx (one row per cycle)
  committees.csv           — scraped from CommitteeDetail.aspx (one row per committee)
  contributions_{year}.csv — bulk download (2000–present)
  expenditures_{year}.csv  — bulk download (2000–present)
  loans_{year}.csv         — bulk download (2000–present)

Output: data/Colorado/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Notes
─────
  • candidates_all.csv has one row per (seq_id, election_cycle).  We deduplicate
    to one row per seq_id, keeping the most recent cycle that has a real office.
    PACs and non-candidate filers get numeric office codes ("4", "5") in TRACER
    instead of names like "Governor" — these are filtered out.
  • candidate_id (= CO_ID) is the person-level stable ID → state_filer_id on
    candidates; id_model="person".
  • Committee candidate_name is populated from the co_id_to_person pre-pass so
    that assign_committee_person_ids can match committees to candidates.
  • Transaction files carry a CandidateName field ("YADIRA CARAVEO") and a
    CO_ID.  Person names are resolved from transactions; committee name comes
    from the CommitteeName field.
  • Loan Type O = origination; Type P = payment/repayment.  Both written to
    loans_debts with record_type set accordingly.
  • Amendment = Y means this row supersedes an earlier filing.
  • Dates arrive as "YYYY-MM-DD HH:MM:SS" — time component is stripped.
  • committees.csv city_state_zip is "CITY ST ZIP" (no comma) — parsed with regex.
"""

import csv
import gzip
import re
import sys
import time
from pathlib import Path
from datetime import datetime, date

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================

RAW_DIR   = PROJECT_ROOT / "data" / "Colorado" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Colorado" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "CO"
MAX_VALID_YEAR = date.today().year + 2

# Matches ContributorType values like:
#   "Individual (Member of LLC: CHENEY GALLUZZI & HOWARD LLC)"
_LLC_MEMBER_RE = re.compile(
    r"Individual\s*\(Member of LLC:\s*(.+?)\)\s*$", re.IGNORECASE
)

# Matches the declared total inside ContributionType values like:
#   "Monetary (Itemized) - LLC Contribution (Total Amount: 625.00)"
_LLC_TOTAL_RE = re.compile(
    r"Total Amount:\s*([\d,.]+)", re.IGNORECASE
)

# Strips the LLC suffix from ContributionType so alias lookup works:
#   "Monetary (Itemized) - LLC Contribution (Total Amount: 625.00)"
#   → "Monetary (Itemized)"
_LLC_TYPE_SUFFIX_RE = re.compile(
    r"\s*-\s*LLC Contribution.*$", re.IGNORECASE
)


# ============================== helpers ================================

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
    'YYYY-MM-DD HH:MM:SS' | 'YYYY-MM-DD' | 'MM/DD/YYYY' → 'YYYY-MM-DD'.
    Returns '' on failure or implausible year.
    """
    v = (val or "").strip().strip('"')
    if not v:
        return ""
    v = v.split(" ")[0]   # strip trailing timestamp
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1970 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def clean_tx_type(val: str) -> str:
    """
    Sanitize a transaction/expenditure type field.
    TRACER exports occasionally produce column-shifted rows where a date
    ('2013-04-14 00:00:00') or a numeric RecordID ('749760') lands in the
    ExpenditureType column.  Return '' for those; pass everything else through.
    """
    v = clean(val)
    if not v:
        return ""
    if _DATE_RE.match(v):   # date timestamp in wrong column
        return ""
    if v.isdigit():          # bare RecordID in wrong column
        return ""
    return v


MIN_VALID_ELECTION_YEAR = 1990


def valid_election_year(val: str) -> str:
    """
    Return val if it's a plausible 4-digit election year, else ''.

    CandidateDetail.aspx's campaigns table normally gives an "election cycle"
    like "2022 General"; cells[1].split()[0] extracts the year. But for
    proposed special districts (e.g. metro/water districts), the cycle label
    is the district's own name instead — "TACINCALA METROPOLITAN DISTRICT
    (PROPOSED) ELECTION CYCLE" — so split()[0] yields "TACINCALA" rather than
    a year. Such a non-numeric/out-of-range value would otherwise fail the
    BIGINT cast in tabulate.py (ignore_errors=true silently drops the *whole*
    row), causing real candidates/committees to vanish from the final tables.
    Normalize to '' here instead — same convention as a candidate with no
    known election cycle.
    """
    v = clean(val)
    if v.isdigit() and MIN_VALID_ELECTION_YEAR <= int(v) <= MAX_VALID_YEAR:
        return v
    return ""


def amended_flag(val: str) -> str:
    """'Y' → '1', 'N' → '0', else ''."""
    v = clean(val).upper()
    if v == "Y":
        return "1"
    if v == "N":
        return "0"
    return ""


# "DENVER CO 80201" or "GOLDEN CO 80401-1234" — no comma between city and state
_CSZ_RE = re.compile(r"^(.*?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$")


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
    """
    'FIRST [M] LAST [SUFFIX]' (Colorado CandidateName format) → 'LAST, FIRST'.
    Strips generational suffixes before inverting so that
    'BILL OWENS JR.' → 'OWENS, BILL' rather than 'JR., BILL'.
    """
    parts = (name or "").strip().split()
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
    """All non-empty files matching glob pattern, sorted by filename."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    """Open a gzip-compressed DictWriter in CLEAN_DIR."""
    path = CLEAN_DIR / filename
    fh   = gzip.open(path, "wt", newline="", encoding="utf-8")
    w    = csv.DictWriter(fh, fieldnames=fieldnames,
                          extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# =============================== run() ================================

def run():
    log = get_logger("colorado", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    cand_count          = 0
    cmte_count          = 0
    file_handles        = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        # ── Candidates ────────────────────────────────────────────────────────
        # Colorado TRACER stores the committee name (e.g. "CARAVEO FOR COLORADO")
        # in lblCandName, not the person's name.  The actual person name arrives
        # as CandidateName in the transaction files ("YADIRA CARAVEO").
        #
        # Pre-pass 1: deduplicate candidates_all.csv to one row per seq_id,
        #             keeping the row with the most recent real office.
        # Pre-pass 2: scan contribution/expenditure files for CO_ID → person name
        #             so we can populate candidate_name with "LAST, FIRST".

        cand_path = RAW_DIR / "candidates_all.csv"
        log.info("  candidates     pre-pass: deduplicating…")

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
                        if cur_office and not prev_office:
                            best[sid] = row
                        elif cur_office and prev_office and cur_year > clean(prev.get("election_year", "")):
                            best[sid] = row

        # Retain only rows whose office contains at least one letter.
        # PACs and ballot committees get numeric office codes ("4", "5") in TRACER;
        # real candidates have names like "Governor", "Colorado Senate", etc.
        best = {
            sid: row for sid, row in best.items()
            if any(c.isalpha() for c in clean(row.get("office", "")))
        }

        # Build the set of candidate_ids we need person names for
        need_names: set[str] = {
            clean(row.get("candidate_id", ""))
            for row in best.values()
            if clean(row.get("candidate_id", ""))
        }

        # Pre-pass 2: scan transaction files for CO_ID → person name.
        # Store the name in "FIRST LAST" format (as it appears in CandidateName)
        # so that candidate_name in the candidates table matches the candidate_name
        # written to contributions rows — the spot-check query joins the two tables
        # directly on that column.
        co_id_to_person: dict[str, str] = {}   # CO_ID → "FIRST LAST" (raw, uppercased)
        remaining = set(need_names)
        for tx_glob in ("contributions_*.csv", "expenditures_*.csv"):
            if not remaining:
                break
            for path in sorted(RAW_DIR.glob(tx_glob), reverse=True):
                if not remaining:
                    break
                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    for row in csv.DictReader(f):
                        co_id = clean(row.get("CO_ID", ""))
                        if co_id not in remaining:
                            continue
                        cname = clean(row.get("CandidateName", ""))
                        if cname:
                            # Keep original "FIRST LAST" format — do NOT invert
                            co_id_to_person[co_id] = cname
                            remaining.discard(co_id)

        found_names = len(need_names) - len(remaining)
        log.info(f"  candidates     {found_names:,}/{len(need_names):,} person names resolved")

        # Write candidates (filtered to entries with a real office)
        ft = time.perf_counter()
        for row_num, (sid, row) in enumerate(best.items(), start=2):
            cand_id   = clean(row.get("candidate_id", ""))
            cmte_name = clean(row.get("name", ""))   # lblCandName holds committee name

            # Prefer person name from transaction lookup; fall back to committee name.
            # candidate_name stored in "FIRST LAST" format to match contributions rows.
            # candidate_first / candidate_last derived via invert_name → split_name.
            person_name = co_id_to_person.get(cand_id, "")
            if person_name:
                display_name = utils.clean_name(person_name)           # "YADIRA CARAVEO"
                cand_last, cand_first = split_name(invert_name(person_name))  # ("CARAVEO","YADIRA")
            else:
                display_name = utils.clean_name(cmte_name)
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
                "election_year":   valid_election_year(row.get("election_year", "")),
                "incumbent":       "",
                "raw_file":        cand_path.name,
                "row_num":         row_num,
            })
            # Capture for committee candidate_name + election_year population below
            if cand_id:
                co_id_to_person[cand_id] = display_name
            cand_count += 1

        co_id_to_election_year: dict[str, str] = {
            clean(row.get("candidate_id", "")): valid_election_year(row.get("election_year", ""))
            for row in best.values()
            if clean(row.get("candidate_id", ""))
        }

        log.file_parsed(cand_path.name, "candidates", cand_count,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=cand_path.stat().st_size if cand_path.exists() else 0)

        # ── Committees ────────────────────────────────────────────────────────
        # committee_id == CO_ID == candidate_id, so we can populate candidate_name
        # from co_id_to_person — enabling assign_committee_person_ids to link them.
        cmte_path = RAW_DIR / "committees.csv"
        ft = time.perf_counter()
        if cmte_path.exists() and cmte_path.stat().st_size > 0:
            with open(cmte_path, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    cmte_id   = clean(row.get("committee_id", ""))
                    cmte_name = utils.clean_name(row.get("committee_name", ""))
                    status    = clean(row.get("status", ""))

                    city, _, zipcode = parse_city_state_zip(
                        clean(row.get("city_state_zip", "")))

                    # Registered agent serves as treasurer proxy in this data
                    treasurer = clean(row.get("registered_agent", ""))

                    # Populate candidate_name so assign_committee_person_ids can
                    # match this committee to its candidate row via person name.
                    candidate_name = utils.clean_name(
                        co_id_to_person.get(cmte_id, ""))

                    cmte_w.writerow({
                        "state":          STATE,
                        "state_filer_id": cmte_id,
                        "committee_name": cmte_name,
                        "committee_type": clean(row.get("committee_type", "")),
                        "election_year":  co_id_to_election_year.get(cmte_id, ""),
                        "candidate_name": candidate_name,
                        "treasurer_name": utils.clean_name(treasurer),
                        "city":           utils.clean_name(city),
                        "zip":            utils.clean_zip(zipcode),
                        "active":         "1" if status == "Active"
                                          else ("0" if status else ""),
                        "raw_file":       cmte_path.name,
                        "row_num":        cmte_count + 2,   # 1-based, header = row 1
                    })
                    cmte_count += 1

        log.file_parsed(cmte_path.name, "committees", cmte_count,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=cmte_path.stat().st_size if cmte_path.exists() else 0)

        # ── Contributions ─────────────────────────────────────────────────────
        # Colorado requires LLCs to disclose their members individually; each
        # member gets their own row with ContributorType =
        #   "Individual (Member of LLC: XYZ LLC)"
        # and ContributionType containing the LLC's total:
        #   "Monetary (Itemized) - LLC Contribution (Total Amount: 625.00)"
        #
        # We flatten these back to one row per LLC contribution event, keyed on
        # (CO_ID, llc_name, contribution_date, filed_date), using the declared
        # total as the amount.  Rows without a declared total (typically refunds
        # or amendments with a negative amount) are written individually since
        # no reliable total is available to deduplicate on.
        seen_llc_keys: set[tuple] = set()

        for path in raw_files("contributions_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = skipped = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("ContributionAmount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    contrib_type_raw = clean(row.get("ContributorType", ""))
                    llc_m = _LLC_MEMBER_RE.match(contrib_type_raw)

                    if llc_m:
                        llc_name = llc_m.group(1).strip()
                        total_m  = _LLC_TOTAL_RE.search(
                            clean(row.get("ContributionType", "")))

                        if total_m:
                            # Itemized LLC contribution — deduplicate to one row
                            total = parse_amount(total_m.group(1))
                            llc_key = (
                                clean(row.get("CO_ID", "")),
                                llc_name,
                                clean(row.get("ContributionDate", ""))[:10],
                                clean(row.get("FiledDate", ""))[:10],
                            )
                            if llc_key in seen_llc_keys:
                                skipped += 1
                                continue
                            seen_llc_keys.add(llc_key)
                            amount = total or amount   # use declared total; fall back if parse fails
                        # else: negative/refund row — keep as-is, just normalize name below

                        contributor      = utils.clean_name(llc_name)
                        contributor_type = "Business"
                    else:
                        # Normal row — build name from individual fields
                        contributor = " ".join(filter(None, [
                            clean(row.get("FirstName", "")),
                            clean(row.get("MI", "")),
                            clean(row.get("LastName", "")),
                            clean(row.get("Suffix", "")),
                        ])).strip()
                        if not contributor:
                            contributor = clean(row.get("LastName", ""))
                        contributor      = utils.clean_name(contributor)
                        contributor_type = contrib_type_raw

                    date_val  = parse_date(row.get("ContributionDate", ""))
                    elec_year = clean(row.get("ContributionDate", ""))[:4] or file_year

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    utils.clean_name(row.get("CommitteeName", "")),
                        "contributor_name":  contributor,
                        "amount":            amount,
                        "date":              date_val,
                        "transaction_type":  _LLC_TYPE_SUFFIX_RE.sub(
                            "", clean(row.get("ContributionType", ""))),
                        "contributor_type":  contributor_type,
                        "contributor_city":  utils.clean_name(row.get("City", "")),
                        "contributor_state": clean(row.get("State", "")),
                        "contributor_zip":   utils.clean_zip(row.get("Zip", "")),
                        "employer":          clean(row.get("Employer", "")),
                        "occupation":        (clean(row.get("Occupation", ""))
                                              or clean(row.get("OccupationComments", ""))),
                        "candidate_name":    utils.clean_name(row.get("CandidateName", "")),
                        "office":            "",
                        "election_year":     elec_year,
                        "filing_id":         clean(row.get("RecordID", "")),
                        "amended":           amended_flag(row.get("Amendment", "")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "contributions", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += count

        # ── Expenditures ──────────────────────────────────────────────────────
        for path in raw_files("expenditures_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = skipped = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("ExpenditureAmount", ""))
                    if not amount:
                        skipped += 1
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
                    category = (clean(row.get("DisbursementType", ""))
                                or clean(row.get("ExpenditureType", "")))

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   utils.clean_name(row.get("CommitteeName", "")),
                        "payee_name":       utils.clean_name(payee),
                        "amount":           amount,
                        "date":             date_val,
                        "transaction_type": clean_tx_type(row.get("ExpenditureType", "")),
                        "purpose":          clean(row.get("Explanation", "")),
                        "category":         category,
                        "payee_city":       utils.clean_name(row.get("City", "")),
                        "payee_state":      clean(row.get("State", "")),
                        "payee_zip":        utils.clean_zip(row.get("Zip", "")),
                        "candidate_name":   utils.clean_name(row.get("CandidateName", "")),
                        "office":           "",
                        "election_year":    elec_year,
                        "filing_id":        clean(row.get("RecordID", "")),
                        "amended":          amended_flag(row.get("Amendment", "")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count

        # ── Loans / Debts ─────────────────────────────────────────────────────
        # Type O = loan originated; Type P = payment/repayment.
        # Both rows are written; record_type distinguishes them.
        # LoanDate is the origination date; PaymentDate is the repayment date.
        for path in raw_files("loans_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = skipped = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    rec_type = clean(row.get("Type", ""))
                    if rec_type not in ("O", "P"):
                        skipped += 1
                        continue

                    loan_amt = parse_amount(row.get("LoanAmount", ""))
                    pay_amt  = parse_amount(row.get("PaymentAmount", ""))
                    # For originations use LoanAmount; for payments prefer PaymentAmount
                    if rec_type == "O":
                        amount   = loan_amt
                        date_val = parse_date(row.get("LoanDate", ""))
                    else:
                        amount   = pay_amt or loan_amt
                        date_val = parse_date(
                            row.get("PaymentDate", "") or row.get("LoanDate", ""))

                    if not amount:
                        skipped += 1
                        continue

                    elec_year = (date_val[:4] if date_val else "") or file_year

                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     utils.clean_name(row.get("CommitteeName", "")),
                        "record_type":        rec_type,
                        "counterparty_name":  utils.clean_name(row.get("Name", "")),
                        "counterparty_city":  utils.clean_name(row.get("City", "")),
                        "counterparty_state": clean(row.get("State", "")),
                        "counterparty_zip":   utils.clean_zip(row.get("Zip", "")),
                        "original_amount":    amount,
                        "date":               date_val,
                        "candidate_name":     utils.clean_name(row.get("CandidateName", "")),
                        "election_year":      elec_year,
                        "filing_id":          clean(row.get("RecordID", "")),
                        "amended":            amended_flag(row.get("Amendment", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_loans += count

        # ── Close all handles, then assign person IDs ─────────────────────────
        # person IDs must be assigned AFTER handles are closed so the gz files
        # are complete before utils reads and rewrites them in place.
        for fh in file_handles:
            fh.close()
        file_handles = []

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")

        # Colorado-specific: committee_id == candidate_id == CO_ID (person-level
        # stable ID), so we can link committees to candidates directly by ID rather
        # than relying on name-based matching.  Name-based matching (via
        # assign_committee_person_ids) is skipped here because candidate_name on
        # committee rows is only populated for the ~13% of committees whose CO_ID
        # appeared in transaction CandidateName fields, whereas the direct lookup
        # works for every committee that has a corresponding candidate entry.
        _cand_path = CLEAN_DIR / "candidates.csv.gz"
        _cmte_path = CLEAN_DIR / "committees.csv.gz"

        # Build CO_ID → person_id from the now-stamped candidates file
        _co_id_to_pid: dict[str, str] = {}
        with gzip.open(_cand_path, "rt", encoding="utf-8") as _f:
            for _row in csv.DictReader(_f):
                _fid = (_row.get("state_filer_id") or "").strip()
                _pid = (_row.get("person_id") or "").strip()
                if _fid and _pid:
                    _co_id_to_pid[_fid] = _pid

        # Stamp person_id onto committees by direct CO_ID match
        with gzip.open(_cmte_path, "rt", encoding="utf-8") as _f:
            _cmte_rows = list(csv.DictReader(_f))

        _matched = 0
        for _row in _cmte_rows:
            _cmte_id = (_row.get("state_filer_id") or "").strip()
            _pid = _co_id_to_pid.get(_cmte_id, "")
            _row["person_id"] = _pid
            if _pid:
                _matched += 1

        _fieldnames = list(_cmte_rows[0].keys()) if _cmte_rows else []
        if "person_id" not in _fieldnames:
            _fieldnames = ["person_id"] + _fieldnames
        with gzip.open(_cmte_path, "wt", encoding="utf-8", newline="") as _f:
            _w = csv.DictWriter(_f, fieldnames=_fieldnames, extrasaction="ignore")
            _w.writeheader()
            _w.writerows(_cmte_rows)

        log.info(f"  committees     {_matched:,}/{len(_cmte_rows):,} matched to candidates by CO_ID")

        # ── Log output file stats ─────────────────────────────────────────────
        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    cmte_count,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    cand_count,
                        role="output", bytes=_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=cmte_count, candidates=cand_count)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=cmte_count, candidates=cand_count)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=cmte_count, candidates=cand_count,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


# =============================== CLI ==================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
