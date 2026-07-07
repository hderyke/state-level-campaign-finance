"""
hawaii.py — Parse Hawaii CSC raw Socrata exports into canonical cleaned CSVs.

Raw files (all in data/Hawaii/raw/):
  CCSchedA_YYYY.csv  — CC Contributions Received      -> contributions
  CCSchedB_YYYY.csv  — CC Expenditures Made           -> expenditures
  CCSchedC_YYYY.csv  — CC Other Receipts              -> contributions
  CCSchedD_YYYY.csv  — CC Loans Received              -> loans_debts
  CCSchedE_YYYY.csv  — CC Unpaid Expenditures         -> loans_debts
  CCSchedF_YYYY.csv  — CC Durable Assets              -> loans_debts
  NCSchedA_YYYY.csv  — NC Contributions Received      -> contributions
  NCSchedB1_YYYY.csv — NC Contributions to Candidates -> expenditures
  NCSchedB2_YYYY.csv — NC Expenditures Made           -> expenditures
  NCSchedC_YYYY.csv  — NC Other Receipts              -> contributions
  NCSchedD_YYYY.csv  — NC Unpaid Expenditures         -> loans_debts
  NCSchedE_YYYY.csv  — NC Durable Assets              -> loans_debts
  SOI_all.csv        — Statement of Intent (candidate registry, all years)
  Affidavits_all.csv — Affidavits (candidate registry, all years)

Candidate Committees (CC) are keyed by reg_no (e.g. "CC12091"); Noncandidate
Committees (NC) are keyed by reg_no (e.g. "NC20717"). state_filer_id = reg_no
for both candidates and committees. Per Henry: CC committee_name falls back to
candidate_name; NC committee_name falls back to noncandidate_committee_name
(neither side has a separate committee-name field in the source data).

SOI + Affidavits are processed first to seed the candidate/committee registry
with office/district/county/election_year for every registrant, regardless of
whether they have any transactions yet — this is what gives the candidates
table high fill rates for src/pipeline/validate.py's required fields.

Output (data/Hawaii/cleaned/):
  contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
  committees.csv.gz, candidates.csv.gz
"""

import csv
import gzip
import re
import sys
import time
from pathlib import Path
from datetime import date, datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Hawaii" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Hawaii" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "HI"
EARLIEST_YEAR  = 1990
MAX_VALID_YEAR = date.today().year + 4


# ============================== Helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Parse a dollar amount string to a plain numeric string; parentheses become negative. Returns '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
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
    """Socrata floating timestamp 'YYYY-MM-DDTHH:MM:SS.000' -> 'YYYY-MM-DD'. Returns '' on failure or out-of-range year."""
    v = (val or "").strip()
    if not v:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", v)
    if not m:
        return ""
    try:
        d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return ""
    if d.year < EARLIEST_YEAR or d.year > MAX_VALID_YEAR:
        return ""
    return d.strftime("%Y-%m-%d")


def parse_election_year(period: str) -> str:
    """Extract the latest 4-digit year from an election_period like '2024-2026'."""
    years = re.findall(r"\d{4}", period or "")
    return str(max(int(y) for y in years)) if years else ""


def split_name(raw: str) -> tuple[str, str]:
    """'Last, First Middle' -> (first_middle, last). No comma -> (\"\", raw)."""
    raw = clean(raw)
    if "," in raw:
        last, _, first = raw.partition(",")
        return first.strip(), last.strip()
    return "", raw


def format_name(raw: str) -> str:
    """'Last, First Middle' -> 'First Middle Last'. No comma -> raw unchanged."""
    first, last = split_name(raw)
    if first and last:
        return f"{first} {last}"
    return last or first


def raw_files(pattern: str) -> list[Path]:
    """Return non-empty raw files matching a glob pattern, sorted by name."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    """Open a gzipped CSV writer in CLEAN_DIR; extra fields are dropped, missing fields default to ''."""
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def _fill(d: dict, key: str, val: str) -> None:
    """Set d[key] = val only if val is truthy and d[key] is currently empty."""
    if val and not d.get(key):
        d[key] = val


# ================================ Main ================================
def run():
    log = get_logger("hawaii", "parse")
    t0  = time.perf_counter()
    log.info("Starting Hawaii parser")
    log._emit("parse_started")

    candidates: dict[str, dict] = {}   # keyed by CC reg_no
    committees: dict[str, dict] = {}   # keyed by reg_no (CC or NC)

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0

    file_handles = []

    # =================== Registry helpers ===================
    def register_cc(reg_no: str, raw_name: str, office: str = "", district: str = "",
                     county: str = "", party: str = "", election_year: str = "",
                     raw_file: str = "", row_num="") -> None:
        """Register/enrich a Candidate Committee (reg_no starts with 'CC')."""
        reg_no = clean(reg_no)
        if not reg_no:
            return

        cand = candidates.get(reg_no)
        if cand is None:
            first, last = split_name(raw_name)
            cand = {
                "state":           STATE,
                "candidate_name":  format_name(raw_name),
                "candidate_first": first,
                "candidate_last":  last,
                "office":          "",
                "district":        "",
                "jurisdiction":    "",
                "party":           "",
                "election_year":   "",
                "incumbent":       "",
                "state_filer_id":  reg_no,
                "raw_file":        raw_file,
                "row_num":         row_num,
            }
            candidates[reg_no] = cand
        _fill(cand, "office",       clean(office))
        _fill(cand, "district",     clean(district))
        _fill(cand, "jurisdiction", clean(county))
        _fill(cand, "party",        clean(party))
        _fill(cand, "election_year", clean(election_year))

        cmte = committees.get(reg_no)
        if cmte is None:
            cname = format_name(raw_name)
            committees[reg_no] = {
                "state":           STATE,
                "committee_name":  cname,
                "committee_type":  "Candidate Committee",
                "election_year":   clean(election_year),
                "candidate_name":  cname,
                "treasurer_name":  "",
                "city":            "",
                "zip":             "",
                "active":          "",
                "state_filer_id":  reg_no,
                "raw_file":        raw_file,
                "row_num":         row_num,
            }
        else:
            _fill(cmte, "election_year", clean(election_year))

    def register_nc(reg_no: str, raw_name: str, election_year: str = "",
                     raw_file: str = "", row_num="") -> None:
        """Register/enrich a Noncandidate Committee / PAC (reg_no starts with 'NC')."""
        reg_no = clean(reg_no)
        if not reg_no:
            return

        cmte = committees.get(reg_no)
        if cmte is None:
            committees[reg_no] = {
                "state":           STATE,
                "committee_name":  utils.clean_name(raw_name),
                "committee_type":  "Noncandidate Committee",
                "election_year":   clean(election_year),
                "candidate_name":  "",
                "treasurer_name":  "",
                "city":            "",
                "zip":             "",
                "active":          "",
                "state_filer_id":  reg_no,
                "raw_file":        raw_file,
                "row_num":         row_num,
            }
        else:
            _fill(cmte, "election_year", clean(election_year))

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, loan_fh]

        # =================== Entity registries ===================
        # SOI + Affidavits: all-years candidate registries, independent of
        # transaction history. Processed first so every registrant gets a
        # candidates + committees row even with zero transactions.
        for fname in ("SOI_all.csv", "Affidavits_all.csv"):
            path = RAW_DIR / fname
            if not (path.exists() and path.stat().st_size > 0):
                continue
            ft    = time.perf_counter()
            count = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    reg_no = clean(row.get("reg_no", ""))
                    if not reg_no:
                        continue
                    register_cc(
                        reg_no, row.get("candidate_name", ""),
                        office=row.get("office", ""),
                        district=row.get("district", ""),
                        county=row.get("county", ""),
                        election_year=clean(row.get("election", "")) or
                                       parse_election_year(row.get("election_period", "")),
                        raw_file=fname, row_num=row_num,
                    )
                    count += 1
            log.registry_loaded(fname, entries=count, relation="candidates",
                                bytes=path.stat().st_size)

        # =================== CC Schedule A — Contributions Received ===================
        for path in raw_files("CCSchedA_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no   = clean(row.get("reg_no", ""))
                    raw_name = row.get("candidate_name", "")
                    cname    = format_name(raw_name)
                    ey       = parse_election_year(row.get("election_period", ""))
                    register_cc(reg_no, raw_name, office=row.get("office", ""),
                                district=row.get("district", ""), county=row.get("county", ""),
                                party=row.get("party", ""), election_year=ey,
                                raw_file=path.name, row_num=row_num)
                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cname,
                        "amount":            amount,
                        "date":              parse_date(row.get("date", "")),
                        "transaction_type":  "Contribution",
                        "contributor_name":  utils.clean_name(row.get("contributor_name", "")),
                        "contributor_type":  clean(row.get("contributor_type", "")),
                        "contributor_city":  utils.clean_name(row.get("city", "")),
                        "contributor_state": clean(row.get("state", "")),
                        "contributor_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "employer":          utils.clean_name(row.get("employer", "")),
                        "occupation":        utils.clean_name(row.get("occupation", "")),
                        "candidate_name":    cname,
                        "office":            clean(row.get("office", "")),
                        "election_year":     ey,
                        "amended":           "",
                        "filing_id":         "",
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_contributions += count

        # =================== CC Schedule B — Expenditures Made ===================
        for path in raw_files("CCSchedB_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no   = clean(row.get("reg_no", ""))
                    raw_name = row.get("candidate_name", "")
                    cname    = format_name(raw_name)
                    ey       = parse_election_year(row.get("election_period", ""))
                    register_cc(reg_no, raw_name, office=row.get("office", ""),
                                district=row.get("district", ""), election_year=ey,
                                raw_file=path.name, row_num=row_num)
                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cname,
                        "amount":           amount,
                        "date":             parse_date(row.get("date", "")),
                        "transaction_type": clean(row.get("authorized_use", "")),
                        "payee_name":       utils.clean_name(row.get("vendor_name", "")),
                        "purpose":          clean(row.get("purpose_of_expenditure", "")),
                        "category":         clean(row.get("expenditure_category", "")),
                        "payee_city":       utils.clean_name(row.get("city", "")),
                        "payee_state":      clean(row.get("state", "")),
                        "payee_zip":        utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":   cname,
                        "office":           clean(row.get("office", "")),
                        "election_year":    ey,
                        "amended":          "",
                        "filing_id":        "",
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_expenditures += count

        # =================== CC Schedule C — Other Receipts ===================
        for path in raw_files("CCSchedC_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no   = clean(row.get("reg_no", ""))
                    raw_name = row.get("candidate_name", "")
                    cname    = format_name(raw_name)
                    ey       = parse_election_year(row.get("election_period", ""))
                    register_cc(reg_no, raw_name, office=row.get("office", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)
                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cname,
                        "amount":            amount,
                        "date":              parse_date(row.get("date", "")),
                        "transaction_type":  clean(row.get("other_receipt_category", "")) or "Other Receipt",
                        "contributor_name":  utils.clean_name(row.get("source_name", "")),
                        "contributor_type":  clean(row.get("source_type", "")),
                        "contributor_city":  utils.clean_name(row.get("city", "")),
                        "contributor_state": clean(row.get("state", "")),
                        "contributor_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "employer":          "",
                        "occupation":        "",
                        "candidate_name":    cname,
                        "office":            clean(row.get("office", "")),
                        "election_year":     ey,
                        "amended":           "",
                        "filing_id":         "",
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_contributions += count

        # =================== CC Schedule D — Loans Received ===================
        for path in raw_files("CCSchedD_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no   = clean(row.get("reg_no", ""))
                    raw_name = row.get("candidate_name", "")
                    cname    = format_name(raw_name)
                    ey       = parse_election_year(row.get("election_period", ""))
                    register_cc(reg_no, raw_name, office=row.get("office", ""),
                                district=row.get("district", ""), county=row.get("county", ""),
                                party=row.get("party", ""), election_year=ey,
                                raw_file=path.name, row_num=row_num)
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cname,
                        "original_amount":    amount,
                        "date":               parse_date(row.get("date", "")),
                        "record_type":        "Loan Received",
                        "counterparty_name":  utils.clean_name(row.get("lender_name", "")),
                        "counterparty_city":  utils.clean_name(row.get("city", "")),
                        "counterparty_state": clean(row.get("state", "")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":     cname,
                        "election_year":      ey,
                        "amended":            "",
                        "filing_id":          clean(row.get("loan_id", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_loans += count

        # =================== CC Schedule E — Unpaid Expenditures ===================
        for path in raw_files("CCSchedE_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no   = clean(row.get("reg_no", ""))
                    raw_name = row.get("candidate_name", "")
                    cname    = format_name(raw_name)
                    ey       = parse_election_year(row.get("election_period", ""))
                    register_cc(reg_no, raw_name, office=row.get("office", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cname,
                        "original_amount":    amount,
                        "date":               parse_date(row.get("date", "")),
                        "record_type":        "Unpaid Expenditure",
                        "counterparty_name":  utils.clean_name(row.get("vendor_name", "")),
                        "counterparty_city":  utils.clean_name(row.get("city", "")),
                        "counterparty_state": clean(row.get("state", "")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":     cname,
                        "election_year":      ey,
                        "amended":            "",
                        "filing_id":          clean(row.get("unpaid_expenditure_id", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_loans += count

        # =================== CC Schedule F — Durable Assets ===================
        for path in raw_files("CCSchedF_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no   = clean(row.get("reg_no", ""))
                    raw_name = row.get("candidate_name", "")
                    cname    = format_name(raw_name)
                    ey       = parse_election_year(row.get("election_period", ""))
                    register_cc(reg_no, raw_name, office=row.get("office", ""),
                                district=row.get("district", ""), county=row.get("county", ""),
                                party=row.get("party", ""), election_year=ey,
                                raw_file=path.name, row_num=row_num)
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cname,
                        "original_amount":    amount,
                        "date":               parse_date(row.get("date", "")),
                        "record_type":        "Durable Asset",
                        "counterparty_name":  utils.clean_name(row.get("vendor_name", "")),
                        "counterparty_city":  utils.clean_name(row.get("city", "")),
                        "counterparty_state": clean(row.get("state", "")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":     cname,
                        "election_year":      ey,
                        "amended":            "",
                        "filing_id":          clean(row.get("durable_asset_id", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_loans += count

        # =================== NC Schedule A — Contributions Received ===================
        for path in raw_files("NCSchedA_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no = clean(row.get("reg_no", ""))
                    cname  = utils.clean_name(row.get("noncandidate_committee_name", ""))
                    ey     = parse_election_year(row.get("election_period", ""))
                    register_nc(reg_no, row.get("noncandidate_committee_name", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)
                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cname,
                        "amount":            amount,
                        "date":              parse_date(row.get("date", "")),
                        "transaction_type":  "Contribution",
                        "contributor_name":  utils.clean_name(row.get("contributor_name", "")),
                        "contributor_type":  clean(row.get("contributor_type", "")),
                        "contributor_city":  utils.clean_name(row.get("city", "")),
                        "contributor_state": clean(row.get("state", "")),
                        "contributor_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "employer":          "",
                        "occupation":        "",
                        "candidate_name":    "",
                        "office":            "",
                        "election_year":     ey,
                        "amended":           "",
                        "filing_id":         "",
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_contributions += count

        # =================== NC Schedule B1 — Contributions Made To Candidates ===================
        # NC committee -> CC candidate. From the NC committee's books this is an
        # outflow, recorded as an expenditure. Also enriches the recipient CC
        # committee/candidate registry via cc_reg_no.
        for path in raw_files("NCSchedB1_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no = clean(row.get("reg_no", ""))
                    ncname = utils.clean_name(row.get("noncandidate_committee_name", ""))
                    ey     = parse_election_year(row.get("election_period", ""))
                    register_nc(reg_no, row.get("noncandidate_committee_name", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)

                    cc_reg = clean(row.get("cc_reg_no", ""))
                    cand_raw = row.get("candidate_name", "")
                    if cc_reg:
                        register_cc(cc_reg, cand_raw or row.get("candidate_committee_name", ""),
                                    office=row.get("office", ""), district=row.get("district", ""),
                                    county=row.get("county", ""), party=row.get("party", ""),
                                    election_year=ey, raw_file=path.name, row_num=row_num)

                    payee = format_name(row.get("candidate_committee_name", "")) or format_name(cand_raw)
                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   ncname,
                        "amount":           amount,
                        "date":             parse_date(row.get("date", "")),
                        "transaction_type": "Contribution to Candidate",
                        "payee_name":       payee,
                        "purpose":          clean(row.get("non_monetary_category", "")),
                        "category":         "Contribution to Candidate",
                        "payee_city":       utils.clean_name(row.get("city", "")),
                        "payee_state":      clean(row.get("state", "")),
                        "payee_zip":        utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":   format_name(cand_raw),
                        "office":           clean(row.get("office", "")),
                        "election_year":    ey,
                        "amended":          "",
                        "filing_id":        "",
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_expenditures += count

        # =================== NC Schedule B2 — Expenditures Made ===================
        for path in raw_files("NCSchedB2_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no = clean(row.get("reg_no", ""))
                    cname  = utils.clean_name(row.get("noncandidate_committee_name", ""))
                    ey     = parse_election_year(row.get("election_period", ""))
                    register_nc(reg_no, row.get("noncandidate_committee_name", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)
                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cname,
                        "amount":           amount,
                        "date":             parse_date(row.get("date", "")),
                        "transaction_type": clean(row.get("independent_expenditure", "")) or "Expenditure",
                        "payee_name":       utils.clean_name(row.get("vendor_name", "")),
                        "purpose":          clean(row.get("purpose_of_expenditure", "")),
                        "category":         clean(row.get("expenditure_category", "")),
                        "payee_city":       utils.clean_name(row.get("city", "")),
                        "payee_state":      clean(row.get("state", "")),
                        "payee_zip":        utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":   utils.clean_name(row.get("candidate_name_s", "")),
                        "office":           "",
                        "election_year":    ey,
                        "amended":          "",
                        "filing_id":        "",
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_expenditures += count

        # =================== NC Schedule C — Other Receipts ===================
        for path in raw_files("NCSchedC_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no = clean(row.get("reg_no", ""))
                    cname  = utils.clean_name(row.get("noncandidate_committee_name", ""))
                    ey     = parse_election_year(row.get("election_period", ""))
                    register_nc(reg_no, row.get("noncandidate_committee_name", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)
                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cname,
                        "amount":            amount,
                        "date":              parse_date(row.get("date", "")),
                        "transaction_type":  clean(row.get("other_receipt_category", "")) or "Other Receipt",
                        "contributor_name":  utils.clean_name(row.get("source_name", "")),
                        "contributor_type":  clean(row.get("source_type", "")),
                        "contributor_city":  utils.clean_name(row.get("city", "")),
                        "contributor_state": clean(row.get("state", "")),
                        "contributor_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "employer":          "",
                        "occupation":        "",
                        "candidate_name":    "",
                        "office":            "",
                        "election_year":     ey,
                        "amended":           "",
                        "filing_id":         "",
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_contributions += count

        # =================== NC Schedule D — Unpaid Expenditures ===================
        for path in raw_files("NCSchedD_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no = clean(row.get("reg_no", ""))
                    cname  = utils.clean_name(row.get("noncandidate_committee_name", ""))
                    ey     = parse_election_year(row.get("election_period", ""))
                    register_nc(reg_no, row.get("noncandidate_committee_name", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cname,
                        "original_amount":    amount,
                        "date":               parse_date(row.get("date", "")),
                        "record_type":        "Unpaid Expenditure",
                        "counterparty_name":  utils.clean_name(row.get("vendor_name", "")),
                        "counterparty_city":  utils.clean_name(row.get("city", "")),
                        "counterparty_state": clean(row.get("state", "")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":     "",
                        "election_year":      ey,
                        "amended":            "",
                        "filing_id":          clean(row.get("unpaid_expenditure_id", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_loans += count

        # =================== NC Schedule E — Durable Assets ===================
        for path in raw_files("NCSchedE_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    reg_no = clean(row.get("reg_no", ""))
                    cname  = utils.clean_name(row.get("noncandidate_committee_name", ""))
                    ey     = parse_election_year(row.get("election_period", ""))
                    register_nc(reg_no, row.get("noncandidate_committee_name", ""),
                                election_year=ey, raw_file=path.name, row_num=row_num)
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cname,
                        "original_amount":    amount,
                        "date":               parse_date(row.get("date", "")),
                        "record_type":        "Durable Asset",
                        "counterparty_name":  utils.clean_name(row.get("vendor_name", "")),
                        "counterparty_city":  utils.clean_name(row.get("city", "")),
                        "counterparty_state": clean(row.get("state", "")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("zip_code", ""))),
                        "candidate_name":     "",
                        "election_year":      ey,
                        "amended":            "",
                        "filing_id":          clean(row.get("durable_asset_id", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_loans += count

        # =================== Flush candidates + committees ===================
        cand_fh, cand_w = open_writer("candidates.csv.gz", C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz", C.COMMITTEES)
        file_handles += [cand_fh, cmte_fh]

        for row in candidates.values():
            row["candidate_name"]  = utils.clean_name(row.get("candidate_name", ""))
            row["candidate_first"] = utils.clean_name(row.get("candidate_first", ""))
            row["candidate_last"]  = utils.clean_name(row.get("candidate_last", ""))
            row["office"]          = utils.clean_name(row.get("office", ""))
            row["district"]        = utils.clean_name(row.get("district", ""))
            row["jurisdiction"]    = utils.clean_name(row.get("jurisdiction", ""))
            row["party"]           = utils.clean_name(row.get("party", ""))
            cand_w.writerow(row)

        for row in committees.values():
            row["committee_name"] = utils.clean_name(row.get("committee_name", ""))
            row["candidate_name"] = utils.clean_name(row.get("candidate_name", ""))
            cmte_w.writerow(row)

        # Close handles before person-ID assignment
        for fh in file_handles:
            fh.close()
        file_handles = []

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions, role="output",
                        bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,  role="output",
                        bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,         role="output",
                        bytes=_out_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    len(committees),     role="output",
                        bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    len(candidates),     role="output",
                        bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees), candidates=len(candidates))

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees), candidates=len(candidates))
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees), candidates=len(candidates),
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


# ====== CLI ==================================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
