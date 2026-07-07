"""
parsers/massachusetts.py — Transform Massachusetts OCPF ZIPs into 5 normalized relations.

Input:  data/Massachusetts/raw/
  ocpf-filers.zip              — all registered filers (all_filers.txt inside)
  ocpf-{year}-reports.zip      — one per year 2002–present, each containing:
      reports.txt              — filing summaries with CPF_ID, office, report year
      report-items.txt         — all transactions, one row per item, tagged by Record_Type_ID

Output: data/Massachusetts/cleaned/
  contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
  committees.csv.gz, candidates.csv.gz

Record_Type_ID routing
──────────────────────
  Contributions : 201 Individual, 202 Committee, 203 Union/Association,
                  204 Non-Contribution Receipt, 210 Payroll Deduction,
                  211 Business/Corporation; 401–403/405 in-kind variants
  Expenditures  : 301 Expenditure, 303 Contribution-to-committee, 307 Reimbursement,
                  308 Credit Card Payment, 309 Vendor Payment,
                  311 Bank-Reported Expenditure (majority), 315 IE,
                  316 Administrative Expense, 319 Merchant Provider Fee,
                  332 Out-of-Pocket Candidate Expense
  Loans/Debts   : 206 Candidate Loan, 331 Out-of-Pocket Loan,
                  501 Liability, 502 IE Liability
  Skip          : sub-items (351/354/951/952), aggregated unitemized (220/320/420),
                  bank/savings entries (205/207/700–754/801), accounting entries
                  (302/304/305/310/314/317), IEPAC transfers (209/317), and all others
                  D102 Year-End Report (Report_Type_ID=11) monetary items (types 201–211,
                  301–332) are skipped for any CPF_ID that also filed Deposit Reports (type 60)
                  in the same ZIP — they duplicate the periodic filings.  In-kind types
                  (401/402/403/405) and loan/liability types (206/331/501/502) on D102 reports
                  are always kept: deposit reports carry only cash, and loan records in D102
                  are never exact duplicates of deposit-report loans (verified 0 overlaps).

Join path: report-items.txt → reports.txt on Report_ID → CPF_ID → filer registry

Notes
─────
  - Bank-reported expenditures (type 311) are the bulk of expenditures for depository
    committees — Massachusetts requires most candidates to use a bank as a depository,
    and the bank files these reports on the committee's behalf.  Clarified_Name and
    Clarified_Purpose override the raw bank-filed name/purpose when available.
  - Reimbursement (307), credit card (308), and vendor (309) records are wrapper
    records — their sub-items (351/354/951) are skipped to avoid double-counting.
  - Amended reports are already excluded at source: OCPF's bulk ZIPs contain only the
    latest version of each report.  No amendment deduplication is needed here.
  - person_id model: "committee" — CPF ID is assigned per committee registration;
    the same candidate gets a new CPF ID for each new campaign committee.
"""

import csv
import gzip
import io
import sys
import time
import zipfile
from datetime import datetime, date
from pathlib import Path

from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Massachusetts" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Massachusetts" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "MA"
MAX_VALID_YEAR = date.today().year + 2

# ========================= record type maps ============================

# Items that map to the contributions output table
CONTRIBUTION_TYPES = {
    201: "Individual Contribution",
    202: "Committee Contribution",
    203: "Union/Association Contribution",
    204: "Non-Contribution Receipt",
    210: "Voluntary Payroll Deduction",
    211: "Business/Corporation Contribution",
    401: "Individual In-Kind Contribution",
    402: "Committee In-Kind Contribution",
    403: "Union/Association In-Kind Contribution",
    405: "Corporate In-Kind Contribution",
}

# contributor_type derived from record type
CONTRIBUTOR_TYPE_MAP = {
    201: "Individual",
    202: "Committee",
    203: "Union/Association",
    204: "Other",
    210: "Individual",
    211: "Business/Corporation",
    401: "Individual",
    402: "Committee",
    403: "Union/Association",
    405: "Business/Corporation",
}

# Items that map to the expenditures output table.
# Note: 307/308/309 are wrapper records; their sub-items (351/354/951) are skipped.
EXPENDITURE_TYPES = {
    301: "Expenditure",
    303: "Contribution to Registered Committee",
    307: "Reimbursement",
    308: "Credit Card Payment",
    309: "Vendor Payment",
    311: "Bank-Reported Expenditure",
    315: "Independent Expenditure",
    316: "Administrative Expense",
    319: "Merchant Provider Fee",
    332: "Out-of-Pocket Candidate Expense",
}

# Items that map to the loans_debts output table
LOAN_TYPES = {
    206: "Candidate Loan",
    331: "Out-of-Pocket Loan",
    501: "Liability",
    502: "IE Liability",
}

# All record types we act on (union — everything else is skipped)
ACTIVE_TYPES = set(CONTRIBUTION_TYPES) | set(EXPENDITURE_TYPES) | set(LOAN_TYPES)

# Account type code → canonical committee_type string (written as-is; alias CSV
# maps these to pipeline canonicals at aggregate time)
ACCT_TYPE_MAP = {
    "D": "Candidate Committee",   # Depository Candidate
    "U": "Candidate Committee",   # Non-Depository (Legislative) Candidate
    "Z": "Candidate Committee",   # Legacy candidate (pre-code)
    "z": "Candidate Committee",   # Same as Z (lowercase variant in data)
    "N": "Candidate Committee",   # Legacy non-depository candidate
    "O": "Candidate Committee",   # Other legacy candidate
    "P": "PAC",
    "I": "Independent Expenditure",
    "L": "Party Committee",       # Local Party Committee (WTC)
    "E": "Party Committee",       # State Party Committee
    "X": "Ballot Measure",        # Referendum/Ballot Question Committee
    "Y": "PAC",                   # People's Committees (small-scale PAC variant)
    "V": "Other",                 # Segregated Accounts
    "S": "Other",                 # Nonpartisan Non-Depository (Barnstable Assembly)
    "B": "Other",                 # Banks (file bank reports on behalf of committees)
}

# Account type codes for filers that have an associated candidate name
CANDIDATE_ACCT_TYPES = {"D", "U", "Z", "z", "N", "O"}


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'1000.0000' → '1000.0', '' on failure."""
    v = (val or "").strip().replace(",", "")
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


def build_name(last: str, first: str) -> str:
    """Build 'Last, First' or just 'Last' when first is empty."""
    last  = (last  or "").strip()
    first = (first or "").strip()
    if first:
        return f"{last}, {first}"
    return last


def raw_zips() -> list[Path]:
    """Return year transaction ZIPs in chronological order, skipping zero-byte files."""
    return sorted(
        (f for f in RAW_DIR.glob("ocpf-*-reports.zip") if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ========================= filer registry =============================

def build_filer_registry(log) -> dict[str, dict]:
    """
    Load all_filers.txt from ocpf-filers.zip in a single pass.
    Returns a dict keyed by CPF ID string with normalized committee/candidate details
    for enriching transaction rows.
    """
    registry: dict[str, dict] = {}
    filers_zip = RAW_DIR / "ocpf-filers.zip"
    if not filers_zip.exists():
        log.warning("ocpf-filers.zip not found — run scraper first")
        return registry

    count = 0
    with zipfile.ZipFile(filers_zip) as zf:
        with zf.open("all_filers.txt") as raw:
            reader = csv.DictReader(
                io.TextIOWrapper(raw, encoding="utf-8", errors="replace"),
                delimiter="\t",
            )
            for row in reader:
                cpf_id = clean(row.get("CPF ID", ""))
                if not cpf_id:
                    continue

                acct    = clean(row.get("Account Type Code", ""))
                comm    = clean(row.get("Comm_Name", ""))
                c_first = clean(row.get("Candidate First Name", ""))
                c_last  = clean(row.get("Candidate Last Name", ""))
                cand    = build_name(c_last, c_first) if c_last else ""
                t_first = clean(row.get("Treasurer First Name", ""))
                t_last  = clean(row.get("Treasurer Last Name", ""))
                treas   = build_name(t_last, t_first) if t_last else ""
                office  = clean(row.get("Office Type Sought", ""))
                # "N/A" means the filer has no specific office — treat as blank
                if office in ("N/A",):
                    office = ""
                district = clean(row.get("District Name Sought", ""))
                if district in ("N/A",):
                    district = ""
                party   = clean(row.get("Party Affiliation", ""))
                city    = clean(row.get("Comm City", ""))
                zipcode = clean(row.get("Comm Zip Code", ""))
                closed  = clean(row.get("Closed Date", ""))

                # When Comm_Name is blank (common for very old pre-digital registrations),
                # synthesize it from the candidate name
                if not comm and cand:
                    comm = cand + " Committee"

                registry[cpf_id] = {
                    "acct_type":       acct,
                    "committee_name":  comm,
                    "committee_type":  ACCT_TYPE_MAP.get(acct, "Other"),
                    "candidate_name":  cand,
                    "candidate_first": c_first,
                    "candidate_last":  c_last,
                    "treasurer_name":  treas,
                    "office":          office,
                    "district":        district,
                    "party":           party,
                    "city":            city,
                    "zip":             zipcode,
                    # Closed date present → committee has dissolved
                    "active":          "0" if closed else "1",
                }
                count += 1

    log.registry_loaded("all_filers.txt", count, relation="filers",
                        bytes=filers_zip.stat().st_size)
    return registry


# ======================= year ZIP parsing =============================

def load_reports(zf: zipfile.ZipFile) -> tuple[dict[str, dict], set[str]]:
    """
    Load reports.txt from an open ZipFile.

    Returns:
      rpt         — {Report_ID: {cpf_id, report_year, report_type_id, office,
                                  district, comm_name}}
      deposit_cpfs — set of CPF_IDs that filed at least one Deposit Report
                     (Report_Type_ID = 60) in this ZIP.  Used to suppress the
                     duplicate summary entries in D102 Year-End Reports (type 11).

    The office/district here reflect the candidate's position at the time of
    filing (from OCPF_Office / OCPF_District) — more accurate than the static
    filer registry when a candidate has sought multiple offices over time.
    """
    rpt: dict[str, dict] = {}
    deposit_cpfs: set[str] = set()
    with zf.open("reports.txt") as raw:
        reader = csv.DictReader(
            io.TextIOWrapper(raw, encoding="utf-8", errors="replace"),
            delimiter="\t",
        )
        for row in reader:
            rid = clean(row.get("Report_ID", ""))
            if not rid:
                continue
            cpf_id      = clean(row.get("CPF_ID", ""))
            type_id     = clean(row.get("Report_Type_ID", ""))
            rpt[rid] = {
                "cpf_id":         cpf_id,
                "report_year":    clean(row.get("Report_Year", "")),
                "report_type_id": type_id,
                # OCPF_Office / OCPF_District are populated from the filer's registration
                # at the time the report was filed — useful for candidates who sought
                # different offices across years
                "office":         clean(row.get("OCPF_Office", "")),
                "district":       clean(row.get("OCPF_District", "")),
                "comm_name":      clean(row.get("OCPF_Comm_Name", "")),
            }
            if type_id == "60":
                deposit_cpfs.add(cpf_id)
    return rpt, deposit_cpfs


# ================================ run =================================

def run():
    log = get_logger("massachusetts", "parse")
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

        # ── Build filer registry + write committees and candidates ─────
        # Single pass over all_filers.txt — build lookup dict and write entity rows
        filer_reg: dict[str, dict] = {}
        filers_zip = RAW_DIR / "ocpf-filers.zip"

        if filers_zip.exists():
            with zipfile.ZipFile(filers_zip) as zf:
                with zf.open("all_filers.txt") as raw:
                    reader = csv.DictReader(
                        io.TextIOWrapper(raw, encoding="utf-8", errors="replace"),
                        delimiter="\t",
                    )
                    for row_num, row in enumerate(reader, start=2):
                        cpf_id = clean(row.get("CPF ID", ""))
                        if not cpf_id:
                            continue

                        acct    = clean(row.get("Account Type Code", ""))
                        comm    = clean(row.get("Comm_Name", ""))
                        c_first = clean(row.get("Candidate First Name", ""))
                        c_last  = clean(row.get("Candidate Last Name", ""))
                        cand    = build_name(c_last, c_first) if c_last else ""
                        t_first = clean(row.get("Treasurer First Name", ""))
                        t_last  = clean(row.get("Treasurer Last Name", ""))
                        treas   = build_name(t_last, t_first) if t_last else ""
                        office  = clean(row.get("Office Type Sought", ""))
                        if office in ("N/A",):
                            office = ""
                        district = clean(row.get("District Name Sought", ""))
                        if district in ("N/A",):
                            district = ""
                        party   = clean(row.get("Party Affiliation", ""))
                        city    = clean(row.get("Comm City", ""))
                        zipcode = clean(row.get("Comm Zip Code", ""))
                        closed  = clean(row.get("Closed Date", ""))

                        if not comm and cand:
                            comm = cand + " Committee"

                        cmte_type = ACCT_TYPE_MAP.get(acct, "Other")
                        active    = "0" if closed else "1"

                        reg = {
                            "acct_type":       acct,
                            "committee_name":  comm,
                            "committee_type":  cmte_type,
                            "candidate_name":  cand,
                            "candidate_first": c_first,
                            "candidate_last":  c_last,
                            "treasurer_name":  treas,
                            "office":          office,
                            "district":        district,
                            "party":           party,
                            "city":            city,
                            "zip":             zipcode,
                            "active":          active,
                        }
                        filer_reg[cpf_id] = reg

                        # Committee row — every registered filer has an account
                        cmte_w.writerow({
                            "state":          STATE,
                            "state_filer_id": cpf_id,
                            "committee_name": utils.clean_name(comm),
                            "committee_type": cmte_type,
                            "candidate_name": utils.clean_name(cand),
                            "treasurer_name": treas,
                            "city":           city,
                            "zip":            zipcode,
                            "active":         active,
                            "raw_file":       "all_filers.txt",
                            "row_num":        row_num,
                        })
                        committees_written += 1

                        # Candidate row — only filers with a candidate name and a
                        # candidate-type account code
                        if cand and acct in CANDIDATE_ACCT_TYPES:
                            cand_w.writerow({
                                "state":           STATE,
                                "state_filer_id":  cpf_id,
                                "candidate_name":  utils.clean_name(cand),
                                "candidate_first": c_first,
                                "candidate_last":  c_last,
                                "office":          office,
                                "district":        district,
                                "party":           party,
                                "raw_file":        "all_filers.txt",
                                "row_num":         row_num,
                            })
                            candidates_written += 1

            log.registry_loaded("all_filers.txt", committees_written,
                                 relation="filers",
                                 bytes=filers_zip.stat().st_size)
            log.info(f"  committees: {committees_written:,}  "
                     f"candidates: {candidates_written:,}")
        else:
            log.warning("ocpf-filers.zip not found — entity tables will be empty")

        # ── Process each year ZIP ──────────────────────────────────────
        year_zips = raw_zips()
        log.info(f"Processing {len(year_zips)} year ZIPs")

        for zip_path in year_zips:
            ft = time.perf_counter()
            cont_count = expn_count = loan_count = skipped = 0

            try:
                with zipfile.ZipFile(zip_path) as zf:
                    # Build Report_ID → metadata lookup + set of CPFs with deposit reports
                    rpt_lookup, deposit_cpfs = load_reports(zf)

                    # Stream report-items.txt — the main transaction file
                    with zf.open("report-items.txt") as raw:
                        reader = csv.DictReader(
                            io.TextIOWrapper(raw, encoding="utf-8", errors="replace"),
                            delimiter="\t",
                        )
                        for row_num, row in enumerate(
                            tqdm(reader, desc=f"  {zip_path.name}",
                                 unit="row", dynamic_ncols=True, leave=False),
                            start=2,
                        ):
                            rtype_raw = clean(row.get("Record_Type_ID", ""))
                            if not rtype_raw:
                                continue
                            try:
                                rtype = int(rtype_raw)
                            except ValueError:
                                continue

                            if rtype not in ACTIVE_TYPES:
                                skipped += 1
                                continue

                            amount = parse_amount(row.get("Amount", ""))
                            if not amount:
                                skipped += 1
                                continue

                            # Look up the filing committee via Report_ID → CPF_ID
                            rid  = clean(row.get("Report_ID", ""))
                            rpt  = rpt_lookup.get(rid, {})
                            cpf  = rpt.get("cpf_id", "")

                            # D102 Year-End Report (type 11) deduplication:
                            # For depository candidates, the D102 re-lists all monetary
                            # contributions and expenditures already in their periodic
                            # Deposit Reports — skip those to avoid double-counting.
                            # Two categories are always kept regardless of report type:
                            #   In-kinds (401/402/403/405): deposit reports carry only cash;
                            #     D102 is the sole source of in-kind data for these filers.
                            #   Loans/liabilities (206/331/501/502): loan records in D102
                            #     are never exact duplicates of deposit-report loan records
                            #     (verified: 0 exact overlaps across all sample years).
                            if (rpt.get("report_type_id") == "11"
                                    and cpf in deposit_cpfs
                                    and rtype not in (401, 402, 403, 405,
                                                      206, 331, 501, 502)):
                                skipped += 1
                                continue

                            reg  = filer_reg.get(cpf, {})

                            # Committee name: prefer report-level snapshot (captures
                            # the name at time of filing, before any later changes)
                            comm_name = rpt.get("comm_name") or reg.get("committee_name", "")
                            cand_name = reg.get("candidate_name", "")

                            # Office/district: report-level (time-of-filing) beats registry
                            office   = rpt.get("office")   or reg.get("office", "")
                            district = rpt.get("district") or reg.get("district", "")
                            rpt_year = rpt.get("report_year", "")

                            # Counterparty name: "Last, First" for individuals,
                            # plain Name for organizations and committees
                            name  = clean(row.get("Name", ""))
                            first = clean(row.get("First_Name", ""))
                            counterparty = build_name(name, first) if first else name

                            item_id = clean(row.get("Item_ID", ""))

                            # ── Route by record type ─────────────────────────────

                            if rtype in CONTRIBUTION_TYPES:
                                cont_w.writerow({
                                    "state":             STATE,
                                    "committee_name":    utils.clean_name(comm_name),
                                    "contributor_name":  counterparty,
                                    "amount":            amount,
                                    "date":              parse_date(row.get("Date", "")),
                                    "transaction_type":  CONTRIBUTION_TYPES[rtype],
                                    "contributor_type":  CONTRIBUTOR_TYPE_MAP.get(rtype, ""),
                                    "contributor_city":  clean(row.get("City", "")),
                                    "contributor_state": clean(row.get("State", "")),
                                    "contributor_zip":   clean(row.get("Zip", "")),
                                    "employer":          clean(row.get("Employer", "")),
                                    "occupation":        clean(row.get("Occupation", "")),
                                    "candidate_name":    utils.clean_name(cand_name),
                                    "office":            office,
                                    "election_year":     rpt_year,
                                    "filing_id":         item_id,
                                    "raw_file":          zip_path.name,
                                    "row_num":           row_num,
                                })
                                cont_count += 1

                            elif rtype in EXPENDITURE_TYPES:
                                # For bank-reported expenditures (311), OCPF lets the
                                # committee add Clarified_Name and Clarified_Purpose after
                                # the bank files — use those when present
                                payee = (clean(row.get("Clarified_Name", ""))
                                         or counterparty)
                                purpose = clean(row.get("Description", ""))
                                if rtype == 311:
                                    clarified = clean(row.get("Clarified_Purpose", ""))
                                    if clarified:
                                        purpose = clarified

                                expn_w.writerow({
                                    "state":            STATE,
                                    "committee_name":   utils.clean_name(comm_name),
                                    "payee_name":       payee,
                                    "amount":           amount,
                                    "date":             parse_date(row.get("Date", "")),
                                    "transaction_type": EXPENDITURE_TYPES[rtype],
                                    "purpose":          purpose,
                                    "payee_city":       clean(row.get("City", "")),
                                    "payee_state":      clean(row.get("State", "")),
                                    "payee_zip":        clean(row.get("Zip", "")),
                                    "candidate_name":   utils.clean_name(cand_name),
                                    "office":           office,
                                    "election_year":    rpt_year,
                                    "filing_id":        item_id,
                                    "raw_file":         zip_path.name,
                                    "row_num":          row_num,
                                })
                                expn_count += 1

                            elif rtype in LOAN_TYPES:
                                loan_w.writerow({
                                    "state":              STATE,
                                    "committee_name":     utils.clean_name(comm_name),
                                    "original_amount":    amount,
                                    "date":               parse_date(row.get("Date", "")),
                                    "record_type":        LOAN_TYPES[rtype],
                                    "counterparty_name":  counterparty,
                                    "counterparty_city":  clean(row.get("City", "")),
                                    "counterparty_state": clean(row.get("State", "")),
                                    "counterparty_zip":   clean(row.get("Zip", "")),
                                    "candidate_name":     utils.clean_name(cand_name),
                                    "election_year":      rpt_year,
                                    "filing_id":          item_id,
                                    "raw_file":           zip_path.name,
                                    "row_num":            row_num,
                                })
                                loan_count += 1

            except zipfile.BadZipFile as e:
                log.warning(f"  {zip_path.name}: bad ZIP — {e}")
                continue

            log.file_parsed(
                zip_path.name, "transactions",
                cont_count + expn_count + loan_count,
                skipped,
                duration_s=round(time.perf_counter() - ft, 2),
                bytes=zip_path.stat().st_size,
                contributions=cont_count,
                expenditures=expn_count,
                loans=loan_count,
            )
            total_contributions += cont_count
            total_expenditures  += expn_count
            total_loans         += loan_count

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # CPF ID is per-registration (each committee cycle gets a new ID) —
        # use committee model: person_id = min(state_filer_id) per candidate name/office
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
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(
            f"Done in {duration}s — "
            f"{total_contributions:,} contributions, "
            f"{total_expenditures:,} expenditures, "
            f"{total_loans:,} loans, "
            f"{committees_written:,} committees, "
            f"{candidates_written:,} candidates"
        )
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
