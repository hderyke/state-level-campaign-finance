"""
parsers/michigan.py — Parse Michigan campaign finance data.

Input files in data/Michigan/raw/:

  Transactions (tab-delimited .txt, one file per year):
    contribution_{year}.txt   — contributions to committees
      Columns: doc_seq_no, contribution_id, cont_detail_id, doc_stmnt_year,
               doc_type_desc, com_legal_name, common_name_acronym, cfr_com_id,
               com_type, can_first_name, can_last_name, contribtype,
               contributor_f_name, contributor_l_name_or_org, contributor_address,
               contributor_city, contributor_state, contributor_zip,
               contributor_occupation, contributor_employer,
               received_date, amount, aggregate, extra_desc, fundraiser
    expenditure_{year}.txt    — committee expenditures
      Columns: doc_seq_no, expenditure_type, gub_elec_type, expense_id,
               detail_id, doc_stmnt_year, doc_type_desc, com_legal_name,
               common_name_acronym, cfr_com_id, com_type, exp_desc, purpose,
               payee_f_name, payee_l_name_or_org, payee_address, payee_city,
               payee_state, payee_zip, exp_date, amount, state_loc, supp_opp,
               candidate, office_dist
    receipt_{year}.txt        — other receipts (in-kind, refunds, transfers)
      Columns: same as contributions with payer_* instead of contributor_*;
               extra receipttype column at end
      Folded into contributions output; "Refund/Rebate" receipttype
      rows written to expenditures with negative amounts (sign reversal).

  Entities (from committee search + detail sweep):
    entities.csv              — one row per registered committee, columns:
      internal_id, committee_id, committee_type, committee_name,
      committee_status, candidate_last, candidate_first, candidate_middle,
      county, party, office_sought, office_sought_district, date_formed, scraped_at

Output files in data/Michigan/cleaned/:
  contributions.csv.gz, expenditures.csv.gz, candidates.csv.gz,
  committees.csv.gz, loans_debts.csv.gz

Notes:
  - cfr_com_id in transaction files = committee_id in entities.csv = state_filer_id.
  - contribtype "Direct Contributions - Loan" → loans_debts.csv.gz.
  - expenditure_type "Direct Expenditures - Loan Owed to/Given By" → loans_debts.
  - Contributor type (Individual vs Organization) is derived from whether
    contributor_f_name is non-blank (Individual) or blank (Organization);
    Michigan's contribtype field describes payment type, not donor type.
  - ZIP codes sometimes have trailing zeros ("489330000") — cleaned to 5-digit.
  - doc_stmnt_year used as election_year (filing statement year ≈ election cycle).
  - person_id model: "committee" — cfr_com_id increments per registration,
    same candidate gets a new ID each cycle.
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

RAW_DIR   = PROJECT_ROOT / "data" / "Michigan" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Michigan" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "MI"
MAX_VALID_YEAR = date.today().year + 2

# ========================= loan routing ===============================

# contribtype values that go to loans_debts instead of contributions
LOAN_CONTRIB_TYPES = {"Direct Contributions - Loan"}

# expenditure_type values that go to loans_debts instead of expenditures
LOAN_EXP_TYPES = {"Direct Expenditures - Loan Owed to/Given By"}

# receipttype values in the receipts file that indicate a refund/rebate —
# these are written to expenditures with a negative amount rather than
# contributions (they represent money leaving the committee).
REFUND_RECEIPT_TYPES = {"Refund/Rebate"}

# ============================== helpers ===============================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'$1,000.00' or '1000.00' → plain numeric string. '' on failure."""
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
    """MM/DD/YYYY or YYYY-MM-DD → YYYY-MM-DD. '' on failure or implausible year."""
    v = clean(val)
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def clean_zip(val: str) -> str:
    """
    Strip trailing zeros that bloat Michigan ZIPs ('489330000' → '48933').
    Keeps 5-digit and ZIP+4 formats; discards 9-digit padded variants.
    """
    v = clean(val)
    if not v:
        return ""
    # Remove non-digit/hyphen characters
    v = re.sub(r"[^\d\-]", "", v)
    # 9 raw digits: if the last 4 are all zeros, keep just the first 5
    if re.fullmatch(r"\d{9}", v) and v[5:] == "0000":
        return v[:5]
    # Standard 5 or ZIP+4
    if re.fullmatch(r"\d{5}(-\d{4})?", v):
        return v
    return v[:5] if len(v) >= 5 else v


def build_name(first: str, last_or_org: str) -> str:
    """Combine first and last/org name. Returns stripped combined string."""
    first      = clean(first)
    last_or_org = clean(last_or_org)
    if first and last_or_org:
        return f"{first} {last_or_org}"
    return first or last_or_org


def contributor_type_from_name(first: str) -> str:
    """Individual if a first name is present; Organization otherwise."""
    return "Individual" if clean(first) else "Organization"


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


# ========================= entity registry ============================

def load_entity_registry() -> dict[str, dict]:
    """
    Load entities.csv into a dict keyed by committee_id (= cfr_com_id in transactions).
    Provides enrichment for candidate_name, party, office, district.
    """
    reg  = {}
    path = RAW_DIR / "entities.csv"
    if not path.exists():
        return reg
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            cid = clean(row.get("committee_id", ""))
            if not cid:
                continue
            # cfr_com_id in transaction files is always zero-padded to 7 digits;
            # entities.csv may store older committee IDs without the leading zero.
            cid = cid.zfill(7)

            first  = clean(row.get("candidate_first", ""))
            middle = clean(row.get("candidate_middle", ""))
            last   = clean(row.get("candidate_last", ""))

            # Build candidate name as "FIRST [MIDDLE] LAST"
            parts = [p for p in [first, middle, last] if p]
            cand_name = " ".join(parts)

            reg[cid] = {
                "committee_name":   clean(row.get("committee_name", "")),
                "committee_type":   clean(row.get("committee_type", "")),
                "committee_status": clean(row.get("committee_status", "")),
                "candidate_first":  first,
                "candidate_middle": middle,
                "candidate_last":   last,
                "candidate_name":   cand_name,
                "party":            clean(row.get("party", "")),
                "office_sought":    clean(row.get("office_sought", "")),
                "district":         clean(row.get("office_sought_district", "")),
                "county":           clean(row.get("county", "")),
                "date_formed":      clean(row.get("date_formed", "")),
            }
    return reg


# ================================ run =================================

def run():
    log = get_logger("michigan", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
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

        # ── Entity registry ────────────────────────────────────────────
        ft  = time.perf_counter()
        reg = load_entity_registry()
        log.registry_loaded(
            "entities.csv", len(reg), relation="committees",
            bytes=(RAW_DIR / "entities.csv").stat().st_size
            if (RAW_DIR / "entities.csv").exists() else 0,
        )

        # ── Candidates ─────────────────────────────────────────────────
        # Write one candidate row per candidate committee in the entity registry
        for cid, e in reg.items():
            cand_name = e["candidate_name"]
            if not cand_name:
                continue   # non-candidate committee
            cand_w.writerow({
                "state":           STATE,
                "state_filer_id":  cid,
                "candidate_name":  utils.clean_name(cand_name),
                "candidate_first": e["candidate_first"],
                "candidate_last":  e["candidate_last"],
                "office":          e["office_sought"],
                "district":        e["district"],
                "party":           e["party"],
                "jurisdiction":    e["county"],
                "raw_file":        "entities.csv",
                "row_num":         "",
            })
            candidates_written += 1

        log.info(f"  candidates: {candidates_written:,}")

        # ── Committees ─────────────────────────────────────────────────
        for cid, e in reg.items():
            cand_name = e["candidate_name"]
            cmte_w.writerow({
                "state":          STATE,
                "state_filer_id": cid,
                "committee_name": utils.clean_name(e["committee_name"]),
                "committee_type": e["committee_type"],
                "candidate_name": utils.clean_name(cand_name) if cand_name else "",
                "active":         "1" if "active" in e["committee_status"].lower()
                                   else ("0" if e["committee_status"] else ""),
                "raw_file":       "entities.csv",
                "row_num":        "",
            })
            committees_written += 1

        log.info(f"  committees: {committees_written:,}")

        # ── Contributions ──────────────────────────────────────────────
        for path in raw_files("contribution_*.txt"):
            file_year = year_from_filename(path)
            ft        = time.perf_counter()
            count = skipped = loans = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter="\t", restkey="__extra__")
                for row_num, row in enumerate(reader, start=2):
                    ctype   = clean(row.get("contribtype", ""))
                    amount  = parse_amount(row.get("amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    cfr_id    = clean(row.get("cfr_com_id", ""))
                    e         = reg.get(cfr_id, {})
                    com_name  = clean(row.get("com_legal_name", "")) \
                                or e.get("committee_name", "")
                    # Candidate name: prefer inline row fields, fall back to registry
                    can_first = clean(row.get("can_first_name", ""))
                    can_last  = clean(row.get("can_last_name", ""))
                    cand_name = build_name(can_first, can_last) \
                                or e.get("candidate_name", "")

                    doc_year  = clean(row.get("doc_stmnt_year", "")) or file_year
                    cont_f    = clean(row.get("contributor_f_name", ""))

                    base = {
                        "state":            STATE,
                        "committee_name":   utils.clean_name(com_name),
                        "amount":           amount,
                        "date":             parse_date(row.get("received_date", "")),
                        "transaction_type": ctype,
                        "contributor_name": utils.clean_name(
                            build_name(cont_f,
                                       row.get("contributor_l_name_or_org", ""))),
                        "contributor_type": contributor_type_from_name(cont_f),
                        "contributor_city":  clean(row.get("contributor_city", "")),
                        "contributor_state": clean(row.get("contributor_state", "")),
                        "contributor_zip":   clean_zip(row.get("contributor_zip", "")),
                        "employer":          clean(row.get("contributor_employer", "")),
                        "occupation":        clean(row.get("contributor_occupation", "")),
                        "candidate_name":    utils.clean_name(cand_name),
                        "office":            e.get("office_sought", ""),
                        "election_year":     doc_year,
                        "filing_id":         clean(row.get("contribution_id", "")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    }

                    if ctype in LOAN_CONTRIB_TYPES:
                        loan_w.writerow(base)
                        loans += 1
                    else:
                        cont_w.writerow(base)
                        count += 1

            if loans:
                log.info(f"    {path.name}: {count:,} contributions, "
                         f"{loans:,} loans")
            log.file_parsed(path.name, "contributions", count + loans,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += count
            total_loans         += loans

        # ── Receipts (fold into contributions / expenditures) ──────────
        # Receipts use the same schema as contributions but with payer_* columns.
        # Refund/Rebate receipttype rows are written as negative-amount expenditures.
        for path in raw_files("receipt_*.txt"):
            file_year = year_from_filename(path)
            ft        = time.perf_counter()
            count = skipped = refunds = loans = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter="\t", restkey="__extra__")
                for row_num, row in enumerate(reader, start=2):
                    ctype       = clean(row.get("contribtype", ""))
                    receipttype = clean(row.get("receipttype", ""))
                    amount      = parse_amount(row.get("amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    cfr_id   = clean(row.get("cfr_com_id", ""))
                    e        = reg.get(cfr_id, {})
                    com_name = clean(row.get("com_legal_name", "")) \
                               or e.get("committee_name", "")
                    can_first = clean(row.get("can_first_name", ""))
                    can_last  = clean(row.get("can_last_name", ""))
                    cand_name = build_name(can_first, can_last) \
                                or e.get("candidate_name", "")
                    doc_year  = clean(row.get("doc_stmnt_year", "")) or file_year
                    payer_f   = clean(row.get("payer_f_name", ""))

                    base = {
                        "state":          STATE,
                        "committee_name": utils.clean_name(com_name),
                        "amount":         amount,
                        "date":           parse_date(row.get("received_date", "")),
                        "transaction_type": receipttype or ctype,
                        "candidate_name": utils.clean_name(cand_name),
                        "office":         e.get("office_sought", ""),
                        "election_year":  doc_year,
                        "filing_id":      clean(row.get("receipt_id", "")),
                        "raw_file":       path.name,
                        "row_num":        row_num,
                    }

                    if ctype in LOAN_CONTRIB_TYPES:
                        loan_w.writerow({
                            **base,
                            "contributor_name": utils.clean_name(
                                build_name(payer_f,
                                           row.get("payer_l_name_or_org", ""))),
                        })
                        loans += 1
                    elif receipttype in REFUND_RECEIPT_TYPES:
                        # Refund/rebate: money leaving the committee — write as
                        # expenditure with negative amount so it shows as a debit
                        expn_w.writerow({
                            **base,
                            "payee_name":  utils.clean_name(
                                build_name(payer_f,
                                           row.get("payer_l_name_or_org", ""))),
                            "payee_city":  clean(row.get("payer_city", "")),
                            "payee_state": clean(row.get("payer_state", "")),
                            "payee_zip":   clean_zip(row.get("payer_zip", "")),
                            "purpose":     "Refund/Rebate",
                            "amount":      f"-{amount}",
                        })
                        refunds += 1
                    else:
                        cont_w.writerow({
                            **base,
                            "contributor_name": utils.clean_name(
                                build_name(payer_f,
                                           row.get("payer_l_name_or_org", ""))),
                            "contributor_type": contributor_type_from_name(payer_f),
                            "contributor_city":  clean(row.get("payer_city", "")),
                            "contributor_state": clean(row.get("payer_state", "")),
                            "contributor_zip":   clean_zip(row.get("payer_zip", "")),
                            "employer":          clean(row.get("payer_employer", "")),
                            "occupation":        clean(row.get("payer_occupation", "")),
                        })
                        count += 1

            log.file_parsed(path.name, "contributions(receipts)", count + loans + refunds,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += count
            total_loans         += loans
            total_expenditures  += refunds  # refunds written as negative expenditures

        # ── Expenditures ───────────────────────────────────────────────
        for path in raw_files("expenditure_*.txt"):
            file_year = year_from_filename(path)
            ft        = time.perf_counter()
            count = skipped = loans = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f, delimiter="\t", restkey="__extra__")
                for row_num, row in enumerate(reader, start=2):
                    etype  = clean(row.get("expenditure_type", ""))
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    cfr_id   = clean(row.get("cfr_com_id", ""))
                    e        = reg.get(cfr_id, {})
                    com_name = clean(row.get("com_legal_name", "")) \
                               or e.get("committee_name", "")

                    # candidate: inline "candidate" field for IE rows;
                    # otherwise fall back to registry
                    ie_cand   = clean(row.get("candidate", ""))
                    cand_name = ie_cand or e.get("candidate_name", "")

                    # office_dist is a combined "office  district" string for IE rows
                    office_dist = clean(row.get("office_dist", ""))
                    # Registry provides cleaner split values
                    office  = e.get("office_sought", "")
                    district = e.get("district", "")

                    doc_year  = clean(row.get("doc_stmnt_year", "")) or file_year
                    payee_f   = clean(row.get("payee_f_name", ""))

                    base = {
                        "state":            STATE,
                        "committee_name":   utils.clean_name(com_name),
                        "amount":           amount,
                        "date":             parse_date(row.get("exp_date", "")),
                        "transaction_type": etype,
                        "payee_name":       utils.clean_name(
                            build_name(payee_f,
                                       row.get("payee_l_name_or_org", ""))),
                        "payee_city":       clean(row.get("payee_city", "")),
                        "payee_state":      clean(row.get("payee_state", "")),
                        "payee_zip":        clean_zip(row.get("payee_zip", "")),
                        "purpose":          clean(row.get("purpose", "")),
                        "candidate_name":   utils.clean_name(cand_name),
                        "office":           office,
                        "district":         district,
                        "election_year":    doc_year,
                        "filing_id":        clean(row.get("expense_id", "")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    }

                    if etype in LOAN_EXP_TYPES:
                        loan_w.writerow(base)
                        loans += 1
                    else:
                        expn_w.writerow(base)
                        count += 1

            if loans:
                log.info(f"    {path.name}: {count:,} expenditures, "
                         f"{loans:,} loans")
            log.file_parsed(path.name, "expenditures", count + loans,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count
            total_loans        += loans

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # MI committee IDs increment per registration cycle — same candidate
        # gets a new cfr_com_id each cycle; group by (name, office, district)
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz",
                                id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions",
                        total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz", "expenditures",
                        total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("committees.csv.gz", "committees",
                        committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz", "candidates",
                        candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("loans_debts.csv.gz", "loans_debts",
                        total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_contributions:,} contributions, "
                 f"{total_expenditures:,} expenditures, {total_loans:,} loans, "
                 f"{candidates_written:,} candidates, {committees_written:,} committees")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)

    except KeyboardInterrupt:
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
