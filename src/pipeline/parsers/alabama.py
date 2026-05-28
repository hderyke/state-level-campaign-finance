"""
parsers/alabama.py — Transform Alabama raw CSVs into the 5 normalized relations.

Alabama is a flat-file state: committee and candidate info appears on every
transaction row and is synthesized here. OtherReceipts splits into loans_debts
(ReceiptType == 'Loan') and contributions (everything else).
"""

import csv
import gzip
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)  # Alabama CSVs can have very long fields

# paths and file prep

RAW_DIR   = PROJECT_ROOT / "data" / "alabama" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "alabama" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)


# ============================== helpers ===============================
def parse_date(val) -> str:
    """MM/DD/YYYY → YYYY-MM-DD. Returns '' on failure."""
    val = (val or "").strip()
    if not val:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return val


def build_name(last, first, mi, suffix) -> str:
    """
    Assemble a full name from parts.
    If FirstName is blank the record is an organization — return LastName as-is.
    """
    last   = (last   or "").strip()
    first  = (first  or "").strip()
    mi     = (mi     or "").strip().rstrip(".")
    suffix = (suffix or "").strip()

    if not first:
        return last  # organization name stored in LastName

    parts = [first]
    if mi:
        parts.append(mi + ".")
    parts.append(last)
    if suffix:
        parts.append(suffix)
    return " ".join(parts)


def yn_to_int(val) -> int:
    """Convert Y/N string to 1/0."""
    return 1 if (val or "").strip().upper() == "Y" else 0


def clean(val) -> str:
    """Strips whitespace and converts None to an empty string"""
    return (val or "").strip()


def is_numeric(val) -> bool:
    """ Return True if val can be cast to float (allows negatives)."""
    try:
        float((val or "").strip())
        return True
    except ValueError:
        return False


def year_from_filename(path: Path) -> int:
    """Extract the 4-digit year prefix from the filename."""
    m = re.match(r"(\d{4})_", path.name)
    return int(m.group(1)) if m else 0


def raw_files(pattern: str) -> list[Path]:
    """Return all raw files matching a glob pattern, sorted by year."""
    return sorted(RAW_DIR.glob(pattern), key=lambda p: p.name)


# ============================== writers ===============================
def open_writer(filename: str, fieldnames: list[str]):
    """Open a gzipped CSV writer in CLEAN_DIR. Extra fields are silently dropped,
        missing fields default to empty string."""
    path = CLEAN_DIR / filename
    fh   = gzip.open(path, "wt", encoding="utf-8", newline="")
    writer = csv.DictWriter(fh, fieldnames=fieldnames,
                            extrasaction="ignore", restval="")
    writer.writeheader()
    return fh, writer


# =============================== parse ================================

def run():
    log = get_logger("alabama", "parse")
    t0  = time.perf_counter()
    log.info("Starting Alabama parser")
    log._emit("parse_started")

    # Accumulate unique committees and candidates across all files
    committees: dict[str, dict] = {}   # state_filer_id → row
    candidates: set[str] = set()
    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    file_handles: list = []

    try:
        # Open all output writers upfront
        cmte_fh,   cmte_w   = open_writer("committees.csv.gz",    C.COMMITTEES)
        cand_fh,   cand_w   = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cont_fh,   cont_w   = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh,   expn_w   = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh,   loan_w   = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cmte_fh, cand_fh, cont_fh, expn_fh, loan_fh]

        def register_committee(filer_id: str, name: str, ctype: str, cand_name: str = ""):
            if filer_id and filer_id not in committees:
                committees[filer_id] = {
                    "state":          "AL",
                    "state_filer_id": filer_id,
                    "committee_name": name,
                    "committee_type": ctype,
                    "candidate_name": cand_name,
                }

        def register_candidate(name: str):
            name = name.strip()
            if name and name not in candidates:
                candidates.add(name)

        # cash + inkind contributions
        for pattern in ("*_CashContributionsExtract1.csv",
                        "*_InKindContributionsExtract1.csv"):
            for path in raw_files(pattern):
                is_cash_file = "CashContributions" in path.name
                year = year_from_filename(path)
                ft = time.perf_counter()
                count = skipped = 0
                try:
                    with open(path, newline="", encoding="utf-8", errors="replace") as f:
                        for row_num, row in enumerate(csv.DictReader(f), start=2):
                            filer_id   = clean(row.get("CommitteeId", ""))
                            amount_raw = clean(row.get("ContributionAmount", ""))

                            if is_cash_file and "in-kind" in clean(
                                    row.get("ContributionType", "")).lower():
                                skipped += 1
                                continue

                            if not filer_id or not filer_id.isdigit() or not is_numeric(amount_raw):
                                skipped += 1
                                continue

                            cmte_name  = utils.clean_name(row.get("CommitteeName", ""))
                            cmte_type  = clean(row.get("CommitteeType", ""))
                            cand_name  = utils.clean_name(row.get("CandidateName", ""))

                            register_committee(filer_id, cmte_name, cmte_type, cand_name)
                            register_candidate(cand_name)

                            resolved_cmte = (cmte_name
                                             or committees.get(filer_id, {}).get("committee_name", "")
                                             or cand_name)

                            cont_w.writerow({
                                "state":             "AL",
                                "committee_name":    resolved_cmte,
                                "contributor_name":  utils.clean_name(build_name(
                                                         row.get("LastName", ""),
                                                         row.get("FirstName", ""),
                                                         row.get("MI", ""),
                                                         row.get("Suffix", ""),
                                                     )),
                                "amount":            amount_raw,
                                "date":              parse_date(row.get("ContributionDate", "")),
                                "transaction_type":  clean(row.get("ContributionType", "")),
                                "contributor_type":  clean(row.get("ContributorType", "")),
                                "contributor_city":  utils.clean_name(row.get("City", "")),
                                "contributor_state": clean(row.get("State", "")),
                                "contributor_zip":   utils.clean_zip(row.get("Zip", "")),
                                "candidate_name":    cand_name,
                                "election_year":     year,
                                "filing_id":         clean(row.get("ContributionID",
                                                           row.get("InKindContributionID", ""))),
                                "amended":           yn_to_int(row.get("Amended", "N")),
                                "raw_file":          path.name,
                                "row_num":           row_num,
                            })
                            count += 1
                    log.file_parsed(path.name, "contributions", count, skipped,
                                    duration_s=time.perf_counter() - ft,
                                    bytes=path.stat().st_size)
                    total_contributions += count
                except Exception as e:
                    log.file_parse_error(path.name, str(e))

        # other receipts (loans + misc contributions)
        for path in raw_files("*_OtherReceiptsExtract1.csv"):
            year = year_from_filename(path)
            log.info(f"  Parsing {path.name}...")
            ft = time.perf_counter()
            loans = contribs = skipped = 0
            try:
                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    for row_num, row in enumerate(csv.DictReader(f), start=2):
                        filer_id    = clean(row.get("CommitteeId", ""))
                        amount_raw  = clean(row.get("ReceiptAmount", ""))

                        if not filer_id or not filer_id.isdigit() or not is_numeric(amount_raw):
                            skipped += 1
                            continue

                        cmte_name    = utils.clean_name(row.get("CommitteeName", ""))
                        cmte_type    = clean(row.get("CommitteeType", ""))
                        cand_name    = utils.clean_name(row.get("CandidateName", ""))
                        receipt_type = clean(row.get("ReceiptType", ""))

                        register_committee(filer_id, cmte_name, cmte_type, cand_name)
                        register_candidate(cand_name)

                        name = utils.clean_name(build_name(
                            row.get("LastName", ""), row.get("FirstName", ""),
                            row.get("MI", ""),       row.get("Suffix", ""),
                        ))
                        resolved_cmte = (cmte_name
                                         or committees.get(filer_id, {}).get("committee_name", "")
                                         or cand_name)

                        if receipt_type == "Loan":
                            loan_w.writerow({
                                "state":              "AL",
                                "committee_name":     resolved_cmte,
                                "record_type":        "loan",
                                "counterparty_name":  name,
                                "counterparty_city":  utils.clean_name(row.get("City", "")),
                                "counterparty_state": clean(row.get("State", "")),
                                "counterparty_zip":   utils.clean_zip(row.get("Zip", "")),
                                "original_amount":    clean(row.get("ReceiptAmount", "")),
                                "date":               parse_date(row.get("ReceiptDate", "")),
                                "candidate_name":     cand_name,
                                "election_year":      year,
                                "filing_id":          clean(row.get("ReceiptID", "")),
                                "amended":            yn_to_int(row.get("Amended", "N")),
                                "raw_file":           path.name,
                                "row_num":            row_num,
                            })
                            loans += 1
                        else:
                            cont_w.writerow({
                                "state":             "AL",
                                "committee_name":    resolved_cmte,
                                "contributor_name":  name,
                                "amount":            clean(row.get("ReceiptAmount", "")),
                                "date":              parse_date(row.get("ReceiptDate", "")),
                                "transaction_type":  receipt_type,
                                "contributor_type":  clean(row.get("ReceiptSourceType", "")),
                                "contributor_city":  utils.clean_name(row.get("City", "")),
                                "contributor_state": clean(row.get("State", "")),
                                "contributor_zip":   utils.clean_zip(row.get("Zip", "")),
                                "candidate_name":    cand_name,
                                "election_year":     year,
                                "filing_id":         clean(row.get("ReceiptID", "")),
                                "amended":           yn_to_int(row.get("Amended", "N")),
                                "raw_file":          path.name,
                                "row_num":           row_num,
                            })
                            contribs += 1
                duration = round(time.perf_counter() - ft, 2)
                log.info(f"  ✓ {path.name} → loans_debts+contributions "
                         f"({loans:,} loans, {contribs:,} contributions"
                         + (f", {skipped:,} skipped" if skipped else "") + ")")
                log._emit("file_parsed", status="ok", filename=path.name,
                          relation="loans_debts", role="source", rows=loans, skipped=0,
                          duration_s=duration)
                log._emit("file_parsed", status="ok", filename=path.name,
                          relation="contributions", role="source", rows=contribs, skipped=skipped,
                          duration_s=duration)
                total_contributions += contribs
                total_loans         += loans
            except Exception as e:
                log.file_parse_error(path.name, str(e))

        # expenditures
        for path in raw_files("*_ExpendituresExtract1.csv"):
            year = year_from_filename(path)
            log.info(f"  Parsing {path.name}...")
            ft = time.perf_counter()
            count = skipped = 0
            try:
                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    for row_num, row in enumerate(csv.DictReader(f), start=2):
                        filer_id   = clean(row.get("CommitteeId", ""))
                        amount_raw = clean(row.get("ExpenditureAmount", ""))

                        # CommitteeId must be a plain integer — rows where it
                        # contains free text (e.g. "Campaign Signs", "Check #110")
                        # are column-shifted malformed rows from Alabama's export
                        # and should be skipped entirely.
                        if not filer_id or not filer_id.isdigit() or not is_numeric(amount_raw):
                            skipped += 1
                            continue

                        cmte_name = utils.clean_name(row.get("CommitteeName", ""))
                        cmte_type = clean(row.get("CommitteeType", ""))
                        cand_name = utils.clean_name(row.get("CandidateName", ""))

                        register_committee(filer_id, cmte_name, cmte_type, cand_name)
                        register_candidate(cand_name)

                        resolved_cmte = (cmte_name
                                         or committees.get(filer_id, {}).get("committee_name", "")
                                         or cand_name)

                        expn_w.writerow({
                            "state":            "AL",
                            "committee_name":   resolved_cmte,
                            "payee_name":       utils.clean_name(build_name(
                                                    row.get("LastName", ""),
                                                    row.get("FirstName", ""),
                                                    row.get("MI", ""),
                                                    row.get("Suffix", ""),
                                                )),
                            "amount":           amount_raw,
                            "date":             parse_date(row.get("ExpenditureDate", "")),
                            "transaction_type": clean(row.get("ExpenditureType", "")),
                            "purpose":          clean(row.get("Purpose", "")),
                            "category":         clean(row.get("Explanation", "")),
                            "payee_city":       utils.clean_name(row.get("City", "")),
                            "payee_state":      clean(row.get("State", "")),
                            "payee_zip":        utils.clean_zip(row.get("Zip", "")),
                            "candidate_name":   cand_name,
                            "election_year":    year,
                            "filing_id":        clean(row.get("ExpenditureID", "")),
                            "amended":          yn_to_int(row.get("Amended", "N")),
                            "raw_file":         path.name,
                            "row_num":          row_num,
                        })
                        count += 1
                log.file_parsed(path.name, "expenditures", count, skipped,
                                duration_s=time.perf_counter() - ft,
                                bytes=path.stat().st_size)
                total_expenditures += count
            except Exception as e:
                log.file_parse_error(path.name, str(e))

        # load pac + pcc registries and flush committees
        log.info("  Loading registries...")
        registry: dict[str, dict] = {}
        for reg_filename in ("pac_committees.csv", "pcc_committees.csv"):
            reg_path = RAW_DIR / reg_filename
            if reg_path.exists():
                before = len(registry)
                with open(reg_path, newline="", encoding="utf-8") as f:
                    for reg_row_num, reg in enumerate(csv.DictReader(f), start=2):
                        cid = reg.get("committee_id", "").strip()
                        if cid:
                            reg["_raw_file"] = reg_filename
                            reg["_row_num"]  = reg_row_num
                            registry[cid] = reg
                log.registry_loaded(reg_filename, len(registry) - before,
                                    relation="committees",
                                    bytes=reg_path.stat().st_size)
            else:
                log.warning(f"  {reg_filename} not found — skipping enrichment")
        log.info(f"  Registry total: {len(registry):,} committees")

        # Flush committees (enriched with registry)
        candidate_info: dict[str, dict] = {}
        enriched = 0
        for filer_id, row in committees.items():
            reg = registry.get(filer_id, {})
            if reg:
                t_first = reg.get("treasurer_first", "").strip()
                t_last  = reg.get("treasurer_last",  "").strip()
                row["treasurer_name"] = utils.clean_name(f"{t_first} {t_last}")
                row["city"]           = utils.clean_name(reg.get("city", ""))
                row["zip"]            = utils.clean_zip(reg.get("zip_code", ""))
                status                = reg.get("committee_status", "").strip()
                row["active"]         = 1 if status == "Active" else (0 if status == "Dissolved" else "")
                row["raw_file"]       = reg.get("_raw_file", "")
                row["row_num"]        = reg.get("_row_num",  "")
                enriched += 1

                cand_name = row.get("candidate_name", "").strip()
                if cand_name and reg.get("committee_type", "") == "Principal Campaign Committee":
                    candidate_info[cand_name] = {
                        "candidate_first": utils.clean_name(reg.get("candidate_first", "")),
                        "candidate_last":  utils.clean_name(reg.get("candidate_last",  "")),
                        "office":          utils.clean_name(reg.get("office",          "")),
                        "district":        utils.clean_name(reg.get("district",        "")),
                        "jurisdiction":    utils.clean_name(reg.get("jurisdiction",    "")),
                        "party":           utils.clean_name(reg.get("party",           "")),
                        "raw_file":        reg.get("_raw_file",       ""),
                        "row_num":         reg.get("_row_num",        ""),
                    }

            # PCCs have no separate committee name in the source — fill from candidate
            if not row.get("committee_name") and row.get("candidate_name"):
                row["committee_name"] = row["candidate_name"]

            cmte_w.writerow(row)

        log.enrichment_summary(
            committees_total=len(committees),
            committees_enriched=enriched,
            candidates_total=len(candidates),
            candidates_enriched=len(candidate_info),
        )

        pcc_path = RAW_DIR / "pcc_committees.csv"
        log._emit("file_parsed", status="ok", filename="pcc_committees.csv",
                  relation="candidates", role="registry", rows=len(candidate_info),
                  skipped=0, duration_s=0.0,
                  bytes=pcc_path.stat().st_size if pcc_path.exists() else 0)

        # flush candidates
        cand_to_filer: dict[str, str] = {}
        for fid, cmte in committees.items():
            cname = cmte.get("candidate_name", "").strip()
            if cname and (cname not in cand_to_filer or fid < cand_to_filer[cname]):
                cand_to_filer[cname] = fid

        for name in sorted(candidates):
            info = candidate_info.get(name, {})
            cand_w.writerow({
                "state":           "AL",
                "state_filer_id":  cand_to_filer.get(name, ""),
                "candidate_name":  name,
                "candidate_first": info.get("candidate_first", ""),
                "candidate_last":  info.get("candidate_last",  ""),
                "office":          info.get("office",          ""),
                "district":        info.get("district",        ""),
                "jurisdiction":    info.get("jurisdiction",    ""),
                "party":           info.get("party",           ""),
                "raw_file":        info.get("raw_file",        ""),
                "row_num":         info.get("row_num",         ""),
            })

        for fh in file_handles:
            fh.close()
        file_handles = []  # prevent double-close in finally

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
        log.file_parsed("committees.csv.gz",    "committees",    len(committees),      role="output",
                        bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    len(candidates),      role="output",
                        bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates))

    except KeyboardInterrupt: # cc user interrupt
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates))
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates),
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
    import argparse
    argparse.ArgumentParser(
        description="Parse Alabama raw data files into 5 normalized relations."
    ).parse_args()
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)