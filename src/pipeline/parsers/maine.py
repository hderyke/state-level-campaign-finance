"""
parsers/maine.py — Parse Maine campaign finance data into the canonical schema.

Input files (data/Maine/raw/):
    me_filer_list.csv         — all filer UUIDs, names, types, and statuses
                                (scraped from /public/filers pages)
    me_filer_profiles.csv     — office, party, treasurer per UUID
                                (scraped from /public/filers/{uuid} pages)
    me_transactions_{year}.csv — all transaction types for each year, 2018–present
                                (scraped from /public/activities pages)

Output (data/Maine/cleaned/):
    candidates.csv.gz, committees.csv.gz,
    contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz

id_model = "committee"
    Maine's MapLight system assigns a UUID per filer registration. The same
    individual running in different election cycles gets a separate UUID each
    time. assign_person_ids() groups by (state, candidate_name, office,
    district) and sets person_id = min(state_filer_id) across registrations.

Transaction type routing (from the display strings in the list view):
    → contributions:  Monetary Contribution, In-Kind Contribution,
                      Returned Expenditure, Returned Independent Expenditure,
                      Transfer from Previous Campaign
    → expenditures:   Expenditure, Independent Expenditure,
                      Returned Contribution, Debt Payment, Loan Payment
    → loans_debts:    Loan, Loan Forgiveness, Debt

Known limitations:
    - No contributor address, city, zip, occupation, or employer — the list
      view does not expose these fields (detail-page enrichment not scraped).
    - election_year on candidates is inferred from transaction dates (earliest
      year the candidate appears in the transaction files), not from the
      Maine system's election registration data.
    - Some filer names in transactions may have minor whitespace variants vs.
      the filer_list; committee_name is taken directly from the transaction row.
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
from src.reporting.logger import get_logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Maine" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Maine" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "ME"

# ============================= constants ==============================

# Transaction type display strings → output table routing.
# These are the human-readable labels from the /public/activities list view.
# Lower-cased for case-insensitive matching.
_CONTRIBUTION_TYPES = {
    "monetary contribution",
    "in-kind contribution",
    "contribution",
    "returned expenditure",
    "returned independent expenditure",
    "transfer from previous campaign",
    "public funding",          # Maine Clean Elections Act disbursements to candidate
}
_EXPENDITURE_TYPES = {
    "expenditure",
    "independent expenditure",
    "returned contribution",
    "debt payment",
    "loan payment",
}
_LOAN_TYPES = {
    "loan",
    "loan forgiveness",
    "debt",
}

# ========================== output writer =============================

def _open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# =========================== helpers ==================================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Strip $ and commas, return plain numeric string. Returns '' on failure."""
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
    """Normalize MM/DD/YYYY or YYYY-MM-DD to YYYY-MM-DD. Returns '' on failure."""
    v = clean(val)
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > date.today().year + 2:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _route_transaction(raw_type: str) -> str:
    """Return 'contribution', 'expenditure', or 'loan' for a raw transaction type."""
    t = raw_type.lower().strip()
    if t in _CONTRIBUTION_TYPES:
        return "contribution"
    if t in _EXPENDITURE_TYPES:
        return "expenditure"
    if t in _LOAN_TYPES:
        return "loan"
    # Fallback heuristic: 'contribution' in the type string → contributions
    if "contribution" in t or "receipt" in t or "income" in t:
        return "contribution"
    if "expenditure" in t or "expense" in t or "payment" in t:
        return "expenditure"
    if "loan" in t or "debt" in t:
        return "loan"
    # Unknown types go to expenditures (safer than inflating contributions)
    return "expenditure"


def _raw_files(pattern: str) -> list[Path]:
    """Return sorted non-empty raw files matching a glob pattern."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def _load_enrichment() -> dict[str, dict]:
    """Load me_enrichment.csv into a dict keyed by transaction_id.

    Returns an empty dict if the file doesn't exist (enrichment not yet run).
    """
    path = RAW_DIR / "me_enrichment.csv"
    if not path.exists():
        return {}
    enrichment: dict[str, dict] = {}
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            tid = row.get("transaction_id", "").strip()
            if tid:
                enrichment[tid] = row
    return enrichment


# ======================= filer registry ===============================

def _build_filer_registry() -> dict[str, dict]:
    """Load me_filer_list.csv and me_filer_profiles.csv into one registry.

    Returns a dict keyed by filer UUID with merged fields:
      uuid, name, filer_type, status, office, party, financing_type,
      treasurer_name, treasurer_email, principal_officer_name,
      principal_officer_email
    """
    registry: dict[str, dict] = {}

    filer_list_path = RAW_DIR / "me_filer_list.csv"
    if not filer_list_path.exists():
        return registry

    with open(filer_list_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            uuid = clean(row.get("uuid", ""))
            if not uuid:
                continue
            registry[uuid] = {
                "uuid":       uuid,
                "name":       clean(row.get("name", "")),
                "filer_type": clean(row.get("filer_type", "")),
                "status":     clean(row.get("status", "")),
                "office":               "",
                "party":                "",
                "financing_type":       "",
                "treasurer_name":       "",
                "treasurer_email":      "",
                "principal_officer_name":  "",
                "principal_officer_email": "",
            }

    profiles_path = RAW_DIR / "me_filer_profiles.csv"
    if profiles_path.exists():
        with open(profiles_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                uuid = clean(row.get("uuid", ""))
                if uuid in registry:
                    registry[uuid].update({
                        k: clean(row.get(k, ""))
                        for k in (
                            "office", "party", "financing_type",
                            "treasurer_name", "treasurer_email",
                            "principal_officer_name", "principal_officer_email",
                        )
                    })

    return registry


# ===================== entities writers ===============================

def _write_committees(registry: dict[str, dict], log, t0: float) -> int:
    """Write committees.csv.gz from the filer registry.

    All filer types become committee rows. Candidates are additionally
    written to candidates.csv.gz by _write_candidates(). Candidates'
    committee rows carry candidate_name so assign_committee_person_ids()
    can link them.
    """
    cmte_fh, cmte_w = _open_writer("committees.csv.gz", C.COMMITTEES)
    count = 0
    try:
        for i, (uuid, r) in enumerate(registry.items(), start=1):
            is_candidate = "candidate" in r["filer_type"].lower()
            cmte_w.writerow({
                "state":          STATE,
                "committee_name": utils.clean_name(r["name"]),
                "committee_type": r["filer_type"],   # normalized by committee_types.csv alias
                "candidate_name": utils.clean_name(r["name"]) if is_candidate else "",
                "treasurer_name": r["treasurer_name"],
                "active":         1 if r["status"].lower() == "active" else 0,
                "state_filer_id": uuid,
                "raw_file":       "me_filer_list.csv",
                "row_num":        i,
            })
            count += 1
    finally:
        cmte_fh.close()

    log.file_parsed("committees.csv.gz", "committees", count,
                    role="output", bytes=(CLEAN_DIR / "committees.csv.gz").stat().st_size)
    return count


def _write_candidates(registry: dict[str, dict], log, t0: float) -> int:
    """Write candidates.csv.gz from filer registry rows where type=Candidate."""
    cand_fh, cand_w = _open_writer("candidates.csv.gz", C.CANDIDATES)
    count = 0
    try:
        for i, (uuid, r) in enumerate(
            ((k, v) for k, v in registry.items()
             if "candidate" in v["filer_type"].lower()),
            start=1,
        ):
            full_name = utils.clean_name(r["name"])
            # Attempt first/last split — Maine stores full name as a single field
            parts = full_name.split()
            first = parts[0] if len(parts) > 1 else ""
            last  = parts[-1] if parts else full_name

            cand_w.writerow({
                "state":          STATE,
                "candidate_name": full_name,
                "candidate_first": first,
                "candidate_last":  last,
                "office":         r["office"],
                "party":          r["party"],
                "state_filer_id": uuid,
                "raw_file":       "me_filer_list.csv",
                "row_num":        i,
            })
            count += 1
    finally:
        cand_fh.close()

    log.file_parsed("candidates.csv.gz", "candidates", count,
                    role="output", bytes=(CLEAN_DIR / "candidates.csv.gz").stat().st_size)
    return count


# ==================== transaction parser ==============================

def _parse_transactions(log) -> tuple[int, int, int]:
    """Parse me_transactions_{year}.csv files and write the three transaction tables.

    Returns (contributions_written, expenditures_written, loans_written).
    """
    enrichment = _load_enrichment()
    if enrichment:
        log.info(f"  Loaded {len(enrichment):,} enrichment rows")

    cont_fh, cont_w = _open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
    expn_fh, expn_w = _open_writer("expenditures.csv.gz",  C.EXPENDITURES)
    loan_fh, loan_w = _open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)

    total_cont = total_expn = total_loan = 0
    file_handles = [cont_fh, expn_fh, loan_fh]

    try:
        for path in _raw_files("me_transactions_*.csv"):
            ft      = time.perf_counter()
            count   = 0
            skipped = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    raw_type     = clean(row.get("transaction_type", ""))
                    filer_name   = clean(row.get("filer_name", ""))
                    source_payee = clean(row.get("source_payee", ""))
                    raw_date     = clean(row.get("date", ""))
                    raw_amount   = clean(row.get("amount", ""))
                    txn_id       = clean(row.get("transaction_id", ""))

                    amount = parse_amount(raw_amount)
                    dt     = parse_date(raw_date)

                    committee_name = utils.clean_name(filer_name)

                    if not committee_name or not amount:
                        skipped += 1
                        continue

                    # "Opening Balance" rows are accounting carryovers, not
                    # real transactions — suppress them to avoid inflating totals.
                    if source_payee.strip().lower() == "opening balance":
                        skipped += 1
                        continue

                    route = _route_transaction(raw_type)
                    enr   = enrichment.get(txn_id, {})

                    # Parse election year from "2026 Primary Election" etc.
                    election_raw = clean(enr.get("election", ""))
                    election_year = ""
                    if election_raw:
                        m = re.match(r"(\d{4})", election_raw)
                        if m:
                            election_year = m.group(1)

                    if route == "contribution":
                        cont_w.writerow({
                            "state":              STATE,
                            "committee_name":     committee_name,
                            "amount":             amount,
                            "date":               dt,
                            "transaction_type":   raw_type,
                            "contributor_name":   utils.clean_name(source_payee),
                            "contributor_type":   clean(enr.get("contact_type", "")),
                            "contributor_city":   clean(enr.get("city", "")),
                            "contributor_state":  clean(enr.get("state_province", "")),
                            "contributor_zip":    clean(enr.get("zip_postal_code", "")),
                            "occupation":         clean(enr.get("occupation_other", ""))
                                                  or clean(enr.get("occupation", "")),
                            "employer":           clean(enr.get("employer", "")),
                            "election_year":      election_year,
                            "filing_id":          txn_id,
                            "raw_file":           path.name,
                            "row_num":            row_num,
                        })
                        total_cont += 1

                    elif route == "expenditure":
                        expn_w.writerow({
                            "state":            STATE,
                            "committee_name":   committee_name,
                            "amount":           amount,
                            "date":             dt,
                            "transaction_type": raw_type,
                            "payee_name":       utils.clean_name(source_payee),
                            "purpose":          clean(enr.get("description", "")),
                            "election_year":    election_year,
                            "filing_id":        txn_id,
                            "raw_file":         path.name,
                            "row_num":          row_num,
                        })
                        total_expn += 1

                    else:  # loan/debt
                        loan_w.writerow({
                            "state":              STATE,
                            "committee_name":     committee_name,
                            "original_amount":    amount,
                            "date":               dt,
                            "record_type":        raw_type,
                            "counterparty_name":  utils.clean_name(source_payee),
                            "filing_id":          txn_id,
                            "raw_file":           path.name,
                            "row_num":            row_num,
                        })
                        total_loan += 1

                    count += 1

            log.file_parsed(path.name, "transactions", count,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass
        file_handles = []

    return total_cont, total_expn, total_loan


# ============================= run ====================================

def run():
    log = get_logger("maine", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    committees_written  = 0
    candidates_written  = 0
    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    file_handles        = []

    try:
        # ── Build filer registry ──────────────────────────────────────
        registry = _build_filer_registry()
        log.registry_loaded("me_filer_list.csv + me_filer_profiles.csv",
                            entries=len(registry), relation="committees/candidates")

        # ── Write entities ────────────────────────────────────────────
        committees_written = _write_committees(registry, log, t0)
        candidates_written = _write_candidates(registry, log, t0)

        # ── Parse transactions ────────────────────────────────────────
        total_contributions, total_expenditures, total_loans = _parse_transactions(log)

        # ── Log output file stats ─────────────────────────────────────
        def _bytes(name: str) -> int:
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        # ── Assign person IDs ─────────────────────────────────────────
        # Closed after _parse_transactions returns, so safe to read/rewrite here.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz",
                                id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

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


# ============================= CLI ====================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
