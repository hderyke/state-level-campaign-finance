"""
parsers/arizona.py — Parse Arizona SeeTheMoney campaign finance data.

Reads bulk CSV exports from data/Arizona/raw/ and writes normalized output
to data/Arizona/cleaned/.

Input files:
  Income_{cycle}_{type}.csv        — contributions received (1998–2026 + Recall_Fann)
  Expenditures_{cycle}_{type}.csv  — expenditures made
  az_committees_all.csv            — full committee registry (all filer types, 43K+ entities)

File types per cycle: Candidate, PAC, Party, Officeholder

Schema notes:
  Files downloaded by the current scraper include CommitteeName and CommitteeID on
  every row (from the AdvancedSearch JSON API). The parser reads CommitteeName
  directly — no registry join needed for committee_name on contributions/expenditures.

  Legacy files (downloaded before the JSON API scraper) lack CommitteeName. For
  those, the parser falls back to a registry lookup via FilerName.

  FilerName    = the committee/filer that RECEIVED the income or MADE the expenditure.
  TransactionName = the counterparty: contributor (Income) or payee (Expenditures).

  Candidate files:    FilerName is "Last, First" — used to populate candidate_name.
  PAC/Party files:    FilerName is mostly empty in the old CSV export; CommitteeName
                      from the JSON API fills this gap.

  Registry join (fallback only): entity_last_name (Candidate) + committee_name
                 (Officeholder/PAC/Party) → committee_name used when CommitteeName
                 column is absent from the source file.

  Amounts arrive as "5000.0000" — kept as plain decimal string.
  Dates arrive as "12/31/2024 12:00:00 AM" or "YYYY-MM-DD" — normalized to YYYY-MM-DD.

  No loan/debt data — loans_debts.csv.gz written empty (header only).
  person_id model: "committee" — Arizona assigns IDs per committee registration.
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
from src.reporting.logger import get_logger

sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# ================================ paths ===============================
RAW_DIR   = PROJECT_ROOT / "data" / "Arizona" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Arizona" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "AZ"

MAX_VALID_YEAR = date.today().year + 2


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'5000.0000' → '5000.0'. Returns '' on failure."""
    val = (val or "").strip().replace("$", "").replace(",", "")
    if not val:
        return ""
    if val.startswith("(") and val.endswith(")"):
        val = "-" + val[1:-1]
    try:
        float(val)
        return val
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """
    '12/31/2024 12:00:00 AM' → '2024-12-31'.
    Also handles plain YYYY-MM-DD. Returns '' on failure or implausible year.
    """
    val = (val or "").strip()
    if not val:
        return ""
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(val, fmt)
            if d.year < 1970 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def year_from_filename(path: Path) -> str:
    """Extract 4-digit year from filename.
    Falls back to a known mapping for named cycles (e.g. Recall_Fann → 2021).
    """
    m = re.search(r"(\d{4})", path.name)
    if m:
        return m.group(1)
    _NAMED_CYCLE_YEARS = {
        "Recall_Fann": "2021",
    }
    for key, year in _NAMED_CYCLE_YEARS.items():
        if key in path.name:
            return year
    return ""


def filer_type_from_filename(path: Path) -> str:
    """Extract Candidate/PAC/Party/Officeholder from filename stem."""
    parts = path.stem.split("_")   # e.g. 'Income_2024_Candidate'
    return parts[-1] if parts else ""


def parse_office_district(office_str: str) -> tuple[str, str]:
    """Split 'State Representative - District 20' into ('State Representative', '20').
    Handles both '- District 20' and '- District No. 26' variants.
    Returns (office, district) — district is '' if no district found.
    """
    m = re.search(r"\s*-\s*District\s+(?:No\.\s*)?(\d+)", office_str, re.IGNORECASE)
    if m:
        return office_str[:m.start()].strip(), m.group(1)
    return office_str, ""


def raw_files(prefix: str) -> list[Path]:
    """Return sorted Income_* or Expenditures_* files with non-zero size."""
    return sorted(
        (f for f in RAW_DIR.glob(f"{prefix}_*.csv") if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


# ============================ name helpers ============================

def _format_name(last_first: str, first: str) -> str:
    """
    Build a canonical 'First Last' name from registry fields.
      'Gallego, Ruben', ''       → 'Ruben Gallego'  (FilerName in transaction files)
      'Gallego',        'Ruben'  → 'Ruben Gallego'  (entity_last/first from registry)
    """
    last_first = (last_first or "").strip()
    first      = (first or "").strip()
    if not last_first:
        return first
    if "," in last_first:
        parts = [p.strip() for p in last_first.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return f"{parts[1]} {parts[0]}"
        return parts[0]
    if first:
        return f"{first} {last_first}"
    return last_first


def _split_last_first(last_first: str) -> tuple[str, str]:
    """'Bagley, Nikki' → ('Bagley', 'Nikki'). Returns (last_first, '') if no comma."""
    if "," in last_first:
        parts = [p.strip() for p in last_first.split(",", 1)]
        return parts[0], parts[1] if len(parts) > 1 else ""
    return last_first, ""


# ========================== registry loader ===========================

def load_registry() -> tuple[dict, dict]:
    """
    Build lookup dicts from az_committees_all.csv.
    Returns (by_lastname, by_cmte_name):
      by_lastname  : entity_last_name → registry row  (Candidate files)
      by_cmte_name : committee_name   → registry row  (Officeholder / PAC / Party)
    """
    by_lastname  = {}
    by_cmte_name = {}
    path = RAW_DIR / "az_committees_all.csv"
    if not path.exists():
        path = RAW_DIR / "az_committees.csv"   # fall back to old candidates-only file
    if not path.exists():
        return by_lastname, by_cmte_name

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            ln = row.get("entity_last_name", "").strip()
            cn = row.get("committee_name", "").strip()
            if ln and ln not in by_lastname:
                by_lastname[ln] = row
            if cn and cn not in by_cmte_name:
                by_cmte_name[cn] = row

    return by_lastname, by_cmte_name


def lookup_filer(filer_name: str, filer_type: str,
                 by_lastname: dict, by_cmte_name: dict) -> dict | None:
    """
    Try registry lookup.
    Candidate:    'Last, First' → entity_last_name index
    Others:       full committee name → committee_name index, with last-name fallback
    Returns registry row or None.
    """
    if not filer_name:
        return None
    if filer_type == "Candidate":
        last = filer_name.split(",")[0].strip() if "," in filer_name else filer_name
        return by_lastname.get(last)
    reg = by_cmte_name.get(filer_name)
    if reg:
        return reg
    # Officeholder FilerNames can also arrive as 'Last, First'
    if "," in filer_name:
        last = filer_name.split(",")[0].strip()
        return by_lastname.get(last)
    return None


# ============================== writers ===============================

def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ================================ run =================================

def run():
    log = get_logger("arizona", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        # Load registry for enrichment
        reg_path = RAW_DIR / "az_committees_all.csv"
        if not reg_path.exists():
            reg_path = RAW_DIR / "az_committees.csv"

        log.info(f"  loading registry from {reg_path.name if reg_path.exists() else '(not found)'}...")
        ft = time.perf_counter()
        by_lastname, by_cmte_name = load_registry()
        log.registry_loaded(
            reg_path.name if reg_path.exists() else "az_committees_all.csv",
            len(by_lastname),
            relation="committees",
            bytes=reg_path.stat().st_size if reg_path.exists() else 0,
        )
        log.info(f"    {len(by_lastname):,} last-name entries, "
                 f"{len(by_cmte_name):,} committee-name entries")

        # Open output writers
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)  # always empty
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        # ── Committees: write from registry (all filer types) ─────────────────
        log.info(f"  committees     {reg_path.name if reg_path.exists() else '(not found)'}...")
        ft = time.perf_counter()
        if reg_path.exists():
            with open(reg_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    etype = clean(row.get("entity_type_name") or row.get("entity_type", ""))
                    cmte_w.writerow({
                        "state":          STATE,
                        "state_filer_id": clean(row.get("entity_id", "")),
                        "committee_name": clean(row.get("committee_name", "")),
                        "committee_type": etype,
                        "candidate_name": _format_name(
                            clean(row.get("entity_last_name", "")),
                            clean(row.get("entity_first_name", "")),
                        ),
                        "treasurer_name": "",
                        "city":           clean(row.get("city", "")),
                        "zip":            clean(row.get("zip", "")),
                        "active":         "",
                        "raw_file":       reg_path.name,
                        "row_num":        row_num,
                    })
                    committees_written += 1
        log.file_parsed(reg_path.name if reg_path.exists() else "az_committees_all.csv",
                        "committees", committees_written,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=reg_path.stat().st_size if reg_path.exists() else 0)

        # ── Candidates: Candidate entity types from registry ──────────────────
        log.info(f"  candidates     {reg_path.name if reg_path.exists() else '(not found)'}...")
        ft = time.perf_counter()
        if reg_path.exists():
            with open(reg_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    etype = clean(row.get("entity_type_name") or row.get("entity_type", ""))
                    if "Candidate" not in etype and "$500 Threshold" not in etype:
                        continue
                    last_first = clean(row.get("entity_last_name", ""))
                    first      = clean(row.get("entity_first_name", ""))
                    cand_name  = _format_name(last_first, first)
                    if not cand_name:
                        continue

                    cand_last, cand_first = _split_last_first(last_first)
                    office_raw = clean(row.get("office_name") or row.get("office", ""))
                    office, district = parse_office_district(office_raw)
                    party  = clean(row.get("party_name")  or row.get("party",  ""))

                    cand_w.writerow({
                        "state":           STATE,
                        "state_filer_id":  clean(row.get("entity_id", "")),
                        "candidate_name":  cand_name,
                        "candidate_first": cand_first or first,
                        "candidate_last":  cand_last,
                        "office":          office,
                        "district":        district,
                        "jurisdiction":    "",
                        "party":           party,
                        "election_year":   "",
                        "status":          etype,
                        "incumbent":       "",
                        "raw_file":        reg_path.name,
                        "row_num":         row_num,
                    })
                    candidates_written += 1
        log.file_parsed(reg_path.name if reg_path.exists() else "az_committees_all.csv",
                        "candidates", candidates_written,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=reg_path.stat().st_size if reg_path.exists() else 0)

        # ── Income → contributions ─────────────────────────────────────────────
        for path in raw_files("Income"):
            filer_type = filer_type_from_filename(path)
            year       = year_from_filename(path)
            log.info(f"  contributions  {path.name}...")
            ft = time.perf_counter()
            count = skipped = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader     = csv.DictReader(f)
                has_api_cols = reader.fieldnames and "CommitteeName" in reader.fieldnames
                for row_num, row in enumerate(reader, start=2):
                    filer_name = clean(row.get("FilerName", ""))
                    amount     = parse_amount(clean(row.get("Amount", "")))

                    # Skip rows with no usable filer AND no amount
                    if not filer_name and not amount and not row.get("CommitteeName", ""):
                        skipped += 1
                        continue

                    # New-format files (JSON API scraper): CommitteeName is on every row.
                    # Legacy files: fall back to registry lookup via FilerName.
                    if has_api_cols:
                        committee_name = clean(row.get("CommitteeName", ""))
                        reg = None
                    else:
                        reg = lookup_filer(filer_name, filer_type, by_lastname, by_cmte_name)
                        committee_name = clean(reg["committee_name"]) if reg else ""

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    committee_name,
                        "contributor_name":  clean(row.get("TransactionName", "")),
                        "amount":            amount,
                        "date":              parse_date(row.get("TransactionDate", "")),
                        "transaction_type":  clean(row.get("TransactionType", "")),
                        "contributor_type":  clean(row.get("TransactionType", "")),
                        "contributor_city":  clean(row.get("City", "")),
                        "contributor_state": clean(row.get("State", "")),
                        "contributor_zip":   clean(row.get("ZipCode", "")),
                        "employer":          clean(row.get("Employer", "")),
                        "occupation":        clean(row.get("Occupation", "")),
                        "candidate_name":    _format_name(filer_name, "") if filer_type == "Candidate" else "",
                        "office":            clean(reg.get("office_name") or reg.get("office", "")) if reg else "",
                        "election_year":     year,
                        "filing_id":         "",
                        "amended":           "",
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "contributions", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += count

        # ── Expenditures ──────────────────────────────────────────────────────
        for path in raw_files("Expenditures"):
            filer_type = filer_type_from_filename(path)
            year       = year_from_filename(path)
            log.info(f"  expenditures   {path.name}...")
            ft = time.perf_counter()
            count = skipped = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader     = csv.DictReader(f)
                has_api_cols = reader.fieldnames and "CommitteeName" in reader.fieldnames
                for row_num, row in enumerate(reader, start=2):
                    filer_name = clean(row.get("FilerName", ""))
                    amount     = parse_amount(clean(row.get("Amount", "")))

                    if not filer_name and not amount and not row.get("CommitteeName", ""):
                        skipped += 1
                        continue

                    if has_api_cols:
                        committee_name = clean(row.get("CommitteeName", ""))
                        reg = None
                    else:
                        reg = lookup_filer(filer_name, filer_type, by_lastname, by_cmte_name)
                        committee_name = clean(reg["committee_name"]) if reg else ""

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   committee_name,
                        "payee_name":       clean(row.get("TransactionName", "")),
                        "amount":           amount,
                        "date":             parse_date(row.get("TransactionDate", "")),
                        "transaction_type": clean(row.get("TransactionType", "")),
                        "purpose":          clean(row.get("TransactionType", "")),
                        "category":         filer_type,
                        "payee_city":       clean(row.get("City", "")),
                        "payee_state":      clean(row.get("State", "")),
                        "payee_zip":        clean(row.get("ZipCode", "")),
                        "candidate_name":   _format_name(filer_name, "") if filer_type == "Candidate" else "",
                        "office":           clean(reg.get("office_name") or reg.get("office", "")) if reg else "",
                        "election_year":    year,
                        "filing_id":        "",
                        "amended":          "",
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count

        # ── Close handles before person-ID assignment ─────────────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

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
