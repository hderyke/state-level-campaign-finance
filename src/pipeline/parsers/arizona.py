"""
parsers/arizona.py — Transform Arizona SeeTheMoney bulk CSVs into 5 normalized relations.

Input:  data/Arizona/raw/Income_{year}_{type}.csv          (1998–2026 + Recall_Fann)
        data/Arizona/raw/Expenditures_{year}_{type}.csv    (1998–2026 + Recall_Fann)
        data/Arizona/raw/az_committees_all.csv             (full committee registry — all filer types)

Output: data/Arizona/cleaned/{relation}.csv

File types per year: Candidate, PAC, Party, Officeholder

Schema notes
────────────
  FilerName    = the committee/filer that RECEIVED the income or MADE the expenditure.
  TransactionName = the counterparty: contributor (Income) or payee (Expenditures).

  Candidate files  : FilerName is "Last, First" → matches registry entity_last_name.
  Officeholder files: FilerName is full committee name → matches registry committee_name.
  PAC/Party files  : FilerName is mostly empty (aggregate export; no per-row filer ID).
                     When non-empty it appears to be a contributing entity, not the filer.

  Registry join: entity_last_name  (Candidate files)
               + committee_name    (Officeholder + PAC/Party fallback)
               → entity_id used as state_filer_id

  az_committees_all.csv covers all filer types (Candidate, PAC, Party,
  Officeholder, BallotMeasure, Other) — 43K+ entities. Columns differ
  from the old candidates-only az_committees.csv:
    entity_type_name (was entity_type), office_name (was office),
    party_name (was party). city/zip/state now available directly.

  Amounts arrive as "5000.0000" — kept as plain decimal string.
  Dates arrive as "12/31/2024 12:00:00 AM" — normalized to YYYY-MM-DD.
  No separate loans/debts file — loans_debts.csv is written empty (header only).
"""

import csv
import re
import sys
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C

csv.field_size_limit(sys.maxsize)

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Arizona" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Arizona" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "AZ"

# File type → committee_type label
FILER_TYPE_MAP = {
    "Candidate":    "Candidate",
    "PAC":          "PAC",
    "Party":        "Political Party",
    "Officeholder": "Officeholder",
}

MAX_VALID_YEAR = date.today().year + 2


# ── Helpers ────────────────────────────────────────────────────────────────────
def parse_date(val: str) -> str:
    """
    '12/31/2024 12:00:00 AM' → '2024-12-31'
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


def clean(val) -> str:
    return (val or "").strip()


def year_from_filename(path: Path) -> str:
    """Extract 4-digit year from filename, or 'Recall_Fann' prefix."""
    m = re.search(r"(\d{4})", path.name)
    return m.group(1) if m else ""


def filer_type_from_filename(path: Path) -> str:
    """Extract Candidate/PAC/Party/Officeholder from filename stem."""
    stem = path.stem  # e.g. 'Income_2024_Candidate'
    parts = stem.split("_")
    return parts[-1] if parts else ""


def raw_files(prefix: str) -> list[Path]:
    """Return sorted list of Income_* or Expenditures_* files."""
    return sorted(RAW_DIR.glob(f"{prefix}_*.csv"), key=lambda p: p.name)


# ── Registry loader ────────────────────────────────────────────────────────────
def load_registry() -> tuple[dict, dict]:
    """
    Returns (by_lastname, by_cmte_name) dicts built from az_committees_all.csv.
    by_lastname  : entity_last_name  → registry row   (Candidate files)
    by_cmte_name : committee_name    → registry row   (Officeholder / PAC / Party)
    """
    by_lastname  = {}
    by_cmte_name = {}
    path = RAW_DIR / "az_committees_all.csv"
    if not path.exists():
        # Fall back to old candidates-only file if new one not yet downloaded
        path = RAW_DIR / "az_committees.csv"
    if not path.exists():
        print(f"  WARNING: committee registry not found — registry lookups disabled")
        return by_lastname, by_cmte_name

    print(f"  loading registry from {path.name}...", end=" ", flush=True)
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            ln = row.get("entity_last_name", "").strip()
            cn = row.get("committee_name", "").strip()
            if ln and ln not in by_lastname:
                by_lastname[ln] = row
            if cn and cn not in by_cmte_name:
                by_cmte_name[cn] = row
    print(f"{len(by_lastname):,} last-name entries, {len(by_cmte_name):,} committee-name entries")
    return by_lastname, by_cmte_name


def lookup_filer(filer_name: str, filer_type: str,
                 by_lastname: dict, by_cmte_name: dict) -> dict | None:
    """
    Try registry lookup.
    Candidate:    "Last, First" → entity_last_name index
    Others:       full committee name → committee_name index
    Returns registry row or None.
    """
    if not filer_name:
        return None
    if filer_type == "Candidate":
        # FilerName in transaction files is "Last, First" — extract last name for index lookup
        last = filer_name.split(",")[0].strip() if "," in filer_name else filer_name
        return by_lastname.get(last)
    return by_cmte_name.get(filer_name) or by_lastname.get(filer_name)


# ── Writers ────────────────────────────────────────────────────────────────────
def open_writer(filename: str, fieldnames: list):
    path = CLEAN_DIR / filename
    fh   = open(path, "w", newline="", encoding="utf-8")
    w    = csv.DictWriter(fh, fieldnames=fieldnames,
                          extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ── Main ───────────────────────────────────────────────────────────────────────
def run():
    by_lastname, by_cmte_name = load_registry()

    cand_fh, cand_w = open_writer("candidates.csv",    C.CANDIDATES)
    cmte_fh, cmte_w = open_writer("committees.csv",    C.COMMITTEES)
    cont_fh, cont_w = open_writer("contributions.csv", C.CONTRIBUTIONS)
    expn_fh, expn_w = open_writer("expenditures.csv",  C.EXPENDITURES)
    loan_fh, loan_w = open_writer("loans_debts.csv",   C.LOANS_DEBTS)  # empty

    # ── Committees: write from registry (all filer types) ─────────────────────
    reg_path = RAW_DIR / "az_committees_all.csv"
    if not reg_path.exists():
        reg_path = RAW_DIR / "az_committees.csv"   # fallback
    print(f"  committees     {reg_path.name}...", end=" ", flush=True)
    cmte_count = 0
    if reg_path.exists():
        with open(reg_path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                # Support both old (entity_type/office/party) and new
                # (entity_type_name/office_name/party_name) column names
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
                })
                cmte_count += 1
    print(f"{cmte_count:,} rows")

    # ── Candidates: derived from committee registry (Candidate entity types) ───
    print(f"  candidates     {reg_path.name}...", end=" ", flush=True)
    cand_count = 0
    if reg_path.exists():
        with open(reg_path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                # Support both old and new column names
                etype = clean(row.get("entity_type_name") or row.get("entity_type", ""))
                # Only Candidate entries become candidates rows
                if "Candidate" not in etype and "$500 Threshold" not in etype:
                    continue
                last_first = clean(row.get("entity_last_name", ""))
                first      = clean(row.get("entity_first_name", ""))
                cand_name  = _format_name(last_first, first)
                if not cand_name:
                    continue

                cand_last, cand_first = _split_last_first(last_first)
                office = clean(row.get("office_name") or row.get("office", ""))
                party  = clean(row.get("party_name")  or row.get("party",  ""))

                cand_w.writerow({
                    "state":           STATE,
                    "candidate_name":  cand_name,
                    "candidate_first": cand_first or first,
                    "candidate_last":  cand_last,
                    "office":          office,
                    "district":        "",
                    "jurisdiction":    "",
                    "party":           party,
                    "election_year":   "",
                    "status":          etype,
                    "incumbent":       "",
                    "raw_file":        reg_path.name,
                    "row_num":         "",
                })
                cand_count += 1
    print(f"{cand_count:,} rows")

    # ── Income → contributions ─────────────────────────────────────────────────
    for path in raw_files("Income"):
        filer_type = filer_type_from_filename(path)
        year       = year_from_filename(path)
        print(f"  contributions  {path.name}...", end=" ", flush=True)
        count = skipped = 0

        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                filer_name = clean(row.get("FilerName", ""))
                amount_raw = clean(row.get("Amount", ""))
                amount     = parse_amount(amount_raw)

                # Look up filer in registry
                reg = lookup_filer(filer_name, filer_type, by_lastname, by_cmte_name)

                # state_filer_id: prefer entity_id, fallback to filer_name
                if reg:
                    state_filer_id = clean(reg["entity_id"])
                    committee_name = clean(reg["committee_name"])
                else:
                    state_filer_id = filer_name
                    committee_name = ""

                # Skip rows with no usable filer AND no amount
                if not state_filer_id and not amount:
                    skipped += 1
                    continue

                cont_w.writerow({
                    "state":             STATE,
                    "state_filer_id":    state_filer_id,
                    "committee_name":    committee_name,
                    "contributor_name":  clean(row.get("TransactionName", "")),
                    "amount":            amount,
                    "date":              parse_date(row.get("TransactionDate", "")),
                    "transaction_type":  clean(row.get("TransactionType", "")),
                    "contributor_type":  filer_type,
                    "contributor_city":  clean(row.get("City", "")),
                    "contributor_state": clean(row.get("State", "")),
                    "contributor_zip":   clean(row.get("ZipCode", "")),
                    "employer":          clean(row.get("Employer", "")),
                    "occupation":        clean(row.get("Occupation", "")),
                    "candidate_name":    _format_name(filer_name, "") if filer_type == "Candidate" else "",
                    "office":            clean(reg.get("office_name") or reg.get("office", "")) if reg else "",
                    "election_year":     year,
                    "filing_id":         "",
                    "amended":           0,
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                count += 1

        print(f"{count:,} rows" + (f" (skipped {skipped})" if skipped else ""))

    # ── Expenditures ──────────────────────────────────────────────────────────
    for path in raw_files("Expenditures"):
        filer_type = filer_type_from_filename(path)
        year       = year_from_filename(path)
        print(f"  expenditures   {path.name}...", end=" ", flush=True)
        count = skipped = 0

        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                filer_name = clean(row.get("FilerName", ""))
                amount_raw = clean(row.get("Amount", ""))
                amount     = parse_amount(amount_raw)

                reg = lookup_filer(filer_name, filer_type, by_lastname, by_cmte_name)

                if reg:
                    state_filer_id = clean(reg["entity_id"])
                    committee_name = clean(reg["committee_name"])
                else:
                    state_filer_id = filer_name
                    committee_name = ""

                if not state_filer_id and not amount:
                    skipped += 1
                    continue

                expn_w.writerow({
                    "state":          STATE,
                    "state_filer_id": state_filer_id,
                    "committee_name": committee_name,
                    "payee_name":     clean(row.get("TransactionName", "")),
                    "amount":         amount,
                    "date":           parse_date(row.get("TransactionDate", "")),
                    "transaction_type": clean(row.get("TransactionType", "")),
                    "purpose":        clean(row.get("TransactionType", "")),
                    "category":       filer_type,
                    "payee_city":     clean(row.get("City", "")),
                    "payee_state":    clean(row.get("State", "")),
                    "payee_zip":      clean(row.get("ZipCode", "")),
                    "candidate_name": _format_name(filer_name, "") if filer_type == "Candidate" else "",
                    "office":         clean(reg.get("office_name") or reg.get("office", "")) if reg else "",
                    "election_year":  year,
                    "filing_id":      "",
                    "amended":        0,
                    "raw_file":       path.name,
                    "row_num":        row_num,
                })
                count += 1

        print(f"{count:,} rows" + (f" (skipped {skipped})" if skipped else ""))

    # ── Close all ──────────────────────────────────────────────────────────────
    for fh in (cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh):
        fh.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\nArizona: done.")
    print(f"  {cmte_count:,} committees")
    print(f"  {cand_count:,} candidates")


# ── Name helpers ───────────────────────────────────────────────────────────────
def _format_name(last_first: str, first: str) -> str:
    """
    Build a canonical 'First Last' name from registry fields.
      'Gallego, Ruben', ''   → 'Ruben Gallego'   (FilerName in transaction files)
      'Gallego',        'Ruben' → 'Ruben Gallego' (entity_last/first from registry)
    """
    last_first = (last_first or "").strip()
    first      = (first or "").strip()
    if not last_first:
        return first
    if "," in last_first:
        # "Last, First" combined string — split it
        parts = [p.strip() for p in last_first.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return f"{parts[1]} {parts[0]}"
        return parts[0]
    # Separate last + first fields from registry
    if first:
        return f"{first} {last_first}"
    return last_first


def _split_last_first(last_first: str) -> tuple[str, str]:
    """'Bagley, Nikki' → ('Bagley', 'Nikki'). Returns ('', '') if no comma."""
    if "," in last_first:
        parts = [p.strip() for p in last_first.split(",", 1)]
        return parts[0], parts[1] if len(parts) > 1 else ""
    return last_first, ""


if __name__ == "__main__":
    run()
