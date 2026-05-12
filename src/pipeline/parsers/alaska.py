"""
parsers/alaska.py — Transform Alaska APOC raw CSVs into the 5 normalized relations.

Input:  data/alaska/raw/CD{Income,Expense,Debts}_{year}.csv  (2008–present)
        data/alaska/raw/CDCandidates_all.csv
Output: data/alaska/cleaned/{relation}.csv

File types
──────────
  CDIncome_{year}     → contributions
  CDExpense_{year}    → expenditures
  CDDebts_{year}      → loans_debts  (record_type = 'debt')
  CDCandidates_all    → candidates   (also seeds committees)

Notes
─────
  • Amount fields arrive as "$1,234.56" — strip $ and commas before casting.
  • Dates arrive as M/D/YYYY — normalize to YYYY-MM-DD.
  • Debts file has two columns named "Name" (creditor, then filer) — handled
    with positional csv.reader rather than DictReader.
  • Candidate names are "Last, First" — reversed and rejoined.
  • No numeric committee ID — filer Name is the unique key for committees.
  • Income and Expense files share the same column layout.
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
RAW_DIR      = PROJECT_ROOT / "data" / "alaska" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "alaska" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "AK"


# ── Helpers ────────────────────────────────────────────────────────────────────
MAX_VALID_YEAR = date.today().year + 2   # anything beyond this is a typo


def parse_date(val) -> str:
    """M/D/YYYY or MM/DD/YYYY → YYYY-MM-DD. Returns '' on failure or implausible year."""
    val = (val or "").strip()
    if not val:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(val, fmt)
            if d.year < 1970 or d.year > MAX_VALID_YEAR:
                return ""   # typo in source (e.g. 2911, 3014)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def parse_amount(val) -> str:
    """
    Parse currency strings to plain decimals.
      '$1,234.56'  → '1234.56'
      '(100.00)'   → '-100.00'   (accounting negative notation)
    Returns '' on failure.
    """
    val = (val or "").strip().replace("$", "").replace(",", "")
    if not val:
        return ""
    # Accounting negatives: (100.00) → -100.00
    if val.startswith("(") and val.endswith(")"):
        val = "-" + val[1:-1]
    try:
        float(val)   # validate
        return val
    except ValueError:
        return ""


def clean(val) -> str:
    return (val or "").strip()


def normalize_candidate_name(val) -> str:
    """
    Alaska stores candidate names as "Last, First" (sometimes with leading
    space). Reformat to "First Last".
    """
    val = (val or "").strip()
    if "," in val:
        parts = [p.strip() for p in val.split(",", 1)]
        if len(parts) == 2 and parts[1]:
            return f"{parts[1]} {parts[0]}"
        return parts[0]
    return val


def build_contributor_name(last: str, first: str) -> str:
    """Join last/business name and first name. If no first, it's an org."""
    last  = (last  or "").strip()
    first = (first or "").strip()
    if not first:
        return last
    return f"{first} {last}"


def year_from_filename(path: Path) -> int:
    m = re.search(r"(\d{4})", path.name)
    return int(m.group(1)) if m else 0


def raw_files(pattern: str) -> list[Path]:
    return sorted(RAW_DIR.glob(pattern), key=lambda p: p.name)


# ── Writers ────────────────────────────────────────────────────────────────────
def open_writer(filename: str, fieldnames: list[str]):
    path = CLEAN_DIR / filename
    fh   = open(path, "w", newline="", encoding="utf-8")
    w    = csv.DictWriter(fh, fieldnames=fieldnames,
                          extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ── Column definitions (canonical — shared across all states) ──────────────────
CANDIDATE_COLS    = C.CANDIDATES
COMMITTEE_COLS    = C.COMMITTEES
CONTRIBUTION_COLS = C.CONTRIBUTIONS
EXPENDITURE_COLS  = C.EXPENDITURES
LOAN_COLS         = C.LOANS_DEBTS


# ── Parse ──────────────────────────────────────────────────────────────────────
def run():
    committees: dict[str, dict] = {}   # committee_name → row
    candidates_seen: set[str]   = set()

    cand_fh, cand_w = open_writer("candidates.csv",   CANDIDATE_COLS)
    cmte_fh, cmte_w = open_writer("committees.csv",   COMMITTEE_COLS)
    cont_fh, cont_w = open_writer("contributions.csv", CONTRIBUTION_COLS)
    expn_fh, expn_w = open_writer("expenditures.csv",  EXPENDITURE_COLS)
    loan_fh, loan_w = open_writer("loans_debts.csv",   LOAN_COLS)

    def register_committee(name: str, filer_type: str, office: str = ""):
        """
        Add filer to committees dict (first-seen wins).
        For Candidate filers, also write a candidates row if not seen before.
        Group/Entity filers go to committees only.
        """
        if not name:
            return
        if name not in committees:
            committees[name] = {
                "state":          STATE,
                "committee_name": name,
                "committee_type": filer_type,
            }
        # Seed candidates table from income/expense filer data
        if filer_type == "Candidate" and name not in candidates_seen:
            candidates_seen.add(name)
            cand_w.writerow({
                "state":          STATE,
                "candidate_name": normalize_candidate_name(name),
                "office":         office,
                "raw_file":       "",
                "row_num":        "",
            })

    # ── Candidates ────────────────────────────────────────────────────────────
    cand_path = RAW_DIR / "CDCandidates_all.csv"
    if cand_path.exists():
        print(f"  candidates     CDCandidates_all.csv...", end=" ", flush=True)
        count = 0
        with open(cand_path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                raw_name = clean(row.get("Candidate", ""))
                name     = normalize_candidate_name(raw_name)
                if not name or name in candidates_seen:
                    continue
                candidates_seen.add(name)

                # Status / incumbent
                status   = clean(row.get("Status", ""))
                won_raw  = clean(row.get("Won", ""))
                won      = 1 if won_raw.lower() in ("yes", "true", "1", "won") else 0

                cand_w.writerow({
                    "state":          STATE,
                    "candidate_name": name,
                    "office":         clean(row.get("Office", "")),
                    "party":          clean(row.get("Party", "")),
                    "election_year":  clean(row.get("Year", "")),
                    "status":         status,
                    "incumbent":      won,
                    "raw_file":       cand_path.name,
                    "row_num":        row_num,
                })

                # Also seed committees from candidate filer info
                # Alaska doesn't give a separate committee name in this file,
                # so we skip — committees come from income/expense files.
                count += 1
        print(f"{count:,} rows")

    # ── Income (contributions) ─────────────────────────────────────────────────
    for path in raw_files("CDIncome_*.csv"):
        year = year_from_filename(path)
        print(f"  contributions  {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                filer_name  = clean(row.get("Name", ""))
                filer_type  = clean(row.get("Filer Type", ""))
                office      = clean(row.get("Office", ""))

                register_committee(filer_name, filer_type, office)

                cont_w.writerow({
                    "state":             STATE,
                    "state_filer_id":    filer_name,
                    "committee_name":    filer_name,
                    "contributor_name":  build_contributor_name(
                                             row.get("Last/Business Name", ""),
                                             row.get("First Name", ""),
                                         ),
                    "amount":            parse_amount(row.get("Amount", "")),
                    "date":              parse_date(row.get("Date", "")),
                    "transaction_type":  clean(row.get("Payment Type", "")),
                    "contributor_type":  filer_type,
                    "contributor_city":  clean(row.get("City", "")),
                    "contributor_state": clean(row.get("State", "")),
                    "contributor_zip":   clean(row.get("Zip", "")),
                    "employer":          clean(row.get("Employer", "")),
                    "occupation":        clean(row.get("Occupation", "")),
                    "candidate_name":    filer_name if filer_type == "Candidate" else "",
                    "office":            office,
                    "election_year":     clean(row.get("Report Year", str(year))),
                    "filing_id":         clean(row.get("Result", "")),
                    "amended":           0,
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                count += 1
        print(f"{count:,} rows")

    # ── Expenditures ──────────────────────────────────────────────────────────
    for path in raw_files("CDExpense_*.csv"):
        year = year_from_filename(path)
        print(f"  expenditures   {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                filer_name = clean(row.get("Name", ""))
                filer_type = clean(row.get("Filer Type", ""))
                office     = clean(row.get("Office", ""))

                register_committee(filer_name, filer_type, office)

                expn_w.writerow({
                    "state":          STATE,
                    "state_filer_id": filer_name,
                    "committee_name": filer_name,
                    "payee_name":     build_contributor_name(
                                          row.get("Last/Business Name", ""),
                                          row.get("First Name", ""),
                                      ),
                    "amount":         parse_amount(row.get("Amount", "")),
                    "date":           parse_date(row.get("Date", "")),
                    "transaction_type": clean(row.get("Payment Type", "")),
                    "purpose":        clean(row.get("Purpose of Expenditure", "")),
                    "payee_city":     clean(row.get("City", "")),
                    "payee_state":    clean(row.get("State", "")),
                    "payee_zip":      clean(row.get("Zip", "")),
                    "candidate_name": filer_name if filer_type == "Candidate" else "",
                    "office":         office,
                    "election_year":  clean(row.get("Report Year", str(year))),
                    "filing_id":      clean(row.get("Result", "")),
                    "amended":        0,
                    "raw_file":       path.name,
                    "row_num":        row_num,
                })
                count += 1
        print(f"{count:,} rows")

    # ── Debts ─────────────────────────────────────────────────────────────────
    # Duplicate "Name" columns — use positional csv.reader.
    # Header: Result, Date, Balance Remaining, Original Amount, Name(creditor),
    #         Address, City, State, Zip, Country, Description/Purpose, --------,
    #         Filer Type, Name(filer), Report Year, Submitted
    DEBT_COLS = [
        "result", "date", "balance_remaining", "original_amount",
        "creditor_name", "address", "city", "state", "zip", "country",
        "purpose", "sep", "filer_type", "filer_name", "report_year", "submitted",
    ]

    for path in raw_files("CDDebts_*.csv"):
        year = year_from_filename(path)
        print(f"  debts          {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header (file may be empty)
            except StopIteration:
                print("0 rows")
                continue
            for row_num, raw in enumerate(reader, start=2):
                if len(raw) < len(DEBT_COLS):
                    raw += [""] * (len(DEBT_COLS) - len(raw))
                row = dict(zip(DEBT_COLS, raw))

                filer_name = clean(row["filer_name"])
                filer_type = clean(row["filer_type"])
                register_committee(filer_name, filer_type)

                loan_w.writerow({
                    "state":               STATE,
                    "state_filer_id":      filer_name,
                    "record_type":         "debt",
                    "counterparty_name":   clean(row["creditor_name"]),
                    "counterparty_city":   clean(row["city"]),
                    "counterparty_state":  clean(row["state"]),
                    "counterparty_zip":    clean(row["zip"]),
                    "original_amount":     parse_amount(row["original_amount"]),
                    "outstanding_balance": parse_amount(row["balance_remaining"]),
                    "date":                parse_date(row["date"]),
                    "purpose":             clean(row["purpose"]),
                    "candidate_name":      filer_name if filer_type == "Candidate" else "",
                    "election_year":       clean(row["report_year"]) or str(year),
                    "raw_file":            path.name,
                    "row_num":             row_num,
                })
                count += 1
        print(f"{count:,} rows")

    # ── Enrich committees from GRForms registration files ─────────────────────
    # GRForms_{year}.csv columns (from APOC export):
    #   Report Year, Name, Abbreviation, Type, Subtype, Address, City, State,
    #   Zip, Country, Treasurer Name, Treasurer Phone, Treasurer Email,
    #   Chair Name, Chair Phone, Chair Email, Submitted, Status
    gr_enriched = 0
    for path in raw_files("GRForms_*.csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                name = clean(row.get("Name", ""))
                if not name:
                    continue
                # Upsert into committees dict — registration data wins on metadata
                entry = committees.get(name, {
                    "state":          STATE,
                    "committee_name": name,
                    "committee_type": clean(row.get("Type", "")),
                })
                entry["committee_type"] = clean(row.get("Type", "")) or entry.get("committee_type", "")
                entry["treasurer_name"] = clean(row.get("Treasurer Name", ""))
                entry["city"]           = clean(row.get("City", ""))
                entry["zip"]            = clean(row.get("Zip", ""))
                committees[name] = entry
                gr_enriched += 1

    if gr_enriched:
        print(f"  GRForms: enriched {gr_enriched:,} committee records")

    # ── Flush committees ──────────────────────────────────────────────────────
    for row in committees.values():
        cmte_w.writerow(row)

    for fh in (cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh):
        fh.close()

    print(f"\nAlaska: done.")
    print(f"  {len(committees):,} committees")
    print(f"  {len(candidates_seen):,} candidates")


if __name__ == "__main__":
    run()
