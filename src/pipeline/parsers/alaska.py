"""
Alaska.py — Parse Alaska APOC raw exports into canonical cleaned CSVs.

Raw files (all in data/Alaska/raw/):
  CDIncome_YYYY.csv      — contributions received
  CDExpense_YYYY.csv     — expenditures made
  CDCandidates_all.csv   — candidate registry
  GRForms_YYYY.csv       — group/committee registrations

Output (data/Alaska/cleaned/):
  contributions.csv, expenditures.csv, committees.csv,
  candidates.csv, loans_debts.csv (empty)
"""

import csv
import re
import sys
from pathlib import Path
from datetime import datetime, date

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C

csv.field_size_limit(sys.maxsize)

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Alaska" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Alaska" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "AK"
MAX_VALID_YEAR = date.today().year + 2


# ── Helpers ───────────────────────────────────────────────────────────────────
def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'$1,000.00' or '(500.00)' → plain decimal string, '' on failure."""
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
    """M/D/YYYY or MM/DD/YYYY → YYYY-MM-DD, '' on failure or implausible year."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1970 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def build_name(last: str, first: str) -> str:
    last, first = (last or "").strip(), (first or "").strip()
    if last and first:
        return f"{last}, {first}"
    return last or first


def invert_name(name: str) -> str:
    """'First [M] Last' (APOC Name field) → 'Last, First' (canonical form).
    Drops middle initials so the result matches the candidates table exactly.
    Used so candidate_name in contributions joins cleanly to candidates."""
    parts = (name or "").strip().split()
    if len(parts) <= 1:
        return name.strip()
    last  = parts[-1]
    first = parts[0]          # drop middle initial(s)
    return f"{last}, {first}"


def normalize_candidate(val: str) -> str:
    """'Last, First' → 'Last, First' (strip leading/trailing space)."""
    return (val or "").strip()


def year_from_filename(path: Path) -> str:
    m = re.search(r"(\d{4})", path.name)
    return m.group(1) if m else ""


def raw_files(pattern: str) -> list[Path]:
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    fh = open(CLEAN_DIR / filename, "w", newline="", encoding="utf-8")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    committees: dict[str, dict] = {}  # committee_name → row (flushed at end)

    cand_fh, cand_w = open_writer("candidates.csv",    C.CANDIDATES)
    cmte_fh, cmte_w = open_writer("committees.csv",    C.COMMITTEES)
    cont_fh, cont_w = open_writer("contributions.csv", C.CONTRIBUTIONS)
    expn_fh, expn_w = open_writer("expenditures.csv",  C.EXPENDITURES)
    loan_fh, loan_w = open_writer("loans_debts.csv",   C.LOANS_DEBTS)

    def register_committee(name: str, ctype: str):
        if name and name not in committees:
            committees[name] = {
                "state":          STATE,
                "state_filer_id": "",
                "committee_name": name,
                "committee_type": ctype,
                "candidate_name": "",
                "treasurer_name": "",
                "city":           "",
                "zip":            "",
                "active":         "",
            }

    # ── Candidates ────────────────────────────────────────────────────────────
    cand_path = RAW_DIR / "CDCandidates_all.csv"
    print("  candidates     CDCandidates_all.csv...", end=" ", flush=True)
    cand_count = 0
    if cand_path.exists() and cand_path.stat().st_size > 0:
        with open(cand_path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                name = normalize_candidate(row.get("Candidate", ""))
                if not name:
                    continue
                # split "Last, First" for first/last cols
                if "," in name:
                    last, _, first = name.partition(",")
                    last, first = last.strip(), first.strip()
                else:
                    last, first = name, ""
                cand_w.writerow({
                    "state":           STATE,
                    "candidate_name":  name,
                    "candidate_first": first,
                    "candidate_last":  last,
                    "office":          clean(row.get("Office", "")),
                    "district":        "",
                    "jurisdiction":    clean(row.get("Election", "")),
                    "party":           clean(row.get("Party", "")),
                    "election_year":   clean(row.get("Year", "")),
                    "status":          clean(row.get("Status", "")),
                    "incumbent":       "",
                    "raw_file":        cand_path.name,
                    "row_num":         row_num,
                })
                cand_count += 1
    print(f"{cand_count:,} rows")

    # ── Contributions (CDIncome) ───────────────────────────────────────────────
    # Alaska APOC re-exports the same transaction once per filing report
    # (original + any amendments all appear as separate rows with different
    # Result numbers). Dedup per file on (contributor, amount, date, committee),
    # keeping the row with the highest Result (most recent amendment).
    for path in raw_files("CDIncome_*.csv"):
        print(f"  contributions  {path.name}...", end=" ", flush=True)
        seen: dict[tuple, dict] = {}   # dedup_key → best row dict
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                amount = parse_amount(row.get("Amount", ""))
                if not amount:
                    continue
                filer      = clean(row.get("Name", ""))
                filer_type = clean(row.get("Filer Type", ""))
                register_committee(filer, filer_type)
                contributor = build_name(
                    row.get("Last/Business Name", ""),
                    row.get("First Name", ""),
                )
                date_val = parse_date(row.get("Date", ""))
                result   = clean(row.get("Result", ""))
                key = (contributor, amount, date_val, filer)
                prev = seen.get(key)
                # Keep row with highest Result number (most recent filing)
                if prev is None or (result.isdigit() and
                        (not prev["filing_id"].isdigit() or
                         int(result) > int(prev["filing_id"]))):
                    seen[key] = {
                        "state":             STATE,
                        "state_filer_id":    filer,
                        "committee_name":    filer,
                        "contributor_name":  contributor,
                        "amount":            amount,
                        "date":              date_val,
                        "transaction_type":  clean(row.get("Transaction Type", "")),
                        "contributor_type":  filer_type,
                        "contributor_city":  clean(row.get("City", "")),
                        "contributor_state": clean(row.get("State", "")),
                        "contributor_zip":   clean(row.get("Zip", "")),
                        "employer":          clean(row.get("Employer", "")),
                        "occupation":        clean(row.get("Occupation", "")),
                        "candidate_name":    invert_name(filer) if filer_type == "Candidate" else "",
                        "office":            clean(row.get("Office", "")),
                        "election_year":     clean(row.get("Report Year", year_from_filename(path))),
                        "filing_id":         result,
                        "amended":           "",
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    }
        for out_row in seen.values():
            cont_w.writerow(out_row)
        count = len(seen)
        print(f"{count:,} rows")

    # ── Expenditures (CDExpense) ───────────────────────────────────────────────
    # Same dedup logic as contributions — keep highest Result per
    # (payee, amount, date, committee) tuple.
    for path in raw_files("CDExpense_*.csv"):
        print(f"  expenditures   {path.name}...", end=" ", flush=True)
        seen: dict[tuple, dict] = {}
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                amount = parse_amount(row.get("Amount", ""))
                if not amount:
                    continue
                filer      = clean(row.get("Name", ""))
                filer_type = clean(row.get("Filer Type", ""))
                register_committee(filer, filer_type)
                payee    = build_name(
                    row.get("Last/Business Name", ""),
                    row.get("First Name", ""),
                )
                date_val = parse_date(row.get("Date", ""))
                result   = clean(row.get("Result", ""))
                key = (payee, amount, date_val, filer)
                prev = seen.get(key)
                if prev is None or (result.isdigit() and
                        (not prev["filing_id"].isdigit() or
                         int(result) > int(prev["filing_id"]))):
                    seen[key] = {
                        "state":            STATE,
                        "state_filer_id":   filer,
                        "committee_name":   filer,
                        "payee_name":       payee,
                        "amount":           amount,
                        "date":             date_val,
                        "transaction_type": clean(row.get("Transaction Type", "")),
                        "purpose":          clean(row.get("Purpose of Expenditure", "")),
                        "category":         clean(row.get("Payment Type", "")),
                        "payee_city":       clean(row.get("City", "")),
                        "payee_state":      clean(row.get("State", "")),
                        "payee_zip":        clean(row.get("Zip", "")),
                        "candidate_name":   invert_name(filer) if filer_type == "Candidate" else "",
                        "office":           clean(row.get("Office", "")),
                        "election_year":    clean(row.get("Report Year", year_from_filename(path))),
                        "filing_id":        result,
                        "amended":          "",
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    }
        for out_row in seen.values():
            expn_w.writerow(out_row)
        count = len(seen)
        print(f"{count:,} rows")

    # ── Committees: enrich from GRForms registration files ────────────────────
    print("  committees     GRForms_*.csv...", end=" ", flush=True)
    gr_count = 0
    for path in raw_files("GRForms_*.csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                name = clean(row.get("Name", ""))
                if not name:
                    continue
                status = clean(row.get("Status", ""))
                entry  = committees.get(name) or {
                    "state":          STATE,
                    "state_filer_id": "",
                    "committee_name": name,
                    "candidate_name": "",
                }
                entry["committee_type"] = (
                    " — ".join(filter(None, [
                        clean(row.get("Type", "")),
                        clean(row.get("Subtype", "")),
                    ]))
                    or entry.get("committee_type", "")
                )
                entry["treasurer_name"] = clean(row.get("Treasurer Name", ""))
                entry["city"]           = clean(row.get("City", ""))
                entry["zip"]            = clean(row.get("Zip", ""))
                entry["active"]         = 1 if status == "Filed" else (0 if status else "")
                entry["state_filer_id"] = clean(row.get("Abbreviation", "")) or entry.get("state_filer_id", "")
                committees[name] = entry
                gr_count += 1

    # Flush committees
    for row in committees.values():
        cmte_w.writerow(row)
    print(f"{len(committees):,} unique ({gr_count:,} GRForms records)")

    for fh in (cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh):
        fh.close()

    print(f"\nAlaska: done.")
    print(f"  {len(committees):,} committees")
    print(f"  {cand_count:,} candidates")


if __name__ == "__main__":
    run()
