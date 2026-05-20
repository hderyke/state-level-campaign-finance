"""
parsers/arkansas.py — Transform Arkansas raw CSVs into the 5 normalized relations.

Input:  data/Arkansas/raw/
  contributions_{year}.csv    — TCON transactions (2022–present)
  expenditures_{year}.csv     — TEXP transactions (2022–present)
  candidates.csv              — registered candidates (from GetCandidateCommitteDetails)
  committees.csv              — registered committees (PAC, CPAC, IEF, PP, ECOMM)

Output: data/Arkansas/cleaned/
  contributions.csv, expenditures.csv, committees.csv,
  candidates.csv, loans_debts.csv (empty — no loan data available)

Notes
─────
  • Address field is a single string "Street, City, ST ZIP" — parsed with regex.
  • Amounts arrive as "$1,000.00" — dollar sign and commas stripped.
  • Non-itemized contributions have no Source Name — rows are kept as aggregate totals.
  • Entity Name for candidates may include committee name in parens:
      "Smith, John  (Smith for Governor)" → candidate_name = "Smith, John"
  • 2022 data is sparse (Nov–Dec only, candidates only) — included as-is.
  • No loans/debts table available from this source.
  • Registry join: Filing Entity ID in transactions = filerEntityID in registry files.
"""

import csv
import gzip
import re
import sys
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Arkansas" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Arkansas" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "AR"
MAX_VALID_YEAR = date.today().year + 2


# ── Helpers ────────────────────────────────────────────────────────────────────
def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'$1,000.00' → '1000.00', '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
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


# Matches "...street..., City, ST 12345" or "..., ST 12345-6789"
_ADDR_RE = re.compile(
    r'^(.*),\s*([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$'
)

def parse_address(addr: str) -> tuple[str, str, str]:
    """
    Parse 'Street, City, ST ZIP' → (city, state, zip).
    Returns ('', '', '') if the pattern doesn't match.
    """
    addr = (addr or "").strip()
    if not addr:
        return "", "", ""
    m = _ADDR_RE.match(addr)
    if not m:
        return "", "", ""
    street_city = m.group(1)
    state       = m.group(2).upper()
    zipcode     = m.group(3)
    # city is the last comma-delimited segment of street_city
    last_comma = street_city.rfind(",")
    city = street_city[last_comma + 1:].strip() if last_comma >= 0 else street_city.strip()
    return city, state, zipcode


def extract_candidate_name(entity_name: str) -> str:
    """
    Strip committee name in parens from Entity Name.
    'Smith, John  (Smith for Governor)' → 'Smith, John'
    """
    name = (entity_name or "").strip()
    paren_idx = name.find("(")
    if paren_idx >= 0:
        name = name[:paren_idx].strip()
    return name


def amended_flag(val: str) -> str:
    v = (val or "").strip().upper()
    if v == "Y":
        return "1"
    if v == "N":
        return "0"
    return ""


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


# ── Main ───────────────────────────────────────────────────────────────────────
def run():
    cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
    cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
    cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
    expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
    loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)

    # ── Load candidate registry ───────────────────────────────────────────────
    # Keyed by filerEntityID (string) for transaction join
    cand_registry: dict[str, dict] = {}
    cand_path = RAW_DIR / "candidates.csv"
    print("  candidates     candidates.csv...", end=" ", flush=True)
    cand_count = 0
    if cand_path.exists():
        with open(cand_path, newline="", encoding="utf-8") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                fid        = clean(row.get("filerEntityID", ""))
                first      = clean(row.get("firstName", ""))
                last       = clean(row.get("lastName", ""))
                suffix     = clean(row.get("suffix", ""))
                filer_name = clean(row.get("filerName", ""))

                # Prefer split first/last; fall back to filerName
                if last:
                    cand_name = f"{last}, {first}".rstrip(", ")
                    if suffix:
                        cand_name += f" {suffix}"
                else:
                    cand_name = extract_candidate_name(filer_name)

                entry = {
                    "filerEntityID":  fid,
                    "candidate_name": cand_name,
                    "candidate_first": first,
                    "candidate_last":  last,
                    "office":          clean(row.get("office", "")),
                    "district":        clean(row.get("officeDistrictName", "")),
                    "jurisdiction":    clean(row.get("jurisdictionName", "")),
                    "party":           clean(row.get("politicalParty", "")),
                    "election_year":   clean(row.get("electionYear", ""))
                                       or clean(row.get("filingYear", "")),
                    "status":          clean(row.get("filerStatus", "")),
                }
                if fid:
                    cand_registry[fid] = entry

                cand_w.writerow({
                    "state":           STATE,
                    "person_id":       fid,   # for Arkansas, person_id = state_filer_id directly
                    "state_filer_id":  fid,
                    "candidate_name":  cand_name,
                    "candidate_first": entry["candidate_first"],
                    "candidate_last":  entry["candidate_last"],
                    "office":          entry["office"],
                    "district":        entry["district"],
                    "jurisdiction":    entry["jurisdiction"],
                    "party":           entry["party"],
                    "election_year":   entry["election_year"],
                    "status":          entry["status"],
                    "incumbent":       "",
                    "raw_file":        cand_path.name,
                    "row_num":         row_num,
                })
                cand_count += 1
    print(f"{cand_count:,} rows")

    # ── Load committee registry ───────────────────────────────────────────────
    cmte_registry: dict[str, dict] = {}
    cmte_path = RAW_DIR / "committees.csv"
    print("  committees     committees.csv...", end=" ", flush=True)
    cmte_count = 0
    if cmte_path.exists():
        with open(cmte_path, newline="", encoding="utf-8") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                fid        = clean(row.get("filerEntityID", ""))
                filer_name = clean(row.get("filerName", ""))
                cmte_name  = clean(row.get("committeeName", "")) or filer_name
                filer_type = clean(row.get("filerType", ""))
                status     = clean(row.get("filerStatus", ""))

                entry = {
                    "committee_name": cmte_name,
                    "committee_type": filer_type,
                    "active":         "1" if status == "Active" else ("0" if status else ""),
                }
                if fid:
                    cmte_registry[fid] = entry

                cmte_w.writerow({
                    "state":          STATE,
                    "state_filer_id": fid,
                    "committee_name": cmte_name,
                    "committee_type": filer_type,
                    "candidate_name": "",
                    "treasurer_name": "",
                    "city":           "",
                    "zip":            "",
                    "active":         entry["active"],
                })
                cmte_count += 1
    print(f"{cmte_count:,} rows")

    # Also write candidate filers as committees (they have campaign accounts)
    extra = 0
    for fid, c in cand_registry.items():
        cmte_name = c["candidate_name"]
        cmte_w.writerow({
            "state":          STATE,
            "state_filer_id": fid,
            "committee_name": cmte_name,
            "committee_type": "Candidate",
            "candidate_name": cmte_name,
            "treasurer_name": "",
            "city":           "",
            "zip":            "",
            "active":         c["status"] == "Active" and "1" or "",
        })
        extra += 1
    print(f"  + {extra:,} candidate accounts written to committees")

    # ── Contributions ─────────────────────────────────────────────────────────
    total_cont = 0
    for path in raw_files("contributions_*.csv"):
        file_year = year_from_filename(path)
        print(f"  contributions  {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                fid        = clean(row.get("Filing Entity ID", ""))
                amount     = parse_amount(row.get("Transaction Amount", ""))
                if not amount:
                    continue

                entity_name = clean(row.get("Entity Name", ""))
                filer_type  = clean(row.get("FilerType", ""))
                is_cand     = (filer_type == "Candidate")

                # committee_name: strip trailing parens for candidates
                cmte_name   = extract_candidate_name(entity_name) if is_cand else entity_name

                # candidate_name from registry (preferred) or extracted from entity name
                if is_cand:
                    cand_name = (cand_registry.get(fid) or {}).get("candidate_name", "") \
                                or extract_candidate_name(entity_name)
                else:
                    cand_name = ""

                # Office/party from candidate registry
                reg       = cand_registry.get(fid, {}) if is_cand else {}
                office    = reg.get("office", "")
                elec_year = clean(row.get("Election Year", "")) or reg.get("election_year", "") or file_year

                # Source address parse
                city, st, zipcode = parse_address(row.get("Source Address", ""))

                # Occupation: prefer Occupation, fall back to Occupation Other
                occupation = clean(row.get("Occupation", "")) \
                             or clean(row.get("Occupation Other", ""))

                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    cmte_name,
                    "contributor_name":  clean(row.get("Source Name", "")),
                    "amount":            amount,
                    "date":              parse_date(row.get("Transaction Date", "")),
                    "transaction_type":  clean(row.get("Transaction Sub Type", "")),
                    "contributor_type":  clean(row.get("Funding Source / Loan Source Type", "")),
                    "contributor_city":  city,
                    "contributor_state": st,
                    "contributor_zip":   zipcode,
                    "employer":          clean(row.get("Employer Name", "")),
                    "occupation":        occupation,
                    "candidate_name":    cand_name,
                    "office":            office,
                    "election_year":     elec_year,
                    "filing_id":         clean(row.get("Transaction ID", "")),
                    "amended":           amended_flag(row.get("Amended", "")),
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                count += 1
        print(f"{count:,} rows")
        total_cont += count

    # ── Expenditures ─────────────────────────────────────────────────────────
    total_expn = 0
    for path in raw_files("expenditures_*.csv"):
        file_year = year_from_filename(path)
        print(f"  expenditures   {path.name}...", end=" ", flush=True)
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                fid    = clean(row.get("Filing Entity ID", ""))
                amount = parse_amount(row.get("Transaction Amount", ""))
                if not amount:
                    continue

                entity_name = clean(row.get("Entity Name", ""))
                filer_type  = clean(row.get("FilerType", ""))
                is_cand     = (filer_type == "Candidate")

                cmte_name = extract_candidate_name(entity_name) if is_cand else entity_name

                if is_cand:
                    cand_name = (cand_registry.get(fid) or {}).get("candidate_name", "") \
                                or extract_candidate_name(entity_name)
                else:
                    cand_name = ""

                reg       = cand_registry.get(fid, {}) if is_cand else {}
                office    = reg.get("office", "")
                elec_year = clean(row.get("Election Year", "")) or reg.get("election_year", "") or file_year

                city, st, zipcode = parse_address(row.get("Payee Address", ""))

                # category: use Transaction Category Others if more specific
                category = clean(row.get("Transaction Category Others", "")) \
                           or clean(row.get("Transaction Category", ""))

                expn_w.writerow({
                    "state":            STATE,
                    "committee_name":   cmte_name,
                    "payee_name":       clean(row.get("Payee Name", "")),
                    "amount":           amount,
                    "date":             parse_date(row.get("Transaction Date", "")),
                    "transaction_type": clean(row.get("Transaction Sub Type", "")),
                    "purpose":          clean(row.get("Transaction Description", "")),
                    "category":         category,
                    "payee_city":       city,
                    "payee_state":      st,
                    "payee_zip":        zipcode,
                    "candidate_name":   cand_name,
                    "office":           office,
                    "election_year":    elec_year,
                    "filing_id":        clean(row.get("Transaction ID", "")),
                    "amended":          amended_flag(row.get("Amended", "")),
                    "raw_file":         path.name,
                    "row_num":          row_num,
                })
                count += 1
        print(f"{count:,} rows")
        total_expn += count

    for fh in (cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh):
        fh.close()

    utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")

    print(f"\nArkansas: done.")
    print(f"  {cand_count:,} candidates  {cmte_count + extra:,} committees")
    print(f"  {total_cont:,} contributions  {total_expn:,} expenditures")
    print(f"  loans_debts: empty (no loan data available)")


if __name__ == "__main__":
    run()
