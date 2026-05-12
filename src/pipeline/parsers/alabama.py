"""
parsers/alabama.py — Transform Alabama raw CSVs into the 5 normalized relations.

Input:  data/alabama/raw/{year}_{FileType}Extract1.csv  (2013–present)
Output: data/alabama/cleaned/{relation}.csv

File types
──────────
  CashContributions    → contributions  (transaction_type: Cash/In-Kind)
  InKindContributions  → contributions  (transaction_type: In-Kind)
  Expenditures         → expenditures
  OtherReceipts        → loans_debts   (ReceiptType == 'Loan')
                       → contributions  (everything else — refunds, interest, etc.)

Notes
─────
  • Alabama is a flat-file state: CandidateName and CommitteeName appear on
    every transaction row. Committees and candidates are synthesized here.
  • PCC registry (pcc_committees.csv) enriches both tables: committee gets
    treasurer/address/active; candidate gets office/district/party/jurisdiction.
  • Contributor/payee name is split across LastName, FirstName, MI, Suffix.
  • Dates arrive as MM/DD/YYYY and are normalized to YYYY-MM-DD.
  • Amended is Y/N → 1/0.
"""

import csv
import re
import sys
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C

csv.field_size_limit(sys.maxsize)

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "alabama" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "alabama" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "AL"


# ── Helpers ────────────────────────────────────────────────────────────────────
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
    return 1 if (val or "").strip().upper() == "Y" else 0


def clean(val) -> str:
    return (val or "").strip()


def is_numeric(val) -> bool:
    """Return True if val can be cast to float (allows negatives)."""
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


# ── Writers ────────────────────────────────────────────────────────────────────
def open_writer(filename: str, fieldnames: list[str]):
    path = CLEAN_DIR / filename
    fh   = open(path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=fieldnames,
                            extrasaction="ignore", restval="")
    writer.writeheader()
    return fh, writer


# ── Column definitions (canonical — shared across all states) ──────────────────
COMMITTEE_COLS    = C.COMMITTEES
CANDIDATE_COLS    = C.CANDIDATES
CONTRIBUTION_COLS = C.CONTRIBUTIONS
EXPENDITURE_COLS  = C.EXPENDITURES
LOAN_COLS         = C.LOANS_DEBTS


# ── Parse ──────────────────────────────────────────────────────────────────────
def run():
    # Accumulate unique committees and candidates across all files
    committees: dict[str, dict] = {}   # state_filer_id → row
    candidates: set[str] = set()

    # Open all output writers upfront
    cmte_fh,   cmte_w   = open_writer("committees.csv",   COMMITTEE_COLS)
    cand_fh,   cand_w   = open_writer("candidates.csv",   CANDIDATE_COLS)
    cont_fh,   cont_w   = open_writer("contributions.csv", CONTRIBUTION_COLS)
    expn_fh,   expn_w   = open_writer("expenditures.csv",  EXPENDITURE_COLS)
    loan_fh,   loan_w   = open_writer("loans_debts.csv",   LOAN_COLS)

    def register_committee(filer_id: str, name: str, ctype: str, cand_name: str = ""):
        if filer_id and filer_id not in committees:
            committees[filer_id] = {
                "state":          STATE,
                "state_filer_id": filer_id,
                "committee_name": name,
                "committee_type": ctype,
                "candidate_name": cand_name,
                # treasurer_name, city, zip, active filled in at flush from PAC registry
            }

    def register_candidate(name: str):
        name = name.strip()
        if name and name not in candidates:
            candidates.add(name)

    # ── Cash + InKind Contributions ───────────────────────────────────────────
    for pattern in ("*_CashContributionsExtract1.csv",
                    "*_InKindContributionsExtract1.csv"):
        for path in raw_files(pattern):
            year = year_from_filename(path)
            print(f"  contributions  {path.name}...", end=" ", flush=True)
            count = skipped = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    filer_id   = clean(row.get("CommitteeId", ""))
                    amount_raw = clean(row.get("ContributionAmount", ""))

                    # Skip column-shifted / malformed rows
                    if not filer_id or not is_numeric(amount_raw):
                        skipped += 1
                        continue

                    cmte_name  = clean(row.get("CommitteeName", ""))
                    cmte_type  = clean(row.get("CommitteeType", ""))
                    cand_name  = clean(row.get("CandidateName", ""))

                    register_committee(filer_id, cmte_name, cmte_type, cand_name)
                    register_candidate(cand_name)

                    cont_w.writerow({
                        "state":             STATE,
                        "state_filer_id":    filer_id,
                        "committee_name":    cmte_name,
                        "contributor_name":  build_name(
                                                row.get("LastName", ""),
                                                row.get("FirstName", ""),
                                                row.get("MI", ""),
                                                row.get("Suffix", ""),
                                             ),
                        "amount":            amount_raw,
                        "date":              parse_date(row.get("ContributionDate", "")),
                        "transaction_type":  clean(row.get("ContributionType", "")),
                        "contributor_type":  clean(row.get("ContributorType", "")),
                        "contributor_city":  clean(row.get("City", "")),
                        "contributor_state": clean(row.get("State", "")),
                        "contributor_zip":   clean(row.get("Zip", "")),
                        "candidate_name":    cand_name,
                        "election_year":     year,
                        "filing_id":         clean(row.get("ContributionID",
                                                   row.get("InKindContributionID", ""))),
                        "amended":           yn_to_int(row.get("Amended", "N")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            skip_str = f"  ({skipped} skipped)" if skipped else ""
            print(f"{count:,} rows{skip_str}")

    # ── Other Receipts (loans → loans_debts, rest → contributions) ────────────
    for path in raw_files("*_OtherReceiptsExtract1.csv"):
        year = year_from_filename(path)
        print(f"  other receipts {path.name}...", end=" ", flush=True)
        loans = contribs = skipped = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                filer_id    = clean(row.get("CommitteeId", ""))
                amount_raw  = clean(row.get("ReceiptAmount", ""))

                if not filer_id or not is_numeric(amount_raw):
                    skipped += 1
                    continue

                cmte_name   = clean(row.get("CommitteeName", ""))
                cmte_type   = clean(row.get("CommitteeType", ""))
                cand_name   = clean(row.get("CandidateName", ""))
                receipt_type = clean(row.get("ReceiptType", ""))

                register_committee(filer_id, cmte_name, cmte_type, cand_name)
                register_candidate(cand_name)

                name = build_name(
                    row.get("LastName", ""), row.get("FirstName", ""),
                    row.get("MI", ""),       row.get("Suffix", ""),
                )

                if receipt_type == "Loan":
                    loan_w.writerow({
                        "state":              STATE,
                        "state_filer_id":     filer_id,
                        "committee_name":     cmte_name,
                        "record_type":        "loan",
                        "counterparty_name":  name,
                        "counterparty_city":  clean(row.get("City", "")),
                        "counterparty_state": clean(row.get("State", "")),
                        "counterparty_zip":   clean(row.get("Zip", "")),
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
                        "state":             STATE,
                        "state_filer_id":    filer_id,
                        "committee_name":    cmte_name,
                        "contributor_name":  name,
                        "amount":            clean(row.get("ReceiptAmount", "")),
                        "date":              parse_date(row.get("ReceiptDate", "")),
                        "transaction_type":  receipt_type,
                        "contributor_type":  clean(row.get("ReceiptSourceType", "")),
                        "contributor_city":  clean(row.get("City", "")),
                        "contributor_state": clean(row.get("State", "")),
                        "contributor_zip":   clean(row.get("Zip", "")),
                        "candidate_name":    cand_name,
                        "election_year":     year,
                        "filing_id":         clean(row.get("ReceiptID", "")),
                        "amended":           yn_to_int(row.get("Amended", "N")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    contribs += 1
        skip_str = f"  ({skipped} skipped)" if skipped else ""
        print(f"{loans:,} loans  {contribs:,} other{skip_str}")

    # ── Expenditures ──────────────────────────────────────────────────────────
    for path in raw_files("*_ExpendituresExtract1.csv"):
        year = year_from_filename(path)
        print(f"  expenditures   {path.name}...", end=" ", flush=True)
        count = skipped = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                filer_id   = clean(row.get("CommitteeId", ""))
                amount_raw = clean(row.get("ExpenditureAmount", ""))

                if not filer_id or not is_numeric(amount_raw):
                    skipped += 1
                    continue

                cmte_name = clean(row.get("CommitteeName", ""))
                cmte_type = clean(row.get("CommitteeType", ""))
                cand_name = clean(row.get("CandidateName", ""))

                register_committee(filer_id, cmte_name, cmte_type, cand_name)
                register_candidate(cand_name)

                expn_w.writerow({
                    "state":          STATE,
                    "state_filer_id": filer_id,
                    "committee_name": cmte_name,
                    "payee_name":     build_name(
                                          row.get("LastName", ""),
                                          row.get("FirstName", ""),
                                          row.get("MI", ""),
                                          row.get("Suffix", ""),
                                      ),
                    "amount":         amount_raw,
                    "date":           parse_date(row.get("ExpenditureDate", "")),
                    "transaction_type": clean(row.get("ExpenditureType", "")),
                    "purpose":        clean(row.get("Purpose", "")),
                    "category":       clean(row.get("Explanation", "")),
                    "payee_city":     clean(row.get("City", "")),
                    "payee_state":    clean(row.get("State", "")),
                    "payee_zip":      clean(row.get("Zip", "")),
                    "candidate_name": cand_name,
                    "election_year":  year,
                    "filing_id":      clean(row.get("ExpenditureID", "")),
                    "amended":        yn_to_int(row.get("Amended", "N")),
                    "raw_file":       path.name,
                    "row_num":        row_num,
                })
                count += 1
        skip_str = f"  ({skipped} skipped)" if skipped else ""
        print(f"{count:,} rows{skip_str}")

    # ── Load PAC + PCC registries for committee enrichment ───────────────────
    registry: dict[str, dict] = {}
    for reg_filename in ("pac_committees.csv", "pcc_committees.csv"):
        reg_path = RAW_DIR / reg_filename
        if reg_path.exists():
            with open(reg_path, newline="", encoding="utf-8") as f:
                before = len(registry)
                for reg in csv.DictReader(f):
                    cid = reg.get("committee_id", "").strip()
                    if cid:
                        registry[cid] = reg
            print(f"  {reg_filename}: {len(registry) - before:,} entries loaded")
        else:
            print(f"  ({reg_filename} not found — skipping)")
    print(f"  Registry total: {len(registry):,} committees")

    # ── Flush committees (enriched with registry where available) ────────────
    # Also build candidate_info: cand_name → PCC attributes for candidate flush
    candidate_info: dict[str, dict] = {}
    enriched = 0
    for filer_id, row in committees.items():
        reg = registry.get(filer_id, {})
        if reg:
            t_first = reg.get("treasurer_first", "").strip()
            t_last  = reg.get("treasurer_last",  "").strip()
            row["treasurer_name"] = f"{t_first} {t_last}".strip()
            row["city"]           = reg.get("city", "").strip()
            row["zip"]            = reg.get("zip_code", "").strip()
            status                = reg.get("committee_status", "").strip()
            row["active"]         = 1 if status == "Active" else (0 if status == "Dissolved" else "")
            enriched += 1

            # If this is a PCC, capture candidate-level attributes keyed by
            # the CandidateName string that appears on transaction rows
            cand_name = row.get("candidate_name", "").strip()
            if cand_name and reg.get("committee_type", "") == "Principal Campaign Committee":
                candidate_info[cand_name] = {
                    "candidate_first": reg.get("candidate_first", "").strip(),
                    "candidate_last":  reg.get("candidate_last",  "").strip(),
                    "office":          reg.get("office",          "").strip(),
                    "district":        reg.get("district",        "").strip(),
                    "jurisdiction":    reg.get("jurisdiction",    "").strip(),
                    "party":           reg.get("party",           "").strip(),
                }

        cmte_w.writerow(row)
    print(f"  {enriched:,} committees enriched from registry")
    print(f"  {len(candidate_info):,} candidates enriched from PCC registry")

    for name in sorted(candidates):
        info = candidate_info.get(name, {})
        cand_w.writerow({
            "state":           STATE,
            "candidate_name":  name,
            "candidate_first": info.get("candidate_first", ""),
            "candidate_last":  info.get("candidate_last",  ""),
            "office":          info.get("office",          ""),
            "district":        info.get("district",        ""),
            "jurisdiction":    info.get("jurisdiction",    ""),
            "party":           info.get("party",           ""),
        })

    for fh in (cmte_fh, cand_fh, cont_fh, expn_fh, loan_fh):
        fh.close()

    print(f"\nAlabama: done.")
    print(f"  {len(committees):,} committees")
    print(f"  {len(candidates):,} candidates")


if __name__ == "__main__":
    run()
