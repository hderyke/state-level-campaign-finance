"""
parsers/minnesota.py — Transform Minnesota CFB raw data into the 5 normalized relations.

Input:  data/Minnesota/raw/
  mn_contributions.csv          — all contribution transactions (2015–present)
  mn_expenditures.csv           — all expenditure transactions
  mn_ind_expenditures.csv       — independent expenditures (merged into expenditures)
  candidates_entity.json        — PCC entity details keyed by RegisteredEntityID
  committees_entity.json        — PCF entity details keyed by RegisteredEntityID
  party_units_entity.json       — PTU entity details keyed by RegisteredEntityID

Output: data/Minnesota/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Schema notes
────────────
  Contributions
    • "Recipient reg num" joins to entity detail JSON via RegisteredEntityID
    • "Receipt type" == "Loan Payable" → loans_debts.csv.gz; all others → contributions
    • In-kind contributions are flagged in "In kind?" column; we fold them into
      transaction_type as "In-Kind"
    • Contributor zip from "Contrib zip" — MN stores zip only (no city/state for contributor)

  Expenditures (regular + independent)
    • Regular: "Committee reg num" = filer; "Vendor *" columns for payee
    • IE: "Spender Reg Num" = filer; "For /Against" value merged into transaction_type;
      "Affected Comte Name" (typo in source) is the supported/opposed committee — stored in purpose
    • IE "Vendor State" (capital S) differs from regular "Vendor state" (lower s) — handled below

  Candidates
    • CandidateMasterNameID is the stable person-level ID (persists across cycles)
    • One row per unique CandidateMasterNameID; kept = entry with highest ElectionYear
    • state_filer_id = CandidateMasterNameID → id_model = "person"
    • CandidateFullName format is "Last, First" — split on first ", "

  Committees
    • One row per RegisteredEntityID across all three entity types (PCC, PCF, PTU)
    • state_filer_id = RegisteredEntityID
    • active = "1" when TerminationDate is null/empty, "0" otherwise

  Loans
    • Only "Loan Payable" receipt_type rows from contributions; sparse (~14 rows)
    • counterparty = contributor (lender); committee = recipient (borrower)

  Entity JSON fallback
    • If entity JSON files are absent (entity sweep not yet run), committee_name and
      candidate_name fall back to the raw transaction column values.
"""

import csv
import gzip
import json
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

# ========================= ZIP3 → state lookup ========================
# Built from USPS prefix ranges. Covers all 50 states + DC.
# Returns '' for military/overseas (090-098, 340, 962-966, 969),
# territories (006-009 PR, 008 VI, 969 GU), and unassigned prefixes.
# Accuracy for domestic political contributions: ~97-98%.
# Edge cases: a handful of ZIPs straddle state lines (e.g. some 885xx
# El Paso ZIPs, Texarkana), and 4-digit ZIPs missing the leading zero
# are zero-padded before lookup.

def _build_zip3_state() -> dict[str, str]:
    m: dict[str, str] = {}
    def r(lo: int, hi: int, st: str):
        for z in range(lo, hi + 1):
            m[f"{z:03d}"] = st
    r(10,  27,  "MA"); r(28,  29,  "RI"); r(30,  38,  "NH")
    r(39,  49,  "ME"); r(50,  59,  "VT"); r(60,  69,  "CT")
    r(70,  89,  "NJ"); r(100, 149, "NY"); r(150, 196, "PA")
    r(197, 199, "DE"); r(200, 205, "DC"); r(206, 219, "MD")
    r(220, 246, "VA"); r(247, 268, "WV"); r(270, 289, "NC")
    r(290, 299, "SC"); r(300, 319, "GA"); r(320, 349, "FL")
    r(350, 369, "AL"); r(370, 385, "TN"); r(386, 397, "MS")
    m["398"] = "GA"; m["399"] = "GA"   # south GA satellite offices
    r(400, 427, "KY"); r(430, 459, "OH"); r(460, 479, "IN")
    r(480, 499, "MI"); r(500, 528, "IA"); r(530, 549, "WI")
    r(550, 567, "MN"); r(570, 577, "SD"); r(580, 588, "ND")
    r(590, 599, "MT"); r(600, 629, "IL"); r(630, 658, "MO")
    r(660, 679, "KS"); r(680, 693, "NE"); r(700, 714, "LA")
    r(716, 729, "AR"); r(730, 749, "OK"); r(750, 799, "TX")
    r(800, 816, "CO"); r(820, 831, "WY"); r(832, 838, "ID")
    r(840, 847, "UT"); r(850, 865, "AZ"); r(870, 884, "NM")
    m["885"] = "TX"                    # El Paso outlier
    r(889, 898, "NV"); r(900, 961, "CA")
    r(967, 968, "HI"); r(970, 979, "OR"); r(980, 994, "WA")
    r(995, 999, "AK")
    return m

_ZIP3_STATE = _build_zip3_state()


def _zip_to_state(zipcode: str) -> str:
    """
    Infer US state abbreviation from ZIP prefix.
    Handles 5-digit, ZIP+4 (55401-1234), and 4-digit dropped-leading-zero ZIPs.
    Returns '' for military, territories, unassigned, or non-numeric input.
    """
    z = (zipcode or "").strip().split("-")[0].strip()
    if not z.isdigit():
        return ""
    if len(z) == 4:        # dropped leading zero (e.g. '1001' → '01001')
        z = "0" + z
    if len(z) < 3:
        return ""
    return _ZIP3_STATE.get(z[:3], "")


# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Minnesota" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Minnesota" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "MN"
MAX_VALID_YEAR = date.today().year + 2


# ============================== helpers ==============================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'1,000.00' or '$1,000.00' or '1000.00' → '1000.00', '' on failure."""
    v = (val or "").strip().lstrip("$").replace(",", "")
    if not v:
        return ""
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """Accept M/D/YYYY, MM/DD/YYYY, YYYY-MM-DD → YYYY-MM-DD, '' on failure."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%-m/%-d/%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Last-chance: try split on "/" if 3 parts
    parts = v.split("/")
    if len(parts) == 3:
        try:
            month, day, yr = int(parts[0]), int(parts[1]), int(parts[2])
            d = datetime(yr, month, day)
            if 1990 <= d.year <= MAX_VALID_YEAR:
                return d.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
    return ""


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def _split_candidate_name(full: str) -> tuple[str, str]:
    """
    'Walz, Tim' → (last='Walz', first='Tim').
    Handles 'Last, First Middle' and 'Last, First' but not 'First Last'.
    Returns ('', full) if no comma found.
    """
    full = (full or "").strip()
    if not full:
        return "", ""
    if ", " in full:
        last, first = full.split(", ", 1)
        return last.strip(), first.strip()
    return "", full


def _parse_json_cache(path: Path) -> dict:
    """Load entity JSON, return {} on missing/corrupt file."""
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ================================ run =================================

def run():
    log = get_logger("minnesota", "parse")
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

        # ── Load entity JSON files ─────────────────────────────────────
        pcc_json = _parse_json_cache(RAW_DIR / "candidates_entity.json")
        pcf_json = _parse_json_cache(RAW_DIR / "committees_entity.json")
        ptu_json = _parse_json_cache(RAW_DIR / "party_units_entity.json")

        log.info(
            f"Entity caches: PCC={len(pcc_json):,} PCF={len(pcf_json):,} "
            f"PTU={len(ptu_json):,}"
        )

        # Combined lookup: reg_num → entity dict (all types)
        entity_registry: dict[str, dict] = {}
        for cache in (pcc_json, pcf_json, ptu_json):
            for reg_num, ent in cache.items():
                if ent:  # skip empty {} fallback entries
                    entity_registry[reg_num] = ent

        # ── Candidates ────────────────────────────────────────────────
        # De-dup by CandidateMasterNameID; keep highest ElectionYear entry
        best_per_person: dict[str, dict] = {}   # {CandidateMasterNameID: entity_dict}
        for reg_num, ent in pcc_json.items():
            if not ent:
                continue
            pid = clean(ent.get("CandidateMasterNameID", ""))
            if not pid:
                continue
            yr = int(ent.get("ElectionYear", 0) or 0)
            existing = best_per_person.get(pid)
            if existing is None:
                best_per_person[pid] = ent
            else:
                existing_yr = int(existing.get("ElectionYear", 0) or 0)
                if yr > existing_yr:
                    best_per_person[pid] = ent

        for row_num, (pid, ent) in enumerate(best_per_person.items(), start=2):
            full_name = clean(ent.get("CandidateFullName", ""))
            last, first = _split_candidate_name(full_name)
            office    = clean(ent.get("OfficeSoughtFullName", ""))
            district  = clean(ent.get("District", "") or "")
            party     = clean(ent.get("PartyAffiliation", ""))
            elec_year = clean(ent.get("ElectionYear", ""))
            incumbent = clean(ent.get("Incumbent", "") or "")
            # TerminationDate null → active
            terminated = clean(ent.get("TerminationDate", "") or "")
            # (candidates file doesn't have active/status — kept for committees)

            cand_w.writerow({
                "state":           STATE,
                "candidate_name":  full_name,
                "candidate_first": first,
                "candidate_last":  last,
                "office":          office,
                "district":        district,
                "jurisdiction":    "",
                "party":           party,
                "election_year":   elec_year,
                "incumbent":       "1" if incumbent == "1" else ("0" if incumbent == "0" else ""),
                "state_filer_id":  pid,
                "raw_file":        "candidates_entity.json",
                "row_num":         row_num,
            })
            candidates_written += 1

        log.info(f"  candidates written: {candidates_written:,}")

        # ── Committees ────────────────────────────────────────────────
        # Write one row per RegisteredEntityID across all three entity types
        all_entity_caches = [
            ("candidates_entity.json",    pcc_json),
            ("committees_entity.json",    pcf_json),
            ("party_units_entity.json",   ptu_json),
        ]
        cmte_row_num = 2
        for raw_file, cache in all_entity_caches:
            for reg_num, ent in cache.items():
                if not ent:
                    continue
                # PCC uses CommitteeName; PCF/PTU use FormattedName
                cmte_name  = (clean(ent.get("CommitteeName", ""))
                              or clean(ent.get("FormattedName", "")))
                raw_type   = clean(ent.get("RegisteredEntityType", ""))
                # Map to canonical display values so per-state DB queries work correctly
                # (aliases are only applied in the aggregate DB, not at tabulate time)
                cmte_type  = {
                    "PCC": "Candidate Committee",
                    "PCF": "PAC",
                    "PTU": "Party Committee",
                }.get(raw_type, raw_type)
                cand_full  = clean(ent.get("CandidateFullName", ""))
                terminated = clean(ent.get("TerminationDate", "") or "")
                active     = "0" if terminated else "1"
                # PCF/PTU have address fields; PCC does not
                city       = clean(ent.get("City", ""))
                zipcode    = clean(ent.get("ZipCode", ""))

                cmte_w.writerow({
                    "state":          STATE,
                    "committee_name": cmte_name,
                    "committee_type": cmte_type,
                    "election_year":  "",
                    "candidate_name": cand_full,
                    "treasurer_name": "",
                    "city":           city,
                    "zip":            zipcode,
                    "active":         active,
                    "state_filer_id": reg_num,
                    "raw_file":       raw_file,
                    "row_num":        cmte_row_num,
                })
                committees_written += 1
                cmte_row_num += 1

        log.info(f"  committees written: {committees_written:,}")

        # ── Contributions ─────────────────────────────────────────────
        cont_path = RAW_DIR / "mn_contributions.csv"
        if cont_path.exists():
            ft      = time.perf_counter()
            count   = 0
            skipped = 0
            loans   = 0
            with open(cont_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    reg_num     = clean(row.get("Recipient reg num", ""))
                    ent         = entity_registry.get(reg_num, {})
                    cmte_name   = (clean(ent.get("CommitteeName", ""))
                                   or clean(ent.get("FormattedName", ""))
                                   or clean(row.get("Recipient", "")))

                    cand_full   = clean(ent.get("CandidateFullName", ""))
                    office      = clean(ent.get("OfficeSoughtFullName", ""))
                    elec_year   = clean(row.get("Year", ""))

                    receipt_type = clean(row.get("Receipt type", ""))
                    in_kind      = clean(row.get("In kind?", "")).upper()
                    # Build transaction_type: in-kind flag overrides receipt_type display
                    if in_kind in ("Y", "YES", "TRUE", "1"):
                        tx_type = "In-Kind"
                    else:
                        tx_type = receipt_type

                    contrib_date = parse_date(row.get("Receipt date", ""))
                    zip_val      = clean(row.get("Contrib zip", ""))

                    # Loan Payable → loans_debts
                    if receipt_type == "Loan Payable":
                        loan_w.writerow({
                            "state":              STATE,
                            "committee_name":     cmte_name,
                            "original_amount":    amount,
                            "date":               contrib_date,
                            "record_type":        "Loan Payable",
                            "counterparty_name":  clean(row.get("Contributor", "")),
                            "counterparty_city":  "",
                            "counterparty_state": _zip_to_state(zip_val),
                            "counterparty_zip":   zip_val,
                            "candidate_name":     cand_full,
                            "election_year":      elec_year,
                            "amended":            "",
                            "filing_id":          "",
                            "raw_file":           cont_path.name,
                            "row_num":            row_num,
                        })
                        loans += 1
                        continue

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cmte_name,
                        "contributor_name":  clean(row.get("Contributor", "")),
                        "amount":            amount,
                        "date":              contrib_date,
                        "transaction_type":  tx_type,
                        "contributor_type":  clean(row.get("Contrib type", "")),
                        "contributor_city":  "",
                        "contributor_state": _zip_to_state(zip_val),
                        "contributor_zip":   zip_val,
                        "employer":          clean(row.get("Contrib Employer name", "")),
                        "occupation":        "",
                        "candidate_name":    cand_full,
                        "office":            office,
                        "election_year":     elec_year,
                        "amended":           "",
                        "filing_id":         "",
                        "raw_file":          cont_path.name,
                        "row_num":           row_num,
                    })
                    count += 1

            log.file_parsed(cont_path.name, "contributions", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=cont_path.stat().st_size)
            log.info(f"  loans from contributions: {loans:,}")
            total_contributions += count
            total_loans         += loans

        # ── Regular Expenditures ──────────────────────────────────────
        expn_path = RAW_DIR / "mn_expenditures.csv"
        if expn_path.exists():
            ft      = time.perf_counter()
            count   = 0
            skipped = 0
            with open(expn_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    reg_num   = clean(row.get("Committee reg num", ""))
                    ent       = entity_registry.get(reg_num, {})
                    cmte_name = (clean(ent.get("CommitteeName", ""))
                                 or clean(ent.get("FormattedName", ""))
                                 or clean(row.get("Committee name", "")))

                    cand_full = clean(ent.get("CandidateFullName", ""))
                    office    = clean(ent.get("OfficeSoughtFullName", ""))
                    elec_year = clean(row.get("Year", ""))

                    in_kind   = clean(row.get("In-kind?", "")).upper()
                    tx_type   = clean(row.get("Type", ""))
                    if in_kind in ("Y", "YES", "TRUE", "1"):
                        tx_type = "In-Kind"

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "payee_name":       clean(row.get("Vendor name", "")),
                        "amount":           amount,
                        "date":             parse_date(row.get("Date", "")),
                        "transaction_type": tx_type,
                        "purpose":          clean(row.get("Purpose", "")),
                        "category":         "",
                        "payee_city":       clean(row.get("Vendor city", "")),
                        "payee_state":      clean(row.get("Vendor state", "")),
                        "payee_zip":        clean(row.get("Vendor zip", "")),
                        "candidate_name":   cand_full,
                        "office":           office,
                        "election_year":    elec_year,
                        "amended":          "",
                        "filing_id":        "",
                        "raw_file":         expn_path.name,
                        "row_num":          row_num,
                    })
                    count += 1

            log.file_parsed(expn_path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=expn_path.stat().st_size)
            total_expenditures += count

        # ── Independent Expenditures ──────────────────────────────────
        ie_path = RAW_DIR / "mn_ind_expenditures.csv"
        if ie_path.exists():
            ft      = time.perf_counter()
            count   = 0
            skipped = 0
            with open(ie_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        skipped += 1
                        continue

                    reg_num   = clean(row.get("Spender Reg Num", ""))
                    ent       = entity_registry.get(reg_num, {})
                    cmte_name = (clean(ent.get("CommitteeName", ""))
                                 or clean(ent.get("FormattedName", ""))
                                 or clean(row.get("Spender", "")))

                    cand_full = clean(ent.get("CandidateFullName", ""))
                    office    = clean(ent.get("OfficeSoughtFullName", ""))
                    elec_year = clean(row.get("Year", ""))

                    # IE transaction_type: "Independent Expenditure For" / "...Against"
                    base_type  = clean(row.get("Type", "")) or "Independent Expenditure"
                    for_against = clean(row.get("For /Against", ""))
                    if for_against:
                        tx_type = f"{base_type} {for_against}".strip()
                    else:
                        tx_type = base_type

                    in_kind = clean(row.get("In kind?", "")).upper()
                    if in_kind in ("Y", "YES", "TRUE", "1"):
                        tx_type = "In-Kind IE"

                    # Affected committee folded into purpose
                    affected  = clean(row.get("Affected Comte Name", ""))
                    purpose   = clean(row.get("Purpose", ""))
                    if affected and purpose:
                        purpose = f"{purpose} [{affected}]"
                    elif affected:
                        purpose = f"[{affected}]"

                    # Note: IE CSV uses "Vendor State" (capital S) unlike regular expenditures
                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "payee_name":       clean(row.get("Vendor name", "")),
                        "amount":           amount,
                        "date":             parse_date(row.get("Date", "")),
                        "transaction_type": tx_type,
                        "purpose":          purpose,
                        "category":         "",
                        "payee_city":       clean(row.get("Vendor city", "")),
                        "payee_state":      clean(row.get("Vendor State", "")),  # capital S
                        "payee_zip":        clean(row.get("Vendor zip", "")),
                        "candidate_name":   cand_full,
                        "office":           office,
                        "election_year":    elec_year,
                        "amended":          "",
                        "filing_id":        "",
                        "raw_file":         ie_path.name,
                        "row_num":          row_num,
                    })
                    count += 1

            log.file_parsed(ie_path.name, "ind_expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=ie_path.stat().st_size)
            total_expenditures += count

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # MN uses CandidateMasterNameID as state_filer_id → id_model="person"
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("committees.csv.gz",     "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",     "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",    "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written,
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
