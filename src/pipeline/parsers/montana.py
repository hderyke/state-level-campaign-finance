"""
parsers/montana.py — Parse Montana CERS raw JSON into the 5 normalized relations.

Input:  data/Montana/raw/
  candidates_{year}.json   — full search-result rows for every candidate active
                             in {year} (from scrapers/montana.py's yearly sweep)
  committees_{year}.json   — same, for committees with reported financial activity
  candidate_{id}.json      — one candidate's full bundle: registry metadata +
                             every filed report's itemized transactions
  committee_{id}.json      — same, for one committee

Output: data/Montana/cleaned/
  contributions.csv.gz, expenditures.csv.gz, candidates.csv.gz,
  committees.csv.gz, loans_debts.csv.gz (empty — CERS has no separate
  loans/debts schedule; candidate/committee loans appear as a contribution
  sub-type instead, see Data Notes below)

Two raw shapes feed contributions/expenditures, depending on report type:

  C5 (candidate periodic) / C6 (committee periodic) / C4 (committee
  independent-expenditure-style periodic) reports carry a bulk pipe-delimited
  schedule export, already split into list-of-dict rows by the scraper with
  the server's own column headers preserved verbatim (Date Paid, Entity Name,
  City, State, Zip, Contribution Type, Amount, Amount Type, Election Type,
  Previous Transaction (Y/N), etc. for contributions; Date Paid, Entity Name,
  Expenditure Type, Amount, Purpose, etc. for expenditures).

  C7 (last-minute contribution notice) / C7E (last-minute expenditure notice)
  reports have no bulk export — the scraper instead saved the server's native
  JSON line items per sub-table (individual/committee/loan donors for C7;
  expendOther for C7E), with fields like entityName, entityAddress (a single
  "street, city, ST zip" string), datePaid (epoch milliseconds), cashAmt,
  inKindAmt, totalAmt, occupationDescr, employerDescr, amountTypeDescr
  (actually the election phase — Primary/General — not a cash/in-kind flag).

This parser normalizes both shapes into the same canonical columns.

Notes
─────
  • "Contribution Type" in the C5/C6/C4 schedule is a numeric code (1-9)
    describing the *source* of the money (individual, PAC, party, loan,
    fundraiser, etc.) — mapped to `contributor_type`, left as the raw code
    string here and canonicalized centrally via src/aliases/contributor_types.csv.
  • "Amount Type" (CA/IK/Mixed cash-vs-in-kind flag) maps to `transaction_type`.
    C7 rows carry cashAmt/inKindAmt directly instead of a precomputed flag —
    this parser derives the same CA/IK/Mixed value from those two fields.
  • CERS candidates and committees are distinct filer types with no linkage
    exposed by the search/report APIs used here — a candidate's own campaign
    account has no separate "committee name," and committees carry no
    candidate_name back-reference. `committee_name` on candidate-sourced rows
    is therefore just the candidate's own name; `committees.csv` rows all have
    a blank candidate_name, so assign_committee_person_ids() will not match
    any of them to a candidate (expected — see docs/states/montana.md).
  • "Loans" (Contribution Type code 3) surface as itemized contribution rows,
    not a separate loans/debts schedule — CERS has no such schedule, hence
    loans_debts.csv.gz is written empty, same treatment as Arkansas/Kansas.
  • person_id model: "committee" — a candidate's electionYear is embedded in
    their own CERS record (like Alabama/Arizona, a person appears to
    re-register with a new candidateId each cycle rather than keeping one
    stable ID across cycles). This is an assumption, not yet confirmed
    against a live multi-cycle dataset — see docs/states/montana.md.
  • row_num: candidates.csv/committees.csv use the 1-based position of the
    entity within its candidates_{year}.json/committees_{year}.json array
    (no header row to offset for, unlike a CSV). contributions.csv/
    expenditures.csv use a running 1-based counter over every itemized row
    written from that entity's raw_file (which bundles every report the
    entity filed, not just one) — still a deterministic, traceable position,
    just not a literal line number in a flat file.
  • Raw pipe-delimited schedule column headers were sourced from a verified
    third-party implementation of this same API (see scrapers/montana.py's
    docstring), not a live sample — if a real scrape turns up different or
    additional header names, the `.get(...)` lookups below should be updated
    to match; they're written defensively (missing keys just yield '').
"""

import csv
import glob
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

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Montana" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Montana" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "MT"
MAX_VALID_YEAR = date.today().year + 2

# Periodic reports with a bulk pipe-delimited schedule (see scrapers/montana.py)
PERIODIC_TYPES = {"C5", "C6", "C4"}

# ============================== helpers ===============================


def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val) -> str:
    """'$1,000.00' / 1000.0 / '(500.00)' → plain numeric string, '' on failure."""
    if val is None:
        return ""
    v = str(val).strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    neg = v.startswith("(") and v.endswith(")")
    if neg:
        v = v[1:-1]
    try:
        f = float(v)
        return str(-f) if neg else str(f)
    except ValueError:
        return ""


def parse_date_mdy(val: str) -> str:
    """'MM/DD/YYYY' → 'YYYY-MM-DD', '' on failure or implausible year."""
    v = (val or "").strip()
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


def parse_epoch_ms(val) -> str:
    """CERS's C7/C7E JSON gives dates as epoch milliseconds → 'YYYY-MM-DD'."""
    if val is None or val == "":
        return ""
    try:
        d = datetime.fromtimestamp(int(val) / 1000)
        if d.year < 1990 or d.year > MAX_VALID_YEAR:
            return ""
        return d.strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return ""


# Matches "...street..., City, ST 12345" or "..., ST 12345-6789"
_ADDR_RE = re.compile(r"^(.*),\s*([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$")


def parse_combined_address(addr: str) -> tuple[str, str, str]:
    """C7/C7E's entityAddress is a single 'street, city, ST zip' string."""
    addr = (addr or "").strip()
    if not addr:
        return "", "", ""
    m = _ADDR_RE.match(addr)
    if not m:
        return "", "", ""
    street_city = m.group(1)
    st          = m.group(2).upper()
    zipcode     = m.group(3)
    last_comma  = street_city.rfind(",")
    city        = street_city[last_comma + 1:].strip() if last_comma >= 0 else street_city.strip()
    return city, st, zipcode


def split_candidate_name(name: str) -> tuple[str, str]:
    """'Smith, Jane A' → ('Jane', 'Smith'). CERS candidateName is 'Last, First ...'."""
    name = clean(name)
    if "," not in name:
        return "", ""
    last, rest = name.split(",", 1)
    parts = rest.strip().split()
    first = parts[0] if parts else ""
    return first, last.strip()


def yn_to_amended(val) -> str:
    v = clean(val).upper()
    if v == "Y":
        return "1"
    if v == "N":
        return "0"
    return ""


def cash_inkind_flag(cash, inkind) -> str:
    """Mirror the CA/IK/Mixed flag the C5/C6/C4 pipe export gives directly,
    for C7 rows which instead carry separate cashAmt/inKindAmt fields."""
    try:
        cash_f   = float(cash or 0)
    except (TypeError, ValueError):
        cash_f = 0.0
    try:
        inkind_f = float(inkind or 0)
    except (TypeError, ValueError):
        inkind_f = 0.0
    if cash_f > 0 and inkind_f > 0:
        return "Mixed"
    if inkind_f > 0:
        return "IK"
    if cash_f > 0:
        return "CA"
    return ""


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def load_json(path: Path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


# ================================ run =================================


def run():
    log = get_logger("montana", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    committees_written   = 0
    candidates_written   = 0
    file_handles          = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        # ── Candidate + committee registries (from the yearly search lists) ──
        # These are the authoritative rosters — every candidate/committee CERS
        # returned for that year gets a row, even if its full-report fetch
        # later failed and no candidate_{id}.json/committee_{id}.json exists.
        cand_meta: dict[str, dict] = {}   # candidateId (str) -> entity metadata
        for path in sorted(RAW_DIR.glob("candidates_*.json")):
            data = load_json(path)
            if not data:
                continue
            ft = time.perf_counter()
            count = 0
            for row_num, row in enumerate(data, start=1):
                cid = clean(str(row.get("candidateId", "")))
                if not cid:
                    continue
                name = clean(row.get("candidateName", ""))
                first, last = split_candidate_name(name)
                office  = clean(row.get("officeTitle", ""))
                elec_yr = clean(str(row.get("electionYear", "")))
                party   = clean(row.get("partyDescr", ""))
                jur     = clean(row.get("candidateTypeDescr", ""))
                status  = clean(row.get("candidateStatusDescr", ""))

                entry = {
                    "candidate_name": name, "candidate_first": first,
                    "candidate_last": last, "office": office,
                    "jurisdiction": jur, "party": party,
                    "election_year": elec_yr,
                }
                # Later years may re-list the same candidateId (e.g. amendments
                # filed the following calendar year) — last one wins, harmless
                # since the underlying registration data is stable per ID.
                cand_meta[cid] = entry

                cand_w.writerow({
                    "state": STATE, "state_filer_id": cid,
                    "candidate_name": name, "candidate_first": first,
                    "candidate_last": last, "office": office,
                    "district": "", "jurisdiction": jur, "party": party,
                    "election_year": elec_yr, "incumbent": "",
                    "raw_file": path.name, "row_num": row_num,
                })
                count += 1
            candidates_written += count
            log.file_parsed(path.name, "candidates", count,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        cmte_meta: dict[str, dict] = {}
        for path in sorted(RAW_DIR.glob("committees_*.json")):
            data = load_json(path)
            if not data:
                continue
            ft = time.perf_counter()
            count = 0
            for row_num, row in enumerate(data, start=1):
                mid = clean(str(row.get("committeeId", "")))
                if not mid:
                    continue
                name    = clean(row.get("committeeName", ""))
                ctype   = clean(row.get("committeeTypeDescr", ""))
                elec_yr = clean(str(row.get("electionYear", "")))
                status  = clean(row.get("committeeStatusDescr", ""))
                active  = "1" if status == "Active" else ("0" if status else "")

                cmte_meta[mid] = {"committee_name": name, "committee_type": ctype,
                                  "election_year": elec_yr}

                cmte_w.writerow({
                    "state": STATE, "state_filer_id": mid,
                    "committee_name": name, "committee_type": ctype,
                    "election_year": elec_yr, "candidate_name": "",
                    "treasurer_name": "", "city": "", "zip": "",
                    "active": active, "raw_file": path.name, "row_num": row_num,
                })
                count += 1
            committees_written += count
            log.file_parsed(path.name, "committees", count,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        log.registry_loaded("candidates_*.json", len(cand_meta), relation="candidates")
        log.registry_loaded("committees_*.json", len(cmte_meta), relation="committees")

        # ── Itemized transactions (from per-entity full-report bundles) ──

        def process_entity_file(path: Path, entity_type: str):
            nonlocal total_contributions, total_expenditures
            data = load_json(path)
            if not data:
                return
            entity_id = clean(str(data.get(f"{entity_type}Id", "")))
            if entity_type == "candidate":
                entity_name  = clean(data.get("candidateName", ""))
                committee_nm = entity_name  # no separate committee entity for candidates
                office       = clean(data.get("officeTitle", ""))
            else:
                entity_name  = clean(data.get("committeeName", ""))
                committee_nm = entity_name
                office       = ""
            elec_yr = clean(str(data.get("electionYear", "")))

            cont_row_num = 0
            expn_row_num = 0
            ft = time.perf_counter()

            for report in data.get("reports", []):
                form_type = report.get("formTypeCode", "")
                report_id = clean(str(report.get("reportId", "")))

                if form_type in PERIODIC_TYPES:
                    for row in report.get("contributions", []):
                        cont_row_num += 1
                        entity_nm_row = clean(row.get("Entity Name", "")) or \
                            ", ".join(p for p in [clean(row.get("Last Name", "")),
                                                  clean(row.get("First Name", ""))] if p)
                        cont_w.writerow({
                            "state": STATE, "committee_name": committee_nm,
                            "amount": parse_amount(row.get("Amount")),
                            "date": parse_date_mdy(row.get("Date Paid", "")),
                            "transaction_type": clean(row.get("Amount Type", "")),
                            "contributor_name": entity_nm_row,
                            "contributor_type": clean(row.get("Contribution Type", "")),
                            "contributor_city": clean(row.get("City", "")),
                            "contributor_state": clean(row.get("State", "")),
                            "contributor_zip": clean(row.get("Zip", "")),
                            "employer": clean(row.get("Employer", "")),
                            "occupation": clean(row.get("Occupation", "")),
                            "candidate_name": entity_name if entity_type == "candidate" else "",
                            "office": office, "election_year": elec_yr,
                            "amended": yn_to_amended(row.get("Previous Transaction (Y/N)", "")),
                            "filing_id": report_id,
                            "raw_file": path.name, "row_num": cont_row_num,
                        })
                        total_contributions += 1

                    for row in report.get("expenditures", []):
                        expn_row_num += 1
                        expn_w.writerow({
                            "state": STATE, "committee_name": committee_nm,
                            "amount": parse_amount(row.get("Amount")),
                            "date": parse_date_mdy(row.get("Date Paid", "")),
                            "transaction_type": clean(row.get("Expenditure Type", "")),
                            "payee_name": clean(row.get("Entity Name", "")),
                            "purpose": clean(row.get("Purpose", "")),
                            "category": "",
                            "payee_city": clean(row.get("City", "")),
                            "payee_state": clean(row.get("State", "")),
                            "payee_zip": clean(row.get("Zip", "")),
                            "candidate_name": entity_name if entity_type == "candidate" else "",
                            "office": office, "election_year": elec_yr,
                            "amended": "",
                            "filing_id": report_id,
                            "raw_file": path.name, "row_num": expn_row_num,
                        })
                        total_expenditures += 1

                elif form_type == "C7":
                    detail = report.get("contributions_c7", {}) or {}
                    for list_name in ("individual", "committee", "loan"):
                        contributor_type_label = list_name.capitalize()
                        for row in detail.get(list_name, []):
                            cont_row_num += 1
                            city, st, zipc = parse_combined_address(row.get("entityAddress", ""))
                            cont_w.writerow({
                                "state": STATE, "committee_name": committee_nm,
                                "amount": parse_amount(row.get("totalAmt")),
                                "date": parse_epoch_ms(row.get("datePaid")),
                                "transaction_type": cash_inkind_flag(
                                    row.get("cashAmt"), row.get("inKindAmt")),
                                "contributor_name": clean(row.get("entityName", "")),
                                "contributor_type": contributor_type_label,
                                "contributor_city": city, "contributor_state": st,
                                "contributor_zip": zipc,
                                "employer": clean(row.get("employerDescr", "")),
                                "occupation": clean(row.get("occupationDescr", "")),
                                "candidate_name": entity_name if entity_type == "candidate" else "",
                                "office": office, "election_year": elec_yr,
                                "amended": yn_to_amended(row.get("previousTransactionInd", "")),
                                "filing_id": report_id,
                                "raw_file": path.name, "row_num": cont_row_num,
                            })
                            total_contributions += 1

                elif form_type == "C7E":
                    detail = report.get("expenditures_c7e", {}) or {}
                    for row in detail.get("expendOther", []):
                        expn_row_num += 1
                        city, st, zipc = parse_combined_address(row.get("entityAddress", ""))
                        expn_w.writerow({
                            "state": STATE, "committee_name": committee_nm,
                            "amount": parse_amount(row.get("totalAmt")),
                            "date": parse_epoch_ms(row.get("datePaid")),
                            "transaction_type": clean(row.get("lineItemCompositeDescr", "")),
                            "payee_name": clean(row.get("entityName", "")),
                            "purpose": clean(row.get("purposeDescr", "")),
                            "category": "",
                            "payee_city": city, "payee_state": st, "payee_zip": zipc,
                            "candidate_name": entity_name if entity_type == "candidate" else "",
                            "office": office, "election_year": elec_yr,
                            "amended": "",
                            "filing_id": report_id,
                            "raw_file": path.name, "row_num": expn_row_num,
                        })
                        total_expenditures += 1
                # Unrecognized form types are skipped — scraper already logs a
                # warning for these at scrape time.

            log.file_parsed(path.name, "transactions", cont_row_num + expn_row_num,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        for path in sorted(RAW_DIR.glob("candidate_*.json")):
            try:
                process_entity_file(path, "candidate")
            except Exception as e:
                log.file_parse_error(filename=path.name, error=str(e))

        for path in sorted(RAW_DIR.glob("committee_*.json")):
            try:
                process_entity_file(path, "committee")
            except Exception as e:
                log.file_parse_error(filename=path.name, error=str(e))

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # "committee" id_model — see module docstring for why this is an
        # assumption rather than a confirmed fact about candidateId stability.
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
