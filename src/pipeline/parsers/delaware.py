"""
parsers/delaware.py — Parse Delaware CFRS raw data into normalized CSVs.

Raw files (all in data/Delaware/raw/):
  de_contributions_{year}.csv   — contributions by year (2000–present)
  de_expenditures_{year}.csv    — expenditures by year (2000–present)
  de_committee_details.csv      — unified committee detail file from ShowReview scrape
                                   (replaces the old de_committees_{type}.csv files)
  de_candidates_{stem}.xlsx     — election candidates from 2024+ (XLSX from elections.delaware.gov)
  de_candidates_{stem}.csv      — election candidates from older elections (shtml-parsed)

Output (data/Delaware/cleaned/):
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Notes:
  - Contribution Type is the payment method (Check, Credit Card, etc.).
    "Candidate Loan" and "Non Candidate Loan" are routed to loans_debts.
  - CF_ID (underscore) in contributions; "CF ID" (space) in expenditures — both
    are the receiving/spending committee's CFRS identifier.
  - de_committee_details.csv is keyed by cf_id; type 05 (Certification of Intention)
    rows are excluded since those filers have no transaction data.
  - XLSX candidates (2024+) carry First/Last name, BallotName, Office, Party,
    DisplayedStatus, and Withdrawal Date.
  - Older shtml-parsed CSV candidates have: office, county, party, candidate_name,
    date_filed, election_stem.
  - Candidate IDs are per-committee-registration (id_model="committee"):
    assign_person_ids groups by (state, candidate_name, office, district) and
    assigns person_id = min(state_filer_id) across all registrations.
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
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Delaware" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Delaware" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "DE"
MAX_VALID_YEAR = date.today().year + 2

# Contribution types that go to loans_debts instead of contributions
LOAN_TYPES = {"Candidate Loan", "Non Candidate Loan"}

# Federal office keywords — filter from candidate pages
_FEDERAL_KW = ("president", "u.s. senat", "in congress", "u.s. represent")

# ── Contributor name normalization ─────────────────────────────────────────
# Delaware committees report out-of-state contributions as aggregate lump sums
# under ~20 different label spellings, with inconsistent capitalization, double
# spaces, and hyphen/space variations (e.g. "CONTRIBUTIONS  NON-DELAWARE",
# "non-DE individuals  Receipts   from", "Non DE Transactions").
# Normalize them all to one canonical value so queries treat them consistently.

AGGREGATE_LABEL_DE = "[Non-Delaware Aggregate]"


def normalize_contributor_de(name: str) -> str | None:
    """Normalize a raw Delaware contributor name.

    Returns:
      AGGREGATE_LABEL_DE  — if name is a Non-Delaware aggregate label (any variant)
      None                — if name is a data artifact (caller should skip the row)
      name (unchanged)    — otherwise
    """
    stripped = (name or "").strip()
    if not stripped:
        return stripped

    # Collapse internal whitespace and uppercase for reliable matching
    upper    = re.sub(r"\s+", " ", stripped.upper())
    # Treat hyphens as spaces so NON-DE and NON DE match the same pattern
    nohyphen = upper.replace("-", " ")

    # Drop known data artifacts
    if "DATE OF REGISTRATION CASH" in upper:
        return None
    # Drop rows where the contributor field is a street address
    # (starts with digits followed by a word — e.g. "7526 BELL BLVD")
    if re.match(r"^\d+\s+[A-Z]", upper):
        return None

    # Normalize Non-Delaware aggregate labels.
    # Key substrings that reliably identify these entries after normalization:
    if "NON DE" in nohyphen or "NON DELAWARE" in nohyphen:
        return AGGREGATE_LABEL_DE

    return name


# ============================== helpers ===============================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Strip $ and commas; return plain numeric string or '' on failure."""
    v = clean(val).replace("$", "").replace(",", "")
    if not v:
        return ""
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """MM/DD/YYYY or YYYY-MM-DD → YYYY-MM-DD. Returns '' on failure."""
    v = clean(val)
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


def year_from_date(val: str) -> str:
    """Extract the 4-digit year from a date string."""
    d = parse_date(val)
    return d[:4] if d else ""


def year_from_filename(path: Path) -> str:
    """Extract the 4-digit year from a filename like de_contributions_2024.csv."""
    m = re.search(r"(\d{4})", path.stem)
    return m.group(1) if m else ""


def is_federal_office(office: str) -> bool:
    o = office.lower()
    return any(kw in o for kw in _FEDERAL_KW)


def parse_de_office(office_str: str) -> tuple[str, str]:
    """Parse the oddly-formatted Office field from contribution rows.

    Examples:
      '(Insurance Commissioner)'            → office='Insurance Commissioner', district=''
      'District 10 (State Senator)'         → office='State Senator', district='District 10'
      'District 4 (State Representative)'  → office='State Representative', district='District 4'
      ''                                    → ('', '')
    """
    v = clean(office_str)
    if not v:
        return "", ""
    # Pattern: optional "District N " prefix, then "(Office Name)"
    m = re.match(r"^(District\s+\S+)\s+\((.+)\)$", v, re.IGNORECASE)
    if m:
        return clean(m.group(2)), clean(m.group(1))
    m2 = re.match(r"^\((.+)\)$", v)
    if m2:
        return clean(m2.group(1)), ""
    return v, ""


def parse_committee_office(office_str: str) -> tuple[str, str, str]:
    """Parse the Office column from the committee CSV.

    Format: "State Office - State Representative - District 38"
    or: "County Office - Kent County Levy Court - District 2"
    Returns (office_level, office, district).
    """
    v = clean(office_str)
    if not v:
        return "", "", ""
    parts = [p.strip() for p in v.split(" - ")]
    if len(parts) >= 3:
        # Last part is often "District N"
        last = parts[-1]
        if re.match(r"^District\s+\d+", last, re.IGNORECASE):
            return parts[0], " - ".join(parts[1:-1]), last
        return parts[0], " - ".join(parts[1:]), ""
    if len(parts) == 2:
        return parts[0], parts[1], ""
    return "", v, ""


def clean_zip_de(val: str) -> str:
    """Delaware CFRS exports zips as '19938-    ' (trailing dash + spaces).
    Strip the trailing dash/whitespace before passing to utils.clean_zip."""
    v = re.sub(r"-\s*$", "", (val or "").strip())
    return utils.clean_zip(v)


def parse_treasurer_address(addr: str) -> tuple[str, str]:
    """Parse a double-space-delimited treasurer address into (city, zip).

    Format: "Street  City  ST  Zip" — split on 2+ spaces.
    """
    v = clean(addr)
    if not v:
        return "", ""
    parts = re.split(r"\s{2,}", v)
    # Last part is usually zip, second-to-last is state abbreviation, before that is city
    if len(parts) >= 3:
        city = parts[-3]
        zip_ = clean_zip_de(parts[-1])
        return city, zip_
    if len(parts) == 2:
        return "", clean_zip_de(parts[-1])
    return "", ""


def stem_year(stem: str) -> str:
    """Extract 4-digit year from an election stem like genl_fcddt_2024."""
    m = re.search(r"(\d{4})", stem)
    return m.group(1) if m else ""


def raw_files(pattern: str) -> list[Path]:
    """Return non-empty raw files matching glob pattern, sorted by name."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


# ============================== writers ===============================

def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ========================= committee registry =========================

DETAILS_FILE = "de_committee_details.csv"

# Trailing artifacts introduced by the first ShowReview scrape run, where
# the parser used single-word regex stops that bled across multi-word labels.
# These are stripped inline so the registry works for both the initial run
# and future properly-parsed runs (where these artifacts won't appear).
_BLEED_MAP: dict[str, list[str]] = {
    "committee_name":   [" Other", " Short"],
    "status":           [" Established"],
    "established_date": [" End"],
    "end_date":         [" Election Participation", " Contact Information",
                         " No records"],
    "physical_zip":     [" Mailing Address"],
    "party":            [" Candidate Information"],
}
_ADDRESS_HEADERS = ("Physical Address", "Residence Address",
                    "Organization Street Address")


_NAME_PREFIXES = {"MR", "MRS", "MS", "MISS", "DR", "HON", "REV", "PROF"}
_NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V",
                  "MD", "DO", "ESQ", "ESQUIRE", "PHD", "PE", "DVM", "DDS",
                  "RET", "CPA", "MBA"}


def split_candidate_name(full_name: str) -> tuple[str, str]:
    """Split 'FIRST [MIDDLE] LAST [SUFFIX]' into (first, last).

    Strips leading honorifics (MR., DR., etc.) and trailing credentials
    (JR, ESQUIRE, MD, etc.), then treats the last remaining token as the
    last name and everything before it as the first/middle name.
    """
    parts = full_name.split()
    if not parts:
        return "", ""
    # Strip leading honorifics
    while parts and parts[0].upper().rstrip(".") in _NAME_PREFIXES:
        parts.pop(0)
    if not parts:
        return "", full_name
    # Strip trailing suffixes
    while len(parts) > 1 and parts[-1].upper().rstrip(".") in _NAME_SUFFIXES:
        parts.pop()
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[:-1]), parts[-1]


def _strip_bleed(val: str, markers: list[str]) -> str:
    v = val.strip()
    for m in markers:
        idx = v.find(m)
        if idx != -1:
            v = v[:idx]
    return v.strip()


def load_details_registry() -> dict[str, dict]:
    """Load de_committee_details.csv, keyed by cf_id.

    Excludes type 05 (Certification of Intention) — those filers have no
    transaction data.  Normalizes field-bleed artifacts from the initial
    scrape run so the registry is usable before a clean re-scrape.
    """
    path = RAW_DIR / DETAILS_FILE
    registry: dict[str, dict] = {}
    if not path.exists() or path.stat().st_size == 0:
        return registry
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row_num, row in enumerate(csv.DictReader(f), start=2):
            cf_id = clean(row.get("cf_id", ""))
            if not cf_id or row.get("ctype_code") == "05":
                continue
            row["_row_num"] = row_num
            # Strip known first-run field-bleed artifacts
            for field, markers in _BLEED_MAP.items():
                row[field] = _strip_bleed(row.get(field, ""), markers)
            # web_address captured section headers as values — clear those
            wa = row.get("web_address", "")
            if any(wa.startswith(h) for h in _ADDRESS_HEADERS):
                row["web_address"] = ""
            # Normalize district: strip leading "District " prefix
            d = row.get("district", "")
            row["district"] = d[9:].strip() if d.upper().startswith("DISTRICT ") else d
            registry[cf_id] = row
    return registry


# ================================ run =================================

def run():
    log = get_logger("delaware", "parse")
    t0  = time.perf_counter()
    log.info("Starting Delaware parser")
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles: list  = []

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, cmte_fh, cand_fh, loan_fh]

        # ── Committee registry ────────────────────────────────────────
        log.info("  Loading committee registry...")
        registry = load_details_registry()
        details_path = RAW_DIR / DETAILS_FILE
        log.registry_loaded(DETAILS_FILE, entries=len(registry),
                            relation="committees",
                            bytes=details_path.stat().st_size
                                  if details_path.exists() else 0)

        # Flush committees
        for cf_id, row in registry.items():
            status = clean(row.get("status", ""))
            # physical_zip may still have trailing words if not from a re-scrape
            zip_raw = clean(row.get("physical_zip", ""))
            zip_ = zip_raw.split()[0] if zip_raw else ""
            cmte_w.writerow({
                "state":          STATE,
                "state_filer_id": cf_id,
                "committee_name": utils.clean_name(row.get("committee_name", "")),
                "committee_type": clean(row.get("ctype_label", "")),
                "candidate_name": utils.clean_name(row.get("candidate_name", "")),
                "office":         utils.clean_name(row.get("office_sought", "")),
                "district":       utils.clean_name(row.get("district", "")),
                "treasurer_name": utils.clean_name(row.get("treasurer_name", "")),
                "city":           utils.clean_name(row.get("physical_city", "")),
                "zip":            zip_,
                "active":         1 if status == "Active" else (0 if status in ("Closed", "Inactive") else ""),
                "raw_file":       DETAILS_FILE,
                "row_num":        row.get("_row_num", ""),
            })
            committees_written += 1
        log.info(f"  Committees written: {committees_written:,}")

        # ── Candidates from committee details (type 01 only) ───────────
        # Candidate Committees are the source of candidate data now that
        # the XLSX/shtml scrape from elections.delaware.gov is retired.
        # candidate_name is populated by the fixed scraper; rows where it
        # is empty (e.g. the initial scrape run) are skipped here.
        for cf_id, row in registry.items():
            if row.get("ctype_code") != "01":
                continue
            candidate_name = utils.clean_name(row.get("candidate_name", ""))
            if not candidate_name:
                continue
            # Strip honorific prefix from candidate_name so registrations like
            # "MR. JOSEPH BIDEN" and "JOSEPH BIDEN" group to the same person_id.
            c_first, c_last = split_candidate_name(candidate_name)
            if c_last:
                candidate_name = f"{c_first} {c_last}".strip()
            cand_w.writerow({
                "state":           STATE,
                "state_filer_id":  cf_id,
                "candidate_name":  candidate_name,
                "candidate_first": c_first,
                "candidate_last":  c_last,
                "office":          utils.clean_name(row.get("office_sought", "")),
                "district":        utils.clean_name(row.get("district", "")),
                "party":           utils.clean_name(row.get("party", "")),
                "election_year":   "",
                "raw_file":        DETAILS_FILE,
                "row_num":         row.get("_row_num", ""),
            })
            candidates_written += 1
        if candidates_written == 0:
            log.info("  Candidates: 0 from details (candidate_name empty — re-scrape entities to populate)")

        # ── Contributions ─────────────────────────────────────────────
        for path in raw_files("de_contributions_*.csv"):
            ft = time.perf_counter()
            count = loans = skipped = 0
            try:
                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    for row_num, row in enumerate(csv.DictReader(f), start=2):
                        amount_raw = parse_amount(row.get("Contribution Amount", ""))
                        if not amount_raw:
                            skipped += 1
                            continue

                        cf_id    = clean(row.get("CF_ID", ""))
                        cont_type = clean(row.get("Contribution Type", ""))
                        date_val  = parse_date(row.get("Contribution Date", ""))
                        cmte_name = utils.clean_name(row.get("Receiving Committee", ""))

                        raw_contributor = clean(row.get("Contributor Name", ""))
                        contributor_name = normalize_contributor_de(raw_contributor)
                        if contributor_name is None:
                            skipped += 1
                            continue

                        # Route loans separately
                        if cont_type in LOAN_TYPES:
                            loan_w.writerow({
                                "state":              STATE,
                                "committee_name":     cmte_name,
                                "record_type":        "loan",
                                "counterparty_name":  utils.clean_name(contributor_name),
                                "counterparty_city":  utils.clean_name(row.get("Contributor City", "")),
                                "counterparty_state": clean(row.get("Contributor State", "")).upper(),
                                "counterparty_zip":   clean_zip_de(row.get("Contributor Zip", "")),
                                "original_amount":    amount_raw,
                                "date":               date_val,
                                "election_year":      year_from_date(row.get("Contribution Date", "")) or year_from_filename(path),
                                "filing_id":          cf_id,
                                "raw_file":           path.name,
                                "row_num":            row_num,
                            })
                            loans += 1
                            continue

                        office_str, district = parse_de_office(row.get("Office", ""))

                        # Look up candidate name from the committee registry via CF_ID.
                        # Apply same prefix-stripping as the candidates table so joins work.
                        cmte_detail        = registry.get(cf_id, {})
                        _raw_cand          = utils.clean_name(cmte_detail.get("candidate_name", ""))
                        if _raw_cand:
                            _cf, _cl       = split_candidate_name(_raw_cand)
                            cand_name_from_reg = f"{_cf} {_cl}".strip() if _cl else _raw_cand
                        else:
                            cand_name_from_reg = ""

                        cont_w.writerow({
                            "state":             STATE,
                            "committee_name":    cmte_name,
                            "contributor_name":  utils.clean_name(contributor_name),
                            "amount":            amount_raw,
                            "date":              date_val,
                            "transaction_type":  cont_type,
                            "contributor_type":  clean(row.get("Contributor Type", "")),
                            "contributor_city":  utils.clean_name(row.get("Contributor City", "")),
                            "contributor_state": clean(row.get("Contributor State", "")).upper(),
                            "contributor_zip":   clean_zip_de(row.get("Contributor Zip", "")),
                            "employer":          clean(row.get("Employer Name", "")),
                            "occupation":        clean(row.get("Employer Occupation", "")),
                            "office":            utils.clean_name(office_str),
                            "candidate_name":    cand_name_from_reg,
                            "election_year":     year_from_date(row.get("Contribution Date", "")) or year_from_filename(path),
                            "filing_id":         cf_id,
                            "raw_file":          path.name,
                            "row_num":           row_num,
                        })
                        count += 1

                log.file_parsed(path.name, "contributions", count, skipped,
                                duration_s=time.perf_counter() - ft,
                                bytes=path.stat().st_size)
                if loans:
                    log._emit("file_parsed", status="ok", filename=path.name,
                              relation="loans_debts", role="source",
                              rows=loans, skipped=0,
                              duration_s=round(time.perf_counter() - ft, 2))
                total_contributions += count
                total_loans         += loans

            except Exception as e:
                log.file_parse_error(path.name, str(e))

        # ── Expenditures ──────────────────────────────────────────────
        for path in raw_files("de_expenditures_*.csv"):
            ft = time.perf_counter()
            count = skipped = 0
            try:
                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    for row_num, row in enumerate(csv.DictReader(f), start=2):
                        amount_raw = parse_amount(row.get("Amount($)", ""))
                        if not amount_raw:
                            skipped += 1
                            continue

                        expn_w.writerow({
                            "state":            STATE,
                            "committee_name":   utils.clean_name(row.get("Committee Name", "")),
                            "payee_name":       utils.clean_name(row.get("Payee Name", "")),
                            "amount":           amount_raw,
                            "date":             parse_date(row.get("Expenditure Date", "")),
                            "transaction_type": clean(row.get("Expense Method", "")),
                            "purpose":          clean(row.get("Expense Purpose", "")),
                            "category":         clean(row.get("Expense Category", "")),
                            "payee_city":       utils.clean_name(row.get("Payee City", "")),
                            "payee_state":      clean(row.get("Payee State", "")).upper(),
                            "payee_zip":        clean_zip_de(row.get("Payee Zip", "")),
                            "election_year":    year_from_date(row.get("Expenditure Date", "")) or year_from_filename(path),
                            "filing_id":        clean(row.get("CF ID", "")),
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

        # ── Close handles before person-ID assignment ─────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # id_model="committee": state_filer_id is the CF_ID from de_committee_details.csv,
        # which is stable per committee registration. assign_person_ids groups by
        # (state, candidate_name, office, district) and assigns person_id = min(cf_id).
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz",
                                id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written)

    except KeyboardInterrupt:
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


# ====== CLI ==================================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
