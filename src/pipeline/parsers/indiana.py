"""
parsers/indiana.py — Transform Indiana raw CSVs into the 5 normalized relations.

Sources:
  - entities.csv: one row per committee/candidate registration (org_id =
    sequential CommitteeDetail.aspx?OrgId=N sweep). Splits into committees
    (all rows) and candidates (committee_type == 'Candidate').
  - contributions_{YYYY}.csv / expenditures_{YYYY}.csv: yearly bulk exports,
    2000-2026. FileNumber == entities.org_id, used to enrich each
    transaction with committee_name / candidate_name / office from the
    registry.

Loan-related rows are routed to loans_debts:
  - contributions: Type in {Loan, Debt, Debt - Debts Owed by this Committee,
    Debt - Debts Owed to this Committee}
  - expenditures: ExpenditureCode == 'Loan Payment'
"""

import csv
import gzip
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

STATE = "IN"

RAW_DIR   = PROJECT_ROOT / "data" / "Indiana" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Indiana" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

LOAN_CONTRIBUTION_TYPES = {
    "Loan",
    "Debt",
    "Debt - Debts Owed by this Committee",
    "Debt - Debts Owed to this Committee",
}
LOAN_EXPENDITURE_CODE = "Loan Payment"

CITY_STATE_ZIP_RE = re.compile(r"^(.*?),?\s+([A-Za-z]{2})\s+(\d{3,9}(?:-\d{4})?)$")


# ============================== helpers ===============================
def clean(val) -> str:
    """Strip whitespace, convert None to empty string."""
    return (val or "").strip()


def is_numeric(val) -> bool:
    """Return True if val can be cast to float (allows negatives)."""
    try:
        float((val or "").strip())
        return True
    except ValueError:
        return False


def parse_date(val) -> str:
    """'YYYY-MM-DD HH:MM:SS' (or 'YYYY-MM-DD') -> 'YYYY-MM-DD'. Returns '' on blank."""
    val = (val or "").strip()
    if not val:
        return ""
    date_part = val.split()[0]
    try:
        datetime.strptime(date_part, "%Y-%m-%d")
        return date_part
    except ValueError:
        return val


def split_city_zip(val: str) -> tuple[str, str]:
    """Split 'CITY ST ZIP' into (city, zip). Falls back to (whole string, '')."""
    val = (val or "").strip()
    if not val:
        return "", ""
    m = CITY_STATE_ZIP_RE.match(val)
    if not m:
        return utils.clean_name(val), ""
    city, _state, zip_code = m.groups()
    return utils.clean_name(city), utils.clean_zip(zip_code)


# ~520 older "Candidate"-type entities.csv rows (all status='Disbanded',
# registered 1978-2020s) have a genuinely blank lblCandidateName on Indiana's
# own CommitteeDetail.aspx pages -- this is a source-data gap, not a scraper
# bug (verified against the live site for OrgId=587). Left empty, these rows
# fail the candidates.candidate_name fill-rate check (13.9% empty vs. 1.0%
# threshold) and, more importantly, risk false person_id merges under
# id_model="committee" (which groups by state/candidate_name/office/district
# -- many empty names would collide on office+district). The patterns below
# recover a name from the committee_name for the common templates ("FRIENDS
# OF X", "COMMITTEE TO (RE-)ELECT X", "X FOR STATE SENATE", "X COMMITTEE",
# etc.), applied iteratively to strip nested wrappers (e.g. "CITIZENS FOR
# JOHN DAY COMMITTEE" -> "JOHN DAY"). Anything left unmatched falls back to
# the (unique) committee_name itself, which is safe for person_id grouping
# even when it isn't a "real" candidate name.
_OFFICE_TAIL = (
    r"(?:STATE\s+)?(?:SENATE|SENATOR|REPRESENTATIVE|REPRESENATIVE|REP\.?|"
    r"HOUSE(?:\s+OF\s+REPRESENTATIVES)?|GOVERNOR|LIEUTENANT\s+GOVERNOR|"
    r"AUDITOR|TREASURER|SECRETARY\s+OF\s+STATE|ATTORNEY\s+GENERAL|CONGRESS|"
    r"COUNTY\s+\w+|MAYOR|CLERK|SHERIFF|COMMISSIONER|COUNCIL\b.*|JUDGE|"
    r"\d+(?:ST|ND|RD|TH)\s+DISTRICT|INDIANA|INDY|INDIANAPOLIS|"
    r"DISTRICT\s*\d*|DIST\.?\s*\d*"
    r")\b.*"
)

_CANDIDATE_NAME_PATTERNS = [
    re.compile(r"^(?:THE\s+)?(?:COMM[IT]+EE|CAMPAIGN|PEOPLE|FRIENDS|CITIZENS)\s+(?:TO|FOR)\s+(?:THE\s+)?(?:RE-?\s*)?ELECT(?:ION\s+OF)?\s+(.+)$", re.IGNORECASE),
    re.compile(r"^(?:RE-?)?ELECT\s+(.+)$", re.IGNORECASE),
    re.compile(r"^VOTE\s+(?:FOR|4)?\s*(.+)$", re.IGNORECASE),
    re.compile(r"^(?:FRIENDS?(?:\s+(?:AND|&)\s+NEIGHBORS)?\s+(?:OF|FOR)|NEIGHBORS\s+FOR|TAXPAYERS\s+FOR|HOOSIERS?\s+(?:FOR|4)|CITIZENS\s+FOR|PATRIOTS\s+FOR)\s+(.+)$", re.IGNORECASE),
    re.compile(r"^(.+?)\s+(?:4|FOR)\s+" + _OFFICE_TAIL + r"$", re.IGNORECASE),
    # trailing "to D-37" / "to District 5" / "to House District 5"
    re.compile(r"^(.+?)\s+TO\s+(?:D-?\d+|(?:HOUSE|SENATE)\s+DISTRICT.*|DISTRICT.*)$", re.IGNORECASE),
    # trailing parenthetical, e.g. "(ERCC)"
    re.compile(r"^(.+?)\s*\([^()]*\)$"),
    re.compile(r"^(?:THE\s+)?(.+?)\s+(?:CAMPAIGN\s+)?(?:EXPLORATORY\s+)?(?:ELECTION\s+)?COMMITTEE$", re.IGNORECASE),
    re.compile(r"^(?:THE\s+)?(.+?)\s+(?:CAMPAIGN\s+)?FUND$", re.IGNORECASE),
    re.compile(r"^(.+?)\s+" + _OFFICE_TAIL + r"$", re.IGNORECASE),
]


def extract_candidate_name_from_committee(committee_name: str, max_iter: int = 5) -> str:
    """Recover a candidate name from a committee name when entities.csv has
    no candidate_name on file. Falls back to the cleaned committee_name."""
    name = (committee_name or "").strip()
    for _ in range(max_iter):
        for pat in _CANDIDATE_NAME_PATTERNS:
            m = pat.match(name)
            if m:
                cand = m.group(1).strip(" -")
                if cand and cand.upper() != "THE" and cand != name:
                    name = cand
                    break
        else:
            break
    return utils.clean_name(name)


def split_name(name: str) -> tuple[str, str]:
    """Split a 'First [Middle] Last' string into (first/middle, last)."""
    parts = name.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def get_amended(row: dict) -> str:
    """Return '0'/'1' for the Amended flag, or '' if unavailable.

    Edge case: some 2024 rows have an unescaped quote inside Received_By
    (e.g. 'Raymond (Butch") L. Kramer'), which shifts the real Amended
    value into an extra list stored under the None key by csv.DictReader.
    """
    val = clean(row.get("Amended", ""))
    if val in ("0", "1"):
        return val
    extra = row.get(None)
    if isinstance(extra, list) and extra and extra[0] in ("0", "1"):
        return extra[0]
    return ""


def year_files(prefix: str) -> list[Path]:
    """Return raw/{prefix}_{YYYY}.csv files sorted by year, with their year."""
    out = []
    for path in sorted(RAW_DIR.glob(f"{prefix}_*.csv")):
        m = re.match(rf"{prefix}_(\d{{4}})\.csv$", path.name)
        if m:
            out.append((int(m.group(1)), path))
    return out


def scan_candidate_names_from_transactions(org_ids: set[str]) -> dict[str, str]:
    """Pre-scan all contribution/expenditure files for the given org_ids and
    return org_id -> most common non-empty CandidateName.

    entities.csv leaves candidate_name blank for ~520 old "Candidate"-type
    committees (a gap on the source site itself), but the yearly bulk
    transaction exports often carry a real CandidateName (e.g. "Mitchell
    Elias Daniels, Jr." for "Mitch for Governor Campaign Committee", org_id
    4960) even when the registry page does not. This recovers those real
    names (covers ~83% of the 521 gap rows) before falling back to the
    committee-name heuristic in extract_candidate_name_from_committee().
    """
    from collections import Counter
    counters: dict[str, Counter] = {}
    for path in [p for _, p in year_files("contributions")] + [p for _, p in year_files("expenditures")]:
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            r = csv.reader(f)
            header = next(r, None)
            if not header or "FileNumber" not in header or "CandidateName" not in header:
                continue
            fi = header.index("FileNumber")
            ci = header.index("CandidateName")
            max_i = max(fi, ci)
            for row in r:
                if len(row) <= max_i:
                    continue
                fn = row[fi].strip()
                if fn in org_ids:
                    cn = row[ci].strip()
                    if cn:
                        counters.setdefault(fn, Counter())[cn] += 1
    return {fn: c.most_common(1)[0][0] for fn, c in counters.items()}


# ============================== writers ===============================
def open_writer(filename: str, fieldnames: list[str]):
    """Open a gzipped CSV writer in CLEAN_DIR. Extra fields ignored, missing
    fields default to empty string."""
    path = CLEAN_DIR / filename
    fh = gzip.open(path, "wt", encoding="utf-8", newline="")
    writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    writer.writeheader()
    return fh, writer


# =============================== parse ================================
def run():
    log = get_logger("indiana", "parse")
    t0 = time.perf_counter()
    log.info("Starting Indiana parser")
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures = 0
    total_loans = 0
    n_committees = 0
    n_candidates = 0
    file_handles: list = []

    try:
        cmte_fh, cmte_w = open_writer("committees.csv.gz", C.COMMITTEES)
        cand_fh, cand_w = open_writer("candidates.csv.gz", C.CANDIDATES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz", C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz", C.LOANS_DEBTS)
        file_handles = [cmte_fh, cand_fh, cont_fh, expn_fh, loan_fh]

        # ---- entities.csv: committees + candidates + enrichment lookup ----
        entities_path = RAW_DIR / "entities.csv"
        org_lookup: dict[str, dict] = {}  # org_id -> {committee_name, candidate_name, office}
        skipped_entities = 0

        # First pass: dedupe by org_id, keeping the last occurrence (chunk-
        # boundary re-scrapes of the entity sweep produced a few duplicates).
        entity_rows: dict[str, tuple[int, dict]] = {}
        with open(entities_path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                org_id = clean(row.get("org_id", ""))
                if not org_id:
                    skipped_entities += 1
                    continue
                entity_rows[org_id] = (row_num, row)

        # Pre-scan transaction files for real candidate names where
        # entities.csv leaves candidate_name blank on a "Candidate"-type
        # committee (see scan_candidate_names_from_transactions docstring).
        missing_candidate_ids = {
            org_id for org_id, (_, row) in entity_rows.items()
            if clean(row.get("committee_type", "")) == "Candidate"
            and not utils.clean_name(row.get("candidate_name", ""))
        }
        tx_candidate_names = scan_candidate_names_from_transactions(missing_candidate_ids)
        log._emit("candidate_name_recovery", status="ok",
                  missing=len(missing_candidate_ids),
                  recovered_from_transactions=len(tx_candidate_names))

        for org_id, (row_num, row) in entity_rows.items():
                committee_name = utils.clean_name(row.get("committee_name", ""))
                candidate_name = utils.clean_name(row.get("candidate_name", ""))
                office = utils.clean_name(row.get("office", ""))
                district = clean(row.get("district", ""))
                party = clean(row.get("party", ""))
                committee_type = clean(row.get("committee_type", ""))
                status = clean(row.get("status", ""))
                city, zip_code = split_city_zip(row.get("city_state_zip", ""))

                # Source-data gap: ~520 old "Candidate"-type committees have
                # no candidate_name on file. Recover the real name from the
                # transaction CSVs where available (covers ~83%), otherwise
                # fall back to a heuristic extraction from committee_name.
                # Either way this keeps candidates.candidate_name fill rate
                # at ~100% and prevents person_id grouping (id_model=
                # "committee") from collapsing unrelated committees on an
                # empty candidate_name.
                if committee_type == "Candidate" and not candidate_name:
                    tx_name = tx_candidate_names.get(org_id)
                    candidate_name = utils.clean_name(tx_name) if tx_name else \
                        extract_candidate_name_from_committee(committee_name)

                org_lookup[org_id] = {
                    "committee_name": committee_name,
                    "candidate_name": candidate_name,
                    "office": office,
                }

                # committees: every entity row
                cmte_w.writerow({
                    "state":          STATE,
                    "committee_name": committee_name or candidate_name,
                    "committee_type": committee_type,
                    "candidate_name": candidate_name,
                    "treasurer_name": utils.clean_name(row.get("treasurer_name", "")),
                    "city":           city,
                    "zip":            zip_code,
                    "active":         1 if status == "Active" else (0 if status else ""),
                    "state_filer_id": org_id,
                    "raw_file":       "entities.csv",
                    "row_num":        row_num,
                })
                n_committees += 1

                # candidates: Candidate-type committee registrations
                if committee_type == "Candidate":
                    first, last = split_name(candidate_name)
                    cand_w.writerow({
                        "state":           STATE,
                        "candidate_name":  candidate_name,
                        "candidate_first": first,
                        "candidate_last":  last,
                        "office":          office,
                        "district":        district,
                        "jurisdiction":    "",
                        "party":           party,
                        "election_year":   "",
                        "incumbent":       "",
                        "state_filer_id":  org_id,
                        "raw_file":        "entities.csv",
                        "row_num":         row_num,
                    })
                    n_candidates += 1

        log.file_parsed("entities.csv", "committees", n_committees, skipped_entities,
                         duration_s=0.0, bytes=entities_path.stat().st_size)
        log.file_parsed("entities.csv", "candidates", n_candidates, role="source")

        # ---- contributions_{YYYY}.csv ----
        for year, path in year_files("contributions"):
            ft = time.perf_counter()
            count = loans = skipped = 0
            try:
                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    for row_num, row in enumerate(csv.DictReader(f), start=2):
                        amount = clean(row.get("Amount", ""))
                        if not is_numeric(amount):
                            skipped += 1
                            continue

                        org_id = clean(row.get("FileNumber", ""))
                        ent = org_lookup.get(org_id, {})
                        committee_name = ent.get("committee_name") or utils.clean_name(row.get("Committee", ""))
                        candidate_name = ent.get("candidate_name") or utils.clean_name(row.get("CandidateName", ""))
                        office = ent.get("office", "")
                        ctype = clean(row.get("Type", ""))

                        common = {
                            "state":          STATE,
                            "committee_name": committee_name,
                            "date":           parse_date(row.get("ContributionDate", "")),
                            "candidate_name": candidate_name,
                            "election_year":  year,
                            "amended":        get_amended(row),
                            "filing_id":      org_id,
                            "raw_file":       path.name,
                            "row_num":        row_num,
                        }

                        if ctype in LOAN_CONTRIBUTION_TYPES:
                            loan_w.writerow({
                                **common,
                                "original_amount":    amount,
                                "record_type":        "loan" if ctype == "Loan" else "debt",
                                "counterparty_name":  utils.clean_name(row.get("Name", "")),
                                "counterparty_city":  utils.clean_name(row.get("City", "")),
                                "counterparty_state": clean(row.get("State", "")),
                                "counterparty_zip":   utils.clean_zip(row.get("Zip", "")),
                            })
                            loans += 1
                        else:
                            cont_w.writerow({
                                **common,
                                "amount":            amount,
                                "transaction_type":  ctype,
                                "contributor_name":  utils.clean_name(row.get("Name", "")),
                                "contributor_type":  clean(row.get("ContributorType", "")),
                                "contributor_city":  utils.clean_name(row.get("City", "")),
                                "contributor_state": clean(row.get("State", "")),
                                "contributor_zip":   utils.clean_zip(row.get("Zip", "")),
                                "employer":          "",
                                "occupation":        clean(row.get("Occupation", "")),
                                "office":            office,
                            })
                            count += 1
                log.file_parsed(path.name, "contributions", count, skipped,
                                 duration_s=time.perf_counter() - ft,
                                 bytes=path.stat().st_size)
                if loans:
                    log._emit("file_parsed", status="ok", filename=path.name,
                              relation="loans_debts", role="source", rows=loans,
                              skipped=0, duration_s=time.perf_counter() - ft)
                total_contributions += count
                total_loans += loans
            except Exception as e:
                log.file_parse_error(path.name, str(e))

        # ---- expenditures_{YYYY}.csv ----
        for year, path in year_files("expenditures"):
            ft = time.perf_counter()
            count = loans = skipped = 0
            try:
                with open(path, newline="", encoding="utf-8", errors="replace") as f:
                    for row_num, row in enumerate(csv.DictReader(f), start=2):
                        amount = clean(row.get("Amount", ""))
                        if not is_numeric(amount):
                            skipped += 1
                            continue

                        org_id = clean(row.get("FileNumber", ""))
                        ent = org_lookup.get(org_id, {})
                        committee_name = ent.get("committee_name") or utils.clean_name(row.get("Committee", ""))
                        candidate_name = ent.get("candidate_name") or utils.clean_name(row.get("CandidateName", ""))
                        office = ent.get("office") or utils.clean_name(row.get("OfficeSought", ""))
                        code = clean(row.get("ExpenditureCode", ""))

                        common = {
                            "state":          STATE,
                            "committee_name": committee_name,
                            "date":           parse_date(row.get("Expenditure_Date", "")),
                            "candidate_name": candidate_name,
                            "election_year":  year,
                            "amended":        get_amended(row),
                            "filing_id":      org_id,
                            "raw_file":       path.name,
                            "row_num":        row_num,
                        }

                        if code == LOAN_EXPENDITURE_CODE:
                            loan_w.writerow({
                                **common,
                                "original_amount":    amount,
                                "record_type":        "loan_payment",
                                "counterparty_name":  utils.clean_name(row.get("Name", "")),
                                "counterparty_city":  utils.clean_name(row.get("City", "")),
                                "counterparty_state": clean(row.get("State", "")),
                                "counterparty_zip":   utils.clean_zip(row.get("Zip", "")),
                            })
                            loans += 1
                        else:
                            expn_w.writerow({
                                **common,
                                "amount":           amount,
                                "transaction_type": clean(row.get("ExpenditureType", "")),
                                "purpose":          clean(row.get("Purpose", "")),
                                "category":         code,
                                "payee_name":       utils.clean_name(row.get("Name", "")),
                                "payee_city":       utils.clean_name(row.get("City", "")),
                                "payee_state":      clean(row.get("State", "")),
                                "payee_zip":        utils.clean_zip(row.get("Zip", "")),
                                "office":           office,
                            })
                            count += 1
                log.file_parsed(path.name, "expenditures", count, skipped,
                                 duration_s=time.perf_counter() - ft,
                                 bytes=path.stat().st_size)
                if loans:
                    log._emit("file_parsed", status="ok", filename=path.name,
                              relation="loans_debts", role="source", rows=loans,
                              skipped=0, duration_s=time.perf_counter() - ft)
                total_expenditures += count
                total_loans += loans
            except Exception as e:
                log.file_parse_error(path.name, str(e))

        for fh in file_handles:
            fh.close()
        file_handles = []  # prevent double-close in finally

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                           CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions, role="output",
                         bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz", "expenditures", total_expenditures, role="output",
                         bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz", "loans_debts", total_loans, role="output",
                         bytes=_out_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz", "committees", n_committees, role="output",
                         bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz", "candidates", n_candidates, role="output",
                         bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=n_committees, candidates=n_candidates)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=n_committees, candidates=n_candidates)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=n_committees, candidates=n_candidates,
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
    import argparse
    argparse.ArgumentParser(
        description="Parse Indiana raw data files into 5 normalized relations."
    ).parse_args()
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
