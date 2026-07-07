"""
parsers/kentucky.py — Parse Kentucky KREF campaign finance CSVs into the
canonical cleaned schema.

Input files (data/Kentucky/raw/):
    candidates_{party}.csv    — one per party (6 files)
    organizations.csv         — PACs, party committees, campaign committees
    contributions_{year}.csv  — one per calendar year
    expenditures_{year}.csv   — one per calendar year

Output (data/Kentucky/cleaned/):
    candidates.csv.gz, committees.csv.gz,
    contributions.csv.gz, expenditures.csv.gz

id_model = "name_hash"
    KREF exports contain no numeric filer IDs. person_id is derived from
    MD5(state_abbr + "|" + normalized_name). "kentucky" must be in
    NAME_HASH_STATES in src/pipeline/validate.py.

Party: available — captured from per-party candidate exports.

Column mapping:

  Candidates (ExportSearch):
    Last Name, First Name           → candidate_last, candidate_first, candidate_name
    Office Sought                   → office
    Location                        → district / jurisdiction
    Election Date                   → election_year (extract year)
    Election Type                   → (ignored — PRIMARY/GENERAL not in schema)
    Is Active                       → active (True/False → 1/0)
    [file party slug]               → party

  Organizations (ExportOrganizationSearch):
    Organization Name               → committee_name
    Organization Type               → committee_type
    Candidate First/Last Name       → candidate_name (linked)
    Office Sought, Location         → office, district
    Election Date                   → election_year
    Is Active                       → active
    Treasurer Name                  → treasurer_name
    Treasurer City                  → city

  Contributions (ExportContributors):
    To Organization                 → committee_name  (org-targeted)
    Recipient Last/First Name       → committee_name  (candidate-targeted)
    From Organization Name          → contributor_name  (org donor)
    Contributor Last/First Name     → contributor_name  (individual donor)
    Amount                          → amount
    Receipt Date                    → date
    Contribution Type               → contributor_type
    Contribution Mode               → transaction_type
    Occupation                      → occupation
    Employer                        → employer
    City, State, Zip                → contributor_city, contributor_state, contributor_zip
    Office Sought                   → office
    Election Date                   → election_year (year)
    Statement Type                  → (informational, not in schema)

  Expenditures (Export):
    From Candidate First/Last Name  → committee_name  (candidate spender)
    From Organization Name          → committee_name  (org spender)
    Recipient Last/First Name +
      Organization Name             → payee_name
    Disbursement Amount             → amount
    Disbursement Date               → date
    Purpose                         → purpose
    Disbursement Code               → transaction_type
    Office Sought                   → office
    Election Date                   → election_year
    Is Independent Expenditure      → category ("Independent Expenditure" if Yes)
"""

import csv
import gzip
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Kentucky" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Kentucky" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "KY"
EARLIEST_YEAR  = 1996
MAX_VALID_YEAR = date.today().year + 4

PARTY_SLUGS = [
    ("republican",    "Republican"),
    ("democratic",    "Democratic"),
    ("libertarian",   "Libertarian"),
    ("independent",   "Independent"),
    ("other",         "Other"),
    ("notapplicable", ""),
]

# ========================= helpers ===================================

def _clean(val: str) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


# State values that mean "not applicable / unknown" in KY source data
_INVALID_STATE = {"N/A", "NA", "n/a", "na", ""}

# KY KREF expenditure payee labels that are summary/accounting rows, not real vendors
_EXPEND_SKIP_PAYEES = {
    "TOTAL",
    "TOTAL DISBURSEMENTS",
    "IN-KIND GIVEN TOTAL",
    "BALANCE TRANSFER TO GENERAL",
}

def _clean_state(val: str) -> str:
    """Return a 2-letter US state abbreviation or empty string.
    Rejects placeholder values ('N/A') and numeric codes."""
    s = (val or "").strip()
    if s in _INVALID_STATE:
        return ""
    if s.isdigit():          # numeric FIPS / data-entry errors
        return ""
    return s


def _parse_date(val: str) -> str:
    """M/D/YYYY or YYYY-MM-DD → YYYY-MM-DD. Returns '' on failure."""
    v = (val or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt).date()
            if EARLIEST_YEAR - 1 <= d.year <= MAX_VALID_YEAR:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_year(val: str) -> str:
    """Extract 4-digit year from a date string. Returns '' on failure."""
    d = _parse_date(val)
    return d[:4] if d else ""


def _parse_amount(val: str) -> str:
    v = (val or "").strip().replace("$", "").replace(",", "")
    try:
        return str(float(v))
    except ValueError:
        return ""


def _join_name(first: str, last: str) -> str:
    first, last = _clean(first), _clean(last)
    if first and last:
        return f"{first} {last}"
    return first or last


def _active(val: str) -> str:
    v = (val or "").strip().lower()
    if v in ("active", "true", "yes", "1"):
        return "1"
    if v in ("inactive", "false", "no", "0"):
        return "0"
    return ""


def _location_to_district(location: str) -> str:
    """Extract a district number from a location string like '64TH DISTRICT'."""
    m = re.search(r"(\d+)(?:ST|ND|RD|TH)?\s+DISTRICT", location.upper())
    if m:
        return m.group(1)
    return _clean(location)


# ========================= candidates ================================

def parse_candidates() -> list[dict]:
    """
    Read all per-party candidate CSVs and merge into a single list.
    Each row gets a 'party' field from the file it came from.
    Rows with the same (last, first, office, location, election_date) appearing
    in multiple party files are deduplicated keeping the first occurrence (i.e.
    the first party file in PARTY_SLUGS order wins).
    """
    seen: set[tuple] = set()
    rows: list[dict] = []

    for slug, party_label in PARTY_SLUGS:
        path = RAW_DIR / f"candidates_{slug}.csv"
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                last  = _clean(row.get("Last Name", ""))
                first = _clean(row.get("First Name", ""))
                office   = _clean(row.get("Office Sought", ""))
                location = _clean(row.get("Location", ""))
                elec_date = _clean(row.get("Election Date", ""))
                key = (last.upper(), first.upper(), office.upper(),
                       location.upper(), elec_date)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "last":        last,
                    "first":       first,
                    "office":      office,
                    "location":    location,
                    "election_date": elec_date,
                    "election_type": _clean(row.get("Election Type", "")),
                    "active":      _active(row.get("Is Active", "")),
                    "party":       party_label,
                })

    return rows


# ========================= organizations =============================

def parse_organizations() -> list[dict]:
    path = RAW_DIR / "organizations.csv"
    if not path.exists():
        return []
    rows = []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            rows.append({
                "committee_name": _clean(row.get("Organization Name", "")),
                "committee_type": _clean(row.get("Organization Type", "")),
                "candidate_first": _clean(row.get("Candidate First Name", "")),
                "candidate_last":  _clean(row.get("Candidate Last Name", "")),
                "office":         _clean(row.get("Office Sought", "")),
                "location":       _clean(row.get("Location", "")),
                "election_date":  _clean(row.get("Election Date", "")),
                "election_type":  _clean(row.get("Election Type", "")),
                "treasurer_name": _clean(row.get("Treasurer Name", "")),
                "city":           _clean(row.get("Treasurer City", "")),
                "active":         _active(row.get("Is Active", "")),
            })
    return rows


# ========================== run ======================================

def run():
    log = get_logger("kentucky", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")
    try:
        _run(log, t0)
    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


def _run(log, t0: float):

    # ── 1. Candidates ──────────────────────────────────────────────────
    log.info("  Parsing candidates…")
    cand_raw = parse_candidates()
    log.info(f"    {len(cand_raw):,} candidate rows (after party dedup)")

    cand_path = CLEAN_DIR / "candidates.csv.gz"
    cand_rows_out: list[dict] = []

    for ri, row in enumerate(cand_raw, start=1):
        full_name = _join_name(row["first"], row["last"])
        if not full_name:
            continue
        election_year = _parse_year(row["election_date"])
        location      = row["location"]
        district      = _location_to_district(location)

        cand_rows_out.append({
            "state":           STATE,
            "person_id":       "",          # filled by assign_person_ids
            "candidate_name":  utils.clean_name(full_name),
            "candidate_first": utils.clean_name(row["first"]),
            "candidate_last":  utils.clean_name(row["last"]),
            "office":          row["office"],
            "district":        district,
            "jurisdiction":    location,
            "party":           row["party"],
            "election_year":   election_year,
            "incumbent":       "",
            "state_filer_id":  "",
            "raw_file":        f"candidates_{row.get('party','').lower() or 'notapplicable'}.csv",
            "row_num":         ri,
        })

    with gzip.open(cand_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.CANDIDATES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(cand_rows_out)

    n_cands = utils.assign_person_ids(cand_path, id_model="name_hash")
    log.info(f"    → {n_cands:,} candidates written")

    # ── 2. Committees (organizations + campaign committees from orgs) ──
    log.info("  Parsing committees…")
    org_raw   = parse_organizations()
    comm_path = CLEAN_DIR / "committees.csv.gz"
    comm_rows: list[dict] = []

    for ri, org in enumerate(org_raw, start=1):
        election_year = _parse_year(org["election_date"])
        cand_name     = _join_name(org["candidate_first"], org["candidate_last"])
        district      = _location_to_district(org["location"])

        comm_rows.append({
            "state":          STATE,
            "person_id":      "",       # filled by assign_committee_person_ids
            "committee_name": org["committee_name"],
            "committee_type": org["committee_type"],
            "election_year":  election_year,
            "candidate_name": utils.clean_name(cand_name) if cand_name else "",
            "treasurer_name": org["treasurer_name"],
            "city":           org["city"],
            "zip":            "",
            "active":         org["active"],
            "state_filer_id": "",
            "raw_file":       "organizations.csv",
            "row_num":        ri,
        })

    with gzip.open(comm_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.COMMITTEES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(comm_rows)

    n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
    log.info(f"    → {len(comm_rows):,} committees written ({n_comm_matched:,} matched to candidates)")

    # ── 3. Contributions ───────────────────────────────────────────────
    log.info("  Parsing contributions…")
    contrib_path = CLEAN_DIR / "contributions.csv.gz"
    contrib_count = 0

    with gzip.open(contrib_path, "wt", newline="", encoding="utf-8") as out_f:
        w = csv.DictWriter(out_f, fieldnames=C.CONTRIBUTIONS,
                           extrasaction="ignore", restval="")
        w.writeheader()

        contrib_files = sorted(RAW_DIR.glob("contributions_*.csv"))
        for raw_file in contrib_files:
            year_tag = raw_file.stem.split("_")[1]
            rows_in = rows_out = 0
            with open(raw_file, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    rows_in += 1
                    # ── Committee name ─────────────────────────────────
                    to_org   = _clean(row.get("To Organization", ""))
                    rec_last = _clean(row.get("Recipient Last Name", ""))
                    rec_first = _clean(row.get("Recipient First Name", ""))
                    if to_org:
                        committee_name = to_org
                    elif rec_last or rec_first:
                        committee_name = utils.clean_name(_join_name(rec_first, rec_last))
                    else:
                        continue   # can't identify recipient

                    # ── Contributor ────────────────────────────────────
                    from_org   = _clean(row.get("From Organization Name", ""))
                    cont_last  = _clean(row.get("Contributor Last Name", ""))
                    cont_first = _clean(row.get("Contributor First Name", ""))
                    # Skip KY KREF quarterly rollup rows — From Organization Name = "TOTAL"
                    # These are summary rows, not real contributions.
                    if from_org.upper() == "TOTAL":
                        continue
                    # "NEW YEAR" = start-of-year balance carryforward encoded as an org name
                    # (OtherText = 'BALANCE CARRIED FORWARD', date always Jan 1). Not a real
                    # contributor — blank it out so it doesn't pollute top-donor queries.
                    if from_org.upper() == "NEW YEAR":
                        from_org = ""
                    if from_org:
                        contributor_name = from_org
                    else:
                        contributor_name = _join_name(cont_first, cont_last)
                    # When from_org and individual name are both absent, fall back to
                    # OtherText (used by older KREF entries to label the source):
                    #   BALANCE_CARRYFORWARD → committee itself (own prior-period funds)
                    #   VARIOUS / other text → use the OtherText value directly
                    if not contributor_name:
                        cont_type  = _clean(row.get("Contribution Type", ""))
                        other_text = _clean(row.get("OtherText", ""))
                        if cont_type == "BALANCE_CARRYFORWARD":
                            contributor_name = committee_name
                        elif other_text and other_text.upper() not in ("BALANCE CARRIED FORWARD",):
                            contributor_name = other_text

                    # ── Amount + date ──────────────────────────────────
                    amount = _parse_amount(row.get("Amount", ""))
                    dt     = _parse_date(row.get("Receipt Date", ""))
                    if not amount or not dt:
                        continue

                    election_year = _parse_year(row.get("Election Date", "")) or year_tag

                    contrib_count += 1
                    rows_out += 1
                    w.writerow({
                        "state":             STATE,
                        "committee_name":    committee_name,
                        "amount":            amount,
                        "date":              dt,
                        "transaction_type":  _clean(row.get("Contribution Mode", "")),
                        "contributor_name":  _clean(contributor_name),
                        "contributor_type":  _clean(row.get("Contribution Type", "")),
                        "contributor_city":  "" if _clean(row.get("City", "")) in ("N/A", "NA") else _clean(row.get("City", "")),
                        "contributor_state": _clean_state(row.get("State", "")),
                        "contributor_zip":   utils.clean_zip(row.get("Zip", "")),
                        "employer":          _clean(row.get("Employer", "")),
                        "occupation":        _clean(row.get("Occupation", "")),
                        "candidate_name":    utils.clean_name(_join_name(rec_first, rec_last)),
                        "office":            _clean(row.get("Office Sought", "")),
                        "election_year":     election_year,
                        "amended":           "",
                        "filing_id":         "",
                        "raw_file":          raw_file.name,
                        "row_num":           contrib_count,
                    })

            log.info(f"    {raw_file.name}: {rows_in} in → {rows_out} out")

    log.info(f"    → {contrib_count:,} contributions total")

    # ── 4. Expenditures ────────────────────────────────────────────────
    log.info("  Parsing expenditures…")
    expend_path = CLEAN_DIR / "expenditures.csv.gz"
    expend_count = 0

    with gzip.open(expend_path, "wt", newline="", encoding="utf-8") as out_f:
        w = csv.DictWriter(out_f, fieldnames=C.EXPENDITURES,
                           extrasaction="ignore", restval="")
        w.writeheader()

        expend_files = sorted(RAW_DIR.glob("expenditures_*.csv"))
        for raw_file in expend_files:
            year_tag = raw_file.stem.split("_")[1]
            rows_in = rows_out = 0
            with open(raw_file, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    rows_in += 1
                    # ── Committee (spender) ────────────────────────────
                    from_cand_first = _clean(row.get("From Candidate First Name", ""))
                    from_cand_last  = _clean(row.get("From Candidate Last Name", ""))
                    from_org        = _clean(row.get("From Organization Name", ""))
                    if from_org:
                        committee_name = from_org
                    elif from_cand_last or from_cand_first:
                        committee_name = utils.clean_name(
                            _join_name(from_cand_first, from_cand_last)
                        )
                    else:
                        continue

                    # ── Payee ──────────────────────────────────────────
                    rec_last  = _clean(row.get("Recipient Last Name", ""))
                    rec_first = _clean(row.get("Recipient First Name", ""))
                    rec_org   = _clean(row.get("Organization Name", ""))
                    if rec_org:
                        payee_name = rec_org
                    else:
                        payee_name = _join_name(rec_first, rec_last)
                    # Skip KY KREF summary/accounting rows (payee = "TOTAL", etc.)
                    if payee_name.upper() in _EXPEND_SKIP_PAYEES:
                        continue

                    # ── Amount + date ──────────────────────────────────
                    amount = _parse_amount(row.get("Disbursement Amount", ""))
                    dt     = _parse_date(row.get("Disbursement Date", ""))
                    if not amount or not dt:
                        continue

                    election_year = _parse_year(row.get("Election Date", "")) or year_tag

                    # Independent expenditure → category
                    indep = (row.get("Is Independent Expenditure", "") or "").strip().lower()
                    category = "Independent Expenditure" if indep in ("yes", "true", "1") else ""

                    expend_count += 1
                    rows_out += 1
                    w.writerow({
                        "state":          STATE,
                        "committee_name": committee_name,
                        "amount":         amount,
                        "date":           dt,
                        "transaction_type": _clean(row.get("Disbursement Code", "")),
                        "payee_name":     _clean(payee_name),
                        "purpose":        _clean(row.get("Purpose", "")),
                        "category":       category,
                        "payee_city":     "",
                        "payee_state":    "",
                        "payee_zip":      "",
                        "candidate_name": utils.clean_name(
                            _join_name(from_cand_first, from_cand_last)
                        ),
                        "office":         _clean(row.get("Office Sought", "")),
                        "election_year":  election_year,
                        "amended":        "",
                        "filing_id":      "",
                        "raw_file":       raw_file.name,
                        "row_num":        expend_count,
                    })

            log.info(f"    {raw_file.name}: {rows_in} in → {rows_out} out")

    log.info(f"    → {expend_count:,} expenditures total")

    duration = round(time.perf_counter() - t0, 1)
    log._emit("parse_completed",
              status="completed",
              duration_s=duration,
              candidates=n_cands,
              committees=len(comm_rows),
              contributions=contrib_count,
              expenditures=expend_count)
    log.info(f"Done in {duration}s")


# ============================== CLI ==================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
