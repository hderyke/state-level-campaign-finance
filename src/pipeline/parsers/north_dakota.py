"""
parsers/north_dakota.py — Transform North Dakota raw CSVs into the 5 relations.

Input:  data/North Dakota/raw/   (written by scrapers/north_dakota.py)
  contributions_{year}.csv       — from CFRS "Contributions"
  expenditures_{year}.csv        — from CFRS "Expenditure" (singular in source)
  committees_{year}.csv          — from CFRS "Registration" (per-cycle filer roster)
  filed_reports_{year}.csv       — from CFRS "FiledReports" (report metadata)
  candidate_committees.xlsx      — from the "Get to Know" DataGrid export; the
                                   only source of office/district/party/address
  reporting_schedule_{year}.csv  — deadline calendar; NOT read by this parser

Output: data/North Dakota/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Source schema (confirmed against real 2025/2026 downloads)
──────────────────────────────────────────────────────────
  Contributions  RegistrantID, CommitteeName, CandidateName, TransactionType,
                 TransactionCategory, TransactionDate, TransactionAmount,
                 ContributorPayeeType, ContributorPayeeName,
                 ContributorAddress, EmployerName, FiledDate
  Expenditure    RegistrantID, CommitteeName, CandidateName, TransactionType,
                 ExpenditureType, ExpenditurePurpose, TransactionDate,
                 TransactionAmount, RecipientType, RecipientName,
                 RecipientAddress, FiledDate
  Registration   RegistrantID, CommitteeName, CandidateName, CommitteeType,
                 CommitteeSubType, RegistrationDate, CommitteeStatus
  FiledReports   RegistrantID, CommitteeName, CandidateName, ReportName,
                 ReportType, StartDate, EndDate, DueDate, FiledDate,
                 ReportVersion
  roster grid    CommitteeName, CandidateName, CommitteeAddress, Office,
                 District, Party, CommitteeStatus      (no RegistrantID)

Header resolution
─────────────────
Columns are still resolved through `_pick()` against a normalized header index
(lowercased, non-alphanumerics stripped) with an ordered list of candidate
spellings — the confirmed CFRS name leads each list, with defensive
alternatives behind it. CFRS has renamed columns between releases, and this way
a rename degrades one field to empty instead of crashing the parse.
`--show-headers` prints every raw header next to the field it resolved to.

Notes
─────
  • `TransactionType` is a constant ("Contributions" / "Expenditures") and
    carries no information. The real vocabulary lives in `TransactionCategory`
    (contributions) and `ExpenditureType` (expenditures), which is what gets
    written to `transaction_type`.
  • **committee_name falls back to CandidateName.** CFRS leaves CommitteeName
    empty for filers registered as `CommitteeSubType = "Candidate"` (a
    candidate filing personally, 177 of 482 registrations in 2026) — only 79%
    of contribution rows carry one. With the fallback, fill is 100%, which
    matters because committee_name is a tier-1 required column.
  • **Addresses come in two different shapes.** Transaction exports use a
    space-run blob — `"PO BOX 179   Minot ND 58702  "` — handled by
    parse_address(), which anchors on the trailing `ST ZIP` (99.8% of non-blank
    values) then walks tokens right-to-left for the city, stopping at the first
    street-ish token so `"1411 32nd St S Suite 7 Fargo"` yields `Fargo`. The
    roster grid instead uses comma-delimited fields —
    `"PO Box 1081, Bismarck, ND, UAS, 58502"` — handled by
    parse_grid_address(), which reads positionally from the right. Street
    address is discarded in both cases; the schema has no column for it.
  • **`amended` and `filing_id` are joined from FiledReports** on
    `(RegistrantID, FiledDate)`. `amended` is only set when every report
    matching that key agrees (≈5% of keys have both an Original and an Amended
    filed the same day; those are left blank rather than guessed). `filing_id`
    is set only when the key maps to exactly one ReportName.
  • **loans_debts is always empty.** CFRS publishes no loan or debt export.
    "Campaign Loan Repayment" appears as an `ExpenditurePurpose` value, but
    that's a disbursement, not a loan record, and stays in expenditures.
  • **Office, district, party and address come from a second source.** The
    Registration export has none of them. The public "Get to Know"
    candidate-committee roster grid does, so it's joined in — see load_roster().
    That grid carries no RegistrantID, so the join is by normalized committee
    name first and an honorific-stripped (FIRST, LAST) name key second: it
    writes "Mr. Coachman, Michael" where Registration writes "Doug Goehring".
    Registration wins wherever it has a value; the roster only fills blanks
    (it's an all-cycles snapshot, Registration is the per-cycle record).
    Measured against real data: 479 of 482 registrations enriched, taking
    candidates.office 0% → 98.6%, district → 91.0%, party → 82.8%, and
    committees.city/zip → 70.3%.
  • **treasurer_name, jurisdiction and incumbent are still empty.** No CFRS
    export publishes them (the roster grid has an OfficerName *filter* but no
    officer column in its output). Source limitation — do not synthesize.
  • `RecipientType` (payee classification) has no home in the canonical
    expenditure schema. It's written to `category`, which columns.py documents
    as per-state-only and drops at aggregate time, so nothing is lost and
    nothing cross-state is polluted.
  • `EmployerName` is only ~6% filled and there is no occupation column at all.
  • person_id model: "committee". `RegistrantID` may or may not be stable
    across cycles — a single year of Registration data can't settle it. The
    "committee" model is safe either way: it groups by
    (state, candidate_name, office, district) and takes min(state_filer_id), so
    it collapses per-cycle IDs if they differ and is a no-op if they don't.
    Roster enrichment materially improves this: office and district are now
    populated on ~98%/91% of candidates, so grouping is by
    (name, office, district) rather than degenerating to name alone.
"""

import csv
import gzip
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# ================================ paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "North Dakota" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "North Dakota" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "ND"
MAX_VALID_YEAR = date.today().year + 2

# Contribution/expenditure types that belong in loans_debts rather than their
# own table. No ND value currently matches — kept so that if CFRS adds a loan
# type to an existing export it lands in the right relation instead of being
# silently counted as a contribution. "Campaign Loan Repayment" is deliberately
# excluded: it's an ExpenditurePurpose on a real disbursement.
_LOAN_TYPE_RE = re.compile(r"\b(loan|debt)\b(?!\s*repayment)", re.IGNORECASE)


# ========================= header resolution ==========================

def _norm_header(h: str) -> str:
    """'Contributor  Zip-Code' → 'contributorzipcode'."""
    return re.sub(r"[^a-z0-9]", "", (h or "").lower())


def header_index(fieldnames) -> dict[str, str]:
    """{normalized header → actual header}. First occurrence wins on collision."""
    idx: dict[str, str] = {}
    for h in fieldnames or []:
        n = _norm_header(h)
        if n and n not in idx:
            idx[n] = h
    return idx


def _pick(row: dict, idx: dict[str, str], *candidates: str) -> str:
    """First non-empty value among candidate header spellings; '' if none match."""
    for cand in candidates:
        actual = idx.get(cand)
        if actual is None:
            continue
        val = (row.get(actual) or "").strip()
        if val:
            return val
    return ""


# Canonical field → ordered candidate header spellings (normalized).
# The confirmed CFRS spelling is first in each tuple.
_FILER_ID       = ("registrantid", "committeeid", "filerid", "filingentityid",
                   "entityid", "registrationid", "candidateid")
_COMMITTEE_NAME = ("committeename", "filername", "filingentityname",
                   "entityname", "committee", "organizationname")
_CANDIDATE_NAME = ("candidatename", "candidatefullname", "candidate")
_FILED_DATE     = ("fileddate", "datefiled", "submitteddate")

_CONTRIB_MAP = {
    "committee_name":   _COMMITTEE_NAME,
    "candidate_name":   _CANDIDATE_NAME,
    "amount":           ("transactionamount", "amount", "contributionamount"),
    "date":             ("transactiondate", "date", "contributiondate"),
    # TransactionCategory holds the real vocabulary; TransactionType is a
    # constant string and only a last-resort fallback.
    "transaction_type": ("transactioncategory", "contributiontype",
                         "receipttype", "transactiontype"),
    "contributor_name": ("contributorpayeename", "contributorname",
                         "contributor", "donorname"),
    "contributor_type": ("contributorpayeetype", "contributortype",
                         "donortype", "entitytype"),
    "address":          ("contributoraddress", "contributorpayeeaddress",
                         "address", "donoraddress"),
    "employer":         ("employername", "employer", "contributoremployer"),
    "occupation":       ("occupation", "occupationname", "principaloccupation"),
    "filed_date":       _FILED_DATE,
}

_EXPEND_MAP = {
    "committee_name":   _COMMITTEE_NAME,
    "candidate_name":   _CANDIDATE_NAME,
    "amount":           ("transactionamount", "amount", "expenditureamount"),
    "date":             ("transactiondate", "date", "expendituredate"),
    "transaction_type": ("expendituretype", "disbursementtype",
                         "transactioncategory", "transactiontype"),
    "purpose":          ("expenditurepurpose", "purpose", "description",
                         "purposeofexpenditure"),
    "payee_name":       ("recipientname", "payeename", "payee", "vendorname"),
    "payee_type":       ("recipienttype", "payeetype", "vendortype"),
    "address":          ("recipientaddress", "payeeaddress", "address",
                         "vendoraddress"),
    "filed_date":       _FILED_DATE,
}

_REGISTRATION_MAP = {
    "state_filer_id":  _FILER_ID,
    "committee_name":  _COMMITTEE_NAME,
    "candidate_name":  _CANDIDATE_NAME,
    "committee_type":  ("committeetype", "filertype", "registrationtype"),
    "committee_subtype": ("committeesubtype", "filersubtype", "subtype"),
    "registration_date": ("registrationdate", "dateregistered"),
    "status":          ("committeestatus", "status", "registrationstatus"),
    # Not present in the CFRS Registration export, but resolved defensively so
    # that if ND ever adds them the parser picks them up without a code change.
    "office":          ("office", "officesought", "officename"),
    "district":        ("district", "districtnumber", "legislativedistrict"),
    "party":           ("party", "partyname", "politicalparty"),
    "jurisdiction":    ("jurisdiction", "county"),
    "treasurer_name":  ("treasurername", "treasurer", "contactname"),
    "city":            ("committeecity", "city", "mailingcity"),
    "zip":             ("committeezip", "zipcode", "zip", "postalcode"),
    "election_year":   ("electionyear", "filingyear", "reportyear"),
}

_REPORT_MAP = {
    "state_filer_id": _FILER_ID,
    "report_name":    ("reportname", "reporttitle", "report"),
    "report_version": ("reportversion", "version", "amendmentstatus"),
    "filed_date":     _FILED_DATE,
}

# The "Get to Know" candidate-committee roster grid (see load_roster).
_ROSTER_MAP = {
    "committee_name": _COMMITTEE_NAME,
    "candidate_name": _CANDIDATE_NAME,
    "address":        ("committeeaddress", "address", "mailingaddress"),
    "office":         ("office", "officesought", "officename"),
    "district":       ("district", "districtname", "districtnumber"),
    "party":          ("party", "partyname", "politicalparty"),
    "status":         ("committeestatus", "status", "orgstatus"),
}


# ============================== helpers ================================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'960.6000' → '960.6000'; '$1,000.00' → '1000.00'; '(50)' → '-50'; '' on failure."""
    v = clean(val).replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]      # accounting-style negative
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """Normalize to YYYY-MM-DD; '' on failure or implausible year.

    CFRS emits ISO dates. The other formats are defensive — the legacy CFRS
    grid exports used MM/DD/YYYY and a future release could revert.
    """
    v = clean(val)
    if not v:
        return ""
    v = re.split(r"[T ]", v, maxsplit=1)[0]   # drop any time component
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def parse_year(val: str) -> str:
    """Extract a plausible 4-digit year, or ''. election_year is BIGINT, so a
    non-numeric value must be dropped rather than passed through."""
    m = re.search(r"(19|20)\d{2}", clean(val))
    if not m:
        return ""
    yr = int(m.group(0))
    return str(yr) if 1990 <= yr <= MAX_VALID_YEAR else ""


# ── address parsing ───────────────────────────────────────────────────
# CFRS concatenates the whole address into one column with runs of spaces
# where the empty sub-fields were, e.g.
#   "PO BOX 179   Minot ND 58702  "
#   "1411 32nd St S Suite 7 Fargo ND 58103"      (no run-of-spaces separator)
# Anchor on the trailing "ST ZIP" pair, then recover the city from what's left.
_ADDR_TAIL_RE = re.compile(
    r"^(?P<rest>.*?)\s+(?P<state>[A-Za-z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?|\d{9})\s*$"
)

# Tokens that mean "still in the street portion" when scanning right-to-left for
# a city. Bare directionals (N/S/E/W) and spelled-out ones are deliberately
# absent: "W Fargo" and "North Fargo" are real city names, and the
# single-letter rule in _city_from_remainder catches "... St W  Sioux Falls".
_STREET_TOKENS = {
    "st", "street", "ave", "avenue", "rd", "road", "dr", "drive", "ln", "lane",
    "blvd", "boulevard", "way", "ct", "court", "cir", "circle", "pl", "place",
    "hwy", "highway", "pkwy", "parkway", "ter", "terrace", "trl", "trail",
    "loop", "bnd", "bend", "cv", "cove", "sq", "square",
    "suite", "ste", "apt", "apartment", "unit", "box", "po", "pob", "rm",
    "room", "fl", "floor", "bldg", "building", "dept", "lot", "trlr", "spc",
    "no", "num",
}


def _city_from_remainder(rest: str) -> str:
    """Recover a city name from the street+city remainder of an address blob.

    CFRS usually separates street from city with a run of 2+ spaces, so the
    last run-delimited segment is normally the city verbatim — that's trusted
    when the segment looks clean, which is what preserves genuine multi-word
    names ("Watford City", "Grand Forks", "W Fargo", "L Anse").

    When the segment is contaminated — the source dropped the separator, as in
    "1411 32nd St S Suite 7 Fargo" — walk tokens right-to-left instead, keeping
    them until one looks like street rather than city: numeric, a street/unit
    keyword, or a lone letter *preceded by* one ("Suite B Fargo" → "Fargo",
    while "L Anse" keeps its "L" because nothing street-like precedes it).
    Capped at 3 tokens.
    """
    segs = [s.strip() for s in re.split(r"\s{2,}", rest) if s.strip()]
    if not segs:
        return ""

    tokens = segs[-1].split()
    bares  = [t.strip(".,#").lower() for t in tokens]

    # Clean segment — the run-of-spaces separator already isolated the city.
    if not any(any(ch.isdigit() for ch in b) or b in _STREET_TOKENS for b in bares):
        return segs[-1]

    picked: list[str] = []
    for i in range(len(tokens) - 1, -1, -1):
        bare = bares[i]
        if not bare or any(ch.isdigit() for ch in bare) or bare in _STREET_TOKENS:
            break
        # A lone letter after a unit/street keyword is an apartment or suite
        # designator ("Suite B"), not the start of a city name.
        if len(bare) == 1 and i > 0 and bares[i - 1] in _STREET_TOKENS:
            break
        picked.append(tokens[i].strip(","))
        if len(picked) == 3:
            break
    return " ".join(reversed(picked))


def parse_address(val: str) -> tuple[str, str, str]:
    """Split a CFRS address blob into (city, state, zip). ('', '', '') if unparseable.

    The street portion is intentionally discarded — the canonical schema has no
    street column.
    """
    v = clean(val)
    if not v:
        return "", "", ""
    m = _ADDR_TAIL_RE.match(v)
    if not m:
        # Street-only value (no city/state/ZIP was captured at source).
        return "", "", ""
    return (_city_from_remainder(m.group("rest")),
            m.group("state").upper(),
            utils.clean_zip(m.group("zip")))


# ── grid address parsing ──────────────────────────────────────────────
# The DataGrid roster export formats addresses as comma-delimited fields
# rather than the space-run blob the transaction exports use:
#   "PO Box 1081, Bismarck, ND, USA, 58502"
#   "301 N 4th St, 1005 N 1st St, Bismarck, ND, USA, 58501"   (two street lines)
# Positions from the right are stable (zip, country, state, city) even when the
# street spans extra fields, so parse from the end.
_GRID_ZIP_RE   = re.compile(r"^\d{5}(?:-\d{4})?$")
_GRID_STATE_RE = re.compile(r"^[A-Za-z]{2}$")


def parse_grid_address(val: str) -> tuple[str, str, str]:
    """Split a DataGrid roster address into (city, state, zip); ('','','') if it
    doesn't match the expected shape.

    Anchored on the right: [-1] zip, [-2] country, [-3] state, [-4] city. The
    country slot is *not* validated against a country list — the source has a
    'UAS' typo that outnumbers 'USA' roughly 3:1, so requiring 'USA' would
    silently drop the majority of addresses.
    """
    parts = [p.strip() for p in clean(val).split(",")]
    if len(parts) < 4:
        return "", "", ""
    if not (_GRID_ZIP_RE.match(parts[-1]) and _GRID_STATE_RE.match(parts[-3])):
        return "", "", ""
    return parts[-4], parts[-3].upper(), utils.clean_zip(parts[-1])


# ── cross-source name matching ────────────────────────────────────────
# The roster grid writes "Mr. Coachman, Michael" while Registration writes
# "Doug Goehring" — different order, and 20% of grid names carry an honorific.
# Both reduce to a (FIRST, LAST) key so the two sources can be joined.
_HONORIFIC_RE = re.compile(
    r"^(?:mr|mrs|ms|miss|dr|rev|hon|sen|rep|pastor|judge|prof)\.?\s+", re.IGNORECASE)
_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "esq"}


def name_key(name: str) -> tuple[str, str] | None:
    """(FIRST, LAST) from either 'Last, First M' or 'First M Last'. None if
    the value can't yield both parts.

    Honorifics and generational/professional suffixes are stripped so
    "Dr. Smith, Jo Ann Jr." and "Jo Ann Smith Jr" produce the same key.
    """
    n = _HONORIFIC_RE.sub("", clean(name))
    if not n:
        return None

    def toks(s: str) -> list[str]:
        return [t for t in s.replace(".", " ").split()
                if t and t.lower().strip(".") not in _NAME_SUFFIXES]

    if "," in n:
        last, _, given = n.partition(",")
        gt, lt = toks(given), toks(last)
        return (gt[0].upper(), lt[-1].upper()) if gt and lt else None
    t = toks(n)
    return (t[0].upper(), t[-1].upper()) if len(t) >= 2 else None


def split_person_name(name: str) -> tuple[str, str]:
    """('Patrick R Hatlestad') → ('Patrick', 'Hatlestad'). ('', '') if not a person name.

    CFRS writes candidate names as "First [Middle] Last", not "Last, First".
    """
    toks = [t for t in clean(name).split() if t]
    if len(toks) < 2:
        return (toks[0], "") if toks else ("", "")
    return toks[0], toks[-1]


def active_flag(status: str) -> str:
    """'1' active, '0' inactive, '' unknown. `active` is BIGINT — 0/1/empty only.

    ND uses exactly "Active"/"Inactive". The negative forms are tested first
    because "Inactive" contains the substring "active".
    """
    s = clean(status).lower()
    if not s:
        return ""
    if s in ("0", "false", "n", "no") or any(
            w in s for w in ("inactive", "not active", "closed", "terminated",
                             "dissolved", "expired", "withdrawn", "revoked")):
        return "0"
    if s in ("1", "true", "y", "yes") or any(
            w in s for w in ("active", "open", "current", "registered")):
        return "1"
    return ""


def year_from_filename(path: Path) -> str:
    """CFRS files are per-year and carry no election-year column, so the
    filename is the only source for election_year."""
    m = re.search(r"((?:19|20)\d{2})", path.name)
    return m.group(1) if m else ""


def raw_files(*patterns: str) -> list[Path]:
    """Non-empty raw files matching any pattern, de-duplicated, name-sorted.

    Name-sorted matters for the registry: later years overwrite earlier ones,
    so a filer's most recent registration wins.
    """
    out: dict[Path, None] = {}
    for pattern in patterns:
        for f in sorted(RAW_DIR.glob(pattern)):
            if f.is_file() and f.stat().st_size > 0:
                out[f] = None
    return list(out)


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def open_nd_csv(path: Path):
    """Open a raw CFRS CSV and return (handle, DictReader).

    utf-8-sig strips a BOM if the export happens to carry one. NUL bytes turn
    up in large CFRS exports; they're removed line-by-line via a generator so
    memory stays flat on big files.
    """
    fh = open(path, encoding="utf-8-sig", errors="replace", newline="")
    return fh, csv.DictReader(line.replace("\x00", "") for line in fh)


# ========================== filed-reports join ========================

def load_reports(log) -> dict[tuple[str, str], dict]:
    """
    Build {(registrant_id, filed_date): {"amended": "0"|"1"|"", "filing_id": str}}
    from the FiledReports exports.

    Transactions carry a FiledDate but no report identifier, so this is the only
    available link between a transaction and the report it arrived in. Where a
    filer filed more than one report on the same date and the versions disagree
    (about 5% of keys), `amended` is left blank rather than guessed; likewise
    `filing_id` is only set when the key maps to a single report name.
    """
    versions: dict[tuple[str, str], set[str]] = defaultdict(set)
    names:    dict[tuple[str, str], set[str]] = defaultdict(set)
    rows_seen = 0

    for path in raw_files("filed_reports*.csv"):
        ft = time.perf_counter()
        here = 0
        raw_fh, reader = open_nd_csv(path)
        try:
            idx = header_index(reader.fieldnames)
            for row in reader:
                fid   = _pick(row, idx, *_REPORT_MAP["state_filer_id"])
                filed = parse_date(_pick(row, idx, *_REPORT_MAP["filed_date"]))
                if not (fid and filed):
                    # ~40% of FiledReports rows carry no RegistrantID (and no
                    # name either) — unusable for a join, skipped.
                    continue
                key = (fid, filed)
                versions[key].add(_pick(row, idx, *_REPORT_MAP["report_version"]).lower())
                names[key].add(_pick(row, idx, *_REPORT_MAP["report_name"]))
                here += 1
        finally:
            raw_fh.close()
        rows_seen += here
        log.file_parsed(path.name, "filed_reports", here,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=path.stat().st_size, role="registry")

    reports: dict[tuple[str, str], dict] = {}
    for key, vs in versions.items():
        if vs == {"amended"}:
            amended = "1"
        elif vs == {"original"}:
            amended = "0"
        else:
            amended = ""      # mixed versions same day — genuinely ambiguous
        ns = {n for n in names[key] if n}
        reports[key] = {"amended": amended,
                        "filing_id": next(iter(ns)) if len(ns) == 1 else ""}

    if reports:
        log.registry_loaded("filed_reports", len(reports), relation="reports")
    return reports


def _report_fields(reports: dict, filer_id: str, filed_date: str) -> tuple[str, str]:
    """(amended, filing_id) for a transaction, or ('', '') if no clean match."""
    r = reports.get((filer_id, filed_date))
    return (r["amended"], r["filing_id"]) if r else ("", "")


# ========================= roster enrichment ==========================
# The Data Download Registration export has no office, district, party or
# address. The public "Get to Know" roster grid does, so it's joined in to fill
# them. It carries no RegistrantID, so the join is by name — see _ROSTER_* below.

_ROSTER_FIELDS = ("office", "district", "party", "city", "zip", "active")


def _roster_paths() -> list[Path]:
    return [p for p in (raw_files("candidate_committees.xlsx")
                        + raw_files("candidate_committees*.csv"))]


def _roster_rows(path: Path) -> tuple[list[str], list[dict]]:
    """Read the roster export (xlsx or csv) as (headers, list-of-dicts)."""
    if path.suffix.lower() in (".xlsx", ".xlsm", ".xls"):
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            ws   = wb.worksheets[0]
            rows = list(ws.iter_rows(values_only=True))
        finally:
            wb.close()
        if not rows:
            return [], []
        hdr = ["" if h is None else str(h) for h in rows[0]]
        out = []
        for r in rows[1:]:
            out.append({h: ("" if v is None else str(v).strip())
                        for h, v in zip(hdr, r)})
        return hdr, out

    fh, reader = open_nd_csv(path)
    try:
        return list(reader.fieldnames or []), [dict(r) for r in reader]
    finally:
        fh.close()


def load_roster(log) -> tuple[dict[str, dict], dict[tuple[str, str], dict]]:
    """
    Build (by_committee_name, by_name_key) enrichment indexes from the roster.

    Two indexes because the roster splits across row shapes: some rows carry
    only a committee name (PACs, party committees), some only a candidate name
    (candidates filing personally), and some both. Registration rows are looked
    up by committee name first, then by candidate name key.

    Ambiguous candidate keys are merged field-by-field rather than dropped: a
    name that appears twice is usually the same person in two cycles, so any
    field where all rows agree is safe to use and only the disagreeing fields
    (typically office, when someone switched races) are withheld.
    """
    by_cmte: dict[str, dict] = {}
    key_rows: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for path in _roster_paths():
        ft = time.perf_counter()
        headers, rows = _roster_rows(path)
        idx = header_index(headers)
        M   = _ROSTER_MAP
        kept = 0

        for row in rows:
            cmte = _pick(row, idx, *M["committee_name"])
            cand = _pick(row, idx, *M["candidate_name"])
            if not (cmte or cand):
                continue
            city, st, zipc = parse_grid_address(_pick(row, idx, *M["address"]))
            entry = {
                "office":   _pick(row, idx, *M["office"]),
                "district": _pick(row, idx, *M["district"]),
                "party":    _pick(row, idx, *M["party"]),
                "city":     city,
                "zip":      zipc,
                "active":   active_flag(_pick(row, idx, *M["status"])),
            }
            if cmte:
                by_cmte.setdefault(utils.clean_name(cmte), entry)
            k = name_key(cand)
            if k:
                key_rows[k].append(entry)
            kept += 1

        log.file_parsed(path.name, "roster", kept, role="registry",
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=path.stat().st_size)

    # Collapse each name key: keep only fields all its rows agree on.
    by_key: dict[tuple[str, str], dict] = {}
    withheld = 0
    for k, entries in key_rows.items():
        if len(entries) == 1:
            by_key[k] = entries[0]
            continue
        merged = {}
        for f in _ROSTER_FIELDS:
            vals = {e[f] for e in entries if e[f]}
            if len(vals) == 1:
                merged[f] = vals.pop()
            else:
                merged[f] = ""
                if vals:
                    withheld += 1
        by_key[k] = merged

    if by_cmte or by_key:
        log.registry_loaded("candidate_committees roster",
                            len(by_cmte) + len(by_key), relation="roster")
        if withheld:
            log.info(f"  roster: {withheld} field(s) withheld on ambiguous names")
    return by_cmte, by_key


def _roster_lookup(by_cmte: dict, by_key: dict,
                   cmte_name: str, cand_name: str) -> dict:
    """Roster entry for a Registration row: committee name first, then name key."""
    if cmte_name and cmte_name in by_cmte:
        return by_cmte[cmte_name]
    k = name_key(cand_name)
    if k and k in by_key:
        return by_key[k]
    return {}


# ============================ registration ============================

def _registration_row(row: dict, idx: dict[str, str]) -> dict:
    """Extract filer fields from one Registration row."""
    M = _REGISTRATION_MAP
    cand_name = _pick(row, idx, *M["candidate_name"])
    cmte_name = _pick(row, idx, *M["committee_name"])

    # CFRS leaves CommitteeName empty for candidates who file personally
    # (CommitteeSubType = "Candidate"). committee_name is tier-1 required, so
    # fall back to the candidate's own name — which is what the filer is.
    if not cmte_name:
        cmte_name = cand_name

    ctype = _pick(row, idx, *M["committee_type"])
    csub  = _pick(row, idx, *M["committee_subtype"])
    # Neither level is sufficient alone: CommitteeType collapses PAC /
    # Multicandidate / Ballot Measure into one bucket, while CommitteeSubType
    # labels ND's Independent Expenditure Committees as plain "Organization".
    # Joined with the same " -> " convention Wisconsin uses for its hierarchy.
    committee_type = f"{ctype} -> {csub}" if (ctype and csub) else (ctype or csub)

    first, last = split_person_name(cand_name)

    return {
        "state_filer_id":  _pick(row, idx, *M["state_filer_id"]),
        "committee_name":  utils.clean_name(cmte_name),
        "committee_type":  committee_type,
        "candidate_name":  utils.clean_name(cand_name),
        "candidate_first": first,
        "candidate_last":  last,
        "treasurer_name":  _pick(row, idx, *M["treasurer_name"]),
        "city":            _pick(row, idx, *M["city"]),
        "zip":             utils.clean_zip(_pick(row, idx, *M["zip"])),
        "office":          _pick(row, idx, *M["office"]),
        "district":        _pick(row, idx, *M["district"]),
        "party":           _pick(row, idx, *M["party"]),
        "jurisdiction":    _pick(row, idx, *M["jurisdiction"]),
        "active":          active_flag(_pick(row, idx, *M["status"])),
        "election_year":   parse_year(_pick(row, idx, *M["election_year"])),
    }


def parse_entities(log, cmte_w, cand_w,
                   roster_cmte: dict, roster_key: dict) -> tuple[dict[str, dict], int, int]:
    """
    Parse the Registration exports into committees and candidates, enriched
    from the roster grid.

    Returns (registry, committees_written, candidates_written). The registry is
    keyed by both RegistrantID and normalized committee name so transactions
    resolve either way; RegistrantID is present on 100% of transaction rows in
    practice, the name key is a safety net.

    One candidates row per (candidate, election year) — a filer re-registers
    each cycle and the candidates table is cycle-scoped via election_year.
    """
    registry: dict[str, dict] = {}
    cmte_count = cand_count = 0
    seen_candidates: set[tuple] = set()
    enriched = 0

    for path in raw_files("committees*.csv", "registration*.csv"):
        ft = time.perf_counter()
        rows_here = 0
        file_year = year_from_filename(path)
        raw_fh, reader = open_nd_csv(path)
        try:
            idx = header_index(reader.fieldnames)

            for row_num, row in enumerate(reader, start=2):
                e = _registration_row(row, idx)
                if not (e["committee_name"] or e["state_filer_id"]):
                    continue   # blank trailer row
                if not e["election_year"]:
                    e["election_year"] = file_year

                # Roster fills what Registration omits. Registration always
                # wins where it has a value — it's the authoritative per-cycle
                # record; the roster is an all-cycles snapshot.
                r = _roster_lookup(roster_cmte, roster_key,
                                   e["committee_name"], e["candidate_name"])
                if r:
                    filled = False
                    for f in _ROSTER_FIELDS:
                        if not e.get(f) and r.get(f):
                            e[f] = r[f]
                            filled = True
                    enriched += filled

                if e["state_filer_id"]:
                    registry[e["state_filer_id"]] = e
                if e["committee_name"]:
                    registry[e["committee_name"]] = e

                cmte_w.writerow({
                    "state":          STATE,
                    "state_filer_id": e["state_filer_id"],
                    "committee_name": e["committee_name"],
                    "committee_type": e["committee_type"],
                    "election_year":  e["election_year"],
                    "candidate_name": e["candidate_name"],
                    "treasurer_name": e["treasurer_name"],
                    "city":           e["city"],
                    "zip":            e["zip"],
                    "active":         e["active"],
                    "raw_file":       path.name,
                    "row_num":        row_num,
                })
                cmte_count += 1
                rows_here  += 1

                key = (e["candidate_name"], e["office"], e["district"],
                       e["election_year"])
                if e["candidate_name"] and key not in seen_candidates:
                    seen_candidates.add(key)
                    cand_w.writerow({
                        "state":           STATE,
                        "state_filer_id":  e["state_filer_id"],
                        "candidate_name":  e["candidate_name"],
                        "candidate_first": e["candidate_first"],
                        "candidate_last":  e["candidate_last"],
                        "office":          e["office"],
                        "district":        e["district"],
                        "jurisdiction":    e["jurisdiction"],
                        "party":           e["party"],
                        "election_year":   e["election_year"],
                        "incumbent":       "",
                        "raw_file":        path.name,
                        "row_num":         row_num,
                    })
                    cand_count += 1
        finally:
            raw_fh.close()

        log.file_parsed(path.name, "committees", rows_here,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=path.stat().st_size)

    if roster_cmte or roster_key:
        log.enrichment_summary(roster_matched=enriched, registrations=cmte_count)

    return registry, cmte_count, cand_count


def _lookup(registry: dict[str, dict], filer_id: str, cmte_name: str) -> dict:
    """Registry hit by RegistrantID, else by normalized committee name, else {}."""
    if filer_id and filer_id in registry:
        return registry[filer_id]
    if cmte_name and cmte_name in registry:
        return registry[cmte_name]
    return {}


# =========================== transactions =============================

def parse_contributions(log, cont_w, loan_w, registry, reports) -> tuple[int, int]:
    """Parse contribution exports into contributions (+ loans_debts if any row
    turns out to be a loan type — none do in current ND data)."""
    total = loans = 0

    for path in raw_files("contributions*.csv", "inkind*.csv"):
        default_type = "In-Kind" if path.name.lower().startswith("inkind") else ""
        file_year    = year_from_filename(path)
        ft = time.perf_counter()
        count = loan_count = skipped = 0
        M = _CONTRIB_MAP

        raw_fh, reader = open_nd_csv(path)
        try:
            idx = header_index(reader.fieldnames)

            for row_num, row in enumerate(reader, start=2):
                amount = parse_amount(_pick(row, idx, *M["amount"]))
                if not amount:
                    skipped += 1
                    continue

                filer_id  = _pick(row, idx, *_FILER_ID)
                cand_name = _pick(row, idx, *M["candidate_name"])
                cmte_name = _pick(row, idx, *M["committee_name"]) or cand_name
                cmte_name = utils.clean_name(cmte_name)
                reg       = _lookup(registry, filer_id, cmte_name)
                if not cmte_name:
                    cmte_name = reg.get("committee_name", "")

                city, st, zipc = parse_address(_pick(row, idx, *M["address"]))
                tx_type  = _pick(row, idx, *M["transaction_type"]) or default_type
                filed    = parse_date(_pick(row, idx, *M["filed_date"]))
                amended, filing_id = _report_fields(reports, filer_id, filed)

                if _LOAN_TYPE_RE.search(tx_type):
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cmte_name,
                        "original_amount":    amount,
                        "date":               parse_date(_pick(row, idx, *M["date"])),
                        "record_type":        tx_type,
                        "counterparty_name":  utils.clean_name(
                                                  _pick(row, idx, *M["contributor_name"])),
                        "counterparty_city":  city,
                        "counterparty_state": st,
                        "counterparty_zip":   zipc,
                        "candidate_name":     utils.clean_name(cand_name)
                                              or reg.get("candidate_name", ""),
                        "election_year":      file_year,
                        "amended":            amended,
                        "filing_id":          filing_id,
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    loan_count += 1
                    continue

                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    cmte_name,
                    "amount":            amount,
                    "date":              parse_date(_pick(row, idx, *M["date"])),
                    "transaction_type":  tx_type,
                    "contributor_name":  utils.clean_name(
                                             _pick(row, idx, *M["contributor_name"])),
                    "contributor_type":  _pick(row, idx, *M["contributor_type"]),
                    "contributor_city":  city,
                    "contributor_state": st,
                    "contributor_zip":   zipc,
                    "employer":          _pick(row, idx, *M["employer"]),
                    "occupation":        _pick(row, idx, *M["occupation"]),
                    "candidate_name":    utils.clean_name(cand_name)
                                         or reg.get("candidate_name", ""),
                    "office":            reg.get("office", ""),
                    # No election-year column in the source — the export is
                    # per-year and the filename is the only signal.
                    "election_year":     file_year,
                    "amended":           amended,
                    "filing_id":         filing_id,
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                count += 1
        finally:
            raw_fh.close()

        log.file_parsed(path.name, "contributions", count, skipped,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=path.stat().st_size)
        total += count
        loans += loan_count

    return total, loans


def parse_expenditures(log, expn_w, registry, reports) -> int:
    """Parse expenditure and independent-expenditure exports."""
    total = 0

    for path in raw_files("expenditures*.csv", "independent_expenditures*.csv"):
        default_type = ("Independent Expenditure"
                        if path.name.lower().startswith("independent") else "")
        file_year = year_from_filename(path)
        ft = time.perf_counter()
        count = skipped = 0
        M = _EXPEND_MAP

        raw_fh, reader = open_nd_csv(path)
        try:
            idx = header_index(reader.fieldnames)

            for row_num, row in enumerate(reader, start=2):
                amount = parse_amount(_pick(row, idx, *M["amount"]))
                if not amount:
                    skipped += 1
                    continue

                filer_id  = _pick(row, idx, *_FILER_ID)
                cand_name = _pick(row, idx, *M["candidate_name"])
                cmte_name = _pick(row, idx, *M["committee_name"]) or cand_name
                cmte_name = utils.clean_name(cmte_name)
                reg       = _lookup(registry, filer_id, cmte_name)
                if not cmte_name:
                    cmte_name = reg.get("committee_name", "")

                city, st, zipc = parse_address(_pick(row, idx, *M["address"]))
                filed = parse_date(_pick(row, idx, *M["filed_date"]))
                amended, filing_id = _report_fields(reports, filer_id, filed)

                expn_w.writerow({
                    "state":            STATE,
                    "committee_name":   cmte_name,
                    "amount":           amount,
                    "date":             parse_date(_pick(row, idx, *M["date"])),
                    "transaction_type": _pick(row, idx, *M["transaction_type"])
                                        or default_type,
                    "payee_name":       utils.clean_name(
                                            _pick(row, idx, *M["payee_name"])),
                    "purpose":          _pick(row, idx, *M["purpose"]),
                    # RecipientType has no canonical home; `category` is
                    # per-state-only and dropped at aggregate (see columns.py),
                    # so it preserves the field without polluting cross-state.
                    "category":         _pick(row, idx, *M["payee_type"]),
                    "payee_city":       city,
                    "payee_state":      st,
                    "payee_zip":        zipc,
                    "candidate_name":   utils.clean_name(cand_name)
                                        or reg.get("candidate_name", ""),
                    "office":           reg.get("office", ""),
                    "election_year":    file_year,
                    "amended":          amended,
                    "filing_id":        filing_id,
                    "raw_file":         path.name,
                    "row_num":          row_num,
                })
                count += 1
        finally:
            raw_fh.close()

        log.file_parsed(path.name, "expenditures", count, skipped,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=path.stat().st_size)
        total += count

    return total


# ============================ diagnostics =============================

def show_headers():
    """Print each raw file's headers and the canonical field each resolved to.

    Onboarding aid for the header layer: a column shown as (unmapped) is either
    genuinely unused or a spelling to add to the maps above.
    """
    maps = {
        "contributions":            _CONTRIB_MAP,
        "inkind":                   _CONTRIB_MAP,
        "expenditures":             _EXPEND_MAP,
        "independent_expenditures": _EXPEND_MAP,
        "committees":               _REGISTRATION_MAP,
        "registration":             _REGISTRATION_MAP,
        "candidates":               _REGISTRATION_MAP,
        "filed_reports":            _REPORT_MAP,
        "candidate_committees":     _ROSTER_MAP,
    }
    # Include the roster workbook, not just the CSV exports
    files = raw_files("*.csv", "*.xlsx", "*.xls", "*.xlsm")
    if not files:
        print(f"No raw files in {RAW_DIR}")
        return

    for path in files:
        stem = re.sub(r"_?(?:19|20)\d{2}$", "", path.stem)
        fmap = maps.get(stem)
        if fmap is None:
            # e.g. reporting_schedule — downloaded for completeness, never read
            print(f"\n{path.name}  (not consumed by this parser)")
            continue

        # Every table also resolves the shared filer-ID candidates
        fmap = {**fmap, "state_filer_id": _FILER_ID}

        resolves: dict[str, str] = {}
        for field, cands in fmap.items():
            for cand in cands:
                resolves.setdefault(cand, field)

        if path.suffix.lower() in (".xlsx", ".xls", ".xlsm"):
            headers, _ = _roster_rows(path)
        else:
            fh, reader = open_nd_csv(path)
            try:
                headers = reader.fieldnames or []
            finally:
                fh.close()

        print(f"\n{path.name}  ({len(headers)} columns, map={stem})")
        for h in headers:
            field = resolves.get(_norm_header(h))
            print(f"  {'✓' if field else '·'} {h:<28} → {field or '(unmapped)'}")


# ================================= run =================================

def run():
    log = get_logger("north_dakota", "parse")
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
        # Always written, always empty — CFRS publishes no loan/debt export.
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        if not raw_files("*.csv"):
            raise FileNotFoundError(
                f"No raw CSVs in {RAW_DIR} — run scrapers/north_dakota.py first."
            )

        # Roster first — it fills the office/district/party/address columns
        # Registration omits, and the registry it feeds is what carries `office`
        # onto transaction rows.
        roster_cmte, roster_key = load_roster(log)
        registry, committees_written, candidates_written = parse_entities(
            log, cmte_w, cand_w, roster_cmte, roster_key)
        reports = load_reports(log)

        total_contributions, inline_loans = parse_contributions(
            log, cont_w, loan_w, registry, reports)
        total_expenditures = parse_expenditures(log, expn_w, registry, reports)
        total_loans        = inline_loans

        log.enrichment_summary(registry_entries=len(registry),
                               report_keys=len(reports),
                               committees=committees_written,
                               candidates=candidates_written)

        # Close before person-ID assignment — those helpers rewrite the files.
        for fh in file_handles:
            fh.close()
        file_handles = []

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
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_contributions:,} contributions, "
                 f"{total_expenditures:,} expenditures, {total_loans:,} loans, "
                 f"{committees_written:,} committees, {candidates_written:,} candidates")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written,
                  loans_debts=total_loans)

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
    import argparse

    ap = argparse.ArgumentParser(
        description="Parse North Dakota campaign finance data.")
    ap.add_argument("--show-headers", action="store_true",
                    help="print raw file headers and their canonical mapping, then exit")
    args, _ = ap.parse_known_args()   # orc forwards scraper flags the parser ignores

    try:
        if args.show_headers:
            show_headers()
        else:
            run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
