"""
parsers/new_mexico.py — Transform New Mexico CFIS raw files into the 5 relations.

Input:  data/New Mexico/raw/
  contributions_{year}.csv   — CFIS "Contributions and Loans" bulk export
  expenditures_{year}.csv    — CFIS "Expenditures" bulk export
  candidates_{year}.json     — /Organization/SearchCandidates response
  committees_{year}.json     — /Organization/SearchCommittees response
  offices_{year}.json        — /Organization/GetOffices response

Output: data/New Mexico/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Column layout
─────────────
The two transaction exports have no published header contract — the SOS
publishes them as *positional* layout keys ("Contributions and Loans File
Layout Key" / "Expenditures File Layout Key", position A…II). Position is
therefore the primary contract here, with header text used only to correct
individual indices when a recognizable name is present. That way a column
inserted upstream is caught by the name pass, and a header-text reword doesn't
break parsing on its own.

Two quirks of the published expenditure key are worth knowing:
  • It labels the Purpose row "Y" while listing it between "T End of Period"
    and "V Expenditure Type" — an obvious typo for U. Purpose is read at U.
  • It reuses the letter Y again for Reporting Entity Type. The sequence
    U…FF used here is the one that makes the column count come out right.

Notes
─────
  • Loans. The Contributions file carries contributions *and* loans received.
    The published layout key says Contribution Type "will be blank for Loans
    Received" — it isn't. The column is an enumerated list and loans carry an
    explicit "Loans Received" value; there are no blank-type rows at all. Those
    rows are routed to loans_debts.csv.gz. See LOAN_TYPES. The Expenditures file
    likewise contains loan *payments*, but carries no equivalent flag, so those
    stay in expenditures.
  • Entity JSON is read defensively. CFIS is an undocumented internal API, so
    _unwrap() walks the plausible envelope shapes and _pick() resolves fields
    through alias lists on a punctuation-insensitive key map. The confirmed
    shape (2026-08-06) is a bare JSON array of flat records. If the API ever
    returns nothing usable, entities are still built in full from the
    transaction files (which always carry Org Id, filer type, committee name and
    candidate name parts) — the JSON only ever adds office, district, party,
    jurisdiction, registration status and committee subtype on top of that.
  • ENTITY IDS. state_filer_id is the CFIS Org Id from column A of the
    transaction exports. The entity search has no public numeric key of its own:
    IDNumber, RegistrationId and MemberID are opaque 44-character tokens, and
    StateID — the one numeric field, and the only one that matches an Org Id —
    is set on 105/3,103 candidate and 1,053/1,452 committee records.
    _pick_numeric_id() therefore refuses non-numeric values, and registry rows
    that can't be keyed are used for enrichment only rather than written with a
    fabricated ID. That costs the ~770 candidates who registered but filed no
    transactions in 2020–2026; they carry no money data by definition.
  • The Election column is "General" / "Primary" / "Special" / "Local" with no
    year in it, so election_year comes from the source filename in practice.
  • person_id model: "committee" — Org Id identifies a reporting entity
    registration, and NM candidates re-register per cycle, so IDs are grouped by
    (name, office, district) and collapsed to the earliest.
  • Independent expenditures. Column W ("Reason") holds either a candidate name
    or a ballot question, and column X ("Stance") holds support/oppose.
    support_oppose is written whenever Stance parses; affiliated_candidate_name
    is only written when Reason matches a name already known to the candidate
    registry, so ballot questions don't leak into a candidate field.
  • Coverage starts at 2020. Earlier NM filings exist only in the decommissioned
    cfis.state.nm.us system and are not reachable from this source.
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

# CFIS description/purpose fields are free text and occasionally very long.
csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "New Mexico" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "New Mexico" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "NM"
MAX_VALID_YEAR = date.today().year + 2

# Contribution Type values that mean "this is a loan, not a contribution", and
# so belong in loans_debts rather than contributions.
#
# The published layout key says Contribution Type "will be blank for Loans
# Received". That is not what the data does — the column is an enumerated list
# and loans carry an explicit "Loans Received" value (2,041 rows across
# 2020–2026, versus zero blank-type rows). Routing on the documented blank
# produced an empty loans table; routing on the value is what actually works.
LOAN_TYPES = {"loans received", "loan received", "loans"}

# ========================== column layouts ============================
# Position → field name, transcribed from the SOS layout keys. The index is the
# contract; HEADER_ALIASES below can override an individual index when the file
# actually ships a recognizable header for that column.

CON_POSITIONS = [
    "org_id",              # A
    "amount",              # B
    "date",                # C
    "last_name",           # D  entity full name when not an individual
    "first_name",          # E
    "middle_name",         # F
    "prefix",              # G
    "suffix",              # H
    "address1",            # I
    "address2",            # J
    "city",                # K
    "contrib_state",       # L
    "zip",                 # M
    "description",         # N
    "check_number",        # O
    "transaction_id",      # P
    "filed_date",          # Q
    "election",            # R
    "report_name",         # S
    "period_start",        # T
    "period_end",          # U
    "contributor_code",    # V
    "contribution_type",   # W  blank ⇒ this row is a loan received
    "entity_type",         # X
    "committee_name",      # Y
    "cand_last",           # Z
    "cand_first",          # AA
    "cand_middle",         # BB
    "cand_prefix",         # CC
    "cand_suffix",         # DD
    "amended",             # EE
    "employer",            # FF
    "occupation",          # GG
    "occupation_comment",  # HH
    "employment_requested",# II
]

EXP_POSITIONS = [
    "org_id",              # A
    "amount",              # B
    "date",                # C
    "last_name",           # D  entity full name when not an individual
    "first_name",          # E
    "middle_name",         # F
    "prefix",              # G
    "suffix",              # H
    "address1",            # I
    "address2",            # J
    "city",                # K
    "payee_state",         # L
    "zip",                 # M
    "description",         # N
    "transaction_id",      # O
    "filed_date",          # P
    "election",            # Q
    "report_name",         # R
    "period_start",        # S
    "period_end",          # T
    "purpose",             # U  (layout key mislabels this "Y")
    "expenditure_type",    # V
    "reason",              # W  candidate name OR ballot question
    "stance",              # X
    "entity_type",         # Y
    "committee_name",      # Z
    "cand_last",           # AA
    "cand_first",          # BB
    "cand_middle",         # CC
    "cand_prefix",         # DD
    "cand_suffix",         # EE
    "amended",             # FF
]

# Header text → field, used to correct positions when the export ships a header
# row. Keys are compared after stripping everything but [a-z0-9].
HEADER_ALIASES = {
    "org_id":               {"orgid", "organizationid", "org"},
    "amount":               {"transactionamount", "expenditureamount", "amount"},
    "date":                 {"transactiondate", "expendituredate", "date"},
    "last_name":            {"lastname", "payeelastname", "contributorlastname"},
    "first_name":           {"firstname", "payeefirstname", "contributorfirstname"},
    "middle_name":          {"middlename", "payeemiddlename", "contributormiddlename"},
    "prefix":               {"prefix", "payeeprefix", "contributorprefix"},
    "suffix":               {"suffix", "payeesuffix", "contributorsuffix"},
    "address1":             {"contributoraddressline1", "payeeaddress1",
                             "address1", "addressline1"},
    "address2":             {"contributoraddressline2", "payeeaddress2",
                             "address2", "addressline2"},
    "city":                 {"contributorcity", "payeecity", "city"},
    "contrib_state":        {"contributorstate"},
    "payee_state":          {"payeestate"},
    "zip":                  {"contributorzipcode", "payeezipcode",
                             "contributorzip", "payeezip", "zipcode", "zip"},
    "description":          {"description"},
    "check_number":         {"checknumber"},
    "transaction_id":       {"transactionid", "expenditureid"},
    "filed_date":           {"fileddate"},
    "election":             {"election"},
    "report_name":          {"reportname"},
    "period_start":         {"startofperiod"},
    "period_end":           {"endofperiod"},
    "contributor_code":     {"contributorcode"},
    "contribution_type":    {"contributiontype"},
    "purpose":              {"purpose"},
    "expenditure_type":     {"expendituretype"},
    "reason":               {"reason"},
    "stance":               {"stance"},
    # CFIS ships the column as "Report Entity Type"; the layout key calls it
    # "Reporting Entity Type". Both spellings are accepted.
    "entity_type":          {"reportentitytype", "reportingentitytype"},
    "committee_name":       {"committeename"},
    "cand_last":            {"candidatelastname"},
    "cand_first":           {"candidatefirstname"},
    "cand_middle":          {"candidatemiddlename"},
    "cand_prefix":          {"candidateprefix"},
    "cand_suffix":          {"candidatesuffix"},
    "amended":              {"amended"},
    "employer":             {"contributoremployer", "employer"},
    "occupation":           {"contributoroccupation", "occupation"},
    "occupation_comment":   {"occupationcomment"},
    "employment_requested": {"employmentinformationrequested"},
}

# ===================== entity JSON field aliases ======================
# Ordered by preference — first key present on the record wins. Field names are
# unconfirmed against a live response, so each list is deliberately generous.

# Confirmed against a live 2020–2026 pull (2026-08-06). Aliases beyond the
# confirmed name are kept as a hedge against CFIS renaming a field, since these
# endpoints are undocumented and the system is slated for replacement.
#
# On IDs: CFIS's entity search exposes no public numeric key. IDNumber,
# RegistrationId and MemberID are all opaque 44-character tokens, and StateID —
# the only numeric one, and the only one that matches an Org Id in the
# transaction exports — is populated on just 105/3,103 candidate records and
# 1,053/1,452 committee records. So the id lists below are resolved through
# _pick_numeric_id(), which refuses anything non-numeric rather than writing a
# token into state_filer_id. See the ENTITY IDS note in the module docstring.
CAND_KEYS = {
    "id":            ["StateID", "StateId", "CandidateId", "CandidateID",
                      "OrgId", "OrgID", "OrganizationId", "OrganizationID",
                      "EntityId", "EntityID"],
    "name":          ["CandidateName", "Candidate", "FullName", "Name"],
    "first":         ["FirstName", "CandidateFirstName"],
    "last":          ["LastName", "CandidateLastName"],
    "office":        ["OfficeName", "OfficeSought", "Office"],
    "district":      ["District", "DistrictName", "OfficeDistrict"],
    "jurisdiction":  ["Jurisdiction", "JurisdictionName"],
    "party":         ["Party", "PartyName", "PoliticalParty"],
    "election_year": ["ElectionYear", "Year"],
    # Registration status (Active/Inactive) — not CompliantStatus, which is a
    # separate filing-compliance judgement and not what `active` means here.
    "status":        ["Status"],
    "incumbent":     ["Incumbent"],
}

CMTE_KEYS = {
    "id":            ["StateID", "StateId", "CommitteeId", "CommitteeID",
                      "OrgId", "OrgID", "OrganizationId", "OrganizationID",
                      "EntityId", "EntityID"],
    "name":          ["CommitteeName", "Committee", "Name"],
    "type":          ["CommitteeType", "Type"],
    "subtype":       ["CommitteeSubtype", "CommitteeSubType", "SubType",
                      "Subtype", "PacType"],
    "election_year": ["ElectionYear", "Year"],
    "status":        ["Status"],
    "candidate":     ["CandidateName", "Candidate"],
    "treasurer":     ["Treasurer", "TreasurerName"],
}

OFFICE_KEYS = {
    "jurisdiction_type": ["JurisdictionType"],
    "jurisdiction":      ["Jurisdiction", "JurisdictionName"],
    "office":            ["OfficeName", "OfficeSought", "Office"],
    "district":          ["District", "DistrictName"],
    "election_year":     ["ElectionYear", "Year"],
    "election_name":     ["ElectionName"],
}


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'$1,000.00' or '(500)' → plain numeric string; '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]   # accounting parentheses = negative
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """Normalize to YYYY-MM-DD; '' on failure or implausible year."""
    v = (val or "").strip()
    if not v:
        return ""
    # CFIS exports occasionally carry a time component on datetime columns
    v = v.split("T")[0].strip()
    if " " in v and "/" in v:
        v = v.split(" ")[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def amended_flag(val: str) -> str:
    """Y/N → 1/0; anything else → ''."""
    v = (val or "").strip().upper()
    if v in ("Y", "YES", "1", "TRUE"):
        return "1"
    if v in ("N", "NO", "0", "FALSE"):
        return "0"
    return ""


def stance_flag(val: str) -> str:
    """Support/Oppose → S/O; anything else → ''."""
    v = (val or "").strip().upper()
    if v.startswith("S"):
        return "S"
    if v.startswith("O"):
        return "O"
    return ""


def person_name(last: str, first: str, middle: str = "", suffix: str = "") -> str:
    """Assemble CFIS name parts into the 'LAST, FIRST MIDDLE SUFFIX' form.

    Matches the format CFIS itself uses on its candidate search screens, which
    is what makes the transaction ↔ entity-registry name join work. Non-person
    filers put their whole name in the last-name field and leave first blank, so
    those pass through unchanged rather than picking up a stray comma.

    Returns a clean_name'd (uppercased, whitespace-collapsed) value because
    every name this builds is a join or grouping key somewhere downstream —
    committee_name against committees.csv, contributor_name in the donor
    rollups, Reason against the candidate registry.
    """
    last, first = clean(last), clean(first)
    middle, suffix = clean(middle), clean(suffix)
    if not last and not first:
        return ""
    if first:
        name = f"{last}, {first}" if last else first
        if middle:
            name += f" {middle}"
    else:
        name = last
    if suffix:
        name += f" {suffix}"
    return utils.clean_name(name)


_YEAR_RE = re.compile(r"(19|20)\d{2}")


def year_from_text(val: str) -> str:
    """Pull a plausible 4-digit year out of free text; '' if there isn't one.

    The Election column is normally just "General" / "Primary" / "Special" with
    no year at all, so this almost always returns '' and the caller falls back
    to the filename year. It earns its keep on the handful of rows where a filer
    typed a year in — and on the one row that reads "2080 General", which is why
    the result is range-checked instead of trusted.
    """
    m = _YEAR_RE.search(val or "")
    if not m:
        return ""
    year = int(m.group(0))
    return m.group(0) if 1990 <= year <= MAX_VALID_YEAR else ""


def year_from_filename(path: Path) -> str:
    m = re.search(r"(\d{4})", path.name)
    return m.group(1) if m else ""


def norm_key(val: str) -> str:
    """Uppercase, whitespace-collapsed key for name-based registry joins."""
    return utils.clean_name(val)


def raw_files(pattern: str) -> list[Path]:
    """Non-empty raw files matching a glob, in filename (i.e. year) order."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ======================= transaction column map =======================

def _norm_header(cell: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (cell or "").lower())


def build_column_map(first_row: list[str], positions: list[str]) -> tuple[dict, bool]:
    """
    Return ({field: index}, header_present).

    Starts from the positional layout (the SOS's actual published contract) and
    then overrides individual entries where the first row carries a recognizable
    header name. Doing both means a column inserted upstream is corrected by the
    name pass, while a reworded header alone can't break the parse.
    """
    colmap = {field: i for i, field in enumerate(positions)}

    normed = [_norm_header(c) for c in first_row]
    # A header row's first cell is the Org Id label; a data row's is a number.
    header_present = bool(normed and normed[0] and not normed[0].isdigit())
    if not header_present:
        return colmap, False

    for idx, cell in enumerate(normed):
        if not cell:
            continue
        for field in positions:
            if cell in HEADER_ALIASES.get(field, ()):
                colmap[field] = idx
                break

    return colmap, True


def get(row: list[str], colmap: dict, field: str) -> str:
    """Read one field from a positional row; '' when short or unmapped."""
    idx = colmap.get(field)
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


# ========================= entity JSON reading ========================

def _unwrap(payload) -> list[dict]:
    """Return the record list from a CFIS JSON response of unknown envelope.

    CFIS's wrapper shape isn't documented, so this checks a bare list first and
    then the handful of container keys .NET result wrappers commonly use, one
    level deep. Returns [] rather than raising — a shape miss degrades entity
    enrichment, it doesn't fail the parse.
    """
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if not isinstance(payload, dict):
        return []

    containers = ("data", "Data", "results", "Results", "items", "Items",
                  "records", "Records", "Table", "List", "rows", "Rows")
    for key in containers:
        inner = payload.get(key)
        if isinstance(inner, list):
            return [r for r in inner if isinstance(r, dict)]
        if isinstance(inner, dict):
            for k2 in containers:
                if isinstance(inner.get(k2), list):
                    return [r for r in inner[k2] if isinstance(r, dict)]

    # Last resort: the longest list of dicts anywhere in the first few levels,
    # whatever it happens to be called. This is what makes an envelope we never
    # anticipated (say {"Payload": {"Rows": [...]}}) still parse — only the
    # field names inside would then need an alias, not the wrapper.
    best: list[dict] = []
    frontier = [(payload, 0)]
    while frontier:
        node, depth = frontier.pop()
        if depth > 3 or not isinstance(node, dict):
            continue
        for value in node.values():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                if len(value) > len(best):
                    best = value
            elif isinstance(value, dict):
                frontier.append((value, depth + 1))
    return [r for r in best if isinstance(r, dict)]


def _pick_numeric_id(record: dict, keymap: dict) -> str:
    """Resolve an entity ID, accepting only a purely numeric value.

    CFIS's entity search returns several ID-shaped fields, but all except
    StateID are opaque 44-character tokens that correspond to nothing in the
    transaction exports. Writing one into state_filer_id would be actively
    harmful: utils._numeric_id strips the non-digits and takes the remainder
    modulo 10^12, so a token silently becomes an arbitrary person_id that can
    collide with a real filer's. Refusing non-numeric values means an entity we
    can't key properly is used for enrichment only and never written with a
    fabricated identifier.
    """
    for alias in keymap.get("id", ()):
        val = _pick(record, {"id": [alias]}, "id")
        if val and val.isdigit() and val != "0":
            return val
    return ""


def _pick(record: dict, keymap: dict, field: str) -> str:
    """Resolve one logical field from a record via its alias list.

    Matching ignores case and punctuation so `electionYear`, `ElectionYear` and
    `Election_Year` all resolve to the same alias.
    """
    lookup = {re.sub(r"[^a-z0-9]", "", k.lower()): v for k, v in record.items()}
    for alias in keymap.get(field, ()):
        val = lookup.get(re.sub(r"[^a-z0-9]", "", alias.lower()))
        if val not in (None, ""):
            return str(val).strip()
    return ""


def load_entity_json(log, pattern: str) -> list[tuple[dict, str, int]]:
    """Load every matching entity JSON file → [(record, filename, row_num)]."""
    out: list[tuple[dict, str, int]] = []
    for path in raw_files(pattern):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError) as e:
            log.file_parse_error(path.name, str(e))
            continue
        records = _unwrap(payload)
        # An empty list is a legitimate answer, not a parse failure — CFIS
        # returns [] for committee/office searches in odd (non-election) years.
        # Only warn when there was a payload we failed to make sense of.
        if not records and payload not in ([], {}, None):
            log.warning(f"  {path.name}: no records found in JSON envelope")
        for i, rec in enumerate(records, start=1):
            out.append((rec, path.name, i))
        log.registry_loaded(path.name, len(records), relation=pattern.split("_")[0],
                            bytes=path.stat().st_size)
    return out


# ================================ run =================================

def run():
    log = get_logger("new mexico", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, cand_fh, cmte_fh, loan_fh]

        # ── Entity registries from the CFIS JSON ───────────────────────
        # Keyed by normalized candidate/committee name — the API's own ID field
        # can't be assumed present, but the name always is, and the transaction
        # exports carry the same name parts, so name is the reliable join.
        api_candidates: dict[str, dict] = {}
        for rec, fname, rnum in load_entity_json(log, "candidates_*.json"):
            name = _pick(rec, CAND_KEYS, "name") or person_name(
                _pick(rec, CAND_KEYS, "last"), _pick(rec, CAND_KEYS, "first"))
            if not name:
                continue
            entry = {
                "candidate_name": name,
                "state_filer_id": _pick_numeric_id(rec, CAND_KEYS),
                "office":         _pick(rec, CAND_KEYS, "office"),
                "district":       _pick(rec, CAND_KEYS, "district"),
                "jurisdiction":   _pick(rec, CAND_KEYS, "jurisdiction"),
                "party":          _pick(rec, CAND_KEYS, "party"),
                "incumbent":      _pick(rec, CAND_KEYS, "incumbent"),
                "election_year":  _pick(rec, CAND_KEYS, "election_year")
                                  or year_from_filename(Path(fname)),
                "raw_file":       fname,
                "row_num":        rnum,
            }
            key = norm_key(name)
            # Most recent cycle wins — later files sort last by filename/year.
            api_candidates[key] = entry

        api_committees: dict[str, dict] = {}
        for rec, fname, rnum in load_entity_json(log, "committees_*.json"):
            name = _pick(rec, CMTE_KEYS, "name")
            if not name:
                continue
            subtype = _pick(rec, CMTE_KEYS, "subtype")
            status  = _pick(rec, CMTE_KEYS, "status")
            api_committees[norm_key(name)] = {
                "committee_name": name,
                "state_filer_id": _pick_numeric_id(rec, CMTE_KEYS),
                # Subtype carries the real distinction (Independent Expenditure
                # vs. Contribution or Coordination vs. Party) where it's set,
                # but it's a useless literal "Other" on 586 of 1,452 registry
                # rows — in that case the parent type at least says whether this
                # is a political committee or an individual IE filer.
                "committee_type": (subtype if subtype and subtype.lower() != "other"
                                   else _pick(rec, CMTE_KEYS, "type") or subtype),
                "election_year":  _pick(rec, CMTE_KEYS, "election_year")
                                  or year_from_filename(Path(fname)),
                "candidate_name": _pick(rec, CMTE_KEYS, "candidate"),
                "treasurer_name": _pick(rec, CMTE_KEYS, "treasurer"),
                # Registration status, Active/Inactive.
                "active":         "1" if status.lower() == "active"
                                  else ("0" if status else ""),
                "raw_file":       fname,
                "row_num":        rnum,
            }

        # Offices are a seat list, not a per-candidate record — used only to
        # backfill jurisdiction when the candidate row itself doesn't carry one.
        offices_by_seat: dict[tuple[str, str, str], dict] = {}
        for rec, fname, rnum in load_entity_json(log, "offices_*.json"):
            office = _pick(rec, OFFICE_KEYS, "office")
            if not office:
                continue
            seat = (norm_key(office),
                    norm_key(_pick(rec, OFFICE_KEYS, "district")),
                    _pick(rec, OFFICE_KEYS, "election_year")
                    or year_from_filename(Path(fname)))
            offices_by_seat[seat] = {
                "jurisdiction":      _pick(rec, OFFICE_KEYS, "jurisdiction"),
                "jurisdiction_type": _pick(rec, OFFICE_KEYS, "jurisdiction_type"),
            }

        log.info(f"  registry: {len(api_candidates):,} candidates, "
                 f"{len(api_committees):,} committees, "
                 f"{len(offices_by_seat):,} offices")

        # ── Org registry, accumulated while parsing transactions ───────
        # org_id → the filer's identity as the transaction files describe it.
        # This is what guarantees a populated state_filer_id on every entity row:
        # Org Id is column A of both exports and is never blank in practice.
        orgs: dict[str, dict] = {}

        def note_org(org_id: str, cmte_name: str, cand_name: str,
                     entity_type: str, elec_year: str, fname: str, rnum: int):
            if not org_id:
                return
            rec = orgs.get(org_id)
            if rec is None:
                rec = {
                    "state_filer_id": org_id,
                    "committee_name": cmte_name,
                    "candidate_name": cand_name,
                    "committee_type": entity_type,
                    "election_year":  elec_year,
                    "raw_file":       fname,
                    "row_num":        rnum,
                }
                orgs[org_id] = rec
                return
            # Fill gaps from later rows; keep the latest election year seen.
            for field, val in (("committee_name", cmte_name),
                               ("candidate_name", cand_name),
                               ("committee_type", entity_type)):
                if val and not rec[field]:
                    rec[field] = val
            if elec_year and elec_year > (rec["election_year"] or ""):
                rec["election_year"] = elec_year

        def candidate_context(cand_name: str) -> dict:
            """Office/party/district for a candidate, from the API registry."""
            return api_candidates.get(norm_key(cand_name), {}) if cand_name else {}

        # ── Contributions and loans ────────────────────────────────────
        for path in raw_files("contributions_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = loans = skipped = 0

            with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
                reader = csv.reader(f)
                try:
                    first = next(reader)
                except StopIteration:
                    continue

                colmap, has_header = build_column_map(first, CON_POSITIONS)
                rows = reader if has_header else _chain(first, reader)
                start = 2 if has_header else 1

                for row_num, row in enumerate(rows, start=start):
                    if not row or not any(row):
                        continue

                    amount = parse_amount(get(row, colmap, "amount"))
                    if not amount:
                        skipped += 1
                        continue

                    org_id      = get(row, colmap, "org_id")
                    entity_type = get(row, colmap, "entity_type")
                    cand_name   = person_name(get(row, colmap, "cand_last"),
                                              get(row, colmap, "cand_first"),
                                              get(row, colmap, "cand_middle"),
                                              get(row, colmap, "cand_suffix"))
                    cmte_col    = utils.clean_name(get(row, colmap, "committee_name"))
                    cmte_name   = cmte_col or cand_name
                    elec_year   = year_from_text(get(row, colmap, "election")) or file_year

                    note_org(org_id, cmte_col, cand_name, entity_type,
                             elec_year, path.name, row_num)

                    # committee_name is tier-1 required; recover it from a
                    # previously seen row for the same Org Id before giving up.
                    if not cmte_name:
                        prior = orgs.get(org_id, {})
                        cmte_name = prior.get("committee_name") or prior.get("candidate_name", "")
                    if not cmte_name:
                        skipped += 1
                        continue

                    contributor = person_name(get(row, colmap, "last_name"),
                                              get(row, colmap, "first_name"),
                                              get(row, colmap, "middle_name"),
                                              get(row, colmap, "suffix"))
                    ctx        = candidate_context(cand_name)
                    txn_type   = get(row, colmap, "contribution_type")
                    tx_date    = parse_date(get(row, colmap, "date"))
                    amended    = amended_flag(get(row, colmap, "amended"))
                    txn_id     = get(row, colmap, "transaction_id")

                    if txn_type.strip().lower() in LOAN_TYPES:
                        loan_w.writerow({
                            "state":               STATE,
                            "committee_name":      cmte_name,
                            "original_amount":     amount,
                            "date":                tx_date,
                            "record_type":         txn_type or "Loan Received",
                            "counterparty_name":   contributor,
                            "counterparty_city":   get(row, colmap, "city"),
                            "counterparty_state":  get(row, colmap, "contrib_state"),
                            "counterparty_zip":    utils.clean_zip(get(row, colmap, "zip")),
                            "candidate_name":      cand_name,
                            "election_year":       elec_year,
                            "amended":             amended,
                            "filing_id":           txn_id,
                            "raw_file":            path.name,
                            "row_num":             row_num,
                        })
                        loans += 1
                        continue

                    # Occupation Comment holds the real answer when the filer
                    # picked "Other" off the occupation list, so a literal
                    # "Other" is replaced by the comment rather than kept.
                    occupation = get(row, colmap, "occupation")
                    comment    = get(row, colmap, "occupation_comment")
                    if comment and (not occupation or occupation.lower() == "other"):
                        occupation = comment

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cmte_name,
                        "amount":            amount,
                        "date":              tx_date,
                        "transaction_type":  txn_type,
                        "contributor_name":  contributor,
                        "contributor_type":  get(row, colmap, "contributor_code"),
                        "contributor_city":  get(row, colmap, "city"),
                        "contributor_state": get(row, colmap, "contrib_state"),
                        "contributor_zip":   utils.clean_zip(get(row, colmap, "zip")),
                        "employer":          utils.clean_name(get(row, colmap, "employer")),
                        "occupation":        occupation,
                        "candidate_name":    cand_name,
                        "office":            ctx.get("office", ""),
                        "election_year":     elec_year,
                        "amended":           amended,
                        "filing_id":         txn_id,
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "contributions", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size, loans=loans)
            total_contributions += count
            total_loans         += loans

        # ── Known candidate names, for the IE "Reason" match below ─────
        # Built after contributions so it covers both the API registry and every
        # candidate filer seen in the transaction data.
        known_candidates = set(api_candidates)
        known_candidates |= {norm_key(o["candidate_name"]) for o in orgs.values()
                             if o.get("candidate_name")}

        # ── Expenditures ───────────────────────────────────────────────
        ie_matched = ie_unmatched = 0

        for path in raw_files("expenditures_*.csv"):
            file_year = year_from_filename(path)
            ft = time.perf_counter()
            count = skipped = 0

            with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
                reader = csv.reader(f)
                try:
                    first = next(reader)
                except StopIteration:
                    continue

                colmap, has_header = build_column_map(first, EXP_POSITIONS)
                rows = reader if has_header else _chain(first, reader)
                start = 2 if has_header else 1

                for row_num, row in enumerate(rows, start=start):
                    if not row or not any(row):
                        continue

                    amount = parse_amount(get(row, colmap, "amount"))
                    if not amount:
                        skipped += 1
                        continue

                    org_id      = get(row, colmap, "org_id")
                    entity_type = get(row, colmap, "entity_type")
                    cand_name   = person_name(get(row, colmap, "cand_last"),
                                              get(row, colmap, "cand_first"),
                                              get(row, colmap, "cand_middle"),
                                              get(row, colmap, "cand_suffix"))
                    cmte_col    = utils.clean_name(get(row, colmap, "committee_name"))
                    cmte_name   = cmte_col or cand_name
                    elec_year   = year_from_text(get(row, colmap, "election")) or file_year

                    note_org(org_id, cmte_col, cand_name, entity_type,
                             elec_year, path.name, row_num)

                    if not cmte_name:
                        prior = orgs.get(org_id, {})
                        cmte_name = prior.get("committee_name") or prior.get("candidate_name", "")
                    if not cmte_name:
                        skipped += 1
                        continue

                    # Reason is a candidate name OR a ballot question — only
                    # promote it to affiliated_candidate_name when it matches a
                    # candidate we already know, so questions don't leak in.
                    reason   = get(row, colmap, "reason")
                    stance   = stance_flag(get(row, colmap, "stance"))
                    affiliated = ""
                    if reason and norm_key(reason) in known_candidates:
                        affiliated = utils.clean_name(reason)
                        ie_matched += 1
                    elif reason and stance:
                        ie_unmatched += 1

                    ctx = candidate_context(cand_name)

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "amount":           amount,
                        "date":             parse_date(get(row, colmap, "date")),
                        "transaction_type": get(row, colmap, "expenditure_type"),
                        "payee_name":       person_name(get(row, colmap, "last_name"),
                                                        get(row, colmap, "first_name"),
                                                        get(row, colmap, "middle_name"),
                                                        get(row, colmap, "suffix")),
                        # Purpose is CFIS's own picklist (with a free-text
                        # override when the filer picks "Other"), so it is the
                        # category; Description is the filer's narrative.
                        "purpose":          get(row, colmap, "description")
                                            or get(row, colmap, "purpose"),
                        "category":         get(row, colmap, "purpose"),
                        "payee_city":       get(row, colmap, "city"),
                        "payee_state":      get(row, colmap, "payee_state"),
                        "payee_zip":        utils.clean_zip(get(row, colmap, "zip")),
                        "candidate_name":   cand_name,
                        "office":           ctx.get("office", ""),
                        "election_year":    elec_year,
                        "affiliated_candidate_name": affiliated,
                        "support_oppose":   stance,
                        "amended":          amended_flag(get(row, colmap, "amended")),
                        "filing_id":        get(row, colmap, "transaction_id"),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count

        log.enrichment_summary(relation="expenditures",
                               field="affiliated_candidate_name",
                               matched=ie_matched, unmatched=ie_unmatched)

        # ── Candidates ─────────────────────────────────────────────────
        # One row per (Org Id) candidate filer, enriched from the API registry.
        # API-only candidates are written too, but only when the API gave us an
        # ID — state_filer_id is tier-1 required for NM.
        seen_cand_keys: set[str] = set()
        api_only_cands = api_only_dropped = 0

        for org_id, rec in orgs.items():
            cand_name = rec.get("candidate_name", "")
            if not cand_name:
                continue
            ctx = api_candidates.get(norm_key(cand_name), {})
            office   = ctx.get("office", "")
            district = ctx.get("district", "")
            elec_yr  = ctx.get("election_year") or rec.get("election_year", "")
            juris    = ctx.get("jurisdiction", "")
            if not juris:
                seat = (norm_key(office), norm_key(district), elec_yr)
                juris = offices_by_seat.get(seat, {}).get("jurisdiction", "")

            # Split "Last, First Middle" back out for the first/last columns.
            last, _, rest = cand_name.partition(",")
            first = rest.strip().split(" ")[0] if rest.strip() else ""

            cand_w.writerow({
                "state":           STATE,
                "state_filer_id":  org_id,
                "candidate_name":  utils.clean_name(cand_name),
                "candidate_first": utils.clean_name(first),
                "candidate_last":  utils.clean_name(last),
                "office":          office,
                "district":        district,
                "jurisdiction":    juris,
                "party":           ctx.get("party", ""),
                "election_year":   elec_yr,
                # Present in the registry schema but empty on all 3,103 records
                # observed — kept wired up in case CFIS starts populating it.
                "incumbent":       ctx.get("incumbent", ""),
                "raw_file":        rec["raw_file"],
                "row_num":         rec["row_num"],
            })
            candidates_written += 1
            seen_cand_keys.add(norm_key(cand_name))

        for key, entry in api_candidates.items():
            if key in seen_cand_keys:
                continue
            if not entry["state_filer_id"]:
                api_only_dropped += 1
                continue
            name = entry["candidate_name"]
            last, _, rest = name.partition(",")
            first = rest.strip().split(" ")[0] if rest.strip() else ""
            juris = entry["jurisdiction"] or offices_by_seat.get(
                (norm_key(entry["office"]), norm_key(entry["district"]),
                 entry["election_year"]), {}).get("jurisdiction", "")
            cand_w.writerow({
                "state":           STATE,
                "state_filer_id":  entry["state_filer_id"],
                "candidate_name":  utils.clean_name(name),
                "candidate_first": utils.clean_name(first),
                "candidate_last":  utils.clean_name(last),
                "office":          entry["office"],
                "district":        entry["district"],
                "jurisdiction":    juris,
                "party":           entry["party"],
                "election_year":   entry["election_year"],
                "incumbent":       entry["incumbent"],
                "raw_file":        entry["raw_file"],
                "row_num":         entry["row_num"],
            })
            candidates_written += 1
            api_only_cands += 1

        # ── Committees ─────────────────────────────────────────────────
        seen_cmte_keys: set[str] = set()
        api_only_cmtes = api_only_cmte_dropped = 0

        for org_id, rec in orgs.items():
            cmte_name = rec.get("committee_name") or rec.get("candidate_name", "")
            if not cmte_name:
                continue
            ctx = api_committees.get(norm_key(cmte_name), {})
            cand_name = rec.get("candidate_name", "")
            cmte_w.writerow({
                "state":          STATE,
                "state_filer_id": org_id,
                "committee_name": utils.clean_name(cmte_name),
                # Prefer the registry's subtype (Independent Expenditure, Party,
                # …); fall back to the filer type the transaction file reports.
                "committee_type": ctx.get("committee_type")
                                  or rec.get("committee_type", ""),
                "election_year":  ctx.get("election_year")
                                  or rec.get("election_year", ""),
                "candidate_name": utils.clean_name(cand_name) if cand_name else "",
                # Present in the registry schema but null on every record
                # observed — wired up in case CFIS starts populating it.
                "treasurer_name": ctx.get("treasurer_name", ""),
                "city":           "",
                "zip":            "",
                "active":         ctx.get("active", ""),
                "raw_file":       rec["raw_file"],
                "row_num":        rec["row_num"],
            })
            committees_written += 1
            seen_cmte_keys.add(norm_key(cmte_name))

        for key, entry in api_committees.items():
            if key in seen_cmte_keys:
                continue
            if not entry["state_filer_id"]:
                api_only_cmte_dropped += 1
                continue
            cmte_w.writerow({
                "state":          STATE,
                "state_filer_id": entry["state_filer_id"],
                "committee_name": utils.clean_name(entry["committee_name"]),
                "committee_type": entry["committee_type"],
                "election_year":  entry["election_year"],
                "candidate_name": utils.clean_name(entry["candidate_name"])
                                  if entry["candidate_name"] else "",
                "treasurer_name": entry["treasurer_name"],
                "city":           "",
                "zip":            "",
                "active":         entry["active"],
                "raw_file":       entry["raw_file"],
                "row_num":        entry["row_num"],
            })
            committees_written += 1
            api_only_cmtes += 1

        log.enrichment_summary(relation="candidates", field="state_filer_id",
                               from_transactions=candidates_written - api_only_cands,
                               from_registry=api_only_cands,
                               registry_dropped_no_id=api_only_dropped)
        log.enrichment_summary(relation="committees", field="state_filer_id",
                               from_transactions=committees_written - api_only_cmtes,
                               from_registry=api_only_cmtes,
                               registry_dropped_no_id=api_only_cmte_dropped)

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # Org Id is a reporting-entity registration, and NM candidates register
        # per cycle, so the same person can hold several — collapse to the
        # earliest ID within (name, office, district).
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
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_contributions:,} contributions, "
                 f"{total_expenditures:,} expenditures, {total_loans:,} loans, "
                 f"{candidates_written:,} candidates, {committees_written:,} committees")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans,
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


def _chain(first_row, reader):
    """Re-emit an already-consumed first row ahead of the rest of the reader.

    Needed because header detection has to look at row 1 before deciding whether
    it is data — when it turns out to be data, it still has to be parsed.
    """
    yield first_row
    yield from reader


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
