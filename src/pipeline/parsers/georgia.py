"""
parsers/georgia.py — Transform Georgia raw CSVs into the 5 normalized relations.

Input:  data/Georgia/raw/
  contributions_{year}.csv  — TCON transactions from Peachfile API (2025–present)
  expenditures_{year}.csv   — TEXP transactions from Peachfile API (2025–present)
  candidates.csv            — all filer registrations from GetCandidateDetails
  committees.csv            — non-candidate rows from GetCandidateDetails. Normally ABSENT:
                              that endpoint returns candidate registrations only, so the
                              scraper writes no file. Parsed if present (the scraper warns
                              when it appears); real committees are in public_committees.csv
  public_committees.csv     — PACs/party/leadership/independent/ballot-question committees
                              from GetCommitteeDetails, deduped against candidates.csv

Output: data/Georgia/cleaned/
  contributions.csv.gz, expenditures.csv.gz, candidates.csv.gz,
  committees.csv.gz, loans_debts.csv.gz

Notes
─────
  • ZIP codes arrive as Excel formula strings: ="30339" — strip the ="…" wrapper.
  • Amounts arrive as "$1,000.00" — dollar sign and commas stripped.
  • Transaction Type routing in contributions files:
      Contribution / Return Contribution → contributions.csv
      Loan Received / Loan Payment / Loan Forgiven → loans_debts.csv
      Interest Earned (Non-Investment Account) → contributions.csv (as income)
  • Contributor name: organizations have only Contributor Last Name set;
    individuals have both Last and First populated.
  • Expenditure CSV headers have trailing spaces on some columns — stripped.
  • Independent Expenditure rows that name candidates/measures are split across
    repeated Transaction ID rows: one "parent" row carries the Transaction
    Amount (target column empty), and one "target" row per candidate/measure
    (with Stance) carries no amount of its own. We keep the PARENT row — it
    holds the money — drop the target rows, and fold their names into
    affiliated_candidate_name ("; "-joined when several) with support_oppose
    set only where every target shares a stance. See load_ie_targets().
    The amount is never divided: Georgia doesn't disclose how a buy splits
    across the candidates it names. candidate_name stays the SPENDER
    throughout, per columns.py.
  • All 1,266 candidates.csv rows have candidateLastName set, i.e. every row is a
    candidate registration; filerStatusCode distinguishes active (FACT) from
    terminated (TERMN) registrations. ~934 of these rows also carry a
    committeeName (their named campaign committee) — those rows are written to
    BOTH candidates.csv.gz and committees.csv.gz (linked by candidate_name) so
    assign_committee_person_ids() can map person_id onto the committee.
  • public_committees.csv (GetCommitteeDetails) uses committeeMailing* instead of
    candidateMailing*, and filerType gives a granular committee type (Political
    Action Committee, Political Party Committee, Leadership Committee,
    Independent Committee, Individual Committee / Other, County or Municipal
    Ballot Question Committee) — written as committee_type instead of the
    generic "PAC" used for candidates.csv PAC rows. candidate_name is left
    blank, so assign_committee_person_ids() leaves person_id empty for these.
  • person_id model: "committee" — filerRegistrationId is per-cycle, not person-level.
  • Filing Entity ID in transactions = filerEntityId in candidates.csv — used for
    registry enrichment (office, party, district).
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

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Georgia" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Georgia" / "cleaned"

# Independent Expenditure rows that name a candidate/measure use this column.
IE_TARGET_COL = "Candidate/Measure Mentioned in the Independent Expenditure"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "GA"
MAX_VALID_YEAR = date.today().year + 2


# ============================== helpers ===============================

def clean(val) -> str:
    return (val or "").strip()


# Qualifiers the legacy site appends to its office label, e.g.
#   "State Representative District: 107"
#   "Councilman District: 1 Post: 2 City:Butler"
#   "Judge Superior Court Circuit: Eastern"
#   "Mayor City:Fargo"                      (note: no space after the colon)
# Split into two groups because they land in different schema columns.
LEGACY_SUBDISTRICT_QUALIFIERS = ("District", "Ward", "Post", "Division")
LEGACY_JURISDICTION_QUALIFIERS = ("County", "City", "Circuit")

_LEGACY_OFFICE_RE = re.compile(
    r"\b(" + "|".join(LEGACY_SUBDISTRICT_QUALIFIERS + LEGACY_JURISDICTION_QUALIFIERS)
    + r")\s*:\s*"
)


def split_legacy_office(raw: str) -> tuple[str, str, str]:
    """Split a legacy office string into (office, district, jurisdiction).

    The legacy portal stores one combined string where Peachfile has three
    separate fields, so `office` arrived looking like
    "State Representative District: 107" while `district` and `jurisdiction`
    sat empty. Two consequences, both of which this fixes:

      * `assign_person_ids()` groups by (state, candidate_name, office,
        district). A candidate's pre-2025 legacy registrations therefore never
        unified with their 2025+ Peachfile ones — same person, different
        person_id. This was the "legacy/Peachfile person_id split" recorded as
        a known limitation in docs/states/georgia.md.
      * office_types.csv maps the bare label ("GA,State Representative"), so
        every district-suffixed value missed the alias table and had to be
        rescued by LIKE patterns in aggregate.py.

    Peachfile's convention is the target: office="State Representative",
    districtName="167", jurisdiction="" — the district TYPE is not kept, just
    its value. This mirrors that, so the two sources now agree.

    Multiple sub-district qualifiers are preserved rather than collapsed:
    "District: 1 Post: 1" becomes district="1 Post 1", because District 1
    Post 1 and District 1 Post 2 are genuinely different seats and flattening
    both to "1" would merge two people under one person_id.

    >>> split_legacy_office("State Representative District: 107")
    ('State Representative', '107', '')
    >>> split_legacy_office("Councilman District: 1 Post: 2 City:Butler")
    ('Councilman', '1 Post 2', 'Butler')
    >>> split_legacy_office("Judge Superior Court Circuit: Eastern")
    ('Judge Superior Court', '', 'Eastern')
    >>> split_legacy_office("County Commission Chair County: Clayton")
    ('County Commission Chair', '', 'Clayton')
    >>> split_legacy_office("UNSP")
    ('UNSP', '', '')
    """
    raw = clean(raw)
    if not raw:
        return "", "", ""

    matches = list(_LEGACY_OFFICE_RE.finditer(raw))
    if not matches:
        return raw, "", ""

    office = raw[:matches[0].start()].strip()
    found: dict[str, str] = {}
    for i, m in enumerate(matches):
        end   = matches[i + 1].start() if i + 1 < len(matches) else len(raw)
        value = raw[m.end():end].strip().strip(",")
        if value and m.group(1) not in found:
            found[m.group(1)] = value

    # "Consolidated Government (City/ County: MACON-BIBB ..." — the label itself
    # contains "City/" and the site's own "County:" is part of that label, not a
    # qualifier. 16 rows; repair the label rather than leave it dangling.
    if office.endswith("(City/"):
        office = office + "County)"

    sub = [(q, found[q]) for q in LEGACY_SUBDISTRICT_QUALIFIERS if q in found]
    if not sub:
        district = ""
    else:
        district = sub[0][1]
        for qual, val in sub[1:]:
            district = f"{district} {qual} {val}"

    jurisdiction = next(
        (found[q] for q in LEGACY_JURISDICTION_QUALIFIERS if q in found), ""
    )
    return office.strip(), district.strip(), jurisdiction.strip()


def amended_flag(val) -> str:
    """Normalize Peachfile's Y/N amendment flag to the schema's 0/1.

    columns.py types `amended` as a boolean int, and validate.py's
    BOOL_INT_FIELDS checks it accepts only "0", "1" or "". Georgia was passing
    the source's raw "N"/"Y" straight through, so the check failed on nearly
    every row it saw — 416,167 contributions and 80,523 expenditures — which
    buried any real warning under noise that could never be actioned.

    Matches the amended_flag() helpers in the Arkansas, Colorado, New Mexico
    and West Virginia parsers. Unrecognized values return "" (unknown) rather
    than guessing a boolean.
    """
    v = (val or "").strip().upper()
    if v in ("Y", "YES", "1", "TRUE"):
        return "1"
    if v in ("N", "NO", "0", "FALSE"):
        return "0"
    return ""


def nul_free(lines):
    """Yield lines with NUL bytes removed.

    contributions_2026.csv contains a stray NUL. Python's csv module refuses it
    outright (`_csv.Error: line contains NUL`), so any reader that touches that
    file dies on it. The contributions block currently survives only because of
    how it happens to read the file; that's luck, not design, and a NUL is never
    meaningful data in a disclosure CSV. Strip it at the door for every reader.
    """
    for line in lines:
        yield line.replace("\x00", "") if "\x00" in line else line


# Columns that sit after the free-text description in Peachfile's transaction
# CSVs. An unbalanced quote in that description shifts every later column one
# place right (see strip_keys), so when a row overflows the header these values
# are misaligned and must not be trusted. Blanked rather than written through:
# "" says "unknown", whereas a shifted value claims to be something it isn't.
UNRELIABLE_AFTER_OVERFLOW = (
    "Amended", "Timed Report Name", "Timed Report Filed Date",
    "Report Name", "Report Filed Date",
)


def strip_keys(row: dict) -> tuple[dict, list | None]:
    """Normalize a csv.DictReader row's keys, tolerating over-long rows.

    Two separate problems with Peachfile's transaction CSVs are handled here:

    1. Some expenditure header names carry trailing spaces ("Filing Entity Name "),
       so every key is stripped.
    2. A source row with MORE fields than the header puts the overflow under
       DictReader's `restkey`, which defaults to None. `None.strip()` then raises
       `AttributeError: 'NoneType' object has no attribute 'strip'`, and because
       this runs inside the parse's try/except, ONE bad row aborted the entire
       Georgia parse — every expenditure, candidate and committee after it was
       silently skipped.

    Real example, transaction 398177 in expenditures_2026.csv: the filer typed a
    stray double-quote into the free-text description ('...Democrats Defending
    Democary" via the actblue link...'). That unbalanced quote splits the
    description in two and shifts every later column one place right, pushing
    "Report Filed Date" past the end of the header.

    Only the tail is corrupted — Filing Entity, Transaction ID/Type, Payee,
    Transaction Date, Amount and Election Year all sit BEFORE the break and stay
    correct — so the row is kept rather than dropped; discarding it would lose a
    real $2,880 expenditure. The shifted tail lands in `amended`, which is a
    passthrough field. The overflow itself is unrecoverable and returned to the
    caller so it can be counted and reported instead of vanishing.
    """
    overflow = row.get(None)
    return {k.strip(): v for k, v in row.items() if k is not None}, overflow


def clean_zip(val: str) -> str:
    """Strip Excel formula wrapper: =\"30339\" → 30339."""
    v = (val or "").strip()
    if v.startswith('="') and v.endswith('"'):
        v = v[2:-1]
    return v


def parse_amount(val: str) -> str:
    """'$1,000.00' → '1000.00', '' on failure."""
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
    """MM/DD/YYYY, YYYY-MM-DD, or legacy 'M/D/YYYY h:mm:ss AM/PM' → YYYY-MM-DD,
    '' on failure or implausible year."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def parse_recordsearch_date(val: str) -> str:
    """recordsearch dates arrive as 'YYYY-MM-DD HH:MM:SS.fffffff' or ISO
    'YYYY-MM-DDTHH:MM:SS' — take the first 10 chars (YYYY-MM-DD) and run
    through parse_date() for year-range validation."""
    v = (val or "").strip()
    if len(v) < 10:
        return ""
    return parse_date(v[:10])


def recordsearch_candidate_name(row: dict) -> str:
    """Build candidate display name from candidateLastName/First/Middle,
    falling back to filerName for non-candidate (PAC/committee) filers."""
    last = clean(row.get("candidateLastName"))
    if last:
        first = clean(row.get("candidateFirstName"))
        mid   = clean(row.get("candidateMiddleName"))
        name = f"{last}, {first}" if first else last
        if first and mid:
            name += f" {mid}"
        return utils.clean_name(name)
    return utils.clean_name(clean(row.get("filerName")))


# ===================== recordsearch dedup helpers =====================
#
# recordsearch.ethics.ga.gov is overwhelmingly net-new data relative to the
# Peachfile + legacy sources already parsed above (~0.4% overlap for 2022,
# ~79% for 2025 in spot-checks — see docs/states/georgia.md). The small
# overlap is still de-duplicated via a composite key: (date, amount) plus a
# stopword-filtered significant-token overlap between the two rows' names
# (contributor/payee vs committee/filer), checked in either direction.

STOP_WORDS = {
    "the", "committee", "to", "elect", "of", "for", "inc", "llc", "corp",
    "corporation", "company", "co", "pac", "association", "assn", "fund",
    "friends", "citizens", "georgia", "ga", "state", "house", "senate",
    "political", "action", "leadership", "first", "retail", "automobile",
    "dealers", "cmte",
}

_TOKEN_RE = re.compile(r"[^a-z0-9]+")


def _significant_tokens(*names: str) -> frozenset[str]:
    """Lowercase, strip punctuation, and keep tokens of length >= 3 that
    aren't in STOP_WORDS, across all provided name strings."""
    tokens = set()
    for name in names:
        if not name:
            continue
        for tok in _TOKEN_RE.sub(" ", name.lower()).split():
            if len(tok) >= 3 and tok not in STOP_WORDS:
                tokens.add(tok)
    return frozenset(tokens)


def _dedup_key(date_str: str, amount_str: str) -> tuple[str, str] | None:
    """(date, amount) composite key, normalized to 2-decimal amount.
    Returns None if either field is missing/unparseable — such rows are
    never indexed and never matched (always treated as net-new)."""
    if not date_str or not amount_str:
        return None
    try:
        return (date_str, f"{float(amount_str):.2f}")
    except ValueError:
        return None


def _index_row(index: dict, date_str: str, amount_str: str, *names: str):
    """Add a parsed contribution/expenditure row's (date, amount) -> name
    tokens to a dedup index, for later matching against recordsearch rows."""
    key = _dedup_key(date_str, amount_str)
    if key is None:
        return
    tokens = _significant_tokens(*names)
    if tokens:
        index[key].append(tokens)


def _is_duplicate(index: dict, date_str: str, amount_str: str, *names: str) -> bool:
    """True if a recordsearch row with this date/amount/name plausibly
    matches an already-indexed Peachfile/legacy row (shares >=1 significant
    token in either direction)."""
    key = _dedup_key(date_str, amount_str)
    if key is None or key not in index:
        return False
    tokens = _significant_tokens(*names)
    if not tokens:
        return False
    return any(tokens & existing for existing in index[key])


def contributor_name(row: dict) -> str:
    """Build contributor display name: 'Last, First Middle' or just 'Last' for orgs."""
    last  = clean(row.get("Contributor Last Name"))
    first = clean(row.get("Contributor First Name"))
    mid   = clean(row.get("Contributor Middle Name"))
    if first:
        name = f"{last}, {first}"
        if mid:
            name += f" {mid}"
        return name
    return last


def payee_name(row: dict) -> str:
    """Build payee display name from Last/First or just Last for orgs."""
    last  = clean(row.get("Payee Last Name"))
    first = clean(row.get("Payee First Name"))
    mid   = clean(row.get("Payee Middle Name"))
    if first:
        name = f"{last}, {first}"
        if mid:
            name += f" {mid}"
        return name
    return last


def legacy_candidate_name(row: dict) -> str:
    """Build 'Last, First Middle Suffix' from legacy Candidate_* columns, '' if no last name."""
    last   = clean(row.get("Candidate_LastName"))
    first  = clean(row.get("Candidate_FirstName"))
    mid    = clean(row.get("Candidate_MiddleName"))
    suffix = clean(row.get("Candidate_Suffix"))
    if not last:
        return ""
    if not first:
        return last
    name = f"{last}, {first}"
    if mid:
        name += f" {mid}"
    if suffix:
        name += f" {suffix}"
    return utils.clean_name(name)


def legacy_payee_name(row: dict) -> str:
    """Build payee display name from legacy LastName/FirstName columns."""
    last  = clean(row.get("LastName"))
    first = clean(row.get("FirstName"))
    if first:
        return f"{last}, {first}"
    return last


def legacy_contributor_info(row: dict) -> tuple[str, str]:
    """
    Return (contributor_name, contributor_type) for a legacy contribution/loan row.

    Legacy data has no single "Contributor Type" field. The "PAC" column holds
    the contributing committee's name when the contributor is a non-candidate
    committee; otherwise LastName/FirstName identify an individual (FirstName
    set) or an organization (FirstName blank, LastName set). All-blank rows
    return ("", "") — analogous to Peachfile's unitemized contribution rows.
    """
    pac = clean(row.get("PAC"))
    if pac:
        return pac, "Non-Candidate Committee"
    last  = clean(row.get("LastName"))
    first = clean(row.get("FirstName"))
    if first:
        return f"{last}, {first}", "Individual"
    if last:
        return last, "Corporation / Business / Unregistered Committee"
    return "", ""


# Legacy filer IDs encode the registration/filing year right after the
# "C"/"NC" prefix, e.g. "C2011004358" -> 2011, "NC2010000038" -> 2010.
LEGACY_FILER_ID_YEAR_RE = re.compile(r"^[A-Za-z]+(\d{4})\d+$")


def raw_files(pattern: str) -> list[Path]:
    """Glob RAW_DIR for pattern, returning non-empty files sorted by name."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def load_ie_targets(path: Path) -> dict[str, list[tuple[str, str]]]:
    """
    Pre-scan an expenditures CSV, collecting each Independent Expenditure's targets.

    When an IE names candidates/measures, Georgia repeats the same Transaction ID
    across several rows: one "parent" row carries the Transaction Amount (with
    IE_TARGET_COL empty), and one "target" row per candidate/measure mentioned
    (IE_TARGET_COL + Stance set) carrying no amount of its own.

    The parser keeps the PARENT row — it holds the money — and folds the targets
    into it via affiliated_candidate_name/support_oppose (see the expenditures
    block). The target rows themselves are dropped.

    This replaces an earlier approach that dropped the parent and copied its
    amount onto every target row. That multiplied each IE by its target count:
    one $2,981,816 buy naming 21 candidates became $62,618,136, and Georgia's
    2026 expenditures overstated by $118,337,800 in total. The amount must
    appear exactly once, and columns.py's affiliated_candidate_name /
    support_oppose exist precisely to carry the targeting alongside it.

    Returns: Transaction ID -> [(target name, raw stance), ...] in file order.
    """
    targets: dict[str, list[tuple[str, str]]] = defaultdict(list)
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(nul_free(f))
        reader.fieldnames = [k.strip() for k in (reader.fieldnames or [])]
        for row in reader:
            row, _ = strip_keys(row)   # malformed rows reported by the main pass
            if clean(row.get("Transaction Type")) != "Independent Expenditure":
                continue
            txid = clean(row.get("Transaction ID"))
            target = clean(row.get(IE_TARGET_COL))
            if txid and target:
                targets[txid].append((target, clean(row.get("Stance"))))
    return dict(targets)


def ie_stance_code(stances: list[str]) -> str:
    """Collapse an IE's per-target stances into one "S"/"O" code.

    Only returned when every target shares the same stance. Georgia does file
    mixed transactions — a single buy supporting one candidate while opposing
    another (28 of 97 multi-target IEs) — and no single code is honest for
    those, so they get "" rather than a forced value.
    """
    codes = {s[:1].upper() for s in stances if s}
    return codes.pop() if len(codes) == 1 and codes <= {"S", "O"} else ""


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ============================= registry ==============================

def build_registry() -> dict[str, dict]:
    """
    Load candidates.csv into a dict keyed by filerEntityId.
    Used to enrich transactions with office, party, district, candidate name.
    """
    registry = {}
    cand_path = RAW_DIR / "candidates.csv"
    if not cand_path.exists():
        return registry
    with open(cand_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = clean(row.get("filerEntityId"))
            if eid:
                registry[eid] = row
    return registry


def build_legacy_registry() -> dict[str, dict]:
    """
    Load legacy_candidates_{A-Z}.csv and legacy_committees_type{1-8}.csv into a
    dict keyed by filer_id / FilerID (e.g. "C2011004358", "NC2010000038").

    Used to enrich legacy transaction rows when their own Candidate_*/
    Committee_Name columns are blank (common for non-candidate-committee
    filers, whose transactions carry no Candidate_* info):
      - candidates ("C..."):  candidate_name, office/district/jurisdiction
                              (the site's combined string, split apart by
                              split_legacy_office)
      - committees ("NC..."): committee_name, committee_type
    First occurrence of a given filer_id wins.
    """
    registry: dict[str, dict] = {}

    for path in raw_files("legacy_candidates_*.csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                fid = clean(row.get("filer_id"))
                if not fid or fid in registry:
                    continue
                l_office, l_district, l_jurisdiction = split_legacy_office(
                    row.get("office"))
                registry[fid] = {
                    "candidate_name": utils.clean_name(row.get("candidate_name", "")),
                    # Split, not raw: legacy transaction rows take `office` from
                    # here, so leaving the combined string would keep
                    # "State Representative District: 107" in contributions and
                    # expenditures even after candidates.csv.gz was fixed.
                    "office":         l_office,
                    "district":       l_district,
                    "jurisdiction":   l_jurisdiction,
                    "committee_name": "",
                    "committee_type": "",
                }

    for path in raw_files("legacy_committees_type*.csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                fid = clean(row.get("FilerID"))
                if not fid or fid in registry:
                    continue
                registry[fid] = {
                    "candidate_name": "",
                    "office":         "",
                    "committee_name": clean(row.get("CommitteeName")),
                    "committee_type": clean(row.get("CommitteeType")),
                }

    return registry


def build_legacy_committee_name_lookup() -> dict[str, str]:
    """
    Scan all legacy transaction files (contributions, loans, expenditures) for
    FilerID -> Committee_Name, first non-blank Committee_Name per filer_id wins.

    legacy_registry has no committee_name for "C..." (candidate) filer IDs —
    legacy_candidates_*.csv carries no committee field at all. But many of a
    candidate's own transaction rows DO carry a Committee_Name (their campaign
    committee, e.g. "Carter for Governor, Inc."); other rows for the same
    filer_id leave it blank. This lookup recovers that committee name so it
    can be used to fill in committee_name on rows where it's otherwise blank.
    """
    lookup: dict[str, str] = {}
    patterns = [
        "legacy_contributions_monetary_*.csv",
        "legacy_contributions_in-kind_*.csv",
        "legacy_contributions_loan_*.csv",
        "legacy_expenditures_expenditure_*.csv",
        "legacy_expenditures_reimbursement_*.csv",
        "legacy_expenditures_credit_card_*.csv",
        "legacy_expenditures_in-kind_*.csv",
    ]
    for pattern in patterns:
        for path in raw_files(pattern):
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    fid   = clean(row.get("FilerID"))
                    cname = clean(row.get("Committee_Name"))
                    if fid and cname and fid not in lookup:
                        lookup[fid] = cname

    return lookup


# ============================== run ==================================

def run():
    log = get_logger("georgia", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    candidates_written  = 0
    committees_written  = 0
    file_handles        = []

    # recordsearch net-new rows + rows skipped as plausible duplicates of
    # Peachfile/legacy data (subsets of total_contributions/total_expenditures)
    total_recordsearch_contributions = 0
    total_recordsearch_expenditures  = 0
    total_recordsearch_dupes_contrib = 0
    total_recordsearch_dupes_expn    = 0

    # Dedup indexes built from Peachfile + legacy rows as they're written,
    # then used to de-duplicate recordsearch rows against them (see
    # "recordsearch dedup helpers" above).
    contrib_index = defaultdict(list)
    expn_index    = defaultdict(list)

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, cand_fh, cmte_fh, loan_fh]

        # ---------------------------------------------------------------- #
        # registry                                                          #
        # ---------------------------------------------------------------- #
        registry = build_registry()
        log.registry_loaded("candidates.csv", entries=len(registry),
                            relation="transactions")

        legacy_registry = build_legacy_registry()
        log.registry_loaded("legacy_candidates+committees", entries=len(legacy_registry),
                            relation="legacy_transactions")

        legacy_committee_names = build_legacy_committee_name_lookup()
        log.registry_loaded("legacy_committee_names_by_filer_id", entries=len(legacy_committee_names),
                            relation="legacy_transactions")

        # ---------------------------------------------------------------- #
        # contributions                                                     #
        # ---------------------------------------------------------------- #
        for path in raw_files("contributions_*.csv"):
            ft = time.perf_counter()
            file_rows = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    tx_type = clean(row.get("Transaction Type"))
                    sub_type = clean(row.get("Transaction Sub Type"))
                    entity_id = clean(row.get("Filing Entity ID"))
                    reg = registry.get(entity_id, {})

                    amount = parse_amount(row.get("Transaction Amount"))
                    # Return Contribution rows are already signed correctly
                    # in the source; no sign flip needed.

                    base = {
                        "state":          STATE,
                        "committee_name": (clean(row.get("Campaign Committee Name"))
                                           or clean(row.get("Filing Entity Name"))),
                        "amount":         amount,
                        "date":           parse_date(row.get("Transaction Date")),
                        "transaction_type": f"{tx_type}" + (f" – {sub_type}" if sub_type else ""),
                        "election_year":  clean(row.get("Election Year")),
                        "amended":        amended_flag(row.get("Amended")),
                        "filing_id":      clean(row.get("Transaction Id")),
                        "raw_file":       path.name,
                        "row_num":        row_num,
                        # from registry
                        "candidate_name": utils.clean_name(
                            reg.get("filerName", "")
                            or clean(row.get("Filing Entity Name"))
                        ),
                        "office":         clean(reg.get("office")),
                    }

                    if tx_type in ("Loan Received", "Loan Payment", "Loan Forgiven"):
                        loan_w.writerow({
                            **base,
                            "loan_type":       tx_type,
                            "lender_name":     contributor_name(row),
                            "lender_city":     clean(row.get("Contributor Address City")),
                            "lender_state":    clean(row.get("Contributor Address State")),
                            "lender_zip":      clean_zip(row.get("Contributor Address Zip Code", "")),
                        })
                        total_loans += 1

                    else:
                        cont_nm = contributor_name(row)
                        cont_w.writerow({
                            **base,
                            "contributor_name":  cont_nm,
                            "contributor_type":  clean(row.get("Contributor Type")),
                            "contributor_city":  clean(row.get("Contributor Address City")),
                            "contributor_state": clean(row.get("Contributor Address State")),
                            "contributor_zip":   clean_zip(row.get("Contributor Address Zip Code", "")),
                            "occupation":        clean(row.get("Contributor/Person Responsible for Loan Occupation")),
                            "employer":          clean(row.get("Contributor/Person Responsible for Loan Employer")),
                        })
                        total_contributions += 1
                        _index_row(contrib_index, base["date"], base["amount"],
                                  cont_nm, base["committee_name"])

                    file_rows += 1

            log.file_parsed(path.name, "contributions", file_rows,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # legacy contributions (monetary / in-kind) — media.ethics.ga.gov   #
        # ---------------------------------------------------------------- #
        for path in (raw_files("legacy_contributions_monetary_*.csv")
                     + raw_files("legacy_contributions_in-kind_*.csv")):
            ft = time.perf_counter()
            file_rows = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    sub_type = clean(row.get("Type"))  # "Monetary" or "In-Kind"
                    filer_id = clean(row.get("FilerID"))
                    reg = legacy_registry.get(filer_id, {})

                    if sub_type == "In-Kind":
                        amount = parse_amount(row.get("In_Kind_Amount"))
                    else:
                        amount = parse_amount(row.get("Cash_Amount"))

                    contributor_nm, contributor_type = legacy_contributor_info(row)
                    candidate_nm = legacy_candidate_name(row) or reg.get("candidate_name", "")
                    committee_nm = (clean(row.get("Committee_Name"))
                                   or reg.get("committee_name", "")
                                   or legacy_committee_names.get(filer_id, "")
                                   or candidate_nm)
                    date_str = parse_date(row.get("Date"))

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    committee_nm,
                        "amount":            amount,
                        "date":              date_str,
                        "transaction_type":  f"Contribution – {sub_type}",
                        "contributor_name":  contributor_nm,
                        "contributor_type":  contributor_type,
                        "contributor_city":  clean(row.get("City")),
                        "contributor_state": clean(row.get("State")),
                        "contributor_zip":   clean_zip(row.get("Zip", "")),
                        "employer":          clean(row.get("Employer")),
                        "occupation":        clean(row.get("Occupation")),
                        "candidate_name":    candidate_nm,
                        "office":            reg.get("office", ""),
                        "election_year":     clean(row.get("Election_Year")),
                        "amended":           "",
                        "filing_id":         "",
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    total_contributions += 1
                    file_rows += 1
                    _index_row(contrib_index, date_str, amount, contributor_nm, committee_nm)

            log.file_parsed(path.name, "legacy_contributions", file_rows,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # legacy loans/debts — media.ethics.ga.gov                          #
        # ---------------------------------------------------------------- #
        for path in raw_files("legacy_contributions_loan_*.csv"):
            ft = time.perf_counter()
            file_rows = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    record_type = clean(row.get("Type"))  # "Loan" or "Credit Received on Loan"
                    filer_id = clean(row.get("FilerID"))
                    reg = legacy_registry.get(filer_id, {})

                    amount = (parse_amount(row.get("Cash_Amount"))
                              or parse_amount(row.get("In_Kind_Amount")))

                    counterparty_nm, _ = legacy_contributor_info(row)
                    candidate_nm = legacy_candidate_name(row) or reg.get("candidate_name", "")

                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     (clean(row.get("Committee_Name"))
                                               or reg.get("committee_name", "")
                                               or legacy_committee_names.get(filer_id, "")
                                               or candidate_nm),
                        "original_amount":    amount,
                        "date":               parse_date(row.get("Date")),
                        "record_type":        record_type,
                        "counterparty_name":  counterparty_nm,
                        "counterparty_city":  clean(row.get("City")),
                        "counterparty_state": clean(row.get("State")),
                        "counterparty_zip":   clean_zip(row.get("Zip", "")),
                        "candidate_name":     candidate_nm,
                        "election_year":      clean(row.get("Election_Year")),
                        "amended":            "",
                        "filing_id":          "",
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    total_loans += 1
                    file_rows += 1

            log.file_parsed(path.name, "legacy_loans", file_rows,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # expenditures                                                      #
        # ---------------------------------------------------------------- #
        for path in raw_files("expenditures_*.csv"):
            ft = time.perf_counter()
            file_rows = 0
            file_skipped = 0
            file_malformed = 0

            ie_targets_by_txid = load_ie_targets(path)

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(nul_free(f))
                # Strip trailing spaces from header keys (present in expenditure CSVs)
                reader.fieldnames = [k.strip() for k in (reader.fieldnames or [])]

                for row_num, row in enumerate(reader, start=2):
                    row, overflow = strip_keys(row)
                    if overflow is not None:
                        file_malformed += 1
                        # Everything after the misquote is shifted — drop it
                        # rather than record a value that claims to be a field
                        # it isn't. See UNRELIABLE_AFTER_OVERFLOW.
                        for col in UNRELIABLE_AFTER_OVERFLOW:
                            row[col] = ""
                        if file_malformed <= 3:      # cap the noise
                            log.warning(
                                f"  {path.name} row {row_num} (txn "
                                f"{clean(row.get('Transaction ID')) or '?'}): "
                                f"{len(overflow)} field(s) past the header — "
                                f"misquoted source row, trailing columns dropped"
                            )
                    tx_type  = clean(row.get("Transaction Type"))
                    sub_type = clean(row.get("Transaction Sub Type"))
                    entity_id = clean(row.get("Filing Entity Id"))
                    reg = registry.get(entity_id, {})

                    if not tx_type:
                        continue

                    txid = clean(row.get("Transaction ID"))
                    ie_affiliated = ""
                    ie_support_oppose = ""
                    if tx_type == "Independent Expenditure":
                        if clean(row.get(IE_TARGET_COL)):
                            # "Target" row: carries a candidate/measure but no
                            # money of its own. The parent row below holds the
                            # amount and absorbs these names, so drop it here —
                            # writing it would duplicate the expenditure.
                            file_skipped += 1
                            continue

                        # "Parent" row — the one with the money. Fold in every
                        # target this transaction named. Multi-target IEs get a
                        # "; "-joined list (longest observed: 594 chars); the
                        # amount stays whole and is never divided, because
                        # Georgia does not disclose how a buy splits across the
                        # candidates it mentions.
                        pairs = ie_targets_by_txid.get(txid, [])
                        ie_affiliated = "; ".join(
                            utils.clean_name(t) for t, _ in pairs)
                        ie_support_oppose = ie_stance_code([s for _, s in pairs])

                    # Reimbursement and Credit Card rows often have a blank
                    # Transaction Amount — fall back to End Recipient Transaction Amount.
                    amount = (parse_amount(row.get("Transaction Amount"))
                              or parse_amount(row.get("End Recipient Transaction Amount")))

                    purpose = clean(row.get("Purpose"))

                    # Always the SPENDER, never the target — an IE committee
                    # spending on a candidate is not that candidate. The target
                    # belongs in affiliated_candidate_name (see columns.py).
                    candidate_name = utils.clean_name(
                        reg.get("filerName", "")
                        or clean(row.get("Filing Entity Name"))
                    )

                    expn_committee_nm = (clean(row.get("Campaign Committee Name"))
                                        or clean(row.get("Filing Entity Name")))
                    expn_date_str = parse_date(row.get("Transaction Date"))
                    expn_payee_nm = payee_name(row)

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   expn_committee_nm,
                        "amount":           amount,
                        "date":             expn_date_str,
                        "transaction_type": f"{tx_type}" + (f" – {sub_type}" if sub_type else ""),
                        "purpose":          purpose,
                        "payee_name":       expn_payee_nm,
                        "payee_city":       clean(row.get("Payee Address City")),
                        "payee_state":      clean(row.get("Payee Address State")),
                        "payee_zip":        clean_zip(row.get("Payee Address Zip Code", "")),
                        "election_year":    clean(row.get("Election Year")),
                        "amended":          amended_flag(row.get("Amended")),
                        "filing_id":        txid,
                        "raw_file":         path.name,
                        "row_num":          row_num,
                        "candidate_name":   candidate_name,
                        "office":           clean(reg.get("office")),
                        "affiliated_candidate_name": ie_affiliated,
                        "support_oppose":            ie_support_oppose,
                    })
                    total_expenditures += 1
                    file_rows += 1
                    _index_row(expn_index, expn_date_str, amount, expn_payee_nm, expn_committee_nm)

            if file_malformed > 3:
                log.warning(f"  {path.name}: {file_malformed:,} malformed rows total "
                            f"(first 3 shown)")

            log.file_parsed(path.name, "expenditures", file_rows,
                            skipped=file_skipped,
                            malformed=file_malformed,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # legacy expenditures — media.ethics.ga.gov                         #
        # (expenditure / reimbursement / credit_card / in-kind)             #
        # ---------------------------------------------------------------- #
        for path in (raw_files("legacy_expenditures_expenditure_*.csv")
                     + raw_files("legacy_expenditures_reimbursement_*.csv")
                     + raw_files("legacy_expenditures_credit_card_*.csv")
                     + raw_files("legacy_expenditures_in-kind_*.csv")):
            ft = time.perf_counter()
            file_rows = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    sub_type = clean(row.get("Type"))  # Expenditure/Reimbursement/Credit Card/In-Kind
                    filer_id = clean(row.get("FilerID"))
                    reg = legacy_registry.get(filer_id, {})

                    amount = parse_amount(row.get("Paid")) or parse_amount(row.get("Other"))
                    candidate_nm = legacy_candidate_name(row) or reg.get("candidate_name", "")
                    legacy_expn_committee_nm = (clean(row.get("Committee_Name"))
                                                or reg.get("committee_name", "")
                                                or legacy_committee_names.get(filer_id, "")
                                                or candidate_nm)
                    legacy_expn_date_str = parse_date(row.get("Date"))
                    legacy_expn_payee_nm = legacy_payee_name(row)

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   legacy_expn_committee_nm,
                        "amount":           amount,
                        "date":             legacy_expn_date_str,
                        "transaction_type": f"Expenditure – {sub_type}",
                        "purpose":          clean(row.get("Purpose")),
                        "payee_name":       legacy_expn_payee_nm,
                        "payee_city":       clean(row.get("City")),
                        "payee_state":      clean(row.get("State")),
                        "payee_zip":        clean_zip(row.get("Zip", "")),
                        "election_year":    clean(row.get("Election_Year")),
                        "amended":          "",
                        "filing_id":        clean(row.get("Key")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                        "candidate_name":   candidate_nm,
                        "office":           reg.get("office", ""),
                    })
                    total_expenditures += 1
                    file_rows += 1
                    _index_row(expn_index, legacy_expn_date_str, amount,
                              legacy_expn_payee_nm, legacy_expn_committee_nm)

            log.file_parsed(path.name, "legacy_expenditures", file_rows,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # recordsearch.ethics.ga.gov (GACFIS) — 4th GA source, 2014-present #
        # Net-new rows only: dedup against contrib_index/expn_index built   #
        # above from Peachfile + legacy rows (composite date+amount+token-  #
        # overlap key — see "recordsearch dedup helpers").                  #
        # ---------------------------------------------------------------- #
        for path in raw_files("recordsearch_contributions_*.csv"):
            ft = time.perf_counter()
            file_rows  = 0
            file_dupes = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    amount = parse_amount(row.get("transactionAmount"))
                    date_str = (parse_recordsearch_date(row.get("sortTransactionDate"))
                                or parse_recordsearch_date(row.get("transactionDate")))
                    category = clean(row.get("transactionCategory"))
                    committee_nm   = (clean(row.get("campaignCommittee"))
                                      or clean(row.get("filerName")))
                    contributor_nm = clean(row.get("sourceName"))

                    if _is_duplicate(contrib_index, date_str, amount,
                                     contributor_nm, committee_nm):
                        file_dupes += 1
                        total_recordsearch_dupes_contrib += 1
                        continue

                    reg = registry.get(clean(row.get("filerEntityId")), {})

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    committee_nm,
                        "amount":            amount,
                        "date":              date_str,
                        "transaction_type":  f"Contribution – {category}" if category else "Contribution",
                        "contributor_name":  contributor_nm,
                        "contributor_type":  clean(row.get("transactionSource")),
                        "contributor_city":  clean(row.get("transactionSourceCity")),
                        "contributor_state": clean(row.get("transactionSourceStateCode")),
                        "contributor_zip":   clean(row.get("transactionSourceZipcode")),
                        "employer":          clean(row.get("payeeEmployer")),
                        "occupation":        clean(row.get("payeeOccupation")),
                        "candidate_name":    (recordsearch_candidate_name(row)
                                              or utils.clean_name(reg.get("filerName", ""))),
                        "office":            clean(reg.get("office")),
                        "election_year":     clean(row.get("electionYear")),
                        "amended":           "",
                        "filing_id":         clean(row.get("transactionId")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    total_contributions += 1
                    total_recordsearch_contributions += 1
                    file_rows += 1

            log.file_parsed(path.name, "recordsearch_contributions", file_rows,
                            skipped=file_dupes,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        for path in raw_files("recordsearch_expenditures_*.csv"):
            ft = time.perf_counter()
            file_rows  = 0
            file_dupes = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    amount = parse_amount(row.get("transactionAmount"))
                    date_str = (parse_recordsearch_date(row.get("sortTransactionDate"))
                                or parse_recordsearch_date(row.get("transactionDate")))
                    category = clean(row.get("transactionCategory"))
                    committee_nm = (clean(row.get("campaignCommittee"))
                                   or clean(row.get("filerName")))
                    payee_nm = clean(row.get("sourceName"))

                    if _is_duplicate(expn_index, date_str, amount,
                                     payee_nm, committee_nm):
                        file_dupes += 1
                        total_recordsearch_dupes_expn += 1
                        continue

                    reg = registry.get(clean(row.get("filerEntityId")), {})
                    purpose = (clean(row.get("transactionPurposeDescription"))
                              or clean(row.get("description")))

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   committee_nm,
                        "amount":           amount,
                        "date":             date_str,
                        "transaction_type": f"Expenditure – {category}" if category else "Expenditure",
                        "purpose":          purpose,
                        "category":         category,
                        "payee_name":       payee_nm,
                        "payee_city":       clean(row.get("transactionSourceCity")),
                        "payee_state":      clean(row.get("transactionSourceStateCode")),
                        "payee_zip":        clean(row.get("transactionSourceZipcode")),
                        "election_year":    clean(row.get("electionYear")),
                        "amended":          "",
                        "filing_id":        clean(row.get("transactionId")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                        "candidate_name":   (recordsearch_candidate_name(row)
                                             or utils.clean_name(reg.get("filerName", ""))),
                        "office":           clean(reg.get("office")),
                    })
                    total_expenditures += 1
                    total_recordsearch_expenditures += 1
                    file_rows += 1

            log.file_parsed(path.name, "recordsearch_expenditures", file_rows,
                            skipped=file_dupes,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # candidates + committees                                           #
        # ---------------------------------------------------------------- #
        cand_path = RAW_DIR / "candidates.csv"
        if cand_path.exists():
            ft = time.perf_counter()
            with open(cand_path, newline="", encoding="utf-8") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    committee_name = clean(row.get("committeeName"))
                    last  = clean(row.get("candidateLastName"))
                    first = clean(row.get("candidateFirstName"))
                    mid   = clean(row.get("candidateMiddleName"))

                    full_name_parts = [last]
                    if first:
                        full_name_parts.append(first)
                    if mid:
                        full_name_parts.append(mid)
                    candidate_name = utils.clean_name(
                        clean(row.get("filerName")) or ", ".join(full_name_parts)
                    )

                    treasurer = ""
                    t_first = clean(row.get("treasurerFirstName"))
                    t_last  = clean(row.get("treasurerLastName"))
                    if t_last:
                        treasurer = f"{t_last}, {t_first}".strip(", ")

                    base = {
                        "state":            STATE,
                        "candidate_name":   candidate_name,
                        "candidate_first":  first,
                        "candidate_last":   last,
                        "office":           clean(row.get("office")),
                        "district":         clean(row.get("districtName")),
                        "jurisdiction":     clean(row.get("jurisdiction")),
                        "party":            clean(row.get("partyAffiliation")),
                        "election_year":    (clean(row.get("electionCycleName")) or "")[:4] or "",
                        "state_filer_id":   clean(row.get("filerRegistrationId")),
                        "raw_file":         cand_path.name,
                        "row_num":          row_num,
                    }

                    # Every row in candidates.csv has a candidateLastName, i.e. is a
                    # candidate registration. Rows that also carry a committeeName
                    # additionally have a named campaign committee — write those to
                    # BOTH candidates.csv.gz and committees.csv.gz (linked by
                    # candidate_name) so assign_committee_person_ids() can match the
                    # committee to its candidate's person_id.
                    cand_w.writerow({
                        **base,
                        "incumbent": "",
                    })
                    candidates_written += 1

                    if committee_name:
                        # Every row here comes from candidates.csv and has
                        # candidate_name/office/district populated above in
                        # `base` — this is the candidate's own campaign
                        # committee (e.g. "Jackson for Governor, Inc."), not
                        # an independent PAC. Previously hardcoded to "PAC",
                        # which collided with the genuinely non-candidate
                        # committees from public_committees.csv (see GA notes
                        # in src/aliases/committee_types.csv).
                        cmte_w.writerow({
                            **base,
                            "committee_name": committee_name,
                            "committee_type": "Candidate Committee",
                            "treasurer_name": treasurer,
                            "city":           clean(row.get("candidateMailingCity")),
                            "zip":            clean(row.get("candidateMailingZipCode")),
                            "active":         "1" if clean(row.get("filerStatusCode")) == "FACT" else "0",
                        })
                        committees_written += 1

            log.file_parsed(cand_path.name, "candidates+committees",
                            candidates_written + committees_written,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=cand_path.stat().st_size)

        # ---------------------------------------------------------------- #
        # committees.csv — non-candidate rows from GetCandidateDetails      #
        # ---------------------------------------------------------------- #
        # Normally absent: GetCandidateDetails returns candidate registrations
        # only, so the scraper's split yields nothing and writes no file (it
        # logs a warning if it ever does — see scrapers/georgia.py
        # download_entities). Handled here so that if Georgia's API does start
        # returning non-candidate rows, they reach committees.csv.gz instead of
        # being silently dropped. Same ENTITY_FIELDS shape as candidates.csv,
        # hence candidateMailing* for the address.
        cmte_raw_path = RAW_DIR / "committees.csv"
        if cmte_raw_path.exists():
            ft = time.perf_counter()
            raw_cmte_written = 0
            with open(cmte_raw_path, newline="", encoding="utf-8") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    committee_name = (clean(row.get("committeeName"))
                                      or clean(row.get("filerName")))
                    if not committee_name:
                        continue

                    treasurer = ""
                    t_first = clean(row.get("treasurerFirstName"))
                    t_last  = clean(row.get("treasurerLastName"))
                    if t_last:
                        treasurer = f"{t_last}, {t_first}".strip(", ")

                    cmte_w.writerow({
                        "state":          STATE,
                        "committee_name": utils.clean_name(committee_name),
                        # No filerType on this endpoint; these are by definition
                        # the rows that carry no candidate, so PAC is the safe
                        # generic. Never "Candidate Committee" — that label is
                        # reserved for the candidates.csv rows above.
                        "committee_type": "PAC",
                        "election_year":  (clean(row.get("electionCycleName")) or "")[:4] or "",
                        "candidate_name": "",
                        "treasurer_name": treasurer,
                        "city":           clean(row.get("candidateMailingCity")),
                        "zip":            clean(row.get("candidateMailingZipCode")),
                        "active":         "1" if clean(row.get("filerStatusCode")) == "FACT" else "0",
                        "state_filer_id": clean(row.get("filerRegistrationId")),
                        "raw_file":       cmte_raw_path.name,
                        "row_num":        row_num,
                    })
                    raw_cmte_written += 1
                    committees_written += 1

            log.file_parsed(cmte_raw_path.name, "committees", raw_cmte_written,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=cmte_raw_path.stat().st_size)

        # ---------------------------------------------------------------- #
        # public (non-candidate) committees — GetCommitteeDetails           #
        # ---------------------------------------------------------------- #
        pub_cmte_path = RAW_DIR / "public_committees.csv"
        if pub_cmte_path.exists():
            ft = time.perf_counter()
            pub_written = 0
            with open(pub_cmte_path, newline="", encoding="utf-8") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    treasurer = ""
                    t_first = clean(row.get("treasurerFirstName"))
                    t_last  = clean(row.get("treasurerLastName"))
                    if t_last:
                        treasurer = f"{t_last}, {t_first}".strip(", ")

                    cmte_w.writerow({
                        "state":          STATE,
                        "committee_name": utils.clean_name(clean(row.get("filerName"))),
                        "committee_type": clean(row.get("filerType")) or "PAC",
                        "election_year":  (clean(row.get("filingCycleName")) or "")[:4] or "",
                        "candidate_name": "",
                        "treasurer_name": treasurer,
                        "city":           clean(row.get("committeeMailingCity")),
                        "zip":            clean(row.get("committeeMailingZipCode")),
                        "active":         "1" if clean(row.get("filerStatusCode")) == "FACT" else "0",
                        "state_filer_id": clean(row.get("filerRegistrationId")),
                        "raw_file":       pub_cmte_path.name,
                        "row_num":        row_num,
                    })
                    pub_written += 1
                    committees_written += 1

            log.file_parsed(pub_cmte_path.name, "public_committees", pub_written,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=pub_cmte_path.stat().st_size)

        # ---------------------------------------------------------------- #
        # legacy candidates — media.ethics.ga.gov name search (A-Z sweep)   #
        # ---------------------------------------------------------------- #
        for path in raw_files("legacy_candidates_*.csv"):
            ft = time.perf_counter()
            file_rows = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    filer_id = clean(row.get("filer_id"))
                    if not filer_id:
                        continue

                    raw_name = clean(row.get("candidate_name"))
                    last, _, rest = raw_name.partition(",")
                    rest = rest.strip()
                    first = rest.split()[0] if rest else ""

                    m = LEGACY_FILER_ID_YEAR_RE.match(filer_id)
                    election_year = m.group(1) if m else ""

                    # The legacy portal combines office + district + jurisdiction
                    # into one string; Peachfile keeps them apart. Split so the
                    # two sources agree and person_ids can unify across them.
                    office, district, jurisdiction = split_legacy_office(
                        row.get("office"))

                    cand_w.writerow({
                        "state":           STATE,
                        "candidate_name":  utils.clean_name(raw_name),
                        "candidate_first": first,
                        "candidate_last":  last.strip(),
                        "office":          office,
                        "district":        district,
                        "jurisdiction":    jurisdiction,
                        "party":           "",
                        "election_year":   election_year,
                        "incumbent":       "",
                        "state_filer_id":  filer_id,
                        "raw_file":        path.name,
                        "row_num":         row_num,
                    })
                    candidates_written += 1
                    file_rows += 1

            log.file_parsed(path.name, "legacy_candidates", file_rows,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # legacy committees — media.ethics.ga.gov by CommitteeType (1-8)    #
        # ---------------------------------------------------------------- #
        for path in raw_files("legacy_committees_type*.csv"):
            ft = time.perf_counter()
            file_rows = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    filer_id = clean(row.get("FilerID"))
                    if not filer_id:
                        continue

                    cmte_w.writerow({
                        "state":          STATE,
                        "committee_name": utils.clean_name(clean(row.get("CommitteeName"))),
                        "committee_type": clean(row.get("CommitteeType")),
                        "election_year":  "",
                        "candidate_name": "",
                        "treasurer_name": clean(row.get("Treasurer")),
                        "city":           clean(row.get("City")),
                        "zip":            clean_zip(row.get("Zip", "")),
                        "active":         "",
                        "state_filer_id": filer_id,
                        "raw_file":       path.name,
                        "row_num":        row_num,
                    })
                    committees_written += 1
                    file_rows += 1

            log.file_parsed(path.name, "legacy_committees", file_rows,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---------------------------------------------------------------- #
        # close handles, assign person IDs                                 #
        # ---------------------------------------------------------------- #
        for fh in file_handles:
            fh.close()
        file_handles = []

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
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        log.info(f"  recordsearch: +{total_recordsearch_contributions:,} contributions "
                f"({total_recordsearch_dupes_contrib:,} deduped), "
                f"+{total_recordsearch_expenditures:,} expenditures "
                f"({total_recordsearch_dupes_expn:,} deduped)")

        duration = round(time.perf_counter() - t0, 1)
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written,
                  recordsearch_contributions=total_recordsearch_contributions,
                  recordsearch_expenditures=total_recordsearch_expenditures,
                  recordsearch_dupes_contrib=total_recordsearch_dupes_contrib,
                  recordsearch_dupes_expn=total_recordsearch_dupes_expn)

    except KeyboardInterrupt:
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


# =============================== cli =================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
