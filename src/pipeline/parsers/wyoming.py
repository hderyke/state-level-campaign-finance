"""
parsers/wyoming.py — Parse Wyoming campaign finance data.

Input: data/Wyoming/raw/
  contributions_all.csv          — full-history bulk export, 8 columns, no
                                    transaction ID, no election_year, no filing_id
  expenditures_all.csv           — full-history bulk export, 8 columns, same gaps
  candidate_committee_roster.pdf — 147-page ActiveReports PDF: candidate name,
                                    party, office, committee name/address,
                                    chairman, treasurer, dates formed/terminated
  pac_roster.pdf                 — 37-page version of the same, minus office/party

Output: data/Wyoming/cleaned/
  contributions.csv.gz, expenditures.csv.gz, candidates.csv.gz,
  committees.csv.gz, loans_debts.csv.gz

Notes
─────
  • Neither export carries any kind of transaction ID, so "AMEND - DELETE"
    rows (a later filing retracting an earlier one) can't be reliably linked
    back to the specific row they retract — the source gives no key to match
    on, and several plausible candidates can share identical
    (date, amount, committee, contributor) tuples. Rather than risk
    subtracting the wrong row, AMEND - DELETE rows are dropped entirely from
    contributions/expenditures/loans_debts (~1,500 / ~640 rows, <0.4% of each
    file). This slightly overstates totals wherever a deletion wasn't offset
    by a corresponding AMEND - ADD elsewhere, but that's a smaller error than
    an incorrect subtraction would be.
  • Contribution Type "LOAN" rows are routed to loans_debts.csv.gz instead of
    contributions.csv.gz (same pattern as Georgia's Loan Received/Payment
    handling) — record_type = "Loan", counterparty = contributor.
    Expenditure Purpose "LOAN PAYMENT" rows stay in expenditures.csv.gz
    (Purpose is free text with 2,700+ distinct values and no separate
    top-level type field to split on cleanly).
  • Contributor name arrives as "LAST, FIRST MIDDLE  (CITY)" for individuals
    or "ORG NAME (CITY)" for organizations/committees — the parenthetical
    city is stripped and cross-checked against the City State Zip column.
  • contributor_type: contributions_all.csv itself has no contributor-type
    column, but the scraper also downloads 12 supplementary
    contributions_source_*.csv files — one per "Source of Contribution"
    category from the site's search filter (INDIVIDUAL, CANDIDATE
    COMMITTEE, CORPORATION, WYOMING PAC, ...; see scrapers/wyoming.py
    module docstring). Neither export carries a transaction ID, so rows are
    matched by the full row tuple (contributor/recipient/type/date/amount/
    city-state-zip) via build_contributor_type_lookup(). That match covers
    ~439,645 of ~444,954 raw contribution rows; the gap is ANONYMOUS/
    UN-ITEMIZED rows with no contributor name, which the site itself never
    assigns a category. Unmatched rows fall back to guess_contributor_type()
    — a name-string heuristic (comma-separated → Individual; org-indicative
    token like "PAC"/"COMMITTEE"/"LLC" → Organization; otherwise blank).
    The 12 raw category labels and the heuristic's two labels both have
    entries in src/aliases/contributor_types.csv.
  • committee_name / candidate_name enrichment: the two roster PDFs are
    parsed into a registry keyed on normalized committee_name (and
    separately candidate_name, for the "CANDIDATE" recipient/filer type,
    where the transaction export's Recipient/Filer Name IS the candidate's
    own name with no separate committee entity). Transactions are enriched
    with office/district/candidate_name via this registry where a match
    exists; unmatched committees (mostly PARTY COMMITTEE / ORGANIZATION,
    which have no dedicated roster on the source site) are still written to
    committees.csv, just without address/treasurer detail.
  • Neither roster PDF exposes a numeric filer ID anywhere — confirmed
    against both the roster PDFs and the transaction exports. id_model =
    "name_hash" (same as Alaska).
  • Roster PDF parsing works from word x-coordinates (pdfplumber
    extract_words()), not the flattened text layer — the two reports are
    column-based (Office Sought | Committee Information | Date Formed |
    Date Terminated for candidates; Committee Name | Committee Information |
    ... for PACs) and the flattened text interleaves columns in a way that's
    ambiguous to re-split (e.g. a PAC's committee name and its street
    address land on the same visual line with no delimiter). Column
    boundaries are hardcoded pixel thresholds derived from the header rows'
    x-positions — see _CAND_COLUMNS / _PAC_COLUMNS.
  • election_year: neither transaction export nor either roster has an
    explicit election-year field, so without this, every candidates.csv/
    committees.csv row would be undated and — since roughly 60% of
    candidates.csv rows are standalone "CANDIDATE"-type filers pulled
    straight from the (heavily recent-skewed) transaction volume, with no
    roster match at all — the whole table would misleadingly read as "this
    cycle only" even though the underlying roster data actually spans
    2001–2026. Two proxies close that gap: for roster-matched entities,
    election_year = the year the committee was *formed* (WY committees are
    cycle-specific — a new one is normally registered per candidacy, so this
    is a reasonable stand-in); for everything else, election_year = the
    earliest year that name appears anywhere in the transaction data (see
    note_committee()). Neither is authoritative — a committee formed in
    year N could plausibly be active for the N or N+1 cycle depending on
    off-year formation timing — but both are real signal, not a guess
    invented from nothing.
"""

import csv
import gzip
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Wyoming" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Wyoming" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "WY"
MAX_VALID_YEAR = date.today().year + 2


# ============================== helpers ===============================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
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
    """M/D/YYYY → YYYY-MM-DD. Returns '' on failure or implausible year."""
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


# Matches "City, ST" / "City, ST ZIP" / "ST" / "ST ZIP" / "" — the transaction
# export's "City State Zip" column and the roster PDFs' last address line
# both use this shape.
_CITY_STATE_ZIP_RE = re.compile(
    r'^(?:(?P<city>[^,]*),\s*)?(?P<state>[A-Za-z]{2})?\s*(?P<zip>\d{5}(?:-\d{4})?)?\s*$'
)


def parse_city_state_zip(val: str) -> tuple[str, str, str]:
    """'MANDAN, ND 58554' -> ('MANDAN', 'ND', '58554'). Falls back to
    (raw, '', '') if the string doesn't match the expected shape (e.g. a
    malformed ZIP in the source data)."""
    v = (val or "").strip()
    if not v:
        return "", "", ""
    m = _CITY_STATE_ZIP_RE.match(v)
    if not m:
        return v, "", ""
    city  = (m.group("city") or "").strip()
    state = (m.group("state") or "").strip().upper()
    zipc  = (m.group("zip") or "").strip()
    if not city and not state and not zipc:
        return v, "", ""
    return city, state, zipc


# Matches a trailing parenthetical city on a contributor/payee name:
# "JONES, KELLY G   (MANDAN)" -> ("JONES, KELLY G", "MANDAN")
_PAREN_CITY_RE = re.compile(r'^(?P<name>.*?)\s*\((?P<city>[^)]*)\)\s*$')


def strip_paren_city(name: str) -> tuple[str, str]:
    v = (name or "").strip()
    if not v:
        return "", ""
    m = _PAREN_CITY_RE.match(v)
    if not m:
        return v, ""
    return m.group("name").strip(), m.group("city").strip()


# Tokens that suggest the contributor is an organization/committee rather
# than an individual. Heuristic only — WY's export has no explicit
# contributor-type column (see module docstring).
_ORG_TOKENS = (
    " PAC", " COMMITTEE", " PARTY", " LLC", " L.L.C", " INC", " INC.", " CORP",
    " CO.", " ASSOCIATION", " UNION", " TRUST", " FUND", " GROUP", " L.P.",
    " FOUNDATION", " COALITION", "ORGANIZATION", " BANK", " CAMPAIGN",
)


def guess_contributor_type(name: str) -> str:
    if not name:
        return ""
    upper = f" {name.upper()} "
    if any(tok in upper for tok in _ORG_TOKENS):
        return "Organization"
    if "," in name:
        return "Individual"
    return ""


# Source of Contribution categories — the raw values written into
# contributor_type for rows matched against the scraper's supplementary
# contributions_source_*.csv exports (see module docstring and
# scrapers/wyoming.py). Keys mirror scrapers/wyoming.py's _slugify() output
# for each label; verified live against the site's ddlSourceOfContribution
# dropdown.
_SOURCE_SLUG_TO_LABEL = {
    "candidate_committee":        "CANDIDATE COMMITTEE",
    "corporation":                "CORPORATION",
    "federal_out_of_state_pac":   "FEDERAL/OUT-OF-STATE PAC",
    "immediate_family_personal":  "IMMEDIATE FAMILY / PERSONAL",
    "individual":                 "INDIVIDUAL",
    "national_party":             "NATIONAL PARTY",
    "organization":               "ORGANIZATION",
    "out_of_state_party":         "OUT OF STATE PARTY",
    "wyoming_county_pac":         "WYOMING COUNTY PAC",
    "wyoming_county_party":       "WYOMING COUNTY PARTY",
    "wyoming_pac":                "WYOMING PAC",
    "wyoming_state_party":        "WYOMING STATE PARTY",
}


def _contribution_row_key(row: dict) -> tuple:
    """Row-identity key shared between contributions_all.csv and the 12
    contributions_source_*.csv files — neither export carries a transaction
    ID, so matching is done on the full raw row tuple. Recipient Type is
    deliberately excluded (constant for a given Recipient Name, identical
    across both file sets)."""
    return (
        clean(row.get("Contributor Name")),
        clean(row.get("Recipient Name")),
        clean(row.get("Contribution Type")),
        clean(row.get("Date")),
        clean(row.get("Filing Status")),
        clean(row.get("Amount")),
        clean(row.get("City State Zip ") or row.get("City State Zip")),
    )


def build_contributor_type_lookup(raw_dir: Path) -> tuple[dict, int]:
    """Build a (row-key) -> Source of Contribution label dict from the
    scraper's supplementary exports. Returns (lookup, files_found) so run()
    can log whether the enrichment files were present at all — an older
    raw/ snapshot from before this feature was added would have none, and
    every contribution row would silently fall back to the heuristic."""
    lookup: dict[tuple, str] = {}
    files_found = 0
    for path in sorted(raw_dir.glob("contributions_source_*.csv")):
        slug = path.stem[len("contributions_source_"):]
        label = _SOURCE_SLUG_TO_LABEL.get(slug)
        if not label:
            continue
        files_found += 1
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # first-seen wins on a duplicate key — a genuine collision
                # (two different transactions sharing every field) is rare
                # at this row count and, if it happens, both rows plausibly
                # share the same category anyway.
                lookup.setdefault(_contribution_row_key(row), label)
    return lookup, files_found


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ======================= roster PDF parsing ============================
#
# Both roster PDFs are ActiveReports-generated tables. pdfplumber's flattened
# text layer interleaves the columns ambiguously (see module docstring), so
# these parsers work from word x-coordinates instead: words are grouped into
# visual rows by y-position (a small gap threshold separates rows; a few
# points of jitter *within* a row is normal — see _page_rows), then each
# row's words are bucketed into columns by x0 threshold.

_CAND_COLUMNS = [("office", 0), ("info", 150), ("date_formed", 560), ("date_term", 650)]
_PAC_COLUMNS  = [("name", 0), ("info", 200), ("date_formed", 550), ("date_term", 650)]

_PARTY_TOKENS = {"DEMOCRAT", "REPUBLICAN", "LIBERTARIAN", "CONSTITUTION",
                 "INDEPENDENT", "UNAFFILIATED", "COUNTRY"}


def _page_rows(page, columns: list[tuple[str, int]], gap: float = 5.0) -> list[dict]:
    """Group a page's words into visual rows, then bucket each row's words
    into named columns by x0 threshold. Returns a list of {col_name: text}."""
    words = page.extract_words()
    words.sort(key=lambda w: w["top"])
    groups: list[list[dict]] = []
    cur: list[dict] = []
    prev_top = None
    for w in words:
        if prev_top is not None and (w["top"] - prev_top) > gap:
            groups.append(cur)
            cur = []
        cur.append(w)
        prev_top = w["top"]
    if cur:
        groups.append(cur)

    out = []
    for g in groups:
        g.sort(key=lambda w: w["x0"])
        cell: dict[str, list[str]] = {name: [] for name, _ in columns}
        for w in g:
            col = None
            for name, x_min in columns:
                if w["x0"] >= x_min:
                    col = name
            if col:
                cell[col].append(w["text"])
        out.append({name: " ".join(cell[name]) for name, _ in columns})
    return out


def _roster_rows(pdf_path: Path, columns: list[tuple[str, int]],
                 header_check, footer_check, skip_check) -> list[dict]:
    """Flatten every page's rows into one list, stripping the repeated
    masthead (everything before header_check matches), the page footer
    (from footer_check onward), and footnote/blank rows (skip_check)."""
    all_rows: list[dict] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            started = False
            for r in _page_rows(page, columns):
                if not started:
                    if header_check(r):
                        started = True
                    continue
                if footer_check(r):
                    break
                if skip_check(r):
                    continue
                all_rows.append(r)
    return all_rows


def _consume_tail(record: dict, info: str, stage: str) -> str:
    """Shared Chairman:/Treasurer:/phone/Email:/Website: tail parsing for
    both roster shapes. Mutates `record` in place; returns the next stage,
    or "" once Website: has been consumed (record is complete)."""
    if stage == "address":
        if info.startswith("Chairman:"):
            record["chairman"] = info[len("Chairman:"):].strip()
            return "treasurer"
        record["address_lines"].append(info)
        return "address"
    if stage == "treasurer":
        if info.startswith("Treasurer:"):
            record["treasurer"] = info[len("Treasurer:"):].strip()
            return "phone_or_email"
        return "treasurer"
    if stage == "phone_or_email":
        if info.startswith("Email:"):
            record["email"] = info[len("Email:"):].strip()
            return "website"
        record["phone"] = info
        return "email"
    if stage == "email":
        if info.startswith("Email:"):
            record["email"] = info[len("Email:"):].strip()
            return "website"
        return "email"
    if stage == "website":
        if info.startswith("Website:"):
            record["website"] = info[len("Website:"):].strip()
            return ""
        return "website"
    return stage


def parse_candidate_committee_roster(pdf_path: Path) -> list[dict]:
    """Returns one dict per candidate-committee registration: party,
    candidate_name, office, date_formed, date_terminated, committee_name,
    address_lines, chairman, treasurer, phone, email, website."""
    rows = _roster_rows(
        pdf_path, _CAND_COLUMNS,
        header_check=lambda r: r["office"] == "Office Sought" and r["info"] == "Committee Information",
        footer_check=lambda r: "Page" in r["date_term"],
        skip_check=lambda r: r["office"].startswith("*")
                             or not any([r["office"], r["info"], r["date_formed"], r["date_term"]]),
    )

    records: list[dict] = []
    cur: dict | None = None
    stage = ""
    current_office = ""

    for r in rows:
        office, info = r["office"].strip(), r["info"].strip()
        date_formed, date_term = r["date_formed"].strip(), r["date_term"].strip()

        if office and office in _PARTY_TOKENS:
            if cur:
                records.append(cur)
            name = info[:-1].strip() if info.endswith("*") else info
            cur = {
                "party": office, "candidate_name": name,
                "date_formed": date_formed, "date_terminated": date_term,
                "office": current_office, "committee_name": "",
                "address_lines": [], "chairman": "", "treasurer": "",
                "phone": "", "email": "", "website": "",
            }
            stage = "committee_name"
            continue

        if office and office not in _PARTY_TOKENS:
            current_office = office
            continue

        if cur is None:
            continue  # stray row before any record started — ignore

        if stage == "committee_name":
            cur["committee_name"] = info
            stage = "address"
            continue

        stage = _consume_tail(cur, info, stage)
        if stage == "":
            records.append(cur)
            cur = None

    if cur:
        records.append(cur)
    return records


def parse_pac_roster(pdf_path: Path) -> list[dict]:
    """Returns one dict per PAC registration: committee_name, date_formed,
    date_terminated, address_lines, chairman, treasurer, phone, email,
    website. No office/party — PACs aren't tied to a specific race."""
    rows = _roster_rows(
        pdf_path, _PAC_COLUMNS,
        header_check=lambda r: r["name"] == "Committee Name" and r["info"] == "Committee Information",
        footer_check=lambda r: "Page" in r["date_term"],
        skip_check=lambda r: not any([r["name"], r["info"], r["date_formed"], r["date_term"]]),
    )

    records: list[dict] = []
    cur: dict | None = None
    stage = ""

    for r in rows:
        name, info = r["name"].strip(), r["info"].strip()
        date_formed, date_term = r["date_formed"].strip(), r["date_term"].strip()

        if name:
            if cur:
                records.append(cur)
            cur = {
                "committee_name": name, "date_formed": date_formed,
                "date_terminated": date_term,
                "address_lines": [info] if info else [],
                "chairman": "", "treasurer": "", "phone": "", "email": "", "website": "",
            }
            stage = "address"
            continue

        if cur is None:
            continue

        stage = _consume_tail(cur, info, stage)
        if stage == "":
            records.append(cur)
            cur = None

    if cur:
        records.append(cur)
    return records


# Office string -> (office, district). "HOUSE DISTRICT 03" -> ("HOUSE", "03");
# "GOVERNOR" -> ("GOVERNOR", "").
_DISTRICT_OFFICE_RE = re.compile(r'^(HOUSE|SENATE) DISTRICT (\d+)$')


def split_office_district(office_raw: str) -> tuple[str, str]:
    m = _DISTRICT_OFFICE_RE.match((office_raw or "").strip())
    if m:
        return m.group(1), m.group(2)
    return (office_raw or "").strip(), ""


def roster_address_city_zip(address_lines: list[str]) -> tuple[str, str]:
    """Committees.csv only has city/zip (no street, no state) — parsed from
    the last address line, which is always the city/state/zip line."""
    if not address_lines:
        return "", ""
    city, _state, zipc = parse_city_state_zip(address_lines[-1])
    return city, zipc


# ============================== run =================================

def run():
    log = get_logger("wyoming", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles: list = []

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, cand_fh, cmte_fh, loan_fh]

        # ------------------------------------------------------------------ #
        # entities — roster PDFs                                             #
        # ------------------------------------------------------------------ #
        cand_committee_registry: dict[str, dict] = {}  # clean_name(committee_name) -> record
        candidate_registry:      dict[str, dict] = {}  # clean_name(candidate_name) -> record

        cand_roster_path = RAW_DIR / "candidate_committee_roster.pdf"
        if cand_roster_path.exists():
            t_file = time.perf_counter()
            records = parse_candidate_committee_roster(cand_roster_path)
            for i, rec in enumerate(records, start=1):
                candidate_name = utils.clean_name(rec["candidate_name"])
                office, district = split_office_district(rec["office"])
                terminated = rec["date_terminated"] not in ("", "N/A")
                city, zipc = roster_address_city_zip(rec["address_lines"])
                # No explicit election-year field in the roster — the year a
                # candidate committee was *formed* is a reasonable proxy (WY
                # committees are cycle-specific: a new one is typically
                # registered per candidacy, not reused across cycles). This
                # is what makes the real 2001–2026 spread in the underlying
                # roster data (see docs/states/wyoming.md) actually visible
                # in the output instead of every row looking undated.
                formed_iso    = parse_date(rec["date_formed"])
                election_year = formed_iso[:4] if formed_iso else ""

                cand_w.writerow({
                    "state": STATE, "candidate_name": candidate_name,
                    "candidate_first": rec["candidate_name"].split()[0] if rec["candidate_name"] else "",
                    "candidate_last":  rec["candidate_name"].split()[-1] if rec["candidate_name"] else "",
                    "office": office, "district": district, "party": rec["party"],
                    "election_year": election_year,
                    "raw_file": "candidate_committee_roster.pdf", "row_num": i,
                })
                candidates_written += 1

                committee_name = utils.clean_name(rec["committee_name"])
                cmte_w.writerow({
                    "state": STATE, "committee_name": committee_name,
                    "committee_type": "CANDIDATE COMMITTEE",
                    "candidate_name": candidate_name,
                    "treasurer_name": rec["treasurer"], "city": city, "zip": zipc,
                    "active": 0 if terminated else 1, "election_year": election_year,
                    "raw_file": "candidate_committee_roster.pdf", "row_num": i,
                })
                committees_written += 1

                entry = {"candidate_name": candidate_name, "office": office, "district": district}
                if committee_name:
                    cand_committee_registry[committee_name] = entry
                if candidate_name:
                    candidate_registry[candidate_name] = entry

            log.file_parsed(cand_roster_path.name, "entities", len(records),
                            duration_s=round(time.perf_counter() - t_file, 2),
                            bytes=cand_roster_path.stat().st_size)
        else:
            log.info("  candidate_committee_roster.pdf not found — skipping")

        pac_roster_path = RAW_DIR / "pac_roster.pdf"
        if pac_roster_path.exists():
            t_file = time.perf_counter()
            records = parse_pac_roster(pac_roster_path)
            for i, rec in enumerate(records, start=1):
                terminated = rec["date_terminated"] not in ("", "N/A")
                city, zipc = roster_address_city_zip(rec["address_lines"])
                committee_name = utils.clean_name(rec["committee_name"])
                formed_iso    = parse_date(rec["date_formed"])
                election_year = formed_iso[:4] if formed_iso else ""

                cmte_w.writerow({
                    "state": STATE, "committee_name": committee_name,
                    "committee_type": "POLITICAL ACTION COMMITTEE",
                    "treasurer_name": rec["treasurer"], "city": city, "zip": zipc,
                    "active": 0 if terminated else 1, "election_year": election_year,
                    "raw_file": "pac_roster.pdf", "row_num": i,
                })
                committees_written += 1
                if committee_name:
                    cand_committee_registry.setdefault(
                        committee_name, {"candidate_name": "", "office": "", "district": ""})

            log.file_parsed(pac_roster_path.name, "entities", len(records),
                            duration_s=round(time.perf_counter() - t_file, 2),
                            bytes=pac_roster_path.stat().st_size)
        else:
            log.info("  pac_roster.pdf not found — skipping")

        # Committee names seen in transactions but not covered by either
        # roster (mainly PARTY COMMITTEE / ORGANIZATION — WY publishes no
        # dedicated roster for either; see module docstring). Collected while
        # scanning contributions/expenditures below, written at the end so
        # every committee gets exactly one row regardless of which file it
        # first appears in.
        #
        # "CANDIDATE" is handled separately (extra_candidates, not
        # extra_committees): that recipient/filer type means the candidate
        # filed as themself with no separate committee entity (mostly small
        # or self-funded campaigns) — writing a committees.csv row for one
        # would misrepresent an individual as a committee.
        # min_year per name tracks the earliest transaction date seen for that
        # committee/candidate — the only date signal available for entities
        # with no roster match, and what keeps them from all reading as
        # undated "this cycle" filler in the output (see docs/states/wyoming.md).
        extra_committees: dict[str, dict] = {}  # clean_name -> {"type": str, "min_year": str}
        extra_candidates: dict[str, str] = {}   # clean_name -> min_year

        def note_committee(name: str, entity_type: str, date_iso: str):
            if not name:
                return
            year = date_iso[:4] if date_iso else ""
            if entity_type == "CANDIDATE":
                if name in candidate_registry:
                    return
                prev = extra_candidates.get(name, "")
                if year and (not prev or year < prev):
                    extra_candidates[name] = year
                elif name not in extra_candidates:
                    extra_candidates[name] = year
                return
            if name in cand_committee_registry:
                return
            entry = extra_committees.setdefault(name, {"type": entity_type, "min_year": year})
            if year and (not entry["min_year"] or year < entry["min_year"]):
                entry["min_year"] = year

        # ------------------------------------------------------------------ #
        # contributions_all.csv                                              #
        # ------------------------------------------------------------------ #
        contrib_path = RAW_DIR / "contributions_all.csv"
        if contrib_path.exists():
            t_file = time.perf_counter()
            skipped_delete = 0
            source_lookup, source_files_found = build_contributor_type_lookup(RAW_DIR)
            matched_by_source = 0
            with open(contrib_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    status = clean(row.get("Filing Status"))
                    if status == "AMEND - DELETE":
                        skipped_delete += 1
                        continue

                    committee_name_raw = clean(row.get("Recipient Name"))
                    committee_name     = utils.clean_name(committee_name_raw)
                    recipient_type     = clean(row.get("Recipient Type"))

                    contributor_raw, paren_city = strip_paren_city(row.get("Contributor Name", ""))
                    contributor_name = utils.clean_name(contributor_raw)
                    city, st, zipc = parse_city_state_zip(row.get("City State Zip ", ""))
                    if not city and paren_city:
                        city = paren_city

                    registry_hit = cand_committee_registry.get(committee_name) or (
                        candidate_registry.get(committee_name) if recipient_type == "CANDIDATE" else None)
                    candidate_name = registry_hit["candidate_name"] if registry_hit else ""
                    office         = registry_hit["office"] if registry_hit else ""

                    amount = parse_amount(row.get("Amount"))
                    date_  = parse_date(row.get("Date"))
                    ctype  = clean(row.get("Contribution Type"))
                    amended = "Y" if status == "AMEND - ADD" else ""

                    source_label = source_lookup.get(_contribution_row_key(row))
                    if source_label:
                        contributor_type = source_label
                        matched_by_source += 1
                    else:
                        contributor_type = guess_contributor_type(contributor_raw)

                    note_committee(committee_name, recipient_type, date_)

                    if ctype == "LOAN":
                        loan_w.writerow({
                            "state": STATE, "committee_name": committee_name,
                            "original_amount": amount, "date": date_, "record_type": "Loan",
                            "counterparty_name": contributor_name,
                            "counterparty_city": city, "counterparty_state": st,
                            "counterparty_zip": zipc, "candidate_name": candidate_name,
                            "amended": amended, "raw_file": "contributions_all.csv",
                            "row_num": row_num,
                        })
                        total_loans += 1
                        continue

                    cont_w.writerow({
                        "state": STATE, "committee_name": committee_name,
                        "amount": amount, "date": date_, "transaction_type": ctype,
                        "contributor_name": contributor_name,
                        "contributor_type": contributor_type,
                        "contributor_city": city, "contributor_state": st, "contributor_zip": zipc,
                        "candidate_name": candidate_name, "office": office,
                        "amended": amended, "raw_file": "contributions_all.csv",
                        "row_num": row_num,
                    })
                    total_contributions += 1

            log.file_parsed(contrib_path.name, "contributions", total_contributions,
                            duration_s=round(time.perf_counter() - t_file, 2),
                            bytes=contrib_path.stat().st_size, skipped=skipped_delete,
                            source_files_found=source_files_found,
                            matched_by_source=matched_by_source)
        else:
            log.info("  contributions_all.csv not found — skipping")

        # ------------------------------------------------------------------ #
        # expenditures_all.csv                                               #
        # ------------------------------------------------------------------ #
        expend_path = RAW_DIR / "expenditures_all.csv"
        if expend_path.exists():
            t_file = time.perf_counter()
            skipped_delete = 0
            with open(expend_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, start=2):
                    status = clean(row.get("Filing Status"))
                    if status == "AMEND - DELETE":
                        skipped_delete += 1
                        continue

                    committee_name_raw = clean(row.get("Filer Name"))
                    committee_name     = utils.clean_name(committee_name_raw)
                    filer_type         = clean(row.get("Filer Type"))

                    payee_name = utils.clean_name(clean(row.get("Payee")))
                    city, st, zipc = parse_city_state_zip(row.get("City State Zip", ""))
                    date_ = parse_date(row.get("Date"))

                    registry_hit = cand_committee_registry.get(committee_name) or (
                        candidate_registry.get(committee_name) if filer_type == "CANDIDATE" else None)
                    candidate_name = registry_hit["candidate_name"] if registry_hit else ""
                    office         = registry_hit["office"] if registry_hit else ""

                    amended = "Y" if status == "AMEND - ADD" else ""

                    note_committee(committee_name, filer_type, date_)

                    expn_w.writerow({
                        "state": STATE, "committee_name": committee_name,
                        "amount": parse_amount(row.get("Amount")),
                        "date": date_, "transaction_type": "EXPENDITURE",
                        "payee_name": payee_name, "purpose": clean(row.get("Purpose")),
                        "payee_city": city, "payee_state": st, "payee_zip": zipc,
                        "candidate_name": candidate_name, "office": office,
                        "amended": amended, "raw_file": "expenditures_all.csv",
                        "row_num": row_num,
                    })
                    total_expenditures += 1

            log.file_parsed(expend_path.name, "expenditures", total_expenditures,
                            duration_s=round(time.perf_counter() - t_file, 2),
                            bytes=expend_path.stat().st_size, skipped=skipped_delete)
        else:
            log.info("  expenditures_all.csv not found — skipping")

        # Committees referenced by transactions but absent from both rosters
        # (Party committees, Organizations, and any PAC/candidate committee
        # the roster's "Both" filter happened to miss) — written last so the
        # scan above has seen every transaction file first. election_year is
        # the earliest transaction year seen for that name — the only date
        # signal available without a roster match (see note_committee).
        for i, (name, info) in enumerate(sorted(extra_committees.items()), start=1):
            cmte_w.writerow({
                "state": STATE, "committee_name": name, "committee_type": info["type"],
                "election_year": info["min_year"],
                "raw_file": "contributions_all.csv+expenditures_all.csv", "row_num": i,
            })
            committees_written += 1

        # Standalone "CANDIDATE" filers (see note_committee above) — no
        # committee row, just a bare candidates.csv entry so their
        # transactions still resolve to a person. office/district/party are
        # unavailable outside the committee roster, but election_year (first
        # year this name appears in the transaction data) still is.
        for i, (name, year) in enumerate(sorted(extra_candidates.items()), start=1):
            cand_w.writerow({
                "state": STATE, "candidate_name": name,
                "candidate_first": name.split()[0] if name else "",
                "candidate_last":  name.split()[-1] if name else "",
                "election_year": year,
                "raw_file": "contributions_all.csv+expenditures_all.csv", "row_num": i,
            })
            candidates_written += 1

        # Close output handles before person-ID assignment (utils reads/rewrites the files).
        for fh in file_handles:
            fh.close()
        file_handles = []

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="name_hash")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        for name, kind, count in [
            ("contributions.csv.gz", "contributions", total_contributions),
            ("expenditures.csv.gz",  "expenditures",  total_expenditures),
            ("candidates.csv.gz",    "candidates",    candidates_written),
            ("committees.csv.gz",    "committees",    committees_written),
            ("loans_debts.csv.gz",   "loans_debts",   total_loans),
        ]:
            log.file_parsed(name, kind, count, role="output", bytes=_bytes(name))

        duration = round(time.perf_counter() - t0, 1)
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)

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


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
