"""
parsers/new_jersey.py — Parse New Jersey ELEC campaign finance data.

Reads raw CSVs from data/New Jersey/raw/ and writes normalized output to
data/New Jersey/cleaned/.

Raw files consumed (all written by scrapers/new_jersey.py):
  entities_{year}.csv        — candidate / joint-candidate / election-related
                               committees for one election year
  pacs_{year}.csv            — PAC, party and legislative leadership committees
  entity_details_{year}.csv  — per-entity treasurer, address and joint-committee
                               linkage from GetEntityDataWithCommittee
  contributions_{year}.csv   — itemized contributions, all entities in that year
  expenditures_{year}.csv    — itemized expenditures, all entities in that year

Notes:
  - ELEC's entity id (ENTITY_S) is per entity **per election cycle**, so one
    person running in the 2023 primary and the 2023 general has two ids. That
    makes id_model="committee": assign_person_ids groups on
    (state, candidate_name, office, district) and takes min(state_filer_id).

  - **Transactions join on (entity_name, election_year), not on the entity id.**
    ELEC's transaction endpoints scope by EntityName + ElectionYears and ignore
    ENTITY_S (see scrapers/new_jersey.py), so a transaction row simply cannot
    be attributed to one cycle. load_entities() therefore returns two views of
    the registry — by_eid for the entity tables, by_name_year for the
    transaction join. The practical effect is that a filer's primary and
    general contributions land under one committee row rather than two.

  - There is no separate candidate registry. A candidate and their committee
    are the same ELEC record, so entities_*.csv feeds BOTH candidates.csv.gz
    and committees.csv.gz; pacs_*.csv feeds only committees.csv.gz.

  - The Office/Cmte column is overloaded: it holds a real office for candidate
    filers (GOVERNOR, STATE SENATE, MAYOR, ...) and a committee type for
    everyone else (JOINT CANDIDATES CMTE, CMTE BALLOT QUESTION, ELECTION
    RELATED POL CMTE, INDEPENDENT EXPENDITURE CMTE (Z), INAUGURAL, OTHER).
    CANDIDATE_OFFICES is the discriminator — see _is_candidate_office().

  - Names are filed surname-first ("AARON, CHARLES S JR"). They're flipped to
    "CHARLES S AARON JR" for candidate_name so that utils' first+last token
    fallback and nickname expansion in assign_committee_person_ids work at
    all — those tokenize on the first and last whitespace-delimited tokens,
    which on the raw form would give "AARON," and "JR". committee_name keeps
    ELEC's as-filed spelling, since that's what the transaction rows join on.

  - Address arrives as one free-text line ("123 MAIN ST, TRENTON, NJ 08608").
    split_address() peels city/state/zip off the tail and is deliberately
    conservative: anything it can't confidently split is left in the street
    portion and the structured fields stay blank, rather than guessing.

  - Location is the jurisdiction, not a mailing city — "25TH LEGISLATIVE
    DISTRICT", "STATEWIDE", "TOMS RIVER (DOVER TWP)". It maps to
    candidates.district / candidates.jurisdiction, never to committees.city.
    committees.city comes from the detail sweep's mailing address.

  - Amounts can be negative (refunds, and ELEC's "$1-$200" bucket routinely
    nets below zero). Parentheses and leading minus are both honoured.

  - NJ publishes no loan or debt schedule through this search system, so
    loans_debts.csv.gz is written with a header and no rows.
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
RAW_DIR   = PROJECT_ROOT / "data" / "New Jersey" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "New Jersey" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "NJ"

# ========================= state-specific constants ===================

# Values of the Office/Cmte column that denote a real office being sought,
# i.e. the filer is a candidate. Everything else in that column is a committee
# type. Taken verbatim from ELEC's own Office dropdown.
CANDIDATE_OFFICES = {
    "GOVERNOR",
    "LIEUTENANT GOVERNOR",
    "STATE SENATE",
    "STATE ASSEMBLY",
    "COUNTY COMMISSIONER",
    "COUNTY SHERIFF",
    "COUNTY CLERK",
    "COUNTY SURROGATE",
    "COUNTY REGISTRAR OF DEEDS",
    "COUNTY EXECUTIVE",
    "MUNICIPAL OFFICE",
    "FIRE COMMISSION",
    "SCHOOL BOARD",
    "CHARTER STUDY COMMISSION",
    "MAYOR",
}

# Name suffixes that must stay attached to the surname when a surname-first
# name is flipped. ELEC files them inside the given-name half ("AARON,
# CHARLES S JR"), so without this list "JR" would be read as a middle name.
NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "JR.", "SR."}

# Location values that are statewide or otherwise not a district. Kept out of
# candidates.district so district stays meaningful for the legislative rows.
NON_DISTRICT_LOCATIONS = {"STATEWIDE", "", "ALL", "OUTSIDE NEW JERSEY"}

_LEG_DISTRICT_RE = re.compile(r"^(\d+)(?:ST|ND|RD|TH)\s+LEGISLATIVE DISTRICT$")

# Trailing "CITY, ST 08608" / "CITY, ST 08608-1234" on a one-line address.
_CITY_STATE_ZIP_RE = re.compile(
    r"^(?P<city>[^,]+),\s*(?P<state>[A-Z]{2})\.?\s+(?P<zip>\d{5}(?:-\d{4})?)\s*$"
)
_ZIP_RE = re.compile(r"^\d{5}(?:-\d{4})?$")


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val) -> str:
    """Parse a dollar amount to a plain numeric string. Returns '' on failure."""
    v = clean(val).replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]   # accounting-style negative
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val) -> str:
    """Normalize a date to YYYY-MM-DD. Returns '' on failure or implausible year."""
    v = clean(val)
    if not v:
        return ""
    # ELEC's JSON sometimes serializes dates as "2023-06-06T00:00:00"
    if "T" in v:
        v = v.split("T", 1)[0]
    elif len(v) > 10 and v[10] == " ":
        v = v[:10]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > date.today().year + 2:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def parse_year(val) -> str:
    """Return a 4-digit year string or ''."""
    v = clean(val)
    if re.fullmatch(r"\d{4}", v):
        return v
    d = parse_date(v)
    return d[:4] if d else ""


def flip_name(val: str) -> str:
    """Convert ELEC's surname-first filing name to natural order.

    "AARON, CHARLES S JR"  → "CHARLES S AARON JR"
    "BUCCO, ANTHONY M"     → "ANTHONY M BUCCO"
    "NJSPBA PAC"           → "NJSPBA PAC"   (no comma — an organization)

    A trailing suffix in the given-name half is moved after the surname rather
    than being left to masquerade as a middle initial.
    """
    v = utils.clean_name(val)
    if "," not in v:
        return v
    last, _, rest = v.partition(",")
    last, rest = last.strip(), rest.strip()
    if not rest:
        return last

    tokens = rest.split()
    suffix = ""
    if len(tokens) > 1 and tokens[-1].rstrip(".") in {s.rstrip(".") for s in NAME_SUFFIXES}:
        suffix = tokens.pop()
    out = " ".join(tokens + [last])
    return f"{out} {suffix}".strip() if suffix else out


def split_person_name(val: str) -> tuple[str, str]:
    """Return (first, last) from a surname-first filing name. ('', '') if unsplittable."""
    v = utils.clean_name(val)
    if "," not in v:
        return "", ""
    last, _, rest = v.partition(",")
    tokens = rest.split()
    if tokens and tokens[-1].rstrip(".") in {s.rstrip(".") for s in NAME_SUFFIXES}:
        tokens.pop()
    return (tokens[0] if tokens else ""), last.strip()


def split_address(val: str) -> tuple[str, str, str, str]:
    """
    Split a one-line address into (street, city, state, zip).

    Conservative by design: if the tail doesn't look like "CITY, ST ZIP", the
    whole string is returned as street and the rest stay blank. A wrong city
    is worse than a missing one — validate.py warns on malformed ZIPs and
    unrecognized state codes, and guessing here would manufacture both.
    """
    v = clean(val).rstrip(",").strip()
    if not v:
        return "", "", "", ""

    parts = [p.strip() for p in v.split(",")]

    # "STREET, CITY, ST ZIP" — the common shape.
    if len(parts) >= 3:
        m = _CITY_STATE_ZIP_RE.match(", ".join(parts[-2:]))
        if m:
            return (", ".join(parts[:-2]).strip(),
                    m.group("city").strip(), m.group("state"), m.group("zip"))

    # "CITY, ST ZIP" on its own — no street component.
    m = _CITY_STATE_ZIP_RE.match(v)
    if m:
        return "", m.group("city").strip(), m.group("state"), m.group("zip")

    # Whitespace-only variant: "... TRENTON NJ 08608"
    tokens = v.split()
    if len(tokens) >= 3 and _ZIP_RE.match(tokens[-1]) and len(tokens[-2]) == 2 \
            and tokens[-2].isalpha():
        return " ".join(tokens[:-2]), "", tokens[-2].upper(), tokens[-1]

    return v, "", "", ""


# ENTITY_TYPE values meaning "this filer is a candidate". Confirmed: "C".
# Other values are unsampled, so anything unrecognized defers to the office
# test rather than being assumed non-candidate.
ENTITY_TYPE_CANDIDATE = {"C"}


def _is_candidate_office(office_cmte: str) -> bool:
    """True when the Office/Cmte value names an office rather than a committee type."""
    return clean(office_cmte).upper() in CANDIDATE_OFFICES


def is_candidate(ent: dict) -> bool:
    """Decide whether an entity is a candidate filer.

    Prefers ELEC's own ENTITY_TYPE flag when present — it's authoritative and
    doesn't depend on CANDIDATE_OFFICES keeping pace with new office labels.
    Falls back to the office-name test when the flag is absent (the entity
    listing doesn't always carry it) or holds a value we haven't seen.
    """
    et = clean(ent.get("entity_type")).upper()
    if et in ENTITY_TYPE_CANDIDATE:
        return True
    if et and et not in ENTITY_TYPE_CANDIDATE and _is_candidate_office(ent.get("office_cmte", "")):
        # Known-but-unmapped type on something that names a real office —
        # trust the office and keep the row rather than silently dropping it.
        return True
    return _is_candidate_office(ent.get("office_cmte", "")) if not et else False


def name_from_parts(ent: dict) -> tuple[str, str, str]:
    """Return (full_name, first, last) from ELEC's split name components.

    Returns ('', '', '') when the components aren't present, so callers can
    fall back to flip_name()/split_person_name() on the "LAST, FIRST" string.
    Preferring these is a correctness win: they're ELEC's own split, so they
    handle the compound surnames and embedded suffixes the heuristic can't.
    """
    first  = utils.clean_name(ent.get("first_name", ""))
    last   = utils.clean_name(ent.get("last_name", ""))
    if not (first or last):
        return "", "", ""
    mi     = utils.clean_name(ent.get("middle_initial", "")).rstrip(".")
    suffix = utils.clean_name(ent.get("suffix", ""))
    parts  = [p for p in (first, mi, last, suffix) if p]
    return " ".join(parts), first, last


def split_location(location: str) -> tuple[str, str]:
    """
    Return (district, jurisdiction) from ELEC's Location value.

    Legislative districts become a bare number so they sort and join sensibly
    ("25TH LEGISLATIVE DISTRICT" → "25"); everything else is a named
    jurisdiction (county, municipality) and district is left blank.
    """
    loc = clean(location).upper()
    if loc in NON_DISTRICT_LOCATIONS:
        return "", loc if loc else ""
    m = _LEG_DISTRICT_RE.match(loc)
    if m:
        return m.group(1), loc
    return "", loc


def raw_files(pattern: str) -> list[Path]:
    """Return non-empty raw files matching pattern, sorted by name (i.e. year)."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# =========================== entity registry ==========================

def load_entities() -> tuple[dict[str, dict], dict[tuple[str, str], dict]]:
    """
    Load every entities_*.csv and pacs_*.csv.

    Returns two views of the same records:

      by_eid       — keyed on ENTITY_S. Drives the committees/candidates
                     tables, where one row per entity per cycle is correct.
      by_name_year — keyed on (normalized name, election_year). Drives the
                     transaction join, because ELEC's transaction endpoints
                     scope by EntityName + ElectionYears rather than by id, so
                     that pair is all a transaction row can be tied back to.

    A filer who ran in both the primary and the general for one year has two
    eids but collapses to a single by_name_year entry. First one wins; they
    differ only in election_type, which the transaction tables don't carry.
    """
    by_eid: dict[str, dict] = {}
    by_name_year: dict[tuple[str, str], dict] = {}

    for pattern in ("entities_*.csv", "pacs_*.csv"):
        for path in raw_files(pattern):
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    eid = clean(row.get("eid"))
                    if not eid or eid in by_eid:
                        continue
                    rec = {
                        "eid":           eid,
                        "entity_kind":   clean(row.get("entity_kind")),
                        "name":          clean(row.get("name")),
                        "location":      clean(row.get("location")),
                        "office_cmte":   clean(row.get("office_cmte")),
                        "party":         clean(row.get("party")),
                        "election_type": clean(row.get("election_type")),
                        "election_year": parse_year(row.get("election_year")),
                        "raw_file":      path.name,
                        "row_num":       row_num,
                    }
                    by_eid[eid] = rec

                    key = (utils.clean_name(rec["name"]), rec["election_year"])
                    if key[0] and key not in by_name_year:
                        by_name_year[key] = rec

    return by_eid, by_name_year


def load_entity_details() -> dict[str, dict]:
    """Load entity_details_*.csv keyed by eid. First record per eid wins.

    Returns {} when the detail sweep hasn't run — treasurer/city/zip simply
    stay blank in that case rather than the parse failing.
    """
    details: dict[str, dict] = {}
    for path in raw_files("entity_details_*.csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                eid = clean(row.get("eid"))
                if eid and eid not in details:
                    details[eid] = row
    return details


# ============================== run() =================================

def run():
    log = get_logger("new_jersey", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
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

        # ───────────────────── entity registry ─────────────────────────
        entities, entities_by_name_year = load_entities()
        log.registry_loaded("entities_*.csv", entries=len(entities),
                            relation="committees")

        def lookup(row: dict) -> dict:
            """Resolve a transaction row to its entity record.

            Transaction rows are keyed on (entity_name, election_year) because
            that is the only scope ELEC's endpoints accept — see the scraper's
            module docstring. Returns {} for a row whose entity isn't in the
            registry, which happens when the transaction sweep ran against a
            stale or partial entity file.
            """
            key = (utils.clean_name(row.get("entity_name", "")),
                   parse_year(row.get("election_year")))
            return entities_by_name_year.get(key, {})

        details = load_entity_details()
        if details:
            log.registry_loaded("entity_details_*.csv", entries=len(details),
                                relation="committees")
        else:
            log.info("  no entity_details_*.csv — falling back to name-flipping "
                     "and the office-name test for candidate detection")

        # ── Resolve every entity ONCE ─────────────────────────────────
        # candidate_name has to be byte-identical between candidates.csv and
        # the transaction tables or the person_id joins silently miss, so both
        # paths read from this instead of recomputing. Keyed both ways because
        # expenditures resolve by eid and contributions only by name+year.
        resolved_by_eid: dict[str, dict] = {}
        for _eid, _ent in entities.items():
            merged = {**_ent, **{k: v for k, v in details.get(_eid, {}).items()
                                 if clean(v)}}
            is_cand = is_candidate(merged)
            cand_name, first, last = name_from_parts(merged)
            if not cand_name:
                cand_name = flip_name(_ent["name"])
                first, last = split_person_name(_ent["name"])
            resolved_by_eid[_eid] = {
                "is_cand":        is_cand,
                "candidate_name": cand_name if is_cand else "",
                "candidate_first": first if is_cand else "",
                "candidate_last":  last if is_cand else "",
                "office":          _ent["office_cmte"] if is_cand else "",
            }

        resolved_by_name_year = {
            key: resolved_by_eid[rec["eid"]]
            for key, rec in entities_by_name_year.items()
            if rec["eid"] in resolved_by_eid
        }
        _EMPTY = {"is_cand": False, "candidate_name": "", "candidate_first": "",
                  "candidate_last": "", "office": ""}

        # ─────────────────────── contributions ─────────────────────────
        for path in raw_files("contributions_*.csv"):
            ft = time.perf_counter()
            rows_in_file = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    ent = lookup(row)

                    _street, city, st, zipc = split_address(row.get("address"))

                    # Prefer the entity name the query was scoped by, then the
                    # registry, then the row's own CAND_NAME. The last is a
                    # display string that drifts from the canonical entity name
                    # on joint-committee filings, so it's the fallback.
                    committee_name = (utils.clean_name(row.get("entity_name", ""))
                                      or utils.clean_name(ent.get("name", ""))
                                      or utils.clean_name(row.get("recipient", "")))

                    res = resolved_by_name_year.get(
                        (utils.clean_name(row.get("entity_name", "")),
                         parse_year(row.get("election_year"))), _EMPTY)
                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    committee_name,
                        "amount":            parse_amount(row.get("amount")),
                        "date":              parse_date(row.get("date")),
                        "transaction_type":  clean(row.get("contribution_type")),
                        "contributor_name":  utils.clean_name(row.get("contributor")),
                        "contributor_type":  clean(row.get("contributor_type")),
                        "contributor_city":  city,
                        "contributor_state": st,
                        "contributor_zip":   zipc,
                        "employer":          clean(row.get("employer")),
                        "occupation":        clean(row.get("occupation")),
                        # Only a candidate filer's own committee gets a
                        # candidate_name — a PAC's contributions must not be
                        # attributed to a candidate. Both come from the shared
                        # resolution above so they match candidates.csv exactly.
                        "candidate_name":    res["candidate_name"],
                        "office":            res["office"],
                        "election_year":     ent.get("election_year")
                                             or parse_year(row.get("election_year")),
                        # No filing_id: ELEC exposes no filing identifier on
                        # contributions. CONTRIB_S looks like one but is a
                        # CONTRIBUTOR surrogate key (the dashboard's `cid`),
                        # so writing it here would be actively misleading.
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    total_contributions += 1
                    rows_in_file += 1

            log.file_parsed(path.name, "contributions", rows_in_file,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ─────────────────────── expenditures ──────────────────────────
        for path in raw_files("expenditures_*.csv"):
            ft = time.perf_counter()
            rows_in_file = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    # Expenditure rows carry ENTITY_S, so prefer the exact
                    # per-cycle entity and only fall back to the name+year
                    # match that contributions are limited to.
                    ent = entities.get(clean(row.get("eid"))) or lookup(row)

                    _street, city, st, zipc = split_address(row.get("address"))

                    committee_name = (utils.clean_name(ent.get("name", ""))
                                      or utils.clean_name(row.get("entity_name", ""))
                                      or utils.clean_name(row.get("recipient", "")))

                    # Expenditures carry ENTITY_S, so resolve by eid where we
                    # can and only fall back to the name+year key.
                    res = resolved_by_eid.get(clean(row.get("eid"))) \
                        or resolved_by_name_year.get(
                            (utils.clean_name(row.get("entity_name", "")),
                             parse_year(row.get("election_year"))), _EMPTY)
                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   committee_name,
                        "amount":           parse_amount(row.get("amount")),
                        "date":             parse_date(row.get("date")),
                        # ELEC has no expenditure "type" separate from the
                        # purpose text, so Expense Desc drives both. The
                        # expenditure_categories aliases map it to a category.
                        "transaction_type": clean(row.get("expense_desc")),
                        "payee_name":       utils.clean_name(row.get("receiver")),
                        "purpose":          clean(row.get("expense_desc")),
                        "category":         clean(row.get("receiver_type")),
                        "payee_city":       city,
                        "payee_state":      st,
                        "payee_zip":        zipc,
                        "candidate_name":   res["candidate_name"],
                        "office":           res["office"],
                        "election_year":    ent.get("election_year")
                                            or parse_year(row.get("election_year")),
                        # Filer-supplied check number — frequently blank, but
                        # it's the only per-expenditure identifier ELEC gives.
                        "filing_id":        clean(row.get("check_num")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    total_expenditures += 1
                    rows_in_file += 1

            log.file_parsed(path.name, "expenditures", rows_in_file,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ──────────────────── candidates + committees ──────────────────
        # One ELEC entity == one committee row, and additionally one candidate
        # row when the Office/Cmte value names a real office.
        for ent in entities.values():
            eid    = ent["eid"]
            office = ent["office_cmte"]
            res    = resolved_by_eid[eid]
            is_cand = res["is_cand"]

            district, jurisdiction = split_location(ent["location"])
            cmte_name = utils.clean_name(ent["name"])

            cmte_w.writerow({
                "state":          STATE,
                "committee_name": cmte_name,
                # For candidate filers the office doubles as the committee
                # type; committee_types.csv maps both shapes to canonical labels.
                "committee_type": office,
                "election_year":  ent["election_year"],
                "candidate_name": res["candidate_name"],
                # treasurer_name / city / zip are intentionally absent: ELEC's
                # search API publishes no committee contact details anywhere.
                # See docs/states/new_jersey.md § Data Notes.
                "state_filer_id": eid,
                "raw_file":       ent["raw_file"],
                "row_num":        ent["row_num"],
            })
            committees_written += 1

            if is_cand:
                cand_w.writerow({
                    "state":           STATE,
                    "candidate_name":  res["candidate_name"],
                    "candidate_first": res["candidate_first"],
                    "candidate_last":  res["candidate_last"],
                    "office":          office,
                    "district":        district,
                    "jurisdiction":    jurisdiction,
                    "party":           ent["party"],
                    "election_year":   ent["election_year"],
                    "state_filer_id":  eid,
                    "raw_file":        ent["raw_file"],
                    "row_num":         ent["row_num"],
                })
                candidates_written += 1

        # loans_debts: NJ publishes no loan/debt schedule through this search
        # system — the header written by open_writer is the whole file.

        # ─────────── close handles before person-ID assignment ─────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # ───────────────────────── person IDs ──────────────────────────
        # "committee": ELEC mints a new ENTITY_S per entity per cycle, so the
        # same person carries a different filer id in every election they run
        # in. Grouping on (state, name, office, district) collapses those.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz",
                                id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        # ───────────────────────── output stats ────────────────────────
        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions",
                        total_contributions, role="output",
                        bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz", "expenditures",
                        total_expenditures, role="output",
                        bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("candidates.csv.gz", "candidates",
                        candidates_written, role="output",
                        bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz", "committees",
                        committees_written, role="output",
                        bytes=_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz", "loans_debts",
                        0, role="output",
                        bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=committees_written,
                  candidates=candidates_written)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=committees_written,
                  candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=committees_written,
                  candidates=candidates_written,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


# ================================ CLI =================================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
