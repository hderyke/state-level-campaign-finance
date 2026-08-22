"""
parsers/south_carolina.py — Transform South Carolina raw JSON into the 5
normalized relations.

Reads data/South Carolina/raw/ and writes data/South Carolina/cleaned/.

Raw inputs (all written by scrapers/south_carolina.py):

    contributions_{year}.json   {"relation","year","retrieved_at","rows":[...]}
    expenditures_{year}.json    same envelope
    reports_{year}.json         same envelope — one row per filed disclosure
                                report; the only bulk source of candidate,
                                office, election-year and election-type data
                                on the portal
    election_history_{year}.csv SC Election Commission election history export,
                                one file per year, each with its own header;
                                used for tier-2 backfill (see below). A single
                                combined election_history.csv written by the
                                pre-split scraper is still read when no per-year
                                files are present.

TOLERANT FIELD LOOKUP
---------------------
The portal's search API is private and undocumented — its JSON key names are
not contractual and have no published schema. Rather than hardcode one
spelling, every field is read through `pick()`, which matches against a
normalized (lowercased, punctuation-stripped) index of the row's keys and
accepts a list of plausible names. `contributorName`, `Contributor_Name` and
`CONTRIBUTORNAME` all resolve identically, and an added or renamed key costs an
alias entry rather than a parser rewrite. Nested objects are flattened into the
same index, both bare and parent-prefixed, so `{"contributor":{"name":...}}`
resolves for either `name` or `contributorName`.

NAME FORMATS DIFFER BETWEEN SCREENS
-----------------------------------
The contributions screen renders candidate names as "Allen Wooten Jr." while
the expenditures and reports screens render the same people as
"Kendrick, Robert S". Left alone, the two halves of the dataset would never
join. `person_name()` detects the inverted form and flips it, so every table
carries "FIRST MIDDLE LAST" and committee/candidate matching works across
sources.

ADDRESSES ARE A SINGLE UNSPLIT STRING
-------------------------------------
Contributor and vendor addresses arrive as one line — "515 Handsome Oak Drive
Hardeeville, SC 29927" — with no delimiter between street and city (the only
comma sits between city and state). State and ZIP are recovered reliably from
the tail. City is recovered by walking backwards from the state code and
stopping at the first token that carries a digit or is a street-type word
("Drive", "Box", "Ste"), which resolves the observed forms correctly. Anything
that doesn't match cleanly leaves city empty rather than guessing.

TIER-2 BACKFILL FROM ELECTION HISTORY
-------------------------------------
ethicsfiling.sc.gov publishes no party, district, incumbency or jurisdiction
data anywhere, and has no candidate or committee registry to enrich from. Those
columns are filled by joining candidates against the election-history files on
normalized candidate name — exact full name first, then an unambiguous
first+last fallback, mirroring utils.assign_committee_person_ids. When a person
appears in several contests the most recent one wins, so party reflects their
latest ballot appearance. Candidates with no election-history match keep those
columns empty; the join rate is reported via log.enrichment_summary.

OTHER NOTES
-----------
  - person_id uses id_model="name_hash", but NOT because the portal has no
    identifier — it has one, and state_filer_id now carries it. The
    contributions screen returns it as `candidateId` and the expenditures screen
    as `candidateFilerId`, and the two are one id space: Henry McMaster is
    {15051, 11951} on both. Both are 100% filled.

    It is not a person key, though. The portal issues an id per candidacy, so a
    filer who runs again gets another one: over 2017, contributions carried 838
    ids across 820 names and expenditures 1,019 across 995, with ~2% of names
    holding several (Kevin L Bryant has three). No id ever spanned two names, so
    recording it cannot merge distinct filers — but keying person_id on it would
    split one person into three, which is worse than the name collisions
    name_hash risks. Hence: name_hash for person_id, the portal's id for
    state_filer_id, most recent candidacy winning where a filer has several.

    The reports screen used to be the only source of state_filer_id, through a
    `personId` the portal has since stopped sending — a 2019 reports file
    carries no identifier at all. That dead lookup, not an absence of data, is
    why state_filer_id validated at 0%.
  - Every filer on ethicsfiling.sc.gov is a candidate or public official — the
    screens sit under /candidates-public-officials — so committees sourced from
    it are written with committee_type "Candidate Committee". ethicsfiling.sc.gov
    itself publishes no loan or debt schedule, so loans_debts.csv.gz is header-only
    from that source.
  - Standalone PACs, Caucus/State/County/City Political Party committees, and
    Ballot Measure committees are all the same second source: apps.sc.gov,
    scraped only when --pacs / --party-caucus / --ballot-measure is passed
    respectively (see scrapers/south_carolina.py and the "Non-Candidate
    Committees" / "Caucus & Party Committees" / "Ballot Measure Committees"
    sections below). Each is optional, so its raw/ directory may not exist —
    when it doesn't, the matching parse_*() function is a no-op and
    loans_debts.csv.gz stays header-only as above.
"""

import csv
import gzip
import json
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

# Expenditure descriptions run long — well past Python's ~131 KB default. Not
# sys.maxsize: csv.field_size_limit takes a C long, which is 32-bit on Windows,
# so sys.maxsize raises OverflowError at import there. 10 MB matches validate.py.
csv.field_size_limit(10 * 1024 * 1024)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "South Carolina" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "South Carolina" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "SC"

# The scraper writes election history one file per year —
# raw/election_history_<year>.csv, each with its own header. See
# scrapers/south_carolina.py for why it is split rather than combined.
ELECTION_HISTORY_RELATION = "election_history"
ELECTION_HISTORY_GLOB     = f"{ELECTION_HISTORY_RELATION}_*.csv"

# What the scraper wrote before the split. Read only when no per-year files
# exist, so a raw/ directory captured by the older scraper still parses without
# a re-scrape — this export is hundreds of MB per year and re-downloading it to
# satisfy a rename would be a poor trade. Never preferred over per-year files:
# a directory holding both is one mid-migration, where the per-year files are
# the current truth and the combined file is the stale copy.
LEGACY_ELECTION_HISTORY_FILE = "election_history.csv"


# ========================= tolerant field lookup ======================

def _nk(key: str) -> str:
    """Normalize a key for matching: lowercase, drop everything non-alphanumeric."""
    return re.sub(r"[^a-z0-9]", "", (key or "").lower())


# Aggregate lines the election-history export carries in its candidate_name
# column. They are per-contest totals, not people, and are indistinguishable
# from a candidate to everything downstream — person_name() happily normalizes
# "Total Ballots Cast" into a name, after which the first+last fallback join can
# match a real filer against it.
_TALLY_ROWS = {_nk(s) for s in (
    "Total Ballots Cast", "Total Votes Cast", "Overvotes/Undervotes",
    "Overvotes", "Undervotes", "Write-In", "Write-Ins", "Blank Votes",
    # Ballot-measure response options. These sit in candidate_name on
    # question rows exactly as the tallies do, and are normally caught earlier
    # by the question_text check in _index_history_file — this is the backstop
    # for a question row that arrives without question_text populated.
    "Yes", "No", "For", "Against",
)}


def index_row(row: dict, _prefix: str = "", _depth: int = 0) -> dict:
    """Flatten a raw record into {normalized_key: scalar}.

    Nested objects are registered twice — once under the bare child key and once
    under parent+child — so a value can be found whether the API nests it or
    not. Depth is capped at 2: these payloads are shallow, and an unbounded walk
    on an unknown schema is an easy way to blow the stack on a self-referencing
    structure.
    """
    out: dict[str, str] = {}
    if not isinstance(row, dict):
        return out

    # Two passes, not one: a top-level key must win over a nested one that
    # normalizes to the same name. A single pass would make precedence depend on
    # dict insertion order, so {"contributor":{"name":...},"contributorName":...}
    # would resolve to whichever the API happened to emit first.
    nested: list[tuple[str, dict]] = []
    for key, val in row.items():
        nk = _nk(key)
        if isinstance(val, dict):
            if _depth < 2:
                nested.append((nk, val))
            continue
        if isinstance(val, list):
            continue
        if isinstance(val, bool):
            val = "Yes" if val else "No"
        text = "" if val is None else str(val).strip()
        out.setdefault(nk, text)
        if _prefix:
            out.setdefault(_prefix + nk, text)

    for child_prefix, child in nested:
        for child_key, child_val in index_row(child, child_prefix, _depth + 1).items():
            out.setdefault(child_key, child_val)
    return out


def pick(idx: dict, *names: str, default: str = "") -> str:
    """First non-empty value among the given candidate field names."""
    for name in names:
        val = idx.get(_nk(name))
        if val:
            return val
    return default


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to an empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Parse a dollar amount to a plain numeric string. Returns '' on failure."""
    v = clean(val).replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]           # accounting-style negative
    try:
        float(v)
        return v
    except ValueError:
        return ""


_DATE_FORMATS = ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%b %d, %Y", "%m/%d/%y")


def parse_date(val: str) -> str:
    """Normalize a date to YYYY-MM-DD. Returns '' on failure or implausible year."""
    v = clean(val)
    if not v:
        return ""
    # ISO-8601 with a time component ("2018-12-31T00:00:00") — keep the date half
    if "T" in v and re.match(r"^\d{4}-\d{2}-\d{2}T", v):
        v = v.split("T", 1)[0]
    for fmt in _DATE_FORMATS:
        try:
            d = datetime.strptime(v, fmt)
        except ValueError:
            continue
        if d.year < 1990 or d.year > date.today().year + 2:
            return ""
        return d.strftime("%Y-%m-%d")
    return ""


def year_of(iso_date: str) -> str:
    """Year component of a YYYY-MM-DD string, or ''."""
    return iso_date[:4] if len(iso_date) >= 4 and iso_date[:4].isdigit() else ""


_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "MD", "DDS", "ESQ", "PHD"}

# Honorifics the portal leaves attached to filer names ("Young, Dr. Hester").
# They break the join against election history, which never carries them.
_TITLES = {"DR", "MR", "MRS", "MS", "MISS", "REV", "HON", "SEN", "REP",
           "JUDGE", "SGT", "CAPT", "COL", "GEN", "PROF"}


def person_name(val: str, strip_title: bool = False) -> str:
    """Normalize a person name to "FIRST MIDDLE LAST", uppercased.

    The portal mixes two conventions across its screens (see module docstring):
    "Kendrick, Robert S" on expenditures/reports vs. "Allen Wooten Jr." on
    contributions. Anything containing a comma is treated as inverted and
    flipped; a trailing generational suffix on the surname half ("Smith Jr.,
    John") is kept attached to the surname so it doesn't migrate to the front.

    Organization names — which also show up in these fields, e.g. "South
    Carolina Federal Credit Union" — have no comma and pass through unchanged
    apart from case and whitespace normalization.

    strip_title drops a leading honorific ("DR. HESTER YOUNG" → "HESTER YOUNG").
    It is opt-in and used only for filer/candidate names: contributor and vendor
    names are frequently organizations, and some legitimately begin with one of
    these words ("MR ROOTER PLUMBING").
    """
    v = utils.clean_name(val)
    if not v:
        return v

    if "," in v:
        last, _, rest = v.partition(",")
        last, rest = last.strip(), rest.strip()
        if not last or not rest:
            v = utils.clean_name(v.replace(",", " "))
        else:
            # "Smith, John Jr." — a suffix trailing the given-name half belongs
            # at the end, not in front of the surname.
            tokens = rest.split()
            suffix = ""
            if len(tokens) > 1 and tokens[-1].rstrip(".") in _SUFFIXES:
                suffix = tokens.pop()
            v = utils.clean_name(
                f"{' '.join(tokens)} {last}" + (f" {suffix}" if suffix else ""))

    if strip_title:
        tokens = v.split()
        while len(tokens) > 2 and tokens[0].rstrip(".") in _TITLES:
            tokens.pop(0)
        v = " ".join(tokens)

    return v


def name_parts(name: str) -> tuple[str, str]:
    """(first, last) from a normalized "FIRST MIDDLE LAST" name.

    Honorifics and generational suffixes are excluded from both ends — without
    this, "ALLEN WOOTEN JR." yields a last name of "JR." and "DR. HESTER YOUNG"
    a first name of "DR.".
    """
    tokens = [t for t in name.split() if t]
    while len(tokens) > 2 and tokens[0].rstrip(".") in _TITLES:
        tokens.pop(0)
    while len(tokens) > 2 and tokens[-1].rstrip(".") in _SUFFIXES:
        tokens.pop()
    if not tokens:
        return "", ""
    return tokens[0], (tokens[-1] if len(tokens) > 1 else "")


# Street-type words that mark the end of the street portion of an address.
# Walking backwards from the state code, the city is whatever sits between one
# of these (or a token containing a digit) and the state.
_STREET_WORDS = {
    "ST", "STREET", "RD", "ROAD", "DR", "DRIVE", "AVE", "AVENUE", "LN", "LANE",
    "CT", "COURT", "BLVD", "BOULEVARD", "WAY", "CIR", "CIRCLE", "PKWY",
    "PARKWAY", "HWY", "HIGHWAY", "TRL", "TRAIL", "PL", "PLACE", "TER",
    "TERRACE", "LOOP", "RUN", "PT", "POINT", "SQ", "SQUARE", "BOX", "PO",
    "APT", "STE", "SUITE", "UNIT", "FLOOR", "FL", "BLDG", "RM",
}

_ADDR_TAIL = re.compile(
    r"[,\s]+(?P<st>[A-Za-z]{2})\.?[,\s]+(?P<zip>\d{5}(?:-\d{4})?|\d{9})\s*$"
)


def split_address(val: str) -> tuple[str, str, str]:
    """Best-effort (city, state, zip) from a single-line address string.

    Returns empty strings for anything that can't be recovered confidently —
    a wrong city is worse than a missing one, since these feed geographic
    rollups in the aggregate database.
    """
    v = re.sub(r"\s+", " ", clean(val))
    if not v:
        return "", "", ""

    m = _ADDR_TAIL.search(v)
    if not m:
        return "", "", ""

    st   = m.group("st").upper()
    zipc = utils.clean_zip(m.group("zip"))
    head = v[: m.start()].strip().rstrip(",").strip()

    city_tokens: list[str] = []
    bounded = False               # did the walk stop at a real street boundary?
    for token in reversed(head.split()):
        bare = token.strip(".,#").upper()
        if not bare or any(ch.isdigit() for ch in bare) or bare in _STREET_WORDS:
            bounded = True
            break
        city_tokens.insert(0, token)
        if len(city_tokens) == 3:
            # Three alphabetic tokens with no house number or street word behind
            # them means the street name itself is bleeding into the city
            # ("123 N Main Mount Pleasant" → "MAIN MOUNT PLEASANT"). Give up
            # rather than emit a wrong city.
            return "", st, zipc
    else:
        # Ran out of tokens — the whole head is the city ("Columbia, SC 29260").
        bounded = True

    if not bounded:
        return "", st, zipc
    return utils.clean_name(" ".join(city_tokens)), st, zipc


# "SC Senate District 10", "School Board Trustee District GREENVILLE",
# "Coroner No. 2" — the district is glued onto the office string and there is no
# separate field for it anywhere in the source.
#
# The `(?![A-Z])` is load-bearing. `\b` only anchors the LEFT edge of the
# keyword, so `NO` also matched the first two letters of any word starting with
# it, and the capture then swallowed the rest of the string: "NORTH AUGUSTA CITY
# COUNCIL" produced the district "RTH AUGUSTA CITY COUNCIL". North Augusta,
# North Charleston and Norway are all real SC municipalities, so this was
# firing on live data via the expenditures screen.
#
# The lookahead is applied to the word keywords only, not to `#` — a bare `#`
# is legitimately followed by a letter ("Seat #A"), and requiring a non-letter
# after it would drop those.
#
# `\b` likewise scopes to the word keywords only. It used to sit in front of the
# whole group, including the `#` branch, where it could never match: `\b` needs a
# word character on one side, and in "Council #3" the `#` has a space to its left
# and a digit to its right. That branch was dead for every string with a space
# before the `#` — which is all of them — so "Council #3" returned no district
# while "Council Seat #A" worked, the `#` there being consumed by the `[:#]?`
# separator rather than matched as a keyword.
_DISTRICT_RE = re.compile(
    r"(?:\b(?:DISTRICT|DIST|SEAT|NO)\.?(?![A-Z])|#)"
    r"\s*[:#]?\s*(?P<d>[A-Z0-9][A-Z0-9 .#/-]*)$"
)


def split_office(val: str) -> tuple[str, str]:
    """(office, district) from a combined SC office string.

    The office is returned whole — truncating it would lose meaning ("SC Senate"
    alone is fine, but "School Board Trustee" without its county is not) — and
    the district is additionally surfaced on its own so it can be grouped on.
    """
    office = utils.clean_name(val)
    if not office:
        return "", ""
    m = _DISTRICT_RE.search(office)
    return office, (m.group("d").strip() if m else "")


def yes_no(val: str) -> str:
    """Normalize a boolean-ish source value to 'Yes' / 'No' / ''."""
    v = clean(val).upper()
    if v in ("YES", "Y", "TRUE", "1"):
        return "Yes"
    if v in ("NO", "N", "FALSE", "0"):
        return "No"
    return ""


def raw_files(pattern: str) -> list[Path]:
    """Non-empty raw files matching a glob, in filename (i.e. year) order."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def load_envelope(path: Path) -> tuple[list[dict], str]:
    """Read a scraper envelope → (rows, year). Tolerates a bare JSON array."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        rows, year = payload, ""
    else:
        rows = payload.get("rows") or []
        year = str(payload.get("year") or "")
    if not year:
        m = re.search(r"_(\d{4})\.json$", path.name)
        year = m.group(1) if m else ""
    return [r for r in rows if isinstance(r, dict)], year


# ==================== Non-Candidate Committees (PACs) ====================
# Raw input: data/South Carolina/raw/noncand/filings/*.json, written by
# scrapers/south_carolina.py's opt-in --pacs sweep against apps.sc.gov -- a
# completely different site from ethicsfiling.sc.gov above, covering
# standalone PACs rather than candidates and public officials. See
# docs/states/south_carolina.md "Non-Candidate Committees" for the full
# reverse-engineering writeup. Each file is one committee's whole filing
# history:
#
#   {"committee": "...", "demographics": {"address","city","state","zip","phone"},
#    "filings": [{"period","date_filed","version",
#                 "contributions"?, "expenditures"?, "loans"?, "loan_payments"?}]}
#
# A filing's category key is present only when that period had itemized rows
# to report -- the scraper fetches a tab only when the summary page's
# PERIOD total for it is nonzero (or, for loans/loan_payments, always, since
# no reliable zero signal exists on the summary page for those two).

NONCAND_GLOB = "noncand/filings/*.json"

_NONCAND_ADDR_TAIL = re.compile(
    r"^(?P<city>.*?),\s*(?P<st>[A-Za-z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$")


def _noncand_address(val: str) -> tuple[str, str, str]:
    """(city, state, zip) from this site's two-line 'street\\ncity, ST zip'
    address format -- distinct from split_address() above, which handles a
    single unsplit line. The last line here is already just 'City, ST ZIP'
    with nothing else mixed in, so no street-word boundary walk is needed."""
    lines = [l.strip() for l in clean(val).split("\n") if l.strip()]
    if not lines:
        return "", "", ""
    m = _NONCAND_ADDR_TAIL.match(re.sub(r"\s+", " ", lines[-1]))
    if not m:
        return "", "", ""
    return (utils.clean_name(m.group("city")), m.group("st").upper(),
            utils.clean_zip(m.group("zip")))


def _noncand_city_state_zip(idx: dict) -> tuple[str, str, str]:
    """(city, state, zip) for a Contributions/Expenditures item, preferring
    the DisplayCsv.aspx export's pre-split CITY/STATE/ZIP columns (see
    scrapers/south_carolina.py's _noncand_csv_rows -- confirmed live
    2026-08-20 across Non-Candidate, Caucus, State Party, and Ballot
    Measure) over the older HTML table's single two-line 'street\\ncity, ST
    zip' Address cell (_noncand_address). The HTML-table shape is still
    what Loans/Repayments always produce (that export doesn't exist for
    those two tabs) and what any committee's raw JSON still carries if it
    hasn't been rewalked since this rebuild, so both paths stay live
    rather than the older one being dropped."""
    city, state, zipc = pick(idx, "city"), pick(idx, "state"), pick(idx, "zip")
    if city or state or zipc:
        return (utils.clean_name(city) if city else "",
                state.upper() if state else "",
                utils.clean_zip(zipc) if zipc else "")
    return _noncand_address(pick(idx, "address"))


_NONCAND_ITEM_CATEGORIES = ("contributions", "expenditures", "loans", "loan_payments")


def _noncand_date_filed_key(date_filed: str):
    """Sortable key for a filing's date_filed ('10/14/2010') -- returns
    datetime.min for anything unparseable so it never wins a tiebreak."""
    try:
        return datetime.strptime(clean(date_filed), "%m/%d/%Y")
    except ValueError:
        return datetime.min


def _noncand_dedupe_filings(filings: list[dict]) -> list[dict]:
    """Collapse duplicate itemized rows that apps.sc.gov re-lists across
    multiple filings for the same committee -- confirmed directly against
    the live site (2026-08-03), not inferred from the output alone:

      1. Same-period amendments are full restatements, not incremental.
         Amendment N for a period re-lists every item Amendment N-1 (or the
         Original) reported for that SAME period, plus whatever changed.
         Verified live: Sumter Committee for Excellence's 'July 10th'
         period grew 2 expenditure items (Original) -> 3 (Amendment 1) ->
         4 (Amendment 2), each version a strict superset of the last.
      2. Some report-index rows don't carry independent content at all.
         Observed cause: rows with a blank period land on ReviewSummary.aspx
         instead of the normal ViewReport.aspx, and the itemized tabs read
         from server-side session state that page apparently doesn't set --
         so the tab silently re-serves whatever filing was last properly
         loaded. Proven by walking filings in a different order and watching
         the blank-period row's "content" change to match whatever was
         walked immediately before it. Also directly provable from the data
         alone: RGA South Carolina 2010 PAC's blank-period filing (filed
         10/07/2010) showed expenditures dated 10/27-10/28/2010 -- a filing
         cannot report a transaction that hadn't happened yet.

    Net effect measured across the full 332-committee dataset before this
    fix existed: 21,338 duplicate contribution rows (14.9% of all noncand
    contributions) and 11,087 duplicate expenditure rows (17.3%), spread
    across 162 committees -- real transactions double/triple/quadruple
    counted, not fabricated ones.

    Fix: collapse to one row per distinct item -- identified by its full set
    of source fields (date, name, amount, description/occupation, exactly as
    the site presents them, so this needs no assumption about which of
    those fields matter) -- across a committee's ENTIRE filing history,
    keeping the copy from whichever filing is the most trustworthy source:
    prefer a filing with a non-blank period (mechanism 2 above never
    produces real content) and, among those, the latest date_filed
    (mechanism 1's most-current restatement). A real transaction recurring
    with byte-identical date/name/amount/description elsewhere in the same
    committee's history by pure coincidence is not a realistic concern, so
    this is safe as a general rule even for edge cases neither mechanism
    above explains."""
    # content key -> (filings[] index, has_period, date_filed_key) of the
    # best candidate seen so far for that key
    best: dict[tuple, tuple[int, bool, object]] = {}
    for i, f in enumerate(filings):
        has_period = bool(clean(f.get("period", "")))
        dt_key = _noncand_date_filed_key(f.get("date_filed", ""))
        for category in _NONCAND_ITEM_CATEGORIES:
            for item in f.get(category, []):
                key = (category, tuple(sorted(item.items())))
                cur = best.get(key)
                if cur is None:
                    best[key] = (i, has_period, dt_key)
                    continue
                _, cur_has_period, cur_dt = cur
                # A non-blank-period source always beats a blank-period one
                # (mechanism 2 -- blank-period content is never trustworthy);
                # among equally-eligible sources, the latest date_filed wins
                # (mechanism 1 -- later restatements supersede earlier ones).
                if (has_period, dt_key) > (cur_has_period, cur_dt):
                    best[key] = (i, has_period, dt_key)

    deduped = []
    for i, f in enumerate(filings):
        new_f = {k: v for k, v in f.items() if k not in _NONCAND_ITEM_CATEGORIES}
        for category in _NONCAND_ITEM_CATEGORIES:
            items = f.get(category)
            if not items:
                continue
            kept, emitted = [], set()
            for item in items:
                key = (category, tuple(sorted(item.items())))
                if best[key][0] != i or key in emitted:
                    # either this filing lost the cross-filing tiebreak, or
                    # this exact item is listed more than once on this one
                    # filing's own page (observed directly, e.g. Justice PAC
                    # Nine's 10/09/2015 filing lists one $1,000 contribution
                    # to Margie Bright Matthews twice in its own table) --
                    # either way, one row per distinct item is correct.
                    continue
                emitted.add(key)
                kept.append(item)
            if kept:
                new_f[category] = kept
        deduped.append(new_f)
    return deduped


def _noncand_election_year(filing: dict) -> str:
    """'2012, January 10th' -> '2012'. Falls back to the last 4 digits of
    date_filed (a real calendar date, so always usable) if period is ever
    unparseable."""
    m = re.match(r"^(\d{4})", clean(filing.get("period")))
    if m:
        return m.group(1)
    date_filed = clean(filing.get("date_filed"))
    return date_filed[-4:] if len(date_filed) >= 4 else ""


def parse_noncand_pacs(log, cmte_w, cont_w, expn_w, loan_w,
                       existing_committee_names: set) -> dict:
    """Read every scraped Non-Candidate committee filing and write
    committees/contributions/expenditures/loans_debts rows directly through
    the run() writers already open. Returns counts for the caller's summary
    line. A no-op (all zeros) when raw/noncand/ doesn't exist -- the scrape
    step is opt-in, so most runs won't have it."""
    paths = raw_files(NONCAND_GLOB)
    if not paths:
        return {"committees": 0, "contributions": 0, "expenditures": 0, "loans": 0}

    n_committees = n_contrib = n_expn = n_loan = 0
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.file_parse_error(path.name, str(e))
            continue

        name = clean(record.get("committee"))
        if not name:
            continue

        if name in existing_committee_names:
            # Not observed in practice, but candidate and PAC names are drawn
            # from unrelated sources with no shared ID -- a collision can only
            # be caught by name. The candidate-side row wins; PAC transactions
            # below are written under the shared name regardless, same as
            # they'd join downstream either way.
            log.warning(f"  [noncand] '{name}' matches an existing candidate "
                        f"committee name -- keeping that row, not adding a "
                        f"second committees.csv entry")
        else:
            demo = record.get("demographics") or {}
            cmte_w.writerow({
                "state":          STATE,
                "person_id":      "",
                "committee_name": name,
                # Raw value, mapped to canonical "PAC" in
                # src/aliases/committee_types.csv -- same passthrough
                # convention as committee_type everywhere else in this parser.
                "committee_type": "Non-Candidate Committee",
                "election_year":  "",
                "candidate_name": "",
                "treasurer_name": "",
                "city":           demo.get("city", ""),
                "zip":            demo.get("zip", ""),
                "active":         "",
                "state_filer_id": "",
                "raw_file":       path.name,
                "row_num":        1,
            })
            existing_committee_names.add(name)
            n_committees += 1

        filings = _noncand_dedupe_filings(record.get("filings", []))
        for row_num, filing in enumerate(filings, start=1):
            date_filed    = clean(filing.get("date_filed"))
            election_year = _noncand_election_year(filing)
            # No per-transaction ID exists on this site at all -- filing_id
            # identifies the REPORT (date_filed), not the individual row,
            # same tradeoff several other states make when the source has no
            # finer-grained key.
            amended = "" if clean(filing.get("version")).lower() == "original" else "Yes"

            for item in filing.get("contributions", []):
                idx = index_row(item)
                # _noncand_city_state_zip(), not _noncand_address(pick(idx,
                # "address")) -- prefers the DisplayCsv.aspx export's
                # pre-split CITY/STATE/ZIP over the older combined-Address
                # cell (falls back to it automatically when those keys
                # aren't present, e.g. an item still in the old HTML-table
                # shape). See _noncand_city_state_zip's docstring.
                city, st, zipc = _noncand_city_state_zip(idx)
                occupation = pick(idx, "occupation")
                if _nk(occupation) == "unknown":
                    occupation = ""
                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    name,
                    "amount":            parse_amount(pick(idx, "amount")),
                    # "contribution_date" -- the CSV export's own column
                    # name (CONTRIBUTION_DATE) normalizes to
                    # "contributiondate" under _nk(), distinct from the old
                    # HTML table's plain "Date" -- both candidates needed so
                    # neither raw-JSON shape loses this field.
                    "date":              parse_date(pick(idx, "date", "contribution_date")),
                    "transaction_type":  "",
                    "contributor_name":  clean(pick(idx, "contributor")),
                    "contributor_type":  "",
                    "contributor_city":  city,
                    "contributor_state": st,
                    "contributor_zip":   zipc,
                    "employer":          "",
                    "occupation":        occupation,
                    "candidate_name":    "",
                    "office":            "",
                    "election_year":     election_year,
                    "amended":           amended,
                    "filing_id":         date_filed,
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                n_contrib += 1

            for item in filing.get("expenditures", []):
                idx = index_row(item)
                # See the matching comment in the contributions loop above.
                city, st, zipc = _noncand_city_state_zip(idx)
                expn_w.writerow({
                    "state":            STATE,
                    "committee_name":   name,
                    # "expenditure_amount"/"expenditure_desc" -- the CSV
                    # export's own column names (EXPENDITURE_AMOUNT,
                    # EXPENDITURE_DESC) normalize to "expenditureamount"/
                    # "expendituredesc" under _nk(), distinct from the old
                    # HTML table's plain "Amount"/"Description" -- both
                    # candidates needed so neither raw-JSON shape loses
                    # these fields. DATE needs no extra candidate: the CSV
                    # export uses a plain "DATE" column here already.
                    "amount":           parse_amount(pick(idx, "amount", "expenditure_amount")),
                    "date":             parse_date(pick(idx, "date")),
                    "transaction_type": "",
                    "payee_name":       clean(pick(idx, "vendor", "payee")),
                    "purpose":          clean(pick(idx, "description", "purpose", "expenditure_desc")),
                    "category":         "",
                    "payee_city":       city,
                    "payee_state":      st,
                    "payee_zip":        zipc,
                    "candidate_name":   "",
                    "office":           "",
                    "election_year":    election_year,
                    "amended":          amended,
                    "filing_id":        date_filed,
                    "raw_file":         path.name,
                    "row_num":          row_num,
                })
                n_expn += 1

            # Loans and Loan Payments share loans_debts.csv, distinguished by
            # record_type. Column names on these two tabs are UNCONFIRMED --
            # no real loan activity turned up during development to check
            # against (PACs rarely carry loans), so this reads tolerantly
            # through pick() with the same field names the other two tabs
            # use. Worth re-checking against a real filing if loans_debts
            # ever comes up empty for a PAC whose summary page shows a
            # nonzero loan balance.
            for label, items in (("Loan", filing.get("loans", [])),
                                 ("Loan Payment", filing.get("loan_payments", []))):
                for item in items:
                    idx = index_row(item)
                    city, st, zipc = _noncand_address(pick(idx, "address"))
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     name,
                        "original_amount":    parse_amount(pick(idx, "amount")),
                        "date":               parse_date(pick(idx, "date")),
                        "record_type":        label,
                        "counterparty_name":  clean(pick(idx, "lender", "vendor",
                                                         "contributor")),
                        "counterparty_city":  city,
                        "counterparty_state": st,
                        "counterparty_zip":   zipc,
                        "candidate_name":     "",
                        "election_year":      election_year,
                        "amended":            amended,
                        "filing_id":          date_filed,
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    n_loan += 1

    if n_committees:
        log.registry_loaded("noncand_committees", n_committees, relation="committees")
    return {"committees": n_committees, "contributions": n_contrib,
            "expenditures": n_expn, "loans": n_loan}


# ==================== Caucus & Party Committees ========================
# Raw input: data/South Carolina/raw/party_caucus/filings/*.json, written by
# scrapers/south_carolina.py's opt-in --party-caucus sweep -- Caucus and
# State/County/City Political Party committees, apps.sc.gov's four
# dropdown-driven committee lookups (as opposed to Non-Candidate's name
# search, above). Same site, same report-index/summary/itemized-tab
# structure, same JSON shape as Non-Candidate's raw files (this function
# reuses _noncand_dedupe_filings/_noncand_address/_noncand_election_year
# unchanged) -- the only real difference is one extra "source" key
# identifying which of the four dropdown lookups a given file came from,
# used here only to pick the right canonical committee_type.
#
# Campaign Disclosure filings only, matching the scraper's scope -- Caucus
# and Party committees also file a second, differently-shaped "Operating
# Disclosure" report the scraper doesn't walk yet (see
# docs/states/south_carolina.md), so there's nothing to filter out here;
# raw/party_caucus/filings/*.json simply never contains those rows.

PARTY_CAUCUS_GLOB = "party_caucus/filings/*.json"

_PARTY_CAUCUS_COMMITTEE_TYPE = {
    "caucus":       "Caucus Committee",
    "state_party":  "State Political Party",
    "county_party": "County Political Party",
    "city_party":   "City Political Party",
}


def parse_party_caucus(log, cmte_w, cont_w, expn_w, loan_w,
                       existing_committee_names: set) -> dict:
    """Read every scraped Caucus/State/County/City Political Party filing
    and write committees/contributions/expenditures/loans_debts rows
    directly through the run() writers already open. Returns counts for the
    caller's summary line. A no-op (all zeros) when raw/party_caucus/
    doesn't exist -- the scrape step is opt-in (--party-caucus), so most
    runs won't have it. Structurally near-identical to parse_noncand_pacs()
    above (same source site, same dedup needs, same collision handling for
    a committee_name that happens to match an existing row) -- see that
    function's docstring for the reasoning this one shares."""
    paths = raw_files(PARTY_CAUCUS_GLOB)
    if not paths:
        return {"committees": 0, "contributions": 0, "expenditures": 0, "loans": 0}

    n_committees = n_contrib = n_expn = n_loan = 0
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.file_parse_error(path.name, str(e))
            continue

        name = clean(record.get("committee"))
        if not name:
            continue

        source = record.get("source", "")
        committee_type = _PARTY_CAUCUS_COMMITTEE_TYPE.get(source, source or "Political Party")

        if name in existing_committee_names:
            # Not observed in practice, but candidate committees, PACs, and
            # Caucus/Party committees are drawn from unrelated sources with
            # no shared ID -- a collision can only be caught by name. The
            # existing row wins; transactions below are written under the
            # shared name regardless, same as they'd join downstream either
            # way.
            log.warning(f"  [party_caucus] '{name}' matches an existing "
                        f"committee name -- keeping that row, not adding a "
                        f"second committees.csv entry")
        else:
            demo = record.get("demographics") or {}
            cmte_w.writerow({
                "state":          STATE,
                "person_id":      "",
                "committee_name": name,
                # Raw value, mapped to canonical values in
                # src/aliases/committee_types.csv -- same passthrough
                # convention as committee_type everywhere else in this parser.
                "committee_type": committee_type,
                "election_year":  "",
                "candidate_name": "",
                "treasurer_name": "",
                "city":           demo.get("city", ""),
                "zip":            demo.get("zip", ""),
                "active":         "",
                "state_filer_id": "",
                "raw_file":       path.name,
                "row_num":        1,
            })
            existing_committee_names.add(name)
            n_committees += 1

        filings = _noncand_dedupe_filings(record.get("filings", []))
        for row_num, filing in enumerate(filings, start=1):
            date_filed    = clean(filing.get("date_filed"))
            election_year = _noncand_election_year(filing)
            amended = "" if clean(filing.get("version")).lower() == "original" else "Yes"

            for item in filing.get("contributions", []):
                idx = index_row(item)
                # _noncand_city_state_zip(), not _noncand_address(pick(idx,
                # "address")) -- prefers the DisplayCsv.aspx export's
                # pre-split CITY/STATE/ZIP over the older combined-Address
                # cell (falls back to it automatically when those keys
                # aren't present, e.g. an item still in the old HTML-table
                # shape). See _noncand_city_state_zip's docstring.
                city, st, zipc = _noncand_city_state_zip(idx)
                occupation = pick(idx, "occupation")
                if _nk(occupation) == "unknown":
                    occupation = ""
                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    name,
                    "amount":            parse_amount(pick(idx, "amount")),
                    # "contribution_date" -- the CSV export's own column
                    # name (CONTRIBUTION_DATE) normalizes to
                    # "contributiondate" under _nk(), distinct from the old
                    # HTML table's plain "Date" -- both candidates needed so
                    # neither raw-JSON shape loses this field.
                    "date":              parse_date(pick(idx, "date", "contribution_date")),
                    "transaction_type":  "",
                    "contributor_name":  clean(pick(idx, "contributor")),
                    "contributor_type":  "",
                    "contributor_city":  city,
                    "contributor_state": st,
                    "contributor_zip":   zipc,
                    "employer":          "",
                    "occupation":        occupation,
                    "candidate_name":    "",
                    "office":            "",
                    "election_year":     election_year,
                    "amended":           amended,
                    "filing_id":         date_filed,
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                n_contrib += 1

            for item in filing.get("expenditures", []):
                idx = index_row(item)
                # See the matching comment in the contributions loop above.
                city, st, zipc = _noncand_city_state_zip(idx)
                expn_w.writerow({
                    "state":            STATE,
                    "committee_name":   name,
                    # "expenditure_amount"/"expenditure_desc" -- the CSV
                    # export's own column names (EXPENDITURE_AMOUNT,
                    # EXPENDITURE_DESC) normalize to "expenditureamount"/
                    # "expendituredesc" under _nk(), distinct from the old
                    # HTML table's plain "Amount"/"Description" -- both
                    # candidates needed so neither raw-JSON shape loses
                    # these fields. DATE needs no extra candidate: the CSV
                    # export uses a plain "DATE" column here already.
                    "amount":           parse_amount(pick(idx, "amount", "expenditure_amount")),
                    "date":             parse_date(pick(idx, "date")),
                    "transaction_type": "",
                    "payee_name":       clean(pick(idx, "vendor", "payee")),
                    "purpose":          clean(pick(idx, "description", "purpose", "expenditure_desc")),
                    "category":         "",
                    "payee_city":       city,
                    "payee_state":      st,
                    "payee_zip":        zipc,
                    "candidate_name":   "",
                    "office":           "",
                    "election_year":    election_year,
                    "amended":          amended,
                    "filing_id":        date_filed,
                    "raw_file":         path.name,
                    "row_num":          row_num,
                })
                n_expn += 1

            # See parse_noncand_pacs() -- same UNCONFIRMED loans/loan_payments
            # header caveat applies here (no real loan activity turned up
            # during development to check against).
            for label, items in (("Loan", filing.get("loans", [])),
                                 ("Loan Payment", filing.get("loan_payments", []))):
                for item in items:
                    idx = index_row(item)
                    city, st, zipc = _noncand_address(pick(idx, "address"))
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     name,
                        "original_amount":    parse_amount(pick(idx, "amount")),
                        "date":               parse_date(pick(idx, "date")),
                        "record_type":        label,
                        "counterparty_name":  clean(pick(idx, "lender", "vendor",
                                                         "contributor")),
                        "counterparty_city":  city,
                        "counterparty_state": st,
                        "counterparty_zip":   zipc,
                        "candidate_name":     "",
                        "election_year":      election_year,
                        "amended":            amended,
                        "filing_id":          date_filed,
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    n_loan += 1

    if n_committees:
        log.registry_loaded("party_caucus_committees", n_committees, relation="committees")
    return {"committees": n_committees, "contributions": n_contrib,
            "expenditures": n_expn, "loans": n_loan}


# ==================== Ballot Measure Committees ========================
# Raw input: data/South Carolina/raw/ballot_measure/filings/*.json, written
# by scrapers/south_carolina.py's opt-in --ballot-measure sweep -- the sixth
# and last of apps.sc.gov's committee-type lookups (see "Non-Candidate
# Committees" and "Caucus & Party Committees" above). Name-search like
# Non-Candidate, not dropdown-driven like Caucus/Party -- same JSON shape as
# both (this function reuses _noncand_dedupe_filings/_noncand_address/
# _noncand_election_year unchanged).
#
# Campaign Disclosure filings only, matching the scraper's scope -- Ballot
# Measure committees also file a second, differently-shaped "Statement of
# Organization" report the scraper doesn't walk (see
# docs/states/south_carolina.md), so there's nothing to filter out here;
# raw/ballot_measure/filings/*.json simply never contains those rows.

BALLOT_MEASURE_GLOB = "ballot_measure/filings/*.json"


def parse_ballot_measure(log, cmte_w, cont_w, expn_w, loan_w,
                         existing_committee_names: set) -> dict:
    """Read every scraped Ballot Measure committee filing and write
    committees/contributions/expenditures/loans_debts rows directly through
    the run() writers already open. Returns counts for the caller's summary
    line. A no-op (all zeros) when raw/ballot_measure/ doesn't exist -- the
    scrape step is opt-in (--ballot-measure), so most runs won't have it.
    Structurally identical to parse_noncand_pacs() above (same source site,
    same name-search discovery, same dedup needs, same collision handling
    for a committee_name that happens to match an existing row) -- see that
    function's docstring for the reasoning this one shares. Only real
    difference: a single fixed committee_type rather than a per-source
    dropdown lookup table, since Ballot Measure has no sub-types."""
    paths = raw_files(BALLOT_MEASURE_GLOB)
    if not paths:
        return {"committees": 0, "contributions": 0, "expenditures": 0, "loans": 0}

    n_committees = n_contrib = n_expn = n_loan = 0
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.file_parse_error(path.name, str(e))
            continue

        name = clean(record.get("committee"))
        if not name:
            continue

        if name in existing_committee_names:
            # Not observed in practice, but candidate committees, PACs,
            # Caucus/Party committees, and Ballot Measure committees are
            # drawn from unrelated sources with no shared ID -- a collision
            # can only be caught by name. The existing row wins;
            # transactions below are written under the shared name
            # regardless, same as they'd join downstream either way.
            log.warning(f"  [ballot] '{name}' matches an existing committee "
                        f"name -- keeping that row, not adding a second "
                        f"committees.csv entry")
        else:
            demo = record.get("demographics") or {}
            cmte_w.writerow({
                "state":          STATE,
                "person_id":      "",
                "committee_name": name,
                # Raw value, mapped to canonical value in
                # src/aliases/committee_types.csv -- same passthrough
                # convention as committee_type everywhere else in this parser.
                "committee_type": "Ballot Measure Committee",
                "election_year":  "",
                "candidate_name": "",
                "treasurer_name": "",
                "city":           demo.get("city", ""),
                "zip":            demo.get("zip", ""),
                "active":         "",
                "state_filer_id": "",
                "raw_file":       path.name,
                "row_num":        1,
            })
            existing_committee_names.add(name)
            n_committees += 1

        filings = _noncand_dedupe_filings(record.get("filings", []))
        for row_num, filing in enumerate(filings, start=1):
            date_filed    = clean(filing.get("date_filed"))
            election_year = _noncand_election_year(filing)
            amended = "" if clean(filing.get("version")).lower() == "original" else "Yes"

            for item in filing.get("contributions", []):
                idx = index_row(item)
                # _noncand_city_state_zip(), not _noncand_address(pick(idx,
                # "address")) -- prefers the DisplayCsv.aspx export's
                # pre-split CITY/STATE/ZIP over the older combined-Address
                # cell (falls back to it automatically when those keys
                # aren't present, e.g. an item still in the old HTML-table
                # shape). See _noncand_city_state_zip's docstring.
                city, st, zipc = _noncand_city_state_zip(idx)
                occupation = pick(idx, "occupation")
                if _nk(occupation) == "unknown":
                    occupation = ""
                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    name,
                    "amount":            parse_amount(pick(idx, "amount")),
                    # "contribution_date" -- the CSV export's own column
                    # name (CONTRIBUTION_DATE) normalizes to
                    # "contributiondate" under _nk(), distinct from the old
                    # HTML table's plain "Date" -- both candidates needed so
                    # neither raw-JSON shape loses this field.
                    "date":              parse_date(pick(idx, "date", "contribution_date")),
                    "transaction_type":  "",
                    "contributor_name":  clean(pick(idx, "contributor")),
                    "contributor_type":  "",
                    "contributor_city":  city,
                    "contributor_state": st,
                    "contributor_zip":   zipc,
                    "employer":          "",
                    "occupation":        occupation,
                    "candidate_name":    "",
                    "office":            "",
                    "election_year":     election_year,
                    "amended":           amended,
                    "filing_id":         date_filed,
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                n_contrib += 1

            for item in filing.get("expenditures", []):
                idx = index_row(item)
                # See the matching comment in the contributions loop above.
                city, st, zipc = _noncand_city_state_zip(idx)
                expn_w.writerow({
                    "state":            STATE,
                    "committee_name":   name,
                    # "expenditure_amount"/"expenditure_desc" -- the CSV
                    # export's own column names (EXPENDITURE_AMOUNT,
                    # EXPENDITURE_DESC) normalize to "expenditureamount"/
                    # "expendituredesc" under _nk(), distinct from the old
                    # HTML table's plain "Amount"/"Description" -- both
                    # candidates needed so neither raw-JSON shape loses
                    # these fields. DATE needs no extra candidate: the CSV
                    # export uses a plain "DATE" column here already.
                    "amount":           parse_amount(pick(idx, "amount", "expenditure_amount")),
                    "date":             parse_date(pick(idx, "date")),
                    "transaction_type": "",
                    "payee_name":       clean(pick(idx, "vendor", "payee")),
                    "purpose":          clean(pick(idx, "description", "purpose", "expenditure_desc")),
                    "category":         "",
                    "payee_city":       city,
                    "payee_state":      st,
                    "payee_zip":        zipc,
                    "candidate_name":   "",
                    "office":           "",
                    "election_year":    election_year,
                    "amended":          amended,
                    "filing_id":        date_filed,
                    "raw_file":         path.name,
                    "row_num":          row_num,
                })
                n_expn += 1

            # See parse_noncand_pacs() -- same UNCONFIRMED loans/loan_payments
            # header caveat applies here (no real loan activity turned up
            # during development to check against).
            for label, items in (("Loan", filing.get("loans", [])),
                                 ("Loan Payment", filing.get("loan_payments", []))):
                for item in items:
                    idx = index_row(item)
                    city, st, zipc = _noncand_address(pick(idx, "address"))
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     name,
                        "original_amount":    parse_amount(pick(idx, "amount")),
                        "date":               parse_date(pick(idx, "date")),
                        "record_type":        label,
                        "counterparty_name":  clean(pick(idx, "lender", "vendor",
                                                         "contributor")),
                        "counterparty_city":  city,
                        "counterparty_state": st,
                        "counterparty_zip":   zipc,
                        "candidate_name":     "",
                        "election_year":      election_year,
                        "amended":            amended,
                        "filing_id":          date_filed,
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    n_loan += 1

    if n_committees:
        log.registry_loaded("ballot_measure_committees", n_committees, relation="committees")
    return {"committees": n_committees, "contributions": n_contrib,
            "expenditures": n_expn, "loans": n_loan}


# ============================== writers ===============================

def open_writer(filename: str, fieldnames: list[str]):
    """Open a gzipped CSV writer in CLEAN_DIR. Extra keys dropped, missing keys ''."""
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ========================= election history ===========================

def _manifest_partial_years() -> list[str]:
    """Truncated election-history years, as recorded by the scraper.

    Returns a flat sorted list of year strings. Each manifest row's
    `partial_years` cell holds the years IN THAT FILE that the service cut short
    — one year at most now that the export is stored per year, but a
    space-separated list in rows written by the pre-split scraper, so the cell
    is split rather than taken whole.

    Read defensively: the manifest predates the `partial_years` column, so a
    file written by an older scraper simply won't have it, and that is not an
    error — it means "unknown", which reads the same as "none" here.
    """
    manifest = RAW_DIR.parent / "manifest.csv"
    if not manifest.exists():
        return []
    years: set[str] = set()
    try:
        with open(manifest, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("relation") != ELECTION_HISTORY_RELATION:
                    continue
                years.update((r.get("partial_years") or "").split())
    except OSError:
        return []
    return sorted(years)


def _election_history_paths(log) -> list[Path]:
    """Election-history files to read, newest-relevant order irrelevant.

    Per-year files first; the pre-split combined file only when there are none
    (see LEGACY_ELECTION_HISTORY_FILE). Sorted by year so the "most recent
    contest wins" rule in load_election_history sees years in ascending order —
    that rule compares election_year per row and doesn't depend on file order,
    but reading in order keeps the warnings and any partial-file diagnosis
    legible.
    """
    paths = sorted(RAW_DIR.glob(ELECTION_HISTORY_GLOB),
                   key=lambda p: (_year_of_history_file(p), p.name))
    if paths:
        return paths
    legacy = RAW_DIR / LEGACY_ELECTION_HISTORY_FILE
    if legacy.exists():
        log.warning(f"  no {ELECTION_HISTORY_GLOB} files — falling back to "
                    f"{LEGACY_ELECTION_HISTORY_FILE} from the previous combined "
                    f"layout. Re-run the scraper to split it by year.")
        return [legacy]
    return []


def _year_of_history_file(path: Path) -> int:
    """Year in an election_history_<year>.csv name; 0 when there isn't one."""
    m = re.search(r"_(\d{4})\.csv$", path.name)
    return int(m.group(1)) if m else 0


def _tally_skip(why: dict, filer, amount, tx_date) -> None:
    """Record which required field sent a row to the skip pile."""
    if not filer:
        why["committee_name"] = why.get("committee_name", 0) + 1
    if amount == "":
        why["amount"] = why.get("amount", 0) + 1
    if not tx_date:
        why["date"] = why.get("date", 0) + 1


def _check_total_skip(log, name: str, relation: str, count: int, skipped: int,
                      why: dict, sample: dict | None) -> None:
    """Escalate a file that produced nothing but had rows to work with.

    A skip is normally a judgement about one row. Every row in a file failing
    the same required-field check is not that — it is the parser and the feed
    disagreeing about a field name, and it has to be loud. This exact shape went
    unnoticed across all nineteen expenditure files: `expDate` was missing from
    the date aliases, so every row was "missing a required field" and each file
    still reported success with 0 rows.
    """
    if count or not skipped:
        return
    lead = max(why, key=why.get) if why else "unknown"
    log.warning(
        f"  {name}: EVERY row ({skipped:,}) was skipped — no {relation} "
        f"emitted. Most common missing required field: {lead} "
        f"({why.get(lead, 0):,} rows). This is usually a field-name mismatch "
        f"rather than bad data.")
    if sample:
        log.warning(f"  {name}: keys actually present on the first row: "
                    f"{sorted(sample)}")


def _index_history_file(path: Path,
                        index: dict[str, dict]) -> tuple[int, int, int]:
    """Fold one election-history CSV into `index`.

    Returns (rows_read, tally_skipped, question_skipped).

    `index` is passed in and mutated rather than returned per file because the
    "most recent contest wins" rule has to be applied across the whole export,
    not within a year: merging separately-built per-year indexes afterwards
    would have to re-implement the same comparison, and getting the two copies
    to agree is exactly the kind of duplication that drifts.
    """
    rows_read = skipped_tally = skipped_question = 0
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        for raw in csv.DictReader(f):
            rows_read += 1
            idx  = index_row(raw)

            # Ballot-measure rows, dropped structurally rather than by name.
            #
            # 13.8% of this export describes ballot questions, not contests, and
            # those rows still fill candidate_name — with the measure's response
            # options. Measured over 300,000 rows, question rows contained
            # exactly five distinct values (YES, NO, TOTAL VOTES CAST,
            # OVERVOTES/UNDERVOTES, TOTAL BALLOTS CAST) in equal counts and no
            # real people at all, so nothing is lost by skipping the row type
            # outright. Three of those five were already in _TALLY_ROWS; YES and
            # NO were not, and were entering the name index as candidates.
            #
            # Testing question_text rather than adding YES/NO to the blocklist
            # is the difference between a rule about what a row IS and a list of
            # strings that has to be extended every time the service adds a
            # response option ("FOR"/"AGAINST", a bond measure's "IN FAVOR").
            if pick(idx, "questionText", "question_text", "questionType"):
                skipped_question += 1
                continue

            name = person_name(pick(idx, "candidate", "candidateName",
                                    "candidate_name", "name", "contestant"),
                               strip_title=True)
            if not name:
                continue
            # The export puts per-contest tally lines in candidate_name
            # alongside real people. Without this they enter the name index as
            # candidates and can be matched against by the fallback first+last
            # join.
            if _nk(name) in _TALLY_ROWS:
                skipped_tally += 1
                continue

            election_date = parse_date(pick(idx, "electionDate", "date"))
            year = (pick(idx, "year", "electionYear", "contestYear")
                    or year_of(election_date))

            # `candidate_party_name` and `division_name` are the real column
            # names in this export. Neither normalizes to anything the generic
            # aliases below match (_nk("candidate_party_name") is
            # "candidatepartyname"), so before they were added every candidate
            # came back with an empty party and jurisdiction while the
            # name-match rate still looked healthy.
            #
            # division_name is only a county when division_type says so — the
            # same column carries precinct names ("Windy Hill 02") on other
            # rows, which must not be written into jurisdiction.
            division = pick(idx, "divisionName", "division")
            county   = division if _nk(pick(idx, "divisionType")) == "county" else ""

            record = {
                "party":        utils.clean_name(pick(idx, "party", "partyName",
                                                      "politicalParty", "affiliation",
                                                      "candidatePartyName")),
                "district":     utils.clean_name(pick(idx, "district", "districtName",
                                                      "division", "divisionName",
                                                      "districtNumber")),
                "jurisdiction": utils.clean_name(pick(idx, "county", "countyName",
                                                      "jurisdiction", "municipality")
                                                 or county),
                "office":       utils.clean_name(pick(idx, "office", "officeName",
                                                      "contest", "contestName")),
                "election_year": year if year.isdigit() else "",
                "incumbent":    yes_no(pick(idx, "incumbent", "isIncumbent")),
            }

            prior = index.get(name)
            if prior is None or (record["election_year"] or "0") >= (prior["election_year"] or "0"):
                # Merge rather than replace: an older row may carry a party or
                # county that the newest row leaves blank.
                merged = dict(prior or {})
                merged.update({k: v for k, v in record.items() if v})
                for k in record:
                    merged.setdefault(k, "")
                index[name] = merged

    return rows_read, skipped_tally, skipped_question


def load_election_history(log) -> dict[str, dict]:
    """Index the SC Election Commission export by normalized candidate name.

    Reads every raw/election_history_<year>.csv (or the pre-split combined file
    if that is all that's there — see _election_history_paths) into one index.

    Returns {NORMALIZED NAME: {party, district, jurisdiction, election_year,
    incumbent}}. When a candidate appears in several contests the most recent
    one wins — party affiliation and district can both change between cycles,
    and the latest ballot appearance is the most useful single answer. That
    comparison is on the row's own election_year, so which file a contest
    arrived in makes no difference to the result.
    """
    paths = _election_history_paths(log)
    if not paths:
        log.warning(f"  no {ELECTION_HISTORY_GLOB} in raw/ — "
                    f"party/district/incumbent backfill skipped")
        return {}

    # The scraper records years the service truncated mid-stream. Those years
    # are present but incomplete, so a candidate whose only ballot appearance
    # falls in one can come back unmatched — or matched to an older contest —
    # with nothing in the join rate to suggest why.
    partial = _manifest_partial_years()
    if partial:
        log.warning(f"  election history year(s) {', '.join(partial)} were "
                    f"downloaded PARTIAL — party/district/jurisdiction backfill "
                    f"is incomplete for candidates whose contests fall in them")

    index: dict[str, dict] = {}
    rows_read = skipped_tally = skipped_question = total_bytes = 0
    for path in paths:
        try:
            read, tallies, questions = _index_history_file(path, index)
        except OSError as e:
            # One unreadable year is not a reason to drop the backfill for the
            # other eighteen — mirrors the scraper's per-year failure handling.
            log.warning(f"  {path.name}: {e} — skipping this year")
            continue
        rows_read       += read
        skipped_tally   += tallies
        skipped_question += questions
        total_bytes     += path.stat().st_size

    if skipped_question:
        log.info(f"  election history: skipped {skipped_question:,} ballot-question "
                 f"rows (Yes/No and their tallies, not candidates)")
    if skipped_tally:
        log.info(f"  election history: skipped {skipped_tally:,} tally rows "
                 f"(Total Ballots Cast and friends)")
    # Reported as one registry across all the files, because that is what it is
    # downstream: a single name index. `files` carries the split so a thin index
    # can be traced back to a year that failed to download.
    log.registry_loaded(f"{ELECTION_HISTORY_RELATION} ({len(paths)} file(s))",
                        len(index), relation="candidates",
                        bytes=total_bytes, rows_read=rows_read,
                        files=len(paths))
    return index


def build_fallback_index(index: dict[str, dict]) -> dict[tuple[str, str], list[str]]:
    """(first_token, last_token) → [full names], for middle-name-tolerant matching."""
    fl: dict[tuple[str, str], list[str]] = {}
    for name in index:
        tokens = [t.rstrip(".") for t in name.split() if t.rstrip(".")]
        if len(tokens) >= 2:
            fl.setdefault((tokens[0], tokens[-1]), []).append(name)
    return fl


def match_election_history(name: str, index: dict, fl_index: dict) -> dict:
    """Look up a candidate: exact name, then unambiguous first+last fallback."""
    hit = index.get(name)
    if hit:
        return hit
    tokens = [t.rstrip(".") for t in name.split() if t.rstrip(".")]
    if len(tokens) >= 2:
        matches = fl_index.get((tokens[0], tokens[-1]), [])
        if len(matches) == 1:      # ambiguous first+last pairs are left unmatched
            return index[matches[0]]
    return {}


# =============================== parse ================================

def run():
    log = get_logger("south carolina", "parse")
    t0  = time.perf_counter()
    log.info("Starting South Carolina parser")
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    candidates: dict[str, dict] = {}   # normalized name → candidate row
    committees: dict[str, dict] = {}   # normalized name → committee row
    file_handles: list = []

    # name → the election year the currently-stored state_filer_id came from.
    # Kept outside the row dicts because it is bookkeeping for the "most recent
    # id wins" rule below, not a column anyone downstream should see.
    filer_id_year: dict[str, str] = {}

    def register_filer(name: str, office: str, district: str,
                       election_year: str = "", raw_file: str = "", row_num="",
                       filer_id: str = ""):
        """Record a filer seen on any screen, keeping the richest version.

        Filers show up on all three screens with differing completeness — a
        transaction row has office but no election year, a reports row has both.
        Fields are filled in on first sight and never overwritten with blanks.

        `filer_id` is the exception to "first sight wins": see the state_filer_id
        block below for why it takes the most recent value instead.
        """
        if not name:
            return
        cand = candidates.setdefault(name, {
            "state":          STATE,
            "candidate_name": name,
            "office":         "",
            "district":       "",
            "election_year":  "",
            "state_filer_id": "",
            "raw_file":       raw_file,
            "row_num":        row_num,
        })
        if office and not cand["office"]:
            cand["office"] = office
        if district and not cand["district"]:
            cand["district"] = district
        # Keep the latest election year seen for this filer
        if election_year and election_year > (cand["election_year"] or ""):
            cand["election_year"] = election_year

        # state_filer_id — most recent wins, unlike every other field here.
        #
        # The portal issues a new id per candidacy, not per person: measured on
        # 2017, contributions carried 838 ids over 820 names and expenditures
        # 1,019 over 995, with no id ever spanning two names but ~2% of names
        # spanning several ids (KEVIN L BRYANT has three). So the id is safe to
        # record — it can never merge two people — but it is not a person key,
        # which is why person_id stays on name_hash. Splitting Bryant into three
        # people would be a worse error than the name collisions name_hash risks.
        #
        # "First sight wins" would pin a filer to their earliest candidacy, whose
        # id may no longer resolve on the portal; the most recent id is the one
        # that matches the site today, which is the entire point of carrying it.
        # Ties go to the incumbent value, so a re-run over the same files is
        # stable.
        if filer_id and election_year >= filer_id_year.get(name, ""):
            cand["state_filer_id"] = filer_id
            filer_id_year[name] = election_year

        cmte = committees.setdefault(name, {
            "state":          STATE,
            "committee_name": name,
            # Every filer on these screens is a candidate or public official —
            # standalone PACs file through a different system and never appear.
            "committee_type": "Candidate Committee",
            "candidate_name": name,
            "election_year":  "",
            "state_filer_id": "",
            "raw_file":       raw_file,
            "row_num":        row_num,
        })
        if election_year and election_year > (cmte["election_year"] or ""):
            cmte["election_year"] = election_year
        # Committees are written one-per-filer from the same names, so they
        # carry the same id — kept in step with the candidate row above rather
        # than tracked separately.
        if filer_id and cand["state_filer_id"]:
            cmte["state_filer_id"] = cand["state_filer_id"]

    try:
        # Registered one at a time rather than as a list after the last call —
        # if the fourth open_writer raises, the first three must still be in
        # file_handles for the `finally` block to close them.
        def _writer(filename: str, fieldnames: list[str]):
            fh, w = open_writer(filename, fieldnames)
            file_handles.append(fh)
            return w

        cont_w = _writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_w = _writer("expenditures.csv.gz",  C.EXPENDITURES)
        cand_w = _writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_w = _writer("committees.csv.gz",    C.COMMITTEES)
        # Empty unless the scraper's opt-in --pacs sweep has raw/noncand/
        # files to read below -- see parse_noncand_pacs().
        loan_w = _writer("loans_debts.csv.gz",   C.LOANS_DEBTS)

        # Contributions.
        for path in raw_files("contributions_*.json"):
            ft = time.perf_counter()
            count = skipped = 0
            skip_why: dict[str, int] = {}
            first_keys = None
            try:
                rows, file_year = load_envelope(path)
                for row_num, raw in enumerate(rows, start=1):
                    idx = index_row(raw)
                    if first_keys is None:
                        first_keys = set(idx)

                    amount = parse_amount(pick(idx, "amount", "contributionAmount",
                                               "amountContributed", "totalAmount"))
                    # `conDate`/`contribDate` are speculative — the expenditures
                    # feed abbreviates (expDate, expDesc, expId) and there is no
                    # reason to think this one doesn't, but no contributions
                    # sample has been checked. _check_total_skip below is what
                    # actually catches it if none of these match.
                    tx_date = parse_date(pick(idx, "conDate", "contribDate",
                                              "date", "contributionDate",
                                              "transactionDate", "dateContributed",
                                              "receivedDate"))
                    filer = person_name(pick(idx, "candidateName", "candidate",
                                             "filerName", "recipientName",
                                             "committeeName"), strip_title=True)

                    # committee_name, amount and date are tier-1 required — a row
                    # missing any of them can't be traced or summed, so drop it
                    # rather than emit a row that fails validation.
                    if not filer or amount == "" or not tx_date:
                        skipped += 1
                        _tally_skip(skip_why, filer, amount, tx_date)
                        continue

                    # `officeName` is the key this endpoint actually uses, and it
                    # is 100% filled — confirmed against contributions_2017.json
                    # (29,699 rows). It has to come first: the aliases that used
                    # to lead this list ("officeRun", "officeRunContributedTo")
                    # were guesses from the grid's column header, "Office Run
                    # Contributed To", and match nothing the API sends. The
                    # result was contributions.office at 0% while
                    # expenditures.office sat at 100% from the same split_office
                    # call — and because this feeds register_filer below, it also
                    # suppressed office and district on the candidates table.
                    #
                    # Deliberately NOT `officeRunId`, the other office-ish key
                    # here: it is an opaque numeric id ("20257"), not a name.
                    office, district = split_office(
                        pick(idx, "officeName", "officeRun",
                             "officeRunContributedTo", "office", "officeSought"))
                    city, st, zipc = split_address(
                        pick(idx, "contributorAddress", "address",
                             "contributorFullAddress"))
                    election_date = parse_date(pick(idx, "electionDate",
                                                    "electionDateContributedTo"))

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    filer,
                        "amount":            amount,
                        "date":              tx_date,
                        "transaction_type":  clean(pick(idx, "contributionType",
                                                        "transactionType", "type")),
                        "contributor_name":  person_name(pick(idx, "contributorName",
                                                              "contributor", "donorName")),
                        # "Group?" is the only contributor classification the
                        # portal exposes — an individual/organization flag, not a
                        # donor category. Mapped to canonical labels in
                        # src/aliases/contributor_types.csv.
                        "contributor_type":  {"Yes": "Group", "No": "Individual"}.get(
                                                 yes_no(pick(idx, "group", "isGroup",
                                                             "contributorIsGroup",
                                                             "groupIndicator")), ""),
                        "contributor_city":  city,
                        "contributor_state": st,
                        "contributor_zip":   zipc,
                        "employer":          clean(pick(idx, "contributorEmployer",
                                                        "employer")),
                        "occupation":        clean(pick(idx, "contributorOccupation",
                                                        "occupation")),
                        "candidate_name":    filer,
                        "office":            office,
                        "election_year":     year_of(election_date) or file_year,
                        "filing_id":         clean(pick(idx, "contributionId", "id",
                                                        "reportId", "transactionId")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    # `candidateId` here and `candidateFilerId` on the
                    # expenditures screen are the SAME id space — Henry McMaster
                    # is {15051, 11951} on both — so a filer's id is consistent
                    # across screens even though the key name is not.
                    register_filer(filer, office, district,
                                   year_of(election_date) or file_year,
                                   path.name, row_num,
                                   filer_id=clean(pick(idx, "candidateId",
                                                       "candidateFilerId")))
                    count += 1

                _check_total_skip(log, path.name, "contributions", count,
                                  skipped, skip_why, first_keys)
                log.file_parsed(path.name, "contributions", count, skipped,
                                duration_s=round(time.perf_counter() - ft, 2),
                                bytes=path.stat().st_size)
                total_contributions += count
            except Exception as e:
                log.file_parse_error(path.name, str(e))

        # Expenditures.
        for path in raw_files("expenditures_*.json"):
            ft = time.perf_counter()
            count = skipped = 0
            skip_why: dict[str, int] = {}
            first_keys = None
            try:
                rows, file_year = load_envelope(path)
                for row_num, raw in enumerate(rows, start=1):
                    idx = index_row(raw)
                    if first_keys is None:
                        first_keys = set(idx)

                    amount = parse_amount(pick(idx, "amount", "expenditureAmount",
                                               "amountPaid", "totalAmount"))
                    # `expDate` is what the API actually sends. Its absence from
                    # this list emptied every expenditure row for every year —
                    # 510k rows across 2008-2026 — while the parser still
                    # reported success, because a row missing a required field
                    # is a legitimate skip and 100% skipped looked like 100%
                    # unusable data rather than one wrong alias.
                    tx_date = parse_date(pick(idx, "expDate", "date",
                                              "expenditureDate", "transactionDate",
                                              "datePaid"))
                    filer = person_name(pick(idx, "candidateName", "candidate",
                                             "filerName", "committeeName"),
                                        strip_title=True)

                    if not filer or amount == "" or not tx_date:
                        skipped += 1
                        _tally_skip(skip_why, filer, amount, tx_date)
                        continue

                    # This screen sends it as `office` and always has. The other
                    # names are tolerance, not observation — `officeName` is
                    # included because the contributions screen uses exactly that
                    # for the same value, and the two screens have already been
                    # seen to disagree about key names for identical fields.
                    office, district = split_office(
                        pick(idx, "office", "officeName", "officeRun",
                             "officeSought"))
                    city, st, zipc = split_address(
                        pick(idx, "vendorAddress", "payeeAddress", "address"))

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   filer,
                        "amount":           amount,
                        "date":             tx_date,
                        "transaction_type": clean(pick(idx, "expenditureType",
                                                       "transactionType", "type")),
                        # Vendor/payee names are usually organizations, not people, and
                        # even ones that look like a person shouldn't be treated as
                        # "Last, First" -- this field is company names far more often
                        # than filer/candidate names are. person_name()'s comma-flip
                        # heuristic (designed for filer names) mangles ordinary
                        # business names that legitimately contain a comma
                        # ("Red Sea, LLC" -> "LLC RED SEA") or a source typo in place
                        # of a period ("GoDaddy,com" -> "COM GODADDY"). clean() matches
                        # the other three payee_name call sites in this file.
                        "payee_name":       clean(pick(idx, "vendorName",
                                                       "payeeName", "vendor",
                                                       "payee")),
                        # expDesc/expId, like expDate, are the abbreviated names
                        # this API really uses.
                        "purpose":          clean(pick(idx, "expDesc",
                                                       "expenditureDescription",
                                                       "description", "purpose")),
                        "category":         clean(pick(idx, "expenditureCategory",
                                                       "category")),
                        "payee_city":       city,
                        "payee_state":      st,
                        "payee_zip":        zipc,
                        "candidate_name":   filer,
                        "office":           office,
                        "election_year":    file_year,
                        "filing_id":        clean(pick(idx, "expId", "expenditureId",
                                                       "id", "reportId",
                                                       "transactionId")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    # Deliberately NOT campaignId or credentialId, the other two
                    # ids this screen carries: campaignId is per campaign (51
                    # names spanned several in 2017, against 23 for
                    # candidateFilerId) and credentialId is a parallel numbering
                    # that shares candidateFilerId's cardinality but none of its
                    # values, so it would not join to the contributions screen.
                    register_filer(filer, office, district, file_year,
                                   path.name, row_num,
                                   filer_id=clean(pick(idx, "candidateFilerId",
                                                       "candidateId")))
                    count += 1

                _check_total_skip(log, path.name, "expenditures", count,
                                  skipped, skip_why, first_keys)
                log.file_parsed(path.name, "expenditures", count, skipped,
                                duration_s=round(time.perf_counter() - ft, 2),
                                bytes=path.stat().st_size)
                total_expenditures += count
            except Exception as e:
                log.file_parse_error(path.name, str(e))

        # Filed reports.
        # Filed disclosure reports. No transactions here — this pass exists to
        # pick up filers who reported no itemized activity, and to attach the
        # election year / office that transaction rows don't carry.
        reports_seen = 0
        for path in raw_files("reports_*.json"):
            ft = time.perf_counter()
            count = 0
            try:
                rows, file_year = load_envelope(path)
                for row_num, raw in enumerate(rows, start=1):
                    idx  = index_row(raw)
                    name = person_name(pick(idx, "candidateName", "candidate",
                                            "filerName", "personName"),
                                       strip_title=True)
                    if not name:
                        continue

                    office, district = split_office(
                        pick(idx, "office", "officeName", "officeRun"))
                    election_year = clean(pick(idx, "electionYear", "year")) or file_year

                    # This screen used to be the only source of state_filer_id,
                    # via a `personId` the portal no longer sends — a 2019
                    # reports file carries none of personId/candidateId/filerId/
                    # seiId. That dead lookup is why state_filer_id validated at
                    # 0% while the transaction screens were carrying a usable id
                    # the whole time. The names are kept as tolerance in case the
                    # field returns, but the id now comes from the transaction
                    # screens (see register_filer).
                    #
                    # Routed through register_filer rather than assigned directly
                    # so it obeys the same most-recent-wins rule; the old direct
                    # assignment overwrote whatever the transaction screens had
                    # found, regardless of year.
                    register_filer(name, office, district, election_year,
                                   path.name, row_num,
                                   filer_id=clean(pick(idx, "candidateId",
                                                       "candidateFilerId",
                                                       "personId", "personID",
                                                       "filerId")))
                    count += 1

                reports_seen += count
                log.file_parsed(path.name, "candidates", count, 0,
                                duration_s=round(time.perf_counter() - ft, 2),
                                bytes=path.stat().st_size)
            except Exception as e:
                log.file_parse_error(path.name, str(e))

        # Non-Candidate committees (PACs) -- a separate source entirely (see
        # module docstring and docs/states/south_carolina.md). Opt-in at scrape
        # time (--pacs), so raw/noncand/ may simply not exist; the function
        # glob-checks and returns zero counts rather than erroring.
        noncand_counts = parse_noncand_pacs(log, cmte_w, cont_w, expn_w, loan_w,
                                            existing_committee_names=set(committees))
        if noncand_counts["committees"]:
            log.info(f"  [noncand] {noncand_counts['committees']:,} PAC committees, "
                     f"{noncand_counts['contributions']:,} contributions, "
                     f"{noncand_counts['expenditures']:,} expenditures, "
                     f"{noncand_counts['loans']:,} loan/repayment rows")
            total_contributions += noncand_counts["contributions"]
            total_expenditures  += noncand_counts["expenditures"]

        # Caucus + State/County/City Political Party committees -- a fourth
        # source (same apps.sc.gov site as Non-Candidate, opt-in at scrape
        # time via --party-caucus). See parse_party_caucus() and
        # docs/states/south_carolina.md "Caucus & Party Committees".
        party_counts = parse_party_caucus(log, cmte_w, cont_w, expn_w, loan_w,
                                          existing_committee_names=set(committees))
        if party_counts["committees"]:
            log.info(f"  [party_caucus] {party_counts['committees']:,} Caucus/Party "
                     f"committees, {party_counts['contributions']:,} contributions, "
                     f"{party_counts['expenditures']:,} expenditures, "
                     f"{party_counts['loans']:,} loan/repayment rows")
            total_contributions += party_counts["contributions"]
            total_expenditures  += party_counts["expenditures"]

        # Ballot Measure committees -- the sixth and last apps.sc.gov source
        # (opt-in at scrape time via --ballot-measure). See
        # parse_ballot_measure() and docs/states/south_carolina.md "Ballot
        # Measure Committees".
        ballot_counts = parse_ballot_measure(log, cmte_w, cont_w, expn_w, loan_w,
                                             existing_committee_names=set(committees))
        if ballot_counts["committees"]:
            log.info(f"  [ballot] {ballot_counts['committees']:,} Ballot Measure "
                     f"committees, {ballot_counts['contributions']:,} contributions, "
                     f"{ballot_counts['expenditures']:,} expenditures, "
                     f"{ballot_counts['loans']:,} loan/repayment rows")
            total_contributions += ballot_counts["contributions"]
            total_expenditures  += ballot_counts["expenditures"]

        # Tier-2 backfill, then flush candidates and committees.
        history  = load_election_history(log)
        fl_index = build_fallback_index(history)

        enriched = 0
        for name, cand in candidates.items():
            hit = match_election_history(name, history, fl_index)
            if hit:
                enriched += 1

            first, last = name_parts(name)
            cand_row = dict(cand)
            cand_row.update({
                "candidate_first": first,
                "candidate_last":  last,
                # Election-history values only fill gaps — the ethics portal is
                # authoritative for anything it actually publishes.
                "office":          cand["office"]        or hit.get("office", ""),
                "district":        cand["district"]      or hit.get("district", ""),
                "election_year":   cand["election_year"] or hit.get("election_year", ""),
                "jurisdiction":    hit.get("jurisdiction", ""),
                "party":           hit.get("party", ""),
                "incumbent":       hit.get("incumbent", ""),
            })
            cand_w.writerow(cand_row)

        for cmte in committees.values():
            cmte_w.writerow(cmte)

        log.enrichment_summary(
            candidates_total=len(candidates),
            candidates_enriched=enriched,
            committees_total=len(committees),
            election_history_entries=len(history),
            reports_rows=reports_seen,
        )

        for fh in file_handles:
            fh.close()
        file_handles = []      # prevent a double close in `finally`

        # name_hash: the portal exposes no filer ID on transaction rows, so the
        # normalized name is the only key shared by all three screens.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="name_hash")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name: str) -> int:
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz", "expenditures", total_expenditures,
                        role="output", bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("candidates.csv.gz", "candidates", len(candidates),
                        role="output", bytes=_out_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz", "committees", len(committees),
                        role="output", bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz", "loans_debts", 0,
                        role="output", bytes=_out_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates))

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates))
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates),
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
    import argparse
    argparse.ArgumentParser(
        description="Parse South Carolina raw data into 5 normalized relations."
    ).parse_known_args()
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
