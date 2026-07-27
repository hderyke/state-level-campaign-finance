"""
parsers/tennessee.py — Parse Tennessee TNCAMP result-page exports into the
canonical cleaned schema.

Raw files (data/Tennessee/raw/, written by scrapers/tennessee.py — one file
per results page, ~100 rows each):

  contributions_{year}_p{NNN}.csv  -> contributions
  expenditures_{year}_p{NNN}.csv   -> expenditures
  candidates_{year}_e{id}_p{NNN}.csv -> candidates (+ their committees)
  pacs_p{NNN}.csv                  -> committees

The candidate roster is election-scoped and the PAC roster is not, because
TNCAMP's candidate search requires an election and its PAC search has no
election criterion at all — see scrapers/tennessee.py. `{id}` is TNCAMP's own
opaque `electionYearSelection` option value, present because a year does not
identify an election there: special elections are registered separately, so
2023 covers five of them. The parser doesn't care about the ID; it only
splits what would otherwise be colliding filenames.

So candidate pages arrive as one set per election, and the same person recurs
across every election they ran in. `register_candidate()` already
de-duplicates on the normalized name and keeps the latest `election_year`
seen, so a candidate spanning four cycles collapses to one row regardless of
how many election files they appear in. `CANDIDATE_GLOBS` also still matches
the older `candidates_{year}_p*.csv` and `candidates_p*.csv` namings, so a raw
directory left over from an earlier scrape run still parses.

Header tolerance
----------------
TNCAMP's export columns are whatever the search form's "display these fields
in my results" checkboxes were set to, and the header text is the form's own
label ("Contributor Name", "Election Year", ...) rather than a stable machine
key. So rather than index by exact header, every header is snake-cased and
resolved through `COLUMN_ALIASES`, which lists the spellings each canonical
field is known to arrive under. An unrecognised header is ignored, and a
canonical field that never resolves simply comes out blank — a label change on
TN's side degrades one column instead of crashing the parse. `run()` logs the
resolved header map for the first file of each relation so a silent
degradation is visible in the run log.

The combined address column
---------------------------
TNCAMP exports the contributor/vendor address as one string with the city,
state and ZIP appended — "1385 5Th Ave #11E, Nashville, TN, 37203". There is
no separate city/state/ZIP column to fall back on, so `split_address()` peels
the tail off right-to-left: ZIP first (it's the only unambiguous token), then
a 2-letter state, then the city, leaving the street. This is a direct
simplification of the four-stage salvage logic in the R script this pipeline
inherited TN from (Investigative Reporting Workshop's `get_tn_contribs.R`),
which existed for the same reason. Rows whose address doesn't fit any of the
patterns keep the whole string as the street and leave city/state/ZIP blank
rather than guessing.

No filer IDs
------------
TNCAMP exposes no filer/committee ID anywhere in its public exports — not in
the transaction results, not in the candidate/PAC roster. `state_filer_id` is
therefore structurally unfillable, which is why `TN` carries
`has_filer_id = 0` in src/aliases/states.csv (that flips validate.py's
`state_filer_id` fill-rate check from a tier-1 failure to a tier-2 warning),
and why `person_id` uses `id_model="name_hash"` — a stable hash of
state + normalized candidate name. Same situation as Alaska, Kansas and
Kentucky.

No loans
--------
TNCAMP's public search covers contributions and expenditures only. Loans
Received / Loan Payments exist as schedules inside individual filed reports
(see the app's own example pages) but aren't exposed by the C&E search, so
`loans_debts.csv.gz` is written with a header and no rows.

The roster export has no combined name column
------------------------------------------------
Despite the search form's single `nameField` checkbox, TNCAMP's actual
candidate / PAC roster export splits a person's name into separate
First Name / Last Name columns, and represents an organization (a PAC or a
corporate donor registered as its own filer) by putting the full org name in
Last Name with First Name left blank — there is no "Name" header at all.
`roster_entity_name()` rebuilds the combined 'Last, First' form from the two
columns (falling back to a single combined column if one is ever present, in
case the export shape changes back) so it feeds `display_name()`/
`split_name()` unchanged from every other name in this parser. First Name
being present is also the signal `is_person_row()` uses to decide whether a
roster row is a candidate or an organization at all — see the next section.

Candidate and PAC rows are mixed in both roster files
-------------------------------------------------------
The candidate files and `pacs_p*.csv` come from two separate searches
(`searchType` "candidate" vs. "pac"), but empirically neither is exclusively
one or the other — organization rows (committee_type PAC, corporate filers)
turn up in the candidate files and person rows turn up in
`pacs_p*.csv`. The file a row
came from is therefore not a reliable type signal. Both roster loops run
every row through `is_person_row()` and register it as a candidate or a
committee based on the row's own content (First Name present vs. blank),
never based on which file it was read from.

Office Sought, Committee Affiliation and Contact Info previously never resolved
--------------------------------------------------------------------------------
As of the 2026-07-25 --discover run, `cp_search_body()` in scrapers/tennessee.py
had been requesting these three columns under the wrong checkbox names —
`officeSoughtField`, `committeeAffiliationField` and `contactInfoField` instead
of the live form's actual `officeField`, `committeeField` and `contactField` —
so the checkboxes were silently never checked and TNCAMP had no reason to
include the columns. That's now fixed in scrapers/tennessee.py; the aliases
below (`office_sought`/`office`, `committee_affiliation`/`committee`,
`contact_info`/`contact`/`address`) already cover either header spelling, so
no parser change was needed. Whether these actually populate now depends on
the next scrape run — if `office`/`committee_affiliation`/`contact_info` are
still blank after a re-scrape, that confirms a genuine TNCAMP export gap
rather than a field-name miss, and validate.py's fill-rate warnings are still
the right place to see that surfaced.

Primary/General are dates with an embedded outcome, not Y/N flags
---------------------------------------------------------------------
The roster's Primary/General columns are not win/loss flags — they carry the
election date with the outcome parenthesized onto the end, e.g.
`"11/08/2016 (L)"` / `"08/07/2014 (W)"`, and are blank entirely for a cycle
that hasn't happened yet. `parse_election_result()` pulls the `(W)`/`(L)`
suffix out; a Y/Yes/Won-style flag match against the whole string never
matches this shape and would silently produce `incumbent = "0"` for every
candidate with a General value instead of the real result — an earlier
version of this parser did exactly that.

No filer identifier on expenditure rows
------------------------------------------
Every expenditure export sampled has "Recipient Name" blank in 100% of rows
— TNCAMP's expenditure schedule has no per-row field naming the committee
that made the expenditure (only "Vendor Name", the payee). This is a genuine
gap in the source, the same category as the missing filer IDs and loan
schedules above: there is nothing here to resolve `committee_name` from, so
it is written blank for every TN expenditure row rather than guessed at.
`validate.py`'s `TIER1_OPTIONAL_FOR_NAME_HASH` downgrades this from a hard
failure to a tier-2 warning for states (TN included) that already lack a
filer ID.

Output (data/Tennessee/cleaned/):
  contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
  committees.csv.gz, candidates.csv.gz
"""

import csv
import gzip
import re
import sys
import time
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Tennessee" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Tennessee" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "TN"
EARLIEST_YEAR  = 1990                   # matches validate.py's plausibility floor
MAX_VALID_YEAR = date.today().year + 4  # matches validate.py's ceiling

# ============================ Column aliases ===========================
# canonical field -> snake_cased header spellings it may arrive under.
# Order matters: the first spelling present in a file wins.
#
# The primary spellings are the snake_case of the form labels the scraper's
# `*Field` checkboxes switch on (see scrapers/tennessee.py). The alternates
# cover the abbreviations the export has been observed to use ("adj" for
# "Adjustment") and the plausible near-misses, so a cosmetic relabel on TN's
# side doesn't blank a column.
COLUMN_ALIASES: dict[str, list[str]] = {
    # shared
    "type":            ["type", "transaction_type", "contribution_type", "expenditure_type"],
    "adjustment":      ["adjustment", "adj", "adjustment_type"],
    "amount":          ["amount", "amt", "contribution_amount", "expenditure_amount"],
    "date":            ["date", "transaction_date", "contribution_date", "expenditure_date"],
    "election_year":   ["election_year", "electionyear", "year"],
    "report_name":     ["report_name", "report", "reportname"],
    "recipient_name":  ["recipient_name", "recipient", "candidate_pac_name",
                        "candidate_or_pac", "candidate_pac"],
    "description":     ["description", "desc", "explanation"],
    # contributions
    "contributor_name":       ["contributor_name", "contributor", "from_name", "name"],
    "contributor_address":    ["contributor_address", "address", "contributor_addr"],
    "contributor_occupation": ["contributor_occupation", "occupation"],
    "contributor_employer":   ["contributor_employer", "employer"],
    # expenditures
    "vendor_name":     ["vendor_name", "vendor", "payee_name", "payee"],
    "vendor_address":  ["vendor_address", "vendor_addr", "payee_address", "address"],
    "purpose":         ["purpose", "purpose_of_expenditure"],
    # entity roster (candidates_*.csv / pacs_*.csv)
    # "entity_name" is only ever populated if TNCAMP's export reverts to a
    # single combined column; the real export splits first/last (see
    # roster_entity_name()), so first_name/last_name are resolved separately.
    "entity_name":     ["name", "candidate_name", "pac_name", "candidate_pac_name"],
    "first_name":      ["first_name"],
    "last_name":       ["last_name"],
    "office_sought":   ["office_sought", "office"],
    "district":        ["district"],
    # "party_affliation" is TN's own misspelling of the header ("Party
    # Affliation", missing the second i) — it's the only spelling the real
    # export has ever been observed to use, but "party_affiliation" is kept
    # too in case that gets corrected on TN's side.
    "party":           ["party", "party_affiliation", "party_affliation"],
    "treasurer_name":  ["treasurer_name", "treasurer"],
    "treasurer_contact": ["treasurer_contact_info", "treasurer_contact",
                          "treasurer_address"],
    "contact_info":    ["contact_info", "contact", "address"],
    "primary":         ["primary", "primary_winner"],
    "general":         ["general", "general_winner"],
    "committee_affiliation": ["committee_affiliation", "committee"],
    "created":         ["created", "created_date", "date_created"],
    "closed":          ["closed", "closed_date", "date_closed"],
}

# Two-letter codes accepted as the state component of a split address. Kept
# local rather than imported from validate.py so the parser has no dependency
# on the validator.
VALID_STATE_ABBS = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "AS", "GU", "MP", "PR", "VI",
}

_ZIP_RE   = re.compile(r"^\d{5}(?:-\d{4})?$")

# Tokens that mark a name as an organization rather than a person, used by
# display_name() to decide whether a comma is a "Last, First" inversion or
# just punctuation inside a business name.
_ORG_MARKER_RE = re.compile(
    r"(?:^|\W)(?:llc|l\.l\.c|llp|lp|pllc|plc|ltd|pc|p\.c|inc|corp|corporation|"
    r"co|company|pac|committee|fund|association|assoc|union|partners|"
    r"partnership|group|trust|foundation|society|institute|council|&)(?:\W|$)",
    re.IGNORECASE,
)


# ============================== Helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def snake(header: str) -> str:
    """Fold an export header to a comparable snake_case key.

    'Contributor Name' -> 'contributor_name'; 'Election Year ' -> 'election_year'."""
    h = re.sub(r"[^\w\s]", " ", (header or ""))
    h = re.sub(r"\s+", "_", h.strip())
    return h.lower().strip("_")


def build_header_map(fieldnames: list[str] | None) -> dict[str, int]:
    """canonical field -> the column index in this file that supplies it.

    Resolved once per file rather than per row: TNCAMP page exports for the
    same relation share a header, but the header may differ between relations
    (contributions vs. expenditures) and could change between scrape runs.

    Maps to a column *index* rather than the header string so each data row
    can be read with plain csv.reader (a list) instead of csv.DictReader (a
    dict rebuilt from scratch every row) — a meaningful cost at the ~5,000
    raw-file, hundreds-of-thousands-of-rows scale this parser runs at."""
    if not fieldnames:
        return {}
    present = {snake(h): i for i, h in enumerate(fieldnames) if h}
    resolved = {}
    for canonical, spellings in COLUMN_ALIASES.items():
        for spelling in spellings:
            if spelling in present:
                resolved[canonical] = present[spelling]
                break
    return resolved


def get(row: list, hmap: dict, field: str) -> str:
    """Read a canonical field out of a raw row via the resolved header map."""
    idx = hmap.get(field)
    if idx is None or idx >= len(row):
        return ""
    return clean(row[idx])


@lru_cache(maxsize=None)
def parse_amount(val: str) -> str:
    """Parse a dollar amount to a plain numeric string. Returns '' on failure.

    Memoized: a pure function of its input string, and amounts repeat heavily
    across a run (the same handful of dollar figures recur constantly), so
    caching avoids re-parsing the same string across hundreds of thousands of
    rows spread over ~5,000 raw files.

    TNCAMP exports amounts with a dollar sign and thousands separators
    ("$1,250.00"); adjustment/refund rows use a leading '-' rather than
    parentheses, but parentheses are handled too."""
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


@lru_cache(maxsize=None)
def parse_date(val: str) -> str:
    """Normalize a date to YYYY-MM-DD. Returns '' on failure or implausible year.
    Memoized — see parse_amount().

    TNCAMP renders dates as M/D/YYYY in the HTML table and the CSV export
    inherits that; the other formats are accepted defensively."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%d-%b-%Y", "%b %d, %Y"):
        try:
            d = datetime.strptime(v, fmt)
        except ValueError:
            continue
        if d.year < EARLIEST_YEAR or d.year > MAX_VALID_YEAR:
            return ""
        return d.strftime("%Y-%m-%d")
    return ""


@lru_cache(maxsize=None)
def parse_year(val: str) -> str:
    """First 4-digit year in a string, or ''.
    Memoized — see parse_amount().

    TN's election-year values are sometimes decorated with the special
    election they belong to ('2023 (HOUSE 51)' appears in the search form's own
    year list), so pull the year out rather than requiring a bare one."""
    m = re.search(r"(19|20)\d{2}", val or "")
    return m.group(0) if m else ""


_ELECTION_RESULT_RE = re.compile(r"\(([WL])\)\s*$", re.IGNORECASE)


@lru_cache(maxsize=None)
def parse_election_result(val: str) -> str:
    """Pull the win/loss outcome off a roster Primary/General value.

    TN's roster export doesn't carry a plain Y/N winner flag — Primary and
    General are the election date with the outcome parenthesized onto the
    end, e.g. '11/08/2016 (L)' or '08/07/2014 (W)'. A cycle with no decided
    outcome yet (an upcoming election) has the date with no parenthetical at
    all. Returns '1' for a win, '0' for a loss, '' for anything else
    (blank, or a date with no outcome recorded)."""
    v = clean(val)
    if not v:
        return ""
    m = _ELECTION_RESULT_RE.search(v)
    if not m:
        return ""
    return "1" if m.group(1).upper() == "W" else "0"


def roster_entity_name(row: list, hmap: dict) -> str:
    """Build a roster row's raw entity name, tolerating the split-name export.

    Prefers a combined column if present (see COLUMN_ALIASES note), otherwise
    rebuilds the 'Last, First' form TN uses everywhere else in this parser
    from the First Name / Last Name columns the real export actually has.
    An organization row (First Name blank) has its full name in Last Name
    already, with no comma to invert."""
    name = get(row, hmap, "entity_name")
    if name:
        return name
    first = get(row, hmap, "first_name")
    last  = get(row, hmap, "last_name")
    if first and last:
        return f"{last}, {first}"
    return last or first


def is_person_row(row: list, hmap: dict) -> bool:
    """True if a roster row names a person (a candidate) rather than an
    organization (a PAC or corporate filer).

    candidates_p*.csv and pacs_p*.csv both mix person and organization rows
    in practice — the file a row came from is not a reliable type signal (see
    module docstring). First Name is only ever populated for a person, so its
    presence is the classification TN's own export gives us; an organization
    row has its full name in Last Name with First Name, District and Party
    all blank."""
    if get(row, hmap, "first_name"):
        return True
    if get(row, hmap, "entity_name"):
        # Combined-name export shape (not currently observed) — first/last
        # aren't split out to check, so fall back to another person-only
        # field instead of guessing from the name string.
        return bool(get(row, hmap, "district")) or bool(get(row, hmap, "office_sought"))
    return False


@lru_cache(maxsize=None)
def split_address(raw: str) -> tuple[str, str, str, str]:
    """Split TNCAMP's single combined address into (street, city, state, zip).
    Memoized — the same address recurs across a filer's many transactions;
    see parse_amount().

    TNCAMP has no separate city/state/ZIP columns — everything arrives as one
    comma-joined string, e.g.:

        "1385 5Th Ave #11E, Nashville, TN, 37203"   -> all four parts
        "PO Box 1234, Memphis, TN"                   -> no ZIP
        "Nashville, TN 37203"                        -> no street, ZIP unsplit
        "Unknown"                                    -> street only

    Peeling right-to-left is what makes the ragged cases work: the ZIP is the
    only token that's unambiguously identifiable, the state is the only
    2-letter token from a closed set, and whatever is left of those is
    street + city. Anything that doesn't fit keeps the full string as the
    street rather than being force-fit into the wrong column — a simplification
    of the four-stage salvage logic in IRW's original R script, kept because
    the failure mode (blank city/state/zip) is safe and visible in the
    validator's fill rates."""
    s = clean(raw)
    if not s:
        return "", "", "", ""

    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        return "", "", "", ""

    zipc = state = ""

    # Trailing "TN 37203" (state and ZIP in one comma-part) is common enough
    # to be worth handling before the part-by-part peel.
    m = re.match(r"^([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$", parts[-1])
    if m and m.group(1).upper() in VALID_STATE_ABBS:
        state, zipc = m.group(1).upper(), m.group(2)
        parts.pop()
    else:
        if parts and _ZIP_RE.match(parts[-1]):
            zipc = parts.pop()
        if parts and parts[-1].upper() in VALID_STATE_ABBS:
            state = parts.pop().upper()

    # Whether the last remaining part is a city or a street depends on whether
    # anything was peeled off after it. "Nashville, TN 37203" has no street at
    # all, and treating its one remaining part as a street would throw the city
    # away — but "Unknown", where nothing was peeled, really is unparseable and
    # keeping it as the street is the documented safe fallback.
    if state or zipc:
        city   = parts.pop() if parts else ""
        street = ", ".join(parts)
    else:
        city   = ""
        street = ", ".join(parts)
    return street, city, state, zipc


@lru_cache(maxsize=None)
def split_name(raw: str) -> tuple[str, str]:
    """Candidate name -> (first_middle, last).
    Memoized — see parse_amount().

    TN roster names arrive last-name-first ("Smith, John Q.") in the candidate
    search, which is the form's own sort order; transaction recipient names
    arrive as displayed. Handle the comma form explicitly and fall back to
    'last token is the surname' otherwise."""
    name = utils.clean_name(raw)
    if not name:
        return "", ""
    if "," in name:
        last, _, rest = name.partition(",")
        return utils.clean_name(rest), utils.clean_name(last)
    parts = name.split()
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


@lru_cache(maxsize=None)
def display_name(raw: str) -> str:
    """'SMITH, JOHN Q.' -> 'JOHN Q. SMITH'; already-forward names pass through.
    Memoized — the same contributor/recipient name recurs constantly across
    a filer's transactions; see parse_amount().

    Committee↔candidate matching in utils.assign_committee_person_ids() is
    name-based and does NOT handle comma inversion, so every name written out
    has to be in the same forward order — otherwise a candidate registered as
    "HASLAM, WILLIAM E." never matches the committee that names them.

    Organization names are left alone. A comma inside an org name is a
    separator, not an inversion ("Smith, Jones & Co" is not a person called
    Jones & Co Smith), so inversion is skipped when either side of the comma
    carries a corporate marker or the trailing part is too long to be a given
    name."""
    # Trailing punctuation is stripped first: a dangling comma ("Smith,") is a
    # data-entry artifact, not an inversion, and would otherwise become part of
    # the registry key.
    s = clean(raw).rstrip(" ,")
    if "," not in s:
        return utils.clean_name(s)
    if _ORG_MARKER_RE.search(s):
        return utils.clean_name(s)
    first, last = split_name(s)
    # A given-name segment is 1–3 tokens ("John", "William E.", "Mary Jo Ann");
    # more than that and this is a list or an org, not "Last, First".
    if not first or len(first.split()) > 3:
        return utils.clean_name(s)
    return utils.clean_name(f"{first} {last}".strip())


def resolve_recipient(raw: str, candidates: dict) -> tuple[str, str, bool]:
    """Work out whether a transaction's recipient is a candidate or a committee.

    TN's C&E export has one "Recipient Name" column for both, with no type
    flag, so the rosters loaded earlier are the only way to tell them apart.
    Returns (candidate_name, committee_name, is_candidate):

      - a recipient known to the candidate roster gets its forward-order name
        in BOTH fields, so contributions.committee_name joins cleanly to the
        committees table and to the candidate's own record
      - anything else is treated as a committee and keeps its name as-is

    Both branches return names in the same normalization the registries use;
    returning the raw comma-ordered string here is what would otherwise leave
    "HASLAM, WILLIAM E." in committee_name and "WILLIAM E. HASLAM" in
    candidate_name on the same row, joining to neither table."""
    fwd = display_name(raw)
    if fwd in candidates:
        return fwd, fwd, True
    return "", utils.clean_name(raw), False


def raw_files(*patterns: str) -> list[Path]:
    """Non-empty raw files matching any of these globs, sorted by name (so page
    order and year order are both preserved: contributions_2010_p001, _p002,
    ...).

    Accepts several patterns because the candidate roster has two possible
    filename shapes on disk — see CANDIDATE_GLOBS. De-duplicated by path, so
    overlapping patterns can't read the same file twice."""
    seen: dict[Path, None] = {}
    for pattern in patterns:
        for f in RAW_DIR.glob(pattern):
            if f.stat().st_size > 0:
                seen[f] = None
    return sorted(seen, key=lambda p: p.name)


# The candidate roster became year-scoped when scrapers/tennessee.py started
# passing TNCAMP's required election-year criterion (see module docstring), so
# its pages are now `candidates_{year}_p{NNN}.csv`. The bare `candidates_p*`
# spelling is kept alongside it so a raw directory written by an older scrape
# run — or a partially re-scraped one holding both shapes — still parses
# instead of silently yielding zero candidates.
CANDIDATE_GLOBS = ("candidates_*_p*.csv", "candidates_p*.csv")


_FILENAME_YEAR_RE = re.compile(r"_((?:19|20)\d{2})(?:_e[^_]+)?_p\d+\.csv$")


def year_from_filename(path: Path) -> str:
    """Election year embedded in a raw page filename, or ''.

    Handles every shape the scraper writes:

        contributions_2024_p001.csv    -> "2024"
        candidates_2023_e238_p001.csv  -> "2023"
        candidates_p001.csv            -> ""     (legacy, pre-election-scoping)
        pacs_p001.csv                  -> ""     (no election year exists)

    The optional `_e{id}` segment is the candidate roster's per-election
    discriminator: TN registers special elections separately, so 2023 alone
    covers five distinct elections and the year does not identify one (see
    scrapers/tennessee.py). The ID is TNCAMP's own opaque option value and
    means nothing to the parser — only the year it is attached to matters
    here, since rows are de-duplicated by candidate name regardless of which
    election file they arrived in."""
    m = _FILENAME_YEAR_RE.search(path.name)
    return m.group(1) if m else ""


def open_writer(filename: str, fieldnames: list):
    """Open a gzipped CSV writer in CLEAN_DIR; extra fields dropped, missing default ''."""
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def _fill(d: dict, key: str, val: str) -> None:
    """Set d[key] = val only if val is truthy and d[key] is currently empty."""
    if val and not d.get(key):
        d[key] = val


# ===================== transaction-type helpers ======================
def _txn_type(type_val: str, adjustment: str) -> str:
    """Combine TN's Type and Adjustment columns into one transaction_type.

    Kept deliberately low-cardinality (TN's Type has ~2 distinct values and
    Adjustment is a short code) so src/aliases/transaction_categories.csv can
    enumerate the result. A blank Type falls back to the schedule-neutral
    labels the aggregate layer expects to see something in."""
    t   = clean(type_val)
    adj = clean(adjustment)
    if t and adj and adj.lower() not in ("n", "no", "none", "0"):
        return f"{t} — {adj}"
    return t


def _amended(adjustment: str) -> str:
    """TN's Adjustment column doubles as the amendment flag: any non-negative
    value means the row corrects an earlier filing."""
    adj = clean(adjustment)
    if not adj:
        return ""
    return "0" if adj.lower() in ("n", "no", "none", "0") else "1"


# ================================ Main ================================
def run():
    log = get_logger("tennessee", "parse")
    t0  = time.perf_counter()
    log.info("Starting Tennessee parser")
    log._emit("parse_started")

    # Both registries are keyed by normalized name — TNCAMP publishes no IDs.
    candidates: dict[str, dict] = {}
    committees: dict[str, dict] = {}

    total_contributions = 0
    total_expenditures  = 0
    file_handles = []

    def register_candidate(name: str, office: str = "", district: str = "",
                           party: str = "", election_year: str = "",
                           incumbent: str = "", raw_file: str = "",
                           row_num="") -> str:
        """Register/enrich a candidate keyed by their normalized forward name.

        Returns the normalized name (the join key used everywhere else), or ''
        for a blank input. Enrichment is first-non-blank-wins: the roster files
        are processed before the transaction files, so roster values (office,
        district, party) are already in place and transaction rows only ever
        fill gaps."""
        cname = display_name(name)
        if not cname:
            return ""
        cand = candidates.get(cname)
        if cand is None:
            first, last = split_name(name)
            cand = {
                "state":           STATE,
                "candidate_name":  cname,
                "candidate_first": utils.clean_name(first),
                "candidate_last":  utils.clean_name(last),
                "office":          clean(office),
                "district":        clean(district),
                "jurisdiction":    "",   # TN publishes no jurisdiction field
                "party":           clean(party),
                "election_year":   parse_year(election_year),
                "incumbent":       clean(incumbent),
                "state_filer_id":  "",   # no filer ID anywhere in TNCAMP exports
                "raw_file":        raw_file,
                "row_num":         row_num,
            }
            candidates[cname] = cand
        else:
            _fill(cand, "office",    clean(office))
            _fill(cand, "district",  clean(district))
            _fill(cand, "party",     clean(party))
            _fill(cand, "incumbent", clean(incumbent))
            # election_year tracks the latest cycle seen, not the first.
            new_y = parse_year(election_year)
            cur_y = cand.get("election_year", "")
            if new_y and (not cur_y or int(new_y) > int(cur_y)):
                cand["election_year"] = new_y
        return cname

    def register_committee(name: str, committee_type: str = "",
                           candidate_name: str = "", treasurer: str = "",
                           city: str = "", zipc: str = "",
                           election_year: str = "", active: str = "",
                           raw_file: str = "", row_num="") -> str:
        """Register/enrich a committee keyed by its normalized name."""
        cname = utils.clean_name(name)
        if not cname:
            return ""
        cmte = committees.get(cname)
        if cmte is None:
            committees[cname] = {
                "state":           STATE,
                "committee_name":  cname,
                "committee_type":  clean(committee_type),
                "election_year":   parse_year(election_year),
                "candidate_name":  display_name(candidate_name),
                "treasurer_name":  display_name(treasurer),
                "city":            utils.clean_name(city),
                "zip":             utils.clean_zip(clean(zipc)),
                "active":          clean(active),
                "state_filer_id":  "",
                "raw_file":        raw_file,
                "row_num":         row_num,
            }
        else:
            _fill(cmte, "committee_type", clean(committee_type))
            _fill(cmte, "candidate_name", display_name(candidate_name))
            _fill(cmte, "treasurer_name", display_name(treasurer))
            _fill(cmte, "city",           utils.clean_name(city))
            _fill(cmte, "zip",            utils.clean_zip(clean(zipc)))
            _fill(cmte, "active",         clean(active))
            _fill(cmte, "election_year",  parse_year(election_year))
        return cname

    def process_roster_row(row: list, hmap: dict, path: Path, row_num: int) -> bool:
        """Register one roster row as a candidate or a committee.

        Shared by both the candidates_p*.csv and pacs_p*.csv loops: which
        file a row came from doesn't say what it is (see module docstring),
        so every row is classified by its own content via is_person_row()
        regardless of source file. Returns False for a row with no usable
        name (blank First Name AND Last Name)."""
        name = roster_entity_name(row, hmap)
        if not name:
            return False

        # The Election Year column is authoritative; the year in the filename
        # is the search criterion the page was fetched under and is used only
        # as a fallback, exactly as the transaction loops do. It's '' for the
        # unyeared PAC roster, which has no election year to fall back to.
        election_year = (get(row, hmap, "election_year")
                         or year_from_filename(path))

        if is_person_row(row, hmap):
            # Primary/General carry the election outcome, not a plain Y/N
            # flag — see parse_election_result(). A general-election win is
            # the closer proxy for a sitting officeholder.
            incumbent = parse_election_result(get(row, hmap, "general"))
            cname = register_candidate(
                name,
                office=get(row, hmap, "office_sought"),
                district=get(row, hmap, "district"),
                party=get(row, hmap, "party"),
                election_year=election_year,
                incumbent=incumbent,
                raw_file=path.name, row_num=row_num,
            )
            # A candidate's own committee is named in the roster's Committee
            # Affiliation column when TN's export carries it — it currently
            # doesn't (see module docstring), so this branch is presently
            # dormant; kept in case TN's export gains the column back.
            affiliation = get(row, hmap, "committee_affiliation")
            if affiliation:
                _, t_city, _, t_zip = split_address(
                    get(row, hmap, "treasurer_contact")
                    or get(row, hmap, "contact_info"))
                register_committee(
                    affiliation,
                    committee_type="Candidate Committee",
                    candidate_name=cname,
                    treasurer=get(row, hmap, "treasurer_name"),
                    city=t_city, zipc=t_zip,
                    election_year=election_year,
                    active="0" if get(row, hmap, "closed") else "1",
                    raw_file=path.name, row_num=row_num,
                )
        else:
            _, city, _, zipc = split_address(
                get(row, hmap, "treasurer_contact")
                or get(row, hmap, "contact_info"))
            register_committee(
                name,
                committee_type="PAC",
                treasurer=get(row, hmap, "treasurer_name"),
                city=city, zipc=zipc,
                election_year=election_year,
                # TN's roster has a "Closed" date rather than an active flag:
                # a populated Closed means the entity is shut.
                active="0" if get(row, hmap, "closed") else "1",
                raw_file=path.name, row_num=row_num,
            )
        return True

    try:
        # =================== 1. Candidate roster ===================
        # Processed first so office/district/party are already populated when
        # the transaction files reference the same people by name.
        logged_headers: set[str] = set()

        # Both roster files are read through the same per-row classifier
        # (process_roster_row) since candidate/organization rows are mixed in
        # both — see module docstring. The "candidates"/"committees" log
        # labels below describe which file was read, not what every row in it
        # turned out to be.
        for path in raw_files(*CANDIDATE_GLOBS):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                hmap   = build_header_map(header)
                if "candidates" not in logged_headers:
                    log.info(f"  candidate roster headers resolved: "
                             f"{sorted(hmap)} (from {header})")
                    logged_headers.add("candidates")
                for row_num, row in enumerate(reader, start=2):
                    if process_roster_row(row, hmap, path, row_num):
                        count += 1
            log.file_parsed(path.name, "candidates", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)

        # =================== 2. PAC roster ===================
        for path in raw_files("pacs_p*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                hmap   = build_header_map(header)
                if "pacs" not in logged_headers:
                    log.info(f"  PAC roster headers resolved: "
                             f"{sorted(hmap)} (from {header})")
                    logged_headers.add("pacs")
                for row_num, row in enumerate(reader, start=2):
                    if process_roster_row(row, hmap, path, row_num):
                        count += 1
            log.file_parsed(path.name, "committees", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)

        log.registry_loaded("candidates_*_p*.csv / pacs_p*.csv",
                            entries=len(candidates) + len(committees),
                            relation="candidates+committees")

        # =================== 3. Contributions ===================
        # Each handle is appended to file_handles as it's created, not in one
        # list literal afterwards: if the second open_writer() raises, the
        # first handle would otherwise never reach the finally block.
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        file_handles.append(cont_fh)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        file_handles.append(expn_fh)
        # TNCAMP's public C&E search exposes no loan schedules — the file is
        # written with a header and no rows so tabulate/aggregate see a
        # consistent five-table output for every state.
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles.append(loan_fh)

        for path in raw_files("contributions_*_p*.csv"):
            ft, count = time.perf_counter(), 0
            file_year = year_from_filename(path)
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                hmap   = build_header_map(header)
                if "contributions" not in logged_headers:
                    log.info(f"  contribution headers resolved: "
                             f"{sorted(hmap)} (from {header})")
                    logged_headers.add("contributions")

                for row_num, row in enumerate(reader, start=2):
                    amount = parse_amount(get(row, hmap, "amount"))
                    if not amount:
                        continue

                    recipient = get(row, hmap, "recipient_name")
                    # The election year column is the authoritative one; the
                    # filename year is the search criterion and is used only
                    # when the column is missing or blank.
                    ey = parse_year(get(row, hmap, "election_year")) or file_year

                    _street, city, st, zipc = split_address(
                        get(row, hmap, "contributor_address"))

                    # The recipient of a TN contribution is a candidate or a
                    # PAC and the export doesn't say which. Resolve against the
                    # rosters loaded above: a name already known as a candidate
                    # is one, anything else is treated as a committee.
                    rec_name, cmte_name, is_candidate = resolve_recipient(
                        recipient, candidates)
                    if not is_candidate and cmte_name not in committees:
                        # Recipient appears in the transactions but not in
                        # either roster — register it as a committee so the
                        # money is still attributable to a named entity.
                        register_committee(recipient, election_year=ey,
                                           raw_file=path.name, row_num=row_num)

                    # A committee recipient inherits the candidate it was
                    # linked to via the roster's Committee Affiliation column,
                    # so office resolves the same way whether the money went to
                    # the candidate directly or to their committee.
                    cand_name = (rec_name if is_candidate else
                                 committees.get(cmte_name, {}).get("candidate_name", ""))
                    office    = candidates.get(cand_name, {}).get("office", "")

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cmte_name,
                        "amount":            amount,
                        "date":              parse_date(get(row, hmap, "date")),
                        # "Type" is TN's contribution type (monetary / in-kind /
                        # etc.); "Adjustment" marks corrections and amendments
                        # and is appended so it isn't lost — see
                        # src/aliases/transaction_categories.csv for the mapping.
                        "transaction_type":  _txn_type(get(row, hmap, "type"),
                                                       get(row, hmap, "adjustment")),
                        "contributor_name":  display_name(get(row, hmap, "contributor_name")),
                        "contributor_type":  "",   # not published by TNCAMP
                        "contributor_city":  utils.clean_name(city),
                        "contributor_state": st,
                        "contributor_zip":   utils.clean_zip(zipc),
                        "employer":          utils.clean_name(get(row, hmap, "contributor_employer")),
                        "occupation":        utils.clean_name(get(row, hmap, "contributor_occupation")),
                        "candidate_name":    cand_name,
                        "office":            office,
                        "election_year":     ey,
                        "amended":           _amended(get(row, hmap, "adjustment")),
                        "filing_id":         get(row, hmap, "report_name"),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)
            total_contributions += count

        # =================== 4. Expenditures ===================
        for path in raw_files("expenditures_*_p*.csv"):
            ft, count = time.perf_counter(), 0
            file_year = year_from_filename(path)
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                hmap   = build_header_map(header)
                if "expenditures" not in logged_headers:
                    log.info(f"  expenditure headers resolved: "
                             f"{sorted(hmap)} (from {header})")
                    logged_headers.add("expenditures")

                for row_num, row in enumerate(reader, start=2):
                    amount = parse_amount(get(row, hmap, "amount"))
                    if not amount:
                        continue

                    recipient = get(row, hmap, "recipient_name")
                    ey = parse_year(get(row, hmap, "election_year")) or file_year
                    _street, city, st, zipc = split_address(
                        get(row, hmap, "vendor_address"))

                    rec_name, cmte_name, is_candidate = resolve_recipient(
                        recipient, candidates)
                    if not is_candidate and cmte_name not in committees:
                        register_committee(recipient, election_year=ey,
                                           raw_file=path.name, row_num=row_num)

                    cand_name = (rec_name if is_candidate else
                                 committees.get(cmte_name, {}).get("candidate_name", ""))
                    office    = candidates.get(cand_name, {}).get("office", "")

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "amount":           amount,
                        "date":             parse_date(get(row, hmap, "date")),
                        "transaction_type": _txn_type(get(row, hmap, "type"),
                                                      get(row, hmap, "adjustment")),
                        "payee_name":       display_name(get(row, hmap, "vendor_name")),
                        "purpose":          get(row, hmap, "purpose") or get(row, hmap, "description"),
                        "category":         "",   # TN has no expenditure category code
                        "payee_city":       utils.clean_name(city),
                        "payee_state":      st,
                        "payee_zip":        utils.clean_zip(zipc),
                        "candidate_name":   cand_name,
                        "office":           office,
                        "election_year":    ey,
                        "amended":          _amended(get(row, hmap, "adjustment")),
                        "filing_id":        get(row, hmap, "report_name"),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)
            total_expenditures += count

        # =================== 5. Flush candidates + committees ===================
        cand_fh, cand_w = open_writer("candidates.csv.gz", C.CANDIDATES)
        file_handles.append(cand_fh)
        cmte_fh, cmte_w = open_writer("committees.csv.gz", C.COMMITTEES)
        file_handles.append(cmte_fh)

        for row in candidates.values():
            cand_w.writerow(row)
        for row in committees.values():
            cmte_w.writerow(row)

        for fh in file_handles:
            fh.close()
        file_handles = []

        # name_hash: TNCAMP publishes no filer ID of any kind, so person_id is
        # derived from MD5(state + normalized candidate name). Same model as
        # Alaska/Kansas/Kentucky — see the module docstring.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="name_hash")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   0,
                        role="output", bytes=_out_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    len(committees),
                        role="output", bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    len(candidates),
                        role="output", bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=0, committees=len(committees),
                  candidates=len(candidates))

    except KeyboardInterrupt:
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


# ====== CLI ==================================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
