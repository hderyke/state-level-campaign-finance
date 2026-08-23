"""
parsers/utah.py — Parse Utah campaign finance data into the canonical
cleaned CSVs.

Reads from data/Utah/raw/ (see scrapers/utah.py for how it gets there):

  transactions_{slug}_{year}.csv   one file per (entity type, report year);
                                   contributions AND expenditures together
  entities.csv                     entity roster from the search grid —
                                   entity_id, name, type, active, years
  OpenStates_People.csv            optional party overlay (--party)
  UT_ElectionResults.csv           optional party overlay (--party)

Writes to data/Utah/cleaned/: contributions, expenditures, candidates,
committees, loans_debts (all .csv.gz).

## The transaction files

One row per itemized transaction. Header, as emitted by the bulk
(EntityType) export:

    FILED, <TYPE>, REPORT, TRAN_ID, TRAN_TYPE, TRAN_DATE, TRAN_AMT,
    INKIND, LOAN, AMENDS, NAME, PURPOSE, ADDRESS1, ADDRESS2, CITY,
    STATE, ZIP, INKIND_COMMENTS

Column 2's *header name* is the entity type (`PAC`, `PIC`, `CORPORATION`,
...) and its value is the filing entity's name. The per-entity export
(`GenerateReport/<id>`) drops the leading `FILED` column, so there the
entity name is column 1. `_header_map()` resolves both by locating
`REPORT` and taking the column immediately to its left, rather than
trusting either position — see the 484-2014.csv sample, which is the
17-column per-entity shape.

Field semantics, as far as the source defines them:

  FILED             "X" when the transaction appears on a filed report;
                    blank when it is sitting in the ledger unfiled. Kept
                    either way (an unfiled ledger entry is still a real
                    disclosed transaction), recorded nowhere in the
                    canonical schema.
  REPORT            reporting period label — "Convention", "Primary",
                    "General", "Year End", "August 31st", ...
  TRAN_TYPE         "Contribution" or "Expenditure". The only routing key.
  INKIND / LOAN     "X" or blank. INKIND folds into transaction_type
                    (a " (In-Kind)" suffix on expenditures, a distinct
                    label on contributions); LOAN diverts the row to
                    loans_debts.csv.gz instead of double-counting it.
  AMENDS            the TRAN_ID this row amends; blank on originals.
                    Presence, not the value, becomes amended=1.
  NAME              the counterparty — contributor on a contribution,
                    payee on an expenditure. Never the filer.
  PURPOSE           on expenditures, filer-typed FREE TEXT — NOT the
                    statutory category list the LG's guides describe.
                    Real values: "Campaign Contribution", "Contribution",
                    "POLITICAL CONTRIBUTION", "Processing fee",
                    "CONTRIBUTATION" (sic), blank. Written raw to
                    `category`; `transaction_type` gets a derived label
                    instead (see expenditure_type). Almost always blank
                    on contributions.
  INKIND_COMMENTS   free-text description of an in-kind item. Written to
                    expenditures.purpose.

ADDRESS1/ADDRESS2 are dropped — the canonical schema has no street
address column for either side of a transaction.

## Quoting defect

Utah's export quotes most fields but does not escape double quotes that
occur *inside* a field, so the files are not valid CSV. `_repair_line()`
rewrites any quote that neither opens nor closes a field (i.e. is not at a
field boundary or adjacent to a comma) into an apostrophe, leaving only
structural quotes behind. That makes the stream safe for `csv.reader`,
including genuinely multi-line quoted fields, which then join correctly
instead of breaking the row.

The repair is inherently ambiguous in one case: a field containing both a
stray quote and a comma can still mis-split. Those rows fail the
field-count check and are skipped and counted (`malformed` in the log),
rather than silently producing garbage columns. The Accountability
Project's Utah cleaner has the same limitation with the same regex
approach.

## The candidacy annotation — where office, district and year come from

The transaction files carry filer *names* and nothing else, so
`entities.csv` is doing more work here than a roster usually does. Every
"Candidates & Office Holders" folder is named

    Last, First (YYYY Office-District)

confirmed live on the search grid: "Abbott, Nelson (2022 House-57)",
"Aagard, Doug (2008 House-15)", "Aalders, Tim (2012 Lieutenant
Governor)". That annotation is the only place Utah states a candidate's
office, district or election cycle anywhere in its published data, and
`parse_candidacy()` reads all three off it. Organization folders reuse
the same bracket syntax for aliases ("4Life Research USA (aka. 4Life)"),
so the split only fires when the group opens with a four-digit year.

It also settles the identity model. A folder is per-**candidacy**, not
per-person: one man's 2008 House-15 run and his 2012 House-15 run are two
folders with two ids. state_filer_id is therefore a registration id, and
**id_model="committee"** — `assign_person_ids()` groups on
`(state, candidate_name, office, district)` and takes
`person_id = min(state_filer_id)`, merging the same person's runs for the
same seat while keeping two same-named people in different seats apart.
This is why the annotation's office and district matter beyond reporting.

One row per entity, then, not per report year: the annotation *is* the
cycle, and organizations keep one folder for life. `election_year` on
committees is populated for candidates and blank for organizations,
matching that column's "sparse — cycle-specific states only" contract.

## Reconciling the two spellings of a filer name

The roster annotates every candidate folder, but the itemized export has
been observed writing bare personal names — "King, Brian S", "Eliason,
Steven" in the Accountability Project's 2000-2023 pull. `Roster.resolve()`
therefore tries the full annotated name first, then treats the value as a
base name and uses the file's report year to pick the candidacy. Both
spellings land on the same roster record, so a filer gets one committee
row rather than two with its transactions split between them, and
`contributions.committee_name` carries the roster's canonical name.
Traceability back to the literal source cell is preserved by
`raw_file` + `row_num`. A filer that still can't be resolved keeps its
raw name and no `state_filer_id`. Resolution tops out around **97.8%** on
a real corpus with a complete roster: the per-year exports still contain
filers the live search grid has since purged, so ~106 named entities can
never take an id. That is why `state_filer_id` is a tier-2 warning for UT
in validate.py rather than a tier-1 failure. The run logs its resolution
rate and warns below 95%, where the cause really is a stale roster.

Two name forms have to be reconciled for that to work, both measured on
real data: 548 of 4,463 roster entities carry an `(aka. …)` group the
transaction export omits, and candidate folders are annotated where the
export writes bare personal names (0 of 358 filers in a real
`transactions_pcc_2024.csv` carried an annotation). `split_alias()` and
`parse_candidacy()` index every spelling an entity answers to; without
the alias half, resolution was 60.2%.

Candidate names are flipped from "Last, First M" to "FIRST M LAST" for
`candidates.candidate_name` and `committees.candidate_name` (what
`utils.assign_committee_person_ids()` matches on), while
`committees.committee_name` keeps Utah's own spelling.

## What Utah does not provide

No party, no contributor type, no employer, no occupation. Office,
district and election year exist only via the candidacy annotation above,
so they are blank for organizations and for candidate folders Utah never
annotated. `candidates.office` is written canonicalized rather than raw —
see `office_out()` for why the usual convention is inverted here. `contributor_type` is deliberately left blank —
`aggregate.py` already backfills it for any contributor whose name
matches a registered committee, which covers Utah's committee-to-committee
flow without this parser guessing.

## Party overlay

`UTEnrichment` reads the two optional files scrapers/utah.py writes under
`--party` and only ever *fills blanks* — it never overwrites anything
Utah itself said. Three tiers, first hit wins:

  1. **entity-id join** (`"exact"`). Open States' `links`/`sources`
     columns contain each sitting legislator's own disclosures.utah.gov
     FolderDetails id, which is the same state_filer_id the roster gives.
     Same id, same person — no name comparison happens at all.
  2. **Open States name match** (always `"high"`). Full normalized name,
     or first+last if the full key misses. Open States' `current_district`
     is the seat the person holds *today* with no year attached, so it is
     used only to raise confidence in the pick, never to reject a
     candidate whose historical seat differs — comparing a 2012 candidacy
     against a 2026 district compares two different things.
  3. **Canvass name match** (`"exact"` / `"high"`). Every canvass row
     names its own election year, so office and district are comparable
     here: a contradicting district means a different seat and is
     discarded, and `exact` requires district and year to agree.

A name that maps to two different parties is declined outright rather
than resolved by preference: a wrong party on a real person is worse than
a blank one. There is no nickname expansion, soundex or edit distance
here for the same reason.

Both name tiers also backfill `office`/`district` on the minority of
candidates whose folder carried no annotation. Everything written to
`candidates.office` — Utah's own "House" and the overlay's "State
Representative" alike — goes through `office_out()` first, so both arrive
as one canonical vocabulary; `party_source`/`match_confidence` mark
exactly which rows came from outside.

!! Tier 3 is only as good as the unverified workbook scanner in
scrapers/utah.py — check its row counts before trusting it. See
docs/states/utah.md.
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
from src.reporting.logger import get_logger
from src.aliases import (canonical_office_type, canonical_party,
                         office_type_mappings)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Utah" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Utah" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "UT"

ENTITIES_FILE   = "entities.csv"
OPENSTATES_FILE = "OpenStates_People.csv"
CANVASS_FILE    = "UT_ElectionResults.csv"

# ========================= entity type vocabulary =====================
# Keys are the filename slugs scrapers/utah.py writes; values are the
# committee_type strings this parser emits (matching the roster's own
# display labels so both paths produce one vocabulary). The
# src/aliases/committee_types.csv rows for UT map these to canonical
# labels at aggregate time.
SLUG_TO_TYPE = {
    "pcc":    "Candidates & Office Holders",
    "pac":    "Political Action Committee",
    "pic":    "Political Issues Committee",
    "party":  "Political Party",
    "corp":   "Corporation",
    "labor":  "Labor Organizations",
    "elect":  "Electioneering",
    "indexp": "Independent Expenditures",
}

# Entity types whose filer is a person standing for office. Only these
# produce candidates.csv rows.
CANDIDATE_TYPES = {"Candidates & Office Holders"}

_TXN_FILE_RE = re.compile(r"^transactions_([a-z]+)_(\d{4})\.csv$")

# ---- the candidacy annotation on PCC folder names ----
#
# Every "Candidates & Office Holders" folder is named
# "Last, First (YYYY Office-District)" — confirmed against the live search
# grid: "Abbott, Nelson (2022 House-57)", "Aagard, Doug (2008 House-15)",
# "Aalders, Tim (2012 Lieutenant Governor)". So a Utah candidate folder is
# per-CANDIDACY, not per-person, and it carries the three things Utah's
# transaction export otherwise omits entirely: election year, office and
# district. That is why this parser reads office/district/election_year off
# the entity name rather than leaving them blank for the overlay.
#
# The trailing group is only treated as a candidacy annotation when it opens
# with a four-digit year — organization folders use the same bracket syntax
# for aliases ("4Life Research USA (aka. 4Life)", "AARP Utah (aka. AARP
# Utah)"), and a name may carry both, in which case the candidacy suffix is
# the last one.
_PCC_ANNOT_RE = re.compile(
    r"\s*\(\s*(?P<year>(?:19|20)\d{2})\s+(?P<rest>[^()]*?)\s*\)\s*$")

# Office and district inside the annotation are joined by a hyphen
# ("House-57"); statewide seats carry no district ("Lieutenant Governor").
# Split on the LAST hyphen so multi-word hyphenated offices survive.
_PCC_DISTRICT_RE = re.compile(r"^(?P<office>.*?)\s*-\s*(?P<district>[A-Za-z0-9]{1,6})$")

# Name suffixes that must not be mistaken for a given name when flipping
# "Last, First" — "Smith Jr., John" and "Smith, John, Jr." both occur.
_NAME_SUFFIXES = {"JR", "JR.", "SR", "SR.", "II", "III", "IV", "V",
                  "MD", "M.D.", "PHD", "PH.D.", "ESQ", "ESQ."}


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return re.sub(r"\s+", " ", (val or "").strip())


def parse_amount(val) -> str:
    """Parse a dollar amount to a plain numeric string. '' on failure."""
    v = clean(val).replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]           # parentheses = negative
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val) -> str:
    """Normalize to YYYY-MM-DD. '' on failure or implausible year.

    Utah writes M/D/YYYY throughout ("4/2/2014"); the other formats are
    cheap insurance against a future export change.
    """
    v = clean(val)
    if not v:
        return ""
    v = v.split(" ")[0]             # drop any trailing time component
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
        try:
            d = datetime.strptime(v, fmt)
        except ValueError:
            continue
        if d.year < 1990 or d.year > date.today().year + 2:
            return ""
        return d.strftime("%Y-%m-%d")
    return ""


# PURPOSE is filer-typed FREE TEXT, not the controlled vocabulary the LG's
# quick guides imply. Measured across four real PAC files: 'Campaign
# Contribution', 'Contribution', 'POLITICAL CONTRIBUTION', 'contribution',
# 'Political contribution', 'Donation', 'Campaign Donation', 'CONTRIBUTATION'
# (sic), 'Processing fee', 'Consulting', 'Office Expenses', 'Staff Support',
# blank — hundreds of spellings of a handful of ideas.
#
# So it cannot drive transaction_category through an alias table: any finite
# list of (state, raw) pairs would leave most rows unmapped, i.e. NULL, which
# is worse than a coarse-but-correct value on every row. The raw string still
# goes to `category` verbatim; transaction_type instead gets a derived label
# from a controlled set of four, which expenditure_categories.csv maps
# exhaustively.
#
# The one distinction worth rescuing from the text is contribution-vs-
# operating-spend: a committee's donation to a candidate is a categorically
# different act from buying printing, and it is by far the most common thing
# these strings say. This matches on the stem so 'contribut', 'Contribution',
# 'CONTRIBUTATION' and 'contributions' all land together.
_PURPOSE_CONTRIB_RE = re.compile(r"contribut|donat", re.I)


def expenditure_type(purpose: str, inkind: bool) -> str:
    """Controlled transaction_type for an expenditure row.

    One of: "Expenditure", "Contribution Expenditure", and their
    " (In-Kind)" variants. In-kind wins over the contribution reading
    because it is a hard flag from the source rather than an inference from
    free text.
    """
    base = ("Contribution Expenditure"
            if _PURPOSE_CONTRIB_RE.search(purpose or "") else "Expenditure")
    return f"{base} (In-Kind)" if inkind else base


def clean_state(val) -> str:
    """Uppercase a two-letter state code, pass anything else through.

    Utah's filers type this field freehand: the raw data carries "Ut",
    "ut", "ny" alongside the correct forms. Uppercasing only the
    two-letter values keeps validate.py's contributor_state/payee_state
    check from flagging what is purely a casing artifact, without
    inventing a code for the long-form or garbage entries.
    """
    v = clean(val)
    return v.upper() if len(v) == 2 and v.isalpha() else v


def flag(val) -> bool:
    """Utah's quasi-boolean columns hold 'X' for true and blank for false."""
    return clean(val).upper() in {"X", "Y", "YES", "TRUE", "1"}


def parse_candidacy(entity_name: str) -> dict:
    """Split a PCC folder name into its person and candidacy parts.

    "Abbott, Nelson (2022 House-57)" ->
        {"base": "Abbott, Nelson", "election_year": "2022",
         "office": "House", "district": "57"}

    A name with no year-led annotation (an organization, or a candidate
    folder from before Utah started annotating them) comes back with the
    whole string as `base` and the other three blank.
    """
    name = clean(entity_name)
    m = _PCC_ANNOT_RE.search(name)
    if not m:
        return {"base": name, "election_year": "", "office": "", "district": ""}
    rest = clean(m.group("rest"))
    office, district = rest, ""
    dm = _PCC_DISTRICT_RE.match(rest)
    if dm:
        office = clean(dm.group("office"))
        district = dm.group("district").lstrip("0") or dm.group("district")
    return {
        "base":          clean(name[:m.start()]),
        "election_year": m.group("year"),
        "office":        office,
        "district":      district,
    }


# A trailing "(... aka X)" group on a roster name. 547 of 4,463 real roster
# entities carry one — "Zions Bancorporation Political Action Committee (aka.
# ZB NA)", "Our Schools Now (aka. Our Kids 1st and Utah Students for Question
# 1)" — and the transaction export writes the primary name WITHOUT it. Left
# unhandled this was the single largest cause of unresolved filers, costing
# ~34 percentage points of filer resolution on real data (PACs 37%, PICs 28%).
#
# The "aka" is not always at the start of the group: "2023 Utah Flag Referedum
# (2023 Utah Flag Referendum aka. Utah Flag Referendum)" puts a corrected
# spelling first. So the group is matched on *containing* an aka token, and
# every comma-free chunk around it is indexed as a name this entity answers to.
_ALIAS_GROUP_RE = re.compile(
    r"\s*\((?P<inner>[^()]*\ba\.?k\.?a\.?\b[^()]*)\)\s*$", re.I)
_AKA_SPLIT_RE = re.compile(r"\ba\.?k\.?a\.?\b\.?\s*", re.I)


def split_alias(name: str) -> tuple[str, list[str]]:
    """('primary name', ['alias', ...]) — strips a trailing '(… aka X)' group.

    Returns the name unchanged with an empty alias list when there is no
    such group, so it is safe to call on every entity.
    """
    m = _ALIAS_GROUP_RE.search(name or "")
    if not m:
        return name, []
    aliases = [p.strip(" .") for p in _AKA_SPLIT_RE.split(m.group("inner"))
               if p.strip(" .")]
    return name[:m.start()].strip(), aliases


def _is_suffix(tok: str) -> bool:
    up = tok.upper()
    return up in _NAME_SUFFIXES or up.rstrip(".") + "." in _NAME_SUFFIXES


def flip_name(raw: str) -> str:
    """'King, Brian S' -> 'BRIAN S KING'. Returns clean_name(raw) unchanged
    when there's no comma (organization-style names, and the handful of PCC
    filers registered under a committee name rather than a personal one).

    A trailing generational/professional suffix is moved to the end rather
    than treated as a given name, so both "Smith, John, Jr." and
    "Smith Jr., John" come out as "JOHN SMITH JR".
    """
    name = clean(raw)
    if "," not in name:
        return utils.clean_name(name)
    parts = [p.strip() for p in name.split(",") if p.strip()]
    if len(parts) < 2:
        return utils.clean_name(name)
    last, first = parts[0], parts[1]
    extra = parts[2:]
    # "Smith Jr., John" — suffix rode along on the surname
    last_toks = last.split()
    suffix = []
    while len(last_toks) > 1 and _is_suffix(last_toks[-1]):
        suffix.insert(0, last_toks.pop())
    last = " ".join(last_toks)
    # Drop the period off a trailing suffix ("JR." -> "JR") so the same
    # person spelled "Smith Jr., John" and "Smith, John, Jr" lands on one
    # name key for the party overlay and for person-id grouping.
    tail = [t.rstrip(".") for t in [*suffix, *extra]]
    return utils.clean_name(" ".join([first, last, *tail]))


def split_person(raw: str) -> tuple[str, str]:
    """(first, last) from a 'Last, First M' filer name. ('', '') if not
    person-shaped."""
    name = clean(raw)
    if "," not in name:
        return "", ""
    parts = [p.strip() for p in name.split(",") if p.strip()]
    if len(parts) < 2:
        return "", ""
    last_toks = [t for t in parts[0].split() if not _is_suffix(t)]
    first_toks = parts[1].split()
    return (utils.clean_name(first_toks[0]) if first_toks else "",
            utils.clean_name(" ".join(last_toks)))


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def raw_files(pattern: str) -> list[Path]:
    return sorted((f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
                  key=lambda p: p.name)


# ======================== CSV quoting repair ==========================

def _repair_line(line: str) -> str:
    """Turn Utah's unescaped in-field double quotes into apostrophes.

    A quote is structural only if it sits at the very start or end of the
    line or is adjacent to a comma on the correct side; anything else is a
    literal quote the exporter failed to escape. Rewriting those leaves a
    stream `csv.reader` can parse — including multi-line quoted fields,
    which only join correctly once the quote count is balanced.
    """
    if '"' not in line:
        return line
    stripped = line.rstrip("\r\n")
    eol = line[len(stripped):]
    n = len(stripped)
    out = []
    for i, ch in enumerate(stripped):
        if ch != '"':
            out.append(ch)
            continue
        opens  = (i == 0)     or stripped[i - 1] == ","
        closes = (i == n - 1) or stripped[i + 1] == ","
        out.append(ch if (opens or closes) else "'")
    return "".join(out) + eol


def _repaired_lines(path: Path):
    """Yield repaired physical lines from a raw transaction CSV."""
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for line in f:
            yield _repair_line(line)


# ---- header resolution ----

# Every column the parser wants, by its header name. The entity-name column
# is the one exception: its header is the entity type, so it's found
# positionally (immediately left of REPORT) instead.
_WANTED = ["REPORT", "TRAN_ID", "TRAN_TYPE", "TRAN_DATE", "TRAN_AMT",
           "INKIND", "LOAN", "AMENDS", "NAME", "PURPOSE",
           "ADDRESS1", "ADDRESS2", "CITY", "STATE", "ZIP", "INKIND_COMMENTS"]


def _header_map(header: list[str]) -> dict[str, int] | None:
    """Map logical field -> column index. None if the header is unusable.

    Works for both export shapes because it anchors on REPORT: the entity
    name is always the column immediately to its left, whether or not a
    leading FILED column is present.
    """
    up = [clean(h).upper().lstrip("﻿") for h in header]
    if "REPORT" not in up:
        return None
    idx = {name: up.index(name) for name in _WANTED if name in up}
    if "TRAN_TYPE" not in idx or "TRAN_AMT" not in idx:
        return None
    ent = up.index("REPORT") - 1
    if ent < 0:
        return None
    idx["ENTITY_NAME"] = ent
    # FILED is deliberately not indexed: an unfiled ledger entry is still a
    # real disclosed transaction, the canonical schema has nowhere to record
    # the distinction, and rows are kept either way.
    return idx


def _get(row: list[str], idx: dict, key: str) -> str:
    i = idx.get(key, -1)
    if i < 0 or i >= len(row):
        return ""
    return clean(row[i])


# ======================== roster (entities.csv) ========================

class Roster:
    """data/Utah/raw/entities.csv, indexed for filer-name lookup.

    Two indexes, because it isn't certain which spelling of a candidate's
    name the transaction export uses:

      _by_full  the whole folder name, annotation included
                ("ABBOTT, NELSON (2022 HOUSE-57)")
      _by_base  the name with the candidacy annotation stripped
                ("ABBOTT, NELSON")

    The live search grid annotates every PCC folder, but the itemized
    export has been observed carrying bare personal names ("King, Brian
    S", "Eliason, Steven" in the Accountability Project's 2000-2023 pull),
    so `resolve()` tries the full name first and falls back to the base
    name disambiguated by report year. Either spelling therefore lands on
    the same roster record — and on one committee row, not two.

    A name that maps to several entity ids and cannot be narrowed is left
    without a `state_filer_id` rather than assigned one of them: a wrong
    id silently merges two real filers under one person_id, which is worse
    than no id at all.
    """

    def __init__(self):
        self.by_id: dict[str, dict] = {}
        self._by_full: dict[str, list[dict]] = defaultdict(list)
        self._by_base: dict[str, list[dict]] = defaultdict(list)
        self.ambiguous: set[str] = set()
        self.annotated = 0
        self.aliased = 0
        self.available = False

    @classmethod
    def load(cls, log) -> "Roster":
        self = cls()
        path = RAW_DIR / ENTITIES_FILE
        if not path.exists() or path.stat().st_size == 0:
            log.warning(f"  No {ENTITIES_FILE} in data/Utah/raw — committees get no "
                        f"state_filer_id, candidates get no office/district/"
                        f"election_year, and the exact-id party join is "
                        f"unavailable. Run `scrapers/utah.py --entities`.")
            return self
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                eid = clean(row.get("entity_id"))
                name = clean(row.get("entity_name"))
                if not eid or not name:
                    continue
                etype = (clean(row.get("entity_type_label"))
                         or SLUG_TO_TYPE.get(clean(row.get("entity_type")).lower(), ""))
                # Only candidate folders carry a candidacy annotation; running
                # the split on organizations would misread an alias bracket.
                ann = (parse_candidacy(name) if etype in CANDIDATE_TYPES
                       else {"base": name, "election_year": "",
                             "office": "", "district": ""})
                if ann["election_year"]:
                    self.annotated += 1
                # Every spelling this entity answers to, beyond its full
                # roster name: the candidacy-annotation-stripped base, the
                # alias-group-stripped primary, and each alias itself. The
                # transaction export uses the bare primary; a name may carry
                # both kinds of group at once ("Jones, Amy (aka. Amie) (2024
                # State School Board-3)"), so the alias split runs on the
                # already-de-annotated base.
                primary, aliases = split_alias(ann["base"])
                if primary != ann["base"]:
                    self.aliased += 1
                alt_keys = {utils.clean_name(k)
                            for k in [ann["base"], primary, *aliases] if k}
                rec = {
                    "entity_id":     eid,
                    "entity_name":   name,
                    "norm_full":     utils.clean_name(name),
                    "norm_base":     utils.clean_name(ann["base"]),
                    "type":          etype,
                    "active":        clean(row.get("active")) or "1",
                    "election_year": ann["election_year"],
                    "office":        ann["office"],
                    "district":      ann["district"],
                    # Years this entity has a downloadable export for. Kept
                    # for traceability only — committee/candidate rows take
                    # their election_year from the folder annotation, not from
                    # this, since one row per entity replaced one per year.
                    "years":         sorted({
                        y for y in (row.get("data_years") or "").split("|")
                        if re.fullmatch(r"\d{4}", y or "")
                    }),
                    "row_num":       row_num,
                }
                self.by_id[eid] = rec
                self._by_full[rec["norm_full"]].append(rec)
                for k in alt_keys - {rec["norm_full"]}:
                    self._by_base[k].append(rec)
        for norm, recs in self._by_full.items():
            if len({r["entity_id"] for r in recs}) > 1:
                self.ambiguous.add(norm)
        self.available = bool(self.by_id)
        if self.available:
            log.registry_loaded(ENTITIES_FILE, entries=len(self.by_id),
                                relation="committees+candidates",
                                bytes=path.stat().st_size)
            log.info(f"  {self.annotated:,} candidate folders carry a "
                     f"'(YYYY Office-District)' annotation — office, district "
                     f"and election year come from there")
            log.info(f"  {self.aliased:,} entities carry an '(aka. …)' group the "
                     f"transaction export omits — indexed under both spellings")
            if self.ambiguous:
                log.info(f"  {len(self.ambiguous):,} roster names map to more than "
                         f"one entity id — those get no state_filer_id")
        return self

    def resolve(self, norm_name: str, report_year: str = "") -> dict | None:
        """Roster record for a filer name as it appears in a transaction file.

        1. Exact full-name hit, when that name belongs to exactly one entity.
        2. Otherwise treat it as a base name and use the report year to pick
           the candidacy: prefer an exact year match, else the most recent
           candidacy at or before the report year. Anything still ambiguous
           returns None.
        """
        recs = self._by_full.get(norm_name)
        if recs and norm_name not in self.ambiguous:
            return recs[0]

        pool = self._by_base.get(norm_name) or []
        if not pool:
            return None
        if len({r["entity_id"] for r in pool}) == 1:
            return pool[0]
        if report_year:
            same = [r for r in pool if r["election_year"] == report_year]
            if len({r["entity_id"] for r in same}) == 1:
                return same[0]
            # A committee keeps filing after its election year (year-end
            # reports, debt retirement), so fall back to the most recent
            # candidacy that had already begun by this report year.
            prior = [r for r in pool if r["election_year"]
                     and r["election_year"] <= report_year]
            if prior:
                newest = max(r["election_year"] for r in prior)
                best = [r for r in prior if r["election_year"] == newest]
                if len({r["entity_id"] for r in best}) == 1:
                    return best[0]
            # Nothing at or before the report year. This is common and not an
            # error: the roster only keeps the folders that still exist, so a
            # long-serving legislator whose 2010 and 2012 folders were retired
            # may be left with 2016 and 2020 only — and their 2010 filings
            # then predate every surviving candidacy. Attribute to the
            # earliest one rather than dropping the filer entirely; it is the
            # same person, and the alternative is no state_filer_id at all.
            dated = [r for r in pool if r["election_year"]]
            if dated:
                oldest = min(r["election_year"] for r in dated)
                best = [r for r in dated if r["election_year"] == oldest]
                if len({r["entity_id"] for r in best}) == 1:
                    return best[0]
        return None

    def type_for(self, norm_name: str) -> str:
        """Entity type even when the id is ambiguous, if the colliding
        entities agree on it."""
        recs = (self._by_full.get(norm_name) or []) + (self._by_base.get(norm_name) or [])
        types = {r["type"] for r in recs if r["type"]}
        return types.pop() if len(types) == 1 else ""


# ===================== party/office enrichment overlay ================

_ENRICH_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "ESQ", "MD", "PHD"}
_ENRICH_PUNCT_RE   = re.compile(r"[.,\-–—']")
_ENRICH_NONWORD_RE = re.compile(r"[^A-Z0-9 ]")


def _enrich_tokens(name: str) -> list[str]:
    up = _ENRICH_PUNCT_RE.sub(" ", (name or "").upper())
    up = _ENRICH_NONWORD_RE.sub(" ", up)
    toks = [t for t in up.split() if t]
    while toks and toks[-1] in _ENRICH_SUFFIXES:
        toks.pop()
    return toks


def name_keys(name: str) -> tuple[str, str]:
    """(full_key, first_last_key) — ('', '') for anything under two tokens."""
    toks = _enrich_tokens(name)
    if len(toks) < 2:
        return "", ""
    return " ".join(toks), f"{toks[0]} {toks[-1]}"


# Canonical office labels src/aliases/office_types.csv actually recognizes.
# canonical_office_type() passes an unmapped value straight through, so
# comparing its output alone can't tell "State Senator" (mapped) from
# "Senate" (unmapped passthrough) — this set is what makes that distinction.
_KNOWN_OFFICES = {v for v in office_type_mappings().values() if v}


def _known_office(raw: str) -> str:
    """Canonical office label, or '' if the value isn't one we recognize."""
    canon = canonical_office_type(STATE, clean(raw)) or ""
    return canon if canon in _KNOWN_OFFICES else ""


def office_out(raw: str) -> str:
    """The office string to WRITE into candidates.office.

    Canonicalized rather than raw, which is a deliberate departure from the
    usual "write the source's own value and let office_types.csv normalize at
    aggregate time" rule. Two reasons:

      1. `assign_person_ids(id_model="committee")` groups on
         (state, candidate_name, office, district) using only
         uppercase/whitespace normalization. Utah's own annotation says
         "House" while the party overlay backfills "State Representative" for
         folders Utah never annotated — writing both would split one person
         across two person_ids, which is the exact failure this id model
         exists to prevent.
      2. Utah's "House"/"Senate" are fragments of a display string, not a
         source vocabulary code, so there is nothing lost in normalizing
         them. The literal annotation is still recoverable: committee_name
         keeps the whole folder name, "(2022 House-57)" included.

    An unrecognized office is passed through verbatim rather than dropped.
    """
    canon = _known_office(raw)
    return canon or clean(raw)


class _Candidacy:
    __slots__ = ("party", "office", "district", "year")

    def __init__(self, party, office, district, year):
        self.party    = party
        self.office   = office
        self.district = district
        self.year     = year


class UTEnrichment:
    """Party/office overlay assembled from the optional --party raw files.

    Indexes, all values lists of _Candidacy so a name that turns out to
    carry two different parties can be detected and declined:

        _by_entity_id   entity_id                -> _Candidacy   (tier 1)
        _os_full/_os_fl normalized name key      -> [_Candidacy] (tier 2)
        _cv_full/_cv_fl normalized name key      -> [_Candidacy] (tier 3)
    """

    def __init__(self):
        self._by_entity_id: dict[str, _Candidacy] = {}
        self._os_full: dict[str, list[_Candidacy]] = defaultdict(list)
        self._os_fl:   dict[str, list[_Candidacy]] = defaultdict(list)
        self._cv_full: dict[str, list[_Candidacy]] = defaultdict(list)
        self._cv_fl:   dict[str, list[_Candidacy]] = defaultdict(list)
        self.canvass_years: set[str] = set()
        self.available = False

    # ---- loading ----

    @classmethod
    def load(cls, log) -> "UTEnrichment":
        self = cls()
        self._load_openstates(RAW_DIR / OPENSTATES_FILE, log)
        self._load_canvass(RAW_DIR / CANVASS_FILE, log)
        self.available = bool(self._by_entity_id or self._os_full or self._cv_full)
        return self

    def _load_openstates(self, path: Path, log) -> None:
        # A missing overlay file means "this source has nothing to say",
        # never an error — the pipeline must run without --party ever having
        # been invoked.
        if not path.exists() or path.stat().st_size == 0:
            return
        n = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                name = clean(row.get("name"))
                if not name:
                    continue
                raw_party = clean(row.get("party"))
                if not raw_party:
                    continue
                cand = _Candidacy(
                    party=canonical_party(raw_party),
                    office=clean(row.get("chamber")),
                    district=clean(row.get("district")).lstrip("0")
                             or clean(row.get("district")),
                    year="",
                )
                eid = clean(row.get("entity_id"))
                if eid:
                    self._by_entity_id[eid] = cand
                full, fl = name_keys(name)
                if full:
                    self._os_full[full].append(cand)
                    if fl != full:
                        self._os_fl[fl].append(cand)
                n += 1
        log.registry_loaded(path.name, entries=n, relation="candidates",
                            bytes=path.stat().st_size)

    def _load_canvass(self, path: Path, log) -> None:
        if not path.exists() or path.stat().st_size == 0:
            return
        n = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                name = clean(row.get("candidate_name"))
                raw_party = clean(row.get("party"))
                if not name:
                    continue
                if not raw_party:
                    continue
                year = clean(row.get("election_year"))
                self.canvass_years.add(year)
                cand = _Candidacy(
                    party=canonical_party(raw_party),
                    office=clean(row.get("office")),
                    district=clean(row.get("district")).lstrip("0")
                             or clean(row.get("district")),
                    year=year,
                )
                full, fl = name_keys(name)
                if full:
                    self._cv_full[full].append(cand)
                    if fl != full:
                        self._cv_fl[fl].append(cand)
                n += 1
        log.registry_loaded(path.name, entries=n, relation="candidates",
                            bytes=path.stat().st_size)

    # ---- matching ----

    @staticmethod
    def _match(pool: list[_Candidacy], want_office: str, want_district: str,
               want_year: str, dated: bool) -> tuple[_Candidacy, str] | None:
        """Pick one candidacy from a name-keyed pool, with a confidence.

        Utah's own folder annotation supplies office, district and election
        year, so all three are available to corroborate a name match rather
        than taking the name on trust.

        `dated` says whether the source's own office/district are tied to a
        stated election year, which decides how much weight they can carry:

          * `dated=True` (the election canvasses — every row names its
            year). A candidacy whose district contradicts Utah's is a
            different seat and is discarded. `exact` when district and year
            both agree; `high` otherwise. Note that a *year* mismatch is
            never grounds to discard, unlike a district one: the same person
            legitimately runs in several cycles, so two different years are
            no evidence of two different people — they just mean this pick
            can't be called exact.
          * `dated=False` (Open States — its `current_district` is the seat
            the person holds *today*, with no year attached). Comparing
            that against a 2012 candidacy compares two different things, so
            a mismatch is not evidence of a different person and is not
            grounds to reject. The seat is used only to *raise* confidence,
            never to veto, and the result stays `high`: name agreement plus
            a party is all this source can honestly support.

        Either way the surviving set must agree on a single party. A name
        two parties both claim is declined, not arbitrated.
        """
        if not pool:
            return None

        want_office = _known_office(want_office)
        scoped = []
        for c in pool:
            if dated:
                # Both sides know the district and they differ -> different seat.
                if want_district and c.district and c.district != want_district:
                    continue
                # Both sides know the office and they differ -> different race.
                # `_known_office` returns "" for anything that doesn't resolve
                # to a recognized canonical label, and a blank disables this
                # check rather than failing the match: an office spelling Utah
                # invents tomorrow should cost precision, not silently reject
                # every candidate who holds it.
                other = _known_office(c.office)
                if want_office and other and other != want_office:
                    continue
            scoped.append(c)
        if not scoped or len({c.party for c in scoped}) != 1:
            return None

        d_ok = bool(want_district) and any(c.district == want_district for c in scoped)
        y_ok = bool(want_year) and any(c.year == want_year for c in scoped)
        conf = "exact" if (dated and d_ok and y_ok) else "high"
        # Prefer a candidacy that corroborates on both axes, then one that at
        # least carries an office (so the office/district backfill has
        # something to work with).
        best = next((c for c in scoped
                     if c.district == want_district and c.year == want_year), None)
        return (best or next((c for c in scoped if c.office), scoped[0])), conf

    def lookup(self, name: str, entity_id: str, election_year: str,
               office: str = "", district: str = "") -> dict | None:
        """Party (and, where missing, office/district) for one candidate.

        Returns {"party", "party_source", "match_confidence", "office",
        "district"} or None. `office`/`district`/`election_year` are what
        Utah's own folder annotation gave the candidate row; they are used
        to corroborate, never overwritten.
        """
        # Tier 1 — same disclosures.utah.gov entity id on both sides. This
        # is an identity, not a similarity: no name comparison happens, so
        # nothing about the name or seat can weaken it.
        if entity_id:
            hit = self._by_entity_id.get(entity_id)
            if hit:
                return {"party": hit.party, "party_source": "openstates",
                        "match_confidence": "exact",
                        "office": hit.office, "district": hit.district}

        full, fl = name_keys(name)
        if not full:
            return None
        want_year = clean(election_year)

        for source, dated, indexes in (
            ("openstates",          False, (self._os_full, self._os_fl)),
            ("ut_election_canvass", True,  (self._cv_full, self._cv_fl)),
        ):
            for index, key in zip(indexes, (full, fl)):
                got = self._match(index.get(key, []), office, district,
                                  want_year, dated)
                if got:
                    hit, conf = got
                    return {"party": hit.party, "party_source": source,
                            "match_confidence": conf,
                            "office": hit.office, "district": hit.district}
        return None

    def coverage_report(self) -> dict:
        return {
            "entity_id_joins":  len(self._by_entity_id),
            "openstates_names": len(self._os_full),
            "canvass_names":    len(self._cv_full),
            "canvass_years":    len(self.canvass_years),
        }


# ================================ run ==================================

def run():
    log = get_logger("utah", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    total_malformed     = 0
    total_skipped_files = 0
    total_resolved      = 0
    total_unresolved    = 0
    committees: dict[str, dict] = {}
    candidates: dict[str, dict] = {}
    file_handles: list = []

    try:
        # ---- 0. overlays and roster ----
        roster = Roster.load(log)
        enrich = UTEnrichment.load(log)
        if enrich.available:
            cov = enrich.coverage_report()
            log.info(f"Loaded party overlay — {cov['entity_id_joins']:,} exact "
                     f"entity-id joins, {cov['openstates_names']:,} Open States "
                     f"names, {cov['canvass_names']:,} canvass names across "
                     f"{cov['canvass_years']} election years")
        else:
            log.warning("  No party overlay in data/Utah/raw — candidates will "
                        "have no party/office. Run "
                        "`python3 src/pipeline/scrapers/utah.py --party`.")

        # ---- 1. seed committees/candidates from the roster ----
        # The roster is the authoritative entity list, so it goes first:
        # every registered filer then appears with its id, type, active flag
        # and (for candidates) its office/district/election year, whether or
        # not it ever filed an itemized transaction.
        #
        # One row per entity, not per year. A Utah candidate folder is
        # per-candidacy — the "(2022 House-57)" annotation IS the cycle — and
        # organizations keep a single folder for life, so there is no
        # per-year grain here to preserve.
        n_ambiguous_seeded = 0
        for rec in roster.by_id.values():
            is_cand = rec["type"] in CANDIDATE_TYPES
            # Flip the person's name only, never the candidacy annotation:
            # "Abbott, Nelson (2022 House-57)" -> "NELSON ABBOTT".
            base = rec["entity_name"] if not is_cand else \
                parse_candidacy(rec["entity_name"])["base"]
            flipped = flip_name(base) if is_cand else ""
            first, last = split_person(base) if is_cand else ("", "")
            # Two registered entities can share a name (Utah carries filers
            # registered as literal placeholders, e.g. "doe, john"). Those
            # collapse to a single committee row with NO state_filer_id:
            # guessing which id owns the name would silently merge two real
            # filers under one person_id, and picking neither is the only
            # answer that can't be wrong. Same rule as Roster.resolve(), so
            # the transaction path agrees.
            eid = "" if rec["norm_full"] in roster.ambiguous else rec["entity_id"]
            if not eid:
                n_ambiguous_seeded += 1
            committees.setdefault(rec["norm_full"], {
                "state":           STATE,
                "committee_name":  rec["norm_full"],
                "committee_type":  rec["type"],
                # Sparse by design: a Utah PCC is cycle-specific and carries
                # its year in the folder name; organizations aren't and get
                # a blank, per the committees.election_year contract.
                "election_year":   rec["election_year"],
                "candidate_name":  flipped,
                "active":          rec["active"],
                "state_filer_id":  eid,
                "raw_file":        ENTITIES_FILE,
                "row_num":         rec["row_num"],
            })
            if is_cand and flipped:
                candidates.setdefault(rec["norm_full"], {
                    "state":            STATE,
                    "candidate_name":   flipped,
                    "candidate_first":  first,
                    "candidate_last":   last,
                    "office":           office_out(rec["office"]),
                    "district":         rec["district"],
                    "jurisdiction":     "",
                    "party":            "",
                    "party_source":     "",
                    "match_confidence": "",
                    "election_year":    rec["election_year"],
                    "state_filer_id":   eid,
                    "raw_file":         ENTITIES_FILE,
                    "row_num":          rec["row_num"],
                })
        if roster.available:
            log.info(f"  Seeded {len(committees):,} committees and "
                     f"{len(candidates):,} candidates from the roster")
            if n_ambiguous_seeded:
                log.warning(f"  {n_ambiguous_seeded:,} roster entities share a "
                            f"name with another entity — written without a "
                            f"state_filer_id (and so without a person_id)")

        # ---- 2. transaction writers ----
        # Appended one at a time, not assigned as a list after all three
        # succeed: if the second or third open_writer raises, the earlier
        # handles must already be in the list for `finally` to close them.
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        file_handles.append(cont_fh)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        file_handles.append(expn_fh)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles.append(loan_fh)

        txn_files = raw_files("transactions_*.csv")
        if not txn_files:
            log.warning("  No transactions_*.csv in data/Utah/raw — nothing to "
                        "parse. Run scrapers/utah.py first.")

        for path in txn_files:
            m = _TXN_FILE_RE.match(path.name)
            if not m:
                log.warning(f"  ! skipping unrecognized raw file {path.name}")
                total_skipped_files += 1
                continue
            slug, year = m.group(1), m.group(2)
            etype = SLUG_TO_TYPE.get(slug, "")
            is_cand_type = etype in CANDIDATE_TYPES

            ft = time.perf_counter()
            n_cont = n_expn = n_loan = n_bad = 0
            n_resolved = n_unresolved = 0
            reader = csv.reader(_repaired_lines(path))
            try:
                header = next(reader)
            except StopIteration:
                log.warning(f"  ! {path.name} is empty")
                total_skipped_files += 1
                continue
            idx = _header_map(header)
            if idx is None:
                log.file_parse_error(path.name,
                                     error=f"unrecognized header: {header[:6]}")
                total_skipped_files += 1
                continue
            width = len(header)

            for row_num, row in enumerate(reader, start=2):
                if not row or not any(clean(c) for c in row):
                    continue
                # A row that doesn't line up with the header is a casualty of
                # the quoting defect (see _repair_line) — skip rather than
                # shift every field one column left.
                if len(row) != width:
                    n_bad += 1
                    continue

                entity = utils.clean_name(_get(row, idx, "ENTITY_NAME"))
                if not entity:
                    n_bad += 1
                    continue

                amount = parse_amount(_get(row, idx, "TRAN_AMT"))
                tdate  = parse_date(_get(row, idx, "TRAN_DATE"))
                ttype  = _get(row, idx, "TRAN_TYPE")
                inkind = flag(_get(row, idx, "INKIND"))
                is_loan = flag(_get(row, idx, "LOAN"))
                amends = _get(row, idx, "AMENDS")
                counterparty = _get(row, idx, "NAME")
                purpose = _get(row, idx, "PURPOSE")

                # --- resolve the filer through the roster ---
                # The export has been seen writing a candidate's bare
                # personal name where the roster carries the annotated folder
                # name, so both spellings are funnelled onto the roster's
                # canonical name here. Without this, "ELIASON, STEVEN" and
                # "ELIASON, STEVEN (2018 HOUSE-45)" would become two
                # committees, and half this filer's transactions would fail
                # to join to either.
                rec = roster.resolve(entity, year)
                if rec is not None:
                    ckey = rec["norm_full"]
                    n_resolved += 1
                else:
                    ckey = entity
                    n_unresolved += 1

                cm = committees.get(ckey)
                if cm is None:
                    # Filer absent from the roster entirely (a roster sweep
                    # that hasn't run, or an entity dropped from the grid).
                    # Type falls back to the file's own entity type, which is
                    # reliable — it came from the request that produced it.
                    # Split the annotation off the name as the file spells it,
                    # not off `entity` (already uppercased by clean_name) —
                    # otherwise these rows get office="HOUSE" where
                    # roster-seeded rows get "House".
                    raw_entity = _get(row, idx, "ENTITY_NAME")
                    ann = parse_candidacy(raw_entity) if is_cand_type else \
                        {"base": raw_entity, "election_year": "",
                         "office": "", "district": ""}
                    cm = {
                        "state":          STATE,
                        "committee_name": ckey,
                        "committee_type": roster.type_for(entity) or etype,
                        "election_year":  ann["election_year"],
                        "candidate_name": flip_name(ann["base"]) if is_cand_type else "",
                        "active":         "",
                        "state_filer_id": "",
                        "raw_file":       path.name,
                        "row_num":        row_num,
                    }
                    committees[ckey] = cm
                    if is_cand_type and cm["candidate_name"]:
                        first, last = split_person(ann["base"])
                        candidates.setdefault(ckey, {
                            "state":            STATE,
                            "candidate_name":   cm["candidate_name"],
                            "candidate_first":  first,
                            "candidate_last":   last,
                            "office":           office_out(ann["office"]),
                            "district":         ann["district"],
                            "jurisdiction":     "",
                            "party":            "",
                            "party_source":     "",
                            "match_confidence": "",
                            "election_year":    ann["election_year"],
                            "state_filer_id":   "",
                            "raw_file":         path.name,
                            "row_num":          row_num,
                        })

                cand_name = cm["candidate_name"] if is_cand_type else ""
                # A candidate's transactions belong to the election cycle
                # their folder names, which is not always the calendar year
                # the report was filed in (year-end reports, debt retirement).
                # Non-candidate filers have no cycle, so the report year is
                # the best available answer.
                txn_year = cm["election_year"] or year

                base = {
                    "state":          STATE,
                    "committee_name": cm["committee_name"],
                    "amount":         amount,
                    "date":           tdate,
                    "candidate_name": cand_name,
                    "election_year":  txn_year,
                    "amended":        "1" if amends else "0",
                    "filing_id":      _get(row, idx, "TRAN_ID"),
                    "raw_file":       path.name,
                    "row_num":        row_num,
                }

                # --- routing ---
                if is_loan:
                    # Loans are diverted here rather than double-counted as a
                    # contribution/expenditure. Direction comes from
                    # TRAN_TYPE: money in is a loan received, money out a
                    # loan made.
                    loan_w.writerow({
                        # `base` carries `amount`, which LOANS_DEBTS has no
                        # column for; extrasaction="ignore" drops it, and
                        # original_amount below is set from the same value.
                        **base,
                        "original_amount":    amount,
                        "record_type":        "Loan Received"
                                              if ttype.lower().startswith("contrib")
                                              else "Loan Made",
                        "counterparty_name":  counterparty,
                        "counterparty_city":  _get(row, idx, "CITY"),
                        "counterparty_state": clean_state(_get(row, idx, "STATE")),
                        "counterparty_zip":   utils.clean_zip(_get(row, idx, "ZIP")),
                    })
                    n_loan += 1
                elif ttype.lower().startswith("contrib"):
                    cont_w.writerow({
                        **base,
                        "transaction_type":  "In-Kind Contribution" if inkind
                                             else "Contribution",
                        "contributor_name":  counterparty,
                        # Left blank on purpose: Utah publishes no contributor
                        # category, and aggregate.py already fills this in for
                        # any contributor whose name matches a registered
                        # committee. See the module docstring.
                        "contributor_type":  "",
                        "contributor_city":  _get(row, idx, "CITY"),
                        "contributor_state": clean_state(_get(row, idx, "STATE")),
                        "contributor_zip":   utils.clean_zip(_get(row, idx, "ZIP")),
                        "office":            "",
                    })
                    n_cont += 1
                elif ttype.lower().startswith("expend"):
                    # A derived label, NOT the raw PURPOSE — see
                    # expenditure_type() for why free text can't drive
                    # transaction_category. The raw string is preserved in
                    # `category` on the same row.
                    expn_w.writerow({
                        **base,
                        "transaction_type": expenditure_type(purpose, inkind),
                        "payee_name":  counterparty,
                        # Utah's statutory expenditure category lives in
                        # PURPOSE; the free-text in-kind description is the
                        # closest thing to a purpose narrative.
                        "category":    purpose,
                        "purpose":     _get(row, idx, "INKIND_COMMENTS"),
                        "payee_city":  _get(row, idx, "CITY"),
                        "payee_state": clean_state(_get(row, idx, "STATE")),
                        "payee_zip":   utils.clean_zip(_get(row, idx, "ZIP")),
                        "office":      "",
                    })
                    n_expn += 1
                else:
                    # Unknown TRAN_TYPE — count it rather than guessing a table.
                    n_bad += 1

            total_contributions += n_cont
            total_expenditures  += n_expn
            total_loans         += n_loan
            total_malformed     += n_bad
            total_resolved      += n_resolved
            total_unresolved    += n_unresolved
            log.file_parsed(path.name, "transactions", n_cont + n_expn + n_loan,
                            skipped=n_bad,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)

        # ---- 3. party/office enrichment ----
        n_party = 0
        n_office = 0
        conf_counts: dict[str, int] = {}
        source_counts: dict[str, int] = {}
        if enrich.available:
            for cand in candidates.values():
                if cand.get("party"):
                    continue                        # Utah's own data always wins
                hit = enrich.lookup(cand["candidate_name"],
                                    cand.get("state_filer_id", ""),
                                    cand.get("election_year", ""),
                                    cand.get("office", ""),
                                    cand.get("district", ""))
                if not hit:
                    continue
                cand["party"]            = hit["party"]
                cand["party_source"]     = hit["party_source"]
                cand["match_confidence"] = hit["match_confidence"]
                n_party += 1
                conf_counts[hit["match_confidence"]] = \
                    conf_counts.get(hit["match_confidence"], 0) + 1
                source_counts[hit["party_source"]] = \
                    source_counts.get(hit["party_source"], 0) + 1
                # Utah supplies neither office nor district, so anything the
                # overlay knows is strictly additive. Never overwrites.
                if hit.get("office") and not cand.get("office"):
                    cand["office"] = office_out(hit["office"])
                    n_office += 1
                if hit.get("district") and not cand.get("district"):
                    cand["district"] = hit["district"]

            log.info(f"  Party enrichment: {n_party:,} of {len(candidates):,} "
                     f"candidates filled "
                     f"(exact {conf_counts.get('exact', 0):,} / "
                     f"high {conf_counts.get('high', 0):,}) — by source: "
                     f"{dict(sorted(source_counts.items(), key=lambda kv: -kv[1]))}")
            if n_office:
                log.info(f"  Office backfilled on {n_office:,} candidates that "
                         f"had no '(YYYY Office-District)' annotation")
            log.enrichment_summary(
                relation="candidates", matched=n_party, total=len(candidates),
                field="party",
                method="entity-id join (Open States links) -> Open States name "
                       "-> UT election canvass name; single-party agreement "
                       "required, no fuzzy matching")

        # ---- 4. flush entities ----
        cand_fh, cand_w = open_writer("candidates.csv.gz", C.CANDIDATES)
        file_handles.append(cand_fh)
        cmte_fh, cmte_w = open_writer("committees.csv.gz", C.COMMITTEES)
        file_handles.append(cmte_fh)
        for row in sorted(candidates.values(),
                          key=lambda r: (r["candidate_name"], r["election_year"])):
            cand_w.writerow(row)
        for row in sorted(committees.values(),
                          key=lambda r: (r["committee_name"], r["election_year"])):
            cmte_w.writerow(row)

        for fh in file_handles:
            fh.close()
        file_handles = []

        # ---- 5. person ids ----
        # id_model="committee": a Utah PCC folder is per-CANDIDACY, not
        # per-person — "Aagard, Doug (2008 House-15)" and a later
        # "Aagard, Doug (2012 House-15)" are two folders with two ids for one
        # man. So state_filer_id is a registration id, exactly the case the
        # "committee" model exists for: it groups on
        # (state, candidate_name, office, district) and assigns
        # person_id = min(state_filer_id) across the group, which is why the
        # office/district read off the folder annotation matter beyond
        # reporting. Two people who share a name but ran for different seats
        # stay separate; the same person's runs for the same seat merge.
        # Candidates with no roster match have a blank state_filer_id and get
        # no person_id — the honest outcome, since there is nothing to key on.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name: str) -> int:
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        for fname, relation, count in (
            ("contributions.csv.gz", "contributions", total_contributions),
            ("expenditures.csv.gz",  "expenditures",  total_expenditures),
            ("loans_debts.csv.gz",   "loans_debts",   total_loans),
            ("candidates.csv.gz",    "candidates",    len(candidates)),
            ("committees.csv.gz",    "committees",    len(committees)),
        ):
            log.file_parsed(fname, relation, count, role="output",
                            bytes=_out_bytes(fname))

        duration = round(time.perf_counter() - t0, 1)
        if total_malformed:
            log.warning(f"  {total_malformed:,} rows skipped (field-count "
                        f"mismatch after quote repair, or unknown TRAN_TYPE)")
        if total_resolved or total_unresolved:
            pct = 100.0 * total_resolved / max(total_resolved + total_unresolved, 1)
            log.info(f"  Filer resolution: {total_resolved:,} of "
                     f"{total_resolved + total_unresolved:,} transaction rows "
                     f"({pct:.1f}%) matched a roster entity; the rest keep "
                     f"their raw filer name and no state_filer_id")
            # ~97.8% is the measured ceiling with a complete roster: the
            # per-year exports still contain filers the live search grid has
            # since purged, so some rows can never take an id (see the "utah"
            # entry in validate.py's TIER1_OPTIONAL_BY_STATE). Warn only well
            # below that, where the cause really is a stale or partial roster
            # rather than the source's own shape.
            if pct < 95.0:
                log.warning(f"  ! only {pct:.1f}% of transaction rows resolved to "
                            f"a roster entity — well below the ~98% ceiling a "
                            f"complete roster reaches, so it is probably stale "
                            f"or partial; re-run `scrapers/utah.py --entities`")
            else:
                log.info(f"  ({100-pct:.1f}% unresolved is expected — those "
                         f"filers no longer appear in the search grid)")
        log.info(f"Done in {duration}s — {total_contributions:,} contributions, "
                 f"{total_expenditures:,} expenditures, {total_loans:,} loans, "
                 f"{len(committees):,} committees, "
                 f"{len(candidates):,} candidates")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions,
                  expenditures=total_expenditures, loans_debts=total_loans,
                  committees=len(committees), candidates=len(candidates),
                  malformed=total_malformed, skipped_files=total_skipped_files,
                  filers_resolved=total_resolved,
                  filers_unresolved=total_unresolved, party_filled=n_party)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates))
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  committees=len(committees), candidates=len(candidates),
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


# ================================ CLI ==================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
