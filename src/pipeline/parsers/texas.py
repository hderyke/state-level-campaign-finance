"""
parsers/texas.py — Parse the Texas Ethics Commission bulk CSV export into the
canonical cleaned schema.

Raw files (data/Texas/raw/, written by scrapers/texas.py):

  filers.csv        -> candidates + committees   (the filer index, 20,748 rows)
  cover.csv         -> candidate enrichment      (party, election date, office)
  spacs.csv         -> committee -> candidate links for specific-purpose PACs
  contribs_##.csv   -> contributions             (102 shards, ~34M rows)
  credits.csv       -> contributions             (Schedule K interest/credits/gains)
  expend_##.csv     -> expenditures              (13 shards)
  cand.csv          -> expenditure enrichment    (direct-campaign-expenditure beneficiaries)
  loans.csv         -> loans_debts               (Schedule E)
  debts.csv         -> loans_debts               (Schedule L outstanding loans)
  expn_catg.csv     -> expenditure category code -> label lookup

Every TEC record shares a nine-field prefix (recordType, formTypeCd,
schedFormTypeCd, reportInfoIdent, receivedDt, infoOnlyFlag, filerIdent,
filerTypeCd, filerName), which is what makes one set of helpers work across
all of them.

Superseded rows
---------------
`infoOnlyFlag = Y` means "superseded by another report" — the row was filed,
then re-filed on a corrected report that is *also* in the archive. Roughly 2%
of contributions and 8% of expenditures carry it. They are dropped, counted,
and reported in the parse log as `skipped_superseded`; keeping them would
double-count every corrected filing.

Special-session / Telegram files
--------------------------------
`cont_ss.csv`, `cont_t.csv` and `expn_t.csv` are not read (and not even
extracted — see scrapers/texas.py). TEC's own README says those rows are
re-reported on the next regular campaign finance report and keeps them in
separate files so consumers don't double-count.

Names
-----
TEC stores every party to a transaction twice over: a display string
("Lucero, Homero R. (Mr.)") and structured components
(`*NameOrganization` for entities, `*NameFirst`/`*NameLast`/`*NameSuffixCd`
for individuals). `tec_name()` prefers the components and falls back to
un-inverting the display string, so a person is always written as
"HOMERO R. LUCERO" whether the row that produced them was structured or not.
This matters because `utils.assign_committee_person_ids()` matches committees
to candidates on the name string and does not handle comma inversion.

Filer identity
--------------
`filerIdent` is TEC's filer account number and is already person-level: it is
issued once per filer and follows them across cycles, so `id_model="person"`
derives `person_id` from it directly.

The `"committee"` model was tried first and measured against the real file: of
10,911 candidate accounts carrying 10,776 distinct names, only 28 names hold
more than one account, and grouping by (name, office, district) merged just 10
groups — most of them TEC's own placeholder records ("DO NOT USE" x88), i.e.
88 unrelated junk accounts collapsing onto one person. The grouping cost more
than it bought, so it isn't used. Genuine duplicate accounts for one person do
exist at that ~0.3% rate and remain separate people in the output; that's the
documented trade.

Those placeholder accounts are filtered out entirely — see PLACEHOLDER_NAME_RE.

Output (data/Texas/cleaned/):
  contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
  committees.csv.gz, candidates.csv.gz
"""

import csv
import gzip
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger
from src.aliases import canonical_office_type, canonical_party

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Texas" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Texas" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "TX"
EARLIEST_YEAR  = 1990                   # matches validate.py's plausibility floor
MAX_VALID_YEAR = date.today().year + 4  # matches validate.py's ceiling

# ============================= Filer types =============================
# filerTypeCd -> human label, from CFS-Codes.txt's form-code table. TEC uses
# the same vocabulary for filerTypeCd as for formTypeCd, minus the correction
# and final-report variants.
FILER_TYPES = {
    "COH":      "Candidate/Officeholder",
    "JCOH":     "Judicial Candidate/Officeholder",
    "SCC":      "State/County Chair Candidate/Officeholder",
    "SPK":      "Candidate for Speaker",
    "GPAC":     "General-Purpose Committee",
    "MPAC":     "Monthly Filing General-Purpose Committee",
    "SPAC":     "Specific-Purpose Committee",
    "JSPC":     "Judicial Specific-Purpose Committee",
    "SCPC":     "State/County Specific-Purpose Committee",
    "ASIFSPAC": "As-If Specific-Purpose Committee",
    "CEC":      "County Executive Committee",
    "MCEC":     "Monthly Filing County Executive Committee",
    "PTYCORP":  "Political Party (Corporate/Labor Funds)",
    "LEG":      "Legislative Caucus",
    "DCE":      "Direct Campaign Expenditure Filer",
}

# Filer types that describe a person running for or holding office. Everything
# else is an organization and gets a committees row only.
#
# SPK (candidate for Speaker of the Texas House) is included: they are
# individuals seeking an office, even though the "electorate" is the House
# membership rather than the public.
CANDIDATE_FILER_TYPES = {"COH", "JCOH", "SCC", "SPK"}

# ============================== Schedules ==============================
# schedFormTypeCd -> transaction_type label, from CFS-Codes.txt's schedule
# table. Bounded and stable, which is what makes it usable as the alias key in
# src/aliases/transaction_categories.csv and expenditure_categories.csv.
CONTRIB_SCHEDULES = {
    "A1":   "Monetary Political Contribution",
    "A2":   "In-Kind Political Contribution",
    "A2SS": "In-Kind Political Contribution (Special Session)",
    "AJ1":  "Monetary Political Contribution (Judicial)",
    "AL":   "Non-Caucus Member Contribution",
    "AS1":  "Monetary Contribution (Speaker)",
    "AS2":  "In-Kind Contribution (Speaker)",
    "C1":   "Monetary Contribution from Corporation or Labor Organization",
    "C2":   "In-Kind Contribution from Corporation or Labor Organization",
    "C3":   "Monetary Support from Corporation or Labor Organization",
    "C4":   "Non-Monetary Support from Corporation or Labor Organization",
    "K":    "Interest, Credit, Gain or Returned Contribution",
}

EXPEND_SCHEDULES = {
    "F1":      "Political Expenditure from Political Contributions",
    "F2":      "Unpaid Incurred Obligation",
    "F3":      "Purchase of Investments from Political Contributions",
    "F4":      "Expenditure Made by Credit Card",
    "FL":      "Legislative Caucus Expenditure",
    "FS":      "Expenditure (Speaker)",
    "G":       "Political Expenditure from Personal Funds",
    "H":       "Payment to the Business of a Candidate/Officeholder",
    "I":       "Non-Political Expenditure from Political Contributions",
    "COHUC2":  "Unexpended Contributions Disbursement",
    "SPKUCFS": "Unexpended Contributions Disbursement (Speaker)",
}

LOAN_SCHEDULES = {
    "E":  "Loan",
    "EJ": "Loan (Judicial)",
    "EL": "Loan to Legislative Caucus",
    "ES": "Loan (Speaker)",
    "L":  "Outstanding Loan",
}

# Only SUPPORT links a specific-purpose committee to a candidate as *their*
# committee. An OPPOSE SPAC exists to campaign against the named candidate, so
# attributing its money to them would be actively wrong; ASSIST/UNKNOWN are
# too vague to rely on. Counts in the live file: SUPPORT 456, OPPOSE 35,
# ASSIST 9, UNKNOWN 3.
SPAC_LINK_POSITIONS = {"SUPPORT"}

# Trailing "(Mr.)" / "(Mrs.)" / "(Dr.)" etc. that TEC appends to display names.
_NAME_TITLE_RE = re.compile(r"\s*\([^)]*\)\s*$")

# TEC's filer index contains reserved/void account numbers whose "name" says so
# outright — "DO NOT USE" (x26), "DO NOT USE, TEC" (x11), "TEC Filer ID Not To
# Be Used" and friends, 88 rows in all. They carry no transactions and are not
# people or committees; left in, they'd become the single largest "candidate"
# in Texas by account count.
PLACEHOLDER_NAME_RE = re.compile(r"do\s*not\s*use|not\s+to\s+be\s+used", re.IGNORECASE)


# ============================== Helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Parse a TEC amount to a plain numeric string. Returns '' on failure.

    TEC writes amounts as unformatted decimals ("1500.00"), but the paren and
    currency handling is kept for robustness."""
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
    """TEC's yyyyMMdd -> 'YYYY-MM-DD'. Returns '' on failure or implausible year.

    Every date column in the archive uses this one format (the record layout
    declares mask yyyyMMdd throughout), so there's no format list to try."""
    v = (val or "").strip()
    if not re.fullmatch(r"\d{8}", v):
        return ""
    try:
        d = datetime(int(v[0:4]), int(v[4:6]), int(v[6:8]))
    except ValueError:
        return ""
    if d.year < EARLIEST_YEAR or d.year > MAX_VALID_YEAR:
        return ""
    return d.strftime("%Y-%m-%d")


def year_of(iso_date: str) -> str:
    """'2024-03-01' -> '2024'; '' -> ''."""
    return iso_date[:4] if iso_date else ""


def undisplay(name: str) -> str:
    """Un-invert a TEC display name: 'Lucero, Homero R. (Mr.)' -> 'HOMERO R. LUCERO'.

    Only used as the fallback when the structured name components are empty.
    Organization names contain commas too ("Smith, Jones & Co"), so inversion
    is skipped when the string carries a corporate marker — the same guard
    parsers/tennessee.py uses, for the same reason."""
    n = _NAME_TITLE_RE.sub("", clean(name))
    if "," not in n:
        return utils.clean_name(n)
    if _ORG_MARKER_RE.search(n):
        return utils.clean_name(n)
    last, _, first = n.partition(",")
    first, last = first.strip(), last.strip()
    if not first or len(first.split()) > 4:
        return utils.clean_name(n)
    return utils.clean_name(f"{first} {last}")


_ORG_MARKER_RE = re.compile(
    r"(?:^|\W)(?:llc|l\.l\.c|llp|lp|inc|corp|corporation|co|company|pac|"
    r"committee|fund|association|assoc|union|partners|partnership|group|"
    r"trust|foundation|society|institute|council|political|&)(?:\W|$)",
    re.IGNORECASE,
)


def tec_name(row: dict, prefix: str, display: str = "") -> str:
    """Build a normalized name from TEC's structured name components.

    Every party to a TEC record — filer, contributor, payee, lender, candidate,
    treasurer — is stored the same way: `{prefix}PersentTypeCd` says INDIVIDUAL
    or ENTITY, `{prefix}NameOrganization` holds the org name, and
    `{prefix}NameFirst`/`NameLast`/`NameSuffixCd` hold the person's. So one
    helper covers all of them.

    `display` is the pre-formatted fallback (e.g. `filerName`) used when the
    components are empty — which happens on older records."""
    org = clean(row.get(f"{prefix}NameOrganization"))
    if org:
        return utils.clean_name(org)

    first  = clean(row.get(f"{prefix}NameFirst"))
    last   = clean(row.get(f"{prefix}NameLast"))
    suffix = clean(row.get(f"{prefix}NameSuffixCd"))
    if first or last:
        return utils.clean_name(" ".join(p for p in (first, last, suffix) if p))

    return undisplay(display) if display else ""


def split_person(row: dict, prefix: str, display: str = "") -> tuple[str, str]:
    """(first_middle, last) for an individual, or ('', '') for an organization."""
    if clean(row.get(f"{prefix}NameOrganization")):
        return "", ""
    first = clean(row.get(f"{prefix}NameFirst"))
    last  = clean(row.get(f"{prefix}NameLast"))
    if first or last:
        return utils.clean_name(first), utils.clean_name(last)
    # Fall back to splitting the display string on its comma.
    n = _NAME_TITLE_RE.sub("", clean(display))
    if "," in n and not _ORG_MARKER_RE.search(n):
        last, _, first = n.partition(",")
        return utils.clean_name(first), utils.clean_name(last)
    return "", ""


def office_of(row: dict) -> tuple[str, str, str]:
    """(office, district, jurisdiction) for a filer, from the most specific source.

    TEC records office in three places, with very different coverage in the
    live file: `contestSeekOffice*` — the office on the ballot application
    (3,878 filers) — then `ctaSeekOffice*` — the office named on the campaign
    treasurer appointment (8,788, the best-populated) — then
    `filerHoldOffice*` — the office currently held (2,997). Sought beats held,
    and the ballot application beats the treasurer appointment when both exist.

    All three carry a county alongside, which is the only jurisdiction-like
    field TEC publishes with any coverage — the filer's *street* county
    (`filerStreetCountyCd`) is 0% populated across all 20,748 rows. County is
    read from the same prefix the office came from, so office and jurisdiction
    always describe the same registration; it stays blank for the ~93% of
    filers seeking statewide or legislative office, which is correct."""
    for pfx in ("contestSeekOffice", "ctaSeekOffice", "filerHoldOffice"):
        office = clean(row.get(f"{pfx}Cd"))
        if office:
            county = (clean(row.get(f"{pfx}CountyDescr"))
                      or clean(row.get(f"{pfx}CountyCd")))
            return office, district_of(row, pfx), county
    return "", "", ""


def district_of(row: dict, pfx: str) -> str:
    """District string for an office, folding in Texas's "Place" seat number.

    Texas identifies appellate and many judicial seats by Place rather than by
    district — 176 filers on the winning prefix have a Place and no District.
    Dropping it would make two different seats on the same court indistinguishable
    in `candidates`, which matters because office+district is part of how
    candidates are keyed downstream."""
    district = clean(row.get(f"{pfx}District"))
    place    = clean(row.get(f"{pfx}Place"))
    if district and place:
        return f"{district} Place {place}"
    if place:
        return f"Place {place}"
    return district


def raw_files(pattern: str) -> list[Path]:
    """Non-empty raw files matching a glob, sorted by name."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


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


def is_superseded(row: dict) -> bool:
    """True when TEC has marked this record as replaced by a later report."""
    return clean(row.get("infoOnlyFlag")).upper() == "Y"


# ==================== Party/office enrichment overlay ====================
# Fill `candidates.party` for Texas filers that TEC's own cover.csv leaves
# blank, from the two fallback sources fetched by this file's --party CLI
# mode (see the bottom of scrapers/texas.py).
#
# Inputs (both optional, written by scrapers/texas.py --party):
#     data/Texas/raw/SOS_RaceSummary.csv
#     data/Texas/raw/OpenStates_People.csv
#
# If neither exists this section is inert: every lookup returns None and the
# parser writes exactly the blank `party` it writes without it. Enrichment
# must never be load-bearing for a state's parse to succeed.
#
# A third source, The Green Papers, was tried in scrapers/texas.py's --party
# mode and removed there — its live markup never settled into a stable,
# parseable contract. This section no longer loads or matches against it.
#
# Priority order
# ---------------
# Unlike New York's enrichment (which treats NYSBOE results and Open States
# as two independent corroborating reads on one fact, and blanks a value
# they disagree on), Texas's two sources are tried in a fixed priority order
# and the first one that produces a strict match wins — there is no
# cross-source corroboration step, because the sources don't overlap much in
# what they cover (SOS has State House/Senate + statewide through 2019; Open
# States has only *current* House/Senate) and forcing them to agree would
# mostly just throw away single-source answers that are individually solid:
#
#     1. tx_sos_results   Texas SOS legacy canvass, 1992-2019 (candidates AND
#                         officeholders, winners and losers alike)
#     2. openstates       Open States bulk CSV, current TX Legislature only
#
# Matching contract
# ------------------
# Strict only, same bar as New York's: name and canonical office must agree,
# plus corroboration from district and/or election year appropriate to that
# source (see each `_match_*` method). No nickname expansion, no soundex, no
# edit distance — a wrong party attached to a real person is a worse outcome
# than a blank one.
#
#     party_source       "tx_sos_results" | "openstates"
#     match_confidence    "exact" | "high"
#
#     exact  name + office + district + year all agree (or, for a statewide
#            office with no district, name + office + year)
#     high   name + office agree and one corroborator (district or year) is
#            present and agrees while the other is simply absent, never
#            disagreeing
#
# Same "never disagreeing" bar applies to Open States even though it has no
# election year to check: a district it names for this name+office that does
# not match the district TEC's filing names counts as disagreement (a
# different seat, or a different person), and a name+office pair that
# currently resolves to more than one district with nothing to pick between
# them is an unresolved ambiguity, not a "high" confidence guess. Both cases
# return no match rather than falling back to an arbitrary same-named
# officeholder.
#
# Coverage ceiling
# -----------------
# Between the two sources this reaches statewide executive offices, State
# Senate/House, State Board of Education and the appellate judiciary (Supreme
# Court / Court of Criminal Appeals / Courts of Appeals) — the offices
# significant enough to be canvassed at the state level or tracked by a
# national legislative database. District Court, County Court, District
# Attorney and other county-certified offices (the bulk of TEC's `JUDGEDIST`,
# `JUDGESTATCO`, `DISTATTY` filer types) are outside both sources and stay
# blank no matter how good the matcher is — `coverage_report()` splits this
# out so a low fill rate reads as a source ceiling, not a broken matcher.

# ---- name canonicalisation ----
_ENRICH_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "ESQ", "MD", "PHD"}
_ENRICH_PUNCT_RE   = re.compile(r"[.,\-–—']")
_ENRICH_NONWORD_RE = re.compile(r"[^A-Z0-9 ]")
_ENRICH_INCUMBENT_MARK_RE = re.compile(r"\(I\)\s*$")


def _enrich_tokens(name: str) -> list[str]:
    up = _ENRICH_INCUMBENT_MARK_RE.sub("", (name or "").upper())
    up = _ENRICH_PUNCT_RE.sub(" ", up)
    up = _ENRICH_NONWORD_RE.sub(" ", up)
    toks = [t for t in up.split() if t]
    while toks and toks[-1] in _ENRICH_SUFFIXES:
        toks.pop()
    return toks


def name_keys(name: str) -> tuple[str, str]:
    """(full_key, first_last_key) — see parsers/new_york.py's name_keys for
    the rationale; identical approach, reused rather than re-derived because
    both solve the same "two disclosure systems spell names differently"
    problem the same way."""
    toks = _enrich_tokens(name)
    if len(toks) < 2:
        return "", ""
    return " ".join(toks), f"{toks[0]} {toks[-1]}"


# ---- office canonicalisation ----
# TEC's own candidates.office holds its raw filing code ("STATEREP",
# "GOVERNOR", "JUDGEDIST", ...); src/aliases/office_types.csv already maps
# every one of those codes to a human label for TX. Reusing that mapping here
# — rather than inventing a second office vocabulary the way NY's enrichment
# does — is what keeps this module's labels and the aggregate database's
# `canonical_office` column saying the same thing for the same seat.
def canonical_tec_office(raw_office_code: str) -> str:
    return (canonical_office_type("TX", raw_office_code) or "").strip()


# Statewide offices have no district, so a district comparison is meaningless
# and the election year alone carries the "exact" match.
_ENRICH_STATEWIDE = {
    "Governor", "Lt. Governor", "Attorney General", "State Comptroller",
    "Commissioner of Public Lands", "Commissioner of Agriculture",
    "Public Utility Commissioner",
}


class Candidacy:
    """One person's appearance on one race's ballot line, from source 1 (SOS)."""

    __slots__ = ("name", "office", "district", "year", "party", "general")

    def __init__(self, name, office, district, year, party):
        self.name     = name
        self.office   = office
        self.district = district
        self.year     = year
        self.party    = party
        self.general  = False


class TXEnrichment:
    """Lookup tables built from the two fallback files.

    Usage from the parser::

        enrich = TXEnrichment.load(RAW_DIR)
        if enrich.available:
            hit = enrich.lookup(name, office_code, district, election_year)
    """

    def __init__(self):
        # (name_key, office) -> [Candidacy], newest general-election first
        self._sos_full: dict[tuple[str, str], list[Candidacy]] = defaultdict(list)
        self._sos_fl:   dict[tuple[str, str], list[Candidacy]] = defaultdict(list)
        self.sos_offices: set[str] = set()

        # (name_key, office, district) -> party  (current only)
        self._openstates: dict[tuple[str, str, str], str] = {}
        self._openstates_fl: dict[tuple[str, str, str], str] = {}

        self.known_names: set[str] = set()
        self.available = False

    # ------------------------------------------------------------------ load
    @classmethod
    def load(cls, raw_dir: Path) -> "TXEnrichment":
        self = cls()
        self._load_sos(raw_dir / "SOS_RaceSummary.csv")
        self._load_openstates(raw_dir / "OpenStates_People.csv")
        self.available = bool(self._sos_full or self._openstates)
        return self

    def _load_sos(self, path: Path) -> None:
        if not path.exists() or path.stat().st_size == 0:
            return
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                name = (row.get("candidate_name") or "").strip()
                full, fl = name_keys(name)
                if not full:
                    continue
                office = (row.get("office") or "").strip()
                if not office:
                    self.known_names.add(full)
                    continue
                district = (row.get("district") or "").strip()
                year     = (row.get("election_year") or "").strip()
                party    = canonical_party((row.get("party") or "").strip()) \
                          if row.get("party") else ""
                if not party:
                    continue
                cand = Candidacy(name, office, district, year, party)
                cand.general = (row.get("stage") or "") == "general"
                self._sos_full[(full, office)].append(cand)
                if fl != full:
                    self._sos_fl[(fl, office)].append(cand)
                self.sos_offices.add(office)
                self.known_names.add(full)

        for bucket in (self._sos_full, self._sos_fl):
            for lst in bucket.values():
                lst.sort(key=lambda c: (c.year or "0000", c.general), reverse=True)

    def _load_openstates(self, path: Path) -> None:
        if not path.exists() or path.stat().st_size == 0:
            return
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                name = (row.get("name") or "").strip()
                full, fl = name_keys(name)
                if not full:
                    continue
                self.known_names.add(full)
                office = (row.get("chamber") or "").strip()
                party  = canonical_party((row.get("party") or "").strip()) \
                        if row.get("party") else ""
                district = (row.get("district") or "").strip().lstrip("0")
                if not (office and party):
                    continue
                self._openstates[(full, office, district)] = party
                self._openstates_fl[(fl, office, district)] = party

    # ---------------------------------------------------------------- lookup
    def lookup(self, name: str, office_code: str, district: str,
               election_year: str) -> dict | None:
        """Strict match, tried in source-priority order. Returns None when
        nothing qualifies.

        On success returns {"party", "party_source", "match_confidence"}.
        District/election_year are never backfilled from these sources (all
        three lag or omit them relative to TEC's own filer index), only party.
        """
        office = canonical_tec_office(office_code)
        if not office:
            return None
        full, fl = name_keys(name)
        if not full:
            return None
        want_district = (district or "").strip().lstrip("0")
        want_year     = (election_year or "").strip()

        hit = self._match_sos(full, fl, office, want_district, want_year)
        if hit:
            return hit
        return self._match_openstates(full, fl, office, want_district)

    def _match_sos(self, full, fl, office, want_district, want_year):
        pool = self._sos_full.get((full, office)) or self._sos_fl.get((fl, office))
        if not pool:
            return None
        statewide = office in _ENRICH_STATEWIDE
        for cand in pool:
            both_d = bool(want_district) and bool(cand.district)
            d_ok = both_d and cand.district == want_district
            y_ok = bool(want_year) and cand.year == want_year
            if both_d and not d_ok:
                continue    # same name+office, different district — a different seat
            if statewide:
                conf = "exact" if y_ok else ("high" if not want_year else None)
            elif d_ok and y_ok:
                conf = "exact"
            elif d_ok or y_ok:
                conf = "high"
            else:
                conf = None
            if conf:
                return {"party": cand.party, "party_source": "tx_sos_results",
                        "match_confidence": conf}
        return None

    def _match_openstates(self, full, fl, office, want_district):
        """Open States has no election year to corroborate with, only
        district — so district is the whole ballgame here, and it has to be
        treated the same way SOS treats it: a district that actively
        disagrees means a different seat (or a different person), not a
        weaker match.

        Collect every current-officeholder district this name+office
        resolves to (full-name key first; first+last key only if the full
        key has nothing, same priority SOS uses) before deciding, rather
        than matching against whichever dict entry happens to be found
        first — a single dict.get() can't tell "the only entry" apart from
        "the first of several disagreeing entries."
        """
        matches = {d: p for (n, o, d), p in self._openstates.items()
                   if n == full and o == office}
        if not matches:
            matches = {d: p for (n, o, d), p in self._openstates_fl.items()
                       if n == fl and o == office}
        if not matches:
            return None

        if want_district:
            party = matches.get(want_district)
            if party:
                return {"party": party, "party_source": "openstates",
                        "match_confidence": "exact"}
            # This name+office currently sits in a different district than
            # the one TEC's filing names. Same rule as SOS candidacies:
            # a disagreeing district is a different seat, so decline rather
            # than guess — even though there IS a same-named officeholder
            # somewhere in this chamber.
            return None

        # No district to corroborate with. Open States tracks one person per
        # currently-held seat, so match only if this name+office resolves to
        # exactly one seat; two or more is a same-name collision within the
        # chamber that a name-only match can't resolve, so it's left blank
        # rather than guessing which one.
        if len(matches) == 1:
            (party,) = matches.values()
            return {"party": party, "party_source": "openstates",
                    "match_confidence": "high"}
        return None

    # ------------------------------------------------------------- reporting
    def coverage_report(self, candidate_offices) -> dict:
        """Split raw TEC office codes into ones a source could ever reach vs
        not, so the parse log states a coverage ceiling rather than implying
        the matcher failed on offices none of these sources have ever heard
        of (District Court, County Court, DA, ...)."""
        covered = self.sos_offices | {o for (_n, o, _d) in self._openstates}
        matchable = unmatchable = 0
        for raw in candidate_offices:
            office = canonical_tec_office(raw)
            if office and office in covered:
                matchable += 1
            else:
                unmatchable += 1
        return {
            "offices_covered": len(covered),
            "candidates_in_scope": matchable,
            "candidates_out_of_scope": unmatchable,
            "sos_candidacies": sum(len(v) for v in self._sos_full.values()),
            "openstates_people": len(self._openstates),
        }


# ================================ Main ================================
def run():
    log = get_logger("texas", "parse")
    t0  = time.perf_counter()
    log.info("Starting Texas parser")
    log._emit("parse_started")

    # =================== 0. External party enrichment overlay ===================
    # TEC's own cover.csv is tried first (section 1b below) — it's an exact
    # filerIdent join straight from the state's own reports. This overlay,
    # built from scrapers/texas.py --party's two fallback sources (TX SOS
    # legacy canvass, Open States bulk CSV — see docs/states/texas.md), only
    # ever fills whatever cover.csv leaves blank. Inert if neither raw file
    # is present.
    enrich = TXEnrichment.load(RAW_DIR)
    if enrich.available:
        log.info("Loaded external party-enrichment overlay "
                 f"({len(enrich.known_names):,} known names across "
                 f"SOS/Open States)")
    else:
        log.warning("  No party-enrichment overlay found in data/Texas/raw "
                    "— run scrapers/texas.py --party to add a fallback for "
                    "whatever cover.csv leaves blank")

    candidates: dict[str, dict] = {}   # filerIdent -> candidates row
    committees: dict[str, dict] = {}   # filerIdent -> committees row
    filer_kind: dict[str, str]  = {}   # filerIdent -> filerTypeCd
    latest_year: dict[str, int] = {}   # filerIdent -> latest transaction year

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    skipped_superseded  = 0
    skipped_no_amount   = 0   # rows whose amount column won't parse
    skipped_placeholder = 0   # TEC's reserved "DO NOT USE" filer accounts
    file_handles: list = []

    def register_from_transaction(row: dict) -> tuple[str, str, str, str]:
        """Resolve a transaction's filer, registering it if the index missed it.

        Returns (filer_id, committee_name, candidate_name, office). Every
        transaction row carries filerIdent/filerTypeCd/filerName inline, so a
        filer absent from filers.csv (TEC's index is maintained by hand from
        treasurer appointments and ballot filings, and lags) still produces a
        usable entity row rather than an orphaned transaction."""
        fid  = clean(row.get("filerIdent"))
        ftyp = clean(row.get("filerTypeCd"))
        if not fid:
            # No account number to key on (does not occur in the current
            # archive). undisplay() rather than clean_name() so the name still
            # comes out in the same forward order as everything else.
            return "", undisplay(row.get("filerName", "")), "", ""

        known = filer_kind.get(fid)
        if known is None:
            # Unknown filer — synthesize a record from the transaction itself.
            filer_kind[fid] = ftyp
            name = undisplay(row.get("filerName", ""))
            if ftyp in CANDIDATE_FILER_TYPES:
                first, last = split_person(row, "filer", row.get("filerName", ""))
                candidates[fid] = {
                    "state":           STATE,
                    "candidate_name":  name,
                    "candidate_first": first,
                    "candidate_last":  last,
                    "office":          "",
                    "district":        "",
                    "jurisdiction":    "",
                    "party":           "",   # filled from cover.csv (1b) or the
                                             # enrichment overlay (6a) if either finds it
                    "election_year":   "",
                    "incumbent":       "",
                    "state_filer_id":  fid,
                    "raw_file":        "",
                    "row_num":         "",
                }
            committees.setdefault(fid, {
                "state":           STATE,
                "committee_name":  name,
                "committee_type":  FILER_TYPES.get(ftyp, ftyp),
                "election_year":   "",
                "candidate_name":  name if ftyp in CANDIDATE_FILER_TYPES else "",
                "treasurer_name":  "",
                "city":            "",
                "zip":             "",
                "active":          "",
                "state_filer_id":  fid,
                "raw_file":        "",
                "row_num":         "",
            })
            known = ftyp

        cmte = committees.get(fid, {})
        cand = candidates.get(fid)
        return (fid,
                cmte.get("committee_name", ""),
                cand["candidate_name"] if cand else cmte.get("candidate_name", ""),
                cand.get("office", "") if cand else "")

    def note_year(fid: str, iso_date: str) -> None:
        """Track the latest transaction year per filer — TEC's index carries no
        election year, so candidates.election_year is derived from activity."""
        y = year_of(iso_date)
        if fid and y and int(y) > latest_year.get(fid, 0):
            latest_year[fid] = int(y)

    try:
        # =================== 1. Filer index ===================
        filers_path = RAW_DIR / "filers.csv"
        if not filers_path.exists() or filers_path.stat().st_size == 0:
            raise FileNotFoundError(
                f"{filers_path} is missing — run scrapers/texas.py first. Without "
                f"the filer index every transaction would produce a bare, "
                f"office-less entity."
            )

        ft = time.perf_counter()
        n_rows = 0
        # filers.csv holds one row per *registration*, not per account: 613 of
        # the 20,125 distinct filerIdents appear two or three times, differing
        # in filerTypeCd, office, address and effective dates as the filer
        # re-registered over the years.
        #
        # Picking the winner on filerEffStartDt alone doesn't work: that column
        # is blank on 57% of rows (11,804 of 20,748), which leaves 252 of the
        # duplicate groups entirely undated and 223 mixed. In the mixed groups a
        # blank row would always lose to a dated one even when the dated one is
        # long expired — e.g. account 00013756's SPK registration
        # (19830111-19930111) would beat its undated, still-open COH row.
        #
        # So rank on (still open, latest start, last seen) instead: a
        # registration with no end date is the current one; among ended ones the
        # latest start wins; ties fall back to file order, which is all the
        # information left.
        #
        # That still fails when TWO registrations are both open (no stop date)
        # and disagree on filerTypeCd, and one of them has no start date either
        # — blank sorts before any dated string, so the truly-current
        # registration loses to a decades-old one whenever the old one happens
        # to carry a start date and the current one doesn't. Confirmed case:
        # filerIdent 00018964 is Bill White's account — an SCC/STATE_CHAIR
        # registration from a 1996-2000 county-chair role (dated) beats the
        # blank-start COH/GOVERNOR registration for his 2010 run, even though
        # the governor run is what every one of its 50,000+ transactions
        # belongs to. Its own cover.csv history is split the same way (12 SCC
        # reports 1996-2000, 11 COH reports 2010-2012), so even a majority
        # vote over cover.csv picks the stale identity by one report — it's
        # the *most recent* filed report that reflects what the account
        # currently is (see section 1b). 87 filerIdents hit this exact
        # registration-level pattern. `all_regs` keeps every registration per
        # fid so section 1b can re-resolve them against cover.csv.
        best_rank: dict[str, tuple] = {}
        all_regs: dict[str, list[dict]] = defaultdict(list)

        with open(filers_path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                fid = clean(row.get("filerIdent"))
                if not fid:
                    continue
                n_rows += 1
                all_regs[fid].append({
                    "row_num": row_num,
                    "ftyp":    clean(row.get("filerTypeCd")),
                    "start":   clean(row.get("filerEffStartDt")),
                    "stop":    clean(row.get("filerEffStopDt")),
                    "row":     row,
                })

                rank = (0 if clean(row.get("filerEffStopDt")) else 1,
                        clean(row.get("filerEffStartDt")),
                        row_num)
                if fid in best_rank and rank < best_rank[fid]:
                    continue          # an older/closed registration — superseded
                best_rank[fid] = rank

                if PLACEHOLDER_NAME_RE.search(clean(row.get("filerName"))):
                    # A reserved/void account number, not an entity. Dropped
                    # from both output tables; they carry no transactions.
                    skipped_placeholder += 1
                    candidates.pop(fid, None)
                    committees.pop(fid, None)
                    filer_kind[fid] = ""
                    continue

                ftyp = clean(row.get("filerTypeCd"))
                # A filer that re-registered under a different type (e.g. COH
                # -> GPAC) must not keep a stale candidates row from the
                # earlier registration.
                if ftyp not in CANDIDATE_FILER_TYPES:
                    candidates.pop(fid, None)
                filer_kind[fid] = ftyp

                name              = tec_name(row, "filer", row.get("filerName", ""))
                office, dist, juri = office_of(row)
                status            = clean(row.get("filerFilerpersStatusCd")).upper()
                cmte_status       = clean(row.get("committeeStatusCd")).upper()

                if ftyp in CANDIDATE_FILER_TYPES:
                    first, last = split_person(row, "filer", row.get("filerName", ""))
                    candidates[fid] = {
                        "state":           STATE,
                        "candidate_name":  name,
                        "candidate_first": first,
                        "candidate_last":  last,
                        "office":          office,
                        "district":        dist,
                        # The county the office itself is scoped to — see
                        # office_of(). Blank for statewide/legislative filers.
                        "jurisdiction":    juri,
                        "party":           "",   # filled from cover.csv (1b) or the
                                                  # enrichment overlay (6a) if either finds it
                        "election_year":   "",   # derived from transactions below
                        # Only the two officeholder codes are evidence either
                        # way. "ACTIVE" (434 rows) describes the filer record's
                        # own state, not whether the person holds office, and
                        # blank/"X" say nothing — all left unknown rather than
                        # asserted as 0.
                        "incumbent":       "1" if status == "CURRENT_OFFICEHOLDER"
                                           else ("0" if status == "NOT_OFFICEHOLDER" else ""),
                        "state_filer_id":  fid,
                        "raw_file":        filers_path.name,
                        "row_num":         row_num,
                    }

                # Every filer gets a committees row, including candidates: in
                # Texas a candidate files under their own account rather than
                # through a separately registered committee, so their filerIdent
                # is what appears as the recipient on every contribution. Without
                # this row those transactions would reference a committee_name
                # that exists in no table.
                committees[fid] = {
                    "state":           STATE,
                    "committee_name":  name,
                    "committee_type":  FILER_TYPES.get(ftyp, ftyp),
                    "election_year":   "",
                    "candidate_name":  name if ftyp in CANDIDATE_FILER_TYPES else "",
                    "treasurer_name":  tec_name(row, "treas"),
                    "city":            utils.clean_name(row.get("filerStreetCity")),
                    "zip":             utils.clean_zip(clean(row.get("filerStreetPostalCode"))),
                    # committeeStatusCd is the PAC filing status and is blank
                    # for candidate accounts; fall back to the presence of an
                    # end date on the filer record.
                    "active":          ("1" if cmte_status == "ACTIVE" else
                                        "0" if cmte_status in ("TERMINATED", "INACTIVE") else
                                        "0" if clean(row.get("filerEffStopDt")) else "1"),
                    "state_filer_id":  fid,
                    "raw_file":        filers_path.name,
                    "row_num":         row_num,
                }

        log.registry_loaded("filers.csv", entries=len(committees),
                            relation="candidates+committees",
                            bytes=filers_path.stat().st_size)
        log.info(f"  filer index: {n_rows:,} registration rows -> "
                 f"{len(committees):,} accounts, of which {len(candidates):,} are "
                 f"candidates ({time.perf_counter() - ft:.1f}s)")

        # Flag the ambiguous-duplicate-registration cases described above, so
        # section 1b can try to resolve them against cover.csv evidence.
        suspect_fids = {
            fid for fid, regs in all_regs.items()
            if len(regs) >= 2
            and not any(r["stop"] for r in regs)
            and len({r["ftyp"] for r in regs}) >= 2
            and any(not r["start"] for r in regs)
            and any(r["start"] for r in regs)
        }
        if suspect_fids:
            log.info(f"  {len(suspect_fids):,} filerIdent(s) carry ambiguous "
                     f"duplicate registrations (open, conflicting filerTypeCd, "
                     f"one undated) — will attempt resolution against "
                     f"cover.csv below")

        # =================== 1b. Cover sheets ===================
        # cover.csv is one row per filed report, and it is the ONLY place in the
        # whole TEC archive that carries a party (`politicalPartyCd`), an actual
        # election date (`electionDt`) and the office as declared on that
        # report (`filerSeekOffice*`). It joins on filerIdent — an exact key, no
        # name matching — which makes it far and away the cheapest enrichment
        # available for Texas. Field names are from TEC's own record layout
        # (CFS-ReadMe.txt, Record #4 CoverSheet1Data, 119 fields).
        #
        # A filer files many reports over many years, so each field is taken
        # from that filer's most recent report that actually has one, ranked on
        # (electionDt, receivedDt, row order). Party is tracked separately from
        # the rest: a filer's latest report may leave the party box blank while
        # an earlier one filled it in, and a blank shouldn't erase a known value.
        #
        # NOTE ON PARTY COVERAGE: whether politicalPartyCd is populated for
        # candidate filers (COH/JCOH) or only for party committees
        # (CEC/MCEC/PTYCORP) is not established — TEC's Form C/OH cover sheet
        # has no party box, and CFS-Codes.txt defines no party code list. The
        # per-filer-type breakdown logged below answers that question on every
        # run. Whatever it leaves blank falls through to the external overlay
        # in section 6a (TX SOS legacy canvass -> Open States; see
        # scrapers/texas.py --party and docs/states/texas.md).
        cover_path  = RAW_DIR / "cover.csv"
        cover_year: dict[str, str] = {}          # filerIdent -> election year
        latest_cover: dict[str, tuple] = {}      # filerIdent -> (rank, ey, office, district, holds_office)
        latest_party: dict[str, tuple] = {}      # filerIdent -> (rank, party)
        # filerIdent -> (rank, filerTypeCd) of its most recent filed report,
        # used below to resolve `suspect_fids` (section 1's ambiguous
        # duplicates). A raw majority count across reports is the wrong
        # signal here — Bill White's account (00018964) has 12 SCC cover
        # sheets from a 1996-2000 county-chair role and only 11 COH ones from
        # his 2010 governor run, so a vote count picks the stale identity by
        # one report. What was filed *most recently* is what the account
        # currently is, so this uses the same recency rank as latest_cover.
        cover_latest_type: dict[str, tuple] = {}
        cover_rows = 0

        if cover_path.exists() and cover_path.stat().st_size > 0:
            ft = time.perf_counter()
            with open(cover_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    if is_superseded(row):
                        continue
                    fid = clean(row.get("filerIdent"))
                    if not fid:
                        continue
                    cover_rows += 1

                    elect = parse_date(row.get("electionDt"))
                    rank  = (elect, parse_date(row.get("receivedDt")), row_num)

                    if fid in suspect_fids:
                        prev = cover_latest_type.get(fid)
                        if prev is None or rank > prev[0]:
                            cover_latest_type[fid] = (rank, clean(row.get("filerTypeCd")))

                    party = clean(row.get("politicalPartyCd"))
                    if party.upper() == "OTHER":
                        party = clean(row.get("politicalPartyOtherDescr")) or party
                    if party:
                        prev = latest_party.get(fid)
                        if prev is None or rank > prev[0]:
                            latest_party[fid] = (rank, party)

                    seek   = clean(row.get("filerSeekOfficeCd"))
                    pfx    = "filerSeekOffice" if seek else "filerHoldOffice"
                    office = seek or clean(row.get("filerHoldOfficeCd"))
                    prev   = latest_cover.get(fid)
                    if prev is None or rank > prev[0]:
                        latest_cover[fid] = (rank, year_of(elect), office,
                                             district_of(row, pfx),
                                             bool(clean(row.get("filerHoldOfficeCd"))))

            # ---- resolve ambiguous duplicate registrations (see section 1) ----
            # For each flagged fid, trust whatever filerTypeCd its most
            # recently filed report carries over the registration metadata's
            # date tiebreak, and rebuild its candidates/committees row from
            # the matching filers.csv registration. Runs before the
            # party/office/district fill below so that fill applies to the
            # corrected record, not the discarded one.
            reg_corrected = 0
            for fid in suspect_fids:
                latest = cover_latest_type.get(fid)
                if not latest:
                    continue   # no cover.csv evidence for this fid — leave as is
                winning_ftyp = latest[1]
                if not winning_ftyp or winning_ftyp == filer_kind.get(fid):
                    continue   # section 1's pick already agrees with the reports
                match = next((r for r in all_regs[fid] if r["ftyp"] == winning_ftyp), None)
                if match is None:
                    continue   # cover.csv names a type filers.csv never registered
                row, row_num = match["row"], match["row_num"]

                name               = tec_name(row, "filer", row.get("filerName", ""))
                office, dist, juri = office_of(row)
                status             = clean(row.get("filerFilerpersStatusCd")).upper()
                cmte_status        = clean(row.get("committeeStatusCd")).upper()

                if winning_ftyp in CANDIDATE_FILER_TYPES:
                    first, last = split_person(row, "filer", row.get("filerName", ""))
                    candidates[fid] = {
                        "state":           STATE,
                        "candidate_name":  name,
                        "candidate_first": first,
                        "candidate_last":  last,
                        "office":          office,
                        "district":        dist,
                        "jurisdiction":    juri,
                        "party":           "",
                        "election_year":   "",
                        "incumbent":       "1" if status == "CURRENT_OFFICEHOLDER"
                                           else ("0" if status == "NOT_OFFICEHOLDER" else ""),
                        "state_filer_id":  fid,
                        "raw_file":        filers_path.name,
                        "row_num":         row_num,
                    }
                else:
                    candidates.pop(fid, None)

                committees[fid] = {
                    "state":           STATE,
                    "committee_name":  name,
                    "committee_type":  FILER_TYPES.get(winning_ftyp, winning_ftyp),
                    "election_year":   "",
                    "candidate_name":  name if winning_ftyp in CANDIDATE_FILER_TYPES else "",
                    "treasurer_name":  tec_name(row, "treas"),
                    "city":            utils.clean_name(row.get("filerStreetCity")),
                    "zip":             utils.clean_zip(clean(row.get("filerStreetPostalCode"))),
                    "active":          ("1" if cmte_status == "ACTIVE" else
                                        "0" if cmte_status in ("TERMINATED", "INACTIVE") else
                                        "0" if clean(row.get("filerEffStopDt")) else "1"),
                    "state_filer_id":  fid,
                    "raw_file":        filers_path.name,
                    "row_num":         row_num,
                }
                filer_kind[fid] = winning_ftyp
                reg_corrected += 1

            if reg_corrected:
                log.info(f"  Resolved {reg_corrected:,} of {len(suspect_fids):,} "
                         f"flagged filerIdent(s) to a different registration "
                         f"using cover.csv report evidence")

            # ---- apply ----
            party_applied = 0
            party_by_type: dict[str, int] = {}
            for fid, (_rank, party) in latest_party.items():
                ftyp = filer_kind.get(fid, "?")
                party_by_type[ftyp] = party_by_type.get(ftyp, 0) + 1
                cand = candidates.get(fid)
                if cand is not None:
                    _fill(cand, "party", party)
                    party_applied += 1

            office_filled = district_filled = incumbent_filled = 0
            for fid, (_rank, ey, office, district, holds) in latest_cover.items():
                if ey:
                    cover_year[fid] = ey
                cand = candidates.get(fid)
                if cand is None:
                    continue
                if office and not cand.get("office"):
                    cand["office"] = office
                    office_filled += 1
                if district and not cand.get("district"):
                    cand["district"] = district
                    district_filled += 1
                # Only ever fills upward: a held office on the filer's latest
                # cover sheet is positive evidence of incumbency, but its
                # absence isn't evidence against, so blanks stay blank.
                if holds and not cand.get("incumbent"):
                    cand["incumbent"] = "1"
                    incumbent_filled += 1

            log.registry_loaded("cover.csv", entries=cover_rows,
                                relation="candidates",
                                bytes=cover_path.stat().st_size)
            log.enrichment_summary(
                relation="candidates", source="cover.csv",
                party_filled=party_applied, office_filled=office_filled,
                district_filled=district_filled, incumbent_filled=incumbent_filled,
                election_years=len(cover_year))
            log.info(f"  cover sheets: {cover_rows:,} reports in "
                     f"{time.perf_counter() - ft:.1f}s — politicalPartyCd present "
                     f"for {len(latest_party):,} filers, by filer type: "
                     f"{dict(sorted(party_by_type.items(), key=lambda kv: -kv[1]))}")
            if not party_applied:
                log.warning("  cover.csv carries no party for any CANDIDATE filer "
                            "— falling back to the external enrichment overlay "
                            "(section 6a) for whatever it can reach")
        else:
            log.warning("  cover.csv not present in data/Texas/raw — candidates.party, "
                        "the real election year and the per-report office will all be "
                        "missing. Re-run scrapers/texas.py to extract it.")

        # =================== 2. SPAC -> candidate links ===================
        # A specific-purpose committee names the candidate it exists to support
        # or oppose. This is the only explicit committee->candidate link TEC
        # publishes, and it's far better than any name heuristic.
        spacs_path = RAW_DIR / "spacs.csv"
        spac_linked = spac_skipped = 0
        if spacs_path.exists() and spacs_path.stat().st_size > 0:
            with open(spacs_path, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    position = clean(row.get("spacPositionCd")).upper()
                    spac_id  = clean(row.get("spacFilerIdent"))
                    cand_nm  = undisplay(row.get("candidateFilerName", ""))
                    if not spac_id or not cand_nm:
                        continue
                    if position not in SPAC_LINK_POSITIONS:
                        spac_skipped += 1
                        continue
                    cmte = committees.get(spac_id)
                    if cmte is None:
                        continue
                    _fill(cmte, "candidate_name", cand_nm)
                    spac_linked += 1
            log.enrichment_summary(relation="committees", matched=spac_linked,
                                   skipped_non_support=spac_skipped,
                                   method="spacs.csv SUPPORT position → candidate_name")

        # =================== 3. Expenditure category labels ===================
        # expn_catg.csv maps the terse code (ADVERTISE) to TEC's own label
        # ("Advertising Expense"). The label is also carried inline on most
        # expenditure rows as expendCatDescr, but it's blank on a large share
        # of them, so the lookup fills the gap.
        cat_labels: dict[str, str] = {}
        catg_path = RAW_DIR / "expn_catg.csv"
        if catg_path.exists() and catg_path.stat().st_size > 0:
            with open(catg_path, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    code  = clean(row.get("expendCategoryCodeValue"))
                    label = clean(row.get("expendCategoryCodeLabel"))
                    if code:
                        cat_labels[code] = label
            log.registry_loaded("expn_catg.csv", entries=len(cat_labels),
                                relation="expenditures")

        # =================== 4. Direct-expenditure beneficiaries ===================
        # cand.csv holds the candidate a direct campaign expenditure was made
        # to benefit — a child record of an EXPN row, joined on expendInfoId.
        # Without it, a PAC's independent spending "for" a candidate has no
        # candidate attached to it anywhere in the output.
        dce_candidate: dict[str, tuple[str, str]] = {}   # expendInfoId -> (name, office)
        dce_multi = dce_superseded = 0
        cand_path = RAW_DIR / "cand.csv"
        if cand_path.exists() and cand_path.stat().st_size > 0:
            ft = time.perf_counter()
            with open(cand_path, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    # cand.csv carries the same infoOnlyFlag as its parent EXPN
                    # rows — 27,155 of its 252,534 rows (10.7%) are superseded,
                    # and letting one overwrite a live beneficiary would
                    # attribute spending to the wrong candidate.
                    if is_superseded(row):
                        dce_superseded += 1
                        continue
                    eid = clean(row.get("expendInfoId"))
                    if not eid:
                        continue
                    nm = tec_name(row, "candidate")
                    if not nm:
                        continue
                    office = (clean(row.get("candidateSeekOfficeCd"))
                              or clean(row.get("candidateHoldOfficeCd")))
                    prev = dce_candidate.get(eid)
                    if prev is not None:
                        # One direct expenditure can legitimately benefit
                        # several candidates (a shared mailer, a slate ad), but
                        # expenditures.candidate_name holds one name. First
                        # wins, and the collisions are counted so the scale of
                        # what's being flattened is visible in the log rather
                        # than invisible.
                        if prev[0] != nm:
                            dce_multi += 1
                        continue
                    dce_candidate[eid] = (nm, office)
            log.registry_loaded("cand.csv", entries=len(dce_candidate),
                                relation="expenditures",
                                bytes=cand_path.stat().st_size)
            log.info(f"  direct-expenditure beneficiaries: {len(dce_candidate):,} "
                     f"({dce_multi:,} expenditures benefit more than one candidate "
                     f"— first kept; {dce_superseded:,} superseded rows ignored) "
                     f"({time.perf_counter() - ft:.1f}s)")

        # =================== 5. Transactions ===================
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        file_handles.append(cont_fh)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        file_handles.append(expn_fh)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles.append(loan_fh)

        # ---- Contributions: contribs_##.csv + credits.csv ----
        contrib_sources = raw_files("contribs_*.csv")
        credits_path    = RAW_DIR / "credits.csv"
        if credits_path.exists() and credits_path.stat().st_size > 0:
            contrib_sources.append(credits_path)

        for path in contrib_sources:
            ft, count, skipped, no_amount = time.perf_counter(), 0, 0, 0
            # credits.csv uses creditDt/creditAmount/payor* where the contribs
            # shards use contributionDt/contributionAmount/contributor*; the
            # rest of the record is identical, so one loop handles both.
            is_credit = path.name == "credits.csv"
            dt_col    = "creditDt"     if is_credit else "contributionDt"
            amt_col   = "creditAmount" if is_credit else "contributionAmount"
            desc_col  = "creditDescr"  if is_credit else "contributionDescr"
            party_pfx = "payor"        if is_credit else "contributor"

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    if is_superseded(row):
                        skipped += 1
                        continue
                    amount = parse_amount(row.get(amt_col))
                    if not amount:
                        no_amount += 1
                        continue

                    fid, cmte_name, cand_name, office = register_from_transaction(row)
                    txn_date = parse_date(row.get(dt_col))
                    note_year(fid, txn_date)

                    sched = clean(row.get("schedFormTypeCd"))
                    # Out-of-state PACs are the one contributor category TEC
                    # flags explicitly; otherwise all it distinguishes is
                    # INDIVIDUAL vs ENTITY. credits.csv (Schedule K) has no
                    # OosPac flag and no employer/occupation columns at all —
                    # its payor is a bank or the filer, not a donor.
                    if (not is_credit
                            and clean(row.get("contributorOosPacFlag")).upper() == "Y"):
                        ctype = "OUT_OF_STATE_PAC"
                    else:
                        ctype = clean(row.get(f"{party_pfx}PersentTypeCd"))

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cmte_name,
                        "amount":            amount,
                        "date":              txn_date,
                        "transaction_type":  CONTRIB_SCHEDULES.get(sched, sched),
                        "contributor_name":  tec_name(row, party_pfx),
                        "contributor_type":  ctype,
                        "contributor_city":  utils.clean_name(row.get(f"{party_pfx}StreetCity")),
                        "contributor_state": clean(row.get(f"{party_pfx}StreetStateCd")),
                        "contributor_zip":   utils.clean_zip(clean(row.get(f"{party_pfx}StreetPostalCode"))),
                        # Schedule K rows have no employer/occupation columns.
                        "employer":          "" if is_credit else utils.clean_name(row.get("contributorEmployer")),
                        "occupation":        "" if is_credit else utils.clean_name(row.get("contributorOccupation")),
                        "candidate_name":    cand_name,
                        "office":            office,
                        "election_year":     year_of(txn_date),
                        # TEC republishes corrected reports as new rows and
                        # marks the superseded ones (dropped above), so a row
                        # that survives is current by construction. The flag
                        # records that this row came off a correction form.
                        "amended":           "1" if clean(row.get("formTypeCd")).startswith("COR") else "0",
                        "filing_id":         clean(row.get("reportInfoIdent")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size,
                            skipped=skipped + no_amount)
            if no_amount:
                log.warning(f"  {path.name}: {no_amount:,} row(s) dropped — "
                            f"unparseable amount")
            total_contributions += count
            skipped_superseded  += skipped
            skipped_no_amount   += no_amount

        # ---- Expenditures: expend_##.csv ----
        for path in raw_files("expend_*.csv"):
            ft, count, skipped, no_amount = time.perf_counter(), 0, 0, 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    if is_superseded(row):
                        skipped += 1
                        continue
                    amount = parse_amount(row.get("expendAmount"))
                    if not amount:
                        no_amount += 1
                        continue

                    fid, cmte_name, cand_name, office = register_from_transaction(row)
                    txn_date = parse_date(row.get("expendDt"))
                    note_year(fid, txn_date)

                    sched = clean(row.get("schedFormTypeCd"))
                    code  = clean(row.get("expendCatCd"))

                    # A direct campaign expenditure names the candidate it
                    # benefits rather than being money the candidate controls;
                    # that beneficiary (from cand.csv) is the more useful
                    # attribution than the spending PAC's own blank.
                    dce = dce_candidate.get(clean(row.get("expendInfoId")))
                    if dce:
                        cand_name = cand_name or dce[0]
                        office    = office or dce[1]

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "amount":           amount,
                        "date":             txn_date,
                        "transaction_type": EXPEND_SCHEDULES.get(sched, sched),
                        "payee_name":       tec_name(row, "payee"),
                        "purpose":          clean(row.get("expendDescr")),
                        "category":         cat_labels.get(code, clean(row.get("expendCatDescr"))) or code,
                        "payee_city":       utils.clean_name(row.get("payeeStreetCity")),
                        "payee_state":      clean(row.get("payeeStreetStateCd")),
                        "payee_zip":        utils.clean_zip(clean(row.get("payeeStreetPostalCode"))),
                        "candidate_name":   cand_name,
                        "office":           office,
                        "election_year":    year_of(txn_date),
                        "amended":          "1" if clean(row.get("formTypeCd")).startswith("COR") else "0",
                        "filing_id":        clean(row.get("reportInfoIdent")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1

            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size,
                            skipped=skipped + no_amount)
            if no_amount:
                log.warning(f"  {path.name}: {no_amount:,} row(s) dropped — "
                            f"unparseable amount")
            total_expenditures += count
            skipped_superseded += skipped
            skipped_no_amount  += no_amount

        # ---- Loans (Schedule E) ----
        loans_path = RAW_DIR / "loans.csv"
        if loans_path.exists() and loans_path.stat().st_size > 0:
            ft, count, skipped = time.perf_counter(), 0, 0
            with open(loans_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    if is_superseded(row):
                        skipped += 1
                        continue
                    amount = parse_amount(row.get("loanAmount"))
                    fid, cmte_name, cand_name, _office = register_from_transaction(row)
                    txn_date = parse_date(row.get("loanDt"))
                    note_year(fid, txn_date)
                    sched = clean(row.get("schedFormTypeCd"))

                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cmte_name,
                        "original_amount":    amount,
                        "date":               txn_date,
                        "record_type":        LOAN_SCHEDULES.get(sched, "Loan"),
                        "counterparty_name":  tec_name(row, "lender"),
                        "counterparty_city":  utils.clean_name(row.get("lenderStreetCity")),
                        "counterparty_state": clean(row.get("lenderStreetStateCd")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("lenderStreetPostalCode"))),
                        "candidate_name":     cand_name,
                        "election_year":      year_of(txn_date),
                        "amended":            "1" if clean(row.get("formTypeCd")).startswith("COR") else "0",
                        "filing_id":          clean(row.get("reportInfoIdent")),
                        "raw_file":           loans_path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(loans_path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=loans_path.stat().st_size, skipped=skipped)
            total_loans        += count
            skipped_superseded += skipped

        # ---- Outstanding loans / debts (Schedule L) ----
        # Schedule L carries the lender's identity but NO amount and NO loan
        # date — TEC's record layout has neither field on this record, and its
        # loanInfoId lives in a different id space from Schedule E's (verified:
        # 20,148 Schedule E ids and 7,678 Schedule L ids, zero overlap), so
        # there is nothing to join the amount back from. The rows are still
        # written, with the report's received date, because they're the only
        # record that an obligation was outstanding at all — but
        # original_amount is necessarily blank.
        debts_path = RAW_DIR / "debts.csv"
        if debts_path.exists() and debts_path.stat().st_size > 0:
            ft, count, skipped = time.perf_counter(), 0, 0
            with open(debts_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    if is_superseded(row):
                        skipped += 1
                        continue
                    fid, cmte_name, cand_name, _office = register_from_transaction(row)
                    rpt_date = parse_date(row.get("receivedDt"))

                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cmte_name,
                        "original_amount":    "",
                        "date":               rpt_date,
                        "record_type":        "Outstanding Loan",
                        "counterparty_name":  tec_name(row, "lender"),
                        "counterparty_city":  utils.clean_name(row.get("lenderStreetCity")),
                        "counterparty_state": clean(row.get("lenderStreetStateCd")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("lenderStreetPostalCode"))),
                        "candidate_name":     cand_name,
                        "election_year":      year_of(rpt_date),
                        "amended":            "1" if clean(row.get("formTypeCd")).startswith("COR") else "0",
                        "filing_id":          clean(row.get("reportInfoIdent")),
                        "raw_file":           debts_path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(debts_path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=debts_path.stat().st_size, skipped=skipped)
            total_loans        += count
            skipped_superseded += skipped

        # =================== 6. Flush candidates + committees ===================
        # election_year has two possible sources, and cover.csv's beats the
        # fallback outright: electionDt is the actual election the report was
        # filed for, whereas latest_year is just the calendar year of the most
        # recent transaction, which drifts (money raised in the January after
        # an election lands in the wrong year). The transaction year is used
        # only for filers with no usable cover sheet.
        for fid in set(latest_year) | set(cover_year):
            ey = cover_year.get(fid) or str(latest_year.get(fid, "") or "")
            if not ey:
                continue
            if fid in candidates:
                candidates[fid]["election_year"] = ey
            if fid in committees:
                committees[fid]["election_year"] = ey

        # ========= 6a. Apply the external party-enrichment overlay =========
        # Runs after election_year and district are as complete as TEC's own
        # data will make them, since the matcher uses both to corroborate a
        # match — and only ever *fills*, never overwrites: cover.csv's party
        # (this state's own data) always wins where it exists.
        n_party = 0
        conf_counts: dict[str, int] = {}
        source_counts: dict[str, int] = {}
        if enrich.available:
            for cand in candidates.values():
                if cand.get("party"):
                    continue
                hit = enrich.lookup(cand["candidate_name"], cand["office"],
                                    cand["district"], cand["election_year"])
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

            cov = enrich.coverage_report(c["office"] for c in candidates.values())
            log.info(
                f"  Party enrichment: {n_party:,} candidates filled "
                f"(exact {conf_counts.get('exact', 0):,} / "
                f"high {conf_counts.get('high', 0):,}) — by source: "
                f"{dict(sorted(source_counts.items(), key=lambda kv: -kv[1]))}"
            )
            # Stated explicitly so a low fill rate reads as the sources'
            # structural ceiling (District Court/County Court/DA are outside
            # both) rather than as a matcher that isn't working.
            log.info(
                f"  Enrichment scope: {cov['candidates_in_scope']:,} of "
                f"{len(candidates):,} candidates hold an office at least one "
                f"fallback source covers; {cov['candidates_out_of_scope']:,} "
                f"hold county/judicial offices none of them reach"
            )
            log.enrichment_summary(
                relation="candidates", matched=n_party, total=len(candidates),
                method="TX SOS legacy canvass → Open States bulk CSV "
                       "(strict name+office+district/year, tried in that order)")

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

        # person: filerIdent is already the person-level key. The "committee"
        # grouping model was measured against this file and rejected — see the
        # module docstring.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_out_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    len(committees),
                        role="output", bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    len(candidates),
                        role="output", bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — dropped {skipped_superseded:,} superseded rows, "
                 f"{skipped_no_amount:,} with an unparseable amount, and "
                 f"{skipped_placeholder:,} placeholder filer accounts")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees),
                  candidates=len(candidates),
                  skipped_superseded=skipped_superseded,
                  skipped_no_amount=skipped_no_amount,
                  skipped_placeholder=skipped_placeholder,
                  spac_linked=spac_linked, dce_multi_candidate=dce_multi)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees),
                  candidates=len(candidates))
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees),
                  candidates=len(candidates),
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
