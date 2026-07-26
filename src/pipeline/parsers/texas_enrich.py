"""
parsers/texas_enrich.py — Fill `candidates.party` for Texas filers that
TEC's own cover.csv leaves blank, from the two fallback sources in
scrapers/texas_party.py.

Loaded by parsers/texas.py, after its cover.csv pass. Kept in its own module
for the same reason parsers/new_york_enrich.py is: the matching logic is
substantial, it is the only part of the TX pipeline that reasons about data
TEC didn't publish, and it needs to be testable without running a 9 GB parse.

Inputs (both optional, written by scrapers/texas_party.py):
    data/Texas/raw/SOS_RaceSummary.csv
    data/Texas/raw/OpenStates_People.csv

If neither exists this module is inert: every lookup returns None and the
parser writes exactly the blank `party` it writes today. Enrichment must
never be load-bearing for a state's parse to succeed.

A third source, The Green Papers, was tried in `scrapers/texas_party.py` and
removed there — its live markup never settled into a stable, parseable
contract. This module no longer loads or matches against it.

Priority order
--------------
Unlike New York's enrichment (which treats NYSBOE results and Open States as
two independent corroborating reads on one fact, and blanks a value they
disagree on), Texas's two sources are tried in a fixed priority order and
the first one that produces a strict match wins — there is no cross-source
corroboration step, because the sources don't overlap much in what they
cover (SOS has State House/Senate + statewide through 2019; Open States has
only *current* House/Senate) and forcing them to agree would mostly just
throw away single-source answers that are individually solid:

    1. tx_sos_results   Texas SOS legacy canvass, 1992-2019 (candidates AND
                        officeholders, winners and losers alike)
    2. openstates       Open States bulk CSV, current TX Legislature only

Matching contract
-----------------
Strict only, same bar as New York's: name and canonical office must agree,
plus corroboration from district and/or election year appropriate to that
source (see each `_match_*` method). No nickname expansion, no soundex, no
edit distance — a wrong party attached to a real person is a worse outcome
than a blank one.

    party_source       "tx_sos_results" | "openstates"
    match_confidence    "exact" | "high"

    exact  name + office + district + year all agree (or, for a statewide
           office with no district, name + office + year)
    high   name + office agree and one corroborator (district or year) is
           present and agrees while the other is simply absent, never
           disagreeing

Same "never disagreeing" bar applies to Open States even though it has no
election year to check: a district it names for this name+office that does
not match the district TEC's filing names counts as disagreement (a
different seat, or a different person), and a name+office pair that
currently resolves to more than one district with nothing to pick between
them is an unresolved ambiguity, not a "high" confidence guess. Both cases
return no match rather than falling back to an arbitrary same-named
officeholder.

Coverage ceiling
----------------
Between the two sources this reaches statewide executive offices, State
Senate/House, State Board of Education and the appellate judiciary (Supreme
Court / Court of Criminal Appeals / Courts of Appeals) — the offices
significant enough to be canvassed at the state level or tracked by a
national legislative database. District Court, County Court, District
Attorney and other county-certified offices (the bulk of TEC's `JUDGEDIST`,
`JUDGESTATCO`, `DISTATTY` filer types) are outside both sources and stay
blank no matter how good the matcher is — `coverage_report()` splits this out
so a low fill rate reads as a source ceiling, not a broken matcher.
"""

import csv
import re
from collections import defaultdict
from pathlib import Path

from src.aliases import canonical_office_type, canonical_party

# ========================== name canonicalisation ==========================
_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "ESQ", "MD", "PHD"}
_PUNCT_RE   = re.compile(r"[.,\-–—']")
_NONWORD_RE = re.compile(r"[^A-Z0-9 ]")
_INCUMBENT_MARK_RE = re.compile(r"\(I\)\s*$")


def _tokens(name: str) -> list[str]:
    up = _INCUMBENT_MARK_RE.sub("", (name or "").upper())
    up = _PUNCT_RE.sub(" ", up)
    up = _NONWORD_RE.sub(" ", up)
    toks = [t for t in up.split() if t]
    while toks and toks[-1] in _SUFFIXES:
        toks.pop()
    return toks


def name_keys(name: str) -> tuple[str, str]:
    """(full_key, first_last_key) — see new_york_enrich.name_keys for the
    rationale; identical approach, reused rather than re-derived because both
    modules solve the same "two disclosure systems spell names differently"
    problem the same way."""
    toks = _tokens(name)
    if len(toks) < 2:
        return "", ""
    return " ".join(toks), f"{toks[0]} {toks[-1]}"


# ========================= office canonicalisation ==========================
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
_STATEWIDE = {
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
        statewide = office in _STATEWIDE
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