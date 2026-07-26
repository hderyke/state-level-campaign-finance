"""
parsers/new_york_enrich.py — Join NYSBOE election results and Open States onto
the New York candidate registry to fill party, incumbent, district and
election_year.

Loaded by parsers/new_york.py. Kept in its own module because the matching
logic is substantial, is the only part of the NY pipeline that reasons about
data NYSBOE didn't publish, and needs to be testable without running an
18M-row parse.

Inputs (both optional, written by scrapers/new_york_party.py):
    data/New York/raw/ElectionStats_Contests.csv
    data/New York/raw/OpenStates_People.csv

If neither exists this module is inert: every lookup returns "no answer" and
the parser writes exactly the blanks it writes today. Enrichment must never be
load-bearing for a state's parse to succeed.

Matching contract
-----------------
Strict only. A match requires the candidate's **name and canonical office** to
agree, plus **either the district or the election year**. There is no nickname
expansion, no soundex, no edit distance, and no single-token surname matching —
NY has 36,486 candidate filers over three decades and enough repeated names
("Eric A. Ulrich" alone holds seven filer_ids) that fuzzy matching would
attach wrong parties to real people at a rate nobody downstream could detect.

Every enriched field is written alongside two provenance columns so a consumer
can set their own bar rather than inheriting ours:

    party_source      "nysboe_results" | "openstates" | "nysboe_results+openstates"
    match_confidence  "exact" | "high"

    exact  name + office + district + election year all agree
    high   name + office agree, and one of district / election year agrees
           (the other is absent from one side rather than contradicting)

A disagreement is never a match. Where NYSBOE results and Open States name
different parties for the same person, the row is left blank and counted in
the conflict tally rather than resolved by preferring a source.

Fusion voting
-------------
New York lets several parties nominate the same candidate, each getting its own
ballot line. `party` is therefore multi-valued: every line that candidacy
carried, canonicalised through src/aliases/parties.csv and joined with "|",
ordered by votes received on that line (descending) so the candidate's primary
line reads first and the ordering is deterministic:

    "DEMOCRAT|WORKING FAMILIES"
    "REPUBLICAN|CONSERVATIVE|INDEPENDENCE"

Consumers wanting a single value should take the substring before the first
"|". Consumers wanting to know whether someone was cross-endorsed can count
the separators. Collapsing to one line here would have thrown that away
irreversibly.

Coverage ceiling
----------------
NYSBOE certifies statewide, congressional, state-legislative and judicial
contests. Town, village, city and school-district races are certified by the
62 county boards and are absent from the results database. Those local offices
are most of the NY filer registry, so party fill on `candidates` is capped
structurally — not by this matcher. `coverage_report()` returns the split so
the number is visible in the parse log rather than being mistaken for a
matching failure.
"""

import csv
import re
from collections import defaultdict
from pathlib import Path

from src.aliases import canonical_party

# ========================= office canonicalisation =========================
# NYSBOE's filer registry and its results database are two different systems
# with two different office vocabularies ("Member of Assembly" vs "Assembly
# Member", "Comptroller" vs "State Comptroller"). Both sides are reduced to
# the tokens below before comparison. Only offices that actually appear in the
# results database are listed — mapping a town-council office would create the
# illusion of a match target that doesn't exist.
#
# Matched longest-pattern-first, so "State Senator" can't be swallowed by a
# looser "Senator" rule.
_OFFICE_PATTERNS: list[tuple[str, str]] = [
    (r"\bunited states senator\b|\bu\.?s\.? senator\b",              "US_SENATE"),
    (r"\brepresentative in congress\b|\bu\.?s\.? (?:house|representative)\b"
     r"|\bmember of congress\b|\bcongress(?:person|man|woman)\b",     "US_HOUSE"),
    (r"\blieutenant governor\b|\blt\.? governor\b",                  "LT_GOVERNOR"),
    (r"\battorney general\b",                                        "ATTORNEY_GENERAL"),
    (r"\b(?:state )?comptroller\b",                                  "COMPTROLLER"),
    (r"\bgovernor\b",                                                "GOVERNOR"),
    (r"\bmember of (?:the )?assembly\b|\bassembly ?(?:member|man|woman)\b"
     r"|\bstate assembly\b",                                         "ASSEMBLY"),
    (r"\bstate senator\b|\bstate senate\b|\bsenator\b",              "STATE_SENATE"),
    (r"\b(?:justice of the )?supreme court(?: justice)?\b",          "SUPREME_COURT"),
    (r"\bcourt of appeals\b",                                        "COURT_OF_APPEALS"),
    (r"\bsurrogate'?s? court\b|\bsurrogate\b",                       "SURROGATE_COURT"),
    (r"\bfamily court\b",                                            "FAMILY_COURT"),
    (r"\bcounty court\b",                                            "COUNTY_COURT"),
    (r"\bcivil court\b",                                             "CIVIL_COURT"),
    (r"\bcity court\b",                                              "CITY_COURT"),
    (r"\bdistrict court\b",                                          "DISTRICT_COURT"),
    (r"\bdistrict attorney\b",                                       "DISTRICT_ATTORNEY"),
    (r"\bcounty executive\b",                                        "COUNTY_EXECUTIVE"),
    (r"\bcounty clerk\b",                                            "COUNTY_CLERK"),
    (r"\bsheriff\b",                                                 "SHERIFF"),
]
_OFFICE_COMPILED = [(re.compile(p, re.IGNORECASE), tok) for p, tok in _OFFICE_PATTERNS]

# Offices elected statewide — they have no district, so a district comparison
# is meaningless and the year alone carries the "high" match.
_STATEWIDE = {"US_SENATE", "GOVERNOR", "LT_GOVERNOR",
              "ATTORNEY_GENERAL", "COMPTROLLER", "COURT_OF_APPEALS"}


def canonical_office(raw: str) -> str:
    """Reduce an office label from either vocabulary to a shared token, or ''.

    '' means "not an office the results database covers", which is the
    overwhelmingly common case for the NY filer registry (town, village and
    school offices). Callers treat '' as "unmatchable", not as an error.
    """
    text = (raw or "").strip()
    if not text:
        return ""
    for rx, token in _OFFICE_COMPILED:
        if rx.search(text):
            return token
    return ""


# ========================== name canonicalisation ==========================
_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V", "ESQ", "MD", "PHD"}
# Hyphens are separators, not part of a token: NYSBOE's filer registry writes
# "Mercedes Vazquez Simmons" where the results database writes "Mercedes
# Vazquez-Simmons", and splitting on the hyphen makes those the same three
# tokens on both sides. Apostrophes are kept — "O'BRIEN" is spelled that way
# consistently by both systems and splitting it would collide with "O BRIEN".
_PUNCT_RE = re.compile(r"[.,\-–—]")
_NONWORD_RE = re.compile(r"[^A-Z0-9' ]")


def _tokens(name: str) -> list[str]:
    """Uppercase, drop punctuation and honorific suffixes, split to tokens."""
    up = _PUNCT_RE.sub(" ", (name or "").upper())
    up = _NONWORD_RE.sub(" ", up)
    toks = [t for t in up.split() if t]
    while toks and toks[-1] in _SUFFIXES:
        toks.pop()
    return toks


def name_keys(name: str) -> tuple[str, str]:
    """Return (full_key, first_last_key) for a person's name.

    full_key         every token, in order  — "DANIEL P MOYNIHAN"
    first_last_key   first and last only    — "DANIEL MOYNIHAN"

    Two keys rather than one because the two sources disagree on middle names
    far more often than they disagree on people: NYSBOE's registry carries
    "Mercedes Vazquez Simmons" where the results database has "Mercedes
    Vazquez-Simmons". Matching on first+last is a *relaxation of the middle
    name only* — it is not fuzzy matching, since both ends must still be
    identical and office plus district/year must still agree independently.

    Single-token names return ("", "") and never match: they are almost always
    organisations that slipped into a name field.
    """
    toks = _tokens(name)
    if len(toks) < 2:
        return "", ""
    return " ".join(toks), f"{toks[0]} {toks[-1]}"


def _party_list(raw_lines) -> str:
    """Canonicalise ballot lines and join them pipe-delimited, de-duplicated.

    `raw_lines` is an iterable of (party_label, votes) pairs. Ordering is by
    votes descending then label, which puts a fusion candidate's main line
    first and stays stable when votes are absent (all zero → alphabetical).
    """
    best: dict[str, int] = {}
    for label, votes in raw_lines:
        canon = canonical_party(label) or (label or "").strip().upper()
        if not canon or canon in {"WRITE-IN", "OTHER"}:
            continue
        best[canon] = max(best.get(canon, 0), votes)
    if not best:
        return ""
    ordered = sorted(best.items(), key=lambda kv: (-kv[1], kv[0]))
    return "|".join(label for label, _ in ordered)


def _int(val) -> int:
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (TypeError, ValueError):
        return 0


def _truthy(val) -> bool:
    return str(val or "").strip().upper() in {"1", "Y", "YES", "TRUE", "W", "WON"}


# ============================== the matcher ==============================
class Candidacy:
    """One person's run for one office in one year, with every ballot line."""

    __slots__ = ("name", "office", "district", "year", "lines", "won", "general")

    def __init__(self, name, office, district, year):
        self.name     = name
        self.office   = office
        self.district = district
        self.year     = year
        self.lines: list[tuple[str, int]] = []
        self.won      = False
        self.general  = False

    @property
    def party(self) -> str:
        return _party_list(self.lines)


class NYEnrichment:
    """Lookup tables built from the two enrichment files.

    Usage from the parser::

        enrich = NYEnrichment.load(RAW_DIR)
        if enrich.available:
            result = enrich.lookup(name, office_desc, district, election_year)
    """

    def __init__(self):
        # (name_key, office_token) -> [Candidacy], newest first
        self._by_full: dict[tuple[str, str], list[Candidacy]] = defaultdict(list)
        self._by_fl:   dict[tuple[str, str], list[Candidacy]] = defaultdict(list)
        # office tokens the results file actually covers, for honest "we can't
        # know" answers on incumbency
        self.covered_offices: set[str] = set()
        # (name_key, office_token, district) -> party, from Open States.
        # Keyed on the seat and not on the name alone: NY's registry holds
        # seven distinct filers named "Eric A. Ulrich", and a name-only key
        # would let one person's party corroborate — or veto — a different
        # person who happens to share their name.
        self._openstates: dict[tuple[str, str, str], str] = {}
        # every person name seen in the results DB — widens committee linkage
        self.known_names: set[str] = set()
        self.conflicts = 0
        self.available = False

    # ------------------------------------------------------------------ load
    @classmethod
    def load(cls, raw_dir: Path) -> "NYEnrichment":
        self = cls()
        self._load_results(raw_dir / "ElectionStats_Contests.csv")
        self._load_openstates(raw_dir / "OpenStates_People.csv")
        self.available = bool(self._by_full or self._openstates)
        return self

    def _load_results(self, path: Path) -> None:
        if not path.exists() or path.stat().st_size == 0:
            return
        # (full_key, office, district, year) -> Candidacy
        merged: dict[tuple, Candidacy] = {}
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                name = (row.get("candidate_name") or "").strip()
                full, fl = name_keys(name)
                if not full:
                    continue
                office = canonical_office(row.get("office") or "")
                if not office:
                    # Still useful for committee-name linkage even when the
                    # office isn't one we can match on.
                    self.known_names.add(full)
                    continue
                year     = (row.get("election_year") or "").strip()
                district = (row.get("district") or "").strip().lstrip("0")
                key = (full, office, district, year)
                cand = merged.get(key)
                if cand is None:
                    cand = merged[key] = Candidacy(name, office, district, year)
                cand.lines.append(((row.get("party") or "").strip(),
                                   _int(row.get("votes"))))
                if _truthy(row.get("is_winner")):
                    cand.won = True
                if re.search(r"general", row.get("stage") or "", re.IGNORECASE):
                    cand.general = True
                self.covered_offices.add(office)
                self.known_names.add(full)

        for (full, office, _district, _year), cand in merged.items():
            _f, fl = name_keys(cand.name)
            self._by_full[(full, office)].append(cand)
            if fl and fl != full:
                self._by_fl[(fl, office)].append(cand)

        # Newest candidacy first, and general elections ahead of primaries in
        # the same year: a general-election row carries the full fusion set,
        # whereas a primary row only proves which primary the person entered.
        for bucket in (self._by_full, self._by_fl):
            for lst in bucket.values():
                lst.sort(key=lambda c: (c.year or "0000", c.general), reverse=True)

    # Open States chamber codes -> this module's office tokens.
    _CHAMBERS = {"lower": "ASSEMBLY", "upper": "STATE_SENATE"}

    def _load_openstates(self, path: Path) -> None:
        if not path.exists() or path.stat().st_size == 0:
            return
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                full, _fl = name_keys(row.get("name") or "")
                if not full:
                    continue
                self.known_names.add(full)
                party  = canonical_party(row.get("current_party") or "")
                office = self._CHAMBERS.get(
                    (row.get("current_chamber") or "").strip().lower(), "")
                district = (row.get("current_district") or "").strip().lstrip("0")
                if party and office:
                    self._openstates[(full, office, district)] = party

    # ---------------------------------------------------------------- lookup
    def lookup(self, name: str, office_desc: str, district: str,
               election_year: str) -> dict | None:
        """Strict match. Returns None when nothing qualifies.

        On success returns::

            {"party", "party_source", "match_confidence",
             "district", "election_year", "incumbent"}

        `district` and `election_year` are the matched candidacy's values, for
        the parser to use as a backfill where its own are blank — they never
        overwrite a value NYSBOE already published.
        """
        office = canonical_office(office_desc)
        if not office:
            return None

        full, fl = name_keys(name)
        if not full:
            return None

        want_district = (district or "").strip().lstrip("0")
        want_year     = (election_year or "").strip()

        pool = self._by_full.get((full, office)) or self._by_fl.get((fl, office))
        if not pool:
            return None

        statewide = office in _STATEWIDE
        best, best_conf = None, None
        for cand in pool:
            both_districts = bool(want_district) and bool(cand.district)
            d_ok = both_districts and cand.district == want_district
            y_ok = bool(want_year) and cand.year == want_year

            # A contradiction disqualifies outright — the same name and office
            # in a *different* district is a different person often enough in
            # NY (Assembly seats get renumbered, relatives run in adjacent
            # seats) that treating it as a match would be exactly the
            # silent-wrong-value failure this matcher exists to avoid.
            if both_districts and not d_ok:
                continue

            if statewide:
                # No district exists for these offices, so the year alone is
                # the whole corroborating signal — and a full one, not a
                # degraded one.
                conf = "exact" if y_ok else None
            elif d_ok and y_ok:
                conf = "exact"
            elif d_ok or y_ok:
                # Exactly one corroborator agreed and the other is absent from
                # one side (a disagreeing district was already skipped above).
                conf = "high"
            else:
                conf = None

            if conf == "exact":
                best, best_conf = cand, conf
                break
            if conf == "high" and best_conf is None:
                # pool is sorted newest-first, so this is the most recent
                # candidacy that corroborates.
                best, best_conf = cand, conf
        if best is None:
            return None

        party = best.party
        source = "nysboe_results"

        # Open States is an independent read on the same fact, consulted only
        # for the *same seat*. Agreement is left silent; disagreement blanks
        # the value rather than picking a winner, because there is no
        # principled basis for preferring either source.
        os_party = self._openstates.get(
            (full, office, best.district or want_district))
        if os_party and party:
            if os_party not in party.split("|"):
                self.conflicts += 1
                return None
            source = "nysboe_results+openstates"
        elif os_party and not party:
            party, source = os_party, "openstates"

        if not party:
            return None

        return {
            "party":            party,
            "party_source":     source,
            "match_confidence": best_conf,
            "district":         best.district,
            "election_year":    best.year,
            "incumbent":        self._incumbent(full, fl, office,
                                                best.district, best.year),
        }

    # ------------------------------------------------------------- incumbency
    def _incumbent(self, full: str, fl: str, office: str,
                   district: str, year: str) -> str:
        """1 / 0 / '' — did this person win the previous election for this seat?

        '' (unknown) rather than 0 whenever the answer can't be established:
        no year to reason from, or no prior candidacy for this person on
        record. Writing 0 in those cases would make `incumbent` look 100%
        filled while being, for most of the table, an assertion the data
        doesn't support.
        """
        if not year or not year.isdigit():
            return ""
        pool = self._by_full.get((full, office)) or self._by_fl.get((fl, office))
        if not pool:
            return ""
        this_year = int(year)
        prior = [
            c for c in pool
            if c.year.isdigit() and int(c.year) < this_year
            and (not district or not c.district or c.district == district)
        ]
        if not prior:
            return ""
        # Most recent prior run for the seat decides it.
        latest = max(prior, key=lambda c: int(c.year))
        return "1" if latest.won else "0"

    # ------------------------------------------------------------- reporting
    def coverage_report(self, candidate_offices) -> dict:
        """Split a collection of raw office_desc values into matchable and not.

        Called by the parser so the log states plainly how much of the
        candidates table these sources *could* reach, separating a coverage
        ceiling in the source from a failure in the matcher.
        """
        matchable = unmatchable = 0
        for raw in candidate_offices:
            if canonical_office(raw) in self.covered_offices:
                matchable += 1
            else:
                unmatchable += 1
        return {
            "offices_covered":    len(self.covered_offices),
            "candidates_in_scope": matchable,
            "candidates_out_of_scope": unmatchable,
            "results_candidacies": sum(len(v) for v in self._by_full.values()),
            "openstates_people":  len(self._openstates),
            "party_conflicts":    self.conflicts,
        }
