"""
parsers/new_york.py — Parse New York NYSBOE raw Socrata exports into canonical
cleaned CSVs.

Raw files (all in data/New York/raw/, written by scrapers/new_york.py):
  Filers.csv                       -> candidates + committees registry (all history)
  ActiveCandidates.csv             -> active-status overlay for candidates
  ActiveCommittees.csv             -> active-status overlay for committees
  Disclosure_YYYY.csv / _misc.csv  -> contributions / expenditures / loans_debts

Unlike most states, NY publishes every kind of money movement in ONE table.
Each disclosure row carries a `filing_sched_abbrev` letter (A–U) that says
which NYSBOE schedule it came from, and that letter is the only thing
separating a contribution from an expenditure from a loan. `SCHEDULES` below
is the full A–U map, built from the live
`$select=filing_sched_abbrev,filing_sched_desc,count(*)&$group=...` output —
all 21 letters are accounted for, none are guessed.

Amounts: there is no column literally named `amount`. `org_amt` is the
transaction amount on every schedule (confirmed by sampling rows from A, D,
F, I, N and O); `owed_amt` is the still-outstanding balance and is populated
only on the liability schedules. loans_debts.original_amount takes `org_amt`
and falls back to `owed_amt`.

Schedule O ("LLCs/Partnership/Subcontractor", 160,842 rows) is deliberately
NOT written to any money table. It's a detail schedule — the itemization of
who is behind an LLC contribution, or which subcontractors a vendor paid —
and 71,401 of its rows carry a `trans_mapping` pointing back at a parent
transaction that is itself already written. Including it would double-count.
The rows are still counted and reported in the parse log as `skipped_detail`
so the drop is visible rather than silent.

Entities
--------
The disclosure rows carry only `cand_comm_name` — no office, no party, no
district. All entity detail comes from Filers.csv (64,464 rows), which holds
both CANDIDATE and COMMITTEE records keyed by the same `filer_id` the
transactions use:

  compliance_type_desc = CANDIDATE  -> candidates row (office_desc, district,
                                       county_desc, address)
  compliance_type_desc = COMMITTEE  -> committees row (committee_type_desc,
                                       treasurer name, address, active)

NY publishes **no party affiliation** anywhere in these four datasets (same
structural gap as NH). It also publishes no election year on the registry, so
`candidates.election_year` is derived here as the max `election_year` seen for
that `filer_id` across the disclosure files.

Party, and the rest of the registry gaps, are filled by an optional external
overlay — see `parsers/new_york_enrich.py`, fed by
`scrapers/new_york_party.py`, which joins NYSBOE's own election-results
database (results.elections.ny.gov, 1994–2025) and Open States onto the
registry on a strict name + office + district/year key. That overlay fills
`party` (pipe-delimited ballot lines, because NY's fusion voting means a
candidate genuinely has more than one), `incumbent`, and blank `district` /
`election_year`, and records `party_source` + `match_confidence` next to them.

The overlay is strictly optional. If its raw files are absent the parser logs
a warning and writes exactly the blanks it wrote before, so a NY parse never
depends on a third-party host being up. Its reach is also capped by the
source, not by the matcher: NYSBOE certifies statewide, congressional,
legislative and judicial contests, while town/village/city/school races are
certified by the 62 county boards and never enter the results database — and
those local offices are most of the 36,486-row candidates table.

Candidate <-> committee linkage
-------------------------------
NYSBOE assigns a candidate and their authorized committee two unrelated
`filer_id`s and publishes no join between them. Rather than leaving every
committee unlinked (PA's situation before its hand-verified override table),
`candidate_from_committee_name()` strips the conventional wrapper off a
committee name ("Friends Of Sheila Marcotte" -> "Sheila Marcotte",
"Joe Lhota For Mayor Inc" -> "Joe Lhota") and accepts the result ONLY if it
matches a registered candidate name exactly after normalization. Anything
ambiguous or unmatched is left blank — a missing link is much cheaper here
than a wrong one, since `utils.assign_committee_person_ids()` propagates
whatever lands in `candidate_name` straight into `person_id`.

person_id model
---------------
`committee`. `filer_id` is per-registration, not per-person: the filer
registry has, for example, seven distinct `filer_id`s all named
"Eric A. Ulrich" (verified in the dataset's own column statistics), because a
filer re-registers for each office/cycle. `assign_person_ids(id_model=
"committee")` collapses those into one `person_id` by taking the minimum
`filer_id` per (state, candidate_name, office, district).

Output (data/New York/cleaned/):
  contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
  committees.csv.gz, candidates.csv.gz
"""

import csv
import gzip
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger
from src.pipeline.parsers.new_york_enrich import NYEnrichment, name_keys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "New York" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "New York" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "NY"
EARLIEST_YEAR  = 1990                  # matches validate.py's plausibility floor
MAX_VALID_YEAR = date.today().year + 4  # matches validate.py's ceiling

# ============================== Schedules ==============================
# filing_sched_abbrev -> (target table, canonical transaction_type/record_type)
#
# Complete A–U map. Row counts in the comments are from the live API
# (2026-07) and are here to make it obvious which schedules actually carry
# volume if these mappings ever need revisiting.
#
# target is one of: "contributions", "expenditures", "loans_debts", None
# (None = detail schedule, counted but not written — see module docstring).
SCHEDULES: dict[str, tuple[str | None, str]] = {
    # ---- receipts -> contributions ----
    "A": ("contributions", "Monetary Contribution — Individual/Partnership"),  # 10,966,454
    "B": ("contributions", "Monetary Contribution — Corporate"),               #    852,284
    "C": ("contributions", "Monetary Contribution — Other"),                   #    933,445
    "D": ("contributions", "In-Kind Contribution"),                            #    100,309
    "E": ("contributions", "Other Receipt"),                                   #    106,282
    "G": ("contributions", "Transfer In"),                                     #     75,109
    "M": ("contributions", "Contribution Refunded"),                           #     94,516
    "P": ("contributions", "Housekeeping Receipt"),                            #    106,155
    "S": ("contributions", "Public Fund Receipt"),                             #      1,435
    # ---- disbursements -> expenditures ----
    "F": ("expenditures",  "Expenditure/Payment"),                             #  4,229,752
    "H": ("expenditures",  "Transfer Out"),                                    #     84,132
    "L": ("expenditures",  "Expenditure Refund"),                              #     43,189
    "Q": ("expenditures",  "Housekeeping Expense"),                            #    264,520
    "R": ("expenditures",  "Expense Allocation Among Candidates"),             #     49,959
    "T": ("expenditures",  "Qualified Expenditure"),                           #     74,649
    "U": ("expenditures",  "Public Fund Repayment"),                           #         43
    # ---- loans and liabilities -> loans_debts ----
    "I": ("loans_debts",   "Loan Received"),                                   #     20,345
    "J": ("loans_debts",   "Loan Repayment"),                                  #     10,504
    "K": ("loans_debts",   "Liability/Loan Forgiven"),                         #      9,666
    "N": ("loans_debts",   "Outstanding Liability/Loan"),                      #    174,611
    # ---- detail schedule, intentionally not written ----
    "O": (None,            "LLC/Partnership/Subcontractor Detail"),            #    160,842
}

# Committee-name wrappers stripped by candidate_from_committee_name(). Ordered
# longest-first within each group so "Committee To Re-Elect" wins over
# "Committee To" would-be prefixes. All observed against real NY committee
# names ("Friends Of ...", "Elect ...", "... For Mayor Inc").
_NAME_PREFIXES = [
    "committee to re-elect", "committee to reelect", "committee to elect",
    "friends and neighbors of", "friends of", "friends for", "the friends of",
    "citizens for", "neighbors for", "supporters of", "people for",
    "residents for", "taxpayers for", "voters for", "team",
    "re-elect", "reelect", "elect",
]

# Trailing office/vehicle words. "for <office>" is handled separately by regex
# because the office phrase itself is open-ended ("For Mayor", "For State
# Senate", "For The 45Th District", ...).
# No "inc." entry: the loop strips trailing punctuation before comparing, so
# "inc" already covers "Inc." — a separate entry would be dead.
_NAME_SUFFIXES = [
    "campaign committee", "election committee", "committee inc", "campaign inc",
    "committee", "campaign", "inc", "llc", "fund",
]

_FOR_OFFICE_RE = re.compile(r"\s+for\s+.*$", re.IGNORECASE)
_TRAILING_YEAR_RE = re.compile(r"\s+(19|20)\d{2}$")

# NY's two transfer_type_desc values are full sentences ("Type 1-Between a
# party or constituted committee and a candidate or a candidate's authorized
# committe" — the truncation and typo are in the source). Only the type number
# is meaningful, and keeping the sentence would make transaction_type
# effectively unmappable in src/aliases/transaction_categories.csv.
_TRANSFER_SHORT = re.compile(r"^\s*(Type\s*\d+)", re.IGNORECASE)


# ============================== Helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Parse a dollar amount string to a plain numeric string; parentheses become
    negative. Returns '' on failure.

    NY's Socrata number columns come through as bare decimal strings ("150",
    "521.83"), but the currency/paren handling is kept for robustness — refund
    schedules (L, M) have historically carried signed values in the source."""
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
    """Socrata floating timestamp 'YYYY-MM-DDTHH:MM:SS.000' -> 'YYYY-MM-DD'.

    Returns '' on failure or on an implausible year, so a data-entry typo in
    sched_date can't push a row outside validate.py's date range and fail the
    whole state."""
    v = (val or "").strip()
    if not v:
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", v)
    if not m:
        return ""
    try:
        d = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return ""
    if d.year < EARLIEST_YEAR or d.year > MAX_VALID_YEAR:
        return ""
    return d.strftime("%Y-%m-%d")


def parse_year(val: str) -> str:
    """Return a 4-digit year string, or '' if the value isn't one."""
    v = clean(val)
    return v if re.fullmatch(r"\d{4}", v) else ""


def entity_name(row: dict) -> str:
    """Build the counterparty name for a disclosure row.

    NY splits the other side of a transaction across two mutually exclusive
    shapes: an organization puts everything in `flng_ent_name`, an individual
    fills `flng_ent_first_name`/`_middle_name`/`_last_name` and leaves
    `flng_ent_name` empty. Organizations win when both are somehow present."""
    org = clean(row.get("flng_ent_name"))
    if org:
        return org
    parts = [clean(row.get("flng_ent_first_name")),
             clean(row.get("flng_ent_middle_name")),
             clean(row.get("flng_ent_last_name"))]
    return " ".join(p for p in parts if p)


def treasurer_name(row: dict) -> str:
    """'First Middle Last' from the filer registry's three treasurer columns."""
    parts = [clean(row.get("treasurer_first_name")),
             clean(row.get("treasurer_middle_name")),
             clean(row.get("treasurer_last_name"))]
    return " ".join(p for p in parts if p)


def split_name(raw: str) -> tuple[str, str]:
    """'First [Middle] Last' -> (first_middle, last).

    NY filer names are already in First-Last order (no comma inversion needed),
    but frequently carry a double space where a middle name would go
    ("Mercedes  Vazquez Simmons") — utils.clean_name collapses that before this
    runs, so the naive split is safe."""
    name = utils.clean_name(raw)
    parts = name.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def jurisdiction_of(row: dict) -> str:
    """Best available geographic scope for a filer.

    `municipality_desc_subdivision` is the most specific ("Manlius,Town"),
    `county_desc` the fallback, `filer_type_desc` ("State"/"County") the last
    resort so statewide filers aren't left blank."""
    return (clean(row.get("municipality_desc_subdivision"))
            or clean(row.get("county_desc"))
            or clean(row.get("filer_type_desc")))


def candidate_from_committee_name(name: str) -> str:
    """Extract the candidate's name from a committee name, or '' if the name
    doesn't follow one of the conventional patterns.

    Handles the three shapes that dominate NY committee names:
        "Friends Of Sheila Marcotte"   -> "SHEILA MARCOTTE"
        "Elect Jennifer Stevenson"     -> "JENNIFER STEVENSON"
        "Joe Lhota For Mayor Inc"      -> "JOE LHOTA"

    The result is only a *candidate* for matching — run() accepts it solely if
    it exactly matches a registered candidate name, so a bad extraction turns
    into a blank link, never a wrong one."""
    n = utils.clean_name(name)
    if not n:
        return ""

    low = n.lower()
    for prefix in _NAME_PREFIXES:
        if low.startswith(prefix + " "):
            n = n[len(prefix) + 1:]
            break

    # "<Name> For <Office>" — drop everything from " for " onward.
    n = _FOR_OFFICE_RE.sub("", n)

    # Trailing vehicle words / years, applied repeatedly: "... Campaign
    # Committee Inc 2022" needs three passes.
    #
    # The trailing-punctuation strip has to happen on `n` itself, not just on
    # the lowercased copy used for the comparison — slicing `len(suffix)` off a
    # string that still has a trailing "." while matching against one that
    # doesn't eats a character of the name ("John Smith Fund." -> "JOHN SMITH F").
    changed = True
    while changed:
        changed = False
        n = _TRAILING_YEAR_RE.sub("", n).rstrip(" .,")
        low = n.lower()
        for suffix in _NAME_SUFFIXES:
            if low.endswith(" " + suffix):
                n = n[: len(n) - len(suffix)].rstrip(" .,")
                changed = True
                break

    n = utils.clean_name(n)
    # A person's name is two or three tokens. One token is almost always a
    # party/organization fragment ("Democrats"), four or more is almost always
    # an org name that survived stripping.
    tokens = n.split()
    if not (2 <= len(tokens) <= 3):
        return ""
    return n


def contribution_subtype(row: dict) -> str:
    """Short, bounded sub-type qualifier for a receipt row, or ''.

    Deliberately bounded: `transaction_type` is one of the columns
    src/aliases/transaction_categories.csv maps per (state, raw value), so its
    cardinality has to stay small enough to enumerate by hand. The three
    sources here contribute at most 3 (schedule D's cntrbn_type_desc),
    4 (schedule E's receipt_type_desc) and 2 (schedule G's transfer_type_desc,
    shortened to "Type 1"/"Type 2") extra values respectively — about 29
    distinct transaction_type strings for NY in total."""
    sub = clean(row.get("cntrbn_type_desc")) or clean(row.get("receipt_type_desc"))
    if sub:
        return sub
    transfer = clean(row.get("transfer_type_desc"))
    if transfer:
        m = _TRANSFER_SHORT.match(transfer)
        return m.group(1).title().replace("  ", " ") if m else ""
    return ""


def raw_files(pattern: str) -> list[Path]:
    """Non-empty raw files matching a glob, sorted by name. The disclosure
    pattern matches both the per-year files and the '_misc.csv' catch-all."""
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


# ================================ Main ================================
def run():
    log = get_logger("new york", "parse")
    t0  = time.perf_counter()
    log.info("Starting New York parser")
    log._emit("parse_started")

    candidates: dict[str, dict] = {}   # filer_id -> candidates row
    committees: dict[str, dict] = {}   # filer_id -> committees row

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    skipped_detail      = 0   # schedule O rows, deliberately not written
    skipped_unknown     = 0   # schedule letters not in SCHEDULES (future-proofing)
    unknown_scheds: set[str] = set()

    file_handles = []

    # =================== Registry loading ===================
    def load_registry(path: Path, overlay: bool = False) -> int:
        """Read a filer-registry CSV into the candidates/committees dicts.

        overlay=True marks the ActiveCandidates/ActiveCommittees snapshots:
        those only ever *enrich* (they force active=1 on committees and fill
        blanks), they never downgrade a record already built from Filers.csv."""
        seen = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                filer_id = clean(row.get("filer_id"))
                if not filer_id:
                    continue
                seen += 1
                kind      = clean(row.get("compliance_type_desc")).upper()
                name      = clean(row.get("filer_name"))
                is_active = clean(row.get("filer_status")).upper() == "ACTIVE"

                # Every row observed in all three live datasets carries
                # compliance_type_desc, but it's the field the whole split
                # hinges on, so fall back to the presence of the committee-only
                # column rather than dropping the row.
                if not kind:
                    kind = "COMMITTEE" if clean(row.get("committee_type_desc")) else "CANDIDATE"

                if kind == "CANDIDATE":
                    cand = candidates.get(filer_id)
                    if cand is None:
                        first, last = split_name(name)
                        candidates[filer_id] = {
                            "state":           STATE,
                            "candidate_name":  utils.clean_name(name),
                            "candidate_first": first,
                            "candidate_last":  last,
                            "office":          clean(row.get("office_desc")),
                            "district":        clean(row.get("district")),
                            "jurisdiction":    jurisdiction_of(row),
                            # party/incumbent are not published by NYSBOE at
                            # all; §4a fills what the external overlay can
                            # reach and leaves the rest blank.
                            "party":            "",
                            "party_source":     "",
                            "match_confidence": "",
                            "election_year":   "",   # derived from disclosure rows
                            "incumbent":       "",
                            "state_filer_id":  filer_id,
                            "raw_file":        path.name,
                            "row_num":         row_num,
                        }
                    else:
                        _fill(cand, "office",       clean(row.get("office_desc")))
                        _fill(cand, "district",     clean(row.get("district")))
                        _fill(cand, "jurisdiction", jurisdiction_of(row))
                else:
                    cmte = committees.get(filer_id)
                    if cmte is None:
                        committees[filer_id] = {
                            "state":           STATE,
                            "committee_name":  utils.clean_name(name),
                            "committee_type":  clean(row.get("committee_type_desc")),
                            "election_year":   "",   # not published on the registry
                            "candidate_name":  "",   # filled by the linkage pass below
                            "treasurer_name":  utils.clean_name(treasurer_name(row)),
                            "city":            utils.clean_name(row.get("city")),
                            "zip":             utils.clean_zip(clean(row.get("zipcode"))),
                            "active":          "1" if is_active else "0",
                            "state_filer_id":  filer_id,
                            "raw_file":        path.name,
                            "row_num":         row_num,
                        }
                    else:
                        _fill(cmte, "committee_type", clean(row.get("committee_type_desc")))
                        _fill(cmte, "treasurer_name", utils.clean_name(treasurer_name(row)))
                        _fill(cmte, "city",           utils.clean_name(row.get("city")))
                        _fill(cmte, "zip",            utils.clean_zip(clean(row.get("zipcode"))))
                        if overlay and is_active:
                            # The active-committees snapshot is the more
                            # current of the two sources — let it flip a
                            # stale "0" from Filers.csv back to "1".
                            cmte["active"] = "1"
        return seen

    try:
        # =================== 0. External enrichment overlay ===================
        # Loaded first because the committee-linkage pass in §2 consults its
        # name universe. Absent files are a warning, never an error — see the
        # module docstring on why this must not be load-bearing.
        enrich = NYEnrichment.load(RAW_DIR)
        if enrich.available:
            log.info("Loaded party/office enrichment overlay "
                     f"({RAW_DIR / 'ElectionStats_Contests.csv'})")
        else:
            log.warning("  No enrichment overlay found in "
                        f"{RAW_DIR} — party/incumbent will be blank. "
                        "Run scrapers/new_york_party.py to populate them.")

        # =================== 1. Entity registries ===================
        # Loaded before any transaction file so every transaction row can look
        # up its filer's office/kind as it's written, in a single pass.
        for stem, overlay in (("Filers.csv", False),
                              ("ActiveCandidates.csv", True),
                              ("ActiveCommittees.csv", True)):
            path = RAW_DIR / stem
            if not path.exists() or path.stat().st_size == 0:
                log.warning(f"  {stem} missing or empty — skipping registry load")
                continue
            n = load_registry(path, overlay=overlay)
            # registry_loaded already emits its own file_parsed event with
            # role="registry" — a second one here would double-count the file
            # in the run report. It also hardcodes duration_s, so only bytes
            # can be passed through.
            log.registry_loaded(stem, entries=n,
                                relation="candidates+committees",
                                bytes=path.stat().st_size)

        # Name -> filer_id index for the committee linkage pass and for
        # stamping candidate_name/office onto transaction rows.
        cand_by_id   = candidates
        name_to_cand: dict[str, list[str]] = {}
        for fid, cand in candidates.items():
            name_to_cand.setdefault(cand["candidate_name"], []).append(fid)

        # =================== 2. Committee -> candidate linkage ===================
        # Only accept an extracted name that resolves to a registered candidate.
        def link_committees() -> tuple[int, int]:
            """Fill blank committees.candidate_name from the committee name.

            Returns (registry_links, results_links). Re-runnable: committees
            already carrying a candidate_name are left alone, so this can be
            called again after the transaction pass picks up any committee
            that was missing from the registry.

            Two acceptance tests, tried in that order:

              1. the extracted name is a registered NY candidate filer — the
                 original rule, and the only one that can produce a person_id;
              2. failing that, the extracted name is someone NYSBOE's election
                 results database has on a ballot.

            Test 2 exists because plenty of NY committees are authorized for a
            candidate who never registered a *candidate* filer_id of their own
            (they file solely through the committee), leaving the committee
            permanently unlinked under test 1 alone. It cannot introduce a
            wrong person_id: assign_committee_person_ids() only assigns one
            when candidate_name matches a row in the candidates table, so a
            results-only link resolves to a named committee with a NULL
            person_id — strictly more information than the blank it replaces.
            """
            n_registry = n_results = 0
            for cmte in committees.values():
                if cmte.get("candidate_name"):
                    continue
                guess = candidate_from_committee_name(cmte["committee_name"])
                if not guess:
                    continue
                if guess in name_to_cand:
                    cmte["candidate_name"] = guess
                    n_registry += 1
                elif enrich.available and name_keys(guess)[0] in enrich.known_names:
                    cmte["candidate_name"] = guess
                    n_results += 1
            return n_registry, n_results

        linked, linked_results = link_committees()

        # =================== 3. Transactions ===================
        # Each handle is appended as it's created rather than in one list
        # literal afterwards: if the second open_writer() raises, the first
        # handle would otherwise never reach the finally block.
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        file_handles.append(cont_fh)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        file_handles.append(expn_fh)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles.append(loan_fh)

        # Tracks the latest election_year seen per candidate filer_id, used to
        # fill candidates.election_year (absent from the registry datasets).
        latest_year: dict[str, int] = {}

        for path in raw_files("Disclosure_*.csv"):
            ft = time.perf_counter()
            n_cont = n_expn = n_loan = 0

            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    sched = clean(row.get("filing_sched_abbrev")).upper()
                    target, label = SCHEDULES.get(sched, ("__unknown__", ""))

                    if target == "__unknown__":
                        # A schedule letter NYSBOE has added since this map was
                        # built. Counted and surfaced rather than crashing or
                        # being silently swallowed.
                        skipped_unknown += 1
                        unknown_scheds.add(sched)
                        continue
                    if target is None:
                        skipped_detail += 1
                        continue

                    amount = parse_amount(row.get("org_amt"))
                    if not amount:
                        amount = parse_amount(row.get("owed_amt"))
                    if not amount:
                        continue

                    filer_id  = clean(row.get("filer_id"))
                    cmte_name = utils.clean_name(row.get("cand_comm_name"))
                    ey        = parse_year(row.get("election_year"))
                    txn_date  = parse_date(row.get("sched_date")) or parse_date(row.get("org_date"))
                    amended   = "1" if clean(row.get("r_amend")).upper() == "Y" else "0"

                    # Resolve the filer to a candidate (directly, or through
                    # its committee's heuristic link) for candidate_name/office.
                    cand = cand_by_id.get(filer_id)
                    if cand is not None:
                        cand_name = cand["candidate_name"]
                        office    = cand["office"]
                    else:
                        cmte = committees.get(filer_id)
                        cand_name = cmte["candidate_name"] if cmte else ""
                        linked_ids = name_to_cand.get(cand_name, []) if cand_name else []
                        office = candidates[linked_ids[0]]["office"] if linked_ids else ""

                    # Schedule R rows describe a *different* candidate than the
                    # filer (the one an allocated expense is attributed to), so
                    # prefer the row's own office/district when present.
                    if clean(row.get("office_desc")):
                        office = clean(row.get("office_desc"))

                    # candidates.election_year isn't published on any of the
                    # registry datasets, so it's derived as the latest cycle
                    # this candidate has activity in — counting both money
                    # filed under their own candidate filer_id and money filed
                    # by a committee that resolved to them above.
                    if ey:
                        if cand is not None:
                            target_cand_ids = (filer_id,)
                        elif cand_name:
                            target_cand_ids = tuple(name_to_cand.get(cand_name, ()))
                        else:
                            target_cand_ids = ()
                        y = int(ey)
                        for cid in target_cand_ids:
                            if y > latest_year.get(cid, 0):
                                latest_year[cid] = y

                    # An unregistered filer_id shouldn't happen (Filers.csv is
                    # the full historical registry) but if NYSBOE's extracts
                    # drift, register the committee from the transaction row so
                    # the money is still attributable to a named entity.
                    if filer_id and filer_id not in candidates and filer_id not in committees:
                        committees[filer_id] = {
                            "state":          STATE,
                            "committee_name": cmte_name,
                            "committee_type": "",
                            "election_year":  ey,
                            "candidate_name": "",
                            "treasurer_name": "",
                            "city":           "",
                            "zip":            "",
                            "active":         "",
                            "state_filer_id": filer_id,
                            "raw_file":       path.name,
                            "row_num":        row_num,
                        }

                    if target == "contributions":
                        # Schedule D's cntrbn_type_desc ("Services/Facilities
                        # Provided", "Property Given", "Campaign Expenses
                        # Paid") and E/G's receipt/transfer subtype refine the
                        # schedule-level label where the source provides one.
                        subtype  = contribution_subtype(row)
                        txn_type = f"{label} — {subtype}" if subtype else label
                        cont_w.writerow({
                            "state":             STATE,
                            "committee_name":    cmte_name,
                            "amount":            amount,
                            "date":              txn_date,
                            "transaction_type":  txn_type,
                            "contributor_name":  utils.clean_name(entity_name(row)),
                            "contributor_type":  clean(row.get("cntrbr_type_desc")),
                            "contributor_city":  utils.clean_name(row.get("flng_ent_city")),
                            "contributor_state": clean(row.get("flng_ent_state")),
                            "contributor_zip":   utils.clean_zip(clean(row.get("flng_ent_zip"))),
                            # NY collects employer/occupation only on
                            # independent-expenditure contributor rows; every
                            # other schedule leaves both blank at the source.
                            "employer":          utils.clean_name(row.get("ie_cntrbr_emp")),
                            "occupation":        utils.clean_name(row.get("ie_cntrbr_occ")),
                            "candidate_name":    cand_name,
                            "office":            office,
                            "election_year":     ey,
                            "amended":           amended,
                            "filing_id":         clean(row.get("trans_number")),
                            "raw_file":          path.name,
                            "row_num":           row_num,
                        })
                        n_cont += 1

                    elif target == "expenditures":
                        expn_w.writerow({
                            "state":            STATE,
                            "committee_name":   cmte_name,
                            "amount":           amount,
                            "date":             txn_date,
                            "transaction_type": label,
                            "payee_name":       utils.clean_name(entity_name(row)),
                            "purpose":          clean(row.get("trans_explntn")),
                            "category":         clean(row.get("purpose_code_desc")),
                            "payee_city":       utils.clean_name(row.get("flng_ent_city")),
                            "payee_state":      clean(row.get("flng_ent_state")),
                            "payee_zip":        utils.clean_zip(clean(row.get("flng_ent_zip"))),
                            "candidate_name":   cand_name,
                            "office":           office,
                            "election_year":    ey,
                            "amended":          amended,
                            "filing_id":        clean(row.get("trans_number")),
                            "raw_file":         path.name,
                            "row_num":          row_num,
                        })
                        n_expn += 1

                    else:  # loans_debts
                        # loan_other_desc ("Candidate", "Bank", "Other
                        # Entities", ...) is the lender category on schedules
                        # I/N and is the only thing distinguishing a candidate
                        # self-loan from a bank loan.
                        lender_kind = clean(row.get("loan_other_desc"))
                        loan_w.writerow({
                            "state":              STATE,
                            "committee_name":     cmte_name,
                            "original_amount":    amount,
                            "date":               txn_date,
                            "record_type":        f"{label} — {lender_kind}" if lender_kind else label,
                            "counterparty_name":  utils.clean_name(entity_name(row)),
                            "counterparty_city":  utils.clean_name(row.get("flng_ent_city")),
                            "counterparty_state": clean(row.get("flng_ent_state")),
                            "counterparty_zip":   utils.clean_zip(clean(row.get("flng_ent_zip"))),
                            "candidate_name":     cand_name,
                            "election_year":      ey,
                            "amended":            amended,
                            "filing_id":          clean(row.get("trans_number")),
                            "raw_file":           path.name,
                            "row_num":            row_num,
                        })
                        n_loan += 1

            elapsed = time.perf_counter() - ft
            size    = path.stat().st_size
            # One event per relation, emitted unconditionally so every source
            # file appears in the report even if it yielded nothing for a given
            # relation (the §5 checklist requires a file_parsed per source
            # file). `bytes` is attached only to the first of the three —
            # repeating it would triple-count the file's size in the report's
            # byte totals.
            log.file_parsed(path.name, "contributions", n_cont,
                            duration_s=elapsed, bytes=size)
            log.file_parsed(path.name, "expenditures", n_expn,
                            duration_s=elapsed)
            log.file_parsed(path.name, "loans_debts", n_loan,
                            duration_s=elapsed)
            total_contributions += n_cont
            total_expenditures  += n_expn
            total_loans         += n_loan

        # =================== 4. Flush candidates + committees ===================
        for fid, year in latest_year.items():
            cand = candidates.get(fid)
            if cand is not None:
                cand["election_year"] = str(year)

        # ============ 4a. Apply the external overlay to candidates ============
        # Runs after election_year is derived so the matcher has a year to key
        # on, and only ever *fills* — a value NYSBOE published always wins over
        # one we inferred, for district and election_year alike.
        n_party = n_incumbent = n_district = n_year = 0
        conf_counts: dict[str, int] = {}
        if enrich.available:
            for cand in candidates.values():
                hit = enrich.lookup(cand["candidate_name"], cand["office"],
                                    cand["district"], cand["election_year"])
                if not hit:
                    continue
                if not cand.get("party"):
                    cand["party"]            = hit["party"]
                    cand["party_source"]     = hit["party_source"]
                    cand["match_confidence"] = hit["match_confidence"]
                    conf_counts[hit["match_confidence"]] = \
                        conf_counts.get(hit["match_confidence"], 0) + 1
                    n_party += 1
                if hit["incumbent"] and not cand.get("incumbent"):
                    cand["incumbent"] = hit["incumbent"]
                    n_incumbent += 1
                if hit["district"] and not cand.get("district"):
                    cand["district"] = hit["district"]
                    n_district += 1
                if hit["election_year"] and not cand.get("election_year"):
                    cand["election_year"] = hit["election_year"]
                    n_year += 1

            cov = enrich.coverage_report(c["office"] for c in candidates.values())
            log.info(
                f"  Enrichment: party {n_party:,} "
                f"(exact {conf_counts.get('exact', 0):,} / "
                f"high {conf_counts.get('high', 0):,}), "
                f"incumbent {n_incumbent:,}, district {n_district:,}, "
                f"election_year {n_year:,}"
            )
            # Stated explicitly so a low party fill rate reads as a coverage
            # ceiling in NYSBOE's results database rather than as a matcher
            # that isn't working.
            log.info(
                f"  Enrichment scope: {cov['candidates_in_scope']:,} of "
                f"{len(candidates):,} candidates hold an office the results "
                f"database covers; {cov['candidates_out_of_scope']:,} hold "
                f"local/other offices it never contains"
            )
            if cov["party_conflicts"]:
                log.warning(f"  {cov['party_conflicts']:,} candidates left blank "
                            f"because NYSBOE results and Open States named "
                            f"different parties for the same seat")
            log.enrichment_summary(relation="candidates", matched=n_party,
                                   total=len(candidates),
                                   method="NYSBOE election results + Open States "
                                          "→ party (strict name+office+district/year)")

        # Committees synthesized during the transaction pass (filer_ids missing
        # from the registry) were created after the first linkage pass, so give
        # them the same chance at a candidate link before they're written.
        again_registry, again_results = link_committees()
        linked         += again_registry
        linked_results += again_results
        log.enrichment_summary(relation="committees",
                               matched=linked + linked_results,
                               total=len(committees),
                               method="committee_name → candidate_name heuristic "
                                      f"({linked:,} via filer registry, "
                                      f"{linked_results:,} via election results)")

        cand_fh, cand_w = open_writer("candidates.csv.gz", C.CANDIDATES)
        file_handles.append(cand_fh)
        cmte_fh, cmte_w = open_writer("committees.csv.gz", C.COMMITTEES)
        file_handles.append(cmte_fh)

        for row in candidates.values():
            cand_w.writerow(row)
        for row in committees.values():
            cmte_w.writerow(row)

        # Close handles before person-ID assignment — those helpers reopen and
        # rewrite the files in place.
        for fh in file_handles:
            fh.close()
        file_handles = []

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        if unknown_scheds:
            log.warning(f"  Unmapped filing_sched_abbrev values seen "
                        f"({skipped_unknown} rows): {sorted(unknown_scheds)} — "
                        f"add them to SCHEDULES")

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
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees),
                  candidates=len(candidates), skipped_detail=skipped_detail,
                  skipped_unknown=skipped_unknown,
                  committees_linked=linked + linked_results,
                  committees_linked_registry=linked,
                  committees_linked_results=linked_results,
                  enrichment_available=enrich.available,
                  party_filled=n_party, incumbent_filled=n_incumbent,
                  district_filled=n_district, election_year_filled=n_year)

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
