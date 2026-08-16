"""
parsers/vermont.py — Parse Vermont campaign finance data into the canonical
cleaned schema.

Input files (data/Vermont/raw/), all produced by scrapers/vermont.py:

    contributions_{YYYY}.csv        bulk Download Data export, TCON, one per
                                    closed filing year (2014-2025 as of the
                                    2026-08-12 snapshot). Contributions,
                                    in-kind, and loans received/forgiven.
    expenditures_{YYYY}.csv         bulk Download Data export, TEXP, one per
                                    closed filing year. Monetary, in-kind,
                                    loan payments, and mass media.
    grid_contributions_{from}_{to}[_amt{lo}-{hi}].csv
    grid_expenditures_{from}_{to}[_amt{lo}-{hi}].csv
                                    browse-grid chunks for years the bulk
                                    export doesn't publish (the open year).
                                    Windows are disjoint by construction.
    candidates.csv                  candidate roster (CandidatePublicGrid)
    committees.csv                  committee roster (CommitteePublicGrid)

Output (data/Vermont/cleaned/):
    contributions.csv.gz, expenditures.csv.gz, candidates.csv.gz,
    committees.csv.gz, loans_debts.csv.gz

Two source shapes, one parser
─────────────────────────────
Vermont publishes the same transactions through two different exports, and
this parser reads both. The bulk export is the vendor's per-filing-year file;
the browse grid is a CSV rendering of the on-screen table, which carries the
grid's own column labels ("Candidate/Committee Name", "Contribution Type",
"Election Cycle") rather than the bulk file's. Rather than maintain two
parsers, every column is resolved by *logical* name through the alias tables
below, which list the labels both shapes are known or expected to use.

All labels below are confirmed against real downloaded files (2026-08-12) and
against the state's own "Download Data Key" PDFs for both bulk feeds. The two
shapes differ more than they look:

    bulk  26/27 cols. Filing Entity Id/Name, Committee Name, Registration
          Type, Transaction Id, Transaction Type, Transaction Subtype, Goods
          Donated/ Service Donated, Contributor|Payee Type/Last/First/Address
          Line 1-2/City/State/Zip, Transaction Date, Transaction Amount,
          Election Type, Election Year, Transaction Comments, Amended,
          Timed Report Name/Filed Date, Report Name, Report Filed Date.
    grid  19/23 cols. Entity Id (or Filing Entity Id), Committee Name,
          Candidate First/Last/Middle Name, Contributor|Payee Type/Name,
          Address Line1-2, City|Payee City, State Code|Payee State,
          Zip Code|Payee Zip Code, Transaction Date, Transaction Amount,
          Election Cycle, Filer Type, Contribution|Expenditure Type,
          Timed Report, Report Name, and for expenditures Expenditure
          Purpose, Candidate Mentioned, Public Question, Stance, Description.

Three differences matter enough to have caused real bugs, and each is handled
in a named helper rather than inline:

  - The filer's name lives in different columns per feed, and "Committee Name"
    is blank on EVERY candidate row in both. See _filer_identity().
  - "Transaction Type" is a coarse bucket; the real type is in "Transaction
    Subtype", which the grid feed doesn't publish at all. See _txn_type().
  - Only the bulk feed has "Transaction Id". See _row_identity().

A file whose required columns (a filer name, amount, date) can't be resolved
is not parsed at all — it raises a file_parse_error and is skipped rather than
emitting rows with blank keys. Every unmapped header is logged once per file,
minus an explicit IGNORED_HEADERS set, so a future column rename shows up as a
printed diagnostic instead of as quietly missing data.

Deduplication
─────────────
Both feeds ship exact duplicate rows, and both are deduplicated — but on
different keys, because using one key for both destroys data:

  bulk  keyed on Transaction Id. The id repeats within a file (~7% of rows);
        every repeated id was checked and the copies are byte-for-byte
        identical across all 26 columns, so they are genuine duplicate rows in
        Vermont's export and collapsing them is correct.
  grid  keyed on the entire raw row, since there is no id column. An earlier
        version fell back to (committee, contributor, date, amount, type),
        which is nowhere near unique — 210 different candidates each received
        a $5.00 sub-threshold contribution on 2026-02-06 — and collapsed
        ~11,400 distinct 2026 transactions into a few hundred. Whole-row
        identity removes only exact duplicates (6,406 of them, also real) and
        can never merge two distinct transactions.

Cross-source double-counting is prevented upstream instead: the scraper
deletes a year's grid chunks once its bulk file lands, so a year is only ever
present from one source.

id_model — decided at runtime
─────────────────────────────
The captured roster requests carry no filingEntityId filter, so whether the
roster CSV exposes a filer-id column at all is unknown until a real file is
read. The parser looks: if any candidate row resolves a real source id, it
uses "committee" grouping (person_id = min id per name/office/district, which
is correct for this platform — Idaho confirmed empirically that the vendor's
Filing Entity Id is per-registration, not per-person, so the same candidate
gets a new one each cycle). If no id column exists, it falls back to
"name_hash". Either way state_filer_id is always populated — with the real id
when there is one, otherwise with a stable name-derived surrogate — because
Vermont is marked has_filer_id=1 in states.csv and validate.py requires that
column to be non-null.

Roster coverage
───────────────
Both rosters are pulled with accountStatus="FACT" (active filers), which is
what the state's own search page sends. They are therefore a current snapshot,
not a historical one: a committee that deregistered in 2018 won't be in them.
Those filers are backfilled from the transaction files instead — name, filer
type and election year, with office/district/party/treasurer left genuinely
blank rather than guessed. Same roster-plus-backfill split New Hampshire and
Idaho use.

Data notes
──────────
  - Contributions under Vermont's itemization threshold are published with the
    contributor redacted, as the literal string "Under Threshold - Name
    Withheld". These are real transactions with real amounts and are kept;
    contributor_name is normalized to the empty string and contributor_type
    left blank rather than storing the placeholder as if it were a name.
  - The election year is a plain year in both feeds ("2024" in the bulk
    "Election Year", "2026" in the grid "Election Cycle"), but the on-screen
    grid renders it as "2026 Nov 03 - General Election", so election_year
    takes the leading four-digit year and works either way.
  - Loans are folded into the same two exports as ordinary money: "Loan
    Received"/"Loan Forgiven" arrive as contribution subtypes and "Loan
    Payment" as an expenditure subtype. Those rows are routed to
    loans_debts.csv.gz instead of contributions/expenditures, so the two money
    tables stay comparable with other states.
  - Loans are invisible in the 2026 (grid) data, and this is a source
    limitation, not a parsing one. The browse page renders a per-row
    "Contribution Type" of "Monetary Contribution" / "Loan Received", but its
    CSV export flattens that column to the literal "Contribution" on every
    row. There is no subtype anywhere in the grid feed, so a 2026 loan is
    indistinguishable from a 2026 contribution until the year closes and its
    bulk file (which does carry Transaction Subtype) is published.
  - Person names are published "LAST, FIRST MIDDLE". Candidate names are
    flipped to "FIRST MIDDLE LAST" so candidate_first/candidate_last are
    meaningful and so committees join to candidates on the same string.
    Contributor and payee names are NOT flipped — that column mixes people
    with organizations ("META PLATFORMS, INC"), and flipping on the comma
    would corrupt the organizations.
"""

import collections
import csv
import gzip
import hashlib
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# =============================== paths ================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Vermont" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Vermont" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "VT"

# Vermont's current system publishes from 2014 (the earliest filing year on
# the Download Data page). Dates outside this window are source errors —
# typo'd years like 0214 or 2202 — and are dropped rather than stored.
EARLIEST_YEAR  = 2014
MAX_VALID_YEAR = date.today().year + 2

# ========================= source vocabularies ========================
# Transaction types that are loan/debt activity rather than ordinary money.
# Taken from the Download Data page's own descriptions of the two files:
# "Contributions include Monetary Contributions, In-Kind Contributions and
# Loan Received. Loan Received information contains the Loan Received and any
# Loan Forgiven amount." / "Expenditures include Monetary Expenditures,
# In-Kind Expenditures, Loan Payments, and Mass Media Expenditures."
LOAN_INCOME_TYPES = {
    "loan received", "loan forgiven", "loan repayment received",
    "outstanding loan",
}
LOAN_EXPENSE_TYPES = {
    "loan payment", "loan repayment", "debt payment", "outstanding debt",
}

# Filer Type as rendered in the browse grids → committee_type. Observed values
# (2026-08-12 snapshot): Candidate, Political Action Committee, Political
# Party Committee. Anything unrecognized passes through unchanged so it shows
# up in the aggregate as an unmapped value rather than being coerced.
CANDIDATE_FILER_TYPES = {"candidate", "candidate committee"}

# The redaction placeholder Vermont writes in place of a sub-threshold
# contributor's name. Matched loosely — the grid renders it with a hyphen and
# spaces that may not survive every export.
REDACTED_NAME_RE = re.compile(r"under\s*threshold.*name\s*withheld", re.I)

# Corporate suffixes that mark a comma in a name as punctuation rather than a
# "LAST, FIRST" separator. Only consulted for candidate names, which are the
# only names this parser reformats.
CORPORATE_SUFFIXES = {
    "inc", "inc.", "llc", "l.l.c.", "llp", "ltd", "ltd.", "co", "co.",
    "corp", "corp.", "pc", "p.c.", "pa", "p.a.", "company", "incorporated",
}


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Parse a dollar amount to a plain numeric string. Returns '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]          # parentheses = negative
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """
    Normalize a date to YYYY-MM-DD. Returns '' on failure or implausible year.

    The two feeds disagree on format, and the bulk one carries a time:
        bulk   "6/9/2024 12:00:00 AM"   — no zero padding, midnight suffix
        grid   "06/30/2026"             — zero padded, date only
    The time component is always midnight and carries no information, so it is
    split off before parsing rather than being matched by a wider format list.
    strptime accepts unpadded %m/%d, so one format covers both.
    """
    v = (val or "").strip()
    if not v:
        return ""
    if "T" in v:                   # ISO datetime — keep the date part only
        v = v.split("T", 1)[0]
    elif " " in v:                 # "M/D/YYYY h:mm:ss AM" — drop the clock
        v = v.split(" ", 1)[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y", "%d-%b-%Y"):
        try:
            d = datetime.strptime(v, fmt)
        except ValueError:
            continue
        if d.year < EARLIEST_YEAR or d.year > MAX_VALID_YEAR:
            return ""
        return d.strftime("%Y-%m-%d")
    return ""


def clean_zip_field(val: str) -> str:
    """utils.clean_zip, tolerant of the float-ish ZIPs spreadsheets produce."""
    v = clean(val)
    if v.endswith(".0"):
        v = v[:-2]
    return utils.clean_zip(v)


def bool01(val: str) -> str:
    """Coerce a source truthiness flag to '1'/'0'. Blank stays blank."""
    v = clean(val).lower()
    if not v:
        return ""
    return "1" if v in ("1", "y", "yes", "true", "t") else "0"


def election_year(val: str) -> str:
    """
    Pull a four-digit year out of an Election Cycle label.

    Vermont renders the cycle as "2026 Nov 03 - General Election", so the year
    is the leading token — but a bare "2026" (as the bulk export may carry)
    works through the same regex.
    """
    m = re.search(r"(19|20)\d{2}", clean(val))
    if not m:
        return ""
    yr = int(m.group(0))
    return str(yr) if EARLIEST_YEAR - 4 <= yr <= MAX_VALID_YEAR else ""


def surrogate_id(name: str) -> str:
    """
    Stable 9-digit surrogate filer id derived from a name.

    Used only when the source exposes no id column for an entity. Deterministic
    across runs, so re-parsing doesn't churn person_id. Kept to 9 digits so it
    stays well inside the 12-digit budget utils._make_person_id pads to.
    """
    key = f"{STATE}|{utils.clean_name(name)}".encode()
    return str(int(hashlib.md5(key).hexdigest(), 16) % 1_000_000_000)


def split_person_name(raw: str) -> tuple[str, str, str]:
    """
    Normalize a person name to (display, first, last).

    Vermont publishes people as "COPELAND HANZAS, SARAH LOUISE" — surname
    first, which may itself be multi-word. The comma is the authoritative
    split, so "COPELAND HANZAS" stays intact as the surname and the display
    form becomes "SARAH LOUISE COPELAND HANZAS".

    A comma followed by a corporate suffix ("META PLATFORMS, INC") is
    punctuation, not a name inversion, and is left alone — this function is
    only applied to candidate names, but the guard keeps an organization
    filing as a candidate from being mangled.
    """
    v = utils.clean_name(raw)
    if not v:
        return "", "", ""

    if "," in v:
        last, rest = v.split(",", 1)
        last, rest = last.strip(), rest.strip()
        tail = rest.split()[0].lower().rstrip(".") if rest else ""
        if rest and tail not in CORPORATE_SUFFIXES:
            display = utils.clean_name(f"{rest} {last}")
            given   = rest.split()
            return display, (given[0] if given else ""), last

    tokens = v.split()
    if len(tokens) == 1:
        return v, "", v
    return v, tokens[0], tokens[-1]


def contributor_display(raw: str) -> str:
    """
    Clean a contributor/payee name without reformatting it.

    That column mixes people ("MAHNKE, ERHARD") with organizations ("META
    PLATFORMS, INC"), and nothing in the row reliably says which — so the
    comma is left where the source put it. Sub-threshold redactions become
    empty rather than storing the placeholder as a name.
    """
    v = utils.clean_name(raw)
    if not v or REDACTED_NAME_RE.search(v):
        return ""
    return v


# ========================= header resolution ==========================

def _norm_header(h: str) -> str:
    h = (h or "").strip().lstrip("﻿")
    h = re.sub(r"\s*/\s*", "/", h)
    h = re.sub(r"[_]+", " ", h)
    h = re.sub(r"\s+", " ", h)
    return h.lower()


# Columns Vermont publishes that this parser deliberately drops, because the
# canonical schema has nowhere honest to put them. Listed explicitly so they
# don't show up in the "unmapped column" diagnostic — that warning is meant to
# catch a header rename, and it's useless if it cries wolf on every run.
#
#   Timed Report / Disclosure Report  which filing the row was reported on.
#                                     Free text ("2026 Aug 1 - Disclosures"),
#                                     not an amendment flag — `amended` is left
#                                     blank rather than inferred from it.
#   Used in Mass Media Activity       a flag alongside the expenditure type,
#                                     which already carries "Mass Media
#                                     Expenditure" where it applies.
#   Public Question / Stance          support/oppose of a ballot question, not
#                                     of a candidate. support_oppose means the
#                                     latter, so folding these in would make
#                                     the column mean two different things.
IGNORED_HEADERS = {
    # filing provenance — free text, not an amendment flag
    "timed report", "timed report name", "timed report filed date",
    "disclosure report", "report name", "report filed date",
    # street address; the schema keeps city/state/zip only
    "contributor address line 1", "contributor address line 2",
    "payee address line 1", "payee address line 2",
    "payee address line1", "payee address line2",
    "address line1", "address line2",
    "candidate address line1", "candidate address line2",
    "committee address",
    # free-text notes with no canonical home
    "transaction comments", "description", "other purpose comments",
    # "Election Type" is Primary/General, distinct from the election *year*
    # that election_cycle reads
    "election type",
    # counterparty classification — the schema types the contributor, not the payee
    "payee type",
    # ballot-question support/oppose. support_oppose means candidate
    # support/oppose, so folding these in would make one column mean two things
    "public question", "stance",
    "used in mass media activity",
    # roster extras: contact details and pre-aggregated totals the pipeline
    # recomputes from transactions rather than trusting
    "candidate email", "candidate phone", "candidate website",
    "candidate state", "candidate address line1", "candidate address line2",
    "treasurer middle name", "treasurer email", "treasurer phone",
    "treasurer address line1", "treasurer address line2",
    "treasurer city", "treasurer state", "treasurer zip code",
    "name of financial institution",
    "name of financial institution address line1",
    "name of financial institution address line2",
    "name of financial institution city",
    "name of financial institution state",
    "name of financial institution zip code",
    "committee website", "financial institution name",
    "abbreviated committee name",
    "total raised", "total spent", "total contributions",
    "total expenditures", "total expenditure", "balance",
}


def _resolve_headers(fieldnames, alias_map: dict) -> tuple[dict, list[str]]:
    """
    Map logical field names to the actual header strings in one file.

    Returns (resolved, unmapped) where resolved is {logical: header|None} and
    unmapped is every source header that neither a logical name nor
    IGNORED_HEADERS claimed — logged by the caller so a header rename shows up
    as a printed diagnostic on the first run against live data rather than as
    quietly missing values.
    """
    lookup = {_norm_header(h): h for h in (fieldnames or []) if h}
    resolved: dict[str, str | None] = {}
    claimed: set[str] = set()
    for logical, aliases in alias_map.items():
        found = None
        for alias in aliases:
            # Normalize the alias too, not just the header. Without this an
            # alias has to be pre-spelled in normalized form to match, which is
            # a silent trap: "goods donated/ service donated" never matched
            # "Goods Donated/ Service Donated", because _norm_header collapses
            # the spaces around the slash on one side of the comparison only.
            if _norm_header(alias) in lookup:
                found = lookup[_norm_header(alias)]
                break
        resolved[logical] = found
        if found:
            claimed.add(_norm_header(found))
    unmapped = [h for k, h in lookup.items()
                if k not in claimed and k not in IGNORED_HEADERS]
    return resolved, unmapped


def _get(row: dict, resolved: dict, logical: str) -> str:
    """Read one logical field out of a row, '' when the column isn't present."""
    col = resolved.get(logical)
    return clean(row.get(col)) if col else ""


# Set to True the first time any source file yields a real, non-empty filer id.
# Drives the id_model choice at the end of run() — see the module docstring.
# A dict rather than a bare global so the parse helpers can flip it without a
# `global` statement in each one.
FLAGS = {"real_filer_id": False}


def _filer_id(row: dict, resolved: dict, fallback_name: str) -> str:
    """
    Source filer id for a row, or a stable name-derived surrogate.

    Records whether a real id was ever seen, which is what decides between the
    "committee" and "name_hash" person-id models later.
    """
    fid = _get(row, resolved, "filer_id")
    if fid:
        FLAGS["real_filer_id"] = True
        return fid
    return surrogate_id(fallback_name)


# ---------------------------------------------------------------------
# Alias tables. First entry in each list is the label verified against the
# live browse grid (2026-08-12); the rest are the bulk export's expected
# labels, taken from the same vendor platform's exports in Idaho and New
# Hampshire. Unmatched headers are reported per file at parse time.
# ---------------------------------------------------------------------
CONTRIBUTION_ALIASES = {
    # The reporting filer. The two feeds name it differently and neither has
    # both: bulk carries "Filing Entity Name" ("BROCK, RANDY" for a candidate),
    # grid carries only "Committee Name" plus separate candidate name parts.
    # These are kept as SEPARATE logical fields — merging them into one alias
    # list is what produced blank committee_name on every 2026 candidate row,
    # because "Committee Name" resolved and is blank for candidate filers.
    "entity_name":        ["filing entity name", "filer name", "registrant name"],
    "campaign_name":      ["campaign name", "committee name"],
    "cand_first":         ["candidate first name"],
    "cand_middle":        ["candidate middle name"],
    "cand_last":          ["candidate last name"],

    "contributor_name":   ["contributor name", "source name"],
    "contributor_first":  ["contributor first name"],
    "contributor_middle": ["contributor middle name"],
    "contributor_last":   ["contributor last name"],
    "contributor_company": ["contributor company name", "contributor organization name"],
    "date":               ["transaction date", "transactiondate", "date of receipt"],
    "amount":             ["transaction amount", "amount of receipt", "amount"],
    # Transaction Type is the coarse bucket ("Contribution" on every row);
    # Transaction Subtype holds the real distinction (Monetary / In-Kind
    # (Non-Money) / Loan Received). Subtype is preferred at read time and Type
    # is the fallback — see _txn_type(). The grid feed has only the coarse
    # "Contribution Type", which is why 2026 loans can't be separated.
    "transaction_subtype": ["transaction subtype", "transaction sub type"],
    "transaction_type":   ["contribution type", "transaction type"],
    "goods_or_service":   ["goods donated/ service donated",
                           "goods donated / service donated",
                           "goods or service donated"],
    "election_cycle":     ["election year", "election cycle", "election"],
    "filer_type":         ["registration type", "filer type",
                           "filing entity type"],
    "contributor_type":   ["contributor type", "source type"],
    # "city"/"state code"/"zip code" are the grid feed's unprefixed contributor
    # address columns (positions 9-11, right after Contributor Name and the two
    # address lines). Listed last so the explicit bulk names always win.
    "contributor_city":   ["contributor address city", "contributor city",
                           "contributor town", "city"],
    "contributor_state":  ["contributor address state", "contributor state",
                           "state code"],
    "contributor_zip":    ["contributor address zip code", "contributor zip code",
                           "contributor zip", "zip code"],
    "employer":           ["contributor employer", "employer"],
    "occupation":         ["contributor occupation", "occupation"],
    "office":             ["office sought", "office"],
    "filer_id":           ["filing entity id", "entity id", "filer entity id",
                           "filer id", "registrant id"],
    "transaction_id":     ["transaction id", "transaction identifier"],
    "amended":            ["amended", "is amended", "amendment"],
}

EXPENDITURE_ALIASES = {
    "entity_name":     ["filing entity name", "filer name", "registrant name"],
    "campaign_name":   ["campaign name", "committee name"],
    "cand_first":      ["candidate first name"],
    "cand_middle":     ["candidate middle name"],
    "cand_last":       ["candidate last name"],

    "payee_name":      ["payee name", "payee/worker/creditor/loan source name"],
    "payee_first":     ["payee first name"],
    "payee_last":      ["payee last name"],
    "payee_company":   ["payee company name", "payee organization name"],
    "date":            ["transaction date", "transactiondate", "expenditure date"],
    "amount":          ["transaction amount", "amount", "expenditure amount"],
    "transaction_subtype": ["transaction subtype", "transaction sub type"],
    "transaction_type": ["expenditure type", "transaction type"],
    "goods_or_service": ["goods donated/ service donated",
                         "goods donated / service donated",
                         "goods or service donated"],
    # "Expenditure Purpose" is the grid's real purpose column ("ActBlue Fees",
    # "IT - Campaign Software"); "Description" is separate free text and must
    # NOT win, which it did while "description" sat in this list.
    "purpose":         ["expenditure purpose", "purpose"],
    "election_cycle":  ["election year", "election cycle", "election"],
    "filer_type":      ["registration type", "filer type", "filing entity type"],
    "payee_city":      ["payee address city", "payee city", "payee town"],
    "payee_state":     ["payee address state", "payee state"],
    "payee_zip":       ["payee address zip code", "payee zip code", "payee zip"],
    "office":          ["office sought", "office"],
    # Vermont's own independent-expenditure attribution. Present in the grid
    # feed and blank on every sampled row, but read anyway so the column starts
    # populating the moment the state fills it in.
    "candidate_mentioned": ["candidate mentioned"],
    "filer_id":        ["filing entity id", "entity id", "filer entity id",
                        "filer id", "registrant id"],
    "transaction_id":  ["transaction id", "transaction identifier"],
    "amended":         ["amended", "is amended", "amendment"],
}

# Roster exports. The captured requests expose these filters — filerName,
# politicalPartyCode, officeSought, officeType, town, election, electionYear,
# filingYear, treasurerName, accountStatus (candidates); filerName,
# committeeType, committeeSubType, treasurerName, politicalPartyCode,
# election, filingYear, publicQuestion, stance (committees) — so the exported
# columns are expected to mirror them. Every one is optional here: a roster
# missing a column degrades that field to blank, it doesn't fail the parse.
CANDIDATE_ROSTER_ALIASES = {
    "filer_name":     ["filer name", "candidate/committee name", "candidate name",
                       "name"],
    "candidate_first": ["candidate first name", "first name"],
    "candidate_middle": ["candidate middle name", "middle name"],
    "candidate_last": ["candidate last name", "last name"],
    "committee_name": ["campaign name", "committee name"],
    "office":         ["office sought", "office"],
    "office_type":    ["office type"],
    # Vermont's roster has no district column at all — "Town Or City" is the
    # jurisdiction the seat is in, not a numbered district. district therefore
    # stays blank, which weakens the (name, office, district) person_id
    # grouping key; see docs/states/vermont.md.
    "district":       ["district", "office district", "office district name"],
    "town":           ["town or city", "town", "county"],
    "city":           ["candidate city", "city"],
    "party":          ["political party", "party", "party affiliation"],
    "election":       ["election", "election cycle"],
    "election_year":  ["election year"],
    "filing_year":    ["filing year"],
    "treasurer":      ["treasurer name"],
    "treasurer_first": ["treasurer first name"],
    "treasurer_last": ["treasurer last name"],
    "zip":            ["candidate zip code", "zip code", "zip"],
    "status":         ["account status", "filer status", "status"],
    "filer_id":       ["filing entity id", "filer entity id", "filer id",
                       "filing entity identifier"],
}

COMMITTEE_ROSTER_ALIASES = {
    "filer_name":     ["filer name", "committee name", "candidate/committee name",
                       "name"],
    "committee_type": ["committee type"],
    "committee_subtype": ["committee sub type", "committee subtype"],
    "candidate_name": ["candidate name"],
    "party":          ["political party", "party"],
    "election":       ["election", "election cycle"],
    "election_year":  ["election year"],
    "filing_year":    ["filing year"],
    "treasurer":      ["treasurer name"],
    "treasurer_first": ["treasurer first name"],
    "treasurer_last": ["treasurer last name"],
    "town":           ["town", "town or city", "city"],
    "zip":            ["committee zip code", "zip code", "zip"],
    "status":         ["account status", "filer status", "status"],
    "filer_id":       ["filing entity id", "filer entity id", "filer id",
                       "filing entity identifier"],
}


# ============================ file access =============================

def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore",
                        restval="")
    w.writeheader()
    return fh, w


def open_source_csv(path: Path):
    """
    Open a raw export, skipping the vendor's title banner when present.

    Grid and roster exports lead with a one-cell line ("Contributions Download
    as of ...") before the real header; the bulk transaction exports don't.
    Detect on shape rather than wording, which differs per grid.

    Returns (file_handle, first_data_row_number) so row_num matches the
    physical line in the raw file, banner included.
    """
    fh = open(path, encoding="utf-8-sig", errors="replace", newline="")
    first = fh.readline()
    cells = next(csv.reader([first]), [])
    if len([c for c in cells if c.strip()]) <= 1:
        return fh, 3          # banner + header consumed → data starts at line 3
    fh.seek(0)
    return fh, 2              # header only → data starts at line 2


def raw_files(pattern: str) -> list[Path]:
    return sorted((f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
                  key=lambda p: p.name)


def _log_unmapped(log, path: Path, unmapped: list[str]) -> None:
    if unmapped:
        log.warning(f"  {path.name}: {len(unmapped)} unmapped column(s) — "
                    f"{', '.join(unmapped[:12])}"
                    f"{' …' if len(unmapped) > 12 else ''}")


# ========================== roster parsing ============================

def parse_candidate_roster(log, path: Path, candidates: dict, committees: dict,
                           registry: dict) -> int:
    """
    Read candidates.csv into the candidates/committees output dicts and build
    the lookup transactions are enriched from.

    The registry is keyed on BOTH the source's own "LAST, FIRST" string and
    the flipped display form, because transaction rows carry the former and
    everything downstream joins on the latter.
    """
    fh, start_row = open_source_csv(path)
    count = 0
    try:
        reader = csv.DictReader(fh)
        resolved, unmapped = _resolve_headers(reader.fieldnames,
                                              CANDIDATE_ROSTER_ALIASES)
        _log_unmapped(log, path, unmapped)

        if not resolved["filer_name"] and not resolved["candidate_last"]:
            log.file_parse_error(path.name,
                                 "no candidate-name column resolved — roster "
                                 "skipped, candidates will be backfilled from "
                                 "transactions only")
            return 0

        for row_num, row in enumerate(reader, start=start_row):
            first = _get(row, resolved, "candidate_first")
            last  = _get(row, resolved, "candidate_last")
            if first or last:
                mid     = _get(row, resolved, "candidate_middle")
                display = utils.clean_name(f"{first} {mid} {last}")
                raw_name = display
            else:
                raw_name = utils.clean_name(_get(row, resolved, "filer_name"))
                display, first, last = split_person_name(raw_name)
            if not display:
                continue

            fid = _filer_id(row, resolved, display)
            office   = _get(row, resolved, "office")
            district = _get(row, resolved, "district")
            party    = _get(row, resolved, "party")
            town     = _get(row, resolved, "town")
            ey       = (election_year(_get(row, resolved, "election_year"))
                        or election_year(_get(row, resolved, "election"))
                        or election_year(_get(row, resolved, "filing_year")))
            treasurer = _get(row, resolved, "treasurer") or utils.clean_name(
                f"{_get(row, resolved, 'treasurer_first')} "
                f"{_get(row, resolved, 'treasurer_last')}"
            )
            status = _get(row, resolved, "status").lower()
            active = "1" if status.startswith("act") else ("0" if status else "")

            key = f"cand|{fid}|{ey}"
            candidates[key] = {
                "state":           STATE,
                "candidate_name":  display,
                "candidate_first": first,
                "candidate_last":  last,
                "office":          office,
                "district":        district,
                "jurisdiction":    town,
                "party":           party,
                "election_year":   ey,
                "incumbent":       "",
                "state_filer_id":  fid,
                "raw_file":        path.name,
                "row_num":         row_num,
                # Local / Statewide / General Assembly / County. Not a canonical
                # output column — carried only to scope party matching below,
                # and dropped on write by extrasaction="ignore".
                "_office_type":    _get(row, resolved, "office_type"),
            }

            committee_name = (utils.clean_name(_get(row, resolved, "committee_name"))
                              or display)
            committees[key] = {
                "state":          STATE,
                "committee_name": committee_name,
                "committee_type": "Candidate",
                "election_year":  ey,
                "candidate_name": display,
                "treasurer_name": treasurer,
                "city":           utils.clean_name(
                    _get(row, resolved, "city") or town),
                "zip":            clean_zip_field(_get(row, resolved, "zip")),
                "active":         active,
                "state_filer_id": fid,
                "raw_file":       path.name,
                "row_num":        row_num,
            }

            entry = {"display": display, "office": office, "district": district,
                     "party": party, "town": town, "filer_id": fid}
            for alias in (raw_name, display):
                if alias:
                    registry.setdefault(alias, entry)
            count += 1
    finally:
        fh.close()
    return count


def parse_committee_roster(log, path: Path, committees: dict,
                           registry: dict) -> int:
    """Read committees.csv (PACs, party committees) into the committees dict."""
    fh, start_row = open_source_csv(path)
    count = 0
    try:
        reader = csv.DictReader(fh)
        resolved, unmapped = _resolve_headers(reader.fieldnames,
                                              COMMITTEE_ROSTER_ALIASES)
        _log_unmapped(log, path, unmapped)

        if not resolved["filer_name"]:
            log.file_parse_error(path.name,
                                 "no committee-name column resolved — roster "
                                 "skipped, committees will be backfilled from "
                                 "transactions only")
            return 0

        for row_num, row in enumerate(reader, start=start_row):
            name = utils.clean_name(_get(row, resolved, "filer_name"))
            if not name:
                continue

            fid = _filer_id(row, resolved, name)
            ey  = (election_year(_get(row, resolved, "election_year"))
                   or election_year(_get(row, resolved, "election"))
                   or election_year(_get(row, resolved, "filing_year")))
            treasurer = _get(row, resolved, "treasurer") or utils.clean_name(
                f"{_get(row, resolved, 'treasurer_first')} "
                f"{_get(row, resolved, 'treasurer_last')}"
            )
            status = _get(row, resolved, "status").lower()
            ctype  = (_get(row, resolved, "committee_type")
                      or _get(row, resolved, "committee_subtype"))

            committees[f"cmte|{fid}|{ey}"] = {
                "state":          STATE,
                "committee_name": name,
                "committee_type": ctype,
                "election_year":  ey,
                # A PAC is not a candidate's own committee — candidate_name is
                # deliberately blank so assign_committee_person_ids leaves
                # person_id NULL for it. Any support/oppose relationship to a
                # candidate belongs in src/registries/committees/VT.csv, which
                # enrich.py reads, not here.
                "candidate_name": "",
                "treasurer_name": treasurer,
                "city":           utils.clean_name(_get(row, resolved, "town")),
                "zip":            clean_zip_field(_get(row, resolved, "zip")),
                "active":         "1" if status.startswith("act") else ("0" if status else ""),
                "state_filer_id": fid,
                "raw_file":       path.name,
                "row_num":        row_num,
            }
            registry.setdefault(name, {"display": "", "office": "", "district": "",
                                       "party": _get(row, resolved, "party"),
                                       "town": _get(row, resolved, "town"),
                                       "filer_id": fid})
            count += 1
    finally:
        fh.close()
    return count


# ======================= transaction parsing ==========================

def _txn_type(row: dict, resolved: dict) -> str:
    """
    The transaction's real type.

    Vermont splits this across two columns. "Transaction Type" is the coarse
    bucket and is the literal string "Contribution" or "Expenditure" on
    essentially every row; "Transaction Subtype" carries what actually matters
    — Monetary Contribution, In-Kind (Non-Money) Contribution, Loan Received,
    Monetary Expenditure, In-Kind (Non-Money) Expenditure.

    Reading the coarse column is why loans_debts came out empty on the first
    full run: 147 "Loan Received" rows in 2024 alone all looked like plain
    contributions. Subtype wins; Type is the fallback for the grid feed, which
    publishes no subtype at all.
    """
    return (_get(row, resolved, "transaction_subtype")
            or _get(row, resolved, "transaction_type"))


def _filer_identity(row: dict, resolved: dict) -> tuple[str, str, bool]:
    """
    Resolve (committee_name, candidate_name, is_candidate) for one row.

    The two feeds describe the filer differently and neither carries the
    other's columns:

        bulk  Filing Entity Name = "BROCK, RANDY"   (the person, for a
              candidate) and Committee Name blank on every candidate row
        grid  Committee Name blank on every candidate row, with the person in
              Candidate First / Middle / Last Name

    So a single "committee name" alias list resolves to a blank column on the
    grid feed, which is exactly what happened: every 2026 candidate row got a
    blank committee_name, and with the old composite dedup key that collapsed
    hundreds of unrelated candidates' sub-threshold contributions into one row.

    For a candidate filer, Vermont registers the person and the campaign as one
    entity, so candidate_name is the person and committee_name is the campaign
    name when the source gives one and the person's name otherwise. That keeps
    the two feeds producing the same strings for the same filer, which is what
    the committee -> candidate person_id join depends on.
    """
    filer_type   = _get(row, resolved, "filer_type")
    is_candidate = filer_type.lower() in CANDIDATE_FILER_TYPES

    entity   = _get(row, resolved, "entity_name")
    campaign = _get(row, resolved, "campaign_name")

    if is_candidate:
        if entity:
            candidate_name, _, _ = split_person_name(entity)
        else:
            first = _get(row, resolved, "cand_first")
            mid   = _get(row, resolved, "cand_middle")
            last  = _get(row, resolved, "cand_last")
            candidate_name = utils.clean_name(f"{first} {mid} {last}")
        committee_name = utils.clean_name(campaign) or candidate_name
        return committee_name, candidate_name, True

    committee_name = utils.clean_name(entity) or utils.clean_name(campaign)
    return committee_name, "", False


def _row_identity(row: dict, resolved: dict, fieldnames) -> str:
    """
    A key that is safe to deduplicate on.

    Two different situations, and conflating them cost ~11,400 real 2026
    transactions on the first full run:

      Bulk feed — has "Transaction Id", and repeats it. Every repeated id was
      checked and the copies are byte-for-byte identical across all 26 columns,
      so they are genuine duplicate rows in Vermont's export (~7% of them) and
      collapsing them is correct.

      Grid feed — has no id column at all. The old fallback keyed on
      (committee, contributor, date, amount, type), which is nowhere near
      unique: 210 different candidates each received a $5.00 sub-threshold
      contribution on 2026-02-06, and all 210 collapsed to one row. The key
      here is instead the ENTIRE raw row, which removes only exact duplicates
      (6,406 of them, also real) and can never merge two distinct transactions.
    """
    txn_id = _get(row, resolved, "transaction_id")
    if txn_id:
        return f"id|{txn_id}"
    return "row|" + "\x1f".join((row.get(c) or "") if isinstance(row.get(c), str)
                                else "" for c in fieldnames)


def parse_contributions(log, path: Path, cont_w, loan_w, registry: dict,
                        backfill: dict, seen: set) -> tuple[int, int, int]:
    """
    Parse one contributions file (bulk year export or grid chunk).

    Returns (contributions, loans, skipped_duplicates).
    """
    fh, start_row = open_source_csv(path)
    n_cont = n_loan = n_dupe = 0
    try:
        reader = csv.DictReader(fh)
        resolved, unmapped = _resolve_headers(reader.fieldnames,
                                              CONTRIBUTION_ALIASES)
        _log_unmapped(log, path, unmapped)
        fieldnames = list(reader.fieldnames or [])

        # A filer name can come from either feed's shape, so require that at
        # least one of the three routes into _filer_identity() exists.
        has_filer = bool(resolved["entity_name"] or resolved["campaign_name"]
                         or resolved["cand_last"])
        missing = [k for k in ("amount", "date") if not resolved[k]]
        if not has_filer:
            missing.append("filer name (entity/campaign/candidate)")
        if missing:
            log.file_parse_error(
                path.name,
                f"required column(s) unresolved: {', '.join(missing)} — file "
                f"skipped. Headers seen: {list(reader.fieldnames or [])[:20]}"
            )
            return 0, 0, 0

        for row_num, row in enumerate(
                tqdm(reader, desc=f"  {path.name}", unit="row",
                     dynamic_ncols=True, leave=False), start=start_row):

            amount = parse_amount(_get(row, resolved, "amount"))
            if not amount:
                continue
            date_ = parse_date(_get(row, resolved, "date"))

            filer_type = _get(row, resolved, "filer_type")
            committee_name, candidate_name, is_candidate = _filer_identity(row, resolved)
            filer_raw = utils.clean_name(_get(row, resolved, "entity_name")) or committee_name

            reg = registry.get(filer_raw) or registry.get(committee_name) or {}
            office = _get(row, resolved, "office") or reg.get("office", "")
            fid    = _get(row, resolved, "filer_id") or reg.get("filer_id", "")
            if _get(row, resolved, "filer_id"):
                FLAGS["real_filer_id"] = True

            ey = election_year(_get(row, resolved, "election_cycle"))

            # Contributor name: prefer split first/last/company columns when the
            # export provides them (bulk shape), fall back to the single
            # combined column the grid renders.
            c_first   = _get(row, resolved, "contributor_first")
            c_last    = _get(row, resolved, "contributor_last")
            c_company = _get(row, resolved, "contributor_company")
            if c_first or c_last:
                c_mid = _get(row, resolved, "contributor_middle")
                contributor_name = utils.clean_name(f"{c_first} {c_mid} {c_last}")
            elif c_company:
                contributor_name = utils.clean_name(c_company)
            else:
                contributor_name = contributor_display(
                    _get(row, resolved, "contributor_name"))

            txn_type = _txn_type(row, resolved)
            txn_id   = _get(row, resolved, "transaction_id")

            key = _row_identity(row, resolved, fieldnames)
            if key in seen:
                n_dupe += 1
                continue
            seen.add(key)

            # Remember every filer seen, so committees/candidates absent from
            # the active-only rosters still land in the entity tables.
            backfill.setdefault(filer_raw, {
                "committee_name": committee_name,
                "candidate_name": candidate_name,
                "filer_type":     filer_type,
                "filer_id":       fid,
                "election_year":  ey,
                "office":         office,
                "raw_file":       path.name,
                "row_num":        row_num,
            })

            contributor_city  = utils.clean_name(_get(row, resolved, "contributor_city"))
            contributor_state = _get(row, resolved, "contributor_state")
            contributor_zip   = clean_zip_field(_get(row, resolved, "contributor_zip"))
            amended   = bool01(_get(row, resolved, "amended"))

            if txn_type.lower() in LOAN_INCOME_TYPES:
                loan_w.writerow({
                    "state":              STATE,
                    "committee_name":     committee_name,
                    "original_amount":    amount,
                    "date":               date_,
                    "record_type":        txn_type,
                    "counterparty_name":  contributor_name,
                    "counterparty_city":  contributor_city,
                    "counterparty_state": contributor_state,
                    "counterparty_zip":   contributor_zip,
                    "candidate_name":     candidate_name,
                    "election_year":      ey,
                    "amended":            amended,
                    "filing_id":          txn_id,
                    "raw_file":           path.name,
                    "row_num":            row_num,
                })
                n_loan += 1
            else:
                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    committee_name,
                    "amount":            amount,
                    "date":              date_,
                    "transaction_type":  txn_type or "Monetary Contribution",
                    "contributor_name":  contributor_name,
                    "contributor_type":  _get(row, resolved, "contributor_type"),
                    "contributor_city":  contributor_city,
                    "contributor_state": contributor_state,
                    "contributor_zip":   contributor_zip,
                    "employer":          _get(row, resolved, "employer"),
                    "occupation":        _get(row, resolved, "occupation"),
                    "candidate_name":    candidate_name,
                    "office":            office,
                    "election_year":     ey,
                    "amended":           amended,
                    "filing_id":         txn_id,
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                n_cont += 1
    finally:
        fh.close()
    return n_cont, n_loan, n_dupe


def parse_expenditures(log, path: Path, expn_w, loan_w, registry: dict,
                       backfill: dict, seen: set) -> tuple[int, int, int]:
    """
    Parse one expenditures file (bulk year export or grid chunk).

    Returns (expenditures, loans, skipped_duplicates).
    """
    fh, start_row = open_source_csv(path)
    n_exp = n_loan = n_dupe = 0
    try:
        reader = csv.DictReader(fh)
        resolved, unmapped = _resolve_headers(reader.fieldnames,
                                              EXPENDITURE_ALIASES)
        _log_unmapped(log, path, unmapped)
        fieldnames = list(reader.fieldnames or [])

        has_filer = bool(resolved["entity_name"] or resolved["campaign_name"]
                         or resolved["cand_last"])
        missing = [k for k in ("amount", "date") if not resolved[k]]
        if not has_filer:
            missing.append("filer name (entity/campaign/candidate)")
        if missing:
            log.file_parse_error(
                path.name,
                f"required column(s) unresolved: {', '.join(missing)} — file "
                f"skipped. Headers seen: {list(reader.fieldnames or [])[:20]}"
            )
            return 0, 0, 0

        for row_num, row in enumerate(
                tqdm(reader, desc=f"  {path.name}", unit="row",
                     dynamic_ncols=True, leave=False), start=start_row):

            amount = parse_amount(_get(row, resolved, "amount"))
            if not amount:
                continue
            date_ = parse_date(_get(row, resolved, "date"))

            filer_type = _get(row, resolved, "filer_type")
            committee_name, candidate_name, is_candidate = _filer_identity(row, resolved)
            filer_raw = utils.clean_name(_get(row, resolved, "entity_name")) or committee_name

            reg = registry.get(filer_raw) or registry.get(committee_name) or {}
            office = _get(row, resolved, "office") or reg.get("office", "")
            fid    = _get(row, resolved, "filer_id") or reg.get("filer_id", "")
            if _get(row, resolved, "filer_id"):
                FLAGS["real_filer_id"] = True

            ey = election_year(_get(row, resolved, "election_cycle"))

            p_first   = _get(row, resolved, "payee_first")
            p_last    = _get(row, resolved, "payee_last")
            p_company = _get(row, resolved, "payee_company")
            if p_first or p_last:
                payee_name = utils.clean_name(f"{p_first} {p_last}")
            elif p_company:
                payee_name = utils.clean_name(p_company)
            else:
                payee_name = contributor_display(_get(row, resolved, "payee_name"))

            txn_type = _txn_type(row, resolved)
            txn_id   = _get(row, resolved, "transaction_id")

            key = _row_identity(row, resolved, fieldnames)
            if key in seen:
                n_dupe += 1
                continue
            seen.add(key)

            backfill.setdefault(filer_raw, {
                "committee_name": committee_name,
                "candidate_name": candidate_name,
                "filer_type":     filer_type,
                "filer_id":       fid,
                "election_year":  ey,
                "office":         office,
                "raw_file":       path.name,
                "row_num":        row_num,
            })

            payee_city  = utils.clean_name(_get(row, resolved, "payee_city"))
            payee_state = _get(row, resolved, "payee_state")
            payee_zip   = clean_zip_field(_get(row, resolved, "payee_zip"))
            amended     = bool01(_get(row, resolved, "amended"))

            if txn_type.lower() in LOAN_EXPENSE_TYPES:
                loan_w.writerow({
                    "state":              STATE,
                    "committee_name":     committee_name,
                    "original_amount":    amount,
                    "date":               date_,
                    "record_type":        txn_type,
                    "counterparty_name":  payee_name,
                    "counterparty_city":  payee_city,
                    "counterparty_state": payee_state,
                    "counterparty_zip":   payee_zip,
                    "candidate_name":     candidate_name,
                    "election_year":      ey,
                    "amended":            amended,
                    "filing_id":          txn_id,
                    "raw_file":           path.name,
                    "row_num":            row_num,
                })
                n_loan += 1
            else:
                # A "Contribution to Registrant" expenditure by a PAC or party
                # committee names the recipient in the payee column. That's a
                # support relationship, not the filer's own identity, so it
                # belongs in affiliated_candidate_name rather than
                # candidate_name.
                #
                # Vermont's own "Candidate Mentioned" column is authoritative
                # where it exists (grid feed). It is blank on every sampled
                # row, so the heuristic below still carries the bulk feed,
                # which has no such column.
                #
                # The heuristic only fires when the payee resolves to a
                # candidate the roster knows about. "Registrant" covers PACs
                # too, and this column means a *candidate* — so an unrecognized
                # payee is left blank rather than asserted. That undercounts
                # the relationship for candidates outside the active-filer
                # roster, which is the safe direction to be wrong in.
                purpose  = _get(row, resolved, "purpose")
                supports = utils.clean_name(_get(row, resolved, "candidate_mentioned"))
                if not supports and not is_candidate \
                        and purpose.lower().startswith("contribution to"):
                    payee_reg = registry.get(payee_name) or {}
                    supports = payee_reg.get("display", "")
                expn_w.writerow({
                    "state":            STATE,
                    "committee_name":   committee_name,
                    "amount":           amount,
                    "date":             date_,
                    "transaction_type": txn_type or "Monetary Expenditure",
                    "payee_name":       payee_name,
                    "purpose":          purpose,
                    "category":         _get(row, resolved, "goods_or_service"),
                    "payee_city":       payee_city,
                    "payee_state":      payee_state,
                    "payee_zip":        payee_zip,
                    "candidate_name":   candidate_name,
                    "office":           office,
                    "election_year":    ey,
                    "affiliated_candidate_name": supports,
                    "support_oppose":   "S" if supports else "",
                    "amended":          amended,
                    "filing_id":        txn_id,
                    "raw_file":         path.name,
                    "row_num":          row_num,
                })
                n_exp += 1
    finally:
        fh.close()
    return n_exp, n_loan, n_dupe


# ==================== party / district enrichment ======================
#
# Vermont's CF system records neither party nor district — see the scraper's
# ARCHIVE_HOSTS note. Both come from the VT Elections Database contest results
# (raw/elections_archive.csv), joined on candidate name.
#
# Coverage is bounded by what that source is, and the limits are structural
# rather than fixable by better matching:
#   - even years only, 2016-2024 (no 2014, no odd-year town meetings, no 2026)
#   - federal / statewide / legislative / county races only. Vermont's 607
#     local candidates (City Councilor, Selectperson, School Director) never
#     appear, because those races aren't in the state election archive.
#   - only people who appeared on a ballot; a filer who registered a committee
#     and withdrew has campaign finance but no election record.
# Measured against the real roster: ~61% of all candidates, ~84% of those in
# federal/statewide/legislative/county offices.

ARCHIVE_FILE = "elections_archive.csv"

# The archive and the CF roster name the same offices differently. Used to
# confirm a name match rather than to require one — an office mismatch
# downgrades confidence, it doesn't veto, because these vocabularies are not
# guaranteed to stay aligned.
ARCHIVE_OFFICE_EQUIV = {
    "state representative":  "state representative",
    "state senate":          "state senator",
    "state senator":         "state senator",
    "governor":              "governor",
    "lieutenant governor":   "lieutenant governor",
    "secretary of state":    "secretary of state",
    "state treasurer":       "state treasurer",
    "auditor":               "auditor of accounts",
    "auditor of accounts":   "auditor of accounts",
    "attorney general":      "attorney general",
    "u.s. senate":           "u.s. senator",
    "u.s. senator":          "u.s. senator",
    "u.s. house":            "u.s. representative",
    "u.s. representative":   "u.s. representative",
    "county sheriff":        "sheriff",
    "sheriff":               "sheriff",
    "district attorney":     "states attorney",
    "states attorney":       "states attorney",
    "state's attorney":      "states attorney",
    "assistant judge":       "assistant judge",
    "probate judge":         "probate judge",
    "high bailiff":          "high bailiff",
}


def _office_key(office: str) -> str:
    o = re.sub(r"\s+", " ", (office or "").strip().lower())
    return ARCHIVE_OFFICE_EQUIV.get(o, o)


def _match_name(name: str) -> str:
    """Normalize a person name for cross-source matching: drop suffixes and punctuation."""
    n = utils.clean_name(name)
    n = re.sub(r"\b(JR|SR|II|III|IV|V)\.?\b", " ", n)
    n = re.sub(r"[^A-Z ]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def _first_last(name: str) -> tuple[str, str] | None:
    t = _match_name(name).split()
    return (t[0], t[-1]) if len(t) >= 2 else None


def _canon_party(raw: str) -> str:
    """
    Canonicalize a Vermont ballot-line label.

    Vermont runs fusion voting and the archive spells it inconsistently —
    "Democratic/Progressive", "Dem/Prog" and "Progressive/Democratic" are all
    the same two lines. Each side is split off and canonicalized through
    src/aliases/parties.csv, then re-joined pipe-delimited, which is the
    convention parsers/new_york.py already established for fusion states.

    Components are then SORTED, which is where this departs from New York. In
    NY the order of ballot lines is meaningful and stable, so that parser
    preserves it. Vermont's archive gives no such guarantee: the same
    Democratic+Progressive fusion appears as "Progressive/Democratic" 57
    times, "Dem/Prog" 36 times and "Democratic/Progressive" 17 times across
    the same dataset, so the order is a clerk's choice rather than a fact.
    Sorting makes one fusion one value; keeping the source order would split
    it three ways in any GROUP BY.
    """
    from src.aliases import canonical_party
    parts = [p.strip() for p in (raw or "").split("/") if p.strip()]
    out = {canonical_party(p) for p in parts if canonical_party(p)}
    return "|".join(sorted(out))


def load_election_archive(log) -> tuple[dict, dict]:
    """
    Read raw/elections_archive.csv into name lookups.

    The file is town-level — one row per candidate per municipality, ~600K
    rows — so it is streamed and collapsed to one record per
    (candidate_id, election_id) rather than held in memory whole.

    Party lives in `candidate_party_name` for general elections and in
    `primary_party` for primaries; taking only the former leaves 42% of
    records blank, so both are consulted. Returns (by_full_name, by_first_last),
    each mapping a normalized name to a list of records.
    """
    # The scraper writes one file per year-batch (elections_archive_2014_2017
    # .csv, …). The bare elections_archive.csv is the older single-file name and
    # is still read so an existing download keeps working; overlapping rows
    # collapse on (candidate_id, election_id) below either way.
    paths = sorted(f for f in RAW_DIR.glob("elections_archive*.csv")
                   if f.stat().st_size > 0)
    if not paths:
        log.warning(f"  no elections_archive*.csv — party and district will be "
                    f"blank (Vermont's CF system publishes neither)")
        return {}, {}

    ft = time.perf_counter()
    seen: set[tuple[str, str]] = set()
    by_full: dict[str, list] = {}
    by_fl: dict[tuple[str, str], list] = {}
    n = 0

    for path in paths:
        before, dupes = n, 0
        with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
            for row in csv.DictReader(f):
                name = (row.get("candidate_name") or "").strip()
                if not name:
                    continue
                key = (row.get("candidate_id", ""), row.get("election_id", ""))
                if key in seen:
                    dupes += 1
                    continue
                seen.add(key)
                party = _canon_party((row.get("candidate_party_name") or "").strip()
                                     or (row.get("primary_party") or "").strip())
                if not party:
                    continue
                rec = {
                    "party":    party,
                    "office":   _office_key(row.get("office_name", "")),
                    "district": (row.get("district_name") or "").strip(),
                    "dtype":    (row.get("district_type") or "").strip(),
                    "year":     (row.get("election_date") or "")[:4],
                }
                nm = _match_name(name)
                by_full.setdefault(nm, []).append(rec)
                fl = _first_last(name)
                if fl:
                    by_fl.setdefault(fl, []).append(rec)
                n += 1
        # Distinguish "this file added nothing because another file already
        # covered these elections" from "this file is empty" — they look
        # identical as a bare row count, and the first is normal.
        note = ""
        if n == before:
            note = (f" — 0 new, all {dupes:,} already covered by an earlier file"
                    if dupes else " — file contains no candidate records")
        log.file_parsed(path.name, "election_archive", n - before,
                        bytes=path.stat().st_size)
        if note:
            log.info(f"  {path.name}{note}")

    log.registry_loaded(f"elections_archive*.csv ({len(paths)} file(s))",
                        entries=n, relation="candidates")
    log.info(f"  election archive: {n:,} candidate records in "
             f"{round(time.perf_counter() - ft, 2)}s")
    return by_full, by_fl


OPENSTATES_FILE = "OpenStates_People.csv"


def load_openstates(log) -> tuple[dict, dict]:
    """
    Read raw/OpenStates_People.csv (written by the scraper) into name lookups.

    Second-tier only. It holds currently-serving legislators, so most of its
    ~180 rows also appear in the elections archive; what it uniquely covers is
    a sitting legislator whose current-cycle candidacy postdates the archive's
    last election. Same record shape as the archive so both feed one matcher.

    `year` is left blank deliberately: "current" isn't an election year, and
    filling one in would let _pick() prefer this over a dated archive record.
    """
    path = RAW_DIR / OPENSTATES_FILE
    if not (path.exists() and path.stat().st_size > 0):
        return {}, {}

    ft = time.perf_counter()
    by_full: dict[str, list] = {}
    by_fl: dict[tuple[str, str], list] = {}
    n = 0
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        for row in csv.DictReader(f):
            name  = (row.get("name") or "").strip()
            party = _canon_party((row.get("party") or "").strip())
            if not (name and party):
                continue
            rec = {
                "party":    party,
                "office":   _office_key(row.get("chamber", "")),
                "district": (row.get("district") or "").strip(),
                "dtype":    "",
                "year":     "",
            }
            by_full.setdefault(_match_name(name), []).append(rec)
            fl = _first_last(name)
            if fl:
                by_fl.setdefault(fl, []).append(rec)
            n += 1

    log.file_parsed(path.name, "openstates", n,
                    duration_s=round(time.perf_counter() - ft, 2),
                    bytes=path.stat().st_size)
    log.registry_loaded(path.name, entries=n, relation="candidates")
    return by_full, by_fl


def _pick(recs: list, office: str, year: str) -> dict | None:
    """
    Choose one archive record for a candidate.

    Preference order: same office AND same election year, then same office,
    then same year, then a single unambiguous party across everything left.
    A set of records that disagree on party with nothing to break the tie is
    rejected rather than guessed at.
    """
    if not recs:
        return None
    ok = _office_key(office)
    for pred in (lambda r: r["office"] == ok and r["year"] == year,
                 lambda r: r["office"] == ok,
                 lambda r: r["year"] == year):
        hits = [r for r in recs if pred(r)]
        if len(hits) == 1:
            return hits[0]
        if hits and len({r["party"] for r in hits}) == 1:
            return hits[0]
    if len({r["party"] for r in recs}) == 1:
        return recs[0]
    return None


def enrich_party_district(log, candidates: dict, sources: list) -> dict:
    """
    Fill party (and district where the CF roster has none) on candidate rows.

    `sources` is an ordered list of (label, by_full, by_fl). Each candidate is
    tried against them in turn and the FIRST source that resolves wins, so
    ordering encodes authority: the VT elections archive (a real ballot record,
    with history) before Open States (current legislators only, no history).

    Writes party_source and match_confidence alongside, exactly as
    parsers/texas.py and parsers/new_york.py do, so a downstream query can tell
    a state-published value from a joined-in one, and can tell the two joins
    apart:
        party_source     "vt_elections_archive" | "openstates"
        match_confidence "exact" — full normalized name matched
                         "high"  — first+last matched unambiguously
    """
    stats = collections.Counter()
    if not any(bf or bfl for _, bf, bfl in sources):
        stats["unmatched"] = len(candidates)
        return stats

    for row in candidates.values():
        name = row.get("candidate_name", "")
        if not name:
            continue
        office, year = row.get("office", ""), row.get("election_year", "")
        # A local candidate (City Councilor, Selectperson, School Director)
        # can't be in a state election archive under that office. They still
        # match when the same person also ran for the legislature, which is
        # common in a state this size — but only on a full-name match. The
        # first+last fallback is too loose to assert across offices.
        # The export stores Office Type as a CODE (OTLOC / OTGA / OTSTW /
        # OTCOU / OTFED), not as the "Local" / "Statewide" label the browse
        # page renders. Both are accepted so a future export that switches to
        # labels doesn't silently disable this gate.
        _ot = (row.get("_office_type") or "").strip().upper()
        local_only = _ot in ("OTLOC", "LOCAL")
        rec = conf = label = None

        for src_label, by_full, by_fl in sources:
            hit = by_full.get(_match_name(name))
            if hit:
                rec = _pick(hit, office, year)
                conf = "exact"
            if rec is None and not local_only:
                fl = _first_last(name)
                hits = by_fl.get(fl) if fl else None
                if hits:
                    rec = _pick(hits, office, year)
                    conf = "high"
            if rec is not None:
                label = src_label
                break

        if rec is None:
            stats["unmatched"] += 1
            continue

        row["party"] = rec["party"]
        row["party_source"] = label
        row["match_confidence"] = conf
        # District only when the offices actually agree. A Barre city
        # councillor who also ran for the legislature legitimately matches on
        # party, but his council seat is not "Washington 3" — carrying the
        # legislative district onto a local office row would be simply wrong.
        if (not row.get("district") and rec["district"]
                and rec["office"] == _office_key(office)):
            row["district"] = rec["district"]
        stats[f"{label}_{conf}"] += 1

    log.enrichment_summary(relation="candidates", **dict(stats))
    return stats


# ============================ backfill ================================

def apply_backfill(backfill: dict, candidates: dict, committees: dict) -> tuple[int, int]:
    """
    Add entity rows for filers that appear in transactions but not in the
    active-only rosters.

    Only name, type and election year are known from a transaction row.
    office/district/party/treasurer stay blank — that is a genuine absence in
    the source for a deregistered filer, not a parsing gap, and inventing
    values would corrupt the person_id grouping key.
    """
    added_cand = added_cmte = 0
    known_cand = {utils.clean_name(r["candidate_name"]) for r in candidates.values()}
    known_cmte = {utils.clean_name(r["committee_name"]) for r in committees.values()}

    for filer_raw, info in backfill.items():
        is_candidate = info["filer_type"].lower() in CANDIDATE_FILER_TYPES
        name = info["candidate_name"] if is_candidate else info["committee_name"]
        if not name:
            continue

        fid = info["filer_id"] or surrogate_id(name)
        ey  = info["election_year"]

        if is_candidate and name not in known_cand:
            display, first, last = split_person_name(filer_raw)
            candidates[f"bf_cand|{fid}|{ey}"] = {
                "state":           STATE,
                "candidate_name":  name,
                "candidate_first": first,
                "candidate_last":  last,
                "office":          info["office"],
                "district":        "",
                "jurisdiction":    "",
                "party":           "",
                "election_year":   ey,
                "incumbent":       "",
                "state_filer_id":  fid,
                "raw_file":        info["raw_file"],
                "row_num":         info["row_num"],
            }
            known_cand.add(name)
            added_cand += 1

        if name not in known_cmte:
            committees[f"bf_cmte|{fid}|{ey}"] = {
                "state":          STATE,
                "committee_name": name,
                "committee_type": info["filer_type"],
                "election_year":  ey,
                "candidate_name": info["candidate_name"] if is_candidate else "",
                "treasurer_name": "",
                "city":           "",
                "zip":            "",
                "active":         "",
                "state_filer_id": fid,
                "raw_file":       info["raw_file"],
                "row_num":        info["row_num"],
            }
            known_cmte.add(name)
            added_cmte += 1

    return added_cand, added_cmte


# ================================ run =================================

def run():
    log = get_logger("vermont", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    total_dupes         = 0
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

        candidates: dict[str, dict] = {}
        committees: dict[str, dict] = {}
        registry:   dict[str, dict] = {}
        backfill:   dict[str, dict] = {}

        # ── Rosters first — they're authoritative for every filer they cover ──
        for filename, fn, relation in (
            ("candidates.csv", parse_candidate_roster, "candidates"),
            ("committees.csv", parse_committee_roster, "committees"),
        ):
            path = RAW_DIR / filename
            if not (path.exists() and path.stat().st_size > 0):
                log.warning(f"  {filename} missing — {relation} will be "
                            f"backfilled from transactions only")
                continue
            ft = time.perf_counter()
            if relation == "candidates":
                n = fn(log, path, candidates, committees, registry)
            else:
                n = fn(log, path, committees, registry)
            log.file_parsed(path.name, relation, n,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            log.registry_loaded(path.name, entries=n, relation=relation)

        # ── Transactions: bulk years first, then grid chunks ──────────────
        # Bulk wins on any collision, so it's parsed first and its transaction
        # ids seed the dedup set. `dedup` is only switched on once both shapes
        # are actually present, so the common single-source case pays nothing
        # beyond building the set.
        cont_bulk = raw_files("contributions_*.csv")
        cont_grid = raw_files("grid_contributions_*.csv")
        expn_bulk = raw_files("expenditures_*.csv")
        expn_grid = raw_files("grid_expenditures_*.csv")

        cont_seen: set[str] = set()
        for path in cont_bulk + cont_grid:
            ft = time.perf_counter()
            n_c, n_l, n_d = parse_contributions(
                log, path, cont_w, loan_w, registry, backfill, cont_seen,
            )
            log.file_parsed(path.name, "contributions", n_c,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            if n_l:
                log.file_parsed(path.name, "loans_debts", n_l,
                                bytes=path.stat().st_size)
            total_contributions += n_c
            total_loans         += n_l
            total_dupes         += n_d
        del cont_seen

        expn_seen: set[str] = set()
        for path in expn_bulk + expn_grid:
            ft = time.perf_counter()
            n_e, n_l, n_d = parse_expenditures(
                log, path, expn_w, loan_w, registry, backfill, expn_seen,
            )
            log.file_parsed(path.name, "expenditures", n_e,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            if n_l:
                log.file_parsed(path.name, "loans_debts", n_l,
                                bytes=path.stat().st_size)
            total_expenditures += n_e
            total_loans        += n_l
            total_dupes        += n_d
        del expn_seen

        if total_dupes:
            log.info(f"  Skipped {total_dupes:,} exact duplicate row(s) — "
                     f"repeated Transaction Ids in the bulk feed and "
                     f"byte-identical rows in the grid feed, both present in "
                     f"Vermont's own exports")

        # ── Entities absent from the active-only rosters ──────────────────
        added_cand, added_cmte = apply_backfill(backfill, candidates, committees)

        # ── Party + district ──────────────────────────────────────────────
        # Vermont's CF system publishes neither, so both are joined in from
        # outside. Ordered by authority: the state elections archive is a real
        # ballot record with history; Open States is current legislators only.
        enrich_party_district(log, candidates, [
            ("vt_elections_archive", *load_election_archive(log)),
            ("openstates",           *load_openstates(log)),
        ])
        if added_cand or added_cmte:
            log.enrichment_summary(backfilled_candidates=added_cand,
                                   backfilled_committees=added_cmte,
                                   roster_candidates=len(candidates) - added_cand,
                                   roster_committees=len(committees) - added_cmte)

        for row in candidates.values():
            cand_w.writerow(row)
            candidates_written += 1
        for row in committees.values():
            cmte_w.writerow(row)
            committees_written += 1

        # Close before person-ID assignment — those helpers rewrite the files
        # in place and would read a partially flushed gzip stream otherwise.
        for fh in file_handles:
            fh.close()
        file_handles = []

        # ── Person IDs ────────────────────────────────────────────────────
        # "committee" whenever the source gave us real ids: this vendor's
        # filer id is per-registration, so the same person gets a new one each
        # cycle and min(id) per (name, office, district) is what merges them.
        # With no id column anywhere, every state_filer_id is already a
        # name-derived surrogate, so name_hash is the honest model — grouping
        # surrogates by min() would just be a slower way of grouping by name.
        id_model = "committee" if FLAGS["real_filer_id"] else "name_hash"
        log.info(f"  person_id model: {id_model}")

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model=id_model)
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name: str) -> int:
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        for name, relation, rows in (
            ("contributions.csv.gz", "contributions", total_contributions),
            ("expenditures.csv.gz",  "expenditures",  total_expenditures),
            ("candidates.csv.gz",    "candidates",    candidates_written),
            ("committees.csv.gz",    "committees",    committees_written),
            ("loans_debts.csv.gz",   "loans_debts",   total_loans),
        ):
            log.file_parsed(name, relation, rows, role="output",
                            bytes=_bytes(name))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_contributions:,} contributions, "
                 f"{total_expenditures:,} expenditures, {total_loans:,} loans, "
                 f"{candidates_written:,} candidates, "
                 f"{committees_written:,} committees")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions,
                  expenditures=total_expenditures,
                  loans_debts=total_loans,
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
