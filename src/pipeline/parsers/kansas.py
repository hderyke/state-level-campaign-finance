"""
parsers/kansas.py — Parse scrapers/kansas.py's scraped CSVs into the
canonical cleaned CSVs.

This replaces the old PDF-based parser (kept for reference as
parsers/kansas_pdf_legacy.py) now that scrapers/kansas.py extracts
structured data directly from the KS SOS CFR Examiner instead of
downloading PDFs.

Input:  data/Kansas/raw/ (written by scrapers/kansas.py)
    candidates_summary.csv        — one row per candidate filing
    schedule_a_contributions.csv  — itemized contributions (incl. loans)
    schedule_b_inkind.csv         — itemized in-kind contributions
    schedule_c_expenditures.csv   — itemized expenditures
    schedule_d_other.csv          — other transactions; NOT read (see below)
    data/Kansas/manifest.csv      — read only to log coverage vs. the scrape

Output: data/Kansas/cleaned/
    contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
    candidates.csv.gz, committees.csv.gz

Joining schedule rows to their candidate:
    Every row in every one of the scraper's output files carries a
    candidate_uid column: "office_group|cycle_label|office_sought|
    district_number|name|original_date|amendment_date" (identical to the
    manifest's candidate_key). That's the join key used here — NOT the
    "candidate" name-text column, which can collide across different
    cycles/offices for two people who happen to share a name.

    office_group and election_year are decoded straight out of candidate_uid
    rather than joining against the manifest, which is only read to log how
    many scraped candidates made it into this parse.

Three ways the same money arrives more than once — all collapsed here:

  1. Re-scrapes. The scraper appends to its CSVs and deliberately re-scrapes
     cycles whose year is >= the current year (to pick up amendments), so a
     filing scraped k times appears k times in every file. 6% of contribution
     rows / $21M in the 2026-08 data. See _dedup_rescrapes.

  2. Amendments. An amendment is a **separate row** in the results grid, not a
     flag on the original, and it restates the entire reporting period. 1,707
     of 5,165 periods had been filed more than once (up to 12 times).

  3. Overlapping search windows. The grid returns a filing under every search
     whose date window contains its *filing* date, so an amendment filed in
     2020 to a 2018 report comes back from both the 2018 and 2020 searches.

  2 and 3 are handled together by resolve_amendments(), which keeps the newest
  version of each (candidate, reporting period). Together these three cut the
  contribution total from $185M to $109.5M — against $111.1M that the filings
  themselves declare in their own summary totals (-1.5%, the expected gap for
  unitemized contributions, which are summarised but never listed).

Loans:
    Schedule A tags some receipts `type_of_payment = "Loan"` — 1,264 rows /
    $31.6M (14% of all KS receipts) in the 2026-08 data. Those are routed to
    loans_debts.csv.gz rather than counted as contributions, following
    Wyoming (which does the same for its LOAN contribution type) and Georgia.

    Schedule D ("other transactions") is deliberately NOT read. Its only
    amount column is `balance_at_close`, a period-end balance rather than an
    original amount: 1,526 of 2,426 (candidate, counterparty, account) triples
    recur across multiple filing periods, so summing it would multiply one
    loan by its number of reporting periods. LOANS_DEBTS has no balance column
    to hold it honestly, so the rows are left in the raw CSV only.

id_model = "name_hash" — same as the old Kansas parser (and Alaska,
Kentucky): Kansas has no numeric filer ID in its source data, so person_id
is derived from MD5(state + normalized candidate_name).

Party: joined from the SOS Candidate List roster — see PartyIndex.

election_year is the *earliest* cycle a filing's period was searched under, not
the cycle whose search returned it: a 2018 report amended in 2020 is still a
2018-cycle filing. See resolve_amendments().
"""

import collections
import csv
import gzip
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
from src.aliases import canonical_party, expand_nickname
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
STATE_DIR = PROJECT_ROOT / "data" / "Kansas"
RAW_DIR   = STATE_DIR / "raw"            # scrapers/kansas.py's output dir
CLEAN_DIR = STATE_DIR / "cleaned"
MANIFEST  = STATE_DIR / "manifest.csv"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

INPUT_NAMES = {
    "summary":    "candidates_summary.csv",
    "schedule_a": "schedule_a_contributions.csv",
    "schedule_b": "schedule_b_inkind.csv",
    "schedule_c": "schedule_c_expenditures.csv",
    "roster":     "candidate_roster.csv",
}

STATE = "KS"
# Filing periods reach back well before the earliest election cycle scraped:
# 2014-cycle filings legitimately carry 2010-2012 transaction dates. A 2013
# floor silently dropped 24,821 rows / $12.3M of real transactions, so this
# matches validate.py's own 1990 horizon (older dates surface there as a
# tier-2 note rather than being deleted here).
EARLIEST_YEAR  = 1990
MAX_VALID_YEAR = date.today().year + 4

# Schedule A's payment types that are loans rather than contributions.
LOAN_PAYMENT_TYPES = {"loan"}

# candidate_uid's office_group tells us which viewer category a filing came
# from. The PAC form's two committee types are scraped as their own groups, so
# a filer's type needs no guessing from its name.
COMMITTEE_TYPE_BY_GROUP = {
    "PAC":   "PAC",
    "Party": "Party Committee",
}
CANDIDATE_COMMITTEE_TYPE = "Candidate"


def _input_path(key: str) -> Path:
    """Resolve one scraper output file.

    Prefers data/Kansas/raw/ (where scrapers/kansas.py writes), falling back
    to data/Kansas/ so CSVs left behind by the pre-integration `kansas_v2`
    scraper — which wrote to the state dir root — still parse without a
    re-scrape.
    """
    name = INPUT_NAMES[key]
    primary = RAW_DIR / name
    if primary.exists():
        return primary
    legacy = STATE_DIR / name
    return legacy if legacy.exists() else primary


# ======================== Date / amount helpers =======================

def _parse_date(val: str) -> str:
    """M/D/YY or MM/DD/YYYY → YYYY-MM-DD. Returns '' on failure.

    Both forms occur — the schedules use unpadded two-digit years for older
    filings ("9/30/10") and four-digit ones elsewhere.
    """
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            d = datetime.strptime(v, fmt).date()
            if EARLIEST_YEAR <= d.year <= MAX_VALID_YEAR:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_amount(val: str) -> str:
    """The scraper already writes amounts as plain floats (e.g. '1400.0') via
    money_to_float(); this re-normalizes and handles the empty-string case
    (the scraper writes '' when a cell was blank)."""
    v = (val or "").strip()
    if not v:
        return ""
    try:
        return str(float(v))
    except ValueError:
        return ""


def _clean(val: str) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


# Real postal codes only. "NA" is what filers leave behind when they skip the
# field, and it passes a naive two-letter test, so the check is membership in
# the same set validate.py uses (states + DC/territories + military APO/FPO).
_VALID_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
    "DC", "PR", "GU", "VI", "AS", "MP", "UM",
    "AA", "AE", "AP",           # military addresses
}


def _clean_state_code(val: str) -> str:
    """Keep only recognised state/territory codes; blank anything else.

    Kentucky and Louisiana both filter their equivalents; without it Kansas's
    "NA" placeholders trip validate.py's state-code check.
    """
    v = _clean(val).upper()
    return v if v in _VALID_STATE_CODES else ""


def _clean_zip(val: str) -> str:
    """Normalize a ZIP, restoring a leading zero the source dropped.

    New England ZIPs arrive numerically truncated ("2144" for Somerville MA,
    1,918 rows), which fails validation. Pad to five digits, hand off to the
    shared helper for the ZIP+4 rules, then blank anything still unusable
    ("-", "0", "3-3149") rather than passing junk downstream.
    """
    v = _clean(val)
    if v.isdigit() and 3 <= len(v) <= 4:
        v = v.zfill(5)
    else:
        m = re.fullmatch(r"(\d{3,4})-(\d{4})", v)
        if m:
            v = f"{m.group(1).zfill(5)}-{m.group(2)}"
    v = utils.clean_zip(v)
    return v if re.fullmatch(r"\d{5}(-\d{4}|\d{4})?", v) else ""


def _election_year_clean(val: str) -> str:
    """'2022-special' → '2022'; '2026' → '2026'. Strips non-numeric suffixes."""
    m = re.match(r"(\d{4})", (val or "").strip())
    return m.group(1) if m else val


def _split_candidate_uid(candidate_uid: str) -> dict:
    """
    Decode a candidate_uid ("office_group|cycle_label|office_sought|
    district_number|name|original_date|amendment_date") back into its parts.
    This is the same string make_candidate_key() built in the scraper.
    Returns all-empty-string parts if candidate_uid doesn't have the expected
    shape (e.g. blank/malformed row).
    """
    parts = (candidate_uid or "").split("|")
    keys = ("office_group", "cycle_label", "office_sought", "district_number",
            "name", "original_date", "amendment_date")
    if len(parts) != len(keys):
        return {k: "" for k in keys}
    return dict(zip(keys, parts))


def _split_name(full: str) -> tuple[str, str]:
    """Split a candidate's name into (first, last) heuristically — 'Last,
    First' if a comma is present, otherwise 'First ... Last' by token
    position. Identical heuristic to the old parser, for consistent
    candidates.csv.gz output across both pipelines."""
    if "," in full:
        parts = full.split(",", 1)
        return _clean(parts[1]), _clean(parts[0])   # (first, last)
    tokens = full.split()
    first = tokens[0] if tokens else ""
    last  = tokens[-1] if len(tokens) > 1 else ""
    return first, last


def _normalize_district(raw: str) -> str:
    """Reduce a district cell to a bare number: '005' -> '5', '/ 38' -> '38'.

    The results grid renders the district with a leading slash, so a plain
    int() cast left the slash in place — which then failed to match the SOS
    roster's "38" when joining party. Falls back to the raw value for a
    genuinely non-numeric district.
    """
    m = re.search(r"\d+", raw or "")
    if m:
        return str(int(m.group()))
    return (raw or "").strip()


# ==================== Expenditure purpose / category ==================
# Schedule C's Purpose column is a fixed form label followed by the filer's
# free text, with only a space between them ("Printing printing/mailing").
# The scraper keeps it whole as purpose_raw because the boundary isn't marked
# up; it is however recoverable, because the label comes from a closed list.
# These 29 labels cover 100% of the 171,326 Schedule C rows in the 2026-08
# data (26 real categories plus three legacy spellings), so the label lands in
# `category` and the remainder in `purpose`.
PURPOSE_LABELS = (
    "Candidate (self)", "Donation/Contrib", "Electronic/Website Advertising",
    "Electronics/Computers", "Filing Fee", "Fundraising Expenses",
    "Meeting/Travel", "Newspaper Ads", "Postage/Shipping", "Radio/TV",
    "Voter file", "Yard signs", "Cell Phone", "Consultant", "Miscellaneous",
    "Newsletter", "Mileage", "Polling", "Printing", "Reimbursement",
    "Refund", "Rental", "Subscription", "Supplies", "Tickets", "Gift",
    # legacy/short spellings seen on a handful of older filings
    "Ads", "Shipping", "TV",
)
# Longest first so "Newspaper Ads" wins over "Ads" and "Radio/TV" over "TV".
_PURPOSE_LABELS_SORTED = tuple(sorted(PURPOSE_LABELS, key=len, reverse=True))

# Purpose labels that describe a different kind of transaction, not just a
# spending category. Everything else is an ordinary expenditure.
_TXN_TYPE_BY_CATEGORY = {
    "Refund":           "Refund",
    "Donation/Contrib": "Contribution",
}


def _split_purpose(raw: str) -> tuple[str, str]:
    """'Printing printing/mailing' → ('Printing', 'printing/mailing').

    Returns ('', raw) when no known label matches, so an unrecognised value is
    preserved in `purpose` rather than silently dropped.
    """
    v = _clean(raw)
    if not v:
        return "", ""
    for label in _PURPOSE_LABELS_SORTED:
        if v == label:
            return label, ""
        if v.startswith(label + " "):
            return label, v[len(label) + 1:].strip()
    return "", v


# ===================== Party enrichment (roster) ======================
# The CFR Examiner publishes no party. The SOS's Candidate List page does, so
# scrapers/kansas.py saves it as candidate_roster.csv and it's joined on here.
#
# This is a name match, not an ID join — neither source carries a filer ID —
# so matches are graded and recorded:
#     party_source     = "ks_sos_candidate_list"
#     match_confidence = "exact"  — last + first + office + district + year
#                        "high"   — last + first + year (office/district
#                                   disagree or are absent on one side)
# Anything weaker is left unmatched rather than guessed at.

# Roster office text -> a coarse office key, and the same for the CFR
# Examiner's own office_sought text. Only offices that exist on both sides can
# match; judicial races (nonpartisan in KS, blank party in the roster) and
# federal races simply never produce a hit.
_OFFICE_KEYS = [
    ("HOUSE",     ("KANSAS HOUSE", "STATE REPRESENTATIVE", "REPRESENTATIVE")),
    ("SENATE",    ("KANSAS SENATE", "STATE SENATE", "SENATOR")),
    ("GOVERNOR",  ("GOVERNOR",)),
    ("AG",        ("ATTORNEY GENERAL",)),
    ("SOS",       ("SECRETARY OF STATE",)),
    ("TREASURER", ("TREASURER",)),
    ("INSURANCE", ("INSURANCE",)),
    ("DA",        ("DISTRICT ATTORNEY", "COUNTY ATTORNEY")),
    ("BOE",       ("BOARD OF EDUCATION",)),
]


def _office_key(raw: str) -> str:
    """Coarse office bucket shared by both sources. 'Kansas House of
    Representatives' and 'State Representative' both -> 'HOUSE'.

    US House/Senate must not collapse into the state chambers, so federal
    offices are tagged separately before the state patterns are tried.
    """
    up = re.sub(r"\s+", " ", (raw or "").strip().upper())
    if not up:
        return ""
    if up.startswith("UNITED STATES") or up.startswith("U.S."):
        return "FEDERAL"
    for key, needles in _OFFICE_KEYS:
        if any(n in up for n in needles):
            return key
    return "OTHER"


def _norm_token(val: str) -> str:
    """Uppercase, strip punctuation/whitespace — for name-part comparison."""
    return re.sub(r"[^A-Z]", "", (val or "").upper())


def _name_variants(first: str) -> set[str]:
    """A first name plus any formal names it's a nickname for, so a roster
    'Michael' still matches a filing's 'Mike'."""
    f = _norm_token(first)
    if not f:
        return set()
    return {f} | {_norm_token(v) for v in expand_nickname(f)}


def _split_last_first(full: str) -> tuple[str, str]:
    """Best-effort (last, first) from either 'Last, First M' or 'First M Last'.
    The CFR Examiner's own name format isn't uniform, so both are handled."""
    name = _clean(full)
    if "," in name:
        last, _, rest = name.partition(",")
        return _norm_token(last), _norm_token(rest.strip().split(" ")[0] if rest.strip() else "")
    parts = [p for p in name.split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return _norm_token(parts[0]), ""
    return _norm_token(parts[-1]), _norm_token(parts[0])


class PartyIndex:
    """Roster lookups keyed strongest-first.

    Built once per parse; `lookup()` returns {"party", "party_source",
    "match_confidence"} or None. A key that the roster maps to two different
    parties (two same-named candidates, or a candidate who switched parties
    between the primary and general) is dropped from that index rather than
    resolved arbitrarily — better a blank party than a confidently wrong one.
    """

    def __init__(self, rows: list[dict]):
        exact: dict[tuple, set[str]] = {}
        loose: dict[tuple, set[str]] = {}
        self.rows = 0

        for r in rows:
            party = canonical_party(_clean(r.get("party", "")))
            if not party:
                continue  # judicial/nonpartisan races carry no party
            year = _election_year_clean(r.get("election_year", ""))
            if not year:
                continue
            last = _norm_token(r.get("last_name", ""))
            first = _norm_token(r.get("first_name", ""))
            if not last:
                last, first = _split_last_first(r.get("candidate", ""))
            if not last:
                continue
            okey = _office_key(r.get("office", ""))
            district = _normalize_district(r.get("district", ""))
            self.rows += 1

            for variant in (_name_variants(first) or {""}):
                exact.setdefault((last, variant, okey, district, year), set()).add(party)
                loose.setdefault((last, variant, year), set()).add(party)

        # keep only unambiguous keys
        self.exact = {k: v.pop() for k, v in exact.items() if len(v) == 1}
        self.loose = {k: v.pop() for k, v in loose.items() if len(v) == 1}

    def lookup(self, candidate_name: str, office: str, district: str,
               election_year: str) -> dict | None:
        last, first = _split_last_first(candidate_name)
        if not last:
            return None
        okey = _office_key(office)
        variants = _name_variants(first) or {""}

        for variant in variants:
            party = self.exact.get((last, variant, okey, district, election_year))
            if party:
                return {"party": party, "party_source": "ks_sos_candidate_list",
                        "match_confidence": "exact"}
        for variant in variants:
            party = self.loose.get((last, variant, election_year))
            if party:
                return {"party": party, "party_source": "ks_sos_candidate_list",
                        "match_confidence": "high"}
        return None


# ========================== CSV loading ===============================

def _read_csv(path: Path) -> list[dict]:
    """Read a scraper CSV, tagging each row with its source line number.

    `_row_num` becomes the cleaned row's `row_num`, so with `raw_file` it
    points back at an exact input line — the traceability pair columns.py
    describes, and what Alaska and Wyoming write.
    """
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rows = []
        for n, row in enumerate(csv.DictReader(f), start=2):   # 2 = after header
            row["_row_num"] = n
            rows.append(row)
        return rows


def _row_signature(row: dict) -> tuple:
    """Everything about a row except where it came from."""
    return tuple(sorted((k, v) for k, v in row.items() if k != "_row_num"))


def _filing_sort_key(row: dict) -> tuple:
    """How recent a filing version is: filed date, then amendment, then original."""
    parts = _split_candidate_uid(row.get("candidate_uid", ""))
    def d(v):
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime((v or "").strip(), fmt).date()
            except ValueError:
                continue
        return date.min
    return (max(d(row.get("filed_date", "")), d(parts["amendment_date"])),
            d(parts["original_date"]),
            row.get("candidate_uid", ""))


def resolve_amendments(summary_by_uid: dict[str, dict]) -> tuple[dict[str, dict], dict[str, str], int]:
    """Keep one filing per (candidate, reporting period): the newest version.

    An amendment is a **separate row** in the CFR Examiner's results grid, not
    a flag on the original, and each version restates the entire period. The
    grid also returns a filing under every search window its filing date falls
    in, so an amendment filed in 2020 to a 2018 report comes back under both
    the 2018 and 2020 cycle searches. Left alone, both effects multiply the
    same money: 1,707 of 5,165 periods in the 2026-08 data were filed more
    than once (up to 12 times), and $105.9M of the $167.4M in those filings is
    superseded.

    This is what the old PDF parser meant by "prefer amendments when both
    exist for the same (candidate, period)".

    Returns (winners_by_uid, election_year_by_uid, superseded_count).
    `election_year` comes from the *earliest* cycle in each group: a 2018
    report is a 2018-cycle filing even when its latest amendment was filed in
    2020 and so was found by the 2020 search.
    """
    groups: dict[tuple, list[dict]] = collections.defaultdict(list)
    ungrouped: list[dict] = []
    for uid, row in summary_by_uid.items():
        period = (_clean(row.get("period_start", "")), _clean(row.get("period_end", "")))
        name = utils.clean_name(_clean(row.get("candidate_name", "")))
        if all(period) and name:
            groups[(name, *period)].append(row)
        else:
            ungrouped.append(row)          # can't identify the period — keep as-is

    winners: dict[str, dict] = {}
    election_year: dict[str, str] = {}
    superseded = 0
    for members in groups.values():
        best = max(members, key=_filing_sort_key)
        superseded += len(members) - 1
        uid = best["candidate_uid"]
        winners[uid] = best
        cycles = [_election_year_clean(_split_candidate_uid(m["candidate_uid"])["cycle_label"])
                  for m in members]
        cycles = [c for c in cycles if c]
        election_year[uid] = min(cycles) if cycles else ""

    for row in ungrouped:
        uid = row["candidate_uid"]
        winners[uid] = row
        election_year[uid] = _election_year_clean(
            _split_candidate_uid(uid)["cycle_label"])

    return winners, election_year, superseded


def _dedup_rescrapes(rows: list[dict], scrape_count: dict[str, int]) -> tuple[list[dict], int]:
    """Collapse rows belonging to filings that were scraped more than once.

    The scraper appends and re-checks current-year cycles on every run, so a
    filing scraped k times contributes k copies of each of its rows. Where the
    row count divides evenly by k the last block is kept — that's the most
    recent scrape, so an amended filing wins. Otherwise (content changed
    between scrapes) each distinct row is kept count//k times, which is the
    conservative option: it preserves genuine repeat donations of the same
    amount on the same day, which do occur.
    """
    if not rows:
        return rows, 0
    by_uid: dict[str, list[dict]] = collections.defaultdict(list)
    for r in rows:
        by_uid[r.get("candidate_uid", "")].append(r)

    kept: list[dict] = []
    dropped = 0
    for uid, group in by_uid.items():
        k = scrape_count.get(uid, 1)
        if k <= 1:
            kept.extend(group)
            continue
        if len(group) % k == 0:
            block = len(group) // k
            kept.extend(group[-block:])
            dropped += len(group) - block
        else:
            quota = {sig: max(1, cnt // k) for sig, cnt
                     in collections.Counter(_row_signature(r) for r in group).items()}
            for r in group:
                sig = _row_signature(r)
                if quota.get(sig, 0) > 0:
                    quota[sig] -= 1
                    kept.append(r)
                else:
                    dropped += 1
    kept.sort(key=lambda r: r.get("_row_num", 0))
    return kept, dropped


# ============================== run ==================================

def run():
    log = get_logger("kansas", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    try:
        _run(log, t0)
    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


def _run(log, t0: float):
    # ── Load the scraper's output CSVs ─────────────────────────────────
    # Raised, not sys.exit()'d: SystemExit is a BaseException, so exiting here
    # would skip run()'s handler and emit no parse_completed event at all,
    # leaving the run report showing a parse that started and never finished.
    summary_path = _input_path("summary")
    summary_rows = _read_csv(summary_path)
    if not summary_rows:
        raise FileNotFoundError(
            f"{summary_path} is missing or empty — run "
            f"src/pipeline/scrapers/kansas.py first")

    sched_a_rows = _read_csv(_input_path("schedule_a"))
    sched_b_rows = _read_csv(_input_path("schedule_b"))
    sched_c_rows = _read_csv(_input_path("schedule_c"))

    if MANIFEST.exists():
        manifest_rows = _read_csv(MANIFEST)
        log.info(f"  {len(summary_rows):,} candidate filings scraped "
                 f"({len(manifest_rows):,} in manifest)")
    else:
        log.info(f"  {len(summary_rows):,} candidate filings scraped (no manifest.csv found)")

    # ── Collapse re-scraped filings ────────────────────────────────────
    scrape_count = collections.Counter(r.get("candidate_uid", "") for r in summary_rows)
    rescraped = {u: c for u, c in scrape_count.items() if c > 1}
    if rescraped:
        log.info(f"  {len(rescraped):,} filings were scraped more than once "
                 f"(up to {max(rescraped.values())}x) — collapsing duplicate rows")

    dropped_dupes = {}
    sched_a_rows, dropped_dupes["schedule_a"] = _dedup_rescrapes(sched_a_rows, scrape_count)
    sched_b_rows, dropped_dupes["schedule_b"] = _dedup_rescrapes(sched_b_rows, scrape_count)
    sched_c_rows, dropped_dupes["schedule_c"] = _dedup_rescrapes(sched_c_rows, scrape_count)

    # Keep the LAST summary row per filing — the most recent scrape.
    summary_by_uid: dict[str, dict] = {}
    for row in summary_rows:
        uid = row.get("candidate_uid", "")
        if uid:
            summary_by_uid[uid] = row

    # ── Keep only the newest version of each reporting period ──────────
    summary_by_uid, election_year_by_uid, superseded = resolve_amendments(summary_by_uid)
    if superseded:
        log.info(f"  {superseded:,} superseded filings dropped (amendments restate "
                 f"the whole period) — {len(summary_by_uid):,} current filings remain")

    live_uids = set(summary_by_uid)
    def _keep_live(rows):
        kept = [r for r in rows if r.get("candidate_uid", "") in live_uids]
        return kept, len(rows) - len(kept)

    sched_a_rows, sup_a = _keep_live(sched_a_rows)
    sched_b_rows, sup_b = _keep_live(sched_b_rows)
    sched_c_rows, sup_c = _keep_live(sched_c_rows)
    dropped_dupes["schedule_a"] += sup_a
    dropped_dupes["schedule_b"] += sup_b
    dropped_dupes["schedule_c"] += sup_c

    for key, rows, dropped in (("schedule_a", sched_a_rows, dropped_dupes["schedule_a"]),
                               ("schedule_b", sched_b_rows, dropped_dupes["schedule_b"]),
                               ("schedule_c", sched_c_rows, dropped_dupes["schedule_c"])):
        log.file_parsed(INPUT_NAMES[key], key, len(rows), skipped=dropped)
    log.file_parsed(INPUT_NAMES["summary"], "summary", len(summary_by_uid),
                    skipped=len(summary_rows) - len(summary_by_uid))

    # ── Build the candidate lookup: candidate_uid -> resolved metadata ──
    cand_by_uid: dict[str, dict] = {}
    for uid, row in summary_by_uid.items():
        parts = _split_candidate_uid(uid)
        cand_name = utils.clean_name(_clean(row.get("candidate_name", "")))
        office    = _clean(row.get("office_sought", "")) or parts["office_sought"]
        district  = _normalize_district(row.get("district", "") or parts["district_number"])
        election_year = election_year_by_uid.get(
            uid, _election_year_clean(parts["cycle_label"]))
        # candidate_uid's amendment_date component is populated by the scraper
        # only when the CFR Examiner grid shows this filing as amended —
        # that's a real, known signal, unlike filing_id (no stable filing
        # identifier exists in the scraped data, so that column stays blank).
        amended = "1" if parts["amendment_date"] else ""
        committee_type = COMMITTEE_TYPE_BY_GROUP.get(
            parts["office_group"], CANDIDATE_COMMITTEE_TYPE)
        cand_by_uid[uid] = {
            # For a PAC or party committee this is the organisation's name and
            # there is no candidate behind it, so candidate_name stays blank on
            # its transactions and it never enters candidates.csv.gz.
            "filer_name":     cand_name,
            "committee_type": committee_type,
            "is_candidate":   committee_type == CANDIDATE_COMMITTEE_TYPE,
            "candidate_name": cand_name if committee_type == CANDIDATE_COMMITTEE_TYPE else "",
            "office":         office,
            "district":       district,
            "election_year":  election_year,
            "office_group":   parts["office_group"],
            "amended":        amended,
            # County is the only jurisdiction-shaped field in the source and is
            # filled on 91% of filings; it's what scopes a District Attorney
            # race. Kentucky and Alaska map their own equivalents the same way.
            "jurisdiction":   _clean(row.get("county", "")),
            # Candidate committees share the candidate's own address —
            # populate committees.csv.gz's city/zip from it below.
            "city":           _clean(row.get("city", "")),
            "zip":            _clean_zip(row.get("zip", "")),
        }

    # ── Output writers ─────────────────────────────────────────────────
    contrib_path = CLEAN_DIR / "contributions.csv.gz"
    expend_path  = CLEAN_DIR / "expenditures.csv.gz"
    loans_path   = CLEAN_DIR / "loans_debts.csv.gz"
    cand_path    = CLEAN_DIR / "candidates.csv.gz"
    comm_path    = CLEAN_DIR / "committees.csv.gz"

    contrib_rownum = expend_rownum = loan_rownum = 0
    unmatched = 0
    dropped_no_amount = dropped_no_date = 0
    contrib_f = expend_f = loans_f = None

    try:
        contrib_f = gzip.open(contrib_path, "wt", newline="", encoding="utf-8")
        expend_f  = gzip.open(expend_path,  "wt", newline="", encoding="utf-8")
        loans_f   = gzip.open(loans_path,   "wt", newline="", encoding="utf-8")

        contrib_w = csv.DictWriter(contrib_f, fieldnames=C.CONTRIBUTIONS,
                                   extrasaction="ignore", restval="")
        expend_w  = csv.DictWriter(expend_f,  fieldnames=C.EXPENDITURES,
                                   extrasaction="ignore", restval="")
        loans_w   = csv.DictWriter(loans_f,   fieldnames=C.LOANS_DEBTS,
                                   extrasaction="ignore", restval="")
        contrib_w.writeheader()
        expend_w.writeheader()
        loans_w.writeheader()

        def _candidate_for(txn: dict) -> dict:
            """Resolved filing metadata, or a name-only fallback.

            A schedule row whose candidate_uid has no summary row would
            otherwise be dropped, taking real money with it. Wyoming keeps such
            rows with blank enrichment instead; the schedules carry the filer's
            name text, which is enough to keep committee_name populated.
            """
            nonlocal unmatched
            cand = cand_by_uid.get(txn.get("candidate_uid", ""))
            if cand is not None:
                return cand
            unmatched += 1
            name = utils.clean_name(_clean(txn.get("candidate", "")))
            return {
                "filer_name": name, "candidate_name": name,
                "committee_type": CANDIDATE_COMMITTEE_TYPE, "is_candidate": True,
                "office": "", "district": "", "election_year": "",
                "office_group": "", "amended": "", "jurisdiction": "",
                "city": "", "zip": "",
            }

        # ── Contributions: Schedule A (minus loans) + Schedule B as In-Kind ──
        def _write_contribution_rows(rows: list[dict], source_key: str,
                                     forced_type: str, amount_field: str):
            nonlocal contrib_rownum, loan_rownum
            nonlocal dropped_no_amount, dropped_no_date
            raw_file = INPUT_NAMES[source_key]
            for txn in rows:
                cand = _candidate_for(txn)
                amount = _parse_amount(txn.get(amount_field, ""))
                txn_date = _parse_date(txn.get("date", ""))
                if not amount:
                    dropped_no_amount += 1
                    continue
                if not txn_date:
                    dropped_no_date += 1
                    continue

                payment_type = _clean(txn.get("type_of_payment", ""))
                shared = {
                    "state":             STATE,
                    "committee_name":    cand["filer_name"],
                    "amount":            amount,
                    "date":              txn_date,
                    "candidate_name":    cand["candidate_name"],
                    "election_year":     cand["election_year"],
                    "amended":           cand["amended"],
                    "raw_file":          raw_file,
                }

                # Loans are receipts the campaign has to repay, not income.
                if payment_type.lower() in LOAN_PAYMENT_TYPES:
                    loan_rownum += 1
                    loans_w.writerow({
                        **shared,
                        "original_amount":    amount,
                        "record_type":        "Loan",
                        "counterparty_name":  _clean(txn.get("contributor_name", "")),
                        "counterparty_city":  _clean(txn.get("contributor_city", "")),
                        "counterparty_state": _clean_state_code(txn.get("contributor_state", "")),
                        "counterparty_zip":   _clean_zip(txn.get("contributor_zip", "")),
                        "row_num":            txn.get("_row_num", ""),
                    })
                    continue

                contrib_rownum += 1
                contrib_w.writerow({
                    **shared,
                    "transaction_type":  forced_type or payment_type,
                    "contributor_name":  _clean(txn.get("contributor_name", "")),
                    "contributor_city":  _clean(txn.get("contributor_city", "")),
                    "contributor_state": _clean_state_code(txn.get("contributor_state", "")),
                    "contributor_zip":   _clean_zip(txn.get("contributor_zip", "")),
                    # Schedule B's own occupation column is used as-is; its
                    # in-kind `description` is transaction text, not an
                    # occupation, so it is deliberately not substituted here.
                    "occupation":        _clean(txn.get("occupation", "")),
                    "office":            cand["office"],
                    "row_num":           txn.get("_row_num", ""),
                })

        _write_contribution_rows(sched_a_rows, "schedule_a", "", "amount")
        _write_contribution_rows(sched_b_rows, "schedule_b", "In-Kind", "value")

        # ── Expenditures: Schedule C ────────────────────────────────────
        raw_file_c = INPUT_NAMES["schedule_c"]
        for txn in sched_c_rows:
            cand = _candidate_for(txn)
            amount = _parse_amount(txn.get("amount", ""))
            txn_date = _parse_date(txn.get("date", ""))
            if not amount:
                dropped_no_amount += 1
                continue
            if not txn_date:
                dropped_no_date += 1
                continue
            category, purpose = _split_purpose(txn.get("purpose_raw", ""))
            expend_rownum += 1
            expend_w.writerow({
                "state":            STATE,
                "committee_name":   cand["filer_name"],
                "amount":           amount,
                "date":             txn_date,
                "transaction_type": _TXN_TYPE_BY_CATEGORY.get(category, "Expenditure"),
                "payee_name":       _clean(txn.get("payee_name", "")),
                "purpose":          purpose,
                "category":         category,
                "payee_city":       _clean(txn.get("payee_city", "")),
                "payee_state":      _clean_state_code(txn.get("payee_state", "")),
                "payee_zip":        _clean_zip(txn.get("payee_zip", "")),
                "candidate_name":   cand["candidate_name"],
                "office":           cand["office"],
                "election_year":    cand["election_year"],
                "amended":          cand["amended"],
                "raw_file":         raw_file_c,
                "row_num":          txn.get("_row_num", ""),
            })
    finally:
        for handle in (contrib_f, expend_f, loans_f):
            if handle is not None:
                handle.close()

    log.file_parsed("contributions.csv.gz", "contributions", contrib_rownum, role="output")
    log.file_parsed("expenditures.csv.gz", "expenditures", expend_rownum, role="output")
    log.file_parsed("loans_debts.csv.gz", "loans_debts", loan_rownum, role="output")
    log.info(f"  Dropped: {dropped_no_amount:,} with no amount, "
             f"{dropped_no_date:,} with an unusable date. "
             f"Unmatched candidate_uid: {unmatched:,}")

    # ── Write candidates.csv.gz ────────────────────────────────────────
    # Dedup key mirrors the old parser: a candidate can file multiple R&E
    # periods within the same cycle, but they collapse to one candidates.csv
    # row (name, office, district, election_year) — NOT one row per
    # candidate_uid, since candidate_uid varies per filing period.
    roster_rows = _read_csv(_input_path("roster"))
    party_index = PartyIndex(roster_rows) if roster_rows else None
    if party_index:
        log.file_parsed(INPUT_NAMES["roster"], "party_roster", len(roster_rows),
                        skipped=len(roster_rows) - party_index.rows)
    else:
        log.warning("  No candidate_roster.csv — candidates will have no party. "
                    "Re-run the scraper without --no-roster to fetch it.")

    candidates_seen: dict[tuple, dict] = {}
    for uid, cand in cand_by_uid.items():
        if not cand["is_candidate"]:
            continue            # PACs and party committees aren't candidates
        cand_key = (cand["candidate_name"], cand["office"], cand["district"], cand["election_year"])
        if cand_key not in candidates_seen:
            candidates_seen[cand_key] = {
                "state":          STATE,
                "candidate_name": cand["candidate_name"],
                "office":         cand["office"],
                "district":       cand["district"],
                "election_year":  cand["election_year"],
                "jurisdiction":   cand["jurisdiction"],
                "party":          "",   # filled from the roster below
                "state_filer_id": "",   # not available in source
                "raw_file":       INPUT_NAMES["summary"],
                "row_num":        summary_by_uid[uid].get("_row_num", ""),
                # carried through to committees.csv.gz below, not part of
                # C.CANDIDATES itself (extrasaction="ignore" drops it there)
                "_city":          cand["city"],
                "_zip":           cand["zip"],
            }

    cand_rows = []
    conf_counts = {"exact": 0, "high": 0}
    for meta_row in candidates_seen.values():
        first, last = _split_name(meta_row["candidate_name"])
        row = {
            **meta_row,
            "person_id":       "",   # filled by assign_person_ids
            "candidate_first": first,
            "candidate_last":  last,
            "incumbent":       "",   # not available in source
        }
        if party_index:
            hit = party_index.lookup(meta_row["candidate_name"], meta_row["office"],
                                     meta_row["district"], meta_row["election_year"])
            if hit:
                row["party"]            = hit["party"]
                row["party_source"]     = hit["party_source"]
                row["match_confidence"] = hit["match_confidence"]
                conf_counts[hit["match_confidence"]] += 1
        cand_rows.append(row)

    if party_index and cand_rows:
        matched = sum(conf_counts.values())
        log.info(f"  Party matched: {matched:,}/{len(cand_rows):,} candidates "
                 f"({matched / len(cand_rows):.0%}) — "
                 f"{conf_counts['exact']:,} exact, {conf_counts['high']:,} high")

    with gzip.open(cand_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.CANDIDATES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(cand_rows)

    n_cands = utils.assign_person_ids(cand_path, id_model="name_hash")
    log.file_parsed("candidates.csv.gz", "candidates", n_cands, role="output")

    # ── Write committees.csv.gz ────────────────────────────────────────
    # One committee row per candidate (their own campaign), same as the old
    # parser — Kansas has no separate committee filer concept in this source.
    # city/zip come from the candidate's own address, since a candidate
    # committee's address is the candidate's address. treasurer_name is
    # deliberately blank: the summary's signature_name is the report's
    # e-signature and can be either the candidate or the treasurer, with no
    # way to tell which, so mapping it would be worse than leaving it empty.
    # Every filer gets a committee row: a candidate's own campaign committee,
    # plus the PACs and party committees scraped from the viewer's other
    # category. `candidate_name` is only populated for a candidate's own
    # committee — a PAC is legally separate from any candidate, and
    # assign_committee_person_ids leaves its person_id NULL by design
    # (see columns.py). Which committee an independent PAC supports is
    # enrichment, handled by enrich.py from a hand-reviewed registry.
    committees_seen: dict[tuple, dict] = {}
    for cand in cand_by_uid.values():
        key = (cand["filer_name"], cand["committee_type"], cand["election_year"])
        committees_seen.setdefault(key, cand)

    with gzip.open(comm_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.COMMITTEES, extrasaction="ignore", restval="")
        w.writeheader()
        for ri, cand in enumerate(committees_seen.values(), start=1):
            w.writerow({
                "state":          STATE,
                "person_id":      "",   # filled by assign_committee_person_ids
                "committee_name": cand["filer_name"],
                "committee_type": cand["committee_type"],
                "election_year":  cand["election_year"],
                "candidate_name": cand["candidate_name"],
                "city":           cand["city"],
                "zip":            cand["zip"],
                "state_filer_id": "",
                "raw_file":       INPUT_NAMES["summary"],
                "row_num":        ri,
            })

    n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
    by_type = collections.Counter(c["committee_type"] for c in committees_seen.values())
    log.file_parsed("committees.csv.gz", "committees", len(committees_seen), role="output")
    log.info(f"  Committee types: {dict(by_type)} — {n_comm_matched:,} matched to candidates")

    duration = round(time.perf_counter() - t0, 1)
    log._emit("parse_completed",
              status="completed",
              duration_s=duration,
              contributions=contrib_rownum,
              expenditures=expend_rownum,
              loans_debts=loan_rownum,
              candidates=n_cands,
              duplicate_rows_dropped=sum(dropped_dupes.values()),
              unmatched=unmatched)
    log.info(f"Done in {duration}s")


# ============================= CLI ===================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
