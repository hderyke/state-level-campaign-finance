"""
parsers/pennsylvania.py — Parse the PA Department of State's "Full Campaign
Finance Export" zip files into canonical cleaned CSVs.

Input:  data/Pennsylvania/raw/{year}.zip (one zip per election-finance year,
2000-present, downloaded as-is from
https://www.pa.gov/agencies/dos/resources/voting-and-elections-resources/campaign-finance-data)

Each zip contains five fixed-format CSVs (see the DOS "Technical
Specifications for Electronic Filing of Campaign Expense Reports"):
    filer_{year}.txt    — one row per report cover sheet (candidate/
                           committee/lobbyist identity + address + party/
                           office for that filing)
    contrib_{year}.txt  — Schedule I Parts A-D (contributions) and
                           Schedule II Parts F-G (in-kind contributions)
    expense_{year}.txt  — Schedule III (expenditures)
    debt_{year}.txt     — Schedule IV (debts/loans owed by the filer)
    receipt_{year}.txt  — Schedule I Part E (other receipts: refunds,
                           interest, returned checks, etc.)

Output: data/Pennsylvania/cleaned/ (combined across every year processed —
PA filer/committee IDs are stable across cycles, so candidates.csv.gz and
committees.csv.gz are deduped globally, not per-year)
    contributions.csv.gz, expenditures.csv.gz, debts.csv.gz,
    candidates.csv.gz, committees.csv.gz

Joining transactions to their filer:
    Every row in every one of the five files carries a CampaignFinanceID —
    a per-report-submission ID, NOT the same as FILERID (which identifies
    the filer entity itself and is stable across an entity's many
    submissions). CampaignFinanceID is the join key used here: it ties a
    transaction to the *exact* cover-sheet row it was submitted with,
    which is more precise than joining on FILERID (that would blur
    together every cycle/amendment a filer ever submitted). See
    filer.txt's CampaignfinanceID column (note the inconsistent
    capitalization vs. the other four files' CampaignFinanceID — both are
    handled below).

Why this parser streams instead of loading full lists (unlike kansas_v2.py):
    Kansas's scraped inputs are small (one state's worth of candidate
    filings). PA's exports are much larger — contrib_2026.txt alone is
    ~370k rows for a single year, and the full site holds 2000-2026. Only
    filer_{year}.txt (a few thousand rows/year) is materialized in memory;
    the four transaction files are streamed row-by-row straight to the
    output gzip writers.

Zip layout: 2025+ zips have the five .txt files at the zip root
(filer_2026.txt); pre-2025 zips nest them one level down inside a
"{year}/" folder (2018/filer_2018.txt) — DOS changed this at some point
and never repackaged the older years. _resolve_member() checks both
locations for every file, so both eras read the same way; nothing else
in this module needs to know which layout a given year uses.

FILERTYPE (from filer.txt): 1 Candidate, 2 Committee, 3 Lobbyist (per the
DOS spec). candidates.csv.gz is built only from type 1 rows, where the
filer name IS the candidate's own "Last, First" name per the spec.
committees.csv.gz includes all three types (committee_type records which).

office/district/party are NOT gated to type 1: a real subset of type-2
"committee" filers (candidate-authorized committees like "FRIENDS OF
THOMAS WEST" or "ELECT ROB FRANCIS PAC") also fill in the office/
district/party cover-sheet fields even though they're typed as
Committee rather than Candidate — that's populated straight through to
committees.csv.gz/contributions.csv.gz/expenditures.csv.gz whenever
present, regardless of FILERTYPE. What this does NOT do is invent a
candidates.csv row or a person_id for those committees: their filer
name is a committee name ("FRIENDS OF X"), not the candidate's personal
name, so there's no reliable way to derive "X" from it and match it to
that candidate's own type-1 row (if one even exists) without risking
wrong merges. Net effect: candidates.csv.gz undercounts real candidates
— many only ever have a type-2 committee and never file a type-1
report themselves — but their transactions are still captured
correctly in committees.csv.gz/contributions.csv.gz/expenditures.csv.gz,
just without a person_id link. This is a genuine limitation of the
source data, not something a parser can safely paper over.

id_model = "committee": FILERID is a per-committee ID, not a per-person
ID (a candidate can end up with more than one committee ID over the
years, e.g. after forming a new committee for a different office). Same
grouping utils.assign_person_ids() uses for AZ/AL/CA — (state,
normalized candidate_name, office, district) — collapses those correctly
for PA too, since candidate filer-name IS the candidate's own name.

Party: raw 3-letter code passed through as scraped (DEM/REP/CST/LIB/REF/
OTH per the DOS table; a few filings use codes outside that table, e.g.
INT/NOA — left as-is rather than guessed at, for aggregate.py to
normalize).

Contribution "Section" codes (IA/IB/IC/ID/IIF/IIG, identifying which part
of Schedule I/II a contribution came from) are passed through as
transaction_type rather than translated to a contributor_type here —
the DOS spec doesn't define a clean contributor-type/entity mapping for
each section, so guessing one would be worse than leaving it to
aggregate.py's canonicalization step.

Dropped fields (no canonical schema slot — documented, not silently
lost):
    - Employer address/city/zip on contrib.txt Part D/Part G rows
      (only employer *name* has a column in C.CONTRIBUTIONS).
    - RECDESC (receipt.txt's description, e.g. "refund", "interest
      income") — C.CONTRIBUTIONS has no free-text description field.
      receipt.txt rows are folded into contributions.csv.gz with
      transaction_type="Other Receipt" so the dollar amounts aren't lost,
      just the description text.
"""

import csv
import gzip
import html
import io
import re
import sys
import time
import zipfile
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
DATA_DIR  = PROJECT_ROOT / "data" / "Pennsylvania"
RAW_DIR   = DATA_DIR / "raw"          # holds {year}.zip files
CLEAN_DIR = DATA_DIR / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "PA"
EARLIEST_YEAR  = 2000          # earliest year DOS publishes a full export for
MAX_VALID_YEAR = date.today().year + 1

FILER_TYPE_LABEL = {"1": "Candidate", "2": "Committee", "3": "Lobbyist"}

# ======================== Date / amount helpers =======================

def _parse_date(val: str) -> str:
    """YYYYMMDD -> YYYY-MM-DD. '' / '0' (PA's "unused" sentinel for
    Contribution Date 2/3) and out-of-range years return ''."""
    v = (val or "").strip()
    if not v or v == "0":
        return ""
    try:
        d = datetime.strptime(v, "%Y%m%d").date()
    except ValueError:
        return ""
    if EARLIEST_YEAR <= d.year <= MAX_VALID_YEAR:
        return d.strftime("%Y-%m-%d")
    return ""


def _parse_amount(val: str) -> str:
    """Plain decimal string, same convention as the rest of the pipeline.
    '' on empty/unparseable input."""
    v = (val or "").strip()
    if not v:
        return ""
    try:
        return str(float(v))
    except ValueError:
        return ""


def _clean(val: str) -> str:
    """Collapse whitespace (PA's fixed-width source fields are padded
    with trailing spaces, e.g. FILERNAME/CITY) and unescape the stray
    HTML entities (&amp; etc.) present in some free-text fields."""
    return re.sub(r"\s+", " ", html.unescape(val or "")).strip()


def _normalize_district(raw: str) -> str:
    """Strip leading zeros (e.g. '005' -> '5'); pass through as-is if
    not numeric."""
    try:
        return str(int(raw))
    except (ValueError, TypeError):
        return _clean(raw)


def _split_name(full: str) -> tuple[str, str]:
    """'Last, First Middle' -> (first, last); PA candidate filer names
    are written 'Last, First' per the DOS spec. Falls back to first/last
    token position if no comma is present (committees/lobbyists, or the
    rare candidate row that doesn't follow the convention)."""
    if "," in full:
        parts = full.split(",", 1)
        return _clean(parts[1]), _clean(parts[0])   # (first, last)
    tokens = full.split()
    first = tokens[0] if tokens else ""
    last  = tokens[-1] if len(tokens) > 1 else ""
    return first, last


def _pick_latest(rows: list[dict]) -> dict:
    """Given every filer.txt row for one FILERID within one year, return
    the one from the most recently submitted report (ties broken by
    CYCLE) — used as the canonical name/address/party for that filer for
    that year, since later reports supersede earlier ones."""
    def _key(r):
        try:
            cycle = int(r.get("CYCLE") or 0)
        except ValueError:
            cycle = 0
        return (r.get("SubmittedDate") or "", cycle)
    return max(rows, key=_key)


# ========================== Zip / CSV helpers ==========================

def _year_zips() -> list[tuple[int, Path]]:
    """Every data/Pennsylvania/raw/{year}.zip on disk, sorted oldest-first."""
    out = []
    for p in sorted(RAW_DIR.glob("*.zip")):
        m = re.fullmatch(r"(\d{4})\.zip", p.name)
        if not m:
            continue
        year = int(m.group(1))
        if EARLIEST_YEAR <= year <= MAX_VALID_YEAR:
            out.append((year, p))
    return out


def _resolve_member(zf: zipfile.ZipFile, name: str) -> str | None:
    """Return the actual zip member path for `name`, checking both known
    DOS layouts: 2025+ zips have it at the root (filer_2026.txt);
    pre-2025 zips nest it one level down inside a "{year}/" folder
    (2018/filer_2018.txt). Returns None if not found in either form."""
    names = zf.namelist()
    if name in names:
        return name
    suffix = "/" + name
    for n in names:
        if n.endswith(suffix):
            return n
    return None


def _rows(zf: zipfile.ZipFile, name: str):
    """Stream a member of the zip as DictReader rows (root or nested
    under a year folder — see _resolve_member). Yields nothing if the
    member isn't present in either form (e.g. an older year's zip
    missing a file that was introduced later)."""
    member = _resolve_member(zf, name)
    if member is None:
        return
    with zf.open(member) as raw:
        # PA's exports are plain ASCII/Latin-1; latin-1 never raises on
        # decode (unlike utf-8), which matters for decades-old files.
        f = io.TextIOWrapper(raw, encoding="latin-1", newline="")
        yield from csv.DictReader(f)


# ================= Committee -> candidate linkage overrides ===========
#
# committees_seen's candidate_name is normally only populated when a
# FILERTYPE=2 committee's own cover sheet lists an OFFICE (see where
# committees_seen is built, above) -- but PA's big statewide "for
# Governor"/"for Pennsylvania" committees usually don't, because PA's
# filer.txt has no field anywhere linking a committee filer to its
# candidate's own filer registration (confirmed by inspecting
# filer_2022.txt directly: "Shapiro for Pennsylvania" FILERID 20160016
# and "SHAPIRO, JOSHUA D" FILERID 2022C0206 share no column). Left
# as-is, a committee like this gets bucketed under "non-candidate
# committees" instead of its candidate -- happened to "Shapiro for
# Pennsylvania" ($130.9M raised) in the 2026-07-11 report.
#
# A general fix (fuzzy-matching committee names against candidate
# surnames) was tried and rejected: even gated on "committee name
# contains a surname unique to one candidate", it produced roughly 1
# false link in 10 on a random sample -- e.g. "Friends of David Freed"
# matched to an unrelated "Slavick David" (whose committee-style
# candidate_name coincidentally ends in "DAVID"), "Friends of James
# Craig" matched to an unrelated "Craig, Cheryl Allen". PA's own source
# data has enough dirty/committee-style candidate_name values and
# first-name/surname collisions that blind matching isn't safe, and
# this isn't the place to build a general fuzzy-identity resolver
# (that's utils.py's job, shared and much more conservative, across
# every state -- see CLAUDE.md on per-state scope).
#
# Instead: a small, hand-verified table. state_filer_id -> the exact
# candidate_name string (as it appears in candidates_seen) for each
# distinct candidacy that filer's committee actually supported, so a
# multi-cycle committee (like Shapiro's, which financed both his 2016
# AG run and his 2022/2026 GOV runs under the same FILERID) links each
# era's committee rows to the correct candidacy.
PA_COMMITTEE_CANDIDATE_OVERRIDE: dict[str, list[tuple[int, str]]] = {
    # "Shapiro for Pennsylvania" -- AG run financed by the same
    # committee that later became his GOV committee.
    "20160016": [(2016, "JOSH SHAPIRO"),
                 (2022, "SHAPIRO, JOSHUA D"),
                 (2026, "SHAPIRO, JOSHUA D")],
    # "Tom Wolf for Governor", 2014 run (FILERID only appears
    # 2013-2017 in committees_seen; no later WOLF committee shares
    # this FILERID, so a single entry is sufficient).
    "20130153": [(2014, "THOMAS W WOLF")],
    # "Friends of Doug Mastriano" / "Mastriano, Doug Friends Of" --
    # 2019/2022 STS+GOV run, later 2024 STS re-election under a
    # slightly different registered name.
    "20190138": [(2022, "DOUG MASTRIANO"),
                 (2024, "DOUGLAS V. MASTRIANO")],
    # "Wagner, Scott for Governor, Inc." -- his 2018 GOV run. (Distinct
    # from "Scott Wagner for Senate", FILERID 20130261, his separate
    # 2014 State Senate committee -- not in this table since it's a
    # much smaller committee, not part of the gap Henry flagged; add
    # an entry for it too if it turns out to matter.)
    "20160359": [(2014, "SCOTT WAGNER"),
                 (2018, "WAGNER,SCOTT R")],
    # "Lou Barletta for Governor" -- his 2022 GOV run. (Not "Barletta,
    # Lou for Congress", a separate earlier FILERID for his U.S. House
    # seat -- no PA candidate row exists for federal office anyway.)
    "20210206": [(2022, "BARLETTA, LOUIS")],
}
# NOTE on Tom Corbett: "Tom Corbett for Governor" (FILERID 2009216) is
# deliberately NOT in this table. Its OFFICE field IS populated on its
# own cover sheet (so the existing heuristic above already sets
# candidate_name to "TOM CORBETT FOR GOVERNOR", not blank) -- but there
# is no FILERTYPE=1 candidate row for him at all in 2010 or 2014
# filer.txt (confirmed directly: Corbett has no GOV-office FILERTYPE=1
# filing either year, only an unrelated ATT filing under a different
# FILERID in 2004/2008). His committee can't be linked to a candidate
# row because no such row exists to link to. Fixing that would mean
# synthesizing a candidates.csv row from committee data alone -- a
# bigger change than this override table, flagged here rather than
# silently invented.


def _override_candidate_name(filer_id: str, election_year) -> str | None:
    """
    Look up PA_COMMITTEE_CANDIDATE_OVERRIDE for `filer_id`, returning
    whichever override entry's election_year is closest to
    `election_year` (so an early-cycle row links to the earlier
    candidacy and a late-cycle one to the later candidacy, rather than
    always picking the same one). Returns None if filer_id isn't in
    the table.

    Shared by both committees_seen (see _apply_committee_candidate_overrides
    below) and every per-transaction candidate_name assignment in _run
    (contributions/receipts/expenditures/debts) -- queries.py's
    "TOP 20 RECIPIENT CANDIDATES" report joins on
    contributions.candidate_name, NOT committees.candidate_name, so
    fixing committees_seen alone (as an earlier version of this fix
    did) has no effect on that report; both need the override applied.
    """
    entries = PA_COMMITTEE_CANDIDATE_OVERRIDE.get(filer_id or "")
    if not entries:
        return None
    try:
        yr = int(election_year)
    except (ValueError, TypeError):
        yr = entries[-1][0]
    _, cand_name = min(entries, key=lambda e: (abs(e[0] - yr), e[0] > yr))
    return cand_name


def _candidate_name_for_rep(rep: dict) -> str:
    """
    candidate_name to attach to a contributions/receipts/expenditures/
    debts row for report `rep` (a report_by_cfid entry). Tries the
    OFFICE heuristic first (matches the committees_seen logic above),
    then falls back to PA_COMMITTEE_CANDIDATE_OVERRIDE keyed on the
    report's own filer_id. Needed because queries.py's "recipient
    candidates" report joins on contributions.candidate_name directly
    -- fixing committees_seen alone doesn't reach it.
    """
    if rep["office"]:
        return rep["filer_name"]
    override = _override_candidate_name(rep.get("filer_id", ""), rep.get("election_year"))
    return override or ""


def _apply_committee_candidate_overrides(committees_seen: dict, log) -> int:
    """
    Apply PA_COMMITTEE_CANDIDATE_OVERRIDE to committees_seen via
    _override_candidate_name.
    """
    linked = 0
    for row in committees_seen.values():
        cand_name = _override_candidate_name(row.get("state_filer_id", ""),
                                              row.get("election_year"))
        if cand_name is None:
            continue
        if row.get("candidate_name") != cand_name:
            row["candidate_name"] = cand_name
            linked += 1

    if linked:
        log.info(f"  Applied {linked:,} hand-verified committee->candidate "
                 f"override(s) (see PA_COMMITTEE_CANDIDATE_OVERRIDE)")
    return linked


# ============================== run ==================================

def run(years: list[int] | None = None):
    log = get_logger("pennsylvania", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    try:
        _run(log, t0, years)
    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


def _run(log, t0: float, years: list[int] | None):
    zips = _year_zips()
    if years is not None:
        wanted = set(years)
        zips = [(y, p) for y, p in zips if y in wanted]

    if not zips:
        print("ERROR: no data/Pennsylvania/raw/{year}.zip files found — "
              "run the Pennsylvania scraper first")
        sys.exit(1)

    log.info(f"  Processing {len(zips):,} year(s): "
             f"{', '.join(str(y) for y, _ in zips)}")

    # ── Output writers (opened once, shared across every year) ─────────
    contrib_path = CLEAN_DIR / "contributions.csv.gz"
    expend_path  = CLEAN_DIR / "expenditures.csv.gz"
    debts_path   = CLEAN_DIR / "debts.csv.gz"
    cand_path    = CLEAN_DIR / "candidates.csv.gz"
    comm_path    = CLEAN_DIR / "committees.csv.gz"

    contrib_f = gzip.open(contrib_path, "wt", newline="", encoding="utf-8")
    expend_f  = gzip.open(expend_path,  "wt", newline="", encoding="utf-8")
    debts_f   = gzip.open(debts_path,   "wt", newline="", encoding="utf-8")

    contrib_w = csv.DictWriter(contrib_f, fieldnames=C.CONTRIBUTIONS,
                               extrasaction="ignore", restval="")
    expend_w  = csv.DictWriter(expend_f,  fieldnames=C.EXPENDITURES,
                               extrasaction="ignore", restval="")
    debts_w   = csv.DictWriter(debts_f,   fieldnames=C.LOANS_DEBTS,
                               extrasaction="ignore", restval="")
    contrib_w.writeheader()
    expend_w.writeheader()
    debts_w.writeheader()

    contrib_rownum = 0
    expend_rownum  = 0
    debt_rownum    = 0
    unmatched      = 0

    # Accumulated across every year, deduped globally at the end.
    # key: (FILERID, election_year) -> latest filer.txt row + decoded fields
    committees_seen: dict[tuple, dict] = {}
    # key: (candidate_name, office, district, election_year) -> same
    candidates_seen: dict[tuple, dict] = {}

    for year, zpath in zips:
        yr_str = str(year)
        raw_prefix = f"{zpath.name}:"
        log.info(f"  {year} ({zpath.name})")

        with zipfile.ZipFile(zpath) as zf:
            filer_name   = f"filer_{yr_str}.txt"
            contrib_name = f"contrib_{yr_str}.txt"
            expense_name = f"expense_{yr_str}.txt"
            debt_name    = f"debt_{yr_str}.txt"
            receipt_name = f"receipt_{yr_str}.txt"

            # ── Load filer.txt for this year (small — a few thousand
            # rows) and group by FILERID so we can pick the latest
            # cover-sheet per filer per year. ─────────────────────────
            filer_rows = list(_rows(zf, filer_name))
            if not filer_rows:
                log.warning(f"  ✗ {filer_name}: not found or empty, skipping {year}")
                continue

            by_filer_id: dict[str, list[dict]] = {}
            report_by_cfid: dict[str, dict] = {}
            for row in filer_rows:
                fid = _clean(row.get("FILERID", ""))
                cfid = _clean(row.get("CampaignfinanceID", ""))
                if fid:
                    by_filer_id.setdefault(fid, []).append(row)
                if not cfid:
                    continue
                office   = _clean(row.get("OFFICE", ""))
                district = _normalize_district(row.get("DISTRICT", ""))
                party    = _clean(row.get("PARTY", ""))
                ftype    = _clean(row.get("FILERTYPE", ""))
                name     = utils.clean_name(_clean(row.get("FILERNAME", "")))
                # office/district/party pass through whenever present on the
                # cover sheet, regardless of FILERTYPE — see module docstring
                # ("FRIENDS OF THOMAS WEST"-style type-2 committees that do
                # fill these in).
                report_by_cfid[cfid] = {
                    "filer_id":       fid,
                    "filer_name":     name,
                    "filer_type":     ftype,
                    "office":         office,
                    "district":       district,
                    "party":          party,
                    "election_year":  yr_str,
                    "amended":        "Y" if _clean(row.get("AMMEND", "")) == "Y" else "N",
                    "terminated":     _clean(row.get("TERMINATE", "")) == "Y",
                    "city":           _clean(row.get("CITY", "")),
                    "zip":            utils.clean_zip(row.get("ZIPCODE", "")),
                }

            # ── committees_seen / candidates_seen: pick the latest
            # report per FILERID for this year as canonical. ──────────
            for fid, rows in by_filer_id.items():
                latest = _pick_latest(rows)
                ftype  = _clean(latest.get("FILERTYPE", ""))
                name   = utils.clean_name(_clean(latest.get("FILERNAME", "")))
                office   = _clean(latest.get("OFFICE", ""))
                district = _normalize_district(latest.get("DISTRICT", ""))
                party    = _clean(latest.get("PARTY", ""))

                comm_key = (fid, yr_str)
                committees_seen[comm_key] = {
                    "state":          STATE,
                    "committee_name": name,
                    "committee_type": FILER_TYPE_LABEL.get(ftype, ftype),
                    "election_year":  yr_str,
                    # Populate candidate_name whenever this filer is evidently
                    # tied to a specific race (office present) — not just for
                    # FILERTYPE 1. For type-2 committees this is the
                    # committee's own name (e.g. "FRIENDS OF THOMAS WEST"),
                    # not a clean personal name — still more useful than
                    # blank, but see module docstring on why it isn't used
                    # to synthesize a candidates.csv row.
                    "candidate_name": name if office else "",
                    "city":           _clean(latest.get("CITY", "")),
                    "zip":            utils.clean_zip(latest.get("ZIPCODE", "")),
                    "active":         "0" if _clean(latest.get("TERMINATE", "")) == "Y" else "1",
                    "state_filer_id": fid,
                    "raw_file":       raw_prefix + filer_name,
                }

                if ftype == "1" and name:
                    cand_key = (name, office, district, yr_str)
                    if cand_key not in candidates_seen:
                        first, last = _split_name(name)
                        candidates_seen[cand_key] = {
                            "state":           STATE,
                            "candidate_name":  name,
                            "candidate_first": first,
                            "candidate_last":  last,
                            "office":          office,
                            "district":        district,
                            "party":           party,
                            "election_year":   yr_str,
                            "state_filer_id":  fid,
                            "raw_file":        raw_prefix + filer_name,
                        }

            # ── Contributions: contrib.txt (Schedule I A-D, II F-G) ───
            # Up to 3 separate (date, amount) pairs per row — PA's way
            # of recording multiple contributions from the same
            # contributor within one reporting period without a
            # separate row each time. Unused pairs are "0"/blank.
            for txn in _rows(zf, contrib_name):
                cfid = _clean(txn.get("CampaignFinanceID", ""))
                rep = report_by_cfid.get(cfid)
                if rep is None:
                    unmatched += 1
                    continue
                contributor_name = _clean(txn.get("CONTRIBUTOR", ""))
                section = _clean(txn.get("Section", ""))
                base = {
                    "state":             STATE,
                    "committee_name":    rep["filer_name"],
                    "transaction_type":  section,
                    "contributor_name":  contributor_name,
                    "contributor_city":  _clean(txn.get("CITY", "")),
                    "contributor_state": _clean(txn.get("STATE", "")),
                    "contributor_zip":   utils.clean_zip(txn.get("ZIPCODE", "")),
                    "employer":          _clean(txn.get("ENAME", "")),
                    "occupation":        _clean(txn.get("OCCUPATION", "")),
                    "candidate_name":    _candidate_name_for_rep(rep),
                    "office":            rep["office"],
                    "election_year":     rep["election_year"],
                    "amended":           rep["amended"],
                    "filing_id":         cfid,
                    "raw_file":          raw_prefix + contrib_name,
                }
                for n in (1, 2, 3):
                    txn_date = _parse_date(txn.get(f"CONTDATE{n}", ""))
                    amount   = _parse_amount(txn.get(f"CONTAMT{n}", ""))
                    if not txn_date or not amount:
                        continue
                    contrib_rownum += 1
                    contrib_w.writerow({
                        **base,
                        "amount":   amount,
                        "date":     txn_date,
                        "row_num":  contrib_rownum,
                    })

            # ── Other receipts: receipt.txt (Schedule I Part E) ───────
            # Folded into contributions.csv.gz — real dollars into the
            # filer's account, just not solicited from a contributor.
            # RECDESC (refund/interest/etc.) has no home in C.CONTRIBUTIONS
            # and is dropped — see module docstring.
            for txn in _rows(zf, receipt_name):
                cfid = _clean(txn.get("CampaignFinanceID", ""))
                rep = report_by_cfid.get(cfid)
                if rep is None:
                    unmatched += 1
                    continue
                amount   = _parse_amount(txn.get("RECAMT", ""))
                txn_date = _parse_date(txn.get("RECDATE", ""))
                if not amount or not txn_date:
                    continue
                contrib_rownum += 1
                contrib_w.writerow({
                    "state":             STATE,
                    "committee_name":    rep["filer_name"],
                    "amount":            amount,
                    "date":              txn_date,
                    "transaction_type":  "Other Receipt",
                    "contributor_name":  _clean(txn.get("RECNAME", "")),
                    "contributor_city":  _clean(txn.get("CITY", "")),
                    "contributor_state": _clean(txn.get("STATE", "")),
                    "contributor_zip":   utils.clean_zip(txn.get("ZIPCODE", "")),
                    "candidate_name":    _candidate_name_for_rep(rep),
                    "office":            rep["office"],
                    "election_year":     rep["election_year"],
                    "amended":           rep["amended"],
                    "filing_id":         cfid,
                    "raw_file":          raw_prefix + receipt_name,
                    "row_num":           contrib_rownum,
                })

            # ── Expenditures: expense.txt (Schedule III) ──────────────
            for txn in _rows(zf, expense_name):
                cfid = _clean(txn.get("CampaignFinanceID", ""))
                rep = report_by_cfid.get(cfid)
                if rep is None:
                    unmatched += 1
                    continue
                amount   = _parse_amount(txn.get("EXPAMT", ""))
                txn_date = _parse_date(txn.get("EXPDATE", ""))
                if not amount or not txn_date:
                    continue
                expend_rownum += 1
                expend_w.writerow({
                    "state":          STATE,
                    "committee_name": rep["filer_name"],
                    "amount":         amount,
                    "date":           txn_date,
                    "payee_name":     _clean(txn.get("EXPNAME", "")),
                    "purpose":        _clean(txn.get("EXPDESC", "")),
                    "payee_city":     _clean(txn.get("CITY", "")),
                    "payee_state":    _clean(txn.get("STATE", "")),
                    "payee_zip":      utils.clean_zip(txn.get("ZIPCODE", "")),
                    "candidate_name": _candidate_name_for_rep(rep),
                    "office":         rep["office"],
                    "election_year":  rep["election_year"],
                    "amended":        rep["amended"],
                    "filing_id":      cfid,
                    "raw_file":       raw_prefix + expense_name,
                    "row_num":        expend_rownum,
                })

            # ── Debts: debt.txt (Schedule IV) ──────────────────────────
            for txn in _rows(zf, debt_name):
                cfid = _clean(txn.get("CampaignFinanceID", ""))
                rep = report_by_cfid.get(cfid)
                if rep is None:
                    unmatched += 1
                    continue
                amount   = _parse_amount(txn.get("DBTAMT", ""))
                txn_date = _parse_date(txn.get("DBTDATE", ""))
                if not amount or not txn_date:
                    continue
                debt_rownum += 1
                debts_w.writerow({
                    "state":              STATE,
                    "committee_name":     rep["filer_name"],
                    "original_amount":    amount,
                    "date":               txn_date,
                    "record_type":        "Debt",
                    "counterparty_name":  _clean(txn.get("DBTNAME", "")),
                    "counterparty_city":  _clean(txn.get("CITY", "")),
                    "counterparty_state": _clean(txn.get("STATE", "")),
                    "counterparty_zip":   utils.clean_zip(txn.get("ZIPCODE", "")),
                    "candidate_name":     _candidate_name_for_rep(rep),
                    "election_year":      rep["election_year"],
                    "amended":            rep["amended"],
                    "filing_id":          cfid,
                    "raw_file":           raw_prefix + debt_name,
                    "row_num":            debt_rownum,
                })

    contrib_f.close()
    expend_f.close()
    debts_f.close()
    log.info(f"  Contributions (incl. other receipts): {contrib_rownum:,}   "
             f"Expenditures: {expend_rownum:,}   Debts: {debt_rownum:,}   "
             f"Unmatched (no CampaignFinanceID match): {unmatched:,}")

    # ── Hand-verified committee->candidate overrides ────────────────────
    # (see PA_COMMITTEE_CANDIDATE_OVERRIDE docstring above)
    _apply_committee_candidate_overrides(committees_seen, log)

    # ── Write candidates.csv.gz ────────────────────────────────────────
    cand_rows = []
    for ri, (key, meta) in enumerate(candidates_seen.items(), start=1):
        cand_rows.append({
            **meta,
            "person_id":    "",   # filled by assign_person_ids
            "incumbent":    "",   # not available in source
            "jurisdiction": "",   # not available in source
            "row_num":      ri,
        })

    with gzip.open(cand_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.CANDIDATES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(cand_rows)

    n_cands = utils.assign_person_ids(cand_path, id_model="committee")
    log.info(f"  Candidates: {n_cands:,}")

    # ── Write committees.csv.gz ────────────────────────────────────────
    with gzip.open(comm_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.COMMITTEES, extrasaction="ignore", restval="")
        w.writeheader()
        for ri, (key, row) in enumerate(committees_seen.items(), start=1):
            w.writerow({
                **row,
                "person_id": "",   # filled by assign_committee_person_ids
                "row_num":   ri,
            })

    n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
    log.info(f"  Committees: {len(committees_seen):,}  (matched {n_comm_matched:,} to candidates)")

    duration = round(time.perf_counter() - t0, 1)
    log._emit("parse_completed",
              status="completed",
              duration_s=duration,
              contributions=contrib_rownum,
              expenditures=expend_rownum,
              debts=debt_rownum,
              candidates=n_cands,
              unmatched=unmatched)
    log.info(f"Done in {duration}s")


# ============================= CLI ===================================

if __name__ == "__main__":
    try:
        # Optional: `python pennsylvania.py 2024 2025 2026` to limit years;
        # with no args, every data/Pennsylvania/raw/{year}.zip found is processed.
        years = [int(a) for a in sys.argv[1:]] or None
        run(years)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)