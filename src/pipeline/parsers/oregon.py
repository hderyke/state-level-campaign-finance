"""
parsers/oregon.py — Parse Oregon campaign finance data.

Reads or_transactions_*.xls chunks from data/Oregon/raw/ (produced by
scrapers/oregon.py — one .xls export per date window, or per date+amount-band
window for windows that hit the 5,000-row cap) and writes normalized output to
data/Oregon/cleaned/.

Schema notes
────────────
  One transaction feed, two directions, one file
    • Unlike most states, ORESTAR's export has no top-level Contribution vs
      Expenditure column — the search form's "Transaction Type" filter
      (Contribution/Expenditure/Other/...) was left blank on every scrape (to
      get the fewest, widest requests against the 5,000-row cap), so the only
      per-row signal is the free-text `Sub Type` column ("Cash Contribution",
      "Cash Expenditure", "In-Kind Contribution", etc). Rows are routed on
      known Sub Type markers; anything unrecognized is counted and reported,
      never guessed silently — see route().
    • Two Sub Types are genuinely ambiguous from a single-committee sample
      (verified against 2022 Kotek-for-Governor data only) and are
      deliberately left unrouted rather than filed on a guess:
      "Refunds and Rebates" (vendor rebates back to the committee — could
      argue either receipt or negative-expenditure) is the main one seen so
      far. If the full corpus surfaces others, they'll show up in the
      unrouted-types warning at the end of every parse run.

  Filer is the committee
    • `Filer` / `Filer Id` is the filing committee on every row → maps to
      committee_name / state_filer_id. `Contributor/Payee` is the other party
      — contributor on the contribution side, payee on the expenditure side.
      When that party is itself a registered committee, `Contributor/Payee
      Committee ID` is populated and the display name carries a redundant
      " (1234)" suffix, which is stripped since the ID is already captured
      separately.
    • committee_name written to contributions/expenditures/loans_debts is
      the entities registry's name for that Filer Id, NOT the transaction
      feed's own `Filer` text, falling back to `Filer` only when the Filer
      Id isn't in the registry (an orphan). This matters because
      committee_name — not state_filer_id — is the join key queries.py and
      the aggregate DB use (see columns.py), so it has to agree with what's
      written to committees.csv or a filer becomes two disconnected,
      unmatchable entities. Confirmed necessary on real data: Filer Id 3591
      files every 2022 transaction as "Run Betsy Run" but is registered in
      every or_entities_*.xls snapshot as "Betsy PAC" — a genuine ORESTAR
      naming inconsistency between its two export feeds, not a scraping
      artifact (checked ~4,500 of that filer's raw transaction rows, 100%
      say "Run Betsy Run"). Using the transaction name would have made this
      committee — one of the largest spenders in the 2022 cycle, $14.5M —
      invisible to any query that joins through committees.csv. Every such
      mismatch is counted and logged, not silently resolved.
    • candidate_name / office / election_year on contributions/expenditures/
      loans_debts also come from the registry (looked up by Filer Id), not
      from the transaction feed — ORESTAR's transaction export carries none
      of these directly, so without the join every "top recipient candidate"
      style query silently returns zero rows (candidates.candidate_name
      never matches an empty string). Blank for orphans and for filers the
      registry marks as non-candidate (PACs, parties, etc).

  Entities (registry) — committees.csv and candidates.csv
    • Primary source is or_entities_*.xls (one file per non-empty bucket from
      the scraper's 13-bucket filerType/special sweep — in practice only 5 of
      the 13 ever have rows: CANDALL, CPCALL, PACALL, SLATE_MAILER,
      IE_FILERS; the other 8 filerType values returned 0 matches against real
      2022 data). Buckets overlap (a slate-mailer or IE filer also shows up
      under its base filerType), so load_entity_registry() dedupes by
      Committee Id, first-seen wins.
    • Transaction-derived committees data (the old sole source) is now a
      fallback "orphans" pass for any Filer Id that shows up in money but not
      in the registry — same pattern Wisconsin's parser uses, for the same
      reason (a filer can go quiet/terminate before a given entities sweep,
      or the sweep's year-fallback window might miss an edge case).
    • Column mapping: Candidate Office Group ("Governor", "State
      Representative", "Mayor", "District Attorney", ...) → office.
      Candidate Office ("statewide", "22nd District", "Gilliam County", "City
      of Gresham", ...) → jurisdiction, verbatim. district is a light regex
      pull of a leading ordinal off Candidate Office ("22nd District" → "22";
      "statewide" / "Gilliam County" → "" — no number to find, left blank
      rather than guessed).
    • Treasurer Mailing Address / Candidate Maling Address [sic] are single
      *space-joined* strings with no delimiter at all ("1915 Pinto Ct West
      Linn OR 97068") — not even a comma before the city. There's no reliable
      way to tell a multi-word city ("West Linn", "Lake Oswego") apart from
      the tail of a multi-word street name without a place-name lookup, so
      city is deliberately left blank; only the trailing ZIP/ZIP+4 token is
      pulled out (a wrong city is worse than a blank one — same call
      Wisconsin's own address parsing makes).
    • No status/active-flag column exists anywhere in the export (confirmed
      against the real header row — 22 columns, none of them a status field,
      despite the search form itself having Approved/Pending/Rejected/
      Discontinued checkboxes) — committees.csv's `active` is left blank for
      every OR row. party isn't in the export either — candidates.csv's
      `party` is also always blank.
    • election_year comes from Active Election's leading 4-digit year
      ("2006 General Election" → "2006"); PACs/CPCs mostly leave Active
      Election blank, so it falls back to Filing Date's year.
    • id_model is "committee", confirmed (not guessed) against real data: the
      same candidate running in multiple cycles gets a *new* Committee Id
      each time (e.g. Shane Bemis for Mayor — ID 8465 in the 2006 cycle, ID
      14710 in 2010; Michael Marsh — ID 5500 as State Rep in 2006, ID 14764
      running for State Treasurer in 2010), so state_filer_id is not a stable
      per-person key and must be grouped by (name, office, district) the same
      way AL/AZ/CA are. Checked by loading every real or_entities_*.xls file
      and looking for candidate names that recur under more than one
      Committee Id — 29 out of ~700 candidates did, all cycle-scoped exactly
      like this.

  Column mapping highlights (45 raw columns; the rest are administrative —
  attest/review workflow dates, loan servicing fields with no schema home —
  and dropped)
    • Sub Type              → transaction_type (raw, routes the row)
    • Book Type             → contributor_type (raw; "Individual", "Business
                              Entity", "Political Committee", "Other", ...)
    • Occptn Txt / Emp Name → occupation / employer
    • Purp Desc + Purpose Codes → purpose (expenditures), joined
    • Payer of Personal Expenditure → folded into purpose as "reimbursed: X"
      for the "Personal Expenditure for Reimbursement" Sub Type, where
      Contributor/Payee is the vendor actually paid and this column names the
      staffer/candidate being reimbursed — losing it would silently drop who
      the money really went to.
    • Tran Status           → amended ("1" if "Amended", else "0")
    • Original Id           → filing_id. Not a real report/filing ID (ORESTAR's
      export doesn't expose one) — it's the transaction this one amends, which
      at least threads amendment chains together. Deliberately not Tran Id
      itself, which is already captured via raw_file + row_num.

Data notes
──────────
  - Amount arrives from xlrd as a native float (not a string like most
    states' CSV exports) — parse_amount accepts both.
  - City/State on the contributor/payee side are already 2-letter codes
    (confirmed 'OR', 'WA', 'CA', 'TX', ... in real data) — no name→abbr
    mapping needed, unlike Wisconsin/Florida.
  - Zip and "Zip Plus Four" are separate columns; combined into one
    XXXXX-XXXX value via utils.clean_zip when both are present.
"""

import csv
import gzip
import re
import sys
import time
from datetime import date
from pathlib import Path

import xlrd
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Oregon" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Oregon" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "OR"
MAX_VALID_YEAR = date.today().year + 2

# ============================= constants ==============================

# Sub Type routing, matched on lowercased substrings so the source can add
# qualifiers without breaking routing (e.g. any future "XYZ Expenditure").
# "account payable"/"account receivable" cover ORESTAR's "Other Account
# Receivable" / "Other Disbursement" top-level types, whose Sub Type text is
# the only place that distinction survives in the export — confirmed against
# a full year of real 2022 data ("Account Payable", "In-Kind/Forgiven Account
# Payable", "Account Payable Rescinded" all route as debts; "Miscellaneous
# Account Receivable" as the mirror case, money owed *to* the committee).
CONTRIBUTION_MARKERS = ("contribution", "receipt")
EXPENDITURE_MARKERS  = ("expenditure", "disbursement")
LOAN_MARKERS         = ("loan", "debt", "promissory", "account payable", "account receivable")

# Exact Sub Type values that don't contain a routing marker but are
# unambiguous once you look at real rows (see module docstring) — checked
# before the marker match, case-insensitively.
EXACT_ROUTES = {
    "return or refund of contribution": "contributions",  # refund paid *to* a donor —
                                                            # ORESTAR's own name says "contribution"
    "lost or returned check":           "contributions",  # bounced/never-deposited
                                                            # contribution, corrected down
    "items sold at fair market value":  "contributions",  # fundraiser-merchandise proceeds — a receipt
    "interest/investment income":       "contributions",  # bank interest / investment gains — a receipt
}

# Sub Types confirmed present in real 2022 data but deliberately left
# unrouted rather than guessed:
#   "Refunds and Rebates" (388 rows)         — direction is genuinely unclear;
#     samples show vendors like an ad agency and an events venue as the other
#     party, which could be a receipt (money back) or a negative expenditure
#     (spending corrected down) with no way to tell from the export alone.
#   "Cash Balance Adjustment" (144 rows)     — a bookkeeping correction with
#     no real counterparty, not a transaction with a direction at all.
#   "Nonpartisan Activity" (1 row)           — too rare and undocumented to
#     classify confidently; falls into the unrouted count if it recurs.
# All three surface every run via the "unrouted transaction sub-types"
# warning — see route().

# "1234)" tail on Contributor/Payee when the other party is itself a
# registered committee, e.g. "Oregon Action Committee for Rural
# Electrification (117)" — Contributor/Payee Committee ID already has the 117.
_COMMITTEE_SUFFIX_RE = re.compile(r"\s*\(\d+\)\s*$")

_CHUNK_RE = re.compile(
    r"^or_transactions_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})(?:_amt.*)?\.xls$"
)

# Entities (registry) helpers — see module docstring.
# "22nd District" → 22; "3rd District" → 3; "statewide" / "Gilliam County" /
# "City of Gresham" have no ordinal-District pattern and correctly don't match.
_DISTRICT_RE = re.compile(r"(\d+)(?:st|nd|rd|th)\s+District", re.IGNORECASE)
# Leading 4-digit year out of "2006 General Election" / "2010 Primary Election".
_ELEC_YEAR_RE = re.compile(r"(\d{4})")
# Trailing ZIP or ZIP+4 off a single space-joined address blob with no
# delimiters — e.g. "1915 Pinto Ct West Linn OR 97068" → ("97068", None).
_ADDR_ZIP_RE = re.compile(r"(\d{5})(?:-(\d{4}))?\s*$")


# ============================== helpers ==============================

def clean(val) -> str:
    """Strip whitespace and coerce None/empty-cell to empty string."""
    return (str(val) if val is not None else "").strip()


def parse_amount(val) -> str:
    """
    Numeric cell (xlrd gives Amount as a native float) or string → plain
    numeric string; '' on failure. Handles both since every other state's
    parse_amount only ever sees strings from a CSV reader.
    """
    if isinstance(val, (int, float)):
        return str(val)
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
    """MM/DD/YYYY → YYYY-MM-DD; '' on failure or implausible year."""
    from datetime import datetime
    v = (val or "").strip()
    if not v:
        return ""
    try:
        d = datetime.strptime(v, "%m/%d/%Y")
    except ValueError:
        return ""
    if d.year < 1990 or d.year > MAX_VALID_YEAR:
        return ""
    return d.strftime("%Y-%m-%d")


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore",
                        restval="")
    w.writeheader()
    return fh, w


def raw_files(pattern: str) -> list[Path]:
    """Non-empty raw files matching a glob, in filename order."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def warn_overlapping_chunks(log, files: list[Path]):
    """
    Warn if two transaction chunks cover the same dates. The scraper emits
    disjoint windows, so an overlap means a stale file from an interrupted or
    re-split run is still on disk and its rows will be counted twice.
    Amount-banded chunks share a date range by design and are excluded.
    """
    spans: list[tuple[str, str, str]] = []
    for f in files:
        m = _CHUNK_RE.match(f.name)
        if m and "_amt" not in f.name:
            spans.append((m.group(1), m.group(2), f.name))
    spans.sort()
    for (a_from, a_to, a_name), (b_from, b_to, b_name) in zip(spans, spans[1:]):
        if b_from <= a_to:
            log.warning(
                f"overlapping raw chunks — {a_name} ({a_from}→{a_to}) and "
                f"{b_name} ({b_from}→{b_to}) both cover {b_from}; rows in "
                f"the overlap will be double counted. Re-run the scraper with "
                f"--force for the affected years."
            )


def route(sub_type: str) -> str:
    """
    Decide which relation a transaction row belongs to, from Sub Type alone
    (ORESTAR's export carries no separate top-level Contribution/Expenditure
    column — see module docstring). Returns "contributions", "expenditures",
    "loans", or "" (unroutable — counted and reported, never guessed).
    """
    st = sub_type.strip().lower()
    if not st:
        return ""
    exact = EXACT_ROUTES.get(st)
    if exact:
        return exact
    if any(m in st for m in LOAN_MARKERS):
        return "loans"
    if any(m in st for m in CONTRIBUTION_MARKERS):
        return "contributions"
    if any(m in st for m in EXPENDITURE_MARKERS):
        return "expenditures"
    return ""


def strip_committee_suffix(name: str, committee_id: str) -> str:
    """Drop a trailing '(1234)' from a contributor/payee name when that
    number is already captured separately as committee_id."""
    if not committee_id:
        return name
    return _COMMITTEE_SUFFIX_RE.sub("", name)


def combined_zip(zip_val: str, plus4_val: str) -> str:
    z, p4 = clean(zip_val), clean(plus4_val)
    if z and p4 and z.isdigit() and p4.isdigit():
        return utils.clean_zip(z + p4)
    return utils.clean_zip(z)


def expenditure_purpose(row: dict) -> str:
    """Purp Desc + Purpose Codes, with the reimbursed staffer/candidate name
    folded in for "Personal Expenditure for Reimbursement" rows — see module
    docstring for why Payer of Personal Expenditure needs to survive here."""
    parts = [clean(row.get("Purp Desc", ""))]
    codes = clean(row.get("Purpose Codes", ""))
    if codes and codes not in parts:
        parts.append(f"[{codes}]")
    payer = clean(row.get("Payer of Personal Expenditure", ""))
    if payer:
        parts.append(f"(reimbursed: {payer})")
    return " ".join(p for p in parts if p)


def entity_district(jurisdiction: str) -> str:
    """'22nd District' → '22'; 'statewide' / 'Gilliam County' / 'City of
    Gresham' (no numbered seat) → ''. See module docstring."""
    m = _DISTRICT_RE.search(jurisdiction or "")
    return m.group(1) if m else ""


def entity_election_year(active_election: str, filing_date: str) -> str:
    """Active Election's leading year, falling back to Filing Date's year
    when Active Election is blank (mostly PACs/CPCs, which don't run in an
    election themselves)."""
    m = _ELEC_YEAR_RE.search(active_election or "")
    if m:
        return m.group(1)
    d = parse_date(filing_date)
    return d[:4] if d else ""


def entity_zip(address: str) -> str:
    """Trailing ZIP/ZIP+4 off a single space-joined address blob with no
    delimiters at all. City is deliberately not attempted — see module
    docstring for why a wrong city is worse than a blank one here."""
    m = _ADDR_ZIP_RE.search((address or "").strip())
    if not m:
        return ""
    z, p4 = m.group(1), m.group(2)
    return utils.clean_zip(z + p4) if p4 else utils.clean_zip(z)


# ============================ xls reading =============================

def read_xls_rows(path: Path):
    """
    Yield (row_num, dict) for every data row in an ORESTAR .xls export.
    row_num is 1-based counting the header as row 1, matching the
    enumerate(reader, start=2) convention every CSV-based parser uses.
    """
    wb = xlrd.open_workbook(str(path))
    sh = wb.sheet_by_index(0)
    headers = [clean(h) for h in sh.row_values(0)]
    for i in range(1, sh.nrows):
        values = sh.row_values(i)
        yield i + 1, dict(zip(headers, values))


# ============================ raw registries ===========================

def load_entity_registry(log) -> dict[str, dict]:
    """
    Committee Id → record, from every or_entities_*.xls file.

    The scraper now sweeps every filerType bucket once blank AND once per
    year (see scrapers/oregon.py's docstring — the blank sweep alone was
    proven incomplete), so the same Committee Id shows up in many files: its
    blank-sweep snapshot plus one snapshot per year it was active. These
    aren't identical rows — Active Election / Filing Date drift between
    them — so dedup keeps whichever snapshot has the *latest* election_year
    rather than first-seen, otherwise a still-active committee's record gets
    pinned to an old cycle (confirmed on real data: Friends of Christine
    Drazan's blank-sweep row carries Active Election "2018 ...", processed
    before the _2022 file that has the real one, and first-seen-wins would
    have silently kept the stale year). A Committee Name mismatch on a dupe
    ID (~25 seen in real data — legitimate mid-history renames like "Crown
    Political Action Committee" vs "Crown PAC") is logged, not an error.
    """
    registry: dict[str, dict] = {}
    files = raw_files("or_entities_*.xls")
    if not files:
        log.warning(
            "no or_entities_*.xls found in raw/ — committees/candidates will "
            "be built from transaction rows only (no type, treasurer, "
            "address, office, or candidate split; run the scraper with "
            "--entities first)"
        )
        return registry

    name_conflicts = 0
    year_upgrades = 0
    for path in files:
        ft = time.perf_counter()
        try:
            rows = list(read_xls_rows(path))
        except Exception as e:
            log.file_parse_error(path.name, error=str(e))
            continue

        n = 0
        for row_num, row in rows:
            cid = clean(row.get("Committee Id", ""))
            if not cid:
                continue
            cname = clean(row.get("Committee Name", ""))

            cand_first = clean(row.get("Candidate First Name", ""))
            cand_last  = clean(row.get("Candidate Last Name", ""))
            treas_first = clean(row.get("Treasurer First Name", ""))
            treas_last  = clean(row.get("Treasurer Last Name", ""))
            jurisdiction = clean(row.get("Candidate Office", ""))
            elec_year = entity_election_year(
                row.get("Active Election", ""), row.get("Filing Date", ""),
            )

            existing = registry.get(cid)
            if existing is not None:
                if existing["committee_name"] != cname:
                    name_conflicts += 1
                # Keep whichever snapshot has the later election_year;
                # blank stays put unless the challenger has a real year.
                if elec_year and elec_year <= existing["election_year"]:
                    continue
                if not elec_year:
                    continue
                year_upgrades += 1

            registry[cid] = {
                "committee_name":  cname,
                "committee_type":  clean(row.get("Committee Type", "")),
                "election_year":   elec_year,
                "candidate_first": cand_first,
                "candidate_last":  cand_last,
                "candidate_name":  f"{cand_first} {cand_last}".strip(),
                "office":          clean(row.get("Candidate Office Group", "")),
                "district":        entity_district(jurisdiction),
                "jurisdiction":    jurisdiction,
                "treasurer_name":  f"{treas_first} {treas_last}".strip(),
                "zip":             entity_zip(clean(row.get("Treasurer Mailing Address", ""))),
                "raw_file":        path.name,
                "row_num":         row_num,
            }
            n += 1

        log.file_parsed(path.name, "entities", n,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=path.stat().st_size)

    if name_conflicts:
        log.warning(
            f"entities registry: {name_conflicts} Committee Id(s) had a "
            f"different Committee Name across bucket files — kept whichever "
            f"snapshot had the latest election_year"
        )
    if year_upgrades:
        log.info(f"  entities registry: {year_upgrades:,} committees upgraded "
                 f"to a later election_year from a per-year bucket file")
    log.registry_loaded("or_entities_*.xls", entries=len(registry), relation="entities")
    return registry


# ================================ run =================================

def run():
    log = get_logger("oregon", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, cand_fh, cmte_fh, loan_fh]

        registry = load_entity_registry(log)

        # Filer Ids seen in transactions but absent from the entities
        # registry — a fallback "orphans" pass, same pattern Wisconsin's
        # parser uses (see module docstring).
        orphans: dict[str, dict] = {}
        # Filer Ids present in the registry whose transaction-feed name
        # differs from their entities-registry name (e.g. Filer Id 3591:
        # "Run Betsy Run" on every transaction row, "Betsy PAC" in every
        # or_entities_*.xls snapshot — a real ORESTAR data quirk, not a
        # parser bug, confirmed by checking dozens of that filer's raw rows).
        # Tracked per filer_id, not per row.
        name_mismatches: dict[str, tuple[str, str]] = {}
        unrouted: dict[str, int] = {}
        skipped_no_amount = 0

        chunks = raw_files("or_transactions_*.xls")
        if not chunks:
            log.warning("no or_transactions_*.xls found in raw/ — run the scraper first")
        warn_overlapping_chunks(log, chunks)

        for path in chunks:
            ft = time.perf_counter()
            n_cont = n_expn = n_loan = 0

            try:
                rows = list(read_xls_rows(path))
            except Exception as e:
                log.file_parse_error(path.name, error=str(e))
                continue

            for row_num, row in tqdm(rows, desc=f"  {path.name}", unit="row",
                                     dynamic_ncols=True, leave=False):
                amount = parse_amount(row.get("Amount", ""))
                if not amount:
                    skipped_no_amount += 1
                    continue

                filer_id   = clean(row.get("Filer Id", ""))
                filer_name = clean(row.get("Filer", ""))
                ent = registry.get(filer_id, {})

                # committee_name is the join key every other relation and
                # queries.py use (not state_filer_id — see columns.py), so
                # this must match whatever's written to committees.csv:
                # prefer the registry's name, falling back to the
                # transaction feed's own Filer name only for orphans.
                cmte_name = ent.get("committee_name") or filer_name
                cand_name = ent.get("candidate_name", "")
                office    = ent.get("office", "")
                elec_year = ent.get("election_year", "")

                if filer_id and filer_id not in registry and filer_id not in orphans:
                    orphans[filer_id] = {
                        "committee_name": filer_name,
                        "raw_file":       path.name,
                        "row_num":        row_num,
                    }
                elif filer_id and ent.get("committee_name") and filer_id not in name_mismatches:
                    if utils.clean_name(ent["committee_name"]) != utils.clean_name(filer_name):
                        name_mismatches[filer_id] = (filer_name, ent["committee_name"])

                sub_type  = clean(row.get("Sub Type", ""))
                tx_date   = parse_date(row.get("Tran Date", ""))
                amended   = "1" if clean(row.get("Tran Status", "")).lower() == "amended" else "0"
                filing_id = clean(row.get("Original Id", ""))
                cp_id     = clean(row.get("Contributor/Payee Committee ID", ""))
                cp_name   = strip_committee_suffix(
                    clean(row.get("Contributor/Payee", "")), cp_id,
                )
                zipcode   = combined_zip(row.get("Zip", ""), row.get("Zip Plus Four", ""))

                dest = route(sub_type)

                if dest == "contributions":
                    cont_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "amount":           amount,
                        "date":             tx_date,
                        "transaction_type": sub_type,
                        "contributor_name": cp_name,
                        "contributor_type": clean(row.get("Book Type", "")),
                        "contributor_city": clean(row.get("City", "")),
                        "contributor_state": clean(row.get("State", "")),
                        "contributor_zip":  zipcode,
                        "employer":         clean(row.get("Emp Name", "")),
                        "occupation":       clean(row.get("Occptn Txt", "")),
                        "candidate_name":   cand_name,
                        "office":           office,
                        "election_year":    elec_year,
                        "amended":          amended,
                        "filing_id":        filing_id,
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    n_cont += 1

                elif dest == "expenditures":
                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cmte_name,
                        "amount":           amount,
                        "date":             tx_date,
                        "transaction_type": sub_type,
                        "payee_name":       cp_name,
                        "purpose":          expenditure_purpose(row),
                        "category":         sub_type,
                        "payee_city":       clean(row.get("City", "")),
                        "payee_state":      clean(row.get("State", "")),
                        "payee_zip":        zipcode,
                        "candidate_name":   cand_name,
                        "office":           office,
                        "election_year":    elec_year,
                        "amended":          amended,
                        "filing_id":        filing_id,
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    n_expn += 1

                elif dest == "loans":
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cmte_name,
                        "original_amount":    amount,
                        "date":               tx_date,
                        "record_type":        sub_type,
                        "counterparty_name":  cp_name,
                        "counterparty_city":  clean(row.get("City", "")),
                        "counterparty_state": clean(row.get("State", "")),
                        "counterparty_zip":   zipcode,
                        "candidate_name":     cand_name,
                        "election_year":      elec_year,
                        "amended":            amended,
                        "filing_id":          filing_id,
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    n_loan += 1

                else:
                    key = sub_type or "(blank)"
                    unrouted[key] = unrouted.get(key, 0) + 1

            log.file_parsed(path.name, "transactions", n_cont + n_expn + n_loan,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += n_cont
            total_expenditures  += n_expn
            total_loans         += n_loan

        # ── Committees and candidates (registry-primary, orphans fallback) ─
        for filer_id, ent in registry.items():
            cmte_w.writerow({
                "state":          STATE,
                "committee_name": ent["committee_name"],
                "committee_type": ent["committee_type"],
                "election_year":  ent["election_year"],
                "candidate_name": ent["candidate_name"],
                "treasurer_name": ent["treasurer_name"],
                "city":           "",
                "zip":            ent["zip"],
                "active":         "",
                "state_filer_id": filer_id,
                "raw_file":       ent["raw_file"],
                "row_num":        ent["row_num"],
            })
            committees_written += 1

            if not ent["candidate_name"]:
                continue
            cand_w.writerow({
                "state":           STATE,
                "candidate_name":  ent["candidate_name"],
                "candidate_first": ent["candidate_first"],
                "candidate_last":  ent["candidate_last"],
                "office":          ent["office"],
                "district":        ent["district"],
                "jurisdiction":    ent["jurisdiction"],
                "party":           "",
                "election_year":   ent["election_year"],
                "incumbent":       "",
                "state_filer_id":  filer_id,
                "raw_file":        ent["raw_file"],
                "row_num":         ent["row_num"],
            })
            candidates_written += 1

        # Filers seen only in transactions — not in any entities bucket.
        for filer_id, orph in orphans.items():
            cmte_w.writerow({
                "state":          STATE,
                "committee_name": orph["committee_name"],
                "committee_type": "",
                "election_year":  "",
                "candidate_name": "",
                "treasurer_name": "",
                "city":           "",
                "zip":            "",
                "active":         "",
                "state_filer_id": filer_id,
                "raw_file":       orph["raw_file"],
                "row_num":        orph["row_num"],
            })
            committees_written += 1

        if orphans:
            log.enrichment_summary(
                relation="committees",
                matched=len(registry),
                unmatched=len(orphans),
                note="filers present in transactions but not in any or_entities_*.xls bucket",
            )
        if name_mismatches:
            examples = list(name_mismatches.values())[:5]
            log.warning(
                f"{len(name_mismatches)} filer(s) use a different name in "
                f"transactions than in the entities registry (real ORESTAR "
                f"data, not deduped away) — committees.csv/contributions/"
                f"expenditures all use the registry name. Examples: "
                + "; ".join(f'"{tx}" -> "{reg}"' for tx, reg in examples)
            )
        if unrouted:
            top = sorted(unrouted.items(), key=lambda kv: -kv[1])[:10]
            log.warning(
                "unrouted transaction sub-types (dropped): "
                + ", ".join(f"{k}={v:,}" for k, v in top)
            )
        if skipped_no_amount:
            log.info(f"  rows skipped for unparseable amount: {skipped_no_amount:,}")

        # ── Close handles before person-ID assignment ─────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # Committee Ids are reissued per election cycle, not stable per
        # candidate — confirmed against real data, see module docstring.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
