"""
parsers/new_hampshire.py -- Parse New Hampshire CFS export CSVs (from
scrapers/new_hampshire.py) into the canonical cleaned schema.

Input files (data/New Hampshire/raw/), all produced by scrapers/new_hampshire.py:
    receipts_{year}.csv       one bulk file per filing year -- transactionTypeCode=TCON
    expenditures_{year}.csv   one bulk file per filing year -- transactionTypeCode=TEXP

Output (data/New Hampshire/cleaned/):
    candidates.csv.gz, committees.csv.gz,
    contributions.csv.gz, expenditures.csv.gz,
    loans_debts.csv.gz (empty -- NH's receipts export folds loan
    activity into the same file as monetary contributions; see
    "Loans" below. There is no separate loan/debt schedule to parse.)

id_model = "committee"
    Filing Entity ID is NH's per-committee registration ID. A candidate who
    runs across multiple cycles appears to register a distinct committee
    (and therefore a distinct Filing Entity ID) each time -- e.g. sample
    data shows the same candidate's committee name changing between
    cycles -- so "committee" grouping (person_id = min Filing Entity ID per
    (state, candidate_name, office, district), see utils.assign_person_ids)
    is used to merge those registrations under one person. Since NH's
    export exposes no office/district data at all (see "No entity roster"
    below), the grouping in practice reduces to candidate_name alone.

VERIFIED vs ASSUMED:
    Every column mapping below was checked against real sample CSVs (one
    receipts file, one expenditures file, both filingYear=2024) and the
    site's own "Download Data File Key" PDFs, both supplied directly by
    the user -- not assumed or reverse-engineered from a legacy system.
    The distinct value sets for every categorical column (Transaction
    Type, Transaction Sub Type, Committee Subtype, Contributor Type,
    Filing Entity Type, Election Period/Type) were enumerated from the
    real sample files (268k receipt rows / 29k expenditure rows) -- see
    docs/states/new_hampshire.md for the full tally. Untested: whether
    other filing years use the exact same header text/casing (the parser
    resolves headers by normalized name via `_resolve_headers`, so a
    genuine rename fails loudly -- file_parse_error + file skipped --
    rather than silently mismapping).

No entity roster:
    Unlike some states, NH's CFS export API has no separate
    candidate/committee roster endpoint -- only two data types exist on
    the Download Data page (Receipts, Expenditures), confirmed against
    the live page's own DOM. Candidates and committees are therefore
    entirely backfilled from the two transaction files, keyed by
    "Filing Entity ID" (-> state_filer_id). This means office, district,
    party, jurisdiction, incumbent, treasurer_name, city, zip, and active
    status are ALL unavailable for NH and are written blank in
    candidates.csv.gz/committees.csv.gz -- not a parsing gap, a genuine
    absence in the source data as exported.

Column mapping -- receipts_{year}.csv (transactionTypeCode=TCON):
    Filing Entity ID                -> state_filer_id (committees/candidates), joins the two files
    Candidate Name                  -> candidate_name (raw "Last, First Middle" -- a trailing
                                        parenthetical alias/ballot-name, e.g.
                                        "Long, Patrick T. (Long, Pat )", is stripped before storing
                                        and before splitting into candidate_first/candidate_last)
    Committee Name                  -> committee_name (falls back to Candidate Name when blank --
                                        confirmed in real data that a candidate committee row can
                                        have an empty Committee Name while Candidate Name is set)
    Committee Subtype               -> committee_type (falls back to "Candidate Committee" when
                                        blank and a candidate name is present -- PACs/party
                                        committees carry a real subtype like "Major Purpose
                                        Electioneering"; candidate committees carry none)
    Transaction Type + Transaction Sub Type -> transaction_type. NH's Transaction Type is mostly
                                        the uninformative "Receipt" (96%+ of rows) with the real
                                        detail in Transaction Sub Type (Itemized Monetary,
                                        Unitemized Monetary, Monetary Contribution, In-Kind
                                        (Non-Money), Interest); the few genuinely distinct Type
                                        values (Return Receipt, Loan Received, Loan Payment, Loan
                                        Forgiven) carry no/blank Sub Type. So: use Sub Type when
                                        Type=="Receipt" and Sub Type is non-blank, else fall back
                                        to Type itself. See src/aliases/transaction_categories.csv
                                        for the resulting canonical mapping.
    Election Period                  -> not mapped (no canonical column: General/Primary/
                                        Exploratory/Special Election -- distinct from election_year)
    Election year                    -> election_year (falls back to the year embedded in the raw
                                        filename if this column is blank on a given row)
    Date of Receipt                  -> date (month/day/year, e.g. "%m/%d/%Y")
    Amount of receipt                -> amount ("$1,234.56" -- strip $ and , ; parenthesized
                                        negative handled the same as other states even though not
                                        observed on the receipts side in sampled data)
    Contributor Type                 -> contributor_type (raw passthrough; see
                                        src/aliases/contributor_types.csv)
    Contributor Name                 -> contributor_name
    Contributor Address Line 1/2     -> not mapped (no canonical street-address column; city/
                                        state/zip below are separate columns and ARE mapped)
    Contributor City/State/Zip Code  -> contributor_city/contributor_state/contributor_zip
    Contributor occupation           -> occupation
    Contributor Employer             -> employer
    Contributor Principle place of Business -> not mapped (no canonical column distinct from
                                        employer; dropping rather than guessing a merge)
    Description                      -> not mapped (CONTRIBUTIONS has no purpose/description
                                        column, unlike EXPENDITURES' `purpose`)
    Timed Report                     -> not mapped (free-text report-type flag -- "48-Hour
                                        Report", "48-Hour Receipt Report" -- not a boolean
                                        amendment flag; `amended` is left blank rather than
                                        guessing 0/1 from this)

Column mapping -- expenditures_{year}.csv (transactionTypeCode=TEXP):
    Filing Entity ID                 -> state_filer_id, joins the two files
    Filing Entity Name               -> committee_name always; ALSO -> candidate_name when
                                        Filing Entity Type is "Candidate Committee" or
                                        "Candidate" (confirmed in real data: for those rows this
                                        field holds a personal "Last, First" name, e.g.
                                        "Friedrich, Ed ", not a campaign brand name -- unlike the
                                        receipts file, which carries candidate and committee name
                                        as two separate columns)
    Filing Entity Type               -> committee_type (raw passthrough; see
                                        src/aliases/committee_types.csv). Observed values:
                                        Political Committee, Candidate Committee, Candidate,
                                        Political Advocacy Organization, Governor's Inaugural
                                        Committee, Speaker of the House.
    Transaction Type + Transaction Sub Type -> transaction_type, same fallback rule as receipts
                                        but keyed on Type=="Expenditure" (the other real Type
                                        values -- Independent Expenditure, Return Expenditure --
                                        carry blank Sub Type in every sampled row).
    Payee/.../type                   -> not mapped (EXPENDITURES has no payee_type column;
                                        used only to help resolve payee_name -- see below)
    Payee/.../Name                   -> payee_name
    Payee/.../Address                -> payee_city/payee_state/payee_zip via best-effort regex
                                        split of "street, city, ST ZIP" (no separate city/state/
                                        zip columns exist for expenditures the way they do for
                                        receipts' contributor address -- NH gives one combined
                                        free-text address field here). Street portion has no
                                        canonical column and is dropped. Rows that don't match
                                        the expected trailing "City, ST ZIP" shape (or are blank,
                                        as most unitemized rows are) get all three left blank
                                        rather than a bad guess.
    Transaction Amount                -> amount
    TransactionDate                   -> date
    Election Type                    -> not mapped (General/Primary/Exploratory -- same
                                        limitation as receipts' Election Period)
    Transaction Description           -> purpose
    Timed Report                     -> not mapped, same reasoning as receipts

    election_year: expenditures_{year}.csv carries NO per-row numeric year column at all (unlike
    receipts' "Election year") -- election_year is taken entirely from the year embedded in the
    raw filename (i.e., the filingYear the scraper requested).

Loans:
    The receipts file's data key states Receipts "include ... Loan Forgiveness", and real sampled
    data additionally shows Transaction Type values "Loan Received" and "Loan Payment" mixed into
    the same file (103 + 15 rows out of 268k in the 2024 sample) -- there is no separate loan
    schedule/file to parse. These rows are written into contributions.csv.gz like every other
    receipt row (transaction_type carries "Loan Received"/"Loan Payment"/"Loan Forgiven" verbatim
    since their Transaction Sub Type is blank -- see mapping above), NOT into loans_debts.csv.gz,
    which is written empty (header only) for schema completeness, matching the ohio.py parser's
    convention for a state with no distinct loan/debt export.
"""

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
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "New Hampshire" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "New Hampshire" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE = "NH"
MIN_VALID_YEAR = 2000
MAX_VALID_YEAR = date.today().year + 2

# Filing Entity Type / committee subtype values observed to indicate the
# filer IS the candidate personally (as opposed to a PAC/party committee).
_CANDIDATE_ENTITY_TYPES = {"candidate committee", "candidate"}


# ========================= header resolution ===========================
#
# Real header text has inconsistent internal spacing around slashes
# ("Payee/ Worker /Creditor/ Loan source type") -- normalize before
# matching so a future year's minor whitespace/casing drift doesn't
# silently break column resolution. Name-based (not positional), same
# defensive pattern as parsers/ohio.py: a genuine rename fails loudly
# (file_parse_error + file skipped) instead of silently mismapping.

def _norm_header(h: str) -> str:
    h = (h or "").strip()
    h = re.sub(r"\s*/\s*", "/", h)
    h = re.sub(r"\s+", " ", h)
    return h.lower()


RECEIPT_ALIASES = {
    "filing_entity_id":      ["filing entity id"],
    "candidate_name":        ["candidate name"],
    "committee_name":        ["committee name"],
    "committee_subtype":     ["committee subtype"],
    "transaction_type":      ["transaction type"],
    "transaction_subtype":   ["transaction sub type"],
    "election_year":         ["election year"],
    "date":                  ["date of receipt"],
    "amount":                ["amount of receipt"],
    "contributor_type":      ["contributor type"],
    "contributor_name":      ["contributor name"],
    "contributor_city":      ["contributor city"],
    "contributor_state":     ["contributor state"],
    "contributor_zip":       ["contributor zip code"],
    "contributor_occupation": ["contributor occupation"],
    "contributor_employer":  ["contributor employer"],
}

EXPEND_ALIASES = {
    "filing_entity_id":   ["filing entity id"],
    "filing_entity_name": ["filing entity name"],
    "filing_entity_type": ["filing entity type"],
    "transaction_type":   ["transaction type"],
    "transaction_subtype": ["transaction sub type"],
    "payee_name":         ["payee/worker/creditor/loan source name"],
    "payee_address":      ["payee/worker/creditor/loan source address"],
    "amount":             ["transaction amount"],
    "date":               ["transactiondate", "transaction date"],
    "purpose":            ["transaction description"],
}


def _resolve_headers(fieldnames: list[str], alias_map: dict[str, list[str]]) -> dict[str, str | None]:
    lookup = {_norm_header(h): h for h in fieldnames if h}
    resolved = {}
    for logical, aliases in alias_map.items():
        found = None
        for alias in aliases:
            if alias in lookup:
                found = lookup[alias]
                break
        resolved[logical] = found
    return resolved


def _get(row: dict, resolved: dict, logical: str) -> str:
    col = resolved.get(logical)
    if not col:
        return ""
    val = row.get(col)
    return val if val is not None else ""


# ========================= value helpers ===============================

def _clean(val) -> str:
    return (val or "").strip()


def _clean_zip_raw(val) -> str:
    """Strip a leading straight-quote seen on about 0.3 percent of sampled ZIP
    values (e.g. a value like Q0506-0506 where Q stands in for a leading
    apostrophe character) -- an Excel/export text-preservation artifact, not
    part of the actual value. The digits themselves are passed through
    unchanged to utils.clean_zip() -- some of these are genuinely truncated
    4-digit codes in the source data (a real NH data-quality gap flagged by
    validate.py's tier-2 ZIP-format check, not something to guess-fix here)."""
    v = (val or "").strip()
    quote_char = chr(39)
    return v[1:] if v.startswith(quote_char) else v


def parse_amount(val: str) -> str:
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]
    try:
        return str(float(v))
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt).date()
        except ValueError:
            continue
        if MIN_VALID_YEAR <= d.year <= MAX_VALID_YEAR:
            return d.strftime("%Y-%m-%d")
        return ""
    return ""


_ALIAS_PAREN_RE = re.compile(r"\s*\([^)]*\)\s*$")


def split_candidate_name(raw: str) -> tuple[str, str, str]:
    """Return (full, first, last) from NH's "Last, First Middle" candidate
    name format, with a trailing parenthetical ballot-name/alias (e.g.
    "Long, Patrick T. (Long, Pat )") stripped -- see module docstring."""
    raw = (raw or "").strip()
    if not raw:
        return "", "", ""
    main = _ALIAS_PAREN_RE.sub("", raw).strip()
    main = re.sub(r"\s+", " ", main)
    if not main:
        return "", "", ""
    if "," in main:
        last, _, rest = main.partition(",")
        last = last.strip()
        rest = rest.strip()
        first = rest.split()[0] if rest else ""
    else:
        tokens = main.split()
        last = tokens[-1] if tokens else ""
        first = tokens[0] if len(tokens) > 1 else ""
    return main, first, last


# "123 Main St, Manchester, NH 03101" or "..., Manchester, NH 03101-1234"
_ADDR_RE = re.compile(
    r"^(?P<street>.*),\s*(?P<city>[^,]+?),\s*(?P<state>[A-Za-z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)\s*$"
)


def split_payee_address(addr: str) -> tuple[str, str, str]:
    """Best-effort split of NH's single combined payee-address field into
    (city, state, zip). No canonical column exists for the street portion
    (dropped). Returns ("", "", "") if the address is blank or doesn't
    match the expected trailing "City, ST ZIP" shape (rather than guessing)."""
    addr = (addr or "").strip()
    if not addr:
        return "", "", ""
    m = _ADDR_RE.match(addr)
    if not m:
        return "", "", ""
    return m.group("city").strip(), m.group("state").strip().upper(), m.group("zip").strip()


def _year_from_filename(name: str) -> str:
    m = re.search(r"(\d{4})", name)
    return m.group(1) if m else ""


# ========================== run ==========================================

def run():
    log = get_logger("new hampshire", "parse")
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

    committees_by_filer: dict[str, dict] = {}
    candidates_by_filer: dict[str, dict] = {}

    def upsert_committee(filer_id, committee_name, committee_type, candidate_name,
                         election_year, raw_file, row_num):
        if not filer_id or not committee_name:
            return
        committees_by_filer[filer_id] = {
            "state": STATE, "person_id": "",
            "committee_name": committee_name,
            "committee_type": committee_type,
            "election_year": election_year,
            "candidate_name": candidate_name,
            "treasurer_name": "", "city": "", "zip": "",
            "active": "",
            "state_filer_id": filer_id,
            "raw_file": raw_file, "row_num": row_num,
        }

    def upsert_candidate(filer_id, candidate_name, candidate_first, candidate_last,
                         election_year, raw_file, row_num):
        if not filer_id or not candidate_name:
            return
        candidates_by_filer[filer_id] = {
            "state": STATE, "person_id": "",
            "candidate_name": candidate_name,
            "candidate_first": candidate_first,
            "candidate_last": candidate_last,
            "office": "", "canonical_office": "",
            "district": "", "jurisdiction": "",
            "party": "", "election_year": election_year,
            "incumbent": "", "state_filer_id": filer_id,
            "raw_file": raw_file, "row_num": row_num,
        }

    # -- 1. Receipts -> contributions.csv.gz ------------------------------
    log.info("  Parsing receipts...")
    contrib_path = CLEAN_DIR / "contributions.csv.gz"
    contrib_count = 0

    receipt_files = sorted(RAW_DIR.glob("receipts_*.csv"), key=lambda p: _year_from_filename(p.name))

    with gzip.open(contrib_path, "wt", newline="", encoding="utf-8") as out_f:
        w = csv.DictWriter(out_f, fieldnames=C.CONTRIBUTIONS, extrasaction="ignore", restval="")
        w.writeheader()

        for raw_file in receipt_files:
            year_from_name = _year_from_filename(raw_file.name)
            rows_in = rows_out = 0
            with open(raw_file, encoding="cp1252", errors="replace") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                resolved = _resolve_headers(reader.fieldnames, RECEIPT_ALIASES)
                if not resolved.get("amount") or not resolved.get("date"):
                    log.file_parse_error(
                        filename=raw_file.name,
                        error=f"could not resolve amount/date columns in header "
                             f"{reader.fieldnames!r} -- check RECEIPT_ALIASES",
                    )
                    continue

                for row in reader:
                    rows_in += 1
                    filer_id = _clean(_get(row, resolved, "filing_entity_id"))

                    # NH's receipts file often sets "Candidate Name" to the exact same
                    # string as "Committee Name" for non-candidate filers (PACs, party
                    # town committees, associations) -- confirmed against real data:
                    # 202,264 of 268k sampled rows have Candidate Name == Committee Name,
                    # and every one of those checked is a PAC/party/association, not an
                    # actual candidate. A genuine candidate committee's Candidate Name is
                    # the person's own name and differs from the (often blank, or a
                    # separate campaign-brand) Committee Name -- e.g. "Craig, Joyce" /
                    # "Joyce Craig for NH". So only treat Candidate Name as real when it
                    # differs from Committee Name (including "differs from blank").
                    raw_committee_col = _clean(_get(row, resolved, "committee_name"))
                    raw_candidate_col = _clean(_get(row, resolved, "candidate_name"))
                    is_candidate_row = bool(raw_candidate_col) and raw_candidate_col != raw_committee_col
                    if is_candidate_row:
                        cand_full, cand_first, cand_last = split_candidate_name(raw_candidate_col)
                    else:
                        cand_full, cand_first, cand_last = "", "", ""

                    committee_name = raw_committee_col or cand_full or raw_candidate_col
                    if not committee_name:
                        continue

                    amount = parse_amount(_get(row, resolved, "amount"))
                    dt     = parse_date(_get(row, resolved, "date"))
                    if not amount or not dt:
                        continue

                    raw_type    = _get(row, resolved, "transaction_type")
                    raw_subtype = _get(row, resolved, "transaction_subtype")
                    txn_type = (raw_subtype.strip()
                               if raw_type.strip().lower() == "receipt" and raw_subtype.strip()
                               else raw_type.strip())

                    election_year = _clean(_get(row, resolved, "election_year")) or year_from_name

                    committee_subtype = _clean(_get(row, resolved, "committee_subtype"))
                    committee_type = committee_subtype or ("Candidate Committee" if cand_full else "")

                    upsert_committee(filer_id, committee_name, committee_type, cand_full,
                                     election_year, raw_file.name, rows_in + 1)
                    if cand_full:
                        upsert_candidate(filer_id, cand_full, cand_first, cand_last,
                                         election_year, raw_file.name, rows_in + 1)

                    contrib_count += 1
                    rows_out += 1
                    w.writerow({
                        "state": STATE,
                        "committee_name": committee_name,
                        "amount": amount,
                        "date": dt,
                        "transaction_type": txn_type,
                        "contributor_name": _clean(_get(row, resolved, "contributor_name")),
                        "contributor_type": _clean(_get(row, resolved, "contributor_type")),
                        "contributor_city": _clean(_get(row, resolved, "contributor_city")),
                        "contributor_state": _clean(_get(row, resolved, "contributor_state")),
                        "contributor_zip": utils.clean_zip(_clean_zip_raw(_get(row, resolved, "contributor_zip"))),
                        "employer": _clean(_get(row, resolved, "contributor_employer")),
                        "occupation": _clean(_get(row, resolved, "contributor_occupation")),
                        "candidate_name": cand_full,
                        "office": "",
                        "election_year": election_year,
                        "amended": "",
                        "filing_id": "",
                        "raw_file": raw_file.name,
                        "row_num": rows_in + 1,
                    })

            log.file_parsed(raw_file.name, "contributions", rows_out, skipped=rows_in - rows_out)

    log.info(f"    -> {contrib_count:,} contributions total")

    # -- 2. Expenditures -> expenditures.csv.gz ----------------------------
    log.info("  Parsing expenditures...")
    expend_path = CLEAN_DIR / "expenditures.csv.gz"
    expend_count = 0

    expend_files = sorted(RAW_DIR.glob("expenditures_*.csv"), key=lambda p: _year_from_filename(p.name))

    with gzip.open(expend_path, "wt", newline="", encoding="utf-8") as out_f:
        w = csv.DictWriter(out_f, fieldnames=C.EXPENDITURES, extrasaction="ignore", restval="")
        w.writeheader()

        for raw_file in expend_files:
            year_from_name = _year_from_filename(raw_file.name)
            rows_in = rows_out = 0
            with open(raw_file, encoding="cp1252", errors="replace") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                resolved = _resolve_headers(reader.fieldnames, EXPEND_ALIASES)
                if not resolved.get("amount") or not resolved.get("date"):
                    log.file_parse_error(
                        filename=raw_file.name,
                        error=f"could not resolve amount/date columns in header "
                             f"{reader.fieldnames!r} -- check EXPEND_ALIASES",
                    )
                    continue

                for row in reader:
                    rows_in += 1
                    filer_id    = _clean(_get(row, resolved, "filing_entity_id"))
                    entity_name = _clean(_get(row, resolved, "filing_entity_name"))
                    if not entity_name:
                        continue

                    amount = parse_amount(_get(row, resolved, "amount"))
                    dt     = parse_date(_get(row, resolved, "date"))
                    if not amount or not dt:
                        continue

                    entity_type = _clean(_get(row, resolved, "filing_entity_type"))
                    if entity_type.lower() in _CANDIDATE_ENTITY_TYPES:
                        cand_full, cand_first, cand_last = split_candidate_name(entity_name)
                    else:
                        cand_full, cand_first, cand_last = "", "", ""

                    raw_type    = _get(row, resolved, "transaction_type")
                    raw_subtype = _get(row, resolved, "transaction_subtype")
                    txn_type = (raw_subtype.strip()
                               if raw_type.strip().lower() == "expenditure" and raw_subtype.strip()
                               else raw_type.strip())

                    election_year = year_from_name   # no per-row year column on this file

                    committee_type = entity_type or ("Candidate Committee" if cand_full else "")

                    upsert_committee(filer_id, entity_name, committee_type, cand_full,
                                     election_year, raw_file.name, rows_in + 1)
                    if cand_full:
                        upsert_candidate(filer_id, cand_full, cand_first, cand_last,
                                         election_year, raw_file.name, rows_in + 1)

                    payee_city, payee_state, payee_zip = split_payee_address(
                        _get(row, resolved, "payee_address"))

                    expend_count += 1
                    rows_out += 1
                    w.writerow({
                        "state": STATE,
                        "committee_name": entity_name,
                        "amount": amount,
                        "date": dt,
                        "transaction_type": txn_type,
                        "payee_name": _clean(_get(row, resolved, "payee_name")),
                        "purpose": _clean(_get(row, resolved, "purpose")),
                        "category": "",
                        "payee_city": payee_city,
                        "payee_state": payee_state,
                        "payee_zip": utils.clean_zip(_clean_zip_raw(payee_zip)),
                        "candidate_name": cand_full,
                        "office": "",
                        "election_year": election_year,
                        "amended": "",
                        "filing_id": "",
                        "raw_file": raw_file.name,
                        "row_num": rows_in + 1,
                    })

            log.file_parsed(raw_file.name, "expenditures", rows_out, skipped=rows_in - rows_out)

    log.info(f"    -> {expend_count:,} expenditures total")

    # -- 3. Write candidates/committees (backfilled from both files) ------
    cand_rows = list(candidates_by_filer.values())
    comm_rows = list(committees_by_filer.values())

    cand_path = CLEAN_DIR / "candidates.csv.gz"
    with gzip.open(cand_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.CANDIDATES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(cand_rows)
    n_cands = utils.assign_person_ids(cand_path, id_model="committee")
    log.file_parsed("candidates.csv.gz", "candidates", n_cands, role="output")

    comm_path = CLEAN_DIR / "committees.csv.gz"
    with gzip.open(comm_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.COMMITTEES, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(comm_rows)
    n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
    log.file_parsed("committees.csv.gz", "committees", len(comm_rows), role="output")
    log.info(f"    -> {n_cands:,} candidates, {len(comm_rows):,} committees "
            f"({n_comm_matched:,} matched to a candidate)")

    # -- 4. Loans/debts -- folded into contributions.csv.gz, see docstring -
    loans_path = CLEAN_DIR / "loans_debts.csv.gz"
    with gzip.open(loans_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.LOANS_DEBTS, extrasaction="ignore", restval="")
        w.writeheader()

    duration = round(time.perf_counter() - t0, 1)
    log._emit("parse_completed", status="completed", duration_s=duration,
              candidates=n_cands, committees=len(comm_rows),
              contributions=contrib_count, expenditures=expend_count)
    log.info(f"Done in {duration}s")


# ============================== CLI ======================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
