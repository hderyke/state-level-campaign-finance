"""
washington.py — Parse Washington PDC raw Socrata exports into canonical cleaned CSVs.

Raw files (all in data/Washington/raw/):
  Contributions_YYYY.csv / Contributions_misc.csv  -> contributions
  Expenditures_YYYY.csv  / Expenditures_misc.csv    -> expenditures
  Loans_YYYY.csv         / Loans_misc.csv           -> loans_debts (loan lifecycle:
                                                       received/payment/interest/forgiven)
  Debt_YYYY.csv          / Debt_misc.csv            -> loans_debts (reported debt)

None of PDC's 4 public datasets is a standalone candidate/committee registry —
every contribution/expenditure/loan/debt row already carries the filer's
identity inline (filer_id, filer_name, office, legislative_district, party,
jurisdiction(_county/_type), plus committee_id). So, unlike states with a
dedicated registry file (e.g. Hawaii's SOI/Affidavits), candidates and
committees here are built entirely from these 4 transaction files as they're
processed — there's no separate "entity pass".

Two different identifiers are in play:
  filer_id     — person-level: "consistent across election years" per PDC's own
                 field docs (the one documented exception is a candidate running
                 for a second office in the same year, who gets a second filer_id
                 with no link to the first — a real ambiguity in the source, not
                 a parsing bug). Used as state_filer_id for CANDIDATES;
                 id_model="person" (same family as AR/CO/MN — no grouping needed,
                 person_id is derived directly from state_filer_id).
  committee_id — committee-level: per PDC docs, single-year committees and
                 candidate committees get a new id every year even when the same
                 person/org is behind them; continuing committees and surplus
                 accounts keep one id across years. Used as state_filer_id for
                 COMMITTEES (committees.person_id is filled in afterwards by
                 utils.assign_committee_person_ids() via candidate_name matching,
                 not from committee_id).

A filer is a candidate only when its "type" field (or "filer_type" on the debt
dataset, which spells it "CA"/"CO" instead) says "Candidate"/"CA" — Political
Committee rows enrich the committees table only, with a blank candidate_name
(same convention as Hawaii's NC committees).

Because the same filer_id/committee_id can recur across many election cycles
with a different office/district/party each time (e.g. someone running for
State Representative in 2018 and State Senate in 2022), candidate enrichment
is recency-weighted rather than first-wins: a row whose election_year is >=
the best year seen so far for that filer_id overwrites office/district/party/
jurisdiction outright; a row from an older cycle only backfills fields that
are still blank. This differs from Hawaii's simple "first non-blank wins"
because Hawaii's reg_no is already split per cycle (one row of registry data
per reg_no), whereas here a single filer_id accumulates rows from every cycle
of that person's career.

Output (data/Washington/cleaned/):
  contributions.csv.gz, expenditures.csv.gz, loans_debts.csv.gz,
  committees.csv.gz, candidates.csv.gz
"""

import csv
import gzip
import re
import sys
import time
from pathlib import Path
from datetime import date, datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Washington" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Washington" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "WA"
EARLIEST_YEAR  = 1990
MAX_VALID_YEAR = date.today().year + 4

# Debt dataset spells the Candidate/Committee split as filer_type "CA"/"CO"
# instead of the "type" field's "Candidate"/"Political Committee" strings used
# by the other 3 datasets.
FILER_TYPE_MAP = {"CA": "Candidate", "CO": "Political Committee"}


# ============================== Helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Parse a dollar amount string to a plain numeric string; parentheses become negative
    (WA's own data already signs negatives with a leading '-', e.g. correction rows, but
    parens are handled too for robustness). Returns '' on failure."""
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
    """Socrata floating timestamp 'YYYY-MM-DDTHH:MM:SS.000' -> 'YYYY-MM-DD'. Returns ''
    on failure or out-of-range year (a handful of source rows carry garbage years like
    '0202' or '2202' — confirmed against the live API, not a parsing artifact)."""
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


def is_amended(origin: str) -> str:
    """WA marks correction records with an origin code that starts with 'C.'
    (e.g. 'C.1', 'C.2', 'C.3' — schedule-C1 corrections), distinct from
    original-filing codes that merely start with 'C' (e.g. 'C3', 'C3.1A').
    Returns '1' for corrections, '' otherwise (most origins, and all of the
    debt/loan datasets, never use this pattern)."""
    return "1" if (origin or "").strip().upper().startswith("C.") else ""


def filer_kind(type_val: str = "", filer_type_val: str = "") -> str:
    """Return 'Candidate' or 'Political Committee' from whichever field the
    dataset provides ('type' on contributions/expenditures/loans, 'filer_type'
    spelled 'CA'/'CO' on debt). Returns '' if neither field has a value."""
    t = clean(type_val)
    if t:
        return "Candidate" if t == "Candidate" else "Political Committee"
    ft = clean(filer_type_val).upper()
    if ft:
        return FILER_TYPE_MAP.get(ft, "Political Committee")
    return ""


def split_name(raw: str) -> tuple[str, str]:
    """'First [Middle] Last' -> (first_middle, last). Strips a trailing
    parenthetical nickname first, e.g. 'Luz D. Barefoot (Lucy Barefoot)' ->
    splits 'Luz D. Barefoot' -> ('Luz D.', 'Barefoot'). No comma-inversion
    needed here (unlike Hawaii) — WA's filer_name is already First-Last order."""
    name = re.sub(r"\s*\([^)]*\)\s*$", "", clean(raw)).strip()
    parts = name.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def raw_files(pattern: str) -> list[Path]:
    """Return non-empty raw files matching a glob pattern, sorted by name.
    The pattern (e.g. 'Contributions_*.csv') matches both the per-year files
    and the '_misc.csv' catch-all for that relation."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    """Open a gzipped CSV writer in CLEAN_DIR; extra fields are dropped, missing fields default to ''."""
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def _fill(d: dict, key: str, val: str) -> None:
    """Set d[key] = val only if val is truthy and d[key] is currently empty."""
    if val and not d.get(key):
        d[key] = val


# ================================ Main ================================
def run():
    log = get_logger("washington", "parse")
    t0  = time.perf_counter()
    log.info("Starting Washington parser")
    log._emit("parse_started")

    candidates: dict[str, dict] = {}   # keyed by filer_id (person-level)
    committees: dict[str, dict] = {}   # keyed by committee_id

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0

    file_handles = []

    # =================== Registry helpers ===================
    def register_candidate(filer_id: str, filer_name: str, kind: str,
                            office: str = "", district: str = "", party: str = "",
                            jurisdiction: str = "", election_year: str = "",
                            raw_file: str = "", row_num="") -> None:
        """Register/enrich a candidate, keyed by the person-level filer_id.
        A filer_id only becomes a candidates row when kind == 'Candidate' —
        Political Committee filer_ids never appear here (they still get a
        committees row via register_committee)."""
        filer_id = clean(filer_id)
        if not filer_id or kind != "Candidate":
            return

        first, last = split_name(filer_name)
        cand = candidates.get(filer_id)
        if cand is None:
            cand = {
                "state":           STATE,
                "candidate_name":  utils.clean_name(filer_name),
                "candidate_first": utils.clean_name(first),
                "candidate_last":  utils.clean_name(last),
                "office":          "",
                "district":        "",
                "jurisdiction":    "",
                "party":           "",
                "election_year":   "",
                "incumbent":       "",
                "state_filer_id":  filer_id,
                "raw_file":        raw_file,
                "row_num":         row_num,
                "_best_year":      None,
            }
            candidates[filer_id] = cand

        ey_str = clean(election_year)
        y = int(ey_str) if ey_str.isdigit() else None
        best = cand.get("_best_year")
        is_latest = y is not None and (best is None or y >= best)

        updates = {"office": office, "district": district,
                   "jurisdiction": jurisdiction, "party": party}
        for key, val in updates.items():
            val = clean(val)
            if not val:
                continue
            if is_latest or not cand.get(key):
                cand[key] = val

        if is_latest:
            cand["_best_year"] = y
            name_clean = utils.clean_name(filer_name)
            if name_clean:
                cand["candidate_name"]  = name_clean
                cand["candidate_first"] = utils.clean_name(first)
                cand["candidate_last"]  = utils.clean_name(last)

        # election_year on the output row tracks the latest cycle seen,
        # independent of the office/district/party recency tie-break above.
        cur_str = cand.get("election_year", "")
        cur_y = int(cur_str) if cur_str.isdigit() else None
        if y is not None and (cur_y is None or y > cur_y):
            cand["election_year"] = str(y)

    def register_committee(committee_id: str, filer_name: str, kind: str,
                            election_year: str = "",
                            raw_file: str = "", row_num="") -> None:
        """Register/enrich a committee, keyed by committee_id. committee_id is
        already scoped to a single committee-cycle (or a genuinely continuing
        committee), so plain first-wins enrichment is sufficient here (unlike
        the recency-weighted candidate registry above)."""
        committee_id = clean(committee_id)
        if not committee_id:
            return

        cname = utils.clean_name(filer_name)
        ctype = "Candidate Committee" if kind == "Candidate" else "Political Committee"

        cmte = committees.get(committee_id)
        if cmte is None:
            committees[committee_id] = {
                "state":           STATE,
                "committee_name":  cname,
                "committee_type":  ctype,
                "election_year":   clean(election_year),
                "candidate_name":  cname if kind == "Candidate" else "",
                "treasurer_name":  "",
                "city":            "",
                "zip":             "",
                "active":          "",
                "state_filer_id":  committee_id,
                "raw_file":        raw_file,
                "row_num":         row_num,
            }
        else:
            _fill(cmte, "election_year", clean(election_year))
            _fill(cmte, "committee_name", cname)
            if kind == "Candidate":
                _fill(cmte, "candidate_name", cname)

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, loan_fh]

        # =================== Contributions ===================
        for path in raw_files("Contributions_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    filer_id    = clean(row.get("filer_id", ""))
                    committee_id = clean(row.get("committee_id", ""))
                    filer_name  = row.get("filer_name", "")
                    kind        = filer_kind(type_val=row.get("type", ""))
                    cname       = utils.clean_name(filer_name)
                    ey          = clean(row.get("election_year", ""))

                    register_candidate(filer_id, filer_name, kind,
                                        office=row.get("office", ""),
                                        district=row.get("legislative_district", ""),
                                        party=row.get("party", ""),
                                        jurisdiction=row.get("jurisdiction", ""),
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)
                    register_committee(committee_id, filer_name, kind,
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)

                    cont_w.writerow({
                        "state":             STATE,
                        "committee_name":    cname,
                        "amount":            amount,
                        "date":              parse_date(row.get("receipt_date", "")),
                        "transaction_type":  clean(row.get("cash_or_in_kind", "")) or "Contribution",
                        "contributor_name":  utils.clean_name(row.get("contributor_name", "")),
                        "contributor_type":  clean(row.get("code", "")) or clean(row.get("contributor_category", "")),
                        "contributor_city":  utils.clean_name(row.get("contributor_city", "")),
                        "contributor_state": clean(row.get("contributor_state", "")),
                        "contributor_zip":   utils.clean_zip(clean(row.get("contributor_zip", ""))),
                        "employer":          utils.clean_name(row.get("contributor_employer_name", "")),
                        "occupation":        utils.clean_name(row.get("contributor_occupation", "")),
                        "candidate_name":    cname if kind == "Candidate" else "",
                        "office":            clean(row.get("office", "")),
                        "election_year":     ey,
                        "amended":           is_amended(row.get("origin", "")),
                        "filing_id":         clean(row.get("report_number", "")),
                        "raw_file":          path.name,
                        "row_num":           row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_contributions += count

        # =================== Expenditures ===================
        for path in raw_files("Expenditures_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    filer_id     = clean(row.get("filer_id", ""))
                    committee_id = clean(row.get("committee_id", ""))
                    filer_name   = row.get("filer_name", "")
                    kind         = filer_kind(type_val=row.get("type", ""))
                    cname        = utils.clean_name(filer_name)
                    ey           = clean(row.get("election_year", ""))

                    register_candidate(filer_id, filer_name, kind,
                                        office=row.get("office", ""),
                                        district=row.get("legislative_district", ""),
                                        party=row.get("party", ""),
                                        jurisdiction=row.get("jurisdiction", ""),
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)
                    register_committee(committee_id, filer_name, kind,
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)

                    expn_w.writerow({
                        "state":            STATE,
                        "committee_name":   cname,
                        "amount":           amount,
                        "date":             parse_date(row.get("expenditure_date", "")),
                        "transaction_type": clean(row.get("itemized_or_non_itemized", "")) or "Expenditure",
                        "payee_name":       utils.clean_name(row.get("recipient_name", "")),
                        "purpose":          clean(row.get("description", "")),
                        "category":         clean(row.get("code", "")),
                        "payee_city":       utils.clean_name(row.get("recipient_city", "")),
                        "payee_state":      clean(row.get("recipient_state", "")),
                        "payee_zip":        utils.clean_zip(clean(row.get("recipient_zip", ""))),
                        "candidate_name":   cname if kind == "Candidate" else "",
                        "office":           clean(row.get("office", "")),
                        "election_year":    ey,
                        "amended":          is_amended(row.get("origin", "")),
                        "filing_id":        clean(row.get("report_number", "")),
                        "raw_file":         path.name,
                        "row_num":          row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_expenditures += count

        # =================== Loans (received / payment / interest / forgiven) ===================
        for path in raw_files("Loans_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    filer_id     = clean(row.get("filer_id", ""))
                    committee_id = clean(row.get("committee_id", ""))
                    filer_name   = row.get("filer_name", "")
                    kind         = filer_kind(type_val=row.get("type", ""))
                    cname        = utils.clean_name(filer_name)
                    ey           = clean(row.get("election_year", ""))
                    txn          = clean(row.get("transaction_type", ""))

                    register_candidate(filer_id, filer_name, kind,
                                        office=row.get("office", ""),
                                        district=row.get("legislative_district", ""),
                                        party=row.get("party", ""),
                                        jurisdiction=row.get("jurisdiction", ""),
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)
                    register_committee(committee_id, filer_name, kind,
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)

                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cname,
                        "original_amount":    amount,
                        "date":               parse_date(row.get("receipt_date", "")),
                        "record_type":        f"Loan {txn}".strip() if txn else "Loan",
                        "counterparty_name":  utils.clean_name(row.get("lenders_name", "")),
                        "counterparty_city":  utils.clean_name(row.get("lenders_city", "")),
                        "counterparty_state": clean(row.get("lenders_state", "")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("lenders_zip", ""))),
                        "candidate_name":     cname if kind == "Candidate" else "",
                        "election_year":      ey,
                        "amended":            is_amended(row.get("origin", "")),
                        "filing_id":          clean(row.get("report_number", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_loans += count

        # =================== Debt ===================
        for path in raw_files("Debt_*.csv"):
            ft, count = time.perf_counter(), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("amount", ""))
                    if not amount:
                        continue
                    filer_id     = clean(row.get("filer_id", ""))
                    committee_id = clean(row.get("committee_id", ""))
                    filer_name   = row.get("filer_name", "")
                    kind         = filer_kind(filer_type_val=row.get("filer_type", ""))
                    cname        = utils.clean_name(filer_name)
                    ey           = clean(row.get("election_year", ""))

                    register_candidate(filer_id, filer_name, kind,
                                        office=row.get("office", ""),
                                        district=row.get("legislative_district", ""),
                                        party=row.get("party", ""),
                                        jurisdiction=row.get("jurisdiction", ""),
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)
                    register_committee(committee_id, filer_name, kind,
                                        election_year=ey,
                                        raw_file=path.name, row_num=row_num)

                    debt_date = parse_date(row.get("debt_date", "")) or parse_date(row.get("thru_date", ""))
                    loan_w.writerow({
                        "state":              STATE,
                        "committee_name":     cname,
                        "original_amount":    amount,
                        "date":               debt_date,
                        "record_type":        "Debt",
                        "counterparty_name":  utils.clean_name(row.get("vendor_name", "")),
                        "counterparty_city":  utils.clean_name(row.get("vendor_city", "")),
                        "counterparty_state": clean(row.get("vendor_state", "")),
                        "counterparty_zip":   utils.clean_zip(clean(row.get("vendor_zip", ""))),
                        "candidate_name":     cname if kind == "Candidate" else "",
                        "election_year":      ey,
                        "amended":            is_amended(row.get("origin", "")),
                        "filing_id":          clean(row.get("report_number", "")),
                        "raw_file":           path.name,
                        "row_num":            row_num,
                    })
                    count += 1
            log.file_parsed(path.name, "loans_debts", count,
                            duration_s=time.perf_counter() - ft, bytes=path.stat().st_size)
            total_loans += count

        # =================== Flush candidates + committees ===================
        cand_fh, cand_w = open_writer("candidates.csv.gz", C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz", C.COMMITTEES)
        file_handles += [cand_fh, cmte_fh]

        for row in candidates.values():
            row.pop("_best_year", None)
            cand_w.writerow(row)

        for row in committees.values():
            cmte_w.writerow(row)

        # Close handles before person-ID assignment
        for fh in file_handles:
            fh.close()
        file_handles = []

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions, role="output",
                        bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,  role="output",
                        bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,         role="output",
                        bytes=_out_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    len(committees),     role="output",
                        bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    len(candidates),     role="output",
                        bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees), candidates=len(candidates))

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees), candidates=len(candidates))
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=len(committees), candidates=len(candidates),
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
