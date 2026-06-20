"""
illinois.py — Parse Illinois ISBE bulk TSV exports into canonical cleaned CSVs.

Raw files (all in data/Illinois/raw/, tab-delimited, latin-1, QUOTE_NONE):
  Candidates.txt          (~32K rows)  — candidate registry, keyed by ID
  Committees.txt          (~34K rows)  — committee registry, keyed by ID
  CmteCandidateLinks.txt  (~37K rows)  — CommitteeID <-> CandidateID links
  Receipts.txt            (~6.5M rows) — every contribution since 1994
  Expenditures.txt        (~4.8M rows) — every expenditure since 1994

id_model="committee": state_filer_id = Candidates.ID for candidates,
Committees.ID for committees. person_id is grouped by (state, candidate_name,
office, district) via utils.assign_person_ids(); committees are matched to
candidates by name via utils.assign_committee_person_ids().

A committee can appear in CmteCandidateLinks.txt linked to more than one
candidate (committees are occasionally reassigned to a new candidate across
cycles). We take the LAST link encountered per committee, so the most
recently-linked candidate "wins" for committee_name/candidate_name/office —
a known simplification since we don't have a per-transaction election year
to disambiguate (FiledDocs.txt is out of scope, see scrapers/illinois.py).

D2Part is ISBE's Schedule A/B "part" code — the closest thing IL has to a
transaction type:
  Receipts (Schedule A): 1A individual, 2A other committee/PAC, 3A loan
    received, 4A other receipts (interest/investment/bank), 5A in-kind /
    other receipts (non-monetary, has Description)
  Expenditures (Schedule B): 6B/8B operating expenditures, 7B loans made,
    9B independent expenditures (carries its own CandidateName/Office/
    Supporting/Opposing — the only D2Part where those fields are populated)

3A receipts and 7B expenditures are diverted to loans_debts.csv as
"Loan Received"/"Loan Made"; everything else keeps its raw D2Part code as
transaction_type. For 9B rows, candidate_name/office come from the row's own
CandidateName/Office (the target of the independent expenditure) rather than
the committee's linked candidate, and category records the Supporting/
Opposing direction.

FiledDocs.txt is out of scope (see scrapers/illinois.py docstring): filing_id
is the raw FiledDocID passthrough; election_year/amended are left blank.

~33 of 6.48M Receipts.txt rows and ~2 of 4.82M Expenditures.txt rows have
embedded newlines/tabs in free-text fields that break column alignment —
these are detected (field count != header count) and skipped.

Archived="True" rows (~2.66M/6.48M Receipts, ~1.47M/4.82M Expenditures) are
skipped entirely. ISBE re-emits a fresh copy (new ID/FiledDocID, Archived=
"False") of every transaction each time a committee's report is amended;
the "Archived" flag marks the prior, superseded copy. A sample check found
95-96% of Archived="True" rows have an exact (CommitteeID, Amount,
date, name) match among Archived="False" rows, confirming these are stale
duplicates, not independent transactions. Including them roughly doubled
contribution/expenditure totals. This filter also resolves the handful of
outlier rows previously flagged here (a $8.1B EF Design Group expenditure
and three >$100M receipts with AggregateAmount=0) — all were orphaned
Archived="True" rows with no current counterpart, i.e. corrected-away
draft-filing artifacts.

Output (data/Illinois/cleaned/):
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
RAW_DIR      = PROJECT_ROOT / "data" / "Illinois" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Illinois" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "IL"
EARLIEST_YEAR  = 1994
MAX_VALID_YEAR = date.today().year + 4

# D2Part codes that represent loans rather than ordinary receipts/expenditures
LOAN_RECEIVED_PART = "3A"
LOAN_MADE_PART     = "7B"


# ============================== Helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val) -> str:
    """Parse a dollar amount string to a plain numeric string; parentheses become negative. Returns '' on failure."""
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


def parse_date(val) -> str:
    """'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DD'. Returns '' on failure or out-of-range year."""
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


def join_name(first, last) -> str:
    """'FirstName' + 'LastOnlyName' -> 'First Last'. PACs/committees only
    populate LastOnlyName, leaving FirstName blank."""
    first, last = clean(first), clean(last)
    if first and last:
        return f"{first} {last}"
    return last or first


def open_writer(filename: str, fieldnames: list):
    """Open a gzipped CSV writer in CLEAN_DIR; extra fields are dropped, missing fields default to ''."""
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def reader_for(name: str):
    """Open a raw ISBE TSV file for DictReader iteration (latin-1, QUOTE_NONE)."""
    fh = open(RAW_DIR / name, encoding="latin-1", newline="")
    return fh, csv.DictReader(fh, delimiter="\t", quoting=csv.QUOTE_NONE)


def is_malformed(row: dict) -> bool:
    """Detect rows whose field count doesn't match the header. A handful of
    rows have embedded newlines/tabs in free-text fields that throw off
    column alignment — DictReader fills missing trailing fields with None
    (restval default) and stashes extra fields under the None key."""
    return None in row.values() or row.get(None) is not None


# ================================ Main ================================
def run(limit=None):
    """Parse Illinois ISBE bulk exports into canonical cleaned CSVs.

    `limit`, if given, caps the number of data rows read from Receipts.txt
    and Expenditures.txt — for quick smoke-testing only; omit for a full run.
    """
    log = get_logger("illinois", "parse")
    t0  = time.perf_counter()
    log.info("Starting Illinois parser")
    log._emit("parse_started", limit=limit)

    total_contributions  = 0
    total_expenditures    = 0
    total_loans           = 0
    skipped_receipts      = 0
    skipped_expenditures  = 0
    archived_receipts     = 0
    archived_expenditures = 0
    n_candidates          = 0
    n_committees          = 0

    file_handles = []

    try:
        # =================== Candidates ===================
        cand_fh, cand_w = open_writer("candidates.csv.gz", C.CANDIDATES)
        file_handles.append(cand_fh)

        candidate_info: dict[str, dict] = {}  # CandidateID -> {name, office}
        ft = time.perf_counter()
        fh, reader = reader_for("Candidates.txt")
        with fh:
            for row_num, row in enumerate(reader, start=2):
                if is_malformed(row):
                    continue
                cid    = clean(row["ID"])
                name   = utils.clean_name(join_name(row["FirstName"], row["LastName"]))
                office = utils.clean_name(row["Office"])
                candidate_info[cid] = {"name": name, "office": office}
                cand_w.writerow({
                    "state":           STATE,
                    "candidate_name":  name,
                    "candidate_first": utils.clean_name(row["FirstName"]),
                    "candidate_last":  utils.clean_name(row["LastName"]),
                    "office":          office,
                    "district":        clean(row["District"]),
                    "jurisdiction":    utils.clean_name(row["DistrictType"]),
                    "party":           utils.clean_name(row["PartyAffiliation"]),
                    "election_year":   "",
                    "incumbent":       "",
                    "state_filer_id":  cid,
                    "raw_file":        "Candidates.txt",
                    "row_num":         row_num,
                })
                n_candidates += 1
        cand_fh.close()
        file_handles.remove(cand_fh)
        log.file_parsed("Candidates.txt", "candidates", n_candidates,
                        duration_s=time.perf_counter() - ft,
                        bytes=(RAW_DIR / "Candidates.txt").stat().st_size)

        # =================== CmteCandidateLinks ===================
        committee_candidate: dict[str, str] = {}  # CommitteeID -> CandidateID (last wins)
        fh, reader = reader_for("CmteCandidateLinks.txt")
        with fh:
            for row in reader:
                if is_malformed(row):
                    continue
                committee_candidate[clean(row["CommitteeID"])] = clean(row["CandidateID"])
        log.registry_loaded("CmteCandidateLinks.txt", len(committee_candidate),
                            relation="committee_candidate_links")

        # =================== Committees ===================
        cmte_fh, cmte_w = open_writer("committees.csv.gz", C.COMMITTEES)
        file_handles.append(cmte_fh)

        committee_info: dict[str, dict] = {}  # CommitteeID -> {name, candidate_name, office}
        ft = time.perf_counter()
        fh, reader = reader_for("Committees.txt")
        with fh:
            for row_num, row in enumerate(reader, start=2):
                if is_malformed(row):
                    continue
                cid     = clean(row["ID"])
                cand_id = committee_candidate.get(cid, "")
                cand    = candidate_info.get(cand_id, {})

                cmte_type = clean(row["TypeOfCommittee"])
                if not cmte_type and cand_id:
                    # ~13K committees have no TypeOfCommittee but ARE linked
                    # to a candidate via CmteCandidateLinks — infer "Candidate".
                    cmte_type = "Candidate"

                status = clean(row["Status"])
                active = "1" if status == "A" else ("0" if status == "F" else "")

                cname = utils.clean_name(row["Name"])
                committee_info[cid] = {
                    "name":           cname,
                    "candidate_name": cand.get("name", ""),
                    "office":         cand.get("office", ""),
                }
                cmte_w.writerow({
                    "state":          STATE,
                    "committee_name": cname,
                    "committee_type": cmte_type,
                    "election_year":  "",
                    "candidate_name": cand.get("name", ""),
                    "treasurer_name": "",
                    "city":           utils.clean_name(row["City"]),
                    "zip":            utils.clean_zip(clean(row["Zip"])),
                    "active":         active,
                    "state_filer_id": cid,
                    "raw_file":       "Committees.txt",
                    "row_num":        row_num,
                })
                n_committees += 1
        cmte_fh.close()
        file_handles.remove(cmte_fh)
        log.file_parsed("Committees.txt", "committees", n_committees,
                        duration_s=time.perf_counter() - ft,
                        bytes=(RAW_DIR / "Committees.txt").stat().st_size)

        # Assign person IDs now — transactions don't need it, but this gets
        # candidates.csv.gz / committees.csv.gz fully finished early.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        # =================== Receipts -> contributions / loans_debts ===================
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz", C.LOANS_DEBTS)
        file_handles += [cont_fh, loan_fh]

        ft = time.perf_counter()
        fh, reader = reader_for("Receipts.txt")
        with fh:
            for row_num, row in enumerate(reader, start=2):
                if limit is not None and row_num - 1 > limit:
                    break
                if is_malformed(row):
                    skipped_receipts += 1
                    continue
                if row["Archived"] == "True":
                    archived_receipts += 1
                    continue
                amount = parse_amount(row["Amount"])
                if not amount:
                    continue

                committee_id = clean(row["CommitteeID"])
                cmte   = committee_info.get(committee_id, {})
                d2part = clean(row["D2Part"])

                contributor_name = utils.clean_name(join_name(row["FirstName"], row["LastOnlyName"]))
                # No explicit contributor-type field in the source; ISBE's D-2
                # forms only collect First/Last name for individuals — PACs and
                # other organizations are recorded in LastOnlyName alone.
                contributor_type = "Individual" if clean(row["FirstName"]) else "Organization"

                city = utils.clean_name(row["City"])
                st   = clean(row["State"])
                zipc = utils.clean_zip(clean(row["Zip"]))

                row_data = {
                    "state":             STATE,
                    "committee_name":    cmte.get("name", ""),
                    "amount":            amount,
                    "original_amount":   amount,
                    "date":              parse_date(row["RcvDate"]),
                    "transaction_type":  d2part,
                    "record_type":       "Loan Received",
                    "contributor_name":  contributor_name,
                    "contributor_type":  contributor_type,
                    "contributor_city":  city,
                    "contributor_state": st,
                    "contributor_zip":   zipc,
                    "counterparty_name":  contributor_name,
                    "counterparty_city":  city,
                    "counterparty_state": st,
                    "counterparty_zip":   zipc,
                    "employer":       utils.clean_name(row["Employer"]),
                    "occupation":     utils.clean_name(row["Occupation"]),
                    "candidate_name": cmte.get("candidate_name", ""),
                    "office":         cmte.get("office", ""),
                    "election_year":  "",
                    "amended":        "",
                    "filing_id":      clean(row["FiledDocID"]),
                    "raw_file":       "Receipts.txt",
                    "row_num":        row_num,
                }
                if d2part == LOAN_RECEIVED_PART:
                    loan_w.writerow(row_data)
                    total_loans += 1
                else:
                    cont_w.writerow(row_data)
                    total_contributions += 1

        log.file_parsed("Receipts.txt", "contributions", total_contributions,
                        duration_s=time.perf_counter() - ft,
                        bytes=(RAW_DIR / "Receipts.txt").stat().st_size,
                        skipped=skipped_receipts, archived=archived_receipts)

        # =================== Expenditures -> expenditures / loans_debts ===================
        expn_fh, expn_w = open_writer("expenditures.csv.gz", C.EXPENDITURES)
        file_handles.append(expn_fh)

        ft = time.perf_counter()
        fh, reader = reader_for("Expenditures.txt")
        with fh:
            for row_num, row in enumerate(reader, start=2):
                if limit is not None and row_num - 1 > limit:
                    break
                if is_malformed(row):
                    skipped_expenditures += 1
                    continue
                if row["Archived"] == "True":
                    archived_expenditures += 1
                    continue
                amount = parse_amount(row["Amount"])
                if not amount:
                    continue

                committee_id = clean(row["CommitteeID"])
                cmte   = committee_info.get(committee_id, {})
                d2part = clean(row["D2Part"])

                payee_name = utils.clean_name(join_name(row["FirstName"], row["LastOnlyName"]))
                city = utils.clean_name(row["City"])
                st   = clean(row["State"])
                zipc = utils.clean_zip(clean(row["Zip"]))

                # 9B independent expenditures carry their own target
                # candidate/office + Supporting/Opposing flags; everything
                # else falls back to the committee's linked candidate.
                ie_candidate = utils.clean_name(row["CandidateName"])
                ie_office    = clean(row["Office"])
                candidate_name = ie_candidate or cmte.get("candidate_name", "")
                office          = ie_office or cmte.get("office", "")

                category = ""
                if d2part == "9B":
                    if row["Supporting"] == "True":
                        category = "Independent Expenditure - Supporting"
                    elif row["Opposing"] == "True":
                        category = "Independent Expenditure - Opposing"
                    else:
                        category = "Independent Expenditure"

                row_data = {
                    "state":            STATE,
                    "committee_name":   cmte.get("name", ""),
                    "amount":           amount,
                    "original_amount":  amount,
                    "date":             parse_date(row["ExpendedDate"]),
                    "transaction_type": d2part,
                    "record_type":      "Loan Made",
                    "payee_name":  payee_name,
                    "purpose":     clean(row["Purpose"]),
                    "category":    category,
                    "payee_city":  city,
                    "payee_state": st,
                    "payee_zip":   zipc,
                    "counterparty_name":  payee_name,
                    "counterparty_city":  city,
                    "counterparty_state": st,
                    "counterparty_zip":   zipc,
                    "candidate_name": candidate_name,
                    "office":         office,
                    "election_year":  "",
                    "amended":        "",
                    "filing_id":      clean(row["FiledDocID"]),
                    "raw_file":       "Expenditures.txt",
                    "row_num":        row_num,
                }
                if d2part == LOAN_MADE_PART:
                    loan_w.writerow(row_data)
                    total_loans += 1
                else:
                    expn_w.writerow(row_data)
                    total_expenditures += 1

        log.file_parsed("Expenditures.txt", "expenditures", total_expenditures,
                        duration_s=time.perf_counter() - ft,
                        bytes=(RAW_DIR / "Expenditures.txt").stat().st_size,
                        skipped=skipped_expenditures, archived=archived_expenditures)

        for f in file_handles:
            f.close()
        file_handles = []

        def _out_bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions, role="output",
                        bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,  role="output",
                        bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,         role="output",
                        bytes=_out_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    n_committees,        role="output",
                        bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    n_candidates,        role="output",
                        bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=n_committees, candidates=n_candidates,
                  skipped_receipts=skipped_receipts, skipped_expenditures=skipped_expenditures)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=n_committees, candidates=n_candidates)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans_debts=total_loans, committees=n_committees, candidates=n_candidates,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for f in file_handles:
            try:
                f.close()
            except Exception:
                pass


# ====== CLI ==================================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Parse Illinois ISBE bulk exports into canonical cleaned CSVs."
    )
    ap.add_argument("--limit", type=int, default=None,
                    help="debug: cap data rows read from Receipts.txt/Expenditures.txt "
                         "(default: unlimited — process everything)")
    args, _ = ap.parse_known_args()
    try:
        run(limit=args.limit)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
