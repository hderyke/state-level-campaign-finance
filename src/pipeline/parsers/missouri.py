"""
parsers/missouri.py — Transform Missouri Ethics Commission (MEC) raw exports into
the 5 normalized relations.

Input:  data/Missouri/raw/
  committees_active.xls, committees_terminated.xls
      — CFSearch.aspx Committee Type tab exports (MECID, Committee, Candidate,
        Treasurer, Deputy Treasurer, Committee Type, Committee Status). Bulk list,
        but carries no party or address.
  committee_detail.csv
      — CommInfo.aspx?MECID=X detail sweep. One row per committee: address, phone,
        committee type/status/term date, candidate name/address/phone, PARTY (only
        source for this field), treasurer name/address/phone.
  election_history.csv
      — one row per (committee, election) the committee's candidate ran in, from
        the same CommInfo.aspx sweep. Empty for non-candidate committees (PACs).
  contributions/{mecid}_{year}.xls
      — CF12_ContrExpend.aspx Contributor > Advanced Search export.
  expenditures/{mecid}_{year}.xls
      — CF12_ContrExpend.aspx Expenditure > Advanced Search export. A committee's
        OWN ordinary spending — carries no supported/opposed-candidate field.
  independent_expenditures_{year}.xls
      — CF_SearchDirExp.aspx "Committee Expenditures for Candidates" export, one
        file per Report Year (statewide, NOT per-committee — MEC exposes a real
        bulk export here, unlike the per-committee/year sweep above). MEC's
        actual independent-expenditure report: money a committee spends
        directly to a vendor to support or oppose a specific candidate, WITHOUT
        that candidate's coordination. Folded into expenditures.csv.gz rather
        than a separate output file — see the independent-expenditures section
        below and columns.py's affiliated_candidate_name/support_oppose fields,
        which exist for exactly this and are otherwise blank across every state.

Output: data/Missouri/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz (empty — no structured loan/debt source
  found on the MEC site; see Data Notes in docs/states/missouri.md)

Notes
─────
  • All exports are HTML tables saved with a .xls extension/content-type, not real
    binary workbooks — read with BeautifulSoup (scrapers/missouri.py's
    _read_xls_table), matching what the scraper already used to enumerate MECIDs.
  • The raw contributions export repeats the header "Committee" twice (filing
    committee, and the Contributor-Committee transfer flag) — the scraper's
    _dedupe_headers renames the second one "Committee.1", matching
    pandas.read_html's convention. Confirmed empirically against a real 2,801-row
    export (see docs/states/missouri.md).
  • Amounts arrive as "$1,500.00"; dates as MM/DD/YYYY.
  • person_id model: "person" — a Missouri candidate committee's MECID persists
    across election cycles AND across different offices run for (confirmed on
    Mike Kehoe's C091155: same MECID for State Senate District 6 in 2010/2014,
    Lieutenant Governor in 2020, and Governor in 2024/2028) — so state_filer_id is
    already a stable person-level key here, unlike states that re-register a new
    committee ID each cycle.
  • candidate_first/candidate_last are a naive first/last-token split of
    candidate_name — MEC does not expose them as separate fields.
  • office/district/election_year on candidates.csv come from election_history.csv
    (one row per election actually run in), not from committee_detail.csv, which
    only carries the committee-level fields (name/type/status/address/party).
  • Contributions/expenditures rows carry election_year from the search year filed
    under, not from a matched election — MEC's per-transaction records don't
    themselves indicate which specific election they relate to.
"""

import csv
import gzip
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
from src.pipeline.scrapers.missouri import _read_xls_table
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR      = PROJECT_ROOT / "data" / "Missouri" / "raw"
CONTRIB_DIR  = RAW_DIR / "contributions"
EXPEND_DIR   = RAW_DIR / "expenditures"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Missouri" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "MO"
MAX_VALID_YEAR = date.today().year + 2


# ============================== helpers ===============================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """'$1,500.00' → '1500.00', '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """MM/DD/YYYY → YYYY-MM-DD, '' on failure or implausible year."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def year_from_date(val: str) -> str:
    iso = parse_date(val)
    return iso[:4] if iso else ""


def amended_flag(report: str) -> str:
    """MEC has no dedicated Amended column — inferred from 'AMENDED' appearing in
    the free-text Report name (e.g. 'AMENDED 30 Day After Primary Election...')."""
    return "1" if "AMENDED" in (report or "").upper() else "0"


_STREET_START_RE = re.compile(r"\b(?:\d+\b|P\.?\s?O\.?\s*Box\b|RR\s*\d)", re.I)
_TRAILING_CITY_STATE_ZIP_RE = re.compile(r"\s+[A-Za-z .'\-]+\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?\s*$")


def split_candidate_name_address(combined: str) -> str:
    """CF_SearchDirExp.aspx's 'Candidates Name and Address' column arrives as a
    single concatenated string (name, street, city/state/zip run together with
    no reliable delimiter between them — confirmed against a live sample: e.g.
    'JT Holman 1103 E BRIGGS DR   Macon MO 63552'). Only the name is needed —
    EXPENDITURES has no candidate-address columns, only affiliated_candidate_name
    — so this extracts just that, best-effort: splits at the first token that
    looks like a street start (a digit run, or a P.O. Box marker); falls back to
    stripping a trailing 'City ST ZIP' chunk if no street-start marker is found,
    or to the whole string if neither pattern matches (better than dropping the
    row's candidate name entirely). Known limitation: a candidate name
    containing a digit, or an address with no leading number, will split wrong."""
    s = clean(combined)
    if not s:
        return ""
    m = _STREET_START_RE.search(s)
    if m and m.start() > 0:
        return s[:m.start()].strip(" ,")
    m2 = _TRAILING_CITY_STATE_ZIP_RE.search(s)
    if m2:
        return s[:m2.start()].strip(" ,")
    return s


def split_name(full: str) -> tuple[str, str]:
    """Naive first/last split — MEC only exposes a single combined candidate_name
    field, not separate first/last."""
    tokens = clean(full).split()
    if not tokens:
        return "", ""
    if len(tokens) == 1:
        return tokens[0], ""
    return tokens[0], tokens[-1]


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ================================ run =================================

def run():
    log = get_logger("missouri", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        # ── Load committee_detail.csv registry (Phase B output) ─────────
        # Keyed by MECID — the enrichment source for party, address, treasurer,
        # and (via election_history.csv) candidate office/district/election_year.
        detail_path = RAW_DIR / "committee_detail.csv"
        cmte_registry: dict[str, dict] = {}
        if detail_path.exists():
            with open(detail_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    mecid = clean(row.get("mecid", ""))
                    if mecid:
                        cmte_registry[mecid] = row
            log.registry_loaded(detail_path.name, len(cmte_registry), relation="committees")

        # ── committees.csv.gz ────────────────────────────────────────────
        if cmte_registry:
            for row_num, (mecid, row) in enumerate(cmte_registry.items(), start=2):
                status = clean(row.get("committee_status", ""))
                cmte_w.writerow({
                    "state":                     STATE,
                    "state_filer_id":            mecid,
                    "committee_name":            clean(row.get("committee_name", "")),
                    "committee_type":            clean(row.get("committee_type", "")),
                    "election_year":             "",  # MO committees persist across cycles — sparse by design
                    "candidate_name":            clean(row.get("candidate_name", "")),
                    "treasurer_name":            clean(row.get("treasurer_name", "")),
                    "city":                      clean(row.get("city", "")),
                    "zip":                       clean(row.get("zip", "")),
                    "active":                    "1" if status == "Active" else ("0" if status else ""),
                    "affiliated_candidate_name": "",
                    "support_oppose":            "",
                    "raw_file":                  detail_path.name,
                    "row_num":                   row_num,
                })
                committees_written += 1
        else:
            # Fall back to the bulk committee-list exports (no party/address/
            # treasurer enrichment, but better than an empty table) if the
            # committee-detail sweep (Phase B) hasn't been run yet.
            for stem in ("committees_active.xls", "committees_terminated.xls"):
                path = RAW_DIR / stem
                if not path.exists():
                    continue
                rows = _read_xls_table(path)
                for row_num, row in enumerate(rows, start=2):
                    status = clean(row.get("Committee Status", ""))
                    cmte_w.writerow({
                        "state":                     STATE,
                        "state_filer_id":            clean(row.get("MECID", "")),
                        "committee_name":            clean(row.get("Committee", "")),
                        "committee_type":            clean(row.get("Committee Type", "")),
                        "election_year":             "",
                        "candidate_name":            clean(row.get("Candidate", "")),
                        "treasurer_name":            clean(row.get("Treasurer", "")),
                        "city":                      "",
                        "zip":                       "",
                        "active":                    "1" if status == "Active" else ("0" if status else ""),
                        "affiliated_candidate_name": "",
                        "support_oppose":            "",
                        "raw_file":                  path.name,
                        "row_num":                   row_num,
                    })
                    committees_written += 1
                log.file_parsed(path.name, "committees", len(rows))

        # ── candidates.csv.gz — one row per (committee, election) ──────
        elec_path = RAW_DIR / "election_history.csv"
        if elec_path.exists() and cmte_registry:
            ft = time.perf_counter()
            count = skipped = 0
            with open(elec_path, newline="", encoding="utf-8") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    mecid = clean(row.get("mecid", ""))
                    cmte  = cmte_registry.get(mecid)
                    if not cmte:
                        skipped += 1
                        continue

                    cand_name = clean(cmte.get("candidate_name", ""))
                    if not cand_name:
                        skipped += 1
                        continue

                    first, last = split_name(cand_name)

                    cand_w.writerow({
                        "state":            STATE,
                        "candidate_name":   cand_name,
                        "candidate_first":  first,
                        "candidate_last":   last,
                        "office":           clean(row.get("office", "")),
                        "canonical_office": "",
                        "district":         clean(row.get("district", "")),
                        "jurisdiction":     "",
                        "party":            clean(cmte.get("party", "")),
                        "party_source":     "",
                        "match_confidence": "",
                        "election_year":    year_from_date(row.get("election_date", "")),
                        "incumbent":        "",
                        "state_filer_id":   mecid,
                        "raw_file":         elec_path.name,
                        "row_num":          row_num,
                    })
                    count += 1
                    candidates_written += 1

            log.file_parsed(elec_path.name, "candidates", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=elec_path.stat().st_size)

        # ── contributions.csv.gz ─────────────────────────────────────────
        # Filenames used to be "{mecid}_{year}.xls" (one file per committee per
        # year, from the old per-committee sweep, retired in favor of the
        # statewide date+amount-chunked sweep -- see scraper module
        # docstring). That old per-committee sweep is gone from the scraper
        # entirely now and its raw files have been deleted, so there's
        # nothing left that could ever produce that filename shape again --
        # the mecid-from-filename/year-from-filename fallback this loop used
        # to carry for it has been removed as genuinely dead code, not just
        # unused. MECID and election_year are read per-row unconditionally:
        # MECID is a real column on every exported table regardless of
        # sweep shape, and election_year comes from each row's own
        # Contribution Date, which is more robust than any filename anyway
        # (e.g. a date-range chunk can straddle a year boundary).
        for path in sorted(CONTRIB_DIR.glob("*.xls")):
            ft = time.perf_counter()
            rows = _read_xls_table(path)
            count = skipped = 0
            for row_num, row in enumerate(rows, start=2):
                amount = parse_amount(row.get("Contribution Amount", ""))
                if not amount:
                    skipped += 1
                    continue

                mecid = clean(row.get("MECID", ""))
                file_year = year_from_date(row.get("Contribution Date", ""))
                cmte  = cmte_registry.get(mecid, {})

                is_transfer = clean(row.get("Committee.1", "")).lower() == "yes"
                company     = clean(row.get("Contributor-Company", ""))
                last        = clean(row.get("Contributor-Last Name", ""))
                first       = clean(row.get("Contributor-First Name", ""))

                if is_transfer:
                    contributor_name = clean(row.get("Contributor-Committee", ""))
                    contributor_type = "Committee"
                elif company:
                    contributor_name = company
                    contributor_type = "Business"
                else:
                    contributor_name = " ".join(p for p in (first, last) if p)
                    contributor_type = "Individual" if contributor_name else ""

                cont_w.writerow({
                    "state":             STATE,
                    "committee_name":    clean(row.get("Committee", "")),
                    "amount":            amount,
                    "date":              parse_date(row.get("Contribution Date", "")),
                    "transaction_type":  clean(row.get("Monetary/In-Kind", "")),
                    "contributor_name":  contributor_name,
                    "contributor_type":  contributor_type,
                    "contributor_city":  clean(row.get("City", "")),
                    "contributor_state": clean(row.get("State", "")),
                    "contributor_zip":   clean(row.get("Zip", "")),
                    "employer":          clean(row.get("Employer", "")),
                    "occupation":        clean(row.get("Occupation", "")),
                    "candidate_name":    clean(cmte.get("candidate_name", "")),
                    "office":            "",
                    "election_year":     file_year,
                    "amended":           amended_flag(row.get("Report", "")),
                    "filing_id":         clean(row.get("Report", "")),
                    "raw_file":          path.name,
                    "row_num":           row_num,
                })
                count += 1

            log.file_parsed(path.name, "contributions", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += count

        # ── expenditures.csv.gz ──────────────────────────────────────────
        # See the matching comment above CONTRIB_DIR's loop -- same retired
        # filename scheme, same per-row-only MECID/election_year derivation.
        for path in sorted(EXPEND_DIR.glob("*.xls")):
            ft = time.perf_counter()
            rows = _read_xls_table(path)
            count = skipped = 0
            for row_num, row in enumerate(rows, start=2):
                amount = parse_amount(row.get("Expenditure Amount", ""))
                if not amount:
                    skipped += 1
                    continue

                mecid = clean(row.get("MECID", ""))
                file_year = year_from_date(row.get("Expenditure Date", ""))
                cmte  = cmte_registry.get(mecid, {})

                company = clean(row.get("Expenditure-Company", ""))
                last    = clean(row.get("Expenditure-Last Name", ""))
                first   = clean(row.get("Expenditure-First Name", ""))
                payee_name = company or " ".join(p for p in (first, last) if p)

                expn_w.writerow({
                    "state":                STATE,
                    "committee_name":       clean(row.get("Committee Name", "")),
                    "amount":               amount,
                    "date":                 parse_date(row.get("Expenditure Date", "")),
                    "transaction_type":     clean(row.get("Expenditure Type", "")),
                    "payee_name":           payee_name,
                    "purpose":              clean(row.get("Expenditure Purpose", "")),
                    "category":             "",
                    "payee_city":           clean(row.get("Expenditure-City", "")),
                    "payee_state":          clean(row.get("Expenditure-State", "")),
                    "payee_zip":            clean(row.get("Expenditure-Zip", "")),
                    "candidate_name":       clean(cmte.get("candidate_name", "")),
                    "office":               "",
                    "election_year":        file_year,
                    "affiliated_candidate_name": "",
                    "support_oppose":       "",
                    "amended":              amended_flag(row.get("Report", "")),
                    "filing_id":            clean(row.get("Report", "")),
                    "raw_file":             path.name,
                    "row_num":              row_num,
                })
                count += 1

            log.file_parsed(path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count

        # ── independent expenditures → folded into expenditures.csv.gz ──
        # CF_SearchDirExp.aspx's "Committee Expenditures for Candidates" report
        # (Phase E of the scraper) is a genuinely different report from the
        # per-committee Expenditure tab above: money a committee spends
        # directly to a vendor to support/oppose a specific candidate, without
        # that candidate's coordination. It has no payee/purpose fields (MEC
        # doesn't disclose the vendor on this report) but does carry exactly
        # what affiliated_candidate_name/support_oppose were built for — see
        # columns.py, and the module docstring above for why this lands in
        # expenditures.csv.gz rather than a separate output table: every other
        # part of the pipeline (tabulate.py, aggregate.py, the combined
        # DuckDB, the API/frontend) already keys off the fixed table list in
        # columns.py, and these two columns exist specifically for this.
        for path in sorted(RAW_DIR.glob("independent_expenditures_*.xls")):
            m = re.match(r"^independent_expenditures_(\d{4})\.xls$", path.name)
            file_year = m.group(1) if m else ""

            ft = time.perf_counter()
            rows = _read_xls_table(path)
            count = skipped = 0
            for row_num, row in enumerate(rows, start=2):
                amount = parse_amount(row.get("Amount", ""))
                if not amount:
                    skipped += 1
                    continue

                stance = clean(row.get("Support/Oppose", "")).lower()
                if stance.startswith("supp"):
                    support_oppose = "S"
                elif stance.startswith("opp"):
                    support_oppose = "O"
                else:
                    support_oppose = ""

                expn_w.writerow({
                    "state":                     STATE,
                    "committee_name":            clean(row.get("Reporting Committee", "")),
                    "amount":                    amount,
                    "date":                      parse_date(row.get("Date", "")),
                    "transaction_type":          "Independent Expenditure",
                    "payee_name":                "",  # not disclosed on this report
                    "purpose":                   "",
                    "category":                  "",
                    "payee_city":                "",
                    "payee_state":               "",
                    "payee_zip":                 "",
                    # never the filing committee's OWN candidate — an IE
                    # committee spending against a candidate never IS that
                    # candidate. See candidate_name vs affiliated_candidate_name
                    # distinction in columns.py.
                    "candidate_name":            "",
                    "office":                    clean(row.get("Office Sought", "")),
                    "election_year":             file_year,
                    "affiliated_candidate_name": split_candidate_name_address(
                                                     row.get("Candidates Name and Address", "")),
                    "support_oppose":            support_oppose,
                    "amended":                   amended_flag(row.get("Report", "")),
                    "filing_id":                 clean(row.get("Report", "")),
                    "raw_file":                  path.name,
                    "row_num":                   row_num,
                })
                count += 1

            log.file_parsed(path.name, "expenditures", count, skipped,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_expenditures += count

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # A Missouri candidate committee's MECID persists across cycles and even
        # across different offices — see module docstring — so it's already a
        # stable person-level key.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   0,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written,
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
