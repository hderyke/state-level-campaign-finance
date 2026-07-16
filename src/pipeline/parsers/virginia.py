"""
parsers/virginia.py — Parse Virginia Dept. of Elections campaign finance CSVs.

Reads data/Virginia/raw/{period}/*.csv (one subdirectory per period —
"1999".."2011" yearly, "2012_01".."present" monthly — see
scrapers/virginia.py) and writes the five normalized tables to
data/Virginia/cleaned/.

Source file roles (confirmed against live downloads, 2012+ era):
  Report.csv    — one row per filed report: committee identity
                   (CommitteeCode/CommitteeName/CommitteeType), the
                   candidate it supports (CandidateName/OfficeSought/
                   District/Party), and filing metadata (ReportId,
                   IsAmendment, IsFinalReport, ElectionCycle). This is the
                   join key (ReportId) every Schedule file's own ReportId
                   column points back to.
  ScheduleA.csv — itemized contributions (> $100 individual threshold).
  ScheduleB.csv — itemized in-kind contributions.
  ScheduleC.csv — other receipts (refunds, interest, returned checks, etc).
  ScheduleD.csv — itemized expenditures.
  ScheduleE.csv — loans (both received and repaid — see TransactionType).
  ScheduleF.csv — unpaid obligations/debts.
  ScheduleG.csv — per-report SUMMARY totals (contribution/expenditure/loan
                   totals for the report) — not itemized transactions, no
                   ReportId-level detail beyond what's already in A-F.
                   Not parsed into any output table.
  ScheduleH.csv — per-report summary of receipts & disbursements (running
                   balances) — same as G, not itemized, not parsed.
  ScheduleI.csv — disposition of surplus/remaining funds on a final report
                   — folded into expenditures.csv.gz (it's money leaving
                   the committee, same as Schedule D).

Known "legacy era" (1999-2011) gap: VA's yearly directories from this era
do not include a Report.csv (confirmed absent from every 1999-2011
directory listing checked) — only the Schedule*.csv / Schedule*_PAC.csv
files. Every Schedule row still carries a ReportId column, but there is no
file anywhere in this era's raw data that maps ReportId -> committee name/
candidate/office. Rather than guess, rows whose ReportId has no Report.csv
match anywhere in the full raw/ tree get committee_name set to a clearly
flagged placeholder ("UNKNOWN (VA pre-2012, ReportId <id>)") instead of
being silently dropped or given a wrong name — see LEGACY_FALLBACK_NOTE and
docs/states/virginia.md's Data Notes for the count of affected rows on the
last full run.

person_id model: "committee" — VA's CommitteeCode (e.g. "CC-15-00531") is
assigned per committee *registration*, and candidates who run in multiple
cycles typically re-register a new committee with a new code each time
(the "15" in "CC-15-00531" tracks the registration year). assign_person_ids
groups by (state, candidate_name, office, district) and picks the earliest
CommitteeCode as the person's canonical ID, same pattern as Maryland/
Pennsylvania.
"""

import csv
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

# ================================ paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Virginia" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Virginia" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "VA"
MAX_VALID_YEAR = date.today().year + 2

# Matches a period directory name: "1999" (yearly era) or "2012_03" (monthly era).
PERIOD_RE = re.compile(r"^((?:19|20)\d{2})(?:_(\d{2}))?$")

# Matches any of this era's Schedule filenames, with or without the legacy
# "_PAC" suffix (e.g. "ScheduleA_PAC.csv", "ScheduleD.csv") — captures the
# schedule letter so both eras dispatch through the same handler.
SCHEDULE_RE = re.compile(r"^Schedule([A-I])(?:_PAC)?\.csv$", re.IGNORECASE)

LEGACY_FALLBACK_NOTE = "see docs/states/virginia.md Data Notes"


# ============================== helpers =================================

def clean(val) -> str:
    return (val or "").strip()


def parse_amount(val) -> str:
    """'50.00', '.00', '-534.86' -> plain numeric string. '' on failure."""
    v = clean(val).replace("$", "").replace(",", "")
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
    """'MM/DD/YYYY' or 'YYYY-MM-DD[ HH:MM:SS[.ffffff]]' -> 'YYYY-MM-DD'.
    '' on failure or an implausible year (VA's raw exports are otherwise
    unvalidated free-text/system-generated date fields)."""
    v = clean(val)
    if not v:
        return ""
    v = v.split(".")[0]   # drop fractional seconds if present
    for fmt in ("%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def full_name(first: str = "", middle: str = "", last: str = "", suffix: str = "") -> str:
    """Natural-order 'First Middle Last Suffix' — matches how VA/COMET
    itself displays names, unlike Maryland's 'Last, First' convention.
    For non-individual rows, First/Middle/Suffix are blank and this just
    returns the company/committee name in LastOrCompanyName."""
    parts = [clean(first), clean(middle), clean(last), clean(suffix)]
    return re.sub(r"\s+", " ", " ".join(p for p in parts if p)).strip()


_NAME_PREFIXES = {"mr", "mrs", "ms", "mx", "dr", "hon", "honorable", "the honorable"}
_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def split_candidate_name(name: str) -> tuple[str, str]:
    """Best-effort first/last split of VA's free-text CandidateName field
    (e.g. 'Mrs. Sara Johnson Ward'). VA does not provide separate name-part
    columns for candidates the way it does for contributors/payees, so this
    strips common prefixes/suffixes and uses the first remaining token as
    candidate_first and the last remaining token as candidate_last; any
    middle name(s) are dropped from both parts (still preserved verbatim in
    candidate_name itself). Not exact for compound surnames — flagged as a
    known limitation in docs/states/virginia.md."""
    tokens = [t for t in re.split(r"\s+", clean(name)) if t]
    tokens = [t for t in tokens if t.strip(".").lower() not in _NAME_PREFIXES]
    tokens = [t for t in tokens if t.strip(".").lower() not in _NAME_SUFFIXES]
    if not tokens:
        return "", ""
    if len(tokens) == 1:
        return tokens[0], ""
    return tokens[0], tokens[-1]


def bool_flag(val) -> str:
    """VA's IsAmendment (and other Is*/boolean-ish) fields arrive as the
    literal strings 'True'/'False' — normalize to '1'/'0' (validate.py's
    check_bool_int expects 0/1/empty, and this matches the convention used
    by most other states' parsers, e.g. Alabama's yn_to_int, Idaho's bool01).
    Empty/unrecognized values pass through as '' rather than being guessed."""
    v = clean(val).lower()
    if v in ("true", "y", "yes", "1"):
        return "1"
    if v in ("false", "n", "no", "0"):
        return "0"
    return ""


def derive_jurisdiction(row: dict) -> str:
    parts = []
    if clean(row.get("IsStateWide", "")).lower() == "true":
        parts.append("Statewide")
    if clean(row.get("IsGeneralAssembly", "")).lower() == "true":
        parts.append("General Assembly")
    if clean(row.get("IsLocal", "")).lower() == "true":
        parts.append("Local")
    return "; ".join(parts)


def derive_election_year(row: dict) -> str:
    """VA's ElectionCycle looks like '11/2015' (month/year of the election
    the cycle runs through) — prefer its year over ReportYear (the calendar
    year the report itself was filed), since a single election cycle spans
    multiple ReportYears. Falls back to ReportYear if ElectionCycle is blank
    or unparseable."""
    m = re.search(r"(\d{4})", clean(row.get("ElectionCycle", "")))
    if m:
        return m.group(1)
    return clean(row.get("ReportYear", ""))


def period_sort_key(period_name: str) -> tuple[int, int]:
    m = PERIOD_RE.match(period_name)
    if not m:
        return (0, 0)
    year = int(m.group(1))
    month = int(m.group(2)) if m.group(2) else 0
    return (year, month)


def list_periods() -> list[Path]:
    if not RAW_DIR.exists():
        return []
    dirs = [p for p in RAW_DIR.iterdir() if p.is_dir()]
    return sorted(dirs, key=lambda p: period_sort_key(p.name))


def open_reader(path: Path):
    fh = open(path, encoding="utf-8-sig", errors="replace", newline="")
    return fh, csv.DictReader(fh)


def open_writer(filename: str, fieldnames: list):
    import gzip
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def raw_file_label(path: Path) -> str:
    """'{period}/{filename}' — used as the raw_file provenance value so
    every output row can be traced back to the exact downloaded file."""
    return f"{path.parent.name}/{path.name}"


# ================================= run =================================

def run():
    log = get_logger("virginia", "parse")
    t0 = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    committees_written   = 0
    candidates_written   = 0
    legacy_fallback_rows = 0
    file_handles         = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        periods = list_periods()
        if not periods:
            log.warning(f"  No period directories found under {RAW_DIR} — "
                        f"run scrapers/virginia.py first.")

        # ── Pass 1: Report.csv -> report registry + committee/candidate dedup ──
        # Chronological order so "last write wins" reflects each committee's
        # most recently reported name/address/status.
        report_registry: dict[str, dict] = {}   # ReportId -> report metadata
        committees_seen: dict[str, dict] = {}   # CommitteeCode -> latest committee row
        candidates_seen: dict[str, dict] = {}   # CommitteeCode -> latest candidate row
        final_committees: set[str] = set()

        for period_dir in periods:
            report_path = period_dir / "Report.csv"
            if not report_path.exists():
                continue   # legacy era (1999-2011) — no Report.csv published

            ft = time.perf_counter()
            count = 0
            raw_fh, reader = open_reader(report_path)
            try:
                for row_num, row in enumerate(reader, start=2):
                    report_id = clean(row.get("ReportId", ""))
                    if not report_id:
                        continue

                    fid           = clean(row.get("CommitteeCode", ""))
                    cmte_name     = clean(row.get("CommitteeName", ""))
                    cmte_type     = clean(row.get("CommitteeType", ""))
                    cand_name     = clean(row.get("CandidateName", ""))
                    office        = clean(row.get("OfficeSought", ""))
                    district      = clean(row.get("District", ""))
                    party         = clean(row.get("Party", ""))
                    city          = clean(row.get("City", ""))
                    zipcode       = clean(row.get("ZipCode", ""))
                    is_final      = clean(row.get("IsFinalReport", "")).lower() == "true"
                    is_amendment  = bool_flag(row.get("IsAmendment", ""))
                    jurisdiction  = derive_jurisdiction(row)
                    election_year = derive_election_year(row)

                    entry = {
                        "committee_name": cmte_name,
                        "committee_type": cmte_type,
                        "candidate_name": cand_name,
                        "office":         office,
                        "district":       district,
                        "party":          party,
                        "jurisdiction":   jurisdiction,
                        "election_year":  election_year,
                        "amended":        is_amendment,
                        "state_filer_id": fid,
                    }
                    report_registry[report_id] = entry

                    if fid:
                        if is_final:
                            final_committees.add(fid)

                        committees_seen[fid] = {
                            "committee_name": cmte_name,
                            "committee_type": cmte_type,
                            "election_year":  election_year,
                            "candidate_name": cand_name,
                            "treasurer_name": "",   # not present in Report.csv
                            "city":           city,
                            "zip":            zipcode,
                            "raw_file":       raw_file_label(report_path),
                            "row_num":        row_num,
                        }

                        # Only (re)write candidate info when this row actually
                        # names one — don't blank out a known candidate with a
                        # later, sparser row for the same committee.
                        if cmte_type.lower().startswith("candidate") and cand_name:
                            first, last = split_candidate_name(cand_name)
                            candidates_seen[fid] = {
                                "candidate_name":  cand_name,
                                "candidate_first": first,
                                "candidate_last":  last,
                                "office":          office,
                                "district":        district,
                                "jurisdiction":    jurisdiction,
                                "party":           party,
                                "election_year":   election_year,
                                "incumbent":       "",   # not available in the source
                                "raw_file":        raw_file_label(report_path),
                                "row_num":         row_num,
                            }
                    count += 1
            finally:
                raw_fh.close()

            log.file_parsed(report_path.name, "report_registry", count,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=report_path.stat().st_size)

        log.registry_loaded("Report.csv (all periods)", len(report_registry),
                            relation="report_registry")

        for fid, entry in committees_seen.items():
            cmte_w.writerow({
                "state":          STATE,
                "state_filer_id": fid,
                "active":         "0" if fid in final_committees else "1",
                **entry,
            })
            committees_written += 1

        for fid, entry in candidates_seen.items():
            cand_w.writerow({
                "state":          STATE,
                "state_filer_id": fid,
                **entry,
            })
            candidates_written += 1

        log.info(f"  committees: {committees_written:,}  candidates: {candidates_written:,}")

        # ── Pass 2: Schedule files -> contributions / expenditures / loans_debts ──
        def registry_lookup(report_id: str, period_dir: Path) -> dict:
            nonlocal legacy_fallback_rows
            entry = report_registry.get(report_id)
            if entry is not None:
                return entry
            legacy_fallback_rows += 1
            return {
                "committee_name": f"UNKNOWN (VA pre-2012, ReportId {report_id})",
                "committee_type": "",
                "candidate_name": "",
                "office":         "",
                "district":       "",
                "party":          "",
                "jurisdiction":   "",
                "election_year":  str(period_sort_key(period_dir.name)[0]) if period_dir.name else "",
                "amended":        "",
                "state_filer_id": "",
            }

        for period_dir in periods:
            for path in sorted(period_dir.iterdir()):
                if not path.is_file():
                    continue
                m = SCHEDULE_RE.match(path.name)
                if not m:
                    continue   # Report.csv (already handled) or an unexpected file
                letter = m.group(1).upper()
                if letter in ("G", "H"):
                    continue   # per-report summary totals, not itemized rows — see module docstring

                handler = _SCHEDULE_HANDLERS.get(letter)
                if handler is None:
                    continue

                ft = time.perf_counter()
                counts = handler(path, registry_lookup, period_dir,
                                 cont_w, expn_w, loan_w)
                log.file_parsed(path.name, counts["relation"], counts["rows"],
                                duration_s=round(time.perf_counter() - ft, 2),
                                bytes=path.stat().st_size)
                total_contributions += counts.get("contributions", 0)
                total_expenditures  += counts.get("expenditures", 0)
                total_loans         += counts.get("loans", 0)

        if legacy_fallback_rows:
            log.warning(f"  {legacy_fallback_rows:,} rows from pre-2012 periods had no "
                        f"matching Report.csv entry — committee_name set to a placeholder "
                        f"({LEGACY_FALLBACK_NOTE})")

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

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
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {total_contributions:,} contributions, "
                 f"{total_expenditures:,} expenditures, {total_loans:,} loans/debts")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written,
                  legacy_fallback_rows=legacy_fallback_rows)

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


# ========================= Schedule handlers ============================
# Each handler reads one Schedule*.csv (or Schedule*_PAC.csv) file, looks up
# its ReportId in the report registry via `lookup`, writes rows into the
# appropriate output writer(s), and returns a small stats dict for logging.

def _schedule_a_or_b(path: Path, lookup, period_dir: Path, cont_w, expn_w, loan_w,
                     kind: str) -> dict:
    """Schedule A (contributions > $100) and Schedule B (in-kind
    contributions) share the same contributor-detail column layout, differing
    only in whether B carries ValuationBasis/ProductOrService."""
    raw_file = raw_file_label(path)
    count = skipped = 0
    raw_fh, reader = open_reader(path)
    try:
        for row_num, row in enumerate(reader, start=2):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            report_id = clean(row.get("ReportId", ""))
            reg = lookup(report_id, period_dir)
            is_individual = clean(row.get("IsIndividual", "")).lower() == "true"

            if kind == "A":
                tx_type = "Contribution"
            else:
                product = clean(row.get("ProductOrService", ""))
                tx_type = f"In-Kind Contribution ({product})" if product else "In-Kind Contribution"

            cont_w.writerow({
                "state":             STATE,
                "committee_name":    reg["committee_name"],
                "amount":            amount,
                "date":              parse_date(row.get("TransactionDate", "")),
                "transaction_type":  tx_type,
                "contributor_name":  full_name(row.get("FirstName", ""), row.get("MiddleName", ""),
                                               row.get("LastOrCompanyName", "")),
                "contributor_type":  "Individual" if is_individual else "Organization",
                "contributor_city":  clean(row.get("City", "")),
                "contributor_state": clean(row.get("StateCode", "")),
                "contributor_zip":   clean(row.get("ZipCode", "")),
                "employer":          clean(row.get("NameOfEmployer", "")),
                "occupation":        clean(row.get("OccupationOrTypeOfBusiness", "")),
                "candidate_name":    reg["candidate_name"],
                "office":            reg["office"],
                "election_year":     reg["election_year"],
                "amended":           reg["amended"],
                "filing_id":         report_id,
                "raw_file":          raw_file,
                "row_num":           row_num,
            })
            count += 1
    finally:
        raw_fh.close()
    return {"relation": "contributions", "rows": count, "contributions": count}


def _schedule_a(path, lookup, period_dir, cont_w, expn_w, loan_w):
    return _schedule_a_or_b(path, lookup, period_dir, cont_w, expn_w, loan_w, kind="A")


def _schedule_b(path, lookup, period_dir, cont_w, expn_w, loan_w):
    return _schedule_a_or_b(path, lookup, period_dir, cont_w, expn_w, loan_w, kind="B")


def _schedule_c(path: Path, lookup, period_dir: Path, cont_w, expn_w, loan_w) -> dict:
    """Schedule C — other receipts (refunds, interest, returned checks).
    Folded into contributions.csv.gz with transaction_type='Other Receipt:
    {ReceiptType}' — real dollars into the committee's account, just not
    solicited from a contributor (same treatment PA gives its own
    receipt_{year}.txt schedule)."""
    raw_file = raw_file_label(path)
    count = skipped = 0
    raw_fh, reader = open_reader(path)
    try:
        for row_num, row in enumerate(reader, start=2):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            report_id = clean(row.get("ReportId", ""))
            reg = lookup(report_id, period_dir)
            is_individual = clean(row.get("IsIndividual", "")).lower() == "true"
            receipt_type = clean(row.get("ReceiptType", ""))

            cont_w.writerow({
                "state":             STATE,
                "committee_name":    reg["committee_name"],
                "amount":            amount,
                "date":              parse_date(row.get("TransactionDate", "")),
                "transaction_type":  f"Other Receipt: {receipt_type}" if receipt_type else "Other Receipt",
                "contributor_name":  full_name(row.get("FirstName", ""), row.get("MiddleName", ""),
                                               row.get("LastOrCompanyName", "")),
                "contributor_type":  "Individual" if is_individual else "Organization",
                "contributor_city":  clean(row.get("City", "")),
                "contributor_state": clean(row.get("StateCode", "")),
                "contributor_zip":   clean(row.get("ZipCode", "")),
                "employer":          "",
                "occupation":        "",
                "candidate_name":    reg["candidate_name"],
                "office":            reg["office"],
                "election_year":     reg["election_year"],
                "amended":           reg["amended"],
                "filing_id":         report_id,
                "raw_file":          raw_file,
                "row_num":           row_num,
            })
            count += 1
    finally:
        raw_fh.close()
    return {"relation": "contributions", "rows": count, "contributions": count}


def _schedule_d(path: Path, lookup, period_dir: Path, cont_w, expn_w, loan_w) -> dict:
    """Schedule D — itemized expenditures."""
    raw_file = raw_file_label(path)
    count = skipped = 0
    raw_fh, reader = open_reader(path)
    try:
        for row_num, row in enumerate(reader, start=2):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            report_id = clean(row.get("ReportId", ""))
            reg = lookup(report_id, period_dir)

            expn_w.writerow({
                "state":            STATE,
                "committee_name":   reg["committee_name"],
                "amount":           amount,
                "date":             parse_date(row.get("TransactionDate", "")),
                "transaction_type": "Expenditure",
                "payee_name":       full_name(row.get("FirstName", ""), row.get("MiddleName", ""),
                                              row.get("LastOrCompanyName", "")),
                "purpose":          clean(row.get("ItemOrService", "")),
                "category":         "",
                "payee_city":       clean(row.get("City", "")),
                "payee_state":      clean(row.get("StateCode", "")),
                "payee_zip":        clean(row.get("ZipCode", "")),
                "candidate_name":   reg["candidate_name"],
                "office":           reg["office"],
                "election_year":    reg["election_year"],
                "amended":          reg["amended"],
                "filing_id":        report_id,
                "raw_file":         raw_file,
                "row_num":          row_num,
            })
            count += 1
    finally:
        raw_fh.close()
    return {"relation": "expenditures", "rows": count, "expenditures": count}


_LOAN_TRANSACTION_TYPES = {"R": "Loan Received", "P": "Loan Payment"}


def _schedule_e(path: Path, lookup, period_dir: Path, cont_w, expn_w, loan_w) -> dict:
    """Schedule E — loans. Each row is either a loan being received (R) or
    repaid (P) — see _LOAN_TRANSACTION_TYPES. Co-borrower fields (present on
    some rows) are not carried into loans_debts.csv.gz — only the primary
    lender is captured, same simplification other states' parsers make for
    secondary/joint parties (documented limitation, see docs/states/virginia.md)."""
    raw_file = raw_file_label(path)
    count = skipped = 0
    raw_fh, reader = open_reader(path)
    try:
        for row_num, row in enumerate(reader, start=2):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            report_id = clean(row.get("ReportId", ""))
            reg = lookup(report_id, period_dir)
            code = clean(row.get("TransactionType", "")).upper()
            record_type = _LOAN_TRANSACTION_TYPES.get(code, f"Loan ({code})" if code else "Loan")

            loan_w.writerow({
                "state":               STATE,
                "committee_name":      reg["committee_name"],
                "original_amount":     amount,
                "date":                parse_date(row.get("TransactionDate", "")),
                "record_type":         record_type,
                "counterparty_name":   full_name(row.get("LenderFirstName", ""),
                                                 row.get("LenderMiddleName", ""),
                                                 row.get("LenderLastOrCompanyName", "")),
                "counterparty_city":   clean(row.get("LenderCity", "")),
                "counterparty_state":  clean(row.get("LenderState", "")),
                "counterparty_zip":    clean(row.get("LenderZipCode", "")),
                "candidate_name":      reg["candidate_name"],
                "election_year":       reg["election_year"],
                "amended":             reg["amended"],
                "filing_id":           report_id,
                "raw_file":            raw_file,
                "row_num":             row_num,
            })
            count += 1
    finally:
        raw_fh.close()
    return {"relation": "loans_debts", "rows": count, "loans": count}


def _schedule_f(path: Path, lookup, period_dir: Path, cont_w, expn_w, loan_w) -> dict:
    """Schedule F — unpaid obligations/debts (not yet paid as of the report date)."""
    raw_file = raw_file_label(path)
    count = skipped = 0
    raw_fh, reader = open_reader(path)
    try:
        for row_num, row in enumerate(reader, start=2):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            report_id = clean(row.get("ReportId", ""))
            reg = lookup(report_id, period_dir)

            loan_w.writerow({
                "state":               STATE,
                "committee_name":      reg["committee_name"],
                "original_amount":     amount,
                "date":                parse_date(row.get("TransactionDate", "")),
                "record_type":         "Unpaid Obligation",
                "counterparty_name":   full_name(row.get("FirstName", ""), row.get("MiddleName", ""),
                                                 row.get("LastOrCompanyName", "")),
                "counterparty_city":   clean(row.get("City", "")),
                "counterparty_state":  clean(row.get("StateCode", "")),
                "counterparty_zip":    clean(row.get("ZipCode", "")),
                "candidate_name":      reg["candidate_name"],
                "election_year":       reg["election_year"],
                "amended":             reg["amended"],
                "filing_id":           report_id,
                "raw_file":            raw_file,
                "row_num":             row_num,
            })
            count += 1
    finally:
        raw_fh.close()
    return {"relation": "loans_debts", "rows": count, "loans": count}


def _schedule_i(path: Path, lookup, period_dir: Path, cont_w, expn_w, loan_w) -> dict:
    """Schedule I — disposition of surplus/remaining funds on a final report.
    Folded into expenditures.csv.gz — it's money leaving the committee, same
    as Schedule D, just with a TypeOfDisposition instead of ItemOrService."""
    raw_file = raw_file_label(path)
    count = skipped = 0
    raw_fh, reader = open_reader(path)
    try:
        for row_num, row in enumerate(reader, start=2):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            report_id = clean(row.get("ReportId", ""))
            reg = lookup(report_id, period_dir)

            expn_w.writerow({
                "state":            STATE,
                "committee_name":   reg["committee_name"],
                "amount":           amount,
                "date":             parse_date(row.get("TransactionDate", "")),
                "transaction_type": "Disposition of Surplus Funds",
                "payee_name":       full_name(row.get("FirstName", ""), row.get("MiddleName", ""),
                                              row.get("LastOrCompanyName", "")),
                "purpose":          clean(row.get("TypeOfDisposition", "")),
                "category":         "",
                "payee_city":       clean(row.get("City", "")),
                "payee_state":      clean(row.get("StateCode", "")),
                "payee_zip":        clean(row.get("ZipCode", "")),
                "candidate_name":   reg["candidate_name"],
                "office":           reg["office"],
                "election_year":    reg["election_year"],
                "amended":          reg["amended"],
                "filing_id":        report_id,
                "raw_file":         raw_file,
                "row_num":          row_num,
            })
            count += 1
    finally:
        raw_fh.close()
    return {"relation": "expenditures", "rows": count, "expenditures": count}


_SCHEDULE_HANDLERS = {
    "A": _schedule_a,
    "B": _schedule_b,
    "C": _schedule_c,
    "D": _schedule_d,
    "E": _schedule_e,
    "F": _schedule_f,
    "I": _schedule_i,
}


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
