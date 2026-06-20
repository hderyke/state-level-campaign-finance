"""
scrapers/connecticut.py — Download Connecticut SEEC eCRIS campaign finance data.

Two-phase download, both using plain HTTP (no Playwright, no auth):

  1. Bulk transaction CSVs from the SEEC data download page:
       https://seec.ct.gov/portal/ecris/CurPreYears
     Predictable URL pattern:
       {BASE_DOWNLOAD}/{RecordType}{year}{PeriodType}{CommitteeType}.{ext}
     Four file types per year (Receipts + Disbursements × CalendarYear +
     ElectionYear), covering Party/PAC and Candidate/Exploratory committees.
     2010–2021 and 2024+: CSV available directly.
     2022–2023: XLSX only (CSV returns 404); downloaded and converted to CSV
     via openpyxl. Years before 2010 have incomplete coverage (no disbursements
     or ElectionYear files) and are excluded.

  2. Committee history pages scraped by numeric ID:
       https://seec.ct.gov/eCrisReporting/CommitteeHistory.aspx?c={id}
     IDs run from 1 to ~14590 (observed ceiling as of 2026-05). Valid pages
     contain a PanelHistory div; missing IDs return a bare page shell without
     it. Sweep stops after MAX_CONSECUTIVE_BLANK consecutive empty responses.
     Each page gives: committee name, type/subtype, status (ACTIVE or
     TERMINATED), address, chairperson name, and treasurer name from the most
     recent registration (amendments are shown newest-first).
"""

import csv
import io
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

from config import USER_AGENT

# =============================== paths ================================
RAW_DIR                = PROJECT_ROOT / "data" / "Connecticut" / "raw"
MANIFEST               = PROJECT_ROOT / "data" / "Connecticut" / "manifest.csv"
COMMITTEE_HISTORY_PATH = RAW_DIR / "committee_history.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]

# ========================= transaction constants ======================
BASE_DOWNLOAD = (
    "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles"
)

# First year with a complete set of all four file types
START_YEAR = 2010

# (source_record_type, source_period_type, source_committee_type, relation_type)
# relation_type is the stable key used in the manifest and output filenames.
FILE_TYPES = [
    ("Receipts",      "CalendarYear",  "PartyPACCommittees",
     "receipts_calendar_partypac"),
    ("Receipts",      "ElectionYear",  "CandidateExploratoryCommittees",
     "receipts_election_candidateexploratory"),
    ("Disbursements", "CalendarYear",  "PartyPACCommittees",
     "disbursements_calendar_partypac"),
    ("Disbursements", "ElectionYear",  "CandidateExploratoryCommittees",
     "disbursements_election_candidateexploratory"),
]

# ========================== entity constants ==========================
COMMITTEE_HISTORY_URL = (
    "https://seec.ct.gov/eCrisReporting/CommitteeHistory.aspx?c={id}"
)

# 14590 was the observed max ID as of 2026-05; +100 cushion for new registrations.
COMMITTEE_ID_CEILING  = 14690
# Set well above the largest known gap in the CT committee ID space. The gap
# between the ~6,000s and ~10,000s is the main offender — 1,500 is safely above it.
MAX_CONSECUTIVE_BLANK = 1500
# On incremental runs, start this many IDs before the highest already-scraped ID
# so we catch any recently added committees near the current ceiling without
# re-probing thousands of already-done low IDs.
INCREMENTAL_CUSHION   = 200

COMMITTEE_HISTORY_COLS = [
    "committee_id",
    "committee_name",
    "committee_type",
    "committee_subtype",
    "status",
    "address",
    "city",
    "state",
    "zip",
    "candidate_name",    # populated for Candidate and Exploratory committees
    "chairperson_name",  # populated for Party and Political committees
    "treasurer_name",
    "downloaded_at",
]

# ========================== manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    """Return set of (relation_type, year) already in the manifest."""
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="") as f:
        return {(r["relation_type"], r["year"]) for r in csv.DictReader(f)}


def upsert_manifest(record: dict) -> None:
    """Add or overwrite the manifest entry matching (relation_type, year)."""
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            existing = [
                r for r in csv.DictReader(f)
                if not (r["relation_type"] == record["relation_type"]
                        and r["year"] == record["year"])
            ]
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)


def strip_manifest(keep_fn) -> None:
    """Rewrite the manifest keeping only rows where keep_fn(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


# ========================= transaction download =======================

def _xlsx_to_csv(xlsx_bytes: bytes) -> str:
    """Convert raw XLSX bytes to a UTF-8 CSV string via openpyxl.
    Used for 2022-2023 files that are only published as XLSX."""
    import openpyxl  # ImportError surfaces as a loud warning in _fetch_file
    wb = openpyxl.load_workbook(
        io.BytesIO(xlsx_bytes), read_only=True, data_only=True
    )
    ws = wb.active
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in ws.iter_rows(values_only=True):
        writer.writerow(["" if v is None else str(v) for v in row])
    wb.close()
    return buf.getvalue()


def _fetch_file(session: requests.Session, record_type: str, period_type: str,
                committee_type: str, year: int) -> tuple[str, int, bytes] | None:
    """Try CSV first; fall back to XLSX for years that only provide XLSX (2022-2023).
    Returns (extension_used, row_count, utf8_content_bytes) or None on failure."""
    base = f"{BASE_DOWNLOAD}/{record_type}{year}{period_type}{committee_type}"

    for url, ext in [(f"{base}.csv", "csv"), (f"{base}.xlsx", "xlsx")]:
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
        except requests.RequestException:
            continue

        if ext == "csv":
            raw = resp.content
            # strip UTF-8 BOM if present
            if raw[:3] == b"\xef\xbb\xbf":
                raw = raw[3:]
            text = raw.decode("utf-8", errors="replace")
        else:
            try:
                text = _xlsx_to_csv(resp.content)
            except ImportError:
                # openpyxl not installed — skip XLSX fallback and surface a
                # clear message so the user knows what to install
                print(
                    "[!] openpyxl is required for XLSX conversion "
                    "(years 2022-2023): pip install openpyxl",
                    file=sys.stderr,
                )
                return None

        row_count = max(0, text.count("\n") - 1)
        return ext, row_count, text.encode("utf-8")

    return None


def download_transactions(log, session: requests.Session,
                          force: bool = False,
                          current_only: bool = False,
                          start_year: int | None = None,
                          end_year: int | None = None,
                          contributions: bool = False,
                          expenditures: bool = False) -> tuple[int, int]:
    """Download bulk transaction CSVs.

    force=True:        re-download everything regardless of manifest.
    current_only=True: only fetch the current year (used by update-transactions).
    start_year/end_year: restrict to this year range; re-downloads all in-range years.
    contributions:     receipts only (maps to CT 'Receipts' record type).
    expenditures:      disbursements only (maps to CT 'Disbursements' record type).
    default:           skip past years in manifest; always re-fetch current year.
    """
    current_year        = str(datetime.today().year)
    year_range_explicit = start_year is not None or end_year is not None
    range_start         = start_year if start_year is not None else START_YEAR
    done                = set() if force else load_manifest()
    ok = err = 0

    # Filter FILE_TYPES by contributions/expenditures flags.
    # CT uses "Receipts" for contributions and "Disbursements" for expenditures.
    if contributions and not expenditures:
        active_types = [ft for ft in FILE_TYPES if ft[0] == "Receipts"]
    elif expenditures and not contributions:
        active_types = [ft for ft in FILE_TYPES if ft[0] == "Disbursements"]
    else:
        active_types = FILE_TYPES

    for record_type, period_type, committee_type, relation_type in active_types:
        log.info(f"\n{relation_type}:")
        for year in range(range_start, int(current_year) + 1):
            year_str = str(year)
            if end_year is not None and year > end_year:
                continue
            key      = (relation_type, year_str)
            stem     = f"{relation_type}_{year_str}"

            # Incremental update: only the current year matters
            if current_only and year_str != current_year:
                log.file_download_skip(filename=f"{stem}.csv")
                continue

            # Normal run: skip past years already in manifest unless range explicitly set
            if key in done and year_str != current_year and not year_range_explicit:
                log.file_download_skip(filename=f"{stem}.csv")
                continue

            log.file_download_start(filename=f"{stem}.csv")
            t0 = time.perf_counter()

            result = _fetch_file(session, record_type, period_type,
                                 committee_type, year)
            if result is None:
                log.file_download_error(
                    filename=f"{stem}.csv",
                    error="not found (tried CSV and XLSX)",
                )
                err += 1
                continue

            ext, row_count, content = result
            out_path = RAW_DIR / f"{stem}.csv"
            out_path.write_bytes(content)

            log.file_download_ok(
                filename=out_path.name,
                bytes=len(content),
                rows=row_count,
                duration_s=round(time.perf_counter() - t0, 2),
            )
            upsert_manifest({
                "relation_type": relation_type,
                "year":          year_str,
                "filename":      out_path.name,
                "row_count":     row_count,
            })
            done.add(key)
            ok += 1
            time.sleep(0.3)

    return ok, err


# ========================= committee history =========================

def _load_done_ids() -> set[int]:
    """Return set of committee_ids already written to committee_history.csv."""
    if not COMMITTEE_HISTORY_PATH.exists():
        return set()
    done: set[int] = set()
    with open(COMMITTEE_HISTORY_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            raw = row.get("committee_id", "")
            if raw:
                done.add(int(raw))
    return done


def _span(html: str, id_suffix: str) -> str:
    """Extract and strip inner text from a span by its ASP.NET control ID suffix."""
    m = re.search(
        rf'id="ctl00_ContentPlaceHolder1_{re.escape(id_suffix)}"[^>]*>(.*?)</span>',
        html, re.DOTALL,
    )
    if not m:
        return ""
    return re.sub(r"<[^>]+>", "", m.group(1)).strip()


def parse_committee_page(html: str) -> dict | None:
    """Parse a CommitteeHistory page into a flat dict.
    Returns None if the page has no PanelHistory div (invalid/missing ID).
    Extracts data from the most recent registration (ctl01 in the repeater).
    """
    if "PanelHistory" not in html:
        return None

    # Committee name lives in a global span at the top, wrapped in <b><i>
    committee_name = re.sub(r"<[^>]+>", "", _span(html, "lblCommitteeName")).strip()
    if not committee_name:
        return None

    status          = _span(html, "lblCommitteeStatus")
    committee_type  = _span(html, "lblCommitteeType")
    # subtype is rendered as "(Two or More Individuals)" — strip parens
    committee_subtype = _span(html, "lblCommitteeSubType").strip("()")

    # Address from most recent registration (ctl01)
    address1   = _span(html, "rptCommittee_ctl01_lblCommitteeAddress1")
    address2   = _span(html, "rptCommittee_ctl01_lblCommitteeAddress2").strip()

    # address2 format: "City ST 00000" or "City ST 00000 0000" or "City ST 00000-0000"
    # The 9-digit zip may appear with a space or hyphen separator.
    city = state = zip_code = ""
    m = re.match(
        r"^(.*?)\s+([A-Z]{2})\s+(\d{5})(?:[-\s]\d{4})?\s*$", address2
    )
    if m:
        city     = m.group(1).strip()
        state    = m.group(2)
        zip_code = m.group(3)

    # Officers — role label precedes the lblOfficerInfo span in the same <td>.
    # Only read from ctl01 (most recent registration).
    # Candidate committees label the candidate as "Candidate" (not "Chairperson").
    role_pattern = re.compile(
        r">(Candidate|Chairperson|Treasurer|Deputy Treasurer)"
        r"<span[^>]*rptCommittee_ctl01_rptOfficer_ctl(\d+)_lblOfficerInfo",
        re.IGNORECASE,
    )
    name_pattern = re.compile(
        r"rptCommittee_ctl01_rptOfficer_ctl(\d+)_lblOfficerName[^>]*>(.*?)</span>"
    )

    roles = {m.group(2): m.group(1).lower() for m in role_pattern.finditer(html)}
    names = {
        m.group(1): re.sub(r"<[^>]+>", "", m.group(2)).strip()
        for m in name_pattern.finditer(html)
    }

    officers: dict[str, str] = {}  # role → name
    for idx, role in roles.items():
        officers[role] = names.get(idx, "")

    return {
        "committee_name":    committee_name,
        "committee_type":    committee_type,
        "committee_subtype": committee_subtype,
        "status":            status,
        "address":           address1,
        "city":              city,
        "state":             state,
        "zip":               zip_code,
        "candidate_name":    officers.get("candidate", ""),
        "chairperson_name":  officers.get("chairperson", ""),
        "treasurer_name":    officers.get("treasurer", ""),
        "downloaded_at":     datetime.today().strftime("%Y-%m-%d"),
    }


def download_entities(log, session: requests.Session,
                      force: bool = False) -> tuple[int, int]:
    """Sweep CommitteeHistory pages 1–COMMITTEE_ID_CEILING, writing new
    records to committee_history.csv. Resumes from existing file unless force.

    Incremental runs start from (max_done_id - INCREMENTAL_CUSHION) rather than
    from 1, so we skip thousands of already-scraped low IDs and go straight to
    the active region near the current ceiling.
    """
    if force and COMMITTEE_HISTORY_PATH.exists():
        COMMITTEE_HISTORY_PATH.unlink()

    done_ids     = set() if force else _load_done_ids()
    write_header = not COMMITTEE_HISTORY_PATH.exists()

    # On incremental runs, start just below the highest known-good ID so we
    # don't re-probe thousands of already-done entries before reaching new ones.
    if done_ids and not force:
        floor = max(1, max(done_ids) - INCREMENTAL_CUSHION)
    else:
        floor = 1

    log.info(
        f"Committee history: sweeping IDs {floor}–{COMMITTEE_ID_CEILING} "
        f"({len(done_ids)} already done, "
        f"stops after {MAX_CONSECUTIVE_BLANK} consecutive blanks)"
    )

    ok = err = consecutive_blank = 0
    t0 = time.perf_counter()

    with open(COMMITTEE_HISTORY_PATH, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=COMMITTEE_HISTORY_COLS, extrasaction="ignore"
        )
        if write_header:
            writer.writeheader()

        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(
                total=COMMITTEE_ID_CEILING - floor + 1,
                desc="  committees",
                unit="id",
                dynamic_ncols=True,
                colour="green",
            ) as bar:
                for cid in range(floor, COMMITTEE_ID_CEILING + 1):
                    bar.update(1)

                    if cid in done_ids:
                        # Already scraped — reset blank counter so done IDs
                        # in the cushion range don't absorb blank budget.
                        consecutive_blank = 0
                        continue

                    url = COMMITTEE_HISTORY_URL.format(id=cid)
                    try:
                        resp = session.get(url, timeout=30)
                        resp.raise_for_status()
                    except requests.RequestException as e:
                        log.page_scrape_error(
                            entity="committee", page_id=cid, error=str(e)
                        )
                        err += 1
                        time.sleep(2)
                        continue

                    parsed = parse_committee_page(resp.text)

                    if parsed is None:
                        consecutive_blank += 1
                        if consecutive_blank >= MAX_CONSECUTIVE_BLANK:
                            log.info(
                                f"  {MAX_CONSECUTIVE_BLANK} consecutive blanks "
                                f"— stopping at ID {cid}"
                            )
                            bar.update(COMMITTEE_ID_CEILING - cid)
                            break
                        time.sleep(0.1)
                        continue

                    consecutive_blank = 0
                    parsed["committee_id"] = cid
                    writer.writerow(parsed)

                    bar.set_postfix_str(
                        parsed["committee_name"][:45].ljust(45), refresh=False
                    )
                    ok += 1
                    time.sleep(0.2)

    total_rows = (
        sum(1 for _ in open(COMMITTEE_HISTORY_PATH, encoding="utf-8")) - 1
        if COMMITTEE_HISTORY_PATH.exists() else 0
    )
    log.page_scrape_complete(
        filename=str(COMMITTEE_HISTORY_PATH),
        rows=total_rows,
        duration_s=round(time.perf_counter() - t0, 1),
        ok=ok,
        err=err,
    )
    return ok, err


# ============================ orchestrator ============================

def run(force: bool = False, entities: bool = False, transactions: bool = False,
        start_year: int | None = None, end_year: int | None = None,
        contributions: bool = False, expenditures: bool = False):
    """Orchestrate download of transaction CSVs and/or committee history pages."""
    log = get_logger("connecticut", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Connecticut scraper")
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures)

    transactions_implied = contributions or expenditures
    do_both         = not entities and not transactions and not transactions_implied
    do_transactions = transactions or transactions_implied or do_both
    do_entities     = entities     or do_both

    files_ok = files_err = pages_ok = pages_err = 0

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    try:
        if force:
            if do_both or do_transactions:
                if MANIFEST.exists():
                    MANIFEST.unlink()
            # entity force-clear (COMMITTEE_HISTORY_PATH) happens inside
            # download_entities to keep the logic co-located with the sweep

        if do_transactions:
            files_ok, files_err = download_transactions(
                log, session, force=force,
                start_year=start_year, end_year=end_year,
                contributions=contributions, expenditures=expenditures,
            )

        if do_entities:
            pages_ok, pages_err = download_entities(log, session, force=force)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit(
            "scrape_completed",
            status="completed",
            duration_s=duration,
            files_ok=files_ok,
            files_err=files_err,
            pages_ok=pages_ok,
            pages_err=pages_err,
        )

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit(
            "scrape_completed",
            status="interrupted",
            duration_s=round(time.perf_counter() - t0, 1),
            files_ok=files_ok,
            files_err=files_err,
            pages_ok=pages_ok,
            pages_err=pages_err,
        )
        raise

    except Exception as e:
        log._emit(
            "scrape_completed",
            status="error",
            duration_s=round(time.perf_counter() - t0, 1),
            files_ok=files_ok,
            files_err=files_err,
            pages_ok=pages_ok,
            pages_err=pages_err,
            error_type=type(e).__name__,
            error=str(e),
        )
        raise


# ================================ CLI =================================
if __name__ == "__main__":
    import argparse

    ap   = argparse.ArgumentParser(
        description="Download Connecticut SEEC eCRIS campaign finance data."
    )
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",        action="store_true",
                      help="re-download everything, ignoring the manifest")
    vert.add_argument("--start-year",   type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); re-downloads all in-range years")
    ap.add_argument("--end-year",       type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")
    ap.add_argument("--transactions",   action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",       action="store_true",
                    help="entities only (committee history ID sweep)")
    ap.add_argument("--contributions",  action="store_true",
                    help="receipts only (CT terminology for contributions)")
    ap.add_argument("--expenditures",   action="store_true",
                    help="disbursements only (CT terminology for expenditures)")
    args, _ = ap.parse_known_args()
    cy = datetime.today().year
    if args.end_year:
        if args.end_year > cy:
            ap.error(f"--end-year cannot exceed current year ({cy})")
        if args.start_year and args.start_year > args.end_year:
            ap.error("--start-year cannot be greater than --end-year")
    if args.force and args.end_year:
        ap.error("--force cannot be combined with --end-year")
    try:
        run(
            force=args.force,
            entities=args.entities,
            transactions=args.transactions,
            start_year=args.start_year,
            end_year=args.end_year,
            contributions=args.contributions,
            expenditures=args.expenditures,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
