"""
scrapers/indiana.py — Download Indiana campaign finance data.

Two components, one file:

  1. Transactions — bulk ZIP downloads from the Indiana Campaign Finance
     System's BulkDataDownloads endpoint:
       https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/{year}_ContributionData.csv.zip
       https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/{year}_ExpenditureData.csv.zip
     Years 2000-present.  Tracked in manifest.csv; current-year files always
     re-fetched.  No separate loans file exists for Indiana.

  2. Entities (committees + candidates) — sequential OrgId sweep across
     CommitteeDetail.aspx:
       https://campaignfinance.in.gov/PublicSite/SearchPages/CommitteeDetail.aspx?OrgId={N}
     Every registered committee AND every candidate share this single detail
     page — candidates are "Candidate" type committees with extra
     Candidate/County/District/Office fields populated.  A single sweep over
     OrgId 1..~8400 produces one raw file (entities.csv) that the parser
     splits into committees.csv and candidates.csv based on committee_type.
     Invalid/unused OrgIds return a page with all info spans empty —
     analogous to Alaska's blank-page sentinel.

No authentication required.  Plain HTTP; no WAF issues observed.
"""

import csv
import io
import sys
import time
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================

RAW_DIR             = PROJECT_ROOT / "data" / "Indiana" / "raw"
MANIFEST_TX         = PROJECT_ROOT / "data" / "Indiana" / "manifest.csv"
ENTITIES_OUT        = RAW_DIR / "entities.csv"
ENTITIES_CHECKPOINT = RAW_DIR / "entities.checkpoint"

RAW_DIR.mkdir(parents=True, exist_ok=True)

# ========================= shared config ==============================

CURRENT_YEAR = datetime.today().year
SLEEP_SEC    = 0.2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
}

# ═══════════════════════════════════════════════════════════════════════════════
# PART 1 — TRANSACTIONS
# ═══════════════════════════════════════════════════════════════════════════════

TX_BASE_URL = "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads"
# The bulk-download system has data starting in 2000 (2001+ confirmed 200,
# 1999 confirmed 404).
TX_START_YEAR    = 2000
TX_YEARS         = list(range(TX_START_YEAR, CURRENT_YEAR + 1))
TX_MANIFEST_COLS = ["year", "data_type", "filename", "downloaded_at", "row_count"]

# (URL label, output file label) — Indiana has no separate loans bulk file;
# loan activity is embedded as a "Type"/"ExpenditureType" of "Loan" within
# the contribution/expenditure files themselves.
DATA_TYPES = [
    ("Contribution", "contributions"),
    ("Expenditure",  "expenditures"),
]


# ── Transaction manifest helpers ──────────────────────────────────────────────

def _tx_load_manifest() -> set[tuple[str, str]]:
    if not MANIFEST_TX.exists():
        return set()
    with open(MANIFEST_TX, newline="") as f:
        return {(r["year"], r["data_type"]) for r in csv.DictReader(f)}


def _tx_strip_manifest(keep_fn):
    if not MANIFEST_TX.exists():
        return
    with open(MANIFEST_TX, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST_TX, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TX_MANIFEST_COLS)
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))


def _tx_append_manifest(record: dict):
    write_header = not MANIFEST_TX.exists()
    with open(MANIFEST_TX, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TX_MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow(record)


# ── Transaction download ───────────────────────────────────────────────────────

def _tx_download(year: int, url_label: str, file_label: str,
                 session: requests.Session):
    """
    Fetch one year/type ZIP, decode the inner CSV, write to RAW_DIR.
    Returns ((filename, row_count), None) on success, (None, error) on failure.
    """
    zip_url  = f"{TX_BASE_URL}/{year}_{url_label}Data.csv.zip"
    filename = f"{file_label}_{year}.csv"
    out_path = RAW_DIR / filename

    try:
        resp = session.get(zip_url, timeout=120, stream=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        return None, str(e)

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            raw_csv = zf.read(zf.namelist()[0])
    except (zipfile.BadZipFile, IndexError) as e:
        return None, f"zip error: {e}"

    # Handle BOM / encoding variants produced by .NET exports
    if raw_csv[:3] == b"\xef\xbb\xbf":
        text = raw_csv[3:].decode("utf-8", errors="replace")
    elif raw_csv[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw_csv.decode("utf-16")
    elif len(raw_csv) > 1 and raw_csv[1] == 0:
        text = raw_csv.decode("utf-16-le", errors="replace")
    else:
        text = raw_csv.decode("utf-8", errors="replace")

    out_path.write_text(text, encoding="utf-8")
    row_count = max(text.count("\n") - 1, 0)
    return (filename, row_count), None


# ── Transaction runner ────────────────────────────────────────────────────────

def run_transactions(log, force: bool = False,
                     start_year: int | None = None, end_year: int | None = None,
                     contributions: bool = False, expenditures: bool = False) -> tuple[int, int]:
    """Download transaction ZIPs for all years × types.  Returns (ok, err)."""
    current_year_str    = str(CURRENT_YEAR)
    year_range_explicit = start_year is not None or end_year is not None

    years = [y for y in TX_YEARS
             if (start_year is None or y >= start_year)
             and (end_year   is None or y <= end_year)]

    if contributions and not expenditures:
        active_types = [(u, f) for u, f in DATA_TYPES if f == "contributions"]
    elif expenditures and not contributions:
        active_types = [(u, f) for u, f in DATA_TYPES if f == "expenditures"]
    else:
        active_types = DATA_TYPES

    if force:
        if MANIFEST_TX.exists():
            MANIFEST_TX.unlink()
        done = set()
    elif year_range_explicit:
        # Wipe manifest entries for the in-range years so they're re-fetched
        # below regardless of manifest state.
        in_range = {str(y) for y in years}

        def _outside_range(r: dict) -> bool:
            return r["year"] not in in_range

        _tx_strip_manifest(_outside_range)
        done = _tx_load_manifest()
    else:
        done = _tx_load_manifest()

    session = requests.Session()
    session.headers.update({**HEADERS,
                             "Referer": "https://campaignfinance.in.gov/PublicSite/"})

    ok = err = 0
    for year in years:
        for url_label, file_label in active_types:
            key      = (str(year), file_label)
            filename = f"{file_label}_{year}.csv"

            # Re-download if: current year, year range explicitly requested, or not yet done.
            if key in done and str(year) != current_year_str and not year_range_explicit:
                log.file_download_skip(filename=filename)
                continue

            t_file = time.perf_counter()
            log.file_download_start(filename=filename)
            result, error = _tx_download(year, url_label, file_label, session)

            if result is None:
                log.file_download_error(filename=filename, error=error)
                err += 1
                continue

            dl_filename, row_count = result
            log.file_download_ok(
                filename=dl_filename,
                bytes=(RAW_DIR / dl_filename).stat().st_size,
                rows=row_count,
                duration_s=round(time.perf_counter() - t_file, 2),
            )
            _tx_append_manifest({
                "year":          str(year),
                "data_type":     file_label,
                "filename":      dl_filename,
                "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                "row_count":     row_count,
            })
            done.add(key)
            ok += 1

    return ok, err


# ═══════════════════════════════════════════════════════════════════════════════
# PART 2 — ENTITIES (OrgId sweep, committees + candidates in one pass)
# ═══════════════════════════════════════════════════════════════════════════════

ENTITY_URL          = "https://campaignfinance.in.gov/PublicSite/SearchPages/CommitteeDetail.aspx"
KNOWN_MAX_ORG_ID    = 8400   # last verified ceiling (8300=valid, 8400+=blank); binary search confirms

ENTITY_COLS = [
    "org_id", "committee_type", "committee_name", "abbrev_name",
    "address1", "address2", "city_state_zip", "party",
    "phone", "status", "fax", "date_organized", "date_terminated",
    "registered_fec", "purpose", "affiliations",
    "supports_entire_ticket", "supports_party", "public_question", "question_position",
    "candidate_name", "county", "exploratory", "district", "office",
    "bank_depositories", "treasurer_name", "treasurer_phone",
    "scraped_at",
]


# ── Binary-search max-ID finder (adapted from Colorado) ───────────────────────

def _find_max_id(fetch_fn, known_max: int, step: int = 200) -> int:
    """
    Return the highest consecutive valid ID at or above known_max.

    Steps upward by `step` until the first gap, then binary-searches
    [last_valid, first_invalid] for the exact edge.  If known_max itself is
    already past the end, searches downward instead.

    fetch_fn(id) must return truthy for a valid page, falsy for a gap/404.
    """
    if not fetch_fn(known_max):
        lo, hi = max(1, known_max // 2), known_max
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if fetch_fn(mid):
                lo = mid
            else:
                hi = mid - 1
        return lo

    lo = known_max
    hi = known_max + step
    while fetch_fn(hi):
        lo = hi
        hi += step

    while lo < hi - 1:
        mid = (lo + hi) // 2
        if fetch_fn(mid):
            lo = mid
        else:
            hi = mid

    return lo


# ── Page helpers ──────────────────────────────────────────────────────────────

def _txt(soup, id_: str) -> str:
    tag = soup.find(id=id_)
    return tag.text.strip() if tag else ""


def _parse_entity_page(org_id: int, html: str) -> dict | None:
    """
    Parse CommitteeDetail.aspx HTML for one OrgId.
    Returns a flat dict, or None for a blank/unused OrgId.
    """
    soup = BeautifulSoup(html, "html.parser")

    def t(id_): return _txt(soup, f"_ctl0_Content_{id_}")

    name = t("lblCommName")
    cid  = t("lblCommitteeID")
    if not name or not cid:
        return None

    return {
        "org_id":                 org_id,
        "committee_type":         t("lblCommitteeType"),
        "committee_name":         name,
        "abbrev_name":            t("lblCommAbbrev"),
        "address1":               t("lblPhysAddress1"),
        "address2":               t("lblPhysAddress2"),
        "city_state_zip":         t("lblPhysCityStateZip"),
        "party":                  t("lblCommParty"),
        "phone":                  t("lblCommPhone"),
        "status":                 t("lblCommStatus"),
        "fax":                    t("lblCommFax"),
        "date_organized":         t("lblCommDateOrganized"),
        "date_terminated":        t("lblCommDateTerminated"),
        "registered_fec":         t("lblRegisteredFEC"),
        "purpose":                t("lblCommPurpose"),
        "affiliations":           t("lblAffiliations"),
        "supports_entire_ticket": t("lblSupportsEntireTicket"),
        "supports_party":         t("lblSupportParty"),
        "public_question":        t("lblPublicQuestion"),
        "question_position":      t("lblQuestionPosition"),
        "candidate_name":         t("lblCandidateName"),
        "county":                 t("lblCounty"),
        "exploratory":            t("lblExploratory"),
        "district":               t("lblDistrict"),
        "office":                 t("lblCandidateOffice"),
        "bank_depositories":      t("lblBankDepositories"),
        "treasurer_name":         t("lblTreasurer"),
        "treasurer_phone":        t("lblTreasurerPhone"),
        "scraped_at":             datetime.today().strftime("%Y-%m-%d"),
    }


# ── Thread-local sessions ─────────────────────────────────────────────────────

_thread_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


def _fetch_org(org_id: int) -> dict | None:
    session = _get_session()
    try:
        r = session.get(ENTITY_URL, params={"OrgId": org_id}, timeout=30)
        r.raise_for_status()
    except requests.RequestException:
        return None
    time.sleep(SLEEP_SEC)
    return _parse_entity_page(org_id, r.text)


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def _load_checkpoint(path: Path) -> int:
    if path.exists():
        try:
            return int(path.read_text().strip())
        except ValueError:
            pass
    return 0


def _save_checkpoint(path: Path, val: int):
    path.write_text(str(val))


# ── Entity writer (thread-safe) ───────────────────────────────────────────────

_write_lock = threading.Lock()


def _write_entity_rows(rows: list[dict]):
    if not rows:
        return
    write_header = not ENTITIES_OUT.exists()
    with _write_lock:
        with open(ENTITIES_OUT, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=ENTITY_COLS, extrasaction="ignore")
            if write_header:
                w.writeheader()
            w.writerows(rows)


# ── Entity runner ─────────────────────────────────────────────────────────────

def run_entities(log, force: bool = False, start_org: int = 0,
                 max_org: int | None = None, workers: int = 8) -> tuple[int, int]:
    """
    Sweep CommitteeDetail.aspx?OrgId=N from 1 -> max_org (auto-detected).
    Resumable via ENTITIES_CHECKPOINT.  Returns (found, errors).

    Covers BOTH committees and candidates — candidates are "Candidate" type
    committee registrations with Candidate/County/District/Office fields
    populated.  The parser splits entities.csv into committees.csv and
    candidates.csv based on committee_type.
    """
    if force:
        for f in [ENTITIES_OUT, ENTITIES_CHECKPOINT]:
            if f.exists():
                f.unlink()

    if max_org is None:
        session = requests.Session()
        session.headers.update(HEADERS)

        def _probe_org(oid):
            try:
                r = session.get(ENTITY_URL, params={"OrgId": oid}, timeout=10)
                r.raise_for_status()
                return _parse_entity_page(oid, r.text) is not None
            except Exception:
                return False

        log.info(f"  Auto-detecting max OrgId (anchor={KNOWN_MAX_ORG_ID:,}) …")
        max_org = _find_max_id(_probe_org, KNOWN_MAX_ORG_ID)
        log.info(f"  Max OrgId: {max_org:,}")

    checkpoint = max(_load_checkpoint(ENTITIES_CHECKPOINT), start_org)
    start_from = checkpoint + 1

    if start_from > max_org:
        log.info(f"Entities: already complete (checkpoint={checkpoint:,}).")
        return 0, 0

    total = max_org - start_from + 1
    found = err = 0
    CHUNK = 200
    BATCH = 50
    t0 = time.perf_counter()

    log.info(f"Entities: OrgId {start_from:,} -> {max_org:,} "
             f"({total:,} IDs, {workers} workers)")

    completed = 0
    buffer: list[dict] = []
    org_iter = iter(range(start_from, max_org + 1))

    with logging_redirect_tqdm(loggers=[log._log]):
        bar = tqdm(total=total, desc="  entities", unit="id", dynamic_ncols=True)
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                pending = {}
                for oid in org_iter:
                    pending[pool.submit(_fetch_org, oid)] = oid
                    if len(pending) >= CHUNK:
                        break

                while pending:
                    for future in as_completed(pending):
                        oid    = pending.pop(future)
                        result = future.result()
                        completed += 1
                        bar.update(1)

                        if result:
                            buffer.append(result)
                            found += 1
                            bar.set_postfix_str(
                                (result.get("committee_name") or "")[:40], refresh=False)
                        else:
                            err += 1

                        for next_oid in org_iter:
                            pending[pool.submit(_fetch_org, next_oid)] = next_oid
                            break

                        if completed % BATCH == 0:
                            _write_entity_rows(buffer)
                            buffer.clear()
                            _save_checkpoint(ENTITIES_CHECKPOINT, oid)

                        break  # rolling window: one future at a time

        finally:
            bar.close()

    _write_entity_rows(buffer)
    _save_checkpoint(ENTITIES_CHECKPOINT, max_org)
    log.page_scrape_complete(
        filename=str(ENTITIES_OUT),
        rows=found,
        duration_s=round(time.perf_counter() - t0, 1),
        ok=found,
        err=err,
    )
    return found, err


# ═══════════════════════════════════════════════════════════════════════════════
# TOP-LEVEL RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def run(force: bool = False, entities: bool = False, transactions: bool = False,
        start_year: int | None = None, end_year: int | None = None,
        contributions: bool = False, expenditures: bool = False,
        candidates: bool = False, committees: bool = False,
        workers: int = 8, start_org: int = 0, max_org: int | None = None):
    """
    Entry point used by orc.py.

    entities=True     — run the OrgId sweep (committees + candidates)
    transactions=True — run bulk ZIP downloads for contributions/expenditures
    (neither flag)    — run everything
    """
    log = get_logger("indiana", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures)

    transactions_implied = contributions or expenditures
    entities_implied     = candidates or committees
    do_both         = not (entities or transactions or transactions_implied or entities_implied)
    do_transactions = transactions or transactions_implied or do_both
    do_entities     = entities or entities_implied or do_both

    files_ok = files_err = 0
    pages_ok = pages_err = 0

    try:
        if do_transactions:
            files_ok, files_err = run_transactions(
                log, force=force,
                start_year=start_year, end_year=end_year,
                contributions=contributions, expenditures=expenditures,
            )

        if do_entities:
            pages_ok, pages_err = run_entities(
                log, force=force, start_org=start_org, max_org=max_org, workers=workers,
            )

        duration = round(time.perf_counter() - t0, 1)
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)

    except KeyboardInterrupt:
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err,
                  error_type=type(e).__name__, error=str(e))
        raise


# =============================== CLI ==================================

if __name__ == "__main__":
    import argparse
    ap   = argparse.ArgumentParser(description="Download Indiana campaign finance data.")
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",        action="store_true",
                      help="re-download everything, ignoring manifest and checkpoint")
    vert.add_argument("--start-year",   type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")
    ap.add_argument("--end-year",       type=int, metavar="YYYY",
                    help="latest year to download (inclusive, <= current year)")
    ap.add_argument("--transactions",   action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",       action="store_true",
                    help="entities only (committees + candidates)")
    ap.add_argument("--contributions",  action="store_true",
                    help="contributions only")
    ap.add_argument("--expenditures",   action="store_true",
                    help="expenditures only")
    ap.add_argument("--candidates",     action="store_true",
                    help="candidates only")
    ap.add_argument("--committees",     action="store_true",
                    help="committees only")
    # Internal resume flags — for manual recovery after a partial entity sweep
    ap.add_argument("--start-org",      type=int, default=0,
                    help="entity sweep: resume from this OrgId")
    ap.add_argument("--max-org",        type=int, default=None,
                    help="entity sweep: max OrgId (default: auto-detect)")
    ap.add_argument("--workers",        type=int, default=8,
                    help="parallel workers for entity sweep (default 8)")
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
            candidates=args.candidates,
            committees=args.committees,
            workers=args.workers,
            start_org=args.start_org,
            max_org=args.max_org,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
