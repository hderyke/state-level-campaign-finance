"""
scrapers/kansas.py — Download Kansas campaign finance PDFs from the Kansas
Public Disclosure Commission (KPDC) at kpdc.kansas.gov.

All filings are individual PDFs organised on static HTML index pages hosted at
www.kansas.gov/ethics/CFAScanned/. Each index page covers one office group and
one election cycle (2-4 years). The table on each index lists every candidate
with links to their filed Receipts & Expenditures (R&E) reports, Appointment
of Treasurer (AT) forms, Pre-Primary / Pre-General Last Minute filings (PLF/GLF),
and any amendments.

Only R&E report PDFs are downloaded (AT forms are skipped — they contain no
financial data; Affidavit PDFs are also skipped).  Amendment PDFs are downloaded
alongside their originals; the parser prefers amendments when both exist for the
same (candidate, period).

Sources (all offices with candidate-level data):
    House         — 2016, 2018, 2020, 2022, 2024, 2026 election cycles
    Senate        — 2016, 2020, 2024, 2028 + occasional special elections
    Statewide     — 2014, 2018, 2022, 2026 (Governor, AG, SOS, Treasurer, Ins.)
    District Atty — 2016, 2020, 2024, 2028

Raw files (data/Kansas/raw/):
    {filename}.pdf   — R&E reports, PLF/GLF, and amendment PDFs (flat directory)
    _index_cache/    — cached HTML index pages (one file per URL, keyed by md5(url))

Manifest (data/Kansas/manifest.csv):
    filename, office, election_year, district, candidate_name, period, url,
    downloaded_at
"""

import csv
import hashlib
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR       = PROJECT_ROOT / "data" / "Kansas" / "raw"
INDEX_CACHE   = RAW_DIR / "_index_cache"
MANIFEST      = PROJECT_ROOT / "data" / "Kansas" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INDEX_CACHE.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [
    "filename", "office", "election_year", "district",
    "candidate_name", "period", "url", "downloaded_at",
]

# ======================== Index page catalog ==========================
# (office_label, election_year, index_url)
# election_year matches the cycle label used in the URL; it is also the year
# written into the manifest and used as the candidate's election_year.
INDEX_PAGES = [
    # House of Representatives — 2-year cycles
    ("House", "2026", "http://www.kansas.gov/ethics/CFAScanned/House/2026ElecCycle/HLinks2026EC.htm"),
    ("House", "2024", "http://www.kansas.gov/ethics/CFAScanned/House/2024ElecCycle/HLinks2024EC.htm"),
    ("House", "2022", "http://www.kansas.gov/ethics/CFAScanned/House/2022ElecCycle/HLinks2022EC.htm"),
    ("House", "2020", "http://www.kansas.gov/ethics/CFAScanned/House/2020ElecCycle/HLinks2020EC.htm"),
    ("House", "2018", "http://www.kansas.gov/ethics/CFAScanned/House/2018ElecCycle/HLinks2018EC.htm"),
    ("House", "2016", "http://www.kansas.gov/ethics/CFAScanned/House/2016ElecCycle/HLinks2016EC.htm"),
    # State Senate — 4-year cycles
    ("Senate", "2028", "http://www.kansas.gov/ethics/CFAScanned/Senate/2028ElecCycle/SLinks2028EC.htm"),
    ("Senate", "2024", "http://www.kansas.gov/ethics/CFAScanned/Senate/2024ElecCycle/SLinks2024EC.htm"),
    ("Senate", "2022-special", "http://www.kansas.gov/ethics/CFAScanned/Senate/2022SpecialElection/SLinks2022SpecialElection.htm"),
    ("Senate", "2020", "http://www.kansas.gov/ethics/CFAScanned/Senate/2020ElecCycle/SLinks2020EC.htm"),
    ("Senate", "2018-special", "http://www.kansas.gov/ethics/CFAScanned/Senate/2018SpecialElection/SLinks2018SpecialElection.htm"),
    ("Senate", "2016", "http://www.kansas.gov/ethics/CFAScanned/Senate/2016ElecCycle/SLinks2016EC.htm"),
    # Statewide (Governor, AG, SOS, Treasurer, Commissioner of Insurance) — 4-year cycles
    ("Statewide", "2026", "http://www.kansas.gov/ethics/CFAScanned/StWide/2026ElecCycle/SWLinks2026EC.htm"),
    ("Statewide", "2022", "http://www.kansas.gov/ethics/CFAScanned/StWide/2022ElecCycle/SWLinks2022EC.htm"),
    ("Statewide", "2018", "http://www.kansas.gov/ethics/CFAScanned/StWide/2018ElecCycle/SWLinks2018EC.htm"),
    ("Statewide", "2014", "http://www.kansas.gov/ethics/CFAScanned/StWide/2014ElecCycle/SWLinks2014EC.htm"),
    # District Attorneys — same 4-year cycle as Senate
    ("DA", "2028", "http://www.kansas.gov/ethics/CFAScanned/DA/2028ElecCycle/DistAttryLink2028.htm"),
    ("DA", "2024", "http://www.kansas.gov/ethics/CFAScanned/DA/2024ElecCycle/DistAttryLink2024.htm"),
    ("DA", "2020", "http://www.kansas.gov/ethics/CFAScanned/DA/2020ElecCycle/DistAttryLink2020.htm"),
    ("DA", "2016", "http://www.kansas.gov/ethics/CFAScanned/DA/2016ElecCycle/DistAttryLink2016.htm"),
]

# PDF link patterns to SKIP: AT (appointment of treasurer) and Affidavit forms.
# These contain no transaction data.
_SKIP_PATTERNS = re.compile(r"_AT\.pdf$|_aff\w*\.pdf$|amendAT\.pdf$", re.IGNORECASE)

# ======================= Session / HTTP helpers =======================
SESSION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
}


def _get(session: requests.Session, url: str, retries: int = 4,
         timeout: int = 60) -> requests.Response:
    last_err = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise last_err


# ========================= Manifest helpers ==========================

def load_manifest() -> dict[str, dict]:
    """Return {filename: row} for all entries in the manifest."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {row["filename"]: row for row in csv.DictReader(f)}


def strip_manifest(keep_fn) -> None:
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict) -> None:
    write_header = not MANIFEST.exists() or MANIFEST.stat().st_size == 0
    with open(MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow(record)


# ====================== Index page parsing ==========================

def _cache_path(url: str) -> Path:
    """Deterministic cache file path for an index URL."""
    key = hashlib.md5(url.encode()).hexdigest()
    return INDEX_CACHE / f"{key}.html"


def fetch_index_html(session: requests.Session, url: str,
                     use_cache: bool = False) -> str:
    """Return HTML for an index page. Optionally serve from disk cache."""
    cache = _cache_path(url)
    if use_cache and cache.exists():
        return cache.read_text(encoding="utf-8", errors="replace")
    r = _get(session, url)
    html = r.text
    cache.write_text(html, encoding="utf-8", errors="replace")
    return html


def _is_pdf_link(href: str) -> bool:
    """True if href points to a PDF we want (R&E report, PLF, GLF, amendment)."""
    if not href or not href.lower().endswith(".pdf"):
        return False
    return not _SKIP_PATTERNS.search(href)


def _period_from_url(url: str) -> str:
    """Extract the period code from a PDF URL, e.g. '202410' from the filename."""
    fname = url.rstrip("/").split("/")[-1]
    # Typical patterns: H001DH_202410.pdf, H001DH_amend2410.pdf,
    #                   H001DH_2024PLF.pdf, H001DH_2024GLF.pdf
    m = re.search(r"_(\w+)\.pdf$", fname, re.IGNORECASE)
    return m.group(1) if m else ""


def parse_index(html: str, office: str, election_year: str) -> list[dict]:
    """
    Parse an index page HTML and return a list of PDF records:
        {office, election_year, district, candidate_name, period, url, filename}

    Table layouts differ by office:

      House / Senate  →  cells[0]=District, cells[1]=Candidate, cells[2]=AT, cells[3:]=periods
      Statewide / DA  →  cells[0]=Candidate, cells[1]=AT,        cells[2:]=periods

    In both cases the AT form cell is iterated over but filtered by _is_pdf_link,
    which rejects filenames matching _SKIP_PATTERNS (_AT.pdf, _aff*.pdf, *amendAT.pdf).
    """
    # House and Senate index pages have a leading district column; others don't.
    has_district_col = office in ("House", "Senate")

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    records: list[dict] = []
    html_rows = table.find_all("tr")

    for row in html_rows[1:]:   # skip header row
        cells = row.find_all("td")

        if has_district_col:
            if len(cells) < 3:
                continue
            district      = cells[0].get_text(strip=True)
            candidate_raw = cells[1].get_text(strip=True)
            pdf_cells     = cells[2:]   # includes AT column + all period columns
        else:
            # Statewide, DA: no district column
            if len(cells) < 2:
                continue
            district      = ""
            candidate_raw = cells[0].get_text(strip=True)
            pdf_cells     = cells[1:]   # includes AT column + all period columns

        if not candidate_raw:
            continue

        # Some Kansas index pages (notably 2026 House "202601" period) append
        # the AT form's period code directly onto the candidate name cell text,
        # e.g. "Helwig, DaleAT202601" or "Croft, ChristopherATAmendment202601".
        # Strip the trailing AT{[Amendment]}{digits} suffix if present.
        candidate_name = re.sub(r'AT(?:Amendment)?\d{4,6}$', '', candidate_raw.strip(), flags=re.IGNORECASE).strip()

        for cell in pdf_cells:
            for a in cell.find_all("a"):
                href = a.get("href", "")
                if not _is_pdf_link(href):
                    continue
                if href.startswith("http"):
                    pdf_url = href
                else:
                    pdf_url = "https://www.kansas.gov" + href

                filename = pdf_url.rstrip("/").split("/")[-1]
                period   = _period_from_url(pdf_url)

                records.append({
                    "office":         office,
                    "election_year":  election_year,
                    "district":       district,
                    "candidate_name": candidate_name,
                    "period":         period,
                    "url":            pdf_url,
                    "filename":       filename,
                })

    return records


# =========================== Downloader ==============================

def _is_current_cycle(election_year: str, current_year: int) -> bool:
    """True if this cycle could still receive new filings (year ≥ current year)."""
    try:
        return int(election_year.split("-")[0]) >= current_year
    except (ValueError, IndexError):
        return False


# ============================ run ====================================

def run(
    force: bool = False,
    entities: bool = False,
    transactions: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
    contributions: bool = False,
    expenditures: bool = False,
    candidates: bool = False,
    committees: bool = False,
):
    """
    Download Kansas R&E PDFs from the KPDC static index pages.

    Horizontal scope flags are all ignored — Kansas PDFs contain both
    contributions and expenditures; there is no way to separate them at
    download time.

    Vertical scope:
        (no flag)        incremental — skip existing files; always re-check
                         PDFs for cycles whose election_year ≥ current_year.
        --start-year     re-download all PDFs with election_year ≥ YYYY
        --end-year       re-download all PDFs with election_year ≤ YYYY
        --force          wipe manifest and re-download everything
    """
    log = get_logger("kansas", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year)

    current_year = datetime.today().year
    files_ok = files_err = 0
    year_range_active = start_year is not None or end_year is not None

    try:
        session = requests.Session()
        session.headers.update(SESSION_HEADERS)

        # ── Manifest prep ──────────────────────────────────────────────
        if force:
            strip_manifest(lambda _: False)
            done = {}
        elif year_range_active:
            def _outside_range(r: dict) -> bool:
                try:
                    yr = int(r["election_year"].split("-")[0])
                except (ValueError, KeyError):
                    return True
                if start_year is not None and yr >= start_year:
                    return False
                if end_year is not None and yr <= end_year:
                    return False
                return True
            strip_manifest(_outside_range)
            done = load_manifest()
        else:
            done = load_manifest()

        # ── Iterate index pages ────────────────────────────────────────
        today = datetime.today().strftime("%Y-%m-%d")

        for office, election_year, index_url in INDEX_PAGES:
            # Apply year filter
            try:
                cy_int = int(election_year.split("-")[0])
            except ValueError:
                cy_int = 0

            if start_year is not None and cy_int < start_year:
                continue
            if end_year is not None and cy_int > end_year:
                continue

            is_current = _is_current_cycle(election_year, current_year)

            # Fetch index HTML (cache past cycles; always re-fetch current)
            log.info(f"  Fetching index: {office} {election_year}")
            try:
                html = fetch_index_html(session, index_url, use_cache=not is_current)
            except Exception as e:
                log.warning(f"    Index fetch failed: {e}")
                continue

            records = parse_index(html, office, election_year)
            if not records:
                log.info(f"    No PDF links found — index may be empty or not yet published")
                continue

            log.info(f"    Found {len(records)} PDF links for {office} {election_year}")
            time.sleep(0.3)

            # ── Download each PDF ──────────────────────────────────────
            for rec in records:
                filename = rec["filename"]
                dest     = RAW_DIR / filename

                # Skip logic: skip if in manifest and file exists,
                # unless this is a current cycle (always re-check) or --force.
                already_done = filename in done and dest.exists() and dest.stat().st_size > 0
                if already_done and not force and not is_current:
                    log.file_download_skip(filename=filename)
                    continue
                if already_done and is_current and dest.exists() and dest.stat().st_size > 0:
                    # Current cycle: only skip if already downloaded this run
                    log.file_download_skip(filename=filename)
                    continue

                log.file_download_start(filename=filename)
                t_file = time.perf_counter()
                try:
                    r = _get(session, rec["url"])
                    dest.write_bytes(r.content)
                    size = dest.stat().st_size
                    log.file_download_ok(
                        filename=filename, bytes=size, rows=0,
                        duration_s=round(time.perf_counter() - t_file, 2),
                    )
                    files_ok += 1
                except Exception as e:
                    log.file_download_error(filename=filename, error=str(e))
                    files_err += 1
                    continue

                append_manifest({
                    "filename":       filename,
                    "office":         rec["office"],
                    "election_year":  rec["election_year"],
                    "district":       rec["district"],
                    "candidate_name": rec["candidate_name"],
                    "period":         rec["period"],
                    "url":            rec["url"],
                    "downloaded_at":  today,
                })
                done[filename] = rec
                time.sleep(0.1)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok:,} downloaded, {files_err} errors")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  error_type=type(e).__name__, error=str(e))
        raise


# ============================= CLI ===================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download Kansas campaign finance PDFs from the KPDC static index pages."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all PDFs, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest election_year cycle to download")

    ap.add_argument("--end-year",      type=int, metavar="YYYY",
                    help="latest election_year cycle to download")

    # Horizontal scope flags — all accepted but ignored for Kansas
    for flag in ("--transactions", "--entities", "--contributions",
                 "--expenditures", "--candidates", "--committees"):
        ap.add_argument(flag, action="store_true",
                        help="(ignored — Kansas PDFs contain all data types)")

    args, _ = ap.parse_known_args()

    cy = datetime.today().year
    if args.end_year and args.end_year > cy + 4:
        ap.error(f"--end-year cannot exceed {cy + 4}")
    if getattr(args, "start_year", None) and args.end_year:
        if args.start_year > args.end_year:
            ap.error("--start-year cannot be greater than --end-year")
    if args.force and args.end_year:
        ap.error("--force cannot be combined with --end-year")

    try:
        run(
            force=args.force,
            start_year=getattr(args, "start_year", None),
            end_year=args.end_year,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
