"""
scrapers/florida.py — Download Florida campaign finance data.

All data from the Florida Division of Elections:
  https://dos.elections.myflorida.com

Entities (requests-based — normal HTML endpoints, no WAF issues):
  Committees (two-phase):
    Phase 1 — A-Z name search via ComLkupByName.asp (searchtype=1,
    "containing") to collect all account IDs regardless of active/closed
    status. Saves fl_committee_links.csv (account_id, name, type, status).

    Phase 2 — Scrape ComDetail.asp?account=X for full detail: address,
    phone, chairperson, treasurer, registered agent, purpose, affiliates.
    Saves fl_committee_details.csv.

    Update mode: POST to extractComList.asp for active-only bulk download
    (fl_committees_active.txt). New account IDs are synced into
    fl_committee_links.csv and their detail pages scraped. The parser diffs
    this file against the prior active download to flag newly-inactive
    committees.

  Candidates (single-phase):
    POST to extractCanList.asp per election cycle (tab-delimited file).
    Saves fl_candidates_{slug}.txt per election with AcctNum, VoterID,
    ElectionID, OfficeCode, party, treasurer, etc. Bulk files provide
    everything needed — CanDetail.asp adds only date_filed/method which
    aren't worth the per-page request overhead.
    Account IDs are per-registration (id_model="committee").

    Update mode: two most recent general elections plus any specials
    from the current year.

Transactions (Playwright — /cgi-bin/*.exe blocks datacenter IPs at CDN):
  Four types: contributions, expenditures, transfers, other.
  For each type the form is filled via Playwright with:
    - election = All
    - search criteria left blank (date range is sufficient)
    - queryformat = tab-delimited file
    - rowlimit = 32000 (confirmed max via form limit toggle)
    - 7-day date-range windows
  Adaptive sub-chunking: if a chunk returns exactly ROW_LIMIT rows
  (truncation), the window is automatically re-requested as 3-day
  sub-chunks. In practice 3-day windows should not exceed 32k even
  during the busiest Florida election periods (confirmed by testing
  Oct 25–Nov 7 2018 — the busiest stretch in Florida campaign finance
  history due to simultaneous governor + Senate recounts).
  Force: 1996-01-01 through today.
  Update: current calendar year only.
"""

import csv
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Florida" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Florida" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "key", "filename", "downloaded_at", "row_count"]

# ============================= constants ==============================

BASE_URL   = "https://dos.elections.myflorida.com"
START_DATE = date(1996, 1, 1)

# Transaction chunking — start large, recurse down as needed
CHUNK_DAYS     = 10      # primary window size
SUB_CHUNK_DAYS = 3       # kept for reference; actual levels defined in _CHUNK_LEVELS
ROW_LIMIT      = 32000   # confirmed max via form limit toggle
SLEEP          = 0.4     # seconds between requests

# contributions uses contrib.exe which accepts election=All + date range.
# The other three CGI endpoints (expend.exe, FundXfers.exe, OtherDist.exe)
# reject election=All + blank criteria — they require a specific election ID.
TRANSACTION_TYPES          = ["contributions", "expenditures", "transfers", "other"]
ELECTION_BASED_TYPES       = {"expenditures", "transfers", "other"}

TRANSACTION_FORM_PAGES = {
    "contributions": f"{BASE_URL}/campaign-finance/contributions/",
    "expenditures":  f"{BASE_URL}/campaign-finance/expenditures/",
    "transfers":     f"{BASE_URL}/campaign-finance/transfers/",
    "other":         f"{BASE_URL}/campaign-finance/other/",
}

TRANSACTION_RELATIONS = set(TRANSACTION_TYPES)
ENTITY_RELATIONS      = {
    "committee_links", "committee_detail", "committees_active",
    "candidate_bulk",
}

# Field name constants for each transaction form's rowlimit input
# (all four forms use the same field names — confirmed by scraping HTML)
TXN_FORM_FIELDS = {
    "election":          "election",
    "rowlimit":          "rowlimit",
    "date_from":         "cdatefrom",
    "date_to":           "cdateto",
    "amount_min":        "cdollar_minimum",
    "amount_max":        "cdollar_maximum",
    "queryformat":       "queryformat",   # value "2" = tab-delimited
    "sort1":             "csort1",
    "sort2":             "csort2",
}

# Amount ranges used when a single day hits ROW_LIMIT (payroll-deduction PACs
# file thousands of $1 and sub-$1 contributions with identical dates).
# Each tuple is (label, min_str, max_str) where empty string = no bound.
AMOUNT_RANGES = [
    ("lt1",       "0",        "0.99"),    # sub-dollar (union dues fractions)
    ("eq1",       "1",        "1"),       # exactly $1 (most common payroll deduction)
    ("1to10",     "1.01",     "10"),      # small amounts
    ("10to100",   "10.01",    "100"),     # medium-small
    ("100to1k",   "100.01",   "1000"),    # medium
    ("1kto10k",   "1000.01",  "10000"),   # medium-large
    ("10kto100k", "10000.01", "100000"),  # large
    ("gt100k",    "100000.01", ""),       # very large (max 32k seems unlikely)
]

COMMITTEE_LINKS_PATH   = RAW_DIR / "fl_committee_links.csv"
COMMITTEE_DETAILS_PATH = RAW_DIR / "fl_committee_details.csv"
COMMITTEE_ACTIVE_PATH  = RAW_DIR / "fl_committees_active.txt"

COMMITTEE_LINKS_COLS = ["account_id", "name", "type", "status"]

COMMITTEE_DETAIL_COLS = [
    "account_id", "name", "type", "status",
    "address", "phone",
    "chairperson_name", "chairperson_address",
    "treasurer_name",   "treasurer_address",
    "registered_agent_name", "registered_agent_address",
    "purpose", "affiliates", "scraped_at",
]


# ========================== manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    done: set[tuple[str, str]] = set()
    if not MANIFEST.exists():
        return done
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["key"]))
    return done


def strip_manifest(keep_fn) -> None:
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict) -> None:
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            existing = [
                r for r in csv.DictReader(f)
                if not (r["relation_type"] == record["relation_type"]
                        and r["key"] == record["key"])
            ]
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)


# ============================ session =================================

def get_session() -> requests.Session:
    """Return a requests session primed with Cloudflare cookies.

    The entity endpoints (HTML pages, form GETs/POSTs) work fine with a
    standard requests session. The /cgi-bin/*.exe transaction endpoints
    are blocked at the CDN for datacenter IPs and require Playwright.
    """
    import config
    s = requests.Session()
    s.headers.update({
        "User-Agent":      config.USER_AGENT,
        "Accept":          "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    })
    # Prime Cloudflare cookies with a warm-up GET
    try:
        s.get(BASE_URL, timeout=15)
    except Exception:
        pass
    return s


# ==================== committee entities — phase 1 ===================

def scrape_committee_links(session: requests.Session, log,
                           force: bool = False) -> int:
    """Phase 1: A-Z name search to discover all committee account IDs.

    Uses searchtype=1 ("containing") so each letter returns every
    committee whose name contains that letter — active and closed alike.
    We deduplicate by account_id across all 26 letters.

    Saves fl_committee_links.csv with account_id, name, type, status.
    Returns total unique account IDs found.
    """
    done = load_manifest()

    existing: list[dict] = []
    if COMMITTEE_LINKS_PATH.exists() and not force:
        with open(COMMITTEE_LINKS_PATH, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))

    seen_ids  = {r["account_id"] for r in existing}
    all_links = list(existing)

    log.info("\nFlorida committee links (phase 1 — A-Z search):")

    for letter in "abcdefghijklmnopqrstuvwxyz":
        key = ("committee_links", letter)
        if key in done and not force:
            log.file_download_skip(filename=f"committee_links/letter={letter}")
            continue

        log.file_download_start(filename=f"committee_links/letter={letter}")
        t0 = time.perf_counter()

        try:
            session.get(f"{BASE_URL}/committees/", timeout=15)
            r = session.post(
                f"{BASE_URL}/committees/ComLkupByName.asp",
                data={"comName": letter, "searchtype": "1",
                      "NameSearchBtn": "Search"},
                headers={"Referer": f"{BASE_URL}/committees/"},
                timeout=30,
            )
            r.raise_for_status()
        except Exception as e:
            log.file_download_error(filename=f"committee_links/letter={letter}",
                                    error=str(e))
            time.sleep(SLEEP * 3)
            continue

        soup      = BeautifulSoup(r.text, "html.parser")
        new_links: list[dict] = []

        for a in soup.find_all("a", href=re.compile(r"ComDetail\.asp\?account=\d+")):
            m = re.search(r"account=(\d+)", a["href"])
            if not m:
                continue
            account_id = m.group(1)
            if account_id in seen_ids:
                continue

            # Type and status sit in sibling <td> cells of the same row
            td = a.find_parent("td")
            if td:
                siblings = td.find_parent("tr").find_all("td")
                type_val   = siblings[1].get_text(strip=True) if len(siblings) > 1 else ""
                status_val = siblings[2].get_text(strip=True) if len(siblings) > 2 else ""
            else:
                type_val = status_val = ""

            new_links.append({
                "account_id": account_id,
                "name":       a.get_text(strip=True),
                "type":       type_val,
                "status":     status_val,
            })
            seen_ids.add(account_id)

        all_links.extend(new_links)
        log.file_download_ok(
            filename=f"committee_links/letter={letter}",
            bytes=len(r.content), rows=len(new_links),
            duration_s=round(time.perf_counter() - t0, 1),
        )

        # Write incrementally so an interruption doesn't lose progress
        with open(COMMITTEE_LINKS_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COMMITTEE_LINKS_COLS,
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_links)

        upsert_manifest({
            "relation_type": "committee_links", "key": letter,
            "filename":      "fl_committee_links.csv",
            "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
            "row_count":     len(new_links),
        })
        done.add(key)
        time.sleep(SLEEP)

    log.info(f"  Total committee links: {len(all_links):,}")
    return len(all_links)


# ==================== committee entities — phase 2 ===================

def _parse_committee_detail(html: str, account_id: str) -> dict:
    """Parse a ComDetail.asp page into a flat dict.

    The page is a simple list of bold label/value pairs. We extract
    text into lines and walk them looking for known labels.
    """
    soup  = BeautifulSoup(html, "html.parser")
    text  = soup.get_text("\n", strip=True)
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    STOPS = (
        "Type", "Status", "Address", "Phone", "Chairperson",
        "Treasurer", "Registered Agent", "Purpose", "Affiliates",
        "Campaign Documents",
    )

    def _after(label: str) -> str:
        """Return the first non-empty line following `label` that isn't itself a label."""
        for i, line in enumerate(lines):
            if line.rstrip(":") == label:
                for j in range(i + 1, min(i + 6, len(lines))):
                    val = lines[j].strip()
                    if val and val.rstrip(":") not in STOPS:
                        return val
        return ""

    def _block(label: str) -> list[str]:
        """Return all lines after `label` until the next known label."""
        collecting = False
        result: list[str] = []
        for line in lines:
            if line.rstrip(":") == label:
                collecting = True
                continue
            if collecting:
                if line.rstrip(":") in STOPS:
                    break
                result.append(line)
        return result

    def _person(label: str) -> tuple[str, str]:
        block = _block(label)
        if not block:
            return "", ""
        return block[0], ", ".join(block[1:])

    # Committee name is the first bold/strong element
    name_tag = soup.find(["strong", "b"])
    name     = name_tag.get_text(strip=True) if name_tag else ""

    chair_name, chair_addr = _person("Chairperson")
    treas_name, treas_addr = _person("Treasurer")
    agent_name, agent_addr = _person("Registered Agent")

    addr_block = _block("Address")
    address    = ", ".join(addr_block)

    phone = _after("Phone")
    phone = re.sub(r"(?i)^phone:\s*", "", phone).strip()

    return {
        "account_id":               account_id,
        "name":                     name,
        "type":                     _after("Type"),
        "status":                   _after("Status"),
        "address":                  address,
        "phone":                    phone,
        "chairperson_name":         chair_name,
        "chairperson_address":      chair_addr,
        "treasurer_name":           treas_name,
        "treasurer_address":        treas_addr,
        "registered_agent_name":    agent_name,
        "registered_agent_address": agent_addr,
        "purpose":                  _after("Purpose"),
        "affiliates":               _after("Affiliates"),
        "scraped_at":               datetime.today().strftime("%Y-%m-%d"),
    }


def scrape_committee_details(session: requests.Session, log,
                             force: bool = False) -> tuple[int, int]:
    """Phase 2: scrape ComDetail.asp for each ID in fl_committee_links.csv.

    Appends to fl_committee_details.csv incrementally.
    Returns (ok, err) counts.
    """
    done = load_manifest()
    ok = err = 0

    if not COMMITTEE_LINKS_PATH.exists():
        log.warning("  [!] fl_committee_links.csv not found — run phase 1 first")
        return 0, 1

    with open(COMMITTEE_LINKS_PATH, newline="", encoding="utf-8") as f:
        links = list(csv.DictReader(f))

    to_scrape = [
        lnk for lnk in links
        if force or ("committee_detail", lnk["account_id"]) not in done
    ]

    log.info(f"\nFlorida committee details (phase 2):")
    log.info(f"  {len(links):,} total, {len(to_scrape):,} to scrape")

    existing_count = 0
    write_header   = force or not COMMITTEE_DETAILS_PATH.exists()
    if force and COMMITTEE_DETAILS_PATH.exists():
        COMMITTEE_DETAILS_PATH.unlink()
    if COMMITTEE_DETAILS_PATH.exists() and not force:
        with open(COMMITTEE_DETAILS_PATH, newline="", encoding="utf-8") as f:
            existing_count = sum(1 for _ in csv.DictReader(f))

    out_f  = open(COMMITTEE_DETAILS_PATH, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=COMMITTEE_DETAIL_COLS,
                            extrasaction="ignore", restval="")
    if write_header:
        writer.writeheader()

    try:
        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(to_scrape, desc="  committee details", unit="cmte",
                      dynamic_ncols=True, colour="green") as bar:
                for lnk in bar:
                    acct = lnk["account_id"]
                    bar.set_postfix_str(f"id={acct}", refresh=False)

                    try:
                        r = session.get(
                            f"{BASE_URL}/committees/ComDetail.asp",
                            params={"account": acct}, timeout=15,
                        )
                        # 302 redirect = non-existent account
                        if r.history and r.history[0].status_code == 302:
                            done.add(("committee_detail", acct))
                            continue
                        r.raise_for_status()

                        detail = _parse_committee_detail(r.text, acct)
                        writer.writerow(detail)
                        out_f.flush()

                        upsert_manifest({
                            "relation_type": "committee_detail",
                            "key":           acct,
                            "filename":      "fl_committee_details.csv",
                            "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                            "row_count":     1,
                        })
                        done.add(("committee_detail", acct))
                        ok += 1
                        time.sleep(SLEEP)

                    except Exception as e:
                        log.page_scrape_error(entity="committee",
                                              page_id=acct, error=str(e))
                        err += 1
                        time.sleep(SLEEP * 3)
    finally:
        out_f.close()

    log.page_scrape_complete(
        filename=str(COMMITTEE_DETAILS_PATH),
        rows=existing_count + ok,
        duration_s=0, ok=ok, err=err,
    )
    return ok, err


# ==================== committee entities — update ====================

def download_active_committees(session: requests.Session, log) -> int:
    """Download the active-committee bulk file (update mode only).

    POSTs to extractComList.asp and saves the tab-delimited result as
    fl_committees_active.txt. The parser compares AcctNums in this file
    against the manifest to identify newly-inactive committees.
    Returns row count.
    """
    filename = "fl_committees_active.txt"
    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        session.get(f"{BASE_URL}/committees/downloadcomlist.asp", timeout=15)
        r = session.post(
            f"{BASE_URL}/committees/extractComList.asp",
            data={"FormSubmit": "Download"},
            headers={"Referer": f"{BASE_URL}/committees/downloadcomlist.asp"},
            timeout=30,
        )
        r.raise_for_status()
        COMMITTEE_ACTIVE_PATH.write_bytes(r.content)
    except Exception as e:
        log.file_download_error(filename=filename, error=str(e))
        return 0

    row_count = max(0, r.text.count("\n") - 1)
    log.file_download_ok(
        filename=filename,
        bytes=COMMITTEE_ACTIVE_PATH.stat().st_size,
        rows=row_count,
        duration_s=round(time.perf_counter() - t0, 1),
    )
    upsert_manifest({
        "relation_type": "committees_active",
        "key":           datetime.today().strftime("%Y-%m-%d"),
        "filename":      filename,
        "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
        "row_count":     row_count,
    })
    return row_count


def _sync_active_to_links(log) -> int:
    """Append any new account IDs from the active bulk file to fl_committee_links.csv.

    Called after download_active_committees() in update mode so that
    scrape_committee_details() picks up newly registered committees.
    Returns count of new IDs appended.
    """
    if not COMMITTEE_ACTIVE_PATH.exists():
        return 0

    existing_ids: set[str] = set()
    existing_rows: list[dict] = []
    if COMMITTEE_LINKS_PATH.exists():
        with open(COMMITTEE_LINKS_PATH, newline="", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))
            existing_ids  = {r["account_id"] for r in existing_rows}

    new_rows: list[dict] = []
    try:
        with open(COMMITTEE_ACTIVE_PATH, newline="",
                  encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                acct = (row.get("AcctNum") or "").strip()
                if acct and acct not in existing_ids:
                    new_rows.append({
                        "account_id": acct,
                        "name":       (row.get("Name") or "").strip(),
                        "type":       (row.get("Type") or "").strip(),
                        # All entries from the active download are currently active
                        "status":     "Active",
                    })
                    existing_ids.add(acct)
    except Exception as e:
        log.warning(f"  [!] Could not read active committees file: {e}")
        return 0

    if new_rows:
        with open(COMMITTEE_LINKS_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COMMITTEE_LINKS_COLS,
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(existing_rows + new_rows)
        log.info(f"  Synced {len(new_rows)} new committee IDs from active download")

    return len(new_rows)


# ==================== candidate entities — phase 1 ===================

def _get_election_ids(session: requests.Session) -> list[tuple[str, str]]:
    """Return all (elecID_value, label) pairs from the candidate download form.

    elecID values look like '20241105-GEN', '20251209-S01', etc.
    Returned newest-first as they appear in the dropdown.
    """
    try:
        r    = session.get(f"{BASE_URL}/candidates/downloadcanlist.asp", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        sel  = soup.find("select", {"name": "elecID"})
        if not sel:
            return []
        return [
            (opt["value"].strip(), opt.get_text(strip=True))
            for opt in sel.find_all("option")
            if opt.get("value", "").strip()
        ]
    except Exception:
        return []


def _elec_slug(label: str) -> str:
    """Convert an election label to a safe filename slug.

    e.g. '2026 Election'           -> '2026_election'
         '2025 Special: House 40'  -> '2025_special_house_40'
    """
    return re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")


def download_candidate_bulk(session: requests.Session, log,
                            force: bool = False,
                            recent_only: bool = False) -> int:
    """Phase 1: download tab-delimited candidate list per election cycle.

    Each file contains: AcctNum, VoterID, ElectionID, OfficeCode,
    OfficeDesc, Juris1num, Juris2num, StatusCode, StatusDesc, PartyCode,
    PartyName, NameLast, NameFirst, NameMiddle, SuppressAddress, Addr1,
    Addr2, City, State, Zip, CountyCode, Phone, TrsNameLast, TrsNameFirst,
    TrsNameMiddle, Email.

    recent_only=True: download only the two most recent general elections
    plus all specials from the current calendar year.
    Returns count of files downloaded.
    """
    done      = load_manifest()
    ok        = 0
    elections = _get_election_ids(session)

    if not elections:
        log.warning("  [!] Could not load election list — skipping candidate bulk")
        return 0

    if recent_only:
        current_year = datetime.today().year
        # Exclude future elections — the dropdown lists upcoming cycles before
        # they've started (e.g. 2028, 2030 appear now). elecID starts with YYYY.
        past_or_current = [e for e in elections
                           if int(e[0][:4]) <= current_year]
        generals = [e for e in past_or_current if "special" not in e[1].lower()]
        specials  = [e for e in past_or_current if "special"     in e[1].lower()]
        elections = generals[:2] + [e for e in specials
                                    if str(current_year) in e[1]]

    log.info(f"\nFlorida candidate bulk ({len(elections)} elections):")

    for elec_id, label in elections:
        slug     = _elec_slug(label)
        filename = f"fl_candidates_{slug}.txt"
        key      = ("candidate_bulk", elec_id)
        out_path = RAW_DIR / filename

        if key in done and not force and out_path.exists():
            log.file_download_skip(filename=filename)
            continue

        log.file_download_start(filename=filename)
        t0 = time.perf_counter()

        try:
            session.get(f"{BASE_URL}/candidates/downloadcanlist.asp", timeout=15)
            r = session.post(
                f"{BASE_URL}/candidates/extractCanList.asp",
                data={
                    "elecID":      elec_id,
                    "office":      "All",
                    "status":      "All",   # all statuses: active, defeated, etc.
                    "cantype":     "STA",   # state candidates only (local = county level)
                    "FormSubmit":  "Download",
                },
                headers={"Referer": f"{BASE_URL}/candidates/downloadcanlist.asp"},
                timeout=30,
            )
            r.raise_for_status()
            out_path.write_bytes(r.content)
        except Exception as e:
            log.file_download_error(filename=filename, error=str(e))
            time.sleep(SLEEP * 3)
            continue

        if not out_path.exists() or out_path.stat().st_size == 0:
            log.file_download_error(filename=filename, error="empty response")
            continue

        row_count = max(
            0, out_path.read_text(encoding="utf-8", errors="replace").count("\n") - 1
        )
        log.file_download_ok(
            filename=filename,
            bytes=out_path.stat().st_size,
            rows=row_count,
            duration_s=round(time.perf_counter() - t0, 1),
            label=label,
        )
        upsert_manifest({
            "relation_type": "candidate_bulk", "key": elec_id,
            "filename":      filename,
            "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
            "row_count":     row_count,
        })
        done.add(key)
        ok += 1
        time.sleep(SLEEP)

    return ok




# ========================== transactions =============================

def _date_chunks(start: date, end: date, days: int) -> list[tuple[date, date]]:
    """Generate non-overlapping windows of `days` covering [start, end]."""
    chunks: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        chunks.append((cur, min(cur + timedelta(days=days - 1), end)))
        cur += timedelta(days=days)
    return chunks


def _get_election_options(page) -> list[tuple[str, str]]:
    """Parse all (value, label) pairs from the election <select> on the current form page."""
    options = page.evaluate("""
        () => Array.from(
            document.querySelectorAll('select[name="election"] option')
        ).map(o => [o.value, o.textContent.trim()])
    """)
    return [(v, lbl) for v, lbl in options if v and v != ""]


def _fill_txn_form(page, txn_type: str,
                   date_from: date | None = None,
                   date_to:   date | None = None,
                   election_id: str = "All",
                   amount_min: str = "",
                   amount_max: str = "") -> None:
    """Fill the transaction search form in the current Playwright page.

    For contributions/transfers/other:
      - election=All, search_on=1 (contributor list), date range required,
        blank contributor name → all records in window.

    For expenditures:
      - expend.exe rejects election=All + blank criteria as "Invalid Date Range".
      - Instead: pass a specific election_id, search_on=2 (payee list),
        no date range → all expenditures for that election.
      - If sub-chunking within an election, also provide date_from/date_to.
    """
    page.select_option('select[name="election"]', value=election_id)

    # contributions: search_on=1 (contributor list, blank name = all).
    # expenditures/transfers: search_on=2 (list under payee/candidate section).
    # other: no search_on radio — form goes straight to recipient list.
    search_on_val = None
    if txn_type == "contributions":
        search_on_val = "1"
    elif txn_type in ("expenditures", "transfers"):
        search_on_val = "2"
    # "other" has no search_on radio — skip

    if search_on_val is not None:
        radio = page.locator(f'input[name="search_on"][value="{search_on_val}"]')
        if radio.count() > 0:
            radio.first.click()

    # Date range — only fill if provided (expenditures by election don't need it
    # unless sub-chunking within a dense election).
    for field, val in [
        (TXN_FORM_FIELDS["date_from"], date_from.strftime("%m/%d/%Y") if date_from else ""),
        (TXN_FORM_FIELDS["date_to"],   date_to.strftime("%m/%d/%Y")   if date_to   else ""),
    ]:
        loc = page.locator(f'input[name="{field}"]').first
        loc.wait_for(state="visible", timeout=10_000)
        loc.fill(val)

    for field, val in [
        (TXN_FORM_FIELDS["amount_min"], amount_min),
        (TXN_FORM_FIELDS["amount_max"], amount_max),
    ]:
        loc = page.locator(f'input[name="{field}"]').first
        if loc.count() > 0:
            loc.fill(val)

    page.locator(f'input[name="{TXN_FORM_FIELDS["rowlimit"]}"]').first.fill(str(ROW_LIMIT))

    qf = page.locator(f'input[name="{TXN_FORM_FIELDS["queryformat"]}"][value="2"]')
    qf.wait_for(state="attached", timeout=5_000)
    qf.click()


# Navigation errors that indicate a stale browser session rather than a
# transient chunk-level problem. When these appear consecutively the session
# needs to be restarted, not just retried.
_NAV_ERROR_PHRASES = (
    "ERR_TOO_MANY_REDIRECTS",
    "ERR_HTTP_RESPONSE_CODE_FAILURE",
    "net::ERR_",
    "Timeout",
)

def _is_nav_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(p in msg for p in _NAV_ERROR_PHRASES)


def _fetch_chunk_playwright(page, txn_type: str,
                            date_from: date | None, date_to: date | None,
                            tmp_path: Path,
                            log,
                            max_retries: int = 3,
                            election_id: str = "All",
                            amount_min: str = "",
                            amount_max: str = "") -> int:
    """Navigate the form, submit, save the download to tmp_path.

    Retries up to max_retries times with a fresh page.goto() on each attempt —
    the site occasionally returns an error page (invalid date range, session
    issue) instead of a file download, requiring a full form reload to recover.

    Returns the row count (header line excluded).
    Raises on navigation or download failure after all retries exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(max_retries):
        if attempt > 0:
            time.sleep(2 + attempt)

        try:
            page.goto(TRANSACTION_FORM_PAGES[txn_type], timeout=30_000)
            page.wait_for_load_state("networkidle")
            time.sleep(1.5)

            # Save a diagnostic snapshot of the raw form (first attempt only)
            # so the page structure is visible even if the run is interrupted.
            if attempt == 0:
                try:
                    (RAW_DIR / f"_debug_{txn_type}_form.png").unlink(missing_ok=True)
                    page.screenshot(
                        path=str(RAW_DIR / f"_debug_{txn_type}_form.png"),
                        full_page=True,
                    )
                    (RAW_DIR / f"_debug_{txn_type}_form.html").write_text(
                        page.content(), encoding="utf-8"
                    )
                except Exception:
                    pass

            _fill_txn_form(page, txn_type, date_from, date_to,
                           election_id=election_id,
                           amount_min=amount_min, amount_max=amount_max)

            # Try the standard submit button; fall back to any submit input or button
            submit_selectors = [
                'input[type="submit"][name="Submit"]',
                'input[type="submit"][name="submit"]',
                'input[type="submit"]',
                'button[type="submit"]',
            ]
            submit_sel = None
            for sel in submit_selectors:
                if page.locator(sel).count() > 0:
                    submit_sel = sel
                    break
            if submit_sel is None:
                raise RuntimeError(
                    f"No submit button found on {TRANSACTION_FORM_PAGES[txn_type]}. "
                    f"Page title: {page.title()!r}"
                )

            try:
                with page.expect_download(timeout=30_000) as dl_info:
                    page.click(submit_sel)
                dl_info.value.save_as(str(tmp_path))
            except Exception as dl_exc:
                # No download event within 30s — site likely rendered an HTML
                # results/error page instead of serving a file. Capture page
                # content for the error message and re-raise.
                page.wait_for_load_state("networkidle", timeout=10_000)
                page_title = page.title()
                page_text  = page.inner_text("body")[:500].replace("\n", " ")
                raise RuntimeError(
                    f"No download triggered (30s timeout): {page_title!r} — {page_text[:200]}"
                ) from dl_exc

            if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                raise RuntimeError("empty download")

            return max(
                0,
                tmp_path.read_text(encoding="utf-8", errors="replace").count("\n") - 1,
            )

        except Exception as e:
            tmp_path.unlink(missing_ok=True)
            last_exc = e
            if _is_nav_error(e):
                raise
            # Otherwise reload and retry
            continue

    raise last_exc


def _append_chunk(src: Path, dst: Path, first: bool) -> None:
    """Append src to the year file dst, skipping src's header if not first."""
    lines = src.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    if not lines:
        return
    with open(dst, "a", encoding="utf-8") as f:
        f.writelines(lines if first else lines[1:])
    src.unlink(missing_ok=True)


# Adaptive chunk sizes in days — each level is tried when the previous hits ROW_LIMIT.
# 15 → 7 → 3 → 1. A single day hitting 32k has not been observed in Florida testing.
_CHUNK_LEVELS = [CHUNK_DAYS, 7, 3, 1]


def _fetch_and_append(page, txn_type: str,
                      chunk_start: date | None, chunk_end: date | None,
                      year_path: Path, first: list,
                      log, level: int = 0,
                      election_id: str = "All",
                      amount_min: str = "",
                      amount_max: str = "") -> tuple[int, int]:
    """Recursively fetch a date window, sub-chunking if ROW_LIMIT is hit.

    `first` is a one-element list so the mutable flag survives recursion.
    For expenditures, pass election_id and leave chunk_start/chunk_end=None
    on the first call; date sub-chunking is added on recursion if needed.
    Returns (rows_written, errors).
    """
    if chunk_start and chunk_end:
        chunk_key = f"{chunk_start:%Y%m%d}_{chunk_end:%Y%m%d}"
    else:
        chunk_key = election_id
    tmp_path = RAW_DIR / f"_tmp_{txn_type}_{chunk_key}.txt"

    try:
        row_count = _fetch_chunk_playwright(
            page, txn_type, chunk_start, chunk_end, tmp_path, log,
            election_id=election_id,
            amount_min=amount_min, amount_max=amount_max,
        )
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        # Navigation errors mean the browser session is stale — propagate so
        # the caller can restart the context and retry.
        if _is_nav_error(e):
            raise
        err_msg = str(e)
        # "No tab-delimited response" for an election-based fetch usually means
        # the server returned a "no results" page — treat as 0 rows, not a failure.
        if "No tab-delimited response" in err_msg and election_id != "All":
            log.info(f"    (no data for {chunk_key})")
            return 0, 0
        log.file_download_error(
            filename=f"{year_path.name} [{chunk_key}]", error=err_msg
        )
        time.sleep(2)
        return 0, 1

    if row_count >= ROW_LIMIT:
        _next_level = level + 1
        if _next_level >= len(_CHUNK_LEVELS):
            if amount_min or amount_max:
                # Already inside an amount-range split and still at the limit —
                # no further sub-splitting is possible. Accept truncation and warn.
                log.warning(
                    f"    [!] {chunk_key} ({row_count} rows) still at limit "
                    f"inside amount band ({amount_min}–{amount_max or '∞'}) "
                    f"— accepting truncated data for this band"
                )
                _append_chunk(tmp_path, year_path, first[0])
                first[0] = False
                return row_count, 0
            # At daily resolution with no amount filter — split by amount range.
            tmp_path.unlink(missing_ok=True)
            log.warning(
                f"    [!] {chunk_key} ({row_count} rows) still at limit "
                f"at 1-day resolution — splitting by amount range"
            )
            total_rows = total_err = 0
            for rng_label, amin, amax in AMOUNT_RANGES:
                rows, errs = _fetch_and_append(
                    page, txn_type, chunk_start, chunk_end,
                    year_path, first, log, level=_next_level,
                    election_id=election_id,
                    amount_min=amin, amount_max=amax,
                )
                total_rows += rows
                total_err  += errs
            return total_rows, total_err

        tmp_path.unlink(missing_ok=True)

        # The FL expenditure form only supports specific election_id + no date
        # range. Date-range queries ("election=All" or election_id + dates) both
        # return "Invalid Date Range". When an election hits ROW_LIMIT, the only
        # way to split is by amount range — jump straight to that.
        if chunk_start is None or chunk_end is None:
            log.warning(
                f"    [!] {chunk_key} ({row_count} rows) hit row limit "
                f"— expenditure form does not support date ranges, "
                f"splitting by amount range (election={election_id!r})"
            )
            total_rows = total_err = 0
            for rng_label, amin, amax in AMOUNT_RANGES:
                rows, errs = _fetch_and_append(
                    page, txn_type, None, None,
                    year_path, first, log, level=_next_level,
                    election_id=election_id,
                    amount_min=amin, amount_max=amax,
                )
                total_rows += rows
                total_err  += errs
            return total_rows, total_err

        # Date-based sub-chunking (contributions/transfers/other only)
        next_days = _CHUNK_LEVELS[_next_level]
        log.warning(
            f"    [!] {chunk_key} ({row_count} rows) hit row limit "
            f"— splitting into {next_days}-day sub-chunks"
        )
        total_rows = total_err = 0
        for sc_start, sc_end in _date_chunks(chunk_start, chunk_end, next_days):
            rows, errs = _fetch_and_append(
                page, txn_type, sc_start, sc_end,
                year_path, first, log, level=_next_level,
                election_id=election_id,
                amount_min=amount_min, amount_max=amount_max,
            )
            total_rows += rows
            total_err  += errs
        return total_rows, total_err

    _append_chunk(tmp_path, year_path, first[0])
    first[0] = False
    time.sleep(1)
    return row_count, 0


def _new_page(p):
    """Create a fresh browser context and page with accept_downloads=True."""
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(accept_downloads=True)
    return browser, context.new_page()


def download_transactions(p, log,
                          force: bool = False,
                          start_year: int | None = None,
                          end_year: int | None = None,
                          contributions: bool = False,
                          expenditures: bool = False) -> tuple[int, int]:
    """Download all four transaction types in 7-day chunks via Playwright.

    Output: one file per year per transaction type — fl_{type}_{year}.txt.
    All 7-day chunks for a year are appended into that single file, with
    the header written only for the first chunk.

    Adaptive sub-chunking: a chunk returning exactly ROW_LIMIT rows is
    re-requested as 3-day sub-windows to avoid truncation.

    Force: 1996 through today (all year files rebuilt from scratch).
    Update: current calendar year only (year file deleted and rebuilt).
    start_year/end_year: restrict to this year range; re-downloads all in-range years.
    contributions/expenditures: restrict to those transaction types.

    Returns (ok, err) counts.
    """
    done                = load_manifest()
    ok = err            = 0
    today               = date.today()
    current_year        = today.year
    year_range_explicit = start_year is not None or end_year is not None

    if force:
        range_start = START_DATE.year
        range_end   = current_year
    elif year_range_explicit:
        range_start = start_year if start_year is not None else START_DATE.year
        range_end   = end_year   if end_year   is not None else current_year
    else:
        range_start = range_end = current_year

    years_list = list(range(range_start, range_end + 1))

    if contributions and not expenditures:
        active_types = [t for t in TRANSACTION_TYPES if t == "contributions"]
    elif expenditures and not contributions:
        active_types = [t for t in TRANSACTION_TYPES if t == "expenditures"]
    else:
        active_types = TRANSACTION_TYPES

    log.info(
        f"\nFlorida transactions "
        f"({len(years_list)} years × {len(TRANSACTION_TYPES)} types):"
    )

    browser, page = _new_page(p)

    try:
        for txn_type in active_types:
            log.info(f"  {txn_type}:")

            for year in years_list:
                year_filename = f"fl_{txn_type}_{year}.txt"
                year_path     = RAW_DIR / year_filename
                year_key      = (txn_type, str(year))

                # Past years already complete — skip unless force or year range explicit
                if year < current_year and year_key in done and not force and not year_range_explicit:
                    log.file_download_skip(filename=year_filename)
                    continue

                # Current year (or force): delete existing file and rebuild
                if year_path.exists():
                    year_path.unlink()
                # Also clear any manifest entries for this year's chunks
                strip_manifest(
                    lambda r, t=txn_type, y=year: not (
                        r["relation_type"] == t
                        and r["key"].startswith(str(y))
                    )
                )
                done = load_manifest()

                log.file_download_start(filename=year_filename)
                t0        = time.perf_counter()
                year_rows = 0

                first_flag = [True]   # mutable flag passed into recursive helper

                if txn_type in ELECTION_BASED_TYPES:
                    # expend.exe / FundXfers.exe / OtherDist.exe all reject
                    # election=All + blank criteria as "Invalid Date Range".
                    # Iterate by specific election ID instead.
                    try:
                        url = TRANSACTION_FORM_PAGES[txn_type]
                        page.goto(url, timeout=45_000)
                        page.wait_for_load_state("networkidle", timeout=20_000)
                        all_elections = _get_election_options(page)
                    except Exception as nav_e:
                        log.warning(
                            f"    [!] Navigation error fetching election list "
                            f"— restarting ({type(nav_e).__name__})"
                        )
                        try:
                            browser.close()
                        except Exception:
                            pass
                        time.sleep(3)
                        browser, page = _new_page(p)
                        try:
                            page.goto(url, timeout=45_000)
                            page.wait_for_load_state("networkidle", timeout=20_000)
                            all_elections = _get_election_options(page)
                        except Exception as nav_e2:
                            log.file_download_error(
                                filename=year_filename,
                                error=f"Can't load form: {nav_e2}",
                            )
                            err += 1
                            continue

                    year_elections = [
                        (v, lbl) for v, lbl in all_elections
                        if v != "All" and v[:4] == str(year)
                    ]
                    if not year_elections:
                        log.warning(f"    [!] No elections found for {year} — skipping")
                        continue
                    log.info(f"    {len(year_elections)} election(s) for {year}")
                    items_to_fetch: list = [(None, None, elec_id)
                                            for elec_id, _ in year_elections]
                else:
                    year_start = max(date(year, 1, 1), START_DATE)
                    year_end   = min(date(year, 12, 31), today)
                    items_to_fetch = [
                        (cs, ce, "All")
                        for cs, ce in _date_chunks(year_start, year_end, CHUNK_DAYS)
                    ]

                for chunk_start, chunk_end, election_id in items_to_fetch:
                    try:
                        rows, errs = _fetch_and_append(
                            page, txn_type, chunk_start, chunk_end,
                            year_path, first_flag, log, level=0,
                            election_id=election_id,
                        )
                    except Exception as e:
                        # Navigation error — browser session stale. Restart and retry once.
                        log.warning(
                            f"    [!] Browser session error — restarting "
                            f"({type(e).__name__})"
                        )
                        try:
                            browser.close()
                        except Exception:
                            pass
                        time.sleep(3)
                        browser, page = _new_page(p)
                        try:
                            rows, errs = _fetch_and_append(
                                page, txn_type, chunk_start, chunk_end,
                                year_path, first_flag, log, level=0,
                                election_id=election_id,
                            )
                        except Exception as e2:
                            chunk_label = election_id if txn_type in ELECTION_BASED_TYPES \
                                else f"{chunk_start:%Y%m%d}_{chunk_end:%Y%m%d}"
                            log.file_download_error(
                                filename=f"{year_filename} [{chunk_label}]",
                                error=str(e2),
                            )
                            rows, errs = 0, 1
                    year_rows += rows
                    err       += errs

                if year_path.exists() and year_path.stat().st_size > 0:
                    log.file_download_ok(
                        filename=year_filename,
                        bytes=year_path.stat().st_size,
                        rows=year_rows,
                        duration_s=round(time.perf_counter() - t0, 1),
                        year=year,
                    )
                    upsert_manifest({
                        "relation_type": txn_type, "key": str(year),
                        "filename":      year_filename,
                        "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                        "row_count":     year_rows,
                    })
                    done.add(year_key)
                    ok += 1
                else:
                    log.file_download_error(
                        filename=year_filename, error="no data written"
                    )
                    err += 1

    finally:
        try:
            browser.close()
        except Exception:
            pass

    return ok, err


# ============================ orchestrator ===========================

def run(force: bool = False, entities: bool = False,
        transactions: bool = False,
        start_year: int | None = None, end_year: int | None = None,
        contributions: bool = False, expenditures: bool = False) -> None:
    """Download Florida campaign finance data.

    Force/empty run:
      Entities   — A-Z committee search + all detail pages; all election
                   years for candidate bulk + all candidate detail pages.
      Transactions — 1996-01-01 through today, 7-day chunks (Playwright).

    Update run (default, no flags):
      Entities   — active committee bulk diff + new detail pages;
                   two most recent generals + current-year specials,
                   new candidate detail pages only.
      Transactions — current calendar year only.
    """
    log = get_logger("florida", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Florida scraper")
    log._emit("scrape_started", force=force,
              entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures)

    transactions_implied = contributions or expenditures
    do_both         = not entities and not transactions and not transactions_implied
    do_transactions = transactions or transactions_implied or do_both
    do_entities     = entities     or do_both

    if force:
        if do_both:
            if MANIFEST.exists():
                MANIFEST.unlink()
        elif do_transactions:
            strip_manifest(lambda r: r["relation_type"] not in TRANSACTION_RELATIONS)
        else:
            strip_manifest(lambda r: r["relation_type"] in TRANSACTION_RELATIONS)

    files_ok = files_err = 0

    try:
        session = get_session()

        # ── Entities (requests) ──────────────────────────────────────
        if do_entities:
            if force:
                # Full committee sweep: A-Z links then all detail pages
                scrape_committee_links(session, log, force=True)
                c_ok, c_err = scrape_committee_details(session, log, force=True)
            else:
                # Update: active bulk download → sync new IDs → new details only
                download_active_committees(session, log)
                _sync_active_to_links(log)
                c_ok, c_err = scrape_committee_details(session, log, force=False)

            files_ok  += c_ok
            files_err += c_err

            # Candidate bulk download then detail pages
            download_candidate_bulk(
                session, log,
                force=force, recent_only=(not force),
            )

        # ── Transactions (Playwright) ────────────────────────────────
        if do_transactions:
            try:
                from playwright.sync_api import sync_playwright
            except ImportError:
                log.error(
                    "[!] Playwright not installed — "
                    "run: pip install playwright && playwright install chromium"
                )
                log._emit(
                    "scrape_completed", status="error",
                    duration_s=round(time.perf_counter() - t0, 1),
                    files_ok=files_ok, files_err=files_err,
                    error="playwright not installed",
                )
                return

            with sync_playwright() as p:
                t_ok, t_err = download_transactions(
                    p, log, force=force,
                    start_year=start_year, end_year=end_year,
                    contributions=contributions, expenditures=expenditures,
                )
                files_ok  += t_ok
                files_err += t_err

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} errors")
        log._emit("scrape_completed", status="completed",
                  duration_s=duration,
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
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


# ============================== CLI ==================================

if __name__ == "__main__":
    import argparse

    ap   = argparse.ArgumentParser(description="Download Florida campaign finance data.")
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",        action="store_true",
                      help="re-download everything (1996–today), ignoring the manifest")
    vert.add_argument("--start-year",   type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); re-downloads all in-range years")
    ap.add_argument("--end-year",       type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")
    ap.add_argument("--transactions",   action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",       action="store_true",
                    help="entities only (committees, candidates)")
    ap.add_argument("--contributions",  action="store_true",
                    help="contributions only")
    ap.add_argument("--expenditures",   action="store_true",
                    help="expenditures only")
    args, _ = ap.parse_known_args()
    cy = date.today().year
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
