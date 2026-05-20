"""
Colorado TRACER scraper
=======================
Three components, one file:

  1. Transactions — bulk ZIP downloads from the BulkDataDownloads endpoint
       Years 2000–present × {contributions, expenditures, loans}

  2. Candidates — SeqID scraper across CandidateDetail.aspx
       Iterates SeqID 1 → MAX_SEQ_ID, one row per candidate/cycle.
       Resumable via checkpoint file.

  3. Committees — OrgID brute-force scraper across CommitteeDetail.aspx
       Iterates OrgID 1 → COMM_MAX_ORG_ID (same pattern as candidates).
       Returns None for gaps; resumable via checkpoint file.

Usage:
    python colorado.py                           # run all three (skips already-done)
    python colorado.py --force                   # wipe everything, re-download
    python colorado.py --update                  # re-fetch current-year transactions + re-scrape
    python colorado.py --transactions            # transactions only
    python colorado.py --candidates              # candidate SeqID scraper only
    python colorado.py --committees              # committee OrgID scraper only
    python colorado.py --candidates --force      # reset candidate scrape from SeqID 1
    python colorado.py --candidates --start 40000  # resume candidates from a specific SeqID
    python colorado.py --committees --start-org 10000  # resume committees from OrgID 10000
    python colorado.py --workers 8               # parallel workers (default 8)
"""

import csv
import io
import time
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT       = Path(__file__).resolve().parents[3]
RAW_DIR            = PROJECT_ROOT / "data" / "Colorado" / "raw"
MANIFEST_TX         = PROJECT_ROOT / "data" / "Colorado" / "manifest.csv"
ENTITIES_OUT        = RAW_DIR / "candidates_all.csv"
ENTITIES_CHECKPOINT = RAW_DIR / "candidates_all.checkpoint"
COMMITTEES_OUT      = RAW_DIR / "committees.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

# ── Shared config ─────────────────────────────────────────────────────────────
CURRENT_YEAR = datetime.today().year

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

TX_BASE_URL = "https://Tracer.sos.colorado.gov/PublicSite/Docs/BulkDataDownloads"

DATA_TYPES = [
    ("Contribution", "contributions"),
    ("Expenditure",  "expenditures"),
    ("Loan",         "loans"),
]

TX_START_YEAR  = 2000
TX_YEARS       = list(range(TX_START_YEAR, CURRENT_YEAR + 1))
TX_MANIFEST_COLS = ["year", "data_type", "filename", "downloaded_at", "row_count"]


# ── Transaction manifest helpers ──────────────────────────────────────────────
def tx_load_manifest() -> set[tuple[str, str]]:
    if not MANIFEST_TX.exists():
        return set()
    with open(MANIFEST_TX, newline="") as f:
        return {(r["year"], r["data_type"]) for r in csv.DictReader(f)}


def tx_strip_manifest(keep_fn):
    if not MANIFEST_TX.exists():
        return
    with open(MANIFEST_TX, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST_TX, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TX_MANIFEST_COLS)
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))


def tx_append_manifest(record: dict):
    write_header = not MANIFEST_TX.exists()
    with open(MANIFEST_TX, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TX_MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow(record)


# ── Transaction download ──────────────────────────────────────────────────────
def tx_download(year: int, url_label: str, file_label: str,
                session: requests.Session) -> tuple[str, int] | None:
    zip_url  = f"{TX_BASE_URL}/{year}_{url_label}Data.csv.zip"
    filename = f"{file_label}_{year}.csv"
    out_path = RAW_DIR / filename

    try:
        resp = session.get(zip_url, timeout=120, stream=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"failed: {e}")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            raw_csv = zf.read(zf.namelist()[0])
    except (zipfile.BadZipFile, IndexError) as e:
        print(f"zip error: {e}")
        return None

    if raw_csv[:3] == b"\xef\xbb\xbf":
        text = raw_csv[3:].decode("utf-8", errors="replace")
    elif raw_csv[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw_csv.decode("utf-16")
    elif len(raw_csv) > 1 and raw_csv[1] == 0:
        text = raw_csv.decode("utf-16-le", errors="replace")
    else:
        text = raw_csv.decode("utf-8", errors="replace")

    out_path.write_text(text, encoding="utf-8")
    return filename, max(text.count("\n") - 1, 0)


# ── Transaction runner ────────────────────────────────────────────────────────
def run_transactions(force: bool = False, update: bool = False):
    current_year_str = str(CURRENT_YEAR)

    if force:
        if MANIFEST_TX.exists():
            MANIFEST_TX.unlink()
        done = set()
    elif update:
        tx_strip_manifest(lambda r: r["year"] != current_year_str)
        done = tx_load_manifest()
    else:
        done = tx_load_manifest()

    session = requests.Session()
    session.headers.update({**HEADERS, "Referer": "https://Tracer.sos.colorado.gov/PublicSite/"})

    total_new = 0
    for year in TX_YEARS:
        for url_label, file_label in DATA_TYPES:
            key = (str(year), file_label)
            if key in done and year != CURRENT_YEAR:
                print(f"  Colorado {file_label} {year}: already downloaded — skipping")
                continue

            print(f"  Colorado {file_label} {year}...", end=" ", flush=True)
            result = tx_download(year, url_label, file_label, session)

            if result is None:
                print(f"  (no file for {year} {file_label}, skipping)")
                continue

            filename, row_count = result
            print(f"→ {filename} ({row_count:,} rows)")
            tx_append_manifest({
                "year":          str(year),
                "data_type":     file_label,
                "filename":      filename,
                "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                "row_count":     row_count,
            })
            done.add(key)
            total_new += 1
            time.sleep(0.3)

    print(f"Colorado transactions: done — {total_new} new file(s) downloaded.")


# ═══════════════════════════════════════════════════════════════════════════════
# PART 2 — ENTITIES (SeqID scraper)
# ═══════════════════════════════════════════════════════════════════════════════

ENTITY_URL       = "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CandidateDetail.aspx"
KNOWN_MAX_SEQ_ID = 67819   # last verified ceiling — binary search starts here
SLEEP_SEC        = 0.25


# ── Binary-search max ID finder ───────────────────────────────────────────────

def find_max_id(fetch_fn, known_max: int, step: int = 1000, label: str = "ID") -> int:
    """
    Return the highest consecutive valid ID above (or at) known_max.

    Algorithm:
      1. If known_max itself is invalid, binary-search downward to find the
         true ceiling (handles the case where the stored constant is stale).
      2. Otherwise step upward by `step` until we overshoot, then binary-
         search the bracket [last_valid, first_invalid] to find the exact edge.

    fetch_fn(id) must return truthy for a valid page, falsy for a gap/404.
    """
    print(f"  Auto-detecting max {label} (starting from {known_max:,}) …", flush=True)

    # ── Step 1: check the anchor ──────────────────────────────────────────────
    if not fetch_fn(known_max):
        # known_max is already past the end — search downward
        lo, hi = max(1, known_max // 2), known_max
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if fetch_fn(mid):
                lo = mid
            else:
                hi = mid - 1
        print(f"  Max {label}: {lo:,}")
        return lo

    # ── Step 2: step upward to find upper bound ───────────────────────────────
    lo = known_max
    hi = known_max + step
    while fetch_fn(hi):
        lo = hi
        hi += step

    # ── Step 3: binary search [lo, hi] ───────────────────────────────────────
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if fetch_fn(mid):
            lo = mid
        else:
            hi = mid

    print(f"  Max {label}: {lo:,}")
    return lo

ENTITY_COLS = [
    "seq_id",
    "name",
    "candidate_id",
    "address1",
    "address2",
    "city_state_zip",
    "phone",
    "email",
    "web",
    "election_cycle",
    "election_year",
    "party",
    "jurisdiction",
    "office",
    "district",
    "cycle_status",
    "current_status",
    "term_date",
    "date_affidavit_filed",
    "voluntary_spending_limit",
    "scraped_at",
]


# ── Entity helpers ────────────────────────────────────────────────────────────
def _txt(soup, id_: str) -> str:
    tag = soup.find(id=id_)
    return tag.text.strip() if tag else ""


def parse_candidate_page(seq_id: int, html: str) -> list[dict] | None:
    """
    Returns one dict per election cycle, or None if the SeqID is a gap.
    """
    soup = BeautifulSoup(html, "html.parser")
    name = _txt(soup, "_ctl0_Content_lblCandName")
    if not name:
        return None

    base = {
        "seq_id":                   seq_id,
        "name":                     name,
        "candidate_id":             _txt(soup, "_ctl0_Content_lblCandidateID"),
        "address1":                 _txt(soup, "_ctl0_Content_lblCandMailAddress1"),
        "address2":                 _txt(soup, "_ctl0_Content_lblCandMailAddress2"),
        "city_state_zip":           _txt(soup, "_ctl0_Content_lblCandMailCityStateZip"),
        "phone":                    _txt(soup, "_ctl0_Content_lblCandPhone"),
        "email":                    _txt(soup, "_ctl0_Content_lnkCandEmail"),
        "web":                      _txt(soup, "_ctl0_Content_lnkCandWeb"),
        "current_status":           _txt(soup, "_ctl0_Content_lblCandStatus"),
        "term_date":                _txt(soup, "_ctl0_Content_lblCandTermDate"),
        "date_affidavit_filed":     _txt(soup, "_ctl0_Content_lblCandDateDeclared"),
        "voluntary_spending_limit": _txt(soup, "_ctl0_Content_lblCandVolSpendLimit"),
        "scraped_at":               datetime.today().strftime("%Y-%m-%d"),
    }

    rows = []
    campaigns_table = soup.find("table", {"id": "_ctl0_Content_dgdCampaigns"})
    if campaigns_table:
        for tr in campaigns_table.find_all("tr")[1:]:
            cells = [td.text.strip() for td in tr.find_all("td")]
            if len(cells) < 7:
                continue
            cycle = cells[1]
            rows.append({
                **base,
                "election_cycle": cycle,
                "election_year":  cycle.split()[0] if cycle else "",
                "party":          cells[2],
                "jurisdiction":   cells[3],
                "office":         cells[4],
                "district":       cells[5],
                "cycle_status":   cells[6],
            })

    if not rows:
        header = _txt(soup, "_ctl0_Content_lblPageHeader")
        rows.append({
            **base,
            "election_cycle": "",
            "election_year":  header.split("Election Year")[-1].strip() if "Election Year" in header else "",
            "party":          _txt(soup, "_ctl0_Content_lblCandParty"),
            "jurisdiction":   _txt(soup, "_ctl0_Content_lblCandJurisdiction"),
            "office":         _txt(soup, "_ctl0_Content_lblCandOffice"),
            "district":       _txt(soup, "_ctl0_Content_lblCandDistrict"),
            "cycle_status":   _txt(soup, "_ctl0_Content_lblCampaignStatus"),
        })

    return rows


# ── Thread-local sessions ─────────────────────────────────────────────────────
_thread_local = threading.local()

def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


def _fetch_seq(seq_id: int) -> list[dict] | None:
    session = _get_session()
    try:
        r = session.get(ENTITY_URL, params={"SeqID": seq_id}, timeout=30)
        r.raise_for_status()
    except requests.RequestException:
        return None
    time.sleep(SLEEP_SEC)
    return parse_candidate_page(seq_id, r.text)


# ── Entity checkpoint + writer ────────────────────────────────────────────────
def _load_checkpoint() -> int:
    if ENTITIES_CHECKPOINT.exists():
        try:
            return int(ENTITIES_CHECKPOINT.read_text().strip())
        except ValueError:
            pass
    return 0


def _save_checkpoint(seq_id: int):
    ENTITIES_CHECKPOINT.write_text(str(seq_id))


_write_lock = threading.Lock()

def _write_rows(rows: list[dict]):
    if not rows:
        return
    write_header = not ENTITIES_OUT.exists()
    with _write_lock:
        with open(ENTITIES_OUT, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=ENTITY_COLS)
            if write_header:
                w.writeheader()
            w.writerows(rows)


# ── Entity runner ─────────────────────────────────────────────────────────────
def run_entities(force: bool = False, start_seq: int = 0,
                 max_seq: int | None = None, workers: int = 8):
    if force:
        for f in [ENTITIES_OUT, ENTITIES_CHECKPOINT]:
            if f.exists():
                f.unlink()

    if max_seq is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        def _probe_seq(sid):
            try:
                r = session.get(ENTITY_URL, params={"SeqID": sid}, timeout=30)
                r.raise_for_status()
                return bool(BeautifulSoup(r.text, "html.parser").find(id="_ctl0_Content_lblCandName"))
            except Exception:
                return False
        max_seq = find_max_id(_probe_seq, KNOWN_MAX_SEQ_ID, step=1000, label="SeqID")

    checkpoint = max(_load_checkpoint(), start_seq)
    start_from = checkpoint + 1

    if start_from > max_seq:
        print(f"Colorado entities: already complete (checkpoint={checkpoint}).")
        return

    total   = max_seq - start_from + 1
    found   = 0
    skipped = 0
    CHUNK   = 200
    BATCH   = 50

    print(f"Colorado entities: SeqID {start_from} → {max_seq} ({total:,} IDs, {workers} workers)")
    print(f"Output: {ENTITIES_OUT}")

    completed     = 0
    buffer: list[dict] = []
    seq_iter      = iter(range(start_from, max_seq + 1))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {}
        for sid in seq_iter:
            pending[pool.submit(_fetch_seq, sid)] = sid
            if len(pending) >= CHUNK:
                break

        while pending:
            for future in as_completed(pending):
                sid    = pending.pop(future)
                result = future.result()
                completed += 1

                if result:
                    buffer.extend(result)
                    found += 1
                else:
                    skipped += 1

                for next_sid in seq_iter:
                    pending[pool.submit(_fetch_seq, next_sid)] = next_sid
                    break

                if completed % BATCH == 0:
                    _write_rows(buffer)
                    buffer.clear()
                    _save_checkpoint(sid)
                    pct     = completed / total * 100
                    eta_min = ((total - completed) * SLEEP_SEC / workers) / 60
                    print(f"  {completed:,}/{total:,} ({pct:.1f}%) — "
                          f"{found:,} records, {skipped:,} gaps "
                          f"— ETA ~{eta_min:.0f} min")
                break  # back to outer while to process next future

    _write_rows(buffer)
    _save_checkpoint(max_seq)
    print(f"Colorado entities: done — {found:,} records written, {skipped:,} gaps.")


# ═══════════════════════════════════════════════════════════════════════════════
# PART 3 — COMMITTEES
# ═══════════════════════════════════════════════════════════════════════════════

COMM_SEARCH_URL = "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CommitteeSearch.aspx"
COMM_DETAIL_URL = "https://tracer.sos.colorado.gov/PublicSite/SearchPages/CommitteeDetail.aspx"

COMMITTEE_COLS = [
    "org_id",
    "committee_id",
    "committee_name",
    "committee_type",
    "status",
    "date_registered",
    "date_terminated",
    "jurisdiction",
    "phone",
    "purpose",
    "registered_agent",
    "agent_phone",
    "agent_email",
    "address1",
    "city_state_zip",
    "mail_address1",
    "mail_city_state_zip",
    "dfa",
    "dfa_phone",
    "web",
    "scraped_at",
]

COMMITTEES_CHECKPOINT    = RAW_DIR / "committees.checkpoint"
KNOWN_COMM_MAX_ORG_ID   = 53_000   # last verified ceiling — binary search starts here


def _extract_tokens(html: str) -> dict:
    """Pull ASP.NET hidden form fields from a page."""
    soup = BeautifulSoup(html, "html.parser")

    def val(id_: str) -> str:
        tag = soup.find("input", {"id": id_}) or soup.find("input", {"name": id_})
        return tag["value"] if tag and tag.get("value") else ""

    sm_tag  = soup.find("input", {"id": lambda x: x and "HiddenField" in x})
    sm_name = sm_tag["name"] if sm_tag else "ctl00$ToolkitScriptManager1$HiddenField"
    sm_val  = sm_tag["value"] if sm_tag else ""

    return {
        "__VIEWSTATE":          val("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION":    val("__EVENTVALIDATION"),
        "__SCROLLPOSITIONX":    "0",
        "__SCROLLPOSITIONY":    "0",
        "__LASTFOCUS":          "",
        sm_name:                sm_val,
    }


def _fetch_committee_csv(session: requests.Session) -> str | None:
    """
    Two-step ASP.NET postback:
      1. GET CommitteeSearch → grab tokens
      2. POST blank search   → grab fresh tokens
      3. POST CSV export     → return raw CSV text
    """
    # Step 1 — GET
    try:
        r1 = session.get(COMM_SEARCH_URL, timeout=60)
        r1.raise_for_status()
    except requests.RequestException as e:
        print(f"  GET failed: {e}")
        return None

    tokens = _extract_tokens(r1.text)

    # Step 2 — POST search by status.  Blank search is rejected ("must enter
    # at least one criterion"), so we search Active + Terminated separately
    # and merge.  status_val is passed in; caller handles both values.
    def _search_one(status_val: str) -> str | None:
        search_payload = {
            **tokens,
            "__EVENTTARGET":   "",
            "__EVENTARGUMENT": "",
            "_ctl0:Content:txtCommitteeName":              "",
            "_ctl0:Content:rblCommitteeNameSearchType":    "1",
            "_ctl0:Content:ddlCommitteeType":              "",
            "_ctl0:Content:txtCommitteeID":                "",
            "_ctl0:Content:txtRegisteredAgent":            "",
            "_ctl0:Content:rblRegisteredAgentSearchType":  "1",
            "_ctl0:Content:ddlJurisdiction":               "",
            "_ctl0:Content:txtPurpose":                    "",
            "_ctl0:Content:rblPurposeSearchType":          "1",
            "_ctl0:Content:ddlStatus":                     status_val,
            "_ctl0:Content:btnSearch":                     "Search",
        }
        try:
            r2 = session.post(COMM_SEARCH_URL, data=search_payload,
                              headers={**HEADERS, "Referer": COMM_SEARCH_URL},
                              timeout=180)
            r2.raise_for_status()
        except requests.RequestException as e:
            print(f"  search POST failed: {e}")
            return None

        import re as _re
        m = _re.search(r"([\d,]+)\s+matching record", r2.text)
        label = {1: "Active", 4: "Terminated"}.get(int(status_val), status_val)
        print(f"  {label}: {m.group(1) if m else '?'} committees", end=" ", flush=True)

        # Step 3 — POST CSV export.
        # The export button uses WebForm_DoPostBackWithOptions, which sets
        # __EVENTTARGET to the button's UniqueID and submits without x/y coords.
        # The results page no longer has search fields — only use hidden tokens
        # from the results page.
        tokens2 = _extract_tokens(r2.text)
        export_payload = {
            **tokens2,
            "__EVENTTARGET":   "_ctl0:Content:ucExport:ibtnCSV",
            "__EVENTARGUMENT": "",
        }
        try:
            r3 = session.post(COMM_SEARCH_URL, data=export_payload,
                              headers={**HEADERS, "Referer": COMM_SEARCH_URL},
                              timeout=180)
            r3.raise_for_status()
        except requests.RequestException as e:
            print(f"  CSV export failed: {e}")
            return None

        if "text/html" in r3.headers.get("Content-Type", ""):
            print(f"  export returned HTML — export button didn't fire")
            return None

        raw = r3.content
        if raw[:3] == b"\xef\xbb\xbf":
            return raw[3:].decode("utf-8", errors="replace")
        return raw.decode("utf-8", errors="replace")

    # Fetch Active (1) + Terminated (4) and merge
    import io as _io
    all_rows: list[dict] = []
    for status_val in ("1", "4"):
        csv_text = _search_one(status_val)
        if csv_text:
            chunk = [{k: v for k, v in row.items() if k is not None}
                     for row in csv.DictReader(_io.StringIO(csv_text))]
            all_rows.extend(chunk)
            print(f"→ {len(chunk):,} rows")
        else:
            print("→ failed")

    print(f"  Total: {len(all_rows):,} committees combined")
    # Re-serialise as a single CSV string so the caller can use DictReader
    if not all_rows:
        return None
    import io as _io2
    buf = _io2.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(all_rows[0].keys()))
    w.writeheader()
    w.writerows(all_rows)
    return buf.getvalue()



def _scrape_committee_detail(org_id: int, session: requests.Session) -> dict | None:
    """
    Fetch CommitteeDetail.aspx?OrgID=<org_id>.
    Returns a dict of fields, or None if the page is empty (org_id unused).
    """
    try:
        r = session.get(COMM_DETAIL_URL, params={"OrgID": org_id}, timeout=30)
        r.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    def t(id_): return _txt(soup, id_)

    name = t("_ctl0_Content_lblCommName")
    if not name:
        return None   # org_id doesn't exist

    return {
        "org_id":           org_id,
        "committee_id":     t("_ctl0_Content_lblCommitteeID"),
        "committee_name":   name,
        "committee_type":   t("_ctl0_Content_lblCommitteeType"),
        "status":           t("_ctl0_Content_lblCommStatus"),
        "date_registered":  t("_ctl0_Content_lblCommDateOrganized"),
        "date_terminated":  t("_ctl0_Content_lblCommDateTerminated"),
        "jurisdiction":     t("_ctl0_Content_lblJurisdiction"),
        "phone":            t("_ctl0_Content_lblCommPhone"),
        "purpose":          t("_ctl0_Content_lblCommPurpose"),
        "registered_agent": t("_ctl0_Content_lblRegisteredAgent"),
        "agent_phone":      t("_ctl0_Content_lblAgentPhone"),
        "agent_email":      t("_ctl0_Content_lnkAgentEmail"),
        "address1":         t("_ctl0_Content_lblPhysAddress1"),
        "city_state_zip":   t("_ctl0_Content_lblPhysCityStateZip"),
        "mail_address1":    t("_ctl0_Content_lblMailAddress1"),
        "mail_city_state_zip": t("_ctl0_Content_lblMailCityStateZip"),
        "dfa":              t("_ctl0_Content_lblDFA"),
        "dfa_phone":        t("_ctl0_Content_lblDFAPhone"),
        "web":              t("_ctl0_Content_lnkCommWebAddress"),
        "scraped_at":       datetime.today().strftime("%Y-%m-%d"),
    }


_comm_write_lock = threading.Lock()


def _write_committee_rows(rows: list[dict]):
    if not rows:
        return
    write_header = not COMMITTEES_OUT.exists()
    with _comm_write_lock:
        with open(COMMITTEES_OUT, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COMMITTEE_COLS, extrasaction="ignore")
            if write_header:
                w.writeheader()
            w.writerows(rows)


def _load_comm_checkpoint() -> int:
    if COMMITTEES_CHECKPOINT.exists():
        try:
            return int(COMMITTEES_CHECKPOINT.read_text().strip())
        except ValueError:
            pass
    return 0


def _save_comm_checkpoint(org_id: int):
    COMMITTEES_CHECKPOINT.write_text(str(org_id))


def _fetch_org(org_id: int) -> dict | None:
    session = _get_session()
    result = _scrape_committee_detail(org_id, session)
    time.sleep(SLEEP_SEC)
    return result


def run_committees(force: bool = False, start_org: int = 0,
                   max_org: int | None = None, workers: int = 8):
    """
    Brute-force OrgID scraper for CommitteeDetail.aspx.
    Iterates OrgID 1 → max_org (auto-detected via binary search if not given),
    same rolling-window pattern as run_entities.
    Resumable via COMMITTEES_CHECKPOINT.
    """
    if force:
        for f in [COMMITTEES_OUT, COMMITTEES_CHECKPOINT]:
            if f.exists():
                f.unlink()

    if max_org is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        def _probe_org(oid):
            try:
                r = session.get(COMM_DETAIL_URL, params={"OrgID": oid}, timeout=30)
                r.raise_for_status()
                return bool(BeautifulSoup(r.text, "html.parser").find(id="_ctl0_Content_lblCommName"))
            except Exception:
                return False
        max_org = find_max_id(_probe_org, KNOWN_COMM_MAX_ORG_ID, step=1000, label="OrgID")

    checkpoint = max(_load_comm_checkpoint(), start_org)
    start_from = checkpoint + 1

    if start_from > max_org:
        print(f"Colorado committees: already complete (checkpoint={checkpoint}).")
        return

    total   = max_org - start_from + 1
    found   = 0
    skipped = 0
    CHUNK   = 200
    BATCH   = 50

    print(f"Colorado committees: OrgID {start_from} → {max_org} ({total:,} IDs, {workers} workers)")
    print(f"Output: {COMMITTEES_OUT}")

    completed     = 0
    buffer: list[dict] = []
    org_iter      = iter(range(start_from, max_org + 1))

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

                if result:
                    buffer.append(result)
                    found += 1
                else:
                    skipped += 1

                for next_oid in org_iter:
                    pending[pool.submit(_fetch_org, next_oid)] = next_oid
                    break

                if completed % BATCH == 0:
                    _write_committee_rows(buffer)
                    buffer.clear()
                    _save_comm_checkpoint(oid)
                    pct     = completed / total * 100
                    eta_min = ((total - completed) * SLEEP_SEC / workers) / 60
                    print(f"  {completed:,}/{total:,} ({pct:.1f}%) — "
                          f"{found:,} committees, {skipped:,} gaps "
                          f"— ETA ~{eta_min:.0f} min")
                break  # back to outer while to process next future

    _write_committee_rows(buffer)
    _save_comm_checkpoint(max_org)
    print(f"Colorado committees: done — {found:,} records written, {skipped:,} gaps.")


# ═══════════════════════════════════════════════════════════════════════════════
# TOP-LEVEL RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def run(force: bool = False, update: bool = False,
        transactions: bool = True, candidates: bool = True, committees: bool = True,
        start_seq: int = 0, max_seq: int | None = None,
        start_org: int = 0, max_org: int | None = None,
        workers: int = 8):

    if transactions:
        run_transactions(force=force, update=update)

    if candidates:
        run_entities(force=force or update, start_seq=start_seq,
                     max_seq=max_seq, workers=workers)

    if committees:
        run_committees(force=force or update, start_org=start_org,
                       max_org=max_org, workers=workers)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Colorado TRACER — transactions + candidates + committees")
    ap.add_argument("--force",        action="store_true",
                    help="Wipe everything and re-download from scratch")
    ap.add_argument("--update",       action="store_true",
                    help="Re-fetch current-year transactions; re-scrape committees + candidates")
    ap.add_argument("--transactions", action="store_true",
                    help="Transactions only")
    ap.add_argument("--candidates",   action="store_true",
                    help="Candidate SeqID scraper only")
    ap.add_argument("--committees",   action="store_true",
                    help="Committee scraper only")
    ap.add_argument("--start",        type=int, default=0,
                    help="Candidate scraper: start from this SeqID (overrides checkpoint)")
    ap.add_argument("--max",          type=int, default=None,
                    help="Candidate scraper: max SeqID (default: auto-detect via binary search)")
    ap.add_argument("--start-org",    type=int, default=0,
                    help="Committee scraper: start from this OrgID (overrides checkpoint)")
    ap.add_argument("--max-org",      type=int, default=None,
                    help="Committee scraper: max OrgID (default: auto-detect via binary search)")
    ap.add_argument("--workers",      type=int, default=8,
                    help="Parallel workers for candidate + committee scrapers (default 8)")
    args = ap.parse_args()

    # If no component flag given, run all three
    any_component = args.transactions or args.candidates or args.committees
    run(
        force=args.force,
        update=args.update,
        transactions=args.transactions or not any_component,
        candidates=args.candidates    or not any_component,
        committees=args.committees    or not any_component,
        start_seq=args.start,
        max_seq=args.max,
        start_org=args.start_org,
        max_org=args.max_org,
        workers=args.workers,
    )
