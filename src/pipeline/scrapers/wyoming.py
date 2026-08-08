"""
scrapers/wyoming.py — Download Wyoming campaign finance data.

Source: the Wyoming Secretary of State's Campaign Finance Information System
(wycampaignfinance.gov), a classic ASP.NET WebForms site (postback-driven,
no REST API). No authentication required.

── Transactions ──────────────────────────────────────────────────────────
SearchContributions.aspx and SearchExpenditures.aspx each have five search
"tabs" (Candidate/Candidate Committees, PACs, Organizations, Political
Parties, All) selected via a __doPostBack on ctl00$BodyContent$mnuContributions
with EVENTARGUMENT 0-4. Tab "4" ("All") searches across every recipient type
at once — confirmed by the Recipient Type column in the export, which
includes CANDIDATE, CANDIDATE COMMITTEE, POLITICAL ACTION COMMITTEE, PARTY
COMMITTEE, and ORGANIZATION values. Submitting a completely blank search on
this tab (Status = "Both Official and Published") followed by the page's
Export button streams the ENTIRE history as CSV in a single request — no
date-range looping, no pagination, no year-splitting. This is a very
different shape from most .NET disclosure sites (compare georgia.py's
Peachfile, which requires one request per year per type): confirmed against
the live "Showing 1-25 of N Records" counter that the Export button ignores
pagination and returns all N rows.

The site has no Last-Modified/ETag signal on these pages (checked via HEAD),
and the export is an unpartitioned all-time snapshot rather than a per-year
file, so there is no reliable way to detect "did this change since last run"
short of downloading it. Normal runs therefore always re-download both
transaction files in full — the manifest here is for observability
(row-count drift between runs) rather than skip-logic. This mirrors the
"always re-fetch the current year" rule scaled up to file granularity, since
the whole file behaves like a single rolling "current" period.

── Entities ───────────────────────────────────────────────────────────────
Reports/ResearchToolsAndLists.aspx has "Run Report" links for a Candidate
Campaign Committee roster and a PAC roster. Clicking "Run Report" doesn't
return the report directly — the server stashes the rendered report in
Session state and the page injects `window.open('ShowReports.aspx', ...)`.
Fetching Reports/ShowReports.aspx in the *same session* immediately after
returns a real PDF (ActiveReports-generated), which extracts cleanly with
pdfplumber: candidate name, party, office sought, committee name/address,
chairman, treasurer, phone, email, dates formed/terminated (PAC roster is
the same shape minus office/party). Neither roster exposes a numeric filer
ID anywhere — confirmed no ID field on either the roster PDFs or the
transaction exports — so the parser uses id_model="name_hash".

There is no separate Organization or Political Party roster on the
Research Tools page. Those two recipient types only exist implicitly in the
transaction exports' Recipient Type / Recipient Name columns; the parser
derives committees.csv rows for them from transaction data.

Downloads are tracked in manifest.csv purely for row-count history; see
above for why normal runs don't skip.

── Contribution source enrichment ─────────────────────────────────────────
The plain contributions export has no contributor-type column — just a
free-text Contributor Name. The "All" tab search form does expose a
`ddlSourceOfContribution` dropdown (12 values: INDIVIDUAL, CANDIDATE
COMMITTEE, CORPORATION, IMMEDIATE FAMILY / PERSONAL, WYOMING PAC, WYOMING
STATE PARTY, ...) that isn't itself a visible column anywhere — it's a
filter. Running the same blank-search-then-Export flow once per category
(read dynamically from the <select> rather than hardcoded, same philosophy
as _form_state) produces 12 files whose rows are, collectively, a subset of
the full export tagged with an authoritative category.

That subset isn't total: the 12 categories summed to 439,645 live rows
against 444,954 raw rows in the plain export at verification time. The
~5,300 row gap is exactly the ANONYMOUS/UN-ITEMIZED contributions, which
have no contributor name at all and so were never assigned a source
category by the site itself — there's nothing to enrich there regardless.
Because of this, download_transaction("contributions") remains the
authoritative full row set; download_contribution_sources() is purely a
supplementary (row) -> category lookup consumed by the parser, matched by
the full row tuple (contributor/recipient/type/date/amount/city-state-zip)
since neither export carries a transaction ID. Rows that don't match any of
the 12 files (ANONYMOUS/UN-ITEMIZED, plus any rare miss) fall back to the
parser's existing name-string heuristic.

INDIVIDUAL alone is ~421K rows — nearly the size of the whole plain export
— so this roughly doubles total scrape time for a contributions run.
"""

import csv
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from config import USER_AGENT

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Wyoming" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Wyoming" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["file_type", "filename", "downloaded_at", "row_count"]

# ========================= state-specific constants ===================

BASE = "https://www.wycampaignfinance.gov/WYCFWebApplication"
CONTRIB_URL   = f"{BASE}/GSF_SystemConfiguration/SearchContributions.aspx"
EXPEND_URL    = f"{BASE}/GSF_SystemConfiguration/SearchExpenditures.aspx"
RESEARCH_URL  = f"{BASE}/Reports/ResearchToolsAndLists.aspx"
SHOWREPORT_URL = f"{BASE}/Reports/ShowReports.aspx"

# mnuContributions tab index for "All" (Candidate/CC=0, PAC=1, Org=2, Party=3, All=4)
ALL_TAB_ARG = "4"

# Source of Contribution filter on the "All" tab — supplementary enrichment,
# not a replacement for the plain export. See module docstring.
SOURCE_FIELD = "ctl00$BodyContent$ddlSourceOfContribution"

TRANSACTION_TARGETS = {
    "contributions": (CONTRIB_URL, "contributions_all.csv"),
    "expenditures":  (EXPEND_URL,  "expenditures_all.csv"),
}

# Roster report targets: (postback event target on ResearchToolsAndLists.aspx,
# extra form fields to set before "Run Report", output filename)
ROSTER_TARGETS = {
    "candidates": (
        "ctl00$BodyContent$lnkRosterCampaignCommittee",
        {"ctl00$BodyContent$ContestType": "0"},   # 0 = "All" offices
        "candidate_committee_roster.pdf",
    ),
    "committees": (
        "ctl00$BodyContent$lnkRosterPAC",
        {},
        "pac_roster.pdf",
    ),
}

# ========================= manifest helpers ==========================

def load_manifest() -> dict[str, dict]:
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="") as f:
        return {row["filename"]: row for row in csv.DictReader(f)}


def update_manifest(filename: str, record: dict):
    """Replace an existing manifest row for `filename` (or append if missing)."""
    rows = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            rows = list(csv.DictReader(f))
    updated = False
    for row in rows:
        if row["filename"] == filename:
            row.update(record)
            updated = True
            break
    if not updated:
        rows.append(record)
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(rows)


# ========================= ASP.NET WebForms helpers ==========================

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _form_state(html: str) -> dict:
    """
    Snapshot every named form control's current value (like a browser would
    submit it) — hidden __VIEWSTATE/__EVENTVALIDATION tokens, text inputs,
    checked radios/checkboxes, and each <select>'s selected (or first)
    option. Submit-type inputs (buttons) are deliberately excluded; callers
    add the one button they mean to "click" by name/value explicitly.

    Rebuilding the full state from each response (rather than hand-listing
    field names) survives the site adding/removing fields across the two
    search pages and between tabs, which turned out to matter here: the
    "All" tab has different field names than the default candidate tab, and
    a hand-maintained field list silently dropped the Status radio group
    default on first attempt, causing the search to return zero rows.
    """
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form")
    data: dict = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        itype = (inp.get("type") or "text").lower()
        if itype in ("checkbox", "radio"):
            if inp.has_attr("checked"):
                data[name] = inp.get("value", "on")
        elif itype in ("submit", "button", "image", "file"):
            continue
        else:
            data[name] = inp.get("value", "")
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        data[name] = opt.get("value", opt.get_text(strip=True)) if opt else ""
    return data


def _postback(session: requests.Session, url: str, state: dict,
              event_target: str = "", event_argument: str = "",
              timeout: int = 60) -> requests.Response:
    """POST a __doPostBack-style event (menu tab switch, report link)."""
    data = {**state, "__EVENTTARGET": event_target, "__EVENTARGUMENT": event_argument}
    resp = session.post(url, data=data, timeout=timeout)
    resp.raise_for_status()
    return resp


def _submit(session: requests.Session, url: str, state: dict,
           button_name: str, button_value: str,
           timeout: int = 60) -> requests.Response:
    """POST a submit-button click (Search / Export / Run Report)."""
    data = {**state, button_name: button_value}
    resp = session.post(url, data=data, timeout=timeout)
    resp.raise_for_status()
    return resp


# ========================== transactions ============================

def download_transaction(log, session: requests.Session,
                         kind: str) -> tuple[str, int] | None:
    """
    Fetch the full-history bulk export for 'contributions' or 'expenditures'.

    Flow: GET the search page -> switch to the "All" tab -> submit a blank
    Search (Status forced to "Both") -> submit Export. Each step is a normal
    ASP.NET postback against the same session/cookies; the Export response
    is the raw CSV content (no wrapper page).
    """
    url, filename = TRANSACTION_TARGETS[kind]
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        r0 = session.get(url, timeout=30)
        r0.raise_for_status()
        state = _form_state(r0.text)

        r1 = _postback(session, url, state,
                       event_target="ctl00$BodyContent$mnuContributions",
                       event_argument=ALL_TAB_ARG, timeout=30)
        state = _form_state(r1.text)

        # Default is already "Both Official and Published" (rdoBoth checked),
        # but set it explicitly — a truly blank Status broke the search
        # entirely on first attempt (server returned "no records found").
        if "ctl00$BodyContent$Status" in state:
            state["ctl00$BodyContent$Status"] = "rdoBoth"

        r2 = _submit(session, url, state,
                    "ctl00$BodyContent$bntSearch", " Search ", timeout=90)
        state = _form_state(r2.text)

        r3 = _submit(session, url, state,
                    "ctl00$BodyContent$btnExport", " Export ", timeout=300)
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    content = r3.content
    # A real export starts with a quoted CSV header. If the search/tab/export
    # sequence broke somewhere, the response is the HTML search page again.
    if content.lstrip()[:1] != b'"':
        log.file_download_error(
            filename=filename,
            error="export did not return CSV — search postback sequence may have failed")
        return None

    out_path.write_bytes(content)
    row_count = max(content.count(b"\n") - 1, 0)

    log.file_download_ok(filename=filename, bytes=len(content), rows=row_count,
                         duration_s=round(time.perf_counter() - t0, 2))
    return filename, row_count


def _slugify(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")


def download_contribution_sources(log, session: requests.Session) -> list:
    """
    Supplementary enrichment download: one filtered export per Source of
    Contribution category (see module docstring). Categories are read live
    from the <select> rather than hardcoded. Each category gets its own
    fresh GET -> tab switch -> filtered Search -> Export sequence, matching
    download_transaction()'s proven flow rather than trying to reuse a
    single page load's VIEWSTATE across 12 searches.

    Returns a list of (filename, row_count, label) for successes, with a
    None entry for any category that failed (mirrors download_transaction's
    per-file error handling so one bad category doesn't abort the rest).
    """
    url = CONTRIB_URL

    r0 = session.get(url, timeout=30)
    r0.raise_for_status()
    state = _form_state(r0.text)
    r1 = _postback(session, url, state,
                   event_target="ctl00$BodyContent$mnuContributions",
                   event_argument=ALL_TAB_ARG, timeout=30)
    soup = BeautifulSoup(r1.text, "html.parser")
    sel = soup.find("select", attrs={"name": SOURCE_FIELD})
    # value="-1" is the "-- Select One --" placeholder (i.e. no filter at
    # all — submitting it returns the same unfiltered full export as
    # download_transaction). Exclude it explicitly; its value is truthy so
    # a plain `if o.get("value")` check lets it through.
    options = [(o.get("value", ""), o.get_text(strip=True))
              for o in sel.find_all("option")
              if o.get("value") and o.get("value") != "-1"]

    results = []
    for code, label in options:
        slug = _slugify(label)
        filename = f"contributions_source_{slug}.csv"
        out_path = RAW_DIR / filename

        log.file_download_start(filename=filename)
        t0 = time.perf_counter()

        try:
            ra = session.get(url, timeout=30)
            ra.raise_for_status()
            state = _form_state(ra.text)

            rb = _postback(session, url, state,
                           event_target="ctl00$BodyContent$mnuContributions",
                           event_argument=ALL_TAB_ARG, timeout=30)
            state = _form_state(rb.text)
            state[SOURCE_FIELD] = code
            if "ctl00$BodyContent$Status" in state:
                state["ctl00$BodyContent$Status"] = "rdoBoth"

            rc = _submit(session, url, state,
                        "ctl00$BodyContent$bntSearch", " Search ", timeout=90)
            state = _form_state(rc.text)

            rd = _submit(session, url, state,
                        "ctl00$BodyContent$btnExport", " Export ", timeout=300)
        except requests.RequestException as e:
            log.file_download_error(filename=filename, error=str(e))
            results.append(None)
            continue

        content = rd.content
        if content.lstrip()[:1] != b'"':
            log.file_download_error(
                filename=filename,
                error="export did not return CSV — search postback sequence may have failed")
            results.append(None)
            continue

        out_path.write_bytes(content)
        row_count = max(content.count(b"\n") - 1, 0)

        log.file_download_ok(filename=filename, bytes=len(content), rows=row_count,
                             duration_s=round(time.perf_counter() - t0, 2))
        results.append((filename, row_count, label))
        time.sleep(0.5)

    return results


# ============================ entities ==============================

def download_roster(log, session: requests.Session, kind: str) -> tuple[str, int] | None:
    """
    Fetch the candidate-committee or PAC roster PDF.

    Flow: GET the research tools page -> click the roster link (reveals a
    filter panel: office/contest type for the candidate roster, active/
    terminated status for both) -> submit "Run Report" with Both (active +
    terminated) selected -> the response embeds a `window.open('ShowReports
    .aspx', ...)` script instead of returning the report directly (the
    server renders it into Session state). GETting ShowReports.aspx in the
    same session immediately after returns the actual PDF.
    """
    event_target, extra_fields, filename = ROSTER_TARGETS[kind]
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        r0 = session.get(RESEARCH_URL, timeout=30)
        r0.raise_for_status()
        state = _form_state(r0.text)

        r1 = _postback(session, RESEARCH_URL, state, event_target=event_target, timeout=30)
        state = _form_state(r1.text)
        state.update(extra_fields)
        # 2 = "Both" (active + terminated) on ddlRosterFilters
        if "ctl00$BodyContent$ddlRosterFilters" in state:
            state["ctl00$BodyContent$ddlRosterFilters"] = "2"

        _submit(session, RESEARCH_URL, state,
               "ctl00$BodyContent$btnRunReport", "Run Report", timeout=90)

        r3 = session.get(SHOWREPORT_URL, timeout=120)
        r3.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    content = r3.content
    if content[:4] != b"%PDF":
        log.file_download_error(
            filename=filename,
            error="ShowReports.aspx did not return a PDF — report postback sequence "
                  "may have failed or session state expired")
        return None

    out_path.write_bytes(content)

    # Row count isn't meaningful for a PDF report — use page count as a rough proxy.
    try:
        import pdfplumber
        with pdfplumber.open(out_path) as pdf:
            page_count = len(pdf.pages)
    except Exception:
        page_count = 0

    log.file_download_ok(filename=filename, bytes=len(content), rows=page_count,
                         duration_s=round(time.perf_counter() - t0, 2))
    return filename, page_count


# ============================== run =================================

def run(
    force: bool = False,
    entities: bool = False,
    transactions: bool = False,
    contributions: bool = False,
    expenditures: bool = False,
    candidates: bool = False,
    committees: bool = False,
):
    """
    Download Wyoming campaign finance data.

    Horizontal scope only — the source has no year-based filtering, so
    start_year/end_year/--force don't change what's downloaded (see module
    docstring: every normal run already re-fetches the full history).

        No flags       — everything
        transactions   — contributions + expenditures
        entities       — candidate committee roster + PAC roster
        contributions  — contributions only (implies transactions), plus the
                         12 supplementary Source of Contribution exports
                         used for contributor_type enrichment
        expenditures   — expenditures only (implies transactions)
        candidates     — candidate committee roster only (implies entities)
        committees     — PAC roster only (implies entities)
    """
    log = get_logger("wyoming", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    do_transactions = no_horizontal or transactions or contributions or expenditures
    do_entities     = no_horizontal or entities or candidates or committees

    tx_kinds = list(TRANSACTION_TARGETS)
    if contributions and not expenditures:
        tx_kinds = ["contributions"]
    elif expenditures and not contributions:
        tx_kinds = ["expenditures"]

    roster_kinds = list(ROSTER_TARGETS)
    if candidates and not committees:
        roster_kinds = ["candidates"]
    elif committees and not candidates:
        roster_kinds = ["committees"]

    files_ok  = 0
    files_err = 0
    today = datetime.today().strftime("%Y-%m-%d")

    try:
        session = _make_session()

        if do_transactions:
            for kind in tx_kinds:
                result = download_transaction(log, session, kind)
                if result is None:
                    files_err += 1
                    continue
                filename, row_count = result
                files_ok += 1
                update_manifest(filename, {
                    "file_type": kind, "filename": filename,
                    "downloaded_at": today, "row_count": row_count,
                })
                time.sleep(0.5)

            if "contributions" in tx_kinds:
                # Supplementary Source of Contribution enrichment — see
                # module docstring. Not part of the authoritative row set,
                # so failures here don't count against files_err/files_ok
                # the way a missing contributions_all.csv would.
                for result in download_contribution_sources(log, session):
                    if result is None:
                        continue
                    filename, row_count, label = result
                    update_manifest(filename, {
                        "file_type": f"contrib_source:{label}", "filename": filename,
                        "downloaded_at": today, "row_count": row_count,
                    })

        if do_entities:
            for kind in roster_kinds:
                result = download_roster(log, session, kind)
                if result is None:
                    files_err += 1
                    continue
                filename, page_count = result
                files_ok += 1
                update_manifest(filename, {
                    "file_type": f"roster_{kind}", "filename": filename,
                    "downloaded_at": today, "row_count": page_count,
                })
                time.sleep(0.5)

        duration = round(time.perf_counter() - t0, 1)
        log._emit("scrape_completed", status="completed", duration_s=duration,
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


# =============================== cli ================================

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Download Wyoming campaign finance data.")
    ap.add_argument("--force",        action="store_true",
                    help="no-op vertical scope — every run already re-downloads the "
                         "full history (kept for CLI-contract compatibility)")
    ap.add_argument("--transactions", action="store_true", help="contributions + expenditures")
    ap.add_argument("--entities",     action="store_true", help="candidate + PAC rosters")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true", help="candidate committee roster only")
    ap.add_argument("--committees",    action="store_true", help="PAC roster only")
    args, _ = ap.parse_known_args()

    try:
        run(
            force=args.force,
            entities=args.entities,
            transactions=args.transactions,
            contributions=args.contributions,
            expenditures=args.expenditures,
            candidates=args.candidates,
            committees=args.committees,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
