"""
scrapers/missouri.py — Download Missouri Ethics Commission (MEC) campaign finance data.

Requires a live browser session via Playwright — MEC's Incapsula/Imperva WAF blocks
datacenter/cloud IPs outright (confirmed: both raw `curl` and headless Playwright get
reset/challenged from a cloud host), so this must be run from a local machine, exactly
like the Alaska scraper. Expected runtime is LONG: Missouri has ~13,000+ registered
committees and the site exposes no bulk per-state transaction export, so contribution
and expenditure data must be pulled per-(committee, year) via a search form. A full
historical backfill (--start-year covering many cycles) can take many hours; run it
unattended and let the manifest make it resumable.

Gotcha, hard-won via interactive testing — MUST pass args=["--disable-popup-blocking"]
when launching Chromium: CF12_ContrExpend.aspx's Search button does a full-page
ASP.NET postback, and only *after* that postback completes does an onload handler
window.open() the results page. Because the popup is opened from an onload handler
rather than synchronously inside the click, the browser's user-activation from the
click does not carry over the navigation, and Chromium's popup blocker silently
swallows the window.open() — no error, no popup, nothing. Disabling the popup blocker
at the browser-launch level is the only reliable fix (allow-listing the origin in a
normal Chrome profile is not available to a fresh Playwright profile).

Three data-acquisition mechanisms, all confirmed against the live site:

  Phase A — committee/candidate registry (CFSearch.aspx, "Committee Type" tab).
      Committee Type must be "Select All" but Committee Status may NOT also be
      "Select All" ("Please Limit the search by Committee Type or Status.") — so we
      run it twice, Status=Active and Status=Terminated, which together cover every
      committee type in two searches instead of looping over ~10 types individually.
      Exported via "Export To Excel", which despite the extension is an HTML table
      (openable with pandas.read_html or, as used here, BeautifulSoup).

  Phase B — per-committee detail sweep (CommInfo.aspx?MECID=X, plain GET).
      This is the ONLY place several fields the schema needs live — notably `party`,
      which is not present anywhere in the bulk committee export. Also carries the
      committee's full election history (one row per election it has run in — a
      candidate committee's MECID in MO persists across cycles and even across
      different offices, e.g. the same committee ID was used for a State Senate run
      in 2010/2014, Lieutenant Governor in 2020, and Governor in 2024/2028 — so
      state_filer_id is effectively already a person-level key here; see
      parsers/missouri.py's id_model="person" choice).

  Phase D — statewide contributions/expenditures, swept by (date range, amount
      range) rather than by committee (CF12_ContrExpend.aspx, Contributor/
      Expenditure tab -> Advanced Search sub-tab). Originally built as a
      per-(committee, year) sweep (~40,000+ searches across ~20,000
      committees) on the assumption that Committee ID was a required filter
      -- confirmed WRONG via live interactive testing: a Year + a From/To
      date range (Committee ID left blank) returns a genuine statewide,
      cross-committee result set (confirmed: 9,436 rows for just January
      2026), each row carrying its own MECID/Committee Name column. The old
      per-committee sweep was also the root cause of a recurring failure
      that looked committee-specific but wasn't: most per-committee/year
      export attempts across a real run hit the same failure regardless of
      which committee. Switching to statewide date+amount chunks cuts total
      searches from ~40,000 to a few hundred per year and sidesteps the
      per-committee looping entirely. See _process_date_amount_chunk's
      docstring for the adaptive chunk-splitting design and
      docs/states/missouri.md for the full investigation, including the
      confirmed root cause of the export failures (a plain HTTP 503 on the
      export POST itself, unrelated to result-set size).

  Phase E — independent expenditures (CF_SearchDirExp.aspx, "Committee
      Expenditures for Candidates" search). This is MEC's actual independent-
      expenditure report: money a committee spends directly to a vendor to
      support or oppose a specific candidate, WITHOUT that candidate's
      coordination — a structurally different report from Phase D's per-
      committee "Expenditure" tab, which only covers a committee's own
      ordinary spending and carries no supported/opposed-candidate field at
      all. Unlike Phase D, this one IS a real bulk export: unlike CF12, a
      blank Committee ID here is fine — Report Year is the only required
      filter, so it's swept once per year (statewide) rather than once per
      committee per year. Confirmed live: 1,718 records for 2026 alone via a
      single "Export Results to Excel" click. There is no MECID on the
      exported rows for the target candidate (no structured committee
      registry to join against for them), and the reporting committee's name
      arrives as plain text, not a MECID either — see parsers/missouri.py for
      how these rows land in expenditures.csv via the affiliated_candidate_name
      / support_oppose columns.
"""

import csv
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# Make project root importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR          = PROJECT_ROOT / "data" / "Missouri" / "raw"
CONTRIB_DIR      = RAW_DIR / "contributions"
EXPEND_DIR       = RAW_DIR / "expenditures"
MANIFEST         = PROJECT_ROOT / "data" / "Missouri" / "manifest.csv"
COMMITTEE_DETAIL_PATH = RAW_DIR / "committee_detail.csv"
ELECTION_HISTORY_PATH = RAW_DIR / "election_history.csv"

# Persistent Playwright profile directory -- NOT under data/Missouri/raw,
# since parsers glob that tree for .xls exports and shouldn't see Chrome
# profile internals. See the launch_persistent_context() note below for why
# this needs to be persistent rather than a fresh throwaway context.
PROFILE_DIR      = PROJECT_ROOT / ".playwright-profiles" / "missouri"

RAW_DIR.mkdir(parents=True, exist_ok=True)
CONTRIB_DIR.mkdir(parents=True, exist_ok=True)
EXPEND_DIR.mkdir(parents=True, exist_ok=True)
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "key", "filename", "row_count"]

# ================================ URLs =================================
CFSEARCH_URL  = "https://mec.mo.gov/mec/Campaign_Finance/CFSearch.aspx"
COMMINFO_URL  = "https://mec.mo.gov/mec/Campaign_Finance/CommInfo.aspx?MECID={mecid}"
CF12_URL      = "https://mec.mo.gov/mec/Campaign_Finance/CF12_ContrExpend.aspx"
CF_DIREXP_URL = "https://mec.mo.gov/mec/Campaign_Finance/CF_SearchDirExp.aspx"

# Committee Status values swept on the Committee Type tab — together these cover
# every Committee Type in just 2 searches (Select-All-Type + Select-All-Status is
# rejected by the site as too broad; splitting by Status is the workaround).
COMMITTEE_STATUSES = ["Active", "Terminated"]

COMMITTEE_DETAIL_COLS = [
    "mecid", "committee_name", "committee_status", "committee_type", "term_date",
    "address", "city", "state", "zip", "phone",
    "candidate_name", "cand_address", "cand_city", "cand_state", "cand_zip", "cand_phone",
    "party",
    "treasurer_name", "tre_address", "tre_city", "tre_state", "tre_zip", "tre_phone",
]

ELECTION_HISTORY_COLS = [
    "mecid", "election_date", "election_type", "office", "district", "political_subdivision",
]

MAX_CONSECUTIVE_ERRORS = 40   # bail Phase B/D early if the site starts erroring out wholesale

# Export-click response wait. Confirmed live, repeatedly: the Export button is
# a plain form POST back to CF12_ContrExpendResults.aspx, and the response
# itself (captured directly via Playwright's Response object, not the
# browser's separate "download" event machinery -- see _click_export) arrives
# within a couple seconds whether it succeeds or fails. There's no legitimate
# case where waiting longer helps, so this stays short.
EXPORT_DOWNLOAD_TIMEOUT_MS = 10_000
# Retries (on the SAME popup, just re-clicking Export) when the export click
# lands on a bot/captcha interstitial.
MAX_EXPORT_BOT_RETRIES = 3
# Retries (same popup, with backoff) when the export POST itself returns an
# HTTP 5xx. Confirmed live: MEC's export endpoint returns a bare 503 Service
# Unavailable on the SAME url (no redirect to Error.aspx) for result sets of
# very different sizes -- a 9,436-row statewide month AND a 396-row single
# day both hit it, back to back -- so this is NOT a "your query is too big"
# signal. It's HTTP's standard "try again later," so it gets real retries
# with backoff, unlike the genuine Error.aspx app-bug case (never retried).
MAX_EXPORT_HTTP_ERROR_RETRIES = 4

# ── Phase D chunking (statewide sweep by date range x amount range) ──────
# Tiers are deliberately NOT equal-width -- real contribution/expenditure
# amounts are heavily skewed toward small-dollar transactions, so equal-width
# tiers would leave the bottom tier far too large while the top tiers sit
# almost empty. These bounds are a reasonable starting partition, not a
# guarantee -- SAFE_EXPORT_ROW_THRESHOLD + the adaptive splitting in
# _process_date_amount_chunk handle whatever a fixed partition doesn't.
AMOUNT_TIER_BOUNDS = [0, 100, 500, 1_000, 5_000, 25_000, 10_000_000]
DATE_CHUNK_DAYS = 14   # initial two-week windows per amount tier per year
# Confirmed live: a 478-row single-committee export succeeded reliably; a
# 9,436-row statewide month did not. This threshold is a conservative guess
# at a safe ceiling, not a hard boundary -- chunks are also split reactively
# on export failure regardless of their reported size (see
# _process_date_amount_chunk), so getting this exactly right isn't critical.
SAFE_EXPORT_ROW_THRESHOLD = 500
MAX_CHUNK_SPLIT_DEPTH = 6   # 2^6 = 64x narrower than the initial chunk, worst case


# ========================== Manifest helpers ==========================
def load_manifest() -> set[tuple[str, str]]:
    """Return the set of (relation_type, key) pairs already recorded in the manifest."""
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
    """Append-only by design, NOT a true read-modify-rewrite upsert — this is
    called after every single item in Phase B/D/E (tens of thousands of times
    over a multi-hour/multi-day run), and a read-the-whole-file-then-rewrite-
    the-whole-file pattern is not crash-safe at that call volume: a kill or
    power loss mid-rewrite truncates the ENTIRE manifest, not just the newest
    row, silently turning a resumable run into one that starts over from
    scratch. Appending is crash-safe by construction (worst case you lose the
    one row that was mid-write) and O(1) per call instead of O(n) — the
    rewrite version was becoming O(n^2) in total I/O as the manifest grew into
    the tens of thousands of rows.

    Duplicate (relation_type, key) rows can accumulate across repeated
    --force/--start-year/--end-year re-runs of the same scope — harmless:
    load_manifest() only checks key presence (a set), never row_count, and
    strip_manifest()'s _outside_range() (called for force/range runs, and
    covering "committees", "contributions", "expenditures", and
    "independent_expenditures" alike) clears the in-scope range up front each
    time, so duplicates don't build up across repeated re-runs of the same
    range. contributing.md §"The manifest" explicitly allows "append or
    upsert" — this isn't a deviation from convention, just picking the
    crash-safe half of that choice."""
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


# ========================= HTML table reading ==========================
def _dedupe_headers(headers: list[str]) -> list[str]:
    """MEC's contribution export repeats the 'Committee' header (once for the
    filing committee, once for the Contributor-Committee transfer flag) — a plain
    dict(zip(headers, cells)) would silently drop the first one's values on the
    duplicate key collision. Rename repeats '<name>.1', '<name>.2', ... matching
    pandas.read_html's convention, since the exact confirmed column names used
    throughout parsers/missouri.py ("Committee" / "Committee.1") assume this."""
    seen: dict[str, int] = {}
    out = []
    for h in headers:
        if h not in seen:
            seen[h] = 0
            out.append(h)
        else:
            seen[h] += 1
            out.append(f"{h}.{seen[h]}")
    return out


def _read_xls_table(path: Path) -> list[dict]:
    """MEC's 'Export to Excel' buttons produce an HTML <table> saved with an .xls
    extension, not a real binary workbook. Parse it with BeautifulSoup (already a
    project dependency) rather than pulling in pandas/html5lib just for this."""
    from bs4 import BeautifulSoup

    with open(path, "rb") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    table = soup.find("table")
    if table is None:
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    headers = _dedupe_headers([td.get_text(strip=True) for td in rows[0].find_all(["td", "th"])])
    out = []
    for tr in rows[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if not cells or all(c == "" for c in cells):
            continue
        out.append(dict(zip(headers, cells)))
    return out


# ========================== WAF / retry helper =========================
def _goto_with_retry(page, url: str, log, tries: int = 2, timeout: int = 30_000) -> bool:
    """Navigate, retrying once on an Incapsula 'Request Rejected' block. Returns
    True on success, False if still blocked after retries."""
    for attempt in range(tries):
        page.goto(url, timeout=timeout)
        page.wait_for_load_state("load")
        text = page.locator("body").inner_text()
        if "Request Rejected" not in text:
            return True
        log.warning(f"WAF rejection at {url}; sleeping and retrying")
        time.sleep(5)
    return False


# Generic WAF/CAPTCHA interstitial signatures, checked case-insensitively.
# MEC's block page on individual pages isn't always the plain "Request
# Rejected" text _goto_with_retry looks for -- sometimes it's a CAPTCHA-style
# challenge with different wording. This list is intentionally broad (covers
# common Incapsula/Cloudflare/Akamai phrasing) and used only as a logging
# signal -- the per-page retry helpers below (fetch_committee_detail,
# _search_cf12) also fall back to a structural check (expected content
# missing after reload), since a block page we don't recognize by text will
# still fail to render the real content either way.
BOT_BLOCK_PHRASES = [
    "request rejected", "pardon our interruption", "verify you are human",
    "verify you're human", "are you a robot", "checking your browser",
    "unusual traffic", "access denied", "captcha", "please stand by",
    "attention required",
]

PAGE_RETRY_TRIES = 4   # reload attempts for a single page before giving up
PAGE_RETRY_SLEEP  = 4  # base seconds between retries; scaled by attempt number


def _looks_bot_blocked(text: str) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in BOT_BLOCK_PHRASES)


def _mec_server_error(url: str, body_text: str) -> str | None:
    """Detects MEC's own generic ASP.NET unhandled-exception page — distinct
    from a WAF/bot-block screen. Confirmed live: a real search can redirect to
    /mec/Error.aspx?aspxerrorpath=... with a message like "Error Message:
    Count cannot be less than zero. Parameter name: count" — a genuine bug in
    MEC's own app for that specific request (often tied to specific data,
    e.g. a particular committee/year combination), NOT a transient block a
    fresh reload reliably clears.

    This must NEVER be treated as a genuinely empty result — none of
    BOT_BLOCK_PHRASES match this page's wording (it says "Server Error" /
    "Oops! Something went wrong!", not anything CAPTCHA/bot-related), so
    without this check it falls straight through to "empty" and permanently
    records a real (if broken-on-MEC's-end) search as done with 0 rows.

    Returns the captured "Error Message: ..." line for logging (or a generic
    fallback string if the page matched but no message line was found), or
    None if this doesn't look like MEC's error page at all."""
    if "Error.aspx" not in url and "aspxerrorpath" not in url:
        return None
    err_line = next(
        (line.strip() for line in body_text.splitlines() if line.strip().startswith("Error Message")),
        None,
    )
    return err_line or "MEC server error page (no message line captured)"


# ASP.NET UniqueID prefix shared by every postback-triggering control inside
# the CF12/CF_SearchDirExp content placeholder (confirmed from the page's own
# HTML, e.g. href="javascript:__doPostBack('ctl00$ctl00$ContentPlaceHolder$
# ContentPlaceHolder1$lbtnContr','')"). Note this uses '$' as the hierarchy
# separator, unlike the client-side element ID prefix P ("..." + '_'), which
# is why it's kept as a separate constant rather than derived from P.
EVENT_TARGET_PREFIX = "ctl00$ctl00$ContentPlaceHolder$ContentPlaceHolder1$"


def _wait_for_postback(page, trigger, timeout: int = 30_000) -> None:
    """Call trigger() to fire an ASP.NET postback, then wait for it to
    actually complete by polling the page's __VIEWSTATE hidden field for a
    changed value, rather than relying on Playwright's navigation/load-state
    event detection.

    Confirmed necessary in production: even `with page.expect_navigation(
    wait_until="load", ...): <trigger the postback>` timed out ("Timeout
    30000ms exceeded ... waiting for navigation until 'load'") despite the
    postback apparently having fired correctly. MEC's __doPostBack-driven
    postbacks are same-URL POST submissions (the URL never changes), which
    is a known weak spot for Playwright's navigation-event detection — it's
    built around frame-lifecycle/URL heuristics that don't reliably fire
    for every same-URL full-page POST. __VIEWSTATE is regenerated by
    ASP.NET on every successful postback, so polling for its value to
    change is a robust, actual-site-behavior-based signal that doesn't
    depend on Playwright correctly classifying the underlying navigation at
    all.
    """
    old_viewstate = page.eval_on_selector("#__VIEWSTATE", "el => el.value")
    trigger()
    page.wait_for_function(
        "(old) => { const el = document.querySelector('#__VIEWSTATE'); "
        "return el && el.value !== old; }",
        arg=old_viewstate,
        timeout=timeout,
    )


def _postback(page, control_name: str, timeout: int = 30_000) -> None:
    """Trigger an ASP.NET __doPostBack for the given control by calling the
    page's own __doPostBack() JS function directly, instead of a physical
    Playwright click on the link — confirmed necessary in production:
    physical page.click() on these tab-switch links proved unreliable (the
    automated browser was observed interacting with the page's YouTube
    video embed near the top of the page instead of the intended link, a
    layout/hit-testing quirk). Calling __doPostBack directly is exactly
    what a successful click on the link would do (confirmed from the
    link's own href), but is immune to anything else on the page
    intercepting the click. See _wait_for_postback for how completion is
    detected.
    """
    _wait_for_postback(
        page,
        lambda: page.evaluate("(t) => __doPostBack(t, '')", f"{EVENT_TARGET_PREFIX}{control_name}"),
        timeout=timeout,
    )


def _select_year_and_postback(page, select_id: str, year: str, control_name: str = "ddYear",
                               timeout: int = 30_000) -> None:
    """Set the Year <select>'s value and trigger its onchange postback.

    Primary mechanism: Playwright's native page.select_option(), which
    dispatches trusted 'input'/'change' DOM events and lets the page's own
    onchange="setTimeout('__doPostBack(...)', 0)" handler fire __doPostBack
    on its own. Completion is still detected via _wait_for_postback's
    __VIEWSTATE-diff poll, not wait_for_load_state/navigation events (see
    _wait_for_postback's docstring for why those are unreliable here).

    This replaces an earlier version that set `.value` and called
    __doPostBack() directly via page.evaluate() -- a fully synthetic
    interaction with no real DOM event or user gesture behind it at all.
    That version worked reliably for months of Phase D sweeps, but was
    confirmed live to get PERMANENTLY stuck (not just slow -- a human
    physically clicking Search was required to unstick it, confirmed by
    direct testing, not just a coincidental/preemptive click during an
    already-recovering retry) partway through a long-running historical
    backfill's Phase E year-select postback. Leading theory: after
    sustained heavy automated traffic, something in MEC's WAF/session
    layer starts silently discarding postbacks that carry no real event
    or user gesture at all, while it still accepts postbacks that
    originate from genuine trusted interactions -- consistent with the
    Search-button fix nearby (a real page.click(), just switched to
    _wait_for_postback-style completion detection) not needing this same
    treatment for the same class of failure.

    Falls back to the original raw-JS __doPostBack() trigger if
    select_option() itself fails (e.g. an actionability check fails
    because something overlaps the control -- the same class of problem
    that made physical clicks on the page's tab links unreliable, see
    _postback's docstring) or if the postback still hasn't completed
    within `timeout` after select_option() ran -- so Phase D's
    already-proven-reliable behavior can't regress even if the theory
    above turns out to be wrong for some page/control.

    Still skips the postback entirely if `year` is already the select's
    current value -- a no-op selection doesn't regenerate __VIEWSTATE, so
    _wait_for_postback's poll would burn the full timeout every attempt
    regardless of which trigger mechanism is used. Confirmed live on
    CF_SearchDirExp.aspx's ddYear, which defaults to the current calendar
    year on a fresh page load; CF12_ContrExpend.aspx's Phase D ddYear
    doesn't default the same way (a full 324-chunk sweep completed
    cleanly under the old trigger), so this guard is a no-op there -- it
    only changes behavior when the value truly doesn't need to change."""
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    current = page.eval_on_selector(f"#{select_id}", "el => el.value")
    if current == year:
        return

    def _raw_js_trigger():
        page.evaluate(
            "(args) => { document.querySelector(args.sel).value = args.val; "
            "__doPostBack(args.target, ''); }",
            {"sel": f"#{select_id}", "val": year, "target": f"{EVENT_TARGET_PREFIX}{control_name}"},
        )

    try:
        _wait_for_postback(page, lambda: page.select_option(f"#{select_id}", year), timeout=timeout)
    except PlaywrightTimeoutError:
        _wait_for_postback(page, _raw_js_trigger, timeout=timeout)


# ============================ Phase A: committees =======================
def download_committee_list(page, status: str, log) -> tuple[str, int] | None:
    """Search CFSearch.aspx's Committee Type tab for Type=Select All,
    Status=<status>, and export the results grid."""
    if not _goto_with_retry(page, CFSEARCH_URL, log):
        return None

    P = "ContentPlaceHolder_ContentPlaceHolder1_"
    page.click(f"#{P}lbtnType")
    page.wait_for_selector(f"#{P}ddType", timeout=30_000)

    page.select_option(f"#{P}ddType", "Select All")
    page.select_option(f"#{P}ddStatus", status)
    page.click(f"#{P}btnSearch")

    # This is a full ASP.NET postback (not an UpdatePanel), so "load" refires once
    # the results grid has actually rendered server-side -- unlike "networkidle",
    # which is unreliable here: MEC's page has a Google Custom Search widget and a
    # satisfaction-survey banner that poll in the background and can prevent the
    # page from ever reaching 500ms of true network silence, causing a spurious
    # timeout regardless of result-set size. "Terminated" is also a much bigger
    # grid than "Active" (~4x the rows) so the postback response itself can
    # legitimately take a while to transfer/parse -- give it a generous timeout.
    page.wait_for_load_state("load", timeout=180_000)

    body_text = page.locator("body").inner_text()
    if "Please Limit the search" in body_text:
        log.warning(f"  committees[{status}]: search rejected as too broad")
        return None
    if "record" not in body_text.lower():
        log.debug(f"  committees[{status}]: no records")
        return None

    filename = f"committees_{status.lower().replace(' ', '_')}.xls"
    out_path = RAW_DIR / filename

    with page.expect_download(timeout=180_000) as dl_info:
        page.click(f"#{P}btnExport")
    dl_info.value.save_as(str(out_path))

    rows = _read_xls_table(out_path)
    return filename, len(rows)


def collect_all_mecids(log) -> list[dict]:
    """Read back both committee-status export files and return the deduped union
    of committee rows (MECID, Committee, Candidate, Treasurer, Deputy Treasurer,
    Committee Type, Committee Status), keyed by MECID."""
    by_mecid: dict[str, dict] = {}
    for status in COMMITTEE_STATUSES:
        path = RAW_DIR / f"committees_{status.lower().replace(' ', '_')}.xls"
        if not path.exists():
            continue
        for row in _read_xls_table(path):
            mecid = row.get("MECID", "").strip()
            if mecid:
                by_mecid[mecid] = row
    log.info(f"  {len(by_mecid)} distinct MECIDs known from committee list exports")
    return list(by_mecid.values())


# ======================= Phase B: committee detail ======================
def _split_address_html(inner_html: str) -> tuple[str, str, str, str]:
    """MEC renders a mailing address as '<street>Line2<br>City, ST ZIP' inside one
    <span> with no space around the <br> — splitting on stripped textContent alone
    would run words together, so this expects raw inner_html and splits on <br>.
    Returns (street, city, state, zip)."""
    if not inner_html:
        return "", "", "", ""
    parts = [re.sub(r"<[^>]+>", "", p).strip() for p in re.split(r"<br\s*/?>", inner_html, flags=re.I)]
    parts = [p for p in parts if p]
    if not parts:
        return "", "", "", ""

    last = parts[-1]
    m = re.match(r"^(.*),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", last)
    if m:
        city, state, zip_code = m.group(1).strip(), m.group(2), m.group(3)
        street = " ".join(parts[:-1])
    else:
        # Couldn't confidently split city/state/zip off the last line — keep
        # everything as street rather than guess wrong.
        city = state = zip_code = ""
        street = " ".join(parts)
    return street, city, state, zip_code


def parse_comminfo_page(page) -> dict | None:
    """Parse a loaded CommInfo.aspx page (Information tab, the default) into a
    committee-detail dict plus a list of election-history dicts."""
    P = "ContentPlaceHolder_ContentPlaceHolder1_"

    def txt(field_id: str) -> str:
        loc = page.locator(f"#{field_id}")
        return loc.inner_text().strip() if loc.count() else ""

    def addr(field_id: str) -> tuple[str, str, str, str]:
        loc = page.locator(f"#{field_id}")
        if not loc.count():
            return "", "", "", ""
        return _split_address_html(loc.inner_html())

    mecid = txt(f"{P}lblMECID")
    if not mecid:
        return None

    street, city, state, zip_code = addr(f"{P}lblAddress")
    cand_street, cand_city, cand_state, cand_zip = addr(f"{P}lblCandAddress")
    tre_street, tre_city, tre_state, tre_zip = addr(f"{P}lblTreAddress")

    detail = {
        "mecid": mecid,
        "committee_name": txt(f"{P}lblCommName"),
        "committee_status": txt(f"{P}lblCommStatus"),
        "committee_type": txt(f"{P}lblCommType"),
        "term_date": txt(f"{P}lblTermDate"),
        "address": street, "city": city, "state": state, "zip": zip_code,
        "phone": txt(f"{P}lblPhone"),
        "candidate_name": txt(f"{P}lblCandName"),
        "cand_address": cand_street, "cand_city": cand_city,
        "cand_state": cand_state, "cand_zip": cand_zip,
        "cand_phone": txt(f"{P}lblCandPhone"),
        "party": txt(f"{P}lblParty"),
        "treasurer_name": txt(f"{P}lblTreName"),
        "tre_address": tre_street, "tre_city": tre_city,
        "tre_state": tre_state, "tre_zip": tre_zip,
        "tre_phone": txt(f"{P}lblTrePhone"),
    }

    # Election History gridview — 0-indexed rows, present only for candidate
    # committees. Stop at the first missing row rather than probing forever.
    elections = []
    i = 0
    while True:
        year_loc = page.locator(f"#{P}gvElecHistory_lblElecYear_{i}")
        if not year_loc.count():
            break
        elections.append({
            "mecid": mecid,
            "election_date": year_loc.inner_text().strip(),
            "election_type": txt(f"{P}gvElecHistory_lblElectionType_{i}"),
            "office": txt(f"{P}gvElecHistory_lblSub_{i}"),
            "district": txt(f"{P}gvElecHistory_lblDistrict_{i}"),
            "political_subdivision": txt(f"{P}gvElecHistory_lblPolSub_{i}"),
        })
        i += 1

    return detail, elections


def fetch_committee_detail(page, mecid: str, log, max_tries: int = PAGE_RETRY_TRIES):
    """Load and parse a single CommInfo.aspx page, reloading up to max_tries
    times if it looks like a bot-block/CAPTCHA interstitial got served
    instead of the real page. A missing #lblMECID after a successful goto()
    (parse_comminfo_page returning None) is ambiguous -- it could mean a bad
    MECID, or it could mean the WAF served a challenge page that doesn't
    match _goto_with_retry's plain 'Request Rejected' check -- so we retry
    with fresh reloads rather than immediately recording it as a hard error.
    Returns (detail, elections) or None if still unparseable after retries."""
    url = COMMINFO_URL.format(mecid=mecid)
    reason = "unknown"
    for attempt in range(1, max_tries + 1):
        if not _goto_with_retry(page, url, log):
            reason = "WAF rejection"
        else:
            parsed = parse_comminfo_page(page)
            if parsed is not None:
                return parsed
            body_text = page.locator("body").inner_text()
            reason = "bot/captcha screen suspected" if _looks_bot_blocked(body_text) else "page did not parse"

        if attempt < max_tries:
            log.warning(f"  committee detail {mecid}: {reason} (attempt {attempt}/{max_tries}) — reloading")
            time.sleep(PAGE_RETRY_SLEEP * attempt)

    log.page_scrape_error(entity="committee", page_id=mecid, error=f"gave up after {max_tries} attempts ({reason})")
    return None


def load_done_mecids() -> set[str]:
    if not COMMITTEE_DETAIL_PATH.exists():
        return set()
    with open(COMMITTEE_DETAIL_PATH, newline="", encoding="utf-8") as f:
        return {row["mecid"] for row in csv.DictReader(f) if row.get("mecid")}


def sweep_committee_details(page, mecids: list[str], log, force: bool = False) -> tuple[int, int]:
    done = set() if force else load_done_mecids()
    todo = [m for m in mecids if m not in done]

    if force:
        for p in (COMMITTEE_DETAIL_PATH, ELECTION_HISTORY_PATH):
            if p.exists():
                p.unlink()

    write_header_detail = force or not COMMITTEE_DETAIL_PATH.exists()
    write_header_elec    = force or not ELECTION_HISTORY_PATH.exists()

    log.info(f"Committee detail sweep: {len(todo)} to fetch ({len(done)} already done)")

    ok = err = consecutive_err = 0
    t0 = time.perf_counter()

    with open(COMMITTEE_DETAIL_PATH, "a", newline="", encoding="utf-8") as fh_detail, \
         open(ELECTION_HISTORY_PATH, "a", newline="", encoding="utf-8") as fh_elec:

        w_detail = csv.DictWriter(fh_detail, fieldnames=COMMITTEE_DETAIL_COLS, extrasaction="ignore")
        w_elec   = csv.DictWriter(fh_elec, fieldnames=ELECTION_HISTORY_COLS, extrasaction="ignore")
        if write_header_detail:
            w_detail.writeheader()
        if write_header_elec:
            w_elec.writeheader()

        with logging_redirect_tqdm(loggers=[log._log]):
            with tqdm(todo, desc="  committee detail", unit="mecid", dynamic_ncols=True, colour="green") as bar:
                for mecid in bar:
                    try:
                        parsed = fetch_committee_detail(page, mecid, log)
                        if parsed is None:
                            # fetch_committee_detail already logged the error
                            # (and exhausted its own reload retries).
                            err += 1
                            consecutive_err += 1
                            if consecutive_err >= MAX_CONSECUTIVE_ERRORS:
                                log.warning("Too many consecutive errors — stopping committee detail sweep")
                                break
                            continue

                        detail, elections = parsed
                        w_detail.writerow(detail)
                        for e in elections:
                            w_elec.writerow(e)

                        bar.set_postfix_str(detail["committee_name"][:40].ljust(40), refresh=False)
                        ok += 1
                        consecutive_err = 0
                        time.sleep(0.15)

                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        log.page_scrape_error(entity="committee", page_id=mecid, error=str(e))
                        err += 1
                        consecutive_err += 1
                        if consecutive_err >= MAX_CONSECUTIVE_ERRORS:
                            log.warning("Too many consecutive errors — stopping committee detail sweep")
                            break
                        time.sleep(1)

    total_rows = sum(1 for _ in open(COMMITTEE_DETAIL_PATH, encoding="utf-8")) - 1 if COMMITTEE_DETAIL_PATH.exists() else 0
    log.page_scrape_complete(filename=str(COMMITTEE_DETAIL_PATH), rows=total_rows,
                             duration_s=time.perf_counter() - t0, ok=ok, err=err)
    return ok, err


# ================= Phase D: contributions / expenditures =================
def _fmt_date(d: date) -> str:
    return d.strftime("%m/%d/%Y")


def _fmt_amt(x: float) -> str:
    """Plain decimal string for an amount-tier bound, e.g. for the
    txtStartAmt/txtEndAmt form fields and for filenames/log messages.
    Python's `:g` format switches to scientific notation once the exponent
    exceeds its precision -- confirmed this bites AMOUNT_TIER_BOUNDS'
    10_000_000 top bound specifically (f"{10_000_000:g}" == "1e+07"), which
    would have silently broken both the actual form field value (MEC almost
    certainly doesn't parse "1e+07" as ten million) and every filename for
    the top amount tier. Plain formatting avoids that entirely."""
    if float(x).is_integer():
        return str(int(x))
    return f"{x:.2f}".rstrip("0").rstrip(".")


def _search_cf12(page, context, main_tab_id: str, year: str, log,
                  from_date: str = "", to_date: str = "",
                  amt_lo: float | None = None, amt_hi: float | None = None,
                  mecid: str = "", max_tries: int = PAGE_RETRY_TRIES
                  ) -> tuple[str, object | None, int | None]:
    """Run one Advanced-Search on CF12_ContrExpend.aspx for a given main tab
    ('lbtnContr' or 'lbtnExpend'). Statewide by default (mecid left blank) --
    a Year + From/To date range is enough to get a real, cross-committee
    result set (confirmed live: 9,436 rows for just January 2026, spanning
    dozens of MECIDs); each row carries its own MECID/Committee Name, so
    there's no need to loop per committee the way this used to work. mecid
    is kept as an optional param for completeness, not because anything
    still relies on it.

    Must launch the browser with args=["--disable-popup-blocking"] — see module
    docstring. The results open in a genuine new window (window.open), not a same-tab
    navigation, fired from an onload handler after the search's full-page postback.

    Retries up to max_tries times (fresh reload of CF12_URL + redo the search)
    whenever the popup fails to open/load, or opens but looks like a bot/CAPTCHA
    interstitial rather than a genuine empty result set — see BOT_BLOCK_PHRASES.
    A genuinely empty result set is NOT retried, since that's legitimate
    "no data in this date/amount chunk".

    Returns (status, popup, result_count):
      ("ok", popup, N)     — popup has results; caller exports then closes it.
                             N is the on-screen "Full Disclosure Reports (N)"
                             count when available, else None.
      ("empty", None, 0)   — genuinely no data in this chunk. Not an error.
      ("blocked", None, None) — gave up after max_tries on a suspected WAF/
                             bot-block/CAPTCHA screen, or MEC's own
                             Error.aspx app bug. Distinct from "empty" so
                             callers can drive a consecutive-failure circuit
                             breaker without tripping it on ordinary empty
                             chunks, and without silently recording a
                             blocked chunk as a permanent "done, 0 rows"
                             manifest entry that a later plain re-run would
                             never retry.
    """
    P = "ContentPlaceHolder_ContentPlaceHolder1_"

    for attempt in range(1, max_tries + 1):
        try:
            if not _goto_with_retry(page, CF12_URL, log):
                raise RuntimeError("WAF rejection loading search form")

            # main_tab_id, lbtnAdvanced, and ddYear all trigger __doPostBack
            # full-page postbacks (confirmed live in the page's own HTML),
            # NOT simple client-side UI updates. main_tab_id/lbtnAdvanced
            # are triggered via _postback() (direct JS __doPostBack calls)
            # rather than physical Playwright clicks — confirmed in
            # production that clicking these links physically is unreliable
            # (the automated browser was observed hitting the page's
            # YouTube video embed instead of the link), which left the
            # postback never firing and the subsequent wait timing out
            # waiting for a navigation that never started. See _postback's
            # docstring for the full story. ddYear is triggered via
            # _select_year_and_postback(), which now prefers a native
            # page.select_option() over a direct JS __doPostBack() call —
            # see that function's docstring for why.
            #
            # lbtnContr ("Contributor") is the page's already-active default
            # tab on a fresh CF12_URL load. Sending its __doPostBack anyway
            # is a no-op on the server — and a no-op postback does not
            # regenerate __VIEWSTATE. Only send the main-tab postback when
            # we're actually switching tabs (i.e. for lbtnExpend).
            if main_tab_id != "lbtnContr":
                _postback(page, main_tab_id)
            page.wait_for_selector(f"#{P}lbtnAdvanced", timeout=30_000)
            _postback(page, "lbtnAdvanced")
            page.wait_for_selector(f"#{P}ddYear", timeout=30_000)

            _select_year_and_postback(page, f"{P}ddYear", year)
            if from_date:
                page.fill(f"#{P}txtFromDate", from_date)
            if to_date:
                page.fill(f"#{P}txtToDate", to_date)
            if amt_lo is not None:
                page.fill(f"#{P}txtStartAmt", _fmt_amt(amt_lo))
            if amt_hi is not None:
                # Adjacent amount tiers/splits share a boundary value (e.g.
                # tier [0,100) and [100,500) both touch 100) -- best-guess
                # assumption (not separately confirmed live) is that "Ending
                # at" is inclusive, same as most such range filters, so
                # filling the boundary value verbatim on both sides would
                # double-count any transaction landing exactly on it (e.g.
                # exactly $100.00, not an unlikely amount for a real
                # contribution). Subtracting a cent on the upper bound only
                # avoids that while keeping amt_hi itself the clean
                # round-number boundary everywhere else (filenames, logs,
                # split arithmetic).
                fill_hi = amt_hi - 0.01 if amt_hi < AMOUNT_TIER_BOUNDS[-1] else amt_hi
                page.fill(f"#{P}txtEndAmt", _fmt_amt(fill_hi))
            if mecid:
                page.fill(f"#{P}txtCommID", mecid)

            # "load" rather than "networkidle" — see download_committee_list's comment on
            # why networkidle is unreliable on this site (background widget polling can
            # prevent it from ever resolving, independent of whether the actual content
            # is ready).
            with context.expect_page(timeout=25_000) as popup_info:
                page.click(f"#{P}btnSearch")
            popup = popup_info.value
            popup.wait_for_load_state("load", timeout=60_000)
        except Exception as e:
            # Any unexpected failure anywhere in the search sequence above —
            # a WAF rejection, a stray in-flight postback blocking the next
            # action, the results popup never opening, etc. — is treated as
            # retryable rather than being allowed to propagate out of this
            # function. `except Exception` (not bare `except:`) so
            # KeyboardInterrupt still propagates, per the project's
            # error-handling contract.

            # Before falling back to the generic (slow) retry: confirmed
            # live in production that when MEC's own server-error bug fires
            # on btnSearch, the results page's onload handler — which is
            # what normally calls window.open() to show the results popup —
            # never runs at all, because the response is the entirely
            # different Error.aspx template rather than a page with that
            # script on it. So the browser navigates the MAIN page/tab to
            # the error in place; no popup ever opens. Checking the main
            # page's own state here lets this case take the same fast
            # "retry once, then give up" path as the popup case below.
            try:
                server_err = _mec_server_error(page.url, page.locator("body").inner_text())
            except Exception:
                server_err = None
            if server_err:
                if attempt == 1:
                    log.warning(f"  cf12 {year}: MEC server error ({server_err}) — retrying once")
                    time.sleep(PAGE_RETRY_SLEEP)
                    continue
                log.warning(f"  cf12 {year}: MEC server error persists ({server_err}) — giving up; "
                           f"likely a bug in MEC's own app for this chunk, not a transient block "
                           f"(won't be recorded as done, so a future re-run will retry it)")
                return "blocked", None, None

            if attempt < max_tries:
                log.warning(f"  cf12 {year}: {e} "
                           f"(attempt {attempt}/{max_tries}) — retrying")
                time.sleep(PAGE_RETRY_SLEEP * attempt)
                continue
            log.warning(f"  cf12 {year}: giving up after {max_tries} attempts ({e})")
            return "blocked", None, None

        popup_url = popup.url
        popup_text = popup.locator("body").inner_text()

        # Check the server-error case FIRST — it doesn't match any
        # BOT_BLOCK_PHRASES wording and would otherwise fall straight
        # through to "empty" and get permanently recorded as done with 0
        # rows. Confirmed live: the popup can navigate to
        # CF12_ContrExpendResults.aspx and get redirected to /mec/Error.aspx
        # with "Error Message: Count cannot be less than zero. Parameter
        # name: count" — a real bug on MEC's end, not a block.
        server_err = _mec_server_error(popup_url, popup_text)
        if server_err:
            popup.close()
            # Retry at most once regardless of max_tries — unlike a WAF
            # block, a fresh reload rarely fixes an application-level
            # exception, so burning the full retry budget just wastes time.
            if attempt == 1:
                log.warning(f"  cf12 {year}: MEC server error ({server_err}) — retrying once")
                time.sleep(PAGE_RETRY_SLEEP)
                continue
            log.warning(f"  cf12 {year}: MEC server error persists ({server_err}) — giving up; "
                       f"likely a bug in MEC's own app for this chunk, not a transient block "
                       f"(won't be recorded as done, so a future re-run will retry it)")
            return "blocked", None, None

        if _looks_bot_blocked(popup_text):
            popup.close()
            if attempt < max_tries:
                log.warning(f"  cf12 {year}: bot/captcha screen suspected in results popup "
                           f"(attempt {attempt}/{max_tries}) — retrying")
                time.sleep(PAGE_RETRY_SLEEP * attempt)
                continue
            return "blocked", None, None

        # Confirmed live: the results page's tab headings read e.g. "Full
        # Disclosure Reports (9436) 48 Hour > $5000 Reports (77)" regardless
        # of whether the search was scoped by committee or statewide —
        # trust this count when present rather than requiring an Export
        # button to already be visible (which needs a scroll/render check).
        m = re.search(r"Full Disclosure Reports \((\d+)\)", popup_text)
        if m:
            count = int(m.group(1))
            if count == 0:
                popup.close()
                return "empty", None, 0
            return "ok", popup, count

        if popup.locator("#ContentPlaceHolder_btnExport").count():
            return "ok", popup, None

        popup.close()
        return "empty", None, 0  # genuine no-results

    return "blocked", None, None


def _click_export(popup, timeout_ms: int = EXPORT_DOWNLOAD_TIMEOUT_MS):
    """Click Export and classify what happened.

    PREVIOUS VERSION OF THIS FUNCTION WAS WRONG — confirmed live in
    production: it read the export click's response directly via
    Response.body() instead of Playwright's "download" event API, on the
    theory that the response body IS the .xls content. That's true in
    principle, but doesn't matter in practice — a real production run
    showed 100% of export attempts failing with "no response received",
    even down to single-day/$25-wide chunks that should have had almost no
    data, WHILE the user could see the browser visibly completing real
    downloads on screen at the same time. Root cause: once Chromium decides
    a navigation is a download (via Content-Disposition), it hands the
    response off to the browser's download manager before Playwright's
    normal response/body-reading machinery gets a usable copy — this is
    exactly the well-known reason Playwright has a *separate* "download"
    event API in the first place, rather than expecting callers to read
    download responses like ordinary ones. Reverted to popup.expect_download()
    as the proven, correct primary mechanism (this is what actually saved
    real files earlier, e.g. A101165/A121061). A lightweight, NON-BLOCKING
    response listener is registered alongside it purely to still get fast
    diagnosis of an HTTP 5xx (confirmed real and reproducible — see
    docs/states/missouri.md) without needing to read the download's own body.

    Returns (kind, payload):
      ("ok", Download)        — success; payload is the Download object,
                                 caller should .save_as(...).
      ("server_error", None)  — MEC's own Error.aspx app bug (see
                                 _mec_server_error). NOT retried by the
                                 caller — confirmed not to be transient.
      ("http_error", status)  — the export POST itself returned an HTTP
                                 5xx. Confirmed live: a bare 503 Service
                                 Unavailable on the SAME url (no redirect to
                                 Error.aspx) for result sets of very
                                 different sizes, so this is HTTP's standard
                                 "try again later," retried with backoff by
                                 the caller — distinct from server_error.
      ("bot_block", None)     — popup body matches BOT_BLOCK_PHRASES after
                                 a non-download response came back.
      ("timeout", str)        — no download and nothing diagnosable in the
                                 window; payload is the raw exception text
                                 for debugging (this used to be silently
                                 discarded, which is part of why the bug
                                 above took a real production run to catch).
    """
    captured = {}

    def _on_response(resp):
        try:
            if resp.request.method == "POST" and "CF12_ContrExpendResults" in resp.url:
                captured["status"] = resp.status
        except Exception:
            pass

    popup.on("response", _on_response)
    try:
        with popup.expect_download(timeout=timeout_ms) as dl_info:
            popup.click("#ContentPlaceHolder_btnExport")
        return "ok", dl_info.value
    except Exception as e:
        click_err = str(e)
    finally:
        popup.remove_listener("response", _on_response)

    status = captured.get("status")
    if status is not None and status >= 500:
        return "http_error", status
    try:
        popup_text = popup.locator("body").inner_text()
    except Exception:
        popup_text = ""
    if _mec_server_error(popup.url, popup_text):
        return "server_error", None
    if _looks_bot_blocked(popup_text):
        return "bot_block", None
    return "timeout", click_err


def _export_from_popup(popup, relation_type: str, year: str, from_s: str, to_s: str,
                       amt_lo: float, amt_hi: float, log) -> tuple[str, tuple[str, int] | None]:
    """Export the results already loaded in `popup` to a chunk-scoped file.
    Returns (status, result) — status is 'ok'/'blocked'; result is
    (filename, row_count) when status == 'ok', else None. Always closes
    popup before returning."""
    out_dir = CONTRIB_DIR if relation_type == "contributions" else EXPEND_DIR
    chunk_label = f"{year}/{relation_type} [{from_s}-{to_s}, ${_fmt_amt(amt_lo)}-${_fmt_amt(amt_hi)}]"
    filename = (f"{year}_{from_s.replace('/', '')}-{to_s.replace('/', '')}"
               f"_{_fmt_amt(amt_lo)}-{_fmt_amt(amt_hi)}.xls")
    out_path = out_dir / filename

    bot_retries = http_retries = dropped_retries = 0
    try:
        while True:
            kind, payload = _click_export(popup)
            if kind == "ok":
                payload.save_as(str(out_path))
                break
            if kind == "server_error":
                log.warning(f"  transactions {chunk_label}: MEC server error on export — "
                           f"giving up (app-level bug, not retried)")
                return "blocked", None
            if kind == "http_error":
                if http_retries < MAX_EXPORT_HTTP_ERROR_RETRIES:
                    http_retries += 1
                    log.warning(f"  transactions {chunk_label}: export POST returned HTTP "
                               f"{payload} (attempt {http_retries}/{MAX_EXPORT_HTTP_ERROR_RETRIES}) "
                               f"— transient, backing off and retrying")
                    time.sleep(PAGE_RETRY_SLEEP * http_retries)
                    continue
                log.warning(f"  transactions {chunk_label}: export POST still returning HTTP "
                           f"errors after {MAX_EXPORT_HTTP_ERROR_RETRIES} retries — giving up "
                           f"on this chunk for now")
                return "blocked", None
            if kind == "bot_block":
                if bot_retries < MAX_EXPORT_BOT_RETRIES:
                    bot_retries += 1
                    log.warning(f"  transactions {chunk_label}: bot/captcha screen on export "
                               f"(attempt {bot_retries}/{MAX_EXPORT_BOT_RETRIES}) — retrying click")
                    time.sleep(PAGE_RETRY_SLEEP * bot_retries)
                    continue
                log.warning(f"  transactions {chunk_label}: bot/captcha screen on export "
                           f"persists — giving up")
                return "blocked", None
            # "timeout" — no download and nothing diagnosable; payload is
            # the actual click/download exception text (see _click_export's
            # docstring for why that matters here).
            if dropped_retries < 2:
                dropped_retries += 1
                log.warning(f"  transactions {chunk_label}: export download didn't complete "
                           f"(attempt {dropped_retries}/2): {payload} — retrying")
                time.sleep(PAGE_RETRY_SLEEP)
                continue
            log.warning(f"  transactions {chunk_label}: export failed after retries: {payload}")
            return "blocked", None
    finally:
        try:
            popup.close()
        except Exception:
            pass

    rows = _read_xls_table(out_path)
    return "ok", (filename, len(rows))


def _process_date_amount_chunk(page, context, relation_type: str, year: int,
                               from_d: date, to_d: date, amt_lo: float, amt_hi: float,
                               log, depth: int = 0) -> list[tuple[str, object, str]]:
    """Search+export one statewide (date range, amount range) chunk,
    recursively narrowing (date axis first, then amount axis) whenever the
    chunk is too large to export or the export itself keeps failing. This
    is the replacement for the old per-committee sweep — see module
    docstring for why: a Committee ID filter turned out not to be required
    at all, just a From/To date range, and per-committee search was also
    the actual cause of most of the recurring export failures, not
    anything specific to individual committees.

    Splitting triggers on two independent signals, since neither alone was
    reliable in production testing: (1) SAFE_EXPORT_ROW_THRESHOLD, a
    proactive guess based on the on-screen result count (checked BEFORE
    attempting export, to avoid wasting time on an export that's very
    likely to fail); and (2) an export that fails anyway after its own
    retries — confirmed live that MEC's export endpoint can return a bare
    HTTP 503 even for a modest 396-row chunk, so size alone doesn't
    guarantee success and a failure needs a fallback response too. Depth is
    capped at MAX_CHUNK_SPLIT_DEPTH so a chunk that's simply unexportable
    (not size-related) can't spin forever — it's reported as 'blocked' and
    left for a future re-run rather than retried indefinitely.

    Returns a list of (status, result, chunk_label) for every LEAF chunk
    actually resolved (each is 'ok'/'empty'/'blocked'), so the caller can
    tally totals across however many pieces this chunk ended up split into.
    """
    main_tab_id = "lbtnContr" if relation_type == "contributions" else "lbtnExpend"
    from_s, to_s = _fmt_date(from_d), _fmt_date(to_d)
    chunk_label = f"{year}/{relation_type} [{from_s}-{to_s}, ${_fmt_amt(amt_lo)}-${_fmt_amt(amt_hi)}]"

    can_split_date = (to_d - from_d).days >= 1
    can_split_amt = (amt_hi - amt_lo) > 1
    can_split = depth < MAX_CHUNK_SPLIT_DEPTH and (can_split_date or can_split_amt)

    status, popup, count = _search_cf12(page, context, main_tab_id, str(year), log,
                                        from_date=from_s, to_date=to_s,
                                        amt_lo=amt_lo, amt_hi=amt_hi)

    if status == "blocked" and can_split:
        log.warning(f"  {chunk_label}: search itself failed — splitting and retrying narrower")
        return _split_and_recurse(page, context, relation_type, year, from_d, to_d,
                                  amt_lo, amt_hi, log, depth, can_split_date)
    if status != "ok":
        return [(status, None, chunk_label)]

    if count is not None and count > SAFE_EXPORT_ROW_THRESHOLD and can_split:
        try:
            popup.close()
        except Exception:
            pass
        log.info(f"  {chunk_label}: {count} rows — splitting before export")
        return _split_and_recurse(page, context, relation_type, year, from_d, to_d,
                                  amt_lo, amt_hi, log, depth, can_split_date)

    export_status, result = _export_from_popup(popup, relation_type, str(year), from_s, to_s,
                                               amt_lo, amt_hi, log)
    if export_status == "blocked" and can_split:
        log.warning(f"  {chunk_label}: export failed even after retries — splitting and retrying narrower")
        return _split_and_recurse(page, context, relation_type, year, from_d, to_d,
                                  amt_lo, amt_hi, log, depth, can_split_date)
    return [(export_status, result, chunk_label)]


def _split_and_recurse(page, context, relation_type: str, year: int, from_d: date, to_d: date,
                       amt_lo: float, amt_hi: float, log, depth: int,
                       prefer_date: bool) -> list[tuple[str, object, str]]:
    """Halve a chunk along one axis (date if it still has more than a day of
    range, else amount) and recurse into both halves."""
    results = []
    if prefer_date:
        mid = from_d + timedelta(days=(to_d - from_d).days // 2)
        results += _process_date_amount_chunk(page, context, relation_type, year, from_d, mid,
                                              amt_lo, amt_hi, log, depth + 1)
        results += _process_date_amount_chunk(page, context, relation_type, year,
                                              mid + timedelta(days=1), to_d,
                                              amt_lo, amt_hi, log, depth + 1)
    else:
        mid = amt_lo + (amt_hi - amt_lo) / 2
        results += _process_date_amount_chunk(page, context, relation_type, year, from_d, to_d,
                                              amt_lo, mid, log, depth + 1)
        results += _process_date_amount_chunk(page, context, relation_type, year, from_d, to_d,
                                              mid, amt_hi, log, depth + 1)
    return results


# ================ Phase E: independent expenditures =====================
def download_independent_expenditures(page, year: str, log,
                                       max_tries: int = PAGE_RETRY_TRIES) -> tuple[str, tuple[str, int] | None]:
    """Search CF_SearchDirExp.aspx ('Committee Expenditures for Candidates' --
    MEC's independent-expenditure report) for Report Year=<year>, Support/
    Oppose filter = All, and export the results grid.

    Unlike Phase D, Committee ID is NOT a required filter here -- Report Year
    is the only required field, so this is a genuine statewide bulk export
    swept once per year rather than once per (committee, year). Retries up to
    max_tries times on a suspected WAF/bot-block/CAPTCHA screen, same pattern
    as fetch_committee_detail/_search_cf12.

    Returns (status, result) — status is 'ok'/'empty'/'blocked' (see
    _search_cf12's docstring for what each means and why they're kept
    distinct); result is (filename, row_count) when status == 'ok', else None."""
    P = "ContentPlaceHolder_ContentPlaceHolder1_"

    for attempt in range(1, max_tries + 1):
        try:
            if not _goto_with_retry(page, CF_DIREXP_URL, log):
                raise RuntimeError("WAF rejection loading search form")

            page.wait_for_selector(f"#{P}ddYear", timeout=30_000)
            # Same ddYear postback as _search_cf12 — see _select_year_and_postback's
            # docstring for the native select_option()-first, raw-JS-fallback
            # trigger strategy and why it replaced a raw-JS-only trigger.
            _select_year_and_postback(page, f"{P}ddYear", year)
            page.check(f"#{P}radbtnAll")

            # btnSearch is a genuine <input type="submit"> (confirmed live via
            # direct DOM inspection: no onclick handler, form action/method
            # point at a same-URL POST) — so on paper a plain click() + wait
            # for "load" should be fine. Confirmed NOT fine in production:
            # a --start-year backfill across older years needed a human to
            # physically click Search in the visible browser before the
            # sweep would proceed at all, even though the exact same click
            # replayed manually in a plain (non-Playwright) browser tab
            # returns real results in a few seconds. Same root cause as
            # every other same-URL-POST navigation on this site (see
            # _wait_for_postback's docstring) -- Playwright's load-state/
            # navigation detection is not reliable for these, regardless of
            # whether the postback is JS-driven (__doPostBack) or a genuine
            # form submit button. Switched to the same VIEWSTATE-diff
            # detection used for every other postback on this page instead
            # of trusting wait_for_load_state to notice the click landed.
            _wait_for_postback(page, lambda: page.click(f"#{P}btnSearch"), timeout=120_000)

            body_text = page.locator("body").inner_text()
        except Exception as e:
            # Same rationale as _search_cf12: any unexpected failure in the
            # search sequence itself (WAF rejection, a stray in-flight
            # postback, a selector timeout, etc.) is retryable rather than
            # being allowed to crash straight past this function's own
            # retry/backoff design. `except Exception` (not bare `except:`)
            # so KeyboardInterrupt still propagates.
            if attempt < max_tries:
                log.warning(f"  independent_expenditures[{year}]: {e} "
                           f"(attempt {attempt}/{max_tries}) — retrying")
                time.sleep(PAGE_RETRY_SLEEP * attempt)
                continue
            log.warning(f"  independent_expenditures[{year}]: giving up after "
                       f"{max_tries} attempts ({e})")
            return "blocked", None

        # Check MEC's own server-error page before anything else — see
        # _search_cf12's comment on why this must never fall through to
        # "empty" (it has neither Export-button-style structure nor any
        # BOT_BLOCK_PHRASES wording, so without this check it would).
        server_err = _mec_server_error(page.url, body_text)
        if server_err:
            if attempt == 1:
                log.warning(f"  independent_expenditures[{year}]: MEC server error "
                           f"({server_err}) — retrying once")
                time.sleep(PAGE_RETRY_SLEEP)
                continue
            log.warning(f"  independent_expenditures[{year}]: MEC server error persists "
                       f"({server_err}) — giving up (won't be recorded as done)")
            return "blocked", None

        if _looks_bot_blocked(body_text):
            if attempt < max_tries:
                log.warning(f"  independent_expenditures[{year}]: bot/captcha screen suspected "
                           f"(attempt {attempt}/{max_tries}) — retrying")
                time.sleep(PAGE_RETRY_SLEEP * attempt)
                continue
            return "blocked", None
        if "record" not in body_text.lower():
            log.debug(f"  independent_expenditures[{year}]: no records")
            return "empty", None

        filename = f"independent_expenditures_{year}.xls"
        out_path = RAW_DIR / filename

        try:
            # See EXPORT_DOWNLOAD_TIMEOUT_MS's comment for why this is short:
            # a failed export never eventually arrives no matter how long we
            # wait, so there's no value in a long timeout.
            with page.expect_download(timeout=EXPORT_DOWNLOAD_TIMEOUT_MS) as dl_info:
                page.click(f"#{P}btnExport")
            dl_info.value.save_as(str(out_path))
        except Exception:
            # Unlike Phase D's _export_from_popup, this loop already reloads
            # the whole search from scratch on any retry (see top of this
            # `for` loop), so both the MEC-server-error and bot-block cases
            # get a fresh attempt here rather than needing separate handling
            # — the MEC-server-error check above (before the export click)
            # will catch a persistent app-level bug and give up appropriately
            # on its own next time through.
            if attempt < max_tries:
                log.warning(f"  independent_expenditures[{year}]: export download failed "
                           f"(attempt {attempt}/{max_tries}) — retrying")
                time.sleep(PAGE_RETRY_SLEEP * attempt)
                continue
            return "blocked", None

        rows = _read_xls_table(out_path)
        return "ok", (filename, len(rows))

    return "blocked", None


# ============================ orchestrator ============================
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
    independent_expenditures: bool = False,
):
    """Orchestrate the Missouri scrape.

    Vertical scope (mutually exclusive):
        force=True              — re-fetch everything in scope, wipe relevant manifest
        start_year / end_year   — sweep Phase D/E (transactions, independent
                                   expenditures) over this year range
        (neither)                — incremental: Phase D/E cover the current year only

    Horizontal scope:
        No flags           — everything (Phase A + B + D + E)
        transactions       — Phase D + E (contributions + expenditures + independent
                              expenditures — all three are "money moving" data swept
                              by year)
        entities           — Phase A + B only (committee list + per-MECID detail)
        contributions       — Phase D, contributions only
        expenditures        — Phase D, expenditures only
        independent_expenditures — Phase E only (committee spending FOR/AGAINST a
                              candidate — see module docstring on how this differs
                              from Phase D's ordinary committee "Expenditure" tab)
        candidates          — Phase A + B (MO's detail page carries candidate fields
                               together with committee fields — can't be split at
                               scrape time, only when the parser builds candidates.csv)
        committees           — Phase A + B (same reasoning as candidates, above)
    """
    log = get_logger("missouri", "scrape")
    t0 = time.perf_counter()
    log.info("Starting Missouri scraper")
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees,
              independent_expenditures=independent_expenditures)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("[!] Playwright not installed. Run: pip install playwright && playwright install chromium")
        log._emit("scrape_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, pages_ok=0, pages_err=0,
                  error="playwright not installed")
        return

    no_horizontal = not (entities or transactions or contributions or expenditures or
                         candidates or committees or independent_expenditures)

    do_committee_list          = no_horizontal or entities or candidates or committees
    do_committee_detail        = no_horizontal or entities or candidates or committees
    do_contributions           = no_horizontal or transactions or contributions
    do_expenditures            = no_horizontal or transactions or expenditures
    do_independent_expenditures = no_horizontal or transactions or independent_expenditures

    current_year = datetime.today().year
    if force:
        # force just means "re-fetch the requested range, or current year if no
        # range was given" — there's no meaningful "all years" for Phase D since
        # the site has no per-committee filing-history index cheap enough to sweep
        # (see docs/states/missouri.md's Election History discussion).
        wipe_years = range(start_year or current_year, (end_year or current_year) + 1)
    elif start_year is not None or end_year is not None:
        wipe_years = range(start_year or (end_year or current_year),
                            (end_year or start_year) + 1)
    else:
        wipe_years = range(current_year, current_year + 1)

    files_ok = files_err = pages_ok = pages_err = 0

    if force or start_year is not None or end_year is not None:
        def _outside_range(r: dict) -> bool:
            # "independent_expenditures" keys are bare years (e.g. "2026").
            # "contributions"/"expenditures" keys are now
            # "{year}_{fromISO}_{toISO}_{amtlo}-{amthi}" (statewide
            # date+amount chunks, not per-committee) -- year is the FIRST
            # token in both formats, so key.split("_")[0] covers both
            # (a bare year has no "_", so split()[0] is just the year).
            if r["relation_type"] not in ("contributions", "expenditures", "independent_expenditures"):
                return True
            key_year = r["key"].split("_")[0]
            return key_year not in {str(y) for y in wipe_years}

        strip_manifest(_outside_range)

    done = load_manifest()

    try:
        with sync_playwright() as p:
            # Persistent profile (PROFILE_DIR), not a fresh launch()/
            # new_context() pair. Confirmed by the user directly: the very
            # first navigation of a scrape can hit an interactive CAPTCHA --
            # not the plain "Request Rejected" text _goto_with_retry already
            # auto-clears with a reload+sleep, but a genuine human-solve
            # challenge (confirmed: reloading alone does NOT get past it).
            # A throwaway new_context() starts with zero cookies every run,
            # so MEC's Incapsula WAF has no way to recognize a returning,
            # already-trusted session and a fresh challenge was effectively
            # guaranteed on every single invocation.
            # launch_persistent_context() writes cookies (including
            # whatever trust/session cookie Incapsula sets after a challenge
            # is solved) to PROFILE_DIR on disk. Solve the CAPTCHA by hand
            # once with this profile and later runs reuse those cookies --
            # as long as the trust cookie hasn't expired/been invalidated,
            # the challenge doesn't reappear and the run needs zero human
            # intervention. If it ever does reappear, it's a signal the
            # cookie expired or MEC's WAF invalidated it, not a code bug --
            # just solve it again once and later runs go back to unattended.
            context = p.chromium.launch_persistent_context(
                str(PROFILE_DIR), headless=False, accept_downloads=True,
                args=["--disable-popup-blocking"],
            )
            # Confirmed live: switching _select_year_and_postback's ddYear
            # trigger to a native page.select_option() (instead of a
            # page.evaluate()-only __doPostBack() call) fixed that specific
            # stuck step. But on the very next run, past that fixed step,
            # Phase E's btnSearch -- already a genuine Playwright
            # page.click(), the most "native" interaction Playwright has to
            # offer for a plain <input type="submit"> -- was STILL confirmed
            # to get stuck requiring a manual click. Since there's no
            # more-native trigger left to swap to, the remaining suspect
            # isn't which interaction API fires the postback at all -- it's
            # that Chromium launched under Playwright's control sets
            # `navigator.webdriver = true` on every page by default (a
            # native Chromium `--enable-automation` behavior, not something
            # Playwright itself adds), which is exactly the kind of signal
            # Incapsula/Imperva-style behavioral bot-scoring is built to
            # key on, independent of whether any individual triggering DOM
            # event is "trusted". Patched here via a context-level
            # add_init_script() -- applies to every page created in this
            # context, including popups (e.g. Phase D's results popup),
            # since it's registered before any page script runs -- to
            # override navigator.webdriver back to undefined, the standard
            # anti-fingerprinting technique for exactly this signal.
            # NOT yet independently confirmed to fix the Search-button
            # stall (this is a third fix attempt for this class of "genuine
            # click needed to unstick it" symptom); worth watching the next
            # backfill run closely rather than assuming it's solved.
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.new_page()

            # ── Phase A: committee/candidate registry ─────────────────
            if do_committee_list:
                log.info("\nMissouri committee list:")
                for status in COMMITTEE_STATUSES:
                    key = status
                    expected_stem = f"committees_{status.lower().replace(' ', '_')}.xls"
                    already_done = ("committees", key) in done and not force
                    if already_done:
                        log.file_download_skip(filename=expected_stem)
                        continue

                    log.file_download_start(filename=expected_stem)
                    t_file = time.perf_counter()
                    try:
                        result = download_committee_list(page, status, log)
                    except Exception as e:
                        log.file_download_error(filename=expected_stem, error=str(e))
                        files_err += 1
                        continue

                    if result is None:
                        continue

                    filename, row_count = result
                    size = (RAW_DIR / filename).stat().st_size
                    log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                         duration_s=time.perf_counter() - t_file)
                    files_ok += 1
                    upsert_manifest({"relation_type": "committees", "key": key,
                                     "filename": filename, "row_count": row_count})
                    done.add(("committees", key))
                    time.sleep(1)

            # ── Phase B: per-committee detail sweep ────────────────────
            if do_committee_detail:
                log.info("\nMissouri committee detail sweep:")
                mecid_rows = collect_all_mecids(log)
                mecids = [r["MECID"].strip() for r in mecid_rows if r.get("MECID", "").strip()]
                p_ok, p_err = sweep_committee_details(page, mecids, log, force=force)
                pages_ok += p_ok
                pages_err += p_err

            # ── Phase D: statewide contributions/expenditures, by date+amount chunk ──
            if do_contributions or do_expenditures:
                log.info("\nMissouri contributions/expenditures:")

                relation_types = []
                if do_contributions:
                    relation_types.append("contributions")
                if do_expenditures:
                    relation_types.append("expenditures")

                # Top-level chunk enumeration is fixed and deterministic (this
                # is what resumability keys off of); each one may fan out
                # into many smaller sub-chunks internally via
                # _process_date_amount_chunk's adaptive splitting, but that
                # splitting is invisible to the manifest — only whether the
                # WHOLE top-level chunk fully succeeded gets recorded. A
                # partial failure (any sub-chunk still "blocked" after
                # hitting max split depth) leaves the top-level chunk
                # unmarked, so a future re-run retries it entirely rather
                # than trying to reconstruct which specific slice failed.
                # Re-downloading already-succeeded sub-chunks on such a
                # retry just overwrites identical files — wasted work, but
                # safe.
                top_chunks = []
                for year in wipe_years:
                    y_start, y_end = date(year, 1, 1), date(year, 12, 31)
                    for amt_lo, amt_hi in zip(AMOUNT_TIER_BOUNDS, AMOUNT_TIER_BOUNDS[1:]):
                        d = y_start
                        while d <= y_end:
                            chunk_end = min(d + timedelta(days=DATE_CHUNK_DAYS - 1), y_end)
                            top_chunks.append((year, d, chunk_end, amt_lo, amt_hi))
                            d = chunk_end + timedelta(days=1)

                total = len(top_chunks) * len(relation_types)
                consecutive_err = 0
                circuit_broken = False
                with logging_redirect_tqdm(loggers=[log._log]):
                    with tqdm(total=total, desc="  transactions", unit="chunk",
                             dynamic_ncols=True, colour="cyan") as bar:
                        for relation_type in relation_types:
                            if circuit_broken:
                                break
                            for (year, from_d, to_d, amt_lo, amt_hi) in top_chunks:
                                if circuit_broken:
                                    break
                                key = (f"{year}_{from_d.isoformat()}_{to_d.isoformat()}"
                                      f"_{_fmt_amt(amt_lo)}-{_fmt_amt(amt_hi)}")
                                if (relation_type, key) in done and not force:
                                    bar.update(1)
                                    continue

                                try:
                                    leaves = _process_date_amount_chunk(
                                        page, context, relation_type, year, from_d, to_d,
                                        amt_lo, amt_hi, log
                                    )
                                except KeyboardInterrupt:
                                    raise
                                except Exception as e:
                                    log.page_scrape_error(entity=relation_type, page_id=key, error=str(e))
                                    files_err += 1
                                    consecutive_err += 1
                                    bar.update(1)
                                    time.sleep(1)
                                    if consecutive_err >= MAX_CONSECUTIVE_ERRORS:
                                        log.warning(f"{MAX_CONSECUTIVE_ERRORS} consecutive errors — "
                                                   f"stopping transactions sweep at {relation_type}/{key}")
                                        circuit_broken = True
                                        break
                                    continue

                                chunk_row_total = 0
                                any_blocked = False
                                for leaf_status, leaf_result, leaf_label in leaves:
                                    if leaf_status == "blocked":
                                        any_blocked = True
                                        files_err += 1
                                    elif leaf_status == "ok":
                                        filename, row_count = leaf_result
                                        out_dir = CONTRIB_DIR if relation_type == "contributions" else EXPEND_DIR
                                        size = (out_dir / filename).stat().st_size
                                        log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                                             duration_s=0.0)
                                        files_ok += 1
                                        chunk_row_total += row_count
                                    # "empty" leaves need no per-leaf action

                                if any_blocked:
                                    # At least one sub-chunk gave up after retries even after
                                    # adaptive splitting — the specific reason(s) were already
                                    # logged as warnings at the point of failure. Do NOT record
                                    # this top-level chunk as done — a later plain re-run needs
                                    # to retry the whole thing (see comment above top_chunks).
                                    log.page_scrape_error(entity=relation_type, page_id=key,
                                                         error="one or more sub-chunks gave up after "
                                                               "retries (see warnings above)")
                                    consecutive_err += 1
                                    if consecutive_err >= MAX_CONSECUTIVE_ERRORS:
                                        log.warning(f"{MAX_CONSECUTIVE_ERRORS} consecutive errors — "
                                                   f"stopping transactions sweep at {relation_type}/{key}")
                                        circuit_broken = True
                                else:
                                    consecutive_err = 0
                                    upsert_manifest({"relation_type": relation_type, "key": key,
                                                     "filename": f"{key}.xls", "row_count": chunk_row_total})
                                    done.add((relation_type, key))

                                bar.update(1)
                                time.sleep(0.2)

                if circuit_broken:
                    log.warning("Transactions sweep stopped early — re-run (without --force) to "
                               "pick up where it left off once the block clears.")

            # ── Phase E: independent expenditures (committee spending FOR/AGAINST
            #    a candidate) ─────────────────────────────────────────────
            if do_independent_expenditures:
                log.info("\nMissouri independent expenditures (committee spending for/against candidates):")
                consecutive_err = 0
                for year in wipe_years:
                    key = str(year)
                    expected_stem = f"independent_expenditures_{year}.xls"
                    if ("independent_expenditures", key) in done and not force:
                        log.file_download_skip(filename=expected_stem)
                        continue

                    log.file_download_start(filename=expected_stem)
                    t_file = time.perf_counter()
                    try:
                        status, result = download_independent_expenditures(page, str(year), log)
                    except Exception as e:
                        log.file_download_error(filename=expected_stem, error=str(e))
                        files_err += 1
                        consecutive_err += 1
                        if consecutive_err >= MAX_CONSECUTIVE_ERRORS:
                            log.warning(f"{MAX_CONSECUTIVE_ERRORS} consecutive errors — "
                                       f"stopping independent expenditures sweep at year {year}")
                            break
                        continue

                    if status == "blocked":
                        # Same reasoning as Phase D: don't record a suspected block as
                        # "done, 0 rows" — that would permanently hide it from a plain re-run.
                        # Specific reason already logged as a warning at the point of failure.
                        log.file_download_error(filename=expected_stem,
                                                error="gave up after retries (see warning above for reason)")
                        files_err += 1
                        consecutive_err += 1
                        if consecutive_err >= MAX_CONSECUTIVE_ERRORS:
                            log.warning(f"{MAX_CONSECUTIVE_ERRORS} consecutive errors — "
                                       f"stopping independent expenditures sweep at year {year}")
                            break
                        continue

                    consecutive_err = 0
                    row_count = 0
                    if status == "ok":
                        filename, row_count = result
                        size = (RAW_DIR / filename).stat().st_size
                        log.file_download_ok(filename=filename, bytes=size, rows=row_count,
                                             duration_s=time.perf_counter() - t_file)
                        files_ok += 1

                    # Record the year as done regardless of row count, so a
                    # genuinely-empty year isn't re-swept every run.
                    upsert_manifest({"relation_type": "independent_expenditures", "key": key,
                                     "filename": expected_stem, "row_count": row_count})
                    done.add(("independent_expenditures", key))
                    time.sleep(1)

            context.close()

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)

    except KeyboardInterrupt:
        log.warning("Interrupted")
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


# ====== CLI ==================================
if __name__ == "__main__":
    # Vertical scope (mutually exclusive):
    #   (no flag)                    incremental — current year only for Phase D
    #   --start-year / --end-year    sweep Phase D over this year range
    #   --force                      re-fetch everything in scope, wipe manifest entries
    #
    # Horizontal scope:
    #   (no flag)                       everything (committee list + detail + transactions + IEs)
    #   --transactions                  contributions + expenditures + independent expenditures
    #   --entities                      committee list + per-MECID detail sweep only
    #   --contributions                 contributions only
    #   --expenditures                  expenditures only
    #   --independent-expenditures      independent expenditures only (see module docstring,
    #                                   Phase E — committee spending FOR/AGAINST a candidate,
    #                                   distinct from --expenditures' ordinary committee spending)
    #   --candidates      committee list + detail sweep (MO can't split cand/comm at scrape time)
    #   --committees      committee list + detail sweep (same as --candidates in MO)
    import argparse
    ap = argparse.ArgumentParser(
        description="Download Missouri Ethics Commission campaign finance data."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force", action="store_true",
                      help="re-fetch everything in scope, wipe relevant manifest entries")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to sweep for transactions (inclusive)")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to sweep for transactions (inclusive, <= current year); "
                         "use with or without --start-year")

    ap.add_argument("--transactions", action="store_true",
                    help="transactions only (contributions + expenditures + independent expenditures)")
    ap.add_argument("--entities", action="store_true",
                    help="entities only (committee list + per-MECID detail sweep)")

    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures", action="store_true", help="expenditures only")
    ap.add_argument("--independent-expenditures", action="store_true",
                    help="independent expenditures only (committee spending FOR/AGAINST a "
                         "candidate — see module docstring, Phase E)")
    ap.add_argument("--candidates", action="store_true",
                    help="committee list + detail sweep (candidate fields live on the same page)")
    ap.add_argument("--committees", action="store_true",
                    help="committee list + detail sweep")

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
            independent_expenditures=args.independent_expenditures,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
