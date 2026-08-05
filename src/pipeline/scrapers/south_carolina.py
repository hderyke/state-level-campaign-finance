"""
scrapers/south_carolina.py — Download South Carolina campaign finance data from
the SC State Ethics Commission's public reporting portal (ethicsfiling.sc.gov).

Source: an Angular SPA with three public search screens, each backed by a Kendo
UI grid:

    /public/campaign-reports/contributions   → itemized contributions, by year
    /public/campaign-reports/expenditures    → itemized expenditures, by year
    /public/campaign-reports/reports         → filed disclosure reports, by
                                               election year (the only place the
                                               portal exposes candidate/office/
                                               election-type metadata in bulk)

Uses Selenium rather than Playwright — SC is the only state in the pipeline on
Selenium (see docs/states/south_carolina.md).

WHY THIS SCRAPER DISCOVERS ITS OWN ENDPOINT
-------------------------------------------
The obvious acquisition path — the grid's "Download Results" button — is a
Kendo `kendoGridExcelCommand`. Kendo builds that .xlsx entirely in the browser
(it zips XML in memory) and hands it to a Blob URL, so no HTTP request for the
file is ever made and there is nothing to intercept or replay. Driving that
button also caps the output at the 7-10 columns the grid happens to render, and
forces the grid to lay out 30k+ rows before the export can start.

What *is* observable is the single JSON search request the app fires when
"Search" is clicked — the grid is not server-paged, so one request returns the
entire result set for that year. That response carries every field the API
knows about, not just the visible columns.

The route for that request lives in a lazy-loaded Angular chunk that is only
fetched once you interact with the tab, so it can't be read off a saved copy of
the page and hardcoding it would mean guessing. Instead this scraper:

    1. Opens the search page in Chrome with CDP network logging enabled.
    2. Runs one search through the UI.
    3. Reads the performance log, pulls the JSON response that actually carried
       the grid rows, and saves that request as a "recipe" —
       {method, url, post body, headers}.
    4. Replays the recipe for every remaining year with an in-page `fetch()`,
       swapping the year token in the URL/body. No UI interaction, no grid
       rendering, no Excel export.

The recipe is cached at data/South Carolina/api_recipe.json so later runs skip
steps 1-3 entirely. If a replay stops returning rows the recipe is discarded and
rediscovered automatically, so a route change costs one extra page load rather
than a code change. `--rediscover` forces that refresh.

HOW THE NETWORK CAPTURE WORKS
-----------------------------
Selenium has no direct network-event listener, so capture goes through Chrome
DevTools Protocol in two parts:

  - `goog:loggingPrefs {"performance": "ALL"}` makes chromedriver buffer CDP
    events, retrievable with `driver.get_log("performance")`. That yields
    `Network.requestWillBeSent` (url, method, headers, post body) and
    `Network.responseReceived` (requestId, mimeType, resource type) — but not
    response bodies.
  - `Network.getResponseBody` fetches each body by requestId. Bodies only live
    in Chrome's per-page buffer, and the default buffer is far too small for a
    30k-row response, so `Network.enable` is re-issued with enlarged
    `maxResourceBufferSize`/`maxTotalBufferSize` — without that the discovery
    body comes back evicted and the run can't find its own endpoint.

`get_log` drains the buffer, so it is drained and discarded immediately before
the search to keep page-load traffic out of the candidate set.

WHY REPLAY GOES THROUGH THE PAGE
--------------------------------
The per-year replays run as `fetch()` inside the live page via
`execute_async_script`, not through Python's `requests`. Same reasoning as
Mississippi's scraper: the call executes in a real browser on the real origin,
so it inherits the session's cookies, headers and TLS fingerprint, and is
same-origin so no CORS preflight is involved.

TIER-2 BACKFILL: SC ELECTION HISTORY
------------------------------------
ethicsfiling.sc.gov exposes no party, district, incumbency, or registry data of
any kind — there is no committee/candidate registry export, and the reports
screen carries only name/office/election year/election type. Those tier-2
columns are filled from the SC Election Commission's election history service
instead (electionhistory.scvotes.gov, backed by sc.elstats.civera.com), whose
search screen offers a full CSV export:

    https://sc.elstats.civera.com/api/download_search.csv?search=<url-encoded JSON>

That host is a different origin from the ethics portal, so it cannot be fetched
with the in-page `fetch()` used for the year replays — CORS would block it.
Instead Chrome navigates to the URL directly and the file is picked up out of
the browser's download directory, which is why the driver is built with a
download dir and `Page.setDownloadBehavior`. If the server serves the CSV
inline rather than as an attachment, no file appears and the scraper falls back
to reading the rendered body text.

Election history is requested and stored ONE YEAR PER FILE —
raw/election_history_<year>.csv, each with its own header — matching how the
three portal relations are stored. It used to be concatenated into a single
election_history.csv, which made the whole export all-or-nothing: a year the
service truncated could not be re-downloaded without re-requesting every other
year, because there is no way to append to a year's rows once they sit in the
middle of a combined file. Per-year files also let the manifest carry a real
per-year skip/refresh decision, and keep any one file to a size the parser can
stream comfortably (a single year runs to hundreds of MB).

CAVEATS
-------
  - Requires Selenium and Google Chrome: `pip install selenium`. Selenium 4.6+
    resolves a matching chromedriver itself via Selenium Manager, so no separate
    driver install is needed.
  - Performance logging is Chrome/Chromium-only. This scraper will not run
    against Firefox or Safari.
  - No authentication and no documented rate limit; a 0.5s pause is used between
    year requests anyway since each one is a full-table scan on their side.
  - Filling more than one search field at once triggers a client-side validation
    error that silently blocks the grid from loading. Only the year dropdown is
    ever set.
  - The year dropdowns are Kendo widgets, not <select> elements — their options
    don't exist in the DOM until the widget is opened. Years are read off the
    live widget rather than hardcoded, so a new year appears without a code
    change. They are read as textContent, not via Selenium's element.text,
    which returns '' for options clipped by the popup's scroll container and
    would silently drop the newest years (see _OPTIONS_JS).
  - The election-history export is saved by Chrome as `elstats_search` with no
    file extension, despite the route ending in .csv, so the download poll
    cannot match on one (see _wait_for_download).
"""

import csv
import glob
import hashlib
import itertools
import json
import os
import re
import string
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote, urlparse, parse_qsl, urlencode, urlunparse, urljoin

import requests
from bs4 import BeautifulSoup

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
STATE_DIR    = PROJECT_ROOT / "data" / "South Carolina"
RAW_DIR      = STATE_DIR / "raw"
MANIFEST     = STATE_DIR / "manifest.csv"
RECIPE       = STATE_DIR / "api_recipe.json"
DOWNLOAD_DIR = STATE_DIR / ".downloads"   # Chrome's download target; transient

RAW_DIR.mkdir(parents=True, exist_ok=True)

# `partial_years` records whether the service truncated this election-history
# year mid-stream (see _STREAM_ERROR): the year itself when it did, empty when
# it didn't. Empty for every other relation. Without it a partial export is
# indistinguishable from a complete one on disk, and the run that produced the
# warning is the only place that knowledge exists.
#
# The column name is a holdover from when election history was one combined
# file and this held a space-separated list of every truncated year. It is kept
# rather than renamed so a manifest written by the older scraper still parses,
# and the meaning is unchanged — it is still "the years in this file that are
# partial", of which a per-year file has at most one.
MANIFEST_COLS = ["relation", "year", "filename", "downloaded_at", "row_count",
                 "partial_years"]

# ========================= state-specific constants ===================
SITE = "https://ethicsfiling.sc.gov"

# One entry per search screen. `year_field` is the id of the Kendo dropdown that
# scopes the search; `probe_year` is the year used for endpoint discovery — a
# mid-range year is deliberate, since the earliest and latest years in these
# dropdowns can be sparse enough to return an empty grid, which would make the
# discovery run unable to tell the data response apart from the lookup calls.
PAGES = {
    "contributions": {
        "url":        f"{SITE}/public/campaign-reports/contributions",
        "year_field": "contributionYear",
        "probe_year": 2018,
    },
    "expenditures": {
        "url":        f"{SITE}/public/campaign-reports/expenditures",
        "year_field": "expenditureYear",
        "probe_year": 2018,
    },
    "reports": {
        "url":        f"{SITE}/public/campaign-reports/reports",
        "year_field": "electionYear",
        "probe_year": 2018,
    },
}

# Portal coverage starts at 2008 (the earliest option in every year dropdown as
# of 2026). Used only as a fallback when the live dropdown can't be read.
FALLBACK_FIRST_YEAR = 2008

# ==================== Non-Candidate Committees (PACs) ====================
# A second, unrelated source. ethicsfiling.sc.gov (above) is candidates and
# public officials only -- standalone PACs, ballot measure committees, and
# political party committees file through apps.sc.gov/PublicReporting
# instead: the SC Ethics Commission's older plain ASP.NET WebForms site, not
# the Angular SPA. This section covers Non-Candidate (PAC) committees only,
# the highest-value of the six committee types that site publishes -- see
# docs/states/south_carolina.md "Non-Candidate Committees" for how each of
# the other five (Ballot Measure, Caucus, State/County/City Party) differs
# and why they aren't covered here yet.
#
# Plain requests, no Selenium -- this is a server-rendered WebForms app with
# no client-side rendering and no bot-wall encountered during reconnaissance.
NONCAND_SITE     = "https://apps.sc.gov/PublicReporting/IndividualCommittee"
NONCAND_SEARCH_URL = f"{NONCAND_SITE}/NonCandidate/SearchNonCand.aspx"
NONCAND_DIR      = RAW_DIR / "noncand"
NONCAND_FILINGS_DIR = NONCAND_DIR / "filings"
NONCAND_REGISTRY = NONCAND_DIR / "committees.json"
NONCAND_SWEEP_MANIFEST = STATE_DIR / "noncand_sweep_manifest.csv"
NONCAND_SWEEP_COLS = ["combo", "hit_count", "swept_at"]
NONCAND_DONE_MANIFEST = STATE_DIR / "noncand_filings_manifest.csv"
NONCAND_DONE_COLS = ["committee", "filings", "walked_at"]

# The search form's "at least three characters" minimum is enforced
# server-side, not just by client-side JS -- confirmed by POSTing a 1-char
# query directly and getting the same rejection a real user would see.
# "Name Contains" over every 3-letter combination is therefore the smallest
# brute-force space that still guarantees no committee is missed: any name
# with 3+ consecutive letters anywhere in it is a substring match for
# exactly one combo in this set. 26**3 = 17,576 queries.
NONCAND_COMBOS = ["".join(c) for c in itertools.product(string.ascii_lowercase, repeat=3)]

# Tabs worth pulling per filing. "Assets" is deliberately excluded -- there
# is no assets table anywhere in columns.py, so scraping it would have
# nowhere to go; if that ever changes, ViewAssets.aspx follows the same
# pattern as the other four.
#
# The summary page carries two totals per category -- "_PERIOD" (this
# filing only) and "_CYCLE" (year-to-date across every filing). Each filing's
# itemized tabs cover its own PERIOD only, so PERIOD is the one that answers
# "does fetching this tab find anything" -- CYCLE can be nonzero from an
# earlier filing even when this one reported nothing (confirmed directly: a
# filing with TOTAL_EXPENDITURE_CYCLE > $0 whose own Expenditures tab reads
# "*** No Expenditures Reported. ***").
NONCAND_TABS = {
    "contributions": ("ViewContributions.aspx", "TOTAL_CONTRIBUTION_PERIOD"),
    "expenditures":  ("ViewExpenditures.aspx",  "TOTAL_EXPENDITURE_PERIOD"),
    "loans":         ("ViewLoans.aspx",         None),   # no reliable $0 signal on the summary page -- always fetched
    "loan_payments": ("ViewRepayments.aspx",    None),
}

# SC Election Commission election history (tier-2 backfill)
ELECTION_HISTORY_URL  = "https://sc.elstats.civera.com/api/download_search.csv"

# One file per year, mirroring the {relation}_{year} naming the three portal
# relations already use. The parser globs for this prefix.
ELECTION_HISTORY_RELATION = "election_history"

# What the scraper wrote before election history was split per year. Never
# written now; recognized only so a run can tell the operator the old file is
# no longer read and is safe to delete.
LEGACY_ELECTION_HISTORY_FILE = "election_history.csv"
# Their search screen defaults to 2008 as the floor; earlier contests aren't in
# the dataset, so asking for them just returns the same rows.
ELECTION_HISTORY_FLOOR = 2008

# Static skeleton of the election-history search object. Every key is required —
# omitting any of them returns an empty body rather than an error. Only
# global.years is varied.
ELECTION_HISTORY_SEARCH = {
    "global":          {"years": {"from": ELECTION_HISTORY_FLOOR, "to": 0}},
    "ballotQuestions": {"text": "", "types": [], "number": "", "divisions": []},
    "contests":        {"candidates": [], "divisions": [], "offices": []},
    "specialElectionsOnly": False,
    "voterStats":           False,
    "stages":               [],
}

# A year's rows arrive newest-first, so a truncated year has kept the newest
# contests and lost the oldest. `stages` partitions a year by general / primary /
# runoff, which is the finest slice the search object offers below the year —
# `global.years` is already at one year, and the other list filters (offices,
# divisions, candidates) take opaque IDs this scraper has no way to enumerate.
# Slicing by stage lets each stage stream on its own connection, so the contest
# that poisons the stream costs its own stage rather than every older contest in
# the year.
#
# A stage is identified by an (id, id2) PAIR, not a name: `id` is the stage type
# (3 general, 4 primary, 6 primary runoff) and `id2` is the party (1 Democratic,
# 2 Republican, absent for general). Sending the display name instead makes the
# service return an empty export — which is how the first version of this failed.
#
# Read off the search page's own `stages` suggestion list:
#   "stages":[{"id":4,"id2":2,"name":"Republican Primary",...},
#             {"id":4,"id2":1,"name":"Democratic Primary",...},
#             {"id":6,"id2":2,"name":"Republican Primary Runoff",...},
#             {"id":6,"id2":1,"name":"Democratic Primary Runoff",...},
#             {"id":3,"id2":null,"name":"General",...}]
# To refresh it, run a search on sc.elstats.civera.com, save the page, and grep
# for `"stages":[{`. Special elections are not a stage — they are the separate
# `specialElectionsOnly` boolean, and are included in these stages either way.
#
# General is deliberately first: it is by far the largest stage, so a year whose
# poison sits in a primary gets its bulk recovered on the first slice.
_ELECTION_STAGES = (
    {"name": "General",                   "id": 3, "id2": None},
    {"name": "Democratic Primary",        "id": 4, "id2": 1},
    {"name": "Republican Primary",        "id": 4, "id2": 2},
    {"name": "Democratic Primary Runoff", "id": 6, "id2": 1},
    {"name": "Republican Primary Runoff", "id": 6, "id2": 2},
)

# The export's own (election_type, primary_party) columns, mapped to the stage
# that requests them. Used to check recovery against what the truncated year is
# already known to contain: if a stage is present in the kept rows but its slice
# comes back empty, the filter is being rejected rather than the stage being
# absent, and that is a code fault, not a fact about the data.
_EXPORT_TYPE_TO_STAGE = {
    ("general", ""):                     ("General", 3, None),
    ("primary", "democratic"):           ("Democratic Primary", 4, 1),
    ("primary", "republican"):           ("Republican Primary", 4, 2),
    ("primary runoff", "democratic"):    ("Democratic Primary Runoff", 6, 1),
    ("primary runoff", "republican"):    ("Republican Primary Runoff", 6, 2),
}

# Contest types the five stages cannot request. Counted across a real 2008-2025
# export: Special 5,082 rows, Special/Democratic 720, Primary Runoff Recount
# 2,040, Primary Recount 2,652, party-less Primary 1,440. Small next to the
# 4.7M General/Primary rows, but real — and the reason stage slicing can add
# rows to a truncated year without ever proving the year is whole again.
_UNFILTERABLE_TYPES = ("special", "primary recount", "primary runoff recount")

# CDP resource types worth inspecting during endpoint discovery.
_XHR_TYPES = {"xhr", "fetch"}

# Timeouts in seconds — Selenium's waits are seconds, unlike Playwright's ms.
# The unfiltered year searches are genuinely slow: a 2018 contributions search
# is a ~30k-row response.
NAV_TIMEOUT     = 60
SEARCH_TIMEOUT  = 180
DOWNLOAD_TIMEOUT = 180

# Chrome's default response-body buffer evicts a 30k-row JSON before
# Network.getResponseBody can read it, which makes discovery fail with an
# unhelpful "No resource with given identifier". Raise it well past the largest
# expected response.
_RESOURCE_BUFFER = 512 * 1024 * 1024    # per resource
_TOTAL_BUFFER    = 1024 * 1024 * 1024   # across the page

# Kendo has renamed its grid internals across major versions, and this portal
# has already been through at least one. Any of these means "the grid rendered
# rows"; the row-count label in the toolbar is checked too because a virtualized
# grid can report its total before the first <tr> is attached.
_GRID_ROW_SELECTORS = (
    "kendo-grid-list table tbody tr",
    ".k-grid-content table tbody tr",
    "kendo-grid tbody tr",
    "kendo-grid-toolbar strong",
)

# Kendo's empty-result placeholder — a real answer ("this year has no rows"),
# not a failure, and worth distinguishing from a search that never ran.
_NO_RECORDS_SELECTOR = ".k-grid-norecords, kendo-grid-list .k-grid-norecords"

# The app blocks the search client-side and renders this into <app-server-error>
# when it thinks no criteria were supplied. Matched on a distinctive fragment so
# minor copy edits upstream don't silently stop it matching.
_VALIDATION_MARKER = "please search for something"

# Headed by default. The portal is an Angular SPA behind a WAF, and every other
# browser-driven scraper in this repo (Alaska, Mississippi) had to run visibly
# for the same class of reason. --headless opts back in once it's confirmed
# working on a given machine.
HEADLESS = False

# Replay runs as a real in-page fetch (see module docstring). execute_async_script
# appends the completion callback as the final argument.
_FETCH_JS = """
const [url, method, body, headers, done] = arguments;
(async () => {
    try {
        const opts = {method: method, headers: headers, credentials: "include"};
        // Truthiness, not `!== null`: fetch throws TypeError if a GET is given
        // a body, and an empty-string post_data would slip past a null check.
        if (body) { opts.body = body; }
        const resp = await fetch(url, opts);
        const text = await resp.text();
        done({ok: resp.ok, status: resp.status, text: text});
    } catch (e) {
        done({ok: false, status: 0, text: "", error: String(e)});
    }
})();
"""


# ========================== manifest helpers ==========================

def load_manifest() -> list[dict]:
    if not MANIFEST.exists():
        return []
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_manifest(rows: list[dict]):
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def upsert_manifest(record: dict):
    """Replace the row for this (relation, year) pair, or append it."""
    key  = (record["relation"], str(record["year"]))
    rows = [r for r in load_manifest()
            if (r.get("relation"), str(r.get("year"))) != key]
    rows.append(record)
    rows.sort(key=lambda r: (r.get("relation", ""), str(r.get("year", ""))))
    write_manifest(rows)


def strip_manifest(keep) -> None:
    """Drop every manifest row for which keep(row) is False."""
    if not MANIFEST.exists():
        return
    write_manifest([r for r in load_manifest() if keep(r)])


# ==================== Non-Candidate Committees (PACs) ====================

def _noncand_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
    })
    return s


def _aspnet_tokens(html: str) -> dict:
    """Pull the three hidden WebForms postback fields off a rendered page."""
    soup = BeautifulSoup(html, "html.parser")
    out = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        tag = soup.find("input", {"name": name})
        out[name] = tag["value"] if tag and tag.has_attr("value") else ""
    return out


def _form_action(html: str, base_url: str) -> str:
    """Resolve the page's <form action> to an absolute URL. Falls back to
    base_url for the tab views, which are plain GETs with no form of their
    own -- session-cookie state, not viewstate, carries which report they
    render."""
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form")
    action = form.get("action") if form else None
    return urljoin(base_url, action) if action else base_url


def _postback(session: requests.Session, url: str, tokens: dict, fields: dict):
    """POST a WebForms page with its hidden state plus whichever fields
    simulate the control being clicked (a text box, a radio button, or one
    of the row buttons that stand in for <a> links on this site)."""
    r = session.post(url, data={**tokens, **fields}, timeout=30)
    r.raise_for_status()
    return r


def _noncand_result_names(html: str) -> list[str]:
    """Committee names off a SearchNonCand.aspx results page. Names in this
    system carry embedded newlines/whitespace as stored (observed directly
    on a real committee), so every read normalizes it away."""
    soup = BeautifulSoup(html, "html.parser")
    names = []
    for tag in soup.find_all("input", {"class": "link"}):
        name = re.sub(r"\s+", " ", tag.get("value", "")).strip()
        if name:
            names.append(name)
    return names


def _noncand_slug(name: str) -> str:
    """Filesystem-safe, collision-resistant filename for a committee name."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:60]
    h = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    return f"{slug}_{h}"


# -------------------------- registry (discovery) ---------------------------

def _load_noncand_registry() -> dict:
    if NONCAND_REGISTRY.exists():
        return json.loads(NONCAND_REGISTRY.read_text())
    return {}


def _save_noncand_registry(registry: dict):
    NONCAND_DIR.mkdir(parents=True, exist_ok=True)
    NONCAND_REGISTRY.write_text(json.dumps(registry, indent=1, sort_keys=True))


def _load_swept_combos() -> set:
    if not NONCAND_SWEEP_MANIFEST.exists():
        return set()
    with open(NONCAND_SWEEP_MANIFEST, newline="", encoding="utf-8") as f:
        return {r["combo"] for r in csv.DictReader(f)}


def _append_swept_combo(combo: str, hit_count: int):
    write_header = not NONCAND_SWEEP_MANIFEST.exists()
    with open(NONCAND_SWEEP_MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=NONCAND_SWEEP_COLS)
        if write_header:
            w.writeheader()
        w.writerow({"combo": combo, "hit_count": hit_count,
                    "swept_at": datetime.today().strftime("%Y-%m-%d")})


def sweep_noncand_registry(log, force: bool = False, limit: int | None = None) -> dict:
    """Brute-force every 3-letter 'Name Contains' query against the
    Non-Candidate committee search to discover every standalone PAC SC has
    ever had a public record of. See docs/states/south_carolina.md.

    Resumable: each combo swept is appended to noncand_sweep_manifest.csv, so
    an interrupted run continues from where it left off on the next call.
    --force clears both the manifest and the registry and starts over.
    `limit` caps how many NEW combos this call sweeps -- used for testing on
    a slice without committing to the full 17,576.
    """
    session = _noncand_session()
    tokens  = _aspnet_tokens(session.get(NONCAND_SEARCH_URL, timeout=30).text)

    if force:
        NONCAND_SWEEP_MANIFEST.unlink(missing_ok=True)
    swept    = set() if force else _load_swept_combos()
    registry = {}    if force else _load_noncand_registry()

    todo = [c for c in NONCAND_COMBOS if c not in swept]
    if limit is not None:
        todo = todo[:limit]
    if not todo:
        log.info(f"  [noncand] sweep already complete "
                 f"({len(swept):,}/{len(NONCAND_COMBOS):,} combos)")
        return registry

    log.info(f"  [noncand] sweeping {len(todo):,} of {len(NONCAND_COMBOS):,} "
             f"3-letter combos ({len(swept):,} already done)")

    new_names = 0
    for i, combo in enumerate(todo, 1):
        try:
            r = _postback(session, NONCAND_SEARCH_URL, tokens, {
                "ctl00$ContentPlaceHolder1$txtName": combo,
                "ctl00$ContentPlaceHolder1$rdList":  "2",   # Name Contains
                "ctl00$ContentPlaceHolder1$btnNext": "Next",
            })
        except requests.RequestException as e:
            log.warning(f"  [noncand] {combo}: {e} -- refreshing session and "
                        f"continuing (this combo will retry on the next run)")
            session = _noncand_session()
            tokens  = _aspnet_tokens(session.get(NONCAND_SEARCH_URL, timeout=30).text)
            continue

        names = _noncand_result_names(r.text)
        for name in names:
            if name not in registry:
                registry[name] = {"first_seen_combo": combo}
                new_names += 1
        _append_swept_combo(combo, len(names))
        swept.add(combo)

        # Refresh tokens periodically even without an error -- a long-running
        # sweep (the full space is 17,576 queries) could outlast the site's
        # own session lifetime in ways that don't necessarily raise.
        if i % 500 == 0:
            tokens = _aspnet_tokens(session.get(NONCAND_SEARCH_URL, timeout=30).text)
        if i % 250 == 0 or i == len(todo):
            _save_noncand_registry(registry)
            log.info(f"  [noncand] {i:,}/{len(todo):,} combos swept -- "
                     f"{len(registry):,} distinct committees found so far "
                     f"(+{new_names:,} this run)")
        time.sleep(0.15)

    _save_noncand_registry(registry)
    log.registry_loaded("noncand_committees", len(registry), relation="committees")
    return registry


# ------------------------ per-committee filing walk -------------------------

def _find_noncand_committee(session, name: str):
    """Land on `name`'s exact result row via 'Begins With'. Returns
    (results_html, form_action_url, button_name, button_value) or
    (None, None, None, None) if the exact name can't be relocated -- e.g. it
    was truncated differently between registry-save and this run."""
    tokens = _aspnet_tokens(session.get(NONCAND_SEARCH_URL, timeout=30).text)
    r = _postback(session, NONCAND_SEARCH_URL, tokens, {
        "ctl00$ContentPlaceHolder1$txtName": name[:80],
        "ctl00$ContentPlaceHolder1$rdList":  "1",   # Name Begins With
        "ctl00$ContentPlaceHolder1$btnNext": "Next",
    })
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup.find_all("input", {"class": "link"}):
        val = re.sub(r"\s+", " ", tag.get("value", "")).strip()
        if val == name:
            return r.text, _form_action(r.text, NONCAND_SEARCH_URL), tag["name"], tag.get("value", "")
    return None, None, None, None


def _noncand_report_rows(html: str) -> list[dict]:
    """[{btn_name, btn_value, report_type, period, date_filed, version}, ...]
    off a committee's report index page (NonCandFilerResult.aspx and its
    dropdown-driven siblings -- LookupCaucusResult.aspx,
    LookupStatePartyResult.aspx, LookupCountyPartyResult.aspx,
    LookupCityPartyResult.aspx all share this exact 4-column table).

    report_type is the button's own display text, not a separate column --
    for Non-Candidate committees it's uniform (effectively unused), but
    Caucus and Party committees file two DIFFERENT report types under this
    one table: "Campaign Disclosure" (same contributions/expenditures shape
    as everything else) and "Operating Disclosure" (a legislative-caucus-
    specific report format, different fields, not yet parsed anywhere in
    this pipeline -- see docs/states/south_carolina.md). Callers that only
    handle Campaign Disclosure must filter on this field themselves; this
    function returns every row unfiltered so no data is silently dropped
    here."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []
    out = []
    for tr in table.find_all("tr")[1:]:      # skip the header row
        tds = tr.find_all("td")
        btn = tr.find("input", {"class": "link"})
        if not btn or len(tds) < 4:
            continue
        out.append({
            "btn_name":    btn["name"],
            "btn_value":   btn.get("value", ""),
            "report_type": btn.get("value", ""),
            "period":      tds[1].get_text(strip=True),
            "date_filed":  tds[2].get_text(strip=True),
            "version":     tds[3].get_text(strip=True),
        })
    return out


def _noncand_demographics(html: str) -> dict:
    """Committee address block off a summary page (ViewReport.aspx) --
    address/city/state/zip/phone, whichever of them the span carries."""
    soup = BeautifulSoup(html, "html.parser")
    out = {}
    for field, span_id in (("address", "lblAddress"), ("city", "lblCity"),
                           ("state", "lblState"), ("zip", "lblZip"),
                           ("phone", "lblPhone")):
        tag = soup.find(id=re.compile(re.escape(span_id) + "$"))
        out[field] = tag.get_text(strip=True) if tag else ""
    return out


def _noncand_filer_name(html: str) -> str:
    """The filer name off a Campaign Disclosure summary page (lblName) --
    needed for the dropdown-driven Caucus/Party walkers, which don't know
    the committee's exact filed name upfront the way a Non-Candidate name
    search does. Confirmed this is the authoritative name, not just the
    dropdown label re-displayed: County of Richland's Democratic Party
    dropdown selection is county="Richland" + party="Democratic", but
    lblName on the resulting page reads 'County of Richland Democratic
    Party' -- a real difference in word order/phrasing that only the site
    itself can give you."""
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find(id=re.compile(re.escape("lblName") + "$"))
    return tag.get_text(strip=True) if tag else ""


def _noncand_summary_nonzero(html: str, span_id: str) -> bool:
    """True if the summary page's total for `span_id` is missing or nonzero.
    Missing is treated as nonzero (fetch it) rather than zero (skip it) --
    an unrecognized page shape should never cause silent data loss."""
    if span_id is None:
        return True
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find(id=re.compile(re.escape(span_id) + "$"))
    if tag is None:
        return True
    val = tag.get_text(strip=True).replace("$", "").replace(",", "")
    try:
        return float(val) != 0.0
    except ValueError:
        return True


def _noncand_itemized_rows(html: str) -> list[dict]:
    """Every itemized row off a ViewContributions/Expenditures/Loans/
    Repayments.aspx tab, keyed by its own column headers -- the four tabs
    don't share a column layout, so the parser reads whichever headers are
    actually present rather than assuming Contributions' shape."""
    soup  = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="tableTabs")
    if not table:
        return []
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    out = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds or len(tds) != len(headers):
            continue   # header row itself, or the trailing Total row
        row = {}
        for h, td in zip(headers, tds):
            # Address cells use <br> to separate street/city -- keep both
            # lines, newline-joined, rather than collapsing them together.
            row[h] = td.get_text("\n", strip=True)
        out.append(row)
    return out


def _load_noncand_done() -> dict:
    if not NONCAND_DONE_MANIFEST.exists():
        return {}
    with open(NONCAND_DONE_MANIFEST, newline="", encoding="utf-8") as f:
        return {r["committee"]: r for r in csv.DictReader(f)}


def _append_noncand_done(committee: str, n_filings: int):
    write_header = not NONCAND_DONE_MANIFEST.exists()
    with open(NONCAND_DONE_MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=NONCAND_DONE_COLS)
        if write_header:
            w.writeheader()
        w.writerow({"committee": committee, "filings": n_filings,
                    "walked_at": datetime.today().strftime("%Y-%m-%d")})


def _walk_report_index(log, session, source_label: str, committee_name: str | None,
                       index_html: str, index_action: str,
                       report_types: set[str] | None = None) -> dict:
    """Shared inner loop for walking a committee's report index once you're
    already positioned on it -- every filing's summary, and the itemized
    tabs whose summary total isn't a known zero. Used by both
    walk_noncand_committee() (which gets here via a name search) and the
    dropdown-driven Caucus/Party walkers (which get here via a direct
    dropdown POST -- see _walk_dropdown_committee() below).

    report_types, if given, restricts which rows get walked by their
    report_type (see _noncand_report_rows -- "Campaign Disclosure" vs
    "Operating Disclosure" for Caucus/Party committees). None walks every
    row, which is correct for Non-Candidate committees (uniform report type)
    and is the default so this never silently drops rows for a caller that
    doesn't pass the argument.

    source_label is just for warning messages ("noncand", "caucus", etc.)."""
    report_rows = _noncand_report_rows(index_html)
    if report_types is not None:
        skipped = [r for r in report_rows if r["report_type"] not in report_types]
        report_rows = [r for r in report_rows if r["report_type"] in report_types]
        if skipped:
            log.info(f"  [{source_label}] {committee_name or '(unnamed)'}: skipping "
                     f"{len(skipped)} filing(s) of report type(s) "
                     f"{sorted(set(r['report_type'] for r in skipped))} -- not yet parsed")

    filings = []
    demographics = {}
    for row in report_rows:
        tokens_idx = _aspnet_tokens(index_html)
        r_summary = _postback(session, index_action, tokens_idx,
                              {row["btn_name"]: row["btn_value"]})
        # r_summary.url (requests' own post-redirect URL), not
        # _form_action(r_summary.text, ...) -- confirmed live (2026-08-03)
        # that Caucus/Party summaries land under an extra NONCAND/ path
        # segment via a server-side redirect that the landed page's own
        # <form action> doesn't reflect (it renders a plain relative
        # "ViewReport.aspx" that resolves to the WRONG folder against the
        # pre-redirect POST target). r_summary.url is ground truth for
        # wherever the server actually put the content, so tab URLs built
        # from it are correct regardless of whether a redirect happened.
        # Non-Candidate committees never cross a folder boundary this way,
        # so this is a strict improvement there too, not just a fix for the
        # new dropdown-driven sources.
        summary_url = r_summary.url

        # Dropdown-driven callers (Caucus/Party) don't know the committee's
        # exact filed name upfront -- pull it off whichever filing's summary
        # page loads first (same page carries lblName and the address block).
        if committee_name is None:
            fname = _noncand_filer_name(r_summary.text)
            if fname:
                committee_name = fname

        # Keep the first filing's address block that actually has one --
        # reports don't arrive in a guaranteed chronological order, and the
        # committee's registered address rarely changes filing to filing.
        if not demographics.get("address"):
            demo = _noncand_demographics(r_summary.text)
            if demo.get("address"):
                demographics = demo

        tabs = {}
        for label, (path, span_id) in NONCAND_TABS.items():
            if not _noncand_summary_nonzero(r_summary.text, span_id):
                continue
            tab_url = urljoin(summary_url, path)
            try:
                r_tab = session.get(tab_url, timeout=30)
                r_tab.raise_for_status()
            except requests.RequestException as e:
                log.warning(f"  [{source_label}] {committee_name} / "
                            f"{row['date_filed']} / {label}: {e}")
                continue
            rows = _noncand_itemized_rows(r_tab.text)
            if rows:
                tabs[label] = rows

        filings.append({
            "period":     row["period"],
            "date_filed": row["date_filed"],
            "version":    row["version"],
            **tabs,
        })
        time.sleep(0.15)

    return {"committee": committee_name, "demographics": demographics, "filings": filings}


def walk_noncand_committee(log, session, name: str) -> dict | None:
    """Pull every filing for one Non-Candidate committee: report index, each
    filing's summary, and the itemized tabs whose summary total isn't a
    known zero. Returns the full record (written by the caller) or None if
    the committee couldn't be relocated by exact name."""
    results_html, action, btn_name, btn_val = _find_noncand_committee(session, name)
    if action is None:
        log.warning(f"  [noncand] could not relocate '{name}' by exact name -- skipping")
        return None

    tokens = _aspnet_tokens(results_html)
    r_index = _postback(session, action, tokens, {btn_name: btn_val})
    index_action = _form_action(r_index.text, action)

    return _walk_report_index(log, session, "noncand", name,
                              r_index.text, index_action)


def run_noncand_pacs(log, force: bool = False, sweep_limit: int | None = None,
                      walk_limit: int | None = None):
    """Discover every SC Non-Candidate committee (PAC) and pull its filing
    history. Two resumable phases: sweep_noncand_registry() builds the name
    registry, then every registered name not already in
    noncand_filings_manifest.csv gets walked. `sweep_limit`/`walk_limit` cap
    how much NEW work each phase does this call -- used for testing.
    """
    registry = sweep_noncand_registry(log, force=force, limit=sweep_limit)

    if force:
        NONCAND_DONE_MANIFEST.unlink(missing_ok=True)
    done = set() if force else set(_load_noncand_done())

    todo = [n for n in registry if n not in done]
    if walk_limit is not None:
        todo = todo[:walk_limit]

    if not todo:
        log.info(f"  [noncand] filing walk already complete "
                 f"({len(done):,}/{len(registry):,} committees)")
        return

    log.info(f"  [noncand] walking {len(todo):,} of {len(registry):,} "
             f"committees ({len(done):,} already done)")

    NONCAND_FILINGS_DIR.mkdir(parents=True, exist_ok=True)
    session = _noncand_session()
    walked = 0
    for i, name in enumerate(todo, 1):
        record = walk_noncand_committee(log, session, name)
        if record is None:
            continue
        out_path = NONCAND_FILINGS_DIR / f"{_noncand_slug(name)}.json"
        out_path.write_text(json.dumps(record, indent=1))
        _append_noncand_done(name, len(record["filings"]))
        walked += 1
        if i % 25 == 0 or i == len(todo):
            log.info(f"  [noncand] {i:,}/{len(todo):,} committees walked "
                     f"({walked:,} succeeded)")

    log.info(f"  [noncand] filing walk done -- {walked:,} committees written")


# ==================== Caucus & Party Committees ========================
# Four more of apps.sc.gov's six committee-type lookups (see "Non-Candidate
# Committees" above for the other two -- Ballot Measure, not yet built, and
# Non-Candidate, built above). These four are plain dropdowns rather than a
# name search: Caucus (13 options) and State Party (11) resolve straight to
# one committee each; County Party and City Party (46 counties x 11 parties,
# 269 cities x 11 parties) need a two-dropdown combo, most of which don't
# correspond to a real filed committee -- so those two get the same
# sweep-then-walk resumable treatment as Non-Candidate's brute-force search,
# just over a real, enumerable space instead of 17,576 blind letter combos.
#
# Confirmed directly against the live site (2026-08-03): every one of these
# four reuses the exact same report-index table shape, tab URLs, and
# itemized-row parsing as Non-Candidate (_noncand_report_rows,
# _noncand_itemized_rows, _walk_report_index). Selecting a Caucus and
# clicking Next lands on LookupCaucusResult.aspx with the identical
# 4-column Report Type/Filing Period/Date Filed/Version table
# NonCandFilerResult.aspx has, and its Campaign Disclosure tabs live under
# .../NONCAND/View*.aspx -- literally the same "NONCAND" URL segment
# Non-Candidate committees use, strongly suggesting this whole reporting
# subsystem is shared code on the SC side.
#
# ONE GENUINE DIFFERENCE: Caucus and Party committees each file two
# different report types under that one table -- "Campaign Disclosure"
# (identical shape to everything else here) and "Operating Disclosure" (a
# legislative-caucus-specific administrative-expense report with different
# fields, landing on a differently-shaped ReviewSummary.aspx rather than
# ViewReport.aspx -- confirmed by inspecting both directly). This build
# covers Campaign Disclosure only, matching Non-Candidate's scope --
# Operating Disclosure rows are detected and logged as skipped
# (REPORT_TYPES_BUILT, consumed by _walk_report_index's report_types
# filter), never silently dropped. Operating Disclosure support is a
# separate, not-yet-started effort.

DROPDOWN_BTN_NEXT = "ctl00$ContentPlaceHolder1$btnNext"
REPORT_TYPES_BUILT = {"Campaign Disclosure"}

PARTY_CAUCUS_DIR         = RAW_DIR / "party_caucus"
PARTY_CAUCUS_FILINGS_DIR = PARTY_CAUCUS_DIR / "filings"

# source -> (url, primary dropdown field, secondary dropdown field or None).
# Caucus and State Party have no secondary field -- the primary dropdown's
# options ARE the complete registry, no cross-product needed.
PARTY_SOURCES = {
    "caucus": (
        f"{NONCAND_SITE}/LegCaucus/LookupCaucus.aspx",
        "ctl00$ContentPlaceHolder1$drpCaucus", None,
    ),
    "state_party": (
        f"{NONCAND_SITE}/StatePolParty/LookupStateParty.aspx",
        "ctl00$ContentPlaceHolder1$drpPoliticalParty", None,
    ),
    "county_party": (
        f"{NONCAND_SITE}/CountyPolParty/LookupCountyParty.aspx",
        "ctl00$ContentPlaceHolder1$drpCounty",
        "ctl00$ContentPlaceHolder1$drpPoliticalParty",
    ),
    "city_party": (
        f"{NONCAND_SITE}/CityPolParty/LookupCityParty.aspx",
        "ctl00$ContentPlaceHolder1$drpCity",
        "ctl00$ContentPlaceHolder1$drpPoliticalParty",
    ),
}


def _party_manifest(source: str) -> Path:
    """One manifest per source (not one shared file) -- keeps --force able
    to reset a single source without touching the other three, and keeps
    the resumability logic identical to Non-Candidate's per-source files."""
    return STATE_DIR / f"party_{source}_manifest.csv"


_PARTY_MANIFEST_COLS = ["key", "primary_label", "secondary_label", "hit",
                        "committee", "filings", "processed_at"]

# SC's quarterly filing deadlines fall Jan/Apr/Jul/Oct 10 -- roughly every 91
# days. A combo that was last checked more than this many days ago gets
# rechecked on the next --party-caucus run rather than being treated as
# "done forever", so a committee that files a new quarterly report (or a
# combo that previously had no committee but gets a new one registered) is
# eventually picked back up without needing --force. 60 days gives real
# margin against the ~91-day cycle -- a run that's a few weeks late still
# catches each new filing well before the following deadline.
PARTY_REFRESH_DAYS = 60


def _load_party_processed_at(source: str) -> dict[str, str]:
    """key -> most recent processed_at (YYYY-MM-DD) seen for that key. The
    manifest is append-only (one row per run, never rewritten -- see
    _append_party_done), so a key can have multiple rows across runs; since
    rows are always appended in chronological order, the last row read for
    a given key is its most recent processed_at."""
    path = _party_manifest(source)
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            out[r["key"]] = r["processed_at"]
    return out


def _append_party_done(source: str, key: str, primary_label: str,
                       secondary_label: str, committee: str, n_filings: int):
    path = _party_manifest(source)
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_PARTY_MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow({
            "key": key, "primary_label": primary_label,
            "secondary_label": secondary_label or "",
            "hit": int(bool(committee)), "committee": committee or "",
            "filings": n_filings,
            "processed_at": datetime.today().strftime("%Y-%m-%d"),
        })


def _dropdown_options(html: str, select_name: str) -> list[tuple[str, str]]:
    """[(value, label), ...] for a <select>'s real options -- skips the
    blank placeholder ('0', '') every one of these four dropdowns starts
    with."""
    soup = BeautifulSoup(html, "html.parser")
    sel = soup.find("select", {"name": select_name})
    if not sel:
        return []
    out = []
    for o in sel.find_all("option"):
        val, label = o.get("value", ""), o.get_text(strip=True)
        if val and val != "0" and label:
            out.append((val, label))
    return out


def _walk_dropdown_committee(log, session, source: str, url: str, fields: dict,
                             fallback_name: str) -> dict | None:
    """POST a dropdown selection (Caucus, State/County/City Party) and walk
    whatever report index it lands on. Unlike Non-Candidate there's no name
    search to resolve first -- the dropdown selection itself IS the
    committee -- so this goes straight to _walk_report_index. Returns None
    if the combo has no filed committee at all (no results table -- the
    normal case for most County/City x Party combos, since most localities
    don't have an organized committee for every party)."""
    r = session.get(url, timeout=30)
    tokens = _aspnet_tokens(r.text)
    r_index = _postback(session, url, tokens, {**fields, DROPDOWN_BTN_NEXT: "Next"})
    if BeautifulSoup(r_index.text, "html.parser").find("table") is None:
        return None

    index_action = _form_action(r_index.text, url)
    record = _walk_report_index(log, session, source, None, r_index.text,
                                index_action, report_types=REPORT_TYPES_BUILT)
    if record["committee"] is None:
        # lblName was never seen -- either every filing on this combo was
        # Operating Disclosure (filtered out above) or it genuinely has zero
        # Campaign Disclosure filings. Nothing to write either way, but fall
        # back to a constructed label so a caller with filings-but-no-name
        # (shouldn't happen, but see _walk_report_index) doesn't lose data.
        if not record["filings"]:
            return None
        record["committee"] = fallback_name
    return record


def run_party_source(log, source: str, force: bool = False,
                     sweep_limit: int | None = None,
                     refresh_days: int = PARTY_REFRESH_DAYS):
    """Discover and pull filing history for every committee under one of
    the four dropdown sources (see PARTY_SOURCES). Single-dropdown sources
    (Caucus, State Party) just walk every option -- no cross-product, no
    "does this exist" check needed. Two-dropdown sources (County/City
    Party) walk the full cross-product, most of which will be a miss; each
    combo checked (hit or miss) is recorded in the manifest, so an
    interrupted run resumes without re-checking known-empty combos.

    A combo is re-checked (not just skipped forever) once its last
    processed_at is more than `refresh_days` old -- see PARTY_REFRESH_DAYS.
    This is what lets --party-caucus be run periodically (e.g. from cron)
    and actually catch new quarterly filings from known committees or newly
    registered committees under a combo that was previously empty, rather
    than being a one-shot sweep. `--force` still wipes the manifest outright
    for a full manual reset regardless of age.

    `sweep_limit` caps how many combos this call processes (new + stale
    combined) -- used for testing on a slice.
    """
    url, primary_field, secondary_field = PARTY_SOURCES[source]
    session = _noncand_session()
    r = session.get(url, timeout=30)
    primary = _dropdown_options(r.text, primary_field)
    secondary = _dropdown_options(r.text, secondary_field) if secondary_field else [("", "")]
    combos = [(pv, pl, sv, sl) for pv, pl in primary for sv, sl in secondary]

    def combo_key(pv, sv):
        return f"{pv}:{sv}" if secondary_field else pv

    if force:
        _party_manifest(source).unlink(missing_ok=True)
    processed_at = _load_party_processed_at(source)
    cutoff = datetime.today() - timedelta(days=refresh_days)

    def _is_stale(key: str) -> bool:
        ts = processed_at.get(key)
        if ts is None:
            return True  # never seen -- always due
        try:
            return datetime.strptime(ts, "%Y-%m-%d") < cutoff
        except ValueError:
            return True  # malformed row -- safest is to recheck

    todo = [c for c in combos if _is_stale(combo_key(c[0], c[2]))]
    if sweep_limit is not None:
        todo = todo[:sweep_limit]

    if not todo:
        log.info(f"  [{source}] already complete and fresh "
                 f"({len(combos):,}/{len(combos):,} combos, "
                 f"<{refresh_days}d old)")
        return

    never_seen = sum(1 for c in combos if combo_key(c[0], c[2]) not in processed_at)
    stale      = len(todo) - min(never_seen, len(todo))
    log.info(f"  [{source}] checking {len(todo):,} of {len(combos):,} combos "
             f"({never_seen:,} new, {stale:,} stale >{refresh_days}d, "
             f"{len(combos) - len(todo):,} still fresh)")

    PARTY_CAUCUS_FILINGS_DIR.mkdir(parents=True, exist_ok=True)
    found = 0
    for i, (pv, pl, sv, sl) in enumerate(todo, 1):
        key = combo_key(pv, sv)
        fields = {primary_field: pv}
        if secondary_field:
            fields[secondary_field] = sv
        fallback_name = f"{sl} {pl}" if sl else pl

        try:
            record = _walk_dropdown_committee(log, session, source, url, fields, fallback_name)
        except requests.RequestException as e:
            log.warning(f"  [{source}] {fallback_name}: {e} -- refreshing session "
                        f"and continuing (this combo will retry on the next run)")
            session = _noncand_session()
            continue

        if record is not None and record.get("filings"):
            record["source"] = source
            out_path = PARTY_CAUCUS_FILINGS_DIR / f"{source}_{_noncand_slug(record['committee'])}.json"
            out_path.write_text(json.dumps(record, indent=1))
            _append_party_done(source, key, pl, sl, record["committee"], len(record["filings"]))
            found += 1
        else:
            _append_party_done(source, key, pl, sl, "", 0)

        if i % 50 == 0 or i == len(todo):
            log.info(f"  [{source}] {i:,}/{len(todo):,} combos checked -- "
                     f"{found:,} committee(s) found this run")
        time.sleep(0.15)

    log.info(f"  [{source}] done -- {found:,} committee(s) written this run")


def run_party_caucus(log, force: bool = False, sweep_limit: int | None = None,
                     refresh_days: int = PARTY_REFRESH_DAYS):
    """Discover and pull filing history for all four dropdown-driven
    committee types: Caucus, State/County/City Political Party. See
    docs/states/south_carolina.md 'Caucus & Party Committees'. Campaign
    Disclosure filings only -- see REPORT_TYPES_BUILT.

    Combos older than `refresh_days` are rechecked automatically -- see
    PARTY_REFRESH_DAYS and run_party_source's docstring. --force still
    wipes the manifest outright for a full manual reset."""
    for source in PARTY_SOURCES:
        run_party_source(log, source, force=force, sweep_limit=sweep_limit,
                         refresh_days=refresh_days)


# ==================== Ballot Measure Committees ========================
#
# The sixth and last of apps.sc.gov's six committee-type lookups (see
# "Non-Candidate Committees" and "Caucus & Party Committees" above). Ballot
# Measure is name-search only, like Non-Candidate -- not a dropdown, like
# Caucus/Party -- and confirmed live (2026-08-03) to be structurally
# identical to Non-Candidate in every other respect: same search form shape
# (txtName/rdList/btnNext against Ballot/SearchBallot.aspx), same "at least
# three characters" server-side minimum on "Name Contains" (POSTing a
# 1-char query returns the same rejection text Non-Candidate does), same
# results-list -> report-index -> summary -> itemized-tabs postback chain
# (BallotFilers.aspx -> BallotFilerResult.aspx -> ViewReport.aspx), same
# span ids (lblName/lblAddress/lblPhone, TOTAL_CONTRIBUTION_PERIOD/
# TOTAL_EXPENDITURE_PERIOD), same two-line <br>-separated address format,
# same itemized-tab table shape. Every low-level helper built for
# Non-Candidate (_noncand_session, _postback, _noncand_result_names,
# _noncand_slug, _noncand_demographics, _noncand_report_rows,
# _noncand_itemized_rows, _noncand_summary_nonzero, _walk_report_index) is
# reused unchanged -- only the search URL, manifest files, and output
# directory below are Ballot-specific. Unlike Caucus/Party, no cross-folder
# redirect was observed on the summary page (it stayed under .../Ballot/
# throughout in testing), but _walk_report_index already uses
# response.url unconditionally, so this is safe either way.
#
# ONE DIFFERENCE FROM NON-CANDIDATE, SAME AS CAUCUS/PARTY: Ballot Measure
# committees also file "Statement of Organization" filings alongside
# "Campaign Disclosure" under the same report-index table -- confirmed live
# by sampling several real filers found via a "com" search (NRA Ballot
# Measure Committee among them). This build covers Campaign Disclosure
# only, via the same REPORT_TYPES_BUILT whitelist Caucus/Party uses --
# Statement of Organization rows are detected and logged as skipped, never
# silently dropped. No "Operating Disclosure" was observed here (that
# report type appears to be legislative-caucus-specific, not general to
# every apps.sc.gov committee type).

BALLOT_SEARCH_URL     = f"{NONCAND_SITE}/Ballot/SearchBallot.aspx"
BALLOT_DIR            = RAW_DIR / "ballot_measure"
BALLOT_FILINGS_DIR    = BALLOT_DIR / "filings"
BALLOT_REGISTRY       = BALLOT_DIR / "committees.json"
BALLOT_SWEEP_MANIFEST = STATE_DIR / "ballot_sweep_manifest.csv"
BALLOT_DONE_MANIFEST  = STATE_DIR / "ballot_filings_manifest.csv"


# -------------------------- registry (discovery) ---------------------------

def _load_ballot_registry() -> dict:
    if BALLOT_REGISTRY.exists():
        return json.loads(BALLOT_REGISTRY.read_text())
    return {}


def _save_ballot_registry(registry: dict):
    BALLOT_DIR.mkdir(parents=True, exist_ok=True)
    BALLOT_REGISTRY.write_text(json.dumps(registry, indent=1, sort_keys=True))


def _load_swept_ballot_combos() -> set:
    if not BALLOT_SWEEP_MANIFEST.exists():
        return set()
    with open(BALLOT_SWEEP_MANIFEST, newline="", encoding="utf-8") as f:
        return {r["combo"] for r in csv.DictReader(f)}


def _append_swept_ballot_combo(combo: str, hit_count: int):
    write_header = not BALLOT_SWEEP_MANIFEST.exists()
    with open(BALLOT_SWEEP_MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=NONCAND_SWEEP_COLS)   # same shape, reused
        if write_header:
            w.writeheader()
        w.writerow({"combo": combo, "hit_count": hit_count,
                    "swept_at": datetime.today().strftime("%Y-%m-%d")})


def sweep_ballot_registry(log, force: bool = False, limit: int | None = None) -> dict:
    """Brute-force every 3-letter 'Name Contains' query against the Ballot
    Measure committee search -- identical approach to
    sweep_noncand_registry(), same 26**3 = 17,576-combo space, same
    resumability (ballot_sweep_manifest.csv). --force clears both the
    manifest and the registry and starts over. `limit` caps how many NEW
    combos this call sweeps -- used for testing on a slice.
    """
    session = _noncand_session()
    tokens  = _aspnet_tokens(session.get(BALLOT_SEARCH_URL, timeout=30).text)

    if force:
        BALLOT_SWEEP_MANIFEST.unlink(missing_ok=True)
    swept    = set() if force else _load_swept_ballot_combos()
    registry = {}    if force else _load_ballot_registry()

    todo = [c for c in NONCAND_COMBOS if c not in swept]
    if limit is not None:
        todo = todo[:limit]
    if not todo:
        log.info(f"  [ballot] sweep already complete "
                 f"({len(swept):,}/{len(NONCAND_COMBOS):,} combos)")
        return registry

    log.info(f"  [ballot] sweeping {len(todo):,} of {len(NONCAND_COMBOS):,} "
             f"3-letter combos ({len(swept):,} already done)")

    new_names = 0
    for i, combo in enumerate(todo, 1):
        try:
            r = _postback(session, BALLOT_SEARCH_URL, tokens, {
                "ctl00$ContentPlaceHolder1$txtName": combo,
                "ctl00$ContentPlaceHolder1$rdList":  "2",   # Name Contains
                "ctl00$ContentPlaceHolder1$btnNext": "Next",
            })
        except requests.RequestException as e:
            log.warning(f"  [ballot] {combo}: {e} -- refreshing session and "
                        f"continuing (this combo will retry on the next run)")
            session = _noncand_session()
            tokens  = _aspnet_tokens(session.get(BALLOT_SEARCH_URL, timeout=30).text)
            continue

        names = _noncand_result_names(r.text)
        for name in names:
            if name not in registry:
                registry[name] = {"first_seen_combo": combo}
                new_names += 1
        _append_swept_ballot_combo(combo, len(names))
        swept.add(combo)

        if i % 500 == 0:
            tokens = _aspnet_tokens(session.get(BALLOT_SEARCH_URL, timeout=30).text)
        if i % 250 == 0 or i == len(todo):
            _save_ballot_registry(registry)
            log.info(f"  [ballot] {i:,}/{len(todo):,} combos swept -- "
                     f"{len(registry):,} distinct committees found so far "
                     f"(+{new_names:,} this run)")
        time.sleep(0.15)

    _save_ballot_registry(registry)
    log.registry_loaded("ballot_committees", len(registry), relation="committees")
    return registry


# ------------------------ per-committee filing walk -------------------------

def _find_ballot_committee(session, name: str):
    """Land on `name`'s exact result row via 'Begins With' -- same approach
    as _find_noncand_committee(). Returns (results_html, form_action_url,
    button_name, button_value) or (None, None, None, None) if the exact
    name can't be relocated."""
    tokens = _aspnet_tokens(session.get(BALLOT_SEARCH_URL, timeout=30).text)
    r = _postback(session, BALLOT_SEARCH_URL, tokens, {
        "ctl00$ContentPlaceHolder1$txtName": name[:80],
        "ctl00$ContentPlaceHolder1$rdList":  "1",   # Name Begins With
        "ctl00$ContentPlaceHolder1$btnNext": "Next",
    })
    soup = BeautifulSoup(r.text, "html.parser")
    for tag in soup.find_all("input", {"class": "link"}):
        val = re.sub(r"\s+", " ", tag.get("value", "")).strip()
        if val == name:
            return r.text, _form_action(r.text, BALLOT_SEARCH_URL), tag["name"], tag.get("value", "")
    return None, None, None, None


def walk_ballot_committee(log, session, name: str) -> dict | None:
    """Pull every Campaign Disclosure filing for one Ballot Measure
    committee -- report index, each filing's summary, and the itemized tabs
    whose summary total isn't a known zero. Statement of Organization
    filings are skipped (REPORT_TYPES_BUILT), same as Caucus/Party. Returns
    the full record (written by the caller) or None if the committee
    couldn't be relocated by exact name."""
    results_html, action, btn_name, btn_val = _find_ballot_committee(session, name)
    if action is None:
        log.warning(f"  [ballot] could not relocate '{name}' by exact name -- skipping")
        return None

    tokens = _aspnet_tokens(results_html)
    r_index = _postback(session, action, tokens, {btn_name: btn_val})
    index_action = _form_action(r_index.text, action)

    return _walk_report_index(log, session, "ballot", name,
                              r_index.text, index_action,
                              report_types=REPORT_TYPES_BUILT)


def _load_ballot_done() -> dict:
    if not BALLOT_DONE_MANIFEST.exists():
        return {}
    with open(BALLOT_DONE_MANIFEST, newline="", encoding="utf-8") as f:
        return {r["committee"]: r for r in csv.DictReader(f)}


def _append_ballot_done(committee: str, n_filings: int):
    write_header = not BALLOT_DONE_MANIFEST.exists()
    with open(BALLOT_DONE_MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=NONCAND_DONE_COLS)   # same shape, reused
        if write_header:
            w.writeheader()
        w.writerow({"committee": committee, "filings": n_filings,
                    "walked_at": datetime.today().strftime("%Y-%m-%d")})


def run_ballot_measure(log, force: bool = False, sweep_limit: int | None = None,
                       walk_limit: int | None = None):
    """Discover every SC Ballot Measure committee and pull its Campaign
    Disclosure filing history. Two resumable phases, identical structure to
    run_noncand_pacs(): sweep_ballot_registry() builds the name registry,
    then every registered name not already in ballot_filings_manifest.csv
    gets walked. `sweep_limit`/`walk_limit` cap how much NEW work each
    phase does this call -- used for testing.
    """
    registry = sweep_ballot_registry(log, force=force, limit=sweep_limit)

    if force:
        BALLOT_DONE_MANIFEST.unlink(missing_ok=True)
    done = set() if force else set(_load_ballot_done())

    todo = [n for n in registry if n not in done]
    if walk_limit is not None:
        todo = todo[:walk_limit]

    if not todo:
        log.info(f"  [ballot] filing walk already complete "
                 f"({len(done):,}/{len(registry):,} committees)")
        return

    log.info(f"  [ballot] walking {len(todo):,} of {len(registry):,} "
             f"committees ({len(done):,} already done)")

    BALLOT_FILINGS_DIR.mkdir(parents=True, exist_ok=True)
    session = _noncand_session()
    walked = 0
    for i, name in enumerate(todo, 1):
        record = walk_ballot_committee(log, session, name)
        if record is None:
            continue
        out_path = BALLOT_FILINGS_DIR / f"{_noncand_slug(name)}.json"
        out_path.write_text(json.dumps(record, indent=1))
        _append_ballot_done(name, len(record["filings"]))
        walked += 1
        if i % 25 == 0 or i == len(todo):
            log.info(f"  [ballot] {i:,}/{len(todo):,} committees walked "
                     f"({walked:,} succeeded)")

    log.info(f"  [ballot] filing walk done -- {walked:,} committees written")


# ============================== driver ================================

def build_driver(headless: bool = True):
    """Chrome with CDP performance logging and a private download directory."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1400,1000")
    # Chrome's default /dev/shm is too small in most containers and it crashes
    # mid-run; harmless on a desktop. --no-sandbox is only added when running as
    # root (the practical container signal) — it weakens Chrome's sandbox, so it
    # shouldn't be on for a normal local run.
    options.add_argument("--disable-dev-shm-usage")
    if getattr(os, "geteuid", lambda: 1000)() == 0:
        options.add_argument("--no-sandbox")
    options.add_experimental_option("prefs", {
        "download.default_directory": str(DOWNLOAD_DIR.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    # Without this capability driver.get_log("performance") raises — it is the
    # only way to see CDP network events from Selenium.
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(NAV_TIMEOUT)
    driver.set_script_timeout(SEARCH_TIMEOUT)

    _enlarge_network_buffer(driver)
    # Browser-scoped, unlike the deprecated Page.setDownloadBehavior, so it
    # survives navigation between origins — which the election-history download
    # relies on. Fall back for older Chrome builds that lack the Browser domain.
    try:
        driver.execute_cdp_cmd("Browser.setDownloadBehavior", {
            "behavior": "allow", "downloadPath": str(DOWNLOAD_DIR.resolve()),
        })
    except Exception:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow", "downloadPath": str(DOWNLOAD_DIR.resolve()),
        })
    return driver


def _enlarge_network_buffer(driver) -> None:
    """Re-issue Network.enable with buffers large enough for a year's response.

    Performance logging already enabled the Network domain; this call only
    resizes the body buffer (see _RESOURCE_BUFFER). It has to be re-issued after
    each navigation: chromedriver re-initializes its perf-logging DevTools
    client with default buffers on a new target, which silently reverts this and
    leaves Network.getResponseBody reporting "No resource with given identifier"
    for exactly the large response discovery needs.
    """
    try:
        driver.execute_cdp_cmd("Network.enable", {
            "maxResourceBufferSize": _RESOURCE_BUFFER,
            "maxTotalBufferSize":    _TOTAL_BUFFER,
        })
    except Exception:
        pass


# ============================ Kendo widgets ===========================

# Shared JS prologue: locate the option list belonging to one Kendo dropdown.
#
# Kendo renders its popup into a `.k-animation-container` appended to <body>,
# NOT inside the widget, so the list cannot be found by descending from the
# widget's element. It is tied back to its owner through the ARIA relationship
# (`aria-owns`/`aria-controls` on the widget → the listbox id), which is the
# only link that survives the popup being detached.
#
# The fallbacks matter because this portal has already been through at least one
# Kendo major version and the class names moved (`li.k-item` → `.k-list-item`).
# Each step is tried in order of specificity; the last resort is the whole
# document, which is what the original code always used.
_POPUP_JS = """
const host = document.getElementById(id);
if (!host) { return null; }
let list = null;
const owner  = host.querySelector('[aria-owns],[aria-controls]') || host;
const listId = owner.getAttribute('aria-owns') || owner.getAttribute('aria-controls');
if (listId) { list = document.getElementById(listId); }
if (!list) {
    const open = Array.from(document.querySelectorAll(
        '.k-animation-container ul.k-list, .k-animation-container kendo-list, ul.k-list'
    )).filter(el => el.getClientRects().length > 0);
    list = open[open.length - 1] || null;
}
const scope = list || document;
const items = Array.from(scope.querySelectorAll(
    'li.k-item, li[role="option"], .k-list-item'
));
"""

# Read option labels from the DOM rather than through Selenium's element.text.
#
# element.text returns *visible* text: the WebDriver atom walks up the ancestor
# chain and yields '' for a node clipped by an `overflow` container. Kendo's
# popup is a fixed-height scrolling list inside an `overflow:hidden` animation
# container, so every option below the fold reads back as an empty string — and
# the caller's `if o` filter then drops it silently. On an ascending year list
# that quietly truncates the NEWEST years, which is exactly how contributions
# came back as 2008-2024 while expenditures got the full range: the two popups
# differed only in how much of the list happened to be rendered in view.
#
# textContent has no such visibility rule and returns every option regardless of
# scroll position.
_OPTIONS_JS = _POPUP_JS + """
return items.map(el => (el.textContent || '').trim()).filter(t => t.length > 0);
"""

# Selecting has the same clipping problem: an option scrolled out of the popup
# is not "visible" to Selenium, so waiting on visibility_of_element_located for
# it times out. Scroll it into view inside the popup first, then click.
_PICK_JS = _POPUP_JS + """
const target = items.find(el => (el.textContent || '').trim() === want);
if (!target) { return false; }
target.scrollIntoView({block: 'center'});
target.click();
return true;
"""


def _select_kendo(driver, wait, dropdown_id: str, option_text: str) -> None:
    """Pick a value from a Kendo Angular <kendo-dropdownlist>.

    These are span/div widgets, not <select> elements, so Selenium's Select
    helper does not apply. The option list is rendered into a detached popup
    only after the widget is opened — the <li> clicked here does not exist in
    the DOM before the first click.
    """
    _open_kendo(driver, wait, dropdown_id)
    picked = driver.execute_script(
        f"const id = arguments[0], want = arguments[1];{_PICK_JS}",
        dropdown_id, option_text)
    if not picked:
        raise RuntimeError(
            f"{dropdown_id} has no option {option_text!r} "
            f"(available: {_kendo_options(driver, wait, dropdown_id)})")

    # Read the value back. A JS-dispatched click on a Kendo <li> can land without
    # Angular committing it to the model — the widget stays on its placeholder,
    # the subsequent Search is treated as an empty search, and the app renders a
    # validation notice instead of running the query. That failure is otherwise
    # invisible: it looks exactly like a grid that never loaded.
    shown = _selected_value(driver, dropdown_id)
    if shown != option_text:
        raise RuntimeError(
            f"{dropdown_id} still reads {shown!r} after selecting {option_text!r} "
            f"— the dropdown selection did not commit")


def _selected_value(driver, dropdown_id: str) -> str:
    """Text currently displayed by a Kendo dropdown ('2018', 'Any', ...)."""
    from selenium.webdriver.common.by import By
    try:
        el = driver.find_element(By.CSS_SELECTOR, f"#{dropdown_id} .k-input")
    except Exception:
        return ""
    return (el.get_attribute("textContent") or "").strip()


def _open_kendo(driver, wait, dropdown_id: str) -> None:
    """Click a Kendo dropdown open and wait for its popup to hold options.

    Waiting on the popup's *contents* rather than on the widget element matters:
    the year lists are populated by their own lookup XHR after the widget
    renders, so a popup opened too early is present but empty. Reading it then
    yields a short list — or none — with no error anywhere.
    """
    from selenium.webdriver.common.by import By

    wrap = driver.find_element(By.CSS_SELECTOR, f"#{dropdown_id} .k-dropdown-wrap")
    wrap.click()
    wait.until(lambda d: d.execute_script(
        f"const id = arguments[0];{_OPTIONS_JS}", dropdown_id))


def _close_kendo(driver) -> None:
    from selenium.webdriver import ActionChains
    from selenium.webdriver.common.keys import Keys

    # Sent to the active element, not the wrap span — W3C Element Send Keys
    # requires a keyboard-interactable target and raises
    # ElementNotInteractableException on a non-focusable <span>.
    ActionChains(driver).send_keys(Keys.ESCAPE).perform()


def _kendo_options(driver, wait, dropdown_id: str) -> list[str]:
    """Return every option label of a Kendo dropdown, then close it.

    Reads textContent from the popup's own DOM, so options scrolled below the
    fold are returned like any other (see _OPTIONS_JS). The list is polled until
    two consecutive reads agree, because the lookup XHR can land between the
    popup opening and the read, growing the list underneath it.
    """
    _open_kendo(driver, wait, dropdown_id)

    previous, opts = None, []
    deadline = time.time() + 15
    while time.time() < deadline:
        opts = driver.execute_script(
            f"const id = arguments[0];{_OPTIONS_JS}", dropdown_id) or []
        if opts and opts == previous:
            break
        previous = opts
        time.sleep(0.4)

    _close_kendo(driver)
    return [o.strip() for o in opts if o and o.strip()]


# ======================= endpoint discovery ===========================

def rows_of(payload):
    """Pull the record list out of a search response, whatever shape it takes.

    Kendo-backed .NET APIs variously return a bare array, a Kendo DataResult
    ({"Data": [...], "Total": n}), or an envelope with a single list-valued key.
    Rather than assume one, unwrap whichever is present and fall back to the
    longest list of objects anywhere one level down.
    """
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload if all(isinstance(x, dict) for x in payload) else []
    if isinstance(payload, dict):
        for key in ("Data", "data", "Results", "results", "Items", "items",
                    "Table", "records", "rows"):
            val = payload.get(key)
            if isinstance(val, list) and (not val or isinstance(val[0], dict)):
                return val
        best = []
        for val in payload.values():
            if isinstance(val, list) and val and isinstance(val[0], dict) and len(val) > len(best):
                best = val
        return best
    return []


def drain_perf_log(driver) -> list[dict]:
    """Read and clear chromedriver's buffered CDP events.

    Each entry wraps the CDP message in a JSON string under "message"; the
    payload of interest is entry["message"]["message"]. Malformed entries are
    skipped rather than raised on — the log is a diagnostic channel, and one bad
    line should not abort a scrape.
    """
    messages = []
    try:
        entries = driver.get_log("performance")
    except Exception:
        return messages
    for entry in entries:
        try:
            messages.append(json.loads(entry["message"])["message"])
        except (KeyError, TypeError, json.JSONDecodeError):
            continue
    return messages


def _captured_json(driver, messages: list[dict]) -> list[tuple[dict, object]]:
    """Pair each JSON XHR response in the log with its originating request.

    Returns [(request_info, parsed_body)]. Requests whose body Chrome has
    already evicted, or whose body isn't valid JSON, are dropped.
    """
    sent: dict[str, dict] = {}
    for msg in messages:
        if msg.get("method") == "Network.requestWillBeSent":
            params = msg.get("params", {})
            req    = params.get("request", {})
            sent[params.get("requestId", "")] = {
                "url":          req.get("url", ""),
                "method":       req.get("method", "GET"),
                "headers":      req.get("headers", {}) or {},
                "post_data":    req.get("postData"),
                "has_post_data": bool(req.get("hasPostData")),
            }

    out: list[tuple[dict, object]] = []
    for msg in messages:
        if msg.get("method") != "Network.responseReceived":
            continue
        params = msg.get("params", {})
        if (params.get("type") or "").lower() not in _XHR_TYPES:
            continue
        mime = (params.get("response", {}).get("mimeType") or "").lower()
        if "json" not in mime:
            continue

        rid = params.get("requestId", "")
        req = sent.get(rid)
        if not req:
            continue

        try:
            body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": rid})
            payload = json.loads(body.get("body") or "null")
        except Exception:
            # Body evicted from Chrome's buffer, or not JSON after all.
            continue

        # Chrome truncates postData above ~64 KB and sets hasPostData instead;
        # fetch the real body so the recipe replays faithfully.
        if req["has_post_data"] and not req["post_data"]:
            try:
                got = driver.execute_cdp_cmd("Network.getRequestPostData",
                                             {"requestId": rid})
                req["post_data"] = got.get("postData")
            except Exception:
                pass

        out.append((req, payload))
    return out


def _page_says(driver, marker: str) -> str:
    """Return the app-server-error text if it contains `marker`, else ''."""
    from selenium.webdriver.common.by import By
    for el in driver.find_elements(By.CSS_SELECTOR, "app-server-error"):
        text = " ".join((el.get_attribute("textContent") or "").split())
        if marker in text.lower():
            return text
    return ""


def _dump_diagnostics(log, driver, relation: str, messages: list[dict]) -> None:
    """Save a screenshot, the visible text, and the XHR list for a failed search.

    Discovery drives a live UI against a site this code cannot see from CI, so
    when it fails the single most useful thing is a picture of what the browser
    was actually looking at.
    """
    try:
        from src.reporting.logger import run_dir_for
        out_dir = run_dir_for(os.environ.get("CF_RUN_ID", "")) / "sc_discovery"
        out_dir.mkdir(parents=True, exist_ok=True)

        driver.save_screenshot(str(out_dir / f"{relation}.png"))

        from selenium.webdriver.common.by import By
        body = driver.find_element(By.TAG_NAME, "body")
        text = " ".join((body.get_attribute("innerText") or "").split())
        urls = sorted({
            m.get("params", {}).get("request", {}).get("url", "")
            for m in messages
            if m.get("method") == "Network.requestWillBeSent"
        })
        (out_dir / f"{relation}.txt").write_text(
            f"url: {driver.current_url}\n\nvisible text:\n{text[:8000]}\n\n"
            f"requests seen ({len(urls)}):\n" + "\n".join(urls),
            encoding="utf-8",
        )
        log.warning(f"  [{relation}] diagnostics written to {out_dir}")
    except Exception as e:
        log.warning(f"  [{relation}] could not write diagnostics: {e}")


def _await_search(log, driver, relation: str) -> list[dict]:
    """Block until the search resolves, and say why if it doesn't.

    Returns the accumulated CDP messages. Raises RuntimeError with the actual
    reason rather than letting a bare selector wait burn SEARCH_TIMEOUT and
    report only that a locator never appeared.

    Three outcomes are watched for concurrently:
      - the app's own validation notice  → fail immediately, quoting it
      - Kendo's "no records" placeholder → fail, but distinguishably
      - a large JSON response finishing, or grid rows appearing → success

    The network signal is the primary one: it is what the recipe is actually
    derived from, and it does not depend on Kendo's DOM structure staying put.
    """
    from selenium.webdriver.common.by import By

    messages: list[dict] = []
    deadline = time.time() + SEARCH_TIMEOUT
    json_ids: set[str] = set()

    while time.time() < deadline:
        messages.extend(drain_perf_log(driver))

        notice = _page_says(driver, _VALIDATION_MARKER)
        if notice:
            raise RuntimeError(f"the site refused the search: {notice}")

        # Cheap scan for a finished JSON response of meaningful size. Full body
        # retrieval is expensive, so it is left until after the loop.
        for msg in messages:
            params = msg.get("params", {})
            if msg.get("method") == "Network.responseReceived":
                mime = (params.get("response", {}) or {}).get("mimeType", "")
                if "json" in mime.lower():
                    json_ids.add(params.get("requestId", ""))
            elif (msg.get("method") == "Network.loadingFinished"
                    and params.get("requestId") in json_ids
                    and (params.get("encodedDataLength") or 0) > 20_000):
                time.sleep(1)                     # let the tail of the log flush
                messages.extend(drain_perf_log(driver))
                return messages

        if any(driver.find_elements(By.CSS_SELECTOR, sel) for sel in _GRID_ROW_SELECTORS):
            time.sleep(1)
            messages.extend(drain_perf_log(driver))
            return messages

        if driver.find_elements(By.CSS_SELECTOR, _NO_RECORDS_SELECTOR):
            raise RuntimeError(
                "the search ran but returned no records for the probe year — "
                "pick a different probe_year in PAGES")

        time.sleep(0.5)

    _dump_diagnostics(log, driver, relation, messages)
    raise RuntimeError(
        f"search did not resolve within {SEARCH_TIMEOUT}s — no grid rows, no "
        f"JSON response, and no error on the page (see the diagnostics dump)")


def discover_recipe(log, driver, wait, relation: str) -> dict | None:
    """Run one UI search and capture the request that fed the grid.

    Returns a replayable recipe dict, or None if no response carried rows.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    spec = PAGES[relation]
    year = spec["probe_year"]

    log.info(f"  [{relation}] discovering search endpoint (probe year {year})...")
    driver.get(spec["url"])
    _enlarge_network_buffer(driver)   # new target resets it — see the docstring
    wait.until(EC.presence_of_element_located((By.ID, spec["year_field"])))
    _select_kendo(driver, wait, spec["year_field"], str(year))
    log.info(f"  [{relation}] {spec['year_field']} = "
             f"{_selected_value(driver, spec['year_field'])!r}")

    # Discard page-load traffic so only the search's own requests are candidates.
    drain_perf_log(driver)

    search_btn = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//button[normalize-space()='Search']")))
    search_btn.click()

    candidates = _captured_json(driver, _await_search(log, driver, relation))

    # The winning response is simply the one that returned the most records —
    # the year search dwarfs the dropdown-population lookups by orders of
    # magnitude, so there is no ambiguity in practice.
    best_req, best_rows = None, []
    for req, payload in candidates:
        rows = rows_of(payload)
        if len(rows) > len(best_rows):
            best_req, best_rows = req, rows

    if best_req is None or not best_rows:
        log.warning(f"  [{relation}] the search resolved but no JSON response "
                    f"carried rows — cannot build a recipe")
        _dump_diagnostics(log, driver, relation, [])
        return None

    # Keep only headers that affect routing/negotiation. Replaying the whole set
    # would drag along content-length and hop-by-hop headers that must not be
    # resent, and fetch() rejects several of them outright.
    keep    = ("accept", "content-type", "authorization", "x-requested-with")
    headers = {k: v for k, v in best_req["headers"].items() if k.lower() in keep}

    recipe = {
        "relation":      relation,
        "method":        best_req["method"],
        "url":           best_req["url"],
        "post_data":     best_req["post_data"],
        "headers":       headers,
        "probe_year":    year,
        "probe_rows":    len(best_rows),
        "discovered_at": datetime.now().isoformat(timespec="seconds"),
    }
    log.info(f"  [{relation}] endpoint: {recipe['method']} {recipe['url']} "
             f"({len(best_rows):,} rows for {year})")
    return recipe


def _try_discover(log, driver, wait, relation: str) -> dict | None:
    """discover_recipe() with its failure contained to one relation.

    Discovery drives a live UI, so it can raise on a navigation timeout, a
    selector change, or a slow search. Per docs/contributing.md §4 that is a
    per-file failure — the other two screens should still be attempted — not
    something that should abort the whole run.
    """
    try:
        recipe = discover_recipe(log, driver, wait, relation)
    except Exception as e:
        log.file_download_error(filename=f"{relation}_*.json",
                                error=f"endpoint discovery failed: {e}")
        return None
    if recipe is None:
        log.file_download_error(filename=f"{relation}_*.json",
                                error="endpoint discovery returned no rows")
    return recipe


def load_recipes() -> dict:
    if not RECIPE.exists():
        return {}
    try:
        return json.loads(RECIPE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_recipes(recipes: dict):
    RECIPE.write_text(json.dumps(recipes, indent=2), encoding="utf-8")


# ========================== recipe replay =============================

def _swap_year(value, old: int, new: int):
    """Replace an exact year value with a new one, recursing through JSON.

    Deliberately exact-match only: a bare `str.replace("2018", "2020")` over the
    whole body would also rewrite an unrelated id that happens to contain those
    digits. Only values that ARE the year (as int or as its string form) change.
    """
    if isinstance(value, dict):
        return {k: _swap_year(v, old, new) for k, v in value.items()}
    if isinstance(value, list):
        return [_swap_year(v, old, new) for v in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value == old:
        return new
    if isinstance(value, str) and value.strip() == str(old):
        return str(new)
    return value


def _retarget(recipe: dict, year: int) -> tuple[str, str, str | None]:
    """Rewrite a recipe for a different year → (method, url, body)."""
    old   = int(recipe["probe_year"])
    token = re.compile(rf"(?<!\d){old}(?!\d)")

    parsed = urlparse(recipe["url"])

    # The year can sit in the path (/api/contributions/2018) as easily as in the
    # query. Missing the path case is silent and total: every year would return
    # probe-year rows under the wrong filename with no error anywhere.
    if parsed.path:
        parsed = parsed._replace(path=token.sub(str(year), parsed.path))

    if parsed.query:
        params  = parse_qsl(parsed.query, keep_blank_values=True)
        swapped = [(k, str(year) if v.strip() == str(old) else v) for k, v in params]
        # Only re-serialize when something actually changed — a parse/urlencode
        # round-trip re-percent-encodes characters the app sent raw (Kendo's
        # filter[filters][0], $top), silently altering the replayed request.
        if swapped != params:
            parsed = parsed._replace(query=urlencode(swapped))
    url = urlunparse(parsed)

    body = recipe.get("post_data")
    if body:
        try:
            body = json.dumps(_swap_year(json.loads(body), old, year))
        except (json.JSONDecodeError, TypeError):
            # Non-JSON body (form-encoded). Fall back to a token-boundary
            # substitution so adjacent digits in ids are left alone.
            body = token.sub(str(year), body)

    return recipe["method"], url, body


def fetch_year(driver, recipe: dict, year: int) -> list[dict]:
    """Replay a recipe for one year via an in-page fetch and return its rows."""
    method, url, body = _retarget(recipe, year)
    result = driver.execute_async_script(
        _FETCH_JS, url, method, body, recipe.get("headers") or {})
    if not result or not result.get("ok"):
        raise RuntimeError(
            f"HTTP {(result or {}).get('status', '?')} from {url}"
            + (f" — {result['error']}" if result and result.get("error") else ""))
    return rows_of(json.loads(result.get("text") or "null"))


def download_year(log, driver, recipe: dict, relation: str, year: int) -> tuple[str, int] | None:
    """Fetch one relation-year and write it to raw/. Returns (filename, rows)."""
    filename = f"{relation}_{year}.json"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()
    try:
        rows = fetch_year(driver, recipe, year)
    except Exception as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    out_path.write_text(
        json.dumps({"relation": relation, "year": year,
                    "retrieved_at": datetime.now().isoformat(timespec="seconds"),
                    "rows": rows}),
        encoding="utf-8",
    )
    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=len(rows),
                         duration_s=round(time.perf_counter() - t0, 2))
    return filename, len(rows)


# ==================== election history (tier-2) =======================

# Chrome's own transient artifacts in the download dir. `.crdownload` is an
# in-progress part file; the `.com.google.Chrome.XXXXXX` scratch entries are
# created before the real name is known. Returning either as the export would
# write garbage into raw/.
_DOWNLOAD_JUNK = re.compile(r"(\.crdownload$|\.tmp$|^\.com\.google\.Chrome\.)")

# How long a settled zero-byte download must stay zero-byte before it is taken
# as an empty result rather than a download still in flight.
_EMPTY_DOWNLOAD_GRACE = 5


class _EmptyExport(RuntimeError):
    """The service returned a body, or an attachment, with no rows.

    Ambiguous on its own. For a stage slice it usually means "this year had no
    Democratic runoff", which is a complete answer — but the API also returns an
    empty result (rather than an error) for a search object it doesn't accept,
    so an empty slice can equally mean the filter value was malformed.
    _recover_year resolves it by looking at the other slices: if any of them
    returned rows, the filter shape is demonstrably right and the empties are
    real.
    """


def _wait_for_download(before: set, timeout: int = DOWNLOAD_TIMEOUT) -> Path | None:
    """Poll the download dir for a new, fully-written file. None on timeout.

    The service saves as `elstats_search_<hash>.csv`, so an extension filter
    does match today. This matches on "new, complete, non-scratch file" anyway —
    the export is the only thing this run downloads, so the extension adds no
    discrimination, and whether the bytes are really CSV is settled by the
    caller's content check, which is the honest test either way.

    NOT the reason a run can fail to find its download: see the caller, where
    the real hazards are the size of this export and the fact that the service
    can terminate the stream early.
    """
    end = time.time() + timeout
    empty_since = None
    while time.time() < end:
        settled = [n for n in set(glob.glob(str(DOWNLOAD_DIR / "*"))) - before
                   if os.path.isfile(n)
                   and not _DOWNLOAD_JUNK.search(os.path.basename(n))
                   and not os.path.exists(n + ".crdownload")]
        fresh = [n for n in settled if os.path.getsize(n) > 0]
        if fresh:
            # Prefer an explicit .csv if the service ever starts sending one,
            # otherwise take the most recently written candidate.
            csvs = [n for n in fresh if n.lower().endswith(".csv")]
            return Path(max(csvs or fresh, key=os.path.getmtime))
        if settled:
            # A settled but zero-byte file is a real answer — the service sent
            # an attachment with no rows — and used to be indistinguishable from
            # "nothing arrived", because the size filter dropped it and the
            # caller then blamed the navigation. That cost a full DOWNLOAD_TIMEOUT
            # and surfaced as "no CSV download appeared", which sent debugging
            # after the browser rather than the query.
            #
            # Still given a grace period: Chrome briefly exposes a 0-byte file at
            # the final name before the bytes land in some versions, so an
            # immediate return here would race a real download to an empty result.
            empty_since = empty_since or time.time()
            if time.time() - empty_since >= _EMPTY_DOWNLOAD_GRACE:
                raise _EmptyExport("the service returned an empty export")
        else:
            empty_since = None
        time.sleep(0.5)
    return None


# The service streams the CSV and, on hitting a contest it can't render, ends
# the stream by appending a JSON error object as a final line instead of
# returning an HTTP error. Everything before it is valid CSV and the file still
# ends in a newline, so the only way to know the export is partial is to look
# for this. Observed: {"errors":[{"message":"this contest does not have a
# division assigned"}]} on a 2022 contest, which truncated a 2008-present
# request down to 2025-2022.
_STREAM_ERROR = re.compile(r'^\s*\{\s*"errors"\s*:')


def _download_history_csv(driver, url: str) -> Path | str:
    """Navigate at an export URL and return the downloaded file, or its text.

    Returns a Path when Chrome saved a file, or the rendered body text when the
    service served the CSV inline instead of as an attachment.
    """
    from selenium.common.exceptions import TimeoutException, WebDriverException

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # Clear anything a previous year or run orphaned here. The directory is
    # scratch space meant to be empty between downloads: a leftover file keeps
    # `before` from being a clean baseline and blocks the rmdir in run()'s
    # finally block.
    for stale in glob.glob(str(DOWNLOAD_DIR / "*")):
        try:
            os.remove(stale)
        except OSError:
            pass
    before = set(glob.glob(str(DOWNLOAD_DIR / "*")))

    try:
        driver.get(url)
    except (TimeoutException, WebDriverException):
        # A navigation that turns into a download raises a page-load timeout in
        # some Chrome/chromedriver combinations even though the download itself
        # succeeds. Cancel the pending load so later commands don't see a
        # half-navigated page, then fall through to the file poll.
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass

    landed = _wait_for_download(before)
    if landed is not None:
        return landed
    if driver.current_url.startswith(ELECTION_HISTORY_URL):
        # Served inline rather than as an attachment — Chrome rendered it.
        # Guarded on current_url: a download leaves the browser on the PREVIOUS
        # page, so an unguarded read would scrape the ethics portal DOM and
        # write it out as if it were the CSV.
        return driver.execute_script("return document.body.innerText;") or ""
    raise RuntimeError("no CSV download appeared and the browser did not "
                       "navigate to the export URL")


def _append_history_year(source: Path | str, out, write_header: bool,
                         seen: set | None = None,
                         dedupe: bool = False) -> tuple[int, bool, str]:
    """Stream one year's export into the open output handle.

    Returns (data_rows_written, truncated, header_line). Streams line by line
    rather than read_text()-ing the whole thing: a single year of this export
    runs to hundreds of MB, and the previous whole-file read materialized it as
    a str and then again as a list from splitlines().

    `seen` accumulates a digest per data line. `dedupe` decides what to do with
    a repeat, and the two are deliberately separable:

      - First pass for a year: seen is filled, dedupe is False. The export is
        written through verbatim. A real 2016 export contains 265 byte-identical
        lines (same contest, candidate, division and vote channel), and dropping
        them would silently disagree with the row count of the same file
        downloaded by hand.
      - Recovery slice: dedupe is True. Slices overlap the original partial by
        construction — the partial holds every contest up to the poison point
        regardless of stage — so without this the overlap would be duplicated.
    """
    if isinstance(source, Path):
        handle = open(source, encoding="utf-8-sig", errors="replace", newline="")
    else:
        import io
        handle = io.StringIO(source)

    rows, truncated, first, header = 0, False, True, ""
    try:
        for line in handle:
            if first:
                first = False
                if "," not in line:
                    raise RuntimeError("response does not look like CSV")
                header = line.rstrip("\r\n")
                if write_header:
                    out.write(line if line.endswith("\n") else line + "\n")
                continue
            if _STREAM_ERROR.match(line):
                truncated = True
                break
            if not line.strip():
                continue
            if seen is not None:
                # 16 bytes: a 64-bit hash() would silently drop a row on a
                # collision, and at a few million lines per year that is a real
                # if small probability. Digesting is also stable across the
                # process, which hash() on str is not.
                digest = hashlib.blake2b(line.strip().encode("utf-8",
                                                             "replace"),
                                         digest_size=16).digest()
                if dedupe and digest in seen:
                    continue
                seen.add(digest)
            out.write(line if line.endswith("\n") else line + "\n")
            rows += 1
    finally:
        handle.close()
        if isinstance(source, Path):
            # Swallowed deliberately: this runs in a finally, so a failure to
            # clean up the scratch copy must not replace the real result (or the
            # real exception) with an unlink error. A leftover file is harmless
            # — the next download purges the directory first.
            try:
                source.unlink(missing_ok=True)
            except OSError:
                pass

    if first:
        raise _EmptyExport("empty response body")
    return rows, truncated, header


def history_filename(year: int) -> str:
    """raw/ filename for one election-history year.

    Kept as a function rather than an f-string at each call site because the
    parser derives its glob from the same prefix — the two have to agree, and a
    single definition is the only way to make that structural rather than a
    convention someone has to remember.
    """
    return f"{ELECTION_HISTORY_RELATION}_{year}.csv"


def _history_url(year: int, stage: dict | None = None) -> str:
    """Export URL for one year, optionally narrowed to a single stage.

    `stage` is an entry of _ELECTION_STAGES. Only its id/id2 are sent — `name`
    is for logging. id2 is omitted entirely for General rather than sent as
    null, mirroring how the search page builds the object.
    """
    search = json.loads(json.dumps(ELECTION_HISTORY_SEARCH))  # deep copy
    search["global"]["years"] = {"from": year, "to": year}
    if stage is not None:
        entry = {"id": stage["id"]}
        if stage["id2"] is not None:
            entry["id2"] = stage["id2"]
        search["stages"] = [entry]
    # Separators matter: their API rejects a body with spaces after delimiters.
    return (f"{ELECTION_HISTORY_URL}?search="
            f"{quote(json.dumps(search, separators=(',', ':')))}")


def _types_present(kept: Path) -> tuple[set, set]:
    """Scan the kept rows for which stages and unfilterable types appear.

    Returns (stage keys as (id, id2), unfilterable type names). Splits on commas
    rather than going through csv.reader: the columns being read are the 4th and
    5th, and every field before them (contest_id, election_id, an ISO date) is
    comma-free. question_text, the first field that can contain a comma, comes
    later and is never reached.
    """
    stages, odd = set(), set()
    try:
        with open(kept, encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                parts = line.split(",", 5)
                if len(parts) < 5:
                    continue
                etype = parts[3].strip().strip('"').lower()
                party = parts[4].strip().strip('"').lower()
                hit = _EXPORT_TYPE_TO_STAGE.get((etype, party))
                if hit:
                    stages.add((hit[1], hit[2]))
                elif etype in _UNFILTERABLE_TYPES:
                    odd.add(etype)
    except OSError:
        pass
    return stages, odd


def _recover_year(log, driver, year: int, kept: Path,
                  seen: set) -> tuple[int, bool]:
    """Re-request a truncated year one stage at a time, appending what's new.

    Rows already written for this year are in `seen`, and `kept` is appended to
    in place, so this can only add rows — a stage that errors, returns nothing,
    or returns only duplicates leaves the year exactly as the first attempt left
    it. That matters because whether `stages` accepts these values at all is
    unconfirmed (see _STAGE_SLICE_FALLBACK).

    Returns rows_recovered. The year stays flagged partial either way, and that
    is not pessimism: the five stages cannot express Special elections or
    recounts (_UNFILTERABLE_TYPES), and those contests were only ever going to
    arrive on the unsliced stream that just got cut. So recovery can add rows
    but can never establish that a year is whole again. The flag means "known
    incomplete", and the honest answer for a truncated year is that it stays
    that way.

    An empty slice is normally a real answer — plenty of years have no
    Democratic runoff. The exception is a stage the kept rows already prove is
    present: that combination means the filter is being rejected, which is a
    fault in _ELECTION_STAGES rather than a fact about the year, and is called
    out loudly because it is otherwise invisible.
    """
    known, odd = _types_present(kept)
    recovered = served = empty = 0

    with open(kept, "a", encoding="utf-8", newline="") as out:
        for stage in _ELECTION_STAGES:
            label = stage["name"]
            key = (stage["id"], stage["id2"])
            try:
                source = _download_history_csv(driver, _history_url(year, stage))
                rows, cut, _ = _append_history_year(source, out,
                                                    write_header=False,
                                                    seen=seen, dedupe=True)
            except _EmptyExport:
                empty += 1
                if key in known:
                    log.warning(
                        f"  [election_history] {year} [{label}]: slice came "
                        f"back empty, but this stage IS present in the rows "
                        f"already downloaded — the `stages` filter is rejecting "
                        f"{{'id':{stage['id']}"
                        + (f",'id2':{stage['id2']}" if stage["id2"] is not None
                           else "")
                        + "}. Check _ELECTION_STAGES against the search page.")
                continue
            except Exception as e:
                log.warning(f"  [election_history] {year} [{label}]: {e}")
                continue
            recovered += rows
            served += 1
            log.info(f"  [election_history] {year} [{label}]: +{rows:,} new rows")
            if cut:
                log.warning(f"  [election_history] {year} [{label}]: slice also "
                            f"ended early")
            time.sleep(0.5)

    if not served:
        log.warning(f"  [election_history] {year}: no stage slice returned any "
                    f"rows ({empty} empty) — the `stages` filter may no longer "
                    f"accept these (id, id2) pairs (see _ELECTION_STAGES). "
                    f"Keeping the original partial.")
    else:
        log.info(f"  [election_history] {year}: recovered {recovered:,} rows "
                 f"from {served} stage slice(s), {empty} empty")
    if odd:
        log.warning(f"  [election_history] {year}: contains {sorted(odd)} "
                    f"contests, which no stage filter can request — any of "
                    f"those past the cut cannot be recovered")

    return recovered


def download_history_year(log, driver,
                          year: int) -> tuple[str, int, bool] | None:
    """Download one election-history year into raw/election_history_<year>.csv.

    Cross-origin to the ethics portal, so this navigates Chrome at the URL and
    collects the resulting download rather than using the in-page fetch() the
    year replays use (see module docstring).

    WHY ONE REQUEST PER YEAR
    ------------------------
    A single 2008-present request does not come back whole. The response is
    streamed, and when the service reaches a contest it can't render it appends
    a JSON error and closes the stream (see _STREAM_ERROR) — so one bad contest
    in 2022 silently truncated the entire export to 2025-2022, while still
    producing a well-formed 822 MB CSV that passed every check the old code made.
    Requesting a year at a time contains that: a poisoned year costs that year,
    is reported, and the remaining years still land. It also keeps each response
    small enough to finish inside DOWNLOAD_TIMEOUT.

    The year is written through a .part file and moved into place only once the
    download (and any stage-by-stage recovery) has finished, so an interrupted
    run cannot leave a half-written year in raw/ that the next run's
    file-existence check would then accept as complete.

    Returns (filename, rows, truncated), or None if the year could not be
    downloaded at all. `truncated` is True when the service cut the stream —
    the file is real and worth keeping, but is known to be missing its oldest
    contests (see _recover_year).
    """
    filename = history_filename(year)
    out_path = RAW_DIR / filename
    part     = out_path.with_name(out_path.name + ".part")

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    # Digests of this year's rows only. Scoped per year so the set is released
    # between years rather than growing to the full nine-million-row export,
    # and because cross-year duplicates are not possible anyway.
    seen: set[bytes] = set()
    cut = False
    try:
        with open(part, "w", encoding="utf-8", newline="") as out:
            source = _download_history_csv(driver, _history_url(year))
            # Header per file now, not once for a combined export: each year
            # has to stand on its own as a readable CSV.
            rows, cut, _ = _append_history_year(source, out,
                                                write_header=True, seen=seen)
        if cut:
            log.warning(f"  [election_history] {year}: the service ended the "
                        f"stream early — {rows:,} rows before the cut, "
                        f"attempting stage-by-stage recovery")
            rows += _recover_year(log, driver, year, part, seen)
    except Exception as e:
        part.unlink(missing_ok=True)
        log.file_download_error(filename=filename, error=str(e))
        return None

    os.replace(part, out_path)
    if cut:
        log.warning(f"  [election_history] {year}: PARTIAL ({rows:,} rows kept "
                    f"after recovery) — party/district/jurisdiction backfill "
                    f"will be incomplete for contests in this year")

    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=rows,
                         duration_s=round(time.perf_counter() - t0, 2))
    return filename, rows, cut


# ============================ year scoping ============================

def resolve_years(log, driver, wait, relation: str,
                  start_year: int | None, end_year: int | None) -> list[int]:
    """Years to download for a relation, read off the live year dropdown."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    spec  = PAGES[relation]
    years: list[int] = []
    try:
        driver.get(spec["url"])
        wait.until(EC.presence_of_element_located((By.ID, spec["year_field"])))
        for opt in _kendo_options(driver, wait, spec["year_field"]):
            if re.fullmatch(r"\d{4}", opt):
                years.append(int(opt))
    except Exception as e:
        log.warning(f"  [{relation}] could not read year dropdown ({e}) — "
                    f"falling back to {FALLBACK_FIRST_YEAR}-{datetime.today().year}")

    if not years:
        # Flagged loudly: the fallback range happens to be identical to what the
        # live dropdown returns, so without this the log looks the same whether
        # the widget was read or never opened at all — which matters, because a
        # dropdown that can't be read is also a dropdown that can't be set.
        years = list(range(FALLBACK_FIRST_YEAR, datetime.today().year + 1))
        log.warning(f"  [{relation}] using the hardcoded year fallback, NOT the "
                    f"live dropdown")
    else:
        log.info(f"  [{relation}] year dropdown read from the page "
                 f"({len(years)} options)")

    years = sorted(set(years))
    if start_year is not None:
        years = [y for y in years if y >= start_year]
    if end_year is not None:
        years = [y for y in years if y <= end_year]
    return years


# ================================ run =================================

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
    rediscover: bool = False,
    headed: bool = False,
    headless: bool = False,
    pacs: bool = False,
    party_caucus: bool = False,
    party_refresh_days: int = PARTY_REFRESH_DAYS,
    ballot_measure: bool = False,
):
    """Download SC contributions, expenditures, filed reports, election history,
    and (opt-in) Non-Candidate committee (PAC), Caucus/Party, and Ballot
    Measure committee filings.

    Horizontal scope maps onto the portal's three screens: --transactions covers
    contributions + expenditures, --entities covers the reports screen (the only
    bulk source of candidate/office metadata) plus the election-history CSV.
    --candidates and --committees both resolve to entities — SC publishes no
    separate registry for either, so the split happens at parse time.

    --pacs, --party-caucus, and --ballot-measure are unrelated sources
    (apps.sc.gov, not ethicsfiling.sc.gov — see the Non-Candidate Committees /
    Caucus & Party Committees / Ballot Measure Committees sections above) and
    are deliberately NOT part of the "no flag = everything" default: --pacs
    and --ballot-measure are each a ~17,576-query brute force name-search
    sweep plus a filing walk over every committee found; --party-caucus is a
    smaller but still real ~3,500-combo dropdown sweep (46 counties + 269
    cities, each x 11 parties, plus 13 caucuses + 11 state parties). All
    three are an order of magnitude more requests than the rest of this
    scraper combined. Ask for any of them explicitly.

    --party-caucus is safe to run periodically (e.g. from cron): combos are
    only skipped while their manifest entry is fresher than
    `party_refresh_days` (default PARTY_REFRESH_DAYS), so known committees
    get rechecked for new quarterly filings and previously-empty combos get
    rechecked for newly-registered committees. --force still wipes the
    manifest outright for a full reset regardless of age. --pacs and
    --ballot-measure do NOT have this yet — both are still "done forever"
    once a committee is walked once; see docs/states/south_carolina.md.
    """
    log = get_logger("south carolina", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, contributions=contributions,
              expenditures=expenditures, candidates=candidates,
              committees=committees, pacs=pacs, party_caucus=party_caucus,
              ballot_measure=ballot_measure,
              start_year=start_year, end_year=end_year)

    any_horizontal = (entities or transactions or contributions or
                      expenditures or candidates or committees or pacs or
                      party_caucus or ballot_measure)
    no_horizontal  = not any_horizontal

    do_contributions = no_horizontal or transactions or contributions
    do_expenditures  = no_horizontal or transactions or expenditures
    do_entities      = no_horizontal or entities or candidates or committees
    do_pacs          = pacs           # never part of the "no flag" default — see docstring
    do_party_caucus  = party_caucus   # never part of the "no flag" default — see docstring
    do_ballot        = ballot_measure # never part of the "no flag" default — see docstring

    targets = []
    if do_contributions: targets.append("contributions")
    if do_expenditures:  targets.append("expenditures")
    if do_entities:      targets.append("reports")

    # --pacs, --party-caucus, and --ballot-measure are plain requests, not
    # Selenium — a run that only asks for these should never need Chrome at
    # all, so all three are handled entirely before the Selenium-only block
    # below, and Selenium is only required when targets is non-empty.
    apps_sc_gov_error = False
    if do_pacs:
        try:
            run_noncand_pacs(log, force=force)
        except Exception as e:
            log.warning(f"  [noncand] PAC scrape failed: {e}")
            apps_sc_gov_error = True
    if do_party_caucus:
        try:
            run_party_caucus(log, force=force, refresh_days=party_refresh_days)
        except Exception as e:
            log.warning(f"  [party_caucus] Caucus/Party scrape failed: {e}")
            apps_sc_gov_error = True
    if do_ballot:
        try:
            run_ballot_measure(log, force=force)
        except Exception as e:
            log.warning(f"  [ballot] Ballot Measure scrape failed: {e}")
            apps_sc_gov_error = True
    if apps_sc_gov_error and not targets:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=1)
        raise RuntimeError("apps.sc.gov scrape(s) failed — see warnings above")
    if not targets:
        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s (apps.sc.gov sources only, no Selenium needed)")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=int(do_pacs or do_party_caucus or do_ballot), files_err=0)
        return

    try:
        from selenium.webdriver.support.ui import WebDriverWait
    except ImportError:
        log.error("[!] Selenium not installed. Run: pip install selenium "
                  "(Chrome must also be installed; chromedriver is auto-managed)")
        log._emit("scrape_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, error="selenium not installed")
        # Re-raise rather than return: a bare return exits 0, which orc.py would
        # read as a successful scrape and happily parse stale raw files.
        raise

    current_year      = datetime.today().year
    year_range_active = start_year is not None or end_year is not None

    # Relations this run will actually re-fetch. election_history only rides
    # along when entities are in scope, so a --force --contributions run must
    # not wipe its manifest entries.
    in_scope = set(targets) | ({ELECTION_HISTORY_RELATION} if do_entities else set())

    # Manifest scoping. Wipe the entries the run is about to refresh, so the
    # file-existence fallback below can't resurrect a skip for a year the
    # operator explicitly asked for.
    if force:
        strip_manifest(lambda r: r.get("relation") not in in_scope)
    elif year_range_active:
        def _keep(r: dict) -> bool:
            # in_scope, not targets: election_history is year-keyed now, so a
            # --start-year run has to clear its rows for the range too. While it
            # was one combined row keyed on "2008-2026" this had to skip it,
            # which is what the int() guard below was for.
            if r.get("relation") not in in_scope:
                return True
            try:
                yr = int(r["year"])
            except (ValueError, KeyError, TypeError):
                # A non-year key can now only be a combined election_history row
                # left by the pre-split scraper. It describes a file this run no
                # longer writes, so drop it rather than let it linger and be
                # read as coverage the raw/ directory doesn't have.
                return r.get("relation") != ELECTION_HISTORY_RELATION
            if start_year is not None and yr < start_year:
                return True
            if end_year is not None and yr > end_year:
                return True
            return False
        strip_manifest(_keep)

    done = {(r.get("relation"), str(r.get("year"))) for r in load_manifest()}

    recipes  = {} if rediscover else load_recipes()
    files_ok = files_err = 0
    driver   = None

    try:
        # HEADLESS is the default (False); --headless opts in, --headed forces
        # visible even if the module default is ever flipped back.
        driver = build_driver(headless=(headless or HEADLESS) and not headed)
        wait   = WebDriverWait(driver, NAV_TIMEOUT)

        for relation in targets:
            years = resolve_years(log, driver, wait, relation, start_year, end_year)
            if not years:
                log.warning(f"  [{relation}] no years in scope — skipping")
                continue
            log.info(f"  [{relation}] {len(years)} year(s): {years[0]}-{years[-1]}")

            # `not recipe`, not `is None`: a truncated api_recipe.json can leave
            # an empty dict here, which would KeyError on 'probe_year' in
            # _retarget rather than triggering rediscovery.
            recipe = recipes.get(relation)
            if not recipe:
                recipe = _try_discover(log, driver, wait, relation)
                if recipe is None:
                    files_err += 1
                    continue
                recipes[relation] = recipe
                save_recipes(recipes)

            for year in years:
                key      = (relation, str(year))
                filename = f"{relation}_{year}.json"
                expected = RAW_DIR / filename

                # The current year is always refreshed: filings for the year in
                # progress keep arriving, so both a manifest hit and an on-disk
                # file from an earlier run are stale by definition. An explicit
                # --start-year/--end-year range also always re-fetches — the
                # operator is asking for a refresh.
                refresh = (year == current_year or year_range_active or force)
                already = not refresh and (
                    key in done
                    # File-existence fallback for a manifest that was lost or
                    # truncated while the raw files survived.
                    or (expected.exists() and expected.stat().st_size > 0)
                )
                if already:
                    log.file_download_skip(filename=filename)
                    continue

                result = download_year(log, driver, recipe, relation, year)

                # A cached recipe that suddenly returns nothing usually means
                # the route moved. Rediscover once and retry before giving up.
                if result is not None and result[1] == 0 and not rediscover:
                    fresh = _try_discover(log, driver, wait, relation)
                    if fresh is None:
                        # Couldn't confirm the route is still good, so an empty
                        # result can't be trusted. Recording it would write a
                        # 0-row file and a manifest entry that make every later
                        # run skip this year permanently.
                        log.file_download_error(
                            filename=filename,
                            error="0 rows and endpoint could not be re-verified")
                        (RAW_DIR / filename).unlink(missing_ok=True)
                        files_err += 1
                        continue
                    recipes[relation] = recipe = fresh
                    save_recipes(recipes)
                    # A second empty result against a freshly verified endpoint
                    # means the year really is empty — record it.
                    result = download_year(log, driver, recipe, relation, year)

                if result is None:
                    files_err += 1
                    continue

                filename, row_count = result
                upsert_manifest({
                    "relation":      relation,
                    "year":          year,
                    "filename":      filename,
                    "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                    "row_count":     row_count,
                })
                files_ok += 1
                time.sleep(0.5)

        # Election history rides along with entities — it exists purely to fill
        # the candidate columns the ethics portal doesn't publish. One file and
        # one manifest row per year, on the same skip rules as the three portal
        # relations.
        if do_entities:
            eh_first = start_year or ELECTION_HISTORY_FLOOR
            eh_last  = end_year   or current_year

            # Drop any combined row left by the pre-split scraper. It claims
            # coverage for a file this run no longer writes, and its year key
            # ("2008-2026") can never match a per-year lookup, so left alone it
            # would sit in the manifest forever describing nothing.
            strip_manifest(lambda r: not (
                r.get("relation") == ELECTION_HISTORY_RELATION
                and not str(r.get("year", "")).isdigit()))

            # Years the service truncated mid-stream on a previous run. A file
            # it cut short is not a finished download: skipping it would make
            # the partial permanent for any range that doesn't reach the current
            # year, so a recorded partial always re-runs — the poisoning contest
            # is server-side and may well be fixed by now.
            eh_partial = {str(r.get("year")) for r in load_manifest()
                          if r.get("relation") == ELECTION_HISTORY_RELATION
                          and (r.get("partial_years") or "").strip()}

            legacy = RAW_DIR / LEGACY_ELECTION_HISTORY_FILE
            if legacy.exists():
                log.warning(
                    f"  [election_history] {LEGACY_ELECTION_HISTORY_FILE} is "
                    f"from the previous combined layout and is no longer read "
                    f"once per-year files exist — safe to delete "
                    f"({legacy.stat().st_size / 1e6:,.0f} MB)")

            for year in range(eh_first, eh_last + 1):
                key      = (ELECTION_HISTORY_RELATION, str(year))
                filename = history_filename(year)
                expected = RAW_DIR / filename

                # Same current-year rule as the portal relations: results for
                # the year in progress keep being certified, so it is stale by
                # definition.
                refresh = (year == current_year or year_range_active or force)
                already = not refresh and str(year) not in eh_partial and (
                    key in done
                    or (expected.exists() and expected.stat().st_size > 0)
                )
                if already:
                    log.file_download_skip(filename=filename)
                    continue
                if str(year) in eh_partial and not refresh:
                    log.info(f"  [election_history] {year}: previous download "
                             f"was partial — re-downloading rather than skipping")

                result = download_history_year(log, driver, year)
                if result is None:
                    files_err += 1
                    continue

                filename, row_count, partial = result
                upsert_manifest({
                    "relation":      ELECTION_HISTORY_RELATION,
                    "year":          year,
                    "filename":      filename,
                    "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                    "row_count":     row_count,
                    # The year itself when truncated, empty otherwise — see
                    # MANIFEST_COLS for why the column keeps its plural name.
                    "partial_years": str(year) if partial else "",
                })
                files_ok += 1
                time.sleep(0.5)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} errors")
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

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
        # Chrome's scratch download dir is transient — remove it if the download
        # handler already reclaimed everything it put there.
        try:
            DOWNLOAD_DIR.rmdir()
        except OSError:
            pass   # non-empty (a failed download left a part file) or absent


# ================================ CLI =================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download South Carolina campaign finance data from ethicsfiling.sc.gov."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all years in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); wipes manifest for range")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, <= current year)")

    ap.add_argument("--transactions", action="store_true", help="contributions + expenditures only")
    ap.add_argument("--entities",     action="store_true",
                    help="filed reports + election history only")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="same as --entities — SC has no separate candidate registry")
    ap.add_argument("--committees",    action="store_true",
                    help="same as --entities — candidate committees only. For "
                         "standalone PACs see --pacs, a different source entirely")
    ap.add_argument("--pacs", action="store_true",
                    help="Non-Candidate committees (PACs) from apps.sc.gov — a "
                         "separate site from the rest of this scraper. NOT part "
                         "of the default (no-flag) run: a full sweep is a "
                         "~17,576-query brute-force name search plus a filing "
                         "walk over every committee found. Plain requests, no "
                         "Chrome needed. Ask for it explicitly")
    ap.add_argument("--party-caucus", action="store_true",
                    help="Caucus + State/County/City Political Party committees "
                         "from apps.sc.gov — same site as --pacs, dropdown-driven "
                         "instead of name search. NOT part of the default "
                         "(no-flag) run: County/City Party is a ~3,200-combo "
                         "sweep (46 counties + 269 cities, each x 11 parties). "
                         "Campaign Disclosure filings only — Operating Disclosure "
                         "is a different report format, not yet parsed anywhere "
                         "in this pipeline. Plain requests, no Chrome needed. "
                         "Ask for it explicitly")
    ap.add_argument("--party-refresh-days", type=int, metavar="N",
                    default=PARTY_REFRESH_DAYS,
                    help=f"--party-caucus: recheck a combo once its manifest "
                         f"entry is older than this many days, instead of "
                         f"skipping it forever (default {PARTY_REFRESH_DAYS}, "
                         f"tied to SC's ~91-day quarterly filing cycle). "
                         f"Use --force for an unconditional full reset instead")
    ap.add_argument("--ballot-measure", action="store_true",
                    help="Ballot Measure committees from apps.sc.gov — same "
                         "site as --pacs, same name-search pattern (not "
                         "dropdown-driven). NOT part of the default (no-flag) "
                         "run: a full sweep is a ~17,576-query brute-force "
                         "name search plus a filing walk over every committee "
                         "found. Campaign Disclosure filings only — Statement "
                         "of Organization filings are detected and skipped, "
                         "not parsed. Plain requests, no Chrome needed. Ask "
                         "for it explicitly")

    ap.add_argument("--rediscover", action="store_true",
                    help="ignore the cached API recipe and re-derive it from the UI")
    ap.add_argument("--headless",   action="store_true",
                    help="run Chrome headless (default is visible — the portal "
                         "has not been confirmed to search correctly headless)")
    ap.add_argument("--headed",     action="store_true",
                    help="run Chrome visibly (useful when discovery fails)")

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
            pacs=args.pacs,
            party_caucus=args.party_caucus,
            party_refresh_days=args.party_refresh_days,
            ballot_measure=args.ballot_measure,
            rediscover=args.rediscover,
            headed=args.headed,
            headless=args.headless,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
