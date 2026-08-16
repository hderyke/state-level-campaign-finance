"""
scrapers/vermont.py — Download Vermont campaign finance data.

Source: the Campaign Finance System of Vermont (https://campaignfinance.vermont.gov),
an Angular single-page app backed by a plain JSON API at
api.campaignfinance.vermont.gov. Pure HTTP — no Playwright, no browser.

Vermont runs the same vendor platform as Idaho (scrapers/idaho.py) and New
Hampshire (scrapers/new_hampshire.py): same `DownloadPublicGridData` /
`GetExportPublicDownloadData` controller pair, same `publicGridName` +
search-filter body shape, same "CSV export of a browse grid" concept. The
controller *prefix* differs per deployment (VT: PublicFilerDetails, ID:
ExportData, NH: PublicGridDownload), so the URLs below are Vermont's own.

Three request shapes, all confirmed from the user's own browser DevTools
(copy-as-cURL) rather than reverse-engineered:

  1. Entity rosters — POST /api/PublicFilerDetails/DownloadPublicGridData
        {"publicGridName": "CandidatePublicGrid",
         "candidateCommitteeSearchFilter": {... "filerTypeCode": "CAN" ...},
         "fileName": "Candidates", "type": "CSV", "openInNewTab": false}
     and the same with "CommitteePublicGrid" / "filerTypeCode": "COM".
     One flat snapshot each, no year parameter.

  2. Bulk transactions by filing year — POST /api/ExportData/GetExportPublicDownloadData
        {"transactionTypeCode": "TCON", "type": "CSV",
         "filingYear": "2014", "openInNewTab": false}
     This is what the Download Data page (/public/cf/downloads) calls.
     TCON = contributions and loans, TEXP = expenditures.

  3. Browse-grid transactions — POST /api/PublicFilerDetails/DownloadPublicGridData
        {"publicGridName": "ContributionsPublicGrid",
         "transactionDetailsSearchFilter": {... "fromDate": null,
             "toDate": null, "transactionAmountMin": null,
             "transactionAmountMax": null, "transactionTypeCode": "TCON" ...},
         "fileName": "Contributions", "type": "CSV", "openInNewTab": false}
     and the same with "ExpendituresPublicGrid" / "TEXP" / "Expenditures".
     This is the "Download Contribution Data" button on /public/cf/contribution.

Why both (2) and (3)
────────────────────
The Download Data page only publishes closed filing years — as of the
2026-08-12 snapshot it listed 2014 through 2025 and had no 2026 row, even
though the browse pages were already serving 2026 transactions (439,895
contributions / 98,311 expenditures total at that snapshot). So the bulk
export alone silently stops one year short of current.

This scraper therefore treats the bulk export as the preferred source and
the browse grid as the fallback: for each year in scope it tries the bulk
export first, and only if that year comes back empty or errors does it pull
the same year out of the browse grid. Years are never taken from both — when
a year later becomes available in bulk, its grid chunk files are deleted from
raw/ before the bulk file is written, so the parser can never double-count it.

The 50,000-row cap
──────────────────
Browse-grid downloads are capped at 50,000 rows, and the cap is enforced by
*refusal*, not truncation: ask for more than 50,000 rows and the site pops up
an error instead of producing a file. That's the good failure mode — there is
no such thing as a short CSV that looks complete — so the scraper can treat an
over-limit response as a signal rather than having to detect truncation after
the fact.

The grid path therefore uses the adaptive-window strategy from
scrapers/wisconsin.py, driven by the refusal: start from calendar months and
recursively split any window the server refuses (month → halves → … → single
day → amount bands via transactionAmountMin/Max) until every piece is small
enough to be served. Windows are disjoint by construction, so chunks
concatenate without deduplication. A row-count check against the cap is kept
as a secondary guard in case the site ever switches to silent truncation.

The refusal doubles as the probe signal for the date filter (below): a
one-month window that comes back refused is a window the server didn't apply,
because no single month of Vermont data is anywhere near 50,000 rows.

Verified vs. inferred
─────────────────────
VERIFIED (captured from the live site's own requests):
  - all three endpoint URLs and their exact request bodies
  - publicGridName values: CandidatePublicGrid, CommitteePublicGrid,
    ContributionsPublicGrid, ExpendituresPublicGrid
  - the filter property names (candidateCommitteeSearchFilter /
    transactionDetailsSearchFilter) and every key inside them
  - transactionTypeCode TCON on the bulk export

INFERRED (documented, and self-checked at runtime where possible):
  - transactionTypeCode TEXP on the bulk export. The capture only covered
    TCON; TEXP is the value the same platform uses for the Expenditures row
    in both Idaho and New Hampshire, and the Download Data page has an
    Expenditures row driven by the identical handler. If it were wrong the
    year would come back empty and fall through to the grid path, which is
    fully verified for expenditures — so a bad guess degrades, not breaks.
  - the wire format of fromDate/toDate. The capture has them as null, so no
    example of a populated value exists. DATE_FORMATS below lists the
    plausible encodings and _probe_date_format() determines which one the
    server actually honors by requesting a single-month window and checking
    the result two ways: the request must not be refused for exceeding the
    50,000-row cap (a month of Vermont data is a few thousand rows, so a
    refusal means the filter was dropped and the whole table was matched),
    and every returned row's transaction date must fall inside the month. The
    winner is cached to data/Vermont/grid_probe.json; delete that file to
    re-probe.
  - electionID. The captured body pins it to "50" (whatever cycle the UI had
    selected). Sending that verbatim would silently scope every windowed pull
    to one election cycle, so this scraper sends "" instead — the same
    no-filter value every other string key in that filter object uses. Set
    VT_ELECTION_ID in the environment to override.

Access notes
────────────
  - No authentication, no cookies, no CSRF token. Plain `requests` with a
    browser User-Agent is sufficient; unlike New Hampshire there is no Akamai
    TLS-fingerprint block, so curl_cffi is not needed.
  - TLS: `requests` validates against certifi's CA bundle, which does not
    include corporate roots. On a network that intercepts TLS (Zscaler,
    Netskope, most corporate laptops) every request fails with
    CERTIFICATE_VERIFY_FAILED "unable to get local issuer certificate", because
    the cert being presented is the proxy's, signed by a root only the OS trust
    store knows about. _resolve_verify() below handles this with no extra
    dependency: on Windows it builds a CA bundle from the OS certificate store
    via stdlib ssl.enum_certificates(), and it honours VT_CA_BUNDLE /
    REQUESTS_CA_BUNDLE / VT_INSECURE as explicit overrides.
  - Responses are checked for CSV shape, so an error page or JSON fault is
    treated as a failed download rather than written to raw/ as data.
  - A full 2014-present backfill is roughly 30 bulk requests plus however many
    windows the open year needs (typically 12-30). Incremental runs re-fetch
    only the current year.
"""

import csv
import io
import json
import os
import re
import ssl
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from urllib.parse import quote

import requests
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# Make project root importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from config import USER_AGENT

# =============================== paths ================================
RAW_DIR    = PROJECT_ROOT / "data" / "Vermont" / "raw"
MANIFEST   = PROJECT_ROOT / "data" / "Vermont" / "manifest.csv"
PROBE_FILE = PROJECT_ROOT / "data" / "Vermont" / "grid_probe.json"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = [
    "relation_type",    # contributions | expenditures | candidates | committees
    "source",           # bulk | grid | roster
    "year",             # filing year; blank for the entity rosters
    "window_from",      # inclusive start (YYYY-MM-DD); blank for bulk/roster
    "window_to",        # inclusive end   (YYYY-MM-DD); blank for bulk/roster
    "amount_from",      # amount band lower bound, blank when unused
    "amount_to",        # amount band upper bound, blank when unused
    "filename",
    "downloaded_at",
    "row_count",
    "truncated",        # "1" when the chunk still hit the row cap and could
                        # not be split further — a known-incomplete window
]

# ========================= source constants ===========================
SITE_ORIGIN = "https://campaignfinance.vermont.gov"
API_BASE    = "https://api.campaignfinance.vermont.gov/api"

# Browse-grid CSV export — entity rosters AND the per-page transaction
# downloads both go through this one controller.
GRID_URL = f"{API_BASE}/PublicFilerDetails/DownloadPublicGridData"

# Download Data page — bulk export of a whole filing year.
BULK_URL = f"{API_BASE}/ExportData/GetExportPublicDownloadData"

# Earliest filing year offered on the Download Data page (2026-08-12 snapshot
# listed 2014-2025 for both Contributions and Expenditures). Vermont's current
# system does not publish anything before this; pass --start-year to probe
# further back if the state ever backfills.
MIN_YEAR = 2014

# Server-side limit on browse-grid CSV downloads. Enforced by refusal: a
# request matching more than this many rows returns an error instead of a
# file, so the number is a splitting threshold rather than a truncation point.
# Also used as a secondary guard on the row count of responses that DO arrive.
ROW_CAP = 50_000

# Body fragments that mark a response as "too many rows", as opposed to a
# genuine fault. Matched case-insensitively against the response text, which
# for this API is a JSON fault object rather than the browser's popup string —
# so the patterns stay loose deliberately, and the row threshold itself
# (50000 / 50,000) is the most reliable of them.
OVER_LIMIT_PATTERNS = (
    "50,000", "50000",
    "exceed", "too many", "too large", "maximum number of records",
    "narrow your search", "refine your search", "row limit",
)

# Politeness delay between requests — each call runs a live query server-side.
REQUEST_SLEEP = 0.4

# Retry schedule for 429 / 5xx / transport errors.
RETRY_WAITS = (5, 20, 60)

# Amount bands, used only as a last resort when a single calendar day is over
# the row cap and cannot be split by date any further. Bounds are inclusive and
# chosen not to overlap at cent precision, so their union stays disjoint. The
# leading (None, 24.99) band catches zero, blank and negative (refund) amounts.
AMOUNT_BANDS: list[tuple[float | None, float | None]] = [
    (None,      24.99),
    (25.00,     99.99),
    (100.00,    249.99),
    (250.00,    999.99),
    (1000.00,   9_999.99),
    (10_000.00, None),
]

# Candidate encodings for fromDate/toDate — see "Verified vs. inferred" in the
# module docstring. Tried in order; the first one the server demonstrably
# honors wins and is cached to grid_probe.json.
DATE_FORMATS = [
    "%Y-%m-%dT00:00:00.000Z",   # Angular's default Date.toJSON()
    "%Y-%m-%d",                 # plain ISO date
    "%m/%d/%Y",                 # US display format, as rendered in the grid
    "%Y-%m-%dT00:00:00",        # ISO datetime, no zone
]

# Election-cycle filter. The captured request pins this to "50"; sending that
# verbatim would scope every pull to one cycle. "" is the no-filter value used
# by every other string key in the same filter object. Override if needed.
ELECTION_ID = os.environ.get("VT_ELECTION_ID", "")

# ---------------------------------------------------------------------
# Transaction groups. Each entry drives BOTH acquisition paths for one
# relation: the bulk export (transactionTypeCode) and the browse grid
# (publicGridName + fileName).
#
#   relation      manifest relation_type and raw-filename stem
#   txn_code      transactionTypeCode for both endpoints
#   grid_name     publicGridName for the browse-grid download
#   file_name     the fileName the site itself sends (cosmetic server-side,
#                 sent verbatim so the request matches the real one)
# ---------------------------------------------------------------------
TRANSACTION_GROUPS = [
    {"relation": "contributions", "txn_code": "TCON",
     "grid_name": "ContributionsPublicGrid", "file_name": "Contributions"},
    {"relation": "expenditures",  "txn_code": "TEXP",
     "grid_name": "ExpendituresPublicGrid",  "file_name": "Expenditures"},
]

# ---------------------------------------------------------------------
# Entity roster filters — copied verbatim from the site's own requests.
#
# accountStatus "FACT" scopes both rosters to active filers, which is what
# the public search page itself sends. It does mean the rosters are a
# *current* snapshot rather than a historical one: a committee that wound up
# and deregistered in, say, 2018 will not appear here. Those filers are
# reconstructed from the transaction files by parsers/vermont.py instead —
# the same roster-plus-backfill split New Hampshire and Idaho use. Widening
# this would mean guessing at other status codes, which the capture doesn't
# cover, so the verified value is sent unchanged.
# ---------------------------------------------------------------------
CANDIDATE_FILTER = {
    "pageNumber": 1,
    "pageSize": 10,          # ignored server-side when type == "CSV" (the
                             # export returns every matching row, not a page)
    "filerTypeCode": "CAN",
    "filerName": "",
    "politicalPartyCode": "",
    "officeSought": "",
    "officeType": "",
    "town": "",
    "election": "",
    "electionYear": None,
    "filingYear": None,
    "totalRaisedMax": None,
    "totalRaisedMin": None,
    "totalSpentMax": None,
    "totalSpentMin": None,
    "accountStatus": "FACT",
    "isUnderThreshold": None,
    "transactionSourceTypeCode": None,
    "treasurerName": None,
}

COMMITTEE_FILTER = {
    "pageNumber": 1,
    "pageSize": 10,
    "filerTypeCode": "COM",
    "filerName": "",
    "committeeType": "",
    "treasurerName": "",
    "politicalPartyCode": "",
    "election": "",
    "totalRaisedMax": None,
    "filingYear": None,
    "totalRaisedMin": None,
    "totalSpentMax": None,
    "totalSpentMin": None,
    "committeeSubType": "",
    "accountStatus": "FACT",
    "publicQuestion": "",
    "stance": "",
}

# ------------------------- elections archive (party/district) -------------
#
# Vermont's campaign finance system records NO party and NO district — the
# candidate search exposes neither as a filter nor as a column, and the roster
# export has neither. (`politicalPartyCode` does appear in the search filter
# payload, but that is vendor boilerplate: the same key exists in Idaho's
# filter, where it IS populated. Vermont leaves it empty.) Party in Vermont is
# a ballot-line fact held by the Elections Division, not by the CF system —
# there is no voter party registration in the state at all.
#
# The VT Elections Database (electionarchive.vermont.gov) has both. It runs on
# the ElectionStats platform — the same software behind canvass.sos.idaho.gov,
# which scrapers/idaho.py already reads — and its "Download" button is a plain
# <a download href> to a CSV endpoint taking one URL-encoded JSON `search`
# object. No auth, no POST, no cookies.
#
# Host policy: the .gov front end is tried first and the vendor backend only as
# a fallback, mirroring the Idaho decision to drop id.electionstats.com in
# favour of the state's own host for the identical platform. Whichever host
# answers is recorded in the manifest's `source` column so a run can be audited
# after the fact.
ARCHIVE_HOSTS = [
    ("gov",    "https://electionarchive.vermont.gov"),
    ("vendor", "https://vt2.elstats.civera.com"),
]
ARCHIVE_PATH     = "/api/download_search.csv"
ARCHIVE_FILENAME = "elections_archive.csv"          # legacy single-file name

# The archive response is town-level: one row per candidate per municipality,
# so a 2014-2026 request is ~200 MB. Requesting it in one go put ~200 MB
# through memory before a single byte reached disk, which behind a scanning
# corporate proxy looks indistinguishable from a hang — and an interrupted run
# left a partial .part file and had to start over.
#
# Batching the year range fixes both: each request is a few tens of MB, and a
# completed batch is recorded in the manifest, so an interrupted run resumes at
# batch granularity instead of restarting.
ARCHIVE_BATCH_YEARS = 4

# Read timeout is per-chunk once streaming, not for the whole transfer, so a
# genuinely stalled connection fails in minutes rather than hanging for the
# full previous 600s budget.
ARCHIVE_TIMEOUT = (30, 180)     # (connect, read-between-chunks)
ARCHIVE_CHUNK   = 1 << 20       # 1 MiB

# The search object the site's own Download button sends, with only the year
# range varied. Empty arrays mean "no filter"; voterStats=false keeps the
# response to contest results rather than turnout tables.
def _archive_search(year_from: int, year_to: int) -> str:
    payload = {
        "global": {"years": {"from": year_from, "to": year_to}},
        "ballotQuestions": {"text": "", "types": [], "number": "", "divisions": []},
        "contests": {"candidates": [], "divisions": [], "offices": []},
        "specialElectionsOnly": False,
        "voterStats": False,
        "stages": [],
    }
    return json.dumps(payload, separators=(",", ":"))


# ------------------------- Open States (second-tier party) ----------------
#
# The nightly CC0 bulk CSV of currently-serving legislators, the same source
# and the same column translation scrapers/texas.py already uses. No API key.
#
# Deliberately a *fallback*, not a primary: it covers only legislators serving
# right now (~180 in Vermont) and carries no history, so nearly everyone in it
# also appears in the elections archive above. Its real value is the seat the
# archive can't reach — a sitting legislator running again in the open year,
# whose current-cycle candidacy postdates the archive's last election.
OPENSTATES_URL      = "https://data.openstates.org/people/current/vt.csv"
OPENSTATES_FILENAME = "OpenStates_People.csv"
OPENSTATES_COLS     = ["openstates_id", "name", "given_name", "family_name",
                       "party", "chamber", "district"]
OPENSTATES_CHAMBERS = {"upper": "State Senator", "lower": "State Representative"}


ENTITY_GROUPS = [
    {"relation": "candidates", "grid_name": "CandidatePublicGrid",
     "file_name": "Candidates", "filter": CANDIDATE_FILTER},
    {"relation": "committees", "grid_name": "CommitteePublicGrid",
     "file_name": "Committees", "filter": COMMITTEE_FILTER},
]


# ========================= manifest helpers ==========================

def _manifest_key(relation: str, year: str, w_from: str, w_to: str,
                  a_from: str, a_to: str) -> str:
    """Stable identity for one downloaded file or chunk."""
    return "|".join((relation, year, w_from, w_to, a_from, a_to))


def _row_key(r: dict) -> str:
    return _manifest_key(r.get("relation_type", ""), r.get("year", ""),
                         r.get("window_from", ""), r.get("window_to", ""),
                         r.get("amount_from", ""), r.get("amount_to", ""))


def load_manifest() -> dict[str, dict]:
    """Return {key: row} for everything already downloaded."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {_row_key(r): r for r in csv.DictReader(f)}


def _write_manifest(rows: list[dict]) -> None:
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore",
                           restval="")
        w.writeheader()
        w.writerows(rows)


def upsert_manifest(record: dict) -> None:
    """Write or overwrite a single manifest row, keyed on chunk identity."""
    rows: list[dict] = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    key = _row_key(record)
    rows = [r for r in rows if _row_key(r) != key]
    rows.append(record)
    _write_manifest(rows)


def strip_manifest(keep) -> None:
    """Rewrite the manifest keeping only rows for which keep(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if keep(r)]
    _write_manifest(rows)


def _drop_files(predicate) -> None:
    """
    Delete raw files whose manifest row matches predicate(row), and drop those
    rows from the manifest.

    Stale chunk files matter more here than for a year-per-file state: a window
    that gets re-split produces different filenames, and a year that graduates
    from the grid to the bulk export leaves its old chunks behind. Either way
    the orphaned file would be counted again at parse time.
    """
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        if not predicate(r):
            continue
        stale = RAW_DIR / (r.get("filename") or "")
        if stale.name and stale.exists():
            stale.unlink()
    _write_manifest([r for r in rows if not predicate(r)])


# ========================== http helpers =============================

class BadResponse(Exception):
    """
    The server answered, but not with usable CSV — an HTTP error status, or a
    body that isn't a CSV export.

    Its own class rather than a RuntimeError on purpose. RecursionError is a
    RuntimeError subclass, so a tuple containing RuntimeError silently absorbs
    a broken ssl stack as though it were a missing filing year — which is
    exactly how one monkeypatch turned into ~260 doomed requests across 13
    years before the real cause was visible.
    """


class TransportError(Exception):
    """
    The host could not be reached at all — DNS, TLS, connection or timeout.

    Kept distinct from an HTTP-level failure on purpose. An HTTP error against
    one filing year means "this year isn't published, try the grid"; a
    transport error means nothing about that year and everything about the
    connection, so it must abort the run instead of quietly demoting every
    year to a grid fallback that is about to fail the same way.
    """


# Extended Key Usage OID for TLS server authentication. ssl.enum_certificates()
# reports per-certificate trust as either True (trusted for everything) or a set
# of purpose OIDs; only certs good for server auth belong in a bundle used to
# validate an HTTPS server.
_SERVER_AUTH_OID = "1.3.6.1.5.5.7.3.1"

# Where the merged trust bundle is written. Under data/, which .gitignore
# already excludes — worth keeping out of version control since it fingerprints
# the machine's corporate network.
#
# Rebuilt on every run rather than cached. It costs a few hundred milliseconds,
# and its inputs (four environment variables plus the OS store) can change
# between runs, so a cache would mostly serve to hide a newly-installed
# corporate root until some timer expired.
_OS_BUNDLE_PATH = PROJECT_ROOT / "data" / "Vermont" / "os_ca_bundle.crt"


# Environment variables that conventionally point at a CA bundle. These are
# treated as ADDITIONAL inputs, never as replacements — see _build_trust_bundle.
_CA_BUNDLE_VARS = ("VT_CA_BUNDLE", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE",
                   "CURL_CA_BUNDLE")


def _windows_root_pems(log) -> list[str]:
    """
    Every TLS-server-auth certificate in the Windows trust store, as PEM.

    `ssl.enum_certificates()` is Windows-only stdlib and reads the same store
    the OS and every browser trust — which is where a corporate TLS proxy's
    root certificate lives. Empty list on any other platform.
    """
    if not hasattr(ssl, "enum_certificates"):
        return []
    pems, seen = [], set()
    # ROOT holds trusted roots; CA holds intermediates, which a proxy sometimes
    # needs to complete its chain.
    for store in ("ROOT", "CA"):
        try:
            certs = ssl.enum_certificates(store)
        except Exception as e:      # store missing or access denied
            log.info(f"  TLS: could not read the Windows '{store}' store ({e})")
            continue
        for der, encoding, trust in certs:
            if encoding != "x509_asn" or der in seen:
                continue
            # trust is True (all purposes) or a set of EKU OIDs.
            if trust is not True and not (
                isinstance(trust, (set, frozenset)) and _SERVER_AUTH_OID in trust
            ):
                continue
            seen.add(der)
            try:
                pems.append(ssl.DER_cert_to_PEM_cert(der))
            except Exception:
                continue           # skip a malformed entry, keep the rest
    return pems


def _build_trust_bundle(log) -> str | None:
    """
    Concatenate every CA source available into one bundle and return its path.

    Deliberately a UNION, not a choice. An earlier version picked the highest
    priority source and stopped, which broke under Anaconda: Anaconda sets
    SSL_CERT_FILE to its own certifi copy, that outranked the Windows trust
    store, and so the one source that actually contained the corporate root was
    never consulted. Any "pick one" ordering has that failure mode, because
    these variables are frequently set ambiently by a Python distribution
    rather than deliberately by the user.

    Inputs, all merged:
      - certifi's bundle (public CAs)
      - whatever VT_CA_BUNDLE / SSL_CERT_FILE / REQUESTS_CA_BUNDLE /
        CURL_CA_BUNDLE point at, if they exist
      - the Windows trust store, which is where a corporate proxy root lives

    Returns None when there is nothing to add beyond certifi, in which case the
    caller just uses requests' default.
    """
    inputs: list[tuple[str, str]] = []          # (label, PEM text)

    try:
        import certifi
        inputs.append(("certifi", Path(certifi.where()).read_text(encoding="utf-8")))
    except Exception:
        pass

    for var in _CA_BUNDLE_VARS:
        val = os.environ.get(var, "").strip()
        if not val:
            continue
        path = Path(val)
        if not path.exists():
            # Only hard-fail on the one set deliberately for this scraper; a
            # stale global shouldn't stop the run.
            if var == "VT_CA_BUNDLE":
                raise RuntimeError(f"VT_CA_BUNDLE points at a file that does "
                                   f"not exist: {val}")
            log.warning(f"  TLS: ignoring {var} — no such file: {val}")
            continue
        try:
            inputs.append((var, path.read_text(encoding="utf-8", errors="replace")))
        except OSError as e:
            log.warning(f"  TLS: could not read {var} ({e})")

    os_pems = _windows_root_pems(log)
    if os_pems:
        inputs.append(("windows trust store", "\n".join(os_pems)))

    # Nothing beyond certifi to contribute → let requests use its default.
    if not any(label != "certifi" for label, _ in inputs):
        return None

    # Dedupe on the base64 body so the same root arriving from three sources is
    # written once.
    blocks: list[str] = []
    seen: set[str] = set()
    counts: dict[str, int] = {}
    for label, text in inputs:
        n = 0
        for m in re.finditer(
            r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", text, re.S
        ):
            body = "".join(m.group(0).split())
            if body in seen:
                continue
            seen.add(body)
            blocks.append(m.group(0))
            n += 1
        counts[label] = n

    if not blocks:
        return None

    try:
        _OS_BUNDLE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _OS_BUNDLE_PATH.with_suffix(".crt.part")
        tmp.write_text("\n".join(blocks) + "\n", encoding="utf-8")
        tmp.replace(_OS_BUNDLE_PATH)
    except OSError as e:
        log.info(f"  TLS: could not write the merged trust bundle ({e})")
        return None

    summary = ", ".join(f"{label} +{n}" for label, n in counts.items() if n)
    log.info(f"  TLS: merged CA bundle — {len(blocks)} certificates "
             f"({summary}) → {_OS_BUNDLE_PATH.name}")
    return str(_OS_BUNDLE_PATH)


def _resolve_verify(log) -> bool | str:
    """
    Decide what to pass as `session.verify`, and say so in the log.

    `requests` validates against certifi's bundle, which contains public CAs
    only. That is the right default on a normal connection and the wrong one
    behind a TLS-intercepting proxy, where the certificate actually presented
    is signed by a corporate root that lives in the OS trust store and nowhere
    else.

    Only one thing here is a genuine override:

      VT_INSECURE=1   skip verification entirely. Last resort — it also removes
                      protection against a real MITM, so it warns loudly and is
                      never the default.

    Everything else is MERGED rather than ranked — certifi, the Windows trust
    store, and whatever VT_CA_BUNDLE / SSL_CERT_FILE / REQUESTS_CA_BUNDLE /
    CURL_CA_BUNDLE point at all end up in one bundle. See _build_trust_bundle()
    for why ranking them was a bug.

    Note on truststore: an earlier version called truststore.inject_into_ssl(),
    which replaces ssl.SSLContext process-wide. That recursed without bound
    against urllib3's own context handling ("maximum recursion depth exceeded"
    on every request), so global patching is gone. truststore can still be used,
    opt-in and scoped to this session only, via VT_USE_TRUSTSTORE=1 — see
    _TruststoreAdapter.
    """
    if os.environ.get("VT_INSECURE", "").strip().lower() in ("1", "true", "yes"):
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        log.warning("VT_INSECURE is set — TLS certificate verification is "
                    "DISABLED. Traffic can be read and modified in transit; "
                    "prefer VT_CA_BUNDLE or the OS trust store.")
        return False

    merged = _build_trust_bundle(log)
    if merged:
        return merged

    # certifi — correct on an unintercepted connection. If it turns out not to
    # be, _transport_error() explains the options.
    return True


def _transport_error(err: Exception) -> TransportError:
    """Wrap a connection failure with the fix, not just the stack trace."""
    msg = str(err)
    if "CERTIFICATE_VERIFY_FAILED" in msg or isinstance(err, requests.exceptions.SSLError):
        on_windows = hasattr(ssl, "enum_certificates")
        auto = ("The Windows trust store is read automatically and still "
                "didn't contain a matching root, so the proxy's certificate "
                "may be issued per-session or the store may be unreadable "
                "from this process.\n"
                if on_windows else
                "This platform has no OS certificate store Python can read "
                "directly (ssl.enum_certificates is Windows-only).\n")
        return TransportError(
            "TLS certificate verification failed against "
            "api.campaignfinance.vermont.gov. This almost always means the "
            "connection is being intercepted by a corporate TLS proxy whose "
            "root certificate is in the OS trust store but not in certifi's "
            "bundle.\n"
            + auto +
            "If you are on Anaconda, note it sets SSL_CERT_FILE to its own "
            "public-CA bundle; that is merged in rather than used alone, so "
            "this is not the cause on its own.\n"
            "If the error mentions 'Basic Constraints', 'not marked critical' "
            "or 'inconsistent certificate extension', the chain is actually "
            "fine and only OpenSSL's strict RFC 5280 checks reject it "
            "(Python 3.13 enables those by default). The scraper retries that "
            "case automatically; VT_RELAX_X509_STRICT=1 forces it.\n"
            "Options, no install required:\n"
            "    set VT_CA_BUNDLE=C:\\path\\to\\corporate-root.crt   "
            "# export the root from certmgr.msc\n"
            "    set VT_INSECURE=1                                 "
            "# last resort, disables verification\n"
            "Or, if you'd rather add a dependency:\n"
            "    pip install truststore\n"
            f"Original error: {msg}"
        )
    return TransportError(
        f"could not reach api.campaignfinance.vermont.gov: {msg}"
    )


# OpenSSL errors that ONLY occur under X509_V_FLAG_X509_STRICT. Each is a
# structural nicety from RFC 5280 that corporate/internal CAs routinely get
# wrong, and that OpenSSL ignores unless strict checking is switched on.
# Python 3.13 switched it on by default in ssl.create_default_context(), which
# is why the same machine can verify fine on 3.12 and fail on 3.13.
_X509_STRICT_ONLY_ERRORS = (
    "basic constraints of ca cert not marked critical",
    "invalid or inconsistent certificate extension",
    "authority and subject key identifier mismatch",
    "certificate extensions are invalid",
)


def _is_strict_only_failure(err: Exception) -> bool:
    """True when a TLS failure is one of the strict-mode-only structural checks."""
    msg = str(err).lower()
    return any(p in msg for p in _X509_STRICT_ONLY_ERRORS)


class _RelaxedStrictAdapter(requests.adapters.HTTPAdapter):
    """
    Verify normally, minus OpenSSL's strict RFC 5280 extension checks.

    This is NOT "skip verification". The certificate chain, expiry, revocation
    settings and hostname are all still checked exactly as usual. The single
    thing switched off is X509_V_FLAG_X509_STRICT, which additionally demands
    that CA certificates get their structural extensions exactly right —
    notably that basicConstraints is marked critical.

    Corporate and appliance-generated CAs frequently fail that check while
    being perfectly valid trust anchors, and OpenSSL ignored it for years.
    Python 3.13 began setting the flag by default, so a proxy root that worked
    under 3.12 suddenly fails under 3.13 with "Basic Constraints of CA cert not
    marked critical". Relaxing exactly that flag keeps every security property
    that actually protects the connection.
    """

    def __init__(self, cafile: str | None, *args, **kwargs):
        self._cafile = cafile
        super().__init__(*args, **kwargs)

    def _build_context(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context(cafile=self._cafile)
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
        return ctx

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._build_context()
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["ssl_context"] = self._build_context()
        return super().proxy_manager_for(*args, **kwargs)


class _TruststoreAdapter(requests.adapters.HTTPAdapter):
    """
    Opt-in adapter that validates through truststore's SSLContext.

    Deliberately scoped: the context is handed to this session's pool manager
    and nothing else in the process is touched. truststore's own
    inject_into_ssl() helper replaces ssl.SSLContext globally, which recurses
    without bound against urllib3's context handling here — so if truststore is
    wanted, this is how it gets used.
    """

    def init_poolmanager(self, *args, **kwargs):
        import truststore
        kwargs["ssl_context"] = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        import truststore
        kwargs["ssl_context"] = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        return super().proxy_manager_for(*args, **kwargs)


def _make_session(log, relax_strict: bool = False) -> requests.Session:
    """
    Session carrying the same headers the site's own XHRs send.

    `relax_strict` mounts _RelaxedStrictAdapter, dropping OpenSSL's strict
    RFC 5280 extension checks while keeping chain, expiry and hostname
    verification. run() sets it after a preflight failure identifies one of
    those strict-only errors, or immediately when VT_RELAX_X509_STRICT is set.
    """
    s = requests.Session()

    if not relax_strict and os.environ.get(
            "VT_RELAX_X509_STRICT", "").strip().lower() in ("1", "true", "yes"):
        relax_strict = True
        log.info("  TLS: VT_RELAX_X509_STRICT is set")

    used_truststore = False
    if os.environ.get("VT_USE_TRUSTSTORE", "").strip().lower() in ("1", "true", "yes"):
        try:
            import truststore  # noqa: F401  — presence check only
            s.mount("https://", _TruststoreAdapter())
            log.info("  TLS: using truststore via a session-scoped adapter")
            used_truststore = True
        except ImportError:
            log.warning("  VT_USE_TRUSTSTORE is set but truststore isn't "
                        "installed — falling back to bundle resolution")

    if not used_truststore:
        verify = _resolve_verify(log)
        if relax_strict and verify is not False:
            cafile = verify if isinstance(verify, str) else None
            s.mount("https://", _RelaxedStrictAdapter(cafile))
            log.warning(
                "  TLS: OpenSSL's strict RFC 5280 extension checks are "
                "disabled for this run. Chain, expiry and hostname are still "
                "verified in full - only the structural CA-extension checks "
                "that Python 3.13 turned on by default are relaxed, because "
                "the intercepting proxy's root fails one of them."
            )
        else:
            s.verify = verify

    s.headers.update({
        "User-Agent":      USER_AGENT,
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type":    "application/json",
        "Origin":          SITE_ORIGIN,
        "Referer":         f"{SITE_ORIGIN}/",
        "sec-fetch-site":  "same-site",
        "sec-fetch-mode":  "cors",
        "sec-fetch-dest":  "empty",
    })
    return s


# The only failures that legitimately mean "the server answered, and this year
# or window just isn't there". Everything outside this tuple — TransportError,
# RecursionError from a broken ssl stack, a plain bug — says nothing about the
# data and must stop the run instead of being absorbed as a fallback.
EXPECTED_HTTP_ERRORS = (requests.RequestException, BadResponse)


class GridLimitExceeded(Exception):
    """
    The browse grid refused a request for matching more than ROW_CAP rows.

    Distinct from a transport or server fault: this one is expected, carries
    information (the window is too wide), and is answered by splitting the
    window rather than by retrying or failing the run.
    """


def _is_over_limit(body: str, status: int) -> bool:
    """
    Does this response mean "too many rows" rather than "something broke"?

    The site surfaces the cap as a popup, so on the wire it's an error payload
    of some shape. Require both a client-error-ish status (or a non-CSV body)
    and one of the known phrases, so an unrelated 400 isn't mistaken for a
    window that needs splitting — a misread here would silently split a real
    failure into 30 identical failures.
    """
    if status >= 500:
        return False
    low = body[:2000].lower()
    return any(p in low for p in OVER_LIMIT_PATTERNS)


def _preflight(log, session: requests.Session) -> None:
    """
    Prove the HTTP stack works before committing to ~30 requests.

    One cheap GET of the public site. Any HTTP status counts as success — the
    point is to exercise DNS, TLS and the proxy path, not to check a response
    body. A failure here is environmental and aborts the run immediately with a
    single actionable error, instead of the same failure repeating once per
    filing year and again per probe window.

    RecursionError is caught explicitly: a monkeypatched ssl stack surfaces
    that way rather than as a requests exception, and it is exactly the class
    of problem this check exists to catch early.
    """
    try:
        resp = session.get(SITE_ORIGIN, timeout=30, headers={
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })
        log.info(f"  Preflight: GET {SITE_ORIGIN} → HTTP {resp.status_code}")
    except (requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as e:
        raise _transport_error(e) from e
    except RecursionError as e:
        raise TransportError(
            "The TLS/HTTP stack is recursing without bound "
            "(RecursionError), so no request can complete. This is caused by "
            "a monkeypatched ssl module — most often "
            "truststore.inject_into_ssl(), which this scraper no longer "
            "calls. If VT_USE_TRUSTSTORE is set, unset it; the Windows trust "
            "store is read directly via stdlib and needs no patching. If "
            "another library in the environment injects into ssl, run this "
            "scraper in a clean virtualenv.\n"
            f"Original error: {e}"
        ) from e
    except requests.RequestException as e:
        # Reachable but unhappy (proxy auth, redirect loop). Not fatal on its
        # own — the API is a different host path — so warn and continue.
        log.warning(f"  Preflight: {SITE_ORIGIN} returned {e} — continuing")


def _looks_like_csv(body: str) -> bool:
    """
    True when the response body is plausibly a CSV export rather than a JSON
    fault or an HTML error page.

    Deliberately loose: it only rules out the shapes an error takes. Vermont's
    grid exports lead with a title/timestamp line before the real header, so
    "first line contains a comma" is not a safe test on its own.
    """
    head = body.lstrip()[:400]
    if not head:
        return False
    if head[0] in "{[<":          # JSON object/array or HTML
        return False
    return True


def _post_csv(session: requests.Session, url: str, body: dict,
              timeout: int = 300, retry_5xx: bool = True) -> str:
    """
    POST a JSON body and return the CSV response text.

    Raises GridLimitExceeded when the grid refuses the request for matching
    more than 50,000 rows — the caller answers that by splitting the window,
    not by retrying, so it short-circuits the retry loop.

    Raises TransportError when the host can't be reached at all (DNS, TLS,
    connection, timeout) — the caller must not mistake that for "this year
    isn't published".

    Raises RuntimeError on any other non-CSV response. A JSON fault or error
    page must never land in raw/ where the parser would read it as data.
    """
    last_err: Exception | None = None
    transport_err: Exception | None = None
    for wait in (0,) + RETRY_WAITS:
        if wait:
            time.sleep(wait)
        try:
            resp = session.post(url, json=body, timeout=timeout)
            # The API serves UTF-8; requests sometimes guesses latin-1 from a
            # missing charset, which mangles names. Pin it.
            resp.encoding = resp.encoding or "utf-8"
            text = resp.text

            # Check the body before raise_for_status: the row-cap refusal
            # arrives as a 4xx with the explanation in the payload, and
            # raise_for_status would throw it away.
            if _is_over_limit(text, resp.status_code):
                raise GridLimitExceeded(text[:200])

            if resp.status_code == 429 or resp.status_code >= 500:
                last_err = BadResponse(f"HTTP {resp.status_code} from {url}")
                # A 5xx is usually transient and worth backing off for — but
                # Vermont also answers an EMPTY date window with 500, and that
                # is perfectly deterministic. Waiting 5+20+60s to re-confirm it
                # is pure cost, so callers that are about to ask
                # _five_hundred_means_empty() opt out of the backoff.
                if resp.status_code >= 500 and not retry_5xx:
                    raise last_err
                continue

            # 4xx (other than the 429 handled above) is deterministic: a 404
            # for an unpublished filing year will still be a 404 in 85 seconds.
            # Retrying it cost ~85s per year per relation before this check.
            if resp.status_code >= 400:
                raise BadResponse(f"HTTP {resp.status_code} from {url}")

            if not _looks_like_csv(text):
                raise BadResponse(
                    f"expected CSV, got Content-Type="
                    f"{resp.headers.get('Content-Type', '')!r} "
                    f"body={text[:160]!r}"
                )
            return text
        except (requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            # Host-level. Retrying can help a flaky connection, but a bad trust
            # chain will fail identically every time — so remember it and
            # report it as a transport failure once the retries are spent.
            last_err = transport_err = e
        except requests.RequestException as e:
            last_err = e

    if transport_err is not None:
        raise _transport_error(transport_err)
    raise BadResponse(
        f"request failed after {len(RETRY_WAITS) + 1} attempts: {last_err}"
    )


# ========================== CSV inspection ===========================

def _csv_rows(body: str) -> tuple[list[str], list[list[str]]]:
    """
    Split a downloaded CSV body into (header, data_rows).

    Grid exports lead with a one-line title/timestamp banner before the real
    header (e.g. "Contributions Download as of 2026-08-12 04:00 PM") — the same
    quirk Idaho's and New Hampshire's exports have. That line is dropped when
    present so the caller always sees the true header. Detection is on shape,
    not on the banner's exact wording, which varies per grid.

    newline="" on the StringIO matters: addresses are quoted and can span
    physical lines, so line counting would over-count and reader() would raise
    on the embedded CR/LF if they were normalized away.
    """
    reader = csv.reader(io.StringIO(body, newline=""))
    rows = [r for r in reader if r and any(c.strip() for c in r)]
    if not rows:
        return [], []
    # A banner line is a single populated cell (or one cell plus padding
    # commas) followed by a genuinely multi-column header.
    if len(rows) > 1 and len([c for c in rows[0] if c.strip()]) <= 1 \
            and len([c for c in rows[1] if c.strip()]) > 1:
        rows = rows[1:]
    return rows[0], rows[1:]


def _count_rows(body: str) -> int:
    """Number of data rows in a downloaded CSV, banner and header excluded."""
    _, data = _csv_rows(body)
    return len(data)


def _count_rows_path(path: Path) -> int:
    """
    Row count for a file on disk, read incrementally.

    The obvious `_count_rows(path.read_text())` would undo the point of
    streaming the download: it pulls the whole file — up to ~100 MB for one
    archive batch — back into memory just to count lines.
    """
    with open(path, encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f)
        try:
            first = next(reader)
        except StopIteration:
            return 0
        n = 0
        # Same banner rule as _csv_rows: a single populated cell followed by a
        # real multi-column header means row one is a title line.
        if len([c for c in first if c.strip()]) <= 1:
            try:
                next(reader)
            except StopIteration:
                return 0
        for row in reader:
            if row and any(c.strip() for c in row):
                n += 1
        return n


def _date_column(header: list[str]) -> int | None:
    """Index of the transaction-date column, or None if it can't be found."""
    norm = [(h or "").strip().lower() for h in header]
    for want in ("transaction date", "date of receipt", "transactiondate"):
        if want in norm:
            return norm.index(want)
    for i, h in enumerate(norm):
        if "date" in h and "report" not in h and "filed" not in h:
            return i
    return None


def _parse_any_date(val: str) -> date | None:
    """Parse a date cell in whichever format the export happens to use."""
    v = (val or "").strip()
    if not v:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(v[:19] if "T" in v else v, fmt).date()
        except ValueError:
            continue
    return None


# ====================== request body construction ====================

def _grid_body(group: dict, w_from: date | None, w_to: date | None,
               a_from: float | None, a_to: float | None,
               date_fmt: str) -> dict:
    """
    Build a browse-grid CSV export body for one transaction group.

    The filter is the captured one with only the window keys populated.
    Contributions and expenditures have slightly different filter objects on
    the wire (contributions carries sourceTypeCode/byState/officeType,
    expenditures carries publicQuestion/stance/isMassMedia/transactionCategory);
    both are sent with their own keys so each request matches the real one
    rather than a merged superset the server has never seen.
    """
    common = {
        "pageNumber": 1,
        "pageSize": 10,          # ignored when type == "CSV"
        "sortBy": "TransactionDate",
        "sortType": "desc",
        "transactionTypeCode": group["txn_code"],
        "filerName": "",
        "sourceName": "",
        "transactionAmountMax": a_to,
        "transactionAmountMin": a_from,
        "committeeType": "",
        "reportName": "",
        "toDate":   None if w_to   is None else w_to.strftime(date_fmt),
        "fromDate": None if w_from is None else w_from.strftime(date_fmt),
        "officeSought": "",
        "electionID": ELECTION_ID,
    }

    if group["txn_code"] == "TCON":
        filt = {
            **common,
            "sourceTypeCode": "",
            "transactionSubTypeCode": "",
            "byState": "",
            "electionYear": None,
            "filingYear": None,
            "officeType": "",
        }
    else:
        filt = {
            **common,
            "publicQuestion": "",
            "stance": "",
            "isMassMedia": None,
            "transactionCategory": "",
        }

    return {
        "publicGridName": group["grid_name"],
        "transactionDetailsSearchFilter": filt,
        "fileName": group["file_name"],
        "type": "CSV",
        "openInNewTab": False,
    }


def _bulk_body(group: dict, year: int) -> dict:
    """Download Data page export body for one (group, filing year)."""
    return {
        "transactionTypeCode": group["txn_code"],
        "type": "CSV",
        "filingYear": str(year),
        "openInNewTab": False,
    }


def _entity_body(group: dict) -> dict:
    """Entity roster export body — verbatim from the site's own request."""
    return {
        "publicGridName": group["grid_name"],
        "candidateCommitteeSearchFilter": group["filter"],
        "fileName": group["file_name"],
        "type": "CSV",
        "openInNewTab": False,
    }


# ====================== date-format probing ==========================

def PROBE_WINDOWS(year: int) -> list[tuple[date, date]]:
    """
    The narrowing ladder _probe_date_format walks for each candidate encoding.

    Month → week → three single days in different months. The widths let a
    refusal be read as "narrower, please" rather than as a verdict, and the
    spread across the calendar means a single unusually busy filing day (which
    could legitimately be over the cap) can't sink an encoding on its own.
    """
    return [
        (date(year, 3, 1),  date(year, 3, 31)),     # month
        (date(year, 3, 10), date(year, 3, 16)),     # week
        (date(year, 3, 12), date(year, 3, 12)),     # single days, spread out
        (date(year, 6, 18), date(year, 6, 18)),
        (date(year, 10, 7), date(year, 10, 7)),
    ]


def _load_probe() -> dict:
    if PROBE_FILE.exists():
        try:
            return json.loads(PROBE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_probe(data: dict) -> None:
    PROBE_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROBE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _window_is_honored(body: str, w_from: date, w_to: date) -> bool | None:
    """
    Did the server actually apply the requested window?

    Returns True (every dated row falls inside the window), False (at least one
    row falls outside — the filter was ignored or misread), or None (nothing to
    judge on: no rows, or no recognizable date column).
    """
    header, data = _csv_rows(body)
    if not data:
        return None
    idx = _date_column(header)
    if idx is None:
        return None
    seen = 0
    for row in data:
        if idx >= len(row):
            continue
        d = _parse_any_date(row[idx])
        if d is None:
            continue
        seen += 1
        if d < w_from or d > w_to:
            return False
    return True if seen else None


def _probe_date_format(log, session: requests.Session, group: dict,
                       year: int) -> str | None:
    """
    Work out which fromDate/toDate encoding the server honors.

    An encoding is accepted only when a bounded request comes back with rows
    and every dated row falls inside the window. That is the check that matters
    — a filter the server parses but applies to the wrong field would still
    return the wrong rows, and a row-count test alone wouldn't notice.

    The complication is that a request can also come back *refused* for
    exceeding the row cap, which is ambiguous on its own: either the filter was
    dropped and the whole table matched, or the window really does hold that
    many rows. So each encoding is tried against PROBE_WINDOWS, a ladder that
    narrows from a month to a week to single days in three different months. A
    refusal just means "try something narrower". Only when every width down to
    a single day is refused is the encoding judged to have been ignored —
    single days spread across the year cannot all legitimately be over the cap.

    Returns the winning strftime format, or None when the relation has no data
    at all (nothing to download, not a failure).

    Raises when the grid demonstrably HAS data but no encoding constrains it.
    That is the case worth stopping for: an ignored date filter makes every
    window request identical, so the scraper would otherwise either write a
    dozen copies of the same rows or fail every window with the same refusal.
    """
    cached = _load_probe().get("date_format")
    if cached in DATE_FORMATS:
        return cached

    rejected: list[str] = []
    for fmt in DATE_FORMATS:
        refusals = 0
        attempts = 0
        for w_from, w_to in PROBE_WINDOWS(year):
            attempts += 1
            try:
                body = _post_csv(session, GRID_URL,
                                 _grid_body(group, w_from, w_to, None, None, fmt))
            except GridLimitExceeded:
                refusals += 1
                continue          # too wide (or ignored) — try a narrower one
            except EXPECTED_HTTP_ERRORS as e:
                log.info(f"  probe {fmt!r} {w_from}→{w_to}: request failed ({e})")
                continue
            # TransportError and anything unexpected propagate: there is no
            # point walking the rest of the ladder when every remaining probe
            # would fail the same way and bury the real cause.
            time.sleep(REQUEST_SLEEP)

            verdict = _window_is_honored(body, w_from, w_to)
            if verdict is True:
                log.info(f"  Date filter format resolved: {fmt!r}")
                probe = _load_probe()
                probe.update({
                    "date_format":      fmt,
                    "election_id":      ELECTION_ID,
                    "verified_at":      date.today().isoformat(),
                    "verified_against": f"{group['relation']} {w_from}→{w_to}",
                })
                _save_probe(probe)
                return fmt
            if verdict is False:
                rejected.append(f"{fmt!r}: returned rows outside the window")
                break             # parsed but wrong — no width will fix that
            # verdict is None → empty window; try the next one down the ladder
        else:
            if refusals == attempts:
                rejected.append(f"{fmt!r}: refused at every width including "
                                f"single days — filter ignored")

    # Nothing was honored. One unfiltered request separates "this relation has
    # no data" from "the date filter is broken".
    try:
        total = _count_rows(_post_csv(
            session, GRID_URL,
            _grid_body(group, None, None, None, None, DATE_FORMATS[0])))
    except GridLimitExceeded:
        total = -1           # refused unfiltered → definitely has lots of data
    except EXPECTED_HTTP_ERRORS as e:
        raise RuntimeError(
            f"could not probe the {group['relation']} grid at all: {e}"
        ) from e

    if total == 0 and not rejected:
        log.warning(f"  {group['relation']}: grid returned no rows at all — "
                    f"nothing to download for {year}")
        return None

    holds = f"more than {ROW_CAP:,}" if total < 0 else f"{total:,}"
    raise RuntimeError(
        f"{group['relation']}: the grid holds {holds} rows but no candidate "
        f"encoding constrained a bounded {year} window "
        f"[{'; '.join(rejected) or 'every probe window came back empty'}]. "
        f"The fromDate/toDate encoding has changed. Capture the real request "
        f"from the browser's Network tab (the 'Download Contribution Data' "
        f"button with a date filter applied) and record the format in "
        f'{PROBE_FILE} as {{"date_format": "<strftime pattern>"}}.'
    )


# ========================= window generation =========================

def _month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """
    Disjoint calendar-month windows covering [start, end].

    No longer used to seed download_grid_year — that starts from the whole
    range and lets the row cap drive the splitting. Kept because it is the
    natural way to express a month-granular sweep and is used by the tests.
    """
    out: list[tuple[date, date]] = []
    cur = start.replace(day=1)
    while cur <= end:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        out.append((max(cur, start), min(nxt - timedelta(days=1), end)))
        cur = nxt
    return out


def _split_window(w_from: date, w_to: date) -> list[tuple[date, date]]:
    """Halve a multi-day window; returns [] for a single day (use amount bands)."""
    span = (w_to - w_from).days
    if span < 1:
        return []
    mid = w_from + timedelta(days=span // 2)
    return [(w_from, mid), (mid + timedelta(days=1), w_to)]


def _chunk_filename(relation: str, w_from: date, w_to: date,
                    a_from: float | None, a_to: float | None) -> str:
    name = f"grid_{relation}_{w_from.isoformat()}_{w_to.isoformat()}"
    if a_from is not None or a_to is not None:
        lo = "min" if a_from is None else f"{a_from:g}"
        hi = "max" if a_to   is None else f"{a_to:g}"
        name += f"_amt{lo}-{hi}"
    return name + ".csv"


def _bulk_filename(relation: str, year: int) -> str:
    return f"{relation}_{year}.csv"


# =========================== grid download ===========================

def _download_grid_chunk(log, session: requests.Session, group: dict,
                         w_from: date, w_to: date, date_fmt: str,
                         a_from: float | None = None, a_to: float | None = None,
                         keep_capped: bool = False,
                         retry_5xx: bool = True) -> tuple[str, int, bool]:
    """
    Download one grid window (optionally amount-banded) and write it to raw/.

    Propagates GridLimitExceeded when the window matches more than the cap —
    that's the normal "split me" signal, not an error.

    Returns (filename, row_count, capped). `capped` covers the secondary case:
    a response that did arrive but sits at exactly the row cap, which would
    mean the site had switched from refusing to truncating. The caller splits
    on that too.

    `keep_capped` writes such a chunk anyway, flagged truncated in the
    manifest — used when there is no split left to try.
    """
    relation = group["relation"]
    filename = _chunk_filename(relation, w_from, w_to, a_from, a_to)
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t_file = time.perf_counter()

    body = _post_csv(session, GRID_URL,
                     _grid_body(group, w_from, w_to, a_from, a_to, date_fmt),
                     retry_5xx=retry_5xx)
    rows   = _count_rows(body)
    capped = rows >= ROW_CAP

    if capped and not keep_capped:
        # Don't keep a chunk we know is short — the caller will split it. An
        # earlier run may have left a file here under this exact name.
        out_path.unlink(missing_ok=True)
        return filename, rows, True

    tmp = out_path.with_suffix(".csv.part")
    tmp.write_text(body, encoding="utf-8", newline="")
    tmp.replace(out_path)

    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=rows,
                         duration_s=round(time.perf_counter() - t_file, 2))
    upsert_manifest({
        "relation_type": relation,
        "source":        "grid",
        "year":          str(w_from.year),
        "window_from":   w_from.isoformat(),
        "window_to":     w_to.isoformat(),
        "amount_from":   "" if a_from is None else f"{a_from:g}",
        "amount_to":     "" if a_to   is None else f"{a_to:g}",
        "filename":      filename,
        "downloaded_at": date.today().isoformat(),
        "row_count":     str(rows),
        "truncated":     "1" if capped else "",
    })
    return filename, rows, capped


# Per-run memo: does a 5xx on this relation reliably mean "no rows matched"?
# Keyed by relation, resolved at most once per run by _five_hundred_means_empty.
_FIVE_HUNDRED_IS_EMPTY: dict[str, bool] = {}


def _five_hundred_means_empty(log, session: requests.Session, group: dict,
                              date_fmt: str) -> bool:
    """
    Decide whether an HTTP 500 from the grid means "this window is empty".

    Vermont returns 500 rather than an empty CSV when a date window matches no
    transactions — confirmed against the site itself, where the 2026-08-01 →
    08-12 range that 500s here shows no contributions at all in the UI.

    Reading every 500 as "empty" would be reckless on its own: if the site also
    500s when a window is too *large*, that reading would silently discard real
    data. So the two are separated with a positive control — one unfiltered
    request, which necessarily matches the entire table (440K+ contributions,
    far over the 50,000 cap):

      - it comes back as GridLimitExceeded → over-limit has its own distinct
        signal, so a 500 is NOT the over-limit case, and "empty" is the sound
        reading. Also proves the API is up.
      - it comes back as CSV → the whole relation is under the cap and the API
        is healthy; a 500 on a sub-window can't be about size either.
      - it 500s too → over-limit and empty are indistinguishable on this
        deployment, or the API is simply down. Refuse to guess; the caller
        keeps its old splitting behaviour and reports a real error.

    One request per relation per run, memoized.
    """
    relation = group["relation"]
    if relation in _FIVE_HUNDRED_IS_EMPTY:
        return _FIVE_HUNDRED_IS_EMPTY[relation]

    verdict = False
    try:
        _post_csv(session, GRID_URL,
                  _grid_body(group, None, None, None, None, date_fmt))
        verdict = True          # whole table served: healthy, and not a size issue
        why = "an unfiltered request returned data"
    except GridLimitExceeded:
        verdict = True          # over-limit is signalled distinctly from 5xx
        why = "an unfiltered request was refused for exceeding the row cap, "\
              "so over-limit is reported distinctly from a 5xx"
    except TransportError:
        raise
    except Exception as e:
        why = f"an unfiltered request also failed ({str(e)[:80]})"

    _FIVE_HUNDRED_IS_EMPTY[relation] = verdict
    log.info(f"  {relation}: treating HTTP 500 as "
             f"{'an empty window' if verdict else 'a real error'} — {why}")
    return verdict


def _record_empty_window(log, group: dict, w_from: date, w_to: date) -> None:
    """
    Note a window that the server reports as having no rows.

    No file is written — there is nothing to write, and an empty file would
    just be something for the parser to glob and skip. The manifest entry is
    what matters: it records that the window was checked and found empty, so a
    later run can tell "no data here" apart from "never fetched".
    """
    log.info(f"  {group['relation']} {w_from} → {w_to}: no transactions in "
             f"this range (server returns 500 for an empty result set)")
    upsert_manifest({
        "relation_type": group["relation"],
        "source":        "grid:empty",
        "year":          str(w_from.year),
        "window_from":   w_from.isoformat(),
        "window_to":     w_to.isoformat(),
        "amount_from":   "", "amount_to": "",
        "filename":      "",
        "downloaded_at": date.today().isoformat(),
        "row_count":     "0",
        "truncated":     "",
    })


def _download_amount_bands(log, session: requests.Session, group: dict,
                           day: date, date_fmt: str) -> tuple[int, int]:
    """
    Last-resort split for a single day over the row cap: slice it by amount.

    Returns (files_ok, files_err). A band the server still refuses cannot be
    split any further by this scraper, so it is counted as an error and logged
    loudly — a visible, recorded gap beats a table that is quietly short.
    """
    ok = err = 0
    for a_from, a_to in AMOUNT_BANDS:
        filename = _chunk_filename(group["relation"], day, day, a_from, a_to)
        try:
            _, _, capped = _download_grid_chunk(
                log, session, group, day, day, date_fmt, a_from, a_to,
                keep_capped=True, retry_5xx=False,
            )
            time.sleep(REQUEST_SLEEP)
            if capped:
                log.warning(
                    f"{filename}: at the {ROW_CAP:,}-row cap after amount "
                    f"banding — chunk may be incomplete (flagged truncated "
                    f"in the manifest)"
                )
            ok += 1
        except GridLimitExceeded:
            log.file_download_error(
                filename=filename,
                error=f"still over the {ROW_CAP:,}-row cap for a single day "
                      f"in a single amount band — no further split available; "
                      f"this slice of {day} is NOT downloaded",
            )
            err += 1
        except EXPECTED_HTTP_ERRORS as e:
            # An amount band with nothing in it 500s for the same reason a
            # date window does — most bands on any given day are empty.
            if "HTTP 5" in str(e) and _five_hundred_means_empty(
                    log, session, group, date_fmt):
                log.info(f"  {filename}: no transactions in this amount band")
                ok += 1
                continue
            log.file_download_error(filename=filename, error=str(e))
            err += 1
    return ok, err


def download_grid_year(log, session: requests.Session, group: dict, year: int,
                       done: dict[str, dict], refresh: bool) -> tuple[int, int]:
    """
    Pull one filing year of a transaction relation out of the browse grid,
    windowed to stay under the row cap.

    Returns (files_ok, files_err).
    """
    relation = group["relation"]
    try:
        date_fmt = _probe_date_format(log, session, group, year)
    except Exception as e:
        log.file_download_error(filename=f"grid_{relation}_{year}", error=str(e))
        raise
    if date_fmt is None:
        return 0, 0

    start = date(year, 1, 1)
    end   = min(date(year, 12, 31), date.today())

    # Seed with ONE window covering the whole year and let the cap split it,
    # rather than pre-splitting into months.
    #
    # Vermont's entire 2026 grid year is ~25K contributions and ~7K
    # expenditures, both far under the 50,000 cap, so the year arrives in a
    # single request instead of twelve. Pre-splitting also manufactured the
    # empty-window problem: August 2026 has no transactions at all, so a
    # monthly seed produced a request that could only ever 500. Asking for the
    # year asks for what exists.
    #
    # A year that IS over the cap costs one refusal and then splits normally —
    # the refusal is issued before any data is transferred, so nothing is
    # wasted but a round trip.
    queue = [(start, end)]
    ok = err = 0

    # A previous run may have written chunks under a different window scheme
    # (e.g. the monthly seed this replaced). Those files are still on disk and
    # still globbed by the parser, so a re-pull of the same year must clear
    # them first or the year would be present twice under two shapes.
    if refresh:
        _drop_files(lambda r, rel=relation, y=str(year):
                    r.get("relation_type") == rel
                    and r.get("year") == y
                    and (r.get("source") or "").startswith("grid"))
        done = {}

    with logging_redirect_tqdm(loggers=[log._log]):
        with tqdm(desc=f"  {relation} {year} (grid)", unit="chunk",
                  dynamic_ncols=True, total=len(queue)) as bar:
            while queue:
                w_from, w_to = queue.pop()
                bar.set_postfix_str(f"{w_from} → {w_to}", refresh=False)

                key = _manifest_key(relation, str(year), w_from.isoformat(),
                                    w_to.isoformat(), "", "")
                if key in done and not refresh:
                    existing = RAW_DIR / (done[key].get("filename") or "")
                    if existing.name and existing.exists():
                        log.file_download_skip(filename=existing.name)
                        bar.update(1)
                        continue

                try:
                    # Fail fast on 5xx: an empty window answers with 500 every
                    # time, so the backoff would only delay the cheap
                    # _five_hundred_means_empty() check below.
                    _, _, capped = _download_grid_chunk(
                        log, session, group, w_from, w_to, date_fmt,
                        retry_5xx=False,
                    )
                    time.sleep(REQUEST_SLEEP)
                except GridLimitExceeded:
                    capped = True          # too wide — fall through and split
                except EXPECTED_HTTP_ERRORS as e:
                    is_5xx = "HTTP 5" in str(e)
                    # Vermont answers a window that matches nothing with a 500
                    # rather than an empty CSV. Splitting such a window just
                    # produces smaller windows that are equally empty and
                    # equally 500 — the 2026-08 partial month recursed
                    # 08-01→08-12, 08-01→08-06, 08-01→08-03, 08-01→08-02 and
                    # would have gone to single days, all for a range with no
                    # transactions in it at all. So establish first whether a
                    # 500 means "empty" on this deployment, and if it does,
                    # record the window as empty and stop.
                    if is_5xx and _five_hundred_means_empty(
                            log, session, group, date_fmt):
                        _record_empty_window(log, group, w_from, w_to)
                        ok += 1
                        bar.update(1)
                        continue

                    if is_5xx:
                        # 500 does NOT mean "empty" on this deployment, so it
                        # may well be transient after all. Now — and only now —
                        # spend the retry budget that was skipped above.
                        try:
                            _, _, capped = _download_grid_chunk(
                                log, session, group, w_from, w_to, date_fmt,
                                retry_5xx=True,
                            )
                            time.sleep(REQUEST_SLEEP)
                            if not capped:
                                ok += 1
                                bar.update(1)
                                continue
                        except GridLimitExceeded:
                            pass          # too wide — the split below handles it
                        except EXPECTED_HTTP_ERRORS:
                            pass          # still failing — split to isolate it

                    # Otherwise a 5xx might still be one bad day inside an
                    # otherwise good window, so isolate it by splitting rather
                    # than discarding everything around it.
                    children = _split_window(w_from, w_to) if is_5xx else []
                    if children:
                        log.info(f"  {relation} {w_from} → {w_to} failed with a "
                                 f"server error — splitting into "
                                 f"{len(children)} windows to isolate it")
                        queue.extend(reversed(children))
                        bar.total = (bar.total or 0) + len(children)
                        bar.update(1)
                        continue
                    log.file_download_error(
                        filename=_chunk_filename(relation, w_from, w_to, None, None),
                        error=str(e),
                    )
                    err += 1
                    bar.update(1)
                    continue

                if not capped:
                    ok += 1
                    bar.update(1)
                    continue

                # Over the cap → drop any manifest row for this window (it no
                # longer describes a file that exists) and split it.
                strip_manifest(lambda r, k=key: _row_key(r) != k)
                children = _split_window(w_from, w_to)
                if children:
                    log.info(f"  {relation} {w_from} → {w_to} is over the "
                             f"{ROW_CAP:,}-row cap — splitting into "
                             f"{len(children)} windows")
                    queue.extend(reversed(children))
                    bar.total = (bar.total or 0) + len(children)
                else:
                    log.info(f"  {relation} {w_from} is a single day over the "
                             f"cap — splitting by amount band")
                    b_ok, b_err = _download_amount_bands(
                        log, session, group, w_from, date_fmt
                    )
                    ok  += b_ok
                    err += b_err
                bar.update(1)

    return ok, err


# =========================== bulk download ===========================

def download_bulk_year(log, session: requests.Session, group: dict,
                       year: int) -> int | None:
    """
    Try the Download Data page export for one (relation, filing year).

    Returns the row count on success, or None when the year isn't published
    there — the server answered with an error or with no data rows. Both are
    normal: the page only lists closed filing years, so the open year (and
    anything the state hasn't posted yet) legitimately has no bulk file and
    belongs to the grid path instead.

    Only EXPECTED_HTTP_ERRORS demote a year to the grid. A TransportError, or
    anything else unexpected (a bug, a broken TLS stack, RecursionError from a
    misbehaving ssl monkeypatch), propagates and stops the run. Demoting those
    would turn one clear error into twenty-six confusing ones, and then several
    hundred more from the grid probe.
    """
    relation = group["relation"]
    filename = _bulk_filename(relation, year)
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t_file = time.perf_counter()
    try:
        body = _post_csv(session, BULK_URL, _bulk_body(group, year))
    except EXPECTED_HTTP_ERRORS as e:
        log.info(f"  {filename}: not available from the bulk export ({e})")
        return None

    rows = _count_rows(body)
    if rows == 0:
        log.info(f"  {filename}: bulk export returned no rows — "
                 f"year not published on the Download Data page")
        return None

    tmp = out_path.with_suffix(".csv.part")
    tmp.write_text(body, encoding="utf-8", newline="")
    tmp.replace(out_path)

    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=rows,
                         duration_s=round(time.perf_counter() - t_file, 2))
    upsert_manifest({
        "relation_type": relation,
        "source":        "bulk",
        "year":          str(year),
        "window_from":   "",
        "window_to":     "",
        "amount_from":   "",
        "amount_to":     "",
        "filename":      filename,
        "downloaded_at": date.today().isoformat(),
        "row_count":     str(rows),
        "truncated":     "",
    })
    return rows


# ========================== entity rosters ===========================

def download_entities(log, session: requests.Session,
                      group: dict) -> tuple[int, int]:
    """
    Download one entity roster in a single unfiltered call.

    Always re-fetched rather than skipped from the manifest: it's a small file
    (a few thousand rows) reflecting live filer status, so a stale cached copy
    is worse than just pulling it again.
    """
    relation = group["relation"]
    filename = f"{relation}.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t_file = time.perf_counter()
    try:
        body = _post_csv(session, GRID_URL, _entity_body(group))
        rows = _count_rows(body)
        if rows >= ROW_CAP:
            log.warning(f"{filename}: at the {ROW_CAP:,}-row cap — the roster "
                        f"has outgrown a single download and now needs "
                        f"partitioning")
        tmp = out_path.with_suffix(".csv.part")
        tmp.write_text(body, encoding="utf-8", newline="")
        tmp.replace(out_path)

        log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                             rows=rows,
                             duration_s=round(time.perf_counter() - t_file, 2))
        upsert_manifest({
            "relation_type": relation,
            "source":        "roster",
            "year":          "",
            "window_from":   "",
            "window_to":     "",
            "amount_from":   "",
            "amount_to":     "",
            "filename":      filename,
            "downloaded_at": date.today().isoformat(),
            "row_count":     str(rows),
            "truncated":     "1" if rows >= ROW_CAP else "",
        })
        return 1, 0
    except GridLimitExceeded:
        # The roster request carries no date filter to narrow, so there is no
        # automatic answer here — it would need partitioning by party, office
        # or town. Flag it rather than pretending the file is fine.
        log.file_download_error(
            filename=filename,
            error=f"roster export refused for exceeding the {ROW_CAP:,}-row "
                  f"cap — it has outgrown a single unfiltered download and "
                  f"now needs partitioning (by office/party/town)",
        )
        return 0, 1
    except EXPECTED_HTTP_ERRORS as e:
        log.file_download_error(filename=filename, error=str(e))
        return 0, 1


def _archive_batches(year_from: int, year_to: int) -> list[tuple[int, int]]:
    """Split a year range into ARCHIVE_BATCH_YEARS-wide inclusive spans."""
    out, lo = [], year_from
    while lo <= year_to:
        hi = min(lo + ARCHIVE_BATCH_YEARS - 1, year_to)
        out.append((lo, hi))
        lo = hi + 1
    return out


def _stream_archive_batch(log, session: requests.Session, host: str,
                          lo: int, hi: int, out_path: Path) -> int:
    """
    Stream one batch to disk, returning the byte count.

    Streamed rather than buffered: the previous version called resp.text,
    which held the entire response in memory (twice — raw bytes plus the
    decoded string) before writing anything, so the download showed no
    progress and no disk activity until it was completely finished. This
    writes as it arrives, in constant memory, with a progress bar.
    """
    url = f"{host}{ARCHIVE_PATH}?search={quote(_archive_search(lo, hi), safe='')}"
    tmp = out_path.with_suffix(".csv.part")
    tmp.unlink(missing_ok=True)

    with session.get(url, timeout=ARCHIVE_TIMEOUT, stream=True,
                     headers={"Accept": "text/csv,*/*"}) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length") or 0)
        written = 0
        first = b""
        with open(tmp, "wb") as fh:
            with logging_redirect_tqdm(loggers=[log._log]):
                with tqdm(total=total or None, unit="B", unit_scale=True,
                          desc=f"  archive {lo}-{hi}", dynamic_ncols=True,
                          leave=False) as bar:
                    for chunk in resp.iter_content(chunk_size=ARCHIVE_CHUNK):
                        if not chunk:
                            continue
                        if not first:
                            first = chunk[:2000]
                            # Fail before writing a whole error page to disk.
                            if b"candidate_name" not in first:
                                raise BadResponse(
                                    f"not the results CSV: {first[:160]!r}")
                        fh.write(chunk)
                        written += len(chunk)
                        bar.update(len(chunk))

    if written == 0:
        raise BadResponse("empty response")
    tmp.replace(out_path)
    return written


def download_elections_archive(log, session: requests.Session,
                               year_from: int, year_to: int,
                               force: bool = False) -> tuple[int, int]:
    """
    Download the VT Elections Database contest results, the only public source
    of candidate party and district for Vermont.

    Tries the .gov host first (see ARCHIVE_HOSTS), then the vendor backend with
    a warning. Fetched in ARCHIVE_BATCH_YEARS-wide batches, each streamed to
    its own file; completed batches are recorded in the manifest and skipped on
    a later run, so an interrupted download resumes rather than restarting.

    The batch covering the current year is always re-fetched — it is the one
    that can still gain rows.
    """
    ok = err = 0
    cur_year = date.today().year
    done = load_manifest()

    # An interrupted earlier run can leave orphaned partials; they are dead
    # weight (the last one was 115 MB) and nothing ever reads them.
    for stale in RAW_DIR.glob("elections_archive*.csv.part"):
        try:
            stale.unlink()
            log.info(f"  removed stale partial: {stale.name}")
        except OSError:
            pass

    for lo, hi in _archive_batches(year_from, year_to):
        filename = f"elections_archive_{lo}_{hi}.csv"
        out_path = RAW_DIR / filename
        key = _manifest_key("elections_archive", "", str(lo), str(hi), "", "")
        is_current = lo <= cur_year <= hi

        if (not force and not is_current and key in done
                and out_path.exists() and out_path.stat().st_size > 0):
            log.file_download_skip(filename=filename)
            continue

        log.file_download_start(filename=filename)
        t_file = time.perf_counter()
        last_err: Exception | None = None
        served_by = None

        for label, host in ARCHIVE_HOSTS:
            try:
                written = _stream_archive_batch(log, session, host, lo, hi, out_path)
                served_by = label
                break
            except (requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                raise _transport_error(e) from e
            except Exception as e:
                last_err = e
                log.info(f"  elections archive {lo}-{hi}: {label} host did not "
                         f"serve it ({str(e)[:120]})")

        if served_by is None:
            log.file_download_error(
                filename=filename,
                error=f"no host served this batch; last error: {last_err}")
            err += 1
            continue

        if served_by == "vendor":
            log.warning(
                "  elections archive came from the VENDOR host "
                "(vt2.elstats.civera.com), not electionarchive.vermont.gov. "
                "This is a third-party domain serving an official .gov site's "
                "data — the same situation that got id.electionstats.com "
                "removed from the Idaho scraper. Review against the .gov-only "
                "source policy before relying on it."
            )

        rows = _count_rows_path(out_path)
        log.file_download_ok(filename=filename, bytes=written, rows=rows,
                             duration_s=round(time.perf_counter() - t_file, 2))
        upsert_manifest({
            "relation_type": "elections_archive",
            "source":        f"archive:{served_by}",
            "year":          "",
            "window_from":   str(lo),
            "window_to":     str(hi),
            "amount_from":   "", "amount_to": "",
            "filename":      filename,
            "downloaded_at": date.today().isoformat(),
            "row_count":     str(rows),
            "truncated":     "",
        })
        ok += 1
        time.sleep(REQUEST_SLEEP)

    # The batch files supersede the older single-file download, whose range
    # (whatever it was) is a subset of what was just fetched. Leaving it in
    # place costs ~95 MB and makes the parser log confusing: read first, it
    # claims most of the records and the batches then report "0 rows" because
    # everything in them was already seen. Only removed when every batch
    # succeeded, so a partial run never destroys the one good copy.
    legacy = RAW_DIR / ARCHIVE_FILENAME
    if err == 0 and ok and legacy.exists():
        try:
            size_mb = legacy.stat().st_size / 1e6
            legacy.unlink()
            log.info(f"  removed {ARCHIVE_FILENAME} ({size_mb:.0f} MB) — "
                     f"superseded by the {ok} year-batch file(s)")
            strip_manifest(lambda r: not (
                r.get("relation_type") == "elections_archive"
                and r.get("filename") == ARCHIVE_FILENAME))
        except OSError as e:
            log.info(f"  could not remove the superseded {ARCHIVE_FILENAME} ({e})")

    return ok, err


def download_openstates(log, session: requests.Session) -> tuple[int, int]:
    """
    Download the Open States current-legislator CSV and normalize its columns.

    Source columns are current_party / current_district / current_chamber;
    they're translated to party / district / chamber on write so the layout
    matches what scrapers/texas.py produces and the parser can stay simple.

    A failure here is non-fatal — it's a fallback source, so the run logs the
    error and continues rather than losing the primary enrichment with it.
    """
    out_path = RAW_DIR / OPENSTATES_FILENAME
    log.file_download_start(filename=OPENSTATES_FILENAME)
    t_file = time.perf_counter()
    try:
        resp = session.get(OPENSTATES_URL, timeout=120,
                           headers={"Accept": "text/csv,*/*"})
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        text = resp.text
        if not text.strip() or "name" not in text[:400]:
            raise BadResponse(f"unexpected body: {text[:160]!r}")
    except (requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout) as e:
        raise _transport_error(e) from e
    except Exception as e:
        log.file_download_error(filename=OPENSTATES_FILENAME, error=str(e))
        log.warning("  Open States unavailable — party enrichment falls back "
                    "to the elections archive alone")
        return 0, 1

    rows: list[dict] = []
    for src in csv.DictReader(io.StringIO(text.lstrip("﻿"), newline="")):
        name    = (src.get("name") or "").strip()
        chamber = OPENSTATES_CHAMBERS.get(
            (src.get("current_chamber") or "").strip().lower(), "")
        party   = (src.get("current_party") or "").strip()
        if not (name and chamber and party):
            continue
        rows.append({
            "openstates_id": (src.get("id") or "").strip(),
            "name":          name,
            "given_name":    (src.get("given_name") or "").strip(),
            "family_name":   (src.get("family_name") or "").strip(),
            "party":         party,
            "chamber":       chamber,
            "district":      (src.get("current_district") or "").strip(),
        })

    tmp = out_path.with_suffix(".csv.part")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OPENSTATES_COLS,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(rows)
    tmp.replace(out_path)

    log.file_download_ok(filename=OPENSTATES_FILENAME,
                         bytes=out_path.stat().st_size, rows=len(rows),
                         duration_s=round(time.perf_counter() - t_file, 2))
    upsert_manifest({
        "relation_type": "openstates", "source": "openstates", "year": "",
        "window_from": "", "window_to": "", "amount_from": "", "amount_to": "",
        "filename": OPENSTATES_FILENAME,
        "downloaded_at": date.today().isoformat(),
        "row_count": str(len(rows)), "truncated": "",
    })
    return 1, 0


# ============================== run =================================

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
    elections: bool = False,
):
    """
    Download Vermont contributions, expenditures, the two entity rosters and
    the elections archive that supplies party and district.

    Horizontal scope (default = everything):
        --transactions              contributions + expenditures
        --contributions             contributions only (TCON)
        --expenditures              expenditures only (TEXP)
        --entities                  candidate + committee rosters + elections archive
        --candidates / --committees the corresponding roster only
        --elections                 party/district sources only: the VT
                                    elections archive plus the Open States
                                    current-legislator CSV

    Vertical scope bounds the transaction years. Each year in scope is tried
    against the bulk export first and falls back to the windowed browse grid
    when the bulk export doesn't publish it. The rosters carry no year
    parameter and are refreshed on every run.

        (no flag)          incremental — fill manifest gaps, always refresh
                           the current year
        --start-year YYYY  re-download years >= YYYY, wiping their manifest
                           entries and raw files first
        --end-year YYYY    re-download years <= YYYY
        --force            re-download every year in scope
    """
    log = get_logger("vermont", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year,
              end_year=end_year)

    # ── Resolve scope ─────────────────────────────────────────────────
    no_h = not (entities or transactions or contributions or expenditures
                or candidates or committees)

    do_contributions = no_h or transactions or contributions
    do_expenditures  = no_h or transactions or expenditures
    do_candidates    = no_h or entities or candidates
    do_committees    = no_h or entities or committees
    # Party/district enrichment. Part of the entity scope: it describes
    # candidates, not transactions.
    do_elections     = no_h or entities or candidates or elections

    groups = [g for g in TRANSACTION_GROUPS
              if (g["relation"] == "contributions" and do_contributions)
              or (g["relation"] == "expenditures"  and do_expenditures)]
    rosters = [g for g in ENTITY_GROUPS
               if (g["relation"] == "candidates" and do_candidates)
               or (g["relation"] == "committees" and do_committees)]

    cur_year = date.today().year
    lo = start_year if start_year is not None else MIN_YEAR
    hi = end_year   if end_year   is not None else cur_year
    hi = min(hi, cur_year)
    years = list(range(lo, hi + 1))

    files_ok = files_err = 0

    try:
        session = _make_session(log)
        try:
            _preflight(log, session)
        except TransportError as e:
            # A strict-mode-only structural complaint about the proxy's CA is
            # recoverable and worth recovering from automatically: the
            # alternative the user is left with is VT_INSECURE, which is far
            # worse. Anything else propagates untouched.
            if not _is_strict_only_failure(e):
                raise
            log.warning(
                "  TLS: the trust chain is fine but fails OpenSSL's strict "
                "RFC 5280 extension checks (Python 3.13 enables these by "
                "default). Retrying with just those checks relaxed."
            )
            session = _make_session(log, relax_strict=True)
            _preflight(log, session)
        txn_relations = {g["relation"] for g in groups}
        year_range_active = start_year is not None or end_year is not None

        # ── Manifest / file scoping ───────────────────────────────────
        # Grid chunks are window-named, so anything dropped from the manifest
        # must also be dropped from disk or its rows get counted twice.
        def _in_scope(r: dict) -> bool:
            if r.get("relation_type") not in txn_relations:
                return False
            try:
                yr = int(r.get("year") or "")
            except ValueError:
                return False
            if start_year is not None and yr < start_year:
                return False
            if end_year is not None and yr > end_year:
                return False
            return True

        if force:
            _drop_files(lambda r: r.get("relation_type") in txn_relations)
        elif year_range_active:
            _drop_files(_in_scope)

        done = load_manifest()

        # ── Transactions, year by year ────────────────────────────────
        for group in groups:
            relation = group["relation"]
            for year in years:
                bulk_key = _manifest_key(relation, str(year), "", "", "", "")
                bulk_path = RAW_DIR / _bulk_filename(relation, year)
                is_open_year = year >= cur_year

                # An explicit year range is a request for a refresh; a bare run
                # only re-pulls the open year.
                have_bulk = (bulk_key in done and bulk_path.exists()
                             and bulk_path.stat().st_size > 0)
                if have_bulk and not is_open_year and not year_range_active \
                        and not force:
                    log.file_download_skip(filename=bulk_path.name)
                    continue

                rows = download_bulk_year(log, session, group, year)
                time.sleep(REQUEST_SLEEP)

                if rows is not None:
                    files_ok += 1
                    # This year now comes from the bulk export. Drop any grid
                    # chunks left over from when it didn't, so the parser can't
                    # read the same transactions twice.
                    _drop_files(lambda r, rel=relation, y=str(year):
                                r.get("relation_type") == rel
                                and r.get("year") == y
                                and r.get("source") == "grid")
                    done = load_manifest()
                    continue

                # ── Bulk export doesn't publish this year → browse grid ──
                bulk_path.unlink(missing_ok=True)
                strip_manifest(lambda r, k=bulk_key: _row_key(r) != k)
                done.pop(bulk_key, None)

                log.info(f"  {relation} {year}: falling back to the browse "
                         f"grid (windowed, {ROW_CAP:,}-row cap)")
                try:
                    ok, err = download_grid_year(
                        log, session, group, year, done,
                        # Re-pull the open year every run; an explicit range is
                        # itself a refresh request.
                        refresh=is_open_year or year_range_active or force,
                    )
                except EXPECTED_HTTP_ERRORS as e:
                    log.warning(f"  {relation} {year}: grid fallback failed "
                                f"({e}) — continuing with other years")
                    files_err += 1
                    continue
                files_ok  += ok
                files_err += err
                done = load_manifest()

        # ── Entity rosters ────────────────────────────────────────────
        for group in rosters:
            ok, err = download_entities(log, session, group)
            files_ok  += ok
            files_err += err
            time.sleep(REQUEST_SLEEP)

        # ── Elections archive (party + district) ──────────────────────
        if do_elections:
            ok, err = download_elections_archive(log, session, lo, hi,
                                                 force=force)
            files_ok  += ok
            files_err += err
            ok, err = download_openstates(log, session)
            files_ok  += ok
            files_err += err

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} err")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise

    except TransportError as e:
        # Print rather than log-only: this one has a multi-line fix in it and
        # is the error most likely to be hit on a first run from a corporate
        # network, so it should be readable in the terminal without digging
        # through the JSONL.
        log.error(f"Cannot reach the Vermont API — aborting.\n{e}")
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  error_type="TransportError", error=str(e))
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  error_type=type(e).__name__, error=str(e))
        raise


# ================================ CLI ================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download Vermont campaign finance data from the Campaign "
                    "Finance System of Vermont."
    )

    # Vertical — mutually exclusive
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all years in scope, wipe manifest and chunks")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive); wipes manifest for range")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest year to download (inclusive, <= current year)")

    # Horizontal — top level
    ap.add_argument("--transactions", action="store_true",
                    help="contributions + expenditures only")
    ap.add_argument("--entities",     action="store_true",
                    help="candidate + committee rosters only")

    # Horizontal — second level
    ap.add_argument("--contributions", action="store_true", help="contributions only (TCON)")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only (TEXP)")
    ap.add_argument("--candidates",    action="store_true", help="candidate roster only")
    ap.add_argument("--committees",    action="store_true", help="committee roster only")
    ap.add_argument("--elections",     action="store_true",
                    help="elections archive only (candidate party + district)")

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
            elections=args.elections,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
