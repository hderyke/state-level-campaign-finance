"""
scrapers/north_dakota.py — Download North Dakota campaign finance data.

Source: ND Secretary of State Campaign Finance Reporting System (CFRS), the
"Ethics Solution" SPA at https://cfrs.sos.nd.gov (Data Download tab). No
Playwright needed — the tab is backed by two plain JSON endpoints:

  1. POST  /api/Public-Service/AccessReport/getDataDownloadDataList
           {"pageNumber": 1, "pageSize": 100}
     Returns the catalog of generated bulk files. Each entry carries an S3
     key under  nd-cfs/CFDataDownload/  named
           {Category}_{Year}_{YYYYMMDDHHMMSS}.csv
     e.g.  nd-cfs/CFDataDownload/Contributions_2026_20260804132501.csv

     Five categories observed as of 2026-08:
       Contributions      → contributions
       Expenditure        → expenditures      (singular in the source)
       Registration       → committees + candidates (the filer roster)
       FiledReports       → report metadata; joined by the parser to derive
                            `amended` and `filing_id`
       ReportingSchedule  → filing-deadline calendar; no filer data, unused
                            downstream, archived on full runs only

  2. POST  /api/Common-Service/AmazonCloudFront/getDownloadLinkWithoutCookies
           {"s3FilePath": "nd-cfs/CFDataDownload/Contributions_2026_....csv"}
     Returns a short-lived signed CloudFront URL for that key, which is then
     fetched with a plain streaming GET.

  3. POST  /api/Common-Service/DataGrid/generateExportGridDataExcel
     A second acquisition path — the public "Get to Know" roster grid, which is
     the only source of office / district / party / address for filers. Unlike
     the two endpoints above it returns the workbook inline as base64:
           {"isSuccess": true,
            "responseData": {"fileBytes": "UEsDBBQ...<base64 xlsx>"},
            "message": null, "skipRecords": null}
     See GRID_EXPORTS and _resolve_grid_body().

No authentication, no cookies, no CSRF token. An Origin/Referer pair matching
the public site is required or the API returns 403 — and for (3) the Referer
must be the grid's own page, not the Data Download tab.

Uses verify=False throughout — CFRS serves a broken certificate chain (it sends
its leaf twice and omits the intermediate CA), so OpenSSL cannot validate it on
any machine. urllib3 warnings are suppressed via disable_warnings. The server's
certificate fingerprint is checked against a known value and a change is warned
about but not treated as fatal. Full rationale in the TLS section below.

Shape-agnostic response handling
────────────────────────────────
The two endpoints are undocumented and their JSON envelopes are not stable
across CFRS releases (the SPA bundle wraps them differently per version).
Rather than hard-coding a key path like ["data"]["items"][i]["filePath"],
this scraper walks the decoded JSON recursively and picks out:

  • listing  — every string that looks like a CFDataDownload key/filename
               (see _FILE_RE); category and year come from the filename
               itself, so no metadata field is required
  • link     — the first string that looks like an http(s) URL

That makes the scraper survive an envelope rename, and it fails loudly with
the raw response body if neither pattern is found.

Local naming
────────────
Source filenames embed a generation timestamp, so they change on every
regeneration. Files land locally under a stable, timestamp-free name
(`contributions_2026.csv`) and the source filename is recorded in the
manifest. An incremental run therefore re-downloads a year only when ND has
actually regenerated it (timestamp changed), plus the current year
unconditionally per pipeline convention.

Some categories are published as .zip — CSV members are extracted and the
archive is discarded.
"""

import csv
import io
import json
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== TLS ===================================
# Uses verify=False throughout — cfrs.sos.nd.gov serves a BROKEN certificate
# chain, so OpenSSL cannot validate it. Confirmed against Qualys SSL Labs
# (grade B, cert chain "issues": 6): the chain the server transmits is
#
#     [ leaf, leaf ]          — the same certificate twice
#
# while every trust path to a root needs
#
#     leaf -> 6542d176…(intermediate) -> root
#
# The intermediate CA is never sent. Browsers hide this by downloading it from
# the leaf's AIA extension; OpenSSL does not, so plain requests fails with
# CERTIFICATE_VERIFY_FAILED "unable to get local issuer certificate" on every
# machine and every network. It is the server's bug, not a client or
# corporate-proxy problem. Same pattern (and same rationale) as
# scrapers/alabama.py; see docs/contributing.md §4 on SSL issues.
#
# Because verification is off, the server is not authenticated. To keep some
# handle on that, check_server_fingerprint() records the SHA-256 of the
# certificate the server actually presents and warns if it differs from the one
# observed when this scraper was written. That detects both a routine ND
# certificate rotation and an actual substitution.
#
# Deliberately a WARNING, not a failure: a hard fail would stop the pipeline on
# rotation day for an entirely routine event. The trade-off is honest — this is
# an audit trail, not enforcement. The fingerprint lands in the scrape_started
# JSONL event every run, so a change is auditable after the fact even if nobody
# reads the console.

import hashlib
import socket
import ssl

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HOST = "cfrs.sos.nd.gov"

# The chain cannot be verified, so requests must be told not to try.
VERIFY = False

# SHA-256 (DER) of the leaf certificate cfrs.sos.nd.gov presented as of
# 2026-08, from the SSL Labs scan of this host. Update this when ND rotates —
# the warning tells you the new value.
EXPECTED_LEAF_SHA256 = (
    "4cc054e06c1ea4eafedfcde144e730550d224e3197a96dfe9a9a2fdb174b07dd"
)


def server_leaf_fingerprint(host: str, port: int = 443, timeout: int = 30) -> str | None:
    """SHA-256 of the leaf certificate `host` presents, or None if unobtainable.

    Verification is off here by necessity — the whole point is to fingerprint a
    certificate whose chain does not validate. Nothing is trusted on the basis
    of this value; it is only compared and logged.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                der = ssock.getpeercert(binary_form=True)
        return hashlib.sha256(der).hexdigest() if der else None
    except Exception:
        # Never let the pin check itself break a scrape.
        return None


def check_server_fingerprint(log) -> str | None:
    """Compare the presented certificate against EXPECTED_LEAF_SHA256.

    Returns the observed fingerprint (or None). Warns on mismatch; never raises.
    """
    fp = server_leaf_fingerprint(HOST)
    if fp is None:
        log.debug("  TLS: could not read the server certificate to fingerprint it")
        return None
    if fp != EXPECTED_LEAF_SHA256:
        log.warning(
            "  [!] cfrs.sos.nd.gov certificate has CHANGED.\n"
            f"      expected {EXPECTED_LEAF_SHA256}\n"
            f"      observed {fp}\n"
            "      Most likely a routine ND certificate rotation — verify the new\n"
            "      fingerprint against the site in a browser, then update\n"
            "      EXPECTED_LEAF_SHA256 in this file. Continuing anyway; TLS\n"
            "      verification is disabled for this host so the certificate is\n"
            "      not otherwise authenticated."
        )
        log._emit("tls_fingerprint_changed", host=HOST,
                  expected=EXPECTED_LEAF_SHA256, observed=fp)
    else:
        log.debug(f"  TLS: server certificate fingerprint matches ({fp[:16]}…)")
    return fp


# ================================ paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "North Dakota" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "North Dakota" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

# source_file holds the timestamped S3 filename — comparing it against the
# current listing is what makes incremental runs precise (see module docstring).
MANIFEST_COLS = ["category", "year", "source_file", "local_file",
                 "downloaded_at", "row_count"]

# ============================== constants ==============================

BASE     = "https://cfrs.sos.nd.gov"
LIST_URL = f"{BASE}/api/Public-Service/AccessReport/getDataDownloadDataList"
LINK_URL = f"{BASE}/api/Common-Service/AmazonCloudFront/getDownloadLinkWithoutCookies"
GRID_URL = f"{BASE}/api/Common-Service/DataGrid/generateExportGridDataExcel"
REFERER  = f"{BASE}/public/accessreports?tab=datadownload"

# ── DataGrid exports ───────────────────────────────────────────────────
# A second, separate acquisition path. The Data Download tab (above) has no
# office / district / party / address for filers at all, but the public
# "Get to Know" grids do, and they can be exported wholesale. Empty/null
# filters mean "everything", and the result is not year-scoped — it's a single
# all-cycles roster, so it's re-fetched on every run like any year-less export.
#
# The grid's own page codes are enumerable from the SPA bundle
# (PUB_GTK_CNCM / ELE / OFC / PTY / VIOL); only CNCM carries filer attributes,
# which is why it's the only one wired up.
GRID_EXPORTS = [
    {
        "slug":      "candidate_committees",
        "group":     "committees",     # scope: --entities / --committees / --candidates
        "grid_name": "GETTOKNOW_CANDIDATECOMMITTEES",
        "page_name": "PUB_GTK_CNCM",
        "referer":   f"{BASE}/public/gettoknow?tab=candidate",
        "ext":       "xlsx",
        "sort":      "registrationDate",
    },
]

# Every filter key the grid expects. Sending the full set with empty/null
# values is what makes the export unfiltered — omitting keys has not been
# tested and the API may reject a partial filter object.
_GRID_FILTER_EMPTY = {
    "EntityId": "", "OrgStatus": "", "OrgSubTypeCode": "", "CandidateName": "",
    "OfficerName": "", "OrgName": "", "OrgType": "",
    "RegistrationStartDate": None, "RegistrationEndDate": None,
    "ElectionID": None, "OfficeID": None, "DistrictID": None,
    "PartyCode": None, "ReportingCycleId": None, "IsJointFundrisingOrg": "",
}

# All Data Download keys live under this prefix. Listing entries are sometimes
# bare filenames rather than full keys, so the prefix is re-applied when needed.
S3_PREFIX = "nd-cfs/CFDataDownload/"

# {Category}_{Year}_{timestamp}.{ext}  — year and timestamp are both optional
# because not every category is published per-year (e.g. a single Committees
# export). Trailing timestamp is 8–14 digits depending on the generator.
_FILE_RE = re.compile(
    r"(?P<label>[A-Za-z][A-Za-z0-9]*(?:[ _-][A-Za-z0-9]+)*?)"
    r"(?:_(?P<year>(?:19|20)\d{2}))?"
    r"_(?P<stamp>\d{8,14})"
    r"\.(?P<ext>csv|zip|txt)$",
    re.IGNORECASE,
)

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)

# Raw category label (normalized: lowercase, non-alphanumerics stripped) →
# (local category slug, relation group). The relation group is what the
# --transactions / --entities horizontal flags filter on.
#
# The five labels CFRS actually publishes as of 2026-08 are Contributions,
# Expenditure (singular), Registration, FiledReports and ReportingSchedule.
# The extra aliases are defensive: CFRS has renamed exports between releases,
# and an unrecognized label is still downloaded under a slug derived from
# itself and grouped "other" rather than silently dropped.
_CATEGORIES = {
    # ── transactions ──
    "contributions":           ("contributions",   "contributions"),
    "contribution":            ("contributions",   "contributions"),
    "inkindcontributions":     ("inkind",          "contributions"),
    "inkind":                  ("inkind",          "contributions"),
    "expenditure":             ("expenditures",    "expenditures"),
    "expenditures":            ("expenditures",    "expenditures"),
    "expenses":                ("expenditures",    "expenditures"),
    "independentexpenditure":  ("independent_expenditures", "expenditures"),
    "independentexpenditures": ("independent_expenditures", "expenditures"),
    "loans":                   ("loans",           "expenditures"),
    "debts":                   ("loans",           "expenditures"),
    # ── entities ──
    # "Registration" is CFRS's filer roster and is the source of BOTH the
    # committees and candidates relations (a candidate's identity in ND data
    # is their registration), so it belongs to the "committees" group and
    # --candidates pulls it too.
    "registration":            ("committees",      "committees"),
    "registrations":           ("committees",      "committees"),
    "committees":              ("committees",      "committees"),
    "committee":               ("committees",      "committees"),
    "filers":                  ("committees",      "committees"),
    "candidates":              ("candidates",      "candidates"),
    "candidate":               ("candidates",      "candidates"),
    # ── metadata companions (see GROUPS_ALWAYS / GROUPS_FULL_RUN_ONLY) ──
    "filedreports":            ("filed_reports",     "reports"),
    "reportingschedule":       ("reporting_schedule", "reference"),
}

# "reports" (FiledReports) is pulled alongside any scope: the parser joins it
# onto transactions to derive `amended` and `filing_id`, and it's also filer
# metadata, so it's relevant to an --entities run as well. It's ~140 KB/year.
GROUPS_ALWAYS = {"reports"}

# "reference" (ReportingSchedule) is a filing-deadline calendar with no filer
# or transaction data at all — nothing in the pipeline reads it. Archived on a
# full run only, so a targeted --contributions run doesn't drag it along.
GROUPS_FULL_RUN_ONLY = {"reference", "other"}

# CFRS launched January 2026 and holds 2025 year-end reporting onward. Reports
# filed 2024 and earlier live in the separate legacy Archive at
# cf.sos.nd.gov/search/cfsearch.aspx, which has no bulk export and is NOT
# scraped — see docs/states/north_dakota.md.
#
# ND is actively migrating history into CFRS: per the SOS, data migration runs
# through summer 2026, transfer completes year-end 2027, and CFRS is intended to
# hold a full five years by January 2028. So the set of available years GROWS
# over time, which is why there is deliberately no hardcoded floor — an absent
# --start-year means "everything the catalog offers", so newly migrated years
# are picked up without a code change.
#
# Documentation only; nothing filters on it.
CFRS_EARLIEST_KNOWN_YEAR = 2025

REQUEST_TIMEOUT  = 120     # listing / link resolution
DOWNLOAD_TIMEOUT = 600     # the CSV fetch itself; full-year files are large


def _norm_label(s: str) -> str:
    """Normalize a category label for _CATEGORIES lookup: lowercase, alnum only."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


# ========================= manifest helpers ===========================

def load_manifest() -> dict[tuple[str, str], dict]:
    """Return {(category, year): manifest_row} for everything downloaded so far."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {(r.get("category", ""), r.get("year", "")): r
                for r in csv.DictReader(f)}


def strip_manifest(keep_fn):
    """Rewrite the manifest keeping only rows for which keep_fn(row) is True."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict):
    """Replace any existing (category, year) row, then append this one.

    Upsert rather than append: source filenames change on every regeneration,
    so an append-only manifest would accumulate one stale row per refresh and
    load_manifest() would resolve to whichever happened to be read last.
    """
    key = (record["category"], record["year"])
    strip_manifest(lambda r: (r.get("category", ""), r.get("year", "")) != key)
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow(record)


# ========================== JSON walk helpers =========================

def _walk_strings(obj):
    """Yield every string found anywhere in a decoded JSON structure."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _walk_strings(v)


def _decode_json(resp: requests.Response):
    """Decode a response body as JSON, tolerating a bare JSON string body.

    Some CFRS endpoints return `"https://..."` (a quoted string) with a
    text/plain content type, which resp.json() rejects on older urllib3 —
    fall back to json.loads on the raw text, then to the text itself.
    """
    try:
        return resp.json()
    except ValueError:
        text = resp.text.strip()
        try:
            return json.loads(text)
        except ValueError:
            return text


def _extract_url(resp: requests.Response) -> str | None:
    """Pull the first http(s) URL out of a link-resolution response."""
    for s in _walk_strings(_decode_json(resp)):
        s = s.strip()
        if _URL_RE.match(s):
            return s
    return None


# ============================ HTTP session ============================

def _make_session() -> requests.Session:
    """Session with the Origin/Referer pair CFRS requires — without them the
    API answers 403 regardless of user agent."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":   "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/136.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type":    "application/json",
        "Origin":          BASE,
        "Referer":         REFERER,
    })
    # Session-wide so every call through it inherits the resolved CA bundle.
    s.verify = VERIFY
    return s


# ============================== listing ===============================

def list_available(log, session: requests.Session,
                   page_size: int = 100, max_pages: int = 50) -> list[dict]:
    """
    Enumerate the Data Download catalog.

    Returns [{category, group, year, source_file, s3_path, stamp, ext}, ...],
    de-duplicated on s3_path and sorted by (category, year).

    Pagination stops as soon as a page contributes no new keys — the endpoint's
    total-count field name isn't stable across releases, so "no new keys" is a
    more reliable terminator than trusting a reported total. max_pages is a
    guard against an endpoint that ignores pageNumber and returns page 1
    forever.
    """
    found: dict[str, dict] = {}
    resp: requests.Response | None = None

    for page in range(1, max_pages + 1):
        payload = {"pageNumber": page, "pageSize": page_size}
        resp = session.post(LIST_URL, json=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = _decode_json(resp)

        new_on_page = 0
        for s in _walk_strings(data):
            s = s.strip()
            if not s:
                continue
            # Accept a full S3 key, a CloudFront URL, or a bare filename
            name = s.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
            m = _FILE_RE.match(name)
            if not m:
                continue

            s3_path = s if "/" in s and not _URL_RE.match(s) else S3_PREFIX + name
            if s3_path in found:
                continue

            slug, group = _CATEGORIES.get(
                _norm_label(m.group("label")),
                (_norm_label(m.group("label")) or "other", "other"),
            )
            found[s3_path] = {
                "category":    slug,
                "group":       group,
                "year":        m.group("year") or "",
                "source_file": name,
                "s3_path":     s3_path,
                "stamp":       m.group("stamp"),
                "ext":         m.group("ext").lower(),
            }
            new_on_page += 1

        if new_on_page == 0:
            break

    if not found:
        # Loud failure with the body — the envelope changed and the walk found
        # nothing that looks like a CFDataDownload key.
        snippet = resp.text[:1000] if resp is not None else "(no response)"
        raise RuntimeError(
            "No CFDataDownload files found in the listing response. The API "
            f"envelope may have changed. First 1000 bytes:\n{snippet}"
        )

    entries = sorted(found.values(), key=lambda e: (e["category"], e["year"]))
    log.info(f"  catalog: {len(entries)} file(s) across "
             f"{len({e['category'] for e in entries})} categor(ies)")
    log._emit("catalog_listed", files=len(entries),
              categories=sorted({e["category"] for e in entries}))
    return entries


# ============================= downloading ============================

def resolve_link(session: requests.Session, s3_path: str) -> str:
    """Exchange an S3 key for a signed CloudFront URL."""
    resp = session.post(LINK_URL, json={"s3FilePath": s3_path},
                        timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    url = _extract_url(resp)
    if not url:
        raise RuntimeError(
            f"No download URL in link response for {s3_path!r}: "
            f"{resp.text[:300]!r}"
        )
    return url


def _write_csv_bytes(body: bytes, out_path: Path) -> int:
    """Write CSV bytes to out_path, normalizing encoding. Returns row count.

    CFRS occasionally emits UTF-16 (the .NET export path) and BOM-prefixed
    UTF-8. Both are normalized to plain UTF-8 so the parser only ever has to
    deal with one encoding.
    """
    if body[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = body.decode("utf-16", errors="replace")
    elif body[:3] == b"\xef\xbb\xbf":
        text = body[3:].decode("utf-8", errors="replace")
    else:
        text = body.decode("utf-8", errors="replace")

    out_path.write_text(text, encoding="utf-8", newline="")
    # Minus the header row; good enough for manifest bookkeeping
    return max(text.count("\n") - 1, 0)


def download_entry(log, session: requests.Session, entry: dict) -> tuple[str, int] | None:
    """
    Resolve, fetch and store one catalog entry.

    Local name is `{category}_{year}.csv`, or `{category}.csv` when the source
    file carries no year. Returns (local_filename, row_count) or None on
    failure — individual file failures are logged and skipped, not fatal.
    """
    local_name = (f"{entry['category']}_{entry['year']}.csv"
                  if entry["year"] else f"{entry['category']}.csv")
    out_path = RAW_DIR / local_name

    log.file_download_start(filename=local_name)
    t0 = time.perf_counter()

    try:
        url  = resolve_link(session, entry["s3_path"])
        # Signed CloudFront URL — send it clean. Carrying the JSON API's
        # Content-Type/Origin headers onto an S3 GET can invalidate the
        # signature.
        resp = requests.get(url, timeout=DOWNLOAD_TIMEOUT, verify=VERIFY,
                            headers={"User-Agent": session.headers["User-Agent"]})
        resp.raise_for_status()
        body = resp.content
    except (requests.RequestException, RuntimeError) as e:
        log.file_download_error(filename=local_name, error=str(e))
        return None

    if not body.strip():
        # Empty export (happens for a year with no filings yet) — not an error
        log.file_download_skip(filename=local_name)
        return local_name, 0

    if entry["ext"] == "zip":
        rows = 0
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as zf:
                members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not members:
                    log.file_download_error(filename=local_name,
                                            error="zip contained no .csv member")
                    return None
                # Multi-member archives are suffixed so nothing is overwritten
                for i, member in enumerate(members):
                    target = (out_path if i == 0 else
                              out_path.with_name(f"{out_path.stem}_{i}.csv"))
                    rows += _write_csv_bytes(zf.read(member), target)
        except zipfile.BadZipFile as e:
            log.file_download_error(filename=local_name, error=f"bad zip: {e}")
            return None
    else:
        rows = _write_csv_bytes(body, out_path)

    log.file_download_ok(filename=local_name,
                         bytes=out_path.stat().st_size,
                         rows=rows,
                         duration_s=round(time.perf_counter() - t0, 2))
    return local_name, rows


# ========================== DataGrid exports ==========================

_XLSX_MAGIC = b"PK\x03\x04"          # xlsx is a zip container
_B64_RE     = re.compile(r"^[A-Za-z0-9+/\s]{512,}={0,2}$")


def _grid_payload(spec: dict) -> dict:
    return {
        "moduleType":    "PUBLIC",
        "gridName":      spec["grid_name"],
        "filterRequest": {"SortColumn":    spec["sort"],
                          "SortDirection": "desc",
                          **_GRID_FILTER_EMPTY},
        "pageName":      spec["page_name"],
        "fieldType":     "G",
    }


def _resolve_grid_body(session: requests.Session, resp: requests.Response,
                       spec: dict, log=None) -> bytes:
    """Turn a generateExportGridDataExcel response into workbook/CSV bytes.

    Confirmed envelope (observed 2026-08) — the workbook is base64 in
    responseData.fileBytes:

        {"isSuccess": true,
         "responseData": {"fileBytes": "UEsDBBQAAAAIA...<base64 xlsx>"},
         "message": null,
         "skipRecords": null}

    That's the primary path. Three fallbacks follow it, in case CFRS reshapes
    the envelope on a future release (its Data Download siblings already differ
    between versions): the raw file inline, a JSON-wrapped http URL, or a
    JSON-wrapped S3 key resolved via LINK_URL. Raises RuntimeError with a body
    excerpt if none apply, so a mismatch fails loudly at scrape time rather
    than writing a corrupt file for the parser to trip over.
    """
    import base64

    body = resp.content

    # Raw workbook inline (no JSON wrapper)
    if body[:4] == _XLSX_MAGIC:
        return body

    data = _decode_json(resp)

    # ── primary: the confirmed envelope ───────────────────────────────
    if isinstance(data, dict):
        if data.get("isSuccess") is False:
            raise RuntimeError(
                f"{spec['grid_name']} grid export reported failure: "
                f"{data.get('message')!r}"
            )

        # Non-null skipRecords means the export dropped rows. Nothing in the
        # observed responses populates it, but a silent row cap is exactly the
        # failure mode Wisconsin hits (see scrapers/wisconsin.py), so surface
        # it loudly rather than quietly shipping a truncated roster.
        skipped = data.get("skipRecords")
        if skipped and log is not None:
            log.warning(f"  [!] {spec['grid_name']} reported skipRecords="
                        f"{skipped!r} — the export may be truncated")
            log._emit("grid_export_skiprecords", grid=spec["grid_name"],
                      skip_records=str(skipped))

        rd = data.get("responseData")
        if isinstance(rd, dict):
            fb = rd.get("fileBytes")
            if isinstance(fb, str) and fb.strip():
                return base64.b64decode(fb, validate=False)

    # ── fallbacks, in case the envelope changes ───────────────────────
    strings = [s.strip() for s in _walk_strings(data) if s and s.strip()]

    for s in strings:                                   # http(s) URL
        if _URL_RE.match(s):
            r = requests.get(s, timeout=DOWNLOAD_TIMEOUT, verify=VERIFY,
                             headers={"User-Agent": session.headers["User-Agent"]})
            r.raise_for_status()
            return r.content

    for s in strings:                                   # S3 key → signed link
        if re.search(r"\.(xlsx|xls|csv)$", s, re.IGNORECASE):
            key = s if "/" in s else S3_PREFIX + s
            url = resolve_link(session, key)
            r = requests.get(url, timeout=DOWNLOAD_TIMEOUT, verify=VERIFY,
                             headers={"User-Agent": session.headers["User-Agent"]})
            r.raise_for_status()
            return r.content

    for s in strings:                                   # base64 under any key
        if _B64_RE.match(s):
            try:
                decoded = base64.b64decode(s, validate=False)
            except Exception:
                continue
            if decoded[:4] == _XLSX_MAGIC or b"," in decoded[:200]:
                return decoded

    raise RuntimeError(
        f"Could not resolve a file from the {spec['grid_name']} grid export "
        f"response (content-type={resp.headers.get('Content-Type')!r}). "
        f"First 400 bytes: {body[:400]!r}"
    )


def _xlsx_row_count(path: Path) -> int:
    """Data-row count of a workbook, or 0 if it can't be read.

    Informational only (manifest + log), so a failure here must never fail the
    download — the parser is what actually validates the file.
    """
    try:
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            ws = wb.worksheets[0]
            return max(sum(1 for _ in ws.iter_rows(values_only=True)) - 1, 0)
        finally:
            wb.close()
    except Exception:
        return 0


def download_grid_exports(log, session: requests.Session,
                          groups: set[str]) -> tuple[int, int]:
    """Fetch every in-scope DataGrid export. Returns (files_ok, files_err).

    These are always re-fetched: the export has no year to compare against and
    registrations change continuously.
    """
    ok = err = 0

    for spec in GRID_EXPORTS:
        if spec["group"] not in groups:
            continue

        local_name = f"{spec['slug']}.{spec['ext']}"
        out_path   = RAW_DIR / local_name
        log.file_download_start(filename=local_name)
        t0 = time.perf_counter()

        try:
            # The grid API checks Referer against the page the grid lives on,
            # not the Data Download tab.
            resp = session.post(GRID_URL, json=_grid_payload(spec),
                                headers={"Referer": spec["referer"]},
                                timeout=DOWNLOAD_TIMEOUT)
            resp.raise_for_status()
            body = _resolve_grid_body(session, resp, spec, log=log)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # Deliberately broad: the roster grid is a single optional
            # enrichment file on a second, less-predictable endpoint. A bad
            # response here must not discard an otherwise-successful catalog
            # scrape — log it, count it, carry on (docs/contributing.md §4).
            log.file_download_error(filename=local_name,
                                    error=f"{type(e).__name__}: {e}")
            err += 1
            continue

        if not body.strip():
            log.file_download_error(filename=local_name, error="empty response")
            err += 1
            continue

        if spec["ext"] == "xlsx":
            out_path.write_bytes(body)
            rows = _xlsx_row_count(out_path)
        else:
            rows = _write_csv_bytes(body, out_path)

        log.file_download_ok(filename=local_name,
                             bytes=out_path.stat().st_size, rows=rows,
                             duration_s=round(time.perf_counter() - t0, 2))
        upsert_manifest({
            "category":      spec["slug"],
            "year":          "",
            "source_file":   spec["grid_name"],
            "local_file":    local_name,
            "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
            "row_count":     rows,
        })
        ok += 1
        time.sleep(0.5)

    return ok, err


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
):
    """Download North Dakota campaign finance bulk exports.

    Vertical scope (mutually exclusive):
        force                 — wipe in-scope manifest rows, re-download everything
        start_year / end_year — restrict to that year range and re-download it

    Horizontal scope (additive):
        no flags        — everything in the catalog
        transactions    — Contributions + Expenditure (+ in-kind/loans if ND
                          ever publishes them separately)
        entities        — Registration
        contributions   — Contributions (+ in-kind) only
        expenditures    — Expenditure (+ independent expenditures/loans) only
        committees      — Registration only
        candidates      — Registration only (same export feeds both relations)

    FiledReports is pulled under every scope (GROUPS_ALWAYS); ReportingSchedule
    only on a full run (GROUPS_FULL_RUN_ONLY).

    Year-less exports, if CFRS ever emits one, are always re-fetched: there's no
    year to compare and registrations change continuously.
    """
    log = get_logger("north_dakota", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    # ── Resolve horizontal scope to a set of relation groups ──────────
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    groups: set[str] = set()
    if no_horizontal:
        groups = ({"contributions", "expenditures", "committees", "candidates"}
                  | GROUPS_ALWAYS | GROUPS_FULL_RUN_ONLY)
    else:
        if transactions:
            groups |= {"contributions", "expenditures"}
        if entities:
            groups |= {"committees", "candidates"}
        if contributions:
            groups.add("contributions")
        if expenditures:
            groups.add("expenditures")
        if committees:
            groups.add("committees")
        if candidates:
            groups.add("candidates")
        # Candidates come out of the same Registration export as committees.
        if candidates:
            groups.add("committees")
        groups |= GROUPS_ALWAYS

    current_year        = datetime.today().year
    current_year_str    = str(current_year)
    year_range_explicit = start_year is not None or end_year is not None
    # No default floor: the catalog defines what exists (see CFRS_EARLIEST_KNOWN_YEAR).
    range_start         = start_year
    # No default upper bound: CFRS forward-dates some exports — the 2026 filing
    # cycle's ReportingSchedule is published as ReportingSchedule_2027 because
    # the year-end report is due in January 2027. Capping at the current year
    # silently dropped it. The catalog itself defines the ceiling; --end-year
    # is still validated against the current year at the CLI, since that flag
    # is about transaction years.
    range_end           = end_year if end_year is not None else None

    files_ok = files_err = files_skipped = 0

    try:
        # Records the certificate fingerprint (and warns if it changed) before
        # any data is fetched. Advisory only — see the TLS notes at the top.
        fingerprint = check_server_fingerprint(log)
        log._emit("tls_fingerprint", host=HOST, sha256=fingerprint,
                  expected=EXPECTED_LEAF_SHA256, verify=VERIFY)

        session = _make_session()
        catalog = list_available(log, session)

        # ── Filter to scope ───────────────────────────────────────────
        wanted = []
        for e in catalog:
            if e["group"] not in groups:
                continue
            if e["year"]:
                yr = int(e["year"])
                if ((range_start is not None and yr < range_start)
                        or (range_end is not None and yr > range_end)):
                    continue
            wanted.append(e)

        # ── Manifest bookkeeping ──────────────────────────────────────
        if force:
            in_scope = {(e["category"], e["year"]) for e in wanted}
            strip_manifest(
                lambda r: (r.get("category", ""), r.get("year", "")) not in in_scope
            )
            done: dict[tuple[str, str], dict] = {}
        elif year_range_explicit:
            # Wipe in-range rows so the manifest is the sole source of truth
            # for the requested range (see docs/contributing.md §8).
            def _outside_range(r: dict) -> bool:
                try:
                    yr = int(r.get("year") or "")
                except ValueError:
                    return True          # year-less rows are always kept
                return ((range_start is not None and yr < range_start)
                        or (range_end is not None and yr > range_end))

            strip_manifest(_outside_range)
            done = load_manifest()
        else:
            done = load_manifest()

        for entry in wanted:
            key  = (entry["category"], entry["year"])
            prev = done.get(key)

            # Skip only when ND hasn't regenerated the file since last run.
            # Current-year and year-less exports are always re-fetched — they
            # are updated in place as filings come in.
            local = RAW_DIR / ((f"{entry['category']}_{entry['year']}.csv")
                               if entry["year"] else f"{entry['category']}.csv")
            unchanged = bool(prev) and prev.get("source_file") == entry["source_file"]
            if (unchanged and local.exists() and local.stat().st_size > 0
                    and entry["year"]
                    and entry["year"] != current_year_str
                    and not year_range_explicit):
                log.file_download_skip(filename=local.name)
                files_skipped += 1
                continue

            result = download_entry(log, session, entry)
            if result is None:
                files_err += 1
                continue

            local_name, row_count = result
            upsert_manifest({
                "category":      entry["category"],
                "year":          entry["year"],
                "source_file":   entry["source_file"],
                "local_file":    local_name,
                "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                "row_count":     row_count,
            })
            files_ok += 1
            time.sleep(0.5)   # be polite between signed-URL requests

        # ── DataGrid exports (separate endpoint, not in the catalog) ───
        grid_ok, grid_err = download_grid_exports(log, session, groups)
        files_ok  += grid_ok
        files_err += grid_err

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} errors, "
                 f"{files_skipped} skipped")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err, files_skipped=files_skipped)

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


# ================================ CLI =================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download North Dakota campaign finance data from the "
                    "CFRS Data Download API."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all files in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")

    ap.add_argument("--transactions",  action="store_true",
                    help="transactions only (contributions + expenditures)")
    ap.add_argument("--entities",      action="store_true",
                    help="entities only (committees + candidates)")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true", help="candidates only")
    ap.add_argument("--committees",    action="store_true", help="committees only")

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
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
