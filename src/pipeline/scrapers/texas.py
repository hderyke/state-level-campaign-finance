"""
scrapers/texas.py — Download the Texas Ethics Commission (TEC) bulk campaign
finance CSV database.

Plain `requests` — no Playwright, no API. TEC publishes its entire electronic
campaign finance archive (everything filed since July 1 2000) as a single
~1 GB zip that is regenerated periodically, plus two plain-text files
documenting the record layouts and code lists.

## Sources

    archive   https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip
    index     https://www.ethics.state.tx.us/search/cf/   (the page linking it)

The record-layout and code-list documentation (CFS-ReadMe.txt, CFS-Codes.txt)
also exists as standalone downloads, but it ships inside the archive too, so
it's extracted from there — that way it always describes the snapshot it came
with rather than a possibly newer one.

Note the archive host: the *current* download lives on `prd.tecprd.ethicsefile.com`,
TEC's filing-application host. The more obvious-looking
`www.ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip` is a **stale 2019
snapshot** — on the live index page that link is commented out and labelled
"As of 11/11/2019", while the active one points at the ethicsefile host. Using
the www URL would silently produce seven-year-old data, so it is recorded in
`LEGACY_ARCHIVE_URL` below purely as a documented do-not-use.

## Detecting a new publication

TEC's index page renders the archive's publication date directly in the link
text — "Campaign Finance CSV Database (As of 07/24/2026)". That string is the
most reliable freshness signal available (it's what TEC itself considers the
data's as-of date), so `published_date()` scrapes it and the manifest records
it. HTTP `Last-Modified` / `ETag` from a HEAD on the archive are recorded too
and used as a fallback when the page can't be read or its wording changes.

A no-flag run re-downloads only when the published date, ETag or Last-Modified
differs from what the manifest recorded, or when an expected member file is
missing from `data/Texas/raw/`. `--force` always re-downloads.

## What gets extracted

The archive holds 138 members and expands to roughly 9 GB. Only the members
the parser actually reads are extracted (see `MEMBER_GROUPS`); the rest —
pledges, notices, assets, travel, final reports — stay in the zip. Three
members are deliberately skipped even though they hold money:

    cont_ss.csv, cont_t.csv, expn_t.csv

Per TEC's own README these hold special-session and special pre-election
("Telegram") report rows that are *re-reported on the next regular campaign
finance report*, and TEC keeps them in separate files precisely so consumers
don't double-count. Parsing them would inflate every total.

The downloaded zip is kept after extraction so members can be re-extracted
(e.g. after widening `MEMBER_GROUPS`) without re-downloading a gigabyte.
Budget roughly 11 GB of free disk for a full Texas run.

## No year scoping

`--start-year` / `--end-year` are accepted (orc forwards them to every
scraper) but have no effect: the source is one monolithic archive with no
year dimension, and its `contribs_##` / `expend_##` shards are split by
internal report id, not by year — the same situation as California's single
bulk file. Horizontal flags do work, and scope which members get extracted.

Raw files (data/Texas/raw/):
  TEC_CF_CSV.zip    the downloaded archive itself, kept after extraction so
                    members can be re-extracted without re-downloading
  contribs_##.csv   102 shards — Schedules A/C contributions
  expend_##.csv     13 shards  — Schedules F/G/H/I expenditures
  filers.csv        filer index (candidates + committees)
  spacs.csv         specific-purpose committee -> candidate links
  cover.csv         report cover sheets — party, election date, office per report
  cand.csv          direct-campaign-expenditure beneficiary candidates
  loans.csv         Schedule E loans
  debts.csv         Schedule L outstanding loans
  credits.csv       Schedule K interest/credits/gains/refunds
  expn_catg.csv     expenditure category code list
  CFS-ReadMe.txt    record layouts (provenance)
  CFS-Codes.txt     code lists (provenance)
"""

import argparse
import csv
import fnmatch
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup, NavigableString

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
import config

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Texas" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Texas" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["item", "published", "etag", "last_modified", "bytes", "rows",
                 "downloaded_at"]

# ========================= state-specific constants ===================
ARCHIVE_URL = "https://prd.tecprd.ethicsefile.com/public/cf/public/TEC_CF_CSV.zip"
INDEX_URL   = "https://www.ethics.state.tx.us/search/cf/"
# Standalone copies of the two documentation files. Not fetched — the same
# files ship inside the archive and are extracted from there, so they always
# describe the snapshot they came with. Recorded for reference.
README_URL  = "https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt"
CODES_URL   = "https://www.ethics.state.tx.us/data/search/cf/CFS-Codes.txt"

# Do NOT switch to this. It is the pre-2019 snapshot TEC left in place when the
# live download moved to the ethicsefile host; the index page has it commented
# out and labelled "As of 11/11/2019". Kept here so the next person who finds
# it in a search engine and "fixes" the URL sees why not.
LEGACY_ARCHIVE_URL = "https://www.ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip"

ARCHIVE_NAME = "TEC_CF_CSV.zip"
ARCHIVE_PATH = RAW_DIR / ARCHIVE_NAME

CHUNK = 1024 * 1024   # 1 MiB streaming chunks

# Members to extract, grouped so the horizontal scope flags can select them.
# Patterns are fnmatch globs against the zip's member names.
#
# Deliberately absent: cont_ss.csv / cont_t.csv / expn_t.csv (special-session
# and special pre-election rows that TEC keeps separate because they are
# re-reported on the next regular report — extracting them would double-count),
# and cover/pledges/notices/assets/travel/finals, which the canonical schema
# has nowhere to put.
MEMBER_GROUPS = {
    "contributions": ["contribs_*.csv", "credits.csv"],
    "expenditures":  ["expend_*.csv", "expn_catg.csv", "cand.csv"],
    "loans":         ["loans.csv", "debts.csv"],
    # cover.csv (195 MB) is the report cover sheets. It's in the entities group
    # because it's the only file in the archive carrying a party
    # (politicalPartyCd), a real election date (electionDt) and the office as
    # declared on each report — all keyed on filerIdent, so the parser joins it
    # to the filer index on an exact key. cover_ss/cover_t are its
    # special-session and Telegram counterparts and are excluded for the same
    # duplicate-risk reason as cont_ss/cont_t/expn_t.
    "entities":      ["filers.csv", "spacs.csv", "cover.csv"],
    # Not data — TEC's own record-layout and code-list documentation, which
    # ships inside the archive. Always extracted (a few hundred KB) so the raw
    # directory is self-describing and the parser's field mapping can be
    # checked against the layout that shipped with the data it was run on.
    # Taken from the zip rather than from README_URL/CODES_URL so they're
    # guaranteed to describe *this* snapshot rather than a newer one.
    "docs":          ["CFS-ReadMe.txt", "CFS-Codes.txt"],
}

# Loans/debts ride along with whichever transaction scope is active — there's
# no --loans flag in the standard set, and they're 8 MB.
DEFAULT_GROUPS = ["docs", "entities", "contributions", "expenditures", "loans"]

HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# ============================ http session ============================
def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


# ========================= manifest helpers ============================
def load_manifest() -> dict[str, dict]:
    """item -> manifest row."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {row["item"]: row for row in csv.DictReader(f)}


def write_manifest(rows: dict[str, dict]) -> None:
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        for item in sorted(rows):
            w.writerow(rows[item])


def upsert_manifest(rows: dict[str, dict], record: dict) -> None:
    rows[record["item"]] = record
    write_manifest(rows)


# ======================= freshness / discovery =========================
def published_date(session: requests.Session) -> str:
    """Publication date TEC advertises next to the archive link, as YYYY-MM-DD.

    The index page renders it in the link text — "Campaign Finance CSV Database
    (As of 07/24/2026)". Returns '' if the page can't be read or the wording
    has changed; callers fall back to the HTTP validators, so a miss degrades
    to "re-download" rather than to "silently stale"."""
    try:
        resp = session.get(INDEX_URL, timeout=60)
        resp.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        if not a["href"].lower().endswith(ARCHIVE_NAME.lower()):
            continue
        # Only trust a link pointing at the live host — the stale www copy is
        # commented out today, but a future edit could uncomment it.
        if "ethicsefile.com" not in a["href"] and "prd.tec" not in a["href"]:
            continue
        m = re.search(r"as of\s+(\d{1,2})/(\d{1,2})/(\d{4})", a.get_text(), re.IGNORECASE)
        if m:
            mm, dd, yyyy = m.groups()
            return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
    return ""


def head_validators(session: requests.Session) -> tuple[str, str, str]:
    """(etag, last_modified, content_length) for the archive, '' where absent."""
    try:
        resp = session.head(ARCHIVE_URL, timeout=60, allow_redirects=True)
        resp.raise_for_status()
    except Exception:
        return "", "", ""
    h = resp.headers
    return (h.get("ETag", "").strip('"'),
            h.get("Last-Modified", ""),
            h.get("Content-Length", ""))


# ============================== download ===============================
def download_archive(session: requests.Session, log) -> int:
    """Stream the archive to disk. Returns bytes written.

    Written to `.part` and renamed on success: a gigabyte download interrupted
    halfway would otherwise leave a truncated file that looks complete to the
    next run's existence check, and zipfile would fail on it much later, during
    extraction, with a confusing error."""
    part = ARCHIVE_PATH.with_suffix(ARCHIVE_PATH.suffix + ".part")
    total = 0
    with session.get(ARCHIVE_URL, stream=True, timeout=(30, 300)) as resp:
        resp.raise_for_status()
        expected = int(resp.headers.get("Content-Length") or 0)
        with open(part, "wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK):
                if not chunk:
                    continue
                f.write(chunk)
                total += len(chunk)
    if expected and total != expected:
        part.unlink(missing_ok=True)
        raise IOError(f"{ARCHIVE_NAME}: expected {expected} bytes, got {total}")
    part.replace(ARCHIVE_PATH)
    return total


# ============================== extract ================================
def select_members(zf: zipfile.ZipFile, groups: list[str]) -> list[str]:
    """Member names in the archive matching any pattern in the given groups."""
    patterns = [p for g in groups for p in MEMBER_GROUPS.get(g, [])]
    names = zf.namelist()
    selected = []
    for name in names:
        if any(fnmatch.fnmatch(name, pat) for pat in patterns):
            selected.append(name)
    return sorted(selected)


def extract_member(zf: zipfile.ZipFile, name: str, dest: Path) -> tuple[int, int]:
    """Stream one member out of the archive. Returns (bytes, data_rows).

    Rows are counted from the same stream rather than by re-reading the file
    afterwards — the contribs shards are ~100 MB each and there are 102 of
    them, so a second pass just to count lines would double the I/O for a
    number that only lands in the manifest as a sanity figure.

    The count is physical lines, so it runs slightly *over* the record count
    wherever TEC quotes a description containing a newline (measured: 232,769
    lines vs 230,242 records in expend_13.csv, +1.1%), and one *under* on a
    file with no trailing newline. Don't treat it as exact; the parser's own
    per-file counts are."""
    n_bytes = 0
    n_lines = 0
    with zf.open(name) as src, open(dest, "wb") as out:
        while True:
            chunk = src.read(CHUNK)
            if not chunk:
                break
            out.write(chunk)
            n_bytes += len(chunk)
            n_lines += chunk.count(b"\n")
    # minus the header row; a file with no trailing newline undercounts by one,
    # which is close enough for a manifest sanity figure.
    return n_bytes, max(n_lines - 1, 0)


# ============================== run ====================================
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
    """Download and unpack the TEC bulk campaign finance archive.

    Vertical scope:
        --force                 re-download the archive even if unchanged
        --start-year/--end-year accepted and ignored — the archive has no year
                                dimension (see the module docstring)

    Horizontal scope (selects which members are extracted; the archive itself
    is one file and is always downloaded in full when it has changed):
        (no flag)               everything the parser reads
        --transactions          contributions + expenditures + loans/debts
        --contributions         contribs_*.csv + credits.csv
        --expenditures          expend_*.csv + expn_catg.csv + cand.csv
        --entities              filers.csv + spacs.csv
        --candidates            same as --entities (one filer index covers both)
        --committees            same as --entities
    """
    log = get_logger("texas", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Texas scraper")
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year,
              entities=entities, transactions=transactions,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    if start_year is not None or end_year is not None:
        log.warning("  --start-year/--end-year have no effect for TX — the source "
                    "is a single archive with no year dimension")

    files_ok = files_err = 0

    # ── Resolve horizontal scope ───────────────────────────────────────
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)
    if no_horizontal:
        groups = list(DEFAULT_GROUPS)
    else:
        groups = ["docs"]
        if entities or candidates or committees:
            groups.append("entities")
        if transactions:
            groups += ["contributions", "expenditures", "loans"]
        else:
            if contributions:
                groups += ["contributions", "loans"]
            if expenditures:
                groups += ["expenditures", "loans"]
        groups = list(dict.fromkeys(groups))   # de-dupe, keep order

    try:
        session  = build_session()
        manifest = load_manifest()
        today    = time.strftime("%Y-%m-%d")

        # ── Is the archive we already have still current? ──────────────
        published = published_date(session)
        etag, last_mod, content_len = head_validators(session)
        if published:
            log.info(f"  TEC advertises the archive as of {published}")
        else:
            log.warning("  Could not read the 'As of' date from the index page — "
                        "falling back to HTTP validators")

        prev = manifest.get(ARCHIVE_NAME, {})
        # The three freshness signals are ranked, not OR-ed. TEC's own
        # published date is authoritative when we have it: if it has moved, the
        # archive is stale no matter what the HTTP validators say (a CDN
        # serving a weak or constant ETag would otherwise pin us to an old copy
        # forever). Only when the index page can't be read do the validators
        # get a vote, and then either one matching is good enough.
        if not (prev and ARCHIVE_PATH.exists()):
            archive_current = False
        elif published:
            archive_current = prev.get("published") == published
        else:
            archive_current = bool(
                (etag and prev.get("etag") == etag)
                or (last_mod and prev.get("last_modified") == last_mod)
            )

        if force:
            # Wipe the member entries so a forced run can't inherit stale row
            # counts, and so a member TEC has since removed disappears from the
            # manifest instead of being "expected" forever.
            manifest = {k: v for k, v in manifest.items() if k == ARCHIVE_NAME}

        # ── Download, unless what we already have is current ────────────
        if archive_current and not force:
            log.file_download_skip(filename=ARCHIVE_NAME)
            log.info(f"  Archive on disk is already the {prev.get('published') or 'current'} "
                     f"publication — not re-downloading")
        else:
            log.file_download_start(filename=ARCHIVE_NAME)
            try:
                size_gb = int(content_len) / 1e9
                log.info(f"  Downloading ~{size_gb:.1f} GB — this takes a while")
            except (TypeError, ValueError):
                log.info("  Downloading (server sent no Content-Length) — this takes a while")
            t_file = time.perf_counter()
            n_bytes = download_archive(session, log)
            log.file_download_ok(filename=ARCHIVE_NAME, bytes=n_bytes, rows=0,
                                 duration_s=round(time.perf_counter() - t_file, 2))
            files_ok += 1
            upsert_manifest(manifest, {
                "item": ARCHIVE_NAME, "published": published, "etag": etag,
                "last_modified": last_mod, "bytes": n_bytes, "rows": 0,
                "downloaded_at": today,
            })
            # A fresh archive invalidates every extracted member.
            manifest = {k: v for k, v in manifest.items() if k == ARCHIVE_NAME}

        # ── Extract ────────────────────────────────────────────────────
        # Extraction is decided per member against the *current* scope, not
        # against whatever the last run happened to extract. That's what makes
        # `--entities` followed by a plain run do the right thing (the plain run
        # extracts the transaction files it's missing) and what delivers the
        # "re-extract without re-downloading a gigabyte" the module docstring
        # promises — the zip on disk is reopened whether or not it was just
        # fetched.
        with zipfile.ZipFile(ARCHIVE_PATH) as zf:
            members = select_members(zf, groups)
            if not members:
                raise RuntimeError(
                    f"No members in {ARCHIVE_NAME} matched the selected groups "
                    f"{groups}. TEC has most likely renamed its files — compare "
                    f"MEMBER_GROUPS against: {sorted(zf.namelist())[:10]} ..."
                )

            def _needs_extract(member: str) -> bool:
                dest = RAW_DIR / Path(member).name
                if force or dest.name not in manifest:
                    return True
                return not (dest.exists() and dest.stat().st_size > 0)

            todo = [m for m in members if _needs_extract(m)]
            log.info(f"  {len(members)} members in scope, {len(todo)} to extract "
                     f"({len(members) - len(todo)} already present)")

            for name in members:
                if name not in todo:
                    log.file_download_skip(filename=Path(name).name)
                    continue
                dest = RAW_DIR / Path(name).name
                log.file_download_start(filename=dest.name)
                t_file = time.perf_counter()
                try:
                    m_bytes, m_rows = extract_member(zf, name, dest)
                except Exception as e:
                    log.file_download_error(filename=dest.name, error=str(e))
                    files_err += 1
                    continue
                log.file_download_ok(filename=dest.name, bytes=m_bytes, rows=m_rows,
                                     duration_s=round(time.perf_counter() - t_file, 2))
                files_ok += 1
                manifest[dest.name] = {
                    "item": dest.name, "published": published, "etag": "",
                    "last_modified": "", "bytes": m_bytes, "rows": m_rows,
                    "downloaded_at": today,
                }
            write_manifest(manifest)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} files, {files_err} errors")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err, published=published)

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


# =============== Party/office enrichment sources (--party) ===============
# Download the external party sources TEC's own cover.csv leaves unresolved
# for Texas candidates.
#
# Why this exists
# ----------------
# The scrape above already reads `politicalPartyCd` off TEC's `cover.csv`
# (Record #4, CoverSheet1Data) and parsers/texas.py joins it to candidates by
# `filerIdent` — an exact key, no name matching. But TEC's own Form C/OH
# cover sheet (the one candidate/officeholder filers actually submit) has no
# party box, and `CFS-Codes.txt` defines no party code list, so whether that
# field is ever populated for a CANDIDATE filer (as opposed to a
# party-committee filer — CEC/MCEC/PTYCORP) is a live question the parser
# measures and logs on every run rather than assumes. This mode is the
# fallback for whatever that join leaves blank. It talks to entirely
# different hosts on a different cadence than the TEC archive above, both
# sources are optional, and a failure on either must never take down the
# 9 GB TEC pull. Run via `--party` (see the CLI block below).
#
# Sources, tried by parsers/texas.py's TXEnrichment in this priority order
# ---------------------------------------------------------------------------
# 1. Texas Secretary of State — https://elections.sos.state.tx.us/
#
#    The SOS's own legacy canvass site, covering 1992-2019. Confirmed live
#    and scraped, not guessed: `index.htm` carries a `<select>` of ~170
#    election names, each mapping to a numeric `eleid`; the "Statewide Race
#    Summary" for election `N` is a static file at `elchist{N}_state.htm` —
#    a real HTML `<table>` with columns RACE | NAME | PARTY | CANVASS VOTES |
#    PERCENT, one header row per race followed by one row per candidate
#    (verified against the 2018 General Election, eleid 331: "State
#    Representative District 1", "Ted Cruz(I) / REP / 4,260,553 / 50.89%",
#    etc.). This is the one of the two sources that speaks with TEC's own
#    state government voice and covers *every* candidate on a
#    statewide-summary ballot line, winners and losers alike — not just
#    currently-serving officeholders. Its ceiling is 2019: SOS's *next*
#    results system (results.texas-election.com, 2019-2024) and the current
#    one (goelect.txelections.civixapps.com, 2025-) are both JavaScript SPAs
#    whose XHR endpoints are not captured here.
#
#    The state-summary table only carries statewide, congressional, State
#    Senate/House, State Board of Education and appellate-judicial races —
#    the races significant enough to canvass at the state level.
#    District-court, county-court and county-office races (the bulk of
#    TEC's `JUDGEDIST`, `JUDGESTATCO`, `DISTATTY` filers) are certified
#    county-by-county and are not in this file at all, the same structural
#    ceiling New York's own election-results enrichment runs into for local
#    offices.
#
# 2. Open States nightly bulk CSV — https://data.openstates.org/people/current/tx.csv
#
#    CC0, no API key required — the same unauthenticated export New York's
#    enrichment already uses. Covers only *current* Texas Legislature
#    members (House + Senate) with their sitting party. It adds no
#    historical depth beyond source 1, and it covers no statewide executive
#    or judicial office at all, but it is an independent read on the same
#    fact for whoever it does cover, and the only source here that speaks to
#    incumbency directly rather than by inferring it from a prior win. If
#    the export is unreachable, this source is skipped with a warning,
#    exactly as the SOS source degrades when unreachable — no source here is
#    ever load-bearing for the scrape to succeed.
#
# A third source, The Green Papers (thegreenpapers.com/G{YY}/TX), was tried
# and removed: its live markup doesn't match a stable, parseable contract
# (office section titles and candidate lines share the same flat
# `<li>`/`<p>` list with no consistent markers, and the per-line format
# itself differs between already-decided cycles and upcoming ones), so it
# kept silently degrading to 0 rows rather than surfacing a real signal. Not
# worth carrying for statewide coverage source 1 already gets a version of,
# at 2019 and earlier, anyway.
#
# Raw files written (data/Texas/raw/):
#   SOS_RaceSummary.csv       one row per candidate per race per election, 1992-2019
#   OpenStates_People.csv     one row per currently-serving TX legislator
#
# Both are optional. parsers/texas.py's TXEnrichment treats a missing file as
# "this source has nothing to say" and degrades to leaving `party` blank, the
# same way parsers/texas.py already degrades when `cover.csv` itself is
# absent.

PARTY_RAW_DIR  = RAW_DIR
PARTY_MANIFEST = PROJECT_ROOT / "data" / "Texas" / "party_manifest.csv"
PARTY_MANIFEST_COLS = ["source", "filename", "row_count", "fetched_at"]

PARTY_REQUEST_PAUSE = 0.3   # polite delay between requests to any one state site

# ---- output schemas ----
SOS_COLS = [
    "eleid", "election_name", "election_year", "stage",
    "race_raw", "office", "district",
    "candidate_name", "incumbent_flag", "party", "votes",
]

PARTY_OPENSTATES_COLS = [
    "openstates_id", "name", "given_name", "family_name",
    "party", "chamber", "district",
]


def party_clean(val) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


def _party_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": config.USER_AGENT, "Accept": "text/html,*/*"})
    return s


def _party_get(sess: requests.Session, url: str, retries: int = 3,
               timeout: int = 60, **kwargs) -> requests.Response | None:
    """GET with linear backoff. None on any failure — callers treat a miss as
    'no data here', never as a reason to abort the whole scrape."""
    for attempt in range(retries):
        try:
            resp = sess.get(url, timeout=timeout, **kwargs)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp
        except Exception:                              # noqa: BLE001
            time.sleep(1.5 * (attempt + 1))
    return None


# ---- source 1: Texas Secretary of State legacy canvass site (1992-2019) ----
SOS_SITE = "https://elections.sos.state.tx.us"

# TEC's electronic archive begins 2000-07-01 (see docs/states/texas.md); SOS's
# legacy site's own last entries are 2019 specials. Elections outside that
# span cannot join to any TEC filer and are skipped rather than fetched.
SOS_EARLIEST_YEAR = 2000
SOS_LATEST_YEAR   = 2019

_PARTY_YEAR_RE  = re.compile(r"\b(19|20)\d{2}\b")
_PARTY_STAGE_RE = [
    (re.compile(r"runoff", re.IGNORECASE),      "runoff"),
    (re.compile(r"primary", re.IGNORECASE),     "primary"),
    (re.compile(r"special", re.IGNORECASE),     "special"),
    (re.compile(r"constitutional amendment", re.IGNORECASE), "constitutional_amendment"),
]


def _election_stage(name: str) -> str:
    for rx, label in _PARTY_STAGE_RE:
        if rx.search(name):
            return label
    return "general"


def _own_text(tag) -> str:
    """A tag's own label text, ignoring anything from a nested descendant
    tag — needed because `discover_elections` below can't use `get_text()`.

    The live SOS `<option>` markup has no closing `</option>` tags (each
    option's HTML is just `<option value="N">Some Election Name` running
    straight into the next `<option ...>`). A browser or an HTML5-compliant
    parser (lxml, html5lib) auto-closes each `<option>` the instant it hits
    the next one. Python's stdlib `html.parser` — what `BeautifulSoup(html,
    "html.parser")` uses here — does not apply that implied-end-tag rule for
    `<option>`, so instead of closing option N it nests option N+1 *inside*
    option N, which nests N+2 inside N+1, and so on to the end of the
    `<select>`. `find_all("option")` still returns all of them (nesting
    doesn't hide a tag from a descendant search), but `opt.get_text()` walks
    every descendant, so option N's text comes back as option N's own label
    plus every option after it concatenated on — confirmed on a live run:
    each successive "sample raw name" in `scrape_sos`'s warning was the same
    giant string with one more election's worth of text sliced off the
    front. Only an option's direct `NavigableString` children are its own
    label; anything else in `.contents` is the next nested `<option>` tag.
    """
    return party_clean("".join(
        c for c in tag.contents if isinstance(c, NavigableString)))


def discover_elections(sess: requests.Session, log) -> list[tuple[str, str]]:
    """[(eleid, election_name), ...] from index.htm's election picker.

    The election `<select>` is identified as whichever `<select>` on the page
    has the most `<option>`s — the site's own second control (report type:
    Statewide/County/Canvass/Local) only ever has four, so this is robust
    without depending on either control's `name`/`id` attribute, which are not
    part of any documented contract.

    Each option's name comes from `_own_text()`, not `opt.get_text()` — see
    that helper for why. A defensive fallback also handles the year not
    being on an option's own text at all (some labels repeat it, e.g. "2018
    General Election"; plenty don't, e.g. "Special Runoff Election, House
    District 124"): if the option sits under a real `<optgroup label="YYYY">`,
    or is itself an inert bare-"YYYY" divider option with no real `eleid`,
    that year is tracked and prefixed onto whichever later option text still
    lacks one. Neither has been observed live — the unclosed-tag nesting is
    the confirmed cause — but they cost nothing to keep handling in case the
    markup changes again.
    """
    resp = _party_get(sess, f"{SOS_SITE}/index.htm")
    if resp is None:
        log.warning("  SOS index.htm unreachable — skipping SOS source entirely")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    selects = soup.find_all("select")
    if not selects:
        log.warning("  SOS index.htm has no <select> — page layout may have "
                    "changed; skipping SOS source")
        return []
    picker = max(selects, key=lambda s: len(s.find_all("option")))
    out = []
    year_ctx = ""
    for opt in picker.find_all("option"):
        eleid = party_clean(opt.get("value"))
        name  = _own_text(opt)
        if not name:
            continue

        optgroup = opt.find_parent("optgroup")
        group_label = party_clean(optgroup.get("label")) if optgroup else ""
        if group_label:
            year_ctx = group_label
        elif not eleid and _PARTY_YEAR_RE.fullmatch(name):
            # An inert "YYYY" divider option, not a real election — track it
            # as context for the options that follow and drop it.
            year_ctx = name
            continue

        if year_ctx and not _PARTY_YEAR_RE.search(name):
            name = f"{year_ctx} {name}".strip()
        if eleid and name:
            out.append((eleid, name))
    return out


def in_scope(election_name: str) -> bool:
    m = _PARTY_YEAR_RE.search(election_name)
    if not m:
        return False
    year = int(m.group(0))
    if not (SOS_EARLIEST_YEAR <= year <= SOS_LATEST_YEAR):
        return False
    return "constitutional amendment" not in election_name.lower()


# Only offices office_types.csv actually maps for TX are worth carrying —
# matching against a race this pipeline can never join to a TEC candidate
# (US House/Senate; TEC has no federal filers) just bloats the file.
_PARTY_OFFICE_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"lieutenant governor", re.IGNORECASE),            "Lt. Governor"),
    (re.compile(r"^governor\b", re.IGNORECASE),                    "Governor"),
    (re.compile(r"attorney general", re.IGNORECASE),               "Attorney General"),
    (re.compile(r"comptroller", re.IGNORECASE),                    "State Comptroller"),
    (re.compile(r"general land office", re.IGNORECASE),            "Commissioner of Public Lands"),
    (re.compile(r"commissioner of agriculture", re.IGNORECASE),    "Commissioner of Agriculture"),
    (re.compile(r"railroad commissioner", re.IGNORECASE),          "Public Utility Commissioner"),
    (re.compile(r"state board of education", re.IGNORECASE),      "State Board of Education"),
    (re.compile(r"state senator", re.IGNORECASE),                  "State Senator"),
    (re.compile(r"state representative", re.IGNORECASE),          "State Representative"),
    (re.compile(r"court of criminal appeals", re.IGNORECASE),      "State Supreme Court Justice"),
    (re.compile(r"supreme court", re.IGNORECASE),                  "State Supreme Court Justice"),
    (re.compile(r"court of appeals", re.IGNORECASE),               "Court of Appeals Judge"),
]
_PARTY_DISTRICT_RE = re.compile(r"district\s+(\d+)", re.IGNORECASE)
_PARTY_PLACE_RE    = re.compile(r"place\s+(\d+)", re.IGNORECASE)


def canonical_race_office(race_raw: str) -> tuple[str, str]:
    """(canonical_office, district) or ("", "") when this race isn't one
    office_types.csv maps for TX — U.S. House/Senate chiefly, since TEC has no
    federal filers to ever match them against."""
    text = party_clean(race_raw).rstrip("-").strip()
    office = ""
    for rx, label in _PARTY_OFFICE_PATTERNS:
        if rx.search(text):
            office = label
            break
    if not office:
        return "", ""

    dm = _PARTY_DISTRICT_RE.search(text)
    pm = _PARTY_PLACE_RE.search(text)
    if dm and pm:
        district = f"{dm.group(1)} Place {pm.group(1)}"
    elif pm:
        district = f"Place {pm.group(1)}"
    elif dm:
        district = dm.group(1)
    else:
        district = ""
    return office, district


_PARTY_INCUMBENT_RE = re.compile(r"\(I\)\s*$")


def parse_state_summary(html: str) -> list[dict]:
    """Race blocks -> candidate rows from one elchist{N}_state.htm page.

    The table has no rowspans: a race's name appears once, in the RACE
    column, on a row whose NAME/PARTY/VOTES cells are all empty; every
    following row until the next non-blank RACE cell is one candidate,
    ending with a "Race Total" row that is not a candidate. Section-divider
    rows (a run of dashes in the RACE cell) and any race this pipeline can't
    map to a TX office (U.S. House/Senate) are skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows_out: list[dict] = []
    current_race = ""
    current_office = ""
    current_district = ""

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [party_clean(td.get_text(" ")) for td in tr.find_all(["td", "th"])]
            if not cells or not any(cells):
                continue
            race_cell = cells[0] if len(cells) > 0 else ""
            name_cell = cells[1] if len(cells) > 1 else ""
            party_cell = cells[2] if len(cells) > 2 else ""
            votes_cell = cells[3] if len(cells) > 3 else ""

            if re.fullmatch(r"-{5,}", race_cell):
                continue   # section divider

            if race_cell and race_cell.upper() != "RACE":
                current_race = race_cell
                current_office, current_district = canonical_race_office(race_cell)
                continue

            if not current_race:
                continue
            if not name_cell or party_cell.strip().upper() in ("", "RACE TOTAL"):
                continue
            if not current_office:
                continue   # a race we can't map to any TX office — e.g. U.S. House/Senate

            incumbent = "1" if _PARTY_INCUMBENT_RE.search(name_cell) else "0"
            name = _PARTY_INCUMBENT_RE.sub("", name_cell).strip()

            rows_out.append({
                "race_raw":        current_race,
                "office":          current_office,
                "district":        current_district,
                "candidate_name":  name,
                "incumbent_flag":  incumbent,
                "party":           party_cell.strip(),
                "votes":           votes_cell.replace(",", "").strip(),
            })
    return rows_out


def scrape_sos(sess: requests.Session, log) -> int:
    elections = discover_elections(sess, log)
    if not elections:
        return 0
    in_scope_elections = [(eid, name) for eid, name in elections if in_scope(name)]
    log.info(f"  SOS index: {len(elections):,} elections listed, "
             f"{len(in_scope_elections):,} in scope "
             f"({SOS_EARLIEST_YEAR}-{SOS_LATEST_YEAR}, non-amendment)")
    if elections and not in_scope_elections:
        log.warning(
            "  SOS: every listed election was rejected as out-of-scope — "
            "this is the same failure mode as the year never being visible "
            "on any option (see discover_elections docstring); sample raw "
            f"names seen: {[name for _, name in elections[:5]]!r}")

    out_rows: list[dict] = []
    n_ok = n_err = 0
    for eleid, name in in_scope_elections:
        resp = _party_get(sess, f"{SOS_SITE}/elchist{eleid}_state.htm")
        time.sleep(PARTY_REQUEST_PAUSE)
        if resp is None:
            n_err += 1
            continue
        year = _PARTY_YEAR_RE.search(name)
        stage = _election_stage(name)
        try:
            parsed = parse_state_summary(resp.text)
        except Exception as e:                        # noqa: BLE001
            log.warning(f"  elchist{eleid}_state.htm failed to parse: {e}")
            n_err += 1
            continue
        for row in parsed:
            row["eleid"] = eleid
            row["election_name"] = name
            row["election_year"] = year.group(0) if year else ""
            row["stage"] = stage
        out_rows.extend(parsed)
        n_ok += 1
        if n_ok % 25 == 0:
            log.info(f"  SOS: {n_ok:,}/{len(in_scope_elections):,} election pages "
                     f"fetched, {len(out_rows):,} candidate rows so far")

    if n_err:
        log.warning(f"  SOS: {n_err:,} of {len(in_scope_elections):,} election "
                    f"pages could not be fetched/parsed and were skipped")

    out = RAW_DIR / "SOS_RaceSummary.csv"
    part = out.with_suffix(".csv.part")
    with open(part, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SOS_COLS, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(out_rows)
    part.replace(out)
    return len(out_rows)


# ---- source 2: Open States nightly bulk CSV (current TX legislators) ----
# https://data.openstates.org/people/current/tx.csv — the same CC0,
# no-key-required nightly export scrapers/new_york.py's --party mode already
# uses in place of the v3 REST API, for the same reason: v3 needs an
# OPENSTATES_API_KEY, which would make this source depend on a per-user
# credential nobody else running this pipeline has.
PARTY_OPENSTATES_CURRENT = "https://data.openstates.org/people/current/tx.csv"

# The CSV export uses the same upper/lower classification the v3 API's
# current_role.org_classification did.
_PARTY_OS_CHAMBERS = {"upper": "State Senator", "lower": "State Representative"}


def _party_iter_csv(text: str):
    """Yield dict rows from CSV text, tolerating a UTF-8 BOM."""
    return csv.DictReader((text or "").lstrip("﻿").splitlines())


def scrape_openstates_party(sess: requests.Session, log) -> int:
    """Write OpenStates_People.csv from the nightly CC0 bulk CSV.

    No API key needed — see module note above. Column names in the export
    are current_party/current_district/current_chamber; translated here to
    this module's existing party/chamber/district output columns so
    parsers/texas.py's TXEnrichment needs no changes.
    """
    resp = _party_get(sess, PARTY_OPENSTATES_CURRENT, timeout=120)
    if resp is None or not resp.text.strip():
        log.warning("  Open States bulk CSV unavailable — skipping this source")
        return 0

    rows: list[dict] = []
    for src in _party_iter_csv(resp.text):
        name = party_clean(src.get("name"))
        if not name:
            continue
        chamber = _PARTY_OS_CHAMBERS.get(party_clean(src.get("current_chamber")).lower(), "")
        party = party_clean(src.get("current_party"))
        if not (chamber and party):
            continue
        district = party_clean(src.get("current_district"))
        rows.append({
            "openstates_id": party_clean(src.get("id")),
            "name":          name,
            "given_name":    party_clean(src.get("given_name")),
            "family_name":   party_clean(src.get("family_name")),
            "party":         party,
            "chamber":       chamber,
            "district":      district.lstrip("0") or district,
        })

    out = RAW_DIR / "OpenStates_People.csv"
    part = out.with_suffix(".csv.part")
    with open(part, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PARTY_OPENSTATES_COLS,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(rows)
    part.replace(out)
    return len(rows)


def _party_upsert_manifest(record: dict) -> None:
    existing = []
    if PARTY_MANIFEST.exists():
        with open(PARTY_MANIFEST, newline="", encoding="utf-8") as f:
            existing = [r for r in csv.DictReader(f)
                       if r.get("source") != record["source"]]
    with open(PARTY_MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PARTY_MANIFEST_COLS,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(existing)
        w.writerow(record)


def run_party(sos: bool = False, openstates: bool = False):
    """Download Texas party-enrichment sources (the --party CLI mode).

    Horizontal scope (additive; no flags = both, tried in the priority order
    parsers/texas.py's TXEnrichment applies them):
        --sos           Texas SOS legacy race-summary canvass (1992-2019)
        --openstates    Open States bulk CSV, current TX legislators
    """
    log = get_logger("texas", "scrape")
    t0 = time.perf_counter()
    log.info("Starting Texas party-enrichment scraper")
    log._emit("scrape_started", source="texas_party",
              sos=sos, openstates=openstates)

    do_sos = sos or not (sos or openstates)
    do_os  = openstates or not (sos or openstates)
    sess = _party_session()
    n_sos = n_os = 0
    today = time.strftime("%Y-%m-%d")

    try:
        if do_sos:
            log.info("Texas Secretary of State (elections.sos.state.tx.us)")
            n_sos = scrape_sos(sess, log)
            _party_upsert_manifest({"source": "sos", "filename": "SOS_RaceSummary.csv",
                                    "row_count": n_sos, "fetched_at": today})
            log.info(f"  wrote SOS_RaceSummary.csv ({n_sos:,} rows)")

        if do_os:
            log.info("Open States bulk CSV (data.openstates.org)")
            n_os = scrape_openstates_party(sess, log)
            _party_upsert_manifest({"source": "openstates", "filename": "OpenStates_People.csv",
                                    "row_count": n_os, "fetched_at": today})
            log.info(f"  wrote OpenStates_People.csv ({n_os:,} rows)")

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  source="texas_party", sos_rows=n_sos, openstates_rows=n_os)

    except KeyboardInterrupt:
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  source="texas_party", sos_rows=n_sos, openstates_rows=n_os)
        raise
    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  source="texas_party", sos_rows=n_sos, openstates_rows=n_os,
                  error_type=type(e).__name__, error=str(e))
        raise


# ================================ CLI ==================================
if __name__ == "__main__":
    if "--party" in sys.argv:
        # Party/office enrichment mode (run_party() above) — different hosts,
        # different flags, so it gets its own small parser rather than being
        # folded into the one below.
        pp = argparse.ArgumentParser(
            description="Download Texas party-enrichment sources (SOS legacy "
                        "canvass + Open States bulk CSV).")
        pp.add_argument("--party", action="store_true", help=argparse.SUPPRESS)
        pp.add_argument("--sos", action="store_true", help="Texas SOS legacy race summary only")
        pp.add_argument("--openstates", action="store_true", help="Open States bulk CSV only")
        pargs, _ = pp.parse_known_args()
        try:
            run_party(sos=pargs.sos, openstates=pargs.openstates)
        except KeyboardInterrupt:
            sys.exit(130)
        except Exception:
            sys.exit(1)
        sys.exit(0)

    ap = argparse.ArgumentParser(
        description="Download the Texas Ethics Commission bulk campaign finance archive."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download the archive even if TEC hasn't republished it")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="accepted for CLI consistency; has no effect (single archive)")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="accepted for CLI consistency; has no effect (single archive)")

    ap.add_argument("--transactions",  action="store_true",
                    help="contributions + expenditures + loans/debts")
    ap.add_argument("--contributions", action="store_true", help="contribs_*.csv + credits.csv")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expend_*.csv + expn_catg.csv + cand.csv")
    ap.add_argument("--entities",      action="store_true", help="filers.csv + spacs.csv")
    ap.add_argument("--candidates",    action="store_true",
                    help="same as --entities (one filer index covers both)")
    ap.add_argument("--committees",    action="store_true", help="same as --entities")

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
