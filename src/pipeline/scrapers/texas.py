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

import csv
import fnmatch
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

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


# ================================ CLI ==================================
if __name__ == "__main__":
    import argparse

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
