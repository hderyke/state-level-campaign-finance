"""
scrapers/texas_party.py — Download the external party sources TEC's own
cover.csv leaves unresolved for Texas candidates.

Why this exists
----------------
`scrapers/texas.py` + `parsers/texas.py` already read `politicalPartyCd` off
TEC's `cover.csv` (Record #4, CoverSheet1Data) and join it to candidates by
`filerIdent` — an exact key, no name matching. But TEC's own Form C/OH cover
sheet (the one candidate/officeholder filers actually submit) has no party
box, and `CFS-Codes.txt` defines no party code list, so whether that field is
ever populated for a CANDIDATE filer (as opposed to a party-committee filer —
CEC/MCEC/PTYCORP) is a live question the parser measures and logs on every
run rather than assumes. This module is the fallback for whatever `party`
that join leaves blank. It is a *separate* scraper from `scrapers/texas.py`
on purpose: unrelated hosts, none of them TEC, both optional, and a failure
on either of them must never take down the 9 GB TEC pull.

Sources, tried by `parsers/texas_enrich.py` in this priority order
-------------------------------------------------------------------
1. **Texas Secretary of State** — https://elections.sos.state.tx.us/

   The SOS's own legacy canvass site, covering 1992-2019. Confirmed live and
   scraped, not guessed: `index.htm` carries a `<select>` of ~170 election
   names, each mapping to a numeric `eleid`; the "Statewide Race Summary" for
   election `N` is a static file at `elchist{N}_state.htm` — a real HTML
   `<table>` with columns RACE | NAME | PARTY | CANVASS VOTES | PERCENT, one
   header row per race followed by one row per candidate (verified against
   the 2018 General Election, eleid 331: "State Representative District 1",
   "Ted Cruz(I) / REP / 4,260,553 / 50.89%", etc.). This is the one of the
   two sources that speaks with TEC's own state government voice and covers
   *every* candidate on a statewide-summary ballot line, winners and losers
   alike — not just currently-serving officeholders. Its ceiling is
   2019: SOS's *next* results system (results.texas-election.com, 2019-2024)
   and the current one (goelect.txelections.civixapps.com, 2025-) are both
   JavaScript SPAs whose XHR endpoints are not captured here.

   The state-summary table only carries statewide, congressional, State
   Senate/House, State Board of Education and appellate-judicial races — the
   races significant enough to canvass at the state level. District-court,
   county-court and county-office races (the bulk of TEC's `JUDGEDIST`,
   `JUDGESTATCO`, `DISTATTY` filers) are certified county-by-county and are
   not in this file at all, the same structural ceiling New York's own
   election-results enrichment runs into for local offices.

2. **Open States nightly bulk CSV** — https://data.openstates.org/people/current/tx.csv

   CC0, no API key required — the same unauthenticated export New York's
   enrichment already uses (data.openstates.org/people/current/ny.csv)
   specifically to avoid a per-user credential dependency. An earlier version
   of this module used the v3 REST API instead (v3.openstates.org, which
   needs an `OPENSTATES_API_KEY`), but that dependency bought nothing: the
   bulk file covers the exact same *current* Texas Legislature members (House
   + Senate) with their sitting party, so switching to it drops the
   credential requirement at no coverage cost. It adds no historical depth
   beyond source 1, and it covers no statewide executive or judicial office
   at all, but it is an independent read on the same fact for whoever it does
   cover, and the only source here that speaks to incumbency directly rather
   than by inferring it from a prior win. If the export is unreachable, this
   source is skipped with a warning, exactly as the SOS source degrades when
   unreachable — no source here is ever load-bearing for the scrape to
   succeed.

A third source, The Green Papers (thegreenpapers.com/G{YY}/TX), was tried and
removed: its live markup doesn't match a stable, parseable contract (office
section titles and candidate lines share the same flat `<li>`/`<p>` list with
no consistent markers, and the per-line format itself differs between
already-decided cycles and upcoming ones), so it kept silently degrading to 0
rows rather than surfacing a real signal. Not worth carrying for statewide
coverage source 1 already gets a version of, at 2019 and earlier, anyway.

Raw files written (data/Texas/raw/):
  SOS_RaceSummary.csv       one row per candidate per race per election, 1992-2019
  OpenStates_People.csv     one row per currently-serving TX legislator

Both are optional. `parsers/texas_enrich.py` treats a missing file as "this
source has nothing to say" and degrades to leaving `party` blank, the same
way `parsers/texas.py` already degrades when `cover.csv` itself is absent.
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup, NavigableString

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from config import USER_AGENT

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Texas" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Texas" / "party_manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["source", "filename", "row_count", "fetched_at"]

REQUEST_PAUSE = 0.3   # polite delay between requests to any one state site

# =========================== output schemas ============================
SOS_COLS = [
    "eleid", "election_name", "election_year", "stage",
    "race_raw", "office", "district",
    "candidate_name", "incumbent_flag", "party", "votes",
]

OPENSTATES_COLS = [
    "openstates_id", "name", "given_name", "family_name",
    "party", "chamber", "district",
]


def clean(val) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,*/*"})
    return s


def _get(sess: requests.Session, url: str, retries: int = 3,
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


# ===================================================================
# Source 1 — Texas Secretary of State legacy canvass site (1992-2019)
# ===================================================================
SOS_SITE = "https://elections.sos.state.tx.us"

# TEC's electronic archive begins 2000-07-01 (see docs/states/texas.md); SOS's
# legacy site's own last entries are 2019 specials. Elections outside that
# span cannot join to any TEC filer and are skipped rather than fetched.
SOS_EARLIEST_YEAR = 2000
SOS_LATEST_YEAR   = 2019

_YEAR_RE  = re.compile(r"\b(19|20)\d{2}\b")
_STAGE_RE = [
    (re.compile(r"runoff", re.IGNORECASE),      "runoff"),
    (re.compile(r"primary", re.IGNORECASE),     "primary"),
    (re.compile(r"special", re.IGNORECASE),     "special"),
    (re.compile(r"constitutional amendment", re.IGNORECASE), "constitutional_amendment"),
]


def _election_stage(name: str) -> str:
    for rx, label in _STAGE_RE:
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
    return clean("".join(
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
    resp = _get(sess, f"{SOS_SITE}/index.htm")
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
        eleid = clean(opt.get("value"))
        name  = _own_text(opt)
        if not name:
            continue

        optgroup = opt.find_parent("optgroup")
        group_label = clean(optgroup.get("label")) if optgroup else ""
        if group_label:
            year_ctx = group_label
        elif not eleid and _YEAR_RE.fullmatch(name):
            # An inert "YYYY" divider option, not a real election — track it
            # as context for the options that follow and drop it.
            year_ctx = name
            continue

        if year_ctx and not _YEAR_RE.search(name):
            name = f"{year_ctx} {name}".strip()
        if eleid and name:
            out.append((eleid, name))
    return out


def in_scope(election_name: str) -> bool:
    m = _YEAR_RE.search(election_name)
    if not m:
        return False
    year = int(m.group(0))
    if not (SOS_EARLIEST_YEAR <= year <= SOS_LATEST_YEAR):
        return False
    return "constitutional amendment" not in election_name.lower()


# Only offices office_types.csv actually maps for TX are worth carrying —
# matching against a race this pipeline can never join to a TEC candidate
# (US House/Senate; TEC has no federal filers) just bloats the file.
_OFFICE_PATTERNS: list[tuple[re.Pattern, str]] = [
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
_DISTRICT_RE = re.compile(r"district\s+(\d+)", re.IGNORECASE)
_PLACE_RE    = re.compile(r"place\s+(\d+)", re.IGNORECASE)


def canonical_race_office(race_raw: str) -> tuple[str, str]:
    """(canonical_office, district) or ("", "") when this race isn't one
    office_types.csv maps for TX — U.S. House/Senate chiefly, since TEC has no
    federal filers to ever match them against."""
    text = clean(race_raw).rstrip("-").strip()
    office = ""
    for rx, label in _OFFICE_PATTERNS:
        if rx.search(text):
            office = label
            break
    if not office:
        return "", ""

    dm = _DISTRICT_RE.search(text)
    pm = _PLACE_RE.search(text)
    if dm and pm:
        district = f"{dm.group(1)} Place {pm.group(1)}"
    elif pm:
        district = f"Place {pm.group(1)}"
    elif dm:
        district = dm.group(1)
    else:
        district = ""
    return office, district


_INCUMBENT_RE = re.compile(r"\(I\)\s*$")


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
            cells = [clean(td.get_text(" ")) for td in tr.find_all(["td", "th"])]
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

            incumbent = "1" if _INCUMBENT_RE.search(name_cell) else "0"
            name = _INCUMBENT_RE.sub("", name_cell).strip()

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
        resp = _get(sess, f"{SOS_SITE}/elchist{eleid}_state.htm")
        time.sleep(REQUEST_PAUSE)
        if resp is None:
            n_err += 1
            continue
        year = _YEAR_RE.search(name)
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


# ===================================================================
# Source 2 — Open States nightly bulk CSV (current TX legislators)
# ===================================================================
# https://data.openstates.org/people/current/tx.csv — the same CC0,
# no-key-required nightly export scrapers/new_york_party.py already uses in
# place of the v3 REST API, for the same reason: v3 needs an
# OPENSTATES_API_KEY, which would make this source depend on a per-user
# credential nobody else running this pipeline has. The bulk file has the
# same "currently serving only" ceiling the v3 source already had here — this
# adds no historical depth beyond source 1 either way — so switching to it
# costs nothing source 1 wasn't already the fallback for.
OPENSTATES_CURRENT = "https://data.openstates.org/people/current/tx.csv"

# The CSV export uses the same upper/lower classification the v3 API's
# current_role.org_classification did.
_OS_CHAMBERS = {"upper": "State Senator", "lower": "State Representative"}


def _iter_csv(text: str):
    """Yield dict rows from CSV text, tolerating a UTF-8 BOM."""
    return csv.DictReader((text or "").lstrip("﻿").splitlines())


def scrape_openstates(sess: requests.Session, log) -> int:
    """Write OpenStates_People.csv from the nightly CC0 bulk CSV.

    No API key needed — see module note above. Column names in the export
    are current_party/current_district/current_chamber; translated here to
    this module's existing party/chamber/district output columns so
    parsers/texas_enrich.py needs no changes.
    """
    resp = _get(sess, OPENSTATES_CURRENT, timeout=120)
    if resp is None or not resp.text.strip():
        log.warning("  Open States bulk CSV unavailable — skipping this source")
        return 0

    rows: list[dict] = []
    for src in _iter_csv(resp.text):
        name = clean(src.get("name"))
        if not name:
            continue
        chamber = _OS_CHAMBERS.get(clean(src.get("current_chamber")).lower(), "")
        party = clean(src.get("current_party"))
        if not (chamber and party):
            continue
        district = clean(src.get("current_district"))
        rows.append({
            "openstates_id": clean(src.get("id")),
            "name":          name,
            "given_name":    clean(src.get("given_name")),
            "family_name":   clean(src.get("family_name")),
            "party":         party,
            "chamber":       chamber,
            "district":      district.lstrip("0") or district,
        })

    out = RAW_DIR / "OpenStates_People.csv"
    part = out.with_suffix(".csv.part")
    with open(part, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OPENSTATES_COLS,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(rows)
    part.replace(out)
    return len(rows)


# ============================ manifest ============================
def upsert_manifest(record: dict) -> None:
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="", encoding="utf-8") as f:
            existing = [r for r in csv.DictReader(f)
                       if r.get("source") != record["source"]]
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(existing)
        w.writerow(record)


# ============================ orchestrator ============================
def run(sos: bool = False, openstates: bool = False):
    """Download Texas party-enrichment sources.

    Horizontal scope (additive; no flags = both, tried in the priority order
    parsers/texas_enrich.py applies them):
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
    sess = _session()
    n_sos = n_os = 0
    today = time.strftime("%Y-%m-%d")

    try:
        if do_sos:
            log.info("Texas Secretary of State (elections.sos.state.tx.us)")
            n_sos = scrape_sos(sess, log)
            upsert_manifest({"source": "sos", "filename": "SOS_RaceSummary.csv",
                             "row_count": n_sos, "fetched_at": today})
            log.info(f"  wrote SOS_RaceSummary.csv ({n_sos:,} rows)")

        if do_os:
            log.info("Open States bulk CSV (data.openstates.org)")
            n_os = scrape_openstates(sess, log)
            upsert_manifest({"source": "openstates", "filename": "OpenStates_People.csv",
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


def _cli():
    p = argparse.ArgumentParser(
        description="Download Texas party-enrichment sources (SOS legacy "
                    "canvass + Open States bulk CSV).")
    p.add_argument("--sos", action="store_true", help="Texas SOS legacy race summary only")
    p.add_argument("--openstates", action="store_true", help="Open States bulk CSV only")
    return p.parse_args()


if __name__ == "__main__":
    args = _cli()
    try:
        run(sos=args.sos, openstates=args.openstates)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
