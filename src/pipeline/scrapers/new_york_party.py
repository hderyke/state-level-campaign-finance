"""
scrapers/new_york_party.py — Download the external party/office/incumbency
sources that NYSBOE's campaign-finance datasets do not publish.

Why this exists
---------------
The four campaign-finance datasets on data.ny.gov carry **no party column at
all**. This is not an extraction gap in scrapers/new_york.py — it is a gap in
the source. Confirmed 2026-07 against the live Socrata catalog: the filer
registry (`7x2g-h32p`) exposes 17 columns and the disclosure table
(`e9ss-239a`) exposes 63, and party is absent from both. So
`candidates.party` can only ever be filled by joining something else in.

This module fetches that "something else" into data/New York/raw/, where
parsers/new_york.py joins it during its candidates pass. It is a *separate*
scraper from scrapers/new_york.py on purpose: it talks to entirely different
hosts on a different cadence, it is optional (the parser degrades to blank
party if these files are absent, exactly as it does today), and a failure
here must never take down the 18M-row disclosure pull.

Sources
-------
1. **NYSBOE Election Results database** — https://results.elections.ny.gov

   The board's official results database, covering **1994–2025**. Despite the
   elections.ny.gov hostname it is not a NYSBOE-built app: it runs on the
   Civera / ElectionStats platform, and the front end is a thin shell over a
   JSON+CSV API at `ny.elstats.civera.com`. Both endpoints below were read
   off the server-rendered markup of a live contest page, not guessed:

       GET /api/download_contest/{contest_id}_{layout}.csv?split_party={bool}
       GET /api/download_search.csv?search={url-encoded json}

   `split_party=true` is the important one. New York is a fusion-voting
   state — a candidate is routinely nominated by several parties at once and
   gets a separate ballot line for each — and split_party=true returns one
   row per candidate *per line* rather than collapsing them into a single
   total. That per-line granularity is what lets the parser populate
   `candidates.party` as "DEMOCRAT|WORKING FAMILIES" instead of silently
   picking a winner between two equally real answers.

   Coverage caveat, and it is the big one: NYSBOE certifies **statewide,
   congressional, state-legislative and judicial** contests. Town, village,
   city-council and school-board races are certified by the 62 county boards
   and are *not* in this database. Those local offices are the bulk of the NY
   filer registry (of 36,486 candidate filers, "Member of Assembly" +
   "State Senator" + the six statewide offices account for only ~6,400 —
   roughly 17%), so party fill on the full candidates table is structurally
   capped well below 100% no matter how good the matching is. See
   docs/states/new_york.md for the measured breakdown.

2. **Open States** — https://data.openstates.org/people/current/ny.csv

   Nightly CSV of currently-serving NY legislators with `current_party`,
   `current_district` and `current_chamber`. CC0-licensed, no API key
   required (the v3 REST API at v3.openstates.org does require one; this
   bulk file deliberately avoids that dependency).

   Only ~213 people, and only those serving *right now*, so it adds little
   raw coverage on top of source 1. It is fetched anyway because it is an
   independent read on the same fact: where both sources name a party for the
   same person and disagree, the parser downgrades match_confidence rather
   than silently trusting one. It is also the only source here that speaks to
   *current* incumbency directly rather than by inference from a prior win.

Raw files written (data/New York/raw/):
  ElectionStats_Contests.csv   one row per candidate per ballot line per contest
  OpenStates_People.csv        one row per currently-serving NY legislator

Neither file is required. parsers/new_york.py treats both as optional
enrichment overlays and logs a warning (not an error) when they are missing.
"""

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from config import USER_AGENT

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "New York" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "New York" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]

# ============================== endpoints ==============================
RESULTS_SITE = "https://results.elections.ny.gov"
CIVERA_API   = "https://ny.elstats.civera.com/api"

OPENSTATES_CURRENT = "https://data.openstates.org/people/current/ny.csv"

# The site's own "Download Data" control on /search emits exactly this object
# with every filter empty, which the API reads as "no filter" — i.e. every
# contest in the database. Rebuilt here as a dict rather than pasted as a
# blob so the shape stays readable and a single filter can be added later
# without hand-editing percent-encoding.
EMPTY_SEARCH = {
    "global":         {"events": []},
    "ballotQuestions": {"text": "", "types": [], "number": "", "divisions": []},
    "contests":       {"candidates": [], "offices": [], "divisions": []},
    "voterStats":      False,
    "stages":          [],
    "specialElectionsOnly": False,
}

# results.elections.ny.gov states its own coverage as 1994–2025 on the /search
# page. The floor matters because contest-ID walking has no other stop signal;
# the ceiling is advisory only (a live site will keep adding years).
EARLIEST_ELECTION_YEAR = 1994

# ID-walk tuning. Contest IDs are dense but not gapless — 1994's contests sit
# in the ~4900s, so ID order is not year order and the walk cannot stop at the
# first 404. MISS_RUN is the number of consecutive misses that ends the walk.
MISS_RUN     = 400
ID_WALK_STOP = 200_000     # absolute ceiling, so a server-side change that
                           # starts 200-ing every ID can't loop forever

REQUEST_PAUSE = 0.25       # polite delay between requests to a state site

# ============================ output schemas ============================
CONTEST_COLS = [
    "contest_id",
    "election_date",
    "election_year",
    "stage",            # "General Election", "Democratic Primary", "Special", ...
    "office",           # as printed by NYSBOE ("Member of Assembly", "Governor")
    "division",         # "State of New York", "127th Assembly District", ...
    "district",         # digits pulled out of division, where it has any
    "candidate_id",
    "candidate_name",
    "party",            # ONE ballot line; fusion candidates get several rows
    "votes",
    "is_winner",
    "source",           # "contest_csv" | "contest_html" | "search_csv"
]

OPENSTATES_COLS = [
    "openstates_id", "name", "given_name", "family_name",
    "current_party", "current_district", "current_chamber",
]

# ======================= header-tolerant CSV mapping =======================
# The live schema of /api/download_search.csv, observed 2026-07-25:
#
#   contest_id, election_id, election_date, election_type, primary_party,
#   question_text, question_type, office_id, office_name, office_modifier,
#   district_id, district_type, district_name, candidate_id, candidate_name,
#   retention_candidate_id, retention_candidate_name, division_id,
#   division_type, division_name, vote_channel, is_winner, number_seats,
#   candidate_party_id, candidate_party_name, votes
#
# Three things in there drive the mapping and are easy to get wrong:
#
#   candidate_party_name  is the candidate's ballot line — the party column.
#   primary_party         is NOT. It says which party's primary a contest is,
#                         so on a general-election row it's blank and on a
#                         primary row it describes the *contest*. Treating it
#                         as the candidate's party would label every
#                         general-election candidate with nothing and every
#                         primary candidate correctly by accident. It is used
#                         only to build the stage string ("Democratic Primary").
#   district_name         is the seat ("127th Assembly District").
#   division_name         is the geography the votes in that row were counted
#                         in. They are different columns and conflating them
#                         puts a county name in `district`.
#
# `division_*` plus `vote_channel` mean the export is one row per candidate per
# ballot line **per division per voting method** — see _aggregate().
_KNOWN_HEADERS: dict[str, str] = {
    "contest_id":           "contest_id",
    "candidate_id":         "candidate_id",
    "candidate_name":       "candidate_name",
    "candidate_party_name": "party",
    "election_date":        "election_date",
    "election_type":        "election_type",
    "primary_party":        "primary_party",
    "office_name":          "office",
    "district_name":        "district_name",
    "division_name":        "division",
    "votes":                "votes",
    "is_winner":            "is_winner",
}

# Fallback for when the schema moves. The endpoints are the site's own download
# buttons, not a published API, so a hardcoded schema alone would silently
# produce an empty file the day a column is renamed. Anything unmatched is
# ignored; a contest whose required fields don't resolve falls through to the
# HTML parser.
_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "candidate_name": ("candidatename", "candidate", "name", "candidatefullname"),
    "party":          ("candidatepartyname", "partyname", "party",
                       "ballotline", "line", "partycode"),
    "votes":          ("votes", "votecount", "totalvotes", "numvotes"),
    "is_winner":      ("iswinner", "winner", "elected", "won"),
    "office":         ("officename", "office", "contestoffice"),
    "district_name":  ("districtname", "district"),
    "division":       ("divisionname", "division", "jurisdiction"),
    "election_type":  ("electiontype", "stage", "contesttype", "type"),
    "primary_party":  ("primaryparty",),
    "election_date":  ("electiondate", "date"),
    "election_year":  ("electionyear", "year"),
    "contest_id":     ("contestid", "contest"),
    "candidate_id":   ("candidateid",),
}


def _norm_header(h: str) -> str:
    """Lowercase and strip everything but letters, so 'Candidate Name',
    'candidate_name' and 'CandidateName' all collapse to 'candidatename'."""
    return re.sub(r"[^a-z]", "", (h or "").lower())


def _map_headers(fieldnames: list[str]) -> dict[str, str]:
    """Build {canonical_col: source_header} for one CSV's header row.

    Exact known headers win; the alias table only fills what they didn't cover,
    so a renamed column degrades to fuzzy matching instead of to nothing.
    """
    present = {h for h in (fieldnames or []) if h}
    mapping = {canon: src for src, canon in _KNOWN_HEADERS.items()
               if src in present}

    normed = {_norm_header(h): h for h in present}
    for canon, aliases in _HEADER_ALIASES.items():
        if canon in mapping:
            continue
        for alias in aliases:
            if alias in normed:
                mapping[canon] = normed[alias]
                break
    return mapping


def _stage_of(election_type: str, primary_party: str) -> str:
    """Build the stage label the parser matches on.

    "Primary" + "Democratic" -> "Democratic Primary"; anything else passes the
    election type through unchanged ("General", "Special"). The parser only
    keys off whether "general" appears, but the fuller string is what the HTML
    path produces, and the two must agree or the same contest would sort
    differently depending on which path fetched it.
    """
    et = clean(election_type)
    pp = clean(primary_party)
    if pp and "primary" in et.lower():
        return f"{pp} {et}" if pp.lower() not in et.lower() else et
    return et


def _normalise_row(src: dict, mapping: dict, source: str,
                   contest_id: str = "") -> dict | None:
    """One source CSV row -> one CONTEST_COLS dict, or None to skip it.

    Skips ballot-question rows, which share the export with contests and carry
    `question_text` but no `candidate_name`.
    """
    row = {c: "" for c in CONTEST_COLS}
    vals = {canon: clean(src.get(header)) for canon, header in mapping.items()}

    row["candidate_name"] = vals.get("candidate_name", "")
    if not row["candidate_name"]:
        return None

    row["contest_id"]    = vals.get("contest_id") or contest_id
    row["candidate_id"]  = vals.get("candidate_id", "")
    row["party"]         = vals.get("party", "")
    row["votes"]         = vals.get("votes", "")
    row["is_winner"]     = vals.get("is_winner", "")
    row["office"]        = vals.get("office", "")
    row["division"]      = vals.get("division", "")
    row["election_date"] = vals.get("election_date", "")
    row["stage"]         = _stage_of(vals.get("election_type", ""),
                                     vals.get("primary_party", ""))

    row["election_year"] = vals.get("election_year", "")
    if not row["election_year"] and row["election_date"]:
        ym = _YEAR_RE.search(row["election_date"])
        row["election_year"] = ym.group(0) if ym else ""

    # District comes from district_name ("127th Assembly District"), never from
    # division_name — division is the geography the votes were counted in.
    dm = _DISTRICT_RE.search(vals.get("district_name", ""))
    row["district"] = dm.group(1) if dm else ""

    row["source"] = source
    return row


def _aggregate(rows: list[dict]) -> list[dict]:
    """Collapse per-division / per-vote-channel rows to one row per ballot line.

    The search export reports a candidate's votes broken out by division and
    voting method, so a single statewide candidacy can span hundreds of rows
    that differ only in `division_name` and `vote_channel`. Left as-is, those
    rows would inflate the file and — worse — make the vote figure attached to
    each ballot line a single county's total, which is what the parser orders
    a fusion candidate's lines by.

    Votes are summed and `is_winner` is OR-ed across the group. Grouping is by
    (contest, candidate, ballot line), so a fusion candidate keeps one row per
    line, which is exactly the granularity CONTEST_COLS promises. Harmless when
    the export is already one row per line: each group is then a single row.
    """
    merged: dict[tuple, dict] = {}
    for r in rows:
        key = (r["contest_id"],
               r["candidate_id"] or r["candidate_name"].upper(),
               (r["party"] or "").upper())
        cur = merged.get(key)
        if cur is None:
            cur = merged[key] = dict(r)
            cur["_votes"] = 0
            cur["_won"] = False
        cur["_votes"] += _int(r["votes"])
        cur["_won"] = cur["_won"] or _truthy(r["is_winner"])
        # A division-level row may leave contest-level fields blank where the
        # statewide summary row filled them; keep the first non-empty value.
        for f in ("office", "district", "stage", "election_date",
                  "election_year", "division"):
            if not cur[f] and r[f]:
                cur[f] = r[f]

    out = []
    for r in merged.values():
        r["votes"]     = str(r.pop("_votes"))
        r["is_winner"] = "1" if r.pop("_won") else "0"
        out.append(r)
    return out


def _int(val) -> int:
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (TypeError, ValueError):
        return 0


def _truthy(val) -> bool:
    return str(val or "").strip().upper() in {"1", "Y", "YES", "TRUE", "W", "WON"}


# ============================== helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return re.sub(r"\s+", " ", (val or "").strip())


def _session() -> requests.Session:
    """Session pinned to the project user agent, with a Referer.

    The Civera download endpoints sit on a different host
    (ny.elstats.civera.com) from the page that links them
    (results.elections.ny.gov) and 403 some clients without a Referer, so it
    is set on every request rather than only where it was observed to matter.
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/csv,text/html;q=0.9,*/*;q=0.8",
        "Referer": f"{RESULTS_SITE}/",
    })
    return s


def _get(sess: requests.Session, url: str, params: dict | None = None,
         retries: int = 3, timeout: int = 60) -> requests.Response | None:
    """GET with linear backoff. Returns None on 404 (a genuine miss during the
    ID walk, not an error) and re-raises nothing — callers treat None as
    'no data here' so one bad contest can't abort a multi-thousand-contest run.
    """
    last_err = None
    for attempt in range(retries):
        try:
            resp = sess.get(url, params=params, timeout=timeout)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp
        except Exception as e:                       # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
    if last_err is not None:
        return None
    return None


_DISTRICT_RE = re.compile(r"(\d+)(?:st|nd|rd|th)?\b")

# "1994 Sep 13 • Democratic Primary • United States Senator • State of New York"
# The separator is a real U+2022 on the live pages; the ASCII fallbacks are
# there because the same string reaches us through <title>, og:title and the
# CSV, and not all three normalise the bullet identically.
_TITLE_SPLIT = re.compile(r"\s*[•·|]\s*")
_YEAR_RE     = re.compile(r"\b(19|20)\d{2}\b")


def parse_contest_title(title: str) -> dict:
    """Break a NYSBOE contest heading into its parts.

    Returns a dict with election_date / election_year / stage / office /
    division / district; any part the heading doesn't carry comes back "".
    Deliberately lenient — the heading is display text, and a contest whose
    heading doesn't parse should still contribute its candidates and parties
    rather than being dropped.
    """
    out = {"election_date": "", "election_year": "", "stage": "",
           "office": "", "division": "", "district": ""}
    parts = [clean(p) for p in _TITLE_SPLIT.split(clean(title)) if clean(p)]
    if not parts:
        return out

    # The leading segment is the date ("1994 Sep 13") — but only consume it as
    # such if it actually contains a year. A heading that opens straight into
    # the office would otherwise lose its first real segment to the date slot
    # and shift office/division one place left.
    rest = list(parts)
    if _YEAR_RE.search(parts[0]):
        head = rest.pop(0)
        out["election_year"] = _YEAR_RE.search(head).group(0)
        for fmt in ("%Y %b %d", "%Y %B %d"):
            try:
                out["election_date"] = datetime.strptime(head, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # Remaining segments are, in order: stage, office, division. Some contests
    # omit the stage (older general elections) — detect it by keyword rather
    # than by position so the office doesn't get read as the stage.
    if rest and re.search(r"primary|general|special|runoff|convention",
                          rest[0], re.IGNORECASE):
        out["stage"] = rest.pop(0)
    if rest:
        out["office"] = rest.pop(0)
    if rest:
        out["division"] = rest.pop(0)

    if out["division"]:
        dm = _DISTRICT_RE.search(out["division"])
        if dm:
            out["district"] = dm.group(1)
    return out


def _iter_csv(text: str):
    """Yield dict rows from CSV text, tolerating a UTF-8 BOM."""
    return csv.DictReader((text or "").lstrip("﻿").splitlines())


# ====================== source 1: NYSBOE / ElectionStats ======================
def fetch_search_csv(sess: requests.Session, log) -> list[dict]:
    """Try the whole-database export in a single request.

    /api/download_search.csv with every filter empty is what the site's own
    "Search Results CSV" button produces for an unfiltered search. When it
    works this is worth thousands of individual contest requests, so it is
    tried first — but it is one undocumented endpoint doing a very large job,
    so an empty or unparseable response is treated as "fall back", never as a
    failure.
    """
    url = f"{CIVERA_API}/download_search.csv"
    params = {"search": json.dumps(EMPTY_SEARCH, separators=(",", ":"))}
    resp = _get(sess, url, params=params, timeout=300)
    if resp is None or not resp.text.strip():
        log.warning("  bulk search export returned nothing — falling back to "
                    "per-contest walk")
        return []

    reader = _iter_csv(resp.text)
    mapping = _map_headers(reader.fieldnames or [])
    missing = [c for c in ("candidate_name", "party") if c not in mapping]
    if missing:
        log.warning(f"  bulk search export unusable — no column resolved for "
                    f"{missing}. Headers seen: {reader.fieldnames}. "
                    f"Falling back to per-contest walk")
        return []

    raw = [r for r in (_normalise_row(s, mapping, "search_csv") for s in reader)
           if r is not None]
    rows = _aggregate(raw)
    # The collapse ratio says whether the export was per-division or already
    # one row per ballot line — worth seeing, since it silently changes how
    # long this takes and how big the raw file is.
    log.info(f"  bulk search export: {len(raw):,} source rows → "
             f"{len(rows):,} candidate-ballot-line rows")
    return rows


def fetch_contest_csv(sess: requests.Session, contest_id: int) -> list[dict]:
    """One contest via the Civera CSV endpoint, split by ballot line.

    split_party=true is what makes fusion legible: a candidate carried by
    Democratic and Working Families comes back as two rows, one per line,
    instead of one row with the votes summed.
    """
    url = f"{CIVERA_API}/download_contest/{contest_id}_table.csv"
    resp = _get(sess, url, params={"split_party": "true"})
    if resp is None or not resp.text.strip():
        return []

    reader = _iter_csv(resp.text)
    mapping = _map_headers(reader.fieldnames or [])
    if "candidate_name" not in mapping or "party" not in mapping:
        return []

    raw = [r for r in (_normalise_row(s, mapping, "contest_csv",
                                      contest_id=str(contest_id))
                       for s in reader) if r is not None]
    return _aggregate(raw)


def fetch_contest_html(sess: requests.Session, contest_id: int) -> list[dict]:
    """One contest by parsing its server-rendered page.

    Fallback for when the CSV endpoint is unavailable or its headers have
    moved. The contest page is plain server-rendered HTML (no JS needed): the
    heading carries date/stage/office/division, and each candidate is an
    <a href="/candidate/{id}"> whose text is the name followed by the party.
    """
    resp = _get(sess, f"{RESULTS_SITE}/contest/{contest_id}")
    if resp is None or not resp.text.strip():
        return []
    soup = BeautifulSoup(resp.text, "html.parser")

    heading = soup.find("h1")
    meta    = soup.find("meta", attrs={"property": "og:title"})
    title   = clean(heading.get_text(" ")) if heading else ""
    if not title and meta:
        title = clean(meta.get("content"))
    parsed = parse_contest_title(title)

    rows: list[dict] = []
    for link in soup.select('a[href*="/candidate/"]'):
        href = link.get("href", "")
        idm  = re.search(r"/candidate/(\d+)", href)
        # The visible link text is initials + name + party run together
        # ("DM Daniel P. Moynihan Democratic"); the accessible name is on the
        # title attribute, so use that as the authoritative name and treat
        # whatever trails it in the link text as the party.
        name = clean(link.get("title"))
        text = clean(link.get_text(" "))
        if not name:
            continue
        party = ""
        pos = text.find(name)
        if pos >= 0:
            party = clean(text[pos + len(name):])
        row = {c: "" for c in CONTEST_COLS}
        row.update(parsed)
        row["contest_id"]     = str(contest_id)
        row["candidate_id"]   = idm.group(1) if idm else ""
        row["candidate_name"] = name
        row["party"]          = party
        row["source"]         = "contest_html"
        rows.append(row)
    return rows


def walk_contests(sess: requests.Session, log,
                  start_id: int, end_id: int | None) -> list[dict]:
    """Enumerate contests by ID until MISS_RUN consecutive IDs come back empty.

    There is no index endpoint to drive this from, and IDs are not ordered by
    year (1994 sits in the ~4900s), so a miss run is the only available stop
    signal. It is set high (400) deliberately: the ID space has real gaps
    where contests were deleted or reserved, and a short run would stop the
    walk in the middle of the database and silently under-collect.
    """
    rows: list[dict] = []
    misses = 0
    cid = start_id
    ceiling = end_id or ID_WALK_STOP
    n_contests = 0

    while cid <= ceiling and misses < MISS_RUN:
        got = fetch_contest_csv(sess, cid) or fetch_contest_html(sess, cid)
        if got:
            rows.extend(got)
            n_contests += 1
            misses = 0
            if n_contests % 250 == 0:
                log.info(f"  walked to contest {cid}: "
                         f"{n_contests:,} contests, {len(rows):,} rows")
        else:
            misses += 1
        cid += 1
        time.sleep(REQUEST_PAUSE)

    log.info(f"  contest walk finished at id {cid - 1}: "
             f"{n_contests:,} contests, {len(rows):,} candidate-line rows")
    return rows


def scrape_electionstats(sess: requests.Session, log,
                         start_id: int, end_id: int | None,
                         force_walk: bool) -> int:
    """Write ElectionStats_Contests.csv. Returns row count."""
    rows: list[dict] = []
    if not force_walk:
        rows = fetch_search_csv(sess, log)
    if not rows:
        rows = walk_contests(sess, log, start_id, end_id)

    # Drop rows whose election year predates the database's own stated
    # coverage — anything earlier is a parse artefact, not real data.
    kept = [
        r for r in rows
        if not r["election_year"]
        or int(r["election_year"]) >= EARLIEST_ELECTION_YEAR
    ]

    out = RAW_DIR / "ElectionStats_Contests.csv"
    part = out.with_suffix(".csv.part")
    with open(part, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CONTEST_COLS,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(kept)
    part.replace(out)
    return len(kept)


# ========================= source 2: Open States =========================
def scrape_openstates(sess: requests.Session, log) -> int:
    """Write OpenStates_People.csv from the nightly CC0 bulk CSV.

    Deliberately the bulk file and not the v3 REST API: v3 requires an API
    key, and making party enrichment depend on a per-user credential would
    make the NY pipeline non-reproducible for anyone who hasn't registered
    one. The tradeoff is that this file covers only *currently serving*
    legislators — historical coverage comes from source 1.
    """
    resp = _get(sess, OPENSTATES_CURRENT, timeout=120)
    if resp is None or not resp.text.strip():
        log.warning("  Open States bulk CSV unavailable — skipping")
        return 0

    rows = []
    for src in _iter_csv(resp.text):
        name = clean(src.get("name"))
        if not name:
            continue
        rows.append({
            "openstates_id":    clean(src.get("id")),
            "name":             name,
            "given_name":       clean(src.get("given_name")),
            "family_name":      clean(src.get("family_name")),
            "current_party":    clean(src.get("current_party")),
            "current_district": clean(src.get("current_district")),
            "current_chamber":  clean(src.get("current_chamber")),
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


# ========================== manifest bookkeeping ==========================
def upsert_manifest(record: dict) -> None:
    """Add or overwrite the manifest entry matching (relation_type, year).

    Shares data/New York/manifest.csv with scrapers/new_york.py — these rows
    use relation_type "electionstats" / "openstates", which that scraper never
    writes, so the two can't collide.
    """
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="", encoding="utf-8") as f:
            existing = [
                r for r in csv.DictReader(f)
                if not (r.get("relation_type") == record["relation_type"]
                        and r.get("year") == record["year"])
            ]
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(existing)
        w.writerow(record)


# ============================= orchestrator =============================
def run(results: bool = False, openstates: bool = False,
        start_id: int = 1, end_id: int | None = None,
        force_walk: bool = False):
    """Download NY party-enrichment sources.

    Horizontal scope (additive; no flags = both sources):
        results     — NYSBOE ElectionStats contests only
        openstates  — Open States current legislators only

    ElectionStats options:
        start_id / end_id  — bound the contest-ID walk (ignored when the bulk
                             search export succeeds)
        force_walk         — skip the bulk export and walk IDs regardless
    """
    log = get_logger("new york", "scrape")
    t0  = time.perf_counter()
    log.info("Starting New York party-enrichment scraper")
    log._emit("scrape_started", source="new_york_party",
              results=results, openstates=openstates,
              start_id=start_id, end_id=end_id, force_walk=force_walk)

    do_results    = results or not (results or openstates)
    do_openstates = openstates or not (results or openstates)
    sess = _session()
    n_results = n_os = 0

    try:
        if do_results:
            log.info("NYSBOE ElectionStats (results.elections.ny.gov)")
            n_results = scrape_electionstats(sess, log, start_id, end_id,
                                             force_walk)
            upsert_manifest({
                "relation_type": "electionstats", "year": "snapshot",
                "filename": "ElectionStats_Contests.csv",
                "row_count": n_results,
            })
            log.info(f"  wrote ElectionStats_Contests.csv ({n_results:,} rows)")

        if do_openstates:
            log.info("Open States (data.openstates.org)")
            n_os = scrape_openstates(sess, log)
            upsert_manifest({
                "relation_type": "openstates", "year": "snapshot",
                "filename": "OpenStates_People.csv",
                "row_count": n_os,
            })
            log.info(f"  wrote OpenStates_People.csv ({n_os:,} rows)")

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  source="new_york_party",
                  electionstats_rows=n_results, openstates_rows=n_os)

    except KeyboardInterrupt:
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  source="new_york_party",
                  electionstats_rows=n_results, openstates_rows=n_os)
        raise
    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  source="new_york_party",
                  electionstats_rows=n_results, openstates_rows=n_os,
                  error_type=type(e).__name__, error=str(e))
        raise


# ================================= CLI =================================
def _cli():
    p = argparse.ArgumentParser(
        description="Download New York party/office enrichment sources "
                    "(NYSBOE election results + Open States).")
    p.add_argument("--results", action="store_true",
                   help="NYSBOE ElectionStats contests only")
    p.add_argument("--openstates", action="store_true",
                   help="Open States current legislators only")
    p.add_argument("--start-id", type=int, default=1,
                   help="first contest id for the ID walk (default 1)")
    p.add_argument("--end-id", type=int, default=None,
                   help="last contest id for the ID walk")
    p.add_argument("--force-walk", action="store_true",
                   help="skip the bulk search export and walk contest IDs")
    return p.parse_args()


if __name__ == "__main__":
    args = _cli()
    try:
        run(results=args.results, openstates=args.openstates,
            start_id=args.start_id, end_id=args.end_id,
            force_walk=args.force_walk)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
