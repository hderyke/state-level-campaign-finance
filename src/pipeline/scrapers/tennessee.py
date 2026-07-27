"""
scrapers/tennessee.py — Download Tennessee campaign finance data from the
Registry of Election Finance's TNCAMP application (https://apps.tn.gov/tncamp).

No Playwright. TNCAMP is a plain server-rendered JSP app: search criteria go
in as a form POST, results come back as an HTML table, and every results page
carries a CSV export link that dumps *that page* of results. So the whole
scrape is `requests` + BeautifulSoup driving a session cookie.

## Sources

    cesearch.htm   Contributions & Expenditures search  -> transactions
    cpsearch.htm   Candidates & PACs search             -> entity roster

Both are linked from https://apps.tn.gov/tncamp/public/search.htm.

## Shape of the scrape

TNCAMP paginates results and the export link only ever covers the page you
are currently on (~100 rows), so a year is not one file — it is a walk:

    GET  cesearch.htm            establish a session cookie
    POST cesearch.htm            submit the search criteria for one year
    GET  ceresults.htm           page 1 of results
      -> follow .exportlinks a   download page 1 as CSV
      -> is .btn-blue present?   yes: more pages remain
    GET  ceresultsnext.htm       page 2 ... and so on

Each page lands as its own raw file (`contributions_2024_p001.csv`) and the
manifest records one row per page, plus a sentinel `page = "complete"` row
once the walk reaches the end of a year.

That sentinel is what makes incremental runs safe. TNCAMP's pagination is
session-based — `ceresultsnext.htm` advances *the session's* cursor, there is
no `?page=7` to jump to — so a year that died at page 3 of 12 cannot be
resumed mid-way; it has to be re-walked from page 1. Skipping on "this year
has at least one page on disk" would therefore freeze that year at three
pages forever. Only a year with the `complete` sentinel is treated as done.

## Provenance of the request bodies — verified vs. assumed

The POST field names in `ce_search_body()` originally came from the
Investigative Reporting Workshop's `get_tn_contribs.R`, which scraped this
exact form successfully (Kiernan Nicholls, Julia Ingram, Yanqi Xu), and an
earlier iteration of this scraper in this repo did pull ~100-row result pages
off `ceresults.htm` with them — see
logs/dev/20260712130338-tennessee-scrape.jsonl.

A `--discover` run against the live forms on 2026-07-25 turned up real drift,
now folded into the body builders below:

  - `cesearch.htm` gained three recipient-side checkboxes — `toCandidate`,
    `toPac`, `toOther` — that parallel the four `from*` checkboxes and, like
    them, default to unchecked (i.e. "match nothing") when omitted. `toType`
    ("both") alone no longer appears to be sufficient. It also gained a
    `reportSelection` filter (report period — "1st Quarter", "Pre-Primary",
    etc.) with no visible blank/"all" option among the values `--discover`
    printed; sent as `""` here, matching how every other optional filter in
    this body is left blank, rather than omitted outright.
  - `cesearch.htm`'s result-column checkboxes gained `candidatePACNameField`,
    `candidateForField` and `soField`. The first is the one that matters:
    parsers/tennessee.py's `COLUMN_ALIASES` already treats
    `candidate_pac_name` as a spelling of `recipient_name`, which strongly
    suggests TNCAMP renamed the "Recipient Name" export column to
    "Candidate/PAC Name" behind a *new* checkbox rather than the old
    `recipientNameField` one — so both are now requested.
  - `cpsearch.htm` had drifted much further: the radio group is `searchType`
    (values `candidate`/`pac`/`both`), not `findType`; the name filter is
    `name`, not `lastName`; office/district/party filters are
    `officeSelection`/`districtSelection`/`partySelection`, not
    `officeSought`/`district`/`party`; there is one `winner` radio (Y/N), not
    separate `primaryWinner`/`generalWinner`; and the election-year filter is
    `electionYearSelection`, not `electionYear`. Three of the result-column
    checkboxes were also misnamed (`contactField` not `contactInfoField`,
    `officeField` not `officeSoughtField`, `committeeField` not
    `committeeAffiliationField`), which is the most likely reason
    parsers/tennessee.py's docstring reports Office Sought, Committee
    Affiliation and Contact Info "never resolve" — the checkbox requesting
    each of those columns was never actually being checked, so TNCAMP had no
    reason to include them. That note in the parser should be revisited once
    a run confirms whether the corrected names bring those columns back.
    `ENTITY_FIND_TYPE` was also sending the plural `"candidates"`/`"pacs"` as
    the radio value where the form only accepts singular `"candidate"`/`"pac"`
    — fixed alongside the field names.

What is still NOT verified: whether TNCAMP has added or renamed anything
*since* 2026-07-25, and what the 16th, un-printed `reportSelection` option is
(`--discover` truncates option lists at 15). `--discover` exists for exactly
this kind of drift: it prints every `<input>`/`<select>` name and every
`<option>` value from both live forms so the ground truth can be read off the
site and pasted into the body builders below. Run it before the first full
scrape, and after any TN.gov redesign.

`run()` fails loudly — raising, so orc marks the state failed — if the first
search of a run comes back with no export link at all, rather than quietly
writing zero files: "the form fields drifted" and "this year has no data"
would otherwise look identical from the outside.

## Rate limiting

TN.gov's WAF resets connections that look like bare scripts. Requests carry a
browser User-Agent and Accept headers, and pages within one year's walk are
spaced by a short randomized sleep — the same courtesy pacing the original R
script used, which ran against this host over a full history of years.

## Concurrency

A full single-threaded run over every (relation, year) pair took ~3 days —
almost entirely wall-clock spent waiting out per-page and per-year sleeps
across ~50 years × 2 relations, one at a time. Each year's walk is
independent (its own search POST, its own session-scoped page cursor), so
`run()` now walks several years in parallel via a small thread pool
(`--workers`, default `DEFAULT_WORKERS`), each worker on its own `requests`
session/cookie jar. The per-page courtesy sleep is unchanged *within* a
session — this doesn't make any single session hit the server faster, it just
runs `--workers` of those sessions at once — so a run that previously took 3
days at `--workers 1` should take roughly `3 days / workers`. Turn `--workers`
down (to 1 for the old fully-sequential behavior) if TN.gov starts responding
with more 429/503s or WAF resets than before; there's no data in this repo's
run logs yet on how many concurrent sessions the WAF tolerates.

## cpsearch.htm is two searches, not one

`cesearch.htm` and `cpsearch.htm` are separate forms, but `cpsearch.htm` is
itself two different searches behind one URL: flipping its `searchType` radio
between `candidate` and `pac` changes both the criteria the form accepts and
the result columns it offers. Two consequences the scraper has to respect:

  - **Candidates are election-scoped; PACs are not.** A TN candidate exists
    per election, and `electionYearSelection` on the candidate side is a
    required criterion rather than an optional filter — submitting it blank
    returns an empty result set. (That is what this scraper did before, and
    why the candidate roster came back empty while the PAC roster worked.) So
    the candidate roster is a walk *per election*, while the PAC roster — a
    standing registration with no election year on the form at all — stays one
    walk for the whole roster.

    Two traps in that selector, both of which this scraper hit:

      1. **Its option values are opaque database IDs, not years.** The list
         reads `<option value="234">2026</option>`,
         `<option value="238">2023 (HOUSE 51)</option>` — the year exists only
         in the *label*. POSTing "2023" matches no option and TNCAMP answers
         with an empty result set, identical in appearance to form drift. Only
         `discover_elections()` reads this select; the year-scanning
         `discover_valid_years()` is for `yearSelection` on the C&E form,
         whose values genuinely are years.
      2. **There are several options per year.** ~50 options span ~28 years,
         because special elections get their own entries — 2023 alone has the
         regular cycle plus HOUSE 3, HOUSE 51, HOUSE 52, HOUSE 86 and HOUSE 86
         AUG GEN, each with its own candidates. So the walk unit is an
         election, not a year, and raw files are keyed
         `candidates_{year}_e{id}_p{NNN}.csv`. Keying on the bare year would
         have the five 2023 searches overwrite each other's page files and let
         whichever finished first claim the year's `complete` sentinel for all
         of them.

    Because the IDs exist nowhere but the live form, there is no static
    fallback for this list the way `MIN_YEAR..current_year` backstops the
    transaction years. `run()` raises if the lookup comes back empty. Falling
    back to a fabricated year range is what produced the original failure.
  - **The two sides have different display checkboxes.** Candidates offer
    office/district/primary/general/election-year columns; PACs offer
    committee/officers/created/closed/responsible-individual. A `*Field` key
    the form doesn't recognise doesn't error, it just means the column never
    appears in the export — the same silent failure the 2026-07-25 --discover
    run traced the missing Office Sought / Committee Affiliation / Contact
    Info columns to. `cp_search_body()` therefore has one branch per side
    rather than sending the union of both.

Because the candidate roster is now election-scoped it also joins the
incremental scheme: each election carries the same `complete` sentinel, is
skipped once complete unless `--force` or an explicit year range is given, and
anything in the open cycle is always re-walked. Past elections' candidate
lists are closed history.

Raw files (data/Tennessee/raw/):
  contributions_{year}_p{NNN}.csv       — one file per results page
  expenditures_{year}_p{NNN}.csv        — one file per results page
  candidates_{year}_e{id}_p{NNN}.csv    — candidate roster pages, one set per
                                          election; `id` is the form's own
                                          electionYearSelection option value
                                          (see above)
  pacs_p{NNN}.csv                       — PAC roster pages, not election-scoped
"""

import csv
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
import config

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Tennessee" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Tennessee" / "manifest.csv"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "page", "filename", "bytes", "rows",
                 "downloaded_at"]

# Written into a manifest row's `page` column to mark that a (relation, year)
# walk reached the last results page. See completed_years().
COMPLETE_SENTINEL = "complete"

# ========================= state-specific constants ===================
HOST = "https://apps.tn.gov"
BASE = f"{HOST}/tncamp"

CE_SEARCH_URL  = f"{BASE}/public/cesearch.htm"        # contributions & expenditures
CE_RESULTS_URL = f"{BASE}/public/ceresults.htm"
CE_NEXT_URL    = f"{BASE}/public/ceresultsnext.htm"
CP_SEARCH_URL  = f"{BASE}/public/cpsearch.htm"        # candidates & PACs
CP_RESULTS_URL = f"{BASE}/public/cpresults.htm"
CP_NEXT_URL    = f"{BASE}/public/cpresultsnext.htm"

# Fallback floor for the year loop. The live form's own year selector is
# authoritative and is read at runtime by discover_valid_years(); this is only
# used when that lookup fails, and matches the 2002 floor the original IRW
# script used.
MIN_YEAR = 2002

# TNCAMP publishes no total-page count — the only "more results" signal is
# whether the next button is rendered — so this caps the walk in case that
# button is ever rendered unconditionally. 100 rows/page × 2000 pages = 200K
# rows for a single year, comfortably above any real TN year.
MAX_PAGES_PER_YEAR = 2000

# How many (relation, year) walks run at once, each on its own session/cookie
# jar. See "Concurrency" in the module docstring — this is the knob that took
# the full-history run from ~3 days to a few hours; turn it down to 1 to get
# the old strictly-sequential behavior back if the WAF starts pushing back.
DEFAULT_WORKERS = 4

# TN.gov's WAF resets connections that look like bare scripts (no User-Agent,
# no Accept headers). config.USER_AGENT is the repo-wide browser string;
# Accept/Accept-Language complete the browser-shaped request.
HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ============================= search bodies ===========================
# Field names below are the ones IRW's get_tn_contribs.R submitted to this same
# form. The `*Field` booleans are the "display these fields in my results"
# checkboxes — they decide which columns land in the CSV export, so the
# parser's expected column set is determined here. Keep them in sync with
# parsers/tennessee.py's COLUMN_ALIASES if you change them.
#
# Run `python3 src/pipeline/scrapers/tennessee.py --discover` to print the live
# forms' actual field names before assuming these are still current.

def ce_search_body(year: int, relation: str) -> dict:
    """Contributions & Expenditures search POST body for one election year.

    `relation` is "contributions" or "expenditures" — TNCAMP switches the
    whole result shape (and therefore the exported columns) off `searchType`.
    Every filter is left wide open so the search returns the full year."""
    body = {
        "searchType":            relation,
        "toType":                "both",
        # who the money came from — all four on, i.e. no filtering
        "fromCandidate":         True,
        "fromPAC":               True,
        "fromIndividual":        True,
        "fromOrganization":      True,
        # who the money went to — same idea, added 2026-07-25: `toType` alone
        # no longer seems to be enough, these default to unchecked (i.e.
        # "match nothing") if omitted. See module docstring.
        "toCandidate":           True,
        "toPac":                 True,
        "toOther":               True,
        "electionYearSelection": "",
        "yearSelection":         year,
        "recipientName":         "",
        "contributorName":       "",
        "employer":              "",
        "occupation":            "",
        "zipCode":               "",
        "candName":              "",
        "vendorName":            "",
        "vendorZipCode":         "",
        "purpose":               "",
        "typeOf":                "all",
        "amountSelection":       "equal",
        "amountDollars":         "",
        "amountCents":           "",
        # Report-period filter, added 2026-07-25. No blank/"all" option was
        # visible in --discover's (truncated) option list, so sent explicitly
        # empty — same convention as electionYearSelection above — rather
        # than omitted, to avoid it defaulting to whatever option the live
        # form would preselect (its first listed option is "1st Quarter",
        # not a placeholder).
        "reportSelection":       "",
        # result columns
        "typeField":                  True,
        "adjustmentField":            True,
        "amountField":                True,
        "dateField":                  True,
        "electionYearField":          True,
        "reportNameField":            True,
        "recipientNameField":         True,
        # Added 2026-07-25: TNCAMP's export header for this column now looks
        # to be driven by candidatePACNameField rather than
        # recipientNameField — see module docstring. Requesting both is
        # harmless (unknown keys are ignored) and covers either shape.
        "candidatePACNameField":      True,
        "candidateForField":         True,
        "soField":                   True,
        "contributorNameField":       True,
        "contributorAddressField":    True,
        "contributorOccupationField": True,
        "contributorEmployerField":   True,
        "descriptionField":           True,
        "_continue":                  "Search",
    }
    if relation == "expenditures":
        # The expenditure side of the form uses vendor-oriented column
        # checkboxes instead of the contributor ones. Sending both sets is
        # harmless — the servlet ignores keys it doesn't know — and means one
        # body shape covers both searches.
        body.update({
            "vendorNameField":    True,
            "vendorAddressField": True,
            "purposeField":       True,
        })
    return body


def cp_search_body(find: str, election=None) -> dict:
    """Candidates & PACs roster search POST body.

    `find` is "candidate" or "pac" — the `searchType` radio group at the top
    of cpsearch.htm. cpsearch.htm is one URL serving two genuinely different
    searches: switching the radio swaps both the *criteria* the form offers
    and the *display checkboxes* it offers, so there is no single body that
    works for both. Hence the two branches below rather than one dict with
    the union of every key.

    The criteria that differ:

      - A candidate search needs `electionYearSelection`. Candidates in TN
        exist per election, and submitting the candidate search with it left
        blank comes back with no results at all (which is what this scraper
        was doing, and why the candidate roster was empty) — it is a required
        criterion, not an optional filter. `election` is therefore mandatory
        when find == "candidate", and must be an Election: the field takes the
        select's opaque option ID ("237"), not the year ("2023"). Posting the
        year matches no option and returns nothing, indistinguishably from
        form drift.
      - office/district/party/winner are candidate-only criteria and are not
        sent on the PAC side at all.

    The display checkboxes that differ: the candidate side offers
    office/district/primary/general/election-year columns; the PAC side offers
    committee/officers/created/closed/responsible-individual instead. Sending
    the wrong set is not merely inert — an unrecognised `*Field` key means the
    column simply never appears in the export, which is the same failure mode
    the 2026-07-25 --discover run traced the missing Office Sought / Committee
    Affiliation / Contact Info columns to.

    Field names below were rewritten 2026-07-25 after --discover showed this
    form had drifted much further from the originally-assumed names than
    ce_search_body() had: the radio group, name filter, office/district/party
    filters, winner flag and every mis-fired display checkbox were all wrong
    — see module docstring for the full list. If office/committee-affiliation
    /contact-info columns still come out blank after this fix, that's a
    genuine TNCAMP export gap, not a leftover field-name miss."""
    if find == "candidate":
        if election is None:
            # Guard rather than silently posting a blank: TNCAMP answers a
            # candidate search with no election selected with an empty result
            # set, indistinguishable downstream from "the form drifted".
            raise ValueError(
                "cp_search_body(find='candidate') requires an Election — "
                "TNCAMP's candidate search returns nothing without one."
            )
        if not getattr(election, "value", None):
            raise ValueError(
                f"cp_search_body(find='candidate') needs an Election carrying "
                f"the form's option ID, got {election!r}. The year alone is "
                f"not a valid electionYearSelection value — see "
                f"discover_elections()."
            )
        return {
            "searchType":            find,
            "name":                  "",
            # The one criterion that makes this search return anything. Note
            # this is the opaque option ID, not the year.
            "electionYearSelection": str(election.value),
            "officeSelection":       "",
            "districtSelection":     "",
            "partySelection":        "",
            "winner":                "",
            # result columns — candidate side
            "nameField":             True,
            "contactField":          True,
            "treasurerNameField":    True,
            "treasurerContactField": True,
            "partyField":            True,
            "officeField":           True,
            "districtField":         True,
            "primaryField":          True,
            "generalField":          True,
            "electionYearField":     True,
            "_continue":             "Search",
        }

    # PAC side. A PAC is a standing registration, not a per-cycle entity, so
    # the form offers no election-year criterion here — one search returns the
    # whole roster. The candidate-only criteria (office/district/party/winner/
    # electionYearSelection) are deliberately absent rather than sent blank.
    return {
        "searchType":                 find,
        "name":                       "",
        # result columns — PAC side
        "nameField":                  True,
        "contactField":               True,
        "treasurerNameField":         True,
        "treasurerContactField":      True,
        "partyField":                 True,
        "committeeField":             True,
        "officersField":              True,
        "createdField":               True,
        "closedField":                True,
        "responsibleIndividualField": True,
        "_continue":                  "Search",
    }


# Horizontal scope groupings. TN calls its non-candidate committees "PACs".
# Values are the `searchType` radio's own values (singular) — sending the
# plural "candidates"/"pacs" here (as this dict did before 2026-07-25) never
# matched any option on the live form.
ENTITY_FIND_TYPE = {"candidates": "candidate", "pacs": "pac"}

# Which entity rosters are searched once per election year, and which are
# searched once outright. Keyed the same way as ENTITY_FIND_TYPE so run() can
# plan both rosters through one loop instead of special-casing "candidates"
# by name in three places. See cp_search_body() for why candidates are
# year-scoped and PACs are not.
ENTITY_YEAR_SCOPED = {"candidates": True, "pacs": False}


# ============================ http session ============================
def build_session() -> requests.Session:
    """Session with browser headers and retries on the transient status codes.

    500 is deliberately excluded from status_forcelist: TNCAMP returns 500 on
    bad form input, not under load, so retrying a 500 replays a request that
    will never succeed and hides the real problem."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=2,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ============================== discovery =============================
def discover_form_fields(session: requests.Session, url: str) -> None:
    """Print every <input>/<select> name (and select options) on a live form.

    The escape hatch for form drift — TNCAMP's field names can only be
    confirmed from the live page, so print them rather than guess."""
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    print(f"\n--- form fields on {url} ---")
    for tag in soup.find_all(["input", "select", "textarea"]):
        name = tag.get("name") or tag.get("id") or "(no name)"
        if tag.name == "select":
            options = [(opt.get("value"), opt.get_text(strip=True))
                       for opt in tag.find_all("option")]
            print(f"select name={name!r}  ({len(options)} options)")
            for value, label in options[:15]:
                print(f"    value={value!r}  label={label!r}")
            if len(options) > 15:
                print(f"    ... and {len(options) - 15} more")
        else:
            print(f"{tag.name} name={name!r} type={tag.get('type')!r} "
                  f"value={tag.get('value')!r}")
    print("--- end form fields ---\n")


def discover_valid_years(session: requests.Session,
                         url: str = CE_SEARCH_URL) -> list[int]:
    """Read the *reporting*-year options off a live search form.

    Returns a sorted list of 4-digit years, or [] if no such select is found.
    This reads `yearSelection` — cesearch.htm's plain reporting-year filter,
    whose option values really are the years ("2024" -> value "2024") — and
    drives the contributions/expenditures walks.

    It deliberately does NOT read `electionYearSelection`, which is a
    different thing in two ways: its options are keyed by opaque database IDs
    rather than years, and there are several per year. See
    discover_elections() for that one."""
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")

    # Prefer the select actually named `yearSelection`; fall back to "whichever
    # select has the most 4-digit option values" only if the form has been
    # renamed, since that heuristic is what found it before the name was known.
    selects = soup.find_all("select", attrs={"name": "yearSelection"}) \
        or soup.find_all("select")

    best: list[int] = []
    for select in selects:
        values  = [opt.get("value") for opt in select.find_all("option")]
        numeric = [int(v) for v in values if v and re.fullmatch(r"\d{4}", v)]
        if len(numeric) > len(best):
            best = numeric
    return sorted(set(best))


class Election(NamedTuple):
    """One option of the `electionYearSelection` select.

    TN does not have "the 2023 election" — it has five of them (the regular
    cycle plus HOUSE 3, HOUSE 51, HOUSE 52, HOUSE 86 and HOUSE 86 AUG GEN
    specials), each its own option with its own candidates. So an election,
    not a year, is the unit the candidate roster is walked in.

    value  the form's own option value: an opaque database ID ("237"), NOT
           the year. This is what gets POSTed.
    year   the 4-digit year parsed out of the option's label, used for
           --start-year/--end-year filtering and for the raw filename.
    label  the full option text ("2023 (HOUSE 51)"), for logging.
    """
    value: str
    year:  int
    label: str

    @property
    def key(self) -> str:
        """Stable per-election identifier used in raw filenames and as the
        manifest's `year` value.

        Has to be unique per *election*, not per year: keying on the year
        alone would have all five 2023 specials write over each other's
        `candidates_2023_p001.csv` and would let the first one to finish claim
        the year's `complete` sentinel for the other four. The year stays at
        the front so the filename sorts and greps by year, and so the parser's
        year_from_filename() still finds it."""
        return f"{self.year}_e{self.value}"


# The option label carries the year plus, for a special election, a
# parenthesized district — and TNCAMP renders it across several lines with
# stray whitespace ("2023\r\n  \r\n    (HOUSE 51)"). Only the leading year is
# needed; the rest is collapsed for the log line.
def _election_label(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def discover_elections(session: requests.Session,
                       url: str = CP_SEARCH_URL) -> list[Election]:
    """Read the `electionYearSelection` options off a live form.

    Returns them newest-year-first, or [] if the select can't be found.

    This exists separately from discover_valid_years() because the two
    selects are not the same shape at all. `yearSelection` is a plain year
    filter whose values are years. `electionYearSelection` is a lookup table:

        <option value="234">2026</option>
        <option value="237">2023 (HOUSE 3)</option>
        <option value="238">2023 (HOUSE 51)</option>

    The values are opaque row IDs, and the year only exists in the label. Two
    consequences the candidate walk depends on:

      - The ID is the only thing the form accepts. Posting "2023" — which is
        what this scraper did before, because discover_valid_years() scanned
        for 4-digit *values*, found none here, and fell back to a fabricated
        MIN_YEAR..current range — matches no option, so TNCAMP returns an
        empty result set for every single search. That is exactly the "no
        export link on page 1" storm this replaces.
      - IDs cannot be guessed or reconstructed offline, so there is no static
        fallback for this list the way there is for yearSelection. A failed
        lookup has to be an error, not a default — see run()."""
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")

    select = soup.find("select", attrs={"name": "electionYearSelection"})
    if select is None:
        return []

    elections: list[Election] = []
    seen: set[str] = set()
    for opt in select.find_all("option"):
        value = (opt.get("value") or "").strip()
        # The placeholder ("- Select Election Year-") has an empty value.
        if not value or value in seen:
            continue
        label = _election_label(opt.get_text())
        m = re.search(r"(?:19|20)\d{2}", label)
        if not m:
            continue
        seen.add(value)
        elections.append(Election(value=value, year=int(m.group()), label=label))

    # Newest first, and stable within a year, so a --start-year run and a full
    # run walk the same elections in the same order.
    return sorted(elections, key=lambda e: (-e.year, e.value))


# ========================= manifest helpers ============================
# strip_manifest()/upsert_manifest() are read-modify-write on one shared CSV
# file. Harmless when years run one at a time, but with --workers > 1 several
# year-walks can finish within the same instant and race on this file — the
# usual outcome of an unguarded read-modify-write under concurrency is a lost
# update (one thread's write clobbers another's), not a crash, so it would go
# unnoticed as silently-missing manifest rows rather than an obvious error.
# One lock serializes every manifest read+write across all worker threads.
_manifest_lock = threading.Lock()


def load_manifest() -> dict[str, dict]:
    """filename -> manifest row."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {row["filename"]: row for row in csv.DictReader(f)}


def completed_years(relation: str) -> set[str]:
    """Years whose page walk ran to completion for this relation.

    Keyed on the `page == "complete"` sentinel row, not on "has any page on
    disk" — see the module docstring: TNCAMP's pagination is session-based, so
    a partially walked year has to be redone from page 1, and treating it as
    done would freeze it at however many pages it got through."""
    return {row["year"] for row in load_manifest().values()
            if row["relation_type"] == relation and row["year"]
            and row["page"] == COMPLETE_SENTINEL}


def strip_manifest(keep_fn) -> None:
    """Rewrite the manifest keeping only rows where keep_fn(row) is True."""
    with _manifest_lock:
        if not MANIFEST.exists():
            return
        with open(MANIFEST, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
            w.writeheader()
            w.writerows(r for r in rows if keep_fn(r))


def upsert_manifest(record: dict) -> None:
    """Add or overwrite the manifest entry for record['filename']."""
    with _manifest_lock:
        rows = []
        if MANIFEST.exists():
            with open(MANIFEST, newline="", encoding="utf-8") as f:
                rows = [r for r in csv.DictReader(f) if r["filename"] != record["filename"]]
        rows.append(record)
        with open(MANIFEST, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
            w.writeheader()
            w.writerows(rows)


# =========================== page-walk helpers ==========================
def _export_link(soup: BeautifulSoup) -> str | None:
    """Absolute URL of the CSV export link on a results page, or None.

    Re-read from every page rather than cached from page 1: it's a relative
    href TNCAMP regenerates per page, and reusing a stale one is how you
    silently download page 1 N times (a bug in the original R script this
    replaces)."""
    for a in soup.select(".exportlinks a"):
        href = a.get("href")
        if not href:
            continue
        # The export block can also hold XML/Excel/PDF links; prefer the CSV
        # one when it's identifiable by href or link text.
        if "csv" in (href + a.get_text()).lower():
            return href if href.startswith("http") else HOST + href
    first = soup.select_one(".exportlinks a")
    if first is None or not first.get("href"):
        return None
    href = first["href"]
    return href if href.startswith("http") else HOST + href


def _has_next_page(soup: BeautifulSoup) -> bool:
    """True when TNCAMP renders its 'more results' button on this page."""
    return soup.select_one(".btn-blue") is not None


def _result_count(soup: BeautifulSoup) -> int | None:
    """Row count parsed out of the results banner, e.g. '103 results found'."""
    banner = soup.select_one(".pagebanner")
    if banner is None:
        return None
    m = re.search(r"[\d,]+", banner.get_text())
    return int(m.group().replace(",", "")) if m else None


def _csv_rows(content: bytes) -> int:
    """Best-effort data-row count of a downloaded CSV (newlines minus header)."""
    return max(content.count(b"\n") - 1, 0)


def _walk_results(session: requests.Session, log, relation: str, year: str,
                  search_url: str, body: dict, results_url: str, next_url: str,
                  stem: str, label: str | None = None) -> tuple[int, int, int, bool]:
    """Run one search and download every results page as its own CSV.

    Returns (pages_downloaded, rows_downloaded, pages_failed, complete).

    `complete` is True only if the walk reached a page with no "next" button —
    that's what earns the year its manifest sentinel. A run that dies partway
    leaves the year unmarked, so the next incremental run redoes it.

    A missing export link on page 1 returns zero pages rather than raising, so
    the caller can weigh it against the rest of the run before deciding whether
    it means "no data for this year" or "the form broke"."""
    # Priming GET: TNCAMP hands out the session cookie the POST needs here.
    session.get(search_url, timeout=60)
    post = session.post(search_url, data=body, timeout=120)
    # TNCAMP answers 500 on bad form input (see build_session), which is
    # precisely the drift --discover exists to catch — so surface it rather
    # than walking on and reporting "no results".
    post.raise_for_status()

    pages = rows = failed = 0
    page_num = 0
    url = results_url
    # `label` is for the log only — the caller can pass something friendlier
    # than the manifest key, e.g. "candidates 2023 (HOUSE 51)" instead of
    # "candidates 2023_e238".
    label    = label or f"{relation} {year}".strip()
    complete = False
    # Old page files for this (relation, year) are cleared lazily — only once
    # the first replacement page is actually in hand. Clearing up front would
    # mean a walk that fails on page 1 destroys the last good copy and puts
    # nothing back.
    cleared = False

    while page_num < MAX_PAGES_PER_YEAR:
        page_num += 1
        try:
            resp = session.get(url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            # A results page that won't load ends this year's walk, but must
            # not take the rest of the run down with it.
            log.page_scrape_error(entity=relation, page_id=f"{year}:{page_num}",
                                  error=str(e))
            failed += 1
            break
        soup = BeautifulSoup(resp.text, "html.parser")

        link = _export_link(soup)
        if link is None:
            if page_num == 1:
                log.warning(f"  {label}: no export link on page 1 (no results, "
                            f"or the form fields have drifted — run --discover)")
                # Nothing to replace the old files with; leave them alone.
                return pages, rows, failed, False
            break

        filename = f"{stem}_p{page_num:03d}.csv"
        dest     = RAW_DIR / filename
        banner   = _result_count(soup)
        if banner is not None and page_num == 1:
            log.info(f"  {label}: results banner reports {banner}")

        log.file_download_start(filename=filename)
        t_file = time.perf_counter()
        try:
            csv_resp = session.get(link, timeout=180)
            csv_resp.raise_for_status()
            content = csv_resp.content
        except Exception as e:
            log.file_download_error(filename=filename, error=str(e))
            failed += 1
        else:
            if not cleared:
                _clear_pages(relation, year)
                cleared = True
            dest.write_bytes(content)
            n_rows = _csv_rows(content)
            log.file_download_ok(filename=filename, bytes=len(content),
                                 rows=n_rows,
                                 duration_s=round(time.perf_counter() - t_file, 2))
            upsert_manifest({
                "relation_type": relation,
                "year":          year,
                "page":          page_num,
                "filename":      filename,
                "bytes":         len(content),
                "rows":          n_rows,
                "downloaded_at": time.strftime("%Y-%m-%d"),
            })
            pages += 1
            rows  += n_rows

        if not _has_next_page(soup):
            complete = True
            break

        # Courtesy pacing between pages — TN.gov is a small state app, not a CDN.
        time.sleep(random.uniform(1.0, 3.0))
        url = next_url

    if page_num >= MAX_PAGES_PER_YEAR and not complete:
        log.warning(f"  {label}: hit MAX_PAGES_PER_YEAR ({MAX_PAGES_PER_YEAR}) — "
                    f"results may be truncated")

    # Only a clean finish with no failed pages earns the sentinel; a year with
    # a hole in it must be re-walked next run.
    complete = complete and failed == 0 and pages > 0
    if complete:
        upsert_manifest({
            "relation_type": relation,
            "year":          year,
            "page":          COMPLETE_SENTINEL,
            "filename":      f"{stem}_p001.csv",
            "bytes":         "",
            "rows":          rows,
            "downloaded_at": time.strftime("%Y-%m-%d"),
        })

    return pages, rows, failed, complete


def _clear_pages(relation: str, year: str) -> None:
    """Delete previously downloaded page files for one (relation, year).

    Needed before a re-walk: a year that shrank from 12 pages to 9 would
    otherwise leave three stale page files on disk for the parser to read as
    if they were current."""
    for path in RAW_DIR.glob(f"{relation}_{year}_p*.csv" if year
                             else f"{relation}_p*.csv"):
        path.unlink()
    strip_manifest(lambda r: not (r["relation_type"] == relation
                                  and r["year"] == year))


def _walk_one_transaction_year(log, relation: str, year: int) -> dict:
    """Run one (relation, year) walk on its own session, for the thread pool.

    Each task gets its own `requests.Session()` — TNCAMP's "more results"
    cursor lives in the session cookie (see module docstring), so sharing one
    session across concurrent walks would have them stomp on each other's
    pagination state. A small random stagger spreads out the moment each
    worker's first request actually hits the server, instead of every worker
    opening its first connection in the same instant."""
    time.sleep(random.uniform(0, 5.0))
    session = build_session()
    y = str(year)
    t_year = time.perf_counter()
    pages, rows, failed, _complete = _walk_results(
        session, log, relation, y,
        CE_SEARCH_URL, ce_search_body(year, relation),
        CE_RESULTS_URL, CE_NEXT_URL,
        stem=f"{relation}_{y}",
    )
    log.page_scrape_complete(filename=f"{relation}_{y}", rows=rows,
                             duration_s=round(time.perf_counter() - t_year, 1),
                             ok=pages, err=failed)
    return {"relation": relation, "year": y, "pages": pages, "rows": rows,
            "failed": failed}


def _walk_one_entity_roster(log, relation: str,
                            election: Election | None = None) -> dict:
    """Run one entity-roster walk on its own session.

    `election` is required for the election-scoped rosters (candidates) and
    must be None for the rest (pacs) — see ENTITY_YEAR_SCOPED and
    cp_search_body(). An election-scoped walk writes
    `candidates_{year}_e{id}_p{NNN}.csv`; the `{year}_e{id}` key goes into the
    manifest's `year` column too, so _clear_pages()'s glob, the `complete`
    sentinel and the parser's year-bearing glob all keep working while still
    telling TN's five separate 2023 elections apart. An unscoped walk keeps
    the bare `pacs_p{NNN}.csv`.

    Runs on its own session for the same reason the transaction walks do:
    TNCAMP's "more results" cursor lives in the session cookie, so concurrent
    walks sharing one session would stomp on each other's pagination. The
    random stagger spreads out the first request of each worker."""
    scoped = ENTITY_YEAR_SCOPED.get(relation, False)
    if scoped and election is None:
        raise ValueError(f"{relation} is election-scoped — "
                         f"_walk_one_entity_roster needs an Election")
    time.sleep(random.uniform(0, 5.0))
    session  = build_session()
    key      = election.key if scoped else ""
    stem     = f"{relation}_{key}" if scoped else relation
    t_roster = time.perf_counter()
    pages, rows, failed, _complete = _walk_results(
        session, log, relation, key,
        CP_SEARCH_URL,
        cp_search_body(ENTITY_FIND_TYPE[relation], election if scoped else None),
        CP_RESULTS_URL, CP_NEXT_URL,
        stem=stem,
        label=f"{relation} {election.label}" if scoped else relation,
    )
    log.page_scrape_complete(filename=stem, rows=rows,
                             duration_s=round(time.perf_counter() - t_roster, 1),
                             ok=pages, err=failed)
    return {"relation": relation, "year": key, "pages": pages, "rows": rows,
            "failed": failed}


def _pending(log, relation: str, items: list, *, force: bool,
             year_range_active: bool, current_year: int) -> tuple[list, int]:
    """Filter a work list down to the units this relation still needs.

    Returns (to_walk, skipped). The incremental rule — skip a unit only if it
    carries the manifest's `complete` sentinel, never skip one in the open
    cycle, and skip nothing at all under --force or an explicit year range —
    is identical for transaction relations and for the election-scoped
    candidate roster, so it lives here rather than being written out twice and
    drifting apart.

    `items` is a list of either plain ints (a transaction year, whose manifest
    key is just the year) or Elections (whose manifest key is `key`, since one
    year can hold several elections — see Election.key)."""
    done    = completed_years(relation)
    walk    = []
    skipped = 0
    for item in items:
        if isinstance(item, Election):
            key, year = item.key, item.year
        else:
            key, year = str(item), int(item)
        is_open_cycle = year >= current_year
        if (key in done and not force and not year_range_active
                and not is_open_cycle):
            log.file_download_skip(filename=f"{relation}_{key}_p001.csv")
            skipped += 1
            continue
        walk.append(item)
    return walk, skipped


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
    workers: int = DEFAULT_WORKERS,
):
    """Download Tennessee TNCAMP data.

    Vertical scope (applies to contributions, expenditures and the candidate
    roster — the PAC roster is the one relation with no election year on its
    form, so it is always a single always-refreshed walk):
        (no flag)               incremental — years already complete in the
                                manifest are skipped; the current year is
                                always re-walked
        --start-year/--end-year restrict, and force a refresh of, the year range
        --force                 re-walk every year in scope

    Horizontal scope:
        (no flag)               contributions + expenditures + candidate/PAC roster
        --transactions          contributions + expenditures
        --contributions         contributions only
        --expenditures          expenditures only
        --entities              candidate + PAC roster
        --candidates            candidate roster only
        --committees            PAC roster only (TN calls committees "PACs")

    `workers` (--workers) is how many (relation, year) searches run at once,
    each on its own session — see "Concurrency" in the module docstring.
    """
    log = get_logger("tennessee", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Tennessee scraper")
    log._emit("scrape_started", force=force, start_year=start_year, end_year=end_year,
              entities=entities, transactions=transactions,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    files_ok = files_err = years_skip = 0

    do_all = not any([entities, transactions, contributions,
                      expenditures, candidates, committees])

    txn_relations: list[str] = []
    if do_all or transactions or contributions:
        txn_relations.append("contributions")
    if do_all or transactions or expenditures:
        txn_relations.append("expenditures")

    entity_relations: list[str] = []
    if do_all or entities or candidates:
        entity_relations.append("candidates")
    if do_all or entities or committees:
        entity_relations.append("pacs")

    try:
        discovery_session = build_session()

        current_year      = datetime.today().year
        year_range_active = start_year is not None or end_year is not None

        def _clip(items: list) -> list:
            """Apply --start-year/--end-year to years or Elections alike."""
            def yr(i):
                return i.year if isinstance(i, Election) else int(i)
            if start_year is not None:
                items = [i for i in items if yr(i) >= start_year]
            if end_year is not None:
                items = [i for i in items if yr(i) <= end_year]
            return items

        # ── Transaction years: cesearch.htm's `yearSelection` ────────────
        # Values here really are years, so a static MIN_YEAR..current fallback
        # is a legitimate (if blunt) substitute if the lookup fails.
        years: list[int] = []
        if txn_relations:
            years = discover_valid_years(discovery_session, CE_SEARCH_URL)
            if years:
                log.info(f"  Discovered {len(years)} transaction years on the "
                         f"live form: {years[0]}–{years[-1]}")
            else:
                years = list(range(MIN_YEAR, current_year + 1))
                log.warning(f"  Could not read the transaction year selector from "
                            f"the live form — falling back to "
                            f"{MIN_YEAR}–{current_year}")
            years = _clip(years)

        # ── Candidate elections: cpsearch.htm's `electionYearSelection` ──
        # Not years — opaque option IDs, several per year (TN has five
        # separate 2023 elections). See discover_elections(). There is no
        # fallback that can work here: the IDs exist only on the live form, so
        # a failed lookup is fatal rather than something to paper over with a
        # fabricated year range. Doing the latter is precisely how this ended
        # up POSTing "2002".."2026" into a field that accepts none of them and
        # getting 25 empty searches back.
        elections: list[Election] = []
        if any(ENTITY_YEAR_SCOPED.get(r) for r in entity_relations):
            elections = discover_elections(discovery_session, CP_SEARCH_URL)
            if not elections:
                raise RuntimeError(
                    "Could not read the `electionYearSelection` options from "
                    f"{CP_SEARCH_URL}. TN's candidate search is keyed by opaque "
                    "per-election option IDs that only exist on the live form, "
                    "so there is no fallback — the candidate roster cannot be "
                    "scraped without them. Run `python3 "
                    "src/pipeline/scrapers/tennessee.py --discover` to see what "
                    "the select looks like now."
                )
            log.info(f"  Discovered {len(elections)} candidate elections on the "
                     f"live form: {elections[-1].year}–{elections[0].year} "
                     f"({len({e.year for e in elections})} distinct years; the "
                     f"surplus are special elections)")
            elections = _clip(elections)

        # ── Transactions ────────────────────────────────────────────────
        # Counts searches that actually ran vs. searches that produced pages.
        # Used at the end to tell "the form contract broke" apart from "this
        # particular year happens to be empty" — one empty year is normal,
        # every year empty is not.
        searches_run = searches_with_pages = 0

        # Build the full (relation, year) task list up front, skipping years
        # already complete in the manifest, then hand it to a thread pool —
        # each task opens its own session (see _walk_one_transaction_year).
        # This is what took a full single-threaded history walk from ~3 days
        # to roughly 3 days / workers — see "Concurrency" in the module
        # docstring for why running several years at once is safe here (each
        # year's pagination cursor lives in its own session's cookie).
        tasks: list[tuple[str, int]] = []
        for relation in txn_relations:
            to_walk, skipped = _pending(
                log, relation, years, force=force,
                year_range_active=year_range_active, current_year=current_year)
            years_skip += skipped
            tasks.extend((relation, year) for year in to_walk)

        if tasks:
            log.info(f"\nTennessee transactions: {len(tasks)} (relation, year) "
                     f"searches queued across {workers} worker(s)")
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                futures = {pool.submit(_walk_one_transaction_year, log, relation, year): (relation, year)
                          for relation, year in tasks}
                for fut in as_completed(futures):
                    relation, year = futures[fut]
                    try:
                        result = fut.result()
                    except Exception as e:
                        # One year's walk raising must not take the rest of
                        # the pool down with it — log and count it as a
                        # failure, same as a page-level error would be.
                        log.page_scrape_error(entity=relation, page_id=str(year), error=str(e))
                        files_err += 1
                        searches_run += 1
                        continue
                    files_ok  += result["pages"]
                    files_err += result["failed"]
                    searches_run += 1
                    if result["pages"]:
                        searches_with_pages += 1

        # ── Entity roster (candidates / PACs) ───────────────────────────
        # cpsearch.htm is one URL serving two different searches, and they are
        # not shaped alike (see cp_search_body):
        #
        #   candidates  election-scoped. A candidate exists per election and
        #               the form's electionYearSelection is a required
        #               criterion, not a filter — a candidate search with none
        #               selected returns nothing. The unit is one *election*,
        #               not one year: TN's list holds ~50 elections across ~28
        #               years because special elections get their own entries
        #               (five separate 2023s), each with its own candidates.
        #               So this is a walk per election, landing as
        #               candidates_{year}_e{id}_p{NNN}.csv, with the same
        #               incremental treatment as the transaction years — an
        #               election with the manifest's `complete` sentinel is
        #               skipped unless --force or an explicit year range is
        #               given, and anything in the open cycle is always
        #               re-walked. Past elections' candidate lists are closed
        #               history.
        #   pacs        not election-scoped. A PAC is a standing registration
        #               with no election year on the form at all, so one walk
        #               pulls the whole roster, always refreshed — it's a
        #               single cheap search and registration/treasurer details
        #               change continuously.
        #
        # Both go through the same pool as one flat (relation, Election|None)
        # task list, so a ~50-election candidate walk gets the same concurrency
        # the transaction walks do instead of running one at a time.
        entity_tasks: list[tuple[str, Election | None]] = []
        for relation in entity_relations:
            if ENTITY_YEAR_SCOPED.get(relation):
                to_walk, skipped = _pending(
                    log, relation, elections, force=force,
                    year_range_active=year_range_active, current_year=current_year)
                years_skip += skipped
                entity_tasks.extend((relation, e) for e in to_walk)
            else:
                entity_tasks.append((relation, None))

        if entity_tasks:
            log.info(f"\nTennessee entity roster: {len(entity_tasks)} search(es) "
                     f"queued across {workers} worker(s) "
                     f"({sorted(set(r for r, _ in entity_tasks))})")
            with ThreadPoolExecutor(max_workers=max(1, min(workers, len(entity_tasks)))) as pool:
                futures = {pool.submit(_walk_one_entity_roster, log, relation, election):
                           (relation, election)
                           for relation, election in entity_tasks}
                for fut in as_completed(futures):
                    relation, election = futures[fut]
                    try:
                        result = fut.result()
                    except Exception as e:
                        log.page_scrape_error(entity=relation,
                                              page_id=election.key if election else "roster",
                                              error=str(e))
                        files_err += 1
                        searches_run += 1
                        continue
                    files_ok  += result["pages"]
                    files_err += result["failed"]
                    searches_run += 1
                    if result["pages"]:
                        searches_with_pages += 1

        # Every search in the run coming back with no export link, and no HTTP
        # failures to explain it, means the form contract changed — TN does not
        # have an empty decade. Fail loudly so orc marks the state failed,
        # rather than leaving the parser to find no input and report a
        # mysteriously empty state. A single empty year is left alone.
        #
        # Checked once here, after both the transaction and the roster walks,
        # rather than between them: a `--candidates`-only or `--committees`-only
        # run has no transaction searches at all, and running the check early
        # would let a totally broken cpsearch.htm body pass silently because
        # `searches_run` was still 0 at that point.
        if searches_run and not searches_with_pages and not files_err:
            raise RuntimeError(
                f"TNCAMP returned no CSV export link for any of the {searches_run} "
                f"searches in this run. The search form's field names have most "
                f"likely changed — run `python3 src/pipeline/scrapers/tennessee.py "
                f"--discover` and update ce_search_body() / cp_search_body()."
            )

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} pages downloaded, "
                 f"{years_skip} years skipped, {files_err} errors")
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


# ================================ CLI ==================================
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download Tennessee TNCAMP campaign finance data."
    )

    ap.add_argument("--discover", action="store_true",
                    help="print the live search forms' field names and exit "
                         "(run this first, and after any TN.gov redesign)")

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-walk every year in scope, wiping their manifest entries")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest election year to download (inclusive)")
    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest election year to download (inclusive, ≤ current year)")

    ap.add_argument("--transactions",  action="store_true", help="contributions + expenditures")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--entities",      action="store_true", help="candidate + PAC roster")
    ap.add_argument("--candidates",    action="store_true", help="candidate roster only")
    ap.add_argument("--committees",    action="store_true",
                    help="PAC roster only (TN calls committees 'PACs')")

    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS, metavar="N",
                    help=f"concurrent (relation, year) searches, each on its own "
                         f"session (default {DEFAULT_WORKERS}); use 1 for the old "
                         f"fully-sequential behavior — see 'Concurrency' in the "
                         f"module docstring")

    args, _ = ap.parse_known_args()

    if args.discover:
        with build_session() as _session:
            for _url in (CE_SEARCH_URL, CP_SEARCH_URL):
                try:
                    discover_form_fields(_session, _url)
                except Exception as _e:
                    print(f"[!] could not read {_url}: {_e}")
        print("Compare the names above against ce_search_body() / cp_search_body() "
              "in this file, and update any that have changed before scraping.")
        sys.exit(0)

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
            workers=args.workers,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
