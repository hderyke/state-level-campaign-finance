"""
src/pipeline/fec_ie.py — FEC "nonfederal candidate" independent-expenditure
enrichment, shared across state scrapers.

Some Super PACs and other committees registered ONLY with the FEC (a federal
agency) spend money on nonfederal (state/local) races — this money never has
to be reported to the state itself, so it's otherwise invisible to a
state-sourced pipeline. Filers self-describe this spending in the free-text
`disbursement_description` field of their FEC Schedule B (disbursements)
filings, e.g.:

    "OH NONFEDERAL CANDIDATE I.E./SUPPORT-RAMASWAMY - DIGITAL MEDIA"
    "MAILER ON NONFEDERAL WYOMING CANDIDATE - BROWN"
    "NONFEDERAL EXPENDITURE: DIGITAL ADS SUPPORTING STATE CANDIDATE"

There is no structured field tying a Schedule B disbursement to a nonfederal
candidate or a support/oppose stance (Schedule E, the FEC's structured
independent-expenditure schedule, only covers FEDERAL candidates) — so this
is fundamentally a free-text, best-effort signal:

  - State attribution: some filers write the state abbreviation, some the
    full name, many omit it entirely. Rows with no state token anywhere in
    the description are NOT attributable and are dropped rather than
    guessed — see `_mentions_state()`.
  - Candidate name + stance: wildly inconsistent phrasing per filer. This
    module does NOT attempt to parse that — it returns the raw disbursement
    rows; parsing candidate/stance is left to a per-filer pattern table
    (see src/aliases/fec_ie_patterns.csv, applied in parsers/ohio.py) so an
    unrecognized filer's rows keep their raw text rather than a guessed
    structure.

Also exposes fetch_committee_receipts() (2026-09-06) -- the OTHER side of
the same committee's filings: who actually funds it (FEC Schedule A),
rather than what it spends (Schedule B, above). Deliberately committee_id-
scoped rather than a free-text search: we only ever call it for a
committee_id already proven, via its own disbursement language, to be
doing state-level IE spending (src/aliases/fec_ie_patterns.csv), so there's
no state-attribution guesswork on this side. Receipts are NEVER attributed
to a candidate or a support/oppose side -- a committee's money is fungible
across its whole account, so the honest unit for this data is the
committee itself, not one candidate's race (see parsers/ohio.py and
cloud/supabase/rpc_candidate_profile.sql for how this gets surfaced without
inventing a per-dollar attribution the source doesn't support).

Verified live against https://api.open.fec.gov/v1/schedules/schedule_b/ :
`disbursement_description` searches ACROSS ALL COMMITTEES (no committee_id
needed) and does fuzzy/token matching, not exact substring — a single query
"nonfederal candidate" matched all three example phrasings above despite
none of them containing that exact phrase. Volume is small (a few hundred
rows nationally per year across all states) so pagination is cheap.
"""

import csv
import os
import re
import sys
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass  # dotenv optional — env vars may already be set

API_BASE = "https://api.open.fec.gov/v1"
PER_PAGE = 100
MAX_RETRIES = 5
TIMEOUT = 30

# Broad net, deliberately short — each phrase is a separate national query
# (cheap: ~400 rows/query across all states/years) whose results get merged
# and deduped by sub_id before state-filtering. Add phrases here as new
# filer conventions are discovered; nothing else needs to change.
QUERY_PHRASES = [
    "nonfederal candidate",
]


def get_api_key(cli_key=None):
    return cli_key or os.environ.get("FEC_API_KEY") or "DEMO_KEY"


def _request(session, path, params, api_key):
    params = dict(params)
    params["api_key"] = api_key
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(f"{API_BASE}{path}", params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt
            print(f"  fec_ie: request error ({exc}); retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = 2 ** attempt
            print(f"  fec_ie: rate limited (429); retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            if attempt == MAX_RETRIES:
                resp.raise_for_status()
            wait = 2 ** attempt
            print(f"  fec_ie: server error ({resp.status_code}); retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"fec_ie: giving up on {path} after {MAX_RETRIES} attempts")


def _flatten(row):
    """Same rationale as fec_committee_export.py's _flatten: drop nested
    committee-metadata objects, keeping the flat fields that already carry
    the useful part (recipient_name/recipient_committee_id etc.).

    Unlike schedule_a, schedule_b has NO flat top-level 'committee_name'
    field at all (verified live) — the filing committee's name only exists
    inside the nested 'committee' object, so it must be extracted here
    before that object is dropped, or every row silently loses its
    committee_name (a tier-1 required field downstream)."""
    row = dict(row)
    committee = row.pop("committee", None)
    if committee:
        row.setdefault("committee_name", committee.get("name"))
    for key in ("recipient_committee",):
        val = row.get(key)
        if isinstance(val, dict):
            row[key] = val.get("committee_id")
    return row


def _search_one_phrase(session, api_key, phrase, min_date, max_date):
    """Page through every schedule_b disbursement (any committee) whose
    description fuzzy-matches `phrase`, within the date range."""
    base_params = {
        "disbursement_description": phrase,
        "min_date": min_date,
        "max_date": max_date,
        "per_page": PER_PAGE,
        "sort": "disbursement_date",
    }
    results = []
    last_index = None
    last_date = None
    while True:
        page_params = dict(base_params)
        if last_index is not None:
            page_params["last_index"] = last_index
            page_params["last_disbursement_date"] = last_date

        data = _request(session, "/schedules/schedule_b/", page_params, api_key)
        page = data.get("results", [])
        if not page:
            break
        results.extend(page)

        indexes = data.get("pagination", {}).get("last_indexes")
        if not indexes or indexes.get("last_index") is None:
            break
        last_index = indexes["last_index"]
        last_date = indexes.get("last_disbursement_date")

        if len(page) < PER_PAGE:
            break
    return results


def _load_state_names():
    """abbr (upper) -> full name (upper), read directly from
    src/aliases/states.csv (kept local/self-contained rather than importing
    src.aliases, matching how each pipeline module is fairly standalone)."""
    path = PROJECT_ROOT / "src" / "aliases" / "states.csv"
    out = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            abbr = (row.get("abbr") or "").strip().upper()
            name = (row.get("name") or "").strip().upper()
            if abbr and name:
                out[abbr] = name
    return out


def _mentions_state(description, state_abbr, state_name):
    """True only if the state's abbreviation or full name appears as a real
    word in the description — not a substring match (e.g. "OH" must not
    match inside "SHOWCASE"). Known limitation: a description that never
    names the state at all (common — see module docstring) is correctly
    excluded here, not guessed some other way."""
    text = (description or "").upper()
    if re.search(rf"\b{re.escape(state_abbr.upper())}\b", text):
        return True
    if state_name and re.search(rf"\b{re.escape(state_name.upper())}\b", text):
        return True
    return False


def _search_committee_receipts(session, api_key, committee_id, min_date, max_date):
    """Page through every schedule_a (receipts) row for one specific
    committee_id within the date range — exact and committee-scoped, unlike
    _search_one_phrase()'s free-text search across every committee
    nationally. We already know which committee_ids matter here (see
    src/aliases/fec_ie_patterns.csv), so there's no fuzzy-matching or
    state-attribution guesswork needed on this side at all."""
    base_params = {
        "committee_id": committee_id,
        "min_date": min_date,
        "max_date": max_date,
        "per_page": PER_PAGE,
        "sort": "contribution_receipt_date",
    }
    results = []
    last_index = None
    last_date = None
    while True:
        page_params = dict(base_params)
        if last_index is not None:
            page_params["last_index"] = last_index
            page_params["last_contribution_receipt_date"] = last_date

        data = _request(session, "/schedules/schedule_a/", page_params, api_key)
        page = data.get("results", [])
        if not page:
            break
        results.extend(page)

        indexes = data.get("pagination", {}).get("last_indexes")
        if not indexes or indexes.get("last_index") is None:
            break
        last_index = indexes["last_index"]
        last_date = indexes.get("last_contribution_receipt_date")

        if len(page) < PER_PAGE:
            break
    return results


def _flatten_receipt(row):
    """Same rationale as _flatten() above, for schedule_a rows. Unlike
    schedule_b, schedule_a's top-level `committee_name` field is also
    always null (verified live) with the real name only inside the nested
    `committee` object -- same fix, extract before dropping."""
    row = dict(row)
    committee = row.pop("committee", None)
    if committee:
        row.setdefault("committee_name", committee.get("name"))
    row.pop("contributor", None)  # always null in practice; not used
    return row


def fetch_committee_receipts(committee_id, min_date, max_date, api_key=None):
    """Return every schedule_a (receipts -- who funds this committee) row
    for one specific, already-known committee_id, within the date range.

    Deliberately NOT attributed to any candidate or support/oppose stance
    here (unlike fetch_state_matches' disbursement side) -- a committee's
    receipts fund its whole account, not one specific race or side of one
    race, so there is no honest per-row candidate signal to extract. Only
    call this for a committee_id already confirmed (via its OWN
    disbursement language matched in fetch_state_matches/QUERY_PHRASES) to
    be doing state-level independent-expenditure spending -- see
    src/aliases/fec_ie_patterns.csv, the closed loop this is meant to stay
    inside of (never call it for an arbitrary/unreviewed committee_id).
    """
    api_key = get_api_key(api_key)
    session = requests.Session()
    rows = [_flatten_receipt(r) for r in _search_committee_receipts(
        session, api_key, committee_id, min_date, max_date)]
    rows.sort(key=lambda r: r.get("contribution_receipt_date") or "")
    return rows


def fetch_state_matches(state_abbr, min_date, max_date, api_key=None, state_name=None):
    """Return deduped, flattened schedule_b rows that (a) fuzzy-match one of
    QUERY_PHRASES and (b) mention this state by abbreviation or full name in
    disbursement_description. One row per FEC `sub_id`, across every
    committee nationally that filed a matching disbursement in range —
    NOT scoped to a single known committee_id.
    """
    api_key = get_api_key(api_key)
    if state_name is None:
        state_name = _load_state_names().get(state_abbr.upper(), "")

    session = requests.Session()
    by_sub_id = {}
    for phrase in QUERY_PHRASES:
        for row in _search_one_phrase(session, api_key, phrase, min_date, max_date):
            sub_id = row.get("sub_id")
            if sub_id is not None and sub_id not in by_sub_id:
                by_sub_id[sub_id] = row

    matches = []
    for row in by_sub_id.values():
        if _mentions_state(row.get("disbursement_description"), state_abbr, state_name):
            flat = _flatten(row)
            flat["matched_state"] = state_abbr.upper()
            matches.append(flat)

    matches.sort(key=lambda r: r.get("disbursement_date") or "")
    return matches
