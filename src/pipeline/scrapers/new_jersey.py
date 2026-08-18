"""
scrapers/new_jersey.py — Download New Jersey campaign finance data.

Source: the NJ Election Law Enforcement Commission (ELEC) "Reports and Data
Search" system at https://www.njelecefilesearch.com.

No Playwright. Every table on the site is a **server-side** DataTables grid
backed by a plain JSON API under /api/, so `requests` is sufficient. The
handlers take a standard DataTables form-encoded body (draw / columns[] /
order[] / start / length / search[]) with ELEC's own filter fields appended
(ElectionYears, OfficeCodes, LastName, NONPACOnly, ...) and return the usual
{draw, recordsTotal, recordsFiltered, data:[...]} envelope.

The entity id is called ENTITY_S throughout ELEC's API. ELEC issues a NEW one
per entity per election cycle, so "BUCCO, ANTHONY M / STATE SENATE / 2020
GENERAL" and the same man's 2020 PRIMARY row are two different ids — hence
id_model="committee" on the parser side.

Acquisition model — a three-stage sweep, not a bulk download:

  1. Entity sweep (paged, one pass per election year per entity kind)
       POST /api/VWEntity/Entities20  with NONPACOnly=true   → candidates
       POST /api/VWEntity/Entities20  with NONPACOnly=false  → PACs / parties
     Written to entities_{year}.csv / pacs_{year}.csv.

  2. Entity detail sweep (one GET per entity, keyed on ENTITY_S)
       GET /api/VWEntity/GetEntityDataWithCommittee?ENTITY_S={eid}
     The only source for committee-level metadata — treasurer, address, and
     the joint-candidates committee an entity files through. Written to
     entity_details_{year}.csv. Skipped entirely by --transactions runs.

  3. Transaction sweep (paged, one pass per entity NAME + YEAR per relation)
       POST /api/VWContributionDetail/GetContBitsDataByObject
       POST /api/VWExpenseDetail/GetBitsDataByObject
     Concatenated into contributions_{year}.csv / expenditures_{year}.csv.
     The two routes are NOT symmetrically named — see the adapter block.

**Stage 3 does not scope by ENTITY_S.** The interactive pages carry ?eid= on
their URL and send an ENTITY_S field, but they send it EMPTY and scope the
query with `EntityName` + `ElectionYears` instead (verified from a live
capture — see the ENDPOINT ADAPTER block). Two consequences:

  - The sweep iterates unique (entity_name, election_year) pairs, not entity
    ids. A name that filed in both the primary and the general for one year is
    ONE request, not two, so this is cheaper than an id sweep.
  - Deduping the target list on (name, year) is mandatory, not an optimization.
    Sweeping per eid would re-fetch and duplicate every joint primary/general
    filer's entire transaction history.
  - The two relations differ in what they give back. The EXPENSE grid returns
    ENTITY_S on every row, so expenditures can still be pinned to one cycle.
    The CONTRIBUTION grid does not — its CONTRIB_S is a *contributor*
    surrogate key (the `cid` the dashboard links use), not an entity or
    transaction id, so contributions are only ever resolvable to name+year.

  A same-name collision within one election year (two different people, one
  name, different towns) would merge into a single result set. This is a real
  if uncommon limitation of the endpoint, not of this scraper — ELEC exposes
  no id-scoped alternative. See docs/states/new_jersey.md § Data Notes.

Caveats:
  - ELEC caps a single query at 65,000 rows ("Download Data - 65000 Max."),
    which is why this sweeps per entity rather than pulling a whole year at
    once. PAGE_SIZE stays well under that.
  - There is no server-side export endpoint to shortcut any of this. The
    "Download Data" / "Excel" buttons are DataTables `buttons-html5` widgets
    that serialize rows already loaded in the browser, so they expose nothing
    the paged API doesn't.
  - ARRAffinity cookies are Azure load-balancer stickiness. `_make_session`
    primes them with a GET to the site root; the API also works cookieless but
    responses are faster and more consistent when pinned to one backend.
  - Source data refreshes between 8-10 AM ET on business days — no point
    running more than once a day.
  - Coverage starts with the 1999 primary. START_YEAR is 2000: the 1990s rows
    are sparse and the scanned reports behind them aren't machine readable.
  - Pre-2021 filings aren't viewable as documents, but their itemized
    contribution and expense rows are still in the search database.
"""

import csv
import itertools
import re
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# Make project root importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
from config import USER_AGENT, resolve_tls, tls_adapter

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "New Jersey" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "New Jersey" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "downloaded_at", "row_count"]

# ========================= state-specific constants ===================

BASE = "https://www.njelecefilesearch.com"

# ELEC began scanning reports with the 1999 primary. 2000 is the practical
# floor for machine-readable itemized data — see the module docstring.
START_YEAR = 2000

# Rows per API request. ELEC's own UI asks for 15; it honours much larger
# values and the hard ceiling is the 65,000-row export cap. 5,000 keeps
# responses to a sane size while cutting round trips ~300x versus the UI.
PAGE_SIZE = 5000

# Concurrent workers for the per-entity transaction sweep, each on its own
# session. That sweep is ~324,000 requests over a full 2000-present backfill
# (162,100 unique name+year pairs × 2 relations, measured from a real entity
# sweep), which is ~54 hours serially and ~4 at 8 workers.
#
# 8 is a deliberate middle: ELEC runs on Azure behind ARRAffinity with no
# observed rate limiting, but it's a small state agency and this is the whole
# reason REQUEST_DELAY_S still exists per worker. Raise it if your runs are
# clean; drop to 1 to get the old serial behaviour back.
DEFAULT_WORKERS = 8

# Politeness delay between requests *per worker*. With 8 workers this is an
# effective ~32 req/s ceiling, not 4.
REQUEST_DELAY_S = 0.25

# Retry budget for a single request before it's logged as an error and skipped.
# Backoff is exponential (2, 4, 8, 16, 32) for ~62s of total tolerance — sized
# deliberately to ride out a VPN drop/reconnect or a laptop waking from sleep,
# which typically take 10-30s. The old linear 3x2s gave only 12s and would
# turn a routine blip into thousands of skipped entities.
MAX_RETRIES     = 5
RETRY_BACKOFF_S = 2.0

# Fraction of per-entity failures above which a sweep is treated as failed
# rather than complete. Without this a network blip mid-sweep would drop a few
# hundred entities, still return "success", and get its year written to the
# manifest — so the gap would never be retried. Silent partial data is worse
# than an obvious failure. A handful of genuinely bad entities out of ~6,000
# shouldn't fail a year, hence 1% rather than zero.
MAX_ERROR_RATE = 0.01

# Successful-but-empty requests tolerated in a per-entity sweep before giving
# up on the whole thing. Most entities genuinely have no transactions (~97%),
# so zero rows is normal in the small — but hundreds in a row means the query
# is malformed, not that the data is absent.
EMPTY_SWEEP_ABORT_AFTER = 400

# Entities spot-checked when a year-wide query returns nothing, to tell a
# genuinely empty year from a broken query before committing to a full
# per-entity sweep.
#
# This distinction is not hypothetical: ELEC has NO itemized expenditure data
# for several early years (2000, 2002 and 2003 all return 0 on every filter
# variant, while 2001 returns 4,412) even though those years have tens of
# thousands of contributions. Assuming "0 means broken" cost ~24 minutes per
# year re-confirming an absence.
EMPTY_YEAR_SPOTCHECK = 50

# Hard stop on pagination. Guards against a handler that ignores `start` and
# returns page 1 forever — without this the loop would never terminate.
MAX_PAGES = 200

# --- year-wide transaction sweep -------------------------------------
#
# VERIFIED 2026-08-16: the transaction endpoints accept a BLANK EntityName and
# scope on ElectionYears alone. Year 2000 returned recordsFiltered=16,894 in
# one query — versus 5,229 separate per-entity queries at ~6s each, ~97% of
# which returned nothing because most NJ filers never file.
#
# That makes the year the sweep unit, not the entity: ~4 paged requests per
# year instead of ~5,000. Two things do NOT work and were tested:
#   ElectionYears="2000,1999"  -> 0 rows (parsed as one invalid year)
#   ElectionYears=""           -> HTTP 500 (a year is required)
#
# Above this many rows in a window, split it by date rather than trusting deep
# pagination — ELEC's own export caps at 65,000 and there's no reason to think
# the handler is happier past that. 50k leaves headroom.
YEAR_WINDOW_SPLIT_THRESHOLD = 50_000

# ELEC's DateFrom/DateTo format. Only exercised on years that exceed the
# threshold above; _collect_window() detects a no-op filter and falls back to
# straight pagination rather than silently dropping rows.
DATE_FMT = "%m/%d/%Y"

# Entity kinds. Both listings hit the same handler and return identical
# columns; NONPACOnly is the only thing that differs.
ENTITY_KINDS = {
    "candidate": "entities",   # NONPACOnly=true  → entities_{year}.csv
    "pac":       "pacs",       # NONPACOnly=false → pacs_{year}.csv
}

# ---------------------------------------------------------------------
# Raw output schemas. These are OUR column names, not ELEC's — the parser
# reads these files and nothing else, so this is the contract between the two
# halves of the state. ELEC's display header is noted alongside each.
# ---------------------------------------------------------------------
ENTITY_COLS = [
    "eid",             # ENTITY_S — per entity, per election cycle
    "entity_kind",     # "candidate" | "pac" — which listing it came from
    "name",            # ENTITYNAME    e.g. "BUCCO, ANTHONY M"
    "location",        # LOCATION      e.g. "25TH LEGISLATIVE DISTRICT"
    "office_cmte",     # OFFICE        office sought, or committee type
    "party",           # PARTY         party, or PAC classification
    "election_type",   # ELECTIONTYPE  e.g. "PRIMARY" / "GENERAL"
    "election_year",   # ELECTIONYEAR
    # Opportunistic. DataTables server-side handlers usually serialize the
    # whole backing model rather than just the declared columns, and the entity
    # view is the same VWEntity the detail route reads. When these come back
    # populated the detail sweep is redundant and run() skips it — see
    # _details_needed(). Blank when ELEC only sends the six display columns.
    "first_name",      # FIRST_NAME
    "middle_initial",  # MIDDLE_INITIAL
    "last_name",       # LAST_NAME
    "suffix",          # SUFFIX
    "non_ind_name",    # NON_IND_NAME
    "entity_type",     # ENTITY_TYPE
]

# Fields the detail sweep exists to obtain. If the entity listing already has
# them, stage 2 is skipped entirely — it's one GET per entity, so this is the
# difference between minutes and hours on a full backfill.
DETAIL_FIELDS = ("first_name", "last_name", "entity_type")

# ENTITY_TYPE values that mean "this filer is a candidate". Confirmed: "C" on
# a State Senate candidate. Other values haven't been sampled, so an
# unrecognized one falls back to the office-name test rather than being
# assumed non-candidate.
ENTITY_TYPE_CANDIDATE = {"C"}

# Transaction rows are keyed on the (entity_name, election_year) pair the
# query was scoped by — NOT on an entity id. See the module docstring.
CONTRIBUTION_COLS = [
    "entity_name",
    "election_year",
    "contributor",        # CONTRIBUTOR
    "address",            # Address           single free-text line
    "employer",           # EMP_NAME
    "employer_address",   # EmployerAddress
    "occupation",         # OccupationName
    "recipient",          # CAND_NAME         the filing committee
    "contributor_type",   # ContributorType   INDIVIDUAL / BUSINESS-CORP / UNION PAC / ...
    "contribution_type",  # ContributionType  MONETARY / IN-KIND / ...
    "date",               # CONT_DATE
    "amount",             # CONT_AMT
    # CONTRIB_S is a CONTRIBUTOR surrogate key (the `cid` the dashboard links
    # use), not a per-transaction id. Kept for traceability; deliberately not
    # written to filing_id, which would imply a filing identifier.
    "contrib_s",          # CONTRIB_S
]

# VERIFIED 2026-08-16 against ENTITY_S=411086. The response is an ARRAY of
# objects with exactly these keys — no treasurer, address, city, zip, phone or
# status anywhere. ELEC does not expose committee contact details through this
# API at all; see docs/states/new_jersey.md § Data Notes.
#
# What it DOES add over the entity grid is worth the sweep:
#   - name split into components by ELEC, rather than guessed from "LAST, FIRST"
#   - ENTITY_TYPE, an explicit candidate/other flag
#   - the *_CODE fields behind each display label
ENTITY_DETAIL_COLS = [
    "eid",              # ENTITY_S
    "election_year",    # ELECTIONYEAR
    "name",             # ENTITYNAME
    "first_name",       # FIRST_NAME       authoritative — no name-flipping needed
    "middle_initial",   # MIDDLE_INITIAL
    "last_name",        # LAST_NAME
    "suffix",           # SUFFIX
    "non_ind_name",     # NON_IND_NAME     set for organizations, null for people
    "pac_name",         # PACNAME
    "entity_type",      # ENTITY_TYPE      "C" = candidate (see ENTITY_TYPE_CANDIDATE)
    "seq_num",          # SEQ_NUM          row ordinal; >1 on multi-row responses
    "office_code",      # OFFICECODE
    "office",           # OFFICE
    "party_code",       # PARTYCODE
    "party",            # PARTY
    "location_code",    # LOCATION_CODE
    "location",         # LOCATION
    "election_type_code",  # ELECTIONTYPECODE
    "election_type",    # ELECTIONTYPE
]

EXPENDITURE_COLS = [
    "entity_name",
    "election_year",
    "receiver",           # PAYEE
    "address",            # Address           (DB column STREET1)
    "recipient",          # CAND_NAME         the filing committee
    "expense_desc",       # EXPENSE_DESC
    "receiver_type",      # PAYEE_TYPE        INDIVIDUAL / NON-INDIVIDUAL
    "check_num",          # CHECK_NUM
    "date",               # CK_DATE
    "amount",             # CK_AMT
    # Unlike the contribution grid, the expense grid really does return the
    # entity id per row — so expenditures CAN be pinned to a single election
    # cycle even though the query was scoped by name+year.
    "eid",                # ENTITY_S
]


# ==================== ENDPOINT ADAPTER ================================
# Everything describing how to talk to ELEC lives here and nowhere else. If a
# route is renamed or a payload reshaped, this is the only region to edit.
#
# HOW TO RE-DERIVE: open the page with DevTools → Network → filter Fetch/XHR,
# run a search, right-click the request → Copy → Copy as cURL, and translate
# the --data-raw body into the *_COLUMNS / *_FILTERS below.
#
# VERIFIED 2026-08-16 from live captures:
#
#   POST /api/VWEntity/Entities20
#     content-type: application/x-www-form-urlencoded; charset=UTF-8
#     x-requested-with: XMLHttpRequest
#     referer: /SearchCandidateReports (or /SearchContributionByEntity — the
#              entity grid is shared across pages and the referer isn't checked
#              beyond being same-origin)
#     body: standard DataTables params + NONPACOnly, FirstName, LastName, MI,
#           Suffix, NonIndName, OfficeCodes, PartyCodes, LocationCodes,
#           ElectionTypeCodes, ElectionYears, SortColumn, SortBy
#
#   GET /api/VWEntity/GetEntityDataWithCommittee?ENTITY_S={eid}
#     plain GET, no body. ENTITY_S is the entity-id parameter name.
#
#   POST /api/VWContributionDetail/GetContBitsDataByObject
#     referer: /SearchContributionInteractive?eid=411086
#     body: DataTables params + ENTITY_S (SENT EMPTY), EntityName,
#           ElectionYears, ContributorLastName, ContributorFirstName,
#           ContributorMI, ContributorSuffix, ContributorNonIndName, EMP_NAME,
#           OccupationName, DateFrom, DateTo, AmountFrom, AmountTo
#     Note the scope is EntityName + ElectionYears, NOT the eid on the referer.
#
#   POST /api/VWExpenseDetail/GetBitsDataByObject
#     referer: /searchexpenseinteractive?eid=411086
#     body: DataTables params + ENTITY_S (SENT EMPTY), EntityName,
#           ElectionYears, PayeeName, ExpenseDesc, CheckNo, DateFrom, DateTo,
#           AmountFrom=0, AmountTo=0
#     The action is GetBitsDataByObject — NOT GetExpBitsDataByObject. The
#     controller carries the entity ("VWExpenseDetail"), the action doesn't
#     repeat it, so the two routes are not symmetrical. Don't "fix" this.
#     Note also AmountFrom/AmountTo are "0" here but "" on the contribution
#     route, which is why the amount bounds live in the per-relation branch of
#     _transaction_filters() rather than in the shared block.

ENTITY_API       = f"{BASE}/api/VWEntity/Entities20"
DETAIL_API       = f"{BASE}/api/VWEntity/GetEntityDataWithCommittee"
CONTRIBUTION_API = f"{BASE}/api/VWContributionDetail/GetContBitsDataByObject"
EXPENSE_API      = f"{BASE}/api/VWExpenseDetail/GetBitsDataByObject"

REFERERS = {
    "candidate":     f"{BASE}/SearchCandidateReports",
    "pac":           f"{BASE}/SearchPACList",
    "details":       f"{BASE}/SearchContributionByEntity",
    "contributions": f"{BASE}/SearchContributionInteractive",
    "expenditures":  f"{BASE}/SearchExpenseInteractive",
}

# DataTables columns as (data, name) pairs, in display order.
#   data — the key on each returned JSON row; what _pick() reads
#   name — the underlying DB column, used for sort/search
# They are NOT always the same: the contribution grid maps Address→STREET1 and
# EmployerAddress→EMP_STREET1. Sending name where data belongs makes the
# handler return rows whose keys don't match what we then look for, which
# fails silently as empty columns — hence the pair.
ENTITY_COLUMNS = [
    ("ENTITYNAME",   "ENTITYNAME"),
    ("LOCATION",     "LOCATION"),
    ("OFFICE",       "OFFICE"),
    ("PARTY",        "PARTY"),
    ("ELECTIONTYPE", "ELECTIONTYPE"),
    ("ELECTIONYEAR", "ELECTIONYEAR"),
]

CONTRIBUTION_COLUMNS = [
    ("CONTRIBUTOR",      "CONTRIBUTOR"),
    ("Address",          "STREET1"),
    ("EMP_NAME",         "EMP_NAME"),
    ("EmployerAddress",  "EMP_STREET1"),
    ("OccupationName",   "OccupationName"),
    ("CAND_NAME",        "CAND_NAME"),
    ("ContributorType",  "ContributorType"),
    ("ContributionType", "ContributionType"),
    ("CONT_DATE",        "CONT_DATE"),
    ("CONT_AMT",         "CONT_AMT"),
    ("CONTRIB_S",        "CONTRIB_S"),
]

EXPENSE_COLUMNS = [
    ("PAYEE",        "PAYEE"),
    ("Address",      "STREET1"),
    ("CAND_NAME",    "CAND_NAME"),
    ("EXPENSE_DESC", "EXPENSE_DESC"),
    ("PAYEE_TYPE",   "PAYEE_TYPE"),
    ("CHECK_NUM",    "CHECK_NUM"),
    ("CK_DATE",      "CK_DATE"),
    ("CK_AMT",       "CK_AMT"),
    ("ENTITY_S",     "ENTITY_S"),
]


def _datatables_body(columns: list[tuple[str, str]], start: int, length: int,
                     order_col: int = 0, order_dir: str = "asc") -> dict:
    """Build the DataTables half of a request body.

    Mirrors exactly what the site's own grids send: every column declared
    searchable and orderable with an empty per-column search. Sending fewer
    fields than the browser does risks a 500 from the model binder, so this
    reproduces the full shape rather than a minimal one.
    """
    body: dict[str, str] = {"draw": "1"}
    for i, (data, name) in enumerate(columns):
        body |= {
            f"columns[{i}][data]":            data,
            f"columns[{i}][name]":            name,
            f"columns[{i}][searchable]":      "true",
            f"columns[{i}][orderable]":       "true",
            f"columns[{i}][search][value]":   "",
            f"columns[{i}][search][regex]":   "false",
        }
    body |= {
        "order[0][column]": str(order_col),
        "order[0][dir]":    order_dir,
        "order[0][name]":   columns[order_col][0],
        "start":            str(start),
        "length":           str(length),
        "search[value]":    "",
        "search[regex]":    "false",
    }
    return body


def _entity_filters(kind: str, year: int) -> dict:
    """ELEC's own filter fields for the entity grid.

    Empty string means "no filter" throughout — the site posts every field on
    every search rather than omitting unused ones, so we mirror that.
    NONPACOnly is the candidate/PAC switch.
    """
    return {
        "NONPACOnly":        "true" if kind == "candidate" else "false",
        "FirstName":         "",
        "LastName":          "",
        "MI":                "",
        "Suffix":            "",
        "NonIndName":        "",
        "OfficeCodes":       "",
        "PartyCodes":        "",
        "LocationCodes":     "",
        "ElectionTypeCodes": "",
        "ElectionYears":     str(year),
        "SortColumn":        "ElectionYear",
        "SortBy":            "desc",
    }


def _transaction_filters(relation: str, entity_name: str, year: int | str) -> dict:
    """ELEC's own filter fields for the contribution / expense grids.

    Scoped by EntityName + ElectionYears. ENTITY_S is included but left EMPTY
    because that is exactly what the site sends — the eid on the interactive
    page's URL never makes it into the request body. Populating it here was
    tried and is not what the handler expects; if a future capture shows it
    working, switching to id-scoping would remove the same-name ambiguity
    documented in the module docstring.
    """
    common = {
        "ENTITY_S":      "",
        "EntityName":    entity_name,
        "ElectionYears": str(year),
        "DateFrom":      "",
        "DateTo":        "",
    }
    if relation == "contributions":
        return common | {
            "ContributorLastName":   "",
            "ContributorFirstName":  "",
            "ContributorMI":         "",
            "ContributorSuffix":     "",
            "ContributorNonIndName": "",
            "EMP_NAME":              "",
            "OccupationName":        "",
            "AmountFrom":            "",   # blank here, "0" on the expense route
            "AmountTo":              "",
        }
    return common | {
        "PayeeName":   "",
        "ExpenseDesc": "",
        "CheckNo":     "",
        "AmountFrom":  "0",   # the expense grid posts 0, not blank — mirrored
        "AmountTo":    "0",
    }


# Maps our raw column names onto the keys ELEC may use in its JSON rows.
# Multiple spellings are listed per field because ELEC is not fully consistent
# between handlers. `_pick()` walks these in order and takes the first
# non-empty hit, so extra candidates are harmless — add rather than replace
# when you discover a new spelling.
FIELD_ALIASES = {
    # entity grid — these are confirmed from the live capture
    "eid":               ["ENTITY_S", "ENTITYID", "EID", "ID", "eid", "entityId"],
    "name":              ["ENTITYNAME", "Name", "name"],
    "location":          ["LOCATION", "Location", "location"],
    "office_cmte":       ["OFFICE", "Office", "office", "OFFICECMTE"],
    "party":             ["PARTY", "Party", "party"],
    "election_type":     ["ELECTIONTYPE", "ElectionType", "electionType"],
    "election_year":     ["ELECTIONYEAR", "ElectionYear", "electionYear", "Year"],
    # contribution grid — confirmed from the live capture
    "contributor":       ["CONTRIBUTOR", "CONTRIBUTORNAME", "ContributorName"],
    "employer":          ["EMP_NAME", "EMPLOYER", "Employer"],
    "employer_address":  ["EmployerAddress", "EMP_STREET1", "EMPADDRESS"],
    "occupation":        ["OccupationName", "OCCUPATION", "Occupation"],
    # The grid labels this "Recipient" but the underlying column is CAND_NAME.
    "recipient":         ["CAND_NAME", "RECIPIENT", "Recipient", "ENTITYNAME"],
    "contributor_type":  ["ContributorType", "CONTRIBUTORTYPE", "CONTTYPE"],
    "contribution_type": ["ContributionType", "CONTRIBUTIONTYPE", "CONTRIBTYPE"],
    "contrib_s":         ["CONTRIB_S", "CONTRIBS", "ContribS"],
    # expense grid — confirmed from the live capture
    "receiver":          ["PAYEE", "RECEIVER", "PAYEENAME", "ReceiverName"],
    "expense_desc":      ["EXPENSE_DESC", "ExpenseDesc", "EXPENSEDESC"],
    "receiver_type":     ["PAYEE_TYPE", "ReceiverType", "RECEIVERTYPE"],
    "check_num":         ["CHECK_NUM", "CHECK_NO", "CheckNo"],
    # entity detail (GetEntityDataWithCommittee) — confirmed from a live response
    "first_name":          ["FIRST_NAME"],
    "middle_initial":      ["MIDDLE_INITIAL"],
    "last_name":           ["LAST_NAME"],
    "suffix":              ["SUFFIX"],
    "non_ind_name":        ["NON_IND_NAME"],
    "pac_name":            ["PACNAME"],
    "entity_type":         ["ENTITY_TYPE"],
    "seq_num":             ["SEQ_NUM"],
    "office_code":         ["OFFICECODE"],
    "party_code":          ["PARTYCODE"],
    "location_code":       ["LOCATION_CODE"],
    "election_type_code":  ["ELECTIONTYPECODE"],
    # shared
    "address":           ["Address", "ADDRESS", "STREET1", "address"],
    "date":              ["CONT_DATE", "CK_DATE", "EXP_DATE", "DATE", "Date"],
    "amount":            ["CONT_AMT", "CK_AMT", "EXP_AMT", "AMOUNT", "Amount"],
}

# ====================== END ENDPOINT ADAPTER ==========================


# ============================== probe =================================
# Support for confirming the transaction routes without needing a browser:
#
#     python3 src/pipeline/scrapers/new_jersey.py --probe
#     python3 src/pipeline/scrapers/new_jersey.py --probe "AARON, CHARLES S JR" --probe-year 2023
#
# It walks a list of plausible route names, reports which answer with JSON
# rows, and prints the row keys so *_COLUMNS and FIELD_ALIASES can be checked
# against reality. Contributions is included even though it's confirmed — it
# doubles as a regression check that the known-good route still answers.
#
# This is a diagnostic, not a pipeline stage — orc.py never calls it.

PROBE_ENTITY = "BUCCO, ANTHONY M"
PROBE_YEAR   = 2020

# Both routes confirmed 2026-08-16. The live one is listed first in each list
# so a match short-circuits; the rest are kept as fallbacks in case ELEC
# renames an action, which is the failure this whole helper exists to diagnose.
PROBE_ROUTES = {
    "contributions": [
        "/api/VWContributionDetail/GetContBitsDataByObject",
        "/api/VWContributionDetail/GetBitsDataByObject",
        "/api/VWContribution/GetContBitsDataByObject",
    ],
    "expenditures": [
        "/api/VWExpenseDetail/GetBitsDataByObject",
        "/api/VWExpenseDetail/GetExpBitsDataByObject",
        "/api/VWExpenditureDetail/GetBitsDataByObject",
        "/api/VWExpense/GetBitsDataByObject",
    ],
}


def probe(entity_name: str = PROBE_ENTITY, year: int = PROBE_YEAR) -> None:
    """Try each candidate transaction route against one entity+year and report.

    The status code carries most of the diagnostic value, so it is interpreted
    rather than just printed:

      404          — no such route; the NAME is wrong, keep guessing
      400/500      — route EXISTS but rejected the body; the name is right and
                     only *_COLUMNS / _transaction_filters need correcting.
                     That is a much smaller fix, so it's reported loudly.
      200 + rows   — done; row keys are printed for *_COLUMNS
      200 + 0 rows — route and body are fine but this entity has no data for
                     that year; retry with --probe-year or another name before
                     concluding anything
    """
    session = _make_session()
    existing: list[tuple[str, int]] = []   # routes that answered but rejected us

    for relation, paths in PROBE_ROUTES.items():
        columns = (CONTRIBUTION_COLUMNS if relation == "contributions"
                   else EXPENSE_COLUMNS)
        body = (_datatables_body(columns, start=0, length=25)
                | _transaction_filters(relation, entity_name, year))
        const = "CONTRIBUTION_API" if relation == "contributions" else "EXPENSE_API"

        print(f"\n{'=' * 72}\n  {relation}  (EntityName={entity_name!r}, "
              f"ElectionYears={year})\n{'=' * 72}")
        for path in paths:
            try:
                resp = session.post(f"{BASE}{path}", data=body,
                                    headers={"Referer": REFERERS[relation]},
                                    timeout=45)
            except requests.RequestException as e:
                print(f"  {path:54s} — {type(e).__name__}")
                continue

            code = resp.status_code
            if code == 404:
                print(f"  {path:54s} — 404 no such route")
                continue
            if code in (400, 500):
                print(f"  {path:54s} — HTTP {code}  ← ROUTE EXISTS, body rejected")
                existing.append((path, code))
                continue
            if code != 200:
                print(f"  {path:54s} — HTTP {code}")
                continue

            try:
                payload = resp.json()
            except ValueError:
                print(f"  {path:54s} — 200 but not JSON "
                      f"({resp.headers.get('content-type', '?')})")
                continue

            rows  = _rows(payload)
            total = _total(payload)
            print(f"  {path:54s} — 200 JSON, {len(rows)} rows, "
                  f"recordsFiltered={total}")
            if rows and isinstance(rows[0], dict):
                print(f"\n       ✓ MATCH — set {const} to this URL")
                print(f"       row keys: {sorted(rows[0])}")
                print(f"       sample:   {rows[0]}")
                break
            print("         (200 but empty — right route, or just no data for "
                  "this entity/year)")
        else:
            print(f"\n  ✗ no confirmed match for {const}")
            _probe_no_match(const, existing)
        existing.clear()


def _probe_no_match(const, existing):
    """Report a failed route probe (helper so probe() stays readable)."""
    if existing:
        print("    But these answered instead of 404-ing, so the route "
              "name is probably right\n    and only the body shape is "
              "wrong — fix *_COLUMNS / _transaction_filters:")
        for path, code in existing:
            print(f"      {code}  {path}")
    else:
        print("    Every candidate 404'd — the route name is wrong. "
              "Capture the XHR in\n    DevTools instead "
              "(docs/states/new_jersey.md § Endpoints).")


def active_entities(year: int, limit: int = 25) -> list[str]:
    """Entity names that demonstrably filed something in `year`, busiest first.

    Read from contributions_{year}.csv, so these are entities with proven
    activity rather than names picked off an alphabetical list. That matters:
    only ~5% of NJ entities in a given year have any transactions at all, so
    an alphabetical sample of 50 has a real chance of hitting nothing even in
    a year that holds data — and would then wrongly declare the year empty.

    Returns [] if contributions for that year haven't been scraped yet.
    """
    path = RAW_DIR / f"contributions_{year}.csv"
    if not path.exists():
        return []
    counts: dict[str, int] = {}
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            name = (row.get("entity_name") or "").strip()
            if name:
                counts[name] = counts.get(name, 0) + 1
    return [n for n, _ in sorted(counts.items(), key=lambda kv: -kv[1])][:limit]


def diagnose_year(year: int, relation: str = "expenditures", top: int = 8) -> None:
    """Ask whether a year is truly empty, using entities known to be active.

    Built for the 2003 question: 46,014 contributions and zero expenditures,
    sitting between 2001 (4,412) and 2005 (15,769). Either ELEC never
    digitised that year's expense schedules, or our query is wrong for it.

    Tests the busiest contribution-receiving committees of that year — bodies
    like the state party, which unquestionably spent money — against every
    scoping we know. If none of them return an expense, the absence is real.
    """
    session = _make_session()
    _cols, api, columns = _relation_bits(relation)

    names = active_entities(year, limit=top)
    if not names:
        print(f"No contributions_{year}.csv on disk — scrape contributions first.")
        return

    eids = {}
    for label in ENTITY_KINDS.values():
        path = RAW_DIR / f"{label}_{year}.csv"
        if path.exists():
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    nm = (row.get("name") or "").strip().upper()
                    if nm and nm not in eids:
                        eids[nm] = (row.get("eid") or "").strip()

    print(f"\n{'=' * 78}\n  {relation} {year} — testing the {len(names)} busiest "
          f"committees of that year\n{'=' * 78}")
    # NB the third column is the committee's ALL-TIME total, not its total for
    # `year`. Labelled explicitly because reading it as year-scoped led to a
    # wrong conclusion once already: a committee with 10,789 lifetime expenses
    # and none at all in the year under test looks like proof the year has data.
    print(f"  {'committee':42s} {'name+yr':>8s} {'ENTITY_S':>9s} {'ALL-TIME':>9s}")

    any_hit = False
    for name in names:
        eid = eids.get(name.upper(), "")
        results = []
        for scope in ("name", "id", "noyear"):
            f = _transaction_filters(relation, name, year)
            if scope == "id":
                f |= {"ENTITY_S": eid, "EntityName": ""}
            elif scope == "noyear":
                f |= {"ElectionYears": ""}
            if scope == "id" and not eid:
                results.append("—")
                continue
            try:
                payload = _post(session, api, REFERERS[relation],
                                _datatables_body(columns, 0, 25) | f, timeout=120)
                n = _total(payload)
                results.append(str(n) if n is not None else "?")
                if n:
                    any_hit = True
            except Exception as e:
                results.append(type(e).__name__[:8])
            time.sleep(REQUEST_DELAY_S)
        print(f"  {name[:42]:42s} {results[0]:>8s} {results[1]:>9s} {results[2]:>8s}")

    print()
    if any_hit:
        print(f"  Something is non-zero. CAREFUL: a non-zero ALL-TIME column proves\n"
              f"  only that the committee filed expenses at some point, NOT that it\n"
              f"  filed any in {year}. Confirm with --diagnose-entity NAME, which\n"
              f"  breaks a committee down year by year against its lifetime total.")
    else:
        print(f"  No expenses for any of the {len(names)} busiest committees of {year},\n"
              f"  on any scoping. The absence is real: ELEC has no itemised expense\n"
              f"  data for {year}. Record it in docs/states/new_jersey.md and move on.")


def _sample_entity(year: int) -> tuple[str, str]:
    """Return a real (name, eid) that actually filed in `year`, from disk.

    The first version of this probe hard-coded one entity and tested it
    against every year — which meant "0 rows" could just mean "that person
    didn't run that year", not "the query is broken". Reading a real entity
    for the year under test removes that ambiguity.
    """
    for label in ENTITY_KINDS.values():
        path = RAW_DIR / f"{label}_{year}.csv"
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name, eid = (row.get("name") or "").strip(), (row.get("eid") or "").strip()
                if name and eid:
                    return name, eid
    return PROBE_ENTITY, ""


def probe_filters(relation: str = "expenditures", year: int = PROBE_YEAR,
                  entity_name: str | None = None) -> None:
    """Bisect which filter field is suppressing results for a relation.

    Varies one field at a time and prints recordsFiltered, so the offending
    parameter identifies itself instead of being guessed at.

    Uses a real entity from that year (see _sample_entity) — testing a
    hard-coded name against an arbitrary year conflates "query is broken" with
    "this person didn't run then", which cost a round of wrong conclusions.

    Includes ENTITY_S variants: the interactive pages send it blank and scope
    on EntityName, but the Search*ByEntity flow may well scope by id instead,
    and that path has never been tested.
    """
    session = _make_session()
    _cols, api, columns = _relation_bits(relation)
    sampled_name, sampled_eid = _sample_entity(year)
    entity_name = entity_name or sampled_name
    base = _transaction_filters(relation, entity_name, year)

    variants = [
        ("as shipped (from the capture)", {}),
        ("Amount bounds blank",            {"AmountFrom": "", "AmountTo": ""}),
        ("Amount keys removed",            {"AmountFrom": None, "AmountTo": None}),
        ("blank EntityName (year-wide)",   {"EntityName": ""}),
        ("Date bounds removed",            {"DateFrom": None, "DateTo": None}),
        ("only EntityName + ElectionYears", "MINIMAL"),
        # --- id-scoped: never tested before ---
        (f"ENTITY_S={sampled_eid} + name",  {"ENTITY_S": sampled_eid}),
        (f"ENTITY_S={sampled_eid}, no name", {"ENTITY_S": sampled_eid,
                                              "EntityName": ""}),
        (f"ENTITY_S={sampled_eid} only",     "ID_ONLY"),
        ("no year, blank name",            {"ElectionYears": "", "EntityName": ""}),
    ]

    print(f"\n{'=' * 72}\n  Filter probe — {relation}, {year}\n"
          f"  entity from disk: {entity_name!r}  (ENTITY_S={sampled_eid or 'unknown'})\n"
          f"{'=' * 72}")
    for label, override in variants:
        if override == "MINIMAL":
            filters = {"EntityName": entity_name, "ElectionYears": str(year)}
        elif override == "ID_ONLY":
            filters = {"ENTITY_S": sampled_eid}
        else:
            filters = dict(base)
            for k, v in override.items():
                if v is None:
                    filters.pop(k, None)
                else:
                    filters[k] = v

        body = _datatables_body(columns, start=0, length=25) | filters
        try:
            payload = _post(session, api, REFERERS[relation], body, timeout=120)
        except Exception as e:
            print(f"  {label:34s} — {type(e).__name__}: {str(e)[:40]}")
            continue
        rows, total = _rows(payload), _total(payload)
        flag = "   <-- WORKS" if rows else ""
        print(f"  {label:34s} — {len(rows):>4} rows, "
              f"recordsFiltered={total if total is not None else '?'}{flag}")

    print("\n  Whichever line returns rows identifies the offending field.\n"
          "  Fix it in _transaction_filters() in the ENDPOINT ADAPTER block.")


def diagnose_entity(entity_name: str, relation: str = "expenditures",
                    years: range | None = None) -> None:
    """Profile ONE committee's transactions per year under each scoping.

    The decisive test for how badly the year-wide expense sweep undercounts.
    For a single committee we can get ground truth — an unfiltered query
    returns everything it ever filed — then ask how much each year-scoping
    recovers of that total.

    If the per-year date queries sum to roughly the unfiltered total, date
    scoping per entity is complete and should replace the year-wide sweep.
    If ElectionYears recovers far less, every expense year already on disk is
    short, not just the four that came back empty.
    """
    session = _make_session()
    _cols, api, columns = _relation_bits(relation)
    years = years or range(START_YEAR, datetime.today().year + 1)

    def ask(**overrides) -> int | None:
        f = _transaction_filters(relation, entity_name, 0) | overrides
        body = _datatables_body(columns, 0, 1) | f
        try:
            return _total(_post(session, api, REFERERS[relation], body, timeout=180))
        except Exception:
            return None

    print(f"\n{'=' * 74}\n  {relation} — {entity_name}\n{'=' * 74}")
    baseline = ask(ElectionYears="", DateFrom="", DateTo="")
    print(f"  no filter at all (ground truth) : {baseline}")

    by_year, by_elec = {}, {}
    for y in years:
        d = ask(ElectionYears="", DateFrom=f"01/01/{y}", DateTo=f"12/31/{y}")
        e = ask(ElectionYears=str(y), DateFrom="", DateTo="")
        if d or e:
            by_year[y], by_elec[y] = d or 0, e or 0
        time.sleep(REQUEST_DELAY_S)

    print(f"\n  {'year':6s} {'by date range':>14s} {'by ElectionYears':>18s}")
    for y in sorted(by_year):
        print(f"  {y:<6d} {by_year[y]:>14,} {by_elec[y]:>18,}")
    sd, se = sum(by_year.values()), sum(by_elec.values())
    print(f"  {'TOTAL':6s} {sd:>14,} {se:>18,}     (ground truth {baseline:,})"
          if baseline is not None else "")
    if baseline:
        print(f"\n  date scoping recovers    {sd / baseline * 100:5.1f}% of this "
              f"committee's records")
        print(f"  ElectionYears recovers   {se / baseline * 100:5.1f}%")


def probe_datescope(year: int = PROBE_YEAR) -> None:
    """Test date-range scoping as a replacement for ElectionYears.

    --diagnose-year 2003 showed EntityName with NO year returns thousands of
    expenses for committees that return zero with ElectionYears=2003. So the
    expense view's ElectionYears filter drops records — but a blank year with
    a blank name is a 500, so something must bound the query.

    DateFrom/DateTo is the only other bound the endpoint accepts, and it's
    arguably the more correct one: an expenditure has a date, whereas
    "election year" is an attribute of the committee that filed it.

    Runs both relations, because if ElectionYears is unreliable on the expense
    view it may be quietly dropping contributions too — and contributions are
    already scraped and would need redoing.
    """
    session = _make_session()
    jan, dec = f"01/01/{year}", f"12/31/{year}"

    for relation in ("contributions", "expenditures"):
        _cols, api, columns = _relation_bits(relation)
        base = _transaction_filters(relation, "", year)
        variants = [
            ("ElectionYears only (current)", {"ElectionYears": str(year),
                                              "DateFrom": "", "DateTo": ""}),
            ("DateFrom/DateTo only",         {"ElectionYears": "",
                                              "DateFrom": jan, "DateTo": dec}),
            ("both year and dates",          {"ElectionYears": str(year),
                                              "DateFrom": jan, "DateTo": dec}),
        ]
        print(f"\n{'=' * 74}\n  {relation} {year} — year-wide (blank EntityName)"
              f"\n{'=' * 74}")
        for label, ov in variants:
            body = _datatables_body(columns, 0, 25) | base | {"EntityName": ""} | ov
            try:
                payload = _post(session, api, REFERERS[relation], body, timeout=180)
            except Exception as e:
                print(f"  {label:30s} — {type(e).__name__}: {str(e)[:36]}")
                continue
            total = _total(payload)
            print(f"  {label:30s} — recordsFiltered={total if total is not None else '?'}")

    print("\n  If 'DateFrom/DateTo only' beats 'ElectionYears only' for expenditures,\n"
          "  switch the expense sweep to date scoping. If it also beats it for\n"
          "  CONTRIBUTIONS, the contributions already on disk are short too.")


def probe_scope(year: int = PROBE_YEAR) -> None:
    """Test whether the transaction sweep can be scoped more cheaply.

    The per-entity sweep is the whole cost of this scraper: ~6s per query,
    ~97% of which return zero rows because most NJ filers never file. Both
    escapes below would collapse that, and each is a single request to check.

      A. blank EntityName    — one paged sweep per year instead of ~5,000
                               per-entity queries (~130x fewer requests)
      B. multi-value years   — ElectionYears is plural; if it takes a list,
                               the sweep unit becomes the 82,132 unique names
                               rather than 162,100 name-year pairs (~2x)

    Prints recordsFiltered for each so you can see what a hit would return
    and whether the 65,000-row export cap is in play.
    """
    session = _make_session()
    baseline = None

    cases = [
        ("per-entity (current)", {"EntityName": PROBE_ENTITY,
                                  "ElectionYears": str(year)}),
        ("A. blank EntityName", {"EntityName": "", "ElectionYears": str(year)}),
        ("B. two years at once", {"EntityName": PROBE_ENTITY,
                                  "ElectionYears": f"{year},{year - 1}"}),
        ("B2. year range hyphen", {"EntityName": PROBE_ENTITY,
                                   "ElectionYears": f"{year - 1}-{year}"}),
        ("A+B. blank name, no year", {"EntityName": "", "ElectionYears": ""}),
    ]

    print(f"\n{'=' * 72}\n  Scope probe — contributions, {year}\n{'=' * 72}")
    for label, overrides in cases:
        body = (_datatables_body(CONTRIBUTION_COLUMNS, start=0, length=PAGE_SIZE)
                | _transaction_filters("contributions", "", year) | overrides)
        try:
            payload = _post(session, CONTRIBUTION_API,
                            REFERERS["contributions"], body, timeout=180)
        except Exception as e:
            print(f"  {label:26s} — FAILED: {type(e).__name__}: {str(e)[:70]}")
            continue

        rows, total = _rows(payload), _total(payload)
        print(f"  {label:26s} — {len(rows):>6,} rows returned, "
              f"recordsFiltered={total if total is not None else '?':>9}")
        if label.startswith("per-entity"):
            baseline = total
        elif total and baseline is not None and total > baseline:
            note = "  ← WIDER than per-entity: this scope works"
            if total >= 65000:
                note += " (>=65k cap — window on DateFrom/DateTo)"
            print(f"  {'':26s}   {note}")

    print("\n  A working 'blank EntityName' turns ~5,000 queries/year into ~40.")
    print("  A working multi-year list halves the sweep on top of that.")


# ========================= manifest helpers ===========================

def load_manifest() -> set[tuple[str, str]]:
    """Return the set of (relation_type, year) pairs already downloaded."""
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        return {(row["relation_type"], row["year"]) for row in csv.DictReader(f)}


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
    """Replace any existing (relation_type, year) row, then append this one."""
    strip_manifest(lambda r: not (r["relation_type"] == record["relation_type"]
                                  and r["year"] == record["year"]))
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            w.writeheader()
        w.writerow(record)


# =========================== http helpers =============================

def _make_session() -> requests.Session:
    """Session preconfigured for ELEC's /api/ handlers."""
    s = requests.Session()
    s.headers.update({
        "User-Agent":       USER_AGENT,
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":           BASE,
    })
    # Pick the least-invasive TLS config that actually works here, probed once
    # per process and cached (see config.resolve_tls). On a normal network
    # this settles on stock verification — certifi, with Python 3.13's strict
    # checks left ON — so a clean machine inherits none of the workarounds a
    # TLS-inspecting corporate proxy requires. Only a machine that genuinely
    # fails escalates, and only as far as it must. Nothing here ever disables
    # verification.
    verify, relax_strict = resolve_tls(BASE)
    s.verify = verify
    if relax_strict:
        s.mount("https://", tls_adapter(verify))
    # Prime the Azure ARRAffinity cookies so the whole sweep pins to one
    # backend instance. Non-fatal if it fails — the API works cookieless.
    try:
        s.get(BASE, timeout=30)
    except requests.RequestException:
        pass
    return s


class TLSTrustError(RuntimeError):
    """Certificate verification failed — a local trust problem, not ELEC's."""


class EmptySweepError(RuntimeError):
    """A sweep succeeded on every request but collected nothing.

    Signals a malformed query rather than absent data, so the year must not be
    recorded as complete — otherwise the manifest remembers a hole as done.
    """


_TLS_HELP = (
    "TLS certificate verification failed against njelecefilesearch.com.\n"
    "  The site itself is fine — this is your machine not trusting the "
    "certificate it was served,\n"
    "  which is what a corporate TLS-inspecting proxy (Zscaler, Netskope, "
    "etc.) looks like.\n"
    "  Fixes, in order of preference:\n"
    "    1. Windows: config.ca_bundle() exports your OS trust store "
    "automatically. If you still\n"
    "       see this, delete the cache and retry:\n"
    "         python3 -c \"import config; print(config.ca_bundle(refresh=True))\"\n"
    "    2. Point requests at your corporate root CA explicitly:\n"
    "         setx REQUESTS_CA_BUNDLE \"C:\\\\path\\\\to\\\\corp-root.pem\"\n"
    "       Export it from Edge: lock icon → Connection is secure → "
    "certificate → Details →\n"
    "       Copy to File → Base-64 encoded X.509 (.CER).\n"
    "    3. If you're on VPN, try off it — split-tunnel setups often only "
    "inspect on-network."
)


def _request(session: requests.Session, method: str, url: str, referer: str,
             body: dict | None = None, params: dict | None = None,
             timeout: int = 120):
    """Issue a request with retries. Returns parsed JSON or raises."""
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.request(method, url, data=body, params=params,
                                   headers={"Referer": referer}, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.SSLError as e:
            # Not transient and not per-request: the trust store won't fix
            # itself on attempt 2. Abort the whole run immediately rather than
            # burning 3 retries × 2 relations × 27 years on a config problem,
            # and say plainly what to do about it.
            raise TLSTrustError(f"{_TLS_HELP}\n\n  Original error: {e}") from e
        except (requests.RequestException, ValueError) as e:
            last_exc = e
            # Exponential, not linear: a 429/503 or a VPN reconnect needs tens
            # of seconds, and hammering it just extends the outage.
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_S * (2 ** attempt))
    raise last_exc  # type: ignore[misc]


def _post(session: requests.Session, url: str, referer: str,
          body: dict, timeout: int = 120):
    """POST a form-encoded DataTables body."""
    return _request(session, "POST", url, referer, body=body, timeout=timeout)


def _rows(payload) -> list:
    """Pull the row list out of a response, tolerating the shapes ELEC emits.

    Normally {draw, recordsTotal, recordsFiltered, data:[...]}, but .NET
    sometimes wraps in "d" and some handlers return a bare array. Anything
    unrecognized yields [] rather than raising, so one odd response can't abort
    a 40,000-entity sweep.
    """
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "Data", "aaData", "rows", "results", "d"):
            inner = payload.get(key)
            if isinstance(inner, list):
                return inner
            if isinstance(inner, dict):
                return _rows(inner)
    return []


def _total(payload) -> int | None:
    """Read recordsFiltered/recordsTotal if present, else None."""
    if not isinstance(payload, dict):
        return None
    for key in ("recordsFiltered", "recordsTotal", "iTotalDisplayRecords"):
        val = payload.get(key)
        if isinstance(val, int):
            return val
        if isinstance(val, str) and val.isdigit():
            return int(val)
    return None


def _fetch_all(session, url: str, referer: str, columns: list[str],
               filters: dict, order_col: int = 0) -> list[dict]:
    """Page through a server-side DataTables endpoint and return every row.

    Stops when the endpoint reports we've seen recordsFiltered rows, when a
    short page comes back, or at MAX_PAGES — whichever comes first.
    """
    out: list[dict] = []
    for page in range(MAX_PAGES):
        body = _datatables_body(columns, start=page * PAGE_SIZE,
                                length=PAGE_SIZE, order_col=order_col) | filters
        payload = _post(session, url, referer, body)
        rows = _rows(payload)
        out.extend(rows)

        if len(rows) < PAGE_SIZE:
            break                       # short page — that was the last one
        total = _total(payload)
        if total is not None and len(out) >= total:
            break
        time.sleep(REQUEST_DELAY_S)
    return out


# ========================= extraction helpers =========================

_TAG_RE = re.compile(r"<[^>]+>")


def _detag(val: str) -> str:
    """Strip HTML from a cell — ELEC wraps names in <a> tags in some grids."""
    return _TAG_RE.sub("", val).replace("&amp;", "&").strip()


def _pick(row: dict, field: str) -> str:
    """Pull `field` out of an ELEC JSON row, trying every known key spelling."""
    if not isinstance(row, dict):
        return ""
    for key in FIELD_ALIASES.get(field, [field]):
        if key in row and row[key] is not None:
            val = str(row[key]).strip()
            if val:
                return val
    # Case/punctuation-insensitive fallback — cheap insurance against a
    # spelling we haven't catalogued yet.
    wanted = field.replace("_", "").lower()
    for key, val in row.items():
        norm = str(key).replace("_", "").replace(" ", "").replace(".", "").lower()
        if norm == wanted:
            return "" if val is None else str(val).strip()
    return ""


def _eid_from(row: dict) -> str:
    """Extract the entity id, including from an embedded ?eid=NNN link."""
    direct = _pick(row, "eid")
    if direct.isdigit():
        return direct
    blob = " ".join(str(v) for v in row.values() if v is not None)
    m = re.search(r"[?&]eid=(\d+)", blob, re.IGNORECASE)
    return m.group(1) if m else direct


# ========================= parallel sweep =============================

def _sweep_failed(log, label: str, ok: int, err: int) -> bool:
    """True if a sweep lost too many targets to be treated as complete.

    Returning True makes the caller return None, which stops run() writing a
    manifest row — so the year is retried on the next run instead of being
    remembered as done with a hole in it.
    """
    attempted = ok + err
    if not err or not attempted:
        return False
    rate = err / attempted
    if rate <= MAX_ERROR_RATE:
        log.warning(f"  {label}: {err:,} of {attempted:,} targets failed "
                    f"({rate:.1%}) — within tolerance, year kept")
        return False
    log.warning(f"  {label}: {err:,} of {attempted:,} targets failed ({rate:.1%}) "
                f"— above the {MAX_ERROR_RATE:.0%} threshold, so this year is NOT "
                f"being marked complete. Re-run to fill the gap. A network drop "
                f"mid-sweep is the usual cause.")
    return True


def _parallel_sweep(log, session, targets: list, fetch, make_rows, writer,
                    desc: str, workers: int, entity_label: str) -> tuple[int, int, int]:
    """Run a per-target fetch across a thread pool and write the results.

    Shared by both per-target sweeps (entity details and transactions) so the
    fiddly parts — session-per-thread, write locking, TLS abort, progress —
    exist once rather than twice.

      fetch(session, target)      -> list of raw JSON rows
      make_rows(target, raw_rows) -> list of dicts ready for `writer`

    Each worker gets its own session: `requests`' Session isn't documented as
    thread-safe and its connection pool is per-session, so sharing one would
    both risk races and serialise everything on a pool of 10.

    Only the write is locked; the fetch stays fully parallel. Per-target
    failures are logged and skipped, but a TLSTrustError cancels the pool and
    is re-raised — a trust problem affects every target, so reporting it
    162,000 times helps nobody.

    Returns (total_rows, ok_targets, failed_targets).
    """
    workers = max(1, min(workers, len(targets))) or 1
    sessions = [session] + [_make_session() for _ in range(workers - 1)]
    local = threading.local()
    handed_out = itertools.count()
    write_lock = threading.Lock()
    total = ok = err = 0
    aborted: list[BaseException] = []

    def _session_for() -> requests.Session:
        """One session per thread, pinned on first use."""
        s = getattr(local, "session", None)
        if s is None:
            s = sessions[next(handed_out) % len(sessions)]   # count() is atomic
            local.session = s
        return s

    def _task(target):
        rows = fetch(_session_for(), target)
        time.sleep(REQUEST_DELAY_S)      # politeness, per worker
        return rows

    try:
        with logging_redirect_tqdm(loggers=[log._log]):
            bar = tqdm(total=len(targets), desc=desc, unit="entity",
                       dynamic_ncols=True)
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_task, t): t for t in targets}
                for fut in as_completed(futures):
                    target = futures[fut]
                    try:
                        raw = fut.result()
                    except TLSTrustError as e:
                        aborted.append(e)
                        for pending in futures:
                            pending.cancel()
                        break
                    except Exception as e:
                        log.page_scrape_error(entity=entity_label,
                                              page_id=str(target), error=str(e))
                        err += 1
                        bar.update(1)
                        continue

                    with write_lock:
                        for rec in make_rows(target, raw):
                            writer.writerow(rec)
                            total += 1
                        ok += 1
                    bar.set_postfix_str(f"{total:,} rows", refresh=False)
                    bar.update(1)

                    # Bail out if a long run of successful requests has
                    # yielded nothing at all. That means the query is wrong,
                    # not that the data is missing — grinding through the
                    # remaining thousands can only waste time. (Learned the
                    # hard way: a bad expense filter cost 24 minutes per year
                    # to collect zero rows.)
                    if total == 0 and ok >= EMPTY_SWEEP_ABORT_AFTER:
                        log.warning(
                            f"  {desc.strip()}: {ok:,} successful requests "
                            f"returned 0 rows — aborting this sweep. The query "
                            f"is almost certainly wrong; run --probe-filters.")
                        for pending in futures:
                            pending.cancel()
                        aborted.append(EmptySweepError(desc.strip()))
                        break
            bar.close()
    finally:
        # Close only the sessions created here; sessions[0] belongs to caller.
        for s in sessions[1:]:
            try:
                s.close()
            except Exception:
                pass

    if aborted:
        raise aborted[0]
    return total, ok, err


# ========================== entity sweep ==============================

def download_entities(log, session, kind: str, year: int) -> tuple[str, int] | None:
    """Fetch one election year's entity listing. Returns (filename, rows) or None."""
    filename = f"{ENTITY_KINDS[kind]}_{year}.csv"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        raw = _fetch_all(session, ENTITY_API, REFERERS[kind],
                         ENTITY_COLUMNS, _entity_filters(kind, year))
    except TLSTrustError:
        raise                # environment problem — abort, don't try 2001 next
    except Exception as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    # Dedupe on eid. ELEC's entity view can repeat a row when an entity is
    # joined to more than one filing, and a duplicate eid would mean fetching
    # (and writing) that entity's whole transaction history twice.
    seen: set[str] = set()
    records = []
    for row in raw:
        eid = _eid_from(row)
        if not eid or eid in seen:
            continue
        seen.add(eid)
        rec = {
            "eid":           eid,
            "entity_kind":   kind,
            "name":          _detag(_pick(row, "name")),
            "location":      _detag(_pick(row, "location")),
            "office_cmte":   _detag(_pick(row, "office_cmte")),
            "party":         _detag(_pick(row, "party")),
            "election_type": _detag(_pick(row, "election_type")),
            "election_year": _detag(_pick(row, "election_year")) or str(year),
        }
        # Opportunistic extras — blank unless the handler returned the full model
        for col in ("first_name", "middle_initial", "last_name", "suffix",
                    "non_ind_name", "entity_type"):
            rec[col] = _detag(_pick(row, col))
        records.append(rec)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ENTITY_COLS, extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(records)

    log.file_download_ok(filename=filename, bytes=out_path.stat().st_size,
                         rows=len(records),
                         duration_s=round(time.perf_counter() - t0, 2))
    if records and all(records[0].get(f) for f in DETAIL_FIELDS):
        log.info(f"    {filename}: listing already carries the detail fields "
                 f"— detail sweep will be skipped")
    return filename, len(records)


def _details_needed(year: int) -> bool:
    """True if the detail sweep would add anything the listing doesn't have.

    The detail route costs one GET per entity, so it's worth not running when
    the listing already returned FIRST_NAME / LAST_NAME / ENTITY_TYPE. Checks
    the first populated row of each listing rather than every row — the
    handler either serializes the full model or it doesn't; it won't do so for
    only some rows.
    """
    for label in ENTITY_KINDS.values():
        path = RAW_DIR / f"{label}_{year}.csv"
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                return not all((row.get(f) or "").strip() for f in DETAIL_FIELDS)
    return True   # no listing on disk — let the caller warn about it


def download_entity_details(log, session, year: int,
                            workers: int = DEFAULT_WORKERS) -> tuple[str, int] | None:
    """
    GET the detail record for every entity in `year`, `workers` at a time.

    Supplies the name components (FIRST_NAME / MIDDLE_INITIAL / LAST_NAME /
    SUFFIX) and ENTITY_TYPE that the listing may not carry. One GET per
    entity, so at ~162k entities across a full backfill this is as expensive
    as a whole transaction relation — hence the same concurrency, and hence
    run() skipping it entirely when _details_needed() says so.

    Per-entity failures are logged and skipped. Returns (filename, rows).
    """
    filename = f"entity_details_{year}.csv"
    out_path = RAW_DIR / filename

    eids = load_eids(year)
    if not eids:
        log.warning(f"  no entity file for {year} — run the entity sweep first; skipping")
        return None

    t0 = time.perf_counter()

    def _fetch(sess, eid):
        payload = _request(sess, "GET", DETAIL_API, REFERERS["details"],
                           params={"ENTITY_S": eid})
        # Normally a JSON array; tolerate a bare object for a single entity.
        rows = _rows(payload)
        if not rows and isinstance(payload, dict):
            rows = [payload]
        return rows

    def _make_rows(eid, raw_rows):
        out = []
        for row in raw_rows:
            rec = {"eid": eid, "election_year": year}
            for col in ENTITY_DETAIL_COLS[2:]:
                rec[col] = _detag(_pick(row, col))
            out.append(rec)
        return out

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ENTITY_DETAIL_COLS,
                                extrasaction="ignore", restval="")
        writer.writeheader()
        total, ok, err = _parallel_sweep(
            log, session, eids, _fetch, _make_rows, writer,
            desc=f"  details {year}", workers=workers, entity_label="entity_detail")

    log.page_scrape_complete(filename=str(out_path), rows=total,
                             duration_s=round(time.perf_counter() - t0, 1),
                             ok=ok, err=err)
    if _sweep_failed(log, f"details {year}", ok, err):
        return None
    return filename, total


def load_eids(year: int) -> list[str]:
    """Return every eid for a year, from whichever entity files exist on disk.

    Used by the detail sweep, which IS id-scoped.
    """
    out: list[str] = []
    seen: set[str] = set()
    for label in ENTITY_KINDS.values():
        path = RAW_DIR / f"{label}_{year}.csv"
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                eid = (row.get("eid") or "").strip()
                if eid and eid not in seen:
                    seen.add(eid)
                    out.append(eid)
    return out


def load_transaction_targets(year: int) -> list[str]:
    """Return the unique entity NAMES to sweep for a year.

    The transaction endpoints scope by EntityName + ElectionYears, so the unit
    of work is a name, not an id. Deduping here is what stops a filer who ran
    in both the primary and the general — two eids, one name — from having
    their entire transaction history fetched and written twice.
    """
    out: list[str] = []
    seen: set[str] = set()
    for label in ENTITY_KINDS.values():
        path = RAW_DIR / f"{label}_{year}.csv"
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name = (row.get("name") or "").strip()
                key  = name.upper()
                if name and key not in seen:
                    seen.add(key)
                    out.append(name)
    return out


# ======================== transaction sweep ===========================

def _relation_bits(relation: str):
    """Return (cols, api, columns) for a relation."""
    if relation == "contributions":
        return CONTRIBUTION_COLS, CONTRIBUTION_API, CONTRIBUTION_COLUMNS
    return EXPENDITURE_COLS, EXPENSE_API, EXPENSE_COLUMNS


def _collect_window(log, session, relation: str, year: int,
                    date_from: str = "", date_to: str = "",
                    depth: int = 0) -> list[dict]:
    """Page one (year, date-window) of a relation, splitting if it's too big.

    Returns raw JSON rows. Recurses by halving the date window whenever the
    handler reports more than YEAR_WINDOW_SPLIT_THRESHOLD matches, so no single
    query has to page past ELEC's 65,000-row comfort zone.

    If splitting doesn't actually reduce the count — i.e. DateFrom/DateTo
    aren't being honoured — it stops recursing and pages the window straight
    through, with a warning. Better to over-fetch and log it than to silently
    return a truncated year.
    """
    _cols, api, columns = _relation_bits(relation)
    filters = _transaction_filters(relation, "", year) | {
        "EntityName": "", "DateFrom": date_from, "DateTo": date_to,
    }

    probe_body = _datatables_body(columns, start=0, length=PAGE_SIZE) | filters
    payload = _post(session, api, REFERERS[relation], probe_body, timeout=300)
    total = _total(payload)
    first = _rows(payload)

    label = f"{year}" + (f" {date_from}..{date_to}" if date_from else "")

    if (total is not None and total > YEAR_WINDOW_SPLIT_THRESHOLD
            and depth < 6 and len(first) >= PAGE_SIZE):
        lo = datetime.strptime(date_from, DATE_FMT).date() if date_from else date(year, 1, 1)
        hi = datetime.strptime(date_to, DATE_FMT).date() if date_to else date(year, 12, 31)
        if lo < hi:
            mid = lo + (hi - lo) / 2
            left = _collect_window(log, session, relation, year,
                                   lo.strftime(DATE_FMT), mid.strftime(DATE_FMT),
                                   depth + 1)
            right = _collect_window(log, session, relation, year,
                                    (mid + timedelta(days=1)).strftime(DATE_FMT),
                                    hi.strftime(DATE_FMT), depth + 1)
            # If the halves together didn't shrink anything, the date filter is
            # being ignored — fall through and page the window instead.
            if len(left) + len(right) >= 1 and (len(left) or len(right)):
                return left + right
            log.warning(f"  {relation} {label}: date split had no effect — "
                        f"DateFrom/DateTo may be ignored; paging straight through")

    # Page the window.
    out = list(first)
    if len(first) >= PAGE_SIZE:
        for page in range(1, MAX_PAGES):
            body = _datatables_body(columns, start=page * PAGE_SIZE,
                                    length=PAGE_SIZE) | filters
            rows = _rows(_post(session, api, REFERERS[relation], body, timeout=300))
            out.extend(rows)
            if len(rows) < PAGE_SIZE:
                break
            if total is not None and len(out) >= total:
                break
            time.sleep(REQUEST_DELAY_S)
    return out


def download_transactions(log, session, relation: str, year: int,
                          workers: int = DEFAULT_WORKERS) -> tuple[str, int] | None:
    """
    Download one relation for one election year.

    Primary path is a YEAR-WIDE sweep: the endpoints accept a blank EntityName
    and scope on ElectionYears alone, so a year costs a handful of paged
    requests instead of one ~6s query per entity (of which ~97% return nothing
    — see the YEAR_WINDOW_SPLIT_THRESHOLD comment).

    Falls back to the old per-entity sweep if the year-wide query comes back
    empty for a year that demonstrably has entities. That's the signal ELEC has
    changed the handler to require a name again; `workers` only applies there,
    since the year-wide path is a short serial page-walk.
    """
    cols, api, columns = _relation_bits(relation)
    filename = f"{relation}_{year}.csv"
    out_path = RAW_DIR / filename
    t0 = time.perf_counter()

    def _write(rows_iter) -> int:
        n = 0
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols,
                                    extrasaction="ignore", restval="")
            writer.writeheader()
            for row in rows_iter:
                rec = {
                    # No EntityName was sent, so the row's own recipient
                    # (CAND_NAME) is the only committee identity available.
                    # Writing it into entity_name keeps the raw schema — and
                    # therefore the parser's (name, year) join — unchanged.
                    "entity_name":   _detag(_pick(row, "recipient")),
                    "election_year": year,
                }
                for col in cols[2:]:
                    rec[col] = _detag(_pick(row, col))
                writer.writerow(rec)
                n += 1
        return n

    # ── Primary: year-wide ────────────────────────────────────────────
    try:
        raw = _collect_window(log, session, relation, year)
    except TLSTrustError:
        raise
    except Exception as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    if raw:
        total = _write(raw)
        log.page_scrape_complete(filename=str(out_path), rows=total,
                                 duration_s=round(time.perf_counter() - t0, 1),
                                 ok=1, err=0)
        return filename, total

    # ── Fallback: per-entity ──────────────────────────────────────────
    targets = load_transaction_targets(year)
    if not targets:
        # Nothing on disk to sweep and nothing came back — a genuinely empty year.
        _write([])
        log.page_scrape_complete(filename=str(out_path), rows=0,
                                 duration_s=round(time.perf_counter() - t0, 1),
                                 ok=0, err=0)
        return filename, 0

    # A year-wide zero is ambiguous: either the query is wrong, or the year
    # genuinely has no data (true of NJ expenditures before ~2004). Spot-check
    # a spread of entities before spending ~25 minutes on a full sweep.
    # Prefer entities with PROVEN activity — the busiest recipients from that
    # year's contributions — over an alphabetical slice. Only ~5% of NJ
    # entities file anything in a given year, so a blind sample of 50 has a
    # real chance of hitting nothing even when the year holds data, and would
    # then wrongly brand it empty. Falls back to the spread if contributions
    # for the year aren't on disk.
    sample = active_entities(year, limit=EMPTY_YEAR_SPOTCHECK)
    basis = "busiest committees by contribution volume"
    if not sample:
        sample = targets[:: max(1, len(targets) // EMPTY_YEAR_SPOTCHECK)][:EMPTY_YEAR_SPOTCHECK]
        basis = "alphabetical spread (no contributions file to rank by)"
    log.info(f"  {relation} {year}: year-wide query returned nothing — "
             f"spot-checking {len(sample)} entities [{basis}]")

    hits = 0
    for entity_name in sample:
        try:
            if _fetch_all(session, api, REFERERS[relation], columns,
                          _transaction_filters(relation, entity_name, year)):
                hits += 1
                break
        except TLSTrustError:
            raise
        except Exception:
            continue          # a bad entity proves nothing either way
        time.sleep(REQUEST_DELAY_S)

    if not hits:
        # Consistent with the year-wide result: this year really is empty.
        _write([])
        log.info(f"  {relation} {year}: no data in ELEC for this year "
                 f"(confirmed against {len(sample)} entities) — recording as empty")
        log.page_scrape_complete(filename=str(out_path), rows=0,
                                 duration_s=round(time.perf_counter() - t0, 1),
                                 ok=len(sample), err=0)
        return filename, 0

    log.warning(f"  {relation} {year}: year-wide returned nothing but individual "
                f"entities have data — year-wide query may be broken; falling "
                f"back to the per-entity sweep (slow)")

    def _fetch(sess, entity_name):
        return _fetch_all(sess, api, REFERERS[relation], columns,
                          _transaction_filters(relation, entity_name, year))

    def _make_rows(entity_name, raw_rows):
        out = []
        for row in raw_rows:
            rec = {"entity_name": entity_name, "election_year": year}
            for col in cols[2:]:
                rec[col] = _detag(_pick(row, col))
            out.append(rec)
        return out

    try:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols,
                                    extrasaction="ignore", restval="")
            writer.writeheader()
            total, ok, err = _parallel_sweep(
                log, session, targets, _fetch, _make_rows, writer,
                desc=f"  {relation} {year}", workers=workers, entity_label=relation)
    except EmptySweepError:
        # Query is wrong, not the data. Returning None keeps the year OUT of
        # the manifest so it's retried once the query is fixed — recording it
        # as a complete 0-row year would bury the bug permanently.
        return None

    log.page_scrape_complete(filename=str(out_path), rows=total,
                             duration_s=round(time.perf_counter() - t0, 1),
                             ok=ok, err=err)
    if _sweep_failed(log, f"{relation} {year}", ok, err):
        return None
    return filename, total


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
    workers: int = DEFAULT_WORKERS,
):
    """Orchestrate the New Jersey ELEC download.

    Vertical scope (mutually exclusive):
        force                   — wipe the manifest for everything in scope, refetch
        start_year / end_year   — restrict to an election-year range

    Horizontal scope (additive):
        no flags                — everything
        entities                — entity listings only (both kinds)
        candidates              — candidate listing only (NONPACOnly=true)
        committees              — PAC / party listing only (NONPACOnly=false)
        transactions            — contributions + expenditures
        contributions           — contributions only
        expenditures            — expenditures only

    The transaction sweep reads eids off the entity CSVs already on disk, so a
    transactions-only run with no entity files is a no-op that warns rather
    than failing. A default run does entities first in the same invocation, so
    this only bites when the flags are used deliberately.
    """
    log = get_logger("new_jersey", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees, workers=workers)

    # ── Resolve scope ─────────────────────────────────────────────────
    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    do_entities     = no_horizontal or entities or candidates or committees
    do_transactions = no_horizontal or transactions or contributions or expenditures

    # NJ has no separate candidate registry — a candidate and their committee
    # are one record. --candidates therefore selects the candidate-side
    # listing and --committees the PAC/party listing, rather than two tables.
    if candidates and not committees:
        active_kinds = ["candidate"]
    elif committees and not candidates:
        active_kinds = ["pac"]
    else:
        active_kinds = list(ENTITY_KINDS)

    if contributions and not expenditures:
        active_relations = ["contributions"]
    elif expenditures and not contributions:
        active_relations = ["expenditures"]
    else:
        active_relations = ["contributions", "expenditures"]

    current_year = datetime.today().year
    range_start  = start_year if start_year is not None else START_YEAR
    years = [y for y in range(range_start, current_year + 1)
             if end_year is None or y <= end_year]

    year_range_explicit = start_year is not None or end_year is not None

    files_ok = files_err = 0

    try:
        session = _make_session()

        # ── Manifest bookkeeping ──────────────────────────────────────
        if force:
            strip_manifest(lambda r: False)
            done: set[tuple[str, str]] = set()
        elif year_range_explicit:
            def _outside_range(r: dict) -> bool:
                try:
                    yr = int(r["year"])
                except (ValueError, KeyError):
                    return True          # non-year rows are always kept
                if start_year is not None and yr < start_year:
                    return True
                if end_year is not None and yr > end_year:
                    return True
                return False             # in range — wipe so it refetches
            strip_manifest(_outside_range)
            done = load_manifest()
        else:
            done = load_manifest()

        def _should_skip(relation_type: str, year: int) -> bool:
            """Manifest hit — except the current year, which is always refreshed,
            and an explicit year range, which means the operator wants a refetch."""
            if year == current_year or year_range_explicit:
                return False
            return (relation_type, str(year)) in done

        # ── Stage 1: entity listings ──────────────────────────────────
        if do_entities:
            for kind in active_kinds:
                label = ENTITY_KINDS[kind]
                for year in years:
                    if _should_skip(label, year):
                        log.file_download_skip(filename=f"{label}_{year}.csv")
                        continue

                    result = download_entities(log, session, kind, year)
                    if result is None:
                        files_err += 1
                        continue

                    filename, row_count = result
                    upsert_manifest({
                        "relation_type": label,
                        "year":          str(year),
                        "filename":      filename,
                        "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                        "row_count":     row_count,
                    })
                    files_ok += 1
                    time.sleep(REQUEST_DELAY_S)

        # ── Stage 2: entity detail records ────────────────────────────
        # Runs with the entity stage because it's entity metadata, not
        # transactions, and it depends on the listing written just above.
        if do_entities:
            for year in years:
                if _should_skip("details", year):
                    log.file_download_skip(filename=f"entity_details_{year}.csv")
                    continue
                if not _details_needed(year):
                    # Listing already carried FIRST_NAME/LAST_NAME/ENTITY_TYPE,
                    # so this whole per-entity sweep would be a no-op.
                    log.file_download_skip(filename=f"entity_details_{year}.csv")
                    continue

                result = download_entity_details(log, session, year,
                                                 workers=workers)
                if result is None:
                    files_err += 1
                    continue

                filename, row_count = result
                upsert_manifest({
                    "relation_type": "details",
                    "year":          str(year),
                    "filename":      filename,
                    "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                    "row_count":     row_count,
                })
                files_ok += 1

        # ── Stage 3: itemized transactions ────────────────────────────
        if do_transactions:
            for relation in active_relations:
                for year in years:
                    if _should_skip(relation, year):
                        log.file_download_skip(filename=f"{relation}_{year}.csv")
                        continue

                    result = download_transactions(log, session, relation, year,
                                                   workers=workers)
                    if result is None:
                        files_err += 1
                        continue

                    filename, row_count = result
                    upsert_manifest({
                        "relation_type": relation,
                        "year":          str(year),
                        "filename":      filename,
                        "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                        "row_count":     row_count,
                    })
                    files_ok += 1

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


# ================================ CLI =================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download New Jersey ELEC campaign finance data."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest election year to download (inclusive)")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="latest election year to download (inclusive, ≤ current year)")

    ap.add_argument("--transactions",  action="store_true",
                    help="transactions only (contributions + expenditures)")
    ap.add_argument("--entities",      action="store_true",
                    help="entity listings only (candidate + PAC)")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="candidate listing only (NONPACOnly=true)")
    ap.add_argument("--committees",    action="store_true",
                    help="PAC / party listing only (NONPACOnly=false)")

    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS, metavar="N",
                    help=f"concurrent per-entity transaction fetches, each on "
                         f"its own session (default {DEFAULT_WORKERS}; 1 = serial)")

    ap.add_argument("--probe", metavar="ENTITY_NAME", nargs="?", const=PROBE_ENTITY,
                    help="diagnostic: find the transaction API routes using one "
                         f"entity name and exit (default {PROBE_ENTITY!r})")
    ap.add_argument("--probe-year", type=int, default=PROBE_YEAR, metavar="YYYY",
                    help=f"election year to probe with (default {PROBE_YEAR})")
    ap.add_argument("--diagnose-entity", metavar="NAME",
                    help="diagnostic: profile one committee per year under each "
                         "scoping, measured against an unfiltered total")
    ap.add_argument("--probe-datescope", type=int, metavar="YYYY",
                    help="diagnostic: compare ElectionYears vs DateFrom/DateTo "
                         "scoping for both relations, then exit")
    ap.add_argument("--diagnose-year", type=int, metavar="YYYY",
                    help="diagnostic: test whether a year is truly empty by "
                         "querying its busiest committees across every scoping")
    ap.add_argument("--probe-filters", metavar="RELATION", nargs="?",
                    const="expenditures",
                    choices=["contributions", "expenditures", None],
                    help="diagnostic: vary one filter field at a time to find "
                         "which one is suppressing results (default expenditures)")
    ap.add_argument("--check-tls", action="store_true",
                    help="diagnostic: try each candidate CA bundle against ELEC "
                         "and report which one actually connects, then exit")
    ap.add_argument("--probe-scope", action="store_true",
                    help="diagnostic: test whether the transaction sweep can be "
                         "scoped by year alone (blank EntityName) or by multiple "
                         "years at once, then exit")

    args, _ = ap.parse_known_args()   # orc.py forwards flags this scraper may not define

    # Diagnostic short-circuits — no manifest, no logging, no downloads.
    if args.check_tls:
        import config as _config
        sys.exit(0 if _config.diagnose_tls(BASE) is not None else 1)

    if args.diagnose_entity:
        try:
            diagnose_entity(args.diagnose_entity)
            sys.exit(0)
        except KeyboardInterrupt:
            sys.exit(130)

    if args.probe_datescope:
        try:
            probe_datescope(args.probe_datescope)
            sys.exit(0)
        except KeyboardInterrupt:
            sys.exit(130)

    if args.diagnose_year:
        try:
            diagnose_year(args.diagnose_year)
            sys.exit(0)
        except KeyboardInterrupt:
            sys.exit(130)

    if args.probe_filters:
        try:
            probe_filters(args.probe_filters, args.probe_year)
            sys.exit(0)
        except KeyboardInterrupt:
            sys.exit(130)

    if args.probe_scope:
        try:
            probe_scope(args.probe_year)
            sys.exit(0)
        except KeyboardInterrupt:
            sys.exit(130)

    if args.probe:
        try:
            probe(args.probe, args.probe_year)
            sys.exit(0)
        except KeyboardInterrupt:
            sys.exit(130)

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
    except TLSTrustError as e:
        # The instructions ARE this exception — swallowing it defeats the point.
        print(f"\n{e}", file=sys.stderr)
        sys.exit(1)
    except Exception:
        # Print before exiting. A bare `except Exception: sys.exit(1)` gives a
        # direct run no output at all, and orc.py's stderr capture nothing to
        # capture — the failure looks like a silent no-op either way.
        traceback.print_exc()
        sys.exit(1)
