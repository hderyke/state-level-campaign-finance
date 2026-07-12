"""
scrapers/mississippi.py — Download Mississippi campaign finance data from the
Secretary of State's Campaign Finance portal (cfportal.sos.ms.gov).

Source: an ASP.NET AJAX (.asmx ScriptService) portal. Three POST endpoints,
each returning the ENTIRE dataset in a single call when sent with blank
filter values — there is no bulk file, no year split, and no pagination:

    .../Services/MS/CampaignFinanceServices.asmx/CandidateNameSearch
        {"SearchBy":"Contains","EntityName":"","SearchType":"All"}
        -> all ~3,300 registered entities: candidates, candidate committees,
           PACs, and ballot-initiative committees. Each row is just
           {EntityId, EntityName, OrganizationType} — no office/party/address.
           Richer per-entity metadata (office, party, filing history) lives
           behind a separate server-rendered HTML detail page
           (ViewXSLTFileByName.aspx?providerName=CF_CandidateDetails&EntityId=...)
           which is NOT scraped here — a ~3,300-page sweep wasn't worth the
           cost for a first pass. See docs/states/mississippi.md.

    .../Services/MS/CampaignFinanceServices.asmx/ContributionSearch
        {"EntityName":"","Description":"","BeginDate":"","EndDate":"",
         "AmountPaid":"","InKindAmount":"","CandidateName":"","CommitteeName":"",
         "ContributionType":"Any"}
        -> all ~96,000 itemized contributions, 2001-present.

    .../Services/MS/CampaignFinanceServices.asmx/ExpenditureSearch
        {"EntityName":"","Description":"","BeginDate":"","EndDate":"",
         "AmountPaid":"","CandidateName":"","CommitteeName":""}
        -> all ~25,000 itemized expenditures.

    .../Services/MS/CampaignFinanceServices.asmx/DistrictSearch
        {"DistrictType":"<type>","DistrictName":"<name>","ElectionYear":"","DistrictNumber":""}
        -> entities running for a specific office. Each row is {EntityId,
           EntityName, OrganizationType, ElectionYear} -- notably, the office
           itself is NOT a field in the response; it's implied entirely by
           which (DistrictType, DistrictName) you queried with. There's no
           single call that returns office-tagged data for everything (a
           DistrictType:"All" query returns every entity that has ever run
           for ANY office, but without saying which one). So this scraper
           issues one call per concrete office name and stamps the response
           rows with that office before writing them out.

           Only Statewide (8 offices: Governor, LieutenantGovernor,
           SecretaryOfState, AttorneyGeneral, Auditor, Treasurer,
           CommissionerOfAgriculture, CommissionerOfInsurance) and Judicial
           (4: SupremeCourt, CourtOfAppeals, CircuitCourt, ChanceryCourt) are
           queried -- 12 calls total. StateDistrict and Legislative offices
           (House/Senate, Transportation/Public Service Commissioner
           districts) are deliberately NOT queried: DistrictSearch has no
           district-number-level granularity in its response, so "House" or
           "Senate" alone pools all ~122/52 individual seats together,
           making it useless for disambiguating same-name candidates in
           different districts -- the exact problem this endpoint exists to
           solve. See docs/states/mississippi.md for the parser-side use of
           this data (candidate/committee name-linking tiebreaker).

Each raw ASMX response is double-JSON-encoded: {"d": "<json string>"}. That
transport envelope is unwrapped here before writing to raw/, so the parser
just reads a plain {"Table": [...]} object.

Note: the portal's own UI claims only reports filed electronically since
9/30/2016 are searchable, with everything earlier being PDF-only. In
practice the blank ContributionSearch call returns rows back to 2001 — the
UI disclaimer appears to undersell what the API itself actually returns.
No special handling applied here; taken at face value.

No authentication required. --start-year/--end-year are accepted (for CLI
consistency with other states) but not implemented — each endpoint always
returns full history in a single call, so there's no year-scoped request to
make. --candidates/--committees are likewise accepted but treated the same
as --entities: CandidateNameSearch returns all entity types in one call:
the candidate vs. committee vs. PAC split happens at parse time via
OrganizationType.

WAF blocks non-browser traffic — NOT just datacenter IPs. A plain `requests`
call gets a 403 "Access Denied" from both a hosted/sandbox IP AND a real
residential IP on a contributor's own machine (confirmed empirically — see
git history for the first, `requests`-based version of this scraper, which
failed identically in both environments). The block is on the request's
fingerprint (TLS handshake / header shape — whatever `requests` produces
doesn't look like a real browser to this WAF), not on IP reputation. A
Playwright-driven real Chromium instance passes it fine, since the fetch()
call below executes inside an actual browser page rather than through
`requests`/urllib3. Same category of workaround as Alaska's WAF, and for
the same reason — this one just happens to need a real browser rather than
just a residential IP.
"""

import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Mississippi" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Mississippi" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation", "filename", "downloaded_at", "row_count"]

# ========================= state-specific constants ===================
PORTAL_URL = "https://cfportal.sos.ms.gov/online/portal/cf/page/cf-search/Portal.aspx"
BASE_URL   = "https://cfportal.sos.ms.gov/online/Services/MS/CampaignFinanceServices.asmx"

ENDPOINTS = {
    "entities": {
        "url":      f"{BASE_URL}/CandidateNameSearch",
        "filename": "entities.json",
        "payload":  {"SearchBy": "Contains", "EntityName": "", "SearchType": "All"},
    },
    "contributions": {
        "url":      f"{BASE_URL}/ContributionSearch",
        "filename": "contributions.json",
        "payload":  {
            "EntityName": "", "Description": "", "BeginDate": "", "EndDate": "",
            "AmountPaid": "", "InKindAmount": "", "CandidateName": "",
            "CommitteeName": "", "ContributionType": "Any",
        },
    },
    "expenditures": {
        "url":      f"{BASE_URL}/ExpenditureSearch",
        "filename": "expenditures.json",
        "payload":  {
            "EntityName": "", "Description": "", "BeginDate": "", "EndDate": "",
            "AmountPaid": "", "CandidateName": "", "CommitteeName": "",
        },
    },
}

DISTRICTS_URL = f"{BASE_URL}/DistrictSearch"

# (DistrictType, DistrictName) pairs to sweep for districts.json. Statewide +
# Judicial only -- see module docstring for why StateDistrict/Legislative are
# excluded (no per-seat granularity, so they'd pool every House/Senate/PSC
# district together and be useless as a name-linking tiebreaker).
DISTRICT_QUERIES = [
    ("Statewide", "Governor"),
    ("Statewide", "LieutenantGovernor"),
    ("Statewide", "SecretaryOfState"),
    ("Statewide", "AttorneyGeneral"),
    ("Statewide", "Auditor"),
    ("Statewide", "Treasurer"),
    ("Statewide", "CommissionerOfAgriculture"),
    ("Statewide", "CommissionerOfInsurance"),
    ("Judicial",  "SupremeCourt"),
    ("Judicial",  "CourtOfAppeals"),
    ("Judicial",  "CircuitCourt"),
    ("Judicial",  "ChanceryCourt"),
]

# JS run inside the page — a real in-browser fetch(), not a Python HTTP client.
_FETCH_JS = """
async ([url, payload]) => {
    const resp = await fetch(url, {
        method: "POST",
        headers: {"Content-Type": "application/json; charset=UTF-8"},
        body: JSON.stringify(payload),
    });
    const text = await resp.text();
    return {status: resp.status, text};
}
"""


# ========================== manifest helpers ==========================

def upsert_manifest(record: dict):
    """Replace an existing manifest row for record['relation'] (or append)."""
    rows = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["relation"] != record["relation"]]
    rows.append(record)
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(rows)


# ========================== download helpers ==========================

def download_relation(log, page, relation: str) -> tuple[str, int] | None:
    """Run the blank-filter query for `relation` as an in-page fetch(), unwrap
    the ASMX {"d": ...} double-JSON envelope, and write the plain
    {"Table": [...]} object to raw/. Returns (filename, row_count) or None
    on failure."""
    spec     = ENDPOINTS[relation]
    filename = spec["filename"]
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    try:
        result = page.evaluate(_FETCH_JS, [spec["url"], spec["payload"]])
        if result["status"] != 200:
            raise RuntimeError(f"HTTP {result['status']}")
        outer = json.loads(result["text"])
        inner = json.loads(outer["d"])
    except Exception as e:
        log.file_download_error(filename=filename, error=str(e))
        return None

    rows = inner.get("Table", [])
    out_path.write_text(json.dumps(inner), encoding="utf-8")

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=len(rows),
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, len(rows)


def download_districts(log, page) -> tuple[str, int] | None:
    """Sweep DISTRICT_QUERIES (12 calls), stamping each returned entity row
    with the (DistrictType, DistrictName) it was queried under -- that office
    context isn't present in the response itself (see module docstring).
    Writes the combined, office-tagged rows to raw/districts.json as a plain
    {"Table": [...]} object. Returns (filename, row_count) or None if every
    sub-call failed.
    """
    filename = "districts.json"
    out_path = RAW_DIR / filename

    log.file_download_start(filename=filename)
    t0 = time.perf_counter()

    all_rows = []
    errors = 0
    for district_type, district_name in DISTRICT_QUERIES:
        payload = {
            "DistrictType": district_type, "DistrictName": district_name,
            "ElectionYear": "", "DistrictNumber": "",
        }
        try:
            result = page.evaluate(_FETCH_JS, [DISTRICTS_URL, payload])
            if result["status"] != 200:
                raise RuntimeError(f"HTTP {result['status']}")
            outer = json.loads(result["text"])
            inner = json.loads(outer["d"])
        except Exception as e:
            log.warning(f"districts: {district_type}/{district_name} failed: {e}")
            errors += 1
            continue
        for row in inner.get("Table", []):
            row["DistrictType"] = district_type
            row["DistrictName"] = district_name
            all_rows.append(row)
        time.sleep(0.15)

    if not all_rows and errors:
        log.file_download_error(filename=filename, error=f"all {errors} sub-calls failed")
        return None

    out_path.write_text(json.dumps({"Table": all_rows}), encoding="utf-8")

    log.file_download_ok(
        filename=filename,
        bytes=out_path.stat().st_size,
        rows=len(all_rows),
        duration_s=round(time.perf_counter() - t0, 2),
    )
    return filename, len(all_rows)


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
    """Download Mississippi's entity registry, contributions, and expenditures.

    Each of the three sources is a single POST returning full history — there
    is no incremental skip logic beyond bookkeeping: every relation in scope
    is re-fetched on every run. The source exposes no last-modified signal to
    check against, and the largest file (~50 MB) is cheap enough to always
    refresh. `force` is accepted for CLI consistency but behaves the same as
    a normal run.

    start_year/end_year are accepted but ignored (no year-scoped request
    exists on this source). candidates/committees are accepted but treated
    the same as entities (see module docstring).
    """
    log = get_logger("mississippi", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error("[!] Playwright not installed. Run: pip install playwright && playwright install chromium")
        log._emit("scrape_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, error="playwright not installed")
        return

    no_horizontal = not (entities or transactions or contributions or
                         expenditures or candidates or committees)

    do_entities      = no_horizontal or entities or candidates or committees
    do_contributions = no_horizontal or transactions or contributions
    do_expenditures  = no_horizontal or transactions or expenditures

    targets = []
    if do_entities:      targets.append("entities")
    if do_contributions: targets.append("contributions")
    if do_expenditures:  targets.append("expenditures")

    files_ok = files_err = 0

    try:
        with sync_playwright() as p:
            # headless=False — same as Alaska's WAF workaround in this repo;
            # this site's bot-detection has only been confirmed to pass with
            # a real, visible browser instance.
            browser = p.chromium.launch(headless=False)
            page    = browser.new_page()
            page.goto(PORTAL_URL, timeout=30_000)
            page.wait_for_load_state("networkidle")

            for relation in targets:
                result = download_relation(log, page, relation)
                if result is None:
                    files_err += 1
                    continue
                filename, row_count = result
                upsert_manifest({
                    "relation":      relation,
                    "filename":      filename,
                    "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                    "row_count":     row_count,
                })
                files_ok += 1
                time.sleep(0.3)

            # districts.json rides along with entities -- it's office metadata
            # for the same entity registry, used by the parser as a name-link
            # disambiguation tiebreaker (see module docstring).
            if do_entities:
                result = download_districts(log, page)
                if result is None:
                    files_err += 1
                else:
                    filename, row_count = result
                    upsert_manifest({
                        "relation":      "districts",
                        "filename":      filename,
                        "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                        "row_count":     row_count,
                    })
                    files_ok += 1

            browser.close()

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
        description="Download Mississippi campaign finance data from the SOS portal."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="accepted for CLI consistency — every run already "
                           "re-fetches fresh, so this is a no-op beyond bookkeeping")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="not supported by this source — accepted, ignored")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="not supported by this source — accepted, ignored")
    ap.add_argument("--transactions", action="store_true",
                    help="contributions + expenditures only")
    ap.add_argument("--entities",     action="store_true",
                    help="entity registry only (candidates, committees, PACs)")
    ap.add_argument("--contributions", action="store_true", help="contributions only")
    ap.add_argument("--expenditures",  action="store_true", help="expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="same as --entities — no separate candidate-only source")
    ap.add_argument("--committees",    action="store_true",
                    help="same as --entities — no separate committee-only source")

    args, _ = ap.parse_known_args()

    cy = datetime.today().year
    if args.end_year and args.end_year > cy:
        ap.error(f"--end-year cannot exceed current year ({cy})")

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
