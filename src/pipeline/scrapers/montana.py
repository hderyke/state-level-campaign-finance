"""
scrapers/montana.py — Download Montana campaign finance data from CERS.

Montana's Campaign Electronic Reporting System (CERS, https://cers-ext.mt.gov/
CampaignTracker/public/search) has no bulk export -- the public UI only lets a
user search one candidate or committee for one election year at a time, then
export a single result. The reference R function this scraper replaces
literally drove a Selenium browser through that UI: click the Contributions
tab, spin the election-year picker up/down, click Search, tick each result
row's checkbox one at a time, click Download, repeat across pages.

This scraper instead talks directly to the JSON/text endpoints the CERS
front-end itself calls via AJAX (jQuery DataTables server-side processing).
These endpoints, payloads, and the pipe-delimited schedule export format were
identified from Montana Free Press's open-source "cers-interface" project
(https://github.com/eidietrich/cers-interface), which has scraped this same
site every election cycle through 2026 using this API -- strong evidence it is
still current. IMPORTANT CAVEAT: this sandbox's network egress does not reach
cers-ext.mt.gov, so these endpoints could not be smoke-tested live from here.
Run a small slice locally (e.g. `--start-year 2024 --candidates`) and check
data/Montana/raw/ before trusting a full backfill.

Flow, per (election_year, entity_type) where entity_type is candidate/committee:
  1. POST search params (blank except electionYear) to establish server-side
     search state for the session, then GET the DataTables results endpoint
     to list every candidate/committee active that year (up to 1,000 -- CERS
     years have never come close to that count per the source project).
  2. For each entity: POST its ID to the report-list endpoint, then GET the
     DataTables list of filed reports (reportId, formTypeCode, date range).
  3. For each report, fetch full transaction detail depending on form type:
       - C5 (candidate periodic) / C6 (committee periodic) / C4 (committee
         independent-expenditure-style periodic): POST reportId+scheduleCode
         to prepareDownloadFileFromSearch, then GET downloadFile with the
         params it returns -- response is a pipe-delimited text export with
         a header row (schedule A/C6A/C4A = contributions, B/C6B/C4B =
         expenditures). Parsed here into list-of-dict rows and saved as JSON,
         preserving the server's own column headers verbatim.
       - C7 (last-minute contribution notice) / C7E (last-minute expenditure
         notice): no bulk export exists for these -- POST retrieveReport to
         set report context, then POST financeRepDetailList once per
         sub-table (individual/committee/loan donors for C7; expendOther for
         C7E) to get the line items directly as JSON. Saved in the server's
         native field names (entityName, datePaid as epoch ms, totalAmt,
         etc.) -- the parser normalizes both shapes into the pipeline schema.

Each candidate/committee's full bundle (metadata + every filed report's
itemized data) is written to one raw JSON file: candidate_{id}.json /
committee_{id}.json. The per-year entity search lists are also saved
(candidates_{year}.json / committees_{year}.json) since they carry registry
fields (office, party, status) the report endpoints don't repeat.

No authentication required. CERS's own election-year picker floors at 2000
(confirmed from the search page's ace_spinner config), so that's the scrape
floor. A session is created fresh for almost every POST+GET pair, mirroring
the reference implementation -- the app appears to tie search/report state to
the session cookie, and reusing one session across unrelated lookups risks
cross-contaminating server-side state.

Downloads are tracked in manifest.csv keyed by (entity_type, entity_id) --
re-running skips already-fetched entities except ones whose own electionYear
is the current year (they may still be actively filing reports).
"""

import csv
import io
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Montana" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Montana" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["entity_type", "entity_id", "election_year", "filename",
                 "downloaded_at", "num_reports"]

# ========================= state-specific constants ===================

BASE = "https://cers-ext.mt.gov/CampaignTracker/public"

CANDIDATE_SEARCH_URL = f"{BASE}/searchResults/searchCandidates"
CANDIDATE_LIST_URL   = f"{BASE}/searchResults/listCandidateResults"
COMMITTEE_SEARCH_URL = f"{BASE}/searchResults/searchFinancials"
COMMITTEE_LIST_URL   = f"{BASE}/searchResults/listFinancialCommitteeResults"

CAND_REPORTS_POST_URL = f"{BASE}/publicReportList/retrieveCampaignReports"
CMTE_REPORTS_POST_URL = f"{BASE}/publicReportList/retrieveCommitteeReports"
REPORTS_LIST_URL      = f"{BASE}/publicReportList/listFinanceReports"

RETRIEVE_REPORT_URL  = f"{BASE}/viewFinanceReport/retrieveReport"
DETAIL_LIST_URL      = f"{BASE}/viewFinanceReport/financeRepDetailList"
PREPARE_DOWNLOAD_URL = f"{BASE}/viewFinanceReport/prepareDownloadFileFromSearch"
DOWNLOAD_FILE_URL    = f"{BASE}/viewFinanceReport/downloadFile"

# CERS's own election-year spinner floors here (see search page JS: min:2000)
START_YEAR = 2025

CANDIDATE_SEARCH_DEFAULT = {
    "lastName": "", "firstName": "", "middleInitial": "",
    "electionYear": "", "candidateTypeCode": "", "officeCode": "",
    "countyCode": "", "partyCode": "",
}

COMMITTEE_SEARCH_DEFAULT = {
    "independentExpendSearch": "false", "electioneeringCommSearch": "false",
    "financialSearchType": "EXPEND", "expendSearchTypeCode": "COMMITTEE",
    "expendCanLastName": "", "expendCanFirstName": "", "expendCommitteeName": "",
    "payeeLastName": "", "payeeFirstName": "", "expendPartyCode": "",
    "expendCandidateTypeCode": "", "expendOfficeCode": "", "expendAmountRangeCode": "",
    "electionYear": "", "expendSearchFromDate": "", "expendSearchToDate": "",
}

# Periodic reports with a bulk pipe-delimited export. Value = (contrib code, expend code).
SCHEDULE_CODES = {
    "C5": ("A", "B"),      # candidate periodic report
    "C6": ("C6A", "C6B"),  # committee periodic report
    "C4": ("C4A", "C4B"),  # committee periodic report (independent-expenditure style)
}

# Last-minute notices -- no bulk export; fetched as JSON line items per sub-table.
C7_LIST_NAMES  = ["individual", "committee", "loan", "candidate", "fundraisers", "refunds", "payment"]
C7E_LIST_NAMES = ["expendOther", "candidate", "pettyCash", "debtLoan"]

MAX_ROWS = 1000  # iDisplayLength -- comfortably above any single year's candidate/committee count


# ========================= manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    if not MANIFEST.exists():
        return set()
    with open(MANIFEST, newline="") as f:
        return {(row["entity_type"], row["entity_id"]) for row in csv.DictReader(f)}


def strip_manifest(keep_fn):
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict):
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


# ========================== request helpers ===========================

def _session() -> requests.Session:
    """Fresh session per logical POST+GET pair -- mirrors the reference
    implementation, which never reuses a session across unrelated lookups.
    The app appears to key search/report state off the session cookie."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    })
    return s


def _cache_bust() -> int:
    return int(time.time() * 1000)


def fetch_candidate_list(election_year: str) -> list[dict]:
    """All candidates active in a given election year, across every office."""
    search = CANDIDATE_SEARCH_DEFAULT.copy()
    search["electionYear"] = election_year
    s = _session()
    s.post(CANDIDATE_SEARCH_URL, data=search, timeout=60)
    url = (
        f"{CANDIDATE_LIST_URL}?sEcho=1&iColumns=9&sColumns=&iDisplayStart=0"
        f"&iDisplayLength={MAX_ROWS}&mDataProp_0=checked&mDataProp_1=candidateName"
        f"&mDataProp_2=electionYear&mDataProp_3=candidateStatusDescr&mDataProp_4=c3FiledInd"
        f"&mDataProp_5=candidateAddress&mDataProp_6=candidateTypeDescr&mDataProp_7=officeTitle"
        f"&mDataProp_8=resCountyDescr&sSearch=&bRegex=false"
        f"&sSearch_0=&bRegex_0=false&bSearchable_0=true"
        f"&sSearch_1=&bRegex_1=false&bSearchable_1=true"
        f"&sSearch_2=&bRegex_2=false&bSearchable_2=true"
        f"&sSearch_3=&bRegex_3=false&bSearchable_3=true"
        f"&sSearch_4=&bRegex_4=false&bSearchable_4=true"
        f"&sSearch_5=&bRegex_5=false&bSearchable_5=true"
        f"&sSearch_6=&bRegex_6=false&bSearchable_6=true"
        f"&sSearch_7=&bRegex_7=false&bSearchable_7=true"
        f"&sSearch_8=&bRegex_8=false&bSearchable_8=true"
        f"&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1"
        f"&bSortable_0=false&bSortable_1=true&bSortable_2=true&bSortable_3=true"
        f"&bSortable_4=false&bSortable_5=false&bSortable_6=true&bSortable_7=true"
        f"&bSortable_8=true&_={_cache_bust()}"
    )
    r = s.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("aaData", [])


def fetch_committee_list(election_year: str) -> list[dict]:
    """All committees with reported financial activity in a given election year."""
    search = COMMITTEE_SEARCH_DEFAULT.copy()
    search["electionYear"] = election_year
    s = _session()
    s.post(COMMITTEE_SEARCH_URL, data=search, timeout=60)
    url = (
        f"{COMMITTEE_LIST_URL}?sEcho=1&iColumns=4&sColumns=&iDisplayStart=0"
        f"&iDisplayLength={MAX_ROWS}&mDataProp_0=checked&mDataProp_1=committeeName"
        f"&mDataProp_2=electionYear&mDataProp_3=committeeTypeDescr&sSearch=&bRegex=false"
        f"&sSearch_0=&bRegex_0=false&bSearchable_0=true"
        f"&sSearch_1=&bRegex_1=false&bSearchable_1=true"
        f"&sSearch_2=&bRegex_2=false&bSearchable_2=true"
        f"&sSearch_3=&bRegex_3=false&bSearchable_3=true"
        f"&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1"
        f"&bSortable_0=false&bSortable_1=true&bSortable_2=true&bSortable_3=true"
        f"&_={_cache_bust()}"
    )
    r = s.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("aaData", [])


def fetch_entity_reports(entity_type: str, entity_id) -> list[dict]:
    """List every report (any form type) filed by one candidate or committee."""
    if entity_type == "candidate":
        post_url = CAND_REPORTS_POST_URL
        payload  = {"candidateId": entity_id, "searchType": "", "searchPage": "public"}
    else:
        post_url = CMTE_REPORTS_POST_URL
        payload  = {"committeeId": entity_id, "financialSearchType": "COMMITTEE",
                    "searchPage": "public"}
    s = _session()
    s.post(post_url, data=payload, timeout=60)
    url = (
        f"{REPORTS_LIST_URL}?sEcho=1&iColumns=6&sColumns=&iDisplayStart=0"
        f"&iDisplayLength={MAX_ROWS}&mDataProp_0=checked&mDataProp_1=fromDateStr"
        f"&mDataProp_2=toDateStr&mDataProp_3=formTypeDescr&mDataProp_4=formTypeCode"
        f"&mDataProp_5=statusDescr&sSearch=&bRegex=false"
        f"&sSearch_0=&bRegex_0=false&bSearchable_0=true"
        f"&sSearch_1=&bRegex_1=false&bSearchable_1=true"
        f"&sSearch_2=&bRegex_2=false&bSearchable_2=true"
        f"&sSearch_3=&bRegex_3=false&bSearchable_3=true"
        f"&sSearch_4=&bRegex_4=false&bSearchable_4=true"
        f"&sSearch_5=&bRegex_5=false&bSearchable_5=true"
        f"&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1"
        f"&bSortable_0=false&bSortable_1=true&bSortable_2=true&bSortable_3=true"
        f"&bSortable_4=true&bSortable_5=true&_={_cache_bust()}"
    )
    r = s.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("aaData", [])


def fetch_schedule(report_id, schedule_code: str, entity_name: str) -> list[dict]:
    """C5/C6/C4 bulk schedule export -- pipe-delimited text, parsed to list-of-dict
    rows with the server's own column headers preserved verbatim."""
    payload = {"reportId": report_id, "scheduleCode": schedule_code, "fname": entity_name}
    s = _session()
    try:
        p = s.post(PREPARE_DOWNLOAD_URL, data=payload, timeout=180)
        p.raise_for_status()
        meta = p.json()
    except Exception:
        return []
    if "fileName" not in meta:
        return []
    r = s.get(DOWNLOAD_FILE_URL, params=meta, timeout=180)
    text = r.text
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="|", quoting=csv.QUOTE_NONE)
    return list(reader)


def fetch_detail_lists(entity_type: str, entity_id, report_id, list_names: list[str]) -> dict:
    """C7/C7E line items -- no bulk export exists, so each sub-table (individual
    donors, committee donors, loans, etc.) is fetched as JSON directly."""
    id_field = "candidateId" if entity_type == "candidate" else "committeeId"
    s = _session()
    s.post(RETRIEVE_REPORT_URL,
          data={id_field: entity_id, "reportId": report_id, "searchPage": "public"},
          timeout=60)
    out = {}
    for list_name in list_names:
        try:
            r = s.post(DETAIL_LIST_URL, data={"listName": list_name}, timeout=60)
            out[list_name] = r.json() if r.text.strip() else []
        except Exception:
            out[list_name] = []
    return out


# ============================ entity fetch =============================

def fetch_entity_full(log, entity_type: str, entity_data: dict) -> dict:
    """
    Fetch every filed report for one candidate/committee and its full itemized
    transaction detail. Returns the JSON structure written to
    candidate_{id}.json / committee_{id}.json.
    """
    id_field    = "candidateId" if entity_type == "candidate" else "committeeId"
    name_field  = "candidateName" if entity_type == "candidate" else "committeeName"
    entity_id   = entity_data[id_field]
    entity_name = entity_data.get(name_field, "")

    raw_reports = fetch_entity_reports(entity_type, entity_id)
    reports_out = []

    log.debug(f"  {entity_type} {entity_id} ({entity_name}): {len(raw_reports)} reports to fetch")

    for i, rep in enumerate(raw_reports, 1):
        form_type = rep.get("formTypeCode", "")
        report_id = rep.get("reportId")
        log.debug(f"    [{i}/{len(raw_reports)}] report {report_id} ({form_type}) "
                 f"{rep.get('fromDateStr')}–{rep.get('toDateStr')}")
        entry = {
            "reportId":      report_id,
            "formTypeCode":  form_type,
            "formTypeDescr": rep.get("formTypeDescr"),
            "fromDateStr":   rep.get("fromDateStr"),
            "toDateStr":     rep.get("toDateStr"),
            "statusDescr":   rep.get("statusDescr"),
            "amendedDate":   rep.get("amendedDate"),
            "contributions": [],
            "expenditures":  [],
        }

        try:
            if form_type in SCHEDULE_CODES:
                code_a, code_b = SCHEDULE_CODES[form_type]
                entry["contributions"] = fetch_schedule(report_id, code_a, entity_name)
                entry["expenditures"]  = fetch_schedule(report_id, code_b, entity_name)
            elif form_type == "C7":
                entry["contributions_c7"] = fetch_detail_lists(
                    entity_type, entity_id, report_id, C7_LIST_NAMES)
            elif form_type == "C7E":
                entry["expenditures_c7e"] = fetch_detail_lists(
                    entity_type, entity_id, report_id, C7E_LIST_NAMES)
            else:
                log.warning(f"  Unhandled report type {form_type!r} "
                           f"({entity_type} {entity_id}, report {report_id})")
        except Exception as e:
            log.page_scrape_error(entity=entity_type, page_id=f"{entity_id}/{report_id}",
                                  error=str(e))

        reports_out.append(entry)
        time.sleep(0.1)

    return {
        "entityType": entity_type,
        **entity_data,
        "reports": reports_out,
    }


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
):
    """
    Vertical scope (mutually exclusive):
        force=True             -- re-download all entities in scope, wipe manifest
        start_year / end_year  -- restrict to this election-year range

    Horizontal scope:
        No flags / entities / transactions   -- both candidates and committees
        candidates (alone)                   -- candidates only
        committees (alone)                   -- committees only

    Note: contributions/expenditures flags are accepted for interface
    consistency but ignored -- fetching an entity's filed reports always
    yields both contributions and expenditures together, so there's no
    cheaper partial fetch to do. transactions/entities are likewise
    equivalent here since CERS doesn't separate "registry" from "financial
    activity" the way e.g. Arkansas's API does.
    """
    log = get_logger("montana", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              start_year=start_year, end_year=end_year,
              contributions=contributions, expenditures=expenditures,
              candidates=candidates, committees=committees)

    if candidates and not committees:
        do_candidates, do_committees = True, False
    elif committees and not candidates:
        do_candidates, do_committees = False, True
    else:
        do_candidates, do_committees = True, True

    current_year = datetime.today().year
    range_start  = start_year if start_year is not None else START_YEAR
    years = [y for y in range(range_start, current_year + 1) if end_year is None or y <= end_year]

    year_range_active = start_year is not None or end_year is not None

    if force:
        strip_manifest(lambda r: not (
            (do_candidates and r["entity_type"] == "candidate") or
            (do_committees and r["entity_type"] == "committee")
        ) or int(r.get("election_year") or 0) not in years)
        done = load_manifest()
    elif year_range_active:
        strip_manifest(lambda r: int(r.get("election_year") or 0) not in years)
        done = load_manifest()
    else:
        done = load_manifest()

    files_ok = files_err = 0

    try:
        for year in years:
            year_str = str(year)
            is_current_year = (year == current_year)

            # -- Candidates ---------------------------------------------
            if do_candidates:
                try:
                    cand_list = fetch_candidate_list(year_str)
                except Exception as e:
                    log.file_download_error(filename=f"candidates_{year_str}.json", error=str(e))
                    files_err += 1
                    cand_list = []

                if cand_list:
                    out_path = RAW_DIR / f"candidates_{year_str}.json"
                    out_path.write_text(json.dumps(cand_list, indent=2), encoding="utf-8")
                    log.file_download_ok(filename=out_path.name, bytes=out_path.stat().st_size,
                                        rows=len(cand_list), duration_s=0.0)
                    files_ok += 1

                for cand in cand_list:
                    entity_id = cand.get("candidateId")
                    if entity_id is None:
                        continue
                    key = ("candidate", str(entity_id))
                    if key in done and not is_current_year and not force and not year_range_active:
                        log.file_download_skip(filename=f"candidate_{entity_id}.json")
                        continue

                    try:
                        full = fetch_entity_full(log, "candidate", cand)
                    except Exception as e:
                        log.page_scrape_error(entity="candidate", page_id=entity_id, error=str(e))
                        files_err += 1
                        continue

                    out_path = RAW_DIR / f"candidate_{entity_id}.json"
                    out_path.write_text(json.dumps(full, indent=2, default=str), encoding="utf-8")
                    append_manifest({
                        "entity_type":    "candidate",
                        "entity_id":      str(entity_id),
                        "election_year":  year_str,
                        "filename":       out_path.name,
                        "downloaded_at":  datetime.today().strftime("%Y-%m-%d"),
                        "num_reports":    len(full.get("reports", [])),
                    })
                    done.add(key)
                    files_ok += 1
                    time.sleep(0.15)

            # -- Committees ----------------------------------------------
            if do_committees:
                try:
                    cmte_list = fetch_committee_list(year_str)
                except Exception as e:
                    log.file_download_error(filename=f"committees_{year_str}.json", error=str(e))
                    files_err += 1
                    cmte_list = []

                if cmte_list:
                    out_path = RAW_DIR / f"committees_{year_str}.json"
                    out_path.write_text(json.dumps(cmte_list, indent=2), encoding="utf-8")
                    log.file_download_ok(filename=out_path.name, bytes=out_path.stat().st_size,
                                        rows=len(cmte_list), duration_s=0.0)
                    files_ok += 1

                for cmte in cmte_list:
                    entity_id = cmte.get("committeeId")
                    if entity_id is None:
                        continue
                    key = ("committee", str(entity_id))
                    if key in done and not is_current_year and not force and not year_range_active:
                        log.file_download_skip(filename=f"committee_{entity_id}.json")
                        continue

                    try:
                        full = fetch_entity_full(log, "committee", cmte)
                    except Exception as e:
                        log.page_scrape_error(entity="committee", page_id=entity_id, error=str(e))
                        files_err += 1
                        continue

                    out_path = RAW_DIR / f"committee_{entity_id}.json"
                    out_path.write_text(json.dumps(full, indent=2, default=str), encoding="utf-8")
                    append_manifest({
                        "entity_type":    "committee",
                        "entity_id":      str(entity_id),
                        "election_year":  year_str,
                        "filename":       out_path.name,
                        "downloaded_at":  datetime.today().strftime("%Y-%m-%d"),
                        "num_reports":    len(full.get("reports", [])),
                    })
                    done.add(key)
                    files_ok += 1
                    time.sleep(0.15)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s -- {files_ok} ok, {files_err} errors")
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


# ================================ CLI ================================

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Download Montana campaign finance data from CERS."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download all entities in scope, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest election year to download (inclusive)")

    ap.add_argument("--end-year", type=int, metavar="YYYY",
                    help="latest election year to download (inclusive, <= current year)")

    ap.add_argument("--transactions", action="store_true", help="ignored -- see module docstring")
    ap.add_argument("--entities",     action="store_true", help="ignored -- see module docstring")
    ap.add_argument("--contributions", action="store_true", help="ignored -- see module docstring")
    ap.add_argument("--expenditures",  action="store_true", help="ignored -- see module docstring")
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
