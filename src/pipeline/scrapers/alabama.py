import csv
import io
import json
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
import requests
import urllib3
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# Make project root importable whether the file is run directly or as a module
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.logger import StateLogger, get_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Alabama" / "raw"
MANIFEST     = PROJECT_ROOT / "data" / "Alabama" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL      = "https://fcpa.alabamavotes.gov/page.request.do"
MANIFEST_COLS = ["id", "filename", "downloaded_at", "row_count"]

PAC_OUT_PATH = RAW_DIR / "pac_committees.csv"
PCC_OUT_PATH = RAW_DIR / "pcc_committees.csv"

CRITERIA_PAC = json.dumps([
    {"field_key": "committeeType", "comparison_type": "equalTo",
     "comparison_value_1": "2"}
])
CRITERIA_PCC = json.dumps([
    {"field_key": "committeeType", "comparison_type": "equalTo",
     "comparison_value_1": "1"}
])

COMMITTEE_COLS = [
    "committee_id", "committee_name", "committee_type", "pac_type",
    "committee_status", "registered_date", "dissolution_date",
    "address_line1", "city", "committee_state", "zip_code",
    "phone", "email", "purpose_of_pac", "duration_of_pac", "party",
    "candidate_first", "candidate_last", "office", "district", "jurisdiction",
    "treasurer_first", "treasurer_last", "treasurer_phone", "treasurer_email",
    "chairperson_first", "chairperson_last",
    "internal_id", "downloaded_at",
]


# ── Manifest helpers ──────────────────────────────────────────────────────────

TRANSACTION_IDS = range(1, 57)   # all known Alabama ZIP IDs


def load_manifest() -> dict[int, str]:
    """Return {id: filename} for every entry in the manifest."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="") as f:
        return {int(row["id"]): row["filename"] for row in csv.DictReader(f)}


def upsert_manifest(record: dict):
    """Add or overwrite the manifest entry for record['id']."""
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            existing = [r for r in csv.DictReader(f)
                        if r["id"] != str(record["id"])]
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)


# ── Transactions ──────────────────────────────────────────────────────────────

def download_zip(log: StateLogger, id: int) -> tuple[str, int] | None:
    params = {"page": "getTransactionData", "id": id}
    t0 = time.perf_counter()
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30, verify=False)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.file_download_error(filename=None, error=str(e), zip_id=id)
        return None

    if "zip" not in resp.headers.get("Content-Type", ""):
        log.file_download_error(
            filename=None,
            error=f"unexpected content type: {resp.headers.get('Content-Type')}",
            zip_id=id,
        )
        return None

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = zf.namelist()
        if not names:
            log.file_download_error(filename=None, error="empty zip", zip_id=id)
            return None
        data = zf.read(names[0]).decode("utf-8", errors="replace")

    out_path = RAW_DIR / names[0]
    out_path.write_text(data, encoding="utf-8")
    row_count = data.count("\n") - 1
    log.file_download_ok(
        filename=names[0],
        bytes=len(resp.content),
        rows=row_count,
        duration_s=time.perf_counter() - t0,
        zip_id=id,
    )
    return names[0], row_count


def download_transactions(log: StateLogger, force: bool = False) -> tuple[int, int]:
    """
    Smart download logic — behaviour depends on what's already present:

    force / manifest empty   → fetch all IDs (full historical download)
    partial (some IDs missing) → fetch missing IDs + refresh current-year IDs
    complete (all IDs present) → refresh current-year IDs only
    """
    current_year = str(datetime.today().year)
    all_ids      = set(TRANSACTION_IDS)

    if force:
        manifest = {}
    else:
        manifest = load_manifest()   # {id: filename}

    done_ids        = set(manifest.keys())
    missing_ids     = all_ids - done_ids
    current_yr_ids  = {i for i, fn in manifest.items() if fn.startswith(current_year)}

    if force or not done_ids:
        to_fetch = sorted(all_ids)
        log.info("Transactions: full download")
    elif missing_ids:
        to_fetch = sorted(missing_ids | current_yr_ids)
        log.info(f"Transactions: {len(missing_ids)} missing + "
                 f"{len(current_yr_ids)} current-year → {len(to_fetch)} ZIP(s) to fetch")
    else:
        # All IDs present — only refresh current year
        to_fetch = sorted(current_yr_ids)
        log.info(f"Transactions: complete — refreshing {len(to_fetch)} current-year ZIP(s)")

    if not to_fetch:
        log.info("Transactions: nothing to download.")
        return 0, 0

    ok = err = 0
    for id in to_fetch:
        log.file_download_start(filename=f"ZIP {id}")
        result = download_zip(log, id)
        if result is None:
            err += 1
            time.sleep(0.5)
            continue
        filename, row_count = result
        upsert_manifest({
            "id":            id,
            "filename":      filename,
            "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
            "row_count":     row_count,
        })
        ok += 1
        time.sleep(0.5)

    return ok, err


# ── Entities (PAC + PCC committees) ──────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36",
        "Referer": f"{BASE_URL}?page=page.acfPublicPoliticalActionCommitteeSearch",
    })
    return s


def fetch_all_committee_ids(log: StateLogger, session: requests.Session,
                            criteria: str) -> list[dict]:
    PAGE_SIZE   = 500
    all_records = []
    page        = 1
    while True:
        params = {
            "page":          "com.acf.common.page.committeesearchresults",
            "pageNumber":    page,
            "pageSize":      PAGE_SIZE,
            "sortDirection": "ASC",
            "sortBy":        "committeeName",
            "criteria":      criteria,
        }
        resp = session.get(f"{BASE_URL}?{urlencode(params)}", timeout=30)
        resp.raise_for_status()
        data    = resp.json()
        records = data["data"]["list"]
        total   = data["data"]["totalRecords"]
        all_records.extend(records)
        log.debug(f"  Page {page}: {len(all_records)}/{total} fetched")
        if len(all_records) >= total or not records:
            break
        page += 1
        time.sleep(0.2)
    return all_records


def fetch_detail(log: StateLogger, session: requests.Session, internal_id: int,
                 type_str: str = "pac") -> dict | None:
    import base64
    params = {
        "page": "page.acfPublicCommitteeDetails",
        "type": base64.b64encode(type_str.encode()).decode(),
        "id":   base64.b64encode(str(internal_id).encode()).decode(),
    }
    try:
        resp = session.get(f"{BASE_URL}?{urlencode(params)}", timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.page_scrape_error(entity=type_str, page_id=internal_id, error=str(e))
        return None
    m = re.search(r"const\s+committeeDetailsObj\s*=\s*(\{.*?\})\s*</script>",
                  resp.text, re.DOTALL)
    if not m:
        log.page_scrape_error(entity=type_str, page_id=internal_id,
                              error="committeeDetailsObj not found")
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        log.page_scrape_error(entity=type_str, page_id=internal_id,
                              error=f"JSON parse: {e}")
        return None


def extract_member(members: list, member_type: str) -> dict:
    for m in members:
        if m.get("memberType", "").lower() == member_type.lower():
            return m
    return {}


def flatten_detail(detail: dict) -> dict:
    members   = detail.get("members", [])
    treasurer = extract_member(members, "Treasurer")
    chair     = extract_member(members, "Chairperson")

    c_first   = detail.get("candidateFirstName", "").strip()
    c_mid     = detail.get("candidateMiddleName", "").strip()
    c_last    = detail.get("candidateLastName",  "").strip()
    cmte_name = detail.get("committeeName", "").strip()
    if not cmte_name and (c_first or c_last):
        cmte_name = " ".join(p for p in [c_first, c_mid, c_last] if p)

    def clean_date(val):
        return (val or "")[:10]

    return {
        "committee_id":      detail.get("committeeId", ""),
        "committee_name":    cmte_name,
        "committee_type":    detail.get("committeeType", ""),
        "pac_type":          detail.get("pacType", ""),
        "committee_status":  detail.get("committeeStatus", ""),
        "registered_date":   clean_date(detail.get("registeredDate")),
        "dissolution_date":  clean_date(detail.get("dissolutionDate")),
        "address_line1":     detail.get("committeeAddressLine1", ""),
        "city":              detail.get("city", ""),
        "committee_state":   detail.get("committeeState", ""),
        "zip_code":          detail.get("zipCode", ""),
        "phone":             detail.get("phone", ""),
        "email":             detail.get("email", ""),
        "purpose_of_pac":    detail.get("purposeOfPac", ""),
        "duration_of_pac":   detail.get("durationOfPac", ""),
        "party":             detail.get("party", ""),
        "candidate_first":   c_first,
        "candidate_last":    c_last,
        "office":            detail.get("office", ""),
        "district":          detail.get("district", ""),
        "jurisdiction":      detail.get("jurisdiction", ""),
        "treasurer_first":   treasurer.get("firstName", ""),
        "treasurer_last":    treasurer.get("lastName", ""),
        "treasurer_phone":   treasurer.get("phone", ""),
        "treasurer_email":   treasurer.get("email", ""),
        "chairperson_first": chair.get("firstName", ""),
        "chairperson_last":  chair.get("lastName", ""),
        "internal_id":       detail.get("id", ""),
        "downloaded_at":     datetime.today().strftime("%Y-%m-%d"),
    }


def fetch_and_write(log: StateLogger, session: requests.Session, criteria: str,
                    type_str: str, out_path: Path, label: str,
                    force: bool) -> tuple[int, int]:
    done_ids:      set[int]   = set()
    existing_rows: list[dict] = []
    t0 = time.perf_counter()

    if out_path.exists() and not force:
        with open(out_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                iid = row.get("internal_id", "")
                if iid and int(iid) not in done_ids:   # skip duplicates
                    done_ids.add(int(iid))
                    existing_rows.append(row)
        log.info(f"  Resuming: {len(done_ids)} {label} already fetched")

    stubs = fetch_all_committee_ids(log, session, criteria)

    # Deduplicate stubs — the API can return the same committee on multiple pages
    seen: set[int] = set()
    unique_stubs: list[dict] = []
    for s in stubs:
        sid = int(s["id"])
        if sid not in seen:
            seen.add(sid)
            unique_stubs.append(s)
    if len(unique_stubs) < len(stubs):
        log.info(f"  {label}: {len(stubs)} stubs from API → {len(unique_stubs)} unique "
                 f"({len(stubs) - len(unique_stubs)} duplicates dropped)")
    stubs = unique_stubs

    to_fetch = [s for s in stubs if int(s["id"]) not in done_ids]
    log.info(f"  {len(done_ids)} already done, {len(to_fetch)} to fetch out of {len(stubs)} unique stubs")

    new_rows: list[dict] = []
    ok = err = 0
    with logging_redirect_tqdm(loggers=[log._log]):
        bar = tqdm(
            to_fetch,
            desc=f"  {label}",
            unit="cmte",
            dynamic_ncols=True,
            colour="green",
        )
        interrupted = False
        try:
            for stub in bar:
                t0   = time.perf_counter()
                iid  = int(stub["id"])
                name = stub.get("committeeName", f"id={iid}")
                bar.set_postfix_str(name[:45].ljust(45), refresh=False)
                detail = fetch_detail(log, session, iid, type_str)
                duration = time.perf_counter() - t0
                if detail is None:
                    err += 1
                    time.sleep(0.25)
                    continue
                flat = flatten_detail(detail)
                new_rows.append(flat)
                ok += 1
                time.sleep(0.25)
        except KeyboardInterrupt:
            bar.close()
            interrupted = True

    all_rows = existing_rows + new_rows
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMMITTEE_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    log.page_scrape_complete(filename=str(out_path), rows=len(all_rows),
                             duration_s=time.perf_counter() - t0, ok=ok, err=err)
    if interrupted:
        raise KeyboardInterrupt
    return ok, err


def download_entities(log: StateLogger, force: bool = False) -> tuple[int, int]:
    session = make_session()
    total_ok = total_err = 0

    log.info("PACs:")
    ok, err = fetch_and_write(log, session, CRITERIA_PAC, "pac", PAC_OUT_PATH, "PACs", force)
    total_ok += ok; total_err += err

    log.info("PCCs:")
    ok, err = fetch_and_write(log, session, CRITERIA_PCC, "pcc", PCC_OUT_PATH, "PCCs", force)
    total_ok += ok; total_err += err

    return total_ok, total_err


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run(force: bool = False, entities: bool = False, transactions: bool = False):
    """
    Flag semantics
    ──────────────
    (no flags)              update everything incrementally
    --transactions          transactions only, incremental
    --entities              entities only, incremental
    --force                 force-refresh everything
    --force --transactions  force-refresh transactions only
    --force --entities      force-refresh entities only
    """
    log = get_logger("alabama", "scrape")
    t0  = time.perf_counter()
    log.info("Starting Alabama scraper")
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions)

    # Resolve scope: if neither flag is set, run both
    do_both         = not entities and not transactions
    do_transactions = transactions or do_both
    do_entities     = entities     or do_both

    files_ok = files_err = pages_ok = pages_err = 0

    try:
        if force and do_transactions and MANIFEST.exists():
            MANIFEST.unlink()

        if do_transactions:
            files_ok, files_err = download_transactions(log, force=force)

        if do_entities:
            pages_ok, pages_err = download_entities(log, force=force)

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err,
                  pages_ok=pages_ok, pages_err=pages_err)
        raise


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Alabama FCPA scraper")
    ap.add_argument("--force",        action="store_true",
                    help="force re-download (scope: all, or --transactions/--entities)")
    ap.add_argument("--transactions", action="store_true",
                    help="transactions only")
    ap.add_argument("--entities",     action="store_true",
                    help="entities (PACs + PCCs) only")
    args = ap.parse_args()
    try:
        run(force=args.force, entities=args.entities, transactions=args.transactions)
    except KeyboardInterrupt:
        sys.exit(130)
