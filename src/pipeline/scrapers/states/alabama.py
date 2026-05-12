import csv
import io
import json
import re
import time
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_ROOT = Path(__file__).resolve().parents[4]
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

def load_manifest() -> set[int]:
    if not MANIFEST.exists():
        return set()
    current_year = str(datetime.today().year)
    with open(MANIFEST, newline="") as f:
        return {
            int(row["id"])
            for row in csv.DictReader(f)
            if not row["filename"].startswith(current_year)
        }


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
    already = load_manifest()
    if int(record["id"]) in already:
        return
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


# ── Transactions ──────────────────────────────────────────────────────────────

def download_zip(id: int) -> tuple[str, int] | None:
    params = {"page": "getTransactionData", "id": id}
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30, verify=False)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [!] ID {id} failed: {e}")
        return None

    if "zip" not in resp.headers.get("Content-Type", ""):
        print(f"  [!] ID {id} — unexpected content type: {resp.headers.get('Content-Type')}")
        return None

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        names = zf.namelist()
        if not names:
            print(f"  [!] ID {id} — empty zip")
            return None
        data = zf.read(names[0]).decode("utf-8", errors="replace")

    out_path = RAW_DIR / names[0]
    out_path.write_text(data, encoding="utf-8")
    return names[0], data.count("\n") - 1


def download_transactions(id_range: range = range(1, 57)):
    already_done = load_manifest()
    to_fetch     = [i for i in id_range if i not in already_done]

    if not to_fetch:
        print("Alabama transactions: nothing new to download.")
        return

    print(f"Alabama transactions: {len(to_fetch)} file(s) to fetch")
    for id in to_fetch:
        print(f"  ID {id}...", end=" ", flush=True)
        result = download_zip(id)
        if result is None:
            print("skipped.")
            continue
        filename, row_count = result
        append_manifest({
            "id":            id,
            "filename":      filename,
            "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
            "row_count":     row_count,
        })
        print(f"→ {filename} ({row_count:,} rows)")
        time.sleep(0.5)


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


def fetch_all_committee_ids(session: requests.Session, criteria: str) -> list[dict]:
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
        print(f"  Page {page}: {len(all_records)}/{total} fetched")
        if len(all_records) >= total or not records:
            break
        page += 1
        time.sleep(0.2)
    return all_records


def fetch_detail(session: requests.Session, internal_id: int,
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
        print(f"    [!] detail fetch failed for id={internal_id}: {e}")
        return None
    m = re.search(r"const\s+committeeDetailsObj\s*=\s*(\{.*?\})\s*</script>",
                  resp.text, re.DOTALL)
    if not m:
        print(f"    [!] committeeDetailsObj not found for id={internal_id}")
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        print(f"    [!] JSON parse error for id={internal_id}: {e}")
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


def fetch_and_write(session, criteria: str, type_str: str,
                    out_path: Path, label: str, force: bool):
    done_ids:      set[int]   = set()
    existing_rows: list[dict] = []

    if out_path.exists() and not force:
        with open(out_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                iid = row.get("internal_id", "")
                if iid:
                    done_ids.add(int(iid))
                    existing_rows.append(row)
        print(f"  Resuming: {len(done_ids)} {label} already fetched")

    stubs = fetch_all_committee_ids(session, criteria)

    if existing_rows and len(stubs) != len(existing_rows):
        print(f"  !! Count mismatch ({len(existing_rows)} on disk vs {len(stubs)} from API) "
              f"— re-fetching all")
        done_ids      = set()
        existing_rows = []

    to_fetch = [s for s in stubs if s["id"] not in done_ids]
    print(f"  Fetching details for {len(to_fetch)} {label}...")

    new_rows: list[dict] = []
    for i, stub in enumerate(to_fetch, 1):
        iid  = stub["id"]
        name = stub.get("committeeName", f"id={iid}")
        print(f"  [{i}/{len(to_fetch)}] {name}...", end=" ", flush=True)
        detail = fetch_detail(session, iid, type_str)
        if detail is None:
            print("skipped")
            continue
        new_rows.append(flatten_detail(detail))
        print(f"ok ({new_rows[-1]['committee_status']})")
        time.sleep(0.25)

    all_rows = existing_rows + new_rows
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMMITTEE_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"  → {out_path.name}: {len(all_rows)} total")


def download_entities(force: bool = False):
    session = make_session()
    print("Alabama PACs:")
    fetch_and_write(session, CRITERIA_PAC, "pac", PAC_OUT_PATH, "PACs", force)
    print("Alabama PCCs:")
    fetch_and_write(session, CRITERIA_PCC, "pcc", PCC_OUT_PATH, "PCCs", force)


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run(force: bool = False, update_transactions: bool = False,
        update_entities: bool = False, id_range: range = range(1, 57)):

    if force:
        if MANIFEST.exists():
            MANIFEST.unlink()

    if update_entities:
        download_entities(force=True)
        return

    download_transactions(id_range)

    if not update_transactions:
        download_entities(force=force)

    print("Alabama: done.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",               action="store_true")
    ap.add_argument("--update-transactions", action="store_true")
    ap.add_argument("--update-entities",     action="store_true")
    args = ap.parse_args()
    run(force=args.force,
        update_transactions=args.update_transactions,
        update_entities=args.update_entities)
