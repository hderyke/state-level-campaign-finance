import argparse
import csv
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests as req_lib

PROJECT_ROOT = Path(__file__).resolve().parents[4]
RAW_DIR      = PROJECT_ROOT / "data" / "Arizona" / "raw"
MANIFEST     = PROJECT_ROOT / "data" / "Arizona" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL      = "https://seethemoney.az.gov/Reporting/AdvancedSearch/"
REPORTING_URL = "https://seethemoney.az.gov/Reporting"

MANIFEST_COLS = ["cycle_label", "filer_type", "category_type", "filename", "downloaded_at", "row_count"]

CYCLES = [
    ("2026",        "44~1/1/2025 12:00:00 AM~12/31/2026 11:59:59 PM"),
    ("2024",        "43~1/1/2023 12:00:00 AM~12/31/2024 11:59:59 PM"),
    ("2022",        "39~1/1/2021 12:00:00 AM~12/31/2022 11:59:59 PM"),
    ("Recall_Fann", "40~7/1/2021 12:00:00 AM~3/31/2022 11:59:59 PM"),
    ("2020",        "30~1/1/2019 12:00:00 AM~12/31/2020 11:59:59 PM"),
    ("2018",        "29~11/9/2016 12:00:00 AM~12/31/2018 11:59:59 PM"),
    ("2016",        "28~11/25/2014 12:00:00 AM~11/8/2016 11:59:59 PM"),
    ("2014",        "27~11/27/2012 12:00:00 AM~11/24/2014 11:59:59 PM"),
    ("2012",        "26~11/23/2010 12:00:00 AM~11/26/2012 11:59:59 PM"),
    ("2010",        "25~11/25/2008 12:00:00 AM~11/22/2010 11:59:59 PM"),
    ("2008",        "8~11/28/2006 12:00:00 AM~11/24/2008 11:59:59 PM"),
    ("2006",        "7~11/23/2004 12:00:00 AM~11/27/2006 11:59:59 PM"),
    ("2004",        "6~11/26/2002 12:00:00 AM~11/22/2004 11:59:59 PM"),
    ("2002",        "5~11/28/2000 12:00:00 AM~11/25/2002 11:59:59 PM"),
    ("2000",        "4~11/24/1998 12:00:00 AM~11/27/2000 11:59:59 PM"),
    ("1998",        "3~11/26/1996 12:00:00 AM~11/23/1998 11:59:59 PM"),
]

CATEGORY_TYPES = ["Income", "Expenditures"]

FILER_TYPES = [
    ("130", "Candidate"),
    ("131", "PAC"),
    ("132", "Party"),
    ("96",  "Officeholder"),
]

DT_COLUMNS = [
    "TransactionDate", "CommitteeName", "Amount", "TransactionName",
    "TransactionType", "Occupation", "Employer", "City", "State", "ZipCode",
]

REGISTRY_PAGES = {
    1: "Candidate",
    2: "PAC",
    3: "Party",
    4: "BallotMeasure",
    5: "Officeholder",
    6: "Other",
}

REGISTRY_COLS = [
    "entity_id", "public_transaction_table_id", "filer_type",
    "entity_type_name", "entity_type_id", "committee_name",
    "entity_last_name", "entity_first_name", "entity_middle_name",
    "office_name", "party_name", "city", "state", "zip",
    "income", "expense", "cash_balance",
    "ie_support", "ie_opposition", "ballot_measure_id", "ballot_name", "identifier",
]

DETAIL_COLS = [
    "entity_id", "committee_name", "committee_type_name",
    "status", "registration_date", "last_amended_date", "last_filed_date",
    "phone", "email", "mailing_address", "filer_address",
    "city", "state", "zip", "county",
    "chairman_name", "treasurer_name", "master_committee_id",
]


# ── Shared helpers ─────────────────────────────────────────────────────────────

def parse_net_date(val) -> str:
    if not val:
        return ""
    if isinstance(val, str):
        m = re.search(r"/Date\((-?\d+)\)/", val)
        if m:
            return datetime.fromtimestamp(int(m.group(1)) / 1000,
                                          tz=timezone.utc).strftime("%Y-%m-%d")
    return str(val)


def s(v) -> str:
    return str(v).strip() if v is not None else ""


def warmup_session(pw_context=None) -> req_lib.Session:
    if pw_context is not None:
        cookies = pw_context.cookies()
    else:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            print("[!] pip install playwright && playwright install chromium")
            sys.exit(1)
        print("Warming up browser session ...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            ctx     = browser.new_context()
            page    = ctx.new_page()
            page.goto(BASE_URL, timeout=30_000)
            page.wait_for_load_state("networkidle")
            cookies = ctx.cookies()
            browser.close()
        print("Session ready.\n")

    session = req_lib.Session()
    for cookie in cookies:
        session.cookies.set(cookie["name"], cookie["value"],
                            domain=cookie.get("domain", ""))
    session.headers.update({
        "User-Agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          BASE_URL,
    })
    return session


# ── Registry ───────────────────────────────────────────────────────────────────

def fetch_registry_page(session: req_lib.Session, page_num: int) -> list[dict]:
    params = {
        "Page":             str(page_num),
        "JurisdictionId":   "0",
        "TablePage":        "1",
        "TableLength":      "999999",
        "IsLessActive":     "false",
        "ShowOfficeHolder": "false",
        "ChartName":        str(page_num),
        "ShowAllYears":     "true",
    }
    r = session.post(f"{REPORTING_URL}/GetTableData", params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def normalize_registry_row(raw: dict, filer_type: str) -> dict:
    return {
        "entity_id":                   s(raw.get("EntityID")),
        "public_transaction_table_id": s(raw.get("PublicTransactionTableID")),
        "filer_type":                  filer_type,
        "entity_type_name":            s(raw.get("EntityTypeName")),
        "entity_type_id":              s(raw.get("EntityTypeId")),
        "committee_name":              s(raw.get("CommitteeName")),
        "entity_last_name":            s(raw.get("EntityLastName")),
        "entity_first_name":           s(raw.get("EntityFirstName")),
        "entity_middle_name":          s(raw.get("EntityMiddleName")),
        "office_name":                 s(raw.get("OfficeName")),
        "party_name":                  s(raw.get("PartyName")),
        "city":                        s(raw.get("PhysicalCity")),
        "state":                       s(raw.get("PhysicalState")),
        "zip":                         s(raw.get("PhysicalZipCode")),
        "income":                      s(raw.get("Income")),
        "expense":                     s(raw.get("Expense")),
        "cash_balance":                s(raw.get("CashBalance")),
        "ie_support":                  s(raw.get("IESupport")),
        "ie_opposition":               s(raw.get("IEOpposition")),
        "ballot_measure_id":           s(raw.get("BallotMeasureId")),
        "ballot_name":                 s(raw.get("BallotName")),
        "identifier":                  s(raw.get("Identifier")),
    }


def download_registry(session: req_lib.Session):
    out   = RAW_DIR / "az_committees_all.csv"
    total = 0
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=REGISTRY_COLS, extrasaction="ignore")
        writer.writeheader()
        for page_num, filer_type in REGISTRY_PAGES.items():
            print(f"  Registry page={page_num} ({filer_type})...", end=" ", flush=True)
            try:
                rows = fetch_registry_page(session, page_num)
            except Exception as e:
                print(f"ERROR: {e}")
                continue
            if not isinstance(rows, list):
                print(f"unexpected type: {type(rows)}")
                continue
            for raw in rows:
                writer.writerow(normalize_registry_row(raw, filer_type))
            total += len(rows)
            print(f"{len(rows):,} rows")
            time.sleep(0.5)
    print(f"  Registry: {total:,} total → {out.name}\n")


# ── Transactions ───────────────────────────────────────────────────────────────

def parse_cycle_dates(cycle_id_str: str) -> tuple[str, str]:
    _, raw_start, raw_end = cycle_id_str.split("~", 2)
    fmt   = "%m/%d/%Y %I:%M:%S %p"
    start = datetime.strptime(raw_start.strip(), fmt).strftime("%Y-%m-%d")
    end   = datetime.strptime(raw_end.strip(),   fmt).strftime("%Y-%m-%d")
    return start, end


def build_hash_url(cycle_id_str: str, category_type: str, filer_type_id: str) -> str:
    start_date, end_date = parse_cycle_dates(cycle_id_str)
    id_part, raw_start, raw_end = cycle_id_str.split("~", 2)
    encoded_cycle = f"{id_part}~{quote(raw_start, safe='')}~{quote(raw_end, safe='')}"
    params = "|".join([
        "JurisdictionId=0", "CommiteeReportId=",
        f"CategoryType={category_type}", f"CycleId={encoded_cycle}",
        f"StartDate={start_date}", f"EndDate={end_date}",
        "FilerName=", "FilerId=", "BallotName=", "BallotMeasureId=",
        f"FilerTypeId={filer_type_id}",
        "OfficeTypeId=", "OfficeId=", "PartyId=",
        "ContributorName=", "VendorName=", "StateId=", "City=",
        "Employer=", "Occupation=", "CandidateName=", "CandidateFilerId=",
        "Position=Support", "LowAmount=", "HighAmount=",
        "TablePage=1", "TableLength=10",
    ])
    return f"{BASE_URL}#{params}"


def download_one(cycle_label, cycle_id_str, filer_type_id, filer_type_label,
                 category_type, pw_context) -> tuple[str, int] | None:
    page     = pw_context.new_page()
    filename = f"{category_type}_{cycle_label}_{filer_type_label}.csv"
    out_path = RAW_DIR / filename

    try:
        page.goto(build_hash_url(cycle_id_str, category_type, filer_type_id),
                  timeout=60_000)
        page.wait_for_load_state("networkidle")
        page.locator("button.ExportDiv:visible").click(timeout=15_000)
        page.wait_for_selector("#ExportFormatOptions", timeout=10_000)
        page.select_option("#ExportFormatOptions", value="CSV")
        with page.expect_download(timeout=180_000) as dl_info:
            page.click("button#Export")
        dl_info.value.save_as(str(out_path))
    except Exception as e:
        print(f"failed: {e}")
        page.close()
        return None

    page.close()

    raw = out_path.read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw.decode("utf-16")
    elif len(raw) > 1 and raw[1] == 0:
        text = raw.decode("utf-16-le")
    elif raw[:3] == b"\xef\xbb\xbf":
        text = raw[3:].decode("utf-8")
    else:
        text = raw.decode("utf-8", errors="replace")

    out_path.write_text(text, encoding="utf-8")
    row_count = max(text.count("\n") - 1, 0)
    return filename, row_count


def load_manifest() -> set[tuple[str, str, str]]:
    if not MANIFEST.exists():
        return set()
    done = set()
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            if "filer_type" in row and row["filer_type"]:
                filer = row["filer_type"]
            else:
                parts = row.get("filename", "").replace(".csv", "").split("_")
                filer = parts[2] if len(parts) >= 3 else ""
            done.add((row["cycle_label"], filer, row["category_type"]))
    return done


def strip_manifest(keep_fn):
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


def append_manifest(record: dict):
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(record)


def download_transactions(pw_context, done: set, current_only: bool = False):
    current_year = str(datetime.today().year)
    cycles = [(lbl, cid) for lbl, cid in CYCLES
              if not current_only or current_year in lbl]

    for cycle_label, cycle_id_str in cycles:
        for filer_type_id, filer_type_label in FILER_TYPES:
            for category_type in CATEGORY_TYPES:
                key = (cycle_label, filer_type_label, category_type)
                if key in done and current_year not in cycle_label:
                    print(f"  {category_type} {cycle_label} {filer_type_label}: skip")
                    continue

                print(f"  {category_type} {cycle_label} {filer_type_label}: ",
                      end="", flush=True)
                result = download_one(cycle_label, cycle_id_str,
                                      filer_type_id, filer_type_label,
                                      category_type, pw_context)
                if result is None:
                    print("failed — will retry next run")
                    continue

                filename, row_count = result
                if row_count == 0:
                    print("no records, skipping")
                    continue

                print(f"→ {filename} ({row_count:,} rows)")
                append_manifest({
                    "cycle_label":   cycle_label,
                    "filer_type":    filer_type_label,
                    "category_type": category_type,
                    "filename":      filename,
                    "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                    "row_count":     row_count,
                })
                done.add(key)
                time.sleep(1)


# ── Committee details ──────────────────────────────────────────────────────────

def load_detail_done() -> set[str]:
    out = RAW_DIR / "az_committee_details.csv"
    if not out.exists():
        return set()
    with open(out, newline="") as f:
        return {row["entity_id"] for row in csv.DictReader(f)}


def load_entity_ids() -> list[str]:
    reg = RAW_DIR / "az_committees_all.csv"
    if not reg.exists():
        reg = RAW_DIR / "az_committees.csv"
    if not reg.exists():
        print("[!] No committee registry found — run without --update-entities first")
        sys.exit(1)
    ids = []
    with open(reg, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = (row.get("entity_id") or "").strip()
            if eid:
                ids.append(eid)
    return sorted(set(ids), key=lambda x: int(x) if x.isdigit() else 0)


def extract_detail_fields(entity_id: str, info: dict) -> dict:
    row = {col: "" for col in DETAIL_COLS}
    row["entity_id"] = entity_id
    if not info:
        return row
    row["committee_name"]      = s(info.get("CommitteeName"))
    row["committee_type_name"] = s(info.get("CommitteeTypeName"))
    row["status"]              = s(info.get("Status"))
    row["registration_date"]   = parse_net_date(info.get("RegistrationDate"))
    row["last_amended_date"]   = parse_net_date(info.get("LastAmendedDate"))
    row["last_filed_date"]     = parse_net_date(info.get("LastFiledDate"))
    row["phone"]               = s(info.get("PhoneNo"))
    row["email"]               = s(info.get("Email"))
    row["mailing_address"]     = s(info.get("CommitteeAddress"))
    row["filer_address"]       = s(info.get("FilerAddress"))
    row["city"]                = s(info.get("City"))
    row["state"]               = s(info.get("State"))
    row["zip"]                 = s(info.get("Zip") or info.get("ZipCode"))
    row["county"]              = s(info.get("County"))
    row["chairman_name"]       = s(info.get("ChairmanName"))
    row["treasurer_name"]      = s(info.get("TreasurerName"))
    row["master_committee_id"] = str(info.get("MasterCommitteeId") or "")
    return row


def download_committee_details(session: req_lib.Session, force: bool = False):
    out        = RAW_DIR / "az_committee_details.csv"
    entity_ids = load_entity_ids()
    done       = set() if force else load_detail_done()
    todo       = [eid for eid in entity_ids if eid not in done]

    print(f"Committee details: {len(entity_ids)} total, {len(done)} done, {len(todo)} to fetch")
    if not todo:
        print("Nothing to do.")
        return

    errors  = 0
    fetched = 0

    with open(out, "w" if force else "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=DETAIL_COLS, extrasaction="ignore")
        if force or not out.exists():
            writer.writeheader()

        for i, eid in enumerate(todo, 1):
            params = {
                "Page": "11", "startYear": "2025", "endYear": "2025",
                "JurisdictionId": "0", "TablePage": "1", "TableLength": "10",
                "Name": f"2~{eid}",
            }
            try:
                r = session.post(f"{REPORTING_URL}/GetDetailedInformation",
                                 params=params, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    info = (data.get("ReportFilerInfo") or {}) if isinstance(data, dict) else {}
                    writer.writerow(extract_detail_fields(eid, info))
                    fetched += 1
                else:
                    print(f"  [{i}/{len(todo)}] {eid}: HTTP {r.status_code}")
                    errors += 1
            except Exception as e:
                print(f"  [{i}/{len(todo)}] {eid}: ERROR {e}")
                errors += 1

            if i % 500 == 0:
                print(f"  {i}/{len(todo)} ({100*i/len(todo):.0f}%)  "
                      f"fetched={fetched}  errors={errors}")

            time.sleep(0.15)

    print(f"Details done: {fetched} fetched, {errors} errors → {out.name}\n")


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run(force: bool = False, update_transactions: bool = False,
        update_entities: bool = False):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[!] pip install playwright && playwright install chromium")
        return

    current_year = str(datetime.today().year)

    if force:
        if MANIFEST.exists():
            MANIFEST.unlink()
        done = set()
    elif update_transactions:
        strip_manifest(lambda r: current_year not in r.get("cycle_label", ""))
        done = load_manifest()
    else:
        done = load_manifest()

    if update_entities:
        session = warmup_session()
        download_registry(session)
        download_committee_details(session, force=True)
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()

        warmup = context.new_page()
        warmup.goto(BASE_URL, timeout=30_000)
        warmup.wait_for_load_state("networkidle")
        warmup.close()

        session = warmup_session(pw_context=context)

        if not update_transactions:
            print("Downloading registry (all filer types)...")
            download_registry(session)

        print("Downloading transactions...")
        download_transactions(context, done, current_only=update_transactions)

        browser.close()

    if not update_transactions:
        print("Downloading committee details...")
        session = warmup_session()
        download_committee_details(session)

    print("\nArizona: done.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",               action="store_true")
    ap.add_argument("--update-transactions", action="store_true")
    ap.add_argument("--update-entities",     action="store_true")
    args = ap.parse_args()
    run(force=args.force,
        update_transactions=args.update_transactions,
        update_entities=args.update_entities)
