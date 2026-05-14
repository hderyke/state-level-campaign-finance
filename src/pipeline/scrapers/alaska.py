import csv
import time
from datetime import datetime
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Alaska" / "raw"
MANIFEST     = PROJECT_ROOT / "data" / "Alaska" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "year", "filename", "row_count"]

# ── Pages ─────────────────────────────────────────────────────────────────────
# Alaska requires a live browser session — Playwright handles this by clicking
# Search then Export just like a user would. Must be run from a local machine;
# datacenter IPs are blocked by Alaska's WAF.
PAGES = {
    "income":       "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx",
    "expenditures": "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDExpenditures.aspx",
    "candidates":   "https://aws.state.ak.us/apocreports/Campaign/AllCandidates.aspx?type=all",
    "groups":       "https://aws.state.ak.us/apocreports/Registration/GroupRegistration/GRForms.aspx",
}

TRANSACTION_RELATIONS = {"income", "expenditures"}
ENTITY_RELATIONS      = {"candidates", "groups"}

# Base filename stem per relation type; year gets appended: CDIncome_2019.csv
STEMS = {
    "income":       "CDIncome",
    "expenditures": "CDExpense",
    "candidates":   "CDCandidates",
    "groups":       "GRForms",
}


# ── Manifest helpers ──────────────────────────────────────────────────────────
def load_manifest() -> tuple[set[tuple[str, str]], set[str]]:
    """
    Return:
      done      — set of (relation_type, year) already downloaded
      has_data  — set of relation_types that have at least one manifest entry

    Backward-compatible: ignores extra columns (e.g. old 'downloaded_at' field).
    """
    done: set[tuple[str, str]] = set()
    has_data: set[str] = set()
    if not MANIFEST.exists():
        return done, has_data
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["year"]))
            has_data.add(row["relation_type"])
    return done, has_data


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
    """Append one record to the manifest (write header if file is new).
    Extra keys in record are silently ignored (extrasaction='ignore').
    """
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(record)


# ── Playwright helpers ────────────────────────────────────────────────────────
def get_available_years(page) -> list[str]:
    """
    Scrape the year dropdown on the current page and return a sorted list
    of year values (strings like '2019'), oldest-first.
    Excludes the sentinel values for 'Any' (-1) and blank options.
    """
    sel = page.locator("select[name*='ddlReportYear']")
    if not sel.count():
        return []
    options = sel.locator("option").all()
    years = [
        opt.get_attribute("value")
        for opt in options
        if opt.get_attribute("value") not in ("-1", "0", "", None)
    ]
    # Sort ascending so the manifest fills in chronologically
    return sorted(set(years))


def download_candidates(page, context) -> tuple[str, int] | None:
    """
    Download the AllCandidates registry.
    Uses year="All" to get every candidate in one export rather than
    looping by year — this is a reference table, not transactional data.
    """
    page_url = PAGES["candidates"]
    page.goto(page_url, timeout=30_000)
    page.wait_for_load_state("networkidle")

    # Set year to "All" (dropdown name differs from transactional pages)
    year_sel = page.locator("select[name*='ddlYear']")
    if year_sel.count():
        year_sel.select_option("All")

    # Click Search
    search_btn = page.locator("input[value='Search']")
    if search_btn.count():
        page.click("input[value='Search']")
        page.wait_for_load_state("networkidle")

    body_text = page.locator("body").inner_text()
    if "No records" in body_text or "0 records" in body_text.lower():
        print("    (no records)")
        return None

    page.click("input[value='Export']")

    csv_link = page.locator("a[id*='hlAllCSV']")
    try:
        csv_link.wait_for(timeout=15_000)
    except Exception:
        print("    [!] Export dialog did not appear for candidates")
        return None

    filename = "CDCandidates_all.csv"
    out_path = RAW_DIR / filename

    with page.expect_download(timeout=180_000) as dl_info:
        csv_link.click()

    dl = dl_info.value
    dl.save_as(str(out_path))

    text      = out_path.read_text(encoding="utf-8", errors="replace")
    row_count = text.count("\n") - 1
    return filename, row_count


def download_year(page, context, relation_type: str, year: str) -> tuple[str, int] | None:
    """
    On an already-loaded APOC search page, set year + status, run Search,
    open the Export dialog, and click the CSV link. Returns (filename, row_count)
    or None on failure.

    `page` should be freshly navigated to the relation's search URL before
    calling this function so ViewState is clean.
    """
    page_url = PAGES[relation_type]

    # Fresh page navigation keeps ASP.NET ViewState clean between years
    page.goto(page_url, timeout=30_000)
    page.wait_for_load_state("networkidle")

    # Set year
    year_sel = page.locator("select[name*='ddlReportYear']")
    if year_sel.count():
        year_sel.select_option(year)

    # Set status to "All Complete Forms" (includes amended filings)
    status_sel = page.locator("select[name*='ddlStatus']")
    if status_sel.count():
        try:
            status_sel.select_option(label="All Complete Forms")
        except Exception:
            status_sel.select_option("0")

    # Search
    page.click("input[value='Search']")
    page.wait_for_load_state("networkidle")

    # Check whether any results came back — no point exporting an empty set
    # Alaska shows "No records found" or a result count in a label
    body_text = page.locator("body").inner_text()
    if "No records" in body_text or "0 records" in body_text.lower():
        print(f"    (no records for {year})")
        return None

    # Open export dialog (or direct download on some pages)
    filename = f"{STEMS[relation_type]}_{year}.csv"
    out_path = RAW_DIR / filename

    csv_link = page.locator("a[id*='hlAllCSV']")

    with page.expect_download(timeout=180_000) as dl_info:
        page.click("input[value='Export']")
        # Some pages (e.g. GRForms) trigger a direct download on Export click;
        # others open a dialog with a CSV link — wait briefly for the dialog.
        try:
            csv_link.wait_for(timeout=8_000)
            csv_link.click()   # dialog appeared — click the CSV link inside it
        except Exception:
            pass               # no dialog → direct download already in flight

    dl = dl_info.value
    dl.save_as(str(out_path))

    text      = out_path.read_text(encoding="utf-8", errors="replace")
    row_count = text.count("\n") - 1
    return filename, row_count


# ── Main runner ───────────────────────────────────────────────────────────────
def run(force: bool = False, update_transactions: bool = False,
        update_entities: bool = False):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[!] Playwright not installed. Run: pip install playwright && playwright install chromium")
        return

    current_year = str(datetime.today().year)

    if force:
        if MANIFEST.exists():
            MANIFEST.unlink()
        done, has_data = set(), set()
    elif update_transactions:
        strip_manifest(lambda r: not (r["relation_type"] in TRANSACTION_RELATIONS
                                      and r["year"] == current_year))
        done, has_data = load_manifest()
    elif update_entities:
        strip_manifest(lambda r: r["relation_type"] not in ENTITY_RELATIONS)
        done, has_data = load_manifest()
    else:
        done, has_data = load_manifest()

    pages_to_run = (TRANSACTION_RELATIONS if update_transactions else
                    ENTITY_RELATIONS      if update_entities      else
                    set(PAGES.keys()))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page    = context.new_page()

        for relation_type, page_url in PAGES.items():
            if relation_type not in pages_to_run:
                continue

            print(f"\nAlaska {relation_type}:")

            # ── Candidates: flat registry, no year dimension ───────────────
            if relation_type == "candidates":
                key = ("candidates", "all")
                cand_file = RAW_DIR / "CDCandidates_all.csv"
                if (key in done or cand_file.exists()) and not force:
                    print("  already on disk — skipping")
                else:
                    print("  downloading...", end=" ", flush=True)
                    try:
                        result = download_candidates(page, context)
                    except Exception as e:
                        print(f"failed ({e})")
                        result = None

                    if result:
                        filename, row_count = result
                        print(f"→ {filename} ({row_count:,} rows)")
                        append_manifest({
                            "relation_type": "candidates",
                            "year":          "all",
                            "filename":      filename,
                            "row_count":     row_count,
                        })
                        done.add(key)
                    else:
                        print("skipped.")
                continue

            # ── Transactional tables: one file per year ────────────────────
            page.goto(page_url, timeout=30_000)
            page.wait_for_load_state("networkidle")

            years = get_available_years(page)
            if not years:
                print("  [!] Could not read year dropdown — skipping")
                continue

            print(f"  Available years: {years[0]}–{years[-1]} ({len(years)} total)")

            for year in years:
                if update_transactions and year != current_year:
                    continue

                key           = (relation_type, year)
                expected_file = RAW_DIR / f"{STEMS[relation_type]}_{year}.csv"
                already_done  = key in done or (expected_file.exists() and expected_file.stat().st_size > 0)

                if already_done and year != current_year and not force:
                    print(f"  {year}: already on disk — skipping")
                    continue

                print(f"  {year}: downloading...", end=" ", flush=True)
                try:
                    result = download_year(page, context, relation_type, year)
                except Exception as e:
                    print(f"failed ({e})")
                    continue

                if result is None:
                    print("skipped.")
                    continue

                filename, row_count = result
                print(f"→ {filename} ({row_count:,} rows)")
                append_manifest({
                    "relation_type": relation_type,
                    "year":          year,
                    "filename":      filename,
                    "row_count":     row_count,
                })
                done.add(key)
                time.sleep(1)

        browser.close()

    print("\nAlaska: done.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--force",                action="store_true")
    ap.add_argument("--update-transactions",  action="store_true")
    ap.add_argument("--update-entities",      action="store_true")
    args = ap.parse_args()
    run(force=args.force,
        update_transactions=args.update_transactions,
        update_entities=args.update_entities)
