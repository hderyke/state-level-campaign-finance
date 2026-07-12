"""
scrapers/maine.py — Download Maine campaign finance data.

Source: Maine Campaign Finance Disclosure System (MapLight/Rails)
  https://www.mainecampaignfinancedisclosure.com/public

Requires Playwright — Cloudflare WAF blocks datacenter IPs.
Run from a local machine with: playwright install chromium

Stages:
  1. Filer list sweep: paginate /public/filers (20/page, ~185 pages)
     to collect all filer UUIDs, names, types, and statuses.
     → data/Maine/raw/me_filer_list.csv

  2. Filer profile pages: visit each /public/filers/{uuid} to extract
     office, party, treasurer, and financing type.
     → data/Maine/raw/me_filer_profiles.csv

  3. Transaction pages: paginate /public/activities filtered by a date
     window (2018–present, 20 rows/page, all types combined). The
     server hard-caps any single query at 10,000 reachable rows — see
     MAX_ROWS below — so a plain per-year query silently truncates any
     year with more than 10,000 true matches. Windows are recursively
     bisected by date (and, as a last resort, by amount range) to stay
     under the cap. The parser routes rows to
     contributions/expenditures/loans_debts.
     → data/Maine/raw/me_transactions_{year}.csv

Limitations:
  - Server caps at 20 rows/page; no bulk CSV export for transactions.
  - **Hard 10,000-row cap per query.** The page footer ("Displaying
    items X-Y of Z in total") silently clamps Z to 10,000 once the true
    match count exceeds it, and requesting a page beyond the resulting
    500-page ceiling redirects back to page 500 rather than erroring —
    there is no error signal, just truncated data if unhandled.
    Confirmed empirically: a 2018 query with 100,060 true matches still
    displayed "of 10000 in total". The TRUE total is available instead
    from "Returned X records using Y filters" at the top of the page;
    see MAX_ROWS / _fetch_window() in the transaction sweep section.
  - List view provides: filer_name, transaction_type, contributor/payee,
    date, amount, transaction_id. Contributor address, employer, and
    occupation require per-transaction detail pages (~400K requests)
    and are not scraped.
  - Data available from 2018 only (system launch year).
"""

import csv
import random
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Maine" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Maine" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "key", "filename", "row_count"]

# ============================= constants ==============================
BASE_URL   = "https://www.mainecampaignfinancedisclosure.com"
START_YEAR = 2018   # system went live in 2018

FILER_LIST_PATH     = RAW_DIR / "me_filer_list.csv"
FILER_PROFILES_PATH = RAW_DIR / "me_filer_profiles.csv"

FILER_LIST_COLS = ["uuid", "name", "filer_type", "status"]

FILER_PROFILE_COLS = [
    "uuid",
    "office", "party", "financing_type",
    "treasurer_name", "treasurer_email",
    "principal_officer_name", "principal_officer_email",
]

TRANSACTION_COLS = [
    "filer_name", "transaction_type", "source_payee",
    "date", "amount", "transaction_id",
]

ENRICHMENT_PATH = RAW_DIR / "me_enrichment.csv"
ENRICHMENT_COLS = [
    "transaction_id",
    "contact_type", "address_1", "address_2", "city",
    "state_province", "zip_postal_code", "country",
    "occupation", "occupation_other", "employer",
    "election", "description",
]

# Restart enrichment browser every N pages to avoid renderer OOM
ENRICH_RESTART_EVERY = 2_000

SLEEP = (0.3, 0.8)   # (min, max) seconds between page requests — randomized

# ========================== manifest helpers ==========================

def load_manifest() -> set[tuple[str, str]]:
    done: set[tuple[str, str]] = set()
    if not MANIFEST.exists():
        return done
    with open(MANIFEST, newline="") as f:
        for row in csv.DictReader(f):
            done.add((row["relation_type"], row["key"]))
    return done


def _read_manifest_rows() -> list[dict]:
    if not MANIFEST.exists():
        return []
    with open(MANIFEST, newline="") as f:
        return list(csv.DictReader(f))


def upsert_manifest(record: dict) -> None:
    existing = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            existing = [
                r for r in csv.DictReader(f)
                if not (r["relation_type"] == record["relation_type"]
                        and r["key"] == record["key"])
            ]
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(existing)
        writer.writerow(record)


def strip_manifest(keep_fn) -> None:
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(r for r in rows if keep_fn(r))


# ========================== HTML parsing helpers ======================

def _parse_total(html: str) -> int:
    """Extract total record count from 'Displaying items X-Y of Z in total'."""
    m = re.search(r"of\s+([\d,]+)\s+in total", html)
    if m:
        return int(m.group(1).replace(",", ""))
    return 0


def _parse_filer_list_page(html: str) -> list[dict]:
    """Extract rows from the /public/filers list page table.

    Each row links to /public/filers/{uuid} — the UUID is the last path
    segment of that href.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.select("table tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        a = tds[0].find("a", href=True)
        if not a:
            continue
        href = a["href"]
        # UUID is the last segment: /public/filers/{uuid}
        uuid = href.rstrip("/").split("/")[-1]
        if len(uuid) < 32:   # sanity check — UUIDs are 36 chars
            continue
        rows.append({
            "uuid":       uuid,
            "name":       a.get_text(strip=True),
            "filer_type": tds[1].get_text(strip=True),
            "status":     tds[2].get_text(strip=True),
        })
    return rows


def _parse_transaction_page(html: str) -> list[dict]:
    """Extract transaction rows from the /public/activities list page.

    Columns: filer_name, transaction_type, source_payee, date, amount.
    transaction_id extracted from the <a href="/public/activities/{uuid}">
    on the filer_name cell.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.select("table tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        a = tds[0].find("a", href=True)
        transaction_id = ""
        if a:
            # href = /public/activities/{uuid}
            transaction_id = a["href"].rstrip("/").split("/")[-1]
        rows.append({
            "filer_name":       tds[0].get_text(strip=True),
            "transaction_type": tds[1].get_text(strip=True),
            "source_payee":     tds[2].get_text(strip=True),
            "date":             tds[3].get_text(strip=True),
            "amount":           tds[4].get_text(strip=True),
            "transaction_id":   transaction_id,
        })
    return rows


# JS injected into each filer profile page via page.evaluate() to extract
# structured fields from the <h2>/<dt>/<dd> DOM. Works for both candidate
# and committee profiles — candidate-only fields (office, party,
# financing_type) come from the "Candidate Information" section which is
# absent for committees, returning empty strings gracefully.
_PROFILE_JS = """
() => {
    const sections = {};
    let cur = null;
    for (const el of document.querySelectorAll('h2, dt')) {
        if (el.tagName === 'H2') {
            cur = el.innerText.trim();
            sections[cur] = {};
        } else if (el.tagName === 'DT' && cur) {
            const dd = el.nextElementSibling;
            if (dd && dd.tagName === 'DD') {
                const v = dd.innerText.trim();
                sections[cur][el.innerText.trim()] = (v === 'Not Provided') ? '' : v;
            }
        }
    }
    const cand  = sections['Candidate Information']       || {};
    const treas = sections['Treasurer Information']       || {};
    const princ = sections['Principal Officer Information'] || {};
    return {
        office:                   cand['Office Sought']      || '',
        party:                    cand['Party Affiliation']  || '',
        financing_type:           cand['Financing Type']     || '',
        treasurer_name:           treas['Name']              || '',
        treasurer_email:          treas['Email']             || '',
        principal_officer_name:   princ['Name']              || '',
        principal_officer_email:  princ['Email']             || '',
    };
}
"""


# ========================= navigation helper ==========================

def _goto(page, url: str, timeout: int = 30_000) -> str:
    """Navigate to url, wait for table content to render, return page HTML.

    Uses domcontentloaded (not networkidle) to avoid hanging on Cloudflare
    challenge XHRs, then waits for 'table tbody tr' so we don't read a
    partially-rendered Turbo/Hotwire page.  Falls back to a timed sleep if
    the selector never appears (e.g. empty result set or CF challenge page).
    """
    page.goto(url, timeout=timeout, wait_until="domcontentloaded")
    try:
        page.wait_for_selector("table tbody tr", timeout=10_000)
    except Exception:
        # Empty result page, or CF still loading — sleep and proceed anyway
        time.sleep(random.uniform(1.0, 2.0))
    time.sleep(random.uniform(*SLEEP))
    return page.content()


# ========================= filer list sweep ===========================

def scrape_filer_list(page, log, force: bool = False) -> int:
    """Paginate /public/filers (~185 pages) and write me_filer_list.csv.

    Returns the number of filers written. Skips if already done and not forced.
    """
    done = load_manifest()
    key  = ("filer_list", "all")

    if key in done and not force and FILER_LIST_PATH.exists():
        log.file_download_skip(filename=FILER_LIST_PATH.name)
        return 0

    log.file_download_start(filename=FILER_LIST_PATH.name)
    t0 = time.perf_counter()

    # Page 1 — also used to determine total filer count
    html  = _goto(page, f"{BASE_URL}/public/filers")
    total = _parse_total(html)
    rows  = _parse_filer_list_page(html)

    if total == 0:
        log.file_download_error(filename=FILER_LIST_PATH.name,
                                error="could not parse total filer count")
        return 0

    total_pages = (total + 19) // 20

    with logging_redirect_tqdm(loggers=[log._log]):
        with tqdm(desc="  me filers", unit="pg", total=total_pages, initial=1,
                  dynamic_ncols=True) as bar:
            for pg in range(2, total_pages + 1):
                html = _goto(page, f"{BASE_URL}/public/filers?page={pg}")
                rows += _parse_filer_list_page(html)
                bar.update(1)

    with open(FILER_LIST_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FILER_LIST_COLS)
        w.writeheader()
        w.writerows(rows)

    size = FILER_LIST_PATH.stat().st_size
    log.file_download_ok(filename=FILER_LIST_PATH.name, bytes=size,
                         rows=len(rows),
                         duration_s=round(time.perf_counter() - t0, 2))
    upsert_manifest({"relation_type": "filer_list", "key": "all",
                     "filename": FILER_LIST_PATH.name, "row_count": len(rows)})
    return len(rows)


# ====================== filer profile sweep ===========================

def scrape_filer_profiles(page, log, force: bool = False) -> int:
    """Visit each /public/filers/{uuid} and write me_filer_profiles.csv.

    Manifest tracks each UUID individually, so interrupted runs resume.
    On --force, clears all profile manifest entries and rewrites the file.
    """
    if not FILER_LIST_PATH.exists():
        log.warning("me_filer_list.csv not found — run filer list sweep first")
        return 0

    if force:
        strip_manifest(lambda r: r["relation_type"] != "filer_profile")

    done = load_manifest()

    with open(FILER_LIST_PATH, newline="", encoding="utf-8") as f:
        filers = list(csv.DictReader(f))

    to_scrape = [r for r in filers if ("filer_profile", r["uuid"]) not in done]

    if not to_scrape:
        log.info("  all filer profiles already scraped")
        return len(filers)

    # Load existing profiles to append/overwrite
    existing: dict[str, dict] = {}
    if FILER_PROFILES_PATH.exists() and not force:
        with open(FILER_PROFILES_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing[row["uuid"]] = row

    ok = err = 0
    t0 = time.perf_counter()

    with logging_redirect_tqdm(loggers=[log._log]):
        with tqdm(desc="  me profiles", unit="filer", total=len(to_scrape),
                  dynamic_ncols=True) as bar:
            for r in to_scrape:
                uuid = r["uuid"]
                try:
                    page.goto(f"{BASE_URL}/public/filers/{uuid}",
                              timeout=30_000, wait_until="domcontentloaded")
                    try:
                        page.wait_for_selector("dt", timeout=10_000)
                    except Exception:
                        time.sleep(random.uniform(1.0, 2.0))
                    time.sleep(random.uniform(*SLEEP))
                    prof = page.evaluate(_PROFILE_JS)
                    prof["uuid"] = uuid
                    existing[uuid] = prof
                    upsert_manifest({
                        "relation_type": "filer_profile",
                        "key":           uuid,
                        "filename":      FILER_PROFILES_PATH.name,
                        "row_count":     1,
                    })
                    ok += 1
                except Exception as e:
                    log.page_scrape_error(entity="filer_profile",
                                          page_id=uuid, error=str(e))
                    err += 1
                bar.update(1)

    with open(FILER_PROFILES_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FILER_PROFILE_COLS)
        w.writeheader()
        w.writerows(existing.values())

    log.page_scrape_complete(
        filename=str(FILER_PROFILES_PATH),
        rows=len(existing),
        duration_s=round(time.perf_counter() - t0, 1),
        ok=ok, err=err,
    )
    return len(existing)


# ====================== transaction sweep =============================

# Server-enforced ceiling on how many rows of a single /public/activities
# query are reachable via pagination. The footer text ("Displaying items
# X-Y of Z in total") silently clamps Z to this value once the true match
# count exceeds it — confirmed empirically: a 2018 date-window query with
# 100,060 true matches still showed "of 10000 in total", and navigating
# directly to page 501 (which would be item 10,001) redirects back to page
# 500 rather than erroring. There is no error signal for a truncated query
# beyond this cap — the old code silently accepted the clamped total.
MAX_ROWS = 10_000

# Fallback split dimension for the (so far unobserved) case where a single
# calendar day still exceeds MAX_ROWS on its own, so date bisection alone
# can't get under the cap. In cents, for q[amount_cents_gteq]/_lteq — the
# only other filter dimension the search form exposes besides date and
# transaction type. Boundaries are generous; most Maine transactions are
# small-dollar retail contributions, so the bulk of any single day's volume
# should land in the first band.
AMOUNT_BANDS_CENTS = [
    ("le100",   "",       "10000"),    # <= $100.00
    ("100to1k", "10001",  "100000"),   # $100.01 - $1,000.00
    ("gt1k",    "100001", ""),         # > $1,000.00
]


def _parse_true_total(html: str) -> int:
    """Extract the TRUE match count from 'Returned X records using Y
    filters' at the top of the page. This is deliberately NOT the same as
    _parse_total() (used for the filer list), which reads the pagination
    footer — that footer is the value the server clamps to MAX_ROWS once
    the true count exceeds it. See MAX_ROWS above."""
    m = re.search(r"Returned\s+([\d,]+)\s+records", html)
    return int(m.group(1).replace(",", "")) if m else 0


def _activities_url(date_from: date, date_to: date,
                    amount_gteq: str = "", amount_lteq: str = "") -> str:
    url = (
        f"{BASE_URL}/public/activities"
        f"?q%5Bdate_gteq%5D={date_from:%Y-%m-%d}"
        f"&q%5Bdate_lteq%5D={date_to:%Y-%m-%d}"
    )
    if amount_gteq:
        url += f"&q%5Bamount_cents_gteq%5D={amount_gteq}"
    if amount_lteq:
        url += f"&q%5Bamount_cents_lteq%5D={amount_lteq}"
    return url


def _goto_retry(page_holder: list, make_page, url: str, log, label: str) -> str:
    """Navigate to url via _goto(), restarting the browser context once on a
    renderer crash and giving up (returning "") after a second failure.

    page_holder is a one-element [(ctx, page)] list rather than a plain
    (ctx, page) tuple so that a mid-fetch context restart — needed because
    _fetch_window() recurses and every recursive call must see the same
    live context — is visible to every caller without threading extra
    return values through the whole call chain.
    """
    for attempt in range(2):
        ctx, pg = page_holder[0]
        try:
            return _goto(pg, url)
        except Exception as e:
            err_str = str(e).lower()
            if attempt == 0 and (
                "crashed" in err_str or "closed" in err_str or "target" in err_str
            ):
                log.warning(f"  Page crash at {label}: {e!s:.120} — restarting browser")
                try:
                    ctx.close()
                except Exception:
                    pass
                page_holder[0] = make_page()
            else:
                log.warning(f"  Skipping {label} after retry: {e!s:.80}")
                return ""
    return ""


def _fetch_window(page_holder: list, make_page, date_from: date, date_to: date,
                  log, depth: int = 0,
                  amount_gteq: str = "", amount_lteq: str = "") -> list[dict]:
    """Recursively fetch every transaction in [date_from, date_to].

    Probes the window's TRUE total (_parse_true_total). If it's within
    MAX_ROWS, paginates the window normally and returns all rows. If it's
    over the cap, bisects the window at its midpoint date and recurses on
    each half — same strategy as florida.py's date-chunk splitting, adapted
    to Maine's plain GET date-range filter instead of a form-fill flow.

    If a single calendar day is still over the cap on its own (date bisection
    exhausted), falls back to splitting that day by amount range
    (AMOUNT_BANDS_CENTS). If a query is already inside an amount band and
    STILL over the cap — no further split axis available — accepts the
    truncated first MAX_ROWS rows and logs a loud warning rather than
    silently losing data with no trace.
    """
    label    = f"{date_from}..{date_to}"
    url_base = _activities_url(date_from, date_to, amount_gteq, amount_lteq)
    html     = _goto_retry(page_holder, make_page, f"{url_base}&page=1", log, f"{label} probe")
    if not html:
        return []

    true_total = _parse_true_total(html)
    if true_total == 0:
        return []

    if true_total <= MAX_ROWS:
        rows = _parse_transaction_page(html)
        total_pages = max((true_total + 19) // 20, 1)
        if total_pages > 1:
            with logging_redirect_tqdm(loggers=[log._log]):
                with tqdm(desc=f"  me txn {label}", unit="pg", total=total_pages,
                          initial=1, dynamic_ncols=True, leave=False) as bar:
                    for pg_num in range(2, total_pages + 1):
                        pg_html = _goto_retry(page_holder, make_page,
                                              f"{url_base}&page={pg_num}", log,
                                              f"{label} pg{pg_num}")
                        rows += _parse_transaction_page(pg_html)
                        bar.update(1)
        return rows

    if date_from < date_to:
        mid = date_from + (date_to - date_from) // 2
        log.warning(f"    [!] {label} ({true_total:,} true rows) over the "
                   f"{MAX_ROWS:,}-row cap — splitting at {mid}")
        left  = _fetch_window(page_holder, make_page, date_from, mid, log,
                              depth + 1, amount_gteq, amount_lteq)
        right = _fetch_window(page_holder, make_page, mid + timedelta(days=1), date_to, log,
                              depth + 1, amount_gteq, amount_lteq)
        return left + right

    if not (amount_gteq or amount_lteq):
        log.warning(f"    [!] {label} ({true_total:,} true rows) over the "
                   f"{MAX_ROWS:,}-row cap at 1-day resolution — splitting by amount")
        rows: list[dict] = []
        for _band_label, amin, amax in AMOUNT_BANDS_CENTS:
            rows += _fetch_window(page_holder, make_page, date_from, date_to, log,
                                  depth + 1, amin, amax)
        return rows

    # Already inside an amount band and still over the cap — no split axis
    # left. Accept the first MAX_ROWS and warn loudly rather than fail
    # silently; this has not been observed in practice.
    log.warning(f"    [!] {label} amount band [{amount_gteq or 0}-{amount_lteq or 'inf'}]c "
               f"({true_total:,} true rows) still over the {MAX_ROWS:,}-row cap with no "
               f"split axis left — accepting truncated data")
    rows = _parse_transaction_page(html)
    total_pages = max((MAX_ROWS + 19) // 20, 1)
    with logging_redirect_tqdm(loggers=[log._log]):
        with tqdm(desc=f"  me txn {label} (truncated)", unit="pg", total=total_pages,
                  initial=1, dynamic_ncols=True, leave=False) as bar:
            for pg_num in range(2, total_pages + 1):
                pg_html = _goto_retry(page_holder, make_page,
                                      f"{url_base}&page={pg_num}", log,
                                      f"{label} pg{pg_num}")
                rows += _parse_transaction_page(pg_html)
                bar.update(1)
    return rows


def scrape_transactions(make_page, log, force: bool = False,
                        start_year: int | None = None,
                        end_year: int | None = None) -> int:
    """make_page() -> (context, page): factory called once per year so the
    browser is restarted between years to avoid renderer OOM crashes."""
    """Fetch /public/activities for each year, all transaction types combined.

    Writes one CSV per year: me_transactions_{year}.csv
    Each row: filer_name, transaction_type, source_payee, date, amount,
              transaction_id (UUID from the row's href).

    Year filtering uses Ransack date predicates (q[date_gteq]/q[date_lteq]),
    delegated per-year to _fetch_window() (Jan 1 - Dec 31), which recursively
    bisects the window by date if the year's true match count is over
    MAX_ROWS — see that function's docstring and the module docstring's
    "Hard 10,000-row cap per query" note for why this is necessary.

    Incremental runs skip years already in the manifest (except current year,
    which is always re-fetched). Year range flags wipe the relevant manifest
    entries and re-download.
    """
    cy = datetime.today().year

    y_start = start_year or START_YEAR
    y_end   = end_year   or cy

    year_range_explicit = start_year is not None or end_year is not None

    if force:
        strip_manifest(lambda r: r["relation_type"] != "transactions")

    if year_range_explicit and not force:
        # Wipe manifest entries for in-range years
        def _outside_range(r: dict) -> bool:
            if r["relation_type"] != "transactions":
                return True   # keep non-transaction entries
            try:
                yr = int(r["key"])
            except ValueError:
                return True
            return yr < y_start or yr > y_end   # keep entries outside range
        strip_manifest(_outside_range)

    done       = load_manifest()
    total_rows = 0

    for year in range(y_start, y_end + 1):
        year_str = str(year)
        key      = ("transactions", year_str)
        out_path = RAW_DIR / f"me_transactions_{year}.csv"

        # Skip already-completed past years on incremental runs
        already_done = (key in done and year != cy and not year_range_explicit
                        and out_path.exists() and out_path.stat().st_size > 0)
        if already_done:
            log.file_download_skip(filename=out_path.name)
            manifest_count = next(
                (int(r["row_count"]) for r in _read_manifest_rows()
                 if r["relation_type"] == "transactions" and r["key"] == year_str),
                0,
            )
            total_rows += manifest_count
            continue

        log.file_download_start(filename=out_path.name)
        t_year = time.perf_counter()

        # Fresh browser context per year — avoids renderer OOM after ~80 min.
        # page_holder lets _fetch_window()'s recursive calls see (and, on a
        # renderer crash, replace) the same live context — see _goto_retry().
        page_holder = [make_page()]

        try:
            year_rows = _fetch_window(
                page_holder, make_page,
                date(year, 1, 1), date(year, 12, 31),
                log,
            )

            if not year_rows:
                log.file_download_skip(filename=out_path.name)
                continue

        finally:
            try:
                page_holder[0][0].close()
            except Exception:
                pass

        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=TRANSACTION_COLS)
            w.writeheader()
            w.writerows(year_rows)

        row_count   = len(year_rows)
        total_rows += row_count

        log.file_download_ok(filename=out_path.name,
                             bytes=out_path.stat().st_size,
                             rows=row_count,
                             duration_s=round(time.perf_counter() - t_year, 2))
        upsert_manifest({
            "relation_type": "transactions",
            "key":           year_str,
            "filename":      out_path.name,
            "row_count":     row_count,
        })

    return total_rows


# ====================== enrichment sweep ==============================

# Extracts contributor/payee detail fields from a /public/activities/{uuid} page.
# Uses explicit label matching to avoid key-normalisation ambiguity with labels
# that contain " / " (e.g. "State / Province", "Zip / Postal Code").
_ENRICHMENT_JS = """
() => {
    const get = label => {
        for (const dt of document.querySelectorAll('dt')) {
            if (dt.innerText.trim() === label) {
                const dd = dt.nextElementSibling;
                if (dd && dd.tagName === 'DD') {
                    const v = dd.innerText.trim();
                    return (v === 'Not Provided') ? '' : v;
                }
            }
        }
        return '';
    };
    return {
        contact_type:     get('Contact Type'),
        address_1:        get('Address 1'),
        address_2:        get('Address 2'),
        city:             get('City'),
        state_province:   get('State / Province'),
        zip_postal_code:  get('Zip / Postal Code'),
        country:          get('Country'),
        occupation:       get('Occupation'),
        occupation_other: get('Occupation Other'),
        employer:         get('Employer'),
        election:         get('Election'),
        description:      get('Description'),
    };
}
"""


def _load_done_enrichments() -> set[str]:
    """Return set of transaction_ids already written to me_enrichment.csv."""
    done: set[str] = set()
    if not ENRICHMENT_PATH.exists():
        return done
    with open(ENRICHMENT_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            tid = row.get("transaction_id", "").strip()
            if tid:
                done.add(tid)
    return done


def scrape_enrichment(make_page, log, force: bool = False) -> int:
    """Visit each /public/activities/{uuid} detail page and append enrichment fields.

    Writes to me_enrichment.csv in append mode — interrupted runs resume where
    they left off. Browser context is restarted every ENRICH_RESTART_EVERY pages
    to prevent renderer OOM over the multi-day run.

    Returns the number of transactions successfully enriched this run.
    """
    # Collect all transaction IDs from scraped CSV files
    all_ids: list[str] = []
    for path in sorted(RAW_DIR.glob("me_transactions_*.csv")):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                tid = row.get("transaction_id", "").strip()
                if tid and len(tid) > 30:
                    all_ids.append(tid)

    if not all_ids:
        log.warning("No transaction IDs found — run transactions scrape first")
        return 0

    done = set() if force else _load_done_enrichments()
    to_do = [tid for tid in all_ids if tid not in done]

    if not to_do:
        log.info(f"  all {len(all_ids):,} enrichments already done")
        return 0

    log.info(f"  Enriching {len(to_do):,} of {len(all_ids):,} transactions")

    # Append mode — safe to interrupt; existing rows are never re-written
    file_exists = ENRICHMENT_PATH.exists() and not force
    mode = "a" if file_exists else "w"

    ok = err = 0
    t0 = time.perf_counter()

    with open(ENRICHMENT_PATH, mode, newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=ENRICHMENT_COLS)
        if not file_exists:
            w.writeheader()

        ctx, pg = make_page()
        pages_this_ctx = 0

        try:
            with logging_redirect_tqdm(loggers=[log._log]):
                with tqdm(desc="  me enrich", unit="txn", total=len(to_do),
                          dynamic_ncols=True) as bar:
                    for tid in to_do:
                        if pages_this_ctx >= ENRICH_RESTART_EVERY:
                            try:
                                ctx.close()
                            except Exception:
                                pass
                            ctx, pg = make_page()
                            pages_this_ctx = 0

                        url = f"{BASE_URL}/public/activities/{tid}"
                        for attempt in range(2):
                            try:
                                pg.goto(url, timeout=30_000,
                                        wait_until="domcontentloaded")
                                try:
                                    pg.wait_for_selector("dt", timeout=10_000)
                                except Exception:
                                    time.sleep(random.uniform(1.0, 2.0))
                                time.sleep(random.uniform(*SLEEP))
                                raw = pg.evaluate(_ENRICHMENT_JS)
                                w.writerow({
                                    "transaction_id":   tid,
                                    "contact_type":     raw.get("contact_type", ""),
                                    "address_1":        raw.get("address_1", ""),
                                    "address_2":        raw.get("address_2", ""),
                                    "city":             raw.get("city", ""),
                                    "state_province":   raw.get("state_province", ""),
                                    "zip_postal_code":  raw.get("zip_postal_code", ""),
                                    "country":          raw.get("country", ""),
                                    "occupation":       raw.get("occupation", ""),
                                    "occupation_other": raw.get("occupation_other", ""),
                                    "employer":         raw.get("employer", ""),
                                    "election":         raw.get("election", ""),
                                    "description":      raw.get("description", ""),
                                })
                                fh.flush()
                                ok += 1
                                pages_this_ctx += 1
                                break
                            except Exception as e:
                                err_s = str(e).lower()
                                if attempt == 0 and (
                                    "crashed" in err_s or "closed" in err_s
                                    or "target" in err_s
                                ):
                                    try:
                                        ctx.close()
                                    except Exception:
                                        pass
                                    ctx, pg = make_page()
                                    pages_this_ctx = 0
                                else:
                                    log.warning(f"  enrich skip {tid[:8]}: {e!s:.60}")
                                    err += 1
                                    break
                        bar.update(1)
        finally:
            try:
                ctx.close()
            except Exception:
                pass

    log.page_scrape_complete(
        filename=str(ENRICHMENT_PATH),
        rows=ok,
        duration_s=round(time.perf_counter() - t0, 1),
        ok=ok, err=err,
    )
    return ok


# ================================ run =================================

def run(
    force: bool = False,
    entities: bool = False,
    transactions: bool = False,
    enrich: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
    contributions: bool = False,
    expenditures: bool = False,
    candidates: bool = False,
    committees: bool = False,
):
    log = get_logger("maine", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, enrich=enrich)

    # Resolve horizontal scope.
    # --enrich is opt-in only — never runs by default.
    do_enrich       = enrich
    do_entities     = entities or candidates or committees or not (
        transactions or contributions or expenditures or enrich)
    do_transactions = transactions or contributions or expenditures or not (
        entities or candidates or committees or enrich)

    files_ok  = 0
    files_err = 0

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=0,
                  error="playwright not installed — run: pip install playwright && playwright install chromium")
        raise RuntimeError(
            "Playwright not installed. Run: pip install playwright && playwright install chromium"
        )

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                timezone_id="America/New_York",
            )
            pw_page = context.new_page()

            # Patch navigator.webdriver and other bot-detection signals.
            def _apply_stealth(p):
                try:
                    from playwright_stealth import stealth_sync
                    stealth_sync(p)
                    return True
                except ImportError:
                    pass
                try:
                    from playwright_stealth import Stealth
                    Stealth().stealth_sync(p)
                    return True
                except Exception:
                    pass
                return False

            def _fresh_page():
                """Return a new context+page, with stealth applied and CF warm-up done."""
                ctx = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/125.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1440, "height": 900},
                    locale="en-US",
                    timezone_id="America/New_York",
                )
                pg = ctx.new_page()
                _apply_stealth(pg)
                pg.goto(f"{BASE_URL}/public", timeout=60_000,
                        wait_until="domcontentloaded")
                time.sleep(random.uniform(3.0, 5.0))
                return ctx, pg

            if not _apply_stealth(pw_page):
                log.warning("playwright-stealth unavailable — Cloudflare may block requests")

            # Warm-up: let Cloudflare run its JS challenge and set cookies.
            pw_page.goto(f"{BASE_URL}/public", timeout=60_000,
                         wait_until="domcontentloaded")
            time.sleep(random.uniform(3.0, 5.0))

            if do_entities:
                scrape_filer_list(pw_page, log, force=force)
                files_ok += 1

                scrape_filer_profiles(pw_page, log, force=force)
                files_ok += 1

            if do_transactions:
                # scrape_transactions calls make_page() once per year so each
                # year gets a fresh renderer — avoids OOM after ~80 min of pages.
                scrape_transactions(_fresh_page, log, force=force,
                                    start_year=start_year, end_year=end_year)
                files_ok += 1

            if do_enrich:
                # Each browser context lives for ENRICH_RESTART_EVERY pages (~2K).
                # Append mode — safe to kill and resume at any time.
                scrape_enrichment(_fresh_page, log, force=force)
                files_ok += 1

            browser.close()

        duration = round(time.perf_counter() - t0, 1)
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
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
        description="Download Maine campaign finance data."
    )

    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything, wipe manifest")
    vert.add_argument("--start-year", type=int, metavar="YYYY",
                      help="earliest year to download (inclusive)")

    ap.add_argument("--end-year",     type=int, metavar="YYYY",
                    help="latest year to download (inclusive, ≤ current year)")
    ap.add_argument("--transactions", action="store_true")
    ap.add_argument("--entities",     action="store_true")
    ap.add_argument("--enrich",       action="store_true",
                    help="visit each transaction detail page for contributor address/employer/occupation")
    ap.add_argument("--contributions",action="store_true")
    ap.add_argument("--expenditures", action="store_true")
    ap.add_argument("--candidates",   action="store_true")
    ap.add_argument("--committees",   action="store_true")

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
            enrich=args.enrich,
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
