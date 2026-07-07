"""
scrapers/minnesota.py — Download Minnesota campaign finance data.

Transactions: three bulk CSV downloads from the Campaign Finance Board (CFB)
self-help data page at cfb.mn.gov/reports-and-data/self-help/data-downloads/.
Single full-history files (2015–present), plain HTTP GET, no authentication.
"All" downloads contain every entity type (PCC/PCF/PTU) in one file — no
need to download the per-type breakdowns.

Entities (candidates, committees, party units): POST to the CFB viewer APIs:
  /reports-and-data/viewers/campaign-finance/candidates/api         (PCC)
  /reports-and-data/viewers/campaign-finance/political-committee-fund/api  (PCF)
  /reports-and-data/viewers/campaign-finance/party-unit/api         (PTU)

These endpoints require a valid PHPSESSID cookie (obtained by GETting the
viewer page) and a non-datacenter IP — the WAF blocks POST from cloud/VPS
ranges (same restriction as Florida). Run entity downloads locally:
  python3 src/pipeline/scrapers/minnesota.py --entities

Entity strategy: read the three transaction CSVs to collect every unique
reg_num by entity type, then POST id={regnum}&year={year}&tabname=information
for each one. Results are cached in JSON files keyed by reg_num so incremental
runs only fetch new entries. The API returns a rich `data` array with
candidate name, party, office, district, and a stable CandidateMasterNameID
(person-level ID that persists across election cycles).

Year logic: try current_year first; if the API returns empty data (entity
may have filed under an earlier cycle or is terminated), fall back to the
max year that reg_num appears in the transaction files.
"""

import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Minnesota" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Minnesota" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["relation_type", "filename", "downloaded_at", "row_count"]

# ============================ constants ==============================

BASE_URL     = "https://cfb.mn.gov"
CURRENT_YEAR = datetime.today().year

# Bulk transaction download IDs (query param ?download=<id>)
TRANSACTION_DOWNLOADS = {
    "contributions":    ("-2113865252", "mn_contributions.csv"),
    "expenditures":     ("-1890073264", "mn_expenditures.csv"),
    "ind_expenditures": ("-617535497",  "mn_ind_expenditures.csv"),
}

# Viewer API paths and output files, keyed by entity type code
ENTITY_VIEWER = {
    "PCC": {
        "api":      "/reports-and-data/viewers/campaign-finance/candidates/api",
        "page":     "/reports-and-data/viewers/campaign-finance/candidates/",
        "outfile":  "candidates_entity.json",
    },
    "PCF": {
        "api":      "/reports-and-data/viewers/campaign-finance/political-committee-fund/api",
        "page":     "/reports-and-data/viewers/campaign-finance/political-committee-fund/",
        "outfile":  "committees_entity.json",
    },
    "PTU": {
        "api":      "/reports-and-data/viewers/campaign-finance/party-unit/api",
        "page":     "/reports-and-data/viewers/campaign-finance/party-unit/",
        "outfile":  "party_units_entity.json",
    },
}

# ========================= manifest helpers ==========================

def load_manifest() -> dict[str, dict]:
    """Return {relation_type: row} for already-downloaded items."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="") as f:
        return {r["relation_type"]: r for r in csv.DictReader(f)}


def upsert_manifest(record: dict):
    """Write or overwrite a single manifest row keyed by relation_type."""
    rows = {}
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            rows = {r["relation_type"]: r for r in csv.DictReader(f)}
    rows[record["relation_type"]] = record
    with open(MANIFEST, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(rows.values())


def clear_manifest(relation_types: list[str] | None = None):
    """Remove manifest entries for the given relation_types (or all if None)."""
    if not MANIFEST.exists():
        return
    with open(MANIFEST, newline="") as f:
        rows = list(csv.DictReader(f))
    if relation_types is not None:
        rows = [r for r in rows if r["relation_type"] not in relation_types]
    else:
        rows = []
    with open(MANIFEST, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(rows)


# ========================= session helpers ==========================

def _make_session(page_path: str) -> requests.Session:
    """
    Create a requests Session with PHPSESSID from the viewer page.
    The session cookie is required for POST requests to the viewer APIs.
    NOTE: POST requests are WAF-blocked from datacenter IPs — run locally.
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/147.0.0.0 Safari/537.36",
        "Accept":          "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":          BASE_URL,
        "sec-fetch-site":  "same-origin",
        "sec-fetch-mode":  "cors",
        "sec-fetch-dest":  "empty",
    })
    url = BASE_URL + page_path
    resp = s.get(url, timeout=30)
    resp.raise_for_status()
    return s


def _post_body(reg_num: str, year: int) -> dict:
    """
    Build POST body for a viewer entity detail lookup by reg_num + year.
    MN biennial cycle: year 2026 → segment 2025–2026.
    """
    return {
        "id":                               reg_num,
        "year":                             str(year),
        "year_data[ElectionSegmentEndDate]":   str(year),
        "year_data[ElectionSegmentStartDate]": str(year - 1),
        "tabname":                          "information",
    }


# ========================= transaction downloads =====================

def download_transactions(log, session: requests.Session,
                          do_contributions: bool,
                          do_expenditures: bool,
                          do_ind_expenditures: bool) -> tuple[int, int]:
    """Download the three bulk transaction CSVs. Returns (files_ok, files_err)."""
    scope = {
        "contributions":    do_contributions,
        "expenditures":     do_expenditures,
        "ind_expenditures": do_ind_expenditures,
    }

    files_ok = files_err = 0

    for relation_type, (download_id, filename) in TRANSACTION_DOWNLOADS.items():
        if not scope[relation_type]:
            continue

        out_path = RAW_DIR / filename

        # Transaction files are full-history; re-fetch every run for freshness.
        # Skip only when manifest says already done AND not forcing.
        # Single full-history files are always re-fetched so new filings are picked up.
        # --force only matters for clearing the entity JSON caches; skip logic here
        # is intentionally absent for transactions.

        url = (f"{BASE_URL}/reports-and-data/self-help/data-downloads/"
               f"campaign-finance?download={download_id}")

        log.file_download_start(filename=filename)
        t_file = time.perf_counter()
        try:
            resp = session.get(url, timeout=180, stream=True)
            resp.raise_for_status()
            with open(out_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)

            row_count = max(sum(1 for _ in open(out_path, encoding="utf-8",
                                                errors="replace")) - 1, 0)
            log.file_download_ok(
                filename=filename,
                bytes=out_path.stat().st_size,
                rows=row_count,
                duration_s=round(time.perf_counter() - t_file, 2),
            )
            upsert_manifest({
                "relation_type": relation_type,
                "filename":      filename,
                "downloaded_at": datetime.today().strftime("%Y-%m-%d"),
                "row_count":     row_count,
            })
            files_ok += 1

        except Exception as e:
            log.file_download_error(filename=filename, error=str(e))
            files_err += 1

    return files_ok, files_err


# ========================= entity collection ========================

def _collect_reg_nums() -> dict[str, dict[str, int]]:
    """
    Read the three transaction CSVs and return unique reg_nums by entity type.
    Returns {entity_type: {reg_num: max_year_seen}}.
    """
    entities: dict[str, dict[str, int]] = {"PCC": {}, "PCF": {}, "PTU": {}}

    def _update(etype: str, reg: str, year_str: str):
        reg = reg.strip()
        if not reg or etype not in entities:
            return
        yr = int(year_str) if year_str.strip().isdigit() else 0
        if yr:
            entities[etype][reg] = max(entities[etype].get(reg, 0), yr)

    cont_file = RAW_DIR / "mn_contributions.csv"
    if cont_file.exists():
        with open(cont_file, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                _update(row.get("Recipient type", ""),
                        row.get("Recipient reg num", ""),
                        row.get("Year", ""))
        # Also register contributors that are themselves filers
        with open(cont_file, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                creg  = row.get("Contrib Reg Num", "").strip()
                ctype = row.get("Contrib type", "").strip()
                yr    = row.get("Year", "").strip()
                # Map contrib type strings to our entity type codes
                etype_map = {
                    "Political Committee/Fund": "PCF",
                    "Party Unit":              "PTU",
                    "Candidate Committee":     "PCC",
                }
                etype = etype_map.get(ctype)
                if etype and creg:
                    _update(etype, creg, yr)

    exp_file = RAW_DIR / "mn_expenditures.csv"
    if exp_file.exists():
        with open(exp_file, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                _update(row.get("Entity type", ""),
                        row.get("Committee reg num", ""),
                        row.get("Year", ""))

    ie_file = RAW_DIR / "mn_ind_expenditures.csv"
    if ie_file.exists():
        with open(ie_file, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                _update(row.get("Spender type", ""),
                        row.get("Spender Reg Num", ""),
                        row.get("Year", ""))

    return entities


def download_entity_type(log, entity_type: str, reg_nums: dict[str, int],
                         session: requests.Session, force: bool) -> int:
    """
    Fetch entity details for every reg_num of the given type and write to JSON.
    Returns count of successfully fetched entities.
    """
    cfg      = ENTITY_VIEWER[entity_type]
    out_file = RAW_DIR / cfg["outfile"]

    # Load cached results (keyed by reg_num)
    cache: dict[str, dict] = {}
    if not force and out_file.exists():
        with open(out_file, encoding="utf-8") as f:
            cache = json.load(f)

    to_fetch = {r: y for r, y in reg_nums.items() if r not in cache}
    if not to_fetch:
        log.info(f"  {entity_type}: {len(cache)} cached, nothing new to fetch")
        return len(cache)

    log.info(f"  {entity_type}: fetching {len(to_fetch)} new, {len(cache)} cached")

    api_url  = BASE_URL + cfg["api"]
    ok = err = 0

    with logging_redirect_tqdm(loggers=[log._log]):
        with tqdm(total=len(to_fetch), desc=f"  {entity_type}", unit="entity",
                  dynamic_ncols=True) as bar:
            for reg_num, max_year in to_fetch.items():

                referer_year = min(max_year, CURRENT_YEAR)
                referer      = (f"{BASE_URL}{cfg['page']}"
                                f"{reg_num}/{referer_year}/")

                def _post(year: int) -> list:
                    r = session.post(
                        api_url,
                        data=_post_body(reg_num, year),
                        headers={"Referer": f"{BASE_URL}{cfg['page']}"
                                            f"{reg_num}/{year}/"},
                        timeout=30,
                    )
                    r.raise_for_status()
                    return r.json().get("data") or []

                try:
                    data = _post(CURRENT_YEAR)
                    # Fall back to max transaction year if entity not found at current year
                    if not data and max_year != CURRENT_YEAR:
                        data = _post(max_year)
                    cache[reg_num] = data[0] if data else {}
                    ok += 1
                except Exception as e:
                    # 403 usually means datacenter IP — warn once then continue
                    if hasattr(e, "response") and e.response is not None \
                            and e.response.status_code == 403:
                        log.warning(
                            "Entity API returned 403 — POST requests are WAF-blocked "
                            "from datacenter IPs. Run --entities from your local machine."
                        )
                    log.page_scrape_error(entity=entity_type, page_id=reg_num,
                                          error=str(e))
                    cache[reg_num] = {}
                    err += 1

                bar.update(1)
                time.sleep(0.2)

    # Persist updated cache
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    log.page_scrape_complete(
        filename=str(out_file),
        rows=ok,
        duration_s=0,
        ok=ok, err=err,
    )
    return ok


# ============================== run =================================

def run(
    force: bool = False,
    entities: bool = False,
    transactions: bool = False,
    contributions: bool = False,
    expenditures: bool = False,
    candidates: bool = False,
    committees: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
):
    """
    Orchestrate Minnesota transaction and entity downloads.

    Horizontal scope (default = all):
        transactions          contributions + expenditures + IE
        entities              candidates + committees + party units
        contributions         contributions only
        expenditures          expenditures + IE only
        candidates            PCC entity details only
        committees            PCF entity details only
        (party units always included with --entities or bare run)

    Vertical scope: year flags are not used (single full-history files).

    NOTE: entity downloads (--entities) require a non-datacenter IP. The CFB
    viewer API POST is WAF-blocked from cloud/VPS addresses. Run locally.
    """
    log = get_logger("minnesota", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions)

    # ── Resolve scope ─────────────────────────────────────────────────
    no_h = not (entities or transactions or contributions or
                expenditures or candidates or committees)

    do_transactions    = no_h or transactions or contributions or expenditures
    do_entities        = no_h or entities or candidates or committees
    do_contributions   = no_h or transactions or contributions
    do_expenditures    = no_h or transactions or expenditures
    do_ie              = do_expenditures      # IE always bundled with expenditures
    do_candidates      = no_h or entities or candidates
    do_committees      = no_h or entities or committees
    do_party_units     = no_h or entities     # PTU always with general --entities

    files_ok = files_err = 0

    try:
        # Plain session for transaction GETs (no PHPSESSID needed)
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/147.0.0.0 Safari/537.36",
        })

        # ── Transactions ──────────────────────────────────────────────
        if do_transactions:
            ok, err = download_transactions(
                log, session,
                do_contributions=do_contributions,
                do_expenditures=do_expenditures,
                do_ind_expenditures=do_ie,
            )
            files_ok  += ok
            files_err += err

        # ── Entities ──────────────────────────────────────────────────
        if do_entities:
            # Collect reg_nums from transaction files (must be on disk)
            missing = [name for name in ("mn_contributions.csv",
                                         "mn_expenditures.csv",
                                         "mn_ind_expenditures.csv")
                       if not (RAW_DIR / name).exists()]
            if missing:
                log.warning(
                    f"Entity download requires transaction files to be downloaded "
                    f"first. Missing: {', '.join(missing)}. "
                    f"Run without --entities first to download transactions."
                )
            else:
                log.info("Collecting unique reg_nums from transaction files …")
                all_entities = _collect_reg_nums()
                log.info(
                    f"  PCC: {len(all_entities['PCC']):,}  "
                    f"PCF: {len(all_entities['PCF']):,}  "
                    f"PTU: {len(all_entities['PTU']):,}"
                )

                # Get a session cookie from the candidates viewer page
                try:
                    entity_session = _make_session(
                        ENTITY_VIEWER["PCC"]["page"]
                    )
                except Exception as e:
                    log.warning(f"Could not get viewer session: {e}")
                    entity_session = session   # fall back, will likely 403

                scope_map = {
                    "PCC": do_candidates,
                    "PCF": do_committees,
                    "PTU": do_party_units,
                }
                for etype, should_run in scope_map.items():
                    if not should_run:
                        continue
                    count = download_entity_type(
                        log, etype, all_entities[etype],
                        entity_session, force=force,
                    )
                    files_ok += 1
                    log.info(f"  {etype}: {count:,} entities cached")

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s — {files_ok} ok, {files_err} err")
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
        description="Download Minnesota campaign finance data from the CFB."
    )

    # Vertical scope — year flags not applicable for MN (single full-history files)
    vert = ap.add_mutually_exclusive_group()
    vert.add_argument("--force",      action="store_true",
                      help="re-download everything in scope, clear cache")

    ap.add_argument("--transactions", action="store_true",
                    help="transaction files only (contributions, expenditures, IE)")
    ap.add_argument("--entities",     action="store_true",
                    help="entity details only — requires local IP (WAF blocks datacenter POST)")
    ap.add_argument("--contributions", action="store_true",
                    help="contributions CSV only")
    ap.add_argument("--expenditures",  action="store_true",
                    help="expenditures + independent expenditures only")
    ap.add_argument("--candidates",    action="store_true",
                    help="PCC (candidate committee) entity details only")
    ap.add_argument("--committees",    action="store_true",
                    help="PCF (political committee/fund) entity details only")

    args, _ = ap.parse_known_args()

    try:
        run(
            force=args.force,
            entities=args.entities,
            transactions=args.transactions,
            contributions=args.contributions,
            expenditures=args.expenditures,
            candidates=args.candidates,
            committees=args.committees,
        )
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
