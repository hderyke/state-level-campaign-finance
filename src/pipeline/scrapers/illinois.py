"""
scrapers/illinois.py — Download Illinois campaign finance bulk data files.

Source: the Illinois State Board of Elections (ISBE) publishes its entire
campaign disclosure database as flat, tab-delimited (latin-1, QUOTE_NONE)
files at:

    https://elections.il.gov/campaigndisclosuredatafiles/{Table}.txt

Each file is the FULL 1994-present history for that table, updated nightly —
there is no year-splitting and no API key. This makes the search pages
(ContributionSearchByAllContributions.aspx, ExpenditureSearchByAllExpenditures.aspx,
CommitteeSearch.aspx) and their 5,000-row caps unnecessary: Receipts.txt and
Expenditures.txt already contain every transaction, and Committees.txt /
Candidates.txt / CmteCandidateLinks.txt give us the full entity registry keyed
by simple sequential integer IDs — no need to crawl per-committee/candidate
detail pages with obfuscated query-string IDs.

Core tables (id_model="committee" — CmteCandidateLinks.txt links CommitteeID
to CandidateID):
    Committees.txt          (~9 MB)    — committee registry
    Candidates.txt          (~3 MB)    — candidate registry
    CmteCandidateLinks.txt  (small)    — committee <-> candidate link table
    Receipts.txt            (~1.0 GB)  — every contribution since 1994
    Expenditures.txt        (~0.8 GB)  — every expenditure since 1994

FiledDocs.txt (~140 MB, filing-level metadata: election year, amended flag,
report dates) is deliberately excluded. Receipts/Expenditures carry their own
FiledDocID, which is written through as the canonical schema's `filing_id` —
no join needed. election_year/amended are optional enrichment fields and are
left blank for IL rather than pulling in a 140 MB file for them.

Strategy (same horizontal approach as california.py — single full-history
files, no year splitting): HEAD each file for Content-Length / Last-Modified.
If the manifest's server_last_modified matches and no partial download is
in progress, skip. Otherwise stream the GET in 8 MB chunks to a `<name>.part`
file. Because Receipts.txt / Expenditures.txt / FiledDocs.txt are large
enough that a single download can exceed a sandboxed run's time limit, each
call to run() only downloads for up to `time_budget_s` seconds per file; if
the budget is hit mid-file, progress (bytes + row count so far) is saved to
`<name>.progress.json` and the next invocation resumes via an HTTP Range
request starting at that byte offset. Re-run the scraper repeatedly until
every file reports "ok" (no remaining `.part` files).

Row counts are derived by counting newline bytes while streaming — exact for
these files since IDS/quoting (QUOTE_NONE) means no embedded newlines within
a field.
"""

import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Make project root and src/pipeline importable before importing local modules
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger

# =============================== paths ================================
RAW_DIR  = PROJECT_ROOT / "data" / "Illinois" / "raw"
MANIFEST = PROJECT_ROOT / "data" / "Illinois" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_COLS = ["filename", "server_last_modified", "server_size",
                 "downloaded_at", "row_count"]

# ========================= state-specific constants ===================
BASE_URL = "https://elections.il.gov/campaigndisclosuredatafiles/{table}.txt"
CHUNK    = 8 * 1024 * 1024  # 8 MB per streamed read

# table name (no extension) -> local filename
ENTITY_TABLES = {
    "Committees":         "Committees.txt",
    "Candidates":         "Candidates.txt",
    "CmteCandidateLinks": "CmteCandidateLinks.txt",
}
TRANSACTION_TABLES = {
    "Receipts":     "Receipts.txt",
    "Expenditures": "Expenditures.txt",
}
ALL_TABLES = {**ENTITY_TABLES, **TRANSACTION_TABLES}


# ============================ Manifest helpers ============================
def load_manifest() -> dict[str, dict]:
    """Return {filename: {server_last_modified, server_size, row_count, ...}}."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="") as f:
        return {row["filename"]: row for row in csv.DictReader(f)}


def update_manifest(filename: str, record: dict):
    """Replace an existing manifest row for `filename` (or append if missing)."""
    rows = []
    if MANIFEST.exists():
        with open(MANIFEST, newline="") as f:
            rows = list(csv.DictReader(f))

    updated = False
    for row in rows:
        if row["filename"] == filename:
            row.update(record)
            updated = True
            break
    if not updated:
        rows.append(record)

    with open(MANIFEST, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        writer.writeheader()
        writer.writerows(rows)


# ============================ Download helpers =============================
def check_file(session: requests.Session, table: str) -> tuple[int, str, str]:
    """HEAD the table's URL. Returns (content_length, last_modified, url)."""
    url  = BASE_URL.format(table=table)
    resp = session.head(url, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    size = int(resp.headers.get("Content-Length", 0))
    lm   = resp.headers.get("Last-Modified", "")
    return size, lm, url


def download_file(session: requests.Session, local_name: str, url: str,
                   server_size: int, server_lm: str,
                   time_budget_s: float) -> tuple[str, int, int]:
    """
    Stream-download `url` to RAW_DIR/<local_name>, resuming from a previous
    `.part` if one exists and matches `server_lm`.

    Returns (status, row_count, bytes_written):
        status = "ok"      — download complete, file renamed into place,
                              row_count = data rows (header excluded)
        status = "partial" — time budget hit; .part + progress saved,
                              row_count/bytes_written are running totals
    """
    out_path  = RAW_DIR / local_name
    part_path = RAW_DIR / f"{local_name}.part"
    prog_path = RAW_DIR / f"{local_name}.progress.json"

    start_byte = 0
    rows       = 0

    if part_path.exists() and prog_path.exists():
        try:
            prev = json.loads(prog_path.read_text())
            if prev.get("last_modified") == server_lm and \
               prev.get("bytes") == part_path.stat().st_size:
                start_byte = prev["bytes"]
                rows       = prev["rows"]
            else:
                part_path.unlink(missing_ok=True)
                prog_path.unlink(missing_ok=True)
        except Exception:
            part_path.unlink(missing_ok=True)
            prog_path.unlink(missing_ok=True)
    elif part_path.exists():
        # .part with no progress sidecar — can't trust it, restart
        part_path.unlink(missing_ok=True)

    # Already fully downloaded (e.g. budget hit right after last chunk
    # but before rename) — finalize without another request.
    if server_size and start_byte >= server_size and part_path.exists():
        prog_path.unlink(missing_ok=True)
        part_path.replace(out_path)
        return "ok", max(rows - 1, 0), start_byte

    headers = {"Range": f"bytes={start_byte}-"} if start_byte else {}
    mode    = "ab" if start_byte else "wb"

    resp = session.get(url, headers=headers, stream=True, timeout=120)
    resp.raise_for_status()

    # Server ignored our Range header (returned 200 instead of 206) —
    # the existing partial bytes don't correspond to this response, restart.
    if start_byte and resp.status_code == 200:
        start_byte = 0
        rows       = 0
        mode       = "wb"

    t0 = time.perf_counter()
    with open(part_path, mode) as f:
        for chunk in resp.iter_content(chunk_size=CHUNK):
            if not chunk:
                continue
            f.write(chunk)
            start_byte += len(chunk)
            rows       += chunk.count(b"\n")

            if time.perf_counter() - t0 > time_budget_s:
                f.flush()
                prog_path.write_text(json.dumps({
                    "bytes": start_byte, "rows": rows,
                    "last_modified": server_lm,
                }))
                return "partial", rows, start_byte

    prog_path.unlink(missing_ok=True)
    part_path.replace(out_path)
    return "ok", max(rows - 1, 0), start_byte


# ============================ Main runner ===============================
def run(force: bool = False, entities: bool = False, transactions: bool = False,
        time_budget_s: float = 35.0):
    """Download Illinois ISBE campaign disclosure bulk data files.

    Horizontal scope:
        No flags     — all 5 core tables
        entities     — Committees, Candidates, CmteCandidateLinks
        transactions — Receipts, Expenditures

    `force` clears any in-progress `.part`/`.progress.json` for the selected
    tables and ignores the manifest, forcing a full re-download.

    Large files (Receipts, Expenditures) may not finish within
    `time_budget_s` seconds — re-run the scraper to resume. A file reporting
    "partial" leaves a `.part` on disk; "ok" means it's complete and was
    renamed into place.
    """
    log = get_logger("illinois", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities,
              transactions=transactions, time_budget_s=time_budget_s)

    no_horizontal = not (entities or transactions)
    targets: dict[str, str] = {}
    if no_horizontal or entities:
        targets.update(ENTITY_TABLES)
    if no_horizontal or transactions:
        targets.update(TRANSACTION_TABLES)

    files_ok = files_err = files_partial = 0

    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36",
            # Cloudflare gzip-compresses the response by default, which omits
            # Content-Length and can break Range-based resume. Request the
            # identity encoding so HEAD gives a true size and GET honors Range.
            "Accept-Encoding": "identity",
        })

        done = {} if force else load_manifest()

        for table, local_name in targets.items():
            if force:
                (RAW_DIR / f"{local_name}.part").unlink(missing_ok=True)
                (RAW_DIR / f"{local_name}.progress.json").unlink(missing_ok=True)

            try:
                server_size, server_lm, url = check_file(session, table)
            except Exception as e:
                log.file_download_error(filename=local_name, error=f"HEAD failed: {e}")
                files_err += 1
                continue

            part_exists = (RAW_DIR / f"{local_name}.part").exists()
            manifest_row = done.get(local_name)
            if not force and not part_exists and manifest_row \
               and manifest_row.get("server_last_modified") == server_lm:
                log.file_download_skip(filename=local_name)
                continue

            log.file_download_start(filename=local_name)
            t_file = time.perf_counter()

            try:
                status, row_count, bytes_written = download_file(
                    session, local_name, url, server_size, server_lm, time_budget_s)
            except Exception as e:
                log.file_download_error(filename=local_name, error=str(e))
                files_err += 1
                continue

            if status == "ok":
                log.file_download_ok(filename=local_name, bytes=bytes_written,
                                     rows=row_count,
                                     duration_s=round(time.perf_counter() - t_file, 2))
                update_manifest(local_name, {
                    "filename":             local_name,
                    "server_last_modified": server_lm,
                    "server_size":          server_size,
                    "downloaded_at":        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "row_count":            row_count,
                })
                files_ok += 1
            else:
                pct = (bytes_written / server_size * 100) if server_size else 0
                log.info(f"  ... {local_name}: {bytes_written/1024/1024:,.1f} MB "
                         f"/ {server_size/1024/1024:,.1f} MB ({pct:.1f}%) — "
                         f"re-run to continue")
                files_partial += 1

        duration = round(time.perf_counter() - t0, 1)
        log._emit("scrape_completed", status="completed", duration_s=duration,
                  files_ok=files_ok, files_err=files_err, files_partial=files_partial)

    except KeyboardInterrupt:
        log._emit("scrape_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err, files_partial=files_partial)
        raise

    except Exception as e:
        log._emit("scrape_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err, files_partial=files_partial,
                  error_type=type(e).__name__, error=str(e))
        raise


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Download Illinois ISBE campaign disclosure bulk data files."
    )
    ap.add_argument("--force",        action="store_true",
                    help="re-download everything, ignoring the manifest")
    ap.add_argument("--entities",     action="store_true",
                    help="entity tables only (Committees, Candidates, CmteCandidateLinks)")
    ap.add_argument("--transactions", action="store_true",
                    help="transaction tables only (Receipts, Expenditures)")
    ap.add_argument("--time-budget",  type=float, default=35.0,
                    help="seconds to spend downloading each file before saving "
                         "progress and returning (default 35s); re-run to resume "
                         "large files")
    args, _ = ap.parse_known_args()
    try:
        run(force=args.force, entities=args.entities, transactions=args.transactions,
            time_budget_s=args.time_budget)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
