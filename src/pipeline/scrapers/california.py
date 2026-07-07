"""
scrapers/california.py — Download California CAL-ACCESS campaign finance tables.

Source: CAL-ACCESS bulk export ZIP, updated daily by the CA Secretary of State.
URL:    https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip

Strategy: selective extraction via HTTP Range requests — we read the ZIP's
central directory, then pull only the compressed bytes for the tables we care
about, decompress in memory, and write as UTF-8 TSVs.  Never downloads the
full 1.5 GB ZIP.

Target tables
=============
  RCPT_CD.TSV                     → contributions received
  EXPN_CD.TSV                     → expenditures made
  FILERNAME_CD.TSV                → committee / filer names
  CVR_CAMPAIGN_DISCLOSURE_CD.TSV  → campaign disclosure cover records
  FILER_TO_FILER_TYPE_CD.TSV      → filer-type mapping
"""

import csv
import struct
import sys
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

import requests

# == Paths =====================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
from src.reporting.logger import get_logger
RAW_DIR      = PROJECT_ROOT / "data" / "California" / "raw"
MANIFEST     = PROJECT_ROOT / "data" / "California" / "manifest.csv"

RAW_DIR.mkdir(parents=True, exist_ok=True)

ZIP_URL       = "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip"
MANIFEST_COLS = ["filename", "server_last_modified", "downloaded_at", "row_count"]

# Tables to extract: ZIP path → local filename
TARGET_TABLES = {
    "CalAccess/DATA/RCPT_CD.TSV":                    "RCPT_CD.tsv",
    "CalAccess/DATA/EXPN_CD.TSV":                    "EXPN_CD.tsv",
    "CalAccess/DATA/FILERNAME_CD.TSV":               "FILERNAME_CD.tsv",
    "CalAccess/DATA/CVR_CAMPAIGN_DISCLOSURE_CD.TSV": "CVR_CAMPAIGN_DISCLOSURE_CD.tsv",
    "CalAccess/DATA/FILER_TO_FILER_TYPE_CD.TSV":     "FILER_TO_FILER_TYPE_CD.tsv",
}

# Transactions change daily; entity/registry files change less often.
CONTRIBUTION_TABLES  = {"RCPT_CD.tsv"}
EXPENDITURE_TABLES   = {"EXPN_CD.tsv"}
TRANSACTION_TABLES   = CONTRIBUTION_TABLES | EXPENDITURE_TABLES
ENTITY_TABLES        = {"FILERNAME_CD.tsv", "CVR_CAMPAIGN_DISCLOSURE_CD.tsv",
                        "FILER_TO_FILER_TYPE_CD.tsv"}


# == Manifest helpers ==========================================================
def load_manifest() -> dict[str, str]:
    """Return {filename: server_last_modified} for already-downloaded files."""
    if not MANIFEST.exists():
        return {}
    with open(MANIFEST, newline="") as f:
        return {row["filename"]: row["server_last_modified"]
                for row in csv.DictReader(f)}


def append_manifest(record: dict):
    write_header = not MANIFEST.exists()
    with open(MANIFEST, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


def update_manifest(filename: str, record: dict):
    """Replace an existing manifest row (or append if missing)."""
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


# == ZIP central directory parsing =============================================
def fetch_bytes(session: requests.Session, start: int, end: int) -> bytes:
    """Fetch a byte range from the ZIP_URL (inclusive on both ends)."""
    resp = session.get(ZIP_URL, headers={"Range": f"bytes={start}-{end}"}, timeout=120)
    resp.raise_for_status()
    return resp.content


def check_zip(session: requests.Session) -> tuple[int, str]:
    """
    HEAD the ZIP URL; return (total_size_bytes, server_last_modified_YYYY-MM-DD).
    """
    resp = session.head(ZIP_URL, timeout=30)
    resp.raise_for_status()
    size = int(resp.headers["Content-Length"])
    lm   = resp.headers.get("Last-Modified", "")
    if lm:
        try:
            date_str = datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
        except ValueError:
            date_str = lm
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return size, date_str


def read_central_directory(session: requests.Session, zip_size: int) -> dict:
    """
    Parse the ZIP central directory and return a dict of
    {zip_path: {comp_size, uncomp_size, lh_offset, method}}.

    Handles both ZIP32 and ZIP64 End-of-Central-Directory records.
    """
    # Fetch enough tail bytes to find the EOCD record (max 64 KB)
    tail_size = min(65536, zip_size)
    tail = fetch_bytes(session, zip_size - tail_size, zip_size - 1)

    # Locate End of Central Directory signature
    eocd_pos = tail.rfind(b"PK\x05\x06")
    if eocd_pos < 0:
        raise ValueError("EOCD signature not found — not a valid ZIP?")

    eocd = tail[eocd_pos:]
    cd_size   = struct.unpack_from("<I", eocd, 12)[0]
    cd_offset = struct.unpack_from("<I", eocd, 16)[0]

    # Check for ZIP64 EOCD locator immediately before EOCD
    z64_locator_pos = eocd_pos - 20
    if z64_locator_pos >= 0 and tail[z64_locator_pos:z64_locator_pos+4] == b"PK\x06\x07":
        z64_eocd_offset = struct.unpack_from("<Q", tail, z64_locator_pos + 8)[0]
        z64_tail = fetch_bytes(session, z64_eocd_offset, z64_eocd_offset + 55)
        if z64_tail[:4] == b"PK\x06\x06":
            cd_size   = struct.unpack_from("<Q", z64_tail, 40)[0]
            cd_offset = struct.unpack_from("<Q", z64_tail, 48)[0]

    # Read the central directory
    cd_data = fetch_bytes(session, cd_offset, cd_offset + cd_size - 1)

    entries = {}
    pos = 0
    while pos < len(cd_data):
        if cd_data[pos:pos+4] != b"PK\x01\x02":
            break

        method       = struct.unpack_from("<H", cd_data, pos + 10)[0]
        comp_size    = struct.unpack_from("<I", cd_data, pos + 20)[0]
        uncomp_size  = struct.unpack_from("<I", cd_data, pos + 24)[0]
        fname_len    = struct.unpack_from("<H", cd_data, pos + 28)[0]
        extra_len    = struct.unpack_from("<H", cd_data, pos + 30)[0]
        comment_len  = struct.unpack_from("<H", cd_data, pos + 32)[0]
        lh_offset    = struct.unpack_from("<I", cd_data, pos + 42)[0]
        fname        = cd_data[pos+46:pos+46+fname_len].decode("utf-8", errors="replace")

        # Parse ZIP64 extra fields if any sizes are 0xFFFFFFFF
        extra = cd_data[pos+46+fname_len : pos+46+fname_len+extra_len]
        if comp_size == 0xFFFFFFFF or uncomp_size == 0xFFFFFFFF or lh_offset == 0xFFFFFFFF:
            xpos = 0
            while xpos + 4 <= len(extra):
                tag  = struct.unpack_from("<H", extra, xpos)[0]
                size = struct.unpack_from("<H", extra, xpos + 2)[0]
                if tag == 0x0001:  # ZIP64 extended information
                    vals = []
                    vpos = xpos + 4
                    if uncomp_size == 0xFFFFFFFF and vpos + 8 <= len(extra):
                        uncomp_size = struct.unpack_from("<Q", extra, vpos)[0]; vpos += 8; vals.append("uncomp")
                    if comp_size == 0xFFFFFFFF and vpos + 8 <= len(extra):
                        comp_size   = struct.unpack_from("<Q", extra, vpos)[0]; vpos += 8; vals.append("comp")
                    if lh_offset == 0xFFFFFFFF and vpos + 8 <= len(extra):
                        lh_offset   = struct.unpack_from("<Q", extra, vpos)[0]; vpos += 8; vals.append("offset")
                    break
                xpos += 4 + size

        entries[fname] = {
            "method":      method,
            "comp_size":   comp_size,
            "uncomp_size": uncomp_size,
            "lh_offset":   lh_offset,
        }

        pos += 46 + fname_len + extra_len + comment_len

    return entries


# == Selective extraction ======================================================
def extract_entry(session: requests.Session, entry: dict, zip_path: str,
                  out_path: Path, log=None) -> int:
    """
    Range-download + decompress one ZIP entry.
    Returns the number of data rows (lines minus 1).

    Strategy:
      1. Fetch the local file header (LFH) to get the precise data start offset
         and the actual compressed size (LFH may differ slightly from CD).
      2. Stream the compressed bytes in chunks, decompressing with zlib as we go.
         The decompressor naturally stops at the end of the deflate stream, so
         fetching a few extra bytes (buffer) is safe.
    """
    lh_offset  = entry["lh_offset"]
    cd_comp    = entry["comp_size"]   # from central directory — used as fallback
    method     = entry["method"]

    # == Step 1: read local file header ========================================
    lh_head = fetch_bytes(session, lh_offset, lh_offset + 1023)
    if lh_head[:4] != b"PK\x03\x04":
        raise ValueError(f"Bad local header signature for {zip_path}")

    lh_flags       = struct.unpack_from("<H", lh_head, 6)[0]
    lh_fname_len   = struct.unpack_from("<H", lh_head, 26)[0]
    lh_extra_len   = struct.unpack_from("<H", lh_head, 28)[0]
    lh_comp_size32 = struct.unpack_from("<I", lh_head, 18)[0]

    data_start = lh_offset + 30 + lh_fname_len + lh_extra_len

    # Resolve actual compressed size: prefer LH value; fall back to CD.
    # If bit 3 of flags is set the LH carries 0 and sizes follow in a data
    # descriptor — use CD's comp_size in that case.
    use_data_descriptor = bool(lh_flags & 0x0008)
    if lh_comp_size32 == 0xFFFFFFFF:
        # ZIP64 extra field in LH
        lh_extra = lh_head[30 + lh_fname_len: 30 + lh_fname_len + lh_extra_len]
        xpos = 0
        while xpos + 4 <= len(lh_extra):
            tag  = struct.unpack_from("<H", lh_extra, xpos)[0]
            esz  = struct.unpack_from("<H", lh_extra, xpos + 2)[0]
            if tag == 0x0001 and xpos + 4 + 16 <= len(lh_extra):
                # uncomp (Q) then comp (Q)
                lh_comp_size32 = struct.unpack_from("<Q", lh_extra, xpos + 12)[0]
                break
            xpos += 4 + esz
        comp_size = lh_comp_size32 if lh_comp_size32 not in (0, 0xFFFFFFFF) else cd_comp
    elif use_data_descriptor or lh_comp_size32 == 0:
        comp_size = cd_comp
    else:
        comp_size = lh_comp_size32

    # Add a small buffer: in practice LH and CD sizes can differ by a few KB
    fetch_size = comp_size + 4096

    if log:
        log.info(f"  fetching {comp_size / 1024 / 1024:.0f} MB compressed...")

    # == Step 2: stream-download + decompress ==================================
    CHUNK   = 8 * 1024 * 1024   # 8 MB per HTTP request
    decomp  = zlib.decompressobj(wbits=-15) if method == 8 else None

    byte_pos   = data_start
    bytes_left = fetch_size
    row_count  = 0

    with open(out_path, "wb") as fout:
        while bytes_left > 0:
            chunk_size = min(CHUNK, bytes_left)
            chunk = fetch_bytes(session, byte_pos, byte_pos + chunk_size - 1)

            if decomp is not None:
                try:
                    decoded = decomp.decompress(chunk)
                except zlib.error:
                    # Reached end of deflate stream inside this chunk — flush
                    decoded = decomp.flush()
                    fout.write(decoded)
                    row_count += decoded.count(b"\n")
                    break
            else:
                decoded = chunk   # method 0 = stored

            fout.write(decoded)
            row_count += decoded.count(b"\n")
            byte_pos   += chunk_size
            bytes_left -= chunk_size

    if decomp is not None:
        try:
            tail = decomp.flush()
            if tail:
                with open(out_path, "ab") as fout:
                    fout.write(tail)
                row_count += tail.count(b"\n")
        except zlib.error:
            pass  # already flushed in the loop

    # row_count = lines - 1 (header row)
    return max(row_count - 1, 0)


# == Main runner ===============================================================
def run(force: bool = False, entities: bool = False, transactions: bool = False,
        contributions: bool = False, expenditures: bool = False):
    """Download California CAL-ACCESS tables via HTTP Range requests.

    Horizontal scope:
        No flags        — all tables (RCPT, EXPN, registry)
        transactions    — RCPT + EXPN
        entities        — registry tables (FILERNAME, CVR, FILER_TO_FILER_TYPE)
        contributions   — RCPT only
        expenditures    — EXPN only

    No year flags — CAL-ACCESS is a single full-history ZIP with no year splitting.
    Loans/debts tables (DEBT_CD, LOAN_CD) are excluded — not used by the parser.
    """
    log = get_logger("california", "scrape")
    t0  = time.perf_counter()
    log._emit("scrape_started", force=force, entities=entities, transactions=transactions,
              contributions=contributions, expenditures=expenditures)

    # ── Resolve scope ─────────────────────────────────────────────────
    no_horizontal   = not (entities or transactions or contributions or expenditures)
    do_transactions = no_horizontal or transactions or contributions or expenditures
    do_entities     = no_horizontal or entities

    if contributions and not expenditures:
        active_tx = CONTRIBUTION_TABLES
    elif expenditures and not contributions:
        active_tx = EXPENDITURE_TABLES
    else:
        active_tx = TRANSACTION_TABLES

    targets = {}
    if do_transactions:
        targets.update({k: v for k, v in TARGET_TABLES.items() if v in active_tx})
    if do_entities:
        targets.update({k: v for k, v in TARGET_TABLES.items() if v in ENTITY_TABLES})

    files_ok = files_err = 0

    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36",
        })

        log.info("California: checking server...")
        zip_size, server_last_mod = check_zip(session)
        log.info(f"  ZIP = {zip_size / 1024**3:.2f} GB, last modified {server_last_mod}")

        done = {} if force else load_manifest()

        # Any explicit scope flag → ignore Last-Modified check for selected tables
        force_selected = force or not no_horizontal

        if not force_selected and all(done.get(n) == server_last_mod for n in targets.values()):
            log.info("California: selected files current — skipping.")
            log._emit("scrape_completed", status="completed",
                      duration_s=round(time.perf_counter() - t0, 1),
                      files_ok=0, files_err=0, note="all_current")
            return

        log.info("California: reading ZIP central directory...")
        cd = read_central_directory(session, zip_size)
        log.info(f"  central directory: {len(cd)} entries")

        for zip_path, local_name in targets.items():
            if not force_selected and done.get(local_name) == server_last_mod:
                log.file_download_skip(filename=local_name)
                continue

            if zip_path not in cd:
                log.file_download_error(filename=local_name,
                                        error="not found in ZIP central directory")
                files_err += 1
                continue

            out_path = RAW_DIR / local_name
            log.file_download_start(filename=local_name)
            t_file = time.perf_counter()

            try:
                row_count = extract_entry(session, cd[zip_path], zip_path, out_path,
                                          log=log)
            except Exception as e:
                log.file_download_error(filename=local_name, error=str(e))
                files_err += 1
                continue

            log.file_download_ok(filename=local_name,
                                 bytes=out_path.stat().st_size,
                                 rows=row_count,
                                 duration_s=round(time.perf_counter() - t_file, 1))
            update_manifest(local_name, {
                "filename":             local_name,
                "server_last_modified": server_last_mod,
                "downloaded_at":        datetime.today().strftime("%Y-%m-%d"),
                "row_count":            row_count,
            })
            files_ok += 1

        log.info("California: done.")
        log._emit("scrape_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
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


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Download California CAL-ACCESS campaign finance tables."
    )
    ap.add_argument("--force",         action="store_true",
                    help="re-download everything, ignoring the manifest")
    ap.add_argument("--transactions",  action="store_true",
                    help="transaction tables only (RCPT + EXPN)")
    ap.add_argument("--entities",      action="store_true",
                    help="entity/registry tables only (FILERNAME, CVR, FILER_TO_FILER_TYPE)")
    ap.add_argument("--contributions", action="store_true",
                    help="RCPT_CD only")
    ap.add_argument("--expenditures",  action="store_true",
                    help="EXPN_CD only")
    args, _ = ap.parse_known_args()
    try:
        run(force=args.force, entities=args.entities, transactions=args.transactions,
            contributions=args.contributions, expenditures=args.expenditures)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
