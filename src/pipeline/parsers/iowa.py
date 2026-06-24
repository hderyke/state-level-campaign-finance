"""
parsers/iowa.py — Parse Iowa DR-2 campaign finance PDFs into 5 normalized relations.

Input:  data/Iowa/raw/
  *.pdf    — DR-2 Summary PDFs downloaded from iecdbblobstorage.blob.core.windows.net
             (one file per committee per filing period)
  manifest.csv — tracks all downloaded PDFs with API metadata (committee name,
                 period year, organization type, candidate name, etc.)

Output: data/Iowa/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Each PDF contains:
  Page 1  — DR-2 Summary page: committee/candidate metadata, treasurer/chairperson
             contact info, and financial summary totals (not parsed for transactions).
  Sch-A   — Cash contributions: date, contributor name+address, relationship, amount.
  Sch-B   — Expenditures: date, payee name+address, purpose, amount.
  Sch-D   — Unpaid bills (skipped — not enough detail for loans_debts table).
  Sch-E   — In-kind contributions: date, contributor, description, estimated value.
  Sch-F1  — Loans received: date, lender name+address, relationship, amount.
  Sch-F2  — Loans paid/forgiven: date, lender, relationship, amount.
  Sch-G   — Consultant breakdown (skipped).
  Sch-H1  — Campaign property (skipped).
  Sch-H2  — Campaign property sales (skipped).

Notes
─────
  • pdfplumber's extract_tables() reliably detects schedule type from the column
    header on every page — no need to track page-to-schedule state across pages.
  • Name+address in contributions and expenditures is a single multi-line cell:
    first line = name, last line = "City, ST ZIP", middle lines = street address.
  • Committees are deduplicated by committee_code — only the most-recently-seen
    metadata is written (latest filing period wins).
  • Candidates come from committees with organizationType="Candidate" in the manifest.
  • In-kind contributions (Sch-E) are written as contributions with
    transaction_type="In-Kind".
  • Loans received (Sch-F1) go to loans_debts with record_type="loan_received".
  • Loans paid (Sch-F2) go to loans_debts with record_type="loan_repaid".
  • person_id model: "committee" — Iowa committee codes are per-registration;
    candidates may re-register each cycle with a new code.
"""

import csv
import gzip
import io
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Iowa" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Iowa" / "cleaned"
MANIFEST  = PROJECT_ROOT / "data" / "Iowa" / "manifest.csv"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "IA"
MAX_VALID_YEAR = date.today().year + 2

# ========================= schedule detection =======================
# Maps a normalized first-column header → schedule type.
# pdfplumber joins multi-line headers with '\n', so we normalize before matching.

def _norm(s: str) -> str:
    """Collapse whitespace (including newlines) for header matching."""
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


# Canonical first-cell values for each schedule's header row
SCH_A_HDR  = "contribution date"
SCH_B_HDR  = "expenditure date"
SCH_E_HDR  = "date"                   # Sch-E: "Date" | "Name & Address..."
SCH_F1_HDR = "date incurred"
SCH_F2_HDR = "date loan paid / forgiven"

# Minimum column count to distinguish data tables from sidebar tables
SCH_A_COLS  = 6
SCH_B_COLS  = 5
SCH_E_COLS  = 5
SCH_F1_COLS = 4
SCH_F2_COLS = 4

# Date pattern used to distinguish data rows from total/header rows
_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")

# City, ST ZIP pattern — Iowa addresses use "City, IA 50000" or "City IA 50000"
_ADDR_RE = re.compile(
    r"^(.*?)(?:,\s*|\s+)([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", re.IGNORECASE
)

# Amount: "$1,000.00" or "(1,000.00)" for negatives
_AMT_RE = re.compile(r"^\$?[\d,]+(?:\.\d+)?$")


# ============================== helpers ==============================

def clean(val) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


def parse_amount(val: str) -> str:
    """'$1,000.00' or '($500.00)' → plain numeric string. '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """M/D/YYYY or MM/DD/YYYY → YYYY-MM-DD. '' on failure or implausible year."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%-m/%-d/%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def parse_name_address(cell: str) -> tuple[str, str, str, str, str]:
    """
    Parse a multi-line name+address cell (newline-delimited) into
    (name, street, city, state, zip).

    Iowa PDFs wrap long org names across multiple lines within a single cell,
    so "first line = name" is wrong for orgs. Instead we:
      1. Find the city/state/zip line by scanning from the end.
      2. Find the start of the street by looking for the first line that starts
         with a digit or "P O Box" (typical Iowa street address patterns).
      3. Everything before the street is the name (joined with spaces).

    Returns (name, street, city, state, zip) — any field may be empty.
    """
    parts = [p.strip() for p in (cell or "").split("\n") if p.strip()]
    if not parts:
        return "", "", "", "", ""

    # 1. Find the city/state/zip line (scan from end — it's always last)
    addr_idx = None
    for i in range(len(parts) - 1, -1, -1):
        if _ADDR_RE.match(parts[i]):
            addr_idx = i
            break

    if addr_idx is None:
        # No address found — return whole content as name
        return " ".join(parts), "", "", "", ""

    m       = _ADDR_RE.match(parts[addr_idx])
    city    = m.group(1).strip().rstrip(",").title()   # normalize case
    state   = m.group(2).upper()
    zipcode = m.group(3)

    # 2. Find where the street starts — first line (after the first) that starts
    #    with a digit or "P O Box" variant
    _STREET_START = re.compile(r"^(?:\d|p\.?\s*o\.?\s*box)", re.IGNORECASE)
    street_idx = None
    for i in range(1, addr_idx):
        if _STREET_START.match(parts[i]):
            street_idx = i
            break

    if street_idx is not None:
        name   = " ".join(parts[:street_idx])
        street = " ".join(parts[street_idx:addr_idx])
    else:
        # No clear street start — everything before city/state/zip is the name
        name   = " ".join(parts[:addr_idx])
        street = ""

    return name, street, city, state, zipcode


def is_data_row(row: list) -> bool:
    """Return True if the first cell of a table row looks like a date."""
    if not row or not row[0]:
        return False
    return bool(_DATE_RE.match(clean(str(row[0]))))


def cell(row: list, idx: int) -> str:
    """Safe cell access — returns '' if index is out of range or cell is None."""
    if idx < len(row) and row[idx] is not None:
        return clean(str(row[idx]))
    return ""


def raw_cell(row: list, idx: int) -> str:
    """Cell access that preserves internal whitespace (newlines). Use for name+address cells."""
    if idx < len(row) and row[idx] is not None:
        return str(row[idx]).strip()
    return ""


# ========================= PDF page parsing ==========================

def classify_table(tbl: list[list]) -> str | None:
    """
    Given a pdfplumber table (list of rows), return the schedule type
    ('A', 'B', 'E', 'F1', 'F2') or None if not a data table.
    """
    if not tbl or len(tbl) < 2:
        return None
    hdr  = tbl[0]
    ncol = len(hdr)
    h0   = _norm(hdr[0] or "")

    if ncol >= SCH_A_COLS  and h0 == SCH_A_HDR:
        return "A"
    if ncol >= SCH_B_COLS  and h0 == SCH_B_HDR:
        return "B"
    if ncol >= SCH_F1_COLS and h0 == SCH_F1_HDR:
        return "F1"
    if ncol >= SCH_F2_COLS and h0 == SCH_F2_HDR:
        return "F2"
    # Sch-E: first col = "date", second col starts with "name & address"
    if ncol >= SCH_E_COLS and h0 == SCH_E_HDR:
        h1 = _norm(hdr[1] or "")
        if "name" in h1 and "address" in h1:
            return "E"
    return None


def parse_pdf_header(pdf: pdfplumber.PDF) -> dict:
    """
    Extract committee metadata from the DR-2 Summary (page 1).

    Uses table extraction exclusively — text extraction merges the two-column
    layout and bleeds right-column values (dates, status labels) into the left-
    column metadata fields.

    Table 0 structure (7 rows):
      row 0: [committee_name, None, "Status: Filed/Amended", None]
      row 1: ["Committee Type: Iowa PAC", None, "Statutory Due Date", date]
      row 2: ["County: Dallas", None, "Adjusted Due Date", ""]
      row 3: ["District: 28", None, "Filed Date", date]
      row 4: ["Committee Code: 2677", None, "Postmark Date", ""]
      row 5: ["Political Party: Democratic", None, "Amendment Date", date]
      row 6: ["Report Date: 2026", "Candidate Name: Walton, Tom", None, None]

    Table 1: treasurer contact info.
    """
    meta = {}
    if not pdf.pages:
        return meta

    tables = pdf.pages[0].extract_tables()
    if not tables:
        return meta

    t0 = tables[0]

    # ── Row 0: committee name + status ────────────────────────────
    if t0 and t0[0]:
        r0 = t0[0]
        if r0[0]:
            meta["committee_name"] = clean(str(r0[0]))
        if len(r0) > 2 and r0[2]:
            s = clean(str(r0[2]))
            if s.startswith("Status:"):
                meta["status"] = s.split(":", 1)[1].strip()

    # ── Rows 1–6: left-column metadata ────────────────────────────
    for row in t0[1:]:
        if not row or not row[0]:
            continue
        left = clean(str(row[0]))
        if left.startswith("Committee Type:"):
            meta["committee_type"] = left.split(":", 1)[1].strip()
        elif left.startswith("County:"):
            meta["county"] = left.split(":", 1)[1].strip()
        elif left.startswith("District:"):
            meta["district"] = left.split(":", 1)[1].strip()
        elif left.startswith("Committee Code:"):
            meta["committee_code"] = left.split(":", 1)[1].strip()
        elif left.startswith("Political Party:"):
            meta["political_party"] = left.split(":", 1)[1].strip()
        elif left.startswith("Report Date:"):
            # Candidate name lives in col 1: "Candidate Name: Walton, Tom"
            if len(row) > 1 and row[1]:
                cn = clean(str(row[1]))
                if cn.startswith("Candidate Name:"):
                    meta["candidate_name"] = cn.split(":", 1)[1].strip()

    # ── Treasurer (Table 1): "Last Name: X" in row 0 col 0 ───────
    for tbl in tables[1:]:
        if not tbl or not tbl[0] or not tbl[0][0]:
            continue
        r0 = tbl[0]
        h0 = clean(str(r0[0]))
        if not h0.startswith("Last Name:"):
            continue
        city_row = tbl[2] if len(tbl) > 2 else []
        meta["treasurer_last"]  = h0.split(":", 1)[1].strip()
        if len(r0) > 2 and r0[2]:
            meta["treasurer_first"] = clean(str(r0[2])).replace("First Name:", "").strip()
        if city_row:
            if city_row[0]:
                meta["treasurer_city"]  = clean(str(city_row[0])).replace("City:", "").strip()
            if len(city_row) > 3 and city_row[3]:
                meta["treasurer_zip"]   = clean(str(city_row[3])).replace("Zip Code:", "").strip()
        break

    return meta


def parse_sch_a_row(row: list, raw_file: str, row_num: int,
                    cmte_meta: dict, api_meta: dict) -> dict | None:
    """Parse a Sch-A (contributions) data row. Returns None if invalid."""
    if not is_data_row(row):
        return None
    date_val = parse_date(cell(row, 0))
    amount   = parse_amount(cell(row, 4))
    if not amount:
        return None

    contributor_name, street, city, addr_state, zipcode = parse_name_address(raw_cell(row, 2))
    relationship = cell(row, 3)
    # "None" is Iowa's way of saying no relationship — leave blank
    if relationship.lower() == "none":
        relationship = ""

    # Col 1 is "Contribution Committee" — committee code of donor committee
    # or payment method ("Check #\n1234"). Not a standard field but useful
    # as a proxy for contributor_type when it contains a committee code.
    col1 = cell(row, 1)

    # Detect unitemized aggregate rows
    if "unitemized" in contributor_name.lower():
        contributor_name = "Unitemized"

    return {
        "state":             STATE,
        "committee_name":    utils.clean_name(cmte_meta.get("committee_name", "")
                                              or api_meta.get("committee_name", "")),
        "contributor_name":  utils.clean_name(contributor_name),
        "amount":            amount,
        "date":              date_val,
        "transaction_type":  "Monetary",
        "contributor_type":  "",           # Iowa Sch-A has no type column
        "contributor_city":  city,
        "contributor_state": addr_state,
        "contributor_zip":   zipcode,
        "candidate_name":    utils.clean_name(
                                 cmte_meta.get("candidate_name", "")
                                 or api_meta.get("candidate_name", "")),
        "office":            _office_from_type(cmte_meta.get("committee_type", "")
                                               or api_meta.get("organization_type", "")),
        "election_year":     api_meta.get("period_year", ""),
        "filing_id":         col1,
        "amended":           "1" if "amended" in (cmte_meta.get("status") or "").lower() else "0",
        "raw_file":          raw_file,
        "row_num":           row_num,
    }


def parse_sch_b_row(row: list, raw_file: str, row_num: int,
                    cmte_meta: dict, api_meta: dict) -> dict | None:
    """Parse a Sch-B (expenditures) data row. Returns None if invalid."""
    if not is_data_row(row):
        return None
    date_val = parse_date(cell(row, 0))
    amount   = parse_amount(cell(row, 4))
    if not amount:
        return None

    payee_name, street, city, addr_state, zipcode = parse_name_address(raw_cell(row, 2))
    purpose = cell(row, 3)

    return {
        "state":            STATE,
        "committee_name":   utils.clean_name(cmte_meta.get("committee_name", "")
                                             or api_meta.get("committee_name", "")),
        "payee_name":       utils.clean_name(payee_name),
        "amount":           amount,
        "date":             date_val,
        "transaction_type": "Monetary",
        "purpose":          purpose,
        "category":         "",
        "payee_city":       city,
        "payee_state":      addr_state,
        "payee_zip":        zipcode,
        "candidate_name":   utils.clean_name(
                                cmte_meta.get("candidate_name", "")
                                or api_meta.get("candidate_name", "")),
        "office":           _office_from_type(cmte_meta.get("committee_type", "")
                                              or api_meta.get("organization_type", "")),
        "election_year":    api_meta.get("period_year", ""),
        "filing_id":        cell(row, 1),
        "amended":          "1" if "amended" in (cmte_meta.get("status") or "").lower() else "0",
        "raw_file":         raw_file,
        "row_num":          row_num,
    }


def parse_sch_e_row(row: list, raw_file: str, row_num: int,
                    cmte_meta: dict, api_meta: dict) -> dict | None:
    """Parse a Sch-E (in-kind contributions) data row."""
    if not is_data_row(row):
        return None
    date_val = parse_date(cell(row, 0))
    amount   = parse_amount(cell(row, 4))
    if not amount:
        return None

    contributor_name, street, city, addr_state, zipcode = parse_name_address(raw_cell(row, 1))
    description = cell(row, 3)

    return {
        "state":             STATE,
        "committee_name":    utils.clean_name(cmte_meta.get("committee_name", "")
                                              or api_meta.get("committee_name", "")),
        "contributor_name":  utils.clean_name(contributor_name),
        "amount":            amount,
        "date":              date_val,
        "transaction_type":  "In-Kind",
        "contributor_type":  "",
        "contributor_city":  city,
        "contributor_state": addr_state,
        "contributor_zip":   zipcode,
        "candidate_name":    utils.clean_name(
                                 cmte_meta.get("candidate_name", "")
                                 or api_meta.get("candidate_name", "")),
        "office":            _office_from_type(cmte_meta.get("committee_type", "")
                                               or api_meta.get("organization_type", "")),
        "election_year":     api_meta.get("period_year", ""),
        "filing_id":         "",
        "amended":           "1" if "amended" in (cmte_meta.get("status") or "").lower() else "0",
        "raw_file":          raw_file,
        "row_num":           row_num,
    }


def parse_sch_f1_row(row: list, raw_file: str, row_num: int,
                     cmte_meta: dict, api_meta: dict) -> dict | None:
    """Parse a Sch-F1 (loans received) row → loans_debts record."""
    if not is_data_row(row):
        return None
    date_val = parse_date(cell(row, 0))
    amount   = parse_amount(cell(row, 3))
    if not amount:
        return None

    lender_name, street, city, addr_state, zipcode = parse_name_address(raw_cell(row, 1))

    return {
        "state":               STATE,
        "committee_name":      utils.clean_name(cmte_meta.get("committee_name", "")
                                                or api_meta.get("committee_name", "")),
        "original_amount":     amount,
        "date":                date_val,
        "record_type":         "loan_received",
        "counterparty_name":   utils.clean_name(lender_name),
        "counterparty_city":   city,
        "counterparty_state":  addr_state,
        "counterparty_zip":    zipcode,
        "candidate_name":      utils.clean_name(
                                   cmte_meta.get("candidate_name", "")
                                   or api_meta.get("candidate_name", "")),
        "election_year":       api_meta.get("period_year", ""),
        "amended":             "1" if "amended" in (cmte_meta.get("status") or "").lower() else "0",
        "filing_id":           "",
        "raw_file":            raw_file,
        "row_num":             row_num,
    }


def parse_sch_f2_row(row: list, raw_file: str, row_num: int,
                     cmte_meta: dict, api_meta: dict) -> dict | None:
    """Parse a Sch-F2 (loans paid/forgiven) row → loans_debts record."""
    if not is_data_row(row):
        return None
    date_val = parse_date(cell(row, 0))
    amount   = parse_amount(cell(row, 3))
    if not amount:
        return None

    lender_name, street, city, addr_state, zipcode = parse_name_address(raw_cell(row, 1))

    return {
        "state":               STATE,
        "committee_name":      utils.clean_name(cmte_meta.get("committee_name", "")
                                                or api_meta.get("committee_name", "")),
        "original_amount":     amount,
        "date":                date_val,
        "record_type":         "loan_repaid",
        "counterparty_name":   utils.clean_name(lender_name),
        "counterparty_city":   city,
        "counterparty_state":  addr_state,
        "counterparty_zip":    zipcode,
        "candidate_name":      utils.clean_name(
                                   cmte_meta.get("candidate_name", "")
                                   or api_meta.get("candidate_name", "")),
        "election_year":       api_meta.get("period_year", ""),
        "amended":             "1" if "amended" in (cmte_meta.get("status") or "").lower() else "0",
        "filing_id":           "",
        "raw_file":            raw_file,
        "row_num":             row_num,
    }


def _office_from_type(committee_type: str) -> str:
    """
    Derive a canonical office label from Iowa's committee type strings.
    E.g. "State House" → "State House", "Statewide" → "Statewide".
    Unknown types and PAC types map to "".
    """
    t = (committee_type or "").strip()
    if not t:
        return ""
    # Candidate committee types that map cleanly to office
    offices = {
        "State House":   "State House",
        "State Senate":  "State Senate",
        "Statewide":     "Statewide",
        "County":        "County",
        "City":          "City",
        "School":        "School",
        "Judicial":      "Judicial",
    }
    for key, val in offices.items():
        if key.lower() in t.lower():
            return val
    return ""


# ========================= manifest index ===========================

def load_manifest_index() -> dict[str, dict]:
    """
    Load manifest.csv into a lookup keyed by filename.
    Used by the parser to get API-provided metadata for each PDF.
    """
    idx: dict[str, dict] = {}
    if not MANIFEST.exists():
        return idx
    with open(MANIFEST, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fn = row.get("filename", "")
            if fn:
                idx[fn] = row
    return idx


# ============================== run ==================================

def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def run():
    log = get_logger("iowa", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        # ── Load manifest index ────────────────────────────────────────
        api_index = load_manifest_index()
        log.info(f"  Manifest index: {len(api_index):,} entries")

        if not api_index:
            log.warning("No manifest found — run the scraper first.")

        # ── Collect PDFs to parse ──────────────────────────────────────
        # Only parse files listed in the manifest (so orphaned old amendments
        # are never re-parsed). Sort by period_year then filename for
        # deterministic output order.
        manifest_files = sorted(
            api_index.keys(),
            key=lambda fn: (api_index[fn].get("period_year", ""), fn),
        )
        pdfs = [RAW_DIR / fn for fn in manifest_files
                if (RAW_DIR / fn).exists() and (RAW_DIR / fn).stat().st_size > 0]

        log.info(f"  PDFs to parse: {len(pdfs):,}")

        # ── Dedup sets — one committee/candidate row per unique code ───
        seen_cmte_codes: set[str] = set()
        seen_cand_codes: set[str] = set()

        # ── Iterate PDFs ───────────────────────────────────────────────
        parse_errors = 0
        for pdf_path in pdfs:
            ft       = time.perf_counter()
            raw_file = pdf_path.name
            api_meta = api_index.get(raw_file, {})

            cont_count = expn_count = loan_count = 0
            row_num    = 1   # 1-based row counter across all tables in this PDF

            try:
                with pdfplumber.open(pdf_path) as pdf:
                    # ── Page 1: committee metadata ─────────────────────
                    cmte_meta = parse_pdf_header(pdf)

                    # Fall back to manifest data when header parse is sparse
                    cmte_code = (cmte_meta.get("committee_code")
                                 or api_meta.get("committee_code", "")).strip()
                    cmte_name = (cmte_meta.get("committee_name")
                                 or api_meta.get("committee_name", "")).strip()
                    cmte_type = (cmte_meta.get("committee_type")
                                 or api_meta.get("organization_type", "")).strip()
                    cand_name = (cmte_meta.get("candidate_name")
                                 or api_meta.get("candidate_name", "")).strip()
                    party     = cmte_meta.get("political_party", "").strip()
                    district  = cmte_meta.get("district", "").strip()
                    county    = cmte_meta.get("county", "").strip()
                    period_yr = api_meta.get("period_year", "")

                    # Treasurer contact (for committee treasurer_name / city / zip)
                    treas_last  = cmte_meta.get("treasurer_last", "")
                    treas_first = cmte_meta.get("treasurer_first", "")
                    treas_name  = f"{treas_last}, {treas_first}".strip(", ") \
                                  if treas_last else ""
                    treas_city  = cmte_meta.get("treasurer_city", "")
                    treas_zip   = cmte_meta.get("treasurer_zip", "")

                    # ── Write committee row (first time we see this code) ──
                    if cmte_code and cmte_code not in seen_cmte_codes:
                        seen_cmte_codes.add(cmte_code)
                        is_candidate_cmte = (
                            api_meta.get("organization_type", "").lower() == "candidate"
                            or bool(cand_name)
                        )
                        cmte_w.writerow({
                            "state":          STATE,
                            "state_filer_id": cmte_code,
                            "committee_name": utils.clean_name(cmte_name),
                            "committee_type": cmte_type,
                            "election_year":  period_yr,
                            "candidate_name": utils.clean_name(cand_name),
                            "treasurer_name": utils.clean_name(treas_name),
                            "city":           treas_city,
                            "zip":            treas_zip,
                            "active":         "",
                            "raw_file":       raw_file,
                            "row_num":        1,
                        })
                        committees_written += 1

                        # ── Write candidate row if this is a candidate committee ──
                        if is_candidate_cmte and cand_name \
                                and cmte_code not in seen_cand_codes:
                            seen_cand_codes.add(cmte_code)
                            # Iowa names in "Last, First" format from the API;
                            # parse to first/last for candidate columns
                            cand_clean = utils.clean_name(cand_name)
                            if "," in cand_clean:
                                cand_last, cand_first = cand_clean.split(",", 1)
                                cand_last  = cand_last.strip()
                                cand_first = cand_first.strip()
                            else:
                                cand_last  = cand_clean
                                cand_first = ""

                            cand_w.writerow({
                                "state":           STATE,
                                "state_filer_id":  cmte_code,
                                "candidate_name":  cand_clean,
                                "candidate_first": cand_first,
                                "candidate_last":  cand_last,
                                "office":          _office_from_type(cmte_type),
                                "district":        district,
                                "jurisdiction":    county,
                                "party":           party,
                                "election_year":   period_yr,
                                "incumbent":       "",
                                "raw_file":        raw_file,
                                "row_num":         1,
                            })
                            candidates_written += 1

                    # ── Pages 2+: schedule data ────────────────────────
                    for page in pdf.pages[1:]:
                        for tbl in page.extract_tables():
                            sch = classify_table(tbl)
                            if sch is None:
                                continue

                            for data_row in tbl[1:]:   # skip header row
                                row_num += 1

                                if sch == "A":
                                    rec = parse_sch_a_row(data_row, raw_file, row_num,
                                                          cmte_meta, api_meta)
                                    if rec:
                                        cont_w.writerow(rec)
                                        cont_count += 1

                                elif sch == "B":
                                    rec = parse_sch_b_row(data_row, raw_file, row_num,
                                                          cmte_meta, api_meta)
                                    if rec:
                                        expn_w.writerow(rec)
                                        expn_count += 1

                                elif sch == "E":
                                    rec = parse_sch_e_row(data_row, raw_file, row_num,
                                                          cmte_meta, api_meta)
                                    if rec:
                                        cont_w.writerow(rec)
                                        cont_count += 1

                                elif sch == "F1":
                                    rec = parse_sch_f1_row(data_row, raw_file, row_num,
                                                           cmte_meta, api_meta)
                                    if rec:
                                        loan_w.writerow(rec)
                                        loan_count += 1

                                elif sch == "F2":
                                    rec = parse_sch_f2_row(data_row, raw_file, row_num,
                                                           cmte_meta, api_meta)
                                    if rec:
                                        loan_w.writerow(rec)
                                        loan_count += 1

            except Exception as e:
                log.warning(f"  Error parsing {raw_file}: {e}")
                parse_errors += 1
                continue

            total_contributions += cont_count
            total_expenditures  += expn_count
            total_loans         += loan_count

            log.file_parsed(raw_file, "pdf",
                            cont_count + expn_count + loan_count,
                            duration_s=round(time.perf_counter() - ft, 3),
                            bytes=pdf_path.stat().st_size)

        if parse_errors:
            log.warning(f"  {parse_errors} PDFs failed to parse")

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # Iowa committee codes are per-registration (candidates may re-register
        # each cycle). Group by (state, candidate_name, office, district) to
        # assign a stable cross-cycle person_id.
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="committee")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(
            f"Done in {duration}s — "
            f"{total_contributions:,} contributions, "
            f"{total_expenditures:,} expenditures, "
            f"{total_loans:,} loans, "
            f"{committees_written:,} committees, "
            f"{candidates_written:,} candidates"
        )
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=committees_written, candidates=candidates_written,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
