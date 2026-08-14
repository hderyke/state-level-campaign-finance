"""
parsers/kansas.py — Parse Kansas KPDC R&E PDFs into canonical cleaned CSVs.

Input:  data/Kansas/raw/
    *.pdf         — Receipts & Expenditures report PDFs (one per candidate per period)
    manifest.csv  — Downloaded file list with office/cycle/district/candidate metadata

Output: data/Kansas/cleaned/
    contributions.csv.gz, expenditures.csv.gz,
    candidates.csv.gz, committees.csv.gz

Each R&E PDF contains:
    Page 1  — Summary (candidate name, office, district, period dates, totals)
    Sch A   — Contributions and Other Receipts (date, contributor, type, amount)
    Sch B   — In-Kind Contributions (treated as contributions, transaction_type="In-Kind")
    Sch C   — Expenditures and Other Disbursements (date, payee, purpose, amount)
    Sch D   — Other Transactions / Loans (skipped)

id_model = "name_hash"
    Kansas has no numeric filer ID in its source data — candidates are identified
    only by name on the HTML index and in the PDF header.  person_id is derived
    from MD5(state + normalized candidate_name), same model as Alaska and Idaho.

Party: not available in source.

Column layout (from pdfplumber word-coordinate analysis):
    Schedule A (contributions):
        Date       x < 100
        Name/Addr  100 ≤ x < 235
        Pay Type   235 ≤ x < 360
        Occupation 360 ≤ x < 475
        Amount     x ≥ 475

    Schedule C (expenditures):
        Date       x < 100
        Name/Addr  100 ≤ x < 290
        Purpose    290 ≤ x < 490
        Amount     x ≥ 490

Parsing strategy:
    For each page belonging to a schedule, extract all words with (x0, top, text)
    from pdfplumber.  Group words into logical rows by clustering on y-coordinate
    (words within ~4pt of each other share a row).  Identify transaction-anchor
    rows by a date-pattern word (MM/DD/YY) appearing in the Date column.  For
    each anchor, collect all words in Name/Addr and other columns between this
    anchor and the next, assemble the address block, and extract the amount.

Amendment handling:
    A PDF named *_amend{period}.pdf is an amendment to the corresponding
    *_{period}.pdf original.  When both exist for the same (candidate, period),
    the amendment is used and the original is skipped.  Only the amendment's
    transactions are emitted; the raw_file field records the amendment filename.
"""

import csv
import gzip
import re
import subprocess
import sys
import time
from collections import defaultdict
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

# =============================== Paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Kansas" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Kansas" / "cleaned"
MANIFEST  = PROJECT_ROOT / "data" / "Kansas" / "manifest.csv"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "KS"
EARLIEST_YEAR  = 2013          # earliest R&E filing cycle (2014 statewide covers 2011-2014)
MAX_VALID_YEAR = date.today().year + 4

# Valid US state/territory codes — used to filter out PDF parsing artifacts
VALID_US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID',
    'IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS',
    'MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK',
    'OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV',
    'WI','WY','DC','PR','GU','VI','AS','MP',
}

# =================== Column x-ranges (points) ========================
# Kansas PDFs are a mix of two formats:
#
#   Scanned physical forms (pre-2024):
#     Sch A:  Date x<100 | Name/Addr 80-235 | Type 235-360 | Occ 360-475 | Amount x≥475
#     Sch C:  Date x<100 | Name/Addr 80-290 | Purpose 290-490 | Amount x≥490
#     (Amounts at x≈542 for Sch A; x≈471-490 for Sch C)
#
#   Web-form exports (online filing system, common 2024+):
#     Sch A:  Date x<100 | Name/Addr ~80-150 | Type ~150-235 | Amount ~380-410 | PrimaryTotal ~470 | GeneralTotal ~530
#     Sch C:  Date x<100 | Name/Addr ~80-150 | Purpose ~150-280 | Amount ~385-410 | PrimaryTotal ~450 | GeneralTotal ~540
#     (Actual contribution amount is the LEFTMOST dollar figure in each row)
#
# Setting A_AMT_MIN / C_AMT_MIN = 350 captures the leftmost amount in both
# formats.  In web forms this is the actual per-entry amount (not the running
# Primary/General totals to its right); in scanned forms the single amount
# column also falls above 350.  Nothing amount-like appears at x=235-400 on
# anchor rows in scanned forms (the "$150" header sits on the non-anchor header
# row and is never picked up by the anchor-row loop).
# Schedule A
A_DATE_MAX  = 100
A_NAME_MIN  =  80;  A_NAME_MAX  = 235
A_TYPE_MIN  = 235;  A_TYPE_MAX  = 360
A_OCC_MIN   = 360;  A_OCC_MAX   = 475
A_AMT_MIN   = 350                        # was 475; lowered to capture web-form amounts at x≈399

# Schedule C
C_DATE_MAX  = 100
C_NAME_MIN  =  80;  C_NAME_MAX  = 290
C_PURP_MIN  = 290;  C_PURP_MAX  = 490
C_AMT_MIN   = 350                        # was 490; lowered to capture web-form amounts at x≈388

# Y-cluster tolerance: words within this many points share the same visual row
Y_TOL = 4.0

# ======================== Date / amount helpers =======================
_DATE_RE   = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}$")
_AMOUNT_RE = re.compile(r"^\$?[\d,]+\.\d{2}$")
# Matches "City ST 12345", "City, ST 12345-6789", or just "ST 12345" (no city).
# The \s* (vs \s+) before the state code handles entries where city is on a
# separate row from state+zip (common in statewide-candidate scanned PDFs).
# The zip allows an optional trailing dash ("66213-") for truncated 9-digit zips.
_CITY_ST_ZIP_RE = re.compile(
    r"^(.*?),?\s*([A-Z]{2})\s+(\d{5}(?:-\d{0,4})?)\s*$", re.IGNORECASE
)
# Zip code alone on a row (used when city+state appeared on the previous row).
_ZIP_ONLY_RE = re.compile(r"^(\d{5}(?:-\d{0,4})?)\s*$")
# Characters OCR commonly substitutes for the digit "1": pipe, capital-I,
# lowercase-l, backslash, brackets.  Used for garbled-zip detection in old PDFs.
_OCR_DIGIT_LIKE_RE = re.compile(r'^[\d\[\\\|IlL]', re.IGNORECASE)
# Strips payment-type keywords (and everything after them) from a text line.
# Used to isolate contributor/payee names when they appear on the anchor row.
_ANCHOR_TYPE_KW_RE = re.compile(
    r'\s*\b(Credit|Debit|Check|Cash|Loan|E-?Transfer|Electronic|Other|Transfer)\b.*$',
    re.IGNORECASE,
)
# If the entire extracted name is just a payment-type keyword, discard it.
# (Covers "Loan" appearing in name col at x≈186 in Kobach statewide format.)
_PAYMENT_TYPE_ONLY_RE = re.compile(
    r'^\s*(Loan|Credit\s+Card|Check|Cash|Debit|Transfer|Electronic|Other)\s*$',
    re.IGNORECASE,
)


def _parse_date(val: str) -> str:
    """MM/DD/YY or MM/DD/YYYY → YYYY-MM-DD. Returns '' on failure."""
    v = val.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            d = datetime.strptime(v, fmt).date()
            if EARLIEST_YEAR <= d.year <= MAX_VALID_YEAR:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_amount(val: str) -> str:
    """'$1,234.56' or '1234.56' → plain decimal string. '' on failure."""
    v = val.strip().replace("$", "").replace(",", "")
    try:
        return str(float(v))
    except ValueError:
        return ""


def _clean(val: str) -> str:
    return re.sub(r"\s+", " ", (val or "").strip())


def _election_year_clean(val: str) -> str:
    """'2022-special' → '2022'; '2026' → '2026'. Strips non-numeric suffixes."""
    m = re.match(r"(\d{4})", (val or "").strip())
    return m.group(1) if m else val


def _valid_state(code: str) -> str:
    """Return the state code if it's a valid US state/territory, else ''."""
    return code.upper() if code.upper() in VALID_US_STATES else ""


# Text in the name column that signals "stop going further back" during the
# backward payee/contributor lookup — column headers and "Not Available" placeholders.
# Note: "Campaign Finance Schedule A Report" often has "Campaign"/"Schedule" at
# x<80 so the name column sees only "A Report" or "Finance Schedule A Report".
_SKIP_BACK_NAME_RE = re.compile(
    r'^\s*(Name\s+and\s+Address|Name\s+and\b|and\s+Address\b'
    r'|of\s+Contributor\b'         # PLF/GLF column header row 2: "of Contributor"
    r'|Date\s+Name\b|^Date\b'      # PLF/GLF column header row 1: "Date Name and…"
    r'|Mailing\s+Address\b'        # PLF/GLF address sub-header
    r'|Not\s*Available\b|NotAvailable'
    r'|Schedule\s+[A-Z]\b'
    r'|Finance\s+Schedule\b'
    r'|[A-Z]\s+Report\b'          # "A Report", "C Report" (partial header)
    r'|Candidate:|Campaign\s+Finance)',
    re.IGNORECASE,
)

# Detects rows that are street addresses rather than contributor/payee names,
# so the backward lookup can skip them with `continue` and look further above
# for the real name.  Scanned Kansas PDFs always put the street number at the
# start of the address row; OCR sometimes fuses digits+street into one token.
_STREET_LIKE_RE = re.compile(
    r'(^\d{3,})'                              # 3+ leading digits → street number
    r'|(\bP\.?O\.?\s*Box\b)'                  # PO Box
    r'|(\b(?:Dr|St|Ave|Blvd|Rd|Ln|Ct|Pl|Ter|Cir|Hwy'
    r'|Drive|Street|Avenue|Boulevard|Road|Lane|Court|Place|Terrace|Circle|Highway)'
    r'\.?\s*$)',                               # ends with a street-type suffix
    re.IGNORECASE,
)


# ========================= pdfplumber helpers ========================

def _words(page) -> list[tuple[float, float, str]]:
    """Return [(x0, top, text), ...] for all words on a page."""
    return [(w["x0"], w["top"], w["text"]) for w in page.extract_words()]


def _cluster_rows(words: list[tuple[float, float, str]]) -> list[list[tuple[float, float, str]]]:
    """Group words into visual rows by clustering on y (top) within Y_TOL."""
    if not words:
        return []
    sorted_w = sorted(words, key=lambda w: w[1])
    rows, current_y, current = [], sorted_w[0][1], []
    for w in sorted_w:
        if abs(w[1] - current_y) <= Y_TOL:
            current.append(w)
        else:
            rows.append(sorted(current, key=lambda ww: ww[0]))
            current_y = w[1]
            current   = [w]
    if current:
        rows.append(sorted(current, key=lambda ww: ww[0]))
    return rows


def _row_text(row: list[tuple], x_min: float, x_max: float) -> str:
    """Join words in a row that fall within [x_min, x_max)."""
    return " ".join(w[2] for w in row if x_min <= w[0] < x_max).strip()


def _any_in_col(row, x_min, x_max) -> bool:
    return any(x_min <= w[0] < x_max for w in row)


# ======================== Schedule detection =========================
_SCH_A_MARKERS = {"SCHEDULE A", "CONTRIBUTIONS AND OTHER RECEIPTS"}
_SCH_B_MARKERS = {"SCHEDULE B", "IN-KIND", "IN KIND"}
_SCH_C_MARKERS = {"SCHEDULEC", "SCHEDULE C", "EXPENDITURES AND OTHER DISBURSEMENTS"}
_SCH_D_MARKERS = {"SCHEDULED", "SCHEDULE D", "OTHER TRANSACTIONS"}
_FOOTER_TEXTS  = {"Print", "this", "form", "or", "Go", "Back",
                   "Total", "Itemized", "Unitemized", "TOTAL", "RECEIPTS",
                   "EXPENDITURES", "DISBURSEMENTS", "THIS", "PERIOD",
                   "Contributions", "Less"}


def _page_schedule(page_text: str) -> str | None:
    """Return 'A', 'B', 'C', 'D', or None based on schedule markers on the page."""
    upper = page_text.upper()
    # Check D before C because "SCHEDULE D" and "SCHEDULED" could both appear
    if any(m in upper for m in ("SCHEDULE D", "SCHEDULED\n", "SCHEDULED ")):
        return "D"
    if any(m in upper for m in ("SCHEDULE C", "SCHEDULEC")):
        return "C"
    if "SCHEDULE B" in upper and "IN-KIND" in upper:
        return "B"
    if "SCHEDULE A" in upper:
        return "A"
    return None


# ========================= PDF header parsing ========================
# From the summary page (page 1) of each R&E PDF.
# Web-form PDFs use "Candidate Name'.Dale" (apostrophe+period) instead of a colon.
# The character class [:.'\s]{1,4} handles both the scanned-form colon and the
# web-form apostrophe/period separators.
_CAND_RE     = re.compile(r"Candidate\s*Name\s*[:.'\s]{1,4}(.+?)(?:\n|$)", re.IGNORECASE)
_OFFICE_RE   = re.compile(r"Office\s*Sought\s*[:.'\s]{1,4}(.+?)(?:District|$)", re.IGNORECASE)
_DIST_RE     = re.compile(r"District\s*(?:No\.?\s*)?:?\s*(\S+)", re.IGNORECASE)
_PERIOD_RE   = re.compile(
    r"(?:covering|period|from)\s+(\d{1,2}/\d{1,2}/\d{4})\s+through\s+(\d{1,2}/\d{1,2}/\d{4})",
    re.IGNORECASE,
)


def _parse_header(page1_text: str) -> dict:
    """Extract candidate name, office, district from the PDF summary page."""
    result = {"candidate_name": "", "office": "", "district": ""}

    m = _CAND_RE.search(page1_text)
    if m:
        result["candidate_name"] = _clean(m.group(1))

    m = _OFFICE_RE.search(page1_text)
    if m:
        result["office"] = _clean(m.group(1))

    m = _DIST_RE.search(page1_text)
    if m:
        result["district"] = _clean(m.group(1))

    return result


# ====================== Schedule A parser ============================
#
# Kansas R&E PDF layout for each contribution entry:
#
#   [name row, y ≈ date_y - 13]  [NAME COL]  "Kent Soucy"
#   [date row, y ≈ date_y]       [DATE] MM/DD/YY | [NAME] "3381 SE Boston Mills" | [TYPE] Cash | [OCC] Business | [AMT] $100
#   [addr row, y ≈ date_y + 13]  [NAME COL]  "Columbus KS 66725"
#
# The contributor NAME appears on the row BEFORE the date anchor.
# The STREET ADDRESS appears on the anchor row's name column.
# The city/state/zip appears on the row after the anchor.
#
# Entries with no known contributor show "Not Available" on the anchor row.

def _parse_schedule_a(pages, schedule_pages: list[int]) -> list[dict]:
    """
    Parse Schedule A (contributions) from the given page indices.
    Returns list of raw contribution dicts.
    """
    transactions = []

    for pi in schedule_pages:
        page  = pages[pi]
        words = _words(page)
        rows  = _cluster_rows(words)

        anchor_indices = []
        for ri, row in enumerate(rows):
            if any(w[0] < A_DATE_MAX and _DATE_RE.match(w[2]) for w in row):
                anchor_indices.append(ri)

        for idx, anchor_ri in enumerate(anchor_indices):
            end_ri      = anchor_indices[idx + 1] if idx + 1 < len(anchor_indices) else len(rows)
            anchor_row  = rows[anchor_ri]

            # ── Date ─────────────────────────────────────────────────
            date_word = next(
                (w for w in anchor_row if w[0] < A_DATE_MAX and _DATE_RE.match(w[2])), None
            )
            if not date_word:
                continue
            parsed_date = _parse_date(date_word[2])
            if not parsed_date:
                continue

            # ── Amount (on anchor row) ────────────────────────────────
            amount = ""
            for cr in rows[anchor_ri:min(anchor_ri + 3, end_ri)]:
                amt_words = [w for w in cr if w[0] >= A_AMT_MIN and _AMOUNT_RE.match(w[2])]
                if amt_words:
                    amount = _parse_amount(amt_words[0][2])
                    break

            # ── Contributor name (row(s) BEFORE the anchor) ───────────
            # Walk backward from anchor_ri - 1, collecting name-column text.
            # Street/zip rows are SKIPPED via `continue`.  Two heuristics stop
            # the scan before crossing into the PREVIOUS entry's address block:
            #
            #  A) found_street + name_parts: after passing the current entry's
            #     street address, any row ending in a valid 2-letter state code
            #     is the previous entry's "City ST" line (no zip on that row).
            #     e.g. "Tonganoxie KS" / "66086" split → breaks at "Tonganoxie KS"
            #
            #  B) prev_was_zip: if the most-recently-skipped row was a bare zip
            #     code ("67042"), the next backward row is very likely the
            #     previous entry's "City ST" — break before adding it.
            #     e.g. skip "67042", then see "ElDorado KS" → break.
            #     This handles entries where the name is on the anchor row itself
            #     (Kobach format: "12/31/25 Ann Peterson Credit Card …").
            name_parts: list[str] = []
            found_street = False
            prev_was_zip = False
            for back_ri in range(anchor_ri - 1, max(-1, anchor_ri - 6), -1):
                back_row = rows[back_ri]
                if any(w[0] < A_DATE_MAX and _DATE_RE.match(w[2]) for w in back_row):
                    break   # previous date anchor — stop
                line = _row_text(back_row, A_NAME_MIN, A_NAME_MAX)
                if not line:
                    break   # gap row — stop
                if _SKIP_BACK_NAME_RE.match(line):
                    break   # column header / "Schedule A" header / "Not Available"
                if _CITY_ST_ZIP_RE.match(line):
                    break   # "City ST 12345" of previous entry — stop
                # Also stop on OCR-garbled "City ST zip" where the zip contains
                # OCR noise ("SALINA KS 6740 I" for "SALINA KS 67401").  Detect
                # as: any token is a valid state code, and the NEXT token starts
                # with a digit-like character (including I/l/\\ OCR artifacts).
                _toks = line.split()
                _garbled = False
                for _j, _tok in enumerate(_toks[1:-1], 1):
                    if _valid_state(_tok) and _OCR_DIGIT_LIKE_RE.match(_toks[_j + 1]):
                        _garbled = True
                        break
                if _garbled:
                    break
                if _STREET_LIKE_RE.search(line):
                    found_street = True
                    prev_was_zip = bool(_ZIP_ONLY_RE.match(line))
                    continue  # address row — skip but keep looking above
                # Heuristic A: once we have ≥1 name row, any subsequent row
                # ending in a valid 2-letter state code is the previous entry's
                # "City ST" line (no zip).  `found_street` is NOT required here;
                # entries where the street is on the anchor row also need this.
                if name_parts:
                    toks = line.split()
                    if toks and len(toks[-1]) == 2 and _valid_state(toks[-1]):
                        break  # "Tonganoxie KS" / "Ottawa, KS" → previous entry
                # Heuristic B: last skipped row was a bare zip → this row is
                # "City ST" (no zip on this row).  name_parts may be empty here
                # (entry whose name is on the anchor row, e.g. "Ann Peterson").
                if prev_was_zip:
                    toks = line.split()
                    if toks and len(toks[-1]) == 2 and _valid_state(toks[-1]):
                        break  # "ElDorado KS" after "67042" → previous entry
                prev_was_zip = False
                name_parts.insert(0, line)

            # Fallback: some Kobach entries embed the name on the anchor row
            # itself (e.g. "12/31/25 Ann Peterson Credit Card retired …").
            # If the backward scan found nothing, try the anchor row's name
            # column, stripping trailing payment-type keywords.  Also reject
            # anything that looks like a street address ("12332 us 24 hwy").
            contributor_name = _clean(" ".join(name_parts))
            # Discard if the entire name is just a payment-type keyword
            # (e.g. "Loan" from x≈186 in Kobach statewide format)
            if _PAYMENT_TYPE_ONLY_RE.match(contributor_name):
                contributor_name = ""
            if not contributor_name:
                anchor_line = _row_text(anchor_row, A_NAME_MIN, A_NAME_MAX)
                anchor_line = _ANCHOR_TYPE_KW_RE.sub("", anchor_line).strip()
                if anchor_line and not _STREET_LIKE_RE.search(anchor_line):
                    contributor_name = _clean(anchor_line)

            # Skip totals / summary rows
            if any(kw in contributor_name.upper()
                   for kw in ("TOTAL", "UNITEMIZED", "SALE OF", "CONTRIBUTOR NOT")):
                continue

            # ── City / State / ZIP (rows after anchor) ────────────────
            # Kansas PDFs use several split layouts for the address block:
            #   "City ST 12345"          (all on one row — easy)
            #   "City" / "ST 12345"      (Kobach statewide: 2 rows)
            #   "City ST" / "12345"      (some entries: city+state then zip-only)
            # pending_city / pending_state track partial address components.
            contributor_city = contributor_state_code = contributor_zip = ""
            pending_city = ""
            pending_state = ""
            for cr in rows[anchor_ri + 1:end_ri]:
                line = _row_text(cr, A_NAME_MIN, A_NAME_MAX)
                if not line:
                    continue
                # Case 1: full "City ST 12345" (or "[blank] ST 12345") on one row
                m = _CITY_ST_ZIP_RE.match(line)
                if m:
                    city_val  = _clean(m.group(1)) or pending_city
                    state_val = _valid_state(m.group(2))
                    zip_val   = m.group(3)
                    if state_val:
                        contributor_city       = city_val
                        contributor_state_code = state_val
                        contributor_zip        = zip_val
                        break
                # Case 2: zip-only row (5 digits, no state) and we already have state
                # e.g. previous row was "Tonganoxie KS", this row is "66086"
                if pending_state:
                    m_zip = _ZIP_ONLY_RE.match(line)
                    if m_zip:
                        contributor_city       = pending_city
                        contributor_state_code = pending_state
                        contributor_zip        = m_zip.group(1)
                        break
                # Case 3: "City ST" (no zip) — carry state forward to next row
                toks = line.split()
                if (toks and len(toks[-1]) == 2 and _valid_state(toks[-1])
                        and not any(c.isdigit() for c in line)):
                    pending_state = _valid_state(toks[-1])
                    pending_city  = _clean(" ".join(toks[:-1]))
                elif not any(c.isdigit() for c in line) and not _STREET_LIKE_RE.search(line):
                    # Plain city name — save in case next row has state+zip
                    pending_city = _clean(line)

            # ── Payment type (on anchor row) ──────────────────────────
            transaction_type = _row_text(anchor_row, A_TYPE_MIN, A_TYPE_MAX)

            # ── Occupation (on anchor row) ────────────────────────────
            occ_raw = _row_text(anchor_row, A_OCC_MIN, A_OCC_MAX)
            # Discard column-header fragments and dollar amounts (web-form PDFs place
            # the actual contribution amount at x≈399, inside the occupation range).
            occupation = ""
            if occ_raw and occ_raw not in ("Amount", "Individual", "Giving",
                                            "Other", "More", "Than", "$150"):
                if not _AMOUNT_RE.match(occ_raw):
                    occupation = occ_raw

            transactions.append({
                "date":              parsed_date,
                "contributor_name":  contributor_name,
                "contributor_city":  contributor_city,
                "contributor_state": contributor_state_code,
                "contributor_zip":   contributor_zip,
                "occupation":        occupation,
                "transaction_type":  transaction_type,
                "amount":            amount,
            })

    return transactions


# ====================== Schedule C parser ============================
#
# Kansas R&E PDF layout for each expenditure entry:
#
#   [purp row, y ≈ date_y - 26]  [PURP COL]  "Miscellaneous Halloween..."
#   [name row, y ≈ date_y - 13]  [NAME COL]  "Sams Club"    [PURP COL] "candy"
#   [date row, y ≈ date_y]       [DATE] MM/DD/YY | [NAME] "Not Available" | [AMT] $188.68
#   [addr row, y ≈ date_y + 13]  [NAME COL]  "NotAvailable NA"
#
# The payee NAME appears on the row(s) BEFORE the date anchor (same as Sch A).
# Purpose text appears in the purpose column on those same pre-anchor rows,
# plus possibly on the anchor row itself.

def _parse_schedule_c(pages, schedule_pages: list[int]) -> list[dict]:
    """
    Parse Schedule C (expenditures) from the given page indices.
    Returns list of raw expenditure dicts.
    """
    transactions = []

    for pi in schedule_pages:
        page  = pages[pi]
        words = _words(page)
        rows  = _cluster_rows(words)

        anchor_indices = []
        for ri, row in enumerate(rows):
            if any(w[0] < C_DATE_MAX and _DATE_RE.match(w[2]) for w in row):
                anchor_indices.append(ri)

        for idx, anchor_ri in enumerate(anchor_indices):
            end_ri     = anchor_indices[idx + 1] if idx + 1 < len(anchor_indices) else len(rows)
            anchor_row = rows[anchor_ri]

            # ── Date ─────────────────────────────────────────────────
            date_word = next(
                (w for w in anchor_row if w[0] < C_DATE_MAX and _DATE_RE.match(w[2])), None
            )
            if not date_word:
                continue
            parsed_date = _parse_date(date_word[2])
            if not parsed_date:
                continue

            # ── Amount ───────────────────────────────────────────────
            amount = ""
            for cr in rows[anchor_ri:min(anchor_ri + 3, end_ri)]:
                amt_words = [w for w in cr if w[0] >= C_AMT_MIN and _AMOUNT_RE.match(w[2])]
                if amt_words:
                    amount = _parse_amount(amt_words[0][2])
                    break

            # ── Payee name (row(s) BEFORE anchor) + purpose (same rows) ──
            name_parts: list[str] = []
            purp_parts: list[str] = []
            found_name = False
            for back_ri in range(anchor_ri - 1, max(-1, anchor_ri - 8), -1):
                back_row = rows[back_ri]
                if any(w[0] < C_DATE_MAX and _DATE_RE.match(w[2]) for w in back_row):
                    break   # hit previous anchor
                name_line = _row_text(back_row, C_NAME_MIN, C_NAME_MAX)
                purp_line = _row_text(back_row, C_PURP_MIN, C_PURP_MAX)
                if not name_line and not purp_line:
                    break   # true empty row = gap between entries
                if name_line:
                    if _SKIP_BACK_NAME_RE.match(name_line):
                        break   # column header / "Schedule C" / "Not Available"
                    if _CITY_ST_ZIP_RE.match(name_line):
                        break   # city/zip of previous entry
                    if found_name:
                        break   # already captured the name; don't cross into prev entry
                    if _STREET_LIKE_RE.search(name_line):
                        # Street row — skip but still collect purpose
                        if purp_line:
                            purp_parts.insert(0, purp_line)
                        continue
                    name_parts.insert(0, name_line)
                    found_name = True
                if purp_line:
                    purp_parts.insert(0, purp_line)

            payee_name = _clean(" ".join(name_parts))
            if any(kw in payee_name.upper()
                   for kw in ("TOTAL", "ITEMIZED", "UNITEMIZED")):
                continue

            # Purpose may also appear on the anchor row itself (rare but possible)
            t = _row_text(anchor_row, C_PURP_MIN, C_PURP_MAX)
            if t:
                purp_parts.append(t)

            purpose = _clean(" ".join(purp_parts))
            for hdr in ("Purpose of Expenditure or Disbursement",
                         "Purpose of Expenditure", "or Disbursement"):
                purpose = purpose.replace(hdr, "").strip()
            purpose = _clean(purpose)

            # ── Payee city / state / zip (rows after anchor) ─────────
            payee_city = payee_state_code = payee_zip = ""
            pending_city = ""
            pending_state = ""
            for cr in rows[anchor_ri + 1:end_ri]:
                line = _row_text(cr, C_NAME_MIN, C_NAME_MAX)
                if not line:
                    continue
                m = _CITY_ST_ZIP_RE.match(line)
                if m:
                    city_val  = _clean(m.group(1)) or pending_city
                    state_val = _valid_state(m.group(2))
                    zip_val   = m.group(3)
                    if state_val:
                        payee_city       = city_val
                        payee_state_code = state_val
                        payee_zip        = zip_val
                        break
                if pending_state:
                    m_zip = _ZIP_ONLY_RE.match(line)
                    if m_zip:
                        payee_city       = pending_city
                        payee_state_code = pending_state
                        payee_zip        = m_zip.group(1)
                        break
                toks = line.split()
                if (toks and len(toks[-1]) == 2 and _valid_state(toks[-1])
                        and not any(c.isdigit() for c in line)):
                    pending_state = _valid_state(toks[-1])
                    pending_city  = _clean(" ".join(toks[:-1]))
                elif not any(c.isdigit() for c in line) and not _STREET_LIKE_RE.search(line):
                    pending_city = _clean(line)

            transactions.append({
                "date":        parsed_date,
                "payee_name":  payee_name,
                "payee_city":  payee_city,
                "payee_state": payee_state_code,
                "payee_zip":   payee_zip,
                "purpose":     purpose,
                "amount":      amount,
            })

    return transactions


# ========================== PDF dispatcher ===========================

def _parse_pdf(pdf_path: Path, meta: dict) -> dict:
    """
    Open one R&E PDF and return:
        {header, contributions: [...], expenditures: [...]}
    meta: {office, election_year, district, candidate_name, period}
    """
    result = {
        "header":        {},
        "contributions": [],
        "expenditures":  [],
    }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return result

            pages = pdf.pages

            # ── Header from page 1 ────────────────────────────────────
            page1_text = pages[0].extract_text() or ""
            result["header"] = _parse_header(page1_text)

            # ── Classify pages by schedule ────────────────────────────
            sch_a_pages: list[int] = []
            sch_b_pages: list[int] = []
            sch_c_pages: list[int] = []

            page_texts: list[str] = []
            for i, page in enumerate(pages):
                pt = page.extract_text() or ""
                page_texts.append(pt)
                sch = _page_schedule(pt)
                if sch == "A":
                    sch_a_pages.append(i)
                elif sch == "B":
                    sch_b_pages.append(i)
                elif sch == "C":
                    sch_c_pages.append(i)
                # D is skipped

            # ── PLF/GLF "Last Minute" fallback ────────────────────────────
            # These filings (Pre-general / General Last-minute contributions)
            # are single-section reports with no "SCHEDULE A" header.  All
            # contribution rows appear on pages 2..n-1; page 1 is the cover
            # and the last page is a signature/declaration.
            if not sch_a_pages and not sch_b_pages and not sch_c_pages:
                period   = meta.get("period",   "").upper()
                filename = meta.get("filename", "").upper()
                if re.search(r"PLF|GLF", period) or re.search(r"PLF|GLF", filename):
                    # PLF/GLF "Last Minute" filings have no SCHEDULE A header.
                    # Parse all pages — the contribution parser naturally ignores
                    # non-contribution pages (cover, instructions, declaration)
                    # because they contain no date-anchors with matching amounts.
                    sch_a_pages = list(range(len(pages)))

            # ── Parse contributions (Sch A + B) ───────────────────────
            contribs = _parse_schedule_a(pages, sch_a_pages)
            inkind   = _parse_schedule_a(pages, sch_b_pages)
            for row in inkind:
                row["transaction_type"] = "In-Kind"

            # ── Duplicate-report dedup (content-based) ────────────────
            # Some Kansas amendments embed two or more full copies of each
            # schedule in a single PDF (e.g. SW01JX_amend1801.pdf has
            # Schedule A pages 1-7 repeated as pages 9-15).  Page-level
            # truncation (_first_block) caused false cuts when a PDF has
            # Schedule A → Schedule C → more Schedule A (a legitimate
            # multi-section layout).  Instead, deduplicate by content key
            # after parsing: same (date, amount, contributor, city) within
            # a single file = duplicate row from a repeated schedule block.
            def _dedup_rows(rows: list[dict], key_fields: list[str]) -> list[dict]:
                seen: set[tuple] = set()
                out: list[dict] = []
                for r in rows:
                    k = tuple(r.get(f, "") for f in key_fields)
                    if k not in seen:
                        seen.add(k)
                        out.append(r)
                return out

            contrib_key = ["date", "amount", "contributor_name", "contributor_city",
                           "transaction_type"]
            contribs = _dedup_rows(contribs, contrib_key)
            inkind   = _dedup_rows(inkind,   contrib_key)
            result["contributions"] = contribs + inkind

            # ── Parse expenditures (Sch C) ────────────────────────────
            expends = _parse_schedule_c(pages, sch_c_pages)
            expend_key = ["date", "amount", "payee_name", "payee_city"]
            result["expenditures"] = _dedup_rows(expends, expend_key)

    except Exception as e:
        # Non-fatal: log and return empty
        result["_error"] = str(e)

    return result


# ======================== Amendment resolution =======================

def _normalize_period(period: str) -> str:
    """
    Normalize a filing period code so that originals and their amendments
    collapse to the same key:

      "amend1807"  →  "1807"   (strip "amend" prefix)
      "201807"     →  "1807"   (strip leading "20" from 6-digit YYYYMM)
      "2018PLF"    →  "2018plf" (unchanged — special period, no amendment)
    """
    p = period.lower().strip()
    if p.startswith("amend"):
        return p[5:]                                # "amend1807" → "1807"
    if len(p) == 6 and p[:2] == "20" and p[2:].isdigit():
        return p[2:]                                # "201807" → "1807"
    return p


def _choose_files(manifest_rows: list[dict]) -> list[dict]:
    """
    For each (candidate_name, normalized_period) group, prefer the amendment
    PDF over the original when both exist.

    "amend1807" and "201807" both normalize to base period "1807", so an
    amendment correctly suppresses its original (fixing the Willis Hartman
    duplicate-contribution problem where both SW01KK_201807 and
    SW01KK_amend1807 were parsed independently).
    """
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in manifest_rows:
        key = (row["candidate_name"], _normalize_period(row["period"]))
        groups[key].append(row)

    chosen = []
    for key, rows in groups.items():
        amendments = [r for r in rows if "amend" in r["filename"].lower()]
        originals  = [r for r in rows if "amend" not in r["filename"].lower()]
        if amendments:
            chosen.extend(amendments)   # amendment supersedes original
        else:
            chosen.extend(originals)
    return chosen


# ============================== run ==================================

def run():
    log = get_logger("kansas", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    try:
        _run(log, t0)
    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


def _run(log, t0: float):
    # ── Load manifest ──────────────────────────────────────────────────
    if not MANIFEST.exists():
        print("ERROR: manifest not found — run the scraper first")
        sys.exit(1)

    with open(MANIFEST, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    # Only parse R&E PDFs that exist on disk
    manifest_rows = [
        r for r in all_rows
        if (RAW_DIR / r["filename"]).exists()
        and "amend" not in r["filename"].lower()  # handled separately by _choose_files
        or ("amend" in r["filename"].lower() and (RAW_DIR / r["filename"]).exists())
    ]
    manifest_rows = _choose_files(manifest_rows)
    # Also exclude AT / affidavit filenames (shouldn't be in manifest but just in case)
    manifest_rows = [
        r for r in manifest_rows
        if not re.search(r"_AT\.pdf$|_aff\w*\.pdf$", r["filename"], re.IGNORECASE)
    ]

    log.info(f"  {len(manifest_rows):,} PDFs to parse (after amendment dedup)")

    # ── Output writers ─────────────────────────────────────────────────
    contrib_path = CLEAN_DIR / "contributions.csv.gz"
    expend_path  = CLEAN_DIR / "expenditures.csv.gz"
    cand_path    = CLEAN_DIR / "candidates.csv.gz"
    comm_path    = CLEAN_DIR / "committees.csv.gz"

    contrib_f = gzip.open(contrib_path, "wt", newline="", encoding="utf-8")
    expend_f  = gzip.open(expend_path,  "wt", newline="", encoding="utf-8")

    contrib_w = csv.DictWriter(contrib_f, fieldnames=C.CONTRIBUTIONS,
                               extrasaction="ignore", restval="")
    expend_w  = csv.DictWriter(expend_f,  fieldnames=C.EXPENDITURES,
                               extrasaction="ignore", restval="")
    contrib_w.writeheader()
    expend_w.writeheader()

    # Accumulators for candidates / committees
    # Key: (normalized_name, office, district, election_year) → most recent meta
    candidates_seen: dict[tuple, dict] = {}
    # Track row numbers across all output files
    contrib_rownum = 0
    expend_rownum  = 0

    errors = 0

    # ── Parse each PDF ─────────────────────────────────────────────────
    for i, meta in enumerate(manifest_rows):
        filename     = meta["filename"]
        pdf_path     = RAW_DIR / filename
        office       = meta.get("office", "")
        election_year = _election_year_clean(meta.get("election_year", ""))
        district     = meta.get("district", "")
        cand_name_raw = meta.get("candidate_name", "")

        if (i + 1) % 100 == 0:
            log.info(f"  Parsed {i + 1:,}/{len(manifest_rows):,} PDFs …")

        parsed = _parse_pdf(pdf_path, meta)

        if "_error" in parsed:
            log.warning(f"    Error in {filename}: {parsed['_error']}")
            errors += 1
            continue

        # ── Resolve candidate name (PDF header > manifest) ─────────────
        hdr       = parsed["header"]
        cand_name = hdr.get("candidate_name") or utils.clean_name(cand_name_raw)
        if not cand_name:
            cand_name = utils.clean_name(cand_name_raw)
        else:
            cand_name = utils.clean_name(cand_name)

        pdf_office   = hdr.get("office") or office
        pdf_district = hdr.get("district") or district

        # Normalize district — strip leading zeros for consistency
        try:
            pdf_district = str(int(pdf_district))
        except (ValueError, TypeError):
            pdf_district = pdf_district or district

        # ── Register candidate ─────────────────────────────────────────
        cand_key = (cand_name, pdf_office, pdf_district, election_year)
        if cand_key not in candidates_seen:
            candidates_seen[cand_key] = {
                "state":          STATE,
                "candidate_name": cand_name,
                "office":         pdf_office,
                "district":       pdf_district,
                "election_year":  election_year,
                "party":          "",
                "state_filer_id": "",
                "raw_file":       filename,
                "row_num":        "",
            }

        # ── Contributions ──────────────────────────────────────────────
        committee_name = cand_name  # candidate's own campaign is the "committee"

        for txn in parsed["contributions"]:
            if not txn.get("amount") or not txn.get("date"):
                continue
            contrib_rownum += 1
            contrib_w.writerow({
                "state":             STATE,
                "committee_name":    committee_name,
                "amount":            txn["amount"],
                "date":              txn["date"],
                "transaction_type":  txn.get("transaction_type", ""),
                "contributor_name":  txn.get("contributor_name", ""),
                "contributor_city":  txn.get("contributor_city", ""),
                "contributor_state": txn.get("contributor_state", ""),
                "contributor_zip":   utils.clean_zip(txn.get("contributor_zip", "")),
                "occupation":        txn.get("occupation", ""),
                "candidate_name":    cand_name,
                "office":            pdf_office,
                "election_year":     election_year,
                "raw_file":          filename,
                "row_num":           contrib_rownum,
            })

        # ── Expenditures ───────────────────────────────────────────────
        for txn in parsed["expenditures"]:
            if not txn.get("amount") or not txn.get("date"):
                continue
            expend_rownum += 1
            expend_w.writerow({
                "state":          STATE,
                "committee_name": committee_name,
                "amount":         txn["amount"],
                "date":           txn["date"],
                "payee_name":     txn.get("payee_name", ""),
                "purpose":        txn.get("purpose", ""),
                "payee_city":     txn.get("payee_city", ""),
                "payee_state":    txn.get("payee_state", ""),
                "payee_zip":      utils.clean_zip(txn.get("payee_zip", "")),
                "candidate_name": cand_name,
                "office":         pdf_office,
                "election_year":  election_year,
                "raw_file":       filename,
                "row_num":        expend_rownum,
            })

    contrib_f.close()
    expend_f.close()
    log.info(f"  Contributions: {contrib_rownum:,}   Expenditures: {expend_rownum:,}   Errors: {errors}")

    # ── Write candidates.csv.gz ────────────────────────────────────────
    cand_rows = []
    for ri, (key, meta_row) in enumerate(candidates_seen.items(), start=1):
        # Split name into first/last heuristically (Last, First or First Last)
        full = meta_row["candidate_name"]
        if "," in full:
            parts  = full.split(",", 1)
            c_last = _clean(parts[0])
            c_first = _clean(parts[1])
        else:
            tokens  = full.split()
            c_first = tokens[0] if tokens else ""
            c_last  = tokens[-1] if len(tokens) > 1 else ""

        cand_rows.append({
            **meta_row,
            "person_id":      "",    # filled by assign_person_ids
            "candidate_first": c_first,
            "candidate_last":  c_last,
            "incumbent":       "",
            "jurisdiction":    "",
            "raw_file":        meta_row["raw_file"],
            "row_num":         ri,
        })

    with gzip.open(cand_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.CANDIDATES,
                           extrasaction="ignore", restval="")
        w.writeheader()
        w.writerows(cand_rows)

    n_cands = utils.assign_person_ids(cand_path, id_model="name_hash")
    log.info(f"  Candidates: {n_cands:,}")

    # ── Write committees.csv.gz ────────────────────────────────────────
    # One committee row per candidate (their campaign committee).
    with gzip.open(comm_path, "wt", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=C.COMMITTEES,
                           extrasaction="ignore", restval="")
        w.writeheader()
        for ri, row in enumerate(cand_rows, start=1):
            w.writerow({
                "state":          STATE,
                "person_id":      "",         # filled by assign_committee_person_ids
                "committee_name": row["candidate_name"],
                "committee_type": "Candidate",
                "election_year":  row["election_year"],
                "candidate_name": row["candidate_name"],
                "state_filer_id": "",
                "raw_file":       row["raw_file"],
                "row_num":        ri,
            })

    n_comm_matched = utils.assign_committee_person_ids(comm_path, cand_path)
    log.info(f"  Committees: {len(cand_rows):,}  (matched {n_comm_matched:,} to candidates)")

    duration = round(time.perf_counter() - t0, 1)
    log._emit("parse_completed",
              status="completed",
              duration_s=duration,
              contributions=contrib_rownum,
              expenditures=expend_rownum,
              candidates=n_cands,
              errors=errors)
    log.info(f"Done in {duration}s")


# ============================= CLI ===================================

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
