"""
parsers/california.py — Transform California CAL-ACCESS raw TSVs into the
5 normalized relations.

Input:  data/California/raw/
  CVR_CAMPAIGN_DISCLOSURE_CD.tsv  — cover records (one per filing/amendment)
  FILERNAME_CD.tsv                — filer name/address registry
  FILER_TO_FILER_TYPE_CD.tsv      — filer type codes, party, active flag
  RCPT_CD.tsv                     — contributions received  (~19 M rows)
  EXPN_CD.tsv                     — expenditures made       (~15 M rows)
  LOAN_CD.tsv                     — loans received/made     (~96 K rows)
  DEBT_CD.tsv                     — debts owed              (~715 K rows)

Output: data/California/cleaned/
  contributions.csv, expenditures.csv, committees.csv,
  candidates.csv, loans_debts.csv

Amendment dedup
───────────────
  CAL-ACCESS stores every amendment as a separate set of rows sharing the
  same FILING_ID but with increasing AMEND_ID.  We pre-load CVR to build
  {filing_id: max_amend_id} and skip any row where the row's AMEND_ID is
  less than that maximum.

Encoding
────────
  All TSVs are latin-1 (ISO-8859-1).  Date strings look like
  '1/20/2000 12:00:00 AM'.

Amount format
─────────────
  Already plain numeric strings ('109.89', '2000') — no $ or commas.
"""

import csv
import re
import sys
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C

csv.field_size_limit(sys.maxsize)

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "California" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "California" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "CA"
MAX_VALID_YEAR = date.today().year + 2
ENCODING       = "latin-1"


# ── Helpers ────────────────────────────────────────────────────────────────────
def clean(val) -> str:
    return (val or "").strip().rstrip("\r")


def parse_amount(val: str) -> str:
    """Plain numeric string → validated float string, '' on failure."""
    v = (val or "").strip()
    if not v:
        return ""
    try:
        float(v)
        return v
    except ValueError:
        return ""


_DATE_FMTS = (
    "%m/%d/%Y %I:%M:%S %p",   # '1/20/2000 12:00:00 AM'  (CAL-ACCESS standard)
    "%m/%d/%Y",                # fallback plain date
    "%Y-%m-%d",                # ISO fallback
)

def parse_date(val: str) -> str:
    """'M/D/YYYY H:MM:SS AM/PM' → YYYY-MM-DD, '' on failure or implausible year."""
    v = (val or "").strip().rstrip("\r")
    if not v:
        return ""
    for fmt in _DATE_FMTS:
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1970 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def build_name(last: str, first: str) -> str:
    last  = (last  or "").strip()
    first = (first or "").strip()
    if last and first:
        return f"{last}, {first}"
    return last or first


# Placeholder values CAL-ACCESS writes into CAND_NAML for non-candidate filings
_NULL_CAND_NAMES = {"n/a", "na", "none", "unknown", "-", ""}

def clean_cand_name(name: str) -> str:
    """Return name if it looks like a real candidate name, else ''."""
    return name if name.lower().strip() not in _NULL_CAND_NAMES else ""


def build_cand_name(last: str, first: str) -> str:
    """
    Like build_name but:
      - Returns '' if the last-name field is a placeholder (N/A, NA, etc.)
      - Normalizes to title case so 'NEWSOM, GAVIN' and 'Newsom, Gavin'
        collapse to the same string across filings.
    """
    if (last or "").strip().lower() in _NULL_CAND_NAMES:
        return ""
    return build_name(last, first).title()


class _NulFilter:
    """Wraps a text file and strips NUL bytes, which appear in some CAL-ACCESS TSVs."""
    def __init__(self, fh):
        self._fh = fh
    def __iter__(self):
        for line in self._fh:
            if "\x00" in line:
                line = line.replace("\x00", "")
            yield line
    def read(self, size=-1):
        return self._fh.read(size).replace("\x00", "")
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self._fh.__exit__(*args)


def open_tsv(path: Path):
    """Return a NUL-filtered reader for a CAL-ACCESS TSV."""
    fh = open(path, newline="", encoding=ENCODING, errors="replace")
    return _NulFilter(fh)


def open_writer(filename: str, fieldnames: list):
    fh = open(CLEAN_DIR / filename, "w", newline="", encoding="utf-8")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


def raw_file(name: str) -> Path:
    return RAW_DIR / name


# ── Phase 1: Pre-load CVR ──────────────────────────────────────────────────────
def load_cvr() -> tuple[dict, dict]:
    """
    Two-pass read to keep memory usage low:
      Pass 1 — build max_amend: {filing_id: max_amend_id_int}
      Pass 2 — re-read, extract minimal metadata only from rows at max amend

    Returns:
      max_amend : {filing_id_str: max_amend_id_int}
      cvr_meta  : {filing_id_str: dict}
                  keys: filer_id, elect_year, office, cand_name, cmte_type,
                        district, juris, tres_name, filer_city, filer_zip
    """
    path = raw_file("CVR_CAMPAIGN_DISCLOSURE_CD.tsv")

    # ── Pass 1: max AMEND_ID per FILING_ID ────────────────────────────────────
    print("  Pre-loading CVR (pass 1/2)...", end=" ", flush=True)
    max_amend: dict[str, int] = {}
    with open_tsv(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            fid = clean(row.get("FILING_ID", ""))
            if not fid:
                continue
            try:
                amid = int(clean(row.get("AMEND_ID", "0")) or "0")
            except ValueError:
                amid = 0
            if amid > max_amend.get(fid, -1):
                max_amend[fid] = amid
    print(f"{len(max_amend):,} filings")

    # ── Pass 2: extract minimal metadata from max-amend rows only ─────────────
    print("  Pre-loading CVR (pass 2/2)...", end=" ", flush=True)
    cvr_meta: dict[str, dict] = {}
    with open_tsv(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            fid = clean(row.get("FILING_ID", ""))
            if not fid:
                continue
            try:
                amid = int(clean(row.get("AMEND_ID", "0")) or "0")
            except ValueError:
                amid = 0
            if amid != max_amend.get(fid, 0):
                continue

            elect_date = parse_date(clean(row.get("ELECT_DATE", "")))
            cand_name  = build_cand_name(
                clean(row.get("CAND_NAML", "")),
                clean(row.get("CAND_NAMF", "")),
            )
            cvr_meta[fid] = {
                "filer_id":   clean(row.get("FILER_ID", "")),
                "elect_year": elect_date[:4] if elect_date else "",
                "office":     clean(row.get("OFFICE_CD", "")),
                "cand_name":  cand_name,
                "cmte_type":  clean(row.get("CMTTE_TYPE", "")),
                "district":   clean(row.get("DIST_NO", "")),
                "juris":      clean(row.get("JURIS_CD", "")),
                "tres_name":  build_name(
                    clean(row.get("TRES_NAML", "")),
                    clean(row.get("TRES_NAMF", "")),
                ),
                "filer_city": clean(row.get("FILER_CITY", "")),
                "filer_zip":  clean(row.get("FILER_ZIP4", "")),
            }

    print(f"{len(cvr_meta):,} rows extracted")
    return max_amend, cvr_meta


# ── Phase 2: Pre-load FILERNAME_CD ────────────────────────────────────────────
def load_filername() -> dict:
    """
    Returns {filer_id: dict} — name, city, zip, status, filer_type.
    When a filer_id appears multiple times, keep the ACTIVE record if one
    exists; otherwise keep the last seen.
    """
    path = raw_file("FILERNAME_CD.tsv")
    print("  Pre-loading FILERNAME_CD...", end=" ", flush=True)
    registry: dict[str, dict] = {}

    with open_tsv(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            fid    = clean(row.get("FILER_ID", ""))
            if not fid:
                continue
            status = clean(row.get("STATUS", "")).upper()
            name   = build_name(
                clean(row.get("NAML", "")),
                clean(row.get("NAMF", "")),
            )
            entry = {
                "filer_id":   fid,
                "xref_id":    clean(row.get("XREF_FILER_ID", "")),
                "name":       name,
                "filer_type": clean(row.get("FILER_TYPE", "")),
                "status":     status,
                "city":       clean(row.get("CITY", "")),
                "zip":        clean(row.get("ZIP4", "")).strip(),
                "active":     "1" if status == "ACTIVE" else ("0" if status else ""),
            }
            prev = registry.get(fid)
            if prev is None or status == "ACTIVE":
                registry[fid] = entry

    print(f"{len(registry):,} filers")
    return registry


# ── Phase 3: Pre-load FILER_TO_FILER_TYPE_CD ──────────────────────────────────
_PARTY_LABELS = {
    "16012": "Democratic",
    "16013": "Republican",
    "16020": "Green",
    "16023": "Libertarian",
    "16025": "American Independent",
    "16027": "Peace and Freedom",
    "16029": "Reform",
    "16999": "Other",
    "0":     "",
}

def load_filer_types() -> dict:
    """
    Returns {filer_id: dict} — party, active.
    We use this table only for party affiliation and active status;
    the numeric FILER_TYPE codes reference a session table we don't have
    so we don't try to interpret them as committee type labels.
    Keep the most recent EFFECT_DT row per filer_id.
    """
    path = raw_file("FILER_TO_FILER_TYPE_CD.tsv")
    print("  Pre-loading FILER_TO_FILER_TYPE_CD...", end=" ", flush=True)
    registry: dict[str, dict] = {}
    best_dt:  dict[str, str]  = {}

    with open_tsv(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            fid = clean(row.get("FILER_ID", ""))
            if not fid:
                continue
            eff_dt    = parse_date(clean(row.get("EFFECT_DT", "")))
            if eff_dt <= best_dt.get(fid, ""):
                continue
            best_dt[fid]  = eff_dt
            active    = clean(row.get("ACTIVE", "")).upper()
            party_raw = clean(row.get("PARTY_CD", ""))
            registry[fid] = {
                "party":  _PARTY_LABELS.get(party_raw, ""),
                "active": "1" if active in ("T", "Y") else ("0" if active else ""),
            }

    print(f"{len(registry):,} entries")
    return registry


# ── Main ───────────────────────────────────────────────────────────────────────
def run():
    # ── Pre-load lookup tables ─────────────────────────────────────────────────
    max_amend, cvr_meta = load_cvr()
    filername   = load_filername()
    filer_types = load_filer_types()

    # ── Open output writers ────────────────────────────────────────────────────
    cand_fh, cand_w = open_writer("candidates.csv",    C.CANDIDATES)
    cmte_fh, cmte_w = open_writer("committees.csv",    C.COMMITTEES)
    cont_fh, cont_w = open_writer("contributions.csv", C.CONTRIBUTIONS)
    expn_fh, expn_w = open_writer("expenditures.csv",  C.EXPENDITURES)
    loan_fh, loan_w = open_writer("loans_debts.csv",   C.LOANS_DEBTS)

    # ── Candidates: extract unique filers with candidate info from CVR ─────────
    # Prefer the most-recent filing per FILER_ID (highest FILING_ID)
    print("  candidates     CVR...", end=" ", flush=True)
    seen_cands: dict[str, dict] = {}   # filer_id → best meta dict
    best_filing: dict[str, int] = {}   # filer_id → highest FILING_ID seen

    for filing_id, meta in cvr_meta.items():
        if not clean_cand_name(meta["cand_name"]):
            continue
        filer_id = meta["filer_id"]
        if not filer_id:
            continue
        try:
            fid_int = int(filing_id)
        except ValueError:
            fid_int = 0
        if fid_int > best_filing.get(filer_id, -1):
            best_filing[filer_id] = fid_int
            seen_cands[filer_id]  = meta

    cand_count = 0
    for filer_id, meta in seen_cands.items():
        name = meta["cand_name"]
        if "," in name:
            last, _, first = name.partition(",")
            last, first = last.strip(), first.strip()
        else:
            last, first = name, ""
        ft = filer_types.get(filer_id, {})
        cand_w.writerow({
            "state":           STATE,
            "candidate_name":  name,
            "candidate_first": first,
            "candidate_last":  last,
            "office":          meta["office"],
            "district":        meta["district"],
            "jurisdiction":    meta["juris"],
            "party":           ft.get("party", ""),
            "election_year":   meta["elect_year"],
            "status":          "",
            "incumbent":       "",
            "raw_file":        "CVR_CAMPAIGN_DISCLOSURE_CD.tsv",
            "row_num":         "",
        })
        cand_count += 1
    print(f"{cand_count:,} candidates")

    # ── Committees: from FILERNAME_CD enriched with filer_types ───────────────
    # FILERNAME_CD.FILER_TYPE is the authoritative committee type text
    # (e.g. "RECIPIENT COMMITTEE", "MAJOR DONOR/INDEPENDENT EXPENDITURE
    # COMMITTEE", "CLIENT").  FILER_TO_FILER_TYPE_CD's numeric FILER_TYPE codes
    # reference a session table we don't have, so we use it only for party
    # and active-status enrichment.
    print("  committees     FILERNAME_CD...", end=" ", flush=True)
    cmte_count = 0
    for fid, fn in filername.items():
        ft        = filer_types.get(fid, {})
        cmte_type = fn["filer_type"]            # text from FILERNAME_CD
        active    = ft.get("active", "") or fn["active"]
        # candidate_name: if this filer is a candidate, pull from cvr_meta via best_filing
        cand_meta = None
        if fid in seen_cands:
            cand_meta = seen_cands[fid]
        cand_name = cand_meta["cand_name"] if cand_meta else ""

        cmte_w.writerow({
            "state":          STATE,
            "state_filer_id": fid,
            "committee_name": fn["name"],
            "committee_type": cmte_type,
            "candidate_name": cand_name,
            "treasurer_name": "",
            "city":           fn["city"],
            "zip":            fn["zip"],
            "active":         active,
        })
        cmte_count += 1
    print(f"{cmte_count:,} committees")

    # ── Helper: resolve filer_id from a transaction row ───────────────────────
    def resolve_filer_id(row: dict, filing_id: str) -> str:
        """CMTE_ID field if present, else FILER_ID from CVR."""
        cmte_id = clean(row.get("CMTE_ID", ""))
        if cmte_id:
            return cmte_id
        return cvr_meta.get(filing_id, {}).get("filer_id", "")

    def committee_name(filer_id: str) -> str:
        return filername.get(filer_id, {}).get("name", "")

    def is_valid_amend(row: dict) -> bool:
        """Return True if this row's AMEND_ID matches the max for its FILING_ID."""
        fid = clean(row.get("FILING_ID", ""))
        if not fid:
            return False
        try:
            amid = int(clean(row.get("AMEND_ID", "0")) or "0")
        except ValueError:
            amid = 0
        return amid == max_amend.get(fid, 0)

    # ── Contributions (RCPT_CD) ────────────────────────────────────────────────
    path = raw_file("RCPT_CD.tsv")
    print(f"  contributions  RCPT_CD.tsv...", end=" ", flush=True)
    total_cont = 0
    skipped_amend = 0
    with open_tsv(path) as f:
        for row_num, row in enumerate(csv.DictReader(f, delimiter="\t"), start=2):
            if not is_valid_amend(row):
                skipped_amend += 1
                continue
            amount = parse_amount(row.get("AMOUNT", ""))
            if not amount:
                continue

            filing_id  = clean(row.get("FILING_ID", ""))
            filer_id   = resolve_filer_id(row, filing_id)
            cmte_name  = committee_name(filer_id)
            meta       = cvr_meta.get(filing_id, {})
            elect_year = meta.get("elect_year", "")
            office     = clean(row.get("OFFICE_CD", "")) or meta.get("office", "")
            cand_name  = (
                build_cand_name(clean(row.get("CAND_NAML", "")), clean(row.get("CAND_NAMF", "")))
                or meta.get("cand_name", "")
            )

            cont_w.writerow({
                "state":             STATE,
                "state_filer_id":    filer_id,
                "committee_name":    cmte_name,
                "contributor_name":  build_name(
                    clean(row.get("CTRIB_NAML", "")),
                    clean(row.get("CTRIB_NAMF", "")),
                ),
                "amount":            amount,
                "date":              parse_date(clean(row.get("RCPT_DATE", ""))),
                "transaction_type":  clean(row.get("TRAN_TYPE", "")),
                "contributor_type":  clean(row.get("ENTITY_CD", "")),
                "contributor_city":  clean(row.get("CTRIB_CITY", "")),
                "contributor_state": clean(row.get("CTRIB_ST", "")),
                "contributor_zip":   clean(row.get("CTRIB_ZIP4", "")),
                "employer":          clean(row.get("CTRIB_EMP", "")),
                "occupation":        clean(row.get("CTRIB_OCC", "")),
                "candidate_name":    cand_name,
                "office":            office,
                "election_year":     elect_year,
                "filing_id":         clean(row.get("TRAN_ID", "")),
                "amended":           "",
                "raw_file":          "RCPT_CD.tsv",
                "row_num":           row_num,
            })
            total_cont += 1
    print(f"{total_cont:,} rows  ({skipped_amend:,} superseded amendments skipped)")

    # ── Expenditures (EXPN_CD) ────────────────────────────────────────────────
    path = raw_file("EXPN_CD.tsv")
    print(f"  expenditures   EXPN_CD.tsv...", end=" ", flush=True)
    total_expn = 0
    skipped_amend = 0
    with open_tsv(path) as f:
        for row_num, row in enumerate(csv.DictReader(f, delimiter="\t"), start=2):
            if not is_valid_amend(row):
                skipped_amend += 1
                continue
            amount = parse_amount(row.get("AMOUNT", ""))
            if not amount:
                continue

            filing_id = clean(row.get("FILING_ID", ""))
            filer_id  = resolve_filer_id(row, filing_id)
            cmte_name = committee_name(filer_id)
            meta      = cvr_meta.get(filing_id, {})
            elect_year = meta.get("elect_year", "")
            office     = clean(row.get("OFFICE_CD", "")) or meta.get("office", "")
            cand_name  = (
                build_cand_name(clean(row.get("CAND_NAML", "")), clean(row.get("CAND_NAMF", "")))
                or meta.get("cand_name", "")
            )

            expn_w.writerow({
                "state":            STATE,
                "state_filer_id":   filer_id,
                "committee_name":   cmte_name,
                "payee_name":       build_name(
                    clean(row.get("PAYEE_NAML", "")),
                    clean(row.get("PAYEE_NAMF", "")),
                ),
                "amount":           amount,
                "date":             parse_date(clean(row.get("EXPN_DATE", ""))),
                "transaction_type": clean(row.get("EXPN_CODE", "")),
                "purpose":          clean(row.get("EXPN_DSCR", "")),
                "category":         clean(row.get("FORM_TYPE", "")),
                "payee_city":       clean(row.get("PAYEE_CITY", "")),
                "payee_state":      clean(row.get("PAYEE_ST", "")),
                "payee_zip":        clean(row.get("PAYEE_ZIP4", "")),
                "candidate_name":   cand_name,
                "office":           office,
                "election_year":    elect_year,
                "filing_id":        clean(row.get("TRAN_ID", "")),
                "amended":          "",
                "raw_file":         "EXPN_CD.tsv",
                "row_num":          row_num,
            })
            total_expn += 1
    print(f"{total_expn:,} rows  ({skipped_amend:,} superseded amendments skipped)")

    # ── Loans (LOAN_CD) ────────────────────────────────────────────────────────
    path = raw_file("LOAN_CD.tsv")
    print(f"  loans          LOAN_CD.tsv...", end=" ", flush=True)
    total_loan = 0
    skipped_amend = 0
    with open_tsv(path) as f:
        for row_num, row in enumerate(csv.DictReader(f, delimiter="\t"), start=2):
            if not is_valid_amend(row):
                skipped_amend += 1
                continue
            # LOAN_AMT1 = original loan amount (LOAN_AMT2 = outstanding, etc.)
            amount = parse_amount(row.get("LOAN_AMT1", ""))
            if not amount:
                continue

            filing_id = clean(row.get("FILING_ID", ""))
            filer_id  = resolve_filer_id(row, filing_id)
            cmte_name = committee_name(filer_id)
            meta      = cvr_meta.get(filing_id, {})

            loan_w.writerow({
                "state":              STATE,
                "state_filer_id":     filer_id,
                "committee_name":     cmte_name,
                "record_type":        clean(row.get("LOAN_TYPE", "")) or "LOAN",
                "counterparty_name":  build_name(
                    clean(row.get("LNDR_NAML", "")),
                    clean(row.get("LNDR_NAMF", "")),
                ),
                "counterparty_city":  clean(row.get("LOAN_CITY", "")),
                "counterparty_state": clean(row.get("LOAN_ST", "")),
                "counterparty_zip":   clean(row.get("LOAN_ZIP4", "")),
                "original_amount":    amount,
                "date":               parse_date(clean(row.get("LOAN_DATE1", ""))),
                "candidate_name":     meta.get("cand_name", ""),
                "election_year":      meta.get("elect_year", ""),
                "filing_id":          clean(row.get("TRAN_ID", "")),
                "amended":            "",
                "raw_file":           "LOAN_CD.tsv",
                "row_num":            row_num,
            })
            total_loan += 1
    print(f"{total_loan:,} rows  ({skipped_amend:,} superseded amendments skipped)")

    # ── Debts (DEBT_CD) ────────────────────────────────────────────────────────
    # DEBT_CD has no date column — use '' for date
    path = raw_file("DEBT_CD.tsv")
    print(f"  debts          DEBT_CD.tsv...", end=" ", flush=True)
    total_debt = 0
    skipped_amend = 0
    with open_tsv(path) as f:
        for row_num, row in enumerate(csv.DictReader(f, delimiter="\t"), start=2):
            if not is_valid_amend(row):
                skipped_amend += 1
                continue
            # Use AMT_INCUR (new amount incurred) as primary; fall back to BEG_BAL
            amount = parse_amount(row.get("AMT_INCUR", "")) \
                     or parse_amount(row.get("BEG_BAL", ""))
            if not amount:
                continue

            filing_id = clean(row.get("FILING_ID", ""))
            filer_id  = resolve_filer_id(row, filing_id)
            cmte_name = committee_name(filer_id)
            meta      = cvr_meta.get(filing_id, {})

            loan_w.writerow({
                "state":              STATE,
                "state_filer_id":     filer_id,
                "committee_name":     cmte_name,
                "record_type":        "DEBT",
                "counterparty_name":  build_name(
                    clean(row.get("PAYEE_NAML", "")),
                    clean(row.get("PAYEE_NAMF", "")),
                ),
                "counterparty_city":  clean(row.get("PAYEE_CITY", "")),
                "counterparty_state": clean(row.get("PAYEE_ST", "")),
                "counterparty_zip":   clean(row.get("PAYEE_ZIP4", "")),
                "original_amount":    amount,
                "date":               "",
                "candidate_name":     meta.get("cand_name", ""),
                "election_year":      meta.get("elect_year", ""),
                "filing_id":          clean(row.get("TRAN_ID", "")),
                "amended":            "",
                "raw_file":           "DEBT_CD.tsv",
                "row_num":            row_num,
            })
            total_debt += 1
    print(f"{total_debt:,} rows  ({skipped_amend:,} superseded amendments skipped)")

    # ── Close all writers ──────────────────────────────────────────────────────
    for fh in (cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh):
        fh.close()

    print(f"\nCalifornia: done.")
    print(f"  {cand_count:,} candidates  {cmte_count:,} committees")
    print(f"  {total_cont:,} contributions  {total_expn:,} expenditures")
    print(f"  {total_loan + total_debt:,} loans/debts ({total_loan:,} loans + {total_debt:,} debts)")


if __name__ == "__main__":
    run()
