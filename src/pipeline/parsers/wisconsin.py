"""
parsers/wisconsin.py — Transform Wisconsin Sunshine raw data into the 5 normalized relations.

Input:  data/Wisconsin/raw/
  transactions_{from}_{to}[_amt{lo}-{hi}].csv   — date-windowed transaction chunks
  reports_{from}_{to}.csv                       — report index (updated-at windows)
  committees.csv                                — full registrant list ("registrants" tab)

Output: data/Wisconsin/cleaned/
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz

Schema notes
────────────
  One transaction feed, two directions
    • Sunshine publishes contributions and disbursements in a single table with
      Contributor / Payee columns on both sides and a `Transaction Type`
      discriminator ("Contribution", "Disbursement"). Rows are routed on that
      type, falling back — for any type the source adds later — to "is the
      registrant the payer?", which is what actually distinguishes money out
      from money in. Unrouted types are counted and reported, never guessed
      silently.
    • Loan / debt / incurred-obligation types go to loans_debts.

  Registrant is the filer
    • `Registrant ID` / `Registrant Name` is the filing committee on every row,
      whichever side of the transaction it sits on → committee_name.
    • Contributions: contributor_* from the Contributor columns.
      Disbursements: payee_* from the Payee columns.
    • Independent expenditures name the supported/opposed committee in
      `Related Entity` and the stance in `Support Stance` — both are folded into
      `purpose` so they survive into the aggregate DB, which drops category.

  Chunk files, no dedup
    • The scraper emits disjoint windows (and disjoint amount bands when a day
      has to be split), so chunks concatenate without deduplicating IDs — which
      matters at 13M rows, where an ID set would cost ~1 GB of memory. Overlaps
      would only come from stale files left by an interrupted run, so the
      parser checks the window ranges encoded in the filenames and warns.

  Candidates and committees
    • committees.csv is the only source for registrant status/party/candidate
      name; office and district aren't in it, so they're accumulated from the
      transaction pass (`Related Office` / `Related District`, keeping the row
      from the most recent ballot event) and joined on Registrant ID.
    • Registrant IDs persist across cycles (party committees still carry their
      1978 registration date), so id_model="person".
    • `Registrant Type` is hierarchical, e.g. "PAC  -> Labor",
      "State Candidate  -> Personal Campaign Committee". Whitespace is
      collapsed to a single "X -> Y" form; canonicalization happens at
      aggregate time via src/aliases/committee_types.csv.

  Reports
    • The report index supplies the amended flag. `Reports` on a transaction row
      reads "2025 July Continuing (ID: 7946)" — possibly several — and the first
      ID is used as filing_id and looked up for Amended.
    • Reports are optional: if no reports_*.csv are on disk, filing_id is still
      populated and amended is left blank.

Data notes
──────────
  - Addresses are single multi-line blocks in committees.csv and reports_*.csv
    ("street\\ncity, State zip, Country"); city/ZIP are pulled off the last line.
  - Contributor/payee state arrives as either "WI" or "Wisconsin" — full names
    are mapped back to two-letter codes via src/aliases/states.csv.
  - Some registrants' address/email/phone read "Redacted pursuant to
    Wis. Stat. § 19.55(2)(cm)2." — treated as blank.
  - Anything the source leaves as "-" or "N/A" is treated as blank.
"""

import csv
import gzip
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Wisconsin" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Wisconsin" / "cleaned"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "WI"
MAX_VALID_YEAR = date.today().year + 2

# ============================= constants ==============================

# Placeholders Sunshine uses for "no value"
NULL_TOKENS = {"", "-", "N/A", "NA", "NONE", "NULL"}

# Statutory redaction notice that appears in place of address/email/phone
REDACTED_PREFIX = "REDACTED PURSUANT TO"

# Transaction Type routing. Matched on lowercased substrings so the source can
# add qualifiers ("In-Kind Contribution") without breaking the routing.
CONTRIBUTION_MARKERS = ("contribution", "receipt", "transfer in", "income")
EXPENDITURE_MARKERS  = ("disbursement", "expenditure", "transfer out", "expense")
LOAN_MARKERS         = ("loan", "debt", "obligation")

# Filename → window range, for the stale-overlap check
_CHUNK_RE = re.compile(
    r"^transactions_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})(?:_amt.*)?\.csv$"
)

# "2025 July Continuing (ID: 7946)" → 7946
_REPORT_ID_RE = re.compile(r"\(ID:\s*(\d+)\)")

# Trailing "city, State zip[, Country]" line of an address block
_CITY_STATE_ZIP_RE = re.compile(
    r"^(?P<city>.+?),\s*(?P<state>[A-Za-z][A-Za-z .]*?)\s+"
    r"(?P<zip>\d{5}(?:-\d{4}|\d{4})?)\b"
)


def _load_state_names() -> dict[str, str]:
    """Full state name (upper) → two-letter abbr, from src/aliases/states.csv."""
    out: dict[str, str] = {}
    path = PROJECT_ROOT / "src" / "aliases" / "states.csv"
    if path.exists():
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name = (row.get("name") or "").strip().upper()
                abbr = (row.get("abbr") or "").strip().upper()
                if name and abbr:
                    out[name] = abbr
    # states.csv covers the 50 states only
    out.update({
        "DISTRICT OF COLUMBIA": "DC", "WASHINGTON DC": "DC",
        "PUERTO RICO": "PR", "GUAM": "GU", "VIRGIN ISLANDS": "VI",
        "AMERICAN SAMOA": "AS", "NORTHERN MARIANA ISLANDS": "MP",
    })
    return out


_STATE_NAMES = _load_state_names()


# ============================== helpers ==============================

def clean(val) -> str:
    """Strip whitespace, collapse runs of spaces, and blank out null placeholders."""
    v = re.sub(r"\s+", " ", (val or "").strip())
    if v.upper() in NULL_TOKENS or v.upper().startswith(REDACTED_PREFIX):
        return ""
    return v


def parse_amount(val: str) -> str:
    """'$1,234.56' / '(500.00)' / '1234.56' → plain numeric string; '' on failure."""
    v = (val or "").strip().replace("$", "").replace(",", "")
    if not v:
        return ""
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]          # parentheses = negative
    try:
        float(v)
        return v
    except ValueError:
        return ""


def parse_date(val: str) -> str:
    """MM/DD/YYYY or YYYY-MM-DD → YYYY-MM-DD; '' on failure or implausible year."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1990 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore",
                        restval="")
    w.writeheader()
    return fh, w


def raw_files(pattern: str) -> list[Path]:
    """Non-empty raw files matching a glob, in filename order."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def state_abbr(val: str) -> str:
    """'WI' → 'WI', 'Wisconsin' → 'WI', anything unrecognized → '' (validator checks this)."""
    v = clean(val).upper()
    if not v:
        return ""
    if len(v) == 2:
        return v
    return _STATE_NAMES.get(v, "")


def parse_address_block(block: str) -> tuple[str, str]:
    """
    Pull (city, zip) out of a multi-line address block.

    Blocks look like "790 N. Water St\\nSte. 2500\\nMilwaukee, WI 532023509",
    sometimes with a trailing ", United States". Returns ('', '') when the last
    line doesn't parse — WI has a fair number of malformed entries
    (e.g. "MADISON, CA WI2428") and a wrong city is worse than a blank one.
    """
    raw = (block or "").strip()
    if not raw or raw.upper().startswith(REDACTED_PREFIX):
        return "", ""
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return "", ""
    last = re.sub(r",\s*(United States|USA|US)\.?$", "", lines[-1],
                  flags=re.IGNORECASE).strip()
    m = _CITY_STATE_ZIP_RE.match(last)
    if not m:
        return "", ""
    return clean(m.group("city")), utils.clean_zip(m.group("zip"))


def election_year(row: dict) -> str:
    """
    Election year for a transaction row.

    Prefers the ballot event it was reported against, then the 4-digit year in
    the event name ("2026 Fall Pre-Primary"), then the transaction date.
    """
    ev_date = parse_date(row.get("Related Ballot Event Date", ""))
    if ev_date:
        return ev_date[:4]
    m = re.match(r"\s*(\d{4})\b", row.get("Related Ballot Event Name", "") or "")
    if m and 1990 <= int(m.group(1)) <= MAX_VALID_YEAR:
        return m.group(1)
    tx_date = parse_date(row.get("Date", ""))
    return tx_date[:4] if tx_date else ""


def first_report_id(val: str) -> str:
    """First report ID in a 'Reports' cell → '7946'; '' when absent."""
    m = _REPORT_ID_RE.search(val or "")
    return m.group(1) if m else ""


def route(row: dict) -> str:
    """
    Decide which relation a transaction row belongs to.

    Returns "contributions", "expenditures", "loans" or "" (unroutable).
    """
    tx_type = clean(row.get("Transaction Type", "")).lower()
    category = clean(row.get("Transaction Category", "")).lower()

    if any(m in tx_type for m in LOAN_MARKERS) or any(m in category for m in LOAN_MARKERS):
        return "loans"
    if any(m in tx_type for m in CONTRIBUTION_MARKERS):
        return "contributions"
    if any(m in tx_type for m in EXPENDITURE_MARKERS):
        return "expenditures"

    # Unknown type — infer direction from which side the filer is on. Money
    # leaving the registrant is a disbursement; anything else is a receipt.
    # Matched on name, not ID: the Contributor/Payee columns carry *entity* IDs
    # from a different ID space than Registrant ID (the same committee is entity
    # 16226 and registrant 0106162), so the IDs are not comparable.
    reg_name   = utils.clean_name(row.get("Registrant Name", ""))
    payer_name = utils.clean_name(
        row.get("Contributor Name (-> Related Payer Name if applicable)", ""))
    if reg_name and payer_name == reg_name:
        return "expenditures"
    return "contributions" if tx_type else ""


def ie_purpose(row: dict) -> str:
    """
    Expenditure purpose, with independent-expenditure context appended.

    `category` is dropped from the aggregate DB, so the stance and the
    supported/opposed committee are folded into purpose to keep them queryable.
    """
    parts = [clean(row.get("Transaction Purpose", ""))]
    stance   = clean(row.get("Support Stance", ""))
    related  = clean(row.get("Related Entity", ""))
    if stance and related:
        parts.append(f"[{stance} {related}]")
    elif related:
        parts.append(f"[re: {related}]")
    comment = clean(row.get("Comment", ""))
    if comment:
        parts.append(comment)
    return " ".join(p for p in parts if p)


def warn_overlapping_chunks(log, files: list[Path]):
    """
    Warn if two transaction chunks cover the same dates.

    The scraper emits disjoint windows, so an overlap means a stale file from an
    interrupted or re-split run is still on disk and its rows will be counted
    twice. Amount-banded chunks share a date range by design and are excluded.
    """
    spans: list[tuple[str, str, str]] = []
    for f in files:
        m = _CHUNK_RE.match(f.name)
        if m and "_amt" not in f.name:
            spans.append((m.group(1), m.group(2), f.name))
    spans.sort()
    for (a_from, a_to, a_name), (b_from, b_to, b_name) in zip(spans, spans[1:]):
        if b_from <= a_to:
            log.warning(
                f"overlapping raw chunks — {a_name} ({a_from}→{a_to}) and "
                f"{b_name} ({b_from}→{b_to}) both cover {b_from}; rows in the "
                f"overlap will be double counted. Re-run the scraper with --force "
                f"for the affected years."
            )


# ============================ raw registries =========================

def load_reports(log) -> dict[str, int]:
    """
    report ID → amended flag (1/0), from every reports_*.csv chunk.

    ~130k rows across all windows; the value is a single int per report, so the
    whole index costs a few MB.
    """
    index: dict[str, int] = {}
    files = raw_files("reports_*.csv")
    for path in files:
        ft = time.perf_counter()
        count = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row in csv.DictReader(f):
                rid = clean(row.get("ID", ""))
                if not rid:
                    continue
                index[rid] = 1 if clean(row.get("Amended", "")).upper() == "YES" else 0
                count += 1
        log.file_parsed(path.name, "reports", count,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=path.stat().st_size)
    if files:
        log.registry_loaded("reports_*.csv", entries=len(index), relation="reports")
    else:
        log.warning("no reports_*.csv found — `amended` will be blank")
    return index


def load_registrants(log) -> dict[str, dict]:
    """Registrant ID → registrant record, from committees.csv."""
    path = RAW_DIR / "committees.csv"
    registry: dict[str, dict] = {}
    if not path.exists():
        log.warning(
            "committees.csv not found — committees and candidates will be built "
            "from transaction rows only (no status, party or candidate names)"
        )
        return registry

    ft = time.perf_counter()
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row_num, row in enumerate(csv.DictReader(f), start=2):
            reg_id = clean(row.get("Registrant ID", ""))
            if not reg_id:
                continue
            city, zipcode = parse_address_block(row.get("Registrant Address", ""))
            status = clean(row.get("Registrant Status", ""))
            registry[reg_id] = {
                "committee_name":  clean(row.get("Registrant Name", "")),
                "committee_type":  clean(row.get("Registrant Type", "")),
                "candidate_name":  clean(row.get("Candidate Name", "")),
                "party":           clean(row.get("Registrant Party", "")),
                "registered":      parse_date(row.get("Registrant Registration date", "")),
                "city":            city,
                "zip":             zipcode,
                # Sunshine's only status signal; "Terminated" is the inactive one
                "active":          "0" if status.upper() == "TERMINATED" else "1",
                "row_num":         row_num,
            }
    log.file_parsed(path.name, "committees", len(registry),
                    duration_s=round(time.perf_counter() - ft, 2),
                    bytes=path.stat().st_size)
    log.registry_loaded(path.name, entries=len(registry), relation="committees")
    return registry


# ================================ run =================================

def run():
    log = get_logger("wisconsin", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    total_loans         = 0
    committees_written  = 0
    candidates_written  = 0
    file_handles        = []

    try:
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cont_fh, expn_fh, cand_fh, cmte_fh, loan_fh]

        reports    = load_reports(log)
        registrants = load_registrants(log)

        # Office / district / party per registrant, harvested from transactions.
        # committees.csv has no office columns, so this is the only place a
        # candidate's office and district appear. Keyed by registrant ID, keeping
        # whichever row has the latest ballot event.
        offices: dict[str, dict] = {}
        # Registrants that show up in transactions but not in committees.csv
        orphans: dict[str, dict] = {}
        unrouted: dict[str, int] = {}
        skipped_no_amount = 0

        chunks = raw_files("transactions_*.csv")
        if not chunks:
            log.warning("no transactions_*.csv found in raw/ — run the scraper first")
        warn_overlapping_chunks(log, chunks)

        for path in chunks:
            ft = time.perf_counter()
            n_cont = n_expn = n_loan = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(
                        tqdm(reader, desc=f"  {path.name}", unit="row",
                             dynamic_ncols=True, leave=False), start=2):

                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        skipped_no_amount += 1
                        continue

                    reg_id    = clean(row.get("Registrant ID", ""))
                    reg       = registrants.get(reg_id, {})
                    cmte_name = reg.get("committee_name") or clean(row.get("Registrant Name", ""))
                    cand_name = reg.get("candidate_name", "")
                    tx_date   = parse_date(row.get("Date", ""))
                    elec_year = election_year(row)
                    report_id = first_report_id(row.get("Reports", ""))
                    amended   = reports.get(report_id, "")
                    office    = clean(row.get("Related Office", ""))
                    district  = clean(row.get("Related District", ""))

                    # ── Registrant metadata harvested from the row ──────────
                    if reg_id:
                        if office or district:
                            prev = offices.get(reg_id)
                            if prev is None or (elec_year or "") >= prev.get("election_year", ""):
                                offices[reg_id] = {
                                    "office":        office,
                                    "district":      district,
                                    "election_year": elec_year,
                                }
                        if reg_id not in registrants and reg_id not in orphans:
                            orphans[reg_id] = {
                                "committee_name": clean(row.get("Registrant Name", "")),
                                "committee_type": clean(row.get("Registrant Type", "")),
                                "party":          clean(row.get("Registrant Party", "")),
                                "raw_file":       path.name,
                                "row_num":        row_num,
                            }

                    dest = route(row)

                    if dest == "contributions":
                        cont_w.writerow({
                            "state":             STATE,
                            "committee_name":    cmte_name,
                            "amount":            amount,
                            "date":              tx_date,
                            "transaction_type":  clean(row.get("Transaction Category", ""))
                                                 or clean(row.get("Transaction Type", "")),
                            "contributor_name":  clean(row.get(
                                "Contributor Name (-> Related Payer Name if applicable)", "")),
                            "contributor_type":  clean(row.get("Contributor Entity Type", "")),
                            "contributor_city":  clean(row.get("Contributor City", "")),
                            "contributor_state": state_abbr(row.get("Contributor State", "")),
                            "contributor_zip":   utils.clean_zip(clean(row.get("Contributor Zip", ""))),
                            "employer":          "",
                            "occupation":        clean(row.get("Contributor Occupation", "")),
                            "candidate_name":    cand_name,
                            "office":            office,
                            "election_year":     elec_year,
                            "amended":           amended,
                            "filing_id":         report_id,
                            "raw_file":          path.name,
                            "row_num":           row_num,
                        })
                        n_cont += 1

                    elif dest == "expenditures":
                        expn_w.writerow({
                            "state":            STATE,
                            "committee_name":   cmte_name,
                            "amount":           amount,
                            "date":             tx_date,
                            "transaction_type": clean(row.get("Transaction Category", ""))
                                                or clean(row.get("Transaction Type", "")),
                            "payee_name":       clean(row.get("Payee Name", "")),
                            "purpose":          ie_purpose(row),
                            "category":         clean(row.get("Transaction Category", "")),
                            "payee_city":       clean(row.get("Payee City", "")),
                            "payee_state":      state_abbr(row.get("Payee State", "")),
                            "payee_zip":        utils.clean_zip(clean(row.get("Payee Zip", ""))),
                            "candidate_name":   cand_name,
                            "office":           office,
                            "election_year":    elec_year,
                            "amended":          amended,
                            "filing_id":        report_id,
                            "raw_file":         path.name,
                            "row_num":          row_num,
                        })
                        n_expn += 1

                    elif dest == "loans":
                        # The registrant is the borrower; the other party is
                        # whichever side isn't the registrant.
                        counterparty = clean(row.get(
                            "Contributor Name (-> Related Payer Name if applicable)", ""))
                        if utils.clean_name(counterparty) == utils.clean_name(cmte_name):
                            counterparty = clean(row.get("Payee Name", ""))
                            cp_city  = clean(row.get("Payee City", ""))
                            cp_state = state_abbr(row.get("Payee State", ""))
                            cp_zip   = clean(row.get("Payee Zip", ""))
                        else:
                            cp_city  = clean(row.get("Contributor City", ""))
                            cp_state = state_abbr(row.get("Contributor State", ""))
                            cp_zip   = clean(row.get("Contributor Zip", ""))
                        loan_w.writerow({
                            "state":              STATE,
                            "committee_name":     cmte_name,
                            "original_amount":    amount,
                            "date":               tx_date,
                            "record_type":        clean(row.get("Transaction Category", ""))
                                                  or clean(row.get("Transaction Type", "")),
                            "counterparty_name":  counterparty,
                            "counterparty_city":  cp_city,
                            "counterparty_state": cp_state,
                            "counterparty_zip":   utils.clean_zip(cp_zip),
                            "candidate_name":     cand_name,
                            "election_year":      elec_year,
                            "amended":            amended,
                            "filing_id":          report_id,
                            "raw_file":           path.name,
                            "row_num":            row_num,
                        })
                        n_loan += 1

                    else:
                        key = clean(row.get("Transaction Type", "")) or "(blank)"
                        unrouted[key] = unrouted.get(key, 0) + 1

            log.file_parsed(path.name, "transactions", n_cont + n_expn + n_loan,
                            duration_s=round(time.perf_counter() - ft, 2),
                            bytes=path.stat().st_size)
            total_contributions += n_cont
            total_expenditures  += n_expn
            total_loans         += n_loan

        # ── Committees and candidates ─────────────────────────────────
        # Written after the transaction pass so office/district are available.
        for reg_id, reg in registrants.items():
            info = offices.get(reg_id, {})
            cmte_w.writerow({
                "state":          STATE,
                "committee_name": reg["committee_name"],
                "committee_type": reg["committee_type"],
                "election_year":  info.get("election_year", ""),
                "candidate_name": reg["candidate_name"],
                "treasurer_name": "",
                "city":           reg["city"],
                "zip":            reg["zip"],
                "active":         reg["active"],
                "state_filer_id": reg_id,
                "raw_file":       "committees.csv",
                "row_num":        reg["row_num"],
            })
            committees_written += 1

            if not reg["candidate_name"]:
                continue
            full = utils.clean_name(reg["candidate_name"])
            first, last = _split_person_name(full)
            cand_w.writerow({
                "state":           STATE,
                "candidate_name":  full,
                "candidate_first": first,
                "candidate_last":  last,
                "office":          info.get("office", ""),
                "district":        info.get("district", ""),
                "jurisdiction":    "",
                "party":           reg["party"],
                "election_year":   info.get("election_year", ""),
                "incumbent":       "",
                "state_filer_id":  reg_id,
                "raw_file":        "committees.csv",
                "row_num":         reg["row_num"],
            })
            candidates_written += 1

        # Registrants seen only in transactions (terminated before the current
        # registrant list was generated, or filed by a non-registrant payer).
        for reg_id, orph in orphans.items():
            info = offices.get(reg_id, {})
            cmte_w.writerow({
                "state":          STATE,
                "committee_name": orph["committee_name"],
                "committee_type": orph["committee_type"],
                "election_year":  info.get("election_year", ""),
                "candidate_name": "",
                "treasurer_name": "",
                "city":           "",
                "zip":            "",
                "active":         "",
                "state_filer_id": reg_id,
                "raw_file":       orph["raw_file"],
                "row_num":        orph["row_num"],
            })
            committees_written += 1

        if orphans:
            log.enrichment_summary(
                relation="committees",
                matched=len(registrants),
                unmatched=len(orphans),
                note="registrants present in transactions but not in committees.csv",
            )
        if unrouted:
            top = sorted(unrouted.items(), key=lambda kv: -kv[1])[:10]
            log.warning(
                "unrouted transaction types (dropped): "
                + ", ".join(f"{k}={v:,}" for k, v in top)
            )
        if skipped_no_amount:
            log.info(f"  rows skipped for unparseable amount: {skipped_no_amount:,}")

        # ── Close handles before person-ID assignment ─────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # Registrant IDs are stable across cycles → the ID *is* the person key
        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="person")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions,
                        role="output", bytes=_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,
                        role="output", bytes=_bytes("expenditures.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   total_loans,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written)

    except KeyboardInterrupt:
        log.warning("Interrupted")
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  loans=total_loans, committees=committees_written,
                  candidates=candidates_written,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass


def _split_person_name(full: str) -> tuple[str, str]:
    """
    'TIM PAUL REPPEN' → ('TIM', 'REPPEN');  'REPPEN, TIM' → ('TIM', 'REPPEN').

    Sunshine stores candidate names in natural "First [Middle] Last" order, but
    a minority are entered "Last, First" — both are handled. Returns
    (first, last).
    """
    name = utils.clean_name(full)
    if not name:
        return "", ""
    if "," in name:
        last, _, rest = name.partition(",")
        first = rest.strip().split(" ")[0] if rest.strip() else ""
        return first, last.strip()
    parts = name.split(" ")
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], parts[-1]


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
