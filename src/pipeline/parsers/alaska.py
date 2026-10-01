"""
Alaska.py — Parse Alaska APOC raw exports into canonical cleaned CSVs.

Raw files (all in data/Alaska/raw/):
  CDIncome_YYYY.csv      — contributions received
  CDExpense_YYYY.csv     — expenditures made
  CDCandidates_all.csv   — candidate registry
  GRForms_YYYY.csv       — group/committee registrations (bulk export)
  gr_details.csv         — group registration detail pages (scraped, richer)
  CRForms_YYYY.csv       — candidate registrations (bulk export, added 2026-09-23)
  cr_details.csv         — candidate registration detail pages (scraped, richer)

Output (data/Alaska/cleaned/):
  contributions.csv.gz, expenditures.csv.gz, committees.csv.gz,
  candidates.csv.gz, loans_debts.csv.gz
"""

import csv
import gzip
import re
import sys
import time
from pathlib import Path
from datetime import datetime, date

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.reporting.logger import get_logger

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import columns as C
import utils

csv.field_size_limit(sys.maxsize)

# =============================== Paths ================================
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR      = PROJECT_ROOT / "data" / "Alaska" / "raw"
CLEAN_DIR    = PROJECT_ROOT / "data" / "Alaska" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "AK"
MAX_VALID_YEAR = date.today().year + 2


# ============================== Helpers ===============================
def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def committee_key(name: str) -> str:
    """Normalize a committee name to a lowercase, punctuation-free key for dedup/matching."""
    name = clean(name).lower()

    # normalize punctuation/spacing
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)

    return name.strip()


def parse_amount(val: str) -> str:
    """Parse a dollar amount string to a plain numeric string; parentheses become negative. Returns '' on failure."""
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
    """MM/DD/YYYY or YYYY-MM-DD → YYYY-MM-DD. Returns '' on failure or out-of-range year."""
    v = (val or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(v, fmt)
            if d.year < 1970 or d.year > MAX_VALID_YEAR:
                return ""
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def build_name(last: str, first: str) -> str:
    """Combine last/first into 'First Last'. Treats 'N/A' first names as absent (Alaska org placeholder)."""
    last  = (last  or "").strip()
    first = (first or "").strip()
    if first.upper() in ("N/A", "NA", "N.A."):
        first = ""
    if first and last:
        return f"{first} {last}"
    return first or last


def contributor_type_from_first_name(first: str) -> str:
    """'Individual' if a real first name is present, 'Non-Individual'
    otherwise -- same convention/literal strings as ohio.py and
    michigan.py use for this identical raw-data shape (a separate First
    Name column distinct from Last/Business Name).

    2026-09-24: contributions.csv's contributor_type used to be a direct
    copy of the raw CDIncome file's "Filer Type" column -- but that
    column describes the RECEIVING committee's own registration type
    ("Candidate" vs "Group" vs "Entity"), not the actual contributor's
    nature. Every donor to any AK candidate committee was showing up as
    contributor_type="Candidate" (a PAC or an individual, indistinguishably)
    and every donor to a PAC/ballot-measure committee as "Group" --
    silently breaking the Individual vs. Non-Individual donor split
    everywhere downstream (materialized_views.sql's
    candidate_contribution_top/latest_individual_donors, which filter on
    contributor_type = 'Individual' exactly). Confirmed live 2026-09-24:
    real named individual donors to Jonathan Kreiss-Tomkins's campaign
    (Anthropic researchers Drake Thomas, Daniel Ziegler, Jan Leike among
    them) were all mislabeled "Candidate" instead of "Individual".

    Verified against AK's own raw data before relying on this: of
    141,165 raw CDIncome rows, 139,086 have a populated First Name and
    2,079 don't; every no-first-name row sampled was a real PAC/org name
    ("Alaska Laborers Local 341 PAC", "Alaska AFL-CIO Gaming Account",
    etc.), not a misformatted person. Same first/last-name-column shape
    Ohio and Michigan already classify this way (and whose correctly-
    labeled output is itself part of what trained
    contributor_type_nb.py's model) -- that NB classifier is for a state
    like Florida whose raw export has no separated name field at all to
    check; AK doesn't need it. Reuses build_name()'s own N/A-placeholder
    handling (Alaska's org placeholder in the First Name column) so a
    First Name of "N/A"/"NA"/"N.A." doesn't get misread as a real name."""
    first = (first or "").strip()
    if first.upper() in ("N/A", "NA", "N.A."):
        first = ""
    return "Individual" if first else "Non-Individual"


def strip_html_br(val: str) -> str:
    """Some AK IE Expenditures exports concatenate the filer's officer
    name onto the group name with a literal HTML tag -- e.g. 'Alaska
    Republican Party<br/>Strutz, Christy Patrice' -- confirmed present in
    12,543/41,871 (~30%) of IEExpenditures Filer Name values (2012-era
    rows especially), confined to that one column (Recipient, Candidate/
    Proposition, and IEContributions' Filer/Filer Name/Contributor are all
    clean). Keep only the group/committee name that precedes the tag.
    """
    return re.split(r"<br\s*/?>", val or "", maxsplit=1, flags=re.IGNORECASE)[0].strip()


def normalize_candidate(val: str) -> str:
    """Strip whitespace from a raw candidate string."""
    return (val or "").strip()


def year_from_filename(path: Path) -> str:
    """Extract the first 4-digit year from a filename, e.g. CDIncome_2022.csv → '2022'."""
    m = re.search(r"(\d{4})", path.name)
    return m.group(1) if m else ""


def raw_files(pattern: str) -> list[Path]:
    """Return non-empty raw files matching a glob pattern, sorted by name."""
    return sorted(
        (f for f in RAW_DIR.glob(pattern) if f.stat().st_size > 0),
        key=lambda p: p.name,
    )


def open_writer(filename: str, fieldnames: list):
    """Open a gzipped CSV writer in CLEAN_DIR; extra fields are dropped, missing fields default to ''."""
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames,
                        extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# ========================= CR detail registry =========================
def load_cr_registry() -> dict[str, dict]:
    """
    Returns dict keyed by committee_key(first + " " + last).
    Also indexes by committee_key(committee_name) when that field is populated,
    so "Andy Josephson for State House" can match if needed.
    Most-recent filing per candidate wins.
    """
    path = RAW_DIR / "cr_details.csv"
    if not path.exists():
        return {}

    registry: dict[str, dict] = {}

    def _update(key: str, row: dict) -> None:
        existing = registry.get(key)
        if existing is None:
            registry[key] = row
            return
        try:
            new_date = datetime.strptime(row["submission_date"],      "%m/%d/%Y")
            old_date = datetime.strptime(existing["submission_date"], "%m/%d/%Y")
            if new_date > old_date:
                registry[key] = row
        except (ValueError, KeyError):
            pass

    with open(path, newline="", encoding="utf-8") as f:
        for row_num, row in enumerate(csv.DictReader(f), start=2):
            first = clean(row.get("candidate_first", ""))
            last  = clean(row.get("candidate_last",  ""))
            if not (first or last):
                continue

            row["_raw_file"] = "cr_details.csv"
            row["_row_num"]  = row_num

            # Primary key: legal first + last (no middle, matches CDIncome filer names)
            _update(committee_key(first + " " + last), row)

            # Secondary: campaign committee name when populated
            cmte = clean(row.get("committee_name", ""))
            if cmte:
                _update(committee_key(cmte), row)

    return registry


# ========================= CR forms bulk registry ======================
def load_cr_forms_registry() -> dict[str, dict]:
    """
    Bulk analog of load_cr_registry() below, built from CRForms_YYYY.csv
    bulk exports (safe Select-Year/Search/Export downloads, same pattern
    as GRForms) instead of the slow, DataDome-prone per-ID cr_details.csv
    sweep. Same dict shape as load_cr_registry() (candidate_display_name/
    treasurer_name/city/zip/submission_date) so it can use the identical
    matching logic where it's applied. Most-recent Submitted date wins
    per key, same tie-break as load_cr_registry().
    """
    registry: dict[str, dict] = {}

    def _update(key: str, row: dict) -> None:
        existing = registry.get(key)
        if existing is None:
            registry[key] = row
            return
        try:
            new_date = datetime.strptime(row["submission_date"],      "%m/%d/%Y")
            old_date = datetime.strptime(existing["submission_date"], "%m/%d/%Y")
            if new_date > old_date:
                registry[key] = row
        except (ValueError, KeyError):
            pass

    for path in raw_files("CRForms_*.csv"):
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                first = clean(row.get("First Name", ""))
                last  = clean(row.get("Last Name",  ""))
                if not (first or last):
                    continue

                entry = {
                    "candidate_display_name": clean(row.get("Display Name",   "")),
                    "treasurer_name":         clean(row.get("Treasurer Name", "")),
                    "city":                   clean(row.get("City", "")),
                    "zip":                    clean(row.get("Zip",  "")),
                    "submission_date":        clean(row.get("Submitted", "")),
                    "_raw_file":              path.name,
                    "_row_num":               row_num,
                }

                _update(committee_key(first + " " + last), entry)

                cmte = clean(row.get("Committee", ""))
                if cmte:
                    _update(committee_key(cmte), entry)

    return registry


# ========================= GR detail registry =========================
def load_gr_registry() -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Returns (name_registry, abbr_registry).

    name_registry : keyed by committee_key(group_name) — primary match path.
    abbr_registry : keyed by committee_key(abbreviation) — fallback for groups
                    whose filer name in transactions matches their APOC
                    abbreviation rather than their full registered name.
    Both keep the most-recent filing per group.
    """
    path = RAW_DIR / "gr_details.csv"
    if not path.exists():
        return {}, {}

    name_registry: dict[str, dict] = {}
    abbr_registry: dict[str, dict] = {}

    def _update(registry: dict, key: str, row: dict) -> None:
        existing = registry.get(key)
        if existing is None:
            registry[key] = row
            return
        try:
            new_date = datetime.strptime(row["submission_date"],      "%m/%d/%Y")
            old_date = datetime.strptime(existing["submission_date"], "%m/%d/%Y")
            if new_date > old_date:
                registry[key] = row
        except (ValueError, KeyError):
            pass

    with open(path, newline="", encoding="utf-8") as f:
        for row_num, row in enumerate(csv.DictReader(f), start=2):
            name = clean(row.get("group_name", ""))
            if not name:
                continue

            row["_raw_file"] = "gr_details.csv"
            row["_row_num"]  = row_num

            _update(name_registry, committee_key(name), row)

            abbr = clean(row.get("abbreviation", ""))
            if abbr and len(abbr) >= 2:
                _update(abbr_registry, committee_key(abbr), row)

    return name_registry, abbr_registry

# ================================ Main ================================
def run():
    log = get_logger("alaska", "parse")
    t0  = time.perf_counter()
    log.info("Starting Alaska parser")
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    committees: dict[str, dict] = {}
    cand_count = 0

    file_handles = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    [c for c in C.COMMITTEES if c != "active"])
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        def register_committee(name: str, ctype: str):
            key = committee_key(name)
            if key not in committees:
                committees[key] = {
                    "state":          STATE,
                    # Default to the filer name so candidates/PCCs are always
                    # joinable to contributions. Groups get this overwritten with
                    # their APOC abbreviation (GRForms) or numeric GR ID
                    # (gr_details), which is higher-quality.
                    "state_filer_id": name,
                    "committee_name": name,
                    "committee_type": ctype,
                    "candidate_name": "",
                    "treasurer_name": "",
                    "city":           "",
                    "zip":            "",
                }

        # GR detail registry (loaded first — highest priority)
        gr_registry, gr_abbr_registry = load_gr_registry()
        if gr_registry:
            _gr_path = RAW_DIR / "gr_details.csv"
            log.registry_loaded(
                "gr_details.csv",
                entries=len(gr_registry),
                relation="committees",
                bytes=_gr_path.stat().st_size if _gr_path.exists() else 0,
            )

        cr_registry = load_cr_registry()
        if cr_registry:
            _cr_path = RAW_DIR / "cr_details.csv"
            log.registry_loaded(
                "cr_details.csv",
                entries=len(cr_registry),
                relation="committees",
                bytes=_cr_path.stat().st_size if _cr_path.exists() else 0,
            )

        # Candidates
        cand_path = RAW_DIR / "CDCandidates_all.csv"
        ft = time.perf_counter()
        if cand_path.exists() and cand_path.stat().st_size > 0:
            with open(cand_path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    name = normalize_candidate(row.get("Candidate", ""))
                    if not name:
                        continue
                    if "," in name:
                        last, _, first = name.partition(",")
                        last, first = last.strip(), first.strip()
                    else:
                        last, first = name, ""
                    clean_first = utils.clean_name(first)
                    clean_last  = utils.clean_name(last)
                    full_name   = f"{clean_first} {clean_last}".strip() if clean_first else clean_last

                    # AK governor/lt.-governor joint-ticket rows list BOTH
                    # running mates in one raw row, e.g. raw "Candidate"
                    # "Kreiss-Tomkins / Johnson, Jonathan / Zachary" ->
                    # clean_first="Jonathan / Zachary", clean_last=
                    # "Kreiss-Tomkins / Johnson". cloud/supabase/transform/
                    # matching.py's _last_name_of() naively takes the LAST
                    # whitespace token as a surname, which for a smashed
                    # joint string like this always lands on the RUNNING
                    # MATE's surname ("Johnson"), never the top-of-ticket
                    # candidate's own ("Kreiss-Tomkins") -- breaking both
                    # display and last-name matching for every joint-ticket
                    # row (confirmed live 2026-09-24: Jonathan Kreiss-Tomkins,
                    # a real, well-funded 2026 Governor candidate, showed up
                    # on the dashboard only as the unrecognizable "Jonathan /
                    # Zachary Kreiss-Tomkins / Johnson", $0 raised, because
                    # of this). state_filer_id stays the FULL joint string
                    # (still a unique, stable per-registration key, and
                    # that's what any committee_candidate_map.csv curation
                    # for this participant must keep using) -- only
                    # candidate_name/first/last are narrowed to the top-of-
                    # ticket person so display and last-name matching both
                    # resolve to the actual candidate instead of their
                    # running mate.
                    display_first, display_last, display_name = clean_first, clean_last, full_name
                    # Delimiter is inconsistent in the raw source -- real
                    # examples seen: "Kreiss-Tomkins / Johnson, Jonathan /
                    # Zachary" (slash, spaced), "Begich/Hnilicka, Thomas/
                    # Julia" (slash, unspaced), "Taylor/ English,
                    # Tregarrick/ Candice" (slash, inconsistently spaced),
                    # "Heilala \ Sumner, Matt \ Jesse" (backslash) -- so
                    # split on either slash with optional surrounding
                    # whitespace rather than a single literal separator.
                    joint_re = re.compile(r"\s*[/\\]\s*")
                    if joint_re.search(clean_first) and joint_re.search(clean_last):
                        first_parts = [p.strip() for p in joint_re.split(clean_first) if p.strip()]
                        last_parts  = [p.strip() for p in joint_re.split(clean_last) if p.strip()]
                        if (len(first_parts) == len(last_parts) and len(first_parts) >= 2
                                and first_parts[0] and last_parts[0]):
                            display_first = first_parts[0]
                            display_last  = last_parts[0]
                            display_name  = f"{display_first} {display_last}".strip()

                    cand_w.writerow({
                        "state":           STATE,
                        "state_filer_id":  full_name,
                        "candidate_name":  display_name,
                        "candidate_first": display_first,
                        "candidate_last":  display_last,
                        "office":          utils.clean_name(row.get("Office", "")),
                        "district":        "",
                        "jurisdiction":    utils.clean_name(row.get("Election", "")),
                        "party":           utils.clean_name(row.get("Party", "")),
                        "election_year":   clean(row.get("Year", "")),
                        "incumbent":       "",
                        "raw_file":        cand_path.name,
                        "row_num":         row_num,
                    })
                    cand_count += 1
        log.file_parsed("CDCandidates_all.csv", "candidates", cand_count,
                        duration_s=time.perf_counter() - ft,
                        bytes=cand_path.stat().st_size if cand_path.exists() else 0)

        # Contributions (CDIncome)
        # Dedup per file on (contributor, amount, date, committee), keeping the
        # row with the highest Result number (most recent amendment).
        for path in raw_files("CDIncome_*.csv"):
            ft   = time.perf_counter()
            seen: dict[tuple, dict] = {}
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        continue
                    filer      = clean(row.get("Name", ""))
                    filer_type = clean(row.get("Filer Type", ""))
                    register_committee(filer, filer_type)
                    contributor = build_name(
                        row.get("Last/Business Name", ""),
                        row.get("First Name", ""),
                    )
                    date_val = parse_date(row.get("Date", ""))
                    result   = clean(row.get("Result", ""))
                    key = (contributor, amount, date_val, filer)
                    prev = seen.get(key)
                    if prev is None or (result.isdigit() and
                            (not prev["filing_id"].isdigit() or
                             int(result) > int(prev["filing_id"]))):
                        seen[key] = {
                            "state":             STATE,
                            "committee_name":    utils.clean_name(filer),
                            "contributor_name":  utils.clean_name(contributor),
                            "amount":            amount,
                            "date":              date_val,
                            "transaction_type":  clean(row.get("Transaction Type", "")),
                            "contributor_type":  contributor_type_from_first_name(row.get("First Name", "")),
                            "contributor_city":  clean(row.get("City", "")),
                            "contributor_state": clean(row.get("State", "")),
                            "contributor_zip":   clean(row.get("Zip", "")),
                            "employer":          clean(row.get("Employer", "")),
                            "occupation":        clean(row.get("Occupation", "")),
                            "candidate_name":    utils.clean_name(filer) if filer_type == "Candidate" else "",
                            "office":            utils.clean_name(row.get("Office", "")),
                            "election_year":     clean(row.get("Report Year", year_from_filename(path))),
                            "filing_id":         result,
                            "amended":           "",
                            "raw_file":          path.name,
                            "row_num":           row_num,
                        }
            for out_row in seen.values():
                cont_w.writerow(out_row)
            count = len(seen)
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)
            total_contributions += count

        # Expenditures (CDExpense)
        for path in raw_files("CDExpense_*.csv"):
            ft   = time.perf_counter()
            seen: dict[tuple, dict] = {}
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        continue
                    filer      = clean(row.get("Name", ""))
                    filer_type = clean(row.get("Filer Type", ""))
                    register_committee(filer, filer_type)
                    payee    = build_name(
                        row.get("Last/Business Name", ""),
                        row.get("First Name", ""),
                    )
                    date_val = parse_date(row.get("Date", ""))
                    result   = clean(row.get("Result", ""))
                    key = (payee, amount, date_val, filer)
                    prev = seen.get(key)
                    if prev is None or (result.isdigit() and
                            (not prev["filing_id"].isdigit() or
                             int(result) > int(prev["filing_id"]))):
                        seen[key] = {
                            "state":            STATE,
                            "committee_name":   utils.clean_name(filer),
                            "payee_name":       utils.clean_name(payee),
                            "amount":           amount,
                            "date":             date_val,
                            "transaction_type": clean(row.get("Transaction Type", "")),
                            "purpose":          clean(row.get("Purpose of Expenditure", "")),
                            "category":         clean(row.get("Payment Type", "")),
                            "payee_city":       clean(row.get("City", "")),
                            "payee_state":      clean(row.get("State", "")),
                            "payee_zip":        clean(row.get("Zip", "")),
                            "candidate_name":   utils.clean_name(filer) if filer_type == "Candidate" else "",
                            "office":           utils.clean_name(row.get("Office", "")),
                            "election_year":    clean(row.get("Report Year", year_from_filename(path))),
                            "filing_id":        result,
                            "amended":          "",
                            "raw_file":         path.name,
                            "row_num":          row_num,
                        }
            for out_row in seen.values():
                expn_w.writerow(out_row)
            count = len(seen)
            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)
            total_expenditures += count

        # Committees: enrich from GRForms bulk exports
        # GRForms CSVs are secondary to gr_details; they fill gaps for groups
        # that appear in transactions but weren't hit by the detail scrape.
        for path in raw_files("GRForms_*.csv"):
            ft         = time.perf_counter()
            file_count = 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row in csv.DictReader(f):
                    name = clean(row.get("Name", ""))
                    if not name:
                        continue

                    key = committee_key(name)
                    entry = committees.get(key) or {
                        "state": STATE,
                        "state_filer_id": "",
                        "committee_name": name,
                        "candidate_name": "",
                    }


                    # Only overwrite fields that are still blank
                    if not entry.get("committee_type"):
                        entry["committee_type"] = (
                            " — ".join(filter(None, [
                                clean(row.get("Type", "")),
                                clean(row.get("Subtype", "")),
                            ]))
                        )
                    if not entry.get("treasurer_name"):
                        entry["treasurer_name"] = clean(row.get("Treasurer Name", ""))
                    if not entry.get("city"):
                        entry["city"] = clean(row.get("City", ""))
                    if not entry.get("zip"):
                        entry["zip"]  = clean(row.get("Zip", ""))
                    if not entry.get("state_filer_id"):
                        entry["state_filer_id"] = clean(row.get("Abbreviation", ""))
                    committees[key] = entry
                    file_count += 1
            log.registry_loaded(path.name, entries=file_count, relation="committees",
                               bytes=path.stat().st_size)

        # Apply GR detail registry (highest priority enrichment)
        def _apply_gr_detail(entry: dict, detail: dict) -> None:
            """Write gr_detail fields onto an existing committee entry."""
            entry["state_filer_id"] = str(detail.get("gr_id", ""))
            entry["committee_type"] = clean(detail.get("group_type",     "")) or entry.get("committee_type", "")
            entry["treasurer_name"] = clean(detail.get("treasurer_name", "")) or entry.get("treasurer_name", "")
            entry["city"]           = utils.clean_name(clean(detail.get("city", "")) or entry.get("city", ""))
            entry["zip"]            = clean(detail.get("zip",            "")) or entry.get("zip",            "")
            entry["raw_file"]       = detail.get("_raw_file", "")
            entry["row_num"]        = detail.get("_row_num",  "")

        gr_matched = gr_abbr_matched = 0

        # Primary pass: full group name
        for key, detail in gr_registry.items():
            canonical_name = clean(detail.get("group_name", ""))
            entry = committees.get(key) or {
                "state": STATE,
                "state_filer_id": "",
                "committee_name": canonical_name,
                "candidate_name": "",
            }
            committees[key] = entry
            _apply_gr_detail(entry, detail)
            gr_matched += 1

        # Fallback pass: APOC abbreviation (e.g. filer uses "HDCC" in transactions
        # but the full name in gr_details is "House Democratic Campaign Committee")
        for key, entry in list(committees.items()):
            if entry.get("state_filer_id"):
                continue          # already enriched by primary pass
            detail = gr_abbr_registry.get(key)
            if detail is None:
                continue
            _apply_gr_detail(entry, detail)
            gr_abbr_matched += 1

        if gr_registry:
            log.enrichment_summary(
                gr_matched=gr_matched,
                gr_abbr_matched=gr_abbr_matched,
                total_committees=len(committees),
            )

        # Committees: enrich Candidate-type entries from CRForms bulk exports
        # CRForms is the candidate-registration analog of GRForms bulk exports
        # above -- a plain Select-Year/Search/Export bulk download (same safe
        # request pattern already proven for GRForms/CDCandidates/CDIncome/
        # CDExpense), NOT the per-ID cr_details sweep, which is the thing that
        # gets DataDome-blocked. Added 2026-09-23. Uses the exact same
        # matching strategy as the CR detail registry below (first+last
        # primary key, committee-name secondary key, most-recent Submitted
        # date wins per key) via load_cr_forms_registry(), and only fills
        # gaps on committees that already exist from real transaction
        # activity -- doesn't fabricate a committee row for a bare
        # registration with no transactions, same restraint cr_details uses.
        # Runs BEFORE cr_details so cr_details (richer/individually verified,
        # where the sweep has reached it) still wins wherever it has data;
        # this just backfills what it doesn't.
        #
        # Real export columns confirmed live 2026-09-23 (see
        # data/Alaska/raw/CRForms_2024.csv): 35 columns, far more than the
        # on-screen "Additional Fields" grid picker showed -- includes
        # Display Name, Last Name, First Name, Committee, Treasurer Name,
        # City, Zip, Submitted. An earlier version of this block guessed
        # "Name" as the column (matching the grid's on-screen label) and
        # silently matched nothing -- the real header is "Display Name".
        cr_forms_registry = load_cr_forms_registry()
        cr_forms_matched  = 0
        for key, entry in committees.items():
            if entry.get("committee_type") != "Candidate":
                continue

            detail = cr_forms_registry.get(key)

            # Fallback: drop middle tokens, same as the CR detail match below
            if detail is None:
                parts = entry.get("committee_name", "").split()
                if len(parts) >= 3:
                    alt_key = committee_key(parts[0] + " " + parts[-1])
                    detail = cr_forms_registry.get(alt_key)

            if detail is None:
                continue

            entry["candidate_name"] = clean(detail.get("candidate_display_name", "")) or entry.get("candidate_name", "")
            entry["city"]           = utils.clean_name(clean(detail.get("city", "")) or entry.get("city", ""))
            entry["zip"]            = clean(detail.get("zip",            "")) or entry.get("zip",            "")
            entry["treasurer_name"] = clean(detail.get("treasurer_name", "")) or entry.get("treasurer_name", "")
            cr_forms_matched += 1

        if cr_forms_registry:
            log.registry_loaded("CRForms_*.csv", entries=len(cr_forms_registry),
                               relation="committees", bytes=0)
            log.enrichment_summary(
                cr_forms_matched=cr_forms_matched,
                total_committees=len(committees),
            )

        # Apply CR detail registry (candidate/PCC enrichment)
        # Matches by committee_key(first + last) against the candidate's filer
        # name from CDIncome/CDExpense. Falls back to stripping middle tokens
        # for names like "Pete B Higgins" → tries "pete higgins".
        cr_matched = 0
        for key, entry in committees.items():
            if entry.get("committee_type") != "Candidate":
                continue

            detail = cr_registry.get(key)

            # Fallback: drop middle tokens — "pete b higgins" → "pete higgins"
            if detail is None:
                parts = entry.get("committee_name", "").split()
                if len(parts) >= 3:
                    alt_key = committee_key(parts[0] + " " + parts[-1])
                    detail = cr_registry.get(alt_key)

            if detail is None:
                continue

            # CR data overwrites blank fields; it won't compete with GR
            entry["state_filer_id"] = str(detail.get("cr_id", "")) or entry.get("state_filer_id", "")
            entry["candidate_name"] = clean(detail.get("candidate_display_name", "")) or entry.get("candidate_name", "")
            entry["city"]           = utils.clean_name(clean(detail.get("city", "")) or entry.get("city", ""))
            entry["zip"]            = clean(detail.get("zip",            "")) or entry.get("zip",            "")
            entry["treasurer_name"] = clean(detail.get("treasurer_name", "")) or entry.get("treasurer_name", "")
            entry["raw_file"]       = detail.get("_raw_file", "")
            entry["row_num"]        = detail.get("_row_num",  "")
            cr_matched += 1

        if cr_registry:
            log.enrichment_summary(
                cr_matched=cr_matched,
                total_committees=len(committees),
            )



        # Independent Expenditures (Form 15-6) -- IEExpenditures_<year>.csv /
        # IEContributions_<year>.csv, bulk-exported by scrapers/alaska.py the
        # same Select-Year/Status/Search/Export way as CDIncome/CDExpense
        # above (confirmed live 2026-09-24 -- an earlier version of this
        # scraper swept per-filing detail pages instead; that was replaced
        # once the bulk grids turned out to export every filing directly,
        # same as every other AK relation). A third, separate disclosure
        # track from the regular CDIncome/CDExpense filings (confirmed
        # empirically non-redundant -- zero overlap spot-checked against the
        # existing contributions dataset), so it's folded into the existing
        # contributions.csv.gz/expenditures.csv.gz outputs here rather than
        # given its own tables -- IE money is still ordinary contribution/
        # expenditure money, just tagged with which candidate/measure it
        # supports or opposes (affiliated_candidate_name/support_oppose,
        # same convention Georgia's ie_stance_code() established).
        #
        # Column-name quirk confirmed live: the two exports don't name the
        # committee the same way. IEExpenditures.csv has only "Filer Name"
        # (which IS the committee name, e.g. "2026 - Repeal Now"), while
        # IEContributions.csv has BOTH "Filer Name" (the group's registered
        # contact person, e.g. "Rappoport, Ann Gayle") and "Filer" (the
        # actual committee name, e.g. "2026 - Alaska March On") -- use
        # "Filer" for contributions, "Filer Name" for expenditures. Same
        # amendment-dedup (highest "Result" wins) pattern as CDIncome/
        # CDExpense, since these exports repeat a row per amendment too
        # (confirmed live: an original "Filed *" row and its "Filed **"
        # amendment share date/amount/recipient but differ in Result).
        def _ie_stance(position: str) -> str:
            position = position.strip().lower()
            if position.startswith("support"):
                return "S"
            if position.startswith("oppos"):
                return "O"
            return ""

        # AK's IE "Candidate/Proposition" column mixes two different kinds
        # of target in one field: a YEAR-REGISTERED candidate entry (e.g.
        # "2026 - Jonathan Kreiss-Tomkins (JKT) / Zac Johnson" for a joint
        # Governor/Lt. Governor ticket) or a ballot measure/proposition
        # (e.g. "Prop 1 & 2", "Retention of judges", "2025-18 / Ballot Prop
        # 1"). Confirmed live 2026-09-24: candidate entries are
        # consistently "YYYY - Name" (literal 4-digit year, space, hyphen,
        # space) -- 957/958 values matching that exact shape are real
        # candidate names (the one exception, "Rocky MacDonald for Borough
        # Assembly", still IS a candidate); propositions never use that
        # shape ("2025-18 / ..." has no spaces around its hyphen, so it
        # correctly falls through untouched). Only candidate entries get
        # cleaned here -- dropping the year prefix, any parenthetical
        # nickname, and -- the actual bug this exists to fix -- everything
        # after a joint-ticket "/" or "\" delimiter. Without this, the
        # RUNNING MATE's surname is left as the last whitespace token, and
        # transform/matching.py's _last_name_of() (used to attribute IE
        # expenditures to a candidate_id) naively takes that last token --
        # the exact same failure mode already fixed for candidates.csv's
        # own joint-ticket rows above in this file's CDCandidates block
        # (see that block's comment for the delimiter-variant reasoning,
        # mirrored here). Confirmed live: without this fix, IE spending
        # supporting/opposing Jonathan Kreiss-Tomkins (a real, well-funded
        # 2026 Governor candidate) was silently un-attributed to him.
        def clean_ie_target_name(val: str) -> str:
            m = re.match(r"^\d{4} - (.+)$", val or "")
            if not m:
                return val
            name = m.group(1)
            name = re.sub(r"\s*\([^)]*\)", "", name)             # drop parenthetical nickname
            name = re.split(r"\s*[/\\]\s*", name, maxsplit=1)[0]  # keep top-of-ticket only
            return name.strip()

        for path in raw_files("IEExpenditures_*.csv"):
            ft   = time.perf_counter()
            seen: dict[tuple, dict] = {}
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        continue
                    filer      = strip_html_br(clean(row.get("Filer Name", "")))
                    filer_type = clean(row.get("Filer Type", ""))
                    if not filer:
                        continue
                    register_committee(filer, filer_type)
                    recipient = clean(row.get("Recipient", ""))
                    date_val  = parse_date(row.get("Date", ""))
                    result    = clean(row.get("Result", ""))
                    key = (recipient, amount, date_val, filer)
                    prev = seen.get(key)
                    if prev is None or (result.isdigit() and
                            (not prev["filing_id"].isdigit() or
                             int(result) > int(prev["filing_id"]))):
                        seen[key] = {
                            "state":                      STATE,
                            "committee_name":             utils.clean_name(filer),
                            "amount":                     amount,
                            "date":                       date_val,
                            "transaction_type":           "Independent Expenditure",
                            "payee_name":                 utils.clean_name(recipient),
                            "purpose":                    clean(row.get("Payment Detail", "")),
                            "category":                   clean(row.get("Payment Type", "")),
                            "payee_city":                 utils.clean_name(clean(row.get("Recipient City", ""))),
                            "payee_state":                clean(row.get("Recipient State", "")),
                            "payee_zip":                  clean(row.get("Recipient Zip", "")),
                            "candidate_name":             utils.clean_name(filer) if filer_type == "Candidate" else "",
                            "office":                     "",
                            "election_year":              clean(row.get("Election Year", "")) or clean(row.get("Report Year", "")),
                            "affiliated_candidate_name":  utils.clean_name(clean_ie_target_name(clean(row.get("Candidate/Proposition", "")))),
                            "support_oppose":             _ie_stance(clean(row.get("Position", ""))),
                            "amended":                    "",
                            "filing_id":                  result,
                            "raw_file":                   path.name,
                            "row_num":                    row_num,
                        }
            for out_row in seen.values():
                expn_w.writerow(out_row)
            count = len(seen)
            log.file_parsed(path.name, "expenditures", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)
            total_expenditures += count

        for path in raw_files("IEContributions_*.csv"):
            ft   = time.perf_counter()
            seen: dict[tuple, dict] = {}
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                for row_num, row in enumerate(csv.DictReader(f), start=2):
                    amount = parse_amount(row.get("Amount", ""))
                    if not amount:
                        continue
                    filer      = clean(row.get("Filer", ""))
                    filer_type = clean(row.get("Filer Type", ""))
                    if not filer:
                        continue
                    register_committee(filer, filer_type)
                    contributor = clean(row.get("Contributor", ""))
                    date_val    = parse_date(row.get("Date", ""))
                    result      = clean(row.get("Result", ""))
                    key = (contributor, amount, date_val, filer)
                    prev = seen.get(key)
                    if prev is None or (result.isdigit() and
                            (not prev["filing_id"].isdigit() or
                             int(result) > int(prev["filing_id"]))):
                        seen[key] = {
                            "state":             STATE,
                            "committee_name":    utils.clean_name(filer),
                            "amount":            amount,
                            "date":              date_val,
                            # This exact sentinel (not a more readable
                            # label) is required -- candidate_ie_contributors
                            # in materialized_views.sql joins contributions
                            # to a candidate's IE-active committees via
                            # 'contribution_type = FEC_NONFEDERAL_IE_RECEIPT'
                            # (contribution_type is contributions.csv.gz's
                            # transaction_type, renamed on push -- see
                            # cloud/supabase/transform/entities.py). Same
                            # convention ohio.py already established for its
                            # own FEC nonfederal-IE committee receipts: a
                            # receipt into an IE committee's account isn't
                            # earmarked to one candidate (the money is
                            # fungible across whatever that committee spends
                            # on), so it can't carry a candidate_id of its
                            # own -- this is the general flag the matview
                            # layer already uses to surface "who funds this
                            # committee" without inventing a per-dollar link
                            # the source data doesn't support. Confirmed
                            # live 2026-09-24: without this, candidate_ie_
                            # contributors silently returned 0 rows for AK
                            # even though candidate_ie_expenditures worked.
                            "transaction_type":  "FEC_NONFEDERAL_IE_RECEIPT",
                            "contributor_name":  utils.clean_name(contributor),
                            # The page's own "Type" column states the donor's
                            # type explicitly (Individual/Registered Group/
                            # Other) -- more reliable than the first-name
                            # heuristic CDIncome needs, so it's passed
                            # through as-is (same convention Delaware/
                            # Maryland/Minnesota/Kentucky use for their own
                            # explicit type columns). Downstream SQL only
                            # distinguishes 'Individual' from everything
                            # else, so any exact-text non-Individual value
                            # still filters correctly.
                            "contributor_type":  clean(row.get("Type", "")),
                            "contributor_city":  utils.clean_name(clean(row.get("Contributor City", ""))),
                            "contributor_state": clean(row.get("Contributor State", "")),
                            "contributor_zip":   clean(row.get("Contributor Zip", "")),
                            "employer":          clean(row.get("Employer", "")),
                            "occupation":        clean(row.get("Occupation", "")),
                            "candidate_name":    utils.clean_name(filer) if filer_type == "Candidate" else "",
                            "office":            "",
                            "election_year":     clean(row.get("Report Year", "")),
                            "amended":           "",
                            "filing_id":         result,
                            "raw_file":          path.name,
                            "row_num":           row_num,
                        }
            for out_row in seen.values():
                cont_w.writerow(out_row)
            count = len(seen)
            log.file_parsed(path.name, "contributions", count,
                            duration_s=time.perf_counter() - ft,
                            bytes=path.stat().st_size)
            total_contributions += count

        # Flush committees
        for row in committees.values():
            row["committee_name"] = utils.clean_name(row.get("committee_name", ""))
            row["candidate_name"] = utils.clean_name(row.get("candidate_name", ""))
            row["treasurer_name"] = utils.clean_name(row.get("treasurer_name", ""))
            row["city"]           = utils.clean_name(row.get("city", ""))
            cmte_w.writerow(row)

        # Close handles before person-ID assignment
        for fh in file_handles:
            fh.close()
        file_handles = []

        utils.assign_person_ids(CLEAN_DIR / "candidates.csv.gz", id_model="name_hash")
        utils.assign_committee_person_ids(CLEAN_DIR / "committees.csv.gz",
                                          CLEAN_DIR / "candidates.csv.gz")

        def _out_bytes(name):
            p = CLEAN_DIR / name
            return p.stat().st_size if p.exists() else 0

        log.file_parsed("contributions.csv.gz", "contributions", total_contributions, role="output",
                        bytes=_out_bytes("contributions.csv.gz"))
        log.file_parsed("expenditures.csv.gz",  "expenditures",  total_expenditures,  role="output",
                        bytes=_out_bytes("expenditures.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   0,                   role="output",
                        bytes=_out_bytes("loans_debts.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    len(committees),      role="output",
                        bytes=_out_bytes("committees.csv.gz"))
        log.file_parsed("candidates.csv.gz",    "candidates",    cand_count,           role="output",
                        bytes=_out_bytes("candidates.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("parse_completed", status="completed", duration_s=duration,
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=cand_count)

    except KeyboardInterrupt:
        log._emit("parse_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=cand_count)
        raise

    except Exception as e:
        log._emit("parse_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  contributions=total_contributions, expenditures=total_expenditures,
                  committees=len(committees), candidates=cand_count,
                  error_type=type(e).__name__, error=str(e))
        raise

    finally:
        for fh in file_handles:
            try:
                fh.close()
            except Exception:
                pass

# ====== CLI ==================================
if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
