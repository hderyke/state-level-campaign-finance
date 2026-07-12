"""
parsers/mississippi.py — Parse Mississippi campaign finance data.

Reads the four raw JSON blobs written by scrapers/mississippi.py from
data/Mississippi/raw/ and writes the five canonical CSVs to
data/Mississippi/cleaned/.

Input (each a plain {"Table": [...]} object — see scraper docstring for the
double-JSON-envelope unwrapping that already happened at download time):

    entities.json       — {EntityId, EntityName, OrganizationType} per row.
                           OrganizationType is one of "Candidate",
                           "Candidate Committee", "Political Committee (PAC)",
                           "Political Initiative Committee". Split here into
                           candidates.csv (OrganizationType == "Candidate")
                           and committees.csv (everything else).
    contributions.json   — {Recipient, ReferenceNumber, FilingDesc, FilingId,
                           Contributor, ContributorType, AddressLine1, City,
                           StateCode, PostalCode, InKind, Occupation, Date,
                           Amount}. Recipient is the candidate/committee
                           receiving the money (-> committee_name).
    expenditures.json    — {Filer, ReferenceNumber, FilingDesc, FilingId,
                           Recipient, AddressLine1, City, StateCode,
                           PostalCode, Description, Date, Amount}. Filer is
                           the committee/candidate spending the money
                           (-> committee_name); Recipient here is the payee
                           (-> payee_name) — note this is the OPPOSITE role
                           "Recipient" plays in contributions.json.
    districts.json       — {EntityId, EntityName, OrganizationType,
                           ElectionYear, DistrictType, DistrictName} per row.
                           Optional -- written by the scraper's Statewide +
                           Judicial DistrictSearch sweep (12 calls). Used only
                           as a candidate<->committee name-linking tiebreaker
                           (see below); absence degrades gracefully to
                           name-only matching.

Notes:
  - Entity records from CandidateNameSearch carry only {EntityId, EntityName,
    OrganizationType} — no office, party, or address. Richer per-entity
    metadata lives behind a server-rendered HTML detail page
    (ViewXSLTFileByName.aspx) that isn't scraped (see scraper docstring), so
    candidates.csv office/party/district/election_year are left blank.
  - EntityId is a GUID, not a numeric filer ID — used as-is (string) for
    state_filer_id per the user's request that state_filer_id be whatever ID
    is distinct per campaign/committee registration. id_model="committee":
    person_id is derived by utils.assign_person_ids via name grouping, not
    assumed to already be person-stable.
  - candidate_name enrichment on contributions/expenditures/committees.csv
    now goes through link_candidate_committees() below, not just an exact
    name match. MS's data model has no shared filer ID between a candidate's
    "Candidate" registration and their "Candidate Committee" registration
    (they're two independent GUIDs) -- see the discovery that motivated this:
    the sitting governor (Tate Reeves) was nearly invisible in candidate-level
    rollups because his committee "Tate for Governor" had no candidate_name
    at all under the old exact-match-only logic. The linking algorithm is
    layered, safest-first:
      1. Name-token match: normalize the committee name and every candidate
         name into token sets (stopwords / office words / suffixes
         stripped). If exactly one candidate shares a token that ISN'T also
         shared by any other candidate (a "distinctive" token), link.
         Resolves ~43% of MS's ~655 candidate committees outright.
      2. Office tiebreak: MS is small enough that common-surname collisions
         are common (e.g. "Tate for Governor" name-matches both "J. Tate
         Reeves" AND "Jeff Tate" on the token "tate" alone -- step 1 leaves
         this ambiguous on purpose rather than guess). When districts.json
         is available, narrow the tied candidates down to whichever one(s)
         are registered for the SAME office as the committee itself. If
         that narrows to exactly one, link. This is what actually resolves
         the Reeves case: his committee and his own candidate registration
         both show up under (Statewide, Governor); Jeff Tate doesn't show up
         under any Statewide/Judicial office at all.
      3. Office-only fallback: if a committee has zero name-token overlap
         with any candidate (nickname, alternate spelling, etc.) but is
         registered for an office that exactly one candidate is also
         registered for, link on office alone.
      4. Otherwise leave candidate_name blank -- most commonly for PACs
         (which fund many candidates, not one) and House/Senate-linked
         committees, where DistrictSearch's office data is deliberately not
         fetched (see scraper docstring: no per-seat granularity, so a
         Legislative office tiebreak would be no safer than a coin flip).
    This is a heuristic, not a source-confirmed identity match. It is
    intentionally conservative -- ties that step 2/3 can't resolve stay
    unlinked rather than risk a wrong link.
  - amended is inferred by checking whether FilingDesc contains the word
    "Amended" (case-insensitive) — the source has no dedicated amended flag
    on individual transaction rows, only on the filing description.
  - No loan/debt data source was found on the portal — loans_debts.csv.gz is
    written empty.
"""

import csv
import gzip
import json
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))
from src.reporting.logger import get_logger
import columns as C
import utils

# =============================== paths ================================
RAW_DIR   = PROJECT_ROOT / "data" / "Mississippi" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "Mississippi" / "cleaned"
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

STATE          = "MS"
MAX_VALID_YEAR = date.today().year + 2

CANDIDATE_TYPE           = "Candidate"            # OrganizationType: a person, not a committee
CANDIDATE_COMMITTEE_TYPE = "Candidate Committee"   # OrganizationType: 1:1 tied to a single candidate
                                                    # (vs. "Political Committee (PAC)", which funds many)

# Stopwords stripped before name-token matching -- committee-naming
# boilerplate, generic office words (deliberately excluded so office words
# never masquerade as a name match), and name suffixes/titles.
_NAME_STOPWORDS = {
    "for", "of", "the", "committee", "friends", "campaign", "elect", "to",
    "and", "re", "reelect", "inc", "pac", "fund", "team", "citizens",
    "people", "jr", "sr", "ii", "iii", "iv", "mississippi", "state",
    "governor", "lieutenant", "secretary", "attorney", "general", "auditor",
    "treasurer", "commissioner", "insurance", "agriculture", "senate",
    "house", "representative", "rep", "senator", "mayor", "judge", "justice",
    "court", "circuit", "chancery", "supreme", "appeals", "district",
}


def name_tokens(name: str) -> set[str]:
    """Lowercase, strip punctuation, split into tokens; drop stopwords and
    tokens of length <= 2 (initials, "Jo", "Al", etc. are too short to be a
    reliable name signal)."""
    cleaned = re.sub(r"[^a-z0-9\s]", " ", (name or "").lower())
    return {t for t in cleaned.split() if len(t) > 2 and t not in _NAME_STOPWORDS}


# ============================== helpers ===============================

def clean(val) -> str:
    """Strip whitespace and coerce None to empty string."""
    return (val or "").strip()


def parse_amount(val: str) -> str:
    """Parse a dollar amount ('$1,000.00') to a plain numeric string. Returns '' on failure."""
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
    """'11/30/2016 12:00:00 AM' -> '2016-11-30'. Returns '' on failure or implausible year.

    Only the date portion (before the first space) is parsed — the time
    component is always midnight in this source and carries no information.
    """
    v = (val or "").strip()
    if not v:
        return ""
    date_part = v.split(" ")[0]
    try:
        d = datetime.strptime(date_part, "%m/%d/%Y")
        if d.year < 1990 or d.year > MAX_VALID_YEAR:
            return ""
        return d.strftime("%Y-%m-%d")
    except ValueError:
        return ""


def year_from_date(iso_date: str) -> str:
    """Pull the year off an already-normalized YYYY-MM-DD string."""
    return iso_date[:4] if iso_date else ""


_AMENDED_RE = re.compile(r"amended", re.IGNORECASE)


def is_amended(filing_desc: str) -> str:
    """'1' if FilingDesc mentions 'Amended', else '0' — no per-row amended flag exists."""
    return "1" if _AMENDED_RE.search(filing_desc or "") else "0"


MAX_TOKEN_DF = 6  # a token shared by more than this many candidates (e.g. a
                   # common first name like "robert") is too generic to use
                   # as a linking signal, even combined with office data


def link_candidate_committees(entities: list[dict], districts: list[dict]) -> dict[str, str]:
    """Return {normalized committee name -> canonical candidate_name} for
    "Candidate Committee" entities that can be safely linked to a "Candidate"
    entity. See the module docstring for the four-step algorithm (name-token
    match -> office tiebreak -> office-only fallback -> leave unmatched).
    Committees not present in the returned dict simply couldn't be resolved
    safely (ambiguous name collision with no districts.json signal to break
    the tie, or a PAC/multi-candidate committee, which is correct to leave
    unlinked).

    Two safety nets beyond the basic token-overlap logic, both found by
    testing against real MS data:
      - Step 2/3 only use "informative" tokens (shared by <= MAX_TOKEN_DF
        candidates). Without this, a committee like "Friends of Robert
        Foster" can spuriously office-tiebreak-match an unrelated "Robert
        ___" candidate purely because "robert" is common and the office's
        candidate roster happens to contain exactly one other "Robert" --
        even though the actual Robert Foster isn't in that roster at all
        (confirmed: MS's own DistrictSearch data doesn't always link a real
        candidate to their office).
      - Step 1 (distinctive-token match) is vetoed if the committee and its
        matched candidate both have known-but-non-overlapping offices --
        i.e. office data affirmatively contradicts the name match. This
        catches cases where a real candidate (e.g. a former governor with
        only a committee registered, no separate "Candidate" entity) has no
        row to match against, and a token coincidentally collides with an
        unrelated person: office data can veto that even without office data
        being able to *confirm* the right answer.
    """
    candidates = [
        (clean(r.get("EntityId", "")), clean(r.get("EntityName", "")))
        for r in entities
        if clean(r.get("OrganizationType", "")) == CANDIDATE_TYPE and clean(r.get("EntityName", ""))
    ]
    cand_tokens   = {name: name_tokens(name) for _, name in candidates}
    cand_eid_by_name = {name: eid for eid, name in candidates}  # last-writer-wins on dupes; fine for a veto check

    token_to_cands: dict[str, set[str]] = {}
    for _, name in candidates:
        for t in cand_tokens[name]:
            token_to_cands.setdefault(t, set()).add(name)

    # entity_office: EntityId -> {(DistrictType, DistrictName), ...}
    entity_office: dict[str, set[tuple[str, str]]] = {}
    for row in districts:
        eid   = clean(row.get("EntityId", ""))
        dtype = clean(row.get("DistrictType", ""))
        dname = clean(row.get("DistrictName", ""))
        if eid and dtype and dname:
            entity_office.setdefault(eid, set()).add((dtype, dname))

    # office -> {candidate names registered for it}
    office_candidates: dict[tuple[str, str], set[str]] = {}
    for eid, name in candidates:
        for off in entity_office.get(eid, ()):
            office_candidates.setdefault(off, set()).add(name)

    links: dict[str, str] = {}
    for row in entities:
        if clean(row.get("OrganizationType", "")) != CANDIDATE_COMMITTEE_TYPE:
            continue
        cname = clean(row.get("EntityName", ""))
        eid   = clean(row.get("EntityId", ""))
        if not cname:
            continue

        ctoks = name_tokens(cname)
        committee_offices = entity_office.get(eid, set())

        # Step 1: a token unique to exactly one candidate name ("distinctive")
        # -- if every distinctive token in the committee name points to the
        # same single candidate, that's a confident match. BUT: if we know
        # this committee's own office (it showed up in the Statewide/Judicial
        # districts.json sweep), require the matched candidate to ALSO be
        # registered for that same office before trusting the name match --
        # not just "no contradiction." Found by testing: "Friends of Phil
        # Bryant" distinctive-matches "Bryant Clark" (an unrelated candidate)
        # purely because the real Phil Bryant has no separate "Candidate"
        # entity in MS's registry at all -- only his committee is registered.
        # Since we know this committee IS a Governor-race entity, requiring
        # the matched candidate to also show up under Governor catches this;
        # a plain "not contradicted" check wouldn't, since Bryant Clark's own
        # office (if any) is simply absent from our data, not conflicting.
        # When the committee's own office is unknown to us (most committees,
        # since only Statewide+Judicial are swept), fall back to trusting
        # the name match alone -- there's nothing to confirm or deny against.
        distinctive_cands = {next(iter(token_to_cands[t])) for t in ctoks
                              if len(token_to_cands.get(t, ())) == 1}
        if len(distinctive_cands) == 1:
            cand = next(iter(distinctive_cands))
            cand_offices = entity_office.get(cand_eid_by_name.get(cand, ""), set())
            needs_confirmation = bool(committee_offices)
            confirmed = bool(cand_offices & committee_offices)
            if confirmed or not needs_confirmation:
                links[utils.clean_name(cname)] = cand
                continue

        # Gather candidates sharing an "informative" token (df <= MAX_TOKEN_DF
        # -- excludes generic first names like "robert"/"james" that are
        # common enough to coincidentally match the wrong person even after
        # office narrowing) and this committee's own registered office(s).
        informative_tokens = {t for t in ctoks
                               if t in token_to_cands and len(token_to_cands[t]) <= MAX_TOKEN_DF}
        name_options: set[str] = set()
        for t in informative_tokens:
            name_options |= token_to_cands[t]

        office_options: set[str] = set()
        for off in committee_offices:
            office_options |= office_candidates.get(off, set())

        # Step 2: office tiebreak -- narrow the name-sharing candidates down
        # to whichever are also registered for the same office as this
        # committee. This is what resolves e.g. "Tate for Governor", tied
        # between "J. Tate Reeves" and "Jeff Tate" on the token "tate" alone.
        if name_options and office_options:
            combined = name_options & office_options
            if len(combined) == 1:
                links[utils.clean_name(cname)] = next(iter(combined))
                continue

        # Step 3: office-only fallback -- no informative shared name token at
        # all (e.g. a nickname/alternate spelling), but the committee's
        # office has exactly one registered candidate.
        if not name_options and len(office_options) == 1:
            links[utils.clean_name(cname)] = next(iter(office_options))
            continue

        # Step 4: unresolved -- ambiguous (or contradicted) and no office
        # signal to safely break the tie. Left out of the returned dict;
        # candidate_name stays blank.

    return links


def load_table(filename: str) -> list[dict]:
    """Read a raw {"Table": [...]} JSON file. Returns [] if missing."""
    path = RAW_DIR / filename
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f).get("Table", [])


def open_writer(filename: str, fieldnames: list):
    fh = gzip.open(CLEAN_DIR / filename, "wt", encoding="utf-8", newline="")
    w  = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
    w.writeheader()
    return fh, w


# =============================== parse ================================

def run():
    log = get_logger("mississippi", "parse")
    t0  = time.perf_counter()
    log._emit("parse_started")

    total_contributions = 0
    total_expenditures  = 0
    candidates_written  = 0
    committees_written  = 0
    file_handles: list  = []

    try:
        cand_fh, cand_w = open_writer("candidates.csv.gz",    C.CANDIDATES)
        cmte_fh, cmte_w = open_writer("committees.csv.gz",    C.COMMITTEES)
        cont_fh, cont_w = open_writer("contributions.csv.gz", C.CONTRIBUTIONS)
        expn_fh, expn_w = open_writer("expenditures.csv.gz",  C.EXPENDITURES)
        loan_fh, loan_w = open_writer("loans_debts.csv.gz",   C.LOANS_DEBTS)
        file_handles = [cand_fh, cmte_fh, cont_fh, expn_fh, loan_fh]

        # ── Entities: split into candidates.csv / committees.csv ──────────
        entities_path  = RAW_DIR / "entities.json"
        districts_path = RAW_DIR / "districts.json"
        ft = time.perf_counter()
        entities  = load_table("entities.json")
        districts = load_table("districts.json")

        # Registry for the light candidate_name enrichment on transactions —
        # normalized "Candidate" entity name -> canonical (as-written) name.
        candidate_names: dict[str, str] = {}

        # normalized "Candidate Committee" name -> linked candidate_name, via
        # the name-token + office-tiebreak heuristic (see module docstring
        # and link_candidate_committees()). committees.json rides on the same
        # entities.json + districts.json inputs, so compute this once up
        # front rather than per-row.
        committee_links = link_candidate_committees(entities, districts)
        committees_linked = 0

        for row_num, row in enumerate(entities, start=1):
            org_type = clean(row.get("OrganizationType", ""))
            name     = clean(row.get("EntityName", ""))
            eid      = clean(row.get("EntityId", ""))
            if not name:
                continue  # a handful of PAC rows have a blank EntityName — unusable either way

            if org_type == CANDIDATE_TYPE:
                cand_w.writerow({
                    "state":           STATE,
                    "state_filer_id":  eid,
                    "candidate_name":  utils.clean_name(name),
                    "raw_file":        "entities.json",
                    "row_num":         row_num,
                })
                candidates_written += 1
                candidate_names[utils.clean_name(name)] = name
            else:
                linked_candidate = committee_links.get(utils.clean_name(name), "")
                if linked_candidate:
                    committees_linked += 1
                cmte_w.writerow({
                    "state":           STATE,
                    "state_filer_id":  eid,
                    "committee_name":  name,
                    "committee_type":  org_type,
                    "candidate_name":  linked_candidate,
                    "raw_file":        "entities.json",
                    "row_num":         row_num,
                })
                committees_written += 1

        log.file_parsed("entities.json", "candidates", candidates_written,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=entities_path.stat().st_size if entities_path.exists() else 0)
        log.file_parsed("entities.json", "committees", committees_written,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=entities_path.stat().st_size if entities_path.exists() else 0)
        log.info(f"Linked {committees_linked} committees to a candidate_name "
                 f"({len(districts)} district rows available)")

        # ── Contributions ──────────────────────────────────────────────
        contributions_path = RAW_DIR / "contributions.json"
        ft = time.perf_counter()
        skipped = 0
        for row_num, row in enumerate(load_table("contributions.json"), start=1):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            recipient    = clean(row.get("Recipient", ""))
            filing_desc  = clean(row.get("FilingDesc", ""))
            in_kind_amt  = parse_amount(row.get("InKind", ""))
            iso_date     = parse_date(row.get("Date", ""))
            norm_recip   = utils.clean_name(recipient)

            cont_w.writerow({
                "state":             STATE,
                "committee_name":    recipient,
                "amount":            amount,
                "date":              iso_date,
                "transaction_type":  "In-Kind" if in_kind_amt else "Monetary",
                "contributor_name":  utils.clean_name(row.get("Contributor", "")),
                "contributor_type":  clean(row.get("ContributorType", "")),
                "contributor_city":  utils.clean_name(row.get("City", "")),
                "contributor_state": clean(row.get("StateCode", "")),
                "contributor_zip":   utils.clean_zip(row.get("PostalCode", "")),
                "occupation":        clean(row.get("Occupation", "")),
                "candidate_name":    candidate_names.get(norm_recip) or committee_links.get(norm_recip, ""),
                "election_year":     year_from_date(iso_date),
                "amended":           is_amended(filing_desc),
                "filing_id":         clean(row.get("FilingId", "")),
                "raw_file":          "contributions.json",
                "row_num":           row_num,
            })
            total_contributions += 1

        log.file_parsed("contributions.json", "contributions", total_contributions, skipped,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=contributions_path.stat().st_size if contributions_path.exists() else 0)

        # ── Expenditures ───────────────────────────────────────────────
        expenditures_path = RAW_DIR / "expenditures.json"
        ft = time.perf_counter()
        skipped = 0
        for row_num, row in enumerate(load_table("expenditures.json"), start=1):
            amount = parse_amount(row.get("Amount", ""))
            if not amount:
                skipped += 1
                continue

            filer       = clean(row.get("Filer", ""))
            filing_desc = clean(row.get("FilingDesc", ""))
            iso_date    = parse_date(row.get("Date", ""))
            norm_filer  = utils.clean_name(filer)

            expn_w.writerow({
                "state":            STATE,
                "committee_name":   filer,
                "amount":           amount,
                "date":             iso_date,
                "payee_name":       utils.clean_name(row.get("Recipient", "")),
                "purpose":          clean(row.get("Description", "")),
                "payee_city":       utils.clean_name(row.get("City", "")),
                "payee_state":      clean(row.get("StateCode", "")),
                "payee_zip":        utils.clean_zip(row.get("PostalCode", "")),
                "candidate_name":   candidate_names.get(norm_filer) or committee_links.get(norm_filer, ""),
                "election_year":    year_from_date(iso_date),
                "amended":          is_amended(filing_desc),
                "filing_id":        clean(row.get("FilingId", "")),
                "raw_file":         "expenditures.json",
                "row_num":          row_num,
            })
            total_expenditures += 1

        log.file_parsed("expenditures.json", "expenditures", total_expenditures, skipped,
                        duration_s=round(time.perf_counter() - ft, 2),
                        bytes=expenditures_path.stat().st_size if expenditures_path.exists() else 0)

        # loans_debts.csv.gz stays empty — no loan/debt data source on this portal

        # ── Close handles before person-ID assignment ──────────────────
        for fh in file_handles:
            fh.close()
        file_handles = []

        # EntityId is a per-campaign/committee-registration GUID, not a
        # person-level ID — id_model="committee" derives person_id by
        # grouping candidates on normalized name (+ office/district, both
        # blank here since we don't scrape entity detail pages).
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
        log.file_parsed("candidates.csv.gz",    "candidates",    candidates_written,
                        role="output", bytes=_bytes("candidates.csv.gz"))
        log.file_parsed("committees.csv.gz",    "committees",    committees_written,
                        role="output", bytes=_bytes("committees.csv.gz"))
        log.file_parsed("loans_debts.csv.gz",   "loans_debts",   0,
                        role="output", bytes=_bytes("loans_debts.csv.gz"))

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
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
