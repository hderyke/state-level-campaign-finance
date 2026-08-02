"""
src/pipeline/enrich.py — Committee-candidate affiliation enrichment.

Pipeline stage 2.5 (scrape -> parse -> enrich -> validate -> tabulate ->
aggregate), invoked as a subprocess by orc.py right after the parser and
before validate. Runs unconditionally for every state, but is a no-op
unless that state has a registry file — most states won't for a while.

What it does
------------
Reads src/registries/committees/{abbr}.csv (hand-reviewed, git-tracked) and
writes two columns onto that state's committees.csv: affiliated_candidate_name
and support_oppose. These identify which candidate a PAC/CCE/ECO is tied to
but legally separate from -- distinct from the existing candidate_name column,
which is only populated for a candidate's OWN committee.

This is the "2b" (hand-made registry) fallback path. The "2a" (automated —
parser extracts the affiliation directly from source disclosure data, e.g.
FL's Statement of Organization Section 7) path is a separate, per-parser
effort tracked independently; when a parser does that extraction itself, its
committees.csv rows will already have affiliated_candidate_name/support_oppose
populated and this stage's registry lookups simply won't find a matching
(still-blank) row to overwrite -- the two paths don't conflict.

Why name-based, not person_id-based: see columns.py's note on COMMITTEES.
person_id was retired from the aggregate DB (2026-07-10) because the
id_model split (person/committee/name_hash) makes it an unreliable identity
for the same person across different offices/cycles. A hand-maintained
registry resolving to a synthetic person_id would inherit that same
unreliability. Instead the registry specifies (candidate_name, office,
election_year) directly, and this stage validates that triple against the
state's own candidates.csv (existence check only -- it never derives or
writes an ID) before copying candidate_name across.

Matching is by normalized (uppercase + whitespace-collapsed) committee_name,
optionally narrowed when the registry row specifies state_filer_id and/or
the secondary fields treasurer_name / registration_year. committee_name is
NOT a reliable unique key on its own -- FL (and likely other states) reuse
a closed/defunct committee's exact name for a new, unrelated registrant
years later. Discovered 2026-07-26: a plain name-only match for "FLORIDA
FIRST PAC" silently tagged both the real 2026 committee (filer_id 89604,
active, backing James Fishback) AND an unrelated closed 2008-era committee
of the same name (filer_id 46391) as Fishback-affiliated.

When committee_name matches more than one committees.csv row, resolution
falls through two tiers before giving up:
  1. state_filer_id, if given on the registry row -- exact and decisive
     on its own, no other field consulted. Always include this when the
     state has real filer IDs (most do); it's normally already on hand
     from confirming the committee in the first place.
  2. Otherwise, AND together whichever of treasurer_name / registration_year
     are filled in on the registry row, normalized-matched against the
     corresponding committees.csv fields (treasurer_name, election_year).
     This is the fallback for states with no filer IDs at all (id_model
     "name_hash" states like AK), or the rare case someone didn't have the
     filer ID handy. registration_year is deliberately a separate column
     from election_year (which means the CANDIDATE's cycle, used for the
     candidates.csv check above) -- a committee's own registration year and
     the candidate cycle it currently backs are different things and can
     differ (a PAC registered in 2024 backing a 2026 candidate).
If neither tier narrows the match down to exactly one row, this stage WARNS
and does NOT write the affiliation to any of them -- never guesses across
multiple real committees.

A registry row that doesn't match any committee in committees.csv, or whose
candidate_name/office/election_year triple doesn't exist in candidates.csv,
is reported as a warning -- never a hard failure. This is hand-maintained
data; a typo shouldn't halt the pipeline, but it should be visible in the
run log and terminal output.

Usage:
    python src/pipeline/enrich.py florida
    python src/pipeline/enrich.py Alaska

Exit codes:
    0 — always, unless the state has no cleaned dir at all (structural error)
"""

import csv
import sys
import time
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.reporting.logger import get_logger
from utils import _open_csv, clean_name

PROJECT_ROOT   = Path(__file__).resolve().parents[2]
REGISTRY_DIR   = PROJECT_ROOT / "src" / "registries" / "committees"

# Reuse the same abbr <-> name registration everyone else reads from.
_STATES_CSV = PROJECT_ROOT / "src" / "aliases" / "states.csv"
with open(_STATES_CSV, encoding="utf-8") as _f:
    _rows = list(csv.DictReader(_f))
NAME_TO_ABBR = {row["name"].strip().lower(): row["abbr"].strip().upper() for row in _rows}


def _norm(val: str) -> str:
    """Normalize for matching: same contract as utils._normalize_name."""
    return clean_name(val)


def _clean_dir(state: str) -> Path:
    state_lower = state.lower()
    d = PROJECT_ROOT / "data" / state_lower / "cleaned"
    if not d.exists():
        d = PROJECT_ROOT / "data" / state.capitalize() / "cleaned"
    return d


# Secondary disambiguators tried (AND'd together) when state_filer_id isn't
# given and a registry row's committee_name matches multiple committees.csv
# rows. Maps registry column -> committees.csv column; both sides normalized
# with _norm before comparing.
SECONDARY_FIELDS = {
    "treasurer_name":   "treasurer_name",
    "registration_year": "election_year",
}


def _load_candidate_keys(candidates_path: Path) -> tuple[set[tuple[str, str, str]], set[tuple[str, str]]]:
    """Returns (full_keys, blank_year_keys).

    full_keys: (normalized candidate_name, normalized office, election_year)
    triples that actually exist in this state's candidates.csv -- the exact
    match tried first.

    blank_year_keys: (normalized candidate_name, normalized office) pairs
    for rows where candidates.csv's OWN election_year is blank. Some states'
    candidates.csv leaves election_year blank on every row (confirmed for
    MI: 0/7,205 rows populated -- unlike FL, which populates it and where
    this fallback is simply never needed). Without this, a registry row
    that correctly records the candidate's cycle (e.g. election_year=2026)
    can never exact-match a state whose own data structurally can't record
    a cycle at all, producing a permanent, misleading "candidate not found"
    warning for a row that's actually correct. Falling back to (name,
    office) when the state's own data has nothing to compare election_year
    against is a real degradation in precision (can't tell 2022's John
    James from 2026's if the office/name repeat across cycles), but it's
    the best available given what the state discloses -- and it only
    kicks in when full_keys' exact triple genuinely can't match."""
    full_keys: set[tuple[str, str, str]] = set()
    blank_year_keys: set[tuple[str, str]] = set()
    if not candidates_path.exists():
        return full_keys, blank_year_keys
    with _open_csv(candidates_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name   = _norm(row.get("candidate_name", ""))
            office = _norm(row.get("office", ""))
            year   = (row.get("election_year", "") or "").strip()
            full_keys.add((name, office, year))
            if not year:
                blank_year_keys.add((name, office))
    return full_keys, blank_year_keys


def run(state: str) -> None:
    state_lower = state.lower()
    abbr        = NAME_TO_ABBR.get(state_lower, state.upper())
    clean_dir   = _clean_dir(state)

    if not clean_dir.exists():
        print(f"ERROR: cleaned dir not found for state '{state}'")
        sys.exit(1)

    log = get_logger(state_lower, "enrich")
    t0  = time.perf_counter()
    log._emit("enrich_started")

    try:
        n_matched, n_warned = _run(abbr, clean_dir, log)
        log._emit("enrich_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  matched=n_matched, warned=n_warned)
    except KeyboardInterrupt:
        log._emit("enrich_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("enrich_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1), error=str(e))
        raise


def _run(abbr: str, clean_dir: Path, log) -> tuple[int, int]:
    registry_path = REGISTRY_DIR / f"{abbr.lower()}.csv"
    committees_path = clean_dir / "committees.csv.gz"
    if not committees_path.exists():
        committees_path = clean_dir / "committees.csv"

    if not registry_path.exists():
        print(f"  ↷ No registry for {abbr} — skipping enrich (nothing to do)")
        return 0, 0

    if not committees_path.exists():
        print(f"  [!] No committees.csv(.gz) found for {abbr} — skipping enrich")
        return 0, 0

    candidates_path = clean_dir / "candidates.csv.gz"
    if not candidates_path.exists():
        candidates_path = clean_dir / "candidates.csv"
    candidate_keys, candidate_blank_year_keys = _load_candidate_keys(candidates_path)

    # ── Load + validate the registry ────────────────────────────────────
    with open(registry_path, newline="", encoding="utf-8") as f:
        registry_rows = list(csv.DictReader(f))

    # normalized committee_name -> registry row (first match wins; duplicate
    # committee_name entries in a registry file are a data-entry error, warned
    # about below rather than silently overwritten twice)
    by_committee: dict[str, dict] = {}
    n_warned = 0

    for r in registry_rows:
        cname = _norm(r.get("committee_name", ""))
        if not cname:
            continue

        reg_name   = _norm(r.get("candidate_name", ""))
        reg_office = _norm(r.get("office", ""))
        key = (reg_name, reg_office, (r.get("election_year", "") or "").strip())
        # Exact triple first; if that fails, fall back to (name, office) only
        # when this STATE's own candidates.csv has nothing to compare
        # election_year against (see _load_candidate_keys docstring) --
        # never as a general "close enough" fallback for states that do
        # populate election_year, where a real mismatch should still warn.
        found = key in candidate_keys or (reg_name, reg_office) in candidate_blank_year_keys
        if not found:
            print(f"  [!] enrich/{abbr}: registry row for {r.get('committee_name')!r} "
                  f"references candidate {r.get('candidate_name')!r} / {r.get('office')!r} / "
                  f"{r.get('election_year')!r}, which was not found in this state's "
                  f"candidates.csv — writing the affiliation anyway, but verify the "
                  f"registry row for typos.")
            log._emit("enrich_warning", reason="candidate_not_found",
                      committee_name=r.get("committee_name"),
                      candidate_name=r.get("candidate_name"),
                      office=r.get("office"), election_year=r.get("election_year"))
            n_warned += 1

        if cname in by_committee:
            print(f"  [!] enrich/{abbr}: duplicate registry entry for committee "
                  f"{r.get('committee_name')!r} — keeping the first, ignoring the rest")
            log._emit("enrich_warning", reason="duplicate_committee",
                      committee_name=r.get("committee_name"))
            n_warned += 1
            continue

        by_committee[cname] = r

    # ── Apply to committees.csv ─────────────────────────────────────────
    with _open_csv(committees_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return 0, n_warned

    # normalized committee_name -> list of committees.csv row indices sharing
    # that name. committee_name alone is not a unique key (see module
    # docstring) -- this lets us detect the ambiguous case instead of
    # silently applying a registry row to every same-named committee.
    rows_by_name: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        rows_by_name.setdefault(_norm(row.get("committee_name", "")), []).append(i)

    n_matched = 0
    matched_names: set[str] = set()
    for cname, reg in by_committee.items():
        candidates_idx = rows_by_name.get(cname, [])
        reg_filer_id = (reg.get("state_filer_id") or "").strip()

        if not candidates_idx:
            continue  # reported below as committee_not_found

        target_idx = candidates_idx
        if len(candidates_idx) > 1:
            if reg_filer_id:
                # Tier 1: state_filer_id given -- exact and decisive alone.
                target_idx = [i for i in candidates_idx
                              if (rows[i].get("state_filer_id") or "").strip() == reg_filer_id]
                if not target_idx:
                    print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
                          f"specifies state_filer_id={reg_filer_id!r}, but none of the "
                          f"{len(candidates_idx)} committees named that matched it — "
                          f"not writing the affiliation, check the filer ID.")
                    log._emit("enrich_warning", reason="filer_id_not_found",
                              committee_name=reg.get("committee_name"), state_filer_id=reg_filer_id)
                    n_warned += 1
                    matched_names.add(cname)
                    continue
            else:
                # Tier 2: no filer_id -- AND together whichever secondary
                # fields (treasurer_name, registration_year) are filled in.
                fields_used = []
                narrowed = candidates_idx
                for reg_col, committee_col in SECONDARY_FIELDS.items():
                    reg_val = _norm(reg.get(reg_col, ""))
                    if not reg_val:
                        continue
                    fields_used.append(reg_col)
                    narrowed = [i for i in narrowed
                               if _norm(rows[i].get(committee_col, "")) == reg_val]

                if not fields_used or len(narrowed) != 1:
                    filer_ids = [rows[i].get("state_filer_id") for i in candidates_idx]
                    if not fields_used:
                        print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
                              f"matches {len(candidates_idx)} different committees in committees.csv "
                              f"(state_filer_id={filer_ids}) — NOT writing the affiliation to any of "
                              f"them. Add state_filer_id (or treasurer_name/registration_year) to "
                              f"the registry row to disambiguate.")
                    else:
                        print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
                              f"matched {len(candidates_idx)} committees; narrowing by "
                              f"{fields_used} left {len(narrowed)} candidate(s), not exactly 1 — "
                              f"NOT writing the affiliation to any of them "
                              f"(state_filer_id={filer_ids}).")
                    log._emit("enrich_warning", reason="ambiguous_committee_name",
                              committee_name=reg.get("committee_name"), state_filer_ids=filer_ids,
                              secondary_fields_tried=fields_used, narrowed_to=len(narrowed))
                    n_warned += 1
                    matched_names.add(cname)
                    continue

                target_idx = narrowed
                print(f"  · enrich/{abbr}: {reg.get('committee_name')!r} disambiguated via "
                      f"{fields_used} (of {len(candidates_idx)} same-named committees)")

        for i in target_idx:
            rows[i]["affiliated_candidate_name"] = reg.get("candidate_name", "")
            rows[i]["support_oppose"] = reg.get("support_oppose", "")
            n_matched += 1
        matched_names.add(cname)

    unmatched = [cname for cname in by_committee if cname not in matched_names]
    for cname in unmatched:
        r = by_committee[cname]
        print(f"  [!] enrich/{abbr}: registry entry for {r.get('committee_name')!r} "
              f"didn't match any committee in committees.csv — check spelling/normalization")
        log._emit("enrich_warning", reason="committee_not_found",
                  committee_name=r.get("committee_name"))
        n_warned += 1

    fieldnames = list(rows[0].keys())
    for col in ("affiliated_candidate_name", "support_oppose"):
        if col not in fieldnames:
            fieldnames.append(col)

    suffixes = "".join(committees_path.suffixes)
    tmp = committees_path.with_name(committees_path.name.replace(suffixes, ".tmp" + suffixes))
    with _open_csv(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    import shutil
    shutil.move(str(tmp), str(committees_path))

    print(f"  ✓ enrich/{abbr}: {n_matched} committee row(s) enriched from "
          f"{len(by_committee)} registry entries ({n_warned} warning(s))")
    return n_matched, n_warned


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/pipeline/enrich.py <state>")
        sys.exit(1)
    run(sys.argv[1])
