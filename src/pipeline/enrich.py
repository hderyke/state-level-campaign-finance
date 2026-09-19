"""
src/pipeline/enrich.py — Committee-candidate affiliation enrichment.

Pipeline stage 2.5 (scrape -> parse -> enrich -> validate -> tabulate ->
aggregate), run as a subprocess by orc.py after the parser, before
validate. No-op unless the state has a registry file.

Reads src/registries/committees/{abbr}.csv (hand-reviewed, git-tracked)
and writes affiliated_candidate_name / support_oppose onto that state's
committees.csv -- which candidate a PAC/CCE/ECO is tied to but legally
separate from. This is the "2b" (hand-made registry) fallback; the "2a"
(automated, per-parser) path leaves these columns already populated, so
registry lookups here just find nothing blank to overwrite.

Why name-based, not person_id-based: person_id was retired from the
aggregate DB (2026-07-10) as unreliable across offices/cycles; a registry
resolving to a synthetic person_id would inherit that. It specifies
(candidate_name, office, election_year) directly instead, validated
(existence only) against the state's own candidates.csv.

committee_name alone is NOT a reliable key -- states reuse a closed
committee's exact name for an unrelated new registrant. Discovered
2026-07-26: a name-only match for "FLORIDA FIRST PAC" tagged both the
real 2026 committee and an unrelated closed 2008-era one the same way.
See _resolve_committee_rows for the two-tier resolution this drove.

Never a hard failure: an unmatched registry row just warns -- this is
hand-maintained data, a typo shouldn't halt the pipeline.

Usage: python src/pipeline/enrich.py florida
Exit codes: 0 -- always, unless the state has no cleaned dir (structural)
"""

import csv
import sys
import time
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.reporting.logger import get_logger
from utils import _open_csv, _normalize_name, find_clean_dir, _atomic_write_csv

PROJECT_ROOT   = Path(__file__).resolve().parents[2]
REGISTRY_DIR   = PROJECT_ROOT / "src" / "registries" / "committees"

# Reuse the same abbr <-> name registration everyone else reads from.
_STATES_CSV = PROJECT_ROOT / "src" / "aliases" / "states.csv"
with open(_STATES_CSV, encoding="utf-8") as _f:
    NAME_TO_ABBR = {row["name"].strip().lower(): row["abbr"].strip().upper()
                    for row in csv.DictReader(_f)}


def _norm(val: str) -> str:
    """Normalize for matching: same contract as utils._normalize_name."""
    return _normalize_name(val)


def _resolve_csv(clean_dir: Path, table: str) -> Path:
    """Prefer {table}.csv.gz; fall back to {table}.csv.

    Doesn't check existence -- "neither exists" means different things to
    different callers (fatal for committees.csv, degraded-mode for
    candidates.csv), so that check stays with the caller.
    """
    gz = clean_dir / f"{table}.csv.gz"
    return gz if gz.exists() else clean_dir / f"{table}.csv"


def _load_candidate_keys(candidates_path: Path) -> tuple[set[tuple[str, str, str]], set[tuple[str, str]]]:
    """Returns (full_keys, blank_year_keys) from this state's candidates.csv.

    full_keys: (name, office, election_year) triples, normalized -- the
    exact match tried first.

    blank_year_keys: (name, office) pairs for rows with a blank
    election_year. Some states (e.g. MI: 0/7,205 rows populated) never
    record it at all, so a correct registry row would otherwise never
    exact-match -- this fallback trades cycle precision (can't tell 2022's
    John James from 2026's) for not producing a permanent false "candidate
    not found" warning. Only used when full_keys can't match.
    """
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


def _validate_candidate_ref(reg: dict, candidate_keys: set[tuple[str, str, str]],
                             candidate_blank_year_keys: set[tuple[str, str]],
                             abbr: str, log) -> int:
    """Warn if a registry row's (candidate_name, office, election_year)
    doesn't check out against this state's candidates.csv (see
    _load_candidate_keys for the blank-year fallback). Returns 1 if it
    warned, 0 if the triple was found.
    """
    reg_name   = _norm(reg.get("candidate_name", ""))
    reg_office = _norm(reg.get("office", ""))
    key = (reg_name, reg_office, (reg.get("election_year", "") or "").strip())
    found = key in candidate_keys or (reg_name, reg_office) in candidate_blank_year_keys
    if found:
        return 0

    print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
          f"references candidate {reg.get('candidate_name')!r} / {reg.get('office')!r} / "
          f"{reg.get('election_year')!r}, which was not found in this state's "
          f"candidates.csv — writing the affiliation anyway, but verify the "
          f"registry row for typos.")
    log._emit("enrich_warning", reason="candidate_not_found",
              committee_name=reg.get("committee_name"),
              candidate_name=reg.get("candidate_name"),
              office=reg.get("office"), election_year=reg.get("election_year"))
    return 1


def _load_registry(registry_path: Path, candidate_keys: set[tuple[str, str, str]],
                    candidate_blank_year_keys: set[tuple[str, str]],
                    abbr: str, log) -> tuple[dict[str, dict], int]:
    """Read and validate registry_path. Returns (by_committee, n_warned).

    by_committee: normalized committee_name -> registry row (first match
    wins; duplicate names in the registry are a data-entry error, warned
    about here rather than silently overwritten).

    Each row's (candidate_name, office, election_year) is checked against
    candidate_keys/candidate_blank_year_keys and a mismatch is warned
    about, but the row is kept either way -- hand-maintained data
    shouldn't block on a typo.
    """
    with open(registry_path, newline="", encoding="utf-8") as f:
        registry_rows = list(csv.DictReader(f))

    by_committee: dict[str, dict] = {}
    n_warned = 0

    for reg in registry_rows:
        cname = _norm(reg.get("committee_name", ""))
        if not cname:
            continue

        if cname in by_committee:
            print(f"  [!] enrich/{abbr}: duplicate registry entry for committee "
                  f"{reg.get('committee_name')!r} — keeping the first, ignoring the rest")
            log._emit("enrich_warning", reason="duplicate_committee",
                      committee_name=reg.get("committee_name"))
            n_warned += 1
            continue

        n_warned += _validate_candidate_ref(reg, candidate_keys, candidate_blank_year_keys, abbr, log)
        by_committee[cname] = reg

    return by_committee, n_warned


def _index_by_name(committee_rows: list[dict]) -> dict[str, list[int]]:
    """normalized committee_name -> list of committees.csv row indices
    sharing that name (committee_name isn't a unique key -- see module
    docstring).
    """
    rows_by_name: dict[str, list[int]] = {}
    for i, row in enumerate(committee_rows):
        rows_by_name.setdefault(_norm(row.get("committee_name", "")), []).append(i)
    return rows_by_name


def _match_by_filer_id(committee_row_idx: list[int], committee_rows: list[dict], filer_id: str) -> list[int]:
    """Tier 1: rows among committee_row_idx whose state_filer_id matches exactly."""
    return [i for i in committee_row_idx
            if (committee_rows[i].get("state_filer_id") or "").strip() == filer_id]


# Secondary disambiguators (AND'd together) tried when state_filer_id isn't
# on the registry row. registry column -> committees.csv column.
SECONDARY_FIELDS = {
    "treasurer_name":   "treasurer_name",
    "registration_year": "election_year",
}


def _match_by_secondary_fields(committee_row_idx: list[int], committee_rows: list[dict],
                                reg: dict) -> tuple[list[int], list[str]]:
    """Tier 2: narrow committee_row_idx by AND-ing together whichever of
    treasurer_name / registration_year are filled in on the registry row.
    Returns (narrowed indices, which registry columns were actually used).
    """
    fields_used = []
    narrowed = committee_row_idx
    for reg_col, committee_col in SECONDARY_FIELDS.items():
        reg_val = _norm(reg.get(reg_col, ""))
        if not reg_val:
            continue
        fields_used.append(reg_col)
        narrowed = [i for i in narrowed
                   if _norm(committee_rows[i].get(committee_col, "")) == reg_val]
    return narrowed, fields_used


def _resolve_committee_rows(reg: dict, committee_row_idx: list[int], committee_rows: list[dict],
                             abbr: str, log) -> tuple[list[int] | None, int]:
    """Resolve one registry entry's committee_name match down to the exact
    committees.csv row indices to write the affiliation to.

    committee_row_idx is every row sharing the registry entry's normalized
    committee_name. 0 or 1 rows: nothing to resolve. More than 1: tries
    Tier 1 (state_filer_id) then Tier 2 (secondary fields) -- see module
    docstring. Warns and returns (None, 1) if neither narrows to exactly 1.
    """
    if len(committee_row_idx) <= 1:
        return committee_row_idx, 0

    reg_filer_id = (reg.get("state_filer_id") or "").strip()

    if reg_filer_id:
        # committees.csv can (rarely) have duplicate rows sharing both name
        # and filer_id -- treat that like Tier 2's ambiguous case, don't
        # write the affiliation to all of them.
        target_row_idx = _match_by_filer_id(committee_row_idx, committee_rows, reg_filer_id)
        if not target_row_idx:
            print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
                  f"specifies state_filer_id={reg_filer_id!r}, but none of the "
                  f"{len(committee_row_idx)} committees named that matched it — "
                  f"not writing the affiliation, check the filer ID.")
            log._emit("enrich_warning", reason="filer_id_not_found",
                      committee_name=reg.get("committee_name"), state_filer_id=reg_filer_id)
            return None, 1
        if len(target_row_idx) > 1:
            print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
                  f"specifies state_filer_id={reg_filer_id!r}, which matched "
                  f"{len(target_row_idx)} committees.csv rows (not 1) sharing that name "
                  f"and filer ID — NOT writing the affiliation to any of them. "
                  f"committees.csv itself has duplicate rows here, not a registry typo.")
            log._emit("enrich_warning", reason="filer_id_ambiguous",
                      committee_name=reg.get("committee_name"), state_filer_id=reg_filer_id,
                      matched_rows=len(target_row_idx))
            return None, 1
        return target_row_idx, 0

    # Tier 2: no filer_id -- AND together whichever secondary fields are given.
    narrowed, fields_used = _match_by_secondary_fields(committee_row_idx, committee_rows, reg)
    if not fields_used or len(narrowed) != 1:
        filer_ids = [committee_rows[i].get("state_filer_id") for i in committee_row_idx]
        if not fields_used:
            print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
                  f"matches {len(committee_row_idx)} different committees in committees.csv "
                  f"(state_filer_id={filer_ids}) — NOT writing the affiliation to any of "
                  f"them. Add state_filer_id (or treasurer_name/registration_year) to "
                  f"the registry row to disambiguate.")
        else:
            print(f"  [!] enrich/{abbr}: registry row for {reg.get('committee_name')!r} "
                  f"matched {len(committee_row_idx)} committees; narrowing by "
                  f"{fields_used} left {len(narrowed)} candidate(s), not exactly 1 — "
                  f"NOT writing the affiliation to any of them "
                  f"(state_filer_id={filer_ids}).")
        log._emit("enrich_warning", reason="ambiguous_committee_name",
                  committee_name=reg.get("committee_name"), state_filer_ids=filer_ids,
                  secondary_fields_tried=fields_used, narrowed_to=len(narrowed))
        return None, 1

    print(f"  · enrich/{abbr}: {reg.get('committee_name')!r} disambiguated via "
          f"{fields_used} (of {len(committee_row_idx)} same-named committees)")
    return narrowed, 0


def _report_unmatched(by_committee: dict[str, dict], matched_names: set[str],
                       abbr: str, log) -> int:
    """Warn about registry entries whose committee_name never matched any
    committees.csv row at all -- as opposed to matching but failing to
    resolve, which _resolve_committee_rows warns about separately. Returns
    the number of warnings emitted.
    """
    unmatched = [cname for cname in by_committee if cname not in matched_names]
    for cname in unmatched:
        reg = by_committee[cname]
        print(f"  [!] enrich/{abbr}: registry entry for {reg.get('committee_name')!r} "
              f"didn't match any committee in committees.csv — check spelling/normalization")
        log._emit("enrich_warning", reason="committee_not_found",
                  committee_name=reg.get("committee_name"))
    return len(unmatched)


def run(state: str) -> None:
    state_lower = state.lower()
    abbr        = NAME_TO_ABBR.get(state_lower, state.upper())
    clean_dir   = find_clean_dir(state)

    if clean_dir is None:
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
    if not registry_path.exists():
        print(f"  ↷ No registry for {abbr} — skipping enrich (nothing to do)")
        return 0, 0

    committees_path = _resolve_csv(clean_dir, "committees")
    if not committees_path.exists():
        print(f"  [!] No committees.csv(.gz) found for {abbr} — skipping enrich")
        return 0, 0

    candidates_path = _resolve_csv(clean_dir, "candidates")
    if not candidates_path.exists():
        print(f"  [!] No candidates.csv(.gz) found for {abbr} — proceeding without "
              f"candidate cross-validation (registry rows will warn but still be applied)")

    candidate_keys, candidate_blank_year_keys = _load_candidate_keys(candidates_path)

    by_committee, n_warned = _load_registry(
        registry_path, candidate_keys, candidate_blank_year_keys, abbr, log)

    with _open_csv(committees_path, newline="", encoding="utf-8") as f:
        committee_rows = list(csv.DictReader(f))

    if not committee_rows:
        return 0, n_warned

    rows_by_name = _index_by_name(committee_rows)

    n_matched = 0
    matched_names: set[str] = set()
    for cname, reg in by_committee.items():
        committee_row_idx = rows_by_name.get(cname, [])

        if not committee_row_idx:
            continue  # reported below as committee_not_found

        # Found by name at all -- not reported as committee_not_found below,
        # even if resolution turns out ambiguous.
        matched_names.add(cname)

        target_row_idx, warned = _resolve_committee_rows(reg, committee_row_idx, committee_rows, abbr, log)
        n_warned += warned
        if target_row_idx is None:
            continue

        for i in target_row_idx:
            committee_rows[i]["affiliated_candidate_name"] = reg.get("candidate_name", "")
            committee_rows[i]["support_oppose"] = reg.get("support_oppose", "")
            n_matched += 1

    n_warned += _report_unmatched(by_committee, matched_names, abbr, log)

    fieldnames = list(committee_rows[0].keys())
    for col in ("affiliated_candidate_name", "support_oppose"):
        if col not in fieldnames:
            fieldnames.append(col)

    _atomic_write_csv(committees_path, fieldnames, committee_rows)

    print(f"  ✓ enrich/{abbr}: {n_matched} committee row(s) enriched from "
          f"{len(by_committee)} registry entries ({n_warned} warning(s))")
    return n_matched, n_warned


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/pipeline/enrich.py <state>")
        sys.exit(1)
    run(sys.argv[1])
