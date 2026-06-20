"""
utils.py — Shared pipeline utilities.

assign_person_ids(path, id_model)
    Post-processing step called at the end of each state parser.
    Reads the written candidates.csv, assigns person_id, rewrites it.

    id_model options:
        "person"     — state_filer_id is already a person-level ID (AR, CO).
                       person_id = state_filer_id directly.
        "committee"  — state_filer_id is a per-committee ID (AZ, AL, CA).
                       person_id = earliest (min) state_filer_id for that
                       person, grouped by normalized candidate_name within state.
        "name_hash"  — no numeric ID exists in the source (AK).
                       person_id = stable 9-digit integer derived from
                       MD5(state + normalized_name).  Same name always
                       produces the same number; collision probability is
                       negligible at state-level candidate counts.

    All models produce a BIGINT of the form {FIPS}{base_id.zfill(12)}.
    States with a single-digit FIPS code (01–09) produce 13-digit IDs;
    states with a two-digit FIPS code (10+) produce 14-digit IDs.
    The ranges never overlap so global uniqueness across states is guaranteed
    in the aggregate database without changing the BIGINT column type.

    Name normalization for grouping: uppercase + collapse whitespace.
    This handles trailing spaces and minor formatting differences but will
    NOT merge "DOUG DUCEY" with "DUCEY, DOUG" — name format must be
    consistent within a state's own data (it usually is).
"""

import csv
import gzip
import hashlib
import re
import shutil
import sys
from pathlib import Path

# Aliases live in src/aliases — add src/ to path so it's importable from anywhere
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.aliases import expand_nickname, fips_code


# ========================== Shared helpers ============================


def _make_person_id(state_abbr: str, base_id: int) -> int:
    """Build a globally unique person_id (13 digits for FIPS < 10, 14 for FIPS ≥ 10).

    Format: {2-digit FIPS}{12-digit zero-padded base_id}

    The FIPS prefix ensures IDs are unique across states in the aggregate
    database without requiring a VARCHAR column type.

    Examples:
        _make_person_id("AL", 26034)       → 1000000026034   (13 digits, FIPS 01)
        _make_person_id("CO", 20005000068) → 8020005000068   (13 digits, FIPS 08)
        _make_person_id("TX", 26034)       → 48000000026034  (14 digits, FIPS 48)
    """
    fips = fips_code(state_abbr)
    return int(f"{fips}{str(base_id).zfill(12)}")


def _open_csv(path: Path, mode: str = "r", **kwargs):
    """Open a plain .csv or gzip-compressed .csv.gz transparently."""
    if path.suffix == ".gz":
        return gzip.open(path, mode + "t", **kwargs)
    return open(path, mode, **kwargs)


def clean_name(val: str) -> str:
    """Normalize a name for output: uppercase + collapse whitespace."""
    return re.sub(r"\s+", " ", (val or "").strip().upper())


def clean_zip(val: str) -> str:
    """Normalize a ZIP code: 9-digit strings become XXXXX-XXXX; others pass through."""
    v = (val or "").strip()
    if re.match(r"^\d{9}$", v):
        return f"{v[:5]}-{v[5:]}"
    return v


def _numeric_id(fid: str) -> int:
    """Coerce a state_filer_id to an int for person_id math.

    Most states' filer IDs are already plain integers. A few (e.g. Hawaii's
    "CC12091" / "NC20717" reg_no values) prefix the number with letters —
    strip any non-digit characters before parsing. Raises ValueError if no
    digits remain, same as int() would for a fully non-numeric string.
    """
    digits = re.sub(r"\D", "", fid or "")
    return int(digits)


def _normalize_name(name: str) -> str:
    """Uppercase and collapse internal whitespace for grouping.

    Thin wrapper around clean_name that gives person-ID grouping logic a
    stable internal contract — if clean_name ever changes (e.g. adds
    punctuation stripping), grouping behavior won't silently change with it.
    """
    return clean_name(name)


# ======================= Public pipeline steps ========================


def assign_person_ids(candidates_path: Path, id_model: str = "committee") -> int:
    """
    Read candidates_path, fill in person_id column, rewrite in place.
    Returns number of rows processed.

    id_model:
        "person"     — state_filer_id is already a person-level ID (AR, CO).
                       person_id = state_filer_id directly.
        "committee"  — state_filer_id is a per-committee ID (AZ, AL, CA).
                       person_id = earliest (min) state_filer_id for that
                       person, grouped by (state, candidate_name, office,
                       district).
        "name_hash"  — no numeric ID exists in the source (AK).
                       person_id = stable 9-digit integer derived from
                       MD5(state + normalized_name).  Same name always
                       produces the same number; collision probability is
                       negligible at state-level candidate counts.
    """
    if not candidates_path.exists():
        return 0

    with _open_csv(candidates_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return 0

    if id_model == "person":
        # person_id == state_filer_id — one-to-one, no grouping needed.
        # Prefixed with FIPS to ensure global uniqueness in the aggregate DB.
        for row in rows:
            state_abbr = row.get("state", "")
            fid        = row.get("state_filer_id", "")
            try:
                row["person_id"] = str(_make_person_id(state_abbr, _numeric_id(fid))) if fid else ""
            except (ValueError, KeyError):
                row["person_id"] = ""

    elif id_model == "committee":
        # person_id = min(state_filer_id) across all committees for that
        # (state, normalized_name, office, district) tuple.
        #
        # Including office + district prevents false merges when two different
        # people share a name but run for different seats, while still correctly
        # collapsing the same candidate's committees across election cycles.
        #
        # Two-pass:
        #   Pass 1 — find the earliest state_filer_id per name+office+district group
        #   Pass 2 — apply FIPS prefix and write back onto every row in that group

        earliest: dict[tuple, str] = {}   # (state, norm_name, office, district) → min filer_id

        for row in rows:
            state    = row.get("state", "")
            norm     = _normalize_name(row.get("candidate_name", ""))
            office   = _normalize_name(row.get("office", ""))
            district = _normalize_name(row.get("district", ""))
            fid      = row.get("state_filer_id", "")
            key      = (state, norm, office, district)

            if not fid:
                continue
            # Compare as integers when possible so "673" sorts before "6247"
            try:
                is_earlier = _numeric_id(fid) < _numeric_id(earliest[key])
            except (ValueError, KeyError):
                is_earlier = key not in earliest or fid < earliest[key]
            if is_earlier:
                earliest[key] = fid

        for row in rows:
            state_abbr = row.get("state", "")
            norm       = _normalize_name(row.get("candidate_name", ""))
            office     = _normalize_name(row.get("office", ""))
            district   = _normalize_name(row.get("district", ""))
            key        = (state_abbr, norm, office, district)
            base_fid   = earliest.get(key, row.get("state_filer_id", ""))
            try:
                row["person_id"] = str(_make_person_id(state_abbr, _numeric_id(base_fid))) if base_fid else ""
            except (ValueError, KeyError):
                row["person_id"] = ""

    elif id_model == "name_hash":
        # No numeric ID in the source — derive a stable surrogate from the name.
        # MD5(state + norm_name) → 12-digit integer, then prefixed with FIPS.
        # Deterministic across runs; state is already embedded in the hash so
        # the FIPS prefix is consistent with the other models.
        for row in rows:
            state_abbr = row.get("state", "")
            norm       = _normalize_name(row.get("candidate_name", ""))
            key        = f"{state_abbr}|{norm}".encode()
            base_id    = int(hashlib.md5(key).hexdigest(), 16) % 1_000_000_000_000  # 12-digit max
            try:
                row["person_id"] = str(_make_person_id(state_abbr, base_id))
            except KeyError:
                row["person_id"] = str(base_id)

    else:
        raise ValueError(f"Unknown id_model: {id_model!r}. Use 'person', 'committee', or 'name_hash'.")

    # Rewrite atomically via a temp file
    fieldnames = list(rows[0].keys())
    # Ensure person_id is in fieldnames (it should be via columns.py restval='')
    if "person_id" not in fieldnames:
        fieldnames = ["person_id"] + fieldnames

    # Preserve the original suffix chain (.csv.gz or .csv) for the temp file
    suffixes = "".join(candidates_path.suffixes)          # e.g. ".csv.gz" or ".csv"
    tmp = candidates_path.with_name(candidates_path.name.replace(suffixes, ".tmp" + suffixes))
    with _open_csv(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    shutil.move(str(tmp), str(candidates_path))
    return len(rows)


def assign_committee_person_ids(committees_path: Path, candidates_path: Path) -> int:
    """
    Match committees to candidates via candidate_name and write person_id
    onto each committee row.  PACs and other non-candidate committees
    (blank candidate_name) are left with an empty person_id.

    Matching is two-pass:
      Pass 1 — exact normalized name match ("PETE HIGGINS" → "PETE HIGGINS").
      Pass 2 — first+last token fallback for names that include middle initials
               or middle names ("PETE B. HIGGINS" → tokens[0]="PETE",
               tokens[-1]="HIGGINS").  The fallback is only applied when
               exactly ONE candidate has that first+last pair — if two or more
               candidates share the same first and last name the match is
               ambiguous and the committee is left unmatched to avoid false
               positives.

    Must be called AFTER assign_person_ids() has already run on candidates_path.
    Returns number of committees matched.
    """
    if not committees_path.exists() or not candidates_path.exists():
        return 0

    # Build exact name → person_id lookup from candidates.
    # Also build (first_token, last_token) → [person_ids] for fallback.
    name_to_pid: dict[str, str] = {}
    fl_to_pids:  dict[tuple[str, str], list[str]] = {}

    with _open_csv(candidates_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid  = row.get("person_id", "").strip()
            name = _normalize_name(row.get("candidate_name", ""))
            if not pid or not name:
                continue
            if name not in name_to_pid:
                name_to_pid[name] = pid
            # first+last index — strip periods so "B." and "B" both reduce to tokens
            tokens = [t.rstrip(".") for t in name.split() if t.rstrip(".")]
            if len(tokens) >= 2:
                fl_key = (tokens[0], tokens[-1])
                fl_to_pids.setdefault(fl_key, [])
                if pid not in fl_to_pids[fl_key]:
                    fl_to_pids[fl_key].append(pid)

    with _open_csv(committees_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return 0

    matched = 0
    fallback_matched = 0
    for row in rows:
        cname = _normalize_name(row.get("candidate_name", ""))
        pid   = name_to_pid.get(cname, "")

        if not pid and cname:
            # Fallback 1: strip middle tokens and try first+last only.
            tokens = [t.rstrip(".") for t in cname.split() if t.rstrip(".")]
            if len(tokens) >= 3:           # at least 3 tokens → middle token(s) present
                fl_key  = (tokens[0], tokens[-1])
                matches = fl_to_pids.get(fl_key, [])
                if len(matches) == 1:      # unambiguous — exactly one candidate
                    pid = matches[0]
                    fallback_matched += 1

        if not pid and cname:
            # Fallback 2: nickname expansion — "MIKE DUNLEAVY" → try "MICHAEL DUNLEAVY".
            # Only applied when exactly one formal name maps unambiguously.
            tokens = [t.rstrip(".") for t in cname.split() if t.rstrip(".")]
            if len(tokens) >= 2:
                formals = expand_nickname(tokens[0])
                for formal in formals:
                    fl_key  = (formal, tokens[-1])
                    matches = fl_to_pids.get(fl_key, [])
                    if len(matches) == 1:
                        pid = matches[0]
                        fallback_matched += 1
                        break

        row["person_id"] = pid
        if pid:
            matched += 1

    fieldnames = list(rows[0].keys())
    if "person_id" not in fieldnames:
        fieldnames = ["person_id"] + fieldnames

    suffixes = "".join(committees_path.suffixes)
    tmp = committees_path.with_name(committees_path.name.replace(suffixes, ".tmp" + suffixes))
    with _open_csv(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    shutil.move(str(tmp), str(committees_path))
    return matched
