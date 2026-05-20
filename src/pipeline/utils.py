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
from pathlib import Path


def _open_csv(path: Path, mode: str = "r", **kwargs):
    """Open a plain .csv or gzip-compressed .csv.gz transparently."""
    if path.suffix == ".gz":
        return gzip.open(path, mode + "t", **kwargs)
    return open(path, mode, **kwargs)


def _normalize_name(name: str) -> str:
    """Uppercase and collapse internal whitespace for grouping."""
    return re.sub(r"\s+", " ", name.strip().upper())


def assign_person_ids(candidates_path: Path, id_model: str = "committee") -> int:
    """
    Read candidates_path, fill in person_id column, rewrite in place.
    Returns number of rows processed.

    id_model: "person" | "committee"
    """
    if not candidates_path.exists():
        return 0

    with _open_csv(candidates_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return 0

    if id_model == "person":
        # person_id == state_filer_id — one-to-one, no grouping needed
        for row in rows:
            row["person_id"] = row.get("state_filer_id", "")

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
        #   Pass 2 — write it back onto every row in that group

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
                is_earlier = int(fid) < int(earliest[key])
            except (ValueError, KeyError):
                is_earlier = key not in earliest or fid < earliest[key]
            if is_earlier:
                earliest[key] = fid

        for row in rows:
            state    = row.get("state", "")
            norm     = _normalize_name(row.get("candidate_name", ""))
            office   = _normalize_name(row.get("office", ""))
            district = _normalize_name(row.get("district", ""))
            key      = (state, norm, office, district)
            row["person_id"] = earliest.get(key, row.get("state_filer_id", ""))

    elif id_model == "name_hash":
        # No numeric ID in the source — derive a stable surrogate from the name.
        # MD5(state + norm_name) → 9-digit integer.  Deterministic across runs.
        for row in rows:
            state = row.get("state", "")
            norm  = _normalize_name(row.get("candidate_name", ""))
            key   = f"{state}|{norm}".encode()
            row["person_id"] = str(int(hashlib.md5(key).hexdigest(), 16) % 1_000_000_000)

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
