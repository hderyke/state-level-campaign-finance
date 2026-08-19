"""
src/aliases/__init__.py — Shared alias/normalization lookups.

Files in this directory:
    nicknames.csv              nickname,formal       (MIKE → MICHAEL, BOB → ROBERT ...)
    committee_types.csv        state,raw,canonical   (AK,Candidate,Candidate Committee ...)
    contributor_types.csv      state,raw,canonical   (AL,Individual,Individual ...)
    transaction_categories.csv state,raw,canonical   (AL,Cash (Itemized),Monetary ...)
    expenditure_categories.csv state,raw,canonical   (AL,Itemized,Monetary ...)
    office_types.csv           state,raw,canonical   (KY,SLATE,Governor/Lt. Governor Ticket ...)
    parties.csv                raw,canonical         (REP → REPUBLICAN, DFL → DEMOCRAT ...)  — optional, not yet populated
    states.csv                 abbr,name,fips        (AK,Alaska,02 ...)

Usage:
    from src.aliases import expand_nickname, canonical_committee_type,
                           canonical_contributor_type, canonical_transaction_category,
                           canonical_office_type, canonical_party, fips_code

    expand_nickname("MIKE")                                    # → ["MICHAEL"]
    canonical_committee_type("AK", "Candidate")                # → "CANDIDATE COMMITTEE"
    canonical_contributor_type("AL", "Individual")             # → "Individual"
    canonical_contributor_type("AK", "Group")                  # → None  (ambiguous)
    canonical_transaction_category("AL", "Cash (Itemized)")    # → "Monetary"
    canonical_transaction_category("CA", "X")                  # → "In-Kind"
    canonical_office_type("KY", "SLATE")                       # → "Governor/Lt. Governor Ticket"
    canonical_party("REP")                                     # → "REPUBLICAN"
    fips_code("AK")                                            # → "02"
"""

import csv
from pathlib import Path

_DIR = Path(__file__).parent

# ============================== Loaders ==============================

def _load_nicknames() -> dict[str, list[str]]:
    """nickname (upper) → [formal names (upper)]  — one nickname can map to multiple formals."""
    path = _DIR / "nicknames.csv"
    result: dict[str, list[str]] = {}
    if not path.exists():
        return result
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            nick   = row.get("nickname", "").strip().upper()
            formal = row.get("formal",   "").strip().upper()
            if nick and formal:
                result.setdefault(nick, [])
                if formal not in result[nick]:
                    result[nick].append(formal)
    return result


def _load_committee_types() -> dict[tuple[str, str], str | None]:
    """(state_upper, raw_upper) → canonical committee type label, or None if intentionally unmapped."""
    return _load_state_keyed("committee_types.csv")


def _load_state_keyed(filename: str) -> dict[tuple[str, str], str | None]:
    """Generic loader for (state, raw) → canonical|None CSVs with # comment lines."""
    path = _DIR / filename
    result: dict[tuple[str, str], str | None] = {}
    if not path.exists():
        return result
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("state", "").startswith("#"):
                continue
            state = row.get("state", "").strip().upper()
            raw   = row.get("raw",   "").strip().upper()
            canon = row.get("canonical", "").strip() or None
            if state and raw:
                result[(state, raw)] = canon
    return result


def _load_contributor_types() -> dict[tuple[str, str], str | None]:
    """(state_upper, raw_upper) → canonical contributor type label, or None if intentionally unmapped."""
    return _load_state_keyed("contributor_types.csv")


def _load_transaction_categories() -> dict[tuple[str, str], str | None]:
    """(state_upper, raw_upper) → broad transaction category string, or None if unmapped."""
    return _load_state_keyed("transaction_categories.csv")


def _load_expenditure_categories() -> dict[tuple[str, str], str | None]:
    """(state_upper, raw_upper) → broad expenditure category string, or None if unmapped."""
    return _load_state_keyed("expenditure_categories.csv")


def _load_office_types() -> dict[tuple[str, str], str | None]:
    """(state_upper, raw_upper) → canonical office label, or None if intentionally unmapped."""
    return _load_state_keyed("office_types.csv")


def _load_parties() -> dict[str, str]:
    """raw_upper → canonical_upper.

    Skips `#` comment lines the same way _load_state_keyed does. The `or ""`
    guards matter here and not in the state-keyed loader: a comment line
    carries fewer fields than the header, so DictReader fills the trailing
    keys with None rather than "" and a bare .strip() raises AttributeError.
    """
    path = _DIR / "parties.csv"
    result: dict[str, str] = {}
    if not path.exists():
        return result
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            raw = (row.get("raw") or "").strip().upper()
            if not raw or raw.startswith("#"):
                continue
            canon = (row.get("canonical") or "").strip().upper()
            if canon:
                result[raw] = canon
    return result


def _load_fips() -> dict[str, str]:
    """abbr_upper → 2-digit FIPS code string (zero-padded)."""
    path = _DIR / "states.csv"
    result: dict[str, str] = {}
    if not path.exists():
        return result
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            abbr = row.get("abbr", "").strip().upper()
            fips = row.get("fips", "").strip().zfill(2)
            if abbr and fips:
                result[abbr] = fips
    return result


# =================== Module-level caches (loaded once) ===================

_NICKNAMES:               dict[str, list[str]]              = _load_nicknames()
_COMMITTEE_TYPES:         dict[tuple[str, str], str | None] = _load_committee_types()
_CONTRIBUTOR_TYPES:       dict[tuple[str, str], str | None] = _load_contributor_types()
_TRANSACTION_CATEGORIES:  dict[tuple[str, str], str | None] = _load_transaction_categories()
_EXPENDITURE_CATEGORIES:  dict[tuple[str, str], str | None] = _load_expenditure_categories()
_OFFICE_TYPES:            dict[tuple[str, str], str | None] = _load_office_types()
_PARTIES:                 dict[str, str]                    = _load_parties()
_FIPS:                    dict[str, str]                    = _load_fips()


# ============================== Public API ==============================

def expand_nickname(name: str) -> list[str]:
    """
    Return all known formal names for a given nickname (uppercase comparison).
    Returns an empty list if the name isn't a known nickname.

    Examples:
        expand_nickname("MIKE")  → ["MICHAEL"]
        expand_nickname("PAT")   → ["PATRICK", "PATRICIA"]
        expand_nickname("JAMES") → []   # already formal
    """
    return list(_NICKNAMES.get(name.strip().upper(), []))


def canonical_committee_type(state: str, raw: str) -> str | None:
    """
    Return the canonical committee type for a state-specific raw value.
    Returns None if intentionally unmapped. Falls back to raw value if no mapping exists.

    Examples:
        canonical_committee_type("AK", "Candidate")                → "Candidate Committee"
        canonical_committee_type("AZ", "Political Action Committee") → "PAC"
    """
    key = (state.strip().upper(), (raw or "").strip().upper())
    if key in _COMMITTEE_TYPES:
        return _COMMITTEE_TYPES[key]
    return raw.strip() or None   # unknown mapping — pass through as-is


def committee_type_mappings() -> dict[tuple[str, str], str | None]:
    """Return the full (state_upper, raw_upper) → canonical|None mapping for SQL generation."""
    return dict(_COMMITTEE_TYPES)


def canonical_contributor_type(state: str, raw: str) -> str | None:
    """
    Return the canonical contributor type for a state-specific raw value.
    Returns None if the value is intentionally unmapped (ambiguous or wrong-column).
    Falls back to the raw value if no mapping exists at all.

    Examples:
        canonical_contributor_type("AL", "Individual")              → "Individual"
        canonical_contributor_type("AL", "Group/Business/Corporation") → "Organization"
        canonical_contributor_type("AK", "Group")                   → None
        canonical_contributor_type("AZ", "PAC")                     → None
    """
    key = (state.strip().upper(), raw.strip().upper())
    if key in _CONTRIBUTOR_TYPES:
        return _CONTRIBUTOR_TYPES[key]
    return raw.strip() or None   # unknown mapping — pass through as-is


def canonical_transaction_category(state: str, raw: str) -> str | None:
    """
    Return the broad transaction category for a state-specific raw transaction_type.
    Returns None if unmapped (unknown or ambiguous).
    Falls back to None for unrecognized values rather than passing through raw
    (raw is preserved in transaction_type; category should be clean or absent).

    Examples:
        canonical_transaction_category("AL", "Cash (Itemized)")  → "Monetary"
        canonical_transaction_category("CA", "X")               → "In-Kind"
        canonical_transaction_category("AK", "Income")          → "Monetary"
        canonical_transaction_category("CA", "0")               → None
    """
    key = (state.strip().upper(), (raw or "").strip().upper())
    return _TRANSACTION_CATEGORIES.get(key)   # None for unknown — don't pass through


def contributor_type_mappings() -> dict[tuple[str, str], str | None]:
    """Return the full (state_upper, raw_upper) → canonical|None mapping for SQL generation."""
    return dict(_CONTRIBUTOR_TYPES)


def transaction_category_mappings() -> dict[tuple[str, str], str | None]:
    """Return the full (state_upper, raw_upper) → canonical|None mapping for SQL generation."""
    return dict(_TRANSACTION_CATEGORIES)


def canonical_expenditure_category(state: str, raw: str) -> str | None:
    """
    Return the broad expenditure category for a state-specific raw transaction_type.
    Returns None if unmapped (unknown, dirty, or unclassifiable).
    Raw value is preserved in transaction_type; category should be clean or absent.

    Examples:
        canonical_expenditure_category("AL", "Itemized")                             → "Monetary"
        canonical_expenditure_category("AZ", "Contribute to a Candidate Committee")  → "Contribution"
        canonical_expenditure_category("CA", "IND")                                  → "Independent Expenditure"
        canonical_expenditure_category("CO", "Unknown")                              → None
    """
    key = (state.strip().upper(), (raw or "").strip().upper())
    return _EXPENDITURE_CATEGORIES.get(key)   # None for unknown — don't pass through


def expenditure_category_mappings() -> dict[tuple[str, str], str | None]:
    """Return the full (state_upper, raw_upper) → canonical|None mapping for SQL generation."""
    return dict(_EXPENDITURE_CATEGORIES)


def canonical_office_type(state: str, raw: str) -> str | None:
    """
    Return the canonical office label for a state-specific raw office value.
    Falls back to the raw value if no mapping exists; returns None if blank.

    Examples:
        canonical_office_type("KY", "SLATE")              → "Governor/Lt. Governor Ticket"
        canonical_office_type("KY", "STATE REPRESENTATIVE") → "State Representative"
        canonical_office_type("AL", "GOVERNOR")           → "GOVERNOR"  (no mapping yet)
    """
    key = (state.strip().upper(), (raw or "").strip().upper())
    if key in _OFFICE_TYPES:
        return _OFFICE_TYPES[key]
    return raw.strip() or None   # unknown mapping — pass through as-is


def office_type_mappings() -> dict[tuple[str, str], str | None]:
    """Return the full (state_upper, raw_upper) → canonical|None mapping for SQL generation."""
    return dict(_OFFICE_TYPES)


def canonical_party(raw: str) -> str:
    """
    Return the canonical party label for a raw value.
    Falls back to the raw value (uppercased) if no mapping exists.

    Example:
        canonical_party("REP") → "REPUBLICAN"
        canonical_party("DFL") → "DEMOCRAT"
    """
    return _PARTIES.get(raw.strip().upper(), raw.strip().upper())


def fips_code(abbr: str) -> str:
    """
    Return the 2-digit zero-padded FIPS code for a state abbreviation.
    Raises KeyError if the abbreviation is not in states.csv.

    Examples:
        fips_code("AK")  → "02"
        fips_code("AL")  → "01"
        fips_code("CO")  → "08"
    """
    key = abbr.strip().upper()
    if key not in _FIPS:
        raise KeyError(f"Unknown state abbreviation: {abbr!r} — add it to src/aliases/states.csv")
    return _FIPS[key]
