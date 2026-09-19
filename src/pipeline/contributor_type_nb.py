"""
src/pipeline/contributor_type_nb.py — Individual vs. Organization classifier
for contributor names, for states whose raw source data doesn't already
distinguish the two (e.g. Florida's transaction export has no contributor-
type field at all).

Multinomial Naive Bayes over character trigrams + word unigrams. Trained
offline on ~4.8M labeled (contributor_name -> Individual/Organization)
pairs pooled from seven other states' own cleaned contributions.csv.gz
(Ohio, Michigan, Illinois, Missouri, Georgia, Colorado, Maryland), where
contributor_type is already populated and canonicalized via
src/aliases/contributor_types.csv. See docs/states/florida.md for the
validation numbers (held-out accuracy, stress-test cases).

No ML library dependency at inference time — just stdlib (re, gzip,
pickle, math) plus the ~560KB trained model file
src/aliases/contributor_type_classifier.pkl.gz.

Usage:
    from contributor_type_nb import classify_contributor_type
    classify_contributor_type("Leading the Future")   # -> "Organization"
    classify_contributor_type("Smith, John")           # -> "Individual"
    classify_contributor_type("")                      # -> ""
"""

import gzip
import math
import pickle
import re
from pathlib import Path

_MODEL_PATH = Path(__file__).resolve().parent.parent / "aliases" / "contributor_type_classifier.pkl.gz"

_CLEAN_RE = re.compile(r"[^A-Z0-9 &\-]")
_WS_RE = re.compile(r"\s+")

_model = None  # lazy-loaded, cached module-level


def _load_model() -> dict:
    global _model
    if _model is None:
        with gzip.open(_MODEL_PATH, "rb") as f:
            _model = pickle.load(f)
    return _model


def _normalize(name: str) -> str:
    n = name.upper()
    n = _CLEAN_RE.sub(" ", n)
    n = _WS_RE.sub(" ", n).strip()
    return n


def _extract_features(name: str) -> list:
    norm = _normalize(name)
    if not norm:
        return []
    feats = []
    padded = " " + norm + " "
    for i in range(len(padded) - 2):
        feats.append("T:" + padded[i:i + 3])
    for w in norm.split(" "):
        if w:
            feats.append("W:" + w)
    return feats


def classify_contributor_type(name: str) -> str:
    """Return 'Individual', 'Organization', or '' if name is blank."""
    if not name or not name.strip():
        return ""
    feats = _extract_features(name)
    if not feats:
        return ""
    model = _load_model()
    best_c, best_score = None, None
    for c in model["classes"]:
        score = model["log_prior"][c]
        loglik_c = model["loglik"][c]
        oov = model["log_oov"][c]
        for f in feats:
            score += loglik_c.get(f, oov)
        if best_score is None or score > best_score:
            best_score, best_c = score, c
    return best_c
