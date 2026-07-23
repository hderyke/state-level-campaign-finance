"""
src/reporting/log_report.py — Convert a pipeline JSONL log to a clean HTML report.

Usage:
    python3 src/log_report.py logs/prod/20260520_161700_update_entities_AL.jsonl
    python3 src/log_report.py logs/prod/20260520_161700_update_entities_AL.jsonl -o report.html

Output defaults to <input>.html in the same directory.
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


# == Helpers ====================================================================

def fmt_rows(n):
    return f"{n:,}" if n is not None else "—"

def fmt_bytes(b):
    if b is None:
        return "—"
    if b >= 1_048_576:
        return f"{b/1_048_576:.1f} MB"
    return f"{b/1024:.1f} KB"

def fmt_dur(s):
    if s is None:
        return "—"
    if s >= 60:
        m = int(s) // 60
        sec = s - m * 60
        return f"{m}m {sec:.0f}s"
    return f"{s:.1f}s"

def ts_display(ts_str):
    try:
        dt = datetime.fromisoformat(ts_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ts_str

def fmt_date(iso_str):
    """'2026-06-30' -> 'Jun 30, 2026'; '—' for missing/unparseable values."""
    if not iso_str:
        return "—"
    try:
        dt = datetime.strptime(iso_str, "%Y-%m-%d")
        return f"{dt.strftime('%b')} {dt.day}, {dt.strftime('%Y')}"
    except Exception:
        return iso_str

def status_class(status):
    mapping = {
        "ok": "ok", "passed": "ok", "completed": "ok",
        "error": "err", "failed": "err",
        "interrupted": "warn",
        "warning": "warn",
        "skipped": "neutral",
    }
    return mapping.get(str(status).lower(), "neutral")

def status_icon(status):
    mapping = {
        "ok": "✓", "passed": "✓", "completed": "✓",
        "error": "✗", "failed": "✗",
        "interrupted": "⚠",
    }
    return mapping.get(str(status).lower(), "·")


def fmt_delta(mb):
    """Format a delta_mb value with sign; returns '—' for None or 0."""
    if mb is None:
        return "—"
    if mb == 0.0:
        return "—"
    sign = "+" if mb > 0 else ""
    if abs(mb) >= 1.0:
        return f"{sign}{mb:.2f} MB"
    return f"{sign}{mb * 1000:.1f} KB"


_META_KEYS = {"ts", "state", "operation", "type"}

def _strip_meta(e: dict) -> dict:
    return {k: v for k, v in e.items() if k not in _META_KEYS}


_STATES_CSV = Path(__file__).resolve().parents[2] / "src" / "aliases" / "states.csv"
with open(_STATES_CSV, encoding="utf-8") as _f:
    _NAME_TO_ABBR: dict[str, str] = {
        row["name"].lower(): row["abbr"]
        for row in csv.DictReader(_f)
    }

def _to_abbr(name: str) -> str:
    """Convert a lowercase state name to its two-letter abbreviation, or return name.upper()."""
    return _NAME_TO_ABBR.get(name.lower(), name.upper())


def _target_key(raw: str) -> str:
    """Canonical push/pull target key. Push/pull events log `state` inconsistently
    — push_started/push_completed use the abbreviation, file_pushed/file_deleted
    use the full lowercase name — so grouping directly on the raw value silently
    splits one state's events into two separate targets ("AK" and "alaska").
    Route everything through _to_abbr so both forms land in the same bucket;
    "all"/"db"/"global" pass through unchanged since they aren't states."""
    if raw in ("all", "db", "global"):
        return raw
    return _to_abbr(raw)


# == Event parsing ==============================================================

def load_events(path: Path) -> list[dict]:
    events = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return events


def build_report(events: list[dict]) -> dict:
    """Organise events into a structured report dict."""
    r = {
        "run": {},
        "states": {},       # state_name → {scrape, parse, validate, tabulate}
        "aggregate": {},
        "targets": {},      # push/pull: target_key → per-target data
        "_state_order": [],
        "_target_order": [],
        "report_type": "pipeline",  # "pipeline" | "push" | "pull"
    }

    state_order = []
    _push_global_tgt: str | None = None  # target key for state-less push events
    _pull_global_tgt: str | None = None  # target key for state-less pull events

    for e in events:
        t    = e.get("type", "")
        st   = e.get("state")
        op   = e.get("operation", "")

        #  Run level
        if t == "run_started":
            r["run"] = {
                "run_id":    e.get("run_id"),
                "command":   e.get("command"),
                "states":    e.get("states", []),
                "ts_start":  e.get("ts"),
            }
        elif t == "run_completed":
            r["run"].update({
                "status":    e.get("status"),
                "duration_s": e.get("duration_s"),
                "passed":    e.get("passed"),
                "failed":    e.get("failed"),
                "ts_end":    e.get("ts"),
            })

        # State level
        elif t == "state_started" and st:
            if st not in r["states"]:
                r["states"][st] = {"scrape": {}, "parse": {}, "validate": {}, "tabulate": {}}
                state_order.append(st)

        elif t == "state_duration" and st:
            if st in r["states"]:
                r["states"][st]["duration_s"] = e.get("duration_s")
                r["states"][st]["status"]     = e.get("status")

        # Scrape
        elif op == "scrape" and st:
            s = r["states"].setdefault(st, {"scrape": {}, "parse": {}, "validate": {}, "tabulate": {}})
            sc = s["scrape"]

            if t == "scrape_started":
                sc.update({
                    "force":        e.get("force"),
                    "entities":     e.get("entities"),
                    "transactions": e.get("transactions"),
                    "downloaded":   [],
                    "scraped":      [],
                })
            elif t == "file_download":
                sc.setdefault("downloaded", []).append({
                    "filename":   e.get("filename"),
                    "status":     e.get("status"),
                    "rows":       e.get("rows"),
                    "bytes":      e.get("bytes"),
                    "duration_s": e.get("duration_s"),
                    "error":      e.get("error"),
                    "zip_id":     e.get("zip_id"),
                })
            elif t == "page_scrape":
                sc.setdefault("scraped", []).append({
                    "filename":   e.get("filename"),
                    "status":     e.get("status"),
                    "rows":       e.get("rows"),
                    "bytes":      e.get("bytes"),
                    "duration_s": e.get("duration_s"),
                    "ok":         e.get("ok"),
                    "err":        e.get("err"),
                })
            elif t == "scrape_completed":
                sc.update({
                    "status":     e.get("status"),
                    "duration_s": e.get("duration_s"),
                    "files_ok":   e.get("files_ok"),
                    "files_err":  e.get("files_err"),
                    "pages_ok":   e.get("pages_ok"),
                    "pages_err":  e.get("pages_err"),
                })

        # Parse
        elif op == "parse" and st:
            s  = r["states"].setdefault(st, {"scrape": {}, "parse": {}, "validate": {}, "tabulate": {}})
            pa = s["parse"]

            if t == "parse_started":
                pa["files"]       = []
                pa["enrichments"] = []
            elif t == "file_parsed":
                pa.setdefault("files", []).append({
                    "filename":   e.get("filename"),
                    "relation":   e.get("relation"),
                    "role":       e.get("role", "source"),
                    "status":     e.get("status"),
                    "rows":       e.get("rows"),
                    "skipped":    e.get("skipped", 0),
                    "bytes":      e.get("bytes"),
                    "duration_s": e.get("duration_s"),
                })
            elif t == "enrichment_summary":
                d = {k: v for k, v in e.items() if k not in ("ts", "state", "operation", "type")}
                pa.setdefault("enrichments", []).append(d)
            elif t == "parse_completed":
                pa.update({
                    "status":     e.get("status"),
                    "duration_s": e.get("duration_s"),
                })
                # pull top-level relation counts
                skip = {"ts", "state", "operation", "type", "status", "duration_s"}
                pa["totals"] = {k: v for k, v in e.items() if k not in skip}

        #  Validate
        elif op == "validate" and st:
            s  = r["states"].setdefault(st, {"scrape": {}, "parse": {}, "validate": {}, "tabulate": {}})
            va = s["validate"]

            if t == "validate_completed":
                va.update({
                    "status":          e.get("status"),
                    "duration_s":      e.get("duration_s"),
                    "tier1_failures":  e.get("tier1_failures", 0),
                    "tier2_warnings":  e.get("tier2_warnings", 0),
                    "drift_warnings":  e.get("drift_warnings", 0),
                    "row_counts":      e.get("row_counts", {}),
                    "sampled_tables":  e.get("sampled_tables", {}),
                    "newest_record":   e.get("newest_record"),
                })

        # Tabulate
        elif op == "tabulate" and st:
            s  = r["states"].setdefault(st, {"scrape": {}, "parse": {}, "validate": {}, "tabulate": {}})
            ta = s["tabulate"]

            if t == "tabulate_started":
                ta["db"]     = e.get("db")
                ta["tables"] = []
            elif t == "table_loaded":
                ta.setdefault("tables", []).append({
                    "table":      e.get("table"),
                    "rows":       e.get("rows"),
                    "duration_s": e.get("duration_s"),
                })
            elif t == "tabulate_completed":
                ta.update({
                    "status":     e.get("status"),
                    "duration_s": e.get("duration_s"),
                    "tables_ok":  e.get("tables_ok"),
                    "tables_err": e.get("tables_err"),
                    "bytes":      e.get("bytes"),
                    "error_type": e.get("error_type"),
                    "error":      e.get("error"),
                })

        # Aggregate
        elif op == "aggregate":
            ag = r["aggregate"]

            if t == "aggregate_started":
                ag["states"]       = e.get("states", [])
                ag["states_count"] = e.get("states_count")
                ag["tables"]       = []
            elif t == "table_built":
                ag.setdefault("tables", []).append({
                    "table":      e.get("table"),
                    "rows":       e.get("rows"),
                    "duration_s": e.get("duration_s"),
                })
            elif t == "aggregate_completed":
                ag.update({
                    "status":     e.get("status"),
                    "duration_s": e.get("duration_s"),
                    "tables_ok":  e.get("tables_ok"),
                    "tables_err": e.get("tables_err"),
                })

        #  Push
        elif op == "push":
            r["report_type"] = "push"
            if st:
                tgt = _target_key(st)
            else:
                if _push_global_tgt is None and t == "push_started":
                    _push_global_tgt = e.get("target", "global")
                tgt = _push_global_tgt or "global"

            if tgt not in r["targets"]:
                r["targets"][tgt] = {
                    "op": "push", "started": {}, "completed": {},
                    "files": [], "deleted": [], "manifest": {}, "diff": {},
                }
                r["_target_order"].append(tgt)
            td = r["targets"][tgt]

            if t == "push_started":
                td["started"] = _strip_meta(e)
            elif t == "push_completed":
                td["completed"] = _strip_meta(e)
            elif t == "file_pushed":
                td["files"].append({
                    "filename":   e.get("filename"),
                    "remote_key": e.get("remote_key"),
                    "status":     e.get("status"),
                    "rows":       e.get("rows"),
                    "delta_mb":   e.get("delta_mb"),
                    "duration_s": e.get("duration_s"),
                    "error":      e.get("error"),
                })
            elif t == "file_deleted":
                td["deleted"].append({
                    "remote_key": e.get("remote_key"),
                    "status":     e.get("status"),
                    "error":      e.get("error"),
                })
            elif t == "manifest_checked":
                td["manifest"] = {"kept": e.get("kept"), "pruned": e.get("pruned")}
            elif t == "diff_completed":
                td["diff"] = {k: e.get(k) for k in
                              ("matched", "only_remote", "only_local", "size_mismatch")}

        #  Pull
        elif op == "pull":
            r["report_type"] = "pull"
            if st:
                tgt = _target_key(st)
            else:
                if _pull_global_tgt is None and t == "pull_started":
                    _pull_global_tgt = e.get("target", "global")
                tgt = _pull_global_tgt or "global"

            if tgt not in r["targets"]:
                r["targets"][tgt] = {
                    "op": "pull", "started": {}, "completed": {},
                    "files": [], "deleted": [], "manifest": {}, "diff": {},
                }
                r["_target_order"].append(tgt)
            td = r["targets"][tgt]

            if t == "pull_started":
                td["started"] = _strip_meta(e)
            elif t == "pull_completed":
                td["completed"] = _strip_meta(e)
            elif t == "file_pulled":
                td["files"].append({
                    "filename":   e.get("filename"),
                    "remote_key": e.get("remote_key"),
                    "status":     e.get("status"),
                    "rows":       e.get("rows"),
                    "bytes":      e.get("bytes"),
                    "duration_s": e.get("duration_s"),
                    "error":      e.get("error"),
                })

    r["_state_order"] = state_order
    return r


# == HTML rendering =============================================================

CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    font-size: 14px;
    background: #0f1117;
    color: #c9d1d9;
    padding: 32px 24px;
    line-height: 1.5;
}
a { color: #58a6ff; text-decoration: none; }

/* == Layout == */
.page { max-width: 960px; margin: 0 auto; }

/* == Header == */
.run-header {
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 20px 24px;
    margin-bottom: 28px;
    background: #161b22;
}
.run-title {
    font-size: 20px;
    font-weight: 600;
    color: #e6edf3;
    margin-bottom: 8px;
    font-family: "SFMono-Regular", Consolas, monospace;
}
.run-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    color: #8b949e;
    font-size: 13px;
}
.run-meta span strong { color: #c9d1d9; }
.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
}
.badge-ok   { background: #1f4f2a; color: #56d364; border: 1px solid #238636; }
.badge-err  { background: #4f1f1f; color: #f85149; border: 1px solid #da3633; }
.badge-warn { background: #4f3b1f; color: #e3b341; border: 1px solid #9e6a03; }
.badge-neutral { background: #21262d; color: #8b949e; border: 1px solid #30363d; }

/* == State cards (collapsible) == */
.state-card {
    border: 1px solid #30363d;
    border-radius: 8px;
    margin-bottom: 20px;
    background: #161b22;
    overflow: hidden;
}
.state-card > summary {
    list-style: none;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 14px 20px;
    border-bottom: 1px solid #21262d;
    background: #1c2128;
    user-select: none;
}
.state-card > summary::-webkit-details-marker { display: none; }
.state-card[open] > summary { border-bottom-color: #30363d; }
.state-card > summary::before {
    content: "▶";
    font-size: 10px;
    color: #484f58;
    transition: transform 0.15s;
    flex-shrink: 0;
}
.state-card[open] > summary::before { transform: rotate(90deg); }
.state-name {
    font-size: 16px;
    font-weight: 600;
    color: #e6edf3;
    text-transform: capitalize;
    flex: 1;
}
.state-dur { font-family: monospace; color: #8b949e; font-size: 13px; }

/* == Pipeline stages == */
.stages { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }
.stage {
    padding: 16px 20px;
    border-right: 1px solid #30363d;
}
.stage:last-child { border-right: none; }
.stage-title {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #8b949e;
    margin-bottom: 10px;
}
.stage-stat {
    font-size: 13px;
    color: #c9d1d9;
    margin-bottom: 4px;
}
.stage-stat .num {
    font-family: "SFMono-Regular", Consolas, monospace;
    color: #e6edf3;
    font-weight: 600;
}
.stage-stat .dim { color: #8b949e; }
.stage-empty { color: #484f58; font-style: italic; font-size: 13px; }

/* == File tables == */
.detail-section {
    padding: 0 20px 16px;
    border-top: 1px solid #30363d;
}
.detail-title {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #8b949e;
    padding: 12px 0 8px;
}
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12.5px;
}
th {
    text-align: left;
    color: #8b949e;
    font-weight: 500;
    padding: 4px 10px 4px 0;
    border-bottom: 1px solid #21262d;
}
td {
    padding: 5px 10px 5px 0;
    border-bottom: 1px solid #21262d;
    color: #c9d1d9;
    vertical-align: top;
}
tr:last-child td { border-bottom: none; }
.mono { font-family: "SFMono-Regular", Consolas, monospace; }
.right { text-align: right; }
td.right { color: #e6edf3; }
.tag {
    display: inline-block;
    padding: 1px 6px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 500;
}
.tag-source   { background: #1f3d5a; color: #79c0ff; }
.tag-registry { background: #2d1f5a; color: #c084fc; }
.tag-output   { background: #1f4f2a; color: #56d364; }
.ok-text      { color: #56d364; }
.err-text     { color: #f85149; }
.warn-text    { color: #e3b341; }
.neutral-text { color: #8b949e; }

/* == Validate detail section == */
.validate-section {
    padding: 0 20px 16px;
    border-top: 1px solid #30363d;
}
.validate-alerts {
    display: flex;
    flex-direction: column;
    gap: 4px;
    margin-top: 8px;
}
.validate-check {
    font-size: 12.5px;
    color: #c9d1d9;
    display: flex;
    gap: 8px;
}
.validate-check .check-label { font-weight: 600; min-width: 52px; }
.validate-check .check-val   { font-family: "SFMono-Regular", Consolas, monospace; }
/* Grid of mini-tables — one per database table */
.validate-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 0 24px;
    margin-top: 4px;
}
.validate-mini-table { min-width: 0; }
.validate-mini-table .vmini-head {
    font-size: 11px;
    font-weight: 600;
    color: #8b949e;
    text-transform: capitalize;
    padding: 8px 0 4px;
    border-bottom: 1px solid #21262d;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.validate-mini-table .vmini-sub {
    font-size: 10.5px;
    color: #484f58;
    font-weight: 400;
}
.validate-mini-table table {
    width: 100%;
    font-size: 12px;
}
.validate-mini-table td {
    padding: 3px 6px 3px 0;
    border-bottom: 1px solid #161b22;
    white-space: nowrap;
}
.validate-mini-table td.pct { text-align: right; padding-right: 0; min-width: 44px; max-width: none; }
.validate-mini-table td.mark { text-align: right; padding-left: 4px; min-width: 16px; max-width: none; }
/* Breakdown rows: allow wrapping so long values don't blow out the column */
.validate-mini-table td.bval { color: #484f58; padding-left: 12px; font-style: italic; white-space: normal; word-break: break-word; }
.validate-mini-table td.bcnt { white-space: normal; }
.validate-mini-table td.bcnt { text-align: right; color: #8b949e; min-width: 60px; max-width: none; }

/* == Query terminal window == */
.query-section {
    border-top: 1px solid #30363d;
}
.query-section > summary {
    list-style: none;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 20px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #8b949e;
    user-select: none;
}
.query-section > summary::-webkit-details-marker { display: none; }
.query-section > summary::before {
    content: "▶";
    font-size: 9px;
    color: #484f58;
    transition: transform 0.15s;
}
.query-section[open] > summary::before { transform: rotate(90deg); }
.query-terminal {
    margin: 0 20px 16px;
    background: #0d1117;
    border: 1px solid #21262d;
    border-radius: 6px;
    overflow: auto;
    max-height: 520px;
}
.query-terminal pre {
    padding: 14px 16px;
    margin: 0;
    font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
    font-size: 11.5px;
    line-height: 1.55;
    color: #c9d1d9;
    white-space: pre;
    tab-size: 2;
}

/* == Aggregate == */
.aggregate-card {
    border: 1px solid #30363d;
    border-radius: 8px;
    background: #161b22;
    overflow: hidden;
    margin-bottom: 20px;
}
.aggregate-header {
    padding: 14px 20px;
    background: #1c2128;
    border-bottom: 1px solid #30363d;
    display: flex;
    align-items: center;
    gap: 12px;
}
.aggregate-header .state-name { font-size: 16px; font-weight: 600; color: #e6edf3; flex: 1; }
.aggregate-body { padding: 16px 20px; }

/* == Tab navigation (per-state, when a run covers more than one) == */
.tab-nav {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 16px;
}
.tab-btn {
    appearance: none;
    border: 1px solid #30363d;
    border-radius: 6px;
    background: #161b22;
    color: #8b949e;
    font: inherit;
    font-size: 13px;
    font-weight: 600;
    padding: 7px 14px;
    cursor: pointer;
    transition: background 0.1s, color 0.1s, border-color 0.1s;
}
.tab-btn:hover { color: #c9d1d9; border-color: #484f58; }
.tab-btn.active { background: #1c2128; color: #e6edf3; border-color: #58a6ff; }
.tab-dot {
    display: inline-block;
    width: 6px;
    height: 6px;
    border-radius: 50%;
    margin-left: 7px;
}
.tab-dot.ok      { background: #56d364; }
.tab-dot.err     { background: #f85149; }
.tab-dot.warn    { background: #e3b341; }
.tab-dot.neutral { background: #484f58; }
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* == Fixed section below tabs (aggregate / db push) == */
.below-tabs { margin-top: 4px; }
"""


def h(text):
    """Minimal HTML escaping."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def badge(status):
    cls = status_class(status)
    icon = status_icon(status)
    return f'<span class="badge badge-{cls}">{icon} {h(status)}</span>'


def render_scrape(sc):
    if not sc:
        return '<div class="stage"><div class="stage-title">Scrape</div><div class="stage-empty">skipped</div></div>'

    status = sc.get("status", "")
    dur    = sc.get("duration_s")
    pages_ok  = sc.get("pages_ok", 0) or 0
    pages_err = sc.get("pages_err", 0) or 0
    files_ok  = sc.get("files_ok", 0) or 0

    flags = []
    if sc.get("force"):        flags.append("force")
    if sc.get("entities"):     flags.append("entities")
    if sc.get("transactions"): flags.append("transactions")

    parts = []
    if files_ok:
        parts.append(f'<div class="stage-stat"><span class="num">{files_ok}</span> <span class="dim">files downloaded</span></div>')
    if pages_ok:
        err_str = f' <span class="err-text">/ {pages_err} err</span>' if pages_err else ""
        parts.append(f'<div class="stage-stat"><span class="num">{pages_ok:,}</span> <span class="dim">pages scraped</span>{err_str}</div>')
    if dur is not None:
        parts.append(f'<div class="stage-stat"><span class="dim">duration</span> <span class="num">{fmt_dur(dur)}</span></div>')
    if flags:
        parts.append(f'<div class="stage-stat"><span class="dim">flags</span> <span class="num">{"  ".join(flags)}</span></div>')

    sc_badge = badge(status) if status else ""
    return f'''<div class="stage">
<div class="stage-title">Scrape {sc_badge}</div>
{"".join(parts) or '<div class="stage-empty">no data</div>'}
</div>'''


def render_parse(pa):
    if not pa:
        return '<div class="stage"><div class="stage-title">Parse</div><div class="stage-empty">skipped</div></div>'

    status  = pa.get("status", "")
    dur     = pa.get("duration_s")
    totals  = pa.get("totals", {})

    parts = []
    for rel, cnt in totals.items():
        parts.append(f'<div class="stage-stat"><span class="num">{fmt_rows(cnt)}</span> <span class="dim">{h(rel)}</span></div>')
    if dur is not None:
        parts.append(f'<div class="stage-stat"><span class="dim">duration</span> <span class="num">{fmt_dur(dur)}</span></div>')

    pa_badge = badge(status) if status else ""
    return f'''<div class="stage">
<div class="stage-title">Parse {pa_badge}</div>
{"".join(parts) or '<div class="stage-empty">no data</div>'}
</div>'''


def render_validate(va):
    if not va:
        return '<div class="stage"><div class="stage-title">Validate</div><div class="stage-empty">skipped</div></div>'

    status = va.get("status", "")
    dur    = va.get("duration_s")
    t1     = va.get("tier1_failures", 0) or 0
    t2     = va.get("tier2_warnings", 0) or 0
    drift  = va.get("drift_warnings", 0) or 0

    t1_cls = "err-text" if t1 else "ok-text"
    t2_cls = "warn-text" if t2 else "dim"
    dr_cls = "warn-text" if drift else "dim"

    newest_record = va.get("newest_record")

    va_badge = badge(status) if status else ""
    return f'''<div class="stage">
<div class="stage-title">Validate {va_badge}</div>
<div class="stage-stat"><span class="{t1_cls} num">{t1}</span> <span class="dim">tier-1 failures</span></div>
<div class="stage-stat"><span class="{t2_cls} num">{t2}</span> <span class="dim">tier-2 warnings</span></div>
<div class="stage-stat"><span class="{dr_cls} num">{drift}</span> <span class="dim">drift warnings</span></div>
<div class="stage-stat"><span class="dim">newest record</span> <span class="num">{fmt_date(newest_record)}</span></div>
<div class="stage-stat"><span class="dim">duration</span> <span class="num">{fmt_dur(dur)}</span></div>
</div>'''


def render_tabulate(ta):
    if not ta:
        return '<div class="stage"><div class="stage-title">Tabulate</div><div class="stage-empty">skipped</div></div>'

    status = ta.get("status", "")
    dur    = ta.get("duration_s")
    db     = ta.get("db", "")
    tables = ta.get("tables", [])

    rows_total = sum(t.get("rows", 0) or 0 for t in tables)

    db_bytes = ta.get("bytes")
    ta_badge = badge(status) if status else ""
    return f'''<div class="stage">
<div class="stage-title">Tabulate {ta_badge}</div>
<div class="stage-stat"><span class="num">{fmt_rows(rows_total)}</span> <span class="dim">total rows</span></div>
<div class="stage-stat"><span class="num">{len(tables)}</span> <span class="dim">tables</span></div>
<div class="stage-stat"><span class="dim">duration</span> <span class="num">{fmt_dur(dur)}</span></div>
{"" if not db_bytes else f'<div class="stage-stat"><span class="dim">db size</span> <span class="num">{fmt_bytes(db_bytes)}</span></div>'}
{"" if not db else f'<div class="stage-stat"><span class="dim">db</span> <span class="num mono">{h(db)}</span></div>'}
</div>'''


def render_scrape_files(sc):
    downloaded = sc.get("downloaded", [])
    scraped    = sc.get("scraped", [])
    if not downloaded and not scraped:
        return ""

    sections = ""

    if downloaded:
        rows_html = ""
        for f in downloaded:
            st      = f.get("status", "")
            st_html = '<span class="ok-text">✓</span>' if st == "ok" else '<span class="err-text">✗</span>'
            name    = f.get("filename") or f'<span class="dim err-text">{h(f.get("error","error"))}</span>'
            rows_html += f"""<tr>
  <td style="width:20px">{st_html}</td>
  <td class="mono">{h(name) if f.get("filename") else name}</td>
  <td class="right">{fmt_rows(f.get("rows"))}</td>
  <td class="right">{fmt_bytes(f.get("bytes"))}</td>
  <td class="right dim">{fmt_dur(f.get("duration_s"))}</td>
</tr>"""
        sections += f'''<div class="detail-title">Downloaded files</div>
<table>
<thead><tr>
  <th style="width:20px"></th><th>File</th>
  <th class="right">Rows</th><th class="right">Size</th><th class="right">Time</th>
</tr></thead>
<tbody>{rows_html}</tbody>
</table>'''

    if scraped:
        rows_html = ""
        for f in scraped:
            st      = f.get("status", "")
            st_html = '<span class="ok-text">✓</span>' if st == "ok" else '<span class="err-text">✗</span>'
            ok      = f.get("ok", 0) or 0
            err     = f.get("err", 0) or 0
            err_str = f' <span class="err-text">/ {err} err</span>' if err else ""
            rows_html += f"""<tr>
  <td style="width:20px">{st_html}</td>
  <td class="mono">{h(f.get("filename",""))}</td>
  <td class="right">{fmt_rows(f.get("rows"))}</td>
  <td class="right">{fmt_bytes(f.get("bytes"))}</td>
  <td class="right"><span class="num">{ok:,}</span>{err_str} <span class="dim">pages</span></td>
  <td class="right dim">{fmt_dur(f.get("duration_s"))}</td>
</tr>"""
        sections += f'''<div class="detail-title">Scraped files</div>
<table>
<thead><tr>
  <th style="width:20px"></th><th>File</th>
  <th class="right">Rows</th><th class="right">Size</th>
  <th class="right">Pages</th><th class="right">Time</th>
</tr></thead>
<tbody>{rows_html}</tbody>
</table>'''

    return f'<div class="detail-section">{sections}</div>'


def render_parse_files(pa):
    files = pa.get("files", [])
    if not files:
        return ""

    # Group by role: source + registry → "parsed files"; output stays separate
    parsed_files = [f for f in files if f.get("role", "source") in ("source", "registry")]
    output_files = [f for f in files if f.get("role") == "output"]

    sections = ""

    if parsed_files:
        # Group by filename so multi-relation files (e.g. pcc_committees.csv →
        # committees + candidates) appear as a single row with combined relations.
        seen_files: dict[str, dict] = {}
        for f in parsed_files:
            fname = f.get("filename", "")
            if fname not in seen_files:
                seen_files[fname] = {**f, "_relations": [f.get("relation", "")]}
            else:
                seen_files[fname]["_relations"].append(f.get("relation", ""))

        rows_html = ""
        for f in seen_files.values():
            role      = f.get("role", "source")
            skipped   = f.get("skipped", 0) or 0
            sk_str    = f'<span class="warn-text">{skipped:,}</span>' if skipped else '<span class="dim">—</span>'
            tag       = f'<span class="tag tag-{role}">{role}</span>'
            relations = ", ".join(r for r in f["_relations"] if r)
            rows_html += f"""<tr>
  <td class="mono">{h(f.get("filename",""))}</td>
  <td>{tag}</td>
  <td>{h(relations)}</td>
  <td class="right">{fmt_rows(f.get("rows"))}</td>
  <td class="right">{fmt_bytes(f.get("bytes"))}</td>
  <td class="right">{sk_str}</td>
  <td class="right dim">{fmt_dur(f.get("duration_s"))}</td>
</tr>"""
        sections += '''<div class="detail-title">Parsed files</div>
<table>
<thead><tr>
  <th>File</th><th>Role</th><th>Relation</th><th class="right">Rows</th>
  <th class="right">Size</th><th class="right">Skipped</th><th class="right">Time</th>
</tr></thead>
<tbody>''' + rows_html + '''</tbody>
</table>'''

    if output_files:
        rows_html = ""
        for f in output_files:
            skipped = f.get("skipped", 0) or 0
            sk_str  = f'<span class="warn-text">{skipped:,}</span>' if skipped else '<span class="dim">—</span>'
            rows_html += f"""<tr>
  <td class="mono">{h(f.get("filename",""))}</td>
  <td>{h(f.get("relation",""))}</td>
  <td class="right">{fmt_rows(f.get("rows"))}</td>
  <td class="right">{fmt_bytes(f.get("bytes"))}</td>
  <td class="right">{sk_str}</td>
  <td class="right dim">{fmt_dur(f.get("duration_s"))}</td>
</tr>"""
        sections += '''<div class="detail-title"><span class="tag tag-output">output</span> files</div>
<table>
<thead><tr>
  <th>File</th><th>Relation</th><th class="right">Rows</th>
  <th class="right">Size</th><th class="right">Skipped</th><th class="right">Time</th>
</tr></thead>
<tbody>''' + rows_html + '''</tbody>
</table>'''

    enrichments = pa.get("enrichments", [])
    if enrichments:
        for en in enrichments:
            parts = " &nbsp;·&nbsp; ".join(
                f'<strong>{h(k)}</strong>: {fmt_rows(v) if isinstance(v,int) else h(v)}'
                for k, v in en.items()
            )
            sections += f'<div class="detail-title">Enrichment</div><div class="stage-stat">{parts}</div>'

    return f'<div class="detail-section">{sections}</div>'


def render_tabulate_tables(ta):
    status     = ta.get("status", "")
    error_type = ta.get("error_type")
    error      = ta.get("error")
    tables     = ta.get("tables", [])

    if not status and not tables:
        return ""

    header = ""
    if status:
        sc  = status_class(status)
        ico = status_icon(status)
        header = f'<div class="detail-title">Tabulate <span style="font-size:10px;font-weight:500" class="{sc}-text">{ico} {h(status)}</span></div>'

    error_html = ""
    if error_type or error:
        err_parts = []
        if error_type:
            err_parts.append(f'<span class="mono err-text">{h(error_type)}</span>')
        if error:
            err_parts.append(f'<span style="color:#c9d1d9">{h(error)}</span>')
        error_html = f'<div class="stage-stat" style="margin:6px 0">{"  ".join(err_parts)}</div>'

    table_html = ""
    if tables:
        rows_html = ""
        for t in tables:
            rows_html += f"""<tr>
  <td class="mono">{h(t.get("table",""))}</td>
  <td class="right">{fmt_rows(t.get("rows"))}</td>
  <td class="right dim">{fmt_dur(t.get("duration_s"))}</td>
</tr>"""
        table_html = f'''<table>
<thead><tr><th>Table</th><th class="right">Rows</th><th class="right">Time</th></tr></thead>
<tbody>{rows_html}</tbody>
</table>'''

    return f'<div class="detail-section">{header}{error_html}{table_html}</div>'


def _pct_class(rate: float, threshold: float = 99.0) -> str:
    if rate >= threshold:
        return "ok-text"
    if rate >= threshold - 10:
        return "warn-text"
    return "err-text"


def _vmini(table: str, total: int, rows_html: str, sampled: int | None = None) -> str:
    """One mini-table cell in the validate grid."""
    if sampled is not None:
        sub = f"sampled {total:,} of {sampled:,}"
    else:
        sub = f"{total:,} rows"
    return (f'<div class="validate-mini-table">'
            f'<div class="vmini-head">{h(table.capitalize())} '
            f'<span class="vmini-sub">({sub})</span></div>'
            f'<table><tbody>{rows_html}</tbody></table>'
            f'</div>')


def render_validate_report(vr: dict) -> str:
    """Render a {state}_validate.json report as a detail section.

    Expected keys (from src/pipeline/validate.py):
      passed, row_counts, tier1_failures, tier1_fill_rates,
      tier2_warnings, tier2_enrichment, tier2_breakdowns, drift_warnings
    """
    if not vr:
        return ""

    out = []

    # == Alert strip: failures / warnings / drift ===============================
    t1_failures    = vr.get("tier1_failures", [])
    t2_warnings    = vr.get("tier2_warnings", [])
    drift_warnings = vr.get("drift_warnings", [])

    alerts = []
    for item in t1_failures:
        table = item.get("table", "")
        check = item.get("check", "")
        for err in item.get("errors", []):
            alerts.append(f'<div class="validate-check"><span class="check-label err-text">tier-1</span>'
                          f'<span class="check-val err-text">&nbsp;[{h(table)}] {h(check)}: {h(err)}</span></div>')
    for item in t2_warnings:
        alerts.append(f'<div class="validate-check"><span class="check-label warn-text">tier-2</span>'
                      f'<span class="check-val warn-text">&nbsp;[{h(item.get("table",""))}] {h(item.get("warning",""))}</span></div>')
    for item in drift_warnings:
        prev = item.get("previous", 0); curr = item.get("current", 0)
        alerts.append(f'<div class="validate-check"><span class="check-label warn-text">drift</span>'
                      f'<span class="check-val warn-text">&nbsp;[{h(item.get("table",""))}] '
                      f'dropped {item.get("drop_pct",0)}%  ({prev:,} → {curr:,})</span></div>')

    ok_msg = ('' if t1_failures else
              '<div class="validate-check"><span class="check-val ok-text">✓ All tier-1 checks passed</span></div>')
    if ok_msg or alerts:
        out.append(f'<div class="validate-alerts">{ok_msg}{"".join(alerts)}</div>')

    sampled_tables = vr.get("sampled_tables", {})   # table → total when sampling applied

    # == Tier-1 fill rates grid =================================================
    fill_rates = vr.get("tier1_fill_rates", {})
    if fill_rates:
        cells = []
        for table, data in fill_rates.items():
            total  = data.get("_total", 0)
            fields = [k for k in data if k != "_total"]
            trows  = ""
            for field in fields:
                rate    = data[field]
                cls     = _pct_class(rate, threshold=99.0)
                ok_mark = "✓" if rate >= 99.0 else "✗"
                trows += (f'<tr><td class="mono">{h(field)}</td>'
                          f'<td class="pct"><span class="{cls}">{rate:.1f}%</span></td>'
                          f'<td class="mark"><span class="{cls}">{ok_mark}</span></td></tr>')
            cells.append(_vmini(table, total, trows,
                                sampled=sampled_tables.get(table)))
        out.append(f'<div class="detail-title" style="margin-top:12px">Tier-1 fill rates</div>'
                   f'<div class="validate-grid">{"".join(cells)}</div>')

    # == Tier-2 enrichment grid =================================================
    enrichment = vr.get("tier2_enrichment", {})
    breakdowns = vr.get("tier2_breakdowns", {})
    if enrichment:
        cells = []
        for table, data in enrichment.items():
            total  = data.get("total", 0)
            fields = [k for k in data if k != "total"]
            trows  = ""
            for field in fields:
                rate  = data[field]
                cls   = _pct_class(rate, threshold=80.0)
                trows += (f'<tr><td class="mono">{h(field)}</td>'
                          f'<td class="pct"><span class="{cls}">{rate:.1f}%</span></td></tr>')
            for bfield, counts in (breakdowns.get(table) or {}).items():
                trows += f'<tr><td colspan="2" class="bval">{h(bfield)}</td></tr>'
                for val, count in counts.items():
                    rate = round(100 * count / total, 1) if total else 0
                    trows += (f'<tr><td class="mono" style="padding-left:12px;color:#8b949e;white-space:normal;word-break:break-word">{h(val)}</td>'
                              f'<td class="bcnt">{count:,} <span class="dim">({rate:.0f}%)</span></td></tr>')
            cells.append(_vmini(table, total, trows,
                                sampled=sampled_tables.get(table)))
        out.append(f'<div class="detail-title" style="margin-top:12px">Tier-2 enrichment</div>'
                   f'<div class="validate-grid">{"".join(cells)}</div>')

    if not out:
        return ""

    return f'''<div class="validate-section">
<div class="detail-title">Validation detail</div>
{"".join(out)}
</div>'''


TAB_SCRIPT = '''<script>
function showTab(key) {
    document.querySelectorAll('.tab-btn').forEach(function(b) {
        b.classList.toggle('active', b.dataset.tab === key);
    });
    document.querySelectorAll('.tab-panel').forEach(function(p) {
        p.classList.toggle('active', p.dataset.tab === key);
    });
}
</script>'''


def render_tabs(entries: list[tuple[str, str, str, str]]) -> str:
    """entries: list of (key, label, status, panel_html), one per state.

    Renders a clickable tab bar (label + pass/fail dot) with one panel visible
    at a time, first entry active by default. A single-entry run (e.g. one
    state) skips the tab chrome entirely and just renders that panel — tabs
    only earn their keep once there's more than one thing to switch between.
    """
    if not entries:
        return ""
    if len(entries) == 1:
        return entries[0][3]

    nav_btns = ""
    panels   = ""
    for i, (key, label, status, panel_html) in enumerate(entries):
        active  = " active" if i == 0 else ""
        dot     = f'<span class="tab-dot {status_class(status)}"></span>' if status else ""
        nav_btns += (f'<button type="button" class="tab-btn{active}" data-tab="{h(key)}" '
                     f'onclick="showTab(\'{h(key)}\')">{h(label)}{dot}</button>')
        panels   += f'<div class="tab-panel{active}" data-tab="{h(key)}">{panel_html}</div>'

    return f'<div class="tab-nav">{nav_btns}</div><div class="tab-panels">{panels}</div>'


def render_query_output(query_text: str) -> str:
    """Render captured test_queries output as a collapsible terminal window."""
    if not query_text or not query_text.strip():
        return ""
    return f'''<details class="query-section">
  <summary>Spot-check queries</summary>
  <div class="query-terminal"><pre>{h(query_text.rstrip())}</pre></div>
</details>'''


def render_state(name, data, validate_report: dict | None = None,
                 query_output: str | None = None):
    sc = data.get("scrape", {})
    pa = data.get("parse", {})
    va = data.get("validate", {})
    ta = data.get("tabulate", {})

    overall_status = data.get("status", "")
    overall_dur    = data.get("duration_s")

    badge_html = badge(overall_status) if overall_status else ""
    dur_html   = f'<span class="state-dur">{fmt_dur(overall_dur)}</span>' if overall_dur else ""

    stages = (
        render_scrape(sc) +
        render_parse(pa) +
        render_validate(va) +
        render_tabulate(ta)
    )

    details = ""
    if sc:
        details += render_scrape_files(sc)
    if pa:
        details += render_parse_files(pa)
    if validate_report:
        details += render_validate_report(validate_report)
    if ta:
        details += render_tabulate_tables(ta)
    if query_output:
        details += render_query_output(query_output)

    return f'''<details class="state-card" open>
  <summary>
    <div class="state-name">{h(name.title())}</div>
    {badge_html}
    {dur_html}
  </summary>
  <div class="stages">{stages}</div>
  {details}
</details>'''


def render_aggregate(ag):
    if not ag:
        return ""

    status = ag.get("status", "")
    dur    = ag.get("duration_s")
    states = ag.get("states", [])
    tables = ag.get("tables", [])

    badge_html = badge(status) if status else ""
    dur_html   = f'<span class="state-dur">{fmt_dur(dur)}</span>' if dur else ""

    stats = ""
    if tables:
        total_rows = sum(t.get("rows", 0) or 0 for t in tables)
        stats += f'<div class="stage-stat"><span class="num">{fmt_rows(total_rows)}</span> <span class="dim">total rows across {len(tables)} table(s)</span></div>'
        for t in tables:
            stats += f'<div class="stage-stat"><span class="num">{fmt_rows(t.get("rows"))}</span> <span class="dim">{h(t.get("table",""))}</span> <span class="dim">({fmt_dur(t.get("duration_s"))})</span></div>'
    if states:
        stats += f'<div class="stage-stat"><span class="dim">states merged:</span> <span class="num">{", ".join(h(s) for s in states)}</span></div>'

    return f'''<div class="aggregate-card">
  <div class="aggregate-header">
    <div class="state-name">Aggregate</div>
    {badge_html}
    {dur_html}
  </div>
  <div class="aggregate-body">{stats}</div>
</div>'''


def load_query_outputs(run_dir: Path | None, state_names: list[str]) -> dict[str, str]:
    """Load {state}_queries.txt files from run_dir, keyed by state name."""
    if not run_dir or not run_dir.is_dir():
        return {}
    out = {}
    for name in state_names:
        p = run_dir / f"{name.lower()}_queries.txt"
        if p.exists():
            try:
                out[name] = p.read_text(encoding="utf-8")
            except Exception:
                pass
    return out


def load_validate_reports(run_dir: Path | None, state_names: list[str]) -> dict[str, dict]:
    """Load {state}_validate.json files from run_dir, keyed by state name."""
    if not run_dir or not run_dir.is_dir():
        return {}
    out = {}
    for name in state_names:
        p = run_dir / f"{name.lower()}_validate.json"
        if p.exists():
            try:
                out[name] = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
    return out


# == Push / pull rendering ======================================================

def render_push_file_table(files: list[dict], op: str) -> str:
    if not files:
        return ""
    delta_col = "Delta" if op == "push" else "Size"
    rows_html = ""
    for f in files:
        st = f.get("status", "")
        if st == "ok":
            st_html = '<span class="ok-text">✓</span>'
        elif st == "error":
            st_html = '<span class="err-text">✗</span>'
        elif st == "skipped":
            st_html = '<span class="neutral-text">–</span>'
        else:
            st_html = h(st)

        if op == "push":
            metric_html = f'<td class="right">{fmt_delta(f.get("delta_mb"))}</td>'
        else:
            metric_html = f'<td class="right">{fmt_bytes(f.get("bytes"))}</td>'

        err_html = (f'<td class="mono err-text" style="font-size:11px">{h(f.get("error",""))}</td>'
                    if f.get("error") else "<td></td>")
        key = f.get("remote_key") or f.get("filename") or ""
        rows_html += f"""<tr>
  <td style="width:20px">{st_html}</td>
  <td class="mono" style="font-size:11.5px;word-break:break-all">{h(key)}</td>
  <td class="right">{fmt_rows(f.get("rows"))}</td>
  {metric_html}
  <td class="right dim">{fmt_dur(f.get("duration_s"))}</td>
  {err_html}
</tr>"""
    return f'''<div class="detail-section">
<div class="detail-title">Files {op}ed</div>
<table><thead><tr>
  <th style="width:20px"></th><th>Remote key</th>
  <th class="right">Rows</th><th class="right">{delta_col}</th>
  <th class="right">Time</th><th>Error</th>
</tr></thead>
<tbody>{rows_html}</tbody></table>
</div>'''


def render_push_deleted_table(deleted: list[dict]) -> str:
    if not deleted:
        return ""
    rows_html = ""
    for d in deleted:
        st_html = ('<span class="ok-text">✓</span>' if d.get("status") == "ok"
                   else '<span class="err-text">✗</span>')
        err_html = (f'<td class="mono err-text" style="font-size:11px">{h(d.get("error",""))}</td>'
                    if d.get("error") else "<td></td>")
        rows_html += f"""<tr>
  <td style="width:20px">{st_html}</td>
  <td class="mono" style="font-size:11.5px;word-break:break-all">{h(d.get("remote_key",""))}</td>
  {err_html}
</tr>"""
    return f'''<div class="detail-section">
<div class="detail-title">Deleted from R2</div>
<table><thead><tr>
  <th style="width:20px"></th><th>Remote key</th><th>Error</th>
</tr></thead>
<tbody>{rows_html}</tbody></table>
</div>'''


def render_push_target(name: str, td: dict) -> str:
    op        = td.get("op", "push")
    completed = td.get("completed", {})
    files     = td.get("files", [])
    deleted   = td.get("deleted", [])
    manifest  = td.get("manifest", {})
    diff      = td.get("diff", {})

    status    = completed.get("status", "")
    dur       = completed.get("duration_s")
    files_ok  = completed.get("files_ok", 0) or 0
    files_err = completed.get("files_err", 0) or 0
    files_skipped = sum(1 for f in files if f.get("status") == "skipped")

    badge_html = badge(status) if status else ""
    dur_html   = f'<span class="state-dur">{fmt_dur(dur)}</span>' if dur else ""

    # Stage 1: files transferred
    err_str  = f' <span class="err-text">/ {files_err} err</span>' if files_err else ""
    skip_str = (f'<div class="stage-stat"><span class="neutral-text num">{files_skipped}</span>'
                f' <span class="dim">skipped (unchanged)</span></div>') if files_skipped else ""
    stage1 = f'''<div class="stage">
<div class="stage-title">Files</div>
<div class="stage-stat"><span class="num">{files_ok}</span> <span class="dim">{op}ed</span>{err_str}</div>
{skip_str}
<div class="stage-stat"><span class="dim">duration</span> <span class="num">{fmt_dur(dur)}</span></div>
</div>'''

    # Stage 2: delta (push) / downloaded size (pull)
    if op == "push":
        total_delta = sum((f.get("delta_mb") or 0) for f in files)
        del_str = (f'<div class="stage-stat"><span class="err-text num">{len(deleted)}</span>'
                   f' <span class="dim">deleted from R2</span></div>') if deleted else ""
        stage2 = f'''<div class="stage">
<div class="stage-title">Delta</div>
<div class="stage-stat"><span class="num">{fmt_delta(total_delta)}</span> <span class="dim">net change</span></div>
{del_str}
</div>'''
    else:
        total_bytes = sum((f.get("bytes") or 0) for f in files)
        stage2 = f'''<div class="stage">
<div class="stage-title">Downloaded</div>
<div class="stage-stat"><span class="num">{fmt_bytes(total_bytes)}</span> <span class="dim">total</span></div>
</div>'''

    # Stage 3: manifest (push only)
    if manifest:
        kept   = manifest.get("kept", 0) or 0
        pruned = manifest.get("pruned", 0) or 0
        prune_str = (f' <span class="warn-text">({pruned} pruned)</span>') if pruned else ""
        stage3 = f'''<div class="stage">
<div class="stage-title">Manifest</div>
<div class="stage-stat"><span class="num">{kept}</span> <span class="dim">entries</span>{prune_str}</div>
</div>'''
    else:
        stage3 = ""

    # Stage 4: diff
    if diff:
        matched = diff.get("matched", 0) or 0
        or_     = diff.get("only_remote", 0) or 0
        ol_     = diff.get("only_local", 0) or 0
        sm_     = diff.get("size_mismatch", 0) or 0
        or_str  = (f'<div class="stage-stat"><span class="warn-text num">{or_}</span>'
                   f' <span class="dim">only in R2</span></div>') if or_ else ""
        ol_str  = (f'<div class="stage-stat"><span class="warn-text num">{ol_}</span>'
                   f' <span class="dim">only local</span></div>') if ol_ else ""
        stage4 = f'''<div class="stage">
<div class="stage-title">Diff</div>
<div class="stage-stat"><span class="num">{matched}</span> <span class="dim">matched</span></div>
{or_str}{ol_str}
</div>'''
    else:
        stage4 = ""

    stages_html = stage1 + stage2 + stage3 + stage4
    details     = render_push_file_table(files, op)
    if deleted:
        details += render_push_deleted_table(deleted)

    # Display name: abbreviation for state names, keep "all"/"db"/"global" as-is
    if name in ("all", "db", "global"):
        display = name
    else:
        display = _to_abbr(name)

    return f'''<details class="state-card" open>
  <summary>
    <div class="state-name">{h(display)}</div>
    {badge_html}
    {dur_html}
  </summary>
  <div class="stages">{stages_html}</div>
  {details}
</details>'''


def render_html_push_pull(report: dict, source_path: Path) -> str:
    op        = report.get("report_type", "push")
    run_id    = (source_path.parent.name
                 if source_path.name == "log.jsonl"
                 else source_path.stem)
    targets   = report.get("targets", {})
    tgt_order = report.get("_target_order", [])

    # Aggregate totals across all targets
    total_ok  = sum(td.get("completed", {}).get("files_ok",  0) or 0 for td in targets.values())
    total_err = sum(td.get("completed", {}).get("files_err", 0) or 0 for td in targets.values())
    total_del = sum(len(td.get("deleted", [])) for td in targets.values())
    total_dur = sum(td.get("completed", {}).get("duration_s", 0) or 0 for td in targets.values())

    overall_status = "error" if total_err else "completed"
    summary_parts  = []
    if total_ok:
        summary_parts.append(f'<span><strong>{total_ok}</strong> {op}ed</span>')
    if total_del:
        summary_parts.append(f'<span><strong>{total_del}</strong> deleted</span>')
    if total_err:
        summary_parts.append(f'<span class="err-text"><strong>{total_err}</strong> errors</span>')
    if total_dur:
        summary_parts.append(f'<span><strong>{fmt_dur(total_dur)}</strong> total</span>')

    # Build targets label for header
    state_names = [t for t in tgt_order if t not in ("all", "db", "global")]
    if state_names:
        targets_str = ", ".join(_to_abbr(s) for s in state_names)
    elif "all" in tgt_order:
        targets_str = "all"
    elif "db" in tgt_order:
        targets_str = "db"
    else:
        targets_str = "—"

    header_html = f'''<div class="run-header">
  <div class="run-title">{h(run_id)}</div>
  <div class="run-meta">
    <span><strong>command</strong> {h(op)}</span>
    <span><strong>targets</strong> {h(targets_str)}</span>
    {badge(overall_status)}
    {"  ".join(summary_parts)}
  </div>
</div>'''

    # Individual states get their own tab (mirrors the sync report); db/all/
    # global targets aren't per-state, so they render as fixed cards below
    # the tab block instead of competing for a tab slot.
    state_keys = [k for k in tgt_order if k not in ("all", "db", "global")]
    other_keys = [k for k in tgt_order if k in ("all", "db", "global")]

    tab_entries = [
        (k, _to_abbr(k), targets[k].get("completed", {}).get("status", ""),
         render_push_target(k, targets[k]))
        for k in state_keys
    ]
    state_html  = render_tabs(tab_entries)
    other_html  = "".join(render_push_target(k, targets[k]) for k in other_keys)
    below_html  = f'<div class="below-tabs">{other_html}</div>' if (state_html and other_html) else other_html

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    footer = (f'<p style="text-align:center;color:#484f58;font-size:12px;margin-top:32px">'
              f'Generated {generated} · {h(source_path.name)}</p>')

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{h(run_id)}</title>
<style>{CSS}</style>
{TAB_SCRIPT}
</head>
<body>
<div class="page">
{header_html}
{state_html}
{below_html}
{footer}
</div>
</body>
</html>'''


def render_html(report: dict, source_path: Path, run_dir: Path | None = None) -> str:
    if report.get("report_type") in ("push", "pull"):
        return render_html_push_pull(report, source_path)

    run = report.get("run", {})
    run_id   = run.get("run_id", source_path.stem)
    command  = run.get("command", "—")
    states   = run.get("states", [])
    ts_start = run.get("ts_start", "")
    ts_end   = run.get("ts_end", "")
    status   = run.get("status", "")
    dur      = run.get("duration_s")
    passed   = run.get("passed")
    failed   = run.get("failed")

    states_str = ", ".join(s.upper() for s in states) if states else "—"

    summary_parts = []
    if passed is not None:
        summary_parts.append(f'<span><strong>{passed}</strong> passed</span>')
    if failed is not None:
        cls = "err-text" if failed else ""
        summary_parts.append(f'<span class="{cls}"><strong>{failed}</strong> failed</span>')
    if dur is not None:
        summary_parts.append(f'<span><strong>{fmt_dur(dur)}</strong> total</span>')

    header_html = f'''<div class="run-header">
  <div class="run-title">{h(run_id)}</div>
  <div class="run-meta">
    <span><strong>command</strong> {h(command)}</span>
    <span><strong>states</strong> {h(states_str)}</span>
    <span><strong>started</strong> {h(ts_display(ts_start))}</span>
    {"<span><strong>ended</strong> " + h(ts_display(ts_end)) + "</span>" if ts_end else ""}
    {badge(status) if status else ""}
    {"  ".join(summary_parts)}
  </div>
</div>'''

    state_order = report.get("_state_order", [])
    validate_reports = load_validate_reports(run_dir, state_order)
    query_outputs    = load_query_outputs(run_dir, state_order)

    tab_entries = []
    for name in state_order:
        vr = validate_reports.get(name)
        qo = query_outputs.get(name)
        status = report["states"][name].get("status", "")
        panel  = render_state(name, report["states"][name],
                              validate_report=vr, query_output=qo)
        tab_entries.append((name, name.title(), status, panel))
    state_html = render_tabs(tab_entries)

    agg_html = render_aggregate(report.get("aggregate", {}))
    below_html = f'<div class="below-tabs">{agg_html}</div>' if (state_html and agg_html) else agg_html

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    footer = f'<p style="text-align:center;color:#484f58;font-size:12px;margin-top:32px">Generated {generated} · {h(source_path.name)}</p>'

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{h(run_id)}</title>
<style>{CSS}</style>
{TAB_SCRIPT}
</head>
<body>
<div class="page">
{header_html}
{state_html}
{below_html}
{footer}
</div>
</body>
</html>'''


# == CLI ========================================================================

def main():
    parser = argparse.ArgumentParser(description="Convert a pipeline JSONL log to HTML.")
    parser.add_argument("input", help="Path to a run directory or log.jsonl file")
    parser.add_argument("-o", "--output", help="Output HTML path (default: report.html in run dir)")
    args = parser.parse_args()

    in_path = Path(args.input)

    # Accept either a run directory or a direct path to log.jsonl
    if in_path.is_dir():
        run_dir  = in_path
        log_path = run_dir / "log.jsonl"
    else:
        log_path = in_path
        run_dir  = in_path.parent if in_path.name == "log.jsonl" else None

    if not log_path.exists():
        print(f"[!] Log file not found: {log_path}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        out_path = Path(args.output)
    elif run_dir:
        out_path = run_dir / "report.html"
    else:
        out_path = log_path.with_suffix(".html")

    events = load_events(log_path)
    report = build_report(events)
    html   = render_html(report, log_path, run_dir=run_dir)

    out_path.write_text(html, encoding="utf-8")
    print(f"  ✓ {out_path}")


if __name__ == "__main__":
    main()
