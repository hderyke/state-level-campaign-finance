"""
src/emailer.py — Run-summary email, sent by daemon.py after every daemon invocation.

NOTE: deliberately not named email.py — `python3 src/daemon.py` causes Python
to prepend src/ itself to sys.path, and a module named email.py there shadows
the stdlib email package for every other library that imports it (urllib3,
smtplib, etc.), breaking the whole venv. Keep this name.

Reads the JSONL event logs already written by orc.py / src/pipeline/validate.py /
cloud/s3.py / src/pipeline/aggregate.py for the run (no new instrumentation
needed beyond what src/main.py's _sync_with_fallback now emits for rollback
tracking), builds a compact HTML summary, attaches the run's report.html
file(s), and sends it via the Resend API.

Env vars (see .env):
    RESEND_API_KEY   required to actually send; if unset, the composed email
                     is printed to stdout instead (safe local/dry-run default).
    RESEND_FROM      sender, e.g. "Campaign Finance <onboarding@resend.dev>".
                     onboarding@resend.dev only delivers to the Resend
                     account's own signup address until a domain is verified
                     — see https://resend.com/docs/dashboard/domains/introduction
    EMAIL_TO         recipient address (comma-separated for multiple).

Usage (called from daemon.py, not run standalone):
    from src import emailer
    run_dir = emailer.find_run_dir("sync", ["IN"], after=t0)
    emailer.send_run_summary(
        command="sync", state_abbrs=["IN"],
        sync_run_dir=run_dir, push_run_dir=push_dir, push_db_run_dir=None,
        agg_ok=True, daemon_duration_s=142.3,
    )
"""

import base64
import csv
import os
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.reporting.log_report import load_events

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGS_PROD    = PROJECT_ROOT / "logs" / "prod"
STATES_CSV   = PROJECT_ROOT / "src" / "aliases" / "states.csv"

# Load .env directly rather than relying on caller import order — daemon.py
# and main.py both trigger this via run_helpers already, but emailer.py should
# work standalone too (e.g. `python3 -m src.emailer` for a manual test send).
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

RESEND_API_URL = "https://api.resend.com/emails"

# abbr → lowercase full name — read directly rather than importing orc.py,
# which drags in aggregate.py (and duckdb) just for this lookup table.
with open(STATES_CSV, encoding="utf-8") as _f:
    ABBR_TO_NAME: dict[str, str] = {
        row["abbr"]: row["name"].lower() for row in csv.DictReader(_f)
    }


# ============================ Run-dir lookup ==============================

def _expected_states_suffix(state_abbrs: list[str]) -> str:
    if len(state_abbrs) == 1 and state_abbrs[0].lower() == "all":
        return "all"
    return "-".join(a.upper() for a in state_abbrs)


def find_run_dir(command_word: str, state_abbrs: list[str],
                 after: float | None = None) -> Path | None:
    """Most recent logs/prod/{ts}_{command_word}[_force]_{STATES} dir.

    `after` (epoch seconds, e.g. from time.time() at the start of the daemon
    step) restricts the match to dirs created at/after that point, so a stale
    prior run with the same state combo isn't picked up by mistake.
    """
    if not LOGS_PROD.exists():
        return None
    suffix  = re.escape(_expected_states_suffix(state_abbrs))
    pattern = re.compile(rf"^\d{{8}}_\d{{6}}_{re.escape(command_word)}(_force)?_{suffix}$")
    candidates = [
        d for d in LOGS_PROD.iterdir()
        if d.is_dir() and pattern.match(d.name)
        and (after is None or d.stat().st_mtime >= after)
    ]
    return max(candidates, key=lambda d: d.name) if candidates else None


# ============================ Event extraction =============================

def _events(run_dir: Path | None) -> list[dict]:
    if not run_dir:
        return []
    log_path = run_dir / "log.jsonl"
    if not log_path.exists():
        return []
    return load_events(log_path)


def _gather_sync_data(sync_events: list[dict]) -> dict:
    """state_abbr → {name, passed, tier1_failures, tier2_warnings,
    drift_warnings, newest_record, row_counts, rolled_back, rollback_status}"""
    states: dict[str, dict] = {}

    for e in sync_events:
        t = e.get("type")

        if t == "state_duration":
            name = e.get("state")
            states.setdefault(name, {"name": name})["passed"] = (e.get("status") == "passed")
            states[name]["duration_s"] = e.get("duration_s")

        elif t == "validate_completed" and e.get("operation") == "validate":
            name = e.get("state")
            d = states.setdefault(name, {"name": name})
            d["tier1_failures"] = e.get("tier1_failures", 0)
            d["tier2_warnings"] = e.get("tier2_warnings", 0)
            d["drift_warnings"] = e.get("drift_warnings", 0)
            d["newest_record"]  = e.get("newest_record")
            d["row_counts"]     = e.get("row_counts", {})

        elif t == "fallback_restore":
            abbr = e.get("state_abbr")
            name = ABBR_TO_NAME.get(abbr, abbr)
            d = states.setdefault(name, {"name": name})
            d["rolled_back"]     = (e.get("status") == "ok")
            d["rollback_status"] = e.get("status")

    return states


def _gather_aggregate(sync_events: list[dict]) -> dict | None:
    for e in reversed(sync_events):
        if e.get("type") == "aggregate_completed":
            return {
                "status":     e.get("status"),
                "duration_s": e.get("duration_s"),
                "totals":     e.get("totals", {}),
                "tables_err": e.get("tables_err", 0),
            }
    return None


def _gather_push_data(push_events: list[dict]) -> dict:
    """state name (or 'db' for the aggregate db push) → {status, files_ok,
    files_err, bytes_total, delta_mb_total}"""
    states: dict[str, dict] = {}

    for e in push_events:
        t = e.get("type")
        key = e.get("state") or "db"

        if t == "push_completed":
            d = states.setdefault(key, {"bytes_total": 0, "delta_mb_total": 0.0})
            d["status"]     = e.get("status")
            d["files_ok"]   = e.get("files_ok", 0)
            d["files_err"]  = e.get("files_err", 0)
            d["duration_s"] = e.get("duration_s")

        elif t == "file_pushed" and e.get("status") == "ok":
            d = states.setdefault(key, {"bytes_total": 0, "delta_mb_total": 0.0})
            d["bytes_total"]     += e.get("bytes") or 0
            d["delta_mb_total"]  += e.get("delta_mb") or 0.0

    return states


# ============================ Formatting helpers ===========================

def _fmt_date(iso_str) -> str:
    if not iso_str:
        return "—"
    return iso_str

def _fmt_bytes(n) -> str:
    if not n:
        return "—"
    mb = n / 1_048_576
    return f"{mb:.1f} MB" if mb >= 1 else f"{n/1024:.1f} KB"

def _fmt_delta(mb) -> str:
    if not mb:
        return "—"
    sign = "+" if mb > 0 else ""
    return f"{sign}{mb:.2f} MB"

def _fmt_dur(s) -> str:
    if s is None:
        return "—"
    if s >= 60:
        return f"{int(s)//60}m {s - (int(s)//60)*60:.0f}s"
    return f"{s:.1f}s"


# =============================== HTML body ==================================

def _row(cells: list[str], bold: bool = False) -> str:
    style = "font-weight:600;" if bold else ""
    tds = "".join(
        f'<td style="padding:6px 10px;border-bottom:1px solid #e5e5e5;'
        f'font-size:13px;{style}">{c}</td>'
        for c in cells
    )
    return f"<tr>{tds}</tr>"


def build_email_html(command: str, state_abbrs: list[str],
                     sync_states: dict, aggregate: dict | None,
                     push_states: dict, agg_ok: bool,
                     daemon_duration_s: float) -> tuple[str, str]:
    """Returns (subject, html_body)."""
    ordered = sorted(sync_states.values(), key=lambda d: d.get("name") or "")

    n_pass = sum(1 for s in ordered if s.get("passed"))
    n_fail = sum(1 for s in ordered if s.get("passed") is False)
    n_rollback = sum(1 for s in ordered if s.get("rolled_back"))

    status_word = "OK" if (n_fail == 0 and agg_ok) else ("PARTIAL" if agg_ok else "FAILED")
    subject = (f"[Campaign Finance] {command} {'-'.join(a.upper() for a in state_abbrs)} "
               f"— {status_word} ({n_pass} passed, {n_fail} failed"
               f"{f', {n_rollback} rolled back' if n_rollback else ''})")

    header = f'''
    <h2 style="font-family:-apple-system,Helvetica,Arial,sans-serif;margin:0 0 4px">
      {command} {'-'.join(a.upper() for a in state_abbrs)} — {status_word}
    </h2>
    <p style="font-family:-apple-system,Helvetica,Arial,sans-serif;color:#666;font-size:13px;margin:0 0 16px">
      daemon runtime {_fmt_dur(daemon_duration_s)} · aggregate {"passed" if agg_ok else "failed"}
    </p>'''

    # -- per-state table --------------------------------------------------
    head_cells = ["State", "Sync", "Rolled back", "Newest record",
                  "Tier-1 / Tier-2 / Drift", "Push", "Byte delta"]
    rows = [_row(head_cells, bold=True)]
    for s in ordered:
        name = (s.get("name") or "").title()
        passed = s.get("passed")
        sync_cell = ("<span style='color:#1a7f37'>✓ passed</span>" if passed
                     else "<span style='color:#c0392b'>✗ failed</span>" if passed is False
                     else "—")
        rb = s.get("rollback_status")
        rb_cell = ({"ok": "<span style='color:#b8860b'>↩ restored from S3</span>",
                    "no_data": "<span style='color:#c0392b'>✗ no fallback data</span>",
                    "error": "<span style='color:#c0392b'>✗ restore failed</span>"}
                   .get(rb, "—"))
        tiers = (f"{s.get('tier1_failures', '—')} / "
                 f"{s.get('tier2_warnings', '—')} / "
                 f"{s.get('drift_warnings', '—')}") if "tier1_failures" in s else "—"

        push = push_states.get((s.get("name") or "").lower())
        if push:
            push_cell  = "✓ pushed" if push.get("status") == "completed" else f"✗ {push.get('status')}"
            delta_cell = _fmt_delta(push.get("delta_mb_total"))
        else:
            push_cell, delta_cell = "—", "—"

        rows.append(_row([name, sync_cell, rb_cell, _fmt_date(s.get("newest_record")),
                          tiers, push_cell, delta_cell]))

    table = (f'<table style="border-collapse:collapse;width:100%;'
             f'font-family:-apple-system,Helvetica,Arial,sans-serif">{"".join(rows)}</table>')

    # -- aggregate + db push section ---------------------------------------
    agg_parts = []
    if aggregate:
        totals = ", ".join(f"{k} {v:,}" for k, v in aggregate.get("totals", {}).items())
        agg_parts.append(
            f'<p style="font-family:-apple-system,Helvetica,Arial,sans-serif;font-size:13px">'
            f'<strong>Aggregate:</strong> {aggregate.get("status")} '
            f'({_fmt_dur(aggregate.get("duration_s"))}) — {totals or "no totals"}</p>')
    db = push_states.get("db")
    if db:
        agg_parts.append(
            f'<p style="font-family:-apple-system,Helvetica,Arial,sans-serif;font-size:13px">'
            f'<strong>DB push:</strong> {db.get("status")} '
            f'({_fmt_bytes(db.get("bytes_total"))}, {_fmt_delta(db.get("delta_mb_total"))})</p>')

    html = header + table + "".join(agg_parts) + (
        '<p style="font-family:-apple-system,Helvetica,Arial,sans-serif;'
        'color:#999;font-size:11px;margin-top:16px">'
        'Full report(s) attached. Raw logs under logs/prod/ on the Mac Mini.</p>'
    )
    return subject, html


# ================================ Sending ===================================

def _attachment(path: Path) -> dict:
    content = base64.b64encode(path.read_bytes()).decode("ascii")
    return {"filename": path.name, "content": content}


def _collect_attachments(run_dirs: list[Path | None]) -> list[dict]:
    atts = []
    for run_dir in run_dirs:
        if not run_dir:
            continue
        report = run_dir / "report.html"
        if report.exists():
            # Disambiguate by run dir name so "sync ... report.html" and
            # "push ... report.html" don't collide as attachments.
            att = _attachment(report)
            att["filename"] = f"{run_dir.name}_report.html"
            atts.append(att)
    return atts


def _send(subject: str, html: str, attachments: list[dict]) -> None:
    api_key = os.environ.get("RESEND_API_KEY")
    sender  = os.environ.get("RESEND_FROM", "Campaign Finance <onboarding@resend.dev>")
    to_env  = os.environ.get("EMAIL_TO", "")
    to      = [a.strip() for a in to_env.split(",") if a.strip()]

    if not api_key or not to:
        print("\n[email] RESEND_API_KEY or EMAIL_TO not set — printing summary instead of sending:")
        print(f"  Subject: {subject}")
        print(f"  ({len(attachments)} attachment(s) would be sent)")
        return

    payload = {"from": sender, "to": to, "subject": subject, "html": html}
    if attachments:
        payload["attachments"] = attachments

    try:
        resp = requests.post(
            RESEND_API_URL,
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        if resp.status_code >= 300:
            print(f"[email] Resend API error {resp.status_code}: {resp.text}")
        else:
            print(f"[email] sent — {resp.json().get('id', '?')}")
    except Exception as e:
        print(f"[email] failed to send: {e}")


# =============================== Public API =================================

def send_run_summary(command: str, state_abbrs: list[str],
                     sync_run_dir: Path | None, push_run_dir: Path | None,
                     push_db_run_dir: Path | None, agg_ok: bool,
                     daemon_duration_s: float) -> None:
    """Build and send (or print, if unconfigured) the run summary email.
    Never raises — a broken email shouldn't fail a daemon run that otherwise
    succeeded."""
    try:
        sync_events = _events(sync_run_dir)
        push_events = _events(push_run_dir) + _events(push_db_run_dir)

        sync_states = _gather_sync_data(sync_events)
        aggregate   = _gather_aggregate(sync_events)
        push_states = _gather_push_data(push_events)

        subject, html = build_email_html(
            command, state_abbrs, sync_states, aggregate,
            push_states, agg_ok, daemon_duration_s,
        )
        attachments = _collect_attachments([sync_run_dir, push_run_dir, push_db_run_dir])
        _send(subject, html, attachments)
    except Exception as e:
        print(f"[email] run summary failed to build: {e}")
