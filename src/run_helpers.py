"""
src/run_helpers.py — shared bootstrap for main.py and daemon.py.

Importing this module:
  - loads .env (dotenv, if installed)
  - inserts PROJECT_ROOT and src/pipeline onto sys.path

Callable helpers:
  setup_run(command, state_abbrs, daemon)  → sets CF_RUN_ID / CF_DAEMON, returns run_id
  generate_report(run_id, no_report)       → builds report.html in logs/prod/{run_id}/
                                              (or logs/daemon/{run_id}/ if CF_DAEMON is set)
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# ── Bootstrap (runs at import time) ──────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[1]

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass  # dotenv optional — env vars may already be set

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "src" / "pipeline") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def setup_run(command: str, state_abbrs: list[str], daemon: bool = False) -> str:
    """Set CF_RUN_ID and (optionally) CF_DAEMON in the environment.

    Pipeline commands (sync/reparse) set their own run ID inside orc._setup_run_id;
    this helper is only needed for data commands (push/pull) and the daemon, which
    run outside orc's normal flow.

    Returns the run_id string.
    """
    if daemon:
        os.environ["CF_DAEMON"] = "1"

    run_id = os.environ.get("CF_RUN_ID")
    if not run_id:
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        tgt = "-".join(a.upper() for a in state_abbrs) if state_abbrs else "all"
        run_id = f"{ts}_{command}_{tgt}"
        os.environ["CF_RUN_ID"] = run_id

    return run_id


def generate_report(run_id: str, no_report: bool = False) -> None:
    """Build the HTML report for a completed run, if one hasn't been suppressed."""
    if no_report or not run_id:
        return

    from src.reporting.logger import run_dir_for
    run_dir  = run_dir_for(run_id)
    log_path = run_dir / "log.jsonl"

    if not log_path.exists():
        return

    try:
        from src.reporting import log_report
        report   = log_report.build_report(log_report.load_events(log_path))
        html     = log_report.render_html(report, log_path, run_dir=run_dir)
        out_path = run_dir / "report.html"
        out_path.write_text(html, encoding="utf-8")
        print(f"  ✓ report → {out_path.relative_to(PROJECT_ROOT)}")
    except Exception as e:
        print(f"  [!] report generation failed: {e}")
