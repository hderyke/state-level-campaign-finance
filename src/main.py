"""
src/main.py — Single entry point for the campaign finance pipeline.

  Pipeline commands          stages run
  ─────────────────────────────────────────────────────────────────
  update <states>                scrape → parse → validate → tabulate → aggregate
  rescrape <states>              scrape --force → parse → validate → tabulate → aggregate
  reparse <states>               parse → validate → tabulate → aggregate  (skip scrape)
  update-entities <states>       scrape --entities → parse → validate → tabulate → aggregate
  update-transactions <states>   scrape --transactions → parse → validate → tabulate → aggregate
  rescrape-entities <states>     scrape --force --entities → parse → validate → tabulate → aggregate
  rescrape-transactions <states> scrape --force --transactions → parse → validate → tabulate → aggregate

  Data commands
  ─────────────────────────────────────────────────────────────────
  push <states|all|db>       upload to Cloudflare R2
  pull <states|all|db>       download from Cloudflare R2

  States:  two-letter abbreviations  AL AK AZ AR CA CO ...
           or 'all' to run every known state

  Flags:   --daemon           silent mode for scheduled/cron runs
           --no-report        skip HTML report generation after run

  Examples
  ─────────────────────────────────────────────────────────────────
  python3 src/main.py update AL AK AZ
  python3 src/main.py rescrape AL
  python3 src/main.py push all
  python3 src/main.py pull db
  python3 src/main.py --daemon update all
"""

import os
import sys
from datetime import datetime
from pathlib import Path


# =========================== Configuration ===========================
# Load .env (R2 credentials, etc.) before anything else
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass  # dotenv optional — env vars may already be set

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "pipeline"))

from src import orc
from src import cloudflare

PIPELINE_COMMANDS = {"update", "rescrape", "reparse",
                     "update-entities", "update-transactions",
                     "rescrape-entities", "rescrape-transactions"}
DATA_COMMANDS     = {"push", "pull"}
ALL_COMMANDS      = PIPELINE_COMMANDS | DATA_COMMANDS

# Set True to automatically generate an HTML report after every run.
AUTO_REPORT = True


def _print_help():
    print(__doc__)


# ====================== Push / pull dispatch =========================

def _push(targets: list[str]):
    """Route a push command to the appropriate cloudflare helper."""
    if not targets:
        print("[!] push requires a target: <states>, all, or db")
        sys.exit(1)

    if len(targets) == 1 and targets[0].lower() == "db":
        # aggregate DB only
        db_path = PROJECT_ROOT / "data" / "state-level-cf.db"
        if not db_path.exists():
            print(f"[!] Aggregate db not found: {db_path}")
            sys.exit(1)
        cloudflare.push_file(db_path, "data/state-level-cf.db")

    elif len(targets) == 1 and targets[0].lower() == "all":
        # all state data directories
        cloudflare.push_all(PROJECT_ROOT)

    else:
        # one or more state abbreviations
        for abbr in targets:
            state_name = orc.ABBR_TO_NAME.get(abbr.upper())
            if not state_name:
                print(f"[!] Unknown state: {abbr}")
                sys.exit(1)
            # Capitalize to match data/ directory name (e.g. Alabama)
            cloudflare.push_state(state_name.capitalize(), PROJECT_ROOT)


def _pull(targets: list[str]):
    """Route a pull command to the appropriate cloudflare helper."""
    if not targets:
        print("[!] pull requires a target: <states>, all, or db")
        sys.exit(1)

    if len(targets) == 1 and targets[0].lower() == "db":
        # aggregate DB only
        db_path = PROJECT_ROOT / "data" / "state-level-cf.db"
        cloudflare.pull_file("data/state-level-cf.db", db_path)

    elif len(targets) == 1 and targets[0].lower() == "all":
        # all state data directories
        cloudflare.pull_all(PROJECT_ROOT)

    else:
        # one or more state abbreviations
        for abbr in targets:
            state_name = orc.ABBR_TO_NAME.get(abbr.upper())
            if not state_name:
                print(f"[!] Unknown state: {abbr}")
                sys.exit(1)
            cloudflare.pull_state(state_name.capitalize(), PROJECT_ROOT)


# ========================== Entry point ==============================

def main():
    """Parse top-level CLI args and dispatch to orc, push, or pull."""
    args = sys.argv[1:]

    # Strip --daemon and --no-report flags wherever they appear
    daemon    = "--daemon" in args
    no_report = "--no-report" in args
    args      = [a for a in args if a not in ("--daemon", "--no-report")]

    if not args or args[0] in ("-h", "--help"):
        _print_help()
        sys.exit(0)

    command = args[0]
    targets = args[1:]

    if command not in ALL_COMMANDS:
        print(f"[!] Unknown command: {command!r}")
        print(f"    Valid commands: {', '.join(sorted(ALL_COMMANDS))}")
        sys.exit(1)

    if not targets:
        print(f"[!] {command} requires at least one target (state abbreviation, all, or db)")
        sys.exit(1)

    # Daemon mode — silence console, subprocesses inherit this
    if daemon:
        os.environ["CF_DAEMON"] = "1"

    # Push/pull get named run IDs here; pipeline commands let orc._setup_run_id handle it
    if command in DATA_COMMANDS and not os.environ.get("CF_RUN_ID"):
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        tgt = (targets[0].lower()
               if len(targets) == 1 and targets[0].lower() in ("all", "db")
               else "-".join(t.upper() for t in targets))
        os.environ["CF_RUN_ID"] = f"{ts}_{command}_{tgt}"

    try:
        if command in PIPELINE_COMMANDS:
            orc.main(command, targets)

        elif command == "push":
            _push(targets)

        elif command == "pull":
            _pull(targets)

    finally:
        # Generate the HTML report in all exit paths — normal completion,
        # KeyboardInterrupt, and unhandled exceptions — as long as there is
        # a log file to render. This ensures Ctrl+C still produces a report
        # that shows how far the run got and what was logged before the interrupt.
        if AUTO_REPORT and not no_report:
            run_id = os.environ.get("CF_RUN_ID")
            if run_id:
                run_dir  = PROJECT_ROOT / "logs" / "prod" / run_id
                log_path = run_dir / "log.jsonl"
                if log_path.exists():
                    try:
                        from src.reporting import log_report
                        report   = log_report.build_report(log_report.load_events(log_path))
                        html     = log_report.render_html(report, log_path, run_dir=run_dir)
                        out_path = run_dir / "report.html"
                        out_path.write_text(html, encoding="utf-8")
                        print(f"  ✓ report → {out_path.relative_to(PROJECT_ROOT)}")
                    except Exception as report_err:
                        print(f"  [!] report generation failed: {report_err}")

# =============== CLI ======================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
