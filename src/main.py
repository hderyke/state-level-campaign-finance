"""
src/main.py — Single entry point for the campaign finance pipeline.

  Pipeline commands          stages run
  ─────────────────────────────────────────────────────────────────
  sync <states>              scrape → parse → validate → tabulate → aggregate
  reparse <states>           parse → validate → tabulate → aggregate  (skip scrape)

  Data commands
  ─────────────────────────────────────────────────────────────────
  push <states|all|db>       upload to Cloudflare R2
  pull <states|all|db>       download from Cloudflare R2

  States:  two-letter abbreviations  AL AK AZ AR CA CO ...
           or 'all' to run every known state

  Global flags
  ─────────────────────────────────────────────────────────────────
  --daemon           silent mode for scheduled/cron runs
  --no-report        skip HTML report generation after run

  Scraper flags (forwarded to the scraper subprocess; orc validates before passing)
  ─────────────────────────────────────────────────────────────────
  Vertical scope — mutually exclusive:
    --force                    re-download all years in scope, wipe manifest
    --start-year YYYY          wipe and re-download years ≥ YYYY
    --end-year YYYY            wipe and re-download years ≤ YYYY (combine with --start-year for a range)

  Horizontal scope — additive:
    --transactions             transactions only (contributions + expenditures)
    --entities                 entities only (committees + candidates)
    --contributions            contributions only
    --expenditures             expenditures only
    --candidates               candidates only
    --committees               committees only

  Notes:
    - --force is mutually exclusive with --start-year / --end-year
    - --end-year cannot exceed the current calendar year
    - Horizontal flags are additive; stacking them unions their scopes
    - Scraper flags have no effect on reparse (scrape stage is skipped)
    - Not all states support all flags — unsupported flags are silently ignored

  Examples
  ─────────────────────────────────────────────────────────────────
  python3 src/main.py sync AL AK AZ
  python3 src/main.py sync AK --start-year 2023
  python3 src/main.py sync AK --force --transactions
  python3 src/main.py sync AL --start-year 2022 --end-year 2024 --contributions
  python3 src/main.py reparse AL
  python3 src/main.py push all
  python3 src/main.py pull db
  python3 src/main.py --daemon sync all
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

PIPELINE_COMMANDS = {"sync", "reparse"}
DATA_COMMANDS     = {"push", "pull"}
ALL_COMMANDS      = PIPELINE_COMMANDS | DATA_COMMANDS

# Flags forwarded to the scraper subprocess via orc
SCRAPER_FLAGS     = {"--force", "--transactions", "--entities",
                     "--contributions", "--expenditures",
                     "--candidates", "--committees"}
YEAR_FLAGS        = {"--start-year", "--end-year"}

# Set True to automatically generate an HTML report after every run.
AUTO_REPORT = True


def _print_help():
    print(__doc__)


def _parse_args(argv: list[str]) -> tuple[bool, bool, str, list[str], list[str]]:
    """
    Parse top-level CLI arguments.

    Returns:
        daemon      — True if --daemon was present
        no_report   — True if --no-report was present
        command     — the pipeline/data command (first non-flag arg)
        state_args  — remaining non-flag args (state abbreviations, 'all', 'db')
        extra_flags — scraper flags to forward via orc
    """
    daemon    = False
    no_report = False
    extra_flags: list[str] = []
    clean_args: list[str]  = []

    force      = False
    start_year = None
    end_year   = None

    i = 0
    while i < len(argv):
        a = argv[i]

        if a == "--daemon":
            daemon = True
            i += 1

        elif a == "--no-report":
            no_report = True
            i += 1

        elif a == "--force":
            force = True
            extra_flags.append(a)
            i += 1

        elif a in YEAR_FLAGS:
            if i + 1 >= len(argv) or argv[i + 1].startswith("--"):
                print(f"[!] {a} requires a YYYY value")
                sys.exit(1)
            yr_str = argv[i + 1]
            try:
                yr = int(yr_str)
            except ValueError:
                print(f"[!] {a} value must be a 4-digit year, got {yr_str!r}")
                sys.exit(1)
            if a == "--start-year":
                start_year = yr
            else:
                end_year = yr
            extra_flags.extend([a, yr_str])
            i += 2

        elif a in SCRAPER_FLAGS:
            extra_flags.append(a)
            i += 1

        else:
            clean_args.append(a)
            i += 1

    # ── Year flag validation ──────────────────────────────────────────
    cy = datetime.now().year
    if end_year is not None and end_year > cy:
        print(f"[!] --end-year cannot exceed current year ({cy})")
        sys.exit(1)
    if start_year is not None and end_year is not None and start_year > end_year:
        print(f"[!] --start-year cannot be greater than --end-year")
        sys.exit(1)
    if force and (start_year is not None or end_year is not None):
        print(f"[!] --force cannot be combined with --start-year or --end-year")
        sys.exit(1)

    command   = clean_args[0] if clean_args else ""
    state_args = clean_args[1:]

    return daemon, no_report, command, state_args, extra_flags


# ====================== Push / pull dispatch =========================

def _push(targets: list[str]):
    """Route a push command to the appropriate cloudflare helper."""
    if not targets:
        print("[!] push requires a target: <states>, all, or db")
        sys.exit(1)

    if len(targets) == 1 and targets[0].lower() == "db":
        db_path = PROJECT_ROOT / "data" / "state-level-cf.db"
        if not db_path.exists():
            print(f"[!] Aggregate db not found: {db_path}")
            sys.exit(1)
        cloudflare.push_file(db_path, "data/state-level-cf.db")

    elif len(targets) == 1 and targets[0].lower() == "all":
        cloudflare.push_all(PROJECT_ROOT)

    else:
        for abbr in targets:
            state_name = orc.ABBR_TO_NAME.get(abbr.upper())
            if not state_name:
                print(f"[!] Unknown state: {abbr}")
                sys.exit(1)
            cloudflare.push_state(state_name.capitalize(), PROJECT_ROOT)


def _pull(targets: list[str]):
    """Route a pull command to the appropriate cloudflare helper."""
    if not targets:
        print("[!] pull requires a target: <states>, all, or db")
        sys.exit(1)

    if len(targets) == 1 and targets[0].lower() == "db":
        db_path = PROJECT_ROOT / "data" / "state-level-cf.db"
        cloudflare.pull_file("data/state-level-cf.db", db_path)

    elif len(targets) == 1 and targets[0].lower() == "all":
        cloudflare.pull_all(PROJECT_ROOT)

    else:
        for abbr in targets:
            state_name = orc.ABBR_TO_NAME.get(abbr.upper())
            if not state_name:
                print(f"[!] Unknown state: {abbr}")
                sys.exit(1)
            cloudflare.pull_state(state_name.capitalize(), PROJECT_ROOT)


# ========================== Entry point ==============================

def main():
    """Parse top-level CLI args and dispatch to orc, push, or pull."""
    daemon, no_report, command, state_args, extra_flags = _parse_args(sys.argv[1:])

    if not command or command in ("-h", "--help"):
        _print_help()
        sys.exit(0)

    if command not in ALL_COMMANDS:
        print(f"[!] Unknown command: {command!r}")
        print(f"    Valid commands: {', '.join(sorted(ALL_COMMANDS))}")
        sys.exit(1)

    if not state_args:
        print(f"[!] {command} requires at least one target (state abbreviation, all, or db)")
        sys.exit(1)

    # Scraper flags are only meaningful for pipeline commands, not push/pull
    if command in DATA_COMMANDS and extra_flags:
        print(f"[!] Scraper flags {extra_flags} have no effect on {command!r}")
        sys.exit(1)

    # Daemon mode — silence console, subprocesses inherit this
    if daemon:
        os.environ["CF_DAEMON"] = "1"

    # Push/pull get named run IDs here; pipeline commands let orc._setup_run_id handle it
    if command in DATA_COMMANDS and not os.environ.get("CF_RUN_ID"):
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        tgt = (state_args[0].lower()
               if len(state_args) == 1 and state_args[0].lower() in ("all", "db")
               else "-".join(t.upper() for t in state_args))
        os.environ["CF_RUN_ID"] = f"{ts}_{command}_{tgt}"

    try:
        if command in PIPELINE_COMMANDS:
            orc.main(command, state_args, extra_flags=extra_flags)

        elif command == "push":
            _push(state_args)

        elif command == "pull":
            _pull(state_args)

    finally:
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
