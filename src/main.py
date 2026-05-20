"""
src/main.py — Single entry point for the campaign finance pipeline.

  Pipeline commands          stages run
  ─────────────────────────────────────────────────────────────────
  update <states>            scrape → parse → validate → tabulate → aggregate
  rescrape <states>          scrape --force → parse → validate → tabulate → aggregate
  update-entities <states>   scrape --update-entities → parse → validate → tabulate → aggregate
  update-transactions <st.>  scrape --update-transactions → parse → validate → tabulate → aggregate

  Data commands
  ─────────────────────────────────────────────────────────────────
  push <states|all|db>       upload to Cloudflare R2
  pull <states|all|db>       download from Cloudflare R2

  States:  two-letter abbreviations  AL AK AZ AR CA CO ...
           or 'all' to run every known state

  Flags:   --daemon           silent mode for scheduled/cron runs

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
from pathlib import Path

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

PIPELINE_COMMANDS = {"update", "rescrape", "update-entities", "update-transactions"}
DATA_COMMANDS     = {"push", "pull"}
ALL_COMMANDS      = PIPELINE_COMMANDS | DATA_COMMANDS


def _print_help():
    print(__doc__)


# ── Push / pull ────────────────────────────────────────────────────────────────
def _push(targets: list[str]):
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
            # Capitalize to match data/ directory name (e.g. Alabama)
            cloudflare.push_state(state_name.capitalize(), PROJECT_ROOT)


def _pull(targets: list[str]):
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


# ── Entrypoint ─────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]

    # Strip --daemon flag wherever it appears
    daemon = "--daemon" in args
    args   = [a for a in args if a != "--daemon"]

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

    # All commands get a run ID so logs always land in logs/runs/
    if not os.environ.get("CF_RUN_ID"):
        import uuid
        from datetime import datetime
        os.environ["CF_RUN_ID"] = (
            datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        )

    if command in PIPELINE_COMMANDS:
        orc.main(command, targets)

    elif command == "push":
        _push(targets)

    elif command == "pull":
        _pull(targets)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
