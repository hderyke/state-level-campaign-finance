"""
src/main.py — Single entry point for the campaign finance pipeline.

  Pipeline commands          stages run
  ─────────────────────────────────────────────────────────────────
  sync <states>              scrape → parse → enrich → validate → tabulate → aggregate
  reparse <states>           parse → enrich → validate → tabulate → aggregate  (skip scrape)

  Data commands
  ─────────────────────────────────────────────────────────────────
  push <states|all|db>       upload to S3
  pull <states|all|db>       download from S3

  States:  two-letter abbreviations  AL AK AZ AR CA CO ...
           or 'all' to run every known state

  Global flags
  ─────────────────────────────────────────────────────────────────
  --daemon           silent mode for scheduled/cron runs
  --no-report        skip HTML report generation after run
  --fallback         on state failure, restore from S3 successful/ and
                     aggregate anyway (mix of fresh + fallback data)
  --keep-local       (pipeline commands only, needs ops/) don't wipe a
                     state's local data/ folder after syncing it back to
                     the external drive — default is to wipe, to keep
                     internal disk space free for aggregate's DuckDB spill

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
from pathlib import Path
from datetime import datetime

# Bootstrap: ensure project root is on sys.path before any src.*/cloud.* imports
_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# run_helpers bootstraps dotenv + sys.path at import time
from src.run_helpers import PROJECT_ROOT, generate_report, setup_run

from src import orc
from src.reporting.logger import get_logger

PIPELINE_COMMANDS = {"sync", "reparse"}


def _cloud_s3():
    """Lazy-load cloud.s3 — only needed for push/pull and --fallback.

    cloud/ holds personal AWS deployment code and is gitignored (not part of
    the public repo — see .gitignore and repo_reorg notes). Importing it
    eagerly at module load would break plain `sync`/`reparse` for anyone who
    clones this repo without their own cloud/ setup, since every command
    would fail at import time even ones that never touch S3.
    """
    try:
        from cloud import s3
        return s3
    except ImportError as e:
        print("[!] This command needs cloud/s3.py, which isn't included in this "
              "repo — it's personal AWS deployment code (bring your own bucket).")
        print(f"    ({e})")
        sys.exit(1)


def _ops_data_sync():
    """Lazy-load ops.data_sync — optional external-drive load/save wrapped
    around a pipeline run (pulls each state onto local disk before the run,
    pushes the updated folder back after a successful one).

    ops/ holds personal Mac-Mini scheduling/deployment scripts and is
    gitignored (not part of the public repo — same reasoning as cloud/, see
    _cloud_s3() above). Unlike cloud/s3, this integration is optional rather
    than required: data_sync's own load_states/save_states already no-op
    (with a warning) if CF_EXTERNAL_DATA_ROOT isn't set or the drive isn't
    mounted, so a clone without ops/ should behave the same way — the
    pipeline just runs against whatever's already in data/, silently
    skipping the external-drive sync step instead of failing.
    """
    try:
        from ops import data_sync
        return data_sync
    except ImportError:
        return None


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


def _parse_args(argv: list[str]) -> tuple[bool, bool, bool, bool, str, list[str], list[str]]:
    """
    Parse top-level CLI arguments.

    Returns:
        daemon      — True if --daemon was present
        no_report   — True if --no-report was present
        fallback    — True if --fallback was present
        keep_local  — True if --keep-local was present
        command     — the pipeline/data command (first non-flag arg)
        state_args  — remaining non-flag args (state abbreviations, 'all', 'db')
        extra_flags — scraper flags to forward via orc
    """
    daemon     = False
    no_report  = False
    fallback   = False
    keep_local = False
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

        elif a == "--fallback":
            fallback = True
            i += 1

        elif a == "--keep-local":
            keep_local = True
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

    return daemon, no_report, fallback, keep_local, command, state_args, extra_flags


# ====================== Push / pull dispatch =========================

def _push(targets: list[str]):
    """Route a push command to the appropriate S3 helper.

    `db` can be combined with state abbrs (or `all`) in one call, e.g.
    `push AL AR AZ db` — states/`all` are pushed first, `db` last, all within
    the same CF_RUN_ID. That's what lets daemon.py produce a single unified
    push report (states first, db at the bottom) instead of two separate
    runs/attachments. `db` alone or `all` alone still work as before.
    """
    s3 = _cloud_s3()
    if not targets:
        print("[!] push requires a target: <states>, all, or db")
        sys.exit(1)

    push_db_too = any(t.lower() == "db" for t in targets)
    other       = [t for t in targets if t.lower() != "db"]

    failed = []

    if other:
        if len(other) == 1 and other[0].lower() == "all":
            s3.push_all(PROJECT_ROOT)
        else:
            for i, abbr in enumerate(other):
                if i > 0:
                    print()  # separates this state's file listing from the
                              # previous state's "N ok, M error(s)" summary line
                state_name = orc.ABBR_TO_NAME.get(abbr.upper())
                if not state_name:
                    print(f"[!] Unknown state: {abbr}")
                    sys.exit(1)
                if not s3.push_state(abbr.upper(), state_name, PROJECT_ROOT):
                    failed.append(abbr.upper())

    if push_db_too:
        if not s3.push_db(PROJECT_ROOT):
            failed.append("DB")

    if failed:
        print(f"\n[!] Push had errors for: {', '.join(failed)}")
        sys.exit(1)


def _pull(targets: list[str]):
    """Route a pull command to the appropriate S3 helper."""
    s3 = _cloud_s3()
    if not targets:
        print("[!] pull requires a target: <states>, all, or db")
        sys.exit(1)

    if len(targets) == 1 and targets[0].lower() == "db":
        s3.pull_db(PROJECT_ROOT)

    elif len(targets) == 1 and targets[0].lower() == "all":
        s3.pull_all(PROJECT_ROOT)

    else:
        for abbr in targets:
            state_name = orc.ABBR_TO_NAME.get(abbr.upper())
            if not state_name:
                print(f"[!] Unknown state: {abbr}")
                sys.exit(1)
            s3.pull_state(abbr.upper(), state_name, PROJECT_ROOT)


# ====================== Fallback pipeline ============================

def _has_data(state_name: str) -> bool:
    """True if cleaned/ has at least one non-empty csv.gz."""
    cleaned = PROJECT_ROOT / "data" / state_name / "cleaned"
    if not cleaned.exists():
        return False
    return any(f.stat().st_size > 0 for f in cleaned.glob("*.csv.gz"))


def _sync_with_fallback(command: str, state_args: list[str],
                        extra_flags: list[str]) -> bool:
    """Run pipeline with fallback: failed states are restored from S3 successful/
    before aggregate runs. Returns True if aggregate succeeded."""
    import aggregate as _aggregate
    s3 = _cloud_s3()

    # Same CF_RUN_ID orc.main() just set — these events land in the same
    # run's log.jsonl, taggable by operation="fallback" for downstream readers
    # (e.g. src/emailer.py) that need to know which states were rolled back.
    log = get_logger(None, "fallback")

    results = orc.main(command, state_args, extra_flags=extra_flags,
                       no_aggregate=True)

    failed = [a for a, ok in results.items() if not ok]
    fresh  = [a for a, ok in results.items() if ok]

    # Restore failed states from S3
    fallback_ok, fallback_fail = [], []
    for abbr in failed:
        name = orc.ABBR_TO_NAME[abbr]
        print(f"\n  ↩  {abbr} failed — restoring from S3 successful/...")
        try:
            s3.pull_state(abbr, name)
            if _has_data(name):
                fallback_ok.append(abbr)
                print(f"     ✓ {abbr} restored")
                log._emit("fallback_restore", state_abbr=abbr, status="ok")
            else:
                fallback_fail.append(abbr)
                print(f"     ✗ {abbr} — nothing usable in S3, skipping")
                log._emit("fallback_restore", state_abbr=abbr, status="no_data")
        except Exception as e:
            fallback_fail.append(abbr)
            print(f"     ✗ {abbr} — S3 pull failed: {e}")
            log._emit("fallback_restore", state_abbr=abbr, status="error", error=str(e))

    if fallback_fail:
        print(f"\n  [!] {len(fallback_fail)} state(s) skipped entirely: "
              f"{', '.join(fallback_fail)}")

    log._emit("fallback_summary", failed=failed, fresh=fresh,
              fallback_ok=fallback_ok, fallback_fail=fallback_fail)

    runnable = fresh + fallback_ok
    if not runnable:
        print("\n[!] No states have usable data — aborting aggregate.")
        return False

    print(f"\n{'=' * 50}\n  Aggregate\n{'=' * 50}")
    try:
        _aggregate.run()
        return True
    except Exception as e:
        print(f"\n[!] Aggregate failed: {e}")
        return False


# ========================== Entry point ==============================

def main():
    """Parse top-level CLI args and dispatch to orc, push, or pull."""
    daemon, no_report, fallback, keep_local, command, state_args, extra_flags = _parse_args(sys.argv[1:])

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

    if command in DATA_COMMANDS and extra_flags:
        print(f"[!] Scraper flags {extra_flags} have no effect on {command!r}")
        sys.exit(1)

    if fallback and command not in PIPELINE_COMMANDS:
        print(f"[!] --fallback only applies to pipeline commands (sync, reparse)")
        sys.exit(1)

    if keep_local and command not in PIPELINE_COMMANDS:
        print(f"[!] --keep-local only applies to pipeline commands (sync, reparse)")
        sys.exit(1)

    # Daemon mode + run ID (push/pull only — pipeline lets orc handle its own run ID)
    if command in DATA_COMMANDS:
        setup_run(command, state_args, daemon=daemon)
    elif daemon:
        os.environ["CF_DAEMON"] = "1"

    try:
        if command in PIPELINE_COMMANDS:
            # Pull these states onto local disk from the external drive (if
            # ops/ is present and configured) before touching them, and push
            # the updated folders back once the run succeeds. See
            # _ops_data_sync() / ops/data_sync.py for why this is optional.
            data_sync    = _ops_data_sync()
            sync_targets = ([abbr for abbr, _ in orc.resolve_states(state_args)]
                            if data_sync else None)
            if data_sync:
                data_sync.load_states(sync_targets)

            agg_ok = False
            try:
                if fallback:
                    agg_ok = _sync_with_fallback(command, state_args, extra_flags)
                    if not agg_ok:
                        sys.exit(1)
                else:
                    orc.main(command, state_args, extra_flags=extra_flags)
                    agg_ok = True  # orc.main() only returns normally when every state passed
            finally:
                if agg_ok and data_sync:
                    data_sync.save_states(sync_targets, wipe_local=not keep_local)

        elif command == "push":
            _push(state_args)

        elif command == "pull":
            _pull(state_args)

    finally:
        if AUTO_REPORT:
            generate_report(os.environ.get("CF_RUN_ID", ""), no_report=no_report)


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
