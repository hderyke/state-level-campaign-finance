"""
src/main.py — Single entry point for the campaign finance pipeline.

  Pipeline commands          stages run
  ─────────────────────────────────────────────────────────────────
  sync <states>              scrape → parse → enrich → validate → tabulate → aggregate
  reparse <states>           parse → enrich → validate → tabulate → aggregate  (skip scrape)

  States:  two-letter abbreviations  AL AK AZ AR CA CO ...
           or 'all' to run every known state

  R2 push/pull no longer runs through this CLI -- see ops/daemon.py (scheduled
  runs) or cloud/r2/r2.py's own __main__ (manual, standalone).

  Global flags
  ─────────────────────────────────────────────────────────────────
  --daemon           silent mode for scheduled/cron runs
  --no-report        skip HTML report generation after run
  --fallback         on state failure, restore from R2 successful/ and
                     aggregate anyway (mix of fresh + fallback data)
  --no-aggregate     skip the aggregate step. Without --fallback, runs just
                     the per-state stages. With --fallback, still restores
                     failed states from R2 but skips the final aggregate.run()
                     call -- see ops/daemon.py, which uses this combination
                     while aggregation is deliberately paused project-wide.

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
  python3 src/main.py sync AL --no-aggregate
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

# 2026-09-19: cloud/ moved under serving/ (serving/cloud/) so it can live in
# its own git repo alongside serving/web/ -- src/ stays put, so the root
# insert above still covers `from src... import`, but `_cloud_backend()`'s
# lazy `from cloud.r2 import r2` needs `serving/` on the path too now.
_serving_root = _root / "serving"
if str(_serving_root) not in sys.path:
    sys.path.insert(0, str(_serving_root))

# run_helpers bootstraps dotenv + sys.path at import time
from src.run_helpers import PROJECT_ROOT, generate_report

from src import orc
from src.reporting.logger import get_logger

# Single source of truth is orc.py -- it's runnable standalone and validates
# its own CLI against this same set independent of main.py, so main.py reads
# orc's copy rather than maintaining a second one that could drift.
PIPELINE_COMMANDS = orc.PIPELINE_COMMANDS


# Flags forwarded to the scraper subprocess via orc. Not every state's
# scraper accepts every flag here -- each scraper's own argparse just
# ignores (via parse_known_args) whatever it doesn't define, per the
# module docstring's "unsupported flags are silently ignored" note. So
# it's safe -- and expected -- to add a state-specific boolean flag here
# once that state's scraper defines it, rather than routing it around
# main.py's parser.
#
# --pacs/--party-caucus/--ballot-measure are South Carolina's opt-in
# non-candidate-committee sources (see docs/states/south_carolina.md).
# Before these were added here, main.py's parser didn't recognize them at
# all: they fell through to `clean_args` and were treated as state
# abbreviations, so `sync SC --pacs` failed with "Unknown state
# abbreviation: --PACS" instead of reaching the scraper.
SCRAPER_FLAGS     = {"--force", "--transactions", "--entities",
                     "--contributions", "--expenditures",
                     "--candidates", "--committees",
                     "--pacs", "--party-caucus", "--ballot-measure"}
YEAR_FLAGS        = {"--start-year", "--end-year"}


def _parse_args(argv: list[str]) -> tuple[bool, bool, bool, bool, str, list[str], list[str]]:
    """
    Parse top-level CLI arguments.

    Returns:
        daemon          — True if --daemon was present
        no_report       — True if --no-report was present
        fallback        — True if --fallback was present
        no_aggregate    — True if --no-aggregate was present
        command         — the pipeline command (first non-flag arg)
        state_args      — remaining non-flag args (state abbreviations, 'all')
        extra_flags     — scraper flags to forward via orc
    """
    daemon          = False
    no_report       = False
    fallback        = False
    no_aggregate    = False
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

        elif a == "--no-aggregate":
            no_aggregate = True
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

    return daemon, no_report, fallback, no_aggregate, command, state_args, extra_flags


def _print_help():
    print(__doc__)


# ====================== Fallback pipeline ============================

def _cloud_backend():
    """Lazy-load cloud.r2.r2 — only needed for --fallback (restoring failed
    states from R2). Push/pull themselves no longer run through main.py —
    see ops/daemon.py (scheduled runs) and cloud/r2/r2.py's own __main__
    (manual, standalone).

    cloud/ holds personal deployment code and is gitignored (not part of the
    public repo — see .gitignore and repo_reorg notes). Importing it eagerly
    at module load would break plain `sync`/`reparse` for anyone who clones
    this repo without their own cloud/ setup, since every command would fail
    at import time even ones that never touch R2.

    Was cloud.s3 (hence the old name _cloud_s3) until 2026-08-23, when
    cloud/s3.py was deleted (S3/AWS retired in favor of R2) and cloud/r2/r2.py's
    push_state/push_all/push_db/pull_state/pull_all/pull_db took over as the
    live push/pull backend — r2.py was deliberately built with full function
    parity with s3.py, so this is a straight swap, not a behavior change to
    anything downstream of it (both modules expose the same six functions
    with the same signatures).

    2026-09-19: this docstring used to point at cloud/dispatch.py's
    PushPullBackend Protocol as where that shared shape was pinned down.
    dispatch.py has since been retired (push/pull no longer route through
    main.py at all -- see this function's own note above -- and each
    backend's own __main__ now does its own routing), so the shape above is
    just stated directly rather than pointing at a Protocol that no longer
    exists.
    """
    try:
        from cloud.r2 import r2
        return r2
    except ImportError as e:
        print("[!] This command needs cloud/r2/r2.py, which isn't included in this "
              "repo — it's personal Cloudflare deployment code (bring your own bucket).")
        print(f"    ({e})")
        sys.exit(1)


def _has_data(name: str) -> bool:
    """True if cleaned/ has at least one non-empty csv.gz."""
    cleaned = PROJECT_ROOT / "data" / name / "cleaned"
    if not cleaned.exists():
        return False
    return any(f.stat().st_size > 0 for f in cleaned.glob("*.csv.gz"))


def _run_with_fallback(command: str, state_args: list[str],
                        extra_flags: list[str], skip_aggregate: bool = False) -> bool:
    """Run pipeline with fallback: failed states are restored from R2 successful/
    before aggregate runs. Returns True if aggregate succeeded, or (when
    skip_aggregate=True) if at least one state produced usable data --
    aggregation is currently paused project-wide (see ops/daemon.py), so
    "success" here means the per-state pipelines are healthy, not that the
    combined master db got rebuilt."""
    backend = _cloud_backend()
    if not skip_aggregate:
        import aggregate as _aggregate

    # Same CF_RUN_ID orc.main() just set — these events land in the same
    # run's log.jsonl, taggable by operation="fallback" for downstream readers
    # (e.g. ops/emailer.py) that need to know which states were rolled back.
    log = get_logger(None, "fallback")

    results = orc.main(command, state_args, extra_flags=extra_flags,
                       no_aggregate=True)

    failed = [a for a, ok in results.items() if not ok]
    fresh  = [a for a, ok in results.items() if ok]

    # Restore failed states from R2
    fallback_ok, fallback_fail = [], []
    for abbr in failed:
        name = orc.ABBR_TO_NAME[abbr]
        print(f"\n  ↩  {abbr} failed — restoring from R2 successful/...")
        try:
            backend.pull_state(abbr, name, PROJECT_ROOT)
            if _has_data(name):
                fallback_ok.append(abbr)
                print(f"     ✓ {abbr} restored")
                log._emit("fallback_restore", state_abbr=abbr, status="ok")
            else:
                fallback_fail.append(abbr)
                print(f"     ✗ {abbr} — nothing usable in R2, skipping")
                log._emit("fallback_restore", state_abbr=abbr, status="no_data")
        except Exception as e:
            fallback_fail.append(abbr)
            print(f"     ✗ {abbr} — R2 pull failed: {e}")
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

    if skip_aggregate:
        print("\n  [--no-aggregate] Skipping aggregate step (per-state data is still pushed).")
        return True

    print(f"\n{'=' * 50}\n  Aggregate\n{'=' * 50}")
    try:
        _aggregate.run()
        return True
    except Exception as e:
        print(f"\n[!] Aggregate failed: {e}")
        return False


# ========================== Entry point ==============================

# Set True to automatically generate an HTML report after every run.
AUTO_REPORT = True


def main():
    """Parse top-level CLI args and dispatch to orc."""
    daemon, no_report, fallback, no_aggregate, command, state_args, extra_flags = \
        _parse_args(sys.argv[1:])

    if not command or command in ("-h", "--help"):
        _print_help()
        sys.exit(0)

    if command not in PIPELINE_COMMANDS:
        print(f"[!] Unknown command: {command!r}")
        print(f"    Valid commands: {', '.join(sorted(PIPELINE_COMMANDS))}")
        sys.exit(1)

    if not state_args:
        print(f"[!] {command} requires at least one target (state abbreviation or all)")
        sys.exit(1)

    if daemon:
        os.environ["CF_DAEMON"] = "1"

    try:
        if fallback:
            ok = _run_with_fallback(command, state_args, extra_flags,
                                     skip_aggregate=no_aggregate)
            if not ok:
                sys.exit(1)
        else:
            orc.main(command, state_args, extra_flags=extra_flags,
                     no_aggregate=no_aggregate)

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
