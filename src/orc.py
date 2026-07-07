"""
src/orc.py — High-level pipeline orchestration.

Usage:
    python3 src/orc.py sync    AK AL AZ
    python3 src/orc.py sync    AK --force
    python3 src/orc.py sync    AK --start-year 2023
    python3 src/orc.py sync    AK --force --transactions
    python3 src/orc.py reparse AK AL
"""

import csv
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "pipeline"))
from src.reporting.logger import get_logger
import aggregate as _aggregate

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# =========================== Configuration ===========================

# Loaded from src/aliases/states.csv — add a row there to register a new state.
_STATES_CSV  = PROJECT_ROOT / "src" / "aliases" / "states.csv"
with open(_STATES_CSV, encoding="utf-8") as _f:
    ABBR_TO_NAME: dict[str, str] = {
        row["abbr"]: row["name"].lower()
        for row in csv.DictReader(_f)
    }



SCRAPER_DIR = PROJECT_ROOT / "src" / "pipeline" / "scrapers"
PARSER_DIR  = PROJECT_ROOT / "src" / "pipeline" / "parsers"
TABULATE    = PROJECT_ROOT / "src" / "pipeline" / "tabulate.py"
VALIDATE    = PROJECT_ROOT / "src" / "pipeline" / "validate.py"

PYTHON = sys.executable

PIPELINE_COMMANDS = {"sync", "reparse"}


# === Helpers ===========================================
def _setup_run_id(command: str, state_abbrs: list[str],
                  extra_flags: list[str] | None = None) -> str:
    """Build a human-readable run ID and set CF_RUN_ID for all subprocesses."""
    ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    states = ("all" if (len(state_abbrs) == 1 and state_abbrs[0].lower() == "all")
              else "-".join(a.upper() for a in state_abbrs))
    # Append --force to the run ID so logs are self-describing
    suffix = "_force" if extra_flags and "--force" in extra_flags else ""
    run_id = f"{ts}_{command}{suffix}_{states}"
    os.environ["CF_RUN_ID"] = run_id
    return run_id


def header(msg: str):
    bar = "─" * 60
    print(f"\n{bar}\n  {msg}\n{bar}")


def _summary(results: dict[str, bool]):
    bar = "=" * 60
    print(f"\n{bar}\n  SUMMARY\n{bar}")
    for abbr, ok in results.items():
        mark = "✓" if ok else "✗"
        print(f"  {mark}  {abbr}")
    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n  {len(failed)} state(s) failed: {', '.join(failed)}")
    else:
        print(f"\n  All {len(results)} state(s) succeeded")


def _subprocess(cmd: list[str], label: str, log=None) -> bool:
    """Run a subprocess, stream stdout, capture stderr, return True on success.

    On failure: stderr (Python tracebacks, etc.) is printed to the terminal
    and emitted to the run JSONL as a subprocess_error event so post-mortem
    log inspection shows the actual exception, not just a non-zero exit code.
    """
    print(f"\n  ▶ {label}")
    print(f"    {' '.join(str(c) for c in cmd)}\n")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"\n  ✗ FAILED (exit {result.returncode}): {label}")
        if result.stderr:
            stderr_text = result.stderr.decode("utf-8", errors="replace").strip()
            if stderr_text:
                print(stderr_text)
                if log:
                    log._emit("subprocess_error", label=label,
                              returncode=result.returncode, stderr=stderr_text)
    else:
        print(f"\n  ✓ OK: {label}")
    return result.returncode == 0


QUERIES     = PROJECT_ROOT / "src" / "pipeline" / "queries.py"
QUERIES_DIR = PROJECT_ROOT / "metadata"


def _run_queries(name: str, log) -> None:
    """Capture queries.py output and save to run dir + metadata/.
    Never blocks the pipeline — errors are printed and logged but ignored."""
    if not QUERIES.exists():
        return

    print(f"\n  ▶ queries/{name}.py")
    t0 = time.perf_counter()
    try:
        result = subprocess.run(
            [PYTHON, str(QUERIES), name],
            capture_output=True, text=True, cwd=PROJECT_ROOT,
        )
        output   = result.stdout
        if result.stderr:
            output += "\n" + result.stderr
        duration = round(time.perf_counter() - t0, 1)

        # Always write to metadata/{state}_queries.txt (latest)
        QUERIES_DIR.mkdir(parents=True, exist_ok=True)
        (QUERIES_DIR / f"{name}_queries.txt").write_text(output, encoding="utf-8")

        # Copy to run dir if we're in orc mode
        run_id = os.environ.get("CF_RUN_ID")
        if run_id:
            run_dir = PROJECT_ROOT / "logs" / "prod" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / f"{name}_queries.txt").write_text(output, encoding="utf-8")

        status = "completed" if result.returncode == 0 else "error"
        print(f"\n  {'✓' if status == 'completed' else '!'} queries: {duration}s")
        log._emit("queries_completed", state=name, status=status, duration_s=duration)

    except Exception as e:
        print(f"\n  [!] test_queries failed for {name}: {e}")
        log._emit("queries_completed", state=name, status="error",
                  duration_s=round(time.perf_counter() - t0, 1), error=str(e))


def _state_slug(state: str) -> str:
    """Convert a state name to a valid Python module filename slug.
    e.g. "New York" → "new_york", "Michigan" → "michigan"
    """
    return state.lower().replace(" ", "_")


def scraper_path(state: str) -> Path | None:
    p = SCRAPER_DIR / f"{_state_slug(state)}.py"
    return p if p.exists() else None


def parser_path(state: str) -> Path | None:
    p = PARSER_DIR / f"{_state_slug(state)}.py"
    return p if p.exists() else None


def resolve_states(abbr_list: list[str]) -> list[tuple[str, str]]:
    """Returns [(abbr, name), ...]. 'all' expands to every known state."""
    if len(abbr_list) == 1 and abbr_list[0].lower() == "all":
        return [(abbr, name) for abbr, name in sorted(ABBR_TO_NAME.items())]
    out = []
    for abbr in abbr_list:
        abbr = abbr.upper()
        name = ABBR_TO_NAME.get(abbr)
        if not name:
            print(f"[!] Unknown state abbreviation: {abbr}")
            sys.exit(1)
        out.append((abbr, name))
    return out





# ====== Orchestration ========================
def _run_state(abbr: str, name: str, command: str,
               log, extra_flags: list[str] | None = None,
               tabulate_on_pass: bool = True) -> bool:
    """
    Full pipeline for one state:
      scraper [extra_flags] → parser → validate → tabulate (if pass)
    Returns True if the whole chain succeeded.
    """
    extra_flags = extra_flags or []
    header(f"{abbr.upper()} — {command}{' ' + ' '.join(extra_flags) if extra_flags else ''}")

    # Scraper
    if command == "reparse":
        print(f"  ↷ Skipping scrape (reparse mode)")
    else:
        sp = scraper_path(name)
        if sp is None:
            print(f"  [!] No scraper found for {name} — skipping scrape step")
        else:
            ok = _subprocess([PYTHON, str(sp)] + extra_flags,
                             f"scraper/{name}.py {' '.join(extra_flags)}", log=log)
            if not ok:
                print(f"  [!] Scraper failed for {abbr} — aborting this state")
                log._emit("state_completed", state=name, status="failed", stage="scrape")
                return False

    # Parser
    pp = parser_path(name)
    if pp is None:
        print(f"  [!] No parser found for {name} — skipping")
        log._emit("state_completed", state=name, status="failed", stage="parse",
                  reason="no parser found")
        return False

    ok = _subprocess([PYTHON, str(pp)], f"parser/{name}.py", log=log)
    if not ok:
        print(f"  [!] Parser failed for {abbr} — aborting this state")
        log._emit("state_completed", state=name, status="failed", stage="parse")
        return False

    # Validate
    ok = _subprocess([PYTHON, str(VALIDATE), name], f"validate.py {name}", log=log)
    if not ok:
        print(f"  [!] Validation FAILED for {abbr} — NOT tabulating")
        log._emit("state_completed", state=name, status="failed", stage="validate")
        # Still run queries — informational even on failure
        _run_queries(name, log)
        return False

    # Tabulate
    if tabulate_on_pass:
        ok = _subprocess([PYTHON, str(TABULATE), name], f"tabulate.py {name}", log=log)
        if not ok:
            print(f"  [!] Tabulate failed for {abbr}")
            log._emit("state_completed", state=name, status="failed", stage="tabulate")
            return False

    # Spot-check queries — runs after tabulate so the DB reflects this run's data
    _run_queries(name, log)

    log._emit("state_completed", state=name, status="passed")
    return True



def main(command: str, state_abbrs: list[str],
         extra_flags: list[str] | None = None,
         no_aggregate: bool = False):
    """Run the full pipeline (scrape → parse → validate → tabulate → aggregate) for the given states.

    Args:
        no_aggregate: If True, skip the aggregate step entirely and return the
                      results dict so the caller (e.g. a daemon) can handle
                      fallback and aggregation itself.
    """
    extra_flags = extra_flags or []
    run_id = _setup_run_id(command, state_abbrs, extra_flags)
    log    = get_logger(None, "orc")
    t0     = time.perf_counter()

    states = resolve_states(state_abbrs)
    log._emit("run_started", run_id=run_id, command=command,
              extra_flags=extra_flags, states=[a for a, _ in states])

    try:
        results = {}

        for abbr, name in states:
            log._emit("state_started", state=name)
            st = time.perf_counter()
            ok = _run_state(abbr, name, command, log,
                            extra_flags=extra_flags, tabulate_on_pass=True)
            results[abbr] = ok
            log._emit("state_duration", state=name,
                      duration_s=round(time.perf_counter() - st, 1),
                      status="passed" if ok else "failed")

        _summary(results)
        failed = [k for k, v in results.items() if not v]

        if no_aggregate:
            duration = round(time.perf_counter() - t0, 1)
            log.info(f"Done in {duration}s (aggregate deferred to caller)")
            log._emit("run_completed", run_id=run_id, status="completed",
                      duration_s=duration, passed=len(results) - len(failed),
                      failed=len(failed), aggregate="deferred")
            return results

        # Auto-aggregate if all states passed
        if not failed:
            header("Aggregate")
            _aggregate.run()
        else:
            print(f"\n  Skipping aggregate — {len(failed)} state(s) failed")

        duration = round(time.perf_counter() - t0, 1)
        log.info(f"Done in {duration}s")
        log._emit("run_completed", run_id=run_id, status="completed",
                  duration_s=duration, passed=len(results) - len(failed),
                  failed=len(failed))

        if failed:
            sys.exit(1)

    except KeyboardInterrupt:
        log._emit("run_completed", run_id=run_id, status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1))
        raise
    except Exception as e:
        log._emit("run_completed", run_id=run_id, status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  error_type=type(e).__name__, error=str(e))
        raise


# ======= CLI =========================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Run the campaign finance pipeline for one or more states.",
        epilog="Example:  python3 src/orc.py sync AK AL --start-year 2023",
    )
    ap.add_argument("command", choices=sorted(PIPELINE_COMMANDS),
                    help="Pipeline command to run")
    ap.add_argument("states", nargs="+", metavar="STATE",
                    help="State abbreviations (e.g. AK AL) or 'all'")
    args, extra = ap.parse_known_args()
    try:
        main(args.command, args.states, extra_flags=extra)
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
