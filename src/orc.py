"""
src/orc.py — High-level pipeline orchestration.

Usage:
    python3 src/orc.py rescrape             AK AL AZ
    python3 src/orc.py update               AK AL
    python3 src/orc.py update-entities      AK AL
    python3 src/orc.py update-transactions  AK AL
"""

import os
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "pipeline"))
from src.logger import get_logger
import aggregate as _aggregate

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ── State registry ─────────────────────────────────────────────────────────────
ABBR_TO_NAME = {
    "AL": "alabama",  "AK": "alaska",   "AZ": "arizona",  "AR": "arkansas",
    "CA": "california", "CO": "colorado",
    # extend as you add states
}

SCRAPER_DIR = PROJECT_ROOT / "src" / "pipeline" / "scrapers"
PARSER_DIR  = PROJECT_ROOT / "src" / "pipeline" / "parsers"
TABULATE    = PROJECT_ROOT / "src" / "pipeline" / "tabulate.py"
VALIDATE    = PROJECT_ROOT / "tests" / "validate.py"

PYTHON = sys.executable

# ── Command → scraper mode ─────────────────────────────────────────────────────
COMMAND_TO_MODE = {
    "rescrape":             "force",
    "update":               "update",
    "update-entities":      "update-entities",
    "update-transactions":  "update-transactions",
}


# ── Helpers ────────────────────────────────────────────────────────────────────
def _setup_run_id() -> str:
    """Generate a unique run ID, set CF_RUN_ID in env so subprocesses share it."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    os.environ["CF_RUN_ID"] = run_id
    return run_id


def header(msg: str):
    bar = "─" * 60
    print(f"\n{bar}\n  {msg}\n{bar}")


def _subprocess(cmd: list[str], label: str) -> bool:
    """Run a subprocess, stream output, return True on success."""
    print(f"\n  ▶ {label}")
    print(f"    {' '.join(str(c) for c in cmd)}\n")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"\n  ✗ FAILED (exit {result.returncode}): {label}")
    else:
        print(f"\n  ✓ OK: {label}")
    return result.returncode == 0


def scraper_path(state: str) -> Path | None:
    p = SCRAPER_DIR / f"{state}.py"
    return p if p.exists() else None


def parser_path(state: str) -> Path | None:
    p = PARSER_DIR / f"{state}.py"
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


def _scraper_flags(state: str, mode: str) -> list[str]:
    if state == "colorado":
        return {
            "force":               ["--force"],
            "update":              ["--update"],
            "update-entities":     ["--update"],
            "update-transactions": ["--update"],
        }[mode]
    if state in ("alabama", "alaska"):
        return {
            "force":               ["--force"],
            "update":              [],
            "update-entities":     ["--entities"],
            "update-transactions": ["--transactions"],
        }[mode]
    return {
        "force":               ["--force"],
        "update":              [],
        "update-entities":     ["--update-entities"],
        "update-transactions": ["--update-transactions"],
    }[mode]


def _run_state(abbr: str, name: str, scraper_mode: str,
               log, tabulate_on_pass: bool = True) -> bool:
    """
    Full pipeline for one state:
      scraper [flags] → parser → validate → tabulate (if pass)
    Returns True if the whole chain succeeded.
    """
    header(f"{abbr.upper()} — {scraper_mode}")

    # 1. Scraper
    sp = scraper_path(name)
    if sp is None:
        print(f"  [!] No scraper found for {name} — skipping scrape step")
    else:
        flags = _scraper_flags(name, scraper_mode)
        ok = _subprocess([PYTHON, str(sp)] + flags,
                         f"scraper/{name}.py {' '.join(flags)}")
        if not ok:
            print(f"  [!] Scraper failed for {abbr} — aborting this state")
            log._emit("state_completed", state=name, status="failed", stage="scrape")
            return False

    # 2. Parser
    pp = parser_path(name)
    if pp is None:
        print(f"  [!] No parser found for {name} — skipping")
        log._emit("state_completed", state=name, status="failed", stage="parse",
                  reason="no parser found")
        return False
    ok = _subprocess([PYTHON, str(pp)], f"parser/{name}.py")
    if not ok:
        print(f"  [!] Parser failed for {abbr} — aborting this state")
        log._emit("state_completed", state=name, status="failed", stage="parse")
        return False

    # 3. Validate
    ok = _subprocess([PYTHON, str(VALIDATE), name], f"validate.py {name}")
    if not ok:
        print(f"  [!] Validation FAILED for {abbr} — NOT tabulating")
        log._emit("state_completed", state=name, status="failed", stage="validate")
        return False

    # 4. Tabulate
    if tabulate_on_pass:
        ok = _subprocess([PYTHON, str(TABULATE), name], f"tabulate.py {name}")
        if not ok:
            print(f"  [!] Tabulate failed for {abbr}")
            log._emit("state_completed", state=name, status="failed", stage="tabulate")
            return False

    log._emit("state_completed", state=name, status="passed")
    return True


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


# ── Main entrypoint ────────────────────────────────────────────────────────────
def main(command: str, state_abbrs: list[str]):
    run_id = _setup_run_id()
    log    = get_logger(None, "orc")
    t0     = time.perf_counter()

    states = resolve_states(state_abbrs)
    log._emit("run_started", run_id=run_id, command=command,
              states=[a for a, _ in states])

    try:
        scraper_mode = COMMAND_TO_MODE[command]
        results      = {}

        for abbr, name in states:
            log._emit("state_started", state=name)
            st = time.perf_counter()
            ok = _run_state(abbr, name, scraper_mode, log, tabulate_on_pass=True)
            results[abbr] = ok
            # state_completed is emitted inside _run_state with stage context;
            # emit duration here as a separate event so it's always recorded
            log._emit("state_duration", state=name,
                      duration_s=round(time.perf_counter() - st, 1),
                      status="passed" if ok else "failed")

        _summary(results)
        failed = [k for k, v in results.items() if not v]

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


# ── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    COMMANDS = list(COMMAND_TO_MODE.keys())

    if len(sys.argv) < 3 or sys.argv[1] not in COMMANDS:
        print("Usage: python3 src/orc.py <command> <STATE> [STATE ...]")
        print(f"Commands: {', '.join(COMMANDS)}")
        print("Example:  python3 src/orc.py update AK AL AZ")
        sys.exit(1)

    try:
        main(sys.argv[1], sys.argv[2:])
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        sys.exit(1)
