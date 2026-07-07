"""
src/daemon.py — scheduling wrapper for the pipeline.

Runs a batch of states through the full pipeline (with fallback), then pushes
and restarts ECS. Intended to be called from cron or launchd on the Mac Mini.

Usage:
    python3 src/daemon.py AL AR AZ CA CO
    python3 src/daemon.py --reparse AL AR

Exit codes:
    0  — sync + aggregate + push all succeeded
    1  — aggregate failed (states were still pushed; DB push and ECS restart skipped)
    2  — no usable data at all (nothing pushed)
"""

import subprocess
import sys
import time
from pathlib import Path

# Bootstrap: make project root importable before anything else
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.run_helpers import PROJECT_ROOT
from src import emailer

CLUSTER = "state-level-cf"
SERVICE = "campaign-finance-api"
REGION  = "us-east-2"
PYTHON  = sys.executable


def _run(*cmd: str) -> int:
    return subprocess.run(list(cmd), cwd=PROJECT_ROOT).returncode


def run(state_abbrs: list[str], command: str = "sync") -> None:
    main = str(PROJECT_ROOT / "src" / "main.py")
    t_daemon = time.perf_counter()

    agg_ok           = False
    sync_run_dir      = None
    push_run_dir      = None
    push_db_run_dir   = None

    try:
        # Step 1: pipeline with fallback + aggregate
        t_step = time.time()
        rc = _run(PYTHON, main, "--daemon", command, *state_abbrs, "--fallback")
        agg_ok = rc == 0
        sync_run_dir = emailer.find_run_dir(command, state_abbrs, after=t_step)

        # Step 2: push states regardless of aggregate outcome
        t_step = time.time()
        rc = _run(PYTHON, main, "push", *state_abbrs)
        if rc != 0:
            print(f"\n[!] State push had errors — check logs.")
        push_run_dir = emailer.find_run_dir("push", state_abbrs, after=t_step)

        # Step 3: push DB + restart ECS only if aggregate passed
        if agg_ok:
            t_step = time.time()
            _run(PYTHON, main, "push", "db")
            push_db_run_dir = emailer.find_run_dir("push", ["db"], after=t_step)
            subprocess.run([
                "aws", "ecs", "update-service",
                "--cluster", CLUSTER, "--service", SERVICE,
                "--force-new-deployment", "--region", REGION, "--no-cli-pager",
            ], check=False)
            print("\n  ✓ ECS restart triggered")
        else:
            print("\n  [!] Aggregate failed — DB push skipped, ECS unchanged.")
    finally:
        # Always attempt the summary email — a partial/failed run is exactly
        # when you most want to hear about it.
        emailer.send_run_summary(
            command=command, state_abbrs=state_abbrs,
            sync_run_dir=sync_run_dir, push_run_dir=push_run_dir,
            push_db_run_dir=push_db_run_dir, agg_ok=agg_ok,
            daemon_duration_s=round(time.perf_counter() - t_daemon, 1),
        )

    if not agg_ok:
        sys.exit(1)


if __name__ == "__main__":
    args    = sys.argv[1:]
    command = "sync"

    if "--reparse" in args:
        command = "reparse"
        args = [a for a in args if a != "--reparse"]

    if not args:
        print("Usage: python3 src/daemon.py [--reparse] AL AR AZ ...")
        sys.exit(1)

    try:
        run(args, command)
    except KeyboardInterrupt:
        sys.exit(130)
