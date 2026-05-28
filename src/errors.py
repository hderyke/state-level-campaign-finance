"""
src/errors.py — Pipeline-level error handling for orc/daemon runs.

In orc/daemon mode (CF_RUN_ID set): exceptions are caught, logged to the state's
JSONL, and the pipeline continues to the next state.

In dev mode (no CF_RUN_ID): this module is a no-op — errors propagate naturally
so you get full stack traces in the terminal.

Usage in orc.py:
    from src.errors import pipeline_stage

    with pipeline_stage(log, component="scraper"):
        alabama.run()
"""

import os
import traceback
from contextlib import contextmanager

from src.reporting.logger import StateLogger


@contextmanager
def pipeline_stage(log: StateLogger, component: str):
    """
    Wraps a single pipeline stage (scraper, parser, validate, tabulate, aggregate).

    Orc/daemon mode  — catches all exceptions, records them to the state's JSONL
                       with full traceback, logs an ERROR line to console, and
                       returns control to orc so the next state can run.

    Dev mode         — complete no-op. Exceptions propagate to the terminal as
                       normal so you get the full stack trace.
    """
    if not os.environ.get("CF_RUN_ID"):  # set by orc.py when running a managed pipeline job
        yield   # dev mode: don't interfere
        return

    try:
        yield
    except KeyboardInterrupt:
        log.warning(f"  ✗ {component} interrupted by user")
        log._emit("interrupted", component=component)
        raise  # always propagate — Ctrl+C stops the whole run
    except Exception as e:
        log.error(f"  ✗ {component} failed: {type(e).__name__}: {e}")
        log._emit(
            "pipeline_error",
            component=component,
            error_type=type(e).__name__,
            message=str(e),
            traceback=traceback.format_exc(),
        )
        # Swallow the exception — orc continues to the next state/stage
