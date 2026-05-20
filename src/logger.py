"""
src/logger.py — Shared logger for all pipeline components.

Three modes, detected automatically from environment variables:

  Dev mode    — no CF_RUN_ID. Console at DEBUG, JSONL to:
                  logs/dev/{ts}-{state}-{operation}.jsonl
                  logs/dev/{ts}-{operation}.jsonl          (state-less, e.g. aggregate)

  Orc mode    — CF_RUN_ID set. Console at INFO, JSONL to:
                  logs/runs/{run_id}.jsonl                 (one file for the entire run)

  Daemon mode — CF_RUN_ID + CF_DAEMON set. Silent console, same JSONL as orc.

Usage:
    from src.logger import get_logger

    log = get_logger("alabama", "scrape")   # state run
    log = get_logger(None, "aggregate")     # state-less run

    log.info("Starting run")
    log.file_download_ok(filename="foo.csv", bytes=12345, rows=4321, duration_s=1.2)
    log.page_scrape_error(entity="pac", page_id=99, error="timeout")
    log.run_summary(duration_s=180.4, files_ok=55, files_err=1)

Operations: scrape | parse | validate | tabulate | aggregate
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR     = PROJECT_ROOT / "logs"
(LOGS_DIR / "runs").mkdir(parents=True, exist_ok=True)
(LOGS_DIR / "dev").mkdir(parents=True, exist_ok=True)


def _resolve_jsonl(state: str | None, operation: str) -> Path:
    run_id = os.environ.get("CF_RUN_ID")
    if run_id:
        return LOGS_DIR / "runs" / f"{run_id}.jsonl"
    ts   = datetime.now().strftime("%Y%m%d%H%M%S")
    name = f"{ts}-{state}-{operation}" if state else f"{ts}-{operation}"
    return LOGS_DIR / "dev" / f"{name}.jsonl"


class StateLogger:
    def __init__(self, state: str | None, operation: str):
        self.state     = state
        self.operation = operation
        self._jsonl    = _resolve_jsonl(state, operation)

        orc    = bool(os.environ.get("CF_RUN_ID"))
        daemon = bool(os.environ.get("CF_DAEMON"))

        # Console label: [alabama.scrape] in dev, [alabama] in orc
        label = f"{state}.{operation}" if (state and not orc) else (state or operation)

        log = logging.getLogger(f"cf.{state or 'global'}.{operation}")
        if not log.handlers:
            log.setLevel(logging.DEBUG)
            if not daemon:
                ch = logging.StreamHandler(sys.stdout)
                ch.setLevel(logging.INFO if orc else logging.DEBUG)
                ch.setFormatter(logging.Formatter(f"[{label}] %(message)s"))
                log.addHandler(ch)

        self._log = log

    # ── Standard passthrough ──────────────────────────────────────────────────

    def debug(self, msg: str):   self._log.debug(msg)
    def info(self, msg: str):    self._log.info(msg)
    def warning(self, msg: str): self._log.warning(msg)
    def error(self, msg: str):   self._log.error(msg)

    # ── JSONL writer ──────────────────────────────────────────────────────────

    def _emit(self, type: str, **kwargs):
        event = {
            "ts":        datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "state":     self.state,
            "operation": self.operation,
            "type":      type,
            **kwargs,
        }
        with open(self._jsonl, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

    # ── file_download events ──────────────────────────────────────────────────

    def file_download_start(self, filename: str, **kwargs):
        """Console only — fires before the request so long fetches are visible."""
        self.info(f"  Downloading {filename}...")

    def file_download_ok(self, filename: str, bytes: int, rows: int,
                         duration_s: float, **kwargs):
        """Console confirmation + JSONL record."""
        self.info(f"  ✓ {filename} ({rows:,} rows, {bytes/1024:.1f} KB)")
        self._emit("file_download", status="ok", filename=filename,
                   bytes=bytes, rows=rows, duration_s=round(duration_s, 2), **kwargs)

    def file_download_error(self, filename: str | None, error: str, **kwargs):
        """Console warning + JSONL record."""
        self.warning(f"  ✗ {filename or '(unknown)'}: {error}")
        self._emit("file_download", status="error", filename=filename,
                   error=error, **kwargs)

    def file_download_skip(self, filename: str, **kwargs):
        """Debug only — skips are noise at INFO level."""
        self.debug(f"  – {filename}: already downloaded, skipping")

    # ── page_scrape events ────────────────────────────────────────────────────

    def page_scrape_ok(self, entity: str, page_id: str | int,
                       duration_s: float, **kwargs):
        """JSONL only — caller owns console display (inline print or tqdm)."""
        self._emit("page_scrape", status="ok", entity=entity,
                   page_id=str(page_id), duration_s=round(duration_s, 2), **kwargs)

    def page_scrape_complete(self, filename: str, rows: int,
                             duration_s: float, ok: int, err: int, **kwargs):
        """Summary event emitted once per scraped entity file."""
        path = Path(filename)
        size = path.stat().st_size if path.exists() else 0
        self._emit("page_scrape", status="ok", filename=path.name,
                   bytes=size, rows=rows, duration_s=round(duration_s, 2),
                   ok=ok, err=err, **kwargs)

    def page_scrape_error(self, entity: str, page_id: str | int,
                          error: str, **kwargs):
        """Console warning + JSONL — errors always surface."""
        self.warning(f"  ✗ {entity} id={page_id}: {error}")
        self._emit("page_scrape", status="error", entity=entity,
                   page_id=str(page_id), error=error, **kwargs)

    # ── file_parsed events ────────────────────────────────────────────────────
    # Single event type for all file activity in a parser run.
    # Distinguish via role:
    #   "source"   — raw input file being transformed into a relation
    #   "registry" — lookup table loaded for enrichment
    #   "output"   — synthesized table flushed to disk at end of run

    def file_parsed(self, filename: str, relation: str, rows: int,
                    skipped: int = 0, duration_s: float = 0.0,
                    role: str = "source", **kwargs):
        """Console confirmation + JSONL record for one parsed file."""
        skip_str = f", {skipped:,} skipped" if skipped else ""
        self.info(f"  ✓ {filename} → {relation} ({rows:,} rows{skip_str})")
        self._emit("file_parsed", status="ok", filename=filename, relation=relation,
                   role=role, rows=rows, skipped=skipped,
                   duration_s=round(duration_s, 2), **kwargs)

    def file_parse_error(self, filename: str, error: str, **kwargs):
        """Console warning + JSONL record for a file that failed to parse."""
        self.warning(f"  ✗ {filename}: {error}")
        self._emit("file_parsed", status="error", filename=filename,
                   error=error, **kwargs)

    # ── enrichment events ─────────────────────────────────────────────────────
    # Use for: loading registry/lookup tables and joining them into output rows.

    def registry_loaded(self, filename: str, entries: int, relation: str = "", **kwargs):
        """Log a registry/lookup file loaded for enrichment.

        Emits file_parsed with role="registry" so all file activity shares
        one event type, queryable by role.
        """
        rel = relation or Path(filename).stem
        self.info(f"  ✓ {filename} ({entries:,} entries)")
        self._emit("file_parsed", status="ok", filename=filename, relation=rel,
                   role="registry", rows=entries, skipped=0, duration_s=0.0, **kwargs)

    def enrichment_summary(self, **kwargs):
        """Log the result of enriching output rows from a registry."""
        parts = ", ".join(f"{k}={v:,}" if isinstance(v, int) else f"{k}={v}"
                          for k, v in kwargs.items())
        self.info(f"  Enriched: {parts}")
        self._emit("enrichment_summary", **kwargs)



def get_logger(state: str | None, operation: str) -> StateLogger:
    """
    Return a StateLogger for the given state and operation.

    Args:
        state:     Lowercase state name ("alabama") or None for state-less ops.
        operation: One of: scrape | parse | validate | tabulate | aggregate
    """
    return StateLogger(state, operation)
