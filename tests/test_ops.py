"""ops/daemon.py: which states a nightly run must NOT publish. ops/ is not
in git, so this file skips itself where ops/ is absent.
Run: python3 tests/test_ops.py"""
import json
import sys
import tempfile
from pathlib import Path
from _harness import run, ROOT

try:
    sys.path.insert(0, str(ROOT / "ops"))
    import daemon
except Exception as e:                      # no ops/ here, or it needs the real machine
    daemon = None
    _why = e


def _run_dir(lines):
    d = Path(tempfile.mkdtemp())
    (d / "log.jsonl").write_text("\n".join(json.dumps(x) for x in lines) + "\n")
    return d


def test_restored_states_are_published_unrestorable_ones_are_not():
    if daemon is None:
        print(f"        (skipped: {_why})"); return
    d = _run_dir([{"event": "start"},
                  {"event": "fallback_summary", "fallback_ok": ["AZ"], "fallback_fail": ["fl", "TX"]}])
    assert daemon._unrestorable_states(d) == {"FL", "TX"}


def test_no_fallback_event_or_no_log_means_nothing_held_back():
    if daemon is None:
        print(f"        (skipped: {_why})"); return
    assert daemon._unrestorable_states(_run_dir([{"event": "start"}])) == set()
    assert daemon._unrestorable_states(Path(tempfile.mkdtemp())) == set()


if __name__ == "__main__":
    sys.exit(run(globals()))
