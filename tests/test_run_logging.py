"""One run folder for a daemon run, and where a push by hand is logged:
orc.py keeps a CF_RUN_ID that is already set, log_report.py merges the
several run_started/run_completed pairs a daemon run writes, and
logger.start_manual_run() starts a logs/prod run for a live push by hand.
Run: python3 tests/test_run_logging.py"""
import os
import sys
from _harness import run, ROOT

sys.path.insert(0, str(ROOT / "src" / "pipeline"))
from src.reporting import logger, log_report  # noqa: E402


class _env:
    """Sets (value) or clears (None) environment variables for a block."""
    def __init__(self, **values):
        self.values, self.saved = values, {}

    def __enter__(self):
        for k, v in self.values.items():
            self.saved[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def __exit__(self, *exc):
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_orc_keeps_a_run_id_that_is_already_set():
    from src import orc
    with _env(CF_RUN_ID="20261008_020001_sync_AZ-AK"):
        assert orc._setup_run_id("sync", ["AK"]) == "20261008_020001_sync_AZ-AK"
    with _env(CF_RUN_ID=None):
        rid = orc._setup_run_id("sync", ["AK"])
        assert rid.endswith("_sync_AK") and os.environ["CF_RUN_ID"] == rid


def test_a_live_push_by_hand_gets_a_prod_run_and_keeps_an_existing_one():
    with _env(CF_RUN_ID=None, CF_DAEMON=None):
        rid = logger.start_manual_run("push-supabase", ["tx", "OH"])
        assert rid.endswith("_push-supabase_TX-OH")
        assert logger.run_dir_for(rid) == logger.LOGS_DIR / "prod" / rid
    with _env(CF_RUN_ID="daemon_run", CF_DAEMON="1"):
        assert logger.start_manual_run("push-supabase", ["TX"]) == "daemon_run"
        assert logger.run_dir_for("daemon_run") == logger.LOGS_DIR / "daemon" / "daemon_run"


def test_report_merges_one_run_per_state_into_one():
    events = [
        {"type": "run_started", "run_id": "R", "command": "sync", "states": ["AZ"], "ts": "t1"},
        {"type": "run_completed", "status": "completed", "duration_s": 10.0, "passed": 1,
         "failed": 0, "ts": "t2"},
        {"type": "run_started", "run_id": "R", "command": "sync", "states": ["AK"], "ts": "t3"},
        {"type": "run_completed", "status": "completed", "duration_s": 5.5, "passed": 0,
         "failed": 1, "ts": "t4"},
    ]
    r = log_report.build_report(events)["run"]
    assert r["states"] == ["AZ", "AK"] and r["ts_start"] == "t1" and r["ts_end"] == "t4"
    assert (r["duration_s"], r["passed"], r["failed"], r["status"]) == (15.5, 1, 1, "completed")


def test_report_keeps_an_interrupted_status_and_a_single_run_is_unchanged():
    events = [
        {"type": "run_started", "run_id": "R", "states": ["AZ"], "ts": "t1"},
        {"type": "run_completed", "status": "interrupted", "duration_s": 3.0, "ts": "t2"},
        {"type": "run_started", "run_id": "R", "states": ["AK"], "ts": "t3"},
        {"type": "run_completed", "status": "completed", "duration_s": 1.0, "passed": 1,
         "failed": 0, "ts": "t4"},
    ]
    assert log_report.build_report(events)["run"]["status"] == "interrupted"
    one = log_report.build_report(events[:2])["run"]
    assert one["states"] == ["AZ"] and one["duration_s"] == 3.0 and one["status"] == "interrupted"


if __name__ == "__main__":
    sys.exit(run(globals()))
