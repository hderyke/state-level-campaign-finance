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


# ---- ops/nothing_ran_check.py ------------------------------------------------
from datetime import datetime, timedelta, timezone, date

try:
    import nothing_ran_check as nrc
except Exception as e:
    nrc = None
    _why_nrc = e

NAMES = {"arizona": "AZ", "texas": "TX"}
T0 = datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc)


def _sync(state, day, status="passed"):
    return {"state": state, "operation": "orc", "type": "state_duration", "status": status,
            "_ts": T0 + timedelta(days=day)}


def _push(state, day, status="completed", **kw):
    return {"state": state, "operation": "push-supabase", "type": "push_completed", "status": status,
            "_ts": T0 + timedelta(days=day, hours=1), **kw}


def test_fresh_means_a_passed_collection_that_was_then_pushed():
    if nrc is None:
        print(f"        (skipped: {_why_nrc})"); return
    got = nrc.last_fresh([_sync("arizona", 0), _push("arizona", 0)], NAMES)
    assert got == {"AZ": T0}


def test_a_failed_run_that_pushes_the_restored_copy_is_not_fresh():
    # Arizona, Aug 26 to Oct 3: failed, rolled back, pushed, every week.
    if nrc is None:
        print("        (skipped)"); return
    ev = [_sync("arizona", 0), _push("arizona", 0),
          _sync("arizona", 7, "failed"), _push("arizona", 7),
          _sync("arizona", 14, "failed"), _push("arizona", 14)]
    assert nrc.last_fresh(ev, NAMES) == {"AZ": T0}


def test_pushes_that_bring_nothing_new_do_not_count():
    if nrc is None:
        print("        (skipped)"); return
    ev = [_push("texas", 0),                               # by hand, no collection before it
          _sync("arizona", 0), _push("arizona", 0, "error"),
          _push("arizona", 1, "dry_run"),
          _push("arizona", 2, target="local"),
          {"state": "arizona", "operation": "push-r2", "type": "push_completed",
           "status": "completed", "_ts": T0 + timedelta(days=3)}]
    assert nrc.last_fresh(ev, NAMES) == {}


def test_a_later_push_by_hand_does_not_make_old_data_newer():
    if nrc is None:
        print("        (skipped)"); return
    ev = [_sync("arizona", 0), _push("arizona", 0), _push("arizona", 20)]
    assert nrc.last_fresh(ev, NAMES) == {"AZ": T0}


def test_stale_after_eight_days_and_never_run_is_stale():
    if nrc is None:
        print("        (skipped)"); return
    now = T0 + timedelta(days=9)
    stale = nrc.stale_states(["AZ", "TX", "GA"], {"AZ": T0, "GA": T0 + timedelta(days=5)}, now)
    assert [s[0] for s in stale] == ["TX", "AZ"]           # never-run first
    assert stale[0][1] is None
    assert nrc.stale_states(["AZ"], {"AZ": T0}, T0 + timedelta(days=7, hours=23)) == []


def test_time_before_the_schedule_changed_is_not_counted():
    if nrc is None:
        print("        (skipped)"); return
    changed = T0 + timedelta(days=30)
    now = changed + timedelta(days=5)
    assert nrc.stale_states(["AZ", "TX"], {"AZ": T0}, now, counting_from=changed) == []
    later = changed + timedelta(days=9)
    assert [s[0] for s in nrc.stale_states(["AZ", "TX"], {"AZ": T0}, later, counting_from=changed)] == ["AZ", "TX"]


def test_missed_2am_slots_are_named():
    if nrc is None:
        print("        (skipped)"); return
    hb = Path(tempfile.mkdtemp()) / "hb.log"
    hb.write_text("2026-10-03 13:00 Sat\n2026-10-04 02:00 Sun\n2026-10-05 01:00 Mon\n"
                  "2026-10-05 03:00 Mon\n2026-10-06 02:00 Tue\n")
    assert nrc.missed_2am(hb, days=8, today=date(2026, 10, 7)) == ["2026-10-05"]
    assert nrc.missed_2am(hb.parent / "none.log") is None


if __name__ == "__main__":
    sys.exit(run(globals()))
