"""Shared by every file in tests/: path setup, a tiny patch helper, and a
runner so each file works with plain `python3 tests/test_x.py` as well as
under pytest."""
import contextlib
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@contextlib.contextmanager
def patched(obj, name, value):
    """Temporarily replace obj.name -- what pytest's monkeypatch would do,
    without needing pytest."""
    missing = object()
    old = getattr(obj, name, missing)
    setattr(obj, name, value)
    try:
        yield
    finally:
        if old is missing:
            delattr(obj, name)
        else:
            setattr(obj, name, old)


def run(module_globals) -> int:
    tests = [(n, f) for n, f in sorted(module_globals.items())
             if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ok    {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  {name}")
            traceback.print_exc()
    print(f"{len(tests) - failed} passed, {failed} failed")
    return 1 if failed else 0
