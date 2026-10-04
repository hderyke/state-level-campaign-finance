"""Run every test file in this folder: python3 tests/run_all.py"""
import subprocess
import sys
from pathlib import Path

here = Path(__file__).parent
bad = []
for f in sorted(here.glob("test_*.py")):
    print(f"\n== {f.name}")
    if subprocess.run([sys.executable, str(f)], cwd=here).returncode:
        bad.append(f.name)
print("\n" + ("FAILED: " + ", ".join(bad) if bad else "all test files passed"))
sys.exit(1 if bad else 0)
