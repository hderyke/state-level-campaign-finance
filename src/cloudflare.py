"""
src/cloudflare.py — Cloudflare R2 push/pull helpers.

Credentials are read from environment variables (loaded via python-dotenv in main.py):
    R2_ACCOUNT_ID
    R2_ACCESS_KEY_ID
    R2_SECRET_ACCESS_KEY
    R2_BUCKET
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.logger import get_logger, StateLogger

import boto3
from botocore.config import Config


# ── Client ─────────────────────────────────────────────────────────────────────

def _client():
    account_id = os.environ["R2_ACCOUNT_ID"]
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )

def _bucket() -> str:
    return os.environ["R2_BUCKET"]


# ── Low-level file ops (no logging — called by higher-level functions) ─────────

def _upload(local_path: Path, remote_key: str) -> int:
    """Upload one file, return bytes transferred."""
    client = _client()
    client.upload_file(str(local_path), _bucket(), remote_key)
    return local_path.stat().st_size


def _download(remote_key: str, local_path: Path) -> int:
    """Download one file, return bytes transferred."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    client = _client()
    client.download_file(_bucket(), remote_key, str(local_path))
    return local_path.stat().st_size


# ── Push ───────────────────────────────────────────────────────────────────────

def push_file(local_path: Path, remote_key: str) -> None:
    """Upload a single file to R2 (db target — state-less)."""
    log = get_logger(None, "push")
    t0  = time.perf_counter()
    log._emit("push_started", target="db", filename=local_path.name)
    files_ok = files_err = 0
    try:
        print(f"  ↑ pushing {remote_key}...")
        ft    = time.perf_counter()
        size  = _upload(local_path, remote_key)
        duration = round(time.perf_counter() - ft, 2)
        print(f"  ✓ pushed  {remote_key}")
        log._emit("file_pushed", status="ok", filename=local_path.name,
                  remote_key=remote_key, bytes=size, duration_s=duration)
        files_ok += 1
        log._emit("push_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
    except KeyboardInterrupt:
        log._emit("push_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise
    except Exception as e:
        log._emit("file_pushed", status="error", filename=local_path.name,
                  remote_key=remote_key, error=str(e))
        log._emit("push_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=1, error=str(e))
        raise


def push_state(state_name: str, project_root: Path) -> None:
    """Upload everything under data/{state_name}/ to R2."""
    state   = state_name.lower()
    log     = get_logger(state, "push")
    t0      = time.perf_counter()
    state_dir = project_root / "data" / state_name
    files     = sorted(f for f in state_dir.rglob("*") if f.is_file())

    log._emit("push_started", target="state", file_count=len(files))

    if not files:
        print(f"  [!] No files found under data/{state_name}/")
        log._emit("push_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, error="no files found")
        return

    files_ok = files_err = 0
    try:
        for local_path in files:
            remote_key = f"data/{state_name}/{local_path.relative_to(state_dir)}"
            ft = time.perf_counter()
            try:
                print(f"  ↑ {remote_key}")
                size     = _upload(local_path, remote_key)
                duration = round(time.perf_counter() - ft, 2)
                log._emit("file_pushed", status="ok", filename=local_path.name,
                          remote_key=remote_key, bytes=size, duration_s=duration)
                files_ok += 1
            except Exception as e:
                log._emit("file_pushed", status="error", filename=local_path.name,
                          remote_key=remote_key, error=str(e))
                print(f"  ✗ {remote_key}: {e}")
                files_err += 1

        log._emit("push_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log._emit("push_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise


def push_all(project_root: Path) -> None:
    """Upload the entire local data/ directory to R2. Requires confirmation."""
    log      = get_logger(None, "push")
    t0       = time.perf_counter()
    data_dir = project_root / "data"
    files    = sorted(f for f in data_dir.rglob("*") if f.is_file())

    if not files:
        print("[!] No files found under data/")
        return

    total_mb = sum(f.stat().st_size for f in files) / 1_000_000
    print(f"\n  About to push {len(files):,} files ({total_mb:.1f} MB) to R2.")
    print("  This will overwrite any existing objects at the same keys.")
    confirm = input("\n  Type 'yes' to continue: ").strip().lower()
    if confirm != "yes":
        print("  Aborted.")
        return

    log._emit("push_started", target="all", file_count=len(files),
              total_mb=round(total_mb, 1))
    files_ok = files_err = 0
    try:
        for local_path in files:
            remote_key = f"data/{local_path.relative_to(data_dir)}"
            ft = time.perf_counter()
            try:
                print(f"  ↑ {remote_key}")
                size     = _upload(local_path, remote_key)
                duration = round(time.perf_counter() - ft, 2)
                log._emit("file_pushed", status="ok", filename=local_path.name,
                          remote_key=remote_key, bytes=size, duration_s=duration)
                files_ok += 1
            except Exception as e:
                log._emit("file_pushed", status="error", filename=local_path.name,
                          remote_key=remote_key, error=str(e))
                print(f"  ✗ {remote_key}: {e}")
                files_err += 1

        log._emit("push_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log._emit("push_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise


# ── Pull ───────────────────────────────────────────────────────────────────────

def pull_file(remote_key: str, local_path: Path) -> None:
    """Download a single file from R2 (db target — state-less)."""
    log = get_logger(None, "pull")
    t0  = time.perf_counter()
    log._emit("pull_started", target="db", filename=local_path.name)
    files_ok = files_err = 0
    try:
        print(f"  ↓ pulling {remote_key}...")
        ft       = time.perf_counter()
        size     = _download(remote_key, local_path)
        duration = round(time.perf_counter() - ft, 2)
        print(f"  ✓ pulled  {remote_key}")
        log._emit("file_pulled", status="ok", filename=local_path.name,
                  remote_key=remote_key, bytes=size, duration_s=duration)
        files_ok += 1
        log._emit("pull_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
    except KeyboardInterrupt:
        log._emit("pull_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise
    except Exception as e:
        log._emit("file_pulled", status="error", filename=local_path.name,
                  remote_key=remote_key, error=str(e))
        log._emit("pull_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=1, error=str(e))
        raise


def pull_state(state_name: str, project_root: Path) -> None:
    """Download all files for a state from R2."""
    state    = state_name.lower()
    log      = get_logger(state, "pull")
    t0       = time.perf_counter()
    client   = _client()
    prefix   = f"data/{state_name}/"
    data_dir = project_root / "data"

    response = client.list_objects_v2(Bucket=_bucket(), Prefix=prefix)
    objects  = response.get("Contents", [])

    log._emit("pull_started", target="state", file_count=len(objects))

    if not objects:
        print(f"  [!] No files found in R2 under {prefix}")
        log._emit("pull_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, error="no files found in R2")
        return

    files_ok = files_err = 0
    try:
        for obj in objects:
            remote_key = obj["Key"]
            local_path = data_dir / remote_key.removeprefix("data/")
            ft = time.perf_counter()
            try:
                print(f"  ↓ {remote_key}")
                size     = _download(remote_key, local_path)
                duration = round(time.perf_counter() - ft, 2)
                log._emit("file_pulled", status="ok", filename=local_path.name,
                          remote_key=remote_key, bytes=size, duration_s=duration)
                files_ok += 1
            except Exception as e:
                log._emit("file_pulled", status="error", filename=local_path.name,
                          remote_key=remote_key, error=str(e))
                print(f"  ✗ {remote_key}: {e}")
                files_err += 1

        log._emit("pull_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log._emit("pull_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise


def pull_all(project_root: Path) -> None:
    """Download the entire data/ directory from R2. Requires confirmation."""
    log      = get_logger(None, "pull")
    t0       = time.perf_counter()
    client   = _client()
    response = client.list_objects_v2(Bucket=_bucket(), Prefix="data/")
    objects  = response.get("Contents", [])

    if not objects:
        print("[!] No files found in R2 under data/")
        return

    total_mb = sum(obj["Size"] for obj in objects) / 1_000_000
    print(f"\n  About to pull {len(objects):,} files ({total_mb:.1f} MB) from R2.")
    print("  This will overwrite any existing local files at the same paths.")
    confirm = input("\n  Type 'yes' to continue: ").strip().lower()
    if confirm != "yes":
        print("  Aborted.")
        return

    log._emit("pull_started", target="all", file_count=len(objects),
              total_mb=round(total_mb, 1))
    data_dir = project_root / "data"
    files_ok = files_err = 0
    try:
        for obj in objects:
            remote_key = obj["Key"]
            local_path = data_dir / remote_key.removeprefix("data/")
            ft = time.perf_counter()
            try:
                print(f"  ↓ {remote_key}")
                size     = _download(remote_key, local_path)
                duration = round(time.perf_counter() - ft, 2)
                log._emit("file_pulled", status="ok", filename=local_path.name,
                          remote_key=remote_key, bytes=size, duration_s=duration)
                files_ok += 1
            except Exception as e:
                log._emit("file_pulled", status="error", filename=local_path.name,
                          remote_key=remote_key, error=str(e))
                print(f"  ✗ {remote_key}: {e}")
                files_err += 1

        log._emit("pull_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)

    except KeyboardInterrupt:
        log._emit("pull_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
        raise
