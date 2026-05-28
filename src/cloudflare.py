"""
src/cloudflare.py — Cloudflare R2 push/pull helpers.

Credentials are read from environment variables (loaded via python-dotenv in main.py):
    R2_ACCOUNT_ID
    R2_ACCESS_KEY_ID
    R2_SECRET_ACCESS_KEY
    R2_BUCKET
    WORKER_URL           — e.g. https://campaign-finance-r2.hgd2003.workers.dev
    WORKER_API_KEY       — shared secret for Worker auth
"""

import csv
import hashlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import boto3
import requests
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.reporting.logger import get_logger


# ========================== Shared helpers ============================

def _pusher() -> str:
    """Return '<git username> / <hostname>' for manifest attribution."""
    hostname = socket.gethostname()
    try:
        git_user = subprocess.check_output(
            ["git", "config", "user.name"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        git_user = os.environ.get("USER", "unknown")
    return f"{git_user} / {hostname}"


def _client():
    """Return a boto3 S3 client pointed at the Cloudflare R2 endpoint."""
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
    """Return the R2 bucket name from the environment."""
    return os.environ["R2_BUCKET"]


def _worker_url() -> str:
    """Return the Worker base URL with any trailing slash stripped."""
    return os.environ["WORKER_URL"].rstrip("/")

def _worker_headers() -> dict:
    """Return auth headers for Worker API requests."""
    return {
        "Content-Type": "application/json",
        "X-Api-Key": os.environ["WORKER_API_KEY"],
    }

def _file_hash(path: Path) -> str:
    """Return the MD5 hex digest of a file, read in 8 MB chunks."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _row_count(path: Path) -> int | None:
    """Count data rows (header excluded) for CSV/TSV files; None for everything else.

    Skips files larger than 500 MB to avoid blocking on giant CA exports.
    Uses raw byte scanning so it's fast even for multi-hundred-MB files.
    """
    if path.suffix.lower() not in (".csv", ".tsv"):
        return None
    try:
        if path.stat().st_size > 500 * 1024 * 1024:
            return None
        count = 0
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                count += chunk.count(b"\n")
        return max(0, count - 1)  # subtract header row
    except Exception:
        return None


# =========================== Manifest check ==========================

def check_manifest(state_dir: Path) -> tuple[int, int]:
    """Remove manifest entries whose raw file no longer exists on disk.

    Reads data/{State}/manifest.csv, drops any row whose `filename` is missing
    from data/{State}/raw/, rewrites the manifest in place, and logs every
    pruned entry.

    Returns (kept, pruned).  Both are 0 if no manifest exists.
    """
    state    = state_dir.name.lower()
    log      = get_logger(state, "push")
    manifest = state_dir / "manifest.csv"
    raw_dir  = state_dir / "raw"

    if not manifest.exists():
        return 0, 0

    with open(manifest, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return 0, 0

    fieldnames = list(rows[0].keys())
    kept:   list[dict] = []
    pruned: list[dict] = []

    for row in rows:
        fname = row.get("filename", "")
        if fname and (raw_dir / fname).exists():
            kept.append(row)
        else:
            pruned.append(row)

    if pruned:
        with open(manifest, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(kept)
        for row in pruned:
            fname = row.get("filename", "(unknown)")
            print(f"  [manifest] pruned missing: {fname}")
            log._emit("manifest_pruned", filename=fname)

    log._emit("manifest_checked", kept=len(kept), pruned=len(pruned))
    return len(kept), len(pruned)


# ================================ Push ===============================

def _worker_intent(files: list[dict]) -> dict[str, dict]:
    """Call /push/intent, return results keyed by remote key."""
    resp = requests.post(
        f"{_worker_url()}/push/intent",
        headers=_worker_headers(),
        json={"files": files},
        timeout=30,
    )
    resp.raise_for_status()
    return {r["key"]: r for r in resp.json()["files"]}

def _worker_confirm(pusher: str, files: list[dict]) -> None:
    """Call /push/confirm to record manifest entry."""
    resp = requests.post(
        f"{_worker_url()}/push/confirm",
        headers=_worker_headers(),
        json={"pusher": pusher, "files": files},
        timeout=30,
    )
    resp.raise_for_status()


def _upload_presigned(upload_url: str, local_path: Path) -> None:
    """PUT a file directly to R2 via pre-signed URL (streams, no size limit)."""
    with open(local_path, "rb") as f:
        resp = requests.put(upload_url, data=f, timeout=600)
    resp.raise_for_status()


def _delete_r2_keys(key_sizes: dict[str, int], log) -> tuple[int, int, list[dict]]:
    """Delete R2 objects in batches. key_sizes: {key: size_bytes}.

    Returns (deleted, errors, confirmed) where confirmed is the list of
    deletion dicts to be included in the caller's _worker_confirm payload.
    """
    if not key_sizes:
        return 0, 0, []
    client    = _client()
    bucket    = _bucket()
    deleted = errors = 0
    confirmed: list[dict] = []
    keys      = list(key_sizes)
    for i in range(0, len(keys), 1000):
        batch_keys = keys[i:i + 1000]
        batch      = [{"Key": k} for k in batch_keys]
        try:
            resp = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": batch, "Quiet": False},
            )
            for obj in resp.get("Deleted", []):
                print(f"  ✗ deleted {obj['Key']}")
                log._emit("file_deleted", status="ok", remote_key=obj["Key"])
                confirmed.append({
                    "key":       obj["Key"],
                    "action":    "deleted",
                    "byteDelta": -key_sizes.get(obj["Key"], 0),
                })
                deleted += 1
            for err in resp.get("Errors", []):
                print(f"  [!] delete error {err['Key']}: {err['Message']}")
                log._emit("file_deleted", status="error",
                          remote_key=err["Key"], error=err["Message"])
                errors += 1
        except Exception as e:
            for k in batch_keys:
                log._emit("file_deleted", status="error", remote_key=k, error=str(e))
            errors += len(batch_keys)
    return deleted, errors, confirmed


def push_file(local_path: Path, remote_key: str) -> None:
    """Upload a single file to R2 via Worker."""
    log    = get_logger(None, "push")
    t0     = time.perf_counter()
    pusher = _pusher()
    log._emit("push_started", target="db", filename=local_path.name)
    try:
        print(f"  ↑ pushing {remote_key}...")
        intent = _worker_intent([{
            "key":  remote_key,
            "size": local_path.stat().st_size,
            "hash": _file_hash(local_path),
        }])
        info = intent[remote_key]

        if info["action"] == "noop":
            print(f"  – skipped {remote_key} (unchanged)")
            log._emit("file_pushed", status="skipped", filename=local_path.name,
                      remote_key=remote_key, rows=_row_count(local_path),
                      delta_mb=0.0, duration_s=0.0)
            log._emit("push_completed", status="completed",
                      duration_s=round(time.perf_counter() - t0, 1),
                      files_ok=0, files_err=0, files_skipped=1)
            return

        ft       = time.perf_counter()
        _upload_presigned(info["uploadUrl"], local_path)
        duration = round(time.perf_counter() - ft, 2)
        print(f"  ✓ pushed  {remote_key}")
        log._emit("file_pushed", status="ok", filename=local_path.name,
                  remote_key=remote_key, rows=_row_count(local_path),
                  delta_mb=round(info["byteDelta"] / 1_000_000, 3),
                  duration_s=duration)
        _worker_confirm(pusher, [{
            "key": info["key"], "action": info["action"], "byteDelta": info["byteDelta"],
        }])
        log._emit("push_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=1, files_err=0)
    except KeyboardInterrupt:
        log._emit("push_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=0)
        raise
    except Exception as e:
        log._emit("file_pushed", status="error", filename=local_path.name,
                  remote_key=remote_key, rows=None, delta_mb=None,
                  duration_s=round(time.perf_counter() - t0, 2), error=str(e))
        log._emit("push_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=1, error=str(e))
        raise


def _push_files(file_meta: list[dict], log, t0: float, pusher: str,
                deleted_files: list[dict] | None = None) -> tuple[int, int]:
    """Shared logic for push_state and push_all — intent, upload loop, confirm."""
    try:
        intent = _worker_intent([{
            "key":  m["remote_key"],
            "size": m["local_path"].stat().st_size,
            "hash": _file_hash(m["local_path"]),
        } for m in file_meta])
    except Exception as e:
        log._emit("push_completed", status="error",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=len(file_meta), error=f"intent failed: {e}")
        raise

    files_ok = files_err = 0
    confirmed: list[dict] = list(deleted_files or [])   # deletions go first
    try:
        for m in file_meta:
            local_path = m["local_path"]
            remote_key = m["remote_key"]
            info = intent[remote_key]
            ft   = time.perf_counter()

            if info["action"] == "noop":
                print(f"  – {remote_key} (unchanged)")
                log._emit("file_pushed", status="skipped", filename=local_path.name,
                          remote_key=remote_key, rows=_row_count(local_path),
                          delta_mb=0.0, duration_s=0.0)
                continue

            try:
                print(f"  ↑ {remote_key}")
                _upload_presigned(info["uploadUrl"], local_path)
                duration = round(time.perf_counter() - ft, 2)
                log._emit("file_pushed", status="ok", filename=local_path.name,
                          remote_key=remote_key, rows=_row_count(local_path),
                          delta_mb=round(info["byteDelta"] / 1_000_000, 3),
                          duration_s=duration)
                confirmed.append({
                    "key": info["key"], "action": info["action"],
                    "byteDelta": info["byteDelta"],
                })
                files_ok += 1
            except Exception as e:
                log._emit("file_pushed", status="error", filename=local_path.name,
                          remote_key=remote_key, rows=None, delta_mb=None,
                          duration_s=round(time.perf_counter() - ft, 2), error=str(e))
                print(f"  ✗ {remote_key}: {e}")
                files_err += 1

        if confirmed:
            _worker_confirm(pusher, confirmed)
        return files_ok, files_err

    except KeyboardInterrupt:
        if confirmed:
            _worker_confirm(pusher, confirmed)
        raise


def push_state(state_name: str, project_root: Path) -> None:
    """Sync local data/{state_name}/ to R2 — uploads new/changed files and
    deletes R2 objects that no longer exist locally."""
    state     = state_name.lower()
    log       = get_logger(state, "push")
    t0        = time.perf_counter()
    pusher    = _pusher()
    state_dir = project_root / "data" / state_name

    check_manifest(state_dir)
    delta = diff_state(state_name, project_root)

    # Delete anything in R2 that's been removed locally
    stale_key_sizes = {
        f"data/{state_name}/{k}": size
        for k, size in delta["only_remote"].items()
    }
    _, _, deleted_files = _delete_r2_keys(stale_key_sizes, log)

    files = sorted(f for f in state_dir.rglob("*") if f.is_file())

    log._emit("push_started", target="state", file_count=len(files))
    if not files:
        print(f"  [!] No files found under data/{state_name}/")
        log._emit("push_completed", status="error", duration_s=0.0,
                  files_ok=0, files_err=0, error="no files found")
        return

    file_meta = [
        {"local_path": f,
         "remote_key": f"data/{state_name}/{f.relative_to(state_dir)}"}
        for f in files
    ]
    try:
        files_ok, files_err = _push_files(file_meta, log, t0, pusher,
                                           deleted_files=deleted_files)
        log._emit("push_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
    except KeyboardInterrupt:
        log._emit("push_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=0)
        raise


def push_all(project_root: Path) -> None:
    """Upload the entire local data/ directory to R2 via Worker. Requires confirmation."""
    log      = get_logger(None, "push")
    t0       = time.perf_counter()
    pusher   = _pusher()
    data_dir = project_root / "data"
    files    = sorted(f for f in data_dir.rglob("*") if f.is_file())

    if not files:
        print("[!] No files found under data/")
        return

    diff_all(project_root)

    total_mb = sum(f.stat().st_size for f in files) / 1_000_000
    print(f"\n  About to push {len(files):,} files ({total_mb:.1f} MB) to R2.")
    print("  This will overwrite any existing objects at the same keys.")
    if input("\n  Type 'yes' to continue: ").strip().lower() != "yes":
        print("  Aborted.")
        return

    log._emit("push_started", target="all", file_count=len(files),
              total_mb=round(total_mb, 1))

    file_meta = [
        {"local_path": f, "remote_key": f"data/{f.relative_to(data_dir)}"}
        for f in files
    ]
    try:
        files_ok, files_err = _push_files(file_meta, log, t0, pusher)
        log._emit("push_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=files_ok, files_err=files_err)
    except KeyboardInterrupt:
        log._emit("push_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=0)
        raise


# ================================ Pull ===============================

def _download(remote_key: str, local_path: Path) -> int:
    """Download remote_key from R2 to local_path; returns bytes written."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    client = _client()
    client.download_file(_bucket(), remote_key, str(local_path))
    return local_path.stat().st_size


def pull_file(remote_key: str, local_path: Path) -> None:
    """Download a single file from R2."""
    log = get_logger(None, "pull")
    t0  = time.perf_counter()
    log._emit("pull_started", target="db", filename=local_path.name)
    try:
        print(f"  ↓ pulling {remote_key}...")
        ft       = time.perf_counter()
        size     = _download(remote_key, local_path)
        duration = round(time.perf_counter() - ft, 2)
        print(f"  ✓ pulled  {remote_key}")
        log._emit("file_pulled", status="ok", filename=local_path.name,
                  remote_key=remote_key, bytes=size, rows=_row_count(local_path),
                  duration_s=duration)
        log._emit("pull_completed", status="completed",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=1, files_err=0)
    except KeyboardInterrupt:
        log._emit("pull_completed", status="interrupted",
                  duration_s=round(time.perf_counter() - t0, 1),
                  files_ok=0, files_err=0)
        raise
    except Exception as e:
        log._emit("file_pulled", status="error", filename=local_path.name,
                  remote_key=remote_key, rows=None,
                  duration_s=round(time.perf_counter() - t0, 2), error=str(e))
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
                          remote_key=remote_key, bytes=size, rows=_row_count(local_path),
                          duration_s=duration)
                files_ok += 1
            except Exception as e:
                log._emit("file_pulled", status="error", filename=local_path.name,
                          remote_key=remote_key, rows=None,
                          duration_s=round(time.perf_counter() - ft, 2), error=str(e))
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
    if input("\n  Type 'yes' to continue: ").strip().lower() != "yes":
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
                          remote_key=remote_key, bytes=size, rows=_row_count(local_path),
                          duration_s=duration)
                files_ok += 1
            except Exception as e:
                log._emit("file_pulled", status="error", filename=local_path.name,
                          remote_key=remote_key, rows=None,
                          duration_s=round(time.perf_counter() - ft, 2), error=str(e))
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


# ========================= Diff (local vs R2) ========================

def _r2_listing(prefix: str) -> dict[str, int]:
    """Return {relative_key: size_bytes} for all R2 objects under prefix.
    Paginates automatically so buckets with >1000 objects are fully covered.
    """
    client  = _client()
    bucket  = _bucket()
    objects: dict[str, int] = {}
    kwargs: dict = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            rel = obj["Key"].removeprefix(prefix)
            objects[rel] = obj["Size"]
        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break
    return objects


def _local_listing(root: Path) -> dict[str, int]:
    """Return {relative_path: size_bytes} for all files under root."""
    if not root.exists():
        return {}
    return {
        str(f.relative_to(root)): f.stat().st_size
        for f in root.rglob("*") if f.is_file()
    }


def _print_diff(label: str,
                only_remote: dict[str, int],
                only_local:  dict[str, int],
                size_diff:   dict[str, tuple[int, int]],
                matched:     int) -> None:
    """Print a formatted diff report to stdout."""
    bar = "─" * 54
    print(f"\n  {label}\n  {bar}")
    if only_remote:
        print(f"\n  In R2 only — missing locally ({len(only_remote)}):")
        for k, sz in sorted(only_remote.items()):
            print(f"    ✗  {k}  ({sz / 1_000:.1f} KB)")
    if only_local:
        print(f"\n  Local only — not yet pushed ({len(only_local)}):")
        for k, sz in sorted(only_local.items()):
            print(f"    ↑  {k}  ({sz / 1_000:.1f} KB)")
    if size_diff:
        print(f"\n  Size mismatch — local vs R2 ({len(size_diff)}):")
        for k, (loc, r2) in sorted(size_diff.items()):
            delta = (loc - r2) / 1_000
            sign  = "+" if delta >= 0 else ""
            print(f"    ≠  {k}  local={loc/1_000:.1f} KB  r2={r2/1_000:.1f} KB  ({sign}{delta:.1f} KB)")
    if not any([only_remote, only_local, size_diff]):
        print(f"  ✓  All {matched} files match")
    else:
        print(f"\n  {matched} matched · "
              f"{len(only_remote)} only-remote · "
              f"{len(only_local)} only-local · "
              f"{len(size_diff)} size-mismatch")


def diff_state(state_name: str, project_root: Path) -> dict:
    """Compare local data/{state_name}/ against R2 and print a diff report.

    Returns a dict with keys: only_remote, only_local, size_mismatch, matched.
    """
    log       = get_logger(state_name.lower(), "push")
    prefix    = f"data/{state_name}/"
    state_dir = project_root / "data" / state_name

    r2    = _r2_listing(prefix)
    local = _local_listing(state_dir)

    r2_keys    = set(r2)
    local_keys = set(local)

    only_remote = {k: r2[k]    for k in r2_keys - local_keys}
    only_local  = {k: local[k] for k in local_keys - r2_keys}
    both        = r2_keys & local_keys
    size_diff   = {k: (local[k], r2[k]) for k in both if local[k] != r2[k]}
    matched     = len(both) - len(size_diff)

    _print_diff(f"diff — {state_name}", only_remote, only_local, size_diff, matched)
    log._emit("diff_completed",
              matched=matched, only_remote=len(only_remote),
              only_local=len(only_local), size_mismatch=len(size_diff))

    return {"only_remote": only_remote, "only_local": only_local,
            "size_mismatch": size_diff, "matched": matched}


def diff_all(project_root: Path) -> dict:
    """Compare the entire local data/ directory against R2 and print a diff report."""
    log      = get_logger(None, "push")
    data_dir = project_root / "data"

    r2    = _r2_listing("data/")
    local = _local_listing(data_dir)

    r2_keys    = set(r2)
    local_keys = set(local)

    only_remote = {k: r2[k]    for k in r2_keys - local_keys}
    only_local  = {k: local[k] for k in local_keys - r2_keys}
    both        = r2_keys & local_keys
    size_diff   = {k: (local[k], r2[k]) for k in both if local[k] != r2[k]}
    matched     = len(both) - len(size_diff)

    _print_diff("diff — all states", only_remote, only_local, size_diff, matched)
    log._emit("diff_completed",
              matched=matched, only_remote=len(only_remote),
              only_local=len(only_local), size_mismatch=len(size_diff))

    return {"only_remote": only_remote, "only_local": only_local,
            "size_mismatch": size_diff, "matched": matched}

