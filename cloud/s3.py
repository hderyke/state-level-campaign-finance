"""
cloud/s3.py — AWS S3 push/pull helpers.

Replaces src/cloudflare.py. Talks directly to S3 via boto3 — no Worker, no
server-side manifest. Delta detection (added / modified / noop) is done by
stashing an MD5 in each object's metadata at upload time and comparing
against a freshly computed local MD5 on the next push, via `head_object`.

Credentials are read from environment variables (loaded via python-dotenv in main.py):
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_REGION       (defaults to us-east-1 if unset)
    S3_BUCKET

Bucket layout:
    data/{State}/{state}.db
    data/{State}/{state}_raw.zip        (zip of data/{State}/raw/)
    data/{State}/{state}_clean.zip      (zip of data/{State}/cleaned/, db excluded)
    data/state-level-cf.db

    metadata/latest/{State}/manifest.json
    metadata/latest/{State}/report.html
    metadata/latest/{State}/validate.json
    metadata/latest/{State}/queries.txt

    metadata/successful/{State}/...     (same four files — only written when
                                          the state's last validation run passed)

`latest/{State}` is overwritten on every push regardless of outcome.
`successful/{State}` is only touched on a passing run, so it always reflects
the last known-good state of the data. Neither directory keeps history.
"""

import csv
import hashlib
import json
import mimetypes
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.reporting.logger import get_logger

PROJECT_ROOT  = Path(__file__).resolve().parents[1]
DATA_DIR      = PROJECT_ROOT / "data"
METADATA_DIR  = PROJECT_ROOT / "metadata"
LOGS_PROD     = PROJECT_ROOT / "logs" / "prod"
STATES_CSV    = PROJECT_ROOT / "src" / "aliases" / "states.csv"

_RUN_DIR_RE = re.compile(r"^\d{8}_\d{6}_(sync|reparse)(_force)?_(?P<states>[A-Z]{2}(-[A-Z]{2})*)$")


# ========================== Shared helpers ============================

def _pusher() -> str:
    """Return '<git username> / <hostname>' for log attribution."""
    hostname = socket.gethostname()
    try:
        git_user = subprocess.check_output(
            ["git", "config", "user.name"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        git_user = os.environ.get("USER", "unknown")
    return f"{git_user} / {hostname}"


def _client():
    """Return a boto3 S3 client. Built-in retry/backoff via Config.

    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are only passed explicitly if
    set in the environment. If they're not set, boto3 falls through to its
    normal credential chain (~/.aws/credentials, AWS_PROFILE, an instance
    role, etc.) instead of erroring — so an existing `aws configure` setup
    works without needing anything in .env at all.
    """
    kwargs: dict = {
        "region_name": os.environ.get("AWS_REGION", "us-east-1"),
        "config": Config(signature_version="s3v4",
                          retries={"max_attempts": 6, "mode": "adaptive"}),
    }
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        kwargs["aws_access_key_id"]     = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("s3", **kwargs)


def _bucket() -> str:
    return os.environ["S3_BUCKET"]


def _file_hash(path: Path) -> str:
    """MD5 hex digest of a file, read in 8 MB chunks."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_states_csv() -> dict[str, str]:
    """Return {name_lower: abbr} from src/aliases/states.csv."""
    with open(STATES_CSV, encoding="utf-8") as f:
        return {row["name"].lower(): row["abbr"] for row in csv.DictReader(f)}


def _resolve_state_dir(state_name: str) -> Path | None:
    """Case-insensitive match of state_name against data/ subdirectories.
    Mirrors the lookup tabulate.py already uses. Returns None if no local
    directory exists yet for this state."""
    if not DATA_DIR.exists():
        return None
    matches = [d for d in DATA_DIR.iterdir()
               if d.is_dir() and d.name.lower() == state_name.lower()]
    return matches[0] if matches else None


def _slug(state_name: str) -> str:
    """'North Carolina' -> 'north_carolina' — matches metadata/{slug}_*.json
    and the {slug}.db filename tabulate.py writes."""
    return state_name.lower().replace(" ", "_")


# ============================ Zip helpers ==============================

def _zip_dir(src_dir: Path, out_zip: Path, exclude: set[str] | None = None) -> None:
    """Zip the contents of src_dir (recursively) into out_zip, storing paths
    relative to src_dir. Files whose name is in `exclude` are skipped."""
    exclude = exclude or set()
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(src_dir.rglob("*")):
            if f.is_file() and f.name not in exclude:
                zf.write(f, arcname=str(f.relative_to(src_dir)))


# ===================== S3 metadata / delta detection ====================

def _remote_head(key: str) -> dict | None:
    """MD5 (from metadata) + size of the object currently at `key`, or None if
    the object doesn't exist. One head_object call feeds both the delta-detection
    check and the byte-delta reported in file_pushed events."""
    try:
        resp = _client().head_object(Bucket=_bucket(), Key=key)
        return {
            "md5":   resp.get("Metadata", {}).get("md5"),
            "bytes": resp.get("ContentLength"),
        }
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def _remote_md5(key: str) -> str | None:
    """MD5 stashed in an object's metadata at its last upload, or None if the
    object doesn't exist (or predates this scheme and has no md5 metadata)."""
    head = _remote_head(key)
    return head["md5"] if head else None


def _upload_if_changed(local_path: Path, key: str, log) -> str | None:
    """Upload local_path to key only if its MD5 differs from what's stored in
    S3's object metadata. Returns 'added' | 'modified' | 'noop' on success,
    None on error (logged, not raised, so one bad file doesn't kill a push)."""
    t0 = time.perf_counter()
    try:
        local_md5  = _file_hash(local_path)
        remote     = _remote_head(key)
        remote_md5 = remote["md5"] if remote else None

        if remote_md5 == local_md5:
            print(f"  – {key} (unchanged)")
            log._emit("file_pushed", status="skipped", filename=local_path.name,
                       remote_key=key, duration_s=0.0)
            return "noop"

        action     = "modified" if remote_md5 is not None else "added"
        new_bytes  = local_path.stat().st_size
        old_bytes  = remote["bytes"] if remote else None
        delta_mb   = round((new_bytes - old_bytes) / 1_048_576, 3) if old_bytes is not None else None

        content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
        _client().upload_file(
            str(local_path), _bucket(), key,
            ExtraArgs={"Metadata": {"md5": local_md5}, "ContentType": content_type},
        )
        duration = round(time.perf_counter() - t0, 2)
        icon = "↑" if action == "added" else "↻"
        delta_str = f", {delta_mb:+.2f} MB" if delta_mb is not None else ""
        print(f"  {icon} {key}{delta_str}")
        log._emit("file_pushed", status="ok", filename=local_path.name, remote_key=key,
                   action=action, bytes=new_bytes, delta_mb=delta_mb, duration_s=duration)
        return action
    except Exception as e:
        print(f"  ✗ {key}: {e}")
        log._emit("file_pushed", status="error", filename=local_path.name,
                   remote_key=key, duration_s=round(time.perf_counter() - t0, 2),
                   error=str(e))
        return None


def _download(key: str, local_path: Path) -> bool:
    """Download key to local_path. Returns False (not an error) if the key
    doesn't exist in the bucket, so callers can skip missing artifacts."""
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        _client().download_file(_bucket(), key, str(local_path))
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# ======================= Manifest / report sourcing =====================

def _find_latest_run_dir(abbr: str) -> Path | None:
    """Most recent logs/prod/{ts}_{sync|reparse}[_force]_{ABBR[-ABBR...]}/ that
    includes this state. Matches both single-state and multi-state run dirs."""
    if not LOGS_PROD.exists():
        return None
    candidates = []
    for d in LOGS_PROD.iterdir():
        if not d.is_dir():
            continue
        m = _RUN_DIR_RE.match(d.name)
        if m and abbr.upper() in m.group("states").split("-"):
            candidates.append(d)
    return max(candidates, key=lambda d: d.name) if candidates else None


def _load_validate_report(slug: str) -> dict | None:
    p = METADATA_DIR / f"{slug}_latest.json"
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def build_manifest(slug: str, abbr: str) -> dict | None:
    """manifest.json payload for a state, built from its latest validate
    report (metadata/{slug}_latest.json). Returns None if that state has
    never been validated (i.e. never fully synced/reparsed)."""
    report = _load_validate_report(slug)
    if report is None:
        return None
    return {
        "state":              abbr.upper(),
        "last_updated":       datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "validated_at":       report.get("run_at"),
        "status":             "success" if report.get("passed") else "failed",
        "row_counts":         report.get("row_counts", {}),
        "newest_record":      report.get("newest_record"),
        "tier1_fill_rates":   report.get("tier1_fill_rates", {}),
        "tier2_warning_count": len(report.get("tier2_warnings", [])),
        "drift_warning_count": len(report.get("drift_warnings", [])),
    }


# ======================= Report injection ================================

def _inject_state_filter(html: str, state_name: str) -> str:
    """Inject a <script> that collapses all state-card sections except
    state_name, so the per-state report only shows that state expanded."""
    script = (
        "<script>"
        "(function(){"
        f"var t={json.dumps(state_name)};"
        "document.querySelectorAll('details.state-card').forEach(function(el){"
        "var n=el.querySelector('.state-name');"
        "if(n&&n.textContent.trim()!==t){el.removeAttribute('open');}"
        "});"
        "})();"
        "</script>"
    )
    if "</body>" in html:
        return html.replace("</body>", script + "\n</body>", 1)
    return html + "\n" + script


# ================================ Push ==================================

def push_state(abbr: str, state_name: str, project_root: Path = PROJECT_ROOT) -> bool:
    """
    Push one state's artifacts to S3:
      - data/{State}/{state}.db, {state}_raw.zip, {state}_clean.zip
      - metadata/latest/{State}/{manifest.json, report.html, validate.json, queries.txt}
      - metadata/successful/{State}/...  (only when the last validate run passed)

    Assumes sync/reparse has already been run locally — this does not trigger
    scraping/parsing itself, only publishes whatever's currently on disk.
    Returns True if the push completed with no per-file errors.
    """
    abbr      = abbr.upper()
    slug      = _slug(state_name)
    state_dir = _resolve_state_dir(state_name)
    log       = get_logger(state_name.lower(), "push")
    t0        = time.perf_counter()

    if state_dir is None:
        print(f"[!] No local data directory found for '{state_name}' under {DATA_DIR}")
        log._emit("push_completed", status="error", duration_s=0.0,
                   error="no local data directory")
        return False

    dir_name = state_dir.name   # preserve whatever casing is actually on disk
    log._emit("push_started", target="state", state=abbr)

    errors = 0
    results: dict[str, str | None] = {}

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)

        # --- data/ artifacts -------------------------------------------
        raw_dir   = state_dir / "raw"
        clean_dir = state_dir / "cleaned"
        db_path   = clean_dir / f"{slug}.db"

        if raw_dir.exists() and any(raw_dir.iterdir()):
            raw_zip = tmp / f"{slug}_raw.zip"
            _zip_dir(raw_dir, raw_zip)
            results["raw_zip"] = _upload_if_changed(
                raw_zip, f"data/{dir_name}/{slug}_raw.zip", log)
        else:
            print(f"  [!] No raw/ files for {dir_name} — skipping raw zip")

        if clean_dir.exists() and any(clean_dir.iterdir()):
            clean_zip = tmp / f"{slug}_clean.zip"
            _zip_dir(clean_dir, clean_zip, exclude={db_path.name})
            results["clean_zip"] = _upload_if_changed(
                clean_zip, f"data/{dir_name}/{slug}_clean.zip", log)
        else:
            print(f"  [!] No cleaned/ files for {dir_name} — skipping clean zip")

        if db_path.exists():
            results["db"] = _upload_if_changed(
                db_path, f"data/{dir_name}/{slug}.db", log)
        else:
            print(f"  [!] No {slug}.db found — run tabulate.py before pushing")

        # --- metadata/ artifacts -----------------------------------------
        manifest      = build_manifest(slug, abbr)
        run_dir       = _find_latest_run_dir(abbr)
        report_path   = (run_dir / "report.html") if run_dir else None
        validate_path = METADATA_DIR / f"{slug}_latest.json"
        queries_path  = METADATA_DIR / f"{slug}_queries.txt"

        if manifest is None:
            print(f"  [!] No validate report for {dir_name} — skipping metadata/")
        else:
            tiers = ["latest"]
            if manifest["status"] == "success":
                tiers.append("successful")
            else:
                print(f"  ↷ last validation FAILED — not touching metadata/successful/{dir_name}")

            manifest_path = tmp / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            if report_path is None or not report_path.exists():
                print(f"  [!] No standalone run report found for {abbr} in logs/prod/ — "
                      f"report.html will not be updated")

            # Build a filtered report (target state expanded, others collapsed)
            filtered_report_path: Path | None = None
            if report_path and report_path.exists():
                raw_html = report_path.read_text(encoding="utf-8")
                filtered_html = _inject_state_filter(raw_html, state_name.title())
                filtered_report_path = tmp / "report.html"
                filtered_report_path.write_text(filtered_html, encoding="utf-8")

            for tier in tiers:
                base = f"metadata/{tier}/{dir_name}"
                results[f"{tier}/manifest"] = _upload_if_changed(
                    manifest_path, f"{base}/manifest.json", log)
                if filtered_report_path:
                    results[f"{tier}/report"] = _upload_if_changed(
                        filtered_report_path, f"{base}/report.html", log)
                if validate_path.exists():
                    results[f"{tier}/validate"] = _upload_if_changed(
                        validate_path, f"{base}/validate.json", log)
                if queries_path.exists():
                    results[f"{tier}/queries"] = _upload_if_changed(
                        queries_path, f"{base}/queries.txt", log)

    errors = sum(1 for v in results.values() if v is None)
    ok     = sum(1 for v in results.values() if v is not None)
    duration = round(time.perf_counter() - t0, 1)

    log._emit("push_completed", status="completed" if errors == 0 else "error",
               duration_s=duration, files_ok=ok, files_err=errors)
    print(f"\n  {'✓' if errors == 0 else '!'} {dir_name}: {ok} ok, {errors} error(s) "
          f"({duration}s)")
    return errors == 0


def push_db(project_root: Path = PROJECT_ROOT) -> bool:
    """Push the aggregate database to data/state-level-cf.db."""
    db_path = project_root / "data" / "state-level-cf.db"
    if not db_path.exists():
        print(f"[!] Aggregate db not found: {db_path}")
        return False
    log = get_logger(None, "push")
    log._emit("push_started", target="db")
    t0 = time.perf_counter()
    result = _upload_if_changed(db_path, "data/state-level-cf.db", log)
    log._emit("push_completed", status="completed" if result is not None else "error",
               duration_s=round(time.perf_counter() - t0, 1),
               files_ok=1 if result is not None else 0,
               files_err=0 if result is not None else 1)
    return result is not None


def push_all(project_root: Path = PROJECT_ROOT) -> None:
    """Push every state that has a local data/ directory, one at a time,
    then the aggregate db. Requires confirmation."""
    data_dir = project_root / "data"
    if not data_dir.exists():
        print("[!] No data/ directory found")
        return

    states_by_name = _load_states_csv()  # {name_lower: abbr}

    state_dirs = [d for d in sorted(data_dir.iterdir())
                  if d.is_dir() and d.name.lower() in states_by_name]
    unknown = [d for d in sorted(data_dir.iterdir())
               if d.is_dir() and d.name.lower() not in states_by_name]
    if unknown:
        print(f"  [!] Skipping unregistered directories: {', '.join(d.name for d in unknown)}")

    if not state_dirs:
        print("[!] No known state directories found under data/")
        return

    print(f"\n  About to push {len(state_dirs)} state(s) + aggregate db to S3.")
    print("  This will overwrite existing objects at the same keys.")
    if input("\n  Type 'yes' to continue: ").strip().lower() != "yes":
        print("  Aborted.")
        return

    results = {}
    for d in state_dirs:
        abbr = states_by_name[d.name.lower()]
        results[abbr] = push_state(abbr, d.name.lower(), project_root)

    push_db(project_root)

    failed = [a for a, ok in results.items() if not ok]
    print(f"\n  {'=' * 60}\n  {len(results) - len(failed)}/{len(results)} state(s) pushed cleanly"
          + (f" — errors on: {', '.join(failed)}" if failed else ""))


# ================================ Pull ==================================

def pull_state(abbr: str, state_name: str, project_root: Path = PROJECT_ROOT) -> None:
    """Download a state's data/ artifacts from S3 and rebuild
    data/{State}/raw/, data/{State}/cleaned/, and the .db file locally.
    Existing local files are overwritten; nothing is deleted from disk."""
    abbr  = abbr.upper()
    slug  = _slug(state_name)
    state_dir = _resolve_state_dir(state_name)
    if state_dir is None:
        state_dir = project_root / "data" / state_name.title()
    dir_name = state_dir.name

    log = get_logger(state_name.lower(), "pull")
    t0  = time.perf_counter()
    log._emit("pull_started", target="state", state=abbr)

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)
        ok = err = 0

        raw_zip = tmp / f"{slug}_raw.zip"
        if _download(f"data/{dir_name}/{slug}_raw.zip", raw_zip):
            (state_dir / "raw").mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(raw_zip) as zf:
                zf.extractall(state_dir / "raw")
            print(f"  ✓ restored raw/ from {slug}_raw.zip")
            log._emit("file_pulled", status="ok", filename=f"{slug}_raw.zip")
            ok += 1
        else:
            print(f"  [!] No raw zip in S3 for {dir_name}")

        clean_zip = tmp / f"{slug}_clean.zip"
        if _download(f"data/{dir_name}/{slug}_clean.zip", clean_zip):
            (state_dir / "cleaned").mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(clean_zip) as zf:
                zf.extractall(state_dir / "cleaned")
            print(f"  ✓ restored cleaned/ from {slug}_clean.zip")
            log._emit("file_pulled", status="ok", filename=f"{slug}_clean.zip")
            ok += 1
        else:
            print(f"  [!] No clean zip in S3 for {dir_name}")

        db_path = state_dir / "cleaned" / f"{slug}.db"
        if _download(f"data/{dir_name}/{slug}.db", db_path):
            print(f"  ✓ pulled {slug}.db")
            log._emit("file_pulled", status="ok", filename=f"{slug}.db")
            ok += 1
        else:
            print(f"  [!] No {slug}.db in S3 for {dir_name}")

    duration = round(time.perf_counter() - t0, 1)
    log._emit("pull_completed", status="completed", duration_s=duration,
               files_ok=ok, files_err=err)
    print(f"\n  ✓ {dir_name}: {ok} artifact(s) restored ({duration}s)")


def pull_db(project_root: Path = PROJECT_ROOT) -> None:
    """Download the aggregate database from data/state-level-cf.db."""
    db_path = project_root / "data" / "state-level-cf.db"
    log = get_logger(None, "pull")
    t0 = time.perf_counter()
    log._emit("pull_started", target="db")
    if _download("data/state-level-cf.db", db_path):
        print(f"  ✓ pulled state-level-cf.db")
        log._emit("pull_completed", status="completed",
                   duration_s=round(time.perf_counter() - t0, 1), files_ok=1, files_err=0)
    else:
        print(f"  [!] No aggregate db found in S3")
        log._emit("pull_completed", status="error",
                   duration_s=round(time.perf_counter() - t0, 1), files_ok=0, files_err=1)


def pull_all(project_root: Path = PROJECT_ROOT) -> None:
    """Pull every registered state's data/ artifacts from S3, then the
    aggregate db. Requires confirmation."""
    states_by_name = _load_states_csv()  # {name_lower: abbr}

    print(f"\n  About to pull {len(states_by_name)} state(s) + aggregate db from S3.")
    print("  This will overwrite existing local files at the same paths.")
    if input("\n  Type 'yes' to continue: ").strip().lower() != "yes":
        print("  Aborted.")
        return

    for name, abbr in sorted(states_by_name.items()):
        pull_state(abbr, name, project_root)

    pull_db(project_root)
