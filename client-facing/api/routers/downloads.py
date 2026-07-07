import csv
import io
import os
import time

import boto3
from botocore.config import Config as BotocoreConfig
from fastapi import APIRouter, HTTPException

router = APIRouter()

# abbr → full state name (matches S3 folder casing)
_STATE_NAMES = {
    "AK": "Alaska", "AL": "Alabama", "AR": "Arkansas", "AZ": "Arizona",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "IA": "Iowa",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "MA": "Massachusetts", "MD": "Maryland",
    "ME": "Maine", "MI": "Michigan", "MN": "Minnesota", "MO": "Missouri",
    "MS": "Mississippi", "MT": "Montana", "NC": "North Carolina",
    "ND": "North Dakota", "NE": "Nebraska", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NV": "Nevada", "NY": "New York",
    "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VA": "Virginia",
    "VT": "Vermont", "WA": "Washington", "WI": "Wisconsin",
    "WV": "West Virginia", "WY": "Wyoming",
}

BUCKET = os.environ.get("S3_BUCKET", "")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
MANIFEST_KEY = "metadata/manifest.csv"
CACHE_TTL = 30  # seconds

_s3_config = BotocoreConfig(signature_version="s3v4")

# Local fallback for dev (sits next to DB or explicit path)
MANIFEST_PATH = (
    os.environ.get("MANIFEST_PATH")
    or os.path.join(os.path.dirname(os.environ.get("DB_PATH", "")), "manifest.csv")
    or "manifest.csv"
)

_cache: dict = {"data": None, "ts": 0.0}


def _fetch() -> list[dict]:
    now = time.time()
    if _cache["data"] is not None and now - _cache["ts"] < CACHE_TTL:
        return _cache["data"]

    if BUCKET:
        s3 = boto3.client("s3", region_name=REGION, config=_s3_config)
        resp = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        reader = csv.DictReader(io.StringIO(resp["Body"].read().decode()))
        states = [
            {
                "state": r["state"],
                "last_synced": r["last_synced"],
                "newest_record": r.get("newest_record") or None,
            }
            for r in reader
        ]
    else:
        try:
            with open(MANIFEST_PATH, newline="") as f:
                reader = csv.DictReader(f)
                states = [
                    {
                        "state": r["state"],
                        "last_synced": r["last_synced"],
                        "newest_record": r.get("newest_record") or None,
                    }
                    for r in reader
                ]
        except FileNotFoundError:
            states = []

    _cache["data"] = states
    _cache["ts"] = now
    return states


@router.get("/manifest")
def get_manifest():
    try:
        return {"states": _fetch()}
    except Exception:
        return {"states": []}


@router.get("/url")
def get_download_url(state: str, type: str = "db"):
    """Return a presigned S3 URL for a state data file. type: db | csv | raw"""
    if not BUCKET:
        raise HTTPException(status_code=503, detail="S3 not configured")

    state = state.upper()
    state_name = _STATE_NAMES.get(state)
    if not state_name:
        raise HTTPException(status_code=404, detail=f"Unknown state: {state}")

    slug = state_name.lower().replace(" ", "_")

    key_map = {
        "db":  f"data/{state_name}/{slug}.db",
        "csv": f"data/{state_name}/{slug}_clean.zip",
        "raw": f"data/{state_name}/{slug}_raw.zip",
    }
    key = key_map.get(type)
    if not key:
        raise HTTPException(status_code=400, detail=f"Invalid type: {type}")

    s3 = boto3.client("s3")
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": key},
        ExpiresIn=300,  # 5 min
    )
    return {"url": url}


@router.get("/master-url")
def get_master_url():
    """Presigned URL for the full combined DuckDB."""
    if not BUCKET:
        raise HTTPException(status_code=503, detail="S3 not configured")
    s3 = boto3.client("s3")
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": "data/state-level-cf.db"},
        ExpiresIn=300,
    )
    return {"url": url}
