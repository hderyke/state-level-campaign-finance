"""
cloud/lambda/manifest_updater/handler.py

Triggered by S3 PutObject on metadata/successful/*/manifest.json.
Reads the triggering state from the event key, updates that one row
in metadata/manifest.csv (all other states are left untouched).
"""

import boto3
import csv
import io
import json

s3 = boto3.client("s3")
MANIFEST_KEY = "metadata/manifest.csv"


def handler(event, context):
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = record["object"]["key"]  # e.g. metadata/successful/Arkansas/manifest.json

    # Pull state abbr + dates from the triggering manifest
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        state_manifest = json.loads(resp["Body"].read())
        state = state_manifest.get("state")
        last_synced = (state_manifest.get("last_updated") or "")[:10]
        newest_record = state_manifest.get("newest_record") or ""
    except Exception as e:
        print(f"[error] could not read triggering manifest {key}: {e}")
        return {"statusCode": 500}

    if not state or not last_synced:
        print(f"[warn] missing state or last_updated in {key}, skipping")
        return {"statusCode": 200}

    # Read existing manifest.csv (if any)
    rows = {}
    try:
        resp = s3.get_object(Bucket=bucket, Key=MANIFEST_KEY)
        reader = csv.DictReader(io.StringIO(resp["Body"].read().decode()))
        for row in reader:
            rows[row["state"]] = {
                "last_synced":   row.get("last_synced", ""),
                "newest_record": row.get("newest_record", ""),
            }
    except s3.exceptions.NoSuchKey:
        pass  # first run — start fresh
    except Exception as e:
        print(f"[warn] could not read existing manifest.csv: {e}")

    # Update just this state
    rows[state] = {"last_synced": last_synced, "newest_record": newest_record}

    # Write back
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["state", "last_synced", "newest_record"])
    for s in sorted(rows):
        writer.writerow([s, rows[s]["last_synced"], rows[s]["newest_record"]])

    s3.put_object(
        Bucket=bucket,
        Key=MANIFEST_KEY,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    print(f"manifest.csv updated — {state} → {last_synced} "
          f"(newest_record={newest_record or 'n/a'}) ({len(rows)} total state(s))")
    return {"statusCode": 200, "states": len(rows)}
