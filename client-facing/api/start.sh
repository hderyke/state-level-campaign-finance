#!/bin/sh
set -e

DB_DIR=$(dirname "$DB_PATH")
mkdir -p "$DB_DIR"

echo "[startup] downloading state-level-cf.db from s3://$S3_BUCKET/data/state-level-cf.db ..."
aws s3 cp "s3://$S3_BUCKET/data/state-level-cf.db" "$DB_PATH"
echo "[startup] downloading manifest.csv from s3://$S3_BUCKET/metadata/manifest.csv ..."
aws s3 cp "s3://$S3_BUCKET/metadata/manifest.csv" "$(dirname "$DB_PATH")/manifest.csv"
echo "[startup] download complete, starting API..."

exec uvicorn main:app --host 0.0.0.0 --port 8000
