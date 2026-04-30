#!/usr/bin/env bash
# Apply db/changes.sql against the running dockerized Postgres.
# Run this between two /ingest calls to validate incremental delta selection.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T postgres psql -U interop -d interop < db/changes.sql
