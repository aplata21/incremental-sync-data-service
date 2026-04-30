"""End-to-end: zero-delta rule for the share artifact.

Spec: "If a successful non-dry run has delta_row_count = 0 for a table:
    - do not create a new share artifact for that table
    - do not modify the existing share artifact for that table
    - report share_path = null in manifest and durable event"
"""

from __future__ import annotations

import json
from datetime import timezone

import psycopg


def test_zero_delta_table_share_file_untouched(
    orchestrator, settings, pg_dsn
) -> None:
    # First ingest: both tables have a full delta.
    orchestrator.run(dry_run=False)
    case_share = settings.share_dir / "cases" / "changes.jsonl"
    case_share_before = case_share.read_bytes()
    case_share_mtime_before = case_share.stat().st_mtime_ns

    # Mutate ONLY customers (insert a new row with a fresh updated_at).
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO customers (name, email, country, updated_at) "
                "VALUES ('Customer 099', 'c099@example.com', 'US', "
                "TIMESTAMPTZ '2026-04-20T10:00:00Z')"
            )

    m = orchestrator.run(dry_run=False)
    by_table = {t.table: t for t in m.tables}

    # Cases: zero delta -> share_path is null, lake_paths empty.
    assert by_table["cases"].delta_row_count == 0
    assert by_table["cases"].share_path is None
    assert by_table["cases"].lake_paths == []
    # Customers: one row picked up.
    assert by_table["customers"].delta_row_count == 1
    assert by_table["customers"].share_path is not None

    # The cases share file is untouched: byte-for-byte identical.
    assert case_share.read_bytes() == case_share_before
    assert case_share.stat().st_mtime_ns == case_share_mtime_before

    # Durable event for cases still exists with delta_row_count=0 / share_path null.
    events = [json.loads(l) for l in (settings.events_dir / f"{m.run_id}.jsonl").read_text().splitlines() if l]
    case_event = next(e for e in events if e["table"] == "cases")
    assert case_event["delta_row_count"] == 0
    assert case_event["share_path"] is None
    assert case_event["lake_paths"] == []
