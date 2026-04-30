"""End-to-end: composite watermark must not skip rows that share updated_at.

The seed places case_id i and i+100 at the same ``updated_at`` for
i in 1..100. We simulate a partial first ingest by writing a checkpoint
that lands *between* a tied pair, then run /ingest and confirm the
remaining rows are pulled with no overlap.
"""

from __future__ import annotations

import json

from caseware_sync.utils.fs import write_bytes_atomic


def test_composite_watermark_handles_ties(orchestrator, settings) -> None:
    # Hand-construct a checkpoint that sits in the middle of a tied pair.
    # case_id 50 has updated_at = anchor + 49 * 7h12m.
    # case_id 150 has the SAME updated_at (i % 100 = 49 for i = 50 and 150).
    # If we set last_pk = 50 at this updated_at, the next run must pull 150
    # via the (updated_at = u AND pk > 50) branch -- and skip 50.
    from datetime import datetime, timezone, timedelta

    anchor = datetime(2026, 3, 1, tzinfo=timezone.utc)
    tied_ts = anchor + 49 * (timedelta(days=30) / 100)
    tied_iso = tied_ts.isoformat().replace("+00:00", "Z")

    cp = {
        "customers": {"updated_at": "0001-01-01T00:00:00Z", "last_pk": 0},
        "cases": {"updated_at": tied_iso, "last_pk": 50},
    }
    write_bytes_atomic(
        settings.checkpoint_path,
        (json.dumps(cp, indent=2) + "\n").encode(),
    )

    m = orchestrator.run(dry_run=False)
    case_table = next(t for t in m.tables if t.table == "cases")

    # Expected: every case row strictly above (tied_ts, 50) is pulled.
    # That's case_id 51..100 (still at tied_ts), then everything past tied_ts.
    # In the seed: 200 - 50 = 150 cases.
    assert case_table.delta_row_count == 150

    # Critically: case_id 50 must NOT appear in this batch (it's at the watermark itself).
    case_share = settings.share_dir / "cases" / "changes.jsonl"
    case_ids = [json.loads(l)["case_id"] for l in case_share.read_text().splitlines() if l]
    assert 50 not in case_ids
    # case_id 150 SHOULD appear -- it shares updated_at with 50 but pk > 50.
    assert 150 in case_ids
    # First record in the batch is case_id 51 (the next pk at the same updated_at).
    assert case_ids[0] == 51
