"""End-to-end: first ingest + replay (no-op) idempotency."""

from __future__ import annotations

import json
from pathlib import Path


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text().splitlines() if l]


def test_first_ingest_pulls_all_seed_rows(orchestrator, settings) -> None:
    m = orchestrator.run(dry_run=False)

    # Manifest shape.
    assert m.dry_run is False
    assert isinstance(m.run_id, str) and len(m.run_id) >= 8
    by_table = {t.table: t for t in m.tables}
    assert by_table["customers"].delta_row_count == 30
    assert by_table["cases"].delta_row_count == 200

    # Lake files exist for both tables, partitioned by UTC date.
    assert any(settings.lake_dir.rglob("data.jsonl"))
    cust_lake_files = sorted((settings.lake_dir / "customers").rglob("data.jsonl"))
    case_lake_files = sorted((settings.lake_dir / "cases").rglob("data.jsonl"))
    assert cust_lake_files and case_lake_files
    # Total rows across all customer partitions = 30; cases = 200.
    assert sum(len(_read_jsonl(p)) for p in cust_lake_files) == 30
    assert sum(len(_read_jsonl(p)) for p in case_lake_files) == 200

    # Share files exist with full delta.
    cust_share = settings.share_dir / "customers" / "changes.jsonl"
    case_share = settings.share_dir / "cases" / "changes.jsonl"
    assert len(_read_jsonl(cust_share)) == 30
    assert len(_read_jsonl(case_share)) == 200

    # Events file written under run_id.
    events_path = settings.events_dir / f"{m.run_id}.jsonl"
    events = _read_jsonl(events_path)
    assert len(events) == 2
    assert {e["table"] for e in events} == {"customers", "cases"}

    # Checkpoint advanced to (last_updated_at, last_pk) per table.
    cp = json.loads(settings.checkpoint_path.read_text())
    assert cp["customers"]["last_pk"] == 30
    assert cp["cases"]["last_pk"] == 200

    # Run staging dir cleaned up.
    assert not (settings.state_dir / "runs").exists() or not any(
        (settings.state_dir / "runs").iterdir()
    )


def test_replay_with_no_changes_is_no_op(orchestrator, settings) -> None:
    """Two consecutive runs with no DB changes: 2nd is a true no-op.

    Per spec:
      - lake unchanged (no duplicates).
      - share files NOT modified for zero-delta tables.
      - events still emitted (delta_row_count = 0 per table).
      - checkpoint unchanged.
    """
    m1 = orchestrator.run(dry_run=False)
    cust_lake_before = (settings.lake_dir / "customers").rglob("data.jsonl")
    snapshot_lake = {str(p): p.read_bytes() for p in cust_lake_before}
    cust_share = settings.share_dir / "customers" / "changes.jsonl"
    case_share = settings.share_dir / "cases" / "changes.jsonl"
    cust_share_before = cust_share.read_bytes()
    case_share_before = case_share.read_bytes()
    cp_before = settings.checkpoint_path.read_bytes()

    m2 = orchestrator.run(dry_run=False)

    # Zero-delta in the manifest.
    by_table = {t.table: t for t in m2.tables}
    assert by_table["customers"].delta_row_count == 0
    assert by_table["cases"].delta_row_count == 0
    # share_path null per spec for zero-delta tables.
    assert by_table["customers"].share_path is None
    assert by_table["cases"].share_path is None
    # lake_paths empty list per spec for zero-delta tables.
    assert by_table["customers"].lake_paths == []
    assert by_table["cases"].lake_paths == []

    # Lake files: byte-identical to before (no duplication).
    for path_str, content_before in snapshot_lake.items():
        assert Path(path_str).read_bytes() == content_before

    # Share files: untouched per spec ("do not modify the existing share artifact").
    assert cust_share.read_bytes() == cust_share_before
    assert case_share.read_bytes() == case_share_before

    # Checkpoint unchanged.
    assert settings.checkpoint_path.read_bytes() == cp_before

    # An events file for the no-op run is still emitted (under a different run_id
    # because run_id is deterministic from inputs, but inputs are the same -- so
    # actually run_id is the SAME and the events file is overwritten with
    # byte-identical content).
    assert m1.run_id == m2.run_id
    events = _read_jsonl(settings.events_dir / f"{m2.run_id}.jsonl")
    assert all(e["delta_row_count"] == 0 for e in events)
