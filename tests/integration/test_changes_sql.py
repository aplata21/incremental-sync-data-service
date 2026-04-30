"""End-to-end: db/changes.sql produces an exact incremental delta."""

from __future__ import annotations

import json
from pathlib import Path


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text().splitlines() if l]


def test_changes_sql_produces_expected_delta(orchestrator, settings, apply_changes_sql) -> None:
    # First ingest pulls the seed.
    orchestrator.run(dry_run=False)

    # Apply changes.sql: 5 case updates + 2 customer inserts + 10 case inserts.
    apply_changes_sql()

    m = orchestrator.run(dry_run=False)
    by_table = {t.table: t for t in m.tables}
    assert by_table["customers"].delta_row_count == 2  # 2 new customers
    assert by_table["cases"].delta_row_count == 15  # 5 updates + 10 inserts

    # New lake partition for 2026-04-15 (the change anchor).
    new_cust_part = settings.lake_dir / "customers" / "date=2026-04-15" / "data.jsonl"
    new_case_part = settings.lake_dir / "cases" / "date=2026-04-15" / "data.jsonl"
    assert new_cust_part.exists() and new_case_part.exists()
    # Customers partition existed only after the change (seed didn't touch this date)
    # so it has exactly 2 rows.
    assert len(_read_jsonl(new_cust_part)) == 2

    # Share files are REPLACED (not appended) and contain only the latest batch.
    cust_share = _read_jsonl(settings.share_dir / "customers" / "changes.jsonl")
    case_share = _read_jsonl(settings.share_dir / "cases" / "changes.jsonl")
    assert len(cust_share) == 2
    assert len(case_share) == 15
    # All records in this batch share the change_anchor timestamp.
    assert {r["updated_at"] for r in cust_share} == {"2026-04-15T12:00:00Z"}
    assert {r["updated_at"] for r in case_share} == {"2026-04-15T12:00:00Z"}

    # Records are ordered by (updated_at, pk). All share the same updated_at,
    # so PK order alone is the deterministic tiebreaker.
    assert [r["case_id"] for r in case_share] == sorted(r["case_id"] for r in case_share)
    assert [r["customer_id"] for r in cust_share] == sorted(r["customer_id"] for r in cust_share)

    # Checkpoint advanced to the change anchor + last PKs.
    cp = json.loads(settings.checkpoint_path.read_text())
    assert cp["customers"]["updated_at"] == "2026-04-15T12:00:00Z"
    assert cp["customers"]["last_pk"] == 32
    assert cp["cases"]["updated_at"] == "2026-04-15T12:00:00Z"
    assert cp["cases"]["last_pk"] == 210
