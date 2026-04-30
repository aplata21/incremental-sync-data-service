"""End-to-end: dry_run=true must compute manifest but write nothing."""

from __future__ import annotations


def test_dry_run_writes_nothing(orchestrator, settings) -> None:
    m = orchestrator.run(dry_run=True)
    assert m.dry_run is True

    # Predicted counts match what a real run would produce.
    by_table = {t.table: t for t in m.tables}
    assert by_table["customers"].delta_row_count == 30
    assert by_table["cases"].delta_row_count == 200

    # Predicted paths included (informational).
    assert by_table["customers"].lake_paths
    assert by_table["customers"].share_path is not None

    # NO files written anywhere.
    assert not settings.checkpoint_path.exists()
    assert not settings.lake_dir.exists() or not any(settings.lake_dir.rglob("*"))
    assert not settings.share_dir.exists() or not any(settings.share_dir.rglob("*"))
    assert not settings.events_dir.exists() or not any(settings.events_dir.rglob("*"))

    # No staging dirs left behind.
    runs = settings.state_dir / "runs"
    assert not runs.exists() or not any(runs.iterdir())


def test_dry_run_after_real_run_predicts_zero_delta(orchestrator, settings) -> None:
    m1 = orchestrator.run(dry_run=False)
    m2 = orchestrator.run(dry_run=True)
    by_table = {t.table: t for t in m2.tables}
    assert by_table["customers"].delta_row_count == 0
    assert by_table["cases"].delta_row_count == 0
    # checkpoint_after equals checkpoint_before for a dry-run no-op.
    assert m2.checkpoint_after == m2.checkpoint_before
    # And on-disk checkpoint is the post-real-run state, untouched by dry run.
    assert m2.checkpoint_before == m1.checkpoint_after
