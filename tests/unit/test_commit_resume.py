"""Commit + resume: every crash point along the timeline must recover."""

from __future__ import annotations

from pathlib import Path

import pytest

from caseware_sync.pipeline.commit import Committer
from caseware_sync.pipeline.staging import CommitPlan, CommitStep, RunStagingDir
from caseware_sync.utils.fs import atomic_rename, write_bytes_atomic


def _make_run(root: Path, run_id: str, *, with_ready: bool = True) -> tuple[RunStagingDir, list[CommitStep]]:
    """Build a complete staged run with N lake/share/events/checkpoint files."""
    runs_root = root / "state" / "runs"
    staging = RunStagingDir(runs_root, run_id)
    staging.ensure_layout()

    lake = root / "lake"
    share = root / "share"
    events = root / "events"
    ckpt = root / "state" / "checkpoint.json"
    steps: list[CommitStep] = []

    for tbl, dk, content in [
        ("customers", "2026-04-15", b'{"customer_id":31}\n'),
        ("cases", "2026-04-15", b'{"case_id":201}\n'),
        ("cases", "2026-04-16", b'{"case_id":210}\n'),
    ]:
        rel = Path(tbl) / f"date={dk}" / "data.jsonl"
        sp = staging.lake_dir / rel
        write_bytes_atomic(sp, content)
        steps.append(CommitStep(staged_path=sp, live_path=lake / rel, label=f"lake:{tbl}:{dk}"))

    for tbl, content in [("cases", b'{"share":1}\n'), ("customers", b'{"share":2}\n')]:
        rel = Path(tbl) / "changes.jsonl"
        sp = staging.share_dir / rel
        write_bytes_atomic(sp, content)
        steps.append(CommitStep(staged_path=sp, live_path=share / rel, label=f"share:{tbl}"))

    sp = staging.events_dir / f"{run_id}.jsonl"
    write_bytes_atomic(sp, b'{"event":1}\n')
    steps.append(CommitStep(staged_path=sp, live_path=events / f"{run_id}.jsonl", label="events"))

    write_bytes_atomic(staging.checkpoint_path, b'{"committed":true}\n')
    steps.append(CommitStep(staged_path=staging.checkpoint_path, live_path=ckpt, label="checkpoint"))

    staging.write_commit_plan(CommitPlan(steps=tuple(steps)))
    if with_ready:
        staging.mark_ready()
    return staging, steps


class TestHappyPath:
    def test_every_file_lands_dir_cleaned(self, tmp_path: Path) -> None:
        staging, steps = _make_run(tmp_path, "abc")
        Committer().commit(staging)
        for s in steps:
            assert s.live_path.exists()
            assert not s.staged_path.exists()
        assert not staging.root.exists()


class TestResume:
    def test_after_stage_before_any_rename(self, tmp_path: Path) -> None:
        staging, steps = _make_run(tmp_path, "abc")
        Committer().resume_pending(tmp_path / "state" / "runs")
        for s in steps:
            assert s.live_path.exists()
        assert not staging.root.exists()

    def test_mid_commit_some_lake_done(self, tmp_path: Path) -> None:
        staging, steps = _make_run(tmp_path, "abc")
        ckpt = tmp_path / "state" / "checkpoint.json"
        # Manually rename the first 3 (all lake), then "crash".
        for s in steps[:3]:
            atomic_rename(s.staged_path, s.live_path)
        assert not ckpt.exists()
        Committer().resume_pending(tmp_path / "state" / "runs")
        for s in steps:
            assert s.live_path.exists()
        assert ckpt.read_bytes() == b'{"committed":true}\n'
        assert not staging.root.exists()

    def test_post_rename_pre_cleanup(self, tmp_path: Path) -> None:
        staging, steps = _make_run(tmp_path, "abc")
        for s in steps:
            atomic_rename(s.staged_path, s.live_path)
        # Run dir still exists, READY marker still there, no staged files left.
        Committer().resume_pending(tmp_path / "state" / "runs")
        assert not staging.root.exists()

    def test_partial_stage_no_ready_discarded(self, tmp_path: Path) -> None:
        staging, steps = _make_run(tmp_path, "abc", with_ready=False)
        Committer().resume_pending(tmp_path / "state" / "runs")
        assert not staging.root.exists()
        for s in steps:
            assert not s.live_path.exists()

    def test_multiple_leftover_runs_sorted(self, tmp_path: Path) -> None:
        sa, sa_steps = _make_run(tmp_path, "run-aaa")
        sb, _ = _make_run(tmp_path, "run-bbb", with_ready=False)
        processed = Committer().resume_pending(tmp_path / "state" / "runs")
        assert processed == ["run-aaa", "run-bbb"]
        for s in sa_steps:
            assert s.live_path.exists()
        assert not sa.root.exists() and not sb.root.exists()

    def test_idempotent_on_clean_state(self, tmp_path: Path) -> None:
        runs = tmp_path / "state" / "runs"
        runs.mkdir(parents=True)
        assert Committer().resume_pending(runs) == []
        # Missing dir is also fine.
        assert Committer().resume_pending(tmp_path / "no-such") == []


class TestSafetyGuards:
    def test_commit_refuses_without_ready(self, tmp_path: Path) -> None:
        staging, _ = _make_run(tmp_path, "abc", with_ready=False)
        with pytest.raises(RuntimeError):
            Committer().commit(staging)


class TestCommitPlan:
    def test_json_round_trip(self) -> None:
        plan = CommitPlan(steps=(
            CommitStep(staged_path=Path("/a"), live_path=Path("/b"), label="x"),
            CommitStep(staged_path=Path("/c"), live_path=Path("/d"), label="y"),
        ))
        assert CommitPlan.from_dict(plan.to_dict()) == plan
