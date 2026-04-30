"""Commit phase + resume.

This module is the *only* place that converts a staged run into the live
filesystem state. Two entry points:

    Committer.commit(staging)
        Execute the run's commit plan top-to-bottom. Each step is
        ``staged -> live`` via ``utils.fs.atomic_rename``. Last step is
        the checkpoint -- which is the actual commit point.

    Committer.resume_pending(runs_root)
        Scan ``./state/runs/`` on startup (and at the start of every
        /ingest call). For each leftover run dir:
          * READY missing  -> abandoned partial stage, delete it.
          * READY present  -> a stage-complete run that may have been
            interrupted mid-commit. Re-execute the plan; idempotent
            steps (already-renamed files) are skipped.

Crash invariants this preserves
    1. The checkpoint is the *last* rename. If we crash before it, the
       checkpoint has not advanced. If we crash after it, every prior
       rename necessarily completed.
    2. Each individual rename is atomic (``os.replace``). There is no
       intermediate state where a single live file is half-written.
    3. The staged content for every step is byte-identical to what the
       final live state should be (deterministic from
       ``checkpoint_before`` + immutable source). Resuming a step is
       just re-running its rename; we never re-merge or re-derive.
    4. ``staged_path`` going missing is the signal that "this step's
       rename already happened in a prior attempt" -- we skip it.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Iterable
from pathlib import Path

from ..utils.fs import atomic_rename
from .staging import CommitStep, RunStagingDir


class Committer:
    """Plan executor + crash-resume scanner."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or logging.getLogger(__name__)

    # -------------------------------------------------------- fresh commit

    def commit(self, staging: RunStagingDir) -> None:
        """Execute the run's commit plan top-to-bottom, then clean up.

        Caller must have already:
            1. Staged all final-form files into ``staging``.
            2. Written ``commit_plan.json`` via ``staging.write_commit_plan``.
            3. Atomically marked ``READY`` via ``staging.mark_ready``.

        Crashes between any two steps are recoverable by ``resume_pending``.
        """
        if not staging.is_ready():
            raise RuntimeError(
                f"refusing to commit run {staging.run_id}: READY marker missing"
            )
        plan = staging.load_commit_plan()
        self._execute_steps(plan.steps)
        staging.cleanup()

    # ----------------------------------------------------------- resume

    def resume_pending(self, runs_root: Path) -> list[str]:
        """Scan ``runs_root`` and bring it back to a clean state.

        Returns the list of run_ids that were touched (committed or
        discarded), in deterministic order. Idempotent: calling twice
        does nothing the second time because the dirs were removed.
        """
        if not runs_root.exists():
            return []

        processed: list[str] = []
        # sorted() so behavior is deterministic across filesystems with
        # different iteration orders.
        for run_dir in sorted(p for p in runs_root.iterdir() if p.is_dir()):
            staging = RunStagingDir(runs_root, run_dir.name)
            if staging.is_ready():
                self._log.info(
                    "resume: completing pending commit for run %s", run_dir.name
                )
                # If commit plan is missing or unreadable, this raises -- which
                # is the right behavior: a READY-marked dir without a plan is
                # corrupt and needs operator attention, not silent drop.
                plan = staging.load_commit_plan()
                self._execute_steps(plan.steps)
                staging.cleanup()
            else:
                self._log.info(
                    "resume: discarding partial-stage run %s", run_dir.name
                )
                shutil.rmtree(staging.root)
            processed.append(run_dir.name)

        return processed

    # ------------------------------------------------------- step execution

    def _execute_steps(self, steps: Iterable[CommitStep]) -> None:
        for step in steps:
            self._execute_step(step)

    def _execute_step(self, step: CommitStep) -> None:
        # Idempotency rule: if the staged file no longer exists, the rename
        # already succeeded in a prior attempt. Skip; do not re-derive.
        if not step.staged_path.exists():
            self._log.debug("commit: %s -- skipped (already done)", step.label)
            return
        atomic_rename(step.staged_path, step.live_path)
        self._log.info("commit: %s -- %s", step.label, step.live_path)
