"""Run staging directory layout.

The staging dir is the *journal* that makes the commit phase crash-consistent.
Layout (under ``./state/runs/<run_id>/``):

    lake/<table>/date=YYYY-MM-DD/data.jsonl     # one per touched partition
    share/<table>/changes.jsonl                  # one per non-zero-delta table
    events/<run_id>.jsonl                        # always present after stage
    checkpoint.json                              # the post-run checkpoint
    commit_plan.json                             # ordered rename plan
    READY                                        # marker; written LAST

The READY marker is the linchpin: its existence means "every staged file is
durably written and the commit plan is on disk; this run is committable".
The commit phase reads the plan and executes ordered renames; the resume
mechanism uses the marker to distinguish "completable" runs from
"abandoned half-staged" runs.

Why a separate ``commit_plan.json`` instead of deriving the plan from
walking the staged tree? Two reasons:

  1. Determinism. The plan is computed once during stage, written once,
     and replayed verbatim during commit and resume. Walking the tree at
     resume time risks reordering or rediscovery bugs as the layout evolves.
  2. Auditability. ``cat ./state/runs/<run_id>/commit_plan.json`` shows
     exactly what would (or did) happen during commit, in order.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from ..utils.fs import ensure_dir, write_bytes_atomic
from ..utils.jsonio import pretty_json_bytes


@dataclass(frozen=True)
class CommitStep:
    """One staged-to-live rename, with a label for logs/inspection."""

    staged_path: Path
    live_path: Path
    label: str  # e.g. "lake:cases:2026-04-15", "share:customers", "events", "checkpoint"

    def to_dict(self) -> dict[str, str]:
        return {
            "staged_path": str(self.staged_path),
            "live_path": str(self.live_path),
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, d: dict[str, str]) -> CommitStep:
        return cls(
            staged_path=Path(d["staged_path"]),
            live_path=Path(d["live_path"]),
            label=d["label"],
        )


@dataclass(frozen=True)
class CommitPlan:
    """Ordered list of rename steps.

    Order is *significant* and forms the commit contract:

        1. lake partitions (any order among themselves; sorted for reproducibility)
        2. share files (one per non-zero-delta table)
        3. events file
        4. checkpoint  <-- the actual commit point

    The orchestrator constructs the plan; the committer executes it.
    """

    steps: tuple[CommitStep, ...]

    def to_dict(self) -> dict[str, list[dict[str, str]]]:
        return {"steps": [s.to_dict() for s in self.steps]}

    @classmethod
    def from_dict(cls, d: dict) -> CommitPlan:
        steps = tuple(CommitStep.from_dict(s) for s in d.get("steps", []))
        return cls(steps=steps)


class RunStagingDir:
    """Path layout + small write helpers for one run's staging dir."""

    def __init__(self, runs_root: Path, run_id: str) -> None:
        if not run_id:
            raise ValueError("run_id must be non-empty")
        self._runs_root = runs_root
        self._run_id = run_id
        self._root = (runs_root / run_id).resolve(strict=False)

    # ------------------------------------------------------- path accessors

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def root(self) -> Path:
        return self._root

    @property
    def lake_dir(self) -> Path:
        return self._root / "lake"

    @property
    def share_dir(self) -> Path:
        return self._root / "share"

    @property
    def events_dir(self) -> Path:
        return self._root / "events"

    @property
    def checkpoint_path(self) -> Path:
        return self._root / "checkpoint.json"

    @property
    def commit_plan_path(self) -> Path:
        return self._root / "commit_plan.json"

    @property
    def ready_marker_path(self) -> Path:
        return self._root / "READY"

    # ------------------------------------------------------- layout helpers

    def ensure_layout(self) -> None:
        """Create the run staging dir and its known sub-roots.

        Idempotent; safe to call before any writer runs.
        """
        ensure_dir(self.root)
        ensure_dir(self.lake_dir)
        ensure_dir(self.share_dir)
        ensure_dir(self.events_dir)

    # --------------------------------------------------- commit-plan helpers

    def write_commit_plan(self, plan: CommitPlan) -> None:
        """Durably write the commit plan to disk. Atomic."""
        write_bytes_atomic(self.commit_plan_path, pretty_json_bytes(plan.to_dict()))

    def load_commit_plan(self) -> CommitPlan:
        raw = self.commit_plan_path.read_bytes()
        return CommitPlan.from_dict(json.loads(raw))

    # ---------------------------------------------------- ready/done lifecycle

    def is_ready(self) -> bool:
        """True iff the READY marker exists.

        The marker's *existence* is the gate that resume uses to tell
        "stage completed, commit was started or pending" apart from
        "stage was abandoned mid-flight".
        """
        return self.ready_marker_path.exists()

    def mark_ready(self) -> None:
        """Atomically write the READY marker.

        Call this *after* every staged file and the commit plan are
        durably on disk. The body of the marker is informational only;
        existence is what counts.
        """
        write_bytes_atomic(self.ready_marker_path, b"READY\n")

    def cleanup(self) -> None:
        """Remove the run dir after a successful commit (or after
        deciding an abandoned partial stage should be discarded).

        Intentionally tolerant: missing dir is fine.
        """
        if self.root.exists():
            shutil.rmtree(self.root)
