"""End-to-end ingest orchestration.

This is the only place that knows the full shape of an ingest run. Every
upstream module (repository, writers, checkpoint store, committer) has a
narrow, single-purpose interface; the orchestrator composes them in the
correct order to satisfy the spec.

A run looks like:

    1.  Acquire the single-flight lock (409 on contention).
    2.  Resume any leftover crashed runs in ./state/runs/.
    3.  Load checkpoint_before. (Missing file -> initial empty watermark.)
    4.  Open one Postgres read session (REPEATABLE READ, READ ONLY) so
        both tables read from the same snapshot.
    5.  Fetch deltas in deterministic order per table.
    6.  Compute run_id (deterministic from checkpoint_before + ordered
        row identities) and per-table schema_fingerprint.
    7.  Compute checkpoint_after by advancing each non-empty table's
        watermark to its delta's last row.
    8.  *If dry_run:* build manifest from predictions, return. No writes.
    9.  Otherwise stage every output (lake, share, events, checkpoint),
        write the commit plan, mark READY, commit, emit stdout events.
    10. Return manifest with actual paths (= predictions, by construction).

The "predictions match actuals by construction" property is critical to
the dry-run contract: it lets the same code path compute lake_paths /
share_path for both the dry-run and real-run manifests, so they cannot
drift.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..core.clock import Clock
from ..core.config import Settings
from ..domain.checkpoint import Checkpoint, TableWatermark
from ..domain.events import Event
from ..domain.manifest import Manifest, TableManifest
from ..domain.tables import TableSpec
from ..outputs.event_writer import EventWriter
from ..outputs.lake_writer import LakeWriter
from ..outputs.share_writer import ShareWriter
from ..source.repository import IncrementalSourceRepository
from ..state.checkpoint_store import CheckpointStore
from .checkpoint_advance import advance_checkpoint, build_row_identities
from .commit import Committer
from .fingerprint import compute_schema_fingerprint
from .lock import acquire_ingest_lock
from .run_id import compute_run_id
from .staging import CommitPlan, CommitStep, RunStagingDir

# Re-export the pure helpers so callers can keep importing from this module.
__all__ = [
    "IngestRunOrchestrator",
    "advance_checkpoint",
    "build_row_identities",
]


# ----------------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------------


class IngestRunOrchestrator:
    """One method, one ingest run."""

    def __init__(
        self,
        *,
        settings: Settings,
        repository: IncrementalSourceRepository,
        checkpoint_store: CheckpointStore,
        lake_writer: LakeWriter,
        share_writer: ShareWriter,
        event_writer: EventWriter,
        committer: Committer,
        clock: Clock,
        table_specs: Sequence[TableSpec],
        run_id_hex_prefix_len: int = 32,
        logger: logging.Logger | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._checkpoint_store = checkpoint_store
        self._lake_writer = lake_writer
        self._share_writer = share_writer
        self._event_writer = event_writer
        self._committer = committer
        self._clock = clock
        self._table_specs = tuple(table_specs)
        self._spec_by_name = {t.name: t for t in self._table_specs}
        self._run_id_hex_prefix_len = run_id_hex_prefix_len
        self._log = logger or logging.getLogger(__name__)

    # --------------------------------------------------- startup-time hook

    def startup_resume(self) -> list[str]:
        """Bring ./state/runs/ to a clean state at process startup.

        Called once during FastAPI lifespan. Equivalent to the resume sweep
        the orchestrator does at the start of every /ingest call -- doing
        it here too means a freshly started service is in a known-good
        state without waiting for the first request.
        """
        return self._committer.resume_pending(self._settings.runs_dir)

    # ---------------------------------------------------------------- run

    def run(self, *, dry_run: bool) -> Manifest:
        with acquire_ingest_lock(self._settings.ingest_lock_path):
            return self._run_locked(dry_run=dry_run)

    def _run_locked(self, *, dry_run: bool) -> Manifest:
        # 1. Resume any leftover crashed runs first. After this call,
        #    state/runs/ is empty and the live filesystem state matches
        #    whatever the last completed run committed (or didn't).
        self._committer.resume_pending(self._settings.runs_dir)

        # 2. Snapshot context.
        started_at = self._clock.now_utc()
        cp_before = self._checkpoint_store.load()

        # 3. Fetch deltas. Both tables read from the same snapshot.
        deltas = self._fetch_deltas(cp_before)

        # 4. Deterministic identities.
        run_id = compute_run_id(
            checkpoint_before=cp_before.to_dict(),
            table_deltas=build_row_identities(deltas, self._spec_by_name),
            hex_prefix_len=self._run_id_hex_prefix_len,
        )
        cp_after = advance_checkpoint(cp_before, deltas, self._spec_by_name)
        fingerprints = {
            spec.name: compute_schema_fingerprint(spec) for spec in self._table_specs
        }

        # 5. Path predictions used by *both* branches.
        predicted_lake_paths: dict[str, list[Path]] = {
            spec.name: self._lake_writer.predict_partitions(spec, deltas[spec.name])
            for spec in self._table_specs
        }
        predicted_share_path: dict[str, Path | None] = {
            spec.name: self._share_writer.predict_share_path(spec, deltas[spec.name])
            for spec in self._table_specs
        }

        # 6a. Dry run: build manifest, return. No staging, no commit.
        if dry_run:
            finished_at = self._clock.now_utc()
            self._log.info(
                "ingest dry_run=true run_id=%s deltas=%s",
                run_id,
                {n: len(r) for n, r in deltas.items()},
            )
            return self._build_manifest(
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                dry_run=True,
                cp_before=cp_before,
                cp_after=cp_after,
                deltas=deltas,
                fingerprints=fingerprints,
                lake_paths=predicted_lake_paths,
                share_path=predicted_share_path,
            )

        # 6b. Real run: stage everything into the run dir.
        staging = RunStagingDir(self._settings.runs_dir, run_id)
        staging.ensure_layout()

        commit_steps = self._stage_outputs(
            staging=staging,
            run_id=run_id,
            deltas=deltas,
            fingerprints=fingerprints,
            cp_after=cp_after,
            predicted_lake_paths=predicted_lake_paths,
            predicted_share_path=predicted_share_path,
        )

        # 7. Plan + READY = commit gate.
        staging.write_commit_plan(CommitPlan(steps=tuple(commit_steps)))
        staging.mark_ready()

        # 8. Execute the commit. After this returns, the live filesystem
        #    reflects the new run and ./state/runs/<run_id>/ is gone.
        self._committer.commit(staging)

        # 9. Best-effort stdout event mirror. Per spec, this is OUTSIDE
        #    the crash-consistency boundary -- the run is already
        #    committed regardless of what happens here.
        self._emit_stdout_events(
            run_id=run_id,
            deltas=deltas,
            fingerprints=fingerprints,
            cp_after=cp_after,
            lake_paths=predicted_lake_paths,
            share_path=predicted_share_path,
        )

        finished_at = self._clock.now_utc()
        self._log.info(
            "ingest dry_run=false run_id=%s deltas=%s",
            run_id,
            {n: len(r) for n, r in deltas.items()},
        )
        return self._build_manifest(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            dry_run=False,
            cp_before=cp_before,
            cp_after=cp_after,
            deltas=deltas,
            fingerprints=fingerprints,
            lake_paths=predicted_lake_paths,
            share_path=predicted_share_path,
        )

    # ------------------------------------------------- fetch / stage helpers

    def _fetch_deltas(
        self, cp_before: Checkpoint
    ) -> dict[str, list[dict[str, Any]]]:
        deltas: dict[str, list[dict[str, Any]]] = {}
        with self._repository.read_session() as reader:
            for spec in self._table_specs:
                rows = list(
                    reader.fetch_delta(spec, cp_before.watermark(spec.name))
                )
                deltas[spec.name] = rows
        return deltas

    def _stage_outputs(
        self,
        *,
        staging: RunStagingDir,
        run_id: str,
        deltas: Mapping[str, Sequence[Mapping[str, Any]]],
        fingerprints: Mapping[str, str],
        cp_after: Checkpoint,
        predicted_lake_paths: Mapping[str, Sequence[Path]],
        predicted_share_path: Mapping[str, Path | None],
    ) -> list[CommitStep]:
        """Stage all output files; return the ordered commit plan steps."""
        steps: list[CommitStep] = []

        # ---- Lake first (earliest in commit order).
        for spec in self._table_specs:
            partitions = self._lake_writer.stage_partitions(
                spec, deltas[spec.name], staging.lake_dir
            )
            for part in partitions:
                steps.append(
                    CommitStep(
                        staged_path=part.staged_path,
                        live_path=part.live_path,
                        label=f"lake:{spec.name}:{part.date_key}",
                    )
                )

        # ---- Share next.
        for spec in self._table_specs:
            staged_share = self._share_writer.stage_share(
                table=spec,
                rows=deltas[spec.name],
                run_id=run_id,
                schema_fingerprint=fingerprints[spec.name],
                checkpoint_after=cp_after.to_dict(),
                run_share_staging=staging.share_dir,
            )
            if staged_share is not None:
                steps.append(
                    CommitStep(
                        staged_path=staged_share.staged_path,
                        live_path=staged_share.live_path,
                        label=f"share:{spec.name}",
                    )
                )

        # ---- Events (always written, even for zero-delta tables).
        events = self._build_events(
            run_id=run_id,
            deltas=deltas,
            fingerprints=fingerprints,
            cp_after=cp_after,
            lake_paths=predicted_lake_paths,
            share_path=predicted_share_path,
        )
        staged_events = self._event_writer.stage_events(
            events=events,
            run_id=run_id,
            run_events_staging=staging.events_dir,
        )
        steps.append(
            CommitStep(
                staged_path=staged_events.staged_path,
                live_path=staged_events.live_path,
                label="events",
            )
        )

        # ---- Checkpoint LAST. This is the actual commit point.
        self._checkpoint_store.stage(cp_after, staging.checkpoint_path)
        steps.append(
            CommitStep(
                staged_path=staging.checkpoint_path,
                live_path=self._checkpoint_store.path,
                label="checkpoint",
            )
        )

        return steps

    # --------------------------------------------------- event/manifest build

    def _build_events(
        self,
        *,
        run_id: str,
        deltas: Mapping[str, Sequence[Mapping[str, Any]]],
        fingerprints: Mapping[str, str],
        cp_after: Checkpoint,
        lake_paths: Mapping[str, Sequence[Path]],
        share_path: Mapping[str, Path | None],
    ) -> list[Event]:
        out: list[Event] = []
        cp_after_dict = cp_after.to_dict()
        for spec in self._table_specs:
            sp = share_path[spec.name]
            out.append(
                Event(
                    table=spec.name,
                    run_id=run_id,
                    schema_fingerprint=fingerprints[spec.name],
                    delta_row_count=len(deltas[spec.name]),
                    lake_paths=[str(p) for p in lake_paths[spec.name]],
                    share_path=str(sp) if sp is not None else None,
                    checkpoint_after=cp_after_dict,
                )
            )
        return out

    def _emit_stdout_events(
        self,
        *,
        run_id: str,
        deltas: Mapping[str, Sequence[Mapping[str, Any]]],
        fingerprints: Mapping[str, str],
        cp_after: Checkpoint,
        lake_paths: Mapping[str, Sequence[Path]],
        share_path: Mapping[str, Path | None],
    ) -> None:
        events = self._build_events(
            run_id=run_id,
            deltas=deltas,
            fingerprints=fingerprints,
            cp_after=cp_after,
            lake_paths=lake_paths,
            share_path=share_path,
        )
        self._event_writer.emit_to_stdout(events)

    def _build_manifest(
        self,
        *,
        run_id: str,
        started_at,
        finished_at,
        dry_run: bool,
        cp_before: Checkpoint,
        cp_after: Checkpoint,
        deltas: Mapping[str, Sequence[Mapping[str, Any]]],
        fingerprints: Mapping[str, str],
        lake_paths: Mapping[str, Sequence[Path]],
        share_path: Mapping[str, Path | None],
    ) -> Manifest:
        tables = []
        for spec in self._table_specs:
            sp = share_path[spec.name]
            tables.append(
                TableManifest(
                    table=spec.name,
                    delta_row_count=len(deltas[spec.name]),
                    lake_paths=[str(p) for p in lake_paths[spec.name]],
                    share_path=str(sp) if sp is not None else None,
                    schema_fingerprint=fingerprints[spec.name],
                )
            )
        return Manifest(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            dry_run=dry_run,
            checkpoint_before=cp_before.to_dict(),
            checkpoint_after=cp_after.to_dict(),
            tables=tables,
        )
