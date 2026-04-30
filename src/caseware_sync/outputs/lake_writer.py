"""Lake JSONL writer.

Output layout (mandated by spec):
    ./lake/<table>/date=YYYY-MM-DD/data.jsonl

The lake is an append-only CDC change log -- each successful non-dry run
*appends* its delta rows to the appropriate date partition(s). But naive
``open(... "a")`` is incompatible with crash-consistent commit:

    - if we crash after a partial write, the file is torn,
    - if we crash after a full write but before the checkpoint advances,
      the next run re-selects the same delta and would *re-append* the
      same rows, producing duplicates.

We solve both with the same single mechanism: every "append" is actually
**read-existing + concat-new + stage + atomic-replace**.

    1. The writer reads the current live partition's bytes (or empty if
       the partition does not yet exist).
    2. Concatenates canonical JSONL bytes for the new rows in order.
    3. Writes the resulting full content to a staging path under the
       run's staging dir.

The atomic rename happens later, in the commit phase. If the run crashes
*before* the commit phase, no live file changed. If the run crashes
*during* the commit phase, the resume mechanism re-runs the rename using
the staged content (which is byte-identical to what we want the final
state to be) without ever re-merging existing live content. That is what
breaks the duplication risk on resume.

Partitioning rule
    The UTC calendar date of each row's ``updated_at`` determines its
    partition. A single delta can touch multiple partitions; we group
    rows by date and stage one file per touched partition. Within a
    partition, rows preserve the deterministic ``(updated_at, pk)`` order
    that the repository already produced -- we do not re-sort.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..domain.tables import TableSpec
from ..utils.fs import write_bytes_atomic
from ..utils.jsonio import canonical_json_line, iso_utc_z
from .staged_file import StagedFile


@dataclass(frozen=True)
class StagedLakePartition(StagedFile):
    """A single staged date partition for a single table."""

    table: str
    date_key: str  # "YYYY-MM-DD"
    row_count: int


class LakeWriter:
    """Stages lake partition files for one ingest run."""

    def __init__(self, lake_root: Path) -> None:
        self._lake_root = lake_root

    # ------------------------------------------------------------------ API

    def predict_partitions(
        self,
        table: TableSpec,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[Path]:
        """Return the live partition file paths for a delta, *without* writing.

        Used by the dry-run path to produce ``lake_paths`` for the manifest,
        and by the orchestrator to populate Event ``lake_paths`` from a
        single source of truth that matches what ``stage_partitions`` will
        actually produce. Empty rows -> empty list (matches the spec's
        ``lake_paths = []`` rule for zero-delta tables).
        """
        if not rows:
            return []
        date_keys = sorted(self._group_by_utc_date(rows))
        return [
            self._lake_root / table.lake_subdir / f"date={d}" / "data.jsonl"
            for d in date_keys
        ]

    def stage_partitions(
        self,
        table: TableSpec,
        rows: Sequence[Mapping[str, Any]],
        run_lake_staging: Path,
    ) -> list[StagedLakePartition]:
        """Group rows by UTC date and stage merged content per partition.

        Args:
            table: spec for the source table.
            rows: ordered delta rows for this table; expected to be already
                sorted by ``(updated_at ASC, pk ASC)``.
            run_lake_staging: staging root for this run's lake outputs,
                e.g. ``./state/runs/<run_id>/lake/``.

        Returns:
            One ``StagedLakePartition`` per touched partition, in
            deterministic ``date_key`` order. Empty list when there are
            no rows (no-op honors the spec's "ensure a no-op run writes
            nothing" requirement).
        """
        if not rows:
            return []

        partitions = self._group_by_utc_date(rows)
        out: list[StagedLakePartition] = []

        # sorted() so the commit plan is deterministic across processes.
        for date_key in sorted(partitions):
            partition_rows = partitions[date_key]
            rel = Path(table.lake_subdir) / f"date={date_key}" / "data.jsonl"
            staged_path = run_lake_staging / rel
            live_path = self._lake_root / rel

            merged = self._build_merged_content(table, live_path, partition_rows)
            write_bytes_atomic(staged_path, merged)

            out.append(
                StagedLakePartition(
                    staged_path=staged_path,
                    live_path=live_path,
                    table=table.name,
                    date_key=date_key,
                    row_count=len(partition_rows),
                )
            )

        return out

    # -------------------------------------------------------------- helpers

    @staticmethod
    def _group_by_utc_date(
        rows: Sequence[Mapping[str, Any]],
    ) -> dict[str, list[Mapping[str, Any]]]:
        """Group rows by UTC calendar date of ``updated_at``.

        Insertion order within each list is preserved -- which means each
        partition's row list is already in ``(updated_at, pk)`` order
        because the input is.
        """
        out: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            ts = row.get("updated_at")
            if not isinstance(ts, datetime):
                raise ValueError(
                    f"row missing/invalid updated_at: {row!r}"
                )
            if ts.tzinfo is None:
                raise ValueError(
                    f"row updated_at must be timezone-aware: {row!r}"
                )
            date_key = ts.astimezone(timezone.utc).strftime("%Y-%m-%d")
            out.setdefault(date_key, []).append(row)
        return out

    def _build_merged_content(
        self,
        table: TableSpec,
        live_path: Path,
        new_rows: Sequence[Mapping[str, Any]],
    ) -> bytes:
        """Existing live partition bytes + canonical JSONL for new rows.

        We deliberately preserve the existing bytes verbatim rather than
        re-serializing them. That guarantees:
            - we never accidentally rewrite history (a prior run's
              records remain byte-identical, no formatting drift),
            - the operation is monotonic (each successful run can only
              add bytes, never modify earlier ones).
        """
        existing = live_path.read_bytes() if live_path.exists() else b""
        # Defensive: if a previous run somehow ended without a trailing
        # newline (e.g., the file was hand-edited), normalize to a clean
        # JSONL boundary before appending. Canonical writes always end
        # with '\n' so this branch is rare.
        if existing and not existing.endswith(b"\n"):
            existing = existing + b"\n"

        appended = bytearray(existing)
        for row in new_rows:
            appended.extend(canonical_json_line(self._row_to_jsonl(table, row)))
        return bytes(appended)

    @staticmethod
    def _row_to_jsonl(table: TableSpec, row: Mapping[str, Any]) -> dict[str, Any]:
        """Project to TableSpec column order and canonicalize datetime values.

        Field order in the JSON object is determined by ``canonical_json_line``
        (which sorts keys), not by this projection -- but projecting to
        the spec's column list keeps unknown DB columns from leaking into
        the lake by accident.
        """
        out: dict[str, Any] = {}
        for col in table.column_names:
            v = row[col]
            # canonical_json_line's encoder also handles datetime, but
            # serializing here makes the resulting dict purely
            # JSON-friendly so it round-trips cleanly through other
            # consumers (tests, debug printers).
            if isinstance(v, datetime):
                out[col] = iso_utc_z(v)
            else:
                out[col] = v
        return out
