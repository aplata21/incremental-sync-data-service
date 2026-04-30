"""Consumer-facing share artifact writer.

Output (mandated by spec):
    ./share/<table>/changes.jsonl

Semantics
    The share file represents the *latest* successful incremental batch
    -- a snapshot, not a log. Each successful non-zero-delta run REPLACES
    the file. There is no append.

Determinism
    "Given the same source data and the same checkpoint, the share
    artifact contents and record order must be byte-for-byte identical
    across repeated successful runs." We achieve this by:
        - building each record from a deterministic dict shape,
        - serializing every datetime through ``iso_utc_z`` (single
          canonical format),
        - using ``canonical_json_line`` (sorted keys, tightest
          separators, ensure_ascii=False),
        - emitting records in the deterministic ``(updated_at, pk)``
          order the repository already produces.

Zero-delta rule
    "If a successful non-dry run has delta_row_count = 0 for a table:
        - do not create a new share artifact for that table
        - do not modify the existing share artifact for that table"
    We honor this by simply returning ``None`` when the row list is
    empty. The orchestrator interprets ``None`` as "no share file
    staged" and reports ``share_path = null`` in the manifest/event.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..domain.tables import TableSpec
from ..utils.fs import write_bytes_atomic
from ..utils.jsonio import canonical_json_line, iso_utc_z
from .staged_file import StagedFile


@dataclass(frozen=True)
class StagedShareFile(StagedFile):
    table: str
    row_count: int


class ShareWriter:
    """Stages a share artifact file for one ingest run."""

    def __init__(self, share_root: Path) -> None:
        self._share_root = share_root

    def predict_share_path(
        self, table: TableSpec, rows: Sequence[Mapping[str, Any]]
    ) -> Path | None:
        """Return the live share path for a non-zero-delta table, else None.

        Mirrors the zero-delta rule from ``stage_share``. Used by the dry-run
        path and to populate Event ``share_path`` from a single source of
        truth.
        """
        if not rows:
            return None
        return self._share_root / table.share_subdir / "changes.jsonl"

    def stage_share(
        self,
        *,
        table: TableSpec,
        rows: Sequence[Mapping[str, Any]],
        run_id: str,
        schema_fingerprint: str,
        checkpoint_after: Mapping[str, Any],
        run_share_staging: Path,
    ) -> StagedShareFile | None:
        """Stage ``./<run>/share/<table>/changes.jsonl``.

        Returns ``None`` for zero-delta tables -- the spec's zero-delta
        rule prohibits creating or modifying the share artifact in that
        case.
        """
        if not rows:
            return None

        rel = Path(table.share_subdir) / "changes.jsonl"
        staged_path = run_share_staging / rel
        live_path = self._share_root / rel

        # Materialize all records up-front into a single bytes blob so the
        # subsequent atomic write is a single syscall sequence.
        buf = bytearray()
        for row in rows:
            record = self._build_record(
                table=table,
                row=row,
                run_id=run_id,
                schema_fingerprint=schema_fingerprint,
                checkpoint_after=dict(checkpoint_after),
            )
            buf.extend(canonical_json_line(record))

        write_bytes_atomic(staged_path, bytes(buf))

        return StagedShareFile(
            staged_path=staged_path,
            live_path=live_path,
            table=table.name,
            row_count=len(rows),
        )

    # -------------------------------------------------------------- helpers

    @staticmethod
    def _build_record(
        *,
        table: TableSpec,
        row: Mapping[str, Any],
        run_id: str,
        schema_fingerprint: str,
        checkpoint_after: dict[str, Any],
    ) -> dict[str, Any]:
        """Compose the share record dict.

        The record's leaf datetime values are pre-converted to ISO-Z
        strings here so the canonical encoder cannot drift between top
        level and nested ``record``. This is the single most important
        place to *not* rely on auto-serialization, because the spec
        demands byte-for-byte determinism for this exact file.
        """
        pk_value = row[table.primary_key]
        updated_at = row["updated_at"]
        if not isinstance(updated_at, datetime):
            raise ValueError(f"row updated_at must be datetime, got {type(updated_at).__name__}")

        record_payload: dict[str, Any] = {}
        for col in table.column_names:
            v = row[col]
            record_payload[col] = iso_utc_z(v) if isinstance(v, datetime) else v

        return {
            "table": table.name,
            "op": "upsert",
            table.primary_key: int(pk_value),
            "updated_at": iso_utc_z(updated_at),
            "run_id": run_id,
            "schema_fingerprint": schema_fingerprint,
            "checkpoint_after": checkpoint_after,
            "record": record_payload,
        }
