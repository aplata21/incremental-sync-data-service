"""Pure helpers for checkpoint advancement and run_id input shaping.

Kept in their own module so they can be unit-tested without pulling in the
HTTP / settings / Pydantic surface that the orchestrator depends on. The
orchestrator imports these; the helpers do not import the orchestrator.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ..domain.checkpoint import Checkpoint, TableWatermark
from ..domain.tables import TableSpec
from .run_id import RowIdentity


def advance_checkpoint(
    checkpoint_before: Checkpoint,
    deltas: Mapping[str, Sequence[Mapping[str, Any]]],
    table_specs: Mapping[str, TableSpec],
) -> Checkpoint:
    """Advance per-table watermarks to the last row of each non-empty delta.

    For empty deltas, the existing watermark is preserved. The repository
    yields rows in ``(updated_at ASC, pk ASC)`` order, so the last row of
    a non-empty delta is the maximum (updated_at, pk) tuple by definition,
    which is exactly what the next run's predicate must skip past.
    """
    cp = checkpoint_before
    for name, rows in deltas.items():
        if not rows:
            continue
        spec = table_specs[name]
        last = rows[-1]
        cp = cp.with_updated(
            name,
            TableWatermark(
                updated_at=last["updated_at"],
                last_pk=int(last[spec.primary_key]),
            ),
        )
    return cp


def build_row_identities(
    deltas: Mapping[str, Sequence[Mapping[str, Any]]],
    table_specs: Mapping[str, TableSpec],
) -> dict[str, list[RowIdentity]]:
    """Derive run_id input from raw delta rows, preserving their order."""
    return {
        name: [
            RowIdentity(
                table=name,
                pk=int(row[table_specs[name].primary_key]),
                updated_at=row["updated_at"],
            )
            for row in rows
        ]
        for name, rows in deltas.items()
    }
