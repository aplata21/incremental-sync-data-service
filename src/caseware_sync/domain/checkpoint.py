"""Composite watermark types.

A checkpoint is *per-table* and uses the (updated_at, last_pk) pair so that
rows sharing an ``updated_at`` value cannot be skipped or revisited:

    next-run predicate (per table):
        updated_at > ckpt.updated_at
        OR (updated_at = ckpt.updated_at AND pk > ckpt.last_pk)

Initial checkpoint
    When ``./state/checkpoint.json`` is missing, the spec says "treat that as
    an initial empty watermark". We materialize this as
    ``(updated_at = 0001-01-01T00:00:00Z, last_pk = 0)`` so the predicate
    above is uniform on first run and never special-cased downstream.

Canonical dict shape
    ``to_dict()`` produces the exact JSON-friendly shape used both for the
    on-disk file and for hashing into ``run_id``. Keeping one canonical dict
    avoids drift between "what we hashed" and "what we wrote".
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..utils.jsonio import iso_utc_z, parse_iso_utc

# 0001-01-01T00:00:00Z; chosen so any real Postgres updated_at is strictly > this.
EMPTY_UPDATED_AT = datetime(1, 1, 1, tzinfo=timezone.utc)
EMPTY_LAST_PK = 0


@dataclass(frozen=True)
class TableWatermark:
    updated_at: datetime
    last_pk: int

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None:
            raise ValueError("watermark updated_at must be timezone-aware")
        if self.last_pk < 0:
            raise ValueError("last_pk must be >= 0")

    @classmethod
    def empty(cls) -> TableWatermark:
        return cls(updated_at=EMPTY_UPDATED_AT, last_pk=EMPTY_LAST_PK)

    def to_dict(self) -> dict[str, Any]:
        return {"updated_at": iso_utc_z(self.updated_at), "last_pk": self.last_pk}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> TableWatermark:
        return cls(
            updated_at=parse_iso_utc(str(d["updated_at"])),
            last_pk=int(d["last_pk"]),
        )


@dataclass(frozen=True)
class Checkpoint:
    """Whole-process checkpoint, keyed by table name.

    Insertion order in ``per_table`` is preserved (Python 3.7+ dict
    semantics), but ``to_dict()`` sorts table names so the on-disk and
    hash-input forms are independent of construction order.
    """

    per_table: Mapping[str, TableWatermark]

    @classmethod
    def initial(cls, table_names: Iterable[str]) -> Checkpoint:
        return cls(per_table={t: TableWatermark.empty() for t in table_names})

    def watermark(self, table: str) -> TableWatermark:
        wm = self.per_table.get(table)
        if wm is None:
            return TableWatermark.empty()
        return wm

    def with_updated(self, table: str, watermark: TableWatermark) -> Checkpoint:
        new = dict(self.per_table)
        new[table] = watermark
        return Checkpoint(per_table=new)

    def to_dict(self) -> dict[str, Any]:
        # sorted so the on-disk file and the run_id input are insertion-order independent
        return {name: self.per_table[name].to_dict() for name in sorted(self.per_table)}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Checkpoint:
        return cls(
            per_table={
                str(name): TableWatermark.from_dict(value) for name, value in d.items()
            }
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Checkpoint):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        # frozen dataclasses with a Mapping field aren't hashable by default;
        # we never hash these objects in production, but tests sometimes do.
        return hash(tuple(sorted((k, v.updated_at, v.last_pk) for k, v in self.per_table.items())))
