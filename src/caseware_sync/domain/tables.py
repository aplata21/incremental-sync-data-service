"""Per-table contract.

A ``TableSpec`` is the single source of truth for everything the pipeline
needs to know about a table:

- the SELECT column list (and its order, which fixes JSONL field order),
- the primary-key column name,
- the column types (used to compute schema_fingerprint),
- the lake / share output roots (so the orchestrator never hardcodes paths).

The repository, the writers, the manifest builder, and the share record
serializer all read from the same TableSpec, so the schema is defined in
exactly one place.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str  # the Postgres type name, used for schema_fingerprint hashing


@dataclass(frozen=True)
class TableSpec:
    """Static description of a source table."""

    name: str
    primary_key: str
    columns: tuple[Column, ...]

    # Output sub-paths under the configured roots; lake uses date partitions.
    # We keep them as members so tests can route output to a tmp dir without
    # patching globals.
    lake_subdir: str = ""
    share_subdir: str = ""

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        if self.primary_key not in names:
            raise ValueError(
                f"primary_key {self.primary_key!r} is not in columns {names!r}"
            )
        if "updated_at" not in names:
            raise ValueError(f"table {self.name!r} must have an 'updated_at' column")
        if len(set(names)) != len(names):
            raise ValueError(f"table {self.name!r} has duplicate column names")
        # default subdirs
        if not self.lake_subdir:
            object.__setattr__(self, "lake_subdir", self.name)
        if not self.share_subdir:
            object.__setattr__(self, "share_subdir", self.name)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def select_clause(self) -> str:
        """Comma-separated, deterministically ordered column list for SELECT."""
        return ", ".join(c.name for c in self.columns)

    def lake_dir(self, lake_root: Path) -> Path:
        return lake_root / self.lake_subdir

    def share_path(self, share_root: Path) -> Path:
        return share_root / self.share_subdir / "changes.jsonl"


# -----------------------------------------------------------------------------
# Concrete table specs. Column order = JSONL field order.
# -----------------------------------------------------------------------------

CUSTOMERS = TableSpec(
    name="customers",
    primary_key="customer_id",
    columns=(
        Column("customer_id", "bigint"),
        Column("name", "text"),
        Column("email", "text"),
        Column("country", "text"),
        Column("updated_at", "timestamptz"),
    ),
)

CASES = TableSpec(
    name="cases",
    primary_key="case_id",
    columns=(
        Column("case_id", "bigint"),
        Column("customer_id", "bigint"),
        Column("title", "text"),
        Column("description", "text"),
        Column("status", "text"),
        Column("updated_at", "timestamptz"),
    ),
)


# Order matters: this is the canonical iteration order used by the
# orchestrator, the manifest, and the events file.
ALL_TABLES: tuple[TableSpec, ...] = (CUSTOMERS, CASES)


def by_name() -> dict[str, TableSpec]:
    return {t.name: t for t in ALL_TABLES}
