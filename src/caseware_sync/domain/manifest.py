"""Manifest returned by /ingest.

The shape is dictated by the spec. We model it as a Pydantic v2 BaseModel so
FastAPI gives us automatic JSON serialization with the right field names and
so the test suite can validate the shape end-to-end.

Path semantics (from the spec):
    - lake_paths is an empty list when delta_row_count = 0.
    - share_path is null when delta_row_count = 0.
    - Both are still populated for dry runs as *predicted* targets even
      though no files are written.

checkpoint_after for dry runs is informational only; the on-disk checkpoint
must be unchanged. The orchestrator enforces that — this model just carries
the value.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_serializer

from ..utils.jsonio import iso_utc_z


class TableManifest(BaseModel):
    table: str
    delta_row_count: int = Field(ge=0)
    lake_paths: list[str] = Field(default_factory=list)
    share_path: str | None = None
    schema_fingerprint: str


class Manifest(BaseModel):
    run_id: str
    started_at: datetime
    finished_at: datetime
    dry_run: bool
    checkpoint_before: dict[str, Any]
    checkpoint_after: dict[str, Any]
    tables: list[TableManifest]

    @field_serializer("started_at", "finished_at")
    def _ser_dt(self, dt: datetime) -> str:
        return iso_utc_z(dt)

    def to_canonical_dict(self) -> dict[str, Any]:
        """Plain JSON-friendly dict (no datetime objects); used by tests and
        when echoing the manifest into events / logs."""
        return self.model_dump(mode="json")
