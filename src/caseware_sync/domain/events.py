"""Per-run delta event.

One event per table per run, written to ./events/<run_id>.jsonl as a
durable summary AND printed to stdout as a best-effort mirror.

Zero-delta tables still get an event (delta_row_count = 0, share_path = null,
lake_paths = []) so downstream consumers can tell "we ran, you have the
latest" apart from "we did not run".
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Event(BaseModel):
    table: str
    run_id: str
    schema_fingerprint: str
    delta_row_count: int = Field(ge=0)
    lake_paths: list[str] = Field(default_factory=list)
    share_path: str | None = None
    checkpoint_after: dict[str, Any]
