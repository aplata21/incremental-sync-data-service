"""Consumer-facing incremental share record.

One record per changed row in the latest successful incremental batch,
written to ./share/<table>/changes.jsonl. Records are ordered by
(updated_at, primary_key) and the file is byte-for-byte deterministic for
a given (source state, checkpoint_before) pair.

``op`` is restricted to ``"upsert"`` for this prototype: the source contract
gives us inserts and updates, never deletes, so a single op is sufficient
and matches the spec ("op may be upsert").
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, field_serializer

from ..utils.jsonio import iso_utc_z


class ShareRecord(BaseModel):
    table: str
    op: Literal["upsert"] = "upsert"
    # exactly one of these is populated, depending on table
    customer_id: int | None = None
    case_id: int | None = None
    updated_at: datetime
    run_id: str
    schema_fingerprint: str
    checkpoint_after: dict[str, Any]
    record: dict[str, Any]

    @field_serializer("updated_at")
    def _ser_dt(self, dt: datetime) -> str:
        return iso_utc_z(dt)

    def to_canonical_dict(self) -> dict[str, Any]:
        """Plain JSON-friendly dict, suitable for canonical JSONL emission.

        Pydantic v2's ``model_dump(mode="json")`` already serializes datetime,
        but we route through it here so the canonical encoder in
        ``utils.jsonio`` does not have to know about Pydantic types.
        """
        # exclude_none keeps the JSONL minimal and stable for byte-for-byte
        # determinism: the customers record never has a case_id field appear.
        return self.model_dump(mode="json", exclude_none=True)
