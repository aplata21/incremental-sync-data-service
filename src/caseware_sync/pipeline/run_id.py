"""Deterministic ``run_id`` derivation.

Property the spec demands:
    "the same source DB state and same checkpoint produce the same run_id."

We satisfy this by hashing a canonical JSON object of:
    - ``checkpoint_before`` (the watermark dict the run started from), and
    - per-table ordered row identities of the *exact* rows the incremental
      query selected.

Each row identity includes (table, pk, updated_at), per the spec. Tables
are sorted alphabetically (canonical JSON sorts keys), and identities
within each table preserve the deterministic query order
``updated_at ASC, pk ASC`` already enforced by the repository.

A SHA-256 hex prefix is used as the actual ``run_id``. The full digest is
overkill at this scale; a 32-hex-char prefix is effectively unique and
fits cleanly in filenames / logs. The prefix length is configurable so a
deployment can dial it up if it cares.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..utils.jsonio import canonical_json_bytes, iso_utc_z


@dataclass(frozen=True)
class RowIdentity:
    """The (table, pk, updated_at) tuple the spec calls a row identity."""

    table: str
    pk: int
    updated_at: datetime

    def to_canonical_dict(self) -> dict[str, Any]:
        # uniform "pk" key so the hash input shape is the same across tables;
        # the spec asks for the primary key value, not a particular field name.
        return {
            "table": self.table,
            "pk": int(self.pk),
            "updated_at": iso_utc_z(self.updated_at),
        }


def compute_run_id(
    *,
    checkpoint_before: Mapping[str, Any],
    table_deltas: Mapping[str, Sequence[RowIdentity]],
    hex_prefix_len: int = 32,
) -> str:
    """Return the deterministic ``run_id`` for this batch.

    Args:
        checkpoint_before: the on-disk checkpoint at the start of the run,
            already serialized to a JSON-friendly dict (e.g. via
            ``Checkpoint.to_dict()``).
        table_deltas: mapping of table name -> ordered row identities for
            that table's delta. Order *must* be ``(updated_at ASC, pk ASC)``
            -- which the repository already guarantees.
        hex_prefix_len: how many hex chars of the SHA-256 digest to return.
            Must be in [8, 64]; 32 is the default and is the spec's
            "stable prefix" sweet spot.

    Returns:
        A lower-case hex string of length ``hex_prefix_len``.
    """
    if not 8 <= hex_prefix_len <= 64:
        raise ValueError(f"hex_prefix_len out of range: {hex_prefix_len}")

    payload = {
        "checkpoint_before": _normalize_checkpoint(checkpoint_before),
        # canonical_json_bytes will sort table names alphabetically
        "tables": {
            name: [ident.to_canonical_dict() for ident in identities]
            for name, identities in table_deltas.items()
        },
    }
    digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    return digest[:hex_prefix_len]


def _normalize_checkpoint(checkpoint_before: Mapping[str, Any]) -> dict[str, Any]:
    """Return a plain JSON-friendly copy.

    Defensive: callers should already pass us a JSON-friendly dict (e.g.
    ``Checkpoint.to_dict()``), but if a raw ``Checkpoint`` accidentally
    leaks through, ``canonical_json_bytes`` would still serialize it via
    its encoder, producing a different hash than the dict form. Normalizing
    here removes that footgun.
    """
    return {str(k): _normalize_value(v) for k, v in checkpoint_before.items()}


def _normalize_value(v: Any) -> Any:
    if isinstance(v, Mapping):
        return {str(k): _normalize_value(val) for k, val in v.items()}
    if isinstance(v, datetime):
        return iso_utc_z(v)
    if isinstance(v, (list, tuple)):
        return [_normalize_value(x) for x in v]
    return v
