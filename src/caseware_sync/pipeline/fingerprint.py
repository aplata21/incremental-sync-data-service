"""Per-table schema fingerprint.

Spec: "A hash of column names and types is sufficient."

We hash a canonical JSON object containing the table name *and* its ordered
column list (each column rendered as ``{name, type}``). Including the
table name is mildly defensive: two tables with structurally identical
columns get distinct fingerprints, so a downstream consumer that keys
state by fingerprint cannot accidentally fold them together.

Like ``run_id``, the output is a SHA-256 hex prefix. Schema fingerprints
don't need to be globally unique, just stable, so a 16-hex-char prefix is
the default — short enough to read at a glance in a manifest.
"""

from __future__ import annotations

import hashlib

from ..domain.tables import TableSpec
from ..utils.jsonio import canonical_json_bytes


def compute_schema_fingerprint(table: TableSpec, *, hex_prefix_len: int = 16) -> str:
    """Return the deterministic schema fingerprint for ``table``.

    The fingerprint changes whenever any of:
        - the table name,
        - the ordered list of column names,
        - any column's pg_type
    changes. That covers the column-add / column-drop / column-rename /
    type-migration cases that downstream consumers need to react to.
    """
    if not 8 <= hex_prefix_len <= 64:
        raise ValueError(f"hex_prefix_len out of range: {hex_prefix_len}")
    payload = {
        "table": table.name,
        "columns": [
            {"name": col.name, "type": col.pg_type} for col in table.columns
        ],
    }
    digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    return digest[:hex_prefix_len]
