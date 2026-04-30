"""Canonical JSON serialization.

Two distinct use cases share these helpers:

1. Hash inputs (run_id, schema_fingerprint) -> we need *byte-stable*
   serialization independent of dict insertion order, microsecond formatting,
   or non-ASCII escapes.

2. On-disk artifacts (lake JSONL, share JSONL, events JSONL, checkpoint.json) ->
   the spec demands byte-for-byte reproducibility for the share artifact and
   sane re-runnability for the others. Reusing the same canonical serializer
   guarantees that.

Datetime handling
    All timestamps are normalized to UTC and emitted as RFC 3339 with a 'Z'
    suffix. Microseconds are included only when non-zero so seed-style
    whole-second timestamps render as ``"2026-03-31T12:00:00Z"``, matching the
    example in the spec, while microsecond-resolution timestamps round-trip
    losslessly.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any


def iso_utc_z(dt: datetime) -> str:
    """RFC 3339 / ISO 8601 in UTC with a 'Z' suffix.

    Determinism: a given ``datetime`` always produces the same string. We
    require tzinfo to avoid silent local-time bugs at ingestion time.
    """
    if dt.tzinfo is None:
        raise ValueError("iso_utc_z requires a timezone-aware datetime")
    s = dt.astimezone(timezone.utc).isoformat()
    # isoformat() emits '+00:00' for UTC; spec uses the 'Z' suffix.
    return s.replace("+00:00", "Z")


def parse_iso_utc(s: str) -> datetime:
    """Inverse of ``iso_utc_z``; tolerates both 'Z' and explicit offsets."""
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        # interpret bare timestamps as UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class _CanonicalEncoder(json.JSONEncoder):
    """Encoder for types that ``json`` doesn't natively support.

    Decimal is included because psycopg returns NUMERIC as ``Decimal``; we
    don't currently use NUMERIC but it costs nothing to be safe.
    """

    def default(self, o: Any) -> Any:  # noqa: D401
        if isinstance(o, datetime):
            return iso_utc_z(o)
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, bytes):
            return o.decode("utf-8")
        return super().default(o)


def canonical_json_bytes(obj: Any) -> bytes:
    """Stable bytes for hashing.

    - sorted keys: insertion order does not affect hashes.
    - tightest separators: no whitespace = no incidental drift.
    - ensure_ascii=False: identical bytes across platforms with the same
      input string content (no random ``\\uXXXX`` escapes for non-ASCII).
    """
    return json.dumps(
        obj,
        cls=_CanonicalEncoder,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_line(obj: Any) -> bytes:
    """Single JSONL record + trailing newline.

    Used for lake/share/events files. Matches ``canonical_json_bytes`` so
    the same record is byte-identical wherever it appears.
    """
    return canonical_json_bytes(obj) + b"\n"


def pretty_json_bytes(obj: Any) -> bytes:
    """For human-readable files like ./state/checkpoint.json. Still
    deterministic (sorted keys, same encoder), just easier to eyeball."""
    return (
        json.dumps(
            obj,
            cls=_CanonicalEncoder,
            sort_keys=True,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
