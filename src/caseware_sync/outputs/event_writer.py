"""Per-run delta event writer.

Output (mandated by spec):
    ./events/<run_id>.jsonl

One JSON line per table included in the run, in the canonical iteration
order of ``ALL_TABLES`` (alphabetical: cases, customers).

Two emission paths
    1. **Durable.** ``stage_events`` writes the events file into the
       run staging dir; the commit phase swings it into ``./events/``.
       This path participates in the crash-consistency boundary.
    2. **Best-effort stdout.** ``emit_to_stdout`` prints one JSON line
       per event for human/operator visibility. Per spec, stdout
       emission is *not* part of the crash-consistency boundary -- a
       run is not considered failed if logging is interrupted.

Zero-delta tables still get an event so consumers can distinguish
"we ran with nothing to ship" from "we never ran". For those tables,
``delta_row_count`` is 0, ``lake_paths`` is ``[]``, and ``share_path``
is ``null``.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from ..utils.fs import write_bytes_atomic
from ..utils.jsonio import canonical_json_bytes, canonical_json_line
from .staged_file import StagedFile

if TYPE_CHECKING:
    from ..domain.events import Event  # noqa: F401  (used in annotations)


class _EventLike(Protocol):
    """Anything with a Pydantic-style ``model_dump(mode="json")``.

    Declared as a Protocol so the writer module does not pull in Pydantic at
    import time -- helpful for environments where the writer is exercised in
    isolation, and a clean abstraction for tests using fakes.
    """

    def model_dump(self, mode: str = ...) -> dict: ...


@dataclass(frozen=True)
class StagedEventFile(StagedFile):
    """The single per-run events file (one line per table)."""

    run_id: str
    event_count: int


class EventWriter:
    """Stages and (best-effort) prints per-run events."""

    def __init__(self, events_root: Path) -> None:
        self._events_root = events_root

    def stage_events(
        self,
        *,
        events: Sequence[_EventLike],
        run_id: str,
        run_events_staging: Path,
    ) -> StagedEventFile:
        live_path = self._events_root / f"{run_id}.jsonl"
        staged_path = run_events_staging / f"{run_id}.jsonl"

        buf = bytearray()
        for event in events:
            buf.extend(canonical_json_line(event.model_dump(mode="json")))

        write_bytes_atomic(staged_path, bytes(buf))
        return StagedEventFile(
            staged_path=staged_path,
            live_path=live_path,
            run_id=run_id,
            event_count=len(events),
        )

    @staticmethod
    def emit_to_stdout(events: Sequence[_EventLike]) -> None:
        """Best-effort mirror; per spec, this is outside the
        crash-consistency boundary. We swallow OSError on the write so a
        broken pipe never aborts a run."""
        for event in events:
            line = canonical_json_bytes(event.model_dump(mode="json"))
            try:
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.write(b"\n")
                sys.stdout.flush()
            except OSError:
                # stdout closed (broken pipe, redirect gone) -- spec is
                # explicit that this must not fail the run.
                return
