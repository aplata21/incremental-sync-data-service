"""Tiny value type passed from writers to the commit phase.

Every writer stages a final-form file at ``staged_path`` and tells the
commit phase where it should ultimately land at ``live_path``. The commit
phase is the only thing that ever does ``staged_path -> live_path``
renames, which is what keeps the writers free of coupling to commit
ordering.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StagedFile:
    staged_path: Path
    live_path: Path
