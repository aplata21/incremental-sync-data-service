"""Single-flight lock for /ingest."""

from __future__ import annotations

from pathlib import Path

import pytest

from caseware_sync.core.errors import IngestInProgressError
from caseware_sync.pipeline.lock import acquire_ingest_lock


def test_held_lock_raises_on_second_acquire(tmp_path: Path) -> None:
    lock = tmp_path / "state" / ".ingest.lock"
    with acquire_ingest_lock(lock):
        with pytest.raises(IngestInProgressError):
            with acquire_ingest_lock(lock):
                pass


def test_lock_releasable_and_reacquirable(tmp_path: Path) -> None:
    lock = tmp_path / "state" / ".ingest.lock"
    for _ in range(3):
        with acquire_ingest_lock(lock):
            pass
    # No leftover state should prevent acquire.


def test_lock_creates_parent_dir(tmp_path: Path) -> None:
    lock = tmp_path / "deep" / "nested" / ".ingest.lock"
    with acquire_ingest_lock(lock):
        assert lock.exists()
