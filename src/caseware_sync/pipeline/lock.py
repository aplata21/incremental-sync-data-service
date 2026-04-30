"""Single-flight lock for the /ingest endpoint.

Two concurrent runs would race on the run staging dir, the lake partition
files, and the checkpoint -- which the spec doesn't require us to handle,
but a 409 is much better than a corrupt commit.

Implementation
    POSIX advisory file lock (``fcntl.flock`` with ``LOCK_EX | LOCK_NB``).
    The lock is held for the lifetime of the context manager and released
    automatically by the kernel when the holding process exits, so a
    crashed process never leaves a stale lock around.
    On unsupported platforms (e.g. Windows in CI) the lock degrades to a
    best-effort O_EXCL create; this is good enough for a local prototype.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from ..core.errors import IngestInProgressError
from ..utils.fs import ensure_dir


@contextmanager
def acquire_ingest_lock(lock_path: Path) -> Iterator[None]:
    """Acquire an exclusive non-blocking lock at ``lock_path``.

    Raises ``IngestInProgressError`` if another holder owns the lock.
    """
    ensure_dir(lock_path.parent)

    if sys.platform.startswith("win"):
        # Best-effort fallback. O_EXCL gives us mutual exclusion across
        # processes that didn't crash. A crashed prior holder requires
        # manual cleanup of the lock file, but production is POSIX.
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        except FileExistsError as e:
            raise IngestInProgressError(
                f"another /ingest is in progress (lock {lock_path})"
            ) from e
        try:
            yield
        finally:
            try:
                os.close(fd)
            finally:
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError:
                    pass
        return

    import fcntl  # POSIX only

    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as e:
            raise IngestInProgressError(
                f"another /ingest is in progress (lock {lock_path})"
            ) from e
        try:
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass
